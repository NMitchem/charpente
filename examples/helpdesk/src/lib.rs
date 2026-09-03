//! helpdesk — a small multi-tenant ticketing backend built on charpente,
//! from docs/GUIDE.md and README.md alone. See examples/helpdesk/FINDINGS.md
//! for the friction log and the design decisions the docs didn't cover.

use charpente::auth::ApiKey;
use charpente::core::io::{Disk, Entropy};
use charpente::core::migration::{MigrationError, TableMigration};
use charpente::core::table::Codec;
use charpente::http::{ApiError, App, AppConfig, Ctx, Cursor, Subscription};
use charpente::kernel::JobError;
use charpente::{
    Id, Schema, Tx, View, Wire, event, events, job, jobs, route, routes, row_shape, table,
};
use serde::{Deserialize, Serialize};
use std::io;
use std::time::Duration;

// ---------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------

#[table]
pub struct Org {
    pub id: Id<Org>,
    pub name: String,
}

#[table]
pub struct Agent {
    pub id: Id<Agent>,
    #[index]
    pub org: Id<Org>,
    #[index]
    pub subject: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TicketState {
    Open,
    Pending,
    Closed,
}

impl TicketState {
    fn as_str(&self) -> &'static str {
        match self {
            TicketState::Open => "Open",
            TicketState::Pending => "Pending",
            TicketState::Closed => "Closed",
        }
    }
}

impl Codec for TicketState {
    fn encode(&self, out: &mut Vec<u8>) {
        let tag: u8 = match self {
            TicketState::Open => 0,
            TicketState::Pending => 1,
            TicketState::Closed => 2,
        };
        tag.encode(out);
    }

    fn decode(input: &mut &[u8]) -> io::Result<Self> {
        match u8::decode(input)? {
            0 => Ok(TicketState::Open),
            1 => Ok(TicketState::Pending),
            2 => Ok(TicketState::Closed),
            other => Err(io::Error::other(format!(
                "invalid TicketState tag {other} — schema drift or corruption"
            ))),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TicketPriority {
    Low,
    Normal,
    High,
    Urgent,
}

impl TicketPriority {
    fn as_str(&self) -> &'static str {
        match self {
            TicketPriority::Low => "Low",
            TicketPriority::Normal => "Normal",
            TicketPriority::High => "High",
            TicketPriority::Urgent => "Urgent",
        }
    }
}

impl Codec for TicketPriority {
    fn encode(&self, out: &mut Vec<u8>) {
        let tag: u8 = match self {
            TicketPriority::Low => 0,
            TicketPriority::Normal => 1,
            TicketPriority::High => 2,
            TicketPriority::Urgent => 3,
        };
        tag.encode(out);
    }

    fn decode(input: &mut &[u8]) -> io::Result<Self> {
        match u8::decode(input)? {
            0 => Ok(TicketPriority::Low),
            1 => Ok(TicketPriority::Normal),
            2 => Ok(TicketPriority::High),
            3 => Ok(TicketPriority::Urgent),
            other => Err(io::Error::other(format!(
                "invalid TicketPriority tag {other} — schema drift or corruption"
            ))),
        }
    }
}

/// The superseded shape (GUIDE §12) — kept only so `TableMigration::migrate`
/// can decode bytes an older binary wrote. No `#[index]`, no redb
/// registration: `Ticket` shipped at this exact shape as its own, separate
/// commit before `priority` existed.
#[row_shape(version = 1)]
pub struct TicketV1 {
    pub id: Id<TicketV1>,
    pub org: Id<Org>,
    pub state: TicketState,
    pub opener_subject: String,
    pub title: String,
    pub body: String,
    pub pending_since_hour: Option<u64>,
}

#[table(version = 2)]
pub struct Ticket {
    pub id: Id<Ticket>,
    #[index]
    pub org: Id<Org>,
    #[index]
    pub state: TicketState,
    pub opener_subject: String,
    pub title: String,
    pub body: String,
    /// The app's own logical-clock reading (`AppClock.hour`, GUIDE §7) at the
    /// moment this ticket last became Pending — there is no wall clock in v0,
    /// so "older than 4 hours" is measured against this counter, not real time.
    pub pending_since_hour: Option<u64>,
    pub priority: TicketPriority,
}

impl TableMigration for Ticket {
    type Prev = TicketV1;
    fn migrate(prev: TicketV1) -> Result<Ticket, MigrationError> {
        Ok(Ticket {
            id: Id::from_raw(prev.id.raw()),
            org: prev.org,
            state: prev.state,
            opener_subject: prev.opener_subject,
            title: prev.title,
            body: prev.body,
            pending_since_hour: prev.pending_since_hour,
            priority: TicketPriority::Normal, // the field's required default
        })
    }
}

#[table]
pub struct Comment {
    pub id: Id<Comment>,
    #[index]
    pub ticket: Id<Ticket>,
    pub org: Id<Org>,
    pub author_subject: String,
    pub body: String,
}

/// One row per successfully-run `SendSurvey` job — GUIDE §4/§6's "job records
/// a survey row (a table; no external IO)".
#[table]
pub struct Survey {
    pub id: Id<Survey>,
    pub ticket: Id<Ticket>,
    pub org: Id<Org>,
}

/// One row per SLA-breach reminder actually sent. `key` is the ticket-hour
/// dedupe key ("<ticket id>:<hour>"), checked before insert so a retried or
/// re-run tick can never double-remind the same ticket in the same hour.
#[table]
pub struct SlaReminder {
    pub id: Id<SlaReminder>,
    pub ticket: Id<Ticket>,
    pub org: Id<Org>,
    pub hour: u64,
    #[index]
    pub key: String,
}

/// The app's own logical clock (GUIDE §7, "A logical clock: reading now
/// inside a transaction") — there is no platform primitive for wall-clock
/// time inside a transaction, so `HourlyTick` advances this singleton row by
/// exactly one every real hour, and SLA-age is computed against it.
#[table]
pub struct AppClock {
    pub id: Id<AppClock>,
    pub hour: u64,
}

/// The id-generation pattern GUIDE §2 documents: a singleton counter row per
/// id space, for rows created inside a transaction with no caller-supplied
/// id to use (`Survey` and `SlaReminder` here — every other table's id comes
/// from the route's own `:id` path param, the same convention every mutation
/// example in the guide uses).
#[table]
pub struct IdCounter {
    pub id: Id<IdCounter>,
    pub next: u64,
}

const SURVEY_ID_SPACE: u64 = 1;
const SLA_REMINDER_ID_SPACE: u64 = 2;
const APP_CLOCK_ID: u64 = 0;

fn next_id<T>(tx: &mut Tx, seq: u64) -> io::Result<Id<T>> {
    let counter_id = Id::<IdCounter>::from_raw(seq);
    let mut counter = tx.get(counter_id)?.unwrap_or(IdCounter {
        id: counter_id,
        next: 0,
    });
    let value = counter.next;
    counter.next += 1;
    tx.put(&counter);
    Ok(Id::from_raw(value))
}

fn read_clock_hour(tx: &mut Tx) -> io::Result<u64> {
    let clock_id = Id::<AppClock>::from_raw(APP_CLOCK_ID);
    Ok(tx.get(clock_id)?.map(|c| c.hour).unwrap_or(0))
}

// ---------------------------------------------------------------------
// Events (GUIDE §11)
// ---------------------------------------------------------------------

#[event]
pub struct TicketOpened {
    pub ticket: Id<Ticket>,
}

#[event]
pub struct TicketClosed {
    pub ticket: Id<Ticket>,
}

#[event]
pub struct CommentAdded {
    pub ticket: Id<Ticket>,
    pub comment: Id<Comment>,
}

// ---------------------------------------------------------------------
// Jobs (GUIDE §6)
// ---------------------------------------------------------------------

/// Enqueued, in the same transaction, when a ticket closes. Records a
/// `Survey` row — no external IO, per the brief.
#[job]
pub struct SendSurvey {
    pub ticket: u64,
}

impl SendSurvey {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        let ticket_id = Id::<Ticket>::from_raw(self.ticket);
        let ticket = tx
            .get(ticket_id)?
            .ok_or_else(|| JobError::App("SendSurvey: ticket missing".to_string()))?;
        let survey_id = next_id::<Survey>(tx, SURVEY_ID_SPACE)?;
        tx.put(&Survey {
            id: survey_id,
            ticket: ticket_id,
            org: ticket.org,
        });
        Ok(())
    }
}

/// The cron-ticked job (GUIDE §7): advances the app's logical clock by one
/// hour, then sweeps every Pending ticket for an SLA breach (pending 4+
/// logical hours) and records at most one reminder per ticket-hour.
#[job]
pub struct HourlyTick {}

impl HourlyTick {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        let clock_id = Id::<AppClock>::from_raw(APP_CLOCK_ID);
        let mut clock = tx.get(clock_id)?.unwrap_or(AppClock {
            id: clock_id,
            hour: 0,
        });
        clock.hour += 1;
        let now_hour = clock.hour;
        tx.put(&clock);

        for ticket in Ticket::by_state(tx, &TicketState::Pending)? {
            let since = match ticket.pending_since_hour {
                Some(h) => h,
                None => continue, // defensive: Pending tickets always set this
            };
            if now_hour.saturating_sub(since) < 4 {
                continue;
            }
            let key = format!("{}:{now_hour}", ticket.id.raw());
            if !SlaReminder::by_key(tx, &key)?.is_empty() {
                continue; // already reminded this exact ticket-hour
            }
            let reminder_id = next_id::<SlaReminder>(tx, SLA_REMINDER_ID_SPACE)?;
            tx.put(&SlaReminder {
                id: reminder_id,
                ticket: ticket.id,
                org: ticket.org,
                hour: now_hour,
                key,
            });
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------
// Tenant scoping: GUIDE never documents a typed scoped-table surface
// (README's own Status section says so explicitly, as of this checkout), so
// every route below derives the caller's org from the authenticated
// subject and filters explicitly — see FINDINGS.md.
// ---------------------------------------------------------------------

fn caller_org(tx: &mut Tx, ctx: &Ctx) -> Result<(Id<Org>, String), ApiError> {
    let subject = ctx
        .principal()
        .expect("auth routes always have a principal")
        .subject
        .clone();
    let agent = Agent::by_subject(tx, &subject)?.into_iter().next().ok_or_else(|| {
        ApiError::Internal("authenticated subject has no agent record".to_string())
    })?;
    Ok((agent.org, subject))
}

// ---------------------------------------------------------------------
// Wire views
// ---------------------------------------------------------------------

#[derive(Wire, Serialize)]
pub struct OrgView {
    pub id: Id<Org>,
    pub name: String,
}

impl From<Org> for OrgView {
    fn from(row: Org) -> Self {
        OrgView {
            id: row.id,
            name: row.name,
        }
    }
}

#[derive(Wire, Serialize, Deserialize)]
pub struct CreateOrgBody {
    pub name: String,
}

#[derive(Wire, Serialize)]
pub struct AgentView {
    pub id: Id<Agent>,
    pub org: Id<Org>,
    pub subject: String,
}

impl From<Agent> for AgentView {
    fn from(row: Agent) -> Self {
        AgentView {
            id: row.id,
            org: row.org,
            subject: row.subject,
        }
    }
}

#[derive(Wire, Serialize, Deserialize)]
pub struct CreateAgentBody {
    pub org: Id<Org>,
    pub subject: String,
}

#[derive(Wire, Serialize)]
pub struct TicketView {
    pub id: Id<Ticket>,
    pub org: Id<Org>,
    pub state: String,
    pub title: String,
    pub body: String,
    pub priority: String,
}

impl From<Ticket> for TicketView {
    fn from(row: Ticket) -> Self {
        TicketView {
            id: row.id,
            org: row.org,
            state: row.state.as_str().to_string(),
            title: row.title,
            body: row.body,
            priority: row.priority.as_str().to_string(),
        }
    }
}

#[derive(Wire, Serialize, Deserialize)]
pub struct CreateTicketBody {
    pub title: String,
    pub body: String,
}

#[derive(Wire, Serialize, Deserialize)]
pub struct TransitionBody {
    pub to: String,
}

#[derive(Wire, Serialize)]
pub struct CommentView {
    pub id: Id<Comment>,
    pub ticket: Id<Ticket>,
    pub author_subject: String,
    pub body: String,
}

impl From<Comment> for CommentView {
    fn from(row: Comment) -> Self {
        CommentView {
            id: row.id,
            ticket: row.ticket,
            author_subject: row.author_subject,
            body: row.body,
        }
    }
}

#[derive(Wire, Serialize, Deserialize)]
pub struct CreateCommentBody {
    pub body: String,
}

// ---------------------------------------------------------------------
// Routes: orgs, agents, auth
// ---------------------------------------------------------------------

/// Bootstrap route: no `auth` (there is nothing to authenticate against
/// before an org exists) — GUIDE never documents an invite/permission
/// scheme, so this is the simplest thing the docs support. See FINDINGS.md.
#[route(POST "/orgs/:id")]
fn create_org(tx: &mut Tx, id: Id<Org>, body: CreateOrgBody) -> Result<OrgView, ApiError> {
    if tx.get(id)?.is_some() {
        return Err(ApiError::BadRequest("org already exists".to_string()));
    }
    let org = Org { id, name: body.name };
    tx.put(&org);
    Ok(org.into())
}

/// Bootstrap route: registers an agent (user) into an org. Also no `auth`,
/// for the same reason as `create_org` — see FINDINGS.md.
#[route(POST "/agents/:id")]
fn create_agent(tx: &mut Tx, id: Id<Agent>, body: CreateAgentBody) -> Result<AgentView, ApiError> {
    if tx.get(id)?.is_some() {
        return Err(ApiError::BadRequest("agent already exists".to_string()));
    }
    if tx.get(body.org)?.is_none() {
        return Err(ApiError::NotFound);
    }
    if !Agent::by_subject(tx, &body.subject)?.is_empty() {
        return Err(ApiError::BadRequest("subject already registered".to_string()));
    }
    let agent = Agent {
        id,
        org: body.org,
        subject: body.subject,
    };
    tx.put(&agent);
    Ok(agent.into())
}

#[route(GET "/agents/:id", auth)]
fn get_agent(tx: &mut Tx, id: Id<Agent>, ctx: &Ctx) -> Result<AgentView, ApiError> {
    let (org, _) = caller_org(tx, ctx)?;
    let agent = tx.get(id)?.ok_or(ApiError::NotFound)?;
    if agent.org.raw() != org.raw() {
        return Err(ApiError::NotFound);
    }
    Ok(agent.into())
}

#[route(POST "/login")]
fn login(tx: &mut Tx, subject: String, ctx: &Ctx) -> Result<String, ApiError> {
    if Agent::by_subject(tx, &subject)?.is_empty() {
        return Err(ApiError::NotFound);
    }
    Ok(ctx.create_session(tx, subject))
}

#[route(POST "/logout", auth)]
fn logout(tx: &mut Tx, ctx: &Ctx) -> Result<&'static str, ApiError> {
    ctx.revoke(tx);
    Ok("bye")
}

#[route(POST "/keys", auth)]
fn mint_key(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let subject = ctx.principal().expect("auth route").subject.clone();
    Ok(ctx.create_api_key(tx, subject))
}

#[route(DELETE "/keys/:id", auth)]
fn revoke_key(tx: &mut Tx, id: Id<ApiKey>, ctx: &Ctx) -> Result<&'static str, ApiError> {
    let _ = ctx;
    tx.delete::<ApiKey>(id);
    Ok("revoked")
}

// ---------------------------------------------------------------------
// Routes: tickets
// ---------------------------------------------------------------------

#[route(POST "/tickets/:id", auth)]
fn create_ticket(
    tx: &mut Tx,
    id: Id<Ticket>,
    body: CreateTicketBody,
    ctx: &Ctx,
) -> Result<TicketView, ApiError> {
    let (org, subject) = caller_org(tx, ctx)?;
    if tx.get(id)?.is_some() {
        return Err(ApiError::BadRequest("ticket already exists".to_string()));
    }
    let ticket = Ticket {
        id,
        org,
        state: TicketState::Open,
        opener_subject: subject,
        title: body.title,
        body: body.body,
        pending_since_hour: None,
        priority: TicketPriority::Normal,
    };
    tx.put(&ticket);
    tx.emit_keyed(org.raw().to_string(), TicketOpened { ticket: id });
    Ok(ticket.into())
}

#[route(GET "/tickets/:id", auth)]
fn get_ticket(tx: &mut Tx, id: Id<Ticket>, ctx: &Ctx) -> Result<TicketView, ApiError> {
    let (org, _) = caller_org(tx, ctx)?;
    let ticket = tx.get(id)?.ok_or(ApiError::NotFound)?;
    if ticket.org.raw() != org.raw() {
        return Err(ApiError::NotFound);
    }
    Ok(ticket.into())
}

#[route(GET "/tickets", auth)]
fn list_tickets(tx: &mut Tx, ctx: &Ctx) -> Result<Vec<TicketView>, ApiError> {
    let (org, _) = caller_org(tx, ctx)?;
    Ok(Ticket::by_org(tx, &org)?.into_iter().map(Into::into).collect())
}

/// The one-transaction close behaviour the brief requires: mark Closed,
/// enqueue the survey job, emit `TicketClosed` — or reject any transition
/// that isn't Open->Pending or Pending->Closed with a named, actionable
/// error. See FINDINGS.md for why exactly these two edges are legal.
#[route(POST "/tickets/:id/transition", auth)]
fn transition_ticket(
    tx: &mut Tx,
    id: Id<Ticket>,
    body: TransitionBody,
    ctx: &Ctx,
) -> Result<TicketView, ApiError> {
    let (org, _) = caller_org(tx, ctx)?;
    let mut ticket = tx.get(id)?.ok_or(ApiError::NotFound)?;
    if ticket.org.raw() != org.raw() {
        return Err(ApiError::NotFound);
    }
    let target = match body.to.as_str() {
        "Open" => TicketState::Open,
        "Pending" => TicketState::Pending,
        "Closed" => TicketState::Closed,
        other => {
            return Err(ApiError::BadRequest(format!(
                "unknown ticket state \"{other}\" — valid states are Open, Pending, Closed"
            )));
        }
    };
    let valid = matches!(
        (ticket.state, target),
        (TicketState::Open, TicketState::Pending) | (TicketState::Pending, TicketState::Closed)
    );
    if !valid {
        return Err(ApiError::BadRequest(format!(
            "cannot transition ticket {} from {} to {} — valid transitions are Open -> Pending -> Closed",
            id.raw(),
            ticket.state.as_str(),
            target.as_str()
        )));
    }
    ticket.state = target;
    if let TicketState::Pending = ticket.state {
        let now_hour = read_clock_hour(tx)?;
        ticket.pending_since_hour = Some(now_hour);
    }
    tx.put(&ticket);
    if let TicketState::Closed = ticket.state {
        tx.enqueue(SendSurvey { ticket: id.raw() });
        tx.emit_keyed(org.raw().to_string(), TicketClosed { ticket: id });
    }
    Ok(ticket.into())
}

// ---------------------------------------------------------------------
// Routes: comments
// ---------------------------------------------------------------------

#[route(POST "/tickets/:ticket_id/comments/:id", auth)]
fn create_comment(
    tx: &mut Tx,
    ticket_id: Id<Ticket>,
    id: Id<Comment>,
    body: CreateCommentBody,
    ctx: &Ctx,
) -> Result<CommentView, ApiError> {
    let (org, subject) = caller_org(tx, ctx)?;
    let ticket = tx.get(ticket_id)?.ok_or(ApiError::NotFound)?;
    if ticket.org.raw() != org.raw() {
        return Err(ApiError::NotFound);
    }
    if tx.get(id)?.is_some() {
        return Err(ApiError::BadRequest("comment already exists".to_string()));
    }
    let comment = Comment {
        id,
        ticket: ticket_id,
        org,
        author_subject: subject,
        body: body.body,
    };
    tx.put(&comment);
    tx.emit_keyed(
        org.raw().to_string(),
        CommentAdded {
            ticket: ticket_id,
            comment: id,
        },
    );
    Ok(comment.into())
}

#[route(GET "/tickets/:ticket_id/comments", auth)]
fn list_comments(tx: &mut Tx, ticket_id: Id<Ticket>, ctx: &Ctx) -> Result<Vec<CommentView>, ApiError> {
    let (org, _) = caller_org(tx, ctx)?;
    let ticket = tx.get(ticket_id)?.ok_or(ApiError::NotFound)?;
    if ticket.org.raw() != org.raw() {
        return Err(ApiError::NotFound);
    }
    Ok(Comment::by_ticket(tx, &ticket_id)?.into_iter().map(Into::into).collect())
}

// ---------------------------------------------------------------------
// Realtime: one SSE feed per org, keyed by org id (GUIDE §11)
// ---------------------------------------------------------------------

#[route(SSE "/events/feed", auth, emits(TicketOpened, TicketClosed, CommentAdded))]
fn events_feed(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let (org, _) = caller_org(tx, ctx)?;
    Ok(Subscription::of::<TicketOpened>()
        .and::<TicketClosed>()
        .and::<CommentAdded>()
        .key(org.raw().to_string())
        .cursor(cursor))
}

// ---------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------

pub fn build_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> io::Result<App> {
    Ok(App::open(
        disk,
        Schema::new()
            .table::<Org>()
            .table::<Agent>()
            .table::<Ticket>()
            .migration::<Ticket>("Ticket")
            .table::<Comment>()
            .table::<Survey>()
            .table::<SlaReminder>()
            .table::<AppClock>()
            .table::<IdCounter>(),
        entropy,
        AppConfig::default(),
    )?
    .mount(routes![
        create_org,
        create_agent,
        get_agent,
        login,
        logout,
        mint_key,
        revoke_key,
        create_ticket,
        get_ticket,
        list_tickets,
        transition_ticket,
        create_comment,
        list_comments,
        events_feed,
    ])
    .mount_jobs(jobs![SendSurvey, HourlyTick])
    .mount_events(events![TicketOpened, TicketClosed, CommentAdded])
    .schedule("hourly-sla-sweep", Duration::from_secs(3600), || HourlyTick {}))
}
