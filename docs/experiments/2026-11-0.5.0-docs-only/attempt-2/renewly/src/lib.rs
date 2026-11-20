//! renewly: a workspace sells plans, its customers subscribe, and each period
//! is renewed by one charge at billing.example.net.
//!
//! The state machine lives on `Subscription.state`:
//!
//! - `active`    → `renewing` when the period ends (`StartRenewal`, a timed
//!   job), → `cancelled` when the customer cancels;
//! - `renewing`  → `active` when the charge settles (`Charge::settle`),
//!   → `past_due` when the charge is given up (`Charge::on_dead`);
//! - `past_due`  → `renewing` when the owner retries;
//! - `cancelled` → nothing.
//!
//! Every period is charged under one provider idempotency key,
//! `renewly-<workspace>-<subscription>-<period>`, built from the period itself
//! rather than the job, so an owner retry of a given-up renewal (a new job)
//! still dedupes against the call that may have reached the provider.

use charpente::auth::Principal;
use charpente::core::io::{Disk, Entropy, WallTime};
use charpente::core::table::Codec;
use charpente::http::{ApiError, App, AppConfig, Ctx};
use charpente::kernel::JobError;
use charpente::outbound::{Call, Host, IdempotencyKey, Reply};
use charpente::wire::WireU64;
use charpente::{
    Dead, Id, Schema, Scoped, ScopedTx, Tenant, Tx, Wire, job, jobs, route, routes, table,
};
use serde::{Deserialize, Serialize};
use std::io;

/// The payment provider every renewal is charged through.
pub const BILLING_HOST: &str = "billing.example.net";
/// Nanoseconds in a day and a second.
pub const DAY_NS: u64 = 86_400_000_000_000;
pub const SECOND_NS: u64 = 1_000_000_000;

/// Sequence spaces, one counter row each, per workspace.
const SEQ_RENEWAL: u64 = 1;
const SEQ_NOTICE: u64 = 2;

#[table(global)]
pub struct Workspace {
    pub id: Id<Workspace>,
    pub name: String,
}

/// Who a credential belongs to. Subjects are minted by the operator who
/// provisions a workspace: `owner:<ws>` or `customer:<ws>:<customer>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Caller {
    Owner { workspace: u64 },
    Customer { workspace: u64, customer: u64 },
}

impl Caller {
    pub fn parse(subject: &str) -> Option<Caller> {
        let mut parts = subject.split(":");
        match (parts.next()?, parts.next()?, parts.next()) {
            ("owner", ws, None) => Some(Caller::Owner {
                workspace: ws.parse().ok()?,
            }),
            ("customer", ws, Some(customer)) => Some(Caller::Customer {
                workspace: ws.parse().ok()?,
                customer: customer.parse().ok()?,
            }),
            _ => None,
        }
    }

    pub fn workspace(self) -> u64 {
        match self {
            Caller::Owner { workspace } | Caller::Customer { workspace, .. } => workspace,
        }
    }
}

impl Tenant for Workspace {
    const NAME: &'static str = "Workspace";
    type Who = Principal;
    type Error = ApiError;

    /// A caller reaches a workspace only when its subject names that
    /// workspace, as its owner or as one of its customers.
    fn authorize(_tx: &Tx, who: &Principal, scope: Id<Workspace>) -> Result<(), ApiError> {
        match Caller::parse(who.subject()) {
            Some(caller) if caller.workspace() == scope.raw() => Ok(()),
            _ => Err(not_a_member(scope)),
        }
    }
}

pub fn not_a_member(scope: Id<Workspace>) -> ApiError {
    ApiError::BadRequest(format!("not a member of workspace {}", scope.raw()))
}

/// A subscription's state. Each state names the transitions it allows, and a
/// refused transition lists them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubState {
    Active,
    Renewing,
    PastDue,
    Cancelled,
}

impl SubState {
    pub fn name(self) -> &'static str {
        match self {
            SubState::Active => "active",
            SubState::Renewing => "renewing",
            SubState::PastDue => "past_due",
            SubState::Cancelled => "cancelled",
        }
    }

    /// The transitions this state allows, by name.
    pub fn allows(self) -> &'static [&'static str] {
        match self {
            SubState::Active => &["cancel", "renew"],
            SubState::Renewing => &["settle", "give up"],
            SubState::PastDue => &["retry"],
            SubState::Cancelled => &[],
        }
    }
}

impl Codec for SubState {
    fn encode(&self, out: &mut Vec<u8>) {
        let tag: u8 = match self {
            SubState::Active => 0,
            SubState::Renewing => 1,
            SubState::PastDue => 2,
            SubState::Cancelled => 3,
        };
        tag.encode(out);
    }

    fn decode(input: &mut &[u8]) -> io::Result<Self> {
        match u8::decode(input)? {
            0 => Ok(SubState::Active),
            1 => Ok(SubState::Renewing),
            2 => Ok(SubState::PastDue),
            3 => Ok(SubState::Cancelled),
            other => Err(io::Error::other(format!(
                "invalid SubState tag {other} — schema drift or corruption"
            ))),
        }
    }
}

/// The text a refused transition answers with: the transition, the state,
/// and what that state allows.
pub fn refusal(transition: &str, subscription: u64, state: SubState) -> String {
    let allowed = match state.allows() {
        [] => "no transitions".to_string(),
        names => names.join(", "),
    };
    format!(
        "cannot {transition} subscription {subscription}: it is {state}, and {state} allows {allowed}",
        state = state.name()
    )
}

#[table(scoped_by = Workspace)]
pub struct Plan {
    pub id: Id<Plan>,
    pub name: String,
    pub price_cents: u64,
    pub period_days: u32,
}

#[table(scoped_by = Workspace)]
pub struct Customer {
    pub id: Id<Customer>,
    pub name: String,
    pub email: String,
}

#[table(scoped_by = Workspace)]
pub struct Subscription {
    pub id: Id<Subscription>,
    #[index]
    pub customer: Id<Customer>,
    pub plan: Id<Plan>,
    /// How far into its first period the subscription was imported, as asked.
    pub started_secs_ago: u64,
    pub state: SubState,
    /// The current period: 0 is the one the subscription started in, and
    /// each settled renewal moves to the next.
    pub period: u32,
    pub period_start_ns: u64,
    pub period_end_ns: u64,
}

/// A cancellation, keyed by its own id so a re-sent one is a repeat.
#[table(scoped_by = Workspace)]
pub struct Cancellation {
    pub id: Id<Cancellation>,
    pub subscription: Id<Subscription>,
    /// The period the cancellation landed in, the last one it pays for.
    pub period: u32,
}

/// An owner's retry of a past-due renewal, keyed by its own id.
#[table(scoped_by = Workspace)]
pub struct RetryRequest {
    pub id: Id<RetryRequest>,
    pub subscription: Id<Subscription>,
    /// The period the retry charges for.
    pub period: u32,
}

/// The start of a renewal: when the period it renews into was due, and when
/// the renewal began, both on this node's clock.
#[table(scoped_by = Workspace)]
pub struct Renewal {
    pub id: Id<Renewal>,
    #[index]
    pub subscription: Id<Subscription>,
    pub period: u32,
    pub due_ns: u64,
    pub started_ns: u64,
}

/// What a customer is told when a renewal is given up.
#[table(scoped_by = Workspace)]
pub struct Notice {
    pub id: Id<Notice>,
    #[index]
    pub customer: Id<Customer>,
    pub subscription: Id<Subscription>,
    pub plan: Id<Plan>,
    pub plan_name: String,
    pub period: u32,
    pub failed_at_ns: u64,
    pub text: String,
}

/// One counter per id space (GUIDE §2, "Generating ids").
#[table(scoped_by = Workspace)]
pub struct Sequence {
    pub id: Id<Sequence>,
    pub next: u64,
}

type WsTx<'a, 'b> = ScopedTx<'a, 'b, Workspace>;

fn next_id(tx: &mut WsTx<'_, '_>, space: u64) -> io::Result<u64> {
    let id = Id::<Sequence>::from_raw(space);
    let mut seq = tx.get(id)?.unwrap_or(Sequence { id, next: 1 });
    let value = seq.next;
    seq.next = value
        .checked_add(1)
        .ok_or_else(|| io::Error::other("id space exhausted"))?;
    tx.put(&seq);
    Ok(value)
}

fn bad(text: impl Into<String>) -> ApiError {
    ApiError::BadRequest(text.into())
}

fn caller(ctx: &Ctx) -> Result<Caller, ApiError> {
    ctx.principal()
        .and_then(|p| Caller::parse(p.subject()))
        .ok_or_else(|| bad("this credential names no owner or customer"))
}

fn require_owner(caller: Caller, what: &str) -> Result<(), ApiError> {
    match caller {
        Caller::Owner { .. } => Ok(()),
        Caller::Customer { .. } => Err(bad(format!("only the workspace owner may {what}"))),
    }
}

/// The owner, or the customer the subscription belongs to.
fn may_act_on(caller: Caller, sub: &Subscription) -> Result<(), ApiError> {
    match caller {
        Caller::Owner { .. } => Ok(()),
        Caller::Customer { customer, .. } if customer == sub.customer.raw() => Ok(()),
        Caller::Customer { .. } => Err(bad(format!(
            "subscription {} belongs to another customer",
            sub.id.raw()
        ))),
    }
}

/// The length of one period of `plan`, in nanoseconds.
fn period_len(plan: &Plan) -> io::Result<u64> {
    u64::from(plan.period_days)
        .checked_mul(DAY_NS)
        .ok_or_else(|| io::Error::other("plan period overflows"))
}

/// The provider idempotency key of one period's charge.
pub fn charge_key(workspace: u64, subscription: u64, period: u32) -> String {
    format!("renewly-{workspace}-{subscription}-{period}")
}

#[derive(Wire, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanBody {
    pub name: String,
    pub price_cents: WireU64,
    pub period_days: u32,
}

#[derive(Wire, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomerBody {
    pub name: String,
    pub email: String,
}

#[derive(Wire, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeBody {
    pub customer: Id<Customer>,
    pub plan: Id<Plan>,
    /// Imports a subscription part-way through its current period: how many
    /// seconds of that period have already passed. Owner only; 0 starts now.
    pub started_secs_ago: WireU64,
}

#[derive(Wire, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransitionBody {
    pub subscription: Id<Subscription>,
}

#[derive(Wire, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionView {
    pub id: Id<Subscription>,
    pub customer: Id<Customer>,
    pub plan: Id<Plan>,
    pub plan_name: String,
    pub state: String,
    pub period: u32,
    pub period_start_ns: WireU64,
    pub period_end_ns: WireU64,
}

#[derive(Wire, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NoticeView {
    pub id: Id<Notice>,
    pub subscription: Id<Subscription>,
    pub plan: Id<Plan>,
    pub plan_name: String,
    pub period: u32,
    pub failed_at_ns: WireU64,
    pub text: String,
}

fn view(tx: &WsTx<'_, '_>, sub: Subscription) -> Result<SubscriptionView, ApiError> {
    let plan = tx.get(sub.plan)?.ok_or(ApiError::NotFound)?;
    Ok(SubscriptionView {
        id: sub.id,
        customer: sub.customer,
        plan: sub.plan,
        plan_name: plan.name,
        state: sub.state.name().to_string(),
        period: sub.period,
        period_start_ns: WireU64(sub.period_start_ns),
        period_end_ns: WireU64(sub.period_end_ns),
    })
}

/// Creates a plan. Owner only. A repeat of the same plan is answered alike;
/// another plan under the same id is a conflict.
#[route(PUT "/workspaces/:ws/plans/:id", auth)]
fn put_plan(
    tx: &mut Tx,
    ws: Id<Workspace>,
    id: Id<Plan>,
    body: PlanBody,
    ctx: &Ctx,
) -> Result<Id<Plan>, ApiError> {
    let mut tx = tx.scope::<Workspace>(ws, ctx)?;
    require_owner(caller(ctx)?, "create plans")?;
    if let Some(plan) = tx.get(id)? {
        if plan.name == body.name
            && plan.price_cents == body.price_cents.0
            && plan.period_days == body.period_days
        {
            return Ok(id);
        }
        return Err(bad(format!(
            "plan {} already exists with other terms — a plan is written once; create the new terms under a new id",
            id.raw()
        )));
    }
    if body.period_days == 0 {
        return Err(bad("a plan period is a whole number of days, at least 1"));
    }
    tx.put(&Plan {
        id,
        name: body.name,
        price_cents: body.price_cents.0,
        period_days: body.period_days,
    });
    Ok(id)
}

/// Registers a customer. Owner only; written once, like a plan.
#[route(PUT "/workspaces/:ws/customers/:id", auth)]
fn put_customer(
    tx: &mut Tx,
    ws: Id<Workspace>,
    id: Id<Customer>,
    body: CustomerBody,
    ctx: &Ctx,
) -> Result<Id<Customer>, ApiError> {
    let mut tx = tx.scope::<Workspace>(ws, ctx)?;
    require_owner(caller(ctx)?, "register customers")?;
    if let Some(customer) = tx.get(id)? {
        if customer.name == body.name && customer.email == body.email {
            return Ok(id);
        }
        return Err(bad(format!(
            "customer {} already exists with other details — a customer is written once",
            id.raw()
        )));
    }
    if !body.email.contains("@") {
        return Err(bad(format!("{:?} is not an email address", body.email)));
    }
    tx.put(&Customer {
        id,
        name: body.name,
        email: body.email,
    });
    Ok(id)
}

/// Subscribes a customer to a plan: the owner for any customer, a customer
/// for itself. The renewal is enqueued in the same commit, due when the
/// period ends.
#[route(PUT "/workspaces/:ws/subscriptions/:id", auth)]
fn subscribe(
    tx: &mut Tx,
    ws: Id<Workspace>,
    id: Id<Subscription>,
    body: SubscribeBody,
    ctx: &Ctx,
) -> Result<Id<Subscription>, ApiError> {
    let mut tx = tx.scope::<Workspace>(ws, ctx)?;
    let who = caller(ctx)?;
    if let Some(sub) = tx.get(id)? {
        if sub.customer == body.customer
            && sub.plan == body.plan
            && sub.started_secs_ago == body.started_secs_ago.0
        {
            return Ok(id);
        }
        return Err(bad(format!(
            "subscription {} already exists with other terms — a subscription is written once",
            id.raw()
        )));
    }
    match who {
        Caller::Owner { .. } => {}
        Caller::Customer { customer, .. } if customer == body.customer.raw() => {
            if body.started_secs_ago.0 != 0 {
                return Err(bad(
                    "only the workspace owner imports a subscription part-way through a period",
                ));
            }
        }
        Caller::Customer { .. } => return Err(bad("a customer subscribes only itself")),
    }
    tx.get(body.customer)?
        .ok_or_else(|| bad(format!("customer {} does not exist", body.customer.raw())))?;
    let plan = tx
        .get(body.plan)?
        .ok_or_else(|| bad(format!("plan {} does not exist", body.plan.raw())))?;
    let len = period_len(&plan)?;
    let elapsed = body
        .started_secs_ago
        .0
        .checked_mul(SECOND_NS)
        .filter(|elapsed| *elapsed < len)
        .ok_or_else(|| bad("an imported subscription must still be inside its current period"))?;
    let start = tx
        .now()
        .as_nanos()
        .checked_sub(elapsed)
        .ok_or_else(|| bad("the period would start before 1970"))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| bad("the period would end past the year 2554"))?;
    tx.put(&Subscription {
        id,
        customer: body.customer,
        plan: body.plan,
        started_secs_ago: body.started_secs_ago.0,
        state: SubState::Active,
        period: 0,
        period_start_ns: start,
        period_end_ns: end,
    });
    tx.enqueue_at(
        WallTime::from_nanos(end),
        StartRenewal {
            subscription: id,
            period: 1,
        },
    );
    Ok(id)
}

/// Cancels a subscription: its customer or the owner. Allowed only while
/// active; the subscription keeps its current period and is never charged
/// again.
#[route(PUT "/workspaces/:ws/cancellations/:id", auth)]
fn cancel(
    tx: &mut Tx,
    ws: Id<Workspace>,
    id: Id<Cancellation>,
    body: TransitionBody,
    ctx: &Ctx,
) -> Result<Id<Cancellation>, ApiError> {
    let mut tx = tx.scope::<Workspace>(ws, ctx)?;
    let who = caller(ctx)?;
    if let Some(done) = tx.get(id)? {
        if done.subscription == body.subscription {
            return Ok(id);
        }
        return Err(bad(format!(
            "cancellation {} already cancelled another subscription — send a new cancellation under a new id",
            id.raw()
        )));
    }
    let mut sub = tx.get(body.subscription)?.ok_or(ApiError::NotFound)?;
    may_act_on(who, &sub)?;
    if sub.state != SubState::Active {
        return Err(bad(refusal("cancel", sub.id.raw(), sub.state)));
    }
    sub.state = SubState::Cancelled;
    tx.put(&Cancellation {
        id,
        subscription: sub.id,
        period: sub.period,
    });
    tx.put(&sub);
    Ok(id)
}

/// The owner asks for a past-due renewal to be charged again: the same
/// period, under the same provider key.
#[route(PUT "/workspaces/:ws/retries/:id", auth)]
fn retry(
    tx: &mut Tx,
    ws: Id<Workspace>,
    id: Id<RetryRequest>,
    body: TransitionBody,
    ctx: &Ctx,
) -> Result<Id<RetryRequest>, ApiError> {
    let mut tx = tx.scope::<Workspace>(ws, ctx)?;
    require_owner(caller(ctx)?, "retry a renewal")?;
    if let Some(done) = tx.get(id)? {
        if done.subscription == body.subscription {
            return Ok(id);
        }
        return Err(bad(format!(
            "retry {} already retried another subscription — send a new retry under a new id",
            id.raw()
        )));
    }
    let mut sub = tx.get(body.subscription)?.ok_or(ApiError::NotFound)?;
    if sub.state != SubState::PastDue {
        return Err(bad(refusal("retry", sub.id.raw(), sub.state)));
    }
    let period = sub.period + 1;
    sub.state = SubState::Renewing;
    tx.put(&RetryRequest {
        id,
        subscription: sub.id,
        period,
    });
    tx.put(&sub);
    tx.enqueue(Charge {
        subscription: sub.id,
        period,
    });
    Ok(id)
}

/// One subscription and its state: the owner, or the customer it belongs to.
#[route(GET "/workspaces/:ws/subscriptions/:id", auth)]
fn read_subscription(
    tx: &mut Tx,
    ws: Id<Workspace>,
    id: Id<Subscription>,
    ctx: &Ctx,
) -> Result<SubscriptionView, ApiError> {
    let tx = tx.scope::<Workspace>(ws, ctx)?;
    let sub = tx.get(id)?.ok_or(ApiError::NotFound)?;
    may_act_on(caller(ctx)?, &sub)?;
    view(&tx, sub)
}

/// Every subscription of the workspace, with its state and period end.
/// Owner only.
#[route(GET "/workspaces/:ws/subscriptions", auth)]
fn list_subscriptions(
    tx: &mut Tx,
    ws: Id<Workspace>,
    ctx: &Ctx,
) -> Result<Vec<SubscriptionView>, ApiError> {
    let tx = tx.scope::<Workspace>(ws, ctx)?;
    require_owner(caller(ctx)?, "list subscriptions")?;
    let mut out = Vec::new();
    for sub in tx.scan::<Subscription>()? {
        out.push(view(&tx, sub)?);
    }
    Ok(out)
}

/// The calling customer's own notices.
#[route(GET "/workspaces/:ws/notices", auth)]
fn read_notices(tx: &mut Tx, ws: Id<Workspace>, ctx: &Ctx) -> Result<Vec<NoticeView>, ApiError> {
    let tx = tx.scope::<Workspace>(ws, ctx)?;
    let Caller::Customer { customer, .. } = caller(ctx)? else {
        return Err(bad("notices are read by the customer they were sent to"));
    };
    Ok(Notice::by_customer(&tx, &Id::from_raw(customer))?
        .into_iter()
        .map(|n| NoticeView {
            id: n.id,
            subscription: n.subscription,
            plan: n.plan,
            plan_name: n.plan_name,
            period: n.period,
            failed_at_ns: WireU64(n.failed_at_ns),
            text: n.text,
        })
        .collect())
}

/// Due when a period ends: moves an active subscription to `renewing` and
/// enqueues the charge for the next period, in one commit. A subscription
/// cancelled meanwhile, or already past this period, is left alone.
#[job(scoped_by = Workspace, retain_dead)]
pub struct StartRenewal {
    pub subscription: Id<Subscription>,
    pub period: u32,
}

impl StartRenewal {
    pub fn run(self, tx: &mut WsTx<'_, '_>) -> Result<(), JobError> {
        let Some(mut sub) = tx.get(self.subscription)? else {
            return Ok(());
        };
        if sub.state != SubState::Active || sub.period + 1 != self.period {
            return Ok(());
        }
        sub.state = SubState::Renewing;
        let renewal = next_id(tx, SEQ_RENEWAL)?;
        let started = tx.now().as_nanos();
        tx.put(&Renewal {
            id: Id::from_raw(renewal),
            subscription: sub.id,
            period: self.period,
            due_ns: sub.period_end_ns,
            started_ns: started,
        });
        tx.put(&sub);
        tx.enqueue(Charge {
            subscription: sub.id,
            period: self.period,
        });
        Ok(())
    }
}

/// One period's charge. Four attempts, 30 s backoff doubling, 10 s each; then
/// it is given up and the subscription is past due.
#[job(
    scoped_by = Workspace,
    on_dead,
    outbound = "billing.example.net",
    retry(max = 4, backoff = "30s"),
    timeout = "10s"
)]
pub struct Charge {
    pub subscription: Id<Subscription>,
    pub period: u32,
}

impl Charge {
    /// The subscription, while this charge is the one it waits on. A
    /// subscription has at most one charge in flight, for the period after its
    /// current one, and only while it is renewing; a renewing subscription
    /// waiting on another period means two charges are live, which would
    /// charge a period twice, so it is refused by name rather than sent.
    fn pending(&self, tx: &WsTx<'_, '_>) -> Result<Option<Subscription>, JobError> {
        let Some(sub) = tx.get(self.subscription)? else {
            return Ok(None);
        };
        if sub.state != SubState::Renewing {
            return Ok(None);
        }
        if sub.period + 1 != self.period {
            return Err(JobError::app(format!(
                "charge for period {} found subscription {} renewing period {} — refusing to charge a period that is not the one due",
                self.period,
                sub.id.raw(),
                sub.period + 1
            )));
        }
        Ok(Some(sub))
    }

    fn plan(tx: &WsTx<'_, '_>, sub: &Subscription) -> Result<Plan, JobError> {
        tx.get(sub.plan)?
            .ok_or_else(|| JobError::app(format!("plan {} is missing", sub.plan.raw())))
    }

    pub fn call(
        &self,
        tx: &WsTx<'_, '_>,
        _key: &IdempotencyKey,
    ) -> Result<Option<Call>, JobError> {
        let Some(sub) = self.pending(tx)? else {
            return Ok(None);
        };
        let plan = Self::plan(tx, &sub)?;
        let ws = tx.scope().raw();
        Ok(Some(
            Call::post("/v1/charges")
                .header("idempotency-key", charge_key(ws, sub.id.raw(), self.period))
                .form(&[
                    ("workspace", ws.to_string()),
                    ("subscription", sub.id.raw().to_string()),
                    ("customer", sub.customer.raw().to_string()),
                    ("period", self.period.to_string()),
                    ("amount_cents", plan.price_cents.to_string()),
                ]),
        ))
    }

    pub fn settle(self, tx: &mut WsTx<'_, '_>, reply: Reply) -> Result<(), JobError> {
        if reply.status() != 200 {
            return Err(JobError::app(format!(
                "{BILLING_HOST} answered {}",
                reply.status()
            )));
        }
        let Some(mut sub) = self.pending(tx)? else {
            return Ok(());
        };
        let len = period_len(&Self::plan(tx, &sub)?)?;
        let start = sub.period_end_ns;
        let end = start
            .checked_add(len)
            .ok_or_else(|| JobError::app("the next period would end past the year 2554"))?;
        sub.state = SubState::Active;
        sub.period = self.period;
        sub.period_start_ns = start;
        sub.period_end_ns = end;
        tx.put(&sub);
        tx.enqueue_at(
            WallTime::from_nanos(end),
            StartRenewal {
                subscription: sub.id,
                period: self.period + 1,
            },
        );
        Ok(())
    }

    /// The renewal is given up: the subscription is past due, and its
    /// customer gets one notice naming the plan and the time.
    pub fn on_dead(self, tx: &mut WsTx<'_, '_>, dead: Dead<Self>) -> Result<(), JobError> {
        let Some(mut sub) = self.pending(tx)? else {
            return Ok(());
        };
        let plan = Self::plan(tx, &sub)?;
        let now = tx.now().as_nanos();
        sub.state = SubState::PastDue;
        tx.put(&sub);
        let id = next_id(tx, SEQ_NOTICE)?;
        tx.put(&Notice {
            id: Id::from_raw(id),
            customer: sub.customer,
            subscription: sub.id,
            plan: plan.id,
            plan_name: plan.name.clone(),
            period: self.period,
            failed_at_ns: now,
            text: format!(
                "The renewal of your {} plan failed at {} (ns since the Unix epoch) after {} attempts; the subscription is past due.",
                plan.name,
                now,
                dead.attempts()
            ),
        });
        Ok(())
    }
}

/// The app, in the shape `charpente::launch` and the sim both take.
pub fn build_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> io::Result<App> {
    Ok(App::open(
        disk,
        Schema::new()
            .table::<Workspace>()
            .table::<Plan>()
            .table::<Customer>()
            .table::<Subscription>()
            .table::<Cancellation>()
            .table::<RetryRequest>()
            .table::<Renewal>()
            .table::<Notice>()
            .table::<Sequence>(),
        entropy,
        AppConfig::default(),
    )?
    .mount(routes![
        put_plan,
        put_customer,
        subscribe,
        cancel,
        retry,
        read_subscription,
        list_subscriptions,
        read_notices,
    ])
    .mount_jobs(jobs![StartRenewal, Charge])
    .outbound(Host::new(BILLING_HOST).bearer_env("RENEWLY_BILLING_KEY")))
}
