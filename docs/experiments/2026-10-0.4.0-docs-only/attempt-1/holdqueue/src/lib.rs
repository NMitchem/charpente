//! holdqueue: library holds on charpente.
//!
//! An operator registers branches and titles and records how many copies of a
//! title each branch holds. A patron places a hold on a title for pickup at a
//! branch; the hold waits (`requested`) until a copy at that branch is set
//! aside for it (`ready`), and then is picked up, expires after seven days
//! uncollected, or is cancelled. Copies go to holds in placement order.
//!
//! The queue itself lives in `queue.rs`; the outbound notification in
//! `notify.rs`. This file declares the tables, the wire shapes and the routes.

pub mod notify;
pub mod queue;

use charpente::core::io::{Disk, Entropy};
use charpente::http::{ApiError, App, AppConfig, Ctx};
use charpente::outbound::Host;
use charpente::{Id, Schema, Tx, View, Wire, jobs, route, routes, table};
use serde::{Deserialize, Serialize};
use std::io;
use std::time::Duration;

pub use notify::{NOTIFY_HOST, NotifyReady};
pub use queue::{HoldState, READY_HOURS, Tick};

/// The subject an operator credential is minted for.
pub const OPERATOR: &str = "operator";

/// A patron, known by id: the subject of their credential is `patron:<id>`.
/// A table only because `Id<T>` crosses the wire just for a `Row` type `T`
/// (GUIDE section 5 says any `T`; the compiler disagrees). The row is written
/// when an operator mints the patron a key.
#[table(global)]
pub struct Patron {
    pub id: Id<Patron>,
}

#[table(global)]
pub struct Branch {
    pub id: Id<Branch>,
    pub name: String,
}

#[table(global)]
pub struct Title {
    pub id: Id<Title>,
    pub name: String,
}

/// How many copies of one title one branch holds, and where they are: set
/// aside for a ready hold, or out with a patron who picked one up. The rest
/// are free on the shelf. `slot` is `"<branch>:<title>"`.
#[table(global)]
pub struct Stock {
    pub id: Id<Stock>,
    #[index]
    pub slot: String,
    pub branch: Id<Branch>,
    pub title: Id<Title>,
    pub copies: u32,
    pub set_aside: u32,
    pub on_loan: u32,
}

impl Stock {
    /// Copies on the shelf that no hold has and no patron has out.
    pub fn free(&self) -> u32 {
        self.copies.saturating_sub(self.set_aside + self.on_loan)
    }
}

/// A hold. `placed_seq`, `ready_seq` and `ended_seq` are stamps from one
/// app-wide sequence, so the order of every placement, readiness and ending
/// is recorded; `ready_hour` and `ended_hour` are readings of the logical
/// hour clock (`queue::Clock`).
#[table(global)]
pub struct Hold {
    pub id: Id<Hold>,
    #[index]
    pub patron: Id<Patron>,
    pub title: Id<Title>,
    pub branch: Id<Branch>,
    #[index]
    pub slot: String,
    #[index]
    pub state: HoldState,
    pub placed_seq: u64,
    pub ready_seq: Option<u64>,
    pub ready_hour: Option<u64>,
    pub ended_seq: Option<u64>,
    pub ended_hour: Option<u64>,
    pub notified: bool,
}

/// A copy returned to a branch, recorded once under the id the operator desk
/// gave it, so a re-sent return frees one copy, not two.
#[table(global)]
pub struct Return {
    pub id: Id<Return>,
    pub branch: Id<Branch>,
    pub title: Id<Title>,
}

/// The one-time operator bootstrap: present once the first operator key was
/// minted.
#[table(global)]
pub struct OperatorSeat {
    pub id: Id<OperatorSeat>,
}

// ---- wire shapes -------------------------------------------------------

#[derive(Wire, Serialize, Deserialize)]
pub struct NameBody {
    pub name: String,
}

#[derive(Wire, Serialize, Deserialize)]
pub struct CopiesBody {
    pub copies: u32,
}

#[derive(Wire, Serialize, Deserialize)]
pub struct HoldBody {
    pub title: Id<Title>,
    pub branch: Id<Branch>,
}

#[derive(Wire, Serialize, Deserialize)]
pub struct ReturnBody {
    pub branch: Id<Branch>,
    pub title: Id<Title>,
}

#[derive(Wire, Serialize, Deserialize)]
pub struct StockView {
    pub branch: Id<Branch>,
    pub title: Id<Title>,
    pub copies: u32,
    pub set_aside: u32,
    pub on_loan: u32,
    pub free: u32,
}

impl From<Stock> for StockView {
    fn from(s: Stock) -> Self {
        let free = s.free();
        StockView {
            branch: s.branch,
            title: s.title,
            copies: s.copies,
            set_aside: s.set_aside,
            on_loan: s.on_loan,
            free,
        }
    }
}

#[derive(Wire, Serialize, Deserialize)]
pub struct HoldView {
    pub id: Id<Hold>,
    pub patron: Id<Patron>,
    pub title: Id<Title>,
    pub branch: Id<Branch>,
    pub state: String,
}

impl From<Hold> for HoldView {
    fn from(h: Hold) -> Self {
        HoldView {
            id: h.id,
            patron: h.patron,
            title: h.title,
            branch: h.branch,
            state: h.state.name().to_string(),
        }
    }
}

#[derive(Wire, Serialize, Deserialize)]
pub struct HoldList {
    pub holds: Vec<HoldView>,
}

// ---- who is asking -----------------------------------------------------

fn subject(ctx: &Ctx) -> Result<String, ApiError> {
    ctx.principal()
        .map(|p| p.subject().to_string())
        .ok_or(ApiError::Unauthorized)
}

/// Refuses every caller but an operator.
fn require_operator(ctx: &Ctx) -> Result<(), ApiError> {
    let who = subject(ctx)?;
    if who == OPERATOR {
        Ok(())
    } else {
        Err(ApiError::BadRequest(format!(
            "operators only: this credential is for {who:?}; use an operator key"
        )))
    }
}

/// The patron a credential speaks for, or a refusal naming the subject.
fn require_patron(ctx: &Ctx) -> Result<Id<Patron>, ApiError> {
    let who = subject(ctx)?;
    who.strip_prefix("patron:")
        .and_then(|n| n.parse::<u64>().ok())
        .map(Id::from_raw)
        .ok_or_else(|| {
            ApiError::BadRequest(format!(
                "patrons only: this credential is for {who:?}; use a patron key"
            ))
        })
}

/// The subject a patron credential is minted for.
pub fn patron_subject(patron: Id<Patron>) -> String {
    format!("patron:{}", patron.raw())
}

// ---- operator routes ---------------------------------------------------

/// Mints the first operator key, once. Every later patron key is minted by an
/// operator. A second call is refused.
#[route(POST "/operator", public)]
fn bootstrap_operator(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let seat = Id::<OperatorSeat>::from_raw(0);
    if tx.get(seat)?.is_some() {
        return Err(ApiError::BadRequest(
            "the operator key was already minted: ask an operator to mint you a key".to_string(),
        ));
    }
    tx.put(&OperatorSeat { id: seat });
    Ok(ctx.create_api_key(tx, OPERATOR.to_string()))
}

/// An operator mints a key for a patron.
#[route(POST "/patrons/:patron/keys", auth)]
fn mint_patron_key(tx: &mut Tx, patron: Id<Patron>, ctx: &Ctx) -> Result<String, ApiError> {
    require_operator(ctx)?;
    tx.put(&Patron { id: patron });
    Ok(ctx.create_api_key(tx, patron_subject(patron)))
}

/// Registers (or renames) a branch.
#[route(PUT "/branches/:branch", auth)]
fn put_branch(tx: &mut Tx, branch: Id<Branch>, body: NameBody, ctx: &Ctx) -> Result<Id<Branch>, ApiError> {
    require_operator(ctx)?;
    tx.put(&Branch {
        id: branch,
        name: body.name,
    });
    Ok(branch)
}

/// Registers (or renames) a title.
#[route(PUT "/titles/:title", auth)]
fn put_title(tx: &mut Tx, title: Id<Title>, body: NameBody, ctx: &Ctx) -> Result<Id<Title>, ApiError> {
    require_operator(ctx)?;
    tx.put(&Title {
        id: title,
        name: body.name,
    });
    Ok(title)
}

/// Records how many copies of `title` `branch` holds. Raising the count frees
/// copies, which go to the waiting holds in placement order.
#[route(PUT "/branches/:branch/titles/:title/copies", auth)]
fn put_copies(
    tx: &mut Tx,
    branch: Id<Branch>,
    title: Id<Title>,
    body: CopiesBody,
    ctx: &Ctx,
) -> Result<StockView, ApiError> {
    require_operator(ctx)?;
    Ok(queue::set_copies(tx, branch, title, body.copies)?.into())
}

/// A copy of `title` came back to `branch`, recorded under the return id.
#[route(POST "/returns/:id", auth)]
fn record_return(tx: &mut Tx, id: Id<Return>, body: ReturnBody, ctx: &Ctx) -> Result<StockView, ApiError> {
    require_operator(ctx)?;
    Ok(queue::record_return(tx, id, body.branch, body.title)?.into())
}

/// The holds waiting on `title` at `branch`, in the order they will be served.
#[route(GET "/branches/:branch/titles/:title/queue", auth)]
fn waiting_queue(tx: &mut Tx, branch: Id<Branch>, title: Id<Title>, ctx: &Ctx) -> Result<HoldList, ApiError> {
    require_operator(ctx)?;
    let holds = queue::waiting(tx, branch, title)?;
    Ok(HoldList {
        holds: holds.into_iter().map(HoldView::from).collect(),
    })
}

/// The patron collected the copy set aside for this hold (recorded at the
/// desk, by an operator).
#[route(POST "/holds/:id/pickup", auth)]
fn pick_up(tx: &mut Tx, id: Id<Hold>, ctx: &Ctx) -> Result<HoldView, ApiError> {
    require_operator(ctx)?;
    Ok(queue::pick_up(tx, id)?.into())
}

// ---- patron routes -----------------------------------------------------

/// Places a hold under the id the patron client chose. The same request
/// again is answered with the hold as it stands and changes nothing; the same
/// id with another title, branch or patron is a conflict.
#[route(POST "/holds/:id", auth)]
fn place_hold(tx: &mut Tx, id: Id<Hold>, body: HoldBody, ctx: &Ctx) -> Result<HoldView, ApiError> {
    let patron = require_patron(ctx)?;
    Ok(queue::place(tx, id, patron, body.title, body.branch)?.into())
}

/// The patron withdraws a hold they have not picked up.
#[route(POST "/holds/:id/cancel", auth)]
fn cancel_hold(tx: &mut Tx, id: Id<Hold>, ctx: &Ctx) -> Result<HoldView, ApiError> {
    let patron = require_patron(ctx)?;
    Ok(queue::cancel(tx, id, patron)?.into())
}

/// The holds of the caller, each with its state, in id order.
#[route(GET "/holds", auth)]
fn my_holds(tx: &mut Tx, ctx: &Ctx) -> Result<HoldList, ApiError> {
    let patron = require_patron(ctx)?;
    let holds = Hold::by_patron(tx, &patron)?;
    Ok(HoldList {
        holds: holds.into_iter().map(HoldView::from).collect(),
    })
}

/// One hold of the caller. A hold of another patron reads as absent.
#[route(GET "/holds/:id", auth)]
fn my_hold(tx: &mut Tx, id: Id<Hold>, ctx: &Ctx) -> Result<HoldView, ApiError> {
    let patron = require_patron(ctx)?;
    match tx.get(id)? {
        Some(h) if h.patron == patron => Ok(h.into()),
        _ => Err(ApiError::NotFound),
    }
}

/// How often the logical hour clock ticks (and expiry is swept).
pub const TICK_EVERY: Duration = Duration::from_secs(3600);

/// The app, in the shape `charpente::launch` and the sim both take.
pub fn build_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> io::Result<App> {
    Ok(App::open(
        disk,
        Schema::new()
            .table::<Branch>()
            .table::<Title>()
            .table::<Stock>()
            .table::<Hold>()
            .table::<Return>()
            .table::<OperatorSeat>()
            .table::<Patron>()
            .table::<queue::Counter>()
            .table::<queue::Clock>(),
        entropy,
        AppConfig::default(),
    )?
    .mount(routes![
        bootstrap_operator,
        mint_patron_key,
        put_branch,
        put_title,
        put_copies,
        record_return,
        waiting_queue,
        pick_up,
        place_hold,
        cancel_hold,
        my_holds,
        my_hold
    ])
    .mount_jobs(jobs![Tick, NotifyReady])
    .schedule("hour", TICK_EVERY, || Tick {})
    .outbound(Host::new(NOTIFY_HOST).bearer_env("NOTIFY_TOKEN")))
}
