//! The hold queue: the state machine, the allocation of free copies to
//! waiting holds in placement order, and the hourly tick that expires holds
//! left uncollected for seven days.
//!
//! Every function here runs inside the transaction of the route or job that
//! calls it, so a state change, the copy accounting it implies, the holds it
//! makes ready and their notification jobs commit as one record.

use crate::notify::NotifyReady;
use crate::{Branch, Hold, Patron, Return, Stock, Title};
use charpente::core::table::Codec;
use charpente::http::ApiError;
use charpente::kernel::JobError;
use charpente::{Id, Tx, View, job, table};
use std::collections::BTreeSet;
use std::io;

/// How long a ready hold waits for its patron, in ticks of the logical hour
/// clock: seven days. A hold made ready at hour `h` expires at the first tick
/// past `h + READY_HOURS`, so it has been ready for at least seven days.
pub const READY_HOURS: u64 = 7 * 24;

/// Where a hold is in its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HoldState {
    /// Placed, waiting for a copy.
    Requested,
    /// A copy at the pickup branch is set aside for it.
    Ready,
    /// The patron collected the copy.
    PickedUp,
    /// Ready for seven days and nobody collected it.
    Expired,
    /// The patron withdrew it before picking it up.
    Cancelled,
}

impl HoldState {
    /// The name a patron or operator reads.
    pub fn name(&self) -> &str {
        match self {
            HoldState::Requested => "requested",
            HoldState::Ready => "ready for pickup",
            HoldState::PickedUp => "picked up",
            HoldState::Expired => "expired",
            HoldState::Cancelled => "cancelled",
        }
    }

    /// The transitions allowed from this state, as a refusal names them.
    pub fn allowed(&self) -> &str {
        match self {
            HoldState::Requested => {
                "become ready (when a copy is set aside for it), cancel (by its patron)"
            }
            HoldState::Ready => {
                "pick up (by an operator), cancel (by its patron), expire (after 7 days uncollected)"
            }
            HoldState::PickedUp | HoldState::Expired | HoldState::Cancelled => {
                "none, because this state is final"
            }
        }
    }
}

impl Codec for HoldState {
    fn encode(&self, out: &mut Vec<u8>) {
        let tag: u8 = match self {
            HoldState::Requested => 0,
            HoldState::Ready => 1,
            HoldState::PickedUp => 2,
            HoldState::Expired => 3,
            HoldState::Cancelled => 4,
        };
        tag.encode(out);
    }

    fn decode(input: &mut &[u8]) -> io::Result<Self> {
        match u8::decode(input)? {
            0 => Ok(HoldState::Requested),
            1 => Ok(HoldState::Ready),
            2 => Ok(HoldState::PickedUp),
            3 => Ok(HoldState::Expired),
            4 => Ok(HoldState::Cancelled),
            other => Err(io::Error::other(format!(
                "invalid HoldState tag {other}: schema drift or corruption"
            ))),
        }
    }
}

/// Counter rows, one per id space (GUIDE section 2, "Generating ids").
#[table(global)]
pub struct Counter {
    pub id: Id<Counter>,
    pub next: u64,
}

/// The id space of `Stock` rows.
pub const STOCK_IDS: u64 = 0;
/// The app-wide sequence that stamps placements, readiness and endings.
pub const EVENT_SEQ: u64 = 1;

/// Draws the next value of counter `which`, starting at 1.
pub fn next(tx: &mut Tx, which: u64) -> io::Result<u64> {
    let id = Id::<Counter>::from_raw(which);
    let mut counter = tx.get(id)?.unwrap_or(Counter { id, next: 1 });
    let value = counter.next;
    counter.next += 1;
    tx.put(&counter);
    Ok(value)
}

/// The logical hour clock: a singleton row only `Tick` advances (GUIDE
/// section 7, "A logical clock").
#[table(global)]
pub struct Clock {
    pub id: Id<Clock>,
    pub hour: u64,
}

/// The current reading of the logical hour clock (0 before the first tick).
pub fn hour(view: &impl View) -> io::Result<u64> {
    Ok(view.get(Id::<Clock>::from_raw(0))?.map_or(0, |c| c.hour))
}

/// True once a hold made ready at `ready_hour` has waited seven days at
/// `now_hour`.
pub fn overdue(ready_hour: u64, now_hour: u64) -> bool {
    now_hour > ready_hour + READY_HOURS
}

/// The index key of one title at one branch.
pub fn slot_key(branch: Id<Branch>, title: Id<Title>) -> String {
    format!("{}:{}", branch.raw(), title.raw())
}

/// The stock row of `title` at `branch`, if the operator recorded one.
pub fn find_stock(view: &impl View, branch: Id<Branch>, title: Id<Title>) -> io::Result<Option<Stock>> {
    Ok(Stock::by_slot(view, &slot_key(branch, title))?.into_iter().next())
}

/// The requested holds on `title` at `branch`, earliest placed first: the
/// order copies go to them.
pub fn waiting(view: &impl View, branch: Id<Branch>, title: Id<Title>) -> io::Result<Vec<Hold>> {
    let mut holds: Vec<Hold> = Hold::by_slot(view, &slot_key(branch, title))?
        .into_iter()
        .filter(|h| h.state == HoldState::Requested)
        .collect();
    holds.sort_by_key(|h| h.placed_seq);
    Ok(holds)
}

/// Gives every free copy of `title` at `branch` to the earliest-placed
/// requested holds, one copy each, and enqueues each such hold its
/// notification in the same transaction. Returns the stock as it now stands.
pub fn serve(tx: &mut Tx, branch: Id<Branch>, title: Id<Title>) -> io::Result<Option<Stock>> {
    let Some(mut stock) = find_stock(&*tx, branch, title)? else {
        return Ok(None);
    };
    let now = hour(&*tx)?;
    for mut hold in waiting(&*tx, branch, title)? {
        if stock.free() == 0 {
            break;
        }
        hold.state = HoldState::Ready;
        hold.ready_seq = Some(next(tx, EVENT_SEQ)?);
        hold.ready_hour = Some(now);
        stock.set_aside += 1;
        tx.put(&hold);
        tx.enqueue(NotifyReady { hold: hold.id });
    }
    tx.put(&stock);
    Ok(Some(stock))
}

fn no_branch(branch: Id<Branch>) -> ApiError {
    ApiError::BadRequest(format!(
        "no branch {0}: an operator registers it first with PUT /branches/{0}",
        branch.raw()
    ))
}

fn no_title(title: Id<Title>) -> ApiError {
    ApiError::BadRequest(format!(
        "no title {0}: an operator registers it first with PUT /titles/{0}",
        title.raw()
    ))
}

/// A refusal of a transition the state machine does not allow, by name, with
/// the transitions that are allowed from the state the hold is in.
pub fn refuse(id: Id<Hold>, verb: &str, state: HoldState) -> ApiError {
    ApiError::BadRequest(format!(
        "transition refused: cannot {verb} hold {}, which is {}; from {} the allowed transitions are: {}",
        id.raw(),
        state.name(),
        state.name(),
        state.allowed()
    ))
}

/// Records that `branch` holds `copies` copies of `title`. A count below the
/// copies already set aside or out is refused. New free copies are served.
pub fn set_copies(tx: &mut Tx, branch: Id<Branch>, title: Id<Title>, copies: u32) -> Result<Stock, ApiError> {
    if tx.get(branch)?.is_none() {
        return Err(no_branch(branch));
    }
    if tx.get(title)?.is_none() {
        return Err(no_title(title));
    }
    let mut stock = match find_stock(&*tx, branch, title)? {
        Some(stock) => stock,
        None => Stock {
            id: Id::from_raw(next(tx, STOCK_IDS)?),
            slot: slot_key(branch, title),
            branch,
            title,
            copies: 0,
            set_aside: 0,
            on_loan: 0,
        },
    };
    if copies < stock.set_aside + stock.on_loan {
        return Err(ApiError::BadRequest(format!(
            "branch {} cannot hold {copies} copies of title {}: {} are set aside for ready holds and {} are out with patrons; record a count of at least {}",
            branch.raw(),
            title.raw(),
            stock.set_aside,
            stock.on_loan,
            stock.set_aside + stock.on_loan
        )));
    }
    stock.copies = copies;
    tx.put(&stock);
    Ok(serve(tx, branch, title)?.unwrap_or(stock))
}

/// Places hold `id` for `patron` on `title` at `branch`. A repeat of the same
/// placement answers the hold as it stands; another placement under the same
/// id is refused as a conflict.
pub fn place(
    tx: &mut Tx,
    id: Id<Hold>,
    patron: Id<Patron>,
    title: Id<Title>,
    branch: Id<Branch>,
) -> Result<Hold, ApiError> {
    if let Some(hold) = tx.get(id)? {
        if hold.patron == patron && hold.title == title && hold.branch == branch {
            return Ok(hold);
        }
        return Err(ApiError::BadRequest(format!(
            "hold {} already names another patron, title or branch: a hold id names one hold; place the new hold under a new id",
            id.raw()
        )));
    }
    if tx.get(branch)?.is_none() {
        return Err(no_branch(branch));
    }
    if tx.get(title)?.is_none() {
        return Err(no_title(title));
    }
    let placed_seq = next(tx, EVENT_SEQ)?;
    tx.put(&Hold {
        id,
        patron,
        title,
        branch,
        slot: slot_key(branch, title),
        state: HoldState::Requested,
        placed_seq,
        ready_seq: None,
        ready_hour: None,
        ended_seq: None,
        ended_hour: None,
        notified: false,
    });
    serve(tx, branch, title)?;
    tx.get(id)?
        .ok_or_else(|| ApiError::Internal(format!("hold {} vanished inside its own transaction", id.raw())))
}

/// Ends `hold` in `state`, stamping when.
fn end(tx: &mut Tx, hold: &mut Hold, state: HoldState) -> io::Result<()> {
    hold.state = state;
    hold.ended_seq = Some(next(tx, EVENT_SEQ)?);
    hold.ended_hour = Some(hour(&*tx)?);
    tx.put(&*hold);
    Ok(())
}

/// The stock a ready or picked-up hold draws on. It always exists: a hold
/// becomes ready only from a stock row.
fn stock_of(tx: &Tx, hold: &Hold) -> Result<Stock, ApiError> {
    find_stock(tx, hold.branch, hold.title)?.ok_or_else(|| {
        ApiError::Internal(format!(
            "hold {} drew a copy, but branch {} has no stock of title {}",
            hold.id.raw(),
            hold.branch.raw(),
            hold.title.raw()
        ))
    })
}

/// The patron collected the copy set aside for hold `id`. A repeat of a
/// pickup answers the hold as it stands.
pub fn pick_up(tx: &mut Tx, id: Id<Hold>) -> Result<Hold, ApiError> {
    let mut hold = tx.get(id)?.ok_or(ApiError::NotFound)?;
    match hold.state {
        HoldState::Ready => {
            let mut stock = stock_of(tx, &hold)?;
            stock.set_aside -= 1;
            stock.on_loan += 1;
            tx.put(&stock);
            end(tx, &mut hold, HoldState::PickedUp)?;
            Ok(hold)
        }
        HoldState::PickedUp => Ok(hold),
        other => Err(refuse(id, "pick up", other)),
    }
}

/// `patron` withdraws hold `id`. A ready hold gives its copy to the next
/// hold in line. A repeat of a cancel answers the hold as it stands; the hold
/// of another patron reads as absent.
pub fn cancel(tx: &mut Tx, id: Id<Hold>, patron: Id<Patron>) -> Result<Hold, ApiError> {
    let mut hold = match tx.get(id)? {
        Some(hold) if hold.patron == patron => hold,
        _ => return Err(ApiError::NotFound),
    };
    match hold.state {
        HoldState::Requested => {
            end(tx, &mut hold, HoldState::Cancelled)?;
            Ok(hold)
        }
        HoldState::Ready => {
            let mut stock = stock_of(tx, &hold)?;
            stock.set_aside -= 1;
            tx.put(&stock);
            end(tx, &mut hold, HoldState::Cancelled)?;
            serve(tx, hold.branch, hold.title)?;
            Ok(hold)
        }
        HoldState::Cancelled => Ok(hold),
        other => Err(refuse(id, "cancel", other)),
    }
}

/// A copy of `title` came back to `branch`: it is free again and goes to the
/// next hold in line. Recorded once under `id`.
pub fn record_return(tx: &mut Tx, id: Id<Return>, branch: Id<Branch>, title: Id<Title>) -> Result<Stock, ApiError> {
    if let Some(done) = tx.get(id)? {
        if done.branch == branch && done.title == title {
            return find_stock(&*tx, branch, title)?
                .ok_or_else(|| ApiError::Internal(format!("return {} has no stock", id.raw())));
        }
        return Err(ApiError::BadRequest(format!(
            "return {} already names another branch or title: record the new return under a new id",
            id.raw()
        )));
    }
    let mut stock = find_stock(&*tx, branch, title)?.ok_or_else(|| {
        ApiError::BadRequest(format!(
            "branch {} holds no copies of title {}: a copy is returned to the branch it was picked up from",
            branch.raw(),
            title.raw()
        ))
    })?;
    if stock.on_loan == 0 {
        return Err(ApiError::BadRequest(format!(
            "no copy of title {} is out from branch {}: a copy is returned to the branch it was picked up from",
            title.raw(),
            branch.raw()
        )));
    }
    stock.on_loan -= 1;
    tx.put(&stock);
    tx.put(&Return { id, branch, title });
    Ok(serve(tx, branch, title)?.unwrap_or(stock))
}

/// One tick of the logical hour clock: advance it, expire every ready hold
/// that has waited seven days, and serve the copies that frees.
pub fn tick(tx: &mut Tx) -> io::Result<()> {
    let id = Id::<Clock>::from_raw(0);
    let mut clock = tx.get(id)?.unwrap_or(Clock { id, hour: 0 });
    clock.hour += 1;
    tx.put(&clock);
    let mut freed: BTreeSet<(u64, u64)> = BTreeSet::new();
    for mut hold in Hold::by_state(&*tx, &HoldState::Ready)? {
        let Some(ready_hour) = hold.ready_hour else {
            continue;
        };
        if !overdue(ready_hour, clock.hour) {
            continue;
        }
        if let Some(mut stock) = find_stock(&*tx, hold.branch, hold.title)? {
            stock.set_aside -= 1;
            tx.put(&stock);
        }
        end(tx, &mut hold, HoldState::Expired)?;
        freed.insert((hold.branch.raw(), hold.title.raw()));
    }
    for (branch, title) in freed {
        serve(tx, Id::from_raw(branch), Id::from_raw(title))?;
    }
    Ok(())
}

/// The cron job behind the logical hour clock.
#[job(global)]
pub struct Tick {}

impl Tick {
    pub fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        tick(tx)?;
        Ok(())
    }
}
