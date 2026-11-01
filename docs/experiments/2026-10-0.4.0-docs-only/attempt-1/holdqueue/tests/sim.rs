//! The proof: properties of the hold queue under the full fault matrix
//! (GUIDE section 8).
//!
//! Every check drives the same drawn scenario through the production kernel
//! with `charpente_sim::drive`: an operator registers two branches and two
//! titles and records drawn copy counts; patrons place 2 to 10 drawn holds;
//! later, at drawn hours, the desk records pickups and returns, patrons
//! cancel, copies are restocked, the operator reads queues, and callers try
//! transitions they may not make. The faulted phase runs nine virtual days
//! under the run faults (disk, clock and outbound); a power cycle follows;
//! the fault-free tail re-sends what went unanswered and runs nine more days,
//! so every hold left ready expires and every notification settles.
//!
//! Each check then judges one behaviour from the durable end state, the
//! answers the clients saw and the messages the modelled service received.

use std::collections::BTreeMap;
use std::io;
use std::time::Duration;

use charpente::auth::ApiKey;
use charpente::core::io::WallTime;
use charpente::core::job::{JobRow, JobStatus};
use charpente::core::table::Codec;
use charpente::http::{ApiError, App, Method, Request};
use charpente::{Id, Snapshot, Tx, View};
use charpente_sim::Sim;
use charpente_sim::checks::durable_corruption_fired;
use charpente_sim::drive::{Drive, Recovered};
use charpente_sim::outbound::{Idempotent, Recorder};
use holdqueue::queue::Clock;
use holdqueue::{
    CopiesBody, Hold, OperatorSeat, Patron, HoldBody, HoldState, NOTIFY_HOST, NameBody, NotifyReady, Return, ReturnBody,
    Stock, build_app,
};
use sha2::{Digest, Sha256};

charpente_sim::sim_tests! {
    an_acked_hold_is_never_lost require ["restart", "judged:acked_hold"],
    copies_go_to_holds_in_placement_order require ["judged:order", "judged:queue_read"],
    copies_are_never_set_aside_twice_or_beyond_stock require ["judged:copies"],
    a_hold_is_ready_once_and_its_patron_is_told require ["judged:told"],
    refused_transitions_name_what_is_allowed require ["judged:refusal", "judged:intruder"],
    callers_reach_only_what_is_theirs require ["judged:bootstrap", "judged:minted", "judged:own_read", "judged:list"],
    repeats_are_answered_alike_and_conflicts_by_name require ["judged:repeat", "judged:conflict", "judged:copies_refused"],
    ready_holds_expire_after_seven_days require ["judged:expired"],
}

// ---- the world the scenario runs in -------------------------------------

const OPERATOR_TOKEN: &str = "operator-test-token";
const PATRONS: [u64; 3] = [1, 2, 3];
const BRANCHES: [(u64, &str); 2] = [(1, "Central"), (2, "Riverside")];
const TITLES: [(u64, &str); 2] = [(1, "Dune"), (2, "Emma")];
/// Seven days of hourly ticks, written out here rather than read from the
/// crate, so a change to the constant there is a change the checks see.
const SEVEN_DAYS_OF_HOURS: u64 = 168;
const PHASE: Duration = Duration::from_secs(9 * 24 * 3600);
const TAIL: Duration = Duration::from_secs(9 * 24 * 3600);

fn patron_token(patron: u64) -> String {
    format!("patron-{patron}-test-token")
}

fn branch_name(branch: u64) -> String {
    BRANCHES
        .iter()
        .find(|(id, _)| *id == branch)
        .map_or_else(|| "?".to_string(), |(_, n)| n.to_string())
}

fn title_name(title: u64) -> String {
    TITLES
        .iter()
        .find(|(id, _)| *id == title)
        .map_or_else(|| "?".to_string(), |(_, n)| n.to_string())
}

fn sha256_hex(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The credentials the scenario presents. The docs give no way to obtain a
/// first credential inside one drive (the faulted phase is one script, so a
/// token minted by a request cannot be read before the next request is
/// built), so the test writes `ApiKey` rows as GUIDE section 5 describes
/// them: the lowercase-hex SHA-256 of the raw key, and the subject.
fn seed_keys(app: &mut App) -> io::Result<()> {
    let mut keys: Vec<(u64, String, String)> =
        vec![(1, holdqueue::OPERATOR.to_string(), OPERATOR_TOKEN.to_string())];
    for patron in PATRONS {
        keys.push((10 + patron, format!("patron:{patron}"), patron_token(patron)));
    }
    app.transact_for_tests(|tx: &mut Tx| -> Result<(), ApiError> {
        for (id, subject, token) in &keys {
            let id = Id::<ApiKey>::from_raw(*id);
            if tx.get(id)?.is_none() {
                tx.put(&ApiKey {
                    id,
                    key_hash: sha256_hex(token),
                    subject: subject.clone(),
                    created_at: WallTime::UNIX_EPOCH,
                });
            }
        }
        Ok(())
    })
    .map_err(|e| io::Error::other(format!("{e:?}")))
}

/// The app on this run disk, with the scenario credentials. The drive calls
/// it once per boot.
fn app(sim: &Sim) -> io::Result<App> {
    let mut app = build_app(Box::new(sim.disk()), Box::new(sim.entropy()))?;
    seed_keys(&mut app)?;
    Ok(app)
}

// ---- the scenario --------------------------------------------------------

/// One request of the scenario, as the check reads it back.
#[derive(Clone, Debug)]
enum Op {
    Branch { id: u64, name: String },
    Title { id: u64, name: String },
    Copies { branch: u64, title: u64, copies: u32 },
    Place { hold: u64, patron: u64, title: u64, branch: u64 },
    /// A placement under an id another placement already holds, differing in
    /// exactly one of patron, title or branch.
    PlaceConflict { hold: u64, patron: u64, title: u64, branch: u64 },
    Pickup { hold: u64 },
    Cancel { hold: u64, patron: u64 },
    Return { id: u64, branch: u64, title: u64 },
    /// A return under an id another return holds, differing in one of
    /// branch or title.
    ReturnConflict { id: u64, branch: u64, title: u64 },
    Queue { branch: u64, title: u64 },
    /// A patron tries the operator-only pickup.
    IntruderPickup { hold: u64, patron: u64 },
    /// A patron tries to cancel the hold of another patron.
    IntruderCancel { hold: u64, patron: u64 },
    /// The one-time, unauthenticated operator bootstrap.
    Bootstrap,
    /// An operator mints a key for a new patron.
    MintKey { patron: u64 },
    /// A patron lists their holds.
    MyHolds { patron: u64 },
    /// A patron reads one hold, their own or not.
    MyHold { hold: u64, patron: u64 },
}

fn as_operator(request: Request) -> Request {
    request.header("authorization", format!("Bearer {OPERATOR_TOKEN}"))
}

fn as_patron(request: Request, patron: u64) -> Request {
    request.header("authorization", format!("Bearer {}", patron_token(patron)))
}

impl Op {
    fn request(&self) -> Request {
        match self {
            Op::Branch { id, name } => as_operator(
                Request::new(Method::Put, format!("/branches/{id}")).json(&NameBody { name: name.clone() }),
            ),
            Op::Title { id, name } => as_operator(
                Request::new(Method::Put, format!("/titles/{id}")).json(&NameBody { name: name.clone() }),
            ),
            Op::Copies { branch, title, copies } => as_operator(
                Request::new(Method::Put, format!("/branches/{branch}/titles/{title}/copies"))
                    .json(&CopiesBody { copies: *copies }),
            ),
            Op::Place { hold, patron, title, branch } | Op::PlaceConflict { hold, patron, title, branch } => as_patron(
                Request::new(Method::Post, format!("/holds/{hold}")).json(&HoldBody {
                    title: Id::from_raw(*title),
                    branch: Id::from_raw(*branch),
                }),
                *patron,
            ),
            Op::Pickup { hold } => as_operator(Request::new(Method::Post, format!("/holds/{hold}/pickup"))),
            Op::Cancel { hold, patron } | Op::IntruderCancel { hold, patron } => {
                as_patron(Request::new(Method::Post, format!("/holds/{hold}/cancel")), *patron)
            }
            Op::Return { id, branch, title } | Op::ReturnConflict { id, branch, title } => as_operator(
                Request::new(Method::Post, format!("/returns/{id}")).json(&ReturnBody {
                    branch: Id::from_raw(*branch),
                    title: Id::from_raw(*title),
                }),
            ),
            Op::Queue { branch, title } => as_operator(Request::new(
                Method::Get,
                format!("/branches/{branch}/titles/{title}/queue"),
            )),
            Op::Bootstrap => Request::new(Method::Post, "/operator"),
            Op::MintKey { patron } => as_operator(Request::new(Method::Post, format!("/patrons/{patron}/keys"))),
            Op::MyHolds { patron } => as_patron(Request::new(Method::Get, "/holds"), *patron),
            Op::MyHold { hold, patron } => as_patron(Request::new(Method::Get, format!("/holds/{hold}")), *patron),
            Op::IntruderPickup { hold, patron } => {
                as_patron(Request::new(Method::Post, format!("/holds/{hold}/pickup")), *patron)
            }
        }
    }
}

/// The next patron after `patron`, wrapping.
fn other_patron(patron: u64) -> u64 {
    PATRONS[(patron as usize) % PATRONS.len()]
}

fn hours(h: u64) -> Duration {
    Duration::from_secs(h * 3600)
}

/// Draws the scenario: what is sent, and when. Sorted into arrival order.
fn draw(sim: &Sim) -> Vec<(Duration, Op)> {
    let input = sim.input();
    let mut ops: Vec<(Duration, Op)> = Vec::new();
    for (i, (id, name)) in BRANCHES.iter().enumerate() {
        ops.push((Duration::from_millis(i as u64), Op::Branch { id: *id, name: name.to_string() }));
    }
    for (i, (id, name)) in TITLES.iter().enumerate() {
        ops.push((Duration::from_millis(10 + i as u64), Op::Title { id: *id, name: name.to_string() }));
    }
    ops.push((Duration::from_millis(2), Op::Bootstrap));
    ops.push((Duration::from_millis(3), Op::Bootstrap));
    let mut at_ms = 20;
    for (branch, _) in BRANCHES {
        for (title, _) in TITLES {
            let copies = input.between("copies", 0, 2) as u32;
            ops.push((Duration::from_millis(at_ms), Op::Copies { branch, title, copies }));
            at_ms += 1;
        }
    }
    let holds = input.between("holds", 2, 10);
    for hold in 1..=holds {
        let patron = *input.pick("patron", &PATRONS);
        let title = *input.pick("title", &[1u64, 2]);
        let branch = *input.pick("branch", &[1u64, 2]);
        let placed = Duration::from_secs(input.between("placed_second", 1, 120));
        ops.push((placed, Op::Place { hold, patron, title, branch }));
        let again = placed + Duration::from_secs(5);
        match *input.pick("second_send", &["none", "repeat", "conflict"]) {
            "repeat" => ops.push((again, Op::Place { hold, patron, title, branch })),
            "conflict" => {
                let (p, t, b) = match *input.pick("conflict_in", &["patron", "title", "branch"]) {
                    "patron" => (other_patron(patron), title, branch),
                    "title" => (patron, 3 - title, branch),
                    _ => (patron, title, 3 - branch),
                };
                ops.push((again, Op::PlaceConflict { hold, patron: p, title: t, branch: b }));
            }
            _ => {}
        }
        let read_at = placed + hours(input.between("read_hour", 0, 100)) + Duration::from_secs(90);
        match *input.pick("read", &["none", "own", "other"]) {
            "own" => ops.push((read_at, Op::MyHold { hold, patron })),
            "other" => ops.push((read_at, Op::MyHold { hold, patron: other_patron(patron) })),
            _ => {}
        }
        let action = *input.pick("action", &["wait", "pickup", "cancel", "pickup_and_return", "intrude"]);
        let act = placed + hours(input.between("act_hour", 0, 190)) + Duration::from_secs(30);
        let a_minute = Duration::from_secs(60);
        match action {
            "pickup" => {
                ops.push((act, Op::Pickup { hold }));
                ops.push((act + a_minute, Op::Pickup { hold }));
            }
            "cancel" => {
                ops.push((act, Op::Cancel { hold, patron }));
                ops.push((act + a_minute, Op::Cancel { hold, patron }));
            }
            "pickup_and_return" => {
                ops.push((act, Op::Pickup { hold }));
                let back = act + hours(1);
                ops.push((back, Op::Return { id: hold, branch, title }));
                ops.push((back + a_minute, Op::Return { id: hold, branch, title }));
                let (b, t) = if *input.pick("return_conflict_in", &["title", "branch"]) == "title" {
                    (branch, 3 - title)
                } else {
                    (3 - branch, title)
                };
                ops.push((back + a_minute * 2, Op::ReturnConflict { id: hold, branch: b, title: t }));
            }
            "intrude" => {
                ops.push((act, Op::IntruderPickup { hold, patron }));
                ops.push((act + Duration::from_secs(1), Op::IntruderCancel { hold, patron: other_patron(patron) }));
            }
            _ => {}
        }
    }
    let mint_for = *input.pick("mint_patron", &[4u64, 5]);
    ops.push((hours(input.between("mint_hour", 0, 100)) + Duration::from_secs(7), Op::MintKey { patron: mint_for }));
    let lister = *input.pick("list_patron", &PATRONS);
    ops.push((hours(input.between("list_hour", 0, 100)) + Duration::from_secs(11), Op::MyHolds { patron: lister }));
    let restocks = input.between("restocks", 0, 2);
    for _ in 0..restocks {
        let branch = *input.pick("restock_branch", &[1u64, 2]);
        let title = *input.pick("restock_title", &[1u64, 2]);
        let copies = input.between("restock_copies", 0, 3) as u32;
        let at = hours(input.between("restock_hour", 1, 200));
        ops.push((at, Op::Copies { branch, title, copies }));
    }
    for (branch, _) in BRANCHES {
        for (title, _) in TITLES {
            let at = hours(input.between("queue_hour", 0, 200)) + Duration::from_secs(150 * 60);
            ops.push((at, Op::Queue { branch, title }));
        }
    }
    ops.sort_by_key(|(at, _)| *at);
    ops
}

/// What one run produced: the scenario, the status each request got in the
/// faulted phase (the acks a client saw before the crash), the final answer
/// to each (the tail answer to a re-sent request, else the phase answer),
/// and the recovered node.
struct Outcome {
    ops: Vec<Op>,
    phase: Vec<Option<u16>>,
    answers: Vec<Option<(u16, String)>>,
    recovered: Recovered,
}

impl Outcome {
    fn snapshot(&self) -> Result<Snapshot, String> {
        self.recovered.kernel.app().db().snapshot().map_err(|e| e.to_string())
    }
}

/// Runs the scenario: faulted phase, power cycle, fault-free tail. `None`
/// means the run was given up by name (a durable refusal at boot).
fn run(sim: &mut Sim) -> Result<Option<Outcome>, String> {
    let scenario = draw(sim);
    sim.remote(NOTIFY_HOST, Idempotent::header("idempotency-key", Recorder::new()));
    let requests: Vec<(Duration, Request)> =
        scenario.iter().map(|(at, op)| (*at, op.request())).collect();
    let drive = Drive::new(sim, app);
    let phase = drive.faulted_phase(&requests, PHASE)?;
    let Some(recovered) = drive.recover(&phase, TAIL)? else {
        return Ok(None);
    };
    let n = scenario.len();
    let phase_status: Vec<Option<u16>> = (0..n).map(|i| phase.status(i)).collect();
    let answers: Vec<Option<(u16, String)>> = (0..n)
        .map(|i| {
            recovered.resent[i]
                .clone()
                .or_else(|| phase.answers[i].clone())
                .map(|(status, body)| (status, String::from_utf8_lossy(&body).into_owned()))
        })
        .collect();
    Ok(Some(Outcome {
        ops: scenario.into_iter().map(|(_, op)| op).collect(),
        phase: phase_status,
        answers,
        recovered,
    }))
}

/// A shortfall the platform excuses only where durable corruption fired:
/// then the run gives up (`Ok(true)`, return at once); anywhere else it is a
/// failure.
fn excused(sim: &Sim, failure: String) -> Result<bool, String> {
    if durable_corruption_fired(sim) {
        sim.give_up("durable corruption fired, so an acked record is excused", failure);
        return Ok(true);
    }
    Err(failure)
}

fn holds_of(snap: &Snapshot) -> Result<Vec<Hold>, String> {
    snap.scan::<Hold>().map_err(|e| e.to_string())
}

fn error_text(body: &str) -> String {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(str::to_string))
        .unwrap_or_else(|| body.to_string())
}

// ---- 1. an acknowledged hold is never lost -------------------------------

/// Every hold placement the faulted phase answered 200 is in the recovered
/// store with the patron, title and branch it was placed with; every pickup
/// or cancel it answered 200 stands (both end states are final).
fn an_acked_hold_is_never_lost(sim: &mut Sim) -> Result<(), String> {
    let Some(out) = run(sim)? else {
        return Ok(());
    };
    let snap = out.snapshot()?;
    let mut judged = 0;
    for (i, op) in out.ops.iter().enumerate() {
        if out.phase[i] != Some(200) {
            continue;
        }
        let (hold_id, want) = match op {
            Op::Place { hold, .. } => (*hold, None),
            Op::Pickup { hold } => (*hold, Some(HoldState::PickedUp)),
            Op::Cancel { hold, .. } => (*hold, Some(HoldState::Cancelled)),
            _ => continue,
        };
        let Some(hold) = snap.get::<Hold>(Id::from_raw(hold_id)).map_err(|e| e.to_string())? else {
            if excused(sim, format!("{op:?} was acked with 200 but hold {hold_id} is gone after recovery"))? {
                return Ok(());
            }
            continue;
        };
        if let Op::Place { patron, title, branch, .. } = op
            && (hold.patron.raw(), hold.title.raw(), hold.branch.raw()) != (*patron, *title, *branch)
        {
            return Err(format!(
                "acked {op:?} came back as patron {} title {} branch {}",
                hold.patron.raw(),
                hold.title.raw(),
                hold.branch.raw()
            ));
        }
        if let Some(state) = want
            && hold.state != state
        {
            if excused(sim, format!("{op:?} was acked with 200 but hold {hold_id} reads {:?}", hold.state))? {
                return Ok(());
            }
            continue;
        }
        judged += 1;
    }
    if judged == 0 {
        sim.give_up("no placement, pickup or cancel was acked, so durability has nothing to prove", "");
        return Ok(());
    }
    sim.cover("judged:acked_hold");
    Ok(())
}

// ---- 2. copies go to holds in placement order ----------------------------

/// From the sequence stamps every hold carries: when a hold became ready, no
/// hold placed earlier for the same title at the same branch was still
/// waiting (it had been served, or had ended, before). And at the end, no
/// requested hold waits on a slot with a free copy. And every queue the
/// operator read listed waiting holds of that slot only, earliest placed
/// first.
fn copies_go_to_holds_in_placement_order(sim: &mut Sim) -> Result<(), String> {
    let Some(out) = run(sim)? else {
        return Ok(());
    };
    let snap = out.snapshot()?;
    let holds = holds_of(&snap)?;
    let mut judged_order = false;
    for g in &holds {
        let Some(r) = g.ready_seq else { continue };
        if r <= g.placed_seq {
            return Err(format!("hold {} became ready (seq {r}) before it was placed (seq {})", g.id.raw(), g.placed_seq));
        }
        for h in holds.iter().filter(|h| h.slot == g.slot && h.placed_seq < g.placed_seq) {
            let served_before = h.ready_seq.is_some_and(|s| s < r);
            let ended_before = h.ended_seq.is_some_and(|s| s < r);
            if !served_before && !ended_before {
                return Err(format!(
                    "out of order: a copy went to hold {} (placed seq {}) at seq {r} while hold {} (placed seq {}) for the same title at the same branch was still waiting",
                    g.id.raw(), g.placed_seq, h.id.raw(), h.placed_seq
                ));
            }
            judged_order = true;
        }
    }
    for stock in snap.scan::<Stock>().map_err(|e| e.to_string())? {
        let free = i64::from(stock.copies) - i64::from(stock.set_aside) - i64::from(stock.on_loan);
        let waiter = holds
            .iter()
            .filter(|h| h.state == HoldState::Requested && (h.branch, h.title) == (stock.branch, stock.title))
            .min_by_key(|h| h.placed_seq);
        if let Some(waiter) = waiter
            && free > 0
        {
            return Err(format!(
                "branch {} has {free} free copies of title {} while hold {} waits for one",
                stock.branch.raw(), stock.title.raw(), waiter.id.raw()
            ));
        }
    }
    let placed: BTreeMap<u64, &Hold> = holds.iter().map(|h| (h.id.raw(), h)).collect();
    for (i, op) in out.ops.iter().enumerate() {
        let Op::Queue { branch, title } = op else { continue };
        let Some((200, body)) = &out.answers[i] else { continue };
        let listed: serde_json::Value = serde_json::from_str(body).map_err(|e| format!("queue body {body}: {e}"))?;
        let mut last = 0;
        for entry in listed["holds"].as_array().cloned().unwrap_or_default() {
            let id: u64 = entry["id"].as_str().and_then(|s| s.parse().ok()).ok_or(format!("queue entry {entry}"))?;
            let hold = placed.get(&id).ok_or(format!("queue of {branch}:{title} lists unknown hold {id}"))?;
            if (hold.branch.raw(), hold.title.raw()) != (*branch, *title) || entry["state"] != "requested" {
                return Err(format!("queue of branch {branch} title {title} lists {entry}"));
            }
            if hold.placed_seq <= last {
                return Err(format!("queue of branch {branch} title {title} is not in placement order: {body}"));
            }
            last = hold.placed_seq;
            sim.cover("judged:queue_read");
        }
    }
    if judged_order {
        sim.cover("judged:order");
    }
    Ok(())
}

// ---- 3. a copy is set aside for one hold, never beyond stock -------------

/// Per title per branch: the copies set aside equal the ready holds, the
/// copies out equal the pickups less the returns, and together they never
/// exceed the copies the branch holds.
fn copies_are_never_set_aside_twice_or_beyond_stock(sim: &mut Sim) -> Result<(), String> {
    let Some(out) = run(sim)? else {
        return Ok(());
    };
    let snap = out.snapshot()?;
    let holds = holds_of(&snap)?;
    let returns = snap.scan::<Return>().map_err(|e| e.to_string())?;
    let mut judged = false;
    for stock in snap.scan::<Stock>().map_err(|e| e.to_string())? {
        let here = |state: HoldState| holds.iter().filter(|h| h.slot == stock.slot && h.state == state).count() as u32;
        let ready = here(HoldState::Ready);
        let picked = here(HoldState::PickedUp);
        let returned = returns.iter().filter(|r| r.branch == stock.branch && r.title == stock.title).count() as u32;
        let slot = &stock.slot;
        if stock.set_aside != ready {
            return Err(format!("slot {slot}: {} copies set aside but {ready} holds are ready", stock.set_aside));
        }
        if stock.on_loan + returned != picked {
            return Err(format!("slot {slot}: {} copies out and {returned} returned but {picked} holds were picked up", stock.on_loan));
        }
        if stock.set_aside + stock.on_loan > stock.copies {
            return Err(format!(
                "slot {slot}: {} set aside and {} out, but the branch holds {} copies",
                stock.set_aside, stock.on_loan, stock.copies
            ));
        }
        if ready + picked > 0 {
            judged = true;
        }
    }
    for hold in holds.iter().filter(|h| h.ready_seq.is_some()) {
        if Stock::by_slot(&snap, &hold.slot).map_err(|e| e.to_string())?.is_empty() {
            return Err(format!("hold {} became ready at a branch with no stock of its title", hold.id.raw()));
        }
    }
    if judged {
        sim.cover("judged:copies");
    }
    Ok(())
}

// ---- 4. a hold becomes ready at most once, and its patron is told --------

/// Each hold that became ready has exactly one notification job, Done after
/// the tail, and reads notified; a hold that never became ready has none.
/// The modelled service received exactly one message per ready hold (it
/// dedupes on the idempotency key, as a real one would), naming the title
/// and the branch, and none for any other hold.
fn a_hold_is_ready_once_and_its_patron_is_told(sim: &mut Sim) -> Result<(), String> {
    let Some(out) = run(sim)? else {
        return Ok(());
    };
    let snap = out.snapshot()?;
    let holds = holds_of(&snap)?;
    let mut jobs: BTreeMap<u64, Vec<JobRow>> = BTreeMap::new();
    for job in snap.scan::<JobRow>().map_err(|e| e.to_string())? {
        if job.name != "NotifyReady" {
            continue;
        }
        let notify = NotifyReady::decode(&mut &job.payload[..]).map_err(|e| e.to_string())?;
        jobs.entry(notify.hold.raw()).or_default().push(job);
    }
    let bodies: Vec<Vec<u8>> = sim.with_remote(NOTIFY_HOST, |m: &Idempotent<Recorder>| {
        m.inner().calls().iter().map(|c| c.body.clone()).collect()
    });
    let mut messages: BTreeMap<u64, Vec<serde_json::Value>> = BTreeMap::new();
    for body in &bodies {
        let msg: serde_json::Value = serde_json::from_slice(body).map_err(|e| format!("message body: {e}"))?;
        let hold: u64 = msg["hold"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .ok_or(format!("message without a hold: {msg}"))?;
        messages.entry(hold).or_default().push(msg);
    }
    let mut told = 0;
    for hold in &holds {
        let id = hold.id.raw();
        let ready = hold.ready_seq.is_some();
        let its_jobs = jobs.get(&id).map_or(0, Vec::len);
        let its_messages = messages.get(&id).map_or(0, Vec::len);
        if its_jobs > 1 {
            return Err(format!("hold {id} became ready {its_jobs} times: it has {its_jobs} notification jobs"));
        }
        if ready != (its_jobs == 1) {
            return Err(format!("hold {id}: ready = {ready} but it has {its_jobs} notification jobs"));
        }
        if ready != hold.notified {
            return Err(format!("hold {id}: ready = {ready} but notified = {}", hold.notified));
        }
        if !ready {
            if its_messages > 0 && excused(sim, format!("hold {id} was never ready but its patron got {its_messages} messages"))? {
                return Ok(());
            }
            continue;
        }
        let job = &jobs[&id][0];
        if job.status != JobStatus::Done {
            return Err(format!("hold {id} is ready but its notification job is {:?}: {:?}", job.status, job.last_error));
        }
        if its_messages != 1 {
            return Err(format!("hold {id} became ready once but its patron got {its_messages} messages"));
        }
        let msg = &messages[&id][0];
        let (title, branch) = (title_name(hold.title.raw()), branch_name(hold.branch.raw()));
        let patron = hold.patron.raw().to_string();
        if msg["title"] != title.as_str() || msg["branch"] != branch.as_str() || msg["patron"] != patron.as_str() {
            return Err(format!("hold {id} (title {title}, branch {branch}, patron {patron}) was told {msg}"));
        }
        let text = msg["text"].as_str().unwrap_or_default();
        if !text.contains(&title) || !text.contains(&branch) {
            return Err(format!("hold {id}: the message text {text:?} does not name {title} and {branch}"));
        }
        told += 1;
    }
    if let Some(stray) = messages.keys().find(|h| !holds.iter().any(|x| x.id.raw() == **h))
        && excused(sim, format!("a message names hold {stray}, which the store does not hold"))?
    {
        return Ok(());
    }
    if told > 0 {
        sim.cover("judged:told");
    }
    Ok(())
}

// ---- 5. refused transitions name what is allowed -------------------------

/// The refusal text for each state, written out independently of the app.
fn expected_refusal(id: u64, verb: &str, state: &str) -> String {
    let allowed = match state {
        "requested" => "become ready (when a copy is set aside for it), cancel (by its patron)",
        "ready for pickup" => {
            "pick up (by an operator), cancel (by its patron), expire (after 7 days uncollected)"
        }
        _ => "none, because this state is final",
    };
    format!(
        "transition refused: cannot {verb} hold {id}, which is {state}; from {state} the allowed transitions are: {allowed}"
    )
}

const STATE_NAMES: [&str; 5] = ["requested", "ready for pickup", "picked up", "expired", "cancelled"];

/// A pickup or cancel the state machine does not allow is refused with a 400
/// that names the transition, the state, and the transitions allowed from
/// it; one it allows is answered 200 and its end state stands. A patron
/// cannot pick up (operators only) nor cancel the hold of another patron,
/// and no hold ends picked up or cancelled without a request that may do so.
fn refused_transitions_name_what_is_allowed(sim: &mut Sim) -> Result<(), String> {
    let Some(out) = run(sim)? else {
        return Ok(());
    };
    let snap = out.snapshot()?;
    let holds = holds_of(&snap)?;
    let by_id: BTreeMap<u64, &Hold> = holds.iter().map(|h| (h.id.raw(), h)).collect();
    let final_state = |id: u64| by_id.get(&id).map(|h| h.state);
    let mut refusals = 0;
    for (i, op) in out.ops.iter().enumerate() {
        let Some((status, body)) = &out.answers[i] else { continue };
        let (hold, verb, refusable): (u64, &str, &[&str]) = match op {
            Op::Pickup { hold } => (*hold, "pick up", &["requested", "expired", "cancelled"]),
            Op::Cancel { hold, .. } => (*hold, "cancel", &["picked up", "expired"]),
            Op::IntruderPickup { .. } => {
                if *status != 400 || !error_text(body).starts_with("operators only") {
                    return Err(format!("{op:?} by a patron was answered {status} {body}"));
                }
                sim.cover("judged:intruder");
                continue;
            }
            Op::IntruderCancel { .. } => {
                if *status != 404 {
                    return Err(format!("{op:?} of the hold of another patron was answered {status} {body}"));
                }
                sim.cover("judged:intruder");
                continue;
            }
            _ => continue,
        };
        match *status {
            200 => {
                let want = if verb == "pick up" { HoldState::PickedUp } else { HoldState::Cancelled };
                // Media corruption can cut the acked record and revert the hold.
                if final_state(hold) != Some(want)
                    && excused(sim, format!("{op:?} was answered 200 but hold {hold} ends {:?}", final_state(hold)))?
                {
                    return Ok(());
                }
            }
            400 => {
                let text = error_text(body);
                let Some(state) = STATE_NAMES.iter().find(|s| text == expected_refusal(hold, verb, s)) else {
                    return Err(format!("{op:?} was refused with {text:?}, which is not the refusal of any state"));
                };
                if !refusable.contains(state) {
                    return Err(format!("{op:?} was refused from {state:?}, a state that allows it: {text}"));
                }
                let terminal = matches!(*state, "picked up" | "expired" | "cancelled");
                if terminal
                    && final_state(hold).map(|s| s.name().to_string()).as_deref() != Some(*state)
                    && excused(sim, format!("{op:?} was refused as {state} but hold {hold} ends {:?}", final_state(hold)))?
                {
                    return Ok(());
                }
                refusals += 1;
            }
            404 if final_state(hold).is_none() => {}
            _ => return Err(format!("{op:?} was answered {status} {body}")),
        }
    }
    for hold in &holds {
        let id = hold.id.raw();
        let owner = hold.patron.raw();
        let picked = out.ops.iter().any(|op| matches!(op, Op::Pickup { hold } if *hold == id));
        let cancelled = out
            .ops
            .iter()
            .any(|op| matches!(op, Op::Cancel { hold, patron } if *hold == id && *patron == owner));
        if hold.state == HoldState::PickedUp && !picked {
            return Err(format!("hold {id} ends picked up, but no operator recorded its pickup"));
        }
        if hold.state == HoldState::Cancelled && !cancelled {
            return Err(format!("hold {id} ends cancelled, but its own patron never cancelled it"));
        }
    }
    if refusals > 0 {
        sim.cover("judged:refusal");
    }
    Ok(())
}

// ---- 6. a ready hold expires after seven days uncollected ----------------

/// On the logical hour clock: an expired hold expired at the first tick past
/// seven days after it became ready, never sooner; a hold still ready at the
/// end has not yet waited seven days; a hold picked up was picked up within
/// its seven days. The tail runs nine days, so holds do expire.
fn ready_holds_expire_after_seven_days(sim: &mut Sim) -> Result<(), String> {
    let Some(out) = run(sim)? else {
        return Ok(());
    };
    let snap = out.snapshot()?;
    let now = snap
        .get::<Clock>(Id::from_raw(0))
        .map_err(|e| e.to_string())?
        .map_or(0, |c| c.hour);
    let mut expired = 0;
    for hold in holds_of(&snap)? {
        let id = hold.id.raw();
        let ready_hour = hold.ready_hour;
        match hold.state {
            HoldState::Expired => {
                let (Some(r), Some(e)) = (ready_hour, hold.ended_hour) else {
                    return Err(format!("hold {id} expired without having been ready"));
                };
                let due = r + SEVEN_DAYS_OF_HOURS + 1;
                if e != due {
                    return Err(format!(
                        "hold {id} became ready at hour {r} and expired at hour {e}, not at the first tick past seven days ({due})"
                    ));
                }
                expired += 1;
            }
            HoldState::Ready => {
                let r = ready_hour.ok_or(format!("hold {id} is ready with no ready hour"))?;
                if now > r + SEVEN_DAYS_OF_HOURS {
                    return Err(format!(
                        "hold {id} became ready at hour {r} and is still ready at hour {now}, past seven days"
                    ));
                }
            }
            HoldState::PickedUp => {
                let (Some(r), Some(e)) = (ready_hour, hold.ended_hour) else {
                    return Err(format!("hold {id} was picked up without having been ready"));
                };
                if e > r + SEVEN_DAYS_OF_HOURS {
                    return Err(format!(
                        "hold {id} became ready at hour {r} and was picked up at hour {e}, after it should have expired"
                    ));
                }
            }
            HoldState::Requested | HoldState::Cancelled => {}
        }
    }
    if expired > 0 {
        sim.cover("judged:expired");
    }
    Ok(())
}

// ---- 7. repeats are answered alike, conflicts and refusals by name --------

fn numbers_in(text: &str) -> Vec<u64> {
    text.split(|c: char| !c.is_ascii_digit())
        .filter(|w| !w.is_empty())
        .filter_map(|w| w.parse().ok())
        .collect()
}

/// A request sent twice is answered alike the second time and changes
/// nothing; a request that reuses an id with other content is refused as a
/// conflict, by name. A copy count below the copies set aside and out is
/// refused naming both and the least count allowed; any other count is
/// recorded as sent. A placement is answered with the hold it placed.
fn repeats_are_answered_alike_and_conflicts_by_name(sim: &mut Sim) -> Result<(), String> {
    let Some(out) = run(sim)? else {
        return Ok(());
    };
    let answered_200 = |want: &dyn Fn(&Op) -> bool| {
        out.ops
            .iter()
            .zip(&out.answers)
            .any(|(op, a)| want(op) && matches!(a, Some((200, _))))
    };
    let mut seen_place: BTreeMap<u64, u32> = BTreeMap::new();
    let mut seen_return: BTreeMap<u64, u32> = BTreeMap::new();
    for (i, op) in out.ops.iter().enumerate() {
        let Some((status, body)) = &out.answers[i] else { continue };
        let text = error_text(body);
        match op {
            Op::Place { hold, patron, title, branch } => {
                let n = seen_place.entry(*hold).or_insert(0);
                *n += 1;
                if *status != 200 {
                    // The setup it rests on was acked; only media corruption cuts it.
                    if text.starts_with("no branch") || text.starts_with("no title") {
                        if excused(sim, format!("{op:?} was answered {status} {text}"))? {
                            return Ok(());
                        }
                        continue;
                    }
                    return Err(format!("{op:?} was answered {status} {body}"));
                }
                let view: serde_json::Value = serde_json::from_str(body).map_err(|e| format!("{body}: {e}"))?;
                let want = [hold.to_string(), patron.to_string(), title.to_string(), branch.to_string()];
                let got = ["id", "patron", "title", "branch"].map(|k| view[k].as_str().unwrap_or_default().to_string());
                if got != want {
                    return Err(format!("{op:?} was answered with {body}"));
                }
                if *n > 1 {
                    sim.cover("judged:repeat");
                }
            }
            Op::PlaceConflict { hold, .. } => {
                let want = format!(
                    "hold {hold} already names another patron, title or branch: a hold id names one hold; place the new hold under a new id"
                );
                if *status == 200 {
                    if excused(sim, format!("{op:?} reused the id of a stored hold and was answered 200 {body}"))? {
                        return Ok(());
                    }
                    continue;
                }
                if *status != 400 || text != want {
                    return Err(format!("{op:?} was answered {status} {text:?}, not refused as a conflict"));
                }
                sim.cover("judged:conflict");
            }
            Op::Return { id, .. } => {
                let n = seen_return.entry(*id).or_insert(0);
                *n += 1;
                if text.contains("already names another") {
                    return Err(format!("{op:?}, a repeat of the same return, was refused as a conflict: {text}"));
                }
                if *status == 200 && *n > 1 {
                    sim.cover("judged:repeat");
                }
            }
            Op::ReturnConflict { id, .. } => {
                let original_stored = answered_200(&|o| matches!(o, Op::Return { id: r, .. } if r == id));
                let want = format!("return {id} already names another branch or title: record the new return under a new id");
                if original_stored && (*status != 400 || text != want) {
                    if *status == 200
                        && excused(sim, format!("{op:?} reused the id of a stored return and was answered 200"))?
                    {
                        return Ok(());
                    }
                    if *status != 200 {
                        return Err(format!("{op:?} was answered {status} {text:?}, not refused as a conflict"));
                    }
                    continue;
                }
                if *status == 400 && text == want {
                    sim.cover("judged:conflict");
                }
            }
            Op::Copies { branch, title, copies } => match *status {
                200 => {
                    let view: serde_json::Value = serde_json::from_str(body).map_err(|e| format!("{body}: {e}"))?;
                    let field = |k: &str| view[k].as_u64().unwrap_or(u64::MAX);
                    if field("copies") != u64::from(*copies) || field("set_aside") + field("on_loan") > field("copies") {
                        return Err(format!("{op:?} was recorded as {body}"));
                    }
                }
                400 if text.starts_with("no branch") || text.starts_with("no title") => {
                    if excused(sim, format!("{op:?} was answered {status} {text}"))? {
                        return Ok(());
                    }
                }
                400 => {
                    let nums = numbers_in(&text);
                    let [b, n, t, set_aside, out_with, least] = nums[..] else {
                        return Err(format!("{op:?} was refused with {text:?}"));
                    };
                    let want = format!(
                        "branch {b} cannot hold {n} copies of title {t}: {set_aside} are set aside for ready holds and {out_with} are out with patrons; record a count of at least {least}"
                    );
                    if text != want || (b, n, t) != (*branch, u64::from(*copies), *title) {
                        return Err(format!("{op:?} was refused with {text:?}"));
                    }
                    if least != set_aside + out_with || n >= least {
                        return Err(format!("{op:?} was refused although {n} copies cover the {set_aside} set aside and {out_with} out: {text}"));
                    }
                    sim.cover("judged:copies_refused");
                }
                _ => return Err(format!("{op:?} was answered {status} {body}")),
            },
            _ => {}
        }
    }
    Ok(())
}

// ---- 8. callers reach only what is theirs ---------------------------------

const ALREADY_MINTED: &str = "the operator key was already minted: ask an operator to mint you a key";

fn is_raw_key(body: &str) -> bool {
    serde_json::from_str::<String>(body)
        .is_ok_and(|k| k.len() == 67 && k.starts_with("ck_") && k[3..].bytes().all(|b| b.is_ascii_hexdigit()))
}

/// The unauthenticated bootstrap mints one operator key, ever, and refuses
/// by name after; an operator mints a patron key for that patron; a patron
/// lists only their own holds, reads their own hold, and reads the hold of
/// another patron as absent.
fn callers_reach_only_what_is_theirs(sim: &mut Sim) -> Result<(), String> {
    let Some(out) = run(sim)? else {
        return Ok(());
    };
    let snap = out.snapshot()?;
    let holds = holds_of(&snap)?;
    let keys = snap.scan::<ApiKey>().map_err(|e| e.to_string())?;
    let owner = |id: u64| holds.iter().find(|h| h.id.raw() == id).map(|h| h.patron.raw());
    let mut bootstrapped = 0;
    for (i, op) in out.ops.iter().enumerate() {
        let Some((status, body)) = &out.answers[i] else { continue };
        let text = error_text(body);
        match op {
            Op::Bootstrap => match *status {
                200 if is_raw_key(body) => bootstrapped += 1,
                400 if text == ALREADY_MINTED => {}
                _ => return Err(format!("the bootstrap was answered {status} {body}")),
            },
            Op::MintKey { patron } => {
                if *status != 200 || !is_raw_key(body) {
                    return Err(format!("{op:?} was answered {status} {body}"));
                }
                let subject = format!("patron:{patron}");
                let stored = keys.iter().any(|k| k.subject == subject)
                    && snap.get::<Patron>(Id::from_raw(*patron)).map_err(|e| e.to_string())?.is_some();
                if !stored {
                    if excused(sim, format!("{op:?} was acked, but no key for {subject} or no patron row is stored"))? {
                        return Ok(());
                    }
                    continue;
                }
                sim.cover("judged:minted");
            }
            Op::MyHolds { patron } => {
                if *status != 200 {
                    return Err(format!("{op:?} was answered {status} {body}"));
                }
                let listed: serde_json::Value = serde_json::from_str(body).map_err(|e| format!("{body}: {e}"))?;
                for entry in listed["holds"].as_array().cloned().unwrap_or_default() {
                    let id: u64 = entry["id"].as_str().and_then(|s| s.parse().ok()).ok_or(format!("entry {entry}"))?;
                    let state_ok = entry["state"].as_str().is_some_and(|s| STATE_NAMES.contains(&s));
                    if entry["patron"] != patron.to_string().as_str() || owner(id) != Some(*patron) || !state_ok {
                        return Err(format!("patron {patron} listed {entry}, which is not theirs"));
                    }
                    sim.cover("judged:list");
                }
            }
            Op::MyHold { hold, patron } => {
                let own = owner(*hold) == Some(*patron);
                match *status {
                    200 if own => {
                        let view: serde_json::Value = serde_json::from_str(body).map_err(|e| format!("{body}: {e}"))?;
                        if view["id"] != hold.to_string().as_str() || view["patron"] != patron.to_string().as_str() {
                            return Err(format!("{op:?} was answered {body}"));
                        }
                        sim.cover("judged:own_read");
                    }
                    404 if !own => sim.cover("judged:own_read"),
                    404 => {
                        let acked_before = out.ops[..i]
                            .iter()
                            .zip(&out.answers[..i])
                            .any(|(o, a)| matches!(o, Op::Place { hold: h, .. } if h == hold) && matches!(a, Some((200, _))));
                        if acked_before && excused(sim, format!("{op:?}: the owner read an acked hold as absent"))? {
                            return Ok(());
                        }
                    }
                    _ => return Err(format!("{op:?} (owner {:?}) was answered {status} {body}", owner(*hold))),
                }
            }
            _ => {}
        }
    }
    let operator_keys = keys.iter().filter(|k| k.subject == holdqueue::OPERATOR).count();
    let seat = snap.get::<OperatorSeat>(Id::from_raw(0)).map_err(|e| e.to_string())?.is_some();
    if bootstrapped > 1 {
        return Err(format!("the bootstrap minted {bootstrapped} operator keys"));
    }
    if operator_keys != 1 + usize::from(seat) || (bootstrapped == 1 && !seat) {
        if excused(sim, format!("{operator_keys} operator keys, seat taken = {seat}, {bootstrapped} bootstrap acked"))? {
            return Ok(());
        }
        return Ok(());
    }
    if bootstrapped == 1 {
        sim.cover("judged:bootstrap");
    }
    Ok(())
}
