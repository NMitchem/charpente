//! Replication data-plane properties (M1 sub-project 2, task 6): ship, ack,
//! seed, diverge — proven at the `Db` level over the fault matrix, before the
//! kernel arm (task 8) wires it in. The `Node`/`spawn_node`/`run_until`
//! harness here is shared with tasks 7 and 8, so it stays general.
//!
//! Fault discipline: every `io::Error` is classified through
//! `charpente_sim::checks::is_fault_fallout` — fallout restarts/reconnects and
//! the workload continues; anything else fails the universe for adjudication.
//! A poisoned `Db` is recovered by reopening the node (expected fallout, not a
//! bug).

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use charpente::repl::Repl;
use charpente::repl::protocol::{Msg, decode, encode};
use charpente::repl::role::Role;
use charpente::repl::witnesses::{load_witnesses, store_witnesses};
use charpente::repl::{
    MAX_MESSAGE_BYTES, PROTO_VERSION, RECONNECT_NANOS, ReplConfig, SWITCHOVER_DEADLINE_NANOS,
    WITNESS_DEADLINE_NANOS,
};
use charpente::{Db, DbConfig, Id, Job, Schema, TxError, table};
use charpente_core::engine::REHELLO_MARKER;
use charpente_core::io::{Clock, Conn, Disk, File, Net};
use charpente_core::log::{Wal, WalConfig};
use charpente_core::migration::{MigrationError, SchemaVersion, TableMigration, schema_version_id};
use charpente_core::table::{Codec, IndexDef, Row, RowShape, Unscoped, View};
use charpente_sim::checks::{
    FaultReason, classify, dir_entry_loss_fired, durable_corruption_fired, is_fault_fallout,
};
use charpente_sim::{FaultConfig, Sim, SimClock, SimHost, run_universes_swarm_with};

// ------------------------------------------------------------- fixtures

#[table(global)]
struct Widget {
    id: Id<Widget>,
    #[index]
    kind: u64,
    weight: i64,
}

#[table(global)]
struct Gadget {
    id: Id<Gadget>,
    label: String,
}

/// A job, so replicated transactions exercise the platform `jobs` table too.
struct Ping {
    n: u64,
}

impl Codec for Ping {
    fn encode(&self, out: &mut Vec<u8>) {
        self.n.encode(out);
    }
    fn decode(input: &mut &[u8]) -> io::Result<Ping> {
        Ok(Ping {
            n: u64::decode(input)?,
        })
    }
}

impl Job for Ping {
    const NAME: &'static str = "ping";
    const FINGERPRINT: u64 = 0x7E57_0006;
    type Scope = Unscoped;
}

fn make_schema() -> Schema {
    Schema::new().table::<Widget>().table::<Gadget>()
}

// ------------------------------------------------ migrations T10 fixtures

/// A hand-rolled v1 shape sharing the "WidgetV2" table with the
/// `#[table(version = 2)]` `WidgetV2` below (mirrors kernel.rs's
/// `GadgetV1`/`Gadget` pattern: the macro always ties `TABLE` to the struct
/// identifier, so a from-scratch v1 fixture needs a hand-written Row+
/// RowShape pair to land on the compiled v2 struct's table).
struct WidgetV1 {
    id: Id<WidgetV1>,
    count: u32,
}
impl Row for WidgetV1 {
    type Scope = Unscoped;
    const TABLE: &'static str = "WidgetV2";
    const INDEXES: &'static [IndexDef<WidgetV1>] = &[];
    const VERSION: u32 = 1;
    const FINGERPRINT: Option<u64> = Some(0x2EB8_0001);
    fn id(&self) -> Id<WidgetV1> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.count.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<WidgetV1> {
        let input = &mut &bytes[..];
        Ok(WidgetV1 {
            id: Id::decode(input)?,
            count: u32::decode(input)?,
        })
    }
}
impl RowShape for WidgetV1 {
    const VERSION: u32 = 1;
    fn encode_row(&self, out: &mut Vec<u8>) {
        Row::encode_row(self, out)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<WidgetV1> {
        <WidgetV1 as Row>::decode_row(bytes)
    }
}

/// The compiled target: adds an index (so the close-out's index rebuild has
/// something concrete to prove converged) plus the `note` field the
/// migration fills.
#[table(version = 2, global)]
struct WidgetV2 {
    id: Id<WidgetV2>,
    #[index]
    count: u32,
    note: String,
}
impl TableMigration for WidgetV2 {
    type Prev = WidgetV1;
    fn migrate(prev: WidgetV1) -> Result<WidgetV2, MigrationError> {
        Ok(WidgetV2 {
            id: Id::from_raw(prev.id.raw()),
            count: prev.count,
            note: "v2".to_string(),
        })
    }
}

fn make_schema_v1() -> Schema {
    Schema::new().table::<WidgetV1>()
}

fn make_schema_v2() -> Schema {
    Schema::new()
        .table::<WidgetV2>()
        .migration::<WidgetV2>("WidgetV2")
}

/// The `by_count` index, content-compared (id, count, note) so the caller
/// never needs a `PartialEq`/`Debug` derive on `WidgetV2` — matches the
/// migrations.rs precedent (`by_count`/`WidgetIndexedV2`).
fn count_index_snapshot(db: &Db, k: u32) -> Vec<(u64, u32, String)> {
    let mut rows = WidgetV2::by_count(&db.snapshot().unwrap(), &k).unwrap();
    rows.sort_by_key(|r| r.id.raw());
    rows.into_iter()
        .map(|r| (r.id.raw(), r.count, r.note))
        .collect()
}

// ------------------------------------------------------------- the node

/// One replication node driven at the `Db` level (no kernel until task 8).
struct Node {
    host: SimHost,
    db: Option<Db>,
    repl: Repl,
    clock: SimClock,
    name: String,
    repl_cfg: ReplConfig,
    db_cfg: DbConfig,
    /// The schema this node opens/reopens under (migrations T10: a schema
    /// override threaded through `spawn_node_with_schema`, so `reopen`/
    /// `recover_node` reopen the SAME (possibly versioned) schema rather
    /// than the file's base `make_schema()`).
    schema: Schema,
    /// Node recoveries so far (poison/crash → reopen) — a clean universe has
    /// zero; the fingerprint oracle skips its byte check when this is nonzero.
    recoveries: u32,
    /// Seeds completed by `Repl` instances retired at recovery (a reopen builds
    /// a fresh `Repl` whose counter starts at 0); added to the live counter.
    carried_seeds: u64,
    /// True once a control verb targeting this universe COULD have flipped this
    /// node's durable role (T7). Gates `open_node`'s torn-role re-init: before
    /// any possible flip, a torn role file is a first-boot tear and deleting it
    /// re-inits from config (T6's sanctioned exit); after one, config no longer
    /// names the truth — re-initializing could resurrect a demoted primary as
    /// writable (sim-found, seed 0x668365a4e50671ba: p's `Follower` role write
    /// failed its sync during switchover completion, the crash tore it, and the
    /// re-init handler brought p back as a second writable primary). The
    /// product path was never wrong — the torn file is a named hard error whose
    /// fix text says restore, not re-init; the universe goes inconclusive
    /// exactly like the bricked-store case.
    flipped: bool,
}

/// Total snapshot seeds this node has completed, across recoveries.
fn total_seeds(node: &Node) -> u64 {
    node.carried_seeds + node.repl.stats().seeds
}

fn spawn_node(sim: &Sim, host: &str, cfg: ReplConfig, db_cfg: DbConfig) -> Result<Node, String> {
    spawn_node_with_schema(sim, host, cfg, db_cfg, make_schema())
}

/// `spawn_node`, parameterized on the schema (migrations T10: a v1/v2/
/// migration-bearing schema needs a node built against it, not the file's
/// base `Widget`/`Gadget` fixtures) — smallest-diff plumbing, no call-site
/// churn on the many existing `spawn_node` callers.
fn spawn_node_with_schema(
    sim: &Sim,
    host: &str,
    cfg: ReplConfig,
    db_cfg: DbConfig,
    schema: Schema,
) -> Result<Node, String> {
    let h = sim.host(host);
    // Open can hit injected faults (a failed sync during store/WAL creation);
    // restart the fresh host and retry, exactly as recovery does.
    for _ in 0..RECOVER_ATTEMPTS {
        match open_node(sim, &h, &cfg, &db_cfg, false, &schema) {
            Ok((db, repl)) => {
                return Ok(Node {
                    host: h,
                    db: Some(db),
                    repl,
                    clock: sim.clock(),
                    name: host.to_string(),
                    repl_cfg: cfg,
                    db_cfg,
                    schema,
                    recoveries: 0,
                    carried_seeds: 0,
                    flipped: false,
                });
            }
            Err(e) if is_fault_fallout(&e) => {
                h.restart();
                continue;
            }
            Err(e) => return Err(format!("{host}: {e}")),
        }
    }
    sim.cover("repl:inconclusive:spawn-exhausted");
    Err(format!(
        "{INCONCLUSIVE}: {host} spawn exhausted {RECOVER_ATTEMPTS} attempts under faults"
    ))
}

/// Marker prefix on an error that means "this universe's disk was too faulty to
/// make progress" — not a bug. The swarm workloads map it to a covered skip,
/// mirroring the single-node suite's injected-fault skips.
const INCONCLUSIVE: &str = "universe-inconclusive";

/// Wraps a swarm universe: an inconclusive result (a node that could not
/// recover under persistent faults) is a covered skip, not a failure.
///
/// `sim.give_up` and `sim.cover` answer different questions and both are
/// wanted. The coverage keys are a floorable distribution over CAUSES (marked
/// at each give-up site, plus this aggregate); `give_up` is the scalar the
/// runner reports — how many universes adjudicated anything at all. Without
/// it, mapping the marker back to `Ok(())` here (above the runner) is exactly
/// what made a give-up indistinguishable from a pass.
fn finish_universe(sim: &Sim, r: Result<(), String>) -> Result<(), String> {
    match r {
        Err(e) if e.contains(INCONCLUSIVE) => {
            sim.cover("repl:inconclusive");
            sim.give_up(e);
            Ok(())
        }
        other => other,
    }
}

fn open_node(
    sim: &Sim,
    h: &SimHost,
    cfg: &ReplConfig,
    db_cfg: &DbConfig,
    flipped: bool,
    schema: &Schema,
) -> Result<(Db, Repl), String> {
    let mut db = Db::open(Box::new(h.disk()), db_cfg.clone(), schema.clone())
        .map_err(|e| format!("Db::open: {e}"))?;
    match Repl::open(Box::new(h.net()), Box::new(h.disk()), cfg.clone(), &mut db) {
        Ok((repl, _notice)) => Ok((db, repl)),
        // A torn witness file (M2 sub-project 0b task 2): keyed on its own
        // full, non-overlapping needle ("witness file failed its integrity
        // check"), an independent arm from the role-file arm below (keyed on
        // ITS own full needle, "role file failed its integrity check") — the
        // two are mutually exclusive by construction, so arm ORDER is
        // irrelevant; both need the FULL string rather than the shared
        // "integrity check" substring precisely because that substring alone
        // cannot tell them apart. Unlike the role file, there is no
        // config-derived witness set to re-init from, and witnesses.rs's own
        // fix text is explicit that deleting it is safe ONLY when the
        // operator is certain no follower of this node was ever promoted —
        // a fact this harness cannot prove in a swarm-fuzzed universe (a
        // witness can be recorded by an ordinary running primary with no
        // role flip at all, so `flipped` is not a safe proxy here either,
        // where it exists). Always inconclusive: the operator-restore path
        // is the fix. Counted separately from the generic inconclusive
        // bucket ("repl:witness-file-inconclusive") so this residual,
        // newly-introduced permanent-unavailability path stays measurable,
        // not invisible (fix round 1, Noah's ruling — a genuinely torn
        // NON-empty witness file is a real design fork, escalated
        // separately, not fixed here). (This comment is kept byte-identical
        // at every copy of this arm — replication.rs's two sites, auth.rs,
        // realtime.rs — so a future reviewer can diff them; see
        // task-2-report.md.)
        Err(e)
            if e.to_string()
                .contains("witness file failed its integrity check") =>
        {
            sim.cover("repl:witness-file-inconclusive");
            Err(format!(
                "{INCONCLUSIVE}: {} torn witness file — the operator restore path is the fix: {e}",
                h.name()
            ))
        }
        // Sim-found bug (seeds 0x23b5e7e7f2132438 unparseable-tear,
        // 0x51dacce702b667c8 parseable-tear naming a ghost primary): a torn
        // first-boot role write. Fixed in role.rs by the CRC integrity line —
        // corruption is now this named hard error instead of a silent
        // wrong-role parse. The error's own fix text applies verbatim here:
        // BEFORE any role flip these nodes never finished first boot, so
        // deleting the file to re-initialize from config is the sanctioned
        // exit, and load_role rewrites the initial on the retry. AFTER a
        // possible flip (T7's control verbs) the same move is the error text's
        // OTHER branch — "otherwise restore this node from a replica or
        // backup" — because config may resurrect a demoted primary as writable
        // (sim-found, seed 0x668365a4e50671ba, pinned below): the harness has
        // no restore rig yet, so the universe is honestly inconclusive.
        Err(e)
            if e.to_string()
                .contains("role file failed its integrity check") =>
        {
            if flipped {
                sim.cover("repl:inconclusive:role-file-torn");
                return Err(format!(
                    "{INCONCLUSIVE}: {} torn role file after a role flip — the operator \
                     restore path (not config re-init) is the fix: {e}",
                    h.name()
                ));
            }
            let _ = h.disk().remove("role");
            let _ = h.disk().sync_dir("role");
            let (repl, _notice) =
                Repl::open(Box::new(h.net()), Box::new(h.disk()), cfg.clone(), &mut db)
                    .map_err(|e| format!("Repl::open (after torn-role re-init): {e}"))?;
            Ok((db, repl))
        }
        Err(e) => Err(format!("Repl::open: {e}")),
    }
}

/// Restart-and-recover a node in place: drop the old `Db`/`Repl`, power-cycle
/// the host (collapse unsynced writes, tear the net so the peer reaps the
/// session and the listener frees), then reopen through the same path. Retried
/// under open-time faults; each attempt marks the `restart` coverage key.
fn recover_node(sim: &Sim, node: &mut Node) -> Result<(), String> {
    sim.cover("restart");
    node.recoveries += 1;
    node.carried_seeds += node.repl.stats().seeds; // the retired Repl's seeds
    node.db = None; // release the old handles before power-cycling
    let mut last = String::new();
    for attempt in 0..RECOVER_ATTEMPTS {
        // Power-cycle on the first attempt (the caller needs the net torn so the
        // peer reaps the session and the listener frees) and only PERIODICALLY
        // after that — not before every try. `settle_boot`'s doc comment
        // (`realtime.rs:706-712`) measured this exact distinction on seed
        // 0x536007ad99fd90bf at write_error 0.144: cycling before each try,
        // 0 of 20_000 boots ever succeeded; retrying in place, the same universe
        // came up after ~16 cycles. The mechanism is that `Store::apply` commits
        // at `Durability::None`, so every `Sim::restart` vanishes the store's
        // uncheckpointed state and forces the next open to rebuild from the WAL —
        // more writes, at the same per-write failure odds, on every single try.
        // Retrying in place is also the faithful model: the machine did not lose
        // power, so the page cache is still there for the next open. The periodic
        // cycle stays because a pending `crash_disk_after` can fire mid-open and a
        // crashed sim disk answers everything with `crashed_error` until a restart.
        if attempt.is_multiple_of(RECOVER_CYCLE_EVERY) {
            node.host.restart();
        }
        match reopen(sim, node) {
            Ok(()) => return Ok(()),
            // Fallout first: an injected fault inside `Store::open` bubbles from
            // `Db::open` as itself and is excused here by its own text. DESIGN
            // §4's compacted-log refusal ("cannot be rebuilt from the log alone")
            // now carries only redb's verdict on the store's bytes, never an
            // injected cause, so reaching it is a failure — under redb 4.3.0 the
            // crash model cannot brick the store. So is the durable-horizon
            // refusal (S3b item 0): every property that recovers through here
            // turns both durable-corruption axes off (`no_media_corruption`),
            // so no fault on this node's disk can explain it, and
            // `is_fault_fallout` no longer excuses it by its text.
            Err(e) if is_fault_fallout(&e) => last = e, // open faulted; retry, cycling periodically
            Err(e) => return Err(format!("recover {}: {e}", node.name)),
        }
    }
    // Db::open replays the un-checkpointed gap through the store, which needs a
    // long fault-free streak of writes; under a persistently faulty disk it may
    // legitimately never succeed (the single-node suite skips such universes
    // too). Mark the universe inconclusive rather than failing it.
    //
    // This reason differs from its siblings in KIND: the others name a disk that
    // cannot be recovered from at all, while this one says only that 500
    // attempts were not enough. It used to say that made it "a BUDGET rather
    // than a state", which the sub-key below has now measured false in both
    // directions — no sticky wedge (0 of 92 non-injected on P1), and no budget
    // either (10x rescued ~4% on the seeded sibling). Treat a growing share as
    // a question about the FAULT RATE the recovery loop is being asked to open
    // through, not as a constant to raise.
    sim.cover("repl:inconclusive:recover-exhausted");
    // ...and the sub-key that says whether "budget" above is TRUE. The comment
    // asserts a budget; the terminal error is the evidence for or against it.
    // An `injected` last error is a run of independent bad luck, which a bigger
    // budget really would rescue. A `redb-latch` or `poisoned` one repeated 500
    // times is a state the loop cannot retry its way out of, and raising
    // `RECOVER_ATTEMPTS` against it just buys the same wedge more slowly. The
    // 10x experiments recorded on `SWITCHOVER_CONVERGED_FLOOR` and
    // `SURVIVOR_PROMISE_FLOOR` already disagree with each other about which
    // this is; this key is how the file stops guessing.
    sim.cover(recover_exhausted_key(&last));
    Err(format!(
        "{INCONCLUSIVE}: recover {} exhausted {RECOVER_ATTEMPTS} attempts (last: {last})",
        node.name
    ))
}

/// The sub-key for a `:recover-exhausted` give-up, naming the family of the
/// error the LAST attempt saw.
///
/// **What this key settles, and what it does not.** The two sites that cover
/// `:recover-exhausted` describe it as "500 attempts were not enough", which
/// reads as a budget too small. This key can refute that outright in one
/// direction: 500 consecutive `redb-latch` or `poisoned` errors are a state
/// the loop cannot retry its way out of, and raising the budget against one
/// buys the same wedge more slowly while hiding it better.
///
/// It cannot CONFIRM a budget, and the measurement below is why the difference
/// matters. **Measured 2026-09-07: `:injected` accounts for 92 of 92 on P1 and
/// 110 of 110 on P2 — every other family is zero.** So there is no sticky
/// wedge here. But `:injected` dominance is not evidence of a budget either:
/// when the fault rate is high enough that an open cannot complete, every
/// attempt fails with a FRESH injected error and more attempts change nothing.
/// P1 carries 109,852 injected write errors across 256 universes, ~430 per
/// universe, and `SEEDED_CONVERGED_FLOOR`'s 10x experiment rescued ~4%
/// (109 -> 105). Near-zero per-attempt success, not a budget one decimal place
/// from working.
///
/// The operative conclusion: **do not raise `RECOVER_ATTEMPTS` against these.**
/// Both readings that would justify it are now measured false.
///
/// Keys are `&'static str` on purpose (`Sim::cover`'s signature), so these are
/// written out rather than formatted: the set stays greppable, and the match
/// is exhaustive, so a new `FaultReason` variant is a compile error here
/// instead of a silently unattributed give-up.
fn recover_exhausted_key(last: &str) -> &'static str {
    match classify(last) {
        FaultReason::Injected => "repl:inconclusive:recover-exhausted:injected",
        FaultReason::Poisoned => "repl:inconclusive:recover-exhausted:poisoned",
        FaultReason::RedbLatch => "repl:inconclusive:recover-exhausted:redb-latch",
        FaultReason::DurableHorizon => "repl:inconclusive:recover-exhausted:durable-horizon",
        // `recover_node` only ever stores a fallout error in `last`, so this is
        // unreachable through that path — but `last` also starts EMPTY, and a
        // loop that exhausts without one failed open (RECOVER_ATTEMPTS == 0, or
        // every attempt returning early) would land here. Attributed rather
        // than asserted: an unattributed give-up is the defect this key exists
        // to remove, and a panic inside a give-up path would convert an
        // inconclusive universe into a dead node.
        FaultReason::Unrelated => "repl:inconclusive:recover-exhausted:unrelated",
    }
}

fn reopen(sim: &Sim, node: &mut Node) -> Result<(), String> {
    let (db, repl) = open_node(
        sim,
        &node.host,
        &node.repl_cfg,
        &node.db_cfg,
        node.flipped,
        &node.schema,
    )?;
    node.repl = repl;
    node.db = Some(db);
    Ok(())
}

const RECOVER_ATTEMPTS: u32 = 500;

/// Power-cycle every Nth recovery attempt rather than every one — the value
/// `settle_boot` already uses (`SETTLE_CYCLE_EVERY`, `realtime.rs:657`), kept
/// the same so the two retry loops model a failed process start identically.
const RECOVER_CYCLE_EVERY: u32 = 8;

// --------------------------------------------------------- the scheduler

/// One deterministic scheduler round: `pump_io` every node, then `step` every
/// node that `has_work` (handling faults), then — if nothing was runnable —
/// advance virtual time to the earliest pending event (a node timer or an
/// in-flight network delivery). `Ok(false)` means the universe is fully
/// quiescent (no work and no pending events).
fn scheduler_round(sim: &Sim, nodes: &mut [&mut Node]) -> Result<bool, String> {
    let now = sim.now().as_nanos();
    for node in nodes.iter_mut() {
        node.repl.pump_io(now);
    }
    let mut worked = false;
    for node in nodes.iter_mut() {
        let has_work = match &node.db {
            Some(db) => node.repl.has_work(db, now),
            None => false,
        };
        if has_work {
            step_node(sim, node)?;
            worked = true;
        }
    }
    if worked {
        return Ok(true);
    }
    // Nothing runnable: jump to the next event.
    let mut next = sim.next_wakeup();
    for node in nodes.iter() {
        if let Some(d) = node.repl.next_deadline(now)
            && d > now
        {
            next = Some(next.map_or(d, |x| x.min(d)));
        }
    }
    match next {
        Some(t) if t > now => {
            sim.clock().sleep(Duration::from_nanos(t - now));
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn step_node(sim: &Sim, node: &mut Node) -> Result<(), String> {
    let stepped = {
        let db = node.db.as_mut().expect("step_node on a live node");
        node.repl.step(db, &node.clock)
    };
    match stepped {
        Ok(()) => {}
        Err(e) if is_fault_fallout(&e.to_string()) => return recover_node(sim, node),
        Err(e) => return Err(format!("{} step: {e}", node.name)),
    }
    if node.repl.db_needs_reseed() {
        let db = node.db.take().expect("db present before reseed");
        match node.repl.run_seed(db, &node.clock) {
            Ok(new_db) => node.db = Some(new_db),
            Err(e) if is_fault_fallout(&e.to_string()) => return recover_node(sim, node),
            Err(e) => return Err(format!("{} run_seed: {e}", node.name)),
        }
    }
    if node.db.as_ref().is_some_and(|db| db.poisoned()) {
        return recover_node(sim, node);
    }
    Ok(())
}

/// Drive the scheduler until `done`, or fail on a stall (no work, no pending
/// events, `done` still false) or on the iteration budget.
fn run_until(
    sim: &Sim,
    nodes: &mut [&mut Node],
    done: impl Fn(&[&mut Node]) -> bool,
    budget: u32,
) -> Result<(), String> {
    for _ in 0..budget {
        if done(nodes) {
            return Ok(());
        }
        if !scheduler_round(sim, nodes)? {
            return if done(nodes) {
                Ok(())
            } else {
                Err(format!(
                    "stalled: no work, no pending events, done() false (last_applied {:?})",
                    nodes
                        .iter()
                        .map(|n| n.db.as_ref().and_then(|d| d.last_applied()))
                        .collect::<Vec<_>>()
                ))
            };
        }
    }
    if done(nodes) {
        Ok(())
    } else {
        Err(format!("budget {budget} exhausted before done()"))
    }
}

/// A bounded burst of scheduler rounds (for interleaving commits with
/// shipping, so the shipper tracks a moving target). Stops early if quiescent;
/// never errors on the budget.
fn drive_rounds(sim: &Sim, nodes: &mut [&mut Node], k: u32) -> Result<(), String> {
    for _ in 0..k {
        if !scheduler_round(sim, nodes)? {
            break;
        }
    }
    Ok(())
}

const CONVERGE_BUDGET: u32 = 500_000;

/// Commit one transaction on the primary node, best effort: `Ok` whether it
/// committed or a platform fault prevented it (recovering the node); `Err`
/// only on a real bug (an app rejection, or a non-fallout IO error).
fn commit_widget(sim: &Sim, node: &mut Node, i: u64) -> Result<(), String> {
    let Some(db) = node.db.as_mut() else {
        return Ok(());
    };
    let r = db.transact(|tx| {
        tx.put(&Widget {
            id: Id::from_raw(i),
            kind: i % 3,
            weight: i as i64,
        });
        if i.is_multiple_of(4) {
            tx.put(&Gadget {
                id: Id::from_raw(i),
                label: format!("gadget-{i}"),
            });
        }
        if i.is_multiple_of(5) {
            tx.enqueue(Ping { n: i });
        }
        Ok::<(), io::Error>(())
    });
    match r {
        Ok(()) => Ok(()),
        Err(TxError::Io(e)) if is_fault_fallout(&e.to_string()) => recover_node(sim, node),
        Err(TxError::Io(e)) => Err(format!("commit io: {e}")),
        Err(TxError::App(e)) => Err(format!("commit app: {e}")),
        Err(TxError::NotPrimary { primary }) => {
            Err(format!("commit refused: not primary of {primary}"))
        }
        Err(TxError::Verifying { unreached }) => {
            // Legitimate since the boot gate landed (M2 sub-project 0b task
            // 5): a primary that restarted with a non-empty witness set
            // serves reads and withholds writes until every witness answers,
            // so a commit offered in that window is refused and simply did
            // not happen — the same "no write, no bug" outcome the fault arm
            // above returns `Ok` for. Covered rather than silent, so the
            // universes that exercise the gate stay visible in the report.
            let _ = unreached;
            sim.cover("repl:witness-gated-write");
            Ok(())
        }
    }
}

// --------------------------------------------------------------- oracles

/// (a) equal `last_applied`, (b) byte-equal `scan_raw` over every table plus
/// both meta counters — the layout-independent content oracle that holds
/// across a seed and across crash-recovery, retried under transient read
/// faults. Since per-batch apply, this is THE replication guarantee rather
/// than a weaker companion to the byte fingerprint (DESIGN §1, property 1),
/// so it runs in every universe and covers `repl:logical-converged`.
fn assert_converged(sim: &Sim, p: &mut Node, f: &mut Node) -> Result<(), String> {
    // The two-node analogue of `retry_read_reopen`: the convergence scan reads
    // BOTH nodes' live redb handles, so a latched poison ("close and re-open
    // the database") can be on either; the `p:`/`f:` prefix `converged_read`
    // tags its redb reads with says which, so only that node is reopened.
    for _ in 0..64 {
        // `last_applied` reads in-memory engine state (no redb snapshot), so it
        // cannot latch the redb poison — check it each attempt, un-prefixed.
        let (pla, fla) = {
            let pdb = p.db.as_ref().ok_or("p has no db")?;
            let fdb = f.db.as_ref().ok_or("f has no db")?;
            (pdb.last_applied(), fdb.last_applied())
        };
        if pla != fla {
            return Err(format!("last_applied mismatch: p={pla:?} f={fla:?}"));
        }
        match converged_read(p, f) {
            Ok(()) => {
                // Logical convergence is THE replication guarantee (DESIGN §1,
                // property 1) — byte-identity is only claimed where the two
                // nodes shared a write schedule. So this oracle runs in every
                // universe, and covers a key the swarm `require:`s to prove it.
                sim.cover("repl:logical-converged");
                return Ok(());
            }
            Err(e) if e.contains("close and re-open the database") => {
                // Latched redb poison — recover_node-style drop+reopen clears
                // it (fresh handle; WAL replay restores the converged applied
                // state). Only the poisoned node, by its `p:`/`f:` tag.
                if e.starts_with("p:") {
                    recover_node(sim, p)?;
                } else {
                    recover_node(sim, f)?;
                }
            }
            Err(e) if is_fault_fallout(&e) => continue,
            Err(e) => return Err(e),
        }
    }
    Err("retry_fallout: 64 attempts all faulted".to_string())
}

/// One convergence-scan attempt. redb reads are tagged `p:`/`f:` so the caller
/// can reopen the poisoned node; logical divergences carry their own message.
fn converged_read(p: &Node, f: &Node) -> Result<(), String> {
    let pdb = p.db.as_ref().ok_or("p has no db")?;
    let fdb = f.db.as_ref().ok_or("f has no db")?;
    let ps = pdb.snapshot().map_err(|e| format!("p: {e}"))?;
    let fs = fdb.snapshot().map_err(|e| format!("f: {e}"))?;
    for table in pdb.table_names() {
        let table_scoped = pdb.table_scoped(table).map_err(|e| format!("p: {e}"))?;
        let pr = ps
            .scan_raw(table, table_scoped)
            .map_err(|e| format!("p: {e}"))?;
        let fr = fs
            .scan_raw(table, table_scoped)
            .map_err(|e| format!("f: {e}"))?;
        if pr != fr {
            return Err(format!(
                "table {table} diverged: p has {} rows, f has {}",
                pr.len(),
                fr.len()
            ));
        }
    }
    if ps.next_job_id().map_err(|e| format!("p: {e}"))?
        != fs.next_job_id().map_err(|e| format!("f: {e}"))?
    {
        return Err("next_job_id diverged".to_string());
    }
    // Same reason as next_job_id, and the reason it is checked at all: both
    // counters are bumped inside the store transaction that carries
    // `applied_seq`, so per-batch apply could desynchronize them between two
    // nodes without moving a single row byte — which every other check here
    // would pass.
    if ps.next_event_id().map_err(|e| format!("p: {e}"))?
        != fs.next_event_id().map_err(|e| format!("f: {e}"))?
    {
        return Err("next_event_id diverged".to_string());
    }
    Ok(())
}

/// (c) The determinism gift, scoped to what the architecture actually
/// promises: after checkpoint + graceful close, two nodes that executed the
/// SAME redb op sequence hold identical store + WAL bytes. The role file is
/// excluded: it differs by design (primary vs follower). Consumes the nodes
/// (graceful close is part of the oracle).
///
/// Two conditions gate it, each with its own coverage key so a green suite
/// cannot hide which one is firing:
///
/// - Injected faults perturb the op sequence beyond grouping (retries and
///   recoveries change redb's history even though logical content converged)
///   — `repl:fingerprint-skipped`.
/// - The two nodes grouped the same records into store transactions
///   differently, which redb's physical layout depends on (DESIGN §1,
///   property 1) — `repl:fingerprint-skipped-grouping`. This is read off
///   `Db::store_write_schedule()`, a digest each node RECORDS as it commits,
///   never inferred from what faults did or did not fire: a follower that
///   caught up in one wide ship applied in one transaction what the primary
///   committed in forty, and no fault count reveals that.
///
/// When neither fires, the comparison runs for real and covers
/// `repl:fingerprint-compared` — which the swarm `require:`s, so a change that
/// made the schedules always differ would go red here rather than quietly
/// retiring this oracle. Oracle (b) `assert_converged` is unconditional and
/// has already run in every universe either way; logical convergence is the
/// guarantee, and this is the sharper check where it is available.
fn assert_bytes_identical(sim: &Sim, mut p: Node, mut f: Node) -> Result<(), String> {
    // Read BEFORE the checkpoints below, which are themselves scheduled writes.
    // They are symmetric (one per node), so comparing here or after is the same
    // verdict — but reading first keeps the digest a fact about the universe
    // rather than about this function.
    let (psched, fsched) = {
        let pdb = p.db.as_ref().ok_or("p has no db")?;
        let fdb = f.db.as_ref().ok_or("f has no db")?;
        (pdb.store_write_schedule(), fdb.store_write_schedule())
    };
    if !universe_is_quiet(sim, &[&p, &f]) {
        sim.cover("repl:fingerprint-skipped");
        return Ok(());
    }
    if psched != fsched {
        sim.cover("repl:fingerprint-skipped-grouping");
        return Ok(());
    }
    // "Quiet" above is a fact about the past: the universe's FaultConfig would
    // still roll faults on the checkpoint, close and reads below, and one did
    // (seed 0xd609e4dde8a4008f: an injected fsync failure in this checkpoint,
    // reported as a failure). Stop them, so the comparison runs on a fault-free
    // tail and any error in it is a real failure.
    sim.quiesce_disk_faults();
    for n in [&mut p, &mut f] {
        n.db.as_mut()
            .expect("quiet universe: db live")
            .checkpoint()
            .map_err(|e| format!("quiet-universe checkpoint failed: {e}"))?;
        n.db = None; // graceful close: redb drop persists, WAL is synced
    }
    let a = data_fingerprint(&p.host)?;
    let b = data_fingerprint(&f.host)?;
    if a != b {
        return Err(format!(
            "converged nodes' store+log bytes diverged in a fault-free universe \
             that also shared one store write schedule ({psched:#018x}): \
             p={a:#018x} f={b:#018x}"
        ));
    }
    sim.cover("repl:fingerprint-compared");
    Ok(())
}

/// True when no *fault* perturbed either host's disk op sequence: no disk
/// faults, no crashes, no restarts, no node recoveries. Net-only events
/// (`conn_drop`, `net_spike`) stay admitted because they still cause no
/// replays and no double-applies — a dropped conn re-Hellos and resumes from
/// the follower's real position.
///
/// What they DO change, since per-batch apply landed, is how many frames ride
/// one ship and therefore how many records share one store transaction. So
/// this is no longer sufficient for byte-comparability on its own, and it is
/// deliberately not stretched to cover the gap: `assert_bytes_identical` pairs
/// it with the recorded write schedule, which observes grouping directly
/// instead of inferring it from which faults fired.
fn universe_is_quiet(sim: &Sim, nodes: &[&Node]) -> bool {
    if nodes.iter().any(|n| n.recoveries > 0) {
        return false;
    }
    sim.coverage().keys().all(|k| {
        *k == "fault:conn_drop"
            || *k == "fault:net_spike"
            || !(k.starts_with("fault:") || k.starts_with("crash:") || *k == "restart")
    })
}

/// FNV-1a over a host's data files — every `wal/` segment plus the store
/// file, sorted by name; the role file is excluded (differs by design).
fn data_fingerprint(host: &SimHost) -> Result<u64, String> {
    let disk = host.disk();
    let mut names = disk.list("wal/").map_err(|e| e.to_string())?;
    names.push("tables.redb".to_string());
    names.sort();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut eat = |bytes: &[u8]| {
        for &b in bytes {
            hash = (hash ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    for name in &names {
        let file = disk.open(name).map_err(|e| e.to_string())?;
        let len = file.len().map_err(|e| e.to_string())?;
        let mut buf = vec![0u8; len as usize];
        if len > 0 {
            file.read_at(0, &mut buf).map_err(|e| e.to_string())?;
        }
        eat(name.as_bytes());
        eat(&len.to_le_bytes());
        eat(&buf);
    }
    Ok(hash)
}

fn retry_fallout<T>(mut f: impl FnMut() -> Result<T, String>) -> Result<T, String> {
    for _ in 0..64 {
        match f() {
            Ok(v) => return Ok(v),
            Err(e) if is_fault_fallout(&e) => continue,
            Err(e) => return Err(e),
        }
    }
    Err("retry_fallout: 64 attempts all faulted".to_string())
}

/// `retry_fallout` for a read that goes through a live redb handle. A single
/// transient injected read error LATCHES redb's handle poison ("Previous I/O
/// error ... close and re-open the database"): every later snapshot off the
/// SAME handle keeps returning it, so plain `retry_fallout` spins its whole
/// budget making no progress. On that specific tell we drop and reopen the
/// node's handle (recover_node-style — a fresh redb handle clears the latch),
/// then retry the read. Any OTHER fault fallout (e.g. a raw transient WAL read
/// at its own site) keeps retrying in place, exactly as `retry_fallout` always
/// did. Same bounded 64-attempt budget; `finish_universe`'s classification is
/// untouched — `recover`'s own INCONCLUSIVE/hard errors propagate as-is.
///
/// Pinned RED→GREEN seed 0xdce31ea4094053cb: first-write adoption (migrations
/// sub-project 5) shifted this universe's 0.02 read-fault draw onto
/// `assert_converged`'s oracle read, latching the poison there — RED on the
/// unfixed harness (`retry_fallout` spins 64× on the poisoned handle), GREEN
/// once the read reopens. Both nodes had FULLY converged; the latched-redb-
/// poison class is pre-existing — same family as the recovery-retry fsyncgate
/// seed 0x5cafb3ac016dccf9 — surfaced, not caused, by the adoption byte-shift.
fn retry_read_reopen<N, T>(
    sim: &Sim,
    node: &mut N,
    recover: impl Fn(&Sim, &mut N) -> Result<(), String>,
    read: impl Fn(&N) -> Result<T, String>,
) -> Result<T, String> {
    let mut last = String::new();
    for _ in 0..64 {
        match read(node) {
            Ok(v) => return Ok(v),
            Err(e) if e.contains("close and re-open the database") => recover(sim, node)?,
            Err(e) if is_fault_fallout(&e) => last = e,
            Err(e) => return Err(e),
        }
    }
    Err(format!(
        "retry_fallout: 64 attempts all faulted (last: {last})"
    ))
}

fn converged(nodes: &[&mut Node]) -> bool {
    let a = nodes[0].db.as_ref().and_then(|d| d.last_applied());
    let b = nodes[1].db.as_ref().and_then(|d| d.last_applied());
    a == b
}

fn primary_cfg(addr: &str) -> ReplConfig {
    ReplConfig {
        listen: Some(addr.to_string()),
        initial_role: Role::Primary,
        peer_secret: None,
        strict_witnesses: false,
    }
}

fn follower_cfg(listen: &str, primary: &str) -> ReplConfig {
    ReplConfig {
        listen: Some(listen.to_string()),
        initial_role: Role::Follower {
            primary: primary.to_string(),
        },
        peer_secret: None,
        strict_witnesses: false,
    }
}

/// The convergence/seeding properties disable the durable-media-corruption
/// axes: those can make the primary lose acked records the follower keeps — a
/// legitimate divergence that is property 8's domain (the above-horizon
/// promise), not a convergence bug. The rest of the fault matrix stays hot.
fn no_media_corruption(cfg: &mut FaultConfig) {
    cfg.bit_flip = 0.0;
    cfg.misdirected_write = 0.0;
}

/// The sub-key is only evidence if the text it classifies still classifies.
/// `classify` keys on error PROSE (DESIGN §7 makes that prose product surface),
/// so a reworded exemplar silently becomes `Unrelated` and every wedge it used
/// to name is filed as `:unrelated` instead — a give-up that looks attributed
/// and is not. One exemplar per family, and a distinctness check, because two
/// arms sharing a literal would merge a wedge into a budget in the counts.
#[test]
fn every_recover_exhausted_sub_key_is_reachable_and_distinct() {
    let cases = [
        (
            "simulated disk write error (injected fault)",
            "repl:inconclusive:recover-exhausted:injected",
        ),
        (
            "WAL poisoned by an earlier IO failure",
            "repl:inconclusive:recover-exhausted:poisoned",
        ),
        (
            "Storage error: close and re-open the database",
            "repl:inconclusive:recover-exhausted:redb-latch",
        ),
        (
            "log frame 3 is invalid but at-or-below the store's durable horizon 7",
            "repl:inconclusive:recover-exhausted:durable-horizon",
        ),
        ("", "repl:inconclusive:recover-exhausted:unrelated"),
    ];
    for (text, expected) in cases {
        assert_eq!(
            recover_exhausted_key(text),
            expected,
            "classify() no longer routes {text:?} to {expected} — either the error was \
             reworded or classify's arms moved, and the give-up it names is now \
             misattributed rather than loudly wrong"
        );
    }
    let mut keys: Vec<&str> = cases
        .iter()
        .map(|(t, _)| recover_exhausted_key(t))
        .collect();
    keys.sort_unstable();
    let before = keys.len();
    keys.dedup();
    assert_eq!(
        before,
        keys.len(),
        "two fault families share a sub-key, so a wedge and a budget count as one thing"
    );
}

// ------------------------------------------------------- property: ship

#[test]
fn follower_converges_to_the_primary_fingerprint() {
    run_universes_swarm_with!(
        0x2EB1_0001,
        256,
        no_media_corruption,
        |sim: &mut Sim| {
            let r = convergence_universe(sim);
            finish_universe(sim, r)
        },
        require: [
            "fault:conn_drop",
            "restart",
            "repl:logical-converged",
            "repl:fingerprint-compared"
        ],
        floor: [
            ("repl:fingerprint-compared", FINGERPRINT_FLOOR),
            ("repl:logical-converged", CONVERGED_FLOOR)
        ]
    );
}

/// Universes that reached a verdict at all, MEASURED: 167 of 256 at base
/// `0x2EB1_0001`, reproduced identically on two runs (2026-09-05) — the run is
/// deterministic at a fixed base, so this number moves only when code does.
///
/// **Why this needs a floor and `require:` is not enough.** The other 89
/// universes end in `finish_universe`'s `INCONCLUSIVE => sim.cover(…); Ok(())`
/// arm: a covered skip, deliberately not a failure, because a disk too faulty to
/// reopen is not a bug. But `repl:inconclusive` is required by nothing and
/// floored by nothing, and the runner enforces no ceiling on it — so before this
/// line existed, the give-up rate could climb from 35% to 90% and this property
/// would stay green while testing almost nothing. That is exactly the failure
/// `FINGERPRINT_FLOOR`'s comment describes one constant above, and the same
/// remedy applies: pin the POSITIVE key, so silence has to survive a number.
///
/// **A drop below this is a finding, not a number to lower.** It means more
/// universes are giving up before the oracle runs. Find out which of the five
/// inconclusive producers grew — `spawn_node` exhaustion (line 241), a torn
/// witness file (304), a torn role file (327), a store bricked below a compacted
/// log (381), or `recover_node` exhaustion (394). Three of those are honestly
/// unrecoverable states and two are budgets, and today they are indistinguishable
/// because all five cover the same key; splitting them is the named next step.
/// (That step has since landed — every one of those five now covers its own
/// `repl:inconclusive:*` cause key, and all 89 of this property's give-ups are
/// `:recover-exhausted`. The store-bricked producer is gone since redb 4.3.0:
/// the crash model cannot brick the store, so that refusal now fails unless an
/// injected fault caused it.)
///
/// **Why this floor sits ON its measurement while the four added in
/// oracle-density task 3 sit 2 SEs below theirs.** The two conventions answer
/// different questions and both are defensible. This one pins an exact-replay
/// number: at a fixed base seed the run is deterministic, so 167 moves only when
/// code moves, and a floor here goes red on *any* drift — maximum detection
/// power, and a false red on any change that merely re-rolls the fault stream.
/// `SEEDED_CONVERGED_FLOOR`, `SWITCHOVER_CONVERGED_FLOOR`, `FAILOVER_PREFIX_FLOOR`
/// and `SURVIVOR_PROMISE_FLOOR` sit at measured − 2·sqrt(n·p·(1−p)) instead, so
/// they survive a re-rolled stream (≈97.7% of the time) at the price of
/// tolerating an 11-31% relative fall in reach before firing. Neither is the
/// "right" convention in general: this one is sharper, those are quieter.
///
/// **This value is deliberately NOT being loosened to match them.** A floor is
/// lowered only with a reason, and "it might flap" is a reason to find out why it
/// moved, not to move it. The decision point is the first stream-shifting change
/// that actually makes it flap — at which point the choice is between
/// re-measuring it here and giving it the same 2-SE headroom, with the new
/// number in hand rather than in advance.
///
/// **That decision point ARRIVED 2026-09-07, and this is the re-measure half of
/// it — the headroom half is deliberately NOT taken here.** The stream-shifting
/// change is `Wal::open_with_horizon`'s directory barrier moving to before the
/// replay loop (`log.rs`, a real acked-write durability fix): a `sync_dir`
/// consumes RNG draws, so relocating an IO op inside `Wal::open` re-rolls every
/// universe at a pinned base. Attribution is a CONTROL, not an inference —
/// checking out the pre-barrier `log.rs` makes this whole suite green again
/// (59 passed), and restoring it reproduces the shortfall. Exactly the "false
/// red on any change that merely re-rolls the fault stream" this comment
/// predicted, and the reach it measures is unchanged in kind: 166 of 256
/// universes converge, against 167 before.
///
/// Re-measured to 166, no margin subtracted, which keeps this floor's stated
/// convention. **Widening it to the siblings' 2-SE headroom is the wrong repair,
/// and DESIGN.md had already settled it before this was framed as an open
/// choice.** The cost being complained about — one correctness fix forcing three
/// pinned-base re-measures in a single day, across `archive.rs`, `realtime.rs`
/// and here — is not this floor being too sharp. It is the fault stream being
/// perturbable. Every disk fault rolls off one shared per-host stream,
/// `keyed_stream(seed, "host-faults:{host}")`, so `sync_dir`'s two draws
/// (`disk_op`, then `chance(sync_error)`) displace every later roll, and moving
/// one barrier inside `Wal::open` re-rolls all 256 universes.
///
/// DESIGN.md met the identical hazard on the object store: a delete roll on the
/// shared stream would have "re-roll[ed] every pinned universe merely by
/// declaring the field, and invalidat[ed] the convergence floors and both
/// recorded seeds with it". The sanctioned repair there was to key the axis onto
/// its own stream and **prove** the counts unchanged — not to loosen the oracle
/// that noticed. Keying the directory barrier the same way is the standing fix.
/// It is not free the way that one was: `object_delete_error` was non-perturbing
/// because it defaulted to zero, whereas `sync_dir` fires at nonzero rates today,
/// so re-keying costs one final re-measure everywhere (including the two seeds
/// pinned in `ci.yml`) and then ends the recurrence.
///
/// Left sharp, therefore, on the doctrine and not on a pending ruling — a floor
/// is lowered with a reason, and "it flapped once, as designed" is the reason to
/// record the flap, not to widen it.
///
/// **166 -> 164 on 2026-09-07 when that standing fix was taken.** Splitting the
/// barrier onto its own stream re-rolled every universe one final time, exactly
/// as this comment predicted it would; the reach it measures is unchanged in
/// kind (164 of 256 against 166). Barrier movement can no longer move this
/// number again.
///
/// **164 -> 253 on 2026-09-30, for redb 4.3.0**, two identical batches. The
/// count at the parent commit under redb 4.1.0 is still 164 (a control run),
/// so the move is the dependency's. Its non-durable commits stay in its own
/// write buffer rather than reaching the file (DESIGN §1, "The store's crash
/// surface"), so far fewer store IOs are exposed to injected faults. The
/// give-ups fell from 92 to 3, all three `:recover-exhausted:injected`. The
/// floor is set on the new count, keeping this constant's convention, so a
/// return to the old give-up rate is now red.
///
/// **253 -> 256 on 2026-10-01**, two identical batches, for S5 and D1 together
/// (`Db::open` no longer discards a store over an IO error; WAL recovery, the
/// role file and the witness slots sync before a boot trusts them; a sync with
/// nothing to flush draws no fault). Every universe reaches a verdict and the
/// three give-ups are gone. Set on the new count, the same convention.
const CONVERGED_FLOOR: u64 = 256;

/// Universes in which the byte-fingerprint oracle actually compared files,
/// MEASURED: 17 over these 256 universes at base `0x2EB1_0001`, reproduced on
/// two unpinned runs (apply-batching T4, 2026-08-20). `require:` alone only
/// catches the oracle going to zero; the rest of the range between 1 and 17 is
/// where it would quietly become decorative — a handful of comparisons out of
/// 256 is not a property, it is a rounding error that still turns the suite
/// green.
///
/// **A drop below this is a finding, not a number to lower.** It means the two
/// nodes' store write schedules diverge more often than they did — the workload
/// stopped producing single-record commits, or shipping got lumpier, or
/// something changed how records are grouped. Find out which before touching
/// this line; adjusting the floor to match the new reality is exactly how the
/// oracle would be retired without anyone deciding to retire it.
const FINGERPRINT_FLOOR: u64 = 17;

fn convergence_universe(sim: &mut Sim) -> Result<(), String> {
    let mut p = spawn_node(sim, "p", primary_cfg("p:1"), DbConfig::default())?;
    let mut f = spawn_node(sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default())?;

    // Drive commits interleaved with shipping (a moving target).
    for i in 0..40u64 {
        commit_widget(sim, &mut p, i)?;
        drive_rounds(sim, &mut [&mut p, &mut f], 3)?;
    }
    run_until(sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET)?;

    assert_converged(sim, &mut p, &mut f)?;
    assert_bytes_identical(sim, p, f)
}

// ------------------------------------------------------- property: seed

/// The seeding swarm's tenant-scoped, secondary-indexed table (hard rule 3).
/// Task 6c's carrier paths — `Seeder::rows`, `Msg::ScopedSnapshotRows` (tag
/// 14) and both follower refusals — move a scope across the wire, and a
/// follower that dropped it merges every tenant into tenant 0. Under an
/// unscoped schema no universe here can express that, so this table rides the
/// seeding property's own fault schedule.
///
/// Hand-written rather than `#[table(scoped_by = …)]`, matching
/// `ScopedSeedItem` above: this file's fixtures are hand-written `Row` impls,
/// and the macro's const assert wants the platform's `Principal`/`ApiError`
/// shape while these nodes drive `Db` directly with no `Ctx`.
///
/// `marker` derives from the tenant, so a leaked or collapsed row is
/// detectable by its CONTENT and not only by the key it was found under.
struct SeedLedger {
    id: Id<SeedLedger>,
    bucket: u32,
    marker: u64,
}

impl Row for SeedLedger {
    type Scope = SeedOrg;
    const TABLE: &'static str = "SeedLedger";
    const INDEXES: &'static [IndexDef<SeedLedger>] = &[IndexDef {
        name: "bucket",
        key: |r, out| r.bucket.encode(out),
    }];
    fn id(&self) -> Id<SeedLedger> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.bucket.encode(out);
        self.marker.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<SeedLedger> {
        let input = &mut &bytes[..];
        Ok(SeedLedger {
            id: Id::decode(input)?,
            bucket: u32::decode(input)?,
            marker: u64::decode(input)?,
        })
    }
}

impl charpente::Tenant for SeedOrg {
    const NAME: &'static str = "SeedOrg";
    type Who = str;
    type Error = io::Error;

    /// Always allows: this property is about the seed path carrying the
    /// scope, not about the app's membership check —
    /// `charpente/tests/tenant.rs` owns the refusing case.
    fn authorize(_tx: &charpente::Tx, _who: &str, _scope: Id<SeedOrg>) -> Result<(), io::Error> {
        Ok(())
    }
}

/// The two tenants, and the id space they SHARE. Ids collide across tenants
/// on purpose: a collapse to one scope is only expressible when two tenants
/// hold the same id, which is exactly why a single-tenant seed proves nothing.
const SEED_TENANTS: [u64; 2] = [1, 2];
const SEED_LEDGER_IDS: u64 = 5;
const SEED_MARKER_STRIDE: u64 = 1_000_000;

fn seed_marker(tenant: u64, id: u64) -> u64 {
    tenant * SEED_MARKER_STRIDE + id
}

fn seeding_schema() -> Schema {
    make_schema().table::<SeedLedger>()
}

/// `commit_widget`'s scoped counterpart, same best-effort contract: the two
/// tenants write the SAME id, one transaction each, so every seed the follower
/// takes carries two scopes for one id — and a crash can land between them.
/// One transaction per tenant because a transaction opens one scope (spec §8).
fn commit_scoped(sim: &Sim, node: &mut Node, i: u64) -> Result<(), String> {
    let id = i % SEED_LEDGER_IDS;
    for tenant in SEED_TENANTS {
        let Some(db) = node.db.as_mut() else {
            return Ok(());
        };
        let r = db.transact(|tx| {
            tx.open_scope(Id::<SeedOrg>::from_raw(tenant), "operator")?
                .put(&SeedLedger {
                    id: Id::from_raw(id),
                    bucket: (id % 3) as u32,
                    marker: seed_marker(tenant, id),
                });
            Ok::<(), io::Error>(())
        });
        match r {
            Ok(()) => {}
            Err(TxError::App(e)) => return Err(format!("scoped commit rejected: {e}")),
            Err(e) => {
                let msg = format!("{e:?}");
                if !is_fault_fallout(&msg) {
                    return Err(format!("scoped commit failed non-injected: {msg}"));
                }
                recover_node(sim, node)?;
            }
        }
    }
    Ok(())
}

/// The tenant oracle on the FOLLOWER's own store, after it converged through
/// the snapshot path: each tenant's slice holds its own rows under its own
/// scope. `assert_converged` already proves p and f agree byte for byte on
/// `scan_raw`, which catches a follower-only collapse; this catches the case
/// that check cannot see — a scope the seed path drops on both sides, or a
/// tenant that lands whole under another's key.
fn follower_kept_its_tenants(sim: &Sim, f: &Node) -> Result<(), String> {
    let fdb = f.db.as_ref().ok_or("f has no db")?;
    // Every read here is fault-classified before it is allowed to fail the
    // universe. These `f:`-tagged reads used to propagate with `?`, so an
    // INJECTED read error surfaced as "f: table store (redb): I/O error:
    // injected read error" — a fault reported as a tenancy violation, which is
    // a false RED. `converged_read`'s siblings have always been classified by
    // `assert_converged`'s retry loop (`Err(e) if is_fault_fallout(&e) =>
    // continue`); this oracle sits outside that loop and was never given the
    // same treatment. It takes `sim` precisely so it can give up instead
    // (docs/LEDGER.md 2026-09-07, surfaced at base 0x2EB1_0002 once the WAL
    // open-path barrier reshuffled this pinned base's universes).
    macro_rules! fallible {
        ($e:expr, $what:literal) => {
            match $e {
                Ok(v) => v,
                Err(e) if is_fault_fallout(&e.to_string()) => {
                    sim.give_up(format!(
                        "follower tenant oracle: {} hit fault fallout: {e}",
                        $what
                    ));
                    return Ok(());
                }
                Err(e) => return Err(format!("f: {e}")),
            }
        };
    }
    let snap = fallible!(fdb.snapshot(), "snapshot");
    let mut seen_any = false;
    for tenant in SEED_TENANTS {
        let rows = fallible!(snap.scan_scoped::<SeedLedger>(tenant), "tenant scan");
        for row in &rows {
            seen_any = true;
            if row.marker / SEED_MARKER_STRIDE != tenant {
                return Err(format!(
                    "follower's tenant {tenant} holds id {} marked for tenant {}",
                    row.id.raw(),
                    row.marker / SEED_MARKER_STRIDE
                ));
            }
        }
        // The index carries the scope prefix too (spec §5's worst failure
        // mode is a forgotten index prefix, and the seed rebuilds indexes).
        for bucket in 0..3u32 {
            for row in fallible!(
                snap.scan_by_scoped::<SeedLedger>(tenant, "bucket", &bucket),
                "tenant index scan"
            ) {
                if row.marker / SEED_MARKER_STRIDE != tenant {
                    return Err(format!(
                        "follower's tenant {tenant} index bucket {bucket} returned id {} marked \
                         for tenant {}",
                        row.id.raw(),
                        row.marker / SEED_MARKER_STRIDE
                    ));
                }
            }
        }
    }
    if seen_any {
        // Both tenants present at once is what makes the collapse expressible;
        // a universe whose faults left the ledger empty proves nothing here.
        let populated = SEED_TENANTS
            .iter()
            .filter(|&&t| {
                snap.scan_scoped::<SeedLedger>(t)
                    .is_ok_and(|rows| !rows.is_empty())
            })
            .count();
        if populated > 1 {
            sim.cover("tenant:seeded-multi-tenant");
        }
    }
    Ok(())
}

fn seeding_db_cfg() -> DbConfig {
    DbConfig {
        wal: WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 128,
        },
        store_path: "tables.redb".to_string(),
        retain_segments: 1,
        checkpoint_every: 8,
        event_retention: 65_536,
        migration_batch_size: 1000,
    }
}

#[test]
fn late_follower_seeds_below_the_gcd_base_and_converges() {
    run_universes_swarm_with!(
        0x2EB1_0002,
        256,
        no_media_corruption,
        |sim: &mut Sim| {
            let r = seeding_universe(sim);
            finish_universe(sim, r)
        },
        require: ["tenant:seeded-multi-tenant", "repl:logical-converged"],
        floor: [("repl:logical-converged", SEEDED_CONVERGED_FLOOR)]
    );
}

/// Universes that reached this property's verdict, MEASURED: 145 of 256 at base
/// `0x2EB1_0002` on 2026-09-06 (`repl:logical-converged`; `tenant:seeded-multi-
/// tenant` measured identically at 145 and is `require:`d above). The other 111
/// are 109 `repl:inconclusive` plus 2 `repl:seed-base-zero` — 145 + 109 + 2 =
/// 256 exactly, so this key is the whole of what "reached a verdict" means here.
/// `repl:seed-base-zero` is a third skip channel and is floored by nothing on
/// purpose: it fires when faults left the primary too small to move its log base
/// past 0, so the below-base seed path this property exists to test cannot
/// happen. At 2 of 256 it is negligible; if it grew it would drain THIS key,
/// which is the floor that would notice.
///
/// **Headroom: 2 binomial SEs below the measurement, unlike `CONVERGED_FLOOR`.**
/// That constant sits exactly ON its measured 167 and justifies it with "the run
/// is deterministic at a fixed base, so this number moves only when code does."
/// True for a byte-identical replay — but any change that shifts the fault
/// stream (a platform change, a workload change) re-rolls all 256 universes, and
/// the count is then a fresh binomial draw around the same rate. At p = 145/256
/// = 0.566, one SE on the count is sqrt(256 · 0.566 · 0.434) = 7.9, so a floor
/// set at the measurement itself would flap red about half the time on a change
/// that altered nothing real. 145 − 2·7.9 = 129.1 → 129: a re-roll of the same
/// distribution clears it with ~97.7% probability, while a genuine collapse of
/// the give-up rate (the 42.6% measured here climbing past ~50%) still fails.
/// **State the other side too**: this floor tolerates an **11.0% relative fall
/// in reach** (145 → 129) before it fires, and every 2-SE floor in this file
/// trades exactly that detection power for that false-red rate.
///
/// **On the two floor conventions now in this file** — this one and
/// `CONVERGED_FLOOR`'s zero-headroom pin — the reconciliation is written out at
/// `CONVERGED_FLOOR` itself, since that is the constant a reader arrives at
/// wondering why it differs.
///
/// **A drop below this is a finding, not a number to lower** — the same rule
/// `CONVERGED_FLOOR` carries: it means more universes are giving up before the
/// oracle runs, and which give-up cause grew is the question to answer first.
///
/// **The one open question behind this number** (task-3-report.md §10): its 109
/// give-ups are all `recover_node` exhaustion, and a 10× retry budget rescued
/// only 4 of them, which is consistent BOTH with disks the fault injector holds
/// unrecoverable and with a fault-triggered wedge the retry cannot clear. The
/// evidence gathered so far does not separate those, and this floor does not
/// depend on which it is: the count it pins moved 145 → 149 under that same 10×.
///
/// **Re-measured 2026-09-30 for redb 4.3.0**, two identical batches: 190 of
/// 256 (the parent commit under redb 4.1.0 measures 144, a control run). The
/// give-ups fell 111 → 28, all `:recover-exhausted:injected`, and
/// `repl:seed-base-zero` rose 1 → 38: universes that used to give up now
/// finish with a primary too small to move its base. 190 + 28 + 38 = 256. At
/// p = 190/256 = 0.742 one SE is 7.0, so 190 − 2·7.0 = 176.0 → 176, the same
/// convention; it tolerates a 7.4% relative fall.
///
/// **Re-set 2026-10-01** for S5 and D1 together, two identical batches: 198 of
/// 256 (190 at their parent, 796bff2). The give-ups fell 28 → 22, all
/// `:recover-exhausted:injected`, and `repl:seed-base-zero` reads 36, so
/// 198 + 22 + 36 = 256. At p = 198/256 = 0.773 one SE is 6.70, so
/// 198 − 2·6.70 = 184.6 → 184, the same convention; it tolerates a 7.1%
/// relative fall.
const SEEDED_CONVERGED_FLOOR: u64 = 184;

fn seeding_universe(sim: &mut Sim) -> Result<(), String> {
    let mut p = spawn_node_with_schema(
        sim,
        "p",
        primary_cfg("p:1"),
        seeding_db_cfg(),
        seeding_schema(),
    )?;
    // Commit far enough that checkpoint-driven GC moves the base past 0.
    for i in 0..60u64 {
        commit_widget(sim, &mut p, i)?;
        // A two-tenant scoped write beside every unscoped one, so the scope
        // rides the same crash schedule the rest of the seed does.
        commit_scoped(sim, &mut p, i)?;
    }
    let base = p.db.as_ref().map_or(0, |d| d.log_base());
    if base == 0 {
        // Under heavy faults the primary may have committed little; that
        // universe cannot exercise the below-base seed path — skip it.
        sim.cover("repl:seed-base-zero");
        return Ok(());
    }

    // A fresh, empty follower: its next needed seq (0) is below p's base, so
    // the session must take the snapshot path.
    let mut f = spawn_node_with_schema(
        sim,
        "f",
        follower_cfg("f:1", "p:1"),
        seeding_db_cfg(),
        seeding_schema(),
    )?;
    run_until(sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET)?;

    if total_seeds(&f) == 0 {
        return Err("the follower converged without ever taking the snapshot path".to_string());
    }
    // The follower seeded above 0 (not a replay from the log start).
    if f.db.as_ref().map_or(0, |d| d.log_base()) == 0 {
        return Err("a seeded follower's WAL base must be above 0".to_string());
    }
    assert_converged(sim, &mut p, &mut f)?;
    follower_kept_its_tenants(sim, &f)
}

/// Sim-found bug, seed 0xcaef83b2feb5edf5 (this file's seeding property):
/// `Seeder::finish` checkpoints the store at N and only then lays + barriers
/// the seed segment — a crash between the two leaves a checkpointed store
/// beside an EMPTY log directory, and `Db::open` let `Wal::open` start a
/// fresh log at 0, tripping the outran-durable-truth assert (a panic, not an
/// error). Fixed in `Db::open`: an empty log dir beside a durable horizon
/// proves nothing above the horizon was ever acked (segment names are
/// dir-barriered before any frame lands in them — fence 2), so open lays the
/// segment `finish` would have. Constructed deterministically here: the
/// vanished seed segment is emulated by removing every `wal/` file after a
/// checkpointed close.
#[test]
fn open_recovers_a_checkpointed_store_beside_an_empty_log_dir() {
    let sim = Sim::with_faults(0x2EB1_0008, FaultConfig::none());
    let h = sim.host("n");
    let mut db = Db::open(Box::new(h.disk()), DbConfig::default(), make_schema()).unwrap();
    for i in 0..5u64 {
        db.transact(|tx| {
            tx.put(&Widget {
                id: Id::from_raw(i),
                kind: 0,
                weight: i as i64,
            });
            Ok::<(), io::Error>(())
        })
        .unwrap();
    }
    db.checkpoint().unwrap();
    let applied = db.last_applied().unwrap();
    drop(db);

    let disk = h.disk();
    for name in disk.list("wal/").unwrap() {
        disk.remove(&name).unwrap();
    }
    disk.sync_dir("wal/").unwrap();

    let db = Db::open(Box::new(h.disk()), DbConfig::default(), make_schema()).unwrap();
    assert_eq!(db.last_applied(), Some(applied));
    assert_eq!(
        db.log_base(),
        applied + 1,
        "open must resume the log at the horizon + 1 boundary"
    );
}

#[test]
fn seed_lands_at_horizon_plus_one() {
    // Deterministic (quiet) check of the seed's landing invariant: right after
    // run_seed completes and before any frame ships, base == last_applied + 1.
    let sim = Sim::with_faults(0x2EB1_0003, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), seeding_db_cfg()).unwrap();
    for i in 0..60u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    assert!(
        p.db.as_ref().unwrap().log_base() > 0,
        "GC must move the base"
    );

    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), seeding_db_cfg()).unwrap();
    // Stop the instant the seed completes — before the resumed frames apply.
    run_until(
        &sim,
        &mut [&mut p, &mut f],
        |ns| ns[1].repl.stats().seeds >= 1,
        CONVERGE_BUDGET,
    )
    .unwrap();

    let fdb = f.db.as_ref().unwrap();
    let last = fdb
        .last_applied()
        .expect("a seeded follower has a durable position");
    assert_eq!(
        fdb.log_base(),
        last + 1,
        "the seed must land the WAL base at the horizon + 1 boundary"
    );
}

// -------------------------------------- fix round 2, N-3 / owed 1(a) fixture

struct SeedOrg;

struct ScopedSeedItem {
    id: Id<ScopedSeedItem>,
    n: u32,
}
impl Row for ScopedSeedItem {
    type Scope = SeedOrg;
    const TABLE: &'static str = "ScopedSeedItem";
    const INDEXES: &'static [IndexDef<ScopedSeedItem>] = &[];
    fn id(&self) -> Id<ScopedSeedItem> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.n.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<ScopedSeedItem> {
        let input = &mut &bytes[..];
        Ok(ScopedSeedItem {
            id: Id::decode(input)?,
            n: u32::decode(input)?,
        })
    }
}

fn scoped_seed_item_schema() -> Schema {
    Schema::new().table::<ScopedSeedItem>()
}

fn scoped_len_prefixed(bytes: &[u8], out: &mut Vec<u8>) {
    (bytes.len() as u32).encode(out);
    out.extend_from_slice(bytes);
}

/// A hand-encoded one-effect `0x05` (scoped Put) record for `ScopedSeedItem`
/// — `ScopedSeedItem` is unversioned (`FINGERPRINT: None`, the default for
/// a hand-written `Row`), so unlike `charpente-core`'s versioned fault-matrix
/// fixtures this needs no `SchemaVersion` adoption row first.
fn scoped_seed_item_put_record(scope: u64, id: u64, n: u32) -> Vec<u8> {
    let mut row_bytes = Vec::new();
    Row::encode_row(
        &ScopedSeedItem {
            id: Id::from_raw(id),
            n,
        },
        &mut row_bytes,
    );
    let mut record = Vec::new();
    1u32.encode(&mut record);
    record.push(5);
    scoped_len_prefixed(ScopedSeedItem::TABLE.as_bytes(), &mut record);
    scope.encode(&mut record);
    scoped_len_prefixed(&row_bytes, &mut record);
    record
}

/// Fix round 2, owed 1(a) (N-3): a real follower seeds a genuinely SCOPED
/// table from a primary over the wire — the exact path the re-review's
/// mutation 3 found untested (forcing `begin_seeding` to send tag 5 for
/// every table left `replication` 58/58 green, because no schema anywhere
/// in this file registered a scoped table). Tenant 0 (R35: a tenant, not
/// "no tenant") and tenant 2 are both in the primary's log before the
/// follower ever connects; the assertion reads the FOLLOWER's own stored
/// composite key bytes back through `scan_raw`, not a round trip through
/// the same encoder that wrote them.
#[test]
fn a_follower_seeds_a_scoped_table_and_stores_both_tenants_under_their_own_scope() {
    let sim = Sim::with_faults(0x6C02_0003, FaultConfig::none());
    let mut p = spawn_node_with_schema(
        &sim,
        "p",
        primary_cfg("p:1"),
        seeding_db_cfg(),
        scoped_seed_item_schema(),
    )
    .unwrap();
    {
        let db = p.db.as_mut().unwrap();
        let mut seq = db.last_applied().map_or(0, |s| s + 1);
        // Pad the log past several segment/checkpoint boundaries first — a
        // fresh (`None`-position) follower only snapshot-seeds against a
        // COMPACTED log (DESIGN §5's ladder); an uncompacted one just ships
        // ordinary `Frames`, and this test needs the real seed path.
        let mut records: Vec<(u64, Vec<u8>)> = (0..80)
            .map(|i| {
                let r = (seq, scoped_seed_item_put_record(1, 100 + i, i as u32));
                seq += 1;
                r
            })
            .collect();
        records.push((seq, scoped_seed_item_put_record(0, 5, 50)));
        seq += 1;
        records.push((seq, scoped_seed_item_put_record(2, 9, 90)));
        db.apply_shipped(&records).unwrap();
        db.checkpoint().unwrap();
        assert!(
            db.log_base() > 0,
            "the padding above must move GC's base for the snapshot-seed path to trigger"
        );
    }

    let mut f = spawn_node_with_schema(
        &sim,
        "f",
        follower_cfg("f:1", "p:1"),
        seeding_db_cfg(),
        scoped_seed_item_schema(),
    )
    .unwrap();
    run_until(
        &sim,
        &mut [&mut p, &mut f],
        |ns| ns[1].repl.stats().seeds >= 1,
        CONVERGE_BUDGET,
    )
    .unwrap();

    let fdb = f.db.as_ref().unwrap();
    let rows = fdb
        .snapshot()
        .unwrap()
        .scan_raw(ScopedSeedItem::TABLE, true)
        .unwrap();
    assert_eq!(
        rows.len(),
        82,
        "80 padding rows plus tenant 0's and tenant 2's"
    );
    let mut expect_0 = Vec::new();
    ScopedSeedItem {
        id: Id::from_raw(5),
        n: 50,
    }
    .encode_row(&mut expect_0);
    let mut expect_2 = Vec::new();
    ScopedSeedItem {
        id: Id::from_raw(9),
        n: 90,
    }
    .encode_row(&mut expect_2);
    assert!(
        rows.contains(&(0u64, 5u64, expect_0)),
        "tenant 0's row must be stored under its own scope, not dropped or collapsed"
    );
    assert!(
        rows.contains(&(2u64, 9u64, expect_2)),
        "tenant 2's row must be stored under its own scope, not dropped or collapsed"
    );
}

// -------------------------------------------------- property: pin holds GC

#[test]
fn live_lagging_session_pins_gc() {
    let sim = Sim::with_faults(0x2EB1_0004, FaultConfig::none());
    let db_cfg = DbConfig {
        wal: WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 128,
        },
        store_path: "tables.redb".to_string(),
        retain_segments: 1,
        checkpoint_every: 4,
        event_retention: 65_536,
        migration_batch_size: 1000,
    };
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), db_cfg.clone()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), db_cfg.clone()).unwrap();

    // Establish a live, caught-up session.
    for i in 0..8u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    let f_pinned = f.db.as_ref().unwrap().last_applied().unwrap();

    // Partition, then drive the primary far past several checkpoint boundaries.
    // The session stays live (a partition drops packets, it doesn't close the
    // conn), so its pin must hold the log at f's acked position + 1.
    sim.partition("p", "f");
    for i in 8..48u64 {
        commit_widget(&sim, &mut p, i).unwrap();
        // Let the primary ship (into the void) and checkpoint/GC.
        drive_rounds(&sim, &mut [&mut p, &mut f], 4).unwrap();
    }
    let p_last = p.db.as_ref().unwrap().last_applied().unwrap();
    assert!(
        p_last > f_pinned + 8,
        "the primary must have advanced well past the follower ({p_last} vs {f_pinned})"
    );
    assert!(
        p.db.as_ref().unwrap().log_base() <= f_pinned + 1,
        "the pin must hold the base at the lagging follower's position + 1 (base {}, follower {f_pinned})",
        p.db.as_ref().unwrap().log_base()
    );
    // The physical check, not just the Wal's bookkeeping: list the dir and
    // parse the lowest retained segment's base off its file name.
    let names = p.host.disk().list("wal/").unwrap();
    let physical_base: u64 = names
        .first()
        .and_then(|n| n["wal/".len()..n.len() - ".log".len()].parse().ok())
        .expect("the primary retains at least one segment");
    assert!(
        physical_base <= f_pinned + 1,
        "the dir itself must retain the pinned segments (lowest on disk {physical_base}, \
         pinned at {})",
        f_pinned + 1
    );

    // Crash the follower: its conn tears down, the primary reaps the session,
    // and the released pin lets GC advance past the old position. GC only runs
    // at a checkpoint, so keep committing (the cadence) to drive it forward.
    f.host.crash_now();
    for i in 48..64u64 {
        commit_widget(&sim, &mut p, i).unwrap();
        drive_rounds(&sim, &mut [&mut p], 2).unwrap();
    }
    assert!(
        p.db.as_ref().unwrap().log_base() > f_pinned + 1,
        "the base must advance past the released pin once the dead session is reaped (base {}, was pinned at {})",
        p.db.as_ref().unwrap().log_base(),
        f_pinned + 1,
    );

    // Repair, then bring the follower back from a FRESH data dir: it fell below
    // the released base, and Design B refuses a POSITIONED below-base rejoin
    // (pinned by a_same_timeline_laggard_below_the_base_is_refused_not_reseeded)
    // — the operator's remedy is exactly this, a wiped data dir that Hellos with
    // an empty position and snapshot-seeds. It must re-seed and converge.
    sim.repair("p", "f");
    f.host.restart();
    {
        let disk = f.host.disk();
        for name in disk.list("").unwrap() {
            let _ = disk.remove(&name);
        }
        let _ = disk.sync_dir("");
        let _ = disk.sync_dir("wal/");
    }
    f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), db_cfg).unwrap();
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert!(
        f.repl.stats().seeds >= 1,
        "a fresh-data-dir follower below the base must re-seed on rejoin"
    );
    assert_converged(&sim, &mut p, &mut f).unwrap();
}

/// Regression (final replication review, finding 2): a peer that opens a conn
/// to the replication port and never sends `Hello` (or a control verb) used
/// to park in `AwaitHello` forever — `pin_floor()` is 0 in that phase, and
/// nothing reaped the session, so it pinned GC at the base indefinitely.
/// Mirrors `live_lagging_session_pins_gc`'s pin/GC assertions, but the peer
/// here never speaks at all.
#[test]
fn a_silent_await_hello_session_stops_pinning_gc_after_its_deadline() {
    let sim = Sim::with_faults(0x2EB1_0010, FaultConfig::none());
    let db_cfg = DbConfig {
        wal: WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 128,
        },
        store_path: "tables.redb".to_string(),
        retain_segments: 1,
        checkpoint_every: 4,
        event_retention: 65_536,
        migration_batch_size: 1000,
    };
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), db_cfg).unwrap();

    // A peer that connects and never sends anything — no Hello, no control
    // verb. Kept alive for the whole test: dropping it would close the wire
    // and let the ordinary dead-session path (drain() sees EOF) reap it,
    // which is not what this test is proving.
    let _silent = sim.host("silent").net().connect("p:1").unwrap();

    // Accept the conn via pump_io directly rather than drive_rounds/
    // scheduler_round: with nothing else scheduled on this quiet primary, the
    // scheduler's "nothing runnable, jump to the next event" fallback would
    // otherwise fast-forward virtual time straight to the arrival deadline on
    // its very first idle round (the deadline is the only pending event),
    // reaping the session before this test can observe the pin holding.
    // pump_io's own accept-loop recomputes the pin (0) without risking that.
    p.repl.pump_io(sim.now().as_nanos());

    // Commit past several checkpoint boundaries — GC is purely a function of
    // commits-since-checkpoint, not of stepping the scheduler, so no further
    // driving is needed to prove the pin holds. Mirrors
    // live_lagging_session_pins_gc's pin/GC assertion.
    for i in 0..16u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    assert_eq!(
        p.db.as_ref().unwrap().log_base(),
        0,
        "a fresh AwaitHello session must pin GC at 0 before its arrival deadline"
    );

    // Advance virtual time past the deadline and drive the primary: now the
    // session itself is due, so the scheduler reaps it instead of idling.
    sim.clock()
        .sleep(Duration::from_nanos(charpente::repl::HELLO_DEADLINE_NANOS));
    drive_rounds(&sim, &mut [&mut p], 4).unwrap();

    // Keep committing so the checkpoint cadence actually runs GC again.
    for i in 16..32u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    assert!(
        p.db.as_ref().unwrap().log_base() > 0,
        "the silent AwaitHello session's deadline must have released the pin (base stuck at {})",
        p.db.as_ref().unwrap().log_base()
    );
}

/// Regression (0b re-review M-2): `Repl::step` reaps dead sessions at the top
/// of the step but recomputes the pin right after `service` (and `pump_io`
/// recomputes over the dead session too), so a session REFUSED during this
/// step is still present with `cursor = 0, acked = None` and pins GC at 0
/// until a step reaches the reap. Pre-existing on every refusal path; task 3's
/// seed-while-verifying refusal is the first a peer retries indefinitely.
///
/// Bounded, not permanent — the reap ends it, measured `0 -> 0 -> 0 -> MAX`.
/// Each occurrence costs one checkpoint's GC. Delayed GC, never wrong GC.
///
/// Uses the peer-secret refusal because it needs no new code — the point is
/// the pin, not which refusal caused it. Drives ONE round at a time and stops
/// the instant the refusal lands, because a *larger* fixed round count has
/// already been reaped by the time it checks, and a bare count of 1 would
/// assume the refusal happened rather than prove it. The commit loop then
/// checkpoints WITHOUT stepping the replicator, so GC runs while the dead
/// session is still present.
#[test]
fn a_refused_session_does_not_pin_gc() {
    let sim = Sim::with_faults(0x2EB1_011B, FaultConfig::none());
    let db_cfg = DbConfig {
        wal: WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 128,
        },
        store_path: "tables.redb".to_string(),
        retain_segments: 1,
        checkpoint_every: 4,
        event_retention: 65_536,
        migration_batch_size: 1000,
    };
    let mut cfg = primary_cfg("p:1");
    cfg.peer_secret = Some("right".to_string());
    let mut p = spawn_node(&sim, "p", cfg, db_cfg).unwrap();

    // A peer that presents the wrong secret: refused, session marked dead,
    // still present when this step recomputes the pin. Raw-conn shape copied
    // from `a_long_invalid_listen_address_is_truncated_in_the_refusal`
    // (replication.rs ~:5076), the file's idiom for driving a Hello by hand.
    let schema_digest = p.db.as_ref().unwrap().schema_digest();
    let conn = sim
        .host("wrong")
        .net()
        .connect("p:1")
        .expect("connect to the primary's listener");
    conn.send(&encode(&Msg::Hello {
        proto: PROTO_VERSION,
        listen: Some("wrong:1".to_string()),
        position: None,
        secret: Some("wrong".to_string()),
        schema_digest,
    }))
    .unwrap();

    // Drive ONE round at a time and stop the instant the refusal reaches the
    // client. At that moment the session was marked dead inside this step's
    // `service` and `recompute_pin` has just run with it still present — that
    // IS the window. Driving further lets `Repl::step`'s pre-existing
    // top-of-step `sessions.retain(|s| !s.dead)` reap it and restore the pin —
    // a *larger* fixed round count would already have missed the window, and
    // a bare count of 1 would assume it landed there rather than prove it
    // (see the `refused` assert below).
    let mut buf = vec![0u8; MAX_MESSAGE_BYTES];
    let mut refused = false;
    for _ in 0..64 {
        drive_rounds(&sim, &mut [&mut p], 1).unwrap();
        if let Ok(Some(n)) = conn.recv(&mut buf) {
            assert!(
                matches!(decode(&buf[..n]).unwrap(), Msg::Refuse { .. }),
                "expected the peer-secret refusal"
            );
            refused = true;
            break;
        }
    }
    assert!(refused, "precondition: the peer must have been refused");

    // Commit past several checkpoint boundaries WITHOUT driving the replicator
    // again. GC is a function of commits-since-checkpoint and `commit_widget`
    // calls `db.transact` directly (replication.rs:515-519) without stepping
    // the scheduler, so this checkpoints while the dead session is still
    // present.
    for i in 0..16u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    assert!(
        p.db.as_ref().unwrap().log_base() > 0,
        "a refused (dead) session must not hold the retention pin; base stuck at {}",
        p.db.as_ref().unwrap().log_base()
    );
}

// -------------------------------------------------- property: divergence

#[test]
fn diverged_follower_is_refused_by_name() {
    let sim = Sim::with_faults(0x2EB1_0005, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();

    for i in 0..10u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    let p_last_before = p.db.as_ref().unwrap().last_applied();

    // Stop shipping, then fork the follower: a wrongly-promoted node accepting
    // a write the primary never had.
    sim.partition("p", "f");
    {
        let fdb = f.db.as_mut().unwrap();
        fdb.allow_writes();
        fdb.transact(|tx| {
            tx.put(&Widget {
                id: Id::from_raw(999),
                kind: 0,
                weight: 1,
            });
            Ok::<(), io::Error>(())
        })
        .expect("the forking write commits locally");
    }

    // Repair and force a reconnect (crash-restart the follower so it re-Hellos
    // with its forked, ahead position). The primary must refuse by name; the
    // follower surfaces it as an io::Error from step().
    sim.repair("p", "f");
    recover_node(&sim, &mut f).unwrap();
    let err = run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET)
        .expect_err("a diverged rejoin must surface as an error, never a merge");
    // Needle + known-value, not byte-exact (error-suite T8's CRC carve-out):
    // `diverged_ahead` embeds the follower's frame CRC, which this test
    // neither computes nor receives — but every OTHER format arg is known
    // (the fork lands at seq p_last_before+1; the primary's last is
    // unchanged), so the reconstruction covers everything except the two
    // `{:#010x}` crc values.
    let pos_seq = p_last_before.expect("the primary has committed records") + 1;
    let last = p_last_before.expect("the primary has committed records");
    assert!(
        err.contains(&format!(
            "diverged: the follower is ahead at seq {pos_seq} (crc "
        )),
        "refusal must name the follower's forked seq {pos_seq}: {err}"
    );
    assert!(
        err.contains(&format!(
            "but this primary's last durable position is seq {last} (crc n/a) — the follower \
             holds records this primary never had (or lost), a forked timeline that can never \
             be merged; re-seed the follower from a fresh data dir (stop it, delete its data \
             dir, and restart it configured as a follower of this primary so it snapshot-seeds \
             from it, never with a primary config: the durable role file that outranks config \
             is deleted with the data dir, so a cleared node started as a primary comes up as a \
             second writable primary)"
        )),
        "refusal must name the primary's last durable seq {last} and the fix: {err}"
    );
    // Nothing was merged: the primary's state is unchanged.
    assert_eq!(
        p.db.as_ref().unwrap().last_applied(),
        p_last_before,
        "the primary must not have merged any of the forked follower's state"
    );
}

/// Like `scheduler_round`'s step loop, but returns the raw `io::Error`
/// instead of `step_node`'s stringified wrapper — the one assertion that
/// needs to inspect the error itself (`Refusal::of`), not just its text.
/// No fault handling (only meant for a `FaultConfig::none()` scenario): the
/// first node whose `step` errors ends the loop.
fn run_until_io_error(sim: &Sim, nodes: &mut [&mut Node], budget: u32) -> io::Result<()> {
    for _ in 0..budget {
        let now = sim.now().as_nanos();
        for node in nodes.iter_mut() {
            node.repl.pump_io(now);
        }
        let mut worked = false;
        for node in nodes.iter_mut() {
            let has_work = match &node.db {
                Some(db) => node.repl.has_work(db, now),
                None => false,
            };
            if has_work {
                let db = node.db.as_mut().expect("checked Some above");
                node.repl.step(db, &node.clock)?;
                worked = true;
            }
        }
        if worked {
            continue;
        }
        let mut next = sim.next_wakeup();
        for node in nodes.iter() {
            if let Some(d) = node.repl.next_deadline(now)
                && d > now
            {
                next = Some(next.map_or(d, |x| x.min(d)));
            }
        }
        match next {
            Some(t) if t > now => sim.clock().sleep(Duration::from_nanos(t - now)),
            _ => return Ok(()), // quiescent, no error — the caller decides what that means
        }
    }
    Ok(())
}

/// A diverged follower's OWN `Repl::step` error is a `Refusal` (payload-
/// shapes spec §5): restarting the follower on the same data dir cannot fix
/// a forked timeline, only a fresh-data-dir re-seed can (GUIDE §10), so it
/// must be told apart from a platform IO failure the same way the primary's
/// symmetric refusal already is. `run_until`'s `String` (as
/// `diverged_follower_is_refused_by_name` uses) throws the `io::Error` away
/// before a test could ask `Refusal::of` about it, so this drives the same
/// fork scenario through `run_until_io_error` instead, which keeps it.
#[test]
fn a_diverged_followers_own_step_error_is_a_refusal() {
    let sim = Sim::with_faults(0x2EB1_0006, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();

    for i in 0..10u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();

    // Fork the follower exactly as `diverged_follower_is_refused_by_name`
    // does: a wrongly-promoted node accepting a write the primary never had.
    sim.partition("p", "f");
    {
        let fdb = f.db.as_mut().unwrap();
        fdb.allow_writes();
        fdb.transact(|tx| {
            tx.put(&Widget {
                id: Id::from_raw(999),
                kind: 0,
                weight: 1,
            });
            Ok::<(), io::Error>(())
        })
        .expect("the forking write commits locally");
    }
    sim.repair("p", "f");
    recover_node(&sim, &mut f).unwrap();

    let err = run_until_io_error(&sim, &mut [&mut p, &mut f], CONVERGE_BUDGET)
        .expect_err("a diverged rejoin must surface as an error, never a merge");
    assert!(
        err.to_string()
            .contains("diverged: the follower is ahead at seq"),
        "the step error must be the same divergence refusal: {err}"
    );
    assert!(
        charpente_core::db::Refusal::of(&err).is_some(),
        "a diverged follower's own step error must be a Refusal — a restart on the same data \
         dir cannot fix it, only a fresh-data-dir re-seed can: {err}"
    );
}

// -------------------------------------------- property: seeded-follower frontier

/// Durably flips one byte of the highest-seq WAL frame's stored CRC on a host,
/// so recovery reads that frame as torn and cuts the log just below it — a
/// deterministic stand-in for the `bit_flip` fault the swarm `wal_corruption`
/// property hits at random. When the store's durable checkpoint sits below the
/// cut, this is an above-horizon cut (recovery succeeds, silently losing the
/// tail), not a below-horizon hard error. Frame layout (log.rs): a 16-byte
/// header `seq(8) plen(4) crc(4)` then `plen` payload bytes.
fn corrupt_last_wal_frame(host: &SimHost) -> Result<(), String> {
    let disk = host.disk();
    let mut names = disk.list("wal/").map_err(|e| e.to_string())?;
    names.sort();
    let seg = names.last().ok_or("no wal segment to corrupt")?;
    let file = disk.open(seg).map_err(|e| e.to_string())?;
    let len = file.len().map_err(|e| e.to_string())?;
    let mut offset = 0u64;
    let mut last_frame: Option<u64> = None;
    while offset + 16 <= len {
        let mut header = [0u8; 16];
        file.read_at(offset, &mut header)
            .map_err(|e| e.to_string())?;
        let plen = u64::from(u32::from_le_bytes(
            header[8..12].try_into().expect("4 bytes"),
        ));
        if plen == 0 || offset + 16 + plen > len {
            break;
        }
        last_frame = Some(offset);
        offset += 16 + plen;
    }
    let frame = last_frame.ok_or("wal segment holds no complete frame")?;
    // Flip a bit in the stored CRC (header bytes 12..16): frame_crc(seq,
    // payload) will no longer match, so read_frame reads this frame as torn.
    let crc_off = frame + 12;
    let mut b = [0u8; 1];
    file.read_at(crc_off, &mut b).map_err(|e| e.to_string())?;
    b[0] ^= 0xff;
    file.write_at(crc_off, &b).map_err(|e| e.to_string())?;
    file.sync().map_err(|e| e.to_string())?;
    Ok(())
}

/// Finding-1 regression (T2 report "Finding 1", seed 0xd4d9c59769a6738c): a
/// follower seeded at frontier F, with ZERO frames shipped since, reconnecting
/// to a primary that lost its tail below F (a legal above-horizon cut) must be
/// REFUSED by name — never silently re-seeded from the lossy primary, which
/// would wipe the converged survivor's data. Before the fix the follower
/// Hello'd `position: None` (its frontier predates its own WAL, so `read_log`
/// refused below-base), and the compacted-log primary answered `Snapshot`,
/// wiping the survivor. The frontier-CRC fallback lets it Hello `(F, crc)`, so
/// the `pos_seq > last` diverged-ahead arm fires. Deterministic: the cut is
/// forced by corrupting the primary's tail frame and crashing (its store
/// reverts to the checkpoint, recovery cuts the torn tail above the horizon).
#[test]
fn a_seeded_follower_with_zero_frames_refuses_a_lossy_primary() {
    let sim = Sim::with_faults(0x2EB1_0011, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), seeding_db_cfg()).unwrap();
    // Commit past several checkpoint boundaries (checkpoint_every 8): the log
    // compacts (base > 0) and a durable store horizon forms below the tail.
    for i in 0..30u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    let f_frontier =
        p.db.as_ref()
            .unwrap()
            .last_applied()
            .expect("the primary has records");
    assert!(
        p.db.as_ref().unwrap().log_base() > 0,
        "the primary's log must be compacted (the seed-below-base precondition)"
    );

    // Seed a fresh follower; stop the instant the seed lands, before any frame
    // ships (the primary holds nothing above the frontier, so none would).
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), seeding_db_cfg()).unwrap();
    run_until(
        &sim,
        &mut [&mut p, &mut f],
        |ns| ns[1].repl.stats().seeds >= 1,
        CONVERGE_BUDGET,
    )
    .unwrap();
    {
        let fdb = f.db.as_ref().unwrap();
        assert_eq!(
            fdb.last_applied(),
            Some(f_frontier),
            "the follower seeded at the primary's frontier"
        );
        assert_eq!(
            fdb.log_base(),
            f_frontier + 1,
            "the seed lays the WAL base above the frontier — exactly the below-base fallback case"
        );
    }
    let fp_before = data_fingerprint(&f.host).unwrap();

    // Partition so nothing ships while the primary is bricked, then force an
    // above-horizon cut: corrupt the tail frame, crash (unsynced store applies
    // collapse back to the checkpoint), recover (recovery cuts the torn tail).
    sim.partition("p", "f");
    corrupt_last_wal_frame(&p.host).unwrap();
    p.host.crash_now(); // crashed disk: the Db drop below cannot gracefully persist
    p.db = None;
    p.host.restart(); // collapse unsynced (store reverts to the checkpoint), reboot
    reopen(&sim, &mut p).expect("an above-horizon cut recovers cleanly, not RESTORE_NEEDED");
    let p_last_after =
        p.db.as_ref()
            .unwrap()
            .last_applied()
            .expect("the primary recovered with records");
    assert!(
        p_last_after < f_frontier,
        "the cut must leave the primary below the follower's frontier (p={p_last_after}, f={f_frontier})"
    );
    assert!(
        p.db.as_ref().unwrap().log_base() > 0,
        "the primary's log stays compacted after the cut (so the buggy path would re-seed)"
    );

    // Reconnect: the follower must be refused by name (diverged ahead), never
    // re-seeded. Its step() surfaces the refusal as an io::Error.
    sim.repair("p", "f");
    let err = run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET)
        .expect_err("a seeded follower ahead of a lossy primary must be refused, never re-seeded");
    // Needle + known-value, not byte-exact (error-suite T8's CRC carve-out):
    // `diverged_ahead` embeds a frame CRC this test neither computes nor
    // receives — every other format arg (the follower's frontier, the
    // primary's post-cut last) is known, so the reconstruction covers
    // everything except the two `{:#010x}` crc values.
    assert!(
        err.contains(&format!(
            "diverged: the follower is ahead at seq {f_frontier} (crc "
        )),
        "refusal must name the follower's frontier {f_frontier}: {err}"
    );
    assert!(
        err.contains(&format!(
            "but this primary's last durable position is seq {p_last_after} (crc n/a) — the \
             follower holds records this primary never had (or lost), a forked timeline that \
             can never be merged; re-seed the follower from a fresh data dir (stop it, delete \
             its data dir, and restart it configured as a follower of this primary so it \
             snapshot-seeds from it, never with a primary config: the durable role file that \
             outranks config is deleted with the data dir, so a cleared node started as a \
             primary comes up as a second writable primary)"
        )),
        "refusal must name the primary's last {p_last_after} and the fix: {err}"
    );
    // No wipe: the follower still holds exactly what it seeded.
    assert_eq!(
        total_seeds(&f),
        1,
        "the follower must not have re-seeded (seeds={})",
        total_seeds(&f)
    );
    assert_eq!(
        data_fingerprint(&f.host).unwrap(),
        fp_before,
        "the follower's data must be untouched — no silent re-seed from the lossy primary"
    );
}

/// The healthy path this fix is most at risk of regressing: a seeded follower
/// that RESTARTS before shipping any frame must still resume against its
/// (intact) primary — the persisted frontier CRC survives the restart (piece
/// 2) and its `(F, crc)` Hello matches the primary's retained frame, so the
/// ladder resumes with `Frames`, never a spurious refuse or re-seed. Then real
/// frames ship and it converges.
#[test]
fn a_restarted_seeded_follower_resumes_without_re_seeding() {
    let sim = Sim::with_faults(0x2EB1_0012, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), seeding_db_cfg()).unwrap();
    for i in 0..30u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    assert!(
        p.db.as_ref().unwrap().log_base() > 0,
        "the primary's log must be compacted"
    );

    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), seeding_db_cfg()).unwrap();
    run_until(
        &sim,
        &mut [&mut p, &mut f],
        |ns| ns[1].repl.stats().seeds >= 1,
        CONVERGE_BUDGET,
    )
    .unwrap();
    let frontier = f.db.as_ref().unwrap().last_applied().expect("f seeded");

    // Restart the follower before any frame ships: it reopens at the frontier,
    // its persisted seed_frontier_crc intact — this is the across-restart
    // durability the fallback depends on.
    recover_node(&sim, &mut f).unwrap();
    let seeds_after_restart = total_seeds(&f);
    assert_eq!(
        seeds_after_restart, 1,
        "the restart itself must not re-seed"
    );
    assert_eq!(
        f.db.as_ref().unwrap().last_applied(),
        Some(frontier),
        "the follower reopened at its frontier"
    );
    assert_eq!(
        f.db.as_ref()
            .unwrap()
            .seed_frontier()
            .unwrap()
            .map(|(s, _)| s),
        Some(frontier),
        "the seed frontier must survive the restart"
    );

    // One more commit on the primary (kept below the checkpoint cadence so the
    // frontier frame stays retained — a further checkpoint's GC would evict it
    // and re-seeding below the base would be the *correct* answer, not the
    // resume this test pins). The follower reconnects with its (frontier, crc)
    // fallback, the primary resumes it (CRC matches its retained frame), and
    // the new frame ships — converging one past the frontier.
    commit_widget(&sim, &mut p, 30).unwrap();
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_converged(&sim, &mut p, &mut f).unwrap();
    assert!(
        f.db.as_ref().unwrap().last_applied() > Some(frontier),
        "frames must have shipped past the frontier after the resume"
    );
    assert_eq!(
        total_seeds(&f),
        1,
        "the follower resumed — it must NOT have taken a spurious second seed (seeds={})",
        total_seeds(&f)
    );
}

/// The seed frontier CRC vouches for exactly ONE seq — the seed's. A seeded
/// follower that shipped frames PAST its seed and then lost its whole log
/// (checkpointed store beside an empty log dir — `Db::open` re-lays the seed
/// segment at horizon + 1) must NOT pair the stale seed CRC with its advanced
/// `last_applied`: that Hello would be refused as diverged-crc by a healthy
/// primary (a false refusal — the brief's named regression risk). The honest
/// answer is an empty position → re-seed, which converges.
#[test]
fn a_seeded_follower_that_advanced_then_lost_its_log_reseeds_not_refused() {
    let sim = Sim::with_faults(0x2EB1_0013, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), seeding_db_cfg()).unwrap();
    for i in 0..30u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    assert!(p.db.as_ref().unwrap().log_base() > 0, "compacted");

    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), seeding_db_cfg()).unwrap();
    run_until(
        &sim,
        &mut [&mut p, &mut f],
        |ns| ns[1].repl.stats().seeds >= 1,
        CONVERGE_BUDGET,
    )
    .unwrap();
    let seed_seq = f.db.as_ref().unwrap().last_applied().expect("f seeded");

    // Ship real frames past the seed, then checkpoint f so its store horizon
    // durably advances beyond the seed frontier.
    commit_widget(&sim, &mut p, 30).unwrap();
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    let advanced = f.db.as_ref().unwrap().last_applied().unwrap();
    assert!(
        advanced > seed_seq,
        "a frame must have shipped past the seed"
    );
    f.db.as_mut().unwrap().checkpoint().unwrap();

    // Lose f's entire log: graceful close, delete every segment (dir-synced),
    // power-cycle (frees the listener; everything above is durable, so the
    // fault-free collapse drops nothing), reopen — Db::open lays an empty
    // segment at horizon + 1, so f's stale seed frontier (seed_seq) no longer
    // matches its last_applied (advanced).
    f.db = None;
    let disk = f.host.disk();
    for name in disk.list("wal/").unwrap() {
        disk.remove(&name).unwrap();
    }
    disk.sync_dir("wal/").unwrap();
    f.host.restart();
    reopen(&sim, &mut f).unwrap();
    assert_eq!(f.db.as_ref().unwrap().last_applied(), Some(advanced));
    assert_eq!(
        f.db.as_ref().unwrap().log_base(),
        advanced + 1,
        "open lays the segment at horizon + 1 over the vanished log"
    );

    // Reconnect (drive explicitly — last_applied already matches, so a
    // converged-predicate wait would return before the Hello even travels):
    // the rejoin must never surface a refusal (the stale CRC paired with the
    // advanced seq would draw diverged-crc from this healthy primary); the
    // honest empty position re-seeds instead.
    drive_rounds(&sim, &mut [&mut p, &mut f], 256).expect("a healthy rejoin must never be refused");
    assert_eq!(
        f.repl.stats().seeds,
        1,
        "the log-less follower re-seeds (cannot prove its advanced position)"
    );
    assert_converged(&sim, &mut p, &mut f).unwrap();
}

// ------------------------------------------- property: diverged BEHIND (Design B)

/// Reconstructs `session::diverged_behind`'s exact text from scenario-known
/// values (error-suite T8's byte-exact family — `diverged_behind` embeds no
/// CRC, so unlike `diverged_ahead`/`diverged_crc` it fully reconstructs).
/// `session::diverged_behind` stays a private fn (plan-review correction: not
/// widened to `pub(crate)`), so this is typed out rather than called — typing
/// it out is what actually pins the text; calling the fn again would just
/// check it against itself.
fn expected_diverged_behind(pos_seq: u64, base: u64) -> String {
    format!(
        "diverged: the follower holds a proven record at seq {pos_seq}, but this primary has \
         GC'd every record below its retained base {base} — with no frame at or below seq \
         {pos_seq} to compare, this primary cannot prove the follower's records are a prefix of \
         its own timeline, and re-seeding here would silently overwrite whatever the follower \
         holds; if the follower merely lagged past the retained window, re-seed it deliberately \
         from a fresh data dir (stop it, delete its data dir, and restart it configured as a \
         follower of this primary so it snapshot-seeds from it, never with a primary config: the \
         durable role file that outranks config is deleted with the data dir, so a cleared node \
         started as a primary comes up as a second writable primary); if instead this primary \
         forked below the follower (recovered past an above-horizon corruption cut and kept \
         serving), promote the follower and re-seed this node from it the same way, configured \
         as a follower of the promoted node"
    )
}

/// The diverged-BEHIND re-seed hole (dossier
/// `.superpowers/sdd/diverged-behind-investigation.md`, Noah-ruled Design B,
/// 2026-07-16): a converged follower holding a PROVEN record at seq N
/// reconnects to a primary that forked BELOW N. The primary recovered past an
/// above-horizon corruption cut (dropping its own frame at N), stayed Primary,
/// re-appended PAST N on a new timeline, and GC'd past N. The follower's
/// `Hello (N, crc_f(N))` now has `pos_seq <= last` (the diverged-AHEAD guard is
/// bypassed) and `N < base` (the CRC-compare arm is skipped) — the single
/// unguarded arm. Before Design B the primary answered `Snapshot`, silently
/// WIPING the follower's converged acked record at N (it lives on the follower,
/// gone from the forked primary). Design B refuses by name instead. The
/// follower's data must be untouched and no new seed may begin. Deterministic,
/// `FaultConfig::none()`.
#[test]
fn a_converged_follower_below_a_forked_primarys_base_is_refused_not_rewound() {
    let sim = Sim::with_faults(0x2EB1_0014, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), seeding_db_cfg()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), seeding_db_cfg()).unwrap();

    // Converge f via real frames: it holds frame N in its own retained log
    // (path (a) of hello_position — a genuine acked record, not a seed
    // frontier), so its Hello is (N, crc_f(N)).
    for i in 0..30u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    let n = f.db.as_ref().unwrap().last_applied().expect("f converged");
    assert!(
        p.db.as_ref().unwrap().log_base() > 0,
        "the primary's log must be compacted (a store horizon below the tail)"
    );

    // Partition, then fork the primary BELOW N: corrupt its tail frame (seq N),
    // crash, recover — an above-horizon cut drops frame N, leaving p.last = N-1.
    sim.partition("p", "f");
    corrupt_last_wal_frame(&p.host).unwrap();
    p.host.crash_now();
    p.db = None;
    p.host.restart();
    // The GUIDE §10 operator escape hatch, and the reason this test needs it
    // is itself the finding: since the witness boot gate landed (M2
    // sub-project 0b task 5), p cannot reach this fork on its own. It shipped
    // to f above, so f is a durable witness; p restarts write-stopped and the
    // partition means f can never answer, so the re-advance below would be
    // refused outright and p would sit at the cut forever.
    //
    // That is the gate working — it makes THIS divergence much harder to
    // reach, which is the whole point of 0b. But diverged-behind stays
    // reachable exactly as spec §4 warns: an operator who clears the witness
    // file to get a stuck primary writable again re-opens the gap. So the
    // clear is not a workaround for the test, it is the scenario — the
    // supported operator action whose cost §4 states out loud, and this test
    // is what proves the refusal still holds after someone takes it.
    charpente::repl::witnesses::clear_witnesses(&p.host.disk()).unwrap();
    reopen(&sim, &mut p).expect("an above-horizon cut recovers cleanly, not RESTORE_NEEDED");
    let p_after_cut = p.db.as_ref().unwrap().last_applied().expect("p recovered");
    assert!(
        p_after_cut < n,
        "the cut must drop the primary below the follower's frontier (p={p_after_cut}, f={n})"
    );

    // Re-advance the primary PAST N on the forked timeline (distinct ids 100..
    // so the re-appended frame at seq N is genuinely different bytes than f's),
    // then checkpoint+GC so base > N. p's restart cleared every session, so GC
    // is unpinned and the base moves freely — the exact shape that reaches the
    // below-base arm with pos_seq <= last (never the diverged-AHEAD guard).
    for i in 100..150u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    p.db.as_mut().unwrap().checkpoint().unwrap();
    let p_last = p.db.as_ref().unwrap().last_applied().unwrap();
    let p_base = p.db.as_ref().unwrap().log_base();
    assert!(
        p_last > n,
        "the primary must have re-advanced past the follower's frontier (last={p_last}, f={n})"
    );
    assert!(
        p_base > n,
        "the primary must have GC'd past the follower's frontier (base={p_base}, f={n}) — the \
         below-base precondition that reaches the diverged-behind arm"
    );

    let fp_before = data_fingerprint(&f.host).unwrap();
    let seeds_before = total_seeds(&f);

    // Reconnect: the follower must be refused by name, never silently re-seeded
    // (which would wipe its converged records). Its step() surfaces the refusal
    // as an io::Error.
    sim.repair("p", "f");
    let err = run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).expect_err(
        "a converged follower below a forked primary's base must be refused, never re-seeded",
    );
    // Byte-exact (error-suite T8): `diverged_behind` embeds no CRC, so the
    // whole surfaced error — the follower's own step() context plus the
    // refusal text — reconstructs fully from n and p_base.
    assert_eq!(
        err,
        format!("f step: {}", expected_diverged_behind(n, p_base))
    );
    // No wipe: the follower took no seed and its bytes are unchanged.
    assert_eq!(
        total_seeds(&f),
        seeds_before,
        "the follower must not have re-seeded (seeds went {seeds_before} -> {})",
        total_seeds(&f)
    );
    assert_eq!(
        data_fingerprint(&f.host).unwrap(),
        fp_before,
        "the follower's data must be untouched — no silent re-seed from the forked primary"
    );
}

/// `diverged_crc`'s text, pinned (S5 R2-I1: a pin for each refusal text that
/// prescribes a re-seed). A follower forked at a seq the primary also holds,
/// with different bytes, is refused by CRC, and the fix names the config the
/// cleared follower must restart under. This test computes neither CRC
/// (error-suite T8's CRC carve-out), so the text is pinned on both sides of
/// them.
#[test]
fn a_follower_forked_at_a_seq_the_primary_holds_is_refused_by_crc_naming_the_reseed_config() {
    let sim = Sim::with_faults(0x5E5F_1002, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    for i in 0..10u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    let fork = p.db.as_ref().unwrap().last_applied().expect("p committed") + 1;

    // Each side writes a different record at the same seq: the follower as a
    // wrongly-promoted node would, the primary as it keeps serving.
    sim.partition("p", "f");
    {
        let fdb = f.db.as_mut().unwrap();
        fdb.allow_writes();
        fdb.transact(|tx| {
            tx.put(&Widget {
                id: Id::from_raw(999),
                kind: 0,
                weight: 1,
            });
            Ok::<(), io::Error>(())
        })
        .expect("the forking write commits locally");
    }
    commit_widget(&sim, &mut p, 10).unwrap();
    assert_eq!(p.db.as_ref().unwrap().last_applied(), Some(fork));
    assert_eq!(f.db.as_ref().unwrap().last_applied(), Some(fork));

    sim.repair("p", "f");
    recover_node(&sim, &mut f).unwrap();
    // Both sit at `fork`, so `converged` holds already: drive until the refusal.
    let err = run_until(&sim, &mut [&mut p, &mut f], |_| false, CONVERGE_BUDGET)
        .expect_err("a follower forked at a seq the primary holds must be refused, never merged");
    assert!(
        err.starts_with(&format!(
            "f step: diverged: the follower's frame at seq {fork} has crc 0x"
        )),
        "{err}"
    );
    assert!(
        err.ends_with(&format!(
            " (this primary is at seq {fork}) — the logs share a seq but not its bytes, a forked \
             timeline that can never be merged; re-seed the follower from a fresh data dir (stop \
             it, delete its data dir, and restart it configured as a follower of this primary so \
             it snapshot-seeds from it, never with a primary config: the durable role file that \
             outranks config is deleted with the data dir, so a cleared node started as a \
             primary comes up as a second writable primary)"
        )),
        "{err}"
    );
}

/// The re-seed the refusals prescribe, on the node it is most dangerous for
/// (S5 R2-I1, the reviewer's probe): a former primary demoted by a
/// switchover, whose durable role file says follower while its startup
/// config still says primary. Its store is removed beside its compacted log,
/// and the boot refuses by name, naming the step. Followed as named (the data
/// dir deleted, the node restarted configured as a follower of the current
/// primary), it re-seeds and converges. The second arm deletes the data dir
/// and restarts the node with its old primary config, which the texts warn
/// against: the role file that outranked that config went with the data dir,
/// so the node comes up as a second writable primary and acks a write. That
/// arm pins the warning's truth. A fence that closed the hazard would turn it
/// red, and the texts would change with it.
#[test]
fn a_wiped_switchover_demoted_primary_rejoins_only_when_restarted_configured_as_a_follower() {
    for as_follower in [true, false] {
        let sim = Sim::with_faults(0x2EB1_0013, FaultConfig::none());
        let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), seeding_db_cfg()).unwrap();
        let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), seeding_db_cfg()).unwrap();
        for i in 0..30u64 {
            commit_widget(&sim, &mut p, i).unwrap();
        }
        run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
        p.flipped = true;
        f.flipped = true;
        let mut cli = CtrlConn::open(
            &sim,
            "p:1",
            &Msg::CtrlSwitchover {
                to: "f:1".to_string(),
                secret: None,
            },
        )
        .unwrap();
        let reply = drive_ctrl(&sim, &mut [&mut p, &mut f], &mut cli, CONVERGE_BUDGET).unwrap();
        assert!(matches!(reply, Msg::CtrlOk { .. }), "switchover: {reply:?}");
        let follower_of_f = Role::Follower {
            primary: "f:1".to_string(),
        };
        assert_eq!(
            p.repl.role(),
            &follower_of_f,
            "the switchover demoted p durably"
        );
        // f moves on as the primary. p is not driven again: f compacts past
        // p's position, so p's next Hello would be refused as diverged behind.
        for i in 30..40u64 {
            commit_widget(&sim, &mut f, i).unwrap();
        }
        p.db.as_mut().unwrap().checkpoint().unwrap();
        let base = p.db.as_ref().unwrap().log_base();
        assert!(base > 0, "setup must compact p's log past seq 0");

        p.db = None;
        let disk = p.host.disk();
        disk.remove("tables.redb").unwrap();
        disk.sync_dir("tables.redb").unwrap();
        p.host.restart();
        let err = reopen(&sim, &mut p).expect_err("a compacted log beside no store must not boot");
        assert!(
            err.contains(&format!(
                "but the log beside it starts at seq {base}: the acknowledged records from seq 0"
            )) && err.contains(
                "stop the node, delete its data dir, and restart it configured as a follower of \
                 the current primary"
            ),
            "the refusal must name the step: {err}"
        );

        for name in disk.list("").unwrap() {
            disk.remove(&name).unwrap();
        }
        disk.sync_dir("").unwrap();
        p.host.restart();
        if as_follower {
            p.repl_cfg = follower_cfg("p:1", "f:1");
            reopen(&sim, &mut p).unwrap();
            assert_eq!(p.repl.role(), &follower_of_f);
            let seeds = total_seeds(&p);
            for i in 40..50u64 {
                commit_widget(&sim, &mut f, i).unwrap();
            }
            run_until(&sim, &mut [&mut f, &mut p], converged, CONVERGE_BUDGET).unwrap();
            assert!(total_seeds(&p) > seeds, "p must re-seed from f");
            assert_converged(&sim, &mut f, &mut p).unwrap();
            assert_eq!(
                commit_widget(&sim, &mut p, 1000)
                    .map_err(|e| e.starts_with("commit refused: not primary of ")),
                Err(true),
                "the re-seeded node must refuse writes as a follower"
            );
        } else {
            reopen(&sim, &mut p).unwrap();
            assert_eq!(p.repl.role(), &Role::Primary);
            assert_eq!(f.repl.role(), &Role::Primary);
            commit_widget(&sim, &mut p, 1000)
                .expect("a cleared node started as a primary acks writes beside the real one");
        }
    }
}

/// Design B's deliberate cost, pinned so it is a choice and not an accident: a
/// POSITIONED follower that merely lagged past the primary's retained window —
/// SAME timeline, its records a true prefix of the primary's — is ALSO refused.
/// The primary GC'd the frame at the follower's position and cannot prove the
/// prefix relation from what it retains, and it cannot distinguish this benign
/// case from the data-losing forked-behind one above, so it refuses both. Pre-
/// Design-B this re-seeded (harmless here); now the operator's remedy text
/// covers it (re-seed a true laggard from a fresh data dir). The None-position
/// fresh-follower seed path is untouched — pinned by
/// `late_follower_seeds_below_the_gcd_base_and_converges`. Deterministic,
/// `FaultConfig::none()`.
#[test]
fn a_same_timeline_laggard_below_the_base_is_refused_not_reseeded() {
    let sim = Sim::with_faults(0x2EB1_0015, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), seeding_db_cfg()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), seeding_db_cfg()).unwrap();

    // Converge f to K on the shared timeline (holds frame K, a proven position).
    for i in 0..20u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    let k = f.db.as_ref().unwrap().last_applied().expect("f converged");

    // Crash-restart the follower: this tears its wire so the primary reaps the
    // session and releases the retention pin, and the follower reopens at K,
    // disconnected until it is next stepped.
    recover_node(&sim, &mut f).unwrap();
    // Drive the primary ALONE so it reaps the follower's dead session (pin ->
    // u64::MAX); the follower is not stepped here, so it stays disconnected.
    drive_rounds(&sim, &mut [&mut p], 16).unwrap();

    // Advance the primary FAR past K on the SAME timeline and GC past K (no
    // fork, no corruption — the benign laggard shape). With the pin released the
    // base moves comfortably past K.
    for i in 20..80u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    p.db.as_mut().unwrap().checkpoint().unwrap();
    let p_last = p.db.as_ref().unwrap().last_applied().unwrap();
    let p_base = p.db.as_ref().unwrap().log_base();
    assert!(
        p_last > k,
        "the primary re-advanced past the laggard (last={p_last}, k={k})"
    );
    assert!(
        p_base > k,
        "the primary GC'd past the laggard's position (base={p_base}, k={k})"
    );

    let seeds_before = total_seeds(&f);

    // Reconnect: the follower re-Hellos with its proven (K, crc). It must be
    // refused by name, not re-seeded, even though its records are a clean prefix.
    let err = run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET)
        .expect_err("a positioned laggard below the base must be refused, never re-seeded");
    // Byte-exact (error-suite T8): same reconstruction as the forked-behind
    // twin above — `diverged_behind` cannot tell this benign laggard apart
    // from that data-losing case, so it is the identical text with k/p_base.
    assert_eq!(
        err,
        format!("f step: {}", expected_diverged_behind(k, p_base))
    );
    // Refused, not re-seeded: no new seed, and f still sits at K (never rewound
    // onto the far-ahead primary).
    assert_eq!(
        total_seeds(&f),
        seeds_before,
        "the laggard must not re-seed (seeds went {seeds_before} -> {})",
        total_seeds(&f)
    );
    assert_eq!(
        f.db.as_ref().unwrap().last_applied(),
        Some(k),
        "the refused laggard stays at its own position, never rewound onto the primary"
    );
}

// --------------------------------------------- error-surface pin: version

/// Drive a single node against a raw client conn until the client gets a reply.
fn exchange_hello(sim: &Sim, node: &mut Node, addr: &str, hello: Msg) -> Result<Msg, String> {
    let client = sim
        .host("client")
        .net()
        .connect(addr)
        .map_err(|e| e.to_string())?;
    client.send(&encode(&hello)).map_err(|e| e.to_string())?;
    let mut buf = vec![0u8; MAX_MESSAGE_BYTES];
    for _ in 0..10_000 {
        let now = sim.now().as_nanos();
        node.repl.pump_io(now);
        let has_work = node
            .db
            .as_ref()
            .is_some_and(|db| node.repl.has_work(db, now));
        if has_work {
            step_node(sim, node)?;
        }
        match client.recv(&mut buf) {
            Ok(Some(0)) => return Err("node closed the connection before replying".to_string()),
            Ok(Some(n)) => return decode(&buf[..n]).map_err(|e| e.to_string()),
            Ok(None) => {}
            Err(e) => return Err(e.to_string()),
        }
        let next = sim.next_wakeup();
        match next {
            Some(t) if t > now => sim.clock().sleep(Duration::from_nanos(t - now)),
            _ => sim.clock().sleep(Duration::from_nanos(1)),
        }
    }
    Err("no reply from the node within the budget".to_string())
}

#[test]
fn hello_with_a_future_protocol_version_is_refused_naming_both_versions() {
    let sim = Sim::with_faults(0x2EB1_0006, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let reply = exchange_hello(
        &sim,
        &mut p,
        "p:1",
        Msg::Hello {
            proto: charpente::repl::PROTO_VERSION + 1,
            listen: None,
            position: None,
            secret: None,
            schema_digest: 0,
        },
    )
    .unwrap();
    match reply {
        Msg::Refuse { message } => {
            // Byte-exact (error-suite T8): reconstructed from decide_hello's
            // own format args — both sides of the mismatch are the test's own
            // scenario values (PROTO_VERSION and PROTO_VERSION + 1).
            let proto = charpente::repl::PROTO_VERSION + 1;
            assert_eq!(
                message,
                format!(
                    "replication protocol version mismatch: this node speaks protocol version \
                     {}, but the peer's Hello declared version {proto} — upgrade the older \
                     binary so both speak the expected version {}",
                    charpente::repl::PROTO_VERSION,
                    charpente::repl::PROTO_VERSION
                )
            );
        }
        other => panic!("expected Refuse, got {other:?}"),
    }
}

/// Fix round 1, ruling R39: `PROTO_VERSION` is now 2 (tag 14,
/// `ScopedSnapshotRows`) — a peer still on 1 (a binary before tenant
/// scoping's wire changes) is refused by name, the same equality check as
/// the "future version" direction above, exercised concretely rather than
/// trusted by symmetry.
#[test]
fn hello_declaring_protocol_version_1_is_refused_by_name() {
    let sim = Sim::with_faults(0x2EB1_0006 ^ 1, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let reply = exchange_hello(
        &sim,
        &mut p,
        "p:1",
        Msg::Hello {
            proto: 1,
            listen: None,
            position: None,
            secret: None,
            schema_digest: 0,
        },
    )
    .unwrap();
    match reply {
        Msg::Refuse { message } => {
            assert_eq!(
                message,
                format!(
                    "replication protocol version mismatch: this node speaks protocol version \
                     {0}, but the peer's Hello declared version 1 — upgrade the older binary so \
                     both speak the expected version {0}",
                    charpente::repl::PROTO_VERSION
                )
            );
        }
        other => panic!("expected Refuse, got {other:?}"),
    }
}

/// PROTO_VERSION 3 (payload shapes): a version-2 binary writes `0x02`/`0x03`
/// and cannot read `0x08`/`0x09`, so it is refused at Hello, by name, before a
/// single frame ships (spec amendment A3). (Protocol 6 since outbound effects;
/// the version-3, version-4 and version-5 tests follow.)
#[test]
fn hello_declaring_protocol_version_2_is_refused_by_name() {
    let sim = Sim::with_faults(0x2EB1_0006 ^ 2, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let reply = exchange_hello(
        &sim,
        &mut p,
        "p:1",
        Msg::Hello {
            proto: 2,
            listen: None,
            position: None,
            secret: None,
            schema_digest: 0,
        },
    )
    .unwrap();
    match reply {
        Msg::Refuse { message } => assert_eq!(
            message,
            "replication protocol version mismatch: this node speaks protocol version 6, but \
             the peer's Hello declared version 2 — upgrade the older binary so both speak the \
             expected version 6"
        ),
        other => panic!("expected Refuse, got {other:?}"),
    }
}

/// PROTO_VERSION 4 (job tenancy, spec §6): a version-3 binary cannot read the
/// scoped enqueue tag `0x0a`, so it is refused at Hello, by name, before a
/// single frame ships — not at its first scoped job. (Protocol 6 since
/// outbound effects; the version-4 and version-5 tests follow.)
#[test]
fn hello_declaring_protocol_version_3_is_refused_by_name() {
    let sim = Sim::with_faults(0x2EB1_0006 ^ 3, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let reply = exchange_hello(
        &sim,
        &mut p,
        "p:1",
        Msg::Hello {
            proto: 3,
            listen: None,
            position: None,
            secret: None,
            schema_digest: 0,
        },
    )
    .unwrap();
    match reply {
        Msg::Refuse { message } => assert_eq!(
            message,
            "replication protocol version mismatch: this node speaks protocol version 6, but \
             the peer's Hello declared version 3 — upgrade the older binary so both speak the \
             expected version 6"
        ),
        other => panic!("expected Refuse, got {other:?}"),
    }
}

/// PROTO_VERSION 5 (tenancy completion, spec §6): a version-4 binary cannot
/// read the scoped emit tag `0x0b`, nor an `EventRow`'s trailing tenant, so
/// it is refused at Hello, by name, before a single frame ships — not at its
/// first scoped emit. (Protocol 6 since outbound effects; the version-5 test
/// follows.)
#[test]
fn hello_declaring_protocol_version_4_is_refused_by_name() {
    let sim = Sim::with_faults(0x2EB1_0006 ^ 4, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let reply = exchange_hello(
        &sim,
        &mut p,
        "p:1",
        Msg::Hello {
            proto: 4,
            listen: None,
            position: None,
            secret: None,
            schema_digest: 0,
        },
    )
    .unwrap();
    match reply {
        Msg::Refuse { message } => assert_eq!(
            message,
            "replication protocol version mismatch: this node speaks protocol version 6, but \
             the peer's Hello declared version 4 — upgrade the older binary so both speak the \
             expected version 6"
        ),
        other => panic!("expected Refuse, got {other:?}"),
    }
}

/// PROTO_VERSION 6 (outbound effects, spec §6): a version-5 binary cannot read
/// the outbound enqueue tags `0x0c`/`0x0d`, nor a `JobRow`'s trailing outbound
/// key (its decoder would call a seeded outbound row corrupt), so it is refused
/// at Hello, by name, before a single frame ships — not at its first outbound
/// enqueue.
#[test]
fn hello_declaring_protocol_version_5_is_refused_by_name() {
    let sim = Sim::with_faults(0x2EB1_0006 ^ 5, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let reply = exchange_hello(
        &sim,
        &mut p,
        "p:1",
        Msg::Hello {
            proto: 5,
            listen: None,
            position: None,
            secret: None,
            schema_digest: 0,
        },
    )
    .unwrap();
    match reply {
        Msg::Refuse { message } => assert_eq!(
            message,
            "replication protocol version mismatch: this node speaks protocol version 6, but \
             the peer's Hello declared version 5 — upgrade the older binary so both speak the \
             expected version 6"
        ),
        other => panic!("expected Refuse, got {other:?}"),
    }
}

#[test]
fn hello_to_a_follower_is_refused_naming_the_not_a_primary_state() {
    let sim = Sim::with_faults(0x2EB1_0007, FaultConfig::none());
    // A follower of an (absent) primary — it still listens for control conns.
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    let digest = f.db.as_ref().unwrap().schema_digest();
    let reply = exchange_hello(
        &sim,
        &mut f,
        "f:1",
        Msg::Hello {
            proto: charpente::repl::PROTO_VERSION,
            listen: Some("x:1".to_string()),
            position: None,
            secret: None,
            schema_digest: digest,
        },
    )
    .unwrap();
    match reply {
        Msg::Refuse { message } => {
            // Byte-exact (error-suite T8): reconstructed from decide_hello's
            // own format args — "p:1" is the follower's configured primary,
            // named twice by the same binding in the format string.
            assert_eq!(
                message,
                "this node is not a primary (it is a follower of p:1) — replication Hellos \
                 are served only by the primary, and a follower never feeds another follower \
                 (no cascading); point the follower at p:1 instead"
            );
        }
        other => panic!("expected Refuse, got {other:?}"),
    }
}

// ------------------------------------------------- property: peer secret

/// Spec §5 (auth T7): a node configured with `peer_secret: Some` refuses any
/// inbound Hello/Ctrl whose proof does not match — named `Refuse`, before any
/// state is touched (no ladder entry, no pin, no seed, no role flip).
fn assert_peer_secret_refusal(reply: &Msg) {
    match reply {
        Msg::Refuse { message } => {
            assert_eq!(
                message,
                "peer secret missing or mismatched — set ReplConfig.peer_secret to match this \
                 node (or run the CLI verb with --secret)"
            );
        }
        other => panic!("expected the peer-secret Refuse, got {other:?}"),
    }
}

#[test]
fn peer_secret_refuses_mismatch_and_admits_match() {
    let sim = Sim::with_faults(0x2EB1_0011, FaultConfig::none());
    let db_cfg = seeding_db_cfg(); // small segments/checkpoint cadence: GC is observable
    let p_cfg = ReplConfig {
        peer_secret: Some("s3cret".to_string()),
        strict_witnesses: false,
        ..primary_cfg("p:1")
    };
    let mut p = spawn_node(&sim, "p", p_cfg, db_cfg.clone()).unwrap();

    // No secret at all: refused by name.
    let reply = exchange_hello(
        &sim,
        &mut p,
        "p:1",
        Msg::Hello {
            proto: charpente::repl::PROTO_VERSION,
            listen: Some("f:1".to_string()),
            position: None,
            secret: None,
            schema_digest: 0,
        },
    )
    .unwrap();
    assert_peer_secret_refusal(&reply);
    drive_rounds(&sim, &mut [&mut p], 4).unwrap(); // reap the refused session

    // The wrong secret: same refusal.
    let reply = exchange_hello(
        &sim,
        &mut p,
        "p:1",
        Msg::Hello {
            proto: charpente::repl::PROTO_VERSION,
            listen: Some("f:1".to_string()),
            position: None,
            secret: Some("wrong".to_string()),
            schema_digest: 0,
        },
    )
    .unwrap();
    assert_peer_secret_refusal(&reply);
    drive_rounds(&sim, &mut [&mut p], 4).unwrap(); // reap the refused session

    // Neither refusal opened a seed or pinned retention: drive enough commits
    // for GC to compact past the base — it must, since no live session (the
    // refused ones died on the spot, and were reaped above) holds it back.
    for i in 0..60u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    assert!(
        p.db.as_ref().unwrap().log_base() > 0,
        "GC must have advanced — a refused Hello must never pin retention"
    );
    assert_eq!(
        p.repl.stats().seeds,
        0,
        "a refused Hello must never begin a snapshot seed"
    );

    // The right secret: admitted, and a real follower converges end-to-end
    // (Follower attaches its configured secret automatically, outbound).
    let f_cfg = ReplConfig {
        peer_secret: Some("s3cret".to_string()),
        strict_witnesses: false,
        ..follower_cfg("f:1", "p:1")
    };
    let mut f = spawn_node(&sim, "f", f_cfg, db_cfg).unwrap();
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_converged(&sim, &mut p, &mut f).unwrap();

    // A CtrlPromote without the secret at a secret-configured node → Refuse,
    // and the role never flips (no state touched on refusal). Reuses the
    // converged follower `f`, which has a listen address (promotable).
    let mut cli = CtrlConn::open(&sim, "f:1", &Msg::CtrlPromote { secret: None }).unwrap();
    let reply = drive_ctrl(&sim, &mut [&mut f], &mut cli, CONVERGE_BUDGET).unwrap();
    assert_peer_secret_refusal(&reply);
    assert!(
        matches!(f.repl.role(), Role::Follower { .. }),
        "a refused promote must leave the durable role untouched: {:?}",
        f.repl.role()
    );
    assert!(
        !f.db.as_ref().unwrap().writes_allowed(),
        "a refused promote must never flip the node to a writable primary"
    );
}

/// Migrations spec §5: a Hello whose `schema_digest` does not match this
/// node's compiled schema is refused by name, before any state is touched
/// (no seed begins); a real follower's auto-attached digest matches and
/// convergence is unaffected.
#[test]
fn hello_schema_digest_mismatch_refuses_zero_state_touched_and_match_converges() {
    let sim = Sim::with_faults(0x2EB8_0001, FaultConfig::none());
    let db_cfg = seeding_db_cfg();
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), db_cfg.clone()).unwrap();
    let real = p.db.as_ref().unwrap().schema_digest();

    let reply = exchange_hello(
        &sim,
        &mut p,
        "p:1",
        Msg::Hello {
            proto: charpente::repl::PROTO_VERSION,
            listen: Some("f:1".to_string()),
            position: None,
            secret: None,
            schema_digest: real.wrapping_add(1),
        },
    )
    .unwrap();
    match &reply {
        Msg::Refuse { message } => assert_eq!(
            message.as_str(),
            "replication schema mismatch: this node's #[table] schema digest does not match \
             the peer's — one side is running a binary compiled against a different table \
             shape; rebuild both nodes from the same source and restart them (stop both, \
             upgrade both, start the primary first)"
        ),
        other => panic!("expected the schema-digest Refuse, got {other:?}"),
    }
    drive_rounds(&sim, &mut [&mut p], 4).unwrap();
    assert_eq!(
        p.repl.stats().seeds,
        0,
        "a digest-refused Hello must never begin a seed"
    );

    // Happy path: a real follower's auto-attached digest matches and
    // convergence is unaffected.
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), db_cfg).unwrap();
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_converged(&sim, &mut p, &mut f).unwrap();
}

/// The half-configured "open" direction (spec §5): an unconfigured primary
/// accepts a follower's proof it never asked for, gracefully — no error, no
/// refusal — and the pair converges exactly as an all-`None` pair would. The
/// guarded direction (a configured primary refusing an unconfigured follower)
/// is already exercised above (the "no secret at all" case).
#[test]
fn peer_secret_unconfigured_node_accepts_a_followers_unrequired_proof() {
    let sim = Sim::with_faults(0x2EB1_0012, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let f_cfg = ReplConfig {
        peer_secret: Some("s3cret".to_string()),
        strict_witnesses: false,
        ..follower_cfg("f:1", "p:1")
    };
    let mut f = spawn_node(&sim, "f", f_cfg, DbConfig::default()).unwrap();

    for i in 0..20u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_converged(&sim, &mut p, &mut f).unwrap();
}

/// The empty-secret config error (2026-07-24 amendment): `Some("")` would
/// authenticate any peer presenting no proof at all — almost never what a
/// configured secret intends — so `Repl::open` refuses it by name, before any
/// socket/state work, on EITHER role (the check runs before `load_role`
/// branches on the durable role at all). Two-host sim, deterministic
/// (`FaultConfig::none()`): a primary config and a follower config each carry
/// `peer_secret: Some(String::new())` and are refused with the identical text.
#[test]
fn empty_peer_secret_is_refused_as_a_named_config_error_on_either_role() {
    let sim = Sim::with_faults(0x2EB1_00EE, FaultConfig::none());
    const EXPECTED: &str = "ReplConfig.peer_secret is Some(\"\") — an empty peer secret \
         authenticates any peer that presents no proof, which is almost never what a \
         configured secret intends; set a real secret, or use None to explicitly accept all \
         peers (DESIGN §8)";

    let mut bad_primary = primary_cfg("p:1");
    bad_primary.peer_secret = Some(String::new());
    let err = spawn_node(&sim, "p", bad_primary, DbConfig::default())
        .err()
        .expect("an empty peer secret must be refused before any socket/state work");
    assert_eq!(err, format!("p: Repl::open: {EXPECTED}"));
    // Refused before `load_role`'s first-boot write: no durable role file.
    assert!(
        sim.host("p").disk().list("role").unwrap().is_empty(),
        "the empty-secret refusal must fire before any state is touched"
    );

    let mut bad_follower = follower_cfg("f:1", "p:1");
    bad_follower.peer_secret = Some(String::new());
    let err = spawn_node(&sim, "f", bad_follower, DbConfig::default())
        .err()
        .expect("a follower config with an empty secret must be refused too");
    assert_eq!(err, format!("f: Repl::open: {EXPECTED}"));
    assert!(sim.host("f").disk().list("role").unwrap().is_empty());
}

// --------------------------------------------- error-surface pin: empty ship

/// Regression (final replication review, finding 1): the real shipper
/// (`Session::ship_frames`) never sends an empty `Frames` batch (it only
/// sends when `read_log` returns something), but nothing on the wire enforces
/// that — a hostile or buggy peer could. `apply_shipped` is a documented
/// no-op on an empty batch (charpente-core db.rs: it never advances
/// `last_applied`), so the follower's ack path used to `.expect()` a
/// just-advanced position that never came, panicking the kernel thread on a
/// fresh follower (`last_applied() == None`). Driven by hand, not
/// `spawn_node`'s primary: a raw listener stands in for the primary so the
/// test can hand back a `Frames { frames: vec![] }` the real shipper never
/// would.
#[test]
fn empty_frames_delivered_to_a_fresh_follower_neither_panics_nor_acks() {
    let sim = Sim::with_faults(0x2EB1_000F, FaultConfig::none());
    let ph = sim.host("p");
    let listener = ph.net().listen("p:1").expect("listen");
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();

    // Drive the follower once: a fresh client (no conn, no backoff) has work
    // immediately, and its one step is connect_and_hello.
    let now = sim.now().as_nanos();
    f.repl.pump_io(now);
    assert!(
        f.db.as_ref().is_some_and(|db| f.repl.has_work(db, now)),
        "a fresh follower client must have work immediately (nothing to wait on)"
    );
    step_node(&sim, &mut f).unwrap();

    let conn = listener
        .accept()
        .unwrap()
        .expect("the follower's connect_and_hello must have dialed p:1");
    let mut buf = vec![0u8; MAX_MESSAGE_BYTES];
    let n = conn
        .recv(&mut buf)
        .unwrap()
        .expect("the follower's Hello must have arrived (zero simulated net latency)");
    assert!(
        matches!(decode(&buf[..n]).unwrap(), Msg::Hello { .. }),
        "the follower's first message on a fresh conn must be a Hello"
    );

    conn.send(&encode(&Msg::Frames { frames: vec![] })).unwrap();

    // Drive the follower to process the empty batch: must not panic (the
    // regression), must not fabricate a durable position, and must not ack.
    for _ in 0..8 {
        let now = sim.now().as_nanos();
        f.repl.pump_io(now);
        if f.db.as_ref().is_some_and(|db| f.repl.has_work(db, now)) {
            step_node(&sim, &mut f).unwrap();
        }
    }
    assert_eq!(
        f.db.as_ref().unwrap().last_applied(),
        None,
        "an empty Frames batch must not fabricate a durable position on a fresh follower"
    );
    match conn.recv(&mut buf) {
        Ok(None) => {}
        other => panic!("the follower acked (or replied to) an empty Frames batch: {other:?}"),
    }
}

// ------------------------------------------- follower-side batch atomicity

/// The follower path gets the same atomicity as the primary: a shipped batch
/// that fails mid-apply rolls back whole. The REHELLO_MARKER assertion is here
/// because submit_batch's seq-validation loop was deliberately NOT touched by
/// per-batch apply — this is what proves it stayed untouched, rather than
/// trusting that the diff looked small.
#[test]
fn a_shipped_batch_that_fails_mid_apply_rolls_back_whole() {
    let sim = Sim::with_faults(0x7AB1_E002, FaultConfig::none());
    let host_a = sim.host("a");
    let host_b = sim.host("b");
    let host_c = sim.host("c");

    // Two good committed records to ship, same tag-99 trick as
    // transactions.rs's primary-side atomicity test: `decode_record` refuses
    // it INSIDE the store transaction, after the two good records have
    // already put rows into that same transaction.
    let mut db_a = Db::open(Box::new(host_a.disk()), DbConfig::default(), make_schema()).unwrap();
    for i in 0..2u64 {
        db_a.transact(|tx| {
            tx.put(&Widget {
                id: Id::from_raw(i),
                kind: 0,
                weight: i as i64,
            });
            Ok::<(), io::Error>(())
        })
        .unwrap();
    }
    let mut frames = db_a.read_log(0, usize::MAX).unwrap();
    assert_eq!(frames.len(), 2);
    frames.push((2, vec![1u8, 0, 0, 0, 99]));

    let mut db_b = Db::open(Box::new(host_b.disk()), DbConfig::default(), make_schema()).unwrap();
    let err = db_b.apply_shipped(&frames).unwrap_err();
    assert!(err.to_string().contains("unknown effect tag 99"), "{err}");
    assert_eq!(
        db_b.last_applied(),
        None,
        "a batch that fails mid-apply must move the follower's applied_seq by zero"
    );
    assert!(
        db_b.poisoned(),
        "a mid-apply failure on the follower path must poison the engine, exactly like the \
         primary path — restart-and-recover is the only exit either way"
    );

    // A second, fresh (unpoisoned) follower proves the seq-validation loop
    // ahead of apply_batch is untouched: an out-of-order shipped seq still
    // refuses by name before apply_batch ever runs, on a log that never saw
    // this task's changes exercised.
    let mut db_c = Db::open(Box::new(host_c.disk()), DbConfig::default(), make_schema()).unwrap();
    let err = db_c.apply_shipped(&[(5, vec![0u8, 0, 0, 0])]).unwrap_err();
    assert!(
        err.to_string().contains(REHELLO_MARKER),
        "an out-of-order shipped seq must still name {REHELLO_MARKER}: {err}"
    );
}

// -------------------------------------- property: mixed-version tag refusal

/// Task 1's dangling World-A obligation, discharged (tenant-scoping Task 6b,
/// Step 4): "a follower on an older binary refuses [a wider record] by
/// name." No genuinely older binary runs in-process, so the oracle is a
/// follower fed a tag THIS binary does not know either — tag 14 (`0x0e`),
/// one past outbound effects' `0x0d` (scoped outbound enqueue). This test's
/// own doc comment predicted exactly this collision when it was written
/// against tag 7 (Task 6b): "so this stays true whichever tag a later task (6c or
/// beyond) claims next" — tag 14 is simply one past the highest tag any
/// mounted feature claims today (job tenancy's `0x0a` moved the fixture from
/// 10 to 11, event tenancy's `0x0b` from 11 to 12, and outbound effects'
/// `0x0c`/`0x0d` from 12 to 14, DESIGN §4). Proves the
/// unknown-tag refusal is wired through the REAL `apply_shipped` path, not
/// just `decode_record`'s own unit test (`db::tests::decode_refuses_unknown_
/// tags`) — and, like the sibling test above, that it refuses the WHOLE
/// batch: two good records ship in the same `Frames` batch ahead of the
/// tag-14 one, and neither must land.
#[test]
fn a_shipped_batch_with_an_effect_tag_this_binary_does_not_know_refuses_whole_by_name() {
    let sim = Sim::with_faults(0x6B00_0004, FaultConfig::none());
    let host_a = sim.host("a");
    let host_b = sim.host("b");

    let mut db_a = Db::open(Box::new(host_a.disk()), DbConfig::default(), make_schema()).unwrap();
    for i in 0..2u64 {
        db_a.transact(|tx| {
            tx.put(&Widget {
                id: Id::from_raw(i),
                kind: 0,
                weight: i as i64,
            });
            Ok::<(), io::Error>(())
        })
        .unwrap();
    }
    let mut frames = db_a.read_log(0, usize::MAX).unwrap();
    assert_eq!(frames.len(), 2);
    frames.push((2, vec![1u8, 0, 0, 0, 14]));

    let mut db_b = Db::open(Box::new(host_b.disk()), DbConfig::default(), make_schema()).unwrap();
    let err = db_b.apply_shipped(&frames).unwrap_err();
    assert!(err.to_string().contains("unknown effect tag 14"), "{err}");
    assert_eq!(
        db_b.last_applied(),
        None,
        "an unknown-tag record must refuse the WHOLE shipped batch — no partial apply of the \
         two good records ahead of it"
    );
}

// ----------------------------------------------------- meta-test: ack<sync

/// Victim: a follower that acks a WAL append BEFORE syncing it (`bug` true),
/// versus the correct order, sync then ack (`bug` false). Each round appends
/// one frame, then crash-restarts the host and recovers the WAL; the oracle
/// requires every acked seq to survive. The unsynced ack loses data whenever a
/// crash collapse drops the frame, so the harness catches the victim.
fn ack_before_sync_workload(sim: &mut Sim, bug: bool) -> Result<(), String> {
    let vh = sim.host("v");
    let cfg = WalConfig::default();
    let mut wal =
        Wal::open(Box::new(vh.disk()), cfg.clone(), |_, _| Ok(())).map_err(|e| e.to_string())?;
    let mut acked: Vec<u64> = Vec::new();
    for round in 0..16u64 {
        let seq = wal
            .append(&[(round as u8).wrapping_add(1); 8])
            .map_err(|e| e.to_string())?;
        if bug {
            acked.push(seq); // ack BEFORE sync — the bug
        } else {
            wal.sync().map_err(|e| e.to_string())?;
            acked.push(seq); // ack AFTER the durability barrier
        }
        vh.crash_now();
        vh.restart();
        let mut last: Option<u64> = None;
        wal = Wal::open(Box::new(vh.disk()), cfg.clone(), |s, _| {
            last = Some(s);
            Ok(())
        })
        .map_err(|e| e.to_string())?;
        for &s in &acked {
            if last.is_none_or(|l| s > l) {
                return Err(format!(
                    "acked seq {s} exceeds recovered durable seq {last:?} — the follower acked \
                     before its WAL sync and the crash lost it"
                ));
            }
        }
    }
    Ok(())
}

#[test]
fn harness_catches_a_follower_that_acks_before_sync() {
    // Fixed base so CI is deterministic; crash collapse is the only fault, so a
    // synced ack is always safe and an unsynced one is caught when a frame drops.
    let cfg = FaultConfig {
        crash_apply: 0.3,
        crash_torn: 0.5,
        ..FaultConfig::none()
    };
    let mut state = 0xACE0_5117_0001u64;
    let mut caught: Option<(u64, String)> = None;
    for _ in 0..64 {
        let seed = charpente_sim::rng::splitmix64(&mut state);
        let mut sim = Sim::with_faults(seed, cfg.clone());
        if let Err(msg) = ack_before_sync_workload(&mut sim, true) {
            caught = Some((seed, msg));
            break;
        }
    }
    let (seed, msg) = caught.expect(
        "the harness no longer catches ack-before-sync under crash collapse — the fault \
         injection itself has regressed",
    );
    eprintln!("caught ack-before-sync: seed {seed:#018x}: {msg}");

    let mut replay = Sim::with_faults(seed, cfg.clone());
    assert_eq!(
        ack_before_sync_workload(&mut replay, true),
        Err(msg),
        "replaying the seed must reproduce the identical failure"
    );

    // The discrimination half: the correct order (sync, then ack) survives
    // the exact universe that killed the victim.
    let mut correct = Sim::with_faults(seed, cfg);
    ack_before_sync_workload(&mut correct, false)
        .expect("sync-before-ack must survive the victim's universe");
}

// --------------------------------------------- meta-test: skip crc on ship

/// The victim shipper: reads a WAL's frames back with raw length arithmetic
/// and NO CRC re-verification (the real `read_from` re-verifies and refuses a
/// corrupt frame by name). Used only in the meta-test below.
fn victim_read_raw(host: &SimHost, dir: &str, up_to: u64) -> io::Result<Vec<(u64, Vec<u8>)>> {
    let disk = host.disk();
    let mut names = disk.list(&format!("{dir}/"))?;
    names.sort();
    let mut out = Vec::new();
    for name in &names {
        let file = disk.open(name)?;
        let len = file.len()?;
        let mut offset = 0u64;
        while offset + 16 <= len {
            let mut header = [0u8; 16];
            file.read_at(offset, &mut header)?;
            let seq = u64::from_le_bytes(header[0..8].try_into().expect("8 bytes"));
            let plen = u64::from(u32::from_le_bytes(
                header[8..12].try_into().expect("4 bytes"),
            ));
            // No CRC check — the whole point. Only a structural guard so a
            // garbage length doesn't read out of bounds.
            if plen == 0 || offset + 16 + plen > len {
                break;
            }
            let mut payload = vec![0u8; plen as usize];
            file.read_at(offset + 16, &mut payload)?;
            if seq <= up_to {
                out.push((seq, payload));
            }
            offset += 16 + plen;
        }
    }
    Ok(out)
}

/// Ship the primary's log to a fresh follower using either the victim reader
/// (`bug` true, no CRC) or the real `read_log` (`bug` false, CRC re-verified).
/// The primary's own store is built from the in-memory (correct) applies, so a
/// WAL frame corrupted on disk by a misdirected write is exactly the shipped
/// corruption the real path refuses and the victim leaks.
fn skip_crc_workload(sim: &mut Sim, bug: bool) -> Result<(), String> {
    let ph = sim.host("p");
    let cfg = DbConfig {
        wal: WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 96,
        },
        store_path: "tables.redb".to_string(),
        retain_segments: 64, // no GC pressure — every frame stays retained
        checkpoint_every: 0, // no checkpoint — the horizon stays at 0
        event_retention: 65_536,
        migration_batch_size: 1000,
    };
    let mut p =
        Db::open(Box::new(ph.disk()), cfg.clone(), make_schema()).map_err(|e| e.to_string())?;
    for i in 0..24u64 {
        // Misdirected writes may corrupt frames on disk; the in-memory apply is
        // unaffected, so the store stays correct.
        let _ = p.transact(|tx| {
            tx.put(&Widget {
                id: Id::from_raw(i),
                kind: i % 4,
                weight: i as i64,
            });
            Ok::<(), io::Error>(())
        });
    }
    let Some(p_last) = p.last_applied() else {
        return Ok(());
    };
    let p_scans = table_scans(&p).map_err(|e| e.to_string())?;

    let fh = sim.host("f");
    let mut f = Db::open(Box::new(fh.disk()), cfg, make_schema()).map_err(|e| e.to_string())?;

    let frames = if bug {
        victim_read_raw(&ph, "wal", p_last).map_err(|e| e.to_string())?
    } else {
        match p.read_log(0, usize::MAX) {
            Ok(frames) => frames,
            // The real path refuses corruption by name: safe, nothing ships.
            Err(_) => return Ok(()),
        }
    };

    match f.apply_shipped(&frames) {
        Ok(()) => {}
        // A genuine injected fault on the follower's own disk is inconclusive.
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(()),
        // The corrupt shipped payload was rejected on apply — the victim leaked
        // it, which is exactly what the CRC re-verify would have prevented.
        Err(e) => {
            return Err(format!(
                "follower rejected a shipped frame ({e}) — the shipper streamed unverified bytes"
            ));
        }
    }
    let f_scans = table_scans(&f).map_err(|e| e.to_string())?;
    if f_scans != p_scans {
        return Err(
            "follower diverged from the primary — the shipper streamed unverified (corrupt) bytes"
                .to_string(),
        );
    }
    Ok(())
}

/// Every table's raw rows, in schema order — the content-equality oracle.
type RawTables = Vec<(String, Vec<(u64, u64, Vec<u8>)>)>;

fn table_scans(db: &Db) -> io::Result<RawTables> {
    let snap = db.snapshot()?;
    let mut out = Vec::new();
    for table in db.table_names() {
        out.push((
            table.to_string(),
            snap.scan_raw(table, db.table_scoped(table)?)?,
        ));
    }
    Ok(out)
}

#[test]
fn harness_catches_a_shipper_that_skips_crc_reverification() {
    // Only the misdirected-write axis is hot (scoped to wal/ by default): the
    // primary's own durable bytes are where shipped corruption comes from.
    let cfg = FaultConfig {
        misdirected_write: 0.1,
        ..FaultConfig::none()
    };
    let mut state = 0xC12C_5117_0001u64;
    let mut caught: Option<(u64, String)> = None;
    for _ in 0..64 {
        let seed = charpente_sim::rng::splitmix64(&mut state);
        let mut sim = Sim::with_faults(seed, cfg.clone());
        if let Err(msg) = skip_crc_workload(&mut sim, true) {
            caught = Some((seed, msg));
            break;
        }
    }
    let (seed, msg) = caught.expect(
        "the harness no longer catches CRC-free shipping under misdirected writes — the fault \
         injection itself has regressed",
    );
    eprintln!("caught CRC-free shipping: seed {seed:#018x}: {msg}");

    let mut replay = Sim::with_faults(seed, cfg.clone());
    assert_eq!(
        skip_crc_workload(&mut replay, true),
        Err(msg),
        "replaying the seed must reproduce the identical failure"
    );

    // The discrimination half: the real CRC-re-verifying read path survives
    // the exact universe that killed the victim (it refuses the corrupt frame
    // by name instead of shipping it).
    let mut correct = Sim::with_faults(seed, cfg);
    skip_crc_workload(&mut correct, false)
        .expect("the CRC-verified shipper must survive the victim's universe");
}

// ======================================================= replicated migrations
//
// Migrations T10 (spec §6): the scenario class the reviewed plan never
// covered — a `#[table]` schema bump under replication. Reuses the Db/Repl
// rig above (T6-T8), not the Kernel rig below (T8's own section) — these
// properties call `db.migrate_schema()` by hand at the points a real
// `Kernel::run`/`do_promote` would call it (spec §2/§4's role-aware gate).

/// Seed 0x2EB8_0002: a FRESH v2 follower must Hello `position: None` (never
/// write anything at open, even under a migration-bearing schema) and take
/// the snapshot path — the regression tripwire for "gate writes at open".
#[test]
fn fresh_v2_follower_seeds_across_the_gate() {
    let sim = Sim::with_faults(0x2EB8_0002, FaultConfig::none());
    let db_cfg = seeding_db_cfg();

    // Pre-upgrade history: seed v1 rows directly (a real pre-upgrade store
    // on disk, independent of the primary node this test builds next).
    {
        let h = sim.host("p");
        let mut seed_db = Db::open(Box::new(h.disk()), db_cfg.clone(), make_schema_v1()).unwrap();
        seed_db
            .transact(|tx| -> Result<(), ()> {
                for i in 0..20u64 {
                    tx.put(&WidgetV1 {
                        id: Id::from_raw(i),
                        count: i as u32,
                    });
                }
                Ok(())
            })
            .unwrap();
        seed_db.checkpoint().unwrap(); // clean shutdown: the tail is drained
    }

    // The primary boots on v2 and migrates — Kernel::run's role-aware gate
    // ("single-node and primary migrate before serving"), reproduced by hand
    // at this Db/Repl-level rig.
    let mut p = spawn_node_with_schema(
        &sim,
        "p",
        primary_cfg("p:1"),
        db_cfg.clone(),
        make_schema_v2(),
    )
    .unwrap();
    p.db.as_mut().unwrap().migrate_schema().unwrap();

    // GC past the base: commit new v2 rows past several checkpoint
    // boundaries (seeding_db_cfg's checkpoint_every: 8).
    for i in 20..60u64 {
        p.db.as_mut()
            .unwrap()
            .transact(|tx| {
                tx.put(&WidgetV2 {
                    id: Id::from_raw(i),
                    count: i as u32,
                    note: format!("added-{i}"),
                });
                Ok::<(), io::Error>(())
            })
            .unwrap();
    }
    assert!(
        p.db.as_ref().unwrap().log_base() > 0,
        "GC must move the base past the migration"
    );

    // A FRESH v2 follower: even with a versioned/migration-bearing schema,
    // Db::open must write nothing.
    let mut f = spawn_node_with_schema(
        &sim,
        "f",
        follower_cfg("f:1", "p:1"),
        db_cfg,
        make_schema_v2(),
    )
    .unwrap();
    assert_eq!(
        f.db.as_ref().unwrap().last_applied(),
        None,
        "a fresh v2 follower's open must write nothing — it must Hello position: None"
    );

    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert!(
        total_seeds(&f) >= 1,
        "a fresh follower below the primary's base must take the snapshot path"
    );
    assert!(
        f.db.as_ref().unwrap().log_base() > 0,
        "a seeded follower's WAL base must be above 0"
    );
    assert_converged(&sim, &mut p, &mut f).unwrap();

    // The migrated rows, with their note values, arrived via the seed...
    let mut rows: Vec<WidgetV2> = f.db.as_ref().unwrap().snapshot().unwrap().scan().unwrap();
    rows.sort_by_key(|r| r.id.raw());
    assert_eq!(rows.len(), 60);
    for r in &rows[..20] {
        assert_eq!(
            r.note,
            "v2",
            "row {} must carry the migrated note",
            r.id.raw()
        );
    }
    for (i, r) in rows[20..].iter().enumerate() {
        assert_eq!(r.note, format!("added-{}", i as u64 + 20));
    }
    // ...and so did the schema_versions row for "WidgetV2" — a completed
    // migration, not an in-flight one.
    let sv: SchemaVersion =
        f.db.as_ref()
            .unwrap()
            .snapshot()
            .unwrap()
            .get(schema_version_id("WidgetV2"))
            .unwrap()
            .expect("the schema_versions row for WidgetV2 arrived via the seed");
    assert_eq!(
        (sv.version, sv.migrating_through, sv.migrating_to),
        (2, None, None),
        "the seeded schema_versions row must show a completed migration"
    );
}

/// Seed 0x2EB8_0003: both nodes converge on v1, clean-shutdown, both restart
/// on v2 — the primary migrates at its startup path, the follower's gate
/// admits it read-only (nothing below-version left to replay past a clean
/// checkpoint) and catches up entirely via shipped `MigratePut`s, never a
/// re-seed.
#[test]
fn converged_v1_follower_catches_up_via_shipped_migrateputs() {
    let sim = Sim::with_faults(0x2EB8_0003, FaultConfig::none());
    let db_cfg = seeding_db_cfg();
    let mut p = spawn_node_with_schema(
        &sim,
        "p",
        primary_cfg("p:1"),
        db_cfg.clone(),
        make_schema_v1(),
    )
    .unwrap();
    let mut f = spawn_node_with_schema(
        &sim,
        "f",
        follower_cfg("f:1", "p:1"),
        db_cfg.clone(),
        make_schema_v1(),
    )
    .unwrap();

    for i in 0..15u64 {
        p.db.as_mut()
            .unwrap()
            .transact(|tx| {
                tx.put(&WidgetV1 {
                    id: Id::from_raw(i),
                    count: i as u32,
                });
                Ok::<(), io::Error>(())
            })
            .unwrap();
        drive_rounds(&sim, &mut [&mut p, &mut f], 3).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_converged(&sim, &mut p, &mut f).unwrap();
    assert_eq!(
        f.repl.stats().seeds,
        0,
        "a live, kept-up follower must converge via shipped frames, never a seed"
    );
    assert_eq!(p.repl.stats().seeds, 0);

    // Clean shutdown on both — follower first, then primary — each via an
    // explicit checkpoint (this rig drives nodes directly, not through the
    // kernel loop, so the clean-exit checkpoint is explicit).
    f.db.as_mut().unwrap().checkpoint().unwrap();
    f.db = None;
    p.db.as_mut().unwrap().checkpoint().unwrap();
    p.db = None;

    // Followers first, then the primary. The order is load-bearing since the
    // witness boot gate landed (M2 sub-project 0b task 5): p shipped to f
    // above, so f is a durable witness of p, and a restarted p withholds
    // writes — including `migrate_schema`, which O-1 moved onto the write
    // gate — until f answers "not a primary". Starting p first here is what
    // the pre-gate version of this test did, and it now deadlocks at the
    // migrate below. This is the same followers-first rollout order the
    // peer-secret work (M1 sub-project 4, T7) already established, and the
    // gate generalizes it: the receiver must be up for the dialer to proceed.
    //
    // The follower's own gate is read-only: the store still holds v1 bytes,
    // admitted because the clean checkpoint above left nothing below-version
    // to replay; it catches up via shipped MigratePuts once reconnected,
    // never running migrate_schema itself.
    f.schema = make_schema_v2();
    f.host.restart();
    reopen(&sim, &mut f).unwrap();

    // Restart the primary on v2 — its startup path migrates.
    p.schema = make_schema_v2();
    p.host.restart();
    reopen(&sim, &mut p).unwrap();
    run_until(
        &sim,
        &mut [&mut p, &mut f],
        |ns: &[&mut Node]| ns[0].db.as_ref().is_some_and(Db::writes_allowed),
        CONVERGE_BUDGET,
    )
    .expect("p's witness gate must open once f answers that it is not a primary");
    p.db.as_mut().unwrap().migrate_schema().unwrap();

    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_converged(&sim, &mut p, &mut f).unwrap();
    assert_eq!(
        f.repl.stats().seeds,
        0,
        "the follower must catch up via shipped MigratePuts, never a re-seed"
    );
    assert_eq!(p.repl.stats().seeds, 0);

    // The follower's rows carry the migrated field...
    let mut rows: Vec<WidgetV2> = f.db.as_ref().unwrap().snapshot().unwrap().scan().unwrap();
    rows.sort_by_key(|r| r.id.raw());
    assert_eq!(rows.len(), 15);
    for r in &rows {
        assert_eq!(
            r.note,
            "v2",
            "row {} must carry the migrated note",
            r.id.raw()
        );
    }
    // ...and its scan_by-visible index state matches the primary's — the
    // close-out apply rebuilt the follower's index at the same log position.
    for k in 0..15u32 {
        assert_eq!(
            count_index_snapshot(p.db.as_ref().unwrap(), k),
            count_index_snapshot(f.db.as_ref().unwrap(), k),
            "index state for count={k} diverged between primary and follower"
        );
    }
}

/// Step 4's own fixture: `migrate_schema`'s batch records are tiny for a
/// handful of rows (well under `FRAMES_BATCH_BYTES`, 256 KiB), so the real
/// shipper would ship an entire small migration in one `Msg::Frames` —
/// nothing to catch "mid-flight". Padding the migrated `note` to ~50 KiB
/// makes each 2-row batch record ~100 KiB, so the shipper genuinely needs
/// multiple rounds to deliver `migration_batch_size: 2` over ~10 rows,
/// letting the test observe (and stop at) a real partial watermark on the
/// follower before killing the primary.
const GIZMO_NOTE_PAD: usize = 50_000;

struct GizmoV1 {
    id: Id<GizmoV1>,
    count: u32,
}
impl Row for GizmoV1 {
    type Scope = Unscoped;
    const TABLE: &'static str = "GizmoV2";
    const INDEXES: &'static [IndexDef<GizmoV1>] = &[];
    const VERSION: u32 = 1;
    const FINGERPRINT: Option<u64> = Some(0x2EB8_0004);
    fn id(&self) -> Id<GizmoV1> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.count.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<GizmoV1> {
        let input = &mut &bytes[..];
        Ok(GizmoV1 {
            id: Id::decode(input)?,
            count: u32::decode(input)?,
        })
    }
}
impl RowShape for GizmoV1 {
    const VERSION: u32 = 1;
    fn encode_row(&self, out: &mut Vec<u8>) {
        Row::encode_row(self, out)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<GizmoV1> {
        <GizmoV1 as Row>::decode_row(bytes)
    }
}

#[table(version = 2, global)]
struct GizmoV2 {
    id: Id<GizmoV2>,
    count: u32,
    note: String,
}
impl TableMigration for GizmoV2 {
    type Prev = GizmoV1;
    fn migrate(prev: GizmoV1) -> Result<GizmoV2, MigrationError> {
        Ok(GizmoV2 {
            id: Id::from_raw(prev.id.raw()),
            count: prev.count,
            note: "x".repeat(GIZMO_NOTE_PAD),
        })
    }
}

fn make_gizmo_schema_v1() -> Schema {
    Schema::new().table::<GizmoV1>()
}

fn make_gizmo_schema_v2() -> Schema {
    Schema::new()
        .table::<GizmoV2>()
        .migration::<GizmoV2>("GizmoV2")
}

fn gizmo_content(db: &Db) -> Vec<(u64, u32, String)> {
    let mut rows: Vec<GizmoV2> = db.snapshot().unwrap().scan().unwrap();
    rows.sort_by_key(|r| r.id.raw());
    rows.into_iter()
        .map(|r| (r.id.raw(), r.count, r.note))
        .collect()
}

/// Seed 0x2EB8_0004: a primary mid-migration (some batches shipped, not all)
/// dies; the promoted follower's `do_promote` gate (T7) resumes
/// `migrate_schema` from exactly the shipped watermark and finishes it,
/// landing on the same content as an uninterrupted single-node twin.
#[test]
fn promote_mid_migration_resumes_from_the_shipped_watermark() {
    let sim = Sim::with_faults(0x2EB8_0004, FaultConfig::none());
    let db_cfg = DbConfig {
        migration_batch_size: 2,
        ..no_gc_cfg()
    };
    const ROWS: u64 = 10;

    // Pre-upgrade history on p's disk.
    {
        let h = sim.host("p");
        let mut seed_db =
            Db::open(Box::new(h.disk()), db_cfg.clone(), make_gizmo_schema_v1()).unwrap();
        seed_db
            .transact(|tx| -> Result<(), ()> {
                for i in 0..ROWS {
                    tx.put(&GizmoV1 {
                        id: Id::from_raw(i),
                        count: i as u32,
                    });
                }
                Ok(())
            })
            .unwrap();
        seed_db.checkpoint().unwrap();
    }

    let mut p = spawn_node_with_schema(
        &sim,
        "p",
        primary_cfg("p:1"),
        db_cfg.clone(),
        make_gizmo_schema_v2(),
    )
    .unwrap();
    let mut f = spawn_node_with_schema(
        &sim,
        "f",
        follower_cfg("f:1", "p:1"),
        db_cfg.clone(),
        make_gizmo_schema_v2(),
    )
    .unwrap();

    // The follower attaches and converges on the original v1 data first
    // (no_gc_cfg: nothing GC'd, ordinary frame shipping, no seed needed).
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_eq!(f.repl.stats().seeds, 0);

    // The primary migrates — 5 batches (batch_size 2 over 10 rows) + 1
    // close-out, each padded batch record alone big enough that the real
    // shipper needs multiple rounds to deliver all of it.
    p.db.as_mut().unwrap().migrate_schema().unwrap();

    // Drive round by round, watching the follower's OWN persisted watermark,
    // until it shows a genuinely in-flight migration — then stop, before it
    // finishes, and kill the primary right there.
    let mut mid_through: Option<u64> = None;
    for _ in 0..64u32 {
        drive_rounds(&sim, &mut [&mut p, &mut f], 1).unwrap();
        let sv: Option<SchemaVersion> =
            f.db.as_ref()
                .unwrap()
                .snapshot()
                .unwrap()
                .get(schema_version_id("GizmoV2"))
                .unwrap();
        if let Some(sv) = sv
            && sv.migrating_to.is_some()
        {
            mid_through = Some(sv.migrating_through.map(|(_, id)| id).unwrap_or(0));
            break;
        }
    }
    let mid_through = mid_through.expect(
        "the follower must be caught mid-migration (a partial, in-flight watermark) before the \
         primary dies — the shipper delivered everything in one shot instead of across rounds",
    );
    eprintln!(
        "caught the follower mid-migration at watermark {mid_through} (of {} rows)",
        ROWS - 1
    );
    assert!(
        mid_through < ROWS - 1,
        "the interrupt must land interior (watermark {mid_through} of {} rows) — at the end, \
         every batch already shipped and the promote would exercise only the empty close-out, \
         never a genuine mid-table resume",
        ROWS - 1
    );

    // Kill the primary — no drain, a real failover — and promote the
    // follower; its do_promote gate (T7) resumes migrate_schema from
    // exactly this shipped watermark.
    p.host.crash_now();
    f.flipped = true;
    let reply = CtrlConn::open(&sim, "f:1", &Msg::CtrlPromote { secret: None })
        .and_then(|mut cli| drive_ctrl(&sim, &mut [&mut f], &mut cli, CONVERGE_BUDGET))
        .unwrap();
    match reply {
        Msg::CtrlOk { .. } => {}
        other => panic!("promote of a mid-migration follower must succeed: {other:?}"),
    }

    // The uninterrupted twin: a fresh single node, seeded with the same rows,
    // migrated in one uninterrupted `migrate_schema` call.
    let twin = {
        let h = sim.host("twin");
        {
            let mut seed_db =
                Db::open(Box::new(h.disk()), db_cfg.clone(), make_gizmo_schema_v1()).unwrap();
            seed_db
                .transact(|tx| -> Result<(), ()> {
                    for i in 0..ROWS {
                        tx.put(&GizmoV1 {
                            id: Id::from_raw(i),
                            count: i as u32,
                        });
                    }
                    Ok(())
                })
                .unwrap();
            seed_db.checkpoint().unwrap();
        }
        let mut db = Db::open(Box::new(h.disk()), db_cfg.clone(), make_gizmo_schema_v2()).unwrap();
        db.migrate_schema().unwrap();
        db
    };

    // Twin equality proves completeness, not exactly-once: GizmoV1
    // prefix-decodes v2 bytes, so re-migrating an already-v2 row is invisible
    // here. Exactly-once is pinned by the non-prefix fixture in
    // charpente-core/tests/migrations.rs (T9).
    let f_content = gizmo_content(f.db.as_ref().unwrap());
    let twin_content = gizmo_content(&twin);
    assert_eq!(f_content.len(), ROWS as usize);
    assert_eq!(
        f_content, twin_content,
        "the promoted node's final table must equal the uninterrupted twin"
    );

    let sv: SchemaVersion = f
        .db
        .as_ref()
        .unwrap()
        .snapshot()
        .unwrap()
        .get(schema_version_id("GizmoV2"))
        .unwrap()
        .expect("GizmoV2's schema_versions row must exist after the promote finished migrating");
    assert_eq!(
        (sv.version, sv.migrating_through, sv.migrating_to),
        (2, None, None),
        "the promoted node's migration must be fully closed out — no in-flight markers left"
    );
}

// ============================================================ control plane
//
// Task 7: switchover (zero-loss), promote (bounded-loss failover), and the
// three-state hand-off state machine. A CLI/test drives the verbs over the
// replication port exactly as the shipped `charpente` CLI will (same-shape
// rule): connect to a node's listener, send a `CtrlSwitchover`/`CtrlPromote`,
// read the `CtrlOk`/`CtrlErr` back on the same conn.

/// The outcome of one offered write — the switchover oracle sorts writes into
/// the acked set (must survive) vs the routed set (the write-stop landed).
enum WriteOutcome {
    Acked,
    NotPrimary,
    /// The witness boot gate is still shut (M2 sub-project 0b task 5): this
    /// node restarted as a primary with recorded witnesses and has not heard
    /// back yet. Deliberately NOT folded into `NotPrimary` — that one means
    /// "the switchover's write-stop landed", which is what the drain oracle
    /// is watching for, and a witness-gated refusal is not that.
    WitnessGated,
    Fallout,
}

/// Offer one widget write at `node`; classify the result. A platform fault
/// recovers the node (fallout, not a bug); a `NotPrimary` rejection is the
/// role gate working. Only a real bug returns `Err`.
fn write_once(sim: &Sim, node: &mut Node, i: u64) -> Result<WriteOutcome, String> {
    let Some(db) = node.db.as_mut() else {
        return Ok(WriteOutcome::Fallout);
    };
    let r = db.transact(|tx| {
        tx.put(&Widget {
            id: Id::from_raw(i),
            kind: i % 3,
            weight: i as i64,
        });
        Ok::<(), io::Error>(())
    });
    match r {
        Ok(()) => Ok(WriteOutcome::Acked),
        Err(TxError::NotPrimary { .. }) => Ok(WriteOutcome::NotPrimary),
        Err(TxError::Io(e)) if is_fault_fallout(&e.to_string()) => {
            recover_node(sim, node)?;
            Ok(WriteOutcome::Fallout)
        }
        Err(TxError::Io(e)) => Err(format!("commit io: {e}")),
        Err(TxError::App(e)) => Err(format!("commit app: {e}")),
        Err(TxError::Verifying { unreached }) => {
            let _ = unreached;
            sim.cover("repl:witness-gated-write");
            Ok(WriteOutcome::WitnessGated)
        }
    }
}

/// A control connection from the out-of-band "cli" host: the conn plus its
/// one recv buffer (allocated once — recv needs a buffer sized to the largest
/// possible message, and allocating it per poll would dominate the universe).
struct CtrlConn {
    conn: Box<dyn Conn>,
    buf: Vec<u8>,
}

impl CtrlConn {
    /// Connect and send the verb. A connect/send failure (the target crashed,
    /// an injected conn drop) is the operator's retry, not a bug: inconclusive.
    fn open(sim: &Sim, addr: &str, verb: &Msg) -> Result<CtrlConn, String> {
        // Cause-keyed like every other inconclusive origin (oracle-density task
        // 3). NOTE for whoever sums these: this site is shared by properties
        // that catch the error into their OWN give-up wrapper — a
        // `repl:failover-inconclusive` universe can carry this key without ever
        // reaching `finish_universe`'s `repl:inconclusive` arm.
        let conn = sim.host("cli").net().connect(addr).map_err(|e| {
            sim.cover("repl:inconclusive:ctrl-connect");
            format!("{INCONCLUSIVE}: ctrl connect {addr}: {e}")
        })?;
        conn.send(&encode(verb)).map_err(|e| {
            sim.cover("repl:inconclusive:ctrl-send");
            format!("{INCONCLUSIVE}: ctrl send: {e}")
        })?;
        Ok(CtrlConn {
            conn,
            buf: vec![0u8; MAX_MESSAGE_BYTES],
        })
    }

    /// Poll for the node's reply. `Ok(None)` — keep driving.
    fn poll(&mut self) -> Result<Option<Msg>, String> {
        match self.conn.recv(&mut self.buf) {
            Ok(Some(0)) => Err("control conn closed before a reply".to_string()),
            Ok(Some(n)) => decode(&self.buf[..n]).map(Some).map_err(|e| e.to_string()),
            Ok(None) => Ok(None),
            Err(e) => Err(format!("ctrl recv: {e}")),
        }
    }
}

/// Drive the given nodes' scheduler until the control conn answers. A lost conn
/// or a quiescent-without-reply state is inconclusive (the peer crashed / the
/// operator path takes over), never a bug.
fn drive_ctrl(
    sim: &Sim,
    nodes: &mut [&mut Node],
    cli: &mut CtrlConn,
    budget: u32,
) -> Result<Msg, String> {
    for _ in 0..budget {
        let advanced = scheduler_round(sim, nodes)?;
        match cli.poll() {
            Ok(Some(m)) => return Ok(m),
            Ok(None) => {}
            Err(e) => {
                sim.cover("repl:inconclusive:ctrl-conn-lost");
                return Err(format!("{INCONCLUSIVE}: {e}"));
            }
        }
        if !advanced {
            sim.cover("repl:inconclusive:ctrl-quiescent");
            return Err(format!("{INCONCLUSIVE}: quiescent before a control reply"));
        }
    }
    sim.cover("repl:inconclusive:ctrl-no-reply");
    Err(format!("{INCONCLUSIVE}: no control reply within budget"))
}

/// At most one node accepts writes — the switchover/failover safety invariant.
fn assert_one_writable(p: &Node, f: &Node) -> Result<(), String> {
    let pw = p.db.as_ref().is_some_and(|d| d.writes_allowed());
    let fw = f.db.as_ref().is_some_and(|d| d.writes_allowed());
    if pw && fw {
        return Err(format!(
            "two writable primaries at once — control-plane safety violated (p role {:?} \
             recoveries {}, f role {:?} recoveries {})",
            p.repl.role(),
            p.recoveries,
            f.repl.role(),
            f.recoveries
        ));
    }
    Ok(())
}

/// Every acked widget id is present in `node`'s store — the zero-loss oracle
/// (switchover) and the drain meta-test's inverting victim both run this.
fn assert_all_present(sim: &Sim, node: &mut Node, ids: &[u64], who: &str) -> Result<(), String> {
    retry_read_reopen(sim, node, recover_node, |node| {
        let db = node.db.as_ref().ok_or_else(|| format!("{who} has no db"))?;
        let snap = db.snapshot().map_err(|e| e.to_string())?;
        let rows = snap
            .scan_raw(Widget::TABLE, false)
            .map_err(|e| e.to_string())?;
        for id in ids {
            if !rows.iter().any(|(_, rid, _)| rid == id) {
                return Err(format!(
                    "acked widget {id} is missing on {who} — the zero-loss oracle caught a lost \
                     record"
                ));
            }
        }
        Ok(())
    })
}

// ------------------------------------------- property: zero-loss switchover

#[test]
fn switchover_mid_write_load_loses_nothing_and_serves_reads_throughout() {
    run_universes_swarm_with!(
        0x2EB1_0009,
        256,
        no_media_corruption,
        |sim: &mut Sim| {
            let r = switchover_universe(sim);
            finish_universe(sim, r)
        },
        require: ["fault:conn_drop", "repl:logical-converged"],
        floor: [("repl:logical-converged", SWITCHOVER_CONVERGED_FLOOR)]
    );
}

/// Universes in which the switchover ran to `CtrlOk` and the FULL oracle ran
/// (zero-loss on the new primary, both roles flipped, then reconvergence),
/// MEASURED: 72 of 256 at base `0x2EB1_0009` on 2026-09-06. The other 184 are
/// 131 `repl:switchover-inconclusive` (a reduced oracle: `assert_one_writable`
/// only), 45 `repl:inconclusive` and 8 `repl:switchover-stalled` — 72 + 131 +
/// 45 + 8 = 256 exactly. `repl:logical-converged` is covered only on the
/// `CtrlOk` arm, which is why it is the verdict key here and the property's old
/// lone `require: ["fault:conn_drop"]` (a FAULT firing, not an oracle running)
/// was not.
///
/// **Headroom: 2 binomial SEs below the measurement**, for the reason
/// `SEEDED_CONVERGED_FLOOR` states in full. At p = 72/256 = 0.281, one SE on the
/// count is sqrt(256 · 0.281 · 0.719) = 7.2, so 72 − 2·7.2 = 57.6 → 57. What
/// that does NOT catch: this floor tolerates a **20.8% relative fall in reach**
/// (72 → 57) before it fires.
///
/// **This property is where a raised retry budget did the most and mattered the
/// least.** At `RECOVER_ATTEMPTS` 5000, `repl:inconclusive` fell 45 → 17
/// (`:recover-exhausted` 43 → 15, 65% of them rescued) — and the rescued
/// universes landed in `repl:switchover-inconclusive` (131 → 158), the
/// reduced-oracle path, moving this key by 1: 72 → 73. Worth knowing beside
/// `SEEDED_CONVERGED_FLOOR`, where the SAME helper's exhaustions barely
/// responded to the same 10× (109 → 105) — so whatever holds those universes is
/// not a wedge in `recover_node` common to both.
///
/// **A drop below this is a finding, not a number to lower.** The likeliest
/// cause is the reduced-oracle path growing: more universes reaching
/// `repl:switchover-inconclusive`, where only `assert_one_writable` runs.
///
/// **Re-measured 2026-09-30 for redb 4.3.0**, two identical batches: 103 of
/// 256 (the parent commit under redb 4.1.0 measures 74, a control run). The
/// give-ups fell 50 → 1, and the rescued universes split between this key and
/// `repl:switchover-inconclusive` (124 → 144), as the retry-budget experiment
/// above predicts. At p = 103/256 = 0.402 one SE is 7.85, so 103 − 2·7.85 =
/// 87.3 → 87; it tolerates a 15.5% relative fall.
///
/// **Re-set 2026-10-01** for S5 and D1 together, two identical batches: 104 of
/// 256 (103 at 796bff2), `repl:switchover-inconclusive` 146, no give-ups. At p
/// = 104/256 = 0.406 one SE is 7.86, so 104 − 2·7.86 = 88.3 → 88; it tolerates
/// a 15.4% relative fall.
///
/// **Re-set 2026-10-02 for the HandOff resend** (D4 round 2), two identical
/// batches: 106 of 256, `repl:switchover-stalled` 6 → 2, one give-up
/// (`repl:inconclusive:role-file-torn`, a universe that now completes its
/// switchover and runs on into a torn role write). Over the same 64 fresh bases
/// the key's mean rose 96.97 (sd 8.41) → 100.16 (sd 8.27), +2.16 Welch SE, and
/// give-ups went 2.94 (sd 1.72) → 3.05 (sd 1.71), +0.36 SE. At p = 106/256 =
/// 0.414 one SE is 7.88, so 106 − 2·7.88 = 90.2 → 90; it tolerates a 15.1%
/// relative fall (16/106).
const SWITCHOVER_CONVERGED_FLOOR: u64 = 90;

/// The result of one switchover attempt's drain.
enum DrainResult {
    Reply(Msg),
    /// The target follower was momentarily disconnected (conn_drop mid-
    /// reconnect) when the verb was processed — retry.
    NoSession,
    /// The primary crashed mid-universe and came back with a recorded witness
    /// it had not yet heard from, so it refused to hand off primacy it could
    /// not confirm it still held (M2 sub-project 0b task 5) — retry.
    ///
    /// This is a RETRY, not a skip, and the distinction is the whole reason
    /// this arm does not weaken spec §4 property 2 (PLAN's M1 exit criterion,
    /// verbatim). The witness here IS `f`, which is live in this rig, so the
    /// gate is transient and self-resolving: the probe reaches it within a
    /// few rounds, the gate opens, and the very next attempt drains a real
    /// switchover that the oracle then adjudicates in full. Tolerating the
    /// refusal as a non-event instead — the tempting one-line fix — would
    /// silently retire the property in exactly the universes where a crash
    /// made it most interesting.
    Verifying,
    /// The control conn was lost or the primary recovered — inconclusive.
    Lost,
}

/// Drive one switchover attempt to its reply under continued write load at p.
/// A follower read must succeed throughout the drain (only an injected fault
/// may fail it).
fn run_switchover_drain(
    sim: &Sim,
    p: &mut Node,
    f: &mut Node,
    cli: &mut CtrlConn,
    acked: &mut Vec<u64>,
    next: &mut u64,
    base_p_recoveries: u32,
) -> Result<DrainResult, String> {
    // Write load continues until the write-stop is observed (the property's
    // point: acked-during-drain writes survive; a NotPrimary is the stop
    // landing). Offering unboundedly would balloon the log in universes where
    // the follower recovers repeatedly (the verb can spend many rounds in
    // flight because rounds with runnable work don't advance virtual time), so
    // the load is also capped — the stop lands within a handful of rounds in
    // any universe that will answer at all.
    let mut stopped = false;
    let mut offered = 0u32;
    for _ in 0..CONVERGE_BUDGET {
        if !stopped && offered < 256 {
            offered += 1;
            match write_once(sim, p, *next)? {
                WriteOutcome::Acked => acked.push(*next),
                WriteOutcome::NotPrimary => stopped = true,
                // Neither acked nor the stop landing: offer another.
                WriteOutcome::WitnessGated | WriteOutcome::Fallout => {}
            }
            *next += 1;
        }
        if let Some(fdb) = f.db.as_ref()
            && let Err(e) = fdb.snapshot()
            && !is_fault_fallout(&e.to_string())
        {
            return Err(format!("follower read rejected during the drain: {e}"));
        }
        let advanced = scheduler_round(sim, &mut [p, f])?;
        match cli.poll() {
            Ok(Some(Msg::CtrlErr { message })) if message.contains("no replication session") => {
                return Ok(DrainResult::NoSession);
            }
            Ok(Some(Msg::CtrlErr { message }))
                if message.contains("still verifying its witnesses") =>
            {
                return Ok(DrainResult::Verifying);
            }
            Ok(Some(m)) => return Ok(DrainResult::Reply(m)),
            Ok(None) => {}
            Err(_) => return Ok(DrainResult::Lost),
        }
        if !advanced {
            return Ok(DrainResult::Lost); // quiescent, no reply coming
        }
        // A primary crash mid-drain loses the switchover state and the control
        // conn — inconclusive. A follower recovering just re-Hellos and the
        // drain continues, so it is not.
        if p.recoveries > base_p_recoveries {
            return Ok(DrainResult::Lost);
        }
    }
    Ok(DrainResult::Lost)
}

fn switchover_universe(sim: &mut Sim) -> Result<(), String> {
    let mut p = spawn_node(sim, "p", primary_cfg("p:1"), DbConfig::default())?;
    let mut f = spawn_node(sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default())?;

    let mut acked: Vec<u64> = Vec::new();
    let mut next = 0u64;
    for _ in 0..12u64 {
        if matches!(write_once(sim, &mut p, next)?, WriteOutcome::Acked) {
            acked.push(next);
        }
        next += 1;
        drive_rounds(sim, &mut [&mut p, &mut f], 3)?;
    }
    let base_p_recoveries = p.recoveries;
    let f_recoveries_at_verb = f.recoveries;
    // From the first verb on, either node's durable role may flip.
    p.flipped = true;
    f.flipped = true;

    // Issue the switchover, retrying with a fresh control conn if the target
    // follower is momentarily reconnecting (conn_drop). Each attempt re-drives
    // convergence so f is connected and caught up before the verb lands.
    let mut reply: Option<Msg> = None;
    for _ in 0..8u32 {
        run_until(sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET)?;
        if p.recoveries > base_p_recoveries {
            break; // p recovered during warm-up — inconclusive below
        }
        let mut cli = match CtrlConn::open(
            sim,
            "p:1",
            &Msg::CtrlSwitchover {
                to: "f:1".to_string(),
                secret: None,
            },
        ) {
            Ok(cli) => cli,
            Err(_) => {
                // The CLI's own conn faulted — retry the verb (the operator's
                // move) after letting the world settle.
                drive_rounds(sim, &mut [&mut p, &mut f], 20)?;
                continue;
            }
        };
        match run_switchover_drain(
            sim,
            &mut p,
            &mut f,
            &mut cli,
            &mut acked,
            &mut next,
            base_p_recoveries,
        )? {
            DrainResult::Reply(m) => {
                reply = Some(m);
                break;
            }
            DrainResult::NoSession => {
                drive_rounds(sim, &mut [&mut p, &mut f], 20)?;
                continue;
            }
            DrainResult::Verifying => {
                // What is MEASURED here, and — deliberately — what is not.
                // An earlier version of this comment asserted a structural
                // mechanism ("reaching this arm implies p recovered inside the
                // switchover loop, so a `continue` lands on the top-of-loop
                // break"). Instrumented cover keys at this base seed say that
                // is FALSE 10 out of 10: every gated universe reaches here with
                // `p.recoveries == base_p_recoveries`. The path that comment
                // missed is that `base_p_recoveries` is captured AFTER the
                // 12-iteration warm-up, and a p that recovers during warm-up is
                // still gated when the baseline is taken — the warm-up's
                // `drive_rounds(3)` bursts cannot span a 500 ms
                // `RECONNECT_NANOS` probe cycle under a virtual clock that only
                // advances when nothing is runnable.
                //
                // MEASURED, at base seed 0x2EB1_0009 over 256 universes:
                //   - 10 universes reach this arm (~3.9%);
                //   - none of them had recovered inside the loop;
                //   - given real headroom the gate DOES open in all 10, so the
                //     earlier `drive_rounds(20)` retry was inert because 20
                //     rounds is ~2 orders of magnitude short of a probe cycle,
                //     NOT because retrying was structurally pointless;
                //   - and even with an unbounded gate wait, 0 of 10 reach
                //     `CtrlOk`. THAT is why `break` is the right shape.
                //
                // NOT measured, and left undiagnosed on purpose rather than
                // guessed at: WHY a gated universe never reaches `CtrlOk` even
                // with headroom, and whether the 10/256 rate holds at other
                // base seeds.
                //
                // The consequence is stated rather than hidden: the zero-loss
                // oracle does not adjudicate those ~3.9% of universes. That is
                // a loss of REACH, not of teeth — a universe that does reach
                // `CtrlOk` runs the full oracle with nothing softened, and
                // `assert_one_writable` still runs on every universe that ends
                // here via the `reply.is_none()` path below.
                sim.cover("repl:switchover-witness-gated");
                break;
            }
            DrainResult::Lost => break,
        }
    }

    // A primary crash mid-drain (or a target that never reconnected) hands the
    // switchover to operator resolution (spec §2); the automated zero-loss
    // oracle only adjudicates a clean run.
    let Some(reply) = reply else {
        sim.cover("repl:switchover-inconclusive");
        return assert_one_writable(&p, &f);
    };

    match reply {
        Msg::CtrlOk { .. } => {
            assert_one_writable(&p, &f)?;
            if !f.db.as_ref().is_some_and(|d| d.writes_allowed()) {
                await_reverified_target(sim, &mut p, &mut f, f_recoveries_at_verb)?;
            }
            if !matches!(f.repl.role(), Role::Primary) {
                return Err(format!(
                    "f's role is not Primary after switchover: {:?}",
                    f.repl.role()
                ));
            }
            if p.db.as_ref().is_some_and(|d| d.writes_allowed()) {
                return Err("p is still writable after handing off".to_string());
            }
            if !matches!(p.repl.role(), Role::Follower { .. }) {
                return Err(format!(
                    "p's role is not Follower after switchover: {:?}",
                    p.repl.role()
                ));
            }
            assert_all_present(sim, &mut f, &acked, "f")?;
            // Keep writing on the new primary; the old primary (now a follower)
            // converges with no divergence.
            for _ in 0..12u64 {
                let _ = write_once(sim, &mut f, next)?;
                next += 1;
                drive_rounds(sim, &mut [&mut p, &mut f], 3)?;
            }
            run_until(sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET)?;
            assert_converged(sim, &mut p, &mut f)?;
            Ok(())
        }
        Msg::CtrlErr { message } => {
            // The hand-off was sent and no confirmation came back (spec §2):
            // either f promoted and the confirmation was lost, or f has not
            // taken it. Over 64 fresh bases (D4 round 2) the second case fell
            // from 229 to 32 once a reconnect's verified Hello position counted
            // as acked: 17 with the HandOff still in flight at the deadline,
            // which f then takes
            // (`a_hand_off_in_flight_at_the_deadline_can_land_after_the_stall_reply`),
            // and 15 with f down past it after its role write failed
            // (`a_switchover_stalled_on_a_never_promoted_target_says_so_and_its_remedy_loses_nothing`).
            // A drain abort (the other CtrlErr, after which p resumes writes)
            // fell from 174 to 45. Neither breaks safety, which
            // `assert_one_writable` checks; this is not the happy-path oracle.
            sim.cover("repl:switchover-stalled");
            if !message.contains("switchover") {
                return Err(format!("unexpected CtrlErr from a switchover: {message}"));
            }
            assert_one_writable(&p, &f)
        }
        other => Err(format!("unexpected switchover reply: {other:?}")),
    }
}

/// `f` unwritable when the `CtrlOk` arrives has one sanctioned cause.
/// `accept_handoff` allows writes before it sends `HandOffDone`, and `p` sends
/// `CtrlOk` only once it reads that, so `f` was writable when the reply left.
/// What can shut it again is a restart of `f` after it recorded `p`, now its
/// follower, as a witness: `f` boots as a durable primary with a non-empty
/// witness set, and the boot gate withholds writes until `p` answers "not a
/// primary" (DESIGN §5; GUIDE §10, "What a restarting primary does"). In seeds
/// `0xbab47736c4b9ccb1`, `0x5b3716b404fe0d0e` and `0x86e23800809100c2` the
/// fsync of that witness write fails, which stops `f` by design.
///
/// So the instant is excused only when all three hold: `f` restarted since
/// the verb, its gate is `Verifying` rather than `NotPrimary`, and its role is
/// `Primary`. Even then only until the gate opens: `p` is live and a follower
/// here, and `WITNESS_DEADLINE_NANOS` is where the platform itself calls a
/// witness unresolved. At most one node is writable at every round of the wait.
fn await_reverified_target(
    sim: &Sim,
    p: &mut Node,
    f: &mut Node,
    f_recoveries_at_verb: u32,
) -> Result<(), String> {
    let restarted = f.recoveries > f_recoveries_at_verb;
    let verifying = f.db.as_ref().is_some_and(|d| d.verifying());
    let primary = matches!(f.repl.role(), Role::Primary);
    if !(restarted && verifying && primary) {
        return Err(format!(
            "f is not writable after the switchover completed, and not for the boot gate's \
             reason (f restarted since the verb: {restarted}; f's gate is verifying: \
             {verifying}; f's role: {:?})",
            f.repl.role()
        ));
    }
    sim.cover("repl:switchover-target-reverified");
    let deadline = sim.now().as_nanos() + WITNESS_DEADLINE_NANOS;
    let mut stopped = "the step budget ran out";
    for _ in 0..CONVERGE_BUDGET {
        if f.db.as_ref().is_some_and(|d| d.writes_allowed()) {
            return Ok(());
        }
        if sim.now().as_nanos() > deadline {
            stopped = "the witness deadline passed";
            break;
        }
        if !scheduler_round(sim, &mut [p, f])? {
            stopped = "the world went quiescent";
            break;
        }
        assert_one_writable(p, f)?;
    }
    // Only what was observed: the gate can stay shut with p answering (a gate
    // that never opens on a resolved set), so this does not name a cause.
    Err(format!(
        "f's witness gate never opened after the switchover completed: f restarted as a primary \
         with p in its witness set, and {stopped} ({}s budget, WITNESS_DEADLINE_NANOS) with f \
         still write-stopped (f's gate is verifying: {}; f's role: {:?}; p's role: {:?})",
        WITNESS_DEADLINE_NANOS / 1_000_000_000,
        f.db.as_ref().is_some_and(|d| d.verifying()),
        f.repl.role(),
        p.repl.role()
    ))
}

/// Replays one universe of `switchover_mid_write_load_loses_nothing_and_serves_reads_throughout`
/// exactly as its swarm runner builds it, and returns the world with the
/// universe's result after `finish_universe`.
fn replay_switchover(seed: u64) -> (Sim, Result<(), String>) {
    let mut state = seed;
    let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
    let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
    charpente_sim::sample_extended_fields(seed, &mut cfg);
    no_media_corruption(&mut cfg);
    let mut sim = Sim::with_faults(seed, cfg);
    let r = switchover_universe(&mut sim);
    let r = finish_universe(&sim, r);
    (sim, r)
}

/// The three universes D4 found failing "f is not writable after the
/// switchover completed" (bases `0x51D31002`, `0xD4F20012`, `0xD4F20025`). In
/// each, `f` restarted after recording `p` as its witness and read the
/// `CtrlOk` while still verifying, and the gate opened within 3–17 ms (virtual)
/// of it. Each now waits that out under the excuse's key and then runs the full
/// oracle, zero-loss and convergence included. A seed names a universe: if one
/// stops covering the key, it no longer meets the scenario and must be re-found
/// by mechanism, not dropped.
#[test]
fn a_switchover_target_restarted_after_the_hand_off_writes_once_its_witness_answers() {
    for seed in [
        0xbab4_7736_c4b9_ccb1_u64,
        0x5b37_16b4_04fe_0d0e,
        0x86e2_3800_8091_00c2,
    ] {
        let (sim, r) = replay_switchover(seed);
        assert_eq!(r, Ok(()), "seed {seed:#x}");
        assert_eq!(sim.gave_up(), None, "seed {seed:#x} reaches a verdict");
        let coverage = sim.coverage();
        assert_eq!(
            coverage.get("repl:switchover-target-reverified"),
            Some(&1),
            "seed {seed:#x}: for this reason, or the seed no longer names the scenario: \
             {coverage:?}"
        );
        assert_eq!(
            coverage.get("repl:logical-converged"),
            Some(&1),
            "seed {seed:#x}: and the full oracle ran after the gate opened: {coverage:?}"
        );
    }
}

// ---------------------------------------------- property: bounded failover

/// No checkpoint/GC: the whole log stays retained from the start, so the
/// prefix oracle can read p's every durable frame back after the crash.
fn no_gc_cfg() -> DbConfig {
    DbConfig {
        wal: WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 256,
        },
        store_path: "tables.redb".to_string(),
        retain_segments: 1024,
        checkpoint_every: 0,
        event_retention: 65_536,
        migration_batch_size: 1000,
    }
}

#[test]
fn failover_preserves_everything_the_follower_acked() {
    run_universes_swarm_with!(
        0x2EB1_000A,
        128,
        no_media_corruption,
        |sim: &mut Sim| {
            let r = failover_universe(sim);
            finish_universe(sim, r)
        },
        require: ["restart", "crash:host", "repl:failover-prefix-compared"],
        floor: [("repl:failover-prefix-compared", FAILOVER_PREFIX_FLOOR)]
    );
}

/// Universes in which the prefix oracle actually compared at least one of f's
/// frames against p's durable log, MEASURED: 46 of 128 at base `0x2EB1_000A` on
/// 2026-09-06. The other 82 all give up, and since oracle-density task 4 routed
/// this property's four `repl:failover-inconclusive` sites through the shared
/// `INCONCLUSIVE` convention they all reach `finish_universe`, so
/// `repl:inconclusive` now counts the whole 82: 41 `:failover-staging-gate` + 2
/// `:failover-promote-timeout` (together the 43 `repl:failover-inconclusive`) +
/// 39 `:recover-exhausted`, and 46 + 82 = 128 exactly. The two keys are no
/// longer mutually exclusive — `repl:failover-inconclusive` is now a subset of
/// `repl:inconclusive`, not a disjoint channel beside it.
/// `repl:failover-prefix-empty` measured 0: every universe that reached the
/// oracle had something to compare.
///
/// This count did not move under either intervention tried on it: a 10× retry
/// budget, and gating `reopen_p_frames`'s power-cycle. Both moved the give-up
/// causes around and left this key at exactly 46 (task-3-report.md §1, §10).
///
/// **Headroom: 2 binomial SEs below the measurement**, for the reason
/// `SEEDED_CONVERGED_FLOOR` states in full. At p = 46/128 = 0.359, one SE on the
/// count is sqrt(128 · 0.359 · 0.641) = 5.4, so 46 − 2·5.4 = 35.1 → 35. What
/// that does NOT catch: this floor tolerates a **23.9% relative fall in reach**
/// (46 → 35) before it fires.
///
/// **A drop below this is a finding, not a number to lower.** The staging gate
/// (`p` still a live primary, `f` never recovered, `f` holding a position) is
/// what most often denies this oracle its universe; a fall here means that gate
/// closed more often, and why is the question.
///
/// **Re-measured 2026-09-30 for redb 4.3.0**, two identical batches: 51 of 128
/// (the parent commit under redb 4.1.0 measures 42, a control run). The 33
/// `:recover-exhausted` give-ups are gone. The staging gate now takes most of
/// what they freed (52 → 75 `:failover-staging-gate`), and 77 give up in all.
/// At p = 51/128 = 0.398 one SE is 5.54, so 51 − 2·5.54 = 39.9 → 39; it
/// tolerates a 23.5% relative fall.
///
/// **Re-set 2026-10-01** for S5 and D1 together, two identical batches: 54 of
/// 128 (51 at 796bff2). All 74 give-ups are `:failover-staging-gate`. At p =
/// 54/128 = 0.422 one SE is 5.59, so 54 − 2·5.59 = 42.8 → 42; it tolerates a
/// 22.2% relative fall.
const FAILOVER_PREFIX_FLOOR: u64 = 42;

fn failover_universe(sim: &mut Sim) -> Result<(), String> {
    let cfg = no_gc_cfg();
    let mut p = spawn_node(sim, "p", primary_cfg("p:1"), cfg.clone())?;
    let mut f = spawn_node(sim, "f", follower_cfg("f:1", "p:1"), cfg.clone())?;

    for next in 0..30u64 {
        let _ = write_once(sim, &mut p, next)?;
        drive_rounds(sim, &mut [&mut p, &mut f], 2)?;
    }
    // Let f follow further — it may still lag (the DECISIONS §6 window).
    drive_rounds(sim, &mut [&mut p, &mut f], 40)?;

    // Stage the crash: p must still be a live primary, f must hold a position.
    // MEASURED (oracle-density task 2, base seed 0x2EB1_000A, 128 universes):
    // this is by far this property's biggest give-up channel, 41/43 of the
    // `repl:failover-inconclusive` universes (task-2-report.md). Routed
    // through the shared `INCONCLUSIVE` convention (oracle-density task 4) so
    // a marker grep finds it and `finish_universe` credits it, same as every
    // other give-up in this file.
    if p.db.is_none() || !matches!(p.repl.role(), Role::Primary) || f.recoveries > 0 {
        sim.cover("repl:failover-inconclusive");
        sim.cover("repl:inconclusive:failover-staging-gate");
        return Err(format!(
            "{INCONCLUSIVE}: failover staging gate: p is no longer a live primary or f already \
             recovered before the crash could be staged (p.db present: {}, p role: {:?}, f \
             recoveries: {})",
            p.db.is_some(),
            p.repl.role(),
            f.recoveries
        ));
    }
    // MEASURED (oracle-density task 2): never fired in 43 give-up universes at
    // this base seed. Two disjuncts, and only the second is live here. `f.db`
    // is cleared at exactly two sites reachable from this prelude:
    // `recover_node` (`:353`), which increments `recoveries` at `:351` first,
    // and `step_node`'s reseed `db.take()` (`:485`). That second take cannot
    // still be observed by the time control returns here: its `Ok` arm puts a
    // `Db` straight back, its fault-fallout arm hands off to `recover_node`
    // (`recoveries` again), and its remaining `Err` arm propagates out through
    // `drive_rounds`' `?` before this line runs. So `f.db.is_none()` here
    // implies `f.recoveries > 0`, which the gate above already returned for:
    // the first disjunct is dead. The second (`last_applied() == None` on a
    // live db) is not — a follower stalled by conn-drop/net-spike faults for
    // the whole 30-write warm-up plus the 40-round settle, never hitting a
    // fault severe enough to force a `recover_node`, applies zero frames.
    // Keyed rather than asserted dead for that reason: a stronger net-fault
    // trial, a shorter warm-up, or more universes could reach it.
    let Some(f_last) = f.db.as_ref().and_then(|d| d.last_applied()) else {
        sim.cover("repl:failover-inconclusive");
        sim.cover("repl:inconclusive:failover-f-not-caught-up");
        return Err(format!(
            "{INCONCLUSIVE}: failover: f has not applied any frames yet, so there is no \
             last_applied reference to fail over from"
        ));
    };

    // Host crash on p — no drain (a real failover).
    p.host.crash_now();

    // Promote f (drive f alone; p is down).
    f.flipped = true;
    let reply = match CtrlConn::open(sim, "f:1", &Msg::CtrlPromote { secret: None })
        .and_then(|mut cli| drive_ctrl(sim, &mut [&mut f], &mut cli, CONVERGE_BUDGET))
    {
        Ok(m) => m,
        // MEASURED (oracle-density task 2): 2/43 give-up universes — and both
        // were `repl:inconclusive:ctrl-conn-lost`, with `:ctrl-quiescent` and
        // `:ctrl-no-reply` at zero (task 4 fix round). The `-timeout` in this
        // key's name is historical and NOT a diagnosis: the site admits every
        // `INCONCLUSIVE` shape `CtrlConn::open`/`drive_ctrl` can return, of
        // which a timeout is only one, and it is not the one observed here.
        // `e` is already `INCONCLUSIVE`-marked and covered at its origin site,
        // so this arm adds only the promote-specific key and must not
        // re-prefix the marker.
        Err(e) if e.contains(INCONCLUSIVE) => {
            sim.cover("repl:failover-inconclusive");
            sim.cover("repl:inconclusive:failover-promote-timeout");
            return Err(format!("failover promote: {e}"));
        }
        Err(e) => return Err(e),
    };
    match reply {
        Msg::CtrlOk { message } => {
            if !message.contains("last_applied") {
                return Err(format!("promote CtrlOk must name last_applied: {message}"));
            }
        }
        Msg::CtrlErr { message } => return Err(format!("promote refused unexpectedly: {message}")),
        other => return Err(format!("unexpected promote reply: {other:?}")),
    }
    // A recovery *during* the promote round is a give-up: the CtrlOk only
    // proves the round finished, not that nothing perturbed f while
    // `drive_ctrl` pumped its scheduler rounds — a disk fault mid-round routes
    // through `step_node` into `recover_node` (incrementing `recoveries`) and
    // f can still answer the control conn on a later round. The frames this
    // oracle is about to read would then not be a fair comparison against p.
    // MEASURED (oracle-density task 2): never fired in 43 give-up universes at
    // this base seed; keyed rather than asserted dead.
    if f.recoveries > 0 {
        sim.cover("repl:failover-inconclusive");
        sim.cover("repl:inconclusive:failover-post-promote-gate");
        return Err(format!(
            "{INCONCLUSIVE}: failover: f recovered {} time(s) during its own promotion, so its \
             frames are no longer a fair comparison against p",
            f.recoveries
        ));
    }
    // Writability, by contrast, is NOT a give-up. `Repl::do_promote` calls
    // `db.allow_writes()` (`repl/mod.rs:909`) BEFORE `reply_ctrl` (`:916`), so
    // the CtrlOk above is itself the promise that writes are already open; and
    // with `f.recoveries == 0` nothing has replaced that `Db` since. A
    // non-writable f here is therefore a promotion violation, and folding it
    // into the give-up above would let a control-plane bug report as green.
    if !f.db.as_ref().is_some_and(|d| d.writes_allowed()) {
        return Err(format!(
            "promote contract violated: f replied CtrlOk to CtrlPromote yet does not allow \
             writes (f.db live: {}, f role {:?}, recoveries 0) — `do_promote` must allow writes \
             before it acks (repl/mod.rs:909 before :916); fix that ordering, not this check",
            f.db.is_some(),
            f.repl.role()
        ));
    }

    // f's WAL frames up to f_last must be a byte-exact prefix of p's durable
    // log — f applied exactly p's records, a consistent prefix (property 3 in
    // the spec matrix). Its store therefore holds every acked id whose seq is
    // ≤ f_last; ids beyond are the honest failover window.
    let f_frames = {
        let fdb = f.db.as_ref().expect("f live");
        retry_fallout(|| {
            fdb.read_log(fdb.log_base(), usize::MAX)
                .map_err(|e| e.to_string())
        })?
    };
    let p_frames = reopen_p_frames(sim, &mut p, &cfg)?;
    let mut compared = 0u32;
    for (seq, bytes) in &f_frames {
        if *seq > f_last {
            continue;
        }
        match p_frames.iter().find(|(s, _)| s == seq) {
            Some((_, pb)) if pb == bytes => compared += 1,
            Some(_) => {
                return Err(format!(
                    "f's frame at seq {seq} diverges from p's — not a prefix of p's timeline"
                ));
            }
            None => {
                return Err(format!(
                    "f holds a frame at seq {seq} that p never had — a forked timeline"
                ));
            }
        }
    }
    // THE verdict key for this property (oracle-density task 3). Reaching this
    // line is not enough: a universe where f held no frame at or below `f_last`
    // runs the loop zero times and returns Ok having compared nothing — green
    // by vacuity, indistinguishable in the coverage from a real prefix check.
    // The two outcomes are keyed apart so the floor below can pin the real one.
    //
    // `> 0` is a shallow bar on purpose, and the depth behind it is MEASURED
    // rather than hoped for: over these 128 universes the 46 that compare split
    // 44 at ≥16 frames, 1 at 5-15, 1 at 2-4 and **0 at exactly one frame**
    // (fix round 1, temporary bucket keys, reverted). So no universe currently
    // passes on a single-frame comparison, and raising the bar to a round number
    // would move the floor's measured value while catching nothing that exists.
    // If `repl:failover-prefix-empty` (0 today) or a shallow tail ever grows,
    // the bar is the thing to revisit — that is what keying the empty case
    // separately is for.
    if compared > 0 {
        sim.cover("repl:failover-prefix-compared");
    } else {
        sim.cover("repl:failover-prefix-empty");
    }
    Ok(())
}

/// Reopen p's crashed disk and read its full durable log — the prefix oracle's
/// reference. Retried under open-time faults; inconclusive if it can't recover.
///
/// The power-cycle is GATED (`RECOVER_CYCLE_EVERY`), not per-attempt: this
/// helper was the fourth site still on the shape Task 1 removed from
/// `recover_auth_node`/`recover_rt_node`, which `settle_boot` measured at 0
/// successes in 20_000 boots — `Store::apply` commits at `Durability::None`, so
/// a restart before every try discards the uncheckpointed store and forces a
/// full WAL rebuild at the same per-write failure odds, forever. Attempt 0 still
/// cycles, which is what a crashed sim disk needs to answer anything at all.
fn reopen_p_frames(
    sim: &Sim,
    node: &mut Node,
    cfg: &DbConfig,
) -> Result<Vec<(u64, Vec<u8>)>, String> {
    for attempt in 0..RECOVER_ATTEMPTS {
        if attempt.is_multiple_of(RECOVER_CYCLE_EVERY) {
            node.host.restart();
        }
        match Db::open(Box::new(node.host.disk()), cfg.clone(), make_schema()) {
            Ok(db) => match db.read_log(db.log_base(), usize::MAX) {
                Ok(frames) => return Ok(frames),
                Err(e) if is_fault_fallout(&e.to_string()) => continue,
                Err(e) => return Err(format!("reopen p read_log: {e}")),
            },
            Err(e) if is_fault_fallout(&e.to_string()) => continue,
            Err(e) => return Err(format!("reopen p: {e}")),
        }
    }
    sim.cover("repl:inconclusive:reopen-p-exhausted");
    Err(format!(
        "{INCONCLUSIVE}: reopening p exhausted {RECOVER_ATTEMPTS} attempts"
    ))
}

// -------------------------- property: no two writable primaries on a crash

#[test]
fn crash_between_handoff_and_demote_never_yields_two_writable_primaries() {
    use charpente::repl::SwitchoverPhase;

    let sim = Sim::with_faults(0x2EB1_000B, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    for i in 0..10u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_one_writable(&p, &f).unwrap();
    // Clear-on-flip, site 2 sanity (task-2 dispatch, Correction 1): p must
    // have durably recorded f as a witness before anything below flips it
    // away from Primary, or the clear assertion at the end proves nothing.
    assert_eq!(
        load_witnesses(&p.host.disk()).unwrap(),
        vec!["f:1".to_string()],
        "sanity: p must have durably recorded f as a witness before the crash/resolve below"
    );

    // Begin the switchover and drive to the HandedOff instant (HandOff sent,
    // HandOffDone not yet processed).
    p.flipped = true;
    f.flipped = true;
    let _cli = CtrlConn::open(
        &sim,
        "p:1",
        &Msg::CtrlSwitchover {
            to: "f:1".to_string(),
            secret: None,
        },
    )
    .unwrap();
    run_until(
        &sim,
        &mut [&mut p, &mut f],
        |ns| ns[0].repl.switchover_phase() == Some(SwitchoverPhase::HandedOff),
        CONVERGE_BUDGET,
    )
    .unwrap();
    assert_eq!(p.repl.switchover_phase(), Some(SwitchoverPhase::HandedOff));
    assert_one_writable(&p, &f).unwrap();

    // Crash p's host before it processes HandOffDone.
    p.host.crash_now();
    assert_one_writable(&p, &f).unwrap();

    // f promotes via the in-flight HandOff (or already has).
    run_until(
        &sim,
        &mut [&mut f],
        |ns| ns[0].db.as_ref().is_some_and(|d| d.writes_allowed()),
        CONVERGE_BUDGET,
    )
    .unwrap();
    assert!(matches!(f.repl.role(), Role::Primary), "f must be primary");
    assert_one_writable(&p, &f).unwrap();

    // Restart p with its durable HandingOff role: it must come up write-stopped
    // and resolve against f (now primary), demoting to Follower{f}.
    recover_node(&sim, &mut p).unwrap();
    assert!(
        matches!(p.repl.role(), Role::HandingOff { .. }),
        "p must reopen write-stopped in HandingOff, got {:?}",
        p.repl.role()
    );
    assert_one_writable(&p, &f).unwrap();

    // Drive both; check the safety invariant after every round until p demotes.
    let mut demoted = false;
    for _ in 0..CONVERGE_BUDGET {
        if matches!(p.repl.role(), Role::Follower { .. }) {
            demoted = true;
            break;
        }
        scheduler_round(&sim, &mut [&mut p, &mut f]).unwrap();
        assert_one_writable(&p, &f).unwrap();
    }
    assert!(demoted, "p must resolve to a follower of f");
    assert_one_writable(&p, &f).unwrap();
    // Clear-on-flip, site 2 (task-2 dispatch, Correction 1): `resolve_to_follower`
    // (mod.rs's crash-interrupted-switchover demote path, distinct from the
    // graceful `complete_switchover` covered by
    // `a_graceful_switchover_clears_the_outgoing_primarys_witnesses`) must
    // clear p's witnesses on its durable flip to Follower.
    assert_eq!(
        load_witnesses(&p.host.disk()).unwrap(),
        Vec::<String>::new(),
        "resolve_to_follower must clear p's witnesses on the durable demote — a stale witness \
         would block writes after a future legitimate promotion"
    );

    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_converged(&sim, &mut p, &mut f).unwrap();
}

/// A switchover driven to its point of no return: `p` is HandedOff, with the
/// `HandOff` sent and not yet taken by `f`. Returns the world with the verb's
/// control conn still waiting, and `p`'s hand-off seq.
fn handed_off_to_f() -> (Sim, Node, Node, CtrlConn, u64) {
    use charpente::repl::SwitchoverPhase;

    let sim = Sim::with_faults(STALLED_SWITCHOVER_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    for i in 0..10u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    p.flipped = true;
    f.flipped = true;
    let cli = CtrlConn::open(
        &sim,
        "p:1",
        &Msg::CtrlSwitchover {
            to: "f:1".to_string(),
            secret: None,
        },
    )
    .unwrap();
    run_until(
        &sim,
        &mut [&mut p, &mut f],
        |ns| ns[0].repl.switchover_phase() == Some(SwitchoverPhase::HandedOff),
        CONVERGE_BUDGET,
    )
    .unwrap();
    let at =
        p.db.as_ref()
            .unwrap()
            .last_applied()
            .expect("handed off at a durable seq");
    assert!(
        matches!(f.repl.role(), Role::Follower { .. }),
        "sanity: f has not taken the HandOff yet: {:?}",
        f.repl.role()
    );
    (sim, p, f, cli, at)
}

/// The `HandOff` resend (D4 round 2). A target that dies before the `HandOff`
/// lands reboots as the follower its role file says it is and reconnects
/// already caught up. A write-stopped `p` ships it nothing, so it never acks
/// again; the `Hello`'s position, which `p` verified against its own log, is
/// what says it holds every record up to the hand-off seq. `p` resends on the
/// new session, `f` takes it, and the switchover completes with nothing lost.
/// D4 found 229 of 635 `CtrlErr`s over 64 fresh bases stalled this way, with
/// no node writable, before the resend counted that position.
#[test]
fn a_switchover_target_that_restarts_before_taking_the_hand_off_is_sent_it_again() {
    let (sim, mut p, mut f, mut cli, at) = handed_off_to_f();
    f.host.crash_now();
    recover_node(&sim, &mut f).unwrap();
    assert!(
        matches!(f.repl.role(), Role::Follower { .. }),
        "sanity: f never applied the HandOff, so it reboots a follower: {:?}",
        f.repl.role()
    );
    let reply = drive_ctrl(&sim, &mut [&mut p, &mut f], &mut cli, CONVERGE_BUDGET).unwrap();
    assert!(
        matches!(&reply, Msg::CtrlOk { .. }),
        "a target that reconnects caught up inside the window must be handed off again: \
         {reply:?}"
    );
    assert_eq!(f.repl.role(), &Role::Primary);
    assert!(f.db.as_ref().is_some_and(|d| d.writes_allowed()));
    assert_eq!(
        f.db.as_ref().unwrap().last_applied(),
        Some(at),
        "f took the hand-off at p's own seq"
    );
    assert_eq!(
        p.repl.role(),
        &Role::Follower {
            primary: "f:1".to_string()
        }
    );
    assert_one_writable(&p, &f).unwrap();
    assert_all_present(&sim, &mut f, &(0..10).collect::<Vec<_>>(), "f").unwrap();
    commit_widget(&sim, &mut f, 10).unwrap();
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_converged(&sim, &mut p, &mut f).unwrap();
}

/// A switchover whose target never took the `HandOff` and did not come back
/// inside the window: `f` stays down until `p`'s deadline passes, so no
/// reconnect reaches `p` to be handed off again (15 of the 32 never-promoted
/// stalls left over 64 fresh bases, D4 round 2, where `f`'s role write failed
/// inside `accept_handoff` and its reboot outlasted the window). `f` then reboots
/// as the follower its role file says it is. Returns the world at the stall,
/// with `p`'s reply and its hand-off seq.
fn stalled_switchover_target_never_promoted() -> (Sim, Node, Node, String, u64) {
    let (sim, mut p, mut f, mut cli, at) = handed_off_to_f();
    f.host.crash_now();
    let message = match drive_ctrl(&sim, &mut [&mut p], &mut cli, CONVERGE_BUDGET).unwrap() {
        Msg::CtrlErr { message } => message,
        other => panic!("a switchover whose target never took the hand-off must stall: {other:?}"),
    };
    recover_node(&sim, &mut f).unwrap();
    assert!(
        matches!(f.repl.role(), Role::Follower { .. }),
        "sanity: f never applied the HandOff, so it reboots a follower: {:?}",
        f.repl.role()
    );
    (sim, p, f, message, at)
}

const STALLED_SWITCHOVER_SEED: u64 = 0x2EB1_0D40;

/// What is true once a switchover stalls with its target never promoted, and
/// that the stall's `CtrlErr` says exactly that (D4 item 4). No node accepts
/// writes, and nothing changes that on its own: `p` does not resume, and
/// restarting `p` alone leaves it handing off. The remedy the text names does
/// change it. Promoting `f` loses nothing (its reply names `p`'s own
/// hand-off seq), and restarting `p` then rejoins it as `f`'s follower.
#[test]
fn a_switchover_stalled_on_a_never_promoted_target_says_so_and_its_remedy_loses_nothing() {
    let (sim, mut p, mut f, message, at) = stalled_switchover_target_never_promoted();
    assert_eq!(
        message,
        format!(
            "switchover to f:1: the hand-off was sent at seq {at} but no confirmation returned \
             before the deadline — this node stays write-stopped and will not resume writes on \
             its own, because f:1 may already be the primary. If f:1's role file says primary, \
             restart this node to resolve against f:1: it rejoins as f:1's follower. If it \
             still says follower, f:1 has not taken the hand-off yet and no node accepts writes: \
             promote it (charpente promote --node f:1) — it confirmed it holds every record up \
             to seq {at} and this node has accepted no write since, so its reply names \
             last_applied Some({at}) and nothing is lost — then restart this node to resolve \
             against f:1. If the promote answers \"cannot promote: this node is already the \
             primary\", f:1 took a hand-off still in flight at the deadline: just restart this \
             node"
        )
    );
    let none_writable = |p: &Node, f: &Node| {
        !p.db.as_ref().is_some_and(|d| d.writes_allowed())
            && !f.db.as_ref().is_some_and(|d| d.writes_allowed())
    };
    assert!(none_writable(&p, &f), "no node accepts writes at the stall");

    // Nothing resolves it on its own, across twice the switchover deadline.
    let until = sim.now().as_nanos() + 2 * SWITCHOVER_DEADLINE_NANOS;
    run_until(
        &sim,
        &mut [&mut p, &mut f],
        |_| sim.now().as_nanos() > until,
        CONVERGE_BUDGET,
    )
    .unwrap();
    assert!(
        matches!(p.repl.role(), Role::HandingOff { .. }),
        "{:?}",
        p.repl.role()
    );
    assert!(
        matches!(f.repl.role(), Role::Follower { .. }),
        "{:?}",
        f.repl.role()
    );
    assert!(
        none_writable(&p, &f),
        "p does not resume and nothing resends"
    );

    // The remedy, as the text orders it: promote f, then restart p.
    let mut promote = CtrlConn::open(&sim, "f:1", &Msg::CtrlPromote { secret: None }).unwrap();
    match drive_ctrl(&sim, &mut [&mut p, &mut f], &mut promote, CONVERGE_BUDGET).unwrap() {
        Msg::CtrlOk { message } => assert!(
            message.starts_with(&format!("promoted to primary at last_applied Some({at}) ")),
            "the promote names p's own hand-off seq, so nothing is lost: {message}"
        ),
        other => panic!("promoting the never-promoted target must succeed: {other:?}"),
    }
    recover_node(&sim, &mut p).unwrap();
    let mut rejoined = false;
    for _ in 0..CONVERGE_BUDGET {
        assert_one_writable(&p, &f).unwrap();
        if matches!(p.repl.role(), Role::Follower { .. }) {
            rejoined = true;
            break;
        }
        scheduler_round(&sim, &mut [&mut p, &mut f]).unwrap();
    }
    assert!(rejoined, "the restarted p resolves against the promoted f");
    assert_all_present(&sim, &mut f, &(0..10).collect::<Vec<_>>(), "f").unwrap();
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_converged(&sim, &mut p, &mut f).unwrap();
}

/// The branch the stall text rules out: with the target never promoted,
/// restarting the stalled primary alone leaves it handing off, and still no
/// node accepts writes.
#[test]
fn restarting_a_stalled_primary_alone_does_not_resolve_a_never_promoted_target() {
    let (sim, mut p, mut f, _, _) = stalled_switchover_target_never_promoted();
    recover_node(&sim, &mut p).unwrap();
    let until = sim.now().as_nanos() + 2 * SWITCHOVER_DEADLINE_NANOS;
    run_until(
        &sim,
        &mut [&mut p, &mut f],
        |_| sim.now().as_nanos() > until,
        CONVERGE_BUDGET,
    )
    .unwrap();
    assert!(
        matches!(p.repl.role(), Role::HandingOff { .. }),
        "{:?}",
        p.repl.role()
    );
    assert!(
        matches!(f.repl.role(), Role::Follower { .. }),
        "{:?}",
        f.repl.role()
    );
    assert!(!p.db.as_ref().is_some_and(|d| d.writes_allowed()));
    assert!(!f.db.as_ref().is_some_and(|d| d.writes_allowed()));
}

/// A `HandOff` still in flight at the deadline: 17 of the 32 never-promoted
/// stalls left over 64 fresh bases (D4 round 2), sent on a live conn up to 11 s
/// before a deadline the drain had mostly used up. Here `f` is not stepped
/// until `p` has replied. The reply's second case holds when it is sent, and
/// then `f` takes the `HandOff` on its own and becomes the primary, which is
/// the first case. A promote sent after that answers that `f` is already the
/// primary, which the stall reply names as this race (review M1), and
/// restarting `p` resolves it with nothing lost.
#[test]
fn a_hand_off_in_flight_at_the_deadline_can_land_after_the_stall_reply() {
    let (sim, mut p, mut f, mut cli, at) = handed_off_to_f();
    let stall = match drive_ctrl(&sim, &mut [&mut p], &mut cli, CONVERGE_BUDGET).unwrap() {
        Msg::CtrlErr { message } => {
            assert!(
                message.starts_with(&format!(
                    "switchover to f:1: the hand-off was sent at seq {at} but no confirmation"
                )),
                "{message}"
            );
            message
        }
        other => panic!("p must stall while f has not taken the hand-off: {other:?}"),
    };
    assert!(matches!(f.repl.role(), Role::Follower { .. }));
    assert!(!p.db.as_ref().is_some_and(|d| d.writes_allowed()));
    assert!(!f.db.as_ref().is_some_and(|d| d.writes_allowed()));

    let mut took = false;
    for _ in 0..CONVERGE_BUDGET {
        assert_one_writable(&p, &f).unwrap();
        if matches!(f.repl.role(), Role::Primary) {
            took = true;
            break;
        }
        scheduler_round(&sim, &mut [&mut p, &mut f]).unwrap();
    }
    assert!(took, "f takes the HandOff that was in flight");
    assert!(f.db.as_ref().is_some_and(|d| d.writes_allowed()));
    assert_eq!(f.db.as_ref().unwrap().last_applied(), Some(at));
    assert!(
        matches!(p.repl.role(), Role::HandingOff { .. }),
        "{:?}",
        p.repl.role()
    );

    let mut promote = CtrlConn::open(&sim, "f:1", &Msg::CtrlPromote { secret: None }).unwrap();
    match drive_ctrl(&sim, &mut [&mut p, &mut f], &mut promote, CONVERGE_BUDGET).unwrap() {
        Msg::CtrlErr { message } => {
            assert_eq!(message, "cannot promote: this node is already the primary");
            assert!(
                stall.ends_with(&format!(
                    "If the promote answers \"{message}\", f:1 took a hand-off still in flight \
                     at the deadline: just restart this node"
                )),
                "the stall reply must name the promote reply this race gets: {stall}"
            );
        }
        other => panic!("a promote after the hand-off landed: {other:?}"),
    }
    recover_node(&sim, &mut p).unwrap();
    let mut rejoined = false;
    for _ in 0..CONVERGE_BUDGET {
        assert_one_writable(&p, &f).unwrap();
        if matches!(p.repl.role(), Role::Follower { .. }) {
            rejoined = true;
            break;
        }
        scheduler_round(&sim, &mut [&mut p, &mut f]).unwrap();
    }
    assert!(rejoined, "the restarted p resolves against f");
    assert_all_present(&sim, &mut f, &(0..10).collect::<Vec<_>>(), "f").unwrap();
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_converged(&sim, &mut p, &mut f).unwrap();
}

/// A switchover on a quiet cluster whose target reconnected since the last
/// write (D4 round 3, review I1). The target is shipped nothing and never acks
/// again, so only its verified `Hello` position shows it is caught up. Before
/// D4 round 2 the drain never saw it caught up: no faults at all, and the
/// switchover aborted at the deadline naming a lag of 10 records that `f`
/// already held ("the follower lag is 10 record(s) (it acked None, this
/// primary is at Some(9))"). This pins the drain's half of the fix; the
/// resend's half is
/// `a_switchover_target_that_restarts_before_taking_the_hand_off_is_sent_it_again`.
#[test]
fn a_switchover_after_the_target_reconnected_with_no_write_since_completes() {
    let sim = Sim::with_faults(0x2EB1_0D41, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    for i in 0..10u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    f.host.crash_now();
    recover_node(&sim, &mut f).unwrap();
    drive_rounds(&sim, &mut [&mut p, &mut f], 50).unwrap();
    let at = p.db.as_ref().unwrap().last_applied();
    assert_eq!(
        f.db.as_ref().unwrap().last_applied(),
        at,
        "sanity: f reconnected already caught up"
    );
    p.flipped = true;
    f.flipped = true;
    let mut cli = CtrlConn::open(
        &sim,
        "p:1",
        &Msg::CtrlSwitchover {
            to: "f:1".to_string(),
            secret: None,
        },
    )
    .unwrap();
    let reply = drive_ctrl(&sim, &mut [&mut p, &mut f], &mut cli, CONVERGE_BUDGET).unwrap();
    assert!(
        matches!(&reply, Msg::CtrlOk { .. }),
        "a target that reconnected caught up must be handed off, not aborted: {reply:?}"
    );
    assert_eq!(f.repl.role(), &Role::Primary);
    assert!(f.db.as_ref().is_some_and(|d| d.writes_allowed()));
    assert_eq!(f.db.as_ref().unwrap().last_applied(), at);
    assert_eq!(
        p.repl.role(),
        &Role::Follower {
            primary: "f:1".to_string()
        }
    );
    assert_one_writable(&p, &f).unwrap();
    assert_all_present(&sim, &mut f, &(0..10).collect::<Vec<_>>(), "f").unwrap();
    commit_widget(&sim, &mut f, 10).unwrap();
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_converged(&sim, &mut p, &mut f).unwrap();
}

/// A verified `Hello` position counts as acked only while it is the answer the
/// session is living under (D4 round 3, review M6). `handle` answers a `Hello`
/// in any phase, so a peer that sends a second one on the same connection (no
/// conformant follower does: one `Hello` per connection) gets it decided
/// afresh. Here the first carries `p`'s own last position and is answered
/// `Frames`; the second carries none, on a log GC'd past its base, and is
/// answered `Snapshot`. The peer has now proved nothing, so a switchover to it
/// must drain and abort by name. Kept from the first `Hello`, the position
/// would read as caught up and `p` would hand off to a peer it is seeding.
///
/// That is all the reset holds: a second `Hello` answered `Snapshot` no longer
/// leaves a stale position. A peer that also sends a stale or forged `Ack` can
/// still look caught up. No real follower or witness probe sends one, and what
/// keeps a non-conformant peer out is `peer_secret`.
#[test]
fn a_second_hello_answered_with_a_snapshot_drops_the_position_the_first_proved() {
    let sim = Sim::with_faults(0x2EB1_0D42, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), seeding_db_cfg()).unwrap();
    for i in 0..60u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    let (last, crc, digest) = {
        let db = p.db.as_ref().unwrap();
        assert!(
            db.log_base() > 0,
            "a log-less Hello must be answered Snapshot"
        );
        let last = db.last_applied().unwrap();
        let frames = db.read_log(last, 1).unwrap();
        let (seq, payload) = frames.first().unwrap();
        (
            last,
            charpente_core::log::frame_crc(*seq, payload),
            db.schema_digest(),
        )
    };
    let hello = |position| Msg::Hello {
        proto: charpente::repl::PROTO_VERSION,
        listen: Some("x:1".to_string()),
        position,
        secret: None,
        schema_digest: digest,
    };
    let peer = sim.host("x").net().connect("p:1").unwrap();
    let mut buf = vec![0u8; MAX_MESSAGE_BYTES];
    let mut answer = |sim: &Sim, p: &mut Node| -> Msg {
        for _ in 0..CONVERGE_BUDGET {
            drive_rounds(sim, &mut [&mut *p], 1).unwrap();
            if let Some(n) = peer.recv(&mut buf).unwrap() {
                assert!(n > 0, "p closed the peer's connection");
                return decode(&buf[..n]).unwrap();
            }
        }
        panic!("p never answered the peer");
    };
    peer.send(&encode(&hello(Some((last, crc))))).unwrap();
    let first = answer(&sim, &mut p);
    assert!(
        matches!(first, Msg::Heartbeat { next_seq } if next_seq == last + 1),
        "the first Hello is answered Frames from {}: {first:?}",
        last + 1
    );
    peer.send(&encode(&hello(None))).unwrap();
    let second = answer(&sim, &mut p);
    assert!(
        matches!(second, Msg::SnapshotStart { .. }),
        "the second Hello is answered Snapshot: {second:?}"
    );

    p.flipped = true;
    let mut cli = CtrlConn::open(
        &sim,
        "p:1",
        &Msg::CtrlSwitchover {
            to: "x:1".to_string(),
            secret: None,
        },
    )
    .unwrap();
    match drive_ctrl(&sim, &mut [&mut p], &mut cli, CONVERGE_BUDGET).unwrap() {
        Msg::CtrlErr { message } => assert_eq!(
            message,
            format!(
                "switchover to x:1 aborted: the follower lag is {} record(s) (it has confirmed \
                 no record yet, this primary is at Some({last})) and the drain did not complete \
                 before the 30000000000ns deadline — this node is primary again and accepting \
                 writes",
                last + 1
            )
        ),
        other => panic!("a switchover to a peer that proved nothing must abort: {other:?}"),
    }
    assert_eq!(p.repl.role(), &Role::Primary);
    assert!(p.db.as_ref().is_some_and(|d| d.writes_allowed()));
}

// ------------------------------ property: switchover aborts on a laggard

#[test]
fn switchover_aborts_on_a_lagging_follower_by_name() {
    let sim = Sim::with_faults(0x2EB1_000C, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    for i in 0..10u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();

    // Partition p↔f so the drain can never complete; advance p past f.
    sim.partition("p", "f");
    for i in 10..20u64 {
        commit_widget(&sim, &mut p, i).unwrap();
        drive_rounds(&sim, &mut [&mut p, &mut f], 2).unwrap();
    }
    let p_last = p.db.as_ref().unwrap().last_applied();
    let f_last = f.db.as_ref().unwrap().last_applied();
    assert!(p_last > f_last, "p must be ahead of the partitioned f");

    p.flipped = true;
    f.flipped = true;
    let mut cli = CtrlConn::open(
        &sim,
        "p:1",
        &Msg::CtrlSwitchover {
            to: "f:1".to_string(),
            secret: None,
        },
    )
    .unwrap();
    let reply = drive_ctrl(&sim, &mut [&mut p, &mut f], &mut cli, CONVERGE_BUDGET).unwrap();
    match reply {
        Msg::CtrlErr { message } => assert_eq!(
            message,
            format!(
                "switchover to f:1 aborted: the follower lag is {} record(s) (it confirmed it \
                 holds every record up to {f_last:?}, this primary is at {p_last:?}) and the \
                 drain did not complete before the 30000000000ns deadline — this node is \
                 primary again and accepting writes",
                p_last.unwrap() - f_last.unwrap()
            )
        ),
        other => panic!("a lagging switchover must abort with CtrlErr, got {other:?}"),
    }

    // p is primary again and writes flow.
    assert!(
        matches!(p.repl.role(), Role::Primary),
        "p must be primary again"
    );
    assert!(
        p.db.as_ref().unwrap().writes_allowed(),
        "p must accept writes"
    );
    commit_widget(&sim, &mut p, 100).expect("writes flow after an aborted switchover");
}

// ----------------------------------- meta-test: the drain oracle inverts

/// The victim move the drain exists to prevent: promoting a measurably-behind
/// follower (no drain). The SAME zero-loss oracle switchover uses must catch
/// the lost records — proving the oracle is what keeps switchover honest.
fn promote_without_drain_workload(sim: &mut Sim) -> Result<(), String> {
    let cfg = DbConfig::default();
    let mut p = spawn_node(sim, "p", primary_cfg("p:1"), cfg.clone())?;
    let mut f = spawn_node(sim, "f", follower_cfg("f:1", "p:1"), cfg.clone())?;

    let mut acked: Vec<u64> = Vec::new();
    let mut next = 0u64;
    for _ in 0..12u64 {
        if matches!(write_once(sim, &mut p, next)?, WriteOutcome::Acked) {
            acked.push(next);
        }
        next += 1;
        drive_rounds(sim, &mut [&mut p, &mut f], 3)?;
    }
    run_until(sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET)?;

    // Partition so f falls measurably behind, then keep acking on p.
    sim.partition("p", "f");
    for _ in 0..12u64 {
        if matches!(write_once(sim, &mut p, next)?, WriteOutcome::Acked) {
            acked.push(next);
        }
        next += 1;
        drive_rounds(sim, &mut [&mut p], 2)?;
    }

    // Promote f WITHOUT a drain.
    f.flipped = true;
    let mut cli = CtrlConn::open(sim, "f:1", &Msg::CtrlPromote { secret: None })?;
    match drive_ctrl(sim, &mut [&mut f], &mut cli, CONVERGE_BUDGET)? {
        Msg::CtrlOk { .. } => {}
        other => return Err(format!("promote failed in the meta-test: {other:?}")),
    }

    // The oracle must catch the skipped drain.
    assert_all_present(sim, &mut f, &acked, "f")
}

#[test]
fn the_zero_loss_oracle_catches_a_promote_without_drain() {
    let mut state = 0x2EB1_000D_u64;
    let mut caught: Option<(u64, String)> = None;
    for _ in 0..64 {
        let seed = charpente_sim::rng::splitmix64(&mut state);
        let mut sim = Sim::with_faults(seed, FaultConfig::none());
        if let Err(msg) = promote_without_drain_workload(&mut sim) {
            caught = Some((seed, msg));
            break;
        }
    }
    let (seed, msg) = caught.expect(
        "the zero-loss oracle no longer catches a promote-without-drain — the drain is not being \
         exercised by the switchover path",
    );
    eprintln!("caught promote-without-drain: seed {seed:#018x}: {msg}");
    assert!(
        msg.contains("zero-loss oracle"),
        "the failure must be the oracle catching the loss: {msg}"
    );
}

// ============================================ kernel-level multi-node (T8)
//
// Task 8 wires the data plane into the kernel: a follower's `Kernel` serves
// reads and answers writes with 421, ships its log, but never runs jobs or
// cron (gated on `Repl::is_primary`); a promoted follower rebuilds its due-heap
// from the replicated store. These nodes wrap a `Kernel` + `SimSource` and
// drive it with `Kernel::step` — the same decision code prod runs — where the
// `Node` above drove `Repl` directly. The scheduler mirrors `scheduler_round`
// but one `kernel.step` per node per round IS the pump+has_work+step the
// Db-level scheduler spelled out.

use charpente::http::{ApiError, App, AppConfig, Method, Request, Session};
use charpente::kernel::{CronRow, Kernel, RecordedResponses, SimSource, Step};
use charpente::wire::WireI64;
use charpente::{Tx, job, jobs, route, routes};
use charpente_core::job::JobRow;

#[table(global)]
struct Tally {
    id: Id<Tally>,
    count: u64,
}

#[job(global)]
struct Bump {
    slot: u64,
}

impl Bump {
    fn run(self, tx: &mut Tx) -> Result<(), charpente::kernel::JobError> {
        let count = tx
            .get(Id::<Tally>::from_raw(self.slot))?
            .map_or(0, |t: Tally| t.count);
        tx.put(&Tally {
            id: Id::from_raw(self.slot),
            count: count + 1,
        });
        Ok(())
    }
}

#[route(POST "/widget/:id", public)]
fn put_widget(tx: &mut Tx, id: Id<Widget>) -> Result<Id<Widget>, ApiError> {
    let raw = id.raw();
    tx.put(&Widget {
        id,
        kind: raw % 3,
        weight: raw as i64,
    });
    tx.enqueue(Bump { slot: raw });
    Ok(id)
}

#[route(GET "/widget/:id", public)]
fn get_widget(tx: &mut Tx, id: Id<Widget>) -> Result<WireI64, ApiError> {
    Ok(WireI64(tx.get(id)?.ok_or(ApiError::NotFound)?.weight))
}

fn kernel_schema() -> Schema {
    Schema::new().table::<Widget>().table::<Tally>()
}

/// `kernel_schema()` plus the versioned `WidgetV2` table and its migration —
/// the boot-gate test needs a kernel with something for `migrate_schema` to
/// do, and `kernel_schema()` has no versioned table at all.
fn kernel_schema_v2() -> Schema {
    kernel_schema()
        .table::<WidgetV2>()
        .migration::<WidgetV2>("WidgetV2")
}

/// What a raw `Db::open` needs to write v1 rows into a data dir a kernel App
/// will later mount at v2 (`full_schema()`'s v1 counterpart).
fn kernel_full_schema_v1() -> Schema {
    kernel_schema()
        .table::<WidgetV1>()
        .table::<Session>()
        .table::<CronRow>()
}

/// The schema a raw `Db::open` needs to match a kernel `App`'s: `App::open`
/// adds `Session` + `CronRow`, `Db::open` adds `JobRow` itself.
fn full_schema() -> Schema {
    kernel_schema().table::<Session>().table::<CronRow>()
}

fn backup_cfg(listen: &str, primary: &str) -> ReplConfig {
    ReplConfig {
        listen: Some(listen.to_string()),
        initial_role: Role::Backup {
            primary: primary.to_string(),
        },
        peer_secret: None,
        strict_witnesses: false,
    }
}

/// One replication node driven through its `Kernel` (T8), not `Repl` directly.
struct KNode {
    host: SimHost,
    kernel: Option<Kernel>,
    /// Scripted requests + the recorded `(status, body)` sink.
    source: SimSource,
    responses: RecordedResponses,
    name: String,
    repl_cfg: ReplConfig,
    db_cfg: DbConfig,
    /// The schema this node opens/reopens under (`Node.schema`'s counterpart).
    schema: Schema,
    recoveries: u32,
    flipped: bool,
    /// A designated survivor (the property's clean follower) is never
    /// recovered — a fault on it makes the universe inconclusive, because the
    /// single-follower promise needs one clean copy.
    recoverable: bool,
    /// The rig's forced CRC flip (`corrupt_last_wal_frame`, the sanctioned
    /// `bit_flip` stand-in) ran on this node's WAL in the current round. It
    /// is part of the property's fault model, a fault injected on this node's
    /// disk like any other, so it explains a below-horizon refusal of it
    /// (`unexplained_restore_needed`). The sim's own markers cannot see it.
    /// - **Recorded on the attempt.** A failure at the helper's `sync` leaves
    ///   its write pending, and the round's crash can still land it. A failure
    ///   before the write cannot land, and recording it then changed no
    ///   outcome in the D3 re-review's measurement.
    /// - **Cleared at the top of every round and after a re-seed's wipe.** The
    ///   round's crash settles the write, and the wipe deletes it (D3 re-review
    ///   m6).
    corrupted_by_rig: bool,
    /// `host`'s durable-corruption and entry-loss counts at its last data-dir
    /// wipe (`reseed_p`), zero until one. The wipe deletes every byte an
    /// earlier fault touched, so only a fault since then explains a refusal of
    /// this node (S3b item 8, D3 re-review m7): `corruption_since_wipe`,
    /// `entry_loss_since_wipe`.
    corruption_at_wipe: u64,
    entry_loss_at_wipe: u64,
}

impl KNode {
    /// Durable corruption (`bit_flip`, `misdirected_write`) fired on this
    /// node's own disk since its last wipe.
    fn corruption_since_wipe(&self) -> bool {
        self.host.durable_corruption_count() > self.corruption_at_wipe
    }

    /// Entry loss fired on this node's own disk since its last wipe.
    fn entry_loss_since_wipe(&self) -> bool {
        self.host.dir_entry_loss_count() > self.entry_loss_at_wipe
    }
}

/// Build the App + Repl + Kernel for one host, with the same torn-role
/// first-boot re-init the Db-level `open_node` uses. Parameterized on the
/// schema for exactly the reason `spawn_node_with_schema` is (migrations T10):
/// the witness-gate test needs a kernel mounted over a versioned table, and
/// `kernel_schema()` has none.
fn open_kernel(
    sim: &Sim,
    h: &SimHost,
    cfg: &ReplConfig,
    db_cfg: &DbConfig,
    flipped: bool,
    schema: &Schema,
) -> Result<Kernel, String> {
    let mut app = App::open(
        Box::new(h.disk()),
        schema.clone(),
        Box::new(h.entropy()),
        AppConfig {
            body_cap: 1 << 20,
            db: db_cfg.clone(),
            ..AppConfig::default()
        },
    )
    .map_err(|e| format!("App::open: {e}"))?
    .mount(routes![put_widget, get_widget])
    .mount_jobs(jobs![Bump])
    .schedule("tick", Duration::from_secs(1), || Bump { slot: 0 });

    let repl = match Repl::open(
        Box::new(h.net()),
        Box::new(h.disk()),
        cfg.clone(),
        app.db_for_tests(),
    ) {
        Ok((repl, _notice)) => repl,
        // A torn witness file (M2 sub-project 0b task 2): keyed on its own
        // full, non-overlapping needle ("witness file failed its integrity
        // check"), an independent arm from the role-file arm below (keyed on
        // ITS own full needle, "role file failed its integrity check") — the
        // two are mutually exclusive by construction, so arm ORDER is
        // irrelevant; both need the FULL string rather than the shared
        // "integrity check" substring precisely because that substring alone
        // cannot tell them apart. Unlike the role file, there is no
        // config-derived witness set to re-init from, and witnesses.rs's own
        // fix text is explicit that deleting it is safe ONLY when the
        // operator is certain no follower of this node was ever promoted —
        // a fact this harness cannot prove in a swarm-fuzzed universe (a
        // witness can be recorded by an ordinary running primary with no
        // role flip at all, so `flipped` is not a safe proxy here either,
        // where it exists). Always inconclusive: the operator-restore path
        // is the fix. Counted separately from the generic inconclusive
        // bucket ("repl:witness-file-inconclusive") so this residual,
        // newly-introduced permanent-unavailability path stays measurable,
        // not invisible (fix round 1, Noah's ruling — a genuinely torn
        // NON-empty witness file is a real design fork, escalated
        // separately, not fixed here). (This comment is kept byte-identical
        // at every copy of this arm — replication.rs's two sites, auth.rs,
        // realtime.rs — so a future reviewer can diff them; see
        // task-2-report.md.)
        Err(e)
            if e.to_string()
                .contains("witness file failed its integrity check") =>
        {
            sim.cover("repl:witness-file-inconclusive");
            return Err(format!(
                "{INCONCLUSIVE}: {} torn witness file — the operator restore path is the fix: {e}",
                h.name()
            ));
        }
        Err(e)
            if e.to_string()
                .contains("role file failed its integrity check") =>
        {
            if flipped {
                // Same cause key as `open_node`'s equivalent arm: the failure
                // shape is identical, and this family had no cause key at all
                // until oracle-density task 3 (task-2-report.md named the gap).
                sim.cover("repl:inconclusive:role-file-torn");
                return Err(format!(
                    "{INCONCLUSIVE}: {} torn role file after a role flip — the operator restore \
                     path (not config re-init) is the fix: {e}",
                    h.name()
                ));
            }
            let _ = h.disk().remove("role");
            let _ = h.disk().sync_dir("role");
            Repl::open(
                Box::new(h.net()),
                Box::new(h.disk()),
                cfg.clone(),
                app.db_for_tests(),
            )
            .map_err(|e| format!("Repl::open (after torn-role re-init): {e}"))?
            .0
        }
        Err(e) => return Err(format!("Repl::open: {e}")),
    };
    Kernel::with_replication(app, Box::new(sim.clock()), repl).map_err(|e| format!("Kernel: {e}"))
}

fn spawn_kernel_node(
    sim: &Sim,
    host: &str,
    cfg: ReplConfig,
    db_cfg: DbConfig,
    horizon: u64,
    recoverable: bool,
) -> Result<KNode, String> {
    spawn_kernel_node_with_schema(
        sim,
        host,
        cfg,
        db_cfg,
        horizon,
        recoverable,
        kernel_schema(),
    )
}

fn spawn_kernel_node_with_schema(
    sim: &Sim,
    host: &str,
    cfg: ReplConfig,
    db_cfg: DbConfig,
    horizon: u64,
    recoverable: bool,
    schema: Schema,
) -> Result<KNode, String> {
    let h = sim.host(host);
    for _ in 0..RECOVER_ATTEMPTS {
        match open_kernel(sim, &h, &cfg, &db_cfg, false, &schema) {
            Ok(kernel) => {
                let source = SimSource::new(Box::new(sim.clock()), horizon);
                let responses = source.responses();
                return Ok(KNode {
                    host: h,
                    kernel: Some(kernel),
                    source,
                    responses,
                    name: host.to_string(),
                    repl_cfg: cfg,
                    db_cfg,
                    schema,
                    recoveries: 0,
                    flipped: false,
                    recoverable,
                    corrupted_by_rig: false,
                    corruption_at_wipe: 0,
                    entry_loss_at_wipe: 0,
                });
            }
            Err(e) if e.contains(INCONCLUSIVE) => return Err(e),
            // A refusal for durable corruption at or below the horizon is
            // fallout only where that corruption fired on this host's own
            // disk (S3b item 0's rule), and then no power-cycle clears the
            // bytes, so the spawn gives up at once. Unexplained, it falls to
            // the failure arm below. Item 0 missed this loop: it went from
            // retrying 500 power-cycles into `spawn-exhausted` to failing,
            // 187 universes over 192 fresh bases (S3b item 8's sweep).
            Err(e)
                if matches!(classify(&e), FaultReason::DurableHorizon)
                    && h.durable_corruption_fired() =>
            {
                sim.cover("repl:inconclusive:spawn-durable-refusal");
                return Err(format!(
                    "{INCONCLUSIVE}: {host} kernel spawn refused for durable corruption that \
                     fired on {host}'s disk, which no power-cycle clears: {e}"
                ));
            }
            // Power-cycle on EVERY attempt here, deliberately — the opposite of
            // `recover_node`/`reopen_p_frames`'s gated shape, and MEASURED
            // rather than assumed (oracle-density task 3, fix round 1).
            //
            // Both halves of the trade run the other way for a spawn. The
            // gate's PAYOFF is preserving a recovering node's accumulated
            // WAL-replay progress across retries, and a fresh create has none to
            // preserve. Its COST is that a sim disk which has crashed
            // (`crash_disk_after` fired) answers every operation with
            // `crashed_error` — "simulated crash: the disk is gone until
            // Sim::restart()" — and `classify` reads "simulated" as injected
            // fallout, so this arm retries a latched-dead disk forever; only
            // `SimHost::restart` clears `disk.crashed`. Under the gate 7 of
            // every 8 attempts are spent against that latch. (What a restart
            // clears, precisely: the crashed latch and the pending crash
            // countdown, plus every unsynced write, collapsed per the crash
            // axes. Synced bytes survive it — a restart is not a wipe.)
            //
            // Porting the gate here (128 universes, base 0x2EB1_000E) moved
            // `repl:inconclusive:spawn-exhausted` 5 → 19, roughly 4 SE, while
            // leaving the property's give-up total at exactly 90 and its floored
            // verdict key at exactly 32: the same universes die, just earlier
            // and having exercised less. Task 1's `settle_boot` measurement
            // (0 successes in 20_000 boots) is about recovery boots and does not
            // transfer to this loop. Do not "fix" this site to match its
            // siblings without re-running that A/B.
            Err(e) if is_fault_fallout(&e) => {
                h.restart();
                continue;
            }
            Err(e) => return Err(format!("{host}: {e}")),
        }
    }
    sim.cover("repl:inconclusive:spawn-exhausted");
    Err(format!(
        "{INCONCLUSIVE}: {host} kernel spawn exhausted {RECOVER_ATTEMPTS} attempts"
    ))
}

/// A node whose local copy is unrecoverable (corruption below the horizon):
/// the named hard error whose fix is restore-from-replica. The above-horizon
/// property turns this into the real restore path.
const RESTORE_NEEDED: &str = "universe-restore-needed";

fn recover_kernel_node(sim: &Sim, node: &mut KNode) -> Result<(), String> {
    sim.cover("restart");
    node.recoveries += 1;
    node.kernel = None; // release old handles before power-cycling
    let mut last = String::new();
    for _ in 0..RECOVER_ATTEMPTS {
        node.host.restart();
        match open_kernel(
            sim,
            &node.host,
            &node.repl_cfg,
            &node.db_cfg,
            node.flipped,
            &node.schema,
        ) {
            Ok(kernel) => {
                node.kernel = Some(kernel);
                return Ok(());
            }
            Err(e) if e.contains(INCONCLUSIVE) => return Err(e),
            // Corruption at or below the durable horizon: not a torn tail — the
            // local copy is bricked and the fix is restore. Surface it (the
            // caller reseeds from the survivor) rather than retry into the same
            // durable bytes forever. DESIGN §4's compacted-log refusal names the
            // same fix but is not this: it goes on to the fallout arm, which an
            // injected fault inside `Store::open` reaches as itself (it no longer
            // becomes the refusal), or fails — under redb 4.3.0 the crash model
            // cannot brick the store.
            Err(e)
                if e.contains("restore this node from a replica or backup")
                    && !e.contains("cannot be rebuilt from the log alone") =>
            {
                return Err(format!(
                    "{RESTORE_NEEDED}: {} corrupted below the horizon",
                    node.name
                ));
            }
            // A vanished base segment (dir_entry_loss lost the entry naming a
            // low segment, leaving a gap Db::open refuses to guess across): the
            // local log directory is damaged unrecoverably — same restore path
            // (workload-model finding, seed 0x6b3de0f93ddc2a36). Gated on the
            // axis having ACTUALLY fired (fire-time-marked, `durable_corruption_
            // fired`'s discipline): with the recovery-cut ordering fixed
            // (log.rs, seed 0x4177819b4a2ba984) the platform never itself
            // leaves a durable gap, so this restore excuse is legitimate only
            // when entry loss could have caused the gap. A gap in a universe
            // where the axis never fired is a platform regression — it falls
            // through to the loud bug arm below, not this mask. Asked of this
            // node's own disk since its last wipe (S3b item 8): entry loss on
            // another node, or before a wipe that deleted the gapped
            // directory, cannot have caused this gap.
            Err(e) if e.contains("refusing to guess") && node.entry_loss_since_wipe() => {
                return Err(format!(
                    "{RESTORE_NEEDED}: {} log directory gapped by entry loss",
                    node.name
                ));
            }
            Err(e) if is_fault_fallout(&e) => last = e,
            Err(e) => return Err(format!("recover {}: {e}", node.name)),
        }
    }
    // The kernel family's copy of `recover_node`'s cause: 500 attempts were not
    // enough, as distinct from a disk that cannot be recovered from at all.
    // Uninstrumented until oracle-density task 3, which is why the
    // wal-corruption property's give-ups had zero cause attribution. See
    // `recover_exhausted_key` for why "not enough" is NOT the same claim as
    // "the budget is too small".
    sim.cover("repl:inconclusive:recover-exhausted");
    // The kernel family's copy of the budget-or-wedge sub-key; see
    // `recover_node`'s site for what the distinction buys.
    sim.cover(recover_exhausted_key(&last));
    Err(format!(
        "{INCONCLUSIVE}: recover {} exhausted {RECOVER_ATTEMPTS} attempts (last: {last})",
        node.name
    ))
}

/// One `Kernel::step`, classifying faults. A recoverable node reopens on
/// fallout (progress); a designated survivor's fault is inconclusive.
fn kstep_node(sim: &Sim, node: &mut KNode) -> Result<Step, String> {
    let stepped = {
        let kernel = node.kernel.as_mut().expect("kstep on a live node");
        kernel.step(&mut node.source)
    };
    match stepped {
        Ok(step) => Ok(step),
        // A recoverable node reopens on transient fallout OR on its own local
        // media corruption surfaced mid-step (a misdirected write to its wal
        // that the shipping read-back refuses): a restart re-runs recovery,
        // which cuts an above-horizon torn tail or routes a below-horizon
        // corruption to the restore path (RESTORE_NEEDED). Workload-model
        // finding, seed 0xd352f1bf5ef94cc6: without this arm the primary's own
        // shipped-frame refusal read as a bug instead of the restart it names.
        Err(e)
            if node.recoverable
                && (is_fault_fallout(&e.to_string()) || is_local_corruption(&e.to_string())) =>
        {
            recover_kernel_node(sim, node)?;
            Ok(Step::Worked)
        }
        // A designated survivor that faults or hits its OWN media corruption
        // can no longer serve as the clean reference: the single-follower
        // promise assumes one undamaged copy, so this universe is inconclusive
        // — not a bug (workload-model finding, seed 0xa90fe2035d7d4bf2: the
        // survivor's wal took the misdirected write, and its shipping refusal
        // read as a failure of the promise it can no longer underwrite). A
        // genuine data/consistency bug never surfaces here (those panic, or
        // the final oracles on the survivor's store catch them).
        //
        // A survivor refused as diverged behind did not fault. It fell below
        // the primary's retained base, either because it lagged past that
        // window or because the primary forked below it, and the refusal
        // cannot tell which (Design B). For the survivor, a re-seed would void
        // the promise it underwrites, so this too gives up — under its own key,
        // because a give-up is named by what happened.
        Err(e) if !node.recoverable => {
            let e = e.to_string();
            if is_diverged_behind(&e) {
                sim.cover("repl:inconclusive:survivor-refused-behind");
                Err(format!(
                    "{INCONCLUSIVE}: {} (the designated survivor) was refused as diverged \
                     behind: {e}",
                    node.name
                ))
            } else {
                sim.cover("repl:inconclusive:survivor-faulted");
                Err(format!(
                    "{INCONCLUSIVE}: {} (the designated survivor) faulted: {e}",
                    node.name
                ))
            }
        }
        Err(e) => Err(format!("{} kernel step: {e}", node.name)),
    }
}

/// A node's own durable copy is damaged (media corruption on the shipping
/// read-back, or a base segment gapped by entry loss) — the named hard errors
/// whose fix is restore, not a local repair.
fn is_local_corruption(e: &str) -> bool {
    e.contains("restore this node from a replica or backup")
        || e.contains("corrupted frame must never be shipped")
        || e.contains("refusing to guess")
}

/// One scheduler round over kernel nodes: step each once; if none worked,
/// advance virtual time to the earliest deadline (node park tick, node timer,
/// or in-flight network delivery). A repl-configured kernel always reports a
/// park-tick deadline, so this effectively never reports full quiescence.
fn kernel_round(sim: &Sim, nodes: &mut [&mut KNode]) -> Result<bool, String> {
    let mut worked = false;
    let mut deadlines: Vec<u64> = Vec::new();
    for node in nodes.iter_mut() {
        match kstep_node(sim, node)? {
            Step::Worked => worked = true,
            Step::Idle { deadline } => {
                if let Some(d) = deadline {
                    deadlines.push(d.as_nanos());
                }
            }
        }
    }
    if worked {
        return Ok(true);
    }
    let now = sim.now().as_nanos();
    let mut next = sim.next_wakeup();
    for d in deadlines {
        if d > now {
            next = Some(next.map_or(d, |x| x.min(d)));
        }
    }
    match next {
        Some(t) if t > now => {
            sim.clock().sleep(Duration::from_nanos(t - now));
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn krun_until(
    sim: &Sim,
    nodes: &mut [&mut KNode],
    done: impl Fn(&[&mut KNode]) -> bool,
    budget: u32,
) -> Result<(), String> {
    for _ in 0..budget {
        if done(nodes) {
            return Ok(());
        }
        if !kernel_round(sim, nodes)? {
            return if done(nodes) {
                Ok(())
            } else {
                Err("kernel stall: no work, no pending events, done() false".to_string())
            };
        }
    }
    if done(nodes) {
        Ok(())
    } else {
        Err(format!("kernel budget {budget} exhausted before done()"))
    }
}

/// Like `krun_until`, but returns the raw `io::Error` instead of
/// `kstep_node`'s stringified wrapper — the one assertion that needs to
/// inspect the error itself (`Refusal::of`), not just its text (mirrors
/// `run_until_io_error`'s own reason for existing, one scheduler family
/// over). No fault handling, and no recoverable-node/survivor
/// classification (only meant for a `FaultConfig::none()` scenario, same
/// restriction as `run_until_io_error`): the first node whose `Kernel::step`
/// errors ends the loop.
fn krun_until_io_error(
    sim: &Sim,
    nodes: &mut [&mut KNode],
    done: impl Fn(&[&mut KNode]) -> bool,
    budget: u32,
) -> io::Result<()> {
    for _ in 0..budget {
        if done(nodes) {
            return Ok(());
        }
        let mut worked = false;
        let mut deadlines: Vec<u64> = Vec::new();
        for node in nodes.iter_mut() {
            let kernel = node.kernel.as_mut().expect("kstep on a live node");
            match kernel.step(&mut node.source)? {
                Step::Worked => worked = true,
                Step::Idle { deadline } => {
                    if let Some(d) = deadline {
                        deadlines.push(d.as_nanos());
                    }
                }
            }
        }
        if worked {
            continue;
        }
        let now = sim.now().as_nanos();
        let mut next = sim.next_wakeup();
        for d in deadlines {
            if d > now {
                next = Some(next.map_or(d, |x| x.min(d)));
            }
        }
        match next {
            Some(t) if t > now => sim.clock().sleep(Duration::from_nanos(t - now)),
            _ => return Ok(()),
        }
    }
    Ok(())
}

fn klast(node: &KNode) -> Option<u64> {
    node.kernel
        .as_ref()
        .and_then(|k| k.app().db().last_applied())
}

fn kconverged(nodes: &[&mut KNode]) -> bool {
    klast(nodes[0]) == klast(nodes[1])
}

/// Push a request onto `target`'s source and drive the whole node set until it
/// is answered; returns the recorded `(status, body)`.
fn drive_request(
    sim: &Sim,
    nodes: &mut [&mut KNode],
    target: usize,
    req: Request,
    budget: u32,
) -> Result<(u16, Vec<u8>), String> {
    drive_request_answered(sim, nodes, target, req, budget)?.ok_or_else(|| {
        sim.cover("repl:inconclusive:request-no-response");
        format!("{INCONCLUSIVE}: no response to a request on node {target} within budget")
    })
}

/// `drive_request` without its verdict on silence: `Ok(None)` when the
/// request was not answered within `budget`. That leaves the meaning of no
/// answer to the caller, and `sacrificial_cut` knows it.
fn drive_request_answered(
    sim: &Sim,
    nodes: &mut [&mut KNode],
    target: usize,
    req: Request,
    budget: u32,
) -> Result<Option<(u16, Vec<u8>)>, String> {
    let sink = nodes[target].responses.clone();
    let before = sink.lock().expect("responses lock").len();
    let now = sim.now().as_nanos();
    nodes[target].source.push_at(now, req);
    for _ in 0..budget {
        if sink.lock().expect("responses lock").len() > before {
            return Ok(Some(sink.lock().expect("responses lock")[before].clone()));
        }
        if !kernel_round(sim, nodes)? {
            break;
        }
    }
    Ok(sink.lock().expect("responses lock").get(before).cloned())
}

/// Drive the given kernel nodes until a control conn to one of them answers.
fn drive_ctrl_kernel(
    sim: &Sim,
    nodes: &mut [&mut KNode],
    cli: &mut CtrlConn,
    budget: u32,
) -> Result<Msg, String> {
    for _ in 0..budget {
        let advanced = kernel_round(sim, nodes)?;
        match cli.poll() {
            Ok(Some(m)) => return Ok(m),
            Ok(None) => {}
            Err(e) => {
                sim.cover("repl:inconclusive:ctrl-conn-lost");
                return Err(format!("{INCONCLUSIVE}: {e}"));
            }
        }
        if !advanced {
            sim.cover("repl:inconclusive:ctrl-quiescent");
            return Err(format!("{INCONCLUSIVE}: quiescent before a control reply"));
        }
    }
    sim.cover("repl:inconclusive:ctrl-no-reply");
    Err(format!("{INCONCLUSIVE}: no control reply within budget"))
}

// ------------------------------------ property: follower gating (deterministic)

#[test]
fn follower_kernel_serves_reads_never_jobs_or_cron() {
    let sim = Sim::with_faults(0x2EB1_0100, FaultConfig::none());
    let horizon = 100 * 1_000_000_000; // 100 virtual seconds — room for cron
    let mut p = spawn_kernel_node(
        &sim,
        "p",
        primary_cfg("p:1"),
        DbConfig::default(),
        horizon,
        true,
    )
    .unwrap();
    let mut f = spawn_kernel_node(
        &sim,
        "f",
        follower_cfg("f:1", "p:1"),
        DbConfig::default(),
        horizon,
        true,
    )
    .unwrap();

    // Writes on the primary: each commits a widget AND enqueues a job.
    for id in 0..6u64 {
        let (status, _) = drive_request(
            &sim,
            &mut [&mut p, &mut f],
            0,
            Request::new(Method::Post, format!("/widget/{id}")),
            CONVERGE_BUDGET,
        )
        .unwrap();
        assert_eq!(status, 200, "the primary commits a POST");
    }
    // Advance until the primary's cron has ticked twice and f has converged.
    krun_until(
        &sim,
        &mut [&mut p, &mut f],
        |ns| ns[0].kernel.as_ref().unwrap().stats().cron_ticks >= 2 && kconverged(ns),
        CONVERGE_BUDGET,
    )
    .unwrap();

    let pstats = p.kernel.as_ref().unwrap().stats();
    let fstats = f.kernel.as_ref().unwrap().stats();
    assert!(pstats.jobs_done > 0, "the primary ran jobs");
    assert!(pstats.cron_ticks >= 2, "the primary ticked cron");
    assert_eq!(fstats.jobs_done, 0, "a follower never runs jobs");
    assert_eq!(fstats.cron_ticks, 0, "a follower never ticks cron");

    // The Done marks are replicated: f's jobs table byte-mirrors p's.
    {
        let ps = p.kernel.as_ref().unwrap().app().db().snapshot().unwrap();
        let fs = f.kernel.as_ref().unwrap().app().db().snapshot().unwrap();
        let pj = ps.scan_raw(JobRow::TABLE, false).unwrap();
        let fj = fs.scan_raw(JobRow::TABLE, false).unwrap();
        assert!(!pj.is_empty(), "there were jobs to replicate");
        assert_eq!(pj, fj, "f's jobs table mirrors p's (Done marks replicated)");
    }

    // A read on the follower answers 200 with the primary's data.
    let (status, body) = drive_request(
        &sim,
        &mut [&mut p, &mut f],
        1,
        Request::new(Method::Get, "/widget/3"),
        CONVERGE_BUDGET,
    )
    .unwrap();
    assert_eq!(status, 200, "the follower serves reads");
    assert_eq!(
        // get_widget returns WireI64 (task 5): a 64-bit integer is ALWAYS
        // string-encoded on the wire, so the body is the JSON string `"3"`,
        // not the bare number `3`.
        body,
        b"\"3\"",
        "the read returns the primary's replicated value"
    );

    // A write on the follower is refused 421 naming the primary.
    let (status, body) = drive_request(
        &sim,
        &mut [&mut p, &mut f],
        1,
        Request::new(Method::Post, "/widget/99"),
        CONVERGE_BUDGET,
    )
    .unwrap();
    assert_eq!(status, 421, "the follower refuses writes");
    // Byte-exact (error-suite T8): decode the `{"error": ...}` envelope (the
    // auth-401 discipline) rather than a raw-bytes `contains`.
    let decoded: serde_json::Value = serde_json::from_slice(&body).expect("421 body is JSON");
    assert_eq!(
        decoded["error"],
        "this node is a read-only follower — writes go to the primary at p:1; retry there \
         (the request was not committed)"
    );
}

// -------------------------------- property: backup opens + promotes (spec §4 #7)

#[test]
fn backup_node_dir_opens_and_promotes_fingerprint_identical() {
    let sim = Sim::with_faults(0x2EB1_0102, FaultConfig::none());
    let horizon = 100 * 1_000_000_000;
    let mut p = spawn_kernel_node(
        &sim,
        "p",
        primary_cfg("p:1"),
        DbConfig::default(),
        horizon,
        true,
    )
    .unwrap();
    // A Backup-role node: its SimSource stays empty (requests never arrive),
    // the in-sim shape of `serve::backup`'s `EmptySource`.
    let mut b = spawn_kernel_node(
        &sim,
        "b",
        backup_cfg("b:1", "p:1"),
        DbConfig::default(),
        horizon,
        true,
    )
    .unwrap();

    for id in 0..8u64 {
        let (status, _) = drive_request(
            &sim,
            &mut [&mut p, &mut b],
            0,
            Request::new(Method::Post, format!("/widget/{id}")),
            CONVERGE_BUDGET,
        )
        .unwrap();
        assert_eq!(status, 200);
    }
    krun_until(&sim, &mut [&mut p, &mut b], kconverged, CONVERGE_BUDGET).unwrap();

    // The backup answered nothing and ran nothing — it only followed.
    let bstats = b.kernel.as_ref().unwrap().stats();
    assert_eq!(bstats.requests, 0, "a backup answers no requests");
    assert_eq!(bstats.jobs_done, 0, "a backup runs no jobs");
    assert_eq!(bstats.cron_ticks, 0, "a backup ticks no cron");

    let seq = p.kernel.as_ref().unwrap().app().db().last_applied();
    let p_scan = table_scans(p.kernel.as_ref().unwrap().app().db()).unwrap();

    // Graceful close: dropping the kernel drops the App/Db (redb persists, WAL
    // synced). The dir then opens clean through a raw Db::open — the promise
    // the compacted-log restore text makes.
    let bkernel = b.kernel.take().unwrap();
    drop(bkernel);
    {
        let db = Db::open(Box::new(b.host.disk()), DbConfig::default(), full_schema())
            .expect("a gracefully-closed backup dir opens clean");
        assert_eq!(
            db.last_applied(),
            seq,
            "the reopened dir holds the converged position"
        );
    }

    // Restart b (safe post-graceful-close — nothing unsynced to collapse) and
    // promote it: it accepts writes from its acked position.
    b.flipped = true;
    b.host.restart();
    b.kernel = Some(open_kernel(&sim, &b.host, &b.repl_cfg, &b.db_cfg, false, &b.schema).unwrap());

    let mut cli = CtrlConn::open(&sim, "b:1", &Msg::CtrlPromote { secret: None }).unwrap();
    let reply = drive_ctrl_kernel(&sim, &mut [&mut b], &mut cli, CONVERGE_BUDGET).unwrap();
    match reply {
        Msg::CtrlOk { message } => assert!(
            message.contains("last_applied"),
            "promote CtrlOk names last_applied: {message}"
        ),
        other => panic!("a backup promote must succeed: {other:?}"),
    }
    assert!(
        b.kernel.as_ref().unwrap().is_primary(),
        "b is a primary after promotion"
    );
    // The promote itself is at the converged position; becoming primary then
    // re-upserts the (already-replicated, identical) cron row, one redundant
    // record past `seq`, so the CONTENT is what must equal p's — the typed
    // scans byte-match at that state.
    let b_scan = table_scans(b.kernel.as_ref().unwrap().app().db()).unwrap();
    assert_eq!(
        b_scan, p_scan,
        "the promoted backup's typed scans equal the primary's at that seq"
    );

    // And it now accepts writes.
    let (status, _) = drive_request(
        &sim,
        &mut [&mut b],
        0,
        Request::new(Method::Post, "/widget/100"),
        CONVERGE_BUDGET,
    )
    .unwrap();
    assert_eq!(status, 200, "a promoted backup accepts writes");
}

// --------------------- property: the above-horizon promise kept (spec §4 #8)

use charpente_core::job::JobStatus;

const SEC_NANOS: u64 = 1_000_000_000;
const KBUDGET: u32 = 200_000;

/// Checkpointing config so a store horizon forms below the WAL tail — the
/// precondition for below-horizon corruption to be distinguishable from a
/// torn tail.
fn horizon_db_cfg() -> DbConfig {
    DbConfig {
        wal: WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 256,
        },
        store_path: "tables.redb".to_string(),
        retain_segments: 2,
        checkpoint_every: 8,
        event_retention: 65_536,
        migration_batch_size: 1000,
    }
}

/// Which acked widget ids the node's store currently holds (it never loses
/// these once present, being the designated survivor).
fn ids_present(node: &KNode, ids: &[u64]) -> Result<Vec<u64>, String> {
    let db = node.kernel.as_ref().ok_or("node has no kernel")?.app().db();
    let snap = db.snapshot().map_err(|e| e.to_string())?;
    let rows = snap
        .scan_raw(Widget::TABLE, false)
        .map_err(|e| e.to_string())?;
    Ok(ids
        .iter()
        .copied()
        .filter(|id| rows.iter().any(|(_, rid, _)| rid == id))
        .collect())
}

/// THE promise (spec §4 property 8): every acked id the follower had converged
/// on still lives on it. No `durable_corruption_fired` excuse — a converged
/// follower exists, so this is a hard oracle.
fn assert_survivor_holds(sim: &Sim, node: &mut KNode, ids: &[u64]) -> Result<(), String> {
    retry_read_reopen(sim, node, recover_kernel_node, |node| {
        let db = node
            .kernel
            .as_ref()
            .ok_or("survivor has no kernel")?
            .app()
            .db();
        let snap = db.snapshot().map_err(|e| e.to_string())?;
        let rows = snap
            .scan_raw(Widget::TABLE, false)
            .map_err(|e| e.to_string())?;
        for id in ids {
            if !rows.iter().any(|(_, rid, _)| rid == id) {
                return Err(format!(
                    "acked+converged widget {id} vanished from the survivor — the above-horizon \
                     promise broke: media corruption on the primary lost a record the follower \
                     had (spec §4 property 8)"
                ));
            }
        }
        Ok(())
    })
}

/// The consistency oracle carried over from the kernel suite: every replicated
/// Done Bump bumped its Tally exactly once (a cut reverts to a prefix, which
/// stays consistent, so this holds through corruption).
fn bump_exactly_once(sim: &Sim, node: &mut KNode) -> Result<(), String> {
    retry_read_reopen(sim, node, recover_kernel_node, |node| {
        let db = node.kernel.as_ref().ok_or("no kernel")?.app().db();
        let snap = db.snapshot().map_err(|e| e.to_string())?;
        let jobs = snap.scan::<JobRow>().map_err(|e| e.to_string())?;
        let tallies: std::collections::BTreeMap<u64, u64> = snap
            .scan::<Tally>()
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|t| (t.id.raw(), t.count))
            .collect();
        let mut done: std::collections::BTreeMap<u64, u64> = Default::default();
        for j in &jobs {
            if j.status == JobStatus::Done && j.name == "Bump" {
                let slot = u64::decode(&mut &j.payload[..]).map_err(|e| e.to_string())?;
                *done.entry(slot).or_insert(0) += 1;
            }
        }
        for (&slot, &d) in &done {
            let c = tallies.get(&slot).copied().unwrap_or(0);
            if c != d {
                return Err(format!(
                    "exactly-once violated on the survivor at slot {slot}: {d} Done bumps but \
                     {c} tally"
                ));
            }
        }
        Ok(())
    })
}

/// The real restore path (property (a)): the survivor is promoted, then the
/// bricked node is wiped and rebuilt as a fresh follower of it — re-seeded from
/// the survivor's snapshot. This is the compacted-log error text's promise made
/// real (the Db-level suite could only mark it inconclusive).
fn restore_p_from_survivor(sim: &Sim, p: &mut KNode, f: &mut KNode) -> Result<(), String> {
    sim.cover("repl:restored-from-follower");
    // Promote the survivor if it is not already primary.
    if !f.kernel.as_ref().is_some_and(|k| k.is_primary()) {
        f.flipped = true;
        let mut cli = CtrlConn::open(sim, "f:1", &Msg::CtrlPromote { secret: None })?;
        match drive_ctrl_kernel(sim, &mut [f], &mut cli, KBUDGET)? {
            Msg::CtrlOk { .. } => {}
            other => {
                sim.cover("repl:inconclusive:restore-promote-refused");
                return Err(format!(
                    "{INCONCLUSIVE}: promoting the survivor failed: {other:?}"
                ));
            }
        }
    }
    reseed_p(sim, p, f)
}

/// Wipe `p`'s data dir and rebuild it as a fresh follower of `f`, which must
/// already be primary: an empty (None-position) follower snapshot-seeds even
/// below the primary's GC'd base. The restore path's second half, and the
/// remedy a laggard refusal names ("re-seed it deliberately from a fresh data
/// dir"), which is why `route_p_error` answers that refusal with it.
///
/// Every error is routed (D3 round 4, review I2), and `above_horizon_universe_routes_every_error_of_p`
/// holds it:
/// - **The wipe** meets injected faults like any boot. A failed step
///   power-cycles `p` and wipes again (`repl:reseed-wipe-retried`), and gives
///   up by name once the faults persist (`repl:inconclusive:reseed-wipe-faulted`).
///   Anything else fails.
///   - Until round 4 the wipe dropped its errors. A failed directory barrier
///     then let the deleted files come back at the next reboot, and `p`
///     reopened holding its own old timeline (seed `0xa38c1195b6343381`).
/// - **`p` reopening non-empty** after a whole, synced wipe fails by name (D3
///   round 5): no injected fault explains it. The wipe's own faults are
///   handled above.
/// - **The convergence** goes through `route_reseed_convergence`.
fn reseed_p(sim: &Sim, p: &mut KNode, f: &mut KNode) -> Result<(), String> {
    p.kernel = None;
    let mut attempts = 1;
    let wiped = loop {
        p.host.restart();
        match wipe_data_dir(&p.host) {
            Ok(()) => break Ok(()),
            Err(e) if is_fault_fallout(&e) && attempts < RECOVER_ATTEMPTS => {
                attempts += 1;
                sim.cover("repl:reseed-wipe-retried");
            }
            Err(e) if is_fault_fallout(&e) => {
                sim.cover("repl:inconclusive:reseed-wipe-faulted");
                break Err(format!(
                    "{INCONCLUSIVE}: re-seeding p: its data dir could not be wiped in \
                     {RECOVER_ATTEMPTS} power cycles under injected faults: {e}"
                ));
            }
            Err(e) => break Err(format!("re-seeding p: wiping its data dir failed: {e}")),
        }
    };
    wiped?;
    // The wipe deleted every byte the rig ever corrupted.
    p.corrupted_by_rig = false;
    // And every byte an earlier sim fault touched: only a fault since then
    // explains a refusal of the re-seeded p.
    p.corruption_at_wipe = p.host.durable_corruption_count();
    p.entry_loss_at_wipe = p.host.dir_entry_loss_count();
    p.repl_cfg = follower_cfg("p:1", "f:1");
    p.flipped = true;
    p.recoveries += 1;
    // The fresh boot meets injected faults like any other: restart the host and
    // boot again, as a supervised process would (`checks::boot_through_fallout`'s
    // reason, on a host of its own), before calling the reopen inconclusive.
    let mut opened = open_kernel(sim, &p.host, &p.repl_cfg, &p.db_cfg, true, &p.schema);
    for _ in 1..RECOVER_ATTEMPTS {
        match &opened {
            Err(e) if is_fault_fallout(e) && !e.contains(INCONCLUSIVE) => {
                sim.cover("boot:retried");
                p.host.restart();
                opened = open_kernel(sim, &p.host, &p.repl_cfg, &p.db_cfg, true, &p.schema);
            }
            _ => break,
        }
    }
    p.kernel = Some(opened.map_err(|e| {
        if e.contains(INCONCLUSIVE) {
            // Already covered at its own origin site — do not double-count.
            e
        } else {
            sim.cover("repl:inconclusive:restore-reopen");
            format!("{INCONCLUSIVE}: restore reopen: {e}")
        }
    })?);
    if klast(p).is_some() {
        return Err(format!(
            "re-seeding p: it reopened holding records up to seq {:?} after a whole, synced \
             wipe of its data dir. No injected fault explains that (the wipe's own faults \
             retry or give up above), so it is a platform durability bug (DESIGN §2)",
            klast(p)
        ));
    }
    // Drive the re-seed to convergence so the restore is real, not just wired.
    let converged = krun_until(sim, &mut [&mut *p, &mut *f], kconverged, KBUDGET);
    route_reseed_convergence(sim, p, converged)
}

/// Every file in `host`'s data dir removed, and both directory barriers
/// synced so the removals are durable. Each step's error comes back whole.
fn wipe_data_dir(host: &SimHost) -> Result<(), String> {
    let disk = host.disk();
    for name in disk.list("").map_err(|e| e.to_string())? {
        disk.remove(&name).map_err(|e| e.to_string())?;
    }
    disk.sync_dir("").map_err(|e| e.to_string())?;
    disk.sync_dir("wal/").map_err(|e| e.to_string())?;
    Ok(())
}

/// The re-seed's convergence, routed by kind (D3 round 4, review I2):
/// - an error that already gave up passes through (it is counted at its
///   origin);
/// - `p` refused as diverged behind again gives up as
///   `repl:inconclusive:reseed-refused-behind`;
/// - RESTORE_NEEDED gives up as `repl:inconclusive:reseed-restore-needed`
///   when an injected fault on `p`'s own disk explains it, and fails by name
///   otherwise (`unexplained_restore_needed`, D3 round 5);
/// - anything else fails by name.
///
/// Until round 4 every error here was `repl:inconclusive:restore-stalled`.
/// Over the review's 24,576 universes all 9 were one of the two refusals
/// above, but a stall, an exhausted budget or an unexpected recovery error
/// would have given up there instead of failing.
fn route_reseed_convergence(sim: &Sim, p: &KNode, r: Result<(), String>) -> Result<(), String> {
    r.map_err(|e| {
        if e.contains(INCONCLUSIVE) {
            e
        } else if is_diverged_behind(&e) {
            sim.cover("repl:inconclusive:reseed-refused-behind");
            format!("{INCONCLUSIVE}: the re-seeded p was refused before it converged: {e}")
        } else if e.contains(RESTORE_NEEDED) {
            match unexplained_restore_needed(p, &e) {
                Some(failure) => failure,
                None => {
                    sim.cover("repl:inconclusive:reseed-restore-needed");
                    format!(
                        "{INCONCLUSIVE}: the re-seeded p needed restoring before it converged: \
                         {e}"
                    )
                }
            }
        } else {
            format!("the re-seed of p from f did not converge: {e}")
        }
    })
}

/// The sacrificial write that drives the round's forced cut: one request to
/// `p` alone, so `f` is neither shipped it nor pulls it. Returns whether it was
/// acked, which is whether there is a frame for the forced cut to corrupt.
///
/// **An unanswered sacrificial write died with `p`'s process** (D3 round 2
/// diagnosis, 31 of 31 over 57 fresh bases).
/// - In every one, an injected fault on `p`'s disk made `kstep_node` reboot
///   `p` while the request was pending in its kernel. The reboot replaced
///   that kernel, so nothing was left to answer the request.
/// - The frame was never applied in any of the 31.
/// - Not the cause: `p`'s witness gate. `p` came back behind it every time,
///   but a gated primary answers a write at once, with a 503.
/// - The fault was on work other than the request's own write; a fault there
///   answers it with a 500. `a_sacrificial_write_lost_to_a_reboot_...`
///   reaches the shape through a due cron tick.
///
/// What the rig observes is no answer and a reboot of `p`. That alone does not
/// prove the frame was never applied, though it was not in any probed case
/// (31 of 31 here, and 0 of 14 in the D3 review's 96-point scan). So the round
/// skips the forced cut and claims nothing, applied or not:
/// - no `repl:above-horizon-cut` credit from this write, so the cut is claimed
///   only by `last_after < last_before_crash`, as before D3;
/// - no give-up (`repl:sacrificial-lost-to-reboot` counts the skip).
///
/// Silence *without* a reboot would leave `p`'s state unknown, so that is
/// routed like any other error of `p`. Every other error goes to
/// `route_p_error` too (before D3 a `matches!` dropped it).
fn sacrificial_cut(
    sim: &Sim,
    p: &mut KNode,
    f: &mut KNode,
    round: u64,
    budget: u32,
) -> Result<bool, String> {
    if !p.kernel.as_ref().is_some_and(|k| k.is_primary()) {
        return Ok(false);
    }
    let recoveries = p.recoveries;
    match drive_request_answered(
        sim,
        &mut [&mut *p],
        0,
        Request::new(Method::Post, format!("/widget/{}", 10_000 + round)),
        budget,
    ) {
        Ok(Some((status, _))) => Ok(status == 200),
        Ok(None) if p.recoveries != recoveries => {
            sim.cover("repl:sacrificial-lost-to-reboot");
            Ok(false)
        }
        Ok(None) => {
            sim.cover("repl:inconclusive:request-no-response");
            let silent = format!(
                "{INCONCLUSIVE}: the sacrificial write to p went unanswered without p rebooting"
            );
            route_p_error(sim, p, f, Err(silent))?;
            Ok(false)
        }
        Err(e) => {
            route_p_error(sim, p, f, Err(e))?;
            Ok(false)
        }
    }
}

/// The survivor's promise scan, routed. An `Err` used to be dropped by
/// `unwrap_or_default`, so that round promised nothing and the oracle checked
/// less while the universe could still pass. Now:
/// - an error whose origin already gave up passes through unchanged (it is
///   already counted there);
/// - a restore-needed refusal of `f`'s store (from the scan's reopen) is fault
///   fallout if and only if the fault that explains it fired on `f`'s own disk
///   (D3 rounds 3 and 5, the rule of S5 R-M3 and S3b item 0; see
///   `unexplained_restore_needed`). If it did, `f` gives up as
///   `repl:inconclusive:survivor-faulted`. If not, it fails by name: nothing
///   injected explains it, so it is a platform durability bug.
/// - injected-fault fallout, by `checks::classify`, gives up by name
///   (`repl:inconclusive:survivor-scan-unreadable`);
/// - anything else is the failure it is.
fn route_promise_scan(
    sim: &Sim,
    f: &KNode,
    scan: Result<Vec<u64>, String>,
) -> Result<Vec<u64>, String> {
    scan.map_err(|e| {
        if e.contains(INCONCLUSIVE) {
            e
        } else if e.contains(RESTORE_NEEDED) {
            match unexplained_restore_needed(f, &e) {
                Some(failure) => failure,
                None => {
                    sim.cover("repl:inconclusive:survivor-faulted");
                    format!(
                        "{INCONCLUSIVE}: {e}, explained by a fault injected on {}'s disk (the \
                         designated survivor)",
                        f.name
                    )
                }
            }
        } else if is_fault_fallout(&e) {
            sim.cover("repl:inconclusive:survivor-scan-unreadable");
            format!(
                "{INCONCLUSIVE}: the survivor's promise scan stayed unreadable under injected \
                 faults: {e}"
            )
        } else {
            e
        }
    })
}

/// `None` when an injected fault on `node`'s own disk explains its
/// RESTORE_NEEDED refusal, and otherwise the failure that names it (D3 round
/// 5; the rule of S5 R-M3 and S3b item 0). `recover_kernel_node` raises the
/// refusal in two forms, and each has exactly one explanation:
/// - **"corrupted below the horizon"** is explained by durable corruption on
///   that node, from any of the three byte-level faults in the property's
///   fault model: a `bit_flip` or `misdirected_write`
///   (`KNode::corruption_since_wipe`), or the rig's forced CRC flip this
///   round (`KNode::corrupted_by_rig`). Seed `0x78f5713c6b6cd7cd` pins a
///   refusal that only the rig's flip explains: it landed on a sacrificial
///   frame the checkpoint had already covered.
/// - **"log directory gapped by entry loss"** is explained by entry loss on
///   that node: `KNode::entry_loss_since_wipe`.
///
/// Each counts only faults since the node's last re-seed wipe, which deleted
/// every byte an earlier fault touched (S3b item 8, D3 re-review m7).
///
/// The universe-wide checkers will not do: this property faults `p` on most
/// rounds, so they would excuse a refusal of one node on the strength of a
/// fault on the other.
fn unexplained_restore_needed(node: &KNode, e: &str) -> Option<String> {
    assert!(
        e.contains(&format!("{RESTORE_NEEDED}: {} ", node.name)),
        "unexplained_restore_needed: the refusal must name {} itself, since only a fault on \
         its own disk can explain it; fix the caller that routed another node's refusal \
         here: {e}",
        node.name
    );
    let (explained, fault) = if e.contains("corrupted below the horizon") {
        (
            node.corruption_since_wipe() || node.corrupted_by_rig,
            "durable corruption (bit_flip, misdirected_write, or the rig's own \
             corrupt_last_wal_frame)",
        )
    } else if e.contains("log directory gapped by entry loss") {
        (node.entry_loss_since_wipe(), "entry loss (dir_entry_loss)")
    } else {
        (false, "known fault")
    };
    (!explained).then(|| {
        format!(
            "{e}, but no {fault} fired on {}'s disk since its last re-seed wipe, if any: a \
             restore-needed refusal that nothing injected on that node explains is a \
             platform durability bug (DESIGN §2), not fallout",
            node.name
        )
    })
}

/// The one place `above_horizon_universe` answers an error from stepping `p`
/// (with or without `f`), so no error is swallowed — a pass over a swallowed
/// error checked nothing. RESTORE_NEEDED restores `p` from the survivor, but
/// only when an injected fault on `p`'s own disk explains it; otherwise it
/// fails by name (`unexplained_restore_needed`, D3 round 5).
///
/// A diverged-behind refusal of `p` is answered with the re-seed it names for
/// a true laggard, and only when `f` never recovered (`f.recoveries == 0`).
/// That condition is the only one that decides anything.
///
/// Three more facts hold whenever it fires. They are asserted, not tested,
/// because the refusal's origin implies them (D3 review m1):
/// - **`p` is the node refused.** An error of `f`'s own step has already given
///   up by the time it gets here (`kstep_node`'s survivor arm), and that is
///   excluded first.
/// - **`p` is a follower.** A primary only sends the refusal; it surfaces on
///   the follower's step.
/// - **`f` is primary.** A survivor that is not primary refuses a follower's
///   `Hello` for its role before it checks position.
///
/// **Why the re-seed is safe: the survivor's own handshake, not the rig's
/// wipe.**
/// - `p` is only ever a follower after `restore_p_from_survivor` re-seeded it
///   from `f`. That re-seed converged only once `f` accepted `p`'s `Hello`:
///   Design B's prefix check at `p`'s position, or a snapshot seed of an empty
///   `p`. The wipe before it retries until it is whole and synced, and a `p`
///   that still reopens non-empty fails by name at `reseed_p`'s freshness
///   check, before that handshake runs. This arm never sees it.
/// - Since then `p` has appended nothing but frames `f` shipped. A shipped
///   gap re-`Hello`s, which is checked again, and a follower's own recovery
///   cut only shortens its prefix.
/// - A primary that never recovered never cut its log.
/// - So `p`'s records are a prefix of `f`'s. The refusal's other remedy (this
///   primary forked below the follower) is impossible, and re-seeding `p`
///   overwrites nothing it alone holds.
///
/// `a_restored_follower_refused_as_a_laggard_is_reseeded_not_failed` pins the
/// arm, and `a_laggard_refusal_is_not_reseeded_once_the_primary_could_have_forked`
/// its gate. `route_p_error_returns_an_unrecognised_error_unchanged` pins that
/// every other error bubbles, to `finish_universe` (an inconclusive gives up)
/// or as the failure it is.
fn route_p_error(
    sim: &Sim,
    p: &mut KNode,
    f: &mut KNode,
    r: Result<(), String>,
) -> Result<(), String> {
    let Err(e) = r else {
        return Ok(());
    };
    if e.contains(RESTORE_NEEDED) {
        if let Some(failure) = unexplained_restore_needed(p, &e) {
            return Err(failure);
        }
        return restore_p_from_survivor(sim, p, f);
    }
    if !e.contains(INCONCLUSIVE) && is_diverged_behind(&e) && f.recoveries == 0 {
        assert!(
            e.starts_with(&format!("{} kernel step: ", p.name)),
            "route_p_error: a diverged-behind refusal that has not given up must be p's own \
             step's (f's gives up in kstep_node's survivor arm); fix the routing that let \
             this through: {e}"
        );
        assert!(
            p.kernel.as_ref().is_some_and(|k| !k.is_primary()),
            "route_p_error: p was refused as diverged behind, so p must be a follower (a \
             primary only sends that refusal); fix the routing that let this through: {e}"
        );
        assert!(
            f.kernel.as_ref().is_some_and(|k| k.is_primary()),
            "route_p_error: p was refused as diverged behind, so f must be primary (a \
             non-primary refuses a Hello for its role first); fix the routing that let this \
             through: {e}"
        );
        sim.cover("repl:laggard-reseeded");
        return reseed_p(sim, p, f);
    }
    Err(e)
}

/// Design B's diverged-behind refusal (DESIGN §5): a positioned follower below
/// the primary's GC'd base. Keyed on the phrase the refusal carries whoever
/// wraps it, since `session::diverged_behind` is private.
fn is_diverged_behind(e: &str) -> bool {
    e.contains("but this primary has GC'd every record below its retained base")
}

/// The above-horizon promise with a follower (spec §4 property 8): media
/// corruption on the primary never loses an acked record its follower had
/// converged on. The universe is `above_horizon_universe`.
///
/// **Its fault model on the nodes' disks**, besides the swarm's transient IO
/// faults and crashes, is three kinds of durable damage:
/// - the swarm's `bit_flip`, overridden to 0.02;
/// - the swarm's `misdirected_write`, overridden to 0.005;
/// - the rig's own forced CRC flip of `p`'s sacrificial frame each round
///   (`corrupt_last_wal_frame`, recorded on `p` as `corrupted_by_rig`).
///
/// When it is enabled, the swarm's `dir_entry_loss` is a fourth, damaging
/// directory entries rather than bytes. `unexplained_restore_needed` checks
/// every restore-needed refusal against this model, on the refused node's own
/// disk. A refusal the model does not explain fails by name.
///
/// **How the rig answers a laggard (sub-project D3).**
/// - **How the refusal arises.** After a restore, `p` is `f`'s follower, and
///   the rig still crashes it every round. Each crash reaps `p`'s session and
///   releases its retention pin, and `f` checkpoints and GCs over a window of
///   two 256-byte segments. So `p` can rejoin below `f`'s retained base, either
///   by lagging or after its own recovery cut, and Design B refuses it as
///   diverged behind.
/// - **The answer.** That refusal is correct, and it names its remedy for a true
///   laggard. `route_p_error` applies that remedy (`repl:laggard-reseeded`),
///   gated so it fires only where `p` is provably a prefix of `f`. The proof
///   is `f`'s handshake prefix check (Design B), which the re-seed had to pass
///   to converge, not the rig's wipe (see `route_p_error`).
/// - **Before D3.** The rig failed the universe when the refusal landed in a
///   request, and dropped it when it landed in a convergence. Every error of
///   `p` now goes through `route_p_error`.
/// - **Before D3 round 4** the re-seed itself still dropped errors (review I2).
///   Its wipe ignored a failed directory barrier, and its convergence gave
///   every error up as `restore-stalled`. Both are routed now (`reseed_p`).
///   The census holds all of it, and the behavioural pins are the oracle.
/// - **Since D3 round 5** a restore-needed refusal of either node is answered
///   (restored, or given up) only when a fault injected on that node's own
///   disk explains it, and otherwise fails by name
///   (`unexplained_restore_needed`). A non-empty reopen after a whole, synced
///   re-seed wipe fails by name too.
/// - **Discovery seeds** (not pins, since any re-key renames them):
///   - `0x6d554d5eb1e76a3` and `0xfd74b66d02fe0371`, both at 0485b76: a
///     follower recovery cut by a misdirected write.
///   - `0xa1d1c3332d5b0668`, at 796bff2 and cbfece0: a pure laggard.
/// - **The pins** are `a_restored_follower_refused_as_a_laggard_is_reseeded_not_failed`
///   and `a_laggard_refusal_is_not_reseeded_once_the_primary_could_have_forked`.
///
/// **What else D3 stopped dropping (round 2).**
/// - **The survivor's promise scan** gives up by name when injected faults
///   exhaust it (`repl:inconclusive:survivor-scan-unreadable`), and fails
///   otherwise. It no longer promises nothing for that round and runs on.
/// - **A sacrificial write that died with a reboot of `p`** is skipped
///   (`repl:sacrificial-lost-to-reboot`): no forced cut and no claim, applied
///   or not, and no give-up. Silence without a reboot still gives up.
/// - Both are pinned deterministically, each with its negative twin, and
///   `above_horizon_universe_routes_every_error_of_p` holds the routing.
#[test]
fn wal_corruption_on_the_primary_no_longer_loses_acked_records_with_a_follower() {
    run_universes_swarm_with!(
        0x2EB1_000E,
        128,
        |cfg: &mut FaultConfig| {
            cfg.bit_flip = 0.02;
            cfg.misdirected_write = 0.005;
        },
        |sim: &mut Sim| {
            let r = above_horizon_universe(sim);
            finish_universe(sim, r)
        },
        require: ["fault:bit_flip", "repl:above-horizon-cut", "repl:promised-nonempty"],
        floor: [("repl:promised-nonempty", SURVIVOR_PROMISE_FLOOR)]
    );
}

/// Universes in which the survivor oracle ran NON-VACUOUSLY — the promise set
/// was non-empty, so `assert_survivor_holds` had something to assert — MEASURED:
/// 32 of 128 at base `0x2EB1_000E` on 2026-09-06. The other 96 are 90
/// `repl:inconclusive` plus 6 that reached the end with an empty promise set
/// (`assert_survivor_holds` passes trivially there, which is why the empty case
/// is not a verdict and this key, not the property's completion, is the floor).
///
/// **Why this floor is legitimate even though the 90 give-ups had no cause
/// attribution when Task 2 measured them.** They have it now: this file's
/// kernel-level helper family was instrumented in the same commit as this floor,
/// and the 90 split 78 `:survivor-faulted`, 5 `:recover-exhausted`, 5
/// `:spawn-exhausted`, 3 `:restore-reopen`, 1 `:request-no-response` (sum 92 —
/// two universes cover a cause key inside the `unwrap_or_default()` promise scan
/// and then continue, so the sum is an upper bound on distinct universes, not a
/// contradiction). 87% of the give-ups are the DESIGNATED SURVIVOR faulting: `f`
/// is spawned `recoverable: false` on purpose, because the single-follower
/// promise needs one undamaged copy, so a fault on it ends the universe by
/// construction — a workload-model rule written down in this file, not a budget.
///
/// **Two interventions, neither of which moved this count.** At
/// `RECOVER_ATTEMPTS` 5000 instead of 500, `repl:inconclusive` stayed at exactly
/// 90 and this key at exactly 32 (`:spawn-exhausted` 5 → 0, `:recover-exhausted`
/// 5 → 0, `:survivor-faulted` 78 → 88 — the ten moved, none were rescued).
/// Gating `spawn_kernel_node_with_schema`'s power-cycle: 90 and 32 again
/// (`:spawn-exhausted` 5 → 19 the other way, see that loop's comment). What that
/// does and does not license is set out in task-3-report.md §10 — it is evidence
/// this number is robust to the retry shape, NOT proof that every give-up here
/// is honest.
///
/// **Headroom: 2 binomial SEs below the measurement**, for the reason
/// `SEEDED_CONVERGED_FLOOR` states in full. At p = 32/128 = 0.25, one SE on the
/// count is sqrt(128 · 0.25 · 0.75) = 4.9, so 32 − 2·4.9 = 22.2 → 22. What that
/// does NOT catch: this floor tolerates a **31.3% relative fall in reach**
/// (32 → 22) before it fires — the loosest of the four, because 32 of 128 is the
/// smallest verdict count and binomial noise is proportionally largest there.
/// The 6 universes that finish with an EMPTY promise set are deliberately
/// outside it: they pass `assert_survivor_holds` vacuously, and if that 6 grew
/// while the 32 shrank this floor is what would notice.
///
/// **A drop below this is a finding, not a number to lower.** With the
/// survivor's two give-up keys (below) at 99% of the give-ups, the thing to
/// check first is whether the fault matrix got harsher on the survivor, or its
/// window shorter — either alone can retire this oracle without a single test
/// going red.
///
/// **Re-measured 2026-09-30 for redb 4.3.0**, two identical batches: 36 of 128
/// (the parent commit under redb 4.1.0 measures 30, a control run). The give-ups
/// fell 94 → 90. Every cause but `:survivor-faulted` and one
/// `:request-no-response` is gone, and `:survivor-faulted` rose 81 → 90. That
/// fits universes that used to end on another cause now running on to the
/// survivor's faults (inferred from the totals, not traced per universe). At
/// p = 36/128 = 0.281 one SE is 5.09, so 36 − 2·5.09 =
/// 25.8 → 25; it tolerates a 30.6% relative fall.
///
/// **Re-set 2026-10-01** for S5 and D1 together, two identical batches: 39 of
/// 128 (36 at 796bff2). The 84 give-ups are `:survivor-faulted` 83 and
/// `:role-file-torn` 1. At p = 39/128 = 0.305 one SE is 5.21, so 39 − 2·5.21 =
/// 28.6 → 28; it tolerates a 28.2% relative fall.
///
/// **Corrected 2026-10-01 (sub-project D3).** Every count above that says "the
/// survivor faulting" (`:survivor-faulted`) also counted the survivor being
/// *refused as diverged behind*, which is not a fault on `f`. In that case `f`
/// fell below `p`'s retained base, by lagging past that two-segment window or
/// because `p` forked below it, and the refusal cannot tell which. It now has
/// its own key, `repl:inconclusive:survivor-refused-behind`:
/// - At the pinned base it is 5 of the 83, now split 78 + 5. Every other count,
///   this key's 39 included, is byte-identical.
/// - Over 57 fresh bases it is 351 of 7296 universes (4.8%), and 6.8% of the
///   give-ups.
///
/// **D3 round 2 left this floor where it was.** The pinned base is
/// byte-identical: it has no sacrificial write lost to a reboot and no
/// unreadable promise scan. Over the same 57 fresh bases:
/// - the 31 `:request-no-response` give-ups became
///   `repl:sacrificial-lost-to-reboot` skips;
/// - 10 of those universes then reached a verdict that promised something, so
///   this key's mean rose 32.91 → 33.09 (+0.17 SE);
/// - `:survivor-scan-unreadable` fired 0 times.
const SURVIVOR_PROMISE_FLOOR: u64 = 28;

fn above_horizon_universe(sim: &mut Sim) -> Result<(), String> {
    let db_cfg = horizon_db_cfg();
    let horizon = 200 * SEC_NANOS;
    let mut p = spawn_kernel_node(sim, "p", primary_cfg("p:1"), db_cfg.clone(), horizon, true)?;
    // The designated survivor: never recovered — a fault on it is inconclusive
    // (the single-follower promise needs one clean copy), so its store keeps
    // every record it applied.
    let mut f = spawn_kernel_node(sim, "f", follower_cfg("f:1", "p:1"), db_cfg, horizon, false)?;

    let mut acked: Vec<u64> = Vec::new();
    let mut promised: std::collections::BTreeSet<u64> = Default::default();

    for round in 0..8u64 {
        // The rig's corruption explains a refusal of p only in the round it
        // was made: the round's crash settles its write, and a later round
        // starts from what recovery made of it.
        p.corrupted_by_rig = false;
        for k in 0..3u64 {
            let id = round * 3 + k;
            match drive_request(
                sim,
                &mut [&mut p, &mut f],
                0,
                Request::new(Method::Post, format!("/widget/{id}")),
                KBUDGET,
            ) {
                Ok((200, _)) => acked.push(id),
                Ok(_) => {} // 421 (p demoted after a restore) / 500 — not acked
                Err(e) => route_p_error(sim, &mut p, &mut f, Err(e))?,
            }
        }
        // Converge f onto p. Every error is routed, none dropped. This site
        // used to drop every error but RESTORE_NEEDED and an inconclusive as
        // "best effort", and what it dropped was p refused as a laggard
        // (sub-project D3). The universe then ran on with p refused and could
        // pass having checked nothing past that point.
        let converged = krun_until(sim, &mut [&mut p, &mut f], kconverged, KBUDGET);
        route_p_error(sim, &mut p, &mut f, converged)?;
        // Record everything f durably holds now — it keeps these forever.
        let scan = retry_read_reopen(sim, &mut f, recover_kernel_node, |f| ids_present(f, &acked));
        promised.extend(route_promise_scan(sim, &f, scan)?);

        // The corruption trigger: crash + restart p (bit_flip rolls on p's
        // wal/ at restart). Recovery may cut above the horizon (silent loss,
        // pre-M1) or hard-error below it (the restore path). Random bit_flip
        // alone drives the above-horizon cut only when its byte lands in p's
        // narrow applied-but-uncheckpointed window (`checkpoint_every` 8), so
        // it fires by luck of the fault schedule — fragile, and a stream shift
        // (e.g. the recovery-cut ordering fix) can zero it out. To drive the
        // above-horizon cut with honest margin we append, when p is the
        // primary, one *sacrificial* request to p ALONE — stepping only p, so
        // f is neither shipped it nor pulls it (and p's crash below drops any
        // in-flight ship) — then deterministically corrupt p's new tail (that
        // frame) with the rig's own `corrupt_last_wal_frame` (the sanctioned
        // bit_flip stand-in from
        // `a_seeded_follower_with_zero_frames_refuses_a_lossy_primary`). The
        // sacrificial frame is applied (acked) and usually above the
        // checkpoint, so recovery cuts it above the horizon (fired as the cover
        // below via the `forced_cut` observable). Not always: when the
        // checkpoint has already covered it, recovery refuses p below the
        // horizon instead, and the restore path answers, with the rig's own
        // corruption as the explanation (112 of 7,424 universes over the pinned
        // and 57 fresh bases, D3 round 5). Two things keep this from weakening the
        // property: (1) f never
        // held the sacrificial, so p re-appending at the cut seq next round is
        // conflict-free — never a primary-diverges-behind-a-longer-follower
        // (that scenario, which corrupting p's *converged* tail would
        // manufacture, is the promote/failover path this property does not
        // model and tests elsewhere); (2) the sacrificial id is untracked, so
        // no oracle asserts it survives — an unshipped acked record legitimately
        // dies in an above-horizon cut (DECISIONS §6's bounded-loss window).
        // Gated on p actually being primary (after a restore f is primary and p
        // a follower that would just 421); best-effort otherwise (an injected
        // fault on the sacrificial means no forced cut that round — bit_flip
        // still rolls and below-horizon rounds still restore).
        let forced_cut = sacrificial_cut(sim, &mut p, &mut f, round, KBUDGET)?;
        let last_before_crash = p.kernel.as_ref().and_then(|k| k.app().db().last_applied());
        // A corruption that did not happen forces no cut. When the helper
        // errs (an injected fault on its own IO), `forced_cut` must not go on
        // to claim one — before D3 its error was dropped and the cover below
        // was claimed anyway. Whether or not it succeeds, the attempt is
        // recorded on `p`: it is a fault injected on p's disk, and when the
        // sacrificial frame sits at or below the horizon (the checkpoint
        // covered it) recovery refuses p below the horizon because of it
        // (D3 round 5, seed 0x78f5713c6b6cd7cd at the pinned base).
        p.corrupted_by_rig |= forced_cut;
        let forced_cut = forced_cut && corrupt_last_wal_frame(&p.host).is_ok();
        p.host.crash_now();
        match recover_kernel_node(sim, &mut p) {
            Ok(()) => {
                // The headline path this property is named for: recovery
                // succeeded (no RESTORE_NEEDED / hard-error / restore path) yet
                // discarded a durable, above-the-checkpoint frame — a silent
                // above-horizon cut. Two observables of that one event, either
                // fires the `require`d cover: (1) `forced_cut` — we corrupted
                // an applied above-checkpoint tail above and recovery still
                // returned Ok, which IS an above-horizon cut by construction (a
                // below-horizon corruption would have returned RESTORE_NEEDED);
                // (2) `last_after < last_before` — a natural bit_flip cut that
                // also shrank the applied set. The first is the reliable driver
                // (the second is a fragile proxy: recovery re-applies durable
                // frames the pre-crash in-memory point hadn't yet, so a real
                // cut can leave last_applied unchanged or higher — which is why
                // it silently stopped firing after the recovery-cut stream
                // shift, and why the forced observable exists).
                let last_after = p.kernel.as_ref().and_then(|k| k.app().db().last_applied());
                if forced_cut || last_after < last_before_crash {
                    sim.cover("repl:above-horizon-cut");
                }
            }
            Err(e) => route_p_error(sim, &mut p, &mut f, Err(e))?,
        }
    }

    // Final convergence so f reflects the latest shipped state, routed like
    // every other: a dropped error here let a universe whose survivor had
    // just given up go on to check the promise anyway.
    let converged = krun_until(sim, &mut [&mut p, &mut f], kconverged, KBUDGET);
    route_p_error(sim, &mut p, &mut f, converged)?;

    // THE promise: every acked+converged id still lives on the survivor. No
    // single-node corruption excuse — a converged follower kept the record.
    let promised: Vec<u64> = promised.into_iter().collect();
    // A non-empty `promised` is the other half of a non-vacuous run: an empty
    // set (the primary faulted before ever shipping anything a round
    // converged on) would pass `assert_survivor_holds` trivially. `require`d
    // below so the batch fails by name if that's all 128 universes ever did.
    if !promised.is_empty() {
        sim.cover("repl:promised-nonempty");
    }
    assert_survivor_holds(sim, &mut f, &promised)?;
    // And the replicated jobs stayed exactly-once through it all.
    bump_exactly_once(sim, &mut f)?;
    Ok(())
}

/// The deterministic shape behind the property's laggard arm (sub-project D3).
/// It is fault-free, so the only thing that happens is the laggard.
const LAGGARD_RESEED_SEED: u64 = 0x2EB1_0D30;

/// Builds the post-restore shape `above_horizon_universe` reaches: `f` primary,
/// `p` its follower rebuilt from `f`. `p` is then down while `f`'s retained
/// base moves past `p`'s position, and is recovered at that position. Returns
/// `(p, f, k)`, where `k` is `p`'s position.
fn restored_laggard(sim: &Sim) -> (KNode, KNode, u64) {
    let horizon = 200 * SEC_NANOS;
    let db_cfg = horizon_db_cfg();
    let mut p = spawn_kernel_node(sim, "p", primary_cfg("p:1"), db_cfg.clone(), horizon, true)
        .expect("p spawns fault-free");
    let mut f = spawn_kernel_node(sim, "f", follower_cfg("f:1", "p:1"), db_cfg, horizon, false)
        .expect("f spawns fault-free");
    for id in 0..3u64 {
        let (status, _) = drive_request(
            sim,
            &mut [&mut p, &mut f],
            0,
            Request::new(Method::Post, format!("/widget/{id}")),
            KBUDGET,
        )
        .expect("a fault-free request is answered");
        assert_eq!(status, 200, "a fault-free request to the primary is acked");
    }
    krun_until(sim, &mut [&mut p, &mut f], kconverged, KBUDGET).expect("f converges onto p");
    restore_p_from_survivor(sim, &mut p, &mut f).expect("the restore completes fault-free");
    assert!(
        f.kernel.as_ref().is_some_and(|k| k.is_primary())
            && p.kernel.as_ref().is_some_and(|k| !k.is_primary()),
        "the restore leaves f primary and p its follower"
    );
    let k = klast(&p).expect("the rebuilt follower holds f's records");

    // p's process dies and stays down, so f reaps its session and releases the
    // retention pin that kept frames above p's position.
    p.kernel = None;
    p.host.crash_now();
    for id in 100..400u64 {
        if f.kernel.as_ref().expect("f is up").app().db().log_base() > k {
            break;
        }
        let (status, _) = drive_request(
            sim,
            &mut [&mut f],
            0,
            Request::new(Method::Post, format!("/widget/{id}")),
            KBUDGET,
        )
        .expect("a fault-free request to f is answered");
        assert_eq!(
            status, 200,
            "a fault-free request to the new primary is acked"
        );
    }
    let base = f.kernel.as_ref().expect("f is up").app().db().log_base();
    assert!(
        base > k,
        "f's retained base {base} must pass p's position {k} — if not, this test no longer \
         reaches the laggard shape"
    );
    recover_kernel_node(sim, &mut p).expect("p reopens fault-free");
    assert_eq!(
        klast(&p),
        Some(k),
        "p reopens at its own durable position, below f's retained base"
    );
    (p, f, k)
}

/// A follower the rig rebuilt from `f` and that then lagged past `f`'s
/// retained window is refused as diverged behind (Design B, DESIGN §5). That
/// is the designed answer, and the refusal names the remedy for a true
/// laggard: re-seed it from a fresh data dir. `route_p_error` answers it with
/// exactly that re-seed (the rig's own action) instead of failing the
/// universe. Its records are a prefix of `f`'s. The proof is `f`'s handshake,
/// not the wipe: the rebuild converged only once `f` accepted its `Hello`
/// (Design B's prefix check), and `f` never recovered, so `f`'s log never cut.
#[test]
fn a_restored_follower_refused_as_a_laggard_is_reseeded_not_failed() {
    let sim = Sim::with_faults(LAGGARD_RESEED_SEED, FaultConfig::none());
    let (mut p, mut f, k) = restored_laggard(&sim);
    let r = krun_until(&sim, &mut [&mut p, &mut f], kconverged, KBUDGET);
    let refusal = r.clone().expect_err(
        "p's rejoin below f's base must be refused — if not, this test no longer pins the arm",
    );
    assert!(
        refusal.starts_with("p kernel step: ")
            && refusal.contains(&format!(
                "diverged: the follower holds a proven record at seq {k}, but this primary has \
                 GC'd every record below its retained base"
            )),
        "the refusal is Design B's diverged-behind answer to p at seq {k}; got: {refusal}"
    );
    route_p_error(&sim, &mut p, &mut f, r).expect(
        "a laggard refusal on the follower the rig rebuilt from f is answered with the re-seed \
         it names, not failed",
    );
    assert_eq!(
        sim.coverage().get("repl:laggard-reseeded"),
        Some(&1),
        "the answer is the laggard re-seed, counted by its own key"
    );
    assert!(
        kconverged(&[&mut p, &mut f]),
        "the re-seeded follower converges onto f"
    );
    assert!(
        klast(&p) > Some(k),
        "p moved past its old position {k}: it was re-seeded, not left refused"
    );
}

/// A survivor promise scan that faults on every attempt gives up by name. It
/// must not promise nothing: before D3 round 2, `unwrap_or_default` dropped
/// the error, that round promised nothing, and the universe could still pass.
/// Exhaustion has to classify as fallout for this, so `retry_read_reopen`'s
/// exhaustion now names the last error it ran out on.
#[test]
fn an_exhausted_survivor_scan_gives_up_by_name_rather_than_promising_nothing() {
    let sim = Sim::with_faults(LAGGARD_RESEED_SEED, FaultConfig::none());
    let mut n = spawn_kernel_node(
        &sim,
        "f",
        primary_cfg("f:1"),
        horizon_db_cfg(),
        200 * SEC_NANOS,
        false,
    )
    .expect("the node spawns fault-free");
    n.host.crash_now();
    let store = n.db_cfg.store_path.clone();
    // `ids_present` reads through redb's page cache, which a crashed disk never
    // reaches. This read goes to the disk on every attempt, so every attempt
    // meets the injected crash.
    let scan = retry_read_reopen(&sim, &mut n, recover_kernel_node, |n| {
        let file = n.host.disk().open(&store).map_err(|e| e.to_string())?;
        let mut byte = [0u8; 1];
        file.read_at(0, &mut byte).map_err(|e| e.to_string())?;
        Ok(Vec::<u64>::new())
    });
    let exhausted = scan
        .clone()
        .expect_err("every attempt meets the crashed disk");
    assert!(
        exhausted.starts_with("retry_fallout: 64 attempts all faulted (last: ")
            && is_fault_fallout(&exhausted),
        "the exhaustion names the injected fault it ran out on, so checks::classify reads it \
         as fallout; got: {exhausted}"
    );
    let routed = route_promise_scan(&sim, &n, scan).expect_err(
        "an exhausted scan gives up by name, where the old site promised nothing and ran on",
    );
    assert!(
        routed.starts_with(INCONCLUSIVE) && routed.contains("promise scan"),
        "the give-up names the scan; got: {routed}"
    );
    assert_eq!(
        sim.coverage()
            .get("repl:inconclusive:survivor-scan-unreadable")
            .copied(),
        Some(1),
        "the give-up is counted under its own key"
    );
}

/// The negative space of the scan's give-up: an error that is not
/// injected-fault fallout is the failure it is, never a give-up.
#[test]
fn a_survivor_scan_error_that_is_not_fallout_fails_the_universe() {
    let sim = Sim::with_faults(LAGGARD_RESEED_SEED, FaultConfig::none());
    let mut n = spawn_kernel_node(
        &sim,
        "f",
        primary_cfg("f:1"),
        horizon_db_cfg(),
        200 * SEC_NANOS,
        false,
    )
    .expect("the node spawns fault-free");
    n.kernel = None;
    let scan = retry_read_reopen(&sim, &mut n, recover_kernel_node, |n| ids_present(n, &[0]));
    assert_eq!(
        route_promise_scan(&sim, &n, scan),
        Err("node has no kernel".to_string()),
        "a scan that fails for a reason no fault explains fails the universe"
    );
    assert_eq!(
        sim.coverage()
            .get("repl:inconclusive:survivor-scan-unreadable"),
        None,
        "and is not counted as a give-up"
    );
}

/// The survivor's promise scan meets `f`'s store refused below the horizon:
/// its read hits the redb latch tell once, and the reopen that follows is
/// refused. Returns what the scan returned.
fn scan_through_a_reopen(sim: &Sim, f: &mut KNode) -> Result<Vec<u64>, String> {
    let latched = std::cell::Cell::new(false);
    retry_read_reopen(sim, f, recover_kernel_node, |f| {
        if !latched.replace(true) {
            return Err("the database must be closed: close and re-open the database".to_string());
        }
        ids_present(f, &[0, 1, 2])
    })
}

/// D3 round 3. A below-horizon refusal is fault fallout if and only if durable
/// corruption fired on that node (the rule of S5 R-M3 and S3b item 0). Here a
/// real `bit_flip` fires on `f`'s disk at the scan's reopen, so `f` gives up
/// under `repl:inconclusive:survivor-faulted`. Round 2 failed the universe
/// instead: `checks::classify` reads `universe-restore-needed` as Unrelated.
#[test]
fn a_survivor_corrupted_below_the_horizon_by_an_injected_fault_gives_up() {
    let sim = Sim::with_faults(
        LAGGARD_RESEED_SEED,
        FaultConfig {
            bit_flip: 1.0,
            ..FaultConfig::none()
        },
    );
    let (_p, mut f) = sacrificial_pair(&sim);
    assert!(
        !f.host.durable_corruption_fired(),
        "nothing restarted yet, so no bit flipped"
    );
    let scan = scan_through_a_reopen(&sim, &mut f);
    assert_eq!(
        scan,
        Err(format!("{RESTORE_NEEDED}: f corrupted below the horizon")),
        "the reopen's bit flip lands below f's horizon — if not, this test no longer reaches \
         the refusal"
    );
    assert!(
        f.host.durable_corruption_fired(),
        "the flip fired on f's disk"
    );
    let routed =
        route_promise_scan(&sim, &f, scan).expect_err("a refused survivor promises nothing");
    assert!(
        routed.starts_with(INCONCLUSIVE),
        "corruption that fired on f is fallout, so the universe gives up; got: {routed}"
    );
    assert_eq!(
        sim.coverage()
            .get("repl:inconclusive:survivor-faulted")
            .copied(),
        Some(1),
        "under the survivor's fault key"
    );
}

/// The twin: the same refusal with no durable-corruption fault fired on `f`.
/// The test corrupts `f`'s first WAL frame itself, so no sim fault fires on
/// `f`. A below-horizon refusal that nothing injected explains is a platform
/// durability bug, so it fails by name.
///
/// A real `bit_flip` does fire on `p`'s disk first. That makes the universe
/// count as corrupted, so this twin also catches an excuse that asks the
/// universe-wide `checks::durable_corruption_fired` instead of `f`'s own disk.
#[test]
fn a_survivor_refused_below_the_horizon_with_no_corruption_fired_fails_by_name() {
    let sim = Sim::with_faults(
        LAGGARD_RESEED_SEED,
        FaultConfig {
            bit_flip: 1.0,
            ..FaultConfig::none()
        },
    );
    let (p, mut f) = sacrificial_pair(&sim);
    p.host.restart();
    sim.quiesce_disk_faults(); // f's reopen below flips nothing
    assert!(
        durable_corruption_fired(&sim) && p.host.durable_corruption_fired(),
        "a bit flipped on p's disk, so the universe counts as corrupted"
    );
    let disk = f.host.disk();
    let mut segments = disk.list("wal/").expect("f has a WAL");
    segments.sort();
    let file = disk
        .open(segments.first().expect("f has a WAL segment"))
        .expect("the segment opens");
    let mut crc = [0u8; 1];
    file.read_at(12, &mut crc).expect("frame 1's header reads");
    crc[0] ^= 0xff;
    file.write_at(12, &crc).expect("frame 1's CRC is rewritten");
    file.sync().expect("and made durable");
    let scan = scan_through_a_reopen(&sim, &mut f);
    assert_eq!(
        scan,
        Err(format!("{RESTORE_NEEDED}: f corrupted below the horizon")),
        "frame 1 sits below f's horizon — if not, this test no longer reaches the refusal"
    );
    assert!(
        !f.host.durable_corruption_fired(),
        "no sim fault fired on f, only on p"
    );
    let routed =
        route_promise_scan(&sim, &f, scan).expect_err("a refused survivor promises nothing");
    assert!(
        !routed.contains(INCONCLUSIVE) && routed.contains("fired on f's disk"),
        "with nothing injected on f, the refusal is a failure that says why; got: {routed}"
    );
    assert_eq!(
        sim.coverage().get("repl:inconclusive:survivor-faulted"),
        None,
        "and not a give-up"
    );
}

/// Builds `p` primary and `f` its converged follower, fault-free, so `p`'s
/// witness set names `f`.
fn sacrificial_pair(sim: &Sim) -> (KNode, KNode) {
    let horizon = 200 * SEC_NANOS;
    let db_cfg = horizon_db_cfg();
    let mut p = spawn_kernel_node(sim, "p", primary_cfg("p:1"), db_cfg.clone(), horizon, true)
        .expect("p spawns fault-free");
    let mut f = spawn_kernel_node(sim, "f", follower_cfg("f:1", "p:1"), db_cfg, horizon, false)
        .expect("f spawns fault-free");
    for id in 0..3u64 {
        drive_request(
            sim,
            &mut [&mut p, &mut f],
            0,
            Request::new(Method::Post, format!("/widget/{id}")),
            KBUDGET,
        )
        .expect("a fault-free request is answered");
    }
    krun_until(sim, &mut [&mut p, &mut f], kconverged, KBUDGET).expect("f converges onto p");
    (p, f)
}

/// D3 round 2's diagnosis, pinned.
/// - The sacrificial request is pending in `p`'s kernel while a due cron tick
///   is served first, and that tick's write meets an injected crash on `p`'s
///   disk. (A crash on the request's own write answers it with a 500 instead.)
/// - `kstep_node` reboots `p`, and the request dies with the kernel that held
///   it. The frame was never applied, and nothing will answer.
/// - The round skips the forced cut, claims no `repl:above-horizon-cut`, and
///   gives nothing up.
/// - In 24e4bad it gave the universe up as `request-no-response`, spending
///   reach on a scenario that never happened.
#[test]
fn a_sacrificial_write_lost_to_a_reboot_skips_the_forced_cut_and_claims_nothing() {
    let sim = Sim::with_faults(LAGGARD_RESEED_SEED, FaultConfig::none());
    let (mut p, mut f) = sacrificial_pair(&sim);
    let recoveries = p.recoveries;
    sim.clock().sleep(Duration::from_secs(1)); // the cron tick is due
    p.host.crash_after(0);
    let cut = sacrificial_cut(&sim, &mut p, &mut f, 0, KBUDGET)
        .expect("a sacrificial write lost to a reboot is not an error of p");
    assert!(!cut, "no frame, so no forced cut");
    assert_eq!(
        p.recoveries,
        recoveries + 1,
        "the injected crash rebooted p while the request was in flight — if not, this test no \
         longer reaches the shape"
    );
    assert!(
        p.kernel
            .as_ref()
            .is_some_and(|k| k.is_primary() && k.app().db().verifying()),
        "p booted behind its witness gate, which waits on f"
    );
    let coverage = sim.coverage();
    assert_eq!(
        coverage.get("repl:sacrificial-lost-to-reboot").copied(),
        Some(1),
        "the skip is counted, by what happened"
    );
    assert_eq!(
        coverage.get("repl:inconclusive:request-no-response"),
        None,
        "and nothing is given up"
    );
    assert_eq!(sim.gave_up(), None, "the universe did not give up");
    assert_eq!(
        ids_present(&p, &[10_000]),
        Ok(Vec::new()),
        "the sacrificial frame was never applied"
    );
}

/// The negative space of the skip. A sacrificial write that goes unanswered
/// while `p` never rebooted leaves `p`'s state unknown, so it is routed: a
/// give-up, never a silent skip. Budget 0 is the deterministic way to get
/// silence from a live `p`.
#[test]
fn an_unanswered_sacrificial_write_without_a_reboot_gives_up() {
    let sim = Sim::with_faults(LAGGARD_RESEED_SEED, FaultConfig::none());
    let (mut p, mut f) = sacrificial_pair(&sim);
    let err = sacrificial_cut(&sim, &mut p, &mut f, 0, 0)
        .expect_err("silence from a live p is routed, not skipped");
    assert!(err.starts_with(INCONCLUSIVE), "it gives up; got: {err}");
    assert_eq!(
        sim.coverage()
            .get("repl:inconclusive:request-no-response")
            .copied(),
        Some(1),
        "under the no-response key"
    );
    assert_eq!(
        sim.coverage().get("repl:sacrificial-lost-to-reboot"),
        None,
        "and is not counted as a reboot skip"
    );
}

/// Replays one universe of `wal_corruption_on_the_primary_no_longer_loses_acked_records_with_a_follower`
/// exactly as its swarm runner builds it (the same fault-config sampling and
/// the same two overrides), and returns the world with the universe's result
/// after `finish_universe`.
fn replay_above_horizon(seed: u64) -> (Sim, Result<(), String>) {
    let mut state = seed;
    let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
    let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
    charpente_sim::sample_extended_fields(seed, &mut cfg);
    cfg.bit_flip = 0.02;
    cfg.misdirected_write = 0.005;
    let mut sim = Sim::with_faults(seed, cfg);
    let r = above_horizon_universe(&mut sim);
    let r = finish_universe(&sim, r);
    (sim, r)
}

/// S3b item 8's sweep found the kernel spawn loop still excusing nothing for
/// a refusal below the horizon. Item 0 had taken that refusal out of
/// `is_fault_fallout`, so 187 universes over 192 fresh bases went from 500
/// futile power-cycles into `spawn-exhausted` to failing the property.
/// - **Seed `0x14c38063db535c63`** (base `0x51D31101`): `p`'s spawn is refused
///   for corruption that fired on `p`'s own disk. It gives up at once, under
///   `repl:inconclusive:spawn-durable-refusal`.
/// - **The twin, built by hand:** a bit flip fires on `f` only, and `p`'s own
///   frame 1 is corrupted by the test. `p`'s spawn is refused the same way,
///   but nothing fired on `p` explains it, so it fails by name.
#[test]
fn a_spawn_refused_below_the_horizon_gives_up_only_for_corruption_on_its_own_disk() {
    let (sim, r) = replay_above_horizon(0x14c3_8063_db53_5c63);
    assert_eq!(r, Ok(()), "the universe gives up, it does not fail");
    let reason = sim.gave_up().expect("the universe gives up");
    assert!(
        reason.contains("p kernel spawn refused for durable corruption that fired on p's disk")
            && reason.contains("durable horizon"),
        "for this reason, or the seed no longer names the scenario: {reason}"
    );
    assert_eq!(
        sim.coverage()
            .get("repl:inconclusive:spawn-durable-refusal")
            .copied(),
        Some(1),
        "under the spawn's own key"
    );

    let sim = Sim::with_faults(
        LAGGARD_RESEED_SEED,
        FaultConfig {
            bit_flip: 1.0,
            ..FaultConfig::none()
        },
    );
    let (p, f) = sacrificial_pair(&sim);
    f.host.restart();
    sim.quiesce_disk_faults();
    assert!(
        f.host.durable_corruption_fired() && !p.host.durable_corruption_fired(),
        "a bit flipped on f's disk, none on p's"
    );
    let disk = p.host.disk();
    let mut segments = disk.list("wal/").expect("p has a WAL");
    segments.sort();
    let file = disk
        .open(segments.first().expect("p has a WAL segment"))
        .expect("the segment opens");
    let mut crc = [0u8; 1];
    file.read_at(12, &mut crc).expect("frame 1's header reads");
    crc[0] ^= 0xff;
    file.write_at(12, &crc).expect("frame 1's CRC is rewritten");
    file.sync().expect("and made durable");
    drop(p);
    let refused = spawn_kernel_node(
        &sim,
        "p",
        primary_cfg("p:1"),
        horizon_db_cfg(),
        200 * SEC_NANOS,
        true,
    )
    .err()
    .expect("p's spawn over its corrupted frame is refused");
    assert!(
        refused.starts_with("p: App::open: log frame") && refused.contains("durable horizon"),
        "nothing fired on p explains the refusal, so it fails by name; got: {refused}"
    );
}

/// D3 round 4 (review I2), seed `0x9827a4c2014d41d4`, found by the review
/// over its own fresh bases. A restore re-seeds `p` from `f`, and before the
/// re-seed converges `p` is refused as diverged behind again. That is the
/// laggard refusal itself, raised inside the re-seed's own convergence. It
/// gives up under its own key, `repl:inconclusive:reseed-refused-behind`.
/// Until round 4 every error there was given up as `restore-stalled`, which
/// would have hidden a refusal no fault explains.
#[test]
fn a_reseed_refused_behind_before_it_converges_gives_up_under_its_own_key() {
    let (sim, r) = replay_above_horizon(0x9827_a4c2_014d_41d4);
    assert_eq!(r, Ok(()), "the universe gives up, it does not fail");
    let reason = sim.gave_up().expect("the universe gives up");
    assert!(
        reason.contains("GC'd every record below its retained base"),
        "and for this reason, or the seed no longer names the scenario: {reason}"
    );
    let coverage = sim.coverage();
    assert_eq!(
        coverage
            .get("repl:inconclusive:reseed-refused-behind")
            .copied(),
        Some(1),
        "under the re-seed's own key: {coverage:?}"
    );
    assert_eq!(
        coverage.get("repl:inconclusive:restore-stalled"),
        None,
        "not the old blanket key: {coverage:?}"
    );
}

/// D3 round 4 (review I2), seed `0xa38c1195b6343381`, found by the review.
/// The re-seed's wipe meets a failed directory barrier.
/// - **Until round 4:** the wipe dropped that error, the deleted files came
///   back at the next reboot, and the "fresh" `p` reopened holding 8 records
///   of its own pre-restore timeline against `f`'s 1. That is not a prefix
///   of `f`.
/// - **Now:** the wipe power-cycles `p` and wipes again
///   (`repl:reseed-wipe-retried`), so every re-seed reopens empty. A non-empty
///   reopen after a whole, synced wipe would fail the universe by name (D3
///   round 5): no injected fault explains it.
#[test]
fn a_reseed_whose_wipe_met_a_failed_barrier_wipes_again_and_reopens_empty() {
    let (sim, r) = replay_above_horizon(0xa38c_1195_b634_3381);
    let coverage = sim.coverage();
    assert_eq!(
        r,
        Ok(()),
        "every re-seed reopened empty, so the universe did not fail: {coverage:?}"
    );
    let retried = coverage.get("repl:reseed-wipe-retried").copied();
    assert!(
        retried.unwrap_or(0) > 0,
        "the wipe met an injected fault and was redone, or the seed no longer names the \
         scenario: {coverage:?}"
    );
}

/// Fires a real fault on a third host's disk, of every durable axis `sim`
/// enables (`bit_flip`, `dir_entry_loss`). The universe then counts as
/// faulted while neither `p` nor `f` does, so a twin that runs after this
/// catches an excuse that asks the universe-wide checkers instead of the
/// refused node's own disk.
fn fault_a_bystander(sim: &Sim) {
    let host = sim.host("bystander");
    let file = host
        .disk()
        .open("wal/bystander")
        .expect("the bystander's disk opens");
    file.write_at(0, &[0xAA; 64]).expect("the bystander writes");
    file.sync()
        .expect("and syncs its bytes, not its directory entry");
    host.restart();
    assert!(
        host.durable_corruption_fired() || host.dir_entry_loss_fired(),
        "a fault fired on the bystander's disk"
    );
}

/// D3 round 5, seed `0x78f5713c6b6cd7cd`, a universe of the pinned base. The
/// rig's forced CRC flip (`corrupt_last_wal_frame`), one of the property's
/// three byte-level faults, lands on a sacrificial frame the checkpoint has
/// already covered, so recovery refuses `p` below the horizon (frame 15 at
/// horizon 15). No sim fault fires anywhere in this universe, so the rig's
/// flip is the only explanation in the fault model. It is one, and `p` is
/// restored.
#[test]
fn a_below_horizon_refusal_the_rigs_own_corruption_explains_is_restored() {
    let (sim, r) = replay_above_horizon(0x78f5_713c_6b6c_d7cd);
    let coverage = sim.coverage();
    assert_eq!(r, Ok(()), "the universe does not fail: {coverage:?}");
    assert_eq!(
        (
            coverage.get("fault:bit_flip"),
            coverage.get("fault:misdirected_write"),
            coverage.get("fault:dir-entry"),
        ),
        (None, None, None),
        "no sim fault could explain the refusal, or this seed no longer pins the rig's own"
    );
    assert!(
        coverage
            .get("repl:restored-from-follower")
            .is_some_and(|&n| n > 0),
        "p was refused below the horizon and restored from f: {coverage:?}"
    );
}

/// The re-seed's convergence is routed by kind (D3 round 4, review I2).
/// Until round 4 every error there was `restore-stalled`, a blanket give-up
/// wide enough to swallow a real failure. Since round 5 a RESTORE_NEEDED
/// refusal gives up only when an injected fault on `p`'s own disk explains it
/// (`unexplained_restore_needed`), and fails by name otherwise.
#[test]
fn a_reseed_convergence_error_is_routed_by_kind() {
    let sim = Sim::with_faults(
        LAGGARD_RESEED_SEED,
        FaultConfig {
            bit_flip: 1.0,
            ..FaultConfig::none()
        },
    );
    let (p, _f) = sacrificial_pair(&sim);
    fault_a_bystander(&sim);
    assert!(
        durable_corruption_fired(&sim) && !p.host.durable_corruption_fired(),
        "the universe counts as corrupted, p does not"
    );
    let given_up = format!("{INCONCLUSIVE}: f (the designated survivor) faulted: injected");
    assert_eq!(
        route_reseed_convergence(&sim, &p, Err(given_up.clone())),
        Err(given_up),
        "an error that already gave up passes through, counted at its origin"
    );
    let behind = "p kernel step: diverged: the follower holds a proven record at seq 5, but \
                  this primary has GC'd every record below its retained base 9";
    let routed = route_reseed_convergence(&sim, &p, Err(behind.to_string()))
        .expect_err("a refused re-seed did not converge");
    assert!(
        routed.starts_with(INCONCLUSIVE) && routed.contains(behind),
        "the laggard refusal gives up, naming itself; got: {routed}"
    );
    let restore = format!("{RESTORE_NEEDED}: p corrupted below the horizon");
    let routed = route_reseed_convergence(&sim, &p, Err(restore.clone()))
        .expect_err("a re-seed that needs restoring did not converge");
    assert!(
        !routed.contains(INCONCLUSIVE)
            && routed.contains(&restore)
            && routed.contains("fired on p's disk"),
        "with no fault on p's own disk, RESTORE_NEEDED fails by name; got: {routed}"
    );
    p.host.restart();
    assert!(
        p.host.durable_corruption_fired(),
        "a bit flipped on p's disk"
    );
    let routed = route_reseed_convergence(&sim, &p, Err(restore.clone()))
        .expect_err("a re-seed that needs restoring did not converge");
    assert!(
        routed.starts_with(INCONCLUSIVE) && routed.contains(&restore),
        "with p's own disk corrupted, RESTORE_NEEDED gives up, naming itself; got: {routed}"
    );
    for other in [
        "kernel stall: no work, no pending events, done() false",
        "kernel budget 200000 exhausted before done()",
        "recover p: an error no arm recognises",
    ] {
        let routed = route_reseed_convergence(&sim, &p, Err(other.to_string()))
            .expect_err("an unrecognised error did not converge either");
        assert!(
            !routed.contains(INCONCLUSIVE) && routed.contains(other),
            "anything else fails, naming the error; got: {routed}"
        );
    }
    let coverage = sim.coverage();
    assert_eq!(
        (
            coverage
                .get("repl:inconclusive:reseed-refused-behind")
                .copied(),
            coverage
                .get("repl:inconclusive:reseed-restore-needed")
                .copied(),
            coverage.get("repl:inconclusive:restore-stalled").copied(),
        ),
        (Some(1), Some(1), None),
        "each give-up under its own key, and none under the old blanket key"
    );
}

/// Refuses `p`'s reopen below the horizon by flipping its first WAL frame's
/// CRC by hand, so no sim fault fires on `p`. Returns the refusal.
fn refuse_p_below_the_horizon_by_hand(sim: &Sim, p: &mut KNode) -> String {
    let disk = p.host.disk();
    let mut segments = disk.list("wal/").expect("p has a WAL");
    segments.sort();
    let file = disk
        .open(segments.first().expect("p has a WAL segment"))
        .expect("the segment opens");
    let mut crc = [0u8; 1];
    file.read_at(12, &mut crc).expect("frame 1's header reads");
    crc[0] ^= 0xff;
    file.write_at(12, &crc).expect("frame 1's CRC is rewritten");
    file.sync().expect("and made durable");
    let refused = recover_kernel_node(sim, p).expect_err("p's reopen is refused");
    assert_eq!(
        refused,
        format!("{RESTORE_NEEDED}: p corrupted below the horizon"),
        "frame 1 sits below p's horizon — if not, this pin no longer reaches the refusal"
    );
    refused
}

/// D3 round 5. A restore-needed refusal of `p` is answered with a restore only
/// when an injected fault on `p`'s own disk explains it. Here nothing injected
/// touched `p` (the test corrupts it by hand), so the refusal is a platform
/// durability bug and fails by name. Before round 5 `route_p_error` restored
/// `p` from `f` whatever explained the refusal. A real bit flip fires on a
/// bystander first, so asking the universe-wide checker fails this pin too.
#[test]
fn a_restore_needed_refusal_of_p_with_no_fault_on_p_fails_by_name() {
    let sim = Sim::with_faults(
        LAGGARD_RESEED_SEED,
        FaultConfig {
            bit_flip: 1.0,
            ..FaultConfig::none()
        },
    );
    let (mut p, mut f) = sacrificial_pair(&sim);
    fault_a_bystander(&sim);
    sim.quiesce_disk_faults(); // p's reopen below flips nothing
    let refused = refuse_p_below_the_horizon_by_hand(&sim, &mut p);
    assert!(
        durable_corruption_fired(&sim) && !p.host.durable_corruption_fired(),
        "the universe counts as corrupted, p does not"
    );
    let routed = route_p_error(&sim, &mut p, &mut f, Err(refused.clone()))
        .expect_err("an unexplained refusal fails the universe");
    assert!(
        !routed.contains(INCONCLUSIVE)
            && routed.contains(&refused)
            && routed.contains("fired on p's disk"),
        "it fails by name, saying no fault on p explains it; got: {routed}"
    );
    assert_eq!(
        sim.coverage().get("repl:restored-from-follower"),
        None,
        "and p is not restored over it"
    );
}

/// The twin: the same refusal, but a real bit flip on `p`'s own disk caused
/// it. That is fallout, so `route_p_error` restores `p` from the survivor as
/// before.
#[test]
fn a_restore_needed_refusal_of_p_with_a_fault_on_p_restores_it() {
    let sim = Sim::with_faults(
        LAGGARD_RESEED_SEED,
        FaultConfig {
            bit_flip: 1.0,
            ..FaultConfig::none()
        },
    );
    let (mut p, mut f) = sacrificial_pair(&sim);
    let refused = recover_kernel_node(&sim, &mut p).expect_err("p's reopen is refused");
    assert_eq!(
        refused,
        format!("{RESTORE_NEEDED}: p corrupted below the horizon"),
        "the reopen's bit flip lands below p's horizon — if not, this pin no longer reaches \
         the refusal"
    );
    assert!(
        p.host.durable_corruption_fired(),
        "the flip fired on p's disk"
    );
    sim.quiesce_disk_faults(); // the restore below runs fault-free
    assert_eq!(
        route_p_error(&sim, &mut p, &mut f, Err(refused)),
        Ok(()),
        "corruption that fired on p is fallout, and the restore answers it"
    );
    assert_eq!(
        sim.coverage().get("repl:restored-from-follower").copied(),
        Some(1),
        "p was restored from f"
    );
    assert!(
        f.kernel.as_ref().is_some_and(|k| k.is_primary()) && klast(&p) == klast(&f),
        "f is primary and p converged onto it"
    );
}

/// S3b item 8 (D3 re-review m7). A re-seed wipe deletes every byte an earlier
/// fault touched, so a refusal of the re-seeded `p` is explained only by a
/// fault since that wipe. Here a real bit flip on `p` is answered by the
/// restore, as in the twin above, which re-seeds `p` through `reseed_p`.
/// Afterwards `p`'s lifetime marker still reads true, and the same refusal,
/// with nothing fired since the wipe, fails by name. Until item 8 the lifetime
/// marker explained it. In a second world a flip after the baseline does
/// explain it. In a third, entry loss on `p` before its re-seed explains no
/// later gap of the re-seeded `p`.
#[test]
fn a_fault_before_a_re_seed_wipe_explains_no_later_refusal_of_that_node() {
    let refusal = format!("{RESTORE_NEEDED}: p corrupted below the horizon");
    let flips = FaultConfig {
        bit_flip: 1.0,
        ..FaultConfig::none()
    };
    let sim = Sim::with_faults(LAGGARD_RESEED_SEED, flips.clone());
    let (mut p, mut f) = sacrificial_pair(&sim);
    let refused = recover_kernel_node(&sim, &mut p).expect_err("p's reopen is refused");
    assert_eq!(
        refused, refusal,
        "as in the twin, or this pin reaches nothing"
    );
    sim.quiesce_disk_faults(); // the restore, and everything after, runs fault-free
    route_p_error(&sim, &mut p, &mut f, Err(refused)).expect("p is restored from f");
    assert!(
        p.host.durable_corruption_fired() && !p.corrupted_by_rig,
        "p's disk took a flip before its wipe, and the rig touched nothing"
    );
    let failure = unexplained_restore_needed(&p, &refusal)
        .expect("a fault before the wipe explains nothing after it");
    assert!(
        failure.contains("fired on p's disk since its last re-seed wipe"),
        "{failure}"
    );

    let sim = Sim::with_faults(LAGGARD_RESEED_SEED, flips);
    let (mut p, _f) = sacrificial_pair(&sim);
    p.host.restart();
    p.corruption_at_wipe = p.host.durable_corruption_count(); // as `reseed_p` does
    assert!(
        p.corruption_at_wipe > 0 && unexplained_restore_needed(&p, &refusal).is_some(),
        "a flip before the baseline explains nothing"
    );
    p.host.restart();
    assert!(
        p.corruption_since_wipe() && unexplained_restore_needed(&p, &refusal).is_none(),
        "a flip since the baseline explains the refusal"
    );

    let gapped = format!("{RESTORE_NEEDED}: p log directory gapped by entry loss");
    let sim = Sim::with_faults(
        LAGGARD_RESEED_SEED,
        FaultConfig {
            dir_entry_loss: 1.0,
            ..FaultConfig::none()
        },
    );
    let (mut p, mut f) = sacrificial_pair(&sim);
    p.host
        .disk()
        .open("wal/unsynced")
        .expect("p creates a file whose directory entry is never synced");
    p.host.restart();
    assert!(
        p.host.dir_entry_loss_fired() && unexplained_restore_needed(&p, &gapped).is_none(),
        "entry loss fired on p, so a gap of p is explained before any wipe"
    );
    sim.quiesce_disk_faults();
    restore_p_from_survivor(&sim, &mut p, &mut f).expect("p is re-seeded from f");
    let failure = unexplained_restore_needed(&p, &gapped)
        .expect("entry loss before the wipe explains no gap after it");
    assert!(
        failure.contains("no entry loss (dir_entry_loss) fired on p's disk since its last"),
        "{failure}"
    );
}

/// S3b item 8. `recover_kernel_node` excuses a gapped log directory only by
/// entry loss on the recovered node's own disk since its last wipe. Here a
/// middle segment of `p`'s WAL is deleted by hand, so nothing injected explains the gap,
/// while entry loss fires on a bystander, so the universe counts as having lost
/// an entry. The recovery fails, naming the gap, instead of answering
/// RESTORE_NEEDED. Until item 8 the arm asked the universe-wide
/// `checks::dir_entry_loss_fired`, and it would have answered RESTORE_NEEDED
/// here.
#[test]
fn a_gap_on_one_node_is_not_excused_by_entry_loss_on_another() {
    let sim = Sim::with_faults(
        LAGGARD_RESEED_SEED,
        FaultConfig {
            dir_entry_loss: 1.0,
            ..FaultConfig::none()
        },
    );
    let (mut p, _f) = sacrificial_pair(&sim);
    let disk = p.host.disk();
    let mut segments = disk.list("wal/").expect("p has a WAL");
    segments.sort();
    assert!(
        segments.len() >= 3,
        "p holds a segment between two others, or this pin cannot gap it: {segments:?}"
    );
    disk.remove(&segments[1])
        .expect("a middle segment of p's log is removed");
    disk.sync_dir("wal/").expect("and the removal made durable");
    fault_a_bystander(&sim);
    sim.quiesce_disk_faults();
    assert!(
        dir_entry_loss_fired(&sim) && !p.host.dir_entry_loss_fired(),
        "the universe lost an entry, p did not"
    );
    let refused = recover_kernel_node(&sim, &mut p).expect_err("p's gapped log is refused");
    assert!(
        refused.starts_with("recover p: ") && refused.contains("refusing to guess"),
        "a gap nothing on p explains fails by name; got: {refused}"
    );
}

/// D3 round 5 (round 3's concern 1). A gapped log directory is explained only
/// by entry loss on that node's own disk, and a below-horizon refusal only by
/// durable corruption there. The survivor's promise scan holds both: each
/// gives up (`repl:inconclusive:survivor-faulted`) only when its own fault
/// fired on `f`, and otherwise fails by name. Before round 5 the gapped form
/// failed whatever had fired.
#[test]
fn a_survivor_restore_needed_refusal_is_explained_only_by_its_own_fault_on_f() {
    let sim = Sim::with_faults(
        LAGGARD_RESEED_SEED,
        FaultConfig {
            dir_entry_loss: 1.0,
            ..FaultConfig::none()
        },
    );
    let (_p, f) = sacrificial_pair(&sim);
    fault_a_bystander(&sim);
    assert!(
        dir_entry_loss_fired(&sim) && !f.host.dir_entry_loss_fired(),
        "the universe counts as having lost an entry, f does not"
    );
    let gapped = format!("{RESTORE_NEEDED}: f log directory gapped by entry loss");
    let corrupted = format!("{RESTORE_NEEDED}: f corrupted below the horizon");
    let routed = route_promise_scan(&sim, &f, Err(gapped.clone()))
        .expect_err("a refused survivor promises nothing");
    assert!(
        !routed.contains(INCONCLUSIVE)
            && routed.contains(&gapped)
            && routed.contains("fired on f's disk"),
        "with no entry loss on f, the gap fails by name; got: {routed}"
    );
    f.host
        .disk()
        .open("wal/unsynced")
        .expect("f creates a file whose directory entry is never synced");
    f.host.restart();
    assert!(
        f.host.dir_entry_loss_fired() && !f.host.durable_corruption_fired(),
        "entry loss fired on f's disk, and nothing else did"
    );
    let routed = route_promise_scan(&sim, &f, Err(gapped.clone()))
        .expect_err("a refused survivor promises nothing");
    assert!(
        routed.starts_with(INCONCLUSIVE) && routed.contains(&gapped),
        "with entry loss on f, the gap gives up; got: {routed}"
    );
    let routed = route_promise_scan(&sim, &f, Err(corrupted.clone()))
        .expect_err("a refused survivor promises nothing");
    assert!(
        !routed.contains(INCONCLUSIVE) && routed.contains(&corrupted),
        "entry loss does not explain a below-horizon refusal; got: {routed}"
    );
    assert_eq!(
        sim.coverage()
            .get("repl:inconclusive:survivor-faulted")
            .copied(),
        Some(1),
        "one give-up, under the survivor's fault key"
    );
}

/// `route_p_error` hands back every error it does not recognise unchanged
/// (D3 round 4, review I1). This holds even in the arm's own shape: `p` a
/// follower of a primary `f` that never recovered. The census does not read
/// the router, so this pin is what stops it from learning to swallow a stall
/// or an exhausted budget.
#[test]
fn route_p_error_returns_an_unrecognised_error_unchanged() {
    let sim = Sim::with_faults(LAGGARD_RESEED_SEED, FaultConfig::none());
    let (mut p, mut f, k) = restored_laggard(&sim);
    let before = sim.coverage();
    for other in [
        "kernel stall: no work, no pending events, done() false",
        "kernel budget 200000 exhausted before done()",
        "p kernel step: an error no arm recognises",
        "recover p: an error no arm recognises",
    ] {
        assert_eq!(
            route_p_error(&sim, &mut p, &mut f, Err(other.to_string())),
            Err(other.to_string()),
            "an unrecognised error comes back as it went in"
        );
    }
    assert_eq!(sim.coverage(), before, "and nothing was counted or done");
    assert_eq!(klast(&p), Some(k), "p was neither restored nor re-seeded");
    assert_eq!(
        route_p_error(&sim, &mut p, &mut f, Ok(())),
        Ok(()),
        "and Ok stays Ok"
    );
}

/// Holds "every error is routed, none dropped" by reading the source of
/// `above_horizon_universe` and the three functions it routes through:
/// `sacrificial_cut`, `reseed_p` (the remedy both restore paths apply) and
/// `wipe_data_dir`.
///
/// A dropped error there is invisible at the pinned base: it never drops one,
/// and the D3 sweep found the drops only over fresh bases. So the property
/// cannot hold this itself, and this census does. It is a structural lint; the
/// behavioural pins remain the oracle. Its rules are deliberately tied to the
/// routing shape's spelling, because a looser count let "route some, drop the
/// rest" rewrites through, during D3 and again in its review (I1).
///
/// - **No drop idiom appears outside a comment**, in any of the four:
///   - `let _ =` or a bare `_ =`;
///   - `drop(`;
///   - `Err(_`;
///   - `matches!(`, `if let`/`let` on `Ok`/`Err`/`Some`;
///   - `.is_err()`, `.ok()`, `.err()`;
///   - `unwrap_or` (since D3 round 2 there are no unrouted `f` sites either);
///   - `.is_ok()`, outside the universe's one deliberate use.
/// - **In the universe:**
///   - every `krun_until` is bound and handed whole to `route_p_error` on the
///     next line;
///   - the survivor's promise scan is handed whole to `route_promise_scan`;
///   - every `drive_request` and `recover_kernel_node` is a `match` whose only
///     `Err` arm routes `Err(e)` whole;
///   - the sacrificial write goes through `sacrificial_cut`, `?`-propagated;
///   - `spawn_kernel_node`, `assert_survivor_holds` and `bump_exactly_once`
///     appear only as `…)?;` statements;
///   - one result is consumed on purpose: the corruption helper's `.is_ok()`
///     (a failed corruption forces no cut);
///   - `corrupted_by_rig` is cleared at the top of every round and set only
///     right before the rig's corruption.
/// - **In `sacrificial_cut`:**
///   - its error and its silence without a reboot are both routed, and its
///     only `Err` pattern is the routed arm;
///   - each of its four match arms is matched whole, so an arm cannot route
///     only some of its errors (D3 re-review m9);
///   - the one unrouted outcome, the skip, is gated on `p` having rebooted.
/// - **In `reseed_p` and `wipe_data_dir`:**
///   - every wipe step is `?`-propagated into a wipe loop whose only `Ok` is
///     a whole, synced wipe;
///   - a non-empty reopen is checked, and fails by name rather than giving up;
///   - the wipe clears `corrupted_by_rig` and baselines `p`'s fault counts
///     (S3b item 8);
///   - the convergence is bound and handed whole to `route_reseed_convergence`.
///
/// `route_p_error` itself is not read here.
/// `route_p_error_returns_an_unrecognised_error_unchanged` holds it, and
/// `a_reseed_convergence_error_is_routed_by_kind` holds `route_reseed_convergence`.
///
/// Restructure any of them and this fails by name: re-establish the routing,
/// then update the census.
#[test]
fn above_horizon_universe_routes_every_error_of_p() {
    let src = include_str!("replication.rs");
    let body = |name: &str| -> String {
        let start = src
            .find(&format!("\nfn {name}("))
            .unwrap_or_else(|| panic!("{name} exists"));
        let end = start + src[start..].find("\n}\n").expect("its body ends");
        src[start..end]
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .map(|l| l.split(" // ").next().unwrap_or(l).trim().to_string())
            .collect::<Vec<_>>()
            .join("\n")
    };
    let fix = "route it (sub-project D3: a pass over a dropped error checked nothing)";
    let universe = body("above_horizon_universe");
    let sacrificial = body("sacrificial_cut");
    let reseed = body("reseed_p");
    let wipe = body("wipe_data_dir");
    for (name, code) in [
        ("above_horizon_universe", &universe),
        ("sacrificial_cut", &sacrificial),
        ("reseed_p", &reseed),
        ("wipe_data_dir", &wipe),
    ] {
        for idiom in [
            "let _ =",
            "drop(",
            "Err(_",
            "matches!(",
            "if let Err",
            "if let Ok",
            "let Ok(",
            "let Err(",
            "let Some(",
            ".is_err()",
            ".ok()",
            ".err()",
            "unwrap_or",
        ] {
            assert!(
                !code.contains(idiom),
                "{name} drops a result with `{idiom}` — {fix}"
            );
        }
        assert!(
            !code
                .lines()
                .any(|l| l.starts_with("_ =") && !l.starts_with("_ =>")),
            "{name} drops a result with a bare `_ =` — {fix}"
        );
        if name != "above_horizon_universe" {
            assert!(
                !code.contains(".is_ok()"),
                "{name} drops a result with `.is_ok()` — {fix}"
            );
        }
    }
    let statement = |call: &str| {
        (
            universe.matches(call).count(),
            universe
                .lines()
                .filter(|l| l.contains(call) && l.ends_with(")?;"))
                .count(),
        )
    };
    let shapes = [
        (
            "every krun_until is bound and routed whole on the next line",
            universe.matches("krun_until(").count(),
            universe
                .matches(
                    "let converged = krun_until(sim, &mut [&mut p, &mut f], kconverged, KBUDGET);\n\
                     route_p_error(sim, &mut p, &mut f, converged)?;",
                )
                .count(),
            2,
        ),
        (
            "the survivor's promise scan is routed whole",
            universe.matches("retry_read_reopen(").count(),
            universe
                .matches(
                    "let scan = retry_read_reopen(sim, &mut f, recover_kernel_node, |f| \
                     ids_present(f, &acked));\npromised.extend(route_promise_scan(sim, &f, scan)?);",
                )
                .count(),
            1,
        ),
        (
            "the sacrificial write goes through sacrificial_cut, ?-propagated",
            universe.matches("sacrificial_cut(").count(),
            universe
                .matches("let forced_cut = sacrificial_cut(sim, &mut p, &mut f, round, KBUDGET)?;")
                .count(),
            1,
        ),
        (
            "every drive_request and recover_kernel_node is a match whose only Err arm routes \
             Err(e) whole",
            universe.matches("drive_request(").count()
                + universe.matches("recover_kernel_node(").count(),
            universe
                .matches("Err(e) => route_p_error(sim, &mut p, &mut f, Err(e))?")
                .count(),
            2,
        ),
        (
            "the universe's only Err patterns are those routed arms",
            universe.matches("Err(").count(),
            2 * universe.matches("Err(e) => route_p_error(").count(),
            4,
        ),
        (
            "spawn_kernel_node appears only as a ?-propagated statement",
            statement("spawn_kernel_node(").0,
            statement("spawn_kernel_node(").1,
            2,
        ),
        (
            "assert_survivor_holds appears only as a ?-propagated statement",
            statement("assert_survivor_holds(").0,
            statement("assert_survivor_holds(").1,
            1,
        ),
        (
            "bump_exactly_once appears only as a ?-propagated statement",
            statement("bump_exactly_once(").0,
            statement("bump_exactly_once(").1,
            1,
        ),
        (
            "the rig records its own corruption of p right before attempting it",
            universe.matches("corrupt_last_wal_frame(").count(),
            universe
                .matches(
                    "p.corrupted_by_rig |= forced_cut;\n\
                     let forced_cut = forced_cut && corrupt_last_wal_frame(&p.host).is_ok();",
                )
                .count(),
            1,
        ),
        (
            "the corruption helper's .is_ok() is the universe's only consumed result",
            universe.matches(".is_ok()").count(),
            universe
                .matches("forced_cut && corrupt_last_wal_frame(&p.host).is_ok()")
                .count(),
            1,
        ),
        (
            "sacrificial_cut routes its error and its silence without a reboot",
            sacrificial.matches("route_p_error(").count(),
            sacrificial
                .matches("route_p_error(sim, p, f, Err(e))?;")
                .count()
                + sacrificial
                    .matches("route_p_error(sim, p, f, Err(silent))?;")
                    .count(),
            2,
        ),
        (
            "sacrificial_cut's only Err pattern is the routed arm",
            sacrificial.matches("Err(").count(),
            sacrificial.matches("Err(e) => {").count() + sacrificial.matches("route_p_error(").count(),
            3,
        ),
        (
            "each of sacrificial_cut's match arms is whole, so none can route only some errors",
            sacrificial.matches(" => ").count(),
            [
                "Ok(Some((status, _))) => Ok(status == 200),",
                "Ok(None) if p.recoveries != recoveries => {\n\
                 sim.cover(\"repl:sacrificial-lost-to-reboot\");\nOk(false)\n}",
                "Ok(None) => {\nsim.cover(\"repl:inconclusive:request-no-response\");\n\
                 let silent = format!(\n\"{INCONCLUSIVE}: the sacrificial write to p went \
                 unanswered without p rebooting\"\n);\n\
                 route_p_error(sim, p, f, Err(silent))?;\nOk(false)\n}",
                "Err(e) => {\nroute_p_error(sim, p, f, Err(e))?;\nOk(false)\n}",
            ]
            .iter()
            .map(|arm| sacrificial.matches(arm).count())
            .sum(),
            4,
        ),
        (
            "the rig's corruption explains a refusal only in its own round",
            universe.matches("corrupted_by_rig").count(),
            universe
                .matches("for round in 0..8u64 {\np.corrupted_by_rig = false;")
                .count()
                + universe.matches("p.corrupted_by_rig |= forced_cut;").count(),
            2,
        ),
        (
            "the re-seed's wipe clears the rig's corruption it deleted",
            reseed.matches("corrupted_by_rig").count(),
            reseed
                .matches("\nwiped?;\np.corrupted_by_rig = false;")
                .count(),
            1,
        ),
        (
            "the re-seed's wipe baselines p's own fault counts, right after it",
            reseed.matches("_at_wipe = ").count(),
            reseed
                .matches(
                    "p.corrupted_by_rig = false;\n\
                     p.corruption_at_wipe = p.host.durable_corruption_count();\n\
                     p.entry_loss_at_wipe = p.host.dir_entry_loss_count();",
                )
                .count()
                * 2,
            2,
        ),
        (
            "sacrificial_cut's skip is gated on p having rebooted",
            sacrificial.matches("Ok(None)").count(),
            sacrificial
                .matches("Ok(None) if p.recoveries != recoveries =>")
                .count()
                + sacrificial.matches("Ok(None) => {").count(),
            2,
        ),
        (
            "every wipe step is ?-propagated",
            wipe.matches("disk.").count(),
            wipe.matches(".map_err(|e| e.to_string())?").count(),
            4,
        ),
        (
            "the wipe's only Ok is a whole wipe",
            wipe.matches("Ok(").count() + reseed.matches("wipe_data_dir(").count(),
            wipe.matches("\nOk(())").count() + reseed.matches("match wipe_data_dir(&p.host) {").count(),
            2,
        ),
        (
            "reseed_p's only Ok is a whole, synced wipe",
            reseed.matches("Ok(").count(),
            2 * reseed.matches("Ok(()) => break Ok(()),").count(),
            2,
        ),
        (
            "reseed_p propagates the wipe's outcome",
            reseed.matches("let wiped =").count() + reseed.matches("wiped?").count(),
            reseed.matches("let wiped = loop {").count() + reseed.matches("\nwiped?;").count(),
            2,
        ),
        (
            "reseed_p fails by name, with no give-up, when p reopens non-empty",
            reseed.matches("if klast(p).is_some() {").count(),
            reseed
                .matches("if klast(p).is_some() {\nreturn Err(format!(\n\"re-seeding p: it reopened")
                .count(),
            1,
        ),
        (
            "reseed_p checks that p reopened empty",
            reseed.matches("klast(p)").count(),
            reseed.matches("if klast(p).is_some() {").count() * 2,
            2,
        ),
        (
            "reseed_p's convergence is bound and routed whole",
            reseed.matches("krun_until(").count(),
            reseed
                .matches(
                    "let converged = krun_until(sim, &mut [&mut *p, &mut *f], kconverged, \
                     KBUDGET);\nroute_reseed_convergence(sim, p, converged)",
                )
                .count(),
            1,
        ),
    ];
    for (rule, found, shaped, expected) in shapes {
        assert_eq!((found, shaped), (expected, expected), "{rule} — {fix}");
    }
}

/// The negative space of the arm above. Once `f` has recovered, its log may
/// have been cut and grown back differently, so the refusal's other remedy
/// (this primary forked below the follower: promote the follower) is live,
/// and the rig cannot tell the two apart. The refusal must reach the
/// property as the failure it is, never a re-seed that would overwrite `p`.
#[test]
fn a_laggard_refusal_is_not_reseeded_once_the_primary_could_have_forked() {
    let sim = Sim::with_faults(LAGGARD_RESEED_SEED, FaultConfig::none());
    let (mut p, mut f, k) = restored_laggard(&sim);
    f.recoveries = 1;
    let r = krun_until(&sim, &mut [&mut p, &mut f], kconverged, KBUDGET);
    let err = route_p_error(&sim, &mut p, &mut f, r)
        .expect_err("a refusal the rig cannot adjudicate must still fail the universe");
    assert!(
        err.starts_with("p kernel step: ")
            && err.contains("GC'd every record below its retained base"),
        "the diverged-behind refusal itself bubbles, unanswered; got: {err}"
    );
    assert_eq!(
        sim.coverage().get("repl:laggard-reseeded"),
        None,
        "no re-seed was counted"
    );
    assert_eq!(klast(&p), Some(k), "p was not re-seeded");
}

// ============================================================ M2 sub-project
// 0b task 2: the durable witness set — recording every follower a primary
// ships to (spec docs/superpowers/specs/2026-07-31-dual-primary-design.md
// §1; dispatch .superpowers/sdd/2026-07-31-dual-primary/task-2-brief.md,
// with four corrections layered on top by the dispatching agent — exact
// placement, persist-before-memory ordering, and the trust-boundary
// refusal for an unrecordable `listen`).

const RECORDS_WITNESS_SEED: u64 = 0x2EB1_0103;

/// The headline: a primary durably records each follower it ships to,
/// asserted through a restart — the point of the set is that it survives
/// one.
#[test]
fn a_primary_records_each_follower_it_ships_to_as_a_witness() {
    let sim = Sim::with_faults(RECORDS_WITNESS_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();

    for i in 0..10u64 {
        commit_widget(&sim, &mut p, i).unwrap();
        drive_rounds(&sim, &mut [&mut p, &mut f], 3).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_converged(&sim, &mut p, &mut f).unwrap();

    // Restart p — the point of the set is that it survives a restart.
    recover_node(&sim, &mut p).unwrap();

    let witnesses = load_witnesses(&p.host.disk()).unwrap();
    assert_eq!(
        witnesses,
        vec!["f:1".to_string()],
        "the primary must durably record every follower it agreed to ship to"
    );
}

// -------------------------------------------------- retry (Correction 3)

/// A `Disk` decorator used only by the retry-property test below: it fails
/// the first `fail_remaining` `write_at` calls on exactly one path (the
/// witness file), passing every other call — and every other path —
/// straight through to `inner`.
struct FlakyWitnessDisk {
    inner: Box<dyn Disk>,
    fail_remaining: Arc<AtomicU32>,
}

impl Disk for FlakyWitnessDisk {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        let file = self.inner.open(path)?;
        // Slot 0 specifically: `Repl::open` establishes slot 0 at generation
        // 0 and slot 1 at generation 1 (fix round 1, §2), so the first REAL
        // witness write targets slot 0 — the slot not holding the max valid
        // generation. This test's node ships to exactly one follower, so
        // that is its one-and-only witness write.
        if path == charpente::repl::witnesses::witness_path(0) {
            Ok(Box::new(FlakyFile {
                inner: file,
                fail_remaining: self.fail_remaining.clone(),
            }))
        } else {
            Ok(file)
        }
    }
    fn remove(&self, path: &str) -> io::Result<()> {
        self.inner.remove(path)
    }
    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.inner.list(prefix)
    }
    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        self.inner.sync_dir(prefix)
    }
}

struct FlakyFile {
    inner: Box<dyn File>,
    fail_remaining: Arc<AtomicU32>,
}

impl File for FlakyFile {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read_at(offset, out)
    }
    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        if self.fail_remaining.load(Ordering::SeqCst) > 0 {
            self.fail_remaining.fetch_sub(1, Ordering::SeqCst);
            return Err(io::Error::other("injected test fault: witness write"));
        }
        self.inner.write_at(offset, data)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync(&self) -> io::Result<()> {
        self.inner.sync()
    }
}

const WITNESS_RETRY_SEED: u64 = 0x2EB1_0104;

/// Correction 3 (task-2 dispatch): `record_witnesses` must persist BEFORE
/// committing to the in-memory mirror. A push-then-store shape would mark
/// the address "known" in memory the instant the (failed) store is
/// attempted, so the next step's `!self.witnesses.contains(addr)` guard
/// would then be false and the durable write would never be retried — the
/// witness lost from disk forever while memory claims it is there
/// (`witnesses.rs`'s own doc names exactly this hazard on the read side —
/// "a torn write must never parse as a SHORTER set"; this is the write-side
/// analogue: a failed write must never be silently treated as a landed one).
///
/// Drives the primary's `Repl` directly rather than through the harness's
/// `step_node`/`recover_node` auto-recovery: a restart reloads `witnesses`
/// fresh from disk, which would mask the very bug this test exists to catch
/// (the in-memory-vs-disk divergence only shows up across two `step` calls
/// on the SAME live `Repl`).
///
/// Fix round 1, Minor #2: this drives a mode production never reaches.
/// `Kernel::step`'s `Ready::Repl` arm propagates a failed `record_witnesses`
/// straight out of the whole kernel loop (fail-stop) rather than taking a
/// second `step` on the live `Repl` the way this test does — production's
/// only retry is the next process start, where `Repl::open` reloads
/// `witnesses` fresh from disk. This test still proves the property that
/// matters for BOTH paths: `record_witnesses`'s in-memory mirror never
/// claims a write landed before it did, so neither an in-process retry
/// (this test) nor a fresh-process retry (production) is starved by a
/// stale "already known" guard.
#[test]
fn a_failed_witness_write_is_retried_not_silently_dropped() {
    let sim = Sim::with_faults(WITNESS_RETRY_SEED, FaultConfig::none());
    let ph = sim.host("p");
    let fh = sim.host("f");

    let mut pdb =
        Db::open(Box::new(ph.disk()), DbConfig::default(), make_schema()).expect("primary db");
    // Armed AFTER `Repl::open`, not before (fix round 1, §2): open now
    // ESTABLISHES both witness slots, so a fault armed here would be spent
    // on initialization instead of on the first real witness write, which is
    // the write this test exists to interrupt.
    let fail_remaining = Arc::new(AtomicU32::new(0));
    let flaky = FlakyWitnessDisk {
        inner: Box::new(ph.disk()),
        fail_remaining: fail_remaining.clone(),
    };
    let (mut prepl, _) = Repl::open(
        Box::new(ph.net()),
        Box::new(flaky),
        primary_cfg("p:1"),
        &mut pdb,
    )
    .expect("primary repl");
    fail_remaining.store(1, Ordering::SeqCst);

    let mut fdb =
        Db::open(Box::new(fh.disk()), DbConfig::default(), make_schema()).expect("follower db");
    let (mut frepl, _) = Repl::open(
        Box::new(fh.net()),
        Box::new(fh.disk()),
        follower_cfg("f:1", "p:1"),
        &mut fdb,
    )
    .expect("follower repl");

    let mut failures = 0u32;
    for _ in 0..500u32 {
        let now = sim.now().as_nanos();
        prepl.pump_io(now);
        frepl.pump_io(now);
        let mut worked = false;
        if frepl.has_work(&fdb, now) {
            frepl
                .step(&mut fdb, &sim.clock())
                .expect("follower step: no faults injected on f");
            worked = true;
        }
        if prepl.has_work(&pdb, now) {
            match prepl.step(&mut pdb, &sim.clock()) {
                Ok(()) => {}
                Err(e) => {
                    failures += 1;
                    assert_eq!(failures, 1, "the injected fault must only fire once: {e}");
                    assert!(
                        e.to_string().contains("injected"),
                        "expected the injected witness fault, got: {e}"
                    );
                }
            }
            worked = true;
        }
        if worked {
            continue;
        }
        let mut next = sim.next_wakeup();
        for d in [prepl.next_deadline(now), frepl.next_deadline(now)] {
            if let Some(d) = d
                && d > now
            {
                next = Some(next.map_or(d, |x| x.min(d)));
            }
        }
        match next {
            Some(t) if t > now => sim.clock().sleep(Duration::from_nanos(t - now)),
            _ => break,
        }
    }

    assert_eq!(
        failures, 1,
        "the injected witness write failure never fired — test setup is wrong"
    );
    match load_witnesses(&ph.disk()) {
        Ok(w) => assert_eq!(
            w,
            vec!["f:1".to_string()],
            "a failed witness write must be retried on a later step and land durably"
        ),
        Err(e) => panic!(
            "expected the retried write to land the witness durably, but load_witnesses failed \
             instead ({e}) — the failed attempt was likely committed to the in-memory mirror \
             before the disk write succeeded, so the retry never re-attempted store_witnesses \
             at all"
        ),
    }
}

// ----------------------------------------------- refusal (Correction 4)

const INVALID_LISTEN_SEED: u64 = 0x2EB1_0105;

/// Correction 4 (task-2 dispatch): a Hello whose `listen` address
/// `store_witnesses` would reject (empty, or containing ASCII whitespace)
/// must be refused by name before a ship session is ever established. With
/// no peer secret configured, ANY client reaching the replication port
/// controls `listen` — so this is the trust-boundary check that keeps an
/// unrecordable peer from silently reopening the dual-primary gap the
/// witness set exists to close. `listen: None` stays legal.
#[test]
fn a_hello_with_an_invalid_listen_address_is_refused_by_name() {
    let sim = Sim::with_faults(INVALID_LISTEN_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let schema_digest = p.db.as_ref().unwrap().schema_digest();

    let conn = sim
        .host("c")
        .net()
        .connect("p:1")
        .expect("connect to the primary's listener");
    conn.send(&encode(&Msg::Hello {
        proto: PROTO_VERSION,
        listen: Some("10.0.0.1 7001".to_string()), // contains a space
        position: None,
        secret: None,
        schema_digest,
    }))
    .unwrap();

    let mut buf = vec![0u8; MAX_MESSAGE_BYTES];
    let mut reply = None;
    for _ in 0..64 {
        drive_rounds(&sim, &mut [&mut p], 1).unwrap();
        match conn.recv(&mut buf) {
            Ok(Some(n)) => {
                reply = Some(decode(&buf[..n]).unwrap());
                break;
            }
            Ok(None) => {}
            Err(e) => panic!("conn recv failed: {e}"),
        }
    }
    let Some(Msg::Refuse { message }) = reply else {
        panic!("expected a Refuse reply, got {reply:?}");
    };
    assert_eq!(
        message,
        "Hello listen address \"10.0.0.1 7001\" is invalid — a witness address must be \
         non-empty, at most 300 bytes, and must not contain ASCII whitespace (space, tab, CR, \
         or LF); this node cannot durably record it as a witness, so the connection is refused \
         rather than shipped-but-unrecorded"
    );
    // Minor #5 (fix round 1): the refused peer must never have become a
    // witness — the whole point of refusing before a ship session exists.
    assert_eq!(
        load_witnesses(&p.host.disk()).unwrap(),
        Vec::<String>::new(),
        "a refused Hello must never leave the refused address in the witness set"
    );

    // Minor #6 (fix round 1): the empty-string half of `valid_witness_address`
    // was unpinned — only the whitespace half was exercised above.
    let conn_empty = sim
        .host("c-empty")
        .net()
        .connect("p:1")
        .expect("connect to the primary's listener");
    conn_empty
        .send(&encode(&Msg::Hello {
            proto: PROTO_VERSION,
            listen: Some(String::new()),
            position: None,
            secret: None,
            schema_digest,
        }))
        .unwrap();
    let mut reply_empty = None;
    for _ in 0..64 {
        drive_rounds(&sim, &mut [&mut p], 1).unwrap();
        match conn_empty.recv(&mut buf) {
            Ok(Some(n)) => {
                reply_empty = Some(decode(&buf[..n]).unwrap());
                break;
            }
            Ok(None) => {}
            Err(e) => panic!("conn recv failed: {e}"),
        }
    }
    let Some(Msg::Refuse {
        message: empty_message,
    }) = reply_empty
    else {
        panic!("expected a Refuse reply for an empty listen address, got {reply_empty:?}");
    };
    assert_eq!(
        empty_message,
        "Hello listen address \"\" is invalid — a witness address must be non-empty, at most \
         300 bytes, and must not contain ASCII whitespace (space, tab, CR, or LF); this node \
         cannot durably record it as a witness, so the connection is refused rather than \
         shipped-but-unrecorded"
    );

    // `listen: None` stays legal — a peer that advertises no address simply
    // is not a witness candidate, and must never be refused for it.
    let conn2 = sim
        .host("c2")
        .net()
        .connect("p:1")
        .expect("connect to the primary's listener");
    conn2
        .send(&encode(&Msg::Hello {
            proto: PROTO_VERSION,
            listen: None,
            position: None,
            secret: None,
            schema_digest,
        }))
        .unwrap();
    let mut reply2 = None;
    for _ in 0..64 {
        drive_rounds(&sim, &mut [&mut p], 1).unwrap();
        match conn2.recv(&mut buf) {
            Ok(Some(n)) => {
                reply2 = Some(decode(&buf[..n]).unwrap());
                break;
            }
            Ok(None) => {}
            Err(e) => panic!("conn recv failed: {e}"),
        }
    }
    // Minor #4 (fix round 1): assert the POSITIVE shape, not merely
    // "not a Refuse" — the old assertion was trivially true when reply2
    // stayed `None` (a 64-round timeout), which would silently pass even a
    // regression that dropped the Hello entirely. Proven vacuous by
    // mutation before this fix: forcing 0 rounds (reply2 stays `None`, the
    // exact shape a dropped Hello leaves) still passed the old assertion.
    // A live, correctly-serviced Frames/Snapshot session sends one of these
    // three message kinds first (Frames when there's a backlog, Heartbeat
    // when idle, SnapshotStart when the log has been compacted).
    assert!(
        matches!(
            reply2,
            Some(Msg::Frames { .. } | Msg::Heartbeat { .. } | Msg::SnapshotStart { .. })
        ),
        "a Hello with listen: None must be served (Frames/Heartbeat/SnapshotStart), not \
         refused and not silently dropped — got {reply2:?}"
    );
}

const LONG_INVALID_LISTEN_SEED: u64 = 0x2EB1_0107;

/// Minor #7 (fix round 1): `MAX_MESSAGE_BYTES` is 17 MiB, so an unbounded
/// echo of a peer-controlled `listen` address lets a peer force this
/// refusal to balloon to megabytes — allocated, encoded, sent back, and
/// logged by an operator. Not an amplification attack (the peer already
/// sent us those bytes) but waste and log-flooding, and every other
/// refusal in the ladder echoes bounded data. A 250-byte invalid (contains
/// a space) address must come back truncated.
#[test]
fn a_long_invalid_listen_address_is_truncated_in_the_refusal() {
    let sim = Sim::with_faults(LONG_INVALID_LISTEN_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let schema_digest = p.db.as_ref().unwrap().schema_digest();

    let long_addr = format!("{} x", "a".repeat(250)); // invalid (space), 252 bytes
    let conn = sim
        .host("c-long")
        .net()
        .connect("p:1")
        .expect("connect to the primary's listener");
    conn.send(&encode(&Msg::Hello {
        proto: PROTO_VERSION,
        listen: Some(long_addr.clone()),
        position: None,
        secret: None,
        schema_digest,
    }))
    .unwrap();

    let mut buf = vec![0u8; MAX_MESSAGE_BYTES];
    let mut reply = None;
    for _ in 0..64 {
        drive_rounds(&sim, &mut [&mut p], 1).unwrap();
        match conn.recv(&mut buf) {
            Ok(Some(n)) => {
                reply = Some(decode(&buf[..n]).unwrap());
                break;
            }
            Ok(None) => {}
            Err(e) => panic!("conn recv failed: {e}"),
        }
    }
    let Some(Msg::Refuse { message }) = reply else {
        panic!("expected a Refuse reply, got {reply:?}");
    };
    assert!(
        !message.contains(&long_addr),
        "the refusal must not echo the full peer-controlled address verbatim: {message}"
    );
    let expected_shown = format!("{}...", &long_addr[..200]);
    assert_eq!(
        message,
        format!(
            "Hello listen address {expected_shown:?} is invalid — a witness address must be \
             non-empty, at most 300 bytes, and must not contain ASCII whitespace (space, tab, \
             CR, or LF); this node cannot durably record it as a witness, so the connection is \
             refused rather than shipped-but-unrecorded"
        ),
        "the truncated refusal's exact shape must be pinned (hard rule 5)"
    );
}

const WITNESS_INIT_ORDERING_SEED: u64 = 0x2EB1_0109;

/// Fix round 1, §2 evidence item 3 — the ORDERING the whole soundness
/// argument rests on, pinned rather than left as prose.
///
/// `open_witnesses` reads "a torn slot whose sibling is absent" as an empty
/// set. That is sound only because the state is reachable *only before
/// initialization completed*, and at that moment the true witness set is
/// necessarily empty. What makes it "only before" is placement:
/// initialization runs inside `Repl::open`, so it is finished before the
/// caller holds a `Repl` at all — before `pump_io`, before `step`, before any
/// Hello can be serviced and any frame can ship.
///
/// So this test drives NOTHING. It asserts the invariant at the instant
/// `Repl::open` returns, which is the earliest instant any shipping could
/// begin: both slots exist, and either one alone still answers, so the very
/// first real witness write already has a predecessor to fall back to.
#[test]
fn both_witness_slots_are_established_before_any_frame_can_ship() {
    for wreck in [0u8, 1u8] {
        let sim = Sim::with_faults(WITNESS_INIT_ORDERING_SEED, FaultConfig::none());
        let ph = sim.host("p");
        let mut pdb =
            Db::open(Box::new(ph.disk()), DbConfig::default(), make_schema()).expect("primary db");
        let (prepl, _) = Repl::open(
            Box::new(ph.net()),
            Box::new(ph.disk()),
            primary_cfg("p:1"),
            &mut pdb,
        )
        .expect("primary repl");
        assert_eq!(
            prepl.stats().seeds,
            0,
            "sanity: this node has done no replication work yet"
        );

        let disk = ph.disk();
        for slot in [0u8, 1u8] {
            let path = charpente::repl::witnesses::witness_path(slot);
            assert!(
                disk.list(&path).unwrap().iter().any(|n| n == &path),
                "slot {slot} must be established the instant Repl::open returns, before any \
                 frame can ship"
            );
        }

        let path = charpente::repl::witnesses::witness_path(wreck);
        let file = disk.open(&path).unwrap();
        file.write_at(0, b"\xff\xfe").unwrap();
        file.sync().unwrap();
        assert_eq!(
            load_witnesses(&disk).unwrap(),
            Vec::<String>::new(),
            "with slot {wreck} destroyed the surviving slot must still answer — before this \
             task, a torn first-ever witness write had no sibling and hard-errored, bricking \
             node startup on every role"
        );
    }
}

const OVERLONG_LISTEN_SEED: u64 = 0x2EB1_0108;

/// Minor #3 (fix round 1): the LENGTH rule had no trust-boundary pin. The
/// three refusal tests above all carry addresses that are already invalid
/// for whitespace or emptiness, so they pin the refusal's new *text* without
/// ever exercising the new *rule* at the wire — and the length rule is the
/// one that arrived latest, on the peer-supplied path `MAX_WITNESS_ADDRESS_-
/// BYTES` exists for. This Hello's `listen` is otherwise perfectly
/// well-formed: no whitespace, non-empty, a plausible `host:port` shape. Only
/// its length makes it unrecordable.
#[test]
fn a_hello_with_an_overlong_but_well_formed_listen_address_is_refused() {
    let sim = Sim::with_faults(OVERLONG_LISTEN_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let schema_digest = p.db.as_ref().unwrap().schema_digest();

    // 301 bytes: whitespace-free, non-empty, `host:port`-shaped — invalid on
    // length alone (the bound is 300).
    let overlong = format!("{}:7001", "h".repeat(296));
    assert_eq!(overlong.len(), 301);

    let conn = sim
        .host("c-overlong")
        .net()
        .connect("p:1")
        .expect("connect to the primary's listener");
    conn.send(&encode(&Msg::Hello {
        proto: PROTO_VERSION,
        listen: Some(overlong.clone()),
        position: None,
        secret: None,
        schema_digest,
    }))
    .unwrap();

    let mut buf = vec![0u8; MAX_MESSAGE_BYTES];
    let mut reply = None;
    for _ in 0..64 {
        drive_rounds(&sim, &mut [&mut p], 1).unwrap();
        match conn.recv(&mut buf) {
            Ok(Some(n)) => {
                reply = Some(decode(&buf[..n]).unwrap());
                break;
            }
            Ok(None) => {}
            Err(e) => panic!("conn recv failed: {e}"),
        }
    }
    let Some(Msg::Refuse { message }) = reply else {
        panic!("expected a Refuse reply for an over-length listen address, got {reply:?}");
    };
    let expected_shown = format!("{}...", &overlong[..200]);
    assert_eq!(
        message,
        format!(
            "Hello listen address {expected_shown:?} is invalid — a witness address must be \
             non-empty, at most 300 bytes, and must not contain ASCII whitespace (space, tab, \
             CR, or LF); this node cannot durably record it as a witness, so the connection is \
             refused rather than shipped-but-unrecorded"
        )
    );
    assert_eq!(
        load_witnesses(&p.host.disk()).unwrap(),
        Vec::<String>::new(),
        "an over-length address must never reach the witness set"
    );

    // The boundary is a MAX, not a stricter cutoff: exactly 300 bytes is
    // served, so the refusal cannot be passing for an off-by-one reason.
    let at_bound = format!("{}:7001", "h".repeat(295));
    assert_eq!(at_bound.len(), 300);
    let conn_ok = sim
        .host("c-at-bound")
        .net()
        .connect("p:1")
        .expect("connect to the primary's listener");
    conn_ok
        .send(&encode(&Msg::Hello {
            proto: PROTO_VERSION,
            listen: Some(at_bound.clone()),
            position: None,
            secret: None,
            schema_digest,
        }))
        .unwrap();
    let mut reply_ok = None;
    for _ in 0..64 {
        drive_rounds(&sim, &mut [&mut p], 1).unwrap();
        match conn_ok.recv(&mut buf) {
            Ok(Some(n)) => {
                reply_ok = Some(decode(&buf[..n]).unwrap());
                break;
            }
            Ok(None) => {}
            Err(e) => panic!("conn recv failed: {e}"),
        }
    }
    assert!(
        matches!(
            reply_ok,
            Some(Msg::Frames { .. } | Msg::Heartbeat { .. } | Msg::SnapshotStart { .. })
        ),
        "a Hello at exactly the length bound must be served, not refused — got {reply_ok:?}"
    );
    assert_eq!(
        load_witnesses(&p.host.disk()).unwrap(),
        vec![at_bound],
        "the served peer must be recorded as a witness"
    );
}

// ------------------------------------------- clear-on-flip (Correction 1)

const SWITCHOVER_CLEARS_WITNESSES_SEED: u64 = 0x2EB1_0110;

/// Clear-on-flip, site 1: `complete_switchover` (mod.rs — the graceful
/// HandOffDone completion) must clear the outgoing primary's witness set on
/// its durable flip to Follower. Deterministic, fault-free. Site 2
/// (`resolve_to_follower`, the crash-interrupted-switchover demote path) is
/// exercised — with a witness assertion added by this task — by
/// `crash_between_handoff_and_demote_never_yields_two_writable_primaries`
/// above, so it is not duplicated here.
#[test]
fn a_graceful_switchover_clears_the_outgoing_primarys_witnesses() {
    let sim = Sim::with_faults(SWITCHOVER_CLEARS_WITNESSES_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    for i in 0..10u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_eq!(
        load_witnesses(&p.host.disk()).unwrap(),
        vec!["f:1".to_string()],
        "sanity: p must have durably recorded f as a witness before the switchover"
    );

    p.flipped = true;
    f.flipped = true;
    let mut cli = CtrlConn::open(
        &sim,
        "p:1",
        &Msg::CtrlSwitchover {
            to: "f:1".to_string(),
            secret: None,
        },
    )
    .unwrap();
    let reply = drive_ctrl(&sim, &mut [&mut p, &mut f], &mut cli, CONVERGE_BUDGET).unwrap();
    assert!(
        matches!(reply, Msg::CtrlOk { .. }),
        "expected a clean, fault-free switchover: {reply:?}"
    );
    assert!(
        matches!(p.repl.role(), Role::Follower { .. }),
        "p must be a follower after the switchover, got {:?}",
        p.repl.role()
    );
    assert_eq!(
        load_witnesses(&p.host.disk()).unwrap(),
        Vec::<String>::new(),
        "complete_switchover must clear p's witnesses on the durable flip to Follower — a \
         stale witness would block writes after a future legitimate promotion"
    );
}

// =================================================== the boot gate (task 5)
//
// The task that arms everything tasks 1-4 built (spec §2's ruled option (c)):
// a node resuming as a durable `Primary` with a non-empty witness set serves
// reads but withholds writes until every witness has answered "not a primary".
// The gap being closed: a primary dies, an operator promotes the follower,
// the old box is restarted EXACTLY as it was configured and — before this
// task — resumed as a second writable primary in total silence.

const RESURRECTION_SEED: u64 = 0x2EB1_0111;
const RESURRECTION_REFUSAL_SEED: u64 = 0x2EB1_0112;
const EMPTY_WITNESS_SEED: u64 = 0x2EB1_0113;
const UNREACHABLE_WITNESS_SEED: u64 = 0x2EB1_0114;
const STRICT_WITNESS_SEED: u64 = 0x2EB1_0115;
const SWITCHOVER_WHILE_VERIFYING_SEED: u64 = 0x2EB1_0116;
const PROMOTE_CLEARS_WITNESSES_SEED: u64 = 0x2EB1_0117;
const GATE_OPENS_KERNEL_SEED: u64 = 0x2EB1_0118;
const UNDECODABLE_WITNESS_SEED: u64 = 0x2EB1_0119;
const INCONCLUSIVE_NOTICE_SEED: u64 = 0x2EB1_011A;
const SEED_WHILE_VERIFYING_SEED: u64 = 0x2EB1_011D;
const WITNESS_RESEED_SEED: u64 = 0x2EB1_011E;
const NEWCOMER_RETRIES_SEED: u64 = 0x2EB1_011F;
const POSITIONED_PEER_SEED: u64 = 0x2EB1_0120;
const REFUSAL_PIN_SEED: u64 = 0x2EB1_0121;

/// p ships to f until the two converge — so p durably records `f:1` as a
/// witness — then p's host crashes with no drain (a real failover). f is left
/// running and untouched; the caller decides what f becomes before p is
/// restarted with its ORIGINAL config.
fn ship_then_crash_the_primary(sim: &Sim, p: &mut Node, f: &mut Node) {
    for i in 0..10u64 {
        commit_widget(sim, p, i).unwrap();
        drive_rounds(sim, &mut [&mut *p, &mut *f], 3).unwrap();
    }
    run_until(sim, &mut [&mut *p, &mut *f], converged, CONVERGE_BUDGET).unwrap();
    assert_eq!(
        load_witnesses(&p.host.disk()).unwrap(),
        vec!["f:1".to_string()],
        "sanity: p must have durably recorded f as a witness before it dies"
    );
    p.host.crash_now();
}

/// Promotes `f` through the real control verb, exactly as `charpente promote`
/// does — never by writing its role file.
fn promote(sim: &Sim, f: &mut Node) {
    f.flipped = true;
    let mut cli = CtrlConn::open(sim, "f:1", &Msg::CtrlPromote { secret: None }).unwrap();
    let reply = drive_ctrl(sim, &mut [&mut *f], &mut cli, CONVERGE_BUDGET).unwrap();
    assert!(
        matches!(reply, Msg::CtrlOk { .. }),
        "the promote must succeed: {reply:?}"
    );
}

/// An effect-free transaction: reads are served throughout, whatever the gate
/// says (spec §2 — this is what "serves reads and withholds writes" means).
fn read_widget_count(node: &mut Node) -> usize {
    node.db
        .as_mut()
        .expect("node is live")
        .transact(|tx| Ok::<usize, io::Error>(tx.scan::<Widget>()?.len()))
        .expect("an effect-free transaction is never gated")
}

/// One effectful transaction, returning whatever the gate had to say.
fn try_write(node: &mut Node, i: u64) -> Result<(), TxError<io::Error>> {
    node.db.as_mut().expect("node is live").transact(|tx| {
        tx.put(&Widget {
            id: Id::from_raw(i),
            kind: 0,
            weight: i as i64,
        });
        Ok::<(), io::Error>(())
    })
}

/// THE HEADLINE (spec §6): a shipped-to follower is promoted, the old primary
/// restarts with its original config — and must not reach `allow_writes`.
/// Asserted on the GATE, never on a message: a notice an operator may or may
/// not read is not what keeps the second primary from acking writes.
#[test]
fn a_resurrected_primary_does_not_accept_writes_until_its_witness_answers() {
    let sim = Sim::with_faults(RESURRECTION_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    ship_then_crash_the_primary(&sim, &mut p, &mut f);
    promote(&sim, &mut f);

    // The old box comes back EXACTLY as it was configured: same --role
    // primary, same data dir. Its durable role file says Primary and the
    // durable role always wins, so nothing in its config hints at the truth.
    recover_node(&sim, &mut p).unwrap();
    assert!(
        matches!(p.repl.role(), Role::Primary),
        "the resurrected node must still be a durable Primary — that is the whole gap"
    );

    assert!(
        !p.db.as_ref().unwrap().writes_allowed(),
        "a durable primary that resumes with a non-empty witness set must NOT accept writes \
         before its witnesses answer"
    );
    assert_eq!(
        p.repl.verifying_witnesses(),
        1,
        "it must be probing the one follower it had shipped to"
    );
    assert_eq!(
        read_widget_count(&mut p),
        10,
        "reads are served throughout (spec §2's ruled option (c))"
    );
    match try_write(&mut p, 100) {
        Err(TxError::Verifying { unreached }) => assert_eq!(
            unreached,
            vec!["f:1".to_string()],
            "the refusal must name the witness it is waiting on"
        ),
        other => panic!("expected a Verifying rejection, got {other:?}"),
    }

    // Driving it against the PROMOTED witness never opens the gate. (The
    // refusal it eventually raises is pinned by name in the next test; here
    // the assertion is only that `allow_writes` is never reached.)
    let _ = run_until(&sim, &mut [&mut p, &mut f], |_| false, 100_000);
    assert!(
        !p.db.as_ref().unwrap().writes_allowed(),
        "a witness that answers AS A PRIMARY must never open this node's gate"
    );
}

/// The refusal, byte-exact (hard rule 5). Follows `decide_hello`'s
/// `diverged_behind` shape: name both nodes and both remedies, and never
/// demote automatically — this node cannot know which timeline the operator
/// wants to keep, and its acked writes are not the platform's to discard.
#[test]
fn a_resurrected_primary_refuses_by_name_when_its_witness_answers_as_a_primary() {
    let sim = Sim::with_faults(RESURRECTION_REFUSAL_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    ship_then_crash_the_primary(&sim, &mut p, &mut f);
    promote(&sim, &mut f);
    recover_node(&sim, &mut p).unwrap();

    let err = run_until(&sim, &mut [&mut p, &mut f], |_| false, 100_000)
        .expect_err("a witness answering as a primary must refuse, not resume");
    let message = err
        .strip_prefix("p step: ")
        .unwrap_or_else(|| panic!("expected p's own step to refuse, got: {err}"));
    assert_eq!(
        message,
        "witness f:1 answers as a primary itself (witness answered Heartbeat — only a node past \
         the role match (primary, or handing off) ever sends one): this node restarted as a \
         durable primary, but a follower it had shipped to has been promoted or handed off to \
         while it was down, so this node is not the primary any more. It has NOT demoted itself \
         — it cannot know which timeline you want to keep, and the writes it acked are not the \
         platform's to discard. If f:1 holds the timeline to keep, re-seed this node from a \
         fresh data dir (stop it, delete its data dir, and restart it configured as a follower \
         of f:1 so it snapshot-seeds from it, never with a primary config: the durable role \
         file that outranks config is deleted with the data dir, so a cleared node started as \
         a primary comes up as a second writable primary); if instead this node's records are \
         the ones to keep, stop f:1, then clear this node's witness set (GUIDE §10) and \
         restart it."
    );

    // The probe must not make its target record it (the probe Hellos with no
    // listen address of its own, deliberately): a witness that agreed to ship
    // would otherwise durably record the prober and gate ITS OWN future
    // restarts on a node that exists only to ask a question.
    assert_eq!(
        load_witnesses(&f.host.disk()).unwrap(),
        Vec::<String>::new(),
        "the questioned node must not record the prober as a witness"
    );
    assert!(
        !p.db.as_ref().unwrap().writes_allowed(),
        "the refused node never accepts writes"
    );
}

/// The single-node regression (spec §2): "empty witness set → `allow_writes()`
/// immediately, silently. Single-node primaries and never-shipped primaries
/// are completely untouched. This is the common case and it must stay free."
/// An absence-asserting guard — it passes on unfixed code by construction, so
/// its teeth are the mutation proof recorded in the task report, not a RED run.
#[test]
fn an_empty_witness_set_resumes_immediately_and_silently() {
    let sim = Sim::with_faults(EMPTY_WITNESS_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();

    assert!(
        p.db.as_ref().unwrap().writes_allowed(),
        "a primary that has never shipped to anyone resumes writable, straight out of open"
    );
    assert_eq!(
        p.repl.verifying_witnesses(),
        0,
        "no probe is started when there is nothing to verify"
    );
    assert!(
        p.repl.take_witness_notice().is_none(),
        "and nothing is said about it"
    );
    try_write(&mut p, 1).expect("writes flow immediately");

    // Still silent across a restart — the empty set is durable, not a
    // first-boot accident.
    recover_node(&sim, &mut p).unwrap();
    assert!(p.db.as_ref().unwrap().writes_allowed(), "still writable");
    assert_eq!(p.repl.verifying_witnesses(), 0, "still no probe");
    assert_eq!(
        load_witnesses(&p.host.disk()).unwrap(),
        Vec::<String>::new(),
        "sanity: the set really is empty (both slots established, no addresses)"
    );
    try_write(&mut p, 2).expect("writes still flow");
    assert!(p.repl.take_witness_notice().is_none(), "still nothing said");
}

/// The ruled option (c) in full (spec §2): with a witness that is simply gone,
/// this node serves reads, refuses writes by name, says so ONCE, and keeps
/// probing — it never resumes on its own and never gives up.
#[test]
fn an_unreachable_witness_serves_reads_and_withholds_writes() {
    let sim = Sim::with_faults(UNREACHABLE_WITNESS_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    ship_then_crash_the_primary(&sim, &mut p, &mut f);
    // And now the witness is gone for good — not promoted, not reachable.
    f.host.crash_now();
    recover_node(&sim, &mut p).unwrap();
    assert!(!p.db.as_ref().unwrap().writes_allowed(), "gated at open");

    // Nothing is said before the deadline: a witness that answers in 200ms
    // must not produce an operator notice at all.
    let start = sim.now().as_nanos();
    run_until(
        &sim,
        &mut [&mut p],
        |_| sim.now().as_nanos() >= start + WITNESS_DEADLINE_NANOS / 2,
        100_000,
    )
    .unwrap();
    assert!(
        p.repl.take_witness_notice().is_none(),
        "the notice must not fire before the deadline"
    );

    run_until(
        &sim,
        &mut [&mut p],
        |_| sim.now().as_nanos() > start + WITNESS_DEADLINE_NANOS,
        100_000,
    )
    .unwrap();

    assert_eq!(
        read_widget_count(&mut p),
        10,
        "reads are served throughout — that is the whole point of option (c)"
    );
    match try_write(&mut p, 100) {
        Err(TxError::Verifying { unreached }) => assert_eq!(unreached, vec!["f:1".to_string()]),
        other => panic!("expected a Verifying rejection, got {other:?}"),
    }
    let notice = p
        .repl
        .take_witness_notice()
        .expect("the deadline must produce one loud operator notice");
    assert_eq!(
        notice,
        "witness verification has not resolved after 30s: this node was a primary \
         before restarting and still cannot confirm that every follower it had shipped to was \
         never promoted, so it is serving reads and refusing writes — writes resume \
         automatically the moment every witness answers. Unresolved: f:1 (no answer yet). If a \
         witness is gone for good, see GUIDE §10's witness-set remedy."
    );

    // Exactly once: driving through several more deadlines' worth of virtual
    // time produces no second notice.
    let then = sim.now().as_nanos();
    run_until(
        &sim,
        &mut [&mut p],
        |_| sim.now().as_nanos() > then + 3 * WITNESS_DEADLINE_NANOS,
        1_000_000,
    )
    .unwrap();
    assert!(
        p.repl.take_witness_notice().is_none(),
        "the notice fires exactly once, not once per deadline"
    );
    assert!(
        !p.db.as_ref().unwrap().writes_allowed(),
        "and the gate never opens on its own"
    );
    assert_eq!(
        p.repl.verifying_witnesses(),
        1,
        "it keeps probing — the node resolves itself the moment the witness returns"
    );
}

/// Stands a raw listener on a departed witness's address and hands the probe
/// one chosen reply, then vanishes.
///
/// The vanishing is load-bearing, not cleanup. A probe that has answered
/// inconclusively reconnects, and a reconnect that *succeeds* leaves an open
/// conn nobody will ever answer — which reports no work and no deadline, so
/// the scheduler stalls instead of advancing virtual time to the notice
/// deadline. With the listener gone the reconnects fail and the backoff
/// drives the clock, exactly as an unreachable witness does.
fn answer_the_probe_once(sim: &Sim, p: &mut Node, reply: &[u8]) {
    let listener = sim.host("f").net().listen("f:1").expect("listen on f:1");
    p.repl.pump_io(sim.now().as_nanos());
    step_node(sim, p).expect("a freshly recovered probe's first step is connect_and_hello");
    let conn = listener
        .accept()
        .expect("accept")
        .expect("the probe must have dialed its witness");
    let mut buf = vec![0u8; MAX_MESSAGE_BYTES];
    let n = conn
        .recv(&mut buf)
        .expect("recv")
        .expect("the probe's Hello must have arrived (zero simulated net latency)");
    assert!(
        matches!(decode(&buf[..n]).unwrap(), Msg::Hello { .. }),
        "a probe opens with a Hello"
    );
    conn.send(reply).expect("send the chosen reply");
    // Move the reply off the wire before the socket goes away.
    p.repl.pump_io(sim.now().as_nanos());
    drop(conn);
    drop(listener);
}

/// Hard rule 5, and spec §2's trichotomy: a witness that answers with bytes
/// this node cannot decode has told it **nothing**, and "nothing" files with
/// unreachable — stay write-stopped, back off, ask again.
///
/// Before the fix, `WitnessProbe::step` took a bare `?` on `decode`, so an
/// undecodable reply became an unnamed `io::Error` that propagated through
/// `Repl::step` and out of `Kernel::run`: anything holding a witness's
/// address — a half-open port, a wrong-protocol service, a corrupted reply —
/// could kill a resurrected primary with eight bytes of garbage, and the
/// operator got no name for the contract, the site, or the fix.
#[test]
fn a_witness_that_answers_with_undecodable_bytes_does_not_kill_the_node() {
    let sim = Sim::with_faults(UNDECODABLE_WITNESS_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    ship_then_crash_the_primary(&sim, &mut p, &mut f);
    f.host.crash_now();
    recover_node(&sim, &mut p).unwrap();
    assert!(!p.db.as_ref().unwrap().writes_allowed(), "gated at open");

    answer_the_probe_once(&sim, &mut p, &[0xff; 8]);

    let start = sim.now().as_nanos();
    run_until(
        &sim,
        &mut [&mut p],
        |_| sim.now().as_nanos() > start + 4 * RECONNECT_NANOS,
        100_000,
    )
    .expect("an undecodable witness answer must not kill this node");

    assert!(
        !p.db.as_ref().unwrap().writes_allowed(),
        "and the gate stays shut — an answer this node cannot read is not permission to write"
    );
    assert_eq!(
        p.repl.verifying_witnesses(),
        1,
        "it keeps asking: undecodable is retryable, not decided"
    );

    // And the operator is told which witness and why (hard rule 5): the
    // reason reaches them through the notice, which is the whole basis for
    // routing this to a verdict instead of an error.
    run_until(
        &sim,
        &mut [&mut p],
        |_| sim.now().as_nanos() > start + WITNESS_DEADLINE_NANOS,
        1_000_000,
    )
    .unwrap();
    let notice = p
        .repl
        .take_witness_notice()
        .expect("the deadline must produce one loud operator notice");
    assert_eq!(
        notice,
        "witness verification has not resolved after 30s: this node was a primary \
         before restarting and still cannot confirm that every follower it had shipped to was \
         never promoted, so it is serving reads and refusing writes — writes resume \
         automatically the moment every witness answers. Unresolved: f:1 (inconclusive: \
         \"undecodable response: repl message decode: unknown tag 255 — this peer speaks a \
         newer protocol version; upgrade this binary or downgrade the peer\"). If a witness is \
         gone for good, see GUIDE §10's witness-set remedy."
    );
}

/// Hard rule 5 says error output is pinned by tests. The notice's
/// `(no answer yet)` variant was pinned byte-exact from the start; its
/// `(inconclusive: …)` sibling — the one that carries the *reason* a witness
/// is unresolved, and so the one an operator actually acts on — was not
/// pinned anywhere. This pins it, including the `{:?}` quoting that bounding
/// the peer-controlled message applies.
#[test]
fn the_deadline_notice_names_an_inconclusive_witness_and_its_reason() {
    let sim = Sim::with_faults(INCONCLUSIVE_NOTICE_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    ship_then_crash_the_primary(&sim, &mut p, &mut f);
    f.host.crash_now();
    recover_node(&sim, &mut p).unwrap();

    // A refusal carrying neither marker lands on `classify`'s default arm,
    // the one absorbing every prefix refusal (secret, version, schema,
    // listen) and any added later.
    answer_the_probe_once(
        &sim,
        &mut p,
        &encode(&Msg::Refuse {
            message: "schema digest mismatch".to_string(),
        }),
    );

    let start = sim.now().as_nanos();
    run_until(
        &sim,
        &mut [&mut p],
        |_| sim.now().as_nanos() > start + WITNESS_DEADLINE_NANOS,
        1_000_000,
    )
    .unwrap();

    let notice = p
        .repl
        .take_witness_notice()
        .expect("the deadline must produce one loud operator notice");
    assert_eq!(
        notice,
        "witness verification has not resolved after 30s: this node was a primary \
         before restarting and still cannot confirm that every follower it had shipped to was \
         never promoted, so it is serving reads and refusing writes — writes resume \
         automatically the moment every witness answers. Unresolved: f:1 (inconclusive: \
         \"schema digest mismatch\"). If a witness is gone for good, see GUIDE §10's \
         witness-set remedy."
    );
}

/// M2 interlude, spec §1: a gated node must not SEED a brand-new follower.
/// `begin_seeding` is read-only so the write gate does not stop it, but a
/// seed manufactures a durable converged replica of a timeline this node has
/// not confirmed it owns — and that replica OUTLIVES this node's own
/// fail-stop when a witness finally answers "primary".
///
/// Rigged so `log_base() == 0`, which makes `decide_hello` answer
/// `Frames { from: 0 }` — a full replay that is functionally a seed. A
/// refusal keyed on the `Decision::Snapshot` VARIANT is green here and seeds
/// anyway; keying on `position.is_none()` inside the None-position arm is
/// what makes both sub-arms impossible to miss (spec §2).
#[test]
fn a_verifying_node_refuses_to_seed_a_brand_new_follower() {
    let sim = Sim::with_faults(SEED_WHILE_VERIFYING_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    ship_then_crash_the_primary(&sim, &mut p, &mut f);
    f.host.crash_now(); // the witness is gone, so the gate cannot resolve
    recover_node(&sim, &mut p).unwrap();
    assert!(
        p.db.as_ref().unwrap().verifying(),
        "precondition: p restarted gated on its recorded witness"
    );
    assert_eq!(
        p.db.as_ref().unwrap().log_base(),
        0,
        "precondition: nothing GC'd, so a None-position peer takes the Frames{{from:0}} arm"
    );

    // A brand-new node, holding nothing, asks to join.
    let mut g = spawn_node(&sim, "g", follower_cfg("g:1", "p:1"), DbConfig::default()).unwrap();
    drive_rounds(&sim, &mut [&mut p, &mut g], 200).unwrap();

    // Assert on the GATE OUTCOME, not on an error shape: the newcomer holds
    // nothing. (The parked attempt asserted a shape the correct fix removes,
    // and took 771s to reach its budget wall.) NOT `total_seeds(&g) == 0`:
    // this rig never checkpoints (see the doc comment above), so a SUCCESSFUL
    // seed would ALSO take the `Frames { from: 0 }` arm and leave
    // `total_seeds` at 0 — that assertion cannot fail either way and would
    // silently pass if the refusal were deleted. `last_applied` is the
    // discriminating check.
    assert_eq!(
        g.db.as_ref().unwrap().last_applied(),
        None,
        "the newcomer must hold nothing — a Frames{{from:0}} replay would have filled it"
    );
    // The bound this buys: a refused newcomer never enters the witness set,
    // because `listen` is assigned only on the two shipping arms.
    assert_eq!(
        load_witnesses(&p.host.disk()).unwrap(),
        vec!["f:1".to_string()],
        "a refused newcomer must not be recorded as a witness"
    );
    // Retryability (task 2) itself is proven by the `.unwrap()` on
    // `drive_rounds` above surviving: a fatal follower error propagates out
    // of `step_node` through `drive_rounds`'s `?` and would have panicked
    // there. This is a sanity check that g is still the live node that
    // `.unwrap()` implies, not a second proof of retryability.
    assert!(g.db.is_some(), "sanity: g is still present");
}

/// The other half of the keying (spec §2): a RECORDED WITNESS whose data dir
/// is empty (GUIDE §10's fresh-data-dir re-seed remedy — not a
/// discard-and-rebuild, which replays the WAL and leaves `last_applied`
/// intact) Hellos with `position: None` — indistinguishable from a newcomer
/// on position alone. It must still be seeded: that is what resolves this
/// node's own gate. This is the case the parked attempt broke.
///
/// Construction: `store_witnesses` hand-writes `w:1` into p's witness set
/// (the same technique `a_promote_clears_a_stale_witness_set_before_the_
/// durable_role_flip` below uses on its own node before `recover_node`),
/// then a THIRD node `w` — never before seen, empty data dir — spawns at
/// that address. `w` stands in for that recorded witness with an empty data
/// dir: p cannot tell the two apart from `position: None` alone, which is
/// exactly the case this test is for.
#[test]
fn a_verifying_node_still_seeds_a_recorded_witness() {
    let sim = Sim::with_faults(WITNESS_RESEED_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    ship_then_crash_the_primary(&sim, &mut p, &mut f);
    // `ship_then_crash_the_primary` leaves p's disk crashed (erroring on every
    // op); `restart()` un-crashes it before this direct write — under
    // `FaultConfig::none()` restart's crash-collapse model is a no-op on
    // already-durable bytes, and `recover_node` below restarts again
    // (idempotent) before reopening.
    p.host.restart();
    // Record a SECOND witness that has never held data, then bring p back.
    store_witnesses(&p.host.disk(), &["f:1".to_string(), "w:1".to_string()]).unwrap();
    recover_node(&sim, &mut p).unwrap();
    assert!(p.db.as_ref().unwrap().verifying(), "precondition: gated");

    // w is a recorded witness with an empty data dir: position None, and it
    // must be seeded anyway.
    let mut w = spawn_node(&sim, "w", follower_cfg("w:1", "p:1"), DbConfig::default()).unwrap();
    run_until(&sim, &mut [&mut p, &mut w], converged, CONVERGE_BUDGET)
        .expect("a recorded witness must still be seeded while the gate is unresolved");
    // NOT `total_seeds(&w) >= 1`: `ship_then_crash_the_primary` never
    // checkpoints, so `log_base()` stays 0 and w's None-position Hello takes
    // the `Frames { from: 0 }` arm (a full replay), not `Decision::Snapshot`
    // — `total_seeds` would stay 0 even on success. `converged` above already
    // proves `last_applied` equality; this proves it actually holds p's data
    // rather than a trivial both-None equality.
    assert_eq!(
        read_widget_count(&mut w),
        10,
        "it must actually hold p's data, not merely be idle-equal"
    );
    // The test's whole claim is "seeded WHILE the gate is unresolved" — f is
    // left running by `ship_then_crash_the_primary` but is never included in
    // a driven node list again in this test, so its own probe-answering
    // logic never runs and p's gate can never open; this must still be true,
    // or the seed above proves nothing about the timing this test is named
    // for.
    assert!(
        p.db.as_ref().unwrap().verifying(),
        "the gate must still be shut — proving the seed happened WHILE verifying"
    );
}

/// Task 2's retryability, end to end: the refused newcomer does not die, and
/// converges once the gate opens. `recover_node` is correct here — f comes
/// back on its OWN data dir as the positioned follower it always was, which
/// is what answers the probe and opens the gate.
#[test]
fn a_refused_newcomer_retries_and_converges_once_the_gate_opens() {
    let sim = Sim::with_faults(NEWCOMER_RETRIES_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    ship_then_crash_the_primary(&sim, &mut p, &mut f);
    recover_node(&sim, &mut p).unwrap();

    let mut g = spawn_node(&sim, "g", follower_cfg("g:1", "p:1"), DbConfig::default()).unwrap();
    drive_rounds(&sim, &mut [&mut p, &mut g], 200).unwrap();
    // NOT `total_seeds(&g) == 0` (review I-1, same defect as M-2 above): this
    // rig never checkpoints, so a SUCCESSFUL seed would ALSO take the
    // `Frames { from: 0 }` arm and leave `total_seeds` at 0 — the assertion
    // cannot fail either way. `last_applied` is the discriminating check.
    assert_eq!(
        g.db.as_ref().unwrap().last_applied(),
        None,
        "refused while the gate is shut"
    );

    // f answers the probe, opening the gate. `converged` is the wrong
    // predicate here: p and f are ALREADY at the same `last_applied` right
    // after `ship_then_crash_the_primary` + `recover_node` (a crash under
    // `FaultConfig::none()` loses no durable data), so it would be true
    // before a single round drives the probe to f and back.
    run_until(
        &sim,
        &mut [&mut p, &mut f],
        |ns| ns[0].db.as_ref().is_some_and(|d| d.writes_allowed()),
        CONVERGE_BUDGET,
    )
    .expect("the witness's probe answer must open the gate");
    assert!(
        p.db.as_ref().unwrap().writes_allowed(),
        "the gate must have opened once the witness answered"
    );
    run_until(&sim, &mut [&mut p, &mut g], converged, CONVERGE_BUDGET)
        .expect("the newcomer must converge once the gate is open");
    // NOT `total_seeds(&g) >= 1`: `ship_then_crash_the_primary` never
    // checkpoints, so `log_base()` stays 0 and the retry's None-position
    // Hello takes the `Frames { from: 0 }` arm (a full replay), not
    // `Decision::Snapshot` — `total_seeds` would stay 0 even on success.
    // `converged` above already proves `last_applied` equality; this proves
    // it is not a trivial both-None equality (p has real commits).
    assert_eq!(
        read_widget_count(&mut g),
        10,
        "and must actually hold p's data on the retry, not merely be idle-equal"
    );
}

/// Negative space (hard rule 4): a POSITIONED peer is unaffected while
/// verifying. Catch-up ships a timeline the peer already holds a prefix of;
/// only a seed creates a new copy. This catches a MEMBERSHIP-AGNOSTIC
/// widening of the refusal — one that drops or bypasses the witness-set
/// check entirely and refuses every Hello outright while verifying — under
/// which every other test here would stay green. It does NOT catch a
/// membership-PRESERVING widening that merely hoists the same check above
/// the position match: `h` is itself a recorded witness (below), so that
/// narrower mistake would still ship to it.
///
/// Two followers on purpose. With one, that follower's own probe answer opens
/// the gate, so the catch-up would ship AFTER verification and prove nothing.
/// Here `f` is permanently gone (gate can never resolve) while `h` is behind
/// and positioned — so any frame `h` receives was shipped while verifying.
/// Does NOT reuse `ship_then_crash_the_primary`, which hard-asserts a single
/// witness.
#[test]
fn a_verifying_node_still_ships_catch_up_to_a_positioned_peer() {
    let sim = Sim::with_faults(POSITIONED_PEER_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    let mut h = spawn_node(&sim, "h", follower_cfg("h:1", "p:1"), DbConfig::default()).unwrap();

    // h converges on the first five records, then stops being driven.
    for i in 0..5u64 {
        commit_widget(&sim, &mut p, i).unwrap();
        drive_rounds(&sim, &mut [&mut p, &mut f, &mut h], 3).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut h], converged, CONVERGE_BUDGET).unwrap();
    let h_before = h.db.as_ref().unwrap().last_applied();
    // Sever h's session before the next commits, not merely stop driving h:
    // p keeps servicing h's OLD session (round-robin ships to every live
    // session regardless of which nodes THIS TEST happens to drive), so the
    // "five more" commits below would otherwise queue their frames on the
    // wire to a peer nobody is draining — h would then absorb them as
    // pre-crash backlog on its first later poll, catching up WITHOUT ever
    // negotiating a fresh Hello, and this test would prove nothing about
    // `decide_hello`. `recover_node` tears h's connection AND reopens it
    // (h's own data survives intact — no faults, no data loss) so the catch
    // up below can only happen through a real reconnect.
    recover_node(&sim, &mut h).unwrap();

    // Five more that only f sees, so h is genuinely behind.
    for i in 5..10u64 {
        commit_widget(&sim, &mut p, i).unwrap();
        drive_rounds(&sim, &mut [&mut p, &mut f], 3).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_eq!(
        load_witnesses(&p.host.disk()).unwrap(),
        vec!["f:1".to_string(), "h:1".to_string()],
        "sanity: both followers are recorded witnesses"
    );
    p.host.crash_now();
    f.host.crash_now(); // permanently gone: the gate can never resolve
    recover_node(&sim, &mut p).unwrap();
    assert!(
        p.db.as_ref().unwrap().verifying(),
        "precondition: gated on both f:1 and h:1 — f:1 is the one that never resolves"
    );

    run_until(&sim, &mut [&mut p, &mut h], converged, CONVERGE_BUDGET)
        .expect("a positioned peer must still catch up while the gate is unresolved");
    assert!(
        h.db.as_ref().unwrap().last_applied() > h_before,
        "h must have actually advanced, not merely been already-converged"
    );
    assert!(
        p.db.as_ref().unwrap().verifying(),
        "and the gate must still be shut — proving the ship happened WHILE verifying"
    );
}

/// Hard rule 5: the refusal an operator actually reads, pinned byte-exact.
/// The property tests above assert on the gate, which is the right oracle for
/// the guarantee but would stay green through any reword of this string.
#[test]
fn the_seed_while_verifying_refusal_is_pinned() {
    let sim = Sim::with_faults(REFUSAL_PIN_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    ship_then_crash_the_primary(&sim, &mut p, &mut f);
    f.host.crash_now();
    recover_node(&sim, &mut p).unwrap();
    assert!(p.db.as_ref().unwrap().verifying(), "precondition: gated");

    let schema_digest = p.db.as_ref().unwrap().schema_digest();
    let conn = sim
        .host("g")
        .net()
        .connect("p:1")
        .expect("connect to the primary's listener");
    conn.send(&encode(&Msg::Hello {
        proto: PROTO_VERSION,
        listen: Some("g:1".to_string()),
        position: None,
        secret: None,
        schema_digest,
    }))
    .unwrap();

    let mut buf = vec![0u8; MAX_MESSAGE_BYTES];
    let mut reply = None;
    for _ in 0..64 {
        drive_rounds(&sim, &mut [&mut p], 1).unwrap();
        match conn.recv(&mut buf) {
            Ok(Some(n)) => {
                reply = Some(decode(&buf[..n]).unwrap());
                break;
            }
            Ok(None) => {}
            Err(e) => panic!("conn recv failed: {e}"),
        }
    }
    let Some(Msg::Refuse { message }) = reply else {
        panic!("expected a Refuse reply, got {reply:?}");
    };
    assert_eq!(
        message,
        "cannot seed a new follower yet: this node restarted as a primary and is still \
         verifying its witnesses — it has not confirmed that none of the followers it had \
         shipped to was promoted while it was down, so seeding would copy a timeline it does \
         not yet know it owns. This clears on its own once every witness answers, and an \
         existing follower's catch-up is unaffected meanwhile. If the witnesses are \
         permanently gone, see GUIDE §10 — note that clearing the witness file re-opens the \
         dual-primary gap it exists to close.",
        "the refusal's exact shape is product surface (hard rule 5)"
    );
}

/// `ReplConfig::strict_witnesses` (task 6, spec §2): the opt-in for operators
/// who would rather a node fail loudly than serve reads it cannot write to.
///
/// The shape is deliberately NOT the spec's "refusal to start at all", and the
/// difference is operator-visible: whether a witness is unreachable is
/// unknowable at open — only probing establishes it — so a strict node starts,
/// serves reads for the deadline window, and only then stops, by returning an
/// error out of `Repl::step` that bubbles through `Kernel::run` (DESIGN §5's
/// restart-and-recover exit).
#[test]
fn strict_witnesses_stops_the_node_instead_of_serving_reads_it_cannot_write_to() {
    let sim = Sim::with_faults(STRICT_WITNESS_SEED, FaultConfig::none());
    let mut cfg = primary_cfg("p:1");
    cfg.strict_witnesses = true;
    let mut p = spawn_node(&sim, "p", cfg, DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    ship_then_crash_the_primary(&sim, &mut p, &mut f);
    f.host.crash_now();
    recover_node(&sim, &mut p).unwrap();
    assert!(!p.db.as_ref().unwrap().writes_allowed(), "gated at open");

    // Before the deadline a strict node is indistinguishable from a default
    // one: it serves reads and says nothing. Strictness is a choice about what
    // to do when verification TIMES OUT, not about refusing to run.
    let start = sim.now().as_nanos();
    run_until(
        &sim,
        &mut [&mut p],
        |_| sim.now().as_nanos() >= start + WITNESS_DEADLINE_NANOS / 2,
        100_000,
    )
    .expect("a strict node runs normally until the deadline");
    assert_eq!(
        read_widget_count(&mut p),
        10,
        "reads are served right up to the deadline"
    );

    let err = run_until(
        &sim,
        &mut [&mut p],
        |_| sim.now().as_nanos() > start + WITNESS_DEADLINE_NANOS,
        100_000,
    )
    .expect_err("past the deadline a strict node must stop, not settle into read-only service");
    assert_eq!(
        err,
        "p step: witness verification did not resolve within 30s and \
         ReplConfig.strict_witnesses is set, so this node is stopping instead of serving reads \
         it cannot write to: it was a primary before restarting and still cannot confirm that \
         every follower it had shipped to was never promoted. Unresolved: f:1 (no answer yet). \
         Start the unreached witnesses and restart this node; clear strict_witnesses to serve \
         reads while it waits instead; or, if a witness is gone for good, see GUIDE §10's \
         witness-set remedy."
    );
    // The gate never opened on the way out — stopping is not a backdoor to
    // writability.
    assert!(
        !p.db.as_ref().unwrap().writes_allowed(),
        "a strict stop must never leave the node writable"
    );
}

/// The sibling above only ever sees the deadline-miss error through
/// `run_until`'s `String` (`step_node` formats it as `"p step: {e}"`), which
/// throws the `io::Error` away before a test could ask `Refusal::of` about
/// it. Payload-shapes spec §5 requires this stop to be told apart from a
/// platform IO failure the same way every other named stop already is — a
/// restart cannot resolve a still-unreached witness any faster, only
/// starting the witness (or clearing `strict_witnesses`) can (the deadline
/// message's own remedy) — so this drives the identical scenario through
/// `run_until_io_error` instead, which keeps the raw error.
#[test]
fn a_strict_witness_deadlines_own_step_error_is_a_refusal() {
    let sim = Sim::with_faults(0x2EB1_0122, FaultConfig::none());
    let mut cfg = primary_cfg("p:1");
    cfg.strict_witnesses = true;
    let mut p = spawn_node(&sim, "p", cfg, DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    ship_then_crash_the_primary(&sim, &mut p, &mut f);
    f.host.crash_now();
    recover_node(&sim, &mut p).unwrap();
    assert!(!p.db.as_ref().unwrap().writes_allowed(), "gated at open");

    // Unlike the sibling test, there is no need to stop halfway to check
    // read service — that behaviour is already pinned there. `sim`'s clock is
    // virtual: the scheduler's quiescent branch sleeps straight to the next
    // deadline, so reaching WITNESS_DEADLINE_NANOS costs a handful of loop
    // iterations, not real time.
    let err = run_until_io_error(&sim, &mut [&mut p], 100_000)
        .expect_err("past the deadline a strict node must stop with an error");
    assert!(
        err.to_string()
            .contains("witness verification did not resolve within 30s"),
        "must be the same deadline refusal as the sibling test: {err}"
    );
    assert!(
        charpente_core::db::Refusal::of(&err).is_some(),
        "a strict witness deadline's own step error must be a Refusal — a restart cannot \
         resolve a still-unreached witness any faster, only starting the witness (or clearing \
         strict_witnesses) can: {err}"
    );
}

/// O-4's other half: `Allowed -> Verifying` is unconstrained by construction
/// (`Db` has no boot/running distinction), so the invariant that the gate is
/// only ever set at boot lives at the call site — and the one operator verb
/// that could otherwise move a verifying node's gate underneath the probes
/// (`switchover`, whose abort arm calls `allow_writes` on a node that never
/// verified) is refused by name instead.
#[test]
fn a_switchover_is_refused_while_the_witness_gate_is_still_verifying() {
    let sim = Sim::with_faults(SWITCHOVER_WHILE_VERIFYING_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    ship_then_crash_the_primary(&sim, &mut p, &mut f);
    f.host.crash_now(); // the witness is unreachable: p stays verifying
    recover_node(&sim, &mut p).unwrap();
    assert!(!p.db.as_ref().unwrap().writes_allowed(), "gated at open");

    let mut cli = CtrlConn::open(
        &sim,
        "p:1",
        &Msg::CtrlSwitchover {
            to: "f:1".to_string(),
            secret: None,
        },
    )
    .unwrap();
    let reply = drive_ctrl(&sim, &mut [&mut p], &mut cli, 100_000).unwrap();
    match reply {
        Msg::CtrlErr { message } => assert_eq!(
            message,
            "cannot switch over: this node is still verifying its witnesses (f:1) — it has not \
             accepted a write since it restarted, so there is nothing to drain; wait for \
             verification to finish, or promote the target directly"
        ),
        other => panic!("expected a named refusal, got {other:?}"),
    }
    assert!(
        matches!(p.repl.role(), Role::Primary),
        "the refused switchover must not have flipped the durable role"
    );
    assert!(
        !p.db.as_ref().unwrap().writes_allowed(),
        "and must not have moved the gate"
    );
}

/// O-5 (Noah ruling 2, task 1b): a torn `clear_witnesses` on a demote path is
/// never re-attempted, so the authoritative clear belongs on the PROMOTE path
/// — and BEFORE the durable role flip, so an interrupted promote is merely
/// incomplete (still a follower, stale set inert, `charpente promote` retries)
/// rather than a primary carrying a stale set it will never clear.
#[test]
fn a_promote_clears_a_stale_witness_set_before_the_durable_role_flip() {
    let sim = Sim::with_faults(PROMOTE_CLEARS_WITNESSES_SEED, FaultConfig::none());
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();

    // The state a torn clear leaves behind by the next boot: a durable
    // FOLLOWER carrying a stale witness set. (`open_witnesses` repairs a torn
    // slot with the resolved — stale, non-empty — set, so there is nothing
    // left in the bytes to detect; this is that end state, staged directly.)
    store_witnesses(&f.host.disk(), &["ghost:1".to_string()]).unwrap();
    recover_node(&sim, &mut f).unwrap();
    assert_eq!(
        load_witnesses(&f.host.disk()).unwrap(),
        vec!["ghost:1".to_string()],
        "sanity: the stale set survives into the follower's next boot"
    );
    assert!(
        !f.db.as_ref().unwrap().writes_allowed(),
        "sanity: a follower is write-stopped for its own reason (NotPrimary), not the gate"
    );
    assert_eq!(
        f.repl.verifying_witnesses(),
        0,
        "sanity: a follower does not probe — the gate is a durable-Primary state"
    );

    promote(&sim, &mut f);

    assert_eq!(
        load_witnesses(&f.host.disk()).unwrap(),
        Vec::<String>::new(),
        "the promote must clear the stale set — otherwise this node withholds writes forever \
         after its next restart, waiting on a witness from a timeline it has left"
    );
    assert!(
        f.db.as_ref().unwrap().writes_allowed(),
        "the promoted node accepts writes"
    );
    // And the proof that the clear is not merely cosmetic: the next restart
    // resumes silently rather than gating on the ghost.
    recover_node(&sim, &mut f).unwrap();
    assert!(
        f.db.as_ref().unwrap().writes_allowed(),
        "a promoted node's next restart must not gate on a witness it has cleared"
    );
    assert_eq!(f.repl.verifying_witnesses(), 0, "and must not probe it");
}

/// O-2: the startup work a gated node SKIPS must run when the gate opens.
/// `Kernel::run`'s schema gate and `Kernel::build`'s `cron.upsert` are both
/// guarded on the write gate and neither is retried, and `after_repl_step`'s
/// re-run fires on a follower->primary ROLE flip that never happens to a node
/// whose role was `Primary` throughout — so without a re-run path a
/// witness-gated node would silently skip its schema migration forever and
/// panic on its first cron tick ("cron invariant: schedule has no row").
///
/// Also the end-to-end 503 (task 3's surface) and the liveness the 503 body
/// promises: "writes resume automatically once that node answers".
#[test]
fn the_kernel_completes_its_skipped_startup_work_when_the_witness_gate_opens() {
    let sim = Sim::with_faults(GATE_OPENS_KERNEL_SEED, FaultConfig::none());
    let horizon = 100 * 1_000_000_000;
    let ph = sim.host("p");

    // p's data dir as a v1 primary that has shipped to w:1 — and on which no
    // kernel has ever run, which is what makes the `cron.upsert` half of this
    // test discriminating (a first kernel life would have written the row).
    {
        let mut db = Db::open(
            Box::new(ph.disk()),
            DbConfig::default(),
            kernel_full_schema_v1(),
        )
        .unwrap();
        db.transact(|tx| {
            tx.put(&WidgetV1 {
                id: Id::from_raw(1),
                count: 7,
            });
            Ok::<(), io::Error>(())
        })
        .unwrap();
        db.checkpoint().unwrap();
    }
    store_witnesses(&ph.disk(), &["w:1".to_string()]).unwrap();

    // The resurrected node, mounted at v2 — its witness is not up yet.
    let mut p = spawn_kernel_node_with_schema(
        &sim,
        "p",
        primary_cfg("p:1"),
        DbConfig::default(),
        horizon,
        true,
        kernel_schema_v2(),
    )
    .unwrap();
    assert!(
        !p.kernel.as_ref().unwrap().app().db().writes_allowed(),
        "the kernel must open gated"
    );

    // O-1: the kernel CONSTRUCTED at all (a gated `cron.upsert` would have
    // `?`-propagated out of `Kernel::build`, silently implementing
    // strict-refuse), and it serves a read.
    let (status, body) = drive_request(
        &sim,
        &mut [&mut p],
        0,
        Request::new(Method::Get, "/widget/1"),
        CONVERGE_BUDGET,
    )
    .unwrap();
    assert_eq!(status, 404, "the gated node serves reads: {body:?}");

    // A write gets task 3's 503, byte-exact, end to end.
    let (status, body) = drive_request(
        &sim,
        &mut [&mut p],
        0,
        Request::new(Method::Post, "/widget/9"),
        CONVERGE_BUDGET,
    )
    .unwrap();
    assert_eq!(status, 503, "the gated node refuses writes");
    let decoded: serde_json::Value = serde_json::from_slice(&body).expect("503 body is JSON");
    assert_eq!(
        decoded["error"],
        "this node is not accepting writes yet: it was a primary before restarting and has not \
         confirmed that its follower w:1 was never promoted — reads are being served normally, \
         and writes resume automatically once that node answers; if it is gone for good, see \
         GUIDE §10's witness-set remedy"
    );

    // Nothing that needs a write has run.
    {
        let snap = p.kernel.as_ref().unwrap().app().db().snapshot().unwrap();
        assert!(
            CronRow::by_name(&snap, &"tick".to_string())
                .unwrap()
                .is_empty(),
            "a gated node must not have written its cron rows"
        );
        let version: Option<SchemaVersion> = snap.get(schema_version_id("WidgetV2")).unwrap();
        assert_eq!(
            version.map(|v| v.version),
            Some(1),
            "and must not have migrated its schema"
        );
    }

    // The witness comes back — as the follower it always was.
    let mut w = spawn_kernel_node_with_schema(
        &sim,
        "w",
        follower_cfg("w:1", "p:1"),
        DbConfig::default(),
        horizon,
        true,
        kernel_schema_v2(),
    )
    .unwrap();
    krun_until(
        &sim,
        &mut [&mut p, &mut w],
        |ns| {
            ns[0]
                .kernel
                .as_ref()
                .is_some_and(|k| k.app().db().writes_allowed())
        },
        CONVERGE_BUDGET,
    )
    .unwrap();

    // The gate opened, and everything the gated node skipped has now run.
    {
        let db = p.kernel.as_ref().unwrap().app().db();
        let snap = db.snapshot().unwrap();
        let version: Option<SchemaVersion> = snap.get(schema_version_id("WidgetV2")).unwrap();
        assert_eq!(
            version.map(|v| v.version),
            Some(2),
            "migrate_schema must run once the gate opens"
        );
        assert_eq!(
            count_index_snapshot(db, 7),
            vec![(1, 7, "v2".to_string())],
            "and must actually have rewritten the row (index rebuilt too)"
        );
        assert!(
            !CronRow::by_name(&snap, &"tick".to_string())
                .unwrap()
                .is_empty(),
            "cron.upsert must run once the gate opens"
        );
    }

    // Liveness, as the 503 body promises: writes resume automatically.
    let (status, _) = drive_request(
        &sim,
        &mut [&mut p, &mut w],
        0,
        Request::new(Method::Post, "/widget/9"),
        CONVERGE_BUDGET,
    )
    .unwrap();
    assert_eq!(status, 200, "writes resume once the witness answers");

    // And the cron rows are real: the node ticks without tripping the "cron
    // invariant: schedule has no row" panic a missed upsert would cause.
    krun_until(
        &sim,
        &mut [&mut p, &mut w],
        |ns| {
            ns[0]
                .kernel
                .as_ref()
                .is_some_and(|k| k.stats().cron_ticks >= 2)
        },
        CONVERGE_BUDGET,
    )
    .unwrap();
}

// ============================================================ job shapes
//
// A Pending job is only ever decoded by a binary that compiled its shape
// (payload-shapes spec §4, amendment A1): a promote over a foreign one is
// refused before its durable flip, and every other way the write gate opens
// stops the kernel by name.

/// A different shape for `Ping` ("ping"): what a follower running a binary
/// whose `Ping` gained a field would register.
const PING_V2_FINGERPRINT: u64 = 0x7E57_0106;

fn ping_refusal(ids: &str, compiled: u64) -> String {
    format!(
        "1 Pending \"ping\" job(s) (ids {ids}) were enqueued under a different shape of ping \
         (recorded {:#x}; this binary {compiled:#x}) — this binary will not decode them; run \
         the binary that enqueued them until they are Done or Dead, then deploy this one, or \
         restore whichever of ping's fields, its `global`/`scoped_by` declaration, its \
         `outbound` declaration or its tenant type's `NAME` changed, and give the new shape its \
         own #[job] type under a new name (GUIDE §6)",
        <Ping as Job>::FINGERPRINT
    )
}

fn enqueue_ping(node: &mut Node) {
    node.db
        .as_mut()
        .unwrap()
        .transact(|tx| {
            tx.enqueue(Ping { n: 7 });
            Ok::<(), io::Error>(())
        })
        .unwrap();
}

fn promote_f(sim: &Sim, f: &mut Node) -> Msg {
    f.flipped = true;
    CtrlConn::open(sim, "f:1", &Msg::CtrlPromote { secret: None })
        .and_then(|mut cli| drive_ctrl(sim, &mut [f], &mut cli, CONVERGE_BUDGET))
        .unwrap()
}

/// `serve_replicated` prints `Repl::open`'s error as its stop line, so a
/// replication listen address that cannot be bound names itself and the fix.
#[test]
fn a_replication_listen_address_already_bound_is_named_with_its_fix() {
    let sim = Sim::with_faults(0x5A9E_0709, FaultConfig::none());
    let h = sim.host("p");
    let _held = h.net().listen("p:1").unwrap();
    let mut db = Db::open(Box::new(h.disk()), DbConfig::default(), make_schema()).unwrap();
    let err = match Repl::open(
        Box::new(h.net()),
        Box::new(h.disk()),
        primary_cfg("p:1"),
        &mut db,
    ) {
        Ok(_) => panic!("Repl::open listened on an address already bound"),
        Err(e) => e,
    };
    assert_eq!(err.kind(), io::ErrorKind::AddrInUse);
    assert_eq!(
        err.to_string(),
        "could not bind the replication listen address p:1: \"p:1\" already has a simulated \
         listener — free that address, or configure this node to listen on another"
    );
}

/// Spec §4 at promote, amendment A1: refused BY NAME before the durable role
/// flips, so the node stays a follower. Promoting first and stopping after
/// would leave a durable primary that refuses to start on every restart.
#[test]
fn a_promote_over_foreign_pending_jobs_is_refused_and_the_node_stays_a_follower() {
    let sim = Sim::with_faults(0x5A9E_0701, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    f.db.as_mut()
        .unwrap()
        .register_job_shapes(&[("ping", PING_V2_FINGERPRINT, None, None)]);
    enqueue_ping(&mut p);
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    p.host.crash_now();
    match promote_f(&sim, &mut f) {
        Msg::CtrlErr { message } => assert_eq!(
            message,
            format!("cannot promote: {}", ping_refusal("0", PING_V2_FINGERPRINT))
        ),
        other => panic!("expected CtrlErr, got {other:?}"),
    }
    assert!(
        matches!(f.repl.role(), Role::Follower { .. }),
        "still a follower in memory"
    );
    assert!(!f.db.as_ref().unwrap().writes_allowed(), "and not writable");
    let (durable, _) = charpente::repl::role::load_role(&f.host.disk(), &Role::Primary).unwrap();
    assert!(
        matches!(durable, Role::Follower { .. }),
        "and still a follower on disk"
    );
}

/// The control: the same promote with a matching shape succeeds.
#[test]
fn a_promote_over_same_shape_pending_jobs_succeeds() {
    let sim = Sim::with_faults(0x5A9E_0702, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    f.db.as_mut()
        .unwrap()
        .register_job_shapes(&[("ping", <Ping as Job>::FINGERPRINT, None, None)]);
    enqueue_ping(&mut p);
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    p.host.crash_now();
    assert!(matches!(promote_f(&sim, &mut f), Msg::CtrlOk { .. }));
}

/// Done rows are never decoded again, so they never refuse anything.
#[test]
fn a_promote_over_foreign_done_jobs_succeeds() {
    promote_over_a_foreign_finished_ping(0x5A9E_0703, JobStatus::Done);
}

/// Nor do Dead ones: a dead-lettered job is never dispatched again.
#[test]
fn a_promote_over_foreign_dead_jobs_succeeds() {
    promote_over_a_foreign_finished_ping(0x5A9E_0707, JobStatus::Dead);
}

fn promote_over_a_foreign_finished_ping(seed: u64, status: JobStatus) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    f.db.as_mut()
        .unwrap()
        .register_job_shapes(&[("ping", PING_V2_FINGERPRINT, None, None)]);
    enqueue_ping(&mut p);
    // Job rows are the platform's: the operator writes the finished state
    // with the `TenantAdmission` its `Group` mints, the dispatcher's own path.
    let mut group = p.db.as_mut().unwrap().begin_group().unwrap();
    let admission = group.tenant_admission();
    group
        .stage(|tx| {
            let mut job = tx
                .get_job(Id::from_raw(0), &admission)?
                .expect("the enqueued ping");
            job.status = status;
            tx.settle_job(&job, &admission);
            Ok::<(), io::Error>(())
        })
        .unwrap();
    group.commit().unwrap();
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    p.host.crash_now();
    assert!(matches!(promote_f(&sim, &mut f), Msg::CtrlOk { .. }));
}

/// Review focus 4, amendment A4: a snapshot re-seed rebuilds the follower's
/// `Db` from its schema, so the job-shape registry must live IN the schema. A
/// registry kept anywhere else comes back empty and this promote goes through.
#[test]
fn a_promote_after_a_snapshot_reseed_still_refuses_foreign_pending_jobs() {
    let sim = Sim::with_faults(0x5A9E_0704, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), seeding_db_cfg()).unwrap();
    enqueue_ping(&mut p);
    for i in 0..60u64 {
        p.db.as_mut()
            .unwrap()
            .transact(|tx| {
                tx.put(&Widget {
                    id: Id::from_raw(i),
                    kind: i % 3,
                    weight: i as i64,
                });
                Ok::<(), io::Error>(())
            })
            .unwrap();
    }
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), seeding_db_cfg()).unwrap();
    f.db.as_mut()
        .unwrap()
        .register_job_shapes(&[("ping", PING_V2_FINGERPRINT, None, None)]);
    run_until(
        &sim,
        &mut [&mut p, &mut f],
        |ns| ns[1].repl.stats().seeds >= 1 && converged(ns),
        CONVERGE_BUDGET,
    )
    .unwrap();
    assert!(
        total_seeds(&f) >= 1,
        "the precondition: f was snapshot-seeded"
    );
    p.host.crash_now();
    match promote_f(&sim, &mut f) {
        Msg::CtrlErr { message } => assert_eq!(
            message,
            format!("cannot promote: {}", ping_refusal("0", PING_V2_FINGERPRINT))
        ),
        other => panic!("the registry did not survive the re-seed: {other:?}"),
    }
}

/// Review focus 3, amendment A1: a switchover onto a follower holding a
/// Pending job of a foreign shape — the ordinary rolling deploy (upgrade the
/// follower, switch over, upgrade the old primary). HandedOff is DESIGN §5's
/// point of no return, so the target does NOT refuse the hand-off: a refusal
/// there leaves the primary write-stopped and the target a follower, with no
/// named way out. The switchover completes, and the new primary names the jobs
/// — its kernel refuses at the write-gate flip (the next test), and that
/// refusal's fix works from exactly this state.
#[test]
fn a_switchover_onto_foreign_pending_jobs_completes_and_the_new_primary_names_them() {
    let sim = Sim::with_faults(0x5A9E_0705, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    f.db.as_mut()
        .unwrap()
        .register_job_shapes(&[("ping", PING_V2_FINGERPRINT, None, None)]);
    enqueue_ping(&mut p);
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    p.flipped = true;
    f.flipped = true;
    let mut cli = CtrlConn::open(
        &sim,
        "p:1",
        &Msg::CtrlSwitchover {
            to: "f:1".to_string(),
            secret: None,
        },
    )
    .unwrap();
    let reply = drive_ctrl(&sim, &mut [&mut p, &mut f], &mut cli, CONVERGE_BUDGET).unwrap();
    assert!(
        matches!(reply, Msg::CtrlOk { .. }),
        "the switchover completes — it is never stranded mid-hand-off: {reply:?}"
    );
    assert!(matches!(f.repl.role(), Role::Primary), "f is the primary");
    assert!(
        matches!(p.repl.role(), Role::Follower { .. }),
        "p follows it"
    );
    assert_eq!(
        f.db.as_ref().unwrap().pending_job_shape_refusal().unwrap(),
        Some(ping_refusal("0", PING_V2_FINGERPRINT)),
        "and the new primary names the foreign jobs — the refusal its kernel stops on"
    );
}

/// Spec §4 at the write gate's opening, through the kernel (amendment A1): a
/// primary restarted with a witness opens gated, holding a Pending job of a
/// foreign shape. When the witness answers, the gate opens, and the kernel
/// refuses by name in `after_repl_step` before it dispatches, migrates or
/// writes anything. A completed switchover's target reaches this same check,
/// through `accept_handoff`'s `allow_writes`.
#[test]
fn a_write_gate_opening_over_foreign_pending_jobs_stops_the_kernel_by_name() {
    let sim = Sim::with_faults(0x5A9E_0706, FaultConfig::none());
    let horizon = 100 * 1_000_000_000;
    let ph = sim.host("p");
    {
        let mut db = Db::open(Box::new(ph.disk()), DbConfig::default(), full_schema()).unwrap();
        db.transact(|tx| {
            tx.enqueue(Ping { n: 7 });
            Ok::<(), io::Error>(())
        })
        .unwrap();
        db.checkpoint().unwrap();
    }
    store_witnesses(&ph.disk(), &["w:1".to_string()]).unwrap();
    let mut p = spawn_kernel_node(
        &sim,
        "p",
        primary_cfg("p:1"),
        DbConfig::default(),
        horizon,
        true,
    )
    .unwrap();
    // This binary's `ping` has another shape. `kernel_schema`'s kernels mount
    // `Bump` only, so registering `ping` by hand collides with nothing.
    p.kernel
        .as_mut()
        .unwrap()
        .app_mut_for_tests()
        .db_for_tests()
        .register_job_shapes(&[("ping", PING_V2_FINGERPRINT, None, None)]);
    assert!(
        !p.kernel.as_ref().unwrap().app().db().writes_allowed(),
        "the precondition: the kernel opens gated, so Kernel::build's own check did not run"
    );
    let mut w = spawn_kernel_node(
        &sim,
        "w",
        follower_cfg("w:1", "p:1"),
        DbConfig::default(),
        horizon,
        true,
    )
    .unwrap();
    let err = krun_until(&sim, &mut [&mut p, &mut w], |_| false, CONVERGE_BUDGET).unwrap_err();
    assert_eq!(
        err,
        format!("p kernel step: {}", ping_refusal("0", PING_V2_FINGERPRINT))
    );
    let jobs = p
        .kernel
        .as_ref()
        .unwrap()
        .app()
        .db()
        .snapshot()
        .unwrap()
        .scan::<JobRow>()
        .unwrap();
    assert_eq!(
        (jobs.len(), jobs[0].status, jobs[0].attempts),
        (1, JobStatus::Pending, 0),
        "the foreign job was never dispatched"
    );
}

/// A switchover between two kernels onto a target holding a Pending job of a
/// foreign shape (amendment A1): the hand-off completes and the operator gets
/// `CtrlOk`, and the target's kernel stops by name the moment its write gate
/// opens — a `Refusal`, before it dispatches anything. The old primary
/// follows it.
#[test]
fn a_kernel_switchover_onto_foreign_pending_jobs_stops_the_target_by_name() {
    let sim = Sim::with_faults(0x5A9E_070A, FaultConfig::none());
    let horizon = 100 * 1_000_000_000;
    {
        // Due far past the horizon, so the primary never dispatches it: the
        // switchover must find it Pending.
        let mut db = Db::open(
            Box::new(sim.host("p").disk()),
            DbConfig::default(),
            full_schema(),
        )
        .unwrap();
        db.transact(|tx| {
            tx.enqueue(Ping { n: 7 });
            Ok::<(), io::Error>(())
        })
        .unwrap();
        let mut group = db.begin_group().unwrap();
        let admission = group.tenant_admission();
        group
            .stage(|tx| {
                let mut job = tx
                    .get_job(Id::from_raw(0), &admission)?
                    .expect("the enqueued ping");
                job.due_at = charpente_core::io::WallTime::from_nanos(u64::MAX / 2);
                tx.settle_job(&job, &admission);
                Ok::<(), io::Error>(())
            })
            .unwrap();
        group.commit().unwrap();
        db.checkpoint().unwrap();
    }
    let mut p = spawn_kernel_node(
        &sim,
        "p",
        primary_cfg("p:1"),
        DbConfig::default(),
        horizon,
        true,
    )
    .unwrap();
    let mut f = spawn_kernel_node(
        &sim,
        "f",
        follower_cfg("f:1", "p:1"),
        DbConfig::default(),
        horizon,
        true,
    )
    .unwrap();
    // This binary's `ping` has another shape. `kernel_schema`'s kernels mount
    // `Bump` only, so registering `ping` by hand collides with nothing.
    f.kernel
        .as_mut()
        .unwrap()
        .app_mut_for_tests()
        .db_for_tests()
        .register_job_shapes(&[("ping", PING_V2_FINGERPRINT, None, None)]);
    krun_until(&sim, &mut [&mut p, &mut f], kconverged, CONVERGE_BUDGET).unwrap();

    let mut cli = CtrlConn::open(
        &sim,
        "p:1",
        &Msg::CtrlSwitchover {
            to: "f:1".to_string(),
            secret: None,
        },
    )
    .unwrap();
    let mut refused: Option<io::Error> = None;
    let mut reply = None;
    for _ in 0..CONVERGE_BUDGET {
        let mut worked = false;
        let mut deadlines = Vec::new();
        for (node, is_target) in [(&mut p, false), (&mut f, true)] {
            if is_target && refused.is_some() {
                continue;
            }
            match node.kernel.as_mut().unwrap().step(&mut node.source) {
                Ok(Step::Worked) => worked = true,
                Ok(Step::Idle { deadline }) => deadlines.extend(deadline.map(|d| d.as_nanos())),
                Err(e) if is_target => refused = Some(e),
                Err(e) => panic!("the old primary's kernel stopped: {e}"),
            }
        }
        if let Some(msg) = cli.poll().unwrap() {
            reply = Some(msg);
            break;
        }
        if !worked {
            let now = sim.now().as_nanos();
            let next = deadlines
                .into_iter()
                .chain(sim.next_wakeup())
                .filter(|&d| d > now)
                .min()
                .expect("quiescent before the switchover's reply");
            sim.clock().sleep(Duration::from_nanos(next - now));
        }
    }
    assert!(
        matches!(reply, Some(Msg::CtrlOk { .. })),
        "the switchover completes — it is never stranded mid-hand-off: {reply:?}"
    );
    let err = refused.expect("the target's kernel stopped when its write gate opened");
    assert!(
        charpente_core::db::Refusal::of(&err).is_some(),
        "the stop is a Refusal: {err}"
    );
    assert_eq!(err.to_string(), ping_refusal("0", PING_V2_FINGERPRINT));
    let target = f.kernel.as_ref().unwrap();
    assert!(target.is_primary(), "the target is the primary");
    assert!(
        !p.kernel.as_ref().unwrap().is_primary(),
        "the old primary follows it"
    );
    let jobs = target
        .app()
        .db()
        .snapshot()
        .unwrap()
        .scan::<JobRow>()
        .unwrap();
    assert_eq!(
        (jobs.len(), jobs[0].status, jobs[0].attempts),
        (1, JobStatus::Pending, 0),
        "the foreign job was never dispatched"
    );
}

/// Same scenario as the test above, through `krun_until_io_error` instead of
/// `krun_until` — the one assertion that needs the raw error, not its text:
/// the write-gate-opening refusal (`after_repl_step`, kernel/mod.rs) is a
/// `Refusal`, not an ordinary platform IO failure a restart could clear.
#[test]
fn a_write_gate_opening_over_foreign_pending_jobs_is_a_refusal() {
    let sim = Sim::with_faults(0x5A9E_0708, FaultConfig::none());
    let horizon = 100 * 1_000_000_000;
    let ph = sim.host("p");
    {
        let mut db = Db::open(Box::new(ph.disk()), DbConfig::default(), full_schema()).unwrap();
        db.transact(|tx| {
            tx.enqueue(Ping { n: 7 });
            Ok::<(), io::Error>(())
        })
        .unwrap();
        db.checkpoint().unwrap();
    }
    store_witnesses(&ph.disk(), &["w:1".to_string()]).unwrap();
    let mut p = spawn_kernel_node(
        &sim,
        "p",
        primary_cfg("p:1"),
        DbConfig::default(),
        horizon,
        true,
    )
    .unwrap();
    p.kernel
        .as_mut()
        .unwrap()
        .app_mut_for_tests()
        .db_for_tests()
        .register_job_shapes(&[("ping", PING_V2_FINGERPRINT, None, None)]);
    let mut w = spawn_kernel_node(
        &sim,
        "w",
        follower_cfg("w:1", "p:1"),
        DbConfig::default(),
        horizon,
        true,
    )
    .unwrap();
    let err =
        krun_until_io_error(&sim, &mut [&mut p, &mut w], |_| false, CONVERGE_BUDGET).unwrap_err();
    assert!(
        charpente_core::db::Refusal::of(&err).is_some(),
        "the write-gate-opening refusal is a Refusal: {err}"
    );
    assert!(
        err.to_string()
            .contains(&ping_refusal("0", PING_V2_FINGERPRINT)),
        "the raw error's own text must still be the named foreign-shape refusal, not just any \
         Refusal: {err}"
    );
}

// ------------------------------- a torn role file, restarted as its text says

/// A role disk whose `role` file fails its next `sync` while armed: the
/// failed flip a torn role file comes from. The flip's write is in the page
/// cache, the flip returns `Err` and the node stops, and a crash can then tear
/// what it wrote.
struct FailRoleSync {
    inner: charpente_sim::SimDisk,
    arm: Arc<std::sync::atomic::AtomicBool>,
}

struct FailRoleSyncFile {
    inner: Box<dyn File>,
    arm: Option<Arc<std::sync::atomic::AtomicBool>>,
}

impl Disk for FailRoleSync {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        Ok(Box::new(FailRoleSyncFile {
            inner: self.inner.open(path)?,
            arm: (path == "role").then(|| Arc::clone(&self.arm)),
        }))
    }
    fn remove(&self, path: &str) -> io::Result<()> {
        self.inner.remove(path)
    }
    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.inner.list(prefix)
    }
    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        self.inner.sync_dir(prefix)
    }
}

impl File for FailRoleSyncFile {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read_at(offset, out)
    }
    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write_at(offset, data)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync(&self) -> io::Result<()> {
        if self
            .arm
            .as_ref()
            .is_some_and(|a| a.swap(false, Ordering::SeqCst))
        {
            return Err(io::Error::other("injected fsync failure"));
        }
        self.inner.sync()
    }
}

/// A node on a fresh host whose role disk fails a `role` sync when `arm` is set.
fn node_failing_role_syncs(
    sim: &Sim,
    name: &str,
    cfg: ReplConfig,
    arm: &Arc<std::sync::atomic::AtomicBool>,
) -> Node {
    let h = sim.host(name);
    let mut db = Db::open(Box::new(h.disk()), DbConfig::default(), make_schema()).unwrap();
    let role_disk = FailRoleSync {
        inner: h.disk(),
        arm: Arc::clone(arm),
    };
    let (repl, _) =
        Repl::open(Box::new(h.net()), Box::new(role_disk), cfg.clone(), &mut db).unwrap();
    Node {
        host: h,
        db: Some(db),
        repl,
        clock: sim.clock(),
        name: name.to_string(),
        repl_cfg: cfg,
        db_cfg: DbConfig::default(),
        schema: make_schema(),
        recoveries: 0,
        carried_seeds: 0,
        flipped: true,
    }
}

/// `scheduler_round` for a fault-free world, except that a node whose step
/// fails stops (its process died) and the round names it, instead of being
/// recovered: a failed flip is the state under test.
fn round_stopping_on_error(sim: &Sim, nodes: &mut [&mut Node]) -> Option<String> {
    let now = sim.now().as_nanos();
    for node in nodes.iter_mut().filter(|n| n.db.is_some()) {
        node.repl.pump_io(now);
    }
    let mut worked = false;
    for node in nodes.iter_mut() {
        let Some(db) = node.db.as_mut() else { continue };
        if !node.repl.has_work(db, now) {
            continue;
        }
        worked = true;
        if let Err(e) = node.repl.step(db, &node.clock) {
            node.db = None;
            return Some(format!("{}: {e}", node.name));
        }
        if node.repl.db_needs_reseed() {
            let db = node.db.take().expect("db present before reseed");
            node.db = Some(node.repl.run_seed(db, &node.clock).unwrap());
        }
    }
    if !worked {
        let mut next = sim.next_wakeup();
        for node in nodes.iter().filter(|n| n.db.is_some()) {
            if let Some(d) = node.repl.next_deadline(now)
                && d > now
            {
                next = Some(next.map_or(d, |x| x.min(d)));
            }
        }
        if let Some(t) = next.filter(|&t| t > now) {
            sim.clock().sleep(Duration::from_nanos(t - now));
        }
    }
    None
}

/// Up to `k` rounds; the first node that stops, named.
fn rounds_stopping_on_error(sim: &Sim, nodes: &mut [&mut Node], k: u32) -> Option<String> {
    (0..k).find_map(|_| round_stopping_on_error(sim, nodes))
}

/// Rounds until `done`, asserting no node stops on the way.
fn rounds_until(sim: &Sim, nodes: &mut [&mut Node], done: impl Fn(&[&mut Node]) -> bool) {
    for _ in 0..20_000 {
        if done(nodes) {
            return;
        }
        if let Some(e) = round_stopping_on_error(sim, nodes) {
            panic!("a node stopped before the state under test: {e}");
        }
    }
    panic!("the state under test was never reached");
}

/// A crash tears `node`'s unsynced write of `line`: the line's prefix lands
/// and the CRC line does not. Then the host power-cycles.
fn tear_role_write(node: &mut Node, line: &str) {
    node.db = None;
    let disk = node.host.disk();
    let file = disk.open("role").unwrap();
    let torn = format!("{}\n00000000", &line[..line.len() - 1]);
    file.write_at(0, torn.as_bytes()).unwrap();
    file.set_len(torn.len() as u64).unwrap();
    file.sync().unwrap();
    disk.sync_dir("role").unwrap();
    node.host.restart();
}

/// Boots `node` as configured, which must meet the torn-role text, then does
/// what the text says for a node a flip tore: delete the role file and
/// restart it configured as `restart_as`.
fn restart_torn_node_as(sim: &Sim, node: &mut Node, restart_as: ReplConfig) {
    let mut db = Db::open(
        Box::new(node.host.disk()),
        node.db_cfg.clone(),
        node.schema.clone(),
    )
    .unwrap();
    let boot = Repl::open(
        Box::new(node.host.net()),
        Box::new(node.host.disk()),
        node.repl_cfg.clone(),
        &mut db,
    );
    let err = boot
        .err()
        .map(|e| e.to_string())
        .expect("a node whose role file tore must not boot");
    assert!(
        err.contains("role file failed its integrity check")
            && err.contains("never as primary beside one"),
        "{}: {err}",
        node.name
    );
    drop(db);
    node.host.disk().remove("role").unwrap();
    node.host.disk().sync_dir("role").unwrap();
    node.repl_cfg = restart_as;
    reopen(sim, node).unwrap();
}

fn widget_count(node: &Node) -> usize {
    let db = node.db.as_ref().expect("a live node");
    db.snapshot()
        .unwrap()
        .scan_raw(Widget::TABLE, false)
        .unwrap()
        .len()
}

fn writable(nodes: &[&Node]) -> Vec<String> {
    nodes
        .iter()
        .filter(|n| n.db.as_ref().is_some_and(|d| d.writes_allowed()))
        .map(|n| n.name.clone())
        .collect()
}

/// p a primary at `p:1` and f its follower at `f:1`, ten widgets each.
fn torn_role_pair(
    sim: &Sim,
    p_arm: &Arc<std::sync::atomic::AtomicBool>,
    f_arm: &Arc<std::sync::atomic::AtomicBool>,
) -> (Node, Node) {
    let mut p = node_failing_role_syncs(sim, "p", primary_cfg("p:1"), p_arm);
    let mut f = node_failing_role_syncs(sim, "f", follower_cfg("f:1", "p:1"), f_arm);
    for i in 0..10u64 {
        commit_widget(sim, &mut p, i).unwrap();
        assert_eq!(
            rounds_stopping_on_error(sim, &mut [&mut p, &mut f], 3),
            None
        );
    }
    rounds_until(sim, &mut [&mut p, &mut f], converged);
    assert_eq!(widget_count(&f), 10);
    (p, f)
}

fn arm() -> Arc<std::sync::atomic::AtomicBool> {
    Arc::new(std::sync::atomic::AtomicBool::new(false))
}

/// S3b R3-M1, the torn-role text's data-keeping exit, followed literally for
/// every flip that writes the role file after first boot. A tear means the
/// flip's write failed and a crash tore it, so each arm fails that flip's
/// sync, tears what it wrote, and boots the node as configured, which meets
/// the text. Then it deletes the role file and restarts the node as the text
/// says: a follower of the writable primary if another node is serving as
/// one, primary otherwise. In every arm the node comes back in that role with
/// its ten widgets, and exactly one node accepts writes.
///
/// Two readings the text does not give are pinned failing beside them. The
/// role the flip was writing (S5 re-review 3's proposal) is right for five
/// flips but not for a switchover's first, to handing-off: the node then
/// stays write-stopped, since that switchover handed nothing off, and no node
/// accepts writes. And primary beside another writable primary, after a
/// promote that tore when the operator promoted a different follower instead,
/// is a second writable primary no witness gate sees: the promote cleared the
/// node's witness set before its flip.
#[test]
fn a_torn_role_file_restarted_as_its_text_says_keeps_its_data_and_one_writable_primary() {
    // Promote (Follower -> Primary). The old primary is dead, so no node is
    // serving as primary: restart as primary. The old primary, back with its
    // own config, is refused by its witness gate.
    let sim = Sim::with_faults(0x53B0_0001, FaultConfig::none());
    let (pa, fa) = (arm(), arm());
    let (mut p, mut f) = torn_role_pair(&sim, &pa, &fa);
    p.db = None;
    p.host.restart();
    fa.store(true, Ordering::SeqCst);
    let _cli = CtrlConn::open(&sim, "f:1", &Msg::CtrlPromote { secret: None }).unwrap();
    let stopped = rounds_stopping_on_error(&sim, &mut [&mut f], 10_000).unwrap();
    assert!(stopped.contains("(to \"primary\")"), "{stopped}");
    tear_role_write(&mut f, "primary");
    restart_torn_node_as(&sim, &mut f, primary_cfg("f:1"));
    reopen(&sim, &mut p).unwrap();
    let refused = rounds_stopping_on_error(&sim, &mut [&mut p, &mut f], 50_000).unwrap();
    assert!(
        refused.starts_with("p: witness f:1 answers as a primary itself"),
        "{refused}"
    );
    assert_eq!(*f.repl.role(), Role::Primary);
    assert_eq!(widget_count(&f), 10);
    assert_eq!(writable(&[&p, &f]), ["f"]);

    // A switchover's first flip (Primary -> HandingOff). The follower never
    // got a hand-off, so no node is serving as primary: restart as primary.
    // Restarted as the role that flip was writing, it never resumes.
    for (as_written, restart_as) in [
        (false, primary_cfg("p:1")),
        (
            true,
            ReplConfig {
                initial_role: Role::HandingOff {
                    to: "f:1".to_string(),
                },
                ..primary_cfg("p:1")
            },
        ),
    ] {
        let sim = Sim::with_faults(0x53B0_0002, FaultConfig::none());
        let (pa, fa) = (arm(), arm());
        let (mut p, mut f) = torn_role_pair(&sim, &pa, &fa);
        pa.store(true, Ordering::SeqCst);
        let to = "f:1".to_string();
        let _cli = CtrlConn::open(&sim, "p:1", &Msg::CtrlSwitchover { to, secret: None }).unwrap();
        let stopped = rounds_stopping_on_error(&sim, &mut [&mut p, &mut f], 10_000).unwrap();
        assert!(stopped.contains("(to \"handing-off:f:1\")"), "{stopped}");
        assert!(writable(&[&p, &f]).is_empty());
        tear_role_write(&mut p, "handing-off:f:1");
        restart_torn_node_as(&sim, &mut p, restart_as);
        assert_eq!(
            rounds_stopping_on_error(&sim, &mut [&mut p, &mut f], 50_000),
            None
        );
        assert_eq!(widget_count(&p), 10);
        if as_written {
            assert!(
                matches!(p.repl.role(), Role::HandingOff { .. }) && writable(&[&p, &f]).is_empty(),
                "restarted as the handing-off its flip was writing, the node stays write-stopped \
                 and no node accepts writes — if this changes, the text's handing-off reading \
                 needs a second look"
            );
        } else {
            assert_eq!(*p.repl.role(), Role::Primary);
            assert_eq!(writable(&[&p, &f]), ["p"]);
        }
    }

    // A switchover's last flip on the old primary (HandingOff -> Follower),
    // after the target took over: restart as the target's follower.
    let sim = Sim::with_faults(0x53B0_0003, FaultConfig::none());
    let (pa, fa) = (arm(), arm());
    let (mut p, mut f) = torn_role_pair(&sim, &pa, &fa);
    let to = "f:1".to_string();
    let _cli = CtrlConn::open(&sim, "p:1", &Msg::CtrlSwitchover { to, secret: None }).unwrap();
    rounds_until(&sim, &mut [&mut p, &mut f], |n| {
        matches!(n[0].repl.role(), Role::HandingOff { .. })
    });
    pa.store(true, Ordering::SeqCst);
    let stopped = rounds_stopping_on_error(&sim, &mut [&mut p, &mut f], 10_000).unwrap();
    assert!(stopped.contains("(to \"follower:f:1\")"), "{stopped}");
    assert_eq!(writable(&[&p, &f]), ["f"]);
    tear_role_write(&mut p, "follower:f:1");
    restart_torn_node_as(&sim, &mut p, follower_cfg("p:1", "f:1"));
    commit_widget(&sim, &mut f, 10).unwrap();
    rounds_until(&sim, &mut [&mut p, &mut f], converged);
    assert_eq!(
        *p.repl.role(),
        Role::Follower {
            primary: "f:1".to_string()
        }
    );
    assert_eq!(widget_count(&p), 11);
    assert_eq!(writable(&[&p, &f]), ["f"]);

    // The same flip at a restart resolving a durable HandingOff against a
    // target that took over (`resolve_to_follower`): restart as its follower.
    let sim = Sim::with_faults(0x53B0_0006, FaultConfig::none());
    let (pa, fa) = (arm(), arm());
    let (mut p, mut f) = torn_role_pair(&sim, &pa, &fa);
    let to = "f:1".to_string();
    let _cli = CtrlConn::open(&sim, "p:1", &Msg::CtrlSwitchover { to, secret: None }).unwrap();
    rounds_until(&sim, &mut [&mut p, &mut f], |n| {
        *n[1].repl.role() == Role::Primary
    });
    p.db = None; // p dies before it hears HandOffDone
    p.host.restart();
    let p_cfg = p.repl_cfg.clone();
    p = node_failing_role_syncs(&sim, "p", p_cfg, &pa);
    assert!(matches!(p.repl.role(), Role::HandingOff { .. }));
    pa.store(true, Ordering::SeqCst);
    let stopped = rounds_stopping_on_error(&sim, &mut [&mut p, &mut f], 50_000).unwrap();
    assert!(stopped.contains("(to \"follower:f:1\")"), "{stopped}");
    tear_role_write(&mut p, "follower:f:1");
    restart_torn_node_as(&sim, &mut p, follower_cfg("p:1", "f:1"));
    commit_widget(&sim, &mut f, 10).unwrap();
    rounds_until(&sim, &mut [&mut p, &mut f], converged);
    assert_eq!(
        *p.repl.role(),
        Role::Follower {
            primary: "f:1".to_string()
        }
    );
    assert_eq!(widget_count(&p), 11);
    assert_eq!(writable(&[&p, &f]), ["f"]);

    // The target's flip (Follower -> Primary). The old primary is write-stopped
    // in HandingOff, so no node is serving as primary: restart as primary. The
    // old primary, restarted as the switchover's stall text says, resolves
    // against it and follows.
    let sim = Sim::with_faults(0x53B0_0004, FaultConfig::none());
    let (pa, fa) = (arm(), arm());
    let (mut p, mut f) = torn_role_pair(&sim, &pa, &fa);
    fa.store(true, Ordering::SeqCst);
    let to = "f:1".to_string();
    let _cli = CtrlConn::open(&sim, "p:1", &Msg::CtrlSwitchover { to, secret: None }).unwrap();
    let stopped = rounds_stopping_on_error(&sim, &mut [&mut p, &mut f], 10_000).unwrap();
    assert!(
        stopped.starts_with("f:") && stopped.contains("(to \"primary\")"),
        "{stopped}"
    );
    assert_eq!(rounds_stopping_on_error(&sim, &mut [&mut p], 20_000), None);
    assert!(matches!(p.repl.role(), Role::HandingOff { .. }) && writable(&[&p]).is_empty());
    tear_role_write(&mut f, "primary");
    restart_torn_node_as(&sim, &mut f, primary_cfg("f:1"));
    p.db = None;
    p.host.restart();
    reopen(&sim, &mut p).unwrap();
    commit_widget(&sim, &mut f, 10).unwrap();
    rounds_until(&sim, &mut [&mut p, &mut f], |n| {
        converged(n) && matches!(n[0].repl.role(), Role::Follower { .. })
    });
    assert_eq!(*f.repl.role(), Role::Primary);
    assert_eq!(
        *p.repl.role(),
        Role::Follower {
            primary: "f:1".to_string()
        }
    );
    assert_eq!(widget_count(&f), 11);
    assert_eq!(writable(&[&p, &f]), ["f"]);

    // An aborted switchover's flip back (HandingOff -> Primary), whose failed
    // write the abort drops: the follower lagged and never took over, so no
    // node is serving as primary — restart as primary.
    let sim = Sim::with_faults(0x53B0_0005, FaultConfig::none());
    let (pa, fa) = (arm(), arm());
    let (mut p, mut f) = torn_role_pair(&sim, &pa, &fa);
    for i in 10..20u64 {
        commit_widget(&sim, &mut p, i).unwrap(); // f, not driven, lags
    }
    let to = "f:1".to_string();
    let _cli = CtrlConn::open(&sim, "p:1", &Msg::CtrlSwitchover { to, secret: None }).unwrap();
    rounds_until(&sim, &mut [&mut p], |n| {
        matches!(n[0].repl.role(), Role::HandingOff { .. })
    });
    pa.store(true, Ordering::SeqCst);
    assert_eq!(rounds_stopping_on_error(&sim, &mut [&mut p], 50_000), None);
    assert!(
        !pa.load(Ordering::SeqCst) && *p.repl.role() == Role::Primary,
        "the abort's role write must have failed, dropped, and left p primary"
    );
    tear_role_write(&mut p, "primary");
    restart_torn_node_as(&sim, &mut p, primary_cfg("p:1"));
    assert_eq!(
        rounds_stopping_on_error(&sim, &mut [&mut p, &mut f], 50_000),
        None
    );
    assert_eq!(*p.repl.role(), Role::Primary);
    assert_eq!(widget_count(&p), 20);
    assert_eq!(writable(&[&p, &f]), ["p"]);

    // A promote that tore while the operator promoted another follower
    // instead: g serves as primary, so restart as its follower. Restarted as
    // primary, the role its flip was writing, it is a second writable primary.
    for as_written in [false, true] {
        let sim = Sim::with_faults(0x53B0_0007, FaultConfig::none());
        let (pa, fa, ga) = (arm(), arm(), arm());
        let (mut p, mut f) = torn_role_pair(&sim, &pa, &fa);
        let mut g = node_failing_role_syncs(&sim, "g", follower_cfg("g:1", "p:1"), &ga);
        rounds_until(&sim, &mut [&mut p, &mut f, &mut g], |n| {
            n[2].db.as_ref().and_then(|d| d.last_applied()) == Some(9)
        });
        p.db = None;
        p.host.restart();
        fa.store(true, Ordering::SeqCst);
        let _cli = CtrlConn::open(&sim, "f:1", &Msg::CtrlPromote { secret: None }).unwrap();
        let stopped = rounds_stopping_on_error(&sim, &mut [&mut f], 10_000).unwrap();
        assert!(stopped.contains("(to \"primary\")"), "{stopped}");
        tear_role_write(&mut f, "primary");
        let mut cli = CtrlConn::open(&sim, "g:1", &Msg::CtrlPromote { secret: None }).unwrap();
        let reply = drive_ctrl(&sim, &mut [&mut g], &mut cli, 10_000).unwrap();
        assert!(matches!(reply, Msg::CtrlOk { .. }), "{reply:?}");
        let restart_as = if as_written {
            primary_cfg("f:1")
        } else {
            follower_cfg("f:1", "g:1")
        };
        restart_torn_node_as(&sim, &mut f, restart_as);
        assert_eq!(
            rounds_stopping_on_error(&sim, &mut [&mut f, &mut g], 50_000),
            None
        );
        assert_eq!(widget_count(&f), 10);
        if as_written {
            assert_eq!(
                writable(&[&f, &g]),
                ["f", "g"],
                "restarted as primary beside another, the node is a second writable primary — \
                 the text's warning, pinned true"
            );
        } else {
            assert_eq!(
                *f.repl.role(),
                Role::Follower {
                    primary: "g:1".to_string()
                }
            );
            assert_eq!(writable(&[&f, &g]), ["g"]);
        }
    }
}

// ------------------------------------------ outbound enqueues ship their key

/// A global outbound job, hand-written so the test mounts nothing: its
/// enqueue mints a key and records tag `0x0c`.
struct ShipCharge {
    n: u64,
}

impl Codec for ShipCharge {
    fn encode(&self, out: &mut Vec<u8>) {
        self.n.encode(out);
    }
    fn decode(input: &mut &[u8]) -> io::Result<ShipCharge> {
        Ok(ShipCharge {
            n: u64::decode(input)?,
        })
    }
}

impl Job for ShipCharge {
    const NAME: &'static str = "ShipCharge";
    const FINGERPRINT: u64 = 0x7E57_0C00;
    type Scope = Unscoped;
    const OUTBOUND: bool = true;
}

/// Its scoped twin: tag `0x0d`.
struct ShipScopedCharge {
    n: u64,
}

impl Codec for ShipScopedCharge {
    fn encode(&self, out: &mut Vec<u8>) {
        self.n.encode(out);
    }
    fn decode(input: &mut &[u8]) -> io::Result<ShipScopedCharge> {
        Ok(ShipScopedCharge {
            n: u64::decode(input)?,
        })
    }
}

impl Job for ShipScopedCharge {
    const NAME: &'static str = "ShipScopedCharge";
    const FINGERPRINT: u64 = 0x7E57_0D00;
    type Scope = SeedOrg;
    const OUTBOUND: bool = true;
}

/// Outbound-effects spec §6: the key is drawn once, on the staging node, and
/// rides in the record. A follower applying a shipped `0x0c` and `0x0d`
/// holds the primary's keys byte for byte, so a failover re-dispatches each
/// job with the key its remote already saw.
#[test]
fn a_follower_applies_shipped_outbound_enqueues_with_the_primary_s_keys() {
    let sim = Sim::with_faults(0x5A9E_0C0D, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    let mint = p.host.entropy();
    let db = p.db.as_mut().unwrap();
    let mut group = db.begin_group_minting(&mint).unwrap();
    let admission = group.tenant_admission();
    group
        .stage(|tx| {
            tx.enqueue(ShipCharge { n: 1 });
            Ok::<(), io::Error>(())
        })
        .unwrap();
    group
        .stage(|tx| {
            tx.admit_scope(Id::<SeedOrg>::from_raw(4), &admission)
                .enqueue(ShipScopedCharge { n: 2 });
            Ok::<(), io::Error>(())
        })
        .unwrap();
    group.commit().map_err(|f| f.error).unwrap();
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();

    let rows = |n: &Node| {
        n.db.as_ref()
            .unwrap()
            .snapshot()
            .unwrap()
            .scan::<JobRow>()
            .unwrap()
    };
    let (pj, fj) = (rows(&p), rows(&f));
    assert_eq!(
        pj.iter()
            .map(|r| (r.name.as_str(), r.scope))
            .collect::<Vec<_>>(),
        vec![("ShipCharge", None), ("ShipScopedCharge", Some(4))],
        "the primary holds one global and one scoped outbound enqueue"
    );
    let keys = |rows: &[JobRow]| {
        rows.iter()
            .map(|r| r.outbound_key.expect("an outbound enqueue carries its key"))
            .collect::<Vec<_>>()
    };
    let (pk, fk) = (keys(&pj), keys(&fj));
    assert_ne!(pk[0], pk[1], "each enqueue drew its own key");
    assert_eq!(
        fk, pk,
        "the follower holds the primary's keys byte for byte"
    );
    let raw = |n: &Node| {
        n.db.as_ref()
            .unwrap()
            .snapshot()
            .unwrap()
            .scan_raw(JobRow::TABLE, false)
            .unwrap()
    };
    assert_eq!(raw(&f), raw(&p), "and the whole job rows, as stored");
}

// ============================================================ clock floor
//
// 0.3.0 spec §5 B at promote: a node never hands out a wall time more than
// `CLOCK_TOLERANCE` behind one its data directory already recorded. A promote
// is refused before the durable role flip (amendment A1's reason: a refused
// promote leaves a follower); a hand-off, the primary's point of no return,
// completes, and the target's kernel stops by name instead.

mod support;
use charpente::http::Ctx;
use charpente_core::io::WallTime;
use support::SteeredClock;

#[route(POST "/clock-key", public)]
fn clock_key(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_api_key(tx, "alice"))
}

#[route(POST "/clock-login", public)]
fn clock_login(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_session(tx, "bob"))
}

/// Where the primary's wall clock stands: 2026-10-22T03:04:05.123Z.
const PRIMARY_WALL: u64 = 1_792_638_245_123_000_000;

const CLOCK_TTL: u64 = 3_600_000_000_000;

/// A replicated kernel on `h` whose wall clock stands at `wall`; virtual
/// time still drives its deadlines.
fn clocked_kernel(sim: &Sim, h: &SimHost, cfg: &ReplConfig, wall: WallTime) -> io::Result<Kernel> {
    let mut app = App::open(
        Box::new(h.disk()),
        Schema::new(),
        Box::new(h.entropy()),
        AppConfig {
            session_ttl: CLOCK_TTL,
            ..AppConfig::default()
        },
    )?
    .mount(routes![clock_key, clock_login]);
    let (repl, _) = Repl::open(
        Box::new(h.net()),
        Box::new(h.disk()),
        cfg.clone(),
        app.db_for_tests(),
    )?;
    let (clock, _) = SteeredClock::new(sim.clock(), wall);
    Kernel::with_replication(app, Box::new(clock), repl)
}

fn clocked_knode(sim: &Sim, host: &str, cfg: ReplConfig, wall: WallTime) -> KNode {
    let h = sim.host(host);
    let kernel = clocked_kernel(sim, &h, &cfg, wall).unwrap();
    let source = SimSource::new(Box::new(sim.clock()), u64::MAX);
    let responses = source.responses();
    KNode {
        host: h,
        kernel: Some(kernel),
        source,
        responses,
        name: host.to_string(),
        repl_cfg: cfg,
        db_cfg: DbConfig::default(),
        schema: Schema::new(),
        recoveries: 0,
        flipped: false,
        recoverable: false,
        corrupted_by_rig: false,
        corruption_at_wipe: 0,
        entry_loss_at_wipe: 0,
    }
}

/// A primary at `PRIMARY_WALL` that minted a key, a follower at `follower`
/// that holds it, and the primary crashed.
fn a_follower_behind(seed: u64, follower: WallTime) -> (Sim, KNode) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let primary = WallTime::from_nanos(PRIMARY_WALL);
    let mut p = clocked_knode(&sim, "p", primary_cfg("p:1"), primary);
    let mut f = clocked_knode(&sim, "f", follower_cfg("f:1", "p:1"), follower);
    let (status, _) = drive_request(
        &sim,
        &mut [&mut p, &mut f],
        0,
        Request::new(Method::Post, "/clock-key"),
        CONVERGE_BUDGET,
    )
    .unwrap();
    assert_eq!(status, 200, "the primary mints a key at its wall time");
    krun_until(&sim, &mut [&mut p, &mut f], kconverged, CONVERGE_BUDGET).unwrap();
    p.host.crash_now();
    (sim, f)
}

fn kpromote(sim: &Sim, f: &mut KNode) -> Msg {
    f.flipped = true;
    let mut cli = CtrlConn::open(sim, "f:1", &Msg::CtrlPromote { secret: None }).unwrap();
    drive_ctrl_kernel(sim, &mut [f], &mut cli, CONVERGE_BUDGET).unwrap()
}

/// Review focus 2: a follower 30s behind the primary is within the
/// tolerance, so it promotes, and the wall time it hands out holds at the
/// latest time the primary recorded rather than at its own clock's.
#[test]
fn a_follower_within_tolerance_promotes() {
    let primary = WallTime::from_nanos(PRIMARY_WALL);
    let (sim, mut f) =
        a_follower_behind(0x5C10_0201, primary.saturating_sub(Duration::from_secs(30)));
    let reply = kpromote(&sim, &mut f);
    assert!(matches!(reply, Msg::CtrlOk { .. }), "promotes: {reply:?}");
    let (status, _) = drive_request(
        &sim,
        &mut [&mut f],
        0,
        Request::new(Method::Post, "/clock-login"),
        CONVERGE_BUDGET,
    )
    .unwrap();
    assert_eq!(status, 200, "the new primary serves a write");
    let sessions = f
        .kernel
        .as_ref()
        .unwrap()
        .app()
        .db()
        .snapshot()
        .unwrap()
        .scan::<Session>()
        .unwrap();
    assert!(
        sessions[0].expires_at >= primary + Duration::from_nanos(CLOCK_TTL),
        "the session expires at {}, before {primary} plus its one-hour TTL: the promoted node \
         handed out its own clock's reading, 30s behind a time the store already recorded",
        sessions[0].expires_at
    );
}

/// 120s behind is beyond the tolerance: the promote is refused by name before
/// the durable role flip, and the node stays a follower.
#[test]
fn a_follower_far_behind_refuses_to_promote_by_name() {
    let primary = WallTime::from_nanos(PRIMARY_WALL);
    let (sim, mut f) = a_follower_behind(
        0x5C10_0202,
        primary.saturating_sub(Duration::from_secs(120)),
    );
    match kpromote(&sim, &mut f) {
        Msg::CtrlErr { message } => assert_eq!(
            message,
            "refusing to promote: this node's wall clock reads 2026-10-22T03:02:05.123Z, 120s \
             behind 2026-10-22T03:04:05.123Z, the latest time this data directory recorded — \
             more than the 60s charpente tolerates (CLOCK_TOLERANCE). Fix the system clock \
             (check NTP); if the clock is right, the recorded time came from a clock that ran \
             ahead, and the node will promote after 2026-10-22T03:03:05.123Z."
        ),
        other => panic!("expected CtrlErr, got {other:?}"),
    }
    let kernel = f.kernel.as_ref().unwrap();
    assert!(!kernel.is_primary(), "still a follower in memory");
    assert!(!kernel.app().db().writes_allowed(), "and not writable");
    let (durable, _) = charpente::repl::role::load_role(&f.host.disk(), &Role::Primary).unwrap();
    assert!(
        matches!(durable, Role::Follower { .. }),
        "and still a follower on disk"
    );
}

/// The hand-off is the primary's point of no return (DESIGN §5), so a target
/// far behind is not refused there: the switchover completes, and the
/// target's kernel stops by name the moment its write gate opens.
#[test]
fn a_switchover_onto_a_follower_far_behind_completes_and_the_target_stops_by_name() {
    let sim = Sim::with_faults(0x5C10_0203, FaultConfig::none());
    let primary = WallTime::from_nanos(PRIMARY_WALL);
    let mut p = clocked_knode(&sim, "p", primary_cfg("p:1"), primary);
    let mut f = clocked_knode(
        &sim,
        "f",
        follower_cfg("f:1", "p:1"),
        primary.saturating_sub(Duration::from_secs(120)),
    );
    let (status, _) = drive_request(
        &sim,
        &mut [&mut p, &mut f],
        0,
        Request::new(Method::Post, "/clock-key"),
        CONVERGE_BUDGET,
    )
    .unwrap();
    assert_eq!(status, 200);
    krun_until(&sim, &mut [&mut p, &mut f], kconverged, CONVERGE_BUDGET).unwrap();
    let mut cli = CtrlConn::open(
        &sim,
        "p:1",
        &Msg::CtrlSwitchover {
            to: "f:1".to_string(),
            secret: None,
        },
    )
    .unwrap();
    let mut refused: Option<io::Error> = None;
    let mut reply = None;
    for _ in 0..CONVERGE_BUDGET {
        let mut worked = false;
        let mut deadlines = Vec::new();
        for (node, is_target) in [(&mut p, false), (&mut f, true)] {
            if is_target && refused.is_some() {
                continue;
            }
            match node.kernel.as_mut().unwrap().step(&mut node.source) {
                Ok(Step::Worked) => worked = true,
                Ok(Step::Idle { deadline }) => deadlines.extend(deadline.map(|d| d.as_nanos())),
                Err(e) if is_target => refused = Some(e),
                Err(e) => panic!("the old primary's kernel stopped: {e}"),
            }
        }
        if let Some(msg) = cli.poll().unwrap() {
            reply = Some(msg);
            break;
        }
        if !worked {
            let now = sim.now().as_nanos();
            let next = deadlines
                .into_iter()
                .chain(sim.next_wakeup())
                .filter(|&d| d > now)
                .min()
                .expect("quiescent before the switchover's reply");
            sim.clock().sleep(Duration::from_nanos(next - now));
        }
    }
    assert!(
        matches!(reply, Some(Msg::CtrlOk { .. })),
        "the switchover completes — it is never stranded mid-hand-off: {reply:?}"
    );
    let err = refused.expect("the target's kernel stopped when its write gate opened");
    assert!(
        charpente_core::db::Refusal::of(&err).is_some(),
        "a named refusal: {err}"
    );
    assert_eq!(
        err.to_string(),
        "refusing to promote: this node's wall clock reads 2026-10-22T03:02:05.123Z, 120s \
         behind 2026-10-22T03:04:05.123Z, the latest time this data directory recorded — more \
         than the 60s charpente tolerates (CLOCK_TOLERANCE). Fix the system clock (check NTP); \
         if the clock is right, the recorded time came from a clock that ran ahead, and the \
         node will promote after 2026-10-22T03:03:05.123Z."
    );
}

/// A primary restarted with a witness opens write-gated, but it is opening,
/// not being promoted: the check runs at open, with `open` in the text.
#[test]
fn a_witness_gated_primary_far_behind_its_store_refuses_to_open() {
    let sim = Sim::with_faults(0x5C10_0204, FaultConfig::none());
    let primary = WallTime::from_nanos(PRIMARY_WALL);
    let h = sim.host("p");
    let cfg = primary_cfg("p:1");
    let mut kernel = clocked_kernel(&sim, &h, &cfg, primary).unwrap();
    let now = sim.now().as_nanos();
    let mut source = SimSource::new(Box::new(sim.clock()), now + 1_000_000_000);
    let responses = source.responses();
    source.push_at(now, Request::new(Method::Post, "/clock-key"));
    kernel.run(&mut source).unwrap();
    assert_eq!(responses.lock().unwrap()[0].0, 200, "the key is minted");
    let app = kernel.into_app();
    h.restart();
    drop(app);
    store_witnesses(&h.disk(), &["w:1".to_string()]).unwrap();
    let err = match clocked_kernel(
        &sim,
        &h,
        &cfg,
        primary.saturating_sub(Duration::from_secs(120)),
    ) {
        Ok(_) => panic!("a primary 120s behind its store opened"),
        Err(e) => e,
    };
    assert_eq!(
        err.to_string(),
        "refusing to open: this node's wall clock reads 2026-10-22T03:02:05.123Z, 120s behind \
         2026-10-22T03:04:05.123Z, the latest time this data directory recorded — more than \
         the 60s charpente tolerates (CLOCK_TOLERANCE). Fix the system clock (check NTP); if \
         the clock is right, the recorded time came from a clock that ran ahead, and the node \
         will open after 2026-10-22T03:03:05.123Z."
    );
}

/// Spec §5 B (controller ruling R4): opening is checked for every role. A
/// follower judges session expiry on reads with its own wall clock, so one far
/// behind the data it holds is refused at open with `open` in the text, as a
/// primary is; within the tolerance it opens.
#[test]
fn a_follower_far_behind_its_store_refuses_to_open_by_name() {
    let sim = Sim::with_faults(0x5C10_0205, FaultConfig::none());
    let primary = WallTime::from_nanos(PRIMARY_WALL);
    let mut p = clocked_knode(&sim, "p", primary_cfg("p:1"), primary);
    let mut f = clocked_knode(&sim, "f", follower_cfg("f:1", "p:1"), primary);
    let (status, _) = drive_request(
        &sim,
        &mut [&mut p, &mut f],
        0,
        Request::new(Method::Post, "/clock-key"),
        CONVERGE_BUDGET,
    )
    .unwrap();
    assert_eq!(status, 200, "the primary mints a key at its wall time");
    krun_until(&sim, &mut [&mut p, &mut f], kconverged, CONVERGE_BUDGET).unwrap();
    let app = f.kernel.take().unwrap().into_app();
    f.host.restart();
    drop(app);
    let cfg = follower_cfg("f:1", "p:1");
    let err = match clocked_kernel(
        &sim,
        &f.host,
        &cfg,
        primary.saturating_sub(Duration::from_secs(120)),
    ) {
        Ok(_) => panic!("a follower 120s behind its store opened"),
        Err(e) => e,
    };
    assert!(
        charpente_core::db::Refusal::of(&err).is_some(),
        "a named refusal: {err}"
    );
    assert_eq!(
        err.to_string(),
        "refusing to open: this node's wall clock reads 2026-10-22T03:02:05.123Z, 120s behind \
         2026-10-22T03:04:05.123Z, the latest time this data directory recorded — more than \
         the 60s charpente tolerates (CLOCK_TOLERANCE). Fix the system clock (check NTP); if \
         the clock is right, the recorded time came from a clock that ran ahead, and the node \
         will open after 2026-10-22T03:03:05.123Z."
    );
    f.host.restart();
    if let Err(e) = clocked_kernel(
        &sim,
        &f.host,
        &cfg,
        primary.saturating_sub(Duration::from_secs(30)),
    ) {
        panic!("a follower 30s behind its store was refused: {e}");
    }
}
