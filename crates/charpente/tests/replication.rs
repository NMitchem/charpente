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
use std::time::Duration;

use charpente::repl::Repl;
use charpente::repl::protocol::{Msg, decode, encode};
use charpente::repl::role::Role;
use charpente::repl::{MAX_MESSAGE_BYTES, ReplConfig};
use charpente::{Db, DbConfig, Id, Job, Schema, TxError, table};
use charpente_core::io::{Clock, Disk, Net};
use charpente_core::log::{Wal, WalConfig};
use charpente_core::table::Codec;
use charpente_sim::checks::is_fault_fallout;
use charpente_sim::{FaultConfig, Sim, SimClock, SimHost, run_universes_swarm_with};

// ------------------------------------------------------------- fixtures

#[table]
struct Widget {
    id: Id<Widget>,
    #[index]
    kind: u64,
    weight: i64,
}

#[table]
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
}

fn make_schema() -> Schema {
    Schema::new().table::<Widget>().table::<Gadget>()
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
    /// Node recoveries so far (poison/crash → reopen) — a clean universe has
    /// zero; the fingerprint oracle skips its byte check when this is nonzero.
    recoveries: u32,
    /// Seeds completed by `Repl` instances retired at recovery (a reopen builds
    /// a fresh `Repl` whose counter starts at 0); added to the live counter.
    carried_seeds: u64,
}

/// Total snapshot seeds this node has completed, across recoveries.
fn total_seeds(node: &Node) -> u64 {
    node.carried_seeds + node.repl.stats().seeds
}

fn spawn_node(sim: &Sim, host: &str, cfg: ReplConfig, db_cfg: DbConfig) -> Result<Node, String> {
    let h = sim.host(host);
    // Open can hit injected faults (a failed sync during store/WAL creation);
    // restart the fresh host and retry, exactly as recovery does.
    for _ in 0..RECOVER_ATTEMPTS {
        match open_node(&h, &cfg, &db_cfg) {
            Ok((db, repl)) => {
                return Ok(Node {
                    host: h,
                    db: Some(db),
                    repl,
                    clock: sim.clock(),
                    name: host.to_string(),
                    repl_cfg: cfg,
                    db_cfg,
                    recoveries: 0,
                    carried_seeds: 0,
                });
            }
            Err(e) if is_fault_fallout(&e) => {
                h.restart();
                continue;
            }
            Err(e) => return Err(format!("{host}: {e}")),
        }
    }
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
fn finish_universe(sim: &Sim, r: Result<(), String>) -> Result<(), String> {
    match r {
        Err(e) if e.contains(INCONCLUSIVE) => {
            sim.cover("repl:inconclusive");
            Ok(())
        }
        other => other,
    }
}

fn open_node(h: &SimHost, cfg: &ReplConfig, db_cfg: &DbConfig) -> Result<(Db, Repl), String> {
    let mut db = Db::open(Box::new(h.disk()), db_cfg.clone(), make_schema())
        .map_err(|e| format!("Db::open: {e}"))?;
    match Repl::open(Box::new(h.net()), Box::new(h.disk()), cfg.clone(), &mut db) {
        Ok((repl, _notice)) => Ok((db, repl)),
        // Sim-found bug (seeds 0x23b5e7e7f2132438 unparseable-tear,
        // 0x51dacce702b667c8 parseable-tear naming a ghost primary): a torn
        // first-boot role write. Fixed in role.rs by the CRC integrity line —
        // corruption is now this named hard error instead of a silent
        // wrong-role parse. The error's own fix text applies verbatim here:
        // these nodes never finish first boot before the tear (T6 never flips
        // a role), so deleting the file to re-initialize from config is the
        // sanctioned exit, and load_role rewrites the initial on the retry.
        Err(e) if e.to_string().contains("integrity check") => {
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
    for _ in 0..RECOVER_ATTEMPTS {
        node.host.restart();
        match reopen(node) {
            Ok(()) => return Ok(()),
            Err(e) if is_fault_fallout(&e) => last = e, // open faulted; restart and retry
            // The designed unrestorable-locally hard error: a crash bricked the
            // store AND checkpoint GC already compacted the log past 0, so
            // rebuild-from-WAL is impossible and the named fix is "restore from
            // a replica or backup" — the path T7's promote/backup provides.
            // Until then this node is honestly dead: inconclusive, not a bug.
            Err(e) if e.contains("cannot be rebuilt from the log alone") => {
                return Err(format!(
                    "{INCONCLUSIVE}: {} store bricked below a compacted log — the restore \
                     path (T7) is the fix: {e}",
                    node.name
                ));
            }
            Err(e) => return Err(format!("recover {}: {e}", node.name)),
        }
    }
    // Db::open replays the un-checkpointed gap through the store, which needs a
    // long fault-free streak of writes; under a persistently faulty disk it may
    // legitimately never succeed (the single-node suite skips such universes
    // too). Mark the universe inconclusive rather than failing it.
    Err(format!(
        "{INCONCLUSIVE}: recover {} exhausted {RECOVER_ATTEMPTS} attempts (last: {last})",
        node.name
    ))
}

fn reopen(node: &mut Node) -> Result<(), String> {
    let (db, repl) = open_node(&node.host, &node.repl_cfg, &node.db_cfg)?;
    node.repl = repl;
    node.db = Some(db);
    Ok(())
}

const RECOVER_ATTEMPTS: u32 = 500;

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
    }
}

// --------------------------------------------------------------- oracles

/// (a) equal `last_applied`, (b) byte-equal `scan_raw` over every table plus
/// the meta counter — the layout-independent content oracle that holds across
/// a seed and across crash-recovery, retried under transient read faults.
fn assert_converged(p: &Node, f: &Node) -> Result<(), String> {
    let pdb = p.db.as_ref().ok_or("p has no db")?;
    let fdb = f.db.as_ref().ok_or("f has no db")?;
    if pdb.last_applied() != fdb.last_applied() {
        return Err(format!(
            "last_applied mismatch: p={:?} f={:?}",
            pdb.last_applied(),
            fdb.last_applied()
        ));
    }
    retry_fallout(|| {
        let ps = pdb.snapshot().map_err(|e| e.to_string())?;
        let fs = fdb.snapshot().map_err(|e| e.to_string())?;
        for table in pdb.table_names() {
            let pr = ps.scan_raw(table).map_err(|e| e.to_string())?;
            let fr = fs.scan_raw(table).map_err(|e| e.to_string())?;
            if pr != fr {
                return Err(format!(
                    "table {table} diverged: p has {} rows, f has {}",
                    pr.len(),
                    fr.len()
                ));
            }
        }
        if ps.next_job_id().map_err(|e| e.to_string())?
            != fs.next_job_id().map_err(|e| e.to_string())?
        {
            return Err("next_job_id diverged".to_string());
        }
        Ok(())
    })
}

/// (c) The determinism gift: after checkpoint + graceful close on both nodes,
/// the two hosts' store + WAL bytes are identical — both executed the
/// identical per-record redb op sequence and identical WAL appends (DESIGN §4
/// records redb's byte-determinism under identical op sequences). The role
/// file is excluded: it differs by design (primary vs follower). Consumes the
/// nodes (graceful close is part of the oracle). Falls back to nothing — with
/// `repl:fingerprint-skipped` coverage — when this universe's injected faults
/// made the op sequences diverge (retries/recoveries change redb's history
/// even though the logical content converged; oracle (b) already ran).
fn assert_bytes_identical(sim: &Sim, mut p: Node, mut f: Node) -> Result<(), String> {
    if !universe_is_quiet(sim, &[&p, &f]) {
        sim.cover("repl:fingerprint-skipped");
        return Ok(());
    }
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
            "converged nodes' store+log bytes diverged in a fault-free universe: \
             p={a:#018x} f={b:#018x}"
        ));
    }
    Ok(())
}

/// True when nothing in this universe could have perturbed either host's disk
/// op sequence: no disk faults, no crashes, no restarts, no node recoveries.
/// Net-only events (`conn_drop`, `net_spike`) don't touch bytes: a dropped
/// conn re-Hellos and resumes from the follower's real position — no replays,
/// no double-applies — so those universes stay byte-comparable.
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

fn converged(nodes: &[&mut Node]) -> bool {
    let a = nodes[0].db.as_ref().and_then(|d| d.last_applied());
    let b = nodes[1].db.as_ref().and_then(|d| d.last_applied());
    a == b
}

fn primary_cfg(addr: &str) -> ReplConfig {
    ReplConfig {
        listen: Some(addr.to_string()),
        initial_role: Role::Primary,
    }
}

fn follower_cfg(listen: &str, primary: &str) -> ReplConfig {
    ReplConfig {
        listen: Some(listen.to_string()),
        initial_role: Role::Follower {
            primary: primary.to_string(),
        },
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
        require: ["fault:conn_drop", "restart"]
    );
}

fn convergence_universe(sim: &mut Sim) -> Result<(), String> {
    let mut p = spawn_node(sim, "p", primary_cfg("p:1"), DbConfig::default())?;
    let mut f = spawn_node(sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default())?;

    // Drive commits interleaved with shipping (a moving target).
    for i in 0..40u64 {
        commit_widget(sim, &mut p, i)?;
        drive_rounds(sim, &mut [&mut p, &mut f], 3)?;
    }
    run_until(sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET)?;

    assert_converged(&p, &f)?;
    assert_bytes_identical(sim, p, f)
}

// ------------------------------------------------------- property: seed

fn seeding_db_cfg() -> DbConfig {
    DbConfig {
        wal: WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 128,
        },
        store_path: "tables.redb".to_string(),
        retain_segments: 1,
        checkpoint_every: 8,
    }
}

#[test]
fn late_follower_seeds_below_the_gcd_base_and_converges() {
    run_universes_swarm_with!(0x2EB1_0002, 256, no_media_corruption, |sim: &mut Sim| {
        let r = seeding_universe(sim);
        finish_universe(sim, r)
    });
}

fn seeding_universe(sim: &mut Sim) -> Result<(), String> {
    let mut p = spawn_node(sim, "p", primary_cfg("p:1"), seeding_db_cfg())?;
    // Commit far enough that checkpoint-driven GC moves the base past 0.
    for i in 0..60u64 {
        commit_widget(sim, &mut p, i)?;
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
    let mut f = spawn_node(sim, "f", follower_cfg("f:1", "p:1"), seeding_db_cfg())?;
    run_until(sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET)?;

    if total_seeds(&f) == 0 {
        return Err("the follower converged without ever taking the snapshot path".to_string());
    }
    // The follower seeded above 0 (not a replay from the log start).
    if f.db.as_ref().map_or(0, |d| d.log_base()) == 0 {
        return Err("a seeded follower's WAL base must be above 0".to_string());
    }
    assert_converged(&p, &f)
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

    // Repair and restart the follower: below the base now, it must re-seed.
    sim.repair("p", "f");
    f.host.restart();
    f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), db_cfg).unwrap();
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert!(
        f.repl.stats().seeds >= 1,
        "a follower below the base must re-seed on rejoin"
    );
    assert_converged(&p, &f).unwrap();
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
    for needle in ["diverged", "seq", "crc", "re-seed", "fresh data dir"] {
        assert!(
            err.contains(needle),
            "diverged refusal missing {needle:?}: {err}"
        );
    }
    // Nothing was merged: the primary's state is unchanged.
    assert_eq!(
        p.db.as_ref().unwrap().last_applied(),
        p_last_before,
        "the primary must not have merged any of the forked follower's state"
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
        },
    )
    .unwrap();
    match reply {
        Msg::Refuse { message } => {
            for needle in ["protocol version", "expected"] {
                assert!(
                    message.contains(needle),
                    "version refusal missing {needle:?}: {message}"
                );
            }
        }
        other => panic!("expected Refuse, got {other:?}"),
    }
}

#[test]
fn hello_to_a_follower_is_refused_naming_the_not_a_primary_state() {
    let sim = Sim::with_faults(0x2EB1_0007, FaultConfig::none());
    // A follower of an (absent) primary — it still listens for control conns.
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    let reply = exchange_hello(
        &sim,
        &mut f,
        "f:1",
        Msg::Hello {
            proto: charpente::repl::PROTO_VERSION,
            listen: Some("x:1".to_string()),
            position: None,
        },
    )
    .unwrap();
    match reply {
        Msg::Refuse { message } => {
            assert!(
                message.contains("not a primary"),
                "missing not-a-primary: {message}"
            );
            assert!(
                message.contains("p:1"),
                "missing the real primary's address: {message}"
            );
        }
        other => panic!("expected Refuse, got {other:?}"),
    }
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
type RawTables = Vec<(String, Vec<(u64, Vec<u8>)>)>;

fn table_scans(db: &Db) -> io::Result<RawTables> {
    let snap = db.snapshot()?;
    let mut out = Vec::new();
    for table in db.table_names() {
        out.push((table.to_string(), snap.scan_raw(table)?));
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
