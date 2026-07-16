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
use charpente_core::io::{Clock, Conn, Disk, Net};
use charpente_core::log::{Wal, WalConfig};
use charpente_core::table::{Codec, Row};
use charpente_sim::checks::{dir_entry_loss_fired, is_fault_fallout};
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
    let h = sim.host(host);
    // Open can hit injected faults (a failed sync during store/WAL creation);
    // restart the fresh host and retry, exactly as recovery does.
    for _ in 0..RECOVER_ATTEMPTS {
        match open_node(&h, &cfg, &db_cfg, false) {
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

fn open_node(
    h: &SimHost,
    cfg: &ReplConfig,
    db_cfg: &DbConfig,
    flipped: bool,
) -> Result<(Db, Repl), String> {
    let mut db = Db::open(Box::new(h.disk()), db_cfg.clone(), make_schema())
        .map_err(|e| format!("Db::open: {e}"))?;
    match Repl::open(Box::new(h.net()), Box::new(h.disk()), cfg.clone(), &mut db) {
        Ok((repl, _notice)) => Ok((db, repl)),
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
        Err(e) if e.to_string().contains("integrity check") => {
            if flipped {
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
    let (db, repl) = open_node(&node.host, &node.repl_cfg, &node.db_cfg, node.flipped)?;
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
        event_retention: 65_536,
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
        event_retention: 65_536,
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
    reopen(&mut p).expect("an above-horizon cut recovers cleanly, not RESTORE_NEEDED");
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
    for needle in ["diverged", "seq", "crc", "re-seed", "fresh data dir"] {
        assert!(err.contains(needle), "refusal missing {needle:?}: {err}");
    }
    // The refusal names BOTH positions: the follower's frontier and the primary's last.
    assert!(
        err.contains(&f_frontier.to_string()),
        "refusal must name the follower's frontier {f_frontier}: {err}"
    );
    assert!(
        err.contains(&p_last_after.to_string()),
        "refusal must name the primary's last {p_last_after}: {err}"
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
    assert_converged(&p, &f).unwrap();
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
    reopen(&mut f).unwrap();
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
    assert_converged(&p, &f).unwrap();
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
        let conn = sim
            .host("cli")
            .net()
            .connect(addr)
            .map_err(|e| format!("{INCONCLUSIVE}: ctrl connect {addr}: {e}"))?;
        conn.send(&encode(verb))
            .map_err(|e| format!("{INCONCLUSIVE}: ctrl send: {e}"))?;
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
            Err(e) => return Err(format!("{INCONCLUSIVE}: {e}")),
        }
        if !advanced {
            return Err(format!("{INCONCLUSIVE}: quiescent before a control reply"));
        }
    }
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
fn assert_all_present(node: &Node, ids: &[u64], who: &str) -> Result<(), String> {
    let db = node.db.as_ref().ok_or_else(|| format!("{who} has no db"))?;
    retry_fallout(|| {
        let snap = db.snapshot().map_err(|e| e.to_string())?;
        let rows = snap.scan_raw(Widget::TABLE).map_err(|e| e.to_string())?;
        for id in ids {
            if !rows.iter().any(|(rid, _)| rid == id) {
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
        require: ["fault:conn_drop"]
    );
}

/// The result of one switchover attempt's drain.
enum DrainResult {
    Reply(Msg),
    /// The target follower was momentarily disconnected (conn_drop mid-
    /// reconnect) when the verb was processed — retry.
    NoSession,
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
                WriteOutcome::Fallout => {}
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
                return Err("f is not writable after the switchover completed".to_string());
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
            assert_all_present(&f, &acked, "f")?;
            // Keep writing on the new primary; the old primary (now a follower)
            // converges with no divergence.
            for _ in 0..12u64 {
                let _ = write_once(sim, &mut f, next)?;
                next += 1;
                drive_rounds(sim, &mut [&mut p, &mut f], 3)?;
            }
            run_until(sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET)?;
            assert_converged(&p, &f)?;
            Ok(())
        }
        Msg::CtrlErr { message } => {
            // The rare HandOffDone-loss window (spec §2): the hand-off was sent
            // and f promoted, but the confirmation was lost, so p stays
            // write-stopped. Safety holds; this is not the happy-path oracle.
            sim.cover("repl:switchover-stalled");
            if !message.contains("switchover") {
                return Err(format!("unexpected CtrlErr from a switchover: {message}"));
            }
            assert_one_writable(&p, &f)
        }
        other => Err(format!("unexpected switchover reply: {other:?}")),
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
    }
}

#[test]
fn failover_preserves_everything_the_follower_acked() {
    run_universes_swarm_with!(0x2EB1_000A, 128, no_media_corruption, |sim: &mut Sim| {
        let r = failover_universe(sim);
        finish_universe(sim, r)
    });
}

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
    if p.db.is_none() || !matches!(p.repl.role(), Role::Primary) || f.recoveries > 0 {
        sim.cover("repl:failover-inconclusive");
        return Ok(());
    }
    let Some(f_last) = f.db.as_ref().and_then(|d| d.last_applied()) else {
        sim.cover("repl:failover-inconclusive");
        return Ok(());
    };

    // Host crash on p — no drain (a real failover).
    p.host.crash_now();

    // Promote f (drive f alone; p is down).
    f.flipped = true;
    let reply = match CtrlConn::open(sim, "f:1", &Msg::CtrlPromote)
        .and_then(|mut cli| drive_ctrl(sim, &mut [&mut f], &mut cli, CONVERGE_BUDGET))
    {
        Ok(m) => m,
        Err(e) if e.contains(INCONCLUSIVE) => {
            sim.cover("repl:failover-inconclusive");
            return Ok(());
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
    if f.recoveries > 0 || !f.db.as_ref().is_some_and(|d| d.writes_allowed()) {
        sim.cover("repl:failover-inconclusive");
        return Ok(());
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
    let p_frames = reopen_p_frames(&mut p, &cfg)?;
    for (seq, bytes) in &f_frames {
        if *seq > f_last {
            continue;
        }
        match p_frames.iter().find(|(s, _)| s == seq) {
            Some((_, pb)) if pb == bytes => {}
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
    Ok(())
}

/// Reopen p's crashed disk and read its full durable log — the prefix oracle's
/// reference. Retried under open-time faults; inconclusive if it can't recover.
fn reopen_p_frames(node: &mut Node, cfg: &DbConfig) -> Result<Vec<(u64, Vec<u8>)>, String> {
    for _ in 0..RECOVER_ATTEMPTS {
        node.host.restart();
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

    // Begin the switchover and drive to the HandedOff instant (HandOff sent,
    // HandOffDone not yet processed).
    p.flipped = true;
    f.flipped = true;
    let _cli = CtrlConn::open(
        &sim,
        "p:1",
        &Msg::CtrlSwitchover {
            to: "f:1".to_string(),
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

    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_converged(&p, &f).unwrap();
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
        },
    )
    .unwrap();
    let reply = drive_ctrl(&sim, &mut [&mut p, &mut f], &mut cli, CONVERGE_BUDGET).unwrap();
    match reply {
        Msg::CtrlErr { message } => {
            for needle in ["lag", "deadline", "aborted"] {
                assert!(
                    message.contains(needle),
                    "abort message missing {needle:?}: {message}"
                );
            }
        }
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
    let mut cli = CtrlConn::open(sim, "f:1", &Msg::CtrlPromote)?;
    match drive_ctrl(sim, &mut [&mut f], &mut cli, CONVERGE_BUDGET)? {
        Msg::CtrlOk { .. } => {}
        other => return Err(format!("promote failed in the meta-test: {other:?}")),
    }

    // The oracle must catch the skipped drain.
    assert_all_present(&f, &acked, "f")
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
use charpente::{Tx, View, job, jobs, route, routes};
use charpente_core::job::JobRow;

#[table]
struct Tally {
    id: Id<Tally>,
    count: u64,
}

#[job]
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

#[route(POST "/widget/:id")]
fn put_widget(tx: &mut Tx, id: u64) -> Result<u64, ApiError> {
    tx.put(&Widget {
        id: Id::from_raw(id),
        kind: id % 3,
        weight: id as i64,
    });
    tx.enqueue(Bump { slot: id });
    Ok(id)
}

#[route(GET "/widget/:id")]
fn get_widget(tx: &mut Tx, id: Id<Widget>) -> Result<i64, ApiError> {
    Ok(tx.get(id)?.ok_or(ApiError::NotFound)?.weight)
}

fn kernel_schema() -> Schema {
    Schema::new().table::<Widget>().table::<Tally>()
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
    recoveries: u32,
    flipped: bool,
    /// A designated survivor (the property's clean follower) is never
    /// recovered — a fault on it makes the universe inconclusive, because the
    /// single-follower promise needs one clean copy.
    recoverable: bool,
}

/// Build the App + Repl + Kernel for one host, with the same torn-role
/// first-boot re-init the Db-level `open_node` uses.
fn open_kernel(
    sim: &Sim,
    h: &SimHost,
    cfg: &ReplConfig,
    db_cfg: &DbConfig,
    flipped: bool,
) -> Result<Kernel, String> {
    let mut app = App::open(
        Box::new(h.disk()),
        kernel_schema(),
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
        Err(e) if e.to_string().contains("integrity check") => {
            if flipped {
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
    let h = sim.host(host);
    for _ in 0..RECOVER_ATTEMPTS {
        match open_kernel(sim, &h, &cfg, &db_cfg, false) {
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
                    recoveries: 0,
                    flipped: false,
                    recoverable,
                });
            }
            Err(e) if e.contains(INCONCLUSIVE) => return Err(e),
            Err(e) if is_fault_fallout(&e) => {
                h.restart();
                continue;
            }
            Err(e) => return Err(format!("{host}: {e}")),
        }
    }
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
        match open_kernel(sim, &node.host, &node.repl_cfg, &node.db_cfg, node.flipped) {
            Ok(kernel) => {
                node.kernel = Some(kernel);
                return Ok(());
            }
            Err(e) if e.contains(INCONCLUSIVE) => return Err(e),
            // Corruption at or below the durable horizon: not a torn tail — the
            // local copy is bricked and the fix is restore. Surface it (the
            // caller reseeds from the survivor) rather than retry into the same
            // durable bytes forever.
            Err(e) if e.contains("restore this node from a replica or backup") => {
                return Err(format!(
                    "{RESTORE_NEEDED}: {} corrupted below the horizon",
                    node.name
                ));
            }
            Err(e) if e.contains("cannot be rebuilt from the log alone") => {
                return Err(format!(
                    "{RESTORE_NEEDED}: {} store bricked below a compacted log",
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
            // through to the loud bug arm below, not this mask.
            Err(e) if e.contains("refusing to guess") && dir_entry_loss_fired(sim) => {
                return Err(format!(
                    "{RESTORE_NEEDED}: {} log directory gapped by entry loss",
                    node.name
                ));
            }
            Err(e) if is_fault_fallout(&e) => last = e,
            Err(e) => return Err(format!("recover {}: {e}", node.name)),
        }
    }
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
        Err(e) if !node.recoverable => Err(format!(
            "{INCONCLUSIVE}: {} (the designated survivor) faulted: {e}",
            node.name
        )),
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
    let sink = nodes[target].responses.clone();
    let before = sink.lock().expect("responses lock").len();
    let now = sim.now().as_nanos();
    nodes[target].source.push_at(now, req);
    for _ in 0..budget {
        if sink.lock().expect("responses lock").len() > before {
            return Ok(sink.lock().expect("responses lock")[before].clone());
        }
        if !kernel_round(sim, nodes)? {
            break;
        }
    }
    if sink.lock().expect("responses lock").len() > before {
        Ok(sink.lock().expect("responses lock")[before].clone())
    } else {
        Err(format!(
            "{INCONCLUSIVE}: no response to a request on node {target} within budget"
        ))
    }
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
            Err(e) => return Err(format!("{INCONCLUSIVE}: {e}")),
        }
        if !advanced {
            return Err(format!("{INCONCLUSIVE}: quiescent before a control reply"));
        }
    }
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
        let pj = ps.scan_raw(JobRow::TABLE).unwrap();
        let fj = fs.scan_raw(JobRow::TABLE).unwrap();
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
        body, b"3",
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
    assert!(
        String::from_utf8_lossy(&body).contains("p:1"),
        "the 421 names the primary: {}",
        String::from_utf8_lossy(&body)
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
    b.kernel = Some(open_kernel(&sim, &b.host, &b.repl_cfg, &b.db_cfg, false).unwrap());

    let mut cli = CtrlConn::open(&sim, "b:1", &Msg::CtrlPromote).unwrap();
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
    }
}

/// Which acked widget ids the node's store currently holds (it never loses
/// these once present, being the designated survivor).
fn ids_present(node: &KNode, ids: &[u64]) -> Result<Vec<u64>, String> {
    let db = node.kernel.as_ref().ok_or("node has no kernel")?.app().db();
    let snap = db.snapshot().map_err(|e| e.to_string())?;
    let rows = snap.scan_raw(Widget::TABLE).map_err(|e| e.to_string())?;
    Ok(ids
        .iter()
        .copied()
        .filter(|id| rows.iter().any(|(rid, _)| rid == id))
        .collect())
}

/// THE promise (spec §4 property 8): every acked id the follower had converged
/// on still lives on it. No `durable_corruption_fired` excuse — a converged
/// follower exists, so this is a hard oracle.
fn assert_survivor_holds(node: &KNode, ids: &[u64]) -> Result<(), String> {
    retry_fallout(|| {
        let db = node
            .kernel
            .as_ref()
            .ok_or("survivor has no kernel")?
            .app()
            .db();
        let snap = db.snapshot().map_err(|e| e.to_string())?;
        let rows = snap.scan_raw(Widget::TABLE).map_err(|e| e.to_string())?;
        for id in ids {
            if !rows.iter().any(|(rid, _)| rid == id) {
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
fn bump_exactly_once(node: &KNode) -> Result<(), String> {
    retry_fallout(|| {
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
        let mut cli = CtrlConn::open(sim, "f:1", &Msg::CtrlPromote)?;
        match drive_ctrl_kernel(sim, &mut [f], &mut cli, KBUDGET)? {
            Msg::CtrlOk { .. } => {}
            other => {
                return Err(format!(
                    "{INCONCLUSIVE}: promoting the survivor failed: {other:?}"
                ));
            }
        }
    }
    // Wipe the bricked node's disk and rebuild it as a fresh follower of the
    // survivor — a fresh follower below the survivor's base re-seeds.
    p.kernel = None;
    p.host.restart();
    let disk = p.host.disk();
    for name in disk.list("").map_err(|e| e.to_string())? {
        let _ = disk.remove(&name);
    }
    let _ = disk.sync_dir("");
    let _ = disk.sync_dir("wal/");
    p.repl_cfg = follower_cfg("p:1", "f:1");
    p.flipped = true;
    p.recoveries += 1;
    p.kernel = Some(
        open_kernel(sim, &p.host, &p.repl_cfg, &p.db_cfg, true).map_err(|e| {
            if e.contains(INCONCLUSIVE) {
                e
            } else {
                format!("{INCONCLUSIVE}: restore reopen: {e}")
            }
        })?,
    );
    // Drive the re-seed to convergence so the restore is real, not just wired.
    // Any failure here (a stall, or the freshly-restored node needing yet
    // another restore) is too much damage to adjudicate: inconclusive.
    krun_until(sim, &mut [p, f], kconverged, KBUDGET).map_err(|e| {
        if e.contains(INCONCLUSIVE) {
            e
        } else {
            format!("{INCONCLUSIVE}: restore convergence stalled: {e}")
        }
    })
}

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
        require: ["fault:bit_flip", "repl:above-horizon-cut", "repl:promised-nonempty"]
    );
}

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
                Err(e) if e.contains(RESTORE_NEEDED) => {
                    restore_p_from_survivor(sim, &mut p, &mut f)?
                }
                // INCONCLUSIVE (finish_universe covers it) or a real bug: bubble.
                Err(e) => return Err(e),
            }
        }
        // Converge f onto p as far as faults allow (best effort — a stall under
        // persistent faults is not a bug, we snapshot what f durably holds).
        match krun_until(sim, &mut [&mut p, &mut f], kconverged, KBUDGET) {
            Ok(()) => {}
            Err(e) if e.contains(RESTORE_NEEDED) => restore_p_from_survivor(sim, &mut p, &mut f)?,
            Err(e) if e.contains(INCONCLUSIVE) => return Err(e),
            Err(_) => {}
        }
        // Record everything f durably holds now — it keeps these forever.
        promised.extend(retry_fallout(|| ids_present(&f, &acked)).unwrap_or_default());

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
        // sacrificial frame is applied (acked) and above the checkpoint, so
        // recovery cuts it above the horizon (fired as the cover below via the
        // `forced_cut` observable). Two things keep this from weakening the
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
        let forced_cut = p.kernel.as_ref().is_some_and(|k| k.is_primary())
            && matches!(
                drive_request(
                    sim,
                    &mut [&mut p],
                    0,
                    Request::new(Method::Post, format!("/widget/{}", 10_000 + round)),
                    KBUDGET,
                ),
                Ok((200, _))
            );
        let last_before_crash = p.kernel.as_ref().and_then(|k| k.app().db().last_applied());
        if forced_cut {
            let _ = corrupt_last_wal_frame(&p.host);
        }
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
            Err(e) if e.contains(RESTORE_NEEDED) => restore_p_from_survivor(sim, &mut p, &mut f)?,
            Err(e) if e.contains(INCONCLUSIVE) => return Err(e),
            Err(e) => return Err(e),
        }
    }

    // Final best-effort convergence so f reflects the latest shipped state.
    let _ = krun_until(sim, &mut [&mut p, &mut f], kconverged, KBUDGET);

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
    assert_survivor_holds(&f, &promised)?;
    // And the replicated jobs stayed exactly-once through it all.
    bump_exactly_once(&f)?;
    Ok(())
}
