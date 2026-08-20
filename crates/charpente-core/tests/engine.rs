//! `Engine::recover` re-anchoring at a GC-compacted log floor (WAL segment
//! management, T3). Before this task, `Engine::recover`'s replay closure
//! hardcoded the expected first seq at 0 — the only shape a log could ever
//! be in, since nothing before this task could produce `base_seq > 0`. Once
//! `Db::checkpoint` can `Wal::gc` away early segments, recovery must resume
//! replay at the log's actual floor instead, or the very first `on_record`
//! call (or, if GC left literally nothing to replay, the first `submit`
//! after recovery) panics against the old seq-0 assumption.
//!
//! This lives as an integration test, not inline in `engine.rs`'s
//! `#[cfg(test)]` module, for the same reason `materialize.rs` does: a unit
//! test compiles `charpente-core` in a distinct compilation unit from the
//! one `charpente-sim` (a dev-dependency that itself depends on
//! `charpente-core`) links against, so trait impls fail to unify across the
//! two. Integration tests share the one normal build both crates see.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use charpente_core::engine::{Engine, REHELLO_MARKER, StateMachine};
use charpente_core::io::{Disk, File};
use charpente_core::log::{Wal, WalConfig};
use charpente_sim::{FaultConfig, Sim};

#[derive(Default)]
struct Counter {
    applied: Vec<(u64, Vec<u8>)>,
}

impl StateMachine for Counter {
    fn apply(&mut self, seq: u64, payload: &[u8]) -> io::Result<()> {
        self.applied.push((seq, payload.to_vec()));
        Ok(())
    }
}

/// Refuses every apply, so one `submit` poisons the engine. The record
/// itself still appends and syncs — apply is the only thing that fails,
/// which is exactly the state the poison flag exists to describe.
struct AlwaysFails;

impl StateMachine for AlwaysFails {
    fn apply(&mut self, _seq: u64, _payload: &[u8]) -> io::Result<()> {
        Err(io::Error::other("the test machine refuses every apply"))
    }
}

/// Succeeds the first `remaining` applies, then fails every one after —
/// lets a test drive `submit_all` partway through a multi-record batch
/// before it fails, so `SubmitFailure::applied`'s nonzero case is exercised
/// (`AlwaysFails` above only ever fails at index 0).
struct FailsAfter {
    remaining: usize,
}

impl StateMachine for FailsAfter {
    fn apply(&mut self, _seq: u64, _payload: &[u8]) -> io::Result<()> {
        if self.remaining == 0 {
            return Err(io::Error::other("configured failure"));
        }
        self.remaining -= 1;
        Ok(())
    }
}

fn wal_cfg() -> WalConfig {
    WalConfig {
        dir: "wal".to_string(),
        segment_bytes: 64,
    }
}

#[test]
fn recover_reanchors_replay_at_a_gc_compacted_log_base() {
    let sim = Sim::with_faults(0xE7E1_0001, FaultConfig::none());

    // Build a log with plenty of segments directly through Wal (a log-layer
    // property, no Db involved), then GC most of it away.
    let mut wal = Wal::open(Box::new(sim.disk()), wal_cfg(), |_, _| Ok(())).unwrap();
    for i in 0..20u64 {
        wal.append(&i.to_le_bytes()).unwrap();
        wal.sync().unwrap();
    }
    wal.gc(15, 1).unwrap();
    let base = wal.base_seq();
    assert!(
        base > 0 && base <= 15,
        "gc should have advanced the floor past 0: {base}"
    );
    drop(wal);

    // Recovery must replay starting at `base`, not panic expecting seq 0.
    let mut engine =
        Engine::recover(Box::new(sim.disk()), wal_cfg(), Counter::default(), None).unwrap();
    let applied = &engine.machine().applied;
    assert_eq!(
        applied.first().map(|(s, _)| *s),
        Some(base),
        "replay must start at the log's base, not 0"
    );
    assert_eq!(applied.last().map(|(s, _)| *s), Some(19));
    assert_eq!(engine.last_applied(), Some(19));

    // A gapless continuation must still be enforced after the base: the
    // engine's own `submit` assert would panic first if `last_applied` had
    // been seeded wrong.
    let seq = engine.submit(&99u64.to_le_bytes()).unwrap();
    assert_eq!(seq, 20);
}

#[test]
fn recover_seeds_last_applied_even_when_the_retained_segment_is_empty() {
    let sim = Sim::with_faults(0xE7E1_0002, FaultConfig::none());

    let mut wal = Wal::open(Box::new(sim.disk()), wal_cfg(), |_, _| Ok(())).unwrap();
    for i in 0..10u64 {
        wal.append(&i.to_le_bytes()).unwrap();
        wal.sync().unwrap();
    }
    wal.gc(u64::MAX, 1).unwrap(); // retain exactly the tail segment
    let base = wal.base_seq();
    assert!(base > 0, "gc should have advanced the floor: {base}");
    drop(wal);

    // Simulate every frame in the retained segment being lost (a torn
    // segment's own recovery would legitimately truncate to 0 too): replay
    // sees zero records, but the log's floor still moved, and recovery must
    // know that without a single `on_record` call to learn it from.
    let names = sim.disk().list("wal/").unwrap();
    assert_eq!(
        names.len(),
        1,
        "gc(u64::MAX, 1) should retain exactly one segment"
    );
    let file = sim.disk().open(&names[0]).unwrap();
    file.set_len(0).unwrap();
    file.sync().unwrap();

    let engine =
        Engine::recover(Box::new(sim.disk()), wal_cfg(), Counter::default(), None).unwrap();
    assert!(
        engine.machine().applied.is_empty(),
        "no frames were replayed"
    );
    assert_eq!(
        engine.last_applied(),
        Some(base - 1),
        "recovery must seed last_applied from the log's floor even with nothing to replay"
    );
}

/// Error-suite T8: `follower.rs`'s `apply_and_ack` substring-matches this
/// exact error to decide "drop and re-Hello" (a seq gap) from a genuine `Db`
/// fault it must propagate instead. Byte-exact AND asserting `contains(the
/// shared marker const)` in the same test — a reword of the format string
/// that drops the marker, or a rename of the const's value on either side,
/// cannot drift without failing here.
#[test]
fn submit_batch_out_of_order_seq_names_the_shared_rehello_marker() {
    let sim = Sim::with_faults(0xE7E1_0003, FaultConfig::none());
    let mut engine =
        Engine::recover(Box::new(sim.disk()), wal_cfg(), Counter::default(), None).unwrap();

    let err = engine
        .submit_batch(&[(5, vec![1, 2, 3])])
        .expect_err("a shipped frame at the wrong seq must be a named error");
    let msg = err.to_string();
    assert_eq!(
        msg,
        format!(
            "shipped frame carries seq 5 but this log expects seq 0 — the ship stream is out \
             of order or from a different timeline; reconnect and {REHELLO_MARKER}"
        )
    );
    assert!(
        msg.contains(REHELLO_MARKER),
        "the engine string must contain the exact marker follower.rs matches on: {msg}"
    );
}

/// Group-commit T2: counts `File::sync` calls across every file a `Disk`
/// opens, so a test can assert "N records, one fsync" without the sim
/// exposing a sync counter of its own. Wraps `SimDisk` rather than
/// replacing it — every fault-injection/latency behavior stays real, only
/// the sync count is observed.
#[derive(Clone)]
struct SyncCounts(Arc<AtomicU64>);

impl SyncCounts {
    fn syncs(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

struct CountingDisk {
    inner: charpente_sim::SimDisk,
    syncs: Arc<AtomicU64>,
}

impl Disk for CountingDisk {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        let file = self.inner.open(path)?;
        Ok(Box::new(CountingFile {
            inner: file,
            syncs: self.syncs.clone(),
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

struct CountingFile {
    inner: Box<dyn File>,
    syncs: Arc<AtomicU64>,
}

impl File for CountingFile {
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
        self.syncs.fetch_add(1, Ordering::SeqCst);
        self.inner.sync()
    }
}

fn engine_with_sync_counter(seed: u64) -> (Engine<Counter>, SyncCounts) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let counts = SyncCounts(Arc::new(AtomicU64::new(0)));
    let disk = CountingDisk {
        inner: sim.disk(),
        syncs: counts.0.clone(),
    };
    let engine = Engine::recover(Box::new(disk), wal_cfg(), Counter::default(), None).unwrap();
    (engine, counts)
}

/// Group-commit T2 (spec §1), the whole feature in one assertion: three
/// records staged as one batch must cost exactly one fsync, not three.
#[test]
fn submit_all_costs_one_sync_for_many_records() {
    let (mut engine, counts) = engine_with_sync_counter(0x60C0_0001);
    let before = counts.syncs();
    // Three is not arbitrary: `wal_cfg`'s segment_bytes is 64 and each
    // record frames to 16 + 1 = 17 bytes, so three fit in one segment. A
    // FOURTH would rotate (68 > 64), and `Wal::rotate` syncs the old tail —
    // a second sync for an entirely legitimate reason. The count below is a
    // property of a batch inside one segment, not of any batch size.
    let seqs = engine
        .submit_all(&[b"a", b"b", b"c"])
        .expect("three records commit");
    assert_eq!(seqs, vec![0, 1, 2], "seqs are assigned in order");
    assert_eq!(
        counts.syncs() - before,
        1,
        "three records must cost exactly one fsync — that is the whole feature"
    );
}

/// An arm with nothing ready must cost nothing: `submit_all(&[])` must not
/// touch the WAL at all, not even a sync.
#[test]
fn submit_all_of_an_empty_slice_does_not_sync() {
    let (mut engine, counts) = engine_with_sync_counter(0x60C0_0002);
    let before = counts.syncs();
    let seqs = engine.submit_all(&[]).expect("an empty batch is a no-op");
    assert!(seqs.is_empty(), "no records means no seqs");
    assert_eq!(
        counts.syncs(),
        before,
        "an empty batch must not sync — an arm with nothing ready must cost nothing"
    );
}

/// The poison check runs BEFORE the empty check, and that order is a policy
/// choice rather than an accident: poison is a property of the engine, not
/// of the argument, so an engine that cannot serve refuses every call — an
/// empty batch included. Swapping the two guards is user-visible and
/// reachable by construction from the request arm (a drain in which every
/// staged handler returned an app `Err` stages zero payloads and calls
/// `submit_all(&[])`, deciding whether those clients see their own errors or
/// a blanket 500), and the T2 review proved that mutation leaves every other
/// test in the workspace green.
#[test]
fn an_empty_batch_on_a_poisoned_engine_still_refuses() {
    let sim = Sim::with_faults(0x60C0_0003, FaultConfig::none());
    let mut engine = Engine::recover(Box::new(sim.disk()), wal_cfg(), AlwaysFails, None).unwrap();

    engine
        .submit(b"x")
        .expect_err("the machine refuses every apply, which poisons the engine");
    assert!(
        engine.poisoned(),
        "a durable record that failed to apply must poison the engine"
    );

    let err = engine
        .submit_all(&[])
        .expect_err("a poisoned engine must refuse even an empty batch");
    assert_eq!(
        err.to_string(),
        "engine poisoned: a durable record failed to apply, so machine state no \
         longer matches the log — restart and recover",
        "the refusal must name the poison contract byte-exact — checks::classify \
         matches on this string's prefix, so a reword reclassifies every poisoned \
         universe in the sim suite"
    );
}

/// Group-commit T4 fix round, Important 1: `Group::commit` needs to tell a
/// caller WHERE a mid-batch apply failure landed, not just THAT it failed
/// (spec §4's last row: records before the failure keep their 200s). The
/// count must be a value `submit_all` itself counted, not one a caller
/// derives from `last_applied()` before/after (this sub-project's own T1
/// review flagged a derived gate input as unsound) — this test is the
/// mechanism `SubmitFailure::applied` exists to prove.
#[test]
fn submit_all_reports_how_many_applied_before_a_mid_batch_failure() {
    let sim = Sim::with_faults(0x60C4_0001, FaultConfig::none());
    let mut engine = Engine::recover(
        Box::new(sim.disk()),
        wal_cfg(),
        FailsAfter { remaining: 2 },
        None,
    )
    .unwrap();

    let failure = engine
        .submit_all(&[b"a", b"b", b"c", b"d"])
        .expect_err("the third record's apply must fail");
    assert_eq!(
        failure.applied, 2,
        "two records applied before the failure — the exact boundary a caller \
         needs to know which requests already got their 200s"
    );
    assert!(
        engine.poisoned(),
        "an apply failure poisons the engine, same as before"
    );
    assert_eq!(
        engine.last_applied(),
        Some(1),
        "last_applied stops at the last record that actually applied (seq 1, the \
         second of four records at seqs 0..3)"
    );
}

/// The default `apply_batch` must reproduce today's per-record semantics
/// exactly — including the partial count, which is the whole reason
/// `SubmitFailure` carries one. `FailsAfter` takes the default impl, so this
/// is the contrast against `TableMachine`'s all-or-nothing override.
#[test]
fn the_default_apply_batch_applies_per_record_and_reports_the_partial_count() {
    // No Sim and no disk: FailsAfter is pure, and the default impl is the
    // thing under test, not the store beneath it.
    let mut machine = FailsAfter { remaining: 2 };
    let records: Vec<(u64, &[u8])> = vec![(0, b"a"), (1, b"b"), (2, b"c")];
    let failure = machine
        .apply_batch(&records)
        .expect_err("the third record's apply must fail");
    assert_eq!(
        failure.applied, 2,
        "the default impl applies per record, so two really did land"
    );
    assert_eq!(failure.error.to_string(), "configured failure");
}

/// The engine advances last_applied by the count the call reports and by
/// nothing else — never inside a loop it no longer has.
#[test]
fn submit_all_advances_last_applied_to_the_last_record_of_the_batch() {
    let sim = Sim::with_faults(0x60C5_0002, FaultConfig::none());
    let mut engine =
        Engine::recover(Box::new(sim.disk()), wal_cfg(), Counter::default(), None).unwrap();

    let seqs = engine.submit_all(&[b"a", b"b", b"c"]).expect("all apply");
    assert_eq!(seqs, vec![0, 1, 2]);
    assert_eq!(
        engine.last_applied(),
        Some(2),
        "last_applied ends at the batch's last seq"
    );
    assert_eq!(
        engine.machine().applied.len(),
        3,
        "every record reached the machine"
    );
}
