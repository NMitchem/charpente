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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use charpente_core::db::{Db, DbConfig, Schema, Seeder, Tenant, Tx, TxError};
use charpente_core::engine::{Engine, REHELLO_MARKER, StateMachine};
use charpente_core::io::{Disk, File};
use charpente_core::job::{JobRow, JobStatus};
use charpente_core::log::{Wal, WalConfig};
use charpente_core::migration::{SchemaVersion, schema_version_id};
use charpente_core::table::{Codec, Id, IndexDef, Row, Snapshot, Store, UNSCOPED, Unscoped, View};
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

/// Fix round 1, Minor 2: `submit_all`'s error-path `last_applied` advancement
/// is pinned by `submit_all_reports_how_many_applied_before_a_mid_batch_failure`
/// above (deleting its `if applied > 0` guard goes red); `submit_batch`'s
/// mirror-image guard had no such test, so deleting it left the whole
/// workspace green. This is that missing mirror.
#[test]
fn submit_batch_advances_last_applied_to_the_last_frame_that_actually_applied() {
    let sim = Sim::with_faults(0x60C6_0003, FaultConfig::none());
    let mut engine = Engine::recover(
        Box::new(sim.disk()),
        wal_cfg(),
        FailsAfter { remaining: 2 },
        None,
    )
    .unwrap();

    engine
        .submit_batch(&[
            (0, b"a".to_vec()),
            (1, b"b".to_vec()),
            (2, b"c".to_vec()),
            (3, b"d".to_vec()),
        ])
        .expect_err("the third frame's apply must fail");
    assert!(
        engine.poisoned(),
        "an apply failure poisons the engine, same as submit_all"
    );
    assert_eq!(
        engine.last_applied(),
        Some(1),
        "last_applied stops at the last frame that actually applied (seq 1, the \
         second of four frames at seqs 0..3)"
    );
}

/// Task 2 (tenant scoping): `Row::Scope` defaults every existing table to
/// `Unscoped`, byte-identically. `engine.rs` has no `#[table]` type of its
/// own to test against (`#[table]` emits `::charpente::` paths and core
/// cannot depend on that crate), so this uses `SchemaVersion` — one of
/// core's own hand-written `Row` impls (DESIGN §3) — as the stand-in the
/// brief's `Widget` comment calls for ("any existing #[table] type in the
/// test module works here"). It used `JobRow` until the job-tenancy write
/// seal gave that table `PlatformOwned`; the next test pins what that kept.
#[test]
fn an_ordinary_table_is_unscoped_by_default() {
    fn assert_unscoped<R: Row<Scope = Unscoped>>() {}
    assert_unscoped::<SchemaVersion>();
}

/// The job-tenancy write seal takes writes away from `JobRow` and nothing
/// else: a transaction still reads it through `View` (`UnscopedRow`, which
/// GUIDE §8's oracles and the dispatcher's own scan rely on), and it still
/// stores under the bare `u64` key (`an_unscoped_table_still_writes_a_bare_u64_key`
/// below). That `Tx::put`/`Tx::delete` refuse it is pinned by the
/// `job_row_*` compile-fail fixtures in `charpente-macros`.
#[test]
fn a_job_row_is_still_read_through_view() {
    fn assert_readable<R: charpente_core::table::UnscopedRow>() {}
    assert_readable::<JobRow>();
}

// `scoped_key`'s ordering property is pinned by `table.rs`'s own
// `scoped_key_tests` unit module — it needs no `Store`, so it does not need
// to be out here, and keeping it there is what lets `scoped_key` stay
// `pub(crate)`.

/// Task 4 (tenant scoping), Step 5: the pinned-seed guarantee rests on
/// unscoped tables staying byte-identical after this change — every pinned
/// simulation seed depends on the stored bytes, and if an unscoped table's
/// key encoding shifted, every existing universe would shift with it.
///
/// Asserts against the STORED bytes read back through `Snapshot::scan_raw`,
/// not a round trip through `encode_row`/`decode_row`: a round trip would
/// pass unchanged even if the key shape had moved, because decoding never
/// looks at the key at all. `scan_raw` itself is part of the proof here —
/// it STILL tries opening the table as `TableDefinition<u64, &[u8]>` first
/// (unchanged by this task), so an unscoped row's physical key shifting to
/// the composite 16 bytes would surface as a `scope` that is never
/// `UNSCOPED`, since only the `[u8;16]`-keyed fallback branch (Task 6c) can
/// produce a nonzero one — this test's `UNSCOPED` in the expected tuple is
/// that half of the proof, the same property
/// `a_scoped_table_stores_under_the_composite_key_not_a_bare_u64` below
/// demonstrates from the scoped side (a real, distinguishable scope value).
#[test]
fn an_unscoped_table_still_writes_a_bare_u64_key() {
    let sim = Sim::with_faults(0x7A2E_0007, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    let row = JobRow {
        id: Id::from_raw(7),
        name: "send-receipt".to_string(),
        payload: b"payload".to_vec(),
        key: None,
        status: JobStatus::Pending,
        attempts: 0,
        due_at: 0,
        last_error: None,
        shape: None,
        scope: None,
    };
    let mut expected_bytes = Vec::new();
    row.encode_row(&mut expected_bytes);

    store.apply(0, |w| w.put(UNSCOPED, &row)).unwrap();

    let snap = store.snapshot().unwrap();
    let raw = snap.scan_raw(JobRow::TABLE, false).unwrap();
    assert_eq!(
        raw,
        vec![(UNSCOPED, 7u64, expected_bytes)],
        "an unscoped table's stored key must still be the bare u64 id and its stored value \
         must still be exactly Row::encode_row's bytes"
    );
}

struct TestOrg;

/// The scope's membership check, as core sees it: `Tenant` is the hook, and
/// `Tx::open_scope` (or, for the operator, `Tx::admit_scope`) is the only
/// route to a scoped row. `Who = str` rather than a struct so the associated
/// type's `?Sized` is exercised somewhere; `Error = io::Error` so a refusal
/// composes with the `?` the staged closures below already use.
impl Tenant for TestOrg {
    type Who = str;
    type Error = io::Error;

    fn authorize(_tx: &Tx, who: &str, _scope: Id<TestOrg>) -> Result<(), io::Error> {
        match who {
            "stranger" => Err(io::Error::other("not a member of this org")),
            _ => Ok(()),
        }
    }
}

/// A hand-written scoped `Row` for the two tests below. `engine.rs` cannot use
/// `#[table(scoped_by = …)]` — the macro emits `::charpente::` paths, and core
/// cannot depend on that crate (same reason `JobRow` stands in for a
/// macro-generated type elsewhere in this file) — so this hand-writes the impl
/// the way `JobRow`/`EventRow` do (DESIGN §3), with `type Scope` set to a
/// marker distinct from `Unscoped`.
///
/// The secondary index is not decoration: it is the only handle an integration
/// test has on a scoped table's contents before Task 5/Task 7 land. See
/// `ticket_index_entry_survives`.
#[derive(Debug)]
struct Ticket {
    id: Id<Ticket>,
    n: u32,
}

impl Row for Ticket {
    type Scope = TestOrg;
    const TABLE: &'static str = "test.ticket";
    const INDEXES: &'static [IndexDef<Ticket>] = &[IndexDef {
        name: "by_n",
        key: |t, out| t.n.encode(out),
    }];

    fn id(&self) -> Id<Ticket> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.n.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<Ticket> {
        let input = &mut &bytes[..];
        let id = Id::decode(input)?;
        let n = u32::decode(input)?;
        Ok(Ticket { id, n })
    }
}

/// Whether `Ticket`'s `by_n` index holds an entry for `n` under `scope`.
/// Resolves through `get_scoped`, so an entry that points at a row which no
/// longer exists panics (index corruption) rather than reading as absent.
fn ticket_index_entry_survives(snap: &Snapshot, scope: u64, n: u32) -> bool {
    !snap
        .scan_by_scoped::<Ticket>(scope, "by_n", &n)
        .expect("scan_by_scoped must not error for a scoped table")
        .is_empty()
}

/// Task 4 (tenant scoping): a scoped Row (`Row::Scope != Unscoped`) is stored
/// in a PHYSICALLY DIFFERENT redb table (`[u8;16]`-keyed) from the
/// bare-`u64`-keyed one the test above pins for the unscoped case.
/// `Writer::put` routes a scoped row to that composite table directly, so
/// `scan_raw(Ticket::TABLE, true)`'s only attempt — the composite shape —
/// opens on the first try and this asserts the read carries the real scope,
/// not `UNSCOPED` (proven distinct from the unscoped case by
/// `an_unscoped_table_still_writes_a_bare_u64_key`, which reads back
/// `UNSCOPED` for a table whose rows are physically bare).
#[test]
fn a_scoped_table_stores_under_the_composite_key_not_a_bare_u64() {
    let sim = Sim::with_faults(0x7A2E_0008, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| {
            w.put(
                10,
                &Ticket {
                    id: Id::from_raw(5),
                    n: 1,
                },
            )
        })
        .expect("a scoped put must not error");

    let snap = store.snapshot().unwrap();
    let raw = snap
        .scan_raw(Ticket::TABLE, true)
        .expect("a scoped table's rows are always composite-keyed once written");
    assert_eq!(
        raw,
        vec![(10u64, 5u64, {
            let mut b = Vec::new();
            Ticket {
                id: Id::from_raw(5),
                n: 1,
            }
            .encode_row(&mut b);
            b
        })],
        "scan_raw must recover the real (scope, id) from the composite key, not UNSCOPED"
    );
}

/// Fix round 2, M-1 under ruling R42: scoping is opt-in per table and never
/// retrofitted onto an existing one (spec §8's non-goal). If a table's rows
/// are physically bare-`u64`-keyed — written before the table ever declared
/// `scoped_by`, or by a schema that never scoped it — `scan_raw`/
/// `scan_raw_from` must refuse the flip by name, not silently read the rows
/// back at a fabricated scope 0. Built with `put_raw(None, …)`, the exact
/// decode-free primitive the WAL replay and migration paths use, so this is
/// not a contrived construction — it is the shape any of those paths would
/// leave behind on a table before it ever gained a scope.
#[test]
fn a_scoped_read_over_a_physically_bare_table_refuses_the_scope_flip_by_name() {
    let sim = Sim::with_faults(0x7A2E_0009, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| w.put_raw(Ticket::TABLE, None, 5, b"old"))
        .expect("a bare put must not error");

    let snap = store.snapshot().unwrap();
    let err = snap.scan_raw(Ticket::TABLE, true).unwrap_err();
    assert!(
        err.to_string()
            .contains("is declared scoped_by but its rows are keyed unscoped"),
        "{err}"
    );
    assert!(
        err.to_string()
            .contains("a table cannot gain a scope in place"),
        "{err}"
    );

    let err2 = snap
        .scan_raw_from(Ticket::TABLE, true, None, 10)
        .unwrap_err();
    assert!(
        err2.to_string()
            .contains("is declared scoped_by but its rows are keyed unscoped"),
        "{err2}"
    );
}

/// Hard rule 5 across the whole scope-flip CLASS, not the two functions that
/// happened to get it first. `scan_raw`/`scan_raw_from` refuse the flip by
/// name; every other scoped entry point fell through to `redb_err`'s
/// "table store (redb): {e}", which names neither the contract nor the fix —
/// so the same operator mistake produced an actionable error from exactly one
/// read path and a bare redb type error from eight public ones.
///
/// A physically bare table is reachable from every entry point here, so each
/// must refuse by name. Built with `put_raw(None, …)` exactly as the sibling
/// test above: the disk shape any pre-scope write path leaves behind, not a
/// contrived one. This pins the BEHAVIOUR (an operator learns the contract and
/// the fix at whichever door they knocked on) rather than the call sites, so a
/// new scoped reader added later without the mapping goes red here.
#[test]
fn every_scoped_entry_point_refuses_the_scope_flip_by_name() {
    let sim = Sim::with_faults(0x7A2E_000A, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| w.put_raw(Ticket::TABLE, None, 5, b"old"))
        .expect("a bare put must not error");

    fn names_the_contract(err: &io::Error, site: &str) {
        let s = err.to_string();
        assert!(
            s.contains("is declared scoped_by but its rows are keyed unscoped"),
            "{site} must name the scope-flip contract, got: {s}"
        );
        assert!(
            s.contains("a table cannot gain a scope in place"),
            "{site} must name the fix, got: {s}"
        );
    }

    let snap = store.snapshot().unwrap();
    names_the_contract(
        &snap
            .get_scoped::<Ticket>(10, Id::from_raw(5))
            .expect_err("must refuse the scope flip"),
        "Snapshot::get_scoped",
    );
    names_the_contract(
        &snap
            .scan_scoped::<Ticket>(10)
            .expect_err("must refuse the scope flip"),
        "Snapshot::scan_scoped",
    );
    names_the_contract(
        &snap
            .scan_from_scoped::<Ticket>(10, 0, 10)
            .expect_err("must refuse the scope flip"),
        "Snapshot::scan_from_scoped",
    );
    drop(snap);

    // Every writer arm below fails before commit, so `applied_seq` stays 0 and
    // each attempt is the same gapless seq 1 rather than a running counter.
    names_the_contract(
        &store
            .apply(1, |w| w.get_raw(Ticket::TABLE, Some(10), 5))
            .expect_err("must refuse the scope flip"),
        "Writer::get_raw",
    );
    names_the_contract(
        &store
            .apply(1, |w| w.put_raw(Ticket::TABLE, Some(10), 6, b"new"))
            .expect_err("must refuse the scope flip"),
        "Writer::put_raw",
    );
    names_the_contract(
        &store
            .apply(1, |w| w.delete_raw(Ticket::TABLE, Some(10), 5))
            .expect_err("must refuse the scope flip"),
        "Writer::delete_raw",
    );
    names_the_contract(
        &store
            .apply(1, |w| w.delete::<Ticket>(10, Id::from_raw(5)))
            .expect_err("must refuse the scope flip"),
        "Writer::delete",
    );
    names_the_contract(
        &store
            .apply(1, |w| w.rebuild_indexes_for::<Ticket>())
            .expect_err("must refuse the scope flip"),
        "Writer::rebuild_indexes_for",
    );
}

/// The mirror of `every_scoped_entry_point_refuses_the_scope_flip_by_name`
/// above. `14863db` fixed the scoped direction at all eleven doors; the
/// reverse — declared unscoped, physically composite-keyed — was named only
/// in `scan_raw`/`scan_raw_from`'s arms via `scan_raw_shape_error`. Every
/// other unscoped entry point fell through to `redb_err`, whose text is
/// redb's own and names neither the contract, the site, nor the fix (hard
/// rule 5).
///
/// `Ticket` (above) cannot stand in for the unscoped fixture: it is the
/// SCOPED type the sibling test uses (`Row::Scope = TestOrg`). `Widget`
/// (`type Scope = Unscoped`, declared below for
/// `an_unscoped_index_entry_stays_byte_identical`) is the exact inverse
/// fixture this test needs, and it already carries a secondary index.
///
/// `View::scan_by` is a ninth public unscoped door and is not one of the 9
/// fixed sites (no `open_table(rows_def(` call of its own — it opens the
/// INDEX table, and reaches the primary table only transitively through
/// `self.get(id)?` for an id an index-range query returns), so it is not
/// knocked on IN THIS TEST. It is pinned separately, in
/// `view_scan_by_reaches_the_reverse_flip_through_a_crafted_scope_collision`
/// below, because reaching it needs a different disk state (a genuinely
/// scoped `put`, not `put_raw`) and a second fixture — see that test's doc
/// comment for why the door IS reachable: an earlier version of this
/// comment claimed it could never be reached, and that claim was FALSE,
/// disproved by review with a working probe. Do not re-derive that
/// "unreachable" conclusion from first principles without re-reading the
/// sibling test first.
#[test]
fn every_unscoped_entry_point_refuses_the_reverse_flip_by_name() {
    let sim = Sim::with_faults(0x7A2E_000B, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    // `Widget` is declared WITHOUT scoped_by. Writing it composite-keyed is
    // the disagreement under test — the exact inverse of the scoped test
    // above, which writes bare and then reads scoped.
    store
        .apply(0, |w| w.put_raw(Widget::TABLE, Some(10), 5, b"old"))
        .expect("a scoped put must not error");

    fn names_the_contract(err: &io::Error, site: &str) {
        let s = err.to_string();
        assert!(
            s.contains("is declared without scoped_by but its rows are keyed with a tenant scope"),
            "{site} must name the reverse-flip contract, got: {s}"
        );
        assert!(
            s.contains("a table cannot lose a scope in place"),
            "{site} must name the fix, got: {s}"
        );
    }

    // The unscoped readers are `View` trait methods on `Snapshot`
    // (`crates/charpente-core/src/table.rs:1415-1496`); `View` must be in
    // scope (imported above alongside `Snapshot`).
    let snap = store.snapshot().unwrap();
    names_the_contract(
        &snap
            .get::<Widget>(Id::from_raw(5))
            .expect_err("must refuse the reverse flip"),
        "View::get",
    );
    names_the_contract(
        &snap
            .scan::<Widget>()
            .expect_err("must refuse the reverse flip"),
        "View::scan",
    );
    names_the_contract(
        &snap
            .scan_from::<Widget>(0, 10)
            .expect_err("must refuse the reverse flip"),
        "View::scan_from",
    );
    drop(snap);

    // Every writer arm below fails before commit, so `applied_seq` stays 0
    // and each attempt is the same gapless seq 1 — the same reasoning the
    // scoped test above records at its writer block.
    names_the_contract(
        &store
            .apply(1, |w| w.get_raw(Widget::TABLE, None, 5))
            .expect_err("must refuse the reverse flip"),
        "Writer::get_raw",
    );
    names_the_contract(
        &store
            .apply(1, |w| w.put_raw(Widget::TABLE, None, 6, b"new"))
            .expect_err("must refuse the reverse flip"),
        "Writer::put_raw",
    );
    names_the_contract(
        &store
            .apply(1, |w| w.delete_raw(Widget::TABLE, None, 5))
            .expect_err("must refuse the reverse flip"),
        "Writer::delete_raw",
    );
    names_the_contract(
        &store
            .apply(1, |w| w.delete::<Widget>(UNSCOPED, Id::from_raw(5)))
            .expect_err("must refuse the reverse flip"),
        "Writer::delete",
    );
    names_the_contract(
        &store
            .apply(1, |w| w.rebuild_indexes_for::<Widget>())
            .expect_err("must refuse the reverse flip"),
        "Writer::rebuild_indexes_for",
    );
    names_the_contract(
        &store
            .apply(1, |w| {
                w.put(
                    UNSCOPED,
                    &Widget {
                        id: Id::from_raw(6),
                        n: 42,
                    },
                )
            })
            .expect_err("must refuse the reverse flip"),
        "Writer::put",
    );
}

/// Exists only to open `Ticket::TABLE` ("test.ticket") through the
/// UNSCOPED door once an ordinary scoped `put` has physically created it
/// composite-keyed — what
/// `view_scan_by_reaches_the_reverse_flip_through_a_crafted_scope_collision`
/// needs. `index_table_name` keys the physical index table purely off
/// `R::TABLE` and `index.name`, never the Rust type
/// (`crates/charpente-core/src/table.rs`'s `index_table_name`), so this
/// type's `by_n` index reads the exact same physical index table `Ticket`'s
/// `by_n` index writes. `decode_row` is never reached in that test: the
/// primary-table open fails (by design — that is the assertion) before any
/// bytes are decoded. `#[derive(Debug)]` for the same reason `Ticket`/
/// `Widget` need it: `Result::expect_err` requires the `Ok` side
/// (`Vec<TicketViaUnscopedDoor>`) to be `Debug`.
#[derive(Debug)]
struct TicketViaUnscopedDoor {
    id: Id<TicketViaUnscopedDoor>,
    n: u32,
}

impl Row for TicketViaUnscopedDoor {
    type Scope = Unscoped;
    const TABLE: &'static str = Ticket::TABLE;
    const INDEXES: &'static [IndexDef<TicketViaUnscopedDoor>] = &[IndexDef {
        name: "by_n",
        key: |t, out| t.n.encode(out),
    }];

    fn id(&self) -> Id<TicketViaUnscopedDoor> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.n.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<TicketViaUnscopedDoor> {
        let input = &mut &bytes[..];
        let id = Id::decode(input)?;
        let n = u32::decode(input)?;
        Ok(TicketViaUnscopedDoor { id, n })
    }
}

/// `View::scan_by`'s own `open_table` call is on the INDEX table
/// (`index_def`), never the primary rows table, so it cannot surface the
/// reverse-flip `TableTypeMismatch` directly — it only reaches the primary
/// table transitively, through `self.get(id)?` for an id an index-range
/// query returns. In the ORDINARY case that never happens for a genuinely
/// scoped write, because `scoped_index_entry` prefixes every entry with an
/// 8-byte big-endian `scope`: those bytes sort outside `scan_by`'s unscoped
/// range for a typical scope/key pair, so the range query returns nothing
/// and `self.get` is never called. `scoped_index_entry_layout_is_scope_be_
/// then_keylen_le_then_key_then_id_be`'s own doc comment
/// (`crates/charpente-core/src/table.rs`) already names the fragility here:
/// "writer and reader agree under a coordinated re-encoding (little-endian
/// scope, or scope moved after the key)" — i.e. this is a BYTE-LAYOUT
/// PROPERTY of the current encoding, not a seal on the door. Nothing in the
/// type system or the write path forbids a scope value whose big-endian
/// bytes happen to coincide with `index_entry`'s `[keylen_le][key]` prefix
/// for some query.
///
/// This test constructs exactly that coincidence rather than arguing it is
/// impossible (an earlier version of the sibling test's doc comment argued
/// exactly that, and was wrong — review disproved it with this same
/// construction): `key = 42u32` encodes little-endian to `[0x2A, 0x00,
/// 0x00, 0x00]`, and `keylen_le` for that 4-byte key is `4u32.to_le_bytes()
/// = [0x04, 0x00, 0x00, 0x00]`. `scan_by`'s unscoped range for querying
/// `42u32` is bounded by `[keylen_le][key][id=0..=u64::MAX]`, i.e. both
/// bounds share the leading 8 bytes `[0x04,0x00,0x00,0x00,0x2A,0x00,0x00,
/// 0x00]`. Choosing `scope = 0x0400_0000_2A00_0000` (whose `to_be_bytes()`
/// is exactly those same 8 bytes) makes ANY scoped index entry written
/// under that scope fall inside `scan_by`'s range for `42u32`, regardless
/// of the entry's own key or id bytes: the entry's 9th byte (the first byte
/// of ITS OWN `keylen_le`, which is `0x04` for any 4-byte index key) sorts
/// strictly between the range's lower bound's corresponding byte (`0x00`,
/// from id=0) and its upper bound's (`0xFF`, from id=`u64::MAX`).
///
/// The write below — `w.put(scope, &Ticket { .. })` — is an ORDINARY
/// scoped `put`, not a contrived raw write: this is exactly the disk state
/// `14863db`'s fix and this task's fix both already handle correctly at
/// every OTHER door. Reaching the primary table this way still meets the
/// same `unscoped_open_err` every other unscoped door gives, so the fix
/// holds — but that conclusion needed proving with a working construction,
/// not arguing from a false "cannot be populated" premise.
#[test]
fn view_scan_by_reaches_the_reverse_flip_through_a_crafted_scope_collision() {
    let sim = Sim::with_faults(0x7A2E_000C, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    let scope = 0x0400_0000_2A00_0000u64;
    store
        .apply(0, |w| {
            w.put(
                scope,
                &Ticket {
                    id: Id::from_raw(5),
                    n: 42,
                },
            )
        })
        .expect("an ordinary scoped put must not error");

    let snap = store.snapshot().unwrap();
    let err = snap
        .scan_by::<TicketViaUnscopedDoor>("by_n", &42u32)
        .expect_err("the crafted-scope entry must route scan_by into the reverse flip");
    let s = err.to_string();
    assert!(
        s.contains("is declared without scoped_by but its rows are keyed with a tenant scope"),
        "View::scan_by must name the reverse-flip contract, got: {s}"
    );
    assert!(
        s.contains("a table cannot lose a scope in place"),
        "View::scan_by must name the fix, got: {s}"
    );
}

/// Task 3 fix round 1, Important 2: `scan_raw`/`scan_raw_from`'s SCOPED
/// branch already routed every open failure through `scoped_open_err` (the
/// confident, named message) for any error, `TableTypeMismatch` included.
/// Their UNSCOPED branch did not match this: it routed every error,
/// `TableTypeMismatch` included, through `scan_raw_shape_error`'s vaguer
/// "neither shape opened it", which names neither the contract nor the fix.
/// So two of the twelve unscoped-open sites answered this exact
/// disagreement with a different message than the other nine and nothing
/// pinned that as deliberate — the same per-site divergence shape hard rule
/// 5 exists to close, and the one `14863db`'s own commit message calls out
/// as "exactly what let the forward gap open unnoticed."
///
/// `TableTypeMismatch` now routes through the same `unscoped_open_err`
/// every other unscoped door uses; `scan_raw_shape_error` remains only for
/// whatever else `open_table` could return there — a case this test does
/// not construct, since it is a materially different condition (genuine
/// doubt about which shape was expected, not a confirmed disagreement).
#[test]
fn scan_raw_and_scan_raw_from_unscoped_branch_name_the_reverse_flip_too() {
    let sim = Sim::with_faults(0x7A2E_000D, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| w.put_raw(Widget::TABLE, Some(10), 5, b"old"))
        .expect("a scoped put must not error");

    let snap = store.snapshot().unwrap();

    let err = snap.scan_raw(Widget::TABLE, false).unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("is declared without scoped_by but its rows are keyed with a tenant scope"),
        "scan_raw's unscoped branch must name the reverse-flip contract, got: {s}"
    );
    assert!(
        s.contains("a table cannot lose a scope in place"),
        "scan_raw's unscoped branch must name the fix, got: {s}"
    );
    assert!(
        !s.contains("neither the scoped composite-key shape"),
        "scan_raw's unscoped branch must no longer answer a TableTypeMismatch with the vague \
         both-shapes-failed message now that it has a confident answer, got: {s}"
    );

    let err2 = snap
        .scan_raw_from(Widget::TABLE, false, None, 10)
        .unwrap_err();
    let s2 = err2.to_string();
    assert!(
        s2.contains("is declared without scoped_by but its rows are keyed with a tenant scope"),
        "scan_raw_from's unscoped branch must name the reverse-flip contract, got: {s2}"
    );
    assert!(
        s2.contains("a table cannot lose a scope in place"),
        "scan_raw_from's unscoped branch must name the fix, got: {s2}"
    );
    assert!(
        !s2.contains("neither the scoped composite-key shape"),
        "scan_raw_from's unscoped branch must no longer answer a TableTypeMismatch with the \
         vague both-shapes-failed message now that it has a confident answer, got: {s2}"
    );
}

/// Task 4's central claim, and the only test that goes red when it stops
/// being true: the SCOPE reaches the stored key. A 16-byte key alone proves
/// nothing — `scoped_key(UNSCOPED, id)` is also 16 bytes — so this pins the
/// consequence of dropping the scope instead of the key's width.
///
/// The mechanism that makes the value observable (via `ticket_index_entry_survives`,
/// now backed by Task 6's `scan_by_scoped`): two tenants write the SAME id.
/// Under a correct composite key those are two distinct rows, so both index
/// entries stand.
/// Under a scope-dropping key they are one row, so tenant 20's `put` reads
/// tenant 10's row back as `old` in `replace_row` and REMOVES tenant 10's
/// index entry — a silent overwrite that no "did it error?" assertion can
/// see. Symmetrically, `Writer::delete`'s own `scoped_key` call is pinned by
/// the second half: deleting tenant 10's row must retire tenant 10's index
/// entry and leave tenant 20's alone; a scope-dropping delete finds nothing at
/// `(0, 5)`, returns its absent-row no-op, and leaves BOTH entries standing.
#[test]
fn two_tenants_rows_survive_each_other_under_the_composite_key() {
    let sim = Sim::with_faults(0x7A2E_000A, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| {
            w.put(
                10,
                &Ticket {
                    id: Id::from_raw(5),
                    n: 1,
                },
            )?;
            w.put(
                20,
                &Ticket {
                    id: Id::from_raw(5),
                    n: 2,
                },
            )
        })
        .expect("two tenants writing the same id must not error");

    let snap = store.snapshot().unwrap();
    assert!(
        ticket_index_entry_survives(&snap, 10, 1),
        "tenant 10's row must survive tenant 20's put of the same id — its index entry is gone, \
         which means the second put overwrote the first: the scope is not reaching the row key"
    );
    assert!(
        ticket_index_entry_survives(&snap, 20, 2),
        "tenant 20's own put must be indexed"
    );
    drop(snap);

    store
        .apply(1, |w| w.delete::<Ticket>(10, Id::from_raw(5)))
        .expect("deleting tenant 10's row must not error");

    let snap = store.snapshot().unwrap();
    assert!(
        !ticket_index_entry_survives(&snap, 10, 1),
        "deleting tenant 10's row must retire tenant 10's index entry — an entry left standing \
         means the delete looked for the row under a different scope and found nothing"
    );
    assert!(
        ticket_index_entry_survives(&snap, 20, 2),
        "deleting tenant 10's id 5 must NOT touch tenant 20's id 5"
    );
}

/// Hard rule 4, negative space: `Writer::put`'s required `scope` parameter
/// is asserted, not merely ignored, on an unscoped table. A caller passing a
/// nonzero scope there is a bug the bare-`u64` key path would otherwise
/// silently swallow — the write would land at plain `id`, indistinguishable
/// from `UNSCOPED`, and the caller's mistaken belief that the row is
/// isolated by tenant would go uncaught.
#[test]
#[should_panic(expected = "Writer::put: table \"charpente.jobs\" is not tenant-scoped")]
fn an_unscoped_put_with_a_nonzero_scope_panics() {
    let sim = Sim::with_faults(0x7A2E_0009, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();
    let _ = store.apply(0, |w| {
        w.put(
            7,
            &JobRow {
                id: Id::from_raw(1),
                name: "x".to_string(),
                payload: vec![],
                key: None,
                status: JobStatus::Pending,
                attempts: 0,
                due_at: 0,
                last_error: None,
                shape: None,
                scope: None,
            },
        )
    });
}

/// `Writer::delete` carries a second, separately-worded copy of the assertion
/// above; deleting it left every test green. Same hard-rule-4 reasoning, and
/// the `expected` string names `Writer::delete` specifically so this cannot
/// pass on `put`'s message instead.
#[test]
#[should_panic(expected = "Writer::delete: table \"charpente.jobs\" is not tenant-scoped")]
fn an_unscoped_delete_with_a_nonzero_scope_panics() {
    let sim = Sim::with_faults(0x7A2E_000B, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();
    // Deleting an ABSENT row: the assertion must fire before the lookup, or a
    // caller's mistaken scope is swallowed by the absent-row no-op.
    let _ = store.apply(0, |w| w.delete::<JobRow>(7, Id::from_raw(1)));
}

// ---------------------------------------------------------------- Task 5

/// Task 5 (tenant scoping)'s central claim: `Snapshot::scan_scoped` is
/// prefix-bounded to exactly one tenant. Tenant 1 writes ids 1, 3, 5 and
/// tenant 2 writes 2, 4, 6 — INTERLEAVED on purpose, so an off-by-one in
/// either range bound (`scoped_key(scope, 0)..=scoped_key(scope,
/// u64::MAX)`) would pull in a neighboring id from the other tenant rather
/// than hiding behind some other tenant's untouched, contiguous block.
#[test]
fn a_scoped_scan_cannot_see_another_tenants_rows() {
    let sim = Sim::with_faults(0x7A2E_000C, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| {
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(1),
                    n: 10,
                },
            )?;
            w.put(
                2,
                &Ticket {
                    id: Id::from_raw(2),
                    n: 20,
                },
            )?;
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(3),
                    n: 30,
                },
            )?;
            w.put(
                2,
                &Ticket {
                    id: Id::from_raw(4),
                    n: 40,
                },
            )?;
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(5),
                    n: 50,
                },
            )?;
            w.put(
                2,
                &Ticket {
                    id: Id::from_raw(6),
                    n: 60,
                },
            )
        })
        .expect("interleaved tenant writes must not error");

    let snap = store.snapshot().unwrap();
    let ids: Vec<u64> = snap
        .scan_scoped::<Ticket>(1)
        .expect("tenant 1's scan must not error")
        .into_iter()
        .map(|t| t.id.raw())
        .collect();
    assert_eq!(
        ids,
        vec![1, 3, 5],
        "tenant 1's scan must return EXACTLY its own ids, in id order — an off-by-one range \
         bound would pull in 2, 4, or 6 from tenant 2"
    );

    let ids2: Vec<u64> = snap
        .scan_scoped::<Ticket>(2)
        .expect("tenant 2's scan must not error")
        .into_iter()
        .map(|t| t.id.raw())
        .collect();
    assert_eq!(
        ids2,
        vec![2, 4, 6],
        "tenant 2's scan must return EXACTLY its own ids, in id order, symmetrically"
    );
}

/// The boundary ids the interleaving above does not exercise: 0 and
/// `u64::MAX` within a single tenant. The range is `..=` (inclusive) at
/// both ends — an exclusive upper bound would drop `u64::MAX`, and a lower
/// bound starting above 0 (e.g. `scoped_key(scope, 1)`) would drop id 0.
#[test]
fn a_scoped_scan_includes_id_zero_and_u64_max_within_one_tenant() {
    let sim = Sim::with_faults(0x7A2E_000D, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| {
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(0),
                    n: 1,
                },
            )?;
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(u64::MAX),
                    n: 2,
                },
            )
        })
        .expect("writing the id-space boundaries must not error");

    let snap = store.snapshot().unwrap();
    let ids: Vec<u64> = snap
        .scan_scoped::<Ticket>(1)
        .unwrap()
        .into_iter()
        .map(|t| t.id.raw())
        .collect();
    assert_eq!(
        ids,
        vec![0, u64::MAX],
        "the scan's range must be INCLUSIVE at both ends of the id space, in id order"
    );
}

/// The other boundary the interleaving does not exercise: adjacency between
/// the LAST id of one tenant and the FIRST id of the next. `scoped_key(1,
/// u64::MAX)` and `scoped_key(2, 0)` sort immediately next to each other
/// with nothing between them (`scoped_key_tests` in `table.rs` pins that
/// ordering) — this pins that the READ side never crosses it either.
#[test]
fn a_scoped_scan_stays_inside_its_tenant_at_the_composite_key_boundary() {
    let sim = Sim::with_faults(0x7A2E_000E, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| {
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(u64::MAX),
                    n: 1,
                },
            )?;
            w.put(
                2,
                &Ticket {
                    id: Id::from_raw(0),
                    n: 2,
                },
            )
        })
        .expect("adjacent-boundary tenant writes must not error");

    let snap = store.snapshot().unwrap();
    let tenant1: Vec<u64> = snap
        .scan_scoped::<Ticket>(1)
        .unwrap()
        .into_iter()
        .map(|t| t.id.raw())
        .collect();
    let tenant2: Vec<u64> = snap
        .scan_scoped::<Ticket>(2)
        .unwrap()
        .into_iter()
        .map(|t| t.id.raw())
        .collect();
    assert_eq!(
        tenant1,
        vec![u64::MAX],
        "tenant 1's scan must see its own u64::MAX row and nothing from tenant 2"
    );
    assert_eq!(
        tenant2,
        vec![0],
        "tenant 2's scan must see its own id-0 row and nothing from tenant 1"
    );
}

/// `Snapshot::get_scoped`: two tenants holding the SAME id must not read
/// back each other's row, and a tenant that never wrote that id sees
/// nothing (not a stale row from a third tenant's write).
#[test]
fn a_scoped_get_cannot_see_another_tenants_row_at_the_same_id() {
    let sim = Sim::with_faults(0x7A2E_000F, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| {
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(5),
                    n: 111,
                },
            )?;
            w.put(
                2,
                &Ticket {
                    id: Id::from_raw(5),
                    n: 222,
                },
            )
        })
        .expect("two tenants writing the same id must not error");

    let snap = store.snapshot().unwrap();
    let t1 = snap
        .get_scoped::<Ticket>(1, Id::from_raw(5))
        .unwrap()
        .expect("tenant 1's row must exist");
    let t2 = snap
        .get_scoped::<Ticket>(2, Id::from_raw(5))
        .unwrap()
        .expect("tenant 2's row must exist");
    assert_eq!(
        t1.n, 111,
        "get_scoped(1, 5) must return tenant 1's row, not tenant 2's"
    );
    assert_eq!(
        t2.n, 222,
        "get_scoped(2, 5) must return tenant 2's row, not tenant 1's"
    );
    assert!(
        snap.get_scoped::<Ticket>(3, Id::from_raw(5))
            .unwrap()
            .is_none(),
        "a third tenant that never wrote id 5 must see nothing"
    );
}

/// `Snapshot::scan_from_scoped`: bounded by BOTH the tenant (never see
/// another tenant's rows regardless of cursor) and the cursor (never see
/// this tenant's own ids below `from_id`).
#[test]
fn a_scoped_scan_from_is_bounded_to_its_tenant_and_its_cursor() {
    let sim = Sim::with_faults(0x7A2E_0010, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| {
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(1),
                    n: 1,
                },
            )?;
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(3),
                    n: 3,
                },
            )?;
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(5),
                    n: 5,
                },
            )?;
            w.put(
                2,
                &Ticket {
                    id: Id::from_raw(2),
                    n: 2,
                },
            )?;
            w.put(
                2,
                &Ticket {
                    id: Id::from_raw(4),
                    n: 4,
                },
            )
        })
        .expect("setup writes must not error");

    let snap = store.snapshot().unwrap();
    let ids: Vec<u64> = snap
        .scan_from_scoped::<Ticket>(1, 3, 10)
        .expect("scan_from_scoped must not error")
        .into_iter()
        .map(|t| t.id.raw())
        .collect();
    assert_eq!(
        ids,
        vec![3, 5],
        "scan_from_scoped(scope=1, from_id=3) must return tenant 1's ids >= 3 — id 1 is \
         excluded by the cursor, and tenant 2's 2/4 must never appear regardless of the cursor"
    );
}

/// `scan_from_scoped`'s upper bound is INCLUSIVE (`..=scoped_key(scope,
/// u64::MAX)`), and `u64::MAX` is the only id where `..=` and `..` diverge —
/// a row there is visible only if the range is closed, not half-open.
#[test]
fn a_scoped_scan_from_includes_the_row_at_u64_max() {
    let sim = Sim::with_faults(0x7A2E_0015, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| {
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(5),
                    n: 5,
                },
            )?;
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(u64::MAX - 1),
                    n: 1,
                },
            )?;
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(u64::MAX),
                    n: 2,
                },
            )?;
            w.put(
                2,
                &Ticket {
                    id: Id::from_raw(u64::MAX),
                    n: 3,
                },
            )
        })
        .expect("setup writes must not error");

    let snap = store.snapshot().unwrap();
    let ids: Vec<u64> = snap
        .scan_from_scoped::<Ticket>(1, u64::MAX - 1, 10)
        .expect("scan_from_scoped must not error")
        .into_iter()
        .map(|t| t.id.raw())
        .collect();
    assert_eq!(
        ids,
        vec![u64::MAX - 1, u64::MAX],
        "scan_from_scoped's upper bound must be INCLUSIVE of u64::MAX, not stop short of it"
    );

    let ids_at_max: Vec<u64> = snap
        .scan_from_scoped::<Ticket>(1, u64::MAX, 10)
        .expect("scan_from_scoped must not error")
        .into_iter()
        .map(|t| t.id.raw())
        .collect();
    assert_eq!(
        ids_at_max,
        vec![u64::MAX],
        "a cursor positioned AT u64::MAX must still see the row there"
    );
}

/// `Snapshot::scan_raw_from` (the migration/archive-export path) must learn
/// the composite `[u8; 16]` key shape rather than erroring with
/// `TableTypeMismatch`, and must see every tenant's rows, in `(scope, id)`
/// order — Task 6c's fix returns the scope alongside each row, so this now
/// pins BOTH: which tenant a row belongs to, and the real ascending order.
///
/// Tenant 10 writes id 5, tenant 20 writes id 1 — chosen so `(scope, id)`
/// order and bare `id` order DISAGREE. Composite order sorts by scope
/// first, so the bare ids still come back `[5, 1]` — not because ids are
/// unordered (Task 6c's real seek fixed that), but because scope 10 sorts
/// before scope 20 regardless of which id each tenant chose.
#[test]
fn scan_raw_from_learns_the_composite_key_shape_and_sees_every_tenant() {
    let sim = Sim::with_faults(0x7A2E_0011, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| {
            w.put(
                10,
                &Ticket {
                    id: Id::from_raw(5),
                    n: 100,
                },
            )?;
            w.put(
                20,
                &Ticket {
                    id: Id::from_raw(1),
                    n: 200,
                },
            )
        })
        .expect("two tenants writing distinct ids on a scoped table must not error");

    let snap = store.snapshot().unwrap();
    let rows = snap.scan_raw_from(Ticket::TABLE, true, None, 10).expect(
        "scan_raw_from must learn the [u8;16] key shape rather than erroring with \
         TableTypeMismatch — migrate_schema() has to be able to walk a scoped table too",
    );
    let scopes_and_ids: Vec<(u64, u64)> = rows.iter().map(|(scope, id, _)| (*scope, *id)).collect();
    assert_eq!(
        scopes_and_ids,
        vec![(10, 5), (20, 1)],
        "scan_raw_from must walk the composite key in ascending (scope, id) order and return \
         each row's real scope, not UNSCOPED"
    );
}

/// Hard rule 4, negative space: each of the three scope-taking read
/// primitives asserts `row_is_scoped::<R>()` rather than silently opening
/// the WRONG physical redb table (`scoped_rows_def`, which the unscoped
/// write path never creates) and reading back an empty, misleadingly
/// "successful" result for a table that actually has rows.
#[test]
#[should_panic(
    expected = "Snapshot::get_scoped: table \"charpente.jobs\" is not tenant-scoped \
                (Row::Scope = Unscoped) — use View::get for an unscoped table"
)]
fn get_scoped_on_an_unscoped_table_panics() {
    let sim = Sim::with_faults(0x7A2E_0012, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();
    let snap = store.snapshot().unwrap();
    let _ = snap.get_scoped::<JobRow>(0, Id::from_raw(1));
}

/// See `get_scoped_on_an_unscoped_table_panics`; same reasoning, `scan_scoped`.
#[test]
#[should_panic(
    expected = "Snapshot::scan_scoped: table \"charpente.jobs\" is not tenant-scoped \
                (Row::Scope = Unscoped) — use View::scan for an unscoped table"
)]
fn scan_scoped_on_an_unscoped_table_panics() {
    let sim = Sim::with_faults(0x7A2E_0013, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();
    let snap = store.snapshot().unwrap();
    let _ = snap.scan_scoped::<JobRow>(0);
}

/// See `get_scoped_on_an_unscoped_table_panics`; same reasoning, `scan_from_scoped`.
#[test]
#[should_panic(
    expected = "Snapshot::scan_from_scoped: table \"charpente.jobs\" is not tenant-scoped \
                (Row::Scope = Unscoped) — use View::scan_from for an unscoped table"
)]
fn scan_from_scoped_on_an_unscoped_table_panics() {
    let sim = Sim::with_faults(0x7A2E_0014, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();
    let snap = store.snapshot().unwrap();
    let _ = snap.scan_from_scoped::<JobRow>(0, 0, 10);
}

/// Task 6 (tenant scoping)'s central claim, and the leak this task closes:
/// a scoped index lookup must not cross tenants — even when both tenants
/// index the SAME key value. Distinct keys would pass by accident with no
/// scope prefix at all (each tenant's row would be the only match for its
/// own key); the SAME key is the case that actually exercises the prefix.
#[test]
fn a_scoped_index_lookup_cannot_see_another_tenants_rows() {
    let sim = Sim::with_faults(0x7A2E_0016, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| {
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(5),
                    n: 42,
                },
            )?;
            w.put(
                2,
                &Ticket {
                    id: Id::from_raw(6),
                    n: 42,
                },
            )
        })
        .expect("two tenants indexing the same key must not error");

    let snap = store.snapshot().unwrap();
    let tenant1: Vec<Id<Ticket>> = snap
        .scan_by_scoped::<Ticket>(1, "by_n", &42u32)
        .expect("scan_by_scoped must not error")
        .iter()
        .map(|t| t.id)
        .collect();
    assert_eq!(
        tenant1,
        vec![Id::from_raw(5)],
        "scan_by_scoped(scope=1, \"by_n\", 42) must return exactly tenant 1's row, not tenant \
         2's"
    );

    let tenant2: Vec<Id<Ticket>> = snap
        .scan_by_scoped::<Ticket>(2, "by_n", &42u32)
        .expect("scan_by_scoped must not error")
        .iter()
        .map(|t| t.id)
        .collect();
    assert_eq!(
        tenant2,
        vec![Id::from_raw(6)],
        "scan_by_scoped(scope=2, \"by_n\", 42) must return exactly tenant 2's row, not tenant \
         1's"
    );
}

/// A hand-written UNSCOPED `Row` with one secondary index — the
/// byte-identity fixture for `an_unscoped_index_entry_stays_byte_identical`,
/// and also the unscoped fixture `every_unscoped_entry_point_refuses_the_
/// reverse_flip_by_name` reads back through `View::get`/`scan`/`scan_from`
/// (hence `#[derive(Debug)]`: `Result::expect_err` needs the Ok side to be
/// `Debug`). Needs its own type because core's other hand-written `Row`
/// impls (`JobRow`, `EventRow`) both declare `INDEXES: &[]`.
#[derive(Debug)]
struct Widget {
    id: Id<Widget>,
    n: u32,
}

impl Row for Widget {
    type Scope = Unscoped;
    const TABLE: &'static str = "test.widget";
    const INDEXES: &'static [IndexDef<Widget>] = &[IndexDef {
        name: "by_n",
        key: |w, out| w.n.encode(out),
    }];

    fn id(&self) -> Id<Widget> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.n.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<Widget> {
        let input = &mut &bytes[..];
        let id = Id::decode(input)?;
        let n = u32::decode(input)?;
        Ok(Widget { id, n })
    }
}

/// Task 6 (tenant scoping): the index-entry counterpart to
/// `an_unscoped_table_still_writes_a_bare_u64_key` — an unscoped table's
/// index entries must stay byte-identical to before the scope prefix
/// existed: `[key len: u32 le][key bytes][id: u64 be]`, no scope prefix.
/// Every pinned simulation seed depends on these bytes.
///
/// Reads the entry through `scan_index_raw`, the raw on-disk bytes — not a
/// round trip through `scan_by`, which decodes a row back out via `get` and
/// never looks at the key that pointed at it (same reasoning as
/// `an_unscoped_table_still_writes_a_bare_u64_key`'s own doc comment).
#[test]
fn an_unscoped_index_entry_stays_byte_identical() {
    let sim = Sim::with_faults(0x7A2E_0017, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| {
            w.put(
                UNSCOPED,
                &Widget {
                    id: Id::from_raw(9),
                    n: 42,
                },
            )
        })
        .unwrap();

    let mut expected = 4u32.to_le_bytes().to_vec(); // keylen: the u32 key is 4 bytes
    expected.extend_from_slice(&42u32.to_le_bytes()); // key bytes, u32's own LE codec
    expected.extend_from_slice(&9u64.to_be_bytes()); // id, big-endian

    let snap = store.snapshot().unwrap();
    let raw = snap.scan_index_raw::<Widget>("by_n").unwrap();
    assert_eq!(
        raw,
        vec![expected],
        "an unscoped table's index entry must stay [keylen u32 le][key][id u64 be], with no \
         scope prefix"
    );
}

/// See `get_scoped_on_an_unscoped_table_panics`; same reasoning,
/// `scan_by_scoped`.
#[test]
#[should_panic(
    expected = "Snapshot::scan_by_scoped: table \"charpente.jobs\" is not tenant-scoped \
                (Row::Scope = Unscoped) — use View::scan_by for an unscoped table"
)]
fn scan_by_scoped_on_an_unscoped_table_panics() {
    let sim = Sim::with_faults(0x7A2E_0018, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();
    let snap = store.snapshot().unwrap();
    let _ = snap.scan_by_scoped::<JobRow>(0, "by_status", &0u32);
}

/// `Writer::put`'s old-entry retirement must carry the scope too — an
/// update (`put` over an existing scoped row) must retire the OLD entry
/// under the CORRECT tenant only, never spilling into (or missing) another
/// tenant's row that happens to share the same old key value.
#[test]
fn a_scoped_put_over_an_existing_row_retires_the_old_entry_under_the_right_scope() {
    let sim = Sim::with_faults(0x7A2E_0019, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| {
            // Tenant 1's ticket starts at n=1. Tenant 2 independently holds
            // a DIFFERENT row already indexed at n=1 too — the value tenant
            // 1 is about to retire, under a different scope.
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(5),
                    n: 1,
                },
            )?;
            w.put(
                2,
                &Ticket {
                    id: Id::from_raw(6),
                    n: 1,
                },
            )
        })
        .expect("initial puts must not error");

    store
        .apply(1, |w| {
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(5),
                    n: 2,
                },
            )
        })
        .expect("the update put must not error");

    let snap = store.snapshot().unwrap();
    assert!(
        !ticket_index_entry_survives(&snap, 1, 1),
        "tenant 1's OLD index entry (n=1) must be retired by the update put"
    );
    assert_eq!(
        snap.scan_by_scoped::<Ticket>(1, "by_n", &2u32)
            .expect("scan_by_scoped must not error")
            .iter()
            .map(|t| t.id)
            .collect::<Vec<_>>(),
        vec![Id::from_raw(5)],
        "tenant 1's NEW index entry (n=2) must be present after the update put"
    );
    assert!(
        ticket_index_entry_survives(&snap, 2, 1),
        "tenant 1's update must NOT touch tenant 2's n=1 entry — a different row, under a \
         different scope"
    );
}

/// `Writer::delete`'s index-entry removal must carry the scope too: two
/// tenants indexing the SAME key, deleting one must retire only that
/// tenant's entry and leave the other tenant's standing.
#[test]
fn a_scoped_delete_retires_only_that_tenants_index_entry() {
    let sim = Sim::with_faults(0x7A2E_001A, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| {
            // SAME id, not just the same key: a wrong-scope removal that
            // happens to land on tenant 2's entry needs tenant 2's entry to
            // actually be at the bytes the mutated removal would compute
            // (scope, key, id) — a mismatched id would make that removal a
            // harmless no-op instead of a cross-tenant destructive write.
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(5),
                    n: 42,
                },
            )?;
            w.put(
                2,
                &Ticket {
                    id: Id::from_raw(5),
                    n: 42,
                },
            )
        })
        .expect("two tenants indexing the same key at the same id must not error");

    store
        .apply(1, |w| w.delete::<Ticket>(1, Id::from_raw(5)))
        .expect("deleting tenant 1's row must not error");

    let snap = store.snapshot().unwrap();
    assert!(
        !ticket_index_entry_survives(&snap, 1, 42),
        "deleting tenant 1's row must retire tenant 1's index entry"
    );
    assert!(
        ticket_index_entry_survives(&snap, 2, 42),
        "deleting tenant 1's row must NOT touch tenant 2's index entry for the same key"
    );
}

/// `scan_by_scoped`'s id bound is INCLUSIVE on both ends — `0` and
/// `u64::MAX` are real, reachable row ids, not sentinels, so excluding
/// either would silently drop a legitimate row from an index lookup. Same
/// reasoning as `scan_from_scoped`'s own inclusive-upper-bound test
/// (`a_scoped_scan_from_includes_the_row_at_u64_max`, above).
#[test]
fn scan_by_scoped_bounds_are_inclusive_of_id_0_and_id_max() {
    let sim = Sim::with_faults(0x7A2E_001B, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| {
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(0),
                    n: 9,
                },
            )?;
            w.put(
                1,
                &Ticket {
                    id: Id::from_raw(u64::MAX),
                    n: 9,
                },
            )
        })
        .expect("puts at the id extremes must not error");

    let snap = store.snapshot().unwrap();
    let mut ids: Vec<u64> = snap
        .scan_by_scoped::<Ticket>(1, "by_n", &9u32)
        .expect("scan_by_scoped must not error")
        .iter()
        .map(|t| t.id.raw())
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![0, u64::MAX],
        "scan_by_scoped's id bound must include both id 0 and id u64::MAX"
    );
}

// ------------------------------------------------------ Task 6b: WAL scope

/// Task 6b's central claim, inverted from its own tripwire: before the WAL
/// record carried a scope, registering `Ticket` (a scoped `Row`) and writing
/// through `Db::transact` panicked on db.rs's R25 assert at apply (the R25
/// assert's exact message, pinned while it was proven real:
/// `"Schema::table: \"test.ticket\" is tenant-scoped (Row::Scope != \
/// Unscoped), but Effect::Put carries no scope field (v0 record format), \
/// so db.rs's apply_put would replay every tenant's rows under scope 0 and \
/// merge them — registering a scoped table is blocked until \
/// tenant-scoping Task 6b adds the scope to the record and lifts this \
/// assertion"`). Now that the record's `0x05`/`0x06` tags carry the scope,
/// the same write survives a crash-restart replay under its own scope.
///
/// Written at scope 0 through `ScopedTx::put`, the only scope-taking write
/// an app has — R35's tenant-0 pin: 0 is a legal
/// tenant for a scoped table, not "no scope", so this must replay through
/// the SCOPED tag (`0x05`), which `Db::open`'s successful return already
/// implies: `resolve_effect_scope` refuses `Db::open` outright had the
/// record instead carried the unscoped `0x00` tag for this scoped table
/// (`db::tests::resolve_effect_scope_refuses_a_scoped_table_with_an_
/// unscoped_tag` pins that refusal at the unit level).
#[test]
fn a_scoped_write_survives_crash_restart_replay_under_its_own_scope() {
    let sim = Sim::with_faults(0x6B00_0001, FaultConfig::none());
    let cfg = DbConfig::default();
    let schema = || Schema::new().table::<Ticket>();

    let mut db = Db::open(Box::new(sim.disk()), cfg.clone(), schema()).unwrap();
    db.transact(|tx| -> Result<(), io::Error> {
        tx.open_scope(Id::<TestOrg>::from_raw(0), "member")?
            .put(&Ticket {
                id: Id::from_raw(5),
                n: 42,
            });
        Ok(())
    })
    .unwrap();

    // Crash-restart, not a clean shutdown: no checkpoint before the
    // restart, so reopening replays the record from the WAL rather than
    // reading it back from the already-materialized store.
    sim.restart();
    drop(db);
    let db2 = Db::open(Box::new(sim.disk()), cfg, schema()).unwrap();

    let snap = db2.snapshot().unwrap();
    let row = snap
        .get_scoped::<Ticket>(0, Id::from_raw(5))
        .expect("get_scoped must not error for a scoped table")
        .expect(
            "the scoped row must survive crash-restart replay under scope 0 — tenant 0 is a \
             legal tenant (R35), not \"no tenant\"",
        );
    assert_eq!(row.n, 42);
}

/// The other replay path Task 6b's format change must cover: a follower's
/// `apply_shipped` (DESIGN §5's write path for every non-primary node) is
/// the exact same `TableMachine::apply_batch` a crash-restart replays
/// through, so a scoped row shipped from a primary lands under its own
/// scope on the follower too — never collapsed to scope 0 of a different
/// (unscoped) definition. Drives `apply_shipped` directly rather than
/// through the wire protocol, same idiom `charpente/tests/replication.rs`'s
/// `a_shipped_batch_that_fails_mid_apply_rolls_back_whole` uses.
#[test]
fn a_followers_apply_shipped_lands_a_scoped_write_under_its_own_scope() {
    let sim = Sim::with_faults(0x6B00_0002, FaultConfig::none());
    let schema = || Schema::new().table::<Ticket>();

    let mut primary = Db::open(Box::new(sim.disk()), DbConfig::default(), schema()).unwrap();
    primary
        .transact(|tx| -> Result<(), io::Error> {
            tx.open_scope(Id::<TestOrg>::from_raw(0), "member")?
                .put(&Ticket {
                    id: Id::from_raw(9),
                    n: 7,
                });
            Ok(())
        })
        .unwrap();
    let frames = primary.read_log(0, usize::MAX).unwrap();
    assert_eq!(frames.len(), 1);

    let mut follower = Db::open(
        Box::new(sim.host("follower").disk()),
        DbConfig::default(),
        schema(),
    )
    .unwrap();
    follower.apply_shipped(&frames).unwrap();

    let row = follower
        .snapshot()
        .unwrap()
        .get_scoped::<Ticket>(0, Id::from_raw(9))
        .expect("get_scoped must not error for a scoped table")
        .expect("a scoped write shipped to a follower must land under its own scope (0)");
    assert_eq!(row.n, 7);
}

/// `[len: u32 le][bytes]` — the length-prefixed shape `db.rs`'s own
/// `encode_bytes` produces, for hand-encoding record bytes by hand below.
fn encode_len_prefixed(bytes: &[u8], out: &mut Vec<u8>) {
    (bytes.len() as u32).encode(out);
    out.extend_from_slice(bytes);
}

/// A hand-encoded one-effect `0x05` (scoped Put) record for `Ticket` — same
/// wire shape `db.rs`'s own `encode_record` produces, built by hand because
/// `Tx::put` cannot express a nonzero scope yet (Task 7).
fn scoped_put_record(scope: u64, id: u64, n: u32) -> Vec<u8> {
    let mut row_bytes = Vec::new();
    Ticket {
        id: Id::from_raw(id),
        n,
    }
    .encode_row(&mut row_bytes);
    let mut record = Vec::new();
    1u32.encode(&mut record); // n = 1 effect
    record.push(5); // tag 0x05: scoped Put
    encode_len_prefixed(Ticket::TABLE.as_bytes(), &mut record);
    scope.encode(&mut record);
    encode_len_prefixed(&row_bytes, &mut record);
    record
}

/// A hand-encoded one-effect `0x06` (scoped Delete) record for `Ticket`.
fn scoped_delete_record(scope: u64, id: u64) -> Vec<u8> {
    let mut record = Vec::new();
    1u32.encode(&mut record); // n = 1 effect
    record.push(6); // tag 0x06: scoped Delete
    encode_len_prefixed(Ticket::TABLE.as_bytes(), &mut record);
    scope.encode(&mut record);
    id.encode(&mut record);
    record
}

/// Every reachable write today lands at scope 0 (`Tx::put` always uses the
/// overlay's default slot; Task 7's `tx.scope()` is what will let a caller
/// choose otherwise), so no test above can tell a correctly-threaded scope
/// apart from `apply_put` silently substituting `table::UNSCOPED` for it —
/// both are the value 0. This test closes that gap by hand-encoding a
/// `0x05` (scoped Put) record at a genuinely nonzero scope, the same way
/// `charpente/tests/replication.rs`'s atomicity tests hand-encode a bad tag,
/// and driving it through the real `apply_shipped` path: the row must land
/// under scope 7, and must NOT be visible under scope 0.
#[test]
fn a_hand_encoded_scoped_record_at_a_nonzero_scope_lands_under_that_scope() {
    let sim = Sim::with_faults(0x6B00_0003, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Ticket>(),
    )
    .unwrap();
    db.apply_shipped(&[(0, scoped_put_record(7, 3, 99))])
        .unwrap();

    let snap = db.snapshot().unwrap();
    assert!(
        snap.get_scoped::<Ticket>(7, Id::from_raw(3))
            .unwrap()
            .is_some(),
        "a hand-encoded scope-7 record must land under scope 7"
    );
    assert!(
        snap.get_scoped::<Ticket>(0, Id::from_raw(3))
            .unwrap()
            .is_none(),
        "a scope-7 write must not be visible under scope 0 — apply_put must use the decoded \
         scope, not silently substitute table::UNSCOPED for it"
    );
}

/// `apply_delete`'s own counterpart to the Put test above, same reasoning:
/// a scope-7 hand-encoded `0x06` delete must retire the scope-7 row, which
/// `Writer::delete`'s absent-row no-op makes an oracle by itself — if
/// `apply_delete` silently substituted `table::UNSCOPED` for the decoded
/// scope, it would look for (and find nothing at) scope 0 and quietly no-op,
/// leaving the scope-7 row standing.
#[test]
fn a_hand_encoded_scoped_delete_at_a_nonzero_scope_retires_that_scopes_row() {
    let sim = Sim::with_faults(0x6B00_0005, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Ticket>(),
    )
    .unwrap();
    db.apply_shipped(&[(0, scoped_put_record(7, 3, 99))])
        .unwrap();
    db.apply_shipped(&[(1, scoped_delete_record(7, 3))])
        .unwrap();

    let snap = db.snapshot().unwrap();
    assert!(
        snap.get_scoped::<Ticket>(7, Id::from_raw(3))
            .unwrap()
            .is_none(),
        "a hand-encoded scope-7 delete must retire the scope-7 row — apply_delete must use \
         the decoded scope, not silently substitute table::UNSCOPED for it"
    );
}

// ------------------------------------------ fix round 1: below-compiled scope

/// A scoped, VERSIONED `Row` — `Ticket` above has `FINGERPRINT: None` (the
/// default for a hand-written type), which opts it out of the versioning
/// gate entirely (`is_below_compiled` always returns `false` for it). The
/// below-compiled-version replay branches below need a table that IS
/// subject to the gate, so this sets `VERSION: 2`/`FINGERPRINT: Some(_)`
/// and reuses `Ticket`'s own `TestOrg` scope marker.
struct VersionedTicket {
    id: Id<VersionedTicket>,
    n: u32,
}

impl Row for VersionedTicket {
    type Scope = TestOrg;
    const TABLE: &'static str = "test.versioned_ticket";
    const INDEXES: &'static [IndexDef<VersionedTicket>] = &[];
    const VERSION: u32 = 2;
    const FINGERPRINT: Option<u64> = Some(0x6B00_F1F1);

    fn id(&self) -> Id<VersionedTicket> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.n.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<VersionedTicket> {
        let input = &mut &bytes[..];
        Ok(VersionedTicket {
            id: Id::decode(input)?,
            n: u32::decode(input)?,
        })
    }
}

/// A hand-encoded unscoped (`0x00`) Put record for `SchemaVersion`,
/// declaring `table`'s persisted version — what seeds the replay-time
/// version map (`db.rs`'s `versions`) so a later record for `table` takes
/// the decode-free `is_below_compiled` branch instead of the ordinary one.
fn schema_version_put_record(table: &str, version: u32, fingerprint: u64) -> Vec<u8> {
    let row = SchemaVersion {
        id: schema_version_id(table),
        table: table.to_string(),
        version,
        fingerprint,
        migrating_through: None,
        migrating_to: None,
    };
    let mut row_bytes = Vec::new();
    row.encode_row(&mut row_bytes);
    let mut record = Vec::new();
    1u32.encode(&mut record); // n = 1 effect
    record.push(0); // tag 0x00: SchemaVersion is itself unscoped
    encode_len_prefixed(SchemaVersion::TABLE.as_bytes(), &mut record);
    encode_len_prefixed(&row_bytes, &mut record);
    record
}

/// A hand-encoded one-effect `0x05` (scoped Put) record for `VersionedTicket`.
fn versioned_scoped_put_record(scope: u64, id: u64, n: u32) -> Vec<u8> {
    let mut row_bytes = Vec::new();
    VersionedTicket {
        id: Id::from_raw(id),
        n,
    }
    .encode_row(&mut row_bytes);
    let mut record = Vec::new();
    1u32.encode(&mut record); // n = 1 effect
    record.push(5); // tag 0x05: scoped Put
    encode_len_prefixed(VersionedTicket::TABLE.as_bytes(), &mut record);
    scope.encode(&mut record);
    encode_len_prefixed(&row_bytes, &mut record);
    record
}

/// A hand-encoded one-effect `0x06` (scoped Delete) record for `VersionedTicket`.
fn versioned_scoped_delete_record(scope: u64, id: u64) -> Vec<u8> {
    let mut record = Vec::new();
    1u32.encode(&mut record); // n = 1 effect
    record.push(6); // tag 0x06: scoped Delete
    encode_len_prefixed(VersionedTicket::TABLE.as_bytes(), &mut record);
    scope.encode(&mut record);
    id.encode(&mut record);
    record
}

/// Fix round 1, Important 1: `apply_batch`'s `is_below_compiled` branches (a
/// full rebuild-from-WAL replaying a table's pre-migration history) apply
/// DECODE-FREE via `put_raw`, which now carries the record's resolved
/// scope. A below-compiled scoped Put must land under its own scope, not
/// panic and not silently collapse to scope 0 — asserted on the stored
/// composite key via `scan_raw`, not a round trip through the same encoder.
#[test]
fn a_below_compiled_scoped_put_replays_under_its_own_scope() {
    let sim = Sim::with_faults(0x6B00_0006, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<VersionedTicket>(),
    )
    .unwrap();
    // Declares VersionedTicket's persisted version (1) behind its compiled
    // one (2), so the next record for it takes the decode-free branch.
    db.apply_shipped(&[(
        0,
        schema_version_put_record(VersionedTicket::TABLE, 1, 0x6B00_F1F1),
    )])
    .unwrap();

    db.apply_shipped(&[(1, versioned_scoped_put_record(7, 3, 99))])
        .unwrap();

    let mut expected_bytes = Vec::new();
    VersionedTicket {
        id: Id::from_raw(3),
        n: 99,
    }
    .encode_row(&mut expected_bytes);
    let stored = db
        .snapshot()
        .unwrap()
        .scan_raw(VersionedTicket::TABLE, true)
        .unwrap();
    assert_eq!(
        stored,
        vec![(7u64, 3u64, expected_bytes)],
        "a below-compiled scoped Put must land under its record's own scope (7), not 0"
    );
}

/// See the Put test's doc comment; this pins `delete_raw`'s identical path.
#[test]
fn a_below_compiled_scoped_delete_replays_under_its_own_scope() {
    let sim = Sim::with_faults(0x6B00_0007, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<VersionedTicket>(),
    )
    .unwrap();
    db.apply_shipped(&[(
        0,
        schema_version_put_record(VersionedTicket::TABLE, 1, 0x6B00_F1F1),
    )])
    .unwrap();
    // A second tenant's row at the same id must survive the first tenant's
    // below-compiled delete — proves the delete is scoped, not table-wide.
    db.apply_shipped(&[(1, versioned_scoped_put_record(9, 3, 1))])
        .unwrap();

    db.apply_shipped(&[(2, versioned_scoped_delete_record(7, 3))])
        .unwrap();

    let mut survivor_bytes = Vec::new();
    VersionedTicket {
        id: Id::from_raw(3),
        n: 1,
    }
    .encode_row(&mut survivor_bytes);
    let stored = db
        .snapshot()
        .unwrap()
        .scan_raw(VersionedTicket::TABLE, true)
        .unwrap();
    assert_eq!(
        stored,
        vec![(9u64, 3u64, survivor_bytes)],
        "a below-compiled scoped Delete at scope 7 must not touch scope 9's row at the same id"
    );
}

/// Task 6c tripwire (must FAIL FIRST against stored bytes, per the task
/// brief's Step 2): tenant 2 writes id 7 on a scoped table. Export via
/// `Snapshot::scan_raw`, re-import via `Seeder::rows`, then read the
/// follower's stored key bytes directly through `Snapshot::scan_raw` again
/// — never a round trip through the same broken encoder, which would pass
/// while both ends are wrong. Before this task: `Seeder::rows` refused a
/// scoped table by name (the guard `seeder_rows_refuses_a_scoped_table_by_
/// name` used to pin, fix round 1 Minor 7) — this test proves the
/// replacement behavior, that the row survives seeding with scope 2 intact,
/// not scope 0.
#[test]
fn a_scoped_row_survives_a_snapshot_seed_with_its_scope_intact() {
    let sim = Sim::with_faults(0x6B00_0008, FaultConfig::none());

    let primary = Store::open(&sim.disk(), "primary.redb").unwrap();
    primary
        .apply(0, |w| {
            w.put(
                2,
                &Ticket {
                    id: Id::from_raw(7),
                    n: 42,
                },
            )
        })
        .unwrap();
    let exported = primary
        .snapshot()
        .unwrap()
        .scan_raw(Ticket::TABLE, true)
        .unwrap();
    assert_eq!(
        exported,
        vec![(2u64, 7u64, {
            let mut b = Vec::new();
            Ticket {
                id: Id::from_raw(7),
                n: 42,
            }
            .encode_row(&mut b);
            b
        })],
        "sanity: the primary's own export must already carry scope 2"
    );

    let mut seeder = Seeder::new(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Ticket>(),
    )
    .unwrap();
    seeder.rows(Ticket::TABLE, &exported).unwrap();
    let db = seeder.finish(0, 0, 0, 0).unwrap();

    let imported = db
        .snapshot()
        .unwrap()
        .scan_raw(Ticket::TABLE, true)
        .unwrap();
    assert_eq!(
        imported, exported,
        "the follower's stored bytes must carry tenant 2's scope, not scope 0 — a bug here \
         would either misdecode with TableTypeMismatch on export or land the row under scope \
         0 on import (Seeder::rows discarding the scope, or apply_put with no scope source)"
    );
}

// ------------------------------------------------- Task 7: the group overlay

/// Tasks 5 and 6 bound the SNAPSHOT ranges; they say nothing about the
/// overlay, and `Tx::group` is the BATCH-level overlay shared by every
/// transaction in the batch — so it holds other tenants' staged rows. Merged
/// by bare id (the shape every read loop had before Task 7), tenant B's staged
/// id 7 overwrites tenant A's id 7 inside A's own scan, and the call that
/// returns it is `ScopedTx<A>::scan`: endorsed by the types, invisible to any
/// single-tenant test, which is why this one drives two tenants through ONE
/// `Group`.
///
/// Three staged transactions, not two, because a transaction opens ONE scope
/// (spec §8): the cross-tenant read has to come from a third transaction that
/// opened A, by which point BOTH tenants' rows are in the group overlay — the
/// exact state the merge conflates.
///
/// `get` is asserted alongside the scans deliberately even though it pins
/// `(scope, id)` exactly and was never leaky: `get`'s exactness and the scans'
/// merge are one change with two different wrong behaviours, so both belong in
/// the pin.
#[test]
fn one_tenants_staged_row_is_invisible_to_another_in_the_same_batch() {
    let sim = Sim::with_faults(0x7000_0001, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Ticket>(),
    )
    .unwrap();
    let a = Id::<TestOrg>::from_raw(1);
    let b = Id::<TestOrg>::from_raw(2);

    let mut group = db.begin_group().unwrap();
    group
        .stage(|tx| -> Result<(), io::Error> {
            tx.open_scope(a, "member")?.put(&Ticket {
                id: Id::from_raw(7),
                n: 11,
            });
            Ok(())
        })
        .unwrap();
    // A second transaction in the SAME batch: tenant A's row 7 reaches this
    // one through the group overlay, tenant B's own through its txn overlay —
    // the two levels the merge conflates, read here from B's side.
    group
        .stage(|tx| -> Result<(), io::Error> {
            let mut tx = tx.open_scope(b, "member")?;
            tx.put(&Ticket {
                id: Id::from_raw(7),
                n: 22,
            });
            assert_eq!(
                tx.get(Id::<Ticket>::from_raw(7))?.map(|t| t.n),
                Some(22),
                "read-your-own-writes still holds inside a scope"
            );
            assert_eq!(
                tx.scan::<Ticket>()?.iter().map(|t| t.n).collect::<Vec<_>>(),
                vec![22],
                "B's scan must return B's row 7, not A's from the group overlay"
            );
            Ok(())
        })
        .unwrap();
    // And from A's side, with both tenants' rows now in the group overlay.
    group
        .stage(|tx| -> Result<(), io::Error> {
            let tx = tx.open_scope(a, "member")?;
            assert_eq!(
                tx.get(Id::<Ticket>::from_raw(7))?.map(|t| t.n),
                Some(11),
                "tenant A's id 7 must still read as A's row, not B's"
            );
            assert_eq!(
                tx.scan::<Ticket>()?.iter().map(|t| t.n).collect::<Vec<_>>(),
                vec![11],
                "A's scan must return A's row 7 — a bare-id overlay merge returns B's, \
                 because (scope, id) iterates in scope order and B's scope is the higher one"
            );
            assert_eq!(
                tx.scan_from::<Ticket>(0, 10)?
                    .iter()
                    .map(|t| t.n)
                    .collect::<Vec<_>>(),
                vec![11],
                "A's scan_from must return A's row 7"
            );
            assert_eq!(
                tx.scan_by::<Ticket>("by_n", &11u32)?
                    .iter()
                    .map(|t| t.n)
                    .collect::<Vec<_>>(),
                vec![11],
                "A's index read on by_n = 11 must find A's row — the index read merges the \
                 same overlay, so a forgotten filter there is the same leak through a second \
                 door"
            );
            assert!(
                tx.scan_by::<Ticket>("by_n", &22u32)?.is_empty(),
                "B's key 22 must not surface under A's index read"
            );
            Ok(())
        })
        .unwrap();
    group.commit().unwrap();

    // And it survives the commit: the two rows are physically distinct.
    let snap = db.snapshot().unwrap();
    assert_eq!(
        snap.get_scoped::<Ticket>(1, Id::from_raw(7))
            .unwrap()
            .map(|t| t.n),
        Some(11)
    );
    assert_eq!(
        snap.get_scoped::<Ticket>(2, Id::from_raw(7))
            .unwrap()
            .map(|t| t.n),
        Some(22)
    );
}

/// The tombstone arm of the same leak, and a separate failure: merged by bare
/// id, tenant B's staged DELETE of id 7 removes tenant A's id 7 from A's own
/// scan result. A row that reads as ABSENT is the more dangerous of the two
/// (the row-overwrite arm at least returns something a caller might notice),
/// and it is a different code path in the merge — the `None` arm — so it
/// needs its own assertion rather than riding on the test above.
#[test]
fn one_tenants_staged_tombstone_cannot_delete_another_tenants_row_from_its_read() {
    let sim = Sim::with_faults(0x7000_0002, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Ticket>(),
    )
    .unwrap();
    let a = Id::<TestOrg>::from_raw(1);
    let b = Id::<TestOrg>::from_raw(2);

    let mut group = db.begin_group().unwrap();
    group
        .stage(|tx| -> Result<(), io::Error> {
            tx.open_scope(a, "member")?.put(&Ticket {
                id: Id::from_raw(7),
                n: 11,
            });
            Ok(())
        })
        .unwrap();
    group
        .stage(|tx| -> Result<(), io::Error> {
            let mut tx = tx.open_scope(b, "member")?;
            tx.delete::<Ticket>(Id::from_raw(7));
            assert!(
                tx.scan::<Ticket>()?.is_empty(),
                "B's own tombstone still applies to B — and A's row 7, in the group overlay, \
                 must not surface here either"
            );
            Ok(())
        })
        .unwrap();
    group
        .stage(|tx| -> Result<(), io::Error> {
            let tx = tx.open_scope(a, "member")?;
            assert_eq!(
                tx.get(Id::<Ticket>::from_raw(7))?.map(|t| t.n),
                Some(11),
                "B's tombstone must not make A's row 7 read as absent"
            );
            assert_eq!(
                tx.scan::<Ticket>()?.iter().map(|t| t.n).collect::<Vec<_>>(),
                vec![11],
                "B's tombstone for id 7 must not remove A's row 7 from A's scan"
            );
            assert_eq!(
                tx.scan_by::<Ticket>("by_n", &11u32)?
                    .iter()
                    .map(|t| t.n)
                    .collect::<Vec<_>>(),
                vec![11],
                "nor from A's index read, whose overlay pass removes before it re-inserts"
            );
            Ok(())
        })
        .unwrap();
    group.commit().unwrap();

    let snap = db.snapshot().unwrap();
    assert_eq!(
        snap.get_scoped::<Ticket>(1, Id::from_raw(7))
            .unwrap()
            .map(|t| t.n),
        Some(11),
        "and the tombstone applied to B's key only, so A's row is still on disk"
    );
}

// ----------------------------------------------- Task 7: opening the scope

/// `open_scope` runs the app's own `authorize` and returns its refusal
/// unchanged — the core half of the contract `charpente/tests/tenant.rs` pins
/// through a real request. Deleting the `S::authorize(...)?` call turns this
/// test RED here, at the one place it runs.
#[test]
fn a_refused_scope_hands_back_the_apps_own_error_and_no_handle() {
    let sim = Sim::with_faults(0x7F00_0001, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Ticket>(),
    )
    .unwrap();

    let refused = db
        .transact(|tx| {
            tx.open_scope(Id::<TestOrg>::from_raw(1), "stranger")?
                .put(&Ticket {
                    id: Id::from_raw(7),
                    n: 11,
                });
            Ok::<(), io::Error>(())
        })
        .expect_err("a refusing authorize must refuse the scope");
    let TxError::App(refused) = refused else {
        panic!("a refusing authorize is the APP's error, not a platform one");
    };
    assert!(
        refused.to_string().contains("not a member of this org"),
        "the app's own refusal reaches the caller unchanged: {refused}"
    );

    // Nothing was written: no handle existed to write through.
    let snap = db.snapshot().unwrap();
    assert!(
        snap.get_scoped::<Ticket>(1, Id::from_raw(7))
            .unwrap()
            .is_none(),
        "a refused scope must leave the tenant untouched"
    );
}

/// Spec §8: a transaction opens ONE scope. Reaching a second tenant through
/// `ScopedTx`'s `DerefMut` is the bypass the private seam no longer allows and
/// this assert closes — the only remaining way to name two tenants in one
/// transaction, and it is a contract violation rather than a cross-tenant
/// write.
#[test]
#[should_panic(expected = "a transaction opens ONE tenant scope")]
fn a_second_scope_on_one_transaction_is_a_contract_violation() {
    let sim = Sim::with_faults(0x7F00_0002, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Ticket>(),
    )
    .unwrap();

    let _ = db.transact(|tx| {
        // The first handle's borrow of `tx` ends with this block, which is
        // what lets a second `open_scope` be reached at all.
        {
            tx.open_scope(Id::<TestOrg>::from_raw(1), "member")?
                .put(&Ticket {
                    id: Id::from_raw(7),
                    n: 11,
                });
        }
        let _second = tx.open_scope(Id::<TestOrg>::from_raw(2), "member")?;
        Ok::<(), io::Error>(())
    });
}

/// Every scope `AuditOrg::authorize` is asked about, in order. A recording
/// scope type rather than `TestOrg`'s, so the ordering pin below observes only
/// its own transaction.
static AUDITED_SCOPES: Mutex<Vec<u64>> = Mutex::new(Vec::new());

struct AuditOrg;

impl Tenant for AuditOrg {
    type Who = str;
    type Error = io::Error;

    fn authorize(_tx: &Tx, _who: &str, scope: Id<AuditOrg>) -> Result<(), io::Error> {
        AUDITED_SCOPES.lock().unwrap().push(scope.raw());
        Ok(())
    }
}

/// `open_scope` decides the one-scope contract BEFORE it asks the app. The
/// violation is decidable without an answer, so a second tenant on this
/// transaction gets no handle whatever the app would say — and the app's
/// `authorize`, with any audit entry or side effect it performs, is never
/// invoked for a scope no handle is issued for.
///
/// The refusal alone does not pin the order: the assert fires either way, so
/// the test above stays green with `authorize` moved back in front of it. What
/// discriminates is WHAT AUTHORIZE SAW — tenant 2 must never reach it.
#[test]
fn a_refused_second_scope_never_reaches_the_apps_authorize() {
    let sim = Sim::with_faults(0x7F00_0004, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Ticket>(),
    )
    .unwrap();

    AUDITED_SCOPES.lock().unwrap().clear();
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = db.transact(|tx| {
            {
                let _first = tx.open_scope(Id::<AuditOrg>::from_raw(1), "member")?;
            }
            let _second = tx.open_scope(Id::<AuditOrg>::from_raw(2), "member")?;
            Ok::<(), io::Error>(())
        });
    }))
    .expect_err("a second tenant on one transaction is a contract violation");
    let msg = panicked
        .downcast_ref::<String>()
        .map_or_else(String::new, String::clone);
    assert!(
        msg.contains("a transaction opens ONE tenant scope"),
        "the refusal must name the contract it broke, not some other panic: {msg}"
    );

    assert_eq!(
        *AUDITED_SCOPES.lock().unwrap(),
        vec![1],
        "authorize must have been asked about tenant 1 and NEVER about tenant 2 — a scope no \
         handle is issued for is not the app's question to answer"
    );
}

/// The same scope twice is not the violation — a handler that opens its
/// tenant, drops the handle, and opens it again is doing nothing dangerous,
/// and an assert that fired here would make the negative space above a
/// tripwire on ordinary code.
#[test]
fn reopening_the_same_scope_is_allowed() {
    let sim = Sim::with_faults(0x7F00_0003, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Ticket>(),
    )
    .unwrap();

    db.transact(|tx| {
        let scope = Id::<TestOrg>::from_raw(1);
        tx.open_scope(scope, "member")?.put(&Ticket {
            id: Id::from_raw(7),
            n: 11,
        });
        assert_eq!(
            tx.open_scope(scope, "member")?
                .get(Id::<Ticket>::from_raw(7))?
                .map(|t| t.n),
            Some(11),
            "the second handle on the same scope reads the first one's write"
        );
        Ok::<(), io::Error>(())
    })
    .unwrap();
}

// ----------------------------------------- job-tenancy Task 2: the admission

/// Entries into `AdmitOrg::authorize`. Its own counter rather than
/// `AUDITED_SCOPES`: `a_refused_second_scope_never_reaches_the_apps_authorize`
/// clears and asserts that one, and tests run on parallel threads.
static ADMIT_ORG_AUTHORIZE_CALLS: AtomicU64 = AtomicU64::new(0);

/// A scope type whose membership check refuses everyone it is asked about, so
/// an admission that asked it would fail twice over — the call would be
/// counted, and the scope refused.
struct AdmitOrg;

impl Tenant for AdmitOrg {
    type Who = str;
    type Error = io::Error;

    fn authorize(_tx: &Tx, _who: &str, _scope: Id<AdmitOrg>) -> Result<(), io::Error> {
        ADMIT_ORG_AUTHORIZE_CALLS.fetch_add(1, Ordering::Relaxed);
        Err(io::Error::other(
            "AdmitOrg refuses everyone it is asked about",
        ))
    }
}

/// Job-tenancy spec §7: the operator opens a tenant with a `TenantAdmission`
/// minted from a `Group`. No `authorize` runs — the operator has no principal
/// to ask about and already holds the store — and the handle it gets is
/// bounded to that tenant exactly as an authorized one is.
#[test]
fn an_admitted_scope_asks_no_authorize_and_writes_inside_its_tenant() {
    let sim = Sim::with_faults(0x7F00_0005, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Ticket>(),
    )
    .unwrap();

    let mut group = db.begin_group().unwrap();
    let admission = group.tenant_admission();
    group
        .stage(|tx| {
            let _ = tx.admit_scope(Id::<AdmitOrg>::from_raw(9), &admission);
            Ok::<(), io::Error>(())
        })
        .unwrap();
    group
        .stage(|tx| {
            tx.admit_scope(Id::<TestOrg>::from_raw(3), &admission)
                .put(&Ticket {
                    id: Id::from_raw(7),
                    n: 11,
                });
            Ok::<(), io::Error>(())
        })
        .unwrap();
    group.commit().unwrap();

    assert_eq!(
        ADMIT_ORG_AUTHORIZE_CALLS.load(Ordering::Relaxed),
        0,
        "an admission is not a question: AdmitOrg::authorize refuses everyone, and it was \
         never asked"
    );
    let snap = db.snapshot().unwrap();
    assert_eq!(
        snap.get_scoped::<Ticket>(3, Id::from_raw(7))
            .unwrap()
            .map(|t| t.n),
        Some(11),
        "the admitted write landed in tenant 3"
    );
    assert!(
        snap.get_scoped::<Ticket>(0, Id::from_raw(7))
            .unwrap()
            .is_none(),
        "and nowhere else — tenant 0 is a tenant, not a default"
    );
}

/// The one-scope contract (spec §8 of the tenant-scoping spec) is the same
/// contract whichever way the scope was entered, because `open_scope` and
/// `admit_scope` share one body (job-tenancy spec §3).
#[test]
#[should_panic(expected = "a transaction opens ONE tenant scope")]
fn an_admitted_second_tenant_is_the_same_one_scope_violation() {
    let sim = Sim::with_faults(0x7F00_0006, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Ticket>(),
    )
    .unwrap();
    let mut group = db.begin_group().unwrap();
    let admission = group.tenant_admission();
    let _ = group.stage(|tx| {
        {
            let _first = tx.admit_scope(Id::<TestOrg>::from_raw(1), &admission);
        }
        let _second = tx.admit_scope(Id::<TestOrg>::from_raw(2), &admission);
        Ok::<(), io::Error>(())
    });
}

/// And across the two constructors: a tenant opened by membership, then a
/// different one admitted, is the same violation — neither path is a way
/// around the other's contract.
#[test]
#[should_panic(expected = "a transaction opens ONE tenant scope")]
fn an_opened_then_admitted_second_tenant_is_the_same_violation() {
    let sim = Sim::with_faults(0x7F00_0007, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Ticket>(),
    )
    .unwrap();
    let mut group = db.begin_group().unwrap();
    let admission = group.tenant_admission();
    let _ = group.stage(|tx| {
        {
            let _first = tx.open_scope(Id::<TestOrg>::from_raw(1), "member")?;
        }
        let _second = tx.admit_scope(Id::<TestOrg>::from_raw(2), &admission);
        Ok::<(), io::Error>(())
    });
}

/// F1 mechanism (no child process — see `crates/edge-harness/tests/
/// open_fence.rs` for the printed-output oracle, which needs one).
/// `Store::open`'s `IN_OPEN_FENCE` thread-local must clear on EVERY exit
/// path, including the one where `catch_unwind` actually caught a real redb
/// panic — a leaked `true` here would silently swallow the next genuine
/// panic on this thread forever, which is worse than F1 itself.
#[test]
fn open_fence_flag_clears_after_catching_a_real_redb_open_panic() {
    assert!(
        !charpente_core::table::in_open_fence(),
        "must start clear: nothing has entered the fence on this thread yet"
    );

    let sim = Sim::with_faults(0x0FEE_1234, FaultConfig::none());
    let disk = sim.disk();
    {
        let store = Store::open(&disk, "probe.redb").unwrap();
        // Enough committed data that redb's durable header records a
        // nontrivial layout, then a graceful close so header layout == file
        // length (same shape as crates/charpente/tests/transactions.rs's
        // `unopenable_store_with_compacted_wal_is_a_named_hard_error`).
        store
            .import(|w| {
                for i in 0..500u64 {
                    w.put_raw("probe", None, i, &[7u8; 64])?;
                }
                Ok(())
            })
            .unwrap();
        store.checkpoint().unwrap();
    }
    assert!(
        !charpente_core::table::in_open_fence(),
        "the ordinary successful open above must also leave the flag clear"
    );

    // Truncate past the durable header's recorded layout so redb's open-time
    // assert (page_manager.rs:231) fires — the exact condition the fence
    // exists to catch.
    let file = disk.open("probe.redb").unwrap();
    let len = file.len().unwrap();
    file.set_len(len - 4096).unwrap();
    file.sync().unwrap();
    drop(file);

    let err = match Store::open(&disk, "probe.redb") {
        Ok(_) => panic!("the corrupted store must fail to open"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("panicked at open"),
        "the fence must still convert redb's open-time assert into a named \
         io::Error: {err}"
    );
    assert!(
        !charpente_core::table::in_open_fence(),
        "the flag must clear even though the fence just caught a REAL panic \
         — this is the guard's teeth, not merely the fence's"
    );
}
