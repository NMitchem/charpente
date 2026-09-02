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
use charpente_core::job::{JobRow, JobStatus};
use charpente_core::log::{Wal, WalConfig};
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
/// cannot depend on that crate), so this uses `JobRow` — one of core's own
/// hand-written `Row` impls (DESIGN §3) — as the stand-in the brief's
/// `Widget` comment calls for ("any existing #[table] type in the test
/// module works here").
#[test]
fn an_ordinary_table_is_unscoped_by_default() {
    fn assert_unscoped<R: Row<Scope = Unscoped>>() {}
    assert_unscoped::<JobRow>();
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
/// it opens the table as `TableDefinition<u64, &[u8]>` (unchanged by this
/// task), so it would fail with a `TableTypeMismatch` if an unscoped row's
/// physical key shape had shifted to the composite 16 bytes, exactly as
/// `a_scoped_table_stores_under_the_composite_key_not_a_bare_u64` below
/// demonstrates it does for a genuinely scoped table.
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
    };
    let mut expected_bytes = Vec::new();
    row.encode_row(&mut expected_bytes);

    store.apply(0, |w| w.put(UNSCOPED, &row)).unwrap();

    let snap = store.snapshot().unwrap();
    let raw = snap.scan_raw(JobRow::TABLE).unwrap();
    assert_eq!(
        raw,
        vec![(7u64, expected_bytes)],
        "an unscoped table's stored key must still be the bare u64 id and its stored value \
         must still be exactly Row::encode_row's bytes"
    );
}

struct TestOrg;

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

/// Whether `Ticket`'s `by_n` index still holds an entry for `n`.
///
/// What `scan_by` can and cannot see on a scoped table, exactly: it walks the
/// index range FIRST and only reaches `Snapshot::get` if that range is
/// non-empty — and `get` is still bare-`u64` keyed until Task 7, so on a
/// `[u8; 16]`-keyed table it fails with redb's key-type mismatch. So an entry
/// that EXISTS makes `scan_by` return that mismatch, and an entry that is GONE
/// makes it return `Ok(vec![])` without ever opening the rows table. That
/// Err/Ok split is the observable — indirect, but exact, and the only one
/// available from outside `table.rs` until Task 5's prefix scan lands. The
/// error's shape is checked, not just its presence, so an unrelated failure
/// cannot masquerade as a surviving entry.
fn ticket_index_entry_survives(snap: &Snapshot, n: u32) -> bool {
    match snap.scan_by::<Ticket>("by_n", &n) {
        Ok(rows) => {
            assert!(
                rows.is_empty(),
                "scan_by cannot decode a scoped row yet — a non-empty Ok means Snapshot::get \
                 started reading scoped tables and this helper's Err/Ok observable is no longer \
                 valid"
            );
            false
        }
        Err(e) => {
            let e = e.to_string();
            assert!(
                e.contains("[u8;16]") || e.contains("[u8; 16]"),
                "the only Err this helper treats as \"index entry present\" is the [u8;16] \
                 key-type mismatch from Snapshot::get; got: {e}"
            );
            true
        }
    }
}

/// Task 4 (tenant scoping): a scoped Row (`Row::Scope != Unscoped`) is stored
/// in a PHYSICALLY DIFFERENT redb table from the bare-`u64`-keyed one the test
/// above pins for the unscoped case — `scan_raw`, still typed
/// `TableDefinition<u64, &[u8]>` (Task 5 teaches it the scoped shape), must
/// fail to open it with `TableTypeMismatch` naming the `[u8;16]` key rather
/// than silently reading it back empty. Silent-empty is exactly what a bug
/// routing a scoped row through the old bare-`u64` path regardless of
/// `Row::Scope` would produce, which is why this checks the failure's shape,
/// not just its presence.
///
/// What this test does NOT observe: the scope's *value*. It passes unchanged
/// if `scoped_key(scope, id)` becomes `scoped_key(UNSCOPED, id)`, because the
/// key stays 16 bytes wide either way. That property is
/// `two_tenants_rows_survive_each_other_under_the_composite_key`'s job.
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
    let err = snap
        .scan_raw(Ticket::TABLE)
        .expect_err(
            "a scoped table's physical redb table must be [u8; 16]-keyed — scan_raw, still \
             typed TableDefinition<u64, &[u8]>, must fail to open it rather than silently \
             reading nothing, which is what a bug routing a scoped row through the unscoped \
             path regardless of Row::Scope would look like",
        )
        .to_string();
    assert!(
        err.contains("[u8;16]") || err.contains("[u8; 16]"),
        "scan_raw's failure must be exactly a key-type mismatch against a [u8;16]-keyed \
         table, not some other error: {err}"
    );
}

/// Task 4's central claim, and the only test that goes red when it stops
/// being true: the SCOPE reaches the stored key. A 16-byte key alone proves
/// nothing — `scoped_key(UNSCOPED, id)` is also 16 bytes — so this pins the
/// consequence of dropping the scope instead of the key's width.
///
/// The mechanism that makes the value observable without Task 5's prefix scan
/// or Task 7's `ScopedTx`: two tenants write the SAME id. Under a correct
/// composite key those are two distinct rows, so both index entries stand.
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
        ticket_index_entry_survives(&snap, 1),
        "tenant 10's row must survive tenant 20's put of the same id — its index entry is gone, \
         which means the second put overwrote the first: the scope is not reaching the row key"
    );
    assert!(
        ticket_index_entry_survives(&snap, 2),
        "tenant 20's own put must be indexed"
    );
    drop(snap);

    store
        .apply(1, |w| w.delete::<Ticket>(10, Id::from_raw(5)))
        .expect("deleting tenant 10's row must not error");

    let snap = store.snapshot().unwrap();
    assert!(
        !ticket_index_entry_survives(&snap, 1),
        "deleting tenant 10's row must retire tenant 10's index entry — an entry left standing \
         means the delete looked for the row under a different scope and found nothing"
    );
    assert!(
        ticket_index_entry_survives(&snap, 2),
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
#[should_panic(expected = "is not tenant-scoped")]
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
    let mut ids: Vec<u64> = snap
        .scan_scoped::<Ticket>(1)
        .expect("tenant 1's scan must not error")
        .into_iter()
        .map(|t| t.id.raw())
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![1, 3, 5],
        "tenant 1's scan must return EXACTLY its own ids — an off-by-one range bound would \
         pull in 2, 4, or 6 from tenant 2"
    );

    let mut ids2: Vec<u64> = snap
        .scan_scoped::<Ticket>(2)
        .expect("tenant 2's scan must not error")
        .into_iter()
        .map(|t| t.id.raw())
        .collect();
    ids2.sort_unstable();
    assert_eq!(
        ids2,
        vec![2, 4, 6],
        "tenant 2's scan must return EXACTLY its own ids, symmetrically"
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
    let mut ids: Vec<u64> = snap
        .scan_scoped::<Ticket>(1)
        .unwrap()
        .into_iter()
        .map(|t| t.id.raw())
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![0, u64::MAX],
        "the scan's range must be INCLUSIVE at both ends of the id space"
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

/// `Snapshot::scan_raw_from` (the migration path) must learn the composite
/// `[u8; 16]` key shape rather than erroring with `TableTypeMismatch`, and
/// must stay scope-agnostic: `migrate_schema()` rewrites every tenant's
/// rows in one pass, so it must see all of them, not just one scope's.
#[test]
fn scan_raw_from_learns_the_composite_key_shape_and_sees_every_tenant() {
    let sim = Sim::with_faults(0x7A2E_0011, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    store
        .apply(0, |w| {
            w.put(
                10,
                &Ticket {
                    id: Id::from_raw(1),
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
        .expect("two tenants writing the same id on a scoped table must not error");

    let snap = store.snapshot().unwrap();
    let rows = snap.scan_raw_from(Ticket::TABLE, None, 10).expect(
        "scan_raw_from must learn the [u8;16] key shape rather than erroring with \
         TableTypeMismatch — migrate_schema() has to be able to walk a scoped table too",
    );
    let mut ns: Vec<u32> = rows
        .iter()
        .map(|(_, bytes)| Ticket::decode_row(bytes).unwrap().n)
        .collect();
    ns.sort_unstable();
    assert_eq!(
        ns,
        vec![100, 200],
        "scan_raw_from must return BOTH tenants' rows — it stays scope-agnostic on purpose, \
         because migrate_schema() rewrites every tenant's rows in one pass"
    );
}

/// Hard rule 4, negative space: each of the three scope-taking read
/// primitives asserts `row_is_scoped::<R>()` rather than silently opening
/// the WRONG physical redb table (`scoped_rows_def`, which the unscoped
/// write path never creates) and reading back an empty, misleadingly
/// "successful" result for a table that actually has rows.
#[test]
#[should_panic(expected = "Snapshot::get_scoped: table \"charpente.jobs\" is not tenant-scoped")]
fn get_scoped_on_an_unscoped_table_panics() {
    let sim = Sim::with_faults(0x7A2E_0012, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();
    let snap = store.snapshot().unwrap();
    let _ = snap.get_scoped::<JobRow>(0, Id::from_raw(1));
}

/// See `get_scoped_on_an_unscoped_table_panics`; same reasoning, `scan_scoped`.
#[test]
#[should_panic(expected = "Snapshot::scan_scoped: table \"charpente.jobs\" is not tenant-scoped")]
fn scan_scoped_on_an_unscoped_table_panics() {
    let sim = Sim::with_faults(0x7A2E_0013, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();
    let snap = store.snapshot().unwrap();
    let _ = snap.scan_scoped::<JobRow>(0);
}

/// See `get_scoped_on_an_unscoped_table_panics`; same reasoning, `scan_from_scoped`.
#[test]
#[should_panic(
    expected = "Snapshot::scan_from_scoped: table \"charpente.jobs\" is not tenant-scoped"
)]
fn scan_from_scoped_on_an_unscoped_table_panics() {
    let sim = Sim::with_faults(0x7A2E_0014, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();
    let snap = store.snapshot().unwrap();
    let _ = snap.scan_from_scoped::<JobRow>(0, 0, 10);
}
