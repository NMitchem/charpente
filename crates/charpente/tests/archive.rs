//! The archive engine ships sealed WAL segments (M2 object-storage-archive
//! T5, task-5-brief.md). These properties are proven at the `Archive` +
//! `SimObjectStore` level, borrowed one `step` at a time (ruling 32) —
//! no `Rc`, no `Sync` bound anywhere.
//!
//! Fixture helpers are defined once here; every later task's tests use them
//! (ruling 31), so each one is a real, working helper — not a name the
//! plan merely announced.

use std::cell::{Cell, RefCell};
use std::io;

use charpente::archive::manifest::{
    Manifest, SnapshotMeta, manifest_key, segment_key, snapshot_batch_key, snapshot_meta_key,
};
use charpente::archive::{Archive, ArchiveConfig};
use charpente::{Db, DbConfig, Id, Schema, table};
use charpente_core::io::{ObjectDone, ObjectOp, ObjectStore};
use charpente_sim::object::SimObjectStore;
use charpente_sim::{FaultConfig, Sim};

const PREFIX: &str = "archive-test-prefix";
const WRITER: &str = "node-a:9000";

/// A fixed-width row: every commit produces an identically-sized WAL frame,
/// which is what makes `RECORDS_PER_SEGMENT` an exact, reusable constant
/// (Ruling 34) rather than an average.
#[table]
struct Rec {
    id: Id<Rec>,
    value: u64,
}

/// MEASURED (Ruling 34), not the plan's assumed 512 — see
/// task-5-report.md for the exact measurement method. This is the
/// steady-state segment size (every segment but the first) at
/// `cfg.wal.segment_bytes = 64 << 10` with the fixed-width `Rec` row above.
const RECORDS_PER_SEGMENT: u64 = 1365;

/// MEASURED alongside `RECORDS_PER_SEGMENT` (an addition this brief did not
/// name — see task-5-report.md): the FIRST segment holds one record fewer
/// than every later one. `Db::open`'s schema-adoption bookkeeping (T6's
/// first-write adoption, db.rs) folds one extra effect into the very first
/// commit to a fresh table, so that commit's WAL frame is bigger and the
/// first segment's budget holds one fewer same-sized `Rec` frame than a
/// later, adoption-free segment does. Used only where a test needs an
/// exact, independently-computed layout for segment 0 specifically.
const FIRST_SEGMENT_RECORDS: u64 = RECORDS_PER_SEGMENT - 1;

// ------------------------------------------------------------- fixtures

/// Wraps a `SimObjectStore`, adding two live switches this repo's own fault
/// matrix cannot give a test (task-5-review.md m5/m7): `SimObjectStore`'s
/// `FaultConfig` is fixed at construction with no public way to reverse it,
/// and `crates/charpente-sim/src/object.rs` is off-limits this task
/// (Ruling 37, deferred to a later whole-branch review) — so a *reversible*
/// PUT/GET outage has to live here instead. Same idiom as the hand-rolled
/// `Disk` wrappers in `charpente/tests/group_commit.rs` (`FaultyTracingDisk`
/// et al.): delegate to `inner` for everything the switch doesn't touch, so
/// this stays byte-identical to a plain `SimObjectStore` on every other
/// path.
///
/// `reachable = false` fails `start` before it ever reaches `inner` — no op
/// goes "in flight" on the inner store, so a subsequent `start` retry after
/// `reachable` flips back to `true` is a clean first attempt, never a
/// double-start (`SimObjectStore::start` asserts against exactly that).
///
/// `start_errors = true` is independent of `reachable`: it makes
/// `ObjectStore::start`'s `Err` arm reachable at all (m7) — `SimObjectStore`
/// itself never fails `start`, only `poll`, so `Archive::step_uploading_
/// segment`'s and `step_uploading_manifest`'s `Err(e)` arms on the `start`
/// call have no oracle without it.
/// `lost_ack_for` (Task 6, carried from Task 5's re-review m2') is the third
/// switch and the one no whole-store fault can stand in for. The re-reviewer
/// instrumented all four of `Archive::step`'s upload arms and found
/// `step_uploading_manifest`'s poll-`Err` arm hit **0** times across the
/// entire suite (`seg_start` 10096, `seg_poll` 4999, `man_start` 1,
/// `man_poll` 0) — the arm that matters most for the ordering property this
/// task exists to establish. `SimObjectStore`'s `object_lost_ack` axis
/// cannot reach it: at 1.0 the very first object's ack is lost and the
/// archive never gets as far as a manifest, and there is no way to aim it.
/// This aims it, by recording the in-flight PUT's key at `start` (the only
/// place a wrapper can see which op is running — `poll` is told nothing) and
/// matching a substring at `poll`.
///
/// The semantics being modelled are precise: the inner store DID apply the
/// PUT, and the archive is told it did not. That is a lost ack, not a failed
/// write, and since `manifest_key` is a FIXED key the retry overwrites with
/// identical bytes rather than duplicating. Only PUTs are affected — a lost
/// ack is a write concept, and rigging GETs too would break every test that
/// reads the bucket back through the same store.
struct FlakyStore {
    inner: SimObjectStore,
    reachable: Cell<bool>,
    start_errors: Cell<bool>,
    in_flight_put: RefCell<Option<String>>,
    lost_ack_for: RefCell<Option<String>>,
}

impl FlakyStore {
    fn new(inner: SimObjectStore) -> FlakyStore {
        FlakyStore {
            inner,
            reachable: Cell::new(true),
            start_errors: Cell::new(false),
            in_flight_put: RefCell::new(None),
            lost_ack_for: RefCell::new(None),
        }
    }

    fn set_reachable(&self, reachable: bool) {
        self.reachable.set(reachable);
    }

    fn set_start_errors(&self, fail: bool) {
        self.start_errors.set(fail);
    }

    /// Every PUT whose key contains `needle` lands on the inner store and is
    /// then reported as failed. `None` clears the switch.
    fn set_lost_ack_for(&self, needle: Option<&str>) {
        *self.lost_ack_for.borrow_mut() = needle.map(|s| s.to_string());
    }

    /// Test-only inspection, delegated straight through — same contract as
    /// `SimObjectStore::keys`.
    fn keys(&self) -> Vec<String> {
        self.inner.keys()
    }
}

impl ObjectStore for FlakyStore {
    fn start(&self, op: ObjectOp) -> io::Result<()> {
        if !self.reachable.get() {
            return Err(io::Error::other(
                "simulated FlakyStore outage: start refused because the store is unreachable \
                 (test-local switch — flips back via restore_store_reachability)",
            ));
        }
        if self.start_errors.get() {
            return Err(io::Error::other(
                "simulated FlakyStore start failure: start refused by the test's start_errors \
                 switch",
            ));
        }
        *self.in_flight_put.borrow_mut() = match &op {
            ObjectOp::Put { key, .. } => Some(key.clone()),
            _ => None,
        };
        self.inner.start(op)
    }

    fn poll(&self) -> Option<io::Result<ObjectDone>> {
        let done = self.inner.poll()?;
        let key = self.in_flight_put.borrow_mut().take();
        if let (Ok(ObjectDone::Put), Some(key)) = (&done, &key)
            && let Some(needle) = self.lost_ack_for.borrow().as_deref()
            && key.contains(needle)
        {
            // The inner store already applied it — that is the whole point.
            return Some(Err(io::Error::other(format!(
                "simulated FlakyStore lost ack: PUT \"{key}\" landed but the ack was lost \
                 (test-local switch)"
            ))));
        }
        Some(done)
    }
}

/// The one place an `ArchiveConfig` is spelled out, so a field added to it
/// lands in every fixture at once.
///
/// `snapshot_every: 0` is not "snapshots off" — it is "no cadence
/// configured", i.e. take this generation's opening snapshot as soon as
/// there is a frontier to name. That is what the existing fixtures pass, and
/// it is deliberate: the archive writes no manifest until its generation
/// carries a snapshot (Defect 4), so a fixture that suppressed the snapshot
/// would suppress every manifest assertion in this file along with it.
fn archive_config(snapshot_every: u64, read_budget_bytes: usize) -> ArchiveConfig {
    ArchiveConfig {
        prefix: PREFIX.to_string(),
        writer: WRITER.to_string(),
        snapshot_every,
        pin_segments: 8,
        read_budget_bytes,
    }
}

fn build_fixture<S>(store: S, read_budget_bytes: usize) -> (Db, S, Archive) {
    build_fixture_with_cadence(store, 0, read_budget_bytes)
}

fn build_fixture_with_cadence<S>(
    store: S,
    snapshot_every: u64,
    read_budget_bytes: usize,
) -> (Db, S, Archive) {
    let sim = Sim::with_faults(0x6172_6368_5f74_3500, FaultConfig::none());
    let mut db_cfg = DbConfig::default();
    db_cfg.wal.segment_bytes = 64 << 10;
    // `checkpoint_every` is left at its DEFAULT on purpose: the archive
    // must survive the real GC cadence. An earlier revision set it to 0
    // to accommodate a pin that released between `open` and the first
    // step, which disabled the one test that catches exactly that bug
    // (M1/M2). The pin now holds from `open`, so no accommodation is
    // needed and the default cadence is itself part of the oracle.
    let mut db = Db::open(Box::new(sim.disk()), db_cfg, Schema::new().table::<Rec>()).unwrap();
    let arc = Archive::open(archive_config(snapshot_every, read_budget_bytes), &mut db).unwrap();
    (db, store, arc)
}

/// Writer identity "node-a:9000", prefix "archive-test-prefix" (per the
/// brief). `segment_bytes` is reduced to 64 KiB so a test seals segments in
/// thousands of records rather than hundreds of thousands — the production
/// default is 64 MiB (`log.rs:75`). Default `read_budget_bytes` (8 MiB, spec
/// default) comfortably covers a whole 64 KiB test segment in one
/// `Db::read_log` call.
/// A Db whose WAL has ALREADY been GC'd by the time `Archive::open` runs —
/// the restart / seeded-follower shape (M1'). `checkpoint_every` is left at
/// its default so the writes below really do drive a checkpoint and a gc.
fn fixture_with_precompacted_log() -> (Db, SimObjectStore, Archive) {
    let sim = Sim::with_faults(0x6172_6368_5f74_3501, FaultConfig::none());
    let mut db_cfg = DbConfig::default();
    db_cfg.wal.segment_bytes = 64 << 10;
    let mut db = Db::open(Box::new(sim.disk()), db_cfg, Schema::new().table::<Rec>()).unwrap();
    write_records(&mut db, 20 * RECORDS_PER_SEGMENT);
    let arc = Archive::open(archive_config(0, 8 << 20), &mut db).unwrap();
    (db, SimObjectStore::new(), arc)
}

fn archive_fixture() -> (Db, SimObjectStore, Archive) {
    build_fixture(SimObjectStore::new(), 8 << 20)
}

/// Starts UNREACHABLE (m5): every earlier revision of this fixture used
/// `SimObjectStore::with_faults(..., object_put_error: 1.0, ...)`, a fault
/// that cannot be reversed through `SimObjectStore`'s public API without
/// touching `object.rs` (off-limits, Ruling 37) — so `restore_store_
/// reachability` could not genuinely restore it and shipped as a panic
/// stub. `FlakyStore`'s `reachable` switch is a real, reversible outage.
fn archive_fixture_with_unreachable_store() -> (Db, FlakyStore, Archive) {
    let store = FlakyStore::new(SimObjectStore::new());
    store.set_reachable(false);
    build_fixture(store, 8 << 20)
}

/// Starts reachable, with `start_errors` on (m7): makes `ObjectStore::
/// start`'s `Err` arm reachable, which `SimObjectStore` itself can never
/// produce (it only ever fails `poll`).
fn archive_fixture_with_start_errors() -> (Db, FlakyStore, Archive) {
    let store = FlakyStore::new(SimObjectStore::new());
    store.set_start_errors(true);
    build_fixture(store, 8 << 20)
}

/// A real, non-zero snapshot cadence: the archive must not write ANYTHING
/// until `last_applied` has crossed `snapshot_every`, because a manifest
/// with no snapshot behind it names a frontier nothing backs (Defect 4).
fn archive_fixture_with_snapshot_cadence(snapshot_every: u64) -> (Db, SimObjectStore, Archive) {
    build_fixture_with_cadence(SimObjectStore::new(), snapshot_every, 8 << 20)
}

/// A store whose MANIFEST PUTs land and are then reported as failed — the
/// only fixture that reaches `step_uploading_manifest`'s poll-`Err` arm
/// (task-5-rereview m2'; see `FlakyStore`).
fn archive_fixture_with_lost_manifest_acks() -> (Db, FlakyStore, Archive) {
    let store = FlakyStore::new(SimObjectStore::new());
    store.set_lost_ack_for(Some("/manifest"));
    build_fixture(store, 8 << 20)
}

fn archive_fixture_with_lost_acks() -> (Db, SimObjectStore, Archive) {
    let cfg = FaultConfig {
        object_lost_ack: 1.0,
        ..Default::default()
    };
    build_fixture(
        SimObjectStore::with_faults(0x6c6f_7374_4163_6b00, cfg),
        8 << 20,
    )
}

/// Ruling 29: `read_budget_bytes` deliberately far smaller than one
/// segment (64 KiB) — a handful of records, not one frame — so every
/// segment takes many `Reading` steps and a short read is the NORMAL case,
/// not a rare edge.
fn archive_fixture_with_tiny_read_budget() -> (Db, SimObjectStore, Archive) {
    build_fixture(SimObjectStore::new(), 256)
}

fn write_records(db: &mut Db, n: u64) {
    for i in 0..n {
        db.transact(|tx| -> Result<(), std::convert::Infallible> {
            tx.put(&Rec {
                id: Id::from_raw(i),
                value: i,
            });
            Ok(())
        })
        .unwrap();
    }
}

/// Bounded, never a panic on timeout: some fixtures (an unreachable store,
/// a permanently lost-ack store) never catch up by design, and those tests
/// want the bounded progress this makes, not a failure. 10,000 steps is
/// generous next to the measured ~529-step worst case in this file (the
/// tiny-budget fixture reading three ~1365-record segments a few records at
/// a time, `read_budget_bytes = 256` against a steady-state 48-byte frame:
/// measured directly, not estimated — see task-5-fix-r2-report.md).
fn step_until_idle(arc: &mut Archive, db: &Db, store: &dyn ObjectStore) {
    for _ in 0..10_000 {
        let _ = arc.step(db, store);
        if arc.idle() {
            return;
        }
    }
}

/// Steps `arc` until it returns to caught-up (`step_until_idle`), then
/// counts the segment objects actually landed — the seal-rate test's
/// "uploads" figure, read from the store rather than a new `Archive`
/// accessor.
fn step_until_idle_counting(arc: &mut Archive, db: &Db, store: &SimObjectStore) -> usize {
    step_until_idle(arc, db, store);
    store
        .keys()
        .iter()
        .filter(|k| k.contains("/segments/"))
        .count()
}

/// Steps `arc` until `step` itself returns `Err`, and returns it. Bounded
/// like `step_until_idle`; unlike it, reaching the bound without ever
/// erroring means the fixture doesn't fault the way the caller expected,
/// so this one panics rather than returning nothing a caller could
/// silently misread as "no error occurred".
fn step_until_error(arc: &mut Archive, db: &Db, store: &dyn ObjectStore) -> io::Error {
    for _ in 0..10_000 {
        if let Err(e) = arc.step(db, store) {
            return e;
        }
    }
    panic!("step_until_error: Archive::step never returned Err in 10,000 steps");
}

/// Same shape as `step_until_error` today — an addition this brief named
/// but did not define (Ruling 31). Kept as a distinct name for later tasks
/// (T6/T7), which will need to tell an ordinary IO retry apart from a
/// terminal notice (e.g. a foreign-writer manifest, spec §4); Task 5's
/// `Archive` makes no such distinction yet, so this is `step_until_error`
/// in substance until it does.
fn step_until_notice(arc: &mut Archive, db: &Db, store: &dyn ObjectStore) -> io::Error {
    step_until_error(arc, db, store)
}

fn sealed_segment_count(db: &Db) -> usize {
    db.sealed_segments().unwrap().len()
}

/// The active tail's base_seq, derived only from `Db`'s public surface: the
/// last `SealedSegment`'s layout end is one less than the tail's base by
/// construction (`Db::sealed_segments` never emits the tail itself), and a
/// fresh WAL's first-ever segment starts at 0. Valid in this file because
/// no fixture here ever runs GC (no checkpoint cadence is reached, and the
/// archive's own retention pin holds everything besides).
fn current_tail_base_seq(db: &Db) -> u64 {
    db.sealed_segments()
        .unwrap()
        .last()
        .map_or(0, |s| s.layout_end + 1)
}

fn drain(store: &dyn ObjectStore) -> io::Result<ObjectDone> {
    for _ in 0..10_000 {
        if let Some(r) = store.poll() {
            return r;
        }
    }
    panic!("drain: ObjectStore::poll returned None 10,000 times in a row — stuck in flight");
}

/// `None` when the key is absent (a `Get` that errors), bytes otherwise.
fn get_object(store: &dyn ObjectStore, key: &str) -> Option<Vec<u8>> {
    store
        .start(ObjectOp::Get {
            key: key.to_string(),
        })
        .unwrap();
    match drain(store) {
        Ok(ObjectDone::Get(bytes)) => Some(bytes),
        Ok(other) => panic!("expected Get, got {other:?}"),
        Err(_) => None,
    }
}

/// Every object key a manifest CLAIMS to be backed by (Defect 3 — the plan
/// named this helper and never defined it): its generation's snapshot meta,
/// every `<table>/<n>` batch object that meta declares, and every segment in
/// the chain.
///
/// The meta key is expected UNCONDITIONALLY, not only when the manifest
/// looks snapshot-shaped. A generation is one full logical snapshot plus the
/// chain continuing it (spec §2), so a manifest with no meta behind it is
/// exactly the incomplete generation the caller is looking for — deriving
/// the expectation from the manifest's own `tables` field instead would let
/// a manifest that forgot its snapshot excuse itself.
///
/// Batch counts come from the meta rather than the manifest because the
/// manifest carries none: it names the tables, never how many objects each
/// one was paged into. That is what `SnapshotMeta::tables` is for.
fn expected_keys(store: &dyn ObjectStore, m: &Manifest) -> Vec<String> {
    let meta_key = snapshot_meta_key(PREFIX, m.generation);
    let mut keys = vec![meta_key.clone()];
    if let Some(bytes) = get_object(store, &meta_key) {
        let meta = SnapshotMeta::decode(&bytes).expect("a valid snapshot meta");
        for t in &meta.tables {
            for n in 0..t.batches {
                keys.push(snapshot_batch_key(PREFIX, m.generation, &t.name, n));
            }
        }
    }
    for s in &m.segments {
        keys.push(segment_key(PREFIX, m.generation, s.base_seq));
    }
    keys
}

/// Runs a FRESH archive over `db` against a FRESH store and stops after
/// exactly `cut` steps — the crash cut (Defect 3: the plan named this helper
/// and never defined it).
///
/// A cut usually lands with one operation in flight, because `Archive::step`
/// deliberately splits `ObjectStore::start` from the `poll` that completes
/// it. `settle` below resolves that op on the STORE side while the archive
/// never learns the outcome, which models the harder of the two crash
/// outcomes: the store accepted the write. The other outcome (the write
/// never landed) leaves strictly fewer objects present, so anything that
/// holds here holds there.
fn replay_archive_to_step(db: &mut Db, cut: usize) -> (SimObjectStore, Archive) {
    let store = SimObjectStore::new();
    let mut arc = Archive::open(archive_config(0, 8 << 20), db).unwrap();
    for _ in 0..cut {
        let _ = arc.step(&*db, &store);
    }
    // Resolve any in-flight op: `SimObjectStore::start` asserts against a
    // second start, so the reads below cannot even run until it is cleared,
    // and `poll` on an idle store is a harmless `None`.
    let _ = store.poll();
    (store, arc)
}

/// Steps a fresh archive over `db` to idle and returns how many steps that
/// took — Defect 6's measurement. The crash-cut loop's bound is derived from
/// this rather than hard-coded, so it can neither run past the end (proving
/// nothing at full replay cost) nor stop short of it.
fn steps_to_idle(db: &mut Db) -> usize {
    let store = SimObjectStore::new();
    let mut arc = Archive::open(archive_config(0, 8 << 20), db).unwrap();
    for n in 0..10_000 {
        arc.step(&*db, &store).unwrap();
        if arc.idle() {
            return n + 1;
        }
    }
    panic!("steps_to_idle: the archive never reached idle in 10,000 steps");
}

/// The archive module's source with whole-line `//` comments removed.
///
/// Both proxy tests below search CODE, never prose. The module doc is
/// REQUIRED to record that the cadence names no clock (task-6-brief Defect
/// 2) and to explain why `scan_raw` is not used (spec §10), so a raw search
/// would match the very sentences that document the properties it guards —
/// a test that forbids its own rationale. Trailing comments are left in
/// place deliberately: `let t = clock.now(); // ...` must still be caught.
fn archive_source_code() -> String {
    let mut out = String::new();
    for src in [
        include_str!("../src/archive/mod.rs"),
        include_str!("../src/archive/manifest.rs"),
    ] {
        for line in src.lines() {
            if line.trim_start().starts_with("//") {
                continue;
            }
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

fn read_manifest(store: &dyn ObjectStore) -> Option<Manifest> {
    let bytes = get_object(store, &manifest_key(PREFIX))?;
    Some(Manifest::decode(&bytes).expect("a valid manifest"))
}

/// The generation's snapshot meta, or `None` if it has not been written yet.
fn read_snapshot_meta(store: &dyn ObjectStore, generation: u64) -> Option<SnapshotMeta> {
    let bytes = get_object(store, &snapshot_meta_key(PREFIX, generation))?;
    Some(SnapshotMeta::decode(&bytes).expect("a valid snapshot meta"))
}

fn write_foreign_manifest(store: &SimObjectStore, writer: &str, generation: u64) {
    let m = Manifest {
        generation,
        writer: writer.to_string(),
        applied_seq: 0,
        next_job_id: 0,
        tables: Vec::new(),
        segments: Vec::new(),
        closed: false,
    };
    store
        .start(ObjectOp::Put {
            key: manifest_key(PREFIX),
            data: m.encode(),
        })
        .unwrap();
    drain(store).unwrap();
}

/// m5 (task-5-review.md): genuinely restores a `FlakyStore` that
/// `archive_fixture_with_unreachable_store` started unreachable. An earlier
/// revision of this fixture shipped as a panic stub — `SimObjectStore`'s own
/// `FaultConfig` really cannot be reversed without touching the off-limits
/// `object.rs` (Ruling 37) — but that was a defect in the fixture's chosen
/// store, not a structural impossibility: `ObjectStore` is a public
/// two-method trait, and `FlakyStore` above implements a real, reversible
/// outage against it.
fn restore_store_reachability(store: &FlakyStore) {
    store.set_reachable(true);
}

// ----------------------------------------------------------------- tests

#[test]
fn only_sealed_segments_ship_never_the_tail() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT); // seals at least two segments
    assert!(
        sealed_segment_count(&db) >= 2,
        "fixture sealed nothing; the rest is vacuous"
    );
    step_until_idle(&mut arc, &db, &store);
    let shipped = store.keys();
    let tail = current_tail_base_seq(&db);
    assert!(
        !shipped.iter().any(|k| k.contains(&format!("{tail:020}"))),
        "shipped the live tail: {shipped:?}"
    );
    assert!(
        shipped.iter().any(|k| k.contains("/segments/")),
        "shipped nothing at all"
    );
    // Addition beyond the brief's literal text (see task-5-report.md): the
    // check above reuses `Db::sealed_segments` (via `current_tail_base_seq`)
    // on BOTH sides, so a bug that makes `sealed_segments` itself report the
    // tail as sealed moves `tail` to match whatever got shipped and this
    // check passes vacuously (Ruling 24's lesson, caught for real during
    // this task's own mutation testing — see the report). This is
    // independent: the true end of the last of the >=2 fully-sealed
    // segments this fixture guarantees, computed purely arithmetically from
    // the two measured constants, never from `Db`'s own accounting.
    let last_sealed_end = FIRST_SEGMENT_RECORDS + 2 * RECORDS_PER_SEGMENT - 1;
    let m = read_manifest(&store).expect("a manifest");
    assert!(
        m.segments.iter().all(|s| s.end_seq <= last_sealed_end),
        "shipped a segment reaching into the live tail (past seq {last_sealed_end}): {:?}",
        m.segments
    );
}

/// RULING 29. The one test this task exists to make true.
#[test]
fn a_short_read_never_becomes_a_short_segment_ref() {
    // read_budget_bytes deliberately far smaller than one segment, so every
    // segment takes many `Reading` steps.
    let (mut db, store, mut arc) = archive_fixture_with_tiny_read_budget();
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);
    let m = read_manifest(&store).expect("a manifest");
    assert!(
        m.segments.len() >= 2,
        "fixture didn't ship enough segments to be a meaningful check: {:?}",
        m.segments
    );
    for (i, s) in m.segments.iter().enumerate() {
        // Independent of `Db::sealed_segments` on purpose (Ruling 24's
        // lesson: an oracle built from the same function it is meant to
        // police proves nothing) — pure arithmetic from the two measured
        // constants above.
        let layout = if i == 0 {
            FIRST_SEGMENT_RECORDS - 1
        } else {
            FIRST_SEGMENT_RECORDS + i as u64 * RECORDS_PER_SEGMENT - 1
        };
        assert_eq!(
            s.end_seq, layout,
            "segment {i} claims a span it did not ship: end_seq {} vs layout end {layout}",
            s.end_seq
        );
    }
}

#[test]
fn the_pin_holds_at_the_lowest_un_uploaded_segment() {
    let (mut db, store, mut arc) = archive_fixture_with_unreachable_store();
    // Ruling 34 fix: the brief's literal 500 records never seals a segment
    // at the measured RECORDS_PER_SEGMENT (1365) — this write count does.
    write_records(&mut db, RECORDS_PER_SEGMENT + 50);
    assert!(
        sealed_segment_count(&db) >= 1,
        "fixture sealed nothing; the rest is vacuous"
    );
    step_until_idle(&mut arc, &db, &store);
    assert_eq!(
        arc.pin_floor(),
        db.log_base(),
        "pin must hold everything un-uploaded"
    );
}

/// M2's missing oracle. `the_pin_holds_at_the_lowest_un_uploaded_segment`
/// above pins the HOLD and nothing else — it compares `pin_floor()` to
/// `db.log_base()` at a point where both are 0, so it cannot tell "pinned at
/// the right segment" from "pinned at 0", and the whole suite passed
/// unchanged against a pin that released on every idle step. This pins the
/// RELEASE semantics: held from `open`, advancing only past what the
/// manifest already names, and never `u64::MAX`.
#[test]
fn the_pin_holds_from_open_advances_with_the_manifest_and_never_releases() {
    let (mut db, store, mut arc) = archive_fixture();
    let at_open = arc.pin_floor();
    assert_ne!(
        at_open,
        u64::MAX,
        "a fresh Archive must hold a floor from `open`; releasing until the first \
         pinning step lets Wal::gc delete the segment next_base() will wait for"
    );

    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);

    let caught_up = arc.pin_floor();
    assert_ne!(
        caught_up,
        u64::MAX,
        "the pin must never release outright, not even when caught up"
    );
    assert!(
        caught_up > at_open,
        "the pin must advance past archived segments: {at_open} -> {caught_up}"
    );

    // Exactly at the seq the next segment to seal will start at — one past
    // the last span the manifest actually claims. Derived from the manifest
    // (what was really shipped), never from `Db::sealed_segments` (ruling
    // 24: an oracle built from the function it polices proves nothing).
    let m = read_manifest(&store).expect("a manifest");
    let last_end = m.segments.last().expect("shipped segments").end_seq;
    assert_eq!(
        caught_up,
        last_end + 1,
        "pin must hold exactly the next seq it needs, not further back (which \
         would starve GC) and not further forward (which would drop a segment)"
    );
}

/// M1' (task-5-rereview.md) — a REGRESSION introduced by r1's fix for M1.
/// A Db whose WAL was already GC'd before any `Archive` existed: any node
/// restarted after its first checkpoint, and EVERY seeded follower
/// (`Seeder::finish` lays the first segment at `applied_seq + 1`,
/// db.rs:2543). `next_base()` answered 0 with nothing shipped, `step_idle`
/// found no sealed segment based at 0, and r1's caught-up arm pinned at
/// `next` == 0 — which ships nothing AND freezes `Wal::gc` forever, because
/// a floor of 0 holds the entire log. r1 turned a silent no-ship into a
/// silent no-ship PLUS unbounded WAL growth.
#[test]
fn an_archive_opened_on_a_compacted_log_starts_at_the_logs_base() {
    let (mut db, store, mut arc) = fixture_with_precompacted_log();
    let base = db.log_base();
    assert!(
        base > 0,
        "fixture needs a log GC'd before the archive opened; got base 0 — the rest is vacuous"
    );

    step_until_idle(&mut arc, &db, &store);

    let shipped = store
        .keys()
        .iter()
        .filter(|k| k.contains("/segments/"))
        .count();
    assert!(
        shipped > 0,
        "archive shipped nothing from a compacted log (base {base}): it looked for a segment \
         based at seq 0, which was reclaimed long ago"
    );

    // The pin must never sit below the log's own base: a floor beneath what
    // still exists holds the whole log and gc can never advance again.
    assert!(
        arc.pin_floor() >= base,
        "pin {} is below log_base {base} — gc is now frozen forever",
        arc.pin_floor()
    );

    // And gc must still be able to advance as new data arrives.
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);
    assert!(
        db.log_base() > base,
        "log_base stuck at {base} after archiving and further writes — gc is frozen"
    );
}

#[test]
fn an_archive_io_failure_never_poisons_the_db() {
    let (mut db, store, mut arc) = archive_fixture_with_unreachable_store();
    // Ruling 34 fix: the brief's literal 100 records never seals a segment,
    // so the archive would never even attempt an upload against this fixture's
    // always-failing store — this write count makes the failure path real.
    write_records(&mut db, RECORDS_PER_SEGMENT + 50);
    for _ in 0..50 {
        let _ = arc.step(&db, &store);
    }
    assert!(
        !db.poisoned(),
        "an archive failure is not a database failure"
    );
    write_records(&mut db, 10); // still writable
}

/// m5's core property (task-5-review.md): the whole reason `restore_store_
/// reachability` is worth building for real. T6/T7 need exactly this —
/// "the store comes back and the archive catches up" — so it must be proven
/// at T5, not merely asserted possible.
#[test]
fn a_store_that_comes_back_lets_the_archive_catch_up() {
    let (mut db, store, mut arc) = archive_fixture_with_unreachable_store();
    // m1' (task-5-rereview.md): the earlier `RECORDS_PER_SEGMENT + 50` sealed
    // exactly ONE segment, so "every sealed segment lands" was proven only
    // for n == 1 — and the chain assertion below cannot distinguish a
    // complete chain from a chain missing its head at n == 1.
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    let sealed = sealed_segment_count(&db);
    assert!(
        sealed >= 2,
        "fixture sealed {sealed} segment(s); this test needs >= 2 or it cannot tell a \
         complete chain from one missing its head"
    );

    // While unreachable: no progress, but no fabricated completion and no
    // poisoned Db either (an_archive_io_failure_never_poisons_the_db covers
    // the poison half on its own; re-asserted here because it is the
    // precondition this test's recovery half depends on).
    for _ in 0..50 {
        let _ = arc.step(&db, &store);
    }
    assert!(
        store.keys().is_empty(),
        "an unreachable store must never receive a PUT: {:?}",
        store.keys()
    );
    assert!(
        !db.poisoned(),
        "an archive failure is not a database failure"
    );

    restore_store_reachability(&store);
    step_until_idle(&mut arc, &db, &store);

    let m = read_manifest(&store).expect("a manifest after the store came back");
    let tail = current_tail_base_seq(&db);
    // Both ENDS, not just the last one. `Manifest`'s contiguity check proves
    // the chain has no interior hole but never that it starts where the log
    // does, so a chain missing its head is internally consistent and would
    // pass a last-segment-only assertion (m1').
    assert_eq!(
        m.segments.first().map(|s| s.base_seq),
        Some(db.log_base()),
        "the chain must start at the log's base, not part-way in: manifest {:?}, log_base {}",
        m.segments,
        db.log_base()
    );
    assert_eq!(
        m.segments.last().map(|s| s.end_seq + 1),
        Some(tail),
        "every sealed segment must land once the store comes back: manifest {:?}, tail base {tail}",
        m.segments
    );
    assert_eq!(
        m.segments.len(),
        sealed,
        "chain covers {} segments but {sealed} were sealed: {:?}",
        m.segments.len(),
        m.segments
    );
}

/// m7 (task-5-review.md): `SimObjectStore::start` always returns `Ok`, so
/// `step_uploading_segment`'s and `step_uploading_manifest`'s `Err(e)` arms
/// on the `start` call have no oracle without `FlakyStore`'s `start_errors`
/// switch. Covers both — the segment's own PUT and the manifest's — in one
/// narrative: the first is the archive's very first `start` attempt (the
/// switch starts on), the second is caught right as the archive transitions
/// from `UploadingSegment` to `UploadingManifest` (the same `step` that
/// lands the segment's PUT also makes that transition — mod.rs:412-441 —
/// so the segment key appearing in the store IS that signal).
#[test]
fn start_errors_surface_from_both_the_segment_and_the_manifest_put() {
    let (mut db, store, mut arc) = archive_fixture_with_start_errors();
    write_records(&mut db, RECORDS_PER_SEGMENT + 50);

    // Phase 1: step_uploading_segment's start Err arm. Idle->Reading and
    // Reading->UploadingSegment never touch the store, so it is safe that
    // the switch was already on before the very first step.
    let e = step_until_error(&mut arc, &db, &store);
    assert!(
        e.to_string().contains("simulated FlakyStore start failure"),
        "expected the start-refusal text, got: {e}"
    );
    assert!(
        store.keys().is_empty(),
        "a refused start must not have PUT anything: {:?}",
        store.keys()
    );

    // Let the segment PUT actually land, then stop the instant it has.
    store.set_start_errors(false);
    for _ in 0..10_000 {
        if store.keys().iter().any(|k| k.contains("/segments/")) {
            break;
        }
        arc.step(&db, &store).unwrap();
    }
    assert!(
        store.keys().iter().any(|k| k.contains("/segments/")),
        "the segment never landed: {:?}",
        store.keys()
    );

    // Phase 2: step_uploading_manifest's start Err arm.
    //
    // Task 6 changed what "the manifest has not been PUT" means here. The
    // generation's opening snapshot lands its own manifest before any
    // segment ships, so a manifest key now exists by this point and the
    // original absence check would pass for the wrong reason. The property
    // that survives is the one that mattered: a refused start must not have
    // ADVANCED the manifest.
    let before = read_manifest(&store).expect("the opening snapshot's manifest");
    assert!(
        before.segments.is_empty(),
        "the opening manifest must name no segment yet: {:?}",
        before.segments
    );
    store.set_start_errors(true);
    let e = arc
        .step(&db, &store)
        .expect_err("start_errors must surface from step_uploading_manifest's start too");
    assert!(
        e.to_string().contains("simulated FlakyStore start failure"),
        "expected the start-refusal text, got: {e}"
    );

    // The switch refuses every `start`, reads included, so it has to come
    // off before the bucket can be read back at all.
    store.set_start_errors(false);
    assert_eq!(
        read_manifest(&store).expect("still the opening snapshot's manifest"),
        before,
        "a refused start must not have advanced the manifest"
    );

    // Recovery: the archive still catches up once the switch flips back.
    step_until_idle(&mut arc, &db, &store);
    assert!(
        read_manifest(&store).is_some(),
        "the archive never caught up after start_errors cleared"
    );
}

#[test]
fn a_lost_ack_retry_uploads_the_same_bytes_and_does_not_duplicate() {
    let (mut db, store, mut arc) = archive_fixture_with_lost_acks();
    // Ruling 34 fix: the brief's literal 300 records never seals a segment
    // at the measured RECORDS_PER_SEGMENT — this write count does.
    write_records(&mut db, RECORDS_PER_SEGMENT + 50);
    step_until_idle(&mut arc, &db, &store);
    let keys = store.keys();
    let mut deduped = keys.clone();
    deduped.dedup();
    assert_eq!(keys, deduped, "retry duplicated an object");
    // The brief's dedup check alone cannot catch a retry that invents a
    // NEW, distinct key each attempt (`SimObjectStore`'s bucket is a
    // `BTreeMap`, so it can never literally hold two equal keys regardless
    // of what the archive does — `dedup` only ever removes adjacent equal
    // elements). Addition beyond the brief's literal text (see
    // task-5-report.md): a retrying key must be the SAME key every time,
    // so the number of distinct segment objects can never exceed the
    // number of sealed segments, no matter how many times a lost ack
    // forces a retry.
    let segment_keys = keys.iter().filter(|k| k.contains("/segments/")).count();
    assert!(
        segment_keys <= sealed_segment_count(&db),
        "retry produced more segment objects ({segment_keys}) than sealed segments \
         ({}) — a key must have been bumped rather than reused on retry: {keys:?}",
        sealed_segment_count(&db)
    );
    // Task 6 strengthening, needed to keep this test from going vacuous. At
    // `object_lost_ack: 1.0` EVERY ack is lost, so the archive can never get
    // past its first object — which since Task 6 is the opening snapshot's
    // first batch rather than a segment. Both of the assertions above are
    // then satisfied by a bucket holding nothing at all, which would prove
    // nothing about retry. This is the property they were reaching for: over
    // 10,000 retries the bucket holds exactly ONE key, so the retry reused
    // it every single time.
    assert_eq!(
        keys.len(),
        1,
        "a permanently unacknowledged PUT must retry ONE key forever, never mint new ones: \
         {keys:?}"
    );
}

/// Discharges spec §3's obligation. Deterministic in the simulator; NO
/// benchmark run — docs/BENCH.md is the only source of quotable numbers, it
/// has no archive measurements, and the box is memory-constrained.
#[test]
fn the_seal_rate_stays_under_one_upload_per_step_budget() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 50 * RECORDS_PER_SEGMENT);
    let seals = sealed_segment_count(&db);
    let uploads = step_until_idle_counting(&mut arc, &db, &store);
    assert!(
        uploads >= seals,
        "the archive shipped {uploads} of {seals} sealed segments (db.log_base()={}, \
         arc.pin_floor()={}). Two contracts can break here and they need different fixes: if \
         uploads is 0 (or stalls at a fixed count) the RETENTION PIN released and `Wal::gc` \
         removed the segment `next_base()` is waiting for (M1); if uploads merely trails seals \
         under sustained load, one-in-flight is insufficient and `ArchiveConfig` needs a \
         documented `in_flight` knob plus a BENCH.md entry",
        db.log_base(),
        arc.pin_floor()
    );
}

// --------------------------------------- Task 6: the manifest swaps last

/// MEASURED (ruling 34's discipline, Defect 6): the number of `Archive::
/// step` calls a fresh archive takes to reach idle over a `Db` holding 5,000
/// records, against a clean store. The plan hard-coded `for cut in 0..200`;
/// at the real count every cut past the end is a full replay proving
/// nothing, and had the count gone the other way the loop would have covered
/// a fraction of the run. Asserted below so it cannot silently drift into
/// either failure — re-measure and update if the archive's step shape
/// changes.
const STEPS_TO_ARCHIVE_5000_RECORDS: usize = 47;

/// The property this task exists to establish (spec §2): upload every object
/// of a generation, THEN swap the manifest. Inter-key ordering is not
/// guaranteed, so the manifest swap is the only operation whose completion
/// means anything — a crash at any earlier point must leave an older
/// COMPLETE generation still pointed at, never a torn one.
#[test]
fn the_manifest_never_points_at_an_incomplete_generation() {
    let (mut db, _store, _arc) = archive_fixture();
    write_records(&mut db, 5_000);

    let steps = steps_to_idle(&mut db);
    assert_eq!(
        steps, STEPS_TO_ARCHIVE_5000_RECORDS,
        "the archive's step count moved; the cut loop below is derived from it, so re-measure \
         and update STEPS_TO_ARCHIVE_5000_RECORDS rather than leaving a loop that runs past \
         the end (every cut a no-op) or stops short of it"
    );

    // One past the end deliberately: the last cut must see a finished,
    // consistent generation, not merely a partial one.
    let mut cuts_with_a_manifest = 0;
    for cut in 0..=steps {
        let (store, _arc) = replay_archive_to_step(&mut db, cut);
        let Some(m) = read_manifest(&store) else {
            continue;
        };
        cuts_with_a_manifest += 1;
        let present = store.keys();
        for key in expected_keys(&store, &m) {
            assert!(
                present.contains(&key),
                "manifest (generation {}) names missing key {key} (cut at step {cut}); present: \
                 {present:?}",
                m.generation
            );
        }
        // The manifest and the snapshot meta must agree about the frontier.
        // Both are written by the same generation, meta strictly first, so a
        // manifest that advanced past its own snapshot is a torn generation
        // even when every key it names happens to exist.
        let meta = read_snapshot_meta(&store, m.generation).expect("checked present above");
        assert_eq!(
            m.applied_seq, meta.applied_seq,
            "manifest frontier {} disagrees with its snapshot meta's {} (cut at step {cut})",
            m.applied_seq, meta.applied_seq
        );
        assert_eq!(
            m.tables,
            meta.tables
                .iter()
                .map(|t| t.name.clone())
                .collect::<Vec<_>>(),
            "manifest table list disagrees with its snapshot meta's (cut at step {cut})"
        );
    }
    // Spec §11, and the reason this loop cannot be trusted without it: "a
    // test asserting something never happens passes trivially against an
    // implementation that never does anything." Every cut above `continue`s
    // when there is no manifest yet, so an archive that stopped writing
    // manifests altogether would sail through the whole loop with an empty
    // body. Count the cuts that actually checked something.
    assert_eq!(
        cuts_with_a_manifest,
        21,
        "{cuts_with_a_manifest} of the {} cuts saw a manifest; MEASURED at 21. Fewer means the \
         loop above checked almost nothing and its silence means nothing; MORE means a manifest \
         is landing earlier in the run than the snapshot that has to precede it. Re-measure only \
         after confirming which",
        steps + 1
    );
}

/// Spec §10, load-bearing: the archive pages `scan_raw_from` and never holds
/// a whole table. A proxy rather than a memory measurement, because the
/// property is structural — `scan_raw` returns a whole table at once, and
/// the archive snapshots on a cadence rather than once per join, so it hits
/// a ceiling seeding never does.
#[test]
fn snapshot_upload_never_holds_a_whole_table_in_memory() {
    assert!(
        !archive_source_code().contains("scan_raw("),
        "the archive must page with scan_raw_from — scan_raw returns a whole table at once \
         (spec §10). Note this does not false-match scan_raw_from("
    );
}

/// Defect 2, and the honest replacement for the plan's clock-skew test.
///
/// The plan asserted that two archive runs under different clock offsets
/// behave identically. That test passes because `Archive::step` takes no
/// `Clock` and `Db` exposes none (`grep -ci clock` over db.rs returns 0), so
/// the archive *structurally cannot observe* a clock — it is green from an
/// absence of capability, and the mutation it owed ("drive cadence off
/// `clock.now()`") cannot be written at all. A mutation row that cannot be
/// applied is a missing oracle, not a passing row.
///
/// So this guards the thing that would have to change FIRST: for a clock to
/// reach the archive's decisions, the module would have to name one. Cadence
/// is seq-driven by construction (spec §3), which is why clock skew is a
/// non-issue here rather than a handled case.
#[test]
fn the_archive_module_names_no_clock() {
    let code = archive_source_code().to_lowercase();
    assert!(
        !code.contains("clock"),
        "the archive names a clock. Snapshot cadence is seq-driven by construction (spec §3): \
         step receives no Clock and Db exposes none, which is what makes clock skew a \
         non-issue rather than a handled case. If a clock is genuinely needed here, that is a \
         spec change, not an implementation detail"
    );
}

/// Defect 4. `Snapshot::applied_seq()` is an `Option`; `Manifest.applied_seq`
/// is a plain `u64`, and there is no room in it for "nothing was applied". A
/// manifest asserting `applied_seq: 0` on a Db that applied nothing lies in
/// exactly the way the CRC cannot catch, so the archive does not snapshot at
/// all until there is a frontier to name — and writes no manifest either,
/// since a manifest with no snapshot behind it names a generation that does
/// not exist.
#[test]
fn a_db_that_has_applied_nothing_is_never_snapshotted() {
    let (db, store, mut arc) = archive_fixture();
    assert!(
        db.last_applied().is_none(),
        "fixture must start with nothing applied or the rest is vacuous"
    );
    for _ in 0..50 {
        arc.step(&db, &store).unwrap();
    }
    assert!(
        store.keys().is_empty(),
        "the archive wrote objects for a Db that has applied nothing — there is no frontier to \
         name, so `applied_seq` could only have been a fabricated 0: {:?}",
        store.keys()
    );
}

/// Defect 4's other half, and the Task 5 carry-forward it clears: T5 wrote
/// `applied_seq: 0, next_job_id: 0, tables: []` as placeholders because no
/// snapshot shipped yet. A restore reading such a manifest reads a frontier
/// of 0 that no snapshot backs, and Task 4 carry-forward 3 records that a
/// snapshot-less generation encodes without complaint — so nothing else
/// catches it.
#[test]
fn the_manifest_carries_the_snapshots_real_frontier_never_a_placeholder() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);

    let m = read_manifest(&store).expect("a manifest");
    let meta = read_snapshot_meta(&store, m.generation).expect("a snapshot meta");

    assert!(
        m.applied_seq > 0,
        "the manifest still carries a placeholder frontier of 0 against a Db that applied {:?}",
        db.last_applied()
    );
    assert_eq!(
        m.applied_seq, meta.applied_seq,
        "the manifest's frontier must be the frontier the snapshot was actually taken at"
    );
    // Independent of the archive's own bookkeeping (ruling 24): the frontier
    // must be a seq this Db really reached.
    assert!(
        Some(m.applied_seq) <= db.last_applied(),
        "manifest frontier {} is past the Db's own last_applied {:?}",
        m.applied_seq,
        db.last_applied()
    );
    assert_eq!(
        m.next_job_id, meta.next_job_id,
        "the manifest's job counter must be the snapshot's"
    );
    assert_eq!(
        m.tables,
        db.table_names()
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>(),
        "the manifest must name every registered table, in schema order — including the \
         platform-owned ones, which carry state restore needs"
    );
    // Every table the meta names, empty ones included: "this table had no
    // rows" is a fact restore needs, not an absence it should infer.
    assert_eq!(
        meta.tables.len(),
        db.table_names().len(),
        "the snapshot meta must cover the whole schema: {:?}",
        meta.tables
    );
    let rec = meta
        .tables
        .iter()
        .find(|t| t.name == "Rec")
        .expect("Rec in the meta");
    assert!(
        rec.batches > 1,
        "Rec holds {} rows and must have been PAGED, not written as one object: {rec:?}",
        3 * RECORDS_PER_SEGMENT
    );
}

/// Cadence is in committed records (spec §3). Until `last_applied` crosses
/// it, the generation has no snapshot — and therefore no manifest, because a
/// manifest names a generation and a generation is a snapshot plus the chain
/// continuing it.
#[test]
fn the_snapshot_waits_for_the_configured_record_cadence() {
    let cadence = 3 * RECORDS_PER_SEGMENT;
    let (mut db, store, mut arc) = archive_fixture_with_snapshot_cadence(cadence);

    write_records(&mut db, RECORDS_PER_SEGMENT + 50);
    assert!(
        sealed_segment_count(&db) >= 1,
        "fixture sealed nothing, so 'shipped nothing' would be vacuous"
    );
    step_until_idle(&mut arc, &db, &store);
    assert!(
        store.keys().is_empty(),
        "the archive shipped before its generation had a snapshot, with the cadence ({cadence}) \
         not yet reached: {:?}",
        store.keys()
    );

    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);
    let m = read_manifest(&store).expect("a manifest once the cadence was crossed");
    assert!(
        m.applied_seq >= cadence,
        "snapshot taken at {} but the cadence is {cadence}",
        m.applied_seq
    );
    assert!(
        !m.segments.is_empty(),
        "the chain must ship once the generation has a snapshot"
    );
}

/// task-5-rereview m2': `step_uploading_manifest`'s poll-`Err` arm had ZERO
/// hits across the whole suite at Task 5 — the arm that matters most for the
/// ordering property, proven by probe and tested by nothing. This is its
/// oracle. The lost ack is the sharpest case: the object DID land, and the
/// archive is told it did not, so a retry that invented a fresh key would
/// leave two manifests in a layout whose one mutable key is the whole safety
/// argument.
#[test]
fn a_lost_manifest_ack_retries_the_same_key_with_the_same_bytes() {
    let (mut db, store, mut arc) = archive_fixture_with_lost_manifest_acks();
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);

    let e = step_until_error(&mut arc, &db, &store);
    assert!(
        e.to_string().contains("simulated FlakyStore lost ack"),
        "expected the lost-ack text from the manifest PUT, got: {e}"
    );
    let first = get_object(&store, &manifest_key(PREFIX)).expect("the manifest object landed");

    // Keep stepping: every attempt fails the same way, forever.
    for _ in 0..200 {
        let _ = arc.step(&db, &store);
    }
    let again = get_object(&store, &manifest_key(PREFIX)).expect("still there");
    assert_eq!(
        first, again,
        "a manifest retry must re-PUT the SAME bytes, not rebuild them"
    );

    let keys = store.keys();
    assert_eq!(
        keys.iter().filter(|k| k.contains("manifest")).count(),
        1,
        "a lost-ack retry minted a second manifest key; the manifest is the ONE mutable key in \
         the layout (spec §2): {keys:?}"
    );
    // And the pin never advances on an unacknowledged manifest: a segment
    // counts as reflected only once the manifest PUT naming it has landed.
    assert_eq!(
        arc.pin_floor(),
        db.log_base(),
        "the pin advanced past a manifest the archive was never told had landed"
    );
    assert!(
        !keys.iter().any(|k| k.contains("/segments/")),
        "the archive moved on to a segment while its manifest was unacknowledged: {keys:?}"
    );
}

// -------------------------------------------------- fixture self-checks
//
// Not part of the brief's Step 2, but the brief also requires every named
// fixture helper to be REAL (Ruling 31) — these exercise the ones Task 5's
// six tests above never call, so they cannot silently bit-rot into a name
// with no working body behind it.

#[test]
fn write_foreign_manifest_round_trips_through_read_manifest() {
    let store = SimObjectStore::new();
    write_foreign_manifest(&store, "node-b:9000", 7);
    let m = read_manifest(&store).expect("a manifest");
    assert_eq!(m.writer, "node-b:9000");
    assert_eq!(m.generation, 7);
}

/// m6 (task-5-review.md): `!e.to_string().is_empty()` is vacuous — no
/// `io::Error` can ever fail it — and the sibling test this collapses
/// (`step_until_notice_surfaces_the_unreachable_stores_failure`) called
/// `step_until_notice`, which is `step_until_error` in substance (its own
/// doc), so the two bodies were identical bar the helper name. One test,
/// calling `step_until_notice` so both fixture helpers stay exercised
/// (Ruling 31), with a real assertion on the error text (hard rule 5).
#[test]
fn step_until_error_surfaces_the_unreachable_stores_failure() {
    let (mut db, store, mut arc) = archive_fixture_with_unreachable_store();
    write_records(&mut db, RECORDS_PER_SEGMENT + 50);
    let e = step_until_notice(&mut arc, &db, &store);
    assert!(
        e.to_string().contains("FlakyStore outage"),
        "expected the unreachable store's own failure text, got: {e}"
    );
}
