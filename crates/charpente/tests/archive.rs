//! The archive engine ships sealed WAL segments (M2 object-storage-archive
//! T5, task-5-brief.md). These properties are proven at the `Archive` +
//! `SimObjectStore` level, borrowed one `step` at a time (ruling 32) —
//! no `Rc`, no `Sync` bound anywhere.
//!
//! Fixture helpers are defined once here; every later task's tests use them
//! (ruling 31), so each one is a real, working helper — not a name the
//! plan merely announced.

use std::cell::Cell;
use std::io;

use charpente::archive::manifest::{Manifest, manifest_key};
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
struct FlakyStore {
    inner: SimObjectStore,
    reachable: Cell<bool>,
    start_errors: Cell<bool>,
}

impl FlakyStore {
    fn new(inner: SimObjectStore) -> FlakyStore {
        FlakyStore {
            inner,
            reachable: Cell::new(true),
            start_errors: Cell::new(false),
        }
    }

    fn set_reachable(&self, reachable: bool) {
        self.reachable.set(reachable);
    }

    fn set_start_errors(&self, fail: bool) {
        self.start_errors.set(fail);
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
        self.inner.start(op)
    }

    fn poll(&self) -> Option<io::Result<ObjectDone>> {
        self.inner.poll()
    }
}

fn build_fixture<S>(store: S, read_budget_bytes: usize) -> (Db, S, Archive) {
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
    let archive_cfg = ArchiveConfig {
        prefix: PREFIX.to_string(),
        writer: WRITER.to_string(),
        snapshot_every: 0,
        pin_segments: 8,
        read_budget_bytes,
    };
    let arc = Archive::open(archive_cfg, &mut db).unwrap();
    (db, store, arc)
}

/// Writer identity "node-a:9000", prefix "archive-test-prefix" (per the
/// brief). `segment_bytes` is reduced to 64 KiB so a test seals segments in
/// thousands of records rather than hundreds of thousands — the production
/// default is 64 MiB (`log.rs:75`). Default `read_budget_bytes` (8 MiB, spec
/// default) comfortably covers a whole 64 KiB test segment in one
/// `Db::read_log` call.
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

fn read_manifest(store: &dyn ObjectStore) -> Option<Manifest> {
    store
        .start(ObjectOp::Get {
            key: manifest_key(PREFIX),
        })
        .unwrap();
    match drain(store) {
        Ok(ObjectDone::Get(bytes)) => Some(Manifest::decode(&bytes).expect("a valid manifest")),
        Ok(other) => panic!("expected Get, got {other:?}"),
        Err(_) => None,
    }
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
    write_records(&mut db, RECORDS_PER_SEGMENT + 50);
    assert!(
        sealed_segment_count(&db) >= 1,
        "fixture sealed nothing; the rest is vacuous"
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
    assert_eq!(
        m.segments.last().map(|s| s.end_seq + 1),
        Some(tail),
        "every sealed segment must land once the store comes back: manifest {:?}, tail base {tail}",
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
    store.set_start_errors(true);
    let e = arc
        .step(&db, &store)
        .expect_err("start_errors must surface from step_uploading_manifest's start too");
    assert!(
        e.to_string().contains("simulated FlakyStore start failure"),
        "expected the start-refusal text, got: {e}"
    );
    assert!(
        !store.keys().iter().any(|k| k.contains("manifest")),
        "a refused start must not have PUT the manifest: {:?}",
        store.keys()
    );

    // Recovery: the archive still catches up once the switch flips back.
    store.set_start_errors(false);
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
