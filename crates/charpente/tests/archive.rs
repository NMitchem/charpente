//! The archive engine ships sealed WAL segments (M2 object-storage-archive
//! T5, task-5-brief.md). These properties are proven at the `Archive` +
//! `SimObjectStore` level, borrowed one `step` at a time (ruling 32) —
//! no `Rc`, no `Sync` bound anywhere.
//!
//! Fixture helpers are defined once here; every later task's tests use them
//! (ruling 31), so each one is a real, working helper — not a name the
//! plan merely announced.

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

fn build_fixture(store: SimObjectStore, read_budget_bytes: usize) -> (Db, SimObjectStore, Archive) {
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

fn archive_fixture_with_unreachable_store() -> (Db, SimObjectStore, Archive) {
    let cfg = FaultConfig {
        object_put_error: 1.0,
        object_get_error: 1.0,
        ..Default::default()
    };
    build_fixture(
        SimObjectStore::with_faults(0x756e_5265_6163_0000, cfg),
        8 << 20,
    )
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
/// generous next to the ~300-step worst case in this file (the tiny-budget
/// fixture reading three ~1365-record segments a few records at a time).
fn step_until_idle(arc: &mut Archive, db: &Db, store: &SimObjectStore) {
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
fn step_until_error(arc: &mut Archive, db: &Db, store: &SimObjectStore) -> io::Error {
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
fn step_until_notice(arc: &mut Archive, db: &Db, store: &SimObjectStore) -> io::Error {
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

fn drain(store: &SimObjectStore) -> io::Result<ObjectDone> {
    for _ in 0..10_000 {
        if let Some(r) = store.poll() {
            return r;
        }
    }
    panic!("drain: ObjectStore::poll returned None 10,000 times in a row — stuck in flight");
}

fn read_manifest(store: &SimObjectStore) -> Option<Manifest> {
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

/// An addition this brief did not resolve (see task-5-report.md): under
/// Ruling 37, `crates/charpente-sim/src/object.rs` is off-limits this task,
/// but `SimObjectStore`'s `FaultConfig` is fixed at construction
/// (`with_faults`) with no public setter, and `fail_next_put` is a one-shot
/// single-failure toggle, not a persistent-reachability one. There is no
/// way to flip a `with_faults(..., object_put_error: 1.0, ...)` store back
/// to reachable through `SimObjectStore`'s current public API without
/// touching object.rs. This fails loudly rather than silently doing
/// nothing, so a future caller cannot mistake a no-op for a real recovery.
fn restore_store_reachability(_store: &SimObjectStore) {
    panic!(
        "restore_store_reachability: SimObjectStore's FaultConfig is fixed at construction and \
         object.rs is off-limits this task (Ruling 37) — there is no public API to reverse a \
         persistent fault. This fixture cannot be implemented under Task 5's constraints; see \
         task-5-report.md's brief-defect list before calling it."
    );
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
        "the archive shipped {uploads} of {seals} sealed segments. Two contracts can \
         break here and they need different fixes: if uploads is 0 (or stalls at a fixed \
         count) the RETENTION PIN released and `Wal::gc` removed the segment `next_base()` \
         is waiting for (M1); if uploads merely trails seals under sustained load, \
         one-in-flight is insufficient and `ArchiveConfig` needs a documented `in_flight` \
         knob plus a BENCH.md entry"
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

#[test]
fn step_until_error_surfaces_the_unreachable_stores_failure() {
    let (mut db, store, mut arc) = archive_fixture_with_unreachable_store();
    write_records(&mut db, RECORDS_PER_SEGMENT + 50);
    let e = step_until_error(&mut arc, &db, &store);
    assert!(!e.to_string().is_empty());
}

#[test]
#[should_panic(expected = "no public API to reverse a persistent fault")]
fn restore_store_reachability_is_not_implementable_this_task() {
    let store = SimObjectStore::new();
    restore_store_reachability(&store);
}

#[test]
fn step_until_notice_surfaces_the_unreachable_stores_failure() {
    let (mut db, store, mut arc) = archive_fixture_with_unreachable_store();
    write_records(&mut db, RECORDS_PER_SEGMENT + 50);
    let e = step_until_notice(&mut arc, &db, &store);
    assert!(!e.to_string().is_empty());
}
