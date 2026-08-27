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
    Manifest, SnapshotMeta, TableBatches, decode_snapshot_batch, manifest_key, segment_key,
    snapshot_batch_key, snapshot_meta_key,
};
use charpente::archive::{Archive, ArchiveConfig};
use charpente::{Db, DbConfig, Id, Schema, table};
use charpente_core::crc::crc32;
use charpente_core::io::{ObjectDone, ObjectOp, ObjectStore};
use charpente_core::table::Row;
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

/// The default retention-pin bound (Task 7), deliberately generous: several
/// tests here starve the archive on purpose — `the_seal_rate_stays_under_
/// one_upload_per_step_budget` writes FIFTY segments before stepping it at
/// all — and under a small bound that starvation would gap, which is a side
/// effect rather than the property those tests exist to check. The gap
/// tests set their own, small bound instead.
const PIN_SEGMENTS: usize = 64;

/// The bound where crossing it IS the property (Task 7). Small enough that
/// the gap tests' `20 * RECORDS_PER_SEGMENT` crosses it twice over, and
/// large enough that the three-segment fixtures sharing
/// `archive_fixture_with_unreachable_store` never reach it.
const GAP_PIN_SEGMENTS: usize = 8;

/// The one place an `ArchiveConfig` is spelled out, so a field added to it
/// lands in every fixture at once.
///
/// `snapshot_every: 0` is not "snapshots off" — it is "no cadence
/// configured", i.e. take this generation's opening snapshot as soon as
/// there is a frontier to name, and never refresh it into a new generation
/// (Task 7). That is what the existing fixtures pass, and it is deliberate:
/// the archive writes no manifest until its generation carries a snapshot
/// (Defect 4), so a fixture that suppressed the snapshot would suppress
/// every manifest assertion in this file along with it.
fn archive_config(snapshot_every: u64, read_budget_bytes: usize) -> ArchiveConfig {
    archive_config_bounded(snapshot_every, read_budget_bytes, PIN_SEGMENTS)
}

fn archive_config_bounded(
    snapshot_every: u64,
    read_budget_bytes: usize,
    pin_segments: usize,
) -> ArchiveConfig {
    ArchiveConfig {
        prefix: PREFIX.to_string(),
        writer: WRITER.to_string(),
        snapshot_every,
        pin_segments,
        read_budget_bytes,
    }
}

fn build_fixture<S>(store: S, read_budget_bytes: usize) -> (Db, S, Archive) {
    build_fixture_with_cadence(store, 0, read_budget_bytes, PIN_SEGMENTS)
}

fn build_fixture_with_cadence<S>(
    store: S,
    snapshot_every: u64,
    read_budget_bytes: usize,
    pin_segments: usize,
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
    let arc = Archive::open(
        archive_config_bounded(snapshot_every, read_budget_bytes, pin_segments),
        &mut db,
    )
    .unwrap();
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
///
/// Runs at `GAP_PIN_SEGMENTS` rather than the file default: this is the
/// fixture Task 7's gap tests use, and a bound they cannot reach would make
/// every one of them vacuous.
fn archive_fixture_with_unreachable_store() -> (Db, FlakyStore, Archive) {
    let store = FlakyStore::new(SimObjectStore::new());
    store.set_reachable(false);
    build_fixture_with_cadence(store, 0, 8 << 20, GAP_PIN_SEGMENTS)
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
    build_fixture_with_cadence(SimObjectStore::new(), snapshot_every, 8 << 20, PIN_SEGMENTS)
}

/// A store whose MANIFEST PUTs land and are then reported as failed — the
/// only fixture that reaches `step_uploading_manifest`'s poll-`Err` arm
/// (task-5-rereview m2'; see `FlakyStore`).
fn archive_fixture_with_lost_manifest_acks() -> (Db, FlakyStore, Archive) {
    let store = FlakyStore::new(SimObjectStore::new());
    store.set_lost_ack_for(Some("/manifest"));
    build_fixture(store, 8 << 20)
}

/// A store whose SEGMENT PUTs land and are then reported as failed (F3).
/// Aimed at `/segments/` alone so the opening snapshot and its manifest
/// complete normally and the archive really does sit in
/// `State::UploadingSegment` across the retry — which
/// `archive_fixture_with_lost_acks` can no longer reach, since at
/// `object_lost_ack: 1.0` nothing gets past the snapshot's first batch.
fn archive_fixture_with_lost_segment_acks() -> (Db, FlakyStore, Archive) {
    let store = FlakyStore::new(SimObjectStore::new());
    store.set_lost_ack_for(Some("/segments/"));
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

/// `write_records`, but starting at `from` — so a test can go on committing
/// FRESH rows mid-run without overwriting the ones it already wrote.
fn write_records_at(db: &mut Db, from: u64, n: u64) {
    for i in from..from + n {
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

/// The two phases every chain assertion in this file needs since RULING 53:
/// a generation's chain carries only what seals ABOVE its snapshot's
/// frontier, so a fixture that writes N records and only then steps produces
/// a snapshot and NO chain at all, and every "the archive shipped a segment"
/// oracle in the file would be asserting against an empty manifest.
///
/// Phase 1 takes the generation's opening snapshot at whatever the `Db` has
/// applied; phase 2 writes `chain_records` more so segments seal above that
/// frontier, and steps again so the chain continuing it actually ships.
fn snapshot_then_chain(
    arc: &mut Archive,
    db: &mut Db,
    store: &dyn ObjectStore,
    chain_records: u64,
) {
    step_until_idle(arc, db, store);
    write_records(db, chain_records);
    step_until_idle(arc, db, store);
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

/// Task 7 makes this genuinely distinct from `step_until_error`, which its
/// Task 5 doc promised and Defect 4 turned into a real defect: on an
/// unreachable-store fixture the FIRST `Err` is an ordinary IO retry, not
/// the gap notice — which cannot arrive until the bound is crossed — so a
/// notice test built on `step_until_error` asserts its needles against the
/// wrong error entirely.
///
/// This skips retryable IO errors and returns the terminal notice. It
/// recognises it by the STATE TRANSITION that accompanies it rather than by
/// matching its prose: the gap notice is the only error `Archive::step`
/// returns on the step that flips `Archive::gapped` from false to true.
/// Matching a substring instead would make the notice test circular — it
/// would find the words it is about to assert are present.
///
/// Task 8's foreign-writer refusal (spec §4) is terminal too and sets no
/// such flag; it will need its own signal here, and this doc is where to
/// add it.
fn step_until_notice(arc: &mut Archive, db: &Db, store: &dyn ObjectStore) -> io::Error {
    for _ in 0..10_000 {
        let before = arc.gapped();
        if let Err(e) = arc.step(db, store)
            && arc.gapped()
            && !before
        {
            return e;
        }
    }
    panic!("step_until_notice: Archive::step never raised a terminal notice in 10,000 steps");
}

fn sealed_segment_count(db: &Db) -> usize {
    db.sealed_segments().unwrap().len()
}

/// The active tail's base_seq, derived only from `Db`'s public surface: the
/// last `SealedSegment`'s layout end is one less than the tail's base by
/// construction (`Db::sealed_segments` never emits the tail itself).
///
/// Defect 5: the `map_or(0, ..)` fallback — "a WAL with no sealed segment
/// has its tail at 0" — is the part GC can falsify, and Task 7 brought GC
/// into this file for the first time (the bound tests call `db.checkpoint()`
/// and assert `log_base` ADVANCED, and an archive holding nothing lets the
/// default cadence reclaim on its own). A GC'd log whose sealed segments
/// were all reclaimed would answer 0 for a tail sitting far above it. The
/// helper is therefore re-derived rather than re-documented: `db.log_base()`
/// is the floor of what the WAL still holds, so a listing with no sealed
/// segment has its tail there, at 0 or anywhere above it. Every caller in
/// this file is unaffected — each has sealed segments — but the answer is
/// now correct without a precondition a later test can silently break.
fn current_tail_base_seq(db: &Db) -> u64 {
    db.sealed_segments()
        .unwrap()
        .last()
        .map_or_else(|| db.log_base(), |s| s.layout_end + 1)
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

/// Every object the bucket holds, key and bytes — one generation exactly as
/// a real archive wrote it, so a fresh store can be seeded with it.
fn store_contents(store: &SimObjectStore) -> Vec<(String, Vec<u8>)> {
    store
        .keys()
        .into_iter()
        .map(|k| {
            let bytes = get_object(store, &k).expect("a key the store just listed");
            (k, bytes)
        })
        .collect()
}

/// Seeds a fresh store with objects captured by `store_contents`.
fn plant(store: &SimObjectStore, objects: &[(String, Vec<u8>)]) {
    for (key, data) in objects {
        store
            .start(ObjectOp::Put {
                key: key.clone(),
                data: data.clone(),
            })
            .unwrap();
        drain(store).unwrap();
    }
}

/// Runs a FRESH archive over `db` against a FRESH store seeded with
/// `planted` and stops after exactly `cut` steps — the crash cut (Defect 3:
/// the plan named this helper and never defined it).
///
/// RULING 53 is why `planted` exists. A fresh archive over a STATIC `Db`
/// takes its snapshot at the current frontier and continues the chain from
/// there, so it can never ship a segment: a cut loop built on it would cover
/// the snapshot's object ordering and nothing of the chain's. Seeded with a
/// generation captured at an EARLIER frontier, the archive under test
/// resumes it instead and ships the segments that sealed since — which is
/// the ordering the loop exists to check.
///
/// A cut usually lands with one operation in flight, because `Archive::step`
/// deliberately splits `ObjectStore::start` from the `poll` that completes
/// it. `settle` below resolves that op on the STORE side while the archive
/// never learns the outcome, which models the harder of the two crash
/// outcomes: the store accepted the write. The other outcome (the write
/// never landed) leaves strictly fewer objects present, so anything that
/// holds here holds there.
///
/// F8: each call `Archive::open`s a fresh archive on the SAME `Db`, and
/// `Db::add_retention_pin` only ever pushes — nothing removes a pin — so a
/// full cut loop leaves one pin per cut behind and `retention_floor()` (the
/// minimum) sticks at the first one. That freezes `Wal::gc` for the rest of
/// the loop, which is harmless here (the loop asserts about objects, never
/// about gc) but is not something to build a gc-sensitive test on. Task 7
/// works on this pin.
fn replay_archive_to_step(
    db: &mut Db,
    planted: &[(String, Vec<u8>)],
    cut: usize,
) -> (SimObjectStore, Archive) {
    let store = SimObjectStore::new();
    plant(&store, planted);
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
fn steps_to_idle(db: &mut Db, planted: &[(String, Vec<u8>)]) -> usize {
    let store = SimObjectStore::new();
    plant(&store, planted);
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

/// Every row a generation's snapshot actually carries for one table, in the
/// order its batch objects lay them out — decoded from the objects, so this
/// is what a restore would read rather than what the archive believes it
/// wrote (F1).
fn read_snapshot_rows(
    store: &dyn ObjectStore,
    generation: u64,
    t: &TableBatches,
) -> Vec<(u64, Vec<u8>)> {
    let mut out = Vec::new();
    for n in 0..t.batches {
        let key = snapshot_batch_key(PREFIX, generation, &t.name, n);
        let bytes = get_object(store, &key)
            .unwrap_or_else(|| panic!("the meta declares batch {key}, which is not in the store"));
        out.extend(decode_snapshot_batch(&bytes).expect("a valid snapshot batch"));
    }
    out
}

/// Key and bytes of every object under `<prefix>/<gen>/snapshot/` — meta and
/// batches alike — read out of the store rather than derived from a meta, so
/// an EXTRA object appearing is as visible as an existing one changing (F2).
fn snapshot_objects(
    store: &dyn ObjectStore,
    generation: u64,
    keys: &[String],
) -> Vec<(String, Vec<u8>)> {
    let under = format!("{PREFIX}/{generation:020}/snapshot/");
    keys.iter()
        .filter(|k| k.starts_with(&under))
        .map(|k| {
            (
                k.clone(),
                get_object(store, k).expect("a key the store just listed"),
            )
        })
        .collect()
}

/// RULING 51: comparing two `Vec<(String, Vec<u8>)>` with `assert_eq!`
/// dumps ~20 KB of raw bytes on failure and names nothing — a message no
/// operator or agent can act on (hard rule 5). This names the first key that
/// differs and reports its length and CRC instead of its contents, which is
/// the whole of what the reader needs: WHICH object changed.
fn assert_snapshot_objects_unchanged(
    now: &[(String, Vec<u8>)],
    before: &[(String, Vec<u8>)],
    why: &str,
) {
    let names = |v: &[(String, Vec<u8>)]| v.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
    assert_eq!(
        names(now),
        names(before),
        "the generation's object SET changed. {why}"
    );
    for ((key, now), (_, before)) in now.iter().zip(before) {
        assert!(
            now == before,
            "object {key} was rewritten: {} bytes (crc {:08x}) -> {} bytes (crc {:08x}). {why}",
            before.len(),
            crc32(before),
            now.len(),
            crc32(now)
        );
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
    // RULING 53: everything written above is the generation's SNAPSHOT's,
    // and only what seals after it is ever shipped — so without a second
    // phase the manifest below names no segment and "never the tail" is a
    // property of an empty chain rather than of the shipper.
    snapshot_then_chain(&mut arc, &mut db, &store, 3 * RECORDS_PER_SEGMENT);
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
    // the two measured constants, never from `Db`'s own accounting. Six
    // segments' worth of records are written in total (three per phase), so
    // the last fully-sealed one ends after the first segment plus five
    // steady-state ones.
    let last_sealed_end = FIRST_SEGMENT_RECORDS + 5 * RECORDS_PER_SEGMENT - 1;
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
    snapshot_then_chain(&mut arc, &mut db, &store, 3 * RECORDS_PER_SEGMENT);
    let m = read_manifest(&store).expect("a manifest");
    assert!(
        m.segments.len() >= 2,
        "fixture didn't ship enough segments to be a meaningful check: {:?}",
        m.segments
    );
    // RULING 53 re-baselines the ORIGIN here, never the property. The old
    // form indexed every span off segment 0 (`FIRST_SEGMENT_RECORDS + i *
    // RECORDS_PER_SEGMENT - 1`), which reads correctly only while the chain
    // starts at the log's base — the overlap itself, written into the
    // oracle. The chain now starts at the segment holding the first record
    // its snapshot does not carry, which phase 1's 3 * RECORDS_PER_SEGMENT
    // records put exactly one first segment plus two steady-state ones in.
    // Independent of `Db::sealed_segments` either way (Ruling 24's lesson:
    // an oracle built from the same function it is meant to police proves
    // nothing) — pure arithmetic from the two measured constants above.
    assert_eq!(
        m.segments[0].base_seq,
        FIRST_SEGMENT_RECORDS + 2 * RECORDS_PER_SEGMENT,
        "the chain does not start at the segment holding the first record its snapshot does \
         not carry, so the spans below would be measured from the wrong origin: {:?}",
        m.segments
    );
    for (i, s) in m.segments.iter().enumerate() {
        // Every segment but the log's first holds exactly
        // RECORDS_PER_SEGMENT records, and the chain contains none of the
        // first. A short read shows up as an end_seq below this.
        let layout = s.base_seq + RECORDS_PER_SEGMENT - 1;
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

    // RULING 53: phase 2 is what gives this generation a chain at all. The
    // records written first are its snapshot's, and the pin advancing past a
    // SHIPPED segment is the whole subject here.
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    snapshot_then_chain(&mut arc, &mut db, &store, 3 * RECORDS_PER_SEGMENT);

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
///
/// RULING 53 renamed this. The old name — `…_starts_at_the_logs_base` — was
/// the overlap stated as a requirement: an archive does NOT start at the
/// log's base any more, it starts at the segment holding the first record
/// its snapshot does not carry. All three properties M1' is about survive
/// verbatim, because none of them was ever about the base itself: the cursor
/// must never be a seq the log has reclaimed, the archive must ship, and gc
/// must keep advancing.
#[test]
fn an_archive_opened_on_a_compacted_log_never_reaches_below_the_logs_base() {
    let (mut db, store, mut arc) = fixture_with_precompacted_log();
    let base = db.log_base();
    assert!(
        base > 0,
        "fixture needs a log GC'd before the archive opened; got base 0 — the rest is vacuous"
    );

    step_until_idle(&mut arc, &db, &store);

    // The pin must never sit below the log's own base: a floor beneath what
    // still exists holds the whole log and gc can never advance again.
    assert!(
        arc.pin_floor() >= base,
        "pin {} is below log_base {base} — gc is now frozen forever",
        arc.pin_floor()
    );

    // The generation's snapshot covers everything already in the log, so
    // what its chain ships is what seals ABOVE it (ruling 53). Write that:
    // "shipped nothing" asserted before it would be a fact about the
    // fixture, not about the cursor.
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);

    let shipped = store
        .keys()
        .iter()
        .filter(|k| k.contains("/segments/"))
        .count();
    assert!(
        shipped > 0,
        "archive shipped nothing from a compacted log (base {base}): it looked for a segment \
         based at a seq that was reclaimed long ago"
    );
    assert!(
        arc.pin_floor() >= base,
        "pin {} is below log_base {base} — gc is now frozen forever",
        arc.pin_floor()
    );
    // And gc must still be able to advance as new data arrives.
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
    // RULING 53: what a recovering archive catches up ON is the chain
    // continuing its snapshot, so phase 2 is what gives it a chain to catch
    // up on at all — the segments sealed during the outage are the
    // snapshot's, and re-shipping them is the overlap ruling 53 removes.
    snapshot_then_chain(&mut arc, &mut db, &store, 3 * RECORDS_PER_SEGMENT);

    let m = read_manifest(&store).expect("a manifest after the store came back");
    let tail = current_tail_base_seq(&db);
    // Both ENDS, not just the last one. `Manifest`'s contiguity check proves
    // the chain has no interior hole but never that it starts where it must,
    // so a chain missing its head is internally consistent and would pass a
    // last-segment-only assertion (m1').
    //
    // RULING 53 re-baselines the head. The old form asserted `== db
    // .log_base()`, which is the overlap stated as a requirement; the head
    // must now be the segment CONTAINING the first record the snapshot does
    // not carry — at or below it, so nothing falls between the two, and
    // reaching above it, so it is not a segment the snapshot already holds.
    let head = *m
        .segments
        .first()
        .expect("a chain after the store came back");
    assert!(
        head.base_seq <= m.applied_seq + 1 && head.end_seq > m.applied_seq,
        "the chain's head [{}, {}] does not hold the first record its snapshot does not carry \
         ({}): below it the records between them are in neither, above it the head is a \
         segment the snapshot already covers",
        head.base_seq,
        head.end_seq,
        m.applied_seq + 1
    );
    assert_eq!(
        m.segments.last().map(|s| s.end_seq + 1),
        Some(tail),
        "every sealed segment must land once the store comes back: manifest {:?}, tail base {tail}",
        m.segments
    );
    let above = db
        .sealed_segments()
        .unwrap()
        .iter()
        .filter(|s| s.base_seq >= head.base_seq)
        .count();
    assert_eq!(
        m.segments.len(),
        above,
        "chain covers {} segments but {above} are sealed at or above its start: {:?}",
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

    // Task 7 put a store READ in front of everything the archive does: its
    // first act is a GET of `<prefix>/manifest` (resume-or-open, carried
    // obligation (a)). `start_errors` refuses every `start`, reads included,
    // so leaving the switch on from step 1 would make Phase 1 below catch
    // the GET's refusal and pass for the wrong reason — F4's hazard, one
    // task later. Let the archive reach its first SEGMENT upload with the
    // switch off, stopping the instant the opening snapshot's manifest lands
    // (nothing is in flight at that moment, so `store.keys()` is safe to
    // read between steps; `read_manifest` would `start` a GET of its own).
    store.set_start_errors(false);
    let manifest = manifest_key(PREFIX);
    for _ in 0..10_000 {
        if store.keys().contains(&manifest) {
            break;
        }
        let _ = arc.step(&db, &store);
    }
    // RULING 53: the segment PUT Phase 1 is named after only happens once a
    // segment seals ABOVE the generation's frontier — everything written so
    // far is the snapshot's. Written here rather than up front so the
    // manifest-landed loop above still stops where it says it does.
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    let keys_before = store.keys();
    assert!(
        keys_before.contains(&manifest),
        "the opening snapshot's manifest never landed; Phase 1 has nothing to sit in front of"
    );
    assert!(
        !keys_before.iter().any(|k| k.contains("/segments/")),
        "a segment shipped before the switch went back on, so Phase 1 would be testing a later \
         start than the one it names: {keys_before:?}"
    );
    store.set_start_errors(true);

    // Phase 1: step_uploading_segment's start Err arm. Idle->Reading and
    // Reading->UploadingSegment never touch the store, so the next `start`
    // after the switch goes on is the segment PUT's.
    let e = step_until_error(&mut arc, &db, &store);
    assert!(
        e.to_string().contains("simulated FlakyStore start failure"),
        "expected the start-refusal text, got: {e}"
    );
    assert_eq!(
        store.keys(),
        keys_before,
        "a refused start must not have PUT anything"
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
    //
    // F4. This asserted `read_manifest(&store).is_some()` at Task 5, when it
    // was a real question — no manifest existed until a segment shipped. It
    // cannot fail under Task 6: the opening snapshot wrote a manifest before
    // this test's second phase began, `before` above already `expect`ed it,
    // and `SimObjectStore` never deletes a key. "Caught up" now means the
    // chain ADVANCED past the empty one `before` captured.
    step_until_idle(&mut arc, &db, &store);
    let caught_up = read_manifest(&store).expect("a manifest");
    assert!(
        !caught_up.segments.is_empty(),
        "the archive never shipped a segment after start_errors cleared; the manifest is \
         still the opening snapshot's ({:?})",
        caught_up.segments
    );
}

#[test]
fn a_lost_ack_retry_uploads_the_same_bytes_and_does_not_duplicate() {
    let (mut db, store, mut arc) = archive_fixture_with_lost_acks();
    // Ruling 34 fix: the brief's literal 300 records never seals a segment
    // at the measured RECORDS_PER_SEGMENT — this write count does.
    write_records(&mut db, RECORDS_PER_SEGMENT + 50);
    step_until_idle(&mut arc, &db, &store);
    // F3. Task 5's two assertions here (`keys == deduped`, and
    // `segment_keys <= sealed_segment_count`) BOTH went vacuous under Task
    // 6's ordering and are gone rather than left standing: at
    // `object_lost_ack: 1.0` every ack is lost, so the archive never gets
    // past its FIRST object — which is now the opening snapshot's first
    // batch, not a segment. The bucket therefore holds one key and no
    // segment at all, which makes `dedup` trivially true of a one-element
    // Vec and `segment_keys <= n` the tautology `0 <= n`. A permanently
    // true assertion is worse than no assertion: it reads as coverage.
    //
    // The SEGMENT half of the property this test used to carry now lives in
    // `a_lost_segment_ack_retries_the_same_key_with_the_same_bytes`, which
    // aims the fault at `/segments/` so the archive really does reach
    // `State::UploadingSegment` and carry its key and bytes across a retry.
    let keys = store.keys();
    assert_eq!(
        keys.len(),
        1,
        "a permanently unacknowledged PUT must retry ONE key forever, never mint new ones: \
         {keys:?}"
    );
    // Name which key, so a future reordering that changes what the archive
    // does first cannot leave this test quietly covering something else.
    assert_eq!(
        keys[0],
        snapshot_batch_key(PREFIX, 0, "Rec", 0),
        "the first object the archive writes is no longer the opening snapshot's first \
         batch; this test's subject moved and its comment above is now wrong"
    );
    let first = get_object(&store, &keys[0]).expect("the object landed despite the lost ack");
    for _ in 0..200 {
        let _ = arc.step(&db, &store);
    }
    assert_eq!(
        get_object(&store, &keys[0]).as_ref(),
        Some(&first),
        "a retry must re-PUT the SAME bytes, not rebuild them"
    );
}

/// Discharges spec §3's obligation. Deterministic in the simulator; NO
/// benchmark run — docs/BENCH.md is the only source of quotable numbers, it
/// has no archive measurements, and the box is memory-constrained.
#[test]
fn the_seal_rate_stays_under_one_upload_per_step_budget() {
    let (mut db, store, mut arc) = archive_fixture();
    // RULING 53: a rate measured over every sealed segment in the log is
    // measuring the OVERLAP, not the seal rate — the segments below the
    // generation's frontier are the snapshot's and are never shipped. So the
    // generation's snapshot is taken first, and the rate is measured over
    // what seals above it.
    write_records(&mut db, RECORDS_PER_SEGMENT + 50);
    step_until_idle(&mut arc, &db, &store);
    // Arithmetic from the measured constants rather than from the archive's
    // own bookkeeping (ruling 24): RECORDS_PER_SEGMENT + 50 records leave the
    // live tail based one first segment in, and that is where the chain
    // starts.
    let chain_start = FIRST_SEGMENT_RECORDS;
    assert!(
        store.keys().iter().all(|k| !k.contains("/segments/")),
        "a segment shipped before the rate window opened, so `uploads` below counts uploads \
         this test did not measure the seals for: {:?}",
        store.keys()
    );
    write_records(&mut db, 50 * RECORDS_PER_SEGMENT);
    let seals = db
        .sealed_segments()
        .unwrap()
        .iter()
        .filter(|s| s.base_seq >= chain_start)
        .count();
    // F4's sweep: `uploads >= seals` cannot fail when `seals` is 0, and a
    // seal count is a measurement, not a given. Guard it.
    assert!(
        seals >= 2,
        "fixture sealed {seals} segment(s) above the generation's frontier; a rate assertion \
         against 0 or 1 is not a rate"
    );
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
///
/// RE-MEASURED at Task 7: 47 -> 49. `State::Opening` puts a manifest GET in
/// front of everything (resume-or-open, ruling 43), and it costs exactly the
/// two steps every store operation costs here — one to `start`, one to
/// `poll`. The delta being 2 and not more is itself the check that Task 7
/// added a prologue rather than changing the shipping loop.
///
/// RE-MEASURED at Task 7.5 (ruling 53), and the run it counts is a different
/// one: the archive now RESUMES a planted generation taken at the frontier
/// of the first 5,000 records and ships only the segments that sealed over
/// the second 5,000, instead of opening a fresh generation and re-shipping
/// the whole log underneath its own snapshot. It is a step count of the cut
/// loop's run, nothing more — the number to keep honest, not to defend.
///
/// RE-MEASURED at Task 8 (spec §4 rule 2): 27 -> 35. Every manifest swap is
/// now preceded by a GET of the manifest it is about to replace
/// (`State::CheckingOwner`), because `ObjectStore` has no compare-and-swap
/// and reading first is the only way not to advance a manifest this node
/// does not own. This run resumes a planted generation and ships the four
/// segments that sealed above its frontier, so it performs four swaps: +8
/// steps, at the two every store operation costs here (one to `start`, one
/// to `poll`). The delta being exactly twice the swap count is itself the
/// check that Task 8 added a prologue per swap rather than changing the
/// shipping loop.
const STEPS_TO_ARCHIVE_5000_RECORDS: usize = 35;

/// The property this task exists to establish (spec §2): upload every object
/// of a generation, THEN swap the manifest. Inter-key ordering is not
/// guaranteed, so the manifest swap is the only operation whose completion
/// means anything — a crash at any earlier point must leave an older
/// COMPLETE generation still pointed at, never a torn one.
///
/// F10, for anyone mutating the archive against this test: the step-count
/// assertion below fires BEFORE the cut loop, so a mutation that changes how
/// many steps a run takes reddens on that assertion and never reaches the
/// ordering property at all. To prove the loop itself, mutate in a
/// step-count-neutral way — reorder the manifest PUT ahead of the meta PUT,
/// rather than removing a state.
#[test]
fn the_manifest_never_points_at_an_incomplete_generation() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 5_000);
    // RULING 53: capture a complete generation at THIS frontier, then write
    // past it. Every cut below replays against a store seeded with it, so
    // the archive under test resumes that generation and ships the segments
    // that sealed afterwards — see `replay_archive_to_step`. Without this
    // the loop would only ever see snapshot objects, and the manifest-swaps-
    // last property would go untested for the chain entirely.
    step_until_idle(&mut arc, &db, &store);
    let planted = store_contents(&store);
    assert!(
        planted.iter().any(|(k, _)| k.ends_with("/snapshot/meta"))
            && planted.iter().any(|(k, _)| k.ends_with("/manifest"))
            && planted.len() > 3,
        "the captured generation must be a COMPLETE published snapshot, or every cut below \
         resumes nothing: {:?}",
        planted.iter().map(|(k, _)| k).collect::<Vec<_>>()
    );
    // `arc` stays ALIVE across these writes, and that is load-bearing since
    // T9's F4 fix (`impl Drop for Archive`): its retention pin is the only
    // thing holding the segments the planted generation's chain continues
    // from. Dropping it first releases the pin, the checkpoint that these
    // 5,000 records drive GCs the log to base 6824 (MEASURED — it was 4094
    // with the archive holding), and every cut below then resumes a
    // generation whose chain start is already reclaimed: a manifest that
    // never grows a chain, and the ordering property untested for segments
    // entirely. Before F4 this read `drop(arc); write_records(...)` and
    // worked only because the dropped archive LEAKED its hold — the test
    // was resting on the defect. Alive-then-dropped is also the honest
    // model of what it means to test: an archive that was running while the
    // writes happened, and a process that then restarted.
    write_records(&mut db, 5_000);
    drop(arc);

    let steps = steps_to_idle(&mut db, &planted);
    assert_eq!(
        steps, STEPS_TO_ARCHIVE_5000_RECORDS,
        "the archive's step count moved; the cut loop below is derived from it, so re-measure \
         and update STEPS_TO_ARCHIVE_5000_RECORDS rather than leaving a loop that runs past \
         the end (every cut a no-op) or stops short of it"
    );

    // One past the end deliberately: the last cut must see a finished,
    // consistent generation, not merely a partial one.
    let mut cuts_with_a_chain = 0;
    for cut in 0..=steps {
        let (store, _arc) = replay_archive_to_step(&mut db, &planted, cut);
        let m = read_manifest(&store).unwrap_or_else(|| {
            panic!(
                "the planted generation's manifest is gone at cut {cut}; the manifest is the \
                 one mutable key in the layout and nothing may remove it"
            )
        });
        if !m.segments.is_empty() {
            cuts_with_a_chain += 1;
        }
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
    // implementation that never does anything." Every cut now starts from a
    // planted manifest, so "saw a manifest" is true by construction and no
    // longer says anything — what does is how many cuts saw the manifest
    // ADVANCED to name a chain, which is the half of the ordering property
    // that involves a segment at all.
    //
    // RE-MEASURED at Task 8: 21 of 28 -> 27 of 36. CONFIRMED as the first of
    // the two readings this message names, not the second: what the
    // ownership check adds is a two-step GET in front of each swap, so the
    // cuts that see NO chain — the prologue before the first chain-naming
    // manifest lands — went 7 -> 9, exactly one prologue, while the total
    // went 28 -> 36, exactly four. A manifest naming segments EARLIER in the
    // run would have moved that prefix the other way.
    assert_eq!(
        cuts_with_a_chain,
        27,
        "{cuts_with_a_chain} of the {} cuts saw a manifest naming a chain; MEASURED. Fewer \
         means the loop above checked the segment half of the ordering almost nowhere and its \
         silence means nothing; MORE means a manifest is naming segments earlier in the run \
         than the objects it names can have landed. Re-measure only after confirming which",
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
    // F11: written out rather than compared against `Db::table_names()`,
    // which is the very function `manifest_bytes` derives this list from
    // (ruling 24 — an oracle built from the function it polices cannot
    // catch that function being wrong). This fixture registers `Rec`; the
    // other three are the platform-owned tables `Db::open` adds, and they
    // are here because they carry state restore needs.
    assert_eq!(
        m.tables,
        vec![
            "Rec".to_string(),
            "charpente.events".to_string(),
            "charpente.jobs".to_string(),
            "charpente.schema_versions".to_string(),
        ],
        "the manifest must name every registered table, in schema (sorted) order"
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

    // RULING 53: the chain continues the generation's snapshot, so it ships
    // what seals above the frontier that snapshot was taken at — hence two
    // phases rather than one. Phase 2 stays BELOW the cadence deliberately:
    // another crossing would roll to a second generation whose own chain is
    // empty again, and the assertion below would be about that one.
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    snapshot_then_chain(&mut arc, &mut db, &store, 2 * RECORDS_PER_SEGMENT);
    let m = read_manifest(&store).expect("a manifest once the cadence was crossed");
    // RULING 51: `applied_seq >= cadence` is ONE RECORD STRICTER than the
    // contract Ruling 48 established — `snapshot_every` counts committed
    // records and `applied_seq` is a 0-based seq, so the Nth record's seq is
    // N - 1. It passes today only because this fixture overshoots the
    // boundary; against correct code sitting exactly on it, it is a false
    // RED inside the test named after that very cadence.
    assert!(
        m.applied_seq + 1 >= cadence,
        "snapshot taken at seq {} ({} records) but the cadence is {cadence} records",
        m.applied_seq,
        m.applied_seq + 1
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
    // And the pin sits exactly where the generation's chain starts, which is
    // where taking the snapshot parked it — never further forward, since
    // nothing has been acknowledged.
    //
    // RULING 53 re-baselines this from `db.log_base()`, which was only ever
    // right because the chain started there. Arithmetic from the two
    // measured constants (ruling 24): 3 * RECORDS_PER_SEGMENT records leave
    // the live tail based one first segment plus two steady-state ones in,
    // and that is the segment holding the first record the snapshot does not
    // carry. The manifest-gated half of this property — a pin that never
    // advances past a SEGMENT whose manifest has not landed — is asserted in
    // `a_lost_segment_ack_retries_the_same_key_with_the_same_bytes`, which
    // is the test that really does sit in `State::UploadingSegment`.
    assert_eq!(
        arc.pin_floor(),
        FIRST_SEGMENT_RECORDS + 2 * RECORDS_PER_SEGMENT,
        "the pin moved off the generation's chain start while its manifest was unacknowledged \
         (log_base {})",
        db.log_base()
    );
    assert!(
        !keys.iter().any(|k| k.contains("/segments/")),
        "the archive moved on to a segment while its manifest was unacknowledged: {keys:?}"
    );
}

/// F1 (task-6-review.md). Every other snapshot test here checks the SHAPE of
/// the snapshot — which keys exist, what the meta claims, that paging
/// happened — and until this one nothing decoded a
/// `<gen>/snapshot/<table>/<n>` object to ask whether the rows inside are
/// the rows the `Db` held. A snapshot that dropped the last row of every
/// batch, or wrote every payload blank, satisfies all of those tests and
/// restores silently wrong data; the CRC cannot catch it either, because the
/// archive would be checksumming its own mistake.
#[test]
fn the_snapshot_objects_hold_exactly_the_rows_the_db_held() {
    let rows = 3 * RECORDS_PER_SEGMENT;
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, rows);
    step_until_idle(&mut arc, &db, &store);

    let m = read_manifest(&store).expect("a manifest");
    let meta = read_snapshot_meta(&store, m.generation).expect("a snapshot meta");

    // Nothing is committed after `step_until_idle`, so a fresh snapshot is
    // the same MVCC view the archive walked. ASSERT that rather than assume
    // it: comparing rows across two different frontiers would report a
    // content bug for what is really just time passing.
    let snap = db.snapshot().unwrap();
    assert_eq!(
        snap.applied_seq().unwrap(),
        Some(meta.applied_seq),
        "the oracle's view sits at a different frontier than the snapshot's, which would make \
         every comparison below meaningless"
    );

    let rec = meta
        .tables
        .iter()
        .find(|t| t.name == "Rec")
        .expect("Rec in the meta");
    assert!(
        rec.batches >= 4,
        "Rec's {rows} rows landed in {} batch object(s); the point of this test is that content \
         survives PAGING, so a single batch would say nothing about the seams",
        rec.batches
    );

    // `Rec` gets the independent oracle (ruling 24): `write_records` wrote
    // ids 0..{rows} with `value == id`, so the archived bytes are checked
    // against what this test itself put in, decoded through the row's own
    // decoder — not against a re-read of the same `scan_raw_from` the
    // archive used to produce them.
    let archived = read_snapshot_rows(&store, m.generation, rec);
    assert_eq!(
        archived.len() as u64,
        rows,
        "the snapshot carries {} of the {rows} rows written",
        archived.len()
    );
    for (i, (id, bytes)) in archived.iter().enumerate() {
        let i = i as u64;
        assert_eq!(
            *id, i,
            "snapshot rows must be complete and in ascending id order; row {i} of the \
             concatenated batches has id {id}"
        );
        let r = Rec::decode_row(bytes).unwrap_or_else(|e| {
            panic!(
                "snapshot row {id} does not decode as a Rec ({e}); {} bytes: {bytes:?}",
                bytes.len()
            )
        });
        assert_eq!(
            (r.id.raw(), r.value),
            (i, i),
            "snapshot row {id} came back holding different data than was written"
        );
    }

    // The platform-owned tables have no such independent expectation, so
    // they are checked against the Db view at the snapshot's own frontier.
    let mut tables_checked = 0;
    for t in &meta.tables {
        let archived = read_snapshot_rows(&store, m.generation, t);
        let live = snap.scan_raw_from(&t.name, None, usize::MAX).unwrap();
        assert_eq!(
            archived,
            live,
            "table {:?}: the snapshot's {} row(s) differ from the {} the Db holds at the same \
             frontier",
            t.name,
            archived.len(),
            live.len()
        );
        tables_checked += 1;
    }
    assert_eq!(
        tables_checked,
        db.table_names().len(),
        "the loop above must cover the whole schema, or an untouched table is an unchecked one"
    );
}

/// F2 (task-6-review.md) — ruling 42's headline property, and the reason
/// this task declined the brief's literal repeated cadence: a generation
/// carries EXACTLY ONE snapshot.
///
/// `<gen>/snapshot/**` keys are immutable once written — spec §2, "the
/// manifest is the only mutable key in the layout" — so a second snapshot
/// taken into a live generation would overwrite the very objects the current
/// manifest still points at. That is a torn generation reachable with no
/// crash and no mis-ordering at all, which is why it is a separate property
/// from `the_manifest_never_points_at_an_incomplete_generation` rather than
/// a case of it. A refresh is a NEW generation (Task 7), never a rewrite of
/// this one.
#[test]
fn a_generation_never_takes_a_second_snapshot() {
    let cadence = 2 * RECORDS_PER_SEGMENT;
    let (mut db, store, mut arc) = archive_fixture_with_snapshot_cadence(cadence);

    write_records(&mut db, cadence);
    step_until_idle(&mut arc, &db, &store);
    // Exactly `cadence` records, not one more: `snapshot_every` counts
    // committed RECORDS (spec §3) while `Db::last_applied` reports the
    // highest 0-based SEQ, so the cadence must be crossed by the
    // {cadence}th record — one late is a real, silent unit bug (found by
    // this test).
    assert_eq!(
        db.last_applied(),
        Some(cadence - 1),
        "the fixture must have committed exactly {cadence} records for the cadence boundary \
         below to be the boundary"
    );
    let before = read_manifest(&store).expect("a manifest once the cadence was crossed");
    let objects_before = snapshot_objects(&store, before.generation, &store.keys());
    assert!(
        objects_before.len() > 2,
        "phase 1 left {} snapshot object(s); with nothing much written there is nothing much to \
         overwrite and phase 2 proves little",
        objects_before.len()
    );

    // Cross the cadence again, four times over.
    write_records(&mut db, 4 * cadence);
    step_until_idle(&mut arc, &db, &store);
    let after = read_manifest(&store).expect("a manifest");

    // Anti-vacuity first (spec §11): "nothing changed" is trivially true of
    // an archive that did nothing in phase 2. Task 7 changed what phase 2's
    // progress LOOKS like — a cadence crossing now opens a new generation
    // (carried obligation (b)) instead of extending generation 0 — so the
    // signal is the roll, not a longer chain.
    assert!(
        after.generation > before.generation,
        "the archive never crossed the cadence in phase 2 (generation {} -> {}), so it never \
         reached the refresh this test exists to survive",
        before.generation,
        after.generation
    );
    assert!(
        after.applied_seq > before.applied_seq,
        "the new generation's snapshot sits at the CLOSED generation's frontier ({}), so the \
         refresh copied a frontier instead of taking one",
        before.applied_seq
    );
    // THE PROPERTY, and it is unchanged by Task 7: a generation's snapshot
    // objects are written ONCE. They are immutable (spec §2 — the manifest
    // is the layout's only mutable key), so overwriting them replaces
    // content a manifest pointed at, which tears a generation without any
    // crash or reordering at all. That is exactly why a refresh has to be a
    // new generation rather than a second snapshot in place.
    assert_snapshot_objects_unchanged(
        &snapshot_objects(&store, before.generation, &store.keys()),
        &objects_before,
        "a refreshed snapshot was written into generation 0's keys instead of a new \
         generation's.",
    );
    // And the frontier a restore would read is this generation's OWN
    // snapshot's, never one carried across the roll.
    let meta = read_snapshot_meta(&store, after.generation).expect("the new generation's meta");
    assert_eq!(
        after.applied_seq, meta.applied_seq,
        "the manifest's frontier is not the frontier its own snapshot was taken at, so a \
         restore would replay from a point that snapshot never reached"
    );
}

/// F3 (task-6-review.md). Task 5 aimed `object_lost_ack: 1.0` at every
/// object, which at the time meant the SEGMENT PUT — the only lost-ack
/// coverage that arm ever had. Under Task 6's ordering the archive never
/// reaches a segment before the snapshot's first batch, so that coverage
/// silently became coverage of a batch PUT instead. This restores it by
/// aiming the fault at `/segments/` alone.
#[test]
fn a_lost_segment_ack_retries_the_same_key_with_the_same_bytes() {
    let (mut db, store, mut arc) = archive_fixture_with_lost_segment_acks();
    // RULING 53: the fault is aimed at `/segments/`, and a segment only
    // ships once one seals ABOVE the generation's frontier. Phase 1 takes
    // the snapshot (unfaulted, since nothing it writes is a segment); phase
    // 2 is what this test is about.
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);

    let e = step_until_error(&mut arc, &db, &store);
    assert!(
        e.to_string().contains("simulated FlakyStore lost ack"),
        "expected the lost-ack text from a segment PUT, got: {e}"
    );
    // Anti-vacuity: the fault only fires on `/segments/`, so reaching it at
    // all proves the snapshot and its manifest completed first — this test
    // really is sitting in `State::UploadingSegment`, not somewhere earlier.
    let key = {
        let segments: Vec<String> = store
            .keys()
            .into_iter()
            .filter(|k| k.contains("/segments/"))
            .collect();
        assert_eq!(
            segments.len(),
            1,
            "expected exactly one segment object in flight, found: {segments:?}"
        );
        segments.into_iter().next().unwrap()
    };
    let first = get_object(&store, &key).expect("the segment object landed despite the lost ack");

    // Keep stepping: every attempt fails the same way, forever.
    for _ in 0..200 {
        let _ = arc.step(&db, &store);
    }

    assert_eq!(
        get_object(&store, &key).as_ref(),
        Some(&first),
        "a segment retry must re-PUT the SAME bytes, not re-read the log and rebuild them"
    );
    let keys = store.keys();
    assert_eq!(
        keys.iter().filter(|k| k.contains("/segments/")).count(),
        1,
        "a lost-ack retry minted a second segment key instead of reusing the first: {keys:?}"
    );
    // And the manifest never names a segment the archive was not told had
    // landed — the ordering property, seen from the segment's side.
    let m = read_manifest(&store).expect("the opening snapshot's manifest");
    assert!(
        m.segments.is_empty(),
        "the manifest names a segment whose PUT was never acknowledged: {:?}",
        m.segments
    );
    // And the pin has not advanced past that segment either: a segment
    // counts as reflected only once the manifest PUT naming it has landed
    // (carried here from `a_lost_manifest_ack_…`, whose own archive never
    // reaches a segment at all once the chain continues its snapshot).
    // Arithmetic from the two measured constants: phase 1's 3 *
    // RECORDS_PER_SEGMENT records leave the chain starting one first segment
    // plus two steady-state ones in, and that is the segment in flight.
    assert_eq!(
        arc.pin_floor(),
        FIRST_SEGMENT_RECORDS + 2 * RECORDS_PER_SEGMENT,
        "the pin advanced past a segment whose PUT the archive was never told had landed"
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
/// `step_until_notice`, which was `step_until_error` in substance (its own
/// doc), so the two bodies were identical bar the helper name.
///
/// Task 7 separated them for real (Defect 4): `step_until_notice` now SKIPS
/// retryable IO errors, so it can no longer stand in here — this fixture's
/// one segment never crosses the bound and no notice ever arrives. It calls
/// `step_until_error` now, and `step_until_notice` is exercised by
/// `the_gap_notice_names_the_bound_the_prefix_and_both_fixes`, which is the
/// test it was named for (Ruling 31 is satisfied by both being used, not by
/// both being used here).
#[test]
fn step_until_error_surfaces_the_unreachable_stores_failure() {
    let (mut db, store, mut arc) = archive_fixture_with_unreachable_store();
    write_records(&mut db, RECORDS_PER_SEGMENT + 50);
    let e = step_until_error(&mut arc, &db, &store);
    assert!(
        e.to_string().contains("FlakyStore outage"),
        "expected the unreachable store's own failure text, got: {e}"
    );
}

// ------------------------- Task 7: the bounded pin, and the named gap

/// SPEC RULING 3 (§5), the OUTAGE case: on outage the archive yields, never
/// the primary. An unbounded pin means an object-store outage grows the WAL
/// until the disk fills and the log poisons — the archive would have
/// acquired the power to stop the thing it derives from.
///
/// RULING 49: the pin releases by STATE, not by VALUE. `pin_floor()` keeps
/// reporting a real, monotonic floor throughout (writing `u64::MAX` into it
/// is verbatim the M1 bug `hold()`'s assert exists to catch); what changes
/// is that `Db::retention_floor` stops consulting it.
#[test]
fn past_the_bound_gc_proceeds_and_the_primary_keeps_writing() {
    let (mut db, store, mut arc) = archive_fixture_with_unreachable_store();
    let base_before = db.log_base();
    write_records(&mut db, 20 * RECORDS_PER_SEGMENT); // far past pin_segments
    let sealed = sealed_segment_count(&db);
    assert!(
        sealed > GAP_PIN_SEGMENTS,
        "fixture sealed {sealed} segment(s) against a bound of {GAP_PIN_SEGMENTS}; the bound is \
         never crossed and every assertion below is vacuous"
    );
    step_until_idle(&mut arc, &db, &store);
    db.checkpoint().unwrap();
    assert!(
        db.log_base() > base_before,
        "GC did not proceed past {base_before}; the archive stalled the primary"
    );
    assert!(
        !db.poisoned(),
        "an archive failure is not a database failure"
    );
    write_records(&mut db, 10); // still writable
    assert!(arc.gapped(), "a released pin must record a gap");
    // Ruling 49, both halves. The pin is not consulted...
    assert_eq!(
        db.retention_floor(),
        u64::MAX,
        "the archive is still holding a floor Wal::gc must respect, so the release did not \
         happen where it has to happen — in what the registered closure reports"
    );
    // ...and yet the floor it carries is a real seq, never the sentinel.
    assert_ne!(
        arc.pin_floor(),
        u64::MAX,
        "the pin released BY VALUE — verbatim the M1 bug hold()'s assert exists to catch, and \
         the value pin_floor()'s own doc promises it never returns"
    );

    // And GC keeps proceeding, which is the half an outage actually tests.
    // The plan's assertion above — `log_base` advanced once — passes against
    // an archive that never released at all, because the gap ALSO forfeits
    // the sealed segments it was holding and moves the floor to the live
    // tail; one checkpoint after that reclaims a great deal either way
    // (measured: this exact mutation leaves that assertion green). What
    // separates "yielded" from "still holding, one segment higher" only
    // shows over a longer outage: the log keeps growing, and a floor that is
    // still consulted holds everything above it forever.
    let floor_at_gap = arc.pin_floor();
    write_records(&mut db, 20 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);
    db.checkpoint().unwrap();
    assert!(
        db.log_base() > floor_at_gap,
        "gc stalled at the floor the archive held when it gapped ({floor_at_gap}); a yielded \
         pin must stop being CONSULTED, not merely stop advancing — the outage continues and \
         the WAL grows behind it"
    );
    assert!(
        !db.poisoned(),
        "an archive failure is not a database failure"
    );
}

/// HARD RULE 5, and the whole operator surface of this task: a gap is the
/// one thing that silently costs data continuity, so the notice must name
/// the bound, the elapsed segments, the prefix, and BOTH fixes (spec §7).
#[test]
fn the_gap_notice_names_the_bound_the_prefix_and_both_fixes() {
    let (mut db, store, mut arc) = archive_fixture_with_unreachable_store();
    write_records(&mut db, 20 * RECORDS_PER_SEGMENT);
    let notice = step_until_notice(&mut arc, &db, &store);
    let t = notice.to_string();
    for needle in ["pin_segments", PREFIX, "raise", "generation"] {
        assert!(t.contains(needle), "notice must name {needle}: {t}");
    }
    // Spec §7 names two more the plan's needle list does not: the bound's
    // VALUE (an operator deciding whether to raise it needs to know what it
    // is) and the elapsed segments (how far behind the archive actually got).
    assert!(
        t.contains(&GAP_PIN_SEGMENTS.to_string()),
        "notice must name the configured bound's value ({GAP_PIN_SEGMENTS}), not just the \
         field's name: {t}"
    );
    assert!(
        t.contains("segment"),
        "notice must name the elapsed segments (spec §7): {t}"
    );
    // F-C. Two different generations are involved in a gap and the notice
    // has to keep them apart: the one being ABANDONED (whose already-written
    // objects nobody collects) and the one the manifest names, which is what
    // `ClosingGeneration` marks closed. An earlier wording said the
    // abandoned one was "closed", which is the one thing that is not true of
    // it — and the orphans it leaves went unmentioned entirely.
    assert!(
        t.contains("orphaned"),
        "notice must say the abandoned generation's objects are orphaned; nothing collects them \
         and the operator is the only one who can: {t}"
    );

    // "ONE operator notice" (spec §7), not one per step: a gap is a latch.
    // `pin_segments` appears in no other error this archive can raise, so
    // its reappearance is exactly a second notice.
    for _ in 0..500 {
        if let Err(e) = arc.step(&db, &store) {
            assert!(
                !e.to_string().contains("pin_segments"),
                "the gap notice repeated; spec §7 asks for one, and a per-step notice is how an \
                 operator learns to filter the one message that matters: {e}"
            );
        }
    }
}

/// SPEC §2: a gap closes a generation, and the next successful contact opens
/// a new one with a fresh FULL snapshot — restore never reasons across a
/// hole, so a generation is either continuous or it is finished.
///
/// RULING 50: generations are 0-based (Task 6 opens 0), so ONE gap opens
/// generation 1. The plan asserted 2, which only a DOUBLE increment would
/// have satisfied — the assertion would have concealed the one bug it looked
/// like it was testing. The second half below pins the increment itself.
#[test]
fn after_a_gap_the_next_contact_opens_a_new_generation_with_a_full_snapshot() {
    let (mut db, store, mut arc) = archive_fixture_with_unreachable_store();
    write_records(&mut db, 20 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);
    assert!(
        arc.gapped(),
        "the fixture never crossed the bound, so there is no gap to recover from"
    );

    restore_store_reachability(&store);
    // The gap forfeited every sealed segment the archive was holding — the
    // new generation's own snapshot covers them — so the chain it builds is
    // of what comes AFTER. Write that, or `!m.segments.is_empty()` below is
    // asserting the archive re-shipped what it just gave up.
    step_until_idle(&mut arc, &db, &store);
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);
    let m = read_manifest(&store).expect("a generation must land");
    assert_eq!(
        m.generation, 1,
        "a gap must close a generation and open the next"
    );
    assert!(
        !m.segments.is_empty() && m.applied_seq > 0,
        "the new generation needs its own snapshot and chain: {m:?}"
    );
    // Its OWN full snapshot, in its own keys — not a manifest pointing back
    // at the closed generation's objects.
    let meta = read_snapshot_meta(&store, 1).expect("generation 1's own snapshot meta");
    assert_eq!(
        meta.applied_seq, m.applied_seq,
        "the new generation's manifest frontier is not its own snapshot's"
    );
    assert_eq!(
        meta.tables.len(),
        db.table_names().len(),
        "a gap's recovery snapshot must be FULL — every table, empty ones included: {:?}",
        meta.tables
    );
    assert!(
        !arc.gapped(),
        "the archive is building a continuous generation again; `gapped` says it is still \
         yielding, and the kernel arm reads it to decide whether the pin is live"
    );

    // RULING 50's other half: EXACTLY one per gap. A counter that advanced
    // by two would have made the plan's `2` correct after one gap, so
    // nothing above can tell the two apart — this can.
    store.set_reachable(false);
    write_records(&mut db, 20 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);
    assert!(arc.gapped(), "the second outage never crossed the bound");
    restore_store_reachability(&store);
    step_until_idle(&mut arc, &db, &store);
    let m2 = read_manifest(&store).expect("a second recovered generation");
    assert_eq!(
        m2.generation, 2,
        "the generation counter must advance by exactly one per gap: two gaps from 0 is 2"
    );
}

/// RULING 43 / carried obligation (a) — the restart hazard Task 6 flagged in
/// its module doc and was allowed to defer only because nothing wires the
/// archive into a kernel arm until Task 9. A restart re-opened generation 0
/// and would re-snapshot into its keys: a generation torn by a RESTART
/// rather than by a cadence, and the same immutability violation
/// `a_generation_never_takes_a_second_snapshot` exists to prevent.
#[test]
fn a_restarted_archive_resumes_the_generation_the_bucket_names() {
    let (mut db, store, mut arc) = archive_fixture();
    // RULING 53: a resumable generation needs a chain to resume, and a chain
    // only exists once a segment seals above the snapshot's frontier.
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    snapshot_then_chain(&mut arc, &mut db, &store, 3 * RECORDS_PER_SEGMENT);
    let before = read_manifest(&store).expect("a manifest");
    let objects_before = snapshot_objects(&store, before.generation, &store.keys());
    assert!(
        objects_before.len() > 2 && !before.segments.is_empty(),
        "phase 1 left {} snapshot object(s) and {} segment(s); with nothing much archived there \
         is nothing much for a restart to resume",
        objects_before.len(),
        before.segments.len()
    );
    drop(arc);

    // The restart: a fresh `Archive` over the same Db and the same bucket.
    let mut restarted = Archive::open(archive_config(0, 8 << 20), &mut db).unwrap();
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut restarted, &db, &store);
    let after = read_manifest(&store).expect("a manifest after the restart");

    assert_eq!(
        after.generation, before.generation,
        "the restart opened a new generation over a resumable one; the bucket's manifest names \
         generation {} and nothing about it was closed or holed",
        before.generation
    );
    assert!(
        after.segments.len() > before.segments.len(),
        "the restarted archive shipped nothing ({} segments before, {} after), so 'it resumed' \
         is vacuous",
        before.segments.len(),
        after.segments.len()
    );
    assert_eq!(
        after.segments[..before.segments.len()],
        before.segments[..],
        "the resumed chain must EXTEND the one the manifest named, not restart it: {:?} vs {:?}",
        after.segments,
        before.segments
    );
    assert_eq!(
        after.applied_seq, before.applied_seq,
        "the restart re-snapshotted: a generation carries exactly ONE snapshot, and its frontier \
         is fixed when that snapshot is taken"
    );
    assert_snapshot_objects_unchanged(
        &snapshot_objects(&store, before.generation, &store.keys()),
        &objects_before,
        "a restart re-snapshotted into the live generation's keys (Ruling 43's restart hazard).",
    );
}

/// RULING 43 / carried obligation (b) — the seq-cadenced refresh Task 6
/// deferred, and an exit-criteria line ("Archive arm ships sealed segments
/// and seq-cadenced snapshots"). A cadence crossing opens a NEW generation
/// with a fresh full snapshot, which is the same transition a gap performs
/// on a different trigger. `a_generation_never_takes_a_second_snapshot`
/// covers the immutability half; this covers that a refresh happens at all,
/// and that the new generation is whole rather than a pointer to the old one.
#[test]
fn a_cadence_crossing_opens_a_new_generation_with_its_own_full_snapshot() {
    let cadence = 2 * RECORDS_PER_SEGMENT;
    let (mut db, store, mut arc) = archive_fixture_with_snapshot_cadence(cadence);
    write_records(&mut db, cadence);
    step_until_idle(&mut arc, &db, &store);
    let before = read_manifest(&store).expect("a manifest once the cadence was crossed");
    assert_eq!(
        before.generation, 0,
        "the opening generation is 0 (Ruling 50)"
    );

    write_records(&mut db, cadence);
    step_until_idle(&mut arc, &db, &store);
    let after = read_manifest(&store).expect("a manifest after the second crossing");

    assert_eq!(
        after.generation, 1,
        "a cadence crossing must open a new generation; a second snapshot into generation 0's \
         keys would overwrite what the live manifest points at"
    );
    assert!(
        after.applied_seq >= before.applied_seq + cadence,
        "the refreshed snapshot sits at seq {} but the previous one was at {} and the cadence \
         is {cadence} records; the refresh did not re-read the Db",
        after.applied_seq,
        before.applied_seq
    );
    let meta = read_snapshot_meta(&store, 1).expect("generation 1's own snapshot meta");
    assert_eq!(
        meta.tables.len(),
        db.table_names().len(),
        "a refresh must be a FULL snapshot — every table, empty ones included: {:?}",
        meta.tables
    );
    assert_eq!(
        after.applied_seq, meta.applied_seq,
        "the refreshed manifest's frontier is not its own snapshot's"
    );
    // A refresh is not a gap: nothing was lost, so no notice and no yield.
    assert!(
        !arc.gapped(),
        "a cadence refresh recorded a gap; a gap means records fell out of the archive's chain, \
         and nothing here did"
    );
}

/// RULING 46 / Task 6 review F5 / carried obligation (c) — the
/// CONFIGURATION-triggered WAL growth Task 6 introduced. The archive ships
/// nothing at all until its generation's opening snapshot lands, and while
/// it waited it held at the log's base — so a `snapshot_every` the Db has
/// not crossed yet froze `Wal::gc` against a perfectly healthy store. At
/// Task 5 the same hazard needed an outage.
///
/// The fix is not to bound that wait but to REMOVE it: an archive with no
/// generation open needs nothing from the log at all — its chain has not
/// started, and whatever it eventually snapshots will name its own frontier.
/// Task 6's implementer put the sharp version of why bounding it is wrong:
/// "a bound sized only for outages may be sized wrong for a cadence that has
/// simply not arrived". Waiting is not a gap and must not spend one.
#[test]
fn an_archive_waiting_for_its_cadence_holds_nothing_against_a_healthy_store() {
    // Far past anything this test writes, so the cadence never arrives.
    let cadence = 100 * RECORDS_PER_SEGMENT;
    let (mut db, store, mut arc) = archive_fixture_with_snapshot_cadence(cadence);
    let base_before = db.log_base();
    write_records(&mut db, 20 * RECORDS_PER_SEGMENT);
    assert!(
        sealed_segment_count(&db) > 2,
        "the fixture sealed nothing to reclaim; 'gc advanced' would be vacuous"
    );
    step_until_idle(&mut arc, &db, &store);
    assert!(
        store.keys().is_empty(),
        "the cadence is not reached, so the archive must ship nothing: {:?}",
        store.keys()
    );

    db.checkpoint().unwrap();
    assert!(
        db.log_base() > base_before,
        "the archive froze Wal::gc at {base_before} while waiting for a cadence it has not \
         reached, against a healthy store (Ruling 46). It has no generation open, so it needs \
         nothing from the log"
    );
    assert!(
        !arc.gapped(),
        "waiting for a cadence is not a gap: nothing fell out of a chain that has not started"
    );
    assert_ne!(
        arc.pin_floor(),
        u64::MAX,
        "ruling 49: the floor stays a real, monotonic seq even while the pin holds nothing"
    );

    // And it still archives once the cadence does arrive — the release is a
    // yield, not a shutdown. Two phases (ruling 53): crossing the cadence
    // buys the generation its snapshot, and the chain continuing it carries
    // what seals above that.
    write_records(&mut db, 100 * RECORDS_PER_SEGMENT);
    snapshot_then_chain(&mut arc, &mut db, &store, 3 * RECORDS_PER_SEGMENT);
    let m = read_manifest(&store).expect("a manifest once the cadence was finally crossed");
    assert!(
        !m.segments.is_empty(),
        "the archive never resumed shipping after its cadence arrived: {m:?}"
    );
    // F-E: asserted as the INVARIANT, not as today's chain-start rule. The
    // chain must claim nothing the log no longer holds, and must not begin
    // above its own snapshot's frontier (which would leave the records
    // between them in neither). Pinning `== db.log_base()` instead would
    // hard-code the behaviour Ruling 53 is about to change and turn this
    // into a false RED against the fix.
    let start = m
        .segments
        .first()
        .map(|s| s.base_seq)
        .expect("checked non-empty above");
    assert!(
        start >= db.log_base() && start <= m.applied_seq + 1,
        "the resumed chain starts at {start}, outside [log_base {}, frontier+1 {}]: below the \
         log's base it claims a span nothing backs, above the frontier it leaves the records \
         between the snapshot and the chain in no generation at all",
        db.log_base(),
        m.applied_seq + 1
    );
}

/// SPEC §2, and the one thing `Manifest::closed` exists for (Task 4 built
/// the field; nothing until now wrote it). Between a gap and the fresh
/// generation's snapshot landing — which is a full snapshot's worth of
/// uploads, so not a short window — the manifest still names the generation
/// the gap broke. "Stale" and "finished" are different facts to a restore,
/// so closing it is the first thing that lands when contact returns, before
/// any object of the next generation.
#[test]
fn a_gap_marks_the_generation_it_broke_closed_before_opening_the_next() {
    // The unreachable fixture for its reachability switch and its small
    // bound; restored immediately, because phase 1 needs a healthy
    // generation 0 for the gap in phase 2 to have something to close.
    let (mut db, store, mut arc) = archive_fixture_with_unreachable_store();
    restore_store_reachability(&store);
    // Two phases (ruling 53): phase 1 buys generation 0 its snapshot, phase 2
    // the chain continuing it — a generation with no chain is not the "live
    // generation with a chain" this test needs something to close.
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    snapshot_then_chain(&mut arc, &mut db, &store, 3 * RECORDS_PER_SEGMENT);
    let before = read_manifest(&store).expect("generation 0's manifest");
    assert!(
        !before.closed && !before.segments.is_empty(),
        "phase 1 must leave a live generation with a chain: {before:?}"
    );

    store.set_reachable(false);
    write_records(&mut db, 20 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);
    assert!(arc.gapped(), "the fixture never crossed the bound");
    restore_store_reachability(&store);
    assert_eq!(
        read_manifest(&store).map(|m| m.closed),
        Some(false),
        "an unreachable store cannot be told anything, so the manifest must still be exactly \
         what generation 0 left"
    );

    // Step until the NEXT generation's first object appears — read off the
    // key listing, which starts no operation of its own (`read_manifest`
    // would `start` a GET, and a step that has just started a PUT leaves one
    // in flight, which `ObjectStore::start` asserts against).
    let next_generation = format!("{PREFIX}/{:020}/", before.generation + 1);
    for _ in 0..10_000 {
        if store.keys().iter().any(|k| k.starts_with(&next_generation)) {
            break;
        }
        let _ = arc.step(&db, &store);
    }
    let closed = read_manifest(&store).expect("a manifest");
    assert!(
        closed.closed,
        "the fresh generation began uploading while the manifest still named generation {} as \
         LIVE; a restore reading it there cannot tell a stale generation from a finished one",
        before.generation
    );
    assert_eq!(
        closed.generation, before.generation,
        "closing must mark the generation the gap broke, not some other one"
    );
    assert_eq!(
        closed.segments, before.segments,
        "closing a generation must not change what it contains — every object it names is still \
         in the bucket and still exactly what it was"
    );
    assert_eq!(
        closed.applied_seq, before.applied_seq,
        "closing a generation must not move its frontier"
    );
}

/// F-A (task-7-review.md). A restart against a HEALTHY store must not
/// declare a gap — and this is deliberately sited where the boundary
/// actually is. `a_restarted_archive_resumes_the_generation_the_bucket_names`
/// runs at `PIN_SEGMENTS` (64) over three segments, so the oracle for
/// resume-or-open existed but sat where a false gap is unobservable.
///
/// The defect: `yield_or_bound` ran BEFORE `State::Opening` had read the
/// bucket, against a `next_needed` that `open` sets to `db.log_base()` as a
/// provisional hold — so every already-archived segment counted as backlog.
/// The first `step` returned the gap notice, latched `gapped`, and
/// `resume_point`'s `if self.gapped` then defeated obligation (a) outright.
#[test]
fn a_restart_over_an_archived_log_is_not_a_gap() {
    let (mut db, store, mut arc) =
        build_fixture_with_cadence(SimObjectStore::new(), 0, 8 << 20, GAP_PIN_SEGMENTS);
    // A SECOND floor, the way spec §5 says the pin must compose: a
    // replication cursor holding the log at its base. Without it the
    // archive's own advancing pin lets gc reclaim what it has archived and a
    // restart sees a two-segment log — which is exactly the shape that hid
    // this defect.
    db.add_retention_pin(Box::new(|| 0));
    // Archived INCREMENTALLY, a segment at a time. Writing all twelve first
    // and then stepping would leave the archive twelve behind against a
    // bound of eight on its very first step, which is a real gap and not the
    // false one under test — an archive may not hold more than its bound
    // even to catch up. This is the state a healthy box is actually in: it
    // archives as it writes, and something else (here a replication cursor)
    // is what keeps the segments on disk afterwards.
    for _ in 0..12 {
        write_records(&mut db, RECORDS_PER_SEGMENT);
        step_until_idle(&mut arc, &db, &store);
    }
    let before = read_manifest(&store).expect("a manifest");
    let sealed = sealed_segment_count(&db);
    assert!(
        sealed > GAP_PIN_SEGMENTS,
        "the fixture left {sealed} sealed segment(s) against a bound of {GAP_PIN_SEGMENTS}; a \
         FALSE gap is not even possible here and the test proves nothing"
    );
    // RULING 53 re-baselines what "nothing is backlog" means. The segments
    // BELOW this generation's frontier are its snapshot's — archiving them
    // is the overlap ruling 53 removes — so the quantity that must already
    // be archived is the sealed segments at or above the chain's own start.
    let start = before
        .segments
        .first()
        .expect("phase 1 must have shipped a chain")
        .base_seq;
    // `start` is read out of the artifact this test polices, so the count
    // below can only catch a HOLE above it, never a wrong ORIGIN (ruling 24:
    // an oracle built from the same output it polices proves nothing about
    // that output). Pin the origin separately, against the generation's own
    // frontier: the chain's head must be the segment CONTAINING the first
    // record the snapshot does not carry.
    assert!(
        start <= before.applied_seq + 1 && before.segments[0].end_seq > before.applied_seq,
        "the chain's head [{}, {}] does not hold the first record its snapshot does not \
         carry ({}), so the count below is measured from the wrong origin",
        start,
        before.segments[0].end_seq,
        before.applied_seq + 1
    );
    let above = db
        .sealed_segments()
        .unwrap()
        .iter()
        .filter(|s| s.base_seq >= start)
        .count();
    assert_eq!(
        before.segments.len(),
        above,
        "every sealed segment at or above the chain's start ({start}) must already be \
         archived, so none of them is backlog: {:?}",
        before.segments
    );
    drop(arc);

    let mut restarted = Archive::open(
        archive_config_bounded(0, 8 << 20, GAP_PIN_SEGMENTS),
        &mut db,
    )
    .unwrap();
    if let Err(e) = restarted.step(&db, &store) {
        panic!(
            "a restart over a FULLY ARCHIVED log failed on its very first step, before it had \
             read the bucket at all: {e}"
        );
    }
    assert!(
        !restarted.gapped(),
        "a restart against a healthy store recorded a gap; nothing was lost and nothing was \
         even behind"
    );

    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut restarted, &db, &store);
    assert!(
        !restarted.gapped(),
        "the restarted archive gapped while catching up on a healthy store"
    );
    let after = read_manifest(&store).expect("a manifest after the restart");
    assert_eq!(
        after.generation, before.generation,
        "a false gap defeated resume-or-open: `resume_point` refuses while `gapped`, so the \
         restart opened a new generation over a perfectly resumable one"
    );
    assert_eq!(
        after.applied_seq, before.applied_seq,
        "the restart re-snapshotted; a generation carries exactly ONE snapshot"
    );
    assert!(
        after.segments.len() > before.segments.len(),
        "the restarted archive shipped nothing ({} -> {}), so 'it resumed' is vacuous",
        before.segments.len(),
        after.segments.len()
    );
}

// ------------------ Task 7.5: the chain CONTINUES the snapshot (ruling 53)

/// A `Db` whose log already holds far more sealed segments than the bound
/// allows by the time `Archive::open` runs, with every one of them still on
/// disk — the shape acceptance criterion 1 is about.
///
/// The second retention pin is not decoration: without it the default
/// checkpoint cadence reclaims as the writes go in and the archive opens on
/// a short log, which is precisely the shape that cannot exhibit the
/// startup gap. Spec §5 says the pin composes as a min over pins, so a
/// replication cursor holding the log at its base is the honest way to put
/// those segments there.
fn fixture_with_a_long_unarchived_log() -> (Db, SimObjectStore, Archive) {
    let sim = Sim::with_faults(0x6172_6368_5f74_3502, FaultConfig::none());
    let mut db_cfg = DbConfig::default();
    db_cfg.wal.segment_bytes = 64 << 10;
    let mut db = Db::open(Box::new(sim.disk()), db_cfg, Schema::new().table::<Rec>()).unwrap();
    db.add_retention_pin(Box::new(|| 0));
    write_records(&mut db, 20 * RECORDS_PER_SEGMENT);
    let arc = Archive::open(
        archive_config_bounded(0, 8 << 20, GAP_PIN_SEGMENTS),
        &mut db,
    )
    .unwrap();
    (db, SimObjectStore::new(), arc)
}

/// A manifest THIS archive's own writer left behind, naming a generation
/// whose snapshot landed and whose chain has not started.
///
/// `resume_point`'s no-chain-yet arm is reachable from a test no other way:
/// an archive that overlaps its snapshot ships a segment before its first
/// manifest can ever be read back with an empty chain, so the arm only has
/// a natural producer once ruling 53 is in place. Writing the manifest by
/// hand makes the arm testable from either side of the change.
fn write_own_manifest(store: &SimObjectStore, db: &Db, generation: u64, applied_seq: u64) {
    let m = Manifest {
        generation,
        writer: WRITER.to_string(),
        applied_seq,
        next_job_id: 0,
        tables: db.table_names().iter().map(|t| t.to_string()).collect(),
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

/// RULING 53, acceptance criterion 1. An archive opening on a log with more
/// sealed segments than its bound must SNAPSHOT that log, not gap on it.
///
/// Task 7 left this as a known symptom and accepted it only because ruling
/// 53 removes it: the generation's snapshot already covers every one of
/// those segments, so once the chain starts at the snapshot's frontier the
/// backlog is zero and there is nothing for the bound to be over.
#[test]
fn an_archive_opening_on_a_long_log_snapshots_it_instead_of_gapping() {
    let (db, store, mut arc) = fixture_with_a_long_unarchived_log();
    let sealed = sealed_segment_count(&db);
    assert!(
        sealed > GAP_PIN_SEGMENTS,
        "the fixture left {sealed} sealed segment(s) against a bound of {GAP_PIN_SEGMENTS}; the \
         bound is never in question and this test proves nothing"
    );
    // Measured BEFORE the archive steps, so the expectation below is not
    // read out of the artifact it polices: nothing is written after this
    // point, so the live tail's base does not move.
    let tail_at_open = current_tail_base_seq(&db);
    for _ in 0..10_000 {
        if let Err(e) = arc.step(&db, &store) {
            panic!(
                "an archive opening on a {sealed}-segment log against a HEALTHY store failed: \
                 {e}"
            );
        }
        if arc.idle() {
            break;
        }
    }
    assert!(
        arc.idle(),
        "the archive never reached idle; the assertions below would be about a half-finished run"
    );
    assert!(
        !arc.gapped(),
        "an archive that has just snapshotted a {sealed}-segment log recorded a gap; the \
         snapshot covers every one of those segments, so nothing was lost and nothing is behind"
    );
    // Spec §3, in the WAL sense that is the whole point of taking a snapshot:
    // it RELEASES the segments lying wholly below it. Every sealed segment
    // here does lie below the frontier, so the pin must have moved off the
    // log's base and up to the live tail — the base of the first segment
    // that can ever seal above this snapshot. Nothing else in this file
    // asserts that; "no gap fired" is a different release (no generation
    // open, or one already forfeited).
    assert!(
        arc.pin_floor() >= tail_at_open,
        "the archive holds the log at {} after snapshotting all {sealed} sealed segments, \
         every one of which lies wholly below its own frontier; the snapshot carries them, \
         so the pin must have released to the live tail's base ({tail_at_open}) — spec §3, \
         'snapshots bound restore time and release the WAL below them'",
        arc.pin_floor()
    );
    let m = read_manifest(&store).expect("the opening snapshot's manifest");
    assert_eq!(
        m.generation, 0,
        "the archive rolled off generation 0, which on a healthy store it does only after a gap"
    );
    assert!(
        m.applied_seq > 0,
        "the generation names a frontier of 0 over a Db that applied {:?}",
        db.last_applied()
    );
}

/// RULING 53 / spec §2 ("the chain of sealed segments CONTINUING it") and
/// §3 ("snapshots ... release the WAL below them").
///
/// A segment lying wholly below the generation's frontier holds nothing the
/// snapshot does not already carry. Shipping it uploads those records twice,
/// holds WAL the archive does not need, and hands restore an overlap it must
/// either skip or double-apply.
#[test]
fn a_generations_chain_continues_its_snapshot_instead_of_overlapping_it() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store); // generation 0's snapshot
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store); // and the chain continuing it

    let m = read_manifest(&store).expect("a manifest");
    assert!(
        !m.segments.is_empty(),
        "the archive shipped no chain at all, so 'the chain does not overlap' is vacuous: {m:?}"
    );
    for s in &m.segments {
        assert!(
            s.end_seq > m.applied_seq,
            "segment [{}, {}] lies wholly below the generation's frontier {}: every record in \
             it is already in the snapshot, so the chain OVERLAPS the snapshot instead of \
             continuing it (spec §2)",
            s.base_seq,
            s.end_seq,
            m.applied_seq
        );
    }
    // And no hole in the other direction: the chain must start at or below
    // the first record the snapshot does not carry, or the records between
    // the two are in neither.
    let start = m.segments[0].base_seq;
    assert!(
        start <= m.applied_seq + 1,
        "the chain starts at {start}, above the first record its snapshot does not carry ({}); \
         the records between them are in no part of this generation",
        m.applied_seq + 1
    );
}

/// RULING 53's second site: `resume_point`'s no-chain-yet arm. A restarted
/// archive that adopts a generation whose snapshot landed and whose chain
/// has not started must continue that snapshot too — resuming at the log's
/// base re-introduces the overlap across every restart.
#[test]
fn a_resumed_generation_continues_its_snapshot_rather_than_the_logs_base() {
    let sim = Sim::with_faults(0x6172_6368_5f74_3503, FaultConfig::none());
    let mut db_cfg = DbConfig::default();
    db_cfg.wal.segment_bytes = 64 << 10;
    let mut db = Db::open(Box::new(sim.disk()), db_cfg, Schema::new().table::<Rec>()).unwrap();
    // Every segment stays on disk, so the archive really can reach back to
    // the log's base and the overlap is observable rather than reclaimed.
    db.add_retention_pin(Box::new(|| 0));
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    assert_eq!(db.log_base(), 0, "the second pin must hold the whole log");

    // A frontier INSIDE the last sealed segment, not at a boundary: the
    // chain must start at that segment's base, and the two segments wholly
    // below it must never ship.
    let frontier = FIRST_SEGMENT_RECORDS + RECORDS_PER_SEGMENT + 100;
    let sealed = db.sealed_segments().unwrap();
    assert!(
        sealed.len() >= 3
            && sealed
                .iter()
                .any(|s| s.base_seq <= frontier && frontier < s.layout_end),
        "the frontier {frontier} must fall strictly inside a sealed segment for this test to \
         distinguish a segment boundary from a snapshot boundary: {sealed:?}"
    );

    let store = SimObjectStore::new();
    write_own_manifest(&store, &db, 0, frontier);
    let mut arc = Archive::open(archive_config(0, 8 << 20), &mut db).unwrap();
    step_until_idle(&mut arc, &db, &store);

    let m = read_manifest(&store).expect("a manifest");
    assert_eq!(
        (m.generation, m.applied_seq),
        (0, frontier),
        "the archive did not resume the generation the bucket named, so nothing below is about \
         the resume path: {m:?}"
    );
    assert!(
        !m.segments.is_empty(),
        "the resumed generation shipped no chain at all, so 'it continued its snapshot' is \
         vacuous: {m:?}"
    );
    for s in &m.segments {
        assert!(
            s.end_seq > m.applied_seq,
            "the resumed chain ships segment [{}, {}], wholly below the generation's frontier \
             {}: the resume path started the chain at the log's base instead of at the \
             snapshot the manifest names",
            s.base_seq,
            s.end_seq,
            m.applied_seq
        );
    }
}

/// RULING 53's other end, and `chain_start`'s empty-listing fallback, which
/// has no other producer in this file: with NO sealed segment at all the
/// whole log is the live tail, so the chain starts at the log's base — and
/// that is not the defect. Nothing lies wholly below the frontier there, so
/// there is nothing to drop; what ruling 53 removes is a chain that starts
/// below segments the snapshot already covers, not the value 0 itself.
///
/// It also exhibits the residue §2 records: the chain's head segment holds
/// the records between its base and the frontier, which the snapshot has
/// too. Bounded by one segment, and skippable from `Manifest::applied_seq`.
#[test]
fn a_generation_snapshotted_before_any_segment_seals_starts_at_the_logs_base() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 100);
    assert_eq!(
        sealed_segment_count(&db),
        0,
        "this test is about a log with NOTHING sealed; the fallback it covers is unreachable \
         once a segment exists"
    );
    step_until_idle(&mut arc, &db, &store);

    let m = read_manifest(&store).expect("the opening snapshot's manifest");
    assert!(
        m.segments.is_empty(),
        "nothing is sealed, so nothing can have shipped: {m:?}"
    );
    assert_eq!(
        arc.pin_floor(),
        db.log_base(),
        "with no sealed segment the whole log is the live tail, and its base is where the \
         chain has to start"
    );

    // And the chain it goes on to build really does start there, head
    // segment and all.
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);
    let after = read_manifest(&store).expect("a manifest once segments sealed");
    assert_eq!(
        after.segments.first().map(|s| s.base_seq),
        Some(db.log_base()),
        "the chain skipped past the base of a log that has nothing below its frontier to \
         skip: {:?}",
        after.segments
    );
    assert!(
        after.segments[0].end_seq > after.applied_seq,
        "even the head segment must reach above the frontier, or it is a segment the \
         snapshot wholly covers: {:?}",
        after.segments
    );
}

/// RULING 53's boundary, and the ONLY input on which `chain_start`'s
/// `frontier + 1` and `frontier` differ: a frontier landing exactly on a
/// sealed segment's `layout_end`. `Manifest::applied_seq` is inclusive, so
/// that segment holds nothing the snapshot does not already carry and the
/// chain must start at the next one. Off by one and it ships that segment
/// again and holds its WAL — ruling 53's defect in miniature.
///
/// Every other frontier in this file falls strictly inside a segment or
/// above every segment, where the two derivations agree, so without this
/// test `let target = frontier.saturating_add(1)` can be changed to
/// `let target = frontier` with the whole file still green. `chain_start`'s
/// two construction asserts do not close that gap: both are written in
/// terms of the same `target`, so both stay satisfied by the wrong answer.
#[test]
fn a_frontier_on_a_segment_boundary_starts_the_chain_at_the_segment_above_it() {
    let sim = Sim::with_faults(0x6172_6368_5f74_3504, FaultConfig::none());
    let mut db_cfg = DbConfig::default();
    db_cfg.wal.segment_bytes = 64 << 10;
    let mut db = Db::open(Box::new(sim.disk()), db_cfg, Schema::new().table::<Rec>()).unwrap();
    // Every segment stays on disk, so a chain reaching below the frontier is
    // observable rather than already reclaimed.
    db.add_retention_pin(Box::new(|| 0));
    write_records(&mut db, 5 * RECORDS_PER_SEGMENT);
    assert_eq!(db.log_base(), 0, "the second pin must hold the whole log");

    // Arithmetic from the two measured constants, not read back out of
    // `Db::sealed_segments` (ruling 24): the inclusive last seq of the
    // second sealed segment.
    let frontier = FIRST_SEGMENT_RECORDS + RECORDS_PER_SEGMENT - 1;
    let sealed = db.sealed_segments().unwrap();
    assert!(
        sealed.iter().any(|s| s.layout_end == frontier),
        "the frontier {frontier} must land EXACTLY on a sealed segment's layout_end, or this \
         test does not distinguish `frontier` from `frontier + 1` at all: {sealed:?}"
    );
    assert!(
        sealed.iter().any(|s| s.base_seq > frontier),
        "a segment must seal ABOVE the frontier or the chain is empty and everything below \
         is vacuous: {sealed:?}"
    );

    let store = SimObjectStore::new();
    write_own_manifest(&store, &db, 0, frontier);
    let mut arc = Archive::open(archive_config(0, 8 << 20), &mut db).unwrap();
    step_until_idle(&mut arc, &db, &store);

    let m = read_manifest(&store).expect("a manifest");
    assert_eq!(
        (m.generation, m.applied_seq),
        (0, frontier),
        "the archive did not resume the generation the bucket named, so nothing below is \
         about the boundary: {m:?}"
    );
    assert!(
        !m.segments.is_empty(),
        "the resumed generation shipped no chain at all, so 'it started above the boundary' \
         is vacuous: {m:?}"
    );
    for s in &m.segments {
        assert!(
            s.end_seq > m.applied_seq,
            "the chain ships segment [{}, {}], which ends at or below the generation's \
             frontier {}: every record in it is already in the snapshot, so the chain starts \
             one segment too low — `chain_start` must derive from `frontier + 1`, the first \
             record the snapshot does NOT carry, not from the frontier itself",
            s.base_seq,
            s.end_seq,
            m.applied_seq
        );
    }
    assert_eq!(
        m.segments[0].base_seq,
        frontier + 1,
        "the frontier is a segment's inclusive last seq, so the next seq is the next \
         segment's base and the chain starts there — neither below it (the overlap ruling 53 \
         removes) nor above it (a hole): {:?}",
        m.segments
    );
}

// ------------------------------------------------- Task 8: the write gate

/// The number of `step`s each write-gate test drives the archive for. Far
/// past the ~6 an UNGATED archive needs to land its first snapshot object
/// (Opening start, Opening poll, Idle, Snapshotting, batch start, batch
/// poll), so "nothing was uploaded" is a property of the gate rather than
/// of a loop that stopped too early.
///
/// That ~6 is RE-DERIVED rather than left as a claim (review R-6): a bound
/// justified by a number nobody re-derives is how a stale oracle starts.
/// `the_gate_opening_releases_the_archive` counts it and asserts this bound
/// is at least ten times it — and the count it takes really is the ungated
/// cold start, because the gate returns before any transition, so an archive
/// gated from `open` is still in `Opening { started: false }` when the gate
/// opens.
///
/// Deliberately NOT `step_until_idle`: a gated archive reports `idle`, so a
/// loop keyed on it would exit after ONE step and every assertion below
/// would hold against an archive that had simply not been driven yet. The
/// oracle has to outlast the thing it is checking.
const GATED_STEPS: usize = 500;

fn step_times(arc: &mut Archive, db: &Db, store: &dyn ObjectStore, n: usize) {
    for _ in 0..n {
        let _ = arc.step(db, store);
    }
}

/// SPEC §4 rule 1, DESIGN §5's "What a `Verifying` node may do as a peer",
/// and `docs/superpowers/specs/2026-08-06-verifying-peer-policy-design.md`.
///
/// That sub-project found `begin_seeding` slipping past the write gate
/// BECAUSE IT WAS READ-ONLY, and a node manufacturing durable replicas of a
/// timeline it had not confirmed it owned. The archive arm is read-only
/// against the `Db` in exactly the same way, and an object in a bucket
/// outlives this node's fail-stop far more thoroughly than a seeded
/// follower does. Their line — "a seed creates a fresh copy of an unverified
/// one" — describes an archive upload exactly.
#[test]
fn a_verifying_node_uploads_nothing() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 500);
    // Durable Primary, witnesses unresolved: this node believes it owns the
    // timeline and has not confirmed it.
    db.verify_writes(vec!["witness-a:9000".into()]);

    step_times(&mut arc, &db, &store, GATED_STEPS);

    assert!(
        store.keys().is_empty(),
        "a verifying node manufactured durable copies of an unconfirmed timeline: {:?}",
        store.keys()
    );
    assert!(
        !arc.gapped(),
        "gating is not a gap — it was never this node's timeline to ship, so there is nothing \
         missing from the archive and nothing for an operator to act on"
    );
}

/// The gate must be re-checked on every `step`, never latched: a node whose
/// witnesses answer "not a primary" is the legitimate primary and its
/// archive must resume. A gate that only closed would turn a transient
/// verification into a permanent archive outage.
#[test]
fn the_gate_opening_releases_the_archive() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 500);
    db.verify_writes(vec!["witness-a:9000".into()]);
    step_times(&mut arc, &db, &store, GATED_STEPS);
    assert!(
        store.keys().is_empty(),
        "the gate did not hold, so 'it reopened' below proves nothing: {:?}",
        store.keys()
    );

    db.allow_writes(); // every witness answered "not a primary"
    let mut ungated = 0;
    for _ in 0..GATED_STEPS {
        ungated += 1;
        let _ = arc.step(&db, &store);
        if !store.keys().is_empty() {
            break;
        }
    }
    step_until_idle(&mut arc, &db, &store);

    assert!(
        !store.keys().is_empty(),
        "the archive never resumed after the gate opened — the guard latched instead of being \
         re-read, and a resolved verification became a permanent archive outage"
    );
    assert!(
        ungated * 10 <= GATED_STEPS,
        "an ungated archive took {ungated} steps to land its first object, so GATED_STEPS = \
         {GATED_STEPS} is no longer FAR past it, and 'nothing was uploaded' in the test above \
         could be a loop that stopped too early rather than a property of the gate"
    );
    assert!(
        read_manifest(&store).is_some(),
        "the archive uploaded objects after the gate opened but never swapped the manifest, so \
         nothing it wrote is reachable by a restore"
    );
}

// -------------------------------------------- Task 8: the writer identity

/// The identity of a node that is NOT this archive — a peer promoted after a
/// switchover chain, which DESIGN §5 states the boot gate cannot see ("a
/// probe answers 'is THIS witness a primary', never 'does another primary
/// exist'"). Deliberately in the same `host:port` shape as `WRITER` so a
/// substring assertion on one cannot accidentally match the other.
const FOREIGN_WRITER: &str = "node-b:9000";

/// SPEC §4 rule 2 / §8's last invariant: "the manifest is NEVER advanced by
/// a node that is not its recorded writer". This is the case the plan's
/// snippet aims at — a manifest advanced by another identity WHILE this
/// archive is running.
///
/// Checked from the BUCKET, never from the archive's own bookkeeping: what
/// makes two writers unrestorable is the object the other node reads, so the
/// oracle is what the manifest key actually holds afterwards.
#[test]
fn a_foreign_manifest_advance_stops_this_writer_rather_than_interleaving() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, RECORDS_PER_SEGMENT + 50);
    step_until_idle(&mut arc, &db, &store);
    let before = read_manifest(&store).expect("this archive's own opening manifest");
    assert_eq!(
        before.writer, WRITER,
        "the fixture did not establish this archive as the bucket's writer, so 'a FOREIGN \
         advance' below is not the thing being tested: {before:?}"
    );

    // A promoted peer takes the prefix over, several generations ahead.
    write_foreign_manifest(&store, FOREIGN_WRITER, before.generation + 5);
    write_records(&mut db, 2 * RECORDS_PER_SEGMENT); // segments seal above the frontier
    step_times(&mut arc, &db, &store, 500);

    let after = read_manifest(&store).expect("a manifest");
    assert_eq!(
        (after.writer.as_str(), after.generation),
        (FOREIGN_WRITER, before.generation + 5),
        "this writer interleaved into a bucket another node owns: the manifest is the archive's \
         one mutable key and its commit point, so a second writer advancing it makes every \
         generation in the bucket unreadable to whichever node reads it next (spec §2, §4)"
    );
    assert!(
        arc.stopped(),
        "the archive did not stop after finding the prefix taken over; it will retry the same \
         swap forever, and every retry is another attempt to clobber another node's manifest"
    );
}

/// The same rule at the OTHER end: an archive that finds another writer's
/// manifest the first time it reads the bucket must never start.
///
/// This case is not reachable by the pre-swap check alone — see the report's
/// deviation note. `adopt` used to decline to EXTEND a foreign generation
/// (`resume_point`) and then open a FRESH one over it, which is interleaving
/// by another name: the fresh generation's own manifest swap replaces the
/// other node's.
#[test]
fn an_archive_that_finds_another_writers_manifest_at_open_never_starts() {
    let (mut db, store, mut arc) = archive_fixture();
    // Enough for segments to SEAL, so an archive that failed to refuse has
    // real work to do and the emptiness asserted below is a property of the
    // refusal rather than of a fixture with nothing to ship. Measured
    // against the open-time refusal removed: the archive resumes the foreign
    // generation and writes segment objects into it.
    write_records(&mut db, 2 * RECORDS_PER_SEGMENT);
    write_foreign_manifest(&store, FOREIGN_WRITER, 3);

    step_times(&mut arc, &db, &store, 500);

    assert_eq!(
        store.keys(),
        vec![manifest_key(PREFIX)],
        "this archive wrote objects into a bucket whose manifest names {FOREIGN_WRITER}; \
         ownership is decided before anything is uploaded, not after"
    );
    let m = read_manifest(&store).expect("a manifest");
    assert_eq!(
        (m.writer.as_str(), m.generation),
        (FOREIGN_WRITER, 3),
        "the manifest was advanced by a node that is not its recorded writer (spec §8)"
    );
    assert!(arc.stopped(), "the archive started anyway");
}

/// The SAME node, redeployed on a new port: `ArchiveConfig::writer` changes
/// and nothing else about the bucket, the `Db` or the timeline does.
const RENAMED_WRITER: &str = "node-a:9001";

/// The COST of comparing identity alone, pinned so the spec's claim about it
/// is checked rather than asserted (spec §4 rule 2's trade, review §1(b)).
///
/// This archive owns the prefix. Renamed, it refuses PERMANENTLY on a bucket
/// that is genuinely its own — at `adopt`, before anything is uploaded —
/// where a comparison that also asked "is the other writer further along"
/// would have kept it archiving under a fresh generation. Nothing in the
/// bucket distinguishes a rename from a takeover: the manifest carries one
/// string and both cases present identically, so this is the deliberate side
/// of an asymmetric trade rather than an oversight. Wrongly continuing
/// leaves a bucket neither node can restore from; wrongly stopping costs
/// availability, with a notice, a live bucket and an untouched `Db`.
///
/// It follows — and `ArchiveConfig::writer`'s doc says so — that `writer`
/// must not be derived from anything a redeploy moves.
#[test]
fn a_renamed_writer_refuses_permanently_on_the_bucket_it_owns() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 500);
    step_until_idle(&mut arc, &db, &store);
    let before = read_manifest(&store).expect("this archive's own manifest");
    assert_eq!(
        (before.writer.as_str(), before.generation),
        (WRITER, 0),
        "the fixture did not leave this archive owning the prefix, so 'its own bucket' below is \
         not what is being tested: {before:?}"
    );
    // Segments seal above that frontier and are left UNSHIPPED, so an
    // archive that failed to refuse has real work to do: without this the
    // bucket assertion below would hold for the wrong reason (the M3b shape
    // the report and the review both record).
    write_records(&mut db, 2 * RECORDS_PER_SEGMENT);
    let owned = store.keys();

    // The same node, back on a new port. Same Db, same bucket, same records.
    let mut renamed = Archive::open(
        ArchiveConfig {
            writer: RENAMED_WRITER.to_string(),
            ..archive_config(0, 8 << 20)
        },
        &mut db,
    )
    .unwrap();
    step_times(&mut renamed, &db, &store, 500);

    assert!(
        renamed.stopped(),
        "a renamed writer kept archiving into a bucket whose manifest names another identity — \
         the comparison is identity alone precisely so it cannot tell this case from a takeover"
    );
    assert!(
        renamed.idle(),
        "a stopped archive reports idle so a kernel arm stops scheduling it; `stopped()` is what \
         says it is not healthy (Archive::idle's doc)"
    );
    assert!(
        !renamed.gapped(),
        "an ownership refusal is not a gap: a gap is recoverable on the next contact and this is \
         not recoverable at all"
    );
    assert_eq!(
        store.keys(),
        owned,
        "the refusal happened at `adopt`, before anything is uploaded — nothing may have landed"
    );
    assert_eq!(
        read_manifest(&store).map(|m| (m.writer, m.generation)),
        Some((WRITER.to_string(), 0)),
        "the renamed archive advanced a manifest it no longer matches"
    );
}

/// The corner that decides the SHAPE of the pre-swap comparison, and the
/// oracle for this task's one deviation from its brief (see the report).
///
/// A foreign manifest sitting at a generation BELOW the one this archive is
/// about to write is still another node's manifest, and advancing it is
/// still the interleave spec §8 forbids. A comparison that additionally
/// required the foreign generation to be at or above this archive's would
/// let exactly this through — and it is not a contrived state: it is what a
/// node that opened over a bucket, rolled a generation, and only then lost
/// the prefix looks like.
#[test]
fn a_foreign_manifest_below_this_archives_generation_stops_it_too() {
    let cadence = 2 * RECORDS_PER_SEGMENT;
    let (mut db, store, mut arc) = archive_fixture_with_snapshot_cadence(cadence);
    write_records(&mut db, cadence);
    step_until_idle(&mut arc, &db, &store);
    let before = read_manifest(&store).expect("a manifest once the cadence was crossed");
    assert_eq!(
        (before.writer.as_str(), before.generation),
        (WRITER, 0),
        "the fixture did not leave this archive owning generation 0: {before:?}"
    );

    // Another writer clobbers the manifest, naming a generation BELOW the
    // one the cadence crossing below is about to make this archive write.
    write_foreign_manifest(&store, FOREIGN_WRITER, 0);
    write_records(&mut db, cadence); // crossing -> this archive rolls to generation 1
    step_times(&mut arc, &db, &store, 1_000);

    let after = read_manifest(&store).expect("a manifest");
    assert_eq!(
        (after.writer.as_str(), after.generation),
        (FOREIGN_WRITER, 0),
        "this archive advanced a manifest it does not own because the foreign generation was \
         BELOW its own — a generation comparison is not an ownership check, and the bucket is \
         now two writers deep"
    );
    assert!(arc.stopped(), "the archive kept going");
}

/// HARD RULE 5 (spec §7's row: "operator notice naming both identities, the
/// generation, and the prefix"). Kept apart from the property tests above so
/// neither is asserting the words it is about to read back.
#[test]
fn the_foreign_writer_refusal_names_both_identities_the_generation_and_the_prefix() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 500);
    write_foreign_manifest(&store, FOREIGN_WRITER, 3);

    let notice = step_until_error(&mut arc, &db, &store);
    let t = notice.to_string();
    for needle in [WRITER, FOREIGN_WRITER, PREFIX, "generation", "prefix"] {
        assert!(t.contains(needle), "the refusal must name {needle}: {t}");
    }
}

/// HARD RULE 5 at the site the test above does not reach: what the refusal
/// says about the BUCKET, at `adopt`, where this archive has uploaded
/// nothing.
///
/// `self.generation` is 0 on a fresh open, so a notice that unconditionally
/// calls "objects under generation {self.generation}" orphaned names the
/// generation the manifest points at — the LIVE one, six intact objects in
/// the review's measurement — and tells the operator their good data is
/// garbage. The foreign manifest here is planted at generation 0 on purpose,
/// so the two numbers collide exactly as they do on a fresh open.
///
/// The claim is checked against the bucket rather than against other words
/// in the same string.
#[test]
fn the_foreign_writer_refusal_at_open_never_calls_the_live_generation_orphaned() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 500);
    write_foreign_manifest(&store, FOREIGN_WRITER, 0);

    let notice = step_until_error(&mut arc, &db, &store);
    let t = notice.to_string();
    assert_eq!(
        store.keys(),
        vec![manifest_key(PREFIX)],
        "this archive uploaded something before it refused, so 'it left nothing behind' is not \
         the fact the notice should be stating here"
    );
    assert!(
        !t.contains("orphan"),
        "the refusal claims orphaned objects at a site where this archive uploaded nothing, and \
         on a fresh open its own generation counter is 0 — so the generation it calls orphaned \
         is the LIVE one the manifest points at, and the operator is told their good data is \
         garbage: {t}"
    );
    assert!(
        t.contains("is the live one and is untouched"),
        "the refusal must say which generation is live at this site — the operator's next act \
         is deciding what to delete: {t}"
    );
}

/// SPEC §5, and the reason a terminal state cannot simply sit there: "a
/// derived artifact must not be able to halt the thing it derives from". An
/// archive that will never ship again must not go on holding the WAL against
/// `Wal::gc` — that is the archive acquiring the power to stop the primary,
/// which is the exact hazard the bounded pin exists for.
///
/// Also pins that the refusal is raised ONCE rather than once per step
/// (the `gap_notice` precedent), which is what keeps a stopped archive from
/// filling an operator's log with the same paragraph forever.
#[test]
fn a_stopped_archive_releases_the_log_and_stops_erroring() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 500);
    write_foreign_manifest(&store, FOREIGN_WRITER, 3);
    assert_ne!(
        db.retention_floor(),
        u64::MAX,
        "the archive was not holding the log before it stopped, so the release below proves \
         nothing"
    );

    step_times(&mut arc, &db, &store, 500);

    assert_eq!(
        db.retention_floor(),
        u64::MAX,
        "a stopped archive is still holding the WAL against gc; it will never ship again, so \
         every segment it holds is held forever — a derived artifact must not be able to halt \
         the thing it derives from (spec §5)"
    );
    let settled = store.keys();
    for _ in 0..200 {
        arc.step(&db, &store).expect(
            "a stopped archive returned its refusal again; the notice is raised once, when it \
             happens, not once per step",
        );
    }
    assert_eq!(
        store.keys(),
        settled,
        "a stopped archive kept writing to a bucket it does not own"
    );
    assert!(
        !db.poisoned(),
        "an archive failure is not a database failure (spec §8)"
    );
}

// ------------------- Task 8: the ownership GET is an operation in flight

/// SPEC §1: never a second `ObjectStore::start` while the first operation is
/// unpolled. This is the oracle for `op_in_flight`'s `State::CheckingOwner`
/// arm, which nothing else in this file exercises.
///
/// The ownership check GETs the manifest in front of every swap, so from
/// Task 8 on the archive has a started store operation that is NOT an
/// upload. `yield_or_bound` runs in front of every transition and its gap
/// arm DISCARDS whatever the archive was doing, so a bound crossed while
/// that GET is in flight abandons it — and the next step's `store.start` is
/// then a second one against an unpolled op, which trips
/// `SimObjectStore::start`'s double-start assert.
///
/// The window is entered deliberately, never waited for: the step that lands
/// the first `/segments/` object is the step that enters `CheckingOwner`,
/// the step after it issues the GET, and only then is the bound crossed. If
/// the archive were NOT sitting on a started operation at that point,
/// `yield_or_bound` would gap on the crossing step and the first assertion
/// below would fire — so this test cannot pass against a fixture that never
/// reached the window.
#[test]
fn a_gap_crossing_while_the_ownership_get_is_in_flight_never_abandons_it() {
    let (mut db, store, mut arc) =
        build_fixture_with_cadence(SimObjectStore::new(), 0, 8 << 20, GAP_PIN_SEGMENTS);
    // The opening snapshot FIRST, at a frontier below the log's tail. With
    // `snapshot_every: 0` a fixture that writes everything up front
    // snapshots at the tail and never ships a segment at all, so the window
    // below would never be entered at all.
    write_records(&mut db, 500);
    step_until_idle(&mut arc, &db, &store);
    assert!(
        read_manifest(&store).is_some(),
        "the opening snapshot never landed, so there is no generation for a segment to ship \
         into: {:?}",
        store.keys()
    );

    // Segments seal ABOVE that frontier, still inside the bound.
    write_records(&mut db, 2 * RECORDS_PER_SEGMENT);
    let mut shipped = false;
    for _ in 0..10_000 {
        let _ = arc.step(&db, &store);
        if store.keys().iter().any(|k| k.contains("/segments/")) {
            shipped = true;
            break;
        }
    }
    assert!(
        shipped,
        "no segment object ever landed, so the archive never reached the pre-swap ownership \
         check and the window below is never opened: {:?}",
        store.keys()
    );
    // That segment's PUT completed on the step above, which is what enters
    // `CheckingOwner`; this step issues the manifest GET and leaves it
    // unpolled.
    arc.step(&db, &store)
        .expect("the step that issues the ownership GET");

    // The bound is crossed with that GET still in flight.
    write_records(&mut db, GAP_PIN_SEGMENTS as u64 * RECORDS_PER_SEGMENT);
    assert!(
        sealed_segment_count(&db) > GAP_PIN_SEGMENTS,
        "the bound was never crossed, so nothing below is about a gap: {} sealed segments \
         against pin_segments = {GAP_PIN_SEGMENTS}",
        sealed_segment_count(&db)
    );
    let crossing = arc.step(&db, &store);
    let gapped_at_crossing = arc.gapped();
    // Stepped BEFORE the assertions, deliberately: this is the step whose
    // `store.start` panics if the one above abandoned the GET, and that
    // panic is the failure worth seeing.
    let after = arc.step(&db, &store);

    assert!(
        crossing.is_ok() && !gapped_at_crossing,
        "the archive gapped while its own ownership GET was still in flight, discarding a \
         started operation — the next `ObjectStore::start` is then a second one against an \
         unpolled op (spec §1), which is a contract violation and not a retry. `op_in_flight` \
         must count `State::CheckingOwner {{ started: true }}` exactly as it counts the upload \
         states: {crossing:?}"
    );
    assert!(
        arc.gapped() && after.is_err(),
        "the bound was crossed with {} segments sealed and the archive never gapped: holding a \
         started operation must DELAY the gap by one step, never cancel it",
        sealed_segment_count(&db)
    );
}

// ------------------------------- Task 8 / F5: a manifest it cannot read

/// The bytes of a real manifest with one body byte flipped: unambiguously a
/// CRC failure rather than a truncation, since `Manifest::decode` verifies
/// the trailing CRC before it reads a single field. Deterministic, so the
/// object planted below is the same one on every run.
///
/// Written as THIS archive's own writer identity on purpose, so the refusal
/// under test is "the manifest is unreadable" and not "the manifest belongs
/// to someone else" — the two land in the same terminal state and would
/// otherwise be indistinguishable.
fn corrupt_manifest_bytes() -> Vec<u8> {
    let mut bytes = Manifest {
        generation: 0,
        writer: WRITER.to_string(),
        applied_seq: 0,
        next_job_id: 0,
        tables: Vec::new(),
        segments: Vec::new(),
        closed: false,
    }
    .encode();
    bytes[0] ^= 0xff;
    bytes
}

fn plant_corrupt_manifest(store: &SimObjectStore) -> Vec<u8> {
    let bytes = corrupt_manifest_bytes();
    plant(store, &[(manifest_key(PREFIX), bytes.clone())]);
    bytes
}

/// F5 (task-6/7 carry-forward, routed to Task 8). A manifest failing its CRC
/// at `Opening` was returned as an `Err` and retried FOREVER: the archive
/// shipped nothing, held the WAL at its pin while it did, and said the same
/// thing every step without ever reaching a verdict.
///
/// The manifest is this archive's only evidence of who owns the prefix and
/// which generation is live, so an archive that cannot read it cannot know
/// whether the objects here are its own — opening a fresh generation over
/// them is spec §4's interleave on no evidence at all. It refuses.
#[test]
fn a_manifest_this_archive_cannot_read_stops_it_instead_of_retrying_forever() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 2 * RECORDS_PER_SEGMENT);
    let corrupt = plant_corrupt_manifest(&store);
    assert_ne!(
        db.retention_floor(),
        u64::MAX,
        "the archive was not holding the log before it refused, so the release below proves \
         nothing"
    );

    step_times(&mut arc, &db, &store, 500);

    assert_eq!(
        get_object(&store, &manifest_key(PREFIX)).expect("a manifest object"),
        corrupt,
        "the archive overwrote a manifest it could not read; with the ownership evidence \
         unreadable it cannot know whose bucket this is (spec §2, §4)"
    );
    assert_eq!(
        store.keys(),
        vec![manifest_key(PREFIX)],
        "the archive uploaded into a bucket whose ownership it could not establish"
    );
    assert_eq!(
        db.retention_floor(),
        u64::MAX,
        "an archive that will never ship is still holding the WAL against gc — the retry loop \
         held the log for as long as it lasted, which is forever (spec §5)"
    );
    for _ in 0..200 {
        arc.step(&db, &store).expect(
            "the refusal is a retry loop, not a stop — the archive raised it again, which is \
             the F5 behaviour this test exists to remove",
        );
    }
    assert!(
        !db.poisoned(),
        "an archive failure is not a database failure (spec §8)"
    );
    assert!(arc.stopped(), "the archive did not reach a verdict");
}

/// The same refusal at the OTHER site that decodes a manifest: the ownership
/// check in front of a swap. An archive part-way through a generation that
/// finds the manifest unreadable is in the same position as one at `Opening`
/// — it cannot tell whether the prefix is still its own — and must not
/// resolve that by overwriting the evidence.
#[test]
fn a_manifest_that_becomes_unreadable_mid_run_stops_this_writer_too() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, RECORDS_PER_SEGMENT + 50);
    step_until_idle(&mut arc, &db, &store);
    assert_eq!(
        read_manifest(&store).expect("a manifest").writer,
        WRITER,
        "the fixture did not establish a readable generation first, so 'it BECAME unreadable' \
         is not what is being tested"
    );

    let corrupt = plant_corrupt_manifest(&store);
    write_records(&mut db, 2 * RECORDS_PER_SEGMENT); // work to do, so the check is reached
    step_times(&mut arc, &db, &store, 1_000);

    assert_eq!(
        get_object(&store, &manifest_key(PREFIX)).expect("a manifest object"),
        corrupt,
        "the archive swapped a manifest it could not read"
    );
    assert_eq!(
        db.retention_floor(),
        u64::MAX,
        "a stopped archive is still holding the WAL against gc (spec §5)"
    );
    assert!(arc.stopped(), "the archive did not reach a verdict");
}

/// HARD RULE 5. The refusal has to name the contract (what the manifest is
/// for), the site (the key and the prefix), and the fix — and it must carry
/// the decode failure it wraps, or an operator cannot tell a CRC mismatch
/// from a truncated object.
#[test]
fn the_unreadable_manifest_refusal_names_the_key_the_prefix_and_the_fix() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 500);
    plant_corrupt_manifest(&store);

    let notice = step_until_error(&mut arc, &db, &store);
    let t = notice.to_string();
    for needle in [PREFIX, &manifest_key(PREFIX), "CRC", "remove", "restart"] {
        assert!(t.contains(needle), "the refusal must name {needle}: {t}");
    }
    // The OTHER direction of the same conditional, which nothing pinned
    // until now: at `Opening` "nothing was uploaded" is the true sentence,
    // and a predicate stuck at "uploaded" would tell an operator with an
    // empty bucket that removing the manifest strands objects they do not
    // have — sending them looking for orphans instead of re-fetching.
    // Checked against the bucket, not against other words in the notice.
    assert_eq!(
        store.keys(),
        vec![manifest_key(PREFIX)],
        "this archive uploaded something before it refused, so 'nothing was uploaded' is not the \
         fact the notice should be stating here"
    );
    assert!(
        t.contains("nothing was uploaded"),
        "the refusal must say what it left behind at this site too — hard rule 5's third element \
         is the fix, and here the fix is re-fetch or restore, with nothing to clean up: {t}"
    );
}

/// HARD RULE 5 at the OTHER site, which the test above does not reach: it
/// plants its corrupt manifest before the first step, so it only ever reads
/// the `Opening` site — where "nothing was uploaded" happens to be true.
///
/// Mid-run it is false. The bucket holds this generation's snapshot and a
/// segment, and an operator told nothing was left behind then follows
/// "remove the manifest" and strands every one of them, with no reclamation
/// anywhere in this design. `foreign_writer_notice` warns about exactly that
/// hazard in its own text; this one used to deny it.
///
/// Asserted against the BUCKET, not against other words in the same string.
#[test]
fn the_unreadable_manifest_refusal_mid_run_does_not_claim_nothing_was_uploaded() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, RECORDS_PER_SEGMENT + 50);
    step_until_idle(&mut arc, &db, &store);
    plant_corrupt_manifest(&store);
    write_records(&mut db, 2 * RECORDS_PER_SEGMENT); // work to do, so the check is reached

    let notice = step_until_error(&mut arc, &db, &store);
    let t = notice.to_string();
    let left = store.keys();
    assert!(
        left.len() > 1,
        "the archive had written nothing but the manifest key when it refused, so the claim \
         under test is not being measured against anything: {left:?}"
    );
    assert!(
        !t.contains("nothing was uploaded"),
        "the refusal tells the operator nothing was uploaded while {} objects sit under this \
         prefix, and then offers to remove the manifest — which strands every one of them, \
         since nothing in this design collects orphaned objects: {left:?}\n{t}",
        left.len()
    );
    assert!(
        t.contains("stay in the bucket"),
        "the refusal must name what it left behind at this site: hard rule 5's third element \
         is the fix, and the fix is what differs between the two sites: {t}"
    );
}

// ---------------------------------------- the FIRST swap's `CheckingOwner`

/// Steps one at a time and stops at the FIRST `CheckingOwner` of generation
/// 0 — the one guarding the OPENING SNAPSHOT's manifest swap. Returns the
/// bucket's keys as they stand there.
///
/// Recognised from the BUCKET, never from the archive's internal state: the
/// snapshot's `meta` object is uploaded LAST of the snapshot (spec §2's
/// ordering, `step_uploading_snapshot_meta`) and the manifest key does not
/// exist yet, a combination true at exactly this moment and nowhere else in
/// the generation. Reading it off `Archive` would need a state accessor this
/// crate does not expose, and would make the oracle depend on the same
/// internals the notice does.
///
/// Every earlier notice oracle in this file reaches its site through
/// `adopt` (planting before the first step) or through a LATER swap
/// (stepping to idle first, so `frontier` is set). Neither can see this one,
/// which is how NEW-1 survived a review and a fix round.
///
/// Safe to plant into the store the instant this returns: `SimObjectStore`
/// takes `in_flight` on `poll`, and the step that made `meta` visible is the
/// step that polled its PUT to completion, so nothing is in flight.
fn step_to_first_swap(arc: &mut Archive, db: &Db, store: &SimObjectStore) -> Vec<String> {
    for _ in 0..10_000 {
        let keys = store.keys();
        if keys.iter().any(|k| *k == snapshot_meta_key(PREFIX, 0))
            && !keys.iter().any(|k| *k == manifest_key(PREFIX))
        {
            return keys;
        }
        arc.step(db, store)
            .expect("no faults are configured on this fixture, so no step may fail");
    }
    panic!(
        "step_to_first_swap: 10,000 steps without reaching the opening snapshot's manifest swap"
    );
}

/// Asserts the bucket really does hold this archive's own generation-0
/// snapshot and no manifest — the anti-vacuity guard both first-swap oracles
/// below stand on. Taken from `store.keys()`, so what the notices are
/// checked against is the bucket's actual contents rather than other words
/// in the notice string.
fn assert_first_swap_bucket(landed: &[String]) {
    assert!(
        landed.contains(&snapshot_meta_key(PREFIX, 0)),
        "not at the opening snapshot's swap: its meta object is not in the bucket: {landed:?}"
    );
    assert!(
        !landed.contains(&manifest_key(PREFIX)),
        "past the opening swap already — a manifest exists, so `frontier` may be set and the \
         site under test is not the one reached: {landed:?}"
    );
    assert!(
        landed.len() > 1,
        "the opening snapshot is meta-only, so 'this archive has objects in the bucket' is not \
         being measured against anything here: {landed:?}"
    );
}

/// HARD RULE 5 at the site NEITHER earlier foreign-writer oracle reaches
/// (NEW-1, `task-8-rereview-r1.md`). `self.frontier` is set only when the
/// manifest swap LANDS, so at every generation's first swap it is `None`
/// and the chain is empty while that generation's whole snapshot is already
/// in the bucket.
///
/// The notice must not tell an operator it "left nothing of its own under
/// this prefix" and then offer to remove the manifest: those objects are
/// orphaned the moment this archive stops, and nothing in this design
/// collects them.
///
/// The foreign manifest is planted at generation 7, away from this
/// archive's own generation 0, so "generation 0 is orphaned" is
/// unambiguous — at generation 0 the two writers would be naming the same
/// keys, which is the interleave the refusal exists to detect, not a claim
/// the notice can untangle.
#[test]
fn the_foreign_writer_refusal_at_the_first_swap_names_the_snapshot_it_stranded() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 500);
    let landed = step_to_first_swap(&mut arc, &db, &store);
    assert_first_swap_bucket(&landed);
    write_foreign_manifest(&store, FOREIGN_WRITER, 7);

    let notice = step_until_error(&mut arc, &db, &store);
    let t = notice.to_string();
    assert!(
        !t.contains("left nothing of its own under this prefix"),
        "the refusal tells the operator this archive left nothing behind while {} of its own \
         generation-0 objects sit under this prefix: {landed:?}\n{t}",
        landed.len()
    );
    assert!(
        t.contains("orphaned in the bucket"),
        "the refusal must name what this archive stranded here — the operator's next act is \
         deciding what to delete, and these objects no manifest names: {landed:?}\n{t}"
    );
}

/// The same site for the OTHER notice (NEW-1's first half: R-1 was never
/// fixed here). `the_unreadable_manifest_refusal_mid_run_does_not_claim_
/// nothing_was_uploaded` steps to idle first, so it only ever reaches a
/// LATER swap, where `frontier` is already set.
///
/// "Nothing was uploaded" plus "remove it so the next start opens a fresh
/// generation" is the sentence pair that strands a whole snapshot.
#[test]
fn the_unreadable_manifest_refusal_at_the_first_swap_does_not_claim_nothing_was_uploaded() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 500);
    let landed = step_to_first_swap(&mut arc, &db, &store);
    assert_first_swap_bucket(&landed);
    plant_corrupt_manifest(&store);

    let notice = step_until_error(&mut arc, &db, &store);
    let t = notice.to_string();
    assert!(
        !t.contains("nothing was uploaded"),
        "the refusal tells the operator nothing was uploaded while {} objects of its own \
         generation-0 snapshot sit under this prefix, and then offers to remove the manifest — \
         which strands every one of them: {landed:?}\n{t}",
        landed.len()
    );
    assert!(
        t.contains("stay in the bucket"),
        "the refusal must name what it left behind at this site: hard rule 5's third element \
         is the fix, and the fix is what differs between the two sites: {landed:?}\n{t}"
    );
}

// ------------------------------------- `idle()`'s enumeration is the spec

/// The `///` block immediately above `pub fn idle`, RAW — `archive_source_
/// code` strips comments, which is right for the no-clock proxies and
/// useless here, where the doc IS the thing under test.
fn idle_doc_block() -> String {
    let mut block: Vec<&str> = Vec::new();
    for line in include_str!("../src/archive/mod.rs").lines() {
        let t = line.trim_start();
        if t.starts_with("pub fn idle(&self) -> bool") {
            return block.join("\n");
        }
        if t.starts_with("///") {
            block.push(t);
        } else {
            block.clear();
        }
    }
    panic!("`pub fn idle(&self) -> bool` not found in crates/charpente/src/archive/mod.rs");
}

/// NEW-2 (`task-8-rereview-r1.md`), and hard rule 6: `Archive::idle`'s doc
/// enumerates the states that answer true, and Task 9's kernel arm builds
/// its ready predicate on that enumeration — so a state the list omits is a
/// defect in the spec, not a wording nit. An enumeration that says "four"
/// and has five is the same defect R-3 was.
///
/// Three halves, and the first is what stops the other two being vacuous:
///
/// 1. The waiting-for-a-generation state is REAL — `idle` true with sealed,
///    un-archived segments and an empty bucket. It is neither "caught up"
///    (two segments are sealed past what has been archived) nor "waiting"
///    (both are fully applied) nor gated nor stopped.
/// 2. The doc names it.
/// 3. The `self.idle = true` site count is the tripwire for the NEXT one: a
///    sixth state means a seventh assignment, and this test reddens until
///    the list is updated with it. Six sites, five states — `Stopped` is
///    reached from `step`'s terminal guard and from `stop()`.
#[test]
fn the_idle_doc_names_every_state_that_answers_true() {
    // Far past anything written below, so the cadence never arrives.
    let (mut db, store, mut arc) = archive_fixture_with_snapshot_cadence(100 * RECORDS_PER_SEGMENT);
    write_records(&mut db, 2 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);

    assert!(arc.idle(), "the fifth state was not reached");
    assert!(!arc.stopped() && !arc.gapped(), "not the fifth state");
    assert!(
        sealed_segment_count(&db) >= 2,
        "nothing is sealed, so 'idle with un-archived segments behind it' is not being measured: \
         {} sealed",
        sealed_segment_count(&db)
    );
    assert!(
        store.keys().is_empty(),
        "the archive shipped something, so this is not the never-opened-a-generation state: {:?}",
        store.keys()
    );

    let doc = idle_doc_block();
    assert!(
        doc.contains("snapshot_every"),
        "`idle` answers true with {} segments sealed and nothing in the bucket, and its doc does \
         not name that state. Task 9 reads this list to decide what its ready predicate must \
         handle, and would conclude idle implies caught-up-or-gated-or-dead: {doc}",
        sealed_segment_count(&db)
    );

    let bullets = doc.lines().filter(|l| l.starts_with("/// - **")).count();
    assert_eq!(
        bullets, 5,
        "the doc claims to enumerate the states answering true and lists {bullets}. If a state \
         was added, name it here and in the doc; if one was removed, likewise — the count is a \
         tripwire on purpose (NEW-2: the list said FOUR and there were five)"
    );
    let sites = include_str!("../src/archive/mod.rs")
        .matches("self.idle = true;")
        .count();
    assert_eq!(
        sites, 6,
        "`self.idle = true` is assigned at {sites} sites, not the 6 the doc's five states account \
         for (`Stopped` is reached twice). A new site is a new state until proven otherwise, and \
         `Archive::idle`'s doc must name it before this test goes green again"
    );
}

// ------------------------------------------- T9 / F4: the dropped-pin leak

/// F4 (Ruling 58). `Db::add_retention_pin` has no unregister and the
/// registered closure keeps its own `Arc` to the `Pin`, so a dropped
/// `Archive` goes on reporting the floor it last held — forever, because
/// nothing can ever advance it again. That is spec §5's hazard (a derived
/// artifact acquiring the power to halt the thing it derives from) arriving
/// with no outage at all: `Wal::gc` can never pass the floor, so the WAL
/// grows until the disk fills.
///
/// Task 9 is the commit that makes it reachable rather than theoretical.
/// Once the `Kernel` owns the `Archive`, dropping the `Kernel` drops the
/// `Archive` — including every test that builds a kernel and lets it fall
/// out of scope, and every `serve::archive` whose `run` returns.
///
/// The first assert is not decoration: without it a `Drop` that did nothing
/// would still pass whenever the archive happened not to be holding, and
/// the test would prove nothing.
#[test]
fn dropping_an_archive_releases_its_retention_pin() {
    let (mut db, store, mut arc) = archive_fixture();
    snapshot_then_chain(&mut arc, &mut db, &store, 3 * RECORDS_PER_SEGMENT);

    let held = arc.pin_floor();
    assert_eq!(
        db.retention_floor(),
        held,
        "the archive is not holding the log, so dropping it could not leak a hold and this test \
         proves nothing"
    );

    drop(arc);

    assert_eq!(
        db.retention_floor(),
        u64::MAX,
        "a dropped Archive left its retention pin reporting {held}: Wal::gc can never pass that \
         seq again and the WAL grows forever, with no archive left to release it"
    );
}

// ------------------------------- T9: the archive as the SIXTH kernel arm
//
// `Ready` already had five variants before this task (Request, Job, Cron,
// Repl, Tail) — `ObjectStore` is the fifth IO TRAIT, which is where the
// plan's "fifth kernel arm" title came from. Everything below drives
// `Kernel::step` directly rather than `Kernel::run`, because the property
// under test is which SOURCE the loop decides is ready, and `run` absorbs
// exactly that into a park.

use charpente::http::{ApiError, App, AppConfig, Method, Request, Response};
use charpente::kernel::{
    EventFrame, EventSink, Kernel, KernelStats, Reply, RequestSource, Respond, SinkStatus, Step,
    Wake,
};
use charpente::{Tx, route, routes};
use charpente_core::io::{Disk, Instant};
use charpente_sim::SimDisk;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Serialize, Deserialize)]
struct RecBody {
    n: u64,
}

/// A writing route, deliberately: an archive with a `Db` that never moves
/// goes caught-up and stays there, and every "does the arm keep up under
/// load" question would then be asked of an archive with nothing to do.
#[route(POST "/rec")]
fn put_rec(tx: &mut Tx, body: RecBody) -> Result<u64, ApiError> {
    tx.put(&Rec {
        id: Id::from_raw(body.n),
        value: body.n,
    });
    Ok(body.n)
}

/// `SimObjectStore` behind a handle the test keeps after the `Kernel` has
/// taken ownership of the `Box<dyn ObjectStore>`.
///
/// `Arc<Mutex<_>>` rather than `Arc<_>`: `SimObjectStore` holds a
/// `RefCell<Rng>` and so is `Send` but not `Sync`, which makes `Arc<SimObject
/// Store>` not `Send` and therefore not an `ObjectStore` at all. `Mutex<T>`
/// is `Sync` whenever `T: Send`, so wrapping restores it. Never contended —
/// the kernel loop is single-threaded and so is every test here.
#[derive(Clone)]
struct SharedStore {
    inner: Arc<Mutex<SimObjectStore>>,
    /// A REVERSIBLE PUT outage. `SimObjectStore`'s `object_put_error` axis is
    /// fixed at construction with no public way to turn it back off, and
    /// `crates/charpente-sim/src/object.rs` is off-limits (Ruling 37) — so a
    /// store that fails and then HEALS has to live here, exactly as
    /// `FlakyStore` above exists for the same reason. Recovery is the
    /// property the retry mechanism is really about, and it cannot be
    /// observed with a one-way fault.
    ///
    /// Fails at `start`, before the op reaches `inner`, so nothing goes in
    /// flight and a later retry is a clean first attempt rather than the
    /// double-start `SimObjectStore::start` asserts against.
    puts_fail: Arc<AtomicBool>,
}

impl SharedStore {
    fn new() -> SharedStore {
        SharedStore::wrapping(SimObjectStore::new())
    }

    fn wrapping(inner: SimObjectStore) -> SharedStore {
        SharedStore {
            inner: Arc::new(Mutex::new(inner)),
            puts_fail: Arc::new(AtomicBool::new(false)),
        }
    }

    fn set_puts_fail(&self, fail: bool) {
        self.puts_fail.store(fail, Ordering::Relaxed);
    }

    fn keys(&self) -> Vec<String> {
        self.lock().keys()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SimObjectStore> {
        self.inner
            .lock()
            .expect("SharedStore lock poisoned by a panic")
    }
}

impl ObjectStore for SharedStore {
    fn start(&self, op: ObjectOp) -> io::Result<()> {
        if self.puts_fail.load(Ordering::Relaxed)
            && let ObjectOp::Put { key, .. } = &op
        {
            return Err(io::Error::other(format!(
                "simulated SharedStore outage: PUT \"{key}\" refused (test-local switch — \
                 flips back via set_puts_fail(false))"
            )));
        }
        self.lock().start(op)
    }

    fn poll(&self) -> Option<io::Result<ObjectDone>> {
        self.lock().poll()
    }
}

/// A `RequestSource` that never runs dry: `poll` always hands back one more
/// `POST /rec`, so `Ready::Request` is in the ready set on EVERY iteration
/// and the only way `stats.requests` can stay 0 is an arm that starves it.
struct EndlessSource {
    next: u64,
}

struct CountingReply;

impl Respond for CountingReply {
    fn send(self: Box<Self>, _response: Response) {}

    fn stream(self: Box<Self>, _response: Response) -> Box<dyn EventSink> {
        Box::new(SilentSink)
    }
}

struct SilentSink;

impl EventSink for SilentSink {
    fn send(&mut self, _frame: &EventFrame) -> SinkStatus {
        SinkStatus::Ok
    }
}

impl RequestSource for EndlessSource {
    fn poll(&mut self) -> Option<(Request, Reply)> {
        self.next += 1;
        let request = Request::new(Method::Post, "/rec").json(&RecBody { n: self.next });
        Some((request, Box::new(CountingReply)))
    }

    fn wait_until(&mut self, _deadline: Option<Instant>) -> Wake {
        Wake::Ready
    }
}

/// SATURATING READ-ONLY LOAD — the load C-2 was found under, and the one the
/// original four-case re-arm analysis missed. Every poll hands back a request
/// that 404s, so `Ready::Request` is in the ready set on every iteration (the
/// kernel therefore NEVER parks) while nothing is ever staged (so
/// `db.last_applied()` never moves). An ordinary state for a read-heavy
/// primary, and the exact intersection where a re-arm keyed on "a commit or a
/// park" fires neither trigger.
struct ReadOnlySource;

impl RequestSource for ReadOnlySource {
    fn poll(&mut self) -> Option<(Request, Reply)> {
        Some((Request::new(Method::Get, "/nope"), Box::new(CountingReply)))
    }

    fn wait_until(&mut self, _deadline: Option<Instant>) -> Wake {
        Wake::Ready
    }
}

/// The shape `serve::archive` runs against (`serve.rs`'s `EmptySource`): no
/// request ever arrives. `wait_until` is never called by these tests — they
/// drive `Kernel::step` so they can SEE `Step::Idle` instead of having a
/// driver absorb it into a park.
struct SilentSource;

impl RequestSource for SilentSource {
    fn poll(&mut self) -> Option<(Request, Reply)> {
        None
    }

    fn wait_until(&mut self, _deadline: Option<Instant>) -> Wake {
        Wake::Deadline
    }
}

/// Counts `Disk::list` calls and delegates everything else, so F2 can be
/// MEASURED rather than argued. `Archive::step`'s `yield_or_bound` does one
/// `Db::sealed_segments` — a `Disk::list` — per step where the archive holds
/// and nothing is in flight, and the question Task 9 had to answer is
/// whether wiring the archive into the kernel turns that into one per KERNEL
/// ITERATION. Same delegate-and-switch idiom as `FlakyStore` above and the
/// `Disk` wrappers in tests/group_commit.rs.
struct ListCountingDisk {
    inner: SimDisk,
    lists: Arc<AtomicU64>,
}

impl Disk for ListCountingDisk {
    fn open(&self, path: &str) -> io::Result<Box<dyn charpente_core::io::File>> {
        self.inner.open(path)
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        self.inner.remove(path)
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        self.inner.sync_dir(prefix)
    }
}

fn kernel_app(seed: u64) -> (Sim, App) {
    let (sim, app, _) = kernel_app_counting_lists(seed);
    (sim, app)
}

fn kernel_app_counting_lists(seed: u64) -> (Sim, App, Arc<AtomicU64>) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let mut cfg = AppConfig::default();
    // Same 64 KiB as every other fixture in this file, so `RECORDS_PER_
    // SEGMENT` keeps its measured meaning here too.
    cfg.db.wal.segment_bytes = 64 << 10;
    let lists = Arc::new(AtomicU64::new(0));
    let disk = ListCountingDisk {
        inner: sim.disk(),
        lists: Arc::clone(&lists),
    };
    let app = App::open(
        Box::new(disk),
        Schema::new().table::<Rec>(),
        Box::new(sim.entropy()),
        cfg,
    )
    .unwrap()
    .mount(routes![put_rec]);
    (sim, app, lists)
}

/// A kernel whose archive already has a backlog: `records` are committed
/// BEFORE `Archive::open`, so the arm has real work from its first
/// iteration rather than only whatever the requests below commit.
fn kernel_with_archive(seed: u64, records: u64) -> (Sim, Kernel, SharedStore) {
    let (sim, mut app) = kernel_app(seed);
    if records > 0 {
        write_records(app.db_for_tests(), records);
    }
    let arc = Archive::open(archive_config(0, 8 << 20), app.db_for_tests()).unwrap();
    let store = SharedStore::new();
    let kernel = Kernel::with_archive(
        app,
        Box::new(sim.clock()),
        arc,
        Box::new(store.clone()) as Box<dyn ObjectStore>,
    )
    .unwrap();
    (sim, kernel, store)
}

fn kernel_with_archive_and_requests() -> (Sim, Kernel, SharedStore, EndlessSource) {
    let (sim, kernel, store) = kernel_with_archive(0x746b_3961_0000_0001, 3 * RECORDS_PER_SEGMENT);
    (sim, kernel, store, EndlessSource { next: 0 })
}

fn kernel_without_archive() -> (Sim, Kernel, EndlessSource) {
    let (sim, app) = kernel_app(0x746b_3961_0000_0002);
    let kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    (sim, kernel, EndlessSource { next: 0 })
}

/// Runs exactly `n` iterations of the loop's DECISION (`Kernel::step`) and
/// hands back the counters. Never parks: a park is what a driver does with
/// `Step::Idle`, and these tests want to observe that value, not consume it.
fn run_kernel_steps(k: &mut Kernel, source: &mut dyn RequestSource, n: usize) -> KernelStats {
    for _ in 0..n {
        k.step(source).expect("the kernel must not stop");
    }
    k.stats()
}

/// Steps until `Kernel::step` reports `Idle`, or gives up after `bound`.
/// `None` means the loop never went quiet — which is the failure mode a
/// spinning ready predicate produces, so it is a verdict, not a timeout.
fn steps_to_kernel_idle(
    k: &mut Kernel,
    source: &mut dyn RequestSource,
    bound: usize,
) -> Option<usize> {
    for n in 0..bound {
        if let Step::Idle { .. } = k.step(source).expect("the kernel must not stop") {
            return Some(n + 1);
        }
    }
    None
}

/// The plan's step-1 test. Two counters, and neither may be zero: an arm
/// that never runs is not an arm, and an arm that runs to completion inside
/// one iteration has taken the loop over.
#[test]
fn the_archive_arm_ticks_without_starving_requests() {
    let (_sim, mut k, store, mut source) = kernel_with_archive_and_requests();
    let served = run_kernel_steps(&mut k, &mut source, 1_000);

    assert!(
        served.requests > 0,
        "the archive arm starved the request arm"
    );
    assert!(served.archive_steps > 0, "the archive arm never ran");
    // WEAKER THAN IT READS, and kept only as a cheap upper bound: a
    // drain-to-completion arm empties the archive and is then held by the
    // data watermark, so this stays green under exactly the mutation it looks
    // like it guards. `the_archive_arm_does_one_step_per_kernel_iteration` is
    // the real oracle for that property.
    assert!(
        served.archive_steps <= 1_000,
        "{} archive steps over 1,000 kernel iterations: the arm ran more than once per \
         iteration, so `read_budget_bytes` no longer bounds what one loop pass costs",
        served.archive_steps
    );
    assert!(
        !store.keys().is_empty(),
        "the arm ran {} times and put nothing in the bucket, so 'it ran' is a counter moving \
         rather than an archive working",
        served.archive_steps
    );
}

/// The plan's step-1 test, second half.
#[test]
fn a_kernel_without_an_archive_never_gates_on_one() {
    let (_sim, mut k, mut source) = kernel_without_archive();
    let served = run_kernel_steps(&mut k, &mut source, 100);

    assert_eq!(served.archive_steps, 0);
    assert!(served.requests > 0);
}

/// One `Archive::step` per kernel iteration, never a loop to completion.
///
/// `Archive::step`'s own `read_budget_bytes` exists precisely so that no
/// single call reads a whole 64 MiB segment — and that bound is worth
/// nothing if the arm drains the archive before returning. Measured against
/// a SILENT source and a three-segment backlog, so the archive arm is the
/// only ready source and the count is exact rather than probabilistic: five
/// iterations, five steps, with plenty left to do.
///
/// The `!idle()` assertion is the non-vacuity guard. Without it the same
/// numbers would pass against an archive that had already finished, which
/// would make "it stopped at five" a statement about the archive rather than
/// about the arm.
#[test]
fn the_archive_arm_does_one_step_per_kernel_iteration() {
    let (_sim, mut k, _store) = kernel_with_archive(0x746b_3961_0000_0005, 3 * RECORDS_PER_SEGMENT);
    let mut source = SilentSource;

    let served = run_kernel_steps(&mut k, &mut source, 5);

    assert!(
        !k.archive_for_tests().expect("built with an archive").idle(),
        "the archive ran out of work inside five steps, so a count of five proves nothing \
         about the arm"
    );
    assert_eq!(
        served.archive_steps, 5,
        "five kernel iterations cost {} archive steps: the arm loops inside one iteration \
         instead of taking one unit of work and returning",
        served.archive_steps
    );
}

/// RULING 57, first half. A caught-up archive must let the loop go quiet.
///
/// This is a CORRECTNESS property, not a tidiness one: `Kernel::step` that
/// returns `Worked` forever never reaches `Step::Idle`, so `RequestSource::
/// wait_until` is never called — a production core pegged at 100% on an
/// archive with nothing to do, and under the simulator virtual time that
/// never advances at all.
///
/// The second assertion is the one that would survive a lucky first: after
/// the loop goes quiet, running it a thousand more times against a `Db` that
/// is not moving must cost ZERO further archive steps. Phrased against a
/// counter read BEFORE the second run rather than against any expression the
/// predicate itself uses (T7.5's I-1).
#[test]
fn a_caught_up_archive_still_lets_the_kernel_reach_idle() {
    let (_sim, mut k, store, _source) = kernel_with_archive_and_requests();
    let mut source = SilentSource;

    let steps = steps_to_kernel_idle(&mut k, &mut source, 10_000)
        .expect("the kernel never went idle: the archive arm spins the loop");
    assert!(
        !store.keys().is_empty(),
        "the kernel idled after {steps} steps having archived nothing, so 'it went quiet' is an \
         archive that never started rather than one that finished"
    );

    let quiet = k.stats().archive_steps;
    let after = run_kernel_steps(&mut k, &mut source, 1_000);
    assert_eq!(
        after.archive_steps,
        quiet,
        "the archive arm ran {} more times over a Db that committed nothing — the ready \
         predicate is not asking whether there is work",
        after.archive_steps - quiet
    );
}

/// F2, MEASURED. The finding routed to this task was that the bounded pin
/// costs a `Disk::list` per `Archive::step` — one syscall in a loop the
/// operator controlled while the archive was standalone, but "a `list` per
/// kernel iteration, on the same thread that serves requests" once it
/// becomes an arm.
///
/// That second half is what the ready predicate makes false. The arm is
/// scheduled only when `db.last_applied()` has moved or a transition is
/// pending, so a caught-up archive over a `Db` that is not committing costs
/// ZERO archive steps and therefore zero `Disk::list`s, however long the
/// kernel loops. Counted at the `Disk` boundary rather than inferred from
/// `archive_steps`, so it is a measurement of the thing itself.
#[test]
fn a_caught_up_archive_costs_no_disk_list_per_kernel_iteration() {
    let (sim, mut app, lists) = kernel_app_counting_lists(0x746b_3961_0000_0006);
    write_records(app.db_for_tests(), 3 * RECORDS_PER_SEGMENT);
    let arc = Archive::open(archive_config(0, 8 << 20), app.db_for_tests()).unwrap();
    let store = SharedStore::new();
    let mut k = Kernel::with_archive(
        app,
        Box::new(sim.clock()),
        arc,
        Box::new(store.clone()) as Box<dyn ObjectStore>,
    )
    .unwrap();
    let mut source = SilentSource;

    steps_to_kernel_idle(&mut k, &mut source, 10_000)
        .expect("the kernel never went idle: the archive arm spins the loop");
    assert!(
        !store.keys().is_empty(),
        "the archive shipped nothing, so a listing count of zero below would be a statement \
         about an archive that never started"
    );

    let caught_up = lists.load(Ordering::Relaxed);
    assert!(
        caught_up > 0,
        "the counter never incremented, so the wrapper is not on the Db's disk and a delta \
         of zero below would measure nothing"
    );
    run_kernel_steps(&mut k, &mut source, 1_000);

    assert_eq!(
        lists.load(Ordering::Relaxed),
        caught_up,
        "1,000 kernel iterations over a Db that committed nothing cost {} Disk::list calls: \
         the archive's bound check is running once per LOOP PASS, on the same thread that \
         serves requests",
        lists.load(Ordering::Relaxed) - caught_up
    );
}

/// Spec §7's last row ("`ObjectStore` IO error on the archive arm | archive
/// arm degrades; **never poisons the Db**") and §8's first invariant ("an
/// archive failure is not a database failure"), which together decide what
/// the arm does with an `Err` — and nothing in the plan pinned it.
///
/// A `?` in the arm would kill a serving node over a transient PUT: exactly
/// the power spec §5 says a derived artifact must not have over the thing it
/// derives from. `run_kernel_steps` expects `Ok` from every `Kernel::step`,
/// so that mutation panics here rather than passing quietly.
///
/// Seeded fault injection (hard rule 3): `object_put_error: 1.0` through
/// `SimObjectStore`'s own fault stream, so the failure is drawn from the
/// seed rather than from a test-local switch.
#[test]
fn an_archive_io_failure_degrades_the_arm_and_never_the_kernel() {
    let (sim, mut app) = kernel_app(0x746b_3961_0000_0007);
    write_records(app.db_for_tests(), 3 * RECORDS_PER_SEGMENT);
    let arc = Archive::open(archive_config(0, 8 << 20), app.db_for_tests()).unwrap();
    let store = SharedStore::wrapping(SimObjectStore::with_faults(
        0x746b_3961_0000_0007,
        FaultConfig {
            object_put_error: 1.0,
            ..Default::default()
        },
    ));
    let mut k = Kernel::with_archive(
        app,
        Box::new(sim.clock()),
        arc,
        Box::new(store.clone()) as Box<dyn ObjectStore>,
    )
    .unwrap();
    let mut source = EndlessSource { next: 0 };

    let served = run_kernel_steps(&mut k, &mut source, 1_000);

    assert!(
        served.archive_steps > 0,
        "the arm never ran, so 'the kernel survived it' is untested"
    );
    assert!(
        served.requests > 0,
        "the kernel stopped serving requests while its archive was failing"
    );
    assert!(
        store.keys().is_empty(),
        "a store failing every PUT still holds objects: {:?}",
        store.keys()
    );
    assert!(
        !k.app().db().poisoned(),
        "an archive IO failure poisoned the Db — an archive failure is not a database \
         failure (spec §8), and the archive must never acquire the power to stop the \
         primary (spec §5)"
    );
}

/// RULING 57, second half, and the trap the whole ruling exists for.
///
/// A write-gated archive (`db.verifying()`) reports `idle() == true` and
/// returns `Ok(())` — the same answer a caught-up archive gives. `step`
/// re-reads the gate every call and "resumes by itself when the gate opens"
/// ONLY IF something keeps stepping it, so a ready predicate that skips on
/// `idle` parks the archive permanently: the gate opening is never observed,
/// because observing it requires the step the predicate is refusing.
///
/// The first assertion is not scenery. Without it a predicate that never
/// gated at all would pass the second one, and the test would be measuring
/// nothing.
#[test]
fn a_write_gated_archive_resumes_archiving_after_the_gate_opens() {
    let (_sim, mut k, store) = kernel_with_archive(0x746b_3961_0000_0003, RECORDS_PER_SEGMENT);
    let mut source = SilentSource;

    k.app_mut_for_tests()
        .db_for_tests()
        .verify_writes(vec!["witness-a:9000".into()]);
    run_kernel_steps(&mut k, &mut source, 500);
    assert!(
        store.keys().is_empty(),
        "the gate did not hold, so 'it resumed' below proves nothing: {:?}",
        store.keys()
    );

    k.app_mut_for_tests().db_for_tests().allow_writes();
    run_kernel_steps(&mut k, &mut source, 500);

    assert!(
        !store.keys().is_empty(),
        "the archive never resumed after the gate opened: the ready predicate read `idle` as \
         'nothing to do' and stopped scheduling the very steps that would have noticed the gate \
         open. A transient verification became a permanent archive outage that reports itself \
         as the healthy steady state"
    );
}

/// A stopped archive reports `idle() == true` and returns `Ok(())` FOREVER
/// (Task 8's `idle()`/`stopped()` split, ruled correct on re-review). An arm
/// that watches only idle-and-errors therefore runs a dead archive
/// indefinitely and presents it as the healthy steady state — a failure with
/// no natural symptom, which is what hard rule 4 asks for an assertion
/// against.
///
/// THE LOAD IS THE ORACLE, and the first version of this test got it wrong.
/// It drove an endless request source, which meant the ready set was never
/// empty, the kernel never parked, and the arm's failure backoff never
/// cleared — so the stopped archive went unstepped for a reason that had
/// nothing to do with `stopped()`, and dropping the `stopped()` clause left
/// the whole suite green. What isolates the clause is a `Db` that keeps
/// COMMITTING while nothing else is ready: every commit moves
/// `db.last_applied()`, which is the watermark the ready predicate re-arms
/// on, so a predicate missing `stopped()` re-arms on every one of them.
#[test]
fn the_kernel_stops_stepping_a_stopped_archive() {
    let (_sim, mut k, store) = kernel_with_archive_over(0x746b_3961_0000_0004, FOREIGN_WRITER);
    let mut source = SilentSource;

    let stopped_at = (0..2_000)
        .find_map(|_| {
            k.step(&mut source).expect("the kernel must not stop");
            k.archive_for_tests()
                .expect("built with an archive")
                .stopped()
                .then(|| k.stats().archive_steps)
        })
        .expect("the planted foreign manifest never stopped the archive");
    let bucket = store.keys();
    let applied_at_stop = k.app().db().last_applied();

    for i in 0..200u64 {
        write_records_at(k.app_mut_for_tests().db_for_tests(), 10_000 + i, 1);
        k.step(&mut source).expect("the kernel must not stop");
    }

    assert_ne!(
        k.app().db().last_applied(),
        applied_at_stop,
        "nothing committed over the second run, so the ready predicate was never re-armed \
         and 'the arm did not run' says nothing about `stopped()`"
    );
    assert_eq!(
        k.stats().archive_steps,
        stopped_at,
        "the kernel drove a permanently stopped archive {} more times: it never asked \
         `stopped()`, and a dead archive is being presented as the healthy steady state",
        k.stats().archive_steps - stopped_at
    );
    assert_eq!(
        store.keys(),
        bucket,
        "a stopped archive wrote to the bucket it refused to own"
    );
}

/// `kernel_with_archive`, but over a bucket a DIFFERENT writer already owns
/// — so `adopt` refuses and latches `State::Stopped` (spec §4 rule 2).
fn kernel_with_archive_over(seed: u64, writer: &str) -> (Sim, Kernel, SharedStore) {
    let (sim, mut app) = kernel_app(seed);
    write_records(app.db_for_tests(), RECORDS_PER_SEGMENT);
    let arc = Archive::open(archive_config(0, 8 << 20), app.db_for_tests()).unwrap();
    let store = SharedStore::new();
    write_foreign_manifest(&store.lock(), writer, 3);
    let kernel = Kernel::with_archive(
        app,
        Box::new(sim.clock()),
        arc,
        Box::new(store.clone()) as Box<dyn ObjectStore>,
    )
    .unwrap();
    (sim, kernel, store)
}

// ------------------- T9 fix round 1: the two clauses that carried no oracle
//
// The review found both Criticals in the same place, and they share a cause:
// the ready predicate's two risk-bearing clauses — the data watermark and
// the failure re-arm — were the two nothing tested. Each mutation below was
// GREEN over all 61 tests before these landed.

/// C-1. The DATA WATERMARK clause, on its own.
///
/// Ruling 57's whole subject is that `Archive::idle()` cannot be the ready
/// predicate: it is a snapshot that only updates when you step, so gating on
/// it is self-sealing — the archive goes idle once and is never scheduled
/// again to notice that the world moved. Replacing ONLY that clause with
/// `!archive.idle()` (leaving `stopped()`/`verifying()` in place) left all
/// 61 tests green while the archive froze at 10 objects and the `Db` ran on
/// past seq 321,000. The task's central trap could be reintroduced in one
/// line with no oracle firing.
///
/// This pins the PROPERTY, never the expression: with the `Db` committing
/// continuously, the bucket must keep GROWING. The first-run guard is what
/// stops it degenerating into "the archive started once".
#[test]
fn the_archive_keeps_shipping_while_the_db_keeps_committing() {
    let (_sim, mut k, store, mut source) = kernel_with_archive_and_requests();

    run_kernel_steps(&mut k, &mut source, 1_000);
    let early = store.keys().len();
    assert!(
        early > 0,
        "the archive shipped nothing at all in the first run, so 'it stopped shipping' below \
         would be measuring an archive that never started"
    );

    run_kernel_steps(&mut k, &mut source, 2_000);
    let late = store.keys().len();

    assert!(
        late > early,
        "the archive shipped {early} objects and then stopped, while the request arm went on \
         committing to seq {:?}. The ready predicate is reading a STALE idle snapshot instead \
         of asking whether the Db has moved since the archive last looked — Ruling 57's Trap 3, \
         and a silent data-retention failure with no crash and no notice",
        k.app().db().last_applied()
    );
}

/// I-1. The FAILURE RE-ARM clause, at the property that actually matters:
/// an archive that failed must come back on its own when the store heals.
///
/// `an_archive_io_failure_degrades_the_arm_and_never_the_kernel` above only
/// asks `archive_steps > 0`, so reinstating the `a359356` defect (park-only
/// re-arm) left it green at 7 steps against a control of 406 — the archive
/// stalled permanently after its first failed PUT and nothing noticed.
/// Recovery is the cheapest honest oracle for the whole mechanism: it fails
/// for any re-arm that stops firing, whatever the reason.
///
/// Committing load here; `a_read_only_flood_still_lets_a_failed_archive_retry`
/// is the same property under the load that has no commits at all.
#[test]
fn an_archive_recovers_on_its_own_when_a_store_outage_heals() {
    let (_sim, mut k, store, mut source) = kernel_with_archive_and_requests();
    store.set_puts_fail(true);

    run_kernel_steps(&mut k, &mut source, 1_000);
    assert!(
        store.keys().is_empty(),
        "the outage did not hold, so 'it recovered' below proves nothing: {:?}",
        store.keys()
    );

    store.set_puts_fail(false);
    run_kernel_steps(&mut k, &mut source, 1_000);

    assert!(
        !store.keys().is_empty(),
        "the store healed and the archive never came back. Its failure re-arm stopped firing, \
         so one transient PUT error is a permanent, silent archive outage — no crash, no \
         notice, and a bucket that quietly stopped growing"
    );
}

/// C-2. The same recovery property under SATURATING READ-ONLY LOAD — the
/// fifth case the `a359356` re-arm analysis enumerated four of and missed.
///
/// The failure re-arm had exactly two triggers: `db.last_applied()` moving,
/// or the ready set emptying (a park). A flood of requests that stage no
/// record fires NEITHER — `pending.is_some()` on every iteration means the
/// kernel never parks, and nothing committing means the watermark never
/// differs. Probed on the shipped code: six archive steps, then nothing ever
/// again, including 5,000 iterations after the store had healed, with 4,096
/// records of committed data left unarchived.
///
/// This is an ordinary state for a read-heavy primary, which is what makes
/// it the worst failure shape this subsystem can have (hard rule 5): the
/// operator's only symptom is a bucket that silently stopped growing.
///
/// The re-arm this pins is not the data watermark. A retry's precondition is
/// ATTEMPTS PASSING, not new data — a healthy archive with no commits has
/// nothing to do, but a FAILED archive with no commits has a whole backlog
/// to retry, and conflating the two is exactly what broke here.
#[test]
fn a_read_only_flood_still_lets_a_failed_archive_retry() {
    let (sim, mut app) = kernel_app(0x746b_3961_0000_0008);
    write_records(app.db_for_tests(), 3 * RECORDS_PER_SEGMENT);
    let arc = Archive::open(archive_config(0, 8 << 20), app.db_for_tests()).unwrap();
    let store = SharedStore::new();
    store.set_puts_fail(true);
    let mut k = Kernel::with_archive(
        app,
        Box::new(sim.clock()),
        arc,
        Box::new(store.clone()) as Box<dyn ObjectStore>,
    )
    .unwrap();
    let mut source = ReadOnlySource;

    let outage = run_kernel_steps(&mut k, &mut source, 500);
    let applied = k.app().db().last_applied();
    assert!(
        outage.requests > 0 && store.keys().is_empty(),
        "the load or the outage did not hold ({} requests, bucket {:?}), so nothing below is \
         being measured",
        outage.requests,
        store.keys()
    );

    store.set_puts_fail(false);
    run_kernel_steps(&mut k, &mut source, 2_000);

    assert_eq!(
        k.app().db().last_applied(),
        applied,
        "the read-only source committed something, so this is no longer the no-commit case \
         C-2 lives in and a green result would prove nothing"
    );
    assert!(
        !store.keys().is_empty(),
        "the store healed 2,000 iterations ago and the archive never retried. Under a flood of \
         non-committing requests the kernel never parks and `last_applied` never moves, so a \
         re-arm keyed on 'a commit or a park' fires neither trigger and the archive stops \
         forever — silently, with a backlog it can still see"
    );
}

/// The retry hold's own correctness content, as opposed to its cost.
///
/// A failed `start` leaves the archive in a non-`Idle` state permanently, so
/// without the hold the arm is READY on every iteration for the whole
/// duration of an outage — and `Kernel::step` that returns `Worked` forever
/// never reaches `Step::Idle`, so `RequestSource::wait_until` is never
/// called. That is a core pegged at 100% in production and, under the
/// simulator, virtual time that never advances at all: the same spin
/// `a_caught_up_archive_still_lets_the_kernel_reach_idle` pins for a HEALTHY
/// archive, which is exactly why it does not cover this one.
///
/// Measured cost of the hold, for the record rather than as an assertion
/// (docs/BENCH.md carries no archive numbers, so no threshold here could be
/// justified): over 2,000 iterations of a saturating read-only outage the arm
/// takes 659 of them with the hold and 989 without, and the request arm
/// serves 85,824 requests against 64,704.
#[test]
fn a_failing_archive_still_lets_the_kernel_park() {
    let (sim, mut app) = kernel_app(0x746b_3961_0000_0009);
    write_records(app.db_for_tests(), 3 * RECORDS_PER_SEGMENT);
    let arc = Archive::open(archive_config(0, 8 << 20), app.db_for_tests()).unwrap();
    let store = SharedStore::new();
    store.set_puts_fail(true);
    let mut k = Kernel::with_archive(
        app,
        Box::new(sim.clock()),
        arc,
        Box::new(store.clone()) as Box<dyn ObjectStore>,
    )
    .unwrap();
    let mut source = SilentSource;

    let steps = steps_to_kernel_idle(&mut k, &mut source, 10_000).expect(
        "the kernel never went idle while its archive was failing: the arm is ready on \
         every iteration, so `wait_until` is never called — a core at 100% in production, \
         and virtual time that never advances under the sim",
    );

    assert!(
        store.keys().is_empty(),
        "the outage did not hold, so the kernel may have gone idle simply because the \
         archive succeeded: {:?}",
        store.keys()
    );
    assert!(
        k.stats().archive_steps > 0,
        "the kernel went idle after {steps} steps without the arm ever running, so this \
         measures an archive that was never scheduled rather than one that failed"
    );
}
