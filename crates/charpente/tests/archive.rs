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
///
/// F8: each call `Archive::open`s a fresh archive on the SAME `Db`, and
/// `Db::add_retention_pin` only ever pushes — nothing removes a pin — so a
/// full cut loop leaves one pin per cut behind and `retention_floor()` (the
/// minimum) sticks at the first one. That freezes `Wal::gc` for the rest of
/// the loop, which is harmless here (the loop asserts about objects, never
/// about gc) but is not something to build a gc-sensitive test on. Task 7
/// works on this pin.
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
    write_records(&mut db, 50 * RECORDS_PER_SEGMENT);
    let seals = sealed_segment_count(&db);
    // F4's sweep: `uploads >= seals` cannot fail when `seals` is 0, and a
    // seal count is a measurement, not a given. Guard it.
    assert!(
        seals >= 2,
        "fixture sealed {seals} segment(s); a rate assertion against 0 or 1 is not a rate"
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
const STEPS_TO_ARCHIVE_5000_RECORDS: usize = 49;

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

    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);
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
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);
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
    // yield, not a shutdown.
    write_records(&mut db, 100 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);
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
    write_records(&mut db, 3 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);
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
    assert_eq!(
        before.segments.len(),
        sealed,
        "every sealed segment must already be archived, so none of them is backlog: {:?}",
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
