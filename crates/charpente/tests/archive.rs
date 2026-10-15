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

mod support;

use charpente::archive::manifest::{
    Manifest, SnapshotMeta, TableBatches, decode_snapshot_batch, manifest_key, segment_key,
    snapshot_batch_key, snapshot_meta_key,
};
use charpente::archive::restore::{RestoreOpts, RestoreReport, restore};
use charpente::archive::{Archive, ArchiveConfig};
use charpente::wire::WireU64;
use charpente::{Db, DbConfig, Id, Schema, Seeder, Wire, event, job, table};
use charpente_core::crc::crc32;
use charpente_core::io::{ObjectDone, ObjectOp, ObjectStore};
use charpente_core::table::{Codec, IndexDef, Row};
use charpente_sim::object::SimObjectStore;
use charpente_sim::{FaultConfig, Sim};

const PREFIX: &str = "archive-test-prefix";
const WRITER: &str = "node-a:9000";

/// Emitted by the restore fixtures so `charpente.events` and the
/// `next_event_id` meta counter are non-trivial. Without one, "the archive
/// ships the Db's event counter" and "the archive ships a zero" are the same
/// assertion — measured: a `next_event_id: 0` mutation survived a fully green
/// suite until this existed.
#[event(global)]
struct Ping {
    // `WireU64`, not a bare `u64`, since main's `Wire` bound landed: an event
    // payload IS the JSON a client sees, and a u64 above 2^53 loses precision
    // in JavaScript silently. Nothing here reads `n` — it exists so
    // `next_event_id` is non-trivial — so the wrapper costs the fixture
    // nothing.
    n: WireU64,
}

/// A fixed-width row: every commit produces an identically-sized WAL frame,
/// which is what makes `RECORDS_PER_SEGMENT` an exact, reusable constant
/// (Ruling 34) rather than an average.
#[table(global)]
struct Rec {
    id: Id<Rec>,
    value: u64,
}

/// Task 6c's archive-restore tripwire needs a genuinely scoped table — the
/// scope marker (`#[table(scoped_by = …)]` requires the app's own `Tenant`
/// impl, trivial here because this file reads the archived bytes rather than
/// going through a scope, same as `charpente/tests/typed_tables.rs`'s `Org`).
#[table(global)]
struct ScopedOrg {
    id: Id<ScopedOrg>,
    name: String,
}

impl charpente::Tenant for ScopedOrg {
    const NAME: &'static str = "ScopedOrg";
    type Who = charpente::auth::Principal;
    type Error = charpente::http::ApiError;

    fn authorize(
        _tx: &charpente::Tx,
        _who: &charpente::auth::Principal,
        _scope: Id<ScopedOrg>,
    ) -> Result<(), charpente::http::ApiError> {
        Ok(())
    }
}

#[table(scoped_by = ScopedOrg)]
struct ScopedRec {
    id: Id<ScopedRec>,
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
    /// GETs left to fail (Task 12). The fourth switch, and the only one
    /// aimed at RESTORE rather than at the archive: `restore` reads the
    /// bucket and writes the destination, so cutting it off part-way is how
    /// spec §9's "crash mid-restore → re-run from scratch converges" axis
    /// is reached at all. `SimObjectStore`'s own `object_get_error` cannot
    /// stand in: it is a per-op probability fixed at construction, so a rate
    /// high enough to cut a restore reliably also stops the RETRY that is
    /// the property, and one low enough to let the retry through cuts almost
    /// nothing. A COUNT is the aimable version of the same fault.
    fail_next_gets: Cell<u64>,
    /// TASK 12.5. What to do with an `ObjectOp::Delete`. Both failure modes
    /// leave the object IN the bucket, which is the realistic shape of a
    /// delete that fails forever — a bucket policy that denies deletion —
    /// and the only shape that can prove reclamation never became a
    /// correctness dependency: if the archive stays correct and keeps
    /// advancing while nothing is ever reclaimed, reclamation is the
    /// optimisation spec §3 says it is. These stay after the delete-axis
    /// task even though `FaultConfig.object_delete_error` now exists and
    /// `SimObjectStore`'s `Delete` arm CAN fail: the axis is a per-op
    /// probability, and these are aimed switches — "every delete from here
    /// on", which is the delete-fails-forever shape a probability cannot
    /// guarantee in any finite run. (The claim this comment used to carry —
    /// that no axis could cover it because adding one would change the
    /// frozen seed->config mapping — was wrong on every clause. The axis
    /// rolls from its own `keyed_stream(seed, "object_store_delete")` and
    /// perturbed nothing: all twelve convergence coverage counts reproduced
    /// at four bases.)
    deletes: Cell<DeleteMode>,
    /// Every DELETE key the archive ASKED for, in order. Attempted, not
    /// necessarily applied — which is what the ownership and
    /// live-generation oracles need to read, since a delete that was
    /// refused is still a delete that was ISSUED.
    deleted: RefCell<Vec<String>>,
    /// A DELETE intercepted at `start` and owed an `Err` from `poll`
    /// (`DeleteMode::FailPoll`). The inner store never sees it, so the
    /// object survives the failure the archive is told about — the same "no
    /// op went in flight" contract `reachable` carries.
    refused_delete: Cell<bool>,
    /// A DELETE forwarded to the inner store and awaiting its `poll`. Only
    /// `DeleteMode::Pass` sets it — the two aimed modes never reach the
    /// inner store, so their failures are not `object_delete_error`'s and
    /// must not be counted as evidence the axis fired.
    in_flight_delete: Cell<bool>,
    /// DELETEs the INNER store failed, i.e. `object_delete_error` firing.
    /// The convergence property's non-vacuity witness: without it the axis
    /// could sample to 0.0 in every universe and the new interleaving
    /// coverage would read as exercised while nothing ever failed.
    delete_failures: Cell<u64>,
    /// Every PUT key the archive asked for, with the CRC of the bytes it
    /// asked to write. A generation's snapshot objects are written ONCE
    /// (spec §2), and now that reclamation DELETES a retired generation's
    /// objects an end-state comparison can no longer see an overwrite —
    /// this ledger can, and it also catches an overwrite that is later
    /// reclaimed away, which an end-state comparison never could.
    put_ledger: RefCell<Vec<(String, u32)>>,
    /// The generation the manifest in the bucket names, mirrored as manifest
    /// PUTs land. Mirrored rather than read back because `assert_deletable`
    /// runs INSIDE `start`, where a nested `ObjectStore` op would trip the
    /// in-flight assert (spec §1).
    live_generation: Cell<Option<u64>>,
    /// The generation of a manifest PUT in flight, promoted to
    /// `live_generation` when it lands.
    manifest_in_flight: Cell<Option<u64>>,
}

/// TASK 12.5. What `FlakyStore` does with an `ObjectOp::Delete`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DeleteMode {
    /// Delegate to the inner store: the object really goes.
    Pass,
    /// Refuse at `start`, before the inner store sees the op.
    FailStart,
    /// Accept at `start`, never forward it, and fail the `poll` — the arm
    /// `FailStart` cannot reach.
    FailPoll,
}

/// THE reclamation invariant (Task 12.5; spec §3, §8), enforced at the
/// INSTANT of the delete by the store itself rather than at the end of a
/// test: **an object may outlive the manifest that named it, never the
/// reverse.**
///
/// Checked here rather than in each test because it has to hold on every
/// step of every test that can reach a delete, including the ones whose
/// subject is something else entirely. A per-test end-state assertion cannot
/// see a window that opened and closed between two `step`s, and the whole
/// hazard this task carries lives in exactly such a window.
///
/// The check is a GENERATION comparison and needs nothing else. Keys are
/// partitioned by generation (`<prefix>/<gen:020>/...`) and a manifest names
/// exactly one generation, so "this key is not the live generation's" is the
/// whole of it: no snapshot meta to read, no nested store op, and no
/// dependence on the batch counts a manifest does not carry.
fn assert_deletable(key: &str, live_generation: Option<u64>) {
    assert_ne!(
        key,
        manifest_key(PREFIX),
        "DELETE of \"{key}\", the manifest key itself: it is the generation pointer and the only \
         evidence of who owns the prefix (spec §2, §4), so nothing in this design may delete it"
    );
    let Some(live) = live_generation else {
        panic!(
            "DELETE \"{key}\" with no manifest in the bucket at all — nothing has been \
             superseded, so nothing is reclaimable. Deletion FOLLOWS the manifest swap (spec §3, \
             Task 12.5); a delete that precedes it can leave a manifest naming objects that are \
             already gone"
        );
    };
    let Some(generation) = key_generation(key) else {
        panic!(
            "DELETE \"{key}\" is not under \"{PREFIX}/<gen:020>/\", so it names no generation and \
             nothing can establish that a manifest stopped referencing it (spec §3, Task 12.5)"
        );
    };
    assert!(
        generation < live,
        "DELETE \"{key}\" belongs to generation {generation} and the manifest in the bucket names \
         generation {live} — deleting it strands that manifest on an object that is gone. Only a \
         generation the manifest NO LONGER names is reclaimable (spec §3, Task 12.5)"
    );
}

/// The generation a `<prefix>/<gen:020>/...` key belongs to. `None` for a key
/// outside this archive's generation layout, which `assert_deletable` treats
/// as a violation rather than as a pass.
fn key_generation(key: &str) -> Option<u64> {
    let rest = key.strip_prefix(PREFIX)?.strip_prefix('/')?;
    rest.split_once('/')?.0.parse().ok()
}

impl FlakyStore {
    fn new(inner: SimObjectStore) -> FlakyStore {
        FlakyStore {
            inner,
            reachable: Cell::new(true),
            start_errors: Cell::new(false),
            in_flight_put: RefCell::new(None),
            lost_ack_for: RefCell::new(None),
            fail_next_gets: Cell::new(0),
            deletes: Cell::new(DeleteMode::Pass),
            deleted: RefCell::new(Vec::new()),
            refused_delete: Cell::new(false),
            in_flight_delete: Cell::new(false),
            delete_failures: Cell::new(0),
            put_ledger: RefCell::new(Vec::new()),
            live_generation: Cell::new(None),
            manifest_in_flight: Cell::new(None),
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

    /// The next `n` GETs fail at `start`, before the inner store sees them
    /// — so no op goes in flight and the next attempt is a clean first one,
    /// the same contract `reachable` carries. `0` clears the switch.
    fn set_failing_gets(&self, n: u64) {
        self.fail_next_gets.set(n);
    }

    /// Test-only inspection, delegated straight through — same contract as
    /// `SimObjectStore::keys`.
    fn keys(&self) -> Vec<String> {
        self.inner.keys()
    }

    /// TASK 12.5. What happens to an `ObjectOp::Delete` from here on.
    fn set_deletes(&self, mode: DeleteMode) {
        self.deletes.set(mode);
    }

    /// Every DELETE this store was asked for, in order.
    fn deleted(&self) -> Vec<String> {
        self.deleted.borrow().clone()
    }

    /// How many forwarded DELETEs the inner store failed.
    fn delete_failures(&self) -> u64 {
        self.delete_failures.get()
    }

    /// A mark into the PUT ledger, for `puts_since`.
    fn put_mark(&self) -> usize {
        self.put_ledger.borrow().len()
    }

    /// Every PUT asked for since `mark`, key and CRC of the bytes.
    fn puts_since(&self, mark: usize) -> Vec<(String, u32)> {
        self.put_ledger.borrow()[mark..].to_vec()
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
        if let ObjectOp::Get { key } = &op
            && self.fail_next_gets.get() > 0
        {
            self.fail_next_gets.set(self.fail_next_gets.get() - 1);
            return Err(io::Error::other(format!(
                "simulated FlakyStore GET failure for \"{key}\" (test-local counted switch)"
            )));
        }
        // TASK 12.5. Every delete this store is ever asked for passes the
        // reclamation invariant first, so it holds on every step of every
        // test rather than at the end of the few that look.
        if let ObjectOp::Delete { key } = &op {
            self.deleted.borrow_mut().push(key.clone());
            assert_deletable(key, self.live_generation.get());
            match self.deletes.get() {
                DeleteMode::Pass => self.in_flight_delete.set(true),
                DeleteMode::FailStart => {
                    return Err(io::Error::other(format!(
                        "simulated FlakyStore delete refusal: DELETE \"{key}\" was refused at \
                         start and never applied (test-local switch)"
                    )));
                }
                DeleteMode::FailPoll => {
                    self.refused_delete.set(true);
                    return Ok(());
                }
            }
        }
        self.manifest_in_flight.set(None);
        if let ObjectOp::Put { key, data } = &op {
            self.put_ledger
                .borrow_mut()
                .push((key.clone(), crc32(data)));
            // Only a manifest that DECODES moves the mirror: the tests that
            // plant an undecodable one are asserting that the archive stops,
            // and a decode failure here must not be read as "the bucket has
            // no manifest".
            if *key == manifest_key(PREFIX)
                && let Ok(m) = Manifest::decode(data)
            {
                self.manifest_in_flight.set(Some(m.generation));
            }
        }
        *self.in_flight_put.borrow_mut() = match &op {
            ObjectOp::Put { key, .. } => Some(key.clone()),
            _ => None,
        };
        self.inner.start(op)
    }

    fn poll(&self) -> Option<io::Result<ObjectDone>> {
        // Nothing was forwarded, so the inner store has nothing in flight
        // and the object is still there — a delete the archive is told
        // failed, and that really did not happen.
        if self.refused_delete.replace(false) {
            return Some(Err(io::Error::other(
                "simulated FlakyStore delete refusal: the DELETE was never applied (test-local \
                 switch)",
            )));
        }
        let done = self.inner.poll()?;
        if self.in_flight_delete.replace(false) && done.is_err() {
            self.delete_failures.set(self.delete_failures.get() + 1);
        }
        let key = self.in_flight_put.borrow_mut().take();
        let landed = self.manifest_in_flight.replace(None);
        if let (Ok(ObjectDone::Put), Some(key)) = (&done, &key)
            && let Some(needle) = self.lost_ack_for.borrow().as_deref()
            && key.contains(needle)
        {
            // The inner store already applied it — that is the whole point,
            // and it is why the manifest mirror advances on this path too:
            // the bucket really does name the new generation now.
            if landed.is_some() {
                self.live_generation.set(landed);
            }
            return Some(Err(io::Error::other(format!(
                "simulated FlakyStore lost ack: PUT \"{key}\" landed but the ack was lost \
                 (test-local switch)"
            ))));
        }
        if matches!(done, Ok(ObjectDone::Put)) && landed.is_some() {
            self.live_generation.set(landed);
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

/// `write_records_at`, but every commit also EMITS. Still exactly one record
/// per row, so the seq <-> id coincidence `source_at` leans on holds
/// unchanged — the events simply ride along inside the same transaction.
fn write_records_emitting(db: &mut Db, from: u64, n: u64) {
    for i in from..from + n {
        db.transact(|tx| -> Result<(), std::convert::Infallible> {
            tx.put(&Rec {
                id: Id::from_raw(i),
                value: i,
            });
            tx.emit(Ping { n: WireU64(i) });
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
    scoped: bool,
) -> Vec<(u64, u64, Vec<u8>)> {
    let mut out = Vec::new();
    for n in 0..t.batches {
        let key = snapshot_batch_key(PREFIX, generation, &t.name, n);
        let bytes = get_object(store, &key)
            .unwrap_or_else(|| panic!("the meta declares batch {key}, which is not in the store"));
        out.extend(decode_snapshot_batch(&t.name, scoped, &bytes).expect("a valid snapshot batch"));
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

fn write_foreign_manifest(store: &dyn ObjectStore, writer: &str, generation: u64) {
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

/// A regression guard on an ARCHITECTURAL property, not coverage of a fault
/// path — read the label before counting this test as either.
///
/// The store is unreachable and every step below fails, but no `ObjectStore`
/// failure can reach `Db::poisoned()` by any route: `Archive::step` takes
/// `db: &Db` while `Db::transact` takes `&mut self`, both `Engine::poisoned`
/// sites are apply-path failure arms, and the `ObjectStore` is a separate IO
/// path with no route into the Db's own store. So this passes BY
/// CONSTRUCTION today, and it is worth keeping for what it does catch: the
/// day the archive is given a write path — a `&mut Db` in its signature —
/// this is where spec §8's first bullet stops being true. What it is not is
/// evidence that the guarantee survives an injected fault, because no
/// injected fault was ever in a position to break it.
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
    let archived = read_snapshot_rows(&store, m.generation, rec, db.table_scoped("Rec").unwrap());
    assert_eq!(
        archived.len() as u64,
        rows,
        "the snapshot carries {} of the {rows} rows written",
        archived.len()
    );
    for (i, (scope, id, bytes)) in archived.iter().enumerate() {
        let i = i as u64;
        assert_eq!(
            *scope,
            charpente::core::table::UNSCOPED,
            "Rec is not tenant-scoped, so every archived row's scope must be UNSCOPED"
        );
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
        let table_scoped = db.table_scoped(&t.name).unwrap();
        let archived = read_snapshot_rows(&store, m.generation, t, table_scoped);
        let live = snap
            .scan_raw_from(&t.name, table_scoped, None, usize::MAX)
            .unwrap();
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
    // TASK 12.5 changed what this test can READ, not what it asserts. A
    // retired generation's objects are now DELETED when the next one's
    // manifest lands, so an end-state comparison of generation 0's snapshot
    // objects finds an empty set and proves nothing. The property — a
    // generation's snapshot objects are written ONCE — moves to the store's
    // PUT ledger, which is strictly stronger: it also catches an overwrite
    // that reclamation later removed, which the end-state comparison never
    // could see at all.
    let (mut db, store, mut arc) = archive_fixture_with_delete_watch(cadence);

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
    let mark = store.put_mark();

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
    //
    // Read off the PUT ledger rather than the bucket, since reclamation has
    // by now removed generation 0 entirely: NOTHING was written to one of
    // its keys after phase 1 — not the same bytes, and certainly not
    // different ones.
    let under = format!("{PREFIX}/{:020}/", before.generation);
    let rewrites: Vec<_> = store
        .puts_since(mark)
        .into_iter()
        .filter(|(k, _)| k.starts_with(&under))
        .collect();
    assert_eq!(
        rewrites,
        Vec::new(),
        "phase 2 wrote into the CLOSED generation {}'s keys: {rewrites:?}. A refreshed snapshot \
         must be a new generation — writing one in place replaces content a manifest pointed at, \
         which tears a generation with no crash and no reordering at all (spec §2: the manifest \
         is the layout's only mutable key)",
        before.generation
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

#[derive(Serialize, Deserialize, Wire)]
struct RecBody {
    n: WireU64,
}

/// A writing route, deliberately: an archive with a `Db` that never moves
/// goes caught-up and stays there, and every "does the arm keep up under
/// load" question would then be asked of an archive with nothing to do.
#[route(POST "/rec", public)]
fn put_rec(tx: &mut Tx, body: RecBody) -> Result<WireU64, ApiError> {
    tx.put(&Rec {
        id: Id::from_raw(body.n.0),
        value: body.n.0,
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
        let request = Request::new(Method::Post, "/rec").json(&RecBody {
            n: WireU64(self.next),
        });
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

/// Steps once and hands back the archive notice that step left, if any.
fn step_for_notice(k: &mut Kernel, source: &mut dyn RequestSource) -> Option<String> {
    k.step(source).expect("the kernel must not stop");
    k.take_archive_notice()
}

/// Spec §7's operator surface at the kernel: a permanent ownership refusal
/// raises exactly ONE notice, on the step that stops the archive, and it
/// names the writer that owns the bucket.
#[test]
fn a_stopped_archive_raises_one_notice() {
    let (_sim, mut k, _store) = kernel_with_archive_over(0x7A13_0030, FOREIGN_WRITER);
    let mut source = SilentSource;
    let mut notices = Vec::new();
    for i in 0..2_000u64 {
        notices.extend(step_for_notice(&mut k, &mut source));
        if i % 10 == 0 {
            write_records_at(k.app_mut_for_tests().db_for_tests(), 10_000 + i, 1);
        }
    }
    assert!(
        k.archive_for_tests()
            .expect("built with an archive")
            .stopped(),
        "the planted foreign manifest never stopped the archive, so the count below says nothing"
    );
    assert_eq!(
        notices.len(),
        1,
        "a stopped archive must raise exactly one notice, got {}: {:?}",
        notices.len(),
        notices.first()
    );
    assert!(notices[0].contains(FOREIGN_WRITER), "{}", notices[0]);
}

/// Spec §7's first row: a failure the archive retries within its bound is
/// not an operator notice. With every PUT failing and the log well inside
/// `pin_segments`, the archive fails attempt after attempt and the kernel
/// raises nothing.
#[test]
fn a_within_bound_archive_failure_raises_no_notice() {
    let (_sim, mut k, store) = kernel_with_archive(0x7A13_0031, 3 * RECORDS_PER_SEGMENT);
    store.set_puts_fail(true);
    let mut source = EndlessSource { next: 0 };
    let mut notices = Vec::new();
    for _ in 0..300 {
        notices.extend(step_for_notice(&mut k, &mut source));
    }
    let archive = k.archive_for_tests().expect("built with an archive");
    assert!(
        store.keys().is_empty() && k.stats().archive_steps > 0,
        "the outage did not hold or the archive never ran ({} steps, bucket {:?})",
        k.stats().archive_steps,
        store.keys()
    );
    assert!(
        !archive.gapped() && !archive.stopped(),
        "the log left the bound, so this is no longer the within-bound case"
    );
    assert!(
        notices.is_empty(),
        "a within-bound retry raised {} operator notices, the first: {:?}",
        notices.len(),
        notices.first()
    );
}

/// Spec §7: a gap raises exactly ONE notice, on the step that gaps, and it
/// names the bound. Every PUT fails while the log seals past
/// `GAP_PIN_SEGMENTS`; the archive keeps failing afterwards and raises
/// nothing more.
#[test]
fn a_gapped_archive_raises_one_notice() {
    let (sim, mut app) = kernel_app(0x7A13_0032);
    let arc = Archive::open(
        archive_config_bounded(0, 8 << 20, GAP_PIN_SEGMENTS),
        app.db_for_tests(),
    )
    .unwrap();
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
    let mut notices = Vec::new();
    let chunk = RECORDS_PER_SEGMENT / 2;
    let gapped = |k: &Kernel| {
        k.archive_for_tests()
            .expect("built with an archive")
            .gapped()
    };
    // Natural count: the first gap lands in round 18 (9 sealed segments
    // against a bound of 8).
    let mut round = 0;
    while !gapped(&k) {
        assert!(
            round < 40,
            "{round} rounds sealed ~{} segments against a bound of {GAP_PIN_SEGMENTS} with every \
             PUT failing, and the archive never gapped, so the count below says nothing",
            round / 2
        );
        write_records_at(k.app_mut_for_tests().db_for_tests(), round * chunk, chunk);
        for _ in 0..4 {
            notices.extend(step_for_notice(&mut k, &mut source));
        }
        round += 1;
    }
    // The archive keeps failing while gapped. A few single-record commits
    // keep its arm ready without sealing toward a second gap: each gap is
    // its own episode, and this test asks about one.
    for i in 0..200u64 {
        if i % 10 == 0 {
            write_records_at(k.app_mut_for_tests().db_for_tests(), 1_000_000 + i, 1);
        }
        notices.extend(step_for_notice(&mut k, &mut source));
    }
    assert_eq!(
        notices.len(),
        1,
        "a gap must raise exactly one notice, got {}: {:?}",
        notices.len(),
        notices.first()
    );
    assert!(notices[0].contains("pin_segments"), "{}", notices[0]);
}

/// `kernel_with_archive`, but over a bucket a DIFFERENT writer already owns
/// — so `adopt` refuses and latches `State::Stopped` (spec §4 rule 2).
fn kernel_with_archive_over(seed: u64, writer: &str) -> (Sim, Kernel, SharedStore) {
    let (sim, mut app) = kernel_app(seed);
    write_records(app.db_for_tests(), RECORDS_PER_SEGMENT);
    let arc = Archive::open(archive_config(0, 8 << 20), app.db_for_tests()).unwrap();
    let store = SharedStore::new();
    write_foreign_manifest(&*store.lock(), writer, 3);
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

// ------------- T10 prerequisite: the snapshot meta carries what Seeder needs

/// `Seeder::finish` takes FOUR values (`applied_seq`, `next_job_id`,
/// `next_event_id`, `frontier_crc`) and until this task the generation's
/// snapshot meta carried two of them. Neither missing value is derivable
/// from the bucket — see `SnapshotMeta`'s own docs — so restore would have
/// had to invent them. This is the oracle that they are READ off the same
/// MVCC view the rows came from, not invented here either.
#[test]
fn the_snapshot_meta_carries_the_event_counter_and_the_frontier_crc() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records_emitting(&mut db, 0, 3 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);
    write_records_emitting(&mut db, 3 * RECORDS_PER_SEGMENT, RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db, &store);

    let m = read_manifest(&store).expect("a manifest");
    let meta = read_snapshot_meta(&store, m.generation).expect("a snapshot meta");

    // The frame at the snapshot's frontier, read back out of the SOURCE's own
    // log and re-checksummed here — so this compares two independent
    // computations of the same value, not the archive's against itself.
    let frame = db
        .read_log(meta.applied_seq, 1)
        .expect("the frontier frame is retained")
        .into_iter()
        .next()
        .expect("read_log returns the frame it does not refuse");
    assert_eq!(frame.0, meta.applied_seq);
    assert_eq!(
        meta.frontier_crc,
        charpente_core::log::frame_crc(frame.0, &frame.1),
        "the meta's frontier_crc is not the checksum of the record at its own frontier"
    );

    // `next_event_id` is a meta counter, not a row property. The fixture
    // emits on every commit precisely so this is not vacuous: a suite whose
    // event counter is always 0 cannot tell "shipped the Db's counter" from
    // "shipped a zero", and a `next_event_id: 0` mutation survived it.
    // The counter belongs to the SNAPSHOT's frontier, not to the live Db —
    // which has committed a whole chain since. The fixture emits exactly one
    // event per record, so the counter at frontier F is F + 1, computed here
    // from the frontier rather than read back off the same accessor the
    // archive used.
    let live = db.next_event_id().unwrap();
    assert!(
        meta.next_event_id > 0 && live > meta.next_event_id,
        "the fixture must emit, and must go on emitting after the snapshot, or this cannot \
         tell a shipped counter from a shipped zero: meta={}, live={live}",
        meta.next_event_id
    );
    assert_eq!(meta.next_event_id, meta.applied_seq + 1);
}

// ------------------------------------------- Task 10: restore, and its refusals
//
// Every test here calls `charpente::archive::restore::restore` itself — the
// same function `charpente restore` will call (Task 11) and the same one the
// convergence property will drive (Task 12). There is no test-only variant.

/// A destination `DbConfig`. `wal.segment_bytes` matches the source fixtures'
/// 64 KiB so a restored chain lays out in comparable segments; everything
/// else is the production default, including `checkpoint_every` — a restore
/// that only worked with GC disabled would be a restore that does not work.
fn restore_cfg() -> DbConfig {
    let mut cfg = DbConfig::default();
    cfg.wal.segment_bytes = 64 << 10;
    cfg
}

fn restore_opts() -> RestoreOpts {
    RestoreOpts {
        prefix: PREFIX.to_string(),
        force: false,
    }
}

fn restore_schema() -> Schema {
    Schema::new().table::<Rec>()
}

/// A world for the DESTINATION, separate from every source fixture's own.
fn fresh_sim() -> Sim {
    Sim::with_faults(0x7265_7374_6f72_6500, FaultConfig::none())
}

/// Task 6c tripwire (must FAIL FIRST against stored bytes, per the task
/// brief's Step 2): the same property `charpente-core/tests/engine.rs`'s
/// `a_scoped_row_survives_a_snapshot_seed_with_its_scope_intact` proves for
/// replication seeding, through `crates/charpente/src/archive/restore.rs:
/// 276` instead. Tenant 2 writes id 7 on a scoped table; the source `Db` is
/// built through `Seeder` (not `Tx::put`, which always writes scope
/// `UNSCOPED` until Task 7's `tx.scope()` lands — this is the only route to
/// a genuinely nonzero-scope row today), archived, restored into a fresh
/// destination, then read back through `Snapshot::scan_raw` directly —
/// never a round trip through the same broken encoder.
#[test]
fn a_scoped_row_survives_archive_restore_with_its_scope_intact() {
    let src_sim = Sim::with_faults(0x6B00_0009, FaultConfig::none());
    let mut src_cfg = DbConfig::default();
    src_cfg.wal.segment_bytes = 64 << 10;

    let mut row_bytes = Vec::new();
    ScopedRec {
        id: Id::from_raw(7),
        value: 42,
    }
    .encode_row(&mut row_bytes);

    let mut seeder = Seeder::new(
        Box::new(src_sim.disk()),
        src_cfg,
        Schema::new().table::<ScopedRec>(),
    )
    .unwrap();
    seeder.rows(ScopedRec::TABLE, &[(2, 7, row_bytes)]).unwrap();
    let mut src = seeder.finish(0, 0, 0, 0).unwrap();

    let mut arc = Archive::open(archive_config(0, 8 << 20), &mut src).unwrap();
    let store = SimObjectStore::new();
    step_until_idle(&mut arc, &src, &store);
    assert!(
        read_manifest(&store).is_some(),
        "sanity: the archive must have written a generation to restore from"
    );

    let dst_sim = Sim::with_faults(0x6B00_000A, FaultConfig::none());
    let mut dst_cfg = DbConfig::default();
    dst_cfg.wal.segment_bytes = 64 << 10;
    restore(
        &store,
        Box::new(dst_sim.disk()),
        dst_cfg.clone(),
        Schema::new().table::<ScopedRec>(),
        RestoreOpts {
            prefix: PREFIX.to_string(),
            force: false,
        },
        &mut |_| {},
    )
    .unwrap();

    let dst = Db::open(
        Box::new(dst_sim.disk()),
        dst_cfg,
        Schema::new().table::<ScopedRec>(),
    )
    .unwrap();
    let restored = dst
        .snapshot()
        .unwrap()
        .scan_raw(ScopedRec::TABLE, true)
        .unwrap();
    let mut expected_bytes = Vec::new();
    ScopedRec {
        id: Id::from_raw(7),
        value: 42,
    }
    .encode_row(&mut expected_bytes);
    assert_eq!(
        restored,
        vec![(2u64, 7u64, expected_bytes)],
        "the restored row must carry tenant 2's scope, not scope 0 — a bug here would either \
         misdecode the export (scan_raw_from opening the bare rows_def) or land the row under \
         scope 0 on restore (Seeder::rows discarding the scope)"
    );
}

/// Records committed above the opening snapshot, so the generation really
/// has a chain rather than a snapshot alone. Two segments' worth: a
/// single-segment chain would leave `verify_chain`'s loop unexercised.
const RESTORE_CHAIN_RECORDS: u64 = 2 * RECORDS_PER_SEGMENT;

/// A source `Db` archived to a COMPLETE generation — an opening snapshot at
/// `rows` records, then a chain of sealed segments continuing it.
///
/// `write_records_at` for the second phase, never `write_records`: the second
/// call has to write FRESH ids so that the i-th committed record is the one
/// that writes `Rec { id: i }`. That coincidence is what makes "the source's
/// state at seq F" computable, and every test below asserts it holds rather
/// than assuming it.
fn archived_db_with_rows(rows: u64) -> (Db, SimObjectStore) {
    let (mut db, store, mut arc) = archive_fixture();
    write_records_emitting(&mut db, 0, rows);
    step_until_idle(&mut arc, &db, &store);
    write_records_emitting(&mut db, rows, RESTORE_CHAIN_RECORDS);
    step_until_idle(&mut arc, &db, &store);
    (db, store)
}

/// The newest recoverable point in the bucket: the chain's end when the
/// generation has one, its snapshot's frontier when it does not.
fn archived_frontier(store: &dyn ObjectStore) -> u64 {
    let m = read_manifest(store).expect("an archived generation");
    m.segments.last().map_or(m.applied_seq, |s| s.end_seq)
}

/// One table's raw rows as a restore comparison sees them.
type TableRows = (String, Vec<(u64, u64, Vec<u8>)>);

/// Every registered table's raw rows, paged. The comparison unit for "a
/// restored Db is logically equal to the source at the archived frontier"
/// (spec §9) — logical, not byte-identical.
fn logical_rows(db: &Db) -> Vec<TableRows> {
    logical_rows_checked(db).unwrap()
}

/// `logical_rows`, fallible. The walk is disk IO like any other, so under a
/// fault matrix an injected read error inside it is the matrix landing on
/// the CHECKER rather than a convergence violation — and a helper that
/// panics cannot be told apart from one. `logical_rows` is the unwrapping
/// entry point the fault-free tests use; there is one walker.
fn logical_rows_checked(db: &Db) -> io::Result<Vec<TableRows>> {
    let snap = db.snapshot()?;
    let mut out = Vec::new();
    for t in db.table_names() {
        let table_scoped = db.table_scoped(t)?;
        let mut rows: Vec<(u64, u64, Vec<u8>)> = Vec::new();
        let mut after = None;
        loop {
            let batch = snap.scan_raw_from(t, table_scoped, after, 1_000)?;
            let Some(last) = batch.last() else { break };
            after = Some((last.0, last.1));
            rows.extend(batch);
        }
        out.push((t.to_string(), rows));
    }
    Ok(out)
}

/// The source's logical state AT seq `f`. Exact rather than approximate
/// because of the fixture's one-record-per-row, `Rec { id: i }`-on-record-`i`
/// construction — which every caller pins with an explicit `last_applied`
/// assertion before relying on this.
fn source_at(db: &Db, f: u64) -> Vec<TableRows> {
    logical_rows(db)
        .into_iter()
        .map(|(name, rows)| {
            // `charpente.events` is filtered on the same rule and for the
            // same reason: the fixture emits exactly one event per record, so
            // event id i is the one record i materialized. That makes the
            // events table a REAL participant in this comparison rather than
            // an always-empty one — which is what catches a restore that
            // seeds `next_event_id` wrong, since a wrong counter re-issues
            // ids the snapshot already carries.
            if name == Rec::TABLE || name == "charpente.events" {
                (
                    name,
                    rows.into_iter().filter(|(_, id, _)| *id <= f).collect(),
                )
            } else {
                (name, rows)
            }
        })
        .collect()
}

/// A destination holding a DIFFERENT timeline at the same seqs: same schema,
/// same record count, different values, so every frame's checksum differs
/// from the archive's at the same seq.
///
/// `checkpoint_every: 0` so the log retains every frame — the comparison seq
/// is the archive's chain end, and a destination that had GC'd past it would
/// make this fixture test "cannot compare" instead of "diverged".
fn plant_diverged_timeline(sim: &Sim, records: u64) {
    let mut cfg = restore_cfg();
    cfg.checkpoint_every = 0;
    let mut db = Db::open(Box::new(sim.disk()), cfg, restore_schema()).unwrap();
    for i in 0..records {
        db.transact(|tx| -> Result<(), std::convert::Infallible> {
            tx.put(&Rec {
                id: Id::from_raw(i),
                // `+ 7`, not `i`: the frame payload at every seq differs from
                // the source's, which is what makes this diverged rather
                // than merely behind.
                value: i + 7,
            });
            Ok(())
        })
        .unwrap();
    }
}

fn put_object(store: &SimObjectStore, key: &str, data: Vec<u8>) {
    store
        .start(ObjectOp::Put {
            key: key.to_string(),
            data,
        })
        .unwrap();
    drain(store).unwrap();
}

fn delete_object(store: &SimObjectStore, key: &str) {
    store
        .start(ObjectOp::Delete {
            key: key.to_string(),
        })
        .unwrap();
    drain(store).unwrap();
}

/// Flips a byte inside the last chain segment's FIRST frame payload, leaving
/// the frame's stored CRC naming bytes that are no longer there.
fn corrupt_one_segment_object(store: &SimObjectStore) -> String {
    let m = read_manifest(store).expect("a manifest");
    let s = *m.segments.last().expect("a chain to corrupt");
    let key = segment_key(PREFIX, m.generation, s.base_seq);
    let mut bytes = get_object(store, &key).expect("the segment object");
    assert!(
        bytes.len() > 20,
        "the segment object is too small to hold a frame payload at offset 20: {}",
        bytes.len()
    );
    bytes[20] ^= 0xff;
    put_object(store, &key, bytes);
    key
}

fn delete_one_snapshot_batch(store: &SimObjectStore) -> String {
    let m = read_manifest(store).expect("a manifest");
    let meta = read_snapshot_meta(store, m.generation).expect("a snapshot meta");
    let t = meta
        .tables
        .iter()
        .find(|t| t.batches > 0)
        .expect("a table with at least one batch object");
    let key = snapshot_batch_key(PREFIX, m.generation, &t.name, 0);
    delete_object(store, &key);
    key
}

/// A bucket that dies after `cut` operations — the crash a restore has to
/// converge from. It OWNS its store (planted from the real one) because
/// `SimObjectStore` is not `Sync`, so a wrapper cannot borrow one and still
/// satisfy `ObjectStore`'s `Send`.
struct CutStore {
    inner: SimObjectStore,
    left: Cell<usize>,
    used: Cell<usize>,
}

impl CutStore {
    fn planted_from(source: &SimObjectStore, cut: usize) -> CutStore {
        let inner = SimObjectStore::new();
        plant(&inner, &store_contents(source));
        CutStore {
            inner,
            left: Cell::new(cut),
            used: Cell::new(0),
        }
    }

    /// Operations this store served — how many a whole restore takes, which
    /// is what the cut points below are derived from rather than guessed.
    fn used(&self) -> usize {
        self.used.get()
    }
}

impl ObjectStore for CutStore {
    fn start(&self, op: ObjectOp) -> io::Result<()> {
        if self.left.get() == 0 {
            return Err(io::Error::other(
                "simulated CutStore crash: the restore was cut off mid-run",
            ));
        }
        self.left.set(self.left.get() - 1);
        self.used.set(self.used.get() + 1);
        self.inner.start(op)
    }

    fn poll(&self) -> Option<io::Result<ObjectDone>> {
        self.inner.poll()
    }
}

#[job(global)]
struct Resurrect {
    slot: u64,
}

impl Resurrect {
    fn run(self, _tx: &mut charpente::Tx) -> Result<(), charpente::kernel::JobError> {
        Ok(())
    }
}

/// A source archived with `n` jobs still Pending in `charpente.jobs`.
fn archived_db_with_pending_jobs(n: u64) -> (Db, SimObjectStore) {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 200);
    for slot in 0..n {
        db.transact(|tx| -> Result<(), std::convert::Infallible> {
            tx.enqueue(Resurrect { slot });
            Ok(())
        })
        .unwrap();
    }
    step_until_idle(&mut arc, &db, &store);
    write_records_at(&mut db, 200 + n, RESTORE_CHAIN_RECORDS);
    step_until_idle(&mut arc, &db, &store);
    (db, store)
}

#[test]
fn restore_reproduces_the_source_at_the_archived_frontier() {
    let rows = 1_000;
    let (src, store) = archived_db_with_rows(rows);
    let total = rows + RESTORE_CHAIN_RECORDS;
    assert_eq!(
        src.last_applied(),
        Some(total - 1),
        "the fixture must commit exactly one record per row, or `source_at` is not the \
         source's state at a seq"
    );

    let f = archived_frontier(&store);
    assert!(
        f < total - 1,
        "the archived frontier must sit BELOW the source's — the live tail never ships \
         (spec §3) — or this test compares two identical Dbs and proves nothing: f={f}, \
         last_applied={:?}",
        src.last_applied()
    );

    let sim = fresh_sim();
    let report = restore(
        &store,
        Box::new(sim.disk()),
        restore_cfg(),
        restore_schema(),
        restore_opts(),
        &mut |_| {},
    )
    .unwrap();

    assert_eq!(report.applied_seq, f);
    assert!(
        report.segments >= 2,
        "the generation must have a multi-segment chain, or the chain loop is unexercised: \
         {report:?}"
    );

    let dst = Db::open(Box::new(sim.disk()), restore_cfg(), restore_schema()).unwrap();
    assert_eq!(dst.last_applied(), Some(f));
    assert_eq!(
        logical_rows(&dst),
        source_at(&src, f),
        "the restored Db is not logically equal to the source at the archived frontier"
    );
}

#[test]
fn restore_refuses_a_diverged_local_timeline_and_names_both_frontiers() {
    let (_src, store) = archived_db_with_rows(1_000);
    let local_records = 1_000 + RESTORE_CHAIN_RECORDS;
    let sim = fresh_sim();
    plant_diverged_timeline(&sim, local_records);
    let before = sim.disk().list("").unwrap();

    let e = restore(
        &store,
        Box::new(sim.disk()),
        restore_cfg(),
        restore_schema(),
        restore_opts(),
        &mut |m| panic!("a refusal must emit no operator notice, got: {m}"),
    )
    .unwrap_err();
    let t = e.to_string();

    assert!(t.contains("diverged"), "{t}");
    assert!(t.contains("--force"), "must name the fix: {t}");
    let f = archived_frontier(&store);
    assert!(
        t.contains(&f.to_string()),
        "must name the archive's frontier {f}: {t}"
    );
    assert!(
        t.contains(&(local_records - 1).to_string()),
        "must name the destination's frontier {}: {t}",
        local_records - 1
    );
    assert_eq!(
        sim.disk().list("").unwrap(),
        before,
        "a refusal must not have touched the destination"
    );
}

#[test]
fn force_discards_and_names_what_it_destroys_before_acting() {
    let (_src, store) = archived_db_with_rows(1_000);
    let local_records = 1_000 + RESTORE_CHAIN_RECORDS;
    let sim = fresh_sim();
    plant_diverged_timeline(&sim, local_records);
    let before = sim.disk().list("").unwrap();
    let seen: RefCell<Vec<String>> = RefCell::new(Vec::new());

    let report = restore(
        &store,
        Box::new(sim.disk()),
        restore_cfg(),
        restore_schema(),
        RestoreOpts {
            force: true,
            ..restore_opts()
        },
        &mut |m| {
            if m.contains("discard") {
                // THE ORDERING ORACLE. A divergence check that ran after
                // `Seeder::new` would find an emptied data directory here,
                // and naming what you destroyed after destroying it is not
                // what spec §6 requires.
                assert_eq!(
                    sim.disk().list("").unwrap(),
                    before,
                    "the destructive notice fired AFTER the destination was discarded: {m}"
                );
            }
            seen.borrow_mut().push(m.to_string());
        },
    )
    .unwrap();

    let notice = seen
        .borrow()
        .iter()
        .find(|m| m.contains("discard"))
        .cloned()
        .expect("--force must emit a destructive notice");
    assert!(notice.contains("diverged"), "{notice}");
    assert!(
        notice.contains(&(local_records - 1).to_string()),
        "the notice must name the frontier it destroys: {notice}"
    );
    assert!(report.applied_seq > 0);
    assert_eq!(report.applied_seq, archived_frontier(&store));
}

#[test]
fn a_corrupt_segment_object_is_refused_never_imported() {
    let (_src, store) = archived_db_with_rows(500);
    let key = corrupt_one_segment_object(&store);
    let sim = fresh_sim();

    let e = restore(
        &store,
        Box::new(sim.disk()),
        restore_cfg(),
        restore_schema(),
        restore_opts(),
        &mut |_| {},
    )
    .unwrap_err();
    let t = e.to_string();

    assert!(t.contains("CRC"), "{t}");
    assert!(t.contains(&key), "must name the object: {t}");
    assert!(
        sim.disk().list("").unwrap().is_empty(),
        "corruption was partially imported: {:?}",
        sim.disk().list("").unwrap()
    );
}

#[test]
fn a_missing_object_named_by_the_manifest_is_refused() {
    let (_src, store) = archived_db_with_rows(500);
    let key = delete_one_snapshot_batch(&store);
    let sim = fresh_sim();

    let e = restore(
        &store,
        Box::new(sim.disk()),
        restore_cfg(),
        restore_schema(),
        restore_opts(),
        &mut |_| {},
    )
    .unwrap_err();
    let t = e.to_string();

    assert!(t.contains("missing"), "{t}");
    assert!(t.contains(&key), "must name the object: {t}");
}

#[test]
fn a_crashed_restore_converges_on_re_run() {
    let (src, store) = archived_db_with_rows(500);
    let f = archived_frontier(&store);
    let sim = fresh_sim();

    // The plan named the cuts 3, 11 and 29. MEASURED against this fixture,
    // a whole restore takes 8 object operations, so two of those three sat
    // past the end and would have cut nothing while looking like coverage.
    // Derived from the measurement instead, on a throwaway destination so
    // the counting run does not itself seed the one under test.
    let ops = {
        let counter = CutStore::planted_from(&store, usize::MAX);
        let throwaway = fresh_sim();
        restore(
            &counter,
            Box::new(throwaway.disk()),
            restore_cfg(),
            restore_schema(),
            restore_opts(),
            &mut |_| {},
        )
        .unwrap();
        counter.used()
    };
    assert!(
        ops >= 8,
        "a whole restore takes {ops} object operations — too few to cut through the manifest \
         read, the verify pass, the row import and the chain apply"
    );
    let mut cuts: Vec<usize> = vec![1, ops / 4, ops / 2, (3 * ops) / 4, ops - 1];
    cuts.sort_unstable();
    cuts.dedup();
    for cut in cuts {
        let cut_store = CutStore::planted_from(&store, cut);
        let e = restore(
            &cut_store,
            Box::new(sim.disk()),
            restore_cfg(),
            restore_schema(),
            restore_opts(),
            &mut |_| {},
        );
        assert!(
            e.is_err(),
            "cut {cut} of {ops} did not actually cut the restore off"
        );
    }

    let report = restore(
        &store,
        Box::new(sim.disk()),
        restore_cfg(),
        restore_schema(),
        restore_opts(),
        &mut |_| {},
    )
    .unwrap();
    assert_eq!(report.applied_seq, f);

    let dst = Db::open(Box::new(sim.disk()), restore_cfg(), restore_schema()).unwrap();
    assert_eq!(logical_rows(&dst), source_at(&src, f));
}

#[test]
fn the_report_names_the_jobs_restore_is_about_to_resurrect() {
    let (_src, store) = archived_db_with_pending_jobs(7);
    let sim = fresh_sim();
    let seen: RefCell<Vec<String>> = RefCell::new(Vec::new());

    let report = restore(
        &store,
        Box::new(sim.disk()),
        restore_cfg(),
        restore_schema(),
        restore_opts(),
        &mut |m| seen.borrow_mut().push(m.to_string()),
    )
    .unwrap();

    assert_eq!(
        report.pending_jobs, 7,
        "operators must be told restore can re-fire effects (spec §6): {report:?}"
    );
    assert!(
        seen.borrow().iter().any(|m| m.contains("pending job")),
        "the consequence belongs in the command's own output too (spec §6): {:?}",
        seen.borrow()
    );
}

/// The reason `SnapshotMeta` carries a `frontier_crc` at all: a restored node
/// has to be able to PROVE its position to a replication peer
/// (`Db::seed_frontier`, `repl/follower.rs`'s Hello, `repl/session.rs`'s
/// vouch). The value it comes up with must be the one the SOURCE would have
/// computed for that same record — checked here against the source's own log,
/// not against the object restore read it out of.
#[test]
fn a_restored_node_can_vouch_the_frontier_its_snapshot_came_from() {
    let (src, store) = archived_db_with_rows(1_000);
    let m = read_manifest(&store).expect("a manifest");
    let sim = fresh_sim();
    restore(
        &store,
        Box::new(sim.disk()),
        restore_cfg(),
        restore_schema(),
        restore_opts(),
        &mut |_| {},
    )
    .unwrap();

    let frame = src
        .read_log(m.applied_seq, 1)
        .expect("the source still retains its snapshot's frontier frame")
        .into_iter()
        .next()
        .expect("read_log returns the frame it does not refuse");
    let expected = charpente_core::log::frame_crc(frame.0, &frame.1);
    assert_ne!(
        expected, 0,
        "a zero checksum would make this indistinguishable from an unset field"
    );

    let dst = Db::open(Box::new(sim.disk()), restore_cfg(), restore_schema()).unwrap();
    assert_eq!(
        dst.seed_frontier().unwrap(),
        Some((m.applied_seq, expected)),
        "a restored node cannot prove its position to a replication peer"
    );
}

// --------------------- T10 fix r1: what a failure says about the destination

/// Serves a key correctly on its FIRST `Get` and corrupted on every later
/// one — the only way to reach `verify_segment` from step 6, where the rows
/// have already been imported. Models an object that changed in the bucket
/// between restore's verify pass and its apply pass, which is reachable
/// whenever anything else is writing to the prefix.
struct SecondGetCorrupts {
    inner: SimObjectStore,
    needle: String,
    /// Keys already served once. PER KEY, not a running total: restore GETs
    /// every segment in the verify pass before it GETs any of them again in
    /// the apply pass, so a global counter corrupts the SECOND SEGMENT
    /// during the verify pass and never reaches the apply pass at all.
    /// Measured — that is exactly what the first version of this fixture did.
    served: RefCell<Vec<String>>,
    in_flight_get: RefCell<Option<String>>,
}

impl SecondGetCorrupts {
    fn planted_from(source: &SimObjectStore, needle: &str) -> SecondGetCorrupts {
        let inner = SimObjectStore::new();
        plant(&inner, &store_contents(source));
        SecondGetCorrupts {
            inner,
            needle: needle.to_string(),
            served: RefCell::new(Vec::new()),
            in_flight_get: RefCell::new(None),
        }
    }
}

impl ObjectStore for SecondGetCorrupts {
    fn start(&self, op: ObjectOp) -> io::Result<()> {
        *self.in_flight_get.borrow_mut() = match &op {
            ObjectOp::Get { key } if key.contains(&self.needle) => Some(key.clone()),
            _ => None,
        };
        self.inner.start(op)
    }

    fn poll(&self) -> Option<io::Result<ObjectDone>> {
        let watched = self.in_flight_get.borrow_mut().take();
        let done = self.inner.poll()?;
        match (watched, done) {
            (Some(key), Ok(ObjectDone::Get(mut bytes))) => {
                let mut served = self.served.borrow_mut();
                if served.contains(&key) {
                    bytes[20] ^= 0xff;
                } else {
                    served.push(key);
                }
                Some(Ok(ObjectDone::Get(bytes)))
            }
            (_, other) => Some(other),
        }
    }
}

/// A store that accepts every operation and never completes one — the shape
/// `ObjectStore::poll`'s `None` describes, and the shape a real network store
/// takes the moment a request is lost. `restore` must refuse by name rather
/// than spin forever.
struct StalledStore;

impl ObjectStore for StalledStore {
    fn start(&self, _op: ObjectOp) -> io::Result<()> {
        Ok(())
    }

    fn poll(&self) -> Option<io::Result<ObjectDone>> {
        None
    }
}

/// An archived generation with a snapshot and NO chain: 1,000 records is
/// under one 64 KiB segment, so nothing has sealed above the frontier and
/// `Manifest::segments` is empty. The bucket then holds no archived FRAME at
/// any seq, which is one of the two ways a destination can be unrelated.
fn archived_db_without_a_chain() -> (Db, SimObjectStore) {
    let (mut db, store, mut arc) = archive_fixture();
    write_records_emitting(&mut db, 0, 1_000);
    step_until_idle(&mut arc, &db, &store);
    (db, store)
}

fn run_restore(
    store: &dyn ObjectStore,
    sim: &Sim,
    opts: RestoreOpts,
) -> io::Result<charpente::archive::restore::RestoreReport> {
    restore(
        store,
        Box::new(sim.disk()),
        restore_cfg(),
        restore_schema(),
        opts,
        &mut |_| {},
    )
}

/// C-1. Spec §6 says a failed restore "says so in the failure's own text".
/// Every failure raised AFTER `Seeder::new` has discarded the destination
/// must therefore name what it left behind — because the state it leaves is
/// a database that opens and serves stale or empty state, and the operator's
/// next act is decided by this sentence.
///
/// Both post-discard sites, because the review measured both: a snapshot
/// batch missing during the row import, and a segment `Get` failing during
/// the chain apply.
#[test]
fn a_failure_after_the_destination_is_discarded_says_what_it_left_behind() {
    // (a) the row-import phase.
    let (_src, store) = archived_db_with_rows(500);
    delete_one_snapshot_batch(&store);
    let sim = fresh_sim();
    let t = run_restore(&store, &sim, restore_opts())
        .unwrap_err()
        .to_string();
    assert!(
        t.contains("PARTIAL restore"),
        "must name the state it left behind: {t}"
    );
    assert!(
        t.contains("Do not start") || t.contains("DO NOT START"),
        "must tell the operator not to start a node on it: {t}"
    );
    assert!(
        t.contains("Re-run restore") || t.contains("re-run restore"),
        "must name the remedy: {t}"
    );
    assert!(
        !t.contains("has been touched"),
        "the destination HAS been touched — this text promises the opposite: {t}"
    );
    // The claim is about a real hazard, not a hypothetical one.
    assert!(
        Db::open(Box::new(sim.disk()), restore_cfg(), restore_schema()).is_ok(),
        "the destination does not open, so the warning would be describing nothing"
    );

    // (b) the chain-apply phase. Cut the store after the row import, which
    // `a_crashed_restore_converges_on_re_run` measures at 11 operations for
    // this fixture, so a cut at 10 lands inside the apply pass.
    let sim = fresh_sim();
    let cut = CutStore::planted_from(&store, 10);
    let t = run_restore(&cut, &sim, restore_opts())
        .unwrap_err()
        .to_string();
    assert!(t.contains("PARTIAL restore"), "{t}");
    assert!(!t.contains("has been touched"), "{t}");
}

/// The other half of the same contract: a refusal raised BEFORE anything is
/// written must say so, and must not carry the partial-restore warning. The
/// two texts have to be distinguishable, because they ask the operator for
/// opposite things.
#[test]
fn a_refusal_before_any_write_promises_an_untouched_destination() {
    let (_src, store) = archived_db_with_rows(500);
    corrupt_one_segment_object(&store);
    let sim = fresh_sim();
    let t = run_restore(&store, &sim, restore_opts())
        .unwrap_err()
        .to_string();

    assert!(
        t.contains("Nothing in the destination has been touched"),
        "a pre-write refusal must promise an untouched destination: {t}"
    );
    assert!(
        !t.contains("PARTIAL restore"),
        "nothing was discarded, so this must not warn about a partial one: {t}"
    );
    assert!(
        sim.disk().list("").unwrap().is_empty(),
        "and the promise must be TRUE: {:?}",
        sim.disk().list("").unwrap()
    );
}

/// `verify_segment`'s string is shared between the pre-write verify pass and
/// the post-discard apply pass. At the second site "corruption is never
/// imported" is false — rows were imported minutes earlier. A message that
/// names a contract the code does not keep at that site is worse than
/// silence (hard rule 5).
#[test]
fn the_never_imported_claim_is_absent_where_rows_were_already_imported() {
    let (_src, store) = archived_db_with_rows(500);
    let flipping = SecondGetCorrupts::planted_from(&store, "/segments/");
    let sim = fresh_sim();

    let t = run_restore(&flipping, &sim, restore_opts())
        .unwrap_err()
        .to_string();

    assert!(
        t.contains("fails frame verification"),
        "this must be the segment-verification refusal, not some other failure: {t}"
    );
    assert!(
        !t.contains("never imported"),
        "rows were imported before this fired: {t}"
    );
    assert!(
        t.contains("PARTIAL restore"),
        "and it must say what it left behind: {t}"
    );
}

/// I-2. `Verdict::Ahead` is new operator-facing text with no oracle, and it
/// is the COMMON case for an in-place restore: the live tail never ships, so
/// a node's own dead data directory is always ahead of the archive.
#[test]
fn a_destination_ahead_of_the_archive_is_refused_and_names_what_it_would_lose() {
    let (_src, store) = archived_db_with_rows(1_000);
    let f = archived_frontier(&store);
    let sim = fresh_sim();
    run_restore(&store, &sim, restore_opts()).unwrap();

    // The restored node comes up and commits ten more records — the ordinary
    // shape of a node that ran for a while after a restore.
    let mut db = Db::open(Box::new(sim.disk()), restore_cfg(), restore_schema()).unwrap();
    write_records_at(&mut db, 90_000, 10);
    let local = db.last_applied().unwrap();
    drop(db);
    assert_eq!(local, f + 10);

    let t = run_restore(&store, &sim, restore_opts())
        .unwrap_err()
        .to_string();
    assert!(t.contains("ahead of the archive"), "{t}");
    assert!(t.contains("--force"), "must name the fix: {t}");
    assert!(
        t.contains(&local.to_string()),
        "must name both frontiers: {t}"
    );
    assert!(t.contains(&f.to_string()), "must name both frontiers: {t}");
    assert!(
        t.contains("10 record(s)"),
        "must name exactly what --force would destroy: {t}"
    );
}

/// I-2, the other refusal. A generation with no chain holds no archived frame
/// at any seq, so a destination that holds frames cannot be compared against
/// it in either direction — neither shown diverged nor shown to be this
/// timeline.
#[test]
fn a_destination_that_cannot_be_compared_is_refused_and_says_why() {
    let (_src, store) = archived_db_without_a_chain();
    assert!(
        read_manifest(&store)
            .expect("a manifest")
            .segments
            .is_empty(),
        "the fixture must produce a chainless generation, or this tests the wrong branch"
    );
    let sim = fresh_sim();
    plant_diverged_timeline(&sim, 50);

    let t = run_restore(&store, &sim, restore_opts())
        .unwrap_err()
        .to_string();
    assert!(t.contains("cannot compare itself against"), "{t}");
    assert!(t.contains("--force"), "must name the fix: {t}");
    assert!(
        t.contains("50 record(s)"),
        "must name exactly what --force would destroy: {t}"
    );
    assert!(
        t.contains("no chain yet"),
        "must say WHY there is nothing to compare against: {t}"
    );
    assert_eq!(
        run_restore(
            &store,
            &sim,
            RestoreOpts {
                force: true,
                ..restore_opts()
            }
        )
        .unwrap()
        .applied_seq,
        read_manifest(&store).expect("a manifest").applied_seq,
        "--force must still be able to restore a chainless generation"
    );
}

/// I-1. `ObjectStore::poll` returning `None` means "still in flight"; a store
/// that never completes is a broken store. The sim's own drain
/// (`charpente-sim/src/object.rs`) bounds its loop for exactly this reason,
/// and its comment says an unbounded one "would silently hang the instant a
/// future task adds latency". A restore that hangs under Task 12's fault
/// matrix fails with no seed and no repro line, which is the worst possible
/// hard-rule-5 outcome.
#[test]
fn a_store_that_never_completes_is_refused_rather_than_spun_on() {
    let sim = fresh_sim();
    let t = run_restore(&StalledStore, &sim, restore_opts())
        .unwrap_err()
        .to_string();
    assert!(
        t.contains("never completed"),
        "must name the liveness contract it gave up on: {t}"
    );
    assert!(
        t.contains(&manifest_key(PREFIX)),
        "must name the operation it was waiting for: {t}"
    );
}

// ------------- Task 11 / N-1: the refusal sentence, said exactly once

/// Overwrites the bucket's manifest with `m` — the shape a bucket takes when
/// the two objects describing one generation stop agreeing.
fn put_manifest(store: &SimObjectStore, m: &Manifest) {
    store
        .start(ObjectOp::Put {
            key: manifest_key(PREFIX),
            data: m.encode(),
        })
        .unwrap();
    drain(store).unwrap();
}

/// How many times the pre-write promise appears in one operator-facing
/// error. The answer is always 1: `untouched` is the only place it is said.
fn untouched_claims(text: &str) -> usize {
    text.to_lowercase()
        .matches("nothing in the destination has been touched")
        .count()
}

/// N-1. `restore.rs`'s wrapper comment claims to be "THE ONE PLACE EITHER
/// CLAIM IS MADE"; `check_generation` hardcoded the same promise at both of
/// its refusals, so real output carried it twice — measured verbatim:
///
/// > …Refusing; nothing in the destination has been touched. Nothing in the
/// > destination has been touched
///
/// True both times, which is why no test caught it: the defect is that an
/// operator reads the same sentence twice at the moment they are deciding
/// what to do. Pinned at both sites, because a wrapper that is the single
/// source of a claim is only single as long as nobody re-states it.
#[test]
fn a_generation_that_disagrees_with_itself_makes_the_untouched_claim_once() {
    let (_src, store) = archived_db_with_rows(500);
    let mut m = read_manifest(&store).expect("a manifest");
    m.applied_seq += 1; // the meta still holds the real frontier
    put_manifest(&store, &m);

    let sim = fresh_sim();
    let t = run_restore(&store, &sim, restore_opts())
        .unwrap_err()
        .to_string();
    assert!(
        t.contains("disagrees with itself"),
        "must name the contract: {t}"
    );
    assert!(
        t.contains(&snapshot_meta_key(PREFIX, m.generation)),
        "must name the site — the object that is not the snapshot named: {t}"
    );
    assert_eq!(
        untouched_claims(&t),
        1,
        "the pre-write promise must be made exactly once: {t}"
    );
}

#[test]
fn a_generation_with_a_hole_makes_the_untouched_claim_once() {
    let (_src, store) = archived_db_with_rows(500);
    let m = read_manifest(&store).expect("a manifest");
    let meta = read_snapshot_meta(&store, m.generation).expect("a snapshot meta");
    let mut m = m;
    assert!(
        !m.segments.is_empty(),
        "fixture must archive a chain, or this branch is unreachable"
    );
    m.segments[0].base_seq = meta.applied_seq + 5; // records the generation now holds nowhere
    put_manifest(&store, &m);

    let sim = fresh_sim();
    let t = run_restore(&store, &sim, restore_opts())
        .unwrap_err()
        .to_string();
    assert!(
        t.contains("has a hole and cannot be restored from"),
        "must name the contract: {t}"
    );
    assert!(
        t.contains("archive a fresh one from a live node"),
        "must name the fix: {t}"
    );
    assert_eq!(
        untouched_claims(&t),
        1,
        "the pre-write promise must be made exactly once: {t}"
    );
}

// ------------- Task 12.5: bucket reclamation — delete what a manifest names
//
// The archive's first `ObjectOp::Delete` in production code. Spec §3: when a
// generation rolls, the superseded generation's objects are deleted BY NAME
// out of documents this writer wrote — its own manifest for the chain, its
// own `snapshot/meta` for the batch objects. No LIST, and so no
// key-discovery surface at all.
//
// Every one of these tests runs against a `FlakyStore`, which runs
// `assert_deletable` on every DELETE it is asked for. That is deliberate and
// is where most of the strength is: the ordering hazard this task carries
// lives inside a window between two `step`s, and an end-state assertion
// cannot see a window.

/// Small enough that four crossings fit in a fast test, large enough that a
/// generation has a real snapshot to reclaim.
const RECLAIM_CADENCE: u64 = 2 * RECORDS_PER_SEGMENT;

/// Enough to seal a segment WITHOUT crossing the cadence, so a generation
/// has a chain under its snapshot and reclamation has segment objects to
/// delete as well as batch objects.
const RECLAIM_CHAIN: u64 = RECORDS_PER_SEGMENT + 10;

/// TASK 12.5. A cadence fixture over a `FlakyStore`, so a test can watch —
/// and refuse — the DELETEs a generation roll issues. `SimObjectStore` can do
/// neither: it has no delete hook, and its `Delete` arm cannot fail.
fn archive_fixture_with_delete_watch(snapshot_every: u64) -> (Db, FlakyStore, Archive) {
    let store = FlakyStore::new(SimObjectStore::new());
    build_fixture_with_cadence(store, snapshot_every, 8 << 20, PIN_SEGMENTS)
}

/// Commits `n` fresh records and runs the archive back to idle. The id
/// cursor is carried so every record has a distinct id and the seq <-> id
/// correspondence `source_at` rests on survives every phase.
fn reclaim_round(arc: &mut Archive, db: &mut Db, store: &FlakyStore, at: &mut u64, n: u64) {
    write_records_emitting(db, *at, n);
    *at += n;
    step_until_idle(arc, db, store);
}

/// Every key the bucket actually holds under one generation.
fn generation_keys(store: &FlakyStore, generation: u64) -> Vec<String> {
    let under = format!("{PREFIX}/{generation:020}/");
    store
        .keys()
        .into_iter()
        .filter(|k| k.starts_with(&under))
        .collect()
}

/// Every object the manifest currently in the bucket names is present.
/// "Objects may outlive their manifest, never the reverse" — spec §2's write
/// ordering read the other way round, and the one thing a reclamation defect
/// destroys.
fn assert_no_dangling_reference(store: &FlakyStore, why: &str) {
    let m = read_manifest(store).expect("a manifest");
    for key in expected_keys(store, &m) {
        assert!(
            get_object(store, &key).is_some(),
            "the manifest names generation {} and \"{key}\" is not in the bucket — the manifest \
             outlived an object it points at, so a restore from this bucket refuses. {why}",
            m.generation
        );
    }
}

/// Restores the bucket into a fresh host and asserts the result is logically
/// equal to `src` at the archived frontier — the flagship guarantee (spec
/// §9), used here as the end-to-end oracle that reclamation removed nothing a
/// restore needs.
fn assert_bucket_restores(store: &FlakyStore, src: &Db, why: &str) {
    let f = archived_frontier(store);
    let sim = fresh_sim();
    restore(
        store,
        Box::new(sim.disk()),
        restore_cfg(),
        restore_schema(),
        restore_opts(),
        &mut |_| {},
    )
    .unwrap_or_else(|e| panic!("restore from the reclaimed bucket failed: {e}. {why}"));
    let dst = Db::open(Box::new(sim.disk()), restore_cfg(), restore_schema()).unwrap();
    assert_eq!(dst.last_applied(), Some(f), "{why}");
    assert_eq!(
        logical_rows(&dst),
        source_at(src, f),
        "the restored Db is not logically equal to the source at the archived frontier. {why}"
    );
}

/// Steps until the roll's manifest has landed AND reclamation has started but
/// not finished — the window the whole ordering argument is about. Returns
/// how many deletes had been issued at that point.
fn step_into_a_partial_reclaim(arc: &mut Archive, db: &Db, store: &FlakyStore) -> usize {
    for _ in 0..10_000 {
        let _ = arc.step(db, store);
        let attempted = store.deleted().len();
        if attempted > 0 && !generation_keys(store, 0).is_empty() {
            // The archive may have left an op in flight, and nothing else
            // may touch the store until it is cleared (spec §1's assert).
            let _ = store.poll();
            return attempted;
        }
    }
    panic!("the archive never reached a partially-reclaimed generation 0 in 10,000 steps");
}

/// THE property (spec §3, review finding R-3): a generation the manifest no
/// longer names does not stay in the bucket forever. This is also the
/// vacuity check for everything else here — if deletion is removed entirely,
/// this is what must go red.
#[test]
fn a_rolled_generations_predecessor_objects_are_deleted_from_the_store() {
    let (mut db, store, mut arc) = archive_fixture_with_delete_watch(RECLAIM_CADENCE);
    let mut at = 0;
    reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CADENCE);
    reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CHAIN);
    let first = read_manifest(&store).expect("a manifest once the cadence was crossed");
    assert_eq!(
        first.generation, 0,
        "the opening generation is 0 (ruling 50)"
    );
    let retired = generation_keys(&store, 0);
    assert!(
        retired.len() > 2,
        "generation 0 left {} object(s); with nothing much in the bucket there is nothing much \
         to reclaim and this test proves little: {retired:?}",
        retired.len()
    );
    assert!(
        retired.iter().any(|k| k.contains("/segments/")),
        "generation 0 shipped no segment, so this test would only ever reclaim snapshot \
         objects: {retired:?}"
    );

    reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CADENCE);
    let live = read_manifest(&store).expect("a manifest after the roll");
    assert_eq!(
        live.generation, 1,
        "the cadence crossing did not roll the generation, so nothing was superseded"
    );

    assert_eq!(
        generation_keys(&store, 0),
        Vec::<String>::new(),
        "generation 1's manifest is in the bucket, so nothing names generation 0 and nothing \
         can ever reach it — but its objects are still there. That is spec §3's \
         unbounded-growth failure mode, which Task 12.5 exists to close"
    );
    assert_no_dangling_reference(&store, "The roll's own generation must be intact.");
}

/// NEGATIVE SPACE (hard rule 4, spec §8): the generation the manifest names
/// is never the one being deleted. `assert_deletable` enforces this inside
/// the store at the instant of every delete; this reads the same property off
/// the end state, over four rolls, so the two are independent.
#[test]
fn the_live_generations_objects_are_never_deleted() {
    let (mut db, store, mut arc) = archive_fixture_with_delete_watch(RECLAIM_CADENCE);
    let mut at = 0;
    for _ in 0..4 {
        reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CADENCE);
        reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CHAIN);
        assert_no_dangling_reference(&store, "Checked after every roll, not only at the end.");
    }
    let live = read_manifest(&store).expect("a manifest");
    assert!(
        live.generation >= 3,
        "four cadence crossings produced generation {} — with fewer rolls than that there is \
         little for reclamation to get wrong",
        live.generation
    );
    let deleted = store.deleted();
    assert!(
        !deleted.is_empty(),
        "no delete was issued at all across {} generations, so 'the live generation is never \
         deleted' is vacuously true",
        live.generation
    );
    let under_live = format!("{PREFIX}/{:020}/", live.generation);
    assert!(
        deleted.iter().all(|k| !k.starts_with(&under_live)),
        "a key of the LIVE generation {} was deleted: {:?}",
        live.generation,
        deleted
            .iter()
            .filter(|k| k.starts_with(&under_live))
            .collect::<Vec<_>>()
    );
    assert!(
        deleted
            .iter()
            .all(|k| key_generation(k).is_some_and(|g| g < live.generation)),
        "a delete named no generation below the live one: {deleted:?}"
    );
    assert_bucket_restores(
        &store,
        &db,
        "Reclamation removed something a restore needed.",
    );
}

/// SPEC §3: "reclamation is an optimisation and never a correctness
/// dependency: the archive stays correct when every delete fails forever."
/// Both failure arms, since they are different code paths — refused at
/// `start`, and refused at `poll` after a clean start — and in both the
/// object survives, which is what a bucket policy that denies deletion
/// actually does.
/// The non-vacuity witness itself, pinned — because the convergence property
/// CANNOT pin it. Measured: with the axis on and `is_err()` dropped from the
/// counter, `archive:delete_failed` goes from 32 universes to 174 and the
/// property stays GREEN. A `require:` key is a floor of one, so an
/// over-counting witness satisfies it just as well as an honest one, and the
/// coverage number then overstates what was exercised by 5x with nothing
/// red. So the counter's meaning lives here instead.
#[test]
fn the_delete_failure_witness_counts_failures_and_not_completions() {
    for rate in [0.0, 1.0] {
        let store = FlakyStore::new(SimObjectStore::with_faults(
            0x5eed,
            FaultConfig {
                object_delete_error: rate,
                ..FaultConfig::none()
            },
        ));
        // Sets the store's live generation to 2, so a DELETE under
        // generation 1 satisfies `assert_deletable`.
        write_foreign_manifest(&store, "w", 2);
        let key = format!("{PREFIX}/1/obj");
        store
            .start(ObjectOp::Put {
                key: key.clone(),
                data: vec![7],
            })
            .unwrap();
        drain(&store).expect("the put axis is off here");
        store.start(ObjectOp::Delete { key }).unwrap();
        let failed = drain(&store).is_err();

        assert_eq!(
            failed,
            rate == 1.0,
            "object_delete_error = {rate} did not decide the DELETE"
        );
        assert_eq!(
            store.delete_failures(),
            u64::from(rate == 1.0),
            "object_delete_error = {rate}: the witness must count DELETEs the inner store \
             FAILED, never ones it COMPLETED. `archive:delete_failed` is a `require:` key and \
             the convergence property's only defence against an axis that never fires; a \
             counter that also fires on success satisfies that requirement while proving \
             nothing, and the property cannot tell the difference."
        );
    }
}

#[test]
fn an_archive_whose_deletes_all_fail_stays_correct_and_keeps_advancing() {
    for mode in [DeleteMode::FailStart, DeleteMode::FailPoll] {
        let (mut db, store, mut arc) = archive_fixture_with_delete_watch(RECLAIM_CADENCE);
        store.set_deletes(mode);
        let mut at = 0;
        for _ in 0..4 {
            reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CADENCE);
            reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CHAIN);
        }
        let live = read_manifest(&store).expect("a manifest");
        assert!(
            live.generation >= 3,
            "{mode:?}: the archive reached only generation {} — it stopped making progress \
             because its deletes were failing, which makes reclamation a correctness dependency",
            live.generation
        );
        assert!(
            !arc.stopped(),
            "{mode:?}: a failed delete stopped the archive permanently. A delete that fails is \
             not an ownership fact and reaches no verdict; only spec §4's two refusals are \
             terminal"
        );
        assert!(
            !arc.gapped(),
            "{mode:?}: a failed delete cost the archive its continuity — a gap means records \
             fell out of the chain, and a delete cannot do that"
        );
        assert!(
            !store.deleted().is_empty(),
            "{mode:?}: no delete was ever attempted, so 'they all failed' is vacuous"
        );
        assert!(
            !generation_keys(&store, 0).is_empty(),
            "{mode:?}: generation 0's objects are gone although every delete was refused — the \
             fixture is not injecting the fault it claims to"
        );
        assert_no_dangling_reference(&store, "A bucket nothing reclaims is still a valid bucket.");
        assert_bucket_restores(
            &store,
            &db,
            "An archive whose deletes all fail must still produce a restorable bucket.",
        );
    }
}

/// THE ORDERING, which is the whole safety argument (spec §2/§3): deletion
/// FOLLOWS the manifest swap. A crash between the two leaves objects that no
/// manifest names — harmless — and never a manifest naming objects that are
/// gone, which is not recoverable at all.
///
/// The crash is a real one: the `Archive` is dropped mid-drain and nothing
/// resumes it, which is what a process death leaves behind.
#[test]
fn a_crash_between_the_manifest_swap_and_the_delete_leaves_no_dangling_reference() {
    let (mut db, store, mut arc) = archive_fixture_with_delete_watch(RECLAIM_CADENCE);
    let mut at = 0;
    reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CADENCE);
    reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CHAIN);
    write_records_emitting(&mut db, at, RECLAIM_CADENCE);

    let attempted = step_into_a_partial_reclaim(&mut arc, &db, &store);
    drop(arc);

    // THE INVARIANT FIRST, before the anti-vacuity guards below. It has to
    // hold at this crash point whatever the crash point turned out to be,
    // and if the ordering is wrong it is the sentence that should be read.
    assert_no_dangling_reference(
        &store,
        "A crash between the manifest swap and the delete must leave the manifest's generation \
         whole.",
    );
    let live = read_manifest(&store).expect("the manifest the roll swapped in");
    assert_eq!(
        live.generation, 1,
        "the crash point must be AFTER the roll's manifest landed, or this test does not \
         straddle the window it exists for"
    );
    assert!(
        attempted > 0,
        "no delete had been issued at the crash point, so this is an ordinary post-swap crash \
         and not the window between the swap and the delete"
    );
    assert!(
        !generation_keys(&store, 0).is_empty(),
        "generation 0 was fully reclaimed before the crash point, so nothing here is partial"
    );
    assert_bucket_restores(
        &store,
        &db,
        "The bucket a mid-reclaim crash leaves behind must still restore.",
    );
}

/// SPEC §7/§8: "an archive that cannot establish ownership of a prefix never
/// writes to it" — and a DELETE is a write. The manifest a swap is about to
/// replace is the only evidence of who owns the prefix, so a foreign one
/// stops the archive before a single key of that generation is deleted.
#[test]
fn no_delete_is_issued_against_a_manifest_this_writer_did_not_write() {
    let (mut db, store, mut arc) = archive_fixture_with_delete_watch(RECLAIM_CADENCE);
    let mut at = 0;
    reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CADENCE);
    reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CHAIN);
    assert_eq!(
        read_manifest(&store).expect("a manifest").generation,
        0,
        "phase 1 must leave a generation for a peer to take over"
    );
    let owned = generation_keys(&store, 0);
    assert!(
        owned.len() > 2,
        "generation 0 left {} object(s), so there is little for a mistaken reclaim to destroy",
        owned.len()
    );

    // A peer is promoted and takes the prefix over at the generation this
    // archive is about to retire.
    write_foreign_manifest(&store, FOREIGN_WRITER, 0);
    write_records_emitting(&mut db, at, RECLAIM_CADENCE);
    for _ in 0..10_000 {
        let _ = arc.step(&db, &store);
        if arc.stopped() {
            break;
        }
    }
    let _ = store.poll();

    assert!(
        arc.stopped(),
        "the archive never refused the foreign manifest, so this test never reaches the \
         question it asks"
    );
    assert_eq!(
        store.deleted(),
        Vec::<String>::new(),
        "the archive deleted objects while the manifest named another writer. Ownership is \
         decided before anything is WRITTEN to a prefix (spec §8) and a DELETE is a write — \
         these keys belong to a bucket this node has just been told it does not own"
    );
    assert_eq!(
        generation_keys(&store, 0),
        owned,
        "generation 0's objects changed under a refusal that must not touch the bucket"
    );
}

/// A gap can land while the archive is reclaiming, and the gap path discards
/// whatever the archive was doing (`yield_or_bound`). `ObjectStore::start`
/// asserts against a second op while one is in flight (spec §1), so a
/// reclaim state missing from `op_in_flight` makes that discard abandon a
/// live GET or DELETE and the next `start` panics.
///
/// Both parities: the reclaim states alternate between "an op is in flight"
/// and "nothing is started", and only one of them is the hazard.
#[test]
fn a_gap_that_lands_mid_reclaim_never_abandons_an_operation_in_flight() {
    for extra in [0usize, 1] {
        let (mut db, store, mut arc) = build_fixture_with_cadence(
            FlakyStore::new(SimObjectStore::new()),
            RECLAIM_CADENCE,
            8 << 20,
            GAP_PIN_SEGMENTS,
        );
        let mut at = 0;
        reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CADENCE);
        reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CHAIN);
        write_records_emitting(&mut db, at, RECLAIM_CADENCE);
        at += RECLAIM_CADENCE;
        step_into_a_partial_reclaim(&mut arc, &db, &store);
        for _ in 0..extra {
            let _ = arc.step(&db, &store);
        }

        // Past the bound while the reclaim is still draining.
        write_records_emitting(
            &mut db,
            at,
            (GAP_PIN_SEGMENTS as u64 + 4) * RECORDS_PER_SEGMENT,
        );
        step_until_idle(&mut arc, &db, &store);

        assert!(
            read_manifest(&store).is_some(),
            "extra={extra}: the bucket lost its manifest across a gap that landed mid-reclaim"
        );
        assert_no_dangling_reference(
            &store,
            "A gap that interrupts a reclaim must leave the manifest's own objects intact.",
        );
    }
}
// ------------- Task 12.5 fix round 1: the two majors, driven from CONTENT
//
// Both of these reach reclamation with bytes that crossed the network, and
// both were red before the fix in this commit. Fixture seed
// `0x6172_6368_5f74_3500` (`build_fixture_with_cadence`), faults off: the
// input is bucket CONTENT, not a fault, so the axis that would inject it
// does not exist and could not.

/// F1 (task-12.5-review.md). A manifest naming THIS writer at a generation
/// BELOW the archive's own, met at a within-generation SEGMENT swap
/// (`commits: None`). That is content — an operator rolling the manifest
/// object back during recovery, or a peer sharing `ArchiveConfig::writer` —
/// and before the fix it fired the `opened_generation` assert and PANICKED
/// the process out of the kernel arm that exists to absorb every archive
/// error.
///
/// The archive must retire NOTHING here: the dangerous act is the DELETE,
/// the swap itself was harmless before Task 12.5, and a permanent
/// `stop()` would punish a legitimate manifest rollback during recovery.
/// One generation's bucket space is the documented residual (spec §3).
#[test]
fn a_lower_generation_manifest_at_a_segment_swap_retires_nothing_and_never_panics() {
    let (mut db, store, mut arc) = archive_fixture_with_delete_watch(RECLAIM_CADENCE);
    let mut at = 0;
    reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CADENCE);
    reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CHAIN);
    reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CADENCE);
    assert_eq!(
        read_manifest(&store).expect("a manifest").generation,
        1,
        "the cadence crossing must have rolled the generation, or the swap below is the \
         opening snapshot's and carries `commits: Some`"
    );
    let deleted_before = store.deleted().len();
    assert!(
        deleted_before > 0,
        "generation 0 was never reclaimed, so this test is not measuring a delete ledger \
         that reclamation actually writes to"
    );
    let live_before = generation_keys(&store, 1);

    // The manifest object is rolled back to one this writer wrote at a
    // LOWER generation. Nothing about it is malformed and nothing about it
    // is foreign: it is this archive's own identity at generation 0.
    write_foreign_manifest(&store, WRITER, 0);

    // A segment swap, NOT a cadence crossing: `commits` is `None`, which is
    // the parity the `opened_generation` assert was about.
    reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CHAIN);

    assert!(
        !arc.stopped(),
        "the archive stopped permanently over a manifest rollback. The swap is harmless — it \
         was harmless before Task 12.5 — and a terminal refusal is the worst answer available \
         during a recovery"
    );
    assert_eq!(
        store.deleted().len(),
        deleted_before,
        "a segment swap issued {} new DELETE(s) after the manifest was rolled back to \
         generation 0. A swap that commits no snapshot opens no successor, so it retires \
         nothing (spec §3)",
        store.deleted().len() - deleted_before
    );
    assert_eq!(
        read_manifest(&store).expect("a manifest").generation,
        1,
        "the swap itself must still land — the fix removes the DELETE, not the swap"
    );
    // Not equality: the swap SHIPS a segment, so the live generation gains
    // an object. What must not happen is losing one.
    let live_after = generation_keys(&store, 1);
    for key in &live_before {
        assert!(
            live_after.contains(key),
            "the live generation lost \"{key}\" to a swap that should have retired nothing"
        );
    }
    assert!(
        live_after.len() > live_before.len(),
        "the segment swap shipped nothing, so this test never reached a `commits: None` swap"
    );
    assert_no_dangling_reference(
        &store,
        "A manifest rollback must not cost the live generation an object.",
    );
}

/// The value `Archive::reclaim_bound_notice` prints, restated here so that
/// moving the constant without re-reading the notice fails a test rather
/// than passing one: `MANIFEST_MAX_BYTES` (16 MiB) / 16 bytes per
/// `SegmentRef`. The archive's copy is private, and duplicating the NUMBER
/// is the point — a test that recomputed it from the same expression could
/// not notice the expression changing.
const RECLAIM_BOUND_IN_NOTICE: &str = "1048576";

/// F2 (task-12.5-review.md). `SnapshotMeta::batches` is a `u64` read back out
/// of the bucket and `Retired::next_key` emits one key per batch, one DELETE
/// per step. `yield_or_bound` does not bound it: its bound counts SEALED
/// SEGMENTS above `next_needed`, so with the `Db` quiescent nothing seals,
/// `behind` never grows, and at `u64::MAX` the archive never returns to
/// `Idle` again — never re-holding its pin at the new floor, so `Wal::gc`
/// stays blocked, one real bucket DELETE per kernel step, silently
/// (`ObjectStore::delete` is idempotent, so every one of them succeeds).
///
/// Before the fix this test never reached `arc.idle()`.
#[test]
fn a_retired_metas_absurd_batch_count_is_refused_rather_than_enumerated() {
    let (mut db, store, mut arc) = archive_fixture_with_delete_watch(RECLAIM_CADENCE);
    let mut at = 0;
    reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CADENCE);
    reclaim_round(&mut arc, &mut db, &store, &mut at, RECLAIM_CHAIN);
    let doomed = read_manifest(&store).expect("a manifest");
    assert_eq!(doomed.generation, 0, "the opening generation is 0");
    let real_meta = get_object(&store, &snapshot_meta_key(PREFIX, 0)).expect("a snapshot meta");
    let real = SnapshotMeta::decode(&real_meta).expect("the archive's own meta decodes");
    let batch_keys = generation_keys(&store, 0)
        .into_iter()
        .filter(|k| k.contains("/snapshot/") && !k.ends_with("/meta"))
        .count();
    assert!(
        batch_keys > 0,
        "generation 0 has no snapshot batch objects, so an absurd count has nothing to lie about"
    );

    // A CRC-valid meta for the generation about to be retired, claiming the
    // largest batch count the field can hold. Everything else is the
    // archive's own.
    // The fold in `step_reading_retired_meta` uses `saturating_add`, and
    // that choice is load-bearing rather than defensive: crafted so the
    // per-table counts SATURATE to `u64::MAX` and WRAP to 0. Under
    // `wrapping_add` the claim reads as zero, passes the bound, and the
    // delete loop this whole test exists to catch re-opens silently.
    assert!(
        real.tables.len() >= 2,
        "this test crafts a claim that wraps to zero across two tables; with {} table(s) it \
         cannot tell `saturating_add` from `wrapping_add` and has stopped pinning the fold",
        real.tables.len()
    );
    let absurd = SnapshotMeta {
        applied_seq: real.applied_seq,
        next_job_id: real.next_job_id,
        next_event_id: real.next_event_id,
        frontier_crc: real.frontier_crc,
        tables: real
            .tables
            .iter()
            .enumerate()
            .map(|(i, t)| TableBatches {
                name: t.name.clone(),
                batches: match i {
                    0 => u64::MAX,
                    1 => 1,
                    _ => 0,
                },
            })
            .collect(),
    };
    store
        .start(ObjectOp::Put {
            key: snapshot_meta_key(PREFIX, 0),
            data: absurd.encode(),
        })
        .unwrap();
    drain(&store).unwrap();

    // Roll, and collect every error the drain raises on the way.
    write_records_emitting(&mut db, at, RECLAIM_CADENCE);
    let mut errors = Vec::new();
    for _ in 0..10_000 {
        if let Err(e) = arc.step(&db, &store) {
            errors.push(e.to_string());
        }
        if arc.idle() {
            break;
        }
    }

    assert!(
        arc.idle(),
        "the archive never returned to Idle in 10,000 steps: an absurd batch count is being \
         ENUMERATED, one DELETE per step, and until it finishes the pin is never re-held at \
         the new floor and Wal::gc stays blocked"
    );
    assert_eq!(
        read_manifest(&store).expect("a manifest").generation,
        1,
        "the roll must still complete — an unreadable retired generation is not a reason to \
         stop archiving"
    );
    assert!(
        !arc.stopped(),
        "reclamation is an optimisation; a meta it will not enumerate must not stop the archive"
    );

    // HARD RULE 5: the notice names the contract, the site and the fix.
    let notice = errors
        .iter()
        .find(|e| e.contains("reclamation of generation"))
        .unwrap_or_else(|| {
            panic!(
                "reclamation refused an absurd batch count SILENTLY. The errors raised were: \
                 {errors:?}"
            )
        });
    let meta_key = snapshot_meta_key(PREFIX, 0);
    for needle in [
        RECLAIM_BOUND_IN_NOTICE,
        PREFIX,
        meta_key.as_str(),
        "ArchiveConfig::writer",
    ] {
        assert!(
            notice.contains(needle),
            "the notice must name {needle:?}: {notice}"
        );
    }

    // The `\<newline>` idiom every sibling notice in `archive/mod.rs` uses
    // collapses to a single space; a literal written without it keeps the
    // next line's indentation INSIDE the message. `cargo fmt` cannot see
    // into a string literal and the needles above match either way, so this
    // is the only thing standing between an operator and a notice with
    // fourteen spaces in the middle of a sentence (hard rule 5).
    assert!(
        !notice.contains("  "),
        "the notice carries a run of spaces from an un-continued string literal: {notice}"
    );

    // The chain is still reclaimed; the snapshot side is left, which is what
    // spec §7 already says a retired generation whose meta will not answer
    // costs (the batch objects stay, because the meta is the only document
    // that names them).
    let left = generation_keys(&store, 0);
    assert!(
        !left.iter().any(|k| k.contains("/segments/")),
        "generation 0's chain is named by its MANIFEST, which decoded fine, so it must still \
         be reclaimed: {left:?}"
    );
    assert!(
        left.iter().any(|k| k.contains("/snapshot/")),
        "the batch objects the archive refused to enumerate should have been LEFT, not deleted"
    );
    assert_no_dangling_reference(
        &store,
        "A refused retired meta must not cost the live generation an object.",
    );
    assert_bucket_restores(
        &store,
        &db,
        "The live generation must still restore after a retired meta was refused.",
    );
}

// ------------------------------- Task 11: the filesystem `ObjectStore`
//
// The same contract `charpente-sim/src/object.rs`'s test module pins for
// `SimObjectStore`, run against the FIRST non-simulator `ObjectStore` in the
// tree — plus the two properties only a real filesystem can have: a PUT that
// fails between the temp write and the rename, and the temp file a crash in
// that window leaves behind.
//
// Module-wide deny-list opt-out, same rationale as `prod.rs`'s: these tests
// exercise the production filesystem impl, and setting up a failed rename or
// planting crash residue means touching the filesystem directly. Nothing
// here is inside the deterministic boundary.
// ==================== Task 12: the convergence property ====================
//
// THE flagship oracle (spec §9): a `Db` restored from the archive is
// logically equal to the source at the archived frontier, over N seeded
// universes with faults. Everything Tasks 1-11 built is machinery; this is
// the evidence, and Task 12.5's delete path is gated on it — deletion must
// not ship before the property proving what it deletes is recoverable.
//
// **The oracle's expectation is the WORKLOAD's, never the archive's.** The
// whole hazard of a convergence property is that it is a self-comparison: it
// restores a `Db` from the archive and compares it to the source, and if
// both sides are read through the same snapshot walk / segment decode /
// materialisation, a mutation in that shared path moves both sides equally
// and the equality assertion is structurally incapable of reddening
// (task-12-hazard-note.md; Task 7.5's I-1 was the same failure one scale
// down). So the universe keeps its OWN record of what it committed —
// `model[i]` is the WAL seq of the record that wrote `Rec { id: i, value: i
// }`, emitted `Ping { n: i }` and, when `jobs[i]`, enqueued one job — built
// from what `Db::transact` returned and from nothing else. Every anchor
// below is a range fixed by the fixture before the archive existed. The
// structural comparison against the source runs LAST, after the anchors,
// and only ever adds to them.
//
// **What is faulted and what is not.** The object store carries the faults
// this property is about — that is where every one of spec §9's eleven axes
// lives — plus the disk fault matrix the swarm samples, which reaches the
// source `Db`. What the universe never does is `Sim::restart` the SOURCE: a
// power cycle cuts the WAL's unsynced tail, and a cut re-frees seqs the
// model has already spent, so the seq↔id correspondence that makes the
// expectation independent would have to be repaired by reading
// `Db::last_applied` back — i.e. by asking the code under test what the
// answer is. The source's own crash recovery is `charpente-sim`'s
// `wal.rs`/`crash_recovery.rs` property over 512 universes each, and is not
// re-litigated here. The archive's OWN restart path (`State::Opening`'s
// resume-or-open, ruling 43) is reached instead by dropping the `Archive`
// and opening a fresh one over the same `Db` and bucket, which is the same
// transition without touching the log.

use charpente::TxError;
use charpente_core::io::Entropy;
use charpente_sim::checks::is_fault_fallout;
use charpente_sim::run_universes_swarm_with;

/// Segment size for the convergence universes. The file's other fixtures use
/// 64 KiB, which seals a segment every `RECORDS_PER_SEGMENT` = 1,365
/// commits. A universe has to seal SEVERAL segments before a generation has
/// a chain at all, and the commits it takes to get there are not free in two
/// different ways: they are redb write transactions spent on nothing the
/// property can see, and — the reason this is 1 KiB rather than merely
/// small — every one of them is another roll of `write_error`/`sync_error`
/// against a `Db` those axes can kill outright. MEASURED at 4 KiB first: 129
/// of 256 universes lost their source permanently, because the number of
/// commits the shape needed was larger than the number the matrix allowed.
const CONV_SEGMENT_BYTES: u64 = 1 << 10;

/// MEASURED, not assumed — `conv_segment_layout_is_what_the_budget_assumes`
/// is the pin, and it asserts against a freshly built fixture rather than
/// against a transcription of this number. Every budget below that is
/// expressed in segments (the pin bound, the outage length) is computed from
/// it, so if the frame size moves the pin fails rather than the property
/// quietly stopping to seal anything.
///
/// Re-measured 2026-09-24: payload shapes grew enqueue/emit records by 8
/// bytes. A conv commit (one `Rec` put, one `Ping` emit) went from a 70-byte
/// frame (16 header + 54 payload) to 78, so a 1 KiB segment holds 13, not 14
/// (13 × 78 = 1014, 14 × 78 = 1092; 14 × 70 = 980, 15 × 70 = 1050).
const CONV_RECORDS_PER_SEGMENT: u64 = 13;

/// Rounds per universe, and the writes and archive steps each round gets.
/// Sized so a universe reaches a chain of several segments AND crosses a
/// snapshot cadence: `CONV_ROUNDS * CONV_WRITES_PER_ROUND / 2` averages ~700
/// records, ~8 segments.
const CONV_ROUNDS: u64 = 10;
const CONV_WRITES_PER_ROUND: u64 = 60;
const CONV_STEPS_PER_ROUND: u64 = 600;

/// One archive step per this many records inside the write loop. The archive
/// reads the log through the `Db`, so a source the matrix kills mid-round
/// takes the archive with it — an archive that had not stepped yet has
/// published nothing at all, and the oracle has nothing to read.
const CONV_WRITES_PER_STEP: u64 = 4;

/// Steps taken at each of those points, or until the archive is caught up —
/// DRAWN PER UNIVERSE, because there is no single right answer and the two
/// ends buy opposite things.
///
/// Opening a generation takes one step per snapshot batch plus the meta plus
/// the manifest swap, so an archive stepped once per few records never
/// finishes its first generation before the matrix kills the source:
/// measured at 1, 108 of 256 universes published nothing at all. But an
/// archive that keeps up perfectly is an archive with nothing outstanding,
/// and the fault axes this property exists to reach are all about what
/// happens to work in flight: measured at 24, `archive:lost_ack_retried`
/// fell from 85 to 31, `archive:gap_closed_generation` from 301 to 125, and
/// the write-gate oracle — which only counts a window where the archive HAD
/// work — from 169 to 20. So the universes draw a lag instead of picking
/// one, the same way they draw `snapshot_every` and `pin_segments`.
const CONV_INTERLEAVE_CHOICES: [u64; 4] = [1, 2, 8, 24];

/// Steps taken against a write-gated archive. A FIXED count, never
/// step-until-idle: `Archive::idle`'s own doc records that a gated archive
/// answers `true` immediately, so a loop keyed on `idle` exits after one
/// step and proves nothing.
const CONV_GATED_STEPS: u64 = 60;

/// The quiesce budget before the oracle runs, and the budget for the
/// foreign-writer probe after it.
const CONV_FINAL_STEPS: u64 = 8_000;
const CONV_FOREIGN_STEPS: u64 = 4_000;

/// Attempts at opening the source `Db`, and source restarts allowed per
/// universe.
///
/// A `Db` whose WAL poisoned is a DEAD NODE: it accepts no further commits,
/// and — because the archive reads the log through it — the archive ships
/// nothing more either. Measured before this existed: 92 of 256 universes
/// published no manifest at all and **every one of them had `poisoned ==
/// true`**, so the fault axis that stopped the property reaching its own
/// oracle was the source's disk, which is not what this property is about.
/// The fix is the universe generator, not the key (the brief's step 2): a
/// real node restarts, so this one does.
/// A LIVENESS BOUND, not a tuning constant — the same shape as
/// `restore.rs`'s `POLLS_PER_OP`. `write_error` and `sync_error` are
/// per-operation probabilities and `Db::open` is many operations, so at the
/// top of the swarm's range a single attempt succeeds a few percent of the
/// time; a disk that refuses this many consecutive opens is a disk that will
/// not open, and a universe with no node in it is not a verdict. Measured at
/// 16 first: 129 of 256 universes gave up here, which made the SOURCE's disk
/// the property's dominant reason for reaching no verdict — the failure mode
/// this task is most exposed to.
const CONV_OPEN_ATTEMPTS: u32 = 16;
const CONV_MAX_RESTARTS: u32 = 64;

/// Restore attempts into the SAME destination before the universe gives up.
/// A failed `restore` leaves a discarded-and-partially-rebuilt destination
/// and does NOT clean it up — Task 11 put the destination cleanup in the CLI
/// verb, and a direct caller owns the `Disk` it passed. So the remedy this
/// property exercises is the one `restore`'s own failure text names: re-run
/// against the same directory, which converges because `Seeder::new`
/// discards rather than resumes. 32 until 2026-10-01; 64 restores one more
/// universe at the unscoped base for no measurable CPU (`CONV_RESTORED_FLOOR`).
const CONV_RESTORE_ATTEMPTS: u32 = 64;

/// Attempts at reading the verdict off a destination that is already
/// restored. Separate from `CONV_RESTORE_ATTEMPTS` because it answers a
/// different question: not "did restore complete" but "did the CHECKER get a
/// clean read", and the fault it retries is `read_error`, which the axis's
/// own doc calls transient. 32 until 2026-10-01; 64 verifies 2 and 4 more
/// universes at the two bases for no measurable CPU (`CONV_RESTORED_FLOOR`).
const CONV_VERIFY_ATTEMPTS: u32 = 64;

/// The base seed `archive_converges` runs from. Explicit, like
/// `replication.rs`'s `0x2EB1_*` family.
const ARCHIVE_CONVERGENCE_BASE: u64 = 0xA2C0_0001;

/// `write_error` and `sync_error` are drawn per DISK OPERATION, from a range
/// calibrated against universes that commit tens of records —
/// `crash_recovery.rs`'s four rounds of at most twelve appends,
/// `replication.rs`'s forty commits. This one commits several hundred,
/// because a generation needs a snapshot AND a chain of sealed segments
/// above it before there is anything to converge, so at the same per-op rate
/// its per-universe exposure is nearly an order of magnitude higher. Divided
/// by the length ratio, it is back in the range the numbers were drawn for.
///
/// This is not a knob that makes a red property green — it decides how many
/// universes have a NODE in them at all, and MEASURED at 1 the source's own
/// disk was the property's dominant reason for reaching no verdict:
///
/// | divisor | reached the oracle | published nothing | source restarts |
/// |---|---|---|---|
/// | 1 | 130 / 256 | 97 | 1,839 |
/// | 4 | 177 / 256 | 64 | 1,033 |
/// | 8 | 197 / 256 | 38 | 1,000 |
/// | 16 | 206 / 256 | 28 | 764 |
///
/// The length ratio argues for about 8 and the measurement puts the knee
/// there too; 16 is the next step and buys the last nine verdicts without
/// collapsing the path the axis exists to drive — three source restarts per
/// universe, still. Past it there is nothing left to buy: what remains is
/// universes whose source is destroyed outright, and those are answered by
/// running the oracle WITHOUT a source
/// (`archive:compared_without_source`) rather than by turning the axis down
/// until they stop happening. **The axis stays ON**, which is the difference
/// between scaling a fault and disabling one.
const CONV_DISK_ERROR_DIVISOR: f64 = 16.0;

/// Measured rates over the 256 universes at `ARCHIVE_CONVERGENCE_BASE`, read
/// off a batch reproduced identically twice (task-12-report.md §4). The other
/// 40 universes reach no verdict and say which kind: 28 never published a
/// generation at all (`archive:nothing_published`) and 12 never got a clean
/// read of their own restored destination
/// (`archive:verify_never_read_cleanly`). Those two plus
/// `archive:restored` account for all 256, which is the arithmetic that
/// makes "no verdict" a measured quantity here rather than an unknown one.
///
/// `require:` has a floor of one, which catches an oracle that stopped firing
/// entirely and misses one that decayed to a token handful. These are the
/// same check at the measured rate, and each names a different way this
/// property can go quietly decorative:
///
/// - `archive:restored` — universes that reached the oracle AT ALL. The
///   number the whole task turns on.
/// - `archive:restored_substantial` — universes whose restored `Db` was
///   `CONV_SUBSTANTIAL_ROWS` rows or larger. A property that only ever
///   compares eleven-row databases is not comparing databases.
/// - `archive:chained` — universes whose generation had a SEGMENT CHAIN, not
///   a snapshot alone. Without this the segment path — the half of the
///   archive Tasks 5 and 7.5 are about — could stop being exercised without
///   a single key going to zero.
///
/// **A drop is a finding, not a number to lower** (the wording is
/// `replication.rs`'s `FINGERPRINT_FLOOR` and the rule is the same one):
/// it means the universes reach that state less often than when the numbers
/// were taken, and why is the finding.
///
/// **These are rates against ONE base, and fresh bases DO trip them. That is
/// the design, not a regression.** Measured over 1,024 universes at four bases:
///
/// | base | `restored` | `substantial` | `chained` | `delete_failed` | `restored_after_delete_failure` |
/// |---|---|---|---|---|---|
/// | `0xA2C0_0001` (pinned) | 219 | 183 | 169 | 32 | 32 |
/// | `0xD9E3_0001` | 216 | 178 | 177 | 23 | 21 |
/// | `0x5B71_0001` | 219 | 178 | 161 | 23 | 23 |
/// | `0x3C0D_0001` | 223 | 170 | 168 | 32 | 32 |
///
/// **Re-measured again when `object_delete_error` was turned on, and again
/// the reason they moved is not decay.** The axis is drawn by a fourth
/// `conv_rate(&entropy, ..)` call, and `conv_rate` draws from `entropy`, so
/// adding it shifted `snapshot_every`, `pin_segments` and `interleave` in
/// every universe: these are different runs, not the same runs performing
/// worse. The tell is that the movement is in BOTH directions at fresh bases
/// — `chained` at `0xD9E3_0001` went 162 -> 177 while at `0x5B71_0001` it
/// went 168 -> 161 — which a decaying rate cannot do. At the PINNED base no
/// floor fell: `restored` 215 -> 219, `substantial` and `chained` unchanged,
/// so the floors below were raised to the new pinned measurement rather than
/// lowered to accommodate anything.
///
/// Two new keys, and they are what the axis bought. `archive:delete_failed`
/// counts universes in which the INNER store failed a DELETE, and it is in
/// `require:` — a batch where no delete ever failed is a RED property, not a
/// quiet one, which is the only defence against an axis that samples to zero
/// and leaves reclamation covered on the happy path while looking covered
/// under fault. `archive:restored_after_delete_failure` counts the universes
/// that then restored anyway: 32/32, 21/23, 23/23, 32/32. The shortfall at
/// `0xD9E3_0001` is not a refused restore — a restore refused for a missing
/// object carries neither "simulated" nor "injected", so `is_fault_fallout`
/// classifies it `Unrelated` and the universe fails with an UNCLASSIFIED
/// error rather than reaching a floor at all. That base's run failed on the
/// floor and nothing else, so mechanically no restore was refused; the two
/// universes reached no restore verdict (`nothing_published`,
/// `verify_never_read_cleanly`, `source_unrecoverable`).
///
/// **All twelve numbers were re-measured at Task 12.5, and the reason they
/// moved is not decay.** Reclamation adds `ObjectStore` operations to the
/// workload, so from a generation's first roll onward every universe's
/// seeded fault stream is drawn against a different op sequence — the runs
/// are as reproducible as they ever were, but they are different runs. The
/// movement is ±2 in BOTH directions at every base (216->215, 223->224,
/// 181->183, 180->179, 168->169, 164->162, ...), which is trace jitter and
/// not a rate that fell. The pinned base's one lost `restored` is matched by
/// `verify_never_read_cleanly` going 12 -> 13, alongside `fault:read_error`
/// 3430 -> 3512: a universe that restores but never gets a clean read of its
/// own destination reports the second key instead of the first, which is the
/// arm directly above `archive:restored`'s `cover`. **A reclaimed object a
/// restore needed is ruled out mechanically rather than by that argument**:
/// restore's missing-object refusal carries neither "simulated" nor
/// "injected", so `is_fault_fallout` classifies it `Unrelated` and
/// `conv_restore_into` fails the universe with an unclassified error. All
/// 256 universes pass at the pinned base, so no restore was refused for a
/// missing object.
///
/// A fresh-base run panicking `restored_substantial: 180 of 181` or
/// `chained: 164 of 168` is therefore reporting a MISSING PRECONDITION rather
/// than a decayed oracle: a rate against a moving base is not a rate, which is
/// why the base is pinned. **The runner's own advice at that panic —
/// "Investigate before touching it" — is generic text addressed to a floor
/// evaluated where it was measured, so if you moved the base deliberately it
/// is not addressed to your run.** Spec §11's convergence criterion carries
/// that scope: universes green is what a fresh base establishes, floors are
/// pinned-base instruments, and the numbers below do not move to accommodate a
/// fresh base. At THIS base a shortfall is exactly what the runner says it is.
/// The table is here rather than in a run report because the evidence bar ASKS
/// for fresh-base runs, so whoever performs one meets these panics — and a
/// report is not in the tree they will be reading. Note the difference from
/// `replication.rs`'s `FINGERPRINT_FLOOR`, whose value was corroborated on
/// unpinned runs: these deliberately are not, and the spread above is why.
///
/// **Re-measured 2026-09-07, at the same bases, for a deliberate behavior
/// change.** `Wal::open_with_horizon` now barriers its directory on the clean
/// recovery path too (`log.rs`), closing a durability hole where an acked
/// append could land in a segment whose name a failed `sync_dir` had left
/// non-durable — see docs/LEDGER.md 2026-09-07 and the pinned mechanism test
/// in `charpente-sim/tests/wal.rs`. That adds one more fallible IO op to every
/// open, so a handful of universes now stop earlier than they used to and
/// never reach the oracle. This is the floor doing its job: it fell, and the
/// why is a known cause, not decay. Verified as such by CONTROL RUN — reverting
/// only the `log.rs` barrier makes both properties pass at these same bases,
/// which is what identifies the cause rather than assuming it.
///
/// | floor | orig | after barrier | after stream split | measured then | after payload shapes | measures now |
/// |---|---|---|---|---|---|---|
/// | `CONV_RESTORED_FLOOR` | 219 | 217 | 217 | 217 | 213 | 213 |
/// | `CONV_SUBSTANTIAL_FLOOR` | 183 | 181 | 181 | 191 | 181 | 187 |
/// | `CONV_CHAINED_FLOOR` | 169 | 169 | 166 | 166 | 166 | 176 |
/// | `CONV_SCOPED_RESTORED_FLOOR` | 214 | 213 | 211 | 211 | 211 | 217 |
/// | `CONV_SCOPED_SUBSTANTIAL_FLOOR` | 164 | 160 | 160 | 163 | 160 | 172 |
/// | `CONV_SCOPED_CHAINED_FLOOR` | 158 | 156 | 153 | 153 | 153 | 163 |
///
/// Every floor but `CONV_RESTORED_FLOOR` sits BELOW what it now measures. That is
/// deliberate: a floor is a MINIMUM, and raising one that is not failing is a
/// separate decision from repairing one that is. Every column here but one is
/// a MEASUREMENT, not a target. "after payload shapes" holds the floors as that
/// change left them: `CONV_RESTORED_FLOOR` re-pinned to its measurement, the
/// other five carried unchanged (the paragraph below).
///
/// **Re-measured 2026-09-24, same bases, for payload shapes.** Every enqueue and
/// emit record grew by 8 bytes (the shape, DESIGN §2) and every stored `EventRow`
/// by 8 more, so both bases are different runs under the same seeds, and
/// `CONV_RECORDS_PER_SEGMENT` moved 14 -> 13 with them. A CONTROL RUN of the
/// parent commit at these bases reproduces the "measured then" column exactly.
/// With the change, the unscoped base's four lost `archive:restored` are exactly
/// `archive:verify_never_read_cleanly` going 16 -> 20, with `nothing_published` (22)
/// and `restore_never_completed` (1) unchanged: universes that restored and then
/// never got a clean read of their own destination, the arm directly above
/// `archive:restored`'s `cover`. The scoped base moved the OTHER way at every
/// floor (`restored` 211 -> 217), which a decaying rate cannot do. Only the
/// failing floor was re-pinned, to its measurement: `CONV_RESTORED_FLOOR`
/// 217 -> 213.
///
/// **These moved TWICE in one day, and the second move is the more instructive.**
/// The barrier first landed after the replay loop and the floors were re-measured
/// to that; it then moved to before the replay loop (a correctness fix — see
/// `log.rs`), and every floor moved again. A `sync_dir` call consumes RNG draws,
/// so changing WHERE an IO op happens in `Wal::open` reshuffles every universe's
/// fault schedule at a pinned base. **Pinned-base floors were therefore coupled to
/// the IO-op ordering of code they do not name**, and every change to that ordering
/// cost a re-measure here.
///
/// **The THIRD move, same day, is the repair — and it is why there should not be a
/// fourth of this kind.** Rather than widen these floors to tolerate the coupling,
/// the coupling itself was removed: `SimDisk::sync_dir` now rolls off its own
/// `dir_faults` stream instead of the shared per-host `faults` stream, so adding or
/// relocating a barrier no longer displaces any read/write/sync roll (DESIGN.md, the
/// same split the object store's delete arm got; the oracle is
/// `a_directory_barrier_does_not_perturb_the_other_disk_faults` in `charpente-sim`).
/// Introducing that split re-rolled every universe ONE last time, which is this
/// column. Barrier movement is now non-perturbing; other workload and IO-shape
/// changes still re-roll, so a future panic here is still usually mechanical and
/// upstream rather than a decayed oracle — just no longer from a `sync_dir`.
///
/// **Re-measured 2026-09-30, same bases, for redb 4.3.0** — an IO-shape change
/// of exactly that kind. Its non-durable commits stay in its own write buffer
/// rather than reaching the file (DESIGN §1, "The store's crash surface"), so
/// every universe re-rolled. Two identical batches at each base; the "4.1.0"
/// column is a control run of the parent commit, and it reproduces "measures
/// now" above exactly. Every floor is set on its new count, in both tables.
/// That is the "separate decision" the table above deferred: the whole matrix
/// moved at once, and a floor left up to 77 below its count would tolerate the
/// old give-up rate coming back. The two `chained` counts fell against 4.1.0
/// but stay above their floors, so no floor is lowered. The unscoped base's
/// 22 universes without a verdict are `nothing_published` 4,
/// `verify_never_read_cleanly` 15 and `restore_never_completed` 3; the scoped
/// base's 20 are 6 and 14 and 0.
///
/// | floor | was | 4.1.0 | now |
/// |---|---|---|---|
/// | `CONV_RESTORED_FLOOR` | 213 | 213 | 234 |
/// | `CONV_SUBSTANTIAL_FLOOR` | 181 | 187 | 235 |
/// | `CONV_CHAINED_FLOOR` | 166 | 176 | 170 |
/// | `CONV_SCOPED_RESTORED_FLOOR` | 211 | 217 | 236 |
/// | `CONV_SCOPED_SUBSTANTIAL_FLOOR` | 160 | 172 | 237 |
/// | `CONV_SCOPED_CHAINED_FLOOR` | 153 | 163 | 161 |
///
/// **Raised 2026-10-01 with the restore and verify budgets**
/// (`CONV_RESTORE_ATTEMPTS` and `CONV_VERIFY_ATTEMPTS`, 32 -> 64). 3 and 4 more
/// universes restore and verify at the two bases, so the two `restored` floors
/// move up to their new counts, 234 -> 237 and 236 -> 240; no other floor's
/// count moved. The unscoped base's 19 universes without a verdict are now
/// `nothing_published` 4, `verify_never_read_cleanly` 13 and
/// `restore_never_completed` 2; the scoped base's 16 are 6 and 10 and 0. A
/// larger manifest-read budget rescued none of the `nothing_published` ones.
///
/// **Re-set 2026-10-01** at both bases, two identical batches each, for S5
/// (`Db::open` no longer discards a store over an IO error, and the diverged
/// plant boots through host restarts) and D1 (WAL recovery syncs each segment
/// before replay; a sync with nothing to flush draws no fault), which re-key
/// every universe. Five floors rose and are set on their counts. The scoped
/// base's `restored` fell 240 -> 238 and is re-set by GUIDE §8's re-key rule:
/// over the same 11 fresh bases (`0x5E5F_0C01`..`0x5E5F_0C0B`), the parent of
/// both (796bff2) reads mean 242.09, sd 2.95, and this tree 241.18, sd 3.49, a
/// difference of -0.91 against a Welch standard error of 1.38 (-0.66 SE), so no
/// decay. The unscoped base's 18 universes without a verdict are
/// `nothing_published` 4, `verify_never_read_cleanly` 12 and
/// `restore_never_completed` 2; the scoped base's 18 are 7 and 11 and 0.
///
/// | floor | was | now |
/// |---|---|---|
/// | `CONV_RESTORED_FLOOR` | 237 | 238 |
/// | `CONV_SUBSTANTIAL_FLOOR` | 235 | 236 |
/// | `CONV_CHAINED_FLOOR` | 170 | 176 |
/// | `CONV_SCOPED_RESTORED_FLOOR` | 240 | 238 |
/// | `CONV_SCOPED_SUBSTANTIAL_FLOOR` | 237 | 238 |
/// | `CONV_SCOPED_CHAINED_FLOOR` | 161 | 168 |
///
/// `restored_substantial` can exceed `restored`: it is covered once the
/// manifest's frontier is known, before the restore and its verification run.
const CONV_RESTORED_FLOOR: u64 = 238;
const CONV_SUBSTANTIAL_FLOOR: u64 = 236;
const CONV_CHAINED_FLOOR: u64 = 176;

/// `archive_converges_with_a_scoped_table`'s own base and floors. Separate
/// numbers because they are rates against a different workload — one extra row
/// and one scoped index entry per record — not a re-baselining of the three
/// above, which stay exactly where they were measured.
///
/// Pinned to the counts measured on this property's first run at this base,
/// with NO margin subtracted — the same discipline the three above follow.
/// `CONV_RESTORED_FLOOR`'s own history is the statement of it: "the floors
/// below were raised to the new pinned measurement rather than lowered to
/// accommodate anything." A floor here equals the measurement, so a later
/// shortfall of even one universe is a finding rather than noise absorbed by
/// slack, and a fresh base will trip these exactly as it trips the originals.
///
/// Measured at `0xA2C0_5C09`, 256 universes:
///
/// | key | measured | floor |
/// |---|---|---|
/// | `archive:restored` | 214 | 214 |
/// | `archive:restored_substantial` | 164 | 164 |
/// | `archive:chained` | 158 | 158 |
///
/// The 42 universes that reach no restore verdict say which kind:
/// `archive:nothing_published` 31, `archive:verify_never_read_cleanly` 10,
/// `archive:restore_never_completed` 1. Those three plus `archive:restored`
/// account for all 256, which is what makes "no verdict" a measured quantity
/// here rather than an unknown one — the same arithmetic
/// `CONV_RESTORED_FLOOR`'s doc performs for the unscoped base.
/// `tenant:restored-multi-tenant` fired 244 times, so the `require:` key is a
/// real floor rather than a decoration.
///
/// Re-measured 2026-09-30 for redb 4.3.0 with the three above, and re-set
/// 2026-10-01 with them; the tables and the reasons are at
/// `CONV_RESTORED_FLOOR`.
const ARCHIVE_SCOPED_CONVERGENCE_BASE: u64 = 0xA2C0_5C09;
const CONV_SCOPED_RESTORED_FLOOR: u64 = 238;
const CONV_SCOPED_SUBSTANTIAL_FLOOR: u64 = 238; // 164, then 160 after the 2026-09-07 open-path barrier; see CONV_RESTORED_FLOOR
const CONV_SCOPED_CHAINED_FLOOR: u64 = 168;

/// The give-up caps, exact at the two pinned bases: 18 and 18 of 256 universes
/// gave up in two identical batches each (2026-10-01, S5 with D1; 19 and 16 at
/// their parent, 796bff2). The breakdown is at `CONV_RESTORED_FLOOR`.
const CONV_GAVE_UP_CEILING: u64 = 18;
const CONV_SCOPED_GAVE_UP_CEILING: u64 = 18;

/// A restored `Db` this size or larger counts as a substantial verdict.
/// `require:`'s floor is one occurrence, which catches an oracle that
/// stopped firing and misses one that decayed to a token handful of rows —
/// this is the measured-rate companion, reported per batch.
const CONV_SUBSTANTIAL_ROWS: u64 = 200;

/// The convergence swarm's tenant-scoped, secondary-indexed table (hard rule
/// 3). Task 6c changed the archive's batch shape and `charpente restore` to
/// carry a scope; under an unscoped schema no universe here can express a
/// restore that drops one, so this table rides the swarm's own object-fault
/// and crash schedule.
///
/// Hand-written rather than `#[table(scoped_by = …)]` for its scope type's
/// `Error`: the workload's transactions return `Infallible`, which is what
/// makes "the app never rejects" a type-level fact in this property, and the
/// macro's const assert wants the platform's `ApiError` shape.
///
/// `marker` derives from the tenant, so a collapsed row is detectable by its
/// CONTENT and not only by the key it was found under.
struct ConvOrg;

struct ConvLedger {
    id: Id<ConvLedger>,
    bucket: u32,
    marker: u64,
}

impl Row for ConvLedger {
    type Scope = ConvOrg;
    const TABLE: &'static str = "ConvLedger";
    const INDEXES: &'static [IndexDef<ConvLedger>] = &[IndexDef {
        name: "bucket",
        key: |r, out| r.bucket.encode(out),
    }];
    fn id(&self) -> Id<ConvLedger> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.bucket.encode(out);
        self.marker.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<ConvLedger> {
        let input = &mut &bytes[..];
        Ok(ConvLedger {
            id: Id::decode(input)?,
            bucket: u32::decode(input)?,
            marker: u64::decode(input)?,
        })
    }
}

impl charpente::Tenant for ConvOrg {
    const NAME: &'static str = "ConvOrg";
    type Who = str;
    type Error = std::convert::Infallible;

    /// Always allows: this property is about the archive and restore paths
    /// carrying the scope, not about the app's membership check —
    /// `charpente/tests/tenant.rs` owns the refusing case.
    fn authorize(
        _tx: &charpente::Tx,
        _who: &str,
        _scope: Id<ConvOrg>,
    ) -> Result<(), std::convert::Infallible> {
        Ok(())
    }
}

/// The two tenants, and the ledger id they SHARE. Record `2k` writes tenant
/// 1's ledger id `k` and record `2k + 1` writes tenant 2's — so every ledger
/// id is held by both tenants, and each `(tenant, id)` pair is written exactly
/// once, which keeps the workload's append-only "row `n` came from record `n`"
/// correspondence that every anchor in this property rests on.
const CONV_TENANTS: [u64; 2] = [1, 2];
const CONV_MARKER_STRIDE: u64 = 1_000_000;

fn conv_marker(tenant: u64, id: u64) -> u64 {
    tenant * CONV_MARKER_STRIDE + id
}

/// The record that wrote ledger row `(scope, id)` — the cut `conv_verify_
/// restored` applies to the source, since the source may have committed past
/// the archived frontier.
///
/// A scope that is not one of the two tenants answers `0`, which KEEPS the row
/// in the comparison rather than cutting it: a collapsed scope is the failure
/// this property exists to catch, and a cut that swallowed it would hide it.
fn conv_ledger_writer(scope: u64, id: u64) -> u64 {
    let slot = CONV_TENANTS.iter().position(|&t| t == scope).unwrap_or(0) as u64;
    id * 2 + slot
}

/// The original property's schema, byte-for-byte what it has always been.
/// `ConvLedger` is deliberately absent: registering it costs one universe on
/// `archive:restored` (218 of 219, measured), and these floors are rates
/// against this exact workload (R56).
fn conv_schema() -> Schema {
    Schema::new().table::<Rec>()
}

/// The sibling property's schema. Same everything, plus the scoped table.
fn conv_scoped_schema() -> Schema {
    conv_schema().table::<ConvLedger>()
}

/// Which of the two convergence properties a universe is running. The whole
/// workload below is shared; this picks the schema, the one scoped write per
/// record, and the restored-tenant oracle. It draws no entropy, so the
/// unscoped mode's fault schedule is identical to what it was before the
/// scoped sibling existed.
#[derive(Clone, Copy, PartialEq)]
enum ConvMode {
    Unscoped,
    Scoped,
}

impl ConvMode {
    fn schema(self) -> Schema {
        match self {
            ConvMode::Unscoped => conv_schema(),
            ConvMode::Scoped => conv_scoped_schema(),
        }
    }
}

fn conv_db_config() -> DbConfig {
    let mut cfg = DbConfig::default();
    cfg.wal.segment_bytes = CONV_SEGMENT_BYTES;
    cfg
}

fn conv_restore_opts(force: bool) -> RestoreOpts {
    RestoreOpts {
        prefix: PREFIX.to_string(),
        force,
    }
}

/// A per-universe object-store fault rate: off in most universes, drawn from
/// a range in the rest — `sample_extended_fields`' own shape.
///
/// Drawn HERE rather than read off the universe's sampled `FaultConfig`
/// because `Sim` exposes `seed()` and `subscriber_stall()` and nothing else
/// about it, and re-deriving the swarm draw inside a test would duplicate a
/// frozen replay contract in a second place where a future reorder drifts
/// silently. Drawn from the sim's own entropy, so it is still a pure
/// function of the seed.
fn conv_rate(entropy: &dyn Entropy, on_in_percent: u64, max_per_mille: u64) -> f64 {
    if entropy.next_u64() % 100 >= on_in_percent {
        return 0.0;
    }
    (entropy.next_u64() % max_per_mille) as f64 / 1000.0
}

/// Everything one universe accumulates: the MODEL, and the observations that
/// are edges rather than states.
///
/// The model is the whole reason this property is not a self-comparison:
/// `model[i]` is the WAL seq of the record that wrote `Rec { id: i, value: i
/// }`, emitted `Ping { n: i }` and — when `jobs[i]` — enqueued one job, and
/// every entry in it comes from what `Db::transact` returned and from
/// nothing else. Nothing here is ever read back out of the archive.
///
/// The observations are carried rather than recomputed because they are
/// EDGES: a gap and an ownership stop are each raised once, at the step that
/// causes them (task-9-report.md T-6), and an edge is not visible in a final
/// state.
#[derive(Default)]
struct ConvState {
    model: Vec<u64>,
    jobs: Vec<bool>,
    /// The commit that was rejected but may be durable anyway
    /// (`TxError::Io`'s doc), carried until a restart resolves it.
    unacked: Option<(u64, bool)>,
    restarts: u32,
    gapped: bool,
    /// The key of a PUT whose ack was lost, still awaiting proof that the
    /// retry landed at the SAME key.
    lost_ack_key: Option<String>,
    rolled_generation: bool,
}

/// Engine/WAL poison ALONE. `Db::poisoned()` is `engine.poisoned() ||
/// store_failed()` and exposes no accessor for the first half, so it is
/// derived — exact until the store latches, and false for every step after
/// that. That bound is why the guard below is written as an edge and why it
/// is not the property's evidence for anything.
fn conv_engine_poisoned(db: &Db) -> bool {
    db.poisoned() && !db.store_failed()
}

/// The disk-error axes that can latch `Store::failed`, summed. Read across a
/// single `arc.step()` so the excusal below is STEP-scoped — the same
/// doctrine as `checks::durable_corruption_fired` and
/// `checks::dir_entry_loss_fired` ("excuse an outcome only when the axis that
/// could cause it actually fired"), narrowed from their universe-wide gate to
/// the one step that has to have caused it.
///
/// These three and no others: the latch is redb's
/// `CheckedBackend.io_failed`, which fires only when a backend call returns
/// `Err`, and in this property's matrix nothing else makes one do that. The
/// corruption axes return `Ok` (that is their whole treachery) and are off
/// here anyway (`no_media_corruption_on_either_side`), `dir_entry_loss`
/// gaps a directory rather than failing a call, and no universe here crashes
/// a disk — `Sim::crash_disk_*` is never called, so `disk_op`'s crashed-world
/// `Err` cannot happen with no coverage to show for it.
fn conv_disk_faults(sim: &Sim) -> u64 {
    let coverage = sim.coverage();
    ["fault:read_error", "fault:write_error", "fault:sync_error"]
        .iter()
        .map(|key| coverage.get(*key).copied().unwrap_or(0))
        .sum()
}

/// One archive step, plus the negative space that must hold on EVERY one of
/// them.
///
/// Returns `Err` only for something that is not fault fallout: the archive's
/// two named notices (the gap and the ownership stop) are `Err` at the step
/// that raises them by design (spec §7), and every store fault surfaces as
/// `Err` too, so an unclassified error is the only one that is a finding.
fn conv_step(
    sim: &Sim,
    arc: &mut Archive,
    db: &Db,
    store: &FlakyStore,
    st: &mut ConvState,
) -> Result<(), String> {
    let was_gapped = arc.gapped();
    let was_stopped = arc.stopped();
    let applied_before = db.last_applied();
    let gate_before = db.writes_allowed();
    let engine_poisoned_before = conv_engine_poisoned(db);
    let store_failed_before = db.store_failed();
    let disk_faults_before = conv_disk_faults(sim);
    let generation_before = arc.pin_floor();
    let r = arc.step(db, store);

    // NEGATIVE SPACE, every step (spec §8's first bullet): "the archive never
    // advances `last_applied`, acks a write, or gates a transaction."
    // Checked as EDGES across the step, because the source `Db` legitimately
    // does all three on its own and only the transition is attributable to
    // `Archive::step`.
    //
    // **These are ARCHITECTURAL REGRESSION GUARDS, not fault coverage**, and
    // the distinction is the whole reason they are labelled here. No fault
    // this property injects can fire any of them: `Archive::step` takes
    // `db: &Db` (archive/mod.rs) while `Db::transact` takes `&mut self`
    // (db.rs) and both `Engine::poisoned` sites are apply-path failure arms
    // (engine.rs), so the archive has no path to commit, ack, gate or poison
    // anything. They go red the day the archive is given a write path — a
    // `&mut Db` in its signature — which is exactly the change that would
    // make §8's first bullet false, and they say so at the step rather than
    // leaving the convergence oracle to notice a symptom later. Counting
    // them as evidence that the guarantee survives faults would overcount
    // this property's oracles by three.
    if db.last_applied() != applied_before {
        return Err(format!(
            "Archive::step advanced last_applied from {applied_before:?} to {:?} (spec §8: the \
             archive never advances `last_applied` or acks a write). The archive is a READER of \
             the Db and holds it as `&Db`; if it now needs to commit, that is a design change to \
             take to §8, not a test to update. The step returned {r:?}",
            db.last_applied()
        ));
    }
    if db.writes_allowed() != gate_before {
        return Err(format!(
            "Archive::step changed the Db's write gate (writes_allowed {gate_before} -> {}) \
             (spec §8: the archive never gates a transaction). The archive READS the gate — it \
             uploads nothing while `verifying()` — and never sets it; if it now needs to, that \
             is a design change to take to §8. The step returned {r:?}",
            db.writes_allowed()
        ));
    }
    if !engine_poisoned_before && conv_engine_poisoned(db) {
        return Err(format!(
            "Archive::step poisoned the engine (spec §8: an archive failure is not a database \
             failure). Engine poison is set only in `Engine::apply_batch`'s failure arms, a path \
             `&Db` cannot reach, so this is a write path the archive should not have. The step \
             returned {r:?}"
        ));
    }
    // A STORE LATCH the disk caused is not a violation, and it is counted
    // rather than asserted on. `Store::failed` is redb's
    // `CheckedBackend.io_failed`, latched by ANY backend read returning `Err`
    // — including a read only the archive issued (`db.snapshot()`, then
    // `scan_raw_from` per batch). So an archiving node can DISCOVER a failing
    // local disk sooner than a non-archiving one; it never causes one, and
    // durability is not implicated (`Store::failed`'s own doc: the WAL is
    // intact and committed records are durable). Spec §8 states that residual
    // in the same terms. Counted so its frequency is a number instead of an
    // argument — seed 0xb803ea7cab19d08c is the universe that made it
    // concrete.
    //
    // **A latch with NO disk error injected during the step is still a
    // violation**, and that narrowing is the whole point of reading the
    // counts across `arc.step()` rather than excusing the outcome outright.
    // This is the fourth path into `Db::poisoned()` — a write to the Db's own
    // redb store — and it is the one the three guards above do not cover: an
    // archive that acquired a write path could latch it without moving
    // `last_applied`, without touching the gate and without poisoning the
    // engine. Excused unconditionally it would tick a key carrying no
    // `require:` and no `floor:`, which nobody watches.
    if !store_failed_before && db.store_failed() {
        if conv_disk_faults(sim) == disk_faults_before {
            return Err(format!(
                "Archive::step latched the Db's store (`Store::failed` false -> true) with no \
                 disk error injected during the step (spec §8: an archive failure is not a \
                 database failure). The latch is redb's `CheckedBackend.io_failed`, set only by \
                 a backend call returning `Err`, and the archive is a READER of that store \
                 (`db.snapshot()`, then `scan_raw_from` per batch) — so a latch with \
                 fault:read_error/write_error/sync_error unmoved at {disk_faults_before} means \
                 the archive has a WRITE path into the Db's own store, which is what makes §8's \
                 first bullet false. That is a design change to take to §8, not a test to \
                 update. The step returned {r:?}"
            ));
        }
        sim.cover("archive:step_discovered_disk_fault");
    }
    // The archive's two operator notices are `Err`s it returns ON PURPOSE,
    // once, at the step that raises them (spec §7; task-9-report.md T-6), so
    // they have to be told apart from a real failure. Recognized by the
    // STATE TRANSITION that accompanies them and never by matching their
    // prose — `step_until_notice` above records why: a notice test that
    // matches a substring finds the words it is about to assert are there.
    let gap_notice = !was_gapped && arc.gapped();
    let stop_notice = !was_stopped && arc.stopped();
    if gap_notice {
        sim.cover("archive:gap_closed_generation");
        st.gapped = true;
    }
    if arc.pin_floor() > generation_before {
        st.rolled_generation = true;
    }
    let Err(e) = r else { return Ok(()) };
    let text = e.to_string();
    if let Some(key) = conv_lost_ack_key(&text) {
        st.lost_ack_key = Some(key);
    }
    if is_fault_fallout(&text) || gap_notice || stop_notice {
        return Ok(());
    }
    Err(format!(
        "Archive::step failed with an unclassified error: {e}"
    ))
}

/// The key named by a lost-ack error, from either of the two stores that
/// produce one (`SimObjectStore`'s `object_lost_ack` axis and `FlakyStore`'s
/// aimed switch). Both texts are `... PUT "<key>" landed but the ack was
/// lost ...`, and the KEY is what the property needs: retry-idempotence
/// (spec §9.2) is the claim that the retry goes to the SAME key.
fn conv_lost_ack_key(text: &str) -> Option<String> {
    if !text.contains("landed but the ack was lost") {
        return None;
    }
    let start = text.find('"')? + 1;
    let end = start + text[start..].find('"')?;
    Some(text[start..end].to_string())
}

/// Clears whatever operation the archive left in flight.
///
/// `ObjectStore::start` asserts against a second op while one is in flight
/// (spec §1), and `SimObjectStore` holds a started op until something polls
/// it — so an `Archive` REPLACED part-way through an upload leaves one
/// sitting there and the next thing to touch the store trips the assert.
/// That is this workload's problem and not the archive's: `Archive::step`
/// always polls what it started, and nothing in the shipped tree swaps a
/// live `Archive` out from under a store it shares. This universe does swap
/// one — that is how it reaches `State::Opening`'s resume-or-open path
/// (ruling 43) — and it also drives the store directly to read the manifest,
/// plant a foreign one, and restore. So it settles the store first, at every
/// one of those seams. Found by seed 0x8304b6d7b52cbe01.
fn conv_settle(store: &FlakyStore) {
    let _ = store.poll();
}

/// Reads the manifest through a store that may be injecting GET errors.
/// `None` only after `tries` consecutive failures, so "nothing is published"
/// is a fact about the bucket rather than about one unlucky roll.
fn conv_read_manifest(store: &FlakyStore, tries: u32) -> Option<Manifest> {
    for _ in 0..tries {
        if let Some(m) = read_manifest(store) {
            return Some(m);
        }
    }
    None
}

/// The newest recoverable point the manifest names: the chain's end when the
/// generation has one, its snapshot's frontier when it does not. NOT
/// `Manifest::applied_seq`, which is the SNAPSHOT's frontier and equals the
/// restored frontier only for a chainless generation (`RestoreReport::
/// applied_seq`'s own doc).
fn conv_frontier(m: &Manifest) -> u64 {
    m.segments.last().map_or(m.applied_seq, |s| s.end_seq)
}

/// A verification failure, split by whether it is the property's VERDICT or
/// the fault matrix landing on the checker's own reads.
///
/// The checker reads the destination through `Disk` like everything else, so
/// an injected read error inside it is not a convergence violation — and a
/// checker that cannot say which it hit turns every such roll into a false
/// RED. `kernel.rs`'s workload takes exactly this care with its
/// `exactly_once_holds` call, and its comment records the bug that taught it
/// to (seed 0x348270b38f166529, then 0xe653f43924f17fe8, then
/// 0xe2d97275c15cc087, then 0x0f34c969dfd340b0, now 0x56731152a87514ae).
enum ConvFail {
    /// Retryable: the matrix hit the checker. Recognized by
    /// `checks::is_fault_fallout`, never by this file's own substring list.
    /// Carries no text on purpose — nothing fails on this path, so a message
    /// here would be a message nothing ever reads.
    Fallout,
    /// The verdict. Fails the universe.
    Violation(String),
}

/// Classifies an error the checker RAISED WHILE READING. Never used on a
/// message the property composes itself — those are `Violation` by
/// construction, and routing one through here would let a violation whose
/// text happened to quote an injected error excuse itself.
fn conv_read_fail(what: &str, e: io::Error) -> ConvFail {
    if is_fault_fallout(&e.to_string()) {
        ConvFail::Fallout
    } else {
        ConvFail::Violation(format!("{what}: {e}"))
    }
}

/// Every `Rec` row a `Db` holds, as `(id, value)`, decoded — not compared as
/// opaque bytes, because the fixture's expectation is about the VALUES.
fn conv_rec_rows(db: &Db) -> Result<Vec<(u64, u64)>, ConvFail> {
    let snap = db.snapshot().map_err(|e| conv_read_fail("snapshot", e))?;
    let mut out = Vec::new();
    let mut after = None;
    loop {
        let batch = snap
            .scan_raw_from(Rec::TABLE, false, after, 1_000)
            .map_err(|e| conv_read_fail("scanning the restored rows", e))?;
        let Some(last) = batch.last() else { break };
        after = Some((last.0, last.1));
        for (_, id, bytes) in batch {
            // A row that will not decode is a VIOLATION, never fallout: the
            // bytes came back through the archive and the store, and "the
            // restored row does not decode" is the verdict.
            let r = Rec::decode_row(&bytes).map_err(|e| {
                ConvFail::Violation(format!(
                    "restored row {id} of {} does not decode: {e}",
                    Rec::TABLE
                ))
            })?;
            out.push((r.id.raw(), r.value));
        }
    }
    Ok(out)
}

/// THE ANCHORS. Every expectation here is the workload's, computed from
/// `records` — the count of committed records at or below the archived
/// frontier — and nothing here reads the archive, the manifest, or the
/// source `Db`. Asserted BEFORE the structural comparison, which is what
/// keeps a mutation inside the shared snapshot/decode path from moving both
/// sides of that comparison together.
///
/// Each clause names the mutation it exists to redden:
///  - the row map: `Seeder::rows` importing nothing, a snapshot batch
///    silently dropped, a decode that returns the wrong field.
///  - the exact COUNT: an import that stops early, and — the direction a
///    set comparison misses — one that imports rows the frontier excludes.
///  - `charpente.events`: a restore that seeds `next_event_id` from the row
///    count instead of the meta, which re-issues ids the snapshot carries.
///  - pending jobs: the same for `next_job_id`, and the operator-facing
///    count `RestoreReport` promises.
fn conv_check_anchors(db: &Db, records: u64, pending_jobs: u64) -> Result<(), ConvFail> {
    let rows = conv_rec_rows(db)?;
    if rows.len() as u64 != records {
        return Err(ConvFail::Violation(format!(
            "the restored Db holds {} `{}` rows; the workload committed {records} at or below \
             the archived frontier",
            rows.len(),
            Rec::TABLE
        )));
    }
    for (i, (id, value)) in rows.iter().enumerate() {
        let i = i as u64;
        if (*id, *value) != (i, i) {
            return Err(ConvFail::Violation(format!(
                "restored `{}` row {i} came back as (id {id}, value {value}); the workload \
                 wrote `Rec {{ id: {i}, value: {i} }}`",
                Rec::TABLE
            )));
        }
    }
    let snap = db.snapshot().map_err(|e| conv_read_fail("snapshot", e))?;
    let events = snap
        .scan_raw_from("charpente.events", false, None, usize::MAX)
        .map_err(|e| conv_read_fail("scanning charpente.events", e))?;
    if events.len() as u64 != records {
        return Err(ConvFail::Violation(format!(
            "the restored Db holds {} event row(s); the workload emitted exactly one per \
             committed record and {records} are at or below the archived frontier",
            events.len()
        )));
    }
    for (i, (_, id, _)) in events.iter().enumerate() {
        if *id != i as u64 {
            return Err(ConvFail::Violation(format!(
                "restored event row {i} has id {id}: the event counter came back wrong, so a \
                 restored node would re-issue ids the snapshot already carries"
            )));
        }
    }
    let pending = snap
        .scan_raw_from("charpente.jobs", false, None, usize::MAX)
        .map_err(|e| conv_read_fail("scanning charpente.jobs", e))?;
    if pending.len() as u64 != pending_jobs {
        return Err(ConvFail::Violation(format!(
            "the restored Db holds {} job row(s); the workload enqueued {pending_jobs} at or \
             below the archived frontier",
            pending.len()
        )));
    }
    Ok(())
}

/// Restores into `host`, re-running into the SAME destination while the
/// failure is fault fallout. Returns the report, or `None` if every attempt
/// was cut off.
///
/// This is spec §9.5 ("crash mid-restore → re-run from scratch converges")
/// and it is also the answer to what these universes do with a destination a
/// failed `restore` left behind: nothing. `restore` does not clean up after
/// itself — the cleanup Task 11 added lives in the CLI verb, and a caller
/// driving `restore` directly owns whatever `Disk` it passed. The partial
/// state is a contiguous prefix of the archived timeline, so the re-run's
/// own divergence check passes and `Seeder::new` discards it (task-10-report
/// D-2). If that were ever false, the re-run would come back as an
/// `InvalidData` refusal, which is not fault fallout and fails the universe
/// by name.
fn conv_restore_into(
    sim: &Sim,
    store: &FlakyStore,
    host: &str,
    force: bool,
    mode: ConvMode,
) -> Result<Option<RestoreReport>, String> {
    let mut cut = false;
    for attempt in 0..CONV_RESTORE_ATTEMPTS {
        match restore(
            store,
            Box::new(sim.host(host).disk()),
            conv_db_config(),
            mode.schema(),
            conv_restore_opts(force),
            &mut |_| {},
        ) {
            Ok(report) => {
                if cut {
                    sim.cover("archive:restore_retry_converged");
                }
                return Ok(Some(report));
            }
            Err(e) if is_fault_fallout(&e.to_string()) => {
                cut = true;
                sim.cover("archive:restore_faulted");
                let _ = attempt;
            }
            Err(e) => {
                return Err(format!(
                    "restore into \"{host}\" failed with an unclassified error: {e}"
                ));
            }
        }
    }
    Ok(None)
}

/// A destination holding a DIFFERENT timeline at the same seqs — same
/// schema, same shape, `value = id + 7` so every frame's checksum differs
/// from the archive's at the same seq. `checkpoint_every: 0` so the log
/// retains every frame: the comparison seq is the archive's chain end, and a
/// destination that had GC'd past it would make this "cannot compare"
/// instead of "diverged".
fn conv_plant_diverged(
    sim: &Sim,
    host: &str,
    records: u64,
    mode: ConvMode,
) -> Result<bool, String> {
    let mut cfg = conv_db_config();
    cfg.checkpoint_every = 0;
    // A boot that meets an injected fault restarts the host and boots again,
    // as a supervised process would (`checks::boot_through_fallout`'s reason,
    // on a host of its own); only a host that never boots skips the plant.
    let mut boots = 1;
    let mut db = loop {
        match Db::open(Box::new(sim.host(host).disk()), cfg.clone(), mode.schema()) {
            Ok(db) => break db,
            Err(e) if !is_fault_fallout(&e.to_string()) => {
                return Err(format!("planting a diverged timeline: open: {e}"));
            }
            Err(_) if boots == CONV_OPEN_ATTEMPTS => return Ok(false),
            Err(_) => {
                boots += 1;
                sim.cover("boot:retried");
                sim.host(host).restart();
            }
        }
    };
    for i in 0..records {
        match db.transact(|tx| -> Result<(), std::convert::Infallible> {
            tx.put(&Rec {
                id: Id::from_raw(i),
                value: i + 7,
            });
            Ok(())
        }) {
            Ok(()) => {}
            // A plant the matrix cut short is not a diverged timeline: the
            // destination has to hold a frame at a seq the archive's chain
            // also covers, or the refusal under test would be "cannot
            // compare" rather than "diverged".
            Err(TxError::Io(e)) if is_fault_fallout(&e.to_string()) => return Ok(false),
            Err(other) => {
                return Err(format!(
                    "planting a diverged timeline: commit {i} rejected: {other:?}"
                ));
            }
        }
    }
    Ok(true)
}

/// The whole verdict on one restored destination: it opens, it opens AT the
/// archived frontier, the fixture's own anchors hold, and only then the
/// structural comparison against the source.
///
/// The order is the point. Every anchor is the WORKLOAD's expectation, fixed
/// before the archive existed; the structural comparison is the only clause
/// with the source on the other side of it, and it runs last so that a
/// mutation inside the shared snapshot/decode path — which would move both
/// sides of it together — has already been caught by an anchor that does not
/// move (task-12-hazard-note.md).
/// Plants a manifest naming another writer, retrying past injected PUT
/// failures. `write_foreign_manifest` unwraps — right in a fault-free test,
/// fatal under this matrix — and a lost ack retries onto the same fixed key
/// with the same bytes, so the retry is idempotent by construction.
/// `false` means the plant never landed and the probe cannot run.
fn conv_plant_foreign_manifest(store: &FlakyStore, generation: u64) -> bool {
    let bytes = Manifest {
        generation,
        writer: FOREIGN_WRITER.to_string(),
        applied_seq: 0,
        next_job_id: 0,
        tables: Vec::new(),
        segments: Vec::new(),
        closed: false,
    }
    .encode();
    for _ in 0..64 {
        if store
            .start(ObjectOp::Put {
                key: manifest_key(PREFIX),
                data: bytes.clone(),
            })
            .is_err()
        {
            continue;
        }
        if drain(store).is_ok() {
            return true;
        }
    }
    false
}

fn conv_verify_restored(
    src: Option<&Db>,
    host: &str,
    sim: &Sim,
    records: u64,
    pending_jobs: u64,
    f: u64,
    mode: ConvMode,
) -> Result<(), ConvFail> {
    let dst = Db::open(
        Box::new(sim.host(host).disk()),
        conv_db_config(),
        mode.schema(),
    )
    .map_err(|e| conv_read_fail("the restored destination will not open", e))?;
    if dst.last_applied() != Some(f) {
        return Err(ConvFail::Violation(format!(
            "the restored Db opens at {:?}, not at the archived frontier {f}",
            dst.last_applied()
        )));
    }
    conv_check_anchors(&dst, records, pending_jobs)?;
    if mode == ConvMode::Scoped {
        conv_check_tenants(&dst, sim)?;
    }

    // The source's state AT the archived frontier. Each of the three tables
    // that grows with the workload is cut by its own WORKLOAD-side counter:
    // `Rec` and `charpente.events` get one row per committed record
    // (`records`), `charpente.jobs` one per enqueue (`pending_jobs`).
    // `charpente.schema_versions` does not grow.
    // The structural comparison is the only clause with the SOURCE on the
    // other side of it, so it is also the only one a destroyed source can
    // take away. Everything above this line still ran.
    let Some(src) = src else {
        sim.cover("archive:compared_without_source");
        return Ok(());
    };
    let dst_rows =
        logical_rows_checked(&dst).map_err(|e| conv_read_fail("reading the restored Db", e))?;
    let src_rows: Vec<TableRows> = logical_rows_checked(src)
        .map_err(|e| conv_read_fail("reading the source Db", e))?
        .into_iter()
        .map(|(name, rows)| {
            // `ConvLedger` is cut by the RECORD that wrote each row, not by
            // the row's own id: its ids are shared between two tenants, so
            // `(scope, id)` and not `id` alone says which record produced it.
            let rows = match name.as_str() {
                n if n == Rec::TABLE || n == "charpente.events" => rows
                    .into_iter()
                    .filter(|(_, id, _)| *id < records)
                    .collect(),
                "charpente.jobs" => rows
                    .into_iter()
                    .filter(|(_, id, _)| *id < pending_jobs)
                    .collect(),
                n if n == ConvLedger::TABLE => rows
                    .into_iter()
                    .filter(|(scope, id, _)| conv_ledger_writer(*scope, *id) < records)
                    .collect(),
                _ => rows,
            };
            (name, rows)
        })
        .collect();
    if dst_rows != src_rows {
        let names = |v: &[TableRows]| {
            v.iter()
                .map(|(n, r)| format!("{n}:{}", r.len()))
                .collect::<Vec<_>>()
        };
        return Err(ConvFail::Violation(format!(
            "the restored Db is not logically equal to the source at the archived frontier \
             {f}: restored {:?} vs source {:?}",
            names(&dst_rows),
            names(&src_rows)
        )));
    }
    Ok(())
}

/// The restored destination's own tenant oracle, on stored composite keys:
/// each tenant's slice holds only its own rows, through the row range and
/// through the scoped index. The structural comparison above catches a
/// restore that collapses one side only; this catches a collapse that is
/// symmetric between source and destination, and it names the tenant that
/// lost its rows rather than a row count.
fn conv_check_tenants(dst: &Db, sim: &Sim) -> Result<(), ConvFail> {
    let snap = dst
        .snapshot()
        .map_err(|e| conv_read_fail("snapshotting the restored Db", e))?;
    let mut populated = 0;
    for tenant in CONV_TENANTS {
        let rows = snap
            .scan_scoped::<ConvLedger>(tenant)
            .map_err(|e| conv_read_fail("reading a restored tenant", e))?;
        if !rows.is_empty() {
            populated += 1;
        }
        for row in &rows {
            if row.marker / CONV_MARKER_STRIDE != tenant {
                return Err(ConvFail::Violation(format!(
                    "the restored Db's tenant {tenant} holds id {} marked for tenant {}",
                    row.id.raw(),
                    row.marker / CONV_MARKER_STRIDE
                )));
            }
        }
        for bucket in 0..3u32 {
            for row in snap
                .scan_by_scoped::<ConvLedger>(tenant, "bucket", &bucket)
                .map_err(|e| conv_read_fail("reading a restored tenant's index", e))?
            {
                if row.marker / CONV_MARKER_STRIDE != tenant {
                    return Err(ConvFail::Violation(format!(
                        "the restored Db's tenant {tenant} index bucket {bucket} returned id {} \
                         marked for tenant {}",
                        row.id.raw(),
                        row.marker / CONV_MARKER_STRIDE
                    )));
                }
            }
        }
    }
    if populated > 1 {
        // A single-tenant restore cannot express a collapse at all — every
        // row legitimately belongs to the only tenant.
        sim.cover("tenant:restored-multi-tenant");
    }
    Ok(())
}

/// Opens (or reopens) the source `Db`, retrying past injected IO. `None`
/// means the matrix never let it open, which is not a violation — it is a
/// universe with no node in it.
fn conv_open_source(sim: &Sim, mode: ConvMode) -> Result<Option<Db>, String> {
    for _ in 0..CONV_OPEN_ATTEMPTS {
        match Db::open(
            Box::new(sim.host("src").disk()),
            conv_db_config(),
            mode.schema(),
        ) {
            Ok(db) => return Ok(Some(db)),
            Err(e) if is_fault_fallout(&e.to_string()) => continue,
            Err(e) => return Err(format!("source Db::open failed non-injected: {e}")),
        }
    }
    Ok(None)
}

/// Reconciles the model with what recovery brought back, after the source
/// was reopened.
///
/// **No `Sim::restart` is involved** (see the section header), so nothing
/// synced is collapsed: every record whose `transact` returned `Ok` was
/// appended AND synced before it answered, and those bytes are still on the
/// disk. Exactly one record can legitimately appear that the model does not
/// have — `TxError::Io`'s own doc: "Io after the WAL sync means the record
/// is durable but unacknowledged and will resurface on recovery" — and it is
/// the record the failing commit was writing, whose content this workload
/// therefore knows. Anything else recovery does to the frontier is a finding
/// and is named as one rather than absorbed, which is the difference between
/// repairing a model and reading the answer out of the thing under test.
fn conv_reconcile(db: &Db, st: &mut ConvState) -> Result<(), String> {
    let unacked = st.unacked.take();
    let acked = st.model.len() as u64;
    match db.last_applied() {
        None if acked == 0 => Ok(()),
        None => Err(format!(
            "recovery came back with an empty log after {acked} acknowledged commit(s): a \
             synced, acknowledged record was lost without a power cycle"
        )),
        Some(l) if l + 1 == acked => Ok(()),
        Some(l) if l == acked => {
            // The durable-but-unacknowledged record resurfaced. It is the
            // one the LAST rejected commit was writing, wherever in the
            // universe that happened — not necessarily the one that
            // triggered this restart, since a rejected commit poisons the
            // `Db` and a later phase can be the first to notice. Getting
            // that wrong is a real defect this property found in its own
            // model: seed 0x7280605429586c42, where an outage-window commit
            // (never a job) resurfaced and the restart carried the NEXT
            // round's job flag onto it, so the model expected 29 pending
            // jobs against the restored 28.
            let Some((id, with_job)) = unacked else {
                return Err(format!(
                    "recovery came back at seq {l} with {acked} acknowledged commit(s) and no \
                     commit outstanding: a record appeared that nothing ever wrote"
                ));
            };
            if id != l {
                return Err(format!(
                    "recovery came back at seq {l}, but the outstanding commit was writing id \
                     {id}"
                ));
            }
            st.model.push(l);
            st.jobs.push(with_job);
            Ok(())
        }
        Some(l) if l + 1 < acked => Err(format!(
            "recovery came back at seq {l} after {acked} acknowledged commit(s): {} \
             synced, acknowledged record(s) were lost without a power cycle",
            acked - (l + 1)
        )),
        Some(l) => Err(format!(
            "recovery came back at seq {l} after {acked} acknowledged commit(s) and one \
             unacknowledged one: it invented {} record(s) nothing ever wrote",
            l - acked
        )),
    }
}

fn archive_convergence_universe(sim: &mut Sim, mode: ConvMode) -> Result<(), String> {
    let entropy = sim.entropy();

    // `Option` because the matrix can destroy the source past reopening, and
    // that is NOT a reason to skip the oracle: the anchors are the
    // workload's own and need no source at all, and "the node is gone and
    // the bucket is what is left" is the case the archive exists for. Only
    // the structural comparison — the one clause with the source on the
    // other side of it — is skipped there.
    let src0 = match conv_open_source(sim, mode)? {
        Some(db) => db,
        None => {
            sim.cover("archive:source_never_opened");
            sim.give_up(
                "the source never opened: every attempt hit injected faults, so nothing was \
                 archived or restored",
            );
            return Ok(());
        }
    };
    // The fixture pin the whole expectation rests on: a fresh `Db` has
    // committed nothing, so the i-th record this workload commits takes seq
    // `i` and writes `Rec { id: i }`. Asserted rather than assumed — if
    // anything ever commits a record during `Db::open`, every anchor below
    // is off by one and this is where it says so.
    if src0.last_applied().is_some() {
        return Err(format!(
            "a freshly opened Db reports last_applied {:?}: the seq-to-id correspondence every \
             anchor in this property rests on is not established by construction any more",
            src0.last_applied()
        ));
    }
    let mut src = Some(src0);

    let object_faults = FaultConfig {
        object_put_error: conv_rate(&entropy, 40, 50),
        object_get_error: conv_rate(&entropy, 30, 10),
        object_lost_ack: conv_rate(&entropy, 30, 30),
        // The delete axis, on for the first time. It draws from `entropy`
        // like its three neighbours, so ADDING it shifted `snapshot_every`,
        // `pin_segments` and `interleave` in every universe — the floors
        // below were re-measured at four bases, not adjusted to fit.
        object_delete_error: conv_rate(&entropy, 40, 40),
        ..FaultConfig::none()
    };
    let store = FlakyStore::new(SimObjectStore::with_faults(sim.seed(), object_faults));

    let snapshot_every = [0u64, 40, 120][(entropy.next_u64() % 3) as usize];
    let pin_segments = 1 + (entropy.next_u64() % 6) as usize;
    let interleave = CONV_INTERLEAVE_CHOICES[(entropy.next_u64() % 4) as usize];
    let archive_cfg = archive_config_bounded(snapshot_every, 8 << 20, pin_segments);
    let mut arc = Archive::open(archive_cfg.clone(), src.as_mut().expect("just opened"))
        .map_err(|e| format!("Archive::open: {e}"))?;

    let mut st = ConvState::default();

    'rounds: for _round in 0..CONV_ROUNDS {
        // ---- writes, with archive steps INTERLEAVED rather than batched
        // after them. Not cosmetic: the archive reads the log through the
        // `Db`, so a source the matrix kills mid-round takes the archive
        // with it, and an archive that had not stepped yet has published
        // nothing for the oracle to read. Measured — before the interleave,
        // universes that lost their source published nothing at all.
        let want = 1 + entropy.next_u64() % CONV_WRITES_PER_ROUND;
        for w in 0..want {
            let id = st.model.len() as u64;
            let with_job = entropy.next_u64().is_multiple_of(24);
            let committed = {
                let Some(db) = src.as_mut() else {
                    break 'rounds;
                };
                db.transact(|tx| -> Result<(), std::convert::Infallible> {
                    tx.put(&Rec {
                        id: Id::from_raw(id),
                        value: id,
                    });
                    // One scoped write per record, alternating tenants over a
                    // shared ledger id. One scope per transaction (spec §8),
                    // and one record per commit either way, so the workload's
                    // seq-to-id correspondence is untouched.
                    if mode == ConvMode::Scoped {
                        let tenant = CONV_TENANTS[(id % 2) as usize];
                        tx.open_scope(Id::<ConvOrg>::from_raw(tenant), "operator")?
                            .put(&ConvLedger {
                                id: Id::from_raw(id / 2),
                                bucket: ((id / 2) % 3) as u32,
                                marker: conv_marker(tenant, id / 2),
                            });
                    }
                    tx.emit(Ping { n: WireU64(id) });
                    if with_job {
                        tx.enqueue(Resurrect { slot: id });
                    }
                    Ok(())
                })
            };
            match committed {
                Ok(()) => {
                    let seq = src
                        .as_ref()
                        .and_then(|db| db.last_applied())
                        .ok_or("a committed record left last_applied None")?;
                    if seq != id {
                        return Err(format!(
                            "the {id}-th committed record landed at seq {seq}: the seq-to-id \
                             correspondence every anchor in this property rests on is broken"
                        ));
                    }
                    st.model.push(seq);
                    st.jobs.push(with_job);
                }
                Err(TxError::Io(e)) => {
                    if !is_fault_fallout(&e.to_string()) {
                        return Err(format!("source commit failed non-injected: {e}"));
                    }
                    sim.cover("archive:source_write_faulted");
                    // `get_or_insert`, never an assignment: the FIRST
                    // rejected commit is the one that may be durable anyway
                    // (`TxError::Io`'s doc), and every rejection after it
                    // hits a poisoned WAL and writes nothing at all.
                    // Overwriting here is the seed-0x7280605429586c42 defect
                    // in `conv_reconcile`'s doc.
                    st.unacked.get_or_insert((id, with_job));
                    if st.restarts >= CONV_MAX_RESTARTS {
                        break 'rounds;
                    }
                    st.restarts += 1;
                    // The node restarts. A poisoned `Db` accepts nothing
                    // further and the archive, which reads the log through
                    // it, ships nothing further either. The old handle goes
                    // FIRST — the store file is opened again, not twice.
                    src = None;
                    match conv_open_source(sim, mode)? {
                        Some(mut fresh) => {
                            conv_reconcile(&fresh, &mut st)?;
                            conv_settle(&store);
                            arc = Archive::open(archive_cfg.clone(), &mut fresh).map_err(|e| {
                                format!("Archive::open after a source restart: {e}")
                            })?;
                            src = Some(fresh);
                            sim.cover("archive:source_restarted");
                        }
                        None => {
                            sim.cover("archive:source_unrecoverable");
                            break 'rounds;
                        }
                    }
                }
                Err(other) => {
                    return Err(format!("impossible source rejection: {other:?}"));
                }
            }
            if w % CONV_WRITES_PER_STEP == 0 {
                for _ in 0..interleave {
                    let Some(db) = src.as_ref() else {
                        break 'rounds;
                    };
                    conv_step(sim, &mut arc, db, &store, &mut st)?;
                    if arc.idle() {
                        break;
                    }
                }
            }
        }

        // ---- one decorated phase per round.
        match entropy.next_u64() % 6 {
            // The write gate (spec §9.11, §4 rule 1, §8's negative space): a
            // `Verifying` node uploads NOTHING. An oracle rather than a
            // scenario — the bucket is compared across the gated window.
            //
            // Covered only when the archive HAD work at the moment the gate
            // closed. Gating an archive that is already caught up proves
            // nothing: no writes are accepted while the gate is shut, so no
            // work can appear behind it either, and a key that counts those
            // windows counts something that would hold against an archive
            // that did nothing at all.
            0 => {
                let Some(db) = src.as_mut() else {
                    break 'rounds;
                };
                // ONE step first, then read `idle()`: the flag is the last
                // step's answer and nothing else, so reading it after a
                // round of writes reports what the archive knew BEFORE
                // them. That staleness is the same one Task 9's ready
                // predicate had to pair `idle()` with a watermark to
                // survive (task-9-report.md §2).
                conv_step(sim, &mut arc, db, &store, &mut st)?;
                let had_work = !arc.idle();
                let before = store.keys();
                db.verify_writes(vec!["witness-a".to_string()]);
                for _ in 0..CONV_GATED_STEPS {
                    conv_step(sim, &mut arc, db, &store, &mut st)?;
                }
                let after = store.keys();
                if after != before {
                    return Err(format!(
                        "a Verifying node uploaded: the bucket went from {} object(s) to {} \
                         while the write gate was closed (spec §4 rule 1)",
                        before.len(),
                        after.len()
                    ));
                }
                if had_work {
                    sim.cover("archive:verifying_gated");
                }
                db.allow_writes();
            }
            // A SUSTAINED store outage past the pin bound (spec §9.3/§9.4,
            // ruling 3). Per-op probabilities cannot produce one: the gap
            // needs the store down for as long as it takes `pin_segments`
            // segments to seal, so the outage is a WINDOW, and the records
            // written inside it are counted in segments rather than picked.
            1 => {
                store.set_reachable(false);
                let inside = (pin_segments as u64 + 2) * CONV_RECORDS_PER_SEGMENT;
                for _ in 0..inside {
                    let id = st.model.len() as u64;
                    let Some(db) = src.as_mut() else {
                        break 'rounds;
                    };
                    match db.transact(|tx| -> Result<(), std::convert::Infallible> {
                        tx.put(&Rec {
                            id: Id::from_raw(id),
                            value: id,
                        });
                        tx.emit(Ping { n: WireU64(id) });
                        Ok(())
                    }) {
                        Ok(()) => {
                            st.model.push(id);
                            st.jobs.push(false);
                        }
                        Err(TxError::Io(e)) if is_fault_fallout(&e.to_string()) => {
                            st.unacked.get_or_insert((id, false));
                            break;
                        }
                        Err(other) => {
                            return Err(format!("outage-window commit rejected: {other:?}"));
                        }
                    }
                    let Some(db) = src.as_ref() else {
                        break 'rounds;
                    };
                    conv_step(sim, &mut arc, db, &store, &mut st)?;
                }
                store.set_reachable(true);
            }
            // A lost ack aimed at a key the archive is actually about to
            // write (spec §9.2). `SimObjectStore`'s own axis cannot be
            // aimed, and at any rate that fires reliably it loses the FIRST
            // object's ack so the archive never reaches a manifest at all —
            // the same reason Task 5's re-review added `FlakyStore`'s
            // needle.
            2 => {
                let needle =
                    ["/manifest", "/segments/", "/snapshot/"][(entropy.next_u64() % 3) as usize];
                store.set_lost_ack_for(Some(needle));
                for _ in 0..CONV_STEPS_PER_ROUND {
                    let Some(db) = src.as_ref() else {
                        break 'rounds;
                    };
                    conv_step(sim, &mut arc, db, &store, &mut st)?;
                    if st.lost_ack_key.is_some() {
                        break;
                    }
                }
                store.set_lost_ack_for(None);
            }
            // The archive's own restart: `State::Opening`'s resume-or-open
            // (ruling 43). Dropping the old `Archive` releases its pin (F4);
            // the fresh one has to read the bucket and decide whether to
            // resume the generation the manifest names.
            3 => {
                let Some(db) = src.as_mut() else {
                    break 'rounds;
                };
                conv_settle(&store);
                arc = Archive::open(archive_cfg.clone(), db)
                    .map_err(|e| format!("Archive::open on restart: {e}"))?;
                sim.cover("archive:reopened");
            }
            _ => {}
        }

        // ---- drive the archive.
        for _ in 0..CONV_STEPS_PER_ROUND {
            let Some(db) = src.as_ref() else {
                break 'rounds;
            };
            conv_step(sim, &mut arc, db, &store, &mut st)?;
            if arc.idle() {
                break;
            }
        }
    }

    // ---- quiesce. Every test-local switch off; the object axes the
    // universe drew stay on, so this is "the outage ended", not "the faults
    // stopped".
    store.set_reachable(true);
    store.set_lost_ack_for(None);
    store.set_failing_gets(0);
    if let Some(db) = src.as_mut()
        && db.verifying()
    {
        db.allow_writes();
    }
    if let Some(db) = src.as_ref() {
        for _ in 0..CONV_FINAL_STEPS {
            conv_step(sim, &mut arc, db, &store, &mut st)?;
            if arc.idle() {
                break;
            }
        }
    }

    // ---- retry-idempotence (spec §9.2): a lost ack's retry goes to the
    // SAME key, so the object is there under the key the failure named.
    // `keys()` is the store's own inventory and takes no fault roll, so this
    // cannot be excused by an unlucky GET.
    //
    // TASK 12.5 adds one excuse, and only one: an object the archive itself
    // RECLAIMED is legitimately absent, and its absence says nothing about
    // whether the retry went to the same key. `deleted()` is the store's own
    // record of what the archive ASKED to delete, so the excuse is the exact
    // event rather than an inference from the bucket — and, like `keys()`, it
    // takes no store operation, which matters here: the archive may have one
    // in flight at this point and a GET would trip spec §1's assert (found at
    // base 0xD9E3_0001, seed 0x71cec6ac14e0525a). That the excuse is needed
    // at all was found by seed 0xfadf45b42f89ce91, where a lost-ack key under
    // generation 3 was reclaimed by generation 4's swap before this ran.
    if let Some(key) = st.lost_ack_key.clone() {
        let reclaimed = store.deleted().contains(&key);
        if store.keys().contains(&key) {
            sim.cover("archive:lost_ack_retried");
        } else if reclaimed {
            sim.cover("archive:lost_ack_key_reclaimed");
        } else if !arc.stopped() && !st.gapped && src.is_some() {
            return Err(format!(
                "a PUT to \"{key}\" landed and its ack was lost, and the object is not in the \
                 bucket under that key afterwards — the retry did not go to the same key \
                 (spec §9.2)"
            ));
        }
    }
    // TASK 12.5 / spec §3. Reclamation across the whole fault matrix, and
    // not only in the six hand-built tests: a generation the manifest has
    // stopped naming does not stay in the bucket forever.
    if !store.deleted().is_empty() {
        sim.cover("archive:generation_reclaimed");
    }
    // NON-VACUITY. `object_delete_error` is a probability, so nothing
    // guarantees it fires — and a delete axis that never fires leaves this
    // property exercising reclamation on the happy path while LOOKING like
    // it covers failure. This key is in `require:`, so a batch in which no
    // delete ever failed is a red property rather than a quiet one. It
    // counts only INNER failures: `DeleteMode`'s aimed switches never reach
    // the inner store, so a hand-driven test cannot satisfy it.
    if store.delete_failures() > 0 {
        sim.cover("archive:delete_failed");
    }
    if st.rolled_generation {
        sim.cover("archive:generation_rolled");
    }
    if src.is_none() {
        sim.cover("archive:source_destroyed");
    }

    // ---- THE ORACLE. The store is the workload's from here on.
    conv_settle(&store);
    let Some(m) = conv_read_manifest(&store, 16) else {
        sim.cover("archive:nothing_published");
        sim.give_up(
            "no manifest was ever read back from the archive, so there was no frontier to \
             restore to",
        );
        return Ok(());
    };
    let f = conv_frontier(&m);
    if !m.segments.is_empty() {
        sim.cover("archive:chained");
    }

    // ANCHOR 0, before one byte is restored: the frontier the archive
    // published is a seq this SOURCE actually committed. It is the anchor
    // that makes the rest of them anchors — every expectation below is
    // computed AT `f`, so a wrong `f` moves both sides of all of them
    // together (the self-comparison shape task-12-hazard-note.md is about).
    // `model[i] == i` is pinned at every commit, so this is exact.
    if f as usize >= st.model.len() {
        return Err(format!(
            "the manifest publishes frontier {f}, above the {} record(s) this source ever \
             committed — the archive named a frontier nothing backs",
            st.model.len()
        ));
    }
    let records = f + 1;
    let pending_jobs = st.jobs[..records as usize].iter().filter(|j| **j).count() as u64;
    if records >= CONV_SUBSTANTIAL_ROWS {
        sim.cover("archive:restored_substantial");
    }

    let Some(report) = conv_restore_into(sim, &store, "dst", false, mode)? else {
        sim.cover("archive:restore_never_completed");
        sim.give_up(
            "the restore never completed past injected faults, so nothing restored was checked",
        );
        return Ok(());
    };
    if report.applied_seq != f {
        return Err(format!(
            "restore came up at {} but the manifest's newest recoverable point is {f} \
             (generation {}, {} segment(s))",
            report.applied_seq, report.generation, report.segments
        ));
    }
    if report.generation != m.generation {
        return Err(format!(
            "restore reports generation {} for a manifest naming {}",
            report.generation, m.generation
        ));
    }
    if report.pending_jobs != pending_jobs {
        return Err(format!(
            "restore reports {} pending job(s); the workload enqueued {pending_jobs} at or \
             below seq {f}",
            report.pending_jobs
        ));
    }

    // Injected read errors are transient by design (`FaultConfig::read_error`:
    // "the next read of the same bytes may succeed") and the verification
    // below is disk IO like anything else, so it is retried on fallout and
    // only on fallout. A universe that never gets a clean read of its own
    // destination reaches no verdict, and says so with a coverage key rather
    // than passing quietly.
    let mut verified = false;
    for _ in 0..CONV_VERIFY_ATTEMPTS {
        match conv_verify_restored(src.as_ref(), "dst", sim, records, pending_jobs, f, mode) {
            Ok(()) => {
                verified = true;
                break;
            }
            Err(ConvFail::Fallout) => continue,
            Err(ConvFail::Violation(msg)) => return Err(msg),
        }
    }
    if !verified {
        sim.cover("archive:verify_never_read_cleanly");
        sim.give_up(format!(
            "the restored destination never read cleanly in {CONV_VERIFY_ATTEMPTS} attempts, so \
             the restore was never verified"
        ));
        return Ok(());
    }
    sim.cover("archive:restored");
    // The Task 2 question, and the one a happy-path-plus-error-return axis
    // could not have asked: a restore that succeeded in a universe where
    // reclamation's deletes were FAILING and retrying, interleaved with the
    // put/get faults and whatever gap this universe drew. `delete` is
    // idempotent, so a failed delete is a retry rather than a lost object —
    // this key is the evidence that the retry path never cost a restore.
    if store.delete_failures() > 0 {
        sim.cover("archive:restored_after_delete_failure");
    }

    // ---- spec §9.8: restore onto a DIVERGED local timeline refuses, and
    // `--force` takes it and converges to the same place.
    if entropy.next_u64().is_multiple_of(3) && conv_plant_diverged(sim, "div", records + 1, mode)? {
        match restore(
            &store,
            Box::new(sim.host("div").disk()),
            conv_db_config(),
            mode.schema(),
            conv_restore_opts(false),
            &mut |_| {},
        ) {
            Ok(r) => {
                return Err(format!(
                    "restore replaced a diverged local timeline without --force: {r:?}"
                ));
            }
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                let text = e.to_string();
                if !text.contains("--force") || !text.contains("Nothing in the destination has") {
                    return Err(format!(
                        "the divergence refusal must name --force and promise the destination \
                         is untouched (hard rule 5): {text}"
                    ));
                }
                sim.cover("archive:diverged_refused");
                // `--force` takes it, and lands at the same place: the same
                // verdict, on a destination that held a different timeline
                // a moment ago.
                if let Some(forced) = conv_restore_into(sim, &store, "div", true, mode)? {
                    if forced.applied_seq != f {
                        return Err(format!(
                            "--force restored to {} rather than the archived frontier {f}",
                            forced.applied_seq
                        ));
                    }
                    for _ in 0..CONV_VERIFY_ATTEMPTS {
                        match conv_verify_restored(
                            src.as_ref(),
                            "div",
                            sim,
                            records,
                            pending_jobs,
                            f,
                            mode,
                        ) {
                            Ok(()) => {
                                sim.cover("archive:diverged_forced_converged");
                                break;
                            }
                            Err(ConvFail::Fallout) => continue,
                            Err(ConvFail::Violation(msg)) => return Err(msg),
                        }
                    }
                }
            }
            Err(e) if is_fault_fallout(&e.to_string()) => {
                sim.cover("archive:diverged_probe_faulted");
            }
            Err(e) => {
                return Err(format!(
                    "restore onto a diverged timeline failed for an unexpected reason: {e}"
                ));
            }
        }
    }

    // ---- spec §9.10 / §4 rule 2: a second writer on one prefix stops
    // rather than interleaving. LAST, because planting the foreign manifest
    // replaces the generation pointer every oracle above reads.
    let Some(db) = src.as_mut() else {
        return Ok(());
    };
    conv_settle(&store);
    if !arc.stopped() && conv_plant_foreign_manifest(&store, m.generation + 1) {
        for _ in 0..CONV_FOREIGN_STEPS {
            let id = st.model.len() as u64;
            if db
                .transact(|tx| -> Result<(), std::convert::Infallible> {
                    tx.put(&Rec {
                        id: Id::from_raw(id),
                        value: id,
                    });
                    Ok(())
                })
                .is_ok()
            {
                st.model.push(id);
                st.jobs.push(false);
            }
            conv_step(sim, &mut arc, db, &store, &mut st)?;
            if arc.stopped() {
                break;
            }
        }
        if arc.stopped() {
            sim.cover("archive:foreign_writer_stopped");
        }
        // The claim that does not depend on the archive NOTICING: it never
        // advances a manifest it does not own. Checked either way.
        conv_settle(&store);
        match conv_read_manifest(&store, 16) {
            Some(now) if now.writer == FOREIGN_WRITER => {}
            Some(now) => {
                return Err(format!(
                    "this archive advanced a manifest owned by \"{FOREIGN_WRITER}\": it now \
                     names \"{}\" at generation {} (spec §4 rule 2)",
                    now.writer, now.generation
                ));
            }
            None => {
                return Err("the manifest is unreadable after the foreign-writer probe".to_string());
            }
        }
    }
    Ok(())
}

/// The durable-media-corruption axes are off here, and the reason is not the
/// one `replication.rs`'s `no_media_corruption` gives — it is narrower and
/// specific to what this property compares.
///
/// `bit_flip` and `misdirected_write` model bitrot of the ONLY LOCAL COPY
/// (`checks::durable_corruption_fired`: "unrecoverable by design; v0's answer
/// is M1 replication"). On the SOURCE that is a fine thing to fault and this
/// property tolerates it — the archive is the second copy, and it refuses to
/// ship a frame that fails verification rather than shipping corruption. On
/// the RESTORE DESTINATION it is something else: `restore` writes the whole
/// chain through `Db::apply_shipped`, so at a per-`write_at` rate these axes
/// silently corrupt the destination's own WAL, and the restored `Db` then
/// opens short, or not at all, for a reason that has nothing to do with the
/// archive. Both hosts write under `corruption_scope: "wal/"` and the scope
/// is a PATH prefix, not a host, so there is no way to fault one and not the
/// other. Faulting the disk a verdict is read off makes the verdict
/// unreadable, so the axes come off; the rest of the matrix — write, sync
/// and read errors, entry loss, the latency ranges — stays hot on both.
fn no_media_corruption_on_either_side(cfg: &mut FaultConfig) {
    cfg.bit_flip = 0.0;
    cfg.misdirected_write = 0.0;
    cfg.write_error /= CONV_DISK_ERROR_DIVISOR;
    cfg.sync_error /= CONV_DISK_ERROR_DIVISOR;
}

/// The flagship oracle (spec §9, §11).
///
/// Fixed base, 256 universes, the same shape `replication.rs`'s own
/// convergence property uses and for the same two reasons: a permanent
/// always-in-the-suite property must not flake batch to batch, and `floor:`
/// — a MEASURED rate rather than `require:`'s floor of one — is only
/// meaningful against a base that does not move. The evidence bar's "N fresh
/// seeds" is satisfied by running fresh bases and recording them (see
/// task-12-report.md §4), not by moving this one.
///
/// **The seed that taught this property what it was actually checking.**
/// Hard rule 3 — recorded here because the frozen runner exposes no public
/// single-seed replay (`run_universe` is `pub(crate)`) and reconstructing the
/// seed->config mapping in a test would fork a contract whose own doc says
/// "do not reorder anything":
///
/// ```text
/// CHARPENTE_SEED=0xb803ea7cab19d08c cargo test -p charpente --test archive -- --nocapture
/// ```
///
/// Found at base `0xD9E3_0001`; the env var overrides the base, so it
/// replays against THIS committed base in 0.02s. It used to fail the
/// negative-space edge in `conv_step`:
///
///   `Archive::step poisoned the Db` ... `"table store (redb): I/O error:
///   injected read error"`
///
/// Mechanism, measured rather than argued (`store_failed=true`,
/// `engine_poison=false`, `last_applied=Some(137)`): the archive's snapshot
/// path reads the Db's redb store (`db.snapshot()`, then `scan_raw_from` per
/// batch). redb latches `CheckedBackend.io_failed` on ANY backend read `Err`,
/// `Store::failed` is that latch, and `Db::poisoned` is
/// `engine.poisoned() || store_failed()`. So an injected read error on a read
/// only the archive issued latched the Db's store handle while the WAL was
/// intact.
///
/// That is the archive DISCOVERING a failing local disk, not causing one, and
/// spec §8's first bullet now says so and states the residual. `conv_step`
/// counts it (`archive:step_discovered_disk_fault`) instead of failing on it
/// — **but only when a disk-error axis actually fired during that step**
/// (`conv_disk_faults`), which is `checks::durable_corruption_fired`'s
/// doctrine narrowed from the universe to the step. A latch with the disk
/// quiet is the fourth path into `Db::poisoned()` and stays a violation. The
/// key is reported and deliberately NOT in `require:`: it is 0 in 256 at this
/// base and 2 in 768 across three fresh bases (`0xD9E3_0001`, `0x5B71_0001`,
/// `0x3C0D_0001` — both at the first), so requiring it would make a permanent
/// property flake on a rare fault (`floor:`'s own argument, run backwards).
/// The earlier reading of 1 in 768 was a LOWER BOUND: while the latch was
/// still fatal the batch stopped at the first occurrence and never counted
/// the second.
///
/// **What `conv_step` asserts across every step — `last_applied`, the write
/// gate, engine poison, and a disk-quiet store latch — no fault in this
/// property can fire any of them.** `Archive::step` takes `&Db`, so those are
/// regression guards on the read-only-ness that makes §8 true, and they go
/// red on a `&mut Db` in the archive's signature rather than on any universe.
/// Labelled, at the site and here, because anyone counting this property's
/// fault-injection oracles will otherwise count four that cannot fire.
/// `an_archive_io_failure_never_poisons_the_db` is the same kind of guard and
/// carries the same label.
#[test]
fn archive_converges() {
    run_universes_swarm_with!(
        ARCHIVE_CONVERGENCE_BASE,
        256,
        no_media_corruption_on_either_side,
        |sim: &mut Sim| archive_convergence_universe(sim, ConvMode::Unscoped),
        require: [
            "archive:restored",
            "archive:gap_closed_generation",
            "archive:lost_ack_retried",
            "archive:diverged_refused",
            "archive:verifying_gated",
            "archive:foreign_writer_stopped",
            "archive:generation_reclaimed",
            "archive:delete_failed",
            "archive:restored_after_delete_failure",
        ],
        floor: [
            ("archive:restored", CONV_RESTORED_FLOOR),
            ("archive:restored_substantial", CONV_SUBSTANTIAL_FLOOR),
            ("archive:chained", CONV_CHAINED_FLOOR),
        ],
        ceiling: [("universe:gave-up", CONV_GAVE_UP_CEILING)]
    );
}

/// The same property over a schema that carries a tenant-scoped,
/// secondary-indexed table, two tenants sharing every ledger id (hard rule 3:
/// Task 6c changed the archive's batch shape and `charpente restore` to carry
/// a scope, and no swarm could express a restore that drops one).
///
/// A SIBLING rather than a widening of `archive_converges` (R56). Registering
/// one more table costs one universe on `archive:restored` there — 218 of 219,
/// measured — because `logical_rows_checked` sweeps `db.table_names()` and one
/// more `scan_raw_from` per verification is one more chance for the matrix to
/// hit a read. The writes cost more: an extra row and a scoped index entry per
/// record means the archive ships more bytes at unchanged per-operation fault
/// rates, so the published frontier lands lower and both frontier-derived
/// floors fall with it. Those floors are rates against ONE workload, so the
/// scoped workload gets its own base and its own three numbers rather than
/// re-baselining numbers that are not decaying.
///
/// Its floors follow the same discipline as the original three: pinned to
/// EXACTLY the count measured at this property's own base, no margin
/// subtracted (`CONV_RESTORED_FLOOR`'s own history — "the floors below were
/// raised to the new pinned measurement rather than lowered to accommodate
/// anything"). A drop is a finding, not a number to lower; a fresh base will
/// trip them, and that is the design.
#[test]
fn archive_converges_with_a_scoped_table() {
    run_universes_swarm_with!(
        ARCHIVE_SCOPED_CONVERGENCE_BASE,
        256,
        no_media_corruption_on_either_side,
        |sim: &mut Sim| archive_convergence_universe(sim, ConvMode::Scoped),
        require: [
            "archive:restored",
            "archive:gap_closed_generation",
            "archive:lost_ack_retried",
            "archive:diverged_refused",
            "archive:verifying_gated",
            "archive:foreign_writer_stopped",
            "archive:generation_reclaimed",
            "archive:delete_failed",
            "archive:restored_after_delete_failure",
            "tenant:restored-multi-tenant",
        ],
        floor: [
            ("archive:restored", CONV_SCOPED_RESTORED_FLOOR),
            ("archive:restored_substantial", CONV_SCOPED_SUBSTANTIAL_FLOOR),
            ("archive:chained", CONV_SCOPED_CHAINED_FLOOR),
        ],
        ceiling: [("universe:gave-up", CONV_SCOPED_GAVE_UP_CEILING)]
    );
}

/// Rebuilds a universe of `archive_converges` exactly as the swarm runner
/// derives it: sampled config, extended fields, then the property's adjust.
fn conv_swarm_sim(seed: u64) -> Sim {
    let mut state = seed;
    let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
    let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
    charpente_sim::sample_extended_fields(seed, &mut cfg);
    no_media_corruption_on_either_side(&mut cfg);
    Sim::with_faults(seed, cfg)
}

/// Each of the four arms that ends a convergence universe before the oracle
/// ran gives up rather than passing, and keeps its own coverage key beside
/// the give-up (LEDGER 2026-09-30, follow-up 9). The source arm is forced (every
/// disk op faults); the other three are universes of `archive_converges`'s
/// pinned base, found by printing each arm's seed over that batch. The verify
/// arm was re-found that way on 2026-10-01 for S5 with D1, which re-key every
/// universe: `0x5228c3f6c429ed5e` no longer ends there, and `0xdcaa7c9885fba707`
/// (universe 28) is the first of the 12 that do.
#[test]
fn every_convergence_arm_that_reaches_no_verdict_gives_up() {
    // Control: the same seed with no faults reaches a verdict.
    let mut control = Sim::with_faults(0xA2C0_0901, FaultConfig::none());
    assert_eq!(
        archive_convergence_universe(&mut control, ConvMode::Unscoped),
        Ok(())
    );
    assert_eq!(
        control.gave_up(),
        None,
        "a fault-free universe reaches a verdict"
    );

    let source = Sim::with_faults(
        0xA2C0_0901,
        FaultConfig {
            read_error: 1.0,
            write_error: 1.0,
            sync_error: 1.0,
            ..FaultConfig::none()
        },
    );
    let cases = [
        (
            source,
            "archive:source_never_opened",
            "the source never opened",
        ),
        (
            conv_swarm_sim(0x30ca_c479_4b7a_961b),
            "archive:nothing_published",
            "no manifest was ever read back",
        ),
        (
            conv_swarm_sim(0x7cc5_c550_10b6_4fd8),
            "archive:restore_never_completed",
            "the restore never completed",
        ),
        (
            conv_swarm_sim(0xdcaa_7c98_85fb_a707),
            "archive:verify_never_read_cleanly",
            "the restored destination never read cleanly",
        ),
    ];
    for (mut sim, key, why) in cases {
        let seed = sim.seed();
        assert_eq!(
            archive_convergence_universe(&mut sim, ConvMode::Unscoped),
            Ok(()),
            "seed {seed:#x}"
        );
        assert!(
            sim.coverage().contains_key(key),
            "seed {seed:#x} must still end at {key}, or it no longer pins that arm: {:?}",
            sim.coverage()
        );
        let reason = sim
            .gave_up()
            .unwrap_or_else(|| panic!("seed {seed:#x} ended at {key} and must give up, not pass"));
        assert!(reason.contains(why), "seed {seed:#x}: {reason}");
    }
}

/// The budget pin. `CONV_RECORDS_PER_SEGMENT` is what the outage window and
/// the pin bound are counted in, so a frame-size change that stopped the
/// universes sealing anything would otherwise leave the property green and
/// empty. Asserted against a freshly built fixture — the thing — never
/// against a transcription of the number.
#[test]
fn conv_segment_layout_is_what_the_budget_assumes() {
    let sim = Sim::with_faults(0x636f_6e76_5f6c_6179, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.host("src").disk()),
        conv_db_config(),
        conv_schema(),
    )
    .unwrap();
    assert_eq!(db.last_applied(), None, "a fresh Db has committed nothing");
    for i in 0..8 * CONV_RECORDS_PER_SEGMENT {
        db.transact(|tx| -> Result<(), std::convert::Infallible> {
            tx.put(&Rec {
                id: Id::from_raw(i),
                value: i,
            });
            tx.emit(Ping { n: WireU64(i) });
            Ok(())
        })
        .unwrap();
    }
    let sealed = db.sealed_segments().unwrap();
    assert!(
        sealed.len() >= 6,
        "8 * CONV_RECORDS_PER_SEGMENT commits sealed only {} segment(s): the constant is \
         stale and every budget counted in segments is wrong. Sealed: {sealed:?}",
        sealed.len()
    );
    // Steady state, skipping segment 0 (schema adoption folds an extra
    // effect into the first commit, so it holds one fewer — the same
    // asymmetry FIRST_SEGMENT_RECORDS records at 64 KiB).
    let steady = sealed[1].layout_end - sealed[1].base_seq + 1;
    assert_eq!(
        steady, CONV_RECORDS_PER_SEGMENT,
        "a steady-state segment holds {steady} records, not the \
         CONV_RECORDS_PER_SEGMENT this file budgets in"
    );
}

mod fs_object_store {
    #![allow(clippy::disallowed_methods, clippy::disallowed_types)]

    use std::io;
    use std::path::PathBuf;

    use charpente::prod::{FsObjectStore, ProdEntropy};
    use charpente_core::io::{Entropy, ObjectDone, ObjectOp, ObjectStore};

    /// A fresh, empty directory nobody else is using. `FsObjectStore::new`
    /// checks its parent and does not create it — a store pointed at an
    /// absent bucket must answer NotFound, not blow up — so the ones that
    /// need it created let a PUT do the creating.
    fn tmp(tag: &str) -> PathBuf {
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        std::env::temp_dir().join(format!(
            "charpente-fsobj-{tag}-{:016x}",
            u64::from_le_bytes(suffix)
        ))
    }

    /// Bounded, exactly as `SimObjectStore`'s own test drain is bounded and
    /// for the same reason (spec §4b): `poll`'s `None` means "still in
    /// flight", so a store answering `None` forever is a broken store, and
    /// an unbounded spin here would hang with no seed and no repro line.
    fn drain(s: &FsObjectStore) -> io::Result<ObjectDone> {
        for _ in 0..10_000 {
            if let Some(r) = s.poll() {
                return r;
            }
        }
        panic!("drain: ObjectStore::poll returned None 10,000 times in a row — stuck in flight");
    }

    fn put(s: &FsObjectStore, key: &str, data: &[u8]) -> io::Result<()> {
        s.start(ObjectOp::Put {
            key: key.to_string(),
            data: data.to_vec(),
        })
        .unwrap();
        drain(s).map(|d| assert!(matches!(d, ObjectDone::Put)))
    }

    fn get(s: &FsObjectStore, key: &str) -> io::Result<Vec<u8>> {
        s.start(ObjectOp::Get {
            key: key.to_string(),
        })
        .unwrap();
        match drain(s)? {
            ObjectDone::Get(v) => Ok(v),
            other => panic!("expected Get, got {other:?}"),
        }
    }

    fn list(s: &FsObjectStore, prefix: &str) -> Vec<String> {
        s.start(ObjectOp::List {
            prefix: prefix.to_string(),
        })
        .unwrap();
        match drain(s).unwrap() {
            ObjectDone::List(v) => v,
            other => panic!("expected List, got {other:?}"),
        }
    }

    // ---------------------------------------- T1's contract, re-run here

    #[test]
    fn put_then_get_roundtrips() {
        let root = tmp("roundtrip");
        let s = FsObjectStore::new(root.clone()).unwrap();
        put(&s, "bk/000/segments/1.log", &[1, 2, 3]).unwrap();
        assert_eq!(get(&s, "bk/000/segments/1.log").unwrap(), vec![1, 2, 3]);
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// On the real filesystem: a store over a bucket root whose parent does
    /// not exist is refused when it is built, by name, with nothing created.
    /// Not left to the first PUT: an archive reads the missing root as a
    /// fresh bucket and absorbs every refused PUT, so the node would run
    /// with no backup and nothing on stderr.
    #[test]
    fn a_bucket_root_whose_parent_is_missing_is_refused_when_the_store_is_built() {
        let missing = tmp("missing-parent");
        let root = missing.join("bucket");
        let Err(err) = FsObjectStore::new(root.clone()) else {
            panic!("a store was built over a bucket root whose parent is missing")
        };
        assert_eq!(err.kind(), io::ErrorKind::NotFound, "{err}");
        assert_eq!(
            err.to_string(),
            format!(
                "the bucket root \"{root}\" has no parent directory: \"{parent}\" is not an \
                 existing directory, and charpente never creates a directory above the bucket \
                 root — an acknowledged PUT rests on every directory entry above it, and \
                 charpente vouches only for the entries it creates and fsyncs. Create \
                 \"{parent}\" when you provision the machine and make it durable before the \
                 node starts (for example, run `sync` after creating it), or point at a \
                 bucket root whose parent exists",
                root = root.display(),
                parent = missing.display()
            )
        );
        assert!(
            !missing.exists(),
            "the refusal created {}",
            missing.display()
        );

        // A parent that exists as a regular file is no directory either: a
        // check for mere existence would build this store, and every PUT
        // would then fail "Not a directory" and be absorbed, silently.
        let file = tmp("file-parent");
        std::fs::write(&file, b"").unwrap();
        let root = file.join("bucket");
        let built = FsObjectStore::new(root.clone());
        let still_a_file = file.is_file();
        std::fs::remove_file(&file).unwrap();
        let Err(err) = built else {
            panic!("a store was built over a bucket root whose parent is a regular file")
        };
        assert_eq!(err.kind(), io::ErrorKind::NotFound, "{err}");
        assert_eq!(
            err.to_string(),
            format!(
                "the bucket root \"{root}\" has no parent directory: \"{parent}\" is not an \
                 existing directory, and charpente never creates a directory above the bucket \
                 root — an acknowledged PUT rests on every directory entry above it, and \
                 charpente vouches only for the entries it creates and fsyncs. Create \
                 \"{parent}\" when you provision the machine and make it durable before the \
                 node starts (for example, run `sync` after creating it), or point at a \
                 bucket root whose parent exists",
                root = root.display(),
                parent = file.display()
            )
        );
        assert!(still_a_file, "the refusal replaced {}", file.display());
    }

    /// An empty path names no bucket root: refused when the store is built,
    /// where it used to panic inside the PUT the kernel's thread polls.
    #[test]
    fn an_empty_bucket_root_is_refused_when_the_store_is_built() {
        let Err(err) = FsObjectStore::new("") else {
            panic!("a store was built over an empty bucket root")
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{err}");
        assert_eq!(
            err.to_string(),
            "the bucket root path is empty — name the directory to use (a relative path is \
             taken from the working directory)"
        );
    }

    /// A parent removed after the store was built: every PUT still refuses
    /// the root by name, and creates nothing (GUIDE's archive section).
    #[test]
    fn a_put_under_a_bucket_root_whose_parent_was_removed_is_refused_by_name() {
        let missing = tmp("removed-parent");
        std::fs::create_dir(&missing).unwrap();
        let root = missing.join("bucket");
        let s = FsObjectStore::new(root.clone()).unwrap();
        std::fs::remove_dir(&missing).unwrap();
        let first = put(&s, "p/k", b"v").expect_err("a PUT created the bucket root's parent");
        let again =
            put(&s, "p/k", b"v").expect_err("a retried PUT created the bucket root's parent");
        let exists = missing.exists();
        let _ = std::fs::remove_dir_all(&missing);
        assert_eq!(first.kind(), io::ErrorKind::NotFound, "{first}");
        assert_eq!(
            first.to_string(),
            format!(
                "object PUT \"p/k\" failed: the bucket root \"{root}\" has no parent \
                 directory: \"{parent}\" is not an existing directory, and charpente never \
                 creates a directory above the bucket root — an acknowledged PUT rests on \
                 every directory entry above it, and charpente vouches only for the entries \
                 it creates and fsyncs. Create \"{parent}\" when you provision the machine \
                 and make it durable before the node starts (for example, run `sync` after \
                 creating it), or point at a bucket root whose parent exists",
                root = root.display(),
                parent = missing.display()
            )
        );
        assert_eq!(again.to_string(), first.to_string());
        assert!(!exists, "the refusal created {}", missing.display());
    }

    #[test]
    fn list_is_sorted_and_prefix_scoped() {
        let root = tmp("list");
        let s = FsObjectStore::new(root.clone()).unwrap();
        for k in ["p/b", "p/a", "q/c"] {
            put(&s, k, &[]).unwrap();
        }
        assert_eq!(list(&s, "p/"), vec!["p/a".to_string(), "p/b".to_string()]);
        assert_eq!(
            list(&s, ""),
            vec!["p/a".to_string(), "p/b".to_string(), "q/c".to_string()],
            "an empty prefix lists the whole bucket"
        );
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn get_absent_key_is_not_found_and_names_the_key() {
        let s = FsObjectStore::new(tmp("absent")).unwrap();
        let e = get(&s, "nope").unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::NotFound);
        assert!(
            e.to_string().contains("nope"),
            "error must name the key: {e}"
        );
    }

    #[test]
    fn delete_removes_the_object() {
        let root = tmp("delete");
        let s = FsObjectStore::new(root.clone()).unwrap();
        put(&s, "a", &[9]).unwrap();
        s.start(ObjectOp::Delete { key: "a".into() }).unwrap();
        assert!(matches!(drain(&s).unwrap(), ObjectDone::Delete));
        assert_eq!(
            get(&s, "a").unwrap_err().kind(),
            io::ErrorKind::NotFound,
            "delete did not remove the object"
        );
        // Deleting an absent key is not an error — same as `SimObjectStore`,
        // and the archive's own retry paths depend on it.
        s.start(ObjectOp::Delete { key: "a".into() }).unwrap();
        assert!(matches!(drain(&s).unwrap(), ObjectDone::Delete));
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    #[should_panic(expected = "operation already in flight")]
    fn second_start_before_poll_asserts() {
        let s = FsObjectStore::new(tmp("inflight")).unwrap();
        s.start(ObjectOp::Get { key: "a".into() }).unwrap();
        let _ = s.start(ObjectOp::Get { key: "b".into() });
    }

    // ------------------------------- the atomic-PUT contract, for real

    /// The contract that distinguishes `ObjectStore` from `Disk` (spec §1),
    /// proven against the mechanism that makes a filesystem able to honour
    /// it: write `<key>.tmp`, then rename.
    ///
    /// The failure is injected by making the bucket directory read-only, so
    /// creating `<key>.tmp` fails — a real `EACCES` from the kernel, not a
    /// test hook. The brief's sketch named a kill between the write and the
    /// rename; there is no way to time that reliably from a single-threaded
    /// test without forking, and this failure lands in the same window with
    /// the same observable consequence. What it costs: it proves the PUT
    /// never REACHED the destination key, not that a half-written one was
    /// discarded. The residue test below covers that half.
    ///
    /// This is the oracle for mutation 1 (write straight to `<key>`): an
    /// existing file can be opened for writing without write permission on
    /// its directory, so the mutation survives the chmod and clobbers the
    /// object.
    #[test]
    fn put_is_atomic_under_a_failed_rename() {
        use std::os::unix::fs::PermissionsExt;

        let root = tmp("atomic");
        let s = FsObjectStore::new(root.clone()).unwrap();
        put(&s, "a", &[1; 4096]).unwrap();

        crate::support::chmod_that_bites(&root, 0o555);
        let e = put(&s, "a", &[2; 4096]).unwrap_err();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(
            e.to_string().contains('a'),
            "a failed PUT must name the key: {e}"
        );
        assert_eq!(
            get(&s, "a").unwrap(),
            vec![1; 4096],
            "a torn or clobbered object escaped the rename"
        );
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// The other half of the contract: a failed PUT to a key that never
    /// existed leaves it ABSENT, not present-and-empty.
    #[test]
    fn a_failed_put_to_a_fresh_key_leaves_it_absent() {
        use std::os::unix::fs::PermissionsExt;

        let root = tmp("fresh");
        let s = FsObjectStore::new(root.clone()).unwrap();
        put(&s, "seed", &[0]).unwrap(); // creates the bucket directory

        crate::support::chmod_that_bites(&root, 0o555);
        assert!(put(&s, "new", &[1, 2, 3]).is_err());
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(
            get(&s, "new").unwrap_err().kind(),
            io::ErrorKind::NotFound,
            "a failed PUT must not create the key"
        );
        assert_eq!(
            list(&s, ""),
            vec!["seed".to_string()],
            "a failed PUT must not appear in the bucket"
        );
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// What a crash between the write and the rename actually leaves on
    /// disk: a `<key>.tmp` holding the new bytes and `<key>` holding the old
    /// ones. Neither `Get` nor `List` may ever surface that residue — a
    /// restore that saw `<key>.tmp` in a `List` would treat it as an object
    /// the manifest never named.
    #[test]
    fn a_temp_left_by_a_crash_is_never_served_or_listed() {
        let root = tmp("residue");
        let s = FsObjectStore::new(root.clone()).unwrap();
        put(&s, "gen/seg", &[1; 64]).unwrap();
        std::fs::write(root.join("gen/seg.tmp"), [2u8; 64]).unwrap();

        assert_eq!(
            get(&s, "gen/seg").unwrap(),
            vec![1; 64],
            "the object's own value must be untouched by the residue"
        );
        assert_eq!(
            list(&s, ""),
            vec!["gen/seg".to_string()],
            "a crash's temp file was listed as an object"
        );
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// Keys ending in `.tmp` are reserved by this implementation: it is the
    /// name the atomic PUT writes through, so accepting one would let a
    /// caller's object be mistaken for another's crash residue.
    #[test]
    #[should_panic(expected = "reserves")]
    fn a_key_ending_in_tmp_is_refused() {
        let s = FsObjectStore::new(tmp("reserved")).unwrap();
        let _ = s.start(ObjectOp::Get {
            key: "a/b.tmp".into(),
        });
    }

    /// Step 4 of the atomic PUT — the fsync of the parent directory that
    /// makes the rename durable — on a real filesystem. `File::open(dir,
    /// O_RDONLY)` needs read permission on the directory, so a parent at
    /// `0o333` (write and traverse, no read) fails that one call and only
    /// that one: creating `<key>.tmp` needs w+x, writing it needs neither,
    /// and the rename needs w+x.
    ///
    /// It pins presence, call site and the error's words, not durability:
    /// removing the fsync was a GREEN mutation for every test in this module
    /// (measured at review, 11/0). Durability is held by the crash model in
    /// `charpente::prod`'s tests (`CrashFs`), which cuts the power after the
    /// PUT and goes red without this fsync or any other on the way down to
    /// the key (D2, 2026-10-01).
    #[test]
    fn a_put_that_cannot_fsync_its_directory_reports_the_failure() {
        use std::os::unix::fs::PermissionsExt;

        let root = tmp("dirsync");
        let s = FsObjectStore::new(root.clone()).unwrap();
        put(&s, "k", &[1; 16]).unwrap(); // creates the bucket directory

        crate::support::chmod_that_bites(&root, 0o333);
        let result = put(&s, "k", &[2; 16]);
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();

        let e = result.expect_err(
            "a PUT whose directory fsync fails must report it — an acked PUT whose new \
             directory entry is not durable is an object that can vanish across a crash",
        );
        assert_eq!(e.kind(), io::ErrorKind::PermissionDenied, "{e}");
        let msg = e.to_string();
        assert!(
            msg.contains(&format!("could not fsync directory \"{}\"", root.display()))
                && msg.contains("ObjectStore's contract"),
            "a failed directory fsync must name the directory and the contract: {msg}"
        );

        // The THIRD outcome the trait's two-outcome sentence does not list:
        // this PUT failed and the key holds the NEW value. Benign, and
        // exactly archive spec §7's "PUT lands but the ack is lost" row —
        // the archive's retry is idempotent. And the GET that sees it has
        // made it durable first: an object a GET returns is durable (the
        // trait's contract), whoever reads it. Pinned so it is a known
        // property of this impl rather than a surprise.
        assert_eq!(
            get(&s, "k").unwrap(),
            vec![2; 16],
            "the rename had already happened; only the directory fsync failed"
        );
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// Spec §4b bounds every `ObjectStore` drive loop at 10,000 polls. That
    /// bound is an ITERATION COUNT, not a timeout, and it is harmless only
    /// while stores complete on the first poll. This store does — pinned
    /// here so the next store's author has to change a test to change the
    /// assumption, rather than inheriting it silently.
    #[test]
    fn every_op_completes_on_the_first_poll() {
        let root = tmp("firstpoll");
        let s = FsObjectStore::new(root.clone()).unwrap();
        for op in [
            ObjectOp::Put {
                key: "k".into(),
                data: vec![7; 128],
            },
            ObjectOp::Get { key: "k".into() },
            ObjectOp::List { prefix: "".into() },
            ObjectOp::Delete { key: "k".into() },
            ObjectOp::Get { key: "gone".into() },
        ] {
            s.start(op).unwrap();
            assert!(
                s.poll().is_some(),
                "FsObjectStore must complete on the FIRST poll — spec §4b's bound is \
                 an iteration count, and a store that needs a second poll makes it live"
            );
        }
        std::fs::remove_dir_all(&root).unwrap();
    }
}
