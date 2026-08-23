//! Object-storage archive (M2, spec docs/superpowers/specs/2026-08-20-
//! object-storage-archive-design.md): ships sealed WAL segments and
//! logical snapshots to object storage behind a generation-pointer
//! manifest (§2), and restores from it (§6). Task 4 landed the manifest and
//! the key layout with nothing uploading yet; Task 5 landed the engine that
//! ships sealed segments (§3's first half); Task 6 lands the snapshot
//! (§3's other half) and the ordering that is the whole safety argument.
//! Restore (§6) is still to come.
//!
//! **The manifest swaps last.** Inter-key ordering is not guaranteed by
//! `ObjectStore` (§1), so the manifest swap is the only operation whose
//! completion means anything: every object a generation contains is PUT
//! first, and the manifest naming them PUT after. A crash at any earlier
//! point therefore leaves an older *complete* generation still pointed at,
//! never a torn one. Every transition below is arranged around that and
//! nothing here may be reordered without breaking it.
//!
//! **A generation carries exactly one snapshot** (§2: "one full logical
//! snapshot plus the chain of sealed segments continuing it"), so the
//! archive takes its generation's snapshot *before* it ships anything else
//! and writes no manifest until that snapshot has landed. Two consequences
//! worth stating because neither is obvious from the code:
//!
//! - A manifest never names a frontier no snapshot backs. `Manifest::
//!   applied_seq` is a plain `u64` with no room for "nothing was applied",
//!   so if `Snapshot::applied_seq` is `None` the archive does not snapshot
//!   at all rather than fabricating a 0 that restore would reason from.
//! - The generation's snapshot objects are therefore written **once**. A
//!   *refreshed* snapshot cannot be written in place: it would overwrite
//!   `<gen>/snapshot/**` while the manifest still names the old frontier,
//!   making a second mutable key in a layout whose safety argument is that
//!   the manifest is the only one (§2), and exposing exactly the torn
//!   generation this task exists to prevent. Refresh is therefore a *new
//!   generation*, and generation transitions land at Task 7 alongside the
//!   bounded pin and the gap. Until then `snapshot_every` gates when the
//!   generation's opening snapshot is taken and nothing more. **Carried to
//!   Task 7**, with the same reasoning: a restart re-opens generation 0 and
//!   would re-snapshot into its keys, so T7's generation management (and
//!   §4's writer-identity check) must either resume the generation the
//!   manifest already names or open a fresh one — Task 5's "always writes
//!   fresh" is idempotent for segment objects, whose bytes are a function
//!   of their key, but not for a snapshot, whose bytes are a function of
//!   when it was taken.
//!
//! **Cadence is seq-driven, and structurally so** (§3). `step` receives no
//! clock, `Db` exposes none, and nothing in this module names one — so
//! clock skew is a non-issue here by construction rather than by handling,
//! and there is no "drive the cadence off the wall clock" defect that could
//! be written in the first place. `the_archive_module_names_no_clock`
//! guards the thing that would have to change first.
//!
//! **Memory** (§10): the snapshot pages through `Snapshot::scan_raw_from`
//! into one `<gen>/snapshot/<table>/<n>` object at a time and never holds a
//! whole table. `scan_raw` returns a table at once and must not appear here
//! — the archive snapshots on a cadence rather than once per join, so it
//! meets a ceiling seeding never does.
//!
//! `Archive::step` is a state machine, one transition per call, the same
//! shape `Repl` already has (DESIGN §5): `Idle` opens the generation's
//! snapshot if it has none, otherwise picks the lowest sealed,
//! un-uploaded WAL segment; `Snapshotting` pages one batch of one table out
//! of the single MVCC `Db::snapshot` view, walking `Db::table_names()` in
//! order and finishing with `snapshot/meta` (which carries the per-table
//! batch counts, so the generation's object set is enumerable rather than
//! guessable); `Reading` pulls a segment through `Db::read_log` (so
//! the CRC is re-verified and nothing above `last_applied` travels) a
//! `read_budget_bytes`-sized bite at a time, re-framed into the same
//! `[seq][len][crc][payload]` shape the WAL itself uses (`frame_crc`) so a
//! downloaded object is byte-identical to what `Wal::open` would recover
//! from the original segment (spec §3: "verbatim"); `UploadingSegment`
//! PUTs the reassembled bytes; `UploadingManifest` swaps the manifest to
//! reference the new segment, the one mutable key and the archive's own
//! commit point (spec §2). A failed PUT retries with the identical key and
//! bytes — never a fresh one — which is what keeps a lost-ack retry from
//! duplicating an object (spec §9.2).
//!
//! The retention pin (`Db::add_retention_pin`) is **unbounded this task**
//! (ruling 23 carry-forward): it holds at the lowest seq this archive still
//! needs — the lowest sealed segment not yet reflected in an uploaded
//! manifest — with no bound on how far behind that seq may be.
//!
//! **The pin is held from `open` and is never released outright**, not while
//! idle, not while waiting on an unapplied segment, and never to `u64::MAX`.
//! That is load-bearing and was a real bug once (M1): a pin that reads
//! `u64::MAX` is a pin that holds *nothing*, so `Wal::gc` was free to delete
//! the very segment `next_base()` was waiting for. After that deletion
//! `step_idle`'s `find` never matches again — the archive ships nothing
//! forever, returns no error, and reports itself caught up. Under the
//! default `checkpoint_every` that cost 50 of 50 segments, silently. Because
//! the pin now always holds, `Wal::gc` genuinely cannot remove a segment out
//! from under this archive, so a "gap" (spec §2) is structurally impossible
//! this task and `gapped()` is always `false`.
//!
//! Ask `Archive::idle` whether there is work left — never `pin_floor()`,
//! which no longer has a caught-up sentinel and never should have been read
//! as one. The bounded pin that makes a gap possible, and the
//! writer-identity ownership check (spec §4), both land at Task 7.

use std::io;
use std::sync::{Arc, Mutex};

use charpente_core::db::Db;
use charpente_core::io::{ObjectDone, ObjectOp, ObjectStore};
use charpente_core::log::frame_crc;
use charpente_core::table::Snapshot;

pub mod manifest;

use manifest::{
    Manifest, SegmentRef, SnapshotMeta, TableBatches, encode_snapshot_batch, manifest_key,
    segment_key, snapshot_batch_key, snapshot_meta_key,
};

/// Rows per `<gen>/snapshot/<table>/<n>` object, and with it the archive's
/// peak snapshot memory: O(batch) rather than O(store) (spec §10). Mirrors
/// `DbConfig::migration_batch_size`'s default of 1,000, whose own comment
/// states the rule this needs — "batching must bound memory, not just
/// record size" — rather than inventing a second number for the same job.
///
/// Deliberately a constant rather than an `ArchiveConfig` field: it bounds
/// an internal buffer, not a durable layout decision, and nothing in the
/// bucket records it. A generation written at one batch size is read back
/// identically at another, because `SnapshotMeta` records how many objects
/// each table was actually paged into.
const SNAPSHOT_BATCH_ROWS: usize = 1_000;

/// Archive engine configuration (spec §3, §4, §5).
#[derive(Clone, Debug)]
pub struct ArchiveConfig {
    /// Bucket key prefix this archive owns (spec §2).
    pub prefix: String,
    /// This node's identity, recorded in every manifest this archive writes
    /// (spec §4's ownership rule): a future writer that finds the manifest
    /// advanced by a different identity is meant to stop rather than
    /// interleave generations. Task 5 always writes fresh (it never reads
    /// an existing manifest before writing), so nothing yet consults this
    /// on the read side — that arrives with restore/ownership at T6/T7.
    pub writer: String,
    /// Cadence for logical snapshots, in committed records — mirrors
    /// `DbConfig::checkpoint_every` (spec §3), and seq-driven rather than
    /// time-driven so that replay never depends on a wall clock.
    ///
    /// It gates when this generation's opening snapshot is taken: the
    /// archive uploads nothing at all until `last_applied` has crossed it,
    /// because a manifest with no snapshot behind it names a generation
    /// that does not exist. `0` means "no cadence configured" — snapshot as
    /// soon as there is a frontier to name.
    ///
    /// A generation carries exactly one snapshot (module docs), so a *later*
    /// multiple does not re-snapshot in place; refreshing means opening a
    /// new generation, which lands at Task 7.
    pub snapshot_every: u64,
    /// The archive's retention-pin bound, in segments (spec §5): 4x
    /// `DbConfig::retain_segments`'s default of 2, bounding the WAL held
    /// above the checkpoint floor at 512 MiB (64 MiB segments) during an
    /// object-store outage. **Unused this task** (ruling 23 carry-forward):
    /// Task 5's pin is unbounded by design; the bound lands at Task 7.
    pub pin_segments: usize,
    /// Bounds `Db::read_log`'s PAYLOAD accounting for one call while
    /// shipping a segment (`bytes += flen`, log.rs:349) — never a
    /// `SegmentRef`'s span (ruling 29), and not the buffer allocation:
    /// `step_reading`'s own re-framed `buf` carries an additional
    /// `HEADER_BYTES` (16) per frame on top of the payload, so one `step`
    /// can allocate somewhat more than this bound, never less. Default
    /// 8 MiB: at the 64 MiB production `WalConfig::segment_bytes` that's 8
    /// `step`s per segment, so no single `step` reads the whole 64 MiB —
    /// which matters once `step` becomes a kernel arm (Task 9), where a
    /// long step is a stalled kernel. Smaller means more steps per
    /// segment, never a shorter `SegmentRef`.
    pub read_budget_bytes: usize,
}

/// Where the generation's snapshot walk has got to. Holds the ONE
/// `Db::snapshot` view for the whole walk (spec §3: one MVCC snapshot,
/// paged reads) — every table is read at the same frontier, or the
/// generation would be a mixture of frontiers no `applied_seq` describes.
struct SnapshotWalk {
    /// The single MVCC view. Owned, not borrowed: `Archive::step` takes a
    /// fresh `&Db` each call, so a view that lived across calls could not
    /// borrow from any one of them (ruling 32's shape, one level down).
    snapshot: Snapshot,
    /// Read off `snapshot` itself, never `Db::last_applied` separately —
    /// they can already differ by the time the walk starts, and the
    /// manifest's frontier has to be the one the rows came from.
    applied_seq: u64,
    next_job_id: u64,
    /// `Db::table_names()`, schema-sorted, including the platform-owned
    /// tables (jobs, events, schema versions) whose rows restore needs.
    tables: Vec<&'static str>,
    /// Index into `tables`; `== tables.len()` once every one is paged, at
    /// which point only `snapshot/meta` is left.
    table: usize,
    /// `scan_raw_from`'s cursor within `tables[table]`.
    after_id: Option<u64>,
    /// Next batch index `n` for `tables[table]`.
    batch: u64,
    /// Batch counts for the tables already walked, in walk order — becomes
    /// `SnapshotMeta::tables`.
    done: Vec<TableBatches>,
}

/// One transition of `Archive::step`'s state machine (module docs).
enum State {
    Idle,
    /// Paging the generation's snapshot out, one batch object per step.
    Snapshotting {
        walk: SnapshotWalk,
    },
    UploadingSnapshotBatch {
        walk: SnapshotWalk,
        key: String,
        bytes: Vec<u8>,
        started: bool,
    },
    /// The snapshot's LAST object. Its completion is what makes the
    /// generation's snapshot whole, and only then may the manifest name it.
    UploadingSnapshotMeta {
        meta: SnapshotMeta,
        bytes: Vec<u8>,
        started: bool,
    },
    Reading {
        base_seq: u64,
        /// The completeness TARGET for this segment (`SealedSegment::
        /// layout_end`) — never itself recorded as `end_seq`; see module
        /// docs and ruling 29.
        layout_end: u64,
        /// Next seq to request from `Db::read_log`.
        next_seq: u64,
        /// Re-framed WAL bytes accumulated so far.
        buf: Vec<u8>,
    },
    UploadingSegment {
        base_seq: u64,
        /// The last seq ACTUALLY read and framed into `bytes` — ruling 29's
        /// whole point: this, never `layout_end`, is what gets recorded.
        end_seq: u64,
        key: String,
        bytes: Vec<u8>,
        /// Whether `store.start` has been called for the current attempt;
        /// `false` means the next step must (re)issue `start` before it can
        /// `poll`.
        started: bool,
    },
    UploadingManifest {
        bytes: Vec<u8>,
        started: bool,
        /// `Some` when this manifest is the one that commits the
        /// generation's opening snapshot. The archive records the snapshot
        /// as its own only when this PUT lands, because the manifest swap
        /// is the archive's commit point (spec §2) — not when the snapshot
        /// objects themselves landed.
        commits: Option<SnapshotMeta>,
    },
}

/// Starts or polls ONE PUT — the single IO operation a `step` is allowed
/// (spec §4). `Ok(true)` means the object landed; `Ok(false)` means it was
/// just started or is still in flight; `Err` means retry, with `started`
/// left `false` so the caller's next step re-issues `start` for the SAME key
/// with the SAME bytes. Never a fresh key: that is what keeps a lost-ack
/// retry from duplicating an object (spec §9.2), and it is a property of
/// every upload arm rather than of any one of them, which is why it lives
/// here instead of being restated four times.
fn put_step(
    store: &dyn ObjectStore,
    key: &str,
    bytes: &[u8],
    started: &mut bool,
) -> io::Result<bool> {
    if !*started {
        store.start(ObjectOp::Put {
            key: key.to_string(),
            data: bytes.to_vec(),
        })?;
        *started = true;
        return Ok(false);
    }
    match store.poll() {
        None => Ok(false),
        Some(Ok(ObjectDone::Put)) => {
            *started = false;
            Ok(true)
        }
        Some(Ok(other)) => unreachable!(
            "a Put was started; only ObjectDone::Put or an error can complete it, got {other:?}"
        ),
        Some(Err(e)) => {
            *started = false;
            Err(e)
        }
    }
}

/// The archive engine for one node (module docs). Borrows `Db`/`ObjectStore`
/// one `step` at a time rather than owning either (ruling 32): `ObjectStore`
/// is `Send` but not `Sync`-bound, and the sim fixture's store is not `Send`
/// at all, so nothing here can hold either behind an `Rc`/`Arc`.
pub struct Archive {
    cfg: ArchiveConfig,
    /// Shared with the closure `Db::add_retention_pin` holds — the ONLY
    /// channel back into `Db` this struct has (ruling 1/32: `open` takes
    /// `&mut Db` just long enough to register it).
    pin: Arc<Mutex<u64>>,
    /// This archive's generation counter (spec §2). Always 0 through Task
    /// 6: nothing here ever closes a generation or opens a second one (both
    /// need the bounded pin, T7), and nothing reads a prior one back (T7).
    generation: u64,
    /// This generation's snapshot, recorded only once the manifest naming
    /// it has landed. `None` means the generation has no snapshot yet, and
    /// while it is `None` the archive ships nothing at all: a manifest with
    /// no snapshot behind it names a frontier restore would reason from and
    /// no object backs (module docs). Every later manifest this archive
    /// writes repeats these values, since a generation carries exactly one
    /// snapshot.
    snapshot: Option<SnapshotMeta>,
    /// The manifest's segment chain as this archive has built it so far,
    /// oldest first, contiguous (`Manifest::encode` asserts this — hard
    /// rule 4). The next un-shipped segment's expected base_seq is
    /// `segments.last().end_seq + 1`, or 0 before anything ships.
    segments: Vec<SegmentRef>,
    state: State,
    /// Set by the two `step_idle` arms that find no available work,
    /// cleared by every `step`. Backs `Archive::idle`.
    idle: bool,
}

impl Archive {
    /// Registers the retention pin on `db` (mirrors `Repl::open`,
    /// repl/mod.rs:254-257) and returns ready to `step`. Never reads the
    /// store — `step` is the only method that ever touches it (ruling 32),
    /// so `open` cannot fail on the store's account and never does.
    pub fn open(cfg: ArchiveConfig, db: &mut Db) -> io::Result<Archive> {
        // Hold from the moment the pin is registered (M1). Starting at
        // `u64::MAX` left a window between `open` and the first pinning
        // `step` in which `Wal::gc` was free to delete the very segment
        // `next_base()` would later look for — after which `step_idle`'s
        // `find` never matches again and the archive ships nothing,
        // forever, reporting no error.
        let pin = Arc::new(Mutex::new(db.log_base()));
        db.add_retention_pin(Box::new({
            let floor = Arc::clone(&pin);
            move || *floor.lock().expect("uncontended")
        }));
        Ok(Archive {
            cfg,
            pin,
            generation: 0,
            snapshot: None,
            segments: Vec::new(),
            state: State::Idle,
            idle: false,
        })
    }

    /// The retention floor this archive holds: the lowest seq it still
    /// needs, which once caught up is the base the next segment to seal
    /// will start at. **Never `u64::MAX`** — this archive holds a floor
    /// from `open` onward and never releases outright (M1); use
    /// `Archive::idle` to ask whether there is work left, never this.
    /// A segment counts as "reflected" only once the manifest PUT
    /// naming it has itself succeeded — not merely once its own bytes
    /// landed — so retention never releases a segment the manifest doesn't
    /// yet acknowledge (spec §2: the manifest swap is the commit point).
    pub fn pin_floor(&self) -> u64 {
        *self.pin.lock().expect("uncontended")
    }

    /// True when the last `step` found nothing it could do: either
    /// nothing is sealed past what has been archived, or the one sealed
    /// segment left is not yet fully applied. Use it to stop stepping.
    /// **Not a durability signal** — it says the archive has no work it
    /// can do right now, never that everything reached the store.
    pub fn idle(&self) -> bool {
        self.idle
    }

    /// True once a gap has closed this generation (spec §2). Always
    /// `false` this task — see the module docs: Task 5's pin is unbounded
    /// (ruling 23 carry-forward), so `Wal::gc` can never remove a sealed
    /// segment this archive has not yet uploaded, which is the only way a
    /// gap happens. The bounded pin that makes one possible lands at Task 7.
    pub fn gapped(&self) -> bool {
        false
    }

    /// The base_seq the next un-shipped sealed segment must have, by the
    /// contiguity this archive maintains in `self.segments`.
    ///
    /// Floored at `db.log_base()` when nothing has shipped yet: **seq 0 is
    /// usually gone.** Any node restarted after its first checkpoint, and
    /// every seeded follower (`Seeder::finish` lays the first segment at
    /// `applied_seq + 1`), opens on a log whose base is far above 0.
    /// Answering 0 there made `step_idle` find no segment, pin the floor at
    /// 0 — which holds the ENTIRE log — and ship nothing, forever, while
    /// `Wal::gc` could never advance again (M1').
    fn next_base(&self, db: &Db) -> u64 {
        let Some(last) = self.segments.last() else {
            return db.log_base();
        };
        let next = last.end_seq.checked_add(1).unwrap_or_else(|| {
            panic!(
                "Archive: the last shipped segment's end_seq is u64::MAX with more to ship \
                 after it — Manifest::encode would refuse this chain (hard rule 4); \
                 unreachable while this archive only ever appends contiguous SegmentRefs"
            )
        });
        // Negative space (hard rule 4): the pin holds everything from `next`
        // up, so gc cannot have reclaimed it. If it has, the chain this
        // archive would build next has a HOLE in it, and silently skipping
        // to the log's new base would produce exactly the manifest that
        // claims a span it never shipped. Closing a generation on a gap is
        // Task 7's job; until then this is unreachable and says so.
        assert!(
            next >= db.log_base(),
            "Archive: next needed seq {next} is below the log's base {} — gc reclaimed a \
             segment this archive still needed, which its retention pin exists to prevent",
            db.log_base()
        );
        next
    }

    /// The pin only ever moves FORWARD. It is a retention floor, and both
    /// directions of getting it wrong have already been real bugs: moving it
    /// to `u64::MAX` releases everything and let gc delete the segment the
    /// archive was waiting for (M1), while moving it BACKWARDS re-holds
    /// segments already released and freezes gc for good (M1'). Neither was
    /// caught by a test; an assert catches both by construction.
    fn hold(&self, floor: u64) {
        let mut pin = self.pin.lock().expect("uncontended");
        assert!(
            floor >= *pin,
            "Archive: retention pin moved backwards, {} -> {floor}; a floor may only advance",
            *pin
        );
        *pin = floor;
    }

    /// One unit of work: start one upload, poll one in flight, or read one
    /// budget's worth of a sealed segment — never more than one IO
    /// operation per call (spec §4). Never blocks, never touches `Db`'s
    /// write path (`Db::read_log` and `Db::sealed_segments` are both
    /// read-only). An archive IO failure is returned here and nowhere
    /// else — it never reaches `Db`, so it can never poison it (spec §7).
    pub fn step(&mut self, db: &Db, store: &dyn ObjectStore) -> io::Result<()> {
        self.idle = false;
        match std::mem::replace(&mut self.state, State::Idle) {
            State::Idle => self.step_idle(db),
            State::Snapshotting { walk } => self.step_snapshotting(walk),
            State::UploadingSnapshotBatch {
                walk,
                key,
                bytes,
                started,
            } => self.step_uploading_snapshot_batch(store, walk, key, bytes, started),
            State::UploadingSnapshotMeta {
                meta,
                bytes,
                started,
            } => self.step_uploading_snapshot_meta(store, meta, bytes, started),
            State::Reading {
                base_seq,
                layout_end,
                next_seq,
                buf,
            } => self.step_reading(db, base_seq, layout_end, next_seq, buf),
            State::UploadingSegment {
                base_seq,
                end_seq,
                key,
                bytes,
                started,
            } => self.step_uploading_segment(store, base_seq, end_seq, key, bytes, started),
            State::UploadingManifest {
                bytes,
                started,
                commits,
            } => self.step_uploading_manifest(store, bytes, started, commits),
        }
    }

    /// The manifest bytes for the generation as it stands: `snapshot`'s
    /// frontier, and whatever segment chain has been built so far. Every
    /// manifest this archive writes goes through here, so the frontier can
    /// never diverge between the one written after the snapshot and the one
    /// written after each segment.
    fn manifest_bytes(&self, snapshot: &SnapshotMeta) -> Vec<u8> {
        Manifest {
            generation: self.generation,
            writer: self.cfg.writer.clone(),
            applied_seq: snapshot.applied_seq,
            next_job_id: snapshot.next_job_id,
            tables: snapshot.tables.iter().map(|t| t.name.clone()).collect(),
            segments: self.segments.clone(),
            closed: false,
        }
        .encode()
    }

    /// Opens this generation's snapshot if the cadence admits one, and
    /// leaves the archive idle if it does not. Returns `true` when a walk
    /// was opened.
    ///
    /// Two reasons to decline, and neither may become an `unwrap_or(0)`
    /// (Defect 4): the cadence has not been reached, or the `Db` has
    /// applied nothing at all, in which case there is no frontier to name
    /// and a manifest asserting `applied_seq: 0` would lie in exactly the
    /// way the CRC cannot catch.
    fn open_snapshot(&mut self, db: &Db) -> io::Result<bool> {
        let Some(last_applied) = db.last_applied() else {
            return Ok(false);
        };
        if self.cfg.snapshot_every > 0 && last_applied < self.cfg.snapshot_every {
            return Ok(false);
        }
        let snapshot = db.snapshot()?;
        // Read off the snapshot, not off `db`: this is the frontier the rows
        // below actually come from.
        let Some(applied_seq) = snapshot.applied_seq()? else {
            return Ok(false);
        };
        let next_job_id = snapshot.next_job_id()?;
        self.state = State::Snapshotting {
            walk: SnapshotWalk {
                snapshot,
                applied_seq,
                next_job_id,
                tables: db.table_names(),
                table: 0,
                after_id: None,
                batch: 0,
                done: Vec::new(),
            },
        };
        Ok(true)
    }

    /// Pages one batch out of the walk's single MVCC view, or moves to the
    /// next table, or (every table walked) produces `snapshot/meta`.
    ///
    /// `scan_raw_from`, never `scan_raw`: peak memory is O(batch), not
    /// O(store) (spec §10). An empty read ends a table — the same close-out
    /// the migration executor uses — so a table with no rows contributes
    /// zero batch objects and still appears in the meta.
    fn step_snapshotting(&mut self, mut walk: SnapshotWalk) -> io::Result<()> {
        if walk.table == walk.tables.len() {
            let meta = SnapshotMeta {
                applied_seq: walk.applied_seq,
                next_job_id: walk.next_job_id,
                tables: walk.done,
            };
            // The walk (and with it the redb read transaction) is dropped
            // here: nothing below re-reads rows, and holding a read view
            // open across the uploads that follow would pin store versions
            // for no reason.
            let bytes = meta.encode();
            self.state = State::UploadingSnapshotMeta {
                meta,
                bytes,
                started: false,
            };
            return Ok(());
        }
        let table = walk.tables[walk.table];
        let rows = match walk
            .snapshot
            .scan_raw_from(table, walk.after_id, SNAPSHOT_BATCH_ROWS)
        {
            Ok(rows) => rows,
            Err(e) => {
                self.state = State::Snapshotting { walk };
                return Err(e);
            }
        };
        if rows.is_empty() {
            walk.done.push(TableBatches {
                name: table.to_string(),
                batches: walk.batch,
            });
            walk.table += 1;
            walk.after_id = None;
            walk.batch = 0;
            self.state = State::Snapshotting { walk };
            return Ok(());
        }
        let last_id = rows.last().expect("checked non-empty").0;
        // Negative space (hard rule 4): `scan_raw_from` returns ids strictly
        // above `after_id`, so the cursor advances on every batch. If it
        // ever did not, this walk would re-upload the same batch under
        // ever-increasing `n` forever and the meta would claim objects that
        // duplicate each other.
        assert!(
            walk.after_id.is_none_or(|prev| last_id > prev),
            "Archive: snapshot cursor for table {table} did not advance ({:?} -> {last_id}); \
             scan_raw_from returns only ids strictly above its cursor",
            walk.after_id
        );
        let bytes = encode_snapshot_batch(&rows);
        let key = snapshot_batch_key(&self.cfg.prefix, self.generation, table, walk.batch);
        walk.after_id = Some(last_id);
        walk.batch += 1;
        self.state = State::UploadingSnapshotBatch {
            walk,
            key,
            bytes,
            started: false,
        };
        Ok(())
    }

    fn step_uploading_snapshot_batch(
        &mut self,
        store: &dyn ObjectStore,
        walk: SnapshotWalk,
        key: String,
        bytes: Vec<u8>,
        mut started: bool,
    ) -> io::Result<()> {
        let done = put_step(store, &key, &bytes, &mut started);
        self.state = match done {
            Ok(true) => State::Snapshotting { walk },
            _ => State::UploadingSnapshotBatch {
                walk,
                key,
                bytes,
                started,
            },
        };
        done.map(|_| ())
    }

    fn step_uploading_snapshot_meta(
        &mut self,
        store: &dyn ObjectStore,
        meta: SnapshotMeta,
        bytes: Vec<u8>,
        mut started: bool,
    ) -> io::Result<()> {
        let key = snapshot_meta_key(&self.cfg.prefix, self.generation);
        let done = put_step(store, &key, &bytes, &mut started);
        if let Ok(true) = done {
            // THE ORDERING (spec §2). Every object of this generation's
            // snapshot has landed — the batches first, this meta last — so
            // and only so may the manifest name it. Moving this transition
            // above any of those PUTs makes the manifest point at a
            // generation whose objects are not all there, which is the one
            // thing the layout has no defence against.
            let manifest = self.manifest_bytes(&meta);
            self.state = State::UploadingManifest {
                bytes: manifest,
                started: false,
                commits: Some(meta),
            };
            return Ok(());
        }
        self.state = State::UploadingSnapshotMeta {
            meta,
            bytes,
            started,
        };
        done.map(|_| ())
    }

    /// `self.state` is already the placeholder `Idle` on entry (`step`'s
    /// `mem::replace`) — safe to use `?` throughout: there is nothing
    /// resumable to lose on an early return, unlike every other arm below.
    fn step_idle(&mut self, db: &Db) -> io::Result<()> {
        // The generation's snapshot comes before anything else it contains,
        // and no manifest is written until it has landed (module docs). A
        // segment shipped first would need a manifest to name it, and that
        // manifest would carry a frontier no snapshot backs.
        if self.snapshot.is_none() {
            if self.open_snapshot(db)? {
                // Holding, not releasing (M1): the whole log from here is
                // still needed — nothing has been archived at all.
                self.hold(self.next_base(db));
                return Ok(());
            }
            self.hold(self.next_base(db));
            self.idle = true;
            return Ok(());
        }
        let next = self.next_base(db);
        let Some(seg) = db
            .sealed_segments()?
            .into_iter()
            .find(|s| s.base_seq == next)
        else {
            // Caught up: hold at the base the NEXT segment to seal will
            // start at, never release (M1). The chain is contiguous, so
            // `next` is exactly that base: GC still reclaims everything
            // already archived and nothing more.
            self.hold(next);
            self.idle = true;
            return Ok(());
        };
        // Ruling 29's entry gate: the second, independent truncation
        // source (read_log caps up_to at last_applied) handled at its
        // source rather than discovered mid-read. Without this, picking
        // the segment anyway is still safe (step_reading below can never
        // produce a short SegmentRef either way — see its doc), but this
        // avoids the futile reads.
        if db.last_applied().is_none_or(|l| seg.layout_end > l) {
            // Holding, NOT releasing (M1): this arm has just identified
            // the segment the archive needs; telling retention it holds
            // nothing would invite GC to delete it.
            self.hold(seg.base_seq);
            self.idle = true;
            return Ok(());
        }
        self.hold(seg.base_seq);
        self.state = State::Reading {
            base_seq: seg.base_seq,
            layout_end: seg.layout_end,
            next_seq: seg.base_seq,
            buf: Vec::new(),
        };
        Ok(())
    }

    /// Reads one `read_budget_bytes`-sized bite and re-frames it into
    /// `buf`. **Clips at `layout_end`**: `Db::read_log`'s budget is a BYTE
    /// bound, not a segment bound, so a generous budget (the 8 MiB
    /// production default against a small test segment, say) can return
    /// records belonging to the FOLLOWING segment in the same call. Without
    /// clipping, those records would be mis-attributed into THIS
    /// `SegmentRef` and `next_seq` could overshoot `layout_end` — and the
    /// following `Idle` step would then find no sealed segment based
    /// exactly at the new (orphaned) `next_base`, wedging the archive.
    /// Clipping also makes ruling 29's guarantee airtight independent of
    /// budget sizing: `end_seq` can only ever be recorded as exactly
    /// `layout_end` (reached by clipped, in-range records) or a smaller
    /// value reflecting a genuinely short read — never past the target.
    fn step_reading(
        &mut self,
        db: &Db,
        base_seq: u64,
        layout_end: u64,
        next_seq: u64,
        mut buf: Vec<u8>,
    ) -> io::Result<()> {
        let batch = match db.read_log(next_seq, self.cfg.read_budget_bytes) {
            Ok(b) => b,
            Err(e) => {
                self.state = State::Reading {
                    base_seq,
                    layout_end,
                    next_seq,
                    buf,
                };
                return Err(e);
            }
        };
        if batch.is_empty() {
            // The last_applied cap truncated mid-read (ruling 29's second
            // truncation source): wait rather than upload a short segment.
            self.state = State::Reading {
                base_seq,
                layout_end,
                next_seq,
                buf,
            };
            return Ok(());
        }
        let mut read_through = next_seq;
        for (seq, payload) in batch {
            if seq > layout_end {
                break; // belongs to the following segment; re-read it later.
            }
            buf.extend_from_slice(&seq.to_le_bytes());
            buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            buf.extend_from_slice(&frame_crc(seq, &payload).to_le_bytes());
            buf.extend_from_slice(&payload);
            read_through = seq + 1;
        }
        if read_through > layout_end {
            let end_seq = read_through - 1;
            let key = segment_key(&self.cfg.prefix, self.generation, base_seq);
            self.state = State::UploadingSegment {
                base_seq,
                end_seq,
                key,
                bytes: buf,
                started: false,
            };
        } else {
            self.state = State::Reading {
                base_seq,
                layout_end,
                next_seq: read_through,
                buf,
            };
        }
        Ok(())
    }

    fn step_uploading_segment(
        &mut self,
        store: &dyn ObjectStore,
        base_seq: u64,
        end_seq: u64,
        key: String,
        bytes: Vec<u8>,
        mut started: bool,
    ) -> io::Result<()> {
        // A failed start or poll STAYS and retries the SAME key with the
        // SAME bytes (ruling 29 / spec §9.2) — `put_step` leaves `started`
        // false for exactly that, so a lost ack never duplicates an object.
        let done = put_step(store, &key, &bytes, &mut started);
        if let Ok(true) = done {
            // The commit point is the MANIFEST swap (spec §2), never
            // this PUT alone. The SegmentRef joins the chain HERE so
            // the manifest bytes below can name it, but the PIN does
            // not advance past `base_seq` until that manifest PUT has
            // itself landed. The chain is never READ in the window
            // between the two — `next_base()` is called only from
            // `step_idle`, unreachable while UploadingManifest — so a
            // manifest that never lands cannot make the archive skip
            // the segment it names.
            self.segments.push(SegmentRef { base_seq, end_seq });
            let manifest_bytes = {
                let snapshot = self.snapshot.as_ref().expect(
                    "Archive: a segment landed for a generation with no snapshot — step_idle \
                     ships nothing until the opening snapshot's manifest has itself landed, so \
                     the manifest built here can never lack a frontier",
                );
                self.manifest_bytes(snapshot)
            };
            self.state = State::UploadingManifest {
                bytes: manifest_bytes,
                started: false,
                commits: None,
            };
            return Ok(());
        }
        self.state = State::UploadingSegment {
            base_seq,
            end_seq,
            key,
            bytes,
            started,
        };
        done.map(|_| ())
    }

    fn step_uploading_manifest(
        &mut self,
        store: &dyn ObjectStore,
        bytes: Vec<u8>,
        mut started: bool,
        commits: Option<SnapshotMeta>,
    ) -> io::Result<()> {
        let key = manifest_key(&self.cfg.prefix);
        let done = put_step(store, &key, &bytes, &mut started);
        if let Ok(true) = done {
            // The manifest swap landed: whatever this manifest names —
            // the generation's snapshot, a segment, or both — is now
            // durably reflected off-box, so the pin may release it. The
            // NEXT `Idle` step recomputes the true floor (another sealed
            // segment may already be waiting, or none is).
            if let Some(meta) = commits {
                self.snapshot = Some(meta);
            }
            self.state = State::Idle;
            return Ok(());
        }
        self.state = State::UploadingManifest {
            bytes,
            started,
            commits,
        };
        done.map(|_| ())
    }
}
