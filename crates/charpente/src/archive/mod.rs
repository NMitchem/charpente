//! Object-storage archive (M2, spec docs/superpowers/specs/2026-08-20-
//! object-storage-archive-design.md): ships sealed WAL segments and
//! logical snapshots to object storage behind a generation-pointer
//! manifest (§2), and restores from it (§6). Task 4 landed the manifest and
//! the key layout with nothing uploading yet; Task 5 landed the engine that
//! ships sealed segments (§3's first half); Task 6 landed the snapshot
//! (§3's other half) and the ordering that is the whole safety argument;
//! Task 7 lands the bounded pin, the named gap, generation transitions and
//! resume-or-open; Task 7.5 makes a generation's chain CONTINUE its snapshot
//! instead of overlapping it (ruling 53); Task 8 lands ownership (§4) — the
//! write gate and the writer identity. Restore (§6) is still to come.
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
//!   generation Task 6 exists to prevent. Refresh is therefore a *new
//!   generation* (`roll_generation`), which is why `snapshot_every` gates
//!   two things that look like one: when a generation's opening snapshot is
//!   taken, and when the live generation is closed in favour of a fresh one.
//!   A RESTART is the same hazard on a different trigger, which is why
//!   `State::Opening` reads the bucket before writing to it: Task 5's
//!   "always writes fresh" is idempotent for segment objects, whose bytes
//!   are a function of their key, but not for a snapshot, whose bytes are a
//!   function of when it was taken.
//!
//! **A generation's chain CONTINUES its snapshot** (ruling 53, spec §2), and
//! that is what makes a snapshot worth taking: §3's "snapshots ... release
//! the WAL below them" — WAL on this node's disk, never objects in the
//! bucket, which nothing here reclaims. The chain starts at the base of the
//! sealed segment holding the first record the snapshot does not carry
//! (`chain_start`), never *below* it — every segment lying wholly below the
//! frontier is the snapshot's, so the archive neither holds nor ships it.
//! Before this the chain began at `db.log_base()` and a generation uploaded
//! its whole log twice: once as a snapshot, once as a chain underneath it,
//! holding all of that WAL while it did.
//!
//! It can still start AT `db.log_base()`, and that is not the defect: when
//! nothing is sealed the whole log is the live tail, whose base is the log's
//! base, and nothing lies wholly below the frontier to drop
//! (`chain_start`'s empty-listing fallback, and the test
//! `a_generation_snapshotted_before_any_segment_seals_starts_at_the_logs_base`).
//! What ruling 53 removes is a chain starting below segments the snapshot
//! already covers, never the value `log_base` itself.
//!
//! The residue this leaves is one segment's worth, and it is structural
//! rather than an oversight: whole segments ship verbatim and a frontier is
//! wherever the `Db` had applied to, never a segment boundary, so the
//! chain's HEAD segment still carries the records between its own base and
//! the frontier. `Manifest::applied_seq` is the seq a restore replays from,
//! which makes that head skippable by arithmetic rather than by search.
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
//! shape `Repl` already has (DESIGN §5): `Opening` GETs the manifest and
//! decides whether to resume the generation the bucket names or open a
//! fresh one; `ClosingGeneration` marks a gapped generation finished;
//! `Idle` opens the generation's
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
//! PUTs the reassembled bytes; `CheckingOwner` GETs the manifest this
//! archive is about to replace and refuses if it names another writer
//! (§4); `UploadingManifest` swaps the manifest to
//! reference the new segment, the one mutable key and the archive's own
//! commit point (spec §2); `Stopped` is where an ownership refusal parks,
//! permanently and holding nothing. A failed PUT retries with the identical key and
//! bytes — never a fresh one — which is what keeps a lost-ack retry from
//! duplicating an object (spec §9.2).
//!
//! **The retention pin is BOUNDED** (spec §5, ruling 3): it holds at the
//! lowest seq this archive still needs — the lowest sealed segment not yet
//! reflected in an uploaded manifest — but never more than
//! `pin_segments` segments' worth. Past the bound the archive **yields**:
//! it releases the pin so `Wal::gc` proceeds, closes its generation, and
//! returns one named notice. Spec §5 states the stake: "an unbounded pin
//! means an object-store outage grows the WAL until the disk fills and the
//! log poisons — the archive would have acquired the power to stop the thing
//! it derives from." On outage the archive yields, never the primary.
//!
//! **The release is a STATE, not a VALUE** (ruling 49). `Db::retention_floor`
//! is a *min* over pin closures and `db.rs` documents `u64::MAX` as what "a
//! pin that is not holding returns" — the identity element of that min, not
//! a hold-everything sentinel. So the registered closure reports `u64::MAX`
//! while `Pin::holding` is clear, while `Pin::floor` stays a real,
//! monotonic seq that `pin_floor()` keeps reporting. Writing `u64::MAX` into
//! the floor instead would be verbatim the M1 bug `hold` asserts against,
//! and re-holding afterwards — at the new generation's lower base — would
//! then trip the same assert in the M1' direction on the way back.
//!
//! **M1, and why yielding does not re-open it.** A pin that reads
//! `u64::MAX` used to be a pin that let `Wal::gc` delete the very segment
//! `next_base()` was waiting for; after that deletion `step_idle`'s `find`
//! never matched again and the archive shipped nothing, forever, reporting
//! no error — 50 of 50 segments, silently. What made that a *bug* was that
//! the archive still needed those records and had frozen its own cursor at
//! a seq that no longer existed. Here the two always move together: the
//! archive yields only when it holds nothing it needs, and `next_needed`
//! tracks `db.log_base()` for exactly as long as it is yielding, so the
//! cursor can never point below the log. `next_base`'s assert is the live
//! check on that.
//!
//! Two states yield, and only one of them is a gap:
//!
//! - **No generation open.** The archive's chain has not started, so it
//!   needs nothing from the log: whatever it eventually snapshots will name
//!   its own frontier. That is the fix for Task 6's configuration-triggered
//!   growth (ruling 46) — the archive used to hold at the log's base while
//!   waiting for a `snapshot_every` the `Db` had not crossed, freezing
//!   `Wal::gc` against a perfectly healthy store. Bounding that wait would
//!   have been the wrong shape: a bound sized for outages is sized wrong for
//!   a cadence that has simply not arrived, and waiting loses nothing, so it
//!   must not spend a gap.
//! - **Gapped.** The bound was crossed while the archive *was* holding
//!   something it needed. Records the log reclaims from here are not in the
//!   archive, so the generation is closed (spec §2: a generation is either
//!   continuous or it is finished) and the next successful contact opens a
//!   fresh one with a full snapshot. `gapped()` reports this, and `step`
//!   returns the operator notice once — when it happens, not once per step.
//!
//! **Two different generations are involved in a gap, and the notice says
//! so** (F-C). The one this archive was BUILDING is abandoned: if it had
//! already written snapshot objects but no manifest yet, those objects are
//! orphaned, and **nothing in this design collects them** — not the archive
//! (it never lists the bucket), not restore, and no task in the plan. The
//! two honest answers are a bucket lifecycle rule on `<prefix>/<gen>/`, or a
//! sweep built on `ObjectOp::Delete`, which the trait already carries and
//! nothing yet calls. The one `ClosingGeneration` marks `closed` is a
//! different thing: the generation the MANIFEST names, which is what a
//! restore would read, and which is only the same generation when this one
//! had already published.
//!
//! **Generations transition on two triggers and one path** (`roll_generation`):
//! a gap, and a `snapshot_every` crossing. A generation carries exactly one
//! snapshot, so a *refresh* is a new generation — never a second snapshot
//! written into `<gen>/snapshot/**`, whose keys the live manifest already
//! points at. A roll deliberately CARRIES `next_needed`, as a FLOOR: it
//! keeps the pin monotonic across the roll (M1'), and the new generation's
//! own snapshot then moves it up to that snapshot's frontier segment. What
//! the old chain never shipped is not lost by that move — the new
//! generation's snapshot contains it, since a snapshot's frontier is at or
//! above every record its predecessor shipped.
//!
//! **`Archive::step`'s first act is a store READ** (`State::Opening`): a GET
//! of `<prefix>/manifest`, which decides whether this archive RESUMES the
//! generation the bucket names or opens a fresh one — never blindly reuses 0
//! (ruling 43). Task 6 could defer this only because nothing wires the
//! archive into a kernel arm until Task 9; a restart that reused generation
//! 0 would re-snapshot into a live generation's immutable keys. The same GET
//! is the archive's contact probe after a gap, which is why the walk for the
//! next generation never opens against a store that has not answered.
//!
//! Ask `Archive::idle` whether there is work left — never `pin_floor()`,
//! which has no caught-up sentinel and never should have been read as one.
//!
//! **OWNERSHIP** (spec §4), Task 8. Both rules are things this module can
//! violate without failing a single test, which is why they are written here
//! as well as asserted there.
//!
//! - **The write gate.** A `Verifying` node uploads NOTHING. `step` returns
//!   before any transition while `db.verifying()`, and reads it afresh every
//!   call rather than latching. The precedent is exact and is the reason
//!   this exists at all: the verifying-peer policy (DESIGN §5,
//!   specs/2026-08-06-verifying-peer-policy-design.md) found `begin_seeding`
//!   slipping past the write gate BECAUSE IT WAS READ-ONLY, manufacturing
//!   durable replicas of a timeline the node had not confirmed it owned. The
//!   archive arm is read-only against the `Db` in exactly the same way, and
//!   an object in a bucket outlives this node's fail-stop far more
//!   thoroughly than a seeded follower does. Gating is NOT a gap: nothing is
//!   missing from an archive that was never this node's to ship, so the
//!   guard sits in front of `yield_or_bound` and no notice fires.
//! - **The writer identity.** The manifest records who wrote it, and this
//!   archive refuses to advance one that names anyone else — at `adopt`,
//!   before a single object is uploaded, and again at `CheckingOwner` before
//!   every swap, since a peer can be promoted at any point in between. Both
//!   are needed and neither subsumes the other: without the `adopt` refusal
//!   an archive opens a fresh generation NUMBERED ABOVE the foreign one and
//!   then swaps the manifest to name itself (measured: generation 4 written
//!   over node-b's generation 3); without the pre-swap check nothing catches
//!   a takeover that happens while this archive is already running.
//!   DETECTION, NOT PREVENTION — `ObjectStore` has no compare-and-swap and
//!   the GET-to-PUT window is real — the same honest posture the promote
//!   path takes toward its bounded-loss window.
//!
//!   **IDENTITY ALONE, and what that costs.** The comparison carries no
//!   generation term, so a node whose `ArchiveConfig::writer` legitimately
//!   CHANGES — the same node redeployed on a new port — refuses permanently
//!   at `adopt` on a bucket it genuinely owns, before anything is uploaded,
//!   where a comparison that also asked "is the other writer further along"
//!   would have kept it archiving. That is the trade taken deliberately, and
//!   the asymmetry is the reason: no fact in the bucket distinguishes a
//!   rename from a takeover (the manifest carries one string, and both
//!   present identically), while getting it wrong toward liveness produces a
//!   bucket neither node can restore from and getting it wrong toward safety
//!   produces a stopped archive with a named notice and a live, intact
//!   bucket. `ArchiveConfig::writer`'s doc carries what follows for whoever
//!   wires this up: it is a stability contract, and must not be derived from
//!   anything a redeploy moves.
//!
//! - **A manifest this archive cannot decode** (F5) is that same question
//!   with no answer at all, and takes the same refusal: it is the only
//!   evidence of who owns the prefix, so an archive that cannot read it
//!   cannot tell whether the objects here are its own, and opening a fresh
//!   generation over them is the interleave above on no evidence
//!   whatsoever. It does not retry — see `unreadable_manifest_notice` for
//!   why, and for what the retry it replaces was costing.
//!
//! A refusal is TERMINAL (`stop`), unlike a gap — but the two refusals reach
//! that for different reasons, and only the first is a fact about the world.
//! Two writers on one prefix is an operator-configuration fact that no
//! amount of retrying reaches. An unreadable manifest is NOT that kind of
//! fact: a re-fetch may well succeed, since a CRC mismatch cannot
//! distinguish a corrupt object from a corrupted read, and
//! `unreadable_manifest_notice` says so in as many words. What makes
//! latching right THERE is measured rather than asserted — retrying forever
//! reaches no verdict while holding the WAL at this archive's pin the whole
//! time (`retention_floor` stuck at 0 at the `Opening` site and at 1364
//! mid-run, ~2500 `Err`s in 5000 steps, no verdict either way) — and
//! latching does not destroy the retry, it hands it to the operator.
//! It releases the retention pin on the way in, because an archive that will
//! never ship again but goes on holding the WAL is the archive acquiring the
//! power to halt the primary (spec §5), and it raises its notice ONCE rather
//! than once per step — `stopped()` is the standing answer after that.

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
    /// advanced by a different identity stops rather than interleaving
    /// generations. Read on both sides since Task 8: written into every
    /// manifest this archive PUTs, and compared against the manifest already
    /// in the bucket at `adopt` and again before every swap
    /// (`step_checking_owner`).
    ///
    /// **A STABILITY CONTRACT, not a label.** The comparison at both sites
    /// is identity ALONE, so this string is part of the archive's durable
    /// state: change it on a prefix this node already owns and the archive
    /// refuses PERMANENTLY at `adopt`, before uploading anything, on a
    /// bucket that is genuinely its own — `stopped() == true` for the life
    /// of the process, recoverable only by the operator action
    /// `foreign_writer_notice` names (remove the manifest object and take a
    /// fresh generation's full snapshot, abandoning the live generation's
    /// objects, which nothing in this design collects).
    ///
    /// That is the deliberate side of an asymmetric trade (spec §4 rule 2),
    /// not an oversight: nothing in the bucket distinguishes "I am the same
    /// node, renamed" from "a second node has taken this prefix over" — the
    /// manifest carries one string and both cases present identically — and
    /// the two errors are not symmetric. Wrongly continuing interleaves two
    /// writers' generations and leaves a bucket neither can restore from;
    /// wrongly stopping costs archive availability, with a named notice, a
    /// live intact bucket, and an untouched `Db`.
    ///
    /// **It follows that `writer` must be stable across a redeploy, and must
    /// not be derived from anything a redeploy moves** — a listen address, a
    /// port, a pod name, a container id, an ephemeral IP. Deriving it from
    /// `serve`'s bind address would make an ordinary port change a permanent
    /// archive outage that reports `idle() == true` while it lasts.
    pub writer: String,
    /// Cadence for logical snapshots, in committed records — mirrors
    /// `DbConfig::checkpoint_every` (spec §3), and seq-driven rather than
    /// time-driven so that replay never depends on a wall clock.
    ///
    /// It gates when this generation's opening snapshot is taken: the
    /// archive uploads nothing at all until this many records have been
    /// committed, because a manifest with no snapshot behind it names a
    /// generation that does not exist. `0` means "no cadence configured" —
    /// snapshot as soon as there is a frontier to name.
    ///
    /// RECORDS, not seqs: `snapshot_every: N` is satisfied by the Nth
    /// committed record, whose `Db::last_applied` seq is `N - 1`. Comparing
    /// against the raw seq would fire one record late.
    ///
    /// A generation carries exactly one snapshot (module docs), so a *later*
    /// multiple does not re-snapshot in place: it opens a NEW generation
    /// with a fresh full snapshot, the same transition a gap performs.
    /// Measured from the live generation's own frontier, so the cadence is
    /// "this many records since the last snapshot", never since seq 0.
    pub snapshot_every: u64,
    /// The archive's retention-pin bound, in segments (spec §5).
    ///
    /// **The default of 8 is PROVISIONAL and its old derivation is void.** It
    /// was justified as "4x `DbConfig::retain_segments`'s default of 2, 512
    /// MiB of held WAL at 64 MiB segments". That is not a coefficient that
    /// needs updating — after ruling 53 it relates two quantities that no
    /// longer have a relationship. Before ruling 53 the pin held everything
    /// from the log's base upward: a HISTORY quantity, growing with how far
    /// behind the archive was, for which a multiple of the `Db`'s own
    /// retention was at least dimensionally sensible. It now holds only what
    /// seals above this generation's snapshot frontier during the upload
    /// window: a RATE x DURATION quantity, unrelated to `retain_segments`,
    /// which bounds the `Db`'s checkpoint recycling and is not a party to the
    /// archive's upload window at all.
    ///
    /// The rule that replaces it:
    ///
    /// ```text
    /// pin_segments >= ceil(write_rate_bytes_per_sec * T_wait / segment_bytes)
    /// T_wait        = max(outage_tolerance, db_logical_bytes / upload_bytes_per_sec)
    /// ```
    ///
    /// Both right-hand inputs are DEPLOYMENT properties, not properties of
    /// charpente, and the dominant one is unmeasured: `docs/BENCH.md` carries
    /// no archive or object-store coverage at all, so no defensible constant
    /// can be quoted for it today (CLAUDE.md makes BENCH.md the only source
    /// of quotable performance numbers). 8 is therefore kept as-is rather
    /// than re-picked, because raising it would look like a re-derivation
    /// while being the same species of unfounded number as the one it
    /// replaced. Spec §14 open item 1 carries the analysis and what to
    /// measure.
    ///
    /// Getting it wrong is not a correctness failure in either direction, and
    /// that is why shipping a provisional constant is acceptable here while
    /// shipping an invalid derivation was not: past the bound the archive
    /// yields, never the primary (spec ruling 3). Too small costs wasted
    /// upload bandwidth and gap churn; too large costs held WAL on disk.
    ///
    /// Counted as sealed segments at or above `next_needed` — exactly what
    /// this archive's pin is keeping alive. Past it the archive yields
    /// (module docs). Size it for the LONGER of the two waits it covers: an
    /// object-store outage, and the time one generation's full snapshot
    /// takes to upload, during which the archive ships no segment at all and
    /// its pin holds every segment that seals meanwhile. (Not every segment
    /// on disk: the snapshot's own frontier is where its chain starts, so
    /// what sealed before it was taken is released rather than held.)
    ///
    /// Must be at least 1; `open` asserts it. Zero would yield at the first
    /// un-archived segment and make every generation one segment long.
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

/// What a generation's manifest repeats on every write: the frontier its one
/// snapshot was taken at, the job counter, and the table names.
///
/// Kept instead of the whole `SnapshotMeta` because the per-table BATCH
/// COUNTS are the reader's business — restore enumerates a generation's
/// objects from them — and never the writer's, and because `State::Opening`
/// reconstructs this from the manifest alone when it resumes a generation
/// (ruling 43), where the counts are not available without a second GET.
struct Frontier {
    applied_seq: u64,
    next_job_id: u64,
    tables: Vec<String>,
}

/// The retention pin as `Db` sees it (module docs, ruling 49).
struct Pin {
    /// The lowest seq this archive still needs. Monotonic and never
    /// `u64::MAX` — `hold` asserts the first, `pin_floor`'s doc promises the
    /// second — in BOTH states, because the release is `holding`, not this.
    floor: u64,
    /// Whether the closure registered with `Db::add_retention_pin` reports
    /// `floor` at all. Clear means it reports `u64::MAX`, which `db.rs`
    /// documents as what "a pin that is not holding returns" and which is
    /// the identity element of `Db::retention_floor`'s min.
    holding: bool,
}

/// One transition of `Archive::step`'s state machine (module docs).
enum State {
    /// The archive's first act, and its first act again after a gap: GET
    /// `<prefix>/manifest` to learn what the bucket already holds, so this
    /// archive resumes that generation or opens a fresh one rather than
    /// blindly reusing 0 (ruling 43). After a gap it doubles as the contact
    /// probe: the next generation's snapshot walk must not open against a
    /// store that has not answered, or it would be taken at a frontier the
    /// log has moved past by the time it lands.
    Opening {
        started: bool,
    },
    /// Marking the generation a gap broke as `closed` (spec §2) before the
    /// fresh one is built. The manifest still names it until the fresh
    /// generation's snapshot lands, which can be a long time, and "stale"
    /// and "finished" are different facts to a restore.
    ClosingGeneration {
        bytes: Vec<u8>,
        started: bool,
    },
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
    /// SPEC §4 rule 2: the ownership check standing in front of EVERY
    /// manifest swap this archive performs. The manifest is the layout's
    /// one mutable key (spec §2) and `ObjectStore` has no compare-and-swap,
    /// so reading it and refusing is the only way not to advance a manifest
    /// this node does not own. DETECTION, NOT PREVENTION — the window
    /// between this GET and the PUT that follows it is real, the same
    /// honest posture the promote path takes toward its bounded-loss
    /// window.
    CheckingOwner {
        bytes: Vec<u8>,
        commits: Option<Frontier>,
        started: bool,
    },
    /// TERMINAL (`stop`). This archive found the prefix owned by another
    /// writer, or a manifest it could not decode at all: it does nothing
    /// further, having released the log on the way in — a stopped archive
    /// that went on holding the WAL would be the archive acquiring the power
    /// to halt the primary (spec §5).
    Stopped,
    UploadingManifest {
        bytes: Vec<u8>,
        started: bool,
        /// `Some` when this manifest is the one that commits the
        /// generation's opening snapshot. The archive records the snapshot
        /// as its own only when this PUT lands, because the manifest swap
        /// is the archive's commit point (spec §2) — not when the snapshot
        /// objects themselves landed.
        commits: Option<Frontier>,
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

/// The seq a generation's chain starts at, given the frontier `F` its one
/// snapshot was taken at. SPEC §2 — the chain CONTINUES the snapshot — and
/// spec §3, which is what makes a snapshot worth taking: it is what lets the
/// archive drop the segments below it (RULING 53).
///
/// `F + 1` is the first record the snapshot does not carry, but the chain
/// ships whole sealed segments verbatim (spec §3), so it starts at the BASE
/// of the segment `F + 1` falls in — the live tail's base when it falls
/// above every sealed segment, that being the base of the first segment that
/// can ever seal above this snapshot.
///
/// The head of the chain therefore still carries the records between its own
/// base and `F`, which the snapshot has too. That residue is bounded by one
/// segment and is structural rather than an oversight: a frontier is
/// wherever the `Db` had applied to, never a segment boundary, and §3 ships
/// segments verbatim rather than re-cut. It is also skippable by arithmetic
/// rather than by search, since `Manifest::applied_seq` is the seq a restore
/// replays from. What ruling 53 removes is the UNBOUNDED part: every segment
/// lying wholly below `F`, which is the whole log at a fresh generation.
fn chain_start(db: &Db, frontier: u64) -> io::Result<u64> {
    // `F + 1`, never `F`: `applied_seq` is INCLUSIVE, so a frontier landing
    // exactly on a `layout_end` belongs wholly to the snapshot and the chain
    // starts at the NEXT segment. Neither assert below catches getting this
    // wrong — both are written in terms of `target`, so both stay satisfied
    // by the shifted answer — and every other frontier in the test file
    // falls strictly inside a segment, where the two agree. The oracle is
    // `a_frontier_on_a_segment_boundary_starts_the_chain_at_the_segment_above_it`.
    let target = frontier.saturating_add(1);
    let sealed = db.sealed_segments()?;
    let start = match sealed.iter().find(|s| target <= s.layout_end) {
        Some(s) => s.base_seq,
        None => sealed
            .last()
            .map_or_else(|| db.log_base(), |s| s.layout_end + 1),
    };
    // Negative space (hard rule 4), and both directions of getting this
    // wrong have a name. ABOVE `target` is a HOLE: the records between the
    // snapshot and the chain would be in neither, which is the one thing
    // spec §2 says a generation never has. Reachable only if gc has taken a
    // segment at or below the frontier, which the retention pin exists to
    // prevent — so it is asserted, not handled.
    assert!(
        start <= target,
        "Archive: a generation's chain would start at {start}, above the first record its \
         snapshot does not carry ({target}) — those records would be in no part of the \
         generation; the log's base is {} and its sealed segments are {sealed:?}",
        db.log_base()
    );
    // BELOW the frontier's own segment is the OVERLAP ruling 53 removes: a
    // sealed segment at or above the chain's start that lies wholly below
    // the frontier holds nothing the snapshot does not already carry, so
    // shipping it uploads those records twice and holds WAL for them.
    assert!(
        sealed
            .iter()
            .all(|s| s.base_seq < start || s.layout_end >= target),
        "Archive: a generation's chain would start at {start} while a sealed segment at or \
         above it lies wholly below the snapshot's frontier {frontier} — the chain must \
         continue the snapshot, not repeat it (spec §2); sealed segments are {sealed:?}"
    );
    Ok(start)
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
    pin: Arc<Mutex<Pin>>,
    /// This archive's generation counter (spec §2), 0-based. Advances by
    /// exactly one per transition, and only through `roll_generation` or
    /// `State::Opening`'s adoption of what the bucket already holds.
    generation: u64,
    /// This generation's frontier, recorded only once the manifest naming
    /// its snapshot has landed. `None` means the generation has no snapshot
    /// yet, and while it is `None` the archive ships nothing at all: a
    /// manifest with no snapshot behind it names a frontier restore would
    /// reason from and no object backs (module docs). Every later manifest
    /// this archive writes repeats these values, since a generation carries
    /// exactly one snapshot.
    frontier: Option<Frontier>,
    /// The manifest's segment chain as this archive has built it so far,
    /// oldest first, contiguous (`Manifest::encode` asserts this — hard
    /// rule 4). Empty until the generation ships its first segment; a
    /// generation roll clears it while deliberately carrying `next_needed`.
    segments: Vec<SegmentRef>,
    /// The base_seq of the next sealed segment this archive needs — the head
    /// of the chain it is still building, and the seq its pin holds at.
    /// Set to the generation's chain start (`chain_start`, ruling 53) when
    /// that generation's snapshot is taken, and advanced one segment at a
    /// time from there as each manifest lands.
    ///
    /// Held as a field rather than derived from `segments` because it
    /// SURVIVES a generation roll as the floor the next one starts from, and
    /// re-deriving it from an empty chain would move a caught-up floor
    /// backwards onto `db.log_base()` (M1').
    next_needed: u64,
    /// The BOUND's baseline while `State::Opening` has not yet told this
    /// archive what it actually needs (F-A, task-7-review.md). `next_needed`
    /// is `db.log_base()` at that point — a PROVISIONAL hold that keeps a
    /// resumable chain intact across the two steps the manifest GET takes,
    /// not a claim that everything above it is un-archived. Counting it as
    /// backlog made a restart over a FULLY ARCHIVED log declare a gap on its
    /// very first `step`, before it had read the bucket at all, and
    /// `resume_point`'s `gapped` refusal then defeated resume-or-open
    /// outright.
    ///
    /// The live tail's base at `open`, so a restart over a fully archived
    /// log counts zero segments behind. This bounds the BOUND and nothing
    /// else — it is NOT what is being retained. The pin's floor tracks
    /// `next_needed`, which stays `db.log_base()` until `adopt` runs, so a
    /// STALLED `Opening` goes on holding every segment already sealed at
    /// `open` as well (measured at 26 held against a bound of 8: R-1,
    /// task-7-rereview-r1.md). That hold is bounded, not unbounded — once
    /// `pin_segments` further segments seal past this baseline the gap fires
    /// and releases it — but while it lasts the quantity the bound measures
    /// is not the quantity spec §5 names. `None` once `adopt` has set a real
    /// cursor.
    opening_baseline: Option<u64>,
    state: State,
    /// Set by the `step_idle` arms that find no available work, cleared by
    /// every `step`. Backs `Archive::idle`.
    idle: bool,
    /// Backs `Archive::gapped` — see its doc for what it does and does not
    /// mean. Distinct from `Pin::holding`, which is also clear while the
    /// archive simply has no generation open and so nothing to hold.
    gapped: bool,
}

impl Archive {
    /// Registers the retention pin on `db` (mirrors `Repl::open`,
    /// repl/mod.rs:254-257) and returns ready to `step`. Never reads the
    /// store — `step` is the only method that ever touches it (ruling 32),
    /// so `open` cannot fail on the store's account and never does.
    pub fn open(cfg: ArchiveConfig, db: &mut Db) -> io::Result<Archive> {
        assert!(
            cfg.pin_segments > 0,
            "ArchiveConfig::pin_segments is 0, which yields at the first un-archived segment and \
             makes every generation one segment long; set it to the number of WAL segments this \
             archive may hold above the checkpoint floor (spec §5)"
        );
        // Hold from the moment the pin is registered (M1). Starting at
        // `u64::MAX` left a window between `open` and the first pinning
        // `step` in which `Wal::gc` was free to delete the very segment
        // `next_base()` would later look for — after which `step_idle`'s
        // `find` never matches again and the archive ships nothing,
        // forever, reporting no error. It matters here for a second reason:
        // `State::Opening` may be about to RESUME the generation the bucket
        // names, and a gc that ran in the two steps that GET takes could
        // otherwise put a hole in the very chain being resumed.
        let pin = Arc::new(Mutex::new(Pin {
            floor: db.log_base(),
            holding: true,
        }));
        let opening_tail = db
            .sealed_segments()?
            .last()
            .map_or_else(|| db.log_base(), |s| s.layout_end + 1);
        db.add_retention_pin(Box::new({
            let pin = Arc::clone(&pin);
            // RULING 49: the release lives HERE, in what the closure
            // reports, and never in `floor`. `Db::retention_floor` is a min
            // and `db.rs` documents `u64::MAX` as "a pin that is not
            // holding" — its identity element.
            move || {
                let pin = pin.lock().expect("uncontended");
                if pin.holding { pin.floor } else { u64::MAX }
            }
        }));
        Ok(Archive {
            cfg,
            pin,
            generation: 0,
            frontier: None,
            segments: Vec::new(),
            next_needed: db.log_base(),
            opening_baseline: Some(opening_tail),
            state: State::Opening { started: false },
            idle: false,
            gapped: false,
        })
    }

    /// The retention floor this archive carries: the lowest seq it still
    /// needs, which once caught up is the base the next segment to seal
    /// will start at. **Never `u64::MAX`**, in either state — when the
    /// archive yields it clears `Pin::holding` and leaves this a real,
    /// monotonic seq (ruling 49). Use `Archive::idle` to ask whether there
    /// is work left and `Archive::gapped` to ask whether the pin is live;
    /// never read a sentinel out of this.
    ///
    /// A segment counts as "reflected" only once the manifest PUT naming it
    /// has itself succeeded — not merely once its own bytes landed — so
    /// retention never releases a segment the manifest doesn't yet
    /// acknowledge (spec §2: the manifest swap is the commit point).
    pub fn pin_floor(&self) -> u64 {
        self.pin.lock().expect("uncontended").floor
    }

    /// True when the last `step` found nothing it could do. Use it to stop
    /// stepping — that is the whole of its meaning, and it is why the
    /// terminal case below answers true rather than false.
    ///
    /// FOUR states answer true here, and this accessor does not distinguish
    /// them:
    ///
    /// - **Caught up** — nothing is sealed past what has been archived.
    /// - **Waiting** — the one sealed segment left is not yet fully applied.
    /// - **Write-gated** — `db.verifying()`, so this node may upload nothing
    ///   at all (spec §4 rule 1). Transient: the archive resumes by itself
    ///   when the gate opens, since the guard is re-read every step. A test
    ///   loop keyed on `idle` therefore exits after ONE step against a gated
    ///   archive and proves nothing; step a fixed count instead.
    /// - **Stopped** — a permanent ownership refusal (spec §4 rule 2, §7),
    ///   where it is true forever. A stopped archive genuinely has nothing
    ///   it can ever do, and answering false would tell every caller to go
    ///   on driving a terminal state.
    ///
    /// **Not a durability signal, and not a health signal.** It says the
    /// archive has no work it can do right now — never that everything
    /// reached the store, and never that anything is well. `gapped()` and
    /// `stopped()` are the two accessors that answer "is something wrong",
    /// and an arm that renders only `idle` presents a permanently dead
    /// archive as a healthy one.
    pub fn idle(&self) -> bool {
        self.idle
    }

    /// True while this archive is YIELDING after a gap (spec §2, ruling 3):
    /// its bounded pin released, its generation closed, and the records
    /// `Wal::gc` reclaims meanwhile absent from the archive. Cleared when
    /// the next generation's snapshot opens — the point at which the archive
    /// is building a continuous generation again and holds the log for it.
    ///
    /// It answers "is the archive yielding right now", which is what a
    /// kernel arm needs; it is not a historical flag. The operator-facing
    /// record that a gap HAPPENED is the notice `step` returns once, when it
    /// happens (hard rule 5).
    ///
    /// Not the same question as "is the pin live": an archive with no
    /// generation open holds nothing either, and that is not a gap.
    pub fn gapped(&self) -> bool {
        self.gapped
    }

    /// True once this archive has STOPPED PERMANENTLY (spec §4, §7): it
    /// found `<prefix>/manifest` naming a writer that is not this node, or
    /// could not decode it at all, so it refused rather than interleaving
    /// its generations into a bucket another node may own. It has released
    /// its retention pin and will do nothing further; only a restart
    /// re-reads the bucket.
    ///
    /// Distinct from `gapped()`, which is recoverable — the next successful
    /// contact opens a fresh generation. This one is not: two writers on one
    /// prefix is an operator-configuration fact, and nothing the archive can
    /// do about it.
    pub fn stopped(&self) -> bool {
        matches!(self.state, State::Stopped)
    }

    /// The base_seq of the next sealed segment this archive needs.
    ///
    /// `next_needed` is floored at `db.log_base()` for as long as the
    /// archive holds nothing (`yield_or_bound`): **seq 0 is usually gone.**
    /// Any node restarted after its first checkpoint, and every seeded
    /// follower (`Seeder::finish` lays the first segment at `applied_seq +
    /// 1`), opens on a log whose base is far above 0. Answering 0 there made
    /// `step_idle` find no segment, pin the floor at 0 — which holds the
    /// ENTIRE log — and ship nothing, forever, while `Wal::gc` could never
    /// advance again (M1').
    fn next_base(&self, db: &Db) -> u64 {
        // Negative space (hard rule 4): while the archive HOLDS, the pin
        // covers everything from here up, so gc cannot have reclaimed it. If
        // it has, the chain this archive would build next has a HOLE in it,
        // and silently skipping to the log's new base would produce exactly
        // the manifest that claims a span it never shipped. Losing records
        // to gc is what a GAP is, and a gap yields the pin and re-derives
        // `next_needed` rather than reaching here.
        assert!(
            self.next_needed >= db.log_base(),
            "Archive: next needed seq {} is below the log's base {} — gc reclaimed a \
             segment this archive still needed, which its retention pin exists to prevent",
            self.next_needed,
            db.log_base()
        );
        self.next_needed
    }

    /// The pin only ever moves FORWARD. It is a retention floor, and both
    /// directions of getting it wrong have already been real bugs: moving it
    /// to `u64::MAX` releases everything and let gc delete the segment the
    /// archive was waiting for (M1), while moving it BACKWARDS re-holds
    /// segments already released and freezes gc for good (M1'). Neither was
    /// caught by a test; an assert catches both by construction.
    ///
    /// RULING 49: releasing therefore does NOT go through the floor. It
    /// clears `Pin::holding` (`yield_pin`) and leaves the floor exactly
    /// where it was, so both failure modes stay unreachable by construction
    /// in a task whose whole subject is releasing the pin.
    fn hold(&self, floor: u64) {
        self.set_floor(floor, true);
    }

    /// Yields: the same monotonic floor, no longer reported to `Db`.
    fn yield_pin(&self, floor: u64) {
        self.set_floor(floor, false);
    }

    fn set_floor(&self, floor: u64, holding: bool) {
        let mut pin = self.pin.lock().expect("uncontended");
        assert!(
            floor >= pin.floor,
            "Archive: retention pin moved backwards, {} -> {floor}; a floor may only advance",
            pin.floor
        );
        pin.floor = floor;
        pin.holding = holding;
    }

    fn holding(&self) -> bool {
        self.pin.lock().expect("uncontended").holding
    }

    /// One unit of work: start one upload, poll one in flight, or read one
    /// budget's worth of a sealed segment — never more than one IO
    /// operation per call (spec §4). Never blocks, never touches `Db`'s
    /// write path (`Db::read_log` and `Db::sealed_segments` are both
    /// read-only). An archive IO failure is returned here and nowhere
    /// else — it never reaches `Db`, so it can never poison it (spec §7).
    pub fn step(&mut self, db: &Db, store: &dyn ObjectStore) -> io::Result<()> {
        // Terminal, and checked before the `mem::replace` below could
        // replace it with `Idle`. A stopped archive holds nothing, writes
        // nothing, and says nothing further: its notice was raised once,
        // when it happened (the `gap_notice` precedent), and `stopped()` is
        // the standing answer.
        if matches!(self.state, State::Stopped) {
            self.idle = true;
            return Ok(());
        }
        // SPEC §4 rule 1 / DESIGN §5, "What a `Verifying` node may do as a
        // peer". A durably-`Primary` node whose witnesses have not answered
        // has not confirmed it owns this timeline, and an upload manufactures
        // a durable copy of it that OUTLIVES this node's own fail-stop —
        // exactly what the verifying-peer policy found `begin_seeding` doing,
        // and for the identical reason: the archive arm is read-only against
        // the `Db`, so the write gate would never see it unless it is asked.
        //
        // Re-read every step, never latched: the gate opening is what makes
        // this the legitimate primary, and the archive must resume there.
        //
        // Before `yield_or_bound`, deliberately. Gating is NOT a gap: the
        // bound measures how far behind the archive is on a timeline it is
        // shipping, and this one it may not ship at all — so a notice here
        // would be a false alarm naming a continuity break that did not
        // happen. Nothing accumulates behind the gate either, since a gated
        // node accepts no writes.
        if db.verifying() {
            // `idle` is the honest answer to "is there work this archive can
            // do right now" (its doc's words), which is what a kernel arm
            // needs to stop scheduling it. Every write-gate oracle in the
            // test file therefore steps a fixed count rather than to idle.
            self.idle = true;
            return Ok(());
        }
        self.idle = false;
        // Before the transition, never inside one: the bound is about how
        // far behind the archive IS, which grows while it is stuck retrying
        // an upload — and a stuck retry is exactly what an outage leaves it
        // doing, so a check that only ran from `Idle` would never run at all
        // in the case it exists for.
        self.yield_or_bound(db)?;
        match std::mem::replace(&mut self.state, State::Idle) {
            State::Opening { started } => self.step_opening(db, store, started),
            State::ClosingGeneration { bytes, started } => {
                self.step_closing_generation(store, bytes, started)
            }
            State::Idle => self.step_idle(db),
            State::CheckingOwner {
                bytes,
                commits,
                started,
            } => self.step_checking_owner(store, bytes, commits, started),
            State::Stopped => unreachable!(
                "State::Stopped is terminal and `step` returns above before this transition; \
                 nothing ever puts the archive back into it"
            ),
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

    /// The manifest bytes for the generation as it stands: its frontier, and
    /// whatever segment chain has been built so far. Every manifest this
    /// archive writes goes through here, so the frontier can never diverge
    /// between the one written after the snapshot and the one written after
    /// each segment.
    fn manifest_bytes(&self, frontier: &Frontier) -> Vec<u8> {
        Manifest {
            generation: self.generation,
            writer: self.cfg.writer.clone(),
            applied_seq: frontier.applied_seq,
            next_job_id: frontier.next_job_id,
            tables: frontier.tables.clone(),
            segments: self.segments.clone(),
            closed: false,
        }
        .encode()
    }

    /// SPEC RULING 3 / §5. Runs before every transition (`step`).
    ///
    /// Two jobs, and they are one function because they are the same
    /// question asked in the two states: *does this archive hold the log,
    /// and may it?*
    fn yield_or_bound(&mut self, db: &Db) -> io::Result<()> {
        if !self.holding() {
            // Holding nothing — either no generation is open or a gap is
            // live. Track the log's base so the floor stays a real,
            // monotonic seq (never `u64::MAX`, ruling 49), so `next_base`'s
            // assert cannot fire on a cursor gc has passed, and so the
            // re-hold on the next contact is monotonic FOR FREE: gc only
            // ever advances the base.
            let base = db.log_base();
            if base > self.next_needed {
                self.next_needed = base;
                self.yield_pin(base);
            }
            return Ok(());
        }
        // Never abandon a started operation: `ObjectStore::start` asserts
        // against a second one while the first is in flight (spec §1), and
        // the gap below discards whatever the archive was doing. A retry
        // that has FAILED leaves `started` false, which is the state an
        // outage parks the archive in, so this costs the bound nothing.
        if self.op_in_flight() {
            return Ok(());
        }
        let sealed = db.sealed_segments()?;
        // F-A: `next_needed` is only a real cursor once `adopt` has read the
        // bucket; until then the bound measures against the tail at `open`.
        let from = self.opening_baseline.unwrap_or(self.next_needed);
        let behind = sealed.iter().filter(|s| s.base_seq >= from).count();
        if behind <= self.cfg.pin_segments {
            return Ok(());
        }
        let notice = self.gap_notice(behind);
        // The gap FORFEITS every sealed segment this archive was holding —
        // that is what yielding means, and the fresh generation's own full
        // snapshot covers all of them, since its frontier can only be at or
        // above the last record in the last of them. Carrying `next_needed`
        // across a gap the way `roll_generation` carries it across a cadence
        // roll would take those segments straight back on the next hold, the
        // bound would be over on the very next step, and the archive would
        // churn generations without ever shipping anything (the notice once
        // per churn). The new chain therefore starts at the live tail's base.
        // F-B: what makes the forfeit safe is that the fresh generation's
        // own snapshot covers every segment given up — and it can only cover
        // what the Db has APPLIED. `SealedSegment::layout_end` may sit above
        // `Db::last_applied` (its own doc says so, and `step_idle`'s gate
        // waits for exactly that case), so the forfeit stops at the first
        // sealed segment that is not fully applied rather than at the tail
        // regardless. `resume_point` refuses the same condition on the read
        // side; this is the write side of it.
        let applied = db.last_applied();
        let unapplied = sealed
            .iter()
            .find(|s| applied.is_none_or(|l| s.layout_end > l));
        let forfeit_to = self.next_needed.max(unapplied.map_or_else(
            || sealed.last().map_or(db.log_base(), |s| s.layout_end + 1),
            |s| s.base_seq,
        ));
        // Negative space (hard rule 4): forfeiting past the applied frontier
        // would leave those records in NO generation at all — not in the
        // closed one, which never shipped them, and not in the fresh one,
        // whose snapshot cannot contain what was never applied.
        //
        // R-2: the `forfeit_to == self.next_needed` disjunct is also the
        // CHURN branch. It holds exactly when the first un-archived sealed
        // segment is itself unapplied, so the forfeit does not advance and
        // the bound is over again on the next step — the very churn the
        // paragraph above says the forfeit exists to prevent (measured: 750
        // notices, nothing shipped, over 3000 steps). Kept deliberately: an
        // unsafe forfeit is traded for a churn reachable only in a strictly
        // narrower corner of the same window, since `Engine::submit` applies
        // per batch and a partial apply poisons the engine, so the frontier
        // can lag by at most one batch and this needs it to lag by eight-plus
        // segments.
        assert!(
            forfeit_to == self.next_needed
                || applied.is_some_and(|l| l.saturating_add(1) >= forfeit_to),
            "Archive: the gap is forfeiting up to seq {forfeit_to} but the Db has applied only \
             {applied:?} — the fresh generation's snapshot cannot cover records that were never \
             applied, so they would end up in no archived generation at all"
        );
        self.next_needed = forfeit_to;
        self.yield_pin(self.next_needed);
        self.gapped = true;
        self.roll_generation();
        self.state = State::Opening { started: false };
        Err(notice)
    }

    /// True while an `ObjectStore` operation this archive started has not
    /// been polled to completion.
    fn op_in_flight(&self) -> bool {
        matches!(
            self.state,
            State::Opening { started: true }
                | State::CheckingOwner { started: true, .. }
                | State::ClosingGeneration { started: true, .. }
                | State::UploadingSnapshotBatch { started: true, .. }
                | State::UploadingSnapshotMeta { started: true, .. }
                | State::UploadingSegment { started: true, .. }
                | State::UploadingManifest { started: true, .. }
        )
    }

    /// Latches this archive into `State::Stopped` and hands back the notice
    /// that put it there, so every caller reads `Err(self.stop(notice))`.
    ///
    /// The pin release is the part that is not optional. A stopped archive
    /// will never ship again, so every segment its pin holds is held
    /// FOREVER — the archive acquiring the power to stop the thing it
    /// derives from, which is exactly what spec §5's bound exists to
    /// prevent. RULING 49: the release is `Pin::holding`, never a
    /// `u64::MAX` written into the floor, so the floor stays the real,
    /// monotonic seq `pin_floor()`'s doc promises.
    fn stop(&mut self, notice: io::Error) -> io::Error {
        let floor = self.pin_floor();
        self.yield_pin(floor);
        self.state = State::Stopped;
        self.idle = true;
        notice
    }

    /// True when this archive's CURRENT generation has objects in the
    /// bucket. Two of the three answers are on `self`: `frontier` is set
    /// when a snapshot's manifest swap LANDS, and `segments` holds the
    /// chain. The third cannot be — hence the argument.
    ///
    /// `snapshot_awaiting_swap` is the window between the opening snapshot's
    /// last object landing and the manifest swap that commits it: every
    /// object of that snapshot is in the bucket and `frontier` is still
    /// `None`, because `step_uploading_manifest` sets it only once the PUT
    /// lands (spec §2 — the swap is the commit point). It is not a race but
    /// EVERY generation's first swap, and a `&self` predicate cannot see it:
    /// `step`'s `mem::replace` leaves `self.state` as `Idle` for the whole
    /// of `step_checking_owner`, so the state machine's own record of it
    /// (`CheckingOwner { commits: Some(_) }`) is not on `self` while the
    /// notice is being built. Passing it means a site that raises a refusal
    /// must ANSWER this question — a new one cannot inherit the wrong
    /// default, because omitting the argument is a compile error.
    ///
    /// Both refusal notices below are raised from sites whose answer here is
    /// OPPOSITE, and each of them used to state one site's answer
    /// unconditionally — which made it false at the others. `adopt` and
    /// `step_opening` refuse before anything is uploaded (a fresh open and a
    /// post-gap re-open both reach them with no frontier and an empty chain,
    /// since `roll_generation` clears both); `step_checking_owner` refuses
    /// part-way through a generation that already has objects under it, at
    /// the first swap as much as at any later one. Hard rule 5's third
    /// element is THE FIX, and the fix genuinely differs: at `adopt` the
    /// operator can act freely, while at `CheckingOwner` "remove that
    /// manifest" is the step that strands a partial generation for good.
    fn uploaded_this_generation(&self, snapshot_awaiting_swap: bool) -> bool {
        self.frontier.is_some() || !self.segments.is_empty() || snapshot_awaiting_swap
    }

    /// HARD RULE 5 / spec §7's row ("operator notice naming both identities,
    /// the generation, and the prefix"): the contract is archive ownership,
    /// the site is this prefix's manifest key, and the fix is a
    /// configuration change no amount of retrying reaches.
    ///
    /// What it says about the BUCKET is conditional on
    /// `uploaded_this_generation`, whose `snapshot_awaiting_swap` argument
    /// this one forwards. At `adopt` on a fresh open `self.generation` is 0,
    /// so an unconditional "objects under generation 0 are orphaned" names
    /// the generation the manifest points at — the LIVE one, intact — and
    /// tells the operator their good data is garbage. At the opening
    /// snapshot's swap the opposite sentence is the false one: "left nothing
    /// of its own under this prefix" is said over a complete snapshot that
    /// no manifest will ever name once this archive stops.
    fn foreign_writer_notice(
        &self,
        theirs: &str,
        their_generation: u64,
        snapshot_awaiting_swap: bool,
    ) -> io::Error {
        let (left_behind, strands) = if self.uploaded_this_generation(snapshot_awaiting_swap) {
            (
                format!(
                    "Detection, not prevention: the objects this archive wrote under its own \
                     generation {} are orphaned in the bucket — no manifest names them now — \
                     and nothing in this design collects them.",
                    self.generation
                ),
                " Doing that abandons this archive's own generation objects for good.",
            )
        } else {
            (
                format!(
                    "Detection, not prevention: this archive refused before uploading anything, \
                     so it has left nothing of its own under this prefix; generation \
                     {their_generation}, which that manifest names, is the live one and is \
                     untouched."
                ),
                "",
            )
        };
        io::Error::other(format!(
            "Archive: the prefix \"{}\" is owned by another writer — its manifest (\"{}\") names \
             writer \"{theirs}\" at generation {their_generation}, while this archive is \"{}\" at \
             generation {}. Two nodes archiving to one prefix both write that single key, and a \
             generation interleaved into it leaves a bucket neither node can restore from (spec \
             §2, §4), so this archive stops rather than advancing a manifest it does not own. It \
             has released its WAL retention pin so Wal::gc proceeds, and the Db is untouched. \
             {left_behind} Fix: give this node its own ArchiveConfig::prefix, or — once you have \
             confirmed \"{theirs}\" has stopped archiving here — remove that manifest object so a \
             fresh generation can open.{strands} Either way this archive resumes only on restart, \
             and restart here is the whole PROCESS: the archive is an arm of this Db, there is no \
             in-process re-open, and recovering a stopped archive therefore costs a database \
             restart.",
            self.cfg.prefix,
            manifest_key(&self.cfg.prefix),
            self.cfg.writer,
            self.generation
        ))
    }

    /// HARD RULE 5, F5 (task-6/7 carry-forward). A manifest that will not
    /// decode is an OWNERSHIP question with no answer: it is this archive's
    /// only evidence of who owns the prefix and which generation is live
    /// (spec §2, §4), so an archive that cannot read it cannot tell whether
    /// the objects under this prefix are its own, and opening a fresh
    /// generation over them is spec §4's interleave on no evidence at all.
    ///
    /// LATCHED, with no retry budget, and the retry it replaces was not
    /// free: it held the WAL at this archive's pin for as long as it lasted,
    /// which is forever (spec §5's hazard, reached without an outage). A
    /// re-fetch may well succeed — a CRC mismatch cannot distinguish a
    /// corrupt object from a corrupted read — so the notice says so and
    /// names the restart that re-reads the bucket. What it removes is
    /// retrying silently and forever, which is how an archive stops shipping
    /// without ever saying so.
    ///
    /// The decode failure is WRAPPED, never replaced: a CRC mismatch, a
    /// truncated object and a malformed body call for different operator
    /// actions and only the decoder can tell them apart.
    ///
    /// Like `foreign_writer_notice`, what it says about the bucket is
    /// conditional on `uploaded_this_generation`, whose
    /// `snapshot_awaiting_swap` argument this one forwards: it is raised
    /// from `step_opening`, where nothing has been uploaded, AND from
    /// `step_checking_owner`, where a snapshot — and at any swap past the
    /// first, part of a chain — is already sitting under the prefix.
    /// Telling that second operator "nothing was uploaded" and then offering
    /// to remove the manifest is the pair of sentences that strands a whole
    /// generation.
    fn unreadable_manifest_notice(
        &self,
        source: &io::Error,
        snapshot_awaiting_swap: bool,
    ) -> io::Error {
        let (left_behind, strands) = if self.uploaded_this_generation(snapshot_awaiting_swap) {
            (
                format!(
                    "the objects it has already written under generation {} stay in the bucket, \
                     named by no manifest it can read",
                    self.generation
                ),
                " Removing it abandons those objects too, and nothing in this design collects \
                 them.",
            )
        } else {
            ("nothing was uploaded".to_string(), "")
        };
        io::Error::other(format!(
            "Archive: refusing to archive to prefix \"{}\" — the manifest at \"{}\" could not be \
             read: {source}. That object is this archive's only evidence of who owns this prefix \
             and which generation is live (spec §2, §4), so an archive that cannot read it \
             cannot tell whether the objects here are its own or another writer's; opening a \
             fresh generation over them would be exactly the interleave spec §4 forbids, on no \
             evidence at all. This archive stops rather than guessing: {left_behind}, its \
             retention pin is released so Wal::gc proceeds, and the Db is untouched. The refusal \
             does not lapse on its own — a re-fetch may well succeed, since a CRC mismatch \
             cannot distinguish a corrupt object from a corrupted read, but retrying it silently \
             and forever is how an archive stops shipping without ever saying so. Fix: re-fetch \
             or restore that manifest object and restart this node, or — once you have confirmed \
             no other node is archiving to this prefix — remove it so the next start opens a \
             fresh generation.{strands} Restart here is the whole PROCESS: the archive is an arm \
             of this Db, there is no in-process re-open, and recovering a stopped archive \
             therefore costs a database restart.",
            self.cfg.prefix,
            manifest_key(&self.cfg.prefix)
        ))
    }

    /// HARD RULE 5, and this task's entire operator surface: a gap is the
    /// one failure here that silently costs continuity, so the text names
    /// the contract (the bound and its value), the site (the prefix and the
    /// generation), and BOTH fixes (spec §7).
    fn gap_notice(&self, behind: usize) -> io::Error {
        io::Error::other(format!(
            "Archive: gap at prefix \"{}\" — {behind} sealed WAL segments are un-archived, past \
             the configured bound of pin_segments = {}. The retention pin is released so \
             Wal::gc can proceed and the primary keeps accepting writes: on outage the archive \
             yields, never the primary (spec ruling 3). Generation {} is abandoned, and whatever \
             it had already written is orphaned in the bucket — nothing in this design collects \
             it. The next successful contact marks the generation the manifest names closed and \
             opens a fresh generation with a full snapshot; whatever the log reclaims meanwhile \
             is in no archived generation at all. Fix: raise \
             ArchiveConfig::pin_segments if the WAL can afford to hold more, or accept the new \
             generation and the gap it leaves.",
            self.cfg.prefix, self.cfg.pin_segments, self.generation
        ))
    }

    /// Closes this generation and opens the next. The ONE path behind both
    /// triggers that need it — a `snapshot_every` crossing and a gap —
    /// because they differ only in what provoked them and in whether the pin
    /// is yielded on the way. A generation carries exactly one snapshot, so
    /// a refresh cannot be a second snapshot in place: `<gen>/snapshot/**`
    /// is what the live manifest already points at.
    ///
    /// `next_needed` is deliberately CARRIED, as a FLOOR: re-deriving it from
    /// the now-empty chain would move a caught-up floor backwards onto
    /// `db.log_base()` and trip `hold`'s M1' assert. It is only a floor
    /// because the new generation's own snapshot moves it up to that
    /// snapshot's frontier segment (`chain_start`, ruling 53); whatever the
    /// old chain never shipped is in that snapshot, which is why skipping
    /// past it loses nothing.
    fn roll_generation(&mut self) {
        self.generation += 1;
        self.frontier = None;
        self.segments.clear();
    }

    /// True when this generation's snapshot is `snapshot_every` records old.
    ///
    /// RECORDS, not seqs (ruling 48): the frontier is the 0-based seq of the
    /// snapshot's last record, so the Nth record after it has seq
    /// `applied_seq + N`. Measuring from seq 0 instead would fire on the
    /// wrong multiple for every generation but the first.
    fn cadence_due(&self, db: &Db) -> bool {
        if self.cfg.snapshot_every == 0 {
            return false;
        }
        let Some(frontier) = &self.frontier else {
            return false;
        };
        db.last_applied()
            .is_some_and(|l| l >= frontier.applied_seq.saturating_add(self.cfg.snapshot_every))
    }

    /// RULING 43 / spec §4: reads `<prefix>/manifest` so this archive
    /// resumes the generation the bucket names or opens a fresh one, never
    /// blindly reuses 0. A missing manifest is a fresh bucket, not an error;
    /// every other failure is a retry, with `started` cleared so the next
    /// step re-issues the GET.
    fn step_opening(&mut self, db: &Db, store: &dyn ObjectStore, started: bool) -> io::Result<()> {
        // `self.state` is the placeholder `Idle` on entry (`step`'s
        // `mem::replace`), and unlike `step_idle` there IS something
        // resumable to lose here: an archive that fell back to `Idle` would
        // snapshot without ever having read the bucket. Every arm below
        // therefore sets the state explicitly.
        if !started {
            let key = manifest_key(&self.cfg.prefix);
            if let Err(e) = store.start(ObjectOp::Get { key }) {
                self.state = State::Opening { started: false };
                return Err(e);
            }
            self.state = State::Opening { started: true };
            return Ok(());
        }
        match store.poll() {
            None => {
                self.state = State::Opening { started: true };
                Ok(())
            }
            Some(Ok(ObjectDone::Get(bytes))) => match Manifest::decode(&bytes) {
                Ok(m) => self.adopt(db, Some(m)),
                Err(e) => {
                    // F5. Untrusted bytes that will not decode: refuse, do
                    // not retry. See `unreadable_manifest_notice` for why
                    // this is terminal rather than a bounded retry.
                    //
                    // `false`: `Opening` is in front of everything, so this
                    // generation has uploaded nothing whatsoever.
                    let notice = self.unreadable_manifest_notice(&e, false);
                    Err(self.stop(notice))
                }
            },
            Some(Ok(other)) => unreachable!(
                "a Get was started; only ObjectDone::Get or an error can complete it, got \
                 {other:?}"
            ),
            // A bucket this archive has never written to. Not a failure.
            Some(Err(e)) if e.kind() == io::ErrorKind::NotFound => self.adopt(db, None),
            Some(Err(e)) => {
                self.state = State::Opening { started: false };
                Err(e)
            }
        }
    }

    /// Decides, from what the bucket already holds, whether this archive
    /// extends that generation or opens a fresh one (ruling 43).
    fn adopt(&mut self, db: &Db, found: Option<Manifest>) -> io::Result<()> {
        // SPEC §4 rule 2, at the FIRST of the two places it has to be asked,
        // and the one the pre-swap check cannot cover. An archive that opens
        // over another writer's manifest declines to EXTEND that generation
        // (`resume_point`) and then opens a fresh one NUMBERED ABOVE it — so
        // by the time it reaches a swap the foreign manifest sits BELOW its
        // own generation, and a check that asked "is the other writer AHEAD
        // of me" would wave it through. It is still another node's manifest
        // and advancing it is still the interleave spec §8 forbids. Refusing
        // here is what makes "ownership is decided before anything is
        // uploaded, not after" true: measured before this landed, an archive
        // opening over node-b's generation 3 wrote a complete generation 4
        // over it and swapped the manifest to name itself.
        if let Some(m) = &found
            && m.writer != self.cfg.writer
        {
            // `false`: `adopt` runs before this archive has uploaded
            // anything at all — no snapshot is in flight to a swap here.
            let notice = self.foreign_writer_notice(&m.writer, m.generation, false);
            return Err(self.stop(notice));
        }
        // The resume decision reads the segment layout (`chain_start`) and
        // so can fail. It is taken before anything below is mutated, and a
        // failure returns to `Opening` rather than to `Idle`: an archive
        // half-adopted here would snapshot without having finished reading
        // the bucket, which is the very thing ruling 43 put this state in
        // front of everything for.
        let resume = match &found {
            Some(m) => match self.resume_point(db, m) {
                Ok(at) => at,
                Err(e) => {
                    self.state = State::Opening { started: false };
                    return Err(e);
                }
            },
            None => None,
        };
        self.state = State::Idle;
        // The bucket has answered: `next_needed` becomes a real cursor below
        // and the bound measures against it from here (F-A).
        self.opening_baseline = None;
        let Some(m) = found else {
            // Nothing in the bucket: generation 0 on a first open, and on a
            // gap whatever `roll_generation` already moved to — a gap spends
            // a generation whether or not the closed one ever wrote, so the
            // counter advances by exactly one either way (ruling 50).
            self.hold_or_yield_for_generation(db);
            return Ok(());
        };
        if let Some(resume_at) = resume {
            self.generation = m.generation;
            self.segments = m.segments;
            self.frontier = Some(Frontier {
                applied_seq: m.applied_seq,
                next_job_id: m.next_job_id,
                tables: m.tables,
            });
            self.next_needed = resume_at;
            self.hold(resume_at);
            return Ok(());
        }
        // A fresh generation over an existing one. `max` rather than
        // `m.generation + 1` alone: after a gap this archive has ALREADY
        // rolled, and re-deriving from the manifest would either double the
        // increment or reuse a number whose objects are in the bucket. With
        // one writer both forms are the same +1, which is what ruling 50's
        // exactly-one assertion pins.
        self.generation = self.generation.max(m.generation + 1);
        self.segments.clear();
        self.frontier = None;
        self.hold_or_yield_for_generation(db);
        // Spec §2: a gap CLOSES the generation it broke. The manifest still
        // names it until the fresh generation's snapshot lands, which can be
        // a long time, and "stale" and "finished" are different facts to a
        // restore. Only ever this archive's own generation — annotating
        // another writer's is spec §4's refusal, never a repair — which the
        // ownership check at the top of this function now guarantees
        // outright, so the writer conjunct that used to stand here would be
        // dead. This is also the one manifest swap that does NOT go through
        // `CheckingOwner`, and deliberately: its bytes come from a manifest
        // GET that completed in the immediately preceding step, so it is
        // already a checked swap and a second GET would only re-read what
        // `adopt` has just decided on.
        if !m.closed {
            let closed = Manifest { closed: true, ..m };
            self.state = State::ClosingGeneration {
                bytes: closed.encode(),
                started: false,
            };
        }
        Ok(())
    }

    /// With no generation open the archive needs nothing from the log
    /// (module docs, ruling 46), so it yields rather than holding at a base
    /// whose records its future snapshot will cover anyway.
    fn hold_or_yield_for_generation(&mut self, db: &Db) {
        self.next_needed = self.next_needed.max(db.log_base());
        self.yield_pin(self.next_needed);
    }

    /// The seq a resumed generation continues its chain at, or `None` when
    /// this generation must not be extended. Every refusal below is a reason
    /// the extension would produce a chain that claims more than it covers.
    ///
    /// A FOREIGN writer is no longer among them: Task 7's "decline to extend
    /// somebody else's chain" was the conservative interim, and `adopt` now
    /// refuses to archive at all before this is ever called (spec §4 rule
    /// 2), so a branch for it here would be dead code claiming a decision it
    /// no longer makes.
    fn resume_point(&self, db: &Db, m: &Manifest) -> io::Result<Option<u64>> {
        if self.gapped {
            // gc has taken records this generation never shipped: extending
            // it is exactly the hole spec §2 says a generation never has.
            return Ok(None);
        }
        if m.closed {
            return Ok(None);
        }
        // Negative space, checked rather than asserted because these bytes
        // crossed the network: a chain starting ABOVE its own snapshot's
        // frontier already fails to cover the records between them.
        if m.segments
            .first()
            .is_some_and(|s| s.base_seq > m.applied_seq.saturating_add(1))
        {
            return Ok(None);
        }
        let at = match m.segments.last() {
            Some(last) => match last.end_seq.checked_add(1) {
                Some(at) => at,
                None => return Ok(None),
            },
            // No chain yet: RULING 53 puts its start where this generation's
            // snapshot stops, never BELOW it — resuming at the log's base
            // re-introduces the overlap on every restart, for as long as the
            // generation lives. (It may still land ON the log's base, when
            // that is the base of the frontier's own segment; that is not
            // the overlap — see the module doc.) The refusal below is what makes the boundary
            // derivable at all: everything from `applied_seq + 1` up must
            // still be in the local log, so the segment holding it is intact
            // and at or above the base.
            None => {
                if db.log_base() > m.applied_seq.saturating_add(1) {
                    return Ok(None);
                }
                chain_start(db, m.applied_seq)?
            }
        };
        // And everything from there must still be in the local log.
        if db.log_base() > at {
            return Ok(None);
        }
        Ok(Some(at))
    }

    fn step_closing_generation(
        &mut self,
        store: &dyn ObjectStore,
        bytes: Vec<u8>,
        mut started: bool,
    ) -> io::Result<()> {
        let key = manifest_key(&self.cfg.prefix);
        let done = put_step(store, &key, &bytes, &mut started);
        self.state = match done {
            Ok(true) => State::Idle,
            _ => State::ClosingGeneration { bytes, started },
        };
        done.map(|_| ())
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
        // `snapshot_every` is a count of committed records (spec §3), and
        // `last_applied` is the highest applied SEQ — seqs are 0-based, so
        // the count is one MORE than the seq. Comparing the raw seq would
        // take every snapshot one record later than the configured cadence
        // says, which no test of the cadence's own unit would notice.
        let applied_records = last_applied.saturating_add(1);
        if self.cfg.snapshot_every > 0 && applied_records < self.cfg.snapshot_every {
            return Ok(false);
        }
        let snapshot = db.snapshot()?;
        // Read off the snapshot, not off `db`: this is the frontier the rows
        // below actually come from.
        let Some(applied_seq) = snapshot.applied_seq()? else {
            return Ok(false);
        };
        let next_job_id = snapshot.next_job_id()?;
        // RULING 53 / spec §2: the chain this generation is about to build
        // CONTINUES this snapshot rather than overlapping it. Sited here,
        // before the walk is committed to `self.state`, for two reasons: the
        // frontier is fixed by the MVCC view above, so every row below it
        // belongs to this generation whatever the WAL does next; and a `?`
        // on this line leaves the archive exactly where it was, with nothing
        // half-moved.
        //
        // Deliberately NOT deferred to the manifest swap that commits the
        // snapshot. The rows below the frontier come from the table store,
        // not the WAL, so a snapshot that never lands costs nothing it
        // released: the next generation simply snapshots at a later
        // frontier. Waiting would instead keep the whole log held for the
        // length of a full snapshot upload — which is exactly the startup
        // gap this task exists to remove.
        self.next_needed = chain_start(db, applied_seq)?;
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
            let frontier = Frontier {
                applied_seq: meta.applied_seq,
                next_job_id: meta.next_job_id,
                tables: meta.tables.iter().map(|t| t.name.clone()).collect(),
            };
            let manifest = self.manifest_bytes(&frontier);
            self.state = State::CheckingOwner {
                bytes: manifest,
                commits: Some(frontier),
                started: false,
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
        // A `snapshot_every` crossing closes this generation and opens the
        // next — the same `roll_generation` a gap performs, on a different
        // trigger (module docs). Safe here and nowhere else: `Idle` is the
        // one state with nothing in flight and nothing half-written.
        if self.cadence_due(db) {
            self.roll_generation();
        }
        // The generation's snapshot comes before anything else it contains,
        // and no manifest is written until it has landed (module docs). A
        // segment shipped first would need a manifest to name it, and that
        // manifest would carry a frontier no snapshot backs.
        if self.frontier.is_none() {
            if self.open_snapshot(db)? {
                // From here the archive is building a chain continuing
                // from the frontier `open_snapshot` has just moved
                // `next_needed` to (ruling 53), and it needs the log again:
                // hold (M1), and the gap — if there was one — is closed by
                // the generation now opening.
                self.hold(self.next_needed);
                self.gapped = false;
                return Ok(());
            }
            // No generation to open: the cadence has not arrived, or the Db
            // has applied nothing. The chain has not started, so the archive
            // needs NOTHING from the log — holding here is ruling 46's
            // configuration-triggered WAL growth, against a store that may
            // be perfectly healthy.
            self.hold_or_yield_for_generation(db);
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
                let frontier = self.frontier.as_ref().expect(
                    "Archive: a segment landed for a generation with no snapshot — step_idle \
                     ships nothing until the opening snapshot's manifest has itself landed, so \
                     the manifest built here can never lack a frontier",
                );
                self.manifest_bytes(frontier)
            };
            self.state = State::CheckingOwner {
                bytes: manifest_bytes,
                commits: None,
                started: false,
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

    /// SPEC §4 rule 2. Reads `<prefix>/manifest` and refuses to swap it if
    /// it names anyone but this writer.
    ///
    /// The comparison is IDENTITY ALONE, with no generation conjunct. A
    /// foreign manifest at a generation below this archive's is still
    /// another node's manifest, and `a_foreign_manifest_below_this_archives_
    /// generation_stops_it_too` is the oracle: a generation comparison
    /// answers "who is further along", which is not the question spec §8's
    /// invariant asks.
    ///
    /// A `NotFound` is a bucket with no manifest at all — nothing to
    /// overwrite, and the ordinary first swap of a fresh generation. Every
    /// other store failure retries with `started` cleared, exactly as the
    /// upload arms do.
    fn step_checking_owner(
        &mut self,
        store: &dyn ObjectStore,
        bytes: Vec<u8>,
        commits: Option<Frontier>,
        started: bool,
    ) -> io::Result<()> {
        if !started {
            let key = manifest_key(&self.cfg.prefix);
            if let Err(e) = store.start(ObjectOp::Get { key }) {
                self.state = State::CheckingOwner {
                    bytes,
                    commits,
                    started: false,
                };
                return Err(e);
            }
            self.state = State::CheckingOwner {
                bytes,
                commits,
                started: true,
            };
            return Ok(());
        }
        match store.poll() {
            None => {
                self.state = State::CheckingOwner {
                    bytes,
                    commits,
                    started: true,
                };
                Ok(())
            }
            Some(Ok(ObjectDone::Get(found))) => match Manifest::decode(&found) {
                // `commits.is_some()` is the evidence `&self` does not
                // carry: this is the swap committing the OPENING SNAPSHOT,
                // so every one of its objects is already in the bucket
                // while `self.frontier` is still `None`. A `commits: None`
                // swap is a segment's, and `self.segments` is non-empty by
                // then, so the predicate's other disjuncts answer it.
                Ok(m) if m.writer != self.cfg.writer => {
                    let notice =
                        self.foreign_writer_notice(&m.writer, m.generation, commits.is_some());
                    Err(self.stop(notice))
                }
                Ok(_) => {
                    self.state = State::UploadingManifest {
                        bytes,
                        started: false,
                        commits,
                    };
                    Ok(())
                }
                // F5, at the second of the two sites that decode a
                // manifest. An archive part-way through a generation that
                // finds this unreadable is in exactly the position one at
                // `Opening` is: it cannot tell whether the prefix is still
                // its own, and must not resolve that by overwriting the
                // evidence.
                Err(e) => {
                    let notice = self.unreadable_manifest_notice(&e, commits.is_some());
                    Err(self.stop(notice))
                }
            },
            Some(Ok(other)) => unreachable!(
                "a Get was started; only ObjectDone::Get or an error can complete it, got \
                 {other:?}"
            ),
            // No manifest yet: this generation's first swap creates it.
            Some(Err(e)) if e.kind() == io::ErrorKind::NotFound => {
                self.state = State::UploadingManifest {
                    bytes,
                    started: false,
                    commits,
                };
                Ok(())
            }
            Some(Err(e)) => {
                self.state = State::CheckingOwner {
                    bytes,
                    commits,
                    started: false,
                };
                Err(e)
            }
        }
    }

    fn step_uploading_manifest(
        &mut self,
        store: &dyn ObjectStore,
        bytes: Vec<u8>,
        mut started: bool,
        commits: Option<Frontier>,
    ) -> io::Result<()> {
        let key = manifest_key(&self.cfg.prefix);
        let done = put_step(store, &key, &bytes, &mut started);
        if let Ok(true) = done {
            // The manifest swap landed: whatever this manifest names —
            // the generation's snapshot, a segment, or both — is now
            // durably reflected off-box, so the pin may release it. The
            // NEXT `Idle` step re-holds at the floor computed here (another
            // sealed segment may already be waiting, or none is).
            if let Some(frontier) = commits {
                self.frontier = Some(frontier);
            }
            if let Some(last) = self.segments.last() {
                self.next_needed = last.end_seq.checked_add(1).unwrap_or_else(|| {
                    panic!(
                        "Archive: the last shipped segment's end_seq is u64::MAX with more to \
                         ship after it — Manifest::encode would refuse this chain (hard rule 4); \
                         unreachable while this archive only ever appends contiguous SegmentRefs"
                    )
                });
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
