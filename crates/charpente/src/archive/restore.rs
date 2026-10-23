//! Restore — rebuilding a `Db`'s data directory from an archive (spec §6).
//!
//! The archive's other half. Everything Tasks 5-9 ship is worth exactly what
//! can be read back, so this is where the bucket stops being an artifact and
//! becomes a database again.
//!
//! **One code path, no analogue.** `restore` takes `&dyn ObjectStore` +
//! `Box<dyn Disk>` and nothing else that touches the world: no `Clock`, no
//! `Entropy`, no `Net`, no thread, no kernel. The simulator drives THIS
//! function, byte for byte the one the CLI calls (spec §6's closing line, and
//! the plan's own doc comment). There is no test-only variant to drift.
//!
//! ## The order, and why each step is where it is
//!
//! 1. GET the manifest and the generation's snapshot meta; cross-check them.
//! 2. Read the DESTINATION's own frontier, frame by frame, off `Disk`.
//! 3. Download and CRC-verify EVERY segment object, holding none.
//! 4. Decide divergence, and refuse — or, under `--force`, say what is about
//!    to be destroyed BEFORE destroying it.
//! 5. Only now `Seeder::new`, which discards the destination's timeline.
//! 6. Import the snapshot batches, `finish` at the frontier, then apply the
//!    chain.
//!
//! Steps 1-4 write nothing. That is what makes "a corrupt segment object is
//! refused, NEVER imported" true rather than aspirational: the refusal
//! happens while the destination is still whatever it was. It costs one extra
//! GET per segment (step 3 verifies and drops; step 6 fetches again to apply)
//! and that is the deliberate trade — a chain is unbounded in length, so
//! holding it to avoid the second GET would put an O(generation) allocation
//! on a machine that is by definition already having a bad day. Peak memory
//! here is one object.
//!
//! ## Why the chain is applied rather than laid into `wal/`
//!
//! Spec §6 step 4 says "lay it into `wal/`". That was written before anyone
//! checked what `Seeder` hands back: `Seeder::new` CONSUMES the `Box<dyn
//! Disk>` and neither `Seeder` nor `Db` lends it out again, so laying files
//! between `rows` and `finish` needs a new public accessor in core. It is not
//! needed. `Db::apply_shipped` is the follower's own append-and-apply path,
//! already public, already the most heavily faulted path in the tree, and it
//! re-frames every record through `Wal::append` — so a restored node's log is
//! built by the same code that builds a follower's. The archived frames go in
//! verbatim, in seq order, and land at the identical seqs. Spec §6 is updated
//! to say so in the same commit (hard rule 6).
//!
//! The chain's first segment carries the records between its own base and the
//! snapshot's frontier — structural residue, spec §2 — which the snapshot
//! already holds. Those frames are dropped by arithmetic (`seq >
//! last_applied`), never by search, exactly as §2 says.

use std::io;

use charpente_core::db::{Db, DbConfig, Schema, Seeder};
use charpente_core::io::{Disk, ObjectDone, ObjectOp, ObjectStore};
use charpente_core::job::{JobRow, JobStatus};
use charpente_core::log::{MAX_RECORD_BYTES, frame_crc};
use charpente_core::table::View;

use super::manifest::{
    Manifest, SegmentRef, SnapshotMeta, decode_snapshot_batch, manifest_key, segment_key,
    snapshot_batch_key, snapshot_meta_key,
};

/// Rows per page while counting the restored job queue. Same bound and the
/// same reason as the archive's own `SNAPSHOT_BATCH_ROWS`: a restored queue
/// is unbounded and the count must not be an O(store) allocation.
const JOB_SCAN_ROWS: usize = 1_000;

/// The WAL frame header: `seq(8) | payload_len(4) | crc(4)`. Not imported
/// from `charpente_core::log` because it is private there; asserted against
/// reality instead — every frame this walker accepts is re-parsed by core's
/// own `read_frame` when the restored `Db` next recovers, so a header layout
/// that disagreed with core would surface as a failed open, never as a silent
/// import.
const FRAME_HEADER_BYTES: usize = 16;

/// Where to restore from, and whether the operator has accepted the loss.
#[derive(Clone, Debug)]
pub struct RestoreOpts {
    /// The bucket key prefix a generation was archived under — the same
    /// string `ArchiveConfig::prefix` carried when it was written.
    pub prefix: String,
    /// Discard a destination timeline this archive cannot prove is its own.
    /// An operator-invoked destructive admin action, not an escape hatch on
    /// a running guarantee (spec §6): it bypasses no invariant the system
    /// upholds at runtime, and it names what it destroys before acting.
    pub force: bool,
}

/// What a completed restore rebuilt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreReport {
    /// The generation the manifest named.
    pub generation: u64,
    /// The frontier the restored `Db` came up at — the newest recoverable
    /// point (ruling 2), which is the chain's end when it has one and the
    /// snapshot's own frontier when it does not.
    pub applied_seq: u64,
    /// Segments in the restored chain.
    pub segments: usize,
    /// `charpente.jobs` rows that came back Pending — jobs the restored node
    /// WILL dispatch. Restore can therefore re-fire emails and third-party
    /// calls that already ran before the loss (spec §6): correct behaviour,
    /// since they are durable transactional state and idempotency keys are
    /// what make the re-fire exactly-once-observable, but a consequence the
    /// operator has to know about in advance rather than discover.
    pub pending_jobs: u64,
}

/// Rebuild a `Db`'s data directory from an archive, to the newest recoverable
/// point (ruling 2). Core-testable: the simulator drives this exact function,
/// not an analogue.
///
/// `notice` carries operator-facing output the CLI prints. It exists because
/// a value returned in `RestoreReport` cannot say anything BEFORE the fact,
/// and spec §6 requires `--force` to name what it destroys before it acts.
pub fn restore(
    store: &dyn ObjectStore,
    disk: Box<dyn Disk>,
    cfg: DbConfig,
    schema: Schema,
    opts: RestoreOpts,
    notice: &mut dyn FnMut(&str),
) -> io::Result<RestoreReport> {
    // THE ONE PLACE EITHER CLAIM IS MADE, and no message below may repeat
    // either clause — measured at Task 11: `check_generation` hardcoded
    // "nothing in the destination has been touched" at both of its refusals,
    // so the wrapper DOUBLED the sentence in real operator output ("…Refusing;
    // nothing in the destination has been touched. Nothing in the destination
    // has been touched"). Still true, so not a correctness bug; still a
    // hard-rule-5 defect, because a sentence an operator reads twice reads as
    // a program that lost its place. Every failure below reaches the
    // operator through exactly one of these two wrappers, so "nothing was
    // touched" and "a partial restore is sitting there" cannot drift apart
    // per message — which is how this defect arrived (Task 8's R-1, its
    // NEW-1 regression, and this task's C-1 are the same class three times:
    // a sentence that is true where it was written and false at another of
    // its sites). `Seeder::new` is INSIDE `rebuild` on purpose: it discards
    // the store file before it discards `wal/`, so a failure part-way
    // through it has already touched the destination.
    let plan = prepare(store, &*disk, &cfg, &opts, notice).map_err(untouched)?;
    rebuild(store, disk, cfg, schema, &plan, &opts.prefix, notice).map_err(discarded)
}

/// A generation, read and fully verified, with the destination judged
/// against it — and not one byte written.
struct Plan {
    m: Manifest,
    meta: SnapshotMeta,
}

/// Everything restore does BEFORE the destination is touched: read the
/// generation, verify every segment object frame by frame, read the
/// destination's own frontier, and decide whether replacing it is allowed.
fn prepare(
    store: &dyn ObjectStore,
    disk: &dyn Disk,
    cfg: &DbConfig,
    opts: &RestoreOpts,
    notice: &mut dyn FnMut(&str),
) -> io::Result<Plan> {
    let prefix = opts.prefix.as_str();

    // ---- 1. the generation pointer, and the snapshot it names.
    // The manifest gets its own absence text. `get`'s NotFound says "the
    // manifest names it, so this generation is incomplete", which is true of
    // every object a manifest names and false of the manifest itself — and
    // the manifest is the one an operator reaches by mistyping `--from` or
    // `--prefix`, which is the likeliest way to see this message at all
    // (measured end-to-end at Task 11 through `charpente restore`).
    let manifest_bytes = get(store, &manifest_key(prefix), prefix).map_err(|e| {
        if e.kind() != io::ErrorKind::NotFound {
            return e;
        }
        io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "restore: there is no manifest at \"{}\" — prefix \"{prefix}\" holds no \
                 archive, so there is nothing here to restore from. Check the bucket and the \
                 prefix against the ArchiveConfig the node archived with",
                manifest_key(prefix)
            ),
        )
    })?;
    let m = Manifest::decode(&manifest_bytes).map_err(|e| {
        if e.kind() == io::ErrorKind::Unsupported {
            return e;
        }
        io::Error::new(
            e.kind(),
            format!(
                "restore: the manifest at \"{}\" will not decode ({e}) — refusing to guess at \
                 a generation pointer. Re-fetch the object, or restore it from the bucket's \
                 own versioning",
                manifest_key(prefix)
            ),
        )
    })?;
    let meta_key = snapshot_meta_key(prefix, m.generation);
    let meta_bytes = get(store, &meta_key, prefix)?;
    let meta = SnapshotMeta::decode(&meta_bytes).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "restore: generation {} names a snapshot meta at \"{meta_key}\" that will not \
                 decode ({e})",
                m.generation
            ),
        )
    })?;
    check_generation(&m, &meta, &meta_key)?;

    // ---- 2. the destination's own frontier, read frame by frame.
    //
    // Deliberately NOT through `Db::open`: it fails on exactly the
    // directories an operator restores (an unopenable store beside a
    // compacted log, a base above the durable horizon, a poisoned log), and
    // it takes the `Box<dyn Disk>` by value and drops it on `Err` — so a
    // failed probe would consume the disk and leave `--force` nothing to
    // force.
    let chain = ChainSpan::of(&m.segments);
    let local = local_frontier(disk, cfg, chain.as_ref().map(|c| c.end))?;

    // ---- 3. download and verify the whole chain, holding none of it.
    let compare_at = local
        .as_ref()
        .zip(chain.as_ref())
        .map(|(l, c)| l.seq.min(c.end));
    let archive_crc = verify_chain(store, prefix, &m, compare_at)?;

    // ---- 4. divergence, decided from frames both sides actually hold.
    let verdict = judge(local.as_ref(), chain.as_ref(), compare_at, archive_crc);
    if let Some(detail) = verdict.refusal(&m) {
        if !opts.force {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "restore: {detail} Refusing rather than overwriting it — restore never \
                     merges timelines, it replaces or refuses (spec §8). Re-run with --force \
                     to discard the destination's timeline and replace it with generation {}",
                    m.generation
                ),
            ));
        }
        notice(&format!(
            "--force: about to discard the destination's timeline. {detail} This is \
             irreversible and nothing here is archived; it happens before any restore write."
        ));
    }
    Ok(Plan { m, meta })
}

/// Everything restore does once the destination is being replaced.
///
/// `Seeder::new` discards the store file, the horizon breadcrumb and every
/// `wal/` segment, dir-barriered (DESIGN §5) — which is also why a crashed
/// restore converges on re-run rather than resuming across itself.
fn rebuild(
    store: &dyn ObjectStore,
    disk: Box<dyn Disk>,
    cfg: DbConfig,
    schema: Schema,
    plan: &Plan,
    prefix: &str,
    notice: &mut dyn FnMut(&str),
) -> io::Result<RestoreReport> {
    let Plan { m, meta } = plan;
    let mut seeder = Seeder::new(disk, cfg, schema)?;
    for t in &meta.tables {
        for n in 0..t.batches {
            let key = snapshot_batch_key(prefix, m.generation, &t.name, n);
            let bytes = get(store, &key, prefix)?;
            let rows = decode_snapshot_batch(&t.name, seeder.table_scoped(&t.name)?, &bytes)
                .map_err(|e| {
                    io::Error::new(
                        e.kind(),
                        format!("restore: snapshot batch \"{key}\" will not decode ({e})"),
                    )
                })?;
            seeder.rows(&t.name, &rows)?;
        }
    }
    let mut db = seeder.finish(
        meta.applied_seq,
        meta.next_job_id,
        meta.next_event_id,
        meta.frontier_crc,
    )?;

    // The chain, applied through the follower's own path.
    for s in &m.segments {
        let key = segment_key(prefix, m.generation, s.base_seq);
        let bytes = get(store, &key, prefix)?;
        let frames = verify_segment(&bytes, s, &key, m.generation, Stage::AfterImport)?;
        let from = db.last_applied().map_or(0, |l| l + 1);
        let frames: Vec<(u64, Vec<u8>)> =
            frames.into_iter().filter(|(seq, _)| *seq >= from).collect();
        db.apply_shipped(&frames)?;
    }

    let applied_seq = db.last_applied().unwrap_or(meta.applied_seq);
    let pending_jobs = count_pending_jobs(&db)?;
    if pending_jobs > 0 {
        notice(&format!(
            "restore: the restored database comes back with {pending_jobs} pending job(s), \
             which WILL dispatch when this node starts — restore can re-fire emails and \
             third-party calls that already ran before the loss (spec §6). They are durable \
             transactional state and this is correct; idempotency keys are what make the \
             re-fire exactly-once-observable"
        ));
    }
    Ok(RestoreReport {
        generation: m.generation,
        applied_seq,
        segments: m.segments.len(),
        pending_jobs,
    })
}

/// The promise every pre-write refusal carries — and it is TRUE by
/// construction, because `prepare` has no write path at all.
fn untouched(e: io::Error) -> io::Error {
    io::Error::new(
        e.kind(),
        format!("{e}. Nothing in the destination has been touched"),
    )
}

/// The warning every post-discard failure carries. The state it describes is
/// not a corrupted directory — it is a database that OPENS, at an earlier
/// frontier of the archived timeline or at nothing at all, and serves that
/// state to whoever starts it. Restore cannot prevent that from outside
/// (`Db::open` would have to learn about a restore-in-progress marker, spec
/// §6), so naming it is the whole mitigation and it has to be on every path
/// that produces the state.
fn discarded(e: io::Error) -> io::Error {
    io::Error::new(
        e.kind(),
        format!(
            "{e}. The destination has been discarded and now holds a PARTIAL restore: its \
             own timeline was replaced before this failed, so what is there is neither the \
             database it was nor the one the archive holds. Do not start a node on it — it \
             will open and serve stale or empty state. Re-run restore against the same \
             directory once the cause above is fixed; restore discards and rebuilds from \
             scratch, so a re-run converges rather than resuming"
        ),
    )
}

// -------------------------------------------------------------- the bucket

/// Polls per operation before restore gives up on the store.
///
/// A LIVENESS ASSERTION, not a tuning constant (hard rule 4, negative
/// space): `ObjectStore::poll` returning `None` means "still in flight", so
/// a store that answers `None` this many times in a row is a store that will
/// never answer. Same number and same reasoning as the two bounded drains
/// this subsystem already has — `charpente-sim/src/object.rs`'s, whose
/// comment says an unbounded loop "would silently hang the instant a future
/// task adds latency" while its own header names latency as future work, and
/// `charpente/tests/archive.rs`'s. `restore` is the PRODUCTION path, so it
/// is the one that must not hang: under Task 12's fault matrix a hang fails
/// with no seed and no repro line, which is the worst outcome hard rule 5
/// admits.
const POLLS_PER_OP: usize = 10_000;

/// Drives one `ObjectStore` operation to completion.
///
/// `restore` is a one-shot that owns the process while it runs — it is not a
/// kernel arm and has nothing to yield to — so the poll loop is the whole
/// loop. It is bounded because exhausting the bound is a real answer an
/// operator can act on, and spinning forever is not.
fn run_op(store: &dyn ObjectStore, op: ObjectOp) -> io::Result<ObjectDone> {
    let described = describe(&op);
    store.start(op)?;
    for _ in 0..POLLS_PER_OP {
        if let Some(done) = store.poll() {
            return done;
        }
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "restore: the object store never completed {described} — it answered \"still in \
             flight\" to {POLLS_PER_OP} consecutive polls, which is a store that will never \
             answer rather than a slow one. Check the store's reachability and credentials, \
             then re-run restore"
        ),
    ))
}

fn describe(op: &ObjectOp) -> String {
    match op {
        ObjectOp::Get { key } => format!("a Get of \"{key}\""),
        ObjectOp::Put { key, .. } => format!("a Put of \"{key}\""),
        ObjectOp::Delete { key } => format!("a Delete of \"{key}\""),
        ObjectOp::List { prefix } => format!("a List of \"{prefix}\""),
    }
}

fn get(store: &dyn ObjectStore, key: &str, prefix: &str) -> io::Result<Vec<u8>> {
    match run_op(
        store,
        ObjectOp::Get {
            key: key.to_string(),
        },
    ) {
        Ok(ObjectDone::Get(bytes)) => Ok(bytes),
        // Negative space (hard rule 4): a store answering a Get with a Put's
        // completion is a broken impl, not a runtime condition.
        Ok(other) => panic!("ObjectStore answered Get \"{key}\" with {other:?}"),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "restore: object \"{key}\" is missing from prefix \"{prefix}\" — the manifest \
                 names it, so this generation is incomplete and restore refuses rather than \
                 treating an absent object as an empty one. Restore the object, or archive a \
                 fresh generation from a live node"
            ),
        )),
        Err(e) => Err(io::Error::new(
            e.kind(),
            format!("restore: reading \"{key}\" from prefix \"{prefix}\" failed: {e}"),
        )),
    }
}

/// The two objects that describe a generation must describe the SAME one.
/// They are written by the same walk but are separate objects with no
/// ordering guarantee between them (spec §1), so a bucket where one is stale
/// is reachable and must be named rather than silently preferred.
fn check_generation(m: &Manifest, meta: &SnapshotMeta, meta_key: &str) -> io::Result<()> {
    let disagreement = if m.applied_seq != meta.applied_seq {
        Some(format!(
            "frontier ({} in the manifest, {} in the snapshot meta)",
            m.applied_seq, meta.applied_seq
        ))
    } else if m.next_job_id != meta.next_job_id {
        Some(format!(
            "job counter ({} in the manifest, {} in the snapshot meta)",
            m.next_job_id, meta.next_job_id
        ))
    } else if m.tables.len() != meta.tables.len()
        || m.tables
            .iter()
            .zip(&meta.tables)
            .any(|(name, t)| *name != t.name)
    {
        Some(format!(
            "table list ({:?} in the manifest, {:?} in the snapshot meta)",
            m.tables,
            meta.tables.iter().map(|t| &t.name).collect::<Vec<_>>()
        ))
    } else {
        None
    };
    if let Some(disagreement) = disagreement {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "restore: generation {} disagrees with itself on the {disagreement} — \
                 \"{meta_key}\" is not the snapshot this manifest names. Refusing",
                m.generation
            ),
        ));
    }
    // Spec §2: the chain CONTINUES the snapshot. A chain starting above the
    // first record the snapshot does not carry leaves those records in no
    // part of the generation at all — the one thing §2 says a generation
    // never has.
    if let Some(first) = m.segments.first()
        && first.base_seq > meta.applied_seq + 1
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "restore: generation {}'s chain starts at base_seq {} but its snapshot's \
                 frontier is {} — records {}..={} are in neither the snapshot nor the chain, \
                 so this generation has a hole and cannot be restored from. Refusing. \
                 Restore an earlier generation, or archive a fresh one from a live node",
                m.generation,
                first.base_seq,
                meta.applied_seq,
                meta.applied_seq + 1,
                first.base_seq - 1
            ),
        ));
    }
    Ok(())
}

// -------------------------------------------------------------- the frames

/// One frame the walker accepted: its seq and the checksum it carries.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct FrameMark {
    seq: u64,
    crc: u32,
}

/// Result of walking `bytes` as a WAL segment based at `base_seq`.
struct FrameWalk {
    /// The last frame that verified, if any.
    last: Option<FrameMark>,
    /// The checksum of the frame at the caller's `want` seq, if the walk
    /// reached it.
    wanted: Option<u32>,
    /// True when the walk consumed the bytes exactly — every frame verified
    /// and nothing was left over. False means it stopped early, at `seq`.
    clean: bool,
    /// The seq the walk stopped at when `clean` is false.
    stopped_at: u64,
}

/// Walks `bytes` as the WAL's own frame format, verifying every frame's
/// checksum and its seq against the position it must occupy. Stops at the
/// first frame that fails either check.
///
/// This is the only frame parser outside `charpente-core`, and it is one
/// because there is no public core API for it: `read_frame` is private and
/// `Wal::open` consumes a `Box<dyn Disk>`. It is not an unchecked duplicate —
/// `Db::apply_shipped` re-frames and re-checksums every payload this returns
/// through `Wal::append`, and `submit_batch` refuses a seq that is not the
/// one the log expects, so a walker that disagreed with core produces a named
/// refusal, never a silent import.
fn walk_frames(bytes: &[u8], base_seq: u64, want: Option<u64>) -> (FrameWalk, Vec<(u64, Vec<u8>)>) {
    let mut off = 0usize;
    let mut seq = base_seq;
    let mut last = None;
    let mut wanted = None;
    let mut frames = Vec::new();
    loop {
        if off == bytes.len() {
            return (
                FrameWalk {
                    last,
                    wanted,
                    clean: true,
                    stopped_at: seq,
                },
                frames,
            );
        }
        if off + FRAME_HEADER_BYTES > bytes.len() {
            break;
        }
        let header = &bytes[off..off + FRAME_HEADER_BYTES];
        let framed_seq = u64::from_le_bytes(header[0..8].try_into().expect("sliced 8 bytes"));
        let payload_len =
            u32::from_le_bytes(header[8..12].try_into().expect("sliced 4 bytes")) as usize;
        let stored_crc = u32::from_le_bytes(header[12..16].try_into().expect("sliced 4 bytes"));
        if payload_len == 0
            || payload_len > MAX_RECORD_BYTES
            || off + FRAME_HEADER_BYTES + payload_len > bytes.len()
        {
            break;
        }
        let payload = &bytes[off + FRAME_HEADER_BYTES..off + FRAME_HEADER_BYTES + payload_len];
        if framed_seq != seq || frame_crc(framed_seq, payload) != stored_crc {
            break;
        }
        if want == Some(seq) {
            wanted = Some(stored_crc);
        }
        last = Some(FrameMark {
            seq,
            crc: stored_crc,
        });
        frames.push((seq, payload.to_vec()));
        seq += 1;
        off += FRAME_HEADER_BYTES + payload_len;
    }
    (
        FrameWalk {
            last,
            wanted,
            clean: false,
            stopped_at: seq,
        },
        frames,
    )
}

/// Which of `verify_segment`'s two call sites is asking. The refusal's
/// closing clause differs between them and MUST: the pre-write pass can
/// honestly say corruption is never imported, and the apply pass — reached
/// when an object changes in the bucket between restore's two GETs — cannot,
/// because the snapshot's rows went in minutes earlier. A message that names
/// a contract the code does not keep at that site is worse than silence
/// (hard rule 5); it is the sentence the operator acts on.
#[derive(Clone, Copy)]
enum Stage {
    BeforeAnyWrite,
    AfterImport,
}

impl Stage {
    fn refusal_clause(self) -> &'static str {
        match self {
            Stage::BeforeAnyWrite => "Refusing; corruption is never imported",
            Stage::AfterImport => "Refusing",
        }
    }
}

/// Verifies one downloaded segment object against what the manifest claims of
/// it, and returns its frames.
fn verify_segment(
    bytes: &[u8],
    s: &SegmentRef,
    key: &str,
    generation: u64,
    stage: Stage,
) -> io::Result<Vec<(u64, Vec<u8>)>> {
    let (walk, frames) = walk_frames(bytes, s.base_seq, None);
    if !walk.clean {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "restore: segment object \"{key}\" fails frame verification at seq {} — its \
                 CRC, its length or its seq does not match the frame the log requires there. \
                 Generation {generation} claims this segment spans {}..={}. {}. Restore the \
                 object, or restore an earlier generation",
                walk.stopped_at,
                s.base_seq,
                s.end_seq,
                stage.refusal_clause()
            ),
        ));
    }
    match walk.last {
        Some(mark) if mark.seq == s.end_seq => Ok(frames),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "restore: segment object \"{key}\" ends at {} but generation {generation}'s \
                 manifest claims it spans {}..={} — every frame's CRC verified, so this is a \
                 short or truncated object, not corruption. Refusing; a generation is either \
                 continuous or it is finished (spec §2)",
                other.map_or("nothing".to_string(), |m| m.seq.to_string()),
                s.base_seq,
                s.end_seq
            ),
        )),
    }
}

/// GETs and verifies every segment the manifest names, holding none of them,
/// and returns the checksum the archive's own frame at `compare_at` carries.
fn verify_chain(
    store: &dyn ObjectStore,
    prefix: &str,
    m: &Manifest,
    compare_at: Option<u64>,
) -> io::Result<Option<u32>> {
    let mut archive_crc = None;
    for s in &m.segments {
        let key = segment_key(prefix, m.generation, s.base_seq);
        let bytes = get(store, &key, prefix)?;
        let (walk, _frames) = walk_frames(&bytes, s.base_seq, compare_at);
        if !walk.clean || walk.last.is_none_or(|l| l.seq != s.end_seq) {
            // Re-run the shared refusal so the two paths cannot drift.
            verify_segment(&bytes, s, &key, m.generation, Stage::BeforeAnyWrite)?;
        }
        if let Some(crc) = walk.wanted {
            archive_crc = Some(crc);
        }
    }
    Ok(archive_crc)
}

// --------------------------------------------------------- the destination

/// What the destination's own log proves about itself: the lowest seq it
/// still retains, its frontier, and that frontier's checksum — all read, none
/// inferred.
struct LocalFrontier {
    base: u64,
    seq: u64,
    crc: u32,
    /// The destination's own checksum at the seq the caller asked about —
    /// the archive's chain end, which is the comparison point whenever the
    /// destination has run PAST the archive. `None` when the destination
    /// holds no verifiable frame there.
    crc_at_want: Option<u32>,
}

/// The destination's frontier, read frame by frame off `Disk`, plus its
/// checksum at `want`.
///
/// Segments are visited highest-base first because a seeded or freshly
/// restored node's log is an EMPTY segment sitting above the last one that
/// holds records — "the newest file" and "the newest frame" are not the same
/// question. The walk stops as soon as it has the frontier AND has settled
/// `want`, where reaching a segment based at or below `want` settles it: a
/// seq not found in the segment that would contain it is not findable in a
/// lower one.
fn local_frontier(
    disk: &dyn Disk,
    cfg: &DbConfig,
    want: Option<u64>,
) -> io::Result<Option<LocalFrontier>> {
    let dir = format!("{}/", cfg.wal.dir);
    let names = disk.list(&dir)?;
    let Some(first) = names.first() else {
        return Ok(None);
    };
    let base = segment_base(&cfg.wal.dir, first)?;
    let mut frontier: Option<FrameMark> = None;
    let mut crc_at_want: Option<u32> = None;
    let mut want_settled = want.is_none();
    for name in names.iter().rev() {
        if frontier.is_some() && want_settled {
            break;
        }
        let seg_base = segment_base(&cfg.wal.dir, name)?;
        let file = disk.open(name)?;
        let len = file.len()?;
        let mut bytes = vec![
            0u8;
            usize::try_from(len).map_err(|_| {
                io::Error::other(format!(
                    "restore: the destination's segment \"{name}\" is {len} bytes, past this \
                 platform's addressable size"
                ))
            })?
        ];
        if !bytes.is_empty() {
            file.read_at(0, &mut bytes)?;
        }
        let (walk, _frames) = walk_frames(&bytes, seg_base, want);
        if let Some(crc) = walk.wanted {
            crc_at_want = Some(crc);
            want_settled = true;
        }
        if frontier.is_none() {
            frontier = walk.last;
        }
        if want.is_some_and(|w| seg_base <= w) {
            want_settled = true;
        }
    }
    Ok(frontier.map(|mark| LocalFrontier {
        base,
        seq: mark.seq,
        crc: mark.crc,
        crc_at_want,
    }))
}

/// `"{dir}/{base:020}.log"`, parsed. The naming rule lives in
/// `charpente_core::log::segment_name`, which is `pub(crate)` there — so this
/// is a named coupling, not a helper. Anything else in the log directory is
/// refused rather than recovered around, exactly as core's own
/// `parse_segment_name` refuses it.
fn segment_base(dir: &str, name: &str) -> io::Result<u64> {
    let bad = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "restore: unexpected file \"{name}\" in the destination's log directory — \
                 segments are named \"{dir}/<seq:020>.log\". Refusing to read a frontier out \
                 of a directory this is not"
            ),
        )
    };
    let stem = name
        .strip_prefix(&format!("{dir}/"))
        .and_then(|s| s.strip_suffix(".log"))
        .ok_or_else(bad)?;
    if stem.len() != 20 || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    stem.parse::<u64>().map_err(|_| bad())
}

// ------------------------------------------------------------- divergence

/// The seqs a generation's chain actually holds frames for.
struct ChainSpan {
    start: u64,
    end: u64,
}

impl ChainSpan {
    fn of(segments: &[SegmentRef]) -> Option<ChainSpan> {
        Some(ChainSpan {
            start: segments.first()?.base_seq,
            end: segments.last()?.end_seq,
        })
    }
}

/// What the destination is, relative to the generation about to replace it.
enum Verdict {
    /// Nothing to compare — the destination holds no frame at all.
    Empty,
    /// The two agree at a frame both hold, and the destination holds nothing
    /// above the archive.
    Agrees,
    /// They agree, but the destination holds records the archive does not.
    Ahead { local: u64, chain_end: u64 },
    /// They hold different records at the same seq.
    Diverged {
        seq: u64,
        local_crc: u32,
        archive_crc: u32,
        local: u64,
    },
    /// There is no seq at which the two can be compared at all.
    Unrelated { base: u64, local: u64 },
}

/// Compares the two frontiers at the highest seq BOTH sides hold a frame for,
/// which is the only seq at which the question can be answered from evidence.
fn judge(
    local: Option<&LocalFrontier>,
    chain: Option<&ChainSpan>,
    compare_at: Option<u64>,
    archive_crc: Option<u32>,
) -> Verdict {
    let Some(local) = local else {
        return Verdict::Empty;
    };
    // No chain at all: the generation is a snapshot and nothing else, so
    // there is no archived FRAME to compare anything against — the records
    // it holds are folded into the snapshot's rows.
    let (Some(chain), Some(seq)) = (chain, compare_at) else {
        return Verdict::Unrelated {
            base: local.base,
            local: local.seq,
        };
    };
    // The comparison frame has to be one BOTH sides hold. Below the chain's
    // start the archive's copy is inside the snapshot rather than in an
    // object; below the destination's base it has been GC'd away locally.
    if seq < chain.start || seq < local.base {
        return Verdict::Unrelated {
            base: local.base,
            local: local.seq,
        };
    }
    let archive_crc = archive_crc.expect(
        "the chain was verified frame by frame across its whole span, so a seq inside it \
         always yields a checksum — a None here is a bug in verify_chain, not a bucket state",
    );
    // `compare_at` is `min(local.seq, chain.end)`, so `seq` is either the
    // destination's own frontier or the chain's end — and the second is
    // exactly the seq `local_frontier` was asked to fetch a checksum for.
    let local_crc = if seq == local.seq {
        local.crc
    } else {
        match local.crc_at_want {
            Some(crc) => crc,
            None => {
                return Verdict::Unrelated {
                    base: local.base,
                    local: local.seq,
                };
            }
        }
    };
    if local_crc != archive_crc {
        // Negative space (hard rule 4). `refusal` renders `local - seq + 1`
        // as the record count an operator is about to destroy, and `seq` is
        // `min(local.seq, chain.end)` — so the ordering holds by
        // construction HERE and is invisible over there. Asserting it at the
        // site that establishes it is what keeps the count from being a
        // wrapped u64 in release, where the workspace sets no
        // `overflow-checks`.
        assert!(
            seq <= local.seq,
            "judge: comparison seq {seq} is above the destination's own frontier {}",
            local.seq
        );
        return Verdict::Diverged {
            seq,
            local_crc,
            archive_crc,
            local: local.seq,
        };
    }
    if local.seq > chain.end {
        return Verdict::Ahead {
            local: local.seq,
            chain_end: chain.end,
        };
    }
    Verdict::Agrees
}

impl Verdict {
    /// The refusal's substance, or `None` when the restore may proceed. One
    /// string, used verbatim both as the `Err` an un-forced restore returns
    /// and as the notice `--force` emits before acting, so the two can never
    /// describe different destruction.
    fn refusal(&self, m: &Manifest) -> Option<String> {
        match self {
            Verdict::Empty | Verdict::Agrees => None,
            Verdict::Diverged {
                seq,
                local_crc,
                archive_crc,
                local,
            } => Some(format!(
                "the destination holds a diverged timeline: at seq {seq} its record checksums \
                 {local_crc:#010x} while generation {}'s carries {archive_crc:#010x}. The \
                 destination's frontier is {local}, the archive's is {}. Discarding it loses \
                 the {} record(s) from {seq} to {local}, which exist nowhere else.",
                m.generation,
                m.segments.last().map_or(m.applied_seq, |s| s.end_seq),
                local.saturating_sub(*seq).saturating_add(1)
            )),
            Verdict::Ahead { local, chain_end } => Some(format!(
                "the destination is ahead of the archive: the two agree through seq \
                 {chain_end}, but the destination's frontier is {local}. Generation {}'s \
                 frontier is {chain_end}. Discarding it loses the {} record(s) above \
                 {chain_end}, which exist nowhere else.",
                m.generation,
                local.saturating_sub(*chain_end)
            )),
            Verdict::Unrelated { base, local } => Some(format!(
                "the destination holds a timeline this archive cannot compare itself against: \
                 its log retains seqs {base}..={local}, and generation {}'s chain holds frames \
                 for {}. There is no seq at which the two can be checked, so the destination \
                 is not shown to be diverged and is not shown to be this timeline either. \
                 Discarding it loses the {} record(s) its log retains.",
                m.generation,
                m.segments.first().map_or_else(
                    || "no seqs at all — the generation is a snapshot with no chain yet"
                        .to_string(),
                    |f| format!(
                        "{}..={}",
                        f.base_seq,
                        m.segments.last().expect("first implies last").end_seq
                    )
                ),
                local.saturating_sub(*base).saturating_add(1)
            )),
        }
    }
}

// ------------------------------------------------------------------ report

/// Pending `charpente.jobs` rows in the restored `Db` — paged, never an
/// O(queue) allocation.
fn count_pending_jobs(db: &Db) -> io::Result<u64> {
    let snap = db.snapshot()?;
    let mut from = 0u64;
    let mut pending = 0u64;
    loop {
        let rows = snap.scan_from::<JobRow>(from, JOB_SCAN_ROWS)?;
        let Some(last) = rows.last() else {
            return Ok(pending);
        };
        from = last.id.raw() + 1;
        pending += rows
            .iter()
            .filter(|r| matches!(r.status, JobStatus::Pending))
            .count() as u64;
        if rows.len() < JOB_SCAN_ROWS {
            return Ok(pending);
        }
    }
}
