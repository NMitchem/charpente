//! The write-ahead log: the one place where things become true (MANIFESTO
//! §5). Segmented, checksummed, append-only.
//!
//! Frame: `[seq: u64 le][len: u32 le][crc: u32 le][payload]`, crc over
//! seq‖len‖payload. Segments are named `<dir>/<base_seq:020>.log`; rotation
//! syncs the old segment before the new one takes writes, so a `sync` on the
//! tail is always a durability barrier for the whole log. Rotation also
//! barriers the new segment's *name* with `Disk::sync_dir` before returning —
//! `File::sync` durably persists a file's bytes but not its directory entry,
//! so an unbarriered rotation could lose a synced segment's name at crash
//! (DESIGN §2).
//!
//! Recovery scans frames in order and stops at the first invalid one: later
//! segments hold only unacknowledged or stale data and are deleted, and the
//! cut segment is truncated at the invalid frame. Two orderings are
//! load-bearing. (1) The later segments are removed FIRST — highest base
//! first, each barriered with `sync_dir` before the next (`gc`'s discipline)
//! — and only then is the cut segment truncated. Shortening the cut segment
//! while a later segment still exists on disk risks a durable gap: truncation
//! and removal are separate durability events, so a crash between them that
//! keeps the truncation but drops the removals strands a shortened segment
//! beside a surviving orphan — a gap the gaplessness check then refuses to
//! recover across, bricking the node on data it was entitled to discard.
//! Remove-then-truncate makes every crash-intermediate a contiguous prefix
//! instead, so recovery re-finds this exact cut and is idempotent at every
//! crash point (found via replication seed 0x4177819b4a2ba984). (2) The
//! truncation is synced before the log accepts appends. Without it, a
//! discarded frame of the same length as its replacement can survive beyond
//! the new frame, checksum-valid and seq-correct, and a later recovery would
//! resurrect a record this recovery chose to discard. Each removal is
//! barriered with `sync_dir` for the reason `gc` is — a later-segment name
//! resurrected by an unsynced removal would reintroduce the exact stale data
//! the cut just discarded (and, now, reopen the gap the ordering closes).
//!
//! Recovery syncs each segment's bytes before handing any of its frames to
//! `on_record` — the same append→sync→apply order `submit` keeps, applied to
//! what recovery reads back. Readable is not durable: a frame the previous
//! process wrote and never synced (it died first, or its fsync failed) is
//! still in the page cache after a process restart and reads back valid, and
//! replaying it unsynced let the store persist a record the next power loss
//! took from the log (the store outran the log, realtime seed
//! 0x7df8c43a1d2591e8). A segment whose sync fails hands nothing to replay.
//! The sync closes the process-death case. It only narrows the failed-fsync
//! one: on Linux a failed fsync can mark the pages clean, so a frame can stay
//! readable without ever reaching the disk, and this sync then succeeds
//! without writing it (DESIGN §2).
//!
//! Every successful `open` also barriers the directory before returning,
//! including the clean recovery path that cut nothing. A `sync_dir` that
//! FAILS leaves the names it covered non-durable while the segment file
//! remains on disk, so the next open finds a non-empty directory, recovers a
//! perfectly valid log, and — before this barrier existed — returned without
//! ever re-establishing the name, then acknowledged appends into a segment a
//! crash could delete outright (seed 0xb957ceaaf1716fd5).
//!
//! Any IO failure poisons the log: after a failed fsync the durability of
//! prior writes is unknown and retrying cannot honestly clear that (the
//! fsyncgate lesson). The only recovery from poison is a restart.
//!
//! `Wal::gc` deletes whole segments once a checkpoint no longer needs them:
//! it removes segments ascending from `base_seq`, barriering after each
//! removal, so a crash mid-gc can resurrect at most the most-recently-removed
//! segment — never a gap between what's retained and what recovery expects.
//! `Wal::open` starts recovery's expected seq at the first remaining
//! segment's base instead of 0.
//!
//! `Wal::open_with_horizon` (DESIGN §2's recovery-horizon rule): plain
//! `open` cannot tell a torn tail from corruption of an already-durable,
//! already-applied record — both are just "the first invalid frame". A
//! caller that knows the durable horizon (`Db::open`, from the store's
//! `applied_seq`) can pass it in: an invalid frame at-or-below the horizon
//! is a hard corruption error instead of a cut, because truncating it would
//! silently discard acknowledged data no cut is ever entitled to lose.

use std::fmt;
use std::io;

use crate::crc::Crc32;
use crate::io::{Disk, File};

pub const MAX_RECORD_BYTES: usize = 1 << 24; // 16 MiB
const HEADER_BYTES: u64 = 16;

#[derive(Clone, Debug)]
pub struct WalConfig {
    /// Directory prefix inside the `Disk` namespace.
    pub dir: String,
    /// Rotation threshold; a segment holds at least one frame regardless.
    pub segment_bytes: u64,
}

impl Default for WalConfig {
    fn default() -> WalConfig {
        WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 64 << 20,
        }
    }
}

pub struct Wal {
    disk: Box<dyn Disk>,
    cfg: WalConfig,
    tail: Box<dyn File>,
    tail_offset: u64,
    next_seq: u64,
    /// Lowest seq the log still retains; recovery starts here instead of 0
    /// once `gc` has run.
    base_seq: u64,
    poisoned: bool,
}

// `disk`/`tail` are `Box<dyn Disk>`/`Box<dyn File>`, neither `Debug`-bound —
// same reasoning as `Engine`'s hand-written impl (engine.rs), and needed for
// the same reason: `Result::unwrap_err` on a `Wal`-returning call needs some
// `Debug` bound on the `Ok` side (used by tests).
impl fmt::Debug for Wal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wal")
            .field("cfg", &self.cfg)
            .field("tail_offset", &self.tail_offset)
            .field("next_seq", &self.next_seq)
            .field("base_seq", &self.base_seq)
            .field("poisoned", &self.poisoned)
            .finish()
    }
}

impl Wal {
    /// Opens the log, replaying every recovered record into `on_record` in
    /// seq order, and leaves the log ready to append after the last one. An
    /// `on_record` error aborts recovery. Standalone behavior: no durable
    /// horizon, so a corrupt frame is always a cut — see
    /// `open_with_horizon`.
    pub fn open(
        disk: Box<dyn Disk>,
        cfg: WalConfig,
        on_record: impl FnMut(u64, &[u8]) -> io::Result<()>,
    ) -> io::Result<Wal> {
        Wal::open_with_horizon(disk, cfg, None, on_record)
    }

    /// Like `open`, but `horizon` (the store's `applied_seq` at `Db::open` —
    /// DESIGN §2's recovery-horizon rule) distinguishes two kinds of invalid
    /// frame: at-or-below the horizon, corruption has hit an already-durable,
    /// already-applied record, and recovery refuses to silently drop it (a
    /// hard error, naming the frame, the horizon, and the restore path);
    /// above it, the frame is an ordinary torn tail and recovery cuts there
    /// exactly as `open` (horizon `None`) always has. The hard-error path
    /// mutates nothing — it returns before any truncation or deletion runs,
    /// so a caller that retries with a wider (or no) horizon still sees
    /// today's cut behavior, untouched.
    ///
    /// Every frame handed to `on_record` is durable: each segment is synced
    /// before its first frame is read, and a failed sync returns `Err` before
    /// any of that segment's frames reaches `on_record` (module docs).
    pub fn open_with_horizon(
        disk: Box<dyn Disk>,
        cfg: WalConfig,
        horizon: Option<u64>,
        mut on_record: impl FnMut(u64, &[u8]) -> io::Result<()>,
    ) -> io::Result<Wal> {
        assert!(
            !cfg.dir.is_empty() && !cfg.dir.contains(':'),
            "WalConfig.dir must be a plain name"
        );
        let prefix = format!("{}/", cfg.dir);
        let names = disk.list(&prefix)?;

        if names.is_empty() {
            let tail = disk.open(&segment_name(&cfg.dir, 0))?;
            disk.sync_dir(&prefix)?;
            return Ok(Wal {
                disk,
                cfg,
                tail,
                tail_offset: 0,
                next_seq: 0,
                base_seq: 0,
                poisoned: false,
            });
        }

        // Re-establish the durability of every name we are about to treat as a
        // real log, BEFORE replaying a single record. A `sync_dir` that FAILS
        // leaves the names it covered non-durable while the segment files stay
        // on disk (fsyncgate: a failed barrier guarantees nothing), so the next
        // open finds a non-empty directory, recovers a perfectly valid log, and
        // would otherwise acknowledge appends into a segment a crash can delete
        // outright (seed 0xb957ceaaf1716fd5, pinned in charpente-sim/tests/wal.rs).
        //
        // BEFORE the replay loop, not after it, and the ordering is
        // load-bearing: `Engine::recover` applies each recovered record to the
        // store from inside this function's `on_record` callback, so a failure
        // AFTER replay returns `Err` having already advanced the store — which
        // `Db::open` then meets as "store applied_seq N is ahead of the durable
        // log end" (db.rs). Barriering here fails before the store is touched.
        // The cut arm's own barrier is kept: it must run before the truncation
        // is treated as durable, which is a stronger ordering than this one.
        disk.sync_dir(&prefix)?;

        let mut expected_seq = parse_segment_name(&cfg.dir, &names[0])?;
        let base_seq = expected_seq;
        let mut tail: Option<(Box<dyn File>, u64)> = None;
        for (index, name) in names.iter().enumerate() {
            let base = parse_segment_name(&cfg.dir, name)?;
            if base != expected_seq {
                return Err(corrupt(format!(
                    "log segment \"{name}\" begins at seq {base} but seq {expected_seq} was expected — \
                     a segment is missing or this directory mixes two logs; refusing to guess"
                )));
            }
            let file = disk.open(name)?;
            // Readable is not durable. A frame the previous process wrote and
            // never synced — it died first, or its fsync failed and poisoned
            // the log — is still in the page cache, and reads back valid. Make
            // the segment's bytes durable BEFORE any of its frames reaches
            // `on_record`: replay applies them to the store, which can persist
            // them (`Db::open`'s checkpoint, or merely the store's close), and
            // a power loss after that would leave the store ahead of the log
            // (realtime seed 0x7df8c43a1d2591e8). A sync that fails returns
            // here, before replay: it guarantees nothing about these bytes
            // (fsyncgate), so none of them may be applied. The error keeps
            // the disk's own kind and text (fault classification reads it).
            file.sync().map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "recovery could not make log segment \"{name}\" durable before replaying \
                         it: {e} — contract: no frame is replayed before its segment is synced \
                         (DESIGN §2), so none of this segment's frames were replayed; fix: \
                         restart to retry recovery, and if this sync keeps failing the disk is \
                         failing"
                    ),
                )
            })?;
            let len = file.len()?;
            let mut offset = 0u64;
            let cut = loop {
                match read_frame(&*file, len, offset, expected_seq)? {
                    Frame::Valid { payload } => {
                        on_record(expected_seq, &payload)?;
                        expected_seq += 1;
                        offset += HEADER_BYTES + payload.len() as u64;
                    }
                    Frame::Invalid => {
                        if horizon.is_some_and(|h| expected_seq <= h) {
                            return Err(corrupt(format!(
                                "log frame {expected_seq} is invalid but at-or-below the store's \
                                 durable horizon {h} — this is corruption of acknowledged records, \
                                 not a torn tail, and truncating it would silently lose data; \
                                 restore this node from a replica or backup. {reseed} \
                                 (DESIGN §2)",
                                h = horizon.expect("checked by is_some_and"),
                                reseed = crate::db::RESTORE_IS_A_RESEED,
                            )));
                        }
                        break Some(offset);
                    }
                    Frame::CleanEnd => break None,
                }
            };
            match cut {
                Some(cut) => {
                    // Orphaned later segments hold only post-cut data and go
                    // FIRST — highest base first, each barriered with `sync_dir`
                    // before the next, exactly as `gc` removes ascending and
                    // barriers each. Only once every later segment is durably
                    // gone is the cut segment truncated: the order is the
                    // invariant. Truncating first would make the truncation and
                    // the removals two separate durability events in the wrong
                    // order — a crash that keeps the truncation but drops the
                    // removals leaves a shortened segment beside a surviving
                    // orphan, a durable gap the gaplessness check then refuses
                    // to recover across (seed 0x4177819b4a2ba984). Removing
                    // highest-first keeps every crash-intermediate a contiguous
                    // prefix, so recovery re-finds this exact cut and is
                    // idempotent at every crash point (module docs).
                    let later = &names[index + 1..];
                    for name in later.iter().rev() {
                        disk.remove(name)?;
                        disk.sync_dir(&prefix)?;
                    }
                    if !later.is_empty() {
                        // Negative space, checked against the disk itself: no
                        // segment past the cut point may still exist before the
                        // truncation runs. A `list` fault here is ordinary
                        // recovery fallout (`?`); a surviving later segment is
                        // a broken invariant (a `remove` returned Ok yet left
                        // the name), never recovered around.
                        let remaining = disk.list(&prefix)?;
                        assert!(
                            remaining
                                .iter()
                                .all(|n| match parse_segment_name(&cfg.dir, n) {
                                    Ok(other) => other <= base,
                                    Err(_) => true, // foreign files are refused elsewhere
                                }),
                            "recovery is about to shorten segment {base} while a later segment \
                             still exists — orphaned segments must be removed before the cut \
                             segment is truncated (gaplessness, seed 0x4177819b4a2ba984): \
                             {remaining:?}"
                        );
                    }
                    assert!(
                        cut <= len,
                        "recovery cut {cut} is past segment length {len} — the cut is an \
                         offset the scan reached inside the file"
                    );
                    if cut != len {
                        // Load-bearing truncation; see the module docs. `!=`
                        // rather than `<`: `cut == len` never reaches here (a
                        // scan that ends at `len` yields `CleanEnd`, not a
                        // cut), so `<`'s mutant (`<=`) changed nothing
                        // reachable, where `!=`'s (`==`) skips every real
                        // truncation and three tests go red (2026-09-10 sweep,
                        // log.rs:263).
                        file.set_len(cut)?;
                        file.sync()?;
                    }
                    disk.sync_dir(&prefix)?; // recovery leaves a durably-clean directory
                    tail = Some((file, cut));
                    break;
                }
                None => tail = Some((file, len)),
            }
        }
        let (tail, tail_offset) = tail.expect("names was non-empty, so a tail segment exists");
        Ok(Wal {
            disk,
            cfg,
            tail,
            tail_offset,
            next_seq: expected_seq,
            base_seq,
            poisoned: false,
        })
    }

    /// Appends one record, buffered — not durable until `sync` returns `Ok`.
    pub fn append(&mut self, payload: &[u8]) -> io::Result<u64> {
        assert!(
            !payload.is_empty() && payload.len() <= MAX_RECORD_BYTES,
            "contract: WAL records are 1..={MAX_RECORD_BYTES} bytes — got {}",
            payload.len()
        );
        self.check_poison()?;
        let frame_len = HEADER_BYTES + payload.len() as u64;
        // `!= 0` rather than `> 0` on a u64: same gate, and its mutant (`== 0`)
        // never rotates a full segment where `> 0`'s (`>= 0`) re-opened the
        // same name and no oracle could see it (2026-09-10 sweep, log.rs:296).
        if self.tail_offset != 0 && self.tail_offset + frame_len > self.cfg.segment_bytes {
            self.rotate()?;
        }
        let mut frame = Vec::with_capacity(frame_len as usize);
        frame.extend_from_slice(&self.next_seq.to_le_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&frame_crc(self.next_seq, payload).to_le_bytes());
        frame.extend_from_slice(payload);
        self.guard(self.tail.write_at(self.tail_offset, &frame))?;
        let seq = self.next_seq;
        self.next_seq += 1;
        self.tail_offset += frame_len;
        Ok(seq)
    }

    /// The durability barrier: on `Ok`, every appended record survives any
    /// crash. Rotation synced earlier segments already, so the tail suffices.
    pub fn sync(&mut self) -> io::Result<()> {
        self.check_poison()?;
        self.guard(self.tail.sync())
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Lowest seq the log still retains; recovery starts here.
    pub fn base_seq(&self) -> u64 {
        self.base_seq
    }

    /// Reads back already-appended frames `[from_seq, up_to]`, re-verifying
    /// every CRC (the shipper's read surface — a frame corrupted on disk
    /// after append must never travel). Stops early once `max_bytes` of
    /// payload have accumulated, but always returns at least one frame when
    /// any is available. `up_to` must not exceed the durable end — callers
    /// pass the engine's `last_applied`.
    pub fn read_from(
        &self,
        from_seq: u64,
        up_to: u64,
        max_bytes: usize,
    ) -> io::Result<Vec<(u64, Vec<u8>)>> {
        if from_seq < self.base_seq {
            return Err(corrupt(format!(
                "read_from({from_seq}) but the log's base is {} — those frames are GC'd; \
                 ship a snapshot instead (DESIGN §5)",
                self.base_seq
            )));
        }
        assert!(
            up_to < self.next_seq,
            "read_from up_to {up_to} is past the log end {}",
            self.next_seq
        );
        let prefix = format!("{}/", self.cfg.dir);
        let names = self.disk.list(&prefix)?;
        let mut out = Vec::new();
        let mut bytes = 0usize;
        for (index, name) in names.iter().enumerate() {
            let base = parse_segment_name(&self.cfg.dir, name)?;
            // A segment's end is the next segment's base (the tail segment
            // ends at next_seq); skip whole segments that end at or before
            // from_seq without even opening them.
            let seg_end = match names.get(index + 1) {
                Some(next) => parse_segment_name(&self.cfg.dir, next)?,
                None => self.next_seq,
            };
            if seg_end <= from_seq {
                continue;
            }
            let file = self.disk.open(name)?;
            let len = file.len()?;
            let mut offset = 0u64;
            let mut seq = base;
            while seq <= up_to {
                match read_frame(&*file, len, offset, seq)? {
                    Frame::Valid { payload } => {
                        let flen = payload.len();
                        if seq >= from_seq {
                            bytes += flen;
                            out.push((seq, payload));
                            if bytes >= max_bytes {
                                return Ok(out);
                            }
                        }
                        seq += 1;
                        offset += HEADER_BYTES + flen as u64;
                    }
                    Frame::CleanEnd => break,
                    Frame::Invalid => {
                        return Err(corrupt(format!(
                            "log frame {seq} failed verification while being read back for \
                             shipping — a corrupted frame must never be shipped; durable bytes \
                             below the acknowledged end are corrupt; restore this node from a \
                             replica or backup. {reseed} (DESIGN §2)",
                            reseed = crate::db::RESTORE_IS_A_RESEED,
                        )));
                    }
                }
            }
            if seq > up_to {
                break;
            }
        }
        Ok(out)
    }

    /// Deletes whole segments every frame of which is below `keep_from`,
    /// always keeping at least `retain_segments` segments. The directory
    /// barrier runs after each removal, so a crash can resurrect at most
    /// the most-recently-removed segment — always adjacent to the retained
    /// set, never a gap (a resurrected segment below a durable checkpoint
    /// replays as no-ops through the store's skip_through gate, once
    /// `Db::open` seeds recovery from `base_seq` — T3).
    pub fn gc(&mut self, keep_from: u64, retain_segments: usize) -> io::Result<()> {
        self.check_poison()?;
        let prefix = format!("{}/", self.cfg.dir);
        let names = self.guard(self.disk.list(&prefix))?;
        let mut bases = Vec::with_capacity(names.len());
        for name in &names {
            bases.push(parse_segment_name(&self.cfg.dir, name)?);
        }
        let deletable = names.len().saturating_sub(retain_segments.max(1));
        for i in 0..deletable {
            // names[i]'s frames end where names[i + 1] begins.
            if bases[i + 1] > keep_from {
                break;
            }
            self.guard(self.disk.remove(&names[i]))?;
            self.guard(self.disk.sync_dir(&prefix))?;
            self.base_seq = bases[i + 1];
        }
        Ok(())
    }

    /// True after any IO failure on this log (fsyncgate: durability of
    /// everything since the last good sync is unknown). The only exit is
    /// restart-and-recover.
    pub fn poisoned(&self) -> bool {
        self.poisoned
    }

    /// Reclaims the underlying `Disk` handle, discarding all log state — the
    /// seed path (`Db::into_seeder`, db.rs): the whole timeline is about to
    /// be replaced, so nothing here needs preserving beyond the raw disk.
    pub(crate) fn into_disk(self) -> Box<dyn Disk> {
        self.disk
    }

    /// Borrows the `Disk` handle without consuming the log — `Db::checkpoint`
    /// (db.rs) needs it to write the horizon breadcrumb sidecar (Design A,
    /// `.superpowers/sdd/horizon-collapse-investigation.md`) after the
    /// store's own checkpoint persists. Plumbing only; the log's own
    /// recovery/cut logic is untouched by the breadcrumb.
    pub(crate) fn disk(&self) -> &dyn Disk {
        self.disk.as_ref()
    }

    fn rotate(&mut self) -> io::Result<()> {
        self.guard(self.tail.sync())?; // sync-before-rotate keeps `sync` a whole-log barrier
        let name = segment_name(&self.cfg.dir, self.next_seq);
        self.tail = self.guard(self.disk.open(&name))?;
        // The new segment's name must be durable before frames land in it —
        // the sync-before-rotate rule extended to the directory entry.
        self.guard(self.disk.sync_dir(&format!("{}/", self.cfg.dir)))?;
        self.tail_offset = 0;
        Ok(())
    }

    fn check_poison(&self) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::other(
                "WAL poisoned by an earlier IO failure — durability is unknown and fsync cannot \
                 honestly be retried; restart and recover",
            ));
        }
        Ok(())
    }

    fn guard<T>(&mut self, result: io::Result<T>) -> io::Result<T> {
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
}

enum Frame {
    Valid { payload: Vec<u8> },
    Invalid,
    CleanEnd,
}

/// A header that does not fit before the end of the file is a torn tail.
/// Its own function so the instrument can name it: at `offset + HEADER_BYTES
/// == len` the header fits and the payload-length check refuses the frame
/// anyway, so the `>=` mutant here is equivalent — excluded by name in
/// `.cargo/mutants.toml`, where the argument is repeated.
fn torn_header(offset: u64, len: u64) -> bool {
    offset + HEADER_BYTES > len
}

fn read_frame(file: &dyn File, len: u64, offset: u64, expected_seq: u64) -> io::Result<Frame> {
    if offset == len {
        return Ok(Frame::CleanEnd);
    }
    if torn_header(offset, len) {
        return Ok(Frame::Invalid);
    }
    let mut header = [0u8; HEADER_BYTES as usize];
    file.read_at(offset, &mut header)?;
    let seq = u64::from_le_bytes(header[0..8].try_into().expect("sliced 8 bytes"));
    let payload_len = u64::from(u32::from_le_bytes(
        header[8..12].try_into().expect("sliced 4 bytes"),
    ));
    let stored_crc = u32::from_le_bytes(header[12..16].try_into().expect("sliced 4 bytes"));
    if payload_len == 0
        || payload_len > MAX_RECORD_BYTES as u64
        || offset + HEADER_BYTES + payload_len > len
    {
        return Ok(Frame::Invalid);
    }
    let mut payload = vec![0u8; payload_len as usize];
    file.read_at(offset + HEADER_BYTES, &mut payload)?;
    if frame_crc(seq, &payload) != stored_crc || seq != expected_seq {
        // A checksum-valid frame with the wrong seq is stale or misdirected
        // data; either way it is not the next record — cut here.
        return Ok(Frame::Invalid);
    }
    Ok(Frame::Valid { payload })
}

/// The frame checksum: both replication sides recompute it independently —
/// the shipper before it leaves the primary, the follower's own `Wal::append`
/// after it lands — the `Hello` handshake's integrity anchor (DESIGN §5).
pub fn frame_crc(seq: u64, payload: &[u8]) -> u32 {
    let mut crc = Crc32::new();
    crc.update(&seq.to_le_bytes())
        .update(&(payload.len() as u32).to_le_bytes())
        .update(payload);
    crc.finish()
}

pub(crate) fn segment_name(dir: &str, base_seq: u64) -> String {
    format!("{dir}/{base_seq:020}.log")
}

pub(crate) fn parse_segment_name(dir: &str, name: &str) -> io::Result<u64> {
    let bad = || {
        corrupt(format!(
            "unexpected file \"{name}\" in the log directory — segments are named \
             \"{dir}/<seq:020>.log\"; refusing to recover around foreign files"
        ))
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

fn corrupt(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
