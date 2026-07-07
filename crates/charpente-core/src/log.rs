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
//! Recovery scans frames in order and stops at the first invalid one: the
//! rest of that segment is truncated and later segments are deleted — they
//! hold only unacknowledged or stale data. The truncation is synced before
//! the log accepts appends; this is load-bearing. Without it, a discarded
//! frame of the same length as its replacement can survive beyond the new
//! frame, checksum-valid and seq-correct, and a later recovery would
//! resurrect a record this recovery chose to discard. The deletions are
//! themselves barriered with `sync_dir` — a later-segment name resurrected by
//! an unsynced removal would reintroduce the exact stale data the cut just
//! discarded.
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

impl Wal {
    /// Opens the log, replaying every recovered record into `on_record` in
    /// seq order, and leaves the log ready to append after the last one. An
    /// `on_record` error aborts recovery.
    pub fn open(
        disk: Box<dyn Disk>,
        cfg: WalConfig,
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
            let len = file.len()?;
            let mut offset = 0u64;
            let cut = loop {
                match read_frame(&*file, len, offset, expected_seq)? {
                    Frame::Valid { payload } => {
                        on_record(expected_seq, &payload)?;
                        expected_seq += 1;
                        offset += HEADER_BYTES + payload.len() as u64;
                    }
                    Frame::Invalid => break Some(offset),
                    Frame::CleanEnd => break None,
                }
            };
            match cut {
                Some(cut) => {
                    if cut < len {
                        // Load-bearing truncation; see the module docs.
                        file.set_len(cut)?;
                        file.sync()?;
                    }
                    for later in &names[index + 1..] {
                        disk.remove(later)?;
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
        if self.tail_offset > 0 && self.tail_offset + frame_len > self.cfg.segment_bytes {
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

fn read_frame(file: &dyn File, len: u64, offset: u64, expected_seq: u64) -> io::Result<Frame> {
    if offset == len {
        return Ok(Frame::CleanEnd);
    }
    if offset + HEADER_BYTES > len {
        return Ok(Frame::Invalid); // partial header: torn tail
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

fn frame_crc(seq: u64, payload: &[u8]) -> u32 {
    let mut crc = Crc32::new();
    crc.update(&seq.to_le_bytes())
        .update(&(payload.len() as u32).to_le_bytes())
        .update(payload);
    crc.finish()
}

fn segment_name(dir: &str, base_seq: u64) -> String {
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
