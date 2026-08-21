//! Object-storage archive (M2, spec docs/superpowers/specs/2026-08-20-
//! object-storage-archive-design.md): ships sealed WAL segments and
//! periodic logical snapshots to object storage behind a generation-pointer
//! manifest (§2), and restores from it (§6). Task 4 landed the manifest and
//! the key layout with nothing uploading yet; this task (T5) lands the
//! engine that actually ships sealed segments (§3's first half) — periodic
//! snapshots (§3's other half, T8) and restore (§6, T6) are still to come.
//!
//! `Archive::step` is a state machine, one transition per call, the same
//! shape `Repl` already has (DESIGN §5): `Idle` picks the lowest sealed,
//! un-uploaded WAL segment; `Reading` pulls it through `Db::read_log` (so
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
//! (ruling 23 carry-forward): it holds at the lowest sealed segment this
//! archive has not yet reflected in an uploaded manifest, with no bound on
//! how far behind that segment may be. Because of that, `Wal::gc` can never
//! remove a segment out from under this archive, so a "gap" (spec §2) is
//! structurally impossible this task — `gapped()` is always `false`. The
//! bounded pin that makes a gap possible, and the writer-identity ownership
//! check (spec §4), both land at Task 7.

use std::io;
use std::sync::{Arc, Mutex};

use charpente_core::db::Db;
use charpente_core::io::{ObjectDone, ObjectOp, ObjectStore};
use charpente_core::log::frame_crc;

pub mod manifest;

use manifest::{Manifest, SegmentRef, manifest_key, segment_key};

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
    /// Cadence for periodic logical snapshots, in committed records —
    /// mirrors `DbConfig::checkpoint_every` (spec §3). **Unused this
    /// task**: Task 8 ships snapshots. Kept here now so `ArchiveConfig`'s
    /// shape doesn't change under later tasks.
    pub snapshot_every: u64,
    /// The archive's retention-pin bound, in segments (spec §5): 4x
    /// `DbConfig::retain_segments`'s default of 2, bounding the WAL held
    /// above the checkpoint floor at 512 MiB (64 MiB segments) during an
    /// object-store outage. **Unused this task** (ruling 23 carry-forward):
    /// Task 5's pin is unbounded by design; the bound lands at Task 7.
    pub pin_segments: usize,
    /// Bounds one `Db::read_log` call's allocation while shipping a
    /// segment — never a `SegmentRef`'s span (ruling 29). Default 8 MiB:
    /// at the 64 MiB production `WalConfig::segment_bytes` that's 8
    /// `step`s per segment, so no single `step` reads the whole 64 MiB —
    /// which matters once `step` becomes a kernel arm (Task 9), where a
    /// long step is a stalled kernel. Smaller means more steps per
    /// segment, never a shorter `SegmentRef`.
    pub read_budget_bytes: usize,
}

/// One transition of `Archive::step`'s state machine (module docs).
enum State {
    Idle,
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
    },
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
    /// This archive's generation counter (spec §2). Always 0 this task:
    /// nothing here ever closes a generation (that needs the bounded pin,
    /// T7) or reads a prior one back (T6/T7).
    generation: u64,
    /// The manifest's segment chain as this archive has built it so far,
    /// oldest first, contiguous (`Manifest::encode` asserts this — hard
    /// rule 4). The next un-shipped segment's expected base_seq is
    /// `segments.last().end_seq + 1`, or 0 before anything ships.
    segments: Vec<SegmentRef>,
    state: State,
}

impl Archive {
    /// Registers the retention pin on `db` (mirrors `Repl::open`,
    /// repl/mod.rs:254-257) and returns ready to `step`. Never reads the
    /// store — `step` is the only method that ever touches it (ruling 32),
    /// so `open` cannot fail on the store's account and never does.
    pub fn open(cfg: ArchiveConfig, db: &mut Db) -> io::Result<Archive> {
        let pin = Arc::new(Mutex::new(u64::MAX));
        db.add_retention_pin(Box::new({
            let floor = Arc::clone(&pin);
            move || *floor.lock().expect("uncontended")
        }));
        Ok(Archive {
            cfg,
            pin,
            generation: 0,
            segments: Vec::new(),
            state: State::Idle,
        })
    }

    /// The retention floor this archive holds: the lowest sealed segment
    /// not yet reflected in an uploaded manifest, or `u64::MAX` when caught
    /// up. A segment counts as "reflected" only once the manifest PUT
    /// naming it has itself succeeded — not merely once its own bytes
    /// landed — so retention never releases a segment the manifest doesn't
    /// yet acknowledge (spec §2: the manifest swap is the commit point).
    pub fn pin_floor(&self) -> u64 {
        *self.pin.lock().expect("uncontended")
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
    fn next_base(&self) -> u64 {
        self.segments.last().map_or(0, |s| {
            s.end_seq.checked_add(1).unwrap_or_else(|| {
                panic!(
                    "Archive: the last shipped segment's end_seq is u64::MAX with more to ship \
                     after it — Manifest::encode would refuse this chain (hard rule 4); \
                     unreachable while this archive only ever appends contiguous SegmentRefs"
                )
            })
        })
    }

    /// One unit of work: start one upload, poll one in flight, or read one
    /// budget's worth of a sealed segment — never more than one IO
    /// operation per call (spec §4). Never blocks, never touches `Db`'s
    /// write path (`Db::read_log` and `Db::sealed_segments` are both
    /// read-only). An archive IO failure is returned here and nowhere
    /// else — it never reaches `Db`, so it can never poison it (spec §7).
    pub fn step(&mut self, db: &Db, store: &dyn ObjectStore) -> io::Result<()> {
        match std::mem::replace(&mut self.state, State::Idle) {
            State::Idle => self.step_idle(db),
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
            State::UploadingManifest { bytes, started } => {
                self.step_uploading_manifest(store, bytes, started)
            }
        }
    }

    /// `self.state` is already the placeholder `Idle` on entry (`step`'s
    /// `mem::replace`) — safe to use `?` throughout: there is nothing
    /// resumable to lose on an early return, unlike every other arm below.
    fn step_idle(&mut self, db: &Db) -> io::Result<()> {
        let next = self.next_base();
        let Some(seg) = db
            .sealed_segments()?
            .into_iter()
            .find(|s| s.base_seq == next)
        else {
            *self.pin.lock().expect("uncontended") = u64::MAX;
            return Ok(());
        };
        // Ruling 29's entry gate: the second, independent truncation
        // source (read_log caps up_to at last_applied) handled at its
        // source rather than discovered mid-read. Without this, picking
        // the segment anyway is still safe (step_reading below can never
        // produce a short SegmentRef either way — see its doc), but this
        // avoids the futile reads.
        if seg.layout_end > db.last_applied().unwrap_or(0) {
            *self.pin.lock().expect("uncontended") = u64::MAX;
            return Ok(());
        }
        *self.pin.lock().expect("uncontended") = seg.base_seq;
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
        started: bool,
    ) -> io::Result<()> {
        if !started {
            return match store.start(ObjectOp::Put {
                key: key.clone(),
                data: bytes.clone(),
            }) {
                Ok(()) => {
                    self.state = State::UploadingSegment {
                        base_seq,
                        end_seq,
                        key,
                        bytes,
                        started: true,
                    };
                    Ok(())
                }
                Err(e) => {
                    self.state = State::UploadingSegment {
                        base_seq,
                        end_seq,
                        key,
                        bytes,
                        started: false,
                    };
                    Err(e)
                }
            };
        }
        match store.poll() {
            None => {
                self.state = State::UploadingSegment {
                    base_seq,
                    end_seq,
                    key,
                    bytes,
                    started: true,
                };
                Ok(())
            }
            Some(Ok(ObjectDone::Put)) => {
                // The commit point is the MANIFEST swap (spec §2), never
                // this PUT alone — SegmentRef only enters `self.segments`
                // (and the pin only advances past `base_seq`) once the
                // manifest naming it has itself landed, in
                // step_uploading_manifest below.
                self.segments.push(SegmentRef { base_seq, end_seq });
                let manifest_bytes = Manifest {
                    generation: self.generation,
                    writer: self.cfg.writer.clone(),
                    // Task 5 ships no snapshot yet (T8): this generation
                    // carries none, so its frontier fields are placeholder
                    // zeros/empty rather than a claim about data that was
                    // never shipped.
                    applied_seq: 0,
                    next_job_id: 0,
                    tables: Vec::new(),
                    segments: self.segments.clone(),
                    closed: false,
                }
                .encode();
                self.state = State::UploadingManifest {
                    bytes: manifest_bytes,
                    started: false,
                };
                Ok(())
            }
            Some(Ok(other)) => unreachable!(
                "a Put was started; only ObjectDone::Put or an error can complete it, got {other:?}"
            ),
            Some(Err(e)) => {
                // STAY and retry the SAME key with the SAME bytes (ruling
                // 29/spec §9.2) — never a fresh key, so a lost ack never
                // duplicates an object.
                self.state = State::UploadingSegment {
                    base_seq,
                    end_seq,
                    key,
                    bytes,
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
        started: bool,
    ) -> io::Result<()> {
        let key = manifest_key(&self.cfg.prefix);
        if !started {
            return match store.start(ObjectOp::Put {
                key,
                data: bytes.clone(),
            }) {
                Ok(()) => {
                    self.state = State::UploadingManifest {
                        bytes,
                        started: true,
                    };
                    Ok(())
                }
                Err(e) => {
                    self.state = State::UploadingManifest {
                        bytes,
                        started: false,
                    };
                    Err(e)
                }
            };
        }
        match store.poll() {
            None => {
                self.state = State::UploadingManifest {
                    bytes,
                    started: true,
                };
                Ok(())
            }
            Some(Ok(ObjectDone::Put)) => {
                // The manifest swap landed: this segment is now durably
                // reflected off-box, so the pin may release it. The NEXT
                // `Idle` step recomputes the true floor (another sealed
                // segment may already be waiting, or none is).
                self.state = State::Idle;
                Ok(())
            }
            Some(Ok(other)) => unreachable!(
                "a Put was started; only ObjectDone::Put or an error can complete it, got {other:?}"
            ),
            Some(Err(e)) => {
                self.state = State::UploadingManifest {
                    bytes,
                    started: false,
                };
                Err(e)
            }
        }
    }
}
