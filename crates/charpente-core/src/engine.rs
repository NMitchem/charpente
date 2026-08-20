//! The single-threaded apply loop (DESIGN.md §1–§2): records go into the log,
//! the log is synced, and only then does the state machine apply them. The
//! standing invariant — machine state is exactly `fold(apply, durable
//! records)` — is what makes replication and recovery replay, not code paths.

use std::fmt;
use std::io;

use crate::io::Disk;
use crate::log::{Wal, WalConfig};

/// A deterministic function of the record stream. Implementations must not
/// touch anything outside their own state and the records handed to them.
/// An `Err` means the machine could not ingest a record the log already made
/// durable; the engine treats its state as diverged (see `submit`).
pub trait StateMachine {
    fn apply(&mut self, seq: u64, payload: &[u8]) -> io::Result<()>;

    /// Applies a contiguous run of records. On failure, `SubmitFailure::applied`
    /// reports how many of them are durably reflected in machine state; an
    /// implementation may apply all or none, so a caller must not assume either
    /// — `TableMachine` is atomic and reports 0, this default is per-record and
    /// reports the boundary.
    fn apply_batch(&mut self, records: &[(u64, &[u8])]) -> Result<(), SubmitFailure> {
        for (i, (seq, payload)) in records.iter().enumerate() {
            if let Err(error) = self.apply(*seq, payload) {
                return Err(SubmitFailure { applied: i, error });
            }
        }
        Ok(())
    }
}

pub struct Engine<S: StateMachine> {
    wal: Wal,
    machine: S,
    last_applied: Option<u64>,
    poisoned: bool,
}

/// The substring `submit_batch`'s out-of-order-seq error names, and
/// `follower.rs`'s `apply_and_ack` substring-matches to distinguish "drop and
/// re-Hello" from a genuine `Db` fault/poison. Shared so a reword of one side
/// cannot silently break the other (error-suite T8).
pub const REHELLO_MARKER: &str = "re-Hello";

// `Wal` holds a `Box<dyn Disk>`/`Box<dyn File>` and `S` is caller-defined, so
// neither is `Debug`-bound; this impl exposes only the state that is (used
// by `Result::unwrap_err` on a `recover` call, e.g. in tests).
impl<S: StateMachine> fmt::Debug for Engine<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Engine")
            .field("last_applied", &self.last_applied)
            .field("poisoned", &self.poisoned)
            .finish()
    }
}

/// `submit_all`'s failure shape (group-commit T4 fix round, Important 1):
/// `applied` is how many of the passed-in records already reached the
/// store — durably written AND applied — before this returned `Err`. See
/// `submit_all`'s own doc for what `0` versus a nonzero `applied` means.
/// `Db::commit` (db.rs) is why this carries a count rather than being a
/// bare `io::Error`: spec §4's mid-batch-apply-failure contract needs to
/// tell a caller records `0..applied` already got their 200s.
#[derive(Debug)]
pub struct SubmitFailure {
    pub applied: usize,
    pub error: io::Error,
}

impl fmt::Display for SubmitFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

/// Lets `submit`'s `?` on `submit_all` keep returning a bare `io::Error`
/// unchanged: `submit`'s single-record case has no meaningful "partial
/// batch" to report (`applied` collapses to a trivial 0-or-nothing), so the
/// count is simply dropped on this path.
impl From<SubmitFailure> for io::Error {
    fn from(failure: SubmitFailure) -> io::Error {
        failure.error
    }
}

impl<S: StateMachine> Engine<S> {
    /// Recovers the log and replays every durable record into `machine`.
    /// Replay can fail (the machine's own IO, or the log's); recovery is
    /// idempotent — restart and retry.
    ///
    /// Replay's expected-first-seq re-anchors to wherever the log actually
    /// starts (`0` for a never-compacted log, `Wal::base_seq()` once
    /// `Db::checkpoint` has GC'd earlier segments — segment-mgmt T3): the
    /// closure trusts its own first invocation as that anchor, and asserts
    /// gapless continuation for every call after it, same as before. If GC
    /// left nothing to replay at all (the retained tail has zero frames),
    /// the closure never runs and `last_applied` is seeded straight from
    /// `wal.base_seq()` once the log is open — recovery must resume there,
    /// not at 0, or the very next `submit` diverges from the log's own
    /// bookkeeping and panics.
    ///
    /// `horizon` is the recovery-horizon rule (DESIGN §2, segment-mgmt T4):
    /// the store's `applied_seq` at open, threaded into `Wal` so a corrupt
    /// frame at-or-below it is a named hard error, never a cut. Callers with
    /// no store in the picture (standalone WAL tests) pass `None`.
    pub fn recover(
        disk: Box<dyn Disk>,
        cfg: WalConfig,
        mut machine: S,
        horizon: Option<u64>,
    ) -> io::Result<Engine<S>> {
        let mut last_applied: Option<u64> = None;
        let wal = Wal::open_with_horizon(disk, cfg, horizon, |seq, payload| {
            if let Some(prev) = last_applied {
                assert_eq!(
                    seq,
                    prev + 1,
                    "replay skipped a seq — the log handed records out of order"
                );
            }
            machine.apply(seq, payload)?;
            last_applied = Some(seq);
            Ok(())
        })?;
        if let Some(h) = horizon {
            // Negative space: a vanished prefix (the log's floor past the
            // horizon by more than one) is a distinct hard error `Db::open`
            // must catch BEFORE calling `recover` (segment-mgmt T4) — this
            // is the state Wal::open_with_horizon's own frame-level check
            // cannot see (no invalid frame is involved; whole segments are
            // just gone). If this ever fires, the caller skipped that check.
            assert!(
                wal.base_seq() <= h + 1,
                "Engine::recover: log base_seq {} is past the durable horizon {h} + 1 — the \
                 caller must reject a vanished prefix before calling recover, not rely on this \
                 assert",
                wal.base_seq()
            );
        }
        if last_applied.is_none() && wal.base_seq() > 0 {
            last_applied = Some(wal.base_seq() - 1);
        }
        Ok(Engine {
            wal,
            machine,
            last_applied,
            poisoned: false,
        })
    }

    /// The length-1 case of `submit_all` — see its docs for the
    /// append→sync→apply discipline this executes, and for what an `Err`
    /// says about durable state. `submit` has no production caller as of
    /// group-commit T4 (`Db::commit` calls `submit_all` directly); it is
    /// kept as a test-facing convenience for the length-1 case, not the
    /// live single-request path.
    pub fn submit(&mut self, payload: &[u8]) -> io::Result<u64> {
        // The payload goes straight into a length-1 slice-of-slices, so this
        // convenience pays no allocation for being routed through the batch
        // core. Do NOT "simplify" this to a signature that copies (`&[Vec<u8>]`
        // + `to_vec()`): that copy is O(payload), up to MAX_RECORD_BYTES.
        // `SubmitFailure` converts to `io::Error` via `From` below, so `?`
        // here needs no explicit mapping.
        Ok(self.submit_all(&[payload])?[0])
    }

    /// Appends every record, syncs once for the batch, then applies each in
    /// seq order — the append→sync→apply discipline of `submit`, N-wide.
    /// Apply stays strictly after sync, which is what keeps ack-after-durable
    /// true for every record in the batch.
    ///
    /// **The batch is NOT durability-atomic, and nothing here claims it is.**
    /// A batch large enough to straddle a segment boundary takes an extra
    /// barrier inside `Wal::append`, which calls `rotate`, which opens with
    /// `tail.sync()` — the same "rotation syncs the old segment" rule the log
    /// format states. A crash between that barrier and the batch's own sync
    /// therefore leaves a durable PREFIX, not nothing. No guarantee moves:
    /// the replies are still held, so nothing in the batch was acked, and an
    /// unacked record that survives a crash is the ordinary "the write may be
    /// committed" case the client contract already covers. Ack-after-durable
    /// is the promise; batch atomicity never was (DESIGN §2, docs/BENCH.md).
    ///
    /// On `Ok`, every record is durable and applied. On `Err`, three cases
    /// with different durable standing, which a caller must NOT conflate:
    ///
    /// - **append or sync failed**: nothing in the batch is acknowledged and
    ///   the log is poisoned. Records already written may still surface
    ///   after a crash, like any unacknowledged write.
    /// - **apply failed at record `i`**: every record in the batch is
    ///   ALREADY DURABLE. Records before `i` are applied and acknowledged;
    ///   `i` and the tail are unacknowledged but WILL apply on restart — so
    ///   their failure is a false negative, not a rejection. The engine
    ///   poisons itself and restart-and-recover is the only exit.
    ///
    /// `last_applied()` is the caller's signal in every case, success
    /// included: its delta across the call is exactly the number of records
    /// acknowledged. `poisoned()` does not discriminate the cases above — it
    /// ORs apply-poison with log-poison.
    ///
    /// On `Err`, `SubmitFailure::applied` names exactly which of the three
    /// cases above this is: `0` for an append/sync failure (or a call
    /// against an already-poisoned engine — nothing in the batch is
    /// durable), or `i` for an apply failure at 0-indexed record `i`
    /// (records `0..i` are already durable, applied, and acknowledged).
    /// This is a COUNTED value from this loop, not something a caller
    /// derives from `last_applied()` before/after — group-commit T4's own
    /// review flagged a derived gate input as unsound.
    pub fn submit_all(&mut self, records: &[&[u8]]) -> Result<Vec<u64>, SubmitFailure> {
        if self.poisoned {
            return Err(SubmitFailure {
                applied: 0,
                error: io::Error::other(
                    "engine poisoned: a durable record failed to apply, so machine state no \
                     longer matches the log — restart and recover",
                ),
            });
        }
        // An arm with nothing ready must cost nothing: no append, no sync.
        // This check stays BELOW the poison check deliberately — poison is a
        // property of the engine, not of the argument, so an engine that
        // cannot serve refuses every call, this one included
        // (`an_empty_batch_on_a_poisoned_engine_still_refuses` pins the
        // order). `submit_batch` diverges here and does sync on an empty
        // frame list; its empty case is guarded upstream at the peer
        // boundary, so do not reconcile the two by making this arm sync.
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let mut seqs = Vec::with_capacity(records.len());
        for payload in records {
            match self.wal.append(payload) {
                Ok(seq) => seqs.push(seq),
                Err(error) => return Err(SubmitFailure { applied: 0, error }),
            }
        }
        if let Err(error) = self.wal.sync() {
            return Err(SubmitFailure { applied: 0, error });
        }
        let batch: Vec<(u64, &[u8])> = seqs.iter().copied().zip(records.iter().copied()).collect();
        assert_eq!(
            batch[0].0,
            self.last_applied.map_or(0, |l| l + 1),
            "apply loop skipped a seq — append and apply have diverged"
        );
        match self.machine.apply_batch(&batch) {
            Ok(()) => {
                self.last_applied = Some(batch[batch.len() - 1].0);
                Ok(seqs)
            }
            Err(SubmitFailure { applied, error }) => {
                self.poisoned = true;
                if applied > 0 {
                    self.last_applied = Some(batch[applied - 1].0);
                }
                Err(SubmitFailure { applied, error })
            }
        }
    }

    /// The follower's apply path: append every frame, one sync, then apply
    /// each — the same append→sync→apply discipline as `submit`, minus the
    /// client ack, amortizing the sync over a shipped batch. Shipped seqs
    /// cross a trust boundary, so a gap is a named error, not an assert.
    pub fn submit_batch(&mut self, frames: &[(u64, Vec<u8>)]) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::other(
                "engine poisoned: a durable record failed to apply, so machine state no \
                 longer matches the log — restart and recover",
            ));
        }
        for (seq, payload) in frames {
            if *seq != self.wal.next_seq() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "shipped frame carries seq {seq} but this log expects seq {} — the ship \
                         stream is out of order or from a different timeline; reconnect and \
                         {REHELLO_MARKER}",
                        self.wal.next_seq()
                    ),
                ));
            }
            self.wal.append(payload)?;
        }
        self.wal.sync()?;
        let batch: Vec<(u64, &[u8])> = frames
            .iter()
            .map(|(seq, payload)| (*seq, payload.as_slice()))
            .collect();
        if let Some((first, _)) = batch.first() {
            assert_eq!(
                *first,
                self.last_applied.map_or(0, |l| l + 1),
                "apply loop skipped a seq — append and apply have diverged"
            );
        }
        match self.machine.apply_batch(&batch) {
            Ok(()) => {
                if let Some((last, _)) = batch.last() {
                    self.last_applied = Some(*last);
                }
                Ok(())
            }
            Err(SubmitFailure { applied, error }) => {
                self.poisoned = true;
                if applied > 0 {
                    self.last_applied = Some(batch[applied - 1].0);
                }
                Err(error)
            }
        }
    }

    pub fn machine(&self) -> &S {
        &self.machine
    }

    pub fn machine_mut(&mut self) -> &mut S {
        &mut self.machine
    }

    pub fn wal(&self) -> &Wal {
        &self.wal
    }

    pub fn wal_mut(&mut self) -> &mut Wal {
        &mut self.wal
    }

    /// Reclaims the engine's WAL and state machine, discarding the poison
    /// bit — the seed path (`Db::into_seeder`, db.rs): the whole timeline is
    /// about to be replaced, so nothing about the engine's own state needs
    /// preserving beyond the raw parts.
    pub fn into_parts(self) -> (Wal, S) {
        (self.wal, self.machine)
    }

    pub fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }

    /// True if a post-sync apply failure poisoned the engine, or any IO
    /// failure poisoned its WAL. A poisoned engine serves no further
    /// transactions; restart-and-recover is the only exit.
    pub fn poisoned(&self) -> bool {
        self.poisoned || self.wal.poisoned()
    }
}
