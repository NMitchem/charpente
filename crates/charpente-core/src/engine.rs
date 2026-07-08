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
}

pub struct Engine<S: StateMachine> {
    wal: Wal,
    machine: S,
    last_applied: Option<u64>,
    poisoned: bool,
}

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

    /// Append → sync → apply → acknowledge. On `Ok`, the record is durable
    /// and applied; on `Err`, it is not acknowledged (it may still surface
    /// after a crash, like any unacknowledged write). If apply fails after a
    /// successful sync, machine state has diverged from durable truth and
    /// the engine poisons itself — the only exit is restart-and-recover.
    pub fn submit(&mut self, payload: &[u8]) -> io::Result<u64> {
        if self.poisoned {
            return Err(io::Error::other(
                "engine poisoned: a durable record failed to apply, so machine state no \
                 longer matches the log — restart and recover",
            ));
        }
        let seq = self.wal.append(payload)?;
        self.wal.sync()?;
        assert_eq!(
            seq,
            self.last_applied.map_or(0, |l| l + 1),
            "apply loop skipped a seq — append and apply have diverged"
        );
        if let Err(e) = self.machine.apply(seq, payload) {
            self.poisoned = true;
            return Err(e);
        }
        self.last_applied = Some(seq);
        Ok(seq)
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
                         re-Hello",
                        self.wal.next_seq()
                    ),
                ));
            }
            self.wal.append(payload)?;
        }
        self.wal.sync()?;
        for (seq, payload) in frames {
            assert_eq!(
                *seq,
                self.last_applied.map_or(0, |l| l + 1),
                "apply loop skipped a seq — append and apply have diverged"
            );
            if let Err(e) = self.machine.apply(*seq, payload) {
                self.poisoned = true;
                return Err(e);
            }
            self.last_applied = Some(*seq);
        }
        Ok(())
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
