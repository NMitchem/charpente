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
    pub fn recover(disk: Box<dyn Disk>, cfg: WalConfig, mut machine: S) -> io::Result<Engine<S>> {
        let mut last_applied: Option<u64> = None;
        let wal = Wal::open(disk, cfg, |seq, payload| {
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

    pub fn machine(&self) -> &S {
        &self.machine
    }

    pub fn machine_mut(&mut self) -> &mut S {
        &mut self.machine
    }

    pub fn wal_mut(&mut self) -> &mut Wal {
        &mut self.wal
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
