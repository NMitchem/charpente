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
    pub fn recover(disk: Box<dyn Disk>, cfg: WalConfig, mut machine: S) -> io::Result<Engine<S>> {
        let mut last_applied = None;
        let wal = Wal::open(disk, cfg, |seq, payload| {
            assert_eq!(
                seq,
                last_applied.map_or(0, |l: u64| l + 1),
                "replay skipped a seq — the log handed records out of order"
            );
            machine.apply(seq, payload)?;
            last_applied = Some(seq);
            Ok(())
        })?;
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

    pub fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}
