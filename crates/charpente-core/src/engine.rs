//! The single-threaded apply loop (DESIGN.md §1–§2): records go into the log,
//! the log is synced, and only then does the state machine apply them. The
//! standing invariant — machine state is exactly `fold(apply, durable
//! records)` — is what makes replication and recovery replay, not code paths.

use std::io;

use crate::io::Disk;
use crate::log::{Wal, WalConfig};

/// A deterministic function of the record stream. Implementations must not
/// touch anything outside their own state and the records handed to them.
pub trait StateMachine {
    fn apply(&mut self, seq: u64, payload: &[u8]);
}

pub struct Engine<S: StateMachine> {
    wal: Wal,
    machine: S,
    last_applied: Option<u64>,
}

impl<S: StateMachine> Engine<S> {
    /// Recovers the log and replays every durable record into `machine`.
    pub fn recover(disk: Box<dyn Disk>, cfg: WalConfig, mut machine: S) -> io::Result<Engine<S>> {
        let mut last_applied = None;
        let wal = Wal::open(disk, cfg, |seq, payload| {
            assert_eq!(
                seq,
                last_applied.map_or(0, |l: u64| l + 1),
                "replay skipped a seq — the log handed records out of order"
            );
            machine.apply(seq, payload);
            last_applied = Some(seq);
        })?;
        Ok(Engine {
            wal,
            machine,
            last_applied,
        })
    }

    /// Append → sync → apply → acknowledge. On `Ok`, the record is durable
    /// and applied; on `Err`, it is neither applied nor acknowledged (it may
    /// still surface after a crash, like any unacknowledged write).
    pub fn submit(&mut self, payload: &[u8]) -> io::Result<u64> {
        let seq = self.wal.append(payload)?;
        self.wal.sync()?;
        assert_eq!(
            seq,
            self.last_applied.map_or(0, |l| l + 1),
            "apply loop skipped a seq — append and apply have diverged"
        );
        self.machine.apply(seq, payload);
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
