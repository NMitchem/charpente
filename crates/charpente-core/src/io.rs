//! The IO capability traits — the only doorway between charpente code and the
//! world (DESIGN.md §1). Every source of nondeterminism (time, randomness,
//! disk, network) is reached through these traits; production implementations
//! live at the platform edge, simulation implementations in `charpente-sim`.
//!
//! The traits are synchronous and poll-shaped, not async (DECISIONS.md §10):
//! the core state machine is single-threaded and its IO is sequential, and
//! crash-interleaving exploration comes from the simulator's write-buffer
//! collapse at crash time, not from async completion reordering.

use std::io;
use std::time::Duration;

/// Monotonic time as nanoseconds since an arbitrary origin — process start in
/// production, universe start in simulation. This is charpente's own type
/// because `std::time::Instant` is opaque and cannot be constructed from
/// virtual time. Not wall-clock time; wall-clock never enters the core.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Instant {
    nanos: u64,
}

impl Instant {
    pub const ZERO: Instant = Instant { nanos: 0 };

    pub fn from_nanos(nanos: u64) -> Instant {
        Instant { nanos }
    }

    pub fn as_nanos(self) -> u64 {
        self.nanos
    }

    pub fn duration_since(self, earlier: Instant) -> Duration {
        assert!(
            earlier.nanos <= self.nanos,
            "Instant::duration_since: {earlier:?} is later than {self:?} — monotonic time ran backwards"
        );
        Duration::from_nanos(self.nanos - earlier.nanos)
    }
}

impl std::ops::Add<Duration> for Instant {
    type Output = Instant;

    fn add(self, d: Duration) -> Instant {
        let d =
            u64::try_from(d.as_nanos()).expect("Instant + Duration: duration exceeds u64 nanos");
        let nanos = self
            .nanos
            .checked_add(d)
            .expect("Instant + Duration overflowed u64 nanos (~584 years of runtime)");
        Instant { nanos }
    }
}

/// Monotonic time. In production, anchored at process start; in simulation,
/// virtual — a sleep advances the universe instead of the wall clock.
pub trait Clock {
    fn now(&self) -> Instant;

    /// Blocks the calling thread in production; advances virtual time in sim.
    fn sleep(&self, duration: Duration);
}

/// Randomness. Seeded and replayable in simulation; OS entropy in production.
///
/// `Send` because `App` (which owns a `Box<dyn Entropy>`) crosses into the
/// kernel thread in `charpente::serve`; simulation impls satisfy it honestly
/// (same reasoning as `File`, below).
pub trait Entropy: Send {
    fn fill(&self, buf: &mut [u8]);

    fn next_u64(&self) -> u64 {
        let mut bytes = [0u8; 8];
        self.fill(&mut bytes);
        u64::from_le_bytes(bytes)
    }
}

/// Durable storage, shaped to carry both the write-ahead log and redb's
/// `StorageBackend` (sync, `&self`, offset-addressed — DESIGN.md §4). Paths
/// are a flat namespace of names, not a directory tree.
///
/// `Send` because `App` (which owns a `Db`, which owns a `Box<dyn Disk>`)
/// crosses into the kernel thread in `charpente::serve`; simulation impls
/// satisfy it honestly (same reasoning as `File`, below).
pub trait Disk: Send {
    /// Opens the named file, creating it empty if absent.
    fn open(&self, path: &str) -> io::Result<Box<dyn File>>;

    fn remove(&self, path: &str) -> io::Result<()>;

    /// All file names starting with `prefix`, sorted — callers must never
    /// depend on filesystem enumeration order.
    fn list(&self, prefix: &str) -> io::Result<Vec<String>>;

    /// Directory barrier: on `Ok`, every prior create/remove of a name
    /// starting with `prefix` survives any crash. `File::sync` does not
    /// persist a file's directory entry; this does (DESIGN §2).
    fn sync_dir(&self, prefix: &str) -> io::Result<()>;
}

/// An open file. Writes are volatile until `sync` returns `Ok`: after a crash,
/// unsynced writes may be applied in full, in part (torn), or not at all, in
/// any combination — that is the contract recovery code is tested against.
///
/// `Send + Sync` because storage engines mounted behind the boundary (redb's
/// `StorageBackend`) require it; simulation impls satisfy it honestly.
#[allow(clippy::len_without_is_empty)] // a file size, not a container
pub trait File: Send + Sync {
    fn len(&self) -> io::Result<u64>;

    /// Reads exactly `out.len()` bytes at `offset`; error if that range
    /// extends past the end of the file.
    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()>;

    /// Writes at `offset`, extending the file if needed; any gap between the
    /// old end and `offset` reads as zeros (pwrite semantics).
    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()>;

    /// Truncates or zero-extends to `len`.
    fn set_len(&self, len: u64) -> io::Result<()>;

    /// Durability barrier: on `Ok`, every prior write on this file survives
    /// any crash. On `Err`, the durability of prior writes is unknown.
    fn sync(&self) -> io::Result<()>;
}

/// Network. Message-oriented for now: one `send` is delivered as one `recv`,
/// reliable and ordered per connection except across partitions and crashes.
/// Byte-stream semantics arrive with the HTTP edge if it needs them.
pub trait Net {
    /// `addr` is `"host:port"`; in simulation, `host` is a sim host name.
    fn listen(&self, addr: &str) -> io::Result<Box<dyn Listener>>;

    fn connect(&self, addr: &str) -> io::Result<Box<dyn Conn>>;
}

pub trait Listener {
    /// Non-blocking: `None` means no pending connection.
    fn accept(&self) -> io::Result<Option<Box<dyn Conn>>>;

    fn local_addr(&self) -> String;
}

pub trait Conn {
    fn send(&self, data: &[u8]) -> io::Result<()>;

    /// Non-blocking: `None` means nothing available yet; `Some(0)` means the
    /// peer closed. `out` must be large enough for the incoming message.
    fn recv(&self, out: &mut [u8]) -> io::Result<Option<usize>>;

    fn peer_addr(&self) -> String;
}

/// One object-storage operation. Whole-object, never offset-addressed —
/// object stores have no partial writes (DESIGN §1, archive spec §1).
#[derive(Debug)]
pub enum ObjectOp {
    Put { key: String, data: Vec<u8> },
    Get { key: String },
    List { prefix: String },
    Delete { key: String },
}

/// The completion of an `ObjectOp`.
#[derive(Debug)]
pub enum ObjectDone {
    Put,
    Get(Vec<u8>),
    /// All keys under the prefix, sorted — callers must never depend on
    /// enumeration order, same rule as `Disk::list`.
    List(Vec<String>),
    Delete,
}

/// Off-box object storage. One operation in flight at a time.
///
/// **A PUT is atomic.** A failed PUT leaves the key absent or holding its
/// previous value, never half of the new one — the precise opposite of
/// `File`'s torn-write contract, and the reason this is not `Disk` reuse.
/// There is no `sync_dir` analogue: there is no directory entry to persist.
/// Ordering between distinct keys is NOT guaranteed.
///
/// `Send` for the same reason as `Disk`: `App` crosses into the kernel thread.
pub trait ObjectStore: Send {
    /// Begins `op`. Asserts nothing is already in flight — a second `start`
    /// before `poll` returns `Some` is a caller bug, not a runtime condition.
    fn start(&self, op: ObjectOp) -> io::Result<()>;

    /// `None` means still in flight.
    fn poll(&self) -> Option<io::Result<ObjectDone>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instant_arithmetic() {
        let t0 = Instant::from_nanos(100);
        let t1 = t0 + Duration::from_nanos(50);
        assert_eq!(t1.as_nanos(), 150);
        assert_eq!(t1.duration_since(t0), Duration::from_nanos(50));
        assert_eq!(t0.duration_since(t0), Duration::ZERO);
        assert!(t0 < t1);
    }

    #[test]
    #[should_panic(expected = "monotonic time ran backwards")]
    fn instant_duration_since_asserts_order() {
        let _ = Instant::from_nanos(1).duration_since(Instant::from_nanos(2));
    }
}
