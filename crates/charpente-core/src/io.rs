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
/// Those are the two outcomes a CALLER may assume; an impl may additionally
/// fail *after* the new value has landed, which is the archive design's
/// "PUT lands but the ack is lost" case and is why every retry in this
/// codebase re-PUTs the same key with the same bytes.
/// Ordering between distinct keys is NOT guaranteed.
///
/// **An object a `Get` or `List` returns is durable, and an acknowledged
/// `Put` is durable.** Durable means it survives a power loss of whatever
/// holds the bucket. Both halves bind every impl: a caller acts on what a
/// `Get` returned (the archive adopts the manifest it reads) exactly as it
/// acts on an ack, so an impl that can see an object only its page cache
/// holds — one a failed `Put` left behind, read by the next process — makes
/// it durable before returning it, or does not return it. Nothing is owed
/// for a `Delete`: a power loss may bring a deleted object back.
///
/// **There is no `sync_dir` analogue, and that is a statement about this
/// trait's surface, not a discharge of the obligation.** Whole-object PUTs
/// give a caller nothing to barrier, so no method is offered. An S3-like
/// store meets the durability sentence above for free. An impl over a real
/// namespace — a filesystem, where the key IS a directory entry — has to
/// earn it, silently, because this trait has no method through which a
/// caller could ask. Read `charpente::prod::FsObjectStore` before writing a
/// second impl: it says what that cost turned out to be.
///
/// `Send` for the same reason as `Disk`: `App` crosses into the kernel thread.
pub trait ObjectStore: Send {
    /// Begins `op`. Asserts nothing is already in flight — a second `start`
    /// before `poll` returns `Some` is a caller bug, not a runtime condition.
    fn start(&self, op: ObjectOp) -> io::Result<()>;

    /// `None` means no completion available — nothing started, or still in
    /// flight; the two are deliberately indistinguishable. Non-blocking like
    /// `Listener::accept`/`Conn::recv`, but nested the other way round: a
    /// failed op IS a completion, so the error rides inside the `Some`.
    fn poll(&self) -> Option<io::Result<ObjectDone>>;
}

/// The HTTP methods an outbound call may use (outbound-effects spec §3). Named
/// `HttpMethod` because `charpente::http::Method` is the inbound one.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl HttpMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Put => "PUT",
            HttpMethod::Patch => "PATCH",
            HttpMethod::Delete => "DELETE",
        }
    }
}

/// An outbound job's idempotency key: 16 bytes drawn from the node's `Entropy`
/// when the enqueue is staged, recorded in the enqueue effect, and stable across
/// every retry, re-dispatch, failover and restore of that job (spec §6). Two
/// diverged timelines draw different ones, which is why a key is never derived
/// from an id. The app places it where its API wants it (spec R5).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct IdempotencyKey {
    raw: [u8; 16],
    hex: [u8; 32],
}

impl IdempotencyKey {
    pub fn from_bytes(raw: [u8; 16]) -> IdempotencyKey {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut hex = [0u8; 32];
        for (i, b) in raw.iter().enumerate() {
            hex[2 * i] = DIGITS[usize::from(b >> 4)];
            hex[2 * i + 1] = DIGITS[usize::from(b & 0x0f)];
        }
        IdempotencyKey { raw, hex }
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.raw
    }

    /// 32 lowercase hex characters.
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.hex).expect("hex digits are ASCII")
    }
}

impl std::fmt::Debug for IdempotencyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IdempotencyKey({})", self.as_str())
    }
}

/// One outbound HTTP request, as the kernel hands it to `Outbound::start`. `host`
/// comes from the job's declaration, never from runtime data (spec §5).
#[derive(Clone, Debug)]
pub struct OutboundCall {
    pub host: &'static str,
    pub method: HttpMethod,
    /// Path and query only, beginning with `/`.
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Attribution only: an impl never transmits it. The app places the key where
    /// its API wants it (spec R5); the sim reads this field to attribute a
    /// delivered call to its job.
    pub key: IdempotencyKey,
}

/// A reply of any status. A transport failure is not a reply: it arrives as the
/// `Err` of a completion. When the remote's body exceeds the host's reply cap,
/// an impl returns at least cap + 1 body bytes: the kernel's over-cap check
/// depends on it.
#[derive(Clone, Debug)]
pub struct OutboundReply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Outbound HTTP to declared hosts (outbound-effects spec §3): the sixth IO
/// trait. Many calls may be in flight, at most one per token. The trait carries
/// no timeout and no retry: both are the kernel's decisions, made on its own
/// `Clock` (spec R10).
///
/// An impl reports a failure that certainly did not reach the remote with
/// `io::ErrorKind::ConnectionRefused`. The kernel words every other error as
/// "the remote may have applied this call".
///
/// `Send` because the kernel that owns it crosses into the kernel thread.
pub trait Outbound: Send {
    /// Begins `call` under `token`. Asserts `token` is not in flight. An `Err`
    /// here is a completion the kernel records, not a kernel failure.
    fn start(&self, token: u64, call: OutboundCall) -> io::Result<()>;

    /// One completion or `None`. A transport failure IS a completion: the error
    /// rides inside the `Some`, as with `ObjectStore::poll`.
    fn poll(&self) -> Option<(u64, io::Result<OutboundReply>)>;

    /// The kernel abandoned `token` (its deadline passed, or the node stopped
    /// being writable). A completion for it may still surface; the kernel
    /// discards it.
    fn cancel(&self, token: u64);

    /// When this handle's next completion is due, asked only while something
    /// is in flight (plan amendment A2). The kernel parks no later than it.
    fn next_wake(&self) -> OutboundWake {
        OutboundWake::Unknown
    }
}

/// `Outbound::next_wake`'s answer.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OutboundWake {
    /// Production's answer, since replies arrive from the network: the kernel
    /// parks no longer than its outbound tick.
    Unknown,
    /// No completion will ever arrive for what is in flight (every reply is
    /// lost): only the kernel's own deadlines wake it.
    Never,
    /// The next completion is due at this instant: `poll` returns it once the
    /// clock reaches the instant. An `At` already passed with nothing to poll
    /// stops the kernel by name, since parking on it would never advance time.
    At(Instant),
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

    #[test]
    fn an_idempotency_key_renders_as_32_lowercase_hex() {
        let key = IdempotencyKey::from_bytes([
            0x00, 0x01, 0x0a, 0x0f, 0x10, 0x7f, 0x80, 0xab, 0xcd, 0xef, 0xff, 0x12, 0x34, 0x56,
            0x78, 0x9a,
        ]);
        assert_eq!(key.as_str(), "00010a0f107f80abcdefff123456789a");
        assert_eq!(key.as_str().len(), 32);
        assert_eq!(key.as_bytes()[2], 0x0a);
        assert_eq!(
            format!("{key:?}"),
            "IdempotencyKey(00010a0f107f80abcdefff123456789a)"
        );
    }

    #[test]
    fn http_methods_render_upper_case() {
        let all = [
            (HttpMethod::Get, "GET"),
            (HttpMethod::Post, "POST"),
            (HttpMethod::Put, "PUT"),
            (HttpMethod::Patch, "PATCH"),
            (HttpMethod::Delete, "DELETE"),
        ];
        for (m, s) in all {
            assert_eq!(m.as_str(), s);
        }
    }
}
