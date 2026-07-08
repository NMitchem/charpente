//! Replication (M1 sub-project 2, spec docs/superpowers/specs/2026-07-08-
//! replication-design.md): the wire protocol codec and the durable role
//! file land in this task. The primary/follower session engines and the
//! kernel's replication arm arrive in later tasks under this same module.

pub mod protocol;
pub mod role;

/// The wire protocol version this binary speaks; carried in every `Hello`
/// and checked against the peer's (spec §5's version-mismatch surface).
pub const PROTO_VERSION: u8 = 1;

/// The hard ceiling on one encoded `Msg` on the wire: one `MAX_RECORD_BYTES`
/// frame (16 MiB) plus envelope overhead (spec Global Constraints).
pub const MAX_MESSAGE_BYTES: usize = 17 << 20;

/// Target byte budget for one `Frames` batch the primary-side shipper
/// assembles before sending (spec Global Constraints) — a packing target,
/// not a hard cap; a single frame over budget still ships alone.
pub const FRAMES_BATCH_BYTES: usize = 256 << 10;

/// Target byte budget for one `SnapshotRows` batch while seeding a follower
/// (spec Global Constraints) — a packing target, not a hard cap.
pub const SNAPSHOT_BATCH_BYTES: usize = 256 << 10;

/// Idle-session heartbeat interval (spec Global Constraints): 1s.
pub const HEARTBEAT_NANOS: u64 = 1_000_000_000;

/// Follower reconnect backoff after a dropped session (spec Global
/// Constraints): 500ms.
pub const RECONNECT_NANOS: u64 = 500_000_000;

/// The kernel's replication park-tick cap when replication is configured
/// (spec §3's "accepted wart"; Global Constraints): 5ms.
pub const REPL_TICK_NANOS: u64 = 5_000_000;

/// `switchover`'s drain deadline before aborting back to `Primary` (spec
/// §2; Global Constraints): 30s.
pub const SWITCHOVER_DEADLINE_NANOS: u64 = 30_000_000_000;
