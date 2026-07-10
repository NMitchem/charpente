//! Replication (M1 sub-project 2, spec docs/superpowers/specs/2026-07-08-
//! replication-design.md). The wire protocol codec (`protocol`) and the
//! durable role file (`role`) landed in task 5; the `Repl` umbrella here and
//! the primary/follower session engines (`session`, `follower`) are the data
//! plane (task 6). The kernel's replication arm and the control-plane admin
//! verbs arrive in later tasks under this same module.
//!
//! `Repl` is driven at the same-shape granularity the kernel uses: each
//! iteration `pump_io` moves bytes without touching the `Db` (accept conns,
//! drain readable messages, note reconnect timers), then — while `has_work` —
//! one `step` does one unit against the `Db` (handle a message, push a frames
//! or snapshot batch, heartbeat, reconnect). Seeding a fresh follower cannot
//! replace a borrowed `Db`, so `step` latches `db_needs_reseed` and the caller
//! runs `run_seed`, which consumes and returns the rebuilt `Db`.

use std::io;
use std::sync::{Arc, Mutex};

use charpente_core::io::{Clock, Disk, Listener, Net};

use crate::Db;
use crate::repl::follower::Follower;
use crate::repl::role::{Role, load_role};
use crate::repl::session::Session;

mod follower;
pub mod protocol;
pub mod role;
mod session;

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

// ---------------------------------------------------------------- Repl

/// Startup replication configuration. The *effective* role is whatever the
/// durable role file says (`role::load_role`); `initial_role` only seeds it on
/// first boot and is named in the mismatch notice otherwise (spec §2).
#[derive(Clone, Debug)]
pub struct ReplConfig {
    /// This node's replication address. Required to be (or become) a primary;
    /// a follower without one cannot be promoted remotely (no control conn).
    pub listen: Option<String>,
    pub initial_role: Role,
}

/// Sim-agnostic counters a workload can assert on without reaching into the
/// simulator — e.g. `stats().seeds` proves a session took the snapshot path.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReplStats {
    /// Snapshot re-seeds this node's follower client has completed.
    pub seeds: u64,
}

/// The replication engine for one node: the durable role, the listener (when
/// configured), the live primary-side sessions, the follower-side client (on
/// a follower/backup), and the retention pin wired into the `Db`.
pub struct Repl {
    role: Role,
    net: Box<dyn Net>,
    // Held for task 7's durable role flips (switchover/promote write here);
    // unused in the data plane.
    #[allow(dead_code)]
    role_disk: Box<dyn Disk>,
    listener: Option<Box<dyn Listener>>,
    sessions: Vec<Session>,
    rr_cursor: usize,
    follower: Option<Follower>,
    /// The retention floor `Db::checkpoint`'s GC never deletes past — the min
    /// over live sessions of each session's pin (spec §1). `u64::MAX` (no
    /// pin) when there are no sessions.
    pin_floor: Arc<Mutex<u64>>,
    stats: ReplStats,
}

impl Repl {
    /// Loads/initializes the durable role, gates the `Db` per role, wires the
    /// retention pin, opens the listener when configured, and builds the
    /// follower client on a follower/backup. Returns the startup mismatch
    /// notice (role file vs config) when they disagree (spec §2).
    pub fn open(
        net: Box<dyn Net>,
        role_disk: Box<dyn Disk>,
        cfg: ReplConfig,
        db: &mut Db,
    ) -> io::Result<(Repl, Option<String>)> {
        let (role, notice) = load_role(&*role_disk, &cfg.initial_role)?;

        let pin_floor = Arc::new(Mutex::new(u64::MAX));
        db.pin_retention(Box::new({
            let floor = Arc::clone(&pin_floor);
            move || *floor.lock().expect("uncontended")
        }));

        match &role {
            Role::Primary => db.allow_writes(),
            Role::Follower { primary } | Role::Backup { primary } => {
                db.forbid_writes(primary.clone())
            }
            // A draining primary is write-stopped but still ships (task 7).
            Role::HandingOff { to } => db.forbid_writes(to.clone()),
        }

        let listener = match &cfg.listen {
            Some(addr) => Some(net.listen(addr)?),
            None => None,
        };

        let follower = match &role {
            Role::Follower { primary } | Role::Backup { primary } => {
                Some(Follower::new(primary.clone(), cfg.listen.clone()))
            }
            _ => None,
        };

        Ok((
            Repl {
                role,
                net,
                role_disk,
                listener,
                sessions: Vec::new(),
                rr_cursor: 0,
                follower,
                pin_floor,
                stats: ReplStats::default(),
            },
            notice,
        ))
    }

    /// True only for a live primary — a `HandingOff` node is write-stopped.
    pub fn is_primary(&self) -> bool {
        matches!(self.role, Role::Primary)
    }

    pub fn role(&self) -> &Role {
        &self.role
    }

    pub fn stats(&self) -> ReplStats {
        self.stats
    }

    /// Non-destructive IO movement, called unconditionally each iteration like
    /// `source.poll()`: accept pending conns, drain readable messages into
    /// internal queues, note reconnect timers. No `Db` access.
    pub fn pump_io(&mut self, now_nanos: u64) {
        if let Some(listener) = &self.listener {
            // Accept until there is no pending conn, or the listener is dead
            // (host crashed — recovery reopens it). `while let` stops on both.
            while let Ok(Some(conn)) = listener.accept() {
                self.sessions.push(Session::new(conn));
            }
            self.recompute_pin(); // fresh AwaitHello sessions floor the pin at 0
        }
        for s in &mut self.sessions {
            s.drain();
        }
        if let Some(f) = &mut self.follower {
            f.drain(now_nanos);
        }
    }

    /// True when a queued message, a send backlog (`cursor ≤ db.last_applied`),
    /// a due heartbeat, a dead session to reap, or a due reconnect exists.
    pub fn has_work(&self, db: &Db, now_nanos: u64) -> bool {
        if self.db_needs_reseed() {
            return true; // the caller must run_seed; step() no-ops until it does
        }
        if self.sessions.iter().any(|s| s.dead) {
            return true;
        }
        if self.sessions.iter().any(|s| s.has_work(db, now_nanos)) {
            return true;
        }
        self.follower
            .as_ref()
            .is_some_and(|f| f.has_work(now_nanos))
    }

    /// The earliest future timer (heartbeat or reconnect) this node wants to
    /// wake for. The scheduler mins this with in-flight network deliveries.
    pub fn next_deadline(&self, now_nanos: u64) -> Option<u64> {
        let mut best: Option<u64> = None;
        let mut fold = |d: u64| best = Some(best.map_or(d, |b: u64| b.min(d)));
        for s in &self.sessions {
            if let Some(d) = s.heartbeat_deadline() {
                fold(d);
            }
        }
        if let Some(d) = self
            .follower
            .as_ref()
            .and_then(|f| f.next_deadline(now_nanos))
        {
            fold(d);
        }
        best
    }

    /// One unit of work: service one incoming session (round-robin), else run
    /// one follower-client action. May latch `db_needs_reseed` for the caller.
    pub fn step(&mut self, db: &mut Db, clock: &dyn Clock) -> io::Result<()> {
        // A completed seed is pending: do nothing here, let the caller run
        // run_seed (which needs to consume and return the Db this only borrows).
        if self.db_needs_reseed() {
            return Ok(());
        }
        let now = clock.now().as_nanos();

        let before = self.sessions.len();
        self.sessions.retain(|s| !s.dead);
        if self.sessions.len() != before {
            self.rr_cursor = 0;
            self.recompute_pin();
        }

        let n = self.sessions.len();
        for offset in 0..n {
            let idx = (self.rr_cursor + offset) % n;
            if self.sessions[idx].has_work(db, now) {
                self.rr_cursor = (idx + 1) % n;
                let role = self.role.clone();
                self.sessions[idx].service(db, now, &role)?;
                self.recompute_pin();
                return Ok(());
            }
        }

        if let Some(f) = &mut self.follower {
            return f.step(&*self.net, db, now);
        }
        Ok(())
    }

    /// True once the follower has buffered a complete snapshot stream
    /// (`SnapshotStart` through `SnapshotEnd`): the caller must run
    /// `run_seed`, which consumes and returns the rebuilt `Db`.
    #[doc(hidden)]
    pub fn db_needs_reseed(&self) -> bool {
        self.follower.as_ref().is_some_and(|f| f.needs_reseed())
    }

    /// Rebuilds a fresh follower's `Db` from a completed snapshot stream:
    /// `into_seeder`s the old `Db`, replays the buffered `SnapshotRows`,
    /// `finish`es, re-gates writes, and acks the seed position so the primary
    /// resumes shipping from `applied_seq + 1`. The stream was buffered one
    /// message per step (follower.rs) rather than blocked on, so the single-
    /// writer primary could keep shipping it — this call just replays the
    /// buffer, no IO wait. A conn death before `SnapshotEnd` never latches a
    /// completed seed; the next Hello restarts it (idempotent — `Seeder::new`
    /// discards).
    pub fn run_seed(&mut self, db: Db, clock: &dyn Clock) -> io::Result<Db> {
        let role = self.role.clone();
        let f = self
            .follower
            .as_mut()
            .expect("run_seed is only called on a follower node");
        let (start, rows) = f
            .take_completed_seed()
            .expect("db_needs_reseed latched a completed seed before run_seed");

        let mut seeder = db.into_seeder()?;
        for (table, batch) in &rows {
            seeder.rows(table, batch)?;
        }
        // Counted before `finish`: `finish` checkpoints the store and lays the
        // seed segment, then reopens through `Db::open`, which can hit a read
        // fault AFTER the seeded state is durable — the disk is seeded even
        // though this call returns an error and the node recovers into it, so
        // the seed must count here, not after a `finish` that may fault-late.
        self.stats.seeds += 1;
        let mut new_db = seeder.finish(start.applied_seq, start.next_job_id)?;
        if let Role::Follower { primary } | Role::Backup { primary } = &role {
            new_db.forbid_writes(primary.clone());
        }
        f.ack_seed(start.applied_seq, clock.now().as_nanos());
        Ok(new_db)
    }

    fn recompute_pin(&self) {
        let floor = self
            .sessions
            .iter()
            .map(|s| s.pin_floor())
            .min()
            .unwrap_or(u64::MAX);
        *self.pin_floor.lock().expect("uncontended") = floor;
    }
}
