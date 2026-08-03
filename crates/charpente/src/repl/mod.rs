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
use crate::repl::protocol::Msg;
use crate::repl::role::{Role, load_role, store_role};
use crate::repl::session::{PendingCtrl, Session};
use crate::repl::witnesses::{clear_witnesses, open_witnesses, store_witnesses};

mod follower;
pub mod probe;
pub mod protocol;
pub mod role;
mod session;
pub mod witnesses;

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

/// A fresh connection that never sends its first message (`Hello` or a
/// control verb) is reaped after this long (final-review finding): with no
/// deadline, a peer that opens the replication port and goes silent parks in
/// `AwaitHello` forever, whose `pin_floor()` is 0 — pinning GC at the base
/// indefinitely. Sized generously above `HEARTBEAT_NANOS` so it never races a
/// slow-but-honest peer's first message.
pub const HELLO_DEADLINE_NANOS: u64 = 10 * HEARTBEAT_NANOS;

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
    /// Peer-auth secret (spec §5, auth T7). `Some` refuses any inbound
    /// Hello/Ctrl whose proof doesn't match (`Msg::Refuse`, no state touched);
    /// `None` accepts everything inbound and ignores an unrequired proof.
    /// Outbound, a configured node always attaches its proof — so a
    /// half-configured pair works in exactly one direction (the configured
    /// side's inbound is guarded, the other's is open). The wire is plaintext
    /// TCP: this authenticates peers, it does not encrypt the link.
    pub peer_secret: Option<String>,
}

/// Sim-agnostic counters a workload can assert on without reaching into the
/// simulator — e.g. `stats().seeds` proves a session took the snapshot path.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReplStats {
    /// Snapshot re-seeds this node's follower client has completed.
    pub seeds: u64,
}

/// Which half of the switchover a draining primary is in (spec §2). `Draining`
/// is abortable (deadline → back to `Primary`); `HandedOff` — the `HandOff` is
/// sent — is the point of no return (deadline → stay write-stopped).
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SwitchoverPhase {
    Draining,
    HandedOff,
}

/// A live switchover in progress on the primary side (spec §2).
struct Switchover {
    /// The target follower's listen address (the switchover target).
    to: String,
    /// The control connection (a `Session` by id) awaiting `CtrlOk`/`CtrlErr`.
    ctrl_id: u64,
    phase: SwitchoverPhase,
    /// Virtual-time nanos at which the drain (or the HandedOff wait) gives up.
    deadline: u64,
    /// The ship session id we last sent a `HandOff` to — so a reconnect (a new
    /// session id for the same follower) gets a fresh `HandOff` (loss liveness).
    handoff_sent_id: Option<u64>,
}

/// The replication engine for one node: the durable role, the listener (when
/// configured), the live primary-side sessions, the follower-side client (on
/// a follower/backup), and the retention pin wired into the `Db`.
pub struct Repl {
    role: Role,
    net: Box<dyn Net>,
    /// The durable role file's disk — every role flip (switchover/promote/
    /// demote) writes here before it takes effect.
    role_disk: Box<dyn Disk>,
    /// This node's own replication listen address (from config), stamped into a
    /// follower client's `Hello` so a later switchover can target this node.
    my_listen: Option<String>,
    /// This node's configured peer secret (spec §5, auth T7): `Some` gates
    /// every inbound Hello/Ctrl on a matching proof; `None` accepts all and
    /// attaches nothing outbound. Threaded to `Session::service` (inbound
    /// enforcement) and to every `Follower` this node builds (outbound proof).
    peer_secret: Option<String>,
    listener: Option<Box<dyn Listener>>,
    sessions: Vec<Session>,
    rr_cursor: usize,
    /// Monotonic id source for accepted sessions (control-conn addressing).
    next_session_id: u64,
    follower: Option<Follower>,
    /// A switchover this node initiated as primary (`None` otherwise).
    switchover: Option<Switchover>,
    /// The retention floor `Db::checkpoint`'s GC never deletes past — the min
    /// over live sessions of each session's pin (spec §1). `u64::MAX` (no
    /// pin) when there are no sessions.
    pin_floor: Arc<Mutex<u64>>,
    stats: ReplStats,
    /// Listen addresses this node has shipped to, mirrored from the durable
    /// `witnesses` file (M2 sub-project 0b task 2). Cleared on every durable
    /// flip away from `Primary` — a demoted-and-re-seeded node is on a new
    /// timeline, and a stale witness would block writes after a future
    /// legitimate promotion. Task 5 gates boot on probing this set.
    witnesses: Vec<String>,
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
        // Before any socket/state work (2026-07-24 amendment): `Some("")`
        // would admit any peer presenting no proof at all — almost never
        // what a configured secret intends, and easy to reach via an empty
        // env var or a shell-quoting slip (`--secret ''`). Refused by name
        // instead of silently behaving like `None`.
        if cfg.peer_secret.as_deref() == Some("") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "ReplConfig.peer_secret is Some(\"\") — an empty peer secret authenticates \
                 any peer that presents no proof, which is almost never what a configured \
                 secret intends; set a real secret, or use None to explicitly accept all \
                 peers (DESIGN §8)",
            ));
        }
        let (role, notice) = load_role(&*role_disk, &cfg.initial_role)?;
        // `open_witnesses`, not `load_witnesses`: this call also ESTABLISHES
        // both witness slots (two empty-set writes on a fresh data dir), and
        // it runs here — before the listener is opened, before any Hello can
        // be serviced, before any frame can ship. That ordering is what makes
        // "a torn slot with an absent sibling means empty" sound; see
        // `witnesses.rs`'s module doc.
        let witnesses = open_witnesses(&*role_disk)?;

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
            Role::Follower { primary } | Role::Backup { primary } => Some(Follower::new(
                primary.clone(),
                cfg.listen.clone(),
                cfg.peer_secret.clone(),
            )),
            // A durable `HandingOff` at open means this node restarted mid-
            // switchover (spec §2): it stays write-stopped and probes `to` to
            // resolve (demote to its follower once `to` is confirmed primary).
            Role::HandingOff { to } => Some(Follower::new_resolving(
                to.clone(),
                cfg.listen.clone(),
                cfg.peer_secret.clone(),
            )),
            Role::Primary => None,
        };

        Ok((
            Repl {
                role,
                net,
                role_disk,
                my_listen: cfg.listen.clone(),
                peer_secret: cfg.peer_secret.clone(),
                listener,
                sessions: Vec::new(),
                rr_cursor: 0,
                next_session_id: 0,
                follower,
                switchover: None,
                pin_floor,
                stats: ReplStats::default(),
                witnesses,
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

    /// The switchover half this node is in, if it is draining one (test hook —
    /// the crash-between-handoff-and-demote property drives to `HandedOff`).
    #[doc(hidden)]
    pub fn switchover_phase(&self) -> Option<SwitchoverPhase> {
        self.switchover.as_ref().map(|s| s.phase)
    }

    /// Non-destructive IO movement, called unconditionally each iteration like
    /// `source.poll()`: accept pending conns, drain readable messages into
    /// internal queues, note reconnect timers. No `Db` access.
    pub fn pump_io(&mut self, now_nanos: u64) {
        if let Some(listener) = &self.listener {
            // Accept until there is no pending conn, or the listener is dead
            // (host crashed — recovery reopens it). `while let` stops on both.
            while let Ok(Some(conn)) = listener.accept() {
                let id = self.next_session_id;
                self.next_session_id += 1;
                self.sessions.push(Session::new(conn, id, now_nanos));
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
        // A received hand-off (follower side) or a switchover deadline reached
        // (primary side) both demand a step even with no session traffic.
        if self.follower.as_ref().is_some_and(|f| f.handoff_pending()) {
            return true;
        }
        if self
            .switchover
            .as_ref()
            .is_some_and(|sw| now_nanos >= sw.deadline)
        {
            return true;
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
            if let Some(d) = s.hello_deadline() {
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
        // The switchover drain/HandedOff deadline: the scheduler must advance
        // virtual time to it so the abort/stall fires when nothing else runs.
        if let Some(sw) = &self.switchover {
            fold(sw.deadline);
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

        // Follower side: a switchover HandOff arrived — become primary.
        if self.follower.as_ref().is_some_and(|f| f.handoff_pending()) {
            let at_seq = self
                .follower
                .as_mut()
                .and_then(Follower::take_handoff)
                .expect("handoff_pending was true");
            return self.accept_handoff(db, at_seq, now);
        }

        // Primary side: drive an active switchover. A terminal transition
        // (HandOff sent, completed, aborted, stalled) consumes the step;
        // otherwise fall through so the round-robin ships and catches the
        // target up.
        if self.switchover.is_some() && self.drive_switchover(db, now)? {
            return Ok(());
        }

        let before = self.sessions.len();
        self.sessions.retain(|s| !s.dead);
        if self.sessions.len() != before {
            self.rr_cursor = 0;
            self.recompute_pin();
        }

        // A control verb parked on a fresh conn: execute it (promote, or begin
        // a switchover). Handled before the round-robin, which skips it.
        for idx in 0..self.sessions.len() {
            if self.sessions[idx].pending_ctrl.is_some() {
                return self.begin_control(db, idx, now);
            }
        }

        let n = self.sessions.len();
        for offset in 0..n {
            let idx = (self.rr_cursor + offset) % n;
            if self.sessions[idx].has_work(db, now) {
                self.rr_cursor = (idx + 1) % n;
                let role = self.role.clone();
                self.sessions[idx].service(db, now, &role, &self.peer_secret)?;
                // `Session::service` returns immediately after handling an
                // inbound message (`return self.handle(...)`, session.rs:218),
                // so the Hello that sets `listen` and that session's first
                // *shipping* step are different steps (`Decision::Frames`/
                // `Decision::Snapshot` only set `listen`/`phase`, never send).
                // Recording here — right after `service`, on every
                // round-robin step — makes a new witness durable strictly
                // before any frame can reach that peer, ON THE SUCCESS PATH
                // (fix round 1, Minor #2 — the original wording claimed this
                // unconditionally, which is false: if `record_witnesses`
                // itself fails, step N+1 services the SAME session again,
                // still `Phase::Shipping`, and `service` ships any backlog
                // FIRST — a frame CAN reach the peer before a retried
                // witness lands). Production safety on the failure path does
                // NOT come from this ordering; it comes from `Kernel::step`'s
                // `Ready::Repl` arm propagating this `?` straight out of the
                // whole kernel loop (fail-stop, not an in-process retry) —
                // the only retry in production is the next process start,
                // where `Repl::open` reloads `witnesses` fresh from disk.
                self.record_witnesses()?;
                self.recompute_pin();
                return Ok(());
            }
        }

        // Follower client (also the HandingOff resolver). A confirmed probe
        // demotes this node to a plain follower of `to`.
        let resolved = if let Some(f) = &mut self.follower {
            f.step(&*self.net, db, now)?;
            f.took_resolution()
        } else {
            false
        };
        if resolved {
            let to = match &self.role {
                Role::HandingOff { to } => to.clone(),
                // took_resolution only fires on a HandingOff resolver.
                _ => return Ok(()),
            };
            self.resolve_to_follower(db, &to)?;
        }
        Ok(())
    }

    // ------------------------------------------------------ control plane

    /// Drives an active switchover one step (spec §2). Returns `true` when a
    /// terminal transition consumed this step; `false` to fall through to the
    /// round-robin so the drain keeps shipping to the target.
    fn drive_switchover(&mut self, db: &mut Db, now: u64) -> io::Result<bool> {
        let (to, phase, deadline, ctrl_id, handoff_sent_id) = {
            let sw = self.switchover.as_ref().expect("active switchover");
            (
                sw.to.clone(),
                sw.phase,
                sw.deadline,
                sw.ctrl_id,
                sw.handoff_sent_id,
            )
        };
        let last = db.last_applied();

        // Read the target's acks + any HandOffDone from EVERY session
        // advertising `to` (a reconnect may have left a stale/dead one still in
        // the Vec before the reap; a HandOffDone can also arrive buffered with
        // the peer's EOF). Take the highest acked seen, and prefer a live
        // session to send the HandOff on.
        let mut got_done = false;
        let mut best_acked: Option<u64> = None;
        let mut live_target: Option<usize> = None;
        for (i, s) in self.sessions.iter_mut().enumerate() {
            if !s.control && s.listen.as_deref() == Some(to.as_str()) {
                got_done |= s.drain_switchover_inbox();
                best_acked = best_acked.max(s.acked);
                if !s.dead {
                    live_target = Some(i);
                }
            }
        }
        let caught_up = best_acked == last && last.is_some();

        match phase {
            SwitchoverPhase::Draining => {
                if now >= deadline {
                    self.abort_switchover(db, ctrl_id, &to, last, best_acked, now);
                    return Ok(true);
                }
                // Send the HandOff only when caught up AND a live session
                // exists. A dead target at catch-up means the follower just
                // disconnected — stay Draining and wait for its reconnect.
                if caught_up && let Some(i) = live_target {
                    let at = last.expect("caught_up implies a durable position");
                    self.sessions[i].reply(&Msg::HandOff { at_seq: at }, now);
                    // A failed send (conn drop) leaves us Draining — the
                    // follower reconnects and the drain resumes. A success is
                    // the point of no return.
                    if !self.sessions[i].dead {
                        let sid = self.sessions[i].id;
                        let sw = self.switchover.as_mut().expect("active switchover");
                        sw.phase = SwitchoverPhase::HandedOff;
                        sw.handoff_sent_id = Some(sid);
                    }
                    return Ok(true);
                }
                Ok(false)
            }
            SwitchoverPhase::HandedOff => {
                if got_done {
                    self.complete_switchover(db, ctrl_id, &to, now)?;
                    return Ok(true);
                }
                if now >= deadline {
                    self.stall_switchover(ctrl_id, &to, now);
                    return Ok(true);
                }
                // The HandOff was lost and the follower reconnected (a fresh
                // session id, caught up again): resend once. If the follower
                // already promoted it will not reconnect, so this never
                // double-promotes.
                if caught_up && let Some(i) = live_target {
                    let sid = self.sessions[i].id;
                    if handoff_sent_id != Some(sid) {
                        let at = last.expect("caught_up implies a durable position");
                        self.sessions[i].reply(&Msg::HandOff { at_seq: at }, now);
                        if !self.sessions[i].dead {
                            let sw = self.switchover.as_mut().expect("active switchover");
                            sw.handoff_sent_id = Some(sid);
                        }
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    /// Executes a control verb parked on session `idx`.
    fn begin_control(&mut self, db: &mut Db, idx: usize, now: u64) -> io::Result<()> {
        let verb = self.sessions[idx]
            .pending_ctrl
            .take()
            .expect("begin_control on a session with a pending verb");
        let ctrl_id = self.sessions[idx].id;
        match verb {
            PendingCtrl::Promote => self.do_promote(db, ctrl_id, now),
            PendingCtrl::Switchover(to) => self.begin_switchover(db, ctrl_id, to, now),
        }
    }

    /// `promote` — failover, bounded loss (spec §2). A follower/backup flips
    /// durably to primary, drops its upstream client, and starts serving.
    fn do_promote(&mut self, db: &mut Db, ctrl_id: u64, now: u64) -> io::Result<()> {
        match self.role.clone() {
            Role::Primary => {
                self.reply_ctrl(
                    ctrl_id,
                    &Msg::CtrlErr {
                        message: "cannot promote: this node is already the primary".to_string(),
                    },
                    now,
                );
                return Ok(());
            }
            Role::HandingOff { to } => {
                self.reply_ctrl(
                    ctrl_id,
                    &Msg::CtrlErr {
                        message: format!(
                            "cannot promote: this node is handing off to {to} — let the \
                             switchover finish or restart to resolve, then promote if needed"
                        ),
                    },
                    now,
                );
                return Ok(());
            }
            Role::Follower { .. } | Role::Backup { .. } => {}
        }
        if self.my_listen.is_none() {
            self.reply_ctrl(
                ctrl_id,
                &Msg::CtrlErr {
                    message: "cannot promote: this node has no replication listen address and \
                              so cannot serve as a primary — restart it with one configured"
                        .to_string(),
                },
                now,
            );
            return Ok(());
        }
        store_role(&*self.role_disk, &Role::Primary)?;
        self.role = Role::Primary;
        db.allow_writes();
        // A promoted follower is now the writable node: run the schema
        // gate before acking — it resumes any in-flight watermark the dead
        // primary left mid-migration (spec §4).
        db.migrate_schema()?;
        self.follower = None; // no upstream — this is the authoritative timeline now
        let last = db.last_applied();
        self.reply_ctrl(
            ctrl_id,
            &Msg::CtrlOk {
                message: format!(
                    "promoted to primary at last_applied {last:?} — records the previous primary \
                     acked but had not yet shipped are lost (the DECISIONS §6 failover window); \
                     this timeline is now authoritative"
                ),
            },
            now,
        );
        self.recompute_pin();
        Ok(())
    }

    /// `switchover` — begin the zero-loss drain (spec §2). Requires a live
    /// session from a follower advertising `to`; flips durably to
    /// `HandingOff(to)`, write-stops, and starts draining. The control conn is
    /// held open (by id) for the eventual `CtrlOk`/`CtrlErr`.
    fn begin_switchover(
        &mut self,
        db: &mut Db,
        ctrl_id: u64,
        to: String,
        now: u64,
    ) -> io::Result<()> {
        match self.role.clone() {
            Role::Follower { primary } | Role::Backup { primary } => {
                self.reply_ctrl(
                    ctrl_id,
                    &Msg::CtrlErr {
                        message: format!(
                            "cannot switch over: this node is not the primary (it follows \
                             {primary}) — issue the switchover to the primary"
                        ),
                    },
                    now,
                );
                return Ok(());
            }
            Role::HandingOff { to: cur } => {
                self.reply_ctrl(
                    ctrl_id,
                    &Msg::CtrlErr {
                        message: format!("cannot switch over: already handing off to {cur}"),
                    },
                    now,
                );
                return Ok(());
            }
            Role::Primary => {}
        }
        let found = self
            .sessions
            .iter()
            .any(|s| !s.control && s.listen.as_deref() == Some(to.as_str()));
        if !found {
            let known: Vec<String> = self
                .sessions
                .iter()
                .filter(|s| !s.control)
                .filter_map(|s| s.listen.clone())
                .collect();
            self.reply_ctrl(
                ctrl_id,
                &Msg::CtrlErr {
                    message: format!(
                        "cannot switch over to {to}: no replication session from a follower \
                         listening at {to} — known followers: {known:?} (the target must be a \
                         connected follower advertising {to} as its listen address)"
                    ),
                },
                now,
            );
            return Ok(());
        }
        // Point of departure: durable role flips BEFORE we act on it.
        store_role(&*self.role_disk, &Role::HandingOff { to: to.clone() })?;
        self.role = Role::HandingOff { to: to.clone() };
        db.forbid_writes(to.clone());
        self.switchover = Some(Switchover {
            to,
            ctrl_id,
            phase: SwitchoverPhase::Draining,
            deadline: now + SWITCHOVER_DEADLINE_NANOS,
            handoff_sent_id: None,
        });
        Ok(())
    }

    /// A `HandOff` landed on the follower client: verify the position, flip
    /// durably to primary, ack with `HandOffDone`, and start serving Hellos.
    fn accept_handoff(&mut self, db: &mut Db, at_seq: u64, _now: u64) -> io::Result<()> {
        if db.last_applied() != Some(at_seq) {
            // The drain waits for acked == last, so this should be impossible;
            // a named protocol error rather than a silent wrong promotion.
            let msg = format!(
                "hand-off protocol error: the primary handed off at seq {at_seq} but this \
                 follower's last_applied is {:?} — the drain did not reach equality",
                db.last_applied()
            );
            if let Some(f) = &self.follower {
                f.refuse(msg.clone());
            }
            return Err(io::Error::other(msg));
        }
        if self.my_listen.is_none() {
            let msg = "hand-off refused: this follower has no replication listen address and \
                       cannot become a primary"
                .to_string();
            if let Some(f) = &self.follower {
                f.refuse(msg.clone());
            }
            return Err(io::Error::other(msg));
        }
        store_role(&*self.role_disk, &Role::Primary)?;
        self.role = Role::Primary;
        db.allow_writes();
        if let Some(f) = &self.follower {
            f.send_handoff_done(); // on the old client conn, before it is dropped
        }
        self.follower = None; // drops the upstream conn — this node is primary
        self.recompute_pin();
        Ok(())
    }

    /// The drain caught up and the follower confirmed `HandOffDone`: flip
    /// durably to `Follower(to)`, drop the old ship sessions, and start a
    /// follower client toward the new primary. No divergence is possible — the
    /// drain reached byte-for-byte equality.
    fn complete_switchover(
        &mut self,
        db: &mut Db,
        ctrl_id: u64,
        to: &str,
        now: u64,
    ) -> io::Result<()> {
        store_role(
            &*self.role_disk,
            &Role::Follower {
                primary: to.to_string(),
            },
        )?;
        self.role = Role::Follower {
            primary: to.to_string(),
        };
        // Clear-on-flip (dispatch Correction 1, site 1): a demoted-and-
        // re-seeded node is on a new timeline, and a stale witness would
        // block writes after a future legitimate promotion.
        clear_witnesses(&*self.role_disk)?;
        self.witnesses.clear();
        db.forbid_writes(to.to_string());
        // Reply on the control conn while it is still in the Vec.
        self.reply_ctrl(
            ctrl_id,
            &Msg::CtrlOk {
                message: format!(
                    "switchover to {to} complete — this node is now a follower of {to}"
                ),
            },
            now,
        );
        self.sessions.clear(); // drop every ship session (and the control conn)
        self.switchover = None;
        self.follower = Some(Follower::new(
            to.to_string(),
            self.my_listen.clone(),
            self.peer_secret.clone(),
        ));
        self.recompute_pin();
        Ok(())
    }

    /// The drain could not catch the follower up before the deadline (still
    /// Draining): abort back to `Primary`, resume writes, and name the lag.
    fn abort_switchover(
        &mut self,
        db: &mut Db,
        ctrl_id: u64,
        to: &str,
        last: Option<u64>,
        acked: Option<u64>,
        now: u64,
    ) {
        // The abort is not durable-critical, but the role must return to
        // Primary before writes resume; a failed write is fault fallout.
        let _ = store_role(&*self.role_disk, &Role::Primary);
        self.role = Role::Primary;
        db.allow_writes();
        let lag = match (last, acked) {
            (Some(l), Some(a)) => l.saturating_sub(a),
            (Some(l), None) => l + 1,
            _ => 0,
        };
        self.reply_ctrl(
            ctrl_id,
            &Msg::CtrlErr {
                message: format!(
                    "switchover to {to} aborted: the follower lag is {lag} record(s) (it acked \
                     {acked:?}, this primary is at {last:?}) and the drain did not complete \
                     before the {SWITCHOVER_DEADLINE_NANOS}ns deadline — this node is primary \
                     again and accepting writes"
                ),
            },
            now,
        );
        self.switchover = None;
        self.recompute_pin();
    }

    /// The `HandOff` was sent (point of no return) but no `HandOffDone` came
    /// back before the deadline: stay write-stopped, name the situation. The
    /// durable role stays `HandingOff` — a restart resolves against `to`.
    fn stall_switchover(&mut self, ctrl_id: u64, to: &str, now: u64) {
        self.reply_ctrl(
            ctrl_id,
            &Msg::CtrlErr {
                message: format!(
                    "switchover to {to}: the hand-off was sent but no confirmation returned \
                     before the deadline — this node stays write-stopped; verify {to} has been \
                     promoted (its role file says primary), then restart this node to resolve \
                     against {to}"
                ),
            },
            now,
        );
        // Stop actively driving; the node remains HandingOff (write-stopped)
        // until an operator restarts it (it never silently resumes Primary).
        self.switchover = None;
    }

    /// A durable demote from `HandingOff` (startup resolution): flip to
    /// `Follower(to)`, drop any ship sessions accepted while draining, and
    /// reset the pin. The resolver follower client keeps running as a plain
    /// follower of `to`.
    fn resolve_to_follower(&mut self, db: &mut Db, to: &str) -> io::Result<()> {
        store_role(
            &*self.role_disk,
            &Role::Follower {
                primary: to.to_string(),
            },
        )?;
        self.role = Role::Follower {
            primary: to.to_string(),
        };
        // Clear-on-flip (dispatch Correction 1, site 2): same reasoning as
        // `complete_switchover`'s clear above.
        clear_witnesses(&*self.role_disk)?;
        self.witnesses.clear();
        db.forbid_writes(to.to_string());
        self.sessions.clear();
        self.recompute_pin();
        Ok(())
    }

    /// Replies on the control conn addressed by `ctrl_id`; a no-op when it has
    /// died (the durable role change stands regardless — the operator re-checks).
    fn reply_ctrl(&mut self, ctrl_id: u64, msg: &Msg, now: u64) {
        if let Some(s) = self.sessions.iter_mut().find(|s| s.id == ctrl_id) {
            s.reply(msg, now);
        }
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
        let mut new_db = seeder.finish(
            start.applied_seq,
            start.next_job_id,
            start.next_event_id,
            start.frontier_crc,
        )?;
        if let Role::Follower { primary } | Role::Backup { primary } = &role {
            new_db.forbid_writes(primary.clone());
        }
        f.ack_seed(start.applied_seq, clock.now().as_nanos());
        Ok(new_db)
    }

    /// Persists any newly-known peer listen address. Called after a session
    /// is serviced (mod.rs `step`): `Session::handle` sets `listen` only on
    /// the two `Decision` arms that agree to ship (session.rs:287-296,
    /// `Frames` and `Snapshot`), so a refused peer is never a witness, and
    /// `decide_hello` refuses an unrecordable `listen` before either
    /// `Decision` is reached (session.rs) — so every address seen here is
    /// already valid and `store_witnesses`'s `Err` below can only be a
    /// genuine IO fault, never a rejected address.
    ///
    /// Persists BEFORE committing to the in-memory mirror (dispatch
    /// Correction 3): a push-then-store shape would mark an address "known"
    /// in memory the instant a (possibly failing) store is attempted, so a
    /// later step's `!self.witnesses.contains(addr)` guard would then be
    /// false and a failed durable write would never be retried — the
    /// witness lost from disk forever while memory claims it is there.
    fn record_witnesses(&mut self) -> io::Result<()> {
        let mut fresh: Vec<String> = Vec::new();
        for s in &self.sessions {
            if let Some(addr) = &s.listen
                && !self.witnesses.contains(addr)
                && !fresh.contains(addr)
            {
                fresh.push(addr.clone());
            }
        }
        if fresh.is_empty() {
            return Ok(());
        }
        let mut next = self.witnesses.clone();
        next.extend(fresh);
        store_witnesses(&*self.role_disk, &next)?;
        self.witnesses = next;
        Ok(())
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
