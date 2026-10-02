//! The primary-side replication session (spec docs/superpowers/specs/
//! 2026-07-08-replication-design.md §1): one `Session` per accepted
//! connection. A session opens on the peer's first message — a `Hello` names
//! its position and runs the decision ladder (resume / re-seed / refuse); a
//! control verb is answered (Task 7 replaces the stub); anything else is
//! refused. Once shipping, the session pushes `Frames` batches as commits
//! land and `Heartbeat`s when idle, and folds the follower's `Ack`s into the
//! retention pin (mod.rs). The follower-side client lives in follower.rs.

use std::collections::VecDeque;
use std::io;

use charpente_core::io::Conn;
use charpente_core::log::frame_crc;

use crate::Db;
use crate::auth::{ct_eq_hex, hash_token};
use crate::repl::protocol::{Msg, decode, encode};
use crate::repl::role::{Role, reseed_steps};
use crate::repl::witnesses::{MAX_WITNESS_ADDRESS_BYTES, valid_witness_address};
use crate::repl::{
    FRAMES_BATCH_BYTES, HEARTBEAT_NANOS, HELLO_DEADLINE_NANOS, MAX_MESSAGE_BYTES, PROTO_VERSION,
    SNAPSHOT_BATCH_BYTES,
};

/// Where a session is in its lifecycle. `AwaitHello` is the just-accepted
/// state; `Seeding` streams a pre-rendered snapshot (one message per `step`);
/// `Shipping` pushes frames and heartbeats.
pub(crate) enum Phase {
    AwaitHello,
    /// The snapshot stream, rendered once from a single MVCC `Snapshot` at
    /// `begin_seeding` and drained one message per step (`SnapshotStart`,
    /// `SnapshotRows`…, `SnapshotEnd`). Consistency comes from the one
    /// snapshot the batches were read out of; the primary keeps committing
    /// underneath, and the retention pin (mod.rs) keeps the frames from
    /// `applied_seq + 1` that must follow.
    Seeding {
        pending: VecDeque<Msg>,
    },
    Shipping,
}

/// A control verb parked on a fresh connection, decoded but not yet acted on:
/// role changes are `Repl`-level (they flip the durable role, re-gate the `Db`,
/// re-wire the retention pin), so the session only records the verb and `Repl`
/// executes it in `step` (Task 7).
pub(crate) enum PendingCtrl {
    Promote,
    Switchover(String),
}

/// One primary-side replication session.
pub(crate) struct Session {
    conn: Box<dyn Conn>,
    /// Stable id assigned at accept: `Repl` addresses the control connection to
    /// reply on (across reaps that reorder the Vec) and the ship session it
    /// last sent a `HandOff` to by this id.
    pub id: u64,
    /// The follower's own replication address, learned from its `Hello`
    /// (switchover targets sessions by it — Task 7).
    pub listen: Option<String>,
    /// The next seq to ship to this follower.
    pub cursor: u64,
    /// The highest seq the follower has acknowledged durable, if any.
    pub acked: Option<u64>,
    /// The position this follower's `Hello` carried, once `decide_hello`
    /// verified it against this log (a `Frames` answer resuming past it). The
    /// follower holds every record up to it durably, so a switchover counts it
    /// as acked: a target that reconnects already caught up to a write-stopped
    /// primary is shipped nothing and never sends another `Ack` (D4 round 2).
    /// A later `Hello` on the same connection answered `Snapshot` clears it.
    pub hello_position: Option<u64>,
    phase: Phase,
    /// A control connection: its first message was a control verb, not a
    /// `Hello`. It ships nothing, heartbeats nothing, and pins no retention;
    /// `Repl` drives the verb and replies on it.
    pub control: bool,
    /// A decoded-but-unexecuted control verb (`Repl` consumes it in `step`).
    pub pending_ctrl: Option<PendingCtrl>,
    /// Lazily allocated once, reused: sized to the largest message the wire
    /// can carry (spec §1's "one buffer per conn").
    recv_buf: Option<Vec<u8>>,
    inbox: VecDeque<Vec<u8>>,
    /// Virtual-time nanos of the last send — the heartbeat cadence clock.
    last_sent: u64,
    /// Virtual-time nanos this session was accepted — the `AwaitHello`
    /// arrival-deadline clock, mirroring `last_sent`'s heartbeat clock.
    created_at: u64,
    /// The peer's wire died (send/recv error or EOF): reaped by mod.rs, which
    /// then recomputes the pin.
    pub dead: bool,
}

impl Session {
    pub(crate) fn new(conn: Box<dyn Conn>, id: u64, now: u64) -> Session {
        Session {
            conn,
            id,
            listen: None,
            cursor: 0,
            acked: None,
            hello_position: None,
            phase: Phase::AwaitHello,
            control: false,
            pending_ctrl: None,
            recv_buf: None,
            inbox: VecDeque::new(),
            last_sent: 0,
            created_at: now,
            dead: false,
        }
    }

    /// The retention floor this session pins: `acked + 1` once acking, else
    /// its `cursor` (0 while awaiting a Hello — conservative; the snapshot
    /// position + 1 while seeding; the resume point while shipping). mod.rs
    /// mins this over live sessions (spec §1's retention pin). A silent
    /// `AwaitHello` session only pins at 0 until `hello_deadline` reaps it —
    /// otherwise it would hold GC at 0 forever.
    pub(crate) fn pin_floor(&self) -> u64 {
        // A control connection ships nothing and must never hold GC.
        if self.control {
            return u64::MAX;
        }
        // Nor does a dead one — and `dead` is set at every wire-failure site
        // too (send/drain error, EOF), not only on refusal, so this guards
        // ANY dead session, including one mid-shipping with a real
        // `cursor`/`acked`: it releases that pin here rather than waiting for
        // the reap to drop the session. `Repl::step` reaps at the top of the
        // step but calls `recompute_pin` right after `service` (and
        // `pump_io` does its own), so a session that died THIS step is still
        // in `self.sessions` when the pin is recomputed — for the refused
        // case (`cursor = 0, acked = None`) that pins GC at 0 until a step
        // reaches the reap, costing that window's checkpoint its GC, once
        // per refusal on every refusal path, and recurring against a peer
        // that keeps retrying (0b re-review M-2). Bounded, not permanent:
        // the reap is what ends it, measured 0 -> 0 -> 0 -> MAX. Delayed GC,
        // never wrong GC.
        if self.dead {
            return u64::MAX;
        }
        self.acked.map_or(self.cursor, |a| a + 1)
    }

    /// Non-destructive IO movement (pump_io): drain every readable message
    /// into the inbox. A recv error or EOF marks the session dead — a dead
    /// wire ships nothing further and is reaped next step.
    pub(crate) fn drain(&mut self) {
        let buf = self
            .recv_buf
            .get_or_insert_with(|| vec![0u8; MAX_MESSAGE_BYTES]);
        loop {
            match self.conn.recv(buf) {
                Ok(Some(0)) => {
                    self.dead = true;
                    return;
                }
                Ok(Some(n)) => self.inbox.push_back(buf[..n].to_vec()),
                Ok(None) => return,
                Err(_) => {
                    self.dead = true;
                    return;
                }
            }
        }
    }

    /// True when this session has a unit of work to do now.
    pub(crate) fn has_work(&self, db: &Db, now: u64) -> bool {
        // A control connection's only work is a decoded verb waiting for `Repl`
        // to execute it; it never ships or heartbeats.
        if self.control {
            return self.pending_ctrl.is_some();
        }
        if !self.inbox.is_empty() {
            return true;
        }
        match self.phase {
            Phase::Seeding { .. } => true,
            Phase::Shipping => {
                let backlog = db.last_applied().is_some_and(|last| self.cursor <= last);
                backlog || now >= self.last_sent + HEARTBEAT_NANOS
            }
            // Due once the arrival deadline passes: `service` reaps it (a
            // peer that opened the wire and never sent Hello or a control
            // verb must not pin retention at 0 forever).
            Phase::AwaitHello => now >= self.created_at + HELLO_DEADLINE_NANOS,
        }
    }

    /// The next future heartbeat deadline, if this session is a caught-up
    /// shipper waiting to send liveness (the scheduler advances virtual time
    /// to the earliest such deadline when nothing else is runnable).
    pub(crate) fn heartbeat_deadline(&self) -> Option<u64> {
        matches!(self.phase, Phase::Shipping).then(|| self.last_sent + HEARTBEAT_NANOS)
    }

    /// The deadline by which a fresh connection must send its first message,
    /// if it is still `AwaitHello` — mirrors `heartbeat_deadline`'s shape so
    /// the scheduler wakes for it the same way. A control conn is excluded:
    /// it DID send its first message (the verb flips `control`, not `phase`),
    /// pins nothing, and is never serviced by the round-robin — a deadline
    /// here would go permanently stale once passed, poisoning the node's
    /// earliest-timer min forever (found by the switchover-abort test: the
    /// stale min hid the real 30s drain deadline behind a `d > now` filter).
    pub(crate) fn hello_deadline(&self) -> Option<u64> {
        (!self.control && matches!(self.phase, Phase::AwaitHello))
            .then(|| self.created_at + HELLO_DEADLINE_NANOS)
    }

    /// One unit of work: handle one inbox message, or push one
    /// snapshot/frames/heartbeat message. `role` decides `Hello` handling — a
    /// primary serves the ladder, a follower/backup refuses (no cascading).
    /// `peer_secret` (spec §5, auth T7) gates every inbound Hello/Ctrl on a
    /// matching proof when configured (`Some`); `None` accepts all.
    /// `witnesses` is this node's durable witness set (M2 interlude, spec
    /// §1/§2) — `decide_hello` refuses to seed a peer absent from it while
    /// `db.verifying()` is true.
    pub(crate) fn service(
        &mut self,
        db: &Db,
        now: u64,
        role: &Role,
        peer_secret: &Option<String>,
        witnesses: &[String],
    ) -> io::Result<()> {
        // A control connection is driven by `Repl` (which owns the durable role
        // and pin); the round-robin never ships on it.
        if self.control {
            return Ok(());
        }
        if let Some(raw) = self.inbox.pop_front() {
            let msg = match decode(&raw) {
                Ok(msg) => msg,
                // Garbage from one inbound conn must not error the node: refuse
                // by name (both ends log the same words) and drop the session.
                Err(e) => {
                    self.send(
                        &Msg::Refuse {
                            message: e.to_string(),
                        },
                        now,
                    );
                    self.dead = true;
                    return Ok(());
                }
            };
            return self.handle(msg, db, now, role, peer_secret, witnesses);
        }
        match &mut self.phase {
            Phase::Seeding { pending } => {
                if let Some(msg) = pending.pop_front() {
                    let end = pending.is_empty();
                    self.send(&msg, now);
                    if end {
                        self.phase = Phase::Shipping;
                    }
                }
                Ok(())
            }
            Phase::Shipping => {
                if db.last_applied().is_some_and(|last| self.cursor <= last) {
                    self.ship_frames(db, now)
                } else {
                    if now >= self.last_sent + HEARTBEAT_NANOS {
                        self.send(
                            &Msg::Heartbeat {
                                next_seq: self.cursor,
                            },
                            now,
                        );
                    }
                    Ok(())
                }
            }
            // Silent past its arrival deadline: reap it (mod.rs's step()
            // retains non-dead sessions and recomputes the pin next round —
            // the same path a dropped wire already takes).
            Phase::AwaitHello => {
                if now >= self.created_at + HELLO_DEADLINE_NANOS {
                    self.dead = true;
                }
                Ok(())
            }
        }
    }

    fn handle(
        &mut self,
        msg: Msg,
        db: &Db,
        now: u64,
        role: &Role,
        peer_secret: &Option<String>,
        witnesses: &[String],
    ) -> io::Result<()> {
        match msg {
            Msg::Hello {
                proto,
                listen,
                position,
                secret,
                schema_digest,
            } => {
                match decide_hello(
                    role,
                    db,
                    proto,
                    &listen,
                    position,
                    peer_secret,
                    &secret,
                    schema_digest,
                    witnesses,
                )? {
                    Decision::Refuse(message) => {
                        self.send(&Msg::Refuse { message }, now);
                        self.dead = true;
                    }
                    Decision::Frames { from } => {
                        self.listen = listen;
                        self.hello_position = from.checked_sub(1);
                        self.cursor = from;
                        self.last_sent = now;
                        self.phase = Phase::Shipping;
                    }
                    Decision::Snapshot => {
                        self.listen = listen;
                        // A position an earlier Hello on this connection proved
                        // is not this answer's: the peer is being seeded, and
                        // only its acks may show it caught up again.
                        self.hello_position = None;
                        self.begin_seeding(db, now)?;
                    }
                }
                Ok(())
            }
            Msg::Ack { durable_seq } => {
                self.acked = Some(durable_seq);
                Ok(())
            }
            // A control verb opens a control connection: record it and let
            // `Repl` execute it (the role flip, `Db` re-gate, and pin re-wire
            // are all `Repl`-level). No reply here — `Repl` answers on this
            // conn once the verb resolves. The secret check comes first (spec
            // §5): a mismatch refuses and closes before `control`/
            // `pending_ctrl` are ever touched — no ladder entry.
            Msg::CtrlPromote { secret } => {
                if !secret_matches(peer_secret, &secret) {
                    self.send(
                        &Msg::Refuse {
                            message: PEER_SECRET_REFUSAL.to_string(),
                        },
                        now,
                    );
                    self.dead = true;
                    return Ok(());
                }
                self.control = true;
                self.pending_ctrl = Some(PendingCtrl::Promote);
                Ok(())
            }
            Msg::CtrlSwitchover { to, secret } => {
                if !secret_matches(peer_secret, &secret) {
                    self.send(
                        &Msg::Refuse {
                            message: PEER_SECRET_REFUSAL.to_string(),
                        },
                        now,
                    );
                    self.dead = true;
                    return Ok(());
                }
                self.control = true;
                self.pending_ctrl = Some(PendingCtrl::Switchover(to));
                Ok(())
            }
            // Hand-off frames travel over an established ship session or the
            // follower client, never a fresh conn — one arriving here is a
            // protocol error (a `HandOffDone` on the live drain session is
            // drained by `Repl` before the round-robin, see
            // `drain_switchover_inbox`).
            other @ (Msg::HandOff { .. } | Msg::HandOffDone) => {
                self.send(
                    &Msg::Refuse {
                        message: format!(
                            "unexpected {} on a fresh replication connection — hand-off frames \
                             belong to an established switchover, not a new conn",
                            other.name()
                        ),
                    },
                    now,
                );
                self.dead = true;
                Ok(())
            }
            other => {
                self.send(
                    &Msg::Refuse {
                        message: format!(
                            "unexpected {} on a fresh replication connection — a session opens \
                             with Hello, an admin connection with a control verb",
                            other.name()
                        ),
                    },
                    now,
                );
                self.dead = true;
                Ok(())
            }
        }
    }

    /// Renders the whole snapshot stream from one MVCC `Snapshot` and queues
    /// it; the session then drains it one message per step. Sets the resume
    /// cursor to `applied_seq + 1` — exactly the frames the retention pin will
    /// keep.
    fn begin_seeding(&mut self, db: &Db, now: u64) -> io::Result<()> {
        let snap = db.snapshot()?;
        let applied_seq = snap
            .applied_seq()?
            .expect("a primary chosen for snapshot seeding has applied records");
        let next_job_id = snap.next_job_id()?;
        let next_event_id = snap.next_event_id()?;
        // The frontier CRC the follower will Hello with before any frame
        // ships: our own `frame_crc` of the record at `applied_seq`. Seeding
        // pins retention (a fresh session pins GC at its cursor 0 until this
        // very call sets it to `applied_seq + 1`), so on an ordinary primary
        // the record is retained and read_log returns it. A primary that was
        // itself seeded and promoted before any commit holds no frame at
        // `applied_seq` at all (its log starts empty above it) — read_log
        // refuses below-base and the frontier chains through this node's own
        // stored seed frontier instead (the CRC the original primary vouched).
        // Any other refusal (a corrupt frame below the acked end) propagates
        // by name — the same surface decide_hello's resume check already has.
        let frontier_crc = match db.read_log(applied_seq, 1) {
            Ok(frames) => frames
                .first()
                .map(|(s, p)| frame_crc(*s, p))
                .expect("read_log(applied_seq, 1) returns that frame when it does not refuse"),
            Err(e) if e.kind() == io::ErrorKind::InvalidData && applied_seq < db.log_base() => {
                match db.seed_frontier()? {
                    Some((seed_seq, crc)) if seed_seq == applied_seq => crc,
                    // Negative space: below-base means base == applied_seq + 1
                    // (gc keeps base <= applied + 1; Db::open refuses vanished
                    // prefixes), and the only route to that shape with no
                    // matching seed frontier is a log that vanished beneath a
                    // durable horizon — a node that cannot vouch a frontier
                    // must not seed followers; surface the refusal.
                    _ => return Err(e),
                }
            }
            Err(e) => return Err(e),
        };
        let mut pending = VecDeque::new();
        pending.push_back(Msg::SnapshotStart {
            applied_seq,
            next_job_id,
            next_event_id,
            frontier_crc,
        });
        for table in db.table_names() {
            let table_scoped = db.table_scoped(table)?;
            let rows = snap.scan_raw(table, table_scoped)?;
            // The message shape follows the table's own scopedness, never a
            // row's value (R35) — an unscoped table's rows travel as the
            // `a2d8bbb`-shaped `SnapshotRows` (tag 5, byte-identical);
            // a scoped table's travel as `ScopedSnapshotRows` (tag 14),
            // since a batch can span more than one tenant.
            if table_scoped {
                let mut batch: Vec<(u64, u64, Vec<u8>)> = Vec::new();
                let mut bytes = 0usize;
                for (scope, id, row) in rows {
                    bytes += row.len() + 20;
                    batch.push((scope, id, row));
                    if bytes >= SNAPSHOT_BATCH_BYTES {
                        pending.push_back(Msg::ScopedSnapshotRows {
                            table: table.to_string(),
                            rows: std::mem::take(&mut batch),
                        });
                        bytes = 0;
                    }
                }
                // One message per table, even when empty: the follower's
                // importer walks the same table list and a no-op keeps the
                // two sides' message counts in lockstep with the schema
                // order.
                pending.push_back(Msg::ScopedSnapshotRows {
                    table: table.to_string(),
                    rows: batch,
                });
            } else {
                let mut batch: Vec<(u64, Vec<u8>)> = Vec::new();
                let mut bytes = 0usize;
                for (scope, id, row) in rows {
                    assert_eq!(
                        scope,
                        charpente_core::table::UNSCOPED,
                        "begin_seeding: table \"{table}\" is not tenant-scoped, but \
                         scan_raw returned a row at scope {scope} — an unscoped table's \
                         rows are always UNSCOPED"
                    );
                    bytes += row.len() + 12;
                    batch.push((id, row));
                    if bytes >= SNAPSHOT_BATCH_BYTES {
                        pending.push_back(Msg::SnapshotRows {
                            table: table.to_string(),
                            rows: std::mem::take(&mut batch),
                        });
                        bytes = 0;
                    }
                }
                pending.push_back(Msg::SnapshotRows {
                    table: table.to_string(),
                    rows: batch,
                });
            }
        }
        pending.push_back(Msg::SnapshotEnd);
        self.cursor = applied_seq + 1;
        self.last_sent = now;
        self.phase = Phase::Seeding { pending };
        Ok(())
    }

    fn ship_frames(&mut self, db: &Db, now: u64) -> io::Result<()> {
        // read_log is CRC-verified and capped at last_applied — the unsynced
        // tail never travels, and a frame corrupted on disk is refused here
        // rather than shipped (DESIGN §5).
        let frames = db.read_log(self.cursor, FRAMES_BATCH_BYTES)?;
        if let Some((last, _)) = frames.last() {
            self.cursor = last + 1;
            self.send(&Msg::Frames { frames }, now);
        }
        Ok(())
    }

    /// Encodes and sends one message; a wire failure marks the session dead
    /// (reaped and re-established by the follower's reconnect) rather than
    /// erroring the whole node — a dropped replication conn is a routine
    /// event, not a fault the engine must recover from.
    fn send(&mut self, msg: &Msg, now: u64) {
        if self.conn.send(&encode(msg)).is_err() {
            self.dead = true;
        } else {
            self.last_sent = now;
        }
    }

    /// Sends one message on this session's conn (a control reply, or the drain's
    /// `HandOff`); marks the session dead on wire failure so `Repl` can react.
    pub(crate) fn reply(&mut self, msg: &Msg, now: u64) {
        self.send(msg, now);
    }

    /// During an active switchover, `Repl` drains the target ship session's
    /// inbox here — instead of the round-robin's `service` — so a `HandOffDone`
    /// buffered alongside the peer's EOF is read before the dead-session reap
    /// discards it. Folds `Ack`s into `acked` (drain-completion rides them);
    /// returns whether a `HandOffDone` was seen. A malformed frame on the
    /// reliable sim wire is ignored (the drain has its deadline backstop).
    pub(crate) fn drain_switchover_inbox(&mut self) -> bool {
        let mut done = false;
        while let Some(raw) = self.inbox.pop_front() {
            if let Ok(msg) = decode(&raw) {
                match msg {
                    Msg::Ack { durable_seq } => self.acked = Some(durable_seq),
                    Msg::HandOffDone => done = true,
                    _ => {}
                }
            }
        }
        done
    }
}

enum Decision {
    Refuse(String),
    Frames { from: u64 },
    Snapshot,
}

/// The peer-secret refusal (spec §5/§6, auth T7): pinned verbatim by tests.
pub(crate) const PEER_SECRET_REFUSAL: &str = "peer secret missing or mismatched — set \
     ReplConfig.peer_secret to match this node (or run the CLI verb with --secret)";

/// The not-a-primary marker fragment (M2 sub-project 0b task 4, the witness
/// probe client): shared by the refusal built below, the follower-side
/// `HandingOff` resolver's check (`follower.rs`), and the probe's classifier
/// (`probe.rs`) — one const, three readers, so producer and consumers cannot
/// drift apart. Rewording the fragment this const carries deliberately
/// reddens both consumers' classification; that coupling is the point, not
/// an accident (see `probe.rs`'s module doc).
pub(crate) const NOT_A_PRIMARY_MARKER: &str = "not a primary";

/// The keyed fragment of the seed-while-verifying refusal (M2 interlude,
/// spec §3) — same CONSUMER discipline as `NOT_A_PRIMARY_MARKER`: `follower.rs`'s
/// BOTH refuse arms match on this fragment (back off and retry rather than
/// dying), and, by omission, `probe.rs::classify`'s default arm files any
/// refusal carrying neither marker as `Inconclusive`. The PRODUCER discipline
/// differs — see `SEED_WHILE_VERIFYING_REFUSAL`'s doc. Rewording this
/// fragment deliberately reddens `follower.rs`'s tests (`probe.rs`'s test
/// asserts on the full string below, not on this fragment, so it does not
/// key off this const the way `probe.rs::classify` itself does for the other
/// two markers).
pub(crate) const VERIFYING_WITNESSES_MARKER: &str = "still verifying its witnesses";

/// Refusing to seed a brand-new follower while the witness boot gate is
/// unresolved (M2 interlude, spec §1). `decide_hello`'s None-position arm
/// emits this byte-exact when `db.verifying()` is true and the peer is not a
/// recorded witness; `the_seed_while_verifying_refusal_is_pinned` (seed
/// `0x2EB1_0121`) pins it from a real refusal.
/// (`a_verifying_node_refuses_to_seed_a_brand_new_follower` is a different,
/// gate-outcome property test — it asserts on `last_applied`/`load_witnesses`,
/// deliberately not on this string.)
///
/// Deliberately carries NEITHER `NOT_A_PRIMARY_MARKER` nor `DIVERGED_LABEL`.
/// A witness probe reading this as `NotPrimary` would treat "I am also
/// unsure" as "I am satisfied" and open its own gate on no evidence; reading
/// it as `IsPrimary` would refuse a healthy node over a phantom split-brain.
/// `probe.rs::classify`'s default arm gives the correct `Inconclusive` for
/// free — the absence of both markers is what earns it, so it is asserted.
///
/// Unlike `NOT_A_PRIMARY_MARKER`/`DIVERGED_LABEL`, which are `format!`-
/// interpolated into their refusals (`decide_hello` below, and
/// `diverged_ahead`/`diverged_crc`/`diverged_behind`), this string is a
/// literal duplicating `VERIFYING_WITNESSES_MARKER`'s text — a `const`
/// cannot `format!`. `the_refusal_carries_its_marker` (`follower.rs`) is
/// what holds the two in sync; nothing here catches them drifting apart at
/// compile time the way interpolation would.
pub(crate) const SEED_WHILE_VERIFYING_REFUSAL: &str = "cannot seed a new follower yet: this \
     node restarted as a primary and is still verifying its witnesses — it has not confirmed \
     that none of the followers it had shipped to was promoted while it was down, so seeding \
     would copy a timeline it does not yet know it owns. This clears on its own once every \
     witness answers, and an existing follower's catch-up is unaffected meanwhile. If the \
     witnesses are permanently gone, see GUIDE §10 — note that clearing the witness file \
     re-opens the dual-primary gap it exists to close.";

/// The diverged-refusal label shared by `diverged_ahead`/`diverged_crc`/
/// `diverged_behind` below and the witness probe's classifier (`probe.rs`,
/// M2 sub-project 0b task 4) — same rationale as `NOT_A_PRIMARY_MARKER`.
pub(crate) const DIVERGED_LABEL: &str = "diverged";

/// The schema-digest refusal (migrations spec §5): pinned verbatim by tests.
const SCHEMA_DIGEST_MISMATCH: &str = "replication schema mismatch: this node's #[table] schema digest does not match the \
     peer's — one side is running a binary compiled against a different table shape; rebuild \
     both nodes from the same source and restart them (stop both, upgrade both, start the \
     primary first)";

/// The bound on the address echoed by `invalid_listen_refusal` (Minor #7,
/// fix round 1): `MAX_MESSAGE_BYTES` is 17 MiB, so an unbounded echo lets a
/// peer force this refusal — allocated, encoded, sent back, and surfaced to
/// operators — to balloon to megabytes. Not an amplification attack (the
/// peer already sent us those bytes), but waste and log-flooding; every
/// other refusal in the ladder echoes bounded data.
const ECHOED_LISTEN_ADDRESS_BYTES: usize = 200;

/// The invalid-listen-address refusal (M2 sub-project 0b task 2, dispatch
/// Correction 4; length rule folded in by task 1b, Task 3 review M-5):
/// pinned verbatim by tests. `Hello.listen` arrives peer-supplied and
/// unvalidated off the wire; an address `store_witnesses` would reject
/// (empty, over `MAX_WITNESS_ADDRESS_BYTES`, or containing ASCII whitespace)
/// is refused here, before a ship session is ever established — neither
/// propagating the error (a remotely-triggerable node failure, since with no
/// peer secret configured any client reaching the replication port controls
/// `listen`) nor shipping to a peer this node can never durably record as a
/// witness (silently reopening the dual-primary gap the witness set exists
/// to close). `listen: None` stays legal — a peer that advertises no address
/// simply is not a witness candidate. The echoed address is bounded to
/// `ECHOED_LISTEN_ADDRESS_BYTES`, cut on a UTF-8 char boundary (never split
/// mid-character), with a literal "..." marking a truncation — a backstop
/// independent of `MAX_WITNESS_ADDRESS_BYTES`, since the two bounds guard
/// different things (this one bounds what gets echoed back to the wire;
/// that one bounds what gets stored durably and, from Task 5 onward,
/// `join(", ")`-ed into every `TxError::Verifying` 503 body).
fn invalid_listen_refusal(addr: &str) -> String {
    let mut cut = addr.len().min(ECHOED_LISTEN_ADDRESS_BYTES);
    while cut > 0 && !addr.is_char_boundary(cut) {
        cut -= 1;
    }
    let shown = if cut < addr.len() {
        format!("{}...", &addr[..cut])
    } else {
        addr.to_string()
    };
    format!(
        "Hello listen address {shown:?} is invalid — a witness address must be non-empty, at \
         most {MAX_WITNESS_ADDRESS_BYTES} bytes, and must not contain ASCII whitespace (space, \
         tab, CR, or LF); this node cannot durably record it as a witness, so the connection is \
         refused rather than shipped-but-unrecorded"
    )
}

/// True when `presented` proves `peer_secret` (spec §5): `peer_secret: None`
/// accepts everything (and a presented proof it did not ask for is ignored,
/// not an error); `Some` requires `ct_eq_hex(hash_token(presented),
/// hash_token(configured))` — constant-time over the SHA-256 digests, never a
/// raw-string compare. A missing `presented` is treated as an empty string,
/// which fails the compare against any real configured secret.
fn secret_matches(peer_secret: &Option<String>, presented: &Option<String>) -> bool {
    match peer_secret {
        None => true,
        Some(configured) => {
            let presented = presented.as_deref().unwrap_or("");
            ct_eq_hex(&hash_token(presented), &hash_token(configured))
        }
    }
}

/// The `Hello` decision ladder (spec §1), pinned by the error-surface tests:
/// peer secret mismatch → refuse, before any other check (spec §5, auth T7);
/// version mismatch → refuse (both versions); schema digest mismatch →
/// refuse (migrations spec §5, before any role logic — a schema-mismatched
/// peer must never seed or ship frames); an invalid `listen` address →
/// refuse (M2 sub-project 0b task 2 — a `listen` this node could never
/// durably record as a witness must never become a ship session, rather
/// than reopening the dual-primary gap the witness set exists to close);
/// not-a-primary → refuse; follower ahead → refuse (diverged); a positioned
/// follower below the retained base → refuse (diverged behind — the primary
/// GC'd its witness, Design B); position retained → verify the frame CRC
/// (mismatch → diverged, match → resume); empty position → refuse if this
/// node's witness boot gate is unresolved and the peer is not a recorded
/// witness (M2 interlude, spec §1 — a seed must not manufacture a durable
/// replica of a timeline this node has not confirmed it owns); otherwise
/// snapshot when the log has been compacted, else frames from 0.
#[allow(clippy::too_many_arguments)] // two more ladder checks since the base shape (M2 0b task 2's listen-address check, the M2 interlude's witness check), each an independent field
fn decide_hello(
    role: &Role,
    db: &Db,
    proto: u8,
    listen: &Option<String>,
    position: Option<(u64, u32)>,
    peer_secret: &Option<String>,
    presented_secret: &Option<String>,
    schema_digest: u64,
    witnesses: &[String],
) -> io::Result<Decision> {
    if !secret_matches(peer_secret, presented_secret) {
        return Ok(Decision::Refuse(PEER_SECRET_REFUSAL.to_string()));
    }
    if proto != PROTO_VERSION {
        return Ok(Decision::Refuse(format!(
            "replication protocol version mismatch: this node speaks protocol version \
             {PROTO_VERSION}, but the peer's Hello declared version {proto} — upgrade the older \
             binary so both speak the expected version {PROTO_VERSION}"
        )));
    }
    if schema_digest != db.schema_digest() {
        return Ok(Decision::Refuse(SCHEMA_DIGEST_MISMATCH.to_string()));
    }
    if let Some(addr) = listen
        && !valid_witness_address(addr)
    {
        return Ok(Decision::Refuse(invalid_listen_refusal(addr)));
    }
    match role {
        Role::Primary | Role::HandingOff { .. } => {}
        Role::Follower { primary } | Role::Backup { primary } => {
            // Producer site for NOT_A_PRIMARY_MARKER (task 4): rewording this
            // fragment deliberately reddens follower.rs's resolving check and
            // probe.rs's classifier — both key on the const, not this prose.
            return Ok(Decision::Refuse(format!(
                "this node is {NOT_A_PRIMARY_MARKER} (it is a follower of {primary}) — \
                 replication Hellos are served only by the primary, and a follower never feeds \
                 another follower (no cascading); point the follower at {primary} instead"
            )));
        }
    }
    let last = db.last_applied();
    let Some((pos_seq, pos_crc)) = position else {
        // The peer holds NOTHING and is asking for a whole copy. If this
        // node's witness boot gate is unresolved and the peer is not one of
        // the witnesses we recorded, refuse (M2 interlude, spec §1): a seed
        // manufactures a fresh durable replica of a timeline we have not
        // confirmed we own, and it outlives our own fail-stop.
        //
        // Both conjuncts are load-bearing. `position.is_none()` alone is NOT
        // "brand-new node" — `hello_position` returns None for any empty-log
        // node, including a RECORDED WITNESS whose data dir is empty (a
        // fresh-data-dir re-seed, not a discard-and-rebuild — that replays
        // the WAL and leaves `last_applied` intact, so it Hellos positioned),
        // which is GUIDE §10's own prescribed remedy and what resolves this
        // gate. Membership alone is not enough either: a positioned peer
        // absent from the set is a genuine follower on this timeline, and
        // catch-up must still ship to it.
        //
        // Sited here, inside the None-position arm, so it covers BOTH the
        // `Frames { from: 0 }` and `Snapshot` answers below. Keying on the
        // `Decision::Snapshot` variant instead would be silently inert on
        // exactly a young primary that has not GC'd anything yet.
        if db.verifying()
            && !listen
                .as_deref()
                .is_some_and(|a| witnesses.iter().any(|w| w == a))
        {
            return Ok(Decision::Refuse(SEED_WHILE_VERIFYING_REFUSAL.to_string()));
        }
        return Ok(if db.log_base() == 0 {
            Decision::Frames { from: 0 }
        } else {
            Decision::Snapshot
        });
    };
    match last {
        None => Ok(Decision::Refuse(diverged_ahead(pos_seq, pos_crc, None))),
        Some(last) if pos_seq > last => Ok(Decision::Refuse(diverged_ahead(
            pos_seq,
            pos_crc,
            Some(last),
        ))),
        Some(last) => {
            let base = db.log_base();
            if pos_seq < base {
                // Refuse, never re-seed (Design B, dossier
                // .superpowers/sdd/diverged-behind-investigation.md): the
                // primary GC'd the frame at pos_seq, so it cannot prove the
                // follower's records are a prefix of its timeline. If the
                // primary forked BELOW the follower (recovered past an
                // above-horizon cut and kept serving), a re-seed here would
                // silently wipe the follower's converged acked records. A
                // None-position (fresh/log-less) follower still auto-seeds —
                // the arm above this match.
                return Ok(Decision::Refuse(diverged_behind(pos_seq, base)));
            }
            let ours = db
                .read_log(pos_seq, 1)?
                .first()
                .map(|(s, p)| frame_crc(*s, p));
            Ok(match ours {
                Some(crc) if crc == pos_crc => Decision::Frames { from: pos_seq + 1 },
                Some(crc) => Decision::Refuse(diverged_crc(pos_seq, pos_crc, last, crc)),
                // A THIRD `Decision::Snapshot` producer (final review, M2 interlude,
                // 2026-08-06 I-1). The verifying guard above covers only the
                // `None`-position arm's two producers; it deliberately does not extend to
                // this one, since reaching it requires a POSITIONED peer (`pos_seq >=
                // base`), which the guard's own reasoning already treats as on this
                // timeline. Believed unreachable: this arm needs a hole in the
                // log at `pos_seq` with `base <= pos_seq <= last` — `read_from` (log.rs)
                // returns empty only via `Frame::CleanEnd` below `from_seq`, and a hole
                // below `last_applied` is exactly what recovery's cut, the gaplessness
                // triple-guard (sub-project 1), and the `pos_seq < base` arm above
                // (`diverged_behind`) jointly exclude. Not proven, only argued — the
                // review could not construct a reachable case either. Extending the
                // verifying guard to this arm (refuse, or assert unreachable) is a
                // policy change outside spec §1's scope (a POSITIONED peer, not a
                // newcomer) and needs a ruling, not a close-out edit.
                None => Decision::Snapshot,
            })
        }
    }
}

// Producer site for DIVERGED_LABEL (task 4): all three functions below
// deliberately lead with `{DIVERGED_LABEL}` rather than the literal word —
// rewording it reddens the witness probe's classifier (`probe.rs`).

fn diverged_ahead(pos_seq: u64, pos_crc: u32, last: Option<u64>) -> String {
    let ours = match last {
        Some(l) => format!("seq {l}"),
        None => "an empty log (no records at all)".to_string(),
    };
    format!(
        "{DIVERGED_LABEL}: the follower is ahead at seq {pos_seq} (crc {pos_crc:#010x}) but this \
         primary's last durable position is {ours} (crc n/a) — the follower holds records this \
         primary never had (or lost), a forked timeline that can never be merged; re-seed the \
         follower from a fresh data dir ({})",
        reseed_steps("this primary")
    )
}

fn diverged_crc(pos_seq: u64, pos_crc: u32, last: u64, ours: u32) -> String {
    format!(
        "{DIVERGED_LABEL}: the follower's frame at seq {pos_seq} has crc {pos_crc:#010x} but \
         this primary's frame at the same seq has crc {ours:#010x} (this primary is at seq \
         {last}) — the logs share a seq but not its bytes, a forked timeline that can never be \
         merged; re-seed the follower from a fresh data dir ({})",
        reseed_steps("this primary")
    )
}

/// A positioned follower whose proven seq sits below the primary's retained
/// base (Design B, dossier .superpowers/sdd/diverged-behind-investigation.md):
/// the primary GC'd that frame, so it cannot prove the follower's records are a
/// prefix of its timeline — a re-seed would silently overwrite the follower,
/// which is data-loss when the primary forked below it. Names both remedies:
/// re-seed a true laggard, or promote the follower if the primary forked behind.
fn diverged_behind(pos_seq: u64, base: u64) -> String {
    format!(
        "{DIVERGED_LABEL}: the follower holds a proven record at seq {pos_seq}, but this \
         primary has GC'd every record below its retained base {base} — with no frame at or \
         below seq {pos_seq} to compare, this primary cannot prove the follower's records are a \
         prefix of its own timeline, and re-seeding here would silently overwrite whatever the \
         follower holds; if the follower merely lagged past the retained window, re-seed it \
         deliberately from a fresh data dir ({}); if instead this primary forked below the \
         follower (recovered past an above-horizon corruption cut and kept serving), promote \
         the follower and re-seed this node from it the same way, configured as a follower of \
         the promoted node",
        reseed_steps("this primary")
    )
}
