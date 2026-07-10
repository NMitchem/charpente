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
use crate::repl::protocol::{Msg, decode, encode};
use crate::repl::role::Role;
use crate::repl::{
    FRAMES_BATCH_BYTES, HEARTBEAT_NANOS, MAX_MESSAGE_BYTES, PROTO_VERSION, SNAPSHOT_BATCH_BYTES,
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

/// One primary-side replication session.
pub(crate) struct Session {
    conn: Box<dyn Conn>,
    /// The follower's own replication address, learned from its `Hello`
    /// (switchover targets sessions by it — Task 7).
    pub listen: Option<String>,
    /// The next seq to ship to this follower.
    pub cursor: u64,
    /// The highest seq the follower has acknowledged durable, if any.
    pub acked: Option<u64>,
    phase: Phase,
    /// Lazily allocated once, reused: sized to the largest message the wire
    /// can carry (spec §1's "one buffer per conn").
    recv_buf: Option<Vec<u8>>,
    inbox: VecDeque<Vec<u8>>,
    /// Virtual-time nanos of the last send — the heartbeat cadence clock.
    last_sent: u64,
    /// The peer's wire died (send/recv error or EOF): reaped by mod.rs, which
    /// then recomputes the pin.
    pub dead: bool,
}

impl Session {
    pub(crate) fn new(conn: Box<dyn Conn>) -> Session {
        Session {
            conn,
            listen: None,
            cursor: 0,
            acked: None,
            phase: Phase::AwaitHello,
            recv_buf: None,
            inbox: VecDeque::new(),
            last_sent: 0,
            dead: false,
        }
    }

    /// The retention floor this session pins: `acked + 1` once acking, else
    /// its `cursor` (0 while awaiting a Hello — conservative; the snapshot
    /// position + 1 while seeding; the resume point while shipping). mod.rs
    /// mins this over live sessions (spec §1's retention pin).
    pub(crate) fn pin_floor(&self) -> u64 {
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
        if !self.inbox.is_empty() {
            return true;
        }
        match self.phase {
            Phase::Seeding { .. } => true,
            Phase::Shipping => {
                let backlog = db.last_applied().is_some_and(|last| self.cursor <= last);
                backlog || now >= self.last_sent + HEARTBEAT_NANOS
            }
            Phase::AwaitHello => false,
        }
    }

    /// The next future heartbeat deadline, if this session is a caught-up
    /// shipper waiting to send liveness (the scheduler advances virtual time
    /// to the earliest such deadline when nothing else is runnable).
    pub(crate) fn heartbeat_deadline(&self) -> Option<u64> {
        matches!(self.phase, Phase::Shipping).then(|| self.last_sent + HEARTBEAT_NANOS)
    }

    /// One unit of work: handle one inbox message, or push one
    /// snapshot/frames/heartbeat message. `role` decides `Hello` handling — a
    /// primary serves the ladder, a follower/backup refuses (no cascading).
    pub(crate) fn service(&mut self, db: &Db, now: u64, role: &Role) -> io::Result<()> {
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
            return self.handle(msg, db, now, role);
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
            Phase::AwaitHello => Ok(()),
        }
    }

    fn handle(&mut self, msg: Msg, db: &Db, now: u64, role: &Role) -> io::Result<()> {
        match msg {
            Msg::Hello {
                proto,
                listen,
                position,
            } => {
                self.listen = listen;
                match decide_hello(role, db, proto, position)? {
                    Decision::Refuse(message) => {
                        self.send(&Msg::Refuse { message }, now);
                        self.dead = true;
                    }
                    Decision::Frames { from } => {
                        self.cursor = from;
                        self.last_sent = now;
                        self.phase = Phase::Shipping;
                    }
                    Decision::Snapshot => self.begin_seeding(db, now)?,
                }
                Ok(())
            }
            Msg::Ack { durable_seq } => {
                self.acked = Some(durable_seq);
                Ok(())
            }
            Msg::CtrlPromote
            | Msg::CtrlSwitchover { .. }
            | Msg::HandOff { .. }
            | Msg::HandOffDone => {
                // The control plane is Task 7; refuse politely by name until
                // then (no todo!/unimplemented! — house rule).
                self.send(
                    &Msg::CtrlErr {
                        message: "replication control verbs (promote/switchover/hand-off) are \
                                  not implemented until replication task 7"
                            .to_string(),
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
        let mut pending = VecDeque::new();
        pending.push_back(Msg::SnapshotStart {
            applied_seq,
            next_job_id,
        });
        for table in db.table_names() {
            let rows = snap.scan_raw(table)?;
            let mut batch: Vec<(u64, Vec<u8>)> = Vec::new();
            let mut bytes = 0usize;
            for (id, row) in rows {
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
            // One message per table, even when empty: the follower's importer
            // walks the same table list and a no-op keeps the two sides'
            // message counts in lockstep with the schema order.
            pending.push_back(Msg::SnapshotRows {
                table: table.to_string(),
                rows: batch,
            });
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
}

enum Decision {
    Refuse(String),
    Frames { from: u64 },
    Snapshot,
}

/// The `Hello` decision ladder (spec §1), pinned by the error-surface tests:
/// version mismatch → refuse (both versions); not-a-primary → refuse; follower
/// ahead → refuse (diverged); position below the retained base → snapshot;
/// position retained → verify the frame CRC (mismatch → diverged, match →
/// resume); empty position → snapshot when the log has been compacted, else
/// frames from 0.
fn decide_hello(
    role: &Role,
    db: &Db,
    proto: u8,
    position: Option<(u64, u32)>,
) -> io::Result<Decision> {
    if proto != PROTO_VERSION {
        return Ok(Decision::Refuse(format!(
            "replication protocol version mismatch: this node speaks protocol version \
             {PROTO_VERSION}, but the peer's Hello declared version {proto} — upgrade the older \
             binary so both speak the expected version {PROTO_VERSION}"
        )));
    }
    match role {
        Role::Primary | Role::HandingOff { .. } => {}
        Role::Follower { primary } | Role::Backup { primary } => {
            return Ok(Decision::Refuse(format!(
                "this node is not a primary (it is a follower of {primary}) — replication Hellos \
                 are served only by the primary, and a follower never feeds another follower \
                 (no cascading); point the follower at {primary} instead"
            )));
        }
    }
    let last = db.last_applied();
    let Some((pos_seq, pos_crc)) = position else {
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
            if pos_seq < db.log_base() {
                return Ok(Decision::Snapshot);
            }
            let ours = db
                .read_log(pos_seq, 1)?
                .first()
                .map(|(s, p)| frame_crc(*s, p));
            Ok(match ours {
                Some(crc) if crc == pos_crc => Decision::Frames { from: pos_seq + 1 },
                Some(crc) => Decision::Refuse(diverged_crc(pos_seq, pos_crc, last, crc)),
                None => Decision::Snapshot,
            })
        }
    }
}

fn diverged_ahead(pos_seq: u64, pos_crc: u32, last: Option<u64>) -> String {
    let ours = match last {
        Some(l) => format!("seq {l}"),
        None => "an empty log (no records at all)".to_string(),
    };
    format!(
        "diverged: the follower is ahead at seq {pos_seq} (crc {pos_crc:#010x}) but this \
         primary's last durable position is {ours} (crc n/a) — the follower holds records this \
         primary never had (or lost), a forked timeline that can never be merged; re-seed the \
         follower from a fresh data dir (stop it, delete its data dir, restart to snapshot-seed \
         from this primary)"
    )
}

fn diverged_crc(pos_seq: u64, pos_crc: u32, last: u64, ours: u32) -> String {
    format!(
        "diverged: the follower's frame at seq {pos_seq} has crc {pos_crc:#010x} but this \
         primary's frame at the same seq has crc {ours:#010x} (this primary is at seq {last}) — \
         the logs share a seq but not its bytes, a forked timeline that can never be merged; \
         re-seed the follower from a fresh data dir (stop it, delete its data dir, restart to \
         snapshot-seed from this primary)"
    )
}
