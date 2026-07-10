//! The follower-side replication client (spec docs/superpowers/specs/
//! 2026-07-08-replication-design.md §1): one outbound connection to the
//! primary. It `Hello`s with its durable position, then appends shipped
//! `Frames` to its own local WAL through `Db::apply_shipped` (append → sync →
//! apply, the same discipline recovery uses) and `Ack`s only after that sync
//! — never on receipt. A `SnapshotStart` opens a re-seed: the follower buffers
//! the stream (one message per step, interleaving with the primary's sends)
//! and, on `SnapshotEnd`, the caller runs `run_seed` (mod.rs) to rebuild and
//! swap the `Db`. A dropped conn is reconnected on a backoff; a `Refuse`
//! surfaces as the contract error that stops the node.

use std::collections::VecDeque;
use std::io;

use charpente_core::io::{Conn, Net};
use charpente_core::log::frame_crc;

use crate::Db;
use crate::repl::protocol::{Msg, decode, encode};
use crate::repl::{MAX_MESSAGE_BYTES, PROTO_VERSION, RECONNECT_NANOS};

/// A latched `SnapshotStart`: `run_seed` finishes the re-seed against these.
pub(crate) struct SeedStart {
    pub applied_seq: u64,
    pub next_job_id: u64,
}

/// The buffered snapshot stream: one `(table, rows)` entry per
/// `SnapshotRows` message, in arrival (= schema walk) order.
pub(crate) type SeedRows = Vec<(String, Vec<(u64, Vec<u8>)>)>;

/// The follower's replication client.
pub(crate) struct Follower {
    /// The primary's replication address.
    primary: String,
    /// This node's own listen address, sent in `Hello` so a later switchover
    /// can target this follower by name (carried, unused until Task 7).
    my_listen: Option<String>,
    conn: Option<Box<dyn Conn>>,
    recv_buf: Option<Vec<u8>>,
    inbox: VecDeque<Vec<u8>>,
    /// When the next reconnect may be attempted (after a drop/backoff).
    reconnect_at: Option<u64>,
    /// The seed in progress: `Some` once `SnapshotStart` opens one. Cleared and
    /// re-opened by a fresh `SnapshotStart` (a mid-seed reconnect restarts it).
    seed: Option<SeedInProgress>,
}

struct SeedInProgress {
    start: SeedStart,
    /// Buffered `SnapshotRows` messages, filled one per step until `SnapshotEnd`.
    rows: SeedRows,
    /// `SnapshotEnd` seen — the caller must now run `run_seed`.
    complete: bool,
}

impl Follower {
    pub(crate) fn new(primary: String, my_listen: Option<String>) -> Follower {
        Follower {
            primary,
            my_listen,
            conn: None,
            recv_buf: None,
            inbox: VecDeque::new(),
            reconnect_at: None,
            seed: None,
        }
    }

    /// True once `SnapshotEnd` has landed: the caller must run `run_seed`.
    pub(crate) fn needs_reseed(&self) -> bool {
        self.seed.as_ref().is_some_and(|s| s.complete)
    }

    /// Non-destructive IO movement (pump_io): drain readable messages into the
    /// inbox; a recv error or EOF drops the conn and schedules a reconnect.
    pub(crate) fn drain(&mut self, now: u64) {
        if self.conn.is_none() {
            return;
        }
        let Follower {
            conn,
            recv_buf,
            inbox,
            ..
        } = self;
        let buf = recv_buf.get_or_insert_with(|| vec![0u8; MAX_MESSAGE_BYTES]);
        let c = conn.as_ref().expect("checked non-none above");
        loop {
            match c.recv(buf) {
                Ok(Some(0)) | Err(_) => {
                    self.conn = None;
                    self.reconnect_at = Some(now + RECONNECT_NANOS);
                    return;
                }
                Ok(Some(n)) => inbox.push_back(buf[..n].to_vec()),
                Ok(None) => return,
            }
        }
    }

    pub(crate) fn has_work(&self, now: u64) -> bool {
        if self.needs_reseed() {
            return false; // run_seed handles it
        }
        if self.conn.is_none() {
            return self.reconnect_at.is_none_or(|t| now >= t);
        }
        !self.inbox.is_empty()
    }

    pub(crate) fn next_deadline(&self, _now: u64) -> Option<u64> {
        if self.conn.is_none() {
            self.reconnect_at
        } else {
            None
        }
    }

    /// One unit of work: connect + `Hello` when disconnected and due, or
    /// handle one inbox message.
    pub(crate) fn step(&mut self, net: &dyn Net, db: &mut Db, now: u64) -> io::Result<()> {
        if self.conn.is_none() {
            if self.reconnect_at.is_some_and(|t| now < t) {
                return Ok(()); // backoff not yet due — has_work gates this,
                // but a caller stepping unconditionally must not spin connects
            }
            return self.connect_and_hello(net, db, now);
        }
        let Some(raw) = self.inbox.pop_front() else {
            return Ok(());
        };
        match decode(&raw)? {
            Msg::Frames { frames } => self.apply_and_ack(db, &frames, now),
            Msg::SnapshotStart {
                applied_seq,
                next_job_id,
            } => {
                // Opens (or restarts, after a mid-seed reconnect) a seed. We
                // buffer the stream rather than blocking so the single-writer
                // primary can keep shipping it one batch per step.
                self.seed = Some(SeedInProgress {
                    start: SeedStart {
                        applied_seq,
                        next_job_id,
                    },
                    rows: Vec::new(),
                    complete: false,
                });
                Ok(())
            }
            Msg::SnapshotRows { table, rows } => {
                match self.seed.as_mut() {
                    Some(seed) if !seed.complete => seed.rows.push((table, rows)),
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "SnapshotRows arrived with no open seed — protocol drift",
                        ));
                    }
                }
                Ok(())
            }
            Msg::SnapshotEnd => {
                match self.seed.as_mut() {
                    Some(seed) if !seed.complete => seed.complete = true,
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "SnapshotEnd arrived with no open seed — protocol drift",
                        ));
                    }
                }
                Ok(())
            }
            Msg::Heartbeat { .. } => Ok(()), // liveness only in v1
            Msg::Refuse { message } => Err(io::Error::other(message)),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "the follower's ship session received a message ({}) it never asks for — \
                     protocol drift",
                    other.name()
                ),
            )),
        }
    }

    fn connect_and_hello(&mut self, net: &dyn Net, db: &Db, now: u64) -> io::Result<()> {
        let conn = match net.connect(&self.primary) {
            Ok(conn) => conn,
            // Partition / primary not yet listening: back off and retry. Not a
            // node fault — nothing to recover, just wait.
            Err(_) => {
                self.reconnect_at = Some(now + RECONNECT_NANOS);
                return Ok(());
            }
        };
        let position = hello_position(db)?; // a read fault here retries the Hello
        let hello = Msg::Hello {
            proto: PROTO_VERSION,
            listen: self.my_listen.clone(),
            position,
        };
        if conn.send(&encode(&hello)).is_err() {
            self.reconnect_at = Some(now + RECONNECT_NANOS);
            return Ok(());
        }
        self.conn = Some(conn);
        self.reconnect_at = None;
        // A reconnect abandons any half-collected seed; the primary answers the
        // fresh Hello with a fresh SnapshotStart (idempotent — Seeder::new
        // discards).
        self.seed = None;
        Ok(())
    }

    fn apply_and_ack(
        &mut self,
        db: &mut Db,
        frames: &[(u64, Vec<u8>)],
        now: u64,
    ) -> io::Result<()> {
        match db.apply_shipped(frames) {
            Ok(()) => {
                let ack = Msg::Ack {
                    durable_seq: db
                        .last_applied()
                        .expect("apply_shipped just advanced the log"),
                };
                self.send_or_drop(&ack, now);
                Ok(())
            }
            // Engine::submit_batch names a seq gap ("reconnect and re-Hello"):
            // the ship stream is out of order or from a different timeline —
            // drop and re-Hello, letting the primary resume from our real
            // position. Any other error is a Db fault/poison the node recovers.
            Err(e) if e.to_string().contains("re-Hello") => {
                self.disconnect(now);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    // ---- seeding support (driven by mod.rs `run_seed`) -------------------

    /// Takes the completed seed for `run_seed` to apply. Returns the start meta
    /// and the buffered rows; leaves the follower ready to resume shipping.
    pub(crate) fn take_completed_seed(&mut self) -> Option<(SeedStart, SeedRows)> {
        let seed = self.seed.take()?;
        if !seed.complete {
            // Not actually complete — put it back (defensive; needs_reseed
            // gates this, so it should not happen).
            self.seed = Some(seed);
            return None;
        }
        Some((seed.start, seed.rows))
    }

    /// Acks the seed position (best effort) so the primary's retention pin
    /// advances and it ships from `applied_seq + 1`; a lost ack just means the
    /// next reconnect re-Hellos from the same position.
    pub(crate) fn ack_seed(&mut self, applied_seq: u64, now: u64) {
        self.send_or_drop(
            &Msg::Ack {
                durable_seq: applied_seq,
            },
            now,
        );
    }

    fn send_or_drop(&mut self, msg: &Msg, now: u64) {
        let dead = self
            .conn
            .as_ref()
            .is_none_or(|c| c.send(&encode(msg)).is_err());
        if dead {
            self.disconnect(now);
        }
    }

    fn disconnect(&mut self, now: u64) {
        self.conn = None;
        self.reconnect_at = Some(now + RECONNECT_NANOS);
    }
}

/// The `Hello` position: `(last durable seq, that frame's CRC)`, or `None`
/// when the follower is empty OR its last frame is below its own retained base
/// (freshly seeded, nothing shipped yet — an empty position forces a re-seed,
/// idempotent because `Seeder::new` discards).
fn hello_position(db: &Db) -> io::Result<Option<(u64, u32)>> {
    let Some(last) = db.last_applied() else {
        return Ok(None);
    };
    match db.read_log(last, 1) {
        Ok(frames) => Ok(frames.first().map(|(s, p)| (*s, frame_crc(*s, p)))),
        // read_from's below-base refusal is the InvalidData kind; a transient
        // read fault is io::Error::other — propagate that to retry the Hello.
        Err(e) if e.kind() == io::ErrorKind::InvalidData => Ok(None),
        Err(e) => Err(e),
    }
}
