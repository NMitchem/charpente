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
    pub next_event_id: u64,
    pub frontier_crc: u32,
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
    /// This node's configured peer secret (spec §5, auth T7), attached to
    /// every outbound `Hello` as this side's proof; `None` when unconfigured.
    peer_secret: Option<String>,
    conn: Option<Box<dyn Conn>>,
    recv_buf: Option<Vec<u8>>,
    inbox: VecDeque<Vec<u8>>,
    /// When the next reconnect may be attempted (after a drop/backoff).
    reconnect_at: Option<u64>,
    /// The seed in progress: `Some` once `SnapshotStart` opens one. Cleared and
    /// re-opened by a fresh `SnapshotStart` (a mid-seed reconnect restarts it).
    seed: Option<SeedInProgress>,
    /// A `HandOff { at_seq }` received from the primary during a switchover:
    /// `Repl` consumes it in `step` (verifies position, flips this node to
    /// primary, sends `HandOffDone`). Task 7.
    handoff: Option<u64>,
    /// Startup resolution of a durable `HandingOff { to }` role (Task 7, spec
    /// §2): this client probes `to`; a ship response confirms `to` is primary
    /// (this node demotes to its follower), a `not-a-primary` refusal means
    /// `to` has not been promoted yet (retry), any other refusal is a diverged
    /// timeline (the operator's re-seed signal, surfaced as the step error).
    resolving: bool,
    /// Set when a probe confirmed `to` is primary; `Repl` reads it to perform
    /// the durable demote, then clears both flags via `took_resolution`.
    resolved: bool,
}

struct SeedInProgress {
    start: SeedStart,
    /// Buffered `SnapshotRows` messages, filled one per step until `SnapshotEnd`.
    rows: SeedRows,
    /// `SnapshotEnd` seen — the caller must now run `run_seed`.
    complete: bool,
}

impl Follower {
    pub(crate) fn new(
        primary: String,
        my_listen: Option<String>,
        peer_secret: Option<String>,
    ) -> Follower {
        Follower {
            primary,
            my_listen,
            peer_secret,
            conn: None,
            recv_buf: None,
            inbox: VecDeque::new(),
            reconnect_at: None,
            seed: None,
            handoff: None,
            resolving: false,
            resolved: false,
        }
    }

    /// A client that probes `to` to resolve a durable `HandingOff { to }` role
    /// at startup (spec §2). Otherwise identical to `new`.
    pub(crate) fn new_resolving(
        to: String,
        my_listen: Option<String>,
        peer_secret: Option<String>,
    ) -> Follower {
        let mut f = Follower::new(to, my_listen, peer_secret);
        f.resolving = true;
        f
    }

    /// True once `SnapshotEnd` has landed: the caller must run `run_seed`.
    pub(crate) fn needs_reseed(&self) -> bool {
        self.seed.as_ref().is_some_and(|s| s.complete)
    }

    /// A received `HandOff` awaiting `Repl` (which owns the durable role flip).
    pub(crate) fn handoff_pending(&self) -> bool {
        self.handoff.is_some()
    }

    /// Takes the received `HandOff`'s `at_seq` for `Repl` to promote against.
    pub(crate) fn take_handoff(&mut self) -> Option<u64> {
        self.handoff.take()
    }

    /// True once a `HandingOff` probe confirmed `to` is primary; consumes the
    /// signal and leaves this a plain follower client of `to`.
    pub(crate) fn took_resolution(&mut self) -> bool {
        if self.resolved {
            self.resolved = false;
            self.resolving = false;
            true
        } else {
            false
        }
    }

    /// Best-effort `Refuse` on the current conn (a named protocol rejection the
    /// primary logs verbatim); the conn is torn down by the caller afterward.
    pub(crate) fn refuse(&self, message: String) {
        if let Some(c) = &self.conn {
            let _ = c.send(&encode(&Msg::Refuse { message }));
        }
    }

    /// Best-effort `HandOffDone` on the current conn — the promoted follower's
    /// last word to the draining primary before the conn is dropped.
    pub(crate) fn send_handoff_done(&self) {
        if let Some(c) = &self.conn {
            let _ = c.send(&encode(&Msg::HandOffDone));
        }
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
        if self.handoff.is_some() {
            return true; // Repl must accept the hand-off
        }
        if self.needs_reseed() {
            return false; // run_seed handles it
        }
        // Buffered messages are processed before any reconnect (a HandOff or
        // trailing frames received right before the conn dropped must not be
        // stranded).
        if !self.inbox.is_empty() {
            return true;
        }
        if self.conn.is_none() {
            return self.reconnect_at.is_none_or(|t| now >= t);
        }
        false
    }

    pub(crate) fn next_deadline(&self, _now: u64) -> Option<u64> {
        if self.conn.is_none() {
            self.reconnect_at
        } else {
            None
        }
    }

    /// One unit of work: handle one buffered message, else connect + `Hello`
    /// when disconnected and due. Buffered messages come first so a `HandOff`
    /// (or trailing frames) received in the same drain as the conn's EOF is
    /// processed rather than lost to the reconnect (the conn only drops once
    /// the inbox is empty, so a re-Hello never races stale buffered frames).
    pub(crate) fn step(&mut self, net: &dyn Net, db: &mut Db, now: u64) -> io::Result<()> {
        let Some(raw) = self.inbox.pop_front() else {
            if self.conn.is_none() && self.reconnect_at.is_none_or(|t| now >= t) {
                return self.connect_and_hello(net, db, now);
            }
            return Ok(()); // backoff not yet due, or connected and idle
        };
        let msg = decode(&raw)?;
        // Startup HandingOff resolution (spec §2): the first response from the
        // probed `to` decides whether this node demotes.
        if self.resolving {
            match &msg {
                // `to` isn't primary yet — nothing to resolve against; back off
                // and retry (never a hard error, never a silent resume).
                Msg::Refuse { message } if message.contains("not a primary") => {
                    self.disconnect(now);
                    return Ok(());
                }
                // A diverged/other refusal is the operator's re-seed signal.
                Msg::Refuse { message } => return Err(io::Error::other(message.clone())),
                // Any ship message proves `to` is a primary: Repl demotes this
                // node to Follower(to) after this step (the message applies
                // normally below — apply_shipped is idempotent under crash, so
                // applying before the durable flip is safe).
                _ => self.resolved = true,
            }
        }
        match msg {
            Msg::Frames { frames } => self.apply_and_ack(db, &frames, now),
            Msg::SnapshotStart {
                applied_seq,
                next_job_id,
                next_event_id,
                frontier_crc,
            } => {
                // Opens (or restarts, after a mid-seed reconnect) a seed. We
                // buffer the stream rather than blocking so the single-writer
                // primary can keep shipping it one batch per step.
                self.seed = Some(SeedInProgress {
                    start: SeedStart {
                        applied_seq,
                        next_job_id,
                        next_event_id,
                        frontier_crc,
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
            // Switchover hand-off: the drain reached `at_seq`. Latch it for
            // Repl, which owns the durable Primary flip and the HandOffDone.
            Msg::HandOff { at_seq } => {
                self.handoff = Some(at_seq);
                Ok(())
            }
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
            secret: self.peer_secret.clone(),
            schema_digest: db.schema_digest(),
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
        // apply_shipped is a documented no-op on an empty batch — it never
        // advances last_applied, so there is nothing to ack. The real shipper
        // (Session::ship_frames) never sends an empty Frames (it only sends
        // when read_log returns something), but nothing on the wire enforces
        // that; a fresh follower with last_applied == None must not treat one
        // as advancing the log (that unwrap-shaped assumption is exactly what
        // used to panic the kernel thread here).
        if frames.is_empty() {
            return Ok(());
        }
        match db.apply_shipped(frames) {
            Ok(()) => {
                let ack = Msg::Ack {
                    durable_seq: db
                        .last_applied()
                        .expect("apply_shipped just advanced the log on a non-empty batch"),
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
/// when this node cannot prove one.
///
/// The ladder (spec §5, finding-1 fix): (a) our retained log holds the frame
/// at `last_applied` — the ordinary path, CRC recomputed from the frame; (b)
/// that frame is below our own base because we were freshly seeded and no
/// frame has shipped yet (the seed lays an empty segment at `applied_seq +
/// 1`, so `last_applied` predates our WAL) — fall back to the seed frontier
/// CRC the primary handed us (`Db::seed_frontier`), proving the position so
/// the ladder can run its diverged/resume checks instead of a `None` that
/// silently re-seeds us from a possibly-lossy primary; (c) `None` — either a
/// genuinely empty store, or a node that truly cannot prove its position:
/// its log vanished/was cut back to a durable horizon past any stored seed
/// frontier, or `read_log` refused a corrupt frame below the acked end
/// (sim-found while landing the fix, frozen-batch seed 0x820d5e907fbb8b55:
/// bitrot below `last_applied` on a follower). For a follower, the re-seed
/// an empty position draws IS that corruption error's named fix — restore
/// this node from a replica. Once a real frame lands, path (a) resumes.
fn hello_position(db: &Db) -> io::Result<Option<(u64, u32)>> {
    let Some(last) = db.last_applied() else {
        return Ok(None); // genuinely empty — nothing applied, nothing seeded
    };
    match db.read_log(last, 1) {
        // (a) the frame is retained: recompute its CRC as always.
        Ok(frames) => {
            let (s, p) = frames.first().expect(
                "read_log at last_applied returns that frame whenever last_applied is Some",
            );
            Ok(Some((*s, frame_crc(*s, p))))
        }
        // read_log's refusals are the InvalidData kind; a transient read
        // fault is io::Error::other — propagate that to retry the Hello.
        Err(e) if e.kind() == io::ErrorKind::InvalidData => {
            if last < db.log_base() {
                // The below-base refusal: our whole log sits above `last`, so
                // the refusal is positional, never a corrupt read. Negative
                // space: gc keeps `base <= applied + 1` and `Db::open`
                // rejects any vanished prefix beyond horizon + 1, so below-
                // base can only mean base == last + 1 exactly.
                assert_eq!(
                    db.log_base(),
                    last + 1,
                    "log base {} is above last_applied {last} + 1 — a vanished prefix \
                     Db::open must have refused",
                    db.log_base()
                );
                // (b) the seed frontier, only when it vouches for `last`
                // itself: the stored CRC belongs to the seed's seq alone; if
                // applies advanced past the seed before the log went missing,
                // the frontier proves nothing about `last` (an empty position
                // and its re-seed are the honest answer).
                if let Some((seed_seq, crc)) = db.seed_frontier()?
                    && seed_seq == last
                {
                    return Ok(Some((last, crc)));
                }
            }
            Ok(None) // (c) cannot prove a position — the primary re-seeds us
        }
        Err(e) => Err(e),
    }
}
