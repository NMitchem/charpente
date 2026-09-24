//! The event-sink seam (realtime T4; DESIGN §6, spec docs/superpowers/specs/
//! 2026-07-14-realtime-design.md §3) plus the Tail registry itself
//! (realtime T5): registration's delivery half — the bounded unit that
//! reads new events off the store and pushes matching frames into a
//! subscriber's sink, floor-checked first so a tail that fell behind
//! retention gets one terminal frame instead of silently replaying gone
//! history. `Kernel` (kernel/mod.rs) owns registration (validation, the
//! ready-set gate, round-robin scheduling); this file owns what happens
//! once a tail is scheduled.

use std::io;
use std::sync::{Arc, Mutex};

use charpente_core::db::Db;
use charpente_core::event::EventRow;
use charpente_core::table::View;

use crate::http::Subscription;

/// Rows read (and filtered) per `deliver_unit` call — the bound that keeps
/// one Tail service a unit of work the kernel's same-shape rule can account
/// for, the same role `TAIL_BATCH_EVENTS`'s peers (job/cron ticks) play.
pub const TAIL_BATCH_EVENTS: usize = 64;

/// The terminal frame a tail gets instead of a decode when a stored event's
/// recorded shape is not the subscribed type's (payload-shapes spec §3). It
/// carries the foreign row's own `id.seq`, so a client that ignores it and
/// reconnects with that cursor meets `validate_subscribe`'s named 409 rather
/// than resuming past an event it never received.
pub const RESHAPED_FRAME: &str = "charpente.reshaped";

/// What a sink reports back from one `send` attempt.
pub enum SinkStatus {
    /// Delivered (buffered, in the prod channel's case).
    Ok,
    /// The sink has no room right now — kernel policy (T5) is to drop the
    /// tail: a slow consumer costs its own connection, never kernel
    /// progress.
    Full,
    /// The consumer is gone.
    Closed,
}

/// One event, wire-shaped for delivery: `id`/`seq` are the resume cursor's
/// two halves (DESIGN §6 — the seq half is what turns a cross-timeline
/// replay into a named divergence error instead of silent misattribution),
/// `name` is the subscribed event's `Event::NAME`, `json` is the already-
/// encoded payload (`SubEntry::encode_json`, `crate::http::Subscription`).
/// Clone is load-bearing: the prod sink's `try_send` takes an owned frame.
#[derive(Clone, Debug)]
pub struct EventFrame {
    pub id: u64,
    pub seq: u64,
    pub name: &'static str,
    pub json: String,
}

/// Driver-supplied delivery target for one subscription's frames. `&mut
/// self` (not spec §3's sketched `&self`) because every real sink — the
/// prod bounded channel, the sim recording buffer — needs a mutable handle
/// to push into; the brief's exact shape governs over the earlier sketch.
pub trait EventSink: Send {
    fn send(&mut self, frame: &EventFrame) -> SinkStatus;
}

/// One live subscription: what it wants (`sub`), where its frames go
/// (`sink`), and how far it has been delivered (`next_id` — the next event
/// id it has not yet seen; the Ready::Tail gate is `next_id <
/// next_event_id`).
pub struct Tail {
    pub sub: Subscription,
    pub sink: Box<dyn EventSink>,
    pub next_id: u64,
}

/// One bounded unit: read up to `TAIL_BATCH_EVENTS` rows from `next_id`,
/// filter, push. Returns whether the tail survives. The floor check runs
/// FIRST: a tail that fell behind retention gets the terminal gap frame and
/// dies — deterministic, part of the unit, independent of whatever the sink
/// reports for that final send (the tail is dying either way, so the send's
/// own `SinkStatus` is not acted on). A row whose recorded shape is not the
/// matching entry's is the other terminal case: the tail gets the terminal
/// `charpente.reshaped` frame instead of a decode, and dies the same way
/// (payload-shapes spec §3).
pub fn deliver_unit(tail: &mut Tail, db: &Db) -> io::Result<bool> {
    let floor = db.event_floor()?;
    if tail.next_id < floor {
        let gap = EventFrame {
            id: tail.next_id,
            seq: 0,
            name: "charpente.gap",
            json: format!(
                "{{\"floor\":{floor},\"tail\":{},\"fix\":\"re-sync current state through reads, then reconnect without a cursor\"}}",
                tail.next_id
            ),
        };
        let _ = tail.sink.send(&gap);
        return Ok(false);
    }
    let rows: Vec<EventRow> = db.snapshot()?.scan_from(tail.next_id, TAIL_BATCH_EVENTS)?;
    for row in rows {
        tail.next_id = row.id.raw() + 1;
        if tail.sub.key.is_some() && tail.sub.key.as_deref() != row.key.as_deref() {
            continue;
        }
        let Some(entry) = tail.sub.entries.iter().find(|e| e.name == row.name) else {
            continue;
        };
        if row.shape != Some(entry.fingerprint) {
            // Never decoded: this binary cannot prove those bytes are its own
            // shape. The tail ends here, the node does not, and every other
            // tail is untouched — the `charpente.gap` lifecycle.
            let _ = tail.sink.send(&EventFrame {
                id: row.id.raw(),
                seq: row.seq,
                name: RESHAPED_FRAME,
                json: format!(
                    "{{\"event\":\"{}\",\"id\":{},\"fix\":\"re-sync current state through reads, then reconnect without a cursor\"}}",
                    entry.name,
                    row.id.raw()
                ),
            });
            return Ok(false);
        }
        // An encode error here is deliberately whole-node-fatal (bubbles out
        // of Kernel::run) — a tail that cannot render its frames is a build
        // bug, not a client condition; the gentler per-tail-drop shape is a
        // ledgered follow-up, and THIS test is its pin:
        // realtime.rs::tail_encode_error_is_whole_node_fatal.
        let frame = EventFrame {
            id: row.id.raw(),
            seq: row.seq,
            name: entry.name,
            json: (entry.encode_json)(&row.payload)?,
        };
        match tail.sink.send(&frame) {
            SinkStatus::Ok => {}
            SinkStatus::Full | SinkStatus::Closed => return Ok(false),
        }
    }
    Ok(true)
}

/// The sim's real `EventSink`: buffers accepted frames, driven by a
/// workload-supplied `stall` closure that decides whether each `send`
/// succeeds or reports `Full` — the sim's stand-in for a slow or bounded
/// real consumer. `Arc<Mutex>` inside is the sim world's honest-uncontended
/// pattern (`SimSinkHandle` is the read side a workload keeps after the
/// sink itself is boxed into a `Tail`).
pub struct SimSink {
    frames: Arc<Mutex<Vec<EventFrame>>>,
    closed: Arc<Mutex<bool>>,
    stall: Box<dyn FnMut() -> bool + Send>,
}

/// The workload's read side of a `SimSink`: every frame the sink accepted,
/// in delivery order, and whether the sink has been dropped — which happens
/// exactly when the kernel drops a dead tail (drop-on-full or the terminal
/// gap), since the `Tail` (and the `Box<dyn EventSink>` inside it) goes with
/// it. `closed` is therefore a lifecycle signal, not a per-send status: a
/// send that itself reported `Full` is not recorded here (it was refused),
/// but the sink is not "closed" until the kernel actually drops the tail.
#[derive(Clone)]
pub struct SimSinkHandle {
    frames: Arc<Mutex<Vec<EventFrame>>>,
    closed: Arc<Mutex<bool>>,
}

impl SimSink {
    pub fn new(stall: Box<dyn FnMut() -> bool + Send>) -> (SimSink, SimSinkHandle) {
        let frames = Arc::new(Mutex::new(Vec::new()));
        let closed = Arc::new(Mutex::new(false));
        (
            SimSink {
                frames: frames.clone(),
                closed: closed.clone(),
                stall,
            },
            SimSinkHandle { frames, closed },
        )
    }
}

impl EventSink for SimSink {
    fn send(&mut self, frame: &EventFrame) -> SinkStatus {
        if (self.stall)() {
            return SinkStatus::Full;
        }
        self.frames
            .lock()
            .expect("sim sink lock poisoned by an earlier panic")
            .push(frame.clone());
        SinkStatus::Ok
    }
}

impl Drop for SimSink {
    /// The tail's connection ends exactly when the `Tail` (and this boxed
    /// sink) is dropped — the kernel never calls a separate "close"; there
    /// is none on `EventSink`. Marking `closed` here, rather than from a
    /// `SinkStatus`, is what makes `SimSinkHandle::closed()` mean "this
    /// tail is gone" regardless of whether its last send happened to
    /// succeed (the terminal gap frame's send is not acted on either way).
    fn drop(&mut self) {
        *self
            .closed
            .lock()
            .expect("sim sink lock poisoned by an earlier panic") = true;
    }
}

impl SimSinkHandle {
    pub fn frames(&self) -> Vec<EventFrame> {
        self.frames
            .lock()
            .expect("sim sink lock poisoned by an earlier panic")
            .clone()
    }

    pub fn closed(&self) -> bool {
        *self
            .closed
            .lock()
            .expect("sim sink lock poisoned by an earlier panic")
    }
}
