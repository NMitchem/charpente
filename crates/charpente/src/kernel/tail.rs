//! The event-sink seam (realtime T4; DESIGN §6, spec docs/superpowers/specs/
//! 2026-07-14-realtime-design.md §3). `EventSink` is what a driver — prod's
//! bounded channel (`crate::serve`), sim's recording buffer — implements so
//! the kernel can push frames to a subscriber without knowing who, or what,
//! is on the other end. The Tail registry itself (registration, floor/
//! divergence validation, the bounded delivery unit, `SimSink`) lands in T5;
//! this file exists now only so `Respond` (kernel/mod.rs) compiles against
//! its exact shapes.

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
