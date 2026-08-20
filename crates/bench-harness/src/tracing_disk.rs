//! A `Disk` that RECORDS each IO op with its duration, so a layer can
//! attribute a commit's wall clock to phases WITHOUT a clock inside the core.
//!
//! **Why this boundary works.** redb is mounted behind our own `Disk` trait —
//! `DiskBackend` implements redb's `StorageBackend` over a `File`
//! (`charpente-core/src/table.rs:958-985`, "the only sanctioned way") — so
//! every byte redb touches crosses this wrapper. `Engine::submit_all`'s three
//! phases (N appends → one sync → N applies) are therefore recoverable from
//! the op stream alone, and hard rule 1 is never in question.
//!
//! **This module makes no arithmetic decision.** It records; every phase,
//! share and verdict is computed in `phase_accounting`. That split is the same
//! observations seam `core_bench`/`core_accounting` use, and it exists so a
//! disagreement about attribution is a disagreement about one pure function
//! over a `Vec<Event>` rather than about a measurement nobody can replay.
//!
//! **Scope, matching `counting.rs`'s decision so the two wrappers agree.**
//! `Disk::sync_dir`, `Disk::remove` and `Disk::list` delegate UNWRAPPED and
//! never reach the trace: they are `Disk` methods, not `File` methods, and a
//! directory barrier is not part of any commit phase. Only `File` ops are
//! recorded.
//!
//! Recording never changes an outcome — every method returns `inner`'s result
//! verbatim, including errors, which are recorded like any other op because a
//! failed fsync still costs time.

use charpente::core::io::{Disk, File};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Which file a traced op touched. Decided once at `open`, not per op: the
/// path is fixed for the life of the handle (`counting.rs` makes the same
/// call for the same reason).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PathClass {
    /// A WAL segment.
    Wal,
    /// The redb store itself.
    Store,
    /// Everything else — the `.horizon` breadcrumb and any future sidecar.
    Other,
}

impl PathClass {
    pub fn classify(path: &str) -> PathClass {
        if path.starts_with("wal/") {
            PathClass::Wal
        } else if path == "tables.redb" {
            // Exact, not `starts_with`: `tables.redb.horizon` is a sidecar
            // written by `write_horizon_breadcrumb`, not store traffic, and
            // folding it into the store would inflate redb's share with a
            // write redb never made.
            PathClass::Store
        } else {
            PathClass::Other
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpKind {
    Len,
    ReadAt,
    WriteAt,
    SetLen,
    Sync,
}

/// One traced op. Times are nanoseconds since the trace's origin, so an
/// `Event` is `Copy` and a whole run's trace is one flat `Vec`.
#[derive(Clone, Copy, Debug)]
pub struct Event {
    pub path: PathClass,
    pub op: OpKind,
    pub start_nanos: u64,
    pub end_nanos: u64,
}

/// The shared sink. `recording: false` is the passthrough arm of the overhead
/// measurement (Task 4): the wrapper still wraps and still pays the virtual
/// dispatch, so the difference between the two arms isolates the cost of
/// RECORDING from the cost of WRAPPING.
pub struct Trace {
    origin: Instant,
    events: Mutex<Vec<Event>>,
    recording: bool,
    /// The event vec's capacity immediately after construction (Task 3
    /// Ruling A, 2026-08-19 phase-attribution plan). Read from `Vec::capacity()`
    /// itself rather than the `capacity` argument passed to `recording` —
    /// an allocator is free to hand back more than requested to fit a size
    /// class, and comparing against the REQUESTED value would read that
    /// rounding as growth that never happened.
    initial_capacity: usize,
}

impl Trace {
    /// `capacity` preallocates the event vec. Size it generously: a reallocation
    /// inside a traced window lands in the measurement it is trying to take —
    /// see `capacity_grew_reason` for what that costs and how a caller detects
    /// it rather than merely hoping this constant was big enough.
    pub fn recording(capacity: usize) -> Arc<Trace> {
        let events = Vec::with_capacity(capacity);
        let initial_capacity = events.capacity();
        Arc::new(Trace {
            origin: Instant::now(),
            events: Mutex::new(events),
            recording: true,
            initial_capacity,
        })
    }

    pub fn passthrough() -> Arc<Trace> {
        let events = Vec::new();
        let initial_capacity = events.capacity();
        Arc::new(Trace {
            origin: Instant::now(),
            events: Mutex::new(events),
            recording: false,
            initial_capacity,
        })
    }

    pub fn is_recording(&self) -> bool {
        self.recording
    }

    /// Ruling A (2026-08-19 phase-attribution plan, "The capacity finding"):
    /// a real 50,000-request kernel-arm run — `bin/bench.rs`'s own default
    /// `--requests` — produced 1,336,846 events against a `1 << 20`
    /// (1,048,576) reservation, guaranteeing at least one reallocation
    /// *during* the traced window, silently: `phase_accounting::attribute`
    /// still returned `Ok` and its closure invariant (`totals.total() ==
    /// window_nanos`) still held exactly, because a reallocation's stall
    /// does not inflate any single traced op's own recorded duration
    /// (`Trace::record`'s `start`/`end` are sampled by the caller *before*
    /// the mutex-guarded `push` that can trigger the regrow) — it lands
    /// instead in whichever `gap_*` bucket happens to surround that
    /// particular `record()` call, indistinguishable after the fact from
    /// genuine idle time in that phase. Gaps are exactly what this
    /// instrument exists to attribute, so this is a wrong number that looks
    /// right.
    ///
    /// `Some` names the contract this trace violated, the site (both arms'
    /// capacity constants), and the fix, in the shape a caller pushes
    /// verbatim into its own `invalid` vec — read `wire_accounting.rs`'s
    /// `syncs_counted` for the idiom this mirrors: a fact about *how* a
    /// number was produced, surfaced beside the number itself rather than
    /// folded silently into it. `None` means the reservation held: nothing
    /// reallocated, so nothing about this trace's gap attribution is
    /// suspect on this account.
    pub fn capacity_grew_reason(&self) -> Option<String> {
        let events = self.events.lock().expect("trace lock");
        let current_capacity = events.capacity();
        if current_capacity <= self.initial_capacity {
            return None;
        }
        Some(format!(
            "contract: this run's phase trace grew its event buffer's capacity from {} to {} \
             while recording ({} events recorded across the whole trace, not just the \
             measured window) — `Trace::recording`'s whole point is a reservation nothing \
             reallocates while recording, because a reallocation's stall does not land in any \
             traced op's own duration (start/end are sampled before the mutex-guarded push \
             that can trigger it) — it lands instead in whichever gap_* phase happens to \
             surround that push, indistinguishable after the fact from genuine idle time in \
             that phase. This run's phase shares cannot be trusted and are withheld rather \
             than published. Fix: raise the capacity `Trace::recording` is constructed with \
             for this workload — `TRACE_CAPACITY` in core_bench.rs, or `trace_capacity()` in \
             kernel_bench.rs.",
            self.initial_capacity,
            current_capacity,
            events.len()
        ))
    }

    /// This trace's own clock, in the same time base every recorded `Event`
    /// uses (`start_nanos`/`end_nanos` are both `duration_since(self.origin)`).
    /// A caller stamps a measured window's boundaries with this — not with a
    /// second, unrelated `Instant` — so `phase_accounting::attribute` can
    /// compare `window_start_nanos`/`window_end_nanos` against event
    /// timestamps directly: both sides of the comparison come from the same
    /// origin, never from two different clocks (`phase_accounting`'s I-2 fix).
    pub fn now_nanos(&self) -> u64 {
        Instant::now().duration_since(self.origin).as_nanos() as u64
    }

    pub fn record(&self, path: PathClass, op: OpKind, start: Instant, end: Instant) {
        if !self.recording {
            return;
        }
        let event = Event {
            path,
            op,
            start_nanos: start.duration_since(self.origin).as_nanos() as u64,
            end_nanos: end.duration_since(self.origin).as_nanos() as u64,
        };
        self.events.lock().expect("trace lock").push(event);
    }

    pub fn events(&self) -> Vec<Event> {
        self.events.lock().expect("trace lock").clone()
    }
}

pub struct TracingDisk {
    inner: Box<dyn Disk>,
    trace: Arc<Trace>,
}

impl TracingDisk {
    pub fn new(inner: Box<dyn Disk>, trace: Arc<Trace>) -> TracingDisk {
        TracingDisk { inner, trace }
    }
}

impl Disk for TracingDisk {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        let file = self.inner.open(path)?;
        Ok(Box::new(TracingFile {
            inner: file,
            path: PathClass::classify(path),
            trace: self.trace.clone(),
        }))
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        self.inner.remove(path)
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.inner.list(prefix)
    }

    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        self.inner.sync_dir(prefix)
    }
}

struct TracingFile {
    inner: Box<dyn File>,
    path: PathClass,
    trace: Arc<Trace>,
}

impl File for TracingFile {
    fn len(&self) -> io::Result<u64> {
        let start = Instant::now();
        let out = self.inner.len();
        self.trace
            .record(self.path, OpKind::Len, start, Instant::now());
        out
    }

    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        let start = Instant::now();
        let result = self.inner.read_at(offset, out);
        self.trace
            .record(self.path, OpKind::ReadAt, start, Instant::now());
        result
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        let start = Instant::now();
        let out = self.inner.write_at(offset, data);
        self.trace
            .record(self.path, OpKind::WriteAt, start, Instant::now());
        out
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        let start = Instant::now();
        let out = self.inner.set_len(len);
        self.trace
            .record(self.path, OpKind::SetLen, start, Instant::now());
        out
    }

    fn sync(&self) -> io::Result<()> {
        let start = Instant::now();
        let out = self.inner.sync();
        self.trace
            .record(self.path, OpKind::Sync, start, Instant::now());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_class_splits_wal_store_and_sidecars() {
        // A real segment name: `log.rs::segment_name` formats `{base_seq:020}.log`.
        assert_eq!(
            PathClass::classify("wal/00000000000000000000.log"),
            PathClass::Wal
        );
        assert_eq!(PathClass::classify("tables.redb"), PathClass::Store);
        // Exact match on purpose: the horizon breadcrumb is a sidecar, not the
        // store, and `starts_with` would swallow it (db.rs `horizon_path`).
        assert_eq!(PathClass::classify("tables.redb.horizon"), PathClass::Other);
    }

    #[test]
    fn passthrough_records_nothing_but_still_wraps() {
        let trace = Trace::passthrough();
        assert!(!trace.is_recording());
        trace.record(PathClass::Wal, OpKind::Sync, Instant::now(), Instant::now());
        assert!(trace.events().is_empty());
    }

    #[test]
    fn recording_keeps_ops_in_call_order() {
        let trace = Trace::recording(8);
        let a = Instant::now();
        trace.record(PathClass::Wal, OpKind::WriteAt, a, Instant::now());
        trace.record(PathClass::Wal, OpKind::Sync, Instant::now(), Instant::now());
        trace.record(
            PathClass::Store,
            OpKind::WriteAt,
            Instant::now(),
            Instant::now(),
        );
        let events = trace.events();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].op, OpKind::WriteAt);
        assert_eq!(events[1].op, OpKind::Sync);
        assert_eq!(events[2].path, PathClass::Store);
        assert!(events[0].end_nanos <= events[1].start_nanos);
    }

    #[test]
    fn now_nanos_shares_the_events_own_time_base() {
        // The property `phase_accounting::attribute`'s window parameters
        // depend on: a sample taken via `now_nanos` before an event and one
        // taken after must bracket that event's own timestamps, because both
        // come from the same `origin`.
        let trace = Trace::recording(4);
        let before = trace.now_nanos();
        let start = Instant::now();
        trace.record(PathClass::Wal, OpKind::WriteAt, start, Instant::now());
        let after = trace.now_nanos();

        let events = trace.events();
        assert_eq!(events.len(), 1);
        assert!(
            before <= events[0].start_nanos,
            "a now_nanos() sample taken before the op must not read later than the op's own start"
        );
        assert!(
            events[0].end_nanos <= after,
            "a now_nanos() sample taken after the op must not read earlier than the op's own end"
        );
    }

    // Task 3 Ruling A (2026-08-19 phase-attribution plan): a real kernel-arm
    // run at 50,000 requests produced 1,336,846 events against a `1 << 20`
    // reservation — a silent reallocation `phase_accounting::attribute` has
    // no way to see, since its closure invariant holds regardless of where
    // the regrow's stall landed. These prove the detector itself, RED first
    // per hard rule 3: `capacity_grew_reason` must distinguish "stayed inside
    // the reservation" from "regrew during recording" using the vec's own
    // actual capacity, not the caller's requested one, and the `Some` arm
    // must name the contract it enforces.

    #[test]
    fn capacity_grew_reason_is_none_while_the_reservation_holds() {
        let trace = Trace::recording(4);
        for _ in 0..4 {
            trace.record(
                PathClass::Wal,
                OpKind::WriteAt,
                Instant::now(),
                Instant::now(),
            );
        }
        assert!(
            trace.capacity_grew_reason().is_none(),
            "four pushes into a 4-capacity reservation must not report growth"
        );
    }

    #[test]
    fn capacity_grew_reason_names_the_contract_once_the_reservation_is_exceeded() {
        let trace = Trace::recording(1);
        // Enough pushes to force at least one regrow past any allocator
        // size-class rounding of the initial reservation.
        for _ in 0..8 {
            trace.record(
                PathClass::Wal,
                OpKind::WriteAt,
                Instant::now(),
                Instant::now(),
            );
        }
        let reason = trace
            .capacity_grew_reason()
            .expect("eight pushes into a 1-capacity reservation must report growth");
        assert!(
            reason.starts_with("contract:"),
            "must use the same refusal prefix phase_accounting's own refusals use, so a \
             caller's \"contract:\"-scoped check catches this too: {reason}"
        );
        assert!(
            reason.contains("Trace::recording"),
            "must name the contract this trace violated: {reason}"
        );
        assert!(
            reason.contains("TRACE_CAPACITY") && reason.contains("trace_capacity()"),
            "must name the fix — both arms' capacity sites: {reason}"
        );
        assert!(
            reason.contains(" 1 ") || reason.contains("from 1 "),
            "must name the initial capacity (1): {reason}"
        );
        assert!(
            reason.contains(&format!("{} events", 8)),
            "must name how many events this trace actually recorded: {reason}"
        );
    }

    #[test]
    fn a_passthrough_trace_never_reports_growth() {
        // `recording: false` means `record` early-returns before ever
        // touching the vec (module doc), so nothing can grow it — proven
        // rather than assumed, since a future edit to `record`'s early
        // return could silently defeat that.
        let trace = Trace::passthrough();
        trace.record(
            PathClass::Wal,
            OpKind::WriteAt,
            Instant::now(),
            Instant::now(),
        );
        assert!(trace.capacity_grew_reason().is_none());
    }
}
