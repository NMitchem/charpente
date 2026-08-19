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
}

impl Trace {
    /// `capacity` preallocates the event vec. Size it generously: a reallocation
    /// inside a traced window lands in the measurement it is trying to take.
    pub fn recording(capacity: usize) -> Arc<Trace> {
        Arc::new(Trace {
            origin: Instant::now(),
            events: Mutex::new(Vec::with_capacity(capacity)),
            recording: true,
        })
    }

    pub fn passthrough() -> Arc<Trace> {
        Arc::new(Trace {
            origin: Instant::now(),
            events: Mutex::new(Vec::new()),
            recording: false,
        })
    }

    pub fn is_recording(&self) -> bool {
        self.recording
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
}
