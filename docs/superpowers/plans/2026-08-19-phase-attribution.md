# Per-Phase Commit Attribution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Measure which phase of a batched commit owns the wall clock, so DECISIONS §7 trigger (a) ("benchmarks show redb limiting the headline throughput claim") can be re-read against the batched path instead of the unbatched one.

**Architecture:** A `TracingDisk` in `bench-harness` wraps any `Disk` and records `(op, path class, start, end)` per IO call. Because redb is mounted behind our own `Disk` trait (`table.rs:958-985`, `DiskBackend`), every redb byte crosses that wrapper — so phase attribution needs **no change to `charpente-core`** and hard rule 1 is untouched. A separate `phase_accounting` module reconstructs phases from the event stream and makes every arithmetic decision; the recorder decides nothing. This is the same observations seam `core_bench`/`core_accounting` already use.

**Tech Stack:** Rust, `std::time::Instant`, `charpente::core::io::{Disk, File}`, existing `bench-harness` report/CLI plumbing.

**Spec:** `.superpowers/sdd/2026-08-19-phase-attribution/SPEC.md`

## Global Constraints

Every task's requirements implicitly include this section.

- **Hard rule 1 — no ambient nondeterminism inside the core.** This plan changes **zero files under `crates/charpente-core/`**. If a task appears to need one, stop and escalate; do not add a clock to core.
- **Hard rule 4 — assert invariants and negative space.** Assertions stay on in release.
- **Hard rule 5 — error messages are product surface.** A refusal names the contract, the site, and the fix.
- **Hard rule 6 — public-API changes update the docs in the same commit.**
- **No published number may move.** `kernel_bench`'s sync counter is UNSCOPED (counts every path) and its published `device_ratio` (0.392 at last measurement) depends on that. Do **not** rescope it, do **not** replace it with `counting::CountingDisk`, and do not "fix" the apples-to-oranges comparison `counting.rs:68-78` documents. That rescope would move a published number and is out of scope.
- **Provenance per number.** Every published figure carries a counted-vs-derived flag, as `syncs_counted` does in `wire_accounting.rs`. A derived phase share is not a measured one.
- **No cross-layer ratios.** A ratio whose numerator and denominator come from different layers reads as meaningful and is not (the `device_ratio` history).
- **Nothing is quotable until it lands in `docs/BENCH.md`** (CLAUDE.md). Intermediate artifacts are observations.
- **`bench-harness` is edge tooling.** Its `lib.rs` already carries `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]`, so real clocks are fine *in this crate*.
- **Workflow:** commit directly to `main`. **No `Co-Authored-By` trailers.**
- **Prove the staged bytes are the tested bytes before every commit.** A chained
  `fmt && clippy && test && git add && git commit` protects against a FAILING gate; it does not
  protect against the file changing underneath the chain. On 2026-08-19 that gap put a mutation
  artifact on `main` (`217aca3`, repaired at `f2b60aa`): the gates passed on correct content, a
  concurrent agent's sweep held the file mutated, and `git add` staged that. Use:

  ```sh
  TESTED=$(git hash-object "$F")
  # ... gates ...
  git add "$F"
  STAGED=$(git ls-files -s "$F" | awk '{print $2}')
  [ "$TESTED" = "$STAGED" ] || { echo "staged content is not what the gates validated"; exit 1; }
  ```
- **Never run a mutation sweep, a `git checkout` of another commit, or any experiment that leaves
  the tree temporarily wrong, inside the main working tree.** Use
  `git worktree add --detach <dir> <ref>` with its own `CARGO_TARGET_DIR` — sharing a target dir
  across worktrees in this repo leaks artifacts between trees and has produced false results.
  Remove the worktree and its target dir when done.
- **CI gates, all three, before any commit:** `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`.
- **This shell is zsh.** `PIPESTATUS` is never populated. Use `${pipestatus[1]}` (lowercase, 1-indexed) or `set -o pipefail`. Pipe cargo through `tee`, never `tail`.

## Scope ruling recorded before execution

The spec says "attach to the existing connection sweep." **That is overridden here, deliberately.** The connection sweep is the *wire* arm, which runs the server as a child process killed with SIGKILL (`lib.rs`'s `SYNC_COUNTER` doc), so exporting a full trace from it would require inventing an HTTP trace-export protocol on top of `/__syncs`. The `core` and `kernel` arms are both **in-process** (`core_bench.rs:54`, `kernel_bench.rs:117-118`) and together give the contrast that actually answers the question: core runs unbatched at 1 record/sync, kernel runs at 61.96 records/sync — batch=1 versus batch≈62, the exact axis §2 of the spec argues redb's share rises along. Cost if wrong: the result is two points rather than a five-point curve, and the wire sweep remains available as a follow-on.

## File Structure

- **Create `crates/bench-harness/src/tracing_disk.rs`** — the recorder. Wraps a `Disk`, records one `Event` per `File` op. Makes no arithmetic decision. Mirrors `counting.rs`'s contract: never changes an outcome.
- **Create `crates/bench-harness/src/phase_accounting.rs`** — reconstruction and every arithmetic decision. Pure functions over `&[Event]`; no IO, so its tests need no disk.
- **Modify `crates/bench-harness/src/lib.rs`** — declare the two modules.
- **Modify `crates/bench-harness/src/core_bench.rs`** — wrap the disk at line 54, carry the trace into observations.
- **Modify `crates/bench-harness/src/kernel_bench.rs`** — wrap `ProdDisk` under the existing local `CountingDisk`, carry the trace.
- **Modify `crates/bench-harness/src/report.rs`** — publish phase fields with provenance.
- **Modify `crates/bench-harness/src/bin/bench.rs`** — surface phases in human and JSON output.
- **Modify `docs/BENCH.md`** — the per-phase table, provenance, measured instrument overhead.
- **Create `.superpowers/sdd/2026-08-19-phase-attribution/trigger-a-rereading.md`** — the drafted §7 re-reading. **Not** a `DECISIONS.md` edit: CLAUDE.md requires Noah's sign-off to reopen a DECISIONS entry.

---

### Task 1: The tracing Disk

**Files:**
- Create: `crates/bench-harness/src/tracing_disk.rs`
- Modify: `crates/bench-harness/src/lib.rs` (add `pub mod tracing_disk;`)
- Test: inline `#[cfg(test)] mod tests` in `tracing_disk.rs`

**Interfaces:**
- Consumes: `charpente::core::io::{Disk, File}`.
- Produces: `PathClass` (`Wal`/`Store`/`Other`), `OpKind` (`Len`/`ReadAt`/`WriteAt`/`SetLen`/`Sync`), `Event { path, op, start_nanos, end_nanos }`, `Trace` (with `Trace::recording(capacity) -> Arc<Trace>`, `Trace::passthrough() -> Arc<Trace>`, `Trace::events(&self) -> Vec<Event>`, `Trace::is_recording(&self) -> bool`), and `TracingDisk::new(inner: Box<dyn Disk>, trace: Arc<Trace>) -> TracingDisk`.

- [ ] **Step 1: Write the failing tests**

Append to `crates/bench-harness/src/tracing_disk.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_class_splits_wal_store_and_sidecars() {
        assert_eq!(PathClass::classify("wal/00000000000000000000.log"), PathClass::Wal);
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
        trace.record(PathClass::Store, OpKind::WriteAt, Instant::now(), Instant::now());
        let events = trace.events();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].op, OpKind::WriteAt);
        assert_eq!(events[1].op, OpKind::Sync);
        assert_eq!(events[2].path, PathClass::Store);
        assert!(events[0].end_nanos <= events[1].start_nanos);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p bench-harness --lib tracing_disk`
Expected: FAIL — the module does not compile yet (`PathClass` not found).

- [ ] **Step 3: Write the implementation**

Write `crates/bench-harness/src/tracing_disk.rs`:

```rust
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
        self.trace.record(self.path, OpKind::Len, start, Instant::now());
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
        self.trace.record(self.path, OpKind::Sync, start, Instant::now());
        out
    }
}
```

Add exactly ONE line to `crates/bench-harness/src/lib.rs`, in the existing `pub mod` block, alphabetically (after `pub mod report;`):

```rust
pub mod tracing_disk;
```

`phase_accounting` arrives in Task 2 and declares itself there. Adding it now breaks the build.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p bench-harness --lib tracing_disk`
Expected: PASS, 3 tests.

- [ ] **Step 5: Run the full gates**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
```
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/bench-harness/src/tracing_disk.rs crates/bench-harness/src/lib.rs
git commit -m "bench: a Disk that records what each IO op cost, so phases need no clock in core"
```

---

### Task 2: Phase reconstruction and the closure invariant

> **SUPERSEDED — read `crates/bench-harness/src/phase_accounting.rs` for the shipped API.**
> This section records the task as *planned*. Three fix rounds changed it materially, and the
> code blocks below are kept as the argument that was made, not as current reference. What
> changed: `attribute` now takes explicit `window_start_nanos`/`window_end_nanos` (the original
> `window_nanos` silently assumed the trace origin was the window start, which refuses every
> real run); it returns `Attribution { totals, events_attributed, events_ignored_before_window,
> events_ignored_after_window }` rather than a bare `PhaseTotals`; `store_io` split into
> `store_write_io`/`store_read_io`/`store_sync_io` with the WAL split to match, taking
> `PhaseTotals` to 14 fields; inverted events and window-straddling events are refused by name.
> The reasons are in `docs/LEDGER.md` and the review files under this plan's workspace.


**Files:**
- Create: `crates/bench-harness/src/phase_accounting.rs`
- Modify: `crates/bench-harness/src/lib.rs` (add `pub mod phase_accounting;`)
- Test: inline `#[cfg(test)] mod tests` in `phase_accounting.rs`

**Interfaces:**
- Consumes: `crate::tracing_disk::{Event, OpKind, PathClass}` from Task 1.
- Produces: `PhaseTotals` (all fields `u64` nanoseconds, listed in the code below) and `attribute(events: &[Event], window_nanos: u64) -> Result<PhaseTotals, Vec<String>>`.

**Why the refusal path exists:** the gap between two ops is idle-or-CPU time **only if the arm is single-threaded**. On a concurrent arm, ops overlap and "the gap" is not attributable to anything. Rather than publish a meaningless number, `attribute` refuses — matching the harness's existing `invalid: Vec<String>` idiom (`core_bench.rs`).

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::tracing_disk::{Event, OpKind, PathClass};

    fn ev(path: PathClass, op: OpKind, start: u64, end: u64) -> Event {
        Event { path, op, start_nanos: start, end_nanos: end }
    }

    #[test]
    fn a_batch_attributes_append_sync_and_apply_separately() {
        // Two WAL appends, one WAL sync, two store ops — `submit_all`'s shape.
        let events = vec![
            ev(PathClass::Wal, OpKind::WriteAt, 0, 10),
            ev(PathClass::Wal, OpKind::WriteAt, 10, 20),
            ev(PathClass::Wal, OpKind::Sync, 20, 1020),
            ev(PathClass::Store, OpKind::WriteAt, 1030, 1050),
            ev(PathClass::Store, OpKind::WriteAt, 1060, 1080),
        ];
        let t = attribute(&events, 1100).expect("single-threaded trace attributes");
        assert_eq!(t.wal_append_io, 20);
        assert_eq!(t.wal_sync_io, 1000);
        assert_eq!(t.store_io, 40);
        assert_eq!(t.gap_wal_to_store, 10, "the sync→apply transition");
        assert_eq!(t.gap_within_store, 10, "redb-internal CPU between its own IOs");
        assert_eq!(t.tail, 20);
    }

    #[test]
    fn every_nanosecond_of_the_window_is_attributed() {
        let events = vec![
            ev(PathClass::Wal, OpKind::WriteAt, 5, 15),
            ev(PathClass::Store, OpKind::Sync, 25, 30),
        ];
        let t = attribute(&events, 100).expect("attributes");
        assert_eq!(t.total(), 100, "totals must close to the window exactly");
    }

    #[test]
    fn overlapping_ops_are_refused_not_averaged() {
        let events = vec![
            ev(PathClass::Wal, OpKind::WriteAt, 0, 100),
            ev(PathClass::Store, OpKind::WriteAt, 50, 150),
        ];
        let invalid = attribute(&events, 200).expect_err("overlap must refuse");
        assert!(
            invalid[0].contains("single-threaded"),
            "the refusal must name the contract it enforces, got: {}",
            invalid[0]
        );
    }

    #[test]
    fn an_op_past_the_window_end_is_refused() {
        let events = vec![ev(PathClass::Wal, OpKind::Sync, 0, 500)];
        let invalid = attribute(&events, 100).expect_err("op outside the window must refuse");
        assert!(invalid[0].contains("window"), "got: {}", invalid[0]);
    }

    #[test]
    fn an_empty_trace_is_all_lead_in() {
        let t = attribute(&[], 100).expect("an empty trace is legal");
        assert_eq!(t.lead_in, 100);
        assert_eq!(t.total(), 100);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p bench-harness --lib phase_accounting`
Expected: FAIL — module does not exist.

- [ ] **Step 3: Write the implementation**

Write `crates/bench-harness/src/phase_accounting.rs`:

```rust
//! Every arithmetic decision about a trace lives here. `tracing_disk` records
//! and decides nothing — the observations seam, as in `core_accounting`.
//!
//! **What a gap is, and the assumption that makes it mean anything.** Between
//! two recorded ops the process is doing something the `Disk` boundary cannot
//! see: CPU, or waiting. Attributing that time requires that ops do not
//! overlap — i.e. that the arm is single-threaded. The `core` and `kernel`
//! arms are; the `wire` arm is not, which is why `attribute` REFUSES an
//! overlapping trace rather than publishing a number nobody can defend.
//!
//! **Measurement and inference are kept apart on purpose.** This module reports
//! where the time went (`gap_within_store`, `gap_wal_to_store`, …). It does not
//! decide that `gap_within_store` "is redb's CPU" — that reading is stated in
//! `docs/BENCH.md` where it can be argued with, not buried in a field name.

use crate::tracing_disk::{Event, OpKind, PathClass};

/// Nanoseconds, every field. The fields partition the measured window: `total`
/// equals the window exactly, by construction, and `attribute` asserts it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PhaseTotals {
    /// Time inside WAL ops that are not `sync` — `submit_all`'s append loop.
    pub wal_append_io: u64,
    /// Time inside WAL `sync` — the one durability barrier per batch.
    pub wal_sync_io: u64,
    /// Time inside ops on `tables.redb` — redb's own IO.
    pub store_io: u64,
    /// Time inside ops on anything else (the `.horizon` breadcrumb).
    pub other_io: u64,
    /// Gap between two consecutive WAL ops.
    pub gap_within_wal: u64,
    /// Gap from a WAL op to a store op — the sync→apply transition.
    pub gap_wal_to_store: u64,
    /// Gap between two consecutive store ops — work redb does between its own
    /// IOs.
    pub gap_within_store: u64,
    /// Gap from a store op back to a WAL op — end of one batch to the start of
    /// the next, so: everything OUTSIDE the commit path.
    pub gap_store_to_wal: u64,
    /// Any gap whose neighbours do not fall in the pairs above.
    pub gap_other: u64,
    /// Window start to the first recorded op.
    pub lead_in: u64,
    /// Last recorded op to window end.
    pub tail: u64,
}

impl PhaseTotals {
    pub fn total(&self) -> u64 {
        self.wal_append_io
            + self.wal_sync_io
            + self.store_io
            + self.other_io
            + self.gap_within_wal
            + self.gap_wal_to_store
            + self.gap_within_store
            + self.gap_store_to_wal
            + self.gap_other
            + self.lead_in
            + self.tail
    }
}

/// Partition `window_nanos` across the phases the trace reveals.
///
/// Refuses (rather than returning a number) when the trace cannot support the
/// arithmetic: overlapping ops, or an op outside the window.
pub fn attribute(events: &[Event], window_nanos: u64) -> Result<PhaseTotals, Vec<String>> {
    let mut invalid = Vec::new();

    for pair in events.windows(2) {
        if pair[1].start_nanos < pair[0].end_nanos {
            invalid.push(format!(
                "contract: phase attribution requires a single-threaded arm, but a traced op \
                 ending at {} ns overlaps the next starting at {} ns. On a concurrent arm the \
                 gap between ops is not idle time and every phase share below would be \
                 meaningless. Fix: run this on the core or kernel arm (both in-process, \
                 single-threaded), not the wire arm.",
                pair[0].end_nanos, pair[1].start_nanos
            ));
            break;
        }
    }

    if let Some(last) = events.last()
        && last.end_nanos > window_nanos
    {
        invalid.push(format!(
            "contract: a traced op ends at {} ns, past the measured window's {} ns. The trace \
             and the window disagree about what was measured. Fix: start the trace after the \
             window opens and read it before the window closes (see kernel_bench's \
             syncs_before/syncs_after pair for the shape).",
            last.end_nanos, window_nanos
        ));
    }

    if !invalid.is_empty() {
        return Err(invalid);
    }

    let mut totals = PhaseTotals::default();

    let Some(first) = events.first() else {
        totals.lead_in = window_nanos;
        return Ok(totals);
    };

    totals.lead_in = first.start_nanos;
    totals.tail = window_nanos - events[events.len() - 1].end_nanos;

    for event in events {
        let duration = event.end_nanos - event.start_nanos;
        match (event.path, event.op) {
            (PathClass::Wal, OpKind::Sync) => totals.wal_sync_io += duration,
            (PathClass::Wal, _) => totals.wal_append_io += duration,
            (PathClass::Store, _) => totals.store_io += duration,
            (PathClass::Other, _) => totals.other_io += duration,
        }
    }

    for pair in events.windows(2) {
        let gap = pair[1].start_nanos - pair[0].end_nanos;
        match (pair[0].path, pair[1].path) {
            (PathClass::Wal, PathClass::Wal) => totals.gap_within_wal += gap,
            (PathClass::Wal, PathClass::Store) => totals.gap_wal_to_store += gap,
            (PathClass::Store, PathClass::Store) => totals.gap_within_store += gap,
            (PathClass::Store, PathClass::Wal) => totals.gap_store_to_wal += gap,
            _ => totals.gap_other += gap,
        }
    }

    // Negative space: the fields partition the window. A residual means a gap
    // or an op was counted twice or not at all, which would show up as a phase
    // share that looks plausible and is wrong.
    assert_eq!(
        totals.total(),
        window_nanos,
        "phase totals must close to the measured window exactly"
    );

    Ok(totals)
}
```

Add `pub mod phase_accounting;` to `crates/bench-harness/src/lib.rs`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p bench-harness --lib phase_accounting`
Expected: PASS, 5 tests.

- [ ] **Step 5: Prove the closure assert has teeth (RED-first, hard rule 3)**

Temporarily delete the `+ self.gap_within_store` term from `PhaseTotals::total`, run the suite, and confirm `a_batch_attributes_append_sync_and_apply_separately` and `every_nanosecond_of_the_window_is_attributed` FAIL on the assert. Restore the term and confirm GREEN. Record both outcomes in the task report — an assertion nobody has seen fail is not evidence.

- [ ] **Step 6: Run the full gates and commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
git add crates/bench-harness/src/phase_accounting.rs crates/bench-harness/src/lib.rs
git commit -m "bench: partition a measured window across commit phases, refusing what it cannot attribute"
```

---

### Task 3: Wire the trace into the core and kernel arms

**REWRITTEN 2026-08-19 after Task 2's review.** The original text specified creating the trace,
then opening the `Db`, then measuring after a warmup loop — which is exactly the origin mismatch
finding I-2 refuses, because `Trace::recording()` stamps its origin at construction while the
window opens much later. Any implementer following the old text would have had every run refused.

**Files:**
- Modify: `crates/bench-harness/src/core_bench.rs` (the `Db::open` at :54, the window at :87, the `WriteObs` literal)
- Modify: `crates/bench-harness/src/kernel_bench.rs` (the local `CountingDisk` at :46, its construction at :117, the window at :183-187)
- Test: create `crates/bench-harness/tests/phase_trace.rs`

**Interfaces:**
- Consumes: `crate::tracing_disk::{Trace, TracingDisk}` and `Trace::now_nanos() -> u64`; `crate::phase_accounting::{attribute, Attribution, PhaseTotals}` where `attribute(events: &[Event], window_start_nanos: u64, window_end_nanos: u64) -> Result<Attribution, Vec<String>>` and `Attribution { totals: PhaseTotals, events_attributed: u64, events_ignored_before_window: u64, events_ignored_after_window: u64 }`.
- `PhaseTotals` has 14 `u64` nanosecond fields: `wal_append_io`, `wal_read_io`, `wal_sync_io`, `store_write_io`, `store_read_io`, `store_sync_io`, `other_io`, `gap_within_wal`, `gap_wal_to_store`, `gap_within_store`, `gap_store_to_wal`, `gap_other`, `lead_in`, `tail`.
- Produces: `phases: Option<PhaseTotals>` on both arms' observation structs. `None` means the attribution was refused (and the refusal strings went to `invalid`) or the arm ran in passthrough. **`None`, never a zeroed `PhaseTotals`** — a zeroed phase block is indistinguishable from a genuinely fast one.

**THE NESTING ORDER IS LOAD-BEARING, not a style choice.** In `kernel_bench`, put `TracingDisk` INSIDE the existing `CountingDisk`:

```rust
CountingDisk {
    inner: Box::new(TracingDisk::new(Box::new(ProdDisk::new(dir)?), trace.clone())),
    syncs: sync_count.clone(),
}
```

That gives a `File` chain of `CountingFile` → `TracingFile` → `ProdFile`, so the tracer times the
REAL IO and excludes `CountingFile`'s atomic increment from the measurement. Reversed, every
recorded duration would include the counter's own overhead. This also keeps `CountingDisk`
unscoped and its published `device_ratio` untouched, per Global Constraints. You will need to
change `CountingDisk`'s `inner` field from `ProdDisk` to `Box<dyn Disk>`; change nothing else
about it.

**ONE `Trace` PER ARM.** Task 1's review flagged that sharing one `Trace` across concurrently-driven
disks serializes their IO on its Mutex. Both arms here are single-threaded so it is dormant, but
do not create a shared static or pass one `Trace` to two disks.

- [ ] **Step 1: Write the failing integration test**

Create `crates/bench-harness/tests/phase_trace.rs`:

```rust
//! The trace must attribute a REAL run, not just synthetic events.
//!
//! These tests deliberately assert NOTHING about which phase is largest. That
//! is the measurement this instrument exists to take, and a test that encodes
//! the expected answer stops being a check and becomes a way to confirm a
//! prior belief.

#[test]
fn the_core_arm_attributes_a_real_run() {
    // bench-harness has NO `tempfile` dependency and does not need one — this
    // is the idiom `tests/harness.rs:21` already uses. Do not add a crate.
    let dir = std::env::temp_dir().join(format!("bench-phase-core-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let (report, invalid) = bench_harness::core_bench::run(
        dir.to_str().expect("utf-8 path"),
        std::time::Duration::from_millis(200),
        std::time::Duration::from_millis(800),
    )
    .expect("core arm runs");

    let phases = report
        .phases
        .expect("a recording run carries phases; refusals were: {invalid:?}");
    assert!(
        invalid.is_empty(),
        "attribution must not refuse a single-threaded arm: {invalid:?}"
    );
    assert_eq!(
        phases.total(),
        report.window_nanos,
        "phases must close to the measured window exactly"
    );
    assert!(
        phases.wal_sync_io > 0,
        "a run that commits must have paid at least one WAL fsync"
    );
}
```

Name `report.window_nanos` (or whatever the arm actually publishes as
`window_end_nanos - window_start_nanos`) consistently between the arm and this test; the
requirement is that the closure check compares against the SAME window the attribution used, not
against `obs.elapsed`. Those two differ by tens of nanoseconds and asserting closure against the
wrong one produces a flaky failure nobody can reproduce.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p bench-harness --test phase_trace`
Expected: FAIL to compile — `report.phases` does not exist yet.

- [ ] **Step 3: Wire `core_bench`**

Replace the disk construction at `core_bench.rs:54`:

```rust
let trace = Trace::recording(TRACE_CAPACITY);
let mut db = Db::open(
    Box::new(TracingDisk::new(Box::new(ProdDisk::new(dir)?), trace.clone())),
    DbConfig::default(),
    schema(),
)?;
```

Stamp the window from the trace's own time base, straddling the throughput window so no op is
half-in:

```rust
let window_start_nanos = trace.now_nanos();
let start = Instant::now();
// ... existing measured write loop, unchanged ...
let elapsed = start.elapsed();
let window_end_nanos = trace.now_nanos();
```

Then attribute BEFORE the `obs.write = WriteObs { .. }` literal, and put `phases` in that literal.
Do not add it as a trailing `obs.write.phases = ...` mutation — the full-literal shape is the C-2
fix and exists precisely so a future edit that drops a field breaks the build instead of silently
zeroing a gate input:

```rust
let events = trace.events();
let phases = match attribute(&events, window_start_nanos, window_end_nanos) {
    Ok(attribution) => {
        // The payoff of fix round 3's R-1: a window stamped from the wrong
        // place no longer fails loudly, it dilutes quietly. Events before the
        // window are warmup and `Db::open` — expected. Events AFTER it are not,
        // on an arm whose window closes at the end of its run, so they mean the
        // window and the trace disagree about what was measured.
        if attribution.events_ignored_after_window > 0 {
            invalid.push(format!(
                "contract: {} traced op(s) fall after the measured window closed. On this arm \
                 the window closes at the end of the run, so this means \
                 `window_end_nanos` was stamped before the last IO completed and the published \
                 phase shares would be diluted by the ops left out. Fix: stamp \
                 `window_end_nanos` with `Trace::now_nanos()` after the measured section \
                 finishes, not before.",
                attribution.events_ignored_after_window
            ));
        }
        Some(attribution.totals)
    }
    Err(refusals) => {
        invalid.extend(refusals);
        None
    }
};
```

A refused attribution must NOT discard the run — its throughput figures are still good. That is
why refusals go to `invalid` and `phases` goes to `None`.

`TRACE_CAPACITY` is `1 << 20` events. `size_of::<Event>()` is 24 bytes (measured in Task 1), so
that is ~25 MB. A reallocation inside the traced window lands in the measurement it is trying to
take.

- [ ] **Step 4: Wire `kernel_bench`**

Same shape, with the nesting above. The window is already bracketed by `syncs_before`/`syncs_after`
at `kernel_bench.rs:183-187`; stamp `window_start_nanos` immediately after `syncs_before` and
`window_end_nanos` immediately after `obs.elapsed = start.elapsed()`.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p bench-harness --test phase_trace`
Expected: PASS.

- [ ] **Step 6: Prove no published number moved**

This is the step that protects every figure already in `docs/BENCH.md`. **Build the baseline in a
detached worktree with its own `CARGO_TARGET_DIR`, never by checking out the previous commit in
the main tree** — a benchmark run takes minutes, and a main tree sitting at the wrong commit for
minutes is how a wrong artifact gets committed. Run both arms at this commit and at the previous
one, and compare `acked_writes_per_sec`, `log_records_per_sec`, `syncs`
and `device_ratio`. Report BOTH sides in the task report. Because these are timing measurements
they will not be bit-identical — state the run-to-run spread from at least 3 runs per side and say
whether the delta sits inside it, rather than declaring a match or a regression from n=1.

A number moving beyond that spread means the wrapper changed an outcome, which its own module doc
forbids. Escalate rather than accepting it.

- [ ] **Step 7: Full gates and commit**

Chain the commit to the gates so a failed gate cannot let it through:

```bash
cargo fmt --all --check \
 && cargo clippy --workspace --all-targets -- -D warnings \
 && cargo test --workspace \
 && git add crates/bench-harness/ \
 && git commit -m "bench: the core and kernel arms carry a phase trace through their measured window"
```

### Task 4: Measure the instrument's own overhead, and gate on it

**Files:**
- Modify: `crates/bench-harness/src/bin/bench.rs` (a `--trace-overhead` mode)
- Modify: `crates/bench-harness/src/phase_accounting.rs` (the plausibility gate)
- Test: inline test in `phase_accounting.rs`

**Interfaces:**
- Consumes: `Trace::recording` / `Trace::passthrough` (Task 1), the traced arms (Task 3).
- Produces: `overhead_gate(totals: &PhaseTotals, overhead_nanos_per_op: u64, op_count: u64) -> Vec<String>`.

**Why this task is not optional.** The spec's own con on approach (A): "a per-op `Instant::now()` perturbs a cheap buffered append far more than a multi-millisecond fsync. Measure that overhead; do not assume it. A trace whose own cost is comparable to the append phase cannot measure the append phase."

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn overhead_comparable_to_the_smallest_phase_is_refused() {
    let totals = PhaseTotals { wal_append_io: 1_000, wal_sync_io: 1_000_000, ..Default::default() };
    // 100 ops at 50 ns of instrument cost each = 5,000 ns, five times the
    // append phase it is trying to measure.
    let invalid = overhead_gate(&totals, 50, 100);
    assert!(!invalid.is_empty(), "overhead above the smallest phase must refuse");
    assert!(invalid[0].contains("append"), "the refusal must name the phase it invalidates");
}

#[test]
fn overhead_far_below_every_phase_passes() {
    let totals = PhaseTotals { wal_append_io: 1_000_000, wal_sync_io: 10_000_000, ..Default::default() };
    assert!(overhead_gate(&totals, 50, 100).is_empty());
}
```

- [ ] **Step 2: Run to verify it fails.** `cargo test -p bench-harness --lib overhead`

- [ ] **Step 3: Implement `overhead_gate`**

Refuse when `overhead_nanos_per_op * op_count` exceeds 10% of any non-zero IO phase. Name the phase, the two magnitudes, and the fix (fewer traced op kinds, or approach (B)) — hard rule 5.

- [ ] **Step 4: Implement `--trace-overhead` and MEASURE**

Run the kernel arm twice at identical settings — once with `Trace::recording`, once with `Trace::passthrough` — and report the throughput delta and the derived per-op cost. Both runs go through `TracingDisk`, so the delta isolates recording from wrapping.

Follow `docs/CLAUDE.md`'s measurement discipline: CPU time, not wall clock; run each arm at least 3 times and report all observations, not a single number. If the delta is inside run-to-run noise, say so — "below the noise floor of n=3" is an honest result and "0%" is not.

- [ ] **Step 5: Run the gates and commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
git add crates/bench-harness/
git commit -m "bench: the phase trace measures its own overhead and refuses a phase it cannot resolve"
```

---

### Task 5: Publish the phases through report and CLI

**Files:**
- Modify: `crates/bench-harness/src/report.rs`
- Modify: `crates/bench-harness/src/bin/bench.rs`
- Test: inline tests in `report.rs`

**Interfaces:**
- Consumes: `PhaseTotals` (Task 2), the arms' `phases` field (Task 3), `overhead_gate` (Task 4).
- Produces: JSON keys `phases` (object of nanosecond fields) and `phases_counted` (bool), plus a human-readable phase table.

**CARRIED FROM TASK 3 (Ruling 14).** `phases: None` now has THREE distinct causes — a
`phase_accounting::attribute` refusal, a trace-buffer reallocation detected by
`Trace::capacity_grew_reason`, and passthrough mode. While nothing is published that ambiguity is
harmless; the moment this task reports provenance it becomes a defect, because a reader cannot
tell "this run was refused" from "this arm was not recording". The published provenance must
distinguish them. The refusal strings already land in `invalid` and are `"contract:"`-prefixed;
that is the raw material, but a consumer should not have to parse prose to learn why a share is
missing.

- [ ] **Step 1: Write the failing test** — assert the JSON carries `phases_counted: true` on a recording run and that `phases` is absent (not zeroed) on a passthrough run, and that the three absence causes above are distinguishable in the output. **A zeroed phase block is indistinguishable from a fast one**; absence is the honest encoding, exactly as `SweepPoint::syncs_per_sec` became `Option<f64>` rather than `0.0`.

- [ ] **Step 2: Run to verify it fails.**

- [ ] **Step 3: Implement.** Follow `syncs_counted`'s precedent in `wire_accounting.rs` for the provenance flag's shape and doc.

- [ ] **Step 4: Run the tests.**

- [ ] **Step 5: Gates and commit.**

```bash
git commit -m "bench: publish per-phase totals with their provenance, absent rather than zeroed"
```

---

### Task 6: Run the measurement, record it, draft the trigger (a) re-reading

**Files:**
- Modify: `docs/BENCH.md`
- Create: `.superpowers/sdd/2026-08-19-phase-attribution/trigger-a-rereading.md`
- Modify: `docs/LEDGER.md`

**This task changes no code.** It runs the finished instrument and records what it says.

- [ ] **Step 1: Run both arms on a quiet box AND a loaded one**

At least 3 runs each. The `MIN_MEASURED_WINDOW` precedent is the reason for the loaded arm: a loaded box makes some plausibility gates PASS, so a quiet-box-only run hides that class of defect.

- [ ] **Step 2: Write the BENCH.md section**

Give core (batch 1) and kernel (batch ≈62) as two columns, one row per phase, each figure with provenance, plus the measured instrument overhead from Task 4. State the inference — which gap is read as redb CPU and which as kernel CPU — as prose that can be argued with, and state what the instrument does **not** resolve (redb's in-memory work that never reaches the `Disk` boundary is visible only as `gap_within_store`, and cannot be separated from allocator or page-fault time).

- [ ] **Step 3: Draft the re-reading**

Write `trigger-a-rereading.md` against the spec's three pre-committed outcomes (redb largest / present-but-dominated / negligible). Scope it the way the 2026-08-10 reading scoped itself: workload, hardware, and what it does not cover.

**Do NOT edit `DECISIONS.md`.** CLAUDE.md: "When a decision contradicts DECISIONS.md, don't silently override it — reopen the entry with the new evidence and get Noah's sign-off." The draft goes to Noah; he edits the entry.

- [ ] **Step 4: LEDGER entry, then commit**

Per the standing lesson: run `git show <hash> --numstat -- <path>` before writing any commit-hash attribution, and prefer a relative reference in any entry that may be amended.

```bash
git add docs/BENCH.md docs/LEDGER.md
git commit -m "docs: BENCH records where a batched commit's time actually goes"
```
