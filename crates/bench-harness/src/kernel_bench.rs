//! Layer 3 (spec §2): the kernel loop fed in-process through RequestSource —
//! adds scheduling, dispatch, and job execution to the core layer's cost.
//! Batch model: N pre-built requests, measured across Kernel::run.
//! Edge tooling outside the deterministic core (DESIGN §1).
//!
//! This module keeps the loop, the Kernel, the Heartbeat and the progress
//! thread, fills a `KernelObs`, and makes no PUBLISHED arithmetic decision of
//! its own — every one of those lives in `kernel_accounting` (interlude spec
//! §5). The one division here is the progress thread's stderr chatter, which
//! is unpublished and reaches no artifact (T6 review, M-6: the shipped claim
//! was unqualified).
//!
//! **This layer's window, stated here because the artifact's methodology
//! header cannot state it** (T6 review, I-4): there is NO warmup, and the
//! window is batch-determined rather than set by `--duration` — this layer
//! reads neither flag. `obs.elapsed` spans one `Kernel::run` call, which
//! encloses two fixed costs the batch has to amortize: a `migrate_schema()`
//! at entry and a `checkpoint()` at shutdown, both fsync-bearing. The
//! published `window_secs` is that span, and `measure::MIN_MEASURED_WINDOW`
//! is what keeps those fixed costs from dominating the rate divided into it.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::device::device_bracket_sample;
use crate::kernel_accounting::{self, KernelObs};
use crate::measure::Heartbeat;
use crate::phase_accounting::{attribute, window_brackets_measured_section};
use crate::report::KernelReport;
use crate::tracing_disk::{MAX_TRACE_CAPACITY, Trace, TracingDisk};
use crate::{SLOTS, build};
use charpente::core::io::{Disk, File};
use charpente::http::{Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente::prod::{ProdClock, ProdDisk, ProdEntropy};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Measured events per request (2026-08-19 phase-attribution plan, Task 3
/// "capacity finding" — `.superpowers/sdd/2026-08-19-phase-attribution/
/// task-3-report.md`). Real runs of this exact arm, counted with a temporary
/// `eprintln!("{}", events.len())` inserted right after `trace.events()`
/// (removed before commit; never part of any committed diff):
///
/// | requests | events recorded | events/request |
/// |---|---|---|
/// | 20,000 | 522,258 | 26.11 |
/// | 50,000 (`bin/bench.rs`'s own default `--requests`) | 1,336,846 | 26.74 |
///
/// The old fixed `TRACE_CAPACITY = 1 << 20` (1,048,576) this constant
/// replaces was 27.5% UNDER the second row — a default kernel bench run
/// guaranteed at least one reallocation inside the traced window, silently
/// (see `tracing_disk::Trace::capacity_grew_reason`'s doc for what that
/// costs). 40 carries real margin over the ~26.7 measured rate (~50%
/// headroom) rather than reusing the bare figure: a different request shape
/// (larger bodies, more job fan-out) could push the real rate up without
/// this crate's own fixtures changing at all. Ruling A's
/// `capacity_grew_reason` is what makes it safe to be wrong in the other
/// direction — an estimate that undershoots now REFUSES loudly instead of
/// silently contaminating a gap bucket.
const EVENTS_PER_REQUEST_ESTIMATE: usize = 40;

/// Floor for small batches, where `requests * EVENTS_PER_REQUEST_ESTIMATE`
/// alone could undershoot this layer's fixed per-run overhead (the
/// `migrate_schema()` at entry and the `checkpoint()` at shutdown this
/// module's doc names — both fsync-bearing, both traced, neither scaling
/// with `requests`). `1 << 16` (65,536) is still generous next to any
/// realistic fixed-cost op count a single run pays once.
const MIN_TRACE_CAPACITY: usize = 1 << 16;

/// Ruling B (2026-08-19 phase-attribution plan): this arm knows `requests`
/// up front (unlike `core_bench`, which is duration-based and keeps a fixed
/// floor — see `core_bench::trace_capacity`'s doc), so its capacity is
/// derived from the actual workload rather than a magic literal.
/// `saturating_mul` rather than plain `*`: an absurd `--requests` value must
/// not panic this arithmetic before the run even starts.
///
/// Task 3 review, M-3: saturating was necessary and not sufficient. Not
/// panicking still left `--requests 200000000` deriving 8e9 events (192 GB)
/// and aborting the process on allocation failure, naming neither the flag
/// nor the trace — a hard rule 5 violation dressed as an OOM. The
/// `MAX_TRACE_CAPACITY` clamp turns that into a run that completes with valid
/// throughput numbers and a phase block withheld by a refusal that names the
/// contract, the site and the fix. See that constant's doc.
fn trace_capacity(requests: u64) -> usize {
    (requests as usize)
        .saturating_mul(EVENTS_PER_REQUEST_ESTIMATE)
        .clamp(MIN_TRACE_CAPACITY, MAX_TRACE_CAPACITY)
}

// `clamp` panics if the bounds invert. Both are consts, so prove it at compile
// time rather than discovering it from a bench run.
const _: () = assert!(MIN_TRACE_CAPACITY <= MAX_TRACE_CAPACITY);

/// Wraps `ProdDisk` to COUNT durable syncs rather than derive them (group-
/// commit T7, C-7): `KernelObs::syncs` used to be `requests_planned +
/// stats.jobs_done` on the premise "one fsync per commit, no batching" —
/// T6 falsified the `requests_planned` half of that and T7 (the job arm)
/// falsifies the other. This layer CAN count, unlike the wire layer, which
/// genuinely cannot from outside the process — so it does, through the same
/// `Disk`/`File` seam T5's `CountingDisk` (`tests/group_commit.rs`) uses,
/// rather than deriving a number `run()` no longer has any way to know is
/// still true.
struct CountingDisk {
    // Task 3 (2026-08-19 phase-attribution plan): was `ProdDisk`. Now boxed
    // so a `TracingDisk` can sit BETWEEN this wrapper and `ProdDisk` — see
    // this module's `run` for the nesting order and why it is load-bearing
    // (the tracer must time real IO, excluding this wrapper's own atomic
    // increment from the measurement). Nothing else about `CountingDisk`
    // changes: it stays unscoped, and its published `device_ratio` does not
    // move.
    inner: Box<dyn Disk>,
    syncs: Arc<AtomicU64>,
}

impl Disk for CountingDisk {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        let file = self.inner.open(path)?;
        Ok(Box::new(CountingFile {
            inner: file,
            syncs: self.syncs.clone(),
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

struct CountingFile {
    inner: Box<dyn File>,
    syncs: Arc<AtomicU64>,
}

impl File for CountingFile {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read_at(offset, out)
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write_at(offset, data)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }

    fn sync(&self) -> io::Result<()> {
        // Fix round M-2, stating this wrapper's scope explicitly (neither
        // choice below is wrong, but neither was written down):
        // EXCLUDED — `Disk::sync_dir` (the directory-entry barrier at
        // rotation/store creation/`.horizon` writes) is a separate `Disk`
        // method this wrapper delegates unwrapped, above, so it never
        // reaches this counter. INCLUDED — every `File::sync` this counts
        // covers BOTH the WAL's own fsyncs and the store's (checkpoint) and
        // `.horizon`'s, since `CountingDisk::open` wraps every path this
        // `Disk` opens, not the WAL's alone; `KernelObs::syncs`'s own doc
        // ("Durable syncs this batch paid") does not name that split either.
        self.syncs.fetch_add(1, Ordering::SeqCst);
        self.inner.sync()
    }
}

/// Which arm `run_with_trace_mode` wires the `TracingDisk` with (Task 4,
/// `bin/bench.rs`'s `--trace-overhead` mode). `Recording` is `run`'s only
/// behavior before this task, unchanged. `Passthrough` still wraps every
/// `Disk`/`File` call through `TracingDisk` and pays the same virtual
/// dispatch (`tracing_disk::Trace`'s module doc) — a caller comparing the
/// two arms' cost isolates RECORDING an event from WRAPPING every IO call,
/// rather than comparing a traced run against a genuinely untraced one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceMode {
    Recording,
    Passthrough,
}

/// This layer's real, default behavior — unchanged from before Task 4: one
/// `Trace::recording` run, sized from `requests` (`trace_capacity`).
/// `run_with_trace_mode` is the general form this delegates to; this drops
/// its third element (the recorded-op count), which no caller of `run`
/// needed before Task 4 added the first one that does
/// (`bin/bench.rs`'s `--trace-overhead`).
pub fn run(dir: &str, requests: u64) -> io::Result<(KernelReport, Vec<String>)> {
    let (report, invalid, _events_recorded) =
        run_with_trace_mode(dir, requests, TraceMode::Recording)?;
    Ok((report, invalid))
}

/// Interlude spec §4.2 (Noah ruling, 2026-08-10): this no longer takes layer
/// 1's `device.syncs_per_sec`. It brackets its own batch window instead, which
/// is the gap the shipped refusal message named in so many words — "this layer
/// takes no bracket samples of its own to show the spread".
///
/// Returns the recorded-op count as the third element (Task 4): the number
/// of times `Trace::record` was called during this run, regardless of
/// `trace_mode` — `record` early-returns before touching the event vec when
/// `trace_mode` is `Passthrough` (`tracing_disk::Trace`'s doc), so this is
/// always 0 in that arm. It is read from the SAME `trace.events()` clone the
/// phase attribution below uses, not a second call, and captured
/// unconditionally — before the `capacity_grew_reason` check — so a caller
/// measuring this trace's own cost gets a count regardless of whether
/// attribution itself was refused.
pub fn run_with_trace_mode(
    dir: &str,
    requests: u64,
    trace_mode: TraceMode,
) -> io::Result<(KernelReport, Vec<String>, u64)> {
    let sync_count = Arc::new(AtomicU64::new(0));
    // Task 3: TracingDisk INSIDE CountingDisk, ProdDisk innermost — the
    // tracer times the real IO, not CountingFile's own atomic increment.
    // Reversed, every recorded duration would include the counter's
    // overhead. ONE `Trace` per arm, not a shared static.
    let trace = match trace_mode {
        TraceMode::Recording => Trace::recording(trace_capacity(requests)),
        TraceMode::Passthrough => Trace::passthrough(),
    };
    let disk = CountingDisk {
        inner: Box::new(TracingDisk::new(
            Box::new(ProdDisk::new(dir)?),
            trace.clone(),
        )),
        syncs: sync_count.clone(),
    };
    let app = build(Box::new(disk), Box::new(ProdEntropy::new()?))?;
    let mut source = SimSource::new(Box::new(ProdClock::new()), 0);
    for i in 0..requests {
        source.push_at(0, Request::new(Method::Post, format!("/hit/{}", i % SLOTS)));
    }
    let responses = source.responses();
    let mut kernel = Kernel::new(app, Box::new(ProdClock::new()))?;

    let mut obs = KernelObs {
        requests_planned: requests,
        ..Default::default()
    };
    // The heartbeat starts BEFORE the first bracket sample and finishes after
    // the window, exactly as `core_bench` does (T5 re-review, N-2): the
    // brackets are published artifact fields, so a suspend or starvation
    // episode during either one has to be catchable too.
    let hb = Heartbeat::start();
    obs.device_before = device_bracket_sample(dir)?;
    // Kernel::run is one blocking call over the whole batch — a watcher
    // thread reports drain progress on stderr so a slow (fsync-bound) run is
    // distinguishable from a hung one. It reads the shared response vec the
    // source already exposes, taking the same mutex `SimReply::send` takes on
    // every response — so this IS contended, by construction, inside the
    // measured window (T6 review, M-6: the shipped comment called it
    // uncontended). Once every 5 s against an fsync-bound path the
    // perturbation is negligible; the word was not.
    let stop_progress = Arc::new(AtomicBool::new(false));
    let progress = {
        let (responses, stop) = (responses.clone(), stop_progress.clone());
        let started = Instant::now();
        std::thread::spawn(move || {
            // 100 ms stop-poll so the post-run join returns promptly; one
            // progress line every 50th tick (~5 s).
            let mut ticks = 0u32;
            loop {
                std::thread::sleep(Duration::from_millis(100));
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                ticks += 1;
                if ticks.is_multiple_of(50) {
                    let done = responses.lock().expect("responses lock").len();
                    eprintln!(
                        "bench: kernel {done}/{requests} requests ({:.0}/s so far)",
                        done as f64 / started.elapsed().as_secs_f64()
                    );
                }
            }
        })
    };
    // The counting window matches `obs.elapsed`'s exactly (same start/end
    // points) so `syncs` counts precisely the syncs THIS batch paid — not
    // `App::open`'s own startup syncs (before). Fix round M-1, correcting
    // this comment's old claim: there IS a shutdown checkpoint here, not
    // none — `Kernel::run` always checkpoints on a clean Shutdown unless the
    // engine is poisoned (`kernel/mod.rs`'s `run`), and this batch's fixed
    // request count always reaches Shutdown. The published number was
    // always right regardless: that checkpoint's own sync fires INSIDE this
    // same `Kernel::run` call, strictly between `syncs_before` and
    // `syncs_after` below, so it is correctly counted as part of what this
    // batch durably paid — only this comment's claim that no such
    // checkpoint exists was wrong.
    let syncs_before = sync_count.load(Ordering::SeqCst);
    // Task 3 (2026-08-19 phase-attribution plan): stamped from the trace's
    // OWN clock (`Trace::now_nanos`), the same origin every recorded `Event`
    // uses, so `phase_accounting::attribute` can compare these against event
    // timestamps directly.
    let window_start_nanos = trace.now_nanos();
    let start = Instant::now();
    let run_outcome = kernel.run(&mut source);
    obs.elapsed = start.elapsed();
    let window_end_nanos = trace.now_nanos();
    let syncs_after = sync_count.load(Ordering::SeqCst);
    stop_progress.store(true, Ordering::Relaxed);
    // Taken here, inside the heartbeat's span, but NOT unwrapped until after
    // `run_outcome?` below: a bracket failure must not mask the kernel's own
    // error, which is the more informative of the two.
    let after = device_bracket_sample(dir);
    // T6 review, M-3: finish the Heartbeat BEFORE the `?`. `Heartbeat` has no
    // `Drop` impl and `stop` is set only by `finish`, which takes `self` — so
    // returning through the `?` first dropped it unstopped and left its tick
    // thread waking every 10 ms for the life of the process. Invisible in the
    // `bench` binary (which exits immediately) and not invisible in a test
    // binary, which calls `run` directly. Finishing here also tightens the
    // heartbeat window to exclude the join below.
    obs.heartbeat = hb.finish();
    run_outcome?;
    obs.device_after = after?;
    let _ = progress.join();

    // Task 3: attribute the batch window against the trace. A refusal goes
    // to `invalid` (below) rather than discarding a run whose throughput
    // figures are still good.
    let mut invalid = Vec::new();
    // Task 3 review, M-2: see core_bench.rs's identical comment. The
    // ignored-event check below catches a window stamped too narrow; this
    // catches one stamped too wide, using `obs.elapsed` as the second,
    // independent clock.
    if let Some(reason) = window_brackets_measured_section(
        window_start_nanos,
        window_end_nanos,
        obs.elapsed.as_nanos() as u64,
    ) {
        invalid.push(reason);
    }
    // Ruling A (2026-08-19 phase-attribution plan): checked BEFORE attribute()
    // is even attempted — see core_bench.rs's identical comment for why
    // attribute() itself cannot usefully see this contamination.
    // Task 4: captured before the capacity-growth check, so a refused trace
    // still reports how many ops it recorded — see this function's own doc.
    let events = trace.events();
    let events_recorded = events.len() as u64;
    // Task 5: whether this arm was recording — checked FIRST, before
    // `capacity_grew_reason`. This is the one arm where `TraceMode::
    // Passthrough` is actually reachable (`--trace-overhead`'s other arm),
    // and before this check `attribute(&[], ...)` on a passthrough's empty
    // trace SUCCEEDED — an empty in-window set is legal (`phase_accounting`'s
    // `an_empty_trace_is_all_lead_in`), so the whole window landed in
    // `lead_in` and every other field was 0. That is a ZEROED phase block
    // wearing a healthy `Some`, exactly what this whole sub-project's
    // "absence, not zeroes" rule forbids — `phases_counted: false` withholds
    // it instead, honestly, without ever calling `attribute` on a trace that
    // was never trying to record.
    let phases_counted = trace.is_recording();
    let mut phases_absent_reason = None;
    let phases = if !phases_counted {
        phases_absent_reason = Some(crate::report::PhasesAbsentReason::NotRecording);
        None
    } else if let Some(reason) = trace.capacity_grew_reason() {
        invalid.push(reason);
        phases_absent_reason = Some(crate::report::PhasesAbsentReason::TraceRegrew);
        None
    } else {
        match attribute(&events, window_start_nanos, window_end_nanos) {
            Ok(attribution) => {
                // See core_bench.rs's identical comment: events after the window
                // mean `window_end_nanos` was stamped before the last IO
                // completed, on an arm whose window closes at the end of its run.
                if attribution.events_ignored_after_window > 0 {
                    invalid.push(format!(
                        "contract: {} traced op(s) fall after the measured window closed. On \
                         this arm the window closes at the end of the run, so this means \
                         `window_end_nanos` was stamped before the last IO completed and the \
                         published phase shares would be diluted by the ops left out. Fix: \
                         stamp `window_end_nanos` with `Trace::now_nanos()` after the measured \
                         section finishes, not before.",
                        attribution.events_ignored_after_window
                    ));
                }
                Some(attribution.totals)
            }
            Err(refusals) => {
                invalid.extend(refusals);
                phases_absent_reason = Some(crate::report::PhasesAbsentReason::AttributionRefused);
                None
            }
        }
    };

    {
        let responses = responses.lock().expect("responses lock");
        obs.responses_recorded = responses.len() as u64;
        // Rendered here because it needs the body bytes; the decision about
        // it belongs to the accounting, which is why this records the first
        // failure rather than asserting on it (T6 amendment, change 2).
        obs.first_failure =
            responses
                .iter()
                .find(|(status, _)| *status != 200)
                .map(|(status, body)| {
                    // T6 review, M-7: cap the body before it goes into a refusal
                    // message. Exposure is nil today (the "peer" is this crate's
                    // own route and bodies are capped at 1 MiB upstream), but T7's
                    // wire layer reuses this shape against a genuinely remote
                    // body, and the repo's habit is bounded_witness_message.
                    const MAX: usize = 256;
                    let text = String::from_utf8_lossy(body);
                    let shown: String = text.chars().take(MAX).collect();
                    let elided = if text.chars().count() > MAX {
                        " …(truncated)"
                    } else {
                        ""
                    };
                    format!("HTTP {status}: {shown}{elided}")
                });
    }
    let stats = kernel.stats();
    // C-2 fix (2026-08-10 group-commit interlude, T1 fix wave): a FULL
    // struct literal, not `obs.field = ...` trailing mutations — the shape
    // that let `syncs` silently stay at `KernelObs::default()`'s 0 with the
    // compiler saying nothing (review C-2). Every field of `KernelObs` is
    // already known by this point; naming them all here means a future field
    // added to the struct, or a future edit that drops `syncs`, breaks the
    // build instead of silently disabling the gate.
    obs = KernelObs {
        requests_planned: obs.requests_planned,
        responses_recorded: obs.responses_recorded,
        stats_requests: stats.requests,
        device_before: obs.device_before,
        device_after: obs.device_after,
        stats_jobs_done: stats.jobs_done,
        // COUNTED, not derived (group-commit T7, C-7): both the request arm
        // (T6) and the job arm (T7) now batch, so "one fsync per commit" is
        // false in general and a derived sum would silently understate the
        // batching win — or, worse, overstate `device_ratio`'s numerator and
        // trip the device-ratio gate on healthy batched code, which is the
        // exact failure the 2026-08-10 interlude built that gate to
        // eliminate. `syncs_before`/`syncs_after` bracket the SAME window as
        // `obs.elapsed` above.
        syncs: syncs_after - syncs_before,
        first_failure: obs.first_failure.clone(),
        elapsed: obs.elapsed,
        heartbeat: obs.heartbeat.clone(),
    };

    let a = kernel_accounting::kernel_accounting(&obs).map_err(io::Error::other)?;
    // Task 3: the phase-attribution refusals collected above join the
    // accounting layer's own non-fatal reasons, in that order — neither list
    // discards the other.
    invalid.extend(a.invalid);
    Ok((
        KernelReport {
            acked_writes_per_sec: a.acked_writes_per_sec,
            log_records_per_sec: a.log_records_per_sec,
            window_secs: a.window_secs,
            syncs_per_sec: a.syncs_per_sec,
            device_ratio: a.device_ratio,
            device_appends_per_sec_before: obs.device_before,
            device_appends_per_sec_after: obs.device_after,
            requests_total: obs.requests_planned,
            jobs_done: obs.stats_jobs_done,
            phases,
            phases_counted,
            phases_absent_reason,
            window_nanos: window_end_nanos - window_start_nanos,
            events_recorded,
        },
        invalid,
        events_recorded,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Ruling B (2026-08-19 phase-attribution plan): `trace_capacity` must
    // clear the measured 26.74 events/request rate at `bin/bench.rs`'s own
    // default `--requests` (50,000) with real margin, not merely equal or
    // exceed it — a formula that JUST clears today's measurement leaves no
    // room for the next workload to differ.
    #[test]
    fn trace_capacity_clears_the_measured_default_run_with_margin() {
        const MEASURED_EVENTS_AT_50K: usize = 1_336_846;
        let capacity = trace_capacity(50_000);
        assert!(
            capacity > MEASURED_EVENTS_AT_50K,
            "capacity {capacity} must exceed the measured 50,000-request event count \
             {MEASURED_EVENTS_AT_50K}"
        );
        assert!(
            capacity as f64 > MEASURED_EVENTS_AT_50K as f64 * 1.1,
            "capacity {capacity} should carry real margin (>10%) over the measured count \
             {MEASURED_EVENTS_AT_50K}, not merely clear it"
        );
    }

    #[test]
    fn trace_capacity_scales_with_requests() {
        assert_eq!(
            trace_capacity(50_000),
            50_000 * EVENTS_PER_REQUEST_ESTIMATE,
            "above the floor, capacity must be a direct multiple of requests"
        );
        assert!(
            trace_capacity(100) >= MIN_TRACE_CAPACITY,
            "a tiny batch must not undershoot the fixed-overhead floor"
        );
    }

    #[test]
    fn trace_capacity_does_not_panic_on_an_absurd_request_count() {
        // `saturating_mul`, not `*`: an operator-supplied `--requests` this
        // large must not panic the arithmetic before the run even starts.
        // Task 3 review, M-3: and it must not saturate to `usize::MAX`
        // either, which does not panic and does abort the process on
        // allocation failure, naming neither the flag nor the trace.
        assert_eq!(trace_capacity(u64::MAX), MAX_TRACE_CAPACITY);
        assert_eq!(trace_capacity(200_000_000), MAX_TRACE_CAPACITY);
    }

    #[test]
    fn the_clamp_leaves_every_realistic_request_count_untouched() {
        // Negative space for M-3: the clamp must be a backstop on absurd
        // input, not a silent ceiling on runs anyone actually performs.
        // `bin/bench.rs` defaults to 50,000.
        assert!(
            trace_capacity(50_000) < MAX_TRACE_CAPACITY,
            "the default run must not be clamped"
        );
        assert_eq!(
            trace_capacity(200_000),
            200_000 * EVENTS_PER_REQUEST_ESTIMATE,
            "a 4x-default run must still be sized from its workload"
        );
    }

    fn scratch(tag: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after 1970")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "bench-kernel-syncs-{tag}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir)
            .unwrap_or_else(|e| panic!("scratch dir {dir:?} creates: {e}"));
        dir.to_string_lossy().into_owned()
    }

    /// C-7 (group-commit T7, ruled into this task): `KernelObs::syncs` used
    /// to be DERIVED as `requests_planned + jobs_done` — "one fsync per
    /// commit, no batching" — and T6/T7 together falsify that premise (the
    /// request and job arms both drain a whole batch under one sync now).
    /// This is the fix's own test, per the brief: if `run` ever regresses
    /// back to deriving rather than counting, `syncs_per_sec` would equal
    /// the OLD formula exactly — literally `obs.requests_planned +
    /// stats.jobs_done`, as it did before this fix — and the wide
    /// inequality below would become a false one.
    ///
    /// `#[ignore]`d like this crate's other real drivers (`tests/harness.rs`
    /// — spawns real processes and does real durable syncs); picked up by
    /// `scripts/bench.sh --smoke`, which is `cargo test -p bench-harness --
    /// --ignored --test-threads=1`.
    #[test]
    #[ignore]
    fn syncs_are_measured_not_derived_under_batching() {
        // Large enough to clear MIN_MEASURED_WINDOW's 1s floor even though
        // batching (T6 + T7) makes this MUCH faster than the pre-batching
        // baseline that picked 500 for the crate's other kernel smoke test
        // (`tests/harness.rs`) — 500 batched requests finish in ~0.2s on
        // this device, which the degenerate-window guard correctly refuses.
        //
        // Fix round M-8: the ORIGINAL bump to 5_000 (T7 report) measured
        // ~140ms for 500 requests, i.e. ~1.4s projected for 5_000 — only
        // ~40% over the 1s floor, on the very box that motivated raising
        // this constant in the first place, and this sub-project exists to
        // make that margin SMALLER over time (T8-T10 add more batching, not
        // less). 20_000 (~4x) trades a few extra seconds of `--ignored` test
        // time for real headroom — if this ever refuses again naming the
        // floor, that is the regression this margin exists to catch, not
        // this constant sitting too close to it.
        const REQUESTS: u64 = 20_000;
        let dir = scratch("syncs");
        let (report, invalid) = run(&dir, REQUESTS).expect(
            // Fix round M-10: under batching, an ordinary SSD can drain
            // REQUESTS this size in under a second — that is now the LIKELY
            // cause of a MIN_MEASURED_WINDOW refusal here, named first; a
            // RAM-backed filesystem (this device doing no real durable
            // syncs at all) is the second, less likely possibility now.
            "kernel layer runs — a refusal here naming the minimum measured window most \
             likely means this device is simply fast enough to drain this batch in under a \
             second now that both arms batch; less likely, it means the store is not on a \
             device doing real durable syncs at all (RAM-backed filesystem?)",
        );
        let _ = std::fs::remove_dir_all(&dir);
        // Same shape as the crate's other smoke tests: the ratio gate is
        // DESIGNED to fire on short/noisy windows, so tolerate that one
        // named reason and nothing else.
        for reason in &invalid {
            assert!(
                reason.contains("device_ratio") && reason.contains("exceeds 1.0"),
                "the ratio gate legitimately fires on some short/noisy windows — but any \
                 OTHER invalid reason here means something regressed: {reason}"
            );
        }
        assert_eq!(report.jobs_done, REQUESTS, "every request's Bump ran");
        let derived_syncs_per_sec =
            (report.requests_total + report.jobs_done) as f64 / report.window_secs;
        assert!(
            report.syncs_per_sec < derived_syncs_per_sec * 0.5,
            "measured syncs/sec ({:.2}) must be well under the old derived value \
             ({derived_syncs_per_sec:.2} = (requests + jobs_done) / window) — both the \
             request and the job arm batch {REQUESTS} same-instant items under MAX_BATCH, \
             so a genuinely counted sync rate must be a small fraction of the record rate; \
             equality here means run() is back to DERIVING syncs instead of counting them",
            report.syncs_per_sec
        );
    }

    /// Task 4: `TraceMode::Passthrough` must record NOTHING (`Trace::record`
    /// early-returns before touching the event vec — `tracing_disk`'s doc)
    /// while `TraceMode::Recording` records something on any real run —
    /// `--trace-overhead`'s whole premise is that these two counts differ
    /// this way. `#[ignore]`d like this module's other real-disk driver
    /// (`syncs_are_measured_not_derived_under_batching`); picked up by
    /// `scripts/bench.sh --smoke`.
    #[test]
    #[ignore]
    fn passthrough_records_nothing_and_recording_records_something() {
        // Mirrors `syncs_are_measured_not_derived_under_batching`'s own
        // REQUESTS choice and its comment: under batching a small request
        // count drains in well under `MIN_MEASURED_WINDOW` (1s), which
        // `kernel_accounting`'s degenerate-batch check turns into a fatal
        // `Err`, not a soft `invalid` entry — this constant has to clear
        // that floor on BOTH arms, not merely avoid a slow test.
        const REQUESTS: u64 = 20_000;
        // Two gates legitimately fire on a short/noisy or loaded box, per
        // this crate's own precedent (`tests/phase_trace.rs`'s comment on
        // `measure::unstable_device_reason`, and this module's own
        // `syncs_are_measured_not_derived_under_batching`): the device-ratio
        // gate, and the device-bracket-stability gate. Anything else here
        // means something regressed.
        let known_noisy = |reason: &str| {
            (reason.contains("device_ratio") && reason.contains("exceeds 1.0"))
                || reason.contains("device brackets either side of this window disagree")
        };

        let dir = scratch("trace-mode-passthrough");
        let (_, invalid, events) = run_with_trace_mode(&dir, REQUESTS, TraceMode::Passthrough)
            .expect("kernel layer runs under passthrough");
        let _ = std::fs::remove_dir_all(&dir);
        for reason in &invalid {
            assert!(
                known_noisy(reason),
                "unexpected invalid reason under passthrough: {reason}"
            );
        }
        assert_eq!(
            events, 0,
            "passthrough must record nothing regardless of workload"
        );

        let dir = scratch("trace-mode-recording");
        let (_, invalid, events) = run_with_trace_mode(&dir, REQUESTS, TraceMode::Recording)
            .expect("kernel layer runs under recording");
        let _ = std::fs::remove_dir_all(&dir);
        for reason in &invalid {
            assert!(
                known_noisy(reason),
                "unexpected invalid reason under recording: {reason}"
            );
        }
        assert!(
            events > 0,
            "recording must record at least one op for a real run of {REQUESTS} requests"
        );
    }
}
