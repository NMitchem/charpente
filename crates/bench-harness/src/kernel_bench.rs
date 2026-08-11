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
use crate::report::KernelReport;
use crate::{SLOTS, build};
use charpente::http::{Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente::prod::{ProdClock, ProdDisk, ProdEntropy};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Interlude spec §4.2 (Noah ruling, 2026-08-10): this no longer takes layer
/// 1's `device.syncs_per_sec`. It brackets its own batch window instead, which
/// is the gap the shipped refusal message named in so many words — "this layer
/// takes no bracket samples of its own to show the spread".
pub fn run(dir: &str, requests: u64) -> io::Result<(KernelReport, Vec<String>)> {
    let app = build(Box::new(ProdDisk::new(dir)?), Box::new(ProdEntropy::new()?))?;
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
    let start = Instant::now();
    let run_outcome = kernel.run(&mut source);
    obs.elapsed = start.elapsed();
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
        // `Engine::submit` is one fsync per commit with no batching, so this
        // equals the log record count (the request's own effects record plus
        // its Bump's Done-mark). A future group-commit primary path is where
        // this stops being a copy of that sum.
        syncs: obs.requests_planned + stats.jobs_done,
        first_failure: obs.first_failure.clone(),
        elapsed: obs.elapsed,
        heartbeat: obs.heartbeat.clone(),
    };

    let a = kernel_accounting::kernel_accounting(&obs).map_err(io::Error::other)?;
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
        },
        a.invalid,
    ))
}
