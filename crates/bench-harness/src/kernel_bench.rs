//! Layer 3 (spec §2): the kernel loop fed in-process through RequestSource —
//! adds scheduling, dispatch, and job execution to the core layer's cost.
//! Batch model: N pre-built requests, measured across Kernel::run.
//! Edge tooling outside the deterministic core (DESIGN §1).
//!
//! This module keeps the loop, the Kernel, the Heartbeat and the progress
//! thread, fills a `KernelObs`, and makes no arithmetic decision of its own —
//! every one of those lives in `kernel_accounting` (interlude spec §5).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

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
    let hb = Heartbeat::start();
    // Kernel::run is one blocking call over the whole batch — a watcher
    // thread reports drain progress on stderr so a slow (fsync-bound) run is
    // distinguishable from a hung one. It reads the shared response vec the
    // source already exposes: one uncontended lock every 5 s.
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
    run_outcome?;
    let _ = progress.join();
    obs.heartbeat = hb.finish();

    {
        let responses = responses.lock().expect("responses lock");
        obs.responses_recorded = responses.len() as u64;
        // Rendered here because it needs the body bytes; the decision about
        // it belongs to the accounting, which is why this records the first
        // failure rather than asserting on it (T6 amendment, change 2).
        obs.first_failure = responses
            .iter()
            .find(|(status, _)| *status != 200)
            .map(|(status, body)| format!("HTTP {status}: {}", String::from_utf8_lossy(body)));
    }
    let stats = kernel.stats();
    obs.stats_requests = stats.requests;
    obs.stats_jobs_done = stats.jobs_done;

    let a = kernel_accounting::kernel_accounting(&obs).map_err(io::Error::other)?;
    Ok((
        KernelReport {
            acked_writes_per_sec: a.acked_writes_per_sec,
            log_records_per_sec: a.log_records_per_sec,
            requests_total: obs.requests_planned,
            jobs_done: obs.stats_jobs_done,
        },
        a.invalid,
    ))
}
