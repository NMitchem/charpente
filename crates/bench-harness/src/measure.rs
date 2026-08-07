//! Measurement discipline (spec §5): process CPU time, the heartbeat
//! sleep-guard (laptops sleep mid-run and wall clocks cannot tell — the
//! charpente measurement-method rule), percentiles, totals-two-ways.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// `struct timeval`'s layout differs by platform, and that difference is
/// stated here rather than assumed. On 64-bit Linux, `suseconds_t` really is
/// an 8-byte `long`, so `tv_usec: i64` alone matches the ABI. On macOS/
/// Darwin, `__darwin_suseconds_t` is a 4-byte `int32_t`, and the real struct
/// is `{tv_sec: i64, tv_usec: i32, <4 bytes compiler padding>}` to keep the
/// enclosing struct 8-byte aligned — 16 bytes total either way. A bare
/// `tv_usec: i64` on macOS would only read the correct value because the
/// kernel happens to zero that padding; cfg'ing the field to its real
/// platform width plus an explicit `_pad` field makes the reliance visible
/// in the type instead of leaving it as an unstated assumption.
#[repr(C)]
struct Timeval {
    tv_sec: i64,
    #[cfg(target_os = "macos")]
    tv_usec: i32,
    #[cfg(target_os = "macos")]
    _pad: i32,
    #[cfg(not(target_os = "macos"))]
    tv_usec: i64,
}

/// ru_utime + ru_stime lead on both macOS and 64-bit Linux — Timeval's own
/// doc comment above carries the per-platform tv_usec width/padding split.
/// The 14 trailing c_longs differ in meaning but not layout across both.
/// RUSAGE_SELF is 0 on both.
#[repr(C)]
struct Rusage {
    ru_utime: Timeval,
    ru_stime: Timeval,
    pad: [i64; 14],
}

unsafe extern "C" {
    fn getrusage(who: i32, usage: *mut Rusage) -> i32;
}

/// User + system CPU time consumed by this process so far.
pub fn process_cpu() -> Duration {
    let mut ru = Rusage {
        ru_utime: Timeval {
            tv_sec: 0,
            #[cfg(target_os = "macos")]
            tv_usec: 0,
            #[cfg(target_os = "macos")]
            _pad: 0,
            #[cfg(not(target_os = "macos"))]
            tv_usec: 0,
        },
        ru_stime: Timeval {
            tv_sec: 0,
            #[cfg(target_os = "macos")]
            tv_usec: 0,
            #[cfg(target_os = "macos")]
            _pad: 0,
            #[cfg(not(target_os = "macos"))]
            tv_usec: 0,
        },
        pad: [0; 14],
    };
    let rc = unsafe { getrusage(0, &mut ru) };
    assert_eq!(
        rc, 0,
        "getrusage(RUSAGE_SELF) cannot fail with valid arguments"
    );
    // Explicit widening: tv_usec is i32 on macOS, i64 elsewhere (Timeval's
    // doc comment above states why); `as i64` on each side is a lossless
    // widen on macOS and a no-op identity cast on Linux, so this line
    // compiles correctly against either layout.
    let usec = ru.ru_utime.tv_usec as i64 + ru.ru_stime.tv_usec as i64;
    Duration::from_secs((ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) as u64)
        + Duration::from_micros(usec as u64)
}

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(10);

/// A ticking thread: if the process is suspended (laptop sleep), ticks stop
/// while wall time inflates — `finish` then marks the run INVALID. The tick
/// thread takes `gate` on every tick; holding it is the test seam that
/// starves the heartbeat through its real path.
pub struct Heartbeat {
    ticks: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    gate: Arc<Mutex<()>>,
    started: Instant,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Heartbeat {
    pub fn start() -> Heartbeat {
        let ticks = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(Mutex::new(()));
        let (t, s, g) = (ticks.clone(), stop.clone(), gate.clone());
        let thread = std::thread::spawn(move || {
            while !s.load(Ordering::Relaxed) {
                std::thread::sleep(HEARTBEAT_INTERVAL);
                drop(g.lock().expect("heartbeat gate poisoned"));
                t.fetch_add(1, Ordering::Relaxed);
            }
        });
        Heartbeat {
            ticks,
            stop,
            gate,
            started: Instant::now(),
            thread: Some(thread),
        }
    }

    pub fn gate(&self) -> Arc<Mutex<()>> {
        self.gate.clone()
    }

    /// None = run valid; Some(reason) = INVALID.
    pub fn finish(mut self) -> Option<String> {
        self.stop.store(true, Ordering::Relaxed);
        let wall = self.started.elapsed();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
        heartbeat_verdict(wall, HEARTBEAT_INTERVAL, self.ticks.load(Ordering::Relaxed))
    }
}

pub fn heartbeat_verdict(wall: Duration, interval: Duration, observed: u64) -> Option<String> {
    let expected = (wall.as_nanos() / interval.as_nanos()).max(1) as u64;
    if observed * 2 < expected {
        Some(format!(
            "INVALID: heartbeat observed {observed} ticks of ~{expected} expected over \
             {wall:?} — the process was suspended mid-run (laptop sleep?); \
             wall-clock rates from this run are meaningless"
        ))
    } else {
        None
    }
}

/// Nearest-rank percentile over an already-sorted slice (latency micros):
/// ordinal rank = `ceil(p/100 * n)`, 1-indexed, clamped into range. This is
/// the true nearest-rank convention — it rounds to the closest order
/// statistic rather than always flooring, so it cannot systematically
/// under-report a tail.
pub fn percentile(sorted: &[u64], p: f64) -> u64 {
    assert!(
        !sorted.is_empty(),
        "percentile of an empty set is meaningless"
    );
    assert!((0.0..=100.0).contains(&p), "percentile out of range: {p}");
    assert!(
        sorted.windows(2).all(|w| w[0] <= w[1]),
        "percentile input must be sorted"
    );
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[index]
}

/// Totals-two-ways (spec §5): per-thread counters vs the grand counter.
/// Disagreement is a harness bug, and the run that produced it is INVALID.
pub fn crosscheck(per_thread: &[u64], grand: u64) -> Result<u64, String> {
    let sum: u64 = per_thread.iter().sum();
    if sum == grand {
        Ok(sum)
    } else {
        Err(format!(
            "INVALID: totals disagree — per-thread sum {sum} vs grand counter {grand}; \
             this is a harness bug, not a measurement"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn percentiles_pick_the_documented_ranks() {
        let sorted: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&sorted, 0.0), 1);
        assert_eq!(percentile(&sorted, 50.0), 50);
        assert_eq!(percentile(&sorted, 99.0), 99);
        assert_eq!(percentile(&sorted, 100.0), 100);
        assert_eq!(percentile(&[7], 99.9), 7);
    }

    #[test]
    #[should_panic(expected = "percentile input must be sorted")]
    fn percentile_refuses_unsorted_input() {
        percentile(&[3, 1], 50.0);
    }

    #[test]
    fn crosscheck_accepts_agreement_and_names_disagreement() {
        assert_eq!(crosscheck(&[2, 3], 5), Ok(5));
        let err = crosscheck(&[2, 2], 5).unwrap_err();
        assert!(err.contains("totals disagree"), "got: {err}");
        assert!(err.contains("INVALID"), "got: {err}");
    }

    #[test]
    fn heartbeat_verdict_flags_a_starved_run_and_passes_a_healthy_one() {
        let interval = Duration::from_millis(10);
        assert!(heartbeat_verdict(Duration::from_secs(30), interval, 2900).is_none());
        let v = heartbeat_verdict(Duration::from_secs(30), interval, 100).expect("starved");
        assert!(v.contains("INVALID"), "got: {v}");
        assert!(v.contains("suspended"), "got: {v}");
    }

    #[test]
    fn process_cpu_advances_under_load() {
        let before = process_cpu();
        let spin_until = Instant::now() + Duration::from_millis(50);
        let mut x = 0u64;
        while Instant::now() < spin_until {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
        }
        std::hint::black_box(x);
        let delta = process_cpu() - before;
        assert!(delta >= Duration::from_millis(20), "cpu delta: {delta:?}");
    }

    #[test]
    fn live_heartbeat_ticks_when_healthy() {
        let hb = Heartbeat::start();
        std::thread::sleep(Duration::from_millis(300));
        assert!(hb.finish().is_none(), "healthy heartbeat must not flag");
    }
}
