//! Measurement discipline (spec §5): process CPU time, the heartbeat
//! sleep-guard (laptops sleep mid-run and wall clocks cannot tell — the
//! charpente measurement-method rule), percentiles, totals-two-ways.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

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

/// Widens `tv_usec` to `i64` for summing. Cfg'd the same way as the
/// `Timeval` field itself: on macOS this is a genuine widening conversion
/// (`i32 -> i64`); on non-macOS `tv_usec` is already `i64`, so the function
/// is the identity — written this way (not a bare `as i64`) because a plain
/// `as i64` cast is a no-op there and `clippy::unnecessary_cast` denies it
/// under `-D warnings`, while `i64::from` would trip `clippy::useless_conversion`
/// on the very same platform.
#[cfg(target_os = "macos")]
fn usec_to_i64(u: i32) -> i64 {
    i64::from(u)
}
#[cfg(not(target_os = "macos"))]
fn usec_to_i64(u: i64) -> i64 {
    u
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
    let usec = usec_to_i64(ru.ru_utime.tv_usec) + usec_to_i64(ru.ru_stime.tv_usec);
    Duration::from_secs((ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) as u64)
        + Duration::from_micros(usec as u64)
}

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(10);

/// How far a suspend-inclusive clock may run ahead of a monotonic one before
/// `suspension_verdict` calls it a suspend rather than ordinary clock jitter
/// or an NTP step. Suspends are seconds-to-hours; steps are milliseconds.
const SUSPENSION_SLACK: Duration = Duration::from_secs(2);

/// A ticking thread plus a suspend-inclusive wall-clock snapshot. `finish`
/// runs two independent checks (T4 review, I-5): `suspension_verdict`
/// (`Instant` vs `SystemTime` divergence — the only signal that survives a
/// real suspend, since a suspended `Instant`-based tick count is internally
/// consistent with a suspended `Instant`-based wall reading) and
/// `heartbeat_verdict` (missed ticks while the system stays awake — thread
/// starvation, not suspension). The tick thread takes `gate` on every tick;
/// holding it is the test seam that starves the heartbeat through its real
/// path.
pub struct Heartbeat {
    ticks: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    gate: Arc<Mutex<()>>,
    started: Instant,
    started_wall: SystemTime,
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
            started_wall: SystemTime::now(),
            thread: Some(thread),
        }
    }

    pub fn gate(&self) -> Arc<Mutex<()>> {
        self.gate.clone()
    }

    /// None = run valid; Some(reason) = INVALID.
    pub fn finish(mut self) -> Option<String> {
        self.stop.store(true, Ordering::Relaxed);
        let monotonic = self.started.elapsed();
        // Falls back to the monotonic reading (never flags) if the system
        // clock ran backwards — that is a clock-step case for an operator to
        // investigate separately, not this guard's job.
        let wall_clock = self.started_wall.elapsed().unwrap_or(monotonic);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
        suspension_verdict(monotonic, wall_clock).or_else(|| {
            heartbeat_verdict(
                monotonic,
                HEARTBEAT_INTERVAL,
                self.ticks.load(Ordering::Relaxed),
            )
        })
    }
}

/// Detects an actual suspend: `Instant` (`CLOCK_UPTIME_RAW` on Apple,
/// `CLOCK_MONOTONIC` on Linux) excludes suspended time; `SystemTime` does
/// not. The two diverge only across a real suspend — small NTP steps aside,
/// which `SUSPENSION_SLACK` absorbs.
pub fn suspension_verdict(monotonic: Duration, wall_clock: Duration) -> Option<String> {
    if wall_clock.saturating_sub(monotonic) > SUSPENSION_SLACK {
        Some(format!(
            "INVALID: wall-clock time ({wall_clock:?}) exceeds monotonic elapsed \
             ({monotonic:?}) by more than {SUSPENSION_SLACK:?} — the process was \
             suspended mid-run (laptop sleep?); wall-clock rates from this run are \
             meaningless"
        ))
    } else {
        None
    }
}

/// Catches the process staying AWAKE while its heartbeat thread is starved
/// (`SIGSTOP`, App Nap, severe CPU oversubscription) — NOT suspension, which
/// this ratio cannot detect (a real suspend leaves it untouched: the tick
/// thread's own sleeps exclude suspended time exactly like `Instant` does,
/// so `wall` here does not grow across a suspend either).
pub fn heartbeat_verdict(wall: Duration, interval: Duration, observed: u64) -> Option<String> {
    let expected = (wall.as_nanos() / interval.as_nanos()).max(1) as u64;
    if observed * 2 < expected {
        Some(format!(
            "INVALID: heartbeat observed {observed} ticks of ~{expected} expected over \
             {wall:?} — the heartbeat thread was starved (SIGSTOP, App Nap, or severe CPU \
             oversubscription) while the process stayed awake; rates from this run are \
             meaningless"
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
        // T4 review, I-5: this arm catches the process staying AWAKE while its
        // heartbeat thread is starved (SIGSTOP, App Nap, CPU oversubstription) —
        // not suspension, which `suspension_verdict` below detects instead (a
        // genuine sleep leaves this ratio untouched, since the tick thread's own
        // sleeps exclude suspended time exactly like `Instant` does).
        let interval = Duration::from_millis(10);
        assert!(heartbeat_verdict(Duration::from_secs(30), interval, 2900).is_none());
        let v = heartbeat_verdict(Duration::from_secs(30), interval, 100).expect("starved");
        assert!(v.contains("INVALID"), "got: {v}");
        assert!(v.contains("starved"), "got: {v}");
    }

    #[test]
    fn suspension_verdict_flags_a_wall_clock_divergence_and_passes_a_healthy_one() {
        // No divergence: healthy run, both clocks agree.
        assert!(suspension_verdict(Duration::from_secs(30), Duration::from_secs(30)).is_none());
        // Small divergence (NTP step, clock jitter) within slack: still healthy.
        assert!(suspension_verdict(Duration::from_secs(30), Duration::from_secs(31)).is_none());
        // A real suspend: monotonic time barely advances, wall clock advances
        // through the whole sleep — exactly what a laptop-sleep run looks like.
        let v = suspension_verdict(Duration::from_secs(2), Duration::from_secs(602))
            .expect("suspended");
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
