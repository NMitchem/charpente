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
/// `verdict` rejects the run. Ordinary NTP *slew* stays far below this — the
/// discipline is capped at 500 ppm, i.e. ~15 ms across a 30 s window, three
/// orders of magnitude under the slack. A `makestep` correction is a
/// different thing: it can be arbitrarily large (hours), and it is reported
/// here as a suspend. That is deliberate, not a false positive — the reason
/// string names both causes, because either one means the measured window
/// was not one continuous run on a settled machine (T4 re-review, M-E).
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
        verdict(monotonic, wall_clock, self.ticks.load(Ordering::Relaxed))
    }
}

/// The composed run verdict: suspension first, starvation second. Extracted
/// from `Heartbeat::finish` so the composition is assertable (T4 re-review,
/// M-D) — before this, transposing the two `Duration` arguments or deleting
/// the suspension arm outright left the whole suite green, because every
/// test called the two arms directly and none exercised the wiring between
/// them.
///
/// Residual, stated rather than hidden: this makes the *composition* testable,
/// not `finish`'s mapping of its own two fields onto it. Proving that last hop
/// would take an actual suspend, which is not constructible in a unit test.
pub fn verdict(monotonic: Duration, wall_clock: Duration, ticks: u64) -> Option<String> {
    suspension_verdict(monotonic, wall_clock)
        .or_else(|| heartbeat_verdict(monotonic, HEARTBEAT_INTERVAL, ticks))
}

/// Detects an actual suspend: `Instant` (`CLOCK_UPTIME_RAW` on Apple,
/// `CLOCK_MONOTONIC` on Linux) excludes suspended time; `SystemTime` does
/// not. The two diverge only across a real suspend — small NTP steps aside,
/// which `SUSPENSION_SLACK` absorbs.
pub fn suspension_verdict(monotonic: Duration, wall_clock: Duration) -> Option<String> {
    if wall_clock.saturating_sub(monotonic) > SUSPENSION_SLACK {
        // T4 re-review, M-E: this must NOT claim the rates are arithmetically
        // corrupted. Every layer divides by an `Instant`, which excludes
        // suspended time — so a suspend pauses the measurement rather than
        // inflating its denominator. The run is rejected for a different and
        // true reason: the window is no longer one continuous run on a settled
        // machine. Naming the false reason here would be the same
        // product-surface defect I-5 was reopened to fix.
        Some(format!(
            "INVALID: the suspend-inclusive clock advanced {wall_clock:?} while monotonic \
             elapsed was only {monotonic:?} — more than {SUSPENSION_SLACK:?} apart. Either \
             the process was suspended mid-run (laptop sleep) or the system clock stepped \
             forward (an NTP makestep, a resuming VM). Rates divide by the monotonic clock, \
             so they are not arithmetically wrong; the run is rejected because the measured \
             window was not one continuous run on a settled machine (cold caches, disturbed \
             thermal and scheduler state) — re-run it undisturbed"
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

/// The minimum op count a measured window must complete to publish a rate
/// (spec §5's degenerate-window floor, amended T5 review C-3). `ops == 0`
/// divides into a *finite* `0.0` rate that `report::collect_non_finite`
/// cannot catch (T4 review, I-2); an op count of `1` is the same defect one
/// step later — a rate that is really just one operation's own latency
/// inverted (T5 review, C-3, reproduced through the shipped CLI at
/// `--duration 0.004` with ratios up to 1.543). Kept low enough (not, say,
/// 1000) that it does not itself break the short `--duration`/`--warmup`
/// values this crate's own smoke tests and manual repros already use.
pub const MIN_MEASURED_OPS: u64 = 5;

/// `None` = the window is a real measurement; `Some(reason)` = refuse it,
/// don't publish it. Spec §5 also asks whether the loop "actually ran its
/// full duration" — every DURATION-BOUNDED measurement loop in this crate is
/// shaped `while Instant::now() < end { op() }`, which can only exit once
/// `Instant::now() >= end`, so `start.elapsed() >= duration` holds
/// unconditionally the moment such a loop returns. That half of the floor is
/// therefore structurally guaranteed by the loop shape itself, not something
/// this function needs to check — the floor collapses to a minimum op count.
///
/// **That reasoning is load-bearing and it is scoped** (T6 review, C-1). It
/// holds only where the operator sets the duration and the op count is the
/// observation. The kernel layer inverts exactly those roles — `--requests`
/// is the input and the window is what gets measured — so this function is
/// the WRONG floor there and `degenerate_batch_reason` below is the right
/// one. Applying this one to a batch bounds the input and leaves the window
/// unbounded, which is precisely how a five-request batch published a rate at
/// exit 0. Any future layer that is not `while now < end` must use the batch
/// floor, or state why neither applies.
pub fn degenerate_window_reason(name: &str, ops: u64, duration: Duration) -> Option<String> {
    if ops < MIN_MEASURED_OPS {
        Some(format!(
            "INVALID: the {name} loop completed only {ops} ops in a {duration:?} window \
             (need at least {MIN_MEASURED_OPS}) — raise --duration; a window this short \
             cannot produce a rate, only one (or a few) operation's own latency inverted"
        ))
    } else {
        None
    }
}

/// The minimum window a BATCH-shaped loop must span to publish a rate (T6
/// review, C-1 / Noah ruling).
///
/// `degenerate_window_reason` above collapses to an op-count floor because
/// every duration-bounded loop in this crate is shaped
/// `while Instant::now() < end { op() }`, so the window is the operator's
/// input and the op count is the observation. A batch-shaped loop inverts
/// exactly those two roles: the operator sets `--requests` and the window is
/// what gets measured. Applying an op-count floor there bounds the INPUT and
/// leaves the measured window unbounded, which is how `bench kernel
/// --requests 5` published 83 acked writes/s at exit 0 with `invalid: []`
/// from a ~46 ms window — while `--requests 4`, a window of essentially the
/// same length, refused as too short to produce a rate. Same defect as T5's
/// C-3, reproduced through the shipped CLI the same way.
///
/// One second is chosen the way `MIN_MEASURED_OPS` was: low enough not to
/// break this crate's own smokes and manual repros (the kernel smoke's 500
/// requests span several seconds on any device doing real `F_FULLFSYNC`s,
/// since this layer costs two durable syncs per request), high enough that
/// the fixed costs enclosed in the window — `Kernel::run`'s entry
/// `migrate_schema()` and its shutdown `checkpoint()` — cannot dominate the
/// rate they are divided into.
pub const MIN_MEASURED_WINDOW: Duration = Duration::from_secs(1);

/// The batch-shaped sibling of `degenerate_window_reason`: bounds the
/// measured WINDOW, and names `--requests` because that is the only knob an
/// operator has at a layer whose duration is an output (T6 review, I-1 — the
/// shipped message named `--duration`, which does not reach the kernel layer
/// at all, so an operator following it re-ran and got the byte-identical
/// refusal). The op-count floor is kept as well: a batch below
/// `MIN_MEASURED_OPS` is degenerate whatever window it spans.
pub fn degenerate_batch_reason(name: &str, ops: u64, window: Duration) -> Option<String> {
    if ops < MIN_MEASURED_OPS {
        return Some(format!(
            "INVALID: the {name} batch was only {ops} ops (need at least \
             {MIN_MEASURED_OPS}) — raise --requests; a batch this small cannot produce a \
             rate, only one (or a few) operation's own latency inverted"
        ));
    }
    if window < MIN_MEASURED_WINDOW {
        return Some(format!(
            "INVALID: the {name} batch of {ops} ops drained in {window:?}, under the \
             {MIN_MEASURED_WINDOW:?} minimum measured window — raise --requests; a window \
             this short is dominated by the fixed costs enclosed in it (schema migration at \
             entry, checkpoint at shutdown) rather than by the path being measured"
        ));
    }
    None
}

/// How much play a device-ceiling ratio may have above 1.0 before it is
/// flagged (T5 review, C-1 / Noah ruling #1; moved here from
/// `core_accounting` at the T6 review's M-5 / Noah ruling, which is what the
/// interlude spec anticipated — "the ratio gate moves to `measure.rs` the
/// moment T6 becomes its second consumer").
///
/// T5 re-review, N-4 / Noah ruling #4: the original 1e-2 was justified as
/// covering "floating-point/timer-resolution noise" — that justification was
/// wrong by roughly seven orders of magnitude. Over a 1s `Instant`-measured
/// window, timer resolution contributes ~1e-9 relative error and f64
/// arithmetic ~1e-16; 1e-2 instead absorbed a band of cross-window SAMPLING
/// noise (single-sample dispersion of 220-310 syncs/s was measured across
/// otherwise-identical runs), which is precisely the phenomenon this gate
/// exists to detect. A ratio anywhere in `(1.0, 1.01]` published unflagged at
/// exit 0 was C-1's exact defect, one order of magnitude smaller, still
/// shipped. Tightened to 1e-6 — still generous for genuine float/timer slop,
/// the only cause this constant is meant to excuse.
pub const DEVICE_RATIO_SLACK: f64 = 1e-6;

/// The gate itself, shared by every layer that has a device ceiling. What
/// differs per layer is the PHYSICS (how many durable syncs one op costs) and
/// therefore the prose, so each layer builds its own reason and calls this for
/// the decision — the rule lives here exactly once, the explanation lives
/// where it is true.
pub fn implausible_ratio(ratio: f64) -> bool {
    ratio > 1.0 + DEVICE_RATIO_SLACK
}

/// The residual error a bracket's own spread CANNOT see: both samples agreeing
/// with each other while the true rate *during* the window differed from both.
///
/// Set from measurement, not from a story (the mistake ruling #4 correctly
/// called out in the `1e-2` that preceded it): 15 `bench device` samples of the
/// same quantity on the T8 machine spanned 197.98–234.76 syncs/s about a mean
/// of 217.70, i.e. ±8.4%, rounded up. **A machine-measured value, not a
/// universal constant** — on hardware with tighter fsync timing this is loose,
/// and on noisier hardware `bracket_spread` is what carries the tolerance
/// instead.
pub const RATIO_TOLERANCE_FLOOR: f64 = 0.10;

/// Structural non-vacuity. No bracket disagreement, however wild, excuses
/// acking 50% faster than the ceiling — without this bound a sufficiently
/// unstable device would tolerate anything, which is the failure mode ruling #4
/// was right to fear. A real violation (ack-before-sync, a dropped fsync, a
/// build that lies about durability) lands at 2x or beyond and is still caught
/// at every spread this harness has ever observed.
///
/// **Its role narrowed when `MAX_BRACKET_SPREAD` landed, and the narrowing is
/// recorded rather than left for someone to rediscover.** Any point that
/// survives the stability gate has spread ≤ 0.20, so `ratio_tolerance` is
/// ≤ 0.20 and the tolerance arm binds at ≤ 1.20 — strictly below this 1.5.
/// A point that does NOT survive is refused whichever way the ratio falls.
/// So this constant no longer decides whether a POINT is published; what it
/// still decides is whether the ratio reason *also* appears alongside the
/// stability reason, which is why a wildly-spread point reports both. Left
/// as it is deliberately: it is the ruled backstop for the case where a
/// future layer brackets differently or the stability gate is relaxed, and
/// removing a backstop because a newer guard currently shadows it is how the
/// newer guard's own regression becomes silent.
pub const RATIO_HARD_LIMIT: f64 = 1.5;

/// How much the two bracket samples disagree, as a fraction of the larger.
/// This is the estimator reporting its own precision for THIS window.
///
/// Non-finite or non-positive input yields `0.0` rather than a widened
/// tolerance: a bracket that could not be measured must not be able to excuse
/// a ratio. The `collect_non_finite` scan is what reports those separately.
pub fn bracket_spread(before: f64, after: f64) -> f64 {
    let hi = before.max(after);
    if !hi.is_finite() || hi <= 0.0 {
        return 0.0;
    }
    let delta = (before - after).abs();
    if !delta.is_finite() {
        return 0.0;
    }
    delta / hi
}

/// The tolerance this point has earned: never below the floor, and above it
/// exactly as far as the harness measured itself to be imprecise here.
pub fn ratio_tolerance(before: f64, after: f64) -> f64 {
    RATIO_TOLERANCE_FLOOR.max(bracket_spread(before, after))
}

/// Beyond this, the two brackets are not two samples of one quantity — the
/// device's own capability moved during the window.
///
/// Set from measurement, in the shape ruling #4 demanded of `RATIO_TOLERANCE_FLOOR`
/// and for the same reason. **The first draft of this constant justified itself
/// with a "0–7% quiet band" and a comfortable gap above it. That was wrong, and
/// the correction is kept here because it is the whole argument.** The
/// interlude's "spreads mostly 0–7%" was read as a range when it says *mostly*.
/// Measured against runs this repo calls clean, the real ceiling is higher:
///
/// | population | worst spread | status |
/// |---|---|---|
/// | `RATIO_TOLERANCE_FLOOR`'s own 15 samples (197.98–234.76) | **15.7%** | clean |
/// | `docs/BENCH.md`'s published core row (222/258) | **14.0%** | clean, published |
/// | T10 attempt 1's three worst points | **33.5 / 34.0 / 41.6%** | contaminated |
///
/// So the admissible window is `(15.7%, 33.5%)`, not the wide gap first claimed,
/// and 0.20 is close to forced rather than comfortably centred: ~1.3x above the
/// worst spread a clean run has produced, ~1.7x below the mildest contamination.
/// **0.15 is not available** — it would refuse BENCH.md's own published core row.
/// A threshold with this little headroom is a real cost of the design, stated
/// rather than hidden: a genuinely clean run that spreads past 20% on some other
/// machine will be refused, and the fix then is to re-measure this constant on
/// that hardware, not to widen it here.
///
/// Deliberately NOT cited as evidence: T10 attempt 1's passing points. They come
/// from the contaminated run, so "the gate passed them" says nothing about
/// whether they deserved to pass.
///
/// **Machine-measured, not universal** — the same caveat `RATIO_TOLERANCE_FLOOR`
/// carries.
pub const MAX_BRACKET_SPREAD: f64 = 0.20;

/// `None` = the device held still enough to be a denominator; `Some(reason)` =
/// refuse the point.
///
/// This is deliberately independent of the ratio, and that independence is the
/// whole point. `bracket_spread` previously had exactly one consumer,
/// `ratio_tolerance`, where a wider spread could only ever WIDEN the gate —
/// so a device disintegrating under the instrument bought its own point a more
/// permissive check and could never be refused by it. A rate is attributable
/// to charpente only if the device it is divided by was one quantity for the
/// duration of the window; when it was not, no ratio, however plausible,
/// rescues the point.
///
/// Non-finite or non-positive brackets refuse here rather than pass.
/// `bracket_spread` maps them to `0.0` so they cannot widen a tolerance, which
/// is right for that job and exactly wrong for this one — 0.0 would read as
/// "perfectly stable" for a bracket that was never measured at all.
pub fn unstable_device_reason(layer: &str, before: f64, after: f64) -> Option<String> {
    if !before.is_finite() || !after.is_finite() || before <= 0.0 || after <= 0.0 {
        // Names the producing site, not an operator action: `device_bracket_sample`
        // returns a positive finite rate or an `io::Error`, so nothing an operator
        // does to the box produces this. Only an observation that was never filled
        // in reaches here, exactly like the adjacent `syncs == 0` guards.
        return Some(format!(
            "INVALID: a {layer} device bracket is not a usable measurement ({before} \
             before / {after} after) — the ratio gate divides by these, and a bracket \
             that could not be measured cannot bound anything. device_bracket_sample \
             yields a positive finite rate or an error, so this is a harness bug: the \
             {layer} layer published a window without filling in its own brackets"
        ));
    }
    let spread = bracket_spread(before, after);
    if spread > MAX_BRACKET_SPREAD {
        // Names both candidate causes rather than asserting one. Asserting "other
        // load on the box" would be the T4 M-E defect shape — a mechanism stated
        // as fact without being measured — and it would be wrong for at least one
        // recorded case: BENCH.md's own worst spread is attributed to this
        // harness's child-process spawn, which is charpente's doing, not the box's.
        Some(format!(
            "INVALID: the {layer} device brackets either side of this window disagree \
             by {:.1}% ({before:.2} before / {after:.2} after), over the {:.1}% ceiling \
             — the device's measured capability moved DURING the window, so the two \
             samples are not two readings of one quantity and the rate has no stable \
             denominator. The cause is not determined here and both candidates are \
             real: other load on the box arriving or leaving mid-run, or this harness's \
             own child spawn/teardown contending with the bracket (docs/BENCH.md records \
             a 78% spread of that second kind). A fixed-duration settle does not fix the \
             first — on a machine where load ARRIVES rather than drains, waiting longer \
             measures a busier box. Re-run it undisturbed; if it reproduces on a quiet \
             box, suspect the harness",
            spread * 100.0,
            MAX_BRACKET_SPREAD * 100.0,
        ))
    } else {
        None
    }
}

/// The bracketed gate (interlude spec §4.3), shared by every layer that
/// brackets its own window — which, after this interlude, is all three.
///
/// Read it as: *is this ratio above 1.0 by more than the error in the two
/// numbers that produced it?* A gate cannot detect a violation smaller than its
/// own measurement error; the `1e-6` this replaces did not buy that detection,
/// it bought ~50% false refusals on every layer whose true ratio is exactly 1.0
/// (spec §1).
pub fn implausible_bracketed_ratio(ratio: f64, before: f64, after: f64) -> bool {
    ratio > RATIO_HARD_LIMIT || ratio > 1.0 + ratio_tolerance(before, after)
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

    // T5 review C-3 (Noah ruling #2): the shared floor a zero-op window AND
    // a below-floor-but-nonzero window must both hit — "1 op" is the same
    // defect as "0 ops" one step later, a rate that is really just one
    // operation's own latency inverted.
    #[test]
    fn a_zero_op_window_is_below_the_floor() {
        let r = degenerate_window_reason("test", 0, Duration::from_millis(4))
            .expect("0 ops must be refused");
        assert!(r.contains("INVALID"), "got: {r}");
        assert!(r.contains("0 ops"), "got: {r}");
        assert!(r.contains("test"), "must name which loop: {r}");
        assert!(r.contains("--duration"), "must name the fix: {r}");
    }

    #[test]
    fn a_below_floor_nonzero_op_window_is_also_refused() {
        // The discriminating case the review reproduced through the shipped
        // CLI: nonzero but degenerate. A naive "ops == 0" check would pass
        // this silently.
        for ops in 1..MIN_MEASURED_OPS {
            let r = degenerate_window_reason("test", ops, Duration::from_millis(4))
                .unwrap_or_else(|| panic!("{ops} ops must still be below the floor"));
            assert!(r.contains(&format!("{ops} ops")), "got: {r}");
        }
    }

    #[test]
    fn a_window_at_or_above_the_floor_is_not_flagged() {
        assert!(
            degenerate_window_reason("test", MIN_MEASURED_OPS, Duration::from_secs(1)).is_none()
        );
        assert!(
            degenerate_window_reason("test", MIN_MEASURED_OPS * 100, Duration::from_secs(1))
                .is_none()
        );
    }

    /// T6 review, C-1. The batch floor's whole reason to exist: an op count
    /// far above `MIN_MEASURED_OPS` does NOT excuse a window too short to
    /// carry a rate. `degenerate_window_reason` returns `None` on exactly this
    /// input, which is the shipped defect in one assertion.
    #[test]
    fn a_large_batch_in_a_tiny_window_is_refused_by_the_batch_floor_only() {
        let ops = MIN_MEASURED_OPS * 1000;
        let tiny = MIN_MEASURED_WINDOW - Duration::from_millis(1);
        assert!(
            degenerate_window_reason("test", ops, tiny).is_none(),
            "the duration-bounded floor cannot see this — that is C-1"
        );
        let r = degenerate_batch_reason("test", ops, tiny).expect("the batch floor sees it");
        assert!(r.contains("INVALID"), "got: {r}");
        assert!(r.contains("--requests"), "must name the fix: {r}");
        assert!(
            !r.contains("--duration"),
            "must not name a flag a batch layer never reads: {r}"
        );
        assert!(r.contains("drained in"), "must name the window: {r}");
    }

    #[test]
    fn the_batch_floor_is_a_boundary_in_both_dimensions() {
        // Exactly at both minimums: publishes.
        assert!(
            degenerate_batch_reason("test", MIN_MEASURED_OPS, MIN_MEASURED_WINDOW).is_none(),
            "the boundary itself must publish"
        );
        // One below the op floor, with an ample window.
        let r = degenerate_batch_reason("test", MIN_MEASURED_OPS - 1, Duration::from_secs(60))
            .expect("below the op floor");
        assert!(r.contains("was only"), "the op-count arm's wording: {r}");
        assert!(r.contains("--requests"), "got: {r}");
        // One below the window floor, with an ample op count.
        let r = degenerate_batch_reason(
            "test",
            MIN_MEASURED_OPS * 1000,
            MIN_MEASURED_WINDOW - Duration::from_nanos(1),
        )
        .expect("below the window floor");
        assert!(r.contains("drained in"), "the window arm's wording: {r}");
        // The two arms are distinguishable — an operator told "only 4 ops"
        // and one told "drained in 999ms" have different things to fix.
        assert!(!r.contains("was only"), "arms must not share wording: {r}");
    }

    #[test]
    fn the_op_floor_is_checked_before_the_window_floor() {
        // Both violated: the op count is the more basic defect and wins, so
        // an operator fixes the batch size rather than chasing the window.
        let r = degenerate_batch_reason("test", 1, Duration::from_millis(1)).expect("refuses");
        assert!(r.contains("was only 1 ops"), "the op arm must win: {r}");
        assert!(!r.contains("drained in"), "got: {r}");
    }

    #[test]
    fn the_ratio_gate_is_a_boundary_at_one_plus_slack() {
        assert!(!implausible_ratio(1.0), "1.0 is possible, not flagged");
        assert!(!implausible_ratio(1.0 + DEVICE_RATIO_SLACK));
        assert!(implausible_ratio(1.0 + DEVICE_RATIO_SLACK + 1e-9));
        assert!(!implausible_ratio(0.5));
    }

    #[test]
    fn the_tolerance_is_the_floor_when_the_brackets_agree() {
        // Real numbers, wire run 1 point 1: 213/216, agreeing to 1.4%.
        let t = ratio_tolerance(213.0, 216.0);
        assert_eq!(t, RATIO_TOLERANCE_FLOOR, "spread below the floor: {t}");
    }

    #[test]
    fn the_tolerance_grows_to_the_measured_spread() {
        // Real numbers, wire run 4 point 1: 147/215, a 31.6% disagreement. The
        // harness measured that it cannot pin this window's denominator; the
        // gate must not then adjudicate a 5% overshoot.
        let t = ratio_tolerance(147.0, 215.0);
        assert!(
            t > RATIO_TOLERANCE_FLOOR,
            "a 31.6% spread must exceed the floor: {t}"
        );
        assert!((t - (215.0 - 147.0) / 215.0).abs() < 1e-12, "got: {t}");
    }

    #[test]
    fn the_spread_is_symmetric_in_its_arguments() {
        // before/after order is an accident of when the sample was taken.
        assert_eq!(bracket_spread(147.0, 215.0), bracket_spread(215.0, 147.0));
    }

    #[test]
    fn the_hard_limit_binds_however_wide_the_spread() {
        // The anti-vacuity case (spec §4.3). A device unstable enough to make
        // `spread` enormous must not thereby excuse an impossible rate.
        //
        // The ratio here is chosen to DISCRIMINATE, and the first draft of this
        // test did not: brackets of 1.0/1000.0 give a spread of 0.999, so the
        // tolerance arm alone already refuses anything above 1.999 and a probe
        // at 2.0 passed with RATIO_HARD_LIMIT deleted entirely (proven by
        // mutation). 1.7 sits BELOW 1.0 + 0.999 and ABOVE the hard limit, so
        // only the hard limit can refuse it.
        assert!(ratio_tolerance(1.0, 1000.0) > 0.9, "premise of this test");
        assert!(
            1.7 < 1.0 + ratio_tolerance(1.0, 1000.0),
            "premise: the tolerance arm must NOT fire here, or this test would \
             pass without the hard limit doing anything"
        );
        assert!(
            implausible_bracketed_ratio(1.7, 1.0, 1000.0),
            "1.7x must refuse even at a 99.9% spread — this is what keeps the \
             gate from being defeated by a noisy device"
        );
    }

    #[test]
    fn a_healthy_one_record_per_op_ratio_is_no_longer_refused() {
        // Every ratio observed across the interlude's 12 healthy wire points
        // plus the 4 core and 3 kernel runs (spec §1), against the brackets
        // measured beside them. All must pass; under the 1e-6 slack this
        // replaces, every one above 1.0 was refused.
        for (ratio, before, after) in [
            (1.086, 208.0, 198.0),
            (1.010, 208.0, 220.0),
            (1.036, 196.0, 218.0),
            (1.053, 147.0, 215.0),
            (1.072, 205.0, 211.0),
            (1.049, 213.0, 246.0),
        ] {
            assert!(
                !implausible_bracketed_ratio(ratio, before, after),
                "healthy point refused: {ratio} with brackets {before}/{after}"
            );
        }
    }

    #[test]
    fn an_unmeasurable_bracket_does_not_widen_the_tolerance() {
        // A bracket that could not be measured must not be able to EXCUSE a
        // ratio — the widest tolerance must come from a real disagreement, not
        // from a missing number. `collect_non_finite` reports these separately.
        for (b, a) in [
            (f64::NAN, 210.0),
            (210.0, f64::NAN),
            (f64::NAN, f64::NAN),
            (f64::INFINITY, 210.0),
            (0.0, 0.0),
            (-1.0, -2.0),
        ] {
            assert_eq!(
                ratio_tolerance(b, a),
                RATIO_TOLERANCE_FLOOR,
                "brackets {b}/{a} must fall back to the floor"
            );
        }
    }

    #[test]
    fn an_infinite_ratio_is_still_refused() {
        // A zero ceiling divides into infinity. This was flagged before the
        // interlude and must stay flagged: the hard limit is what catches it
        // now, and three layers' tests assert the reason exists.
        assert!(implausible_bracketed_ratio(f64::INFINITY, 0.0, 0.0));
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
        // T4 re-review, M-E: the reason must not claim an arithmetic
        // corruption that does not happen — every layer divides by `Instant`,
        // which excludes suspended time.
        assert!(
            !v.contains("meaningless"),
            "must not claim the rates are corrupted; they divide by a monotonic clock: {v}"
        );
        assert!(
            v.contains("clock stepped forward"),
            "must name the second cause that fires this arm: {v}"
        );
    }

    // T4 re-review, M-D: `Heartbeat::finish` composes the two arms, and that
    // composition was previously unreachable from any test — transposing the
    // arguments or deleting the suspension arm left the suite green. These
    // two tests are the teeth for the wiring itself, not for either arm.
    #[test]
    fn verdict_puts_suspension_ahead_of_starvation() {
        // Both arms fire: a 9s clock divergence AND zero observed ticks.
        let v = verdict(Duration::from_secs(1), Duration::from_secs(10), 0)
            .expect("both arms fire, so a reason must come back");
        assert!(v.contains("suspended"), "suspension must win: {v}");
        assert!(
            !v.contains("starved"),
            "starvation must not be the reported reason when both fire: {v}"
        );
    }

    #[test]
    fn verdict_reads_monotonic_first_and_wall_clock_second() {
        // A healthy tick count, so `heartbeat_verdict` cannot fire either way
        // and the only thing under test is the argument order.
        let healthy = (Duration::from_secs(10).as_nanos() / HEARTBEAT_INTERVAL.as_nanos()) as u64;
        assert!(
            verdict(Duration::from_secs(1), Duration::from_secs(10), healthy).is_some(),
            "monotonic 1s vs wall 10s is a suspend"
        );
        // The transposition is a wall clock BEHIND monotonic — not a suspend,
        // and with healthy ticks not starvation either. So a swapped wiring
        // turns a real suspend into a silent pass, and this asymmetry is what
        // makes the swap detectable.
        assert!(
            verdict(Duration::from_secs(10), Duration::from_secs(1), healthy).is_none(),
            "transposed arguments must NOT also return a reason, or the order is unpinned"
        );
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

    /// The T10 attempt-1 bracket pairs, recorded verbatim from that run's
    /// artifact. Seven points where the box held still, three where it did
    /// not. This is the gate's regression evidence and the reason
    /// `MAX_BRACKET_SPREAD` is 0.20 rather than a rounder-looking number.
    const T10_ATTEMPT_1_BRACKETS: &[(f64, f64, bool)] = &[
        // bench-app sweep, 1/4/16/64/256 conns
        (259.0, 258.0, false),
        (261.0, 233.0, false),
        (238.0, 224.0, false),
        (209.0, 122.0, true), // 41.6% — the worst point in the run
        (181.0, 192.0, false),
        // invoicing sweep, same conns
        (226.0, 203.0, false),
        (215.0, 143.0, true), // 33.5%
        (238.0, 157.0, true), // 34.0%
        // 18.1%. NOT evidence that 18.1% is fine: this point comes from the
        // same contaminated run, so its passing says only what the gate does,
        // never what it should have done. The clean-run anchor lives on
        // MAX_BRACKET_SPREAD's doc comment.
        (216.0, 177.0, false),
        (168.0, 181.0, false),
    ];

    #[test]
    fn the_gate_refuses_exactly_the_contaminated_points_of_t10_attempt_1() {
        for &(before, after, must_refuse) in T10_ATTEMPT_1_BRACKETS {
            let refused = unstable_device_reason("core", before, after).is_some();
            assert_eq!(
                refused,
                must_refuse,
                "brackets {before}/{after} (spread {:.1}%) — expected refuse={must_refuse}",
                bracket_spread(before, after) * 100.0,
            );
        }
    }

    /// The behaviour change this gate exists for. Before it, a wide spread
    /// could only ever WIDEN `ratio_tolerance` — a device falling apart under
    /// the instrument bought the point a more permissive gate, never a
    /// refusal. A ratio that is itself perfectly plausible must not rescue a
    /// window whose denominator moved underneath it.
    #[test]
    fn an_unstable_device_is_refused_even_when_the_ratio_looks_fine() {
        let (before, after) = (209.0, 122.0);
        let ratio = 0.392; // the kernel run's own, comfortably under 1.0
        assert!(
            !implausible_bracketed_ratio(ratio, before, after),
            "premise: this ratio passes the ratio gate",
        );
        assert!(
            unstable_device_reason("core", before, after).is_some(),
            "a plausible ratio must not excuse a device that moved mid-window",
        );
    }

    #[test]
    fn the_spread_gate_is_symmetric_and_bounded_at_the_threshold() {
        assert_eq!(
            unstable_device_reason("core", 209.0, 122.0).is_some(),
            unstable_device_reason("core", 122.0, 209.0).is_some(),
        );
        // Exactly at the threshold passes; a hair over does not.
        assert!(
            unstable_device_reason("core", 100.0, 80.0).is_none(),
            "spread 0.20"
        );
        assert!(unstable_device_reason("core", 100.0, 79.9).is_some());
    }

    /// An unmeasurable bracket must not silently pass this gate. `bracket_spread`
    /// deliberately yields 0.0 for non-finite input so it cannot WIDEN a
    /// tolerance; that same 0.0 must not be read here as "the device was stable".
    #[test]
    fn an_unmeasurable_bracket_does_not_read_as_a_stable_device() {
        assert!(unstable_device_reason("core", f64::NAN, 200.0).is_some());
        assert!(unstable_device_reason("core", 200.0, f64::INFINITY).is_some());
        assert!(unstable_device_reason("core", 0.0, 0.0).is_some());
    }

    #[test]
    fn the_refusal_names_both_brackets_and_the_threshold() {
        let reason = unstable_device_reason("kernel", 209.0, 122.0).expect("must refuse");
        // Every other refusal in this crate names its layer; this one is
        // shared across all three, so the layer arrives as an argument and
        // the message must actually spend it.
        assert!(
            reason.contains("kernel"),
            "the refusal must name its layer (hard rule 5): {reason}",
        );
        // The two brackets, the measured spread and the ceiling it broke —
        // hard rule 5 wants the contract, the site and the fix, and "the fix"
        // here is the re-run instruction (matched case-insensitively: the
        // assertion is that the message TELLS you to, not how it capitalises).
        for needle in ["209", "122", "41.6", "20.0"] {
            assert!(
                reason.contains(needle),
                "reason must name {needle:?} (hard rule 5): {reason}",
            );
        }
        assert!(
            reason.to_lowercase().contains("re-run"),
            "reason must tell the operator to re-run (hard rule 5): {reason}",
        );
    }
}
