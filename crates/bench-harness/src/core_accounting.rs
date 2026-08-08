//! The accounting half of layer 2 (interlude spec
//! docs/superpowers/specs/2026-08-08-bench-accounting-split-design.md): every
//! delta, guard, rate division and INVALID decision the core layer makes, as
//! pure functions over observations that were already taken.
//!
//! Nothing here reads a clock, touches a database, a disk or a thread, or
//! performs any IO — that is the point, and it is enforced rather than
//! promised: unlike every other module in this crate, this one carries no
//! blanket `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]`,
//! so hard rule 1's workspace deny-list is what holds spec §9.1.
//!
//! A benchmark measures wall-clock performance on real hardware and is
//! definitionally outside the deterministic core (DESIGN §1), so the simulator
//! cannot be its oracle. But only the *measuring* half is irreducibly
//! nondeterministic; the accounting is ordinary total functions that happened
//! to live inside a measurement loop, where the only way to exercise them was
//! to run a real benchmark. That is why N-1 — a two-line bug over a pair of
//! `Option<u64>` — needed real hardware and an unusual flag to surface. This
//! module is charpente's own central principle applied to the one crate that
//! was violating it.

use crate::SLOTS;
use crate::measure::{crosscheck, degenerate_window_reason};
use std::time::Duration;

/// What the measured write window observed. Every field is a count, a duration
/// or a verdict that was actually observed; nothing here is derived and
/// nothing here is a decision.
#[derive(Debug, Clone, Default)]
pub struct WriteObs {
    /// Ops the measured write loop completed.
    pub ops: u64,
    /// The same count taken off the loop's own index — the crosscheck's
    /// second, nominally independent source (spec §5, totals two ways).
    pub loop_delta: u64,
    /// Monotonic time the measured window actually took.
    pub elapsed: Duration,
    /// `Db::last_applied()` either side of the window. `None` means ZERO
    /// records applied; `Some(0)` means one — seq 0 itself. WAL seqs are
    /// 0-based (log.rs's `next_seq` starts at 0), which is the whole of N-1.
    pub applied_before: Option<u64>,
    pub applied_after: Option<u64>,
    /// `Heartbeat::finish()` spanning both bracket samples and the write
    /// window. `None` = one continuous run on a settled machine.
    pub heartbeat: Option<String>,
    /// The device ceiling this run divides by — `device.syncs_per_sec`, a
    /// single sample from the device layer's own separate invocation.
    pub device_ceiling: f64,
    /// This run's own device-sync bracket samples, either side of the write
    /// window, so the ratio's error bar is visible in the artifact.
    pub device_before: f64,
    pub device_after: f64,
}

/// What the measured read window observed.
#[derive(Debug, Clone, Default)]
pub struct ReadObs {
    pub reads: u64,
    pub hits: u64,
    pub elapsed: Duration,
    pub heartbeat: Option<String>,
}

/// The observations seam (interlude spec §1): the whole raw record of one
/// `core_bench::run`.
///
/// Split into halves on purpose. Spec §2 keeps the refusal points where they
/// are — `run` fills `write`, calls `write_accounting` (which may refuse
/// *before* the population loop spends seconds of real fsyncs on a run about
/// to be discarded), then fills `read` and calls `read_accounting`. So the
/// read half is still `Default` while the write accounting runs. Rather than
/// pin "the write accounting ignores the read half" with a test, each
/// accounting function takes only its own half — spec §1's "or the relevant
/// subset", made structural. `write_accounting` therefore **cannot** read a
/// read field, and that stays true when T6 adds one, with no test to
/// remember to extend. Do not add a test for it; there is nothing to test.
///
/// `duration` is the configured window rather than an observation, which is
/// why it sits here and is passed alongside a half rather than inside one:
/// both windows use the same value (`report.duration_secs` is global, spec
/// §5), and both floors name it in their refusal messages.
#[derive(Debug, Clone, Default)]
pub struct CoreObservations {
    pub duration: Duration,
    pub write: WriteObs,
    pub read: ReadObs,
}

/// What the write half publishes, plus every non-fatal reason it collected.
/// `invalid` is in push order; `run` appends it to the report's list as-is,
/// and `the_write_half_pushes_its_reasons_in_a_fixed_order` pins that order.
#[derive(Debug, Clone)]
pub struct WriteAccounting {
    pub acked_writes_per_sec: f64,
    pub log_records_per_sec: f64,
    pub device_ratio: f64,
    pub invalid: Vec<String>,
}

/// What the read half publishes, plus its non-fatal reasons.
#[derive(Debug, Clone)]
pub struct ReadAccounting {
    pub reads_per_sec: f64,
    pub invalid: Vec<String>,
}

/// How much play `device_ratio` may have above 1.0 before it is flagged (T5
/// review, C-1 / Noah ruling #1). `Engine::submit` performs exactly one
/// `wal.sync()` per transaction with no group commit, so the commit path
/// cannot durably ack faster than the device's own sync rate in fact — a
/// ratio above 1.0 is IMPOSSIBLE, not merely unlikely, so anything above it
/// is a measurement artifact worth flagging, not a real durability finding.
///
/// T5 re-review, N-4 / Noah ruling #4 (fixed here): the original 1e-2 was
/// justified as covering "floating-point/timer-resolution noise" — that
/// justification was wrong by roughly seven orders of magnitude. Over a
/// 1s `Instant`-measured window, timer resolution contributes ~1e-9 relative
/// error and f64 arithmetic ~1e-16; 1e-2 instead absorbed a band of
/// cross-window SAMPLING noise (single-sample dispersion of 220-310 syncs/s
/// was measured across otherwise-identical runs), which is precisely the
/// phenomenon this gate exists to detect. A ratio anywhere in `(1.0, 1.01]`
/// published unflagged at exit 0 was C-1's exact defect, one order of
/// magnitude smaller, still shipped. Tightened to 1e-6 — still generous for
/// genuine float/timer slop, the only cause this constant is meant to
/// excuse. The accepted cost: more short/noisy runs now exit 1; those runs
/// ARE genuinely invalid measurements, and the bracket samples
/// (device_syncs_per_sec_before/_after) are what tell the reader why, not a
/// wider slack that hides the same fact.
pub const DEVICE_RATIO_SLACK: f64 = 1e-6;

/// `None` = plausible; `Some(reason)` = INVALID, naming BOTH candidate
/// causes rather than asserting one (the review measured 220-310 syncs/s
/// spread across otherwise-identical runs — noise comparable to the whole
/// signal the ratio is supposed to carry).
pub fn device_ratio_reason(
    ratio: f64,
    denominator: f64,
    before: f64,
    after: f64,
) -> Option<String> {
    if ratio > 1.0 + DEVICE_RATIO_SLACK {
        Some(format!(
            "INVALID: device_ratio {ratio:.3} exceeds 1.0 + {DEVICE_RATIO_SLACK} — \
             Engine::submit performs exactly one wal.sync() per transaction with no group \
             commit, so the commit path cannot durably ack faster than the device's own sync \
             rate; a ratio above 1.0 is a measurement artifact rather than a real durability \
             finding. Two candidate causes, either or both: (1) cross-window variance — \
             device.syncs_per_sec ({denominator:.2}) is a single sample from a separate \
             invocation, and this run's own bracket samples ({before:.2} before / {after:.2} \
             after the write window) show the spread that single sample cannot; (2) the \
             ratio's denominator (device.syncs_per_sec, the one-byte-overwrite loop) is not \
             the same op shape as the canonical commit's WAL append — device.appends_per_sec \
             is closer and may be exceeded less, or not at all"
        ))
    } else {
        None
    }
}

/// Records applied across the measured window, total over all four shapes of
/// `(before, after)`.
///
/// T5 re-review, N-1 (Critical): WAL seqs are 0-based, so `Db::last_applied()`
/// returning `None` means "zero records applied" while `Some(0)` means "one
/// record applied" (seq 0 itself). The shipped `unwrap_or(0)` treated both the
/// same — reading "no records applied yet" (a fresh `Db`, `warmup == 0`) as
/// "record 0 already applied" — undercounting the delta by exactly one on
/// every such run, publishing a wrong `log_records_per_sec` AND raising a
/// named INVALID that accused charpente's commit path of breaking a contract
/// that did not break (hard rule 5). It is a two-line total function over a
/// pair of `Option<u64>`; it needed real hardware and `--warmup 0` to surface
/// only because it was welded to a measurement loop. That is this whole
/// interlude in one function.
pub fn applied_delta(before: Option<u64>, after: Option<u64>) -> u64 {
    match (before, after) {
        (Some(b), Some(a)) => a - b,
        (None, Some(a)) => a + 1,
        (None, None) => 0,
        (Some(_), None) => unreachable!("last_applied cannot go backwards to None"),
    }
}

/// The write half's accounting. `Err` refuses the run — the caller turns it
/// into an `io::Error` and returns before the population loop can spend
/// seconds of fsyncs on it (spec §2: the seam moves where the logic lives, not
/// when it runs). `Ok` carries the published values plus every non-fatal
/// reason in push order.
pub fn write_accounting(duration: Duration, w: &WriteObs) -> Result<WriteAccounting, String> {
    let mut invalid = Vec::new();
    if let Some(reason) = &w.heartbeat {
        invalid.push(reason.clone());
    }
    // Refuse a degenerate write window by name — parity with
    // `device::timed_loop` — before anything downstream treats the op count as
    // a real measurement (T5 review, C-3 / Noah ruling #2). The caller stops
    // its heartbeat before calling in, so this early return leaks no thread.
    if let Some(reason) = degenerate_window_reason("core write", w.ops, duration) {
        return Err(reason);
    }
    // Totals two ways (spec §5): the loop counter delta vs the grand counter.
    // Structurally vacuous today — both increment in the same statement of the
    // same single-threaded loop — kept so every layer's throughput has a
    // uniform cross-check shape; it becomes a real check only if that loop
    // ever gains concurrency (the wire/kernel layers' cross-checks are the
    // genuinely independent ones).
    if let Err(reason) = crosscheck(&[w.loop_delta], w.ops) {
        invalid.push(reason);
    }
    let acked_writes_per_sec = w.ops as f64 / w.elapsed.as_secs_f64();

    // I-3's cross-check: the canonical transaction is documented to commit
    // exactly one effects record per transact, so the applied-seq delta must
    // equal the acked-write count.
    let delta = applied_delta(w.applied_before, w.applied_after);
    let ops = w.ops;
    if delta != ops {
        invalid.push(format!(
            "INVALID: log_records_per_sec measured {delta} applied records against \
             {ops} acked writes — core_bench's canonical transaction is documented to \
             commit exactly one effects record per transact (no dispatcher runs at this \
             layer, so nothing doubles it with a job Done-mark), so these must agree; a \
             mismatch means that contract broke"
        ));
    }
    let log_records_per_sec = delta as f64 / w.elapsed.as_secs_f64();

    let device_ratio = acked_writes_per_sec / w.device_ceiling;
    if let Some(reason) = device_ratio_reason(
        device_ratio,
        w.device_ceiling,
        w.device_before,
        w.device_after,
    ) {
        invalid.push(reason);
    }

    Ok(WriteAccounting {
        acked_writes_per_sec,
        log_records_per_sec,
        device_ratio,
        invalid,
    })
}

/// The read half's accounting. Both refusals here are `Err` because both mean
/// the read number is not a measurement at all.
pub fn read_accounting(duration: Duration, r: &ReadObs) -> Result<ReadAccounting, String> {
    let mut invalid = Vec::new();
    if let Some(reason) = &r.heartbeat {
        invalid.push(reason.clone());
    }
    // T5 re-review, N-6 (Minor): this arm is UNREACHABLE through
    // `core_bench::run`'s public signature — `run` drives both windows from
    // the same `duration`, reads run on the order of 1.1M/s (needing a window
    // under ~4.5us to trip this floor) while writes run on the order of 250/s
    // (tripping the write arm above under ~20ms), three to four orders of
    // magnitude earlier, so any duration small enough to reach here would
    // already have returned Err from the write arm. It was the fifth
    // assertion this sub-project found that could not fail through the path
    // that reaches it. Extracting the accounting is what gives it teeth:
    // `a_below_floor_read_window_refuses_and_an_at_floor_one_does_not` calls
    // this function directly, so the arm is now reachable without `run`
    // needing separate read/write durations.
    if let Some(reason) = degenerate_window_reason("core read", r.reads, duration) {
        return Err(reason);
    }
    // T5 review, C-2: count HITS, not just read attempts. Every key in
    // 0..SLOTS is populated before the read window, so under a working read
    // loop every read is a hit. The mutation the review demonstrated (a loop
    // body reduced to `reads += 1;` with no `get` call, publishing 42M
    // "reads/sec" from a loop touching the store zero times) leaves `hits` at
    // 0 while `reads` climbs into the millions.
    if r.hits != r.reads {
        let (reads, hits) = (r.reads, r.hits);
        return Err(format!(
            "INVALID: the core read loop completed {reads} reads but only {hits} were hits \
             against a fully populated store (every key in 0..{SLOTS} is written before the \
             read window, spec §3) — either population failed or this loop stopped \
             exercising the store"
        ));
    }

    Ok(ReadAccounting {
        reads_per_sec: r.reads as f64 / r.elapsed.as_secs_f64(),
        invalid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // T5 review, C-1 / Noah ruling #1: a physically-impossible ratio must be
    // named, not silently published. Pure function — no hardware timing
    // dependency — so this asserts the gate itself, independent of whether
    // a real run happens to reproduce a >1.0 ratio (the review saw it 2 of
    // 7 runs on a quiet machine, not reliably reproducible on demand).
    #[test]
    fn a_ratio_above_one_plus_slack_is_named_invalid() {
        let r = device_ratio_reason(1.05, 250.0, 245.0, 255.0).expect("must flag");
        assert!(r.contains("INVALID"), "got: {r}");
        assert!(r.contains("1.050"), "must name the offending value: {r}");
        assert!(
            r.contains("cross-window variance"),
            "must name candidate cause 1: {r}"
        );
        assert!(
            r.contains("not the same op shape"),
            "must name candidate cause 2: {r}"
        );
        assert!(
            r.contains("device.syncs_per_sec"),
            "must name which operand the ratio divides by: {r}"
        );
        assert!(r.contains("245.00"), "must show the before sample: {r}");
        assert!(r.contains("255.00"), "must show the after sample: {r}");
    }

    #[test]
    fn a_ratio_at_or_below_the_slack_boundary_is_not_flagged() {
        assert!(device_ratio_reason(1.0, 250.0, 245.0, 255.0).is_none());
        assert!(
            device_ratio_reason(1.0 + DEVICE_RATIO_SLACK, 250.0, 245.0, 255.0).is_none(),
            "the boundary itself must not flag — only strictly above it"
        );
        assert!(device_ratio_reason(0.79, 250.0, 245.0, 255.0).is_none());
    }

    #[test]
    fn a_ratio_just_over_the_slack_boundary_is_flagged() {
        assert!(
            device_ratio_reason(1.0 + DEVICE_RATIO_SLACK + 0.001, 250.0, 245.0, 255.0).is_some()
        );
    }
}
