//! The accounting half of layer 2 (interlude spec
//! docs/superpowers/specs/2026-08-08-bench-accounting-split-design.md): every
//! delta, guard, rate division and INVALID decision the core layer makes, as
//! pure functions over observations that were already taken.
//!
//! Nothing here reads a clock, touches a database, a disk or a thread, or
//! performs any IO — that is the point. Unlike every other module in this
//! crate, this one carries no blanket
//! `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]`, so hard
//! rule 1's workspace deny-list is what holds the no-clock/no-IO half of spec
//! §9.1 mechanically — `Instant::now`, `std::fs`, `std::thread` and
//! `std::net` all redden `-D warnings` here. The no-`Db`/no-`Disk` half is
//! not on the deny-list (those are charpente types) and is held by review
//! instead: adding a `Db` or `Disk` parameter to anything in this module
//! defeats the seam even though CI stays green.
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
use crate::measure::{
    RATIO_TOLERANCE_FLOOR, crosscheck, degenerate_window_reason, implausible_bracketed_ratio,
    ratio_tolerance,
};
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
    /// Durable syncs this window paid — the device-ratio gate's subject
    /// (2026-08-10 group-commit interlude, T1). `Engine::submit` today is
    /// unconditionally `append`→`sync`→`apply` with no batching, so one
    /// acked write costs exactly one fsync and this equals `ops`; a future
    /// group-commit primary path is what makes the two diverge, and this
    /// field exists so that path has an obvious place to publish its own
    /// count rather than an assumed one.
    pub syncs: u64,
    /// `Heartbeat::finish()` spanning both bracket samples and the write
    /// window. `None` = one continuous run on a settled machine.
    pub heartbeat: Option<String>,
    /// This run's own device bracket samples, either side of the write window
    /// — and, since the 2026-08-10 interlude, the ratio's DENOMINATOR rather
    /// than merely its error bar. Layer 1's one-shot `device.syncs_per_sec` was
    /// what this divided by through T8; it is a single sample from a separate
    /// invocation, and run 2 of the interlude's evidence divided 206 acked
    /// writes/s by 166 while these two read 218 and 215 (spec §3.2). The
    /// accounting takes `max(before, after)` — the worse of the two, so the
    /// ratio stays conservative rather than merely less noisy.
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
    /// The device-ratio gate's numerator (2026-08-10 group-commit interlude,
    /// T1 fix wave, I-4) — published so `device_ratio` is reconstructable
    /// from the artifact, mirroring the standing ruling behind
    /// `CoreReport::device_ratio_denominator`.
    pub syncs_per_sec: f64,
    pub device_ratio: f64,
    pub invalid: Vec<String>,
}

/// What the read half publishes, plus its non-fatal reasons.
#[derive(Debug, Clone)]
pub struct ReadAccounting {
    pub reads_per_sec: f64,
    pub invalid: Vec<String>,
}

/// `None` = plausible; `Some(reason)` = INVALID, naming BOTH candidate
/// causes rather than asserting one (the review measured 220-310 syncs/s
/// spread across otherwise-identical runs — noise comparable to the whole
/// signal the ratio is supposed to carry).
///
/// The threshold and the comparison moved to `measure::DEVICE_RATIO_SLACK` /
/// `measure::implausible_ratio` at the T6 review's M-5 (Noah ruling), leaving
/// the PROSE here where it is true: this layer's physics is one `wal.sync()`
/// per transaction, and the bracket samples this message cites exist only
/// around the core write window.
///
/// **2026-08-10 interlude (Noah ruling).** Both of the candidate causes the
/// shipped message used to *offer the reader* have since been measured, and
/// both were real, so neither is offered as a guess any more — they are fixed
/// instead. Cause (2), the op-shape mismatch, is gone: `device_bracket_sample`
/// now appends (spec §4.1). Cause (1), cross-window variance, is gone as a
/// DENOMINATOR problem: this divides by the brackets either side of its own
/// window rather than by a one-shot sample from another invocation. What
/// remains is the irreducible error in the brackets themselves, and
/// `implausible_bracketed_ratio` tolerates exactly that much and no more
/// (spec §4.3). The `1e-6` slack this replaces did not buy detection — it
/// bought a ~50% false-refusal rate at every layer whose true ratio is 1.0.
pub fn device_ratio_reason(ratio: f64, before: f64, after: f64) -> Option<String> {
    if implausible_bracketed_ratio(ratio, before, after) {
        let tolerance = ratio_tolerance(before, after);
        let ceiling = before.max(after);
        Some(format!(
            "INVALID: device_ratio {ratio:.3} exceeds 1.0 by more than this window's own \
             measurement error ({:.1}%) — Engine::submit performs exactly one wal.sync() per \
             transaction with no group commit, so the commit path cannot durably ack faster \
             than the device's own sync rate. The denominator is this run's own bracket max \
             ({ceiling:.2}) from the samples either side of the write window ({before:.2} \
             before / {after:.2} after); the tolerance is the wider of those two disagreeing \
             ({:.1}%) and the {:.1}% floor for drift a bracket cannot see. A ratio this far \
             above 1.0 is not a measurement artifact the tolerance can absorb",
            tolerance * 100.0,
            crate::measure::bracket_spread(before, after) * 100.0,
            RATIO_TOLERANCE_FLOOR * 100.0,
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
        // `checked_sub`, not `a - b`: this workspace sets no `overflow-checks`
        // override, so release inherits `false` and a bare subtraction would
        // WRAP rather than trap — publishing ~1.8e19 records/sec and letting the
        // run continue behind a non-fatal INVALID, in exactly the build mode a
        // real benchmark uses. Hard rule 4: the assertion stays on in release.
        (Some(b), Some(a)) => a
            .checked_sub(b)
            .unwrap_or_else(|| panic!("last_applied went backwards: {b} -> {a}")),
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

    // Interlude spec §4.2: divide by the WORSE (larger) of this window's own
    // two bracket samples, exactly as the wire layer has since T7 rev. 3 — not
    // by a one-shot `device.syncs_per_sec` from a separate invocation.
    //
    // 2026-08-10 group-commit interlude, T1: the numerator is SYNCS, not
    // acked writes. Today `Engine::submit` pays one fsync per acked write, so
    // `syncs_per_sec` equals `acked_writes_per_sec` exactly and this is a
    // no-op change in value (pinned by
    // `while_one_txn_costs_one_fsync_the_gate_input_is_unchanged` below); a
    // future group-commit primary path amortizes one fsync over N acked
    // writes, and only the sync rate stays bounded by the device's own sync
    // ceiling.
    //
    // T1 fix wave, C-2: `syncs == 0` on a window that already cleared the op
    // floor is impossible under any engine, batched or not — a nonzero batch
    // syncs at least once. Without this a producing site that leaves
    // `WriteObs::syncs` at `WriteObs::default()`'s 0 (e.g. a deleted trailing
    // assignment) makes `device_ratio` a silently-published 0.0, incapable of
    // ever refusing anything — the whole point of this gate defeated by one
    // missed field.
    if w.syncs == 0 {
        return Err(format!(
            "INVALID: the core write window completed {} ops but recorded 0 durable syncs — \
             the device-ratio gate divides by this, and a zero numerator makes it incapable \
             of refusing anything; core_bench::run must set WriteObs::syncs",
            w.ops
        ));
    }
    let syncs_per_sec = w.syncs as f64 / w.elapsed.as_secs_f64();
    let device_ratio = syncs_per_sec / w.device_before.max(w.device_after);
    if let Some(reason) = device_ratio_reason(device_ratio, w.device_before, w.device_after) {
        invalid.push(reason);
    }

    Ok(WriteAccounting {
        acked_writes_per_sec,
        log_records_per_sec,
        syncs_per_sec,
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
    // already have returned Err from the write arm. Kept for parity with
    // `device::timed_loop`'s shared floor — Noah ruling #2 asked for BOTH
    // core loops to carry it, so this arm is not removable on unreachability
    // grounds. It was the fifth assertion this sub-project found that could
    // not fail through the path that reaches it. Extracting the accounting is
    // what gives it teeth:
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
    // 0 while `reads` climbs into the millions — caught here, on run's own
    // path, not by a test-side bound that a portable rate check could not
    // substitute for.
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
    use crate::measure::MIN_MEASURED_OPS;

    /// The gate's subject is SYNCS, not records. Today they are equal (one
    /// transaction, one fsync), which is why this passes trivially before
    /// group commit — that equality is asserted by
    /// `while_one_txn_costs_one_fsync_the_gate_input_is_unchanged` below.
    /// After group commit the two diverge and only this framing stays
    /// physically meaningful.
    #[test]
    fn the_gate_compares_syncs_to_the_device_ceiling_not_records() {
        // 1000 records committed under 100 syncs, against a 120/s device ceiling.
        // Records/sec (1000) is 8.3x the ceiling and would be refused; syncs/sec
        // (100) is below it and is the honest comparison.
        let w = WriteObs {
            ops: 1000,
            loop_delta: 1000,
            syncs: 100,
            elapsed: Duration::from_secs(1),
            applied_before: Some(0),
            applied_after: Some(1000),
            device_before: 120.0,
            device_after: 120.0,
            heartbeat: None,
        };
        // M-1 fix (2026-08-10 group-commit interlude, T1 fix wave): the
        // premise, asserted rather than merely stated in prose — both
        // siblings (kernel's `..._not_log_records`, wire's
        // `..._not_the_counted_request_rate`) already carry this.
        let old_records_rate_ratio =
            w.ops as f64 / w.elapsed.as_secs_f64() / w.device_before.max(w.device_after);
        assert!(
            old_records_rate_ratio > crate::measure::RATIO_HARD_LIMIT,
            "premise: the OLD records-rate gate must have refused this, or this test \
             proves nothing about the new one — ratio {old_records_rate_ratio}"
        );
        let a = write_accounting(Duration::from_secs(1), &w).expect("must not refuse");
        assert!(
            a.invalid.is_empty(),
            "a batched run under the sync ceiling must not be refused: {:?}",
            a.invalid
        );
    }

    /// C-2 fix (2026-08-10 group-commit interlude, T1 fix wave): a producing
    /// site that leaves `syncs` at 0 must refuse, not silently publish a
    /// device_ratio of 0.0 that can never fire. RED-first per hard rule 3:
    /// this fails against code without the guard (`write_accounting` returns
    /// `Ok` with `device_ratio: 0.0` and an empty `invalid`).
    #[test]
    fn a_zero_sync_count_refuses_rather_than_silently_disabling_the_gate() {
        let mut o = healthy();
        o.write.syncs = 0;
        let err = write_accounting(o.duration, &o.write).expect_err("a zero-sync window refuses");
        assert!(err.contains("INVALID"), "got: {err}");
        assert!(err.contains("0 durable syncs"), "got: {err}");
        assert!(err.contains("WriteObs::syncs"), "must name the fix: {err}");
    }

    // T5 review, C-1 / Noah ruling #1: a physically-impossible ratio must be
    // named, not silently published. Pure function — no hardware timing
    // dependency — so this asserts the gate itself, independent of whether
    // a real run happens to reproduce a >1.0 ratio (the review saw it 2 of
    // 7 runs on a quiet machine, not reliably reproducible on demand).
    /// Interlude spec §6.3, the discriminating case. Real numbers from `bench
    /// core` run 2: 206 acked writes/s over a 1 s window, with THIS run's own
    /// brackets reading 218 and 215 — while layer 1's one-shot
    /// `device.syncs_per_sec`, sampled in a separate invocation seconds
    /// earlier, read 166. Dividing by the one-shot gives 1.239 and refuses a
    /// healthy run; dividing by the brackets beside the window gives 0.945.
    ///
    /// This is T7 rev. 3's fix, finally applied at the layer it skipped.
    #[test]
    fn the_gate_divides_by_this_runs_own_brackets_not_a_one_shot_sample() {
        let w = WriteObs {
            ops: 206,
            loop_delta: 206,
            syncs: 206, // one fsync per transaction — today's engine
            elapsed: Duration::from_secs(1),
            applied_before: None,
            applied_after: Some(205),
            heartbeat: None,
            device_before: 218.0,
            device_after: 215.0,
        };
        let a = write_accounting(Duration::from_secs(1), &w).expect("a measurement");
        assert!(
            a.invalid.is_empty(),
            "a healthy one-record-per-transact run was refused: {:?}",
            a.invalid
        );
        assert!(
            (a.device_ratio - 206.0 / 218.0).abs() < 1e-9,
            "the denominator must be max(before, after) = 218, got ratio {}",
            a.device_ratio
        );
    }

    /// T1's no-behaviour-change claim, ASSERTED rather than implied. While one
    /// transaction costs one fsync, the gate's new subject and its old one are the
    /// same number, so the published ratio must equal the records-based ratio
    /// exactly — that equality is the claim, and a test that merely ran a healthy
    /// case without asserting it would pass no matter what T1 broke.
    ///
    /// A later task makes syncs and records diverge. This test is then expected to
    /// be REPLACED by the divergence case, never silently deleted.
    #[test]
    fn while_one_txn_costs_one_fsync_the_gate_input_is_unchanged() {
        let ceiling = 250.0;
        let w = WriteObs {
            ops: 500,
            loop_delta: 500,
            syncs: 500, // one fsync per transaction — today's engine
            elapsed: Duration::from_secs(2),
            applied_before: Some(0),
            applied_after: Some(500),
            device_before: ceiling,
            device_after: ceiling,
            heartbeat: None,
        };
        let a = write_accounting(Duration::from_secs(2), &w).expect("healthy run");
        // M-4 fix (2026-08-10 group-commit interlude, T1 fix wave): derive
        // the comparand from the PUBLISHED `acked_writes_per_sec` rather than
        // recomputing with a hard-coded `/ 2.0` window — the fixture's
        // `duration` and `elapsed` happen to agree (both `Duration::from_
        // secs(2)`), so the hard-coded divisor could not see a swap between
        // the two go wrong. `a.acked_writes_per_sec / ceiling` is strictly
        // stronger and shorter (mirrors kernel_accounting's twin, which
        // already derives from a published field).
        let records_ratio = a.acked_writes_per_sec / ceiling;
        assert_eq!(
            a.device_ratio, records_ratio,
            "with syncs == records the new gate input must equal the old one exactly, \
             or T1 changed behaviour it claimed not to"
        );
        assert!(a.invalid.is_empty(), "{:?}", a.invalid);
    }

    #[test]
    fn a_ratio_beyond_this_windows_own_error_is_named_invalid() {
        // Brackets agreeing to 3.9%, so the tolerance is the 10% floor and
        // 1.15 is genuinely beyond it.
        let r = device_ratio_reason(1.15, 245.0, 255.0).expect("must flag");
        assert!(r.contains("INVALID"), "got: {r}");
        assert!(r.contains("1.150"), "must name the offending value: {r}");
        assert!(
            r.contains("one wal.sync() per transaction"),
            "must name the physics the gate rests on: {r}"
        );
        assert!(
            r.contains("255.00"),
            "must name the denominator it actually divided by: {r}"
        );
        assert!(r.contains("245.00"), "must show the before sample: {r}");
        assert!(
            r.contains("10.0%"),
            "must show the tolerance the ratio had to clear: {r}"
        );
    }

    #[test]
    fn a_ratio_inside_this_windows_own_error_is_not_flagged() {
        // The whole point of the interlude: a healthy one-record-per-op layer
        // sits at exactly 1.0, so everything in this band is expected.
        assert!(device_ratio_reason(1.0, 245.0, 255.0).is_none());
        assert!(device_ratio_reason(1.05, 245.0, 255.0).is_none());
        assert!(
            device_ratio_reason(1.0 + RATIO_TOLERANCE_FLOOR, 245.0, 255.0).is_none(),
            "the boundary itself must not flag — only strictly above it"
        );
        assert!(device_ratio_reason(0.79, 245.0, 255.0).is_none());
    }

    #[test]
    fn a_ratio_just_over_the_tolerance_is_flagged() {
        assert!(device_ratio_reason(1.0 + RATIO_TOLERANCE_FLOOR + 1e-9, 245.0, 255.0).is_some());
    }

    /// The spread term, at this layer: the SAME ratio that is refused beside
    /// stable brackets passes beside brackets that disagree by 30%, because in
    /// the second case the harness has measured that it cannot tell.
    #[test]
    fn a_wide_bracket_spread_widens_this_layers_tolerance_too() {
        assert!(device_ratio_reason(1.15, 245.0, 255.0).is_some());
        assert!(device_ratio_reason(1.15, 147.0, 215.0).is_none());
    }

    /// A plausible healthy run on this class of hardware: 7,500 acked writes
    /// over 30s (250/s against a 305/s bracket max — a ratio of 0.820,
    /// comfortably below the gate), 33M reads over 30s. Every derived quantity
    /// is exact in f64 — 250.0, 250.0 and 1_100_000.0 all are — EXCEPT
    /// `device_ratio`: 50/61 is a repeating binary fraction. Its assertion is
    /// still an equality rather than a tolerance, but it holds by
    /// bit-reproducibility, not exactness: both sides are the
    /// identically-rounded result of the same division on the same operands,
    /// since `7500.0/30.0` is exactly `250.0`. This fixture is
    /// also spec §4 bullet 4's ABOVE-the-boundary
    /// case for both windows: 7,500 and 33,000,000 are both far above
    /// `MIN_MEASURED_OPS`, and the two `a_healthy_*` tests assert neither
    /// window is flagged. The two boundary tests below cover at and under.
    fn healthy() -> CoreObservations {
        CoreObservations {
            duration: Duration::from_secs(30),
            write: WriteObs {
                ops: 7_500,
                loop_delta: 7_500,
                syncs: 7_500, // one fsync per transaction — today's engine
                elapsed: Duration::from_secs(30),
                applied_before: Some(99),
                applied_after: Some(7_599),
                heartbeat: None,
                device_before: 295.0,
                device_after: 305.0,
            },
            read: ReadObs {
                reads: 33_000_000,
                hits: 33_000_000,
                elapsed: Duration::from_secs(30),
                heartbeat: None,
            },
        }
    }

    #[test]
    fn a_healthy_write_half_publishes_and_flags_nothing() {
        let o = healthy();
        let a = write_accounting(o.duration, &o.write).expect("a healthy window publishes");
        assert!(a.invalid.is_empty(), "got: {:?}", a.invalid);
        assert_eq!(a.acked_writes_per_sec, 250.0);
        assert_eq!(
            a.log_records_per_sec, 250.0,
            "an applied delta that agrees with the ack count publishes the same rate"
        );
        assert_eq!(
            a.device_ratio,
            250.0 / 305.0,
            "the denominator is max(before, after), this run's own worse bracket"
        );
    }

    #[test]
    fn a_healthy_read_half_publishes_and_flags_nothing() {
        let o = healthy();
        let a = read_accounting(o.duration, &o.read).expect("a healthy window publishes");
        assert!(a.invalid.is_empty(), "got: {:?}", a.invalid);
        assert_eq!(a.reads_per_sec, 1_100_000.0);
    }

    // Interlude spec §4, first bullet. N-1 (Critical, T5 re-review) was a
    // two-line total function over a pair of `Option<u64>` that needed real
    // hardware and `--warmup 0` to surface, because it was welded to a
    // measurement loop. All four shapes, including the one asserted
    // unreachable.
    #[test]
    fn applied_delta_is_total_over_every_option_pair() {
        assert_eq!(applied_delta(Some(99), Some(7_599)), 7_500);
        assert_eq!(
            applied_delta(None, Some(0)),
            1,
            "seq 0 is a real record: Some(0) means ONE record applied, not zero"
        );
        assert_eq!(applied_delta(None, Some(9)), 10);
        assert_eq!(applied_delta(None, None), 0);
        assert_eq!(
            applied_delta(Some(7), Some(7)),
            0,
            "a window that applied nothing"
        );
    }

    #[test]
    #[should_panic(expected = "last_applied cannot go backwards to None")]
    fn applied_delta_refuses_a_backwards_last_applied() {
        applied_delta(Some(1), None);
    }

    /// The `(Some, Some)` sibling of the arm above, and the reason it needs an
    /// explicit check rather than Rust's arithmetic trap.
    ///
    /// The workspace sets no `overflow-checks` override — the only `[profile.*]`
    /// entries in the root `Cargo.toml` are redb's `debug-assertions`/`opt-level`
    /// ones — so release inherits `overflow-checks = false` and a bare `a - b`
    /// WRAPS there instead of panicking. Under `cargo test` alone this test would
    /// pass against the unfixed code for the wrong reason (debug traps the
    /// overflow, just with a different message), so **its teeth are in release**:
    /// `cargo test -p bench-harness --release` is where the unfixed code does not
    /// panic at all and this goes red. That is also the mode a real benchmark runs
    /// in, where the wrap would publish ~1.8e19 records/sec and let the run
    /// continue behind a non-fatal INVALID instead of stopping and naming the
    /// contract that broke (hard rule 4: assertions stay on in release).
    ///
    /// Unreachable through `core_bench::run` today — `Db::last_applied()` is
    /// monotonic and the two samples are taken in window order — so this is
    /// negative-space assertion work, not a live bug.
    #[test]
    #[should_panic(expected = "last_applied went backwards: 5 -> 3")]
    fn applied_delta_refuses_a_last_applied_that_decreased() {
        applied_delta(Some(5), Some(3));
    }

    // The pure twin of tests/harness.rs's #[ignore]d disk regression
    // `a_warmup_zero_core_write_window_reports_the_correct_log_record_count`.
    // Both are kept deliberately (spec §7): this one proves the arithmetic,
    // the disk one proves the wiring that hands it the arithmetic's inputs.
    #[test]
    fn a_warmup_zero_window_does_not_raise_a_false_contract_break() {
        // Exactly the shape of a fresh Db with `--warmup 0`: nothing applied
        // before the window, and `Some(ops - 1)` after it, because seq 0 is
        // the first record rather than the absence of one.
        let mut o = healthy();
        o.write.applied_before = None;
        o.write.applied_after = Some(o.write.ops - 1);
        let a = write_accounting(o.duration, &o.write).expect("publishes");
        assert!(
            a.invalid.is_empty(),
            "a warmup==0 run must not accuse charpente's commit path of breaking a \
             contract that did not break (hard rule 5): {:?}",
            a.invalid
        );
        assert_eq!(a.log_records_per_sec, a.acked_writes_per_sec);
    }

    // Interlude spec §4, second bullet — and the negative half is
    // `a_healthy_write_half_publishes_and_flags_nothing` above.
    #[test]
    fn an_applied_delta_that_disagrees_with_the_ack_count_is_named_invalid() {
        let mut o = healthy();
        o.write.applied_after = Some(7_600); // one record more than the acks
        let a = write_accounting(o.duration, &o.write).expect("a mismatch is named, not refused");
        assert_eq!(a.invalid.len(), 1, "got: {:?}", a.invalid);
        let r = &a.invalid[0];
        assert!(r.contains("INVALID"), "got: {r}");
        assert!(
            r.contains("7501 applied records"),
            "must name the measured delta: {r}"
        );
        assert!(
            r.contains("7500 acked writes"),
            "must name the ack count it disagrees with: {r}"
        );
        assert!(
            r.contains("exactly one effects record per transact"),
            "must name the contract that broke: {r}"
        );
    }

    // Interlude spec §4, fourth bullet — under and at the boundary for the
    // write window; `healthy()` is the above-boundary case.
    #[test]
    fn a_below_floor_write_window_refuses_and_an_at_floor_one_does_not() {
        let mut o = healthy();
        o.write.ops = MIN_MEASURED_OPS - 1;
        o.write.loop_delta = MIN_MEASURED_OPS - 1;
        let err = write_accounting(o.duration, &o.write).expect_err("below the floor refuses");
        assert!(err.contains("INVALID"), "got: {err}");
        assert!(err.contains("core write"), "must name which loop: {err}");
        assert!(err.contains("--duration"), "must name the fix: {err}");

        o.write.ops = MIN_MEASURED_OPS;
        o.write.loop_delta = MIN_MEASURED_OPS;
        o.write.applied_before = None;
        o.write.applied_after = Some(MIN_MEASURED_OPS - 1);
        let a = write_accounting(o.duration, &o.write).expect("the floor itself publishes");
        assert!(a.invalid.is_empty(), "got: {:?}", a.invalid);
    }

    // Interlude spec §4, fourth bullet — under and at the boundary for the
    // read window; `healthy()` is the above-boundary case. N-6 recorded this
    // arm as UNREACHABLE through `run` (both windows share one duration, and
    // the write floor trips three to four orders of magnitude earlier), which
    // made it the fifth assertion this sub-project found that could not fail.
    // Calling the accounting directly is what gives it teeth.
    #[test]
    fn a_below_floor_read_window_refuses_and_an_at_floor_one_does_not() {
        let mut o = healthy();
        o.read.reads = MIN_MEASURED_OPS - 1;
        o.read.hits = MIN_MEASURED_OPS - 1;
        let err = read_accounting(o.duration, &o.read).expect_err("below the floor refuses");
        assert!(err.contains("INVALID"), "got: {err}");
        assert!(err.contains("core read"), "must name which loop: {err}");
        assert!(err.contains("--duration"), "must name the fix: {err}");

        o.read.reads = MIN_MEASURED_OPS;
        o.read.hits = MIN_MEASURED_OPS;
        let a = read_accounting(o.duration, &o.read).expect("the floor itself publishes");
        assert!(a.invalid.is_empty(), "got: {:?}", a.invalid);
    }

    // Interlude spec §4, fifth bullet. The defect this catches is C-2's own
    // demonstrated mutation: a read loop reduced to `reads += 1;` with no
    // `get` call, publishing 42M "reads/sec" from a loop that touched the
    // store zero times.
    #[test]
    fn a_read_loop_that_stopped_hitting_the_store_refuses_by_name() {
        let mut o = healthy();
        o.read.hits = o.read.reads - 1;
        let err =
            read_accounting(o.duration, &o.read).expect_err("a miss against a populated store");
        assert!(err.contains("INVALID"), "got: {err}");
        assert!(
            err.contains("33000000 reads"),
            "must name the read count: {err}"
        );
        assert!(
            err.contains("32999999 were hits"),
            "must name the hit count: {err}"
        );
        assert!(
            err.contains(&format!("0..{SLOTS}")),
            "must name the key range that was populated: {err}"
        );

        o.read.hits = o.read.reads;
        read_accounting(o.duration, &o.read).expect("hits == reads publishes");
    }

    // Interlude spec §4, last bullet, and Task 3's standing note to T4-T8:
    // naming a non-finite float is `report::collect_non_finite`'s single job.
    // The accounting must not grow a local copy — two places deciding the same
    // thing is how the two renderers drift apart.
    #[test]
    fn a_zero_device_ceiling_yields_a_non_finite_ratio_left_for_the_renderer() {
        let mut o = healthy();
        // Both brackets, since the denominator is now their max.
        o.write.device_before = 0.0;
        o.write.device_after = 0.0;
        let a = write_accounting(o.duration, &o.write).expect("a non-finite ratio is no refusal");
        assert!(a.device_ratio.is_infinite(), "got: {}", a.device_ratio);
        // Checked first (T2 fix round, I-1): this is the assertion this test's
        // name and doc comment exist for, and it must not be shadowed by the
        // length check below firing first on a mutation that adds a second,
        // unrelated entry to `invalid`.
        assert!(
            !a.invalid.iter().any(|r| r.contains("not finite")),
            "the accounting must NOT name the non-finite value itself — that is \
             report::collect_non_finite's job, and a local copy here would double-report \
             it in the artifact: {:?}",
            a.invalid
        );
        // The ratio gate fires — infinity is certainly above 1.0 + slack —
        // exactly as it does today. Preserved, not new.
        assert_eq!(a.invalid.len(), 1, "got: {:?}", a.invalid);
        assert!(
            a.invalid[0].contains("device_ratio"),
            "got: {:?}",
            a.invalid
        );
    }

    // Interlude spec §3.3: reasons are pushed in a fixed order and both
    // renderers derive from the same list, so a reordering would be invisible
    // to every other assertion in this file and visible in the published
    // artifact. Force all four non-fatal write-half reasons at once and pin
    // the sequence. (The cross-half order — every write reason before the read
    // heartbeat reason — is guaranteed by `run`'s statement order and is not
    // pinned here; forcing both heartbeats to fire needs a starved machine.)
    #[test]
    fn the_write_half_pushes_its_reasons_in_a_fixed_order() {
        let mut o = healthy();
        o.write.heartbeat = Some("INVALID: heartbeat starved".to_string());
        o.write.loop_delta = o.write.ops - 1; // totals disagree
        o.write.applied_after = Some(7_600); // applied delta disagrees
        o.write.device_before = 1.0; // ratio far above 1.0
        o.write.device_after = 1.0;
        let a = write_accounting(o.duration, &o.write).expect("all four of these are non-fatal");
        let shape: Vec<&str> = a
            .invalid
            .iter()
            .map(|r| {
                if r.contains("heartbeat starved") {
                    "heartbeat"
                } else if r.contains("totals disagree") {
                    "crosscheck"
                } else if r.contains("log_records_per_sec measured") {
                    "applied"
                } else if r.contains("device_ratio") {
                    "ratio"
                } else {
                    "UNKNOWN"
                }
            })
            .collect();
        assert_eq!(
            shape,
            vec!["heartbeat", "crosscheck", "applied", "ratio"],
            "got: {:?}",
            a.invalid
        );
    }

    #[test]
    fn the_read_half_carries_its_heartbeat_reason_through() {
        let mut o = healthy();
        o.read.heartbeat = Some("INVALID: heartbeat starved".to_string());
        let a = read_accounting(o.duration, &o.read).expect("a starved heartbeat is no refusal");
        assert_eq!(a.invalid, vec!["INVALID: heartbeat starved".to_string()]);
        assert_eq!(a.reads_per_sec, 1_100_000.0);
    }
}
