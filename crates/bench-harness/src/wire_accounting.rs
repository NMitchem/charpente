//! The accounting half of layer 4. Interlude spec
//! docs/superpowers/specs/2026-08-08-bench-accounting-split-design.md §5 rules
//! that this layer adopts the observations seam from inception rather than
//! having it retrofitted: every floor, crosscheck, rate division and INVALID
//! decision the wire layer makes is a pure function over observations already
//! taken.
//!
//! Nothing here reads a clock, spawns a process, opens a socket or touches a
//! file. As in `kernel_accounting`, that half is enforced rather than
//! promised: this module carries no blanket
//! `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]`, so hard
//! rule 1's workspace deny-list is what holds it. The no-`Conn`/no-`Child`
//! half is NOT on the deny-list and is held by review instead — adding either
//! to anything in this module defeats the seam with CI still green.
//!
//! **What this layer's `log_records_per_sec` is, stated here because this is
//! the one place a reader can check it.** Layer 3 OBSERVES
//! `KernelStats::jobs_done`. This layer cannot see the server's log from
//! outside the process, so its record rate is DERIVED —
//! `records_per_request * throughput` — and that derivation assumes the job
//! queue is at steady state inside the window. `device_ratio` (this field's
//! own ratio against the ceiling) is published but never gated on — see
//! below for what IS gated on, which as of the 2026-08-10 group-commit
//! interlude's T1 fix wave is a DIFFERENT derived quantity, not the counted
//! rate T7 rev. 4 moved the gate to. Read `device_ratio` as "how close this
//! run came to the device ceiling, if the 2-records-per-request derivation
//! held" — informative, and above 1.0 on short windows without anything
//! being wrong.
//!
//! **The gate's subject is `syncs_per_sec`, and it is ALSO a derived
//! quantity — this reverses T7 rev. 4's ruling, deliberately, not by
//! oversight (2026-08-10 group-commit interlude, T1 fix wave, review
//! finding C-1).** A client outside the process cannot count the server's
//! `wal.sync()` calls any more than it can count its log records, so
//! `WireObs::syncs` is `Target::syncs_per_request() * throughput`
//! (`wire.rs`) — today numerically identical to `log_records_per_sec`, since
//! `Engine::submit` costs exactly one fsync per commit with no batching
//! (`Target::syncs_per_request`'s doc records the equality and when it
//! ends). T7 rev. 4 moved OFF a derived quantity because the derived record
//! rate read 1.02x-1.10x over a bracket-stable ceiling on three consecutive
//! healthy runs (see the paragraph below) and a `1.0 + 1e-6` gate turned
//! that expected derivation error into a refusal every time. What makes
//! deriving safe to gate on again is NOT a better derivation — it is the
//! SAME one — but a wider tolerance: the 2026-08-10 device-ratio-gate
//! interlude (landed before this one, §4.3 below) replaced `1.0 + 1e-6` with
//! `max(10% floor, this point's own bracket spread)` everywhere, which
//! absorbs exactly the 2-10% overshoot that broke rev. 4's premise. Read
//! `syncs_per_sec` the same way `device_ratio` above is read: sound while
//! one commit costs one fsync, and NOT sound the moment a group-commit
//! primary path makes it cost less.
//!
//! **Owed work — DISCHARGED 2026-08-14. The paragraph is kept verbatim because
//! it predicted this exactly and its warning earned its keep:** "Owed work,
//! stated so a future task does not discover this at the wire and reach for a
//! fudge factor (review C-1's own warning). This layer cannot be made
//! syncs-based FROM THE CLIENT SIDE once batching actually lands —
//! `syncs_per_request` stops being a sound multiplier the moment N commits can
//! share one fsync, and there is no way to observe that from outside the
//! process. Either a server-reported sync counter ships (surfaced over HTTP or
//! in the child's exit output), or this gate is re-scoped honestly and BENCH.md
//! says so plainly. See the group-commit plan's Task 10." All of it happened on
//! schedule: T6/T7 landed batching, and T10's first full matrix refused 8 of 8
//! contended wire points at 2.39x-28.45x the ceiling.
//!
//! **What shipped: the server-reported counter, over HTTP.** `bench-app` wraps
//! its `Disk` in `counting::CountingDisk` scoped to `wal/` and publishes the
//! tally through a `/__syncs` route; `wire::sweep_point` samples it at both
//! window edges and gates on the DELTA. Over HTTP rather than at exit because
//! `BenchChild::drop` stops the child with `child.kill()` — SIGKILL, so no exit
//! hook can run. `invoicing` mounts no such route and keeps deriving;
//! `SweepPoint::syncs_counted` records which a point is, so two numbers of
//! different evidentiary standing never sit alike in one table. The caveat
//! above — "sound while one commit costs one fsync, and NOT sound the moment a
//! group-commit primary path makes it cost less" — now applies to the derived
//! case only.
//!
//! **The fudge factor was reached for, and this warning is why it was not
//! taken.** With the counter first wired as a single cumulative read the gate
//! still refused, at 1.14-1.20x, and the tempting reading was that the device
//! bracket had stopped being an upper bound — i.e. widen
//! `RATIO_TOLERANCE_FLOOR`. It had not. `/__syncs` is monotonic since process
//! start, so one read carries the warmup's syncs and the child's startup syncs
//! while the divisor is the measured window alone: for the smoke's 300ms/2s
//! split that is exactly (2.0 + 0.3) / 2.0 = 1.15x, which is what was measured.
//! The defect was in the NEW numerator, not the old denominator. Widening the
//! tolerance to absorb it would have shipped a real bug and cost the gate a
//! fifth of its resolution permanently.
//!
//! **The ceiling is bracketed per point, not a single cross-window sample
//! (T7 rev. 3 ruling).** Rev. 2 divided by layer 1's one-shot
//! `device.syncs_per_sec` and shipped as a false-alarm generator: on real
//! hardware the wire number was stable (228-253 rec/s) while the layer-1
//! ceiling swung 198-243 syncs/s between probes, firing the gate on healthy
//! runs at margins up to 1.24x. `wire::sweep_point` now takes its own
//! `device_before`/`device_after` bracket samples with the child NOT
//! running, and `point_accounting` divides by `max(before, after)` — the
//! worse of the two, so both ratios stay conservative rather than merely
//! less noisy.
//!
//! **2026-08-10 interlude (Noah ruling).** This paragraph used to continue
//! "unlike the core layer, which publishes its own brackets as prose evidence
//! but still gates on a threaded-in ceiling". That is no longer true and the
//! difference it described was itself the bug: core and kernel now bracket
//! their own windows too, and all three layers divide by their own bracket
//! max. Two further changes land here. The bracket measures a sequential
//! APPEND rather than a one-byte rewrite (§4.1) — the shape it is bounding,
//! and ~4.2% faster on apfs, so the old shape was not an upper bound at all.
//! And the gate compares against this point's OWN measurement error rather
//! than `1.0 + 1e-6` (§4.3): invoicing commits exactly one record per
//! request, so its true ack ratio is exactly 1.0 and a zero-tolerance
//! comparison was decided by noise, refusing healthy runs about half the
//! time. bench-app hid this for the whole of T7 because 2.0
//! records/request put its ack ratio near 0.5.
//!
//! **What rev. 3 did not fix, recorded so the bracket is not credited with
//! it.** Bracketing removed the drift and thereby refuted its own premise:
//! with samples agreeing to ~1% the derived record rate STILL read 1.02x to
//! 1.10x over the ceiling on three consecutive healthy runs. The denominator
//! was never the problem — improving it alone did not fix rev. 3's own
//! symptom, and that half of the lesson still holds. What DID fix it, and
//! what rev. 4 did not have available, is the wider tolerance the
//! 2026-08-10 device-ratio-gate interlude landed afterward (§4.3 above):
//! rev. 4's own conclusion — "a derived quantity cannot be made gate-worthy
//! by improving the quantity it is divided by" — is still true of the
//! DENOMINATOR, and is superseded regarding the NUMERATOR by the paragraph
//! above: a derived numerator CAN be gate-worthy once the tolerance is wide
//! enough to absorb its known derivation error. That is exactly what T1's
//! C-1 fix relies on, and it is why the gate is back on a derived quantity
//! rather than the counted ack rate rev. 4 moved it to.

use crate::measure::{
    MIN_MEASURED_WINDOW, RATIO_TOLERANCE_FLOOR, bracket_spread, crosscheck,
    degenerate_window_reason, implausible_bracketed_ratio, percentile, ratio_tolerance,
};
use crate::report::SweepPoint;
use std::time::Duration;

/// Above this, the client's own CPU — not the server — is what the number
/// measures. Published per point rather than used as a refusal: a saturated
/// point is a real observation about this client, and the sweep's shape is
/// what makes it readable.
pub const SATURATION_FRACTION: f64 = 0.9;

#[derive(Debug, Clone, Default)]
pub struct WireObs {
    /// This point's connection count — an INPUT, and the label every reason
    /// this point raises is prefixed with.
    pub connections: usize,
    /// Per-worker measured request counts, collected through `join`.
    pub per_thread: Vec<u64>,
    /// The shared atomic every worker also bumped. Note what this pair does
    /// and does not prove: `per_thread`, `grand` and `latencies_us` are all
    /// bumped in ONE unbranched block in the worker loop, so as a check on
    /// the MEASURED PATH they are one check, not three (the T6 review's I-2
    /// finding, which applies here verbatim). They stay genuinely independent
    /// as a check on this harness's own collection plumbing — a dropped
    /// `join`, a mis-accumulated sum — which is the only thing the crosscheck
    /// is claimed to catch.
    pub grand: u64,
    /// Per-request latencies in microseconds, measured window only, in
    /// completion order. `point_accounting` sorts its own copy: the driver
    /// hands these over UNSORTED so that `percentile`'s sorted-input
    /// precondition is held by a function a unit test can reach.
    pub latencies_us: Vec<u64>,
    /// The measured window — `Instant::elapsed` across the `--duration`
    /// sleep, warmup excluded.
    pub elapsed: Duration,
    /// This process's user+system CPU across the window. The server is a
    /// separate process, so this is the CLIENT's cost and nothing else.
    pub client_cpu: Duration,
    /// This point's own device-sync bracket samples (T7 rev. 3 ruling),
    /// taken with the child NOT running — one before `spawn_target`, one
    /// after `drop(node)` — on a sibling scratch directory the child never
    /// touches. They replace layer 1's one-shot cross-window
    /// `device.syncs_per_sec` sample, which drifted between probes. Note what
    /// bracketing did NOT do: rev. 3 expected it to stop the false alarms and
    /// it did not — with the ceiling bracket-stable, the DERIVED record rate
    /// still read over 1.0 on three healthy runs, which is what moved the
    /// gate to the counted ack rate in rev. 4. See this module's doc.
    pub device_before: f64,
    pub device_after: f64,
    /// Log records committed per acked request for this target — an INPUT,
    /// the per-target constant from `Target::records_per_request` (spec §3).
    pub records_per_request: f64,
    /// Durable syncs this point paid — the device-ratio gate's subject
    /// (2026-08-10 group-commit interlude, T1). NOT a counted quantity (M-5
    /// fix, T1 fix wave — the shipped doc here claimed it was "the same
    /// counted acked-request total as `grand`", which review C-1 found wrong
    /// by 2x for `Target::BenchApp`): a client outside the process cannot
    /// count the server's `wal.sync()` calls, so `wire::sweep_point` sets
    /// this to `Target::syncs_per_request() * throughput` — today
    /// numerically identical to the derived `log_records_per_sec` above,
    /// since one commit costs one fsync with no batching on the primary
    /// path. See this module's doc for why gating on a derived quantity here
    /// is safe as of this fix (it previously was not, T7 rev. 4), and what a
    /// future group-commit primary path still owes this field (a real
    /// server-reported sync count).
    pub syncs: u64,
    /// Whether `syncs` above is a real server-reported count or the fallback
    /// derivation. **Provenance, not arithmetic** — nothing in this module
    /// branches on it, so the gate's maths is unchanged and this flag cannot
    /// alter a verdict. It exists so the report and BENCH.md can say, per
    /// target, which numbers are counted and which are derived, rather than
    /// publishing both as though they were the same kind of fact.
    ///
    /// `true` for a `bench-app` that answered `/__syncs`; `false` for
    /// `invoicing`, which mounts no such route. Under batching only the counted
    /// form is sound — a derived `syncs_per_request` assumes one fsync per
    /// commit, which is exactly what group commit removes.
    pub syncs_counted: bool,
    /// `Heartbeat::finish()` over the measured window.
    pub heartbeat: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WireAccounting {
    pub point: SweepPoint,
    pub invalid: Vec<String>,
}

/// The whole of this layer's arithmetic, and every decision it makes.
///
/// `Err` means this point is not a measurement at all, and the sweep drops it
/// fail-soft (spec §5): the points already measured are kept and the loss is
/// named. `Ok` carries the point plus any reasons that make the RUN
/// unpublishable — a non-empty `invalid` still exits 1.
pub fn point_accounting(w: &WireObs) -> Result<WireAccounting, String> {
    let n = w.connections;
    let at = |reason: String| format!("[{n} conns] {reason}");
    let ops = w.latencies_us.len() as u64;

    // Two floors, in this order, both refusing.
    //
    // The op floor is `degenerate_window_reason`'s fourth CONSUMER (device,
    // core's two windows, and this one) — sharing, not the third COPY
    // interlude spec §5 warns about. It also protects `percentile`'s
    // non-empty assert structurally: MIN_MEASURED_OPS > 0, so nothing below
    // reaches a percentile of an empty slice, and rev. 1's separate
    // empty-latency check is subsumed rather than dropped.
    //
    // The window floor compares against the same `MIN_MEASURED_WINDOW` layer
    // 3 uses but carries its own prose, on the T6 ruling's principle that the
    // RULE is shared and the PROSE is not: this layer's window IS
    // `--duration`. Without it, five ops across 256 connections can clear the
    // op floor inside a 4 ms window and publish a rate from it — the exact
    // shape of the T6 review's C-1. It is also what keeps `secs` off zero on
    // every published path, so no `inf` rate reaches `collect_non_finite`
    // through this layer's throughput division.
    if let Some(reason) = degenerate_window_reason("wire", ops, w.elapsed) {
        return Err(at(reason));
    }
    if w.elapsed < MIN_MEASURED_WINDOW {
        return Err(at(format!(
            "INVALID: the wire measured window was {:?}, under the \
             {MIN_MEASURED_WINDOW:?} minimum — raise --duration; a window this \
             short is dominated by connection ramp-up and scheduler noise \
             rather than by the path being measured",
            w.elapsed
        )));
    }

    // Fixed push order, pinned by a test: heartbeat, then the crosscheck,
    // then the ratio gate. Both renderers derive from this list, so a
    // reordering would be invisible to most assertions and visible in the
    // artifact (interlude §3.3).
    let mut invalid = Vec::new();
    if let Some(reason) = w.heartbeat.clone() {
        invalid.push(at(reason));
    }
    let total = match crosscheck(&w.per_thread, w.grand) {
        Ok(t) => t,
        Err(reason) => {
            invalid.push(at(reason));
            w.grand
        }
    };

    let secs = w.elapsed.as_secs_f64();
    let throughput = total as f64 / secs;
    let log_records_per_sec = w.records_per_request * throughput;
    // T7 rev. 3: divide by the WORSE (larger) of the two bracket samples, not
    // the min and not `before` alone — see this module's doc for why a
    // single cross-window sample made this gate a false-alarm generator.
    let ceiling = w.device_before.max(w.device_after);
    let device_ratio = log_records_per_sec / ceiling;
    // Rev. 3 bracketed the ceiling and thereby disproved the hypothesis it was
    // built on. With samples taken either side of each point the ceiling is
    // stable — 241.80/244.48, 241.92/239.95, 241.31/244.48, agreeing within
    // ~1% — and the derived record rate STILL read 1.02x, 1.04x, 1.05x, 1.07x
    // and 1.10x over it on three consecutive healthy runs. The overshoot is
    // the `records_per_request` derivation assuming a steady-state job drain
    // that a short window has not reached, exactly the second cause rev. 3's
    // own refusal prose named.
    //
    // 2026-08-10 interlude §4.3 (Noah ruling): the comparison is against this
    // point's OWN measurement error, not `1.0 + 1e-6`. invoicing commits
    // exactly one record per request, so its true ratio is exactly 1.0 and the
    // sign of a zero-tolerance comparison was decided entirely by noise — the
    // gate refused healthy runs about half the time (spec §1). bench-app never
    // showed it because 2.0 records/request put its ack ratio near 0.5.
    //
    // 2026-08-10 group-commit interlude, T1 fix wave, C-1: the numerator is
    // `w.syncs`, honestly derived (`Target::syncs_per_request() * throughput`
    // in `wire.rs`), NOT copied from `grand`/`throughput` — the shipped form
    // was wrong by 2x for `Target::BenchApp`. Today numerically identical to
    // `log_records_per_sec` above, since one commit costs one fsync with no
    // batching. This is what makes gating on a derived quantity safe again
    // (T7 rev. 4 moved OFF one for the reason in the comment above) — not a
    // better derivation, but the interlude's wider tolerance absorbing the
    // 2-10% overshoot that broke rev. 4's premise. See this module's doc for
    // the full argument and the owed work a future group-commit primary path
    // still needs (a real server-reported sync count).
    let syncs_per_sec = w.syncs as f64 / secs;
    // T1 fix wave, C-2: mirrors core_accounting's and kernel_accounting's
    // guards. `syncs == 0` on a point that already cleared both floors above
    // is impossible under any engine, batched or not — a nonzero point syncs
    // at least once.
    if w.syncs == 0 {
        return Err(at(format!(
            "INVALID: this point completed {ops} ops but recorded 0 durable syncs — the \
             device-ratio gate divides by this, and a zero numerator makes it incapable of \
             refusing anything; wire::sweep_point must set WireObs::syncs"
        )));
    }
    let ack_ratio = syncs_per_sec / ceiling;
    if implausible_bracketed_ratio(ack_ratio, w.device_before, w.device_after) {
        invalid.push(at(format!(
            // I-1 fix (T1 fix wave): print BOTH numbers rather than labelling
            // the syncs rate "acked requests/s" — the two coincide today
            // (one request, one commit, one fsync) but the label would
            // mislead the moment C-1's honest derivation makes them diverge
            // for bench-app. Neither pinning test needs touching: both match
            // on the substring "acked requests/s", which still appears here.
            "INVALID: {syncs_per_sec:.0} durable syncs/s from {throughput:.0} acked \
             requests/s is {ack_ratio:.2}x the measured device ceiling of {ceiling:.0} \
             durable appends/s, beyond this point's own measurement error ({:.1}%) — the \
             bracket samples either side of this point ({:.2} before / {:.2} after) \
             disagree by {:.1}%, and the tolerance is the wider of that and the {:.1}% \
             floor. The device cannot sync faster than its own measured ceiling, so this \
             rate is not physically possible and the run is not a measurement",
            ratio_tolerance(w.device_before, w.device_after) * 100.0,
            w.device_before,
            w.device_after,
            bracket_spread(w.device_before, w.device_after) * 100.0,
            RATIO_TOLERANCE_FLOOR * 100.0,
        )));
    }

    let mut sorted = w.latencies_us.clone();
    sorted.sort_unstable();
    let client_busy_fraction = w.client_cpu.as_secs_f64() / (n as f64 * secs);
    Ok(WireAccounting {
        point: SweepPoint {
            connections: n,
            throughput_per_sec: throughput,
            // Spec §3's pairing at this layer; see the module doc for why this
            // is a per-target derivation and not a count.
            log_records_per_sec,
            syncs_per_sec,
            // Provenance carried through unchanged: this module makes no
            // decision on it, so a counted and a derived point are gated
            // identically and only the artifact can tell them apart.
            syncs_counted: w.syncs_counted,
            device_ratio,
            device_appends_per_sec_before: w.device_before,
            device_appends_per_sec_after: w.device_after,
            p50_us: percentile(&sorted, 50.0),
            p99_us: percentile(&sorted, 99.0),
            p999_us: percentile(&sorted, 99.9),
            client_busy_fraction,
            saturated: client_busy_fraction > SATURATION_FRACTION,
        },
        invalid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::measure::MIN_MEASURED_OPS;
    use std::time::Duration;

    /// A plausible healthy point: 4 connections, 1000 requests measured over
    /// a 2s window, all totals agreeing. Every derived quantity is exact in
    /// f64 (1000/2 = 500, 2.0*500 = 1000, 1000/4000 = 0.25 — all exact
    /// binary fractions), so most assertions below are equalities rather
    /// than tolerances. `device_before`/`device_after` are equal so the
    /// bracket max collapses to the same single number rev. 2's tests were
    /// written against; the bracket-specific tests below construct their own
    /// asymmetric fixtures.
    fn healthy() -> WireObs {
        WireObs {
            connections: 4,
            per_thread: vec![250, 250, 250, 250],
            grand: 1000,
            latencies_us: vec![500u64; 1000],
            elapsed: Duration::from_secs(2),
            client_cpu: Duration::from_millis(125),
            device_before: 4000.0,
            device_after: 4000.0,
            records_per_request: 2.0,
            // One sync per acked request: the DERIVED form, which is what this
            // fixture models. ("today's engine" was the old wording here and is
            // now wrong — group commit landed, so N commits can share one
            // fsync; a counted point is the one that shows it.)
            syncs: 1000,
            syncs_counted: false,
            heartbeat: None,
        }
    }

    #[test]
    fn a_healthy_point_publishes_and_flags_nothing() {
        let a = point_accounting(&healthy()).expect("a healthy point publishes");
        assert!(a.invalid.is_empty(), "got: {:?}", a.invalid);
        assert_eq!(a.point.throughput_per_sec, 500.0);
    }

    /// The layer's headline accounting rule (spec §3, T7 rev. 2): the record
    /// rate is the per-TARGET multiple of throughput, not a hardcoded 2x
    /// (bench-app's 2.0 and invoicing's 1.0 both).
    #[test]
    fn the_record_rate_is_the_per_target_multiple_of_throughput() {
        let a = point_accounting(&healthy()).expect("publishes");
        assert_eq!(
            a.point.log_records_per_sec,
            2.0 * a.point.throughput_per_sec
        );

        let mut o = healthy();
        o.records_per_request = 1.0;
        let a = point_accounting(&o).expect("publishes");
        assert_eq!(a.point.log_records_per_sec, a.point.throughput_per_sec);
    }

    #[test]
    fn a_below_floor_point_refuses_and_an_at_floor_one_does_not() {
        let mut o = healthy();
        o.latencies_us = vec![500u64; (MIN_MEASURED_OPS - 1) as usize];
        let err = point_accounting(&o).expect_err("below the floor refuses");
        assert!(err.contains("INVALID"), "got: {err}");
        assert!(err.contains("wire"), "must name which loop: {err}");
        assert!(err.contains("--duration"), "must name the fix: {err}");

        o.latencies_us = vec![500u64; MIN_MEASURED_OPS as usize];
        let a = point_accounting(&o).expect("the floor itself publishes");
        assert!(a.invalid.is_empty(), "got: {:?}", a.invalid);
    }

    /// The T6 review's C-1 shape, at this layer: five ops across 256
    /// connections can clear the op floor inside a millisecond window, and
    /// the op floor alone cannot see that.
    #[test]
    fn a_short_window_refuses_even_when_the_op_floor_is_cleared() {
        let mut o = healthy();
        o.elapsed = MIN_MEASURED_WINDOW - Duration::from_millis(1);
        let err = point_accounting(&o).expect_err("a sub-minimum window refuses");
        assert!(err.contains("INVALID"), "got: {err}");
        assert!(err.contains("wire"), "must name which loop: {err}");
        assert!(err.contains("--duration"), "must name the fix: {err}");
        assert!(
            err.contains("wire measured window was"),
            "must name the WINDOW as the defect, not the op count: {err}"
        );
    }

    #[test]
    fn the_window_floor_admits_a_window_exactly_at_the_minimum() {
        let mut o = healthy();
        o.elapsed = MIN_MEASURED_WINDOW;
        let a = point_accounting(&o).expect("the minimum window itself publishes");
        assert!(a.invalid.is_empty(), "got: {:?}", a.invalid);
    }

    /// Both floors violated: the op floor is the more basic defect and wins,
    /// matching `degenerate_batch_reason`'s own internal precedence.
    #[test]
    fn the_op_floor_refuses_before_the_window_floor() {
        let mut o = healthy();
        o.latencies_us = vec![500u64; (MIN_MEASURED_OPS - 1) as usize];
        o.elapsed = MIN_MEASURED_WINDOW - Duration::from_millis(1);
        let err = point_accounting(&o).expect_err("refuses");
        assert!(
            err.contains(&format!("{} ops", MIN_MEASURED_OPS - 1)),
            "the op arm must win: {err}"
        );
        assert!(
            !err.contains("wire measured window was"),
            "the window arm's wording must not also appear: {err}"
        );
    }

    #[test]
    fn a_disagreeing_crosscheck_is_named_and_falls_back_to_the_grand_counter() {
        let mut o = healthy();
        o.per_thread = vec![250, 250, 250, 249]; // sums to 999, grand says 1000
        let a = point_accounting(&o).expect("a mismatch is named, not refused");
        assert_eq!(a.invalid.len(), 1, "got: {:?}", a.invalid);
        assert!(
            a.invalid[0].contains("totals disagree"),
            "got: {:?}",
            a.invalid
        );
        assert!(a.invalid[0].contains("INVALID"), "got: {:?}", a.invalid);
        assert_eq!(
            a.point.throughput_per_sec,
            o.grand as f64 / o.elapsed.as_secs_f64(),
            "must fall back to the grand counter, not the disagreeing sum"
        );
    }

    /// The gate is on `syncs_per_sec` (500/s in the fixture — `healthy()`
    /// sets `syncs` to `grand`, i.e. 1.0 syncs/request, independent of
    /// `records_per_request`'s 2.0), not on the derived record rate
    /// (1000/s). Named "ack rate" because at 1.0 syncs/request the two
    /// coincide; `the_gate_input_scales_with_syncs_per_request` below is
    /// where the two are pulled apart.
    #[test]
    fn an_ack_rate_above_the_device_ceiling_is_flagged_and_at_the_ceiling_is_not() {
        let mut o = healthy();
        // Exactly at the ceiling: 500 acked requests/s against a 500 syncs/s
        // bracket max (both samples equal, so the bracket collapses to one).
        o.device_before = 500.0;
        o.device_after = 500.0;
        let a = point_accounting(&o).expect("publishes");
        assert!(
            a.invalid.is_empty(),
            "1.0 is possible, not flagged: {:?}",
            a.invalid
        );
        // ...and the DERIVED record ratio is 2.0 here, published and pointedly
        // NOT flagged. If the gate ever moves back onto `device_ratio`, this
        // single assertion is what goes red.
        assert_eq!(a.point.device_ratio, 2.0);

        // Above it by more than the tolerance: flagged by name. The two
        // samples AGREE here, so the tolerance is the floor and 1.2x is
        // genuinely beyond it (2026-08-10 interlude §4.3).
        o.device_before = 500.0 / 1.2;
        o.device_after = o.device_before;
        let a = point_accounting(&o).expect("implausible is named, not refused");
        assert_eq!(a.invalid.len(), 1, "got: {:?}", a.invalid);
        assert!(
            a.invalid[0].contains("acked requests/s"),
            "got: {:?}",
            a.invalid
        );
        assert!(a.invalid[0].contains("INVALID"), "got: {:?}", a.invalid);
    }

    /// The gate's subject is SYNCS, not the counted request rate — mirroring
    /// core_accounting's and kernel_accounting's discriminating tests. Today
    /// the two are equal at this layer too (one acked request, one commit,
    /// one fsync), which is why
    /// `the_gate_input_scales_with_syncs_per_request`'s 1.0 case
    /// passes trivially; after group commit they diverge and only the
    /// syncs framing stays physically meaningful.
    #[test]
    fn the_gate_compares_syncs_to_the_device_ceiling_not_the_counted_request_rate() {
        let mut o = healthy();
        // 4000 requests counted (2000/s) against a 600/s ceiling would
        // refuse under the OLD ack-rate gate (ratio 3.33, past the 1.5 hard
        // limit). Set syncs to a batched-looking 1000 (500/s) — under the
        // ceiling — to prove the gate now compares syncs, not the counted
        // request rate.
        o.per_thread = vec![1000, 1000, 1000, 1000];
        o.grand = 4000;
        o.latencies_us = vec![500u64; 4000];
        o.device_before = 600.0;
        o.device_after = 600.0;
        let old_ack_rate_ratio =
            o.grand as f64 / o.elapsed.as_secs_f64() / o.device_before.max(o.device_after);
        assert!(
            old_ack_rate_ratio > crate::measure::RATIO_HARD_LIMIT,
            "premise: the OLD ack-rate gate must have refused this, or this \
             test proves nothing about the new one — ratio {old_ack_rate_ratio}"
        );
        o.syncs = 1000;
        let a = point_accounting(&o).expect("must not refuse");
        assert!(
            a.invalid.is_empty(),
            "a batched point under the sync ceiling must not be refused: {:?}",
            a.invalid
        );
    }

    /// I-2 fix (2026-08-10 group-commit interlude, T1 fix wave): replaces
    /// `while_one_request_costs_one_fsync_the_gate_verdict_is_unchanged`,
    /// which the review found was a no-op duplicate of
    /// `an_ack_rate_above_the_device_ceiling_is_flagged_and_at_the_ceiling_is_not`
    /// — `healthy()` already has `syncs == grand`, so `o.syncs = o.grand;`
    /// assigned nothing, and the test carried no assertion able to see the
    /// gate's NUMERATOR drift (proven by the review's mutation 5: scaling the
    /// numerator by `1.000000001` left every wire test green). This asserts
    /// the numerator directly, now that I-4 publishes it as
    /// `SweepPoint::syncs_per_sec`, at two multipliers — mirroring the exact
    /// equality core's and kernel's twins already assert, and discriminating
    /// exactly what the old test could not.
    #[test]
    fn the_gate_input_scales_with_syncs_per_request() {
        // 1.0 syncs/request (invoicing's shape): the published syncs rate
        // equals the counted acked-request rate exactly — the pre-T1 gate's
        // own quantity.
        let mut o = healthy();
        o.syncs = o.grand;
        let a = point_accounting(&o).expect("publishes");
        let ack_rate = o.grand as f64 / o.elapsed.as_secs_f64();
        assert_eq!(
            a.point.syncs_per_sec, ack_rate,
            "at 1.0 syncs/request the published syncs rate must equal the old ack rate \
             exactly"
        );

        // 2.0 syncs/request (bench-app's shape): exactly double.
        o.syncs = o.grand * 2;
        let a2 = point_accounting(&o).expect("publishes");
        assert_eq!(
            a2.point.syncs_per_sec,
            2.0 * a.point.syncs_per_sec,
            "at 2.0 syncs/request the published syncs rate must be exactly 2x the 1.0 case"
        );
    }

    /// C-2 fix (2026-08-10 group-commit interlude, T1 fix wave): mirrors
    /// core_accounting's and kernel_accounting's guard tests. RED-first per
    /// hard rule 3: fails against code without the guard (`point_accounting`
    /// returns `Ok` with `device_ratio` computed off a 0.0 syncs rate and an
    /// empty `invalid`).
    #[test]
    fn a_zero_sync_count_refuses_rather_than_silently_disabling_the_gate() {
        let mut o = healthy();
        o.syncs = 0;
        let err = point_accounting(&o).expect_err("a zero-sync point refuses");
        assert!(err.contains("INVALID"), "got: {err}");
        assert!(err.contains("0 durable syncs"), "got: {err}");
        assert!(err.contains("WireObs::syncs"), "must name the fix: {err}");
    }

    /// 2026-08-10 interlude, the DISCRIMINATING case for this layer's gate —
    /// the same gap mutation found at the kernel layer. The 1.0 and 1.2x
    /// probes above pass under BOTH the old `1.0 + 1e-6` gate and the new
    /// bracketed one, so neither could detect a revert. This is the band where
    /// they differ, and it is the band every observed invoicing point landed
    /// in: 1.007, 1.010, 1.013, 1.036, 1.052, 1.086 (spec §1).
    #[test]
    fn an_ack_rate_just_over_one_is_inside_this_points_measurement_error() {
        let mut o = healthy();
        // 500 acked/s against agreeing brackets of ~490 => 1.02.
        o.device_before = 500.0 / 1.02;
        o.device_after = o.device_before;
        let a = point_accounting(&o).expect("publishes");
        assert!(
            500.0 / o.device_before > 1.0 + crate::measure::DEVICE_RATIO_SLACK,
            "premise: the OLD gate must have refused this, or this test proves \
             nothing about the new one"
        );
        assert!(
            a.invalid.is_empty(),
            "a one-record-per-request target sits at exactly 1.0, so 1.02 is \
             inside this point's own error and must publish: {:?}",
            a.invalid
        );
    }

    /// **The regression test for the finding that produced rev. 4.** On real
    /// hardware the derived record rate read 1.02x-1.10x of a bracket-stable
    /// ceiling on three consecutive HEALTHY runs, because
    /// `records_per_request` assumes a steady-state job drain a short window
    /// has not reached. A derived overshoot on its own must publish, not flag
    /// — this fixture puts the ceiling (800) between the counted ack rate
    /// (500) and the derived record rate (1000), which is exactly the shape
    /// the hardware produced.
    #[test]
    fn a_derived_record_rate_above_the_ceiling_does_not_flag_by_itself() {
        let mut o = healthy();
        o.device_before = 800.0;
        o.device_after = 800.0;
        let a = point_accounting(&o).expect("publishes");
        assert_eq!(a.point.device_ratio, 1.25, "the derived ratio IS above 1.0");
        assert!(
            a.invalid.is_empty(),
            "a derived record rate over the ceiling is published, never flagged \
             — the ack rate (500/s vs an 800/s ceiling) is what gates: {:?}",
            a.invalid
        );
    }

    /// The refusal explains why the rate it names is impossible rather than
    /// merely surprising (hard rule 5). Renamed and reworded (2026-08-10
    /// group-commit interlude, T1 fix wave, I-1): the argument used to be
    /// ack-after-durable ("every acked request pays at least one durable
    /// sync"), which justified gating on the ACK rate. Since C-1's fix the
    /// gate's subject is SYNCS directly, and the argument is simpler — the
    /// device cannot sync faster than its own measured ceiling.
    #[test]
    fn the_ceiling_refusal_names_the_device_ceiling_argument() {
        let mut o = healthy();
        // 1.2x, not 1.0 + 2*slack: the 2026-08-10 interlude gates against this
        // point's own measurement error, and with the two samples equal that is
        // the RATIO_TOLERANCE_FLOOR, so the ratio must clear the floor to fire.
        o.device_before = 500.0 / 1.2;
        o.device_after = o.device_before;
        let a = point_accounting(&o).expect("implausible is named, not refused");
        assert!(
            a.invalid[0].contains("cannot sync faster than its own measured ceiling"),
            "got: {:?}",
            a.invalid
        );
        assert!(
            a.invalid[0].contains("not physically possible"),
            "got: {:?}",
            a.invalid
        );
    }

    /// T7 rev. 3's gate rule, still load-bearing under rev. 4's ack gate: the
    /// ceiling is the bracket MAX, not the min and not `before` alone. The
    /// fixture is tuned so the two answers straddle the threshold — the ack
    /// rate is 500/s, so `max` (600.0) gives 0.83 and clears, while both the
    /// min AND the "before" sample (450.0) give 1.11 and would wrongly flag.
    /// Rev. 3's original fixture (900.0/1200.0) could not fail under the ack
    /// gate: both answers land far below 1.0.
    #[test]
    fn the_ratio_gate_divides_by_the_bracket_max_not_the_min_or_before() {
        let mut o = healthy();
        o.device_before = 450.0;
        o.device_after = 600.0;
        let a = point_accounting(&o).expect("publishes");
        assert_eq!(
            a.point.device_ratio,
            1000.0 / 600.0,
            "must divide by the bracket MAX (600.0), not the min or `before` (450.0)"
        );
        assert!(
            a.invalid.is_empty(),
            "the max bracket sample clears this point; only the min/before would \
             wrongly flag it: {:?}",
            a.invalid
        );
    }

    /// The product-level reading of the same mechanism: a single noisy
    /// sample — here the AFTER sample, dipping low from transient contention
    /// right as the child is killed — must not by itself flag a point whose
    /// rate is plausible against the healthy sample. `device_before` (600.0)
    /// clears the 500/s ack rate; only a `min`-or-single-sample gate would
    /// wrongly flag this point on `device_after` (450.0) alone.
    #[test]
    fn an_under_measured_single_bracket_sample_does_not_by_itself_flag_a_healthy_point() {
        let mut o = healthy();
        o.device_before = 600.0;
        o.device_after = 450.0;
        let a = point_accounting(&o).expect("publishes");
        assert!(
            a.invalid.is_empty(),
            "a single noisy sample must not flag a point the other bracket \
             sample shows is healthy: {:?}",
            a.invalid
        );
    }

    /// `report.rs`'s renderers and `collect_non_finite` need both bracket
    /// samples on the published point, not folded away into the ratio alone
    /// — this is what lets an operator see the spread the artifact claims to
    /// show.
    #[test]
    fn both_bracket_samples_reach_the_published_point() {
        let mut o = healthy();
        o.device_before = 3500.0;
        o.device_after = 3600.0;
        let a = point_accounting(&o).expect("publishes");
        assert_eq!(a.point.device_appends_per_sec_before, 3500.0);
        assert_eq!(a.point.device_appends_per_sec_after, 3600.0);
    }

    /// A one-connection, one-second fixture so `client_busy_fraction` IS
    /// `client_cpu.as_secs_f64()` with no division at all — the only way to
    /// hit the `>` vs `>=` boundary BIT-exactly rather than within a margin.
    /// `SATURATION_FRACTION` (0.9) is exactly 900_000_000 nanoseconds, so
    /// `Duration::new(0, 900_000_000).as_secs_f64()` round-trips to the same
    /// f64 bit pattern as the `0.9` literal (both are the IEEE754
    /// correctly-rounded double nearest the true value 0.9) — an equality a
    /// margin-based construction (tried first; see git history) cannot
    /// deliver, and which the row-7 mutation table below caught the first
    /// version failing to prove.
    fn saturation_fixture() -> WireObs {
        WireObs {
            connections: 1,
            per_thread: vec![10],
            grand: 10,
            latencies_us: vec![500u64; 10],
            elapsed: Duration::from_secs(1),
            client_cpu: Duration::ZERO,
            // Large enough the ratio gate never fires here, either sample.
            device_before: 1e12,
            device_after: 1e12,
            records_per_request: 1.0,
            // The derived form, as in `healthy()` above.
            syncs: 10,
            syncs_counted: false,
            heartbeat: None,
        }
    }

    #[test]
    fn saturation_is_flagged_above_the_fraction_and_not_at_it() {
        let mut o = saturation_fixture();
        o.client_cpu = Duration::new(0, 900_000_000); // exactly 0.9s
        let a = point_accounting(&o).expect("publishes");
        assert_eq!(a.point.client_busy_fraction, SATURATION_FRACTION);
        assert!(
            !a.point.saturated,
            "exactly at the fraction must not be saturated"
        );

        o.client_cpu = Duration::new(0, 900_000_001); // one nanosecond over
        let a = point_accounting(&o).expect("publishes");
        assert!(a.point.client_busy_fraction > SATURATION_FRACTION);
        assert!(
            a.point.saturated,
            "one nanosecond over the fraction must be saturated"
        );
    }

    #[test]
    fn every_reason_carries_its_connection_count() {
        let prefix = format!("[{} conns] ", 4);

        let mut o = healthy();
        o.heartbeat = Some("INVALID: heartbeat starved".to_string());
        let a = point_accounting(&o).expect("non-fatal");
        assert!(a.invalid[0].starts_with(&prefix), "got: {:?}", a.invalid);

        let mut o = healthy();
        o.per_thread = vec![250, 250, 250, 249];
        let a = point_accounting(&o).expect("non-fatal");
        assert!(a.invalid[0].starts_with(&prefix), "got: {:?}", a.invalid);

        let mut o = healthy();
        // 500.0, not 1000.0: rev. 4 gates on the ACK rate (500/s), so a
        // ceiling just under the derived record rate no longer flags at all.
        // 1.2x, not 1.0 + 2*slack: the 2026-08-10 interlude gates against this
        // point's own measurement error, and with the two samples equal that is
        // the RATIO_TOLERANCE_FLOOR, so the ratio must clear the floor to fire.
        o.device_before = 500.0 / 1.2;
        o.device_after = o.device_before;
        let a = point_accounting(&o).expect("non-fatal");
        assert!(a.invalid[0].starts_with(&prefix), "got: {:?}", a.invalid);

        let mut o = healthy();
        o.latencies_us = vec![500u64; (MIN_MEASURED_OPS - 1) as usize];
        let err = point_accounting(&o).expect_err("refuses");
        assert!(err.starts_with(&prefix), "got: {err}");

        let mut o = healthy();
        o.elapsed = MIN_MEASURED_WINDOW - Duration::from_millis(1);
        let err = point_accounting(&o).expect_err("refuses");
        assert!(err.starts_with(&prefix), "got: {err}");

        // All three non-fatal reasons at once, pinning BOTH properties this
        // test's name promises: every reason still carries its connection
        // count, AND they arrive in the fixed push order (heartbeat, then
        // crosscheck, then the ratio gate) both renderers derive from —
        // mirroring `kernel_accounting`'s
        // `the_reasons_are_pushed_in_a_fixed_order`. It stays folded in here
        // because both properties are claims about the SAME assembled
        // `invalid` list and share this fixture; the rev. 2 reason for
        // folding — that Step 11 pinned a count of 13 — no longer holds, since
        // rev. 3 and rev. 4 took this module to 18 tests.
        let mut o = healthy();
        o.heartbeat = Some("INVALID: heartbeat starved".to_string());
        o.per_thread = vec![250, 250, 250, 249];
        // 500.0, not 1000.0: rev. 4 gates on the ACK rate (500/s), so a
        // ceiling just under the derived record rate no longer flags at all.
        // 1.2x, not 1.0 + 2*slack: the 2026-08-10 interlude gates against this
        // point's own measurement error, and with the two samples equal that is
        // the RATIO_TOLERANCE_FLOOR, so the ratio must clear the floor to fire.
        o.device_before = 500.0 / 1.2;
        o.device_after = o.device_before;
        let a = point_accounting(&o).expect("all three of these are non-fatal");
        assert_eq!(a.invalid.len(), 3, "got: {:?}", a.invalid);
        for reason in &a.invalid {
            assert!(reason.starts_with(&prefix), "got: {:?}", a.invalid);
        }
        let shape: Vec<&str> = a
            .invalid
            .iter()
            .map(|r| {
                if r.contains("heartbeat starved") {
                    "heartbeat"
                } else if r.contains("totals disagree") {
                    "crosscheck"
                } else if r.contains("the measured device ceiling") {
                    "ratio"
                } else {
                    "UNKNOWN"
                }
            })
            .collect();
        assert_eq!(
            shape,
            vec!["heartbeat", "crosscheck", "ratio"],
            "got: {:?}",
            a.invalid
        );
    }

    /// Interlude §4's standing rule: the accounting must not grow its own
    /// non-finite guard. A zero bracket max makes the ratio infinite, and it
    /// still publishes (`Ok`) — `report.rs`'s central `collect_non_finite`
    /// is the one place this gets caught for rendering purposes.
    #[test]
    fn a_zero_device_ceiling_leaves_a_non_finite_ratio_for_the_central_collector() {
        let mut o = healthy();
        o.device_before = 0.0;
        o.device_after = 0.0;
        let a = point_accounting(&o).expect("a zero ceiling is not a local refusal");
        assert!(
            a.point.device_ratio.is_infinite(),
            "got: {}",
            a.point.device_ratio
        );
    }

    /// A non-finite BRACKET SAMPLE (not just a zero ceiling collapsing into
    /// one) is not locally guarded — the RAW sample flows through to the
    /// published point unmodified, so that `report::collect_non_finite`
    /// (which scans the published `device_appends_per_sec_before`/`_after`
    /// fields directly, per T7 rev. 3) is what catches it, not a local check
    /// here.
    ///
    /// What does NOT happen, discovered RED (this test's first version
    /// asserted it did): a single NaN sample does not by itself poison
    /// `device_ratio`. `f64::max` ignores a NaN operand and returns the
    /// other one (`NaN.max(y) == y`, IEEE 754/Rust's documented semantics,
    /// unlike a plain comparison or an arithmetic operator) — so `max` only
    /// goes non-finite when BOTH samples are.
    #[test]
    fn a_non_finite_bracket_sample_is_not_locally_guarded_and_flows_through() {
        let mut o = healthy();
        o.device_before = f64::NAN;
        let a = point_accounting(&o).expect("a non-finite bracket sample is not a local refusal");
        assert!(
            a.point.device_appends_per_sec_before.is_nan(),
            "the raw sample must reach the published point unmodified"
        );
        assert!(
            a.point.device_ratio.is_finite(),
            "f64::max ignores a lone NaN operand, so a single non-finite sample must \
             not poison the ratio: got {}",
            a.point.device_ratio
        );

        // Both samples non-finite: max(NaN, NaN) IS NaN, so this does
        // propagate — the discriminating case against a mutation that
        // replaces `max` with something that always ignores NaN.
        o.device_after = f64::NAN;
        let a = point_accounting(&o).expect("non-finite samples are not a local refusal");
        assert!(
            a.point.device_ratio.is_nan(),
            "got: {}",
            a.point.device_ratio
        );
    }

    #[test]
    fn percentiles_come_from_a_sorted_copy_of_unsorted_observations() {
        let mut o = healthy();
        o.latencies_us = vec![900, 100, 500, 300, 700, 200, 800, 400, 600, 1000];
        o.per_thread = vec![3, 3, 2, 2];
        o.grand = 10;
        let a = point_accounting(&o).expect("publishes");
        let mut sorted = o.latencies_us.clone();
        sorted.sort_unstable();
        assert_eq!(a.point.p50_us, percentile(&sorted, 50.0));
        assert_eq!(a.point.p99_us, percentile(&sorted, 99.0));
        assert_eq!(a.point.p999_us, percentile(&sorted, 99.9));
    }
}
