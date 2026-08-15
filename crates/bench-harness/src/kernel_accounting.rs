//! The accounting half of layer 3. Interlude spec
//! docs/superpowers/specs/2026-08-08-bench-accounting-split-design.md §5 rules
//! that this layer adopts the observations seam from inception rather than
//! having it retrofitted: every crosscheck, rate division and INVALID decision
//! the kernel layer makes is a pure function over observations already taken.
//!
//! Nothing here reads a clock, touches a database, a disk, a socket or a
//! thread. The clock/thread/socket/`std::fs` half is enforced rather than
//! promised: unlike `kernel_bench` beside it, this module carries no blanket
//! `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]`, so hard
//! rule 1's workspace deny-list is what holds it.
//!
//! The no-`Db`/no-`Disk` half is NOT on the deny-list (those are charpente
//! types) and is held by review instead: adding a `Db` or `Disk` parameter to
//! anything in this module defeats the seam even though CI stays green. Said
//! here because the shipped version of this doc claimed the whole list was
//! mechanical (T6 review, I-3), and a module whose stated reason for existing
//! is mechanical enforcement is the last place to overstate it.

use crate::measure::{
    RATIO_TOLERANCE_FLOOR, bracket_spread, crosscheck, degenerate_batch_reason,
    implausible_bracketed_ratio, ratio_tolerance, unstable_device_reason,
};
use std::time::Duration;

/// What one measured kernel batch observed. Every field is a count, a duration
/// or a verdict that was actually observed; nothing here is derived and
/// nothing here is a decision.
#[derive(Debug, Clone, Default)]
pub struct KernelObs {
    /// Requests pushed into the source before `Kernel::run`.
    pub requests_planned: u64,
    /// Responses the recording sink actually collected — totals source 1.
    /// Independent of source 2 only as a check on the harness's own sink
    /// plumbing; see the crosscheck block in `kernel_accounting` for why.
    pub responses_recorded: u64,
    /// `KernelStats::requests` — totals source 2, counted by the kernel's own
    /// dispatch rather than by the sink.
    pub stats_requests: u64,
    /// This layer's own device bracket samples, taken either side of the
    /// `Kernel::run` batch window (interlude spec §4.2, Noah ruling
    /// 2026-08-10). Through T8 this was `device_ceiling` — layer 1's one-shot
    /// `device.syncs_per_sec` from a separate invocation — and the shipped
    /// refusal message itself named the gap: "this layer takes no bracket
    /// samples of its own to show the spread". It does now, and the accounting
    /// divides by `max(before, after)`, the worse of the two.
    pub device_before: f64,
    pub device_after: f64,
    /// `KernelStats::jobs_done` — totals source 3. Every `/hit/:id` enqueues
    /// exactly one `Bump` (lib.rs's canonical route), so a drained batch must
    /// finish exactly as many jobs as it served requests.
    pub stats_jobs_done: u64,
    /// Durable syncs this batch paid — the device-ratio gate's subject
    /// (2026-08-10 group-commit interlude, T1). `Engine::submit` today is
    /// unconditionally `append`→`sync`→`apply` with no batching, so this
    /// equals the log record count (`requests_planned + stats_jobs_done`,
    /// one commit each); a future group-commit primary path is what makes
    /// the two diverge.
    pub syncs: u64,
    /// The first non-200 response, already rendered, or `None` if every
    /// request succeeded. Rendering happens in the driver because it needs the
    /// body bytes; the DECISION about it happens here.
    pub first_failure: Option<String>,
    /// Monotonic time `Kernel::run` took to drain the batch.
    pub elapsed: Duration,
    /// `Heartbeat::finish()` spanning the whole batch.
    pub heartbeat: Option<String>,
}

/// What the kernel layer publishes, plus every non-fatal reason it collected,
/// in push order.
#[derive(Debug, Clone)]
pub struct KernelAccounting {
    pub acked_writes_per_sec: f64,
    pub log_records_per_sec: f64,
    /// The measured window itself, published as a field rather than left to
    /// prose (T6 review, I-4 / Noah ruling). This layer reads neither
    /// `--warmup` nor `--duration` — the two the artifact's methodology header
    /// carries — so without this field the header describes a window this
    /// layer never used.
    pub window_secs: f64,
    /// The device-ratio gate's numerator (2026-08-10 group-commit interlude,
    /// T1 fix wave, I-4) — published so `device_ratio` is reconstructable
    /// from the artifact, mirroring the standing ruling behind
    /// `CoreReport::device_ratio_denominator`. Today equals
    /// `log_records_per_sec` exactly (one commit, one fsync, no batching).
    pub syncs_per_sec: f64,
    /// `syncs_per_sec / max(device_before, device_after)`. See the gate in
    /// `kernel_accounting` for why syncs, not the log rate, is the operand
    /// (2026-08-10 group-commit interlude, T1) — today the two are the same
    /// number, one commit per sync, so this is unchanged in value from
    /// before T1.
    pub device_ratio: f64,
    pub invalid: Vec<String>,
}

/// The kernel layer's accounting. `Err` refuses the run — both refusals mean
/// the batch is not a measurement of the serving path at all. `Ok` carries the
/// published values plus every non-fatal reason in push order.
///
/// Decision order blends both core halves deliberately (T6 review, M-6: the
/// shipped doc named `read_accounting` alone as the model, which is checkable
/// and wrong — that function has no trailing non-fatal crosschecks): the
/// heartbeat reason is collected first and is non-fatal, as in both, then the
/// conditions that mean there is nothing to measure refuse by name, as in
/// `read_accounting`, then the non-fatal crosschecks, as in
/// `write_accounting`. Unlike the core layer there is no expensive work
/// downstream of this call, so no short-circuit ordering constraint applies
/// (interlude spec §2 is about `run`'s population loop, which has no analogue
/// here) — the order is for reader parity across layers, and it is pinned.
pub fn kernel_accounting(k: &KernelObs) -> Result<KernelAccounting, String> {
    let mut invalid = Vec::new();
    if let Some(reason) = &k.heartbeat {
        invalid.push(reason.clone());
    }
    // T6 review, C-1 / Noah ruling: the BATCH-shaped floor, which bounds the
    // measured window and not the operator's `--requests` input. The
    // duration-bounded `degenerate_window_reason` shipped here first and was
    // checking an input, leaving the window unbounded entirely.
    if let Some(reason) = degenerate_batch_reason("kernel", k.requests_planned, k.elapsed) {
        return Err(reason);
    }
    if let Some(failure) = &k.first_failure {
        let planned = k.requests_planned;
        return Err(format!(
            "INVALID: a canonical request did not return 200 — the batch pushed {planned} \
             requests and at least one failed, so this is not a measurement of the kernel's \
             serving path at all, it is a measurement of how fast it can refuse; first \
             failure: {failure}"
        ));
    }
    // Totals THREE ways (spec §5), each against the planned count: the
    // recording sink, the kernel's own request counter, and its job counter.
    //
    // T6 review, I-2 — the shipped comment here called all three "genuinely
    // independent at this layer, unlike the core write loop's
    // structurally-vacuous pair", and that is false as written.
    // `Kernel::step`'s `Ready::Request` arm pushes exactly one response on
    // every arm (`send`, `stream`, or `send` on the validate error) and then
    // increments `stats.requests` unconditionally, with no branch and no early
    // return in between, so for any `SimSource`-driven kernel
    // `responses_recorded == stats_requests` holds invariantly. The precise
    // claim: as a check on the MEASURED PATH those two are one check; they stay
    // independent only as a check on the harness's own sink plumbing (an
    // `Arc<Mutex<Vec>>` owned by the source vs. a `u64` owned by the kernel),
    // which is a real but narrow class. `stats_jobs_done` is the genuinely
    // independent third. All three are kept — a sink-wiring bug is worth
    // catching — but the artifact can double-report one fact as two reasons,
    // and that is the honest cost, not a hidden one.
    if let Err(reason) = crosscheck(&[k.responses_recorded], k.requests_planned) {
        invalid.push(format!(
            "{reason} (recorded responses must equal the requests pushed; if \
             KernelStats::requests is flagged too these are one fact, not two)"
        ));
    }
    if let Err(reason) = crosscheck(&[k.stats_requests], k.requests_planned) {
        invalid.push(format!(
            "{reason} (KernelStats::requests must equal the requests pushed; a kernel that \
             returned Ok having drained fewer would be a charpente bug, not only a harness one)"
        ));
    }
    if let Err(reason) = crosscheck(&[k.stats_jobs_done], k.requests_planned) {
        invalid.push(format!(
            "{reason} (jobs_done must equal requests: every /hit/:id enqueues exactly one Bump)"
        ));
    }

    let secs = k.elapsed.as_secs_f64();
    // Each request commits one effects record; each Bump's Done-mark commits
    // another (spec §3) — 2x by construction at this layer, which is why this
    // is a SUM and not a copy of the ack rate. Deriving it from
    // `stats_jobs_done` rather than from `requests_planned` twice is
    // deliberate: if the job counter ever disagrees, the published number shows
    // it instead of hiding it behind an assumed multiplier.
    let log_records_per_sec = (k.requests_planned + k.stats_jobs_done) as f64 / secs;
    // T6 review, M-5 / Noah ruling: this layer's physical ceiling —
    // `syncs_per_sec` (the DURABLE SYNC rate this batch actually paid) cannot
    // exceed the device's own durable rate, which `device_ratio` gates on
    // below. `log_records_per_sec` stays published beside it (each request
    // commits one effects record and its Bump's Done-mark commits another —
    // 2x by construction, unchanged) because it is the number the physics
    // claim used to be justified in terms of.
    //
    // Fix round I-1 (group-commit T7), correcting this comment's own old
    // claim: it used to say "Engine::submit performs exactly one wal.sync()
    // per transaction with no group commit — so this layer spends TWO
    // durable syncs per request" and derived the gate from that
    // (`log_records_per_sec / syncs_per_sec`, equivalently
    // `acked_writes_per_sec <= syncs_per_sec / 2`). Both clauses are false
    // now: both the request arm (T6) and the job arm (T7) BATCH — the commit
    // path is `Group::commit` -> `Engine::submit_all`, one sync for however
    // many records land in a batch, not one sync per record. `k.syncs` is
    // COUNTED at the `Disk` boundary (`kernel_bench.rs`'s `CountingDisk`),
    // never derived from `log_records_per_sec` — the gate's actual physics
    // claim is unchanged; what changed is WHY `syncs_per_sec` is what it is.
    //
    // 2026-08-10 group-commit interlude, T1: the numerator is `k.syncs`, not
    // `log_records_per_sec` — PRE-T6 they were the same number (one commit,
    // one fsync, pinned by
    // `while_one_commit_costs_one_fsync_the_gate_input_is_unchanged`); T6/T7
    // together are the group-commit primary path where they diverge.
    //
    // T1 fix wave, C-2: mirrors core_accounting's guard. `syncs == 0` on a
    // batch that already cleared the floor above is impossible under any
    // engine, batched or not — a nonzero batch syncs at least once. Without
    // this a producing site that leaves `KernelObs::syncs` at
    // `KernelObs::default()`'s 0 makes `device_ratio` a silently-published
    // 0.0, incapable of ever refusing anything.
    if k.syncs == 0 {
        return Err(format!(
            "INVALID: the kernel batch drained {} requests but recorded 0 durable syncs — \
             the device-ratio gate divides by this, and a zero numerator makes it incapable \
             of refusing anything; kernel_bench::run must set KernelObs::syncs",
            k.requests_planned
        ));
    }
    let ceiling = k.device_before.max(k.device_after);
    let syncs_per_sec = k.syncs as f64 / secs;
    let device_ratio = syncs_per_sec / ceiling;
    if implausible_bracketed_ratio(device_ratio, k.device_before, k.device_after) {
        invalid.push(format!(
            "INVALID: device_ratio {device_ratio:.3} exceeds 1.0 by more than this window's \
             own measurement error ({:.1}%) — this layer's syncs are counted directly at the \
             Disk boundary, not derived (both the request arm and the job arm batch under \
             Group::commit -> Engine::submit_all, so a batch pays however many syncs its own \
             commits actually took, not one per record), so {syncs_per_sec:.2} durable syncs/s \
             ({log_records_per_sec:.2} log records/s) cannot exceed the device's own durable \
             rate. The denominator is this batch's own \
             bracket max ({ceiling:.2}) from the samples either side of the window ({:.2} \
             before / {:.2} after); the tolerance is the wider of those two disagreeing \
             ({:.1}%) and the {:.1}% floor for drift a bracket cannot see",
            ratio_tolerance(k.device_before, k.device_after) * 100.0,
            k.device_before,
            k.device_after,
            bracket_spread(k.device_before, k.device_after) * 100.0,
            RATIO_TOLERANCE_FLOOR * 100.0,
        ));
    }
    // Independent of the ratio above, and checked even when that one passed:
    // the ratio gate asks whether the rate is plausible against the device,
    // this asks whether "the device" was one thing for the length of the
    // window at all.
    if let Some(reason) = unstable_device_reason("kernel", k.device_before, k.device_after) {
        invalid.push(reason);
    }

    Ok(KernelAccounting {
        acked_writes_per_sec: k.requests_planned as f64 / secs,
        window_secs: secs,
        syncs_per_sec,
        device_ratio,
        log_records_per_sec,
        invalid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::measure::{MIN_MEASURED_OPS, MIN_MEASURED_WINDOW};

    /// A plausible healthy batch: 10,000 requests drained in 2s, all three
    /// totals agreeing, every response 200. Every derived quantity is exact in
    /// f64 (10000/2 = 5000, 20000/2 = 10000 — both representable, both exact
    /// quotients), so these assertions are equalities, not tolerances. This is
    /// also the ABOVE-the-floor case; the boundary test below covers at/under.
    fn healthy() -> KernelObs {
        KernelObs {
            requests_planned: 10_000,
            responses_recorded: 10_000,
            stats_requests: 10_000,
            stats_jobs_done: 10_000,
            // One fsync per commit — today's engine, so this equals the log
            // record count (10_000 + 10_000).
            syncs: 20_000,
            first_failure: None,
            elapsed: Duration::from_secs(2),
            heartbeat: None,
            // 10_000 syncs/s (== 10_000 log records/s today) against a
            // 20_000/s ceiling — ratio 0.5, comfortably plausible, and an
            // exact binary fraction so the equality below is legitimate
            // rather than lucky.
            device_before: 20_000.0,
            device_after: 20_000.0,
        }
    }

    #[test]
    fn a_healthy_batch_publishes_and_flags_nothing() {
        let a = kernel_accounting(&healthy()).expect("a healthy batch publishes");
        assert!(a.invalid.is_empty(), "got: {:?}", a.invalid);
        assert_eq!(a.acked_writes_per_sec, 5_000.0);
    }

    /// The bracket-stability gate reaches THIS layer. Added because it did not:
    /// the gate was wired into all three layers and pinned in only two, since
    /// every `KernelObs` fixture here sets `device_before == device_after` and
    /// so cannot distinguish a wired gate from an unwired one. Deleting the
    /// `unstable_device_reason` call from `kernel_accounting` left
    /// `cargo test --workspace` green — the exact vacuity this repo keeps
    /// finding in its own instruments, in a change whose entire subject is a
    /// gate that never fired.
    ///
    /// The fixture isolates the stability gate from the ratio gate on purpose:
    /// 30_000/20_000 is a 33.3% spread (over the 20% ceiling), while the ratio
    /// is 10_000/30_000 = 0.33 and passes comfortably. So exactly one reason
    /// may appear, and if the wiring is removed there is nothing left to
    /// report.
    #[test]
    fn the_bracket_stability_gate_is_wired_into_this_layer() {
        let mut o = healthy();
        o.device_before = 30_000.0;
        o.device_after = 20_000.0;
        let a = kernel_accounting(&o).expect("publishes with a reason");
        assert!(
            a.device_ratio < 1.0,
            "premise: the ratio gate must NOT be what fires here (ratio {})",
            a.device_ratio,
        );
        assert_eq!(
            a.invalid.len(),
            1,
            "exactly the stability reason, nothing else: {:?}",
            a.invalid,
        );
        assert!(
            a.invalid[0].contains("moved DURING") && a.invalid[0].contains("kernel"),
            "got: {:?}",
            a.invalid,
        );
    }

    /// The layer's headline accounting rule (spec §3): one effects record per
    /// request PLUS one per Bump Done-mark, so the log rate is exactly twice
    /// the ack rate on a healthy batch — by construction, not by measurement.
    #[test]
    fn the_log_record_rate_is_the_sum_of_requests_and_job_done_marks() {
        let a = kernel_accounting(&healthy()).expect("publishes");
        assert_eq!(a.log_records_per_sec, 10_000.0);
        assert_eq!(
            a.log_records_per_sec,
            2.0 * a.acked_writes_per_sec,
            "2x by construction at this layer"
        );
    }

    /// …and it is derived from the OBSERVED job count, not from a hardcoded
    /// multiplier — so a batch whose jobs did not all finish publishes a lower
    /// log rate rather than an assumed one. (It is also flagged; that is the
    /// crosscheck test below. Both matter: one is what we PUBLISH, the other
    /// is what we SAY.)
    #[test]
    fn a_short_job_count_lowers_the_published_log_rate_rather_than_being_assumed() {
        let mut o = healthy();
        o.stats_jobs_done = 9_000;
        let a = kernel_accounting(&o).expect("a mismatch is named, not refused");
        assert_eq!(a.log_records_per_sec, 9_500.0);
        assert!(
            a.log_records_per_sec < 2.0 * a.acked_writes_per_sec,
            "must not publish the assumed 2x"
        );
    }

    #[test]
    fn a_below_floor_batch_refuses_and_an_at_floor_one_does_not() {
        let mut o = healthy();
        o.requests_planned = MIN_MEASURED_OPS - 1;
        o.responses_recorded = MIN_MEASURED_OPS - 1;
        o.stats_requests = MIN_MEASURED_OPS - 1;
        o.stats_jobs_done = MIN_MEASURED_OPS - 1;
        let err = kernel_accounting(&o).expect_err("below the floor refuses");
        assert!(err.contains("INVALID"), "got: {err}");
        assert!(err.contains("kernel batch"), "must name which loop: {err}");
        // T6 review, I-1: the core layer's twin asserts the remedy and this
        // test shipped without that third assertion — which is exactly the
        // assertion that would have caught the shipped message naming
        // `--duration`, a flag that never reaches this layer.
        assert!(err.contains("--requests"), "must name the fix: {err}");
        assert!(
            !err.contains("--duration"),
            "must not name a flag this layer never reads: {err}"
        );

        o.requests_planned = MIN_MEASURED_OPS;
        o.responses_recorded = MIN_MEASURED_OPS;
        o.stats_requests = MIN_MEASURED_OPS;
        o.stats_jobs_done = MIN_MEASURED_OPS;
        let a = kernel_accounting(&o).expect("the floor itself publishes");
        assert!(a.invalid.is_empty(), "got: {:?}", a.invalid);
    }

    /// T6 amendment, change 2: a failed request makes the batch not a
    /// measurement, so it refuses by name rather than panicking in the driver
    /// where no test could reach it.
    #[test]
    fn a_non_200_response_refuses_by_name_and_quotes_the_failure() {
        let mut o = healthy();
        o.first_failure = Some("HTTP 500: boom".to_string());
        let err = kernel_accounting(&o).expect_err("a failed request refuses");
        assert!(err.contains("INVALID"), "got: {err}");
        assert!(err.contains("did not return 200"), "got: {err}");
        assert!(
            err.contains("HTTP 500: boom"),
            "must quote the failure it saw: {err}"
        );
        assert!(
            err.contains("10000"),
            "must name the batch size that was attempted: {err}"
        );
    }

    /// Each of the three totals sources is checked SEPARATELY and named
    /// distinguishably — a single merged crosscheck would tell an operator
    /// that something disagreed without saying which counter, which is the
    /// whole value of having three.
    #[test]
    fn each_of_the_three_totals_sources_is_named_when_it_alone_disagrees() {
        for (label, needle, mutate) in [
            (
                "sink",
                "recorded responses",
                (|o: &mut KernelObs| o.responses_recorded -= 1) as fn(&mut KernelObs),
            ),
            ("kernel", "KernelStats::requests", |o: &mut KernelObs| {
                o.stats_requests -= 1
            }),
            ("jobs", "every /hit/:id enqueues", |o: &mut KernelObs| {
                o.stats_jobs_done -= 1
            }),
        ] {
            let mut o = healthy();
            mutate(&mut o);
            let a = kernel_accounting(&o).expect("a totals mismatch is named, not refused");
            assert_eq!(a.invalid.len(), 1, "{label}: got {:?}", a.invalid);
            assert!(
                a.invalid[0].contains(needle),
                "{label}: must name which counter disagreed: {:?}",
                a.invalid
            );
            assert!(a.invalid[0].contains("INVALID"), "{label}: {:?}", a.invalid);
        }
    }

    #[test]
    fn the_heartbeat_reason_is_carried_through_and_is_not_fatal() {
        let mut o = healthy();
        o.heartbeat = Some("INVALID: heartbeat starved".to_string());
        let a = kernel_accounting(&o).expect("a starved heartbeat is no refusal");
        assert_eq!(a.invalid, vec!["INVALID: heartbeat starved".to_string()]);
        assert_eq!(a.acked_writes_per_sec, 5_000.0);
    }

    /// Reasons are pushed in a fixed order and the renderers derive from this
    /// one list, so a reordering is invisible to every other assertion here
    /// and visible in the published artifact. Force all four at once.
    #[test]
    fn the_reasons_are_pushed_in_a_fixed_order() {
        let mut o = healthy();
        o.heartbeat = Some("INVALID: heartbeat starved".to_string());
        o.responses_recorded -= 1;
        o.stats_requests -= 2;
        o.stats_jobs_done -= 3;
        o.device_before = 1.0; // forces the ratio reason too
        o.device_after = 1.0;
        let a = kernel_accounting(&o).expect("all five of these are non-fatal");
        let shape: Vec<&str> = a
            .invalid
            .iter()
            .map(|r| {
                if r.contains("heartbeat starved") {
                    "heartbeat"
                } else if r.contains("recorded responses") {
                    "sink"
                } else if r.contains("KernelStats::requests") {
                    "kernel"
                } else if r.contains("every /hit/:id enqueues") {
                    "jobs"
                } else if r.contains("device_ratio") {
                    "ratio"
                } else {
                    "UNKNOWN"
                }
            })
            .collect();
        assert_eq!(
            shape,
            vec!["heartbeat", "sink", "kernel", "jobs", "ratio"],
            "got: {:?}",
            a.invalid
        );
    }

    /// The floor refuses BEFORE the non-200 arm, so a degenerate batch that
    /// also failed names the floor — the condition an operator must fix first.
    /// T6 review, C-1 (the Critical). A batch WELL above the op floor whose
    /// measured window is too short to mean anything must refuse. The shipped
    /// code checked only the op count — the operator's own input — so this
    /// case published a rate at exit 0 from a ~46 ms window. Note this obs is
    /// internally consistent: 10,000 requests really did drain in 500 ms; the
    /// only thing wrong with it is that no such window can carry a rate.
    #[test]
    fn a_batch_that_drains_faster_than_the_minimum_window_refuses() {
        let mut o = healthy();
        o.elapsed = MIN_MEASURED_WINDOW - Duration::from_millis(1);
        let err = kernel_accounting(&o).expect_err("a sub-minimum window refuses");
        assert!(err.contains("INVALID"), "got: {err}");
        assert!(err.contains("kernel batch"), "must name which loop: {err}");
        assert!(err.contains("--requests"), "must name the fix: {err}");
        assert!(
            err.contains("drained in"),
            "must name the WINDOW as the defect, not the op count: {err}"
        );
        // The two floors are separate gates with distinguishable messages —
        // an operator told "only 4 ops" and an operator told "drained in
        // 999ms" have different things to fix.
        assert!(
            !err.contains("was only"),
            "the op-count floor's wording must not be reused here: {err}"
        );

        // …and the window floor itself publishes, so the gate is a boundary
        // and not a blanket refusal of every short run.
        o.elapsed = MIN_MEASURED_WINDOW;
        let a = kernel_accounting(&o).expect("the minimum window itself publishes");
        assert!(a.invalid.is_empty(), "got: {:?}", a.invalid);
    }

    /// T6 review, I-4: the artifact's methodology header carries `--warmup`
    /// and `--duration`, neither of which this layer reads, so the layer must
    /// publish the window it actually measured.
    #[test]
    fn the_measured_window_is_published_as_a_field() {
        let a = kernel_accounting(&healthy()).expect("publishes");
        assert_eq!(a.window_secs, 2.0);
        assert_eq!(
            a.acked_writes_per_sec * a.window_secs,
            10_000.0,
            "the published window must be the one the published rate divides by"
        );
    }

    /// The gate's subject is SYNCS, not log records — mirroring
    /// core_accounting's discriminating test. Today the two are equal at this
    /// layer too (one commit, one fsync — the request's own effects record
    /// and its Bump's Done-mark each pay their own sync), which is why
    /// `while_one_commit_costs_one_fsync_the_gate_input_is_unchanged` below
    /// passes trivially; after group commit they diverge and only the syncs
    /// framing stays physically meaningful.
    #[test]
    fn the_gate_compares_syncs_to_the_device_ceiling_not_log_records() {
        let mut o = healthy();
        // healthy()'s 20,000 log records over 2s (10,000/s) against a
        // 6,000/s ceiling would be refused under the OLD log-rate gate
        // (ratio 1.667, past the 1.5 hard limit). Set syncs to a
        // batched-looking 10,000 (5,000/s) — under the ceiling — to prove
        // the gate now compares syncs, not the log record rate.
        o.device_before = 6_000.0;
        o.device_after = 6_000.0;
        let old_log_rate_ratio = (o.requests_planned + o.stats_jobs_done) as f64
            / o.elapsed.as_secs_f64()
            / o.device_before.max(o.device_after);
        assert!(
            old_log_rate_ratio > crate::measure::RATIO_HARD_LIMIT,
            "premise: the OLD log-rate gate must have refused this, or this \
             test proves nothing about the new one — ratio {old_log_rate_ratio}"
        );
        o.syncs = 10_000;
        let a = kernel_accounting(&o).expect("must not refuse");
        assert!(
            a.invalid.is_empty(),
            "a batched batch under the sync ceiling must not be refused: {:?}",
            a.invalid
        );
    }

    /// C-2 fix (2026-08-10 group-commit interlude, T1 fix wave): mirrors
    /// core_accounting's guard test. RED-first per hard rule 3: fails against
    /// code without the guard (`kernel_accounting` returns `Ok` with
    /// `device_ratio: 0.0` and an empty `invalid`).
    #[test]
    fn a_zero_sync_count_refuses_rather_than_silently_disabling_the_gate() {
        let mut o = healthy();
        o.syncs = 0;
        let err = kernel_accounting(&o).expect_err("a zero-sync batch refuses");
        assert!(err.contains("INVALID"), "got: {err}");
        assert!(err.contains("0 durable syncs"), "got: {err}");
        assert!(err.contains("KernelObs::syncs"), "must name the fix: {err}");
    }

    /// T1's no-behaviour-change claim, ASSERTED — mirrors core_accounting's
    /// `while_one_txn_costs_one_fsync_the_gate_input_is_unchanged`. At this
    /// layer one sync per commit means `syncs` equals the log record count
    /// (`requests_planned + stats_jobs_done`), so the published ratio must
    /// equal the old log-rate-based one exactly.
    #[test]
    fn while_one_commit_costs_one_fsync_the_gate_input_is_unchanged() {
        let mut o = healthy();
        o.syncs = o.requests_planned + o.stats_jobs_done; // one fsync per commit
        let a = kernel_accounting(&o).expect("healthy batch");
        let log_rate_ratio = a.log_records_per_sec / o.device_before.max(o.device_after);
        assert_eq!(
            a.device_ratio, log_rate_ratio,
            "with syncs == log records the new gate input must equal the old one \
             exactly, or T1 changed behaviour it claimed not to"
        );
        assert!(a.invalid.is_empty(), "{:?}", a.invalid);
    }

    /// T6 review, M-5 / Noah ruling: the SYNC rate is what the device
    /// ceiling bounds one-for-one — `healthy()`'s fixture below is tuned so
    /// `syncs_per_sec == log_records_per_sec` (the pre-batching shape, still
    /// pinned by `while_one_commit_costs_one_fsync_the_gate_input_is_unchanged`),
    /// which is why this test's own name still speaks of the LOG RECORD
    /// rate: it is checking the SAME number under that fixture, not
    /// asserting the old "two syncs per request" premise (fix round I-1
    /// corrected that premise; see `kernel_accounting`'s own comment).
    #[test]
    fn a_log_rate_above_the_device_ceiling_is_flagged_and_at_the_ceiling_is_not() {
        let mut o = healthy();
        // Exactly at the ceiling: 10_000 log records/s against 10_000 syncs/s.
        // This is where a healthy two-syncs-per-request batch actually sits,
        // which is the whole reason the interlude widened the tolerance.
        o.device_before = 10_000.0;
        o.device_after = 10_000.0;
        let a = kernel_accounting(&o).expect("publishes");
        assert_eq!(a.device_ratio, 1.0);
        assert!(
            a.invalid.is_empty(),
            "1.0 is possible, not flagged: {:?}",
            a.invalid
        );

        // Above the tolerance: flagged by name. The brackets AGREE here, so
        // the tolerance is the floor and 1.2 is genuinely beyond it.
        o.device_before = 10_000.0 / 1.2;
        o.device_after = 10_000.0 / 1.2;
        let a = kernel_accounting(&o).expect("implausible is named, not refused");
        assert_eq!(a.invalid.len(), 1, "got: {:?}", a.invalid);
        assert!(
            a.invalid[0].contains("device_ratio"),
            "got: {:?}",
            a.invalid
        );
        assert!(
            // Fix round I-1: the string this pinned was falsified by
            // batching (T6/T7) and corrected — this now pins the CURRENT
            // physics claim (syncs are counted, not derived) instead.
            a.invalid[0].contains("counted directly at the Disk boundary"),
            "must state this layer's physics: {:?}",
            a.invalid
        );
        assert!(a.invalid[0].contains("INVALID"), "got: {:?}", a.invalid);
    }

    /// 2026-08-10 interlude, the DISCRIMINATING case for this layer's gate.
    ///
    /// Found by mutation: with only the 1.0 and 1.2x probes above, swapping
    /// `implausible_bracketed_ratio` back for the old `ratio > 1.0 + 1e-6`
    /// left every kernel test green — 1.0 passes under both gates and 1.2x
    /// fails under both, so nothing here could tell them apart. This is the
    /// band where they differ, and it is not hypothetical: `bench kernel` run
    /// 2 of the interlude's own evidence measured exactly 1.022, and the old
    /// gate refused it.
    #[test]
    fn a_log_rate_just_over_one_is_inside_this_batchs_measurement_error() {
        let mut o = healthy();
        // 10_000 log records/s against agreeing brackets of 9_785 => 1.022.
        o.device_before = 10_000.0 / 1.022;
        o.device_after = o.device_before;
        let a = kernel_accounting(&o).expect("publishes");
        assert!(
            a.device_ratio > 1.0 + crate::measure::DEVICE_RATIO_SLACK,
            "premise: the OLD gate must have refused this, or the test proves \
             nothing about the new one — ratio {}",
            a.device_ratio
        );
        assert!(
            a.invalid.is_empty(),
            "a two-syncs-per-request batch sits at exactly 1.0, so 1.022 is \
             inside this window's own error and must publish: {:?}",
            a.invalid
        );
    }

    /// The ceiling operand is the LOG rate, not the ack rate — getting this
    /// wrong by a factor of two would silently double the headroom and is
    /// invisible to every other assertion here.
    #[test]
    fn the_ratio_divides_the_log_rate_not_the_ack_rate() {
        let a = kernel_accounting(&healthy()).expect("publishes");
        assert_eq!(a.device_ratio, a.log_records_per_sec / 20_000.0);
        assert_ne!(
            a.device_ratio,
            a.acked_writes_per_sec / 20_000.0,
            "the ack rate would understate the syncs this layer actually spends"
        );
    }

    #[test]
    fn the_floor_refuses_before_the_non_200_arm() {
        let mut o = healthy();
        o.requests_planned = 1;
        o.responses_recorded = 1;
        o.stats_requests = 1;
        o.stats_jobs_done = 1;
        o.first_failure = Some("HTTP 500: boom".to_string());
        let err = kernel_accounting(&o).expect_err("refuses");
        assert!(err.contains("kernel batch"), "the floor wins: {err}");
        assert!(!err.contains("did not return 200"), "got: {err}");
    }
}
