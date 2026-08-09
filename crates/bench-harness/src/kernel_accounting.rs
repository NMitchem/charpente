//! The accounting half of layer 3. Interlude spec
//! docs/superpowers/specs/2026-08-08-bench-accounting-split-design.md §5 rules
//! that this layer adopts the observations seam from inception rather than
//! having it retrofitted: every crosscheck, rate division and INVALID decision
//! the kernel layer makes is a pure function over observations already taken.
//!
//! Nothing here reads a clock, touches a database, a disk, a socket or a
//! thread. That is enforced rather than promised: unlike `kernel_bench` beside
//! it, this module carries no blanket
//! `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]`, so hard
//! rule 1's workspace deny-list is what holds it.

use crate::measure::{crosscheck, degenerate_window_reason};
use std::time::Duration;

/// What one measured kernel batch observed. Every field is a count, a duration
/// or a verdict that was actually observed; nothing here is derived and
/// nothing here is a decision.
#[derive(Debug, Clone, Default)]
pub struct KernelObs {
    /// Requests pushed into the source before `Kernel::run`.
    pub requests_planned: u64,
    /// Responses the recording sink actually collected — totals source 1.
    pub responses_recorded: u64,
    /// `KernelStats::requests` — totals source 2, counted by the kernel's own
    /// dispatch rather than by the sink.
    pub stats_requests: u64,
    /// `KernelStats::jobs_done` — totals source 3. Every `/hit/:id` enqueues
    /// exactly one `Bump` (lib.rs's canonical route), so a drained batch must
    /// finish exactly as many jobs as it served requests.
    pub stats_jobs_done: u64,
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
    pub invalid: Vec<String>,
}

/// The kernel layer's accounting. `Err` refuses the run — both refusals mean
/// the batch is not a measurement of the serving path at all. `Ok` carries the
/// published values plus every non-fatal reason in push order.
///
/// Decision order mirrors `core_accounting::read_accounting` deliberately: the
/// heartbeat reason is collected first and is non-fatal, then the two
/// conditions that mean there is nothing to measure refuse by name, then the
/// non-fatal crosschecks. Unlike the core layer there is no expensive work
/// downstream of this call, so no short-circuit ordering constraint applies
/// (interlude spec §2 is about `run`'s population loop, which has no analogue
/// here) — the order is for reader parity across layers, and it is pinned.
pub fn kernel_accounting(k: &KernelObs) -> Result<KernelAccounting, String> {
    let mut invalid = Vec::new();
    if let Some(reason) = &k.heartbeat {
        invalid.push(reason.clone());
    }
    if let Some(reason) = degenerate_window_reason("kernel batch", k.requests_planned, k.elapsed) {
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
    // These are genuinely independent at this layer, unlike the core write
    // loop's structurally-vacuous pair.
    if let Err(reason) = crosscheck(&[k.responses_recorded], k.requests_planned) {
        invalid.push(format!(
            "{reason} (recorded responses must equal the requests pushed)"
        ));
    }
    if let Err(reason) = crosscheck(&[k.stats_requests], k.requests_planned) {
        invalid.push(format!(
            "{reason} (KernelStats::requests must equal the requests pushed)"
        ));
    }
    if let Err(reason) = crosscheck(&[k.stats_jobs_done], k.requests_planned) {
        invalid.push(format!(
            "{reason} (jobs_done must equal requests: every /hit/:id enqueues exactly one Bump)"
        ));
    }

    let secs = k.elapsed.as_secs_f64();
    Ok(KernelAccounting {
        acked_writes_per_sec: k.requests_planned as f64 / secs,
        // Each request commits one effects record; each Bump's Done-mark
        // commits another (spec §3) — 2x by construction at this layer, which
        // is why this is a SUM and not a copy of the ack rate. Deriving it
        // from `stats_jobs_done` rather than from `requests_planned` twice is
        // deliberate: if the job counter ever disagrees, the published number
        // shows it instead of hiding it behind an assumed multiplier.
        log_records_per_sec: (k.requests_planned + k.stats_jobs_done) as f64 / secs,
        invalid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::measure::MIN_MEASURED_OPS;

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
            first_failure: None,
            elapsed: Duration::from_secs(2),
            heartbeat: None,
        }
    }

    #[test]
    fn a_healthy_batch_publishes_and_flags_nothing() {
        let a = kernel_accounting(&healthy()).expect("a healthy batch publishes");
        assert!(a.invalid.is_empty(), "got: {:?}", a.invalid);
        assert_eq!(a.acked_writes_per_sec, 5_000.0);
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
        let a = kernel_accounting(&o).expect("all four of these are non-fatal");
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
                } else {
                    "UNKNOWN"
                }
            })
            .collect();
        assert_eq!(
            shape,
            vec!["heartbeat", "sink", "kernel", "jobs"],
            "got: {:?}",
            a.invalid
        );
    }

    /// The floor refuses BEFORE the non-200 arm, so a degenerate batch that
    /// also failed names the floor — the condition an operator must fix first.
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
