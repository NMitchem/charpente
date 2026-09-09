//! Every arithmetic and validity decision about a compile measurement lives
//! here. `compile.rs` runs builds and decides nothing — the same observations
//! seam as `core_accounting`/`tracing_disk` and `phase_accounting`.
//!
//! **This module carries no deny-list suppression, and that is the point.**
//! Its siblings on the measure side open with
//! `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]` because
//! they touch clocks and processes. This file does not, so hard rule 1's CI
//! deny-list mechanically holds it free of clocks, filesystems and process
//! spawning: it sees only the numbers a caller hands it. If a future edit
//! reaches for `Instant::now()` here, the build fails rather than a reviewer
//! having to notice.
//!
//! **A compile is one-shot, not a rate**, so BENCH's windowing does not apply:
//! there is no denominator to divide by and no per-second figure to publish.
//! Repeated observations are the substitute, which is why [`summarize`]
//! refuses fewer than [`MIN_OBSERVATIONS`].
//!
//! **What each phase tells an adopter** — see [`Phase`]. The three are not
//! interchangeable, and quoting one as "the compile time" is the error this
//! instrument exists to prevent.
//!
//! # BENCH's invalidation rules, adjudicated for this instrument
//!
//! An instrument with no invalidation rule produces numbers nobody may quote,
//! so each of BENCH.md's rules is answered by name. (BENCH lists **six**
//! bullets under "What can invalidate a run", not five as the plan says — the
//! extra one is the bracket-stability gate, added after the device-ratio gate.)
//!
//! | BENCH rule | Here | Why |
//! |---|---|---|
//! | Heartbeat guard | **Applies unchanged** | Supplied by the measure half; [`Observation::suspension`] carries its verdict and [`verdict`] refuses a flagged run. A laptop that sleeps mid-build inflates wall time without inflating CPU. |
//! | Totals two ways | **Applies, re-aimed** | The two independent quantities here are parent and child CPU. A build whose children did not out-consume a parent that only waits was not aggregated — [`Invalid::ChildCpuNotAggregated`]. This is the instrument's own worst failure mode, so it is a gate rather than a note. |
//! | Client saturation | **Does not apply** | There is no client and no server; nothing here measures a remote party that could be the bottleneck instead of the subject. |
//! | Device-ratio gate | **Does not apply** | It bounds a durable *sync rate* against measured hardware. A compile issues no durable syncs whose rate this harness has ever measured, so there is no physical ceiling to compare against. **Nothing replaces it**, and that is a real reduction in this instrument's defences, stated rather than papered over. |
//! | Bracket-stability gate | **Does not apply literally; its question does** | There is no device bracket to straddle a window. Its question — did the measured quantity move underneath us? — is answered by [`Invalid::Unstable`] over repeated observations instead. |
//! | Floors | **Applies, adapted** | "Under one second" becomes [`MIN_CHILD_CPU`] — on compiler CPU rather than wall time, for the reason recorded there; "under five operations" has no analogue for a one-shot, and [`MIN_OBSERVATIONS`] replaces it — repetition is what a one-shot has instead of a window. |
//!
//! **The contention record is a first-class field, not a footnote** (BENCH has
//! been burned once by an unrecorded contention factor, and this machine runs
//! Spotlight and `syspolicyd` hot). [`Observation::load_before`] gates;
//! [`Observation::load_after`] deliberately does not — see its doc for why
//! gating on it would refuse healthy runs.
//!
//! **One caveat that must travel with every number this produces.** The root
//! `Cargo.toml` sets `[profile.dev.package.redb] opt-level = 3`, which slows
//! dev and incremental builds on purpose. A charpente incremental figure is
//! therefore **not representative of a generic Rust project**, and quoting it
//! as one would be wrong in charpente's own favour or against it depending on
//! the phase.

use std::time::Duration;

/// The three phases, and what an adopter learns from each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Everything from scratch, dependencies included. This is the CI number
    /// and the first-clone number — the one an adopter pays once per machine
    /// and once per branch switch that busts the cache. It is dominated by
    /// third-party crates, so it says the least about charpente itself.
    Clean,
    /// One handler body edited, nothing else. **This is the inner-loop number
    /// and the one that decides whether the framework is pleasant to use** —
    /// the edit an app author makes hundreds of times a day. Nothing about the
    /// schema changed, so no macro output downstream of it should need
    /// regenerating.
    HandlerBody,
    /// A `#[table]` schema changed. Every query that mentions that table is
    /// re-typechecked, which is exactly the compile-time contract doing its
    /// job — and exactly the cost DESIGN §9.5's prebuilt-dylib idea targets.
    /// The gap between this and [`Phase::HandlerBody`] is the price of the
    /// oracle, stated rather than hidden.
    Schema,
}

/// Whether the build started from a populated target directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cache {
    /// After `cargo clean` — nothing to reuse.
    Cold,
    /// A populated target directory, which is what an incremental phase means.
    Warm,
}

/// One measured build, as handed over by the measure half. Every field is
/// data: this module does not know how any of it was obtained.
#[derive(Debug, Clone)]
pub struct Observation {
    pub phase: Phase,
    pub cache: Cache,
    /// Wall time from just before the spawn to just after the wait returned.
    pub wall: Duration,
    /// `measure::children_cpu()` differenced across the window — every `rustc`.
    pub child_cpu: Duration,
    /// `measure::process_cpu()` differenced across the same window. The parent
    /// only waits, so this is the control that proves `child_cpu` is not
    /// secretly the parent's own figure.
    pub parent_cpu: Duration,
    /// Logical cores available, supplied by the caller — this module may not
    /// ask the OS. Used only to normalize the contention record.
    pub cores: usize,
    /// 1-minute load average sampled **before** the build. This is the
    /// contention record: it describes the machine this measurement entered.
    pub load_before: Option<f64>,
    /// The same, sampled after. **Deliberately not a gate.** A 1-minute load
    /// average rises because of our own build, so a higher `load_after` is the
    /// expected consequence of measuring, not evidence of a foreign process.
    /// It is carried as a disclosure so a reader can see the machine's state,
    /// and gating on it would refuse healthy runs for doing their job.
    pub load_after: Option<f64>,
    /// `None` when the measure half's heartbeat saw a settled machine; the
    /// reason string when it did not.
    pub suspension: Option<String>,
    /// Whether the build actually succeeded. A failed build stops early and
    /// its timing measures the failure, not a compile.
    pub succeeded: bool,
}

/// Why a reading may not be published. Each names the rule it broke.
#[derive(Debug, Clone, PartialEq)]
pub enum Invalid {
    BuildFailed,
    Suspended(String),
    /// The heartbeat's compile-shaped sibling: the parent process only waits,
    /// so a build that reports no more CPU than its parent did not aggregate
    /// its `rustc` children. This is the totals-two-ways rule for this
    /// instrument, and the failure it catches is `process_cpu()` used where
    /// `children_cpu()` was meant.
    ChildCpuNotAggregated {
        child_millis: u128,
        parent_millis: u128,
    },
    /// Below the floor: too little compiler CPU for a compiler to have done
    /// meaningful work.
    BelowFloor {
        child_cpu_millis: u128,
    },
    /// The machine was already busy before the build began.
    Contended {
        load_before: f64,
        cores: usize,
    },
    /// Too few observations to publish a one-shot quantity.
    TooFewObservations {
        got: usize,
    },
    /// The observations disagree so widely that they are not samples of one
    /// quantity — the compile-shaped replacement for the bracket-stability
    /// gate.
    Unstable {
        spread_percent: u64,
    },
}

impl std::fmt::Display for Invalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Invalid::BuildFailed => write!(
                f,
                "the build did not succeed, so its timing measures a failure and not a compile"
            ),
            Invalid::Suspended(why) => write!(
                f,
                "the machine did not stay settled across the build ({why}) — wall time spans a \
                 gap the process did not run through"
            ),
            Invalid::ChildCpuNotAggregated {
                child_millis,
                parent_millis,
            } => write!(
                f,
                "child CPU ({child_millis} ms) did not exceed parent CPU ({parent_millis} ms): \
                 the parent only waits, so this build's rustc processes were NOT aggregated. \
                 Use measure::children_cpu (RUSAGE_CHILDREN), not measure::process_cpu"
            ),
            Invalid::BelowFloor { child_cpu_millis } => write!(
                f,
                "the build spent {child_cpu_millis} ms of compiler CPU, under the {} ms floor \
                 — that is a no-op rebuild in which cargo checked fingerprints and spawned no \
                 real work, not a compile",
                MIN_CHILD_CPU.as_millis()
            ),
            Invalid::Contended { load_before, cores } => write!(
                f,
                "load average was {load_before:.2} across {cores} cores before the build \
                 started, over the {CONTENTION_CEILING_PER_CORE:.2}/core ceiling — the machine \
                 was already busy, and BENCH.md has been burned once by an unrecorded \
                 contention factor"
            ),
            Invalid::TooFewObservations { got } => write!(
                f,
                "{got} observation(s): a compile is one-shot rather than a rate, so repetition \
                 is the only thing standing in for a window. {MIN_OBSERVATIONS} are required"
            ),
            Invalid::Unstable { spread_percent } => write!(
                f,
                "the observations spread {spread_percent}%, over the \
                 {MAX_SPREAD_PERCENT}% limit — they are not samples of one quantity, and a \
                 median over them would have no defensible meaning"
            ),
        }
    }
}

impl Invalid {
    /// Whether this refusal describes the MACHINE rather than the measurement.
    /// Only these may be retried (spec §4): re-running after a busy or
    /// suspended box is legitimate, while re-running after `Unstable` is
    /// rerolling until the spread looks tight, and re-running after
    /// `BelowFloor`, `BuildFailed` or `ChildCpuNotAggregated` hides a defect
    /// instead of reporting it.
    ///
    /// `TooFewObservations` is environmental only in that it is always
    /// downstream of per-observation refusals. The driver therefore classifies
    /// a phase by ALL of its refusals, not by the summary's alone — otherwise a
    /// `BelowFloor` observation would be laundered into a retryable verdict.
    pub fn is_environmental(&self) -> bool {
        match self {
            Invalid::Contended { .. }
            | Invalid::Suspended(_)
            | Invalid::TooFewObservations { .. } => true,
            Invalid::BuildFailed
            | Invalid::ChildCpuNotAggregated { .. }
            | Invalid::BelowFloor { .. }
            | Invalid::Unstable { .. } => false,
        }
    }
}

/// Compiler CPU below which no meaningful compilation happened.
///
/// **This floor is on CPU, not wall time, and the first reading is why.** It
/// began as `MIN_WALL = 1500 ms` on the reasoning that a short build must be
/// dominated by cargo's startup and fingerprint scan. Taking the reading
/// refuted that: incremental builds of `examples/invoicing` finish in
/// 456–897 ms of wall while spending **1.5–2.1 s of child CPU** at a
/// parallelism of 2.6–3.7. Real `rustc` work, refused by a rule that had
/// mistaken a short wall clock for an absent compiler.
///
/// Wall time was never the quantity the rule cared about. What it wanted to
/// exclude is a **no-op rebuild** — cargo checks fingerprints, spawns no
/// `rustc`, and returns — and compiler CPU measures that directly instead of
/// by proxy. A no-op rebuild spawns no compiler at all, so its child CPU is
/// cargo's own fingerprint scan and nothing more; a real incremental build is
/// an order of magnitude above it. The threshold sits in that gap.
///
/// Changing a gate after it refuses a reading is exactly how a real oracle
/// becomes decorative, so the justification is the evidence above and not the
/// inconvenience: the old rule was refuting its own stated reason.
pub const MIN_CHILD_CPU: Duration = Duration::from_millis(500);

/// Load average per core, sampled before the build, above which the machine
/// was already busy. Half a core's worth of pre-existing work is generous —
/// this machine idles far below it — and anything above means whatever else is
/// running competed with `rustc` for the cores the number is divided by.
pub const CONTENTION_CEILING_PER_CORE: f64 = 0.5;

/// A one-shot quantity needs repetition where a rate would have a window.
pub const MIN_OBSERVATIONS: usize = 3;

/// How far the fastest and slowest observation may sit apart before they stop
/// being samples of one quantity. The compile-shaped replacement for BENCH's
/// bracket-stability gate: there is no device bracket here, but the same
/// question applies — did the thing being measured move underneath us?
pub const MAX_SPREAD_PERCENT: u64 = 30;

/// One accepted build.
#[derive(Debug, Clone, PartialEq)]
pub struct Reading {
    pub phase: Phase,
    pub cache: Cache,
    pub wall: Duration,
    pub cpu: Duration,
    /// `cpu / wall` — how many cores the build kept busy on average. Reported
    /// rather than gated: a low figure on an incremental build is a real
    /// property of that build (one crate, one `rustc`), not a defect.
    pub parallelism: f64,
}

/// Accepts or refuses a single observation. Every rejection names its rule.
pub fn verdict(o: &Observation) -> Result<Reading, Invalid> {
    if !o.succeeded {
        return Err(Invalid::BuildFailed);
    }
    if let Some(why) = &o.suspension {
        return Err(Invalid::Suspended(why.clone()));
    }
    if o.child_cpu <= o.parent_cpu {
        return Err(Invalid::ChildCpuNotAggregated {
            child_millis: o.child_cpu.as_millis(),
            parent_millis: o.parent_cpu.as_millis(),
        });
    }
    if o.child_cpu < MIN_CHILD_CPU {
        return Err(Invalid::BelowFloor {
            child_cpu_millis: o.child_cpu.as_millis(),
        });
    }
    if let Some(load) = o.load_before {
        let cores = o.cores.max(1);
        if load / cores as f64 > CONTENTION_CEILING_PER_CORE {
            return Err(Invalid::Contended {
                load_before: load,
                cores,
            });
        }
    }
    Ok(Reading {
        phase: o.phase,
        cache: o.cache,
        wall: o.wall,
        cpu: o.child_cpu,
        parallelism: o.child_cpu.as_secs_f64() / o.wall.as_secs_f64(),
    })
}

/// The published figure over repeated observations of one phase.
#[derive(Debug, Clone, PartialEq)]
pub struct Summary {
    pub phase: Phase,
    pub n: usize,
    pub median_wall: Duration,
    pub fastest_wall: Duration,
    pub slowest_wall: Duration,
    pub median_cpu: Duration,
    pub spread_percent: u64,
}

/// Combines readings of one phase into the figure BENCH may quote. Refuses
/// too few, and refuses a set too spread out to have a median worth stating.
pub fn summarize(phase: Phase, readings: &[Reading]) -> Result<Summary, Invalid> {
    if readings.len() < MIN_OBSERVATIONS {
        return Err(Invalid::TooFewObservations {
            got: readings.len(),
        });
    }
    let mut walls: Vec<Duration> = readings.iter().map(|r| r.wall).collect();
    let mut cpus: Vec<Duration> = readings.iter().map(|r| r.cpu).collect();
    walls.sort_unstable();
    cpus.sort_unstable();
    let fastest = walls[0];
    let slowest = walls[walls.len() - 1];
    // Spread against the FASTEST, not the median: it is the reading least
    // likely to contain someone else's work, so it is the one a slow outlier
    // has to be explained against.
    let spread_percent = (((slowest.as_secs_f64() / fastest.as_secs_f64()) - 1.0) * 100.0).round();
    let spread_percent = spread_percent.max(0.0) as u64;
    if spread_percent > MAX_SPREAD_PERCENT {
        return Err(Invalid::Unstable { spread_percent });
    }
    Ok(Summary {
        phase,
        n: readings.len(),
        median_wall: walls[walls.len() / 2],
        fastest_wall: fastest,
        slowest_wall: slowest,
        median_cpu: cpus[cpus.len() / 2],
        spread_percent,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A healthy observation: a real build on an idle 10-core machine.
    fn healthy() -> Observation {
        Observation {
            phase: Phase::HandlerBody,
            cache: Cache::Warm,
            wall: Duration::from_secs(10),
            child_cpu: Duration::from_secs(40),
            parent_cpu: Duration::from_millis(30),
            cores: 10,
            load_before: Some(1.0),
            load_after: Some(6.0),
            suspension: None,
            succeeded: true,
        }
    }

    #[test]
    fn a_healthy_observation_is_accepted_and_reports_parallelism() {
        let r = verdict(&healthy()).expect("healthy");
        assert!((r.parallelism - 4.0).abs() < 1e-9, "{}", r.parallelism);
        assert_eq!(r.cpu, Duration::from_secs(40), "cpu is the CHILDREN's cpu");
    }

    /// The instrument's own worst failure mode: `process_cpu()` used where
    /// `children_cpu()` was meant. The parent only waits, so the numbers
    /// collapse together and the published figure would be milliseconds.
    #[test]
    fn child_cpu_that_did_not_aggregate_rustc_is_refused_by_name() {
        let mut o = healthy();
        o.child_cpu = o.parent_cpu; // what RUSAGE_SELF would have produced
        let err = verdict(&o).expect_err("must refuse");
        let text = err.to_string();
        assert!(text.contains("children_cpu"), "must name the fix: {text}");
        assert!(
            text.contains("RUSAGE_CHILDREN"),
            "must name the probe: {text}"
        );
    }

    /// A no-op rebuild — cargo checked fingerprints and spawned no compiler.
    #[test]
    fn a_no_op_rebuild_is_refused_on_compiler_cpu_not_wall_time() {
        let mut o = healthy();
        o.child_cpu = MIN_CHILD_CPU - Duration::from_millis(1);
        assert!(matches!(verdict(&o), Err(Invalid::BelowFloor { .. })));
    }

    /// The regression the first reading exposed. A wall-time floor discarded
    /// every incremental observation in that run, including builds with two
    /// seconds of `rustc` behind a sub-second wall clock.
    #[test]
    fn a_fast_build_with_real_compiler_cpu_is_accepted() {
        let mut o = healthy();
        o.wall = Duration::from_millis(700);
        o.child_cpu = Duration::from_millis(2000);
        verdict(&o).expect("456-897 ms wall over 1.5-2.1 s of rustc CPU is a real build");
    }

    #[test]
    fn a_machine_already_busy_is_refused_before_its_number_is_believed() {
        let mut o = healthy();
        o.load_before = Some(9.0); // 0.9/core on 10 cores
        assert!(matches!(verdict(&o), Err(Invalid::Contended { .. })));
    }

    /// `load_after` is a disclosure, never a gate. Our own build drives the
    /// 1-minute average up by design, so gating on it would refuse healthy
    /// runs for the crime of doing work.
    #[test]
    fn load_after_is_a_disclosure_and_never_refuses_a_run() {
        let mut o = healthy();
        o.load_after = Some(60.0); // our own rustc processes, six per core
        verdict(&o).expect("a build's own load must not invalidate it");
    }

    #[test]
    fn a_failed_build_is_not_a_measurement() {
        let mut o = healthy();
        o.succeeded = false;
        assert_eq!(verdict(&o), Err(Invalid::BuildFailed));
    }

    #[test]
    fn a_run_the_heartbeat_flagged_is_refused() {
        let mut o = healthy();
        o.suspension = Some("laptop slept".to_string());
        assert!(matches!(verdict(&o), Err(Invalid::Suspended(_))));
    }

    fn reading(secs: u64) -> Reading {
        Reading {
            phase: Phase::HandlerBody,
            cache: Cache::Warm,
            wall: Duration::from_secs(secs),
            cpu: Duration::from_secs(secs * 4),
            parallelism: 4.0,
        }
    }

    #[test]
    fn one_shot_quantities_need_repetition_where_a_rate_would_have_a_window() {
        let got = summarize(Phase::HandlerBody, &[reading(10), reading(10)]);
        assert_eq!(got, Err(Invalid::TooFewObservations { got: 2 }));
    }

    #[test]
    fn a_set_too_spread_out_has_no_median_worth_stating() {
        // 10s vs 20s is 100% spread — not two samples of one quantity.
        let err = summarize(Phase::HandlerBody, &[reading(10), reading(12), reading(20)])
            .expect_err("must refuse");
        assert!(matches!(err, Invalid::Unstable { .. }), "{err}");
    }

    #[test]
    fn a_stable_set_summarizes_with_its_spread_stated() {
        let s = summarize(Phase::HandlerBody, &[reading(10), reading(11), reading(12)])
            .expect("within the limit");
        assert_eq!(s.n, 3);
        assert_eq!(s.median_wall, Duration::from_secs(11));
        assert_eq!(s.fastest_wall, Duration::from_secs(10));
        assert_eq!(s.slowest_wall, Duration::from_secs(12));
        assert_eq!(s.spread_percent, 20, "measured against the FASTEST reading");
    }

    /// Spec §4. Retrying a refusal that describes the MACHINE is legitimate;
    /// retrying one that describes the MEASUREMENT is rerolling a result until
    /// it looks tight. The split is encoded here rather than left to a shell
    /// script's judgement at 3 a.m.
    #[test]
    fn only_refusals_about_the_machine_may_be_retried() {
        assert!(
            Invalid::Contended {
                load_before: 9.0,
                cores: 10
            }
            .is_environmental()
        );
        assert!(Invalid::Suspended("the box slept".into()).is_environmental());
        assert!(Invalid::TooFewObservations { got: 1 }.is_environmental());

        assert!(!Invalid::Unstable { spread_percent: 90 }.is_environmental());
        assert!(
            !Invalid::BelowFloor {
                child_cpu_millis: 10
            }
            .is_environmental()
        );
        assert!(!Invalid::BuildFailed.is_environmental());
        assert!(
            !Invalid::ChildCpuNotAggregated {
                child_millis: 1,
                parent_millis: 2
            }
            .is_environmental()
        );
    }
}
