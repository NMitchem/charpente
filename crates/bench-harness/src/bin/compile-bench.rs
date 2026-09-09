//! Takes the compile-latency reading (oracle-density Task 12). The instrument
//! is `bench_harness::compile` + `compile_accounting`; this binary only drives
//! it and prints what came back. Every publishable/refused decision belongs to
//! the accounting half.
//!
//! Run against a dedicated `CARGO_TARGET_DIR` — the clean phase deletes build
//! artifacts, and pointing it at the repo's own `target/` would both destroy
//! the working cache and let an unrelated `cargo` run perturb the measurement.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::path::Path;
use std::time::Duration;

use bench_harness::compile::{Edit, clean, measure_build};
use bench_harness::compile_accounting::{
    Cache, Invalid, Phase, PhaseVerdict, Reading, classify_phase, summarize, verdict,
};
use bench_harness::measure::load_average;

/// The handler-body edit: one statement added inside a route body. No
/// signature changes, so nothing downstream of it can need re-typechecking —
/// which is exactly what makes it the inner-loop number.
const HANDLER_FIND: &str = "    Ok(tx.scan::<Customer>()?.into_iter().map(Into::into).collect())";
const HANDLER_REPLACE: &str = "    let _compile_bench: u8 = 0;\n    Ok(tx.scan::<Customer>()?.into_iter().map(Into::into).collect())";

/// The schema edit: an index added to an existing `#[table]` field. A real
/// schema change that still compiles — adding a *field* would break every
/// constructor and measure a build failure instead of a recompile.
const SCHEMA_FIND: &str = "    /// App-relative day this invoice is due, in `AppClock` units — not a\n    /// wall-clock timestamp (see module doc).\n    pub due_day: u64,";
const SCHEMA_REPLACE: &str = "    /// App-relative day this invoice is due, in `AppClock` units — not a\n    /// wall-clock timestamp (see module doc).\n    #[index]\n    pub due_day: u64,";

const OBSERVATIONS: usize = 3;

/// Wait for the machine to go quiet before each measured build, and why one is
/// needed at all: **a measured build poisons the next observation's contention
/// record.** The load average is a one-minute exponential average, so the
/// 44 seconds of compiler CPU this harness itself spends on a clean build is
/// still inside the window when the next observation samples `load_before`.
/// The first reading refused three clean builds for contention that the
/// harness had substantially manufactured.
///
/// Polling for the condition rather than sleeping a fixed span keeps a
/// genuinely idle machine fast and a busy one honest: if the load never falls,
/// the wait expires and the observation is taken anyway, so
/// [`Invalid::Contended`] still gets to refuse it. This is a driver concern,
/// not an accounting one — the gate is untouched.
const QUIESCE_TARGET_PER_CORE: f64 = 0.4;
const QUIESCE_POLL: Duration = Duration::from_secs(10);
const QUIESCE_MAX: Duration = Duration::from_secs(300);

/// Blocks until the load average falls under [`QUIESCE_TARGET_PER_CORE`], or
/// until [`QUIESCE_MAX`] elapses. Reports either way.
fn quiesce(cores: usize) {
    let target = QUIESCE_TARGET_PER_CORE * cores as f64;
    let start = std::time::Instant::now();
    loop {
        let Some(load) = load_average() else {
            eprintln!("  quiesce: no load average available, not waiting");
            return;
        };
        if load <= target {
            eprintln!(
                "  quiesce: load {load:.2} <= {target:.2} after {:?}",
                start.elapsed()
            );
            return;
        }
        if start.elapsed() >= QUIESCE_MAX {
            eprintln!(
                "  quiesce: gave up after {:?} at load {load:.2} (target {target:.2}) — \
                 taking the observation anyway and letting the contention gate judge it",
                start.elapsed()
            );
            return;
        }
        eprintln!("  quiesce: load {load:.2} > {target:.2}, waiting");
        std::thread::sleep(QUIESCE_POLL);
    }
}

fn main() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let app = repo.join("examples/invoicing");
    let routes = app.join("src/routes.rs");
    let model = app.join("src/model.rs");

    eprintln!("compile-bench: app = {}", app.display());
    eprintln!(
        "compile-bench: CARGO_TARGET_DIR = {:?}",
        std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "<unset — refusing>".into())
    );
    if std::env::var("CARGO_TARGET_DIR").is_err() {
        eprintln!(
            "compile-bench: refusing to run without a dedicated CARGO_TARGET_DIR: the clean \
             phase deletes artifacts, and sharing the repo's target/ would destroy the \
             working cache and let unrelated cargo runs perturb the measurement"
        );
        std::process::exit(2);
    }

    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    eprintln!("compile-bench: cores = {cores}");

    let mut clean_readings = Vec::new();
    let mut handler_readings = Vec::new();
    let mut schema_readings = Vec::new();
    let mut clean_refusals: Vec<Invalid> = Vec::new();
    let mut handler_refusals: Vec<Invalid> = Vec::new();
    let mut schema_refusals: Vec<Invalid> = Vec::new();

    for i in 0..OBSERVATIONS {
        // --- clean --------------------------------------------------------
        clean(&app).expect("cargo clean");
        quiesce(cores);
        record(
            &mut clean_readings,
            &mut clean_refusals,
            measure_build(&app, Phase::Clean, Cache::Cold).expect("clean build"),
            i,
            "clean",
        );

        // --- incremental: handler body -----------------------------------
        // Settle first: the tree must be built and unedited, or the measured
        // build would also be paying for whatever the previous phase left.
        let settle = measure_build(&app, Phase::HandlerBody, Cache::Warm).expect("settle");
        no_op_evidence(&settle, i, "handler");
        quiesce(cores);
        {
            let _edit = Edit::apply(&routes, HANDLER_FIND, HANDLER_REPLACE).expect("handler edit");
            record(
                &mut handler_readings,
                &mut handler_refusals,
                measure_build(&app, Phase::HandlerBody, Cache::Warm).expect("handler build"),
                i,
                "handler",
            );
        }

        // --- incremental: schema ------------------------------------------
        let settle = measure_build(&app, Phase::Schema, Cache::Warm).expect("settle");
        no_op_evidence(&settle, i, "schema");
        quiesce(cores);
        {
            let _edit = Edit::apply(&model, SCHEMA_FIND, SCHEMA_REPLACE).expect("schema edit");
            record(
                &mut schema_readings,
                &mut schema_refusals,
                measure_build(&app, Phase::Schema, Cache::Warm).expect("schema build"),
                i,
                "schema",
            );
        }
    }

    let mut any_refused = false;
    let mut any_final = false;

    for (label, phase, readings, refusals) in [
        ("clean", Phase::Clean, &clean_readings, &clean_refusals),
        (
            "handler-body",
            Phase::HandlerBody,
            &handler_readings,
            &handler_refusals,
        ),
        ("schema", Phase::Schema, &schema_readings, &schema_refusals),
    ] {
        let summary = summarize(phase, readings);
        match &summary {
            Ok(s) => println!(
                "SUMMARY {label}: n={} median_wall={:?} fastest={:?} slowest={:?} \
                 median_cpu={:?} spread={}%",
                s.n, s.median_wall, s.fastest_wall, s.slowest_wall, s.median_cpu, s.spread_percent
            ),
            Err(e) => println!("SUMMARY {label}: REFUSED — {e}"),
        }
        // Classified by ALL of the phase's refusals, not just summarize's own
        // (compile_accounting::classify_phase) — a BelowFloor or BuildFailed
        // refusal cannot hide behind a TooFewObservations OR an Ok summary.
        match classify_phase(&summary, refusals) {
            PhaseVerdict::Accepted => {}
            PhaseVerdict::Retryable => any_refused = true,
            PhaseVerdict::Final => {
                any_refused = true;
                any_final = true;
            }
        }
    }

    let code = if any_final {
        3
    } else if any_refused {
        1
    } else {
        0
    };
    println!("VERDICT exit={code} (0 accepted, 1 refused-retryable, 2 usage, 3 refused-final)");
    std::process::exit(code);
}

/// The settle build is a no-op rebuild: the tree is already built and
/// unedited, so cargo checks fingerprints and spawns no compiler. Printing its
/// compiler CPU is what calibrates [`MIN_CHILD_CPU`] against something measured
/// rather than assumed — the floor is only meaningful if the case it exists to
/// reject actually sits well below it.
fn no_op_evidence(o: &bench_harness::compile_accounting::Observation, i: usize, label: &str) {
    println!(
        "NOOP {label}[{i}] wall={:?} child_cpu={:?}   (calibrates the compiler-CPU floor)",
        o.wall, o.child_cpu
    );
}

fn record(
    into: &mut Vec<Reading>,
    refusals: &mut Vec<Invalid>,
    o: bench_harness::compile_accounting::Observation,
    i: usize,
    label: &str,
) {
    println!(
        "OBS {label}[{i}] wall={:?} child_cpu={:?} parent_cpu={:?} load_before={:?} \
         load_after={:?} cores={}",
        o.wall, o.child_cpu, o.parent_cpu, o.load_before, o.load_after, o.cores
    );
    match verdict(&o) {
        Ok(r) => {
            println!("    accepted, parallelism={:.2}", r.parallelism);
            into.push(r);
        }
        Err(e) => {
            println!("    REFUSED — {e}");
            refusals.push(e);
        }
    }
}
