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

use std::path::PathBuf;

use bench_harness::compile::{Edit, clean, measure_build};
use bench_harness::compile_accounting::{
    Cache, Invalid, Observation, Phase, PhaseVerdict, Reading, classify_phase, exit_code,
    settle_verdict, summarize, target_dir_inside_repo, verdict,
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

/// Absolute, symlink-resolved, and tolerant of a path that does not exist yet
/// — cargo creates the target directory, so the check that it is outside the
/// repo has to run before it is there. Canonicalizes the deepest existing
/// ancestor and re-attaches the rest, which is what makes macOS's
/// `/tmp -> /private/tmp` and `/var -> /private/var` compare correctly against
/// a canonicalized repo root. Filesystem access, so it stays in the driver:
/// `compile_accounting` owns the rule and never resolves a path.
fn resolve(p: &Path) -> PathBuf {
    let abs = if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(p)
    };
    for ancestor in abs.ancestors() {
        if let Ok(real) = ancestor.canonicalize() {
            let rest = abs.strip_prefix(ancestor).expect("ancestor of abs");
            return real.join(rest);
        }
    }
    abs
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
    let Ok(target_dir) = std::env::var("CARGO_TARGET_DIR") else {
        eprintln!(
            "compile-bench: refusing to run without a dedicated CARGO_TARGET_DIR: the clean \
             phase deletes artifacts, and sharing the repo's target/ would destroy the \
             working cache and let unrelated cargo runs perturb the measurement"
        );
        std::process::exit(2);
    };
    // Set is not the same as dedicated, and "dedicated" is the rule the number
    // depends on. Point CARGO_TARGET_DIR at the repo's own target/ and a
    // concurrent cargo blocks this build on the target-dir lock: the wait lands
    // in `wall` while child_cpu stays normal and the load average stays low, so
    // BelowFloor, Contended and Unstable all pass and an inflated wall figure is
    // published as accepted. The gate this class of defect defeats is every gate
    // there is, which is why it is refused here rather than left to the script.
    if target_dir_inside_repo(&resolve(&PathBuf::from(&target_dir)), &resolve(repo)) {
        eprintln!(
            "compile-bench: refusing to run with CARGO_TARGET_DIR inside the repository.\n  \
             CARGO_TARGET_DIR = {target_dir}\n  repository       = {}\n  \
             The rule: the target directory must be DEDICATED and OUTSIDE the repo. Sharing it \
             means a concurrent cargo can block this build on the target-dir lock — that wait \
             lands in wall time while child CPU and load average stay normal, so every gate \
             passes and an inflated figure is published as accepted. The clean phase would also \
             delete the repo's working cache.\n  \
             Fix: CARGO_TARGET_DIR=\"${{TMPDIR:-/tmp}}/charpente-compile-bench-target\" (and not \
             a git worktree's target either — a shared target dir leaks one tree's artifacts \
             into another's verification with `git diff` clean).",
            repo.display()
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
        // This one settles a tree that the clean build above left already
        // built and unedited, so it is the GENUINE no-op — see
        // `no_op_evidence`.
        let settle = measure_build(&app, Phase::HandlerBody, Cache::Warm).expect("settle");
        no_op_evidence(&settle, i, "handler");
        judge_settle(&settle, &mut handler_refusals, i, "handler");
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
        // NOT a no-op: the handler `Edit` guard dropped at the close of the
        // block above and rewrote routes.rs, so this settle recompiles the
        // revert. It is a full incremental build wearing a settle's name, and
        // labelling it NOOP is how a future reader would recalibrate
        // MIN_CHILD_CPU upward off a number that never measured a no-op.
        let settle = measure_build(&app, Phase::Schema, Cache::Warm).expect("settle");
        settle_evidence(&settle, i, "schema");
        judge_settle(&settle, &mut schema_refusals, i, "schema");
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

    let mut verdicts: Vec<PhaseVerdict> = Vec::new();

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
        let v = classify_phase(&summary, refusals);
        println!("VERDICT {label}: {v:?}");
        verdicts.push(v);
    }

    // The fold lives in the accounting half (compile_accounting::exit_code) and
    // is covered there. Inline, this was the last hop of spec §4's integrity
    // property with no oracle at all: transposing the 3 and the 1 turned "never
    // retry Unstable" into "always retry Unstable" with the whole suite green.
    let code = exit_code(&verdicts);
    println!("VERDICT exit={code} (0 accepted, 1 refused-retryable, 2 usage, 3 refused-final)");
    std::process::exit(code);
}

/// The **handler** settle, and only that one, is a genuine no-op rebuild: the
/// clean build immediately above it left the tree built and unedited, so cargo
/// checks fingerprints and spawns no compiler. Printing its compiler CPU is
/// what calibrates `MIN_CHILD_CPU` against something measured rather than
/// assumed — the floor is only meaningful if the case it exists to reject
/// actually sits well below it.
///
/// **`NOOP` is printed here and nowhere else, deliberately.** The schema
/// phase's settle recompiles the reverted handler edit and is a full
/// incremental build; it prints `SETTLE` (see [`settle_evidence`]) so nobody
/// can read it as floor evidence. This line is the only no-op calibration this
/// instrument produces.
fn no_op_evidence(o: &Observation, i: usize, label: &str) {
    println!(
        "NOOP {label}[{i}] wall={:?} child_cpu={:?}   (calibrates the compiler-CPU floor)",
        o.wall, o.child_cpu
    );
}

/// A settle build that is NOT a no-op: it exists to get the tree back to a
/// known state before the next measured edit, and it recompiles whatever the
/// previous phase's `Edit` guard reverted. Reported for the trace, never as
/// floor evidence, and never published as a reading.
fn settle_evidence(o: &Observation, i: usize, label: &str) {
    println!(
        "SETTLE {label}[{i}] wall={:?} child_cpu={:?}   (rebuilds the reverted edit — NOT a \
         no-op, NOT floor evidence)",
        o.wall, o.child_cpu
    );
}

/// A settle is not published, but it is a real build, and before this existed
/// neither settle passed through any check at all: a settle that failed to
/// build, or that spanned a laptop suspend, left no trace and every
/// observation taken after it was believed anyway. The floor and the
/// aggregation rule deliberately do NOT apply — a genuine settle sits below
/// the floor by construction — so this goes through
/// `compile_accounting::settle_verdict` rather than `verdict`.
fn judge_settle(o: &Observation, refusals: &mut Vec<Invalid>, i: usize, label: &str) {
    if let Some(e) = settle_verdict(o) {
        println!("    SETTLE REFUSED {label}[{i}] — {e}");
        refusals.push(e);
    }
}

fn record(
    into: &mut Vec<Reading>,
    refusals: &mut Vec<Invalid>,
    o: Observation,
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
