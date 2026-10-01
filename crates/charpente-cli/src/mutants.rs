//! `charpente test --mutants`: run `cargo-mutants` over a NAMED target set and
//! report caught / missed / unviable / timed-out **separately**.
//!
//! The ruling this implements is
//! `docs/superpowers/specs/2026-09-06-oracle-density-design.md`, "Mutation
//! mode, ruled": mutation happens by patching a scratch copy of the source,
//! never by a neutering mode compiled into the platform.
//!
//! **This module exists because the raw tool exits 0 in two situations where
//! nothing was measured**, and both were observed on this workspace rather
//! than assumed:
//!
//! - a filter that matches nothing prints "Found 0 mutants to test", writes no
//!   `outcomes.json` at all, and exits **0**;
//! - a failed baseline exits 4, but the `outcomes.json` it leaves behind reads
//!   `total_mutants: 0, missed: 0, caught: 0` — every counter zero, which to a
//!   reader of the JSON alone is indistinguishable from a clean sweep.
//!
//! Both are the "absent measurement wearing the shape of a passing one" shape
//! this sub-project exists to remove, so every one of them is turned into a
//! loud failure below rather than a green line. The exit code is read in ONE
//! place only: with no `outcomes.json` at all, exit 0 is the filter shape
//! above and anything else is the tool dying before it ran — which this verb
//! used to blame on the filter (observed 2026-09-12: a missing `--output`
//! parent, cargo-mutants exit 1, and "the usual cause is a --file or --re").
//!
//! `.cargo/mutants.toml` (read by cargo-mutants from the `--dir` root) hides
//! mutants PROVEN equivalent — unobservable through any surface the oracle may
//! see — by name, one rationale comment per entry; the list's size is pinned
//! by guide-examples' `mutants_exclusions` gate. Hiding is never the answer to
//! a MISSED: that is a property to write.
//!
//! Exit codes: 0 every viable mutant was caught, 1 the run produced a finding
//! (mutants missed) or failed to resolve (timeouts), 2 usage, infrastructure,
//! or a baseline that was not green.

use std::path::PathBuf;
use std::process::Command;

/// Flags this verb recognizes. Every one takes exactly one value, which is
/// what lets a plain linear scan tell flags from values (`sim.rs`'s rule).
/// `--file` and `--re` may repeat; the rest are last-wins.
const KNOWN_FLAGS: &[&str] = &[
    "--file",
    "--re",
    "--dir",
    "--timeout",
    "--jobs",
    "--test-package",
    "--property",
];

/// The default target set: the oracle-bearing files. A mutation run over the
/// whole workspace is 4193 mutants (measured 2026-09-07) and would take days,
/// so an unscoped default would be an instrument nobody runs — the ruling's
/// stated risk.
const DEFAULT_TARGETS: &[&str] = &[
    "crates/charpente-core/src/log.rs",
    "crates/charpente-sim/src/checks.rs",
];

/// The packages whose tests judge those mutants, and this is **not** a
/// convenience default — it corrects a systematic understatement. Without
/// this, the verb judges every mutant with `charpente-core` and
/// `charpente-sim`'s tests — the default target set's oracles — so a
/// `--file` in any other crate must name its own package here, or its
/// mutants are judged by suites that never exercise them and all report
/// MISSED. Most of `charpente-core`'s real oracle coverage lives
/// elsewhere: the WAL's properties are in `charpente-sim/tests/wal.rs`, not in
/// `charpente-core`. Measured 2026-09-07: the `||` -> `&&` mutant in
/// `parse_segment_name` is reported MISSED under the default package scope and
/// CAUGHT once `charpente-sim`'s tests are in scope — the same mutant, two
/// answers, decided entirely by which suites were allowed to run. A report that
/// did not say so would be claiming coverage it never measured.
///
/// Each package's suite is also hermetic inside `cargo-mutants`' scratch copy.
/// That matters because a non-hermetic suite fails the BASELINE and aborts the
/// run: `charpente-cli`'s `tests/ctl.rs` shells out through
/// `env!("CARGO_BIN_EXE_charpente")` and binds real ports, and its
/// `promote_against_a_dead_port_…` case was seen failing that way once on
/// 2026-09-07 and passing on a later identical run — INTERMITTENT, not a
/// standing failure, and recorded at that strength. A green `cargo test` in the
/// real tree does not guarantee a green baseline in a scratch copy.
const DEFAULT_TEST_PACKAGES: &[&str] = &["charpente-core", "charpente-sim"];

pub const USAGE: &str = "usage: charpente test --mutants [--file PATH]… [--re REGEX] \
[--dir PATH] [--timeout SECS] [--jobs N]\n\
       --file PATH  a source file to mutate (repeatable; default: the oracle-bearing set)\n\
       --re REGEX   only mutants whose description matches\n\
       --dir PATH   the workspace directory to run in (default: .)\n\
       --jobs N     cargo-mutants' parallelism (also CARGO_MUTANTS_JOBS); refused above 1 when \
CARGO_TARGET_DIR is inherited\n\
       --test-package NAME  whose tests judge the mutants (repeatable). Without this, \
the verb judges every mutant with charpente-core and charpente-sim's tests — the default \
target set's oracles — so a --file in any other crate must name its own package here, or \
its mutants are judged by suites that never exercise them and all report MISSED.\n\
       --property NAME  judge the mutants with ONE sim property only — the way to ask \
whether that property alone would notice. Without it every property runs, and one honest \
sibling is enough to report a mutant CAUGHT.\n\
       --timeout    per-mutant test timeout in seconds; a TIMEOUT IS NOT A CATCH. Without it, \
the test packages are timed with one `cargo test` first and the verb passes 5x that whenever \
they are not exactly the --files' one package (the default set is two), because \
cargo-mutants' own baseline times only the mutated file's package";

/// The fields of `mutants.out/outcomes.json` this verb reads. Deserialized
/// rather than string-scanned (unlike `sim.rs`'s scan of cargo's message
/// stream) because these are the numbers the report states, and a silently
/// missing field must be an error rather than a zero.
#[derive(serde::Deserialize)]
struct Outcomes {
    total_mutants: u64,
    caught: u64,
    missed: u64,
    timeout: u64,
    unviable: u64,
    #[serde(default)]
    outcomes: Vec<Outcome>,
}

#[derive(serde::Deserialize)]
struct Outcome {
    scenario: serde_json::Value,
    summary: String,
}

/// `Some(refusal)` when the effective `--jobs` (flag or `CARGO_MUTANTS_JOBS`,
/// cargo-mutants' own env alias) is `> 1` combined with an inherited
/// `CARGO_TARGET_DIR`. cargo-mutants isolates parallel jobs by building each
/// scratch tree into its own `target/`; an inherited `CARGO_TARGET_DIR`
/// points every job at ONE directory, and cargo's artifact filename hash does
/// not vary by mutation content — so a mutant's tests can run against a
/// different mutant's (or the baseline's) build and report a MISSED or a
/// CAUGHT that measured nothing. Observed 2026-09-10: one rlib hash shared by
/// 21 of 36 mutants' logs including the baseline's, and a MISSED that flipped
/// to CAUGHT with only `--jobs` changed. The combination is reachable by
/// following this repo's own advice (a dedicated `CARGO_TARGET_DIR` per
/// worktree), so the verb refuses it by name rather than trusting the operator
/// to know. Pure so the message is unit-testable; `run` supplies the env.
/// `jobs` is `(how it was set, value)` — `("--jobs ", "4")` or
/// `("CARGO_MUTANTS_JOBS=", "4")` — so the message names its source.
fn shared_target_dir_refusal(
    jobs: Option<(&str, &str)>,
    target_dir: Option<&std::ffi::OsStr>,
) -> Option<String> {
    let (how, val) = jobs?;
    let n: u64 = val.parse().ok()?;
    if n <= 1 {
        return None;
    }
    let dir = target_dir?.to_string_lossy().into_owned();
    Some(format!(
        "error: {how}{n} with CARGO_TARGET_DIR set ({dir}) — cargo-mutants builds each \
         job's scratch tree into a target dir of its own, and an inherited CARGO_TARGET_DIR \
         points every job at ONE directory; cargo's artifact hash does not vary by mutation \
         content, so a mutant's tests can run against a different mutant's build and report \
         a MISSED (or a CAUGHT) that measured nothing (observed 2026-09-10: one rlib hash \
         shared by 21 of 36 mutants' logs). Fix: run with --jobs 1, or unset CARGO_TARGET_DIR \
         (and CARGO_MUTANTS_JOBS) for this invocation."
    ))
}

pub fn run(args: &[String]) -> i32 {
    let opts = match parse(args) {
        Ok(opts) => opts,
        Err(code) => return code,
    };

    // cargo-mutants' own precedence (clap): an explicit --jobs flag overrides
    // CARGO_MUTANTS_JOBS.
    let env_jobs = std::env::var("CARGO_MUTANTS_JOBS").ok();
    let jobs = opts
        .jobs
        .as_deref()
        .map(|j| ("--jobs ", j))
        .or(env_jobs.as_deref().map(|j| ("CARGO_MUTANTS_JOBS=", j)));
    if let Some(refusal) =
        shared_target_dir_refusal(jobs, std::env::var_os("CARGO_TARGET_DIR").as_deref())
    {
        eprintln!("{refusal}");
        return 2;
    }

    // The tool is checked for before anything that takes time: the timing run
    // below can take minutes, and a machine without cargo-mutants should
    // learn that first. Process spawning is edge tooling (DESIGN.md §1).
    #[allow(clippy::disallowed_methods)]
    let probe = Command::new("cargo")
        .args(["mutants", "--version"])
        .current_dir(&opts.dir)
        .output();
    if !probe.as_ref().is_ok_and(|out| out.status.success()) {
        let how = match &probe {
            Ok(out) => format!(
                "exited {}",
                out.status
                    .code()
                    .map_or("on a signal".to_string(), |c| c.to_string())
            ),
            Err(e) => format!("could not run: {e}"),
        };
        eprintln!(
            "error: cargo-mutants is not installed (`cargo mutants --version` {how}) — install \
             the pinned version with\n  cargo install cargo-mutants --version 27.1.0 --locked"
        );
        return 2;
    }

    let out_dir = PathBuf::from(&opts.dir).join("target/charpente-mutants");
    let mut argv: Vec<String> = vec!["mutants".into(), "--output".into(), display(&out_dir)];
    for file in &opts.files {
        argv.push("--file".into());
        argv.push(file.clone());
    }
    for re in &opts.res {
        argv.push("--re".into());
        argv.push(re.clone());
    }
    for pkg in &opts.test_packages {
        argv.push("--test-package".into());
        argv.push(pkg.clone());
    }
    if let Some(prop) = &opts.property {
        // The app-side convention this CLI already assumes elsewhere
        // (`sim.rs`'s `find_sim_executable`): the sim target is `tests/sim.rs`
        // with `harness = false`, so its own `--filter` reaches it only after a
        // bare `--`. Naming `--test sim` is not optional: without it the args
        // also reach the crate's libtest targets, which have no `--filter` flag
        // and exit 101 — failing the BASELINE rather than filtering anything.
        for a in ["--cargo-test-arg", "--test", "--cargo-test-arg", "sim"] {
            argv.push(a.into());
        }
        for a in ["--cargo-test-arg", "--", "--cargo-test-arg", "--filter"] {
            argv.push(a.into());
        }
        argv.push("--cargo-test-arg".into());
        argv.push(prop.clone());
    }
    let timeout = match &opts.timeout {
        Some(t) => Some(t.clone()),
        // cargo-mutants 27.1.0 times its baseline over ONE package, the
        // mutated file's, while every mutant runs all the test packages: its
        // auto-timeout is then too short whenever those differ, and a
        // slow-but-caught mutant reads TIMEOUT. So the verb measures the set
        // itself. Wall time, not CPU time: `--timeout` is a
        // wall-clock bound on this same command, so wall time is its unit.
        // CPU time would undershoot a suite that sleeps or waits (the serve
        // and edge tests do) and overshoot a parallel one. And the error a
        // wall-time measurement can carry (a machine that slept or was busy
        // mid-run) only lengthens it, which is the safe side for a timeout.
        None if baseline_differs(&opts.dir, &opts.files, &opts.test_packages) => {
            match time_the_package_set(&opts.dir, &opts.test_packages) {
                Ok(secs) => {
                    let derived = derived_timeout(secs);
                    // Under --property each mutant runs one sim property, a
                    // subset of what was timed, so the time only bounds it.
                    let what = match &opts.property {
                        None => "that is what each mutant runs".to_string(),
                        Some(p) => format!(
                            "each mutant runs only the sim property `{p}`, so that is an upper \
                             bound on a mutant's run"
                        ),
                    };
                    println!(
                        "mutants: `cargo test` over {} took {secs:.1} s of wall time; {what}, \
                         and cargo-mutants' own baseline times only the mutated file's package, \
                         so the verb is passing --timeout {derived} (5x)",
                        opts.test_packages.join(", ")
                    );
                    Some(derived.to_string())
                }
                Err(msg) => {
                    eprintln!("{msg}");
                    return 2;
                }
            }
        }
        None => None,
    };
    if let Some(t) = &timeout {
        argv.push("--timeout".into());
        argv.push(t.clone());
    }
    if let Some(j) = &opts.jobs {
        argv.push("--jobs".into());
        argv.push(j.clone());
    }

    // cargo-mutants needs `--output`'s parent to exist and dies with its own
    // error when it does not; under an exported CARGO_TARGET_DIR `./target`
    // is never created, so the verb makes the path it chose exist itself
    // (edge tooling, outside the deterministic core — DESIGN.md §1).
    #[allow(clippy::disallowed_methods)]
    if let Err(e) = std::fs::create_dir_all(&out_dir) {
        eprintln!(
            "error: could not create the output directory {}: {e}",
            display(&out_dir)
        );
        return 2;
    }

    // Process spawning is edge tooling here, outside the deterministic core
    // (DESIGN.md §1) — this shells out to cargo-mutants, which patches a
    // scratch copy of the tree; nothing inside the core spawns processes.
    #[allow(clippy::disallowed_methods)]
    let status = Command::new("cargo")
        .args(&argv)
        .current_dir(&opts.dir)
        .status();

    let status = match status {
        Ok(status) => status,
        Err(e) => {
            eprintln!("error: failed to run cargo-mutants: {e}");
            return 2;
        }
    };

    report(
        &out_dir.join("mutants.out/outcomes.json"),
        opts.property.as_deref(),
        status.code(),
    )
}

/// Whether cargo-mutants' own baseline times a different `cargo test` than the
/// one each mutant runs. The baseline runs the mutated file's package and each
/// mutant runs every `--test-package`, so they are the same command only when
/// there is one test package and every `--file` is in it. A file whose package
/// cannot be read counts as different: the timing run costs one `cargo test`,
/// and a timeout set too short reports caught mutants as TIMEOUT.
fn baseline_differs(dir: &str, files: &[String], test_packages: &[String]) -> bool {
    match test_packages {
        [only] => !files
            .iter()
            .all(|file| package_of(dir, file).as_deref() == Some(only.as_str())),
        _ => true,
    }
}

/// The package `file` belongs to: the `name` in the `[package]` table of the
/// nearest `Cargo.toml` above it inside `dir`, or `None` when there is none.
// Reading manifests is edge tooling, outside the deterministic core
// (DESIGN.md §1).
#[allow(clippy::disallowed_methods)]
fn package_of(dir: &str, file: &str) -> Option<String> {
    let text = std::path::Path::new(file)
        .ancestors()
        .skip(1)
        .find_map(|up| {
            std::fs::read_to_string(PathBuf::from(dir).join(up).join("Cargo.toml")).ok()
        })?;
    let mut in_package = false;
    for line in text.lines().map(str::trim) {
        if line.starts_with('[') {
            in_package = line == "[package]";
        } else if in_package
            && let Some(value) = line
                .strip_prefix("name")
                .and_then(|rest| rest.trim_start().strip_prefix('='))
        {
            return Some(value.trim().trim_matches('"').to_string());
        }
    }
    None
}

/// Five times the measured wall time, rounded up to whole seconds: the
/// multiplier cargo-mutants 27.1.0 applies to its own baseline (13.5 s gave
/// "Auto-set test timeout to 68s").
fn derived_timeout(secs: f64) -> u64 {
    (secs * 5.0).ceil().max(1.0) as u64
}

/// Builds the package set's tests, then times one `cargo test` over all of
/// them — the command each mutant runs — in wall-clock seconds. The build is
/// outside the measurement, as it is in cargo-mutants' own baseline.
// Spawning cargo and reading the wall clock are edge tooling, outside the
// deterministic core (DESIGN.md §1).
#[allow(clippy::disallowed_methods, clippy::disallowed_types)]
fn time_the_package_set(dir: &str, packages: &[String]) -> Result<f64, String> {
    let mut args: Vec<&str> = vec!["test"];
    for pkg in packages {
        args.push("--package");
        args.push(pkg);
    }
    let build = [&args[..1], &["--no-run"], &args[1..]].concat();
    let run = |args: &[&str]| -> Result<(), String> {
        let status = Command::new("cargo")
            .args(args)
            .current_dir(dir)
            .status()
            .map_err(|e| {
                format!(
                    "error: could not run `cargo {}` to time the test set: {e}",
                    args.join(" ")
                )
            })?;
        if status.success() {
            return Ok(());
        }
        Err(format!(
            "error: `cargo {}` exited {} while timing the test set for --timeout — the \
             unmutated suite is not green, so no mutant result would mean anything \
             (cargo-mutants' own baseline refuses it too). Fix the suite first.",
            args.join(" "),
            status
                .code()
                .map_or("on a signal".to_string(), |c| c.to_string())
        ))
    };
    run(&build)?;
    let start = std::time::Instant::now();
    run(&args)?;
    Ok(start.elapsed().as_secs_f64())
}

/// What to say when the run left no `outcomes.json` at all. The tool's exit
/// code is ignored for the VERDICT (0 is a clean sweep and also a filter that
/// matched nothing), but here it is the one signal that separates "matched
/// nothing" from "died before running": observed 2026-09-12, a missing
/// `--output` parent made cargo-mutants exit 1 with its own error and this
/// verb blamed the filter. `code` is `None` when a signal killed the tool.
fn no_outcomes_diagnosis(code: Option<i32>, path: &str, err: &str) -> String {
    match code {
        Some(0) => format!(
            "error: cargo-mutants wrote no outcomes at {path} ({err}) — NOTHING WAS MEASURED. \
             The usual cause is a --file or --re that matched no mutants, which the tool \
             itself reports as success. Check the filter names a real path or function."
        ),
        Some(n) => format!(
            "error: cargo-mutants exited {n} before measuring anything (no outcomes at {path}: \
             {err}) — NOTHING WAS MEASURED; its own error is printed above this line and is \
             the diagnosis (a missing directory, a malformed --re, a usage error — not a \
             mutant's verdict)."
        ),
        None => format!(
            "error: cargo-mutants was killed by a signal before measuring anything (no outcomes \
             at {path}: {err}) — NOTHING WAS MEASURED."
        ),
    }
}

/// Reads the run's own record and turns every "nothing was measured" shape
/// into a loud failure. The tool's exit code is NOT consulted for the
/// verdict — it is 0 both for a clean sweep and for a filter that selected
/// nothing — and IS consulted in exactly one place, when there is no record
/// at all (`no_outcomes_diagnosis`).
fn report(path: &std::path::Path, property: Option<&str>, code: Option<i32>) -> i32 {
    // Reading cargo-mutants' own run record is edge tooling, outside the
    // deterministic core (DESIGN.md §1) — nothing inside charpente-core
    // touches the filesystem directly.
    #[allow(clippy::disallowed_methods)]
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) => {
            eprintln!(
                "{}",
                no_outcomes_diagnosis(code, &path.display().to_string(), &e.to_string())
            );
            return 2;
        }
    };
    let o: Outcomes = match serde_json::from_str(&text) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("error: could not parse {}: {e}", path.display());
            return 2;
        }
    };
    let v = verdict(&o, &path.display().to_string(), property);
    for line in &v.lines {
        if v.code == 2 {
            eprintln!("{line}");
        } else {
            println!("{line}");
        }
    }
    v.code
}

struct Verdict {
    code: i32,
    lines: Vec<String>,
}

/// The decision, split out of `report`'s IO so the ruling's two anti-vacuity
/// rules are TESTED rather than merely intended — the spec requires exactly
/// that, and an untested honesty gate is the thing it is guarding against.
fn verdict(o: &Outcomes, where_: &str, property: Option<&str>) -> Verdict {
    if let Some(bad) = o
        .outcomes
        .iter()
        .find(|x| x.scenario == "Baseline" && x.summary != "Success")
    {
        return Verdict {
            code: 2,
            lines: vec![format!(
                "error: the UNMUTATED baseline was not green ({}) — no mutant result from this \
                 run means anything, and every counter in {where_} reads zero regardless. Fix \
                 the suite, or scope --file to a package whose tests pass inside cargo-mutants' \
                 scratch copy (tests that shell out via env!(\"CARGO_BIN_EXE_…\") may not).",
                bad.summary
            )],
        };
    }

    if o.total_mutants == 0 {
        return Verdict {
            code: 2,
            lines: vec![
                "error: 0 mutants were generated — a zero-mutant run is NOT a clean run. \
                 \"0 missed\" here means the filter selected nothing, not that the oracles held."
                    .to_string(),
            ],
        };
    }

    let mut lines = vec![format!(
        "mutants: {} total — {} caught, {} missed, {} unviable, {} timed out",
        o.total_mutants, o.caught, o.missed, o.unviable, o.timeout
    )];
    let mut code = 0;
    if o.missed > 0 {
        lines.push(match property {
            // Hard rule 5: name the contract, the site, and the fix.
            Some(prop) => format!(
                "  {} MISSED, judged by \"{prop}\" ALONE: that property runs, reaches a \
                 verdict, and does not notice this change — it is ONE-SIDED with respect to \
                 the mutated code, and would pass just the same if the code were deleted. \
                 Fix: assert the EFFECT the mutated code produces (a value that differs when \
                 it does not run), not merely that the surrounding state is still readable. \
                 The surviving diff is in the run directory.",
                o.missed
            ),
            None => format!(
                "  {} MISSED: no test IN THE PACKAGES UNDER TEST caught this mutant. That is \
                 either a one-sided property, or an oracle that lives in a package outside \
                 --test-package. Without --test-package the verb judges every mutant with \
                 charpente-core and charpente-sim's tests — the default target set's oracles \
                 — so a --file in any other crate must name its own package here, or its \
                 mutants are judged by suites that never exercise them and all report \
                 MISSED; the scope is part of the claim. Re-run with \
                 --property NAME to ask which single property is blind to it. See the run \
                 directory for the surviving diffs.",
                o.missed
            ),
        });
        code = 1;
    }
    if o.timeout > 0 {
        // The ruling's second anti-vacuity rule: an unresolved mutant is not a
        // caught one, and summing it into `caught` would be the same lie the
        // simulator's give-up accounting exists to prevent.
        lines.push(format!(
            "  {} TIMED OUT: unresolved, NOT caught — raise --timeout and rerun before reading \
             this batch as clean.",
            o.timeout
        ));
        code = 1;
    }
    Verdict { code, lines }
}

struct Opts {
    dir: String,
    files: Vec<String>,
    res: Vec<String>,
    test_packages: Vec<String>,
    property: Option<String>,
    timeout: Option<String>,
    jobs: Option<String>,
}

fn parse(args: &[String]) -> Result<Opts, i32> {
    let mut dir = ".".to_string();
    let mut files = Vec::new();
    let mut res = Vec::new();
    let mut test_packages = Vec::new();
    let mut property = None;
    let mut timeout = None;
    let mut jobs = None;

    let mut iter = args.iter();
    while let Some(flag) = iter.next() {
        if !KNOWN_FLAGS.contains(&flag.as_str()) {
            eprintln!(
                "error: unknown flag \"{flag}\" — valid flags: {}\n\n{USAGE}",
                KNOWN_FLAGS.join(", ")
            );
            return Err(2);
        }
        let Some(value) = iter.next() else {
            eprintln!("error: {flag} requires a value\n\n{USAGE}");
            return Err(2);
        };
        match flag.as_str() {
            "--dir" => dir = value.clone(),
            "--file" => files.push(value.clone()),
            "--re" => res.push(value.clone()),
            "--test-package" => test_packages.push(value.clone()),
            "--property" => property = Some(value.clone()),
            "--timeout" => timeout = Some(value.clone()),
            "--jobs" => jobs = Some(value.clone()),
            _ => unreachable!("KNOWN_FLAGS and this match are the same list"),
        }
    }

    if files.is_empty() {
        files = DEFAULT_TARGETS.iter().map(|s| (*s).to_string()).collect();
    }
    if test_packages.is_empty() {
        test_packages = DEFAULT_TEST_PACKAGES
            .iter()
            .map(|s| (*s).to_string())
            .collect();
    }
    Ok(Opts {
        dir,
        files,
        res,
        test_packages,
        property,
        timeout,
        jobs,
    })
}

fn display(p: &std::path::Path) -> String {
    p.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcomes(json: &str) -> Outcomes {
        serde_json::from_str(json).expect("fixture parses")
    }

    const OK_BASELINE: &str = r#""outcomes":[{"scenario":"Baseline","summary":"Success"}]"#;

    #[test]
    fn every_viable_mutant_caught_is_the_only_green() {
        let v = verdict(
            &outcomes(&format!(
                r#"{{"total_mutants":5,"caught":5,"missed":0,"timeout":0,"unviable":0,{OK_BASELINE}}}"#
            )),
            "out",
            None,
        );
        assert_eq!(v.code, 0, "{:?}", v.lines);
    }

    #[test]
    fn a_timeout_is_not_a_catch() {
        let v = verdict(
            &outcomes(&format!(
                r#"{{"total_mutants":5,"caught":4,"missed":0,"timeout":1,"unviable":0,{OK_BASELINE}}}"#
            )),
            "out",
            None,
        );
        assert_eq!(
            v.code, 1,
            "an UNRESOLVED mutant must not let the batch read as clean"
        );
        assert!(
            v.lines.iter().any(|l| l.contains("TIMED OUT")),
            "the timeout must be named in its own right: {:?}",
            v.lines
        );
        assert!(
            v.lines[0].contains("4 caught") && v.lines[0].contains("1 timed out"),
            "a timeout must be reported BESIDE caught, never summed into it: {:?}",
            v.lines
        );
    }

    /// The fixture is the real artifact, not an invention: a run whose baseline
    /// failed leaves every counter at zero, so "0 missed" is what a reader of
    /// the JSON alone would see. Observed on this workspace 2026-09-07.
    #[test]
    fn a_failed_baseline_is_never_a_result() {
        let v = verdict(
            &outcomes(
                r#"{"total_mutants":0,"caught":0,"missed":0,"timeout":0,"unviable":0,
                   "outcomes":[{"scenario":"Baseline","summary":"Failure"}]}"#,
            ),
            "out",
            None,
        );
        assert_eq!(v.code, 2, "{:?}", v.lines);
        assert!(
            v.lines[0].contains("baseline"),
            "the diagnosis must be the baseline, not the zero count it also produces: {:?}",
            v.lines
        );
    }

    #[test]
    fn zero_mutants_is_not_a_clean_run() {
        let v = verdict(
            &outcomes(&format!(
                r#"{{"total_mutants":0,"caught":0,"missed":0,"timeout":0,"unviable":0,{OK_BASELINE}}}"#
            )),
            "out",
            None,
        );
        assert_eq!(v.code, 2, "{:?}", v.lines);
    }

    /// Hard rule 5: the report names the property, says what is wrong with it,
    /// and says what to do. A bare "1 missed" would name neither.
    #[test]
    fn a_missed_mutant_under_one_property_names_that_property_and_the_fix() {
        let v = verdict(
            &outcomes(&format!(
                r#"{{"total_mutants":1,"caught":0,"missed":1,"timeout":0,"unviable":0,{OK_BASELINE}}}"#
            )),
            "out",
            Some("counter_bump_job_leaves_the_row_readable"),
        );
        assert_eq!(v.code, 1);
        let text = v.lines.join(" ");
        assert!(
            text.contains("counter_bump_job_leaves_the_row_readable"),
            "must name the property judged: {text}"
        );
        assert!(text.contains("ONE-SIDED"), "must say what is wrong: {text}");
        assert!(text.contains("Fix:"), "must say what to do: {text}");
    }

    #[test]
    fn a_missed_mutant_is_a_finding() {
        let v = verdict(
            &outcomes(&format!(
                r#"{{"total_mutants":5,"caught":4,"missed":1,"timeout":0,"unviable":0,{OK_BASELINE}}}"#
            )),
            "out",
            None,
        );
        assert_eq!(v.code, 1);
        assert!(
            v.lines.iter().any(|l| l.contains("MISSED")),
            "{:?}",
            v.lines
        );
    }

    #[test]
    fn parallel_jobs_under_a_shared_target_dir_are_refused_by_name() {
        let dir = std::ffi::OsStr::new("/tmp/shared");
        let msg = shared_target_dir_refusal(Some(("--jobs ", "4")), Some(dir)).expect("refused");
        assert!(msg.starts_with("error: --jobs 4 with CARGO_TARGET_DIR set (/tmp/shared)"));
        assert!(msg.contains("observed 2026-09-10"));
        assert!(msg.ends_with(
            "run with --jobs 1, or unset CARGO_TARGET_DIR (and CARGO_MUTANTS_JOBS) for this invocation."
        ));

        let env_msg = shared_target_dir_refusal(Some(("CARGO_MUTANTS_JOBS=", "4")), Some(dir))
            .expect("refused");
        assert!(
            env_msg
                .starts_with("error: CARGO_MUTANTS_JOBS=4 with CARGO_TARGET_DIR set (/tmp/shared)")
        );
    }

    #[test]
    fn the_refusal_needs_both_halves() {
        let dir = std::ffi::OsStr::new("/tmp/shared");
        assert!(
            shared_target_dir_refusal(Some(("--jobs ", "1")), Some(dir)).is_none(),
            "one job cannot collide"
        );
        assert!(
            shared_target_dir_refusal(None, Some(dir)).is_none(),
            "no --jobs and no CARGO_MUTANTS_JOBS is the tool's default of 1"
        );
        assert!(
            shared_target_dir_refusal(Some(("--jobs ", "4")), None).is_none(),
            "no shared dir, no collision"
        );
        assert!(
            shared_target_dir_refusal(Some(("--jobs ", "many")), Some(dir)).is_none(),
            "an unparseable --jobs is cargo-mutants' to refuse, not ours"
        );
        assert!(
            shared_target_dir_refusal(Some(("CARGO_MUTANTS_JOBS=", "1")), Some(dir)).is_none(),
            "one env-sourced job cannot collide either"
        );
    }

    /// The multiplier and rounding are cargo-mutants' own, read off its logs:
    /// a 13.5 s baseline became 68 s and a 32.4 s one 162 s.
    #[test]
    fn the_derived_timeout_is_five_times_the_measurement_rounded_up() {
        assert_eq!(derived_timeout(13.5), 68);
        assert_eq!(derived_timeout(32.4), 162);
        assert_eq!(derived_timeout(40.0), 200);
        assert_eq!(derived_timeout(0.001), 1, "never a zero timeout");
    }

    #[test]
    fn a_tool_that_died_is_named_by_its_exit_not_blamed_on_the_filter() {
        let msg = no_outcomes_diagnosis(Some(1), "x/outcomes.json", "No such file");
        assert!(msg.starts_with("error: cargo-mutants exited 1 before measuring anything"));
        assert!(msg.contains("NOTHING WAS MEASURED"));
        assert!(msg.contains("its own error is printed above"));
        assert!(!msg.contains("matched no mutants"));
        // A malformed --re also exits 1, so the verb must not rule the
        // filter out; the tool's own line above is the diagnosis.
        assert!(!msg.contains("not a filter"));
        assert!(no_outcomes_diagnosis(Some(4), "x", "e").contains("exited 4 before"));
    }

    #[test]
    fn a_tool_that_exited_zero_with_no_outcomes_is_the_filter_shape() {
        let msg = no_outcomes_diagnosis(Some(0), "x/outcomes.json", "No such file");
        assert!(msg.starts_with("error: cargo-mutants wrote no outcomes at x/outcomes.json"));
        assert!(msg.contains("matched no mutants"));
        assert!(!msg.contains("exited 0"));
    }

    #[test]
    fn a_tool_killed_by_a_signal_says_so() {
        let msg = no_outcomes_diagnosis(None, "x", "e");
        assert!(msg.starts_with("error: cargo-mutants was killed by a signal before measuring"));
        assert!(!msg.contains("matched no mutants"));
    }
}
