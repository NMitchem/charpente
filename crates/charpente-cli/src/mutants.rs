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
//! loud failure below rather than a green line.
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
/// convenience default — it corrects a systematic understatement.
/// `cargo-mutants` runs only the MUTATED package's own tests unless told
/// otherwise, and most of `charpente-core`'s real oracle coverage lives
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
       --test-package NAME  whose tests judge the mutants (repeatable). Without this, \
cargo-mutants runs only the MUTATED package's tests, which understates coverage.\n\
       --property NAME  judge the mutants with ONE sim property only — the way to ask \
whether that property alone would notice. Without it every property runs, and one honest \
sibling is enough to report a mutant CAUGHT.\n\
       --timeout    per-mutant test timeout in seconds; a TIMEOUT IS NOT A CATCH";

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

pub fn run(args: &[String]) -> i32 {
    let opts = match parse(args) {
        Ok(opts) => opts,
        Err(code) => return code,
    };

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
    if let Some(t) = &opts.timeout {
        argv.push("--timeout".into());
        argv.push(t.clone());
    }
    if let Some(j) = &opts.jobs {
        argv.push("--jobs".into());
        argv.push(j.clone());
    }

    // Process spawning is edge tooling here, outside the deterministic core
    // (DESIGN.md §1) — this shells out to cargo-mutants, which patches a
    // scratch copy of the tree; nothing inside the core spawns processes.
    #[allow(clippy::disallowed_methods)]
    let status = Command::new("cargo")
        .args(&argv)
        .current_dir(&opts.dir)
        .status();

    if let Err(e) = &status
        && e.kind() == std::io::ErrorKind::NotFound
    {
        eprintln!(
            "error: cargo-mutants is not installed — install the pinned version with\n  \
             cargo install cargo-mutants --version 27.1.0 --locked"
        );
        return 2;
    }
    if let Err(e) = status {
        eprintln!("error: failed to run cargo-mutants: {e}");
        return 2;
    }

    report(
        &out_dir.join("mutants.out/outcomes.json"),
        opts.property.as_deref(),
    )
}

/// Reads the run's own record and turns every "nothing was measured" shape
/// into a loud failure. The tool's exit code is deliberately NOT consulted:
/// it is 0 both for a clean sweep and for a filter that selected nothing.
fn report(path: &std::path::Path, property: Option<&str>) -> i32 {
    // Reading cargo-mutants' own run record is edge tooling, outside the
    // deterministic core (DESIGN.md §1) — nothing inside charpente-core
    // touches the filesystem directly.
    #[allow(clippy::disallowed_methods)]
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) => {
            eprintln!(
                "error: cargo-mutants wrote no outcomes at {} ({e}) — NOTHING WAS MEASURED. \
                 The usual cause is a --file or --re that matched no mutants, which the tool \
                 itself reports as success. Check the filter names a real path or function.",
                path.display()
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
                 --test-package — cargo-mutants runs only the mutated package's own tests \
                 unless told otherwise, so the scope is part of the claim. Re-run with \
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
}
