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
//! loud failure below rather than a green line. The same shape at function
//! grain, a function whose every mutant was unviable, is named as untested
//! instead, beside the totals: a return type the tool cannot build would
//! otherwise fail every run, so it is treated as the sim treats an untested
//! check. The exit code is read in ONE
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
//!
//! Over an app, a test package with a `sim_tests!` harness, the run is two
//! passes on one master seed (0.4.0 spec §4 E). Pass 1 judges every mutant on
//! the first `--first-runs` runs of each check, where only a failed run counts;
//! pass 2 judges what pass 1 missed or timed out at the full count. A quick
//! pass's runs are a prefix of the full count's for every check, so the
//! verdicts are the ones a single full pass gives. Before either pass the verb
//! runs the unmutated tests once at the full count, so the coverage rules and
//! the untested rule judge the baseline even when pass 2 has nothing to judge.

use std::collections::BTreeMap;
use std::io::Write as _;
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
    "--first-runs",
    "--master-seed",
];

/// Pass 1's runs per check when `--first-runs` is not given.
const DEFAULT_FIRST_RUNS: u64 = 32;

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

/// Set on cargo-mutants and on the timing run, so every test binary they run
/// inherits it: an app's sim harness then reports a failing check's first
/// failure unshrunk. A mutant needs only red or green, and a shrink of up to
/// 500 serial replays would spend the timeout a caught mutant is judged by.
/// An environment variable, not a harness flag: in a mixed test set a flag
/// would also reach libtest binaries, which refuse one they do not know.
/// charpente-sim's `harness::NO_SHRINK_ENV`, spelled out here because this
/// crate does not link charpente-sim; guide-examples' `hook_census` holds the
/// two spellings equal.
const NO_SHRINK_ENV: &str = "CHARPENTE_SIM_NO_SHRINK";

/// Set to the run's master seed on every process the verb starts, so every
/// sim batch in both passes draws the same runs, and the rerun line repeats
/// them. charpente-sim's `harness::MASTER_SEED_ENV`, spelled out for
/// `NO_SHRINK_ENV`'s reason and held equal by the same census.
const MASTER_SEED_ENV: &str = "CHARPENTE_SIM_MASTER_SEED";

/// Set on pass 1 only, and removed everywhere else, so a value exported in the
/// verb's own environment cannot make a full-count run a quick pass.
/// charpente-sim's `harness::FIRST_RUNS_ENV`, held equal the same way.
const FIRST_RUNS_ENV: &str = "CHARPENTE_SIM_FIRST_RUNS";

pub const USAGE: &str = "usage: charpente test --mutants [--file PATH]… [--re REGEX] \
[--dir PATH] [--timeout SECS] [--jobs N] [--first-runs N] [--master-seed 0x…]\n\
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
cargo-mutants' own baseline times only the mutated file's package\n\
       --first-runs N  pass 1's runs per sim check (default 32). When a --test-package has \
a sim_tests! harness (tests/sim.rs, harness = false), pass 1 judges every mutant on the first N \
runs of each check, failed runs only, and pass 2 judges what it missed or timed out at the full \
count; the verdicts are a single full pass's. Packages without that harness run in full in both.\n\
       --master-seed 0x…  the master seed every sim batch in the run derives its runs from \
(default: drawn fresh and printed); the report's last line reruns the run with it\n\
       Every test the verb runs, the timing run's and each mutant's, sees \
CHARPENTE_SIM_NO_SHRINK=1, so a failing sim check reports its first failure unshrunk: a mutant \
needs only red or green, and a shrink would spend the timeout. Each also sees \
CHARPENTE_SIM_MASTER_SEED, and pass 1's see CHARPENTE_SIM_FIRST_RUNS.";

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
    scenario: Scenario,
    summary: String,
    /// Relative to `mutants.out`; the baseline's is its build/test log.
    log_path: Option<String>,
    /// The pass this verdict came from; set by the verb, not read.
    #[serde(skip)]
    pass: Pass,
}

/// The pass that decided a mutant: a run's one full pass, or pass 1 or 2 of two.
#[derive(Clone, Copy, Default)]
enum Pass {
    #[default]
    Only,
    First,
    Second,
}

/// cargo-mutants' `scenario`: the unmutated baseline, or one mutant.
#[derive(serde::Deserialize)]
enum Scenario {
    Baseline,
    Mutant(Mutant),
}

/// A mutant's file, its name (file, line, column and change, the key pass 2's
/// `--iterate` matches on), and the function it sits in, absent for a mutant
/// outside any function.
#[derive(serde::Deserialize)]
struct Mutant {
    file: String,
    name: String,
    function: Option<Function>,
}

#[derive(serde::Deserialize)]
struct Function {
    function_name: String,
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

/// The tables cargo-mutants 27.1.0 does not rewrite a relative `path` in. It rewrites
/// `[dependencies]`, `[replace]` and `[patch]` only, so in these the copy under
/// the temp dir resolves `../outside` against itself and the unmutated baseline
/// fails with a bare "no matching package". Measured 2026-10-30: the same
/// manifest, with `outside` moved inside the workspace root, goes green, so the
/// copied tree is `workspace_root`; and a NON-test workspace member's entry
/// fails the baseline too, since cargo loads every member to resolve the workspace.
/// Cargo still reads the underscore spellings `[dev_dependencies]` and
/// `[build_dependencies]` in an edition-2021 manifest, and a real 27.1.0 baseline
/// fails on them the same way, so both spellings are scanned and the refusal
/// prints the table as the manifest spells it.
fn unrewritten_path_refusal(
    manifest: &std::path::Path,
    copied_root: &std::path::Path,
) -> Option<String> {
    // Reading manifests is edge tooling (DESIGN.md §1).
    #[allow(clippy::disallowed_methods)]
    let text = std::fs::read_to_string(manifest).ok()?;
    let doc: toml::Table = text.parse().ok()?;
    let mut tables: Vec<(String, &toml::Table)> = Vec::new();
    for kind in [
        "dev-dependencies",
        "dev_dependencies",
        "build-dependencies",
        "build_dependencies",
    ] {
        if let Some(t) = doc.get(kind).and_then(toml::Value::as_table) {
            tables.push((kind.to_string(), t));
        }
        if let Some(targets) = doc.get("target").and_then(toml::Value::as_table) {
            for (spec, body) in targets {
                if let Some(t) = body.get(kind).and_then(toml::Value::as_table) {
                    let bare = spec
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || "_-".contains(c));
                    let spec = if bare {
                        spec.clone()
                    } else {
                        format!("'{spec}'")
                    };
                    tables.push((format!("target.{spec}.{kind}"), t));
                }
            }
        }
    }
    let base = manifest.parent()?;
    for (table, deps) in tables {
        for (name, dep) in deps {
            let Some(path) = dep.get("path").and_then(toml::Value::as_str) else {
                continue;
            };
            let path = std::path::Path::new(path);
            if path.is_absolute() {
                continue;
            }
            // Lexical: the target need not exist for the copy to miss it.
            let mut at = base.to_path_buf();
            for part in path.components() {
                match part {
                    std::path::Component::ParentDir => {
                        at.pop();
                    }
                    std::path::Component::CurDir => {}
                    part => at.push(part),
                }
            }
            if !at.starts_with(copied_root) {
                return Some(format!(
                    "error: {}: [{table}] {name} = {{ path = \"{}\" }} is a relative path outside \
                     the tree cargo-mutants copies ({}), and cargo-mutants 27.1.0 rewrites \
                     relative paths only in [dependencies], [replace] and [patch], so the copy \
                     cannot find it and the unmutated baseline fails. Give it an absolute path, \
                     or move it under [patch.crates-io].",
                    manifest.display(),
                    path.display(),
                    copied_root.display()
                ));
            }
        }
    }
    None
}

/// `cargo metadata --no-deps` for the tree `dir` belongs to, or `None` when
/// cargo cannot say: the next cargo command, the verb's own timing run or
/// cargo-mutants itself, then fails with cargo's own error.
fn workspace_metadata(dir: &str) -> Option<serde_json::Value> {
    #[allow(clippy::disallowed_methods)]
    let out = Command::new("cargo")
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(dir)
        .output()
        .ok()
        .filter(|o| o.status.success())?;
    serde_json::from_slice(&out.stdout).ok()
}

/// The first refusal over every workspace member in `meta`.
fn unrewritten_path_refusal_in(meta: &serde_json::Value) -> Option<String> {
    let root = PathBuf::from(meta.get("workspace_root")?.as_str()?);
    meta.get("packages")?
        .as_array()?
        .iter()
        .filter_map(|p| p.get("manifest_path")?.as_str())
        .find_map(|m| unrewritten_path_refusal(std::path::Path::new(m), &root))
}

/// The verdict line for a baseline that failed because cargo-mutants' scratch
/// copy ran out of disk, or `None` for any other failure. `log` is the baseline's log.
fn out_of_disk_line(log: &str, tmp: &std::path::Path, tmpdir: Option<&str>) -> Option<String> {
    log.contains("No space left on device").then(|| {
        format!(
            "error: the unmutated baseline ran out of disk space in cargo-mutants' scratch copy \
             under {} — point TMPDIR at a disk with room for a full build of this workspace \
             (TMPDIR is {})",
            tmp.display(),
            tmpdir.unwrap_or("unset")
        )
    })
}

/// The first check a baseline's log reports as untested: the sim's check line,
/// `check <name> ... untested  (0 tested, N skipped)`, which cargo-mutants'
/// log carries as the sim binary printed it.
fn untested_check(log: &str) -> Option<&str> {
    log.lines().find_map(|line| {
        let (name, rest) = line.strip_prefix("check ")?.split_once(' ')?;
        rest.trim_start_matches('.')
            .starts_with(" untested  (")
            .then_some(name)
    })
}

/// The error for an unmutated baseline that a sim check testing nothing made
/// red, or `None` when `log` names no such check. One text for every baseline
/// the verb judges: cargo-mutants' own, read from its log, and the verb's own
/// `cargo test` runs, read from their output. A sim batch with an untested
/// check exits 1, so the baseline is red for that reason alone, and a generic
/// text would send the reader hunting for a failing test that does not exist.
fn untested_baseline(log: &str) -> Option<String> {
    untested_check(log).map(|check| {
        format!(
            "error: the UNMUTATED baseline was not green: check {check} tested nothing, and a \
             check that tested nothing fails the batch. Fix its setup, or scope --property to a \
             check that runs."
        )
    })
}

/// The test packages with a `sim_tests!` harness: a `[[test]]` named `sim` with
/// `harness = false`, the convention `sim.rs`'s `find_sim_executable` assumes.
/// Only that harness reads `FIRST_RUNS_ENV`, so only these take a quick pass,
/// and the rest run in full in both passes. Empty when `cargo metadata` cannot
/// say, which makes the run one full pass: slower, never another verdict.
fn sim_harness_packages(meta: Option<&serde_json::Value>, test_packages: &[String]) -> Vec<String> {
    let Some(packages) = meta.and_then(|m| m.get("packages")?.as_array()) else {
        return Vec::new();
    };
    test_packages
        .iter()
        .filter(|name| {
            packages.iter().any(|p| {
                p.get("name").and_then(serde_json::Value::as_str) == Some(name.as_str())
                    && p.get("manifest_path")
                        .and_then(serde_json::Value::as_str)
                        .and_then(|path| {
                            // Reading manifests is edge tooling (DESIGN.md §1).
                            #[allow(clippy::disallowed_methods)]
                            std::fs::read_to_string(path).ok()
                        })
                        .is_some_and(|text| has_sim_harness(&text))
            })
        })
        .cloned()
        .collect()
}

/// Whether a manifest declares the `sim_tests!` target: `[[test]]` with
/// `name = "sim"` and `harness = false`.
fn has_sim_harness(manifest: &str) -> bool {
    let Ok(doc) = manifest.parse::<toml::Table>() else {
        return false;
    };
    doc.get("test")
        .and_then(toml::Value::as_array)
        .is_some_and(|tests| {
            tests.iter().any(|t| {
                t.get("name").and_then(toml::Value::as_str) == Some("sim")
                    && t.get("harness").and_then(toml::Value::as_bool) == Some(false)
            })
        })
}

/// A fresh master seed for a run given none, as charpente-sim's private
/// `ambient_base_seed` draws one: the verb prints it and its rerun line, so
/// freshness costs no reproducibility. splitmix64's finalizer spreads nearby
/// clock reads apart.
// Reading the wall clock is edge tooling, outside the deterministic core (DESIGN.md §1).
#[allow(clippy::disallowed_methods, clippy::disallowed_types)]
fn fresh_master_seed() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos()) as u64;
    let mut z = nanos.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Sets the environment every process the verb starts runs under: shrinking
/// off, the run's master seed, and pass 1's run count, or no run count at all.
fn sim_env(command: &mut Command, master_seed: u64, first_runs: Option<u64>) -> &mut Command {
    command
        .env(NO_SHRINK_ENV, "1")
        .env(MASTER_SEED_ENV, format!("{master_seed:#x}"));
    match first_runs {
        Some(n) => command.env(FIRST_RUNS_ENV, n.to_string()),
        None => command.env_remove(FIRST_RUNS_ENV),
    }
}

/// The command that reruns this mutation run: the flags given, in order, less
/// any `--master-seed`, then `--master-seed` with the seed this run used. A
/// value a shell would split or expand is single-quoted, so a `--re` holding
/// `.*` pastes whole.
fn rerun_line(args: &[String], master_seed: u64) -> String {
    let mut line = "rerun exactly this mutation run: charpente test --mutants".to_string();
    // `parse` has checked that `args` is flag and value pairs.
    for pair in args.chunks(2).filter(|pair| pair[0] != "--master-seed") {
        for word in pair {
            line.push(' ');
            if !word.is_empty()
                && word
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || "-_./:,@+".contains(c))
            {
                line.push_str(word);
            } else {
                line.push_str(&format!("'{}'", word.replace('\'', r"'\''")));
            }
        }
    }
    line.push_str(&format!(" --master-seed {master_seed:#x}"));
    line
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

    let meta = workspace_metadata(&opts.dir);
    if let Some(refusal) = meta.as_ref().and_then(unrewritten_path_refusal_in) {
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

    // Absolute, because the verb and the tool resolve it from different
    // places: the verb creates and reads it from its own working directory,
    // and cargo-mutants runs from `--dir`, where a relative `<dir>/target/…`
    // would name `<dir>/<dir>/target/…` (outbound-effects final review,
    // carry 16).
    let out_dir =
        match std::path::absolute(PathBuf::from(&opts.dir).join("target/charpente-mutants")) {
            Ok(path) => path,
            Err(e) => {
                eprintln!(
                    "error: could not resolve --dir {} to an absolute path: {e}",
                    opts.dir
                );
                return 2;
            }
        };
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
    // The app-side convention this CLI already assumes elsewhere (`sim.rs`'s
    // `find_sim_executable`): the sim target is `tests/sim.rs` with
    // `harness = false`, so its own `--filter` reaches it only after a bare
    // `--`. Naming `--test sim` is not optional: without it the args also
    // reach the crate's libtest targets, which have no `--filter` flag and
    // exit 101, failing the BASELINE rather than filtering anything.
    let property_args: Vec<&str> = match &opts.property {
        Some(prop) => vec!["--test", "sim", "--", "--filter", prop],
        None => Vec::new(),
    };
    for arg in &property_args {
        argv.push("--cargo-test-arg".into());
        argv.push((*arg).into());
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

    let master_seed = opts.master_seed.unwrap_or_else(fresh_master_seed);
    println!(
        "mutants: master seed {master_seed:#x}, set on every test this run starts as \
         {MASTER_SEED_ENV}"
    );
    let sims = sim_harness_packages(meta.as_ref(), &opts.test_packages);
    // cargo-mutants 27.1.0 times its baseline over ONE package, the mutated
    // file's, while every mutant runs all the test packages: its auto-timeout
    // is then too short whenever those differ, and a slow-but-caught mutant
    // reads TIMEOUT. So the verb times the set itself (`time_the_package_set`).
    let differs =
        opts.timeout.is_none() && baseline_differs(&opts.dir, &opts.files, &opts.test_packages);
    let outcomes = out_dir.join("mutants.out/outcomes.json");
    let (mut stdout, mut stderr) = (std::io::stdout(), std::io::stderr());
    let mut run = Run {
        args,
        master_seed,
        first_runs: None,
        full_count: opts
            .test_packages
            .iter()
            .filter(|pkg| !sims.contains(pkg))
            .cloned()
            .collect(),
        property: opts.property.as_deref(),
        where_: display(&outcomes),
    };

    if sims.is_empty() {
        let timeout = match pass_timeout(&opts, differs, master_seed, None, "") {
            Ok(timeout) => timeout,
            Err(code) => return code,
        };
        let code = match mutants_pass(&opts, &argv, &[], timeout.as_deref(), master_seed, None) {
            Ok(code) => code,
            Err(code) => return code,
        };
        return match read_pass(&outcomes, code, &mut stderr) {
            Ok(only) => report(only, None, &run, &mut stdout, &mut stderr),
            Err(code) => code,
        };
    }

    let first_runs = opts.first_runs;
    run.first_runs = Some(first_runs);
    println!(
        "mutants: {} {} a sim_tests! harness, so this run is two passes: pass 1 judges every \
         mutant on the first {first_runs} runs of each check, failed runs only, and pass 2 \
         judges what pass 1 missed or timed out at the full count",
        sims.join(", "),
        if sims.len() == 1 { "has" } else { "have" }
    );
    // The unmutated tests at the full count, before either pass: the baseline
    // pass 2 judges against, which the coverage rules and the untested rule
    // must judge even when pass 1 leaves pass 2 nothing (spec §4 E). It runs
    // what each pass-2 mutant runs, so it also times pass 2.
    let secs = match time_the_package_set(
        &opts.dir,
        &opts.test_packages,
        &property_args,
        master_seed,
        None,
        "at the full count, the baseline pass 2 judges against",
    ) {
        Ok(secs) => secs,
        Err(msg) => {
            eprintln!("{msg}");
            return 2;
        }
    };
    let over = format!("{} at the full count", opts.test_packages.join(", "));
    let full_timeout = match &opts.timeout {
        Some(t) => Some(t.clone()),
        None if differs => {
            let derived = derived_timeout(secs);
            println!(
                "{}",
                timing_line(
                    "pass 2: ",
                    &over,
                    secs,
                    "that is what each mutant runs",
                    derived
                )
            );
            Some(derived.to_string())
        }
        None => {
            println!("mutants: `cargo test` over {over} is green ({secs:.1} s of wall time)");
            None
        }
    };
    let quick_timeout =
        match pass_timeout(&opts, differs, master_seed, Some(first_runs), "pass 1: ") {
            Ok(timeout) => timeout,
            Err(code) => return code,
        };
    println!(
        "mutants: pass 1 of 2, {FIRST_RUNS_ENV}={first_runs}: a MISSED or TIMEOUT line from it is \
         judged again in pass 2"
    );
    let code = match mutants_pass(
        &opts,
        &argv,
        &[],
        quick_timeout.as_deref(),
        master_seed,
        Some(first_runs),
    ) {
        Ok(code) => code,
        Err(code) => return code,
    };
    let first = match read_pass(&outcomes, code, &mut stderr) {
        Ok(first) => first,
        Err(code) => return code,
    };
    let left = first.missed + first.timeout;
    if left == 0 {
        println!("mutants: pass 1 caught every viable mutant, so pass 2 has none to judge");
        return report(first, None, &run, &mut stdout, &mut stderr);
    }
    // `--iterate` skips the mutants the record in `--output` lists as caught
    // or unviable, matching on each mutant's name, and moves that record to
    // `mutants.out.old`; pass 1's is already read.
    println!(
        "mutants: pass 2 of 2, at the full count: the {left} mutant{} pass 1 did not catch",
        if left == 1 { "" } else { "s" }
    );
    let code = match mutants_pass(
        &opts,
        &argv,
        &["--iterate"],
        full_timeout.as_deref(),
        master_seed,
        None,
    ) {
        Ok(code) => code,
        Err(code) => return code,
    };
    match read_pass(&outcomes, code, &mut stderr) {
        Ok(second) => report(first, Some(second), &run, &mut stdout, &mut stderr),
        Err(code) => code,
    }
}

/// One cargo-mutants pass: `argv`, then `extra` (pass 2's `--iterate`), the
/// pass's timeout and `--jobs`, under the pass's environment. `Ok` carries the
/// tool's exit code, which `read_pass` reads only when it left no record.
// Process spawning is edge tooling here, outside the deterministic core
// (DESIGN.md §1) — this shells out to cargo-mutants, which patches a scratch
// copy of the tree; nothing inside the core spawns processes.
#[allow(clippy::disallowed_methods)]
fn mutants_pass(
    opts: &Opts,
    argv: &[String],
    extra: &[&str],
    timeout: Option<&str>,
    master_seed: u64,
    first_runs: Option<u64>,
) -> Result<Option<i32>, i32> {
    let mut command = Command::new("cargo");
    command.args(argv).args(extra);
    if let Some(t) = timeout {
        command.args(["--timeout", t]);
    }
    if let Some(j) = &opts.jobs {
        command.args(["--jobs", j]);
    }
    match sim_env(&mut command, master_seed, first_runs)
        .current_dir(&opts.dir)
        .status()
    {
        Ok(status) => Ok(status.code()),
        Err(e) => {
            eprintln!("error: failed to run cargo-mutants: {e}");
            Err(2)
        }
    }
}

/// The `--timeout` for a pass with no explicit one: five times a timed
/// `cargo test` over the whole test set, under the pass's environment, when
/// cargo-mutants' own baseline times another command (`differs`), else the
/// tool's own. `label` names the pass in the timing line.
fn pass_timeout(
    opts: &Opts,
    differs: bool,
    master_seed: u64,
    first_runs: Option<u64>,
    label: &str,
) -> Result<Option<String>, i32> {
    if let Some(t) = &opts.timeout {
        return Ok(Some(t.clone()));
    }
    if !differs {
        return Ok(None);
    }
    match time_the_package_set(
        &opts.dir,
        &opts.test_packages,
        &[],
        master_seed,
        first_runs,
        "while timing the test set for --timeout",
    ) {
        Ok(secs) => {
            let derived = derived_timeout(secs);
            // Under --property each mutant runs one sim property, a subset of
            // what was timed, so the time only bounds it.
            let what = match &opts.property {
                None => "that is what each mutant runs".to_string(),
                Some(p) => format!(
                    "each mutant runs only the sim property `{p}`, so that is an upper bound on \
                     a mutant's run"
                ),
            };
            println!(
                "{}",
                timing_line(label, &opts.test_packages.join(", "), secs, &what, derived)
            );
            Ok(Some(derived.to_string()))
        }
        Err(msg) => {
            eprintln!("{msg}");
            Err(2)
        }
    }
}

fn timing_line(label: &str, over: &str, secs: f64, what: &str, derived: u64) -> String {
    format!(
        "mutants: {label}`cargo test` over {over} took {secs:.1} s of wall time; {what}, and \
         cargo-mutants' own baseline times only the mutated file's package, so the verb is \
         passing --timeout {derived} (5x)"
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
/// them, `tail` appended, under the pass's environment, in wall-clock seconds.
/// The build is outside the measurement, as it is in cargo-mutants' own
/// baseline. Wall time, not CPU time: `--timeout` is a wall-clock bound on
/// this same command, so wall time is its unit. CPU time would undershoot a
/// suite that sleeps or waits (the serve and edge tests do) and overshoot a
/// parallel one. And the error a wall-time measurement can carry (a machine
/// that slept or was busy mid-run) only lengthens it, the safe side for a
/// timeout.
///
/// The timed run's output is held, and printed only when the run is red,
/// above the error: a run red because a sim check tested nothing gets the
/// same error cargo-mutants' baseline would (`untested_baseline`). `why` says
/// what the run was for, in the error for any other red.
// Spawning cargo and reading the wall clock are edge tooling, outside the
// deterministic core (DESIGN.md §1).
#[allow(clippy::disallowed_methods, clippy::disallowed_types)]
fn time_the_package_set(
    dir: &str,
    packages: &[String],
    tail: &[&str],
    master_seed: u64,
    first_runs: Option<u64>,
    why: &str,
) -> Result<f64, String> {
    let mut args: Vec<&str> = vec!["test"];
    for pkg in packages {
        args.push("--package");
        args.push(pkg);
    }
    let build = [&args[..1], &["--no-run"], &args[1..]].concat();
    args.extend(tail);
    let cargo = |args: &[&str]| {
        let mut command = Command::new("cargo");
        command.args(args).current_dir(dir);
        sim_env(&mut command, master_seed, first_runs);
        command
    };
    let red = |args: &[&str], code: Option<i32>| {
        format!(
            "error: `cargo {}` exited {} {why} — the unmutated suite is not green, so no \
             mutant result would mean anything (cargo-mutants' own baseline refuses it too). \
             Fix the suite first.",
            args.join(" "),
            code.map_or("on a signal".to_string(), |c| c.to_string())
        )
    };
    let could_not = |args: &[&str], e: std::io::Error| {
        format!(
            "error: could not run `cargo {}` to time the test set: {e}",
            args.join(" ")
        )
    };
    let status = cargo(&build).status().map_err(|e| could_not(&build, e))?;
    if !status.success() {
        return Err(red(&build, status.code()));
    }
    let start = std::time::Instant::now();
    let out = cargo(&args).output().map_err(|e| could_not(&args, e))?;
    let secs = start.elapsed().as_secs_f64();
    if out.status.success() {
        return Ok(secs);
    }
    std::io::stdout()
        .write_all(&out.stdout)
        .expect("failed printing to stdout");
    std::io::stderr()
        .write_all(&out.stderr)
        .expect("failed printing to stderr");
    let log = [
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    ]
    .concat();
    Err(untested_baseline(&log).unwrap_or_else(|| red(&args, out.status.code())))
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

/// The run's record, or the error that names its path and the shape the verb
/// reads: another cargo-mutants version may write another one, and the install
/// probe checks only that the tool runs.
fn parse_outcomes(text: &str, path: &str) -> Result<Outcomes, String> {
    serde_json::from_str(text).map_err(|e| {
        format!(
            "error: could not parse {path}: {e}\n  the verb reads cargo-mutants 27.1.0's \
             outcomes.json, and this one has another shape: check `cargo mutants --version`, \
             and install the pinned version with\n  cargo install cargo-mutants --version \
             27.1.0 --locked"
        )
    })
}

/// Reads one pass's record and turns every "nothing was measured" shape into a
/// loud failure, written to `err`, whose exit code is the `Err`. The tool's
/// exit code is NOT consulted for the verdict — it is 0 both for a clean sweep
/// and for a filter that selected nothing — and IS consulted in exactly one
/// place, when there is no record at all (`no_outcomes_diagnosis`).
fn read_pass(
    path: &std::path::Path,
    code: Option<i32>,
    err: &mut dyn std::io::Write,
) -> Result<Outcomes, i32> {
    // Reading cargo-mutants' own run record is edge tooling, outside the
    // deterministic core (DESIGN.md §1) — nothing inside charpente-core
    // touches the filesystem directly.
    #[allow(clippy::disallowed_methods)]
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) => {
            writeln!(
                err,
                "{}",
                no_outcomes_diagnosis(code, &path.display().to_string(), &e.to_string())
            )
            .expect("failed printing to stderr");
            return Err(2);
        }
    };
    let o = match parse_outcomes(&text, &path.display().to_string()) {
        Ok(o) => o,
        Err(e) => {
            writeln!(err, "{e}").expect("failed printing to stderr");
            return Err(2);
        }
    };
    let baseline_log = o
        .outcomes
        .iter()
        .find(|x| matches!(x.scenario, Scenario::Baseline) && x.summary != "Success")
        .and_then(|x| x.log_path.as_deref())
        .and_then(|rel| {
            #[allow(clippy::disallowed_methods)]
            std::fs::read_to_string(path.parent()?.join(rel)).ok()
        });
    if let Some(line) = baseline_log.as_deref().and_then(|log| {
        out_of_disk_line(
            log,
            &std::env::temp_dir(),
            std::env::var("TMPDIR").ok().as_deref(),
        )
    }) {
        writeln!(err, "{line}").expect("failed printing to stderr");
        return Err(2);
    }
    let v = verdict(
        &o,
        &path.display().to_string(),
        None,
        baseline_log.as_deref(),
    );
    if v.code == 2 {
        for line in &v.lines {
            writeln!(err, "{line}").expect("failed printing to stderr");
        }
        return Err(2);
    }
    Ok(o)
}

/// What the report says about the run around its verdicts.
struct Run<'a> {
    /// The verb's own arguments, which the rerun line repeats.
    args: &'a [String],
    master_seed: u64,
    /// Pass 1's runs per check, or `None` for a run of one full pass.
    first_runs: Option<u64>,
    /// The test packages with no `sim_tests!` harness, judged at the full
    /// count in every pass.
    full_count: Vec<String>,
    property: Option<&'a str>,
    /// Where the record is, for `verdict`'s text.
    where_: String,
}

/// The run's result, from its one pass, or from pass 1 with pass 2's verdicts
/// in place of the mutants pass 2 judged again: each mutant's verdict and the
/// pass that decided it, the totals and their findings (`verdict`), the
/// packages judged at the full count, the master seed and the command that
/// reruns the run. It goes to `out`; `err` gets the one error, two passes that
/// do not add up to one run.
fn report(
    first: Outcomes,
    second: Option<Outcomes>,
    run: &Run,
    out: &mut dyn std::io::Write,
    err: &mut dyn std::io::Write,
) -> i32 {
    let o = match merge(first, second, run.first_runs.is_some()) {
        Ok(o) => o,
        Err(e) => {
            writeln!(err, "{e}").expect("failed printing to stderr");
            return 2;
        }
    };
    let mut lines = Vec::new();
    for x in &o.outcomes {
        let Scenario::Mutant(m) = &x.scenario else {
            continue;
        };
        if lines.is_empty() {
            lines.push("mutants: each mutant's verdict, and the pass that decided it:".to_string());
        }
        let word = match x.summary.as_str() {
            "CaughtMutant" => "caught",
            "MissedMutant" => "MISSED",
            "Timeout" => "TIMEOUT",
            "Unviable" => "unviable",
            other => other,
        };
        let pass = match x.pass {
            Pass::Only => "one full pass",
            Pass::First => "pass 1",
            Pass::Second => "pass 2",
        };
        lines.push(format!("  {word:<8} {} ({pass})", m.name));
    }
    let v = verdict(&o, &run.where_, run.property, None);
    lines.extend(v.lines);
    let seed = run.master_seed;
    match run.first_runs {
        Some(n) => {
            lines.push(format!(
                "  two passes on master seed {seed:#x}: pass 1 judged every mutant on the first \
                 {n} runs of each sim check, failed runs only, and pass 2 judged what it did not \
                 catch at the full count"
            ));
            if !run.full_count.is_empty() {
                lines.push(format!(
                    "  judged at the full count in both passes, with no sim_tests! harness to \
                     take a quick pass: {}",
                    run.full_count.join(", ")
                ));
            }
        }
        None => lines.push(format!(
            "  one full pass on master seed {seed:#x}: no test package has a sim_tests! harness \
             to take a quick pass ({})",
            run.full_count.join(", ")
        )),
    }
    lines.push(rerun_line(run.args, seed));
    for line in &lines {
        writeln!(out, "{line}").expect("failed printing to stdout");
    }
    v.code
}

/// Pass 1's record, each mutant tagged with its pass, with pass 2's verdict in
/// place of each mutant pass 2 judged again: the verdicts one full pass gives.
/// Pass 2 judges exactly the mutants pass 1 missed or timed out, matched by
/// name, so anything else means the tree changed between the passes.
fn merge(mut first: Outcomes, second: Option<Outcomes>, two: bool) -> Result<Outcomes, String> {
    for x in &mut first.outcomes {
        x.pass = if two { Pass::First } else { Pass::Only };
    }
    let Some(second) = second else {
        return Ok(first);
    };
    let left = first.missed + first.timeout;
    let mut matched = 0;
    for x in second.outcomes {
        let Scenario::Mutant(m) = &x.scenario else {
            continue;
        };
        let Some(slot) = first.outcomes.iter_mut().find(|y| {
            matches!(&y.scenario, Scenario::Mutant(n) if n.name == m.name)
                && matches!(y.summary.as_str(), "MissedMutant" | "Timeout")
        }) else {
            continue;
        };
        *slot = Outcome {
            pass: Pass::Second,
            ..x
        };
        matched += 1;
    }
    if second.total_mutants != left || matched != left {
        return Err(format!(
            "error: pass 1 left {left} mutants uncaught, and pass 2 judged {} of which {matched} \
             were among them — the source changed between the passes, so their verdicts are \
             not one run's. Rerun with the same --master-seed on a tree nothing edits meanwhile.",
            second.total_mutants
        ));
    }
    first.caught += second.caught;
    first.missed = second.missed;
    first.timeout = second.timeout;
    first.unviable += second.unviable;
    Ok(first)
}

struct Verdict {
    code: i32,
    lines: Vec<String>,
}

/// The decision, split out of `report`'s IO so the ruling's two anti-vacuity
/// rules are TESTED rather than merely intended — the spec requires exactly
/// that, and an untested honesty gate is the thing it is guarding against.
/// `baseline_log` is a failed baseline's log, which `report` reads.
fn verdict(
    o: &Outcomes,
    where_: &str,
    property: Option<&str>,
    baseline_log: Option<&str>,
) -> Verdict {
    if let Some(bad) = o
        .outcomes
        .iter()
        .find(|x| matches!(x.scenario, Scenario::Baseline) && x.summary != "Success")
    {
        if let Some(line) = baseline_log.and_then(untested_baseline) {
            return Verdict {
                code: 2,
                lines: vec![line],
            };
        }
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
    // A function whose every mutant was unviable was never measured: the
    // zero-mutant run's absent measurement, at function grain. It is named,
    // as the sim names an untested check, and is never a failure on its own,
    // since a return type the tool cannot build would fail every run.
    let mut measured: BTreeMap<(&str, &str), bool> = BTreeMap::new();
    for x in &o.outcomes {
        if let Scenario::Mutant(m) = &x.scenario {
            let function = m
                .function
                .as_ref()
                .map_or("(outside any function)", |f| f.function_name.as_str());
            *measured.entry((m.file.as_str(), function)).or_insert(false) |=
                x.summary != "Unviable";
        }
    }
    let untested: Vec<(&str, &str)> = measured
        .into_iter()
        .filter(|&(_, measured)| !measured)
        .map(|(at, _)| at)
        .collect();
    if !untested.is_empty() {
        lines.push(match untested.len() {
            1 => "  1 function had no viable mutant, so this run says nothing about it (untested):"
                .to_string(),
            n => format!(
                "  {n} functions had no viable mutant, so this run says nothing about them \
                 (untested):"
            ),
        });
        let mut file = "";
        for &(at, function) in &untested {
            if at != file {
                lines.push(format!("    {at}"));
                file = at;
            }
            lines.push(format!("      {function}"));
        }
        lines.push(
            "  To measure one, give its return type a value cargo-mutants can build, such as a \
             derived Default, or check its effect in a property (GUIDE §8 \"Finding a one-sided \
             property\")."
                .to_string(),
        );
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
    first_runs: u64,
    master_seed: Option<u64>,
}

fn parse(args: &[String]) -> Result<Opts, i32> {
    let mut dir = ".".to_string();
    let mut files = Vec::new();
    let mut res = Vec::new();
    let mut test_packages = Vec::new();
    let mut property = None;
    let mut timeout = None;
    let mut jobs = None;
    let mut first_runs = DEFAULT_FIRST_RUNS;
    let mut master_seed = None;

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
            "--first-runs" => match value.parse() {
                Ok(n) if n > 0 => first_runs = n,
                _ => {
                    eprintln!(
                        "error: --first-runs expects a number of runs, 1 or more, got \
                         \"{value}\"\n\n{USAGE}"
                    );
                    return Err(2);
                }
            },
            "--master-seed" => {
                let digits = value
                    .strip_prefix("0x")
                    .or_else(|| value.strip_prefix("0X"))
                    .unwrap_or(value);
                match u64::from_str_radix(digits, 16) {
                    Ok(seed) => master_seed = Some(seed),
                    Err(_) => {
                        eprintln!(
                            "error: --master-seed expects a hex value like 0xdeadbeef, got \
                             \"{value}\"\n\n{USAGE}"
                        );
                        return Err(2);
                    }
                }
            }
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
        first_runs,
        master_seed,
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

    /// A run's context for `report`: master seed 0x1, no flags given, and
    /// `first_runs` for two passes or `None` for one.
    fn run_ctx(first_runs: Option<u64>, full_count: &[&str]) -> Run<'static> {
        Run {
            args: &[],
            master_seed: 0x1,
            first_runs,
            full_count: full_count.iter().map(|s| (*s).to_string()).collect(),
            property: None,
            where_: "out".to_string(),
        }
    }

    /// `path`'s record read as the verb reads a pass's, then reported as a run
    /// of that one pass: the exit code, stdout and stderr.
    fn one_pass_report(path: &std::path::Path, code: Option<i32>) -> (i32, String, String) {
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = match read_pass(path, code, &mut err) {
            Ok(only) => report(only, None, &run_ctx(None, &["app"]), &mut out, &mut err),
            Err(code) => code,
        };
        (
            code,
            String::from_utf8(out).unwrap(),
            String::from_utf8(err).unwrap(),
        )
    }

    /// R50: a run that measured nothing (code 2) is reported on stderr, and a
    /// result, a clean one or a finding, on stdout, so a script reading the
    /// verb's stdout never takes an error for a result.
    // Writing the records `report` reads is edge tooling, as `report` is.
    #[test]
    #[allow(clippy::disallowed_methods)]
    fn a_run_that_measured_nothing_is_reported_on_stderr_and_a_result_on_stdout() {
        let dir =
            std::env::temp_dir().join(format!("charpente-mutants-report-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let report_of = |name: &str, record: Option<&str>| {
            let path = dir.join(name);
            if let Some(record) = record {
                std::fs::write(&path, record).expect("write the record");
            }
            one_pass_report(&path, Some(0))
        };
        let failed_baseline = r#"{"total_mutants":3,"caught":0,"missed":0,"timeout":0,"unviable":0,
            "outcomes":[{"scenario":"Baseline","summary":"Failure"}]}"#;
        let (code, out, err) = report_of("failed-baseline.json", Some(failed_baseline));
        assert_eq!((code, out.as_str()), (2, ""), "{err}");
        assert!(
            err.starts_with("error: the UNMUTATED baseline was not green (Failure)")
                && err.ends_with(" may not).\n")
                && err.lines().count() == 1,
            "{err}"
        );
        for (name, record) in [
            ("missing.json", None),
            ("another-shape.json", Some(r#"{"total_mutants":1}"#)),
        ] {
            let (code, out, err) = report_of(name, record);
            assert_eq!((code, out.as_str()), (2, ""), "{name}: {err}");
            assert!(err.starts_with("error: "), "{name}: {err}");
        }
        let clean = format!(
            r#"{{"total_mutants":5,"caught":5,"missed":0,"timeout":0,"unviable":0,{OK_BASELINE}}}"#
        );
        let (code, out, err) = report_of("clean.json", Some(&clean));
        assert_eq!(
            (code, out.as_str(), err.as_str()),
            (
                0,
                "mutants: 5 total — 5 caught, 0 missed, 0 unviable, 0 timed out\n  one full pass \
                 on master seed 0x1: no test package has a sim_tests! harness to take a quick \
                 pass (app)\nrerun exactly this mutation run: charpente test --mutants \
                 --master-seed 0x1\n",
                ""
            )
        );
        let missed = format!(
            r#"{{"total_mutants":5,"caught":4,"missed":1,"timeout":0,"unviable":0,{OK_BASELINE}}}"#
        );
        let (code, out, err) = report_of("missed.json", Some(&missed));
        assert_eq!((code, err.as_str()), (1, ""), "{out}");
        assert!(
            out.starts_with(
                "mutants: 5 total — 4 caught, 1 missed, 0 unviable, 0 timed out\n  1 MISSED: "
            ),
            "{out}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A record in another shape than 27.1.0's, here a mutant with no `file`,
    /// is refused with its path, the version the verb reads and the line that
    /// installs it (CLAUDE.md hard rule 5).
    #[test]
    fn a_record_in_another_shape_names_the_version_the_verb_reads() {
        let record = r#"{"total_mutants":1,"caught":1,"missed":0,"timeout":0,"unviable":0,
            "outcomes":[{"scenario":"Baseline","summary":"Success"},
            {"scenario":{"Mutant":{"path":"src/lib.rs"}},"summary":"CaughtMutant"}]}"#;
        let Err(e) = parse_outcomes(record, "out/outcomes.json") else {
            panic!("a mutant with no `file` parses");
        };
        let (first, rest) = e.split_once('\n').unwrap_or_else(|| panic!("{e}"));
        assert!(
            first.starts_with("error: could not parse out/outcomes.json: missing field `file`"),
            "{e}"
        );
        assert_eq!(
            rest,
            "  the verb reads cargo-mutants 27.1.0's outcomes.json, and this one has another \
             shape: check `cargo mutants --version`, and install the pinned version with\n  \
             cargo install cargo-mutants --version 27.1.0 --locked",
        );
    }

    #[test]
    fn every_viable_mutant_caught_is_the_only_green() {
        let v = verdict(
            &outcomes(&format!(
                r#"{{"total_mutants":5,"caught":5,"missed":0,"timeout":0,"unviable":0,{OK_BASELINE}}}"#
            )),
            "out",
            None,
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
            None,
        );
        assert_eq!(v.code, 2, "{:?}", v.lines);
        assert!(
            v.lines[0].contains("baseline"),
            "the diagnosis must be the baseline, not the zero count it also produces: {:?}",
            v.lines
        );
    }

    /// Step 3: a baseline that failed for lack of disk names the scratch location and
    /// TMPDIR; any other failed baseline is left to `verdict`'s generic text.
    #[test]
    #[allow(clippy::disallowed_methods)]
    fn a_baseline_that_ran_out_of_disk_names_tmpdir() {
        let dir = std::env::temp_dir().join(format!("charpente-disk-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("log")).expect("scratch");
        let json = r#"{"total_mutants":0,"caught":0,"missed":0,"timeout":0,"unviable":0,
            "outcomes":[{"scenario":"Baseline","summary":"Failure","log_path":"log/baseline.log"}]}"#;
        std::fs::write(dir.join("outcomes.json"), json).expect("outcomes");
        let report_of = || {
            let (code, _, err) = one_pass_report(&dir.join("outcomes.json"), Some(2));
            (code, err)
        };
        std::fs::write(
            dir.join("log/baseline.log"),
            "error: No space left on device (os error 28)\n",
        )
        .expect("log");
        let (code, err) = report_of();
        assert_eq!(code, 2);
        // The whole line, written out rather than rebuilt with `out_of_disk_line`,
        // so `read_pass` passing another path or dropping the variable is red here.
        // A known TMPDIR is set in a child process by tests/cli.rs, since setting
        // it here would move every other test's `temp_dir()`.
        assert_eq!(
            err,
            format!(
                "error: the unmutated baseline ran out of disk space in cargo-mutants' scratch \
                 copy under {} — point TMPDIR at a disk with room for a full build of this \
                 workspace (TMPDIR is {})\n",
                std::env::temp_dir().display(),
                std::env::var("TMPDIR").as_deref().unwrap_or("unset")
            )
        );
        assert_eq!(
            out_of_disk_line("No space left on device", std::path::Path::new("/t"), None)
                .as_deref(),
            Some(
                "error: the unmutated baseline ran out of disk space in cargo-mutants' scratch copy \
                 under /t — point TMPDIR at a disk with room for a full build of this workspace \
                 (TMPDIR is unset)"
            )
        );
        assert_eq!(
            out_of_disk_line(
                "No space left on device",
                std::path::Path::new("/big/T"),
                Some("/big")
            )
            .as_deref(),
            Some(
                "error: the unmutated baseline ran out of disk space in cargo-mutants' scratch copy \
                 under /big/T — point TMPDIR at a disk with room for a full build of this \
                 workspace (TMPDIR is /big)"
            )
        );
        std::fs::write(
            dir.join("log/baseline.log"),
            "error: no matching package named `x`\n",
        )
        .expect("log");
        let (code, err) = report_of();
        assert_eq!(code, 2);
        assert!(
            err.starts_with("error: the UNMUTATED baseline was not green"),
            "{err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A sim batch with an untested check exits 1, so an unmutated baseline can
    /// be red for that alone. The error names the check, read from the baseline's
    /// own log through `log_path`, and no other cause: the fixture log also holds
    /// an `ok` check and a red `test result:` that would point elsewhere.
    #[test]
    #[allow(clippy::disallowed_methods)]
    fn a_baseline_red_for_an_untested_check_names_the_check() {
        let dir = std::env::temp_dir().join(format!("charpente-untested-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("log")).expect("scratch");
        let json = r#"{"total_mutants":0,"caught":0,"missed":0,"timeout":0,"unviable":0,
            "outcomes":[{"scenario":"Baseline","summary":"Failure","log_path":"log/baseline.log"}]}"#;
        std::fs::write(dir.join("outcomes.json"), json).expect("outcomes");
        let report_of = || one_pass_report(&dir.join("outcomes.json"), Some(2));
        std::fs::write(
            dir.join("log/baseline.log"),
            "*** baseline\n\n*** cargo test --package=app\n     Running tests/sim.rs \
             (target/debug/deps/sim-0123456789abcdef)\n\
             running 2 sim checks, 256 runs each, under random injected faults (master seed 0x1)\n\
             \n\
             skipped runs hit a fault before reaching the check, so they prove nothing either way:\n\
             \x20 note_survives — never tested: every run was skipped\n\
             \x20   256  no POST /notes became durable (first: --seed 0x2)\n\
             \n\
             check echo_matches .... ok  (256 tested)\n\
             check note_survives ... untested  (0 tested, 256 skipped)\n\
             \n\
             sim result: FAILED. 1 passed; 0 failed; 1 untested. Runs tested per check, in the \
             order above: 256, 0 of 256.\n\
             error: test failed, to rerun pass `--test sim`\n",
        )
        .expect("log");
        let (code, out, err) = report_of();
        assert_eq!((code, out.as_str()), (2, ""), "{err}");
        assert_eq!(
            err,
            "error: the UNMUTATED baseline was not green: check note_survives tested nothing, \
             and a check that tested nothing fails the batch. Fix its setup, or scope --property \
             to a check that runs.\n"
        );
        // A red baseline whose log names no untested check keeps the generic text.
        std::fs::write(
            dir.join("log/baseline.log"),
            "check echo_matches ... FAILED  (256 tested: 3 failed)\n",
        )
        .expect("log");
        let (code, _, err) = report_of();
        assert_eq!(code, 2);
        assert!(
            err.starts_with("error: the UNMUTATED baseline was not green (Failure) — "),
            "{err}"
        );
        assert_eq!(
            untested_check("check a ... untested  (0 tested, 4 skipped)"),
            Some("a")
        );
        assert_eq!(untested_check("check untested ... ok  (4 tested)"), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn zero_mutants_is_not_a_clean_run() {
        let v = verdict(
            &outcomes(&format!(
                r#"{{"total_mutants":0,"caught":0,"missed":0,"timeout":0,"unviable":0,{OK_BASELINE}}}"#
            )),
            "out",
            None,
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
            None,
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
            None,
        );
        assert_eq!(v.code, 1);
        assert!(
            v.lines.iter().any(|l| l.contains("MISSED")),
            "{:?}",
            v.lines
        );
    }

    /// One mutant's record in cargo-mutants 27.1.0's `outcomes.json` shape,
    /// the fields this verb reads where a real record carries them (the
    /// `Prices::lowest` record below is one, verbatim but for its timings).
    fn mutant(file: &str, function: &str, summary: &str) -> String {
        format!(
            r#"{{"scenario":{{"Mutant":{{"name":"{file}:1:1: replace {function}","package":"app","file":"{file}","function":{{"function_name":"{function}","return_type":"-> T"}},"replacement":"Default::default()","genre":"FnValue"}}}},"summary":"{summary}"}}"#
        )
    }

    fn run_of(counts: &str, mutants: &[String]) -> Outcomes {
        outcomes(&format!(
            r#"{{{counts},"outcomes":[{{"scenario":"Baseline","summary":"Success"}},{}]}}"#,
            mutants.join(",")
        ))
    }

    /// From the T22 build's run (cargo-mutants 27.1.0), less `phase_results`.
    const REAL_UNVIABLE: &str = r#"{"scenario":{"Mutant":{"name":"src/lib.rs:88:9: replace Prices::lowest -> Obs with Default::default()","package":"pricewatch","file":"src/lib.rs","function":{"function_name":"Prices::lowest","return_type":"-> Obs","span":{"start":{"line":87,"column":5},"end":{"line":93,"column":6}}},"span":{"start":{"line":88,"column":9},"end":{"line":92,"column":10}},"replacement":"Default::default()","genre":"FnValue"}},"summary":"Unviable","log_path":"log/src__lib.rs_line_88_col_9.log","diff_path":"diff/src__lib.rs_line_88_col_9.diff"}"#;

    /// R45: a function whose every mutant was unviable was never measured,
    /// so the verb names it below the totals, grouped by file, and the run
    /// still exits by its missed and timed-out mutants alone (none here).
    #[test]
    fn a_function_whose_every_mutant_is_unviable_is_named_untested() {
        let v = verdict(
            &run_of(
                r#""total_mutants":6,"caught":3,"missed":0,"timeout":0,"unviable":3"#,
                &[
                    mutant("src/merge.rs", "lowest", "Unviable"),
                    mutant("src/merge.rs", "lowest", "Unviable"),
                    mutant("src/lib.rs", "submit_report", "CaughtMutant"),
                    REAL_UNVIABLE.to_string(),
                    mutant("src/lib.rs", "read_product", "CaughtMutant"),
                    mutant("src/lib.rs", "read_product", "CaughtMutant"),
                ],
            ),
            "out",
            None,
            None,
        );
        assert_eq!(
            v.code, 0,
            "untested is named, never a failure: {:?}",
            v.lines
        );
        assert_eq!(
            v.lines,
            [
                "mutants: 6 total — 3 caught, 0 missed, 3 unviable, 0 timed out",
                "  2 functions had no viable mutant, so this run says nothing about them \
                 (untested):",
                "    src/lib.rs",
                "      Prices::lowest",
                "    src/merge.rs",
                "      lowest",
                "  To measure one, give its return type a value cargo-mutants can build, such \
                 as a derived Default, or check its effect in a property (GUIDE §8 \"Finding \
                 a one-sided property\").",
            ]
        );
    }

    /// A single viable mutant measures its function, whatever the tool made
    /// of the rest: caught, missed and timed out each count as viable.
    #[test]
    fn a_function_with_one_viable_mutant_is_not_named() {
        for viable in ["CaughtMutant", "MissedMutant", "Timeout"] {
            let v = verdict(
                &run_of(
                    r#""total_mutants":3,"caught":1,"missed":0,"timeout":0,"unviable":2"#,
                    &[
                        mutant("src/merge.rs", "latest", "Unviable"),
                        mutant("src/merge.rs", "latest", viable),
                        mutant("src/merge.rs", "latest", "Unviable"),
                    ],
                ),
                "out",
                None,
                None,
            );
            assert!(
                !v.lines.iter().any(|l| l.contains("no viable mutant")),
                "{viable}: {:?}",
                v.lines
            );
        }
        // One function per name: `latest` in another file is its own.
        let v = verdict(
            &run_of(
                r#""total_mutants":2,"caught":1,"missed":0,"timeout":0,"unviable":1"#,
                &[
                    mutant("src/merge.rs", "latest", "CaughtMutant"),
                    mutant("src/lib.rs", "latest", "Unviable"),
                ],
            ),
            "out",
            None,
            None,
        );
        assert_eq!(
            v.lines[1..3],
            [
                "  1 function had no viable mutant, so this run says nothing about it \
                 (untested):",
                "    src/lib.rs",
            ],
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

    /// One mutant's record, named as cargo-mutants names it.
    fn named(line: u32, function: &str, summary: &str) -> String {
        format!(
            r#"{{"scenario":{{"Mutant":{{"name":"src/lib.rs:{line}:5: replace {function} with ()","file":"src/lib.rs","function":{{"function_name":"{function}"}}}}}},"summary":"{summary}"}}"#
        )
    }

    /// A pass's record with these counters and mutants, after a green baseline.
    fn record(counts: [u64; 5], mutants: &[String]) -> String {
        let [total, caught, missed, timeout, unviable] = counts;
        format!(
            r#"{{"total_mutants":{total},"caught":{caught},"missed":{missed},"timeout":{timeout},"unviable":{unviable},"outcomes":[{{"scenario":"Baseline","summary":"Success"}},{}]}}"#,
            mutants.join(",")
        )
    }

    /// Writes each record to its own `outcomes.json` and reads it back as the
    /// verb reads a pass's.
    // Writing the records `read_pass` reads is edge tooling, as `read_pass` is.
    #[allow(clippy::disallowed_methods)]
    fn passes_from_files(tag: &str, records: &[String]) -> Vec<Outcomes> {
        let dir =
            std::env::temp_dir().join(format!("charpente-mutants-{tag}-{}", std::process::id()));
        let read = records
            .iter()
            .enumerate()
            .map(|(n, text)| {
                let path = dir.join(format!("pass-{n}/outcomes.json"));
                std::fs::create_dir_all(path.parent().unwrap()).expect("scratch");
                std::fs::write(&path, text).expect("record");
                let mut err = Vec::new();
                read_pass(&path, Some(0), &mut err)
                    .unwrap_or_else(|_| panic!("{}", String::from_utf8_lossy(&err)))
            })
            .collect();
        let _ = std::fs::remove_dir_all(&dir);
        read
    }

    /// Spec §4 E: pass 2's verdict replaces pass 1's for each mutant pass 2
    /// judged again, each mutant is tagged with the pass that decided it, and
    /// the totals, the findings and the tail are one run's. The rerun line
    /// repeats the flags given with this run's seed in place of the one given.
    #[test]
    fn two_passes_report_as_one_run_each_mutant_tagged_with_the_pass_that_decided_it() {
        let mut passes = passes_from_files(
            "merge",
            &[
                record(
                    [5, 1, 2, 1, 1],
                    &[
                        named(3, "every_run", "CaughtMutant"),
                        named(7, "in_band", "MissedMutant"),
                        named(11, "covered", "MissedMutant"),
                        named(15, "slow", "Timeout"),
                        named(19, "opaque", "Unviable"),
                    ],
                ),
                record(
                    [3, 2, 1, 0, 0],
                    &[
                        named(7, "in_band", "CaughtMutant"),
                        named(11, "covered", "MissedMutant"),
                        named(15, "slow", "CaughtMutant"),
                    ],
                ),
            ],
        );
        let second = passes.pop();
        let first = passes.pop().expect("two records");
        let args = [
            "--file",
            "src/lib.rs",
            "--re",
            "in|cov",
            "--master-seed",
            "0x5",
        ]
        .map(String::from);
        let run = Run {
            args: &args,
            master_seed: 0xabc,
            ..run_ctx(Some(32), &["charpente-core"])
        };
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = report(first, second, &run, &mut out, &mut err);
        let (out, err) = (
            String::from_utf8(out).unwrap(),
            String::from_utf8(err).unwrap(),
        );
        assert_eq!((code, err.as_str()), (1, ""), "{out}");
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(
            lines[..7],
            [
                "mutants: each mutant's verdict, and the pass that decided it:",
                "  caught   src/lib.rs:3:5: replace every_run with () (pass 1)",
                "  caught   src/lib.rs:7:5: replace in_band with () (pass 2)",
                "  MISSED   src/lib.rs:11:5: replace covered with () (pass 2)",
                "  caught   src/lib.rs:15:5: replace slow with () (pass 2)",
                "  unviable src/lib.rs:19:5: replace opaque with () (pass 1)",
                "mutants: 5 total — 3 caught, 1 missed, 1 unviable, 0 timed out",
            ],
            "{out}"
        );
        assert!(lines[7].starts_with("  1 MISSED: "), "{out}");
        assert!(
            !out.contains("TIMED OUT"),
            "pass 2 resolved the timeout: {out}"
        );
        assert_eq!(
            lines[lines.len() - 3..],
            [
                "  two passes on master seed 0xabc: pass 1 judged every mutant on the first 32 \
                 runs of each sim check, failed runs only, and pass 2 judged what it did not \
                 catch at the full count",
                "  judged at the full count in both passes, with no sim_tests! harness to take a \
                 quick pass: charpente-core",
                "rerun exactly this mutation run: charpente test --mutants --file src/lib.rs --re \
                 'in|cov' --master-seed 0xabc",
            ],
            "{out}"
        );
        // Pass 1 alone, when it left pass 2 nothing: every tag is pass 1's.
        let first = passes_from_files(
            "merge-one",
            &[record(
                [1, 1, 0, 0, 0],
                &[named(3, "every_run", "CaughtMutant")],
            )],
        )
        .pop()
        .expect("one record");
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = report(first, None, &run_ctx(Some(32), &[]), &mut out, &mut err);
        let out = String::from_utf8(out).unwrap();
        assert_eq!((code, err.as_slice()), (0, &b""[..]), "{out}");
        assert!(
            out.contains("  caught   src/lib.rs:3:5: replace every_run with () (pass 1)\n")
                && !out.contains("judged at the full count in both passes"),
            "{out}"
        );
    }

    /// Two passes add up to one run only when pass 2 judged exactly the
    /// mutants pass 1 left, by name; a tree edited between them moves a name,
    /// and the report refuses rather than mix two trees' verdicts.
    #[test]
    fn passes_that_do_not_add_up_are_refused() {
        let mut passes = passes_from_files(
            "shifted",
            &[
                record(
                    [2, 1, 1, 0, 0],
                    &[
                        named(3, "every_run", "CaughtMutant"),
                        named(7, "in_band", "MissedMutant"),
                    ],
                ),
                record([1, 1, 0, 0, 0], &[named(8, "in_band", "CaughtMutant")]),
            ],
        );
        let second = passes.pop();
        let first = passes.pop().expect("two records");
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = report(first, second, &run_ctx(Some(32), &[]), &mut out, &mut err);
        let err = String::from_utf8(err).unwrap();
        assert_eq!((code, out.as_slice()), (2, &b""[..]), "{err}");
        assert_eq!(
            err,
            "error: pass 1 left 1 mutants uncaught, and pass 2 judged 1 of which 0 were among \
             them — the source changed between the passes, so their verdicts are not one run's. \
             Rerun with the same --master-seed on a tree nothing edits meanwhile.\n"
        );
    }

    /// Spec §4 E: only a `sim_tests!` harness reads the quick pass's variable,
    /// so only a package declaring one takes a quick pass; the rest, and every
    /// package when `cargo metadata` cannot say, run in full.
    #[test]
    #[allow(clippy::disallowed_methods)]
    fn only_a_package_with_a_sim_tests_harness_takes_a_quick_pass() {
        let app = "[package]\nname = \"app\"\n\n[[test]]\nname = \"sim\"\nharness = false\n";
        assert!(has_sim_harness(app));
        for platform in [
            "[package]\nname = \"core\"\n",
            "[[test]]\nname = \"sim\"\n",
            "[[test]]\nname = \"sim\"\nharness = true\n",
            "[[test]]\nname = \"wal\"\nharness = false\n",
            "[[test\n",
        ] {
            assert!(!has_sim_harness(platform), "{platform}");
        }
        let dir =
            std::env::temp_dir().join(format!("charpente-mutants-split-{}", std::process::id()));
        let mut packages = Vec::new();
        for (name, manifest) in [("app", app), ("core", "[package]\nname = \"core\"\n")] {
            let path = dir.join(name).join("Cargo.toml");
            std::fs::create_dir_all(path.parent().unwrap()).expect("scratch");
            std::fs::write(&path, manifest).expect("manifest");
            packages.push(serde_json::json!({"name": name, "manifest_path": display(&path)}));
        }
        let meta = serde_json::json!({ "packages": packages });
        let test_packages = ["core", "app", "absent"].map(String::from);
        assert_eq!(sim_harness_packages(Some(&meta), &test_packages), ["app"]);
        assert!(sim_harness_packages(None, &test_packages).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_rerun_line_quotes_what_a_shell_would_expand() {
        let args = [
            "--re",
            "BumpCounter::run.*Ok",
            "--master-seed",
            "0X5",
            "--property",
            "it's",
            "--dir",
            "../app",
        ]
        .map(String::from);
        assert_eq!(
            rerun_line(&args, 0xbeef),
            "rerun exactly this mutation run: charpente test --mutants --re \
             'BumpCounter::run.*Ok' --property 'it'\\''s' --dir ../app --master-seed 0xbeef"
        );
        assert_eq!(
            rerun_line(&[], 0x1),
            "rerun exactly this mutation run: charpente test --mutants --master-seed 0x1"
        );
    }

    #[test]
    fn first_runs_defaults_to_32_and_the_master_seed_reads_as_hex() {
        let opts = parse(&[]).expect("no flags parse");
        assert_eq!((opts.first_runs, opts.master_seed), (32, None));
        let opts = parse(&["--first-runs", "8", "--master-seed", "0xBEEF"].map(String::from))
            .expect("parses");
        assert_eq!((opts.first_runs, opts.master_seed), (8, Some(0xbeef)));
        for bad in [
            ["--first-runs", "0"],
            ["--first-runs", "x"],
            ["--master-seed", "zz"],
        ] {
            assert_eq!(parse(&bad.map(String::from)).err(), Some(2), "{bad:?}");
        }
    }
}
