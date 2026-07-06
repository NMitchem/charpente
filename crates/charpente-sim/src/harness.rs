//! `charpente test --sim` harness core (step-7 spec: docs/superpowers/specs/
//! 2026-07-05-sim-cli-design.md). Registers app-authored sim workloads
//! (`SimTest`), parses runner flags (`HarnessArgs`), and runs them as a
//! sequential batch reusing the frozen single-universe executor
//! (`crate::run_universe`) — the same seed→behavior mapping the internal
//! swarm macro uses. Parallelism and JSON output (`Report::to_json`) land on
//! top of this module later without changing these types.
//!
//! Batch model: one splitmix64 chain, seeded from the master seed, consumed
//! in registry order — each selected test draws `args.universes` seeds in
//! turn. `--filter` selects tests BEFORE any seed is drawn, so a filtered
//! run consumes a different (but still fully reproducible) prefix of the
//! chain than an unfiltered one; that's why every repro line carries the
//! explicit universe seed rather than relying on the batch's master seed.
//! `--seed 0x…` bypasses the chain entirely: each selected test runs once on
//! that literal seed, and coverage requirements are skipped (a single
//! universe can't be expected to hit everything a batch would).

use std::collections::BTreeMap;

use crate::rng::splitmix64;
use crate::{FaultConfig, Sim, ambient_base_seed, run_universe};

/// One registered sim workload. `require` lists sometimes-coverage keys that
/// must be hit at least once across the test's universes in a batch run
/// (skipped under `--seed`) — the same semantics as `run_universes_swarm!`.
pub struct SimTest {
    pub name: &'static str,
    pub run: fn(&mut Sim) -> Result<(), String>,
    pub require: &'static [&'static str],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Human,
    Json,
}

#[derive(Debug)]
pub struct HarnessArgs {
    pub universes: u64,
    pub seed: Option<u64>,
    pub master_seed: Option<u64>,
    pub filter: Option<String>,
    pub jobs: usize,
    pub format: Format,
}

const VALID_FLAGS: &str = "--universes, --seed, --master-seed, --filter, --jobs, --format";

impl HarnessArgs {
    pub fn parse(mut args: impl Iterator<Item = String>) -> Result<HarnessArgs, String> {
        let mut universes = 256u64;
        let mut seed = None;
        let mut master_seed = None;
        let mut filter = None;
        let mut jobs = default_jobs();
        let mut format = Format::Human;

        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--universes" => universes = parse_number(&mut args, &flag)?,
                "--seed" => seed = Some(parse_hex(&mut args, &flag)?),
                "--master-seed" => master_seed = Some(parse_hex(&mut args, &flag)?),
                "--filter" => filter = Some(next_value(&mut args, &flag)?),
                "--jobs" => jobs = parse_number(&mut args, &flag)?,
                "--format" => {
                    let value = next_value(&mut args, &flag)?;
                    format = match value.as_str() {
                        "human" => Format::Human,
                        "json" => Format::Json,
                        _ => {
                            return Err(format!(
                                "--format expects \"human\" or \"json\", got \"{value}\""
                            ));
                        }
                    };
                }
                other => {
                    return Err(format!(
                        "unknown flag \"{other}\" — valid flags: {VALID_FLAGS}"
                    ));
                }
            }
        }

        Ok(HarnessArgs {
            universes,
            seed,
            master_seed,
            filter,
            jobs,
            format,
        })
    }
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_number<T: std::str::FromStr>(
    args: &mut impl Iterator<Item = String>,
    flag: &str,
) -> Result<T, String> {
    let raw = next_value(args, flag)?;
    raw.parse()
        .map_err(|_| format!("{flag} expects a number, got \"{raw}\""))
}

fn parse_hex(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<u64, String> {
    let raw = next_value(args, flag)?;
    let hex = raw
        .strip_prefix("0x")
        .or_else(|| raw.strip_prefix("0X"))
        .unwrap_or(&raw);
    u64::from_str_radix(hex, 16)
        .map_err(|_| format!("{flag} expects a hex value like 0xdeadbeef, got \"{raw}\""))
}

// The harness runs outside the deterministic boundary — it orchestrates
// universes, each of which is its own sealed, seed-determined world.
// Querying the host's core count to size the (later) thread pool isn't
// ambient nondeterminism the way a core-code wall-clock read would be, and
// it's not on the clippy.toml deny-list.
fn default_jobs() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1)
}

/// One universe's failure: enough to file it AND reproduce it (DESIGN §7 /
/// CLAUDE.md hard rule 5).
#[derive(Debug)]
pub struct Failure {
    pub test: &'static str,
    pub seed: u64,
    pub config: FaultConfig,
    pub message: String,
}

#[derive(Debug)]
pub struct Report {
    pub master_seed: u64,
    pub universes: u64,
    pub failures: Vec<Failure>,
    pub coverage: BTreeMap<&'static str, u64>,
    /// (test name, key) — a `require`d key never hit across that test's
    /// universes in this batch.
    pub missing_coverage: Vec<(String, String)>,
}

/// Runs every test in `tests` whose name contains `args.filter` (all of them
/// if unset) and writes a human report to `out`. Exit code: 0 green, 1 if
/// any universe failed or a coverage requirement went unmet, 2 if the
/// selection is empty — a filter that matches nothing is a usage error, not
/// a green run of zero tests.
pub fn run(tests: &[SimTest], args: &HarnessArgs, out: &mut dyn std::io::Write) -> i32 {
    let selected: Vec<&SimTest> = tests
        .iter()
        .filter(|t| match &args.filter {
            Some(f) => t.name.contains(f.as_str()),
            None => true,
        })
        .collect();
    if selected.is_empty() {
        let _ = writeln!(
            out,
            "no sim tests matched{} — {} registered",
            args.filter
                .as_deref()
                .map(|f| format!(" filter \"{f}\""))
                .unwrap_or_default(),
            tests.len()
        );
        return 2;
    }

    let report = match args.seed {
        Some(seed) => run_single_seed(&selected, seed),
        None => run_batch(
            &selected,
            args.master_seed.unwrap_or_else(ambient_base_seed),
            args.universes,
        ),
    };

    write_human(&report, out);
    if report.failures.is_empty() && report.missing_coverage.is_empty() {
        0
    } else {
        1
    }
}

fn run_single_seed(tests: &[&SimTest], seed: u64) -> Report {
    let mut report = Report {
        master_seed: seed,
        universes: 1,
        failures: Vec::new(),
        coverage: BTreeMap::new(),
        missing_coverage: Vec::new(),
    };
    for test in tests {
        match run_universe(seed, &test.run) {
            Ok(cov) => merge(&mut report.coverage, &cov),
            Err((config, message)) => report.failures.push(Failure {
                test: test.name,
                seed,
                config,
                message,
            }),
        }
    }
    report
}

fn run_batch(tests: &[&SimTest], master_seed: u64, universes: u64) -> Report {
    let mut report = Report {
        master_seed,
        universes,
        failures: Vec::new(),
        coverage: BTreeMap::new(),
        missing_coverage: Vec::new(),
    };
    let mut state = master_seed;
    for test in tests {
        let mut covered: BTreeMap<&'static str, u64> = BTreeMap::new();
        for _ in 0..universes {
            let seed = splitmix64(&mut state);
            match run_universe(seed, &test.run) {
                Ok(cov) => merge(&mut covered, &cov),
                Err((config, message)) => report.failures.push(Failure {
                    test: test.name,
                    seed,
                    config,
                    message,
                }),
            }
        }
        merge(&mut report.coverage, &covered);
        for key in test.require {
            if covered.get(key).copied().unwrap_or(0) == 0 {
                report
                    .missing_coverage
                    .push((test.name.to_string(), (*key).to_string()));
            }
        }
    }
    report
}

fn merge(into: &mut BTreeMap<&'static str, u64>, from: &BTreeMap<&'static str, u64>) {
    for (key, count) in from {
        *into.entry(key).or_insert(0) += count;
    }
}

fn write_human(report: &Report, out: &mut dyn std::io::Write) {
    for failure in &report.failures {
        let _ = write!(out, "{}", human_failure_block(failure));
    }
    for (test, key) in &report.missing_coverage {
        let _ = writeln!(
            out,
            "sim coverage requirement failed in {test}: \"{key}\" never occurred over {} \
             universes — raise the universe count, adjust the workload, or drop the \
             requirement (a required state that never occurs is an untested claim)",
            report.universes
        );
    }
    if report.failures.is_empty() && report.missing_coverage.is_empty() {
        let _ = writeln!(
            out,
            "sim: {} universe(s) per test, all green (master seed {:#018x})",
            report.universes, report.master_seed
        );
    }
}

fn human_failure_block(f: &Failure) -> String {
    format!(
        "\nsim failure in {}: {}\n  seed:   {:#018x}\n  config: {:?}\n  repro:  charpente test --sim --filter {} --seed {:#x}\n",
        f.test, f.message, f.seed, f.config, f.test, f.seed
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn always_ok(_sim: &mut Sim) -> Result<(), String> {
        Ok(())
    }

    fn always_fails(_sim: &mut Sim) -> Result<(), String> {
        Err("toy failure".into())
    }

    fn parse(flags: &[&str]) -> Result<HarnessArgs, String> {
        HarnessArgs::parse(flags.iter().map(|s| s.to_string()))
    }

    // ---------------------------------------------------------- arg parsing

    #[test]
    fn defaults_with_no_flags() {
        let args = parse(&[]).unwrap();
        assert_eq!(args.universes, 256);
        assert_eq!(args.seed, None);
        assert_eq!(args.master_seed, None);
        assert_eq!(args.filter, None);
        assert_eq!(args.format, Format::Human);
        assert!(args.jobs >= 1, "jobs must default to at least 1");
    }

    #[test]
    fn parses_universes() {
        let args = parse(&["--universes", "10"]).unwrap();
        assert_eq!(args.universes, 10);
    }

    #[test]
    fn parses_seed_with_and_without_0x_prefix() {
        assert_eq!(parse(&["--seed", "0xdead"]).unwrap().seed, Some(0xdead));
        assert_eq!(parse(&["--seed", "dead"]).unwrap().seed, Some(0xdead));
    }

    #[test]
    fn parses_master_seed() {
        let args = parse(&["--master-seed", "0xbeef"]).unwrap();
        assert_eq!(args.master_seed, Some(0xbeef));
    }

    #[test]
    fn parses_filter() {
        let args = parse(&["--filter", "invoices"]).unwrap();
        assert_eq!(args.filter, Some("invoices".to_string()));
    }

    #[test]
    fn parses_jobs() {
        let args = parse(&["--jobs", "4"]).unwrap();
        assert_eq!(args.jobs, 4);
    }

    #[test]
    fn parses_format_human_and_json() {
        assert_eq!(parse(&["--format", "human"]).unwrap().format, Format::Human);
        assert_eq!(parse(&["--format", "json"]).unwrap().format, Format::Json);
    }

    #[test]
    fn combines_multiple_flags() {
        let args = parse(&[
            "--universes",
            "5",
            "--filter",
            "receipts",
            "--format",
            "json",
        ])
        .unwrap();
        assert_eq!(args.universes, 5);
        assert_eq!(args.filter, Some("receipts".to_string()));
        assert_eq!(args.format, Format::Json);
    }

    #[test]
    fn unknown_flag_names_the_flag_and_lists_valid_ones() {
        let err = parse(&["--bogus"]).unwrap_err();
        assert!(err.contains("--bogus"), "{err}");
        for flag in [
            "--universes",
            "--seed",
            "--master-seed",
            "--filter",
            "--jobs",
            "--format",
        ] {
            assert!(err.contains(flag), "{err} must list {flag}");
        }
    }

    #[test]
    fn flag_missing_its_value_names_the_flag() {
        let err = parse(&["--seed"]).unwrap_err();
        assert!(err.contains("--seed"), "{err}");
    }

    #[test]
    fn bad_number_names_the_flag_and_the_bad_value() {
        let err = parse(&["--universes", "abc"]).unwrap_err();
        assert!(err.contains("--universes"), "{err}");
        assert!(err.contains("abc"), "{err}");
    }

    #[test]
    fn bad_format_value_names_the_valid_options() {
        let err = parse(&["--format", "xml"]).unwrap_err();
        assert!(err.contains("human"), "{err}");
        assert!(err.contains("json"), "{err}");
        assert!(err.contains("xml"), "{err}");
    }

    // --------------------------------------------------- human failure block

    #[test]
    fn human_failure_block_matches_the_pinned_shape() {
        let config = FaultConfig {
            write_error: 0.0,
            sync_error: 0.118,
            crash_apply: 0.002,
            crash_torn: 0.664668,
            disk_latency: (Duration::ZERO, Duration::ZERO),
            net_latency: (Duration::ZERO, Duration::from_millis(250)),
        };
        let failure = Failure {
            test: "receipts_exactly_once",
            seed: 0x8bee_1752_811a_e873,
            config,
            message: "exactly-once violated: hits 2, done 1 for key 3".to_string(),
        };
        let block = human_failure_block(&failure);
        assert!(
            block.contains("sim failure in receipts_exactly_once"),
            "{block}"
        );
        assert!(block.contains("  seed:   0x8bee1752811ae873"), "{block}");
        assert!(
            block.contains(
                "  config: FaultConfig { write_error: 0.0, sync_error: 0.118, crash_apply: 0.002, crash_torn: 0.664668, disk_latency: (0ns, 0ns), net_latency: (0ns, 250ms) }"
            ),
            "{block}"
        );
        assert!(
            block.contains(
                "  repro:  charpente test --sim --filter receipts_exactly_once --seed 0x8bee1752811ae873"
            ),
            "{block}"
        );
    }

    // -------------------------------------------------------------- run()

    #[test]
    fn empty_registry_with_a_filter_matching_nothing_is_a_usage_error() {
        let args = parse(&["--filter", "nothing_matches_this"]).unwrap();
        let mut out = Vec::new();
        let code = run(&[], &args, &mut out);
        assert_eq!(code, 2);
    }

    #[test]
    fn a_filter_matching_nothing_in_a_nonempty_registry_is_also_a_usage_error() {
        let tests = [SimTest {
            name: "always_ok",
            run: always_ok,
            require: &[],
        }];
        let args = parse(&["--filter", "no_such_test"]).unwrap();
        let mut out = Vec::new();
        let code = run(&tests, &args, &mut out);
        assert_eq!(code, 2);
    }

    #[test]
    fn a_failing_toy_workload_exits_1() {
        let tests = [SimTest {
            name: "always_fails",
            run: always_fails,
            require: &[],
        }];
        let args = parse(&["--universes", "1"]).unwrap();
        let mut out = Vec::new();
        let code = run(&tests, &args, &mut out);
        assert_eq!(code, 1);
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("sim failure in always_fails"), "{text}");
        assert!(text.contains("toy failure"), "{text}");
    }

    #[test]
    fn a_passing_toy_workload_exits_0() {
        let tests = [SimTest {
            name: "always_ok",
            run: always_ok,
            require: &[],
        }];
        let args = parse(&["--universes", "2"]).unwrap();
        let mut out = Vec::new();
        let code = run(&tests, &args, &mut out);
        assert_eq!(code, 0);
    }

    #[test]
    fn a_never_covered_required_key_is_reported_and_exits_1() {
        fn covers_something_else(sim: &mut Sim) -> Result<(), String> {
            sim.cover("sometimes:happens");
            Ok(())
        }
        let tests = [SimTest {
            name: "that_test",
            run: covers_something_else,
            require: &["never:happens"],
        }];
        let args = parse(&["--universes", "3"]).unwrap();
        let mut out = Vec::new();
        let code = run(&tests, &args, &mut out);
        assert_eq!(code, 1);
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("that_test"), "{text}");
        assert!(text.contains("never:happens"), "{text}");
    }

    #[test]
    fn seed_mode_runs_once_and_skips_coverage_requirements() {
        fn never_covers(_sim: &mut Sim) -> Result<(), String> {
            Ok(())
        }
        let tests = [SimTest {
            name: "solo",
            run: never_covers,
            require: &["never:happens"],
        }];
        let args = parse(&["--seed", "0x1234"]).unwrap();
        let mut out = Vec::new();
        let code = run(&tests, &args, &mut out);
        assert_eq!(
            code, 0,
            "coverage requirements must be skipped under --seed"
        );
    }

    #[test]
    fn filter_selects_only_matching_tests() {
        let tests = [
            SimTest {
                name: "invoices_survive_crashes",
                run: always_ok,
                require: &[],
            },
            SimTest {
                name: "receipts_exactly_once",
                run: always_fails,
                require: &[],
            },
        ];
        let args = parse(&["--universes", "1", "--filter", "invoices"]).unwrap();
        let mut out = Vec::new();
        let code = run(&tests, &args, &mut out);
        assert_eq!(code, 0, "the failing test must be excluded by the filter");
    }
}
