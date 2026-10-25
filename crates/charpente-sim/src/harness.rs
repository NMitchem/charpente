//! `charpente test --sim` harness core (step-7 spec: docs/superpowers/specs/
//! 2026-07-05-sim-cli-design.md). Registers app-authored sim workloads
//! (`SimTest`), parses runner flags (`HarnessArgs`), and runs them as a
//! sequential batch, each run through `crate::run_app_universe`: the
//! internal swarm macro's seed→behavior mapping plus the clock axes, which
//! only this harness samples. Runs a batch in parallel over a `--jobs`-sized
//! thread pool and can render the report as JSON (`Report::to_json`) for
//! downstream tooling.
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
//!
//! Parallelism: all seeds for a batch are pre-derived on the main thread, in
//! the exact registry order above, before any work is distributed — a pool
//! of `args.jobs` workers then race over an index counter, so `--jobs 1` and
//! `--jobs 8` consume the identical seed sequence and produce byte-identical
//! `Report`s (worker scheduling is unobservable; only universe index orders
//! the output).

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::rng::splitmix64;
use crate::{FaultConfig, Sim, UniverseEnd, UniverseFailure, ambient_base_seed, run_app_universe};

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

        if universes == 0 {
            return Err(
                "--universes must be at least 1 (0 universes is not a trustworthy signal — a \
                 green run of zero universes proves nothing)"
                    .to_string(),
            );
        }
        if seed.is_some() && master_seed.is_some() {
            return Err(
                "--seed and --master-seed cannot be combined — --seed replays exactly one \
                 literal universe, --master-seed only picks where a batch's seed chain starts"
                    .to_string(),
            );
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
    /// What actually fired in THIS universe. `config` is the enabled axes and
    /// their probabilities; this is the events. Kept per-failure rather than
    /// folded into `Report::coverage` (which stays the batch statistic that
    /// `require:`/floors are measured against, unchanged) — the question a
    /// failure raises is "which axis explains THIS universe", and the batch
    /// aggregate cannot answer it.
    pub coverage: BTreeMap<&'static str, u64>,
    pub message: String,
}

/// How one test's universes ended in this batch. A give-up is a universe the
/// injected faults stopped from adjudicating anything — reported, never
/// folded into the pass count (that fold is what this type exists to undo).
#[derive(Debug, PartialEq, Eq)]
pub struct TestOutcomes {
    pub test: &'static str,
    pub verdicts: u64,
    pub gave_up: u64,
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
    /// Per test, in registry order: how many universes reached a verdict and
    /// how many gave up. MANIFESTO §9 rests on being able to state this.
    pub outcomes: Vec<TestOutcomes>,
}

impl Report {
    pub fn verdicts(&self) -> u64 {
        self.outcomes.iter().map(|o| o.verdicts).sum()
    }

    pub fn gave_up(&self) -> u64 {
        self.outcomes.iter().map(|o| o.gave_up).sum()
    }
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
            args.jobs,
        ),
    };

    match args.format {
        Format::Human => write_human(&report, out),
        Format::Json => {
            let _ = writeln!(out, "{}", report.to_json());
        }
    }
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
        outcomes: Vec::new(),
    };
    for test in tests {
        let mut outcomes = TestOutcomes {
            test: test.name,
            verdicts: 0,
            gave_up: 0,
        };
        match run_app_universe(seed, None, &[], &test.run) {
            Ok(end) => {
                tally(&mut outcomes, &end);
                merge(&mut report.coverage, &end.coverage);
            }
            Err(UniverseFailure {
                config,
                coverage,
                message,
                ..
            }) => report.failures.push(Failure {
                test: test.name,
                seed,
                config,
                coverage,
                message,
            }),
        }
        report.outcomes.push(outcomes);
    }
    report
}

/// A universe that failed is neither: it never lands here (the failure branch
/// files a `Failure` instead), so verdicts + gave_up + failures = universes.
fn tally(outcomes: &mut TestOutcomes, end: &UniverseEnd) {
    if end.outcome.gave_up() {
        outcomes.gave_up += 1;
    } else {
        outcomes.verdicts += 1;
    }
}

/// One (test, universe) unit of work: the seed is pre-derived on the main
/// thread before any worker starts, so `jobs` only changes scheduling, never
/// the seed each universe runs with.
type UniverseOutcome = Result<UniverseEnd, UniverseFailure>;

fn run_batch(tests: &[&SimTest], master_seed: u64, universes: u64, jobs: usize) -> Report {
    // DESIGN §7: the master seed is printed at startup, before any universe
    // runs — a failing run from OS entropy must still reveal the seed that
    // would replay it. stderr, so `--format json`'s stdout stays pure JSON.
    eprintln!("sim: {universes} universe(s) per test from master seed {master_seed:#018x}");

    let mut report = Report {
        master_seed,
        universes,
        failures: Vec::new(),
        coverage: BTreeMap::new(),
        missing_coverage: Vec::new(),
        outcomes: Vec::new(),
    };

    // Pre-derive every universe's seed on the main thread, in registry
    // order, before any work is distributed — this is the entire reason
    // `--jobs 1` and `--jobs 8` produce byte-identical reports.
    let mut state = master_seed;
    let mut work: Vec<(usize, u64)> = Vec::with_capacity(tests.len() * universes as usize);
    for test_index in 0..tests.len() {
        for _ in 0..universes {
            work.push((test_index, splitmix64(&mut state)));
        }
    }

    let next = AtomicUsize::new(0);
    let slots: Mutex<Vec<Option<UniverseOutcome>>> =
        Mutex::new((0..work.len()).map(|_| None).collect());
    let jobs = jobs.max(1);

    // The one sanctioned thread-spawning site in the workspace besides prod
    // impls: the harness orchestrates universes from OUTSIDE the deterministic
    // boundary — each universe inside a worker is a sealed, single-threaded,
    // seed-determined world, and reports are ordered by universe index so the
    // pool's scheduling is unobservable.
    #[allow(clippy::disallowed_methods)]
    std::thread::scope(|scope| {
        for _ in 0..jobs {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    if index >= work.len() {
                        return;
                    }
                    let (test_index, seed) = work[index];
                    let outcome = run_app_universe(seed, None, &[], &tests[test_index].run);
                    slots.lock().unwrap()[index] = Some(outcome);
                }
            });
        }
    });
    let mut slots = slots.into_inner().unwrap();

    let mut index = 0;
    for test in tests {
        let mut covered: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut outcomes = TestOutcomes {
            test: test.name,
            verdicts: 0,
            gave_up: 0,
        };
        for _ in 0..universes {
            let (_, seed) = work[index];
            let outcome = slots[index]
                .take()
                .expect("every work item is filled before the pool returns");
            match outcome {
                Ok(end) => {
                    tally(&mut outcomes, &end);
                    merge(&mut covered, &end.coverage);
                }
                Err(UniverseFailure {
                    config,
                    coverage,
                    message,
                    ..
                }) => report.failures.push(Failure {
                    test: test.name,
                    seed,
                    config,
                    coverage,
                    message,
                }),
            }
            index += 1;
        }
        report.outcomes.push(outcomes);
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

impl Report {
    /// Hand-assembled JSON (no dependency): field order is a pinned contract
    /// downstream tooling parses (spec: docs/superpowers/specs/
    /// 2026-07-05-sim-cli-design.md, "Failure output"). Seeds are always hex
    /// strings, never JSON numbers — a `u64` seed routinely exceeds 2^53,
    /// the largest integer a JSON-consuming float type can represent
    /// exactly. Each failure record carries its own `master_seed` (the spec
    /// requires a record to be self-contained once extracted from the
    /// document); `summary` is the trailing field, exactly `{master_seed,
    /// universes, failures, coverage}` per spec.
    pub fn to_json(&self) -> String {
        let mut out = String::new();
        out.push('{');

        out.push_str("\"master_seed\":\"");
        out.push_str(&format!("{:#x}", self.master_seed));
        out.push_str("\",\"universes\":");
        out.push_str(&self.universes.to_string());

        out.push_str(",\"failures\":[");
        for (i, f) in self.failures.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str("{\"test\":\"");
            json_escape(f.test, &mut out);
            out.push_str("\",\"seed\":\"");
            out.push_str(&format!("{:#x}", f.seed));
            out.push_str("\",\"master_seed\":\"");
            out.push_str(&format!("{:#x}", self.master_seed));
            out.push_str("\",\"config\":");
            config_json(&f.config, &mut out);
            out.push_str(",\"coverage\":");
            coverage_json(&f.coverage, &mut out);
            out.push_str(",\"message\":\"");
            json_escape(&f.message, &mut out);
            out.push_str("\",\"repro\":\"");
            json_escape(
                &format!(
                    "charpente test --sim --filter {} --seed {:#x}",
                    f.test, f.seed
                ),
                &mut out,
            );
            out.push_str("\"}");
        }
        out.push(']');

        out.push_str(",\"verdicts\":");
        out.push_str(&self.verdicts().to_string());
        out.push_str(",\"gave_up\":");
        out.push_str(&self.gave_up().to_string());

        out.push_str(",\"outcomes\":[");
        for (i, o) in self.outcomes.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str("{\"test\":\"");
            json_escape(o.test, &mut out);
            out.push_str(&format!(
                "\",\"verdicts\":{},\"gave_up\":{}}}",
                o.verdicts, o.gave_up
            ));
        }
        out.push(']');

        out.push_str(",\"coverage\":");
        coverage_json(&self.coverage, &mut out);

        out.push_str(",\"missing_coverage\":[");
        for (i, (test, key)) in self.missing_coverage.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str("{\"test\":\"");
            json_escape(test, &mut out);
            out.push_str("\",\"key\":\"");
            json_escape(key, &mut out);
            out.push_str("\"}");
        }
        out.push(']');

        out.push_str(&format!(
            ",\"summary\":{{\"master_seed\":\"{:#x}\",\"universes\":{},\"failures\":{},\"coverage\":",
            self.master_seed,
            self.universes,
            self.failures.len(),
        ));
        coverage_json(&self.coverage, &mut out);
        out.push('}');

        out.push('}');
        out
    }
}

fn coverage_json(coverage: &BTreeMap<&'static str, u64>, out: &mut String) {
    out.push('{');
    for (i, (key, count)) in coverage.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        json_escape(key, out);
        out.push_str("\":");
        out.push_str(&count.to_string());
    }
    out.push('}');
}

/// Every axis of the config, not a subset. The human format prints
/// `{cfg:?}` — all of it — while this listed four probabilities and the two
/// latencies, so the MACHINE-readable record silently omitted
/// `dir_entry_loss`, `bit_flip`, `misdirected_write`, `read_error`, the four
/// `object_*`, `corruption_scope`, `conn_drop`, `net_spike` and
/// `subscriber_stall`. A consumer diffing configs across runs could not see
/// the axis that explained the failure (docs/LEDGER.md 2026-09-07). Adding a
/// field to `FaultConfig` without adding it here reopens that gap:
/// `config_json_carries_every_fault_axis` fails the moment it does.
fn config_json(cfg: &FaultConfig, out: &mut String) {
    out.push_str(&format!(
        "{{\"write_error\":{},\"sync_error\":{},\"crash_apply\":{},\"crash_torn\":{},\
         \"dir_entry_loss\":{},\"bit_flip\":{},\"misdirected_write\":{},\"read_error\":{},\
         \"object_put_error\":{},\"object_get_error\":{},\"object_lost_ack\":{},\
         \"object_delete_error\":{},\"corruption_scope\":\"",
        cfg.write_error,
        cfg.sync_error,
        cfg.crash_apply,
        cfg.crash_torn,
        cfg.dir_entry_loss,
        cfg.bit_flip,
        cfg.misdirected_write,
        cfg.read_error,
        cfg.object_put_error,
        cfg.object_get_error,
        cfg.object_lost_ack,
        cfg.object_delete_error,
    ));
    json_escape(&cfg.corruption_scope, out);
    out.push_str(&format!(
        "\",\"conn_drop\":{},\"net_spike\":{},\"subscriber_stall\":{},\
         \"disk_latency\":{{\"lo_ns\":{},\"hi_ns\":{}}},\"net_latency\":{{\"lo_ns\":{},\"hi_ns\":{}}},\"outbound_refused\":{},\"outbound_lost_request\":{},\"outbound_lost_reply\":{},\"outbound_throttle\":{},\"outbound_slow\":{},\"outbound_latency\":{{\"lo_ns\":{},\"hi_ns\":{}}}",
        cfg.conn_drop,
        cfg.net_spike,
        cfg.subscriber_stall,
        cfg.disk_latency.0.as_nanos(),
        cfg.disk_latency.1.as_nanos(),
        cfg.net_latency.0.as_nanos(),
        cfg.net_latency.1.as_nanos(),
        cfg.outbound_refused,
        cfg.outbound_lost_request,
        cfg.outbound_lost_reply,
        cfg.outbound_throttle,
        cfg.outbound_slow,
        cfg.outbound_latency.0.as_nanos(),
        cfg.outbound_latency.1.as_nanos(),
    ));
    out.push_str(&format!(
        ",\"clock_skew\":{},\"clock_step_back\":{{\"chance\":{},\"max_ns\":{}}},\
         \"clock_downtime\":{{\"chance\":{},\"max_ns\":{}}}}}",
        cfg.clock_skew,
        cfg.clock_step_back.0,
        cfg.clock_step_back.1.as_nanos(),
        cfg.clock_downtime.0,
        cfg.clock_downtime.1.as_nanos(),
    ));
}

/// Escapes `"`, `\`, and control characters for a JSON string body (the
/// caller supplies the surrounding quotes) — no `serde_json` dependency for
/// output this small.
fn json_escape(s: &str, out: &mut String) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
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
    // Printed on green AND red runs, and printed unconditionally: a batch that
    // adjudicated nothing must never read as an unqualified success, and "0
    // gave up" is the claim being made rather than noise (MANIFESTO §9).
    for o in &report.outcomes {
        if o.gave_up > 0 {
            let _ = writeln!(
                out,
                "sim: {} of {} universes of \"{}\" gave up — reached no verdict",
                o.gave_up, report.universes, o.test
            );
        }
        // `gave_up == universes`, not `verdicts == 0`: a batch where some
        // universes FAILED and the rest gave up has zero verdicts too, and
        // calling that "every universe gave up" would misreport a red run.
        if report.universes > 0 && o.gave_up == report.universes {
            let _ = writeln!(
                out,
                "sim: every universe of \"{}\" gave up — it adjudicated nothing over {} \
                 universes; the property is vacuous until the faults it cannot survive are \
                 narrowed",
                o.test, report.universes
            );
        }
    }
    let status = match (report.failures.len(), report.missing_coverage.len()) {
        (0, 0) => "no failures".to_string(),
        (0, unmet) => format!("no failures, {unmet} unmet coverage requirement(s)"),
        (failures, 0) => format!("{failures} failure(s)"),
        (failures, unmet) => {
            format!("{failures} failure(s), {unmet} unmet coverage requirement(s)")
        }
    };
    let _ = writeln!(
        out,
        "sim: {} universe(s) per test, {} verdict(s), {} gave up, {status} (master seed: {:#018x})",
        report.universes,
        report.verdicts(),
        report.gave_up(),
        report.master_seed
    );
}

fn human_failure_block(f: &Failure) -> String {
    format!(
        "\nsim failure in {}: {}\n  seed:   {:#018x}\n  config: {:?}\n  fired:  {}\n  repro:  charpente test --sim --filter {} --seed {:#x}\n",
        f.test,
        f.message,
        f.seed,
        f.config,
        crate::render_fired(&f.coverage),
        f.test,
        f.seed
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

    fn always_gives_up(sim: &mut Sim) -> Result<(), String> {
        sim.give_up("toy give-up", "");
        Ok(())
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
            dir_entry_loss: 0.0,
            bit_flip: 0.0,
            misdirected_write: 0.0,
            read_error: 0.0,
            object_put_error: 0.0,
            object_get_error: 0.0,
            object_lost_ack: 0.0,
            object_delete_error: 0.0,
            corruption_scope: "wal/".to_string(),
            conn_drop: 0.0,
            net_spike: 0.0,
            subscriber_stall: 0.0,
            outbound_refused: 0.0,
            outbound_lost_request: 0.0,
            outbound_lost_reply: 0.0,
            outbound_throttle: 0.0,
            outbound_slow: 0.0,
            outbound_latency: (Duration::ZERO, Duration::ZERO),
            disk_latency: (Duration::ZERO, Duration::ZERO),
            net_latency: (Duration::ZERO, Duration::from_millis(250)),
            clock_skew: 41_207_118_532,
            clock_step_back: (0.0, Duration::ZERO),
            clock_downtime: (0.5, Duration::from_secs(3 * 86_400)),
        };
        let failure = Failure {
            test: "receipts_exactly_once",
            seed: 0x8bee_1752_811a_e873,
            config,
            coverage: BTreeMap::from([("fault:dir-entry", 3), ("restart", 7)]),
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
                "  config: FaultConfig { write_error: 0.0, sync_error: 0.118, crash_apply: 0.002, crash_torn: 0.664668, dir_entry_loss: 0.0, bit_flip: 0.0, misdirected_write: 0.0, read_error: 0.0, object_put_error: 0.0, object_get_error: 0.0, object_lost_ack: 0.0, object_delete_error: 0.0, corruption_scope: \"wal/\", conn_drop: 0.0, net_spike: 0.0, subscriber_stall: 0.0, outbound_refused: 0.0, outbound_lost_request: 0.0, outbound_lost_reply: 0.0, outbound_throttle: 0.0, outbound_slow: 0.0, outbound_latency: (0ns, 0ns), disk_latency: (0ns, 0ns), net_latency: (0ns, 250ms), clock_skew: 41207118532, clock_step_back: (0.0, 0ns), clock_downtime: (0.5, 259200s) }"
            ),
            "{block}"
        );
        assert!(
            block.contains("  fired:  fault:dir-entry: 3, restart: 7"),
            "{block}"
        );
        assert!(
            block.contains(
                "  repro:  charpente test --sim --filter receipts_exactly_once --seed 0x8bee1752811ae873"
            ),
            "{block}"
        );
        // The GUIDE calls this five load-bearing lines; if that stops being
        // true the doc is wrong (hard rule 6, docs/GUIDE.md "Reading a
        // failure, replaying its seed").
        assert_eq!(
            block.trim_matches('\n').lines().count(),
            5,
            "the GUIDE documents five lines: {block}"
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

    // ------------------------------------------- what actually fired

    /// The oracle for the whole change: a FAILING universe must report the
    /// faults that fired in it. This was structurally impossible until
    /// 2026-09-07 — `run_universe`'s `Err` payload was `(FaultConfig, String)`,
    /// with nowhere to put coverage — so on the one run where a reader most
    /// needs to know which axis explains the failure, the report carried none
    /// at all, while a GREEN run of the same workload reported it fine. The
    /// harness's runs now fail with a named `UniverseFailure`, whose
    /// `coverage` carries it.
    ///
    /// Asserts through the public `Report`, not the renderer, so it fails if
    /// the payload is ever narrowed again regardless of how the text is
    /// formatted.
    #[test]
    fn a_failing_universe_reports_the_coverage_it_marked() {
        fn restarts_then_fails(sim: &mut Sim) -> Result<(), String> {
            sim.restart();
            Err("toy failure after a restart".into())
        }
        let tests = [SimTest {
            name: "solo",
            run: restarts_then_fails,
            require: &[],
        }];
        let refs: Vec<&SimTest> = tests.iter().collect();
        let report = run_single_seed(&refs, 0x1234);
        assert_eq!(report.failures.len(), 1, "{:?}", report.failures);
        assert_eq!(
            report.failures[0].coverage.get("restart").copied(),
            Some(1),
            "a failing universe must carry the coverage it marked, got {:?}",
            report.failures[0].coverage
        );
    }

    /// A failure with NO faults fired must say so in words rather than by
    /// printing an empty list — "nothing fired" is the most informative thing
    /// a report can say (it means the bug is not fault-induced), and it is
    /// exactly the reading that an absent line leaves a reader to guess at.
    #[test]
    fn a_failure_with_no_faults_says_nothing_fired() {
        let tests = [SimTest {
            name: "solo",
            run: always_fails,
            require: &[],
        }];
        let refs: Vec<&SimTest> = tests.iter().collect();
        let report = run_single_seed(&refs, 0x1234);
        let block = human_failure_block(&report.failures[0]);
        assert!(
            block.contains("fired:  nothing (no faults or events marked coverage"),
            "{block}"
        );
    }

    /// `config_json` listed four probabilities and two latencies, so the
    /// machine-readable record omitted the axis a diagnosis actually turned on.
    /// Pinned against `{cfg:?}` — the human format, which has always printed
    /// every field — so adding a `FaultConfig` field without adding it here
    /// reddens this test instead of silently reopening the hole.
    #[test]
    fn config_json_carries_every_fault_axis() {
        let cfg = FaultConfig {
            write_error: 0.068,
            sync_error: 0.049,
            crash_apply: 0.354,
            crash_torn: 0.507_756,
            dir_entry_loss: 0.477,
            bit_flip: 0.0,
            misdirected_write: 0.0,
            read_error: 0.0,
            object_put_error: 0.0,
            object_get_error: 0.0,
            object_lost_ack: 0.0,
            object_delete_error: 0.0,
            corruption_scope: "wal/".to_string(),
            conn_drop: 0.02,
            net_spike: 0.081,
            subscriber_stall: 0.0,
            outbound_refused: 0.0,
            outbound_lost_request: 0.0,
            outbound_lost_reply: 0.0,
            outbound_throttle: 0.0,
            outbound_slow: 0.0,
            outbound_latency: (Duration::ZERO, Duration::ZERO),
            disk_latency: (Duration::ZERO, Duration::ZERO),
            net_latency: (Duration::ZERO, Duration::from_millis(20)),
            clock_skew: -73_000_000_000,
            clock_step_back: (0.012, Duration::from_secs(120)),
            clock_downtime: (0.5, Duration::from_secs(3 * 86_400)),
        };
        let mut json = String::new();
        config_json(&cfg, &mut json);
        // Every field name the Debug rendering shows must appear as a JSON key.
        for field in format!("{cfg:?}")
            .split(&['{', '}', ',', ' '][..])
            .filter(|t| t.ends_with(':'))
            .map(|t| t.trim_end_matches(':'))
        {
            assert!(
                json.contains(&format!("\"{field}\":")),
                "config_json omits `{field}`, which the human format prints: {json}"
            );
        }
        assert!(json.contains("\"dir_entry_loss\":0.477"), "{json}");
    }

    #[test]
    fn json_failure_record_carries_the_coverage_that_fired() {
        fn restarts_then_fails(sim: &mut Sim) -> Result<(), String> {
            sim.restart();
            Err("toy failure after a restart".into())
        }
        let tests = [SimTest {
            name: "solo",
            run: restarts_then_fails,
            require: &[],
        }];
        let refs: Vec<&SimTest> = tests.iter().collect();
        let json = run_single_seed(&refs, 0x1234).to_json();
        assert!(json.contains("\"coverage\":{\"restart\":1}"), "{json}");
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

    // --------------------------------------------------- parse hardening

    #[test]
    fn universes_zero_is_a_usage_error() {
        let err = parse(&["--universes", "0"]).unwrap_err();
        assert!(err.contains("--universes"), "{err}");
        assert!(err.contains('0'), "{err}");
    }

    #[test]
    fn seed_and_master_seed_together_is_a_usage_error_naming_both_flags() {
        let err = parse(&["--seed", "0x1", "--master-seed", "0x2"]).unwrap_err();
        assert!(err.contains("--seed"), "{err}");
        assert!(err.contains("--master-seed"), "{err}");
    }

    #[test]
    fn non_hex_seed_value_reports_the_expected_message() {
        let err = parse(&["--seed", "zzz"]).unwrap_err();
        assert_eq!(
            err,
            "--seed expects a hex value like 0xdeadbeef, got \"zzz\""
        );
    }

    // --------------------------------------------- parallel == sequential

    fn sometimes_fails_from_entropy(sim: &mut Sim) -> Result<(), String> {
        use charpente_core::io::Entropy;
        let mut b = [0u8; 1];
        sim.entropy().fill(&mut b);
        if b[0] % 5 == 0 {
            Err(format!("flaky: entropy byte {} was divisible by 5", b[0]))
        } else {
            Ok(())
        }
    }

    #[test]
    fn parallel_and_sequential_batches_produce_identical_reports() {
        let test = SimTest {
            name: "flaky",
            run: sometimes_fails_from_entropy,
            require: &[],
        };
        let master_seed = 0xf00d_beef_u64;

        let sequential = run_batch(&[&test], master_seed, 64, 1);
        let parallel = run_batch(&[&test], master_seed, 64, 8);

        assert!(
            !sequential.failures.is_empty(),
            "the flaky workload must trigger at least one failure over 64 universes for this \
             test to exercise failure ordering"
        );
        assert_eq!(sequential.master_seed, parallel.master_seed);
        assert_eq!(sequential.universes, parallel.universes);
        assert_eq!(sequential.coverage, parallel.coverage);
        assert_eq!(sequential.missing_coverage, parallel.missing_coverage);
        assert_eq!(sequential.outcomes, parallel.outcomes);
        assert_eq!(
            sequential.failures.len(),
            parallel.failures.len(),
            "sequential: {:?}\nparallel: {:?}",
            sequential.failures,
            parallel.failures
        );
        for (a, b) in sequential.failures.iter().zip(parallel.failures.iter()) {
            assert_eq!(a.test, b.test);
            assert_eq!(
                a.seed, b.seed,
                "failures must be ordered by universe index, not completion order"
            );
            assert_eq!(a.message, b.message);
            assert_eq!(format!("{:?}", a.config), format!("{:?}", b.config));
        }
    }

    // ------------------------------------------------- coverage (parallel)

    #[test]
    fn missing_coverage_pairs_test_name_with_key_exactly_under_a_parallel_run() {
        fn covers_something_else(sim: &mut Sim) -> Result<(), String> {
            sim.cover("sometimes:happens");
            Ok(())
        }
        let test = SimTest {
            name: "that_test",
            run: covers_something_else,
            require: &["never:happens"],
        };
        let report = run_batch(&[&test], 0x1234, 8, 4);
        assert_eq!(
            report.missing_coverage,
            vec![("that_test".to_string(), "never:happens".to_string())]
        );
    }

    #[test]
    fn coverage_requirement_guidance_text_survives_a_parallel_run() {
        fn covers_something_else(sim: &mut Sim) -> Result<(), String> {
            sim.cover("sometimes:happens");
            Ok(())
        }
        let tests = [SimTest {
            name: "that_test",
            run: covers_something_else,
            require: &["never:happens"],
        }];
        let args = parse(&["--universes", "8", "--jobs", "4"]).unwrap();
        let mut out = Vec::new();
        let code = run(&tests, &args, &mut out);
        assert_eq!(code, 1);
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("that_test"), "{text}");
        assert!(text.contains("never:happens"), "{text}");
        assert!(
            text.contains("raise the universe count, adjust the workload, or drop the requirement"),
            "{text}"
        );
    }

    // ---------------------------------------------------------- outcomes

    /// The collapse this whole type exists to undo: a workload that gives up
    /// returns `Ok(())` exactly like one that passes, so nothing about the
    /// returned `Result` distinguishes them. The outcome comes off the `Sim`.
    #[test]
    fn a_give_up_is_counted_apart_from_a_verdict() {
        let gives_up = SimTest {
            name: "gives_up",
            run: always_gives_up,
            require: &[],
        };
        let passes = SimTest {
            name: "passes",
            run: always_ok,
            require: &[],
        };
        let report = run_batch(&[&gives_up, &passes], 0x1234, 4, 2);
        assert_eq!(
            report.outcomes,
            vec![
                TestOutcomes {
                    test: "gives_up",
                    verdicts: 0,
                    gave_up: 4,
                },
                TestOutcomes {
                    test: "passes",
                    verdicts: 4,
                    gave_up: 0,
                },
            ]
        );
        assert_eq!(report.verdicts(), 4);
        assert_eq!(report.gave_up(), 4);
        assert!(report.failures.is_empty(), "giving up is not a failure");
    }

    /// A workload that gives up and then fails is a failure: failures outrank
    /// give-ups, so this change can never turn a red universe green.
    #[test]
    fn a_failure_after_a_give_up_is_still_a_failure() {
        fn gives_up_then_fails(sim: &mut Sim) -> Result<(), String> {
            sim.give_up("toy give-up", "");
            Err("toy failure".into())
        }
        let test = SimTest {
            name: "both",
            run: gives_up_then_fails,
            require: &[],
        };
        let report = run_batch(&[&test], 0x1234, 3, 1);
        assert_eq!(report.failures.len(), 3);
        assert_eq!(report.verdicts(), 0);
        assert_eq!(report.gave_up(), 0, "a failed universe is neither");
    }

    #[test]
    fn a_batch_that_adjudicated_nothing_does_not_print_as_green() {
        let tests = [SimTest {
            name: "gives_up",
            run: always_gives_up,
            require: &[],
        }];
        let args = parse(&["--universes", "4"]).unwrap();
        let mut out = Vec::new();
        let code = run(&tests, &args, &mut out);
        assert_eq!(code, 0, "giving up is not a failure");
        let text = String::from_utf8(out).unwrap();
        assert!(!text.contains("all green"), "{text}");
        assert!(text.contains("0 verdict(s), 4 gave up"), "{text}");
        assert!(
            text.contains("every universe of \"gives_up\" gave up"),
            "{text}"
        );
        assert!(text.contains("adjudicated nothing"), "{text}");
    }

    /// The vacuity line is about universes that ADJUDICATED NOTHING, not about
    /// a zero verdict count — a batch whose universes half failed and half gave
    /// up also has zero verdicts, and calling that "every universe gave up"
    /// would describe a red run as a vacuous green one.
    #[test]
    fn a_batch_that_failed_is_not_reported_as_every_universe_gave_up() {
        fn gives_up_or_fails(sim: &mut Sim) -> Result<(), String> {
            use charpente_core::io::Entropy;
            let mut b = [0u8; 1];
            sim.entropy().fill(&mut b);
            if b[0] % 2 == 0 {
                sim.give_up("toy give-up", "");
                Ok(())
            } else {
                Err("toy failure".into())
            }
        }
        let tests = [SimTest {
            name: "mixed",
            run: gives_up_or_fails,
            require: &[],
        }];
        let args = parse(&["--universes", "16", "--master-seed", "0xf00d"]).unwrap();
        let mut out = Vec::new();
        assert_eq!(run(&tests, &args, &mut out), 1);
        let text = String::from_utf8(out).unwrap();
        assert!(
            text.contains("0 verdict(s)"),
            "no universe adjudicated: {text}"
        );
        // Non-vacuity: this batch really does mix failures with give-ups, so
        // the absence of the vacuity line below is a decision, not an accident.
        assert!(
            text.contains("universes of \"mixed\" gave up"),
            "some universes must have given up for this test to mean anything: {text}"
        );
        assert!(
            !text.contains("every universe of"),
            "some universes FAILED — they did not give up: {text}"
        );
        assert!(text.contains("failure(s)"), "{text}");
    }

    /// A run red only on coverage used to end "…, 0 failure(s)", which reads as
    /// green to anyone skimming the last line.
    #[test]
    fn a_coverage_only_red_says_what_is_actually_unmet() {
        fn covers_something_else(sim: &mut Sim) -> Result<(), String> {
            sim.cover("sometimes:happens");
            Ok(())
        }
        let tests = [SimTest {
            name: "that_test",
            run: covers_something_else,
            require: &["never:happens"],
        }];
        let args = parse(&["--universes", "2"]).unwrap();
        let mut out = Vec::new();
        assert_eq!(run(&tests, &args, &mut out), 1);
        let text = String::from_utf8(out).unwrap();
        assert!(
            text.contains("no failures, 1 unmet coverage requirement(s)"),
            "{text}"
        );
        assert!(!text.contains("0 failure(s)"), "{text}");
    }

    #[test]
    fn a_fully_green_batch_still_states_its_verdict_count() {
        let tests = [SimTest {
            name: "always_ok",
            run: always_ok,
            require: &[],
        }];
        let args = parse(&["--universes", "5"]).unwrap();
        let mut out = Vec::new();
        assert_eq!(run(&tests, &args, &mut out), 0);
        let text = String::from_utf8(out).unwrap();
        assert!(
            text.contains("5 verdict(s), 0 gave up, no failures"),
            "\"0 gave up\" is the claim being made, not noise: {text}"
        );
    }

    #[test]
    fn give_up_marks_its_own_coverage_key() {
        let test = SimTest {
            name: "gives_up",
            run: always_gives_up,
            require: &[],
        };
        let report = run_batch(&[&test], 0x1234, 6, 1);
        assert_eq!(report.coverage.get("universe:gave-up").copied(), Some(6));
    }

    // -------------------------------------------------------------- JSON

    #[test]
    fn json_master_seed_and_failure_seed_are_hex_strings_not_numbers() {
        let tests = [SimTest {
            name: "always_fails",
            run: always_fails,
            require: &[],
        }];
        let args = parse(&[
            "--universes",
            "1",
            "--master-seed",
            "0xabc123",
            "--format",
            "json",
        ])
        .unwrap();
        let mut out = Vec::new();
        let code = run(&tests, &args, &mut out);
        assert_eq!(code, 1);
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("\"master_seed\":\"0x"), "{text}");
        assert!(
            text.contains("\"test\":\"always_fails\",\"seed\":\"0x"),
            "{text}"
        );
        // Every failure record is self-contained: it carries its own
        // master_seed (spec field order test, seed, master_seed, config), on
        // top of the document's top-level and summary master_seed fields —
        // three occurrences total for this one-failure batch.
        let occurrences = text.matches("\"master_seed\":\"0xabc123\"").count();
        assert_eq!(
            occurrences, 3,
            "expected the top-level, per-failure, and summary master_seed: {text}"
        );
        let obj_start = text.find("\"test\":\"always_fails\"").unwrap();
        let seed_pos = text[obj_start..]
            .find("\"seed\":\"0x")
            .map(|p| p + obj_start)
            .unwrap();
        let master_seed_pos = text[seed_pos..]
            .find("\"master_seed\":\"0xabc123\"")
            .map(|p| p + seed_pos)
            .unwrap();
        let config_pos = text[master_seed_pos..]
            .find("\"config\":{")
            .map(|p| p + master_seed_pos)
            .unwrap();
        assert!(
            obj_start < seed_pos && seed_pos < master_seed_pos && master_seed_pos < config_pos,
            "failure record field order must be test, seed, master_seed, config: {text}"
        );
    }

    #[test]
    fn json_failure_object_has_config_message_and_repro_fields() {
        let tests = [SimTest {
            name: "always_fails",
            run: always_fails,
            require: &[],
        }];
        let args = parse(&["--universes", "1", "--format", "json"]).unwrap();
        let mut out = Vec::new();
        let code = run(&tests, &args, &mut out);
        assert_eq!(code, 1);
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("\"config\":{"), "{text}");
        assert!(text.contains("\"message\":\"toy failure\""), "{text}");
        assert!(
            text.contains("\"repro\":\"charpente test --sim --filter always_fails --seed 0x"),
            "{text}"
        );
    }

    #[test]
    fn json_summary_matches_the_spec_shape() {
        let tests = [SimTest {
            name: "always_fails",
            run: always_fails,
            require: &[],
        }];
        let args = parse(&[
            "--universes",
            "1",
            "--master-seed",
            "0xabc123",
            "--format",
            "json",
        ])
        .unwrap();
        let mut out = Vec::new();
        let code = run(&tests, &args, &mut out);
        assert_eq!(code, 1);
        let text = String::from_utf8(out).unwrap();
        let trimmed = text.trim_end();
        // The spec pins the trailing summary object's field set exactly:
        // {master_seed, universes, failures, coverage} (docs/superpowers/
        // specs/2026-07-05-sim-cli-design.md, "Failure output").
        assert!(
            trimmed.ends_with(
                "\"summary\":{\"master_seed\":\"0xabc123\",\"universes\":1,\"failures\":1,\"coverage\":{}}}"
            ),
            "{trimmed}"
        );
    }
}
