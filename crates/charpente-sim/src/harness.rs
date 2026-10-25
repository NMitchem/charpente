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
//! universe can't be expected to hit everything a batch would). `--inputs`
//! and `--faults-off` replay that run with given input choices and with fault
//! axes off, which is how a shrunk failure's repro line replays it.
//!
//! Shrinking: after a batch, each check's first failure in seed order is
//! shrunk (`shrink`) by replaying smaller candidates under a budget counted in
//! replays, and its block reports the smallest run that still fails the check.
//!
//! Parallelism: all seeds for a batch are pre-derived on the main thread, in
//! the exact registry order above, before any work is distributed — a pool
//! of `args.jobs` workers then race over an index counter, so `--jobs 1` and
//! `--jobs 8` consume the identical seed sequence and produce byte-identical
//! `Report`s (worker scheduling is unobservable; only universe index orders
//! the output).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::rng::splitmix64;
use crate::{
    FAULT_AXES, FaultConfig, Sim, UniverseEnd, UniverseFailure, ambient_base_seed, axis_on,
    run_app_universe,
};

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
    /// `--inputs`: the choice sequence a `--seed` run replays (then 0s).
    pub inputs: Option<Vec<u64>>,
    /// `--faults-off`: the `FaultConfig` axes a `--seed` run turns off, in
    /// field order.
    pub faults_off: Vec<&'static str>,
}

const VALID_FLAGS: &str =
    "--universes, --seed, --master-seed, --filter, --jobs, --format, --inputs, --faults-off";

impl HarnessArgs {
    pub fn parse(mut args: impl Iterator<Item = String>) -> Result<HarnessArgs, String> {
        let mut universes = 256u64;
        let mut seed = None;
        let mut master_seed = None;
        let mut filter = None;
        let mut jobs = default_jobs();
        let mut format = Format::Human;
        let mut inputs = None;
        let mut faults_off: Vec<&'static str> = Vec::new();

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
                "--inputs" => {
                    let value = next_value(&mut args, &flag)?;
                    inputs = Some(decode_inputs(&value).ok_or_else(|| {
                        format!(
                            "--inputs: \"{value}\" is not a choice sequence (lowercase hex of \
                             LEB128 numbers, as a failure's repro line prints it)"
                        )
                    })?);
                }
                "--faults-off" => {
                    for name in next_value(&mut args, &flag)?.split(',') {
                        let Some(&axis) = FAULT_AXES.iter().find(|a| **a == name) else {
                            // A field, but a scope rather than an axis, so the
                            // binding refusal's "not a FaultConfig field" would
                            // be false for it.
                            return Err(if name == "corruption_scope" {
                                "--faults-off: \"corruption_scope\" is not a fault axis: it \
                                 scopes bit_flip and misdirected_write, so turn those off instead"
                                    .to_string()
                            } else {
                                format!("--faults-off: \"{name}\" is not a FaultConfig field")
                            });
                        };
                        if !faults_off.contains(&axis) {
                            faults_off.push(axis);
                        }
                    }
                    faults_off.sort_by_key(|a| FAULT_AXES.iter().position(|f| f == a));
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
        if inputs.is_some() && seed.is_none() {
            return Err("--inputs needs --seed: it replays one run's inputs".to_string());
        }
        if !faults_off.is_empty() && seed.is_none() {
            return Err(
                "--faults-off needs --seed: it replays one run with those faults off".to_string(),
            );
        }

        Ok(HarnessArgs {
            universes,
            seed,
            master_seed,
            filter,
            jobs,
            format,
            inputs,
            faults_off,
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
    /// The inputs this run drew, by name, in draw order.
    pub drawn: Vec<(&'static str, String)>,
    /// What a `--seed` run was replayed with, so its repro passes them back.
    pub(crate) inputs: Option<Vec<u64>>,
    pub(crate) faults_off: Vec<&'static str>,
    /// A check's first failure in a batch, shrunk.
    pub(crate) shrunk: Option<Shrunk>,
}

/// The smallest run `shrink` found that still fails the check: the shortest
/// and smallest input choices, then the fewest fault axes on. Every field but
/// the last three describes that run, which `--seed`, `--inputs` and
/// `--faults-off` replay exactly.
#[derive(Debug)]
pub(crate) struct Shrunk {
    /// The run's input choices, trimmed of trailing zeros: a replay reads 0s
    /// past the end, so they are implied.
    pub choices: Vec<u64>,
    /// The axes turned off, in `FaultConfig` field order.
    pub faults_off: Vec<&'static str>,
    pub drawn: Vec<(&'static str, String)>,
    pub message: String,
    pub coverage: BTreeMap<&'static str, u64>,
    /// The seed's config with `faults_off` turned off.
    pub config: FaultConfig,
    /// Candidate runs replayed, kept or not; at most `SHRINK_BUDGET`.
    pub replays: u32,
    /// Whether the repro must pass `--inputs`: false only when the choices
    /// the run consumed, zeros past the kept ones included, begin the first
    /// failure's, which are the seed's own stream. Compared untrimmed: a run
    /// that read 0s past its kept choices where the stream would give other
    /// values needs `--inputs`, or the repro replays a different run
    /// (`a_run_that_read_zeros_past_its_choices_replays_with_inputs`).
    pub replay_inputs: bool,
    /// False when nothing smaller failed, so this is the first failure itself.
    pub smaller: bool,
}

/// The shrinker's budget, counted in replayed runs: nothing in the harness
/// reads a clock, so the result is the same on any machine.
const SHRINK_BUDGET: u32 = 500;

/// Shrinks the first failure of `test` on `seed` (a batch shrinks only each
/// check's first failure, in seed order) by replaying candidates, each run by
/// `run_app_universe` exactly as `--seed --inputs --faults-off` would run it.
/// A candidate is kept when it fails the same check, with any message; a run
/// that passes or gives up has not. Candidates come from four passes, repeated
/// until a round keeps nothing or the budget is spent:
/// 1. delete chunks of the choices (8, 4, 2, then 1 long, from the end);
/// 2. set each choice to 0;
/// 3. binary-search each choice toward 0;
/// 4. turn off each fault axis that is on, in `FaultConfig` field order.
///
/// Every candidate is strictly smaller than the best so far (shorter or
/// lexicographically smaller choices, or one more axis off), so each round
/// shrinks or ends the loop. It depends only on its arguments, so the result
/// is identical for any `--jobs`.
fn shrink(test: &SimTest, seed: u64, first: &UniverseFailure) -> Shrunk {
    let mut s = Shrinker {
        test,
        seed,
        first_choices: &first.choices,
        tried: BTreeSet::new(),
        best: Shrunk {
            choices: trimmed(&first.choices),
            faults_off: Vec::new(),
            drawn: first.drawn.clone(),
            message: first.message.clone(),
            coverage: first.coverage.clone(),
            config: first.config.clone(),
            replays: 0,
            replay_inputs: false,
            smaller: false,
        },
    };
    loop {
        let round = (s.best.choices.clone(), s.best.faults_off.clone());
        s.delete_chunks();
        s.zero_each();
        s.search_each_toward_zero();
        s.turn_off_axes();
        if s.spent() || (&s.best.choices, &s.best.faults_off) == (&round.0, &round.1) {
            return s.best;
        }
    }
}

struct Shrinker<'a> {
    test: &'a SimTest,
    seed: u64,
    first_choices: &'a [u64],
    tried: BTreeSet<(Vec<u64>, Vec<&'static str>)>,
    best: Shrunk,
}

impl Shrinker<'_> {
    fn spent(&self) -> bool {
        self.best.replays >= SHRINK_BUDGET
    }

    /// Replays a candidate and keeps it if it fails the check. A candidate
    /// already replayed, or past the budget, is not run again.
    fn try_keep(&mut self, choices: &[u64], faults_off: Vec<&'static str>) -> bool {
        let choices = trimmed(choices);
        assert!(
            faults_off.len() > self.best.faults_off.len()
                || (choices.len(), &choices) < (self.best.choices.len(), &self.best.choices),
            "a shrink candidate must be smaller than the best so far"
        );
        if self.spent() || !self.tried.insert((choices.clone(), faults_off.clone())) {
            return false;
        }
        self.best.replays += 1;
        let Err(failed) = run_app_universe(self.seed, Some(&choices), &faults_off, &self.test.run)
        else {
            return false;
        };
        // The run read at most `choices` and then 0s, so what it consumed,
        // trimmed, is no larger than the candidate.
        let kept = trimmed(&failed.choices);
        assert!(
            (kept.len(), &kept) <= (choices.len(), &choices),
            "a replay consumed choices it was not given"
        );
        self.best = Shrunk {
            replay_inputs: !self.first_choices.starts_with(&failed.choices),
            choices: kept,
            faults_off,
            drawn: failed.drawn,
            message: failed.message,
            coverage: failed.coverage,
            config: failed.config,
            replays: self.best.replays,
            smaller: true,
        };
        true
    }

    fn delete_chunks(&mut self) {
        for size in [8, 4, 2, 1] {
            let mut at = self.best.choices.len().checked_sub(size);
            while let Some(i) = at {
                let mut c = self.best.choices.clone();
                c.drain(i..i + size);
                at = if self.try_keep(&c, self.best.faults_off.clone()) {
                    // What now sits at `i` is new, so try `i` again.
                    self.best
                        .choices
                        .len()
                        .checked_sub(size)
                        .map(|last| last.min(i))
                } else {
                    i.checked_sub(1)
                };
            }
        }
    }

    fn zero_each(&mut self) {
        let mut i = 0;
        while i < self.best.choices.len() {
            if self.best.choices[i] != 0 {
                self.keep_choice(i, 0);
            }
            i += 1;
        }
    }

    /// Binary search from 0, taken as passing (pass 2 tried it), toward the
    /// choice, which fails. It gallops up first (1, 2, 4, …) and bisects the
    /// step where a value first fails, so a small failing value is found
    /// whatever the choice's size: a draw maps its choice by a remainder
    /// (`lo + raw % span`), so a choice billions of spans up fails exactly as
    /// one inside the first span does, and a plain bisection of
    /// `0..choice` would settle on one far up.
    fn search_each_toward_zero(&mut self) {
        let mut i = 0;
        'choices: while i < self.best.choices.len() {
            let (mut lo, mut hi) = (0, self.best.choices[i]);
            let mut probe = Some(1u64);
            while let Some(p) = probe.filter(|&p| p < hi) {
                if self.spent() {
                    return;
                }
                if self.keep_choice(i, p) {
                    match self.best.choices.get(i) {
                        Some(&v) => hi = v,
                        None => continue 'choices,
                    }
                    break;
                }
                lo = p;
                probe = p.checked_mul(2);
            }
            while lo + 1 < hi {
                if self.spent() {
                    return;
                }
                let mid = lo + (hi - lo) / 2;
                if self.keep_choice(i, mid) {
                    match self.best.choices.get(i) {
                        Some(&v) => hi = v,
                        None => continue 'choices,
                    }
                } else {
                    lo = mid;
                }
            }
            i += 1;
        }
    }

    /// The best choices with choice `i` set to `value`, kept if that fails.
    fn keep_choice(&mut self, i: usize, value: u64) -> bool {
        let mut c = self.best.choices.clone();
        c[i] = value;
        self.try_keep(&c, self.best.faults_off.clone())
    }

    fn turn_off_axes(&mut self) {
        for axis in FAULT_AXES {
            if self.best.faults_off.contains(&axis) || !axis_on(&self.best.config, axis) {
                continue;
            }
            let mut off = self.best.faults_off.clone();
            off.push(axis);
            off.sort_by_key(|a| FAULT_AXES.iter().position(|f| f == a));
            self.try_keep(&self.best.choices.clone(), off);
        }
    }
}

/// `choices` without its trailing zeros, which a replay implies.
fn trimmed(choices: &[u64]) -> Vec<u64> {
    let len = choices.iter().rposition(|&c| c != 0).map_or(0, |i| i + 1);
    choices[..len].to_vec()
}

/// `--inputs`' encoding: each choice as unsigned LEB128, the bytes in
/// lowercase hex. No choices encode as one zero choice, which replays the same
/// (a replay reads 0s past the end), so the flag always has a value.
fn encode_inputs(choices: &[u64]) -> String {
    let mut hex = String::new();
    for &choice in choices {
        let mut v = choice;
        loop {
            let low = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                hex.push_str(&format!("{low:02x}"));
                break;
            }
            hex.push_str(&format!("{:02x}", low | 0x80));
        }
    }
    if hex.is_empty() {
        "00".to_string()
    } else {
        hex
    }
}

/// `encode_inputs` read back: `None` for an empty value, a character that is
/// not lowercase hex, an odd length, a number cut off mid-way, or one past 64
/// bits. `encode_inputs` never prints an empty value, and the likeliest way to
/// pass one is an unset shell variable, which must not replay all zeros.
fn decode_inputs(hex: &str) -> Option<Vec<u64>> {
    if hex.is_empty()
        || !hex.len().is_multiple_of(2)
        || !hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        return None;
    }
    let mut choices = Vec::new();
    let (mut value, mut shift, mut open) = (0u64, 0u32, false);
    for i in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[i..i + 2], 16).ok()?;
        let low = u64::from(byte & 0x7f);
        if shift > 63 || (shift == 63 && low > 1) {
            return None;
        }
        value |= low << shift;
        open = byte & 0x80 != 0;
        if open {
            shift += 7;
        } else {
            choices.push(value);
            (value, shift) = (0, 0);
        }
    }
    (!open).then_some(choices)
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
        Some(seed) => run_single_seed(&selected, seed, args.inputs.as_deref(), &args.faults_off),
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

/// One run per test on `seed`, replaying `inputs` and with `faults_off`
/// turned off when given: what a failure's repro line runs. It shrinks
/// nothing, since it replays one run, and its repro passes back what it was
/// given.
fn run_single_seed(
    tests: &[&SimTest],
    seed: u64,
    inputs: Option<&[u64]>,
    faults_off: &[&'static str],
) -> Report {
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
        match run_app_universe(seed, inputs, faults_off, &test.run) {
            Ok(end) => {
                tally(&mut outcomes, &end);
                merge(&mut report.coverage, &end.coverage);
            }
            Err(failed) => {
                let mut failure = Failure::of(test.name, seed, &failed);
                failure.inputs = inputs.map(<[u64]>::to_vec);
                failure.faults_off = faults_off.to_vec();
                report.failures.push(failure);
            }
        }
        report.outcomes.push(outcomes);
    }
    report
}

impl Failure {
    fn of(test: &'static str, seed: u64, failed: &UniverseFailure) -> Failure {
        Failure {
            test,
            seed,
            config: failed.config.clone(),
            coverage: failed.coverage.clone(),
            message: failed.message.clone(),
            drawn: failed.drawn.clone(),
            inputs: None,
            faults_off: Vec::new(),
            shrunk: None,
        }
    }
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

    // The same large-Err trade as `run_app_universe`.
    #[allow(clippy::result_large_err)]
    let results: Vec<UniverseOutcome> = in_parallel(&work, jobs, |&(test, seed)| {
        run_app_universe(seed, None, &[], &tests[test].run)
    });
    let mut results = results.into_iter().zip(&work);

    // Each test's first failure in seed order (universe index, never
    // completion order), with where its `Failure` sits in the report.
    let mut firsts: Vec<(usize, usize, u64, UniverseFailure)> = Vec::new();
    for (test_index, test) in tests.iter().enumerate() {
        let mut covered: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut outcomes = TestOutcomes {
            test: test.name,
            verdicts: 0,
            gave_up: 0,
        };
        let mut first = true;
        for _ in 0..universes {
            let (outcome, &(_, seed)) = results.next().expect("one result per work item");
            match outcome {
                Ok(end) => {
                    tally(&mut outcomes, &end);
                    merge(&mut covered, &end.coverage);
                }
                Err(failed) => {
                    report.failures.push(Failure::of(test.name, seed, &failed));
                    if first {
                        firsts.push((report.failures.len() - 1, test_index, seed, failed));
                        first = false;
                    }
                }
            }
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

    // Each shrink is a function of its check, seed and failure alone, so
    // running them in parallel changes nothing but the wall time.
    let shrunk = in_parallel(&firsts, jobs, |(_, test, seed, failed)| {
        shrink(tests[*test], *seed, failed)
    });
    for ((at, ..), shrunk) in firsts.iter().zip(shrunk) {
        report.failures[*at].shrunk = Some(shrunk);
    }
    report
}

/// `work` over every item on a pool of `jobs` threads, returned in item
/// order, so the pool's scheduling is unobservable.
fn in_parallel<T: Sync, R: Send>(
    items: &[T],
    jobs: usize,
    work: impl Fn(&T) -> R + Sync,
) -> Vec<R> {
    let next = AtomicUsize::new(0);
    let slots: Mutex<Vec<Option<R>>> = Mutex::new((0..items.len()).map(|_| None).collect());
    // The one sanctioned thread-spawning site in the workspace besides prod
    // impls: the harness orchestrates universes from OUTSIDE the deterministic
    // boundary — each universe inside a worker is a sealed, single-threaded,
    // seed-determined world, and results are ordered by item index so the
    // pool's scheduling is unobservable.
    #[allow(clippy::disallowed_methods)]
    std::thread::scope(|scope| {
        for _ in 0..jobs.max(1) {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    if index >= items.len() {
                        return;
                    }
                    let result = work(&items[index]);
                    slots.lock().unwrap()[index] = Some(result);
                }
            });
        }
    });
    slots
        .into_inner()
        .unwrap()
        .into_iter()
        .map(|r| r.expect("every item is filled before the pool returns"))
        .collect()
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
            // The run this record describes: a batch's records are the runs
            // the batch decided, unshrunk; the human block shows the shrink.
            json_escape(
                &repro(f.test, f.seed, f.inputs.as_deref(), &f.faults_off),
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

/// The failure block (GUIDE §8 "Reading a failure"). A check's first failure
/// in a batch prints its shrunk run, with a `shrunk:` line saying how it
/// shrank and a `first:` line when the shrunk run failed with another
/// message; its repro replays the shrunk run. Any other failure, and a
/// `--seed` replay, prints its own run.
fn human_failure_block(f: &Failure) -> String {
    let (message, config, coverage, drawn, inputs, faults_off) = match &f.shrunk {
        Some(s) => (
            &s.message,
            &s.config,
            &s.coverage,
            &s.drawn,
            s.replay_inputs.then_some(&s.choices[..]),
            &s.faults_off[..],
        ),
        None => (
            &f.message,
            &f.config,
            &f.coverage,
            &f.drawn,
            f.inputs.as_deref(),
            &f.faults_off[..],
        ),
    };
    let mut block = format!(
        "\nsim failure in {}: {message}\n  seed:   {:#018x}\n  config: {config:?}\n  fired:  {}\n",
        f.test,
        f.seed,
        crate::render_fired(coverage),
    );
    if !drawn.is_empty() {
        let inputs: Vec<String> = drawn.iter().map(|(n, v)| format!("{n}={v}")).collect();
        block.push_str(&format!("  inputs: {}\n", inputs.join(", ")));
    }
    if let Some(s) = &f.shrunk {
        if s.smaller {
            let off = match s.faults_off.is_empty() {
                true => "none".to_string(),
                false => s.faults_off.join(", "),
            };
            block.push_str(&format!(
                "  shrunk: {} draws to {}, faults off: {off}, in {} replays\n",
                f.drawn.len(),
                s.drawn.len(),
                s.replays
            ));
        } else {
            block.push_str(&format!(
                "  shrunk: nothing smaller fails, in {} replays\n",
                s.replays
            ));
        }
        if s.message != f.message {
            block.push_str(&format!("  first:  {}\n", f.message));
        }
    }
    block.push_str(&format!(
        "  repro:  {}\n",
        repro(f.test, f.seed, inputs, faults_off)
    ));
    block
}

/// The command that replays one run: its check, its seed, and the inputs and
/// fault axes the seed alone would not reproduce.
fn repro(test: &str, seed: u64, inputs: Option<&[u64]>, faults_off: &[&str]) -> String {
    let mut line = format!("charpente test --sim --filter {test} --seed {seed:#x}");
    if let Some(choices) = inputs {
        line.push_str(&format!(" --inputs {}", encode_inputs(choices)));
    }
    if !faults_off.is_empty() {
        line.push_str(&format!(" --faults-off {}", faults_off.join(",")));
    }
    line
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
        let config_line = "  config: FaultConfig { write_error: 0.0, sync_error: 0.118, crash_apply: 0.002, crash_torn: 0.664668, dir_entry_loss: 0.0, bit_flip: 0.0, misdirected_write: 0.0, read_error: 0.0, object_put_error: 0.0, object_get_error: 0.0, object_lost_ack: 0.0, object_delete_error: 0.0, corruption_scope: \"wal/\", conn_drop: 0.0, net_spike: 0.0, subscriber_stall: 0.0, outbound_refused: 0.0, outbound_lost_request: 0.0, outbound_lost_reply: 0.0, outbound_throttle: 0.0, outbound_slow: 0.0, outbound_latency: (0ns, 0ns), disk_latency: (0ns, 0ns), net_latency: (0ns, 250ms), clock_skew: 41207118532, clock_step_back: (0.0, 0ns), clock_downtime: (0.5, 259200s) }\n";
        let mut failure = Failure {
            test: "receipts_exactly_once",
            seed: 0x8bee_1752_811a_e873,
            config: config.clone(),
            coverage: BTreeMap::from([("fault:crash", 2)]),
            message: "exactly-once violated: hits 3, done 1 for key 812".to_string(),
            drawn: vec![
                ("key", "812".into()),
                ("key", "77".into()),
                ("order", "[1, 0]".into()),
            ],
            inputs: None,
            faults_off: Vec::new(),
            shrunk: Some(Shrunk {
                choices: vec![3, 200],
                faults_off: vec!["bit_flip", "read_error"],
                drawn: vec![("key", "3".into()), ("order", "[1, 0]".into())],
                message: "exactly-once violated: hits 2, done 1 for key 3".to_string(),
                coverage: BTreeMap::from([("fault:dir-entry", 3), ("restart", 7)]),
                config,
                replays: 87,
                replay_inputs: true,
                smaller: true,
            }),
        };
        // The binding copy, line for line: each label padded so the values
        // align, `inputs:` because the run drew, `first:` because the shrink
        // slipped to another message, and a repro that replays the shrunk run.
        assert_eq!(
            human_failure_block(&failure),
            format!(
                "\nsim failure in receipts_exactly_once: exactly-once violated: hits 2, done 1 for key 3\n\
                 \x20 seed:   0x8bee1752811ae873\n\
                 {config_line}\
                 \x20 fired:  fault:dir-entry: 3, restart: 7\n\
                 \x20 inputs: key=3, order=[1, 0]\n\
                 \x20 shrunk: 3 draws to 2, faults off: bit_flip, read_error, in 87 replays\n\
                 \x20 first:  exactly-once violated: hits 3, done 1 for key 812\n\
                 \x20 repro:  charpente test --sim --filter receipts_exactly_once --seed 0x8bee1752811ae873 --inputs 03c801 --faults-off bit_flip,read_error\n"
            )
        );

        // Nothing smaller failed: the block is the first failure's own, the
        // `shrunk:` line says so, and the repro is the seed alone.
        let first = failure.shrunk.take().unwrap();
        failure.shrunk = Some(Shrunk {
            choices: vec![812, 77, 1],
            faults_off: Vec::new(),
            drawn: failure.drawn.clone(),
            message: failure.message.clone(),
            coverage: failure.coverage.clone(),
            config: first.config,
            replays: 41,
            replay_inputs: false,
            smaller: false,
        });
        assert_eq!(
            human_failure_block(&failure),
            format!(
                "\nsim failure in receipts_exactly_once: exactly-once violated: hits 3, done 1 for key 812\n\
                 \x20 seed:   0x8bee1752811ae873\n\
                 {config_line}\
                 \x20 fired:  fault:crash: 2\n\
                 \x20 inputs: key=812, key=77, order=[1, 0]\n\
                 \x20 shrunk: nothing smaller fails, in 41 replays\n\
                 \x20 repro:  charpente test --sim --filter receipts_exactly_once --seed 0x8bee1752811ae873\n"
            )
        );

        // A run that drew nothing has no `inputs:` line.
        failure.drawn.clear();
        failure.shrunk.as_mut().unwrap().drawn.clear();
        assert!(!human_failure_block(&failure).contains("inputs:"));
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
        let report = run_single_seed(&refs, 0x1234, None, &[]);
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
        let report = run_single_seed(&refs, 0x1234, None, &[]);
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
        let json = run_single_seed(&refs, 0x1234, None, &[]).to_json();
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

    // ------------------------------------------------------------ shrinking

    fn sim_test(name: &'static str, run: fn(&mut Sim) -> Result<(), String>) -> SimTest {
        SimTest {
            name,
            run,
            require: &[],
        }
    }

    fn run_to_string(tests: &[SimTest], flags: &[&str]) -> (i32, String) {
        let mut out = Vec::new();
        let code = run(tests, &parse(flags).unwrap(), &mut out);
        (code, String::from_utf8(out).unwrap())
    }

    /// The first failure block for `check` in `out`: its headline through its
    /// repro line.
    fn first_block<'a>(out: &'a str, check: &str) -> &'a str {
        let start = out
            .find(&format!("sim failure in {check}: "))
            .unwrap_or_else(|| panic!("no failure block for {check}:\n{out}"));
        let rest = &out[start..];
        let repro = rest
            .find("  repro:  ")
            .expect("a block ends with its repro line");
        &rest[..repro + rest[repro..].find('\n').unwrap() + 1]
    }

    fn line<'a>(block: &'a str, label: &str) -> Option<&'a str> {
        block.lines().find_map(|l| l.strip_prefix(label))
    }

    /// Runs a block's repro line as `charpente test --sim` would, and returns
    /// the replay's block for `check`.
    fn replay(tests: &[SimTest], block: &str, check: &str) -> String {
        let repro = line(block, "  repro:  ").unwrap();
        let args: Vec<&str> = repro.split_whitespace().collect();
        assert_eq!(args[..3], ["charpente", "test", "--sim"], "{repro}");
        let (code, out) = run_to_string(tests, &args[3..]);
        assert_eq!(code, 1, "the repro replays a failure:\n{out}");
        first_block(&out, check).to_string()
    }

    /// The block a replay prints: the batch's, less the lines only shrinking
    /// prints. A replay is one run, so it shrinks nothing.
    fn unshrunk(block: &str) -> String {
        block
            .lines()
            .filter(|l| !l.starts_with("  shrunk: ") && !l.starts_with("  first:  "))
            .map(|l| format!("{l}\n"))
            .collect()
    }

    /// Draws up to six numbers in 0..=1000 and returns how many it had drawn
    /// when one landed 500 or more from an earlier one.
    fn far_apart(sim: &Sim) -> Option<usize> {
        let mut seen: Vec<u64> = Vec::new();
        while seen.len() < 6 {
            let n = sim.input().between("n", 0, 1000);
            let apart = seen.iter().any(|m| m.abs_diff(n) >= 500);
            seen.push(n);
            if apart {
                return Some(seen.len());
            }
        }
        None
    }

    fn two_far_apart(sim: &mut Sim) -> Result<(), String> {
        match far_apart(sim) {
            Some(_) => Err("two numbers are 500 or more apart".into()),
            None => Ok(()),
        }
    }

    /// `two_far_apart` with its draw count in the message, so a shrink that
    /// draws fewer slips to another message (Review Focus 5).
    fn two_far_apart_after_n(sim: &mut Sim) -> Result<(), String> {
        match far_apart(sim) {
            Some(n) => Err(format!("after {n} draws, two are 500 or more apart")),
            None => Ok(()),
        }
    }

    #[test]
    fn a_failure_shrinks_to_the_smallest_inputs_that_still_fail() {
        let tests = [sim_test("two_far_apart", two_far_apart)];
        let flags = ["--universes", "4", "--master-seed", "0x5400", "--jobs", "2"];
        let (code, out) = run_to_string(&tests, &flags);
        assert_eq!(code, 1, "{out}");
        let block = first_block(&out, "two_far_apart");
        // The smallest failing draws: one number 500 from the 0 a replay
        // reads past the end of its choices.
        assert_eq!(line(block, "  inputs: "), Some("n=500, n=0"), "{block}");
        let shrunk = line(block, "  shrunk: ").unwrap_or_else(|| panic!("{block}"));
        let (from, rest) = shrunk.split_once(" draws to ").unwrap();
        assert!(
            from.parse::<usize>().unwrap() > 2,
            "non-vacuous: the first failure drew more than it needed: {block}"
        );
        // `{to}` counts the draws the `inputs:` line lists.
        assert!(rest.starts_with("2, faults off: "), "{block}");
        assert!(rest.ends_with(" replays"), "{block}");
        assert_eq!(
            line(block, "  first:  "),
            None,
            "a fixed message cannot slip: {block}"
        );
        // A replay reads 0s past the end, so `--inputs` carries one choice for
        // the two draws: the trailing zero is trimmed.
        let repro = line(block, "  repro:  ").unwrap();
        let hex = repro
            .split(" --inputs ")
            .nth(1)
            .unwrap()
            .split(' ')
            .next()
            .unwrap();
        assert_eq!(decode_inputs(hex).map(|c| c.len()), Some(1), "{repro}");
        assert_eq!(replay(&tests, block, "two_far_apart"), unshrunk(block));
    }

    #[test]
    fn shrinking_turns_off_faults_the_failure_does_not_need() {
        let on = |seed: u64| -> Vec<&'static str> {
            let cfg = crate::app_config(seed, &[]);
            FAULT_AXES
                .into_iter()
                .filter(|a| axis_on(&cfg, a))
                .collect()
        };
        // A fault-heavy run: the first master seed whose run has 15 axes on.
        let (master, seed) = (0u64..)
            .map(|m| {
                let mut s = m;
                (m, splitmix64(&mut s))
            })
            .find(|&(_, s)| on(s).len() >= 15)
            .unwrap();
        let axes = on(seed);
        let tests = [sim_test("always_fails", always_fails)];
        let master = format!("{master:#x}");
        let (code, out) = run_to_string(&tests, &["--universes", "1", "--master-seed", &master]);
        assert_eq!(code, 1, "{out}");
        let block = first_block(&out, "always_fails");
        // Every axis that was on, in field order, one replay each.
        let want = format!(
            "0 draws to 0, faults off: {}, in {} replays",
            axes.join(", "),
            axes.len()
        );
        assert_eq!(line(block, "  shrunk: "), Some(want.as_str()), "{block}");
        let config = format!("{:?}", crate::app_config(seed, &axes));
        assert_eq!(line(block, "  config: "), Some(config.as_str()), "{block}");
        let repro = line(block, "  repro:  ").unwrap();
        assert!(
            repro.ends_with(&format!(" --faults-off {}", axes.join(","))),
            "{block}"
        );
        assert!(!repro.contains("--inputs"), "it drew nothing: {block}");
        assert_eq!(replay(&tests, block, "always_fails"), unshrunk(block));
    }

    /// Draws one number per disk write until the disk crashes or a write hits
    /// an injected error, so how many it draws depends on the faults that
    /// fired (Review Focus 4).
    fn draws_until_the_disk_gives_out(sim: &mut Sim) -> Result<(), String> {
        use charpente_core::io::{Disk, Entropy};
        let mut b = [0u8; 1];
        sim.entropy().fill(&mut b);
        sim.crash_disk_after(u64::from(b[0] % 24));
        let Ok(file) = sim.disk().open("draws") else {
            sim.give_up("the disk crashed before the first draw", "");
            return Ok(());
        };
        let mut seen: Vec<u64> = Vec::new();
        while !sim.disk_crashed() && seen.len() < 24 {
            seen.push(sim.input().between("n", 0, 1000));
            if file.write_at(0, b"x").is_err() && !sim.disk_crashed() {
                break;
            }
        }
        match (seen.iter().min(), seen.iter().max()) {
            (Some(lo), Some(hi)) if hi - lo >= 500 => Err("two draws are 500 or more apart".into()),
            _ => Ok(()),
        }
    }

    #[test]
    fn a_fault_dependent_draw_count_shrinks_deterministically_and_its_repro_replays() {
        let check = "draws_until_the_disk_gives_out";
        let tests = [sim_test(check, draws_until_the_disk_gives_out)];
        let flags = |jobs| {
            [
                "--universes",
                "24",
                "--master-seed",
                "0xd2aa",
                "--jobs",
                jobs,
            ]
        };
        let (code, out) = run_to_string(&tests, &flags("1"));
        assert_eq!(code, 1, "{out}");
        assert_eq!(run_to_string(&tests, &flags("1")).1, out, "two runs differ");
        assert_eq!(
            run_to_string(&tests, &flags("4")).1,
            out,
            "--jobs 4 differs"
        );
        let block = first_block(&out, check);
        // Non-vacuous: the faults the shrink turned off changed how many
        // draws the loop made, so the replay ran past the first run's choices.
        let shrunk = line(block, "  shrunk: ").unwrap_or_else(|| panic!("{block}"));
        let (from, rest) = shrunk.split_once(" draws to ").unwrap();
        assert_ne!(from, rest.split_once(',').unwrap().0, "{block}");
        assert_eq!(replay(&tests, block, check), unshrunk(block));
    }

    #[test]
    fn a_slipped_shrink_prints_the_first_message_and_replays_the_shrunk_one() {
        let check = "two_far_apart_after_n";
        let tests = [sim_test(check, two_far_apart_after_n)];
        let (code, out) = run_to_string(&tests, &["--universes", "4", "--master-seed", "0x5400"]);
        assert_eq!(code, 1, "{out}");
        let block = first_block(&out, check);
        let shrunk = "after 2 draws, two are 500 or more apart";
        assert!(
            block.starts_with(&format!("sim failure in {check}: {shrunk}\n")),
            "the headline is the shrunk run's own message: {block}"
        );
        let first = line(block, "  first:  ")
            .unwrap_or_else(|| panic!("a slipped shrink prints the first message: {block}"));
        assert_ne!(first, shrunk, "{block}");
        assert!(
            first.ends_with(" draws, two are 500 or more apart"),
            "{block}"
        );
        let replayed = replay(&tests, block, check);
        assert!(
            replayed.starts_with(&format!("sim failure in {check}: {shrunk}\n")),
            "{replayed}"
        );
        assert_eq!(replayed, unshrunk(block));
    }

    /// Fails only on its seed's own draw under its seed's own config, so no
    /// smaller input and no axis turned off still fails it.
    fn fails_only_as_sampled(sim: &mut Sim) -> Result<(), String> {
        let n = sim.input().between("n", 0, u64::MAX);
        let drawn_as_sampled = n == crate::keyed_stream(sim.seed(), "inputs").next_u64();
        let config_as_sampled =
            format!("{:?}", sim.world.cfg) == format!("{:?}", crate::app_config(sim.seed(), &[]));
        if drawn_as_sampled && config_as_sampled {
            Err(format!("only {n} fails"))
        } else {
            Ok(())
        }
    }

    #[test]
    fn nothing_smaller_fails_prints_so() {
        let check = "fails_only_as_sampled";
        let tests = [sim_test(check, fails_only_as_sampled)];
        let (code, out) = run_to_string(&tests, &["--universes", "1", "--master-seed", "0x77"]);
        assert_eq!(code, 1, "{out}");
        let block = first_block(&out, check);
        let replays: u32 = line(block, "  shrunk: ")
            .and_then(|l| l.strip_prefix("nothing smaller fails, in "))
            .and_then(|l| l.strip_suffix(" replays"))
            .unwrap_or_else(|| panic!("{block}"))
            .parse()
            .unwrap();
        assert!(replays > 0 && replays <= SHRINK_BUDGET, "it tried: {block}");
        assert_eq!(line(block, "  first:  "), None, "{block}");
        assert_eq!(
            line(block, "  repro:  "),
            Some(
                format!("charpente test --sim --filter {check} --seed {:#x}", {
                    let mut s = 0x77;
                    splitmix64(&mut s)
                })
                .as_str()
            ),
            "the seed alone replays it: {block}"
        );
        assert_eq!(replay(&tests, block, check), unshrunk(block));
    }

    /// Draws three numbers and fails only when the first is the seed's own
    /// stream value, so the shrink keeps the first and reads 0 for the other
    /// two, where the seed's stream would give other values.
    fn fails_on_its_own_first_draw(sim: &mut Sim) -> Result<(), String> {
        let first = sim.input().between("n", 0, u64::MAX);
        let _ = sim.input().between("n", 0, u64::MAX);
        let _ = sim.input().between("n", 0, u64::MAX);
        if first == crate::keyed_stream(sim.seed(), "inputs").next_u64() {
            Err("the first draw is the seed's own".into())
        } else {
            Ok(())
        }
    }

    /// The prefix rule behind every repro: the kept choices, trimmed, are a
    /// prefix of the seed's own stream here, but the run read 0s after them,
    /// so the repro must still pass `--inputs` or it replays the stream's
    /// values instead (task 12 review I1).
    #[test]
    fn a_run_that_read_zeros_past_its_choices_replays_with_inputs() {
        let check = "fails_on_its_own_first_draw";
        let tests = [sim_test(check, fails_on_its_own_first_draw)];
        let (code, out) = run_to_string(&tests, &["--universes", "1", "--master-seed", "0x77"]);
        assert_eq!(code, 1, "{out}");
        let block = first_block(&out, check);
        let seed = {
            let mut s = 0x77;
            splitmix64(&mut s)
        };
        let own = crate::keyed_stream(seed, "inputs").next_u64();
        assert_eq!(
            line(block, "  inputs: "),
            Some(format!("n={own}, n=0, n=0").as_str()),
            "{block}"
        );
        // The trap: trimmed, the kept choices are exactly the stream's first
        // value, a prefix of what the seed draws.
        let repro = line(block, "  repro:  ").unwrap();
        let hex = repro
            .split(" --inputs ")
            .nth(1)
            .unwrap_or_else(|| panic!("the run read 0s the seed would not draw: {block}"))
            .split(' ')
            .next()
            .unwrap();
        assert_eq!(decode_inputs(hex), Some(vec![own]), "{block}");
        assert_eq!(replay(&tests, block, check), unshrunk(block));
    }

    /// A clock axis sampled with a chance of 0 cannot fire whatever its size,
    /// so the shrink spends no replay on it and `faults off:` never lists it.
    #[test]
    fn an_axis_that_cannot_fire_is_never_listed_as_turned_off() {
        let (master, seed) = (0u64..)
            .map(|m| {
                let mut s = m;
                (m, splitmix64(&mut s))
            })
            .find(|&(_, s)| {
                let cfg = crate::app_config(s, &[]);
                cfg.clock_step_back.0 == 0.0 && cfg.clock_step_back.1 > Duration::ZERO
            })
            .unwrap();
        let cfg = crate::app_config(seed, &[]);
        let axes: Vec<&str> = FAULT_AXES
            .into_iter()
            .filter(|a| axis_on(&cfg, a))
            .collect();
        assert!(!axes.contains(&"clock_step_back"), "{axes:?}");
        let tests = [sim_test("always_fails", always_fails)];
        let master = format!("{master:#x}");
        let (code, out) = run_to_string(&tests, &["--universes", "1", "--master-seed", &master]);
        assert_eq!(code, 1, "{out}");
        let block = first_block(&out, "always_fails");
        assert!(
            line(block, "  config: ")
                .unwrap()
                .contains("clock_step_back: (0.0, 120s)"),
            "{block}"
        );
        let want = format!(
            "0 draws to 0, faults off: {}, in {} replays",
            axes.join(", "),
            axes.len()
        );
        assert_eq!(line(block, "  shrunk: "), Some(want.as_str()), "{block}");
        assert_eq!(replay(&tests, block, "always_fails"), unshrunk(block));
    }

    /// Two checks fail in one batch: each one's first failure is shrunk, the
    /// shrinks run in parallel, and the output does not depend on `--jobs`.
    #[test]
    fn every_failing_check_shrinks_its_first_failure_under_any_jobs() {
        let checks = ["two_far_apart", "draws_until_the_disk_gives_out"];
        let tests = [
            sim_test(checks[0], two_far_apart),
            sim_test(checks[1], draws_until_the_disk_gives_out),
        ];
        let flags = |jobs| {
            [
                "--universes",
                "8",
                "--master-seed",
                "0x5400",
                "--jobs",
                jobs,
            ]
        };
        let (code, out) = run_to_string(&tests, &flags("1"));
        assert_eq!(code, 1, "{out}");
        assert_eq!(
            run_to_string(&tests, &flags("4")).1,
            out,
            "--jobs 4 differs"
        );
        for check in checks {
            let block = first_block(&out, check);
            assert!(line(block, "  shrunk: ").is_some(), "{block}");
            assert_eq!(replay(&tests, block, check), unshrunk(block));
        }
    }

    /// Fails while each of its sixty draws is at least 2^32, so every draw
    /// has a long way to shrink.
    fn sixty_large_draws(sim: &mut Sim) -> Result<(), String> {
        let mut all_large = true;
        for _ in 0..60 {
            all_large &= sim.input().between("n", 0, u64::MAX) >= 1 << 32;
        }
        if all_large {
            Err("every draw is at least 2^32".into())
        } else {
            Ok(())
        }
    }

    #[test]
    fn the_shrink_budget_is_a_replay_count() {
        let test = sim_test("sixty_large_draws", sixty_large_draws);
        let first = run_app_universe(0x5eed, None, &[], &test.run).unwrap_err();
        let shrunk = shrink(&test, 0x5eed, &first);
        assert!(shrunk.replays <= SHRINK_BUDGET, "{}", shrunk.replays);
        assert_eq!(
            shrunk.replays, SHRINK_BUDGET,
            "the budget, not a fixed point, ended it"
        );
        assert!(shrunk.smaller);
        let failure = Failure {
            test: test.name,
            seed: 0x5eed,
            config: first.config.clone(),
            coverage: first.coverage.clone(),
            message: first.message.clone(),
            drawn: first.drawn.clone(),
            inputs: None,
            faults_off: Vec::new(),
            shrunk: Some(shrunk),
        };
        let block = human_failure_block(&failure);
        assert!(block.contains(", in 500 replays\n"), "{block}");
    }

    #[test]
    fn the_repro_flags_parse_what_a_repro_line_prints() {
        let choices = vec![0, 1, 127, 128, 300, u64::MAX];
        let hex = encode_inputs(&choices);
        assert_eq!(hex, "00017f8001ac02ffffffffffffffffff01");
        assert_eq!(decode_inputs(&hex), Some(choices.clone()));
        let args = parse(&[
            "--seed",
            "0x1",
            "--inputs",
            &hex,
            "--faults-off",
            "clock_skew,write_error",
        ])
        .unwrap();
        assert_eq!(args.inputs, Some(choices));
        assert_eq!(
            args.faults_off,
            ["write_error", "clock_skew"],
            "field order"
        );
        // An all-zero sequence still prints a value: one zero choice replays
        // the same as none, since a replay reads 0s past the end.
        assert_eq!(encode_inputs(&[]), "00");
    }

    #[test]
    fn the_repro_flags_refuse_by_name() {
        assert_eq!(
            parse(&["--inputs", "05"]).unwrap_err(),
            "--inputs needs --seed: it replays one run's inputs"
        );
        assert_eq!(
            parse(&["--faults-off", "write_error"]).unwrap_err(),
            "--faults-off needs --seed: it replays one run with those faults off"
        );
        // Empty (an unset shell variable must not replay all zeros), not
        // hex, odd length, upper case, a truncated number, one past 64 bits,
        // and stray whitespace.
        for bad in ["", "zz", "0", "0A", "80", "ffffffffffffffffffff01", " 05"] {
            assert_eq!(
                parse(&["--seed", "0x1", "--inputs", bad]).unwrap_err(),
                format!(
                    "--inputs: \"{bad}\" is not a choice sequence (lowercase hex of LEB128 \
                     numbers, as a failure's repro line prints it)"
                )
            );
        }
        assert_eq!(
            parse(&["--seed", "0x1", "--faults-off", "write_error,disk_crash"]).unwrap_err(),
            "--faults-off: \"disk_crash\" is not a FaultConfig field"
        );
        assert_eq!(
            parse(&["--seed", "0x1", "--faults-off", "corruption_scope"]).unwrap_err(),
            "--faults-off: \"corruption_scope\" is not a fault axis: it scopes bit_flip and \
             misdirected_write, so turn those off instead"
        );
    }
}
