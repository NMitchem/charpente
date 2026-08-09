# Bench Accounting/Measuring Split Implementation Plan (M2 sub-project 1, interlude)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `core_bench.rs` into an impure measuring driver and a pure accounting layer, so every delta, guard, rate division and INVALID decision the core layer makes becomes a deterministic unit test instead of something only a real benchmark run can exercise.

**Architecture:** A new module `crates/bench-harness/src/core_accounting.rs` holds `CoreObservations` (the data seam) plus the pure functions that turn observations into published values and invalidity reasons. `core_bench::run` keeps the loops, the `Db`, the `Heartbeat`s and the bracket samples, fills a `CoreObservations` in two stages, and makes no arithmetic decision of its own. The two accounting calls happen exactly where `run` makes each decision today — write accounting after the write window and **before** the population loop, read accounting after the read window — so short-circuit ordering and runtime are unchanged.

**Tech Stack:** Rust 2024, no new dependencies. `crates/bench-harness` only.

**Spec:** `docs/superpowers/specs/2026-08-08-bench-accounting-split-design.md` (Noah-ruled 2026-08-08, mid-plan between Task 5 and Task 6).
**Parent spec:** `docs/superpowers/specs/2026-08-06-benchmark-harness-design.md`.
**Parent plan:** `docs/superpowers/plans/2026-08-06-benchmark-harness.md`.

**Plan revision 2 (2026-08-08).** Rev. 1 was reviewed by two adversarial opus reviewers before any execution — `.superpowers/sdd/2026-08-06-benchmark-harness/interlude-plan-review-spec-fidelity.md` and `…-execution.md`. Both returned READY WITH FIXES; every Critical and Important is applied here. Three of their findings changed the shape of the work rather than its wording, and are called out at the sites they affect:

1. **The observations seam is nested, not flat** (spec review I-4, Noah-ruled 2026-08-08). `CoreObservations` holds `write: WriteObs` and `read: ReadObs`, and each accounting function takes only its own half — spec §1's "or the relevant subset", made structural. The write accounting therefore **cannot** read a read field, today or after T6 adds one. Rev. 1 kept a flat struct and pinned the property with a test; the reviewer's objection was that the test perturbs only today's four read fields, so it could not catch the failure mode its own doc comment claimed it prevented. A compile-time guarantee needs no test, so rev. 1's `the_write_accounting_ignores_the_read_half` is **deleted rather than reworded** — do not re-add it.
2. **The new module carries no `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]`** (execution review M-1). Every other module in this crate needs that allow because it genuinely calls `Instant::now`/`std::fs`/`std::thread`; this one is *defined by not doing that*. The workspace deny-list already covers `Instant::now`, `SystemTime`, `std::fs::File` and `std::thread::Builder` (verified in `clippy.toml`), and nothing the accounting does touches it (`Duration`, `String`, `Vec`, `f64`, `Option` are all absent). Omitting the allow upgrades spec §9.1 from a grep into a compiler gate.
3. **The mutation loop restores from a pristine copy, not `git checkout --`** (execution review C-1). At Task 2 Step 3 the new tests are written and *not yet committed*, so `git checkout --` would revert to Task 1's state and delete all 12 of them — after which the remaining mutation rounds run against a file that no longer contains the tests being tested.

## Global Constraints

- **The Global Constraint (parent plan):** `git diff --stat -- crates/charpente-core crates/charpente` must print **nothing**. This interlude touches `crates/bench-harness` and two doc files only. If anything prints, STOP — find and revert before continuing.
- **Zero behaviour change (spec §3).** Not one published number, reason string, exit code or ordering may change. This is a refactor.
- **`report::json_shape_is_pinned` is untouched and green.** If the pin string needs editing, the refactor changed behaviour and is wrong.
- **No existing test may be edited to accommodate the refactor (spec §3.2).** New tests are added; existing assertions stay byte-identical. Task 1 *relocates* three tests with their assertion text byte-identical and *corrects one stale comment* in `tests/harness.rs` — both are called out explicitly at their steps, and neither changes an assertion.
- **No new report fields, no spec §5 amendment (spec §6).** If the refactor appears to need either, it has changed behaviour.
- **The accounting must not grow its own non-finite check.** Naming a non-finite float is `report::collect_non_finite`'s single job (Task 3's standing note from the parent plan).
- **Commit convention:** `bench:` prefix, direct to `main`, named paths only, no `Co-Authored-By` trailers.
- **Gates (spec §8), every task:** `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo clippy -p bench-harness --all-targets --target x86_64-unknown-linux-gnu -- -D warnings` (the target is installed and clippy is check-only, so no cross-linker is needed); `cargo test -p bench-harness`. The full-workspace and `--ignored` runs are Task 3's.
- **Test-count reconciliation:** this plan adds exactly **12** new tests, all in Task 2, all in `crates/bench-harness/src/core_accounting.rs`'s `#[cfg(test)] mod tests`. Task 1 adds zero (it moves three). Static baseline today, by `grep -cE '^\s*#\[test\]'`: `bench_harness` lib **30** (core_bench 3 + device 5 + measure 12 + report 10), `bench` bin **15**, `tests/harness.rs` **6** (all `#[ignore]`). After Task 2 the lib target is **42** and `cargo test -p bench-harness --lib core_accounting` reports **15 passed** (3 moved + 12 new). Workspace delta: **+12**. Any other delta at Task 3 is unexplained and must be accounted for before close-out — **and the resolution is never to delete a test to make the arithmetic agree**; it is to find out what changed.
- **Hard rule 3, with this sub-project's sharpening (spec §7):** the accounting tests pin *already-shipped* behaviour, so they arrive **passing**, and their teeth are proven by **mutation** — mutate the accounting, capture the real RED, restore byte-exact (`sha256sum` both sides), report both halves. A compile error is **not** a RED for an assertion ("novelty is not teeth"). This sub-project has already found five assertions that could not fail; a refactor whose whole purpose is testability must not add a sixth.

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `crates/bench-harness/src/core_accounting.rs` | The pure layer: `CoreObservations`/`WriteObs`/`ReadObs`, `applied_delta`, `device_ratio_reason`, `DEVICE_RATIO_SLACK`, `write_accounting`, `read_accounting`. No `Instant::now()`, no `Db`, no `Disk`, no thread, no IO — enforced by the workspace deny-list, since this module deliberately carries no blanket allow. | **Create** |
| `crates/bench-harness/src/core_bench.rs` | The impure driver: loops, `Db`, `Heartbeat`s, bracket samples. Fills `CoreObservations`, calls the accounting, assembles `CoreReport`. | **Modify** (381 → ~300 lines) |
| `crates/bench-harness/src/lib.rs` | Module declarations. | **Modify** (one line) |
| `crates/bench-harness/tests/harness.rs` | One **comment-only** correction (Task 1 Step 6). No assertion changes. | **Modify** (comment only) |
| `docs/superpowers/specs/2026-08-06-benchmark-harness-design.md` | One path citation corrected — hard rule 6 (Task 3 Step 3). | **Modify** (one line) |
| `crates/bench-harness/src/measure.rs` | Unchanged. `degenerate_window_reason`, `crosscheck` and `MIN_MEASURED_OPS` are already the shared accounting spec §5 points at; nothing new moves here this round, because the ratio gate has exactly one consumer today. A **second** consumer (T6/T7) is the trigger to move it, per spec §5's "a third copy … is the signal that it belonged in `measure.rs` one layer earlier". | **No change** |
| `crates/bench-harness/src/report.rs` | Unchanged. Its `collect_non_finite` stays the sole non-finite authority. | **No change** |
| `crates/bench-harness/src/device.rs` | Unchanged (spec §6 non-goal — an untouched refactor is how a refactor introduces a defect). | **No change** |

---

### Task 1: The split — extract the accounting, rewire `run`

**Files:**
- Create: `crates/bench-harness/src/core_accounting.rs`
- Modify: `crates/bench-harness/src/lib.rs` (the `pub mod` block)
- Modify: `crates/bench-harness/src/core_bench.rs` (imports, `run`'s body, delete the moved items and their tests)
- Modify: `crates/bench-harness/tests/harness.rs` (one comment)

**Interfaces:**
- Consumes: `crate::measure::{crosscheck, degenerate_window_reason}`, `crate::SLOTS`, `crate::report::CoreReport`.
- Produces, for Task 2 and for T6/T7's adoption of the same seam (spec §5) — **note every struct field is `pub`; `run` lives in another module**:
  - `pub struct WriteObs { pub ops: u64, pub loop_delta: u64, pub elapsed: Duration, pub applied_before: Option<u64>, pub applied_after: Option<u64>, pub heartbeat: Option<String>, pub device_ceiling: f64, pub device_before: f64, pub device_after: f64 }` — `Debug + Clone + Default`
  - `pub struct ReadObs { pub reads: u64, pub hits: u64, pub elapsed: Duration, pub heartbeat: Option<String> }` — `Debug + Clone + Default`
  - `pub struct CoreObservations { pub duration: Duration, pub write: WriteObs, pub read: ReadObs }` — `Debug + Clone + Default`
  - `pub struct WriteAccounting { pub acked_writes_per_sec: f64, pub log_records_per_sec: f64, pub device_ratio: f64, pub invalid: Vec<String> }`
  - `pub struct ReadAccounting { pub reads_per_sec: f64, pub invalid: Vec<String> }`
  - `pub fn applied_delta(before: Option<u64>, after: Option<u64>) -> u64`
  - `pub fn write_accounting(duration: Duration, w: &WriteObs) -> Result<WriteAccounting, String>`
  - `pub fn read_accounting(duration: Duration, r: &ReadObs) -> Result<ReadAccounting, String>`
  - `pub const DEVICE_RATIO_SLACK: f64` and `pub fn device_ratio_reason(ratio: f64, denominator: f64, before: f64, after: f64) -> Option<String>` (both moved from `core_bench.rs`, unchanged).

- [ ] **Step 1: Record the pre-change baseline**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
git rev-parse HEAD | tee /tmp/parent-sha.txt
cargo test -p bench-harness 2>&1 | tee /tmp/bench-baseline.log | grep -E '^test result:'
```

Expected: the lib target reports **30 passed**, the `bench` bin **15 passed**, `tests/harness.rs` **0 passed; 6 ignored**. Record the HEAD sha — Task 3's artifact-equivalence run needs it as the parent commit. If the counts differ from the Global Constraints table, stop and reconcile before touching anything: the baseline is what the +12 is measured against.

- [ ] **Step 2: Create `crates/bench-harness/src/core_accounting.rs`**

The two moved items (`DEVICE_RATIO_SLACK`, `device_ratio_reason`) are reproduced **verbatim** below, doc comments included — do not reword them; their text is the record of two Noah rulings. **There is deliberately no `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]` in this file** (see plan revision note 2) — if you find yourself needing one, you have put something impure in the pure layer.

```rust
//! The accounting half of layer 2 (interlude spec
//! docs/superpowers/specs/2026-08-08-bench-accounting-split-design.md): every
//! delta, guard, rate division and INVALID decision the core layer makes, as
//! pure functions over observations that were already taken.
//!
//! Nothing here reads a clock, touches a database, a disk or a thread, or
//! performs any IO — that is the point, and it is enforced rather than
//! promised: unlike every other module in this crate, this one carries no
//! blanket `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]`,
//! so hard rule 1's workspace deny-list is what holds spec §9.1.
//!
//! A benchmark measures wall-clock performance on real hardware and is
//! definitionally outside the deterministic core (DESIGN §1), so the simulator
//! cannot be its oracle. But only the *measuring* half is irreducibly
//! nondeterministic; the accounting is ordinary total functions that happened
//! to live inside a measurement loop, where the only way to exercise them was
//! to run a real benchmark. That is why N-1 — a two-line bug over a pair of
//! `Option<u64>` — needed real hardware and an unusual flag to surface. This
//! module is charpente's own central principle applied to the one crate that
//! was violating it.

use crate::SLOTS;
use crate::measure::{crosscheck, degenerate_window_reason};
use std::time::Duration;

/// What the measured write window observed. Every field is a count, a duration
/// or a verdict that was actually observed; nothing here is derived and
/// nothing here is a decision.
#[derive(Debug, Clone, Default)]
pub struct WriteObs {
    /// Ops the measured write loop completed.
    pub ops: u64,
    /// The same count taken off the loop's own index — the crosscheck's
    /// second, nominally independent source (spec §5, totals two ways).
    pub loop_delta: u64,
    /// Monotonic time the measured window actually took.
    pub elapsed: Duration,
    /// `Db::last_applied()` either side of the window. `None` means ZERO
    /// records applied; `Some(0)` means one — seq 0 itself. WAL seqs are
    /// 0-based (log.rs's `next_seq` starts at 0), which is the whole of N-1.
    pub applied_before: Option<u64>,
    pub applied_after: Option<u64>,
    /// `Heartbeat::finish()` spanning both bracket samples and the write
    /// window. `None` = one continuous run on a settled machine.
    pub heartbeat: Option<String>,
    /// The device ceiling this run divides by — `device.syncs_per_sec`, a
    /// single sample from the device layer's own separate invocation.
    pub device_ceiling: f64,
    /// This run's own device-sync bracket samples, either side of the write
    /// window, so the ratio's error bar is visible in the artifact.
    pub device_before: f64,
    pub device_after: f64,
}

/// What the measured read window observed.
#[derive(Debug, Clone, Default)]
pub struct ReadObs {
    pub reads: u64,
    pub hits: u64,
    pub elapsed: Duration,
    pub heartbeat: Option<String>,
}

/// The observations seam (interlude spec §1): the whole raw record of one
/// `core_bench::run`.
///
/// Split into halves on purpose. Spec §2 keeps the refusal points where they
/// are — `run` fills `write`, calls `write_accounting` (which may refuse
/// *before* the population loop spends seconds of real fsyncs on a run about
/// to be discarded), then fills `read` and calls `read_accounting`. So the
/// read half is still `Default` while the write accounting runs. Rather than
/// pin "the write accounting ignores the read half" with a test, each
/// accounting function takes only its own half — spec §1's "or the relevant
/// subset", made structural. `write_accounting` therefore **cannot** read a
/// read field, and that stays true when T6 adds one, with no test to
/// remember to extend. Do not add a test for it; there is nothing to test.
///
/// `duration` is the configured window rather than an observation, which is
/// why it sits here and is passed alongside a half rather than inside one:
/// both windows use the same value (`report.duration_secs` is global, spec
/// §5), and both floors name it in their refusal messages.
#[derive(Debug, Clone, Default)]
pub struct CoreObservations {
    pub duration: Duration,
    pub write: WriteObs,
    pub read: ReadObs,
}

/// What the write half publishes, plus every non-fatal reason it collected.
/// `invalid` is in push order; `run` appends it to the report's list as-is,
/// and `the_write_half_pushes_its_reasons_in_a_fixed_order` pins that order.
#[derive(Debug, Clone)]
pub struct WriteAccounting {
    pub acked_writes_per_sec: f64,
    pub log_records_per_sec: f64,
    pub device_ratio: f64,
    pub invalid: Vec<String>,
}

/// What the read half publishes, plus its non-fatal reasons.
#[derive(Debug, Clone)]
pub struct ReadAccounting {
    pub reads_per_sec: f64,
    pub invalid: Vec<String>,
}

/// How much play `device_ratio` may have above 1.0 before it is flagged (T5
/// review, C-1 / Noah ruling #1). `Engine::submit` performs exactly one
/// `wal.sync()` per transaction with no group commit, so the commit path
/// cannot durably ack faster than the device's own sync rate in fact — a
/// ratio above 1.0 is IMPOSSIBLE, not merely unlikely, so anything above it
/// is a measurement artifact worth flagging, not a real durability finding.
///
/// T5 re-review, N-4 / Noah ruling #4 (fixed here): the original 1e-2 was
/// justified as covering "floating-point/timer-resolution noise" — that
/// justification was wrong by roughly seven orders of magnitude. Over a
/// 1s `Instant`-measured window, timer resolution contributes ~1e-9 relative
/// error and f64 arithmetic ~1e-16; 1e-2 instead absorbed a band of
/// cross-window SAMPLING noise (single-sample dispersion of 220-310 syncs/s
/// was measured across otherwise-identical runs), which is precisely the
/// phenomenon this gate exists to detect. A ratio anywhere in `(1.0, 1.01]`
/// published unflagged at exit 0 was C-1's exact defect, one order of
/// magnitude smaller, still shipped. Tightened to 1e-6 — still generous for
/// genuine float/timer slop, the only cause this constant is meant to
/// excuse. The accepted cost: more short/noisy runs now exit 1; those runs
/// ARE genuinely invalid measurements, and the bracket samples
/// (device_syncs_per_sec_before/_after) are what tell the reader why, not a
/// wider slack that hides the same fact.
pub const DEVICE_RATIO_SLACK: f64 = 1e-6;

/// `None` = plausible; `Some(reason)` = INVALID, naming BOTH candidate
/// causes rather than asserting one (the review measured 220-310 syncs/s
/// spread across otherwise-identical runs — noise comparable to the whole
/// signal the ratio is supposed to carry).
pub fn device_ratio_reason(
    ratio: f64,
    denominator: f64,
    before: f64,
    after: f64,
) -> Option<String> {
    if ratio > 1.0 + DEVICE_RATIO_SLACK {
        Some(format!(
            "INVALID: device_ratio {ratio:.3} exceeds 1.0 + {DEVICE_RATIO_SLACK} — \
             Engine::submit performs exactly one wal.sync() per transaction with no group \
             commit, so the commit path cannot durably ack faster than the device's own sync \
             rate; a ratio above 1.0 is a measurement artifact rather than a real durability \
             finding. Two candidate causes, either or both: (1) cross-window variance — \
             device.syncs_per_sec ({denominator:.2}) is a single sample from a separate \
             invocation, and this run's own bracket samples ({before:.2} before / {after:.2} \
             after the write window) show the spread that single sample cannot; (2) the \
             ratio's denominator (device.syncs_per_sec, the one-byte-overwrite loop) is not \
             the same op shape as the canonical commit's WAL append — device.appends_per_sec \
             is closer and may be exceeded less, or not at all"
        ))
    } else {
        None
    }
}

/// Records applied across the measured window, total over all four shapes of
/// `(before, after)`.
///
/// T5 re-review, N-1 (Critical): WAL seqs are 0-based, so `Db::last_applied()`
/// returning `None` means "zero records applied" while `Some(0)` means "one
/// record applied" (seq 0 itself). The shipped `unwrap_or(0)` treated both the
/// same — reading "no records applied yet" (a fresh `Db`, `warmup == 0`) as
/// "record 0 already applied" — undercounting the delta by exactly one on
/// every such run, publishing a wrong `log_records_per_sec` AND raising a
/// named INVALID that accused charpente's commit path of breaking a contract
/// that did not break (hard rule 5). It is a two-line total function over a
/// pair of `Option<u64>`; it needed real hardware and `--warmup 0` to surface
/// only because it was welded to a measurement loop. That is this whole
/// interlude in one function.
pub fn applied_delta(before: Option<u64>, after: Option<u64>) -> u64 {
    match (before, after) {
        (Some(b), Some(a)) => a - b,
        (None, Some(a)) => a + 1,
        (None, None) => 0,
        (Some(_), None) => unreachable!("last_applied cannot go backwards to None"),
    }
}

/// The write half's accounting. `Err` refuses the run — the caller turns it
/// into an `io::Error` and returns before the population loop can spend
/// seconds of fsyncs on it (spec §2: the seam moves where the logic lives, not
/// when it runs). `Ok` carries the published values plus every non-fatal
/// reason in push order.
pub fn write_accounting(duration: Duration, w: &WriteObs) -> Result<WriteAccounting, String> {
    let mut invalid = Vec::new();
    if let Some(reason) = &w.heartbeat {
        invalid.push(reason.clone());
    }
    // Refuse a degenerate write window by name — parity with
    // `device::timed_loop` — before anything downstream treats the op count as
    // a real measurement (T5 review, C-3 / Noah ruling #2). The caller stops
    // its heartbeat before calling in, so this early return leaks no thread.
    if let Some(reason) = degenerate_window_reason("core write", w.ops, duration) {
        return Err(reason);
    }
    // Totals two ways (spec §5): the loop counter delta vs the grand counter.
    // Structurally vacuous today — both increment in the same statement of the
    // same single-threaded loop — kept so every layer's throughput has a
    // uniform cross-check shape; it becomes a real check only if that loop
    // ever gains concurrency (the wire/kernel layers' cross-checks are the
    // genuinely independent ones).
    if let Err(reason) = crosscheck(&[w.loop_delta], w.ops) {
        invalid.push(reason);
    }
    let acked_writes_per_sec = w.ops as f64 / w.elapsed.as_secs_f64();

    // I-3's cross-check: the canonical transaction is documented to commit
    // exactly one effects record per transact, so the applied-seq delta must
    // equal the acked-write count.
    let delta = applied_delta(w.applied_before, w.applied_after);
    let ops = w.ops;
    if delta != ops {
        invalid.push(format!(
            "INVALID: log_records_per_sec measured {delta} applied records against \
             {ops} acked writes — core_bench's canonical transaction is documented to \
             commit exactly one effects record per transact (no dispatcher runs at this \
             layer, so nothing doubles it with a job Done-mark), so these must agree; a \
             mismatch means that contract broke"
        ));
    }
    let log_records_per_sec = delta as f64 / w.elapsed.as_secs_f64();

    let device_ratio = acked_writes_per_sec / w.device_ceiling;
    if let Some(reason) =
        device_ratio_reason(device_ratio, w.device_ceiling, w.device_before, w.device_after)
    {
        invalid.push(reason);
    }

    Ok(WriteAccounting {
        acked_writes_per_sec,
        log_records_per_sec,
        device_ratio,
        invalid,
    })
}

/// The read half's accounting. Both refusals here are `Err` because both mean
/// the read number is not a measurement at all.
pub fn read_accounting(duration: Duration, r: &ReadObs) -> Result<ReadAccounting, String> {
    let mut invalid = Vec::new();
    if let Some(reason) = &r.heartbeat {
        invalid.push(reason.clone());
    }
    // T5 re-review, N-6 (Minor): this arm is UNREACHABLE through
    // `core_bench::run`'s public signature — `run` drives both windows from
    // the same `duration`, reads run on the order of 1.1M/s (needing a window
    // under ~4.5us to trip this floor) while writes run on the order of 250/s
    // (tripping the write arm above under ~20ms), three to four orders of
    // magnitude earlier, so any duration small enough to reach here would
    // already have returned Err from the write arm. It was the fifth
    // assertion this sub-project found that could not fail through the path
    // that reaches it. Extracting the accounting is what gives it teeth:
    // `a_below_floor_read_window_refuses_and_an_at_floor_one_does_not` calls
    // this function directly, so the arm is now reachable without `run`
    // needing separate read/write durations.
    if let Some(reason) = degenerate_window_reason("core read", r.reads, duration) {
        return Err(reason);
    }
    // T5 review, C-2: count HITS, not just read attempts. Every key in
    // 0..SLOTS is populated before the read window, so under a working read
    // loop every read is a hit. The mutation the review demonstrated (a loop
    // body reduced to `reads += 1;` with no `get` call, publishing 42M
    // "reads/sec" from a loop touching the store zero times) leaves `hits` at
    // 0 while `reads` climbs into the millions.
    if r.hits != r.reads {
        let (reads, hits) = (r.reads, r.hits);
        return Err(format!(
            "INVALID: the core read loop completed {reads} reads but only {hits} were hits \
             against a fully populated store (every key in 0..{SLOTS} is written before the \
             read window, spec §3) — either population failed or this loop stopped \
             exercising the store"
        ));
    }

    Ok(ReadAccounting {
        reads_per_sec: r.reads as f64 / r.elapsed.as_secs_f64(),
        invalid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // T5 review, C-1 / Noah ruling #1: a physically-impossible ratio must be
    // named, not silently published. Pure function — no hardware timing
    // dependency — so this asserts the gate itself, independent of whether
    // a real run happens to reproduce a >1.0 ratio (the review saw it 2 of
    // 7 runs on a quiet machine, not reliably reproducible on demand).
    #[test]
    fn a_ratio_above_one_plus_slack_is_named_invalid() {
        let r = device_ratio_reason(1.05, 250.0, 245.0, 255.0).expect("must flag");
        assert!(r.contains("INVALID"), "got: {r}");
        assert!(r.contains("1.050"), "must name the offending value: {r}");
        assert!(
            r.contains("cross-window variance"),
            "must name candidate cause 1: {r}"
        );
        assert!(
            r.contains("not the same op shape"),
            "must name candidate cause 2: {r}"
        );
        assert!(
            r.contains("device.syncs_per_sec"),
            "must name which operand the ratio divides by: {r}"
        );
        assert!(r.contains("245.00"), "must show the before sample: {r}");
        assert!(r.contains("255.00"), "must show the after sample: {r}");
    }

    #[test]
    fn a_ratio_at_or_below_the_slack_boundary_is_not_flagged() {
        assert!(device_ratio_reason(1.0, 250.0, 245.0, 255.0).is_none());
        assert!(
            device_ratio_reason(1.0 + DEVICE_RATIO_SLACK, 250.0, 245.0, 255.0).is_none(),
            "the boundary itself must not flag — only strictly above it"
        );
        assert!(device_ratio_reason(0.79, 250.0, 245.0, 255.0).is_none());
    }

    #[test]
    fn a_ratio_just_over_the_slack_boundary_is_flagged() {
        assert!(
            device_ratio_reason(1.0 + DEVICE_RATIO_SLACK + 0.001, 250.0, 245.0, 255.0).is_some()
        );
    }
}
```

- [ ] **Step 3: Declare the module in `lib.rs`**

In `crates/bench-harness/src/lib.rs`, replace the four-line `pub mod` block (`pub mod core_bench;` … `pub mod report;`) with five lines:

```rust
pub mod core_accounting;
pub mod core_bench;
pub mod device;
pub mod measure;
pub mod report;
```

- [ ] **Step 4: Rewire `core_bench.rs`'s imports**

Replace the whole `use` block at the top of `crates/bench-harness/src/core_bench.rs` (the run of `use` lines between the `#![allow(...)]` and `fn canonical`) with:

```rust
use crate::core_accounting::{self, CoreObservations};
use crate::device::timed_loop;
use crate::measure::Heartbeat;
use crate::report::CoreReport;
use crate::{Bump, SLOTS, Slot, SlotBumped, schema};
use charpente::core::io::Disk;
use charpente::prod::ProdDisk;
use charpente::{Db, DbConfig, Id, TxError, View};
use std::io;
use std::time::{Duration, Instant};
```

`crosscheck` and `degenerate_window_reason` are no longer imported here — they are the accounting's, and leaving them would fail `-D warnings` as unused. `core_bench.rs` **keeps** its own `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]`; it still calls `Instant::now()`.

- [ ] **Step 5: Delete the moved items and the moved tests from `core_bench.rs`**

**Anchor by content, not by line number.** Step 4 has already changed the file's line count, so any line range quoted against HEAD is stale by the time you get here.

1. Delete the item pair `DEVICE_RATIO_SLACK` + `fn device_ratio_reason`: from the line beginning `/// How much play \`device_ratio\` may have above 1.0` through the closing `}` of `device_ratio_reason`, i.e. everything up to (not including) the blank line before `/// A short, independent device-sync sample`.
2. Delete the entire trailing `#[cfg(test)] mod tests { … }` block: from the `#[cfg(test)]` line to the end of the file.
3. **Fix the whitespace both deletions leave behind** — deletion 1 leaves two consecutive blank lines where the pair was, deletion 2 leaves a trailing blank line at EOF. Both are rustfmt violations and Step 9's `cargo fmt --all --check` will be red without this. Either fix them by hand or let Step 9's `cargo fmt --all` do it.

**Deletion 2 is a relocation, not an adjustment (Global Constraints).** The three test functions' bodies and every assertion string in them are byte-identical on both sides. Verify:

```bash
git show HEAD:crates/bench-harness/src/core_bench.rs | sed -n '336,381p' > /tmp/tests-before.txt
sed -n '/^mod tests {/,$p' crates/bench-harness/src/core_accounting.rs > /tmp/tests-after-raw.txt
diff <(grep -v '^\s*$' /tmp/tests-before.txt | grep -v 'mod tests\|use super') \
     <(grep -v '^\s*$' /tmp/tests-after-raw.txt | grep -v 'mod tests\|use super')
```

Expected: **no output.** (`336`, not `335`: HEAD's line 335 is `#[cfg(test)]`, which the "after" slice starts below — rev. 1's `335` produced a guaranteed phantom diff and an instruction to undo correct work.) This check only holds at this moment, while `core_accounting.rs`'s test module contains **only** the three moved tests; it is meaningless after Task 2 adds more. If anything differs, the relocation edited a test — undo and copy verbatim.

Record in your task report that the three tests' fully-qualified names change (`core_bench::tests::*` → `core_accounting::tests::*`). That is visible in test output and must not later be mistaken for a deletion plus an addition.

- [ ] **Step 6: Correct the one comment the refactor makes false**

`crates/bench-harness/tests/harness.rs` contains, above `a_below_floor_core_write_window_is_refused`, the parenthetical "(unit-tested in core_bench.rs's own `#[cfg(test)]` module)". After Step 5, `core_bench.rs` has no test module at all. (The citation is already imprecise today — the floor's own unit tests live in `measure.rs` — so the refactor makes pre-existing drift dangle outright.)

Replace that parenthetical with: `(unit-tested in measure.rs's own #[cfg(test)] module)`.

**This is comment-only.** No assertion in that file changes, so §3.2 is not engaged — that rule forbids adjusting tests to accommodate a refactor, and a dangling pointer left behind in a sub-project whose whole subject is "the docs are the spec" is the opposite of that. State in your task report that exactly one comment changed and no assertion did.

- [ ] **Step 7: Rewrite `run`'s body**

Identify `run` by its `pub fn run(` signature through its closing brace — the line numbers in earlier revisions of this plan are stale once Steps 4-6 have run. Replace it with the following. Every rationale comment from the original is preserved at the site it explains; the arithmetic is gone.

```rust
pub fn run(
    dir: &str,
    warmup: Duration,
    duration: Duration,
    device_syncs_per_sec: f64,
) -> io::Result<(CoreReport, Vec<String>)> {
    let mut db = Db::open(Box::new(ProdDisk::new(dir)?), DbConfig::default(), schema())?;
    let mut invalid = Vec::new();
    let mut i = 0u64;
    // The observations seam (interlude spec §1). Every count, duration and
    // verdict this function takes is recorded here, and every decision about
    // them is made in `core_accounting` — this function makes no arithmetic
    // decision of its own. Filled in two stages so the refusal points do not
    // move (spec §2): the write half is accounted BEFORE the population loop,
    // which can cost seconds of real fsyncs on a run about to be discarded.
    let mut obs = CoreObservations {
        duration,
        ..Default::default()
    };
    obs.write.device_ceiling = device_syncs_per_sec;

    let warm_end = Instant::now() + warmup;
    while Instant::now() < warm_end {
        canonical(&mut db, i)?;
        i += 1;
    }

    // T5 review, C-1 / Noah ruling #1: bracket the measured write window
    // with an independent device-sync sample on either side.
    //
    // T5 re-review, N-2 (Important): the bracket samples are PUBLISHED
    // artifact fields (device_syncs_per_sec_before/_after) — I-1's whole
    // point was that a measured, published window needs a suspension/
    // starvation guard, and the original fix covered only the write loop
    // itself, leaving these two new sibling windows uncovered in the same
    // commit that added them. `hb` starts before the first bracket sample and
    // finishes after the second, so a suspend or starvation episode during
    // either bracket is caught exactly like one during the write loop.
    let hb = Heartbeat::start();
    obs.write.device_before = device_bracket_sample(dir)?;

    let start = Instant::now();
    let end = start + duration;
    let measured_from = i;
    let mut grand = 0u64;
    // T5 review, I-3: log_records_per_sec is a MEASUREMENT of
    // Db::last_applied() sampled either side of the window, not a copy of
    // acked_writes_per_sec — see `core_accounting::write_accounting`.
    obs.write.applied_before = db.last_applied();
    while Instant::now() < end {
        canonical(&mut db, i)?;
        i += 1;
        grand += 1;
    }
    obs.write.elapsed = start.elapsed();
    obs.write.applied_after = db.last_applied();
    obs.write.device_after = device_bracket_sample(dir)?;
    obs.write.heartbeat = hb.finish();
    obs.write.ops = grand;
    obs.write.loop_delta = i - measured_from;

    // May refuse — and refuses HERE, before the population loop below, which
    // is the ordering spec §2 requires be preserved.
    let write =
        core_accounting::write_accounting(obs.duration, &obs.write).map_err(io::Error::other)?;
    invalid.extend(write.invalid);

    // Reads: one snapshot, typed gets. No log involvement — this number shows
    // where the write ceiling does NOT apply (spec §3). Measured under the
    // SAME warmup/duration the caller passed: the Report's warmup_secs/
    // duration_secs fields are global, and a silently different read window
    // would belie them (spec §5 — fields, not caveats buried in code).
    //
    // T5 review, I-2: spec §3 says "typed gets against a POPULATED store" —
    // population was previously an accident of how many writes the window
    // above happened to complete (up to +73% inflated reads/sec on a short
    // run, since a miss is cheaper than a hit over a smaller tree). Populate
    // every key in 0..SLOTS here, OUTSIDE any measured window and outside
    // `grand`/`acked` — this is setup, not a measurement.
    //
    // T5 re-review, N-2 (Important): `read_hb` starts BEFORE this population
    // loop rather than after it. Population can run for several seconds of
    // real fsyncs (I-2's own cost account), and a sleep or starvation episode
    // there would disturb the machine for the read window that follows with
    // nothing able to see it — the same gap I-1 was reopened to close, one
    // loop earlier.
    let read_hb = Heartbeat::start();
    for slot in 0..SLOTS {
        db.transact(|tx| -> Result<(), io::Error> {
            let slot_id = Id::<Slot>::from_raw(slot);
            if tx.get(slot_id)?.is_none() {
                tx.put(&Slot {
                    id: slot_id,
                    hits: 0,
                });
            }
            Ok(())
        })
        .map_err(|e| match e {
            TxError::Io(io) => io,
            TxError::App(io) => io,
            other => io::Error::other(format!("unexpected transact refusal: {other:?}")),
        })?;
    }

    let snap = db.snapshot()?;
    // T5 review, I-1: `read_hb` (started above, before the population loop)
    // covers the read warmup and the read window too — symmetric with the
    // write window's own `hb`, and now spanning everything measured or
    // published in this half of the layer, per N-2 above.
    let read_warm_end = Instant::now() + warmup;
    let mut r = 0u64;
    while Instant::now() < read_warm_end {
        let _ = snap.get(Id::<Slot>::from_raw(r % SLOTS))?;
        r += 1;
    }
    let read_start = Instant::now();
    let read_end = read_start + duration;
    let mut reads = 0u64;
    // T5 review, C-2: count HITS, not just read attempts — the invariant that
    // consumes them lives in `core_accounting::read_accounting`.
    let mut hits = 0u64;
    while Instant::now() < read_end {
        if snap.get(Id::<Slot>::from_raw(reads % SLOTS))?.is_some() {
            hits += 1;
        }
        reads += 1;
    }
    obs.read.elapsed = read_start.elapsed();
    obs.read.heartbeat = read_hb.finish();
    obs.read.reads = reads;
    obs.read.hits = hits;

    let read =
        core_accounting::read_accounting(obs.duration, &obs.read).map_err(io::Error::other)?;
    invalid.extend(read.invalid);

    Ok((
        CoreReport {
            acked_writes_per_sec: write.acked_writes_per_sec,
            log_records_per_sec: write.log_records_per_sec,
            reads_per_sec: read.reads_per_sec,
            device_ratio: write.device_ratio,
            device_ratio_denominator: "device.syncs_per_sec",
            device_syncs_per_sec_before: obs.write.device_before,
            device_syncs_per_sec_after: obs.write.device_after,
        },
        invalid,
    ))
}
```

- [ ] **Step 8: Verify the ordering did not move**

The refusal and push order is behaviour (spec §2, §3.3). Read the new `run` against `git show HEAD:crates/bench-harness/src/core_bench.rs` and confirm, in order:

1. write heartbeat stopped (`hb.finish()`) **after** `device_after`, **before** any write decision;
2. within the write window's sampling: `elapsed` → `applied_after` → `device_after` → `hb.finish()`, in that order;
3. degenerate-write refusal **before** the population loop;
4. crosscheck push, then applied-delta push, then ratio push;
5. population loop, read warmup, read window;
6. `read.elapsed` taken **before** `read_hb.finish()`;
7. degenerate-read refusal, then the `hits != reads` refusal;
8. `write.invalid` extended into `invalid` **before** `read.invalid`.

Write the eight-point confirmation into your task report.

**Two things to state in that report rather than let a reader re-derive:**
- On the degenerate-write refusal, HEAD pushes the heartbeat reason into `run`'s own `invalid` before returning `Err`; the new code pushes it into `write_accounting`'s local vec before returning `Err`. Both vectors are discarded on the `Err` path, so no caller can tell. Not a behaviour change.
- §3.3 is pinned for the write half's four reasons (Task 2's ordering test). The **cross-half** order — every write reason before the read heartbeat reason — is guaranteed by `run`'s statement order alone and is pinned by no test, because forcing both heartbeats to fire needs a genuinely starved machine. State that residual; do not let the ledger read as if §3.3 were fully pinned.

- [ ] **Step 9: Run the gates**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
cargo fmt --all
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p bench-harness --all-targets --target x86_64-unknown-linux-gnu -- -D warnings
cargo test -p bench-harness 2>&1 | tee /tmp/bench-after-t1.log | grep -E '^test result:'
git diff --stat -- crates/charpente-core crates/charpente
git diff --stat -- crates/bench-harness/src/report.rs crates/bench-harness/src/device.rs crates/bench-harness/src/measure.rs
```

Expected: both clippy runs clean; test counts **identical to Step 1's baseline** (lib 30, bin 15, harness 6 ignored — the three tests moved between modules inside one target, so even the per-target counts are unchanged); the two `git diff --stat` lines print **nothing** (the Global Constraint, and the three files spec §6 declares untouched).

If `cargo fmt --all` rewrote anything outside `core_accounting.rs`/`core_bench.rs`, inspect it before continuing — this task should not be reformatting unrelated files.

- [ ] **Step 10: Commit**

```bash
git add crates/bench-harness/src/core_accounting.rs \
        crates/bench-harness/src/core_bench.rs \
        crates/bench-harness/src/lib.rs \
        crates/bench-harness/tests/harness.rs
git commit -m "bench: split core_bench's accounting from its measuring (interlude T1)

The observations seam: core_bench::run keeps the loops, the Db, the
Heartbeats and the bracket samples, fills a CoreObservations, and makes no
arithmetic decision of its own. Every delta, guard, rate division and INVALID
decision moves to core_accounting as pure total functions.

The seam is nested — CoreObservations holds write: WriteObs and read: ReadObs,
and each accounting function takes only its own half (spec §1's 'or the
relevant subset'). The write accounting therefore cannot read a read field,
today or after T6 adds one: a compile-time guarantee rather than a test that
would have to be extended to keep being true.

The module deliberately carries no blanket clippy allow, so the workspace
deny-list is what enforces spec §9.1's no-clock/no-IO criterion.

Refusal ordering is behaviour and is preserved (spec §2): the write
accounting is called after the write window and BEFORE the population loop,
which can cost seconds of fsyncs on a run about to be discarded; the read
accounting after the read window. No published number, reason string, exit
code or ordering changes.

DEVICE_RATIO_SLACK, device_ratio_reason and its three tests move verbatim —
assertion text byte-identical on both sides, mechanically diffed. One comment
in tests/harness.rs corrected where the refactor made it dangle; no assertion
in that file changed."
```

---

### Task 2: The accounting tests, each with teeth proven by mutation

**Files:**
- Modify: `crates/bench-harness/src/core_accounting.rs` (extend `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: everything Task 1 produced, plus `crate::measure::MIN_MEASURED_OPS`.
- Produces: nothing for later tasks — this task is entirely test code.

**Why these tests arrive passing.** They pin already-shipped behaviour (spec §7), so hard rule 3's "RED first" is satisfied here by **mutation** instead: for every test, mutate the accounting, capture the real RED output, then restore byte-exact. A test that compiles-but-fails is not a RED for an assertion — the mutation must leave the code compiling and change *behaviour*.

**There are 12 new tests.** Count them when you are done; the number is a close-out gate in Task 3.

- [ ] **Step 1: Add the import, the fixture, and the twelve tests**

Add `use crate::measure::MIN_MEASURED_OPS;` **at the top of `mod tests`, on the line immediately after `use super::*;`**. Add the fixture and the twelve tests **after the three moved ratio tests**, at the end of the module.

```rust
    /// A plausible healthy run on this class of hardware: 7,500 acked writes
    /// over 30s (250/s against a 300/s device ceiling — a ratio of 0.833,
    /// comfortably below the gate), 33M reads over 30s. Every derived quantity
    /// is exact in f64, so the assertions can be equalities rather than
    /// tolerances. This fixture is also spec §4 bullet 4's ABOVE-the-boundary
    /// case for both windows: 7,500 and 33,000,000 are both far above
    /// `MIN_MEASURED_OPS`, and the two `a_healthy_*` tests assert neither
    /// window is flagged. The two boundary tests below cover at and under.
    fn healthy() -> CoreObservations {
        CoreObservations {
            duration: Duration::from_secs(30),
            write: WriteObs {
                ops: 7_500,
                loop_delta: 7_500,
                elapsed: Duration::from_secs(30),
                applied_before: Some(99),
                applied_after: Some(7_599),
                heartbeat: None,
                device_ceiling: 300.0,
                device_before: 295.0,
                device_after: 305.0,
            },
            read: ReadObs {
                reads: 33_000_000,
                hits: 33_000_000,
                elapsed: Duration::from_secs(30),
                heartbeat: None,
            },
        }
    }

    #[test]
    fn a_healthy_write_half_publishes_and_flags_nothing() {
        let o = healthy();
        let a = write_accounting(o.duration, &o.write).expect("a healthy window publishes");
        assert!(a.invalid.is_empty(), "got: {:?}", a.invalid);
        assert_eq!(a.acked_writes_per_sec, 250.0);
        assert_eq!(
            a.log_records_per_sec, 250.0,
            "an applied delta that agrees with the ack count publishes the same rate"
        );
        assert!((a.device_ratio - 250.0 / 300.0).abs() < 1e-12);
    }

    #[test]
    fn a_healthy_read_half_publishes_and_flags_nothing() {
        let o = healthy();
        let a = read_accounting(o.duration, &o.read).expect("a healthy window publishes");
        assert!(a.invalid.is_empty(), "got: {:?}", a.invalid);
        assert_eq!(a.reads_per_sec, 1_100_000.0);
    }

    // Interlude spec §4, first bullet. N-1 (Critical, T5 re-review) was a
    // two-line total function over a pair of `Option<u64>` that needed real
    // hardware and `--warmup 0` to surface, because it was welded to a
    // measurement loop. All four shapes, including the one asserted
    // unreachable.
    #[test]
    fn applied_delta_is_total_over_every_option_pair() {
        assert_eq!(applied_delta(Some(99), Some(7_599)), 7_500);
        assert_eq!(
            applied_delta(None, Some(0)),
            1,
            "seq 0 is a real record: Some(0) means ONE record applied, not zero"
        );
        assert_eq!(applied_delta(None, Some(9)), 10);
        assert_eq!(applied_delta(None, None), 0);
        assert_eq!(
            applied_delta(Some(7), Some(7)),
            0,
            "a window that applied nothing"
        );
    }

    #[test]
    #[should_panic(expected = "last_applied cannot go backwards to None")]
    fn applied_delta_refuses_a_backwards_last_applied() {
        applied_delta(Some(1), None);
    }

    // The pure twin of tests/harness.rs's #[ignore]d disk regression
    // `a_warmup_zero_core_write_window_reports_the_correct_log_record_count`.
    // Both are kept deliberately (spec §7): this one proves the arithmetic,
    // the disk one proves the wiring that hands it the arithmetic's inputs.
    #[test]
    fn a_warmup_zero_window_does_not_raise_a_false_contract_break() {
        // Exactly the shape of a fresh Db with `--warmup 0`: nothing applied
        // before the window, and `Some(ops - 1)` after it, because seq 0 is
        // the first record rather than the absence of one.
        let mut o = healthy();
        o.write.applied_before = None;
        o.write.applied_after = Some(o.write.ops - 1);
        let a = write_accounting(o.duration, &o.write).expect("publishes");
        assert!(
            a.invalid.is_empty(),
            "a warmup==0 run must not accuse charpente's commit path of breaking a \
             contract that did not break (hard rule 5): {:?}",
            a.invalid
        );
        assert_eq!(a.log_records_per_sec, a.acked_writes_per_sec);
    }

    // Interlude spec §4, second bullet — and the negative half is
    // `a_healthy_write_half_publishes_and_flags_nothing` above.
    #[test]
    fn an_applied_delta_that_disagrees_with_the_ack_count_is_named_invalid() {
        let mut o = healthy();
        o.write.applied_after = Some(7_600); // one record more than the acks
        let a = write_accounting(o.duration, &o.write).expect("a mismatch is named, not refused");
        assert_eq!(a.invalid.len(), 1, "got: {:?}", a.invalid);
        let r = &a.invalid[0];
        assert!(r.contains("INVALID"), "got: {r}");
        assert!(
            r.contains("7501 applied records"),
            "must name the measured delta: {r}"
        );
        assert!(
            r.contains("7500 acked writes"),
            "must name the ack count it disagrees with: {r}"
        );
        assert!(
            r.contains("exactly one effects record per transact"),
            "must name the contract that broke: {r}"
        );
    }

    // Interlude spec §4, fourth bullet — under and at the boundary for the
    // write window; `healthy()` is the above-boundary case.
    #[test]
    fn a_below_floor_write_window_refuses_and_an_at_floor_one_does_not() {
        let mut o = healthy();
        o.write.ops = MIN_MEASURED_OPS - 1;
        o.write.loop_delta = MIN_MEASURED_OPS - 1;
        let err = write_accounting(o.duration, &o.write).expect_err("below the floor refuses");
        assert!(err.contains("INVALID"), "got: {err}");
        assert!(err.contains("core write"), "must name which loop: {err}");
        assert!(err.contains("--duration"), "must name the fix: {err}");

        o.write.ops = MIN_MEASURED_OPS;
        o.write.loop_delta = MIN_MEASURED_OPS;
        o.write.applied_before = None;
        o.write.applied_after = Some(MIN_MEASURED_OPS - 1);
        let a = write_accounting(o.duration, &o.write).expect("the floor itself publishes");
        assert!(a.invalid.is_empty(), "got: {:?}", a.invalid);
    }

    // Interlude spec §4, fourth bullet — under and at the boundary for the
    // read window; `healthy()` is the above-boundary case. N-6 recorded this
    // arm as UNREACHABLE through `run` (both windows share one duration, and
    // the write floor trips three to four orders of magnitude earlier), which
    // made it the fifth assertion this sub-project found that could not fail.
    // Calling the accounting directly is what gives it teeth.
    #[test]
    fn a_below_floor_read_window_refuses_and_an_at_floor_one_does_not() {
        let mut o = healthy();
        o.read.reads = MIN_MEASURED_OPS - 1;
        o.read.hits = MIN_MEASURED_OPS - 1;
        let err = read_accounting(o.duration, &o.read).expect_err("below the floor refuses");
        assert!(err.contains("INVALID"), "got: {err}");
        assert!(err.contains("core read"), "must name which loop: {err}");
        assert!(err.contains("--duration"), "must name the fix: {err}");

        o.read.reads = MIN_MEASURED_OPS;
        o.read.hits = MIN_MEASURED_OPS;
        read_accounting(o.duration, &o.read).expect("the floor itself publishes");
    }

    // Interlude spec §4, fifth bullet. The defect this catches is C-2's own
    // demonstrated mutation: a read loop reduced to `reads += 1;` with no
    // `get` call, publishing 42M "reads/sec" from a loop that touched the
    // store zero times.
    #[test]
    fn a_read_loop_that_stopped_hitting_the_store_refuses_by_name() {
        let mut o = healthy();
        o.read.hits = o.read.reads - 1;
        let err =
            read_accounting(o.duration, &o.read).expect_err("a miss against a populated store");
        assert!(err.contains("INVALID"), "got: {err}");
        assert!(
            err.contains("33000000 reads"),
            "must name the read count: {err}"
        );
        assert!(
            err.contains("32999999 were hits"),
            "must name the hit count: {err}"
        );
        assert!(
            err.contains(&format!("0..{SLOTS}")),
            "must name the key range that was populated: {err}"
        );

        o.read.hits = o.read.reads;
        read_accounting(o.duration, &o.read).expect("hits == reads publishes");
    }

    // Interlude spec §4, last bullet, and Task 3's standing note to T4-T8:
    // naming a non-finite float is `report::collect_non_finite`'s single job.
    // The accounting must not grow a local copy — two places deciding the same
    // thing is how the two renderers drift apart.
    #[test]
    fn a_zero_device_ceiling_yields_a_non_finite_ratio_left_for_the_renderer() {
        let mut o = healthy();
        o.write.device_ceiling = 0.0;
        let a = write_accounting(o.duration, &o.write).expect("a non-finite ratio is no refusal");
        assert!(a.device_ratio.is_infinite(), "got: {}", a.device_ratio);
        // The ratio gate fires — infinity is certainly above 1.0 + slack —
        // exactly as it does today. Preserved, not new.
        assert_eq!(a.invalid.len(), 1, "got: {:?}", a.invalid);
        assert!(a.invalid[0].contains("device_ratio"), "got: {:?}", a.invalid);
        assert!(
            !a.invalid.iter().any(|r| r.contains("not finite")),
            "the accounting must NOT name the non-finite value itself — that is \
             report::collect_non_finite's job, and a local copy here would double-report \
             it in the artifact: {:?}",
            a.invalid
        );
    }

    // Interlude spec §3.3: reasons are pushed in a fixed order and both
    // renderers derive from the same list, so a reordering would be invisible
    // to every other assertion in this file and visible in the published
    // artifact. Force all four non-fatal write-half reasons at once and pin
    // the sequence. (The cross-half order — every write reason before the read
    // heartbeat reason — is guaranteed by `run`'s statement order and is not
    // pinned here; forcing both heartbeats to fire needs a starved machine.)
    #[test]
    fn the_write_half_pushes_its_reasons_in_a_fixed_order() {
        let mut o = healthy();
        o.write.heartbeat = Some("INVALID: heartbeat starved".to_string());
        o.write.loop_delta = o.write.ops - 1; // totals disagree
        o.write.applied_after = Some(7_600); // applied delta disagrees
        o.write.device_ceiling = 1.0; // ratio far above 1.0
        let a = write_accounting(o.duration, &o.write).expect("all four of these are non-fatal");
        let shape: Vec<&str> = a
            .invalid
            .iter()
            .map(|r| {
                if r.contains("heartbeat starved") {
                    "heartbeat"
                } else if r.contains("totals disagree") {
                    "crosscheck"
                } else if r.contains("log_records_per_sec measured") {
                    "applied"
                } else if r.contains("device_ratio") {
                    "ratio"
                } else {
                    "UNKNOWN"
                }
            })
            .collect();
        assert_eq!(
            shape,
            vec!["heartbeat", "crosscheck", "applied", "ratio"],
            "got: {:?}",
            a.invalid
        );
    }

    #[test]
    fn the_read_half_carries_its_heartbeat_reason_through() {
        let mut o = healthy();
        o.read.heartbeat = Some("INVALID: heartbeat starved".to_string());
        let a = read_accounting(o.duration, &o.read).expect("a starved heartbeat is no refusal");
        assert_eq!(a.invalid, vec!["INVALID: heartbeat starved".to_string()]);
        assert_eq!(a.reads_per_sec, 1_100_000.0);
    }
```

- [ ] **Step 2: Run them — they must arrive green**

```bash
cargo test -p bench-harness --lib core_accounting 2>&1 | tee /tmp/t2-arrive.log | grep -E '^test result:'
```

Expected: **`ok. 15 passed`** (3 moved + 12 new). If the number is not 15, count your `#[test]` items before doing anything else. If any test FAILS on arrival, the refactor changed behaviour — stop and report it as a Task 1 defect rather than adjusting the test.

- [ ] **Step 3: Prove the teeth by mutation — record every RED**

Take a sha **and a pristine copy** first. The copy is the restore mechanism: at this point the new tests are written and **not committed**, so `git checkout --` would revert to Task 1's state and delete all twelve of them.

```bash
sha256sum crates/bench-harness/src/core_accounting.rs | tee /tmp/accounting-sha-before.txt
cp crates/bench-harness/src/core_accounting.rs /tmp/core_accounting.pristine.rs
```

For **each** row below: apply the mutation, run the named test, capture the actual failure output verbatim, then restore with

```bash
cp /tmp/core_accounting.pristine.rs crates/bench-harness/src/core_accounting.rs
```

Every mutation leaves the code **compiling** — a compile error is not a RED.

| # | Mutation (in `core_accounting.rs`) | Test that must go RED | Note |
|---|---|---|---|
| 1 | `(None, Some(a)) => a + 1` → `(None, Some(a)) => a` | `applied_delta_is_total_over_every_option_pair` | |
| 2 | `(Some(_), None) => unreachable!(...)` → `(Some(_), None) => 0` | `applied_delta_refuses_a_backwards_last_applied` | fails "did not panic" |
| 3 | same edit as row 1 | `a_warmup_zero_window_does_not_raise_a_false_contract_break` | **same mutation as row 1, different test** — not a mis-copy |
| 4 | `if delta != ops` → `if false` | `an_applied_delta_that_disagrees_with_the_ack_count_is_named_invalid` | `delta` and `ops` both stay used |
| 5 | delete the `return Err(reason)` in the `"core write"` floor arm; add `let _ = reason;` so the binding stays used | `a_below_floor_write_window_refuses_and_an_at_floor_one_does_not` | |
| 6 | same, for the `"core read"` floor arm | `a_below_floor_read_window_refuses_and_an_at_floor_one_does_not` | |
| 7 | `if r.hits != r.reads` → `if false` | `a_read_loop_that_stopped_hitting_the_store_refuses_by_name` | |
| 8 | add, after the ratio push: `if !device_ratio.is_finite() { invalid.push("device_ratio is not finite".to_string()); }` | `a_zero_device_ceiling_yields_a_non_finite_ratio_left_for_the_renderer` | **report which assertion fired**: `invalid.len()` becomes 2, so the length assertion fires *before* the `"not finite"` one. Both are real; the ledger must not claim the "not finite" assertion was the one proven |
| 9 | move the `crosscheck` block below the applied-delta block | `the_write_half_pushes_its_reasons_in_a_fixed_order` | no data dependency, so it compiles |
| 10 | delete the `if let Some(reason) = &r.heartbeat { invalid.push(reason.clone()); }` block in `read_accounting` | `the_read_half_carries_its_heartbeat_reason_through` | leaves the field unread; still compiles |
| 11 | `w.ops as f64 / w.elapsed.as_secs_f64()` → `... / (w.elapsed.as_secs_f64() * 2.0)` | `a_healthy_write_half_publishes_and_flags_nothing` | |
| 12 | `r.reads as f64 / r.elapsed.as_secs_f64()` → `r.reads as f64 * 2.0 / r.elapsed.as_secs_f64()` | `a_healthy_read_half_publishes_and_flags_nothing` | |
| 13 | in the `hits != reads` message, `{hits} were hits` → `{reads} were hits` | `a_read_loop_that_stopped_hitting_the_store_refuses_by_name` | emits an `unused_variables` **warning** for `hits` and still compiles — the RED is behavioural. Do not "fix" the warning; that would void the mutation |

Every one of the twelve tests is named by at least one row (row 9's test and row 13's share their targets with rows 1/3 and 7 respectively). The three **moved** ratio tests are deliberately not re-mutated — they carry T5's own mutation evidence and are byte-identical to what shipped; say so in the report so the ledger does not read as if they were skipped.

Then confirm the file is byte-identical to where it started:

```bash
diff /tmp/accounting-sha-before.txt <(sha256sum crates/bench-harness/src/core_accounting.rs)
```

Expected: no output. **Report both halves** for every row — the mutation applied and the RED it produced, quoted from the actual test output. A row you could not make go red is a finding, not something to omit.

- [ ] **Step 4: Run the gates**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p bench-harness --all-targets --target x86_64-unknown-linux-gnu -- -D warnings
cargo test -p bench-harness 2>&1 | tee /tmp/bench-after-t2.log | grep -E '^test result:'
git diff --stat -- crates/charpente-core crates/charpente
```

Expected: clean; the `bench_harness` lib target is **42** (Task 1's 30 + 12); nothing prints for the Global Constraint.

- [ ] **Step 5: Commit**

```bash
git add crates/bench-harness/src/core_accounting.rs
git commit -m "bench: exhaustive accounting tests, teeth proven by mutation (interlude T2)

Twelve deterministic unit tests over the pure accounting — every case in the
interlude spec §4, both sides of every boundary, no benchmark run. The N-1
Critical is now a five-line assertion instead of something that needed real
hardware and --warmup 0 to surface; the read floor N-6 recorded as
unreachable through run() is reachable here.

These pin already-shipped behaviour, so they arrive passing and their teeth
are proven by mutation instead (spec §7): 13 mutations applied, each RED
captured, file restored byte-exact (sha256 verified). The invalid ordering is
pinned explicitly — a reordering was invisible to every other assertion and
visible in the artifact."
```

---

### Task 3: Prove zero behaviour change end to end, then close out

**Files:**
- Modify: `docs/superpowers/specs/2026-08-06-benchmark-harness-design.md` (one path citation)
- Add: `docs/superpowers/specs/2026-08-08-bench-accounting-split-design.md` (currently untracked)
- Add: `docs/superpowers/plans/2026-08-08-bench-accounting-split.md` (currently untracked)
- Modify: `.superpowers/sdd/2026-08-06-benchmark-harness/progress.md` (ledger, gitignored)

**Interfaces:**
- Consumes: the parent-commit sha from `/tmp/parent-sha.txt` (Task 1 Step 1).
- Produces: the close-out record. No code.

- [ ] **Step 1: The artifact-equivalence run (spec §9.4)**

Build the binary at the parent commit in an isolated worktree — another session may be working in the main tree, so do **not** check out the old commit in place. Use the session scratchpad, not `/tmp`, for the worktree.

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
SCRATCH=/private/tmp/claude-501/-Users-noahmitchem-Documents-GitHub-charpente/c687a231-95e3-4a5a-a875-d28320afd2fb/scratchpad
PARENT=$(cat /tmp/parent-sha.txt)
git worktree add "$SCRATCH/bench-parent" "$PARENT"
(cd "$SCRATCH/bench-parent" && cargo build -p bench-harness --bin bench)
cargo build -p bench-harness --bin bench
mkdir -p "$SCRATCH/bench-cmp/parent" "$SCRATCH/bench-cmp/head"
"$SCRATCH/bench-parent/target/debug/bench" core --duration 3 --warmup 1 --dir "$SCRATCH/bench-cmp/parent" --json > "$SCRATCH/bench-cmp/parent.json"
echo "parent exit: $?"
./target/debug/bench core --duration 3 --warmup 1 --dir "$SCRATCH/bench-cmp/head" --json > "$SCRATCH/bench-cmp/head.json"
echo "head exit: $?"
```

(No `2>/dev/null` on the build: a genuine failure must be visible, not swallowed. `mkdir -p` before the run matters — `bin/bench.rs` refuses a `--dir` that does not already exist, exit 2.)

Normalize away the measured numbers and diff the rest — field names, order, nesting, the `invalid` array's shape. **Define and use `norm` in one shell invocation**; shell functions do not survive between tool calls.

```bash
SCRATCH=/private/tmp/claude-501/-Users-noahmitchem-Documents-GitHub-charpente/c687a231-95e3-4a5a-a875-d28320afd2fb/scratchpad
norm() { python3 -c 'import re,sys; print(re.sub(r"-?\d+\.\d+|(?<=:)-?\d+", "N", sys.stdin.read()))' < "$1"; }
diff <(norm "$SCRATCH/bench-cmp/parent.json") <(norm "$SCRATCH/bench-cmp/head.json") && echo "ARTIFACTS EQUIVALENT"
```

Expected: `ARTIFACTS EQUIVALENT`, and the two exit codes equal.

**Two things to state honestly in the report rather than hide:**
- At `--duration 3` the ratio gate can legitimately fire on one run and not the other (that is C-1's gate working as designed on a short window), showing up as a differing `invalid` array. If that happens, re-run both a second time and report what was observed **both** times — do not silently keep the run that matched. If the arrays differ for any other reason, the refactor changed behaviour: stop and report.
- `norm` does not normalize bare integers that are not preceded by `:` — so a number inside a reason *string* survives. That is deliberate and harmless for the expected case (the ratio-gate reason formats all its numbers as floats and normalizes fully). It matters only for the applied-delta, crosscheck and heartbeat reasons, none of which should fire on a healthy run. A diff containing a bare integer means **a reason fired that should not have**, not that `norm` is broken.

Clean up:

```bash
SCRATCH=/private/tmp/claude-501/-Users-noahmitchem-Documents-GitHub-charpente/c687a231-95e3-4a5a-a875-d28320afd2fb/scratchpad
git worktree remove "$SCRATCH/bench-parent" --force
git worktree prune
rm -rf "$SCRATCH/bench-cmp"
```

- [ ] **Step 2: Verify exit criterion 1 mechanically**

Spec §9.1 is a mechanical criterion, so the check must be one — an empty result, not a list to triage. The module's own doc comments name `Db`, `Disk` and clocks in prose, so comment lines are excluded:

```bash
grep -nE 'Instant::now|SystemTime|\bDb\b|\bDisk\b|thread|std::fs|File' \
  crates/bench-harness/src/core_accounting.rs | grep -vE ':\s*(///|//!|//)'
```

Expected: **no output at all.** Any line is a spec §9.1 failure.

Then confirm the stronger form — that the file carries no blanket clippy allow, so the workspace deny-list is enforcing this rather than a grep:

```bash
grep -nE '^\s*#!?\[allow\(clippy::disallowed' crates/bench-harness/src/core_accounting.rs
```

Expected: **no output at all.** For contrast, every other module in the crate should match:

```bash
grep -lE '^\s*#!\[allow\(clippy::disallowed' crates/bench-harness/src/*.rs
```

Expected: `device.rs`, `lib.rs`, `core_bench.rs`, `measure.rs`, `report.rs` — five files, and
**not** `core_accounting.rs`.

**The check must be anchored to an attribute, not to the bare string** (corrected at T3; rev. 2
shipped `grep -c 'allow(clippy::disallowed'` expecting `0`). The module doc this plan itself
authors in Task 1 Step 2 *names* the allow in prose while explaining its absence — "this one
carries no blanket `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]`" — so the
unanchored count returns **1** on a correct file. That is the third verification command in
this plan's history to report failure on correct work, the exact class rev. 2's self-review
claimed to have eliminated. The real enforcement is neither grep: it is `cargo clippy -D
warnings` passing against the workspace deny-list, which these greps only document.

- [ ] **Step 3: Fix the doc drift this refactor causes (hard rule 6)**

`docs/superpowers/specs/2026-08-06-benchmark-harness-design.md:270` reads "(`core_bench::DEVICE_RATIO_SLACK`)". After Task 1 that path does not resolve — the constant lives in `core_accounting`. Edit that parenthetical to `` (`core_accounting::DEVICE_RATIO_SLACK`) ``.

Interlude spec §6's "no spec §5 amendment" forbids amending the parent spec's *report-fields* section; it does not license leaving a path citation false.

Two neighbouring citations were checked and are **still true** — record the judgment in the ledger rather than editing them:
- `:242` "applied uniformly by `measure::degenerate_window_reason` … to both of `core_bench::run`'s write and read loops" — still true through `run`, now via the accounting.
- `:279` "`core_bench::run`'s read loop counts hits separately from attempts and refuses" — `run` still returns the `Err`, so still true.

- [ ] **Step 4: The full gate sweep (spec §8)**

Pipe through `tee`, never `tail` — `tail` eats cargo's aggregate counts and replaces its exit status with its own. `cargo test --workspace` prints `test result:` once per binary, so watch for process exit, not for that line; it takes ~6 minutes, so run it in the background rather than against a 10-minute tool timeout.

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p bench-harness --all-targets --target x86_64-unknown-linux-gnu -- -D warnings
cargo build -p guide-examples
cargo test -p bench-harness 2>&1 | tee /tmp/sweep-bench.log | grep -E '^test result:'
cargo test -p bench-harness -- --ignored 2>&1 | tee /tmp/sweep-bench-ignored.log | grep -E '^test result:'
cargo test --workspace 2>&1 | tee /tmp/sweep-workspace.log | grep -E '^test result:|^error'
git diff --stat -- crates/charpente-core crates/charpente
```

Expected: every stage clean; the workspace total is **+12** against the pre-interlude baseline, stated as a number in the close-out rather than asserted vaguely. The `--ignored` run includes the N-1 disk regression that spec §7 keeps (6 tests) — it must still pass.

**CI's two `--release` seed replays are deliberately not run.** They exercise `charpente-sim`/`charpente` only, which the Global Constraint forbids this interlude from touching, so they cannot move. Say so in the close-out so the question is not reopened.

- [ ] **Step 5: Write the ledger entry**

Append to `.superpowers/sdd/2026-08-06-benchmark-harness/progress.md` an interlude section recording: the three commits; the eight-point ordering confirmation and the two stated residuals from Task 1 Step 8; the full 13-row mutation table with the RED each produced (and, for row 8, which assertion fired); the artifact-equivalence result including any ratio-gate caveat observed; the exit-criterion-1 grep result and the zero-allow confirmation; the sweep output with the +12 reconciliation; the `:242`/`:279` judgment from Step 3; the relocated tests' changed module paths; and — per this sub-project's own convention — anything found while landing that was not in the spec.

Also record what the two plan reviews caught before execution, since that is this sub-project's own evidence that reviewing a plan is worth its cost: 2 Critical (the mutation loop's `git checkout --` would have deleted the twelve tests it was testing; the test count was stated as 12 when rev. 1 had 13), plus the nested-seam ruling that deleted a test rev. 1 would have shipped as a guarantee it could not keep.

- [ ] **Step 6: Commit the spec, the plan and the doc fix**

```bash
git add docs/superpowers/specs/2026-08-08-bench-accounting-split-design.md \
        docs/superpowers/plans/2026-08-08-bench-accounting-split.md \
        docs/superpowers/specs/2026-08-06-benchmark-harness-design.md
git commit -m "bench: close the accounting/measuring interlude — spec, plan and evidence

Exit criteria, with evidence:
1. core_accounting.rs contains no clock read, no Db, no Disk, no IO — and
   carries no blanket clippy allow, so the workspace deny-list enforces that
   rather than a grep. Reachable from tests without running a benchmark.
2. Every case in spec §4 has a deterministic unit test, teeth proven by 13
   mutations with the RED captured for each.
3. The byte-exact JSON pin is unmoved, every pre-existing test passes
   unedited, and the invalid ordering is pinned explicitly (write half; the
   cross-half residual is recorded, not claimed).
4. A bench core run produces the same artifact as at the parent commit,
   diffed modulo the measured numbers themselves.

Also corrects the parent spec's DEVICE_RATIO_SLACK path citation, which this
refactor made false (hard rule 6).

Sets the seam convention T6 (kernel_bench) and T7 (wire) adopt from
inception, so they inherit a tested pure function instead of copying an
untested pattern."
```

- [ ] **Step 7: Report the state of the parent plan**

State plainly in your task report where the parent plan now stands: Tasks 1-5 landed, this interlude closed, **Task 6 (`kernel_bench.rs`) is next**, and it adopts this seam per spec §5 — raw observations struct, pure accounting, thin driver, and the ratio gate moves to `measure.rs` the moment T6 becomes its second consumer.

Carry forward the two items the T5 re-review deferred to T9, so they are not lost:
- **N-8:** nothing mechanically compares the bracket samples against the ratio's denominator. Observed live: denominator 129.23 against brackets 147.55/171.95 (15-30% low), and the only reason the reader was told is that the ratio happened to cross the gate.
- **Needs a ruling, not a re-review instruction:** moving the population loop *before* the write warmup costs the same wall time and removes the same bias from the write number (today a short run's `put` is an insert into a small tree, a long run's an update into a full one). Fsync swamps it, so it is tidiness not correctness.

---

## Self-Review (rev. 2)

**1. Spec coverage.** §1 the split → T1 Steps 2, 7; the nested halves satisfy §1's field list in aggregate and its "or the relevant subset" clause structurally. §2 short-circuit ordering → T1 Step 7 (write accounting called before the population loop) and Step 8's eight-point confirmation. §3.1 JSON pin unmoved → T1 Step 9's `git diff --stat` on `report.rs` + T3 Step 4. §3.2 no test edited → Global Constraints + T1 Step 5's byte-identical diff; T1 Step 6's comment-only change is called out with its reasoning. §3.3 ordering pinned → T2's `the_write_half_pushes_its_reasons_in_a_fixed_order`, with the cross-half residual stated rather than glossed. §4 all six bullets → T2 Step 1; bullet 3 (the ratio gate) is covered by the three tests moved verbatim in T1 Step 2, the only §4 bullet already satisfied before this interlude; bullet 4's "above the boundary" is `healthy()`, named in the fixture's own doc. §5 → T1's `Produces` block, the File Structure table's move-trigger note, and T3 Step 7. §6 non-goals → `device.rs`, `measure.rs` and `report.rs` are all asserted untouched by T1 Step 9's second `git diff --stat`. §7 tests + mutation → T2 Steps 1-3, with the pristine-copy restore; the `#[ignore]`d N-1 disk test stays and runs in T3 Step 4. §8 → Global Constraints. §9 → T3 Steps 1, 2, 4.

**2. Placeholder scan.** No TBDs. Every code step carries real code; every mutation names the exact edit, the test it must redden, and any warning it emits; every shell block is self-contained.

**3. Type consistency.** `WriteObs`/`ReadObs`/`CoreObservations` field names are identical in T1's `run` (`obs.write.ops`, `obs.write.loop_delta`, `obs.write.elapsed`, `obs.write.applied_before/after`, `obs.write.heartbeat`, `obs.write.device_ceiling`, `obs.write.device_before/after`, `obs.read.reads/hits/elapsed/heartbeat`, `obs.duration`) and in T2's `healthy()`. Both accounting functions take `(Duration, &half)` and return `Result<_, String>`; `run` maps with `io::Error::other`, matching the original's inline `io::Error::other(reason)`. Every struct field is `pub` in both the `Produces` block and the Step 2 code. `MIN_MEASURED_OPS`, `crosscheck` and `degenerate_window_reason` are `pub` in `measure.rs` today.

**4. What rev. 1 got wrong, so it is not reintroduced.** The count was 12-vs-13; the mutation restore destroyed its own work; deletions were keyed to line numbers that Step 4 had already invalidated; two verification commands reported failure on correct work; the deletions left rustfmt-illegal whitespace with no step to fix it; the new module copied a clippy allow that defeated its own exit criterion; one test had no mutation row; and a tracked parent-spec path citation was left to dangle. All are fixed above.
