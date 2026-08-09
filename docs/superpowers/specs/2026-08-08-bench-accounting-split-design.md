# M2 sub-project 1 interlude — separating the accounting from the measuring

Spec for the bench-harness refactor ruled by Noah on 2026-08-08, mid-plan, between Task 5
and Task 6. Parent spec: `docs/superpowers/specs/2026-08-06-benchmark-harness-design.md`.
Parent plan: `docs/superpowers/plans/2026-08-06-benchmark-harness.md`.

## Context

Task 5 shipped `core_bench.rs` and then took two review rounds and two fix rounds. The
defects that survived into shipped code, and the way each was eventually caught, have one
thing in common:

- **C-1** — the harness published `1.09x device`, a commit path acking faster than the
  hardware can durably sync. Found by *running the binary and reading the output*.
- **N-1 (Critical)** — `last_applied()` returns `None` on a fresh `Db` and `unwrap_or(0)`
  read that as "record 0 was applied", so `--warmup 0` published a `log_records_per_sec`
  one record low *and* raised a named INVALID accusing charpente's commit path of breaking
  a contract that did not break (hard rule 5). Found by *running the binary with an unusual
  flag*.
- **C-2 / N-6** — assertions that could not fail. Found by *mutating the source and
  watching the test stay green*.

None was found by reading the diff. That is not a reviewer failing; it is structural. Every
other subsystem in charpente has the simulator as its oracle — a wrong implementation is
caught by a property test under fault injection, with a seed to replay it. A benchmark
measures wall-clock performance on real hardware and is *definitionally* outside the
deterministic core (DESIGN §1), so the simulator cannot help. The only oracle available is
a human or agent looking at a number and judging it implausible.

But that is only true of *half* the code. `core_bench::run` fuses two kinds of logic:

- **Measuring** — loops driven by `Instant::now()` against real fsyncs, real `Db::transact`,
  real `Heartbeat` threads. Irreducibly nondeterministic. No oracle exists.
- **Accounting** — delta arithmetic, the degenerate-window floor, the `device_ratio` gate,
  the `hits == reads` invariant, `applied_delta` reconciliation, `crosscheck`, every rate
  division, and the assembly of `CoreReport` plus its `Vec<String>` of invalidity reasons.
  This is pure logic that happens to live inside the measurement loop.

Because they are fused, the only way to exercise the accounting is to run a real benchmark.
That is why N-1 required real hardware and a `--warmup 0` invocation to surface, when it is
in fact a two-line total-function bug over `(Option<u64>, Option<u64>)` — a case a table of
unit tests would have covered in seconds.

This is charpente's own central design principle, unapplied to the one crate that violates
it: put the nondeterminism behind a boundary so the logic is deterministic and testable.

## 1. The split

`core_bench.rs` separates into two layers with an explicit data seam.

**`CoreObservations`** — a plain struct of raw observations. Every field is a count or a
`Duration` that was actually observed. Nothing in it is derived, and nothing in it is a
decision. It carries at least: the write op count, the write loop-counter delta (the
crosscheck's second source), the write window's elapsed time, `applied_before` and
`applied_after` as `Option<u64>`, the read count, the hit count, the read window's elapsed
time, the device ceiling, both bracket samples, and both heartbeat verdicts.

**The accounting functions** — pure, taking `&CoreObservations` (or the relevant subset) and
returning the published values plus invalidity reasons. No `Instant::now()`, no `Db`, no
`Disk`, no thread, no IO of any kind. Every guard, every delta, every rate division and
every INVALID decision lives here.

**`run`** keeps the loops, the `Db`, the `Heartbeat`s and the bracket samples, fills a
`CoreObservations`, and calls the accounting. It makes no arithmetic decision of its own.

## 2. Short-circuit ordering is behaviour, and it is preserved

A single end-of-run tally is **wrong** and must not be built. Today the write window's
degenerate-window refusal returns `Err` *before* the population loop runs, and population
can cost seconds of fsyncs. Collapsing every check into one call at the end would make a
refused run pay for work it is about to discard, changing observable behaviour and runtime.

So the accounting is called at the points `run` currently makes each decision — the write
accounting after the write window and before population, the read accounting after the read
window — and each may refuse. The seam is about *where the logic lives*, not about deferring
when it runs. An implementation that preserves the pure/impure split while changing the
order in which refusals fire has failed this spec.

## 3. Zero behaviour change — and how that is proved

This is a refactor. Not one published number, reason string, exit code, or ordering may
change. Three mechanical proofs, all required:

1. **The byte-exact JSON pin does not move.** `report::json_shape_is_pinned` is untouched
   and green. If the pin string needs editing, the refactor changed behaviour and is wrong.
2. **Every existing test passes unchanged.** No test file may be edited to accommodate the
   refactor. New tests are added; existing ones are not adjusted. (`tests/harness.rs` and
   `bin/bench.rs`'s test module may gain tests; their existing assertions stay byte-identical.)
3. **The `invalid` reason ordering is preserved.** Reasons are pushed in a fixed order today
   and both renderers derive from the same list; a reordering would be invisible to most
   assertions and visible in the artifact. Pin the order explicitly if no existing test does.

## 4. What the split must make testable, at minimum

Each of these is a defect this sub-project actually shipped or nearly shipped, and each
becomes a deterministic unit test with no benchmark run:

- `applied_before: None` with `applied_after: Some(a)` yields a delta of `a + 1` (N-1, the
  Critical). Cover all four `(Option, Option)` combinations, including the `(Some, None)`
  case currently asserted `unreachable!`.
- `applied_delta != write_ops` raises the named INVALID — and does **not** raise it when they
  agree.
- `device_ratio` above `1.0 + DEVICE_RATIO_SLACK` is flagged; at and below the boundary it is
  not (ruling #4's 1e-6 boundary, both sides).
- The degenerate-window floor (`MIN_MEASURED_OPS`) at, above and below the boundary, for both
  windows.
- `hits != reads` refuses by name; `hits == reads` does not.
- A zero device ceiling produces a non-finite ratio that flows to `report.rs`'s central
  `collect_non_finite` **unmodified** — the accounting must not grow its own non-finite check
  (Task 3's standing note).

## 5. The convention this sets for T6 onward

Task 6 (`kernel_bench.rs`) and Task 7 (`wire`) add layers with the same shape. They adopt
this seam: raw observations struct, pure accounting, thin driver. The point of doing this
between T5 and T6 rather than after is that T6 is about to duplicate `core_bench`'s
accounting rather than share it — extracting now means T6 inherits a tested pure function
instead of copying an untested pattern.

Where the layers genuinely share accounting (the degenerate floor and the ratio gate already
are shared, in `measure` and `core_bench` respectively), the shared function moves to
`measure.rs` rather than being copied. A third copy of any accounting rule is the signal that
it belonged in `measure.rs` one layer earlier.

## 6. Non-goals

- **No change to what is measured, or how.** The loops, their durations, the bracket samples,
  the population loop and the canonical transaction are untouched. This spec moves logic; it
  does not re-open any measurement decision.
- **No retrofit of `device.rs`.** Its accounting is already nearly pure (`timed_loop` returns
  a rate; the floor is a shared function). It adopts the convention if and when it is next
  touched for another reason — not here, because a refactor with no defect behind it is how a
  refactor introduces one.
- **Not the fix for C-1's class.** Cross-window sampling variance producing a ratio above 1.0
  is a measurement-methodology problem. No amount of pure-function testing reveals it; only
  running the binary does. This spec shrinks the class of defects that need real hardware to
  find — it does not eliminate it, and the review convention of running the binary before
  reading the diff stays in force.
- **No new report fields, no spec §5 amendment.** If the refactor appears to need either, it
  has changed behaviour — see §3.

## 7. Tests

- Exhaustive unit tests over the accounting functions, covering every case in §4 including
  both sides of every boundary.
- Hard rule 3 applies with the sharpening this sub-project earned: the accounting tests pin
  *already-shipped* behaviour, so they arrive passing and **their teeth are proven by
  mutation** — mutate the accounting, capture the real RED, restore byte-exact (sha256sum both
  sides), report both halves. A compile error is not a RED for an assertion (the T5 finding:
  "novelty is not teeth"). This sub-project has found five assertions that could not fail; a
  refactor whose whole purpose is testability must not add a sixth.
- The N-1 regression test added in fix round 2 (`#[ignore]`, real disk IO) **stays**, and its
  pure-function twin is added beside the accounting. Keeping both is deliberate: the pure test
  proves the arithmetic, the disk test proves the wiring that hands it the arithmetic's inputs.

## 8. Non-negotiables carried from the parent plan

- The Global Constraint: `git diff --stat -- crates/charpente-core crates/charpente` empty.
- Gates: `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings`;
  the same for `-p bench-harness --target x86_64-unknown-linux-gnu`; `cargo test -p
  bench-harness` and `-- --ignored`; `cargo test --workspace`, reconciled against the
  then-current baseline.
- `bench:` commit prefix, direct to main, named paths only, no Co-Authored-By.

## 9. Exit criteria

1. `core_bench.rs`'s accounting contains no `Instant::now()`, no `Db`, no `Disk`, no IO, and
   is reachable from tests without running a benchmark.
2. Every case in §4 has a deterministic unit test, each with its teeth proven by mutation.
3. The byte-exact JSON pin is unmoved, every pre-existing test passes unedited, and the
   `invalid` ordering is pinned.
4. A `bench core` run produces the same artifact as it did at the parent commit — verified by
   running both and diffing the JSON modulo the measured numbers themselves.
