# Compile latency: earning the number — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** produce one accepted compile-latency reading, publish it in `docs/BENCH.md`, and
move the two documents that currently assert the number does not exist.

**Architecture:** The instrument is finished and is not rebuilt here. Task 1 gives it an
exit-code contract so an unattended run can be told from a refused one; Task 2 wraps it in a
runner that encodes the retry rule; Task 3 is the run itself; Task 4 publishes; Task 5 reads
the number against the prebuilt-dylib premise. No task touches a gate constant.

**Tech Stack:** Rust, `bench-harness` (outside the deterministic boundary — hard rule 1 does
not apply to its measure half, and its accounting half carries no deny-list suppression so
it must stay free of clocks and IO), bash, macOS `caffeinate`.

**Spec:** `docs/superpowers/specs/2026-09-08-compile-latency-reading-design.md` — read it
before Task 1, especially §4, which is the integrity rule the whole plan exists to enforce.

## Global Constraints

- **Hard rule 5 — error messages are product surface.** Every refusal already names its
  rule; keep it that way. Write tests against error text.
- **Hard rule 6 — public-API changes update the docs in the same commit.** Task 4 publishes
  the number and moves both doc rows in ONE commit.
- **`docs/BENCH.md` is the only source of quotable performance numbers.** A figure in a
  ledger entry, a run artifact or a commit message is an observation until it lands there.
- **No gate may be adjusted.** `MIN_CHILD_CPU`, `CONTENTION_CEILING_PER_CORE`,
  `MAX_SPREAD_PERCENT`, `MIN_OBSERVATIONS` are frozen for this sub-project. A gate moves
  only when it contradicts its own stated reason (the 2026-09-07 precedent), never because
  it refused a reading.
- **The accounting half stays pure.** `compile_accounting.rs` may gain a `match` but never a
  clock, an env read, or IO. The `#![allow(clippy::disallowed_methods, …)]` at the top of
  `bin/compile-bench.rs` must NOT be copied into `compile_accounting.rs`.
- **Branch:** work directly on `main` (CLAUDE.md), `git add` **named paths only**, never
  `-A` or `.`.
- **This shell is zsh.** `${PIPESTATUS[0]}` expands to empty and reads as "no failure"; use
  `${pipestatus[1]}` or `set -o pipefail`. The runner script is `#!/usr/bin/env bash`, where
  `PIPESTATUS` works — but it deliberately uses no pipe at the point that matters.
- **Full check:** `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --
  -D warnings`, `cargo test --workspace --no-fail-fast`. Confirm green by COUNTING
  `grep -c "^test result: ok"`, never by the absence of `FAILED`.

## Ground truth established by survey (do not re-derive)

| fact | citation |
|---|---|
| `main()` prints `SUMMARY … REFUSED` and **always exits 0** | `crates/bench-harness/src/bin/compile-bench.rs:160-163` |
| the seven refusal variants | `crates/bench-harness/src/compile_accounting.rs:121-153` |
| per-observation gates, in order: `BuildFailed`, `Suspended`, `ChildCpuNotAggregated`, `BelowFloor`, `Contended` | `compile_accounting.rs:256-282` (`verdict`) |
| summary-level gates: `TooFewObservations`, `Unstable` | `compile_accounting.rs:307-325` (`summarize`) |
| `MIN_CHILD_CPU = 500ms`, `CONTENTION_CEILING_PER_CORE = 0.5`, `MIN_OBSERVATIONS = 3`, `MAX_SPREAD_PERCENT = 30` | `compile_accounting.rs:225,231,234,240` |
| `OBSERVATIONS = 3`, three phases (Clean / HandlerBody / Schema) | `bin/compile-bench.rs:33`, `:107-147` |
| the binary asserts `CARGO_TARGET_DIR` is set and refuses without it (currently a panic → exit 101) | `bin/compile-bench.rs:93-98` |
| `quiesce` polls to `0.4/core`, every 10s, max 300s | `bin/compile-bench.rs:45-47` |
| existing accounting unit-test helpers `healthy()` and `reading(n)` | `compile_accounting.rs:342,430` |
| BENCH's compile section spans lines 1506 to EOF (1602) | `docs/BENCH.md:1506` |
| PLAN's risk row | `PLAN.md:194` |
| DESIGN §9.5 | `DESIGN.md:1561` |
| script conventions: `#!/usr/bin/env bash`, `set -euo pipefail`, `cd "$(dirname "${BASH_SOURCE[0]}")/.."`, a doc header explaining WHY | `scripts/bench.sh`, `scripts/mutants.sh` |

---

### Task 1: The exit-code contract

Without this the runner cannot distinguish a green night from one where all nine
observations were refused — the binary prints the refusals and exits 0 either way. This is
the same defect class the 2026-09-07 entry named: *an absent measurement wearing the shape
of a passing one.*

**Files:**
- Modify: `crates/bench-harness/src/compile_accounting.rs` (add `Invalid::is_environmental`, add tests)
- Modify: `crates/bench-harness/src/bin/compile-bench.rs` (collect refusals, exit by contract)

**Interfaces:**
- Produces: `Invalid::is_environmental(&self) -> bool` — consumed by `bin/compile-bench.rs`
  and by nothing else. Exit contract consumed by Task 2's script.

- [ ] **Step 1: Write the failing test**

Append inside the existing `#[cfg(test)] mod tests` block in
`crates/bench-harness/src/compile_accounting.rs`:

```rust
    /// Spec §4. Retrying a refusal that describes the MACHINE is legitimate;
    /// retrying one that describes the MEASUREMENT is rerolling a result until
    /// it looks tight. The split is encoded here rather than left to a shell
    /// script's judgement at 3 a.m.
    #[test]
    fn only_refusals_about_the_machine_may_be_retried() {
        assert!(Invalid::Contended { load_before: 9.0, cores: 10 }.is_environmental());
        assert!(Invalid::Suspended("the box slept".into()).is_environmental());
        assert!(Invalid::TooFewObservations { got: 1 }.is_environmental());

        assert!(!Invalid::Unstable { spread_percent: 90 }.is_environmental());
        assert!(!Invalid::BelowFloor { child_cpu_millis: 10 }.is_environmental());
        assert!(!Invalid::BuildFailed.is_environmental());
        assert!(
            !Invalid::ChildCpuNotAggregated { child_millis: 1, parent_millis: 2 }
                .is_environmental()
        );
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p bench-harness only_refusals_about_the_machine_may_be_retried`
Expected: FAIL to compile — `no method named `is_environmental` found for enum `Invalid``.

A compile failure is acceptable as the RED here ONLY because the method does not exist yet;
the assertions themselves must be meaningful once it does. Do not proceed until Step 4
turns this green on assertions rather than on the method merely existing.

- [ ] **Step 3: Write the minimal implementation**

In `crates/bench-harness/src/compile_accounting.rs`, immediately after the
`impl std::fmt::Display for Invalid` block:

```rust
impl Invalid {
    /// Whether this refusal describes the MACHINE rather than the measurement.
    /// Only these may be retried (spec §4): re-running after a busy or
    /// suspended box is legitimate, while re-running after `Unstable` is
    /// rerolling until the spread looks tight, and re-running after
    /// `BelowFloor`, `BuildFailed` or `ChildCpuNotAggregated` hides a defect
    /// instead of reporting it.
    ///
    /// `TooFewObservations` is environmental only in that it is always
    /// downstream of per-observation refusals. The driver therefore classifies
    /// a phase by ALL of its refusals, not by the summary's alone — otherwise a
    /// `BelowFloor` observation would be laundered into a retryable verdict.
    pub fn is_environmental(&self) -> bool {
        match self {
            Invalid::Contended { .. }
            | Invalid::Suspended(_)
            | Invalid::TooFewObservations { .. } => true,
            Invalid::BuildFailed
            | Invalid::ChildCpuNotAggregated { .. }
            | Invalid::BelowFloor { .. }
            | Invalid::Unstable { .. } => false,
        }
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p bench-harness only_refusals_about_the_machine_may_be_retried`
Expected: PASS.

- [ ] **Step 5: Replace the `CARGO_TARGET_DIR` panic with the documented usage code**

In `crates/bench-harness/src/bin/compile-bench.rs`, replace the `assert!` at `:93-98` with:

```rust
    if std::env::var("CARGO_TARGET_DIR").is_err() {
        eprintln!(
            "compile-bench: refusing to run without a dedicated CARGO_TARGET_DIR: the clean \
             phase deletes artifacts, and sharing the repo's target/ would destroy the \
             working cache and let unrelated cargo runs perturb the measurement"
        );
        std::process::exit(2);
    }
```

- [ ] **Step 6: Collect refusals per phase and exit by contract**

In `crates/bench-harness/src/bin/compile-bench.rs`:

Change `record`'s signature and body to also collect the refusal:

```rust
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
```

Add `Invalid` to the import line:

```rust
use bench_harness::compile_accounting::{Cache, Invalid, Phase, Reading, summarize, verdict};
```

Declare one refusal vector per phase, immediately after the three reading vectors at
`:103-105`:

```rust
    let mut clean_readings = Vec::new();
    let mut handler_readings = Vec::new();
    let mut schema_readings = Vec::new();
    let mut clean_refusals: Vec<Invalid> = Vec::new();
    let mut handler_refusals: Vec<Invalid> = Vec::new();
    let mut schema_refusals: Vec<Invalid> = Vec::new();
```

Pass each to its own `record` call — the three call sites become:

```rust
        record(
            &mut clean_readings,
            &mut clean_refusals,
            measure_build(&app, Phase::Clean, Cache::Cold).expect("clean build"),
            i,
            "clean",
        );
```

```rust
            record(
                &mut handler_readings,
                &mut handler_refusals,
                measure_build(&app, Phase::HandlerBody, Cache::Warm).expect("handler build"),
                i,
                "handler",
            );
```

```rust
            record(
                &mut schema_readings,
                &mut schema_refusals,
                measure_build(&app, Phase::Schema, Cache::Warm).expect("schema build"),
                i,
                "schema",
            );
```

Replace the final summary loop with:

```rust
    let mut any_refused = false;
    let mut any_final = false;

    for (label, phase, readings, refusals) in [
        ("clean", Phase::Clean, &clean_readings, &clean_refusals),
        ("handler-body", Phase::HandlerBody, &handler_readings, &handler_refusals),
        ("schema", Phase::Schema, &schema_readings, &schema_refusals),
    ] {
        match summarize(phase, readings) {
            Ok(s) => println!(
                "SUMMARY {label}: n={} median_wall={:?} fastest={:?} slowest={:?} \
                 median_cpu={:?} spread={}%",
                s.n, s.median_wall, s.fastest_wall, s.slowest_wall, s.median_cpu, s.spread_percent
            ),
            Err(e) => {
                println!("SUMMARY {label}: REFUSED — {e}");
                any_refused = true;
                // A phase is classified by ALL its refusals, so a BelowFloor
                // observation cannot hide behind a TooFewObservations summary.
                if !e.is_environmental() || refusals.iter().any(|r| !r.is_environmental()) {
                    any_final = true;
                }
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
    println!(
        "VERDICT exit={code} (0 accepted, 1 refused-retryable, 2 usage, 3 refused-final)"
    );
    std::process::exit(code);
```

- [ ] **Step 7: Verify the binary compiles and the contract is stated**

Run: `cargo build -p bench-harness --bin compile-bench`
Expected: builds clean.

Run: `cargo run -p bench-harness --bin compile-bench` (with `CARGO_TARGET_DIR` unset)
Expected: prints the refusal message and exits 2. Confirm with `echo $?` on its own line —
**not** after a pipe, which would report the filter's status instead.

- [ ] **Step 8: Full check and commit**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p bench-harness --no-fail-fast
git add crates/bench-harness/src/compile_accounting.rs crates/bench-harness/src/bin/compile-bench.rs
git commit -m "feat(bench): compile-bench gets an exit-code contract

It printed REFUSED and exited 0, so an unattended run could not tell a
green night from one where all nine observations were refused — an absent
measurement wearing the shape of a passing one.

0 accepted, 1 refused-retryable, 2 usage, 3 refused-final. The retryable/
final split is Invalid::is_environmental, encoded next to the variants and
unit-tested rather than left to a shell script: retrying Contended or
Suspended is legitimate, retrying Unstable is rerolling until the spread
looks tight. A phase is classified by all its refusals, so a BelowFloor
observation cannot launder itself into a retryable TooFewObservations."
```

---

### Task 2: `scripts/compile-bench.sh` — the unattended runner

**Files:**
- Create: `scripts/compile-bench.sh`

**Interfaces:**
- Consumes: Task 1's exit contract (0/1/2/3).
- Produces: `target/compile-bench/<timestamp>/run.log` and `.../status` per attempt.

- [ ] **Step 1: Write the script**

Create `scripts/compile-bench.sh`:

```bash
#!/usr/bin/env bash
# Takes the compile-latency reading unattended (spec
# docs/superpowers/specs/2026-09-08-compile-latency-reading-design.md).
#
# Why a runner exists at all: the instrument is finished, but the reading needs
# a quiet machine, and the quietest this box gets is while nobody is using it.
# That is also when macOS schedules Spotlight and Time Machine, so "unattended"
# is not automatically "quiet" — every attempt records its load trace so a lost
# night is diagnosable rather than mysterious.
#
# The retry rule is spec §4 and is NOT this script's judgement call: the binary
# classifies its own refusals and says so in its exit code.
#   0 accepted        -> stop, we have the reading
#   1 refused (env)   -> retry; the box was busy or slept
#   2 usage/launch    -> stop, operator error
#   3 refused (final) -> stop and REPORT; Unstable/BelowFloor/BuildFailed/
#                        ChildCpuNotAggregated describe the measurement, and
#                        rerolling them is how a reading gets manufactured
#
# caffeinate is belt and Invalid::Suspended is braces. Prevention exists so the
# night is not wasted, never so the gate is bypassed. Both stay armed.
#
#   scripts/compile-bench.sh                 # start now
#   scripts/compile-bench.sh --delay 14400   # start in 4 hours
#
# Detach it, because it runs for hours:
#   nohup scripts/compile-bench.sh --delay 14400 > /dev/null 2>&1 &
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

ATTEMPTS="${ATTEMPTS:-4}"
GAP_SECONDS="${GAP_SECONDS:-1800}"
# Outside the repo on purpose: the clean phase deletes artifacts, and sharing
# the repo's target/ would destroy the working cache and let an unrelated cargo
# perturb the measurement. Never point this at a worktree's target either — a
# shared CARGO_TARGET_DIR lets one tree's artifacts leak into another's
# verification with `git diff` clean.
BENCH_TARGET_DIR="${COMPILE_BENCH_TARGET_DIR:-${TMPDIR:-/tmp}/charpente-compile-bench-target}"
OUT_ROOT="target/compile-bench"

if [[ "${1:-}" == "--delay" ]]; then
  echo "compile-bench: sleeping ${2}s before the first attempt" >&2
  sleep "$2"
  shift 2
fi

# Pre-flight: our own cargo is not running yet, so anything found here is
# somebody else's and would show up as contention we cannot attribute.
if pgrep -x cargo >/dev/null || pgrep -x rustc >/dev/null; then
  echo "compile-bench: refusing to start — a cargo or rustc is already running" >&2
  exit 2
fi

# Build the harness BEFORE the measured window so its own compile is not part
# of the load the first observation samples.
cargo build --release -p bench-harness --bin compile-bench
BIN="target/release/compile-bench"

for attempt in $(seq 1 "$ATTEMPTS"); do
  ts="$(date +%Y%m%dT%H%M%S)"
  out="$OUT_ROOT/$ts"
  mkdir -p "$out"
  echo "compile-bench: attempt $attempt/$ATTEMPTS -> $out/run.log" >&2

  # No pipe here on purpose: $? after a pipe reads the filter, not the binary.
  set +e
  CARGO_TARGET_DIR="$BENCH_TARGET_DIR" caffeinate -i "$BIN" > "$out/run.log" 2>&1
  status=$?
  set -e
  echo "$status" > "$out/status"

  case "$status" in
    0) echo "compile-bench: attempt $attempt ACCEPTED — $out/run.log" >&2; exit 0 ;;
    1) echo "compile-bench: attempt $attempt refused (environment) — retrying" >&2 ;;
    3) echo "compile-bench: attempt $attempt refused (measurement) — NOT retrying, this is a finding" >&2; exit 3 ;;
    *) echo "compile-bench: attempt $attempt launch fault (exit $status)" >&2; exit 2 ;;
  esac

  if [[ "$attempt" -lt "$ATTEMPTS" ]]; then
    sleep "$GAP_SECONDS"
  fi
done

echo "compile-bench: $ATTEMPTS attempts, all refused on environment — no reading earned" >&2
exit 1
```

- [ ] **Step 2: Make it executable and verify the pre-flight refusal fires**

```bash
chmod +x scripts/compile-bench.sh
```

Run, while a `cargo build` is deliberately running in another terminal:
`scripts/compile-bench.sh`
Expected: `refusing to start — a cargo or rustc is already running`, exit 2. Check with a
bare `echo $?`.

This is the one step that proves the guard fires rather than assuming it. A guard nobody has
seen refuse is a guard nobody knows works.

- [ ] **Step 3: Commit**

```bash
git add scripts/compile-bench.sh
git commit -m "feat(bench): the unattended compile-latency runner

Encodes spec §4's retry rule by reading the binary's exit contract rather
than parsing its prose: retry only exit 1 (environment), stop and report on
exit 3 (measurement). Dedicated CARGO_TARGET_DIR outside the repo,
caffeinate so the night is not lost to sleep, a pre-flight refusal when
another cargo is live, and no pipe at the point where \$? is read."
```

---

### Task 3: Take the reading

This task is a run, not an edit. Its deliverable is an artifact plus a verdict.

**Files:**
- Produces: `target/compile-bench/<timestamp>/run.log` (untracked)

- [ ] **Step 1: Arm the shot**

Decide a start time when the box will be idle, convert to seconds, and detach:

```bash
nohup scripts/compile-bench.sh --delay 14400 > target/compile-bench-runner.log 2>&1 &
```

- [ ] **Step 2: Collect the outcome the next morning**

```bash
cat target/compile-bench-runner.log
for d in target/compile-bench/*/; do echo "$d -> $(cat "$d/status" 2>/dev/null)"; done
```

- [ ] **Step 3: Read the artifact and classify**

- **Exit 0** → proceed to Task 4. Record the `SUMMARY` line for all three phases plus every
  `OBS`/`NOOP` line; they are the conditioning that must travel with the number.
- **Exit 1 after all attempts** → the machine could not produce the reading. This is
  publishable as a finding in its own right (spec §7). Re-arm on another night, or take the
  refusals to Noah with their load traces. **Do not touch a gate.**
- **Exit 3** → a measurement defect. Stop, and open it as a finding with the refusing
  variant named. Do not re-run hoping for a different answer.

- [ ] **Step 4: Record the attempt in the ledger regardless of outcome**

Append a `docs/LEDGER.md` entry naming the attempt count, the exit code, and — if refused —
the load traces. A refused night is evidence, and an unrecorded one gets re-litigated.

```bash
git add docs/LEDGER.md
git commit -m "docs(ledger): compile-latency reading, attempt <N> — <outcome>"
```

---

### Task 4: Publish the number, and move both documents

Do this ONLY after Task 3 exits 0. Hard rule 6 makes the publish and the two doc rows one
commit — a number in BENCH with PLAN still saying "unmeasured" is drift shipped deliberately.

**Files:**
- Modify: `docs/BENCH.md:1506` to EOF (replace the "the reading is refused" section)
- Modify: `PLAN.md:194` (the risk row)
- Modify: `DESIGN.md:1561` (§9.5)

- [ ] **Step 1: Replace BENCH's compile-latency section**

Retitle from `## Compile latency — the instrument exists, the reading is refused
(2026-09-07)` to `## Compile latency (2026-09-08)`. Keep the existing "What the instrument
measures" subsection verbatim — it is still accurate and explains why the three phases are
not interchangeable. Add, with real values from the artifact:

- a table of median wall, median CPU, fastest, slowest and spread per phase;
- the machine: model, core count, and that this is a laptop rather than a server;
- the attempt count and every refusal from every night, per spec §4;
- the standing caveat that `Clean` is not "the compile time" — quoting one phase as the
  headline is the error the three-phase split exists to prevent.

- [ ] **Step 2: Move PLAN's risk row**

`PLAN.md:194` — replace `The bet accepted going in; **still unmeasured**` with the read the
number supports, and replace the trailing sentence *"This row's read stays 'accepted bet'
because no number has yet been earned to move it."* with a pointer to the BENCH section.
Keep the "did not happen in M0" admission: it is history, not status.

- [ ] **Step 3: Move DESIGN §9.5**

`DESIGN.md:1561` — replace **"Still unquantified as of 2026-09-07"** and the sentence
*"nobody has measured which phase actually hurts"* with what was measured. Leave the
prebuilt-dylib item itself open; Task 5 decides it.

- [ ] **Step 4: Verify the docs still build**

Run: `cargo build -p guide-examples`
Expected: builds clean (it compiles every ```rust block in docs/GUIDE.md; BENCH/PLAN/DESIGN
carry none, so this is a cheap regression check rather than a no-op).

- [ ] **Step 5: Commit**

```bash
git add docs/BENCH.md PLAN.md DESIGN.md
git commit -m "docs(bench): the compile-latency reading, and two rows that said it did not exist

<one line per phase: median wall / median cpu>

Earned on attempt <N>; every refusal and its load trace is in the section.
PLAN's risk row and DESIGN §9.5 both asserted the absence of this number
in as many words, so hard rule 6 makes them the same commit."
```

---

### Task 5: Read the number against the prebuilt-dylib premise

**Files:**
- Modify: `docs/LEDGER.md` (the verdict)
- Modify: `DECISIONS.md` ONLY if the verdict contradicts an existing entry — in which case
  stop and take it to Noah with the evidence instead of applying it.

- [ ] **Step 1: State the question before looking at the answer**

DESIGN §9.5's premise is that a prebuilt platform dylib is the right mitigation. It is
aimed at whichever phase dominates. Write down, before reading the table, what each outcome
would mean:

- **Handler-body slow** → the inner loop hurts; the dylib is aimed correctly.
- **Handler-body fast, clean/schema slow** → the mitigation targets a phase an agent meets
  rarely, and the premise is wrong even though the number is real.
- **All three fast** → the risk row was over-weighted and can be downgraded on evidence.

Committing to the reading in advance is what stops the number being narrated into whichever
conclusion was already preferred.

- [ ] **Step 2: Write the verdict into the ledger**

Name which phase dominates, what that says about the dylib, and what it does NOT say. One
reading on one laptop does not settle a cross-platform claim — say so explicitly.

- [ ] **Step 3: Commit**

```bash
git add docs/LEDGER.md
git commit -m "docs(ledger): what the compile-latency number says about the dylib premise"
```

- [ ] **Step 4: Stop**

Building the dylib, cranelift hot reload, and any performance work the number suggests are
separate sub-projects that this one's output authorizes. Do not start them here.
