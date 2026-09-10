# Autonomous subagent run: design

## Context

Noah wants a long unattended window in which subagents implement, review and verify
charpente work with no human in the loop, designed once up front and then left to run.
This document is that design. It is a *process* spec, not a subsystem spec — the
precedent for one living here is `2026-08-14-agent-skills-design.md`.

The repo is unusually well suited to unattended work, because dense mechanical oracles
are literally the product (MANIFESTO §2). It is also, on its own record, unusually
hostile to careless delegation. Every failure mode below is one this repo has already
paid for, and the design is shaped around them rather than around a generic
orchestration ideal.

The starting state: main is clean and 9 commits ahead of `origin/main`; one worktree
(the main dir); nothing in `.superpowers/sdd/` mid-flight except the stalled
agent-skills sub-project, which §2 removes from scope.

## §1 Rulings (Noah, 2026-09-09)

1. **Harness-first was proposed and then withdrawn.** The original plan opened by
   building `.claude/agents/` personas and a dispatch skill. That was withdrawn on
   discovering `2026-08-14-agent-skills-design.md`, whose §Scope rulings it would have
   violated on two counts: the Iron Law that a skill whose baseline does not fail is not
   written, and that personas ship only after beating a `general-purpose` control at the
   same model with the same preloaded skills.
2. **Work splits by trust boundary.** Decided doc changes commit to main. Code changes
   happen in an isolated worktree on a branch, reviewed by Noah on return.
3. **Blocked work proceeds under a logged assumption**, recorded with its reasoning and
   what would falsify it. The single exception: anything contradicting DECISIONS.md stops
   that task dead, per CLAUDE.md's workflow rule.
4. **Orchestration is phase-gated hybrid** — sequential between phases, parallel within a
   phase where tasks are genuinely independent, with a controller-verified commit at every
   phase boundary.
5. **The agent-skills sub-project is out of scope except for its mechanical half.** T3's
   baseline *collection* runs unattended; T3's *scoring* waits for Noah.

## §2 Why the agent-skills sub-project is out of scope

It is the highest-judgment work in the backlog and the worst candidate for an unattended
run. Its status as of 2026-08-15:

| Task | Outcome |
|---|---|
| T1 — CLAUDE.md split | Complete (1341 → 120 lines; byte-exactness derived three times) |
| T2 — `charpente-sim-properties` | No skill. Premise evidenced, gate undischarged |
| T3 — `charpente-adversarial-review` | Unstarted. Baselines collected, never scored |
| T4 — `charpente-evidence-sweep` | Complete — documented drop |
| T5 — `charpente-doc-truth` | Complete — documented drop |
| Personas (§3 of that spec) | Never reached; gated behind the skills |

Three drops out of four is the RED-first gate working, not the sub-project failing. But
the measuring instrument was found defective five separate times, the headline case being
that two baseline sets ran in a workspace containing their own grading rubric — caught by
the T4 scorer, not by the controller. That sub-project's own verdict on the class is the
reason it is excluded here: every finding "was caught by a human holding the line... None
was caught by a mechanism."

**The transferable rule it produced, which Phase 2 below obeys:** an evaluation scenario
must not run in a workspace that contains the evaluation. Pre-registering criteria
protects against the scorer moving the target; it does nothing if the subject can read
them. The two protections are independent.

## §3 Phases

Ordering is forced by real dependencies and by machine contention, not by preference.

### Phase 0 — Land the dylib decision (controller, main, ~30 min)

Noah ruled on 2026-09-09 to **drop** the prebuilt-platform-dylib item rather than re-scope
it. The evidence: its stated mechanism ("app iteration recompiles app code only") is
already delivered by cargo — 9 of 9 incremental observations printed exactly one
`Compiling` line and every one was `invoicing` — and its residual headroom is the
once-per-checkout clean build, which is not what the M2 bullet justified it on.

- `PLAN.md:184` M2 bullet, and the `:194` risk row's Mitigation column.
- `DESIGN.md` §9 item 5 — the "prebuilt-dylib item itself stays open" sentence resolves.
- A dated `docs/LEDGER.md` entry carrying the decision and its reasoning.
- Gate: `cargo test -p guide-examples` counted green; tree clean; then push all commits.

The LEDGER entry may record the Amdahl-style bound on the inner loop's serial (link)
portion — `C <= P(w-s) + s` gives s <= 309 ms at a 12-core ceiling and s <= 187 ms at the
6.62x parallelism this run actually observed — **explicitly labeled as arithmetic over
published figures, not a measurement.** It does not go in BENCH.md, which is the only
source of quotable numbers and which states that its table sizes no mitigation.

### Phase 1 — The two open defects (one worktree per defect)

Two implementers in parallel. They touch different code and cannot conflict in *content*,
but they must not share infrastructure: **each defect gets its own worktree, its own branch,
and its own `CARGO_TARGET_DIR` outside the repo.** Two agents building in one target
directory is the hazard §5 names — a concurrent cargo blocks on the target-dir lock, and
the wait lands in wall time while everything else looks normal.

Each implementer is followed by a *different* verifier that re-runs from scratch rather
than reading the implementer's report.

- **F1 — the panic-hook false alarm.** The July `catch_unwind` fence around `Store::open`
  (redb `page_manager` :231/:257, commit `1c439d5`) lets the default panic hook print
  `panicked at ...page_manager.rs` before the panic is caught: 134 such lines on a fully
  green 10k-universe batch. A docs-only agent read them as failure and discarded a correct
  full-App-stack property; a later hunt's controller was caught by the same trace. Repro:
  master seed `0x12124e6ebd9cf8b9`. Fix per the ledger: silence the hook for that scope, or
  print one named line. Its oracle is an assertion over *output* — zero unnamed
  `page_manager.rs` traces on a green batch — which is hard rule 5 ("write tests against
  error output"), not a sim property.
- **Reverse scope-flip gap.** The named sibling of the bug fixed in `14863db`: the same
  class in the opposite direction, named only in `scan_raw`'s unscoped arm via
  `scan_raw_shape_error`. It wants its own failing test rather than being folded in behind
  the other's green.

Neither branch is pushed; GHA minutes are limited and Noah reviews both on return. The
branches are left unmerged — merging them is Noah's call, and a merge inside an unattended
run would put unreviewed code on main.

### Phase 2 — T3 adversarial-review baseline collection (mechanical only)

Collect the baselines the stalled T3 never scored. Runs in worktrees detached at
`ee17b8c`, which predates the plan file and is therefore clean of the rubric — the §2
contamination rule, applied deliberately this time rather than by accident.

**Scoring is explicitly out of scope.** Transcripts are archived to
`.superpowers/sdd/2026-08-14-agent-skills/` with a note recording that they are unscored
and why. Whoever scores T3 must read `task-3-baseline-b-COMPLETED.md`, not the truncated
placeholder — a hazard that sub-project already recorded.

### Phase 3 — Mutation sweep (the graceful tail)

`scripts/mutants.sh` over the default named oracle-bearing target set, in its own worktree
against main with its own `CARGO_TARGET_DIR`, last, when nothing else is competing for the
machine. `cargo-mutants 27.1.0` is installed and matches the pin.

Verdicts are mechanical: **MISSED** means no oracle noticed, i.e. a one-sided property. A
**timeout is not a catch**. The verb's own output is authoritative, never cargo-mutants'
exit code — the raw tool exits 0 when a filter matches nothing, and a failed baseline
leaves every counter reading zero.

Findings are appended to a file as they are found, so whatever the window allows is useful
and the phase can be cut off the moment Noah returns. Fixes are **not** applied here;
a MISSED finding is a report, because fixing a one-sided property is design work.

## §4 The phase-boundary gate

Run by the controller at every boundary, never read from a report:

1. `git status --porcelain` empty in the relevant tree.
2. HEAD sha matches what the agent claimed it committed.
3. The verification re-run by the controller, with the **expected count stated before
   running**, then counted from the output. A `grep` matching nothing exits 1, so under
   `pipefail` "nothing failed" and "the check itself broke" produce the same status; the
   count is what distinguishes them.
4. Any claim *about a commit* checked with `git show <sha>:<path>`, never the working tree,
   because another session may be writing here.

**If a gate fails twice on the same phase, the run stops and writes the report.** Without
that rule an agent can thrash for hours and produce nothing.

## §5 Failure modes this design is built around

All observed in this repo; each maps to a rule above.

- Naming an Agent dispatch routes it to the teammate protocol, where the default return
  path is never sent — measured 8.6% silent vs 0.2% unnamed. Dispatches are unnamed, and
  **every agent writes its result to a file before returning**; delivery-hop failure is
  measured at n=4 and strikes *after* the expensive work is done.
- A subagent that backgrounds `cargo test` can park forever after results arrive.
  Verification is foregrounded in every dispatch.
- The workspace suite swings 568–2482 s against a 600 s tool ceiling. Subagents partition
  per package; the controller backgrounds anything longer.
- `cargo test` stops at the first failing package, and later suites that never ran look
  identical to passes. `--no-fail-fast`, and count `test result: ok`.
- zsh does not populate bash's `PIPESTATUS`; `${PIPESTATUS[0]}` expands empty, which reads
  as "no failure". Use `${pipestatus[1]}` or `set -o pipefail`. zsh also does not
  word-split, so an unquoted `$spec` stays one argument and yields a false red.
- Never share `CARGO_TARGET_DIR` across worktrees: a mutation proof's artifact leaks into
  the main tree with `git diff` clean.
- An agent dying mid-mutation leaves a fix silently reverted behind a green-looking gate —
  hence the sha-vs-HEAD check at every handoff.
- **Grepping logs for `panicked` is not a failure detector in this repo** — it has caught
  two agents already, which is the F1 defect Phase 1 fixes. Key on process exit code and
  `test result: FAILED`.
- `timeout`/`gtimeout` are both absent on this machine; a bare `timeout` exits 127 and that
  reads as a result. Use a perl alarm with a pristine control.
- `CHARPENTE_SEED` replays one universe, not the batch; every coverage requirement is
  skipped. It is never evidence that a floor holds.
- A green mutation is ambiguous between a vacuous test and an inert mutation. Print
  observable behaviour under any green mutation before concluding.

## §6 Blocked work and reporting

Blocked items go to `decisions-pending.md`: the ambiguity, the reading taken, the
reasoning, and what would falsify it. Anything contradicting DECISIONS.md gets no reading —
that task stops and the run moves on.

The report lives in the **main** tree at
`.superpowers/sdd/2026-09-09-autonomous-run/report.md` — main tree, not the worktree, so it
survives the worktree. Per phase: what landed, shas, and counted verification evidence.
Then the decisions-pending list, the mutation findings, what was left undone and why, and an
explicit **"what I could not verify"** section.

Nothing is cleaned up. The worktrees stay for review; a worktree's gitignored
`.superpowers/sdd` dies with it otherwise.

## §7 Operational preconditions

- **The terminal stays open.** This session is the controller; closing it ends the run.
- **The machine must not sleep.** `caffeinate` is held for the duration —
  `scripts/compile-bench.sh` already does this, and there is a prior incident of a laptop
  sleeping mid-run where elapsed time could not detect it.
- Only Phase 0 pushes. Everything else stays local.

## §8 Non-goals

- **No orchestration framework.** No persona files, no dispatch skill, no scheduler. Those
  are the agent-skills sub-project's scope and are gated behind its measurements.
- **No scoring of T3.** Collection only.
- **No fixes applied from mutation findings.** They are reported.
- **No cleanup of stale branches or worktrees.** `oracle-density` and `tenant-scoping` are
  provably merged and deletable, but a destructive step bundled into an unattended run is
  exactly the shape that has misfired here before.
- **No new BENCH.md numbers.** No phase in this run is an instrument.

## §9 Exit criteria

1. Phase 0's four edits are committed to main and pushed, with a counted-green gate.
2. Each Phase 1 defect has a test that was observed RED before its fix and green after,
   with the seed recorded, verified by an agent other than the one that wrote it.
3. Phase 2's transcripts are archived and explicitly marked unscored.
4. Phase 3's findings file distinguishes MISSED from timeouts and from unresolved runs.
5. The report exists, and its "what I could not verify" section is populated rather than
   empty-by-omission.
6. Every phase boundary has a recorded gate result — including any that failed.
