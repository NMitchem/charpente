# Agent Skills and Personas Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Encode charpente's subagent-driven development discipline into repo-committed
skills and personas, each one gated on a recorded baseline failure rather than on assertion.

**Architecture:** Four skills in `.claude/skills/<name>/SKILL.md` carry the procedures;
up to three personas in `.claude/agents/*.md` wire them in via the `skills:` preload field.
Every skill is written **against a recorded baseline failure** produced by dispatching a
fresh subagent at a realistic charpente scenario without it — a skill whose baseline does
not fail is not written. CLAUDE.md sheds its 1,238-line narrative to `docs/LEDGER.md` first,
so every baseline runs against the conditions that will actually ship.

**Tech Stack:** Markdown. No Rust source is touched by this plan.

**Spec:** `docs/superpowers/specs/2026-08-14-agent-skills-design.md`. Read it before Task 1.
Section references below (§1…§8) are to that spec.

## Global Constraints

- **Hard rule 3 governs this plan.** RED before GREEN. A skill is written only after a
  baseline dispatch exhibits the failure it targets, and the transcript is archived.
- **Pre-register the failure criteria before running each baseline.** Write them into the
  ledger first, then dispatch. Judging "did it fail?" after reading the transcript is the
  same post-hoc reasoning this whole sub-project exists to stop.
- **The control condition binds (§5).** Baseline does not fail → the skill is not written,
  the negative result is recorded, and the task closes as a documented drop.
- Skills use **only `name` and `description`** in frontmatter — the portable six-field
  subset, and what all 14 superpowers skills use. **No `allowed-tools` in any committed
  skill** (§6): workspace trust does not gate that field, and this repo is going public.
- `name` must equal the directory name; lowercase, digits and hyphens only; no consecutive
  hyphens; must not contain the reserved words "claude" or "anthropic".
- `description` states **triggering conditions only** — third person, no summary of the
  skill's own workflow. A description that summarizes workflow gets followed *instead of*
  the body (§1).
- SKILL.md body: **under 500 lines and under ~5,000 tokens.** The body enters the
  conversation once and persists for the session, so every line is a recurring cost — write
  standing instructions, not one-time steps (§1).
- **Persona bodies do not restate CLAUDE.md** (§3). Every subagent already receives it.
- **Commit directly to `main`. No Co-Authored-By trailers.** (CLAUDE.md workflow.)
- **A concurrent session is live on this repo** (§4). Before every commit: `git status` and
  `git log --oneline -3`. **Stage named paths only — never `git add -A`.**
- Ledger for this sub-project: `.superpowers/sdd/2026-08-14-agent-skills/` (gitignored —
  `.superpowers/sdd/.gitignore` is `*`). Baselines, transcripts and criteria live there.
- Gates owed per task: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --
  -D warnings`, `cargo build -p guide-examples`. The 11-stage sweep is **not** owed — no
  Rust source is touched (§8, exit criterion 6).

## Task Ordering Rationale

The spec presents the CLAUDE.md split as §4. **This plan runs it first, as Task 1**, for a
methodological reason the spec does not address: baselines run in subagents, and subagents
receive CLAUDE.md in full. Running the baselines before the split would measure agent
behaviour under a 28k-token context that will not exist by the time the skills ship, making
every recorded baseline unreproducible. Split first, then measure.

Tasks 2–5 (the four skills) are independent of each other and may be reviewed in any order,
but each must complete its own RED→GREEN cycle. Task 6 (personas) is last among the build
tasks per spec ruling 3.

## File Structure

| File | Responsibility |
|---|---|
| `CLAUDE.md` | Standing instructions only, ≤200 lines (T1) |
| `docs/LEDGER.md` | The sub-project narrative, moved verbatim (T1) |
| `.claude/skills/charpente-sim-properties/SKILL.md` | Writing a sim property that can fail (T2) |
| `.claude/skills/charpente-adversarial-review/SKILL.md` | Interrogating a diff (T3) |
| `.claude/skills/charpente-evidence-sweep/SKILL.md` | Evidence before "done" (T4) |
| `.claude/skills/charpente-doc-truth/SKILL.md` | Prose checked against code (T5) |
| `.claude/agents/charpente-reviewer.md` | Parallel review persona (T6, conditional) |
| `.claude/agents/charpente-sweeper.md` | Sweep persona, context isolation (T6, conditional) |
| `.claude/agents/charpente-preflight.md` | Census persona, context isolation (T6, conditional) |
| `.superpowers/sdd/2026-08-14-agent-skills/` | Criteria, transcripts, progress (all tasks, untracked) |

## The Baseline Protocol (used by Tasks 2–5)

Every skill task follows this cycle. It is stated once here and referenced by each task.

**RED — establish the baseline:**
1. Write the task's pre-registered failure criteria to
   `.superpowers/sdd/2026-08-14-agent-skills/task-N-criteria.md`. Each criterion is a
   yes/no question answerable from a transcript without interpretation.
2. Dispatch **two** fresh `general-purpose` subagents on the scenario, `model: sonnet`,
   with no mention of skills, discipline, or this sub-project. Two, not one, because a
   single sample cannot distinguish a real behavioural tendency from one draw.
3. Save both transcripts to `task-N-baseline-{a,b}.md`. Score each against the
   pre-registered criteria and record the scores.
4. **Decision gate:** the skill is written only if a criterion fails in **both** runs. One
   failure out of two is inconclusive — dispatch a third and use the majority.

**GREEN — verify the skill changes behaviour:**
5. Write `SKILL.md` addressing **only the criteria that actually failed**. Content for
   criteria that passed is not written, however good it sounds.
6. Re-dispatch two fresh subagents on the same scenario with the skill body supplied in the
   dispatch prompt. Save to `task-N-green-{a,b}.md` and score against the same criteria.
7. **Pass condition:** every previously-failing criterion now passes in both runs. If one
   still fails, revise the skill and re-run — up to three rounds, then escalate.

**Note on the GREEN configuration.** Supplying the body in the dispatch prompt reproduces
the *content* of the shipping preload path (`skills:` on a persona) but not its mechanism.
That is deliberate: personas do not load until a session restart (§3), so mechanism
verification happens once in Task 6 rather than being entangled with content measurement
here. Record this in each report so the two are not conflated.

---

### Task 1: Split CLAUDE.md, narrative to docs/LEDGER.md

**Why first:** baselines run inside subagents, and subagents receive CLAUDE.md in full.
Measuring against a context that is about to change by 26k tokens would make every recorded
baseline unreproducible.

**Files:**
- Modify: `CLAUDE.md` (currently 1,341 lines)
- Create: `docs/LEDGER.md`

**Interfaces:**
- Produces: a CLAUDE.md whose "Current state" section is a short pointer; `docs/LEDGER.md`
  holding the moved narrative. Tasks 2–5 consume this as the baseline context.

- [ ] **Step 1: Re-check repo state before touching anything**

```bash
git status --porcelain
git log --oneline -3
```

Expected: clean tree. If `CLAUDE.md` is modified, **stop** — the concurrent session is mid-edit.
Wait, re-check, and proceed only against a clean tree.

- [ ] **Step 2: Record the before-measurement**

```bash
wc -l CLAUDE.md && wc -c CLAUDE.md | awk '{print int($1/4)" tokens approx"}'
```

Record both numbers in the ledger. Exit criterion 5 requires the saving to be **measured and
stated**, which needs a before.

- [ ] **Step 3: Create `docs/LEDGER.md` with the narrative moved verbatim**

The two sections that move are `## Current state / immediate next steps` (1,145 lines) and
`## M2 follow-up backlog (carried from M1, ruled — written to be read cold)` (93 lines).

`docs/LEDGER.md` opens with this header, then the two sections **byte-identical** — no
rewording, no summarizing, no reordering:

```markdown
# charpente — development ledger

The chronological record of every milestone, sub-project and ruled interlude. Moved here
from CLAUDE.md on 2026-08-14: it is the project's memory, but it is not a standing
instruction, and every subagent dispatched in this repo was loading all of it.

CLAUDE.md carries the standing rules. This file carries what happened. The underlying
detail lives in git history and `.superpowers/sdd/`.
```

- [ ] **Step 4: Verify the move is byte-exact before deleting anything**

Extract the same range from git and diff it against what landed. Do not skip this — a
"verbatim" move that silently reflows is the doc-truth failure committed by the task meant
to enable the doc-truth skill.

Derive the offsets rather than hardcoding them, and **prove both operands are non-empty
before believing the comparison** — `diff` reports two empty files as identical, so an
extraction that silently yields nothing produces a pass that means nothing:

```bash
git show HEAD:CLAUDE.md | awk '/^## Current state \/ immediate next steps/,0' > /tmp/claude-tail.txt
START=$(grep -n "^## Current state / immediate next steps" docs/LEDGER.md | head -1 | cut -d: -f1)
tail -n +${START} docs/LEDGER.md > /tmp/ledger-body.txt
test -s /tmp/claude-tail.txt && test -s /tmp/ledger-body.txt || { echo "VACUOUS: an operand is empty"; exit 1; }
diff /tmp/claude-tail.txt /tmp/ledger-body.txt && echo "BYTE-EXACT"
shasum -a 256 /tmp/claude-tail.txt /tmp/ledger-body.txt
```

Expected: `BYTE-EXACT` and two identical hashes. Any diff means fix the copy, not the check.

Then prove **nothing was dropped anywhere**, which byte-exactness of the moved region alone
does not establish: the kept region's line count plus the moved region's must equal the
original file's exactly, so every line is accounted for in exactly one output file.

- [ ] **Step 5: Trim CLAUDE.md**

Keep, unchanged: `# CLAUDE.md — charpente`, `## What this is`, `## Commands`,
`## Workspace map`, `## Confidentiality boundary`, `## Hard rules`, `## Style`,
`## Workflow`. Replace both moved sections with:

```markdown
## Current state

**M1 complete** (2026-07-27). **M2 in progress**: sub-projects 0a (store-latch poison), 0b
(dual-primary), 1 (benchmark harness) and three ruled interludes are done; **sub-project 2,
primary-side group commit, is in flight** — T1–T7 committed, T8–T11 open. The authoritative
mid-plan record is the ledger at `.superpowers/sdd/2026-08-10-group-commit/progress.md`.

The full chronological record — every milestone, sub-project, ruled interlude, and the M2
follow-up backlog — is **[docs/LEDGER.md](docs/LEDGER.md)**. Read it when you need the
history behind a decision; it is not required to start work.

## Subagent dispatch

This repo commits its own skills (`.claude/skills/`) and personas (`.claude/agents/`).
When dispatching a subagent for implementation, review, a census or an evidence sweep,
select the matching charpente persona rather than `general-purpose`. Note that
superpowers' `subagent-driven-development` templates hard-code `general-purpose` and vary
only the model — override that here.
```

The `## Subagent dispatch` section is added by this task but describes artifacts that do not
exist until Tasks 2–6. That is intentional: it lands with the file it belongs in, and Task 7
verifies the names it references resolve.

- [ ] **Step 6: Verify the result**

```bash
wc -l CLAUDE.md
grep -c "^## " CLAUDE.md
```

Expected: CLAUDE.md ≤200 lines. Confirm every one of the eight kept sections is present and
that no hard rule was lost — the six numbered rules must still be there verbatim.

- [ ] **Step 7: Run the gates**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo build -p guide-examples
```

Expected: all three clean. None should be affected, and confirming that is the point.

- [ ] **Step 8: Commit named paths only**

```bash
git status --porcelain
git add CLAUDE.md docs/LEDGER.md
git commit -m "docs: CLAUDE.md keeps the rules, docs/LEDGER.md takes the narrative

1238 of 1341 lines were chronological changelog. Anthropic's sub-agents doc confirms
a subagent's context includes CLAUDE.md at every level, so every dispatch in this repo
was loading ~28k tokens of sub-project history to read 95 lines of standing rules.

Nothing deleted; the two narrative sections move byte-exact, verified by diff against
git HEAD before the trim. CLAUDE.md gains a subagent-dispatch section pointing at the
personas that Tasks 2-6 of this sub-project build."
```

---

### Task 2: `charpente-sim-properties`

**Files:**
- Create: `.claude/skills/charpente-sim-properties/SKILL.md`
- Ledger: `task-2-criteria.md`, `task-2-baseline-{a,b}.md`, `task-2-green-{a,b}.md`, `task-2-report.md`

**Interfaces:**
- Produces: skill name `charpente-sim-properties`, preloaded by `charpente-reviewer` in Task 6.

- [ ] **Step 1: Pre-register the failure criteria**

Write to `task-2-criteria.md` **before dispatching**:

```markdown
# Task 2 pre-registered failure criteria

Scored yes/no from the transcript alone. "Fail" = the agent did NOT do the thing.

- C1 RED-first: did the agent prove the new test fails before the code that makes it pass,
  or (for a test that passes on arrival) mutate the production code and watch it redden?
- C2 End-state oracle: does the property assert a final-state invariant, not only that the
  run did not crash?
- C3 Per-round verification: does the oracle verify at each round, rather than once at the
  end where a later round can paper over a loss?
- C4 Coverage keys: does it register a `require:` key asserting the interesting case
  actually occurred?
- C5 Anti-vacuity: did the agent state, for its main assertion, the concrete state in which
  it would fail?
- C6 Seed pinning: is the seed pinned and recorded?
```

- [ ] **Step 2: Dispatch two baseline subagents**

`general-purpose`, `model: sonnet`, no mention of skills or this sub-project. Prompt:

```
You are working in the charpente repo at /Users/noahmitchem/Documents/GitHub/charpente.
Read CLAUDE.md first.

Task: add a simulation property test to crates/charpente/tests/group_commit.rs that
verifies no acknowledged write is lost across a crash and restart when the request arm
commits a contended batch, under the full fault matrix.

Study the existing tests in that file and in crates/charpente/tests/kernel.rs for the
established shape. Write the test, run it, and report what you did and what evidence you
have that it works. Write your report to
.superpowers/sdd/2026-08-14-agent-skills/task-2-baseline-<a|b>.md
```

- [ ] **Step 3: Score both transcripts against C1–C6**

Record the scores in `task-2-report.md`. **Do not** revise the criteria now — that is the
post-hoc move this protocol exists to prevent.

- [ ] **Step 4: Apply the decision gate**

A criterion counts as failed only if it failed in **both** runs. If none did, **stop**: the
skill is not written, record the negative result, and close the task as a documented drop.

- [ ] **Step 5: Write the skill against the failing criteria only**

```markdown
---
name: charpente-sim-properties
description: Use when writing or changing a charpente simulation property, workload, oracle, or seeded test — anything touching run_universes_swarm!, Sim, checks::, require: coverage keys, or a pinned seed. Also use when a sim test passed on its first run.
---
```

Body structure — a red-flag table plus checklists, the form that fits a discipline failure
(§2). Draw content **only** for failed criteria, from this evidence catalogue:

- The four required parts: workload fn, recovery/classify loop, `require:` coverage keys,
  end-state oracle. "A swarm property without an end-state oracle is a crash test, not a
  property."
- Vacuity traps, each from a real finding: comparisons trivially true at zero (`0 < 0` when
  `watermark == 0`, 50 of 256 universes); assertions true either way (`assert_eq!(total_seeds, 0)`
  when the rig never checkpoints); end-only verification (T8b I-2: a genuine loss of 7 acked
  writes reported GREEN); 92 of 256 universes asserting nothing at all.
- "Not driving a node does not isolate it" — `crash_now`/`teardown_host` tear down only the
  crashed side; a peer never polled drains stale pre-crash frames and appears to converge.
  Use `recover_node` to force a real reconnect.
- Mutation-proven teeth: a test passing on arrival is unproven. Mutate, watch it redden **by
  name**, restore byte-exact and prove it (`sha256` or `git diff`).
- Use `checks::classify` / `is_fault_fallout`; do not hand-roll a fifth copy.
- Never share `CARGO_TARGET_DIR` across worktrees when mutating.

Close with a red-flag table in the established form:

```markdown
## Red flags — STOP

| Thought | Reality |
|---|---|
| "The test passes, so it works" | A test that passed on arrival is unproven. Mutate the code and watch it redden by name. |
| "The oracle checks the end state" | A later round can write the count back above the watermark. Verify every round. |
| "I didn't drive that node, so it's isolated" | `teardown_host` tears down only the crashed side. Use `recover_node`. |
```

- [ ] **Step 6: Run the GREEN verification**

Re-dispatch two fresh subagents on the identical scenario with the skill body in the prompt.
Score against C1–C6. Pass condition: every previously-failing criterion passes in both runs.

- [ ] **Step 7: Check the body budget**

```bash
wc -l .claude/skills/charpente-sim-properties/SKILL.md
wc -w .claude/skills/charpente-sim-properties/SKILL.md
```

Expected: under 500 lines. Over ~3,500 words, move reference material to `references/`.

- [ ] **Step 8: Commit**

```bash
git status --porcelain
git add .claude/skills/charpente-sim-properties/SKILL.md
git commit -m "skills: charpente-sim-properties — the anti-vacuity skill

Baseline: 2 fresh subagents asked to add a crash-safety property to
group_commit.rs. Both failed <criteria>, verbatim transcripts in the ledger.
Written against those failures only; criteria that passed got no content.
GREEN re-run: both comply."
```

---

### Task 3: `charpente-adversarial-review`

**Files:**
- Create: `.claude/skills/charpente-adversarial-review/SKILL.md`
- Ledger: `task-3-criteria.md`, `task-3-baseline-{a,b}.md`, `task-3-green-{a,b}.md`, `task-3-report.md`

**Interfaces:**
- Produces: skill name `charpente-adversarial-review`, preloaded by `charpente-reviewer` in Task 6.

**Why this scenario is strong:** it uses a diff whose ground truth is already known — a real
reviewer caught the defect, and the finding is recorded. The baseline is not being judged
against my opinion of what a good review is; it is being judged against a defect that
demonstrably existed and was demonstrably findable.

- [ ] **Step 1: Locate the ground-truth diff**

The verifying-peer-policy interlude's Task 3 review found I-1: `assert_eq!(total_seeds(&g), 0, …)`
could not fail, because that rig never checkpoints, so a refused seed and a successful one both
leave `total_seeds` at 0. Recover the pre-fix state:

```bash
git log --oneline --all -- crates/charpente/tests/replication.rs | head -30
git log -S "total_seeds" --oneline -- crates/charpente/tests/replication.rs
```

Record the commit and the exact pre-fix hunk in `task-3-criteria.md`. If the pre-fix state
cannot be recovered from history, use the `.superpowers/sdd/2026-08-06-verifying-peer-policy/`
review file, which quotes it.

- [ ] **Step 2: Pre-register the failure criteria**

```markdown
# Task 3 pre-registered failure criteria

- C1 Found the defect: did the review identify that the assertion cannot fail?
- C2 Ran an experiment: did it execute anything (mutation, test run), or only read?
- C3 Isolation: if it mutated, did it work in a detached worktree with its own
  CARGO_TARGET_DIR, and prove the main tree byte-identical afterwards?
- C4 Report to file: did it write the report to a file rather than only returning prose?
- C5 Verdict format: READY / READY WITH FIXES / NOT READY with Critical/Important/Minor counts?
```

- [ ] **Step 3: Dispatch two baseline subagents**

`general-purpose`, `model: opus` (reviewers get the capable tier — matching how this review
would really be dispatched). Prompt:

```
You are working in the charpente repo at /Users/noahmitchem/Documents/GitHub/charpente.
Read CLAUDE.md first.

Review the diff at <path to the extracted pre-fix diff>. It is a set of test changes to
crates/charpente/tests/replication.rs. Report whether it is ready to merge, and any
problems you find. Write your review to
.superpowers/sdd/2026-08-14-agent-skills/task-3-baseline-<a|b>.md
```

- [ ] **Step 4: Score, gate, and write the skill against failed criteria only**

```markdown
---
name: charpente-adversarial-review
description: Use when reviewing a charpente diff, task, branch or fix round, including re-reviews, and when judging whether a test suite's assertions can actually fail.
---
```

Form: **positive recipe** — state what the report IS, in order — plus the defect-class
checklist. Not a prohibition list; prohibitions backfire on output-shaping failures (§1).
Evidence catalogue:

- The report is a file. Return a short status. Deliveries get lost — both research agents in
  this sub-project's own design session went idle without delivering; only the files survived.
- The four defect classes: (i) an assertion that cannot fail; (ii) an assertion that fails for
  a reason other than the one it states; (iii) a property the report claims is covered that
  nothing asserts; (iv) prose the code disproves.
- **Measure, don't argue.** Strongest finding on record: "I proved the coverage gate has teeth
  by a mutation the implementer did not run." Strongest counter-example: 0b's I-2, where the
  recommended fix was implemented, measured, and reverted — the new cover key fired in 0 of 256
  universes and inconclusive was 127 with the machinery and 127 without.
- Isolation protocol: detached worktree at BASE, its own `CARGO_TARGET_DIR`, every mutation
  reverted, tree proven byte-identical before and after (`diff -q` against the packaged diff,
  plus `sha256`).
- Verdict format and finding severities.

- [ ] **Step 5: GREEN verification, budget check, commit**

Same shape as Task 2 Steps 6–8. Commit message states which criteria failed at baseline.

---

### Task 4: `charpente-evidence-sweep`

**Files:**
- Create: `.claude/skills/charpente-evidence-sweep/SKILL.md`
- Ledger: `task-4-criteria.md`, `task-4-baseline-{a,b}.md`, `task-4-green-{a,b}.md`, `task-4-report.md`

**Interfaces:**
- Produces: skill name `charpente-evidence-sweep`, preloaded by `charpente-sweeper` in Task 6.

- [ ] **Step 1: Pre-register the failure criteria**

```markdown
# Task 4 pre-registered failure criteria

- C1 Ran the gates: fmt, clippy -D warnings, guide gate — actually executed, not asserted?
- C2 Blast radius: did it state which stages this change owes and why, rather than running
  an arbitrary subset or everything?
- C3 Count reconciliation: did it state the test-count delta and reconcile it exactly?
- C4 tee not tail: did it avoid piping cargo through `tail` (which eats the counts and
  replaces cargo's exit status)?
- C5 Honest reporting: if any stage went red then green, did it report both runs?
- C6 No claim without evidence: did it avoid claiming "complete"/"passing" before the
  commands finished?
```

- [ ] **Step 2: Dispatch two baseline subagents**

The scenario must be a real change with a genuinely narrow blast radius, so that "run
everything" and "run the right subset" are distinguishable. Use the Task 1 CLAUDE.md split —
a docs-only change — which by §2's own rule owes fmt/clippy/guide-gate and nothing else.

```
You are working in the charpente repo at /Users/noahmitchem/Documents/GitHub/charpente.
Read CLAUDE.md first.

The most recent commit split CLAUDE.md, moving its narrative sections to docs/LEDGER.md.
Verify that this change is complete and correct, and report your evidence. Write your
report to .superpowers/sdd/2026-08-14-agent-skills/task-4-baseline-<a|b>.md
```

- [ ] **Step 3: Score, gate, write against failed criteria only**

```markdown
---
name: charpente-evidence-sweep
description: Use before claiming a charpente task, interlude or sub-project is complete, before committing, and when deciding which verification stages a change owes.
---
```

Form: **positive recipe plus one conditional** keyed to an observable predicate. Evidence
catalogue:

- The stage list: fmt; clippy `-D warnings`; `cargo build -p guide-examples`;
  `cargo test --workspace` twice with identical counts; pinned debug replays
  `CHARPENTE_SEED=0x8bee1752811ae873` (charpente-sim crash_recovery) and `0x348270b38f166529`
  (charpente kernel); both release mode-skew tripwires exactly as `ci.yml` runs them; a fresh
  invoicing 1024-universe batch; `scripts/edge-harness.sh`.
- **The blast-radius conditional:** the edge matrix is owed *iff* replication, auth or realtime
  behaviour is touched. A docs-only change owes fmt/clippy/guide-gate and a stated reason for
  skipping the rest. Both precedents are in the ledger, including one sweep that skipped the
  edge matrix and said why.
- **Count reconciliation is mandatory.** "572→583 reconciles as Task 1 +1, Task 2 +5, Task 3 +5."
  An unreconciled delta is a finding.
- **`tee`, never `tail`.** Recorded scar: tail ate one sweep's counts and replaced cargo's exit
  status with its own.
- Do not run `scripts/bench.sh --smoke` back-to-back with `cargo test --workspace`.
- Report a red-then-green stage as both runs, never as its clean attempt.
- Measure CPU time, not wall clock.

- [ ] **Step 4: GREEN verification, budget check, commit**

Same shape as Task 2 Steps 6–8.

---

### Task 5: `charpente-doc-truth`

**Files:**
- Create: `.claude/skills/charpente-doc-truth/SKILL.md`
- Ledger: `task-5-criteria.md`, `task-5-baseline-{a,b}.md`, `task-5-green-{a,b}.md`, `task-5-report.md`

**Interfaces:**
- Produces: skill name `charpente-doc-truth`, preloaded by `charpente-sweeper` in Task 6.

**This task is the most likely to close as a documented drop** (§7's risk table, and the
spec's own prediction). A capable agent asked to verify a doc claim may already check it
against the code. If both baselines pass, that is the result — record it and do not write
the skill.

- [ ] **Step 1: Recover a ground-truth false claim**

The verifying-peer-policy interlude shipped a GUIDE §10 paragraph stating a gated node
"will still seed a brand-new follower" and records the newcomer "as a witness like any
other" — both the opposite of shipped behaviour, disproven by
`the_seed_while_verifying_refusal_is_pinned` (seed `0x2EB1_0121`). Recover the pre-correction
text:

```bash
git log -S "will still seed a brand-new follower" --oneline -- docs/GUIDE.md
```

Extract the paragraph and the commit into `task-5-criteria.md`.

- [ ] **Step 2: Pre-register the failure criteria**

```markdown
# Task 5 pre-registered failure criteria

- C1 Checked the code: did the agent look for the test or code path that establishes the claim?
- C2 Found the contradiction: did it identify that the shipped behaviour is the opposite?
- C3 Sourced the strings: did it quote the refusal from its pinning test rather than paraphrase?
- C4 Marked the unpinned: did it distinguish claims backed by a test from claims backed by nothing?
```

- [ ] **Step 3: Dispatch two baseline subagents**

```
You are working in the charpente repo at /Users/noahmitchem/Documents/GitHub/charpente.
Read CLAUDE.md first.

Review this paragraph from docs/GUIDE.md for accuracy and improve its wording:

<the recovered pre-correction paragraph, verbatim>

Write your assessment and your improved wording to
.superpowers/sdd/2026-08-14-agent-skills/task-5-baseline-<a|b>.md
```

The prompt asks for a **wording** improvement, not a correctness audit. An agent that checks
the code anyway has genuinely internalised the discipline; one that polishes false prose has
exhibited exactly the failure. Asking "is this accurate?" would tip the answer and measure
nothing.

- [ ] **Step 4: Score, gate, write against failed criteria only**

```markdown
---
name: charpente-doc-truth
description: Use when editing DESIGN.md, docs/GUIDE.md, docs/BENCH.md, README.md, CLAUDE.md or a spec; when a change alters public API; and at sub-project close-out.
---
```

Form: **positive recipe.** Evidence catalogue:

- Hard rule 6 as procedure: for each factual claim, locate the code or test that establishes it;
  quote every error string from its pinning test byte-exact; mark claims nothing pins.
- The case file: GUIDE's seed-a-brand-new-follower paragraph, contradicted by its own branch's
  tests; BENCH.md's three defects, all found by reading prose against code and **none by a
  reviewer**; DESIGN's promote-path dual-primary claim, disproven by a green test on the same branch.
- Prose coupling is deliberate in two places — redb's remedy prose in the latch stop message,
  matched by `checks::classify` — so a reword reddens locally by design. Do not "clean up" a
  message that a matcher depends on.

- [ ] **Step 5: GREEN verification, budget check, commit**

Same shape as Task 2 Steps 6–8. **If the task closed as a drop at Step 4's gate**, commit only
the ledger note — nothing under `.claude/skills/` — and record the negative result in the report.

---

### Task 6: Personas, measured against a control

**Files:**
- Create (conditional): `.claude/agents/charpente-reviewer.md`, `.claude/agents/charpente-sweeper.md`,
  `.claude/agents/charpente-preflight.md`
- Ledger: `task-6-criteria.md`, `task-6-control-{a,b}.md`, `task-6-persona-{a,b}.md`, `task-6-report.md`

**Interfaces:**
- Consumes: the skill names committed by Tasks 2–5. A typo here is skipped with only a
  debug-log warning (§3) — Step 4 exists to catch that.

**Spec ruling 3 binds:** a persona ships only if it beats a `general-purpose` dispatch carrying
the same model and the same skill content. Anthropic ships an eval harness for skills and none
for personas, and the claim that personas outperform equivalent prompt text is folklore (§1).

- [ ] **Step 1: Write the three persona files**

```yaml
---
name: charpente-reviewer
description: Adversarial reviewer for charpente diffs, tasks and branches. Use when a change needs review, a fix round needs re-review, or a test suite needs judging for assertions that cannot fail. Dispatch three in parallel, one per defect class.
model: opus
effort: high
skills:
  - charpente-adversarial-review
  - charpente-sim-properties
color: red
---
```

Body: short. The status contract (`READY` / `READY WITH FIXES` / `NOT READY` with severity
counts), report-to-file-first, the no-subagents contract, and explicit permission to escalate
("bad work is worse than no work"). **It must not restate CLAUDE.md** (§3) — that already
reaches every subagent. Omit any skill name from Tasks 2–5 that closed as a drop.

`charpente-sweeper`: `skills: [charpente-evidence-sweep, charpente-doc-truth]`, no `model`
override (inherits). `charpente-preflight`: `model: opus`, no preloaded skills — its job is
census and measurement, and its body states that its output is a table with `file:line`
citations and a CLEARED/OPEN split, never a verdict.

**Do not set `tools:` on any persona.** A `tools:` allowlist silently removes `Skill` (§3),
which would cut off the very preloading these personas exist for. Use `disallowedTools` if
restriction is ever needed.

- [ ] **Step 2: Restart the session**

A new `.claude/agents/` directory does not load until a session restart (§3), and charpente has
none today. Nothing below this line works until this is done.

- [ ] **Step 3: Verify the personas load**

```bash
claude --debug -p "list the available agent types" 2>&1 | grep -i charpente
```

Expected: all three names appear. If one is missing, check for a `name:` that does not match, or
a duplicate name elsewhere in the tree (`/doctor` reports duplicates).

- [ ] **Step 4: Verify the `skills:` preload actually resolved**

```bash
claude --debug -p "dispatch charpente-sweeper to report which skills it has preloaded" 2>&1 | grep -iE "skill|preload|warn"
```

Expected: no "skipped"/"not found" warnings. **A typo in `skills:` fails silently outside
`--debug`** — this step is the only place it surfaces.

- [ ] **Step 5: Run the control comparison**

For `charpente-reviewer`, on a diff with a known defect (reuse Task 3's ground-truth diff):

- **Control arm:** two `general-purpose` dispatches, `model: opus`, with the
  `charpente-adversarial-review` body supplied in the prompt.
- **Persona arm:** two `charpente-reviewer` dispatches with only the task.

Score both arms against Task 3's C1–C5. Record all four transcripts.

- [ ] **Step 6: Apply the ruling**

Ship the persona if the persona arm scores **at least as well** as the control on every
criterion, since it also costs less prompt construction per dispatch. Do **not** ship a persona
whose arm scores worse. If the arms tie, say so plainly in the report — a tie means the persona
buys convenience and parallelism, not quality, and the report must not claim otherwise.

`charpente-sweeper` and `charpente-preflight` are justified on **context isolation**, which the
comparison above does not measure. For those two, record the observable that does: the token
volume returned to the controller, persona arm vs control arm.

- [ ] **Step 7: Commit whichever personas earned it**

```bash
git status --porcelain
git add .claude/agents/<each persona that passed>.md
git commit -m "agents: charpente personas that beat their control

<per-persona: which arm won, on which criteria, with the scores>
Personas that did not beat the control are recorded in the ledger and not shipped."
```

---

### Task 7: Close-out — verification, docs, exit criteria

**Files:**
- Modify: `CLAUDE.md` (current-state entry for this sub-project)
- Ledger: `task-7-sweep.log`, `progress.md` final entry

- [ ] **Step 1: Verify every exit criterion, one at a time**

Against §8, producing a yes/no with evidence for each:

1. Every shipped skill has a recorded baseline failure **and** a recorded pass.
2. Every skill dropped for a non-failing baseline is documented **with its transcript**.
3. Every shipped persona beat its control; every unshipped one is recorded.
4. Skills and personas load — confirmed in `/doctor` and once under `--debug`.
5. CLAUDE.md ≤200 lines, narrative verbatim in `docs/LEDGER.md`, **token saving measured and
   stated** (before from T1 Step 2, after measured now).
6. `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
   `cargo build -p guide-examples` all clean.

- [ ] **Step 2: Run the owed gates, piped through tee**

```bash
mkdir -p .superpowers/sdd/2026-08-14-agent-skills
{ cargo fmt --all --check && echo "FMT OK"; } 2>&1 | tee -a .superpowers/sdd/2026-08-14-agent-skills/task-7-sweep.log
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tee -a .superpowers/sdd/2026-08-14-agent-skills/task-7-sweep.log
cargo build -p guide-examples 2>&1 | tee -a .superpowers/sdd/2026-08-14-agent-skills/task-7-sweep.log
```

`tee`, not `tail` — the skill this sub-project ships says so, and a close-out that violated it
would be its own counter-example.

- [ ] **Step 3: Measure the token saving**

```bash
wc -l CLAUDE.md && wc -c CLAUDE.md | awk '{print int($1/4)" tokens approx"}'
```

State before → after against T1 Step 2's recorded numbers.

- [ ] **Step 4: Verify the skill listing cost**

```bash
claude --debug -p "/doctor" 2>&1 | grep -iA5 skill
```

Confirm the four skills' descriptions are **not** being truncated. The listing budget is 1% of
context and drops least-used descriptions first (§1); four skills should sit far under it, and
confirming that is what makes the number a criterion rather than an assumption.

- [ ] **Step 5: Write the CLAUDE.md close-out entry**

Under `## Current state`, a short entry naming: what shipped, what was **dropped and why**, the
measured token saving, and the evidence. Any skill dropped for a non-failing baseline is named
explicitly — a close-out that reports only what shipped is the selective reporting this repo's
own evidence standard forbids.

- [ ] **Step 6: Final commit**

```bash
git status --porcelain
git log --oneline -3
git add CLAUDE.md
git commit -m "docs: agent-skills sub-project close-out

<N> skills shipped, <M> dropped for non-failing baselines (named, with transcripts in the
ledger). <P> personas shipped after beating their control; <Q> did not and are recorded.
CLAUDE.md <before> -> <after> lines, ~<X>k tokens saved per conversation and per dispatch.
Gates: fmt, clippy -D warnings, guide gate all clean. The 11-stage sweep is not owed —
no Rust source touched."
```

---

## Self-Review

**Spec coverage.** §1 research basis → informs T2–T6 form choices and T6's ruling. §2 four
skills → T2, T3, T4, T5 one each. §3 personas → T6, including all four documented footguns
(`tools:` cutting off `Skill` in T6 S1; silent `skills:` typo in S4; restart in S2; duplicate
names in S3). §4 CLAUDE.md split → T1, with the sequencing hazard as T1 S1 and S8. §5 RED-first
→ the Baseline Protocol plus T2–T5 Steps 1–4 and the GREEN steps. §6 non-goals → Global
Constraints (no `allowed-tools`, portable frontmatter only, personas don't restate CLAUDE.md).
§7 risks → T1 S1 (concurrent session), T7 S4 (listing budget), T5's drop warning. §8 exit
criteria → T7 S1, one-for-one. **No gaps found.**

**Placeholder scan.** The four SKILL.md bodies are deliberately not written out in full, and
this is the plan's one structural departure from "no placeholders." It is required by the
spec's own ruling: a skill written before its baseline would violate hard rule 3, and the
Iron Law's control condition means some of these files may correctly never exist. What each
task *does* specify completely: exact frontmatter, exact form (table vs recipe), the exact
evidence catalogue to draw from, and the rule that only failed criteria get content. The
undetermined part is emphasis, and it is undetermined **by design** rather than unspecified.

**Type consistency.** Skill names are identical in the file paths, the frontmatter blocks, the
T6 `skills:` lists and the File Structure table: `charpente-sim-properties`,
`charpente-adversarial-review`, `charpente-evidence-sweep`, `charpente-doc-truth`. Persona
names match between their frontmatter and T6's commit step. Ledger filenames follow
`task-N-{criteria,baseline-a,baseline-b,green-a,green-b,report}.md` throughout.

**One inconsistency found and fixed inline:** T6 originally preloaded `charpente-doc-truth`
into `charpente-sweeper` unconditionally, but T5 may close as a drop. T6 Step 1 now says to
omit any skill name whose task dropped — which matters, because a stale name in `skills:`
fails silently.
