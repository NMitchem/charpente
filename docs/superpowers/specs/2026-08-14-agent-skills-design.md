# Agent skills and personas for charpente: design

## Context

Charpente's development discipline is more developed than its tooling. Eight sub-projects
deep, the lifecycle in `.superpowers/sdd/` is stable and repeated: spec → plan → preflight
census → per-task brief → implementer → report → adversarial opus review (written to a
file, mutations run in an isolated worktree) → fix brief → re-review → commit → an N-stage
evidence sweep. It works. DESIGN §7 states the premise it serves — "charpente treats the
coding agent as the primary developer persona."

None of it is encoded anywhere an agent can find. `.claude/` contains one empty
`worktrees/` directory. There are no skills and no agent definitions, at any scope, on this
machine or in this repo. The discipline is transmitted by Noah re-typing it into each task
brief, which is why the briefs run 8–13 KB and why the same instructions appear in all of
them.

The failures this costs are documented in CLAUDE.md and the ledgers, and they are strikingly
consistent. They are not Rust bugs:

- **Assertions that cannot fail.** Three separate vacuity findings in the verifying-peer-policy
  interlude alone — one controller-authored test that could not go RED, one that passed under a
  mutation that should have reddened it, and `assert_eq!(total_seeds(&g), 0, …)` which was true
  whether the seed was refused or succeeded. T8b's review measured a fourth shape: **92 of 256
  universes (36%) asserting nothing at all**, 42 exhausting the verification loop unverified and
  50 verifying with `watermark == 0`, where `0 < 0` passes trivially.
- **Assertions that fail for a different reason than they state.** Sub-project 0a's review found
  both of its Importants in this class — assertions that "could not fail for the reason they
  stated while DESIGN and the spec claimed they could."
- **Docs the code disproves.** CLAUDE.md names this outright: "the class this repo keeps paying
  for." GUIDE shipped claiming a gated node "will still seed a brand-new follower" while the same
  branch's tests proved the opposite. Three BENCH.md defects were found at close-out by reading
  prose against code — **none by a reviewer**.
- **Green that means nothing.** "In this simulator, not driving a node does not isolate it" —
  `teardown_host` tears down only the crashed side, so a peer never polled still "converges" by
  draining stale pre-crash frames.

Every one of these was caught by a human holding the line — RED-first enforcement, an
adversarial reviewer running a mutation the implementer did not, or a close-out prose audit.
None was caught by a mechanism. This sub-project builds the mechanism.

A second, measured problem surfaced while scoping this one. `CLAUDE.md` is **1,341 lines /
~28k tokens, of which 1,238 lines (92%) are chronological changelog**; the standing
instructions are 95 lines. Anthropic's sub-agents documentation states that a subagent's
initial context includes "CLAUDE.md files at every level" — so every subagent dispatched in
this repo pays ~28k tokens of sub-project narrative it has no use for, and reads the hard
rules at 7% signal. Both research passes reached this independently; the second put it
plainly: "the cheapest way to make subagents cheaper here is trimming CLAUDE.md, not trimming
personas."

## Scope (Noah rulings, 2026-08-14)

1. **RED-first, for real.** Anthropic's `writing-skills` carries an Iron Law — "NO SKILL
   WITHOUT A FAILING TEST FIRST" — and its own control condition: "if the control doesn't
   exhibit the failure, there is nothing to fix — stop, don't author the guidance." This is
   hard rule 3 pointed at documentation, and it binds. A skill whose baseline does not fail
   **is not written**. Shipping three evidenced skills beats four assumed ones.
2. **The CLAUDE.md split is in scope**, in this sub-project, as its own commit. Nothing is
   deleted; the narrative moves verbatim to `docs/LEDGER.md` with a pointer left behind.
3. **Personas are built second and measured against a control.** Skills first. Each persona
   must beat a `general-purpose` dispatch carrying the same model and the same preloaded
   skills, or it does not ship — the I-2 precedent from sub-project 0b, where the reviewer's
   recommended fix was implemented, measured, and reverted because the machinery was inert.

## §1 The research basis, and the one claim this design refuses

Two research passes ran against 2026 primary sources; full findings are archived in the
session scratchpad and the load-bearing claims are restated here with provenance, because a
design resting on folklore is the defect this repo keeps finding in its own docs.

**Documented by Anthropic, and load-bearing here:**

- A subagent's initial context is its system prompt (the agent file's markdown body, which
  **replaces** rather than appends), the delegation message, and **CLAUDE.md at every level**.
  It does *not* receive the parent's conversation history, prior tool results, or skills the
  parent invoked. `Explore` and `Plan` are the only agents that skip CLAUDE.md, unchangeably.
- The `skills:` field on an agent definition **preloads named skills in full at startup**. The
  `tools:` doc steers authors here explicitly: "To preload Skills into context, use the
  `skills` field rather than listing `Skill` here." Preloading bypasses description-matching,
  the flakiest link in the chain.
- The skill listing is preloaded for every skill at ~100 tokens each, with a hard budget of
  **1% of the model's context window**. On overflow, descriptions are dropped **starting with
  the skills invoked least**. Skill count is arithmetic, not taste.
- A rendered SKILL.md **enters the conversation once and stays for the session**; Claude Code
  does not re-read it on later turns. Write standing instructions, not one-time steps.
- Composition rule, stated in the docs from both sides: **reference-shaped skills are preloaded
  into a persona; task-shaped skills fork themselves into one** (`context: fork` + `agent:`).
  The warning is explicit — "`context: fork` only makes sense for skills with explicit
  instructions … the subagent receives the guidelines but no actionable prompt, and returns
  without meaningful output."
- Prohibitions backfire on output-shaping failures. In superpowers' head-to-head wording tests
  the prohibition arm produced *more* of the unwanted content than the recipe arm, and trended
  worse than the no-guidance control. Prohibition is correct only for genuine discipline
  violations.
- A description that summarizes the *workflow* causes agents to follow the description instead
  of reading the body — one recorded case did one review where the skill's flowchart specified
  two.

**Refused as folklore.** The claim that a persona's system prompt produces better work than
the same instructions pasted into a dispatch prompt has **no measurement behind it**,
first-party or third-party. The second research pass looked for it specifically. What exists
instead: Anthropic's multi-agent post reports 90.2% over single-agent, but that is
*architecture*, and its own variance analysis attributes **80% of the variance to token
volume**; its one persona-adjacent finding is negative (vague instructions produced duplicate
work across subagents). Meanwhile skills have a first-party eval harness — `skill-creator`
ships with-skill/without-skill benchmarking, blind A/B between versions, and description
tuning measured on should-trigger/should-not-trigger hit rates — and **personas have none**.
Anthropic built a measurement loop for one and not the other.

Corroborating repo census: `anthropics/skills` ships **zero** agents. `obra/superpowers`
ships **zero** agents. Across every repo shipping both, skills outnumber personas 3–5×.

**The one documented justification for a persona is parallelism, not knowledge.** Anthropic's
own `feature-dev` plugin launches "3 code-reviewer agents examining simplicity, correctness,
and conventions" concurrently. The persona is the unit of parallelism. Every persona in §4
is justified on that ground, on context isolation, or on model pinning — never on "it makes
the worker smarter."

## §2 The four skills

Each skill exists to stop a failure this repo has actually paid for, cited to its ledger
entry. A skill without such a citation is not in this set.

### `charpente-sim-properties`

**Trigger:** writing or modifying a simulation property, workload, or oracle — anything
touching `run_universes_swarm!`, `Sim`, `checks::`, `require:` coverage keys, or a seeded
universe.

**The failure:** a test that cannot fail. This is the highest-value skill in the set and the
one with the most ledger evidence behind it.

**Content:**
- The four required parts of a property — workload fn, recovery/classify loop, `require:`
  coverage keys, end-state oracle. Noah's own words from the T8b brief: "a swarm property
  without an end-state oracle is a crash test, not a property."
- The vacuity checklist, every entry from a real finding: can this assertion fail — construct
  the state where it does; comparisons trivially true at zero (`0 < 0`); assertions true either
  way (`total_seeds == 0` when the rig never checkpoints); end-only verification a later round
  papers over (T8b I-2 measured a genuine loss of 7 acked writes reported GREEN); a per-universe
  anti-vacuity guard and a coverage key that notices when the unverified fraction rises.
- **"Not driving a node does not isolate it"** — `crash_now`/`teardown_host` tear down only the
  crashed side; use `recover_node` to force a real reconnect.
- Mutation-proven teeth: a test that passes on arrival has not been shown to work. Mutate the
  production code, watch it redden **by name**, restore byte-exact and prove it (sha256 or
  `git diff` against the packaged diff).
- Seeds: pin every one, use the reserved range, record it in the plan's seed table.
- Use `checks::classify` / `is_fault_fallout`; do not hand-roll a fifth copy.
- The `CARGO_TARGET_DIR` worktree hazard when running mutations.

**Form:** red-flag table plus checklist. This is a genuine discipline failure — the agent knows
what a test is and writes one that cannot fail — which is the one case where prohibition-shaped
guidance is the right instrument.

### `charpente-adversarial-review`

**Trigger:** reviewing a charpente diff, task, or branch, including re-reviews after a fix round.

**The failure:** review that reads code instead of interrogating it.

**Content:**
- **Write the report to a file first; return a short status.** Deliveries get lost — both
  research agents in this very session went idle without delivering their summaries, and only
  the file survived.
- The three defect classes, verbatim from the repo's own reviews: (i) an assertion that cannot
  fail, (ii) an assertion that fails for a reason other than the one it states, (iii) a property
  the report claims is covered that nothing asserts. Plus the fourth this repo keeps hitting:
  prose the code disproves.
- **Measure, don't argue.** The strongest finding in the ledger is "I proved the coverage gate
  has teeth by a mutation the implementer did not run." The strongest counter-example is 0b's
  I-2, where the recommended fix was implemented, measured, and reverted because the new cover
  key fired in 0 of 256 universes.
- The isolation protocol: detached worktree at BASE, its own `CARGO_TARGET_DIR`, every mutation
  reverted and the tree proven byte-identical before and after.
- Verdict format: `READY` / `READY WITH FIXES` / `NOT READY`, with Critical / Important / Minor
  counts.

**Form:** positive recipe — the report IS these parts, in this order — plus the defect-class
checklist.

### `charpente-evidence-sweep`

**Trigger:** before claiming a task, sub-project, or interlude complete, or before committing.

**The failure:** "done" asserted without the evidence that backs it.

**Content:**
- The stage list: fmt; clippy `-D warnings`; the guide compile gate; `cargo test --workspace`
  twice with identical counts; pinned debug replays (`0x8bee1752811ae873`,
  `0x348270b38f166529`); both release mode-skew tripwires exactly as `ci.yml` runs them; a fresh
  invoicing 1024-universe batch; the edge matrix.
- **A blast-radius conditional, not a flat rule** — keyed to an observable predicate, per §1's
  form-selection finding. The edge matrix is owed *iff* replication, auth or realtime behaviour
  is touched. A docs-only commit owes fmt/clippy/guide-gate and a stated reason for skipping the
  rest. Both precedents are in CLAUDE.md, including one sweep that deliberately skipped the edge
  matrix and said why.
- **Count reconciliation is mandatory.** State the test-count delta and reconcile it exactly
  ("572→583 reconciles as Task 1 +1, Task 2 +5, Task 3 +5"). An unreconciled delta is a finding.
- **`tee`, never `tail`** — tail eats the aggregate counts *and* replaces cargo's exit status
  with its own. This is recorded scar tissue: it cost one sweep its counts and nearly cost another.
- Do not run `scripts/bench.sh --smoke` back-to-back with `cargo test --workspace`.
- Report honestly: a stage that went red then green is reported as both runs, never as its clean
  attempt.
- Measure CPU time, not wall clock.

**Form:** positive recipe plus one conditional.

### `charpente-doc-truth`

**Trigger:** a change touching public API; sub-project close-out; editing DESIGN, GUIDE, BENCH,
README, or a spec.

**The failure:** shipping documentation the code disproves.

**Content:**
- Hard rule 6 as procedure: for each factual claim, locate the code or test that establishes it;
  quote every error string from its pinning test byte-exact; mark claims nothing pins.
- The case file: GUIDE's seed-a-brand-new-follower paragraph, contradicted by its own branch's
  tests; BENCH.md's three defects; DESIGN's promote-path dual-primary claim disproven by a green
  test on the same branch.
- Prose coupling is real and deliberate in two places — redb's remedy prose in the latch stop
  message, matched by `checks::classify` — so a reword reddens locally by design.

**Form:** positive recipe.

## §3 The personas

Built after the skills, each justified on a documented ground, each measured against a control.

| Persona | Ground | Composition |
|---|---|---|
| `charpente-reviewer` | **Parallelism** — three concurrent instances, one per defect class, mirroring `feature-dev`'s three-reviewer pattern. Plus opus pinning. | `model: opus`, `effort: high`, `skills: [charpente-adversarial-review, charpente-sim-properties]` |
| `charpente-sweeper` | **Context isolation** — an 11-stage sweep emits enormous output that must not land in the controller's window | `skills: [charpente-evidence-sweep, charpente-doc-truth]` |
| `charpente-preflight` | **Context isolation** — a census reads broadly and returns a table | `model: opus` |

`charpente-implementer` is **deliberately not in this table.** Its only justification is model
pinning, which superpowers already achieves with `model:` on a general-purpose dispatch. It is
built only if §5's measurement shows a persona beating that control.

**Persona bodies do not restate project rules.** CLAUDE.md already reaches every subagent; a
persona that repeats it pays the cost twice.

**Footguns, all documented, all designed around:**
- Restricting `tools:` **silently removes `Skill`** — every "read-only researcher" persona copied
  from the docs example loses skill access. Use `disallowedTools`, or list `Skill` explicitly.
- A typo in `skills:` is **skipped with only a debug-log warning**. Verify preloading with
  `--debug` at least once per persona.
- A new `.claude/agents/` directory **does not load until a session restart**. Charpente has none
  today, so this applies to the first commit.
- Duplicate `name` in one tree resolves by filesystem read order, undocumented. `/doctor` reports it.

## §4 The CLAUDE.md split

`CLAUDE.md` keeps the standing instructions only: what this is, commands, workspace map,
confidentiality boundary, the six hard rules, style, workflow, a short current-state, and one
new line directing subagent dispatch to the personas rather than `general-purpose`.

The 1,238-line narrative moves **verbatim, nothing deleted** to `docs/LEDGER.md`, with a pointer
from CLAUDE.md. Git history and `.superpowers/sdd/` already hold the underlying detail; this
changes where the narrative lives, not whether it exists.

Expected saving: ~26k tokens per conversation **and** per subagent dispatch.

**Sequencing hazard.** A concurrent session (`charpente-34`) is mid-flight on group-commit T9 and
also edits CLAUDE.md — commit `25d81d2` in this branch's recent history is exactly that. HEAD moved
from `fceb21d` to `ba30ad7` during this design session. The split lands as its own commit, after a
fresh `git status`/`git log` check, committing named paths only.

## §5 The RED-first method

Per skill, in order:

1. Choose a realistic task from the repo's own history (for `sim-properties`, a property test with
   a known vacuity trap; for `doc-truth`, a doc edit whose claim the code contradicts).
2. Dispatch a fresh subagent **without** the skill, with the repo as it stands. Record verbatim what
   it produces and where it fails.
3. **The control condition binds:** if the baseline does not exhibit the failure, the skill is not
   written. Record the negative result.
4. Write the skill against the specific failures observed — not against hypothetical ones.
5. Re-run the same scenario with the skill preloaded. Record compliance.
6. Archive both transcripts in the ledger.

`skill-creator`'s benchmark tooling (with-skill vs without-skill pass rate, blind A/B, description
tuning on should-trigger/should-not-trigger hit rates) is available on this machine and is used
where it fits.

Persona measurement (§3) uses the same shape: the control is a `general-purpose` dispatch with the
same model and the same preloaded skills.

## §6 Non-goals

- **No skill for the hard rules, the deny-list, or the workspace map.** Those are always-true facts
  and belong in CLAUDE.md, which every subagent already receives. Anthropic's framing: CLAUDE.md =
  facts, skills = repeatable procedures.
- **No fork of superpowers.** These skills layer on top and defer to it for the generic process
  (brainstorming, writing-plans, TDD, worktrees, the sdd loop itself).
- **No persona zoo.** The census is explicit that persona-maximalist collections are where
  description-collision failures live.
- **No `allowed-tools` grants in any committed skill.** Anthropic's warning is that workspace trust
  does not gate that field and a repo skill can grant itself broad access; a public-facing repo
  should not ship one.
- **No rewrite of existing specs, plans, or ledgers.** They are the record.

## §7 Risks

| Risk | Read | Mitigation |
|---|---|---|
| Skills go stale as the codebase moves | Real — this is the doc-truth failure applied to the skills themselves | Every skill cites its ledger evidence, so a stale claim is traceable; `charpente-doc-truth` covers `.claude/skills/` |
| Skill-listing budget dilution | Arithmetic, documented | Four skills; `/doctor` estimates the cost; descriptions kept narrow and non-overlapping |
| Concurrent session conflict on CLAUDE.md | Live, observed this session | Named-path commits, fresh git check before each |
| Personas ship without earning it | The failure this design is most exposed to | §3's control; a persona that does not beat it is not shipped |
| A baseline shows no failure | Expected for at least one skill | That is a result, not a setback — record it and drop the skill |

## §8 Exit criteria

1. Every shipped skill has a **recorded baseline failure** and a **recorded pass** with the skill
   present. No skill ships on assertion alone.
2. Any skill whose baseline did not fail is **documented as dropped**, with the transcript.
3. Every persona has beaten its `general-purpose` control, or is not shipped.
4. `.claude/skills/` and `.claude/agents/` are committed and verified loading — skills visible in
   `/doctor`, preloads confirmed once under `--debug`.
5. CLAUDE.md is ≤200 lines with the narrative preserved verbatim in `docs/LEDGER.md`, and the
   token saving is measured and stated.
6. The repo's own gates stay green across the change: `cargo fmt --all --check`, `cargo clippy
   --workspace --all-targets -- -D warnings`, and `cargo build -p guide-examples`. No Rust source
   is touched by this sub-project, so the full 11-stage sweep is **not** owed — per §2's own
   blast-radius rule, and stated here so the omission is a criterion rather than a gap.
