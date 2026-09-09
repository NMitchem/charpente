# Compile latency: earning the number — design

**Status:** draft, 2026-09-08. Authority for the compile-latency reading sub-project.

## 1. Why this sub-project exists

Three north-star documents point at the same unfinished item, and none of them leaves it
to taste.

**PLAN.md's risk table** carries six rows. Exactly one reads *"still unmeasured"*:

> | Rust compile latency poisons the dev loop | The bet accepted going in; **still unmeasured** | … The "measure app-only incremental times in M0" mitigation **did not happen in M0**. An instrument landed 2026-09-07 … and its first reading was refused in full … This row's read stays "accepted bet" because no number has yet been earned to move it. |

**DESIGN §9.5** names the stakes and the gap in the same breath:

> **Compile latency mitigation** — platform as prebuilt dylib so app iteration recompiles
> app code only; quantify the win early, **it is the adoption risk** … **Still unquantified
> as of 2026-09-07.** … Until a quiet machine produces an accepted reading, this item's
> premise that a prebuilt dylib is the right mitigation is untested: **nobody has measured
> which phase actually hurts.**

**MANIFESTO §3** explains why this is product surface rather than developer comfort:

> An agent pointed at a framework succeeds or fails based on **how fast and how precisely
> the framework says *no***.

Charpente's whole thesis is that a schema change becomes a compile error instead of a 500.
That trade is only a win if the compile is fast enough to stay inside the loop. Compile
latency is the "how fast" half of the competitive axis named in §3 — and it is the one
number the project has never had.

**PLAN's M2 list already contains the mitigation** — *"Compile-latency work:
platform-as-prebuilt-dylib; optionally cranelift hot reload for handlers if it proves
out."* DESIGN says that bullet's premise is untested. Building the mitigation before the
measurement would be choosing a remedy for an undiagnosed problem, so this sub-project
stops at the number.

## 2. What already exists, so the plan does not rebuild it

The instrument shipped 2026-09-07 and is complete. This sub-project adds a *runner*, not an
instrument.

`crates/bench-harness/src/compile.rs` (measure) and `compile_accounting.rs` (accounting)
split along hard rule 1's line: the accounting half carries no deny-list suppression, so it
is mechanically free of clocks and IO. `crates/bench-harness/src/bin/compile-bench.rs`
drives them and prints what came back; **every publish/refuse decision belongs to the
accounting half**, and this sub-project must not move that boundary.

Three phases against `examples/invoicing`, three observations each:

- **Clean** — `cargo clean` then a full build. The worst case, met first by a new contributor.
- **Handler body** — one statement inside a route body, no signature change, so nothing
  downstream needs re-typechecking. **The inner loop: the edit an agent makes hundreds of times.**
- **Schema** — an `#[index]` on an existing `#[table]` field. A real schema change that
  still compiles.

`verdict()` refuses an observation by name, in this order: `BuildFailed`, `Suspended`,
`ChildCpuNotAggregated`, `BelowFloor` (`MIN_CHILD_CPU = 500 ms`), `Contended`
(`CONTENTION_CEILING_PER_CORE = 0.5`). A summary additionally refuses
`TooFewObservations` (`MIN_OBSERVATIONS = 3`) and `Unstable`
(`MAX_SPREAD_PERCENT = 30`).

Two guards already handle hazards this sub-project would otherwise have to invent:

- **`Invalid::Suspended`** — *"the machine did not stay settled across the build … wall time
  spans a gap the process did not run through."* Laptop sleep is detected, not silently
  averaged into a wall figure. This matters because the run is unattended.
- **`compile-bench` asserts `CARGO_TARGET_DIR` is set** and refuses without it: the clean
  phase deletes artifacts, and sharing the repo's `target/` would destroy the working cache
  *and* let an unrelated `cargo` perturb the measurement.

The driver also quiesces before each measured build (`QUIESCE_TARGET_PER_CORE = 0.4`, polled
every 10 s up to 300 s) because the load average is a one-minute exponential average and the
harness's own 44 s clean build was still inside the window when the next observation sampled
`load_before` — the first reading refused three clean builds for contention it had
substantially manufactured.

`scripts/` holds `bench.sh`, `edge-harness.sh` and `mutants.sh`. There is no compile-bench
equivalent. That gap is this sub-project's first task.

## 3. Scope, in dependency order

1. **`scripts/compile-bench.sh` — the unattended runner.** A dedicated `CARGO_TARGET_DIR`
   outside the repo; `caffeinate -i` held for the run's duration; full stdout/stderr to a
   timestamped artifact; exit status captured **without a pipe**, since `$?` after a pipe
   reads the filter and not the command. Refuses to start when another `cargo` is live in
   the repo — contention and cache corruption are the same hazard wearing two faces.
   `caffeinate` is *belt* and `Invalid::Suspended` is *braces*: prevention exists so the
   night is not wasted, never so the gate is bypassed. Both stay.

2. **The retry rule (§4).** Encoded in the runner, not left to the operator's judgement at
   3 a.m.

3. **One scheduled shot.** We need one accepted reading, not a nightly job. A one-shot
   avoids installing a LaunchAgent nobody remembers to remove. If a night is lost the shot
   is re-armed by hand — cheaper than owning a recurring job for a one-time measurement.

4. **Publish to `docs/BENCH.md`.** It is the only source of quotable performance numbers in
   this repo; a figure elsewhere is an observation, not provenance. The existing section,
   *"Compile latency — the instrument exists, the reading is refused (2026-09-07)"*, is
   replaced by the reading, its conditioning (hardware, cores, cache state, what each phase
   does and does not mean) and **every refusal along the way, including refused attempts on
   other nights.**

5. **Move the two rows that say "unquantified."** PLAN's risk table and DESIGN §9.5 both
   assert the absence of this number in as many words. Hard rule 6 makes updating them part
   of the same commit as the publish, not a follow-up.

6. **Read the number against the dylib premise — decide, do not build.** DESIGN: *"nobody
   has measured which phase actually hurts."* If clean and schema dominate while the
   handler-body inner loop is already fast, a prebuilt platform dylib is aimed at the phase
   an agent meets least often, and that is a finding worth recording even though it builds
   nothing. The output of this task is a DECISIONS-shaped verdict, and if it contradicts an
   existing DECISIONS entry it goes to Noah with the evidence rather than being applied.

## 4. The integrity rule

This is the core design decision of the sub-project, because "take the reading" degrades
into "run until the numbers look good" through entirely reasonable-seeming steps.

**Retry only on refusals about the environment.** `Contended`, `Suspended`, and the
`TooFewObservations` that follows from them describe the machine, not the system under
measurement. Re-running after them is legitimate.

**Never retry `Unstable`, `ChildCpuNotAggregated`, `BuildFailed` or `BelowFloor`.**
`Unstable` (spread > 30%) is a fact *about the measurement*: it says the fastest and
slowest observations are not samples of one quantity, and rerolling it is precisely
p-hacking. `ChildCpuNotAggregated` and `BuildFailed` are bugs. `BelowFloor` means no
compiler ran. Each of these stops the run and gets reported as a finding.

**Every attempt is recorded and the attempt count is published with the number.** A reading
earned on the fourth night is a different claim from one earned on the first, and the
reader is entitled to know which.

**The gates are not touched.** The 2026-09-07 entry established the bar: `MIN_WALL` was
moved only because it *contradicted its own stated reason*, not because it was in the way,
and moving a gate right after it refuses a reading is how a live oracle turns into
decoration. Nothing in this sub-project may adjust `MIN_CHILD_CPU`,
`CONTENTION_CEILING_PER_CORE`, `MAX_SPREAD_PERCENT` or `MIN_OBSERVATIONS`. If a gate
refuses everything for several nights, that is a finding to report, not a constant to edit.

## 5. What could still make the reading wrong

- **macOS schedules its maintenance when the box is idle.** Spotlight indexing and Time
  Machine are likelier at 3 a.m. than at 3 p.m., so "unattended" is not automatically
  "quiet." The contention gate and the quiesce loop both stay armed; the runner records the
  load trace so a lost night is diagnosable rather than mysterious.
- **A concurrent agent session.** This repo has run two sub-projects at once before. The
  runner's refusal-to-start check covers the repo's own `target/`, but another session's
  unrelated build still costs CPU. The load gate catches it; the artifact records it.
- **Sleep.** Prevented by `caffeinate`, detected by `Invalid::Suspended` if prevention
  fails.
- **The dedicated target dir must not be shared** with any worktree. A shared
  `CARGO_TARGET_DIR` lets one tree's artifacts leak into another's verification with
  `git diff` clean.

## 6. Exit criteria

A reading that `summarize()` accepts for all three phases, published in `docs/BENCH.md`
with its conditioning and its full refusal history; PLAN's risk row and DESIGN §9.5 both
carrying what the number says instead of "unquantified"; and a recorded verdict on whether
the prebuilt-dylib mitigation is aimed at the phase that actually hurts.

**Explicitly not in scope:** building the prebuilt dylib, cranelift hot reload, any change
to the instrument's gates, and any performance work suggested by the number. Those are
separate sub-projects that this one's output would authorize.

## 7. Risks

| Risk | Read | Mitigation |
|---|---|---|
| Several nights refuse and no number is earned | Plausible; the first attempt refused all nine observations | Every refusal is recorded with its load trace, so the refusals themselves become the evidence that this machine cannot produce the reading — which is a publishable finding, not a failure |
| Pressure to relax a gate after a refusal | The named failure mode from 2026-09-07 | §4 forbids it; a gate moves only when it contradicts its own stated reason |
| The number is unflattering | Possible, and it is the point | MANIFESTO §2 and the BENCH provenance rules both say an honest number beats an absent one; PLAN's risk row exists to be moved by evidence in either direction |
