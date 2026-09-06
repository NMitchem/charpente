# Oracle density: the reviewer reviews itself — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** make "the suite is green" mean "the suite reached a verdict," and make the
framework catch mechanically what an outside agent got wrong when it built on the docs.

**Spec:** `docs/superpowers/specs/2026-09-06-oracle-density-design.md` — read it before
Task 1. The plan argues from the spec; both travel together. Rulings R1–R5 are already
taken there and are not reopened by a task.

**Tech Stack:** Rust, `charpente-sim` (seeded fault injection, the swarm runner),
`charpente` (the suites under `tests/`), `charpente-macros` (proc macros + trybuild),
`charpente-cli` (may spawn processes), `bench-harness` (outside the deterministic boundary).

## Global Constraints

- **Hard rule 1 — no ambient nondeterminism inside the core.** All IO through
  `Clock`/`Entropy`/`Disk`/`Net`/`ObjectStore`. The clippy deny-list enforces it; **no
  `#[allow]` may be added to pass clippy.**
- **Hard rule 2 — no escape hatches on guarantees.**
- **Hard rule 3 — sim-first.** A bug found in sim gets its seed recorded. **State the seed
  count in every commit that touches a property.**
- **Hard rule 4 — assertions in the TigerBeetle style,** including negative space; on in
  release.
- **Hard rule 5 — error messages are product surface.** A contract violation names the
  contract, the site, and the fix. Write tests against error text.
- **Hard rule 6 — public-API changes update the docs in the same commit.**
- **Branch `oracle-density`, in the main worktree (spec R2). Do NOT merge to `main`
  (spec R3).** No Co-Authored-By trailers. `git add` **named paths only**, never `-A`
  or `.`.
- **Full check:** `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --
  -D warnings`, `cargo test --workspace`. **The workspace suite cannot be relied on to
  finish inside one Bash call — partition per package.** This machine OOM-killed two runs
  on 2026-09-06.
- **This shell is zsh.** `${PIPESTATUS[0]}` expands to empty and reads as "no failure".
  Use `${pipestatus[1]}` or `set -o pipefail`. Pipe cargo through `tee`, **never `tail`** —
  `tail` eats the aggregate counts.
- **The replication/auth/realtime suites live in `crates/charpente/tests/`, not
  `charpente-sim/tests/`.** Run them as `cargo test -p charpente --test replication`.
- **Replay:** `CHARPENTE_SEED=0x… cargo test -p charpente --test <file> -- --nocapture`.
  Coverage `require:` keys are **skipped** under `CHARPENTE_SEED` — a single-seed replay
  can never prove a floor.
- **Attribution runs need one property per invocation:** the coverage block is tagged with
  the test FILE, never the test function, so two properties in one file are
  indistinguishable even run serially. Use
  `cargo test -p charpente --test replication <name> -- --exact --test-threads=1 --nocapture`.

## Ground truth established by survey (do not re-derive)

| fact | citation |
|---|---|
| `finish_universe` maps an `INCONCLUSIVE` error to `cover("repl:inconclusive")` + `Ok(())` | `crates/charpente/tests/replication.rs:253-261` |
| cause keys already exist at the failure sites | `:240` spawn-exhausted, `:327` role-file-torn, `:382` store-bricked, `:402` recover-exhausted |
| `RECOVER_ATTEMPTS = 500`, `RECOVER_CYCLE_EVERY = 8`; `recover_node` gates the power-cycle | `replication.rs:423,428,355-404` |
| `recover_auth_node` power-cycles EVERY attempt, budget 200 | `crates/charpente/tests/auth.rs:36,277-297` |
| `recover_rt_node` power-cycles EVERY attempt, budget 200 | `crates/charpente/tests/realtime.rs:1443,1645-1677` |
| the correct gated shape already exists in a realtime sibling | `realtime.rs:657` (`SETTLE_CYCLE_EVERY = 8`), `:718` |
| `CONVERGED_FLOOR = 167` / `FINGERPRINT_FLOOR = 17`, wired to one property | `replication.rs:924,897-898` |
| the generic floor check | `crates/charpente-sim/src/lib.rs:1695-1712` |
| coverage is a flat `BTreeMap<&'static str, u64>`; **no pass/gave-up outcome exists** | `charpente-sim/src/lib.rs:405-417,588-596` |
| `run_universe`'s Ok branch collapses verdict and gave-up | `charpente-sim/src/lib.rs:1727-1741` |
| `UniverseOutcome` in the CLI harness, same collapse | `charpente-sim/src/harness.rs:253` |
| a green `charpente test --sim` prints only `"sim: N universe(s) per test, all green (master seed: 0x…)"` | `harness.rs` formatters |
| `RouteInfo.auth` already exists (spec R4) | `crates/charpente/src/describe.rs:76` |
| `TableOps.scoped` exists at runtime but never reaches describe | `crates/charpente-core/src/db.rs:173`, `:2674-2696`, `describe.rs:55-63` |
| `Db::table_scoped` is already public | `charpente-core/src/db.rs:2662` |
| the field-addition precedent to copy | `crates/charpente/tests/describe.rs:1170-1206` + `capability_app()` `:1090-1163` |

## The five properties sharing `finish_universe`

| # | property | line | universes | base seed | `require:` today | `repl:logical-converged` reachable? |
|---|---|---|---|---|---|---|
| P1 | `follower_converges_to_the_primary_fingerprint` | :880 | 256 | `0x2EB1_0001` | 4 keys | yes — **the only floored property** |
| P2 | `late_follower_seeds_below_the_gcd_base_and_converges` | :1142 | 256 | `0x2EB1_0002` | 1 key | yes, unrequired |
| P3 | `switchover_mid_write_load_loses_nothing_and_serves_reads_throughout` | :3700 | 256 | `0x2EB1_0009` | 1 key | only on the clean `CtrlOk` branch |
| P4 | `failover_preserves_everything_the_follower_acked` | :3979 | 128 | `0x2EB1_000A` | **none** | **never** — different oracle |
| P5 | `wal_corruption_on_the_primary_no_longer_loses_acked_records_with_a_follower` | :5187 | 128 | `0x2EB1_000E` | 3 keys | **never** — kernel-level oracle |

**P4 and P5 are the two 128-universe properties, and P4 has no `require:` at all.** If the
ledger's unattributed `109` lands on either, that is an ~85% give-up rate on a property
reporting green. Task 2 measures which.

---

### Task 1: Port the gated power-cycle to `recover_auth_node` and `recover_rt_node`

The ledger is explicit that this must land **before** any floor is measured on the auth or
realtime properties, or the floors get taken against a known-bad retry shape. `settle_boot`
measured the every-attempt power-cycle at 0 successes in 20,000 boots.

**Files:**
- Modify: `crates/charpente/tests/auth.rs` (`recover_auth_node`, ~:273-300)
- Modify: `crates/charpente/tests/realtime.rs` (`recover_rt_node`, ~:1645-1677)
- Read first: `crates/charpente/tests/replication.rs:355-404` (`recover_node` — the shape to
  copy) and `crates/charpente/tests/realtime.rs:716-720` (`settle_boot` — the same shape
  already correct in this very file)

**Interfaces:**
- Consumes: nothing.
- Produces: a retry shape Task 3 may measure floors against.

- [ ] **Step 1: Read all three reference implementations before editing anything.**

```bash
sed -n '349,410p' crates/charpente/tests/replication.rs
sed -n '650,725p'  crates/charpente/tests/realtime.rs
sed -n '270,302p'  crates/charpente/tests/auth.rs
sed -n '1640,1680p' crates/charpente/tests/realtime.rs
```

Write down, in your report, the exact difference between the gated shape and the
every-attempt shape. Do not guess it from the constant names.

- [ ] **Step 2: Add the cycle constant to `auth.rs` and gate the power-cycle.**

Mirror `replication.rs:428`'s constant and `:370`'s gate. Name it `AUTH_RECOVER_CYCLE_EVERY`
and give it the same value (`8`) as its two siblings, with a rationale comment saying why a
power-cycle every attempt is the wrong shape (it never lets a boot settle) and citing the
`settle_boot` 0-of-20,000 measurement.

- [ ] **Step 3: Do the same in `realtime.rs` for `recover_rt_node`.**

`realtime.rs` already has `SETTLE_CYCLE_EVERY = 8` at `:657`. **Reuse it rather than
declaring a second constant** — one file, one meaning for "how often to power-cycle a stuck
boot." If reuse turns out to be wrong because the two callers want different cadences, say
so in your report with the reason rather than silently adding a constant.

- [ ] **Step 4: Prove the change fires.**

This is a retry-shape change, so the proof is that the give-up count MOVES. Before and
after, run each suite once and capture the `INCONCLUSIVE`-related coverage counts:

```bash
set -o pipefail
cargo test -p charpente --test auth -- --test-threads=1 --nocapture 2>&1 | tee /tmp/auth-after.log
cargo test -p charpente --test realtime -- --test-threads=1 --nocapture 2>&1 | tee /tmp/rt-after.log
```

Record the before/after counts in your report. **A count that does not move is a finding,
not a pass** — report it rather than declaring success.

- [ ] **Step 5: Commit.**

Named paths only. The message states the seed count of the suites run and the
before/after give-up counts.

---

### Task 2: Attribute every give-up, one property per invocation

Pure measurement. No production code changes. This produces the number the whole phase
turns on, and the ledger records it as never having been made.

**Files:**
- Create: `docs/superpowers/specs/2026-09-06-oracle-density-design.md` gains an
  "Attribution, measured" section (append; do not rewrite existing sections)
- Read: `crates/charpente/tests/replication.rs`

**Interfaces:**
- Consumes: Task 1's retry shape (auth/realtime only; replication was already correct).
- Produces: the per-property give-up table Task 3 sets floors from. **Task 3 cannot start
  without it.**

- [ ] **Step 1: Run each of P1–P5 alone and capture coverage.**

One invocation per property — the coverage block is tagged by FILE, so two properties in one
run are indistinguishable. Do NOT set `CHARPENTE_SEED` (it disables `require:` handling).

```bash
set -o pipefail
for p in follower_converges_to_the_primary_fingerprint \
         late_follower_seeds_below_the_gcd_base_and_converges \
         switchover_mid_write_load_loses_nothing_and_serves_reads_throughout \
         failover_preserves_everything_the_follower_acked \
         wal_corruption_on_the_primary_no_longer_loses_acked_records_with_a_follower; do
  cargo test -p charpente --test replication "$p" -- --exact --test-threads=1 --nocapture \
    2>&1 | tee "/tmp/attr-$p.log"
done
```

These are long runs. Run them one at a time and let each finish; if a run exceeds the tool's
time limit, run it in the background and collect the log rather than reducing the universe
count — a reduced count answers a different question.

- [ ] **Step 2: Build the attribution table.**

For each property record: universe count, `repl:inconclusive` total, and the split across
the four cause keys (`:spawn-exhausted`, `:role-file-torn`, `:store-bricked`,
`:recover-exhausted`). Compute the give-up **rate** as a percentage of that property's
universe count.

- [ ] **Step 3: Answer the ledger's open question explicitly.**

The ledger asks where the `109` lands and whether it is an ~85% rate on a 128-universe
property. Answer it in one sentence with the measured number. If the counts you measure
differ from the ledger's `90, 39, 45, 109, 89`, **say so and report both** — the ledger's
figures came from one run and are not a baseline you must reproduce.

- [ ] **Step 4: Also attribute the sixth wrapper.**

`repl:failover-inconclusive` has four `cover(); return Ok(())` sites
(`replication.rs:4000-4003, 4004-4007, 4014-4023, 4033-4036`) that bypass the `INCONCLUSIVE`
string convention entirely, on a property with no `require:`. Record its count and which of
the four sites fire.

- [ ] **Step 5: Write the section and commit.**

Append "## Attribution, measured (2026-09-06)" to the spec with the table, the rate column,
the answer to the ledger's question, and the command used. Commit the spec alone.

---

### Task 3: A floor for every property, on a key its own oracle can reach

`CONVERGED_FLOOR` pins P1. The other four are unpinned, and two of them cannot reach
`repl:logical-converged` at all — so pinning that key there would be an assertion that can
never fire, the exact defect this sub-project exists to remove.

**Files:**
- Modify: `crates/charpente/tests/replication.rs` (the `require:`/`floor:` clauses of P2–P5)

**Interfaces:**
- Consumes: Task 2's measured table. **Every floor must be justified by a measured number,
  not chosen.**
- Produces: properties that go red when their give-up rate regresses.

- [ ] **Step 0 (added 2026-09-06 by controller ruling J): find out whether the give-ups are
      a budget or a liveness bug, BEFORE setting any floor.**

Task 2 measured give-up rates of 64.1% (P4, union of both channels) and 70.3% (P5), with
P1/P2/P4 at 100% `:recover-exhausted`. A floor set on top of that pins the status quo — and
if the status quo is a liveness bug rather than a budget that is too small, the floor
enshrines the bug and makes it permanent. The ledger has carried this question open since
2026-09-05 ("whether `RECOVER_ATTEMPTS` is too small or masks a liveness bug") with the
experiment named and explicitly not run.

Run it. Raise `RECOVER_ATTEMPTS` (`replication.rs:423`) from 500 to 5000 **as a temporary
local edit, not a commit**, and re-run three properties: P2
(`late_follower_seeds_below_the_gcd_base_and_converges`, 100% recover-exhausted), P4
(`failover_preserves_everything_the_follower_acked`), and P5
(`wal_corruption_on_the_primary_…`). Then revert the edit and verify the file's blob
matches HEAD.

Read the result as follows, and state which case you are in:
- **Give-up rate collapses** → it is a budget. Report the new rate, and say what budget
  would be needed for a given target verdict rate. Set floors against the CURRENT budget
  (do not change it in this task — that is its own decision, with its own runtime cost),
  and record the finding.
- **Give-up rate barely moves** → it is not a budget. Something is failing to make progress
  under fault, and that is a real liveness finding on a property that guards a shipped
  guarantee. Say so plainly, set no floor on the affected property, and stop for a ruling.

**Do not skip this step because the floors can be set without it.** They can. The point is
that a floor whose number encodes an unexamined bug is the exact defect this sub-project
exists to remove.

- [ ] **Step 1: For each of P2–P5, identify the key its oracle actually reaches.**

P1's floors are on `repl:fingerprint-compared` and `repl:logical-converged`. P4 and P5 use
different oracles (a WAL-prefix byte check and a kernel-level check). Read each property and
name the coverage key that means "this universe reached a real verdict" **for that
property**. If a property has no such key, add one at its own verdict site — that is in
scope for this task.

- [ ] **Step 2: Set each floor from Task 2's measurement, with headroom.**

Follow `CONVERGED_FLOOR`'s existing precedent for how much headroom a floor carries below
the measured value, and quote that precedent in a comment. Each constant gets a doc comment
naming the measured value it came from and the date.

- [ ] **Step 3: Give P4 a `require:` set.**

P4 (`failover_preserves_everything_the_follower_acked`, 128 universes) has none. At minimum
it requires the key that means its oracle ran.

- [ ] **Step 4: Mutation-prove each new floor.**

A floor that cannot fail is worse than no floor. For **each** new floor constant, raise it
above the measured value, confirm the property goes RED **by name**, restore it, and confirm
GREEN. Record both messages verbatim in your report. Restore every mutation and verify the
file's git blob matches HEAD before committing.

- [ ] **Step 5: Commit,** stating the universe counts and the measured values each floor
      rests on.

---

### Task 4: The sixth wrapper adopts the convention it bypasses

`repl:failover-inconclusive`'s four sites return `Ok(())` directly rather than going through
the `INCONCLUSIVE` string, which is why a marker grep cannot find them and why no `require:`
covers them.

**Files:**
- Modify: `crates/charpente/tests/replication.rs` (~:3979-4040)

**Interfaces:**
- Consumes: Task 2's count for this wrapper.
- Produces: a sixth wrapper that is visible to the same tooling as the other five.

- [ ] **Step 1: Read all four sites in full** and record what condition reaches each.
- [ ] **Step 2: Route each through the `INCONCLUSIVE` convention** so `finish_universe`
      credits it, or — if a site is genuinely a *verdict* rather than a give-up — say so
      explicitly in your report and leave it, with the reason. Do not convert a verdict into
      a give-up to make the shapes uniform.
- [ ] **Step 3: Add a cause key per site**, in the style of `:spawn-exhausted` et al.
- [ ] **Step 4: Add the `require:`/floor** justified by Task 2's measurement.
- [ ] **Step 5: Mutation-prove** as in Task 3 Step 4, and commit.

---

### Task 5: "Gave up" becomes a reported outcome, not a coverage string

The generalization the whole phase exists for. Today a universe that gives up is
indistinguishable from one that passed: both are `Ok(coverage_map)`. This makes the
distinction structural, so no future property can open this hole silently.

**Files:**
- Modify: `crates/charpente-sim/src/lib.rs` (`run_universe` ~:1727-1741, the swarm runner,
  the floor check ~:1695-1712)
- Modify: `crates/charpente-sim/src/harness.rs` (`UniverseOutcome` ~:253, `Report`, both
  formatters)
- Modify: `crates/charpente-cli/` (the `test --sim` human and json output)
- Modify: `docs/GUIDE.md` (hard rule 6 — the sim chapter's description of what a run prints)

**Interfaces:**
- Consumes: nothing structural from Tasks 1–4; land it after them so the counts it prints
  are already honest.
- Produces: the channel Phase 3 reports through.

- [ ] **Step 1: Design the outcome type before writing it.** Record in your report: the
      enum, where it is produced, and every function signature that changes. `run_universe`
      returning `Ok(coverage_map)` for both cases is the thing being fixed.
- [ ] **Step 2: Thread it** through `run_universe` → the batch runner → `Report`.
- [ ] **Step 3: Print it.** Human format: a green run must state verdicts and give-ups, not
      just "all green". JSON format: a machine-readable field. **A run where every universe
      gave up must not print as an unqualified success** — that is the sentence in
      MANIFESTO §9 this task exists to make true.
- [ ] **Step 4: Test it.** A property that always gives up must produce a report that says
      so. Use `crates/sim-fixture` for an end-to-end assertion, per the precedent in the
      2026-09-05 `acked_implies_durable` fix (a real app workload, so the assertion runs
      through the harness rather than a unit test).
- [ ] **Step 5: Mutation-prove** that the give-up count is real: make the fixture property
      give up in a known number of universes and assert the reported number matches.
- [ ] **Step 6: Update GUIDE** in the same commit (hard rule 6) and commit.

---

### Task 6: `TableInfo.scoped` — the inventory that is actually missing

Spec R4: the route-auth half of this item already shipped. The table half did not. The
runtime bool exists and is already public; it simply never reaches the describe output.

**Files:**
- Modify: `crates/charpente-core/src/db.rs` (`TableDescription` ~:194-200,
  `describe_tables()` ~:2674-2696)
- Modify: `crates/charpente/src/describe.rs` (`TableInfo` ~:55-63, its builder, `to_human`)
- Modify: `crates/charpente-cli/src/genclient.rs` (the hand-maintained `Deserialize` mirror
  and its converter)
- Modify: the three golden JSON files pinned by the describe suites
- Modify: `docs/GUIDE.md` §13 (hard rule 6; see Task 7 for its oracle)
- Read first: `crates/charpente/tests/describe.rs:1090-1206` — `capability_app()` and
  `describe_names_exactly_the_cross_tenant_routes`, the precedent for both the field and
  its test

**Interfaces:**
- Consumes: nothing.
- Produces: a `scoped` field on every table in describe output. Task 7 gives GUIDE's copy
  of that output an oracle.

- [ ] **Step 1: Read the `cross_tenant` precedent end to end** — macro → registration →
      `RouteInfo` → golden → test. Your field follows the same path from
      `TableOps.scoped`. Note in your report where your path differs (it starts at a
      registration-time computed bool, not a macro flag).
- [ ] **Step 2: Carry `scoped` from `TableOps` into `TableDescription`, then `TableInfo`.**
      `row_is_scoped::<R>()` already computes it at `Schema::table::<R>()`
      (`db.rs:240`); `Db::table_scoped` (`db.rs:2662`) already exposes it. Do not recompute
      it — carry the existing value.
- [ ] **Step 3: Give the field a doc comment in the house style.** `RouteInfo.cross_tenant`'s
      comment is the model: it says why the field is there and why it is beside its
      neighbour. Say why an audit asks this of a table.
- [ ] **Step 4: Update `to_human` and the CLI mirror.** Both are compiler-checked; a missed
      one will not build.
- [ ] **Step 5: Update the three golden JSON files.** Find them by running the describe
      suites and reading the failures; do not guess their paths.
- [ ] **Step 6: Add the test**, modelled on `describe_names_exactly_the_cross_tenant_routes`.
      `capability_app()` already mounts a scoped `Ledger`/`Org` table and asserts nothing
      about scoping — assert it. The test must name **both** a scoped and an unscoped table,
      or it cannot fail if the field is hard-coded.
- [ ] **Step 7: Mutation-prove:** hard-code the field to `true`, confirm the test goes RED by
      name; hard-code to `false`, confirm RED. Restore and confirm GREEN. Both directions,
      because a single direction leaves the constant unpinned.
- [ ] **Step 8: Commit** with GUIDE §13 updated in the same commit.

---

### Task 7: An oracle for GUIDE §13's describe JSON

Spec R5. `docs/GUIDE.md` §13 carries a full `describe` JSON block whose own text claims
byte-for-byte parity with real output, and nothing checks it: `guide-examples`' build.rs
extracts only ` ```rust ` fences, so that block drifts silently. Task 6 is about to change
that output, which makes the gap ours under hard rule 6.

**Files:**
- Modify: `crates/guide-examples/` (build.rs and/or a new test)
- Modify: `docs/GUIDE.md` §13 if the block proves already drifted
- Read first: `crates/guide-examples/build.rs`, `crates/guide-examples/tests/doc_inventory.rs`
  (the existing doc-oracle precedent in this crate)

**Interfaces:**
- Consumes: Task 6's new field.
- Produces: a gate that fails when GUIDE §13's JSON and real describe output disagree.

- [ ] **Step 1: Determine whether the block is already drifted** — before building the
      oracle, generate the real output for the app §13 documents and diff it by hand.
      Report the answer; a block that is already stale is itself a finding worth its own
      line in the commit message.
- [ ] **Step 2: Decide the oracle's shape and record the reasoning.** The two candidates are
      (a) extract the ```json fence and compare it to output generated in a test, and
      (b) generate the block and require the doc to match a committed artifact. Follow
      `doc_inventory.rs`'s precedent for how this crate states such a failure. **Hard rule 5
      applies to the failure message:** it must name the doc, the section, and the command
      that regenerates it.
- [ ] **Step 3: Implement it.**
- [ ] **Step 4: Mutation-prove:** perturb one byte of GUIDE §13's JSON, confirm the gate goes
      RED by name; restore, confirm GREEN. Then perturb the *code* side (a field rename) and
      confirm RED from that direction too.
- [ ] **Step 5: Add the new gate to CLAUDE.md's test inventory** if it is a new test file —
      `guide-examples`' `doc_inventory` gate enforces that list, so a new test outside it
      reddens the build. Commit.

---

### Task 8: Rule the mutation-mode design, and record it

The one genuinely open design question in this plan. Nothing in Phase 3 may start until it
is answered, because the answer changes the blast radius by an order of magnitude.

**Files:**
- Modify: `docs/superpowers/specs/2026-09-06-oracle-density-design.md` (append a
  "Mutation mode, ruled" section)
- Read: `crates/charpente-macros/src/job.rs` (esp. the glue at ~:208-217, `run_call`),
  `crates/charpente-macros/src/route.rs`, `crates/charpente-cli/src/sim.rs` (the sanctioned
  `Command::new` precedent)

**Interfaces:**
- Consumes: nothing.
- Produces: the ruling Tasks 9 and 10 implement.

**Survey findings you are ruling on (established; do not re-derive):**
- `cargo-mutants` (MIT, v27.1.0, actively maintained, CLI-only) patches source in a scratch
  directory and does a full rebuild per mutant. No cheap mode. Scoping via `-f`/`--file`,
  `-F`/`--re`, `#[mutants::skip]`, and `--cargo-test-arg`. Expect minutes-plus per mutant on
  this proc-macro-heavy workspace.
- `mutagen` is dead (2018, nightly-only).
- **Asymmetry:** `#[job]` never sees the handler body, but its emitted glue is a clean
  neutering point because every job's `run` uniformly returns `Result<(), JobError>` — so
  "make this job a no-op" is generic and cheap. `#[route]` *does* see the body but has an
  app-chosen `Result<T, ApiError>` with **no generic safe stub**, so route-body neutering is
  not a small change.
- Neither macro has any cfg/test-mode branching today.
- **The trap, and it is the important one:** cargo's fingerprinting does not track
  env-var-driven proc-macro output. A driver that does not force a rebuild between mutation
  runs will silently reuse a stale, unmutated expansion and report a **false green** — the
  precise failure this whole sub-project exists to eliminate. Any ruling that selects a
  mutant by env var owes a forced-rebuild mechanism and a test that proves the rebuild
  happened.

- [ ] **Step 1: Read the two macros and the CLI's process-spawning precedent in full.**
- [ ] **Step 2: Rule.** Choose one of: (a) wire `cargo-mutants` from `charpente-cli`;
      (b) build the job-neutering macro mode; (c) both, with a stated division of labour;
      (d) something the survey missed. State the reasoning, the biggest risk, and what it
      costs if the ruling is wrong.
- [ ] **Step 3: State where the code lives** with respect to hard rule 1 — `charpente-cli`
      may spawn processes; core may not. Proc-macro expansion runs in the proc-macro process
      at compile time, not in the deterministic runtime, but say so explicitly rather than
      leaving a reviewer to infer it.
- [ ] **Step 4: Scope Tasks 9 and 10** to the ruling: if the ruling makes either task
      substantially larger or smaller than written below, amend them in this plan file and
      say what changed.
- [ ] **Step 5: Commit the spec and plan amendments together.**

---

### Task 9: Implement the ruled mutation mode

**Files:** per Task 8's ruling.

**Interfaces:**
- Consumes: Task 8's ruling.
- Produces: the ability to neuter one named app handler per run.

- [ ] **Step 1: Implement the mechanism.**
- [ ] **Step 2: Close the stale-expansion trap.** If the ruling selects a mutant by env var,
      the driver MUST force the rebuild, and you MUST prove it did: run the same mutant twice
      with different selections and show the emitted code differed. **A mutation run that
      cannot prove it rebuilt is not evidence** — this is the false-green class the whole
      sub-project targets, and shipping it here would be self-refuting.
- [ ] **Step 3: Test the mechanism itself,** not just its output. The negative-space
      assertion (hard rule 4): selecting a handler that does not exist must fail loudly by
      name, never silently neuter nothing and report green.
- [ ] **Step 4: Commit.**

---

### Task 10: Catch a one-sided property, end to end

The acceptance test for Phase 3, and the reason it exists. The helpdesk app shipped five
properties where deleting the entire SLA sweep loop left the suite green.

**Files:**
- Modify: `crates/sim-fixture/` (a deliberately one-sided property as the victim, and an
  honest sibling as the control)
- Modify: `docs/GUIDE.md` (hard rule 6 — how an app author runs this)
- Modify: `crates/charpente-cli/` per Task 8's ruling

**Interfaces:**
- Consumes: Tasks 5 and 9.
- Produces: the demonstrated claim.

- [ ] **Step 1: Write the victim and the control** in `sim-fixture`: one property that
      genuinely tests its job's effect, and one that is one-sided in the helpdesk shape
      (it asserts something true whether or not the job ran). Comment both as deliberate
      fixtures so a future reader does not "fix" the victim.
- [ ] **Step 2: Demonstrate the catch:** with the job neutered, the control goes RED and the
      victim stays GREEN, and the tool **reports the victim as one-sided**. Capture the exact
      output.
- [ ] **Step 3: Hard rule 5 on the report text** — it must name the property, the handler
      that was neutered, and what the author should do about it.
- [ ] **Step 4: Document it in GUIDE** in the same commit, and add any new test file to
      CLAUDE.md's inventory.
- [ ] **Step 5: Commit,** stating universe counts.

---

### Task 11: The compile-latency instrument

PLAN.md's risk table calls compile latency the accepted headline risk; DESIGN §9.5 calls it
*"the adoption risk"*; M0's plan said to measure it. It has never been measured. This task
builds the instrument only — Task 12 takes the reading.

**Files:**
- Create/modify: `crates/bench-harness/` (a new measure/accounting pair, following the
  existing split)
- Read first: `docs/BENCH.md`'s method sections, `crates/bench-harness/src/*_accounting.rs`
  headers (the split's convention), `crates/bench-harness/src/measure.rs` (`process_cpu()`)

**Interfaces:**
- Consumes: nothing.
- Produces: the instrument Task 12 reads.

**Survey findings (established; do not re-derive):**
- Nothing in the harness times `cargo build` today. This is new instrument work, not a flag.
- **`measure::process_cpu()` measures the parent process only.** A `cargo build` wrapper must
  aggregate child `rustc` processes; the existing instrument does not do this. **This is the
  single most likely way to produce a confidently wrong number.**
- Root `Cargo.toml` sets `[profile.dev.package.redb] opt-level = 3`, which directly slows
  dev/incremental builds — so a charpente incremental number **is not representative of a
  generic Rust project**, and that caveat must travel with it.
- A compile is one-shot, not a rate to window-average: BENCH's usual windowing does not
  apply, and repeated observations are the substitute.

- [ ] **Step 1: Obey the measure/accounting split.** The accounting half carries no
      deny-list suppression, so hard rule 1 mechanically holds it free of clocks and IO.
      Read a sibling's doc header and follow it exactly.
- [ ] **Step 2: Solve the child-process CPU problem explicitly.** State in your report how
      you aggregate `rustc` children, and prove it: a build known to spawn multiple `rustc`
      processes must report more CPU than the parent alone.
- [ ] **Step 3: Define the phases to measure** — at minimum: clean build; incremental after
      a handler-body edit; incremental after a schema change (the case DESIGN §9.5's
      prebuilt-dylib idea targets). Name what each phase tells an adopter.
- [ ] **Step 4: Record cache state and contention as first-class fields.** BENCH.md has
      already been burned once by an unrecorded contention factor; this machine runs
      Spotlight and `syspolicyd` hot. A reading with no contention record is INVALID by
      BENCH's own rules.
- [ ] **Step 5: Implement BENCH's invalidation rules for this instrument** — state which of
      the existing five apply, which do not, and what replaces them. An instrument with no
      invalidation rule produces numbers nobody may quote.
- [ ] **Step 6: Commit** the instrument alone, with no numbers in the message.

---

### Task 12: Take the reading, and write the BENCH.md section

**Files:**
- Modify: `docs/BENCH.md` (a new dated section)
- Modify: `PLAN.md` and/or `DESIGN.md` §9.5 if the reading answers their open question
  (hard rule 6)

**Interfaces:**
- Consumes: Task 11's instrument.
- Produces: the first compile-latency number of record.

- [ ] **Step 1: Establish a quiet machine, and prove it.** Record load before and after.
      **Do not run this concurrently with any test suite.** If the box cannot be made quiet,
      say so and stop — an invalid number in BENCH.md is worse than no number, and BENCH.md
      is the only source of quotable figures in this repo.
- [ ] **Step 2: Take multiple observations per phase,** not one draw. Report the spread, not
      just a central value.
- [ ] **Step 3: Write the section** with everything BENCH requires to travel with a number:
      the layer beneath it, hardware/filesystem/build profile, counted-vs-derived flags, the
      `opt-level = 3` caveat from Task 11, and the cache state.
- [ ] **Step 4: Answer the risk explicitly.** PLAN.md's risk table and DESIGN §9.5 both ask a
      question this reading answers. State the answer plainly — including if the answer is
      "the risk is worse than assumed."
- [ ] **Step 5: Update PLAN/DESIGN** in the same commit (hard rule 6) and commit.

---

## Self-Review

Before declaring the plan complete, confirm each:

- [ ] Every new floor and `require:` was justified by a measured number, not chosen.
- [ ] Every new floor was mutation-proved RED in both directions where a direction exists.
- [ ] No `#[allow]` was added to pass clippy (hard rule 1).
- [ ] Every mutation applied during proof was restored, and each file's blob verified
      against HEAD before commit.
- [ ] Public-API changes updated docs in the same commit (hard rule 6).
- [ ] New test files appear in CLAUDE.md's inventory (`doc_inventory` enforces this).
- [ ] The branch was never merged to `main` (spec R3).
- [ ] Every commit states its seed/universe counts where it touched a property.
