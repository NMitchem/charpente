# Job Tenancy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `Tenant::authorize` is only ever asked about a real, unforgeable principal. Every other way into a tenant is a declaration that `describe` lists, and a scoped job runs in the tenant it was enqueued from. This is M3 pre-publish fix C-2.

**Architecture:**
- **The operator's admission.** Core gains `TenantAdmission`, minted only by `Group::tenant_admission`, and `Tx::admit_scope`, which opens a scope without asking `authorize`. `open_scope` and `admit_scope` share one private body, so the one-scope contract is checked once for both.
- **Route stances.** A route declares `auth`, `public` or `public(Org)`. `Scoped::scope` applies three rules in order:
  1. A route declared `public(S)` for this `S` is admitted by declaration, and no `authorize` runs.
  2. A credentialed caller runs `authorize` once.
  3. An anonymous caller gets a named 401, and nothing below the scope line runs.
- **The principal.** `authorize` takes `&Principal`, not `Option<&Principal>`, and `Principal`'s fields become `pub(crate)`.
- **Scoped jobs.** A job declares `global` or `scoped_by = Org`. A scoped job is enqueued only through `ScopedTx::enqueue*` or the declared fan-out `Tx::enqueue_in`. The tenant rides in a new record tag `0x0a` and a trailing `JobRow.scope`. The dispatcher opens that tenant for the job's `run` through a `JobScope` only it can construct.
- **The fingerprint.** A `scoped_by` job's fingerprint mixes in its scope, so a tenancy change is a shape change and is refused by name.
- **The inventory.** `describe` lists `public_scope` per route and `scoped_by` per job.

**Tech Stack:**
- Rust: `charpente-core` records and tables, and the `syn`/`quote` proc macros.
- The deterministic simulator (`run_universes_swarm_with!`, `SimSource`, the full fault matrix).
- trybuild compile-fail pins; `guide-examples`' doc gates (`doc_truth`, `describe_contract`, the compiled GUIDE blocks).
- `cargo-mutants` 27.1.0 through `scripts/mutants.sh`.

**Spec:** `docs/superpowers/specs/2026-09-27-job-tenancy-design.md` (commit `94c64b6`, approved by Noah on 2026-09-27, including §14's DECISIONS §12 prose correction). Read it first; the plan argues from it. Where this plan differs, the difference is listed under "Spec amendments" below, and Task 1 writes those into the spec.

**Base:** `main` at `d363baf`, which contains the 2026-09-27 pre-publish follow-ups (spec §1 "Sequencing").

**Working directory for mid-flight state:** `.superpowers/sdd/2026-09-27-job-tenancy/` (gitignored). Put these there:
- `progress.md`;
- one gate log per task (`gate-task-N.log`);
- the coverage baselines (Task 1) and each later comparison;
- mutation outputs;
- every reviewer's report.

**A reviewer writes its full report to a file there before returning a summary**, because deliveries have been lost in flight before.

## Spec amendments (Task 1 appends these to the spec, verbatim, under a new "## Amendments (implementation plan)" heading)

- **A1 — the bare `#[job]` refusal lands with `scoped_by`, not before it.** Spec §11 accepts the job words as no-ops, sweeps `global` onto every `#[job]`, and then refuses the bare form. The refusal's text tells the reader to write `#[job(scoped_by = Org)]`, so the refusal lands in the same commit that makes that advice work (Task 6). Task 1 accepts `global`, sweeps it, and proves the sweep with a grep. Task 6's refusal is where the compiler proves it.
- **A2 — `Who = str` scope types keep `open_scope` and pass a name.** Four test scope types implement core's generic `Tenant` with `Who = str`, not the platform's `TenantScope`:
  - `ConvOrg` (`charpente/tests/archive.rs`);
  - `SeedOrg` (`charpente/tests/replication.rs`);
  - `TestOrg` and `AuditOrg` (`charpente-core/tests/engine.rs`).

  Their `open_scope(…, None)` calls become `open_scope(…, "operator")`, or `"member"` in engine.rs (A6). Their `authorize` still runs and still writes nothing. §7's form applies to `Who = Principal` sites: `tenant.rs`'s six. So spec §7's "eight platform-test sites" are six moved to `admit_scope` and two kept on `open_scope` with a name. Core's `Tenant` stays generic over `Who` (spec §2 R2), so these calls remain the honest spelling at that layer.
- **A3 — `build_ctx` takes the route's `Stance`.** `Stance` is private to `charpente::http` and holds three things:
  - the method word (`SSE` for a stream route) and the pattern;
  - whether the route said `auth`;
  - for a `public(S)` route, `S`'s `PublicScope` and the `TenantAdmission` minted for it.

  It is built in two steps. `Declared::of(route)` copies the route's declaration out of the router. `.admitted(&group)` then mints the admission, for a `public(S)` route only.
  - The split exists for the drift probe, which must stop borrowing the router before `App::begin_group` borrows the whole `App`. That is why the probe already reads `found.cross_tenant` first.
  - The request path and the probe build it the same way, so the probe honours `public(S)` exactly as a request does (spec §4).
- **A4 — `tenant.rs`'s counters become thread-locals.** Oracle 2 has seven tests that read them, and `cargo test` runs them on parallel threads of one binary. A request is handled on the thread that called `App::handle`, so a thread-local counts exactly one test's requests. `AUTHORIZE_CALLS` now records the subject of each call, so test (e) can assert *which* principal was asked about.
- **A5 — oracle 4 correlates replies by body.** Every write route returns its request's nonce, so a 200 names the request that produced it. The 401s carry fixed text and no nonce. "Wrote nothing" is therefore checked on the store and the job queue, where every row and job carries its nonce, and on the 200s. It is not checked by matching each 401 to its request.
- **A6 — core's `engine.rs` fixtures pass `"member"` where they passed `None`, and `"stranger"` where they passed `Some("stranger")`.**
- **A7 — Tasks 3 and 4 split spec §11's "land together" group.** Task 3 lands the stances, the three rules and the 401 while `authorize` still takes `Option<&Principal>`; rule 2 passes `Some`. Task 4 then lands the signature and `Principal`'s privacy. Both commits are green. The split lets a reviewer judge the semantic change apart from the compile-forced migration. In the commit between them, nothing the platform does passes `None`; only direct `open_scope(…, None)` calls still can.
- **A8 — the NoJobTenant pinned block quotes the diagnostic's template.** The block uses `<job>` and `<scope>` placeholders, because the attribute source spells `{J}` and `{Self}` and `doc_truth` matches source text. GUIDE §16 also shows the rendered rustc output, unpinned; the `.stderr` fixture pins that. The 401 pinned block quotes oracle 3's concrete instance, which its test pins byte-exact. `PINNED_FLOOR` goes 21 → 22 (Task 3) → 24 (Task 6).
- **A9 — the fingerprint pins are computed, then verified at base.** `SessionSweep` (`before: u64`) is FNV-1a over `"before\0u64\0"` = `0x7e72_9c20_6b79_cbad`. `kernel.rs`'s `CountUp` (`slot: u64`) is FNV-1a over `"slot\0u64\0"` = `0x766b_8ba4_18db_4d08`. Task 1 runs the pin at `d363baf` before changing anything. If it fails there, the literal is wrong, not the code: stop and report.
- **A10 — oracle 4's stripped replays are on all three routes.** A stripped replay on the `public(Shop)` route legitimately succeeds, so the oracle refuses a stripped write only on `auth` and plain `public`. That is exactly spec §9 item 4's "not declared `public(Org)`".
- **A11 — `Stance` records `auth` too.** The drift probe does not enforce `auth`. An `auth` stream route that opens a scope therefore meets rule 3 under the probe, and the probe's `Err` quotes the rule-3 text. With `auth` recorded, that text names the route's real stance.

## Global Constraints

- **Commits.**
  - Commit directly on `main` (CLAUDE.md "Workflow").
  - **No `Co-Authored-By` trailers.** CLAUDE.md is the user's instruction and it overrides the harness's attribution reminder.
  - Before each commit, re-check `git status` and `git log -1`: another session may be writing this tree. Stage named paths only, never `git add -A`.
- **Every commit green.** Tasks are ordered so that no commit leaves the workspace red. Each compile-forced change fixes every site in the same commit.
- **The gate is three commands, counted:**
  1. `cargo fmt --all --check`
  2. `cargo clippy --workspace --all-targets -- -D warnings`
  3. `cargo test --workspace --no-fail-fast 2>&1 | tee <log>`

  Green means three things:
  - `grep -c "^test result:" <log>` equals `grep -c "^test result: ok" <log>`;
  - `grep -c FAILED <log>` is 0;
  - neither count was inferred from the absence of `FAILED`.

  The baseline at `d363baf` is **87 `test result:` lines**. This plan adds no test file, so it stays 87.
  - `crates/charpente-cli/tests/ctl.rs`'s `promote_against_a_dead_port_exits_2_with_the_connect_message` is a known ephemeral-port flake (LEDGER 2026-09-27, its second sighting).
  - If it alone is red, run it alone five times. Report it as the flake only if all five pass, and say so in the gate record.
- **Two markers are filled at execution, never guessed.** `2026-09-2x` is the date of the commit that lands the text. `N` in a universe count or a commit message is the value measured in that task's timing or gate step.
- **The full suite exceeds the 600 s tool ceiling.**
  - **Implementers** run the per-package, per-file commands each task lists, in the foreground.
  - **The controller** runs the full workspace gate in the background (`run_in_background`) and polls the log file, never a pipe through `tail`, before it marks a task done.
  - A subagent that backgrounds `cargo test` can park forever, so dispatches must say "foreground only".
- **zsh, not bash.**
  - `${PIPESTATUS[0]}` is empty here; use `${pipestatus[1]}`.
  - An unquoted `$var` does not word-split.
  - Quote globs (`'*.rs'`).
  - `echo ====` is a zsh error; use `echo '----'`.
  - `timeout` does not exist on this machine; bound a run with perl `alarm` if needed.
- **Error text is product surface (hard rule 5).** Every message below is fixed wording, pinned byte-exact by a test that spells it out independently of the product's `format!`. Do not "improve" a message without changing its pin and its GUIDE quote in the same commit.
- **Docs in the same commit as the behaviour (hard rule 6).**
  - `docs/GUIDE.md`'s ```rust blocks are compiled by `crates/guide-examples/build.rs`, **each block as its own module**, so every block must carry its own `use`s and types. Check them alone with `cargo build -p guide-examples`.
  - A ` ```text` block preceded by `<!-- pinned -->` is checked by `crates/guide-examples/tests/doc_truth.rs`. **Every non-blank pinned line's fragments must appear in some `.rs` under `crates/` or `examples/`.** A fragment is the text between `<lowercase placeholder>`s, trimmed, 16 or more characters. The check applies Rust's literal rules: a `\` + newline continuation vanishes, `\"` becomes `"`, and `\n` becomes a newline.
  - Comment lines are not part of that corpus. A pinning test's literal is, on purpose.
  - `PINNED_FLOOR` is a floor, raised in the same commit as each new pinned block.
- **Red before green (spec §9, hard rule 3).** Every behavioural test is run and seen failing *before* its implementation lands, and the red output is quoted in the task report. A compile error is not a behavioural red.
  - For a test that needs new API, write it against the pre-change behaviour where the spec names that behaviour (Task 3 does this for oracles 2a and 3).
  - Otherwise, prove it red by mutation after the fact. The task names the mutation, and a mutation is reverted and checked with `git diff` before commit.
- **Byte identity is measured, not assumed (spec §9 item 7).** Task 1 records three coverage baselines at `d363baf`. Tasks 3, 4 and 6 must reproduce them exactly, because every change they make to an existing universe writes the same bytes:
  - Task 3: `authorize` writes nothing, and an admission is not a record;
  - Task 4: the tenant swarm's operator path is `transact`'s own sequence;
  - Task 6: a `global` enqueue is still `0x08` with its old fingerprint.

  A moved baseline is a finding. Stop, and adjudicate it before touching an oracle (memory `pinned-seed-names-a-universe`).
- **Sim failures are findings.** A property failure is a finding: stop, pin the seed, and adjudicate before weakening anything. A timeout is not a catch, and a green replay of one seed proves nothing about a rate or a floor.
- **Mutation runs.**
  - Commit before any mutation run.
  - Never share `CARGO_TARGET_DIR` across worktrees.
  - `charpente test --mutants` judges mutants with the core+sim suites unless `--test-package <crate>` is given, whatever `--file` says. `--test-package` is repeatable and replaces the default set.
  - Validate each `--re` against `cargo mutants --list` for its function first.
  - `exclude_re` is a substring match. No `.cargo/mutants.toml` entry may be added (spec §9 item 5).
- **Confidentiality.** This repo is intended to become public. Examples stay generic (`Org`, `Shop`, `Desk`, invoices, incidents).
- **The statuspage checkout is read-only.** `../statuspage-run/statuspage` is at `6037704` with a `[patch.crates-io]` pointing at this checkout. Task 9 builds a copy of it, outside this repo; nothing edits or commits there.

## Review Focus

These are the inputs the spec implies but does not spell out that are most likely to bite someone using this. Each one's test is added to the task that owns the code.

1. **A deploy that changes a job from `global` to `scoped_by` over Pending rows.** This is the first thing every existing app does after upgrading. The Pending rows were enqueued with no tenant. The new binary must refuse to start over them, by name, with the tenancy half of the fix clause. It must never dispatch one into no tenant, and never trip the dispatcher's scope assert. Owner: Task 6, `a_job_that_gains_scoped_by_is_refused_at_startup_over_its_pending_rows`.
2. **Tenant 0 is a real tenant for a job, too.** `scope: Some(0)` must survive the record (presence, not value), the row and the dispatcher's `JobScope`, and land its writes under tenant 0, not "no tenant". Owners:
   - Task 5, `scoped_enqueue_bytes_are_pinned_with_tenant_zero` and `a_shipped_scoped_enqueue_materializes_with_its_scope_tenant_zero_included`;
   - Task 6, `a_scoped_job_runs_in_its_enqueuers_tenant_tenant_zero_included`.
3. **A scoped job's retries keep its tenant.** The dispatcher's failure record re-puts the row it read. If that path dropped `scope`, the next attempt would trip the dispatch assert and stop the node. Owner: Task 6, `a_failing_scoped_job_keeps_its_tenant_through_every_retry_to_dead`.
4. **A follower applying a shipped `0x0a` record materializes the same `JobRow.scope`.** Replication, restore and replay all apply through that one path. Owner: Task 5, `a_shipped_scoped_enqueue_materializes_with_its_scope_tenant_zero_included`, through `Db::apply_shipped`.
5. **On `public(Org)`, the declaration beats the membership check.** A credentialed caller whose `authorize` would refuse is still admitted, with zero calls, and the same caller on the route's `auth` twin is refused by the app's own error. Owner: Task 3, test (c) `public_scope_admits_a_credentialed_caller_its_membership_check_would_refuse`.

## File map

| File | Responsibility | Tasks |
|---|---|---|
| `crates/charpente-core/src/db.rs` | `TenantAdmission`, `Group::tenant_admission`, `enter_scope`/`open_scope`/`admit_scope`, `Tenant::authorize(&Who)`, `Job::Scope`, tag `0x0a`, `Effect::Enqueue.scope`, `push_enqueue`, `enqueue`/`enqueue_keyed`/`enqueue_in`, `ScopedTx::enqueue*`, the refusal's tenancy clause | 2, 4, 5, 6 |
| `crates/charpente-core/src/{job,table}.rs` | `JobRow.scope` and its codec; `materialize_job(…, scope)`; the sealed `NoJobTenant`/`UnscopedJob`; `NoTenant`'s job note | 5, 6 |
| `crates/charpente-macros/src/{job,route,fingerprint,lib}.rs` | the job parser (`global`, `scoped_by`, any order); `public(Scope)`; `compute_job`; the refusal texts | 1, 3, 6 |
| `crates/charpente/src/http/{app,router,mod}.rs` | `PublicScope`, `Route.public_scope`, `Stance`, `Ctx`'s stance, `ApiError::AnonymousScope`, the probe, `App::schedule`'s bound | 3, 6 |
| `crates/charpente/src/tenant.rs` | `Scoped::scope`'s three rules; `TenantScope`'s note; the module doc | 3, 4, 6 |
| `crates/charpente/src/auth/ladder.rs` | `Principal`'s private fields and accessors | 4 |
| `crates/charpente/src/kernel/{dispatch,mod}.rs` | `JobDef.scoped_by`, `JobGlue`'s fourth argument, `JobScope`, the dispatch assert and binding | 6 |
| `crates/charpente/src/repl/mod.rs` | `PROTO_VERSION` 4 | 5 |
| `crates/charpente/src/describe.rs`, `crates/charpente-cli/src/genclient.rs` | `RouteInfo.public_scope`, `JobInfo.scoped_by`, the human dump | 7 |
| `crates/charpente/src/{serve,launch}.rs`, `crates/charpente/src/repl/{mod,probe}.rs`, `crates/charpente/src/auth/sweep.rs` | hand-written `Job` impls and `JobDef`s; `SessionSweep` becomes `global` | 1, 6 |
| tests: `crates/charpente/tests/{tenant,kernel,realtime,describe,archive,replication,typed_tables,transactions,http,auth,group_commit}.rs`, `crates/charpente-core/tests/{engine,materialize}.rs`, `crates/charpente-macros/tests/compile_{fail,pass}/`, `crates/charpente-tsgen/tests/`, `crates/guide-examples/tests/doc_truth.rs`, `examples/invoicing/tests/describe_golden.rs`, `crates/charpente-cli/tests/describe.rs` | oracles, migrations, goldens | all |
| `examples/invoicing/{src/jobs.rs,src/bin/tenant_node.rs,examples/seed_foreign_job.rs}`, `crates/{sim-fixture,bench-harness}/src/lib.rs`, `crates/charpente-cli/templates/lib.rs` | `global` sweep; `&Principal` | 1, 4, 6 |
| `docs/GUIDE.md`, `DESIGN.md`, `DECISIONS.md` §12, `README.md`, earlier specs, `PLAN.md`, `docs/LEDGER.md` | spec §10 | all |

No new test files, so CLAUDE.md's exhaustive test list and `doc_inventory` stay as they are. New trybuild fixtures are globbed by `tests/trybuild.rs`.

---

### Task 1: `#[job(global)]` is accepted and swept onto every job; baselines recorded

**Files:**
- Modify `crates/charpente-macros/src/job.rs`: the module doc (lines 1–8) and `impl Parse for JobArgs` (lines 69–137). Add `enum JobArg`.
- Modify every `#[job]` attribute site, 28 in Rust:
  - `crates/sim-fixture/src/lib.rs:33`
  - `crates/charpente-cli/templates/lib.rs:34`
  - `crates/bench-harness/src/lib.rs:50`
  - `crates/charpente/src/auth/sweep.rs:30`
  - `crates/charpente/src/launch.rs:857`
  - `crates/charpente/tests/describe.rs:132,1149`
  - `crates/charpente/tests/group_commit.rs:791,889,1430,2017`
  - `crates/charpente/tests/kernel.rs:28,82,91,106,124,322,1352,3006`
  - `crates/charpente/tests/archive.rs:5458`
  - `crates/charpente/tests/replication.rs:4898`
  - `examples/invoicing/src/jobs.rs:19,82,107`
  - `crates/charpente-macros/tests/compile_fail/job_bad_retry.rs:10`
  - `crates/charpente-macros/tests/compile_pass/path_qualified_registration.rs:21`
  - `crates/charpente-macros/tests/compile_pass/job_cross_tenant_capability.rs:36,52`
- Modify the eleven compiled `#[job]` blocks in `docs/GUIDE.md`. Find them by struct:
  - §6: `Basic`, `Custom`, `CountUp` (handler), `CountUp` (`mount_jobs`), `SendReminder`;
  - §7: `SendDigest`, `Tick`, `AdvanceDay`, `SendDigest` ("Putting it together");
  - §8: `CountUp`;
  - §16: `Rollup`.

  Also the two `rust,ignore` rejection examples in §6's "What `#[job]` rejects".
- Create `crates/charpente-macros/tests/compile_pass/job_global_forms.rs`.
- Create `crates/charpente-macros/tests/compile_fail/job_duplicate_global.rs`, `job_duplicate_retry.rs` and their `.stderr` files.
- Modify `crates/charpente-macros/tests/compile_fail/job_bad_retry.stderr` (its column moves).
- Test: `crates/charpente/tests/kernel.rs` (the fingerprint pin).
- Docs:
  - `docs/GUIDE.md` §6 ("`#[job]` and its retry policy"), §9 (the `#[job]` catalog);
  - `DESIGN.md` §3's **Jobs** bullet (line ~596);
  - the spec, which gets its amendments.

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `#[job(...)]` accepts the words `global`, `cross_tenant` and `retry(...)` in any order, each at most once, comma-separated, with an optional trailing comma. `global` expands to nothing. A bare `#[job]` still compiles; Task 6 refuses it (amendment A1).
  - New refusals, fixed text:
    - "`global` given twice — write it once"
    - "`retry(...)` given twice — write it once"
    - "`cross_tenant` given twice — write it once" (unchanged)
  - Every `#[job]` in the tree says `global`.
  - `.superpowers/sdd/2026-09-27-job-tenancy/{tenant,kernel,realtime}-coverage-base.txt`, which Tasks 3, 4 and 6 diff against.

- [ ] **Step 1: Record the baselines at `d363baf`, before any change**

Confirm `git rev-parse --short HEAD` prints `d363baf` and `git status --short` is empty. Then capture three swarms' coverage. These are the three existing universes this plan's changes touch.

```zsh
D=.superpowers/sdd/2026-09-27-job-tenancy
mkdir -p $D
cargo test -p charpente --test tenant -- every_scoped_read_returns_only_its_own_tenants_rows --exact --nocapture 2>&1 | tee $D/tenant-swarm-base.log | grep "^test result"
CHARPENTE_BASE_SEED=0x3E7A0927 cargo test -p charpente --test kernel -- kernel_jobs_are_exactly_once_under_the_fault_matrix --exact --nocapture 2>&1 | tee $D/kernel-swarm-base.log | grep "^test result"
cargo test -p charpente --test realtime -- a_reconnecting_tenant_subscriber_misses_nothing_and_leaks_nothing --exact --nocapture 2>&1 | tee $D/realtime-swarm-base.log | grep "^test result"
for f in tenant kernel realtime; do
  grep -E "^charpente-sim|^  [^ ]" $D/$f-swarm-base.log > $D/$f-coverage-base.txt
  wc -l $D/$f-coverage-base.txt
done
```

Expected:
- each run prints `test result: ok. 1 passed`;
- each coverage file holds the "N universes from base seed" header, the "coverage over N universes:" header and its key lines.

An empty file means the grep missed the runner's format: read the log and fix the pattern before going on. Every later comparison is `diff` against these files, and **a non-empty diff is a finding**.

Then add the fingerprint pin to `crates/charpente/tests/kernel.rs`, directly after `a_jobs_fingerprint_follows_its_fields_not_its_name`:

```rust
/// Job-tenancy spec §6 and §9 item 7: a `global` job's fingerprint is exactly
/// the fold a bare `#[job]` computed, so no Pending job of an existing type
/// changes shape when its declaration gains `global`. `SessionSweep` is the
/// platform's own job, whose shape a release must never move (its doc comment
/// says why). The literals are FNV-1a over charpente's own rendering —
/// `"before\0u64\0"` and `"slot\0u64\0"` — and were run green at `d363baf`,
/// before any job said `global`.
#[test]
fn a_global_jobs_fingerprint_is_the_bare_jobs_fingerprint() {
    assert_eq!(
        <charpente::auth::SessionSweep as Job>::FINGERPRINT,
        0x7e72_9c20_6b79_cbad
    );
    assert_eq!(<CountUp as Job>::FINGERPRINT, 0x766b_8ba4_18db_4d08);
}
```

Run it at `d363baf` (nothing else changed yet):

```zsh
cargo test -p charpente --test kernel -- a_global_jobs_fingerprint_is_the_bare_jobs_fingerprint --exact
```

Expected: PASS. It is a characterization pin: it must hold before and after. If it fails here, the computed literal is wrong (amendment A9). Stop and report the printed values; do not edit them in.

- [ ] **Step 2: Write the failing macro fixtures**

`crates/charpente-macros/tests/compile_pass/job_global_forms.rs`:

```rust
// `#[job]`'s words — `global`, `cross_tenant` and `retry(...)` — in any
// order, each at most once (job-tenancy spec §5), the precedent `#[table]`'s
// parser set. `global` expands to nothing: a `global` job is what a bare
// `#[job]` was, so its fingerprint and its `run` signature are unchanged.
use charpente::kernel::JobError;
use charpente::{CrossTenant, Tx, job};

#[job(global)]
struct Plain {
    n: u64,
}

impl Plain {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        let _ = self.n;
        Ok(())
    }
}

#[job(retry(max = 2, backoff = "5s"), global)]
struct RetryFirst {}

impl RetryFirst {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

#[job(cross_tenant, global,)]
struct CapabilityFirst {}

impl CapabilityFirst {
    fn run(self, _tx: &mut Tx, cap: &CrossTenant) -> Result<(), JobError> {
        let _ = cap;
        Ok(())
    }
}

fn main() {}
```

`crates/charpente-macros/tests/compile_fail/job_duplicate_global.rs`:

```rust
// A job word is a declaration, so a repeat is always a typo or a bad merge —
// refused by name rather than last-write-wins, the call `#[table]` and
// `#[route]` already make.
use charpente::job;

#[job(global, global)]
struct Twice {
    n: u64,
}

fn main() {}
```

`crates/charpente-macros/tests/compile_fail/job_duplicate_retry.rs`:

```rust
// Two `retry(...)` clauses would make one of them silently win; the job
// declares one policy.
use charpente::job;

#[job(global, retry(max = 1, backoff = "1s"), retry(max = 2, backoff = "2s"))]
struct Twice {
    n: u64,
}

fn main() {}
```

In `crates/charpente-macros/tests/compile_fail/job_bad_retry.rs`, change `#[job(retry(count = 3))]` to `#[job(global, retry(count = 3))]`. After Task 6 a bare `#[job]` is refused before its retry clause is read, so the fixture must already say `global` to keep pinning the retry-field refusal.

- [ ] **Step 3: Run the fixtures and record why each is red**

```zsh
cargo test -p charpente-macros --test trybuild 2>&1 | tee .superpowers/sdd/2026-09-27-job-tenancy/task1-red.log | grep -E "^test |mismatch|error:" | head -40
```

Expected red, quoted in the report:
- `job_global_forms.rs` fails to compile with ACCEPTED_FORMS, because `global` is not a word today;
- the two new compile-fail fixtures have no `.stderr` yet (trybuild prints the actual error);
- `job_bad_retry` reports the ACCEPTED_FORMS error at `global` instead of the pinned retry-field text.

- [ ] **Step 4: Implement the any-order parser**

In `crates/charpente-macros/src/job.rs`, replace the module doc's first line with:

```rust
//! `#[job(global[, cross_tenant][, retry(max = N, backoff = "30s")])]`, the
//! words in any order, each at most once (DESIGN.md §6, specs
//! 2026-07-02-queue-cron-design.md and 2026-09-27-job-tenancy-design.md §5):
```

Keep the rest of the doc. Leave `ACCEPTED_FORMS` and `JobArgs` as they are: Task 6 changes both, together with the words they describe (amendment A1).

Replace `impl Parse for JobArgs` (the whole impl, lines 69–137) with:

```rust
/// One word of the attribute, parsed one at a time so each can come at most
/// once and in any order — `#[table]`'s `TableArg` is the precedent, for the
/// same reason: a declaration repeated is a typo or a bad merge, never an
/// override.
enum JobArg {
    Global(proc_macro2::Span),
    CrossTenant(proc_macro2::Span),
    Retry(proc_macro2::Span, Vec<RetryField>),
}

impl Parse for JobArg {
    fn parse(input: ParseStream) -> syn::Result<JobArg> {
        let ident: Ident = input
            .parse()
            .map_err(|e| Error::new(e.span(), ACCEPTED_FORMS))?;
        match ident.to_string().as_str() {
            "global" => Ok(JobArg::Global(ident.span())),
            "cross_tenant" => Ok(JobArg::CrossTenant(ident.span())),
            "retry" => {
                // Peeked, so `retry` without its parentheses gets the house
                // message rather than syn's "expected parentheses".
                if !input.peek(syn::token::Paren) {
                    return Err(Error::new(ident.span(), ACCEPTED_FORMS));
                }
                let content;
                parenthesized!(content in input);
                let fields = content.parse_terminated(RetryField::parse, Token![,])?;
                Ok(JobArg::Retry(ident.span(), fields.into_iter().collect()))
            }
            _ => Err(Error::new(ident.span(), ACCEPTED_FORMS)),
        }
    }
}

impl Parse for JobArgs {
    fn parse(input: ParseStream) -> syn::Result<JobArgs> {
        // `global` changes nothing in expansion yet: it is read only to refuse
        // a repeat. Task 6 of the job-tenancy plan makes it one of the two
        // required answers.
        let mut global: Option<proc_macro2::Span> = None;
        let mut cross_tenant: Option<proc_macro2::Span> = None;
        let mut retry: Option<Vec<RetryField>> = None;
        while !input.is_empty() {
            match input.parse::<JobArg>()? {
                JobArg::Global(span) => {
                    if global.is_some() {
                        return Err(Error::new(span, "`global` given twice — write it once"));
                    }
                    global = Some(span);
                }
                JobArg::CrossTenant(span) => {
                    if cross_tenant.is_some() {
                        return Err(Error::new(
                            span,
                            "`cross_tenant` given twice — write it once",
                        ));
                    }
                    cross_tenant = Some(span);
                }
                JobArg::Retry(span, fields) => {
                    if retry.is_some() {
                        return Err(Error::new(span, "`retry(...)` given twice — write it once"));
                    }
                    retry = Some(fields);
                }
            }
            if input.is_empty() {
                break;
            }
            input
                .parse::<Token![,]>()
                .map_err(|e| Error::new(e.span(), ACCEPTED_FORMS))?;
        }

        let mut max = None;
        let mut backoff_millis = None;
        for field in retry.unwrap_or_default() {
            match field {
                RetryField::Max(lit) => {
                    if max.is_some() {
                        return Err(Error::new(lit.span(), "`max` given twice — write it once"));
                    }
                    max = Some(lit.base10_parse::<u32>()?);
                }
                RetryField::Backoff(lit) => {
                    if backoff_millis.is_some() {
                        return Err(Error::new(
                            lit.span(),
                            "`backoff` given twice — write it once",
                        ));
                    }
                    backoff_millis = Some(parse_backoff(&lit)?);
                }
            }
        }
        let defaults = JobArgs::default();
        Ok(JobArgs {
            max: max.unwrap_or(defaults.max),
            backoff_millis: backoff_millis.unwrap_or(defaults.backoff_millis),
            cross_tenant: cross_tenant.is_some(),
        })
    }
}
```

`global` is a local `Option<Span>` rather than a `JobArgs` field on purpose. A field nothing reads is a `dead_code` warning, which `-D warnings` turns into a red gate.

- [ ] **Step 5: Sweep `global` onto every job**

At every site in this task's file list:
- `#[job]` becomes `#[job(global)]`;
- `#[job(<words>)]` becomes `#[job(global, <words>)]`.

That covers the 28 Rust sites and the eleven compiled GUIDE blocks. In GUIDE §6's "What `#[job]` rejects" (`rust,ignore`):
- `#[job(retry(max = 3, backoff = "10"))]` becomes `#[job(global, retry(max = 3, backoff = "10"))]`;
- the `#[job]` on `enum Baz` becomes `#[job(global)]`;
- `#[job(bogus)]` stays: it is the unknown-word example.

`crates/charpente-cli/templates/lib.rs` is what `charpente new` writes. Its test (`crates/charpente-cli/tests/new.rs`) compiles the scaffold, so the sweep is checked there.

Prove the sweep complete:

```zsh
grep -rnE '^\s*#\[job(\]\s*$|\((retry|cross_tenant))' --include='*.rs' crates examples
grep -nE '^#\[job(\]\s*$|\((retry|cross_tenant))' docs/GUIDE.md
```

Expected: no output from either. The first pattern excludes `crates/charpente-macros/src` by construction: those are doc comments, not attributes.

- [ ] **Step 6: Run the task's tests, and pin the new stderr**

```zsh
TRYBUILD=overwrite cargo test -p charpente-macros --test trybuild
git diff --stat crates/charpente-macros/tests/compile_fail/
cargo test -p charpente-macros --test trybuild
cargo test -p charpente --test kernel -- a_global_jobs_fingerprint_is_the_bare_jobs_fingerprint a_jobs_fingerprint_follows_its_fields_not_its_name job_macro_derives_codec_name_retry_and_glue
cargo build -p guide-examples
cargo test -p charpente-cli --test new
```

Review each generated or changed `.stderr` line by line against the macro source before keeping it. Expected:
- `job_duplicate_global.stderr`: "`global` given twice — write it once", pointing at the second `global`;
- `job_duplicate_retry.stderr`: "`retry(...)` given twice — write it once", pointing at the second `retry`;
- `job_bad_retry.stderr`: the same unknown-retry-field text as before, only the column moved (13 → 21). A changed *message* is a bug in Step 4.

No other `.stderr` may change. Then everything passes.

- [ ] **Step 7: Docs**

`docs/GUIDE.md` §6, "`#[job]` and its retry policy". Replace the sentence beginning "Bare `#[job]` takes `RetryPolicy::DEFAULT`" through "one of `ms`, `s`, `m`, `h`." with:

> A job says which tenancy it has. Every job in this guide until §16 writes `#[job(global)]`: it belongs to no tenant. `#[job(global)]` takes `RetryPolicy::DEFAULT` (5 attempts, 30s base backoff), and `#[job(global, retry(max = <int>, backoff = "<n><unit>"))]` overrides either or both. `backoff` is a bare number plus a unit, one of `ms`, `s`, `m`, `h`. The words `global`, `cross_tenant` (§16) and `retry(...)` may come in any order, each at most once.

`docs/GUIDE.md` §9, the `#[job]` catalog's text block: add these two lines after "`backoff` given twice — write it once":

```text
`global` given twice — write it once
`retry(...)` given twice — write it once
```

In the prose after the block, change "`max`/`backoff` written twice" to "`global`, `retry(...)`, `max` or `backoff` written twice".

`DESIGN.md` §3, the **Jobs** bullet: `` `#[job]` structs `` becomes `` `#[job(global)]` structs ``. Task 6 rewrites the bullet.

Append this plan's "Spec amendments" section to `docs/superpowers/specs/2026-09-27-job-tenancy-design.md`, verbatim, under a new `## Amendments (implementation plan)` heading. Also change its **Status** line to:

> approach D approved in chat (Noah, 2026-09-27); the written spec, §14 included, approved 2026-09-27; spec of record for the plan `docs/superpowers/plans/2026-09-27-job-tenancy.md`.

- [ ] **Step 8: Gate and commit**

Run the three-command gate (Global Constraints) and count it: 87 of 87 `test result: ok`, 0 FAILED. Then:

```zsh
git status --short
git add crates/charpente-macros crates/sim-fixture/src/lib.rs crates/charpente-cli/templates/lib.rs \
  crates/bench-harness/src/lib.rs crates/charpente/src/auth/sweep.rs crates/charpente/src/launch.rs \
  crates/charpente/tests/describe.rs crates/charpente/tests/group_commit.rs crates/charpente/tests/kernel.rs \
  crates/charpente/tests/archive.rs crates/charpente/tests/replication.rs examples/invoicing/src/jobs.rs \
  docs/GUIDE.md DESIGN.md docs/superpowers/specs/2026-09-27-job-tenancy-design.md
git commit -m "feat(job-tenancy): #[job(global)] accepted in any order and swept onto every job; SessionSweep's fingerprint pinned"
```

---

### Task 2: The operator's admission — `TenantAdmission` and `Tx::admit_scope`

**Files:**
- Modify `crates/charpente-core/src/db.rs`:
  - `Tx::open_scope` (~1288–1342): its body moves into a private `enter_scope`;
  - add `Tx::admit_scope`;
  - add `TenantAdmission` and `Group::tenant_admission` after `CrossTenant`'s `impl Group` block (~1486–1497).
- Modify `crates/charpente/src/lib.rs`: re-export `TenantAdmission`.
- Test: `crates/charpente-core/tests/engine.rs`, after `reopening_the_same_scope_is_allowed`.
- Create `crates/charpente-macros/tests/compile_fail/tenant_admission_cannot_be_forged.rs` and its `.stderr`.
- Docs:
  - `docs/GUIDE.md` §16, "Where the guarantee stops";
  - `DESIGN.md` §8's **Multi-tenancy** bullet (~1587–1627).

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces (spec §3, §7):
  - `pub struct TenantAdmission(());` — `#[derive(Clone)]`, private field. Re-exported as `charpente::TenantAdmission`.
  - `impl Group<'_> { pub fn tenant_admission(&self) -> TenantAdmission }` — the only constructor.
  - `impl<'a> Tx<'a> { pub fn admit_scope<S: Tenant>(&mut self, scope: Id<S>, admission: &TenantAdmission) -> ScopedTx<'_, 'a, S> }` — runs no `authorize`. The one-scope contract is `open_scope`'s own, by construction.
  - `open_scope`'s signature is unchanged here (`who: Option<&S::Who>`); Task 4 changes it.

- [ ] **Step 1: Write the core tests**

In `crates/charpente-core/tests/engine.rs`, after `reopening_the_same_scope_is_allowed`:

```rust
// ----------------------------------------- job-tenancy Task 2: the admission

/// Entries into `AdmitOrg::authorize`. Its own counter rather than
/// `AUDITED_SCOPES`: `a_refused_second_scope_never_reaches_the_apps_authorize`
/// clears and asserts that one, and tests run on parallel threads.
static ADMIT_ORG_AUTHORIZE_CALLS: AtomicU64 = AtomicU64::new(0);

/// A scope type whose membership check refuses everyone it is asked about, so
/// an admission that asked it would fail twice over — the call would be
/// counted, and the scope refused.
struct AdmitOrg;

impl Tenant for AdmitOrg {
    type Who = str;
    type Error = io::Error;

    fn authorize(_tx: &Tx, _who: Option<&str>, _scope: Id<AdmitOrg>) -> Result<(), io::Error> {
        ADMIT_ORG_AUTHORIZE_CALLS.fetch_add(1, Ordering::Relaxed);
        Err(io::Error::other("AdmitOrg refuses everyone it is asked about"))
    }
}

/// Job-tenancy spec §7: the operator opens a tenant with a `TenantAdmission`
/// minted from a `Group`. No `authorize` runs — the operator has no principal
/// to ask about and already holds the store — and the handle it gets is
/// bounded to that tenant exactly as an authorized one is.
#[test]
fn an_admitted_scope_asks_no_authorize_and_writes_inside_its_tenant() {
    let sim = Sim::with_faults(0x7F00_0005, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Ticket>(),
    )
    .unwrap();

    let mut group = db.begin_group().unwrap();
    let admission = group.tenant_admission();
    group
        .stage(|tx| {
            let _ = tx.admit_scope(Id::<AdmitOrg>::from_raw(9), &admission);
            Ok::<(), io::Error>(())
        })
        .unwrap();
    group
        .stage(|tx| {
            tx.admit_scope(Id::<TestOrg>::from_raw(3), &admission)
                .put(&Ticket {
                    id: Id::from_raw(7),
                    n: 11,
                });
            Ok::<(), io::Error>(())
        })
        .unwrap();
    group.commit().unwrap();

    assert_eq!(
        ADMIT_ORG_AUTHORIZE_CALLS.load(Ordering::Relaxed),
        0,
        "an admission is not a question: AdmitOrg::authorize refuses everyone, and it was \
         never asked"
    );
    let snap = db.snapshot().unwrap();
    assert_eq!(
        snap.get_scoped::<Ticket>(3, Id::from_raw(7))
            .unwrap()
            .map(|t| t.n),
        Some(11),
        "the admitted write landed in tenant 3"
    );
    assert!(
        snap.get_scoped::<Ticket>(0, Id::from_raw(7))
            .unwrap()
            .is_none(),
        "and nowhere else — tenant 0 is a tenant, not a default"
    );
}

/// The one-scope contract (spec §8 of the tenant-scoping spec) is the same
/// contract whichever way the scope was entered, because `open_scope` and
/// `admit_scope` share one body (job-tenancy spec §3).
#[test]
#[should_panic(expected = "a transaction opens ONE tenant scope")]
fn an_admitted_second_tenant_is_the_same_one_scope_violation() {
    let sim = Sim::with_faults(0x7F00_0006, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Ticket>(),
    )
    .unwrap();
    let mut group = db.begin_group().unwrap();
    let admission = group.tenant_admission();
    let _ = group.stage(|tx| {
        {
            let _first = tx.admit_scope(Id::<TestOrg>::from_raw(1), &admission);
        }
        let _second = tx.admit_scope(Id::<TestOrg>::from_raw(2), &admission);
        Ok::<(), io::Error>(())
    });
}

/// And across the two constructors: a tenant opened by membership, then a
/// different one admitted, is the same violation — neither path is a way
/// around the other's contract.
#[test]
#[should_panic(expected = "a transaction opens ONE tenant scope")]
fn an_opened_then_admitted_second_tenant_is_the_same_violation() {
    let sim = Sim::with_faults(0x7F00_0007, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Ticket>(),
    )
    .unwrap();
    let mut group = db.begin_group().unwrap();
    let admission = group.tenant_admission();
    let _ = group.stage(|tx| {
        {
            let _first = tx.open_scope(Id::<TestOrg>::from_raw(1), None)?;
        }
        let _second = tx.admit_scope(Id::<TestOrg>::from_raw(2), &admission);
        Ok::<(), io::Error>(())
    });
}
```

Create `crates/charpente-macros/tests/compile_fail/tenant_admission_cannot_be_forged.rs`:

```rust
// The seal on `TenantAdmission` (job-tenancy spec §3, §7): its field is
// private and `Group::tenant_admission` is its only constructor, and a
// handler holds `&mut Tx` and `&Ctx`, never a `Group`. So a handler that
// forges one to enter a tenant without its membership check does not
// compile — the seal `cross_tenant_token_cannot_be_forged.rs` pins for
// `CrossTenant`, on the admission.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::wire::WireU64;
use charpente::{Id, Tenant, TenantAdmission, Tx, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(scoped_by = Org)]
struct Invoice {
    id: Id<Invoice>,
    total: u32,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: Option<&Principal>, _scope: Id<Org>) -> Result<(), ApiError> {
        Err(ApiError::Unauthorized)
    }
}

#[route(GET "/orgs/:org/invoices", public)]
fn invoices(tx: &mut Tx, org: Id<Org>) -> Result<WireU64, ApiError> {
    let tx = tx.admit_scope::<Org>(org, &TenantAdmission(()));
    Ok(WireU64(tx.scan::<Invoice>()?.len() as u64))
}

fn main() {}
```

These tests name API that does not exist yet, so they cannot be red by behaviour at base. Step 4 proves each red by mutation.

- [ ] **Step 2: Implement**

In `crates/charpente-core/src/db.rs`, replace `open_scope`'s body with a call into a new private `enter_scope`, and add `admit_scope`. The whole `impl<'a> Tx<'a>` block after `pub trait Tenant` becomes:

```rust
impl<'a> Tx<'a> {
    /// The one body both scope constructors share (job-tenancy spec §3). The
    /// one-scope contract is checked FIRST, because it is decidable without
    /// asking anyone: a second tenant on this transaction gets no handle
    /// whatever `admit` would answer, so the app's `authorize` — and any
    /// audit entry or side effect it performs — is never invoked for a scope
    /// no handle is issued for.
    fn enter_scope<S: Tenant, E>(
        &mut self,
        scope: Id<S>,
        admit: impl FnOnce(&Tx<'a>) -> Result<(), E>,
    ) -> Result<ScopedTx<'_, 'a, S>, E> {
        let opening = (TypeId::of::<S>(), std::any::type_name::<S>(), scope.raw());
        if let Some(prev) = self.opened_scope {
            assert!(
                (prev.0, prev.2) == (opening.0, opening.2),
                "contract: a transaction opens ONE tenant scope (spec §8) — this one already \
                 opened {}({}) and is now asked for {}({}); run the second tenant's work in its \
                 own transaction",
                prev.1,
                prev.2,
                opening.1,
                opening.2
            );
        }
        admit(self)?;
        self.opened_scope = Some(opening);
        Ok(ScopedTx { tx: self, scope })
    }

    /// Opens a tenant scope on a member's word: `S::authorize` runs, and its
    /// refusal is returned unchanged. With `admit_scope` below, the only
    /// constructors of `ScopedTx` — the only route to the six scope-taking
    /// primitives above, which are private.
    ///
    /// Apps call `tx.scope::<Org>(org, ctx)?` (`charpente::Scoped`), which
    /// maps the request to `who` and delegates here. The name differs
    /// because an inherent method shadows an extension trait's method of the
    /// same name, and the app-facing spelling is the one with the `Ctx`.
    ///
    /// `who` is `None` for a direct `open_scope(scope, None)` call — an
    /// `authorize` that admits `None` has declared anonymous access to that
    /// scope; refuse it unless that is intended. `None` is not a bypass:
    /// `S::authorize` runs either way, and what it does with `None` is the
    /// app's own policy.
    ///
    /// # Panics
    ///
    /// A transaction opens ONE scope (spec §8). A second `open_scope` or
    /// `admit_scope` naming a different tenant — including one reached
    /// through `ScopedTx`'s `DerefMut` — is a contract violation, not a
    /// cross-tenant write. Re-opening the SAME scope is a no-op.
    pub fn open_scope<S: Tenant>(
        &mut self,
        scope: Id<S>,
        who: Option<&S::Who>,
    ) -> Result<ScopedTx<'_, 'a, S>, S::Error> {
        self.enter_scope(scope, |tx| S::authorize(tx, who, scope))
    }

    /// Opens a tenant scope on the operator's word, with no
    /// `Tenant::authorize` (job-tenancy spec §7): the `TenantAdmission` is
    /// the proof that the holder of a `Group` — whoever holds the `Db` —
    /// admitted this transaction, and there is no principal to ask about.
    /// The one-scope contract and the handle are exactly `open_scope`'s: the
    /// two share one body.
    ///
    /// # Panics
    ///
    /// As `open_scope`: a second, different tenant on one transaction.
    pub fn admit_scope<S: Tenant>(
        &mut self,
        scope: Id<S>,
        admission: &TenantAdmission,
    ) -> ScopedTx<'_, 'a, S> {
        let TenantAdmission(()) = admission;
        let Ok(scoped) = self.enter_scope(scope, |_| Ok::<(), std::convert::Infallible>(()));
        scoped
    }
}
```

After `CrossTenant`'s `impl<'a> Group<'a>` block, add:

```rust
// ------------------------------------------------------- admission

/// Proof that the operator admitted this transaction into one tenant —
/// `Tx::admit_scope`'s argument (job-tenancy spec §3, §7).
///
/// Sealed exactly as `CrossTenant` is, and for the same reason: its field is
/// private and `Group::tenant_admission` is the only constructor, and a
/// handler holds `&mut Tx` and `&Ctx`, never a `Group`. Whoever holds the
/// `Db` a group borrows is the operator, who already reaches every tenant
/// through `Db::snapshot` (ruling R48); the admission makes that standing
/// explicit at a scope line instead of leaving it an exception to the
/// guarantee.
///
/// `Clone` for `CrossTenant`'s reason: holding one at all is the admission,
/// so a copy widens nothing.
#[derive(Clone)]
pub struct TenantAdmission(());

impl<'a> Group<'a> {
    /// Mints the admission `Tx::admit_scope` takes. The only constructor;
    /// see `TenantAdmission`.
    pub fn tenant_admission(&self) -> TenantAdmission {
        TenantAdmission(())
    }
}
```

Tasks 3 and 6 each add one holder to this doc comment as their holder lands: the `public(S)` route's `Ctx`, then the dispatcher.

In `crates/charpente/src/lib.rs`, add `TenantAdmission` to the `pub use charpente_core::db::{…}` list, in alphabetical order after `TenantScope`'s neighbours: `…, Seeder, Tenant, TenantAdmission, Tx, TxError,`.

`let Ok(scoped) = …;` compiles without an `else` because `Result<_, Infallible>`'s `Err` arm is uninhabited (stable since Rust 1.82).

- [ ] **Step 3: Run**

```zsh
cargo test -p charpente-core --test engine -- admitted
cargo test -p charpente-core --test engine -- scope
TRYBUILD=overwrite cargo test -p charpente-macros --test trybuild
cat crates/charpente-macros/tests/compile_fail/tenant_admission_cannot_be_forged.stderr
cargo test -p charpente-macros --test trybuild
```

Expected:
- all engine scope tests pass, the old ones included;
- the new `.stderr` is `error[E0423]: cannot initialize a tuple struct which contains private fields` at `TenantAdmission(())`, with the note pointing at `pub struct TenantAdmission(());` in `db.rs` — the same shape as `cross_tenant_token_cannot_be_forged.stderr`;
- no other `.stderr` changed (`git diff --stat crates/charpente-macros/tests/compile_fail/`).

- [ ] **Step 4: Prove each test red by mutation**

Save the implementation first, so each mutation reverts exactly (`git checkout -p` is interactive and unavailable here):

```zsh
D=.superpowers/sdd/2026-09-27-job-tenancy
git diff crates/charpente-core/src/db.rs > $D/task2-db.patch
```

Apply each mutation alone, run the named test, and quote the failure. Then restore:

```zsh
git checkout crates/charpente-core/src/db.rs && git apply $D/task2-db.patch
git diff --stat crates/charpente-core/src/db.rs
```

The diff must be exactly Step 2's again.

1. In `admit_scope`, pass `Id::from_raw(0)` instead of `scope` to `enter_scope`. `an_admitted_scope_asks_no_authorize_and_writes_inside_its_tenant` must fail at "the admitted write landed in tenant 3".
2. In `enter_scope`, delete the `if let Some(prev) … { assert!(…) }` block. Both `should_panic` tests must fail ("test did not panic as expected"), and so must the pre-existing `a_second_scope_on_one_transaction_is_a_contract_violation`.

Both assertions of the first test have their own red: the count cannot be mutated independently, because `admit_scope` holds no `Who` to pass. That "cannot" is the property.

- [ ] **Step 5: Docs**

`docs/GUIDE.md` §16, "Where the guarantee stops". Replace the paragraph that begins "**So: code outside a handler that wants the guarantee should go through a transaction and `tx.scope::<Org>(org, ctx)?` exactly as a handler does.**", through "that is the one shape that looks tenant-aware and is not.", with:

````markdown
**Operator code that must act inside one tenant** — seeding, a test oracle, an admin
step in `main` — does not take a scope on its own word either. It begins a group, mints
a `TenantAdmission` from it, and opens the tenant with `admit_scope`:

```rust
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::{Db, Id, Tenant, Tx, TxError, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: Option<&Principal>, _scope: Id<Org>) -> Result<(), ApiError> {
        // Refuses everyone: the seed below never asks.
        Err(ApiError::Unauthorized)
    }
}

#[table(scoped_by = Org)]
struct Memo {
    id: Id<Memo>,
    text: String,
}

/// Seeds one tenant's first memo. `db.begin_group()` is the operator's
/// handle, and the admission it mints is what `admit_scope` asks for.
fn seed(db: &mut Db, org: Id<Org>) -> std::io::Result<()> {
    let mut group = db.begin_group()?;
    let admission = group.tenant_admission();
    let staged = group.stage(|tx| {
        tx.admit_scope::<Org>(org, &admission).put(&Memo {
            id: Id::from_raw(1),
            text: "welcome".to_string(),
        });
        Ok::<(), std::io::Error>(())
    });
    // `commit` on every path: a group dropped with staged writes panics.
    group.commit().map_err(|failure| failure.error)?;
    match staged {
        Ok(()) => Ok(()),
        Err(TxError::App(e)) | Err(TxError::Io(e)) => Err(e),
        Err(other) => Err(std::io::Error::other(format!("{other:?}"))),
    }
}
```

No `authorize` runs there. The operator has no principal to ask about, and already
holds the store. The admission carries the seal `CrossTenant` carries: its field is
private and `Group::tenant_admission` is its only constructor. So a handler, which holds
a `Tx` and a `Ctx` and never a `Group`, cannot mint one; `TenantAdmission(())` is
`error[E0423]`. The one-scope rule is the same contract whichever way a scope was
entered. Do not build an app-internal helper that takes a raw scope and reads through
`Db::snapshot`: that is the one shape that looks tenant-aware and is not.
````

`DESIGN.md` §8's **Multi-tenancy** bullet: after the sentence about `#[route(…, cross_tenant)]` / `#[job(cross_tenant)]` minting the `CrossTenant` token, add:

> The operator acts inside one tenant with a `TenantAdmission` minted from a `Group` (`Tx::admit_scope`), sealed the way `CrossTenant` is. No `authorize` runs, because the operator has no principal and already holds the store (DECISIONS §12).

- [ ] **Step 6: Gate and commit**

Run the gate and count it (87 of 87). Then:

```zsh
git status --short
git add crates/charpente-core/src/db.rs crates/charpente/src/lib.rs crates/charpente-core/tests/engine.rs \
  crates/charpente-macros/tests/compile_fail/tenant_admission_cannot_be_forged.rs \
  crates/charpente-macros/tests/compile_fail/tenant_admission_cannot_be_forged.stderr \
  docs/GUIDE.md DESIGN.md
git commit -m "feat(job-tenancy): TenantAdmission and Tx::admit_scope — the operator enters a tenant on a Group-minted, sealed admission"
```

---

### Task 3: Route stances — `public(Org)`, the three rules, and the named 401

**Files:**
- Modify `crates/charpente/src/http/router.rs`: add `PublicScope`, and add `Route.public_scope` after `auth`. The test helper `route()` (~263) gets `public_scope: None`.
- Modify `crates/charpente/src/http/mod.rs`:
  - re-export `PublicScope`;
  - add `ApiError::AnonymousScope(String)`, answered 401.
- Modify `crates/charpente/src/http/app.rs`:
  - `Declared` and `Stance` (new, private to the module);
  - `Ctx.stance`, `Ctx::public_admission`, `Ctx::anonymous_scope`;
  - `build_ctx` takes the `Stance`;
  - `stage_or_subscribe` (~706) builds the `Stance`;
  - `subscription_event_names` (~535) builds the `Stance`.
- Modify `crates/charpente/src/tenant.rs`: `Scoped::scope`'s three rules; `TenantScope`'s `who` paragraph.
- Modify `crates/charpente-core/src/db.rs`: `TenantAdmission`'s doc names its second holder.
- Modify `crates/charpente-macros/src/route.rs`:
  - the module doc;
  - `RouteArgs.public_scope`;
  - `public(...)` parsing;
  - `FLAGS`, `PUBLIC_SCOPE_ARITY`, the missing-method text, the neither-refusal;
  - `undeclared_capability`'s stance;
  - `public_scope_tokens`;
  - both `Route` literals.
- Modify the hand-built `Route` literals to add `public_scope: None`: `crates/charpente/src/serve.rs` (960, 1037, 1051, 1197, 1284, 1298) and `crates/charpente/tests/realtime.rs:2821`.
- Migrate the public routes that open scopes:
  - `crates/charpente/tests/kernel.rs:1435` `post` → `public(Org)`;
  - `crates/charpente/tests/realtime.rs` `emit_scoped_ping` (4624), `scoped_feed` (4637), `emit_leaky_ping` (4860) → `public(Org)`;
  - `crates/charpente/tests/tenant.rs` `write_note`/`count_notes` → `public(Team)`;
  - `write_memo`/`count_memos` → `auth` (spec §11's table).
- Test: `crates/charpente/tests/tenant.rs` (oracle 2's eight tests, oracle 3).
- Create in `crates/charpente-macros/tests/`:
  - `compile_pass/route_public_scope.rs`;
  - `compile_fail/route_public_scope_arity.rs`, `compile_fail/route_public_scope_missing_tenant_scope.rs`, `compile_fail/route_public_scope_undeclared_capability.rs`, each with its `.stderr`.
- Re-pin `compile_fail/{helpdesk_route_neither_auth_nor_public,route_unknown_flag,route_missing_method}.stderr`.
- Modify `crates/guide-examples/tests/doc_truth.rs`: `PINNED_FLOOR` 21 → 22.
- Docs:
  - `docs/GUIDE.md` §5, §9, §11 and §16;
  - `DESIGN.md` §3's **Routes** bullet and §8's **Multi-tenancy** bullet;
  - `README.md` `:142-143`.

**Interfaces:**
- Consumes (Task 2): `TenantAdmission`, `Group::tenant_admission(&self) -> TenantAdmission`, `Tx::admit_scope<S: Tenant>(&mut self, Id<S>, &TenantAdmission) -> ScopedTx<'_, 'a, S>`.
- Produces:
  - `pub struct PublicScope { pub name: &'static str, pub type_id: fn() -> TypeId }` — `#[derive(Clone, Copy, Debug)]`, re-exported as `charpente::http::PublicScope`.
  - `Route.public_scope: Option<PublicScope>`. Task 7's `describe` reads `.name`.
  - `#[route(METHOD "/path", public(Path))]`, accepted in any flag position. `public(Path)` implies `public`.
  - `ApiError::AnonymousScope(String)` → 401.
  - `Ctx::public_admission<S: 'static>(&self) -> Option<&TenantAdmission>` and `Ctx::anonymous_scope<S>(&self) -> ApiError`, both `pub(crate)`. Task 4's rule 2 is the only other change to `Scoped::scope`.
  - `authorize`'s signature is unchanged here (amendment A7). Rule 2 passes `Some(principal)`.

- [ ] **Step 1: Write the tests that compile at base**

Oracle 2's tests (a), (d), (e) and (g), and oracle 3, can be written against today's API. Each of (a), (g) and oracle 3 then shows the pre-change behaviour as its red. Write them first, and run them at the Task 2 commit before any Step 3 change.

In `crates/charpente/tests/tenant.rs`:

Replace the imports' first line and add the cell types:

```rust
use std::cell::{Cell, RefCell};
```

(`std::sync::atomic` goes: nothing else in the file uses it.) Add to the `charpente::http` import `Cursor` and `Subscription`, and to the `charpente` import `event`.

Replace the two statics and their doc comments (lines 55–63) with:

```rust
thread_local! {
    /// The subject of every call into `Org::authorize`, in order — `"<nobody>"`
    /// for a call with no principal. So a test asserts that the check ran at
    /// all AND whom it was asked about: a status code alone would also pass if
    /// the platform refused for its own reasons and never asked the app.
    ///
    /// Thread-local, not a static (job-tenancy amendment A4): eight tests read
    /// these, `cargo test` runs them on parallel threads of one binary, and a
    /// request is handled on the thread that called `App::handle`.
    static AUTHORIZE_CALLS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };

    /// Handler bodies that got PAST `scope()`. The "no scoped read happened"
    /// half: a refusal that still let the body run would leave this nonzero
    /// even with the response mapped to an error.
    static BODIES_ENTERED: Cell<u64> = const { Cell::new(0) };
}

fn reset_counters() {
    AUTHORIZE_CALLS.with(|calls| calls.borrow_mut().clear());
    BODIES_ENTERED.with(|n| n.set(0));
}

fn authorize_calls() -> Vec<String> {
    AUTHORIZE_CALLS.with(|calls| calls.borrow().clone())
}

fn bodies_entered() -> u64 {
    BODIES_ENTERED.with(Cell::get)
}
```

`Org::authorize`'s first line becomes:

```rust
    fn authorize(_tx: &Tx, who: Option<&Principal>, scope: Id<Org>) -> Result<(), ApiError> {
        AUTHORIZE_CALLS.with(|calls| {
            calls
                .borrow_mut()
                .push(who.map_or_else(|| "<nobody>".to_string(), |p| p.subject.clone()))
        });
```

The rest of its body is unchanged.

Replace `write_memo`, `count_memos` and `memo_app` (lines 88–122) with:

```rust
/// The shape spec §1 prescribes: one `scope()` line, everything below it
/// bounded to that tenant. Returns the tenant's row count BEFORE the write, so
/// a body that ran when it should not have is visible in the response too.
/// Shared by the stance twins below, so they differ in their declaration and
/// nothing else.
fn memo_write(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    BODIES_ENTERED.with(|n| n.set(n.get() + 1));
    let before = tx.scan::<Memo>()?.len() as u64;
    tx.put(&Memo {
        id: Id::from_raw(7),
        text: format!("memo for org {}", org.raw()),
    });
    Ok(WireU64(before))
}

/// Members only: rule 2, with the `auth` gate in front of it.
#[route(POST "/memo/:org", auth)]
fn write_memo(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    memo_write(tx, org, ctx)
}

/// Every caller reaches the handler; the membership check decides the
/// tenant (rule 2), and a caller with no credential gets rule 3's 401.
#[route(POST "/memo-public/:org", public)]
fn write_memo_public(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    memo_write(tx, org, ctx)
}

/// Reads one tenant's rows back, so the refused tenant's emptiness is checked
/// on the store and not only on the counters.
#[route(GET "/memo/:org", auth)]
fn count_memos(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(WireU64(tx.scan::<Memo>()?.len() as u64))
}

#[event]
struct MemoNoted {
    n: WireU64,
}

/// A stream route that opens a scope. Driven through `App::handle`, which has
/// no sink for a subscription, so the only answers it can give a test are its
/// errors — which is all rule 3 needs.
#[route(SSE "/memo-feed/:org", public, emits(MemoNoted))]
fn memo_feed(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(Subscription::of::<MemoNoted>()
        .key(tx.event_key())
        .cursor(cursor))
}

#[route(POST "/memo-login", public)]
fn memo_login(tx: &mut Tx, subject: String, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_session(tx, subject))
}

fn memo_app(seed: u64) -> (Sim, App) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Memo>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![
        memo_login,
        write_memo,
        write_memo_public,
        count_memos,
        memo_feed
    ]);
    (sim, app)
}

/// Logs `subject` in and returns its bearer token. The login is one write
/// record, which the `last_applied` assertions below count.
fn login(app: &mut App, subject: &str) -> String {
    let response = app.handle(Request::new(Method::Post, "/memo-login").json(&subject));
    assert_eq!(response.status, 200, "{}", body_text(&response));
    serde_json::from_slice(&response.body).unwrap()
}

fn as_user(request: Request, token: &str) -> Request {
    request.header("Authorization", format!("Bearer {token}"))
}

/// The `error` field of the `{"error": …}` envelope every non-200 carries.
fn error_text(response: &Response) -> String {
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    body["error"]
        .as_str()
        .expect("a non-200 body is an error envelope")
        .to_string()
}
```

Replace `a_refusing_authorize_is_the_answer_and_nothing_below_the_scope_line_runs` with test (e), today's test re-expressed on `auth` routes:

```rust
/// Oracle 2 (e). A refusing `authorize` is what the client gets, and nothing
/// below the `scope()` line runs — now on `auth` routes, where the caller is
/// always a principal, so the assertion can name WHICH principal was asked.
///
/// Four of its assertions go RED if the `S::authorize(...)?` call disappears
/// from `Tx::open_scope`, the single place it runs — this one from the app's
/// side of the platform boundary, `charpente-core/tests/engine.rs`'s
/// `a_refused_scope_hands_back_the_apps_own_error_and_no_handle` from core's.
/// Without them that deletion is invisible, and the design's whole
/// audit-surface thesis rests on an unpinned line.
#[test]
fn a_refusing_authorize_is_the_answer_and_nothing_below_the_scope_line_runs() {
    let (_sim, mut app) = memo_app(0x7E4A_1701);
    let token = login(&mut app, "alice");
    reset_counters();

    // Positive control first: the admitted tenant proves the route works, so a
    // 400 below is the membership verdict and not a broken handler.
    let admitted = app.handle(as_user(
        Request::new(Method::Post, format!("/memo/{ADMITTED}")),
        &token,
    ));
    assert_eq!(admitted.status, 200, "{}", body_text(&admitted));
    assert_eq!(
        authorize_calls(),
        vec!["alice".to_string()],
        "opening a scope must call the app's own authorize, exactly once, about the caller"
    );
    assert_eq!(bodies_entered(), 1, "an admitted tenant's handler body runs");

    let refused = app.handle(as_user(
        Request::new(Method::Post, format!("/memo/{REFUSED}")),
        &token,
    ));
    assert_eq!(
        refused.status,
        400,
        "the app's own refusal maps to its own ApiError, not a platform error: {}",
        body_text(&refused)
    );
    assert_eq!(
        body_text(&refused),
        "{\"error\":\"org 2 is not one of this principal's orgs\"}",
        "the client gets the refusal `authorize` returned, byte for byte"
    );
    assert_eq!(
        authorize_calls(),
        vec!["alice".to_string(), "alice".to_string()],
        "the refused request must have ASKED the app, about this principal — a \
         platform-side refusal that never called authorize would pass a status-code \
         assertion alone"
    );
    assert_eq!(
        bodies_entered(),
        1,
        "no handler body ran past the refused `scope()` line"
    );

    // And on the store: the refused tenant has no rows, so no scoped write or
    // read reached it. Asked through the ADMITTED tenant's own scope, since
    // asking through the refused one is (correctly) impossible.
    let admitted_rows = app.handle(as_user(
        Request::new(Method::Get, format!("/memo/{ADMITTED}")),
        &token,
    ));
    assert_eq!(admitted_rows.status, 200, "{}", body_text(&admitted_rows));
    assert_eq!(
        body_text(&admitted_rows),
        "\"1\"",
        "the admitted tenant kept its one row"
    );
    assert_eq!(
        app.db().last_applied(),
        Some(1),
        "exactly two write records, the login and the admitted memo: the refused \
         request appended nothing"
    );
}
```

After it, add (a), (d) and (g):

```rust
/// Oracle 2 (a), spec §4 rule 3. A caller with no credential on a plain
/// `public` route reaches the handler, but not the tenant: the scope line
/// answers the named 401, no `authorize` runs (there is no principal to ask
/// about), and nothing below the line runs or writes.
#[test]
fn an_anonymous_caller_on_a_plain_public_route_that_opens_a_scope_gets_the_named_401() {
    let (_sim, mut app) = memo_app(0x7E4A_1702);
    reset_counters();
    let refused = app.handle(Request::new(
        Method::Post,
        format!("/memo-public/{ADMITTED}"),
    ));
    assert_eq!(refused.status, 401, "{}", body_text(&refused));
    assert_eq!(
        error_text(&refused),
        "authentication required — `POST /memo-public/:org` is declared `public` and opened \
         the tenant scope `Org` for a caller with no credential; only a route declared \
         `public(Org)` admits one. Send \"Authorization: Bearer <token>\" or the \
         charpente_session cookie, or, if every caller may act inside this tenant here, \
         declare `#[route(POST \"/memo-public/:org\", public(Org))]`"
    );
    assert_eq!(
        authorize_calls(),
        Vec::<String>::new(),
        "rule 3 refuses before any membership check — authorize is never asked about nobody"
    );
    assert_eq!(bodies_entered(), 0, "nothing below the scope line ran");
    assert_eq!(app.db().last_applied(), None, "and nothing was written");
}

/// Oracle 2 (d), rule 2 on a plain `public` route: a caller WITH a credential
/// is asked about exactly once, as on an `auth` route. The control for (a):
/// the 401 there is about the missing credential, not about the route.
#[test]
fn a_credentialed_caller_on_a_plain_public_route_is_asked_about_once() {
    let (_sim, mut app) = memo_app(0x7E4A_1704);
    let token = login(&mut app, "alice");
    reset_counters();
    let admitted = app.handle(as_user(
        Request::new(Method::Post, format!("/memo-public/{ADMITTED}")),
        &token,
    ));
    assert_eq!(admitted.status, 200, "{}", body_text(&admitted));
    assert_eq!(authorize_calls(), vec!["alice".to_string()]);
    assert_eq!(bodies_entered(), 1);
}

/// Oracle 2 (g). Rule 3 on a stream: the site in the 401 is the word the
/// route was declared with, `SSE`, not the `GET` it is served on — the fix it
/// quotes must be an attribute the author can paste.
#[test]
fn an_anonymous_subscriber_that_opens_a_scope_gets_the_401_naming_the_stream() {
    let (_sim, mut app) = memo_app(0x7E4A_1707);
    reset_counters();
    let refused = app.handle(Request::new(
        Method::Get,
        format!("/memo-feed/{ADMITTED}"),
    ));
    assert_eq!(refused.status, 401, "{}", body_text(&refused));
    assert_eq!(
        error_text(&refused),
        "authentication required — `SSE /memo-feed/:org` is declared `public` and opened the \
         tenant scope `Org` for a caller with no credential; only a route declared \
         `public(Org)` admits one. Send \"Authorization: Bearer <token>\" or the \
         charpente_session cookie, or, if every caller may act inside this tenant here, \
         declare `#[route(SSE \"/memo-feed/:org\", public(Org))]`"
    );
    assert_eq!(authorize_calls(), Vec::<String>::new());
}
```

In the `Firm` doc comment, replace from "Two mechanical reasons, neither stylistic." through "would make that assertion flake." with:

```rust
/// `Org::authorize` admits exactly one tenant (`ADMITTED`) and must keep
/// refusing `REFUSED`, so it cannot host the N >= 2 writable tenants a leak
/// is only expressible with.
```

At the end of the file, add oracle 3:

```rust
// ------------------------------------------ the statuspage regression (oracle 3)

/// Job-tenancy spec §9 item 3. The statuspage experiment's tenant policy, in
/// the shape of `6037704:src/lib.rs:31-59`: `authorize` ADMITS `None`,
/// because its public page, its public feed and its jobs all opened an org
/// with `open_scope(org, None)`. Its finding I-1: the same policy made every
/// `auth` write one word away from an anonymous write. Flip `open_incident`
/// from `auth` to `public`, and any anonymous caller opens incidents in any
/// org. Under the three rules the flipped route meets rule 3, and the store
/// is unchanged.
mod statuspage {
    use charpente::auth::Principal;
    use charpente::http::{ApiError, Ctx};
    use charpente::wire::WireU64;
    use charpente::{Id, Scoped, Tenant, Tx, route, table};

    #[table(global)]
    pub struct Org {
        pub id: Id<Org>,
        pub name: String,
    }

    impl Tenant for Org {
        type Who = Principal;
        type Error = ApiError;

        fn authorize(_tx: &Tx, who: Option<&Principal>, scope: Id<Org>) -> Result<(), ApiError> {
            let Some(principal) = who else {
                return Ok(());
            };
            if principal.subject == format!("operator:{}", scope.raw()) {
                Ok(())
            } else {
                Err(ApiError::BadRequest(format!(
                    "{} is not an operator of org {}",
                    principal.subject,
                    scope.raw()
                )))
            }
        }
    }

    #[table(scoped_by = Org)]
    pub struct Incident {
        pub id: Id<Incident>,
        pub title: String,
    }

    #[derive(charpente::Wire, serde::Serialize, serde::Deserialize)]
    pub struct OpenIncidentBody {
        pub title: String,
    }

    /// `6037704:src/incidents.rs:335-357`'s route, with I-1's one-word edit
    /// (`auth` became `public`) and its body reduced to the write.
    #[route(POST "/orgs/:org/incidents", public)]
    pub fn open_incident(
        tx: &mut Tx,
        org: Id<Org>,
        body: OpenIncidentBody,
        ctx: &Ctx,
    ) -> Result<WireU64, ApiError> {
        let mut tx = tx.scope::<Org>(org, ctx)?;
        tx.put(&Incident {
            id: Id::from_raw(1),
            title: body.title,
        });
        Ok(WireU64(1))
    }
}

#[test]
fn statuspage_i1_an_incident_route_flipped_to_public_refuses_every_anonymous_write() {
    let sim = Sim::with_faults(0x7E4A_1709, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<statuspage::Incident>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![statuspage::open_incident]);

    let refused = app.handle(
        Request::new(Method::Post, "/orgs/1/incidents").json(&statuspage::OpenIncidentBody {
            title: "database down".to_string(),
        }),
    );
    assert_eq!(refused.status, 401, "{}", body_text(&refused));
    // This literal is also GUIDE §5's pinned rule-3 block: doc_truth finds
    // the guide's text here.
    assert_eq!(
        error_text(&refused),
        "authentication required — `POST /orgs/:org/incidents` is declared `public` and \
         opened the tenant scope `Org` for a caller with no credential; only a route declared \
         `public(Org)` admits one. Send \"Authorization: Bearer <token>\" or the \
         charpente_session cookie, or, if every caller may act inside this tenant here, \
         declare `#[route(POST \"/orgs/:org/incidents\", public(Org))]`"
    );
    assert_eq!(
        app.db().last_applied(),
        None,
        "the store is unchanged: no incident, and no record of one"
    );
}
```

- [ ] **Step 2: Run them at the Task 2 commit, and record the red**

```zsh
cargo test -p charpente --test tenant -- --test-threads 1 2>&1 | tee .superpowers/sdd/2026-09-27-job-tenancy/task3-red.log | grep -E "^test |panicked|left:|right:"
```

Expected — quote each in the task report:
- `an_anonymous_caller_on_a_plain_public_route_that_opens_a_scope_gets_the_named_401`: FAIL, `left: 200` / `right: 401`. `Org::authorize(None, 1)` admits tenant 1 and the body runs.
- `an_anonymous_subscriber_that_opens_a_scope_gets_the_401_naming_the_stream`: FAIL, `left: 500` / `right: 401`. `authorize` admitted, the handler returned a `Subscription`, and `App::handle` answers a subscription 500.
- `statuspage_i1_an_incident_route_flipped_to_public_refuses_every_anonymous_write`: FAIL, `left: 200` / `right: 401`. This is I-1 reproduced.
- `a_refusing_authorize_is_the_answer_and_nothing_below_the_scope_line_runs` and `a_credentialed_caller_on_a_plain_public_route_is_asked_about_once`: PASS, as controls.
- Every other test in the file passes.

`--test-threads 1` is only for a readable log; the thread-local counters make the parallel run correct as well.

- [ ] **Step 3: Implement**

**`crates/charpente/src/http/router.rs`.** Add `use std::any::TypeId;`. Add after `EmittedEvent`:

```rust
/// `#[route(…, public(Org))]` — the tenant scope every caller of this route,
/// credentialed or not, may act inside (job-tenancy spec §4). `Scoped::scope`
/// admits a scope of exactly this type by the declaration, with no
/// `Tenant::authorize`; any other scope type the handler opens meets the
/// membership check as on any route.
#[derive(Clone, Copy, Debug)]
pub struct PublicScope {
    /// The scope type's last path segment, as the macro's refusals render it.
    pub name: &'static str,
    /// The scope type's `TypeId`, behind a fn pointer like `response` and
    /// `error`, so a `Route` stays plain `'static` data and fn pointers.
    pub type_id: fn() -> TypeId,
}
```

In `pub struct Route`, after `pub auth: bool,`:

```rust
    /// `public(Org)`: `Some` exactly when the route declared a public tenant
    /// scope. Read by `stage_or_subscribe`, which mints the request's
    /// `TenantAdmission` for this route only, and listed by `describe`.
    pub public_scope: Option<PublicScope>,
```

The test helper `route()` gets `public_scope: None,` after `auth: false,`.

**`crates/charpente/src/http/mod.rs`.** Add `PublicScope` to `pub use router::{…}`. Add the variant after `Oidc(OidcReject),`:

```rust
    /// No credential presented, at a tenant scope line the route did not
    /// declare `public(S)` for (job-tenancy spec §4, rule 3). The text is
    /// built by `Ctx`, because it quotes the route's site and stance.
    AnonymousScope(String),
```

And its arm in `ApiError::response`, after the `Oidc` arm:

```rust
            ApiError::AnonymousScope(msg) => Response::error(401, msg),
```

**`crates/charpente/src/http/app.rs`.**
- Add `use std::any::TypeId;`.
- Add `TenantAdmission` to the `charpente_core::db` import.
- Change the router import to `use crate::http::router::{PublicScope, RouteGlue, RouteMatch, Router};`.

Add a field to `Ctx`, after `cap`:

```rust
    /// The matched route's stance (job-tenancy spec §4): the admission a
    /// `public(S)` route carries, and the site and declaration rule 3's 401
    /// quotes. Private, and never exposed — a handler cannot reach the
    /// admission, only `Scoped::scope` can.
    stance: Stance,
```

Add two methods to `impl Ctx`, after `cap()`:

```rust
    /// Rule 1 of `Scoped::scope` (job-tenancy spec §4): the admission this
    /// request carries when its route declared `public(S)` for exactly this
    /// `S`. `None` otherwise, including on a `public(Org)` route that opens
    /// some other scope type, which then meets rules 2 and 3 like any route.
    pub(crate) fn public_admission<S: 'static>(&self) -> Option<&TenantAdmission> {
        match &self.stance.public {
            Some((declared, admission)) if (declared.type_id)() == TypeId::of::<S>() => {
                Some(admission)
            }
            _ => None,
        }
    }

    /// Rule 3's refusal: a caller with no credential reached a scope line
    /// that its route did not declare `public(S)` for. Names the contract,
    /// the site, and both fixes, the caller's and the author's (hard rule 5).
    pub(crate) fn anonymous_scope<S>(&self) -> ApiError {
        let Stance {
            method,
            pattern,
            auth,
            public,
        } = &self.stance;
        let declared = match public {
            Some((scope, _)) => format!("public({})", scope.name),
            None if *auth => "auth".to_string(),
            None => "public".to_string(),
        };
        // The last path segment, as `decode_body` renders a type.
        let scope = std::any::type_name::<S>()
            .rsplit("::")
            .next()
            .unwrap_or("the scope type");
        ApiError::AnonymousScope(format!(
            "authentication required — `{method} {pattern}` is declared `{declared}` and \
             opened the tenant scope `{scope}` for a caller with no credential; only a route \
             declared `public({scope})` admits one. Send \"Authorization: Bearer <token>\" or \
             the charpente_session cookie, or, if every caller may act inside this tenant \
             here, declare `#[route({method} \"{pattern}\", public({scope}))]`"
        ))
    }
```

Add after `impl Ctx`:

```rust
/// A matched route's declaration, copied out of the router: the half of a
/// `Stance` that needs no `Group` (job-tenancy amendment A3). It is split
/// from `Stance` so the drift probe can read it before `App::begin_group`
/// borrows the whole `App`, the reason the probe already reads
/// `cross_tenant` first.
#[derive(Clone, Copy)]
struct Declared {
    /// `SSE` for a stream route, else the HTTP method: the word the route was
    /// declared with, which the 401's site and its fix both quote.
    method: &'static str,
    pattern: &'static str,
    auth: bool,
    public_scope: Option<PublicScope>,
}

impl Declared {
    fn of(route: &Route) -> Declared {
        Declared {
            method: match route.glue {
                RouteGlue::Sse(_) => "SSE",
                RouteGlue::Http(_) => route.method.name(),
            },
            pattern: route.pattern,
            auth: route.auth,
            public_scope: route.public_scope,
        }
    }

    /// Mints the admission for a `public(S)` route, and only for one.
    fn admitted(self, group: &Group) -> Stance {
        Stance {
            method: self.method,
            pattern: self.pattern,
            auth: self.auth,
            public: self
                .public_scope
                .map(|scope| (scope, group.tenant_admission())),
        }
    }
}

/// What `Scoped::scope` reads off the route to apply its three rules.
struct Stance {
    method: &'static str,
    pattern: &'static str,
    /// Recorded for the 401's text only (amendment A11): the drift probe
    /// does not enforce `auth`, so an `auth` stream route that opens a scope
    /// meets rule 3 there, and its text must name the real stance.
    auth: bool,
    public: Option<(PublicScope, TenantAdmission)>,
}
```

In `build_ctx`:
- add a final parameter `stance: Stance`;
- add `stance,` to the `Ctx { … }` literal;
- in the comment above its `#[allow(clippy::too_many_arguments)]`, change "One more independent request fact than clippy's limit" to "Two more independent request facts than clippy's limit", and "the declared capability joins" to "the declared capability and the route's stance join".

In `stage_or_subscribe`, directly after `let cap = route.cross_tenant.then(|| group.cross_tenant_cap());`:

```rust
        // Beside the capability, for the same reason: a `public(S)` route's
        // admission is minted from the `Group`, which a handler never holds
        // (job-tenancy spec §4).
        let stance = Declared::of(route).admitted(group);
```

Pass `stance` as the last argument of both `build_ctx` calls. Each call sits in its own match arm, so each moves it once.

In `subscription_event_names`, directly after `let declares_cross_tenant = found.cross_tenant;`, add `let declared = Declared::of(found);`. After the `let cap = …` line, add `let stance = declared.admitted(&group);`, and pass `stance` as `build_ctx`'s last argument. In its doc comment's "Three deliberate departures" paragraph, after "so an `auth` stream route is probeable)", insert: "— unless it opens a tenant scope: with no principal, `tx.scope` meets rule 3 exactly as a request would, and the probe reports the route as not probeable, quoting that 401. A `public(S)` route's admission is minted for the probe as it is for a request".

**`crates/charpente/src/tenant.rs`.** Replace `impl<'a> Scoped<'a> for Tx<'a>`'s method and its doc:

```rust
impl<'a> Scoped<'a> for Tx<'a> {
    /// The three rules, in order (job-tenancy spec §4):
    ///
    /// 1. The route declared `public(S)` for this `S`: admitted by that
    ///    declaration, and no `authorize` runs, whoever the caller is.
    /// 2. A principal is present: `open_scope` runs `S::authorize` once,
    ///    about that principal, and its refusal is the answer.
    /// 3. Otherwise, the named 401, and nothing below the scope line runs.
    ///
    /// An `auth` route never reaches rule 3, because `build_ctx` refused the
    /// anonymous caller before the handler ran. So the platform never asks
    /// `authorize` about nobody.
    fn scope<S: TenantScope>(
        &mut self,
        scope: Id<S>,
        ctx: &Ctx,
    ) -> Result<ScopedTx<'_, 'a, S>, ApiError> {
        if let Some(admission) = ctx.public_admission::<S>() {
            return Ok(self.admit_scope(scope, admission));
        }
        match ctx.principal() {
            Some(principal) => self.open_scope(scope, Some(principal)),
            None => Err(ctx.anonymous_scope::<S>()),
        }
    }
}
```

In `TenantScope`'s doc, replace the paragraph "`who` is `None` for an unauthenticated request and for a direct `Tx::open_scope(scope, None)` call — … unless that is intended." with:

```rust
/// `who` is `None` only for a direct `Tx::open_scope(scope, None)` call.
/// `tx.scope` never passes `None`: rule 3 answers a caller with no credential
/// with a 401 instead (`Scoped::scope`). So an `authorize` that admits `None`
/// admits exactly those direct calls.
```

**`crates/charpente-core/src/db.rs`.** In `TenantAdmission`'s doc, after the first paragraph, add:

```rust
/// Holders: the operator, through a `Group`; and the platform, which mints
/// one per request for a route declared `public(S)` and keeps it private in
/// that request's `Ctx`, so `tx.scope::<S>` admits by the declaration.
```

**`crates/charpente-macros/src/route.rs`.**

Module doc, first line:

```rust
//! `#[route(METHOD "/path/:param", auth | public | public(Scope)[, cross_tenant][, emits(Ev, …)])]`
```

In `RouteArgs`, after `public: bool,`:

```rust
    /// `public(Scope)` — the declaration that every caller, credentialed or
    /// not, may act inside the `Scope` tenant this route opens, with no
    /// `authorize` (job-tenancy spec §4). `Some` implies `public`.
    public_scope: Option<syn::Path>,
```

In `impl Parse for RouteArgs`:
- the method error's text becomes `"write `#[route(METHOD \"/path\", auth | public | public(<Scope>))]` — e.g. #[route(GET \"/notes/:id\", auth)]"`;
- add `let mut public_scope: Option<syn::Path> = None;` beside `let mut public = false;`;
- replace the `public` branch with:

```rust
            } else if flag == "public" {
                if public {
                    return Err(Error::new(
                        flag.span(),
                        "`public` given twice — write it once",
                    ));
                }
                if auth {
                    return Err(Error::new(flag.span(), ONE_OR_THE_OTHER));
                }
                public = true;
                if input.peek(syn::token::Paren) {
                    let content;
                    parenthesized!(content in input);
                    let mut scopes = content.parse_terminated(syn::Path::parse, Token![,])?;
                    if scopes.len() != 1 {
                        return Err(Error::new(flag.span(), PUBLIC_SCOPE_ARITY));
                    }
                    public_scope = scopes.pop().map(|pair| pair.into_value());
                }
```

- add `public_scope,` to the returned `RouteArgs`.

Replace `FLAGS`, and add `PUBLIC_SCOPE_ARITY` after `ONE_OR_THE_OTHER`:

```rust
const FLAGS: &str = "a route flag is `auth`, `public`, `public(<Scope>)`, `cross_tenant` or \
                     `emits(Ev, …)` — write `#[route(GET \"/path\", auth)]`, \
                     `#[route(GET \"/path\", public)]`, \
                     `#[route(GET \"/orgs/:org/status\", public(Org))]`, or \
                     `#[route(SSE \"/path\", emits(NoteAdded, NoteRemoved))]`";

/// `public(...)` with no path, or with several. Spanned at the flag, the one
/// token both shapes share.
const PUBLIC_SCOPE_ARITY: &str = "`public(...)` names exactly one tenant scope type — write \
                                  `public(Org)`, or plain `public` if anonymous callers enter \
                                  no tenant here";
```

In `undeclared_capability`, replace the `stance` line and its comment:

```rust
    // `expand` refused a route with neither flag before reaching here.
    let stance = match &args.public_scope {
        Some(path) => format!(", public({})", scope_name(path)),
        None if args.auth => ", auth".to_string(),
        None => ", public".to_string(),
    };
```

The neither-refusal in `expand` becomes:

```rust
            format!(
                "`{name}` declares neither `auth` nor `public` — a route says which callers \
                 reach it: write `#[route({method} \"{path}\", auth)]` to require a principal \
                 (`ctx.principal()` is `Some`), `#[route({method} \"{path}\", public)]` to \
                 serve anonymous callers, or `#[route({method} \"{path}\", public(Org))]` to \
                 also let every caller act inside the `Org` tenant it opens. None is the \
                 default."
            ),
```

Add after `arg_name`:

```rust
/// A scope path's last segment — `Org` for `public(crate::tenancy::Org)` — as
/// `PublicScope.name` carries it and the refusals render it.
fn scope_name(path: &syn::Path) -> String {
    path.segments
        .last()
        .expect("syn parses a path with at least one segment")
        .ident
        .to_string()
}

/// `public(Scope)`'s two emissions: the `PublicScope` the `Route` carries, and
/// `#[table(scoped_by = …)]`'s `TenantScope` const assert, so a scope type
/// with no membership check is refused with the same diagnostic here as there.
fn public_scope_tokens(args: &RouteArgs) -> (TokenStream, TokenStream) {
    let Some(path) = &args.public_scope else {
        return (quote! { ::std::option::Option::None }, quote! {});
    };
    let name = scope_name(path);
    (
        quote! {
            ::std::option::Option::Some(::charpente::http::PublicScope {
                name: #name,
                type_id: ::std::any::TypeId::of::<#path>,
            })
        },
        quote! {
            const _: () = {
                const fn __charpente_assert_tenant_scope<T: ::charpente::TenantScope>() {}
                __charpente_assert_tenant_scope::<#path>();
            };
        },
    )
}
```

In both `expand_http` and `expand_sse`:
- next to `let auth = args.auth;`, add `let (public_scope, tenant_scope_assert) = public_scope_tokens(&args);`;
- in the emitted `quote!`, add `#tenant_scope_assert` on the line after `#func`;
- in the `Route` literal, add `public_scope: #public_scope,` after `auth: #auth,`.

**Hand-built `Route` literals.** Add `public_scope: None,` after `auth: false,` at `serve.rs` 960, 1037, 1051, 1197, 1284 and 1298, and at `realtime.rs:2821`. Then prove none is missed: `cargo check --workspace --all-targets` compiles.

- [ ] **Step 4: Migrate the public routes that open a scope**

Each of these is served to callers with no credential and opens a tenant its `authorize` admits unconditionally. Under rule 3 they would answer 401. `public(Org)` is what each one always meant: every caller may act inside the tenant. None of them writes a different byte, because `authorize` wrote nothing, and minting an admission is neither a record nor an entropy draw. Step 7 measures that.

- `crates/charpente/tests/kernel.rs:1435`: `#[route(POST "/post", public)]` becomes `#[route(POST "/post", public(Org))]`. Replace `Org::authorize`'s doc comment (~1386–1389) with:

  ```rust
      /// Always allows, and `post` does not even ask: it is declared
      /// `public(Org)`, so the swarm's credential-free clients enter any tenant by
      /// the declaration (job-tenancy spec §4). This property is about the
      /// platform's isolation guarantee, not the app's membership check —
      /// `charpente/tests/tenant.rs` owns the refusing case.
  ```

- `crates/charpente/tests/realtime.rs`:
  - `emit_scoped_ping`, `scoped_feed` and `emit_leaky_ping` become `public(Org)` (`scoped_feed` keeps its `emits(Ping)`: `#[route(SSE "/scoped-feed/:org", public(Org), emits(Ping))]`).
  - `Org::authorize`'s doc sentence "Always allows: this property is about the platform's key derivation, not the app's membership check" becomes "Always allows, and the scoped routes below do not ask: they are declared `public(Org)`. This property is about the platform's key derivation, not the app's membership check".
  - `scoped_feed`'s doc: "opening the scope both runs the app's `authorize` for the subscriber and yields the very key the emitter used" becomes "opening the scope admits the subscriber by the route's `public(Org)` declaration and yields the very key the emitter used".
  - `emit_leaky_ping`'s doc: "`authorize` still runs, the row-level tenant bound still holds" becomes "the declaration still admits, and the row-level tenant bound still holds".
- `crates/charpente/tests/tenant.rs`: `write_note` becomes `#[route(PUT "/note/:team/:id", public(Team))]` and `count_notes` becomes `#[route(GET "/note/:team", public(Team))]`.

Then run every suite that drives these routes:

```zsh
cargo test -p charpente --test tenant
cargo test -p charpente --test realtime
cargo test -p charpente --test kernel
```

Expected: all pass.

- [ ] **Step 5: Add the tests and fixtures that need the new syntax**

In `crates/charpente/tests/tenant.rs`, after `write_memo_public`:

```rust
/// Every caller may act inside the org it names: rule 1, no membership check.
#[route(POST "/memo-open/:org", public(Org))]
fn write_memo_open(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    memo_write(tx, org, ctx)
}

/// Declared `public(Org)`, but it opens `Team`: the declaration names one scope
/// type and admits that type only.
#[route(POST "/memo-team/:team", public(Org))]
fn write_team_memo(tx: &mut Tx, team: Id<Team>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Team>(team, ctx)?;
    BODIES_ENTERED.with(|n| n.set(n.get() + 1));
    Ok(WireU64(tx.scope().raw()))
}
```

After `memo_feed`:

```rust
#[route(SSE "/memo-feed-open/:org", public(Org), emits(MemoNoted))]
fn memo_feed_open(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(Subscription::of::<MemoNoted>()
        .key(tx.event_key())
        .cursor(cursor))
}

#[route(SSE "/memo-feed-auth/:org", auth, emits(MemoNoted))]
fn memo_feed_auth(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(Subscription::of::<MemoNoted>()
        .key(tx.event_key())
        .cursor(cursor))
}
```

`memo_app` mounts all nine:

```rust
    .mount(routes![
        memo_login,
        write_memo,
        write_memo_public,
        write_memo_open,
        write_team_memo,
        count_memos,
        memo_feed,
        memo_feed_open,
        memo_feed_auth
    ]);
```

After test (g), add (b), (c), (f) and (h):

```rust
/// Oracle 2 (b), rule 1: a `public(Org)` route admits a caller with no
/// credential into the org it names, and asks no membership check.
#[test]
fn public_scope_admits_an_anonymous_caller_with_no_authorize() {
    let (_sim, mut app) = memo_app(0x7E4A_1703);
    reset_counters();
    let admitted = app.handle(Request::new(
        Method::Post,
        format!("/memo-open/{ADMITTED}"),
    ));
    assert_eq!(admitted.status, 200, "{}", body_text(&admitted));
    assert_eq!(
        authorize_calls(),
        Vec::<String>::new(),
        "rule 1: the declaration admits, and no membership check runs"
    );
    assert_eq!(bodies_entered(), 1);
    assert_eq!(app.db().last_applied(), Some(0), "the admitted write committed");
}

/// Oracle 2 (c), rule 1 before rule 2, and Review Focus 5: on `public(Org)`
/// the declaration beats the membership check. A credentialed caller whose
/// `authorize` WOULD refuse this tenant is admitted with zero calls, and the
/// same caller on the route's `auth` twin is refused by the app's own error.
#[test]
fn public_scope_admits_a_credentialed_caller_its_membership_check_would_refuse() {
    let (_sim, mut app) = memo_app(0x7E4A_1705);
    let token = login(&mut app, "alice");
    reset_counters();

    let control = app.handle(as_user(
        Request::new(Method::Post, format!("/memo/{REFUSED}")),
        &token,
    ));
    assert_eq!(control.status, 400, "{}", body_text(&control));
    assert_eq!(error_text(&control), "org 2 is not one of this principal's orgs");
    assert_eq!(authorize_calls(), vec!["alice".to_string()]);
    assert_eq!(bodies_entered(), 0);

    reset_counters();
    let admitted = app.handle(as_user(
        Request::new(Method::Post, format!("/memo-open/{REFUSED}")),
        &token,
    ));
    assert_eq!(admitted.status, 200, "{}", body_text(&admitted));
    assert_eq!(
        authorize_calls(),
        Vec::<String>::new(),
        "the route said every caller may act in this org, so nobody's membership is asked — \
         not even a principal's"
    );
    assert_eq!(bodies_entered(), 1);
}

/// Oracle 2 (f). `public(Org)` admits `Org` scopes only. The route below opens
/// `Team`, so an anonymous caller meets rule 3, and the 401 names the stance
/// the route really declared.
#[test]
fn public_scope_for_one_scope_type_does_not_admit_another() {
    let (_sim, mut app) = memo_app(0x7E4A_1706);
    reset_counters();
    let refused = app.handle(Request::new(Method::Post, "/memo-team/5"));
    assert_eq!(refused.status, 401, "{}", body_text(&refused));
    assert_eq!(
        error_text(&refused),
        "authentication required — `POST /memo-team/:team` is declared `public(Org)` and \
         opened the tenant scope `Team` for a caller with no credential; only a route \
         declared `public(Team)` admits one. Send \"Authorization: Bearer <token>\" or the \
         charpente_session cookie, or, if every caller may act inside this tenant here, \
         declare `#[route(POST \"/memo-team/:team\", public(Team))]`"
    );
    assert_eq!(bodies_entered(), 0);
}

/// Spec §4: the `emits(...)` drift probe meets the three rules exactly as a
/// request does. A `public(Org)` stream is probeable, because the probe mints
/// the admission a request would. The probe presents no credential and does
/// not enforce `auth`, so on a plain `public` or an `auth` stream the scope
/// line is rule 3, and the probe's refusal quotes the route's real stance
/// (amendment A11).
#[test]
fn the_drift_probe_meets_the_three_rules_as_a_request_does() {
    let (_sim, mut app) = memo_app(0x7E4A_1708);
    assert_eq!(
        app.subscription_event_names("memo_feed_open"),
        Ok(vec!["memo_noted"])
    );
    let public = app.subscription_event_names("memo_feed").unwrap_err();
    assert!(
        public.contains(
            "`SSE /memo-feed/:org` is declared `public` and opened the tenant scope `Org`"
        ),
        "{public}"
    );
    let auth = app.subscription_event_names("memo_feed_auth").unwrap_err();
    assert!(
        auth.contains(
            "`SSE /memo-feed-auth/:org` is declared `auth` and opened the tenant scope `Org`"
        ),
        "{auth}"
    );
}
```

Create `crates/charpente-macros/tests/compile_pass/route_public_scope.rs`:

```rust
// `#[route(…, public(Org))]` (job-tenancy spec §4): every caller,
// credentialed or not, may act inside the `Org` tenant the route opens. The
// flag composes with `cross_tenant` and `emits(...)` in any order, the scope
// may be path-qualified, and the `Route` carries its name and `TypeId`.
// trybuild runs `main`, so its assertions are part of the pin.
use charpente::http::{ApiError, Ctx, Cursor, RouteDef, Subscription};
use charpente::wire::WireU64;
use charpente::{CrossTenant, Id, Scoped, Tx, event, route, table};

mod tenancy {
    use charpente::auth::Principal;
    use charpente::http::ApiError;
    use charpente::{Id, Tenant, Tx, table};

    #[table(global)]
    pub struct Org {
        pub id: Id<Org>,
        pub name: String,
    }

    impl Tenant for Org {
        type Who = Principal;
        type Error = ApiError;
        fn authorize(_tx: &Tx, _who: Option<&Principal>, _scope: Id<Org>) -> Result<(), ApiError> {
            Err(ApiError::Unauthorized)
        }
    }
}
use tenancy::Org;

#[table(scoped_by = Org)]
struct Incident {
    id: Id<Incident>,
    title: String,
}

#[event]
struct IncidentOpened {
    n: WireU64,
}

#[route(GET "/orgs/:org/status", public(Org))]
fn status_page(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(WireU64(tx.scan::<Incident>()?.len() as u64))
}

#[route(GET "/orgs/:org/summary", public(tenancy::Org))]
fn summary(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(WireU64(tx.scope().raw()))
}

#[route(GET "/status", cross_tenant, public(Org))]
fn every_status(tx: &mut Tx, cap: &CrossTenant) -> Result<WireU64, ApiError> {
    Ok(WireU64(
        tx.across_tenants::<Org>(cap).scan::<Incident>()?.len() as u64,
    ))
}

#[route(SSE "/orgs/:org/feed", public(Org), emits(IncidentOpened))]
fn feed(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(Subscription::of::<IncidentOpened>()
        .key(tx.event_key())
        .cursor(cursor))
}

fn main() {
    for route in [
        status_page::route(),
        summary::route(),
        every_status::route(),
        feed::route(),
    ] {
        let scope = route
            .public_scope
            .expect("public(Org) is carried on the Route");
        assert_eq!(scope.name, "Org", "{}", route.name);
        assert_eq!((scope.type_id)(), std::any::TypeId::of::<Org>(), "{}", route.name);
        assert!(!route.auth, "public(Org) is public: {}", route.name);
    }
}
```

Create `crates/charpente-macros/tests/compile_fail/route_public_scope_arity.rs`:

```rust
// `public(...)` names exactly one tenant scope type (job-tenancy spec §4),
// and it is still `public`: it cannot join `auth`, and a route says `public`
// once, in either form. Each handler below is one refusal, reported at the
// `public` flag. Types are named by path, because a refused route emits
// nothing, so an import would come back as an unused-import warning.
use charpente::route;

#[route(GET "/a", public())]
fn no_scope(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

#[route(GET "/b", public(Org, Team))]
fn two_scopes(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

#[route(GET "/c", auth, public(Org))]
fn with_auth(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

#[route(GET "/d", public, public(Org))]
fn twice(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
```

Create `crates/charpente-macros/tests/compile_fail/route_public_scope_missing_tenant_scope.rs`:

```rust
// `public(Foo)` puts `Foo` in a tenant scope's place, so `Foo` must carry
// the app's own membership check. The route emits the same `TenantScope`
// const assert `#[table(scoped_by = …)]` does, and gets the same diagnostic,
// spanned at the path inside `public(...)`.
use charpente::http::ApiError;
use charpente::wire::WireU64;
use charpente::{Id, Tx, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[route(GET "/orgs/:org/status", public(Org))]
fn status_page(tx: &mut Tx, org: Id<Org>) -> Result<WireU64, ApiError> {
    let _ = tx;
    Ok(WireU64(org.raw()))
}

fn main() {}
```

Create `crates/charpente-macros/tests/compile_fail/route_public_scope_undeclared_capability.rs`:

```rust
// The capability refusal (tenant-scoping spec §3) reconstructs the attribute
// the author should have written, so on a `public(Org)` route it keeps the
// `public(Org)` (job-tenancy spec §4). `CrossTenant` and `WireU64` are named
// by path for `across_tenants_without_capability.rs`'s reason.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::{Id, Tenant, Tx, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: Option<&Principal>, _scope: Id<Org>) -> Result<(), ApiError> {
        Err(ApiError::Unauthorized)
    }
}

#[route(GET "/orgs/:org/revenue", public(Org))]
fn revenue(
    tx: &mut Tx,
    org: Id<Org>,
    cap: &charpente::CrossTenant,
) -> Result<charpente::wire::WireU64, ApiError> {
    let _ = (tx, org, cap);
    Ok(charpente::wire::WireU64(0))
}

fn main() {}
```

- [ ] **Step 6: Run, and review every pin**

```zsh
cargo test -p charpente --test tenant
TRYBUILD=overwrite cargo test -p charpente-macros --test trybuild
git diff --stat crates/charpente-macros/tests/compile_fail/
for f in route_public_scope_arity route_public_scope_missing_tenant_scope route_public_scope_undeclared_capability; do
  echo "--- $f"; cat crates/charpente-macros/tests/compile_fail/$f.stderr
done
git diff crates/charpente-macros/tests/compile_fail/
cargo test -p charpente-macros --test trybuild
```

Expected:
- `tenant`: every test passes, all eight of oracle 2's, oracle 3, and the rest.
- `route_public_scope_arity.stderr`: four errors, each spanned at a `public`:
  - `/a` and `/b` read `` `public(...)` names exactly one tenant scope type — write `public(Org)`, or plain `public` if anonymous callers enter no tenant here``;
  - `/c` reads `` `auth` and `public` on one route — a route is one or the other; drop one``;
  - `/d` reads `` `public` given twice — write it once``, at the second `public`.
- `route_public_scope_missing_tenant_scope.stderr`: `error[E0277]: `Org` is used as a tenant scope but has no `Tenant` impl`, with the label `no `impl Tenant for Org`` under the `Org` in `public(Org)`, and the note beginning "a scoped table needs the app's own membership check; add:". Task 4 rewords that note's opening, and re-pins this file.
- `route_public_scope_undeclared_capability.stderr`: `` `revenue` takes `cap: &CrossTenant`, but `#[route(GET "/orgs/:org/revenue", public(Org))]` does not declare `cross_tenant` — … Write `#[route(GET "/orgs/:org/revenue", public(Org), cross_tenant)]`, or drop the `cap` argument …``, spanned at `cap`.
- Changed pins, and only these:
  - `helpdesk_route_neither_auth_nor_public.stderr`: the neither-refusal's new text, with `public(Org)` and "None is the default.";
  - `route_unknown_flag.stderr`: the new `FLAGS`;
  - `route_missing_method.stderr`: `write `#[route(METHOD "/path", auth | public | public(<Scope>))]` — e.g. #[route(GET "/notes/:id", auth)]`.

  Any other changed `.stderr` is a finding: read it before accepting it.
- Check each new or changed `.stderr` line by line against the source strings in `route.rs`. Hard rule 5 is only as good as that read.

- [ ] **Step 7: Prove red by mutation, and measure byte identity**

Save the implementation first, so each mutation reverts exactly:

```zsh
D=.superpowers/sdd/2026-09-27-job-tenancy
git diff crates/charpente/src/tenant.rs crates/charpente/src/http/app.rs > $D/task3-impl.patch
```

Apply each mutation alone, run `cargo test -p charpente --test tenant`, and quote the named failures. Then restore with `git checkout crates/charpente/src/tenant.rs crates/charpente/src/http/app.rs && git apply $D/task3-impl.patch`, and check `git diff --stat` shows Step 3's diff again.

1. In `Scoped::scope`, delete rule 1 (the `if let Some(admission) … { return …; }` block). Expected: (b) fails with 401, and (c) fails with 400.
2. Move rule 1 below rule 2: `if let Some(principal) = ctx.principal() { return self.open_scope(scope, Some(principal)); }`, then the rule-1 block, then `Err(ctx.anonymous_scope::<S>())`. Expected: (c) fails at `left: 400` (the membership check ran first); (b) passes. This is Review Focus 5's own red.
3. Replace rule 3's arm with `None => self.open_scope(scope, None),`, which is today's behaviour. Expected: (a) and oracle 3 fail at `left: 200`, and (g) at `left: 500`.
4. In `Ctx::public_admission`, drop the `TypeId` guard: `Some((_, admission)) => Some(admission),`. Expected: (f) fails at `left: 200`.
5. In `subscription_event_names`, replace `Declared::of(found)` with `Declared { public_scope: None, ..Declared::of(found) }`. Expected: (h) fails at its first assertion.
6. In `Ctx::anonymous_scope`, delete the `None if *auth => "auth".to_string(),` arm. Expected: (h) fails on the `memo_feed_auth` assertion, and (e) still passes, because an `auth` request never reaches rule 3.

Then measure byte identity against Task 1's baselines:

```zsh
D=.superpowers/sdd/2026-09-27-job-tenancy
cargo test -p charpente --test tenant -- every_scoped_read_returns_only_its_own_tenants_rows --exact --nocapture 2>&1 | tee $D/tenant-swarm-task3.log | grep "^test result"
CHARPENTE_BASE_SEED=0x3E7A0927 cargo test -p charpente --test kernel -- kernel_jobs_are_exactly_once_under_the_fault_matrix --exact --nocapture 2>&1 | tee $D/kernel-swarm-task3.log | grep "^test result"
cargo test -p charpente --test realtime -- a_reconnecting_tenant_subscriber_misses_nothing_and_leaks_nothing --exact --nocapture 2>&1 | tee $D/realtime-swarm-task3.log | grep "^test result"
for f in tenant kernel realtime; do
  grep -E "^charpente-sim|^  [^ ]" $D/$f-swarm-task3.log > $D/$f-coverage-task3.txt
  diff $D/$f-coverage-base.txt $D/$f-coverage-task3.txt && echo "$f: identical"
done
```

Expected: three `identical` lines. `kernel.rs`'s `post` and `realtime.rs`'s three routes now admit by declaration where they used to ask an `authorize` that wrote nothing, so no universe may move. A non-empty diff is a finding (Global Constraints).

- [ ] **Step 8: Docs**

**`docs/GUIDE.md` §5, "The `#[route]` argument convention".** Replace "exactly one of `, auth` / `, public` follows the path — a route says which callers reach it, and a route that says neither is a compile error naming both; `auth` is covered under Sessions below)" with:

> exactly one of `, auth`, `, public` or `, public(Org)` follows the path — a route says which callers reach it, and `public(Org)` adds that every caller may act inside the `Org` tenant it opens (see "A route's stance" below); a route that says none is a compile error naming all three; `auth` is covered under Sessions below)

**§5, "Auth: sessions, API keys, and the resolution ladder".** In the paragraph beginning "`#[route(METHOD "/path", auth)]` rejects the request before your handler runs", after "(possibly `None`)" insert: " — though a handler that then opens a tenant scope with no principal gets a 401 of its own, unless the route declared `public(Org)` (below)".

**§5, a new subsection before "### `ApiError` and status mapping":**

````markdown
### A route's stance, and who reaches a tenant

Every route declares one of three stances after its path, and the stance decides what
`tx.scope::<Org>(org, ctx)?` (§16) does for each caller:

| Stance | Who reaches the handler | What the scope line does |
|---|---|---|
| `auth` | a caller with a credential | runs `Org::authorize` once, about that principal |
| `public` | every caller | with a credential, the same; with none, the 401 below |
| `public(Org)` | every caller | admits every caller into the `Org` tenant it names; no `authorize` runs |

The scope line applies three rules, in order:

1. If the route declared `public(S)` for this very `S`, the scope is admitted by that
   declaration. No `authorize` runs, whoever the caller is. A credentialed caller whose
   membership check would refuse is admitted too: the route said every caller may act
   here.
2. Otherwise, with a principal present, `S::authorize` runs once, about that principal,
   and its `Err` is the handler's `Err`.
3. Otherwise the caller has no credential, and the request is answered 401, by name.
   Nothing below the scope line runs.

An `auth` route never reaches rule 3, because the `auth` gate refused the anonymous caller
before your handler ran. So your `authorize` is only ever asked about a real principal. A
`public(Org)` route that opens a *different* scope type gets rules 2 and 3 for it, and the
401 names `public(Org)` as the route's stance.

The rule-3 401, for a `POST /orgs/:org/incidents` declared plain `public`:

<!-- pinned -->
```text
authentication required — `POST /orgs/:org/incidents` is declared `public` and opened the tenant scope `Org` for a caller with no credential; only a route declared `public(Org)` admits one. Send "Authorization: Bearer <token>" or the charpente_session cookie, or, if every caller may act inside this tenant here, declare `#[route(POST "/orgs/:org/incidents", public(Org))]`
```

The site is the word the route was declared with (`SSE` for a stream) and its pattern;
the scope is the type's own name. Both fixes are there: the caller's (send a credential)
and the author's (declare the tenant public, if that really is the policy). §16's "Who
reaches a tenant" shows the three side by side.
````

**§5, "`ApiError` and status mapping".** In the 401-family bullet, after "`Oidc(..)` (one per failed OIDC check)" insert ", and `AnonymousScope(..)`, a scope line reached with no credential on a route that did not declare that tenant public (above)".

**§5, "What `#[route]` rejects".** Replace the pinned block's first line with:

```text
`<handler>` declares neither `auth` nor `public` — a route says which callers reach it: write `#[route(<method> "<path>", auth)]` to require a principal (`ctx.principal()` is `Some`), `#[route(<method> "<path>", public)]` to serve anonymous callers, or `#[route(<method> "<path>", public(Org))]` to also let every caller act inside the `Org` tenant it opens. None is the default.
```

**§9, "`#[route]` argument-convention violations".**
- Replace the catalog's first line with the same text as the pinned line above.
- After `` `public` given twice — write it once``, insert `` `public(...)` names exactly one tenant scope type — write `public(Org)`, or plain `public` if anonymous callers enter no tenant here``.
- Replace the `a route flag is …` line with `` a route flag is `auth`, `public`, `public(<Scope>)`, `cross_tenant` or `emits(Ev, …)` — write `#[route(GET "/path", auth)]`, `#[route(GET "/path", public)]`, `#[route(GET "/orgs/:org/status", public(Org))]`, or `#[route(SSE "/path", emits(NoteAdded, NoteRemoved))]` ``.
- Replace the last line with `` write `#[route(METHOD "/path", auth | public | public(<Scope>))]` — e.g. #[route(GET "/notes/:id", auth)]``.
- In the prose after the catalog, replace "a flag after the path is none of `auth`, `public`, `cross_tenant`, `emits(...)`;" with "a flag after the path is none of `auth`, `public`, `public(<Scope>)`, `cross_tenant`, `emits(...)`; `public(...)` names no scope type, or more than one;".

**§11, "Emitting: `tx.emit` and `tx.emit_keyed`".** The feed example's attribute becomes `#[route(SSE "/feed/:org", auth, emits(NoteAdded))]`. Its "authorizes the subscriber, too" comment is now true for every caller who reaches it.

**§16, the introduction.** After the bullet "exactly one line per handler, `let tx = tx.scope::<Org>(org, ctx)?;`, after which everything reads and writes inside that tenant.", add:

> - each route that opens a scope says who may: `auth` or `public`, where the membership check decides for each principal, or `public(Org)`, where every caller may by declaration (§5, "A route's stance").

**§16, "Declaring a scope and a scoped table".** In the example's `authorize` doc comment, replace "`who` is `None` for an unauthenticated request: an `authorize` that admits `None` has declared anonymous access to that scope." with "A caller with no credential never reaches it: `tx.scope` answers that caller 401, unless the route declared `public(Org)` (§5), and then it admits without asking."

**§16, a new subsection after "Declaring a scope and a scoped table":**

````markdown
### Who reaches a tenant: membership or declaration

A tenant is entered one of two ways, and never on an anonymous caller's word:

- **By membership.** On an `auth` or `public` route, `tx.scope` runs your `authorize`
  about the caller's principal. A caller with no credential gets the 401 from §5, not a
  call to `authorize` with nobody in it.
- **By declaration.** `public(Org)` on the route says every caller may act inside the org
  the handler opens. `tx.scope::<Org>` then admits without asking. The operator path in
  "Where the guarantee stops" below is the other declared way in.

```rust
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::wire::WireU64;
use charpente::{Id, Scoped, Tenant, Tx, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, who: Option<&Principal>, scope: Id<Org>) -> Result<(), ApiError> {
        match who {
            Some(p) if p.subject == format!("operator:{}", scope.raw()) => Ok(()),
            _ => Err(ApiError::BadRequest(format!(
                "not an operator of org {}",
                scope.raw()
            ))),
        }
    }
}

#[table(scoped_by = Org)]
struct Incident {
    id: Id<Incident>,
    title: String,
}

/// Operators only: `Org::authorize` runs once, about the caller.
#[route(POST "/orgs/:org/incidents/:id", auth)]
fn open_incident(
    tx: &mut Tx,
    org: Id<Org>,
    id: Id<Incident>,
    title: String,
    ctx: &Ctx,
) -> Result<WireU64, ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.put(&Incident { id, title });
    Ok(WireU64(id.raw()))
}

/// The public status page: every caller, with a credential or without,
/// reads inside the org it names, and no `authorize` runs.
#[route(GET "/orgs/:org/status", public(Org))]
fn status_page(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(WireU64(tx.scan::<Incident>()?.len() as u64))
}
```

Flip `open_incident` to plain `public` and it still refuses every anonymous write: the
scope line is rule 3, whatever `authorize` would have said about nobody.
````

**`DESIGN.md` §3, the Routes bullet.**
- Its first line's syntax becomes `#[route(METHOD "/path/:param", auth | public | public(Org)[, cross_tenant])]`.
- Replace its last sentence, "A route declares exactly one of `auth`/`public`; neither is a compile error naming both.", with:

  > A route declares exactly one of `auth`, `public` or `public(Org)`; none is a compile error naming all three. `public(Org)` declares that every caller, credentialed or not, may act inside the `Org` tenant the handler opens, with no `authorize` (spec 2026-09-27-job-tenancy §4). On any other route, a scope line reached with no credential is a named 401, so `authorize` is only ever asked about a real principal.

**`DESIGN.md` §8, the Multi-tenancy bullet.** After "…which runs the app's own `Tenant::authorize` first, and whose refusal is the app's own `ApiError`.", insert:

> A tenant is entered by membership (a principal, through `authorize`) or by declaration (`public(Org)` on the route, or the operator's `TenantAdmission`), never by an anonymous `None`. A caller with no credential at a scope line its route did not declare `public(Org)` for gets a named 401 (spec 2026-09-27-job-tenancy §4).

**`README.md`, the "since 2026-09-16" sentence (`:142-143`).** "a route says `auth` or `public`, a table says `scoped_by` or `global`, and neither is a default." becomes "a route says `auth`, `public` or `public(Org)`, a table says `scoped_by` or `global`, and neither is a default." Task 6 extends the same sentence to jobs.

**`crates/guide-examples/tests/doc_truth.rs`.** `const PINNED_FLOOR: usize = 22;` (was 21): §5 gained the rule-3 block.

Check the docs build and gates:

```zsh
cargo build -p guide-examples
cargo test -p guide-examples
```

Expected: both green. If `doc_truth` names a missing fragment in the 401 block, the test literal in oracle 3 and the guide disagree. Fix the guide to the literal, never the reverse.

- [ ] **Step 9: Gate and commit**

Run the gate and count it (87 of 87). Re-check `git status` and `git log -1` first. Then:

```zsh
git add crates/charpente/src/http/router.rs crates/charpente/src/http/mod.rs crates/charpente/src/http/app.rs \
  crates/charpente/src/tenant.rs crates/charpente/src/serve.rs crates/charpente-core/src/db.rs \
  crates/charpente-macros/src/route.rs \
  crates/charpente/tests/tenant.rs crates/charpente/tests/kernel.rs crates/charpente/tests/realtime.rs \
  crates/charpente-macros/tests/compile_pass/route_public_scope.rs \
  crates/charpente-macros/tests/compile_fail/route_public_scope_arity.rs \
  crates/charpente-macros/tests/compile_fail/route_public_scope_arity.stderr \
  crates/charpente-macros/tests/compile_fail/route_public_scope_missing_tenant_scope.rs \
  crates/charpente-macros/tests/compile_fail/route_public_scope_missing_tenant_scope.stderr \
  crates/charpente-macros/tests/compile_fail/route_public_scope_undeclared_capability.rs \
  crates/charpente-macros/tests/compile_fail/route_public_scope_undeclared_capability.stderr \
  crates/charpente-macros/tests/compile_fail/helpdesk_route_neither_auth_nor_public.stderr \
  crates/charpente-macros/tests/compile_fail/route_unknown_flag.stderr \
  crates/charpente-macros/tests/compile_fail/route_missing_method.stderr \
  crates/guide-examples/tests/doc_truth.rs docs/GUIDE.md DESIGN.md README.md
git commit -m "feat(job-tenancy): route stances — public(Org) admits by declaration, a credential-free scope line is a named 401; statuspage I-1 pinned"
```

---

### Task 4: `authorize` takes `&Principal`, and `Principal` cannot be forged

**Files:**
- Modify `crates/charpente-core/src/db.rs`:
  - `Tenant::authorize(tx, who: &Self::Who, scope)` and its doc;
  - `Tx::open_scope(scope, who: &S::Who)` and its doc.
- Modify `crates/charpente/src/auth/ladder.rs`: `Principal`'s fields become `pub(crate)`; add `subject()` and `via()`.
- Modify `crates/charpente/src/tenant.rs`:
  - rule 2 passes the principal;
  - `TenantScope`'s note (two fragments) and its `who` paragraph.
- Migrate every `impl Tenant` to the new signature:
  - Macro fixtures. Compile-fail: `scoped_read_from_bare_tx`, `scoped_tx_wrong_scope_type` (two impls), `cross_tenant_token_cannot_be_forged`, `scoped_seam_from_bare_tx`, `across_tenants_without_capability`, `unscoped_row_impl_from_an_app`, `no_tenant_impl_from_an_app`, `tenant_admission_cannot_be_forged` (Task 2), `route_public_scope_undeclared_capability` (Task 3). Compile-pass: `scoped_read_through_scoped_tx`, `route_public_cross_tenant`, `job_cross_tenant_capability`, `route_cross_tenant_capability`, `table_scoped_by_with_tenant_scope_impl`, `route_public_scope` (Task 3).
  - `crates/charpente/tests/`: `tenant.rs` (`Org`, `Firm`, `Team`, `statuspage::Org`), `kernel.rs:1382`, `realtime.rs:4604`, `describe.rs:1114`, `typed_tables.rs:736`, `archive.rs:64` (`Who = Principal`), `archive.rs:7357` (`ConvOrg`, `Who = str`), `replication.rs:1187` (`SeedOrg`, `Who = str`).
  - `crates/charpente-core/tests/engine.rs`: `TestOrg`, `AuditOrg`, `AdmitOrg` (Task 2).
  - `examples/invoicing/src/bin/tenant_node.rs:64-79`.
  - `docs/GUIDE.md`: the four compiled blocks with an `impl Tenant` (§16 "Declaring a scope", §16 "Who reaches a tenant" from Task 3, §16 "Reading across every tenant", §16 "Where the guarantee stops" from Task 2).
- Migrate the `open_scope` call sites:
  - `tenant.rs`'s six operator sites (450, 524, 580, 684, 988, 1046) move to `admit_scope` (spec §7, amendment A2);
  - `archive.rs:8261` and `replication.rs:1229` pass `"operator"`;
  - `engine.rs`'s sixteen pass `"member"`, or `"stranger"` where they passed `Some("stranger")` (amendment A6).
- Migrate the `Principal` field readers:
  - `crates/charpente/tests/{auth.rs:80,kernel.rs:2088,http.rs:64}`;
  - `tenant.rs` (`Org::authorize`, `statuspage::Org::authorize`);
  - `tenant_node.rs:69`;
  - GUIDE §5 (`:887-891` prose, `:966`, `:979`, `:1034` prose).
- Create in `crates/charpente-macros/tests/compile_fail/`: `open_scope_without_a_principal.rs` and `principal_cannot_be_forged.rs`, with their `.stderr`.
- Re-pin in `compile_fail/`: `table_scoped_by_missing_tenant_scope.stderr` and `route_public_scope_missing_tenant_scope.stderr` (the note's text).
- Docs:
  - `docs/GUIDE.md` §5 and §16;
  - `DESIGN.md` §3's **Scope** bullet;
  - `docs/superpowers/specs/2026-09-16-default-secure-surfaces-design.md` §2 and §8 get dated "superseded" lines.

**Interfaces:**
- Consumes:
  - Task 2's `TenantAdmission`, `Group::tenant_admission`, `Tx::admit_scope`;
  - Task 3's `Scoped::scope` (rule 2 is the line that changes here).
- Produces (spec §3):
  - `trait Tenant { fn authorize(tx: &Tx<'_>, who: &Self::Who, scope: Id<Self>) -> Result<(), Self::Error>; }`
  - `Tx::open_scope<S: Tenant>(&mut self, scope: Id<S>, who: &S::Who) -> Result<ScopedTx<'_, 'a, S>, S::Error>`
  - `impl Principal { pub fn subject(&self) -> &str; pub fn via(&self) -> Via; }`. The fields `subject` and `via` are `pub(crate)`.
  - `TenantScope`'s note reads "a tenant scope needs the app's own membership check; add:", with the line `fn authorize(tx: &Tx, who: &Principal, scope: Id<Self>) -> Result<(), ApiError> { … }`. Task 6's `#[job(scoped_by = …)]` const assert gets the same diagnostic.

- [ ] **Step 1: Write the two seals, and see them compile at the Task 3 commit**

Create `crates/charpente-macros/tests/compile_fail/principal_cannot_be_forged.rs`:

```rust
// The seal on `who` (job-tenancy spec §2 R2, §3). `Principal`'s fields are
// `pub(crate)`, and every construction lives inside `charpente`: the
// resolution ladder's three rungs and `build_ctx`. So a handler cannot build
// a principal the ladder never produced and hand it to `open_scope` or
// `authorize`. The ladder's output is the only principal there is.
use charpente::auth::{Principal, Via};
use charpente::http::ApiError;
use charpente::{Tx, route};

#[route(GET "/whoami", public)]
fn whoami(tx: &mut Tx) -> Result<String, ApiError> {
    let _ = tx;
    let forged = Principal {
        subject: "operator:1".to_string(),
        via: Via::Oidc,
    };
    Ok(format!("{forged:?}"))
}

fn main() {}
```

Create `crates/charpente-macros/tests/compile_fail/open_scope_without_a_principal.rs` with the Task 3 signature, which Step 3 changes:

```rust
// C-2 edit (b), from the statuspage review (job-tenancy spec, Context): an
// `auth` write route rewritten to open its tenant with
// `open_scope(org, None)` instead of `tx.scope::<Org>(org, ctx)?`. Under
// `Option<&Principal>` that compiled. It asked `authorize` about nobody, and
// with an `authorize` that admitted `None` for its jobs' sake, one org's key
// wrote another org's incidents while the sim batch stayed byte-identical.
// `authorize` now takes `&Principal`, so there is no nobody to pass.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::wire::WireU64;
use charpente::{Id, Tenant, Tx, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: Option<&Principal>, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[table(scoped_by = Org)]
struct Incident {
    id: Id<Incident>,
    title: String,
}

#[route(POST "/orgs/:org/incidents/:id/resolve", auth)]
fn resolve_incident(
    tx: &mut Tx,
    org: Id<Org>,
    id: Id<Incident>,
    ctx: &Ctx,
) -> Result<WireU64, ApiError> {
    let _ = ctx;
    let mut tx = tx.open_scope::<Org>(org, None)?;
    tx.delete::<Incident>(id);
    Ok(WireU64(id.raw()))
}

fn main() {}
```

Run:

```zsh
cargo test -p charpente-macros --test trybuild 2>&1 | grep -E "principal_cannot_be_forged|open_scope_without_a_principal|Expected test case to fail"
```

Expected — the red for both seals, quoted in the task report: both files are reported as compiling when they should not ("Expected test case to fail to compile, but it succeeded" or trybuild's equivalent wording). Today a handler can forge a `Principal`, and can pass `None`.

- [ ] **Step 2: Change the core signature and the principal**

`crates/charpente-core/src/db.rs`, the `Tenant` trait's method:

```rust
    /// Asked only about a real `who`: there is no `None` to pass
    /// (job-tenancy spec §3). A way into a scope with no one to ask about is
    /// declared instead: `Tx::admit_scope`, whose `TenantAdmission` only a
    /// `Group` mints.
    fn authorize(tx: &Tx<'_>, who: &Self::Who, scope: Id<Self>) -> Result<(), Self::Error>;
```

In the trait's doc comment, change "The platform calls `authorize` in exactly one place — `Tx::open_scope`, below —" to "The platform calls `authorize` in exactly one place, `Tx::open_scope` below, and only with a principal —".

`open_scope`:
- its parameter becomes `who: &S::Who`;
- its body is unchanged (`self.enter_scope(scope, |tx| S::authorize(tx, who, scope))`);
- delete its doc paragraph beginning "`who` is `None` for a direct `open_scope(scope, None)` call" and ending "the app's own policy.". Put in its place:

```rust
    /// `who` is whoever the app is asking about: `tx.scope` passes the
    /// request's principal, and nothing else can. A caller with no principal
    /// has no business here. It enters by a declaration (`admit_scope`), or
    /// not at all.
```

`crates/charpente/src/auth/ladder.rs`:

```rust
/// The authenticated identity for a request. `subject` is whatever
/// `create_session`/`create_api_key` was called with, or an OIDC token's
/// `sub` claim; `via` names which ladder rung produced it.
///
/// The fields are `pub(crate)`, so every `Principal` is one the resolution
/// ladder produced (job-tenancy spec §2 R2). This is what makes
/// `Tenant::authorize`'s `who` unforgeable: a handler reads a principal and
/// never builds one.
#[derive(Debug, Clone)]
pub struct Principal {
    pub(crate) subject: String,
    pub(crate) via: Via,
}

impl Principal {
    /// The subject the credential was minted for, or the OIDC `sub` claim.
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// The ladder rung that authenticated this request.
    pub fn via(&self) -> Via {
        self.via
    }
}
```

`crates/charpente/src/tenant.rs`:
- rule 2 becomes `Some(principal) => self.open_scope(scope, principal),`;
- in `Scoped::scope`'s doc, rule 2's line stays as it is;
- in `TenantScope`'s doc, replace Task 3's paragraph ("`who` is `None` only for a direct `Tx::open_scope(scope, None)` call. …") with:

```rust
/// `authorize` is asked only about a real principal: `tx.scope`'s rule 3
/// answers a caller with no credential with a 401, and a route declared
/// `public(S)` admits without asking (`Scoped::scope`).
```

In `#[diagnostic::on_unimplemented]`'s `note`, change two fragments only (spec §3):
- "a scoped table needs the app's own membership check; add:" becomes "a tenant scope needs the app's own membership check; add:";
- `fn authorize(tx: &Tx, who: Option<&Principal>, scope: Id<Self>)` becomes `fn authorize(tx: &Tx, who: &Principal, scope: Id<Self>)`.

The opening changes because the diagnostic now fires for `public(Foo)` (Task 3) and `#[job(scoped_by = Foo)]` (Task 6), not only for a table.

- [ ] **Step 3: Migrate every site the compiler names**

**`impl Tenant` signatures.** Every `who: Option<&Principal>` / `_who: Option<&Principal>` becomes `who: &Principal` / `_who: &Principal`, and every `Option<&str>` becomes `&str`, at each impl listed under **Files**. Bodies that read `who` change as follows:

- `engine.rs` `TestOrg`:

  ```rust
      fn authorize(_tx: &Tx, who: &str, _scope: Id<TestOrg>) -> Result<(), io::Error> {
          match who {
              "stranger" => Err(io::Error::other("not a member of this org")),
              _ => Ok(()),
          }
      }
  ```

- `tenant.rs` `Org::authorize`'s counter line becomes `AUTHORIZE_CALLS.with(|calls| calls.borrow_mut().push(who.subject().to_string()));`. In `AUTHORIZE_CALLS`' doc, delete "— `"<nobody>"` for a call with no principal".
- `tenant.rs` `statuspage::Org::authorize`:

  ```rust
          fn authorize(_tx: &Tx, who: &Principal, scope: Id<Org>) -> Result<(), ApiError> {
              if who.subject() == format!("operator:{}", scope.raw()) {
                  Ok(())
              } else {
                  Err(ApiError::BadRequest(format!(
                      "{} is not an operator of org {}",
                      who.subject(),
                      scope.raw()
                  )))
              }
          }
  ```

  In the module's doc comment, after "and the store is unchanged.", add: "The `None` arm that made I-1 possible is gone with the `Option` (spec §3): the platform never asks about nobody."
- `tenant_node.rs`:

  ```rust
      fn authorize(_tx: &Tx, who: &Principal, scope: Id<Org>) -> Result<(), ApiError> {
          let subject = who.subject();
          if subject == format!("org-{}", scope.raw()) {
  ```

  The rest of its body is unchanged.
- GUIDE §16 "Declaring a scope", the example's `authorize` and its doc comment:

  ```rust
      /// Runs once per `tx.scope::<Org>(…)` that has a principal to ask about,
      /// and it is the app's own policy — the platform never derives a tenant
      /// from a subject, a header or a row field. It is only ever asked about a
      /// real principal: a caller with no credential gets §5's 401, and a
      /// `public(Org)` route admits without asking.
      fn authorize(_tx: &Tx, who: &Principal, scope: Id<Org>) -> Result<(), ApiError> {
          if who.subject() == format!("org:{}", scope.raw()) {
              Ok(())
          } else {
              Err(ApiError::BadRequest(format!(
                  "not a member of org {}",
                  scope.raw()
              )))
          }
      }
  ```

- GUIDE §16 "Who reaches a tenant" (Task 3's block):

  ```rust
      fn authorize(_tx: &Tx, who: &Principal, scope: Id<Org>) -> Result<(), ApiError> {
          if who.subject() == format!("operator:{}", scope.raw()) {
              Ok(())
          } else {
              Err(ApiError::BadRequest(format!(
                  "not an operator of org {}",
                  scope.raw()
              )))
          }
      }
  ```

**`Principal` field readers.** `ctx.principal().expect("auth route").subject.clone()` becomes `ctx.principal().expect("auth route").subject().to_string()` at `auth.rs:80`, `kernel.rs:2088`, `http.rs:64`, and GUIDE §5's `mint_key` (`:979`). GUIDE §5's `whoami` (`:962-967`) becomes:

```rust
    Ok(ctx
        .principal()
        .expect("auth routes always have a principal")
        .subject()
        .to_string())
```

**Core's `open_scope` calls in `engine.rs`.** Each `open_scope(…, None)` becomes `open_scope(…, "member")`, and the one `open_scope(…, Some("stranger"))` (~2598) becomes `open_scope(…, "stranger")`. That is sixteen sites: lines 1967, 2010, 2416, 2428, 2449, 2525, 2534, 2546, 2644, 2650, 2695, 2697, 2734, 2739, the stranger site at 2598, and Task 2's `an_opened_then_admitted_second_tenant_is_the_same_violation`.

**`Who = str` operator sites.** `archive.rs:8261` becomes `tx.open_scope(Id::<ConvOrg>::from_raw(tenant), "operator")?`, and `replication.rs:1229` becomes `tx.open_scope(Id::<SeedOrg>::from_raw(tenant), "operator")?` (amendment A2).

**`tenant.rs`'s operator sites move to the admission (spec §7).** After `two_tenant_db`, add:

```rust
/// `Db::transact`, with the operator's admission (job-tenancy spec §7): the
/// same begin_group -> stage -> commit sequence and the same commit rule,
/// line for line. So the swarm writes the bytes it wrote when these sites
/// said `open_scope(…, None)`, and Task 1's coverage baseline measures that.
fn transact_admitted<T, E>(
    db: &mut Db,
    f: impl FnOnce(&mut Tx, &TenantAdmission) -> Result<T, E>,
) -> Result<T, TxError<E>> {
    let mut g = db.begin_group().map_err(TxError::Io)?;
    let admission = g.tenant_admission();
    let out = g.stage(|tx| f(tx, &admission))?;
    let staged = !g.is_empty();
    if let Err(failure) = g.commit() {
        let reported_elsewhere = db.poisoned() || db.store_failed();
        if failure.invalidates(0, staged) || !reported_elsewhere {
            return Err(TxError::Io(failure.error));
        }
    }
    Ok(out)
}
```

Add `TenantAdmission` to the `charpente` import. Then:

- `oracle` (~450):

  ```rust
          let verdict = transact_admitted(db, |tx, admission| -> Result<usize, String> {
              bounded_reads(&tx.admit_scope(Id::<Firm>::from_raw(tenant), admission), tenant)
          });
  ```

- the two-tenant batch (~513–530): after `let mut g = match db.begin_group() { … };`, add `let admission = g.tenant_admission();`. The staged closure becomes:

  ```rust
                      match g.stage(|tx| -> Result<(), String> {
                          tx.admit_scope(Id::<Firm>::from_raw(tenant), &admission)
                              .put(&ledger_row(tenant, id));
                          Ok(())
                      }) {
  ```

- the ordinary path (~579):

  ```rust
                  match transact_admitted(&mut db, |tx, admission| -> Result<(), String> {
                      tx.admit_scope(Id::<Firm>::from_raw(tenant), admission)
                          .put(&ledger_row(tenant, id));
                      Ok(())
                  }) {
  ```

- `two_tenant_db` (~683):

  ```rust
              transact_admitted(&mut db, |tx, admission| -> Result<(), String> {
                  tx.admit_scope(Id::<Firm>::from_raw(tenant), admission)
                      .put(&ledger_row(tenant, id));
                  Ok(())
              })
              .unwrap();
  ```

- `a_cross_tenant_read_sees_staged_writes_and_tombstones_per_tenant` (~983–990): after `let cap = group.cross_tenant_cap();`, add `let admission = group.tenant_admission();`. The scoped line becomes `let mut scoped = tx.admit_scope(Id::<Team>::from_raw(2), &admission);`. Its comment "The operator path to the token" becomes "The operator path to both tokens".
- `tenant_zero_is_a_real_tenant_to_a_cross_tenant_read` (~1041–1046): the same two edits, with `Id::<Team>::from_raw(0)`.

`Firm::authorize` and `Team::authorize` are no longer called by anything in the file; they remain because `TenantScope` requires them. Add a doc line to `Firm`'s impl: `/// Never called: the swarm is operator code, and opens its tenants with an admission (spec §7).`

**The seal fixture.** In `open_scope_without_a_principal.rs`, the `impl Tenant`'s signature becomes `_who: &Principal`. The handler is unchanged.

Then run until clean:

```zsh
cargo check --workspace --all-targets 2>&1 | grep -E "^(error|warning)" | sort | uniq -c
```

Expected: no output. Then prove the sweep is complete, with every non-comment mention of the old form gone:

```zsh
grep -rnE "who: Option<|open_scope\(.*(None|Some\()" crates examples docs/GUIDE.md README.md --include='*.rs' --include='*.md' | grep -vE ':[0-9]+:\s*//'
```

Expected: exactly one line, `open_scope_without_a_principal.rs`'s `tx.open_scope::<Org>(org, None)?`, which is the point of that fixture.

- [ ] **Step 4: Run, and review the pins**

```zsh
cargo test -p charpente-core --test engine
cargo test -p charpente --test tenant
cargo test -p charpente --test auth
cargo test -p charpente --test http
cargo test -p charpente --test typed_tables
cargo test -p charpente --test describe
TRYBUILD=overwrite cargo test -p charpente-macros --test trybuild
git diff --stat crates/charpente-macros/tests/compile_fail/
cat crates/charpente-macros/tests/compile_fail/principal_cannot_be_forged.stderr crates/charpente-macros/tests/compile_fail/open_scope_without_a_principal.stderr
git diff crates/charpente-macros/tests/compile_fail/
cargo test -p charpente-macros --test trybuild
cargo build -p guide-examples
cargo build -p invoicing --bins
```

Expected:
- every suite passes;
- `principal_cannot_be_forged.stderr` is `error[E0451]`, naming the private fields `subject` and `via` of `Principal`, at the struct literal;
- `open_scope_without_a_principal.stderr` is `error[E0308]: mismatched types` at `None`, "expected `&Principal`, found `Option<_>`";
- the changed pins are these two new files plus `table_scoped_by_missing_tenant_scope.stderr` and `route_public_scope_missing_tenant_scope.stderr`, whose note now reads "a tenant scope needs the app's own membership check; add:" and `fn authorize(tx: &Tx, who: &Principal, scope: Id<Self>) -> Result<(), ApiError> { … }`.

  Line numbers in fixtures whose `authorize` signature shrank from a wrapped form may shift. Read every other changed `.stderr` and confirm the only change is a line number.

`engine.rs`'s `a_refused_scope_hands_back_the_apps_own_error_and_no_handle` still passes: `"stranger"` is still refused. That test is the core half of the refusal pin, and it must not be edited beyond its argument.

- [ ] **Step 5: Measure byte identity**

```zsh
D=.superpowers/sdd/2026-09-27-job-tenancy
cargo test -p charpente --test tenant -- every_scoped_read_returns_only_its_own_tenants_rows --exact --nocapture 2>&1 | tee $D/tenant-swarm-task4.log | grep "^test result"
grep -E "^charpente-sim|^  [^ ]" $D/tenant-swarm-task4.log > $D/tenant-coverage-task4.txt
diff $D/tenant-coverage-base.txt $D/tenant-coverage-task4.txt && echo "tenant: identical"
cargo test -p charpente --test tenant -- the_tenant_workload_replays_identically_for_the_same_seed --exact
```

Expected: `tenant: identical`, and the replay test passes. `transact_admitted` is `transact`'s own sequence, and `admit_scope` runs nothing that `open_scope` ran with an always-`Ok` `authorize`. A moved line is a finding.

- [ ] **Step 6: Docs**

**`docs/GUIDE.md` §5, "Auth: sessions, API keys, and the resolution ladder".** The first `ctx.principal()` bullet becomes:

> - `ctx.principal() -> Option<&Principal>` — the authenticated identity, if any. `principal.subject()` is the subject the credential was minted for, and `principal.via()` names which rung authenticated it (`Via::Session(id)`, `Via::ApiKey(id)`, or `Via::Oidc`). Read the subject from here on any `auth` route, whatever the credential kind. A `Principal` is read-only to your code: its fields are private, so the only principals are the ones the ladder produced, and `Tenant::authorize` can trust the one it is handed.

**§5, "OIDC (static JWKS)".** "A verified token yields a `Principal` with `via: Via::Oidc` and `subject` = the token's `sub` claim" becomes "A verified token yields a `Principal` whose `via()` is `Via::Oidc` and whose `subject()` is the token's `sub` claim".

**§16, "The compile errors, so you recognize them".** The second rendered block's note becomes:

```text
  = note: a tenant scope needs the app's own membership check; add:
          impl Tenant for Org {
              type Who = Principal;
              type Error = ApiError;
              fn authorize(tx: &Tx, who: &Principal, scope: Id<Self>) -> Result<(), ApiError> { … }
          }
```

**§16, "Reading across every tenant"** and **§16, "Where the guarantee stops"** (Task 2's `seed()` block): each `impl Tenant`'s `_who: Option<&Principal>` becomes `_who: &Principal`.

**`DESIGN.md` §3, the Scope bullet.** After "…which runs the app's `authorize` before it returns the `ScopedTx` that `get`/`scan`/`scan_from`/`scan_by`/`put`/`delete` live on.", insert:

> `authorize(tx, who: &Principal, scope)` is asked only about a principal the resolution ladder produced (`Principal`'s fields are private): a caller with no credential is a 401 at the scope line, and the declared ways in, `public(Org)` and the operator's `TenantAdmission`, ask nobody (spec 2026-09-27-job-tenancy §3).

**`docs/superpowers/specs/2026-09-16-default-secure-surfaces-design.md`.** After §2's **Semantics** paragraph, add:

> **Superseded 2026-09-2x** by `2026-09-27-job-tenancy-design.md` §3–§4: `authorize` takes `&Principal`, so there is no `None` to admit. A caller with no credential at a scope line gets a named 401, unless the route declares `public(Org)`.

After the §8 item "**Any change to `Tenant::authorize`'s `Option<&Principal>`.**", add:

> **Superseded 2026-09-2x** by `2026-09-27-job-tenancy-design.md` §2 R2: the parameter is `&Principal`, and `Principal`'s fields are `pub(crate)`.

Then:

```zsh
cargo build -p guide-examples
cargo test -p guide-examples
```

Expected: both green.

- [ ] **Step 7: Gate and commit**

Run the gate and count it (87 of 87). Re-check `git status` and `git log -1` first. `git status --short` lists exactly the files this task names; stage those by path:

```zsh
git status --short
git add crates/charpente-core/src/db.rs crates/charpente/src/auth/ladder.rs crates/charpente/src/tenant.rs \
  crates/charpente-core/tests/engine.rs \
  crates/charpente/tests/tenant.rs crates/charpente/tests/kernel.rs crates/charpente/tests/realtime.rs \
  crates/charpente/tests/describe.rs crates/charpente/tests/typed_tables.rs crates/charpente/tests/archive.rs \
  crates/charpente/tests/replication.rs crates/charpente/tests/auth.rs crates/charpente/tests/http.rs \
  examples/invoicing/src/bin/tenant_node.rs \
  crates/charpente-macros/tests/compile_fail crates/charpente-macros/tests/compile_pass \
  docs/GUIDE.md DESIGN.md docs/superpowers/specs/2026-09-16-default-secure-surfaces-design.md
git commit -m "feat(job-tenancy): authorize takes &Principal and Principal's fields are pub(crate) — nothing asks about nobody, and nobody forges a principal"
```

Staging the two fixture directories by name is deliberate: every file this task touches there is listed above. Confirm that `git diff --cached --stat` names no fixture outside that list.

---

### Task 5: The tenant rides the record — tag `0x0a`, `JobRow.scope`, protocol 4

**Files:**
- Modify `crates/charpente-core/src/db.rs`:
  - the module doc's record-format list;
  - `struct Enqueue` (new), replacing `TxEffects.enqueues`' tuple;
  - `Effect::Enqueue.scope`;
  - `encode_record` / `decode_record`;
  - the apply arm (~1786);
  - the stage-time shape check (~3366);
  - `Tx::enqueue` / `Tx::enqueue_keyed`;
  - the unit tests that push enqueues (~3747, 3864, 3870, 3929) and `decode_refuses_unknown_tags` (~4123).
- Modify `crates/charpente-core/src/job.rs`: `JobRow.scope`, its codec, and its tests.
- Modify `crates/charpente-core/src/table.rs`: `materialize_job(…, scope)` (~1239).
- Modify `crates/charpente/src/repl/mod.rs`: `PROTO_VERSION` 3 → 4.
- Modify the `JobRow` literals at `crates/charpente-core/tests/engine.rs` 494 and 1148, and the `materialize_job` calls at `crates/charpente/tests/typed_tables.rs` 624, 625 and 631.
- Test:
  - `crates/charpente-core/src/db.rs` (unit);
  - `crates/charpente-core/src/job.rs` (unit);
  - `crates/charpente-core/tests/materialize.rs`;
  - `crates/charpente/tests/replication.rs` (~2735).
- Docs: `DESIGN.md` §4's **Record payload** paragraph (~426–447) and §5's `PROTO_VERSION` sentence (~1010).

**Interfaces:**
- Consumes: nothing from Tasks 2–4. This task is the record format alone, and nothing writes a scoped enqueue until Task 6.
- Produces (spec §6):
  - `struct Enqueue { job: &'static str, payload: Vec<u8>, key: Option<String>, shape: u64, scope: Option<u64> }` — private to `db.rs`. A 5-tuple trips clippy's `type_complexity`.
  - The record tag `0x0a [job][payload][key: Option<String>][shape: u64 le][scope: u64 le]`, written exactly when `Enqueue.scope` is `Some`, including `Some(0)`.
  - `Effect::Enqueue { job, payload, key, shape: Option<u64>, scope: Option<u64> }`. Tags `0x02` and `0x08` decode with `scope: None`.
  - `pub scope: Option<u64>` on `JobRow`, the last field, encoded after `shape` and only when `Some`. Asserted: `scope.is_some()` implies `shape.is_some()`.
  - `Writer::materialize_job(&mut self, name: &str, payload: &[u8], key: Option<&str>, shape: Option<u64>, scope: Option<u64>) -> io::Result<u64>`.
  - `PROTO_VERSION = 4`.
  - Task 6 builds on `Enqueue.scope` (the enqueue family fills it) and `JobRow.scope` (the dispatcher reads it).

- [ ] **Step 1: Write the tests that are red at the Task 4 commit**

In `crates/charpente/tests/replication.rs`, after `hello_declaring_protocol_version_2_is_refused_by_name`:

```rust
/// PROTO_VERSION 4 (job tenancy, spec §6): a version-3 binary cannot read the
/// scoped enqueue tag `0x0a`, so it is refused at Hello, by name, before a
/// single frame ships — not at its first scoped job.
#[test]
fn hello_declaring_protocol_version_3_is_refused_by_name() {
    let sim = Sim::with_faults(0x2EB1_0006 ^ 3, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let reply = exchange_hello(
        &sim,
        &mut p,
        "p:1",
        Msg::Hello {
            proto: 3,
            listen: None,
            position: None,
            secret: None,
            schema_digest: 0,
        },
    )
    .unwrap();
    match reply {
        Msg::Refuse { message } => assert_eq!(
            message,
            "replication protocol version mismatch: this node speaks protocol version 4, but \
             the peer's Hello declared version 3 — upgrade the older binary so both speak the \
             expected version 4"
        ),
        other => panic!("expected Refuse, got {other:?}"),
    }
}
```

Run it at the Task 4 commit:

```zsh
cargo test -p charpente --test replication -- hello_declaring_protocol_version_3_is_refused_by_name --exact
```

Expected: FAIL, "expected Refuse, got …". A version-3 Hello is today's own version, so the primary does not refuse it. Quote the panic in the report.

The record and row tests below need the new fields to compile. Each is proved red by mutation in Step 5.

- [ ] **Step 2: The record**

In `crates/charpente-core/src/db.rs`, the module doc's tag list gains a line after the `0x09` line:

```rust
//! `0x0a [job][payload][key: Option<String>][shape: u64 le][scope: u64 le]`
//! scoped enqueue, for a `#[job(scoped_by = …)]` job (job-tenancy spec §6) —
```

"`0x0a` is the next free tag." becomes "`0x0b` is the next free tag." After the sentence ending "…never `0x00`/`0x01`/`0x04`, even at scope 0).", add:

```rust
//! A scoped enqueue is `0x0a` rather than a widened `0x08` for the same
//! reason, and the same presence-not-value rule chooses it: an enqueue
//! carrying `Some(0)` is `0x0a`.
```

Next to `TxEffects`, add:

```rust
/// One buffered enqueue. `shape` is bare because a shapeless enqueue cannot
/// be represented (payload-shapes amendment A2). `scope` is the tenant a
/// `#[job(scoped_by = …)]` job was enqueued in, `None` for a `global` job:
/// presence, not value, selects tag `0x0a`, so `Some(0)` is tenant 0
/// (job-tenancy spec §6).
struct Enqueue {
    job: &'static str,
    payload: Vec<u8>,
    key: Option<String>,
    shape: u64,
    scope: Option<u64>,
}
```

`TxEffects.enqueues` becomes `enqueues: Vec<Enqueue>,`. The comment above it now covers `emits` alone: "The emit shape is a bare `u64`, so a shapeless emit cannot be represented (payload-shapes amendment A2)."

`Effect::Enqueue` gains a field. Its comment and body become:

```rust
    // `shape`: `None` for the pre-shape tags `0x02`/`0x03`, `Some` for
    // `0x08`/`0x09`/`0x0a` — the wire presence, like `scope` above. `None` is
    // foreign to every check downstream (payload-shapes spec §2). `scope`:
    // `Some` exactly for `0x0a`, the scoped enqueue (job-tenancy spec §6).
    Enqueue {
        job: String,
        payload: Vec<u8>,
        key: Option<String>,
        shape: Option<u64>,
        scope: Option<u64>,
    },
```

In `encode_record`, the enqueue loop becomes:

```rust
    for Enqueue {
        job,
        payload,
        key,
        shape,
        scope,
    } in &effects.enqueues
    {
        out.push(if scope.is_some() { 10 } else { 8 });
        encode_bytes(job.as_bytes(), &mut out);
        encode_bytes(payload, &mut out);
        key.encode(&mut out);
        shape.encode(&mut out);
        if let Some(scope) = scope {
            scope.encode(&mut out);
        }
    }
```

In `decode_record`:
- arms `2` and `8` gain `scope: None,`;
- add after arm `9`:

```rust
            10 => Effect::Enqueue {
                job: String::decode(input)?,
                payload: Vec::<u8>::decode(input)?,
                key: Option::<String>::decode(input)?,
                shape: Some(u64::decode(input)?),
                scope: Some(u64::decode(input)?),
            },
```

The apply arm (~1786) becomes:

```rust
                    Effect::Enqueue {
                        job,
                        payload,
                        key,
                        shape,
                        scope,
                    } => {
                        new_ids.push(w.materialize_job(
                            job,
                            payload,
                            key.as_deref(),
                            *shape,
                            *scope,
                        )?);
                    }
```

The stage-time shape check (~3366) becomes `for Enqueue { job: name, shape, .. } in &effects.enqueues {`, with its body unchanged.

`Tx::enqueue` and `Tx::enqueue_keyed` push the struct:

```rust
        self.effects.enqueues.push(Enqueue {
            job: J::NAME,
            payload,
            key: None, // `Some(key)` in enqueue_keyed
            shape: J::FINGERPRINT,
            scope: None,
        });
```

The unit tests change as follows:
- Each `effects.enqueues.push(("name", payload, key, shape))` becomes `effects.enqueues.push(Enqueue { job: "name", payload, key, shape, scope: None })`. That covers `effects_fixture`, `keyed_enqueue_roundtrips` and `shaped_enqueue_and_emit_bytes_are_pinned`.
- Each expected `Effect::Enqueue { … }` literal gains `scope: None`. That covers `record_roundtrip_is_canonical`, `keyed_enqueue_roundtrips`, `a_pre_shape_enqueue_and_emit_decode_with_no_shape` and `shaped_enqueue_and_emit_bytes_are_pinned`.

`shaped_enqueue_and_emit_bytes_are_pinned`'s literal bytes do not change. That is the `global` half of byte identity, pinned.

`decode_refuses_unknown_tags` moves to the next free tag:

```rust
    /// `0x0b`, the next free tag: `0x0a` is the scoped enqueue.
    #[test]
    fn decode_refuses_unknown_tags() {
        let mut out = Vec::new();
        1u32.encode(&mut out);
        out.push(11);
        let err = decode_record(&out).unwrap_err();
        assert!(err.to_string().contains("unknown effect tag 11"), "{err}");
    }
```

Add after `shaped_enqueue_and_emit_bytes_are_pinned`:

```rust
    /// Job-tenancy spec §6 and Review Focus 2: the scoped enqueue, pinned as
    /// literal bytes. It is the `0x08` layout under tag `0x0a`, plus the
    /// tenant as eight little-endian bytes, at tenant 0, because presence
    /// and not value selects the tag. An unscoped enqueue in the same record
    /// is still `0x08`, byte for byte, with nothing after its shape.
    #[test]
    fn scoped_enqueue_bytes_are_pinned_with_tenant_zero() {
        let mut effects = TxEffects::default();
        effects.enqueues.push(Enqueue {
            job: "ping",
            payload: vec![7],
            key: None,
            shape: 0x0102_0304_0506_0708,
            scope: Some(0),
        });
        effects.enqueues.push(Enqueue {
            job: "ping",
            payload: vec![9],
            key: None,
            shape: 0x0102_0304_0506_0708,
            scope: None,
        });
        assert_eq!(
            encode_record(&effects),
            vec![
                2, 0, 0, 0, // two effects
                10, 4, 0, 0, 0, b'p', b'i', b'n', b'g', 1, 0, 0, 0, 7, 0, // scoped enqueue
                8, 7, 6, 5, 4, 3, 2, 1, // its shape
                0, 0, 0, 0, 0, 0, 0, 0, // its tenant: 0, a real tenant
                8, 4, 0, 0, 0, b'p', b'i', b'n', b'g', 1, 0, 0, 0, 9, 0, // unscoped enqueue
                8, 7, 6, 5, 4, 3, 2, 1, // its shape, and nothing after it
            ]
        );
        assert_eq!(
            decode_record(&encode_record(&effects)).unwrap(),
            vec![
                Effect::Enqueue {
                    job: "ping".into(),
                    payload: vec![7],
                    key: None,
                    shape: Some(0x0102_0304_0506_0708),
                    scope: Some(0),
                },
                Effect::Enqueue {
                    job: "ping".into(),
                    payload: vec![9],
                    key: None,
                    shape: Some(0x0102_0304_0506_0708),
                    scope: None,
                },
            ]
        );
    }
```

- [ ] **Step 3: The row and its materialization**

In `crates/charpente-core/src/job.rs`, add a field after `shape`:

```rust
    /// The tenant a `#[job(scoped_by = …)]` job was enqueued in, from the
    /// record's `0x0a` tag. `None` for a `global` job (`0x08`) and for a row
    /// applied from a pre-shape record. Encoded after `shape`, and only when
    /// `Some`. A scoped row always has a shape, because only `0x0a` writes a
    /// scope, so every row written before this field existed decodes
    /// unchanged. `Some(0)` is tenant 0: presence, not value (job-tenancy
    /// spec §6).
    pub scope: Option<u64>,
```

`encode_row`'s tail becomes:

```rust
        if let Some(shape) = self.shape {
            shape.encode(out);
        }
        if let Some(scope) = self.scope {
            assert!(
                self.shape.is_some(),
                "JobRow {}: a tenant scope is encoded after the shape, so a scoped row must \
                 carry one — only tag 0x0a writes a scope, and 0x0a always carries the shape",
                self.id.raw()
            );
            scope.encode(out);
        }
```

`decode_row`'s tail becomes:

```rust
        let (shape, scope) = match input.len() {
            0 => (None, None),
            8 => (Some(u64::decode(input)?), None),
            16 => (Some(u64::decode(input)?), Some(u64::decode(input)?)),
            n => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "row decode: {n} bytes after a JobRow's last_error — the shape and the \
                         tenant scope after it are 8 bytes each, present or absent, so the \
                         stored row is corrupt"
                    ),
                ));
            }
        };
```

Its `Ok(JobRow { … })` gains `scope,`.

The tests change as follows:
- `job_row_roundtrips`, `a_pre_shape_job_row_decodes_with_no_shape` and `a_torn_job_row_shape_is_a_named_decode_error` gain `scope: None` in their literals.
- `job_row_roundtrips` gains `assert_eq!(back.scope, row.scope);`.
- The torn test's expected text becomes `"row decode: 3 bytes after a JobRow's last_error — the shape and the tenant scope after it are 8 bytes each, present or absent, so the stored row is corrupt"`.

Then add:

```rust
    /// Job-tenancy spec §6 and Review Focus 2: a scoped row carries its tenant
    /// after its shape, and tenant 0 round-trips as `Some(0)`, never as "no
    /// tenant". The unscoped row is the control: its bytes end at the shape.
    #[test]
    fn a_scoped_job_row_roundtrips_its_tenant_zero_included() {
        let row = |scope| JobRow {
            id: Id::from_raw(7),
            name: "file".into(),
            payload: vec![1, 2],
            key: None,
            status: JobStatus::Pending,
            attempts: 0,
            due_at: 0,
            last_error: None,
            shape: Some(0xFEED),
            scope,
        };
        let mut unscoped = Vec::new();
        row(None).encode_row(&mut unscoped);
        for scope in [Some(0), Some(7)] {
            let mut bytes = Vec::new();
            row(scope).encode_row(&mut bytes);
            assert_eq!(bytes.len(), unscoped.len() + 8, "the tenant is eight more bytes");
            assert_eq!(JobRow::decode_row(&bytes).unwrap().scope, scope);
        }
        assert_eq!(JobRow::decode_row(&unscoped).unwrap().scope, None);
    }

    /// A tear inside the tenant is named like a tear inside the shape.
    #[test]
    fn a_torn_job_row_scope_is_a_named_decode_error() {
        let row = JobRow {
            id: Id::from_raw(7),
            name: "file".into(),
            payload: vec![1, 2],
            key: None,
            status: JobStatus::Pending,
            attempts: 0,
            due_at: 0,
            last_error: None,
            shape: Some(0xFEED),
            scope: Some(3),
        };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        bytes.truncate(bytes.len() - 5);
        let err = JobRow::decode_row(&bytes)
            .err()
            .expect("11 bytes where a shape and a scope should be");
        assert_eq!(
            err.to_string(),
            "row decode: 11 bytes after a JobRow's last_error — the shape and the tenant scope \
             after it are 8 bytes each, present or absent, so the stored row is corrupt"
        );
    }

    /// Negative space (hard rule 4): the tail is positional, so a scope with
    /// no shape before it would decode as a shape. The encoder refuses to
    /// write one.
    #[test]
    #[should_panic(expected = "a scoped row must carry one")]
    fn a_scope_without_a_shape_is_refused_at_encode() {
        let row = JobRow {
            id: Id::from_raw(7),
            name: "file".into(),
            payload: vec![],
            key: None,
            status: JobStatus::Pending,
            attempts: 0,
            due_at: 0,
            last_error: None,
            shape: None,
            scope: Some(3),
        };
        row.encode_row(&mut Vec::new());
    }
```

In `crates/charpente-core/src/table.rs`, `materialize_job` gains a final parameter `scope: Option<u64>`, and its `JobRow` literal gains `scope,`. Its doc gains: "`scope` is the record's tenant for a scoped job (`0x0a`), carried into the row unchanged."

The remaining literals and calls:
- `engine.rs` 494 and 1148: add `scope: None,`.
- `typed_tables.rs` 624, 625 and 631: add a final `None` argument to each `materialize_job` call.

In `crates/charpente-core/tests/materialize.rs`, after `a_pre_shape_record_materializes_with_no_shape`:

```rust
/// Job-tenancy spec §6, Review Focus 2 and 4. A shipped record carrying the
/// scoped enqueue tag `0x0a`, applied the way a follower, a restore and a
/// replay all apply one, materializes a `JobRow` whose `scope` is the
/// record's. Tenant 0 comes through as `Some(0)`, and the `0x08` enqueue
/// beside it as `None`.
#[test]
fn a_shipped_scoped_enqueue_materializes_with_its_scope_tenant_zero_included() {
    let sim = Sim::with_faults(0x607, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    let mut record = Vec::new();
    3u32.encode(&mut record);
    for (payload, scope) in [(1u8, Some(0u64)), (2, Some(7)), (3, None)] {
        record.push(if scope.is_some() { 10 } else { 8 });
        "ping".to_string().encode(&mut record);
        vec![payload].encode(&mut record);
        None::<String>.encode(&mut record);
        0x5E4Du64.encode(&mut record);
        if let Some(scope) = scope {
            scope.encode(&mut record);
        }
    }
    let seq = db.last_applied().map_or(0, |s| s + 1);
    db.apply_shipped(&[(seq, record)]).unwrap();
    let rows = db.snapshot().unwrap().scan::<JobRow>().unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| (r.payload.clone(), r.shape, r.scope))
            .collect::<Vec<_>>(),
        vec![
            (vec![1], Some(0x5E4D), Some(0)),
            (vec![2], Some(0x5E4D), Some(7)),
            (vec![3], Some(0x5E4D), None),
        ],
        "each row carries its record's tenant; tenant 0 is Some(0), not None"
    );
}
```

- [ ] **Step 4: Protocol 4**

`crates/charpente/src/repl/mod.rs`:

```rust
/// The wire protocol version this binary speaks; carried in every `Hello`
/// and checked against the peer's (spec §5's version-mismatch surface).
/// Bumps whenever a message body's shape changes — tenant-scoping's
/// `ScopedSnapshotRows` (tag 14) made it 2; payload shapes' `0x08`/`0x09`
/// record tags made it 3; job tenancy's `0x0a` scoped enqueue makes it 4, so
/// a version-3 binary is refused at Hello rather than at its first scoped
/// job.
pub const PROTO_VERSION: u8 = 4;
```

In `crates/charpente/tests/replication.rs`, `hello_declaring_protocol_version_2_is_refused_by_name`'s expected text becomes "…this node speaks protocol version 4, but the peer's Hello declared version 2 — upgrade the older binary so both speak the expected version 4". Its doc comment gains: "(Protocol 4 since job tenancy; the version-3 test follows.)"

- [ ] **Step 5: Run, then prove red by mutation**

```zsh
cargo test -p charpente-core --lib
cargo test -p charpente-core --test materialize
cargo test -p charpente-core --test engine
cargo test -p charpente --test replication 2>&1 | tee .superpowers/sdd/2026-09-27-job-tenancy/task5-replication.log | grep "^test result"
cargo test -p charpente --test typed_tables
```

Expected: all green, including the version-3 Hello test. The replication suite's pinned seeds are part of spec §9 item 7's measurement. The bump changes one `Hello` byte's value, not its length, and Hello travels on the simulated network, never the disk. So every pinned seed must still reproduce its asserted reason. A pinned-seed failure is a finding.

Save the implementation, so each mutation reverts exactly:

```zsh
D=.superpowers/sdd/2026-09-27-job-tenancy
git diff crates/charpente-core/src/db.rs crates/charpente-core/src/job.rs crates/charpente-core/src/table.rs > $D/task5-impl.patch
```

Apply each mutation alone, run the named test, and quote the failure. Restore with `git checkout crates/charpente-core/src/db.rs crates/charpente-core/src/job.rs crates/charpente-core/src/table.rs && git apply $D/task5-impl.patch`.

1. `encode_record`: `if scope.is_some() { 10 } else { 8 }` becomes `8`. Expected: `scoped_enqueue_bytes_are_pinned_with_tenant_zero` fails at the first byte of the first effect.
2. `decode_record` arm `10`: `scope: Some(u64::decode(input)?)` becomes `scope: { u64::decode(input)?; None }`. Expected: the pinned-bytes test's decode assertion fails, and `a_shipped_scoped_enqueue_materializes_with_its_scope_tenant_zero_included` fails.
3. The apply arm passes `None` instead of `*scope`. Expected: the materialize test fails, with every scope `None`.
4. `JobRow::encode_row`: drop the `scope.encode(out)`. Expected: `a_scoped_job_row_roundtrips_its_tenant_zero_included` fails on the length.
5. `decode_row`'s `16` arm returns `(Some(u64::decode(input)?), None)` after reading both. Expected: the round-trip test fails at `Some(0)`, which is the tenant-0 red.

- [ ] **Step 6: Docs**

`DESIGN.md` §4, **Record payload**:
- After "emit `0x09 [event][payload][key: Option<String>][shape: u64]`,", insert "scoped enqueue `0x0a [job][payload][key: Option<String>][shape: u64][scope: u64]` (a `#[job(scoped_by = …)]` job's tenant, spec 2026-09-27-job-tenancy §6),".
- Replace "`0x0a` is the next free tag." with "The scoped enqueue is a new tag rather than a widened `0x08` for the same reason, chosen by presence and never by the tenant's value, so tenant 0 is `0x0a` too; its `JobRow` carries the tenant as a trailing `scope`, absent for every other row. `0x0b` is the next free tag."

`DESIGN.md` §5: "`PROTO_VERSION` is 3 — … payload shapes' `0x08`/`0x09` record tags made it 3, and a `Hello` declaring an older version is refused by name." becomes "`PROTO_VERSION` is 4 — … payload shapes' `0x08`/`0x09` record tags made it 3, job tenancy's `0x0a` made it 4, and a `Hello` declaring an older version is refused by name."

- [ ] **Step 7: Gate and commit**

Run the gate and count it (87 of 87). Re-check `git status` and `git log -1` first. Then:

```zsh
git add crates/charpente-core/src/db.rs crates/charpente-core/src/job.rs crates/charpente-core/src/table.rs \
  crates/charpente/src/repl/mod.rs crates/charpente-core/tests/engine.rs crates/charpente-core/tests/materialize.rs \
  crates/charpente/tests/typed_tables.rs crates/charpente/tests/replication.rs DESIGN.md
git commit -m "feat(job-tenancy): record tag 0x0a carries a scoped job's tenant into JobRow.scope; PROTO_VERSION 4"
```

---

### Task 6: Scoped jobs — `#[job(scoped_by = Org)]`, the bare-`#[job]` refusal, and the dispatcher's tenant

**Files:**
- Modify `crates/charpente-core/src/db.rs`:
  - `Job::Scope`;
  - `Tx::push_enqueue` (new, private);
  - `Tx::enqueue` / `enqueue_keyed` bound on `UnscopedJob`;
  - `Tx::enqueue_in` (new);
  - `ScopedTx::enqueue` / `enqueue_keyed` (new);
  - the Pending-shape refusal's fix clause (~2781);
  - `TenantAdmission`'s doc names the dispatcher.
- Modify `crates/charpente-core/src/table.rs`: `NoJobTenant`, `sealed::NotScopedJob`, `UnscopedJob` (new, beside `NoTenant`/`UnscopedRow`), and `NoTenant`'s note gains the job case.
- Modify `crates/charpente-macros/src/job.rs`:
  - the module doc;
  - `ACCEPTED_FORMS`;
  - `JobArg::ScopedBy`, `Tenancy`, `JobArgs.tenancy`;
  - the parser's conflicts;
  - `expand`'s bare refusal, `type Scope`, the `TenantScope` assert, the scoped glue, and `JobDef.scoped_by`.
- Modify `crates/charpente-macros/src/fingerprint.rs`: `fold` and `compute_job`, plus a unit test.
- Modify `crates/charpente-macros/src/route.rs`: `scope_name` becomes `pub(crate)`.
- Modify `crates/charpente/src/kernel/dispatch.rs`: `JobDef.scoped_by`, `JobGlue`'s fourth argument, `JobScope`, and `run_due_into`'s admission, assert and binding.
- Modify `crates/charpente/src/kernel/mod.rs`: re-export `JobScope`.
- Modify `crates/charpente/src/http/app.rs`: `App::schedule`'s bound.
- Modify `crates/charpente/src/tenant.rs`: the module doc's remedy (spec §14).
- Add `type Scope = Unscoped;` to the fourteen hand `impl Job` blocks:
  - `crates/charpente-core/tests/materialize.rs:30`;
  - `crates/charpente/tests/transactions.rs` 49, 328, 376;
  - `crates/charpente/tests/kernel.rs:2927`;
  - `crates/charpente/tests/http.rs:1144`;
  - `crates/charpente/tests/replication.rs:65`;
  - `crates/charpente/src/serve.rs` 722, 781, 814;
  - `crates/charpente/src/launch.rs:414`;
  - `crates/charpente/src/repl/probe.rs:407`;
  - `crates/charpente/src/repl/mod.rs:1380`;
  - `examples/invoicing/examples/seed_foreign_job.rs:38`.

  The fifteenth is GUIDE §4's `SendReceipt` block.
- Give the hand glues a fourth parameter, and their `JobDef`s `scoped_by: None`: `launch.rs:418/435`, `serve.rs:726/755` and `serve.rs:823/839`. The direct glue call at `crates/charpente/tests/kernel.rs:71` gains its fourth argument.
- Test:
  - `crates/charpente/tests/kernel.rs`: `mod scoped_jobs`, and the refusal pins at `receipt_refusal` (~2615) and ~3213;
  - `crates/charpente/tests/replication.rs:~7873`.
- Create in `crates/charpente-macros/tests/`, each compile-fail file with its `.stderr`:
  - compile-fail: `statuspage_job_no_tenancy.rs`, `job_bare.rs`, `job_global_and_scoped_by.rs`, `job_duplicate_scoped_by.rs`, `job_scoped_by_missing_tenant_scope.rs`, `scoped_job_enqueued_on_bare_tx.rs`, `scoped_job_scheduled_by_cron.rs`, `scoped_tx_enqueues_another_scopes_job.rs`, `scoped_tx_enqueue_of_a_global_job.rs`, `enqueue_in_without_capability.rs`, `scoped_job_run_takes_bare_tx.rs`;
  - compile-pass: `scoped_job_enqueued_inside_its_scope.rs`, `global_cross_tenant_fan_out.rs`.
- Re-pin in `compile_fail/`: `job_bad_retry.stderr`, `scoped_read_from_bare_tx.stderr`, `unscoped_row_impl_from_an_app.stderr`.
- Modify `crates/guide-examples/tests/doc_truth.rs`: `PINNED_FLOOR` 22 → 24.
- Docs:
  - `docs/GUIDE.md` §4, §6, §7, §9, §12 and §16;
  - `DESIGN.md` §3's **Jobs** bullet, §6's **Queue** and **Cron** bullets, §8;
  - `DECISIONS.md` §12 (spec §14's correction, verbatim);
  - `README.md` (`:30-54`, `:62-64`, `:140-143`);
  - `docs/superpowers/specs/2026-09-16-default-secure-surfaces-design.md` §8's `#[job]` item;
  - `docs/superpowers/specs/2026-08-30-tenant-scoping-design.md` ruling 3.

**Interfaces:**
- Consumes:
  - Task 2: `TenantAdmission`, `Group::tenant_admission`, `Tx::admit_scope`.
  - Task 3: `route::scope_name`.
  - Task 4: `TenantScope`'s note ("a tenant scope needs…").
  - Task 5: `Enqueue { job, payload, key, shape, scope }`, `JobRow.scope`, tag `0x0a`.
- Produces (spec §5):
  - `trait Job { type Scope: 'static; … }`. No default.
  - `pub trait NoJobTenant<J>: sealed::NotATenant {}`, implemented for `Unscoped` only. `pub trait UnscopedJob: Job + sealed::NotScopedJob {}`, blanket-implemented where `J::Scope: NoJobTenant<J>`. Both live in `charpente_core::table`.
  - `Tx::enqueue<J: UnscopedJob>(&mut self, job: J)` and `Tx::enqueue_keyed<J: UnscopedJob>(&mut self, key: impl Into<String>, job: J)`.
  - `Tx::enqueue_in<S: Tenant, J: Job<Scope = S>>(&mut self, scope: Id<S>, job: J, cap: &CrossTenant)`.
  - `ScopedTx::enqueue<J: Job<Scope = S>>(&mut self, job: J)` and `ScopedTx::enqueue_keyed<J: Job<Scope = S>>(&mut self, key: impl Into<String>, job: J)`.
  - `JobDef.scoped_by: Option<&'static str>`. Task 7's `describe` reads it.
  - `pub type JobGlue = fn(&mut Tx, &[u8], Option<&CrossTenant>, Option<JobScope<'_>>) -> Result<(), JobError>;`
  - `pub struct JobScope<'j>`, with private fields and `pub fn open<'t, 'a, S: TenantScope>(self, tx: &'t mut Tx<'a>) -> ScopedTx<'t, 'a, S>`, re-exported as `charpente::kernel::JobScope`.
  - `App::schedule<J: UnscopedJob + 'static>(…)`.
  - A scoped job's `run` is `fn run(self, tx: &mut ScopedTx<'_, '_, Org>[, cap: &CrossTenant]) -> Result<(), JobError>`, and it is called through a fn-pointer binding.
  - Refusal texts (spec §5, §6), fixed:
    - "`{Name}` declares no tenancy — a job says which: write `#[job(scoped_by = Org)]` if it acts inside one tenant (it is enqueued from inside that tenant's scope, and its `run` receives that tenant's `ScopedTx`), or `#[job(global)]` if it belongs to no tenant (the only kind cron can schedule). Neither is the default."
    - "`global` and `scoped_by = {Scope}` on one job — a job is one or the other; drop one"
    - "`scoped_by` given twice — write it once"
    - `ACCEPTED_FORMS`: "a job declares `global` or `scoped_by = <Type>`, and may add `cross_tenant` and `retry(max = <int>, backoff = \"<n><unit>\")` (unit is one of ms, s, m, h), in any order — e.g. `#[job(scoped_by = Org, retry(max = 3, backoff = \"10s\"))]` or `#[job(global, cross_tenant)]`"
    - the refusal's fix clause: "…or restore {name}'s old fields and its `global`/`scoped_by` declaration and give the new shape its own #[job] type under a new name (GUIDE §6)".

- [ ] **Step 1: Pin the bare form, and see it compile at the Task 5 commit**

Create `crates/charpente-macros/tests/compile_fail/statuspage_job_no_tenancy.rs`:

```rust
// The statuspage oracle's job (job-tenancy spec, Context and §9 item 1),
// quoted from `6037704:src/notify.rs:10-17`: attribute, doc comment and
// fields verbatim. A job that acts inside one org, declared with a bare
// `#[job]`. It had no `Ctx`, so it opened its org with
// `open_scope(org, None)`, which is why that app's `authorize` admitted
// `None` for every caller. The bare form is now a refusal that names both
// answers. `run` is left out: a refused job emits no type for an `impl` to
// name.
use charpente::job;

#[job]
pub struct NotifySubscribers {
    pub org: u64,
    pub incident: u64,
    /// The id-counter stamp the resolve transaction drew: subscriptions with an id
    /// below it existed at resolve time; later ones did not.
    pub stamp: u64,
}

fn main() {}
```

Create `crates/charpente-macros/tests/compile_fail/job_bare.rs`:

```rust
// Neither `global` nor `scoped_by` (job-tenancy spec §5, DECISIONS §13): a
// job's tenancy is a required choice, and absence is a compile error naming
// both answers — including when the attribute carries other words. Spanned at
// the attribute, as `#[table]`'s refusal is.
use charpente::job;

#[job]
struct Bare {}

#[job(retry(max = 3, backoff = "10s"))]
struct RetryOnly {}

#[job(cross_tenant)]
struct CapabilityOnly {}

fn main() {}
```

Run at the Task 5 commit:

```zsh
cargo test -p charpente-macros --test trybuild 2>&1 | grep -E "statuspage_job_no_tenancy|job_bare|Expected test case to fail"
```

Expected, the red to quote in the report: both files compile (trybuild reports they were expected to fail). The bare form is accepted today, and Task 1's sweep is the only thing keeping it out of the tree.

- [ ] **Step 2: Core — the job's scope, the sealed pair, and the enqueue family**

`crates/charpente-core/src/db.rs`, in `pub trait Job`, after `const FINGERPRINT: u64;`:

```rust
    /// The tenant scope type a `#[job(scoped_by = Org)]` job runs in, or
    /// `Unscoped` for a `#[job(global)]` one (job-tenancy spec §5). No
    /// default, for `FINGERPRINT`'s reason: a hand-written impl states it,
    /// or it would slip past the enqueue family's bounds unseen. `'static`
    /// because `push_enqueue` compares it by `TypeId`.
    type Scope: 'static;
```

`crates/charpente-core/src/table.rs`. Replace `NoTenant`'s `note`:

```rust
    note = "open the tenant scope first — in a handler, `{Self}`'s own `Tenant::authorize` runs \
            there for a credentialed caller:\n    \
            let tx = tx.scope::<{Self}>(scope, ctx)?;\n    \
            tx.scan::<{R}>()?\n\
            in a job, declare `#[job(scoped_by = {Self})]` and read through the `ScopedTx` its \
            `run` receives"
```

Add `pub trait NotScopedJob {}` to `mod sealed`. After `impl<R: Row> UnscopedRow for R where R::Scope: NoTenant<R> {}`, add:

```rust
/// The job twin of `NoTenant` (job-tenancy spec §5): `Unscoped` implements it
/// for every job type, and no other scope type implements it at all. It
/// carries the refusal for a `#[job(scoped_by = …)]` job enqueued where no
/// tenant is named — `Tx::enqueue` on a bare `Tx`, or a cron schedule — and it
/// hangs on a trait the SCOPE type implements or not for `NoTenant`'s reason:
/// an associated-type mismatch cannot carry a custom diagnostic. `{J}` puts
/// the job's name in the text, as `{R}` puts the row's.
///
/// SEALED by `NotATenant`, the same seal, and `UnscopedJob` below by its own:
/// either one alone would leave the other as a one-line, orphan-legal opt-out
/// (`impl NoJobTenant<Notify> for Org {}`, or `impl UnscopedJob for Notify {}`).
#[diagnostic::on_unimplemented(
    message = "`{J}` is a tenant-scoped job (`#[job(scoped_by = {Self})]`) and a bare `Tx` \
               names no tenant to run it in",
    label = "`{J}` is scoped by `{Self}`",
    note = "enqueue it from inside the tenant it acts in, and it runs in that tenant:\n    \
            let mut tx = tx.scope::<{Self}>(scope, ctx)?;\n    \
            tx.enqueue(job);\n\
            or fan it out from a `cross_tenant` route or job, naming the tenant:\n    \
            tx.enqueue_in::<{Self}>(scope, job, cap);\n\
            cron (`App::schedule`) enqueues on a bare `Tx`, so it schedules `global` jobs only"
)]
pub trait NoJobTenant<J>: sealed::NotATenant {}

impl<J> NoJobTenant<J> for Unscoped {}

/// `UnscopedJob`'s seal, derived from `Job::Scope` by the same condition
/// `UnscopedJob` uses, so the seal admits exactly the jobs the bound admits.
impl<J: crate::db::Job> sealed::NotScopedJob for J where J::Scope: NoJobTenant<J> {}

/// A job with no tenant: exactly the `Job`s whose `Scope` is `Unscoped`,
/// derived from `Job::Scope` as `UnscopedRow` is from `Row::Scope`. This is
/// the bound on `Tx::enqueue`, `Tx::enqueue_keyed` and `App::schedule`, so a
/// scoped job at any of them is an unsatisfied bound whose text names the fix,
/// not a job that runs in no tenant. Sealed for `UnscopedRow`'s reason.
pub trait UnscopedJob: crate::db::Job + sealed::NotScopedJob {}

impl<J: crate::db::Job> UnscopedJob for J where J::Scope: NoJobTenant<J> {}
```

`crates/charpente-core/src/db.rs`: add `UnscopedJob` to the `use crate::table::{…}` list (~46); `TypeId` is already imported (~36). Replace `Tx::enqueue` and `Tx::enqueue_keyed` (Task 5's struct-pushing versions) with:

```rust
    /// The one body of the enqueue family (job-tenancy spec §5–§6). `scope`
    /// is the tenant a scoped job runs in, and `None` for a `global` one.
    /// The public methods' bounds decide which, and this asserts that they
    /// agree with the job's own declaration, so a scoped job is never
    /// written without its tenant and a global one never with one (hard
    /// rule 4).
    fn push_enqueue<J: Job>(&mut self, scope: Option<u64>, key: Option<String>, job: J) {
        assert_eq!(
            scope.is_some(),
            TypeId::of::<J::Scope>() != TypeId::of::<crate::table::Unscoped>(),
            "contract: job \"{}\" was enqueued with tenant {scope:?}, but a scoped job carries \
             its tenant and a global one none — the enqueue family's bounds guarantee this, so \
             reaching here is a platform bug",
            J::NAME
        );
        if let Some(key) = &key {
            assert!(
                !key.is_empty(),
                "contract: an idempotency key names the job's external identity — it cannot be \
                 empty"
            );
        }
        let mut payload = Vec::new();
        job.encode(&mut payload);
        self.effects.enqueues.push(Enqueue {
            job: J::NAME,
            payload,
            key,
            shape: J::FINGERPRINT,
            scope,
        });
    }

    /// Enqueues a `global` job into this transaction's record. A
    /// `#[job(scoped_by = Org)]` job does not compile here: it names a
    /// tenant, and a bare `Tx` names none (`NoJobTenant`'s note says where to
    /// enqueue it instead).
    pub fn enqueue<J: UnscopedJob>(&mut self, job: J) {
        self.push_enqueue(None, None, job);
    }

    /// Enqueues with an idempotency key: the job's stable identity to the world
    /// OUTSIDE the log (consumed by M1's external-IO surface). Inside the
    /// deterministic boundary the key means nothing — exactly-once state effects
    /// are structural, so the platform neither indexes nor dedupes on it.
    pub fn enqueue_keyed<J: UnscopedJob>(&mut self, key: impl Into<String>, job: J) {
        self.push_enqueue(None, Some(key.into()), job);
    }

    /// The declared fan-out (job-tenancy spec §2 R5): enqueues a scoped job
    /// into the tenant `scope` names, from a route or job that declared
    /// `cross_tenant`. The token is the declaration, so one inventory,
    /// `describe`'s `cross_tenant=true`, answers "what reaches every tenant
    /// here", by reads and by fan-out. The job runs in `scope` exactly as if
    /// it had been enqueued from inside it.
    pub fn enqueue_in<S: Tenant, J: Job<Scope = S>>(
        &mut self,
        scope: Id<S>,
        job: J,
        cap: &CrossTenant,
    ) {
        let CrossTenant(()) = cap;
        self.push_enqueue(Some(scope.raw()), None, job);
    }
```

In `impl<'a, S: 'static> ScopedTx<'_, 'a, S>`, after `emit`:

```rust
    /// Enqueues a job scoped by this tenant's type; it runs in this tenant
    /// (job-tenancy spec §5). This shadows `Tx::enqueue` the way `emit`
    /// shadows `Tx::emit`. A `global` job enqueued inside a scope is written
    /// `(*tx).enqueue(job)` in a handler that owns its `ScopedTx`, and
    /// `(**tx).enqueue(job)` in a scoped job's `run`, which holds
    /// `&mut ScopedTx`.
    pub fn enqueue<J: Job<Scope = S>>(&mut self, job: J) {
        let scope = self.scope.raw();
        self.tx.push_enqueue(Some(scope), None, job);
    }

    /// `enqueue`, with `Tx::enqueue_keyed`'s idempotency key.
    pub fn enqueue_keyed<J: Job<Scope = S>>(&mut self, key: impl Into<String>, job: J) {
        let scope = self.scope.raw();
        self.tx.push_enqueue(Some(scope), Some(key.into()), job);
    }
```

The Pending-shape refusal (~2781): change "or restore {name}'s old fields and give the new shape its own #[job] type under a new name (GUIDE §6)" to "or restore {name}'s old fields and its `global`/`scoped_by` declaration and give the new shape its own #[job] type under a new name (GUIDE §6)".

`TenantAdmission`'s doc, the holders paragraph from Task 3, gains its third holder: "…and the dispatcher, which mints one per job run and hands a scoped job's glue a `JobScope` that opens only the tenant its row carries."

- [ ] **Step 3: The macro — `scoped_by`, the refusals, the fingerprint and the glue**

`crates/charpente-macros/src/route.rs`: `fn scope_name` becomes `pub(crate) fn scope_name`.

`crates/charpente-macros/src/fingerprint.rs`. Split `compute` into `fold` and `compute`, and add `compute_job`:

```rust
/// The NUL-separated field fold `compute` hashes (see `compute`).
fn fold(field_idents: &[syn::Ident], field_types: &[Type]) -> String {
    let mut buf = String::new();
    for (ident, ty) in field_idents.iter().zip(field_types) {
        buf.push_str(&ident.to_string());
        buf.push('\0');
        let mut words = Vec::new();
        render(ty.to_token_stream(), &mut words);
        buf.push_str(&words.join(" "));
        buf.push('\0');
    }
    buf
}

/// NUL-separated fold so ("ab","c") cannot collide with ("a","bc"). Each type
/// is rendered by `render`, not by `TokenStream`'s `Display`: that printer's
/// spacing is the compiler's and is documented as subject to change, and
/// these hashes are persisted.
pub(crate) fn compute(field_idents: &[syn::Ident], field_types: &[Type]) -> u64 {
    fnv1a(fold(field_idents, field_types).as_bytes())
}

/// A job's fingerprint (job-tenancy spec §6). A `global` job's is exactly
/// `compute`'s, so no job changes shape by declaring `global`. A `scoped_by`
/// job's is the same fold followed by `#scoped_by\0<path tokens>\0`, so a
/// change of tenancy is a change of shape, which the Pending-shape refusal
/// names. No identifier begins with `#`, so no field list can produce the
/// suffix. `cross_tenant` is not mixed in, because it changes neither the
/// stored bytes nor decode.
pub(crate) fn compute_job(
    field_idents: &[syn::Ident],
    field_types: &[Type],
    scoped_by: Option<&syn::Path>,
) -> u64 {
    let mut buf = fold(field_idents, field_types);
    if let Some(path) = scoped_by {
        buf.push_str("#scoped_by\0");
        let mut words = Vec::new();
        render(path.to_token_stream(), &mut words);
        buf.push_str(&words.join(" "));
        buf.push('\0');
    }
    fnv1a(buf.as_bytes())
}
```

Add to its tests:

```rust
    /// Spec §6: `global` is the bare fold, and `scoped_by` appends exactly
    /// `#scoped_by\0<path>\0`. The literal pins the operands, not only that
    /// the two differ.
    #[test]
    fn a_scoped_jobs_fingerprint_mixes_its_scope_and_a_global_ones_does_not() {
        let idents: Vec<syn::Ident> = vec![parse_quote!(n)];
        let types: Vec<Type> = vec![parse_quote!(u64)];
        let org: syn::Path = parse_quote!(Org);
        let team: syn::Path = parse_quote!(Team);
        assert_eq!(compute_job(&idents, &types, None), compute(&idents, &types));
        assert_eq!(
            compute_job(&idents, &types, Some(&org)),
            fnv1a(b"n\0u64\0#scoped_by\0Org\0")
        );
        assert_ne!(
            compute_job(&idents, &types, Some(&org)),
            compute_job(&idents, &types, Some(&team))
        );
    }
```

`crates/charpente-macros/src/job.rs`. The module doc's first line (Task 1's) becomes:

```rust
//! `#[job(global | scoped_by = Org[, cross_tenant][, retry(max = N, backoff = "30s")])]`,
//! the words in any order, each at most once, and exactly one of `global` /
//! `scoped_by` (DESIGN.md §6, specs 2026-07-02-queue-cron-design.md and
//! 2026-09-27-job-tenancy-design.md §5):
```

`ACCEPTED_FORMS` becomes:

```rust
const ACCEPTED_FORMS: &str = "a job declares `global` or `scoped_by = <Type>`, and may add \
     `cross_tenant` and `retry(max = <int>, backoff = \"<n><unit>\")` (unit is one of ms, s, \
     m, h), in any order — e.g. `#[job(scoped_by = Org, retry(max = 3, backoff = \"10s\"))]` \
     or `#[job(global, cross_tenant)]`";
```

Add the tenancy type, and a field to `JobArgs` (with `tenancy: None` in its `Default`):

```rust
/// A job's answer to "which tenant does this act in" (job-tenancy spec §5).
pub(crate) enum Tenancy {
    Global,
    ScopedBy(syn::Path),
}
```

```rust
    /// `global` or `scoped_by = Org`. `None` only while parsing: `expand`
    /// refuses a job that gave neither, by name, before anything else.
    pub tenancy: Option<Tenancy>,
```

`JobArg` gains `ScopedBy(proc_macro2::Span, syn::Path)`, parsed in `impl Parse for JobArg`:

```rust
            "scoped_by" => {
                input
                    .parse::<Token![=]>()
                    .map_err(|e| Error::new(e.span(), ACCEPTED_FORMS))?;
                let path: syn::Path = input
                    .parse()
                    .map_err(|e| Error::new(e.span(), ACCEPTED_FORMS))?;
                Ok(JobArg::ScopedBy(ident.span(), path))
            }
```

In `impl Parse for JobArgs`:
- delete Task 1's "`global` changes nothing in expansion yet" comment;
- add `let mut scoped_by: Option<syn::Path> = None;`;
- the `Global` arm and a new `ScopedBy` arm become:

```rust
                JobArg::Global(span) => {
                    if global.is_some() {
                        return Err(Error::new(span, "`global` given twice — write it once"));
                    }
                    if let Some(path) = &scoped_by {
                        return Err(Error::new(span, global_and_scoped_by(path)));
                    }
                    global = Some(span);
                }
                JobArg::ScopedBy(span, path) => {
                    if scoped_by.is_some() {
                        return Err(Error::new(span, "`scoped_by` given twice — write it once"));
                    }
                    if global.is_some() {
                        return Err(Error::new(span, global_and_scoped_by(&path)));
                    }
                    scoped_by = Some(path);
                }
```

The returned `JobArgs` gains:

```rust
            tenancy: match (global, scoped_by) {
                (_, Some(path)) => Some(Tenancy::ScopedBy(path)),
                (Some(_), None) => Some(Tenancy::Global),
                (None, None) => None,
            },
```

Add after `parse_backoff`:

```rust
/// `global` and `scoped_by` on one job, reported at whichever came second.
/// Names the scope by its last segment, as the other tenancy refusals do.
fn global_and_scoped_by(path: &syn::Path) -> String {
    format!(
        "`global` and `scoped_by = {}` on one job — a job is one or the other; drop one",
        crate::route::scope_name(path)
    )
}
```

In `expand`, first, before the generics check:

```rust
    let Some(tenancy) = &args.tenancy else {
        // Spanned at the attribute, as `#[table]`'s neither-refusal is: the
        // fix is a word the attribute lacks, so there is no token to point
        // at inside it.
        return Err(Error::new(
            proc_macro2::Span::call_site(),
            format!(
                "`{name_str}` declares no tenancy — a job says which: write \
                 `#[job(scoped_by = Org)]` if it acts inside one tenant (it is enqueued from \
                 inside that tenant's scope, and its `run` receives that tenant's `ScopedTx`), \
                 or `#[job(global)]` if it belongs to no tenant (the only kind cron can \
                 schedule). Neither is the default."
            ),
        ));
    };
```

Replace the fingerprint line with:

```rust
    let scoped_path = match tenancy {
        Tenancy::Global => None,
        Tenancy::ScopedBy(path) => Some(path),
    };
    let fingerprint =
        crate::fingerprint::compute_job(&field_idents, &field_types, scoped_path);
```

After the `cross_tenant` lines, replace `run_call` with the tenancy-aware emission:

```rust
    let cap_expect = quote! {
        cap.expect(
            "job declares cross_tenant but the dispatcher passed no CrossTenant — \
             the flag on JobDef and the token come from the same expansion"
        )
    };
    // A `global` job's glue is today's. A scoped job's opens the tenant the
    // dispatcher's `JobScope` carries, then calls `run` through a fn-pointer
    // binding rather than a method call (job-tenancy spec §5): `job.run(&mut
    // tx)` would deref-coerce `&mut ScopedTx` to `&mut Tx`, so a scoped job
    // whose `run` takes a bare `Tx` would compile and run in no tenant. The
    // binding makes that a compile error, pinned by
    // `compile_fail/scoped_job_run_takes_bare_tx.rs`.
    let (scope_ty, tenant_scope_assert, scoped_by_name, run_call) = match tenancy {
        Tenancy::Global => {
            let call = if cross_tenant {
                quote! { job.run(tx, #cap_expect) }
            } else {
                quote! { job.run(tx) }
            };
            (
                quote! { ::charpente::core::table::Unscoped },
                quote! {},
                quote! { ::std::option::Option::None },
                quote! {
                    let _ = scope; // a global job is handed no tenant
                    #call
                },
            )
        }
        Tenancy::ScopedBy(path) => {
            let scope_name = crate::route::scope_name(path);
            let (cap_ty, cap_arg) = if cross_tenant {
                (quote! { , &::charpente::CrossTenant }, quote! { , #cap_expect })
            } else {
                (quote! {}, quote! {})
            };
            (
                quote! { #path },
                quote! {
                    const _: () = {
                        const fn __charpente_assert_tenant_scope<T: ::charpente::TenantScope>() {}
                        __charpente_assert_tenant_scope::<#path>();
                    };
                },
                quote! { ::std::option::Option::Some(#scope_name) },
                quote! {
                    let scope = scope.expect(
                        "job declares scoped_by but the dispatcher passed no JobScope — the \
                         dispatcher asserts a row carries a tenant exactly when JobDef.scoped_by \
                         is set"
                    );
                    let mut tx = scope.open::<#path>(tx);
                    let run: fn(
                        #name,
                        &mut ::charpente::ScopedTx<'_, '_, #path>
                        #cap_ty
                    ) -> ::std::result::Result<(), ::charpente::kernel::JobError> = #name::run;
                    run(job, &mut tx #cap_arg)
                },
            )
        }
    };
```

In the emitted `quote!`:
- add `#tenant_scope_assert` on the line after `#input`;
- in `impl ::charpente::core::db::Job for #name`, add `type Scope = #scope_ty;` after `FINGERPRINT`;
- the glue gains a fourth parameter, `scope: ::std::option::Option<::charpente::kernel::JobScope<'_>>,`;
- the `JobDef` literal gains `scoped_by: #scoped_by_name,` after `cross_tenant`.

`#run_call` stays the glue's last expression.

- [ ] **Step 4: The platform — `JobScope`, the dispatcher, the schedule bound, the hand impls**

`crates/charpente/src/kernel/dispatch.rs`:
- the imports gain `ScopedTx, TenantAdmission` (from `charpente_core::db`) and `use crate::tenant::TenantScope;`;
- in `JobDef`, after `cross_tenant`:

```rust
    /// `#[job(scoped_by = Org)]` — the scope type's last path segment, as
    /// `describe` lists it; `None` for a `global` job. Dispatch asserts that
    /// a row carries a tenant exactly when this is `Some`.
    pub scoped_by: Option<&'static str>,
```

- `JobGlue` and its doc:

```rust
/// The glue signature `#[job]` generates: decode the payload, call the
/// payload's `run`, hand back its verdict. Named, like `http::Glue`, so the
/// field above stays under clippy's type-complexity threshold. The last
/// argument is `Some` exactly for a scoped job: the tenant its row carries,
/// which only the dispatcher can construct.
pub type JobGlue =
    fn(&mut Tx, &[u8], Option<&CrossTenant>, Option<JobScope<'_>>) -> Result<(), JobError>;

/// The tenant a scoped job was enqueued in, handed to its glue by the
/// dispatcher and by nothing else (job-tenancy spec §5). Its fields are
/// private and it has no public constructor, so a glue, generated or
/// hand-written, can open exactly the tenant its row carries and no other.
pub struct JobScope<'j> {
    id: u64,
    admission: &'j TenantAdmission,
}

impl JobScope<'_> {
    /// Opens the enqueuer's tenant on the job's transaction, by the
    /// dispatcher's admission. No `authorize` runs: a job has no principal.
    /// Its authority is its enqueuer's, who was either inside this tenant or
    /// declared `cross_tenant` to name it.
    pub fn open<'t, 'a, S: TenantScope>(self, tx: &'t mut Tx<'a>) -> ScopedTx<'t, 'a, S> {
        tx.admit_scope(Id::from_raw(self.id), self.admission)
    }
}
```

In `run_due_into`, after `let cap = group.cross_tenant_cap();`:

```rust
        // Beside it, for the same reason: a scoped job's tenant is opened with
        // the dispatcher's admission (job-tenancy spec §5), minted from the
        // `Group` no glue ever holds.
        let admission = group.tenant_admission();
```

After the shape `assert_eq!`, before `match catch_unwind(…)`:

```rust
            // The fingerprint mixes the declaration in (spec §6), so a row of
            // this shape was enqueued by a type with this tenancy, and the
            // enqueue family writes a tenant exactly for a scoped type. A
            // mismatch is unreachable — asserted, not handled (hard rule 4).
            assert_eq!(
                row.scope.is_some(),
                def.scoped_by.is_some(),
                "dispatcher invariant: job {id} (\"{}\") carries tenant {:?} but this binary \
                 declares it {} — the fingerprint mixes the declaration in, so a row of this \
                 shape cannot disagree with it",
                row.name,
                row.scope,
                def.scoped_by
                    .map_or_else(|| "global".to_string(), |s| format!("scoped_by = {s}"))
            );
            let scope = row.scope.map(|id| JobScope {
                id,
                admission: &admission,
            });
```

The glue call becomes `(def.glue)(tx, &row.payload, def.cross_tenant.then_some(&cap), scope)`.

`crates/charpente/src/kernel/mod.rs`: `pub use dispatch::{Dispatcher, JobDef, JobEntry, JobError, JobOutcome, JobScope};`.

`crates/charpente/src/http/app.rs`: `App::schedule`'s bound becomes `J: UnscopedJob + 'static`, importing `charpente_core::table::UnscopedJob`. Its doc gains: "A schedule enqueues on a bare `Tx`, so it takes `global` jobs only; a sweep that acts per tenant is a `global, cross_tenant` job that fans out with `Tx::enqueue_in` (GUIDE §7)."

**The hand-written jobs.**
- Each of the fourteen `impl Job` blocks gains `type Scope = Unscoped;`. Import `Unscoped` from `charpente_core::table` inside `charpente-core`'s and `charpente`'s own crates, or from `charpente::core::table` in the example.
- Each hand glue gains a fourth parameter, `_scope: Option<crate::kernel::JobScope<'_>>`, and each of those three `JobDef` literals gains `scoped_by: None,`.
- `crates/charpente/tests/kernel.rs:71` becomes `.transact(|tx| (def.glue)(tx, &payload, None, None))`, and its comment becomes "`None, None`: `CountUp` declares no `cross_tenant` and is `global`, so its glue is handed neither a token nor a tenant."

**The refusal pins** gain the clause. In `receipt_refusal` (kernel.rs ~2621), in the literal at kernel.rs ~3213, and in replication.rs ~7873, change "restore <Name>'s old fields and give the new shape" to "restore <Name>'s old fields and its `global`/`scoped_by` declaration and give the new shape".

**`crates/charpente/src/tenant.rs` module doc** (spec §14). Replace:
- "A handler and a job receive a `Tx`; through a `Tx`, a scoped table is unreachable without `tx.scope::<Org>(org, ctx)?`" with "A handler receives a `Tx`, and a scoped job the `ScopedTx` of the tenant it was enqueued in; through a `Tx`, a scoped table is unreachable without `tx.scope::<Org>(org, ctx)?`";
- the closing sentence "Code outside a handler that wants the guarantee should go through a transaction and `tx.scope::<Org>(org, ctx)?` like a handler does." with:

```rust
//! Code outside a handler never takes a scope on its own word. A job declares
//! `#[job(scoped_by = Org)]` and runs in the tenant its enqueuer opened;
//! operator code that must act inside one tenant — seeding, a test oracle —
//! opens it with a `TenantAdmission` minted from a `Group` (`Tx::admit_scope`),
//! the seal `CrossTenant` already carries, which is the operator's standing
//! made explicit rather than an exception to the guarantee. *(Corrected
//! 2026-09-27, job-tenancy spec: the earlier remedy named a `Ctx` that no code
//! outside a handler holds.)*
```

Then:

```zsh
cargo check --workspace --all-targets 2>&1 | grep -E "^(error|warning)" | sort | uniq -c
```

Expected: no output.

- [ ] **Step 5: The kernel tests**

In `crates/charpente/tests/kernel.rs`, after `a_global_jobs_fingerprint_is_the_bare_jobs_fingerprint`:

```rust
// ------------------------------------------------ job tenancy: scoped jobs

/// Job-tenancy spec §5–§6 and Review Focus 1–3. A `#[job(scoped_by = …)]` job
/// runs in the tenant it was enqueued from (tenant 0 included), keeps that
/// tenant through every retry, hands it to its own follow-ups, lands in the
/// tenant a fan-out names, and a deploy that changes a job's tenancy is
/// refused by name over the rows it left Pending.
mod scoped_jobs {
    use super::*;
    use charpente::http::ApiError;
    use charpente::{CrossTenant, ScopedTx};

    #[table(global)]
    pub struct Desk {
        pub id: Id<Desk>,
        pub name: String,
    }

    impl Tenant for Desk {
        type Who = Principal;
        type Error = ApiError;
        /// Never asked: every test here enqueues through the operator path or
        /// a declared fan-out, and a job has no principal.
        fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Desk>) -> Result<(), ApiError> {
            Ok(())
        }
    }

    #[table(scoped_by = Desk)]
    pub struct Filed {
        pub id: Id<Filed>,
        pub note: u64,
    }

    /// Writes one row in its tenant. An even `n` also hands its tenant a
    /// scoped follow-up, and enqueues a global tally beside it.
    #[job(scoped_by = Desk)]
    pub struct File {
        pub n: u64,
    }

    impl File {
        fn run(self, tx: &mut ScopedTx<'_, '_, Desk>) -> Result<(), JobError> {
            tx.put(&Filed {
                id: Id::from_raw(self.n),
                note: self.n,
            });
            if self.n.is_multiple_of(2) {
                tx.enqueue(Stamp { n: self.n });
                // `run` holds `&mut ScopedTx`, so the global deref is `**`.
                (**tx).enqueue(Tally { n: self.n });
            }
            Ok(())
        }
    }

    #[job(scoped_by = Desk)]
    pub struct Stamp {
        pub n: u64,
    }

    impl Stamp {
        fn run(self, tx: &mut ScopedTx<'_, '_, Desk>) -> Result<(), JobError> {
            tx.put(&Filed {
                id: Id::from_raw(self.n + 100),
                note: self.n,
            });
            Ok(())
        }
    }

    #[job(global)]
    pub struct Tally {
        pub n: u64,
    }

    impl Tally {
        fn run(self, tx: &mut Tx) -> Result<(), JobError> {
            tx.put(&Counter {
                id: Id::from_raw(self.n),
                hits: 1,
            });
            Ok(())
        }
    }

    /// Always fails, naming the tenant its `run` was handed, so every
    /// attempt's `last_error` records which tenant that attempt ran in.
    #[job(scoped_by = Desk, retry(max = 3, backoff = "1s"))]
    pub struct Jam {
        pub n: u64,
    }

    impl Jam {
        fn run(self, tx: &mut ScopedTx<'_, '_, Desk>) -> Result<(), JobError> {
            Err(JobError::app(format!(
                "job {} jammed in desk {}",
                self.n,
                tx.scope().raw()
            )))
        }
    }

    /// The declared fan-out (spec §2 R5): one `File` into each desk it names.
    #[job(global, cross_tenant)]
    pub struct Sweep {}

    impl Sweep {
        fn run(self, tx: &mut Tx, cap: &CrossTenant) -> Result<(), JobError> {
            for desk in [0, 4] {
                tx.enqueue_in(Id::<Desk>::from_raw(desk), File { n: 11 }, cap);
            }
            Ok(())
        }
    }

    fn desk_app(seed: u64) -> (Sim, App) {
        let sim = Sim::with_faults(seed, FaultConfig::none());
        let app = App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Filed>().table::<Counter>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .unwrap()
        .mount_jobs(jobs![File, Stamp, Tally, Jam, Sweep]);
        (sim, app)
    }

    /// The operator path (spec §7): enqueue `job` inside `desk`, as a seed or
    /// an admin step would.
    fn enqueue_in_desk<J: Job<Scope = Desk>>(db: &mut Db, desk: u64, job: J) {
        let mut group = db.begin_group().unwrap();
        let admission = group.tenant_admission();
        group
            .stage(|tx| {
                tx.admit_scope(Id::<Desk>::from_raw(desk), &admission)
                    .enqueue(job);
                Ok::<(), std::io::Error>(())
            })
            .unwrap();
        group.commit().unwrap();
    }

    /// Runs every job due at `now`, follow-ups included, until none is left,
    /// and returns how many ran.
    fn drain(dispatcher: &mut Dispatcher, db: &mut Db, now: u64) -> usize {
        let mut ran = 0;
        loop {
            dispatcher.note_enqueued(db.drain_enqueued());
            if !dispatcher.has_due(now) {
                return ran;
            }
            dispatcher.run_due(db, now).unwrap();
            ran += 1;
        }
    }

    /// Review Focus 2: two desks enqueue the same job with the same payload,
    /// and each runs in its own desk. Desk 0 is a desk, not "no desk".
    #[test]
    fn a_scoped_job_runs_in_its_enqueuers_tenant_tenant_zero_included() {
        let (_sim, mut app) = desk_app(0x5C0B_0001);
        let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
        let db = app.db_for_tests();
        enqueue_in_desk(db, 0, File { n: 1 });
        enqueue_in_desk(db, 5, File { n: 1 });
        assert_eq!(drain(&mut dispatcher, db, 0), 2);

        let snap = db.snapshot().unwrap();
        let rows: Vec<(Option<u64>, JobStatus)> = snap
            .scan::<JobRow>()
            .unwrap()
            .into_iter()
            .map(|r| (r.scope, r.status))
            .collect();
        assert_eq!(
            rows,
            vec![(Some(0), JobStatus::Done), (Some(5), JobStatus::Done)],
            "each row carries its enqueuer's tenant; tenant 0 is Some(0)"
        );
        for desk in [0, 5] {
            assert_eq!(
                snap.get_scoped::<Filed>(desk, Id::from_raw(1))
                    .unwrap()
                    .map(|f| f.note),
                Some(1),
                "desk {desk}'s job wrote inside desk {desk}"
            );
        }
        assert!(
            snap.scan_scoped::<Filed>(3).unwrap().is_empty(),
            "and nothing landed in a desk that enqueued nothing"
        );
    }

    /// Review Focus 3: the failure record re-puts the row it read, so every
    /// retry runs in the same tenant, through to Dead. Were `scope` dropped
    /// there, the next attempt would trip the dispatch assert and stop the node.
    #[test]
    fn a_failing_scoped_job_keeps_its_tenant_through_every_retry_to_dead() {
        let (_sim, mut app) = desk_app(0x5C0B_0002);
        let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
        let db = app.db_for_tests();
        enqueue_in_desk(db, 7, Jam { n: 1 });
        dispatcher.note_enqueued(db.drain_enqueued());
        let mut now = 1_000;
        for attempt in 1..=3u32 {
            let outcome = dispatcher.run_due(db, now).unwrap();
            let row = db.snapshot().unwrap().scan::<JobRow>().unwrap().remove(0);
            assert_eq!(row.scope, Some(7), "attempt {attempt} kept the row's tenant");
            assert_eq!(
                row.last_error.as_deref(),
                Some("job 1 jammed in desk 7"),
                "attempt {attempt} ran in desk 7"
            );
            match outcome {
                JobOutcome::Retried {
                    attempts, due_at, ..
                } => {
                    assert_eq!(attempts, attempt);
                    now = due_at;
                }
                JobOutcome::Dead { attempts, .. } => assert_eq!((attempts, attempt), (3, 3)),
                JobOutcome::Done { .. } => panic!("Jam never succeeds"),
            }
        }
        let row = db.snapshot().unwrap().scan::<JobRow>().unwrap().remove(0);
        assert_eq!((row.status, row.scope), (JobStatus::Dead, Some(7)));
    }

    /// A scoped job's follow-up inherits its tenant through
    /// `ScopedTx::enqueue`; a global job it enqueues through the deref carries
    /// none.
    #[test]
    fn a_scoped_jobs_follow_ups_carry_its_tenant_and_a_global_one_none() {
        let (_sim, mut app) = desk_app(0x5C0B_0003);
        let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
        let db = app.db_for_tests();
        enqueue_in_desk(db, 3, File { n: 2 });
        assert_eq!(
            drain(&mut dispatcher, db, 0),
            3,
            "File, then its Stamp and its Tally"
        );

        let snap = db.snapshot().unwrap();
        let rows: Vec<(String, Option<u64>, JobStatus)> = snap
            .scan::<JobRow>()
            .unwrap()
            .into_iter()
            .map(|r| (r.name, r.scope, r.status))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("File".to_string(), Some(3), JobStatus::Done),
                ("Stamp".to_string(), Some(3), JobStatus::Done),
                ("Tally".to_string(), None, JobStatus::Done),
            ]
        );
        assert_eq!(
            snap.get_scoped::<Filed>(3, Id::from_raw(102))
                .unwrap()
                .map(|f| f.note),
            Some(2),
            "the follow-up wrote in desk 3"
        );
        assert!(
            snap.get_scoped::<Filed>(0, Id::from_raw(102))
                .unwrap()
                .is_none(),
            "and not in tenant 0"
        );
        assert_eq!(
            snap.get(Id::<Counter>::from_raw(2)).unwrap().map(|c| c.hits),
            Some(1),
            "the global tally ran, in no tenant"
        );
    }

    /// Spec §2 R5: a declared fan-out lands each job in the tenant it names,
    /// tenant 0 included, and each runs there.
    #[test]
    fn a_declared_fan_out_lands_each_job_in_the_tenant_it_names() {
        let (_sim, mut app) = desk_app(0x5C0B_0005);
        let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
        let db = app.db_for_tests();
        db.transact(|tx| {
            tx.enqueue(Sweep {});
            Ok::<(), std::io::Error>(())
        })
        .unwrap();
        assert_eq!(drain(&mut dispatcher, db, 0), 3, "the sweep, then one File per desk");

        let snap = db.snapshot().unwrap();
        let scopes: Vec<(String, Option<u64>)> = snap
            .scan::<JobRow>()
            .unwrap()
            .into_iter()
            .map(|r| (r.name, r.scope))
            .collect();
        assert_eq!(
            scopes,
            vec![
                ("Sweep".to_string(), None),
                ("File".to_string(), Some(0)),
                ("File".to_string(), Some(4)),
            ]
        );
        for desk in [0, 4] {
            assert!(
                snap.get_scoped::<Filed>(desk, Id::from_raw(11))
                    .unwrap()
                    .is_some(),
                "the fan-out's job ran in desk {desk}"
            );
        }
    }

    mod ledger_v1 {
        use charpente::kernel::JobError;
        use charpente::{Tx, job};

        #[job(global)]
        pub struct Reconcile {
            pub n: u64,
        }

        impl Reconcile {
            fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
                let _ = self.n;
                Ok(())
            }
        }
    }

    mod ledger_v2 {
        use super::Desk;
        use charpente::kernel::JobError;
        use charpente::{ScopedTx, job};

        /// v1 with its tenancy changed, and nothing else.
        #[job(scoped_by = Desk)]
        pub struct Reconcile {
            pub n: u64,
        }

        impl Reconcile {
            fn run(self, _tx: &mut ScopedTx<'_, '_, Desk>) -> Result<(), JobError> {
                let _ = self.n;
                Ok(())
            }
        }
    }

    /// Review Focus 1: the first thing an existing app does after upgrading is
    /// move a job from `global` to `scoped_by`, over Pending rows enqueued
    /// with no tenant. The new binary must refuse to start over them, by
    /// name, with the tenancy half of the fix. It must never dispatch one
    /// into no tenant, and never trip the dispatch assert.
    #[test]
    fn a_job_that_gains_scoped_by_is_refused_at_startup_over_its_pending_rows() {
        let v1 = <ledger_v1::Reconcile as Job>::FINGERPRINT;
        let v2 = <ledger_v2::Reconcile as Job>::FINGERPRINT;
        assert_ne!(
            v1, v2,
            "scoped_by is mixed into the fingerprint (spec §6): a tenancy change is a shape \
             change"
        );
        let sim = Sim::with_faults(0x5C0B_0004, FaultConfig::none());
        let open = |defs: Vec<JobDef>| {
            App::open(
                Box::new(sim.disk()),
                Schema::new(),
                Box::new(sim.entropy()),
                AppConfig::default(),
            )
            .unwrap()
            .mount_jobs(defs)
        };
        let mut old = open(jobs![ledger_v1::Reconcile]);
        old.db_for_tests()
            .transact(|tx| {
                tx.enqueue(ledger_v1::Reconcile { n: 1 });
                tx.enqueue(ledger_v1::Reconcile { n: 2 });
                Ok::<(), std::io::Error>(())
            })
            .unwrap();
        sim.restart();
        drop(old);

        let err = match Kernel::new(open(jobs![ledger_v2::Reconcile]), Box::new(sim.clock())) {
            Ok(_) => panic!(
                "the kernel started over two Pending rows enqueued with no tenant, which it \
                 would then have dispatched into none"
            ),
            Err(e) => e,
        };
        assert_eq!(
            err.to_string(),
            format!(
                "2 Pending \"Reconcile\" job(s) (ids 0, 1) were enqueued under a different shape \
                 of Reconcile (recorded {v1:#x}; this binary {v2:#x}) — this binary will not \
                 decode them; run the binary that enqueued them until they are Done or Dead, \
                 then deploy this one, or restore Reconcile's old fields and its \
                 `global`/`scoped_by` declaration and give the new shape its own #[job] type \
                 under a new name (GUIDE §6)"
            )
        );
        assert!(
            charpente::core::db::Refusal::of(&err).is_some(),
            "a refusal, not an IO failure"
        );
    }
}
```

`open` borrows `sim` shared, and `Sim::restart` takes `&self`, so the two coexist.

- [ ] **Step 6: The fixtures**

Each compile-fail fixture below names types by path wherever an import would otherwise go unused in a refused item. `across_tenants_without_capability.rs` explains why: an unused-import warning would sit in the pin. Each file opens with a comment stating what it pins, as the existing fixtures do.

`compile_fail/job_global_and_scoped_by.rs`:

```rust
// A job is `global` or `scoped_by`, never both (job-tenancy spec §5),
// reported at whichever came second.
use charpente::job;

#[job(global, scoped_by = Org)]
struct GlobalFirst {}

#[job(scoped_by = Org, global)]
struct ScopedFirst {}

fn main() {}
```

`compile_fail/job_duplicate_scoped_by.rs`:

```rust
// A declaration repeated is a typo or a bad merge, never an override — the
// table parser's precedent (job-tenancy spec §5).
use charpente::job;

#[job(scoped_by = Org, scoped_by = Org)]
struct Twice {}

fn main() {}
```

`compile_fail/job_scoped_by_missing_tenant_scope.rs`:

```rust
// `#[job(scoped_by = Org)]` puts `Org` in a tenant scope's place, so `Org`
// must carry the app's own membership check: the same `TenantScope` assert
// `#[table(scoped_by = …)]` and `public(Org)` emit, and the same diagnostic.
use charpente::kernel::JobError;
use charpente::{Id, ScopedTx, job, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[job(scoped_by = Org)]
struct File {
    n: u64,
}

impl File {
    fn run(self, _tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        let _ = self.n;
        Ok(())
    }
}

fn main() {}
```

`compile_fail/scoped_job_enqueued_on_bare_tx.rs`, the statuspage's enqueue site in shape (`6037704:src/incidents.rs:261`), with its job now declared:

```rust
// A scoped job enqueued on a bare `Tx` (job-tenancy spec §5): the `Tx` names
// no tenant for the job to run in. The statuspage's resolve route enqueued
// `NotifySubscribers` exactly like this, and the job then opened its org on
// its own word. The `NoJobTenant` note says where to enqueue it instead.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::kernel::JobError;
use charpente::{Id, ScopedTx, Tenant, Tx, job, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[job(scoped_by = Org)]
struct NotifySubscribers {
    incident: u64,
}

impl NotifySubscribers {
    fn run(self, _tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        let _ = self.incident;
        Ok(())
    }
}

#[route(POST "/orgs/:org/incidents/:id/resolve", auth)]
fn resolve(tx: &mut Tx, org: Id<Org>, id: charpente::wire::WireU64, ctx: &Ctx) -> Result<(), ApiError> {
    let _ = (org, ctx);
    tx.enqueue(NotifySubscribers { incident: id.0 });
    Ok(())
}

fn main() {}
```

`compile_fail/scoped_job_scheduled_by_cron.rs`:

```rust
// A cron tick has no tenant (job-tenancy spec §5): `App::schedule` enqueues
// on a bare `Tx`, so it takes `global` jobs only, and a scoped one meets the
// same `NoJobTenant` note, at the schedule.
use charpente::auth::Principal;
use charpente::core::io::{Disk, Entropy};
use charpente::http::{ApiError, App, AppConfig};
use charpente::kernel::JobError;
use charpente::{Id, Schema, ScopedTx, Tenant, Tx, job, table};
use std::time::Duration;

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[job(scoped_by = Org)]
struct MaintenanceSweep {}

impl MaintenanceSweep {
    fn run(self, _tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        Ok(())
    }
}

fn build(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> std::io::Result<App> {
    Ok(App::open(disk, Schema::new(), entropy, AppConfig::default())?
        .schedule("maintenance-sweep", Duration::from_secs(3600), || {
            MaintenanceSweep {}
        }))
}

fn main() {
    let _ = build;
}
```

`compile_fail/scoped_tx_enqueues_another_scopes_job.rs`:

```rust
// A `ScopedTx<Org>` enqueues `Org`'s jobs only (job-tenancy spec §5): a job
// scoped by another type would run in a tenant of a type this transaction
// never opened. rustc's own E0271 — pinned so GUIDE §16 can show it.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::kernel::JobError;
use charpente::{Id, Scoped, ScopedTx, Tenant, Tx, job, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(global)]
struct Team {
    id: Id<Team>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

impl Tenant for Team {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Team>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[job(scoped_by = Team)]
struct Rebuild {}

impl Rebuild {
    fn run(self, _tx: &mut ScopedTx<'_, '_, Team>) -> Result<(), JobError> {
        Ok(())
    }
}

#[route(POST "/orgs/:org/rebuild", auth)]
fn rebuild(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.enqueue(Rebuild {});
    Ok(())
}

fn main() {}
```

`compile_fail/scoped_tx_enqueue_of_a_global_job.rs`:

```rust
// A global job enqueued through a `ScopedTx` without the deref (job-tenancy
// spec §5) is an E0271, not a silent enqueue in no tenant.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::kernel::JobError;
use charpente::{Id, Scoped, Tenant, Tx, job, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[job(global)]
struct Audit {}

impl Audit {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

// `ScopedTx::enqueue` shadows `Tx::enqueue`, and Rust does not fall through the
// deref when the inherent method's bound fails: a global job is enqueued inside
// a scope as `(*tx).enqueue(Audit {})`. This pins the E0271 an author meets
// first, so GUIDE §16 can show it.
#[route(POST "/orgs/:org/audit", auth)]
fn audit(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.enqueue(Audit {});
    Ok(())
}

fn main() {}
```

`compile_fail/enqueue_in_without_capability.rs`:

```rust
// Fan-out is the `cross_tenant` capability's (job-tenancy spec §2 R5): a
// route that never declared it has no `&CrossTenant` to hand `enqueue_in`,
// and `#[route]` refuses the argument, reconstructing the attribute to write.
// `CrossTenant` and `WireU64` are named by path for
// `across_tenants_without_capability.rs`'s reason.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::kernel::JobError;
use charpente::{Id, ScopedTx, Tenant, Tx, job, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[job(scoped_by = Org)]
struct ApplyWindow {}

impl ApplyWindow {
    fn run(self, _tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        Ok(())
    }
}

#[route(POST "/admin/apply/:org", auth)]
fn apply_everywhere(
    tx: &mut Tx,
    org: Id<Org>,
    cap: &charpente::CrossTenant,
) -> Result<charpente::wire::WireU64, ApiError> {
    tx.enqueue_in(org, ApplyWindow {}, cap);
    Ok(charpente::wire::WireU64(org.raw()))
}

fn main() {}
```

`compile_fail/scoped_job_run_takes_bare_tx.rs`:

```rust
// The fn-pointer binding (job-tenancy spec §5): a scoped job's `run` takes
// its tenant's `ScopedTx`. A `run` that takes a bare `Tx` would compile under
// a method call, because `&mut ScopedTx` deref-coerces to `&mut Tx`, and it
// would then run in no tenant. The binding makes it a type error instead.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::kernel::JobError;
use charpente::{Id, Tenant, Tx, job, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[job(scoped_by = Org)]
struct File {
    n: u64,
}

impl File {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        let _ = self.n;
        Ok(())
    }
}

fn main() {}
```

`compile_pass/scoped_job_enqueued_inside_its_scope.rs`:

```rust
// The shapes a scoped job is written in (job-tenancy spec §5): enqueued, keyed
// or not, from inside its tenant's scope; `scoped_by` composed with
// `cross_tenant` and `retry(...)`; a follow-up handed on through the job's
// own `ScopedTx`; and a global job enqueued beside it through the deref.
// trybuild runs `main`, so its assertions are part of the pin.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::kernel::{JobEntry, JobError};
use charpente::{CrossTenant, Id, Scoped, ScopedTx, Tenant, Tx, job, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[table(scoped_by = Org)]
struct Incident {
    id: Id<Incident>,
    open: bool,
}

#[job(retry(max = 2, backoff = "1s"), scoped_by = Org)]
struct Notify {
    incident: u64,
}

impl Notify {
    fn run(self, tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        if let Some(incident) = tx.get(Id::<Incident>::from_raw(self.incident))? {
            let _ = incident.open;
        }
        tx.enqueue(Tally {});
        (**tx).enqueue(Audit {});
        Ok(())
    }
}

#[job(scoped_by = Org, cross_tenant)]
struct Tally {}

impl Tally {
    fn run(self, tx: &mut ScopedTx<'_, '_, Org>, cap: &CrossTenant) -> Result<(), JobError> {
        let _ = (tx.scope(), (**tx).across_tenants::<Org>(cap).scan::<Incident>()?);
        Ok(())
    }
}

#[job(global)]
struct Audit {}

impl Audit {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

#[route(POST "/orgs/:org/incidents/:id/resolve", auth)]
fn resolve(tx: &mut Tx, org: Id<Org>, id: Id<Incident>, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.put(&Incident { id, open: false });
    tx.enqueue(Notify { incident: id.raw() });
    tx.enqueue_keyed(format!("notify:{}", id.raw()), Notify { incident: id.raw() });
    (*tx).enqueue(Audit {});
    Ok(())
}

fn main() {
    assert_eq!(<Notify as JobEntry>::def().scoped_by, Some("Org"));
    assert_eq!(<Tally as JobEntry>::def().scoped_by, Some("Org"));
    assert!(<Tally as JobEntry>::def().cross_tenant);
    assert_eq!(<Audit as JobEntry>::def().scoped_by, None);
    assert_eq!(<Notify as JobEntry>::def().retry.max, 2);
}
```

`compile_pass/global_cross_tenant_fan_out.rs`:

```rust
// The declared fan-out (job-tenancy spec §2 R5): a cron-scheduled
// `global, cross_tenant` sweep reads every tenant and enqueues a scoped job
// into each tenant it names, and so does a `cross_tenant` route.
use charpente::auth::Principal;
use charpente::core::io::{Disk, Entropy};
use charpente::http::{ApiError, App, AppConfig};
use charpente::kernel::JobError;
use charpente::{CrossTenant, Id, Schema, ScopedTx, Tenant, Tx, job, jobs, route, routes, table};
use std::time::Duration;

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[table(scoped_by = Org)]
struct Window {
    id: Id<Window>,
    applied: bool,
}

#[job(scoped_by = Org)]
struct ApplyWindow {
    window: u64,
}

impl ApplyWindow {
    fn run(self, tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        if let Some(mut w) = tx.get(Id::<Window>::from_raw(self.window))? {
            w.applied = true;
            tx.put(&w);
        }
        Ok(())
    }
}

#[job(cross_tenant, global)]
struct MaintenanceSweep {}

impl MaintenanceSweep {
    fn run(self, tx: &mut Tx, cap: &CrossTenant) -> Result<(), JobError> {
        let due = tx.across_tenants::<Org>(cap).scan::<Window>()?;
        for (org, window) in due {
            if !window.applied {
                tx.enqueue_in(org, ApplyWindow { window: window.id.raw() }, cap);
            }
        }
        Ok(())
    }
}

#[route(POST "/admin/sweep", auth, cross_tenant)]
fn sweep_now(tx: &mut Tx, cap: &CrossTenant) -> Result<(), ApiError> {
    let windows = tx.across_tenants::<Org>(cap).scan::<Window>()?;
    for (org, window) in windows {
        tx.enqueue_in(org, ApplyWindow { window: window.id.raw() }, cap);
    }
    Ok(())
}

fn build(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> std::io::Result<App> {
    Ok(App::open(disk, Schema::new().table::<Window>(), entropy, AppConfig::default())?
        .mount(routes![sweep_now])
        .mount_jobs(jobs![ApplyWindow, MaintenanceSweep])
        .schedule("maintenance-sweep", Duration::from_secs(3600), || {
            MaintenanceSweep {}
        }))
}

fn main() {
    let _ = build;
}
```

- [ ] **Step 7: Run, review every pin, and prove red by mutation**

```zsh
cargo test -p charpente-macros --lib
cargo test -p charpente --test kernel -- scoped_jobs
cargo test -p charpente --test kernel -- refus
cargo test -p charpente --test replication -- shape
TRYBUILD=overwrite cargo test -p charpente-macros --test trybuild
git diff --stat crates/charpente-macros/tests/compile_fail/
git diff crates/charpente-macros/tests/compile_fail/
cargo test -p charpente-macros --test trybuild
```

Expected:
- all green;
- the new `.stderr` files, each read against its source string:
  - `statuspage_job_no_tenancy`: the bare refusal naming `NotifySubscribers`, at `#[job]`;
  - `job_bare`: three refusals, naming `Bare`, `RetryOnly` and `CapabilityOnly`;
  - `job_global_and_scoped_by`: "`global` and `scoped_by = Org` on one job — …" twice, at `scoped_by` and at `global` respectively;
  - `job_duplicate_scoped_by`: "`scoped_by` given twice — write it once", at the second;
  - `job_scoped_by_missing_tenant_scope`: E0277 "`Org` is used as a tenant scope but has no `Tenant` impl", with Task 4's note, under the `Org` in the attribute. A second instance of the same diagnostic from the glue's `open::<Org>` is acceptable, and the review must say which one it saw;
  - `scoped_job_enqueued_on_bare_tx`: E0277 "`NotifySubscribers` is a tenant-scoped job (`#[job(scoped_by = Org)]`) and a bare `Tx` names no tenant to run it in", labelled "`NotifySubscribers` is scoped by `Org`", with the full note from Step 2;
  - `scoped_job_scheduled_by_cron`: the same diagnostic for `MaintenanceSweep`, at `.schedule`;
  - `scoped_tx_enqueues_another_scopes_job`: E0271, expected `Org`, found `Team`;
  - `scoped_tx_enqueue_of_a_global_job`: E0271, expected `Org`, found `Unscoped`;
  - `enqueue_in_without_capability`: `#[route]`'s undeclared-capability refusal, with `, auth` and the fix `#[route(POST "/admin/apply/:org", auth, cross_tenant)]`;
  - `scoped_job_run_takes_bare_tx`: E0308 mismatched types, expected a fn pointer taking `&mut ScopedTx<'_, '_, Org>` and found `File::run`'s `&mut Tx`.
- The changed pins, and only these:
  - `job_bad_retry.stderr`: the unknown-field text now ends with the new `ACCEPTED_FORMS`;
  - `scoped_read_from_bare_tx.stderr` and `unscoped_row_impl_from_an_app.stderr`: `NoTenant`'s new note, with its job line.

  Any other changed `.stderr` is a finding: read it before accepting it.

Then commit nothing yet. Save the implementation, apply each mutation alone, run the named test, and quote the failure. Restore with `git checkout` of the file and `git apply` of the saved patch:

```zsh
D=.superpowers/sdd/2026-09-27-job-tenancy
git diff crates/charpente/src/kernel/dispatch.rs crates/charpente-core/src/db.rs crates/charpente-macros/src/fingerprint.rs > $D/task6-impl.patch
```

1. `JobScope::open`: `Id::from_raw(0)` in place of `Id::from_raw(self.id)`. Expected: `a_scoped_job_runs_in_its_enqueuers_tenant_tenant_zero_included` fails on desk 5's row, and `a_failing_scoped_job_keeps_its_tenant_through_every_retry_to_dead` on "jammed in desk 0".
2. In `run_due_into`'s failure-record path, write `row.scope = None;` before the row is re-put. Expected: the retry test fails at attempt 1, `left: None`.
3. `ScopedTx::enqueue`: `push_enqueue(Some(0), …)` in place of `Some(scope)`. Expected: the follow-ups test fails, with `Stamp` at `Some(0)`.
4. `Tx::enqueue_in`: `push_enqueue(Some(0), …)`. Expected: the fan-out test fails, with desk 4's `File` at `Some(0)`.
5. `compute_job`: ignore `scoped_by` (return `fnv1a(fold(…).as_bytes())`). Expected: the refused-at-startup test fails at its `assert_ne`, and the fingerprint unit test fails.
6. `run_due_into`: pass `None` in place of `scope` to the glue. Expected: the tenant-0 test fails. The glue's `expect` panics, dispatch absorbs the panic as an attempt, and no `Filed` row is written.

- [ ] **Step 8: Byte identity**

```zsh
D=.superpowers/sdd/2026-09-27-job-tenancy
cargo test -p charpente --test kernel -- a_global_jobs_fingerprint_is_the_bare_jobs_fingerprint --exact
cargo test -p charpente --test tenant -- every_scoped_read_returns_only_its_own_tenants_rows --exact --nocapture 2>&1 | tee $D/tenant-swarm-task6.log | grep "^test result"
CHARPENTE_BASE_SEED=0x3E7A0927 cargo test -p charpente --test kernel -- kernel_jobs_are_exactly_once_under_the_fault_matrix --exact --nocapture 2>&1 | tee $D/kernel-swarm-task6.log | grep "^test result"
cargo test -p charpente --test realtime -- a_reconnecting_tenant_subscriber_misses_nothing_and_leaks_nothing --exact --nocapture 2>&1 | tee $D/realtime-swarm-task6.log | grep "^test result"
for f in tenant kernel realtime; do
  grep -E "^charpente-sim|^  [^ ]" $D/$f-swarm-task6.log > $D/$f-coverage-task6.txt
  diff $D/$f-coverage-base.txt $D/$f-coverage-task6.txt && echo "$f: identical"
done
```

Expected: the fingerprint pin passes, meaning `SessionSweep` and `CountUp` kept their shapes, and three `identical` lines. Every job in those universes is `global`, so it still writes `0x08` with its old fingerprint. A moved line is a finding.

- [ ] **Step 9: Docs**

**`docs/GUIDE.md` §4**, the `impl Job for SendReceipt` block: add `type Scope = Unscoped;` after `FINGERPRINT`, with the comment `// A global job: it belongs to no tenant (§6, §16).`, and add `Unscoped` to its `charpente::core::table` import.

**§6, "`#[job]` and its retry policy".** Task 1's paragraph becomes:

> A job says which tenancy it has, and neither is the default: `#[job(global)]` belongs to no tenant, and `#[job(scoped_by = Org)]` acts inside one (below, and §16). A bare `#[job]` is a compile error naming both. Either form takes `RetryPolicy::DEFAULT` (5 attempts, 30s base backoff), and adding `retry(max = <int>, backoff = "<n><unit>")` overrides either or both. `backoff` is a bare number plus a unit, one of `ms`, `s`, `m`, `h`. The words `global` or `scoped_by`, `cross_tenant` (§16) and `retry(...)` may come in any order, each at most once.

**§6, "The handler signature".** After "an inherent method with exactly this shape: `fn run(self, tx: &mut Tx) -> Result<(), JobError>`.", insert: "That is a `global` job's. A `scoped_by = Org` job's `run` takes `tx: &mut ScopedTx<'_, '_, Org>` instead (next subsection), and `cross_tenant` adds `cap: &CrossTenant` after `tx` in either form."

**§6, a new subsection after "The handler signature": "### A job inside a tenant: `scoped_by`".**

````markdown
A job that acts inside one tenant says so, and then it runs in the tenant it was
enqueued from. It is enqueued from inside that tenant's scope: `tx.enqueue` on the
`ScopedTx` a handler's `tx.scope::<Org>(org, ctx)?` returns. The enqueue records the
tenant beside the job, and when the job runs, its `run` receives that tenant's
`ScopedTx`. No `authorize` runs for it: a job has no principal, and its authority is
its enqueuer's, who was inside the tenant already. Its retries run in the same tenant,
and a follow-up it enqueues through its own `ScopedTx` inherits the tenant too.

```rust
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::kernel::JobError;
use charpente::{Id, Scoped, ScopedTx, Tenant, Tx, View, job, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, who: &Principal, scope: Id<Org>) -> Result<(), ApiError> {
        if who.subject() == format!("org:{}", scope.raw()) {
            Ok(())
        } else {
            Err(ApiError::BadRequest(format!(
                "not a member of org {}",
                scope.raw()
            )))
        }
    }
}

#[table(scoped_by = Org)]
struct Invoice {
    id: Id<Invoice>,
    receipts: u32,
}

#[table(global)]
struct Mailer {
    id: Id<Mailer>,
    sent: u64,
}

#[job(scoped_by = Org)]
struct SendReceipt {
    invoice: u64,
}

impl SendReceipt {
    fn run(self, tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        // Inside the org it was enqueued in, exactly as the handler was.
        if let Some(mut inv) = tx.get(Id::<Invoice>::from_raw(self.invoice))? {
            inv.receipts += 1;
            tx.put(&inv);
        }
        // A global table, through the deref: `run` holds `&mut ScopedTx`,
        // so it is `**tx` here where a handler writes `*tx`.
        let sent = (**tx).get(Id::<Mailer>::from_raw(0))?.map_or(0, |m| m.sent);
        (**tx).put(&Mailer {
            id: Id::from_raw(0),
            sent: sent + 1,
        });
        Ok(())
    }
}

#[route(POST "/orgs/:org/invoices/:id/receipt", auth)]
fn send_receipt(tx: &mut Tx, org: Id<Org>, id: Id<Invoice>, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.get(id)?.ok_or(ApiError::NotFound)?;
    tx.enqueue(SendReceipt { invoice: id.raw() }); // runs in this org
    Ok(())
}
```

`ScopedTx::enqueue` takes a job scoped by that same type, so a job scoped by another type
is a type error (E0271, expected `Org`, found `Team`). So is a `global` job: it is
enqueued inside a scope through the explicit deref, `(*tx).enqueue(job)` in a handler
and `(**tx).enqueue(job)` in a scoped job's `run`. A scoped job on a bare `Tx`, where no
tenant is named, is refused with the note shown in "What `#[job]` rejects". Cron takes
`global` jobs only (§7), because a tick has no tenant. A sweep that must act in every
tenant fans out instead (§7).
````

**§6, "Enqueueing: `enqueue` vs `enqueue_keyed`".** After the first sentence, add: "On a bare `Tx` these take `global` jobs. A `scoped_by` job is enqueued the same two ways on its tenant's `ScopedTx` (above)."

**§6, "Changing a job's fields".** The pinned block's line becomes:

```text
<n> Pending "<job>" job(s) (ids <ids>) were enqueued under a different shape of <job> (recorded <recorded>; this binary <fingerprint>) — this binary will not decode them; run the binary that enqueued them until they are Done or Dead, then deploy this one, or restore <job>'s old fields and its `global`/`scoped_by` declaration and give the new shape its own #[job] type under a new name (GUIDE §6)
```

After the block, add: "A job's tenancy is part of its shape. `scoped_by` is fingerprinted with the fields, so moving a job between `global` and `scoped_by`, or between two scope types, is refused the same way. A deploy therefore never hands a row enqueued with no tenant to a scoped `run`, or one enqueued in a tenant to a global one."

**§6, "What `#[job]` rejects".** Replace the section's opening with:

````markdown
### What `#[job]` rejects

Every rejection is a compile error naming the contract and the fix. The first is the
one every job meets once, and the next three are its neighbours:

<!-- pinned -->
```text
`<job>` declares no tenancy — a job says which: write `#[job(scoped_by = Org)]` if it acts inside one tenant (it is enqueued from inside that tenant's scope, and its `run` receives that tenant's `ScopedTx`), or `#[job(global)]` if it belongs to no tenant (the only kind cron can schedule). Neither is the default.
`global` and `scoped_by = <scope>` on one job — a job is one or the other; drop one
`global` given twice — write it once
`scoped_by` given twice — write it once
```

A `scoped_by` job enqueued where no tenant is named — `tx.enqueue` on a bare `Tx`, or
`App::schedule` — is an unsatisfied bound, and rustc prints this note with your job and
scope in place of `<job>` and `<scope>` (§16 shows it rendered):

<!-- pinned -->
```text
`<job>` is a tenant-scoped job (`#[job(scoped_by = <scope>)]`) and a bare `Tx` names no tenant to run it in
enqueue it from inside the tenant it acts in, and it runs in that tenant:
    let mut tx = tx.scope::<<scope>>(scope, ctx)?;
    tx.enqueue(job);
or fan it out from a `cross_tenant` route or job, naming the tenant:
    tx.enqueue_in::<<scope>>(scope, job, cap);
cron (`App::schedule`) enqueues on a bare `Tx`, so it schedules `global` jobs only
```

The rest are about the attribute's syntax and the struct's shape:
````

The `rust,ignore` examples below it keep their code. Their quoted messages change: `#[job(bogus)]`'s comment quotes the new `ACCEPTED_FORMS`, and the `backoff = "10"` comment quotes "backoff \"10\" has no unit — " followed by it.

**§7, "Cron", the introduction.** Add a paragraph:

> A tick has no tenant, so `App::schedule` takes `global` jobs only; a `scoped_by` job there is the §6 compile error. A sweep that must act in every tenant is a `global, cross_tenant` job: it reads every tenant through the `CrossTenant` it declared, and fans a `scoped_by` job out into each tenant it names with `tx.enqueue_in(org, job, cap)`. Each fanned-out job then runs in its own tenant, exactly as if enqueued from inside it.

After "Registering a schedule", add a compiled block showing that pattern:

```rust
use charpente::auth::Principal;
use charpente::http::{ApiError, App, AppConfig};
use charpente::kernel::JobError;
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::{CrossTenant, Id, Schema, ScopedTx, Tenant, Tx, job, jobs, table};
use std::time::Duration;

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        // Never asked by the sweep: a job has no principal.
        Err(ApiError::Unauthorized)
    }
}

#[table(scoped_by = Org)]
struct Window {
    id: Id<Window>,
    applied: bool,
}

/// Acts inside one org, the one the sweep named when it fanned this out.
#[job(scoped_by = Org)]
struct ApplyWindow {
    window: u64,
}

impl ApplyWindow {
    fn run(self, tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        if let Some(mut w) = tx.get(Id::<Window>::from_raw(self.window))? {
            w.applied = true;
            tx.put(&w);
        }
        Ok(())
    }
}

/// Reads every org's windows and fans one `ApplyWindow` into each window's org.
#[job(global, cross_tenant)]
struct MaintenanceSweep {}

impl MaintenanceSweep {
    fn run(self, tx: &mut Tx, cap: &CrossTenant) -> Result<(), JobError> {
        let windows = tx.across_tenants::<Org>(cap).scan::<Window>()?;
        for (org, window) in windows {
            if !window.applied {
                tx.enqueue_in(org, ApplyWindow { window: window.id.raw() }, cap);
            }
        }
        Ok(())
    }
}

fn build_app(data_dir: &str) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(ProdDisk::new(data_dir)?),
        Schema::new().table::<Window>(),
        Box::new(ProdEntropy::new()?),
        AppConfig::default(),
    )?
    .mount_jobs(jobs![ApplyWindow, MaintenanceSweep])
    .schedule("maintenance-sweep", Duration::from_secs(3600), || {
        MaintenanceSweep {}
    }))
}
```

**§9, "`#[job]` retry-syntax and shape violations".** Rename the heading to "`#[job]` tenancy, retry-syntax and shape violations (`charpente-macros/src/job.rs`, compile time)". Replace its text block with:

```text
`NotifySubscribers` declares no tenancy — a job says which: write `#[job(scoped_by = Org)]` if it acts inside one tenant (it is enqueued from inside that tenant's scope, and its `run` receives that tenant's `ScopedTx`), or `#[job(global)]` if it belongs to no tenant (the only kind cron can schedule). Neither is the default.
`global` and `scoped_by = Org` on one job — a job is one or the other; drop one
`global` given twice — write it once
`scoped_by` given twice — write it once
`cross_tenant` given twice — write it once
`retry(...)` given twice — write it once
a job declares `global` or `scoped_by = <Type>`, and may add `cross_tenant` and `retry(max = <int>, backoff = "<n><unit>")` (unit is one of ms, s, m, h), in any order — e.g. `#[job(scoped_by = Org, retry(max = 3, backoff = "10s"))]` or `#[job(global, cross_tenant)]`
backoff "10" has no unit — a job declares `global` or `scoped_by = <Type>`, and may add `cross_tenant` and `retry(max = <int>, backoff = "<n><unit>")` (unit is one of ms, s, m, h), in any order — e.g. `#[job(scoped_by = Org, retry(max = 3, backoff = "10s"))]` or `#[job(global, cross_tenant)]`
unknown retry field `jitter` — a job declares `global` or `scoped_by = <Type>`, and may add `cross_tenant` and `retry(max = <int>, backoff = "<n><unit>")` (unit is one of ms, s, m, h), in any order — e.g. `#[job(scoped_by = Org, retry(max = 3, backoff = "10s"))]` or `#[job(global, cross_tenant)]`
`max` given twice — write it once
`backoff` given twice — write it once
backoff "12xs": "12x" is not a whole number — a job declares `global` or `scoped_by = <Type>`, and may add `cross_tenant` and `retry(max = <int>, backoff = "<n><unit>")` (unit is one of ms, s, m, h), in any order — e.g. `#[job(scoped_by = Org, retry(max = 3, backoff = "10s"))]` or `#[job(global, cross_tenant)]`
backoff "99999999999999h" overflows u64 milliseconds — a job declares `global` or `scoped_by = <Type>`, and may add `cross_tenant` and `retry(max = <int>, backoff = "<n><unit>")` (unit is one of ms, s, m, h), in any order — e.g. `#[job(scoped_by = Org, retry(max = 3, backoff = "10s"))]` or `#[job(global, cross_tenant)]`
#[job] applies to structs — enums and unions cannot be job payloads
#[job] structs cannot be generic — a job is one concrete payload shape; make `Baz` non-generic
#[job] needs named fields — `Baz` must be a braced struct
```

Replace the prose paragraph after it (from "Anything other than bare `#[job]`" to "only structs, only non-generic, only named fields.") with:

> The first is the one every job meets: a job declares its tenancy, and a bare `#[job]` names both answers. The next five refuse a word given twice or both tenancies at once. Anything else the attribute does not recognise hits the accepted-forms message, which lists every word, and the four after it are that message with more specific context prepended: a backoff string missing its unit, an unrecognized field inside `retry(...)`, `max`/`backoff` written twice, a backoff whose number doesn't parse, or one so large it overflows `u64` milliseconds. The last three are the struct-shape checks `#[table]` makes (§2): only structs, only non-generic, only named fields. A scoped job enqueued where no tenant is named, or scheduled by cron, is a trait-bound error rather than a macro one; §6 pins its note.

**§12, "Versioning a table".** After "…so respelling a field's type is a shape change, and a compiler upgrade never is.", insert: "A job's `scoped_by` path is fingerprinted the same way, as written, so a change of a job's tenancy is a shape change (§6)."

**§16, the introduction's bullet list.** Add:

> - `#[job(scoped_by = Org)]` on every job that acts inside one tenant, and `#[job(global)]` on every job that belongs to none. A scoped job's `run` receives the `ScopedTx` of the tenant it was enqueued from.

**§16, "The compile errors, so you recognize them".**
- Change "Both are rendered below" to "Each is rendered below".
- The first rendered block's note lines become:

```text
   = note: open the tenant scope first — in a handler, `Org`'s own `Tenant::authorize` runs there for a credentialed caller:
               let tx = tx.scope::<Org>(scope, ctx)?;
               tx.scan::<Invoice>()?
           in a job, declare `#[job(scoped_by = Org)]` and read through the `ScopedTx` its `run` receives
```

- After the `scoped_by`-with-no-`impl Tenant` block, add:

````markdown
A scoped job enqueued on a bare `Tx`:

```text
error[E0277]: `NotifySubscribers` is a tenant-scoped job (`#[job(scoped_by = Org)]`) and a bare `Tx` names no tenant to run it in
  --> src/incidents.rs:44:16
   |
44 |     tx.enqueue(NotifySubscribers { incident: id.0 });
   |        ------- ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ `NotifySubscribers` is scoped by `Org`
   |
   = note: enqueue it from inside the tenant it acts in, and it runs in that tenant:
               let mut tx = tx.scope::<Org>(scope, ctx)?;
               tx.enqueue(job);
           or fan it out from a `cross_tenant` route or job, naming the tenant:
               tx.enqueue_in::<Org>(scope, job, cap);
           cron (`App::schedule`) enqueues on a bare `Tx`, so it schedules `global` jobs only
```
````

  Copy the header, the span lines and the label from `scoped_job_enqueued_on_bare_tx.stderr` as it was pinned, with rustc's `help:` chains trimmed as the section's opening says. The block above shows the expected shape; the `.stderr` is the authority on the exact bytes.

**§16, "Reading across every tenant".**
- Replace "It is also read-only: there is no `put` or `delete` on the cross-tenant handle." with "The handle itself is read-only: there is no `put` or `delete` on it. The one thing a declared route or job does beyond reading is fan out: `tx.enqueue_in::<Org>(org, job, cap)` enqueues a `scoped_by = Org` job into the org it names, where it runs as if enqueued from inside (§7). So the capability is reads, plus fan-out enqueue, and `describe`'s `cross_tenant=true` inventory covers both."
- In the fourth bullet, "lists every endpoint and job that can read across tenants" becomes "lists every endpoint and job that can read across tenants or fan out into them".

**§16, "Where the guarantee stops".**
- "A route handler and a job receive a `Tx`; through a `Tx`, a scoped table is unreachable without `tx.scope::<Org>(org, ctx)?`." becomes "A route handler receives a `Tx`, and a scoped job the `ScopedTx` of the tenant it was enqueued in; through a `Tx`, a scoped table is unreachable without `tx.scope::<Org>(org, ctx)?`."
- Task 2's operator paragraph opens with "**Code outside a handler never takes a scope on its own word.** A job declares `#[job(scoped_by = Org)]` and runs in the tenant its enqueuer opened (§6). **Operator code that must act inside one tenant** — seeding, …" and continues as Task 2 wrote it.

**`DESIGN.md` §3, the Jobs bullet.** Replace it with:

> - **Jobs.** `#[job(global)]` or `#[job(scoped_by = Org)]` structs, with typed payloads and a typed retry policy. Neither tenancy is the default, and a bare `#[job]` is a compile error naming both. A `global` job's `run` takes `&mut Tx`; a scoped job's takes the `ScopedTx` of the tenant it was enqueued in, which the dispatcher reopens from the tenant its record carries (§6). A scoped job is enqueued only from inside its tenant (`ScopedTx::enqueue`) or by a declared fan-out (`Tx::enqueue_in` with a `CrossTenant`). `Tx::enqueue` and cron take `global` jobs only, and a scoped job at either is an unsatisfied bound whose text names the fix. Enqueue sites and handler signatures are checked against each other (spec 2026-09-27-job-tenancy §5).

**`DESIGN.md` §6.**
- Queue: "a `#[job]` handler is `fn run(self, tx: &mut Tx) -> Result<(), JobError>` — itself a transaction." becomes "a `global` job's handler is `fn run(self, tx: &mut Tx) -> Result<(), JobError>` — itself a transaction — and a `scoped_by = Org` job's takes `&mut ScopedTx<Org>` instead. The enqueue records the tenant (record tag `0x0a`, `JobRow.scope`), and the dispatcher reopens exactly that tenant with its own `TenantAdmission` through a `JobScope` only it constructs, so a scoped job runs where it was enqueued, retries included."
- Queue: after "Every enqueue records the payload type's structural fingerprint (§3).", add "A scoped job's fingerprint also covers its `scoped_by`, so a change of tenancy is refused like a change of fields."
- Cron: after "enqueues through the normal path", add "— on a bare `Tx`, so a schedule takes `global` jobs only; a sweep that acts per tenant is a `global, cross_tenant` job that fans out with `enqueue_in`".

**`DESIGN.md` §8, the Multi-tenancy bullet.** After Task 3's sentence ending "(spec 2026-09-27-job-tenancy §4).", add: "A job acts inside a tenant only by declaring `#[job(scoped_by = Org)]`, and then only inside the tenant its enqueuer opened or its fan-out named."

**`DECISIONS.md` §12.** Replace the remedy clause "with the remedy: code outside a handler that wants the guarantee should go through a transaction and `tx.scope::<Org>(org, ctx)?` exactly as a handler does." with spec §14's text, verbatim:

> with the remedy: code outside a handler never takes a scope on its own word. A job declares `#[job(scoped_by = Org)]` and runs in the tenant its enqueuer opened; operator code that must act inside one tenant — seeding, a test oracle — opens it with a `TenantAdmission` minted from a `Group`, the seal `CrossTenant` already carries, which is the operator's standing made explicit rather than an exception to the guarantee. *(Corrected 2026-09-27, job-tenancy spec: the earlier remedy named a `Ctx` that no code outside a handler holds.)*

**`README.md`.**
- In the "What it looks like" example, after `Invoice`, add:

  ```rust
  #[job(scoped_by = Org)]
  struct SendReceipt {
      invoice: Id<Invoice>,
  }
  ```

  The enqueue line's comment becomes `// job enqueued IN the same transaction; it runs in this org`.
- "Note also what's declared: the route says `auth`, the table says which tenant owns its rows, and a route or table that says neither does not compile." becomes "Note also what's declared: the route says `auth`, the table says which tenant owns its rows, the job says it acts inside that tenant, and a route, table or job that says none of this does not compile."
- In the "since 2026-09-16" sentence (`:140-143`), Task 3's text becomes "a route says `auth`, `public` or `public(Org)`, a table and a job say `scoped_by` or `global`, and none is a default."

**`docs/superpowers/specs/2026-09-16-default-secure-surfaces-design.md` §8**, after the item "**A `#[job]` change.**":

> **Superseded 2026-09-2x** by `2026-09-27-job-tenancy-design.md` §5: a job declares `global` or `scoped_by = Org`, and neither is the default.

**`docs/superpowers/specs/2026-08-30-tenant-scoping-design.md`**, after ruling 3:

> **Read 2026-09-2x** under `2026-09-27-job-tenancy-design.md` ruling R10: the platform carries an app-named `Id<S>` and hands it back as the same type — a job's tenant, named at its enqueuer's `tx.scope` line or in `enqueue_in`'s argument — and never derives one.

**`crates/guide-examples/tests/doc_truth.rs`.** `const PINNED_FLOOR: usize = 24;` (was 22): §6 gained two pinned blocks.

```zsh
cargo build -p guide-examples
cargo test -p guide-examples
```

Expected: both green. If `doc_truth` names a fragment of the `NoJobTenant` block, the pinned line and the `on_unimplemented` source disagree. Fix the guide line to the source, never the source to the guide. If the checker splits `::<<scope>>` differently than intended, a line of source text it cannot find will say so. Then write that line so the placeholder stands alone, and record the change in the task report.

- [ ] **Step 10: Gate and commit**

Run the gate and count it (87 of 87). Re-check `git status` and `git log -1`. Every path this task names is staged by name; the two fixture directories are staged whole, after `git diff --stat` confirms they hold only this task's files:

```zsh
git status --short
git add crates/charpente-core/src/db.rs crates/charpente-core/src/table.rs \
  crates/charpente-macros/src/job.rs crates/charpente-macros/src/fingerprint.rs crates/charpente-macros/src/route.rs \
  crates/charpente/src/kernel/dispatch.rs crates/charpente/src/kernel/mod.rs crates/charpente/src/http/app.rs \
  crates/charpente/src/tenant.rs crates/charpente/src/serve.rs crates/charpente/src/launch.rs \
  crates/charpente/src/repl/probe.rs crates/charpente/src/repl/mod.rs \
  crates/charpente-core/tests/materialize.rs crates/charpente/tests/transactions.rs crates/charpente/tests/kernel.rs \
  crates/charpente/tests/http.rs crates/charpente/tests/replication.rs \
  examples/invoicing/examples/seed_foreign_job.rs \
  crates/charpente-macros/tests/compile_fail crates/charpente-macros/tests/compile_pass \
  crates/guide-examples/tests/doc_truth.rs docs/GUIDE.md DESIGN.md DECISIONS.md README.md \
  docs/superpowers/specs/2026-09-16-default-secure-surfaces-design.md \
  docs/superpowers/specs/2026-08-30-tenant-scoping-design.md
git commit -m "feat(job-tenancy): #[job(scoped_by = Org)] runs in its enqueuer's tenant — ScopedTx::enqueue, declared fan-out, the dispatcher's JobScope; a bare #[job] is refused"
```

---

### Task 7: `describe` names the scopes — `public_scope` and `scoped_by`

**Files:**
- Modify `crates/charpente/src/describe.rs`: `RouteInfo.public_scope`, `JobInfo.scoped_by`, `route_info`, `job_info`, `to_human`.
- Modify `crates/charpente-cli/src/genclient.rs`: the mirror's `RouteInfo`/`JobInfo`, `into_route`/`into_job`, and `EVERY_SHAPE`.
- Modify `crates/charpente-tsgen/tests/common/mod.rs` (the five `RouteInfo` literals at ~63, 85, 98, 119, 144), `crates/charpente-tsgen/tests/routes.rs` (~460, 572, 615) and `crates/charpente-tsgen/tests/revive.rs` (~61, 863).
- Test:
  - `crates/charpente/tests/describe.rs`: the capability app gains a `public(Org)` route and a scoped job, and gains two tests; re-pin its golden and its two `to_human` pins;
  - `examples/invoicing/tests/describe_golden.rs`: the golden;
  - `crates/charpente-cli/tests/describe.rs`: `GOLDEN`.
- Docs:
  - `docs/GUIDE.md` §13 (the introduction and the JSON), §16 (the `describe` excerpt);
  - `DESIGN.md` §7's **Introspection** bullet and §8's cross-tenant sentence.

**Interfaces:**
- Consumes:
  - Task 3: `Route.public_scope: Option<PublicScope>`, where `PublicScope { name: &'static str, type_id }`.
  - Task 6: `JobDef.scoped_by: Option<&'static str>`.
- Produces (spec §8):
  - `RouteInfo.public_scope: Option<&'static str>`, serialized after `auth`.
  - `JobInfo.scoped_by: Option<&'static str>`, serialized after `retry`.
  - JSON renders each as a name or `null`.
  - The human dump renders `public_scope=Org|none` after `auth=`, and `scoped_by=Org|none` after `backoff_nanos=`.

- [ ] **Step 1: Write the tests**

In `crates/charpente/tests/describe.rs`, add `Scoped, ScopedTx` to the `use charpente::{…}` list at the top (line 16). Then, in the capability section (~1100), after `admin_plain`, add:

```rust
/// `public(Org)` (job-tenancy spec §4): every caller may act inside the org
/// this handler opens, with no `authorize`. `describe` must name the scope,
/// or the audit "who reaches a tenant without a membership check" is a grep
/// again.
#[route(GET "/orgs/:org/ledger", public(Org))]
fn org_ledger(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(WireU64(tx.scan::<Ledger>()?.len() as u64))
}
```

After `Rollup`'s impl, add:

```rust
/// A job that acts inside one org (job-tenancy spec §5): `describe` names
/// the scope it acts in.
#[job(scoped_by = Org)]
struct Reconcile {
    day: u64,
}

impl Reconcile {
    fn run(self, tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        let _ = (self.day, tx.scan::<Ledger>()?);
        Ok(())
    }
}
```

`capability_app` mounts `routes![admin_revenue, admin_plain, org_ledger]` and `jobs![Rollup, Ping, Reconcile]`.

`describe_names_exactly_the_cross_tenant_routes` keeps its property, and its floor grows with the app:
- `all` becomes `vec!["admin_revenue", "admin_plain", "org_ledger"]`;
- `jobs` becomes `vec![("SessionSweep", false), ("Rollup", true), ("Ping", false), ("Reconcile", false)]`.

`declared` stays `vec!["admin_revenue"]`: `public(Org)` is not `cross_tenant`, and that is part of what the test pins.

`to_human_carries_the_cross_tenant_flag`'s two literals gain `public_scope=none ` after `auth=false `.

Add after it:

```rust
/// Job-tenancy spec §8: `describe` names, by type, the scope a `public(S)`
/// route admits every caller to and the scope a `scoped_by` job acts in.
/// The `None`s are the floor: without them, a field that always said "Org"
/// would pass.
#[test]
fn describe_names_every_public_scope_route_and_scoped_job() {
    let c = capability_app().describe();
    let routes: Vec<(&str, bool, Option<&str>)> = c
        .routes
        .iter()
        .map(|r| (r.name, r.auth, r.public_scope))
        .collect();
    assert_eq!(
        routes,
        vec![
            ("admin_revenue", false, None),
            ("admin_plain", false, None),
            ("org_ledger", false, Some("Org")),
        ],
        "`auth=false public_scope=none` is plain public; `public_scope=Org` admits every \
         caller to Org"
    );
    let jobs: Vec<(&str, Option<&str>)> = c.jobs.iter().map(|j| (j.name, j.scoped_by)).collect();
    assert_eq!(
        jobs,
        vec![
            ("SessionSweep", None),
            ("Rollup", None),
            ("Ping", None),
            ("Reconcile", Some("Org")),
        ]
    );
}

/// The human dump carries both, in spec §8's positions: `public_scope`
/// after `auth`, and `scoped_by` after the retry policy.
#[test]
fn to_human_carries_public_scope_and_scoped_by() {
    let out = charpente::describe::to_human(&capability_app().describe());
    for line in [
        "  GET /orgs/:org/ledger name=org_ledger auth=false public_scope=Org cross_tenant=false \
         stream=false\n",
        "  GET /admin/plain name=admin_plain auth=false public_scope=none cross_tenant=false \
         stream=false\n",
        "  Reconcile max=5 backoff_nanos=30000000000 scoped_by=Org cross_tenant=false \
         builtin=false\n",
        "  Rollup max=5 backoff_nanos=30000000000 scoped_by=none cross_tenant=true \
         builtin=false\n",
    ] {
        assert!(out.contains(line), "missing {line:?} in:\n{out}");
    }
}
```

- [ ] **Step 2: Run them at the Task 6 commit, and record the red**

```zsh
cargo test -p charpente --test describe 2>&1 | grep -E "^error\[E" | sort | uniq -c
```

Expected: E0609 "no field `public_scope` on type `&RouteInfo`", and the same for `scoped_by` on `&JobInfo`. A red by compile error is the honest red for a field that does not exist yet. Step 5 proves the tests red against a wrong value too.

- [ ] **Step 3: Implement**

`crates/charpente/src/describe.rs`, in `RouteInfo`, after `auth`:

```rust
    /// `#[route(…, public(Org))]`: the tenant scope this route admits every
    /// caller to, with no `authorize` (job-tenancy spec §4), by the scope
    /// type's name. `None` for `auth` and plain `public`. Beside `auth`
    /// because together they answer "who reaches what here": `auth=false`
    /// with `None` is plain public, and with `Some` it admits everyone to
    /// that tenant.
    pub public_scope: Option<&'static str>,
```

In `JobInfo`, after `retry`:

```rust
    /// `#[job(scoped_by = Org)]`: the tenant scope this job acts in, by the
    /// scope type's name; `None` for a `global` job (job-tenancy spec §8).
    pub scoped_by: Option<&'static str>,
```

In `route_info`, after `auth: route.auth,`: `public_scope: route.public_scope.map(|s| s.name),`. In `job_info`, after the `retry` block: `scoped_by: def.scoped_by,`.

In `to_human`, the route line becomes:

```rust
        writeln!(
            out,
            "  {} {} name={} auth={} public_scope={} cross_tenant={} stream={}",
            r.method,
            r.pattern,
            r.name,
            r.auth,
            r.public_scope.unwrap_or("none"),
            r.cross_tenant,
            r.stream
        )
        .unwrap();
```

and the job line becomes:

```rust
        writeln!(
            out,
            "  {} max={} backoff_nanos={} scoped_by={} cross_tenant={} builtin={}",
            j.name,
            j.retry.max,
            j.retry.backoff_nanos,
            j.scoped_by.unwrap_or("none"),
            j.cross_tenant,
            j.builtin
        )
        .unwrap();
```

`none` follows `fingerprint=none`'s precedent in the tables line.

`crates/charpente-cli/src/genclient.rs`:
- the mirror `RouteInfo` gains `public_scope: Option<String>,` after `auth`, and `JobInfo` gains `scoped_by: Option<String>,` after `retry`;
- `into_route` gains `public_scope: r.public_scope.map(leak),` and `into_job` gains `scoped_by: j.scoped_by.map(leak),`;
- in `EVERY_SHAPE`, the `create_widget` route gains `"public_scope": null,`, the `feed` route gains `"public_scope": "Org",`, and the `SendMail` job gains `"scoped_by": "Org",`. Each sits after `"auth"` or after `"retry"`, matching serialization order. One `null` and one name per field, so the round-trip covers both spellings.

The mirror uses plain `Option<String>`, as the mirror's `request` and `fingerprint` already do. serde then reads a missing field as `None`, as it does for those two. Making the mirror strict about optional fields is its own change, and not this one.

`crates/charpente-tsgen/tests/`: each `RouteInfo { … }` literal gains `public_scope: None,` after `auth`. There are ten: `common/mod.rs` ×5, `routes.rs` ×3 and `revive.rs` ×2. The compiler names any that are missed.

- [ ] **Step 4: Re-pin the goldens, by insertion, and count it**

The four JSON goldens gain exactly one line per route and one per job, and nothing else. They are rewritten by one mechanical insertion, then counted, then checked by the tests that own them. This is not an overwrite-from-output step, so a golden cannot quietly absorb an unrelated change:

```zsh
for f in docs/GUIDE.md examples/invoicing/tests/describe_golden.rs \
         crates/charpente-cli/tests/describe.rs crates/charpente/tests/describe.rs; do
  perl -0pi -e 's/^( *)"auth": (true|false),\n/$&$1"public_scope": null,\n/mg; s/^( *)"retry": \{\n(?:.*\n)*?\1\},\n/$&$1"scoped_by": null,\n/mg' "$f"
done
git diff --numstat docs/GUIDE.md examples/invoicing/tests/describe_golden.rs \
  crates/charpente-cli/tests/describe.rs crates/charpente/tests/describe.rs
```

Expected numstat: `16 0` for each of GUIDE.md, `describe_golden.rs` and the CLI's `describe.rs`, which is 12 routes plus 4 jobs each. `crates/charpente/tests/describe.rs` shows 6 golden insertions (4 routes plus 2 jobs) on top of Step 1's additions. Read that file's diff hunk by hunk to separate the two.

Every golden route is `auth` or plain `public`, and every golden job is `global` (Task 1 swept them), so every inserted value is `null`. A test failing on a `null` that should be a name means that assumption broke: read it, never patch the value in.

`crates/charpente/tests/describe.rs`'s two `to_human` pins, `to_human_pins_a_known_routes_type_line` (~510) and `to_human_pins_a_stream_routes_emits` (~948), gain `public_scope=none ` after `auth=false `.

- [ ] **Step 5: Run, then prove red by mutation**

```zsh
cargo test -p charpente --test describe
cargo test -p invoicing --test describe_golden
cargo test -p charpente-cli --bin charpente genclient
cargo test -p charpente-cli --test describe
cargo test -p charpente-tsgen
cargo test -p ts-fixture-app
cargo test -p guide-examples --test describe_contract
```

Expected: all green, and `ts-fixture-app`'s staleness diff unmoved (spec §8: the TS client does not read either field).

Save the implementation with `git diff crates/charpente/src/describe.rs > .superpowers/sdd/2026-09-27-job-tenancy/task7-impl.patch`. Apply each mutation alone and restore it with `git checkout crates/charpente/src/describe.rs && git apply` of the patch:
1. `route_info`: `public_scope: None`. Expected: `describe_names_every_public_scope_route_and_scoped_job` fails on `org_ledger`, and `to_human_carries_public_scope_and_scoped_by` on its first line.
2. `job_info`: `scoped_by: None`. Expected: both fail on `Reconcile`.
3. `to_human`: print `r.auth` in `public_scope`'s place. Expected: the human test fails. This pins the renderer's operand, not only its presence.

- [ ] **Step 6: Docs**

**`docs/GUIDE.md` §13, the introduction.** "every route (method, pattern, name, whether it requires auth, whether it's an SSE stream)" becomes "every route (method, pattern, name, whether it requires auth, the tenant scope it admits every caller to if it is declared `public(Org)`, whether it declared `cross_tenant`, whether it's an SSE stream)". "every job (name, retry policy)" becomes "every job (name, retry policy, the tenant scope it acts in if it is `scoped_by`, whether it declared `cross_tenant`)". The JSON block was re-pinned in Step 4.

**§16, the `describe` excerpt.**
- The four lines gain their fields:
  ```text
    GET /admin/revenue name=admin_revenue auth=true public_scope=none cross_tenant=true stream=false
  ```
  ```text
    GET /memos/:org name=count_memos auth=true public_scope=none cross_tenant=false stream=false
  ```
  ```text
    Rollup max=2 backoff_nanos=5000000000 scoped_by=none cross_tenant=true builtin=false
  ```
- After "which is what makes \"audit the cross-tenant reads\" a machine check.", add:

  > The same dump carries the other two tenancy declarations beside it: a route's `public_scope` names the tenant a `public(Org)` route admits every caller to, and `none` otherwise, while a job's `scoped_by` names the tenant it acts in. So "who reaches a tenant without a membership check" and "what acts inside one" are inventories too, in JSON as a name or `null`.

**`DESIGN.md` §7, Introspection.**
- "whether it declared `auth` (`false` is a declared `public`, never an omission)," becomes "whether it declared `auth` (`false` is a declared `public`, never an omission), **the scope a `public(Org)` route admits every caller to (`public_scope`, a type name or `null`)**,".
- "jobs (name, retry policy, `cross_tenant`)" becomes "jobs (name, retry policy, **`scoped_by`, the scope type a job acts in or `null`**, `cross_tenant`)".

**`DESIGN.md` §8.** "and `charpente describe` lists every route and job that declared it — an inventory, not a grep." becomes "and `charpente describe` lists every route and job that declared it — an inventory, not a grep — beside each route's `public_scope` and each job's `scoped_by`."

```zsh
cargo build -p guide-examples
cargo test -p guide-examples
```

Expected: both green.

- [ ] **Step 7: Gate and commit**

Run the gate and count it (87 of 87). Re-check `git status` and `git log -1`. Then:

```zsh
git add crates/charpente/src/describe.rs crates/charpente-cli/src/genclient.rs \
  crates/charpente-tsgen/tests/common/mod.rs crates/charpente-tsgen/tests/routes.rs crates/charpente-tsgen/tests/revive.rs \
  crates/charpente/tests/describe.rs examples/invoicing/tests/describe_golden.rs crates/charpente-cli/tests/describe.rs \
  docs/GUIDE.md DESIGN.md
git commit -m "feat(job-tenancy): describe names public_scope and scoped_by — who reaches a tenant, and what acts inside one, by type"
```

---

### Task 8: Oracle 4 — every job runs in the tenant it was enqueued from, under the fault matrix

**Files:**
- Test: `crates/charpente/tests/kernel.rs` — a new `mod job_tenancy` at the end of the file.
- No production code. If this task finds a bug, the fix gets its own failing seed first (hard rule 3). The fix is a separate commit ahead of this one, and it is reported to the controller before it is written.

**Interfaces:**
- Consumes:
  - Task 2: `Group::tenant_admission`, `Tx::admit_scope`.
  - Task 3: `public(Shop)`, and the anonymous-scope 401.
  - Task 4: `authorize(…, who: &Principal, …)` and `Principal::subject()`.
  - Task 5: `JobRow.scope`.
  - Task 6: `#[job(scoped_by = …)]`, `ScopedTx::enqueue`, `Tx::enqueue_in`, cron's `global`-only bound.
- Produces: the swarm property `job_tenancy::every_job_runs_in_the_tenant_it_was_enqueued_from`, its base seed `0x70B5_0001`, its universe count N, and the coverage keys `tenant:job_multi_tenant_write`, `tenant:job_run_after_crash`, `tenant:job_fan_out` and `tenant:anonymous_refused`. Task 9 and the LEDGER quote them.

The workload is spec §9 item 4's, in the idioms of the two swarms it extends. The universe loop follows `kernel_workload` in this file: three rounds of crash, reopen, oracle, requests, kernel, restart, then a quiet final check. The content-derived marker and the shared oracle with naive victims follow `tenant.rs`'s `ledger_marker` and `only_own_rows`.

- **The marker.** Every row and every job payload carries `marker = shop << 32 | nonce`, with `nonce = seq << 3 | route << 1 | stripped`. A row or job therefore says, by its content, which shop's request produced it, through which route, and whether that request carried a credential.
- **The requests.** Row ids collide across shops (`nonce % IDS`), so a leak is expressible. Every request is sent twice: once with its shop's API key, and once stripped.
- **The two properties.** A scoped job's `JobRow.scope` must equal its marker's shop, and every job-written row must lie under its marker's shop. A stripped request's nonce may appear in the store only when it came through `public(Shop)`.

- [ ] **Step 1: Write the workload, the oracle and the property**

Append to `crates/charpente/tests/kernel.rs`:

```rust
// ------------------------------------------ job tenancy under the fault matrix

/// Job-tenancy spec §9 item 4: tenant isolation, extended to jobs. The
/// workload has its own tables, jobs and base seed, so no existing universe
/// is re-keyed (spec §9 item 7).
///
/// Every row and job payload carries a content-derived marker (the
/// `ledger_marker` idiom): `shop << 32 | nonce`, with
/// `nonce = seq << 3 | route << 1 | stripped`. So a row found under the wrong
/// shop, a job run in the wrong shop, and a credential-free write that
/// should have been refused are each detectable by content alone.
mod job_tenancy {
    use super::*;
    use charpente::auth::{ApiKey, hash_token};
    use charpente::{CrossTenant, ScopedTx, TenantAdmission};

    /// Shops in play, and the row-id space they share. Ids COLLIDE across
    /// shops: a leak is only expressible when two shops hold the same id.
    const SHOPS: u64 = 4;
    const IDS: u64 = 5;

    const ROUTE_ORDER: u64 = 0; // `auth`
    const ROUTE_WALKIN: u64 = 1; // `public(Shop)`
    const ROUTE_PEEK: u64 = 2; // plain `public`
    const PATHS: [&str; 3] = ["/shop/order", "/shop/walkin", "/shop/peek"];

    fn nonce(seq: u64, route: u64, stripped: bool) -> u64 {
        (seq << 3) | (route << 1) | u64::from(stripped)
    }

    fn marker(shop: u64, nonce: u64) -> u64 {
        (shop << 32) | nonce
    }

    fn marker_shop(marker: u64) -> u64 {
        marker >> 32
    }

    fn marker_nonce(marker: u64) -> u64 {
        marker & 0xFFFF_FFFF
    }

    fn nonce_route(nonce: u64) -> u64 {
        (nonce >> 1) & 3
    }

    fn nonce_stripped(nonce: u64) -> bool {
        nonce & 1 == 1
    }

    fn key_of(shop: u64) -> String {
        format!("ck_shop_{shop}")
    }

    fn subject_of(shop: u64) -> String {
        format!("shop-{shop}")
    }

    /// What a failing `Fulfil` records. It names the shop its `run` was
    /// handed, so every retry's `last_error` says which tenant that attempt
    /// ran in.
    fn jammed(marker: u64, shop: u64) -> String {
        format!("fulfil {marker:#x} jammed in shop {shop}")
    }

    /// A scope type needs no table of its own: `Org` above is the precedent.
    struct Shop;

    impl Tenant for Shop {
        type Who = Principal;
        type Error = ApiError;
        /// Membership: a shop's own key, and no other.
        fn authorize(_tx: &Tx, who: &Principal, scope: Id<Shop>) -> Result<(), ApiError> {
            if who.subject() == subject_of(scope.raw()) {
                Ok(())
            } else {
                Err(ApiError::BadRequest(format!(
                    "{} is not a member of shop {}",
                    who.subject(),
                    scope.raw()
                )))
            }
        }
    }

    /// Written by a route, inside the shop the request named.
    #[table(scoped_by = Shop)]
    struct Order {
        id: Id<Order>,
        marker: u64,
    }

    /// Written by `Fulfil`, in the shop its row carries.
    #[table(scoped_by = Shop)]
    struct Shipment {
        id: Id<Shipment>,
        marker: u64,
    }

    /// Written by `Notify`, a scoped follow-up `Fulfil` enqueues.
    #[table(scoped_by = Shop)]
    struct Notice {
        id: Id<Notice>,
        marker: u64,
    }

    /// Written by `Restock`, which the cron sweep fans out.
    #[table(scoped_by = Shop)]
    struct Restocked {
        id: Id<Restocked>,
        marker: u64,
    }

    #[derive(WireDerive, Serialize, Deserialize)]
    struct Place {
        shop: WireU64,
        nonce: WireU64,
    }

    /// Every route's body: open the named shop, write a marked order, and
    /// enqueue its fulfilment inside the shop.
    fn place(tx: &mut Tx, body: Place, ctx: &Ctx) -> Result<WireU64, ApiError> {
        let (shop, nonce) = (body.shop.0, body.nonce.0);
        let mut tx = tx.scope::<Shop>(Id::from_raw(shop), ctx)?;
        let marker = marker(shop, nonce);
        tx.put(&Order {
            id: Id::from_raw(nonce % IDS),
            marker,
        });
        tx.enqueue(Fulfil { marker });
        Ok(WireU64(nonce))
    }

    #[route(POST "/shop/order", auth)]
    fn order(tx: &mut Tx, body: Place, ctx: &Ctx) -> Result<WireU64, ApiError> {
        place(tx, body, ctx)
    }

    #[route(POST "/shop/walkin", public(Shop))]
    fn walkin(tx: &mut Tx, body: Place, ctx: &Ctx) -> Result<WireU64, ApiError> {
        place(tx, body, ctx)
    }

    #[route(POST "/shop/peek", public)]
    fn peek(tx: &mut Tx, body: Place, ctx: &Ctx) -> Result<WireU64, ApiError> {
        place(tx, body, ctx)
    }

    /// Fails every attempt for a fifth of its nonces, so retries, and the
    /// death that ends them, run under the fault matrix. Hands a third of its
    /// nonces a scoped follow-up.
    #[job(scoped_by = Shop, retry(max = 2, backoff = "1s"))]
    struct Fulfil {
        marker: u64,
    }

    impl Fulfil {
        fn run(self, tx: &mut ScopedTx<'_, '_, Shop>) -> Result<(), JobError> {
            let nonce = marker_nonce(self.marker);
            if nonce.is_multiple_of(5) {
                return Err(JobError::app(jammed(self.marker, tx.scope().raw())));
            }
            tx.put(&Shipment {
                id: Id::from_raw(nonce % IDS),
                marker: self.marker,
            });
            if nonce.is_multiple_of(3) {
                tx.enqueue(Notify {
                    marker: self.marker,
                });
            }
            Ok(())
        }
    }

    #[job(scoped_by = Shop)]
    struct Notify {
        marker: u64,
    }

    impl Notify {
        fn run(self, tx: &mut ScopedTx<'_, '_, Shop>) -> Result<(), JobError> {
            tx.put(&Notice {
                id: Id::from_raw(marker_nonce(self.marker) % IDS),
                marker: self.marker,
            });
            Ok(())
        }
    }

    #[job(scoped_by = Shop)]
    struct Restock {
        marker: u64,
    }

    impl Restock {
        fn run(self, tx: &mut ScopedTx<'_, '_, Shop>) -> Result<(), JobError> {
            tx.put(&Restocked {
                id: Id::from_raw(marker_nonce(self.marker) % IDS),
                marker: self.marker,
            });
            Ok(())
        }
    }

    /// The declared fan-out (spec §2 R5), on cron: one `Restock` into each
    /// shop holding an order, naming the shop the order was found under.
    #[job(global, cross_tenant)]
    struct Sweep {}

    impl Sweep {
        fn run(self, tx: &mut Tx, cap: &CrossTenant) -> Result<(), JobError> {
            let orders = tx.across_tenants::<Shop>(cap).scan::<Order>()?;
            let mut named = BTreeSet::new();
            for (shop, order) in orders {
                if named.insert(shop.raw()) {
                    tx.enqueue_in(
                        shop,
                        Restock {
                            marker: order.marker,
                        },
                        cap,
                    );
                }
            }
            Ok(())
        }
    }

    fn shop_app(sim: &Sim) -> std::io::Result<App> {
        Ok(App::open(
            Box::new(sim.disk()),
            Schema::new()
                .table::<Order>()
                .table::<Shipment>()
                .table::<Notice>()
                .table::<Restocked>(),
            Box::new(sim.entropy()),
            crash_config(),
        )?
        .mount(routes![order, walkin, peek])
        .mount_jobs(jobs![Fulfil, Notify, Restock, Sweep])
        .schedule("shop-sweep", Duration::from_secs(30), || Sweep {}))
    }

    /// One API key per shop, whose subject is that shop's member. Put every
    /// round: the puts are idempotent, and a round's requests must not
    /// depend on an earlier round having committed.
    fn seed_keys(db: &mut Db) -> Result<(), String> {
        db.transact(|tx| {
            for shop in 0..SHOPS {
                tx.put(&ApiKey {
                    id: Id::from_raw(shop),
                    key_hash: hash_token(&key_of(shop)),
                    subject: subject_of(shop),
                    created_at: 0,
                });
            }
            Ok::<(), std::io::Error>(())
        })
        .map_err(|e| match e {
            TxError::Io(e) => e.to_string(),
            other => format!("seeding keys: {other:?}"),
        })
    }

    /// The operator's read (job-tenancy spec §7): one group, its admission,
    /// and reads only, so the group commits nothing. This goes through
    /// `ScopedTx`'s own bounds, not the snapshot's raw-scope reads, so the
    /// oracle reads the way an app does.
    fn admitted_read<T>(
        db: &mut Db,
        f: impl FnOnce(&mut Tx, &TenantAdmission) -> Result<T, String>,
    ) -> Result<T, String> {
        let mut group = db.begin_group().map_err(|e| e.to_string())?;
        let admission = group.tenant_admission();
        let out = match group.stage(|tx| f(tx, &admission)) {
            Ok(out) => out,
            Err(TxError::App(msg)) => return Err(msg),
            Err(TxError::Io(e)) => return Err(e.to_string()),
            Err(TxError::NotPrimary { .. }) | Err(TxError::Verifying { .. }) => {
                unreachable!("this workload never gates writes")
            }
        };
        group.commit().map_err(|failure| failure.error.to_string())?;
        Ok(out)
    }

    /// What one oracle pass saw, for the coverage keys. Every field is an
    /// OBSERVED outcome on the durable image, never what the workload
    /// scheduled.
    #[derive(Default)]
    struct Seen {
        /// Shops holding a row that some job wrote.
        job_writers: BTreeSet<u64>,
        /// A fanned-out `Restock` has written its row.
        fanned: bool,
        /// Scoped jobs still Pending. A round ends in a restart, so any of
        /// these found finished at the next pass ran after a crash.
        pending_scoped: BTreeSet<u64>,
        /// Jobs that are Done or Dead.
        finished: BTreeSet<u64>,
    }

    /// A stripped request's nonce may appear in the store only when it came
    /// through `public(Shop)`. Anywhere else, the request had to be refused
    /// with nothing written.
    fn never_stripped(what: &str, shop: u64, marker: u64) -> Result<(), String> {
        let nonce = marker_nonce(marker);
        let route = nonce_route(nonce);
        if nonce_stripped(nonce) && route != ROUTE_WALKIN {
            return Err(format!(
                "{what} in shop {shop} carries nonce {nonce:#x}, a credential-free request to {} \
                 — only a route declared `public(Shop)` admits one, so that request had to be \
                 refused with nothing written",
                PATHS[route as usize]
            ));
        }
        Ok(())
    }

    /// A row lies in the shop its marker names, and was not written by a
    /// request that should have been refused.
    fn lies_in(table: &str, shop: u64, id: u64, marker: u64) -> Result<(), String> {
        if marker_shop(marker) != shop {
            return Err(format!(
                "tenant leak: {table} id {id} under shop {shop} carries shop {}'s marker \
                 {marker:#x} — a row lies in the tenant its writer was in",
                marker_shop(marker)
            ));
        }
        never_stripped(table, shop, marker)
    }

    /// THE ORACLE, shared by the property and its three naive victims below.
    ///
    /// For every job row: a scoped job carries the tenant its marker names,
    /// a global one carries none, and a failed attempt's `last_error` names
    /// the tenant that attempt ran in. For every shop, read through the
    /// operator's admission: each row of all four tables lies in the shop
    /// its marker names. A stripped request's nonce appears nowhere it
    /// should have been refused.
    fn jobs_stay_in_their_tenant(db: &mut Db) -> Result<Seen, String> {
        admitted_read(db, |tx, admission| {
            let mut seen = Seen::default();
            for row in tx.scan::<JobRow>().map_err(|e| e.to_string())? {
                let id = row.id.raw();
                if !matches!(row.name.as_str(), "Fulfil" | "Notify" | "Restock") {
                    if row.scope.is_some() {
                        return Err(format!(
                            "job {id} (\"{}\") is global but carries tenant {:?}",
                            row.name, row.scope
                        ));
                    }
                    continue;
                }
                let marker = u64::decode(&mut &row.payload[..]).map_err(|e| e.to_string())?;
                let shop = marker_shop(marker);
                if row.scope != Some(shop) {
                    return Err(format!(
                        "tenant leak: job {id} (\"{}\") carries tenant {:?} but was enqueued from \
                         shop {shop} (marker {marker:#x}) — a scoped job runs in the tenant it \
                         was enqueued from",
                        row.name, row.scope
                    ));
                }
                never_stripped(&format!("job {id} (\"{}\")", row.name), shop, marker)?;
                if let Some(error) = &row.last_error
                    && error.starts_with("fulfil ")
                    && *error != jammed(marker, shop)
                {
                    return Err(format!(
                        "tenant leak: job {id} (\"Fulfil\") recorded \"{error}\", but every \
                         attempt runs in shop {shop}: \"{}\"",
                        jammed(marker, shop)
                    ));
                }
                match row.status {
                    JobStatus::Pending => {
                        seen.pending_scoped.insert(id);
                    }
                    JobStatus::Done | JobStatus::Dead => {
                        seen.finished.insert(id);
                    }
                }
            }
            for shop in 0..SHOPS {
                let stx = tx.admit_scope(Id::<Shop>::from_raw(shop), admission);
                for o in stx.scan::<Order>().map_err(|e| e.to_string())? {
                    lies_in("Order", shop, o.id.raw(), o.marker)?;
                }
                let shipments = stx.scan::<Shipment>().map_err(|e| e.to_string())?;
                for s in &shipments {
                    lies_in("Shipment", shop, s.id.raw(), s.marker)?;
                }
                let notices = stx.scan::<Notice>().map_err(|e| e.to_string())?;
                for n in &notices {
                    lies_in("Notice", shop, n.id.raw(), n.marker)?;
                }
                let restocked = stx.scan::<Restocked>().map_err(|e| e.to_string())?;
                for r in &restocked {
                    lies_in("Restocked", shop, r.id.raw(), r.marker)?;
                }
                if !(shipments.is_empty() && notices.is_empty() && restocked.is_empty()) {
                    seen.job_writers.insert(shop);
                }
                seen.fanned |= !restocked.is_empty();
            }
            Ok(seen)
        })
    }

    /// Credits the coverage keys from what one pass saw. `carried` is the
    /// previous pass's `pending_scoped`.
    fn credit(sim: &Sim, seen: &Seen, carried: &BTreeSet<u64>) {
        // A single-shop universe cannot detect a job leak at all: every row
        // legitimately belongs to the only shop (tenant spec §5).
        if seen.job_writers.len() > 1 {
            sim.cover("tenant:job_multi_tenant_write");
        }
        if seen.fanned {
            sim.cover("tenant:job_fan_out");
        }
        if carried.iter().any(|id| seen.finished.contains(id)) {
            sim.cover("tenant:job_run_after_crash");
        }
    }

    const UNAUTHORIZED: &str = "authentication required — send \"Authorization: Bearer \
                                <token>\" or the charpente_session cookie";

    /// Task 3's anonymous-scope 401 for the plain-`public` route, spelled out
    /// rather than taken from the product's `format!`.
    const PEEK_REFUSED: &str = "authentication required — `POST /shop/peek` is declared \
                                `public` and opened the tenant scope `Shop` for a caller with \
                                no credential; only a route declared `public(Shop)` admits \
                                one. Send \"Authorization: Bearer <token>\" or the \
                                charpente_session cookie, or, if every caller may act inside \
                                this tenant here, declare `#[route(POST \"/shop/peek\", \
                                public(Shop))]`";

    /// Every reply, as the wire saw it. A 200 names its request's nonce, so a
    /// credential-free request that was admitted where it must be refused is
    /// caught here even when it wrote nothing durable. A 401 must be one of
    /// the two refusals a stripped request can meet. Any other 4xx is a
    /// finding: a member's request is admitted, and a stripped one is
    /// answered 401, so a 400 means `authorize` was asked about someone it
    /// should never have been asked about. A 5xx is fault fallout, since
    /// injected IO answers 500 (`ApiError::Internal`), and the durable image
    /// is where a wrong write would show.
    fn check_response(sim: &Sim, status: u16, body: &[u8]) -> Result<(), String> {
        match status {
            200 => {
                let WireU64(nonce) = serde_json::from_slice::<WireU64>(body)
                    .map_err(|e| format!("a 200 whose body is not a nonce: {e}"))?;
                let route = nonce_route(nonce);
                if nonce_stripped(nonce) && route != ROUTE_WALKIN {
                    return Err(format!(
                        "{} admitted a caller with no credential: 200 for nonce {nonce:#x} — \
                         only a route declared `public(Shop)` admits one",
                        PATHS[route as usize]
                    ));
                }
                Ok(())
            }
            401 => {
                let body: serde_json::Value =
                    serde_json::from_slice(body).map_err(|e| e.to_string())?;
                let text = body["error"].as_str().unwrap_or_default();
                if text == UNAUTHORIZED || text == PEEK_REFUSED {
                    sim.cover("tenant:anonymous_refused");
                    Ok(())
                } else {
                    Err(format!("a 401 no stripped request should meet: {text}"))
                }
            }
            400..=499 => Err(format!(
                "a {status} no request here should meet — a member's request is admitted and a \
                 stripped one is answered 401: {}",
                String::from_utf8_lossy(body)
            )),
            _ => Ok(()),
        }
    }

    /// One universe: `kernel_workload`'s shape. Three rounds, each of which
    /// crashes the disk at a random point, reopens, runs the oracle, seeds
    /// the keys, scripts paired requests, runs the kernel (dispatch and cron
    /// included) and restarts. A quiet final pass then runs the oracle once
    /// more.
    fn job_tenancy_universe(sim: &mut Sim) -> Result<(), String> {
        let entropy = sim.entropy();
        let mut seq = 0u64;
        let mut carried = BTreeSet::new();
        for _round in 0..3 {
            sim.crash_disk_after(64 + entropy.next_u64() % 768);
            let mut app = match shop_app(sim) {
                Ok(app) => app,
                Err(e) if is_fault_fallout(&e.to_string()) => {
                    sim.restart();
                    continue;
                }
                Err(e) => return Err(format!("open failed non-injected: {e}")),
            };
            match jobs_stay_in_their_tenant(app.db_for_tests()) {
                Ok(seen) => {
                    credit(sim, &seen, &carried);
                    carried = seen.pending_scoped;
                }
                Err(msg) if is_fault_fallout(&msg) => {
                    sim.restart();
                    continue;
                }
                Err(msg) => return Err(msg),
            }
            if let Err(e) = seed_keys(app.db_for_tests()) {
                if !is_fault_fallout(&e) {
                    return Err(format!("seeding keys failed non-injected: {e}"));
                }
                sim.restart();
                continue;
            }

            let base = sim.now().as_nanos();
            let mut source = SimSource::new(Box::new(sim.clock()), base + 400 * SEC);
            for i in 0..(1 + entropy.next_u64() % 6) {
                let shop = entropy.next_u64() % SHOPS;
                let route = entropy.next_u64() % 3;
                // The credentialed request, then its stripped replay a
                // nanosecond later: every write is also tried with no
                // credential at all.
                for stripped in [false, true] {
                    let mut request = Request::new(Method::Post, PATHS[route as usize]).json(
                        &Place {
                            shop: WireU64(shop),
                            nonce: WireU64(nonce(seq, route, stripped)),
                        },
                    );
                    if !stripped {
                        request = request
                            .header("Authorization", format!("Bearer {}", key_of(shop)));
                    }
                    source.push_at(base + i * SEC + u64::from(stripped), request);
                }
                seq += 1;
            }
            let responses = source.responses();
            let run = (|| -> Result<(), std::io::Error> {
                let mut kernel = Kernel::new(app, Box::new(sim.clock()))?;
                let out = kernel.run(&mut source);
                if kernel.stats().jobs_retried > 0 {
                    sim.cover("job:retried");
                }
                out
            })();
            for (status, body) in responses.lock().unwrap().iter() {
                check_response(sim, *status, body)?;
            }
            if let Err(e) = run
                && !is_fault_fallout(&e.to_string())
            {
                return Err(format!("kernel stopped non-injected: {e}"));
            }
            sim.restart();
        }

        for _attempt in 0..8 {
            match shop_app(sim) {
                Ok(mut app) => match jobs_stay_in_their_tenant(app.db_for_tests()) {
                    Ok(seen) => {
                        credit(sim, &seen, &carried);
                        return Ok(());
                    }
                    Err(msg) if is_fault_fallout(&msg) => sim.restart(),
                    Err(msg) => return Err(msg),
                },
                Err(e) if is_fault_fallout(&e.to_string()) => sim.restart(),
                Err(e) => return Err(format!("final open failed non-injected: {e}")),
            }
        }
        Ok(())
    }

    /// Job-tenancy spec §9 item 4. A fixed base, like every permanent swarm
    /// property here. N was set by timing one batch (Step 3): the commit and
    /// the LEDGER state it.
    #[test]
    fn every_job_runs_in_the_tenant_it_was_enqueued_from() {
        run_universes_swarm_with!(
            0x70B5_0001,
            256,
            |_cfg: &mut FaultConfig| {},
            job_tenancy_universe,
            require: [
                "tenant:job_multi_tenant_write",
                "tenant:job_run_after_crash",
                "tenant:job_fan_out",
                "tenant:anonymous_refused",
                "job:retried",
            ]
        );
    }

    /// Determinism: the workload replays to the same durable state from the
    /// same seed, so its seeds are repro lines and not merely labels.
    #[test]
    fn the_job_tenancy_workload_replays_identically_for_the_same_seed() {
        let trace = |seed: u64| {
            let mut sim = Sim::new(seed);
            let result = job_tenancy_universe(&mut sim);
            assert_eq!(result, Ok(()), "seed {seed:#x}");
            (sim.durable_fingerprint(), sim.now())
        };
        for seed in [0x70B5_5EED, 0x0BAD_70B5_0BAD_70B5] {
            assert_eq!(trace(seed), trace(seed));
        }
    }
}
```

- [ ] **Step 2: Write the three naive victims and their controls**

Inside `mod job_tenancy`, after the replay test:

```rust
    // ------------------------------------------------ the naive victims

    /// A fault-free store for a victim and its control: the same app, the
    /// same seed.
    fn victim_app(seed: u64) -> (Sim, App) {
        let sim = Sim::with_faults(seed, FaultConfig::none());
        let app = shop_app(&sim).unwrap();
        (sim, app)
    }

    /// Operator writes, committed as one group.
    fn operator_writes(db: &mut Db, f: impl FnOnce(&mut Tx, &TenantAdmission)) {
        let mut group = db.begin_group().unwrap();
        let admission = group.tenant_admission();
        group
            .stage(|tx| {
                f(tx, &admission);
                Ok::<(), std::io::Error>(())
            })
            .unwrap();
        group.commit().unwrap();
    }

    /// Victim 1, the row a broken enqueue writes: shop 1's job carried
    /// shop 2's tenant. The control enqueues the same job in shop 1.
    #[test]
    fn the_oracle_names_a_job_row_carrying_another_shops_tenant() {
        let m = marker(1, nonce(7, ROUTE_ORDER, false));
        for (enqueued_in, leaks) in [(1, false), (2, true)] {
            let (_sim, mut app) = victim_app(0x70B5_0101);
            operator_writes(app.db_for_tests(), |tx, admission| {
                tx.admit_scope(Id::<Shop>::from_raw(enqueued_in), admission)
                    .enqueue(Fulfil { marker: m });
            });
            let verdict = jobs_stay_in_their_tenant(app.db_for_tests());
            if !leaks {
                assert!(verdict.is_ok(), "the control: {:?}", verdict.err());
                continue;
            }
            let err = verdict.err().expect("a job in the wrong shop must fail the oracle");
            assert_eq!(
                err,
                format!(
                    "tenant leak: job 0 (\"Fulfil\") carries tenant Some(2) but was enqueued \
                     from shop 1 (marker {m:#x}) — a scoped job runs in the tenant it was \
                     enqueued from"
                )
            );
        }
    }

    /// Victim 2, what a dispatcher that opens tenant 0 for every job
    /// leaves behind: shop 3's shipment, written under shop 0. The control
    /// writes it under shop 3.
    #[test]
    fn the_oracle_names_a_job_written_row_under_tenant_zero() {
        let m = marker(3, nonce(4, ROUTE_WALKIN, false));
        for (written_in, leaks) in [(3, false), (0, true)] {
            let (_sim, mut app) = victim_app(0x70B5_0102);
            operator_writes(app.db_for_tests(), |tx, admission| {
                tx.admit_scope(Id::<Shop>::from_raw(written_in), admission)
                    .put(&Shipment {
                        id: Id::from_raw(4),
                        marker: m,
                    });
            });
            let verdict = jobs_stay_in_their_tenant(app.db_for_tests());
            if !leaks {
                assert!(verdict.is_ok(), "the control: {:?}", verdict.err());
                continue;
            }
            let err = verdict.err().expect("a row in the wrong shop must fail the oracle");
            assert_eq!(
                err,
                format!(
                    "tenant leak: Shipment id 4 under shop 0 carries shop 3's marker {m:#x} — \
                     a row lies in the tenant its writer was in"
                )
            );
        }
    }

    /// Victim 3, a fan-out that lands every enqueue in the first tenant:
    /// shop 2's restock is enqueued into shop 0 and then really dispatched,
    /// so its row is written there too. The oracle must name the job row,
    /// which it reads first. The control fans the job into shop 2 and runs
    /// it the same way.
    #[test]
    fn the_oracle_names_a_fan_out_landed_in_the_first_tenant() {
        let m = marker(2, nonce(9, ROUTE_PEEK, false));
        for (fanned_into, leaks) in [(2, false), (0, true)] {
            let (_sim, mut app) = victim_app(0x70B5_0103);
            let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
            let db = app.db_for_tests();
            operator_writes(db, |tx, admission| {
                tx.admit_scope(Id::<Shop>::from_raw(fanned_into), admission)
                    .enqueue(Restock { marker: m });
            });
            dispatcher.note_enqueued(db.drain_enqueued());
            assert!(matches!(
                dispatcher.run_due(db, 0).unwrap(),
                JobOutcome::Done { .. }
            ));
            let verdict = jobs_stay_in_their_tenant(db);
            if !leaks {
                let seen = verdict.expect("the control");
                assert!(seen.fanned, "the control's restock ran and wrote its row");
                continue;
            }
            let err = verdict.err().expect("a fan-out into the wrong shop must fail the oracle");
            assert_eq!(
                err,
                format!(
                    "tenant leak: job 0 (\"Restock\") carries tenant Some(0) but was enqueued \
                     from shop 2 (marker {m:#x}) — a scoped job runs in the tenant it was \
                     enqueued from"
                )
            );
        }
    }
```

Job id 0 in all three: a fresh store's job-id counter starts at 0, and `old_binary_leaves_pending_receipts`'s refusal pins `ids 0, 1, 2` on the same basis. If a victim's id differs, read why before changing the literal. The `SessionSweep` cron entry schedules, but enqueues nothing until the kernel ticks, and no kernel runs here.

- [ ] **Step 3: Run the victims, then time one batch and set N**

```zsh
cargo test -p charpente --test kernel -- job_tenancy::the_oracle_names
```

Expected: 3 passed. Each victim fails the shared oracle with its exact text, and its control passes on the same app.

Time one batch in CPU time, not wall clock: the laptop may sleep mid-run, and wall clock cannot detect that. Time the existing kernel swarm the same way beside it:

```zsh
D=.superpowers/sdd/2026-09-27-job-tenancy
cargo test -p charpente --test kernel --no-run
/usr/bin/time -p cargo test -p charpente --test kernel -- job_tenancy::every_job_runs_in_the_tenant_it_was_enqueued_from --exact 2>&1 | tee $D/job-tenancy-swarm-256.log | grep -E "^test result|^user|^sys"
/usr/bin/time -p cargo test -p charpente --test kernel -- kernel_jobs_are_exactly_once_under_the_fault_matrix --exact 2>&1 | tee $D/kernel-swarm-timing.log | grep -E "^test result|^user|^sys"
```

The rule for N:
- Keep 256, the permanent-swarm standard, if the new batch's user+sys time is at most twice the exactly-once swarm's.
- Otherwise halve N until it is. Never go below 64.
- If 64 is still over twice, stop and report: the workload is too heavy, and the fix is a lighter round, not fewer universes.

Record both measurements and the chosen N in `$D/progress.md`. N and base `0x70B5_0001` go in the commit message.

If a `require:` key does not fire at the chosen N, the batch fails, and the fix is the workload, never the key list. A key that cannot fire certifies nothing. Before touching anything, print which key did not fire and count how often it fired, from the coverage lines in the log.

If the batch fails on a universe, stop. Paste the printed `CHARPENTE_SEED=… cargo test …` line into the report, replay it, and read which oracle message fired. A real leak is a bug in Tasks 2–6: a finding for the controller. Do not loosen the oracle.

- [ ] **Step 4: Prove the property has teeth, by mutation**

Commit nothing yet. This task changes only `kernel.rs`, and the mutations below touch only production files, so each one is reverted with `git checkout <that file>`. Apply each mutation alone, run the swarm, confirm it fails, and quote the failing universe's seed line and oracle message:

1. `crates/charpente-core/src/db.rs`, `ScopedTx::enqueue`: `push_enqueue(Some(0), …)` in place of `Some(scope)`. Expected: "tenant leak: job … (\"Fulfil\") carries tenant Some(0) but was enqueued from shop …".
2. `crates/charpente/src/kernel/dispatch.rs`, `JobScope::open`: `Id::from_raw(0)`. Expected: "tenant leak: Shipment id … under shop 0 carries shop …'s marker …", or the jammed-text leak.
3. `crates/charpente-core/src/db.rs`, `Tx::enqueue_in`: `push_enqueue(Some(0), …)`. Expected: a `Restock` job row carrying `Some(0)` for a shop other than 0.
4. `crates/charpente/src/tenant.rs`, `Scoped::scope`: rule 3 asks `authorize` about nobody, which is the shape C-2 removes. Replace `None => Err(ctx.anonymous_scope::<S>())` with `None => self.open_scope(scope, &crate::auth::Principal { subject: String::new(), via: crate::auth::Via::Oidc })`. This compiles inside the crate because Task 4 made the fields `pub(crate)`. `Shop::authorize` refuses the empty subject, so the stripped `/shop/peek` request is answered 400 instead of the named 401. Expected: "a 400 no request here should meet — … is not a member of shop …".

For each mutation, a green swarm is not "the mutation was harmless". Before concluding anything, print what the mutated build actually did — say, one universe's job rows and their scopes. A green run is ambiguous between a vacuous oracle and an inert mutation. Then confirm the tree is clean: `git status --short` must show only this task's `kernel.rs`.

- [ ] **Step 5: Fresh bases**

Re-base the property on fresh seeds, where coverage requirements are skipped by design, so each run is a correctness sweep:

```zsh
D=.superpowers/sdd/2026-09-27-job-tenancy
for base in 0x70B5_A001 0x70B5_B002 0x70B5_C003; do
  CHARPENTE_BASE_SEED=$base cargo test -p charpente --test kernel -- job_tenancy::every_job_runs_in_the_tenant_it_was_enqueued_from --exact 2>&1 | tee $D/job-tenancy-fresh-$base.log | grep "^test result"
done
```

Expected: three `test result: ok. 1 passed` lines. This is 3N fresh universes in all, and the count goes in the commit.

- [ ] **Step 6: Gate and commit**

Run the gate and count it. The count is 87 of 87: the new tests sit in the existing `kernel` suite, so the number of `test result: ok` lines does not change. Re-check `git status` and `git log -1`. Then:

```zsh
git add crates/charpente/tests/kernel.rs
git commit -m "test(job-tenancy): oracle 4 — every job runs in the tenant it was enqueued from, under the fault matrix (base 0x70B5_0001, N universes, 3N fresh); three named victims"
```

Replace `N` in the message with the chosen count and `3N` with its product.

---

### Task 9: Close-out — mutation run, hand inversions, fresh seeds, the statuspage rebuilt, the counted gate, the record

**Files:**
- Modify:
  - `PLAN.md`: M3 sub-project 3's closing sentence; the After-launch header and table.
  - `docs/superpowers/specs/2026-09-27-job-tenancy-design.md`: the Status line.
  - `docs/LEDGER.md`: a new entry.
- Test files only if Step 1 finds a MISSED mutant: write the property that kills it, in the owning task's suite.
- `.cargo/mutants.toml` and `crates/guide-examples/tests/mutants_exclusions.rs` do not change (spec §9 item 5: no exclusion may be added).
- Read-only: `../statuspage-run/statuspage`. It is built from a scratch copy and never written.

- [ ] **Step 1: The mutation run (spec §9 item 5)**

Commit before starting: the tree at Task 8's commit is what gets mutated. First confirm `git status --short` is empty. Then validate each `--re` against the tool's own list, one target at a time. A regex that matches nothing makes the raw tool exit 0 and write no `outcomes.json`. The verb reports that as "matched nothing", which is not a pass:

```zsh
cargo mutants --list --file crates/charpente-core/src/db.rs | grep -E 'tenant_admission|admit_scope|enter_scope|open_scope|push_enqueue|enqueue|encode_record|decode_record' | head -40
cargo mutants --list --file crates/charpente/src/http/app.rs | grep -E 'build_ctx|stage_or_subscribe|subscription_event_names|Declared|Stance|public_admission|anonymous_scope' | head -40
cargo mutants --list --file crates/charpente-macros/src/route.rs | grep -E 'scope_name|public_scope_tokens|undeclared_capability|RouteArgs' | head -40
```

Every function Tasks 2–7 added or changed must appear in some target's list. If one does not, widen that target's regex: a function outside every list was never mutated.

Then run each target. **Read the verb's output, not cargo-mutants' exit code.** Each `--file` outside core and sim names the package whose suites exercise it. Without that, the verb judges its mutants with core and sim's tests alone, and reports them all MISSED having measured nothing.

```zsh
D=.superpowers/sdd/2026-09-27-job-tenancy
scripts/mutants.sh --file crates/charpente-core/src/db.rs --re 'tenant_admission|admit_scope|enter_scope|open_scope|push_enqueue|enqueue|encode_record|decode_record' --test-package charpente-core --test-package charpente 2>&1 | tee $D/mutants-db.log
scripts/mutants.sh --file crates/charpente-core/src/table.rs --re 'materialize_job|NoJobTenant|UnscopedJob' --test-package charpente-core --test-package charpente 2>&1 | tee $D/mutants-table.log
scripts/mutants.sh --file crates/charpente-core/src/job.rs --test-package charpente-core --test-package charpente 2>&1 | tee $D/mutants-job.log
scripts/mutants.sh --file crates/charpente/src/tenant.rs --test-package charpente 2>&1 | tee $D/mutants-tenant.log
scripts/mutants.sh --file crates/charpente/src/http/app.rs --re 'build_ctx|stage_or_subscribe|subscription_event_names|Declared|Stance|public_admission|anonymous_scope|schedule' --test-package charpente 2>&1 | tee $D/mutants-app.log
scripts/mutants.sh --file crates/charpente/src/kernel/dispatch.rs --re 'run_due_into|JobScope' --test-package charpente 2>&1 | tee $D/mutants-dispatch.log
scripts/mutants.sh --file crates/charpente/src/describe.rs --re 'route_info|job_info|to_human' --test-package charpente 2>&1 | tee $D/mutants-describe.log
scripts/mutants.sh --file crates/charpente-macros/src/job.rs --file crates/charpente-macros/src/fingerprint.rs --test-package charpente-macros --test-package charpente 2>&1 | tee $D/mutants-macros-job.log
scripts/mutants.sh --file crates/charpente-macros/src/route.rs --re 'scope_name|public_scope_tokens|undeclared_capability|RouteArgs' --test-package charpente-macros --test-package charpente 2>&1 | tee $D/mutants-macros-route.log
```

The target is zero MISSED and zero timeouts (a timeout is not a catch):
- A MISSED mutant is a one-sided property. Write the test that kills it, commit it, and re-run that target. Never add a `.cargo/mutants.toml` entry.
- Before writing a killer test for a MISSED mutant, print what the mutated build actually does. A mutant can be missed because no test looks, or because it is truly inert. The second is the equivalence argument, and it goes to the controller: it is not settled by the implementer alone.
- Record every run's caught, missed, unviable and timeout counts in the report.

These runs are long. The whole charpente suite judges each mutant, and a run can outlast the 600 s tool ceiling. The controller runs each one in the background and waits on its log file, never on a blocking read of a running agent.

- [ ] **Step 2: The hand inversions, in an isolated worktree**

These are spec §9 item 5's hand inversions, plus the dispatch assert, on the committed tree. Each runs in its own worktree with its own target dir. A mutation in the main tree could be abandoned mid-run and read afterwards as working code, and a shared target dir can carry a mutated artifact past a clean `git diff`.

```zsh
MAIN=$PWD
D=$MAIN/.superpowers/sdd/2026-09-27-job-tenancy
git worktree add ../charpente-c2-inversions HEAD
cd ../charpente-c2-inversions
export CARGO_TARGET_DIR=$PWD/target
```

Apply each inversion alone, run the named tests, and quote the failure. Restore with `git checkout <file>` and confirm `git status --short` is empty before the next one:

| # | File, site | Inversion | Must fail |
|---|---|---|---|
| 1 | `crates/charpente/src/tenant.rs`, `Scoped::scope` rule 1 | `ctx.public_admission::<S>()` → `None::<&TenantAdmission>` | `cargo test -p charpente --test tenant` — test (b), anonymous on `public(Org)`, gets 401, not 200 |
| 2 | same, rule 2 | `Some(principal) => self.open_scope(scope, principal)` → `Some(_) => Err(ctx.anonymous_scope::<S>())` | test (d), a credentialed caller on plain `public`, and (e) |
| 3 | same, rule 3 | `None => Err(ctx.anonymous_scope::<S>())` → `None => self.open_scope(scope, &crate::auth::Principal { subject: String::new(), via: crate::auth::Via::Oidc })` | test (a) (400, not the 401) and `job_tenancy::every_job_runs_in_the_tenant_it_was_enqueued_from` |
| 4 | `crates/charpente/src/kernel/dispatch.rs`, the binding | `JobScope { id, admission: &admission }` → `JobScope { id: id ^ 1, admission: &admission }` | `scoped_jobs::a_scoped_job_runs_in_its_enqueuers_tenant_tenant_zero_included` and the swarm |
| 5 | same, the assert | `row.scope.is_some() == def.scoped_by.is_some()` → `!=` | every `scoped_jobs` test: the assert fires on the first run |
| 6 | `crates/charpente-core/src/db.rs`, `push_enqueue`'s assert | `TypeId::of::<J::Scope>() != …` → `==` | every enqueue panics: `scoped_jobs` and the kernel suite fail on the first one |

Operand inversions (1–4) prove each arm's input is read, not only that the arm exists. A green result under any inversion is ambiguous: the test may be vacuous, or the inversion inert. Print the observable behaviour — the status, the calls counted, the row's scope — before concluding either.

Then clean up, one git-native step at a time, and copy nothing out: the logs were written to `$D` in the main tree.

```zsh
cd "$MAIN"
git worktree remove ../charpente-c2-inversions
```

The worktree's `target/` is multi-GB, and the removal can outrun the default 2-minute tool timeout. Run it with a longer timeout rather than splitting it into `rm -rf`.

- [ ] **Step 3: Fresh seeds, and every existing pin**

Run every swarm this work touches over fresh bases. Record each base and its result:

```zsh
D=.superpowers/sdd/2026-09-27-job-tenancy
for base in 0xC2F0_0001 0xC2F0_0002 0xC2F0_0003; do
  CHARPENTE_BASE_SEED=$base cargo test -p charpente --test tenant -- every_scoped_read_returns_only_its_own_tenants_rows --exact 2>&1 | tee $D/fresh-tenant-$base.log | grep -E "^test result|FAILED|CHARPENTE_SEED"
  CHARPENTE_BASE_SEED=$base cargo test -p charpente --test kernel -- kernel_jobs_are_exactly_once_under_the_fault_matrix --exact 2>&1 | tee $D/fresh-kernel-$base.log | grep -E "^test result|FAILED|CHARPENTE_SEED"
  CHARPENTE_BASE_SEED=$base cargo test -p charpente --test realtime -- a_reconnecting_tenant_subscriber_misses_nothing_and_leaks_nothing --exact 2>&1 | tee $D/fresh-realtime-$base.log | grep -E "^test result|FAILED|CHARPENTE_SEED"
done
```

`CHARPENTE_BASE_SEED` skips `require:` and `floor:`. These runs are evidence about failures only, never about coverage. The commit and the LEDGER state the total: each property's N × 3 bases, plus Task 8's 3N. Any failing seed is pinned as a regression test, with its reason asserted, before anything else happens.

Then CI's replay pins, exactly as `.github/workflows/ci.yml` lists them. Read the file at this commit; do not copy the lines from this plan. Expected: each green.

One pin needs more than a green replay: `0xe2d97275c15cc087` is the kernel swarm's live RED-first reproduction. Task 3 moved `post` to `public(Org)`, and a moved op schedule is what defanged its predecessors. So re-verify its meaning in a scratch worktree, the way it was first proven: delete the `is_fault_fallout` classification in `kernel_workload`'s checker arm and replay the seed. It must be RED. Restore the classification and replay again: it must be GREEN.

If it is GREEN with the classification deleted, the pin no longer reaches the arm. Report it; do not fix it silently. The re-pin procedure is in the ci.yml comment above that line: a 10,000-universe batch at base `0x577d39b752d077c7`.

- [ ] **Step 4: The statuspage, rebuilt against the finished tree (spec §13 criterion 2)**

The checkout at `../statuspage-run/statuspage` is read-only. Its `[patch]` table points at this repo by relative path, so building it in place would build against this tree, but it would write that checkout's `target/` and possibly its `Cargo.lock`. Build a copy instead.

The copy goes outside this repo. Inside it, cargo would take the copy for a member of this workspace and refuse to build it. `S` holds its path for the rest of this step:

```zsh
S="$(mktemp -d)/statuspage-6037704"
D=$PWD/.superpowers/sdd/2026-09-27-job-tenancy
mkdir -p "$S"
git -C ../statuspage-run/statuspage archive 6037704 | tar -x -C "$S"
grep -n 'path = "../../charpente' "$S/Cargo.toml"
```

The copy's two `[patch]` paths are relative to the original location. Rewrite them to this repo's absolute path, in the copy only. Then check:

```zsh
perl -pi -e "s#path = \"\\.\\./\\.\\./charpente/#path = \"$PWD/#" "$S/Cargo.toml"
grep -n "path = \"$PWD" "$S/Cargo.toml"
(cd "$S" && CARGO_TARGET_DIR="$S/target" cargo check --all-targets 2>&1) | tee $D/statuspage-pass1.log | grep -E "^(error|warning)" | sort | uniq -c
```

Criterion 2 expects errors at:
- `lib.rs:42-45`, the `authorize` signature;
- every `open_scope(org, None)`: `public.rs:41`, `:90`, `:127`, `notify.rs:24` and `maintenance.rs:149`;
- every bare `#[job]`: `notify.rs:10`, `maintenance.rs:109` and `:139`.

A proc-macro refusal can stop rustc before type checking, so pass 1 may report only the three `#[job]` refusals. If it does, make a pass 2 in the copy that changes the three attributes to `#[job(global)]` and nothing else, so type checking proceeds. Record that edit verbatim beside its output:

```zsh
perl -pi -e 's/^#\[job\]$/#[job(global)]/' "$S/src/notify.rs" "$S/src/maintenance.rs"
(cd "$S" && CARGO_TARGET_DIR="$S/target" cargo check --all-targets 2>&1) | tee $D/statuspage-pass2.log | grep -E "^error" -A 3 | head -80
```

Every site the criterion names must appear in pass 1 or pass 2, each named by `file:line`. An expected site that compiles is a finding: report it, and do not explain it away.

Pass 2's edit induces errors of its own, and those are not findings. `incidents.rs:261` enqueues `NotifySubscribers` on a `ScopedTx<Org>`, and a `global` job no longer satisfies `ScopedTx::enqueue` (E0271, expected `Org`, found `Unscoped`). That error is the platform refusing exactly the enqueue C-2 exists to refuse. List every error pass 2 reports, and mark each one either as a criterion site or as induced by the edit, with the reason. Any error that is neither is a finding.

Every error text goes in the LEDGER entry, with its pass. Finally, confirm the original checkout is untouched:

```zsh
git -C ../statuspage-run/statuspage status --short
git -C ../statuspage-run/statuspage log -1 --format=%h
```

Expected: empty, then `6037704`.

- [ ] **Step 5: The full counted gate**

The controller runs it in the background. The suite can outlast the 600 s tool ceiling:

```zsh
LOG=.superpowers/sdd/2026-09-27-job-tenancy/gate-final.log
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && \
  cargo test --workspace --no-fail-fast 2>&1 | tee $LOG; echo "exit ${pipestatus[1]}"
grep -c "^test result:" $LOG; grep -c "^test result: ok" $LOG; grep -c FAILED $LOG
```

Expected: the two counts are equal and both 87, and 0 FAILED. A `grep -c` that finds nothing prints `0` and exits 1, so read the numbers, not the exit status. Also run CI's other steps exactly as `.github/workflows/ci.yml` lists them: the TS gate, the ignored bench test and the two launch guards.

- [ ] **Step 6: The record**

**`PLAN.md`, M3 sub-project 3's closing sentence.**
- "The 0.1.0 publish now waits on two platform fixes this build found:" becomes "The 0.1.0 publish waited on two platform fixes this build found, and both are fixed:".
- After "(2) a job cannot open a tenant scope the way GUIDE §16 documents, and `authorize` sees neither the operation nor the caller kind", insert "— **fixed 2026-09-2x** (spec `docs/superpowers/specs/2026-09-27-job-tenancy-design.md`: a job declares `global` or `scoped_by`, and a scoped one runs in the tenant it was enqueued from; `authorize` is asked about a principal only; a route's stance, `auth`, `public` or `public(Org)`, says who reaches a tenant)".
- Replace `2026-09-2x` with the date of Task 8's commit.

**`PLAN.md`, the After-launch header.** "…three from M3's payload-shapes fix, each with the trigger that reopens it" becomes "…three from M3's payload-shapes fix, one from M3's job-tenancy fix, each with the trigger that reopens it". Append this row:

```markdown
| **`enqueue_keyed_in`** — a keyed declared fan-out (job-tenancy spec §12) | The first fan-out whose job needs an external idempotency key. `enqueue_in` and `ScopedTx::enqueue_keyed` both exist; the keyed fan-out is the one combination deferred. |
```

**The spec's Status line** becomes "Implemented 2026-09-2x (commits `<Task 1 sha>`..`<Task 8 sha>`); plan `docs/superpowers/plans/2026-09-27-job-tenancy.md`."

**`docs/LEDGER.md`** gets a new entry at the end, in the house style:
- the heading is `## 2026-09-2x — job tenancy: a job declares its tenant and runs in it, authorize is asked about principals only, and a route's stance says who reaches a tenant`;
- the body says what shipped, one line per task with its commit sha;
- the seeds:
  - oracle 4's base `0x70B5_0001`, its N and how N was set (both CPU timings);
  - the fresh bases and total universes;
  - the three coverage diffs (Tasks 3, 4, 6), which are identical, so spec §9 item 7's prediction held;
  - `0xe2d97275c15cc087`'s RED-first re-verification;
  - any seed pinned in this work;
- the mutation table, one row per target with caught, missed, unviable and timeout counts, beside the six hand inversions;
- the statuspage's rebuilt error list, pass by pass;
- the gate counted (`87 of 87`).

Update memory `charpente-m1-progress.md`: C-2 is fixed, so both of the 0.1.0 publish's platform blockers are closed. Pushing and publishing are Noah's calls.

- [ ] **Step 7: Commit**

```zsh
git status --short
git add PLAN.md docs/superpowers/specs/2026-09-27-job-tenancy-design.md docs/LEDGER.md
git commit -m "ledger(job-tenancy): C-2 fixed — mutants clean over every new function, six hand inversions red, N universes over fresh seeds, the statuspage refused at every site, the workspace gate counted 87 of 87"
```

If Step 1 added killer tests, they were committed with their own messages before this one. `git status --short` must show only the three files above.

Do not push. Pushing and publishing are Noah's calls.

