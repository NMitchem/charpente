# `Drive::recover` runs the platform checks — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `Drive::recover` judges every `Drive` workload on three platform rules after the fault-free tail, so no app has to remember to call them.

**Architecture:** `recover` already calls `mark_dead_coverage` after the tail settles. Just before it, `recover` takes one snapshot and runs `keys_are_per_effect`, `calls_have_durable_jobs` (given up under durable corruption) and `dead_hooks_succeeded`, returning a failure as its `Err` in the check's own words. The copies apps wrote after `recover` are then deleted, along with the `outbound_settles_by_horizon` copies, which cannot fail after `recover`.

**Tech Stack:** Rust workspace; `charpente-sim` (`src/drive.rs`, `src/checks.rs`, `tests/drive.rs`); `charpente-cli` tests (`demo.rs`, `new.rs`); docs/GUIDE.md compiled by `guide-examples`.

**Spec:** `docs/superpowers/specs/2026-11-26-drive-runs-platform-checks-design.md` (approved 2026-11-26, revised while planning: give-up reason pinned, GOLDEN exception named).

## Global Constraints

- Give-up reason, verbatim: `injected corruption cut an enqueue after its call reached the remote`
- Check order in `recover`: `keys_are_per_effect`, then `calls_have_durable_jobs`, then `dead_hooks_succeeded`, then the existing `mark_dead_coverage` (which stays last; GUIDE §8 says so).
- `outbound_settles_by_horizon` is NOT added to `recover`.
- No universe moves. Every recorded batch stays byte-identical except the one GOLDEN/README give-up line named in Task 1.
- Ships in 0.7.0: the CHANGELOG entry goes under `## 0.7.0 (unreleased)` → `### Breaking changes`.
- Hard rule 6: the GUIDE text describing `recover`'s new behaviour lands in the same commit as the code (Task 1).
- Checks to run, the same as CI: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test … --no-fail-fast` piped through `tee`. Confirm green by counting `test result: ok` lines, never by the absence of `FAILED`. The shell is zsh: use `${pipestatus[1]}`, never `PIPESTATUS`. The workspace suite can run past 600 s, so run it per package in the foreground or in the background with a notification.
- `$SCRATCH` in the commands below is any scratch directory outside the repo (your session's scratchpad), never the repo itself.
- Commit only the paths you name. Do not add a `Co-Authored-By` trailer (CLAUDE.md). Commit directly to `main`.

## Review Focus

1. **A given-up universe must not also be judged.** When the tail's boot gives up, `recover` returns `Ok(None)` before the checks and records exactly one give-up reason. Existing `a_durable_refusal_in_the_tail_gives_the_run_up` covers this; it must stay green unchanged.
2. **An app with no hosts and no jobs** (`notes`) must pass the three checks trivially and stay byte-identical. Every existing `tests/drive.rs` pinned-seed test covers this; none may change its assertions.
3. **`recover` called more than once on one `Sim`** (`charpente-sim/tests/determinism.rs` builds a second `Sim` inside its check, and `calls_have_durable_jobs` reads every delivery the `Sim` recorded). Expected: still green. If a second `recover` on the same `Sim` sees the first cycle's deliveries against a store that holds their jobs, it passes; if not, stop and report rather than scope the check.
4. **A Dead job whose hook succeeded** (`a_hook_that_reenqueues_on_every_dead_ends_with_the_outage`, 31 Dead links per chain) must pass `dead_hooks_succeeded`. It stays green unchanged.
5. **The check's text is the whole error.** A failing check's `Err` is returned unwrapped and unprefixed. Task 1's tests compare its prefix exactly.

---

### Task 1: `recover` runs the three checks, with tests, docs and the GOLDEN line

**Files:**
- Modify: `crates/charpente-sim/src/drive.rs` (imports at lines 31-34; `recover` at line 414, before `mark_dead_coverage`; `recover`'s doc comment's `# Errors` list)
- Test: `crates/charpente-sim/tests/drive.rs` (four new tests and their small apps, appended)
- Modify: `crates/charpente-cli/tests/demo.rs:46` and `README.md:119` (the give-up line)
- Modify: `docs/GUIDE.md` §8: the drive section near "**Coverage for a Dead path.**" (line ~3955) and the checker list (lines ~4454-4495)
- Modify: `crates/guide-examples/tests/doc_truth.rs` (`PINNED_FLOOR` 63 → 64, and its dated comment)
- Modify: `CHANGELOG.md` (`## 0.7.0 (unreleased)` → `### Breaking changes`)

**Interfaces:**
- Consumes: `checks::{keys_are_per_effect, calls_have_durable_jobs, dead_hooks_succeeded, durable_corruption_fired}`, `Sim::give_up(&self, reason: &'static str, detail: impl Display)`, `Sim::cover(&self, what: &'static str)`, `Sim::outbound(&self, hosts) -> io::Result<SimOutbound>`, `Outbound::start(&self, token: u64, call: OutboundCall)`.
- Produces: no new public items. `Drive::recover` keeps its signature `fn recover(&self, phase: &Phase, tail: Duration) -> Result<Option<Recovered>, String>`, and gains three `Err` cases and one `Ok(None)` case, which Task 2 relies on.

- [ ] **Step 1: Write the four failing tests in `tests/drive.rs`**

Each uses `Sim::with_faults(<seed>, FaultConfig::none())`, so the seed only names the universe. Pick seeds from `0xD7_0C00` up.

```rust
/// Two outbound jobs minted under one key: the app opens on an entropy that
/// returns the same bytes every draw, so the route's two enqueues share a key.
#[test]
fn recover_fails_a_run_whose_two_jobs_share_a_key() {
    // build: `mailer`'s shape, but `Box::new(Constant)` for entropy, where
    //   `struct Constant; impl Entropy for Constant { fn fill(&self, b: &mut [u8]) { b.fill(7) } }`
    // route `POST /pair/:id` enqueues `Mail { note: id }` and `Mail { note: id + 1 }`
    // remote: `sim.remote("mail.example", Counting(..))`
    // phase: one POST /pair/1 at offset 0, horizon 1 s; recover tail 60 s
    let err = drive.recover(&phase, Duration::from_secs(60)).err().expect("the check fails the run");
    assert!(err.starts_with("keys_are_per_effect: jobs 0 and 1 carry the same idempotency key 07070707070707070707070707070707"), "{err}");
}

/// A call reached the remote with a key no job in the store carries.
#[test]
fn recover_fails_a_run_whose_call_has_no_durable_job() {
    // app: `notes`; `sim.remote("mail.example", Counting(..))`
    // after faulted_phase, before recover:
    //   sim.outbound(&["mail.example"]).unwrap().start(0, OutboundCall { host: "mail.example",
    //     method: HttpMethod::Post, path: "/x".into(), headers: vec![], body: vec![],
    //     key: CallKey::from_bytes([9; 16]) }).unwrap();
    let err = drive.recover(&phase, Duration::from_secs(1)).err().expect("the check fails the run");
    assert!(err.starts_with("calls_have_durable_jobs: a call to mail.example with key 09090909090909090909090909090909 reached the remote"), "{err}");
}

/// The same orphan call where durable corruption is marked as fired: given up, not failed.
/// Constructed: `sim.cover("fault:bit_flip")` stands in for a bit flip that cut the
/// enqueue; the natural seed is the double-charge GOLDEN's 0x11c23979e56d3608.
#[test]
fn recover_gives_up_an_orphan_call_where_corruption_fired() {
    // as above, plus `sim.cover("fault:bit_flip")` before recover
    assert!(drive.recover(&phase, Duration::from_secs(1)).unwrap().is_none());
    assert_eq!(sim.gave_up(), Some("injected corruption cut an enqueue after its call reached the remote"));
}

/// An on_dead hook that returns Err: the Dead mark commits alone and the run fails.
#[test]
fn recover_fails_a_run_whose_dead_hook_failed() {
    // job: #[job(global, on_dead, outbound = "hapless.test", retry(max = 1))] struct Hapless { note: Id<Note> }
    //   call: Call::post("/x"); settle: Err(JobError::app("hapless.test refused")) unless 200;
    //   on_dead: Err(JobError::app("the hook gave up"))
    // remote: charpente_sim::outbound::from_fn answering 503 to every call
    // route POST /hapless/:id enqueues it; phase one POST at 0, horizon 1 s; tail 60 s
    let err = drive.recover(&phase, Duration::from_secs(60)).err().expect("the check fails the run");
    assert!(err.starts_with("dead_hooks_succeeded: job 0 (\"Hapless\") is Dead and its on_dead hook failed"), "{err}");
}
```

The comments describe the setup each test owns; write it in the file's existing style (`mailer`, `Counting`, `Hasty` at lines 179-243 and 1653-1694 are the models).

- [ ] **Step 2: Run them and confirm each fails for the right reason**

Run: `cargo test -p charpente-sim --test drive -- recover_ 2>&1 | tee $SCRATCH/t1-red.log`
Expected: the four new tests FAIL (the existing `recover_refuses_a_phase_without_one_answer_per_request` matches the filter and passes). The three `Err` tests panic with "the check fails the run" (because `recover` returned `Ok`), and the give-up test fails its `is_none()` assertion.
**Stop condition:** if `recover_fails_a_run_whose_two_jobs_share_a_key` instead panics inside the kernel or the enqueue (the platform refusing the duplicate key before it is stored), `keys_are_per_effect` cannot fire through `Drive`. Stop and report: the spec's set changes.

- [ ] **Step 3: Implement the checks in `recover`**

In `drive.rs`, after the extension loop yields `wall` and before `mark_dead_coverage(sim, kernel.app())?`: take one `kernel.app().db().snapshot()` (map its error with `to_string`), then call the three checks in the Global Constraints order. On `calls_have_durable_jobs`'s `Err(e)`: if `durable_corruption_fired(sim)`, call `sim.give_up("injected corruption cut an enqueue after its call reached the remote", e)` and `return Ok(None)`; otherwise return `Err(e)`. Return the other two checks' `Err` unchanged. Add the three cases and the give-up to `recover`'s `# Errors` doc list, in the shape the list already uses.

- [ ] **Step 4: Run the Drive suite and charpente-sim whole**

Run: `cargo test -p charpente-sim --no-fail-fast 2>&1 | tee $SCRATCH/t1-sim.log; echo "exit ${pipestatus[1]}"; grep -c "^test result: ok" $SCRATCH/t1-sim.log`
Expected: exit 0, and the `test result: ok` count equals the number of test binaries in the log (count the `Running` lines). The four new tests pass, and no existing test changed.

- [ ] **Step 5: Prove each call is load-bearing**

First save the working file: `cp crates/charpente-sim/src/drive.rs $SCRATCH/drive.rs.good`. Then, for each of the three calls and the `durable_corruption_fired` branch in turn: remove it, run `cargo test -p charpente-sim --test drive -- recover_`, confirm exactly its own new test fails, and restore with `cp $SCRATCH/drive.rs.good crates/charpente-sim/src/drive.rs && touch crates/charpente-sim/src/drive.rs`. Never restore with `mv`, which keeps the old mtime and lets cargo reuse the mutated build, and never with `git checkout`, which would discard the uncommitted Step 3. After the last restore, `cmp` the file against the saved copy. Record the four red results in the commit body.

- [ ] **Step 6: Update the GOLDEN line and README's quote**

In `crates/charpente-cli/tests/demo.rs:46` and `README.md:119`, change only `…reached the gateway (first: --seed 0x11c23979e56d3608)` to `…reached the remote (first: --seed 0x11c23979e56d3608)`. The count `1`, the seed and every other line stay as they are.

Run: `cargo test -p charpente-cli --test demo --no-fail-fast 2>&1 | tee $SCRATCH/t1-demo.log; echo "exit ${pipestatus[1]}"`
Expected: exit 0. If any other GOLDEN line differs, that is a universe that moved: stop and report it (spec, Evidence).

- [ ] **Step 7: Docs and CHANGELOG**

- GUIDE §8, drive section: before "**Coverage for a Dead path.**", a paragraph saying that after the tail `recover` runs `keys_are_per_effect`, `calls_have_durable_jobs` and `dead_hooks_succeeded`, returns a failure as its `Err` in the check's words, and gives the run up under durable corruption. Follow it with a pinned block:
  ````
  <!-- pinned -->
  ```text
  injected corruption cut an enqueue after its call reached the remote
  ```
  ````
- GUIDE §8 checker list: `keys_are_per_effect`, `calls_have_durable_jobs` and `dead_hooks_succeeded` each say "`Drive::recover` runs it after the tail; a hand driver calls it." `outbound_settles_by_horizon` says it is for hand drivers, because `recover` returns only once nothing it would judge is left.
- `doc_truth.rs`: `PINNED_FLOOR` to 64, and append to its comment: `64 with `Drive::recover`'s corruption give-up (2026-11-26).`
- CHANGELOG 0.7.0, a new breaking-changes bullet: `**`Drive::recover` runs the platform checks after the tail.**` It runs `keys_are_per_effect`, `calls_have_durable_jobs` and `dead_hooks_succeeded`, and returns a failure as its `Err` in the check's own words. When a call lost its durable job and durable corruption fired, it gives the run up under the reason above. What breaks: a `Drive` workload that broke one of the three rules without calling its check now fails, and the double-charge batch's corruption give-up now carries this reason. Cite GUIDE §8.

Run: `cargo build -p guide-examples && cargo test -p guide-examples --no-fail-fast 2>&1 | tee $SCRATCH/t1-guide.log; echo "exit ${pipestatus[1]}"`
Expected: exit 0, `doc_truth` green.

- [ ] **Step 8: fmt, clippy, commit**

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings`
Expected: both clean.

```bash
git add crates/charpente-sim/src/drive.rs crates/charpente-sim/tests/drive.rs crates/charpente-cli/tests/demo.rs README.md docs/GUIDE.md crates/guide-examples/tests/doc_truth.rs CHANGELOG.md
git commit -m "drive: recover runs keys_are_per_effect, calls_have_durable_jobs and dead_hooks_succeeded after the tail" -m "<the four red results from Step 5; the one GOLDEN line that moved and why>"
```

---

### Task 2: Delete the copies `recover` now makes redundant

**Files:**
- Modify: `examples/double-charge/tests/sim.rs:62-73` and its `use charpente_sim::checks::{…}` (lines 17-20)
- Modify: `crates/charpente/examples/modelled_remote.rs:240-249` and its imports (lines 51-54)
- Modify: `crates/charpente-cli/templates/sim.rs.tmpl:171` (`dead_hooks_succeeded(&snap)?;`) and its import (line 35)
- Modify: `docs/GUIDE.md` §8's outbound example (lines ~5150-5285: imports and the checker calls after `recover`) and the prose around it ("calls the three outbound checkers", "passes all three checkers having checked")
- Modify: `crates/charpente/examples/api.rs:1191-1192` (comment only)
- Unchanged on purpose: hand drivers (`crates/outbound-fixture/tests/sim.rs`, `crates/charpente/tests/outbound.rs`, `examples/invoicing`), and `charpente-sim/tests/drive.rs`'s `outbound_settles_by_horizon` assertions (they test `recover`'s own loop).

**Interfaces:**
- Consumes: Task 1's `recover` (three `Err` cases, the give-up as `Ok(None)`).
- Produces: nothing new.

- [ ] **Step 1: Delete the copies after `recover`**

In double-charge, `modelled_remote` and GUIDE's example, delete the `keys_are_per_effect`, `outbound_settles_by_horizon` and `calls_have_durable_jobs` calls and the corruption give-up after `recover`, plus the imports they leave unused. Keep what comes after them (the app's own property). In the scaffold template, delete `dead_hooks_succeeded(&snap)?;` and its import. In GUIDE's prose, say that `recover` has already judged the three outbound rules, so the example goes straight to its own property. In `api.rs`, change the comment above `mark_dead_coverage` to: `` `Drive::recover` runs this and the three checks above itself; a hand driver calls them on its last kernel's App.``

- [ ] **Step 2: Run everything these files feed**

Run, each piped through `tee` with `${pipestatus[1]}` checked:
- `cargo build -p guide-examples` (GUIDE's example compiles)
- `cargo build -p charpente --examples`
- `cargo test -p charpente-cli --test demo --no-fail-fast` (double-charge's patched copy: GOLDEN unchanged from Task 1)
- `cargo test -p charpente-cli --test new --no-fail-fast` (the scaffold builds, lints clean and passes)
- `cargo test -p guide-examples --no-fail-fast`

Expected: all exit 0. The GOLDEN must match Task 1's byte for byte. If it doesn't, the deleted copies were doing something `recover` doesn't: stop and report.

- [ ] **Step 3: fmt, clippy, commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
git add examples/double-charge/tests/sim.rs crates/charpente/examples/modelled_remote.rs crates/charpente-cli/templates/sim.rs.tmpl docs/GUIDE.md crates/charpente/examples/api.rs
git commit -m "examples, scaffold, GUIDE: drop the checks recover now runs, and outbound_settles_by_horizon, which cannot fail after it"
```

---

### Task 3: Evidence and the ledger entry

**Files:**
- Modify: `docs/LEDGER.md` (a new dated entry at the end)
- Modify: `docs/superpowers/specs/2026-11-26-drive-runs-platform-checks-design.md` (status: built, with a pointer to the ledger entry)

- [ ] **Step 1: Workspace suite, counted**

Run per package (the whole workspace can pass 600 s): for each workspace member with tests, `cargo test -p <pkg> --no-fail-fast 2>&1 | tee $SCRATCH/t3-<pkg>.log`, record `${pipestatus[1]}`, then `grep -c "^test result: ok"` plus `grep -c "sim result: ok"`. Then `scripts/edge-harness.sh` the same way.
Expected: every package exits 0; the total of `ok` lines equals the total of test binaries (the 0.7.0 baseline was 109 of 109). fmt and clippy clean.

- [ ] **Step 2: Fresh master seeds**

Draw one fresh master seed (e.g. `printf '0x%016x\n' $(od -An -N8 -tu8 /dev/urandom)`) and run `cargo run -p charpente-cli -- test --sim --runs 500 --master-seed <it> --dir <app>` for `crates/sim-fixture`, `examples/invoicing`, `crates/outbound-fixture`, and a fresh `charpente new` scaffold in a scratch directory. Run the double-charge patched copy through `cargo test -p charpente-cli --test demo` (its other-master-seeds test runs eight more).
Expected: every check passes, each run's tested count is recorded, and so is any give-up under the new reason.

- [ ] **Step 3: Ledger entry and spec status, commit**

The LEDGER entry, `## 2026-11-26 — Drive::recover runs the platform checks (0.7.0, unreleased)`, records: why (the twin review, the spec path); what landed (both commit shas); the four red results from Task 1 Step 5; the one GOLDEN line that moved and why; the evidence from Steps 1 and 2 with seeds and counts; and that `outbound_settles_by_horizon` was found unfireable after `recover`. Mark the spec `Status: built 2026-11-26 (LEDGER 2026-11-26)`.

```bash
git add docs/LEDGER.md docs/superpowers/specs/2026-11-26-drive-runs-platform-checks-design.md
git commit -m "ledger: Drive::recover runs the platform checks — evidence at <Task 2 sha>"
```
