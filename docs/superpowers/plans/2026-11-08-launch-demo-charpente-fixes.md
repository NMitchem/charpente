# Launch demo, step 1: the outside agents' three fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the three first-five-minutes problems outside agents hit on 0.4.0, before anyone is pointed at charpente.

**Architecture:** Three small changes in `charpente-cli` and `docs/GUIDE.md`, each made red-first and committed separately, then one ledger entry that also closes 0.4.0's CI follow-up.

**Tech Stack:** Rust 1.99, the existing `charpente-cli` test harness (`tests/cli.rs`'s cargo shim), `guide-examples`' doc gates.

**Spec:** `docs/superpowers/specs/2026-11-06-launch-demo-design.md` §9 step 1.

## Global Constraints

- Follow CLAUDE.md: hard rule 5 (an error names the contract, the site and the fix), hard rule 6 (public behaviour changes update the docs in the same commit), commits go straight to `main`, and commits carry **no Co-Authored-By trailer**.
- Another session may be writing to this tree. Before each commit, run `git status --short` and commit only the paths you named.
- The shell is zsh: use `${pipestatus[1]}`, never `${PIPESTATUS[0]}`. Pipe cargo through `tee`, never `tail`.
- Green means counting: every `test result:` line reads `ok`, and the count of `test result: ok` equals the count of `test result:`.
- CHANGELOG entries go under a new top heading `## 0.5.0 (unreleased)`, which the first task to edit CHANGELOG creates (with `### Changed` / `### Fixed` subheadings as needed).

## Review Focus

1. **`--dir` pointing at a member of a multi-crate app workspace.** The refusal must name the package at `--dir` rather than an arbitrary one. Pinned in Task 1, test 5.
2. **A workspace where `cargo metadata` fails** (a broken manifest). The verb must keep today's behaviour, where cargo's own error surfaces on the next cargo command, rather than a refusal that blames the wrong thing. Pinned in Task 1, test 4.
3. **Inside charpente's own repository, nothing changes.** Every existing `--mutants` shim test must stay green once the shim workspace models charpente's (Task 1, step 2).
4. **A scaffold that later gains or loses a sim property.** The usage line must then fail its pin rather than drift again (Task 2).
5. **GUIDE's Rust blocks still compile** after the prose edits (Tasks 1 and 3 run `cargo build -p guide-examples`).

---

### Task 1: `--mutants` outside charpente's own workspace is refused, naming the fix

Plain `charpente test --mutants` inside an app crate judged the mutants with `charpente-core` and `charpente-sim`'s published test suites, which do not build outside this repository (21 compile errors, reported by agent B), and never said `--test-package` was the fix. Without `--file` it also mutated `crates/charpente-core/src/log.rs` and `crates/charpente-sim/src/checks.rs`, which are not in an app's tree. The verb now refuses before anything runs.

**Files:**
- Modify: `crates/charpente-cli/src/mutants.rs`. Add two fields to `Opts`, have `parse` set them, add the refusal function, call it in `run`, and change the `--test-package` line of `USAGE`.
- Modify: `crates/charpente-cli/tests/cli.rs`. The shim workspace gains two packages, and four tests are added. Three unit tests go in `mutants.rs`'s test module.
- Modify: `docs/GUIDE.md` §8, "Finding a one-sided property" (two sentences).
- Modify: `CHANGELOG.md`.

**Interfaces:**
- Produces, in `mutants.rs`:
  - `Opts` gains `files_defaulted: bool` and `test_packages_defaulted: bool`. They are true exactly when `parse` filled `files` from `DEFAULT_TARGETS` or `test_packages` from `DEFAULT_TEST_PACKAGES`.
  - `fn outside_workspace_refusal(meta: &serde_json::Value, opts: &Opts) -> Option<String>`.
    - Returns `None` when neither field was defaulted, or when any entry of `meta["packages"]` has `name == "charpente-core"`.
    - Otherwise returns the refusal below.
- In `run`, call it immediately after the `unrewritten_path_refusal_in` check, inside the same `if let Some(meta)` shape. Print the text to stderr and return 2. When `workspace_metadata` returned `None`, skip it, as the path check is skipped.

**The refusal text (exact; tests assert on it):**

```
error: --mutants needs {missing} outside charpente's own workspace: {root} has no member named charpente-core, so the verb's defaults do not exist here — {reasons}. Run:
  charpente test --mutants --test-package {pkg} {files}
(GUIDE §8, "Finding a one-sided property")
```

- **`{missing}`** is `--test-package and --file`, `--test-package` or `--file`, by which fields were defaulted.
- **`{root}`** is `meta["workspace_root"]`.
- **`{reasons}`** joins with `; `, in this order:
  - when `test_packages_defaulted`: `without --test-package it would judge every mutant with charpente-core and charpente-sim's tests, and their published suites do not build outside charpente's repository`
  - when `files_defaulted`: `without --file it would mutate crates/charpente-core/src/log.rs and crates/charpente-sim/src/checks.rs, which are not in this tree`
- **`{pkg}`**, in the first form that applies:
  - the given `--test-package` names, joined with ` --test-package `, when `test_packages` was not defaulted;
  - otherwise the `name` of the package whose `manifest_path`'s parent directory equals `std::path::absolute(opts.dir)`;
  - otherwise the only package's name, when `packages` has exactly one;
  - otherwise `<your package>`.
- **`{files}`** is `--file <the file to mutate, such as src/jobs.rs>` when `files_defaulted`, otherwise each given file as `--file <path>`, space-joined.

**Steps:**

- [ ] **Step 1: Record the baseline.** Run `cargo test -p charpente-cli --test cli --no-fail-fast 2>&1 | tee /tmp/cli-before.log` and note the passed count from `test result:`.

- [ ] **Step 2: Model charpente's workspace in the shim.** In `run_mutants_with_cargo_shim_in`, change the scratch-package loop from `["pkg-a", "pkg-b", "app"]` to `["pkg-a", "pkg-b", "app", "charpente-core", "charpente-sim"]`. Re-run the cli suite. It must show the same passed count as Step 1 (no behaviour change yet). If any test's output changed, stop and report it; don't edit that test.

- [ ] **Step 3: Write the failing tests** in `tests/cli.rs`, beside the other shim tests and under `#[cfg(unix)]`. Each test writes its own metadata file to a fresh temp path and passes it through the helper's `envs` as `("SHIM_METADATA", &path)`, which overrides the default because `.envs` is applied last. Use the workspace root `/nonexistent-app-ws`.
  1. `mutants_outside_charpentes_workspace_without_test_package_or_file_is_refused_naming_both`.
     - Metadata: one package `app` with `manifest_path` `/nonexistent-app-ws/app/Cargo.toml`. Extra args: none.
     - Assert: exit code 2.
     - Assert: stderr contains `--mutants needs --test-package and --file outside charpente's own workspace`, `/nonexistent-app-ws has no member named charpente-core`, `charpente test --mutants --test-package app --file <the file to mutate, such as src/jobs.rs>` and `(GUIDE §8, "Finding a one-sided property")`.
     - Assert: no recorded shim line starts with `cargo mutants`. The refusal comes before the install probe.
  2. `mutants_outside_charpentes_workspace_with_test_package_but_no_file_is_refused_naming_file`.
     - Same metadata. Extra args: `["--test-package", "app"]`.
     - Assert: exit 2; stderr contains `--mutants needs --file outside` and `--test-package app --file <the file to mutate, such as src/jobs.rs>`.
     - Assert: stderr does not contain `without --test-package`.
  3. `mutants_outside_charpentes_workspace_with_both_flags_goes_ahead`.
     - Same metadata. Extra args: `["--test-package", "app", "--file", "app/src/lib.rs", "--timeout", "60"]`.
     - Assert: some recorded line equals `cargo mutants --version`.
     - Assert: stderr does not contain `has no member named charpente-core`.
  4. `mutants_with_unreadable_metadata_keeps_cargos_own_error` (Review Focus 2).
     - `SHIM_METADATA` points at a file holding `not json`. Extra args: none.
     - Assert: stderr does not contain `has no member named charpente-core`.
     - Assert: a recorded line equals `cargo mutants --version`. The run went on to the probe, as today.

  Then add three unit tests to `mutants.rs`'s existing `#[cfg(test)]` module. Each builds `Opts` with `parse(&["--dir".into(), <dir>])` (plus flags), builds `meta` with `serde_json::json!`, and calls `outside_workspace_refusal` directly. The pure function is tested here because a shim run cannot know its scratch path in advance.
  5. `outside_refusal_names_the_package_at_dir_in_a_multi_crate_app` (Review Focus 1).
     - `<dir>` is a fresh temp directory `<t>/worker`, created on disk.
     - `meta` has `workspace_root` `<t>`, with packages `api` at `<t>/api/Cargo.toml` and `worker` at `<t>/worker/Cargo.toml`.
     - Assert: the text contains `--test-package worker` and not `--test-package api`.
  6. `outside_refusal_is_none_inside_charpentes_workspace`.
     - `meta` lists a package named `charpente-core`.
     - Assert: `None`.
  7. `outside_refusal_says_your_package_when_none_is_at_dir`.
     - `<dir>` is `<t>`; packages `api` and `worker` as in test 5.
     - Assert: the text contains `--test-package <your package>`.

- [ ] **Step 4: Run them and watch them fail.** Run `cargo test -p charpente-cli --test cli mutants_ -- --nocapture`. Tests 1 and 2 fail (no refusal yet), and tests 3 and 4 pass. The unit tests 5-7 don't compile until Step 5 adds the function. That counts as red; confirm the error names `outside_workspace_refusal`.

- [ ] **Step 5: Implement.** Add the two `Opts` fields, set them in `parse` where the defaults are filled (`mutants.rs:1512-1520`), add `outside_workspace_refusal`, and call it in `run`.

- [ ] **Step 6: Update `USAGE`'s `--test-package` line.** Replace `Without this, the verb judges every mutant with charpente-core and charpente-sim's tests — the default target set's oracles — so a --file in any other crate must name its own package here, or its mutants are judged by suites that never exercise them and all report MISSED.` with `Required outside charpente's own workspace. Inside it, the default is charpente-core and charpente-sim's tests — the default target set's oracles — so a --file in any other crate must name its own package here, or its mutants are judged by suites that never exercise them and all report MISSED.` Also change the `--file` line's `(repeatable; default: the oracle-bearing set)` to `(repeatable; required outside charpente's own workspace, where the default is the oracle-bearing set)`.

- [ ] **Step 7: Update GUIDE §8.**
  - At `docs/GUIDE.md:5057-5058`, replace `(without it the verb uses charpente's own suites; "Scope is part of the claim",\nbelow)` with `(outside charpente's own repository it is required, as \`--file\` is: without either, the verb exits 2 before running anything, naming the missing flags and your package; "Scope is part of the claim", below)`.
  - At `docs/GUIDE.md:5128`, replace `without this, the verb judges every mutant with` with `without this, inside charpente's own repository, the verb judges every mutant with`.
  - Reflow both paragraphs to the file's line width.

- [ ] **Step 8: Run the gates.**
  - `cargo test -p charpente-cli --no-fail-fast 2>&1 | tee /tmp/cli-after.log`. Every `test result:` is ok, the cli binary's passed count is Step 1's plus 4, and the unit-test binary gains 3.
  - `cargo build -p guide-examples`, then `cargo test -p guide-examples --no-fail-fast 2>&1 | tee /tmp/ge.log`, all ok.
  - `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 9: CHANGELOG.** Under `## 0.5.0 (unreleased)` / `### Changed`, add one bullet. It is bold-led, in the file's existing voice, says what agents hit (21 compile errors, no named fix) and what happens now (exit 2 before the install check, naming the missing flags, your package and the command), and ends `GUIDE §8 says so.`

- [ ] **Step 10: Commit.**
```bash
git add crates/charpente-cli/src/mutants.rs crates/charpente-cli/tests/cli.rs docs/GUIDE.md CHANGELOG.md
git commit -m "mutants: refuse a run outside charpente's own workspace that names no --test-package or --file, naming the flags, the package at --dir and the command, before the install check — agent B's 21 compile errors on 0.4.0 with no named fix"
```

---

### Task 2: `charpente new`'s usage line counts the scaffold's sim properties

**Files:**
- Modify: `crates/charpente-cli/src/new.rs:9-11` (`USAGE`), and the `#[cfg(test)] mod tests` at `new.rs:160`.
- Modify: `CHANGELOG.md`.

- [ ] **Step 1: Write the failing test** in `new.rs`'s test module:

```rust
#[test]
fn usage_counts_the_templates_sim_properties() {
    let block = SIM_RS.split("sim_tests! {").nth(1).unwrap().split('}').next().unwrap();
    let n = block.lines().filter(|l| l.trim_end().ends_with(',')).count();
    let word = ["zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"][n];
    assert!(
        USAGE.contains(&format!("and {word} sim properties")),
        "templates/sim.rs.tmpl declares {n} sim properties, but USAGE says otherwise:\n{USAGE}"
    );
}
```

- [ ] **Step 2: Run it and watch it fail.** Run `cargo test -p charpente-cli --bin charpente usage_counts`. Expected: FAIL, with a message saying the template declares 3. If the crate's unit tests live under `--lib` rather than `--bin charpente`, use the target that holds `new.rs`, found with `cargo test -p charpente-cli usage_counts`.

- [ ] **Step 3: Fix.** In `USAGE`, change `and two sim properties` to `and three sim properties`.

- [ ] **Step 4: Run it and watch it pass.** Then run `cargo test -p charpente-cli --no-fail-fast 2>&1 | tee /tmp/cli2.log`; all ok.

- [ ] **Step 5: CHANGELOG.** Under `### Fixed`: `` `charpente new`'s usage line says the scaffold has three sim properties. It said two from 0.3.0, when the event check made three; a unit test now counts the template's checks. ``

- [ ] **Step 6: Commit.**
```bash
git add crates/charpente-cli/src/new.rs CHANGELOG.md
git commit -m "new: the usage line says three sim properties, as the scaffold has had since 0a06e5eb; a unit test counts the template's sim_tests! entries so it cannot drift again"
```

---

### Task 3: GUIDE §8 says what a re-send's timing means for a check

**Files:**
- Modify: `docs/GUIDE.md` §8, Drive step 4 ("Re-send what went unanswered"), right after the sentence ending `is sent again at the tail's start.` (`docs/GUIDE.md:3389-3390`).
- Modify: `CHANGELOG.md`.

- [ ] **Step 1: Insert, inside the numbered item with the item's 3-space indent:**

```
   A re-sent request takes effect at the tail's start, not at its offset, so
   work its offset came before may already have run by then: a cancel drawn
   ahead of a reminder's due time that went unanswered can land after the
   reminder was sent. A check that judges when a request took effect judges a
   re-sent one (`recovered.resent`) at the tail's start.
```

- [ ] **Step 2: Run the gates.** `cargo build -p guide-examples`, then `cargo test -p guide-examples --no-fail-fast 2>&1 | tee /tmp/ge3.log`. Every `test result:` is ok.

- [ ] **Step 3: CHANGELOG.** Under `### Changed`: `GUIDE §8 says a request \`Drive\` re-sends takes effect at the tail's start, not at its offset, so a check that judges timing judges it there (agent A found it through a sim failure).`

- [ ] **Step 4: Commit.**
```bash
git add docs/GUIDE.md CHANGELOG.md
git commit -m "guide: §8 says a re-sent request takes effect at the tail's start, so a check that judges timing judges it there — agent A found it through a sim failure on 0.4.0"
```

---

### Task 4: The full gate, and the ledger entry

**Files:**
- Modify: `docs/LEDGER.md` (a new entry at the end).

- [ ] **Step 1: Run the full gate.** The workspace suite outruns the tool ceiling, so run it with `run_in_background` and wait for the notification:

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente && cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace --no-fail-fast > /tmp/gate.log 2>&1; echo "TEST_EXIT=$?" >> /tmp/gate.log
```

- [ ] **Step 2: Count.**
  - `grep -c "^test result:" /tmp/gate.log` and `grep -c "^test result: ok" /tmp/gate.log` must be equal.
  - `grep "TEST_EXIT=" /tmp/gate.log` must read `TEST_EXIT=0`.
  - Record both counts and the passed total (sum the `N passed` fields).

- [ ] **Step 3: Gather 0.4.0's CI close-out.**
  - Run `gh run list --limit 15 --json databaseId,workflowName,headSha,conclusion,createdAt`.
  - Ids known on 2026-11-06:
    - `ci.yml` run `41331921799` on `535bcbb9`: success;
    - `ci.yml` run `41332202376` on `5d43d2f3`: success;
    - the first nightly `edge-harness.yml` on the tip, `41486954475` on `5d43d2f3` (2026-11-02): success;
    - green each night through `42238713546` (2026-11-06).
  - Re-verify each id's conclusion, and extend the "through" id to the latest green nightly.
  - Note the fact the entry must carry: the nightlies ran on `5d43d2f3`, whose diff over the published `535bcbb9` is records only (CHANGELOG, PLAN, README, LEDGER).

- [ ] **Step 4: Write the entry.** Add `## <today> — the outside agents' 0.4.0 retest: three fixes, and 0.4.0's CI close-out` with these bullets:
  - The retest. Agents A and B re-ran their builds on 0.4.0, and both judged it better. Name what each verified, and what each found still open.
  - The three fixes, each with its commit sha, and the counts from Step 2.
  - The launch-demo spec at `eb79df46` and this plan's path.
  - The CI close-out from Step 3.

- [ ] **Step 5: Commit.**
```bash
git add docs/LEDGER.md
git commit -m "ledger: the outside agents' 0.4.0 retest — three fixes (<sha1>, <sha2>, <sha3>), the gate at <N>/<N>, and 0.4.0's CI close-out (ci 41331921799, edge-harness nightlies green on 5d43d2f3 through <id>)"
```
