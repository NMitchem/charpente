# 0.2.1 — readable sim output — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `charpente test --sim` output a stranger can read cold (cargo-test-shaped, with skipped runs' reasons counted), plus `charpente --version` and two README fixes, released as a semver-compatible 0.2.1 and accepted by a stranger re-run.

**Architecture:** Rendering only. `charpente-sim` gains `Sim::give_up_with(reason, detail)` and carries each run's reason (and, under `--seed`, its detail) from the universe to the harness through crate-private types; the harness renders a private `Batch` (the unchanged public `Report` plus per-check extras), so no public type gains a field. Nothing that draws from a seed changes.

**Tech Stack:** Rust 1.99 (pinned), `charpente-sim` harness (`crates/charpente-sim/src/{lib,harness,checks}.rs`), `charpente-cli`, cargo-semver-checks 0.51.0 (verification only), the launch-run tools (`docs/experiments/2026-10-launch-run/tools/`).

**Spec:** `docs/superpowers/specs/2026-10-17-readable-sim-output-design.md` (d972ef9d). Executors read it before Task 1.

**Workspace:** `SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-17-readable-sim-output` (gitignored; ledger `$SDD/progress.md`).

## Global Constraints

- **Rendering only.** No change to seed derivation, fault draws, `run_universe`'s workload execution, or which runs pass, fail or give up. Oracle 3 (Task 1's script) must report every batch unchanged at every task that touches `charpente-sim`, the template, the fixtures or invoicing.
- **Exit codes unchanged:** 0 green (all-untested included), 1 any failure or unmet `require`, 2 usage/build/infrastructure/no test matched.
- **Semver-compatible with 0.2.0.** No field added to, and no signature changed on, any existing public item: `harness::{Report, TestOutcomes, Failure, SimTest, HarnessArgs}`, `UniverseEnd`, `Outcome`, `Sim::give_up`, `Sim::gave_up`, `Report::to_json`. New data rides in new crate-private types; new public API is additive (`Sim::give_up_with`). Checked by cargo-semver-checks 0.51.0 in Task 8.
- **`Report::to_json()` output stays byte-identical to 0.2.0's.** The harness's own `--format json` uses the extended renderer (Task 5).
- **Copy is binding.** Every product-surface string in Tasks 3, 4, 5 and 7 is exact; tests pin it (hard rule 5). Where this plan gives no string, the spec's §3 shape and §7 apply.
- **Ordering:** checks in registry order; a check's reasons by count descending, then reason text ascending (byte order); output identical for any `--jobs`.
- **Hard rule 6:** docs and `CHANGELOG.md` land in the task whose code they describe.
- **Commits:** on `main`, named paths only, **no trailers** (CLAUDE.md). Check `git status` and `git log -1` immediately before each commit; stop if HEAD moved or unrelated files changed.
- **Verification:** implementers run per-package suites (`--no-fail-fast`, count `^test result: ok` from a `tee`d log, exit via `${pipestatus[1]}` — zsh). Never `cargo test --workspace` (exceeds the tool ceiling); the controller runs the full gate in `~/Documents/GitHub/charpente-gate`. Bound any long run with `perl -e 'alarm N; exec @ARGV' --` (no `timeout` on this machine).
- **Network:** no requests beyond cargo's own (build, install, search, the semver baseline) and `docker pull`. Never put any email address in a request; if a contact is ever required, it is `noah@noahmitchem.com`.

## Review Focus

1. **A reason or detail containing a newline or tab** (apps write free text): the human output stays one line per reason — `\n`, `\r` and `\t` print as those two-character escapes. Test in Task 3 (reasons) and Task 4 (detail).
2. **"rerun exactly this batch" after `--filter` or a non-default `--universes`:** the printed command must carry both, or it reruns a different batch. Test in Task 3.
3. **An app still on `give_up(format!("…: {e}"))`** (every 0.2.0 app): one distinct reason per error text — the cap and the "and K more" line keep it to nine lines per check (8 reasons + 1), deterministic under any `--jobs`. Test in Task 3.
4. **One check with failures, skipped runs and an unmet `require` at once:** its line says all three and the result is FAILED. Test in Task 3.
5. **`--seed` on a run that gave up via the old `give_up`** (no detail): prints the reason and no `detail:` line; via `give_up_with`, prints both. Test in Task 4.

---

### Task 1: Oracle-3 baseline, and 0.2.1 with the scaffold requiring the full version

**Files:**
- Create: `$SDD/oracle3.sh`, `$SDD/oracle3-filter.jq`, `$SDD/baseline/*.json` (not committed)
- Modify: `Cargo.toml` (workspace `version`, the three inter-crate `version`s), `crates/charpente-cli/Cargo.toml`, `crates/ts-fixture-app/Cargo.toml` (the `charpente-tsgen` pins), `crates/charpente-cli/src/new.rs` (`dependency_version` and its unit test), `crates/charpente-cli/tests/new.rs`, `docs/GUIDE.md` (§1's manifest block; the `describe` JSON sample's `charpente_version`), `crates/charpente/tests/describe.rs`, `crates/charpente-cli/tests/describe.rs`, `examples/invoicing/tests/describe_golden.rs`, `Cargo.lock`, `CHANGELOG.md`

**Interfaces:**
- Produces: `$SDD/oracle3.sh <label>` → exit 0 when every batch matches `$SDD/baseline/`, 1 on any change (diff in `$SDD/oracle3-<label>/`), 2 on a launch error. Every later task that touches sim behaviour runs it.
- Produces: `new.rs` `fn dependency_version() -> String` returning `env!("CARGO_PKG_VERSION")` whole (`"0.2.1"`).

- [ ] **Step 1: Write `$SDD/oracle3.sh` and `$SDD/oracle3-filter.jq`.** Model: `.superpowers/sdd/2026-10-13-outbound-effects/oracle3.sh` (zsh; `rc`, never `status`; refuse an empty report). Six batches, each `charpente test --sim --dir <dir> --universes 256 --master-seed <seed> --format json`:
  - `crates/sim-fixture` at `0x912313adf8115ad0` and `0x483e230baed4922b`
  - `examples/invoicing` at the same two seeds
  - `crates/outbound-fixture` at `0xf207f4dcbfade216`
  - the scaffold: `target/debug/charpente new myapp` into a fresh `$SDD/scaffold-<label>/`, with a `[patch.crates-io]` pointing `charpente`, `charpente-sim`, `charpente-core` and `charpente-macros` at the workspace paths (copy the approach `crates/charpente-cli/tests/new.rs` uses), at `0x1`

  The filter projects outcomes so Task 5's additive keys are not a diff:
  ```jq
  { master_seed, universes, verdicts, gave_up, coverage, missing_coverage,
    outcomes: [.outcomes[] | {test, verdicts, gave_up}],
    failures: [.failures[] | {test, seed, message}] }
  ```
- [ ] **Step 2: Record the baseline at the base sha.** Run the script with the baseline written to `$SDD/baseline/` (a `--baseline` mode or a one-line variant; your choice), at the current HEAD before any edit. Expected: six JSON files, each non-empty. Record the sha and each batch's `verdicts`/`gave_up` in `$SDD/progress.md`.
- [ ] **Step 3: Prove the oracle can fail.** Temporarily edit one baseline file's `verdicts` by 1, run `oracle3.sh negcheck`, expect exit 1 naming that batch; restore the file and rerun, expect exit 0.
- [ ] **Step 4: Change the scaffold's requirement test first.** In `new.rs`'s unit test, `assert_eq!(dependency_version(), env!("CARGO_PKG_VERSION"));` (it was `"0.2"`). Run `cargo test -p charpente-cli --lib new` — expect FAIL.
- [ ] **Step 5: Implement** `dependency_version()` as the full package version; update its doc comment (the requirement is the CLI's own version, so a scaffold never resolves to an older release than the CLI that wrote it — spec §4).
- [ ] **Step 6: Bump to 0.2.1** everywhere 0.2.0's bump touched (`git show 37616d8f --stat` is the list), with GUIDE §1's block now `charpente = "0.2.1"` and `charpente-sim = "0.2.1"`; `cargo update --workspace` for the lock. `CHANGELOG.md`: the 0.2.0 header becomes `## 0.2.0 (2026-10-17)`, and a new top section:
  ```markdown
  ## 0.2.1 (unreleased)

  ### Changed
  - `charpente new` writes the CLI's full version as the app's requirement (`charpente = "0.2.1"`), so a new app never resolves to a release older than the CLI that wrote it.
  ```
- [ ] **Step 7: Verify.** `cargo test -p charpente-cli --no-fail-fast` (includes the GUIDE §1 byte-identity test and the scaffold builds), `cargo test -p guide-examples --no-fail-fast`, `cargo test -p charpente --test describe`, `cargo test -p invoicing --test describe_golden`, `cargo test -p ts-fixture-app --no-fail-fast`; all `test result: ok`, 0 FAILED. `cargo publish --workspace --dry-run --no-verify` exit 0, six crates at 0.2.1. `oracle3.sh task1` exit 0.
- [ ] **Step 8: Commit** (named paths; no `$SDD` files):
  `release: 0.2.1 (unreleased) — the workspace and every install-facing version, and charpente new requires the CLI's full version; CHANGELOG dates 0.2.0`

---

### Task 2: `Sim::give_up_with` — a stable reason and a per-run detail

**Files:**
- Modify: `crates/charpente-sim/src/lib.rs` (the world's give-up fields, `Sim::give_up`'s guard, the new method, `end_of`, `run_universe` and its callers)

**Interfaces:**
- Produces (public, additive): `pub fn give_up_with(&self, reason: &'static str, detail: impl std::fmt::Display)` on `Sim`. `&'static str` is deliberate: the grouping key cannot be built per run, so the type keeps it stable (spec §5 A).
- Produces (crate-private): `pub(crate) fn gave_up_detail(&self) -> Option<String>` on `Sim`; `pub(crate) struct Ended { pub(crate) end: UniverseEnd, pub(crate) detail: Option<String> }`; `pub(crate) fn run_universe(seed: u64, run: …same as today…) -> Result<Ended, (FaultConfig, BTreeMap<&'static str, u64>, String)>`. Existing callers that need only the end take `.end`.
- Unchanged: `Sim::give_up(reason: impl Into<String>)`, `Sim::gave_up() -> Option<String>` (returns the reason for either form), `Outcome`, `UniverseEnd`.

- [ ] **Step 1: Write the failing tests** in lib.rs's test module:
  - `give_up_with_keeps_the_reason_and_the_detail_apart` — after `sim.give_up_with("the store hit fault fallout", "injected sync error at seq 14")`: `sim.gave_up() == Some("the store hit fault fallout".into())` and `sim.gave_up_detail() == Some("injected sync error at seq 14".into())`.
  - `the_first_give_up_wins_whichever_form_it_takes` — `give_up_with("a", "x")` then `give_up("b")`: reason `"a"`, detail `"x"`; and in a fresh sim `give_up("b")` then `give_up_with("a", "x")`: reason `"b"`, detail `None`.
  - `give_up_with_marks_the_gave_up_key_once_per_universe` — two calls, `coverage()["universe:gave-up"] == 1`.
  - `a_universe_that_gave_up_with_a_detail_ends_carrying_it` — `run_universe` on a workload calling `give_up_with`: `Ok(ended)` with `ended.end.outcome == Outcome::GaveUp { reason: "…".into() }` and `ended.detail == Some(…)`.
- [ ] **Step 2: Run** `cargo test -p charpente-sim --lib give_up` — expect compile errors naming `give_up_with`/`gave_up_detail`/`Ended`.
- [ ] **Step 3: Implement.** One first-call guard for both methods; `give_up` leaves the detail `None`. Doc comment on `give_up_with`: why the reason is `&'static str` (it is what `charpente test --sim` groups and counts), that the detail shows only on a single-seed replay, and the same sticky/`return` rule `give_up`'s doc states (cross-reference it rather than copy it).
- [ ] **Step 4: Run** `cargo test -p charpente-sim --no-fail-fast` — all ok. `$SDD/oracle3.sh task2` exit 0.
- [ ] **Step 5: CHANGELOG** under 0.2.1, `### Added`: `` `Sim::give_up_with(reason, detail)`: a give-up whose reason is a fixed phrase `charpente test --sim` groups and counts, with the per-run detail shown when one seed is replayed. `Sim::give_up` is unchanged. ``
- [ ] **Step 6: Commit:** `sim: Sim::give_up_with — a fixed reason the harness can group, and a per-run detail a single-seed replay shows (additive; give_up unchanged)`

---

### Task 3: The batch's human output

**Files:**
- Modify: `crates/charpente-sim/src/harness.rs` (`run`, `run_batch`, `tally`, `write_human`, the startup `eprintln!`, the coverage-requirement message; tests), `crates/charpente-cli/tests/cli.rs` (the pins on the old lines), `docs/GUIDE.md` (§8 "Giving up: a universe that reached no verdict": its two sample output blocks and the sentences around them; any other quote of the old batch lines), `CHANGELOG.md`

**Interfaces:**
- Consumes: Task 2's `Ended { end, detail }` from `run_universe`.
- Produces (private to harness.rs, used by Tasks 4 and 5):
  ```rust
  struct Batch { report: Report, checks: Vec<CheckExtras>, rerun: Option<String> }
  struct CheckExtras { skipped: Vec<(String, u64)>, detail: Option<String> }
  const DEFAULT_UNIVERSES: u64 = 256;
  ```
  `checks` is parallel to `report.outcomes` (registry order); `skipped` is sorted per Global Constraints; `rerun` is `Some` in batch mode, `None` under `--seed`; `detail` is filled only under `--seed` (Task 4). `run_batch` and `run_single_seed` return `Batch`; existing tests read `.report`.
- Per check: `tested = verdicts + failures for that test`, `skipped = gave_up`; status `FAILED` if it has a failure or an unmet `require`, else `untested` if `tested == 0`, else `ok`.

**Copy (binding).** `W` = the longest selected check name; dots after a name = `.` × (`W` − its length + 3). Counts are bare numbers; "tested"/"skipped" never pluralize.

- Startup, stderr, replacing today's `sim: … universe(s) per test from master seed …`:
  `running {n} sim check(s), {u} run(s) each, under random injected faults (master seed {seed:#018x})` — with `check`/`checks` and `run`/`runs` chosen by count (no literal `(s)`).
- Coverage requirement, replacing today's text:
  `sim coverage requirement failed in {test}: "{key}" never occurred in {u} runs — raise --universes, adjust the workload, or drop the requirement (a required state that never occurs is an untested claim)`
- Then a blank line and one line per check: `check {name} {dots} {status}  ({counts})`, where `{counts}` is
  - ok: `{t} tested, {s} skipped`, or `{t} tested` when `s == 0`
  - untested: `0 tested, {s} skipped`
  - FAILED: `{t} tested: {f} failed` + (`; {s} skipped` if `s > 0`) + (`; required "{k1}", "{k2}" never occurred` if any unmet) — and with no failures: `{t} tested, {s} skipped; required "{k}" never occurred` (drop `, {s} skipped` when 0)
- If any check skipped any run, a blank line and:
  ```text
  skipped runs hit a fault before reaching the check, so they prove nothing either way:
    {check}
      {count}  {reason}
  ```
  An untested check's heading line is `  {check} — never tested: every run was skipped`. Counts right-aligned to the widest count in that check's list. At most 8 reason lines per check (the scaffold's first check alone can meet 7 distinct reasons, so a stranger sees them all), then `    and {k} more reason(s) ({m} run(s)): --format json lists every one` (pluralized by count). In reasons, `\n`, `\r`, `\t` print as the two-character escapes.
- A blank line, then the result: `sim result: {word}. {p} passed; {f} failed{; {x} untested}. {T} of {N} runs tested a check.` — `{word}` is `FAILED` if any check failed, else `nothing tested` if none passed, else `ok`; `; {x} untested` only when `x > 0`; `T` = Σ tested, `N` = checks × universes.
- Last line: `rerun exactly this batch: charpente test --sim --master-seed {seed:#x}` + ` --universes {u}` when `u != DEFAULT_UNIVERSES` + ` --filter {f}` when a filter was given.
- Failure blocks (`\nsim failure in …`) are unchanged and still come first, then coverage lines, then the check lines.

- [ ] **Step 1: Write the failing tests** (harness.rs test module; fixtures beside the existing `always_gives_up`):
  - `a_batch_prints_cargo_shaped_check_lines_reasons_and_a_rerun` — tests `always_passes` (verdict) and `always_gives_up` (`give_up("the fixture always gives up")`), master `0x1234`, 6 universes, rendered to a `Vec<u8>`; assert the output ends with exactly:
    ```text
    check always_passes ..... ok  (6 tested)
    check always_gives_up ... untested  (0 tested, 6 skipped)

    skipped runs hit a fault before reaching the check, so they prove nothing either way:
      always_gives_up — never tested: every run was skipped
        6  the fixture always gives up

    sim result: ok. 1 passed; 0 failed; 1 untested. 6 of 12 runs tested a check.
    rerun exactly this batch: charpente test --sim --master-seed 0x1234 --universes 6
    ```
  - `a_batch_where_every_check_is_untested_never_reads_ok` — only `always_gives_up`: result line starts `sim result: nothing tested. 0 passed; 0 failed; 1 untested.`
  - `the_rerun_line_carries_the_filter_and_a_non_default_universe_count` — through `run()` with `--filter always_p --universes 6`: last line `rerun exactly this batch: charpente test --sim --master-seed 0x1234 --universes 6 --filter always_p`; with 256 universes and no filter: `… --master-seed 0x1234` and nothing after.
  - `more_than_eight_reasons_print_eight_and_count_the_rest_identically_under_any_jobs` — a fixture that gives up with `format!("fallout: {}", seed % 12)`-style reasons (twelve distinct, 256 universes): exactly 8 reason lines then `    and 4 more reasons (… runs): --format json lists every one`; output bytes identical for `jobs` 1 and 4.
  - `a_check_with_failures_skips_and_an_unmet_requirement_says_all_three` — a fixture that fails on some seeds, gives up on others, with a `require` key never marked: its line is `check … FAILED  ({t} tested: {f} failed; {s} skipped; required "never:marked" never occurred)` with `t`, `f`, `s` read from the report, and the result word is `FAILED`.
  - `a_reason_with_a_newline_stays_on_one_line` — `give_up("line one\nline two\tend")`: the reason line contains `line one\nline two\tend` as literal backslash escapes and the block has no extra line.
  - `the_startup_line_names_checks_runs_and_the_master_seed` — the header text for 2 checks × 6 runs at `0x1234`, and for 1 check × 1 run (singulars). (Factor the header into a `fn startup_line(…) -> String` so it can be asserted without capturing stderr.)
- [ ] **Step 2: Run** `cargo test -p charpente-sim --lib harness` — the new tests fail.
- [ ] **Step 3: Implement** `Batch`/`CheckExtras`, reason counting in `tally`, and `write_human(batch: &Batch, out: &mut dyn Write)` per the copy above. Delete the old give-up and "vacuous" lines. Keep the comment's rule (a batch that adjudicated nothing never reads as an unqualified success) attached to the `nothing tested`/`untested` logic.
- [ ] **Step 4: Re-pin the CLI tests** in `crates/charpente-cli/tests/cli.rs` that read the old lines (`sim_green_run_exits_zero_and_prints_master_seed`, `sim_reports_a_real_give_up_count_over_a_pinned_batch`, `a_batch_where_every_universe_gave_up_is_not_an_unqualified_success`, and any other matching `gave up`, `verdict(s)`, `universe(s) per test`, `adjudicated nothing`) to the new copy, keeping what each test proves.
- [ ] **Step 5: GUIDE §8** "Giving up": replace the two sample blocks with the new output for the same situations and rewrite the surrounding sentences in the new vocabulary (run / tested / skipped / untested), keeping "universe" where GUIDE defines the concept and the `--universes` flag. `cargo build -p guide-examples`.
- [ ] **Step 6: Verify.** `cargo test -p charpente-sim --no-fail-fast`, `cargo test -p charpente-cli --no-fail-fast`, `cargo test -p guide-examples --no-fail-fast`; all ok. `oracle3.sh task3` exit 0.
- [ ] **Step 7: CHANGELOG** under 0.2.1 `### Changed`: `` `charpente test --sim` prints one line per check in `cargo test`'s shape (ok / FAILED / untested, with how many runs tested it and how many were skipped), the skipped runs' reasons with counts, a result line, and the command that reruns exactly the batch. The human output is not a parsed format; `--format json` is. ``
- [ ] **Step 8: Commit:** `sim: the batch output a stranger can read — cargo-test-shaped check lines (ok/FAILED/untested, tested and skipped counts), the skipped runs' reasons counted, a result line and a rerun command`

---

### Task 4: The single-seed replay's human output

**Files:**
- Modify: `crates/charpente-sim/src/harness.rs` (`run_single_seed`, `write_human`'s `--seed` branch; tests), `crates/charpente-cli/tests/cli.rs` (any pin on single-seed output), `docs/GUIDE.md` (any quote of single-seed output), `CHANGELOG.md`

**Interfaces:**
- Consumes: Task 3's `Batch`, `CheckExtras.detail`, `Ended.detail`.

**Copy (binding).** No startup line (as today). Failure blocks first, unchanged. Then a blank line and per check `check {name} {dots} {status}` with status `ok` | `FAILED` | `skipped`; under a skipped check `    skipped: {reason}` and, when the run gave one, `    detail:  {detail}` (two spaces after `detail:`, so the values align). Escapes as in Task 3. Then a blank line and `sim result: {word}. {p} passed; {f} failed; {s} skipped, on seed {seed:#x}.` — `{word}` as Task 3 (`nothing tested` when `p == 0` and `f == 0`). No rerun line.

- [ ] **Step 1: Write the failing tests:**
  - `a_replayed_seed_that_gave_up_with_a_detail_prints_both` — a fixture calling `give_up_with("the store hit fault fallout", "seq 14")`, seed `0x1234`: output ends with
    ```text
    check gives_up_with_detail ... skipped
        skipped: the store hit fault fallout
        detail:  seq 14

    sim result: nothing tested. 0 passed; 0 failed; 1 skipped, on seed 0x1234.
    ```
  - `a_replayed_seed_that_gave_up_without_a_detail_prints_no_detail_line` — `always_gives_up`: a `skipped:` line and no `detail:` line.
  - `a_replayed_failing_seed_prints_its_block_and_failed` — the existing failing fixture: its failure block, `check … FAILED`, result word `FAILED`.
  - `a_detail_with_a_newline_stays_on_one_line` — detail `"a\nb"` prints `a\nb` escaped.
- [ ] **Step 2: Run** `cargo test -p charpente-sim --lib harness` — the new tests fail.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Verify** as Task 3 Step 6 (`oracle3.sh task4`).
- [ ] **Step 5: CHANGELOG** under 0.2.1 `### Changed`: `` Replaying one seed (`--seed`) prints each check as ok, FAILED or skipped, and a skipped run's reason with its detail. ``
- [ ] **Step 6: Commit:** `sim: a replayed seed says ok, FAILED or skipped per check, with a skipped run's reason and detail`

---

### Task 5: `--format json` gains the same facts

**Files:**
- Modify: `crates/charpente-sim/src/harness.rs` (`Report::to_json` → a shared `fn render_json(report: &Report, checks: Option<&[CheckExtras]>) -> String`; `run`'s JSON branch; tests), `docs/GUIDE.md` (the `--format json` field list, today at "`outcomes` (the same split per test: `{test, verdicts, gave_up}`)"), `CHANGELOG.md`

**Interfaces:**
- `Report::to_json(&self)` = `render_json(self, None)`: byte-identical to 0.2.0. `run`'s `--format json` = `render_json(&batch.report, Some(&batch.checks))`.
- Each `outcomes[]` entry gains, after `gave_up` and in this order: `"failed":<n>` and `"skipped_reasons":[{"reason":"…","count":<n>}, …]` — every reason (no cap), in the human order. Under `--seed`, an entry for a run with a detail is `{"reason":"…","count":1,"detail":"…"}`. No other key moves or changes.

- [ ] **Step 1: Write the failing tests:**
  - `report_to_json_is_unchanged_from_0_2_0` — a fixed report's `to_json()` equals the string the current code produces (capture it before editing; the existing JSON tests already pin most of it — extend one).
  - `the_harness_json_adds_failed_and_skipped_reasons_per_check` — a batch of `always_passes` + the twelve-reason fixture: each outcomes entry has the two keys in order, all twelve reasons, counts summing to `gave_up`.
  - `a_replayed_seed_json_carries_the_detail` — `give_up_with` under `--seed`: the entry has `"detail":"seq 14"`.
- [ ] **Step 2: Run** — new tests fail.
- [ ] **Step 3: Implement** with `json_escape` for every string.
- [ ] **Step 4: Verify** as Task 3 Step 6 (`oracle3.sh task5` — its filter already ignores the new keys; a diff means an old key changed).
- [ ] **Step 5: CHANGELOG** `### Added`: `` `--format json`: each `outcomes` entry adds `failed` and `skipped_reasons` (every reason with its count; under `--seed`, the run's detail). Existing fields are unchanged. ``
- [ ] **Step 6: Commit:** `sim: --format json carries each check's failed count and its skipped runs' reasons (additive; Report::to_json unchanged)`

---

### Task 6: The scaffold, the checker library, the fixtures and GUIDE use `give_up_with`

**Files:**
- Modify: `crates/charpente-cli/templates/sim.rs.tmpl`, `crates/charpente-sim/src/checks.rs`, `crates/sim-fixture/tests/sim.rs`, `examples/invoicing/tests/sim.rs`, `docs/GUIDE.md` (its `give_up` examples and the paragraph that introduces giving up), `crates/charpente-cli/tests/new.rs`, `CHANGELOG.md`

**Interfaces:**
- Consumes: `Sim::give_up_with(reason: &'static str, detail: impl Display)`.
- Rule: a call whose reason embeds per-run text (`format!("…: {e}")`, a count, a seq) becomes `give_up_with("<the fixed part>", <the variable part>)`; a call whose reason is already fixed stays `give_up`. Reason wording keeps its meaning (the fixed part of today's text, minus the trailing `: `).

- [ ] **Step 1: Write the failing test** in `crates/charpente-cli/tests/new.rs`: `the_scaffolds_green_run_groups_its_skipped_runs_by_fixed_reasons` — the scaffold's green batch at master seed `0x1` (reuse the file's existing scaffold/patch setup): in the stdout's skipped block, no reason line contains `fault fallout: ` (no per-run detail leaked into a grouped reason), and no check lists an `and … more reasons` line.
- [ ] **Step 2: Run** `cargo test -p charpente-cli --test new the_scaffolds_green_run` — FAIL (today's reasons embed `{e}`).
- [ ] **Step 3: Convert** the template's six detail-bearing calls (`sim.rs.tmpl` lines with `{e}` or `{msg}`), `checks.rs`'s two, and the fixtures' and invoicing's detail-bearing calls. In GUIDE, teach `give_up_with` where a reason carries an error, and say why (the CLI groups by the fixed reason; the detail shows on `--seed`). `cargo build -p guide-examples`.
- [ ] **Step 4: Verify.** `cargo test -p charpente-cli --no-fail-fast`, `cargo test -p charpente-sim --no-fail-fast`, `cargo test -p guide-examples --no-fail-fast`, `cargo test -p sim-fixture --no-fail-fast`, `cargo test -p invoicing --no-fail-fast`; all ok. **`oracle3.sh task6` exit 0** — reason text changed, decisions must not.
- [ ] **Step 5: CHANGELOG** `### Changed`: `` The scaffold, `charpente_sim::checks` and GUIDE's examples give up with `give_up_with`, so their skipped runs group by reason. ``
- [ ] **Step 6: Commit:** `sim: the scaffold, the checker library, the fixtures and GUIDE give up with a fixed reason and a per-run detail, so a batch's skipped runs group`

---

### Task 7: `charpente --version`, and README's "Try it"

**Files:**
- Modify: `crates/charpente-cli/src/main.rs`, `crates/charpente-cli/tests/cli.rs`, `README.md`, `CHANGELOG.md`

**Copy (binding).**
- `charpente --version` and `charpente -V`, with no further argument: stdout `charpente {CARGO_PKG_VERSION}\n`, exit 0. Anything else unmatched: usage on stderr, exit 2, as today; the usage text gains a first line `usage: charpente --version`.
- README "Try it" block:
  ```text
  cargo install charpente-cli
  charpente new myapp && cd myapp
  cargo clippy --all-targets -- -D warnings   # hard rule 1, in your app (no clippy? rustup component add clippy)
  charpente test --sim                        # seeded fault-injection runs
  ```
  and the paragraph after it: ``Then delete the `tx.enqueue` line in `src/lib.rs` and run the sim again: the check `acked_post_implies_the_echo_after_recovery` goes red (`check acked_post_implies_the_echo_after_recovery ... FAILED`) with a seed, and the seed replays. That is the whole pitch.``

- [ ] **Step 1: Write the failing tests** in `cli.rs`: `version_prints_the_name_and_version_and_exits_zero` (both `--version` and `-V`: stdout exactly `format!("charpente {}\n", env!("CARGO_PKG_VERSION"))`, empty stderr, exit 0) and `an_unknown_argument_still_prints_usage_and_exits_two` (stderr starts `usage: charpente --version`, exit 2).
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement**; update the module doc's verb list.
- [ ] **Step 4: README** per the copy. Confirm the red line's text matches Task 3's FAILED line for the scaffold (the dots depend on the two check names: `acked_post_implies_the_echo_after_recovery` is the longer, so it gets exactly ` ... `).
- [ ] **Step 5: Verify.** `cargo test -p charpente-cli --no-fail-fast`, `cargo test -p guide-examples --no-fail-fast` (claims_census reads README); all ok.
- [ ] **Step 6: CHANGELOG** `### Added`: `` `charpente --version` (and `-V`). `` and `### Changed`: `README's "Try it" says how to add clippy when it is missing, and names the check that goes red.`
- [ ] **Step 7: Commit:** `cli: charpente --version and -V; README's Try it adds the missing-clippy fix and names the check that goes red`

---

### Task 8: Evidence — semver, oracle 3, the gate, the ledger (controller)

**Files:**
- Modify: `docs/LEDGER.md`

- [ ] **Step 1: Semver.** `cargo install cargo-semver-checks --version 0.51.0 --locked` (if it cannot read rustc 1.99's rustdoc JSON, use the oldest release that can, and record which). Then `cargo semver-checks check-release --baseline-version 0.2.0 -p charpente-core -p charpente -p charpente-sim -p charpente-tsgen` (and `-p charpente-macros` if the tool supports a proc-macro crate; record if it skips it). Expected: no breaking change reported. A reported break is a finding to fix in the task that owns the code, not to argue.
- [ ] **Step 2: Oracle 3** at the final head: `$SDD/oracle3.sh final` exit 0, six diffs empty.
- [ ] **Step 3: The full gate** at the final head in `~/Documents/GitHub/charpente-gate` (the outbound workspace's `gate.sh` shape): fmt 0, clippy 0, `cargo test --workspace --no-fail-fast` exit 0; count `^test result: ok` against the base (92), FAILED 0, `error: test failed` 0. Any count change is explained by name.
- [ ] **Step 4: The ledger entry**, dated, numbers copied from `$SDD` files: what landed (commits by sha), oracle 3, semver, the gate, and the open items (Tasks 9–10). Commit: `ledger: 0.2.1 readable sim output — oracle 3 unchanged over six batches, semver-compatible with 0.2.0, gate N/N`.
- [ ] **Step 5: Push on Noah's word**, then count CI's workspace step as the 0.2.0 close-out did; record the run id.

---

### Task 9: A stranger reads it — the rehearsal re-run (controller)

The launch run's procedure (`docs/superpowers/plans/2026-10-06-launch-run.md` Tasks 4–7, tools in `docs/experiments/2026-10-launch-run/tools/`, used in place) with these differences:

- **Record:** `REC=docs/experiments/2026-10-readable-sim-output`. `BRIEF.md` copied byte-identical from the launch run's (sha256 `dae25ebbfa812bb93f9d650d254e77863ab651005b82c35ebf257aafdc674dd9`; verify before every attempt).
- **Image:** re-pin `rust:latest` (launch-run Task 4 Step 1) and record the digest; a missing clippy is again a fact the stranger meets.
- **Registry:** `make-registry.sh` at the post-Task-8 head (packaged 0.2.1; no network in the box).
- **Preflight** (launch-run Task 4, by hand): bar (a) is now an exit 0 whose stdout has `sim result: ok.` (the string `no failures` no longer prints); (b) unchanged (`sim failure in acked_post_implies_the_echo_after_recovery`, exit 1); (c)/(d) unchanged (the failure block did not change).
- **Dispatch:** the launch-run prompt verbatim, plus one line appended to its "About this machine" paragraph: `Make no network requests from outside the box.` Agent tool, `model: opus`, **no `name`**.
- **The follow-up:** after the stranger's final report, one SendMessage with exactly `In two or three sentences, what did the last sim run you did tell you?`, its answer recorded verbatim in `$A/followup.md`.
- **Pass bar** (spec §6.5), across the report and the follow-up: (a) per check, whether it held and how many runs tested it, correct; (b) at least the most common skip reason, correct; (c) nothing calls the output overstated, misleading or unclear; (d) every claim about charpente right, censused as the launch run's were (right / wrong / right for the wrong reason, `file:line` each). Plus the launch run's (a)–(c) mechanical bar and sealing (e).
- **Verdict to Noah: STOP** for his sign-off. A FAIL: name the output defect, fix it as its own pinned change, and run attempt N+1 (cap three, then Noah).
- [ ] **Step 1:** Preflight. **Step 2:** `BRIEF.md` + record skeleton, commit. **Step 3:** Attempt 1 (launch-run Task 6). **Step 4:** Adjudicate (launch-run Task 7 with the bar above). **Step 5:** Verdict to Noah. **Step 6:** Commit the attempt's record and a ledger entry.

---

### Task 10: Publish and the registry smoke

- [ ] **Step 1:** `scripts/publish.sh` (dry run, full verify) exit 0 at the signed head — controller.
- [ ] **Step 2: Noah publishes:** `scripts/publish.sh --for-real`.
- [ ] **Step 3:** For each of the six crates, the crates.io sparse-index `cksum` for 0.2.1 equals `shasum -a 256 target/package/tmp-crate/<crate>-0.2.1.crate` (no contact header).
- [ ] **Step 4: Registry smoke** in the session scratchpad with its own `CARGO_TARGET_DIR` and `CARGO_INSTALL_ROOT`: `cargo install charpente-cli --version 0.2.1 --locked`, `charpente --version` prints `charpente 0.2.1`, `charpente new myapp` (requirement `"0.2.1"`), clippy, `charpente test --sim` exit 0 with `sim result: ok.`; read the output as the stranger would.
- [ ] **Step 5:** `CHANGELOG.md`'s `## 0.2.1 (unreleased)` becomes `## 0.2.1 (<publish date>)`, with a ledger line for the publish, CI and the nightly edge-harness run ids; one commit, pushed on Noah's word.
