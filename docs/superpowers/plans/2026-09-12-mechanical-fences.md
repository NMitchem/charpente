# Mechanical Fences Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn six invariants that currently hold because a reviewer checked them into invariants a machine refuses — a deny-list entry for the panic hook, a truthful doc on the hook's `Once`, one dead env marker removed, a doc-truth gate over GUIDE's quoted error text, a `--jobs`/`CARGO_TARGET_DIR` refusal in the mutation verb, and one indentation ride-along — with no runtime behaviour changed.

**Architecture:** Four tasks, each independently reviewable, executed sequentially on one branch in one worktree. Every item ships RED-first: its failing case is *observed*, not argued, before the fix is considered done. The output is oracles — `clippy.toml`, doc comments, one new test binary, one refusal in edge tooling, and docs. The one item with real design content (the doc-truth gate, Task 3) is designed here so its three decoration failure modes (spec §4) are closed by construction rather than by care.

**Tech Stack:** Rust workspace (`charpente-core`, `charpente-sim`, `charpente-cli`, `guide-examples`, `edge-harness`), clippy `disallowed_methods`, `cargo-mutants` 27.1.0, zsh on macOS.

**Spec:** `docs/superpowers/specs/2026-09-10-mechanical-fences-design.md` (committed `ec08c36`; binding authority — read it first, §3 and §4 in particular).

## Global Constraints

Every task's requirements implicitly include this section.

- **No runtime behaviour changes** (spec §3). The diff touches `clippy.toml`, doc comments, docs, test files, `scripts/mutants.sh`, and ONE edge-tooling refusal in `charpente-cli` that fires before anything is spawned. If a task seems to need more, stop and record it in `decisions-pending.md` (below) rather than doing it.
- **RED first, observed** (spec §4, last paragraph). Each item ships only after its failing case has been *observed*. A test that merely fails to compile against the old code proves nothing (the 2026-09-08 ledger entry) — the RED must be a runtime refusal, an assertion, or a clippy error naming the site.
- **No expansion of the deny-list beyond Item 1** (spec §3). Two entries: `std::panic::set_hook` and `std::panic::take_hook`. Nothing else.
- **No fixing the deferred Minors** the autonomous run's final review said can stand (spec §3): `table.rs`'s own "panicked at open" text, `scan_by`'s silent `Ok([])`, `:981`'s event table.
- **Verification is foregrounded.** Never background `cargo test`. An agent that backgrounds it can park forever after the results arrive.
- **`--no-fail-fast` is load-bearing.** Confirm green by COUNTING: `2>&1 | tee LOG`, then `grep -c "^test result: ok" LOG`. Never conclude green from the absence of `FAILED`.
- **State the expected count BEFORE running.** The whole-workspace count on `main` at the plan commit is **82** `test result: ok` lines (measured on `62e3c0c`; the one commit since added no test binary). Task 3 adds ONE test binary (`guide-examples/tests/doc_truth.rs`), so after Task 3 the prediction is **83**. Task 2 adds tests to an EXISTING binary (`charpente-cli/tests/cli.rs`) and changes no count.
- **This shell is zsh.** `${PIPESTATUS[0]}` expands to the empty string; use `${pipestatus[1]}` (lowercase, 1-indexed) or `set -o pipefail` and test `$?`. zsh does not word-split an unquoted `$var`. zsh globs, so quote `--include="*.rs"`.
- **Pipe cargo through `tee`, never `tail`.**
- **Worktree and target dir.** All work happens in `/Users/noahmitchem/Documents/GitHub/charpente-fences` on branch `mechanical-fences` (base: the plan commit on `main` — `git merge-base main mechanical-fences`), with `export CARGO_TARGET_DIR=/tmp/charpente-fences-target` in every shell. Never share a `CARGO_TARGET_DIR` across worktrees. Task 2 is *about* this hazard: its smoke test deliberately sets the variable and passes `--jobs 2`, and must see the verb refuse.
- **`timeout`/`gtimeout` are both absent on this machine.** A bare `timeout` exits 127 and that reads as a result.
- **Grepping for `panicked` is not a failure detector here.** Key on exit codes and `test result: FAILED`.
- **Commit named paths only**, never `git add -A`. Every commit ends with the two attribution lines the last commits on `main` carry (`Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_017gCrhXqM7QqtVbwYF1266M`).
- **Any claim about a commit is checked with `git show <sha>:<path>`**, never the working tree.
- **Dispatches are unnamed**, and every agent writes its report to a FILE under `.superpowers/sdd/2026-09-12-mechanical-fences/` before returning.
- **Blocked work** goes to `.superpowers/sdd/2026-09-12-mechanical-fences/decisions-pending.md` with the reading taken, the reasoning, and what would falsify it. Anything contradicting `DECISIONS.md` stops that task dead.
- **Scratch edits used to force a RED are removed before the commit**, and the removal is verified with `git diff --stat` showing only the intended files.

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `clippy.toml` | The deny-list gains `std::panic::set_hook` / `take_hook` | 1 |
| `crates/charpente-sim/src/harness.rs` | `install_open_fence_hook`: the ONE sanctioned installer, now with a site-local allow and a truthful doc | 1 |
| `crates/edge-harness/tests/open_fence.rs` | Drops the dead `CHARPENTE_F1_CHILD` marker | 1 |
| `crates/charpente-cli/src/mutants.rs` | The `--jobs > 1` + external `CARGO_TARGET_DIR` refusal, pure-function-shaped and unit-tested | 2 |
| `crates/charpente-cli/tests/cli.rs` | The refusal observed through the real binary, RED-first | 2 |
| `scripts/mutants.sh` | Execs the binary from wherever `CARGO_TARGET_DIR` put it | 2 |
| `crates/guide-examples/tests/doc_truth.rs` | NEW — the doc-truth gate: pinned GUIDE text blocks vs the normalised source corpus, with a floor | 3 |
| `docs/GUIDE.md` | `<!-- pinned -->` markers on the blocks that pin as-is; the convention stated in the preamble | 3 |
| `CLAUDE.md` | `guide-examples` test list gains `doc_truth`; the GUIDE-editing bullet names the marker | 3 |
| `DESIGN.md` | §9 item 5's six continuation lines indented like their neighbours | 4 |

---

### Task 1: The panic-hook install-site invariant becomes mechanical (Items 1, 2, 3)

The F1 fix's safety argument is that the process-global panic hook is installed in exactly one place, by the simulator, and never inside the deterministic boundary. Today that is a paragraph in a doc comment (`table.rs:803-804`) and nothing refuses a second installer. This task makes clippy refuse it, corrects the doc that calls a `std::sync::Once` "idempotent", and removes an env marker nothing reads.

**Files:**
- Modify: `clippy.toml` (the `disallowed-methods` list, after the `std::process::Command::spawn` entry)
- Modify: `crates/charpente-sim/src/harness.rs:209-243` (`OPEN_FENCE_HOOK` and `install_open_fence_hook`)
- Modify: `crates/edge-harness/tests/open_fence.rs:50` (the `.env("CHARPENTE_F1_CHILD", "1")` line)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: nothing other tasks call. The deny-list entries are workspace-wide and Task 3's new test file must not trip them (it will not — it installs no hook).

- [ ] **Step 1: Confirm the census before touching anything**

Run from the worktree:

```bash
grep -rn 'set_hook\|take_hook' --include="*.rs" crates/ examples/
```

Expected: exactly two CALL sites, both in `crates/charpente-sim/src/harness.rs` (`take_hook` at :229, `set_hook` at :230), plus two doc-comment mentions in `crates/charpente-core/src/table.rs:803-804`. If a third call site exists, stop — the spec's premise is wrong and that goes to `decisions-pending.md`.

- [ ] **Step 2: Add the two deny-list entries**

In `clippy.toml`, directly after the `std::process::Command::spawn` line inside `disallowed-methods`, add:

```toml
    # The process-global panic hook is installed by exactly ONE site, the simulator's
    # `install_open_fence_hook` (charpente-sim/src/harness.rs), which filters redb's
    # benign open-fence trace (F1, 2026-09-09). A second installer anywhere — and above
    # all one inside the deterministic boundary — replaces that filter silently, and
    # can mute a genuine panic on a parallel sim worker. This was a review-enforced
    # invariant in a repo whose hard rule 1 forbids that arrangement.
    { path = "std::panic::set_hook", reason = "the process-global panic hook has one sanctioned installer, charpente-sim's install_open_fence_hook — a second one silently replaces the redb open-fence filter (F1) and can mute a real panic on another sim worker; route new hook needs through that function" },
    { path = "std::panic::take_hook", reason = "pairs with set_hook — taking the hook outside charpente-sim's install_open_fence_hook is the first half of installing a second one" },
```

- [ ] **Step 3: RED #1 — the entry fires on the sanctioned site itself**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente-fences && export CARGO_TARGET_DIR=/tmp/charpente-fences-target
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tee /tmp/fences-t1-red1.log; echo "exit ${pipestatus[1]}"
```

Expected: exit **non-zero**, and the log contains `use of a disallowed method \`std::panic::set_hook\`` pointing at `crates/charpente-sim/src/harness.rs:230` and the same for `take_hook` at `:229`, each followed by the `reason` text above. This proves the entry is live and the site is the one the census found. Paste the two `--> crates/charpente-sim/src/harness.rs:NNN` lines into the task report.

- [ ] **Step 4: Sanction the one site, and make its doc true (Item 2)**

Replace the block from `static OPEN_FENCE_HOOK` through the end of `install_open_fence_hook` in `crates/charpente-sim/src/harness.rs` with:

```rust
static OPEN_FENCE_HOOK: std::sync::Once = std::sync::Once::new();

/// Replaces redb's benign open-fence trace with one named line, and delegates
/// every other panic to the previous hook untouched. Deliberately contains no
/// `panicked` substring: a log grep for that word is what F1 trained agents
/// to misuse.
///
/// **Install-once-EVER, not idempotent.** The `Once` fires the first time any
/// entry point calls this and never again — so calling it from several entry
/// points is safe, which is what the old "idempotent" wording meant. What it
/// does NOT give is restoration: if anything installs a hook *after* this one,
/// the filter is gone and a second call here restores nothing, because the
/// `Once` has already fired. That is why `std::panic::set_hook` and
/// `take_hook` are on `clippy.toml`'s deny-list with this function as the one
/// sanctioned site (mechanical-fences Item 1): the invariant "exactly one
/// installer, in the simulator" is what makes the filter trustworthy, and a
/// deny-list is the only thing that makes the invariant hold without a
/// reviewer.
///
/// Muting is gated on BOTH the fence flag AND the panic's own source location
/// naming redb — the flag alone is a time window, not a guarantee about what
/// panicked inside it (review finding, F1 round 2). `DiskBackend` (table.rs)
/// calls straight through to whatever `Disk`/`File` the app provided, and a
/// real backend can panic for its own reasons while `Store::open`'s
/// `catch_unwind` is active — `SimFile`'s own `panic!`/`expect` sites in this
/// crate (lib.rs, `file_state` and `read_at`) are exactly that. Muting on the
/// flag alone would silence a genuine simulator bug the same way F1's
/// original defect silenced a benign one, one notch narrower and strictly
/// worse: `Db::open`'s discard-and-rebuild path (db.rs) swallows the
/// resulting `io::Error` unprinted either way.
// The ONE sanctioned installer of the process-global panic hook (clippy.toml's
// `std::panic::set_hook`/`take_hook` entries name this function). The allow is
// function-scoped because the function IS the site; nothing else in the
// workspace may carry this allow for these two paths.
#[allow(clippy::disallowed_methods)]
pub fn install_open_fence_hook() {
    OPEN_FENCE_HOOK.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let from_redb = info
                .location()
                .is_some_and(|loc| loc.file().contains("redb"));
            if from_redb && charpente_core::table::in_open_fence() {
                eprintln!(
                    "charpente: redb open-fence engaged (benign) — the store is treated \
                     as unopenable and Db::open rebuilds it; see Store::open"
                );
                return;
            }
            prev(info);
        }));
    });
}
```

The closure body is byte-identical to today's; only the doc, the rationale comment and the `#[allow]` change. Confirm that with `git diff crates/charpente-sim/src/harness.rs` — the `-`/`+` lines must not include any line inside `call_once`.

- [ ] **Step 5: GREEN — the sanctioned site passes**

```bash
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tee /tmp/fences-t1-green1.log; echo "exit ${pipestatus[1]}"
```

Expected: exit 0.

- [ ] **Step 6: RED #2 — a second installer is refused (the spec's required observation)**

Append this scratch function to the END of `crates/charpente-core/src/lib.rs`:

```rust
// SCRATCH — mechanical-fences Task 1 Step 6; must not be committed.
pub fn scratch_second_installer() {
    std::panic::set_hook(Box::new(|_| {}));
}
```

```bash
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tee /tmp/fences-t1-red2.log; echo "exit ${pipestatus[1]}"
```

Expected: exit non-zero; the log names `crates/charpente-core/src/lib.rs` and the `set_hook` reason. Paste the `-->` line into the task report — this is the observation spec §5 criterion 1 demands ("a second installer has been observed being refused").

Then remove the scratch function and confirm:

```bash
# The scratch block was APPENDED, so cut from its comment line to end of file.
perl -0pi -e 's/\n\/\/ SCRATCH — mechanical-fences Task 1 Step 6.*\z//s' crates/charpente-core/src/lib.rs
git diff --stat   # must list ONLY clippy.toml and crates/charpente-sim/src/harness.rs
grep -c scratch_second_installer crates/charpente-core/src/lib.rs   # must print 0
```

- [ ] **Step 7: Item 3 — remove the dead env marker, and say why**

In `crates/edge-harness/tests/open_fence.rs`, delete the line `.env("CHARPENTE_F1_CHILD", "1")` (line 50). Then, in the same file's module doc (the `//!` block at the top), append after the last `//!` line:

```rust
//!
//! The child is NOT marked with an env var. An earlier draft set
//! `CHARPENTE_F1_CHILD=1` on the spawned `cargo test` "as a marker the
//! child's output can be attributed to", and nothing ever read it — dead
//! scaffolding that looked load-bearing (mechanical-fences Item 3). The child
//! runs a test in a DIFFERENT crate (`-p charpente`), so it cannot re-enter
//! this test and needs no recursion guard either. Do not confuse this with
//! `CHARPENTE_F1_PANIC_CHILD`, which `crates/charpente-sim/tests/open_fence.rs`
//! DOES read: that one gates its tests to run only as a spawned child, and
//! removing it would make those tests fire inside the ordinary sweep.
```

Confirm the sibling variable is untouched and the dead one is gone:

```bash
grep -rn 'CHARPENTE_F1_CHILD' crates/ examples/ scripts/        # must print nothing
grep -rn 'CHARPENTE_F1_PANIC_CHILD' crates/charpente-sim/tests/open_fence.rs | wc -l   # must be >= 2
```

- [ ] **Step 8: Verify the touched crates**

Predict BEFORE running: `charpente-sim` **7** binaries ok, `edge-harness` **9** ok (its `open_fence` binary's tests spawn a child `cargo test` and take ~1-2 min).

```bash
cargo fmt --all && cargo fmt --all --check; echo "fmt $?"
cargo test -p charpente-sim --no-fail-fast 2>&1 | tee /tmp/fences-t1-sim.log; echo "exit ${pipestatus[1]}"; grep -c "^test result: ok" /tmp/fences-t1-sim.log
cargo test -p edge-harness --no-fail-fast 2>&1 | tee /tmp/fences-t1-edge.log; echo "exit ${pipestatus[1]}"; grep -c "^test result: ok" /tmp/fences-t1-edge.log
```

Expected: fmt 0; both exit 0; counts 7 and 9; `grep -c "FAILED" ` on both logs prints 0.

- [ ] **Step 9: Commit**

```bash
git add clippy.toml crates/charpente-sim/src/harness.rs crates/edge-harness/tests/open_fence.rs
git commit -F - <<'EOF'
fence(sim): the panic hook has one installer, and clippy now says so

std::panic::set_hook and take_hook join clippy.toml's deny-list; the one
sanctioned site is install_open_fence_hook, function-scoped allow with
the rationale at the site. Observed refusing: first the sanctioned site
itself before the allow (proving the entry is live), then a scratch
second installer in charpente-core (proving the fence reaches the
deterministic boundary). Both removed; the closure body is byte-identical.

The function's doc no longer calls a std::sync::Once "idempotent": it is
install-once-EVER, and a later hook silently ends the filter with nothing
to restore it — which is exactly why the deny-list exists.

CHARPENTE_F1_CHILD is removed from edge-harness's open_fence oracle: set
by one .env(), read by nothing. CHARPENTE_F1_PANIC_CHILD is a different
variable and stays; charpente-sim's child tests gate on it.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017gCrhXqM7QqtVbwYF1266M
EOF
```

---

### Task 2: `charpente test --mutants` refuses the combination that lies (Item 5)

With an externally-set `CARGO_TARGET_DIR` and `--jobs N > 1`, every cargo-mutants scratch tree builds into ONE directory; cargo's artifact hash does not vary by mutation content, so a mutant's tests can run against another mutant's build. Observed 2026-09-10 in `.superpowers/sdd/2026-09-09-autonomous-run/mutation-findings.md` ("METHODOLOGY FINDING"): one rlib hash in 21 of 36 mutants' logs including the baseline's, and `log.rs:68:39` flipping MISSED→CAUGHT with only `--jobs` changed — the false-MISSED case the spec requires observed *before* the fix. That observation stands as the RED for the mechanism; this task's own REDs are the refusal's absence, observed through the binary.

**Files:**
- Modify: `crates/charpente-cli/src/mutants.rs` (`run`, just before the `Command::new("cargo")` spawn; a new pure function; unit tests)
- Modify: `crates/charpente-cli/tests/cli.rs` (one new integration test)
- Modify: `scripts/mutants.sh` (the `exec` line)

**Interfaces:**
- Produces: `fn shared_target_dir_refusal(jobs: Option<&str>, target_dir: Option<&std::ffi::OsStr>) -> Option<String>` in `mutants.rs` — `Some(message)` when the combination must be refused. Private to the module; the integration test drives it through the binary.

- [ ] **Step 1: Read how `cli.rs` drives the binary, and pick the pattern**

```bash
grep -n 'CARGO_BIN_EXE_charpente' crates/charpente-cli/tests/cli.rs | head -3
sed -n '1,40p' crates/charpente-cli/tests/cli.rs
```

Use whatever helper the file already has for spawning `env!("CARGO_BIN_EXE_charpente")` with args and reading `status`/`stderr`. The test below writes `std::process::Command` directly if no helper exists; the file already carries the deny-list allow for that (check the `#![allow(...)]` at its top — if absent, the test's `Command::new` will fail clippy, and the fix is the same file-level allow `crates/charpente-cli/tests/ctl.rs` uses).

- [ ] **Step 2: Write the integration test (RED-first through the binary)**

Append to `crates/charpente-cli/tests/cli.rs`:

```rust
/// `--jobs > 1` with an inherited `CARGO_TARGET_DIR` makes every cargo-mutants
/// scratch tree build into ONE directory, and cargo's artifact hash does not
/// vary by mutation content — so a mutant's tests can judge another mutant's
/// build. Observed 2026-09-10 (one rlib hash in 21 of 36 mutants' logs; a
/// MISSED that flipped to CAUGHT with only `--jobs` changed). The verb must
/// refuse BEFORE spawning anything, exit 2, and name the contract, the site
/// and both fixes (hard rule 5).
#[test]
fn mutants_refuses_parallel_jobs_under_a_shared_target_dir() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_charpente"))
        .args(["test", "--mutants", "--jobs", "2", "--re", "no-such-mutant-anywhere"])
        .env("CARGO_TARGET_DIR", "/tmp/charpente-shared-target-dir-for-this-test")
        .output()
        .expect("charpente binary runs");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(2),
        "the refusal is a usage error (exit 2); stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("--jobs 2 with CARGO_TARGET_DIR set")
            && stderr.contains("run with --jobs 1, or unset CARGO_TARGET_DIR"),
        "the refusal must name the combination and both fixes; got:\n{stderr}"
    );
    // The negative space — `--jobs 1`, no `--jobs`, no variable, an
    // unparseable `--jobs` — is pinned on the pure function in mutants.rs's
    // unit tests. Spawning the non-refused shape here would run cargo-mutants
    // for real against this crate's cwd and leave a stray
    // `crates/charpente-cli/target/` behind (`.gitignore` is `/target`,
    // root-only).
}
```

- [ ] **Step 3: RED — run it against the unchanged verb**

```bash
cargo test -p charpente-cli --test cli mutants_refuses_parallel_jobs_under_a_shared_target_dir 2>&1 | tee /tmp/fences-t2-red.log; echo "exit ${pipestatus[1]}"
```

Expected: exit non-zero, `test result: FAILED`, and the first assertion's message — the old verb spawns `cargo mutants` (which with a no-match `--re` reports zero mutants) and never prints the refusal text. Note in the report what exit code the OLD verb produced and the first ~3 lines of its stderr: that is the "no error, no warning" shape the spec names. If `cargo-mutants` is not installed in the worktree's environment the old verb exits 2 with "cargo-mutants is not installed" — the SECOND assertion (message text) still fails, which is the RED that matters; say which of the two you observed.

The old verb's spawn runs in the test's cwd (`crates/charpente-cli`) and may leave a stray `crates/charpente-cli/target/charpente-mutants/` behind — `.gitignore` is `/target`, root-only, so it shows as untracked. Remove it before going on:

```bash
rm -rf crates/charpente-cli/target
git status --short   # must show only crates/charpente-cli/tests/cli.rs as modified
```

- [ ] **Step 4: Implement the refusal as a pure function, and call it before the spawn**

In `crates/charpente-cli/src/mutants.rs`, add above `pub fn run`:

```rust
/// `Some(refusal)` when `--jobs > 1` is combined with an inherited
/// `CARGO_TARGET_DIR`. cargo-mutants isolates parallel jobs by building each
/// scratch tree into its own `target/`; an inherited `CARGO_TARGET_DIR`
/// points every job at ONE directory, and cargo's artifact filename hash does
/// not vary by mutation content — so a mutant's tests can run against a
/// different mutant's (or the baseline's) build and report a MISSED or a
/// CAUGHT that measured nothing. Observed 2026-09-10: one rlib hash shared by
/// 21 of 36 mutants' logs including the baseline's, and a MISSED that flipped
/// to CAUGHT with only `--jobs` changed. The combination is reachable by
/// following this repo's own advice (a dedicated `CARGO_TARGET_DIR` per
/// worktree), so the verb refuses it by name rather than trusting the operator
/// to know. Pure so the message is unit-testable; `run` supplies the env.
fn shared_target_dir_refusal(
    jobs: Option<&str>,
    target_dir: Option<&std::ffi::OsStr>,
) -> Option<String> {
    let n: u64 = jobs?.parse().ok()?;
    if n <= 1 {
        return None;
    }
    let dir = target_dir?.to_string_lossy().into_owned();
    Some(format!(
        "error: --jobs {n} with CARGO_TARGET_DIR set ({dir}) — cargo-mutants builds each \
         job's scratch tree into a target dir of its own, and an inherited CARGO_TARGET_DIR \
         points every job at ONE directory; cargo's artifact hash does not vary by mutation \
         content, so a mutant's tests can run against a different mutant's build and report \
         a MISSED (or a CAUGHT) that measured nothing (observed 2026-09-10: one rlib hash \
         shared by 21 of 36 mutants' logs). Fix: run with --jobs 1, or unset CARGO_TARGET_DIR \
         for this invocation."
    ))
}
```

Then in `run`, immediately after the `let opts = match parse(args) { ... };` block and BEFORE `let out_dir = ...`, insert:

```rust
    if let Some(refusal) =
        shared_target_dir_refusal(opts.jobs.as_deref(), std::env::var_os("CARGO_TARGET_DIR").as_deref())
    {
        eprintln!("{refusal}");
        return 2;
    }
```

- [ ] **Step 5: Unit-test the pure function (the message is product surface)**

Inside the existing `#[cfg(test)] mod tests` in `mutants.rs`, add:

```rust
    #[test]
    fn parallel_jobs_under_a_shared_target_dir_are_refused_by_name() {
        let dir = std::ffi::OsStr::new("/tmp/shared");
        let msg = shared_target_dir_refusal(Some("4"), Some(dir)).expect("refused");
        assert!(msg.starts_with("error: --jobs 4 with CARGO_TARGET_DIR set (/tmp/shared)"));
        assert!(msg.contains("observed 2026-09-10"));
        assert!(msg.ends_with("run with --jobs 1, or unset CARGO_TARGET_DIR for this invocation."));
    }

    #[test]
    fn the_refusal_needs_both_halves() {
        let dir = std::ffi::OsStr::new("/tmp/shared");
        assert!(shared_target_dir_refusal(Some("1"), Some(dir)).is_none(), "one job cannot collide");
        assert!(shared_target_dir_refusal(None, Some(dir)).is_none(), "no --jobs is the tool's default of 1");
        assert!(shared_target_dir_refusal(Some("4"), None).is_none(), "no shared dir, no collision");
        assert!(shared_target_dir_refusal(Some("many"), Some(dir)).is_none(), "an unparseable --jobs is cargo-mutants' to refuse, not ours");
    }
```

- [ ] **Step 6: GREEN**

```bash
cargo test -p charpente-cli --no-fail-fast 2>&1 | tee /tmp/fences-t2-green.log; echo "exit ${pipestatus[1]}"; grep -c "^test result: ok" /tmp/fences-t2-green.log
```

Predict BEFORE running: `charpente-cli` has 6 integration binaries plus the lib's unit tests and a doctest binary — read the count from Task 1's baseline if unsure by running `cargo test -p charpente-cli 2>&1 | grep -c "^test result: ok"` on `main` in the MAIN tree first (it is 8 at `62e3c0c`: lib unit tests, 6 `tests/*.rs`, doctests). Expected: exit 0, count unchanged from baseline, `grep -c FAILED` prints 0.

- [ ] **Step 7: `scripts/mutants.sh` finds the binary wherever cargo put it**

Replace the last line of `scripts/mutants.sh` (`exec ./target/debug/charpente test --mutants "$@"`) with:

```bash
# An exported CARGO_TARGET_DIR moves the build; exec from wherever cargo put it
# rather than from ./target, which under that variable holds nothing.
exec "${CARGO_TARGET_DIR:-./target}/debug/charpente" test --mutants "$@"
```

Verify both changes together — the script finds the binary AND the verb refuses:

```bash
CARGO_TARGET_DIR=/tmp/charpente-fences-target scripts/mutants.sh --jobs 2 --re no-such-mutant; echo "exit $?"
```

Expected: exit 2 and the refusal text on stderr, within seconds (nothing spawned). Then the non-refused shape:

```bash
CARGO_TARGET_DIR=/tmp/charpente-fences-target scripts/mutants.sh --jobs 1 --re no-such-mutant; echo "exit $?"
```

Expected: exit 2 for a DIFFERENT reason — the verb's own "zero mutants is not a clean run" refusal (or "cargo-mutants is not installed" if absent) — and NOT the shared-target-dir text. Paste both stderrs into the report.

- [ ] **Step 8: fmt + clippy, then commit**

```bash
cargo fmt --all && cargo fmt --all --check; echo "fmt $?"
cargo clippy -p charpente-cli --all-targets -- -D warnings 2>&1 | tail -2; echo "clippy ${pipestatus[1]}"
git add crates/charpente-cli/src/mutants.rs crates/charpente-cli/tests/cli.rs scripts/mutants.sh
git commit -F - <<'EOF'
fence(cli): --mutants refuses --jobs > 1 under an inherited CARGO_TARGET_DIR

The 2026-09-10 sweep found its own instrument lying: with the target dir
exported for worktree hygiene and --jobs 4, one rlib hash appeared in 21
of 36 mutants' build logs including the baseline's, and a MISSED at
log.rs:68:39 flipped to CAUGHT with only --jobs changed. The combination
is reachable by following this repo's own advice, and nothing said so.

The verb now refuses it before spawning anything — exit 2, naming the
mechanism, the observation and both fixes — and the check is a pure
function so its text is pinned. RED observed through the binary: the old
verb printed no refusal. scripts/mutants.sh execs the binary from
${CARGO_TARGET_DIR:-./target}, which under that variable it never did.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017gCrhXqM7QqtVbwYF1266M
EOF
```

---

### Task 3: The doc-truth gate (Item 4)

GUIDE quotes error text in ```` ```text ```` blocks that `guide-examples`' build.rs does not compile, and "docs the code disproves" is this repo's self-named recurring defect. This task adds a gate that proves the code can still emit what a marked block quotes.

**The design, and why each part is there.** A one-time census (2026-09-12, over all 41 text blocks) found that a blanket rule fails: the blocks mix byte-exact error contracts with CLI usage, SSE wire frames, rustc output, sim reports with instance numbers, and an S3 lifecycle table. A gate that guesses which is which is the decoration spec §4 forbids. So:

1. **Explicit opt-in.** A block is pinned only when the line immediately before its ```` ```text ```` fence is exactly `<!-- pinned -->`. HTML comments render as nothing. A marker not followed by a text fence is a hard error — a marker that pins nothing is decoration.
2. **Fragment matching, not line matching.** The census showed lines fail for four reasons: source-side escapes (`\"`), source-side `\`-newline continuations, doc-side `<placeholder>` tokens, and instance values (`/notes/:key`, `20000000`). The first two are Rust's own literal rules and are undone by normalising the corpus; the third is undone by splitting each pinned line on placeholders; the fourth is out of scope (those blocks stay unmarked). Each fragment left after the split, trimmed, of at least `FRAGMENT_MIN = 16` characters must appear as a substring of the normalised corpus. A pinned line that yields NO fragment that long is an error — nothing on it is evidence.
3. **A placeholder is `<` + lowercase-initial run + `>`.** GUIDE's placeholders are `<detail>`, `<e>`, `<token>`, `<app>`, `<id>.<seq>`, `<the underlying decode error>`; generics in quoted code are `Id<Ping>`, `<T>`, `<Widget>`. Lowercase-initial is the rule that separates them, so `Id<Ping>` stays literal text.
4. **The corpus** is every `.rs` file under `crates/` and `examples/`, excluding any `target/` directory — source AND tests, because a rendered instance ("got 0") is pinned by a test's literal where the source has `{}`.
5. **A floor.** `PINNED_FLOOR` is the count of pinned blocks at landing, hand-verified by the census. Fewer is red. This closes §4's failure modes 1 (a pattern that matches nothing) and 2 (a corpus that silently shrinks).
6. **The failure message says which of the two things happened** (§4 failure mode 3) and names the fix: update the quote to what the code emits today, or fix the code — never unmark the block.

The census measured **14** blocks that pin as-is under these rules. The executor re-measures in Step 5; the floor is whatever that measurement says.

**Files:**
- Create: `crates/guide-examples/tests/doc_truth.rs`
- Modify: `docs/GUIDE.md` (markers on the blocks that pin; one sentence in the preamble)
- Modify: `CLAUDE.md:66` (`guide-examples: describe_contract/doc_inventory` → `.../doc_truth`) and `CLAUDE.md:89-90` (the GUIDE-editing bullet)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: the `<!-- pinned -->` convention, stated in GUIDE's preamble and CLAUDE.md.

- [ ] **Step 1: Write the gate, with its pure functions**

Create `crates/guide-examples/tests/doc_truth.rs`:

```rust
//! The doc-truth gate: a ```text block in docs/GUIDE.md whose preceding line
//! is `<!-- pinned -->` quotes what an operator actually sees, and this test
//! proves the code can still emit it. Hard rule 5 makes error text product
//! surface; hard rule 6 makes doc drift a bug. Until this file, neither was
//! checkable for the text blocks — build.rs compiles ```rust blocks only —
//! and "docs the code disproves" was found by reading, not by a gate, at
//! least four separate times (mechanical-fences spec, Item 4).
//!
//! What is pinned is a per-block, explicit choice. A one-time census
//! (2026-09-12) of all 41 text blocks found byte-exact error contracts mixed
//! with CLI usage, SSE frames, rustc output, sim reports carrying instance
//! numbers and an S3 lifecycle table; a rule that guessed which was which
//! would be the decoration spec §4 forbids. So a block is in scope only when
//! marked, a marker that pins nothing is an error, and the number of pinned
//! blocks is a FLOOR — lowering it is a deliberate act for a commit message,
//! never a way to make a red run green.
//!
//! Matching is by FRAGMENT: each pinned line is split on `<placeholder>`
//! tokens (lowercase-initial, so `Id<Ping>` stays literal), and every piece
//! of at least `FRAGMENT_MIN` characters must be a substring of the corpus —
//! every `.rs` under crates/ and examples/, with Rust's own literal rules
//! applied so the comparison is against what a literal DENOTES rather than
//! how it was typed (backslash-newline continuations vanish, `\"` decodes).
//! Tests are in the corpus on purpose: a rendered instance ("got 0") lives in
//! a pinning test's literal where the source has `{}`.
//!
//! Scope, so it is not read wider: an UNMARKED block is not checked. The
//! blocks left unmarked at landing are the ones that carry instance values
//! (`/notes/:key`, `20000000 bytes`) or are not source text at all; pinning
//! them is a doc rewrite with its own judgement, not this gate's.

// Reads repo docs and sources at test time: build tooling outside the
// deterministic boundary, the same sanctioned-edge pattern as this crate's
// build.rs and tests/doc_inventory.rs.
#![allow(clippy::disallowed_methods)]

use std::fs;
use std::path::{Path, PathBuf};

/// Hand-counted at landing (2026-09-12): the number of `<!-- pinned -->`
/// blocks in docs/GUIDE.md. Fewer than this is red.
const PINNED_FLOOR: usize = 14;

/// Below this a fragment is not evidence — `usage:` or `id: 1.0` would match
/// almost any corpus. Long enough that a coincidental hit is implausible,
/// short enough that the tail after one `<placeholder>` still counts.
const FRAGMENT_MIN: usize = 16;

const MARKER: &str = "<!-- pinned -->";
const FENCE: &str = "```text";

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// A pinned block: the GUIDE line its marker sits on, and its non-empty
/// lines each with their own GUIDE line number.
#[derive(Debug, PartialEq)]
struct Pinned {
    marker_line: usize,
    lines: Vec<(usize, String)>,
}

/// Every `<!-- pinned -->` block. A marker not immediately followed by a
/// ```text fence, or a fence never closed, is a panic naming the line — a
/// marker that pins nothing is decoration, and this gate refuses to carry it.
fn pinned_blocks(guide: &str) -> Vec<Pinned> {
    let lines: Vec<&str> = guide.lines().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        if lines[i].trim_end() == MARKER {
            let marker_line = i + 1;
            assert_eq!(
                lines.get(i + 1).map(|l| l.trim_end()),
                Some(FENCE),
                "docs/GUIDE.md:{marker_line}: `{MARKER}` must sit on the line immediately \
                 before a {FENCE} fence — a marker that pins nothing is decoration"
            );
            let start = i + 2;
            let mut j = start;
            while j < lines.len() && lines[j].trim_end() != "```" {
                j += 1;
            }
            assert!(
                j < lines.len(),
                "docs/GUIDE.md:{start}: the pinned {FENCE} block is never closed"
            );
            let block: Vec<(usize, String)> = (start..j)
                .filter(|k| !lines[*k].trim().is_empty())
                .map(|k| (k + 1, lines[k].trim_end().to_string()))
                .collect();
            assert!(
                !block.is_empty(),
                "docs/GUIDE.md:{marker_line}: the pinned block is empty"
            );
            out.push(Pinned {
                marker_line,
                lines: block,
            });
            i = j;
        }
        i += 1;
    }
    out
}

/// Whether `s` starting at byte `at` (which is `<`) opens a placeholder:
/// `<` + a lowercase letter + any run without `<`/`>` + `>`. Returns the
/// byte index just past the closing `>`.
fn placeholder_end(s: &str, at: usize) -> Option<usize> {
    let rest = &s[at + 1..];
    if !rest.chars().next().is_some_and(|c| c.is_ascii_lowercase()) {
        return None;
    }
    let close = rest.find('>')?;
    if rest[..close].contains('<') {
        return None;
    }
    Some(at + 1 + close + 1)
}

/// The evidence-bearing parts of one pinned line: the text between
/// placeholders, trimmed, at least `FRAGMENT_MIN` long.
fn fragments(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut piece_start = 0;
    let mut i = 0;
    while i < line.len() {
        if line.as_bytes()[i] == b'<'
            && let Some(end) = placeholder_end(line, i)
        {
            push_fragment(&mut out, &line[piece_start..i]);
            piece_start = end;
            i = end;
            continue;
        }
        i += 1;
    }
    push_fragment(&mut out, &line[piece_start..]);
    out
}

fn push_fragment(out: &mut Vec<String>, piece: &str) {
    let piece = piece.trim();
    if piece.chars().count() >= FRAGMENT_MIN {
        out.push(piece.to_string());
    }
}

/// Rust's string-literal rules applied to a source file, so a doc quote is
/// compared against what a literal denotes: `\` + newline + the next line's
/// leading whitespace vanish (a continuation), and `\"`, `\'`, `\\`, `\n`,
/// `\t` decode. Any other backslash pair is kept as typed. Raw strings need
/// nothing and are untouched by construction (they contain no escapes).
fn unescape(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    let mut chars = src.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('\n') => {
                while chars.peek().is_some_and(|c| *c == ' ' || *c == '\t') {
                    chars.next();
                }
            }
            Some('"') => out.push('"'),
            Some('\'') => out.push('\''),
            Some('\\') => out.push('\\'),
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

/// Every `.rs` under `crates/` and `examples/`, `target/` excluded, unescaped
/// and joined. Order does not matter — only membership.
fn normalized_corpus() -> String {
    fn walk(dir: &Path, out: &mut String) {
        let entries = fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
        for entry in entries {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n == "target") {
                    continue;
                }
                walk(&path, out);
            } else if path.extension().is_some_and(|x| x == "rs") {
                let src = fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
                out.push_str(&unescape(&src));
                out.push('\n');
            }
        }
    }
    let mut corpus = String::new();
    walk(&repo().join("crates"), &mut corpus);
    walk(&repo().join("examples"), &mut corpus);
    corpus
}

/// Every pinned fragment the corpus lacks, as `docs/GUIDE.md:LINE: "fragment"`
/// — plus every pinned line that yields no fragment at all. Pure, so the
/// gate's fireability is provable against a fabricated corpus.
fn missing(pinned: &[Pinned], corpus: &str) -> Vec<String> {
    let mut out = Vec::new();
    for block in pinned {
        for (line_no, text) in &block.lines {
            let frags = fragments(text);
            if frags.is_empty() {
                out.push(format!(
                    "docs/GUIDE.md:{line_no}: nothing on this line is at least {FRAGMENT_MIN} \
                     characters outside a <placeholder>, so it cannot be evidence — rewrite it \
                     or unpin the block at line {}",
                    block.marker_line
                ));
                continue;
            }
            for frag in frags {
                if !corpus.contains(&frag) {
                    out.push(format!("docs/GUIDE.md:{line_no}: {frag:?}"));
                }
            }
        }
    }
    out
}

#[test]
fn pinned_guide_text_is_what_the_code_emits() {
    let guide = fs::read_to_string(repo().join("docs/GUIDE.md")).expect("docs/GUIDE.md");
    let pinned = pinned_blocks(&guide);
    let missing = missing(&pinned, &normalized_corpus());
    assert!(
        missing.is_empty(),
        "docs/GUIDE.md quotes text the code no longer emits — {} fragment(s):\n  {}\n\
         contract: a `{MARKER}` block shows what the operator sees, byte-exact (hard rules 5 \
         and 6); every fragment of {FRAGMENT_MIN}+ characters outside a <placeholder> must \
         appear in some .rs file under crates/ or examples/.\n\
         fix: EITHER the doc drifted — update the quote to what the code emits today (grep \
         crates/ for a distinctive part of the old text to find the site) — OR the code \
         regressed and the doc is right — fix the code. A rephrasing of the CODE's message \
         is a doc update; a rephrasing of the DOC's quote is exactly what this gate refuses. \
         Do not unpin the block to pass: the pinned count is a floor ({PINNED_FLOOR}) and \
         lowering it belongs in its own commit message.",
        missing.len(),
        missing.join("\n  "),
    );
}

#[test]
fn the_pinned_block_floor_holds() {
    let guide = fs::read_to_string(repo().join("docs/GUIDE.md")).expect("docs/GUIDE.md");
    let n = pinned_blocks(&guide).len();
    assert!(
        n >= PINNED_FLOOR,
        "docs/GUIDE.md has {n} `{MARKER}` block(s); the floor is {PINNED_FLOOR}.\n\
         contract: the gate's corpus may not silently shrink — a marker was removed, or the \
         marker line drifted away from its fence (it must be the line IMMEDIATELY before \
         ```text).\n\
         fix: restore the marker; if a block was unpinned deliberately, lower PINNED_FLOOR in \
         the same commit and say why in its message."
    );
}

// ----------------------------------------------------------- fireability

#[test]
fn a_fragment_the_corpus_lacks_is_named_with_its_guide_line() {
    let pinned = vec![Pinned {
        marker_line: 10,
        lines: vec![
            (12, "peer secret missing or mismatched — set ReplConfig.peer_secret".into()),
            (13, "this exact sentence is nowhere in any source file".into()),
        ],
    }];
    let corpus = "eprintln!(\"peer secret missing or mismatched — set ReplConfig.peer_secret\")";
    assert_eq!(
        missing(&pinned, corpus),
        vec!["docs/GUIDE.md:13: \"this exact sentence is nowhere in any source file\"".to_string()]
    );
    let full = "peer secret missing or mismatched — set ReplConfig.peer_secret\n\
                this exact sentence is nowhere in any source file";
    assert!(missing(&pinned, full).is_empty());
}

#[test]
fn fragments_split_on_lowercase_placeholders_and_keep_generics() {
    assert_eq!(
        fragments("job decode for SendReceipt: <the underlying decode error> — schema drift or corruption"),
        vec![
            "job decode for SendReceipt:".to_string(),
            "— schema drift or corruption".to_string()
        ]
    );
    // `Id<Ping>` is a generic, not a placeholder: capital P.
    assert_eq!(
        fragments("missing `id: Id<Ping>` field — every #[table] row needs a typed primary key"),
        vec!["missing `id: Id<Ping>` field — every #[table] row needs a typed primary key".to_string()]
    );
    // Two placeholders back to back: the `.` between them is not evidence
    // (too short); the 16-character head and the tail both are.
    assert_eq!(
        fragments("cursor must be \"<id>.<seq>\" — got \"not-a-cursor\"; omit it"),
        vec![
            "cursor must be \"".to_string(),
            "\" — got \"not-a-cursor\"; omit it".to_string()
        ]
    );
    assert!(fragments("id: 1.0").is_empty());
    assert!(fragments("<e>").is_empty());
}

#[test]
fn unescape_applies_rusts_literal_rules() {
    assert_eq!(
        unescape("\"one \\\n         two\""),
        "\"one two\"",
        "a backslash-newline continuation and the indentation after it vanish"
    );
    assert_eq!(unescape(r#"send \"Authorization: Bearer <token>\""#), r#"send "Authorization: Bearer <token>""#);
    assert_eq!(unescape(r"a\\b"), r"a\b");
    assert_eq!(unescape(r"tab\there"), "tab\there");
    assert_eq!(unescape(r"\x41 stays"), r"\x41 stays", "unknown pairs are kept as typed");
}

#[test]
fn a_marker_that_pins_nothing_is_refused() {
    let guide = "prose\n<!-- pinned -->\n```rust\nfn x() {}\n```\n";
    let r = std::panic::catch_unwind(|| pinned_blocks(guide));
    assert!(r.is_err(), "a marker before a non-text fence must panic, not silently pin nothing");
    let guide = "prose\n<!-- pinned -->\n```text\nline one\n\nline two\n```\nmore\n";
    assert_eq!(
        pinned_blocks(guide),
        vec![Pinned {
            marker_line: 2,
            lines: vec![(4, "line one".into()), (6, "line two".into())]
        }]
    );
}
```

Note on `catch_unwind` in the last test: it does not install a hook and trips no deny-list entry. If clippy objects to `catch_unwind` on a closure that borrows, wrap the argument in `std::panic::AssertUnwindSafe`.

- [ ] **Step 2: Run the pure tests only — they must pass before any marker exists**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente-fences && export CARGO_TARGET_DIR=/tmp/charpente-fences-target
cargo test -p guide-examples --test doc_truth -- fragments unescape a_fragment a_marker 2>&1 | tee /tmp/fences-t3-pure.log; echo "exit ${pipestatus[1]}"
```

Expected: exit 0, `4 passed`. If `fragments_split_on_lowercase_placeholders_and_keep_generics` fails on the third case, check whether `— got "not-a-cursor"; omit it` is ≥ 16 chars (it is: 27) and whether `placeholder_end` handled `<id>.<seq>` as two placeholders.

- [ ] **Step 3: RED #1 — the floor refuses an empty corpus of markers**

```bash
cargo test -p guide-examples --test doc_truth the_pinned_block_floor_holds 2>&1 | tee /tmp/fences-t3-red-floor.log; echo "exit ${pipestatus[1]}"
```

Expected: exit non-zero, message `docs/GUIDE.md has 0 \`<!-- pinned -->\` block(s); the floor is 14`. This is §4 failure mode 1 observed: without the floor, a gate over zero blocks would be green.

- [ ] **Step 4: Mark the blocks that pin as-is**

Run this census script from the worktree root; it prints, for every ```text block in GUIDE, whether it pins under the gate's exact rules, and why not when it does not. It is the same logic as the test, in a form that can be read at a glance:

```bash
cat > /tmp/fences-census.py <<'PYEOF'
import re, pathlib
MIN = 16
def unescape(t):
    t = re.sub(r"\\\n[ \t]*", "", t)
    return t.replace('\\"', '"').replace("\\'", "'").replace("\\n", "\n").replace("\\t", "\t").replace("\\\\", "\\")
corpus = "\n".join(unescape(p.read_text(errors="replace"))
    for p in list(pathlib.Path("crates").rglob("*.rs")) + list(pathlib.Path("examples").rglob("*.rs"))
    if "/target/" not in str(p))
PLACEHOLDER = re.compile(r"<[a-z][^<>]*>")
guide = pathlib.Path("docs/GUIDE.md").read_text().split("\n")
i = 0; pins = []
while i < len(guide):
    if guide[i].rstrip() == "```text":
        start = i + 1; j = start
        while guide[j].rstrip() != "```": j += 1
        lines = [l.rstrip() for l in guide[start:j] if l.strip()]
        ok = True; why = []
        for l in lines:
            frags = [f.strip() for f in PLACEHOLDER.split(l) if len(f.strip()) >= MIN]
            if not frags: ok = False; why.append(f"no fragment >= {MIN}: {l[:60]!r}"); continue
            for f in frags:
                if f not in corpus: ok = False; why.append(f"missing: {f[:70]!r}")
        marker_line = i  # 1-based line number of the line BEFORE the fence == i (0-based index i-1 +1)
        print(f"{'PIN' if ok else '   '} fence at GUIDE.md:{i+1}  {lines[0][:70]}")
        for w in why[:2]: print(f"        {w}")
        if ok: pins.append(i + 1)
        i = j
    i += 1
print(f"\npinnable as-is: {len(pins)}; fence lines: {pins}")
PYEOF
python3 /tmp/fences-census.py | tee /tmp/fences-census.log
```

Expected: `pinnable as-is: 14` (the 2026-09-12 census). If the number differs, the corpus changed since the census — that is fine; the floor is what YOU measure, and you record the difference in the report.

For every fence line the census printed as `PIN`, insert the line `<!-- pinned -->` immediately before that ```` ```text ```` line. Do it with one pass that works from the bottom up so earlier line numbers stay valid:

```bash
for n in $(python3 -c "import re; print(' '.join(str(x) for x in sorted($(sed -n 's/.*fence lines: //p' /tmp/fences-census.log), reverse=True)))"); do
  perl -pi -e 'print "<!-- pinned -->\n" if $. == '"$n"'' docs/GUIDE.md
done
grep -c '^<!-- pinned -->$' docs/GUIDE.md
```

Expected: the count printed equals the census's `pinnable as-is` number. Then confirm every marker is immediately followed by a text fence:

```bash
grep -n -A1 '^<!-- pinned -->$' docs/GUIDE.md | grep -v 'pinned\|```text\|^--$' ; echo "(anything printed above is a misplaced marker)"
```

- [ ] **Step 5: Set the floor to the measured count, and GREEN**

If the census number is not 14, edit `PINNED_FLOOR` in `doc_truth.rs` to the measured number and update the "Hand-counted at landing" comment. Then:

```bash
cargo test -p guide-examples --test doc_truth 2>&1 | tee /tmp/fences-t3-green.log; echo "exit ${pipestatus[1]}"
```

Expected: exit 0, `6 passed` (2 gates + 4 fireability tests).

- [ ] **Step 6: RED #2 — falsify one pinned doc line and watch the gate name it (the spec's required observation)**

Pick the pinned block at the `peer secret missing or mismatched` line (fence near GUIDE.md:2840). Change ONE word inside the block — `mismatched` → `mismatchedd` — with a scratch edit:

```bash
LN=$(grep -n '^peer secret missing or mismatched' docs/GUIDE.md | head -1 | cut -d: -f1); echo "line $LN"
perl -pi -e 's/^peer secret missing or mismatched/peer secret missing or mismatchedd/ if $. == '"$LN" docs/GUIDE.md
cargo test -p guide-examples --test doc_truth pinned_guide_text_is_what_the_code_emits 2>&1 | tee /tmp/fences-t3-red-drift.log; echo "exit ${pipestatus[1]}"
```

Expected: exit non-zero, and the message names `docs/GUIDE.md:<that line>:` with the fragment beginning `"peer secret missing or mismatchedd`. Paste that line into the report: this is spec §5 criterion 4's "observed RED against a deliberately falsified doc string". Restore:

```bash
perl -pi -e 's/mismatchedd/mismatched/ if $. == '"$LN" docs/GUIDE.md
cargo test -p guide-examples --test doc_truth 2>&1 | grep -E '^test result'
git diff --stat docs/GUIDE.md   # only the marker insertions: N insertions, 0 deletions
```

- [ ] **Step 7: State the convention where authors look**

In `docs/GUIDE.md`, the preamble paragraph beginning `*(This file is compile-gated: every \`rust\` code block below is extracted` — after the sentence ending `if a block stops compiling.` insert:

```
A `text` block preceded by the line `<!-- pinned -->` is gated too:
`crates/guide-examples/tests/doc_truth.rs` proves every fragment of it
outside a `<placeholder>` still appears in the code, so a quoted error
message cannot drift from what the binary prints.
```

In `CLAUDE.md`:
- Line 66: change `guide-examples: describe_contract/doc_inventory;` to `guide-examples: describe_contract/doc_inventory/doc_truth;`.
- The bullet at line 89 beginning `- Editing docs/GUIDE.md can break the build:` — after `\`cargo build -p guide-examples\` checks it alone.` append: ` A \`\`\`text block preceded by \`<!-- pinned -->\` is gated by \`tests/doc_truth.rs\`: its quoted text must still appear in the code, and the pinned count is a floor.`

Verify the inventory gate accepts the new file (it would have been red since Step 1 — `doc_inventory` refuses an unlisted test file, which is the gate working):

```bash
cargo test -p guide-examples --no-fail-fast 2>&1 | tee /tmp/fences-t3-crate.log; echo "exit ${pipestatus[1]}"; grep -c "^test result: ok" /tmp/fences-t3-crate.log
```

Predict BEFORE running: **5** `test result: ok` (was 4; `doc_truth` adds one binary). Expected: exit 0, 5.

- [ ] **Step 8: fmt + clippy on the crate, then commit**

```bash
cargo fmt --all && cargo fmt --all --check; echo "fmt $?"
cargo clippy -p guide-examples --all-targets -- -D warnings 2>&1 | tail -2; echo "clippy ${pipestatus[1]}"
git diff --stat   # docs/GUIDE.md, CLAUDE.md, and the new test file only
git add crates/guide-examples/tests/doc_truth.rs docs/GUIDE.md CLAUDE.md
git commit -F - <<'EOF'
fence(docs): GUIDE's quoted error text is gated against the code that emits it

A ```text block marked `<!-- pinned -->` now has every fragment of 16+
characters outside a <placeholder> checked for presence in the .rs corpus
(crates/ and examples/, Rust's literal rules applied so `\"` and
backslash-newline continuations compare as what they denote). N blocks
pinned at landing, hand-counted by a census over all 41; that count is a
FLOOR, so the gate cannot go green by losing its corpus. RED observed
twice: the floor over zero markers, and a one-letter falsification of a
pinned line named by GUIDE line and fragment.

The unmarked blocks carry instance values or are not source text (rustc,
sim reports, SSE frames, an S3 table); pinning them is a doc rewrite for
another day, and the gate says so rather than guessing.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017gCrhXqM7QqtVbwYF1266M
EOF
```

Replace `N` in the message with the measured count.

---

### Task 4: The ride-along, and the whole-corpus gate (Item 6, spec §5 criteria 6-7)

**Files:**
- Modify: `DESIGN.md:1575-1580`

- [ ] **Step 1: Indent the six continuation lines**

`DESIGN.md` §9 item 5's list entry wraps with 3-space continuation; the last six lines — from `stated mechanism — "app iteration recompiles app code only"` through `typecheck, codegen and link, and show link dominant.` — carry none. Confirm, then fix:

```bash
grep -n '^stated mechanism\|^every incremental\|^dylib could still\|^dev-loop risk\|^decomposition that\|^typecheck, codegen' DESIGN.md
```

Expected: six hits, lines 1575-1580. Then:

```bash
perl -pi -e 's/^(?=stated mechanism|every incremental|dylib could still|dev-loop risk|decomposition that|typecheck, codegen)/   / if $. >= 1575 && $. <= 1580' DESIGN.md
git diff DESIGN.md | grep -c '^+   '   # must print 6
git diff DESIGN.md | grep '^[-+]' | grep -v '^[-+][-+]' | awk '{print length($0)}' | sort -n | tail -1   # sanity: no line grew past ~100
```

- [ ] **Step 2: The whole-workspace gate, counted**

Predict BEFORE running: **83** `test result: ok` (82 on `main` + `doc_truth`).

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente-fences && export CARGO_TARGET_DIR=/tmp/charpente-fences-target
cargo fmt --all && cargo fmt --all --check; echo "fmt $?"
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tee /tmp/fences-t4-clippy.log | tail -2; echo "clippy ${pipestatus[1]}"
cargo test --workspace --no-fail-fast 2>&1 | tee /tmp/fences-t4-test.log; echo "test exit ${pipestatus[1]}"
grep -c "^test result: ok" /tmp/fences-t4-test.log
grep -c "^test result: FAILED" /tmp/fences-t4-test.log
```

Expected: fmt 0, clippy 0, test exit 0, **83** ok, 0 FAILED. This run can exceed 10 minutes cold; run it in the foreground regardless and wait.

- [ ] **Step 3: Prove no runtime behaviour changed (spec §5 criterion 7)**

```bash
git diff --stat "20 20 12 61 79 80 81 701 33 98 100 204 250 395 398 399 400git merge-base main HEAD)"..HEAD
```

Expected files, and nothing else: `clippy.toml`, `crates/charpente-sim/src/harness.rs`, `crates/edge-harness/tests/open_fence.rs`, `crates/charpente-cli/src/mutants.rs`, `crates/charpente-cli/tests/cli.rs`, `scripts/mutants.sh`, `crates/guide-examples/tests/doc_truth.rs`, `docs/GUIDE.md`, `CLAUDE.md`, `DESIGN.md`. Of these, the only non-test, non-doc, non-config source files are `harness.rs` (doc + attribute + comment only — verify with `git diff "20 20 12 61 79 80 81 701 33 98 100 204 250 395 398 399 400git merge-base main HEAD)"..HEAD -- crates/charpente-sim/src/harness.rs | grep '^[-+]' | grep -v '^[-+][-+]' | grep -v '^[-+]\s*//'` printing only the `#[allow]` line) and `mutants.rs` (a refusal that returns before the spawn). State this in the report verbatim with the grep output.

- [ ] **Step 4: Commit**

```bash
git add DESIGN.md
git commit -F - <<'EOF'
docs(design): §9 item 5's continuation lines carry the list's indent

Six lines introduced by the 2026-09-09 plan text at zero indent where
every neighbour carries three. Renders identically under CommonMark's
lazy continuation; fixed so the source reads as the list it is.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017gCrhXqM7QqtVbwYF1266M
EOF
```

---

## Controller-owned steps after Task 4 (not subagent tasks)

1. Whole-branch review (opus), written to `.superpowers/sdd/2026-09-12-mechanical-fences/final-review.md`, checking each spec §5 criterion against the observations the task reports pasted — the `-->` lines from Task 1, the old verb's stderr from Task 2, the named GUIDE line from Task 3.
2. Merge `mechanical-fences` into `main` with `--no-ff`; re-run the gate on the merge in the MAIN tree (predict 83), then push.
3. Ledger entry: the six items, each with its RED observation quoted, the measured pinned count and the unpinned classes, and what this does NOT settle (the 15 WAL one-sided properties, queued next; the unpinned GUIDE blocks).
4. Remove the worktree and `/tmp/charpente-fences-target`.
