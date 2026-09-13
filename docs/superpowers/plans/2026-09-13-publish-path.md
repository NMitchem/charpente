# Publish Path and `charpente new` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A stranger can `cargo install charpente-cli`, run `charpente new myapp`, and have an app that is clippy-clean under hard rule 1's fence and green under `charpente test --sim` — with the six crates publishable at `0.1.0` behind a dry-run gate, and every line of the scaffold held to the guide by a test.

**Architecture:** Manifests move to `0.1.0` and `charpente-tsgen` becomes publishable; `cargo publish --workspace --dry-run` (cargo ≥ 1.90 orders the workspace itself) is the gate, in CI without verify and in `scripts/publish.sh` with it. `charpente new` is one new module in `charpente-cli` that renders six `include_str!`'d templates with three placeholders and refuses by name. Its oracles are `tests/new.rs`: refusal text, a byte-diff of the rendered `Cargo.toml` against GUIDE §1's block, a subset check of the emitted `clippy.toml` against the workspace's, and one end-to-end test that scaffolds into a temp dir, patches the unpublished crates in, and runs fmt, clippy and the sim.

**Tech Stack:** Rust 1.95 / cargo 1.95 (`cargo publish --workspace`); `charpente::launch`, `charpente::http::{App, Request, Method}`, `charpente::kernel::{Kernel, SimSource}`, `charpente_sim::{Sim, sim_tests!, checks}`; `std::process::Command` (sanctioned in `charpente-cli` only).

**Spec:** `docs/superpowers/specs/2026-09-13-publish-path-design.md`. §4 is the authority on the verb's refusals, §5 on the template, §7 on the oracles. This plan corrects one line of the spec (§10 criterion 5 and the matching §5 sentence) in Task 0, for the reason given there.

## Global Constraints

Every task's requirements implicitly include this section.

- **Worktree and target dir.** All work in `/Users/noahmitchem/Documents/GitHub/charpente-publish-path` on branch `publish-path` (cut from `main` at the plan commit), with `export CARGO_TARGET_DIR=/tmp/charpente-publish-path-target` in every shell. Never share a `CARGO_TARGET_DIR` across worktrees. The per-plan SDD directory lives in the **main** tree — `/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-09-13-publish-path/` — so it survives the worktree's removal; every agent writes its report there before returning.
- **Verification is foregrounded.** Never background `cargo test`. If a command may exceed the 600 s tool ceiling (Task 2's verify build; the workspace suite), run it with `run_in_background` writing to a log file under the SDD directory, then read the log — never `TaskOutput` with `block=true`.
- **Count, never infer.** Pipe cargo through `tee`, never `tail`; count `^test result: ok` lines; state the expected count BEFORE running. `cargo test --workspace --no-fail-fast` is **85** `test result: ok` at the plan commit (ledger, 2026-09-12). Task 3 adds one binary (`crates/charpente-cli/tests/new.rs`) → **86**. No other task adds a binary.
- **This shell is zsh.** `${pipestatus[1]}`, not `${PIPESTATUS[0]}`; an unquoted `$var` is one word; `timeout`/`gtimeout` are absent.
- **No behaviour change anywhere but `charpente-cli`.** The other five crates change only in `Cargo.toml` metadata and three pinned version strings in tests. If a task seems to need a change in `charpente`, `charpente-core`, `charpente-sim`, `charpente-macros` or `charpente-tsgen` source, stop and record it in `decisions-pending.md` in the SDD directory.
- **The deny-list is site-local.** Every `std::fs::*`, `Command::new` or `std::env::temp_dir` call in new code carries `#[allow(clippy::disallowed_methods)]` on the statement or fn with a one-line rationale, in the shape `crates/charpente-cli/src/genclient.rs:73-77` and `tests/cli.rs:3-9` use. Never a file-level or module-level allow (`hook_census.rs` sees through them and the convention refuses them).
- **Hard rule 5 in every refusal**: the contract, the site, the fix. Tests assert the text.
- **Commit named paths only**, never `git add -A`. Every commit ends with `Claude-Session: https://claude.ai/code/session_012dRc31xomeEgW4QZUYdjPr` and **no** `Co-Authored-By` line (CLAUDE.md, Workflow).
- **Any claim about a commit is checked with `git show <sha>:<path>`**, never the working tree.
- **Dispatches are unnamed.**
- **Blocked work** goes to `decisions-pending.md` in the SDD directory with the reading taken, the reasoning, and what would falsify it.

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `docs/superpowers/specs/2026-09-13-publish-path-design.md` | §5/§10 correction: the outbox bug proper needs DEMO.md's framework patch | 0 |
| `Cargo.toml` | `version = "0.1.0"`, dep versions, `repository` | 1 |
| `crates/{charpente-core,charpente-macros,charpente-sim,charpente,charpente-tsgen,charpente-cli}/Cargo.toml` | `repository.workspace = true`; tsgen loses `publish = false`; `readme` on two | 1 |
| `crates/ts-fixture-app/Cargo.toml` | tsgen dep version | 1 |
| `crates/charpente/tests/describe.rs`, `crates/charpente-cli/tests/describe.rs`, `crates/charpente-cli/src/genclient.rs`, `examples/invoicing/tests/describe_golden.rs`, `docs/GUIDE.md` §13 | the pinned `"charpente_version"` string | 1 |
| `.github/workflows/ci.yml` | the publish dry-run gate | 1 |
| `scripts/publish.sh` | NEW — dirty-tree refusal, dry-run default, `--for-real` | 2 |
| `crates/charpente-cli/templates/{Cargo.toml.tmpl,clippy.toml,gitignore,lib.rs,main.rs.tmpl,sim.rs.tmpl}` | NEW — the scaffold | 3 |
| `crates/charpente-cli/src/new.rs` | NEW — the verb: validation, rendering, writing, refusals | 3 |
| `crates/charpente-cli/src/main.rs` | dispatch `new`; its `USAGE` in the fallthrough | 3 |
| `crates/charpente-cli/tests/new.rs` | NEW — refusals, six files, no `{{`, GUIDE byte-diff, clippy subset, end-to-end | 3, 4, 5 |
| `CLAUDE.md` | test inventory gains `new` | 3 |
| `docs/GUIDE.md` §1 | install, `new`, the six files, the fence, two dependencies, the `Cargo.toml` block | 5 |
| `README.md` | "Try it" block | 5 |
| `PLAN.md` | M3 sub-project 1 and 5 wording | 5 |
| `DESIGN.md` §7 | verb inventory gains `new` | 5 |
| `docs/LEDGER.md` | close entry with the evidence | 6 |

---

### Task 0: Worktree, SDD directory, and the spec correction

**Files:**
- Modify: `docs/superpowers/specs/2026-09-13-publish-path-design.md` (§5 last paragraph before "Nothing one-sided ships", and §10 item 5)

Why the correction: `examples/invoicing/DEMO.md` records that the outbox bug **cannot be written in app code** — routes only see `&mut Tx`, so splitting the commit required a temporary framework patch (`Ctx::enqueue_in_a_second_transaction`, commit `c0dbcea`). The spec's "moving the enqueue outside the transaction turns that property red" is therefore true only under that patch. The property's shape is unchanged — it is DEMO.md's own invariant pair (ack ⇒ job row durable; effect ≡ Done), which catches a deleted enqueue, a neutered job, and the dual-write when the patch is applied — but the app-level RED a stranger can produce is *deleting* the enqueue, and the plan's evidence (Task 6) is that.

- [ ] **Step 1: Cut the worktree and create the SDD directory**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
git status --short            # must be empty
git worktree add -b publish-path /Users/noahmitchem/Documents/GitHub/charpente-publish-path main
mkdir -p /Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-09-13-publish-path
cd /Users/noahmitchem/Documents/GitHub/charpente-publish-path
export CARGO_TARGET_DIR=/tmp/charpente-publish-path-target
git log -1 --format='%h %s'   # the plan commit
```

- [ ] **Step 2: Correct the spec's two sentences**

In §5, replace:

```
   one property. It goes red when `Echo::run` is neutered (`charpente test --mutants`) and
   when the enqueue is moved outside the transaction (the DEMO.md outbox bug) — so the
   launch run mutates the scaffold, not invoicing.
```

with:

```
   one property, and it is DEMO.md's own oracle shape (`receipts_exactly_once`): acked ⇒ an
   `Echo` job row is durable; `echoed` ≡ that job is Done. It goes red when `Echo::run` is
   neutered (`charpente test --mutants`), when the `tx.enqueue` line is deleted, and — under
   the framework patch DEMO.md describes (`c0dbcea`), which is the only way to write it —
   when the enqueue is split into a second transaction. So the launch run mutates the
   scaffold, not invoicing; whether it re-applies that patch is sub-project 5's call.
```

In §10, replace item 5 with:

```
5. Under the template, `charpente test --mutants` reports `Echo::run` → `Ok(())` **caught**
   by `acked_post_implies_the_echo_after_recovery`; and deleting the `tx.enqueue` line turns
   that property red under `--sim`, with a seed. Both recorded — they are what the launch
   run will show. (The dual-write proper needs DEMO.md's framework patch; sub-project 5.)
```

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-09-13-publish-path-design.md
git commit -m "spec(publish-path): the outbox bug proper needs DEMO.md's framework patch; the app-level RED is a deleted enqueue

Claude-Session: https://claude.ai/code/session_012dRc31xomeEgW4QZUYdjPr"
```

---

### Task 1: Manifests to 0.1.0, tsgen publishable, the version pins, and the CI gate

**Files:**
- Modify: `Cargo.toml:19-26`
- Modify: `crates/charpente-core/Cargo.toml`, `crates/charpente-macros/Cargo.toml`, `crates/charpente-sim/Cargo.toml`, `crates/charpente/Cargo.toml`, `crates/charpente-tsgen/Cargo.toml`, `crates/charpente-cli/Cargo.toml:19`, `crates/ts-fixture-app/Cargo.toml:32`
- Modify: `crates/charpente/tests/describe.rs:166`, `crates/charpente-cli/tests/describe.rs:34`, `crates/charpente-cli/src/genclient.rs:601`, `examples/invoicing/tests/describe_golden.rs:36`, `docs/GUIDE.md:3942`
- Modify: `.github/workflows/ci.yml` (the `check` job, after the `cargo check -p charpente-core` step)
- Modify: `Cargo.lock` (cargo rewrites it)

**Interfaces:**
- Produces: the six publishable crates at `0.1.0`; `cargo publish --workspace --dry-run --no-verify` exits 0. Task 3's `dependency_version()` reads `CARGO_PKG_VERSION` and must return `"0.1"`.

- [ ] **Step 1: Observe the gate RED on the plan commit**

Run (network required — cargo updates the index):

```bash
cargo publish --workspace --dry-run --no-verify 2>&1 | tee ../charpente/.superpowers/sdd/2026-09-13-publish-path/t1-red.log | tail -8
echo "exit=${pipestatus[1]}"
```

Expected: exit **101**, and the log ends with
`no matching package named `charpente-tsgen` found` … `required by package `charpente-cli``.
That is the finding the spec names: `--workspace` skips `publish = false` members, so the
CLI's dependency is missing from the set. Also note the `warning: manifest has no
documentation, homepage or repository` lines — `repository` fixes them.

- [ ] **Step 2: Bump the workspace version and dependency requirements**

In `Cargo.toml`, the `[workspace.package]` and `[workspace.dependencies]` blocks become:

```toml
[workspace.package]
version = "0.1.0"
edition = "2024"
license = "MIT OR Apache-2.0"
repository = "https://github.com/NMitchem/charpente"

[workspace.dependencies]
charpente-core = { path = "crates/charpente-core", version = "0.1.0" }
charpente-macros = { path = "crates/charpente-macros", version = "0.1.0" }
charpente = { path = "crates/charpente", version = "0.1.0" }
```

In `crates/charpente-cli/Cargo.toml` line 19 and `crates/ts-fixture-app/Cargo.toml` line 32,
`version = "0.0.0"` → `version = "0.1.0"` on the `charpente-tsgen` dependency.

- [ ] **Step 3: Per-crate metadata**

In each of `crates/charpente-core/Cargo.toml`, `crates/charpente-macros/Cargo.toml`,
`crates/charpente-sim/Cargo.toml`, `crates/charpente-tsgen/Cargo.toml`: add
`repository.workspace = true` directly under `license.workspace = true`.

In `crates/charpente-tsgen/Cargo.toml`: delete the `publish = false` line.

In `crates/charpente/Cargo.toml` and `crates/charpente-cli/Cargo.toml`: add, under
`license.workspace = true`:

```toml
repository.workspace = true
readme = "../../README.md"
```

- [ ] **Step 4: Observe the version pins RED**

Predict: four test targets fail on `"charpente_version": "0.0.0"` vs `"0.1.0"`.

```bash
cargo test -p charpente --test describe 2>&1 | tee ../charpente/.superpowers/sdd/2026-09-13-publish-path/t1-pins-red.log | grep -E '^test result|charpente_version' | head
cargo test -p charpente-cli --test describe --bin charpente 2>&1 | grep -E '^test result|charpente_version' | head
cargo test -p invoicing --test describe_golden 2>&1 | grep -E '^test result|charpente_version' | head
cargo test -p guide-examples --test describe_contract 2>&1 | grep -E '^test result|charpente_version' | head
```

Expected: each prints at least one `FAILED` result line. (`--bin charpente` covers
`genclient.rs:601`, which is inside that binary's `#[cfg(test)]`.) If a target is
GREEN here, its pin is not where this plan says — find it with
`grep -rn '"charpente_version"' crates examples docs` before changing anything.

- [ ] **Step 5: Move the five pins**

```bash
for f in crates/charpente/tests/describe.rs crates/charpente-cli/tests/describe.rs crates/charpente-cli/src/genclient.rs examples/invoicing/tests/describe_golden.rs docs/GUIDE.md; do
  sed -i '' 's/"charpente_version": "0\.0\.0"/"charpente_version": "0.1.0"/' "$f"
done
grep -rn '"charpente_version": "0' crates examples docs | grep -v target   # every hit reads 0.1.0
```

- [ ] **Step 6: Observe the pins GREEN and the gate GREEN**

Re-run the four commands from Step 4: every `test result:` line reads `ok`.

Then the gate:

```bash
cargo publish --workspace --dry-run --no-verify 2>&1 | tee ../charpente/.superpowers/sdd/2026-09-13-publish-path/t1-green.log | tail -12
echo "exit=${pipestatus[1]}"
```

Expected: exit **0**; six `Packaging` lines (`charpente-core`, `charpente-macros`,
`charpente-sim`, `charpente`, `charpente-tsgen`, `charpente-cli`, in that dependency
order); **no** `manifest has no documentation, homepage or repository` warning. Then
prove the readme travelled:

```bash
cargo package --list -p charpente | grep -c '^README.md$'        # 1
cargo package --list -p charpente-cli | grep -c '^README.md$'    # 1
```

If `cargo publish --dry-run` refuses for want of a token ("no token found"), record the
exact text in `decisions-pending.md` and use
`cargo package --no-verify -p charpente-core -p charpente-macros -p charpente-sim -p charpente -p charpente-tsgen -p charpente-cli`
as the gate instead, here and in Step 7 — it packages the same six and catches the same
manifest defects, minus the "is the dependency in the publishable set" check.

- [ ] **Step 7: The CI step**

In `.github/workflows/ci.yml`, after the `- run: cargo check -p charpente-core` step and
its comment block, add:

```yaml
      # The publish gate (spec 2026-09-13-publish-path §3): `--workspace` skips
      # `publish = false` members and then refuses a publishable crate whose dependency
      # is not in the set — which is exactly how charpente-tsgen's `publish = false`
      # was caught on 2026-09-13. `--no-verify` skips the six verify builds; the full
      # verify runs in scripts/publish.sh's dry-run, not here (metered minutes). This
      # proves the manifests, not the packaged sources.
      - run: cargo publish --workspace --dry-run --no-verify
```

- [ ] **Step 8: fmt, clippy, and the workspace suite**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -3
```

Both clean. Then, predicting **85**:

```bash
cargo test --workspace --no-fail-fast 2>&1 | tee ../charpente/.superpowers/sdd/2026-09-13-publish-path/t1-suite.log > /dev/null
grep -c '^test result: ok' ../charpente/.superpowers/sdd/2026-09-13-publish-path/t1-suite.log   # 85
grep -c 'FAILED' ../charpente/.superpowers/sdd/2026-09-13-publish-path/t1-suite.log            # 0 (grep exits 1 — that is the pass)
```

(This may exceed 600 s cold; if so, background it with the log path above and read the
counts from the file.)

- [ ] **Step 9: Commit**

```bash
git add Cargo.toml Cargo.lock crates/charpente-core/Cargo.toml crates/charpente-macros/Cargo.toml crates/charpente-sim/Cargo.toml crates/charpente/Cargo.toml crates/charpente-tsgen/Cargo.toml crates/charpente-cli/Cargo.toml crates/ts-fixture-app/Cargo.toml crates/charpente/tests/describe.rs crates/charpente-cli/tests/describe.rs crates/charpente-cli/src/genclient.rs examples/invoicing/tests/describe_golden.rs docs/GUIDE.md .github/workflows/ci.yml
git commit -m "release: the six crates to 0.1.0, tsgen publishable, and the publish dry-run as a CI gate

cargo publish --workspace --dry-run --no-verify was RED on the plan commit
(no matching package named charpente-tsgen — --workspace skips publish=false
members and then refuses the CLI whose dependency is missing) and is GREEN
here. repository on every crate, readme on the two a stranger lands on;
five charpente_version pins moved. Workspace suite 85 ok.

Claude-Session: https://claude.ai/code/session_012dRc31xomeEgW4QZUYdjPr"
```

---

### Task 2: `scripts/publish.sh` and the full verify dry-run

**Files:**
- Create: `scripts/publish.sh`

**Interfaces:**
- Produces: `scripts/publish.sh [--for-real]`. Exit 0 on a clean dry-run; 2 on a dirty tree or bad flag; cargo's own code otherwise.

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# Publish the six charpente crates to crates.io, or (default) prove that they would
# publish. Spec: docs/superpowers/specs/2026-09-13-publish-path-design.md §3.
#
# `cargo publish --workspace` (cargo >= 1.90) orders the crates itself and skips
# every `publish = false` member. The dry-run here is the FULL one — it verify-builds
# each packaged crate — which is the half CI's --no-verify step deliberately leaves
# out. Run this once before every real publish; there is no other place it runs.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -n "$(git status --porcelain)" ]]; then
  echo "publish.sh: the tree is dirty — commit or stash first; a publish must be reproducible from a commit" >&2
  git status --short >&2
  exit 2
fi

version="$(grep -m1 '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/')"
head="$(git rev-parse --short HEAD)"

case "${1:-}" in
  "")
    echo "publish.sh: DRY RUN of charpente $version at $head (full verify build; pass --for-real to publish)"
    cargo publish --workspace --dry-run
    ;;
  --for-real)
    echo "publish.sh: PUBLISHING charpente $version at $head to crates.io"
    cargo publish --workspace
    ;;
  *)
    echo "usage: scripts/publish.sh [--for-real]   (default: dry run with verify)" >&2
    exit 2
    ;;
esac
```

```bash
chmod +x scripts/publish.sh
```

- [ ] **Step 2: Prove the two refusals**

```bash
scripts/publish.sh --nonsense; echo "exit=$?"     # usage line, exit 2
touch scratch.tmp; scripts/publish.sh; echo "exit=$?"; rm scratch.tmp   # dirty-tree refusal, exit 2
```

- [ ] **Step 3: The full dry-run, recorded**

This verify-builds six crates and will likely exceed the 600 s tool ceiling cold. Run it
in the background writing to the SDD directory, then read the log:

```bash
scripts/publish.sh > ../charpente/.superpowers/sdd/2026-09-13-publish-path/t2-dry-run.log 2>&1; echo "exit=$?" >> ../charpente/.superpowers/sdd/2026-09-13-publish-path/t2-dry-run.log
```

Expected in the log: six `Packaging`, six `Verifying`, six `Compiling <crate> v0.1.0`,
no `error`, and a final `exit=0`. Copy the six `Verifying` lines and the exit line into
the task report — they are ledger evidence for spec §10 criterion 1.

- [ ] **Step 4: Commit**

```bash
git add scripts/publish.sh
git commit -m "release: scripts/publish.sh — dirty-tree refusal, full-verify dry run by default, --for-real to publish

Dry run at 0.1.0 exits 0 with all six crates verified (log in the SDD dir).

Claude-Session: https://claude.ai/code/session_012dRc31xomeEgW4QZUYdjPr"
```

---

### Task 3: The templates, the verb, and its refusals

**Files:**
- Create: `crates/charpente-cli/templates/Cargo.toml.tmpl`, `crates/charpente-cli/templates/clippy.toml`, `crates/charpente-cli/templates/gitignore`, `crates/charpente-cli/templates/lib.rs`, `crates/charpente-cli/templates/main.rs.tmpl`, `crates/charpente-cli/templates/sim.rs.tmpl`
- Create: `crates/charpente-cli/src/new.rs`
- Modify: `crates/charpente-cli/src/main.rs:8-14` (mod list) and `:20-40` (dispatch + usage)
- Create: `crates/charpente-cli/tests/new.rs`
- Modify: `CLAUDE.md:62-63` (the `charpente-cli:` test inventory)

**Interfaces:**
- Produces: `charpente new <name>` → exit 0 with `<name>/{Cargo.toml,clippy.toml,.gitignore,src/lib.rs,src/main.rs,tests/sim.rs}`; exit 2 with the refusal texts below. `new::USAGE: &str`. Template Rust that Task 4 compiles; a rendered `Cargo.toml` that Task 5 diffs against GUIDE §1.

- [ ] **Step 1: Write the failing integration tests**

`crates/charpente-cli/tests/new.rs`:

```rust
//! `charpente new` — the scaffold's oracles (spec 2026-09-13-publish-path §7).
//! Refusals by name; six files and nothing else; no placeholder survives.
//! Tasks 4 and 5 add the end-to-end build and the GUIDE byte-diff below.

use std::path::{Path, PathBuf};
use std::process::Command;

// Process spawning and scratch directories are edge tooling here, outside
// the deterministic core (DESIGN.md §1): this file drives the built binary
// as a real subprocess, exactly as a user would.
#[allow(clippy::disallowed_methods)]
fn charpente() -> Command {
    Command::new(env!("CARGO_BIN_EXE_charpente"))
}

/// A fresh, empty scratch directory named for the test, under the OS temp
/// dir, removed first if a previous run left it behind.
#[allow(clippy::disallowed_methods)]
fn scratch(test: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("charpente-new-{test}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[allow(clippy::disallowed_methods)]
fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn run_in(dir: &Path, args: &[&str]) -> (i32, String, String) {
    let out = charpente().current_dir(dir).args(args).output().unwrap();
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn no_name_is_a_usage_error() {
    let dir = scratch("no-name");
    let (code, _, err) = run_in(&dir, &["new"]);
    assert_eq!(code, 2, "stderr: {err}");
    assert!(err.contains("usage: charpente new <name>"), "{err}");
}

#[test]
fn a_name_cargo_would_reject_is_refused_with_the_rule_and_an_example() {
    let dir = scratch("bad-name");
    let (code, _, err) = run_in(&dir, &["new", "123app"]);
    assert_eq!(code, 2, "stderr: {err}");
    assert!(
        err.contains("`123app` is not a crate name — cargo needs `[a-zA-Z][a-zA-Z0-9_-]*`; write `charpente new my-app`"),
        "{err}"
    );
    assert!(!dir.join("123app").exists(), "a refused name must write nothing");
}

#[test]
fn a_rust_keyword_is_refused_because_the_ident_cannot_be_used() {
    let dir = scratch("keyword");
    let (code, _, err) = run_in(&dir, &["new", "match"]);
    assert_eq!(code, 2, "stderr: {err}");
    assert!(
        err.contains("`match` is a Rust keyword, so `use match::…` cannot compile — pick another name, e.g. `charpente new match-app`"),
        "{err}"
    );
}

#[test]
#[allow(clippy::disallowed_methods)]
fn an_existing_path_is_never_written_into() {
    let dir = scratch("exists");
    std::fs::create_dir_all(dir.join("taken")).unwrap();
    std::fs::write(dir.join("taken/keep.txt"), "untouched").unwrap();
    let (code, _, err) = run_in(&dir, &["new", "taken"]);
    assert_eq!(code, 2, "stderr: {err}");
    assert!(
        err.contains("already exists — `charpente new` never writes into an existing directory; pick another name or remove it"),
        "{err}"
    );
    assert_eq!(read(&dir.join("taken/keep.txt")), "untouched");
    assert!(!dir.join("taken/Cargo.toml").exists());
}

const SIX: [&str; 6] = [
    "Cargo.toml",
    "clippy.toml",
    ".gitignore",
    "src/lib.rs",
    "src/main.rs",
    "tests/sim.rs",
];

#[test]
#[allow(clippy::disallowed_methods)] // read_dir: counting what the verb wrote, edge tooling
fn success_writes_exactly_six_files_and_names_them() {
    let dir = scratch("success");
    let (code, out, err) = run_in(&dir, &["new", "my-app"]);
    assert_eq!(code, 0, "stderr: {err}");
    let app = dir.join("my-app");
    for rel in SIX {
        assert!(app.join(rel).is_file(), "missing {rel}");
        assert!(out.contains(rel), "stdout must list {rel}:\n{out}");
    }
    assert!(out.contains("cargo clippy") && out.contains("charpente test --sim"), "{out}");
    // Exactly six: nothing else at the top level or under src/ and tests/.
    let mut found = Vec::new();
    for sub in ["", "src", "tests"] {
        for entry in std::fs::read_dir(app.join(sub)).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                let rel = if sub.is_empty() {
                    entry.file_name().to_string_lossy().into_owned()
                } else {
                    format!("{sub}/{}", entry.file_name().to_string_lossy())
                };
                found.push(rel);
            }
        }
    }
    found.sort();
    let mut expected: Vec<String> = SIX.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(found, expected);
}

#[test]
fn no_placeholder_survives_rendering_and_the_ident_is_underscored() {
    let dir = scratch("render");
    let (code, _, err) = run_in(&dir, &["new", "my-app"]);
    assert_eq!(code, 0, "stderr: {err}");
    let app = dir.join("my-app");
    for rel in SIX {
        let text = read(&app.join(rel));
        assert!(!text.contains("{{"), "{rel} still carries a placeholder:\n{text}");
    }
    assert!(read(&app.join("Cargo.toml")).contains("name = \"my-app\""));
    assert!(read(&app.join("src/main.rs")).contains("my_app::build_app"));
    assert!(read(&app.join("tests/sim.rs")).contains("use my_app::"));
    assert!(read(&app.join("Cargo.toml")).contains("charpente = \"0.1\""));
}
```

- [ ] **Step 2: Run them to see the whole file fail**

```bash
cargo test -p charpente-cli --test new 2>&1 | tee ../charpente/.superpowers/sdd/2026-09-13-publish-path/t3-red.log | grep -E '^test |^test result'
```

Expected: every test FAILED — the binary prints the combined usage and exits 2 for an
unknown verb, so `no_name_is_a_usage_error` may pass by accident on the exit code alone
but fails on `usage: charpente new <name>`; the other five fail on missing files or text.
Note which ones fail and why in the report; a test that is green here is a test that
proves nothing.

- [ ] **Step 3: The six templates**

`crates/charpente-cli/templates/Cargo.toml.tmpl` (this, rendered for `myapp`, must be
byte-identical to the block GUIDE §1 will carry after Task 5 — the placeholders are the
only difference):

```toml
[package]
name = "{{name}}"
version = "0.1.0"
edition = "2024"

[dependencies]
charpente = "{{version}}"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
charpente-sim = "{{version}}"

[[test]]
name = "sim"
harness = false

[lints.clippy]
disallowed_methods = "deny"
disallowed_types = "deny"
```

`crates/charpente-cli/templates/clippy.toml` — every entry is copied verbatim from the
workspace `clippy.toml` (Task 5's subset test diffs them entry by entry):

```toml
# The fence around your app's deterministic core (charpente DESIGN.md §1): no
# ambient time, randomness, filesystem, network, or thread spawning in a
# transaction. All IO goes through the Clock/Entropy/Disk/Net traits; the
# production impls charpente ships are the one sanctioned boundary. This is
# the platform's own fence minus the entries that guard its internals.

disallowed-methods = [
    { path = "std::time::SystemTime::now", reason = "ambient wall clock — use the Clock trait (DESIGN.md §1)" },
    { path = "std::time::Instant::now", reason = "ambient monotonic clock — use the Clock trait (DESIGN.md §1)" },
    { path = "std::thread::sleep", reason = "ambient time — use Clock::sleep (DESIGN.md §1)" },
    { path = "std::thread::spawn", reason = "ambient task spawning — scheduling goes through the deterministic executor (DESIGN.md §1)" },
    { path = "std::thread::scope", reason = "ambient task spawning — scheduling goes through the deterministic executor (DESIGN.md §1)" },
    { path = "std::thread::Scope::spawn", reason = "ambient task spawning — scheduling goes through the deterministic executor (DESIGN.md §1)" },
    { path = "std::fs::read", reason = "ambient filesystem — use the Disk trait (DESIGN.md §1)" },
    { path = "std::fs::read_to_string", reason = "ambient filesystem — use the Disk trait (DESIGN.md §1)" },
    { path = "std::fs::write", reason = "ambient filesystem — use the Disk trait (DESIGN.md §1)" },
    { path = "std::fs::copy", reason = "ambient filesystem — use the Disk trait (DESIGN.md §1)" },
    { path = "std::fs::rename", reason = "ambient filesystem — use the Disk trait (DESIGN.md §1)" },
    { path = "std::fs::remove_file", reason = "ambient filesystem — use the Disk trait (DESIGN.md §1)" },
    { path = "std::fs::create_dir", reason = "ambient filesystem — use the Disk trait (DESIGN.md §1)" },
    { path = "std::fs::create_dir_all", reason = "ambient filesystem — use the Disk trait (DESIGN.md §1)" },
    { path = "std::fs::remove_dir", reason = "ambient filesystem — use the Disk trait (DESIGN.md §1)" },
    { path = "std::fs::remove_dir_all", reason = "ambient filesystem — use the Disk trait (DESIGN.md §1)" },
    { path = "std::fs::read_dir", reason = "ambient filesystem — use the Disk trait (DESIGN.md §1)" },
    { path = "std::fs::metadata", reason = "ambient filesystem — use the Disk trait (DESIGN.md §1)" },
    { path = "std::net::TcpStream::connect", reason = "ambient network — use the Net trait (DESIGN.md §1)" },
    { path = "std::net::TcpListener::bind", reason = "ambient network — use the Net trait (DESIGN.md §1)" },
    { path = "std::net::UdpSocket::bind", reason = "ambient network — use the Net trait (DESIGN.md §1)" },
]

disallowed-types = [
    { path = "std::collections::HashMap", reason = "nondeterministic iteration order and an ambient hash seed — use BTreeMap; #[allow] at the edges only where order provably cannot matter" },
    { path = "std::collections::HashSet", reason = "nondeterministic iteration order and an ambient hash seed — use BTreeSet; #[allow] at the edges only where order provably cannot matter" },
    { path = "std::fs::File", reason = "ambient filesystem — use the Disk trait (DESIGN.md §1)" },
    { path = "std::fs::OpenOptions", reason = "ambient filesystem — use the Disk trait (DESIGN.md §1)" },
    { path = "std::net::TcpStream", reason = "ambient network — use the Net trait (DESIGN.md §1)" },
    { path = "std::net::TcpListener", reason = "ambient network — use the Net trait (DESIGN.md §1)" },
    { path = "std::net::UdpSocket", reason = "ambient network — use the Net trait (DESIGN.md §1)" },
    { path = "std::time::SystemTime", reason = "ambient wall clock — use the Clock trait (DESIGN.md §1)" },
    { path = "std::thread::Builder", reason = "ambient task spawning — scheduling goes through the deterministic executor (DESIGN.md §1)" },
]
```

`crates/charpente-cli/templates/gitignore`:

```
/target
/data
```

`crates/charpente-cli/templates/lib.rs` (verbatim — it never names the crate):

```rust
//! One table, one route, one job, one event. Three effects in one commit,
//! and a sim that proves it under injected faults (`tests/sim.rs`).

use charpente::core::io::{Disk, Entropy};
use charpente::http::{ApiError, App, AppConfig};
use charpente::kernel::JobError;
use charpente::{Id, Schema, Tx, View, Wire, event, job, jobs, route, routes, table};
use serde::{Deserialize, Serialize};
use std::io;

/// A row. `#[table]` makes this a typed table keyed by `id`; its storage
/// shape and its wire shape are separate on purpose (GUIDE §5).
#[table]
pub struct Note {
    pub id: Id<Note>,
    pub body: String,
    pub echoed: bool,
}

/// The POST body. `Wire` lets it cross HTTP (GUIDE §13); serde parses it.
#[derive(Wire, Serialize, Deserialize)]
pub struct NewNote {
    pub body: String,
}

/// The realtime event a subscriber receives when a note lands.
#[event]
pub struct NoteAdded {
    pub note: Id<Note>,
}

/// The job: runs after commit, off the durable queue, and marks the note
/// echoed. Its effect is a row change, so a sim property can pin it.
#[job]
pub struct Echo {
    pub note: Id<Note>,
}

impl Echo {
    pub fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        if let Some(mut note) = tx.get(self.note)? {
            note.echoed = true;
            tx.put(&note);
        }
        Ok(())
    }
}

/// Three effects, one commit: the row, the job and the event all become
/// true together or not at all. There is no way to split them.
#[route(POST "/notes/:id")]
fn add_note(tx: &mut Tx, id: Id<Note>, body: NewNote) -> Result<Id<Note>, ApiError> {
    tx.put(&Note {
        id,
        body: body.body,
        echoed: false,
    });
    tx.enqueue(Echo { note: id });
    tx.emit(NoteAdded { note: id });
    Ok(id)
}

/// The app, in the shape `charpente::launch` and the sim both take.
pub fn build_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> io::Result<App> {
    Ok(App::open(disk, Schema::new().table::<Note>(), entropy, AppConfig::default())?
        .mount(routes![add_note])
        .mount_jobs(jobs![Echo]))
}
```

`crates/charpente-cli/templates/main.rs.tmpl`:

```rust
fn main() -> std::io::Result<()> {
    charpente::launch("0.0.0.0:8080", "./data", {{ident}}::build_app)
}
```

`crates/charpente-cli/templates/sim.rs.tmpl`:

```rust
//! Two properties, both honest: each asserts a claim that holds under ANY
//! fault schedule and gives up — rather than passing — in a universe where
//! injected faults stopped it before the claim could be tested (GUIDE §8).
//!
//! Run them: `charpente test --sim`. Watch one fail: delete the `tx.enqueue`
//! line in `src/lib.rs` and run again.

use charpente::core::job::{JobRow, JobStatus};
use charpente::http::{App, Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente::Id;
use charpente_sim::Sim;
use charpente_sim::checks::{durable_corruption_fired, is_fault_fallout};
use {{ident}}::{NewNote, Note, build_app};

charpente_sim::sim_tests! {
    acked_note_survives_a_restart require ["restart"],
    acked_post_implies_the_echo_after_recovery require ["restart"],
}

const SEC: u64 = 1_000_000_000; // SimSource timestamps are virtual nanoseconds

/// Opens the app on this universe's disk, or gives up on fault fallout.
fn open_app(sim: &mut Sim) -> Result<Option<App>, String> {
    match build_app(Box::new(sim.disk()), Box::new(sim.entropy())) {
        Ok(app) => Ok(Some(app)),
        Err(e) if is_fault_fallout(&e.to_string()) => {
            sim.give_up(format!("App::open hit fault fallout: {e}"));
            Ok(None)
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Runs the production event loop against a scripted source and hands the
/// app back, or gives up on fault fallout.
fn run_kernel(sim: &mut Sim, app: App, source: &mut SimSource) -> Result<Option<App>, String> {
    let mut kernel = match Kernel::new(app, Box::new(sim.clock())) {
        Ok(kernel) => kernel,
        Err(e) if is_fault_fallout(&e.to_string()) => {
            sim.give_up(format!("Kernel::new hit fault fallout: {e}"));
            return Ok(None);
        }
        Err(e) => return Err(e.to_string()),
    };
    if let Err(e) = kernel.run(source) {
        if is_fault_fallout(&e.to_string()) {
            sim.give_up(format!("the kernel loop hit fault fallout: {e}"));
            return Ok(None);
        }
        return Err(e.to_string());
    }
    Ok(Some(kernel.into_app()))
}

/// POST one note, crash, reopen. Returns the reopened app and whether the
/// POST was acked with a 200 — the only case in which there is a claim.
fn post_then_crash(sim: &mut Sim) -> Result<Option<(App, bool)>, String> {
    let Some(app) = open_app(sim)? else {
        return Ok(None);
    };
    let mut source = SimSource::new(Box::new(sim.clock()), 2 * SEC);
    source.push_at(
        0,
        Request::new(Method::Post, "/notes/1").json(&NewNote {
            body: "hello".to_string(),
        }),
    );
    let responses = source.responses();
    let Some(app) = run_kernel(sim, app, &mut source)? else {
        return Ok(None);
    };
    let acked = responses
        .lock()
        .map_err(|_| "responses lock poisoned".to_string())?
        .iter()
        .any(|(status, _)| *status == 200);
    sim.restart();
    drop(app);
    let Some(app) = open_app(sim)? else {
        return Ok(None);
    };
    Ok(Some((app, acked)))
}

/// An acked write is durable: after a crash the row is still there.
fn acked_note_survives_a_restart(sim: &mut Sim) -> Result<(), String> {
    let Some((app, acked)) = post_then_crash(sim)? else {
        return Ok(());
    };
    if !acked {
        sim.give_up("the POST was never acked, so durability has nothing to prove");
        return Ok(());
    }
    let snap = match app.db().snapshot() {
        Ok(snap) => snap,
        Err(e) if is_fault_fallout(&e.to_string()) => {
            sim.give_up(format!("the snapshot hit fault fallout: {e}"));
            return Ok(());
        }
        Err(e) => return Err(e.to_string()),
    };
    match snap.get::<Note>(Id::from_raw(1)) {
        Ok(Some(_)) => Ok(()),
        // Acked-but-lost is excused ONLY where the harness injected durable
        // corruption into this universe (GUIDE §8, "Giving up").
        Ok(None) if durable_corruption_fired(sim) => {
            sim.give_up("durable corruption fired, so the acked row is excused");
            Ok(())
        }
        Ok(None) => Err("acked note 1 lost across a crash".to_string()),
        Err(e) if is_fault_fallout(&e.to_string()) => {
            sim.give_up(format!("the read-back hit fault fallout: {e}"));
            Ok(())
        }
        Err(e) => Err(e.to_string()),
    }
}

/// The end-state oracle: two invariants that hold under any fault schedule.
/// - Ack-durability: a 200 from `/notes/:id` committed its enqueue in the
///   same log record, so an `Echo` job row exists.
/// - Exactly-once: `echoed` is true iff an `Echo` job is Done — a job's
///   effect and its Done mark commit as one record.
fn check_invariants(app: &App, acked: bool) -> Result<(), String> {
    let snap = app.db().snapshot().map_err(|e| e.to_string())?;
    let echo_jobs: Vec<JobRow> = snap
        .scan::<JobRow>()
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter(|job| job.name == "Echo")
        .collect();
    if acked && echo_jobs.is_empty() {
        return Err(
            "ack-durability violated: POST /notes/1 was acked with 200 but no Echo job is durable — \
             the enqueue did not commit with the row"
                .to_string(),
        );
    }
    let done = echo_jobs
        .iter()
        .filter(|job| job.status == JobStatus::Done)
        .count();
    for note in snap.scan::<Note>().map_err(|e| e.to_string())? {
        if note.echoed != (done > 0) {
            return Err(format!(
                "exactly-once violated: note {} reads echoed = {} but {done} Echo job(s) are Done — \
                 a job's effect and its Done mark commit as one record",
                note.id.raw(),
                note.echoed
            ));
        }
    }
    Ok(())
}

/// An acked POST implies the job's effect once the queue has drained —
/// through a crash in between. Delete `tx.enqueue` in `src/lib.rs`, or make
/// `Echo::run` do nothing, and this goes red with a seed.
fn acked_post_implies_the_echo_after_recovery(sim: &mut Sim) -> Result<(), String> {
    let Some((app, acked)) = post_then_crash(sim)? else {
        return Ok(());
    };
    // No requests this time: the loop just runs whatever is due, then idles.
    let mut idle = SimSource::new(Box::new(sim.clock()), 60 * SEC);
    let Some(app) = run_kernel(sim, app, &mut idle)? else {
        return Ok(());
    };
    match check_invariants(&app, acked) {
        Ok(()) => Ok(()),
        // The oracle's own reads go through the same faulted disk.
        Err(msg) if is_fault_fallout(&msg) => {
            sim.give_up(format!("the end-state read hit fault fallout: {msg}"));
            Ok(())
        }
        Err(msg) => Err(msg),
    }
}
```

- [ ] **Step 4: The verb**

`crates/charpente-cli/src/new.rs`:

```rust
//! `charpente new <name>` — the scaffold (spec docs/superpowers/specs/
//! 2026-09-13-publish-path-design.md §4, §5). Six files, three placeholders,
//! no git, no cargo, no prompts. The templates are the `templates/` directory
//! next to `src/`, `include_str!`'d so the binary is self-contained; their
//! oracles are `tests/new.rs`.

use std::path::Path;

pub const USAGE: &str = "usage: charpente new <name>\n       \
       writes <name>/ with Cargo.toml, clippy.toml, .gitignore, src/lib.rs, src/main.rs and \
       tests/sim.rs — one table, one route, one job, one event, and two sim properties";

const CARGO_TOML: &str = include_str!("../templates/Cargo.toml.tmpl");
const CLIPPY_TOML: &str = include_str!("../templates/clippy.toml");
const GITIGNORE: &str = include_str!("../templates/gitignore");
const LIB_RS: &str = include_str!("../templates/lib.rs");
const MAIN_RS: &str = include_str!("../templates/main.rs.tmpl");
const SIM_RS: &str = include_str!("../templates/sim.rs.tmpl");

/// Rust's strict and reserved keywords, plus the crate names cargo itself
/// refuses. The name becomes `use <ident>::…` in two files, so a keyword
/// would fail there rather than here — refuse it where the fix is nameable.
const KEYWORDS: &[&str] = &[
    "abstract", "alloc", "as", "async", "await", "become", "box", "break", "const",
    "continue", "core", "crate", "do", "dyn", "else", "enum", "extern", "false", "final",
    "fn", "for", "gen", "if", "impl", "in", "let", "loop", "macro", "match", "mod", "move",
    "mut", "override", "priv", "proc_macro", "pub", "ref", "return", "self", "Self",
    "static", "std", "struct", "super", "test", "trait", "true", "try", "type", "typeof",
    "unsafe", "unsized", "use", "virtual", "where", "while", "yield",
];

/// `major.minor` of this binary's own version: a new app's requirement is
/// `^0.1`, and the number lives in one place, the workspace manifest.
fn dependency_version() -> String {
    let mut parts = env!("CARGO_PKG_VERSION").splitn(3, '.');
    format!(
        "{}.{}",
        parts.next().unwrap_or("0"),
        parts.next().unwrap_or("0")
    )
}

/// Why `name` is refused, or `None` when cargo would accept it.
fn refuse_name(name: &str) -> Option<String> {
    let mut chars = name.chars();
    let shape_ok = matches!(chars.next(), Some(c) if c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if !shape_ok {
        return Some(format!(
            "`{name}` is not a crate name — cargo needs `[a-zA-Z][a-zA-Z0-9_-]*`; write \
             `charpente new my-app`"
        ));
    }
    let ident = name.replace('-', "_");
    if KEYWORDS.contains(&ident.as_str()) {
        return Some(format!(
            "`{name}` is a Rust keyword, so `use {ident}::…` cannot compile — pick another \
             name, e.g. `charpente new {name}-app`"
        ));
    }
    None
}

/// The six files, as (relative path, contents), rendered for `name`.
fn render(name: &str) -> Vec<(&'static str, String)> {
    let ident = name.replace('-', "_");
    let version = dependency_version();
    let fill = |template: &str| {
        template
            .replace("{{name}}", name)
            .replace("{{ident}}", &ident)
            .replace("{{version}}", &version)
    };
    vec![
        ("Cargo.toml", fill(CARGO_TOML)),
        ("clippy.toml", CLIPPY_TOML.to_string()),
        (".gitignore", GITIGNORE.to_string()),
        ("src/lib.rs", LIB_RS.to_string()),
        ("src/main.rs", fill(MAIN_RS)),
        ("tests/sim.rs", fill(SIM_RS)),
    ]
}

pub fn run(args: &[String]) -> i32 {
    let Some(name) = args.first() else {
        eprintln!("error: `charpente new` needs a name\n\n{USAGE}");
        return 2;
    };
    if let Some(why) = refuse_name(name) {
        eprintln!("error: {why}");
        return 2;
    }
    let root = Path::new(name);
    if root.exists() {
        eprintln!(
            "error: `{}` already exists — `charpente new` never writes into an existing \
             directory; pick another name or remove it",
            root.display()
        );
        return 2;
    }
    for (rel, contents) in render(name) {
        let path = root.join(rel);
        // Writing the scaffold is edge tooling, outside the deterministic
        // core (DESIGN.md §1) — nothing inside charpente-core touches the
        // filesystem directly.
        #[allow(clippy::disallowed_methods)]
        let written = path
            .parent()
            .map_or(Ok(()), std::fs::create_dir_all)
            .and_then(|()| std::fs::write(&path, contents));
        if let Err(e) = written {
            eprintln!(
                "error: could not write `{}`: {e} — the partial `{}/` is left for inspection",
                path.display(),
                root.display()
            );
            return 2;
        }
        println!("  wrote {}", path.display());
    }
    println!(
        "\n`{name}` is ready. Next:\n  cd {name}\n  cargo clippy --all-targets -- -D warnings   \
         # hard rule 1, in your app\n  charpente test --sim                        \
         # seeded fault-injection universes"
    );
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependency_version_is_major_minor_of_this_binary() {
        assert_eq!(dependency_version(), "0.1");
    }

    #[test]
    fn refuse_name_accepts_what_cargo_accepts_and_names_the_rule_otherwise() {
        assert_eq!(refuse_name("myapp"), None);
        assert_eq!(refuse_name("my-app"), None);
        assert_eq!(refuse_name("My_App2"), None);
        assert!(refuse_name("123app").unwrap().contains("[a-zA-Z][a-zA-Z0-9_-]*"));
        assert!(refuse_name("").unwrap().contains("[a-zA-Z][a-zA-Z0-9_-]*"));
        assert!(refuse_name("my app").unwrap().contains("[a-zA-Z][a-zA-Z0-9_-]*"));
        assert!(refuse_name("match").unwrap().contains("Rust keyword"));
        assert!(refuse_name("proc-macro").unwrap().contains("Rust keyword"));
    }

    #[test]
    fn render_fills_every_placeholder_in_every_file() {
        for (rel, contents) in render("my-app") {
            assert!(!contents.contains("{{"), "{rel}:\n{contents}");
        }
    }
}
```

In `crates/charpente-cli/src/main.rs`: add `mod new;` to the module list (alphabetical,
between `mutants` and `restore`), add the dispatch arm

```rust
        (Some("new"), _) => new::run(&args[1..]),
```

after the `restore` arm, and add `new::USAGE` to the fallthrough `eprintln!` (one more
`{}` and one more argument, after `restore::USAGE`). Update the module doc comment's verb
list to include `` `new` (scaffold an app) ``.

- [ ] **Step 5: Run the tests GREEN**

```bash
cargo test -p charpente-cli --test new 2>&1 | grep -E '^test |^test result'
cargo test -p charpente-cli --bin charpente -- new::tests 2>&1 | grep -E '^test |^test result'
```

Expected: 6 ok and 3 ok. Then the whole package, and fmt/clippy on it:

```bash
cargo test -p charpente-cli 2>&1 | grep -E '^test result'
cargo fmt --all --check
cargo clippy -p charpente-cli --all-targets -- -D warnings 2>&1 | tail -3
```

Every `test result:` line `ok`; fmt and clippy clean. The template `.rs`/`.tmpl` files are
not compiled by anything yet — Task 4 does that; if `rustfmt` is run on
`templates/lib.rs` it must not change it (run `rustfmt --check crates/charpente-cli/templates/lib.rs`).

- [ ] **Step 6: The CLAUDE.md inventory**

In `CLAUDE.md`, the test list currently reads (across lines 62-63)
`charpente-cli: cli/ctl/describe/genclient/repro_meta/restore`. Make it
`charpente-cli: cli/ctl/describe/genclient/new/repro_meta/restore`, re-wrapping so no line
exceeds the neighbouring width. Then:

```bash
cargo test -p guide-examples --test doc_inventory 2>&1 | grep -E '^test result'   # ok, 3 passed
```

- [ ] **Step 7: Commit**

```bash
git add crates/charpente-cli/templates crates/charpente-cli/src/new.rs crates/charpente-cli/src/main.rs crates/charpente-cli/tests/new.rs CLAUDE.md
git commit -m "cli: charpente new — six files, three placeholders, four refusals by name

Templates are include_str!'d from crates/charpente-cli/templates/; the
rendered Cargo.toml carries charpente = \"0.1\" from CARGO_PKG_VERSION, the
clippy.toml is the workspace fence's std-library subset, and tests/sim.rs
carries two honest properties in the give-up idiom. tests/new.rs pins the
refusal texts, the six paths, and that no placeholder survives; the
template's Rust is compiled by the next task's end-to-end oracle.

Claude-Session: https://claude.ai/code/session_012dRc31xomeEgW4QZUYdjPr"
```

---

### Task 4: The end-to-end oracle — fmt, clippy under the fence, and the sim

**Files:**
- Modify: `crates/charpente-cli/tests/new.rs` (append)
- Modify (if the compile says so): `crates/charpente-cli/templates/lib.rs`, `crates/charpente-cli/templates/sim.rs.tmpl`

**Interfaces:**
- Consumes: `charpente new` from Task 3; the workspace at `0.1.0` from Task 1 (the `[patch]` path crates must satisfy `^0.1`).
- Produces: a green scaffold. Task 6's evidence steps re-use this test's scratch layout.

- [ ] **Step 1: Append the end-to-end test**

```rust
/// The workspace's target directory, derived from the binary under test so
/// the scaffold's build shares the dependency graph already compiled here
/// (`target/<profile>/charpente` → `target`).
fn target_dir() -> PathBuf {
    Path::new(env!("CARGO_BIN_EXE_charpente"))
        .parent()
        .and_then(Path::parent)
        .expect("the test binary lives at <target>/<profile>/charpente")
        .to_path_buf()
}

fn run_ok(cmd: &mut Command, what: &str) {
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "{what} failed (exit {:?})\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The scaffold, built against THIS workspace rather than crates.io. The
/// Cargo book's own mechanism for an unpublished version: `[patch.crates-io]`
/// satisfies the app's `charpente = "0.1"` with the path crate's 0.1.0
/// whether or not the registry has it yet. The only delta from a stranger's
/// build is the registry itself, which scripts/publish.sh's dry-run covers.
#[test]
#[allow(clippy::disallowed_methods)]
fn a_scaffolded_app_is_fmt_clean_clippy_clean_under_the_fence_and_green_under_the_sim() {
    let dir = scratch("e2e");
    let (code, _, err) = run_in(&dir, &["new", "e2e"]);
    assert_eq!(code, 0, "stderr: {err}");
    let app = dir.join("e2e");

    let ws = std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap();
    let mut manifest = read(&app.join("Cargo.toml"));
    manifest.push_str(&format!(
        "\n[patch.crates-io]\ncharpente = {{ path = {:?} }}\ncharpente-sim = {{ path = {:?} }}\n",
        ws.join("crates/charpente"),
        ws.join("crates/charpente-sim")
    ));
    std::fs::write(app.join("Cargo.toml"), manifest).unwrap();

    let target = target_dir();
    run_ok(
        Command::new("cargo")
            .current_dir(&app)
            .env("CARGO_TARGET_DIR", &target)
            .args(["fmt", "--check"]),
        "cargo fmt --check on the scaffold",
    );
    // `-D warnings` with the emitted clippy.toml in force: the fence is live
    // against the template's own code, and any unused import fails here.
    run_ok(
        Command::new("cargo")
            .current_dir(&app)
            .env("CARGO_TARGET_DIR", &target)
            .args(["clippy", "--all-targets", "--", "-D", "warnings"]),
        "cargo clippy under the scaffold's fence",
    );
    run_ok(
        charpente()
            .current_dir(&app)
            .env("CARGO_TARGET_DIR", &target)
            .args(["test", "--sim", "--universes", "8"]),
        "charpente test --sim on the scaffold",
    );
}
```

- [ ] **Step 2: Run it and fix the template until it is green**

```bash
cargo test -p charpente-cli --test new -- a_scaffolded_app 2>&1 | tee ../charpente/.superpowers/sdd/2026-09-13-publish-path/t4-first.log | tail -40
```

The first run compiles the scaffold against the workspace and is the template's first
compile ever. Expected shapes of RED and the fix for each, in the order they appear:

- `cargo fmt --check` diff → run `rustfmt` on the rendered file in the scratch dir, copy
  the formatting back into `templates/lib.rs` or `templates/sim.rs.tmpl` (keeping the
  `{{ident}}` placeholder).
- `unused import` under `-D warnings` → remove it from the template.
- A trait method not in scope (`get`/`put`/`scan` need `View`; `raw()` on `Id`) → add the
  import the compiler names.
- `no method named json` / `push_at` signature → the GUIDE §8 "Driving the whole app"
  example is the reference; match it.
- A sim failure with a seed → replay it with
  `CHARPENTE_SEED=<seed> charpente test --sim --dir <scratch>/e2e` and read the message.
  A failure in `acked_post_implies_the_echo_after_recovery` whose text is
  `exactly-once violated` under a fault schedule means the property's assumption is wrong,
  not the platform — stop, write the seed and the config into `decisions-pending.md`, and
  do not weaken the property to pass.

Every template edit is followed by a re-run. Record each RED→fix pair in the report.

- [ ] **Step 3: Whole file green, then the package**

```bash
cargo test -p charpente-cli --test new 2>&1 | grep -E '^test |^test result'    # 7 ok
cargo test -p charpente-cli 2>&1 | grep -E '^test result'                       # every line ok
cargo fmt --all --check && cargo clippy -p charpente-cli --all-targets -- -D warnings 2>&1 | tail -2
```

- [ ] **Step 4: Commit**

```bash
git add crates/charpente-cli/tests/new.rs crates/charpente-cli/templates
git commit -m "cli(new): the end-to-end oracle — the scaffold is fmt-clean, clippy-clean under its own fence, and green under the sim

Scaffolds into a scratch dir, patches charpente/charpente-sim to the
workspace (the Cargo book's unpublished-version mechanism), shares the
workspace target dir, and runs fmt --check, clippy -D warnings, and
charpente test --sim --universes 8. Template fixes from its first compile
are in the task report.

Claude-Session: https://claude.ai/code/session_012dRc31xomeEgW4QZUYdjPr"
```

---

### Task 5: GUIDE §1, the byte-diff and subset oracles, README, PLAN, DESIGN

**Files:**
- Modify: `docs/GUIDE.md:25-83` (§1)
- Modify: `crates/charpente-cli/tests/new.rs` (append two tests)
- Modify: `README.md` (a "Try it" block before the doc table, line ~132)
- Modify: `PLAN.md` (M3 sub-project 1 parenthetical; sub-project 5's install line)
- Modify: `DESIGN.md` (§7, after the TypeScript-client bullet at line ~1346)

**Interfaces:**
- Consumes: the rendered `Cargo.toml` from Task 3 (its exact text is the block below).

- [ ] **Step 1: Write the two failing tests**

Append to `crates/charpente-cli/tests/new.rs`:

```rust
/// The first ```toml block after GUIDE §1's heading, verbatim.
fn guide_section_1_cargo_toml() -> String {
    let guide = read(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/GUIDE.md"));
    let section = guide
        .split("## 1. Project setup")
        .nth(1)
        .expect("docs/GUIDE.md has a `## 1. Project setup` heading");
    let block = section
        .split("```toml\n")
        .nth(1)
        .expect("GUIDE §1 has a ```toml block");
    block
        .split("```")
        .next()
        .expect("the ```toml block is closed")
        .to_string()
}

/// The doc is the spec: the file the verb writes and the file the guide
/// shows are one file, held together here (the describe_contract pattern).
#[test]
fn the_rendered_cargo_toml_is_guide_section_1s_block_byte_for_byte() {
    let dir = scratch("guide-diff");
    let (code, _, err) = run_in(&dir, &["new", "myapp"]);
    assert_eq!(code, 0, "stderr: {err}");
    let rendered = read(&dir.join("myapp/Cargo.toml"));
    let documented = guide_section_1_cargo_toml();
    assert_eq!(
        rendered, documented,
        "\n`charpente new myapp` writes a Cargo.toml that is not the block GUIDE §1 shows — \
         the doc is the spec (hard rule 6); update whichever one drifted.\n"
    );
}

/// Every entry in the emitted clippy.toml is an entry in the workspace's,
/// character for character — the app's fence is a subset of the platform's,
/// never a paraphrase of it.
#[test]
fn the_emitted_clippy_toml_is_a_subset_of_the_workspace_fence() {
    let dir = scratch("clippy-subset");
    let (code, _, err) = run_in(&dir, &["new", "myapp"]);
    assert_eq!(code, 0, "stderr: {err}");
    let emitted = read(&dir.join("myapp/clippy.toml"));
    let workspace = read(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../clippy.toml"));
    let entries: Vec<&str> = emitted
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("{ path = "))
        .collect();
    assert!(entries.len() >= 20, "the emitted fence lost its entries: {}", entries.len());
    for entry in entries {
        assert!(
            workspace.contains(entry),
            "emitted clippy.toml carries an entry the workspace fence does not:\n{entry}"
        );
    }
}
```

- [ ] **Step 2: Run them RED**

```bash
cargo test -p charpente-cli --test new -- guide_section clippy_toml 2>&1 | grep -E '^test |^test result|panicked'
```

Expected: the byte-diff test FAILS (GUIDE §1 still shows the old block with
`charpente = "0.0.0"` and no `serde`); the subset test PASSES already — that is fine: it
guards Task 3's copy against future paraphrase, and its RED case is shown by hand in
Step 4.

- [ ] **Step 3: Rewrite GUIDE §1**

Replace everything from `## 1. Project setup` up to (not including) `### The data directory`
with:

````markdown
## 1. Project setup

Install the CLI once. The crate is `charpente-cli`; the binary it installs is `charpente`:

```text
cargo install charpente-cli
```

Then scaffold an app and run its two gates:

```text
charpente new myapp
cd myapp
cargo clippy --all-targets -- -D warnings   # hard rule 1, in your app
charpente test --sim                        # seeded fault-injection universes
```

`charpente new` writes six files and does nothing else — no `git init`, no build, no
prompts. It refuses a name cargo would reject, a Rust keyword, and an existing path, each
by name.

| File | What it is |
|---|---|
| `Cargo.toml` | the manifest below, byte for byte |
| `clippy.toml` | the fence: the standard-library calls a transaction must never make |
| `.gitignore` | `/target` and `/data` |
| `src/lib.rs` | one table, one route, one job, one event, and `build_app` |
| `src/main.rs` | `charpente::launch(...)` — one line (§13) |
| `tests/sim.rs` | two properties in the give-up idiom (§8) |

### `Cargo.toml`

A charpente app is a Rust crate that links against the `charpente` platform
crate and compiles into one binary. Deployment is one binary and one data
directory — no database server, queue service, or config store to
provision. Two dependencies: `charpente`, and `serde` for the request and
response types you define — `charpente` re-exports serde, but
`#[derive(Deserialize)]` looks the crate up by name in your own dependency
list (§5). This is the file `charpente new myapp` writes; a test holds the
two together:

```toml
[package]
name = "myapp"
version = "0.1.0"
edition = "2024"

[dependencies]
charpente = "0.1"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
charpente-sim = "0.1"

[[test]]
name = "sim"
harness = false

[lints.clippy]
disallowed_methods = "deny"
disallowed_types = "deny"
```

`charpente-sim` is dev-only: it drives your app's tables, routes, jobs, and
time through a deterministic simulator with seeded fault injection, and
never links into the shipped binary. The `[[test]] name = "sim", harness =
false` block is required for `charpente test --sim` (§8): it tells Cargo to
build a `tests/sim.rs` binary without the default libtest harness, so the
sim runner supplies its own `main` instead.

### The fence

`clippy.toml` names the standard-library calls a transaction must never
make — `SystemTime::now`, `Instant::now`, `thread::spawn`, every `std::fs`
function, `TcpStream::connect`, `HashMap` — with the trait to use instead.
The `[lints.clippy]` lines above turn each into an error, so a handler that
reaches for the wall clock is refused at `cargo clippy` by name:

```text
error: use of a disallowed method `std::time::SystemTime::now`
  --> src/lib.rs:51:17
   |
   = note: ambient wall clock — use the Clock trait (DESIGN.md §1)
```

It is the platform's own fence (its CLAUDE.md hard rule 1) minus the entries
that guard platform internals, and a test keeps it a subset. Where a real
edge needs ambient IO — the production `Disk` your `main` mounts — the
sanctioned form is `#[allow(clippy::disallowed_methods)]` on that one
statement with a rationale comment, never at file level.

````

Keep `### The data directory` and everything after it in §1 unchanged.

- [ ] **Step 4: Run GREEN, and show the subset test's RED by hand**

```bash
cargo test -p charpente-cli --test new 2>&1 | grep -E '^test |^test result'   # 9 ok
```

Then, without committing: change one word inside a `reason = "…"` string in
`crates/charpente-cli/templates/clippy.toml` (e.g. `wall clock` → `wallclock` on the
first entry), rebuild, run the subset test, watch it FAIL naming that entry, then
`git checkout -- crates/charpente-cli/templates/clippy.toml` and confirm
`git status --short` shows only the intended files. Paste the failure line into the
report.

- [ ] **Step 5: The doc-truth gate and the GUIDE build**

GUIDE §1 has no ```rust block in the rewritten portion and touches no pinned block, but
prove it rather than assume it:

```bash
cargo build -p guide-examples 2>&1 | tail -2
cargo test -p guide-examples 2>&1 | grep -E '^test result'    # every line ok (doc_truth, doc_inventory, describe_contract, hook_census, mutants_exclusions)
```

- [ ] **Step 6: README, PLAN, DESIGN**

`README.md` — insert before the `| Doc | What it is |` table:

````markdown
## Try it

```text
cargo install charpente-cli
charpente new myapp && cd myapp
cargo clippy --all-targets -- -D warnings   # hard rule 1, in your app
charpente test --sim                        # seeded fault-injection universes
```

Then delete the `tx.enqueue` line in `src/lib.rs` and run the sim again: the second
property goes red with a seed, and the seed replays. That is the whole pitch.

````

`PLAN.md`, M3 sub-project 1: replace
`(verify the 2026-07-01 name reservation under "Naming" below actually happened — nothing in the tree records it)`
with `(five names reserved 2026-07-01, confirmed 2026-09-13; `charpente-tsgen` publishes fresh)`,
and in the same item replace `GUIDE §1's `charpente = "0.0.0"` dependency line becomes true.`
with `GUIDE §1's dependency line is `charpente = "0.1"`, and a test holds the guide's
`Cargo.toml` block byte-identical to the verb's output.` In sub-project 5, replace
`cargo install charpente` with `cargo install charpente-cli`. In "Naming", append one
sentence: `Published at 0.1.0 — see the 2026-09-13 ledger entries.` (Task 6 makes that true.)

`DESIGN.md` §7 — after the "generated TypeScript client" bullet, add:

```markdown
- **`charpente new` (v0, implemented; `crates/charpente-cli/src/new.rs`).** The scaffold
  a stranger starts from: six files — the GUIDE §1 manifest byte for byte, hard rule 1's
  standard-library fence as the app's own `clippy.toml`, one table/route/job/event, a
  one-line `launch` main, and two sim properties in the give-up idiom, the second of
  which is DEMO.md's outbox oracle. Its templates are compiled, linted under their own
  fence and run under the sim on every push (`tests/new.rs`), and refusals name the rule
  and the fix (docs/GUIDE.md §1).
```

- [ ] **Step 7: fmt/clippy/inventory, then commit**

```bash
cargo fmt --all --check
cargo test -p guide-examples 2>&1 | grep -E '^test result'
git add docs/GUIDE.md crates/charpente-cli/tests/new.rs README.md PLAN.md DESIGN.md
git commit -m "docs(new): GUIDE §1 around the scaffold; the guide's Cargo.toml block is the verb's expected output

Two oracles: the rendered Cargo.toml byte-equals GUIDE §1's block (read at
test time), and the emitted clippy.toml is an entry-for-entry subset of the
workspace fence (watched RED under a one-word paraphrase). GUIDE §1 now says
two dependencies and why. README gets a Try-it block; PLAN's install line is
charpente-cli; DESIGN §7 inventories the verb.

Claude-Session: https://claude.ai/code/session_012dRc31xomeEgW4QZUYdjPr"
```

---

### Task 6: Evidence for the exit criteria, the ledger, and the merge

**Files:**
- Modify: `docs/LEDGER.md` (append)
- Modify: `.superpowers/sdd/2026-09-13-publish-path/progress.md` (main tree; close-out)

**Interfaces:**
- Consumes: everything above. Produces nothing new in code.

- [ ] **Step 1: The two REDs a stranger can produce (spec §10 item 5)**

Scaffold by hand and patch as the e2e does:

```bash
cd /tmp && rm -rf charpente-evidence && mkdir charpente-evidence && cd charpente-evidence
"$CARGO_TARGET_DIR/debug/charpente" new demo && cd demo
WS=/Users/noahmitchem/Documents/GitHub/charpente-publish-path
printf '\n[patch.crates-io]\ncharpente = { path = "%s/crates/charpente" }\ncharpente-sim = { path = "%s/crates/charpente-sim" }\n' "$WS" "$WS" >> Cargo.toml
"$CARGO_TARGET_DIR/debug/charpente" test --sim --universes 64 2>&1 | tail -3     # green; note the master seed
```

**RED 1 — the deleted enqueue.** Delete the line `    tx.enqueue(Echo { note: id });` from
`src/lib.rs` (`grep -c 'tx.enqueue' src/lib.rs` must go 1 → 0). Run:

```bash
"$CARGO_TARGET_DIR/debug/charpente" test --sim --universes 64 --filter acked_post_implies_the_echo_after_recovery 2>&1 | tee /Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-09-13-publish-path/t6-red-enqueue.log | grep -E 'sim failure|seed:|repro:' | head -6
```

Expected: `sim failure in acked_post_implies_the_echo_after_recovery: ack-durability
violated: POST /notes/1 was acked with 200 but no Echo job is durable …`, a `seed:` line
and a `repro:` line. Replay the seed with the `repro:` command verbatim and confirm the
same message. Restore the line (re-scaffold or re-add it) and confirm green.

**RED 2 — the neutered job.** Replace `Echo::run`'s body with `Ok(())` (the exact shape
cargo-mutants applies). Run the same command with the same filter. Expected: `sim failure
… exactly-once violated: note 1 reads echoed = false but 1 Echo job(s) are Done …`, with
a seed and a repro line. Replay; restore; green.

**The instrument.** cargo-mutants 27.1.0 is installed on this machine. Against the
restored scaffold:

```bash
"$CARGO_TARGET_DIR/debug/charpente" test --mutants --dir /tmp/charpente-evidence/demo --re 'Echo::run' 2>&1 | tee /Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-09-13-publish-path/t6-mutants.log | tail -12
```

Expected: the `Echo::run` → `Ok(())` mutant reported **caught**, verb exit 0. If the verb
refuses the scratch directory (its `--dir` handling was written for workspace members;
the ledger records two minor `--dir` defects), record the exact refusal in
`decisions-pending.md` — RED 2 above already proves the property, and the instrument's
`--dir` behaviour on an external app becomes a finding for the ledger, not a blocker.

- [ ] **Step 2: The whole gate, counted**

Predict **86** (`85` + `tests/new.rs`).

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente-publish-path
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -2
cargo test --workspace --no-fail-fast 2>&1 | tee ../charpente/.superpowers/sdd/2026-09-13-publish-path/t6-suite.log > /dev/null
grep -c '^test result: ok' ../charpente/.superpowers/sdd/2026-09-13-publish-path/t6-suite.log   # 86
grep -c 'FAILED' ../charpente/.superpowers/sdd/2026-09-13-publish-path/t6-suite.log            # 0
cargo publish --workspace --dry-run --no-verify 2>&1 | tail -3; echo "exit=${pipestatus[1]}"    # 0
```

- [ ] **Step 3: The ledger entry**

Append to `docs/LEDGER.md` an entry headed
`## 2026-09-13 — publish path: 0.1.0 is publishable, and `charpente new` is the app the guide describes`
covering, in the ledger's voice: the tsgen finding and the `--workspace`-skips-`publish =
false` reading from Task 1 (with the exact error text); the six `Verifying` lines from
Task 2's dry-run; the serde finding (GUIDE §1 vs §5) and its fix; every template RED→fix
pair from Task 4's first compile; the byte-diff and subset oracles and the subset test's
hand-watched RED; RED 1 and RED 2 from Step 1 with their seeds and the exact failure
text; the `--mutants` result or refusal; the counted gate (86 / 0 / dry-run exit 0). End
with what stays open: the real publish (`scripts/publish.sh --for-real`, Noah's hand),
the post-publish run against the registry that spec §10 item 2 asks for, and the
`Wire`-derive/serde question refused in spec §9.

- [ ] **Step 4: Commit, then hand off the merge**

```bash
git add docs/LEDGER.md
git commit -m "ledger: publish path — tsgen was the missing crate, the scaffold is the guide, and both REDs have seeds

Claude-Session: https://claude.ai/code/session_012dRc31xomeEgW4QZUYdjPr"
```

Write the close-out to
`/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-09-13-publish-path/progress.md`:
the branch tip sha, the gate counts, the two seeds, and the three open items. **Do not
merge, publish, or push** — merging `publish-path` into `main` and running
`scripts/publish.sh --for-real` are Noah's calls. Removing the worktree and
`/tmp/charpente-publish-path-target` waits until after the merge.
