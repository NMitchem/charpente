# Autonomous Subagent Run Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute a long unattended window of charpente work — land one ruled decision, fix two open defects, collect one stalled experiment's baselines, and run a mutation sweep — with every phase boundary verified by the controller rather than reported by an agent.

**Architecture:** Phase-gated hybrid. Sequential between tasks, parallel within a task where sub-units are genuinely independent. Task 1 commits to `main`. Tasks 2 and 3 each get their own worktree, their own branch and their own `CARGO_TARGET_DIR`, and are left unmerged for Noah. Task 4 collects data only. Task 5 is the graceful tail and may be cut off at any point.

**Tech Stack:** Rust workspace (`charpente`, `charpente-core`, `charpente-sim`, `charpente-cli`, `guide-examples`), `redb` behind the `Disk` trait, `cargo-mutants` 27.1.0, zsh on macOS.

**Spec:** `docs/superpowers/specs/2026-09-09-autonomous-run-design.md`

## Global Constraints

Every task's requirements implicitly include this section. These are not style notes; each is a failure this repo has already paid for.

- **Verification is foregrounded.** Never background `cargo test` inside a subagent — an agent that backgrounds it can park forever after the results arrive.
- **`--no-fail-fast` is load-bearing.** Without it cargo stops at the first failing package and every later suite silently never runs. Confirm green by COUNTING: `2>&1 | tee LOG`, then `grep -c "^test result: ok" LOG`. Never conclude green from the absence of `FAILED`.
- **State the expected count BEFORE running it.** A `grep` matching nothing exits 1, so under `set -o pipefail` "nothing failed" and "the check itself broke" produce the same status.
- **This shell is zsh.** `${PIPESTATUS[0]}` expands to the empty string in every case; use `${pipestatus[1]}` (lowercase, 1-indexed) or `set -o pipefail` and test `$?`. zsh does not word-split, so an unquoted `$spec` stays ONE argument. zsh DOES glob, so `--include=*.rs` must be quoted as `--include="*.rs"`.
- **Pipe cargo through `tee`, never `tail`.** `tail` eats the aggregate counts and replaces cargo's exit status with its own.
- **Never share `CARGO_TARGET_DIR` across worktrees.** A mutation proof's artifact leaks into the main tree with `git diff` clean.
- **`timeout`/`gtimeout` are both absent on this machine.** A bare `timeout` exits 127 and that reads as a result. Use a perl alarm with a pristine control.
- **Grepping logs for `panicked` is not a failure detector in this repo.** It has caught two agents already — it is the very defect Task 2 fixes. Key on process exit code and `test result: FAILED`.
- **`CHARPENTE_SEED` replays ONE universe, not the batch.** Every coverage requirement is skipped, `floor:` as well as `require:`. It is never evidence that a rate or floor holds.
- **Commit named paths only**, never `git add -A`; another session may be writing in this tree.
- **Any claim about a commit is checked with `git show <sha>:<path>`**, never the working tree.
- **Dispatches are unnamed**, and every agent writes its result to a FILE before returning. The delivery hop is measured at n=4 as the least reliable part of a dispatch, and it fails *after* the expensive work is done.
- **Blocked work** goes to `.superpowers/sdd/2026-09-09-autonomous-run/decisions-pending.md` with the reading taken, the reasoning, and what would falsify it. Anything contradicting `DECISIONS.md` stops that task dead instead.
- **Hard rule 3 binds:** a bug without a failing test observed RED first has not been reproduced.
- **No Co-Authored-By omission:** commits end with the two attribution lines used by the last several commits on `main` (`Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>` and the `Claude-Session:` line).

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `PLAN.md` | M2 bullet list and the risks table — both carry the dylib item | 1 |
| `DESIGN.md` | §9 open question 5 — the dylib's home | 1 |
| `docs/LEDGER.md` | The dated record of the decision | 1 |
| `crates/charpente-core/src/table.rs` | `Store::open`'s redb fence; the new thread-local and its accessor | 2 |
| `crates/charpente-sim/src/harness.rs` | Installs the filtering panic hook (the one sanctioned thread-spawning site) | 2 |
| `crates/charpente/tests/transactions.rs` | Existing deterministic fence regressions; gains the output oracle | 2 |
| `crates/charpente-core/src/table.rs` | `scan_raw_shape_error` and the twelve `rows_def(` open sites | 3 |
| `crates/charpente-core/tests/engine.rs` | Home of the scope-flip contract tests | 3 |
| `.superpowers/sdd/2026-08-14-agent-skills/` | Archive for T3's unscored baselines | 4 |
| `.superpowers/sdd/2026-09-09-autonomous-run/report.md` | The run's report, in the MAIN tree so it survives the worktrees | all |

---

### Task 1: Land the dylib decision

Noah ruled 2026-09-09 to **drop** the prebuilt-platform-dylib item rather than re-scope it. Controller runs this directly on `main`; it is doc-only and no Rust source is touched.

**Files:**
- Modify: `PLAN.md:184` (M2 bullet), `PLAN.md:194` (risk row, Mitigation column)
- Modify: `DESIGN.md:1561-1574` (§9 item 5)
- Modify: `docs/LEDGER.md` (append a dated entry)

**Interfaces:**
- Consumes: nothing.
- Produces: a clean `main` at a known sha, which Task 5's mutation baseline requires.

- [ ] **Step 1: Re-check repo state before touching anything**

```bash
git status --porcelain && git rev-parse --short HEAD && git log --oneline -1
```

Expected: empty status, HEAD `595063d` or later. If another session has moved HEAD, read what changed before proceeding.

- [ ] **Step 2: Rewrite the `PLAN.md` M2 bullet**

Replace the bullet at `PLAN.md:184-185`:

```markdown
- **Compile-latency work:** platform-as-prebuilt-dylib; optionally cranelift hot reload for
  handlers if it proves out.
```

with:

```markdown
- **Compile-latency work: DROPPED 2026-09-09**, on the reading in docs/BENCH.md
  "Compile latency (2026-09-09)". The prebuilt-platform-dylib item was justified on the
  inner loop, and the inner loop is not where the cost is: its stated mechanism — "app
  iteration recompiles app code only" — is already delivered by cargo's fingerprinting,
  proven by 9 of 9 incremental observations printing exactly one `Compiling` line, every
  one of them `invoicing`. Its residual headroom is the once-per-checkout clean build,
  which is a CI-checkout cost and not this item's justification. Cranelift hot reload
  stays an open question in DESIGN §9.1, unchanged and unpromoted.
```

- [ ] **Step 3: Rewrite the risk row's Mitigation column**

In the `PLAN.md:194` row, replace the leading `Prebuilt platform dylib; cranelift hot reload later; toolchains are improving underneath us.` with:

```
Prebuilt platform dylib **dropped 2026-09-09** — see the M2 list above; cranelift hot reload remains an open question, not a scheduled mitigation; toolchains are improving underneath us.
```

Leave the rest of that cell (the "did not happen in M0" history and the BENCH.md pointer) byte-identical. The Read column is already correct and is not edited.

- [ ] **Step 4: Resolve DESIGN §9 item 5**

Replace the final sentence at `DESIGN.md:1574`:

```markdown
no mitigation on its own. The prebuilt-dylib item itself stays open.
```

with:

```markdown
no mitigation on its own. **The prebuilt-dylib item is dropped (Noah, 2026-09-09).** Its
stated mechanism — "app iteration recompiles app code only" — is already what cargo does:
every incremental observation in that run rebuilt exactly one crate, the app's. What a
dylib could still reach is the clean build, which is a CI-checkout cost rather than the
dev-loop risk this item was opened against. Reopening it on inner-loop grounds needs the
decomposition that reading does not have: split the 436 ms into macro expansion,
typecheck, codegen and link, and show link dominant.
```

- [ ] **Step 5: Append the LEDGER entry**

Append to `docs/LEDGER.md` a section headed `## 2026-09-09 — the prebuilt dylib is dropped`. It must state: the ruling and that it is Noah's; the two grounds (mechanism already delivered by cargo; residual headroom is the clean build); the size split (the "app code only" half is size-independent because it is cargo fingerprinting, while the 436 ms is size-dependent and grows in the app crate, which a dylib does not compile); the DECISIONS check (no contradiction; §9's "the platform is the app's binary" is an adjacency that a separately-shipped dylib would have weakened, and dropping the item retires that thread); and what the decision does NOT settle (one laptop, one small app, `dev`, n=3, no decomposition).

Include the bound, labeled as reasoning rather than measurement:

```markdown
**An upper bound on what a dylib could have bought the inner loop, from published
figures rather than new measurement.** Link is serial; the handler-body build sustained
4.19–4.23x parallelism over 436.1 ms at 1.838 s of compiler CPU. Treating the build as a
parallel region plus a serial link of duration `s`, `C <= P(w-s) + s` gives `s <= 309 ms`
at a theoretical 12-core ceiling and `s <= 187 ms` at the 6.62x this run actually
observed in its clean builds. This is arithmetic over BENCH.md's table, **not a
measurement**, and BENCH.md is right that its table sizes no mitigation — an upper bound
can only flatter the dylib, which is why it is usable here: even granting the generous
ceiling, and even pretending a dylib eliminates link time rather than changing it, the
inner loop lands near 250 ms instead of 436 ms. That is not an adoption risk being
retired. It is not a quotable number and does not belong in BENCH.md.
```

- [ ] **Step 6: Run the gate**

The doc gate is what `docs/GUIDE.md`'s extracted examples compile against; no Rust source changed, so per the blast-radius rule the full sweep is not owed.

Expected before running: exit 0, and **4** lines matching `^test result: ok` (the count `36f4680` recorded for this same gate).

```bash
cargo fmt --all --check
cargo test -p guide-examples --no-fail-fast 2>&1 | tee /tmp/t1-gate.log
echo "exit=${pipestatus[1]}"
grep -c "^test result: ok" /tmp/t1-gate.log
grep -c "FAILED" /tmp/t1-gate.log || echo "0 FAILED"
```

If the count is not 4, stop and reconcile the delta before committing. An unreconciled count delta is a finding, not a rounding error.

- [ ] **Step 7: Commit named paths only and push**

```bash
git add PLAN.md DESIGN.md docs/LEDGER.md
git status --porcelain
git commit -F /tmp/t1-msg.txt
git log --oneline -1
git push origin main
git rev-parse --short HEAD origin/main
```

The last command must print the same sha twice. Write the message to `/tmp/t1-msg.txt` first; it states the ruling, the two grounds, the gate counts, and ends with the two attribution lines.

---

### Task 2: F1 — the redb open-fence false alarm

**The defect.** `Store::open`'s `catch_unwind` fence (added `1c439d5`) converts redb's open-time assert into an open failure, but the **default panic hook has already printed** `panicked at ...page_manager.rs` by the time the panic is caught. 134 such lines appeared on a fully green 10k-universe batch. A docs-only agent read them as failure and discarded a correct full-App-stack property; a later hunt's own controller was caught by the same trace. DESIGN §7 and hard rule 5 make the fast, precise *no* the product — this is its inverse, a false alarm that cost a competent agent good work.

**The design, pre-registered here rather than left to the implementer.** The obvious fix — swap the hook with `std::panic::set_hook` around the fence — is **wrong and must not be used**. `set_hook` is process-global, and sim universes run on parallel worker threads (`crates/charpente-sim/src/harness.rs:343`, `std::thread::scope`). Swapping the hook around this fence would silence a *different* worker's genuine panic for the duration, trading a benign false alarm for a suppressed real one. Instead: a thread-local flag set around the fence, and one process-wide hook installed once that consults it.

The replacement line deliberately contains **no `panicked` substring**, because a log grep for that word is exactly what this defect trains agents to misuse.

**Files:**
- Modify: `crates/charpente-core/src/table.rs:757-796` (`Store::open` and its new thread-local)
- Modify: `crates/charpente-sim/src/harness.rs` (hook installation)
- Create: `crates/edge-harness/tests/open_fence.rs` (the end-to-end output oracle)
- Modify: `CLAUDE.md` (add `open_fence` to the exhaustive `edge-harness` suite list)
- Test (mechanism only, no child process): `crates/charpente-core/tests/engine.rs`
- Read: `crates/charpente/tests/transactions.rs:1282` and `:1433` — the two existing
  deterministic fence regressions the child process drives. **Not modified.**

> **Controller ruling (pre-flight).** The output oracle does NOT live in
> `crates/charpente/tests/transactions.rs`. It calls `std::process::Command::new`, which is
> on the clippy deny-list (`clippy.toml`: "ambient process spawning — edge tooling only
> (charpente-cli); nothing inside the deterministic boundary spawns processes"), and clippy
> runs `--workspace --all-targets` as a Global Constraint of this plan — the test would fail
> its own gate. It lands in `crates/edge-harness/tests/open_fence.rs` instead, the crate
> CLAUDE.md calls the "black-box wire rig outside the deterministic boundary", using the
> file-level allow that crate already uses at `tests/failover.rs:38`. Do NOT put a
> file-level allow on `transactions.rs` — that would disable the fence across a large
> existing suite to spare one test a move.

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `charpente_core::table::in_open_fence() -> bool` — public, consumed by the sim's hook. `charpente_sim::harness::install_open_fence_hook()` — public, idempotent.

- [ ] **Step 1: Create the worktree and prove the target dir is separate**

```bash
git worktree add -b f1-open-fence ../charpente-f1 main
export CARGO_TARGET_DIR=/tmp/charpente-f1-target
mkdir -p "$CARGO_TARGET_DIR"
cd ../charpente-f1 && git log --oneline -1 && echo "target=$CARGO_TARGET_DIR"
```

`CARGO_TARGET_DIR` must be outside both the main repo and this worktree.

- [ ] **Step 2: Write the failing test — observe the trace RED**

Create `crates/edge-harness/tests/open_fence.rs`. The two existing regressions in
`crates/charpente/tests/transactions.rs` (`:1282`, `:1433`) already drive the fence
deterministically; this runs one as a child and asserts on what reaches its stderr.
The file opens with the allow that `edge-harness` already uses crate-wide:

```rust
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
```

```rust
/// F1: the fence catches redb's open-time assert, but the DEFAULT panic hook
/// prints `panicked at ...page_manager.rs` before the catch. 134 such lines on
/// a fully green 10k-universe batch taught a competent agent to read green as
/// red, and later caught a hunt's own controller. Hard rule 5 makes the precise
/// *no* the product; a false alarm is its inverse.
///
/// The child process is the oracle: a panic hook's output cannot be captured
/// in-process without installing a hook, which is the thing under test.
#[test]
fn open_fence_does_not_print_a_raw_redb_panic_trace() {
    let out = std::process::Command::new(env!("CARGO"))
        .args([
            "test",
            "-p",
            "charpente",
            "--test",
            "transactions",
            "--",
            "unopenable_store_with_compacted_wal_is_a_named_hard_error",
            "--nocapture",
            "--exact",
        ])
        .env("CHARPENTE_F1_CHILD", "1")
        .output()
        .expect("child cargo test runs");

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    assert!(
        out.status.success(),
        "the fence regression itself must pass; this test asserts only on its output:\n{combined}"
    );
    assert!(
        !combined.contains("page_manager.rs"),
        "raw redb panic trace reached the log — this is F1, the false alarm that \
         cost an agent a correct property:\n{combined}"
    );
    assert!(
        !combined.contains("panicked at"),
        "a `panicked at` line reached the log from a benign fenced open:\n{combined}"
    );
}
```

The child runs a test in a DIFFERENT crate (`-p charpente`), so it cannot re-enter this
test and no recursion guard is needed. Keep the `CHARPENTE_F1_CHILD` env var anyway as a
marker the child's own output can be attributed to.

- [ ] **Step 3: Run it and confirm it fails for the stated reason**

```bash
cd ../charpente-f1 && cargo test -p edge-harness --test open_fence \
  open_fence_does_not_print_a_raw_redb_panic_trace -- --nocapture 2>&1 | tee /tmp/f1-red.log
echo "exit=${pipestatus[1]}"
```

Expected: **FAIL**, on the `page_manager.rs` assertion, with the trace quoted in the panic message. If it passes, STOP — the defect is not reproduced here and hard rule 3 forbids proceeding. Record the negative result in `decisions-pending.md`.

- [ ] **Step 4: Add the thread-local and its accessor**

In `crates/charpente-core/src/table.rs`, above `impl Store`:

```rust
thread_local! {
    /// Set only while `Store::open`'s redb fence is active.
    ///
    /// A thread-local rather than a `set_hook` swap, and the reason is not
    /// style: `std::panic::set_hook` is process-global while sim universes
    /// run on parallel worker threads (`charpente-sim`'s harness, under
    /// `thread::scope`). Swapping the hook around this fence would silence a
    /// DIFFERENT worker's genuine panic for its duration — trading a benign
    /// false alarm for a suppressed real one, which is strictly worse.
    static IN_OPEN_FENCE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// True while this thread sits inside `Store::open`'s redb open-panic fence.
/// A panic hook consults this to replace redb's benign `page_manager.rs`
/// trace with one named line (F1).
pub fn in_open_fence() -> bool {
    IN_OPEN_FENCE.with(std::cell::Cell::get)
}

/// Clears the flag on unwind as well as on return. The hook runs BEFORE
/// unwinding, so the flag is still set when it reads it.
struct OpenFenceGuard;

impl OpenFenceGuard {
    fn enter() -> Self {
        IN_OPEN_FENCE.with(|f| f.set(true));
        OpenFenceGuard
    }
}

impl Drop for OpenFenceGuard {
    fn drop(&mut self) {
        IN_OPEN_FENCE.with(|f| f.set(false));
    }
}
```

- [ ] **Step 5: Wrap the fence with the guard**

In `Store::open` at `crates/charpente-core/src/table.rs:773-780`, replace

```rust
        let latch = Arc::new(AtomicBool::new(false));
        let db = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Builder::new().create_with_backend(DiskBackend {
                file,
                latch: latch.clone(),
            })
        }))
```

with

```rust
        let latch = Arc::new(AtomicBool::new(false));
        let db = {
            let _fence = OpenFenceGuard::enter();
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                Builder::new().create_with_backend(DiskBackend {
                    file,
                    latch: latch.clone(),
                })
            }))
        }
```

Leave the two `.map_err(...)` calls that follow byte-identical.

- [ ] **Step 6: Install the filtering hook in the simulator**

In `crates/charpente-sim/src/harness.rs`:

```rust
static OPEN_FENCE_HOOK: std::sync::Once = std::sync::Once::new();

/// Replaces redb's benign open-fence trace with one named line, and delegates
/// every other panic to the previous hook untouched. Idempotent — call it from
/// any entry point. Deliberately contains no `panicked` substring: a log grep
/// for that word is what F1 trained agents to misuse.
pub fn install_open_fence_hook() {
    OPEN_FENCE_HOOK.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if charpente_core::table::in_open_fence() {
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

**Install it from `Sim::with_faults` (`crates/charpente-sim/src/lib.rs:572`), NOT from
`run_batch`.** Controller ruling, pre-flight: the deterministic fence regressions this task's
oracle drives construct `Sim::with_faults(...)` **directly** — they never enter `run_batch`
or `run_universes!`. Installing only at those entry points would leave the hook uninstalled
in the child process, and Step 7 would fail even after a correct implementation.
`Sim::new` (`:568`) funnels into `Sim::with_faults`, so `with_faults` is the single
constructor funnel and covers every path: direct construction, the CLI batch, and the
in-test runner alike.

This scoping is correct rather than merely convenient. The 134-line flood F1 describes is a
*simulation* artifact — benign traces from faults the simulator injected on purpose. In
production a fenced redb open-panic is a genuinely notable event and printing it is right,
so the filter belongs where the simulator is, not inside `Store::open`. Do NOT install the
hook from `charpente-core`: that would put process-global mutation inside the deterministic
boundary.

- [ ] **Step 7: Run the new test and confirm it passes**

```bash
cd ../charpente-f1 && cargo test -p edge-harness --test open_fence \
  open_fence_does_not_print_a_raw_redb_panic_trace -- --nocapture 2>&1 | tee /tmp/f1-green.log
echo "exit=${pipestatus[1]}"
```

Expected: PASS.

- [ ] **Step 8: Prove the hook still reports a REAL panic**

A fix that silences everything would pass Step 7 and be far worse than the bug. Add to the same `crates/edge-harness/tests/open_fence.rs` and run:

```rust
/// The fence must not become a blanket mute. A panic OUTSIDE the fence still
/// reaches the hook in full.
#[test]
fn a_panic_outside_the_open_fence_still_prints() {
    if std::env::var("CHARPENTE_F1_PANIC_CHILD").is_ok() {
        charpente_sim::harness::install_open_fence_hook();
        assert!(!charpente_core::table::in_open_fence());
        panic!("deliberate out-of-fence panic");
    }
    let out = std::process::Command::new(env!("CARGO"))
        .args([
            "test", "-p", "edge-harness", "--test", "open_fence",
            "a_panic_outside_the_open_fence_still_prints", "--", "--nocapture", "--exact",
        ])
        .env("CHARPENTE_F1_PANIC_CHILD", "1")
        .output()
        .expect("child runs");
    let err = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        err.contains("deliberate out-of-fence panic"),
        "the hook swallowed a real panic — strictly worse than F1:\n{err}"
    );
}
```

- [ ] **Step 9: Mutation-prove the guard has teeth**

Delete the `let _fence = OpenFenceGuard::enter();` line, re-run Step 7's test, and confirm it goes **RED by name**. Then restore and prove the restore byte-exact:

```bash
git diff --stat
git status --porcelain
shasum -a 256 crates/charpente-core/src/table.rs
git show HEAD:crates/charpente-core/src/table.rs | shasum -a 256
```

The two hashes must differ only by the intended edit; run this from the worktree, and never with the main tree's `CARGO_TARGET_DIR`.

- [ ] **Step 10: Full-package verification, foregrounded**

Expected before running, measured on `main` at `b15cdc7` by the controller and to be
reconciled exactly:

| package | baseline `test result: ok` | expected after this task | why |
|---|---|---|---|
| `charpente-core` | 10 | **10** | mechanism tests join the existing `engine` suite; no new binary |
| `charpente` | 14 | **14** | `transactions.rs` is read, not modified |
| `charpente-sim` | 6 | **6** | `harness.rs` gains a function, no new test binary |
| `edge-harness` | 8 | **9** | the new `open_fence.rs` is one new test binary |

`test result: ok` counts test BINARIES, not individual tests. An unreconciled delta is a
finding, not a rounding error.

```bash
cd ../charpente-f1
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tee /tmp/f1-clippy.log
cargo test -p charpente --no-fail-fast 2>&1 | tee /tmp/f1-charpente.log
cargo test -p charpente-core --no-fail-fast 2>&1 | tee /tmp/f1-core.log
cargo test -p charpente-sim --no-fail-fast 2>&1 | tee /tmp/f1-sim.log
cargo test -p edge-harness --no-fail-fast 2>&1 | tee /tmp/f1-edge.log
for f in /tmp/f1-charpente.log /tmp/f1-core.log /tmp/f1-sim.log /tmp/f1-edge.log; do
  echo "$f ok=$(grep -c '^test result: ok' $f) failed=$(grep -c 'FAILED' $f)"
done
```

Partition per package — the whole workspace suite swings 568–2482 s against a 600 s ceiling.

- [ ] **Step 11: Confirm the original symptom is gone at batch scale**

```bash
cd ../charpente-f1
cargo run -p charpente-cli -- test --sim --dir crates/sim-fixture --universes 1024 2>&1 | tee /tmp/f1-batch.log
echo "exit=${pipestatus[1]}"
grep -c "page_manager.rs" /tmp/f1-batch.log || echo "0 page_manager lines"
grep -c "redb open-fence engaged" /tmp/f1-batch.log || echo "0 fence lines"
```

Expected: exit 0, **0** `page_manager.rs` lines. The fence-engaged count may be zero if this batch never trips it — that is not evidence of a fix, only the absence of a counter-example, and must be reported as such.

- [ ] **Step 11b: Add the new suite to CLAUDE.md's exhaustive list**

CLAUDE.md's per-crate suite lists are exhaustive by contract, and `guide-examples`'
`doc_inventory` gate enforces it — a new suite file that is not listed fails the gate.
Add `open_fence` to the `edge-harness` list (currently
`acked_durable/failover/hunts/realtime_resume/smoke/tenant_wire`), keeping alphabetical
order, then confirm:

```bash
cd ../charpente-f1 && cargo test -p guide-examples --no-fail-fast 2>&1 | tee /tmp/f1-inventory.log
echo "ok=$(grep -c '^test result: ok' /tmp/f1-inventory.log)"
```

- [ ] **Step 12: Commit named paths only**

```bash
git add crates/charpente-core/src/table.rs crates/charpente-sim/src/harness.rs \
        crates/edge-harness/tests/open_fence.rs crates/charpente-core/tests/engine.rs CLAUDE.md
git status --porcelain
git commit -F /tmp/f1-msg.txt
git log --oneline -1
```

The message records: the defect and its cost, why `set_hook` around the fence was refused (process-global vs parallel worker threads), the RED observation from Step 3, the mutation proof from Step 9, and the counted results from Step 10.

**Do not push, and do not merge to main.** Noah reviews the branch.

---

### Task 3: The reverse scope-flip gap

**The defect.** `14863db` fixed one direction: a table declared `scoped_by` whose rows are physically bare-`u64`-keyed now meets `scope_flip_error` at all eleven scoped doors, single-sourced through `scoped_open_err`. The **reverse** — a table declared unscoped whose rows are physically composite-keyed — is named at only two doors, `scan_raw` and `scan_raw_from`, via `scan_raw_shape_error` (`crates/charpente-core/src/table.rs:512`, used at `:1839` and `:1916`). Every other unscoped open falls through to `redb_err`, whose text is redb's own and names neither the contract, the site, nor the fix — the same hard-rule-5 violation `14863db` repaired in the other direction. The ledger is explicit that this "wants its own failing test rather than being folded in behind this one's green."

**The precedent to mirror** is `14863db` itself: a RED assertion test that knocks on every public door, then a single-sourced error mapping. Its test is `every_scoped_entry_point_refuses_the_scope_flip_by_name` at `crates/charpente-core/tests/engine.rs:687` — read it before writing the mirror.

**Files:**
- Modify: `crates/charpente-core/src/table.rs` (a new `unscoped_open_err`, and the `rows_def(` open sites)
- Test: `crates/charpente-core/tests/engine.rs`

**Interfaces:**
- Consumes: nothing from other tasks. Runs in its own worktree, so it cannot conflict with Task 2.
- Produces: `fn unscoped_open_err(table: &str, e: redb::TableError) -> io::Error` — private to `table.rs`, mirroring `scoped_open_err` at `:545`.

- [ ] **Step 1: Create the worktree**

```bash
git worktree add -b reverse-scope-flip ../charpente-rsf main
export CARGO_TARGET_DIR=/tmp/charpente-rsf-target
mkdir -p "$CARGO_TARGET_DIR"
cd ../charpente-rsf && git log --oneline -1
```

- [ ] **Step 2: Census the unscoped doors — count them, do not sample**

```bash
cd ../charpente-rsf
grep -n "open_table(rows_def(" crates/charpente-core/src/table.rs
grep -n "open_table(rows_def(" crates/charpente-core/src/table.rs | wc -l
```

Twelve sites are expected at `main` (`:981, :1124, :1248, :1294, :1317, :1345, :1394, :1417, :1429, :1483, :1829, :1891`). Classify each: which are user row tables, which is the event table (`:981`), and which two already name the contract (`:1829`, `:1891` — `scan_raw`'s arms). Write the classification to the report file BEFORE writing any code; the per-site shape is what let the forward gap open unnoticed, and a census that confirms known instances and stops is the failure mode here.

- [ ] **Step 3: Write the failing test — knock on every public unscoped door**

Add to `crates/charpente-core/tests/engine.rs`, mirroring `every_scoped_entry_point_refuses_the_scope_flip_by_name` at `:687`. Build a store whose table is declared unscoped while its rows are written composite-keyed, then call each public unscoped entry point and assert the error text names the contract:

```rust
/// The mirror of `every_scoped_entry_point_refuses_the_scope_flip_by_name`
/// (`crates/charpente-core/tests/engine.rs:687`). `14863db` fixed the scoped
/// direction at all eleven doors; the reverse — declared unscoped, physically
/// composite-keyed — is named only in `scan_raw`'s arms via
/// `scan_raw_shape_error`. Every other door returns redb's own text, which
/// names neither the contract, the site, nor the fix (hard rule 5).
#[test]
fn every_unscoped_entry_point_refuses_the_reverse_flip_by_name() {
    let sim = Sim::with_faults(0x7A2E_000B, FaultConfig::none());
    let store = Store::open(&sim.disk(), "store.redb").unwrap();

    // `Ticket` is declared WITHOUT scoped_by. Writing it composite-keyed is
    // the disagreement under test — the exact inverse of the scoped test,
    // which writes bare and then reads scoped.
    store
        .apply(0, |w| w.put_raw(Ticket::TABLE, Some(10), 5, b"old"))
        .expect("a scoped put must not error");

    fn names_the_contract(err: &io::Error, site: &str) {
        let s = err.to_string();
        assert!(
            s.contains("is declared without scoped_by but its rows are keyed with a tenant scope"),
            "{site} must name the reverse-flip contract, got: {s}"
        );
        assert!(
            s.contains("a table cannot lose a scope in place"),
            "{site} must name the fix, got: {s}"
        );
    }

    // The unscoped readers are `View` trait methods on `Snapshot`
    // (`crates/charpente-core/src/table.rs:1415-1496`); `View` must be in scope.
    let snap = store.snapshot().unwrap();
    names_the_contract(
        &snap.get::<Ticket>(Id::from_raw(5)).expect_err("must refuse the reverse flip"),
        "View::get",
    );
    names_the_contract(
        &snap.scan::<Ticket>().expect_err("must refuse the reverse flip"),
        "View::scan",
    );
    names_the_contract(
        &snap.scan_from::<Ticket>(0, 10).expect_err("must refuse the reverse flip"),
        "View::scan_from",
    );
    drop(snap);

    // Every writer arm fails before commit, so `applied_seq` stays 0 and each
    // attempt is the same gapless seq 1 — the same reasoning the scoped test
    // records at its writer block.
    names_the_contract(
        &store
            .apply(1, |w| w.get_raw(Ticket::TABLE, None, 5))
            .expect_err("must refuse the reverse flip"),
        "Writer::get_raw",
    );
    names_the_contract(
        &store
            .apply(1, |w| w.put_raw(Ticket::TABLE, None, 6, b"new"))
            .expect_err("must refuse the reverse flip"),
        "Writer::put_raw",
    );
    names_the_contract(
        &store
            .apply(1, |w| w.delete_raw(Ticket::TABLE, None, 5))
            .expect_err("must refuse the reverse flip"),
        "Writer::delete_raw",
    );
}
```

`View::scan_by` (`:1442`) is a fourth unscoped door and belongs in this test too — add it once Step 2's census confirms its signature. **The door list is the census's, not this snippet's:** if Step 2 found a public unscoped door not covered above, it goes in. Do not write the test against a subset — a census that confirms known instances and stops is the failure mode this task exists to correct.

- [ ] **Step 4: Run it and confirm it fails with the bare redb text**

```bash
cd ../charpente-rsf && cargo test -p charpente-core --test engine \
  every_unscoped_entry_point_refuses_the_reverse_flip_by_name -- --nocapture 2>&1 | tee /tmp/rsf-red.log
echo "exit=${pipestatus[1]}"
```

Expected: **FAIL**, quoting redb's own "is of type" text for the unfixed doors. If it passes, STOP — record it; the gap may already be closed and the task becomes a documented no-op.

- [ ] **Step 5: Add `unscoped_open_err` and route every door through it**

Mirror `scoped_open_err` at `crates/charpente-core/src/table.rs:545`:

```rust
/// An unscoped row-table open failed. `TableTypeMismatch` is the reverse
/// scope-flip contract: the schema asked for the bare-`u64` shape and the disk
/// holds composite-keyed rows. Every unscoped entry point routes its open
/// failure through here, so an operator meets the named contract at whichever
/// door they knocked on — the mirror of `scoped_open_err`, and the per-site
/// shape is exactly what let the forward gap open unnoticed (`14863db`).
fn unscoped_open_err(table: &str, e: redb::TableError) -> io::Error {
    match e {
        redb::TableError::TableTypeMismatch { .. } => io::Error::other(format!(
            "table \"{table}\" is declared without scoped_by but its rows are keyed with a \
             tenant scope: a table cannot lose a scope in place; declare it scoped_by, or \
             create a new unscoped table and copy rows through a job"
        )),
        other => redb_err(other),
    }
}
```

Replace `.map_err(redb_err)?` with `.map_err(|e| unscoped_open_err(table, e))?` at every `open_table(rows_def(...))` site the census classified as a user row table. Leave the event table at `:981` alone unless the census shows it reachable by the same disagreement — and if it does, say so in the report rather than changing it silently.

- [ ] **Step 6: Run the test and confirm it passes**

```bash
cd ../charpente-rsf && cargo test -p charpente-core --test engine \
  every_unscoped_entry_point_refuses_the_reverse_flip_by_name -- --nocapture 2>&1 | tee /tmp/rsf-green.log
echo "exit=${pipestatus[1]}"
```

- [ ] **Step 7: Mutation-prove the mapping**

Revert `unscoped_open_err`'s match arm to `redb_err(e)` unconditionally, re-run Step 6, confirm RED by name, restore, and prove the restore byte-exact with `shasum -a 256` against `git show HEAD:crates/charpente-core/src/table.rs`. A green mutation proves nothing on its own — if it stays green, print the observable behaviour before concluding.

- [ ] **Step 8: Verify the forward direction did not regress**

```bash
cd ../charpente-rsf && cargo test -p charpente-core --test engine \
  every_scoped_entry_point_refuses_the_scope_flip_by_name -- --nocapture 2>&1 | tee /tmp/rsf-fwd.log
```

Expected: PASS, unchanged.

- [ ] **Step 9: Package verification, foregrounded and partitioned**

```bash
cd ../charpente-rsf
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tee /tmp/rsf-clippy.log
cargo test -p charpente-core --no-fail-fast 2>&1 | tee /tmp/rsf-core.log
cargo test -p charpente --no-fail-fast 2>&1 | tee /tmp/rsf-charpente.log
for f in /tmp/rsf-core.log /tmp/rsf-charpente.log; do
  echo "$f ok=$(grep -c '^test result: ok' $f) failed=$(grep -c 'FAILED' $f)"
done
```

State the expected counts before running and reconcile any delta exactly (the new test is +0 suites, +1 test).

- [ ] **Step 10: Check whether GUIDE promises this already**

`14863db` found `docs/GUIDE.md:5516` already promising the fixed behaviour unconditionally, so no doc change was owed. Check the same for this direction:

```bash
grep -n "scope" docs/GUIDE.md | grep -in "flip\|keyed\|shape" | head
```

If GUIDE already promises it, record that no doc change is owed. If it does not, hard rule 6 applies and the doc change lands in this commit.

- [ ] **Step 11: Commit named paths only**

```bash
git add crates/charpente-core/src/table.rs crates/charpente-core/tests/engine.rs
git status --porcelain
git commit -F /tmp/rsf-msg.txt
git log --oneline -1
```

The message states the census count ("N doors out of M"), the RED observation, the mutation proof, the counted results, and whether a doc change was owed. **Do not push or merge.**

---

### Task 4: T3 adversarial-review baseline collection

Collection only. **Scoring is explicitly out of scope** — it is the judgment half, and the sub-project's own record is that every finding in this class was caught by a human holding the line, none by a mechanism.

**Files:**
- Read: `.superpowers/sdd/2026-08-14-agent-skills/task-3-brief.md`, `task-3-criteria.md`, `task-3-scenario.diff`, `task-3-external-observables.md`
- Create: `.superpowers/sdd/2026-08-14-agent-skills/task-3-baseline-{c,d}.md` and a `task-3-collection-note.md`

**Interfaces:**
- Consumes: nothing from Tasks 1–3.
- Produces: archived transcripts, explicitly marked unscored.

- [ ] **Step 1: Read the completed baseline, not the placeholder**

`task-3-baseline-b-COMPLETED.md` is the real one; `task-3-baseline-b.md` is truncated. This is a hazard that sub-project already recorded and paid for. Read the COMPLETED file.

- [ ] **Step 2: Create the baseline worktrees at `ee17b8c`, detached**

```bash
git worktree add --detach ../charpente-t3-c ee17b8c
git worktree add --detach ../charpente-t3-d ee17b8c
cd ../charpente-t3-c && git log --oneline -1
```

`ee17b8c` predates the plan file, so the rubric is absent from the tree. **This is the contamination fix, applied deliberately.** Verify it:

```bash
test -f ../charpente-t3-c/docs/superpowers/plans/2026-08-14-agent-skills.md && echo "CONTAMINATED — STOP" || echo "clean: plan absent"
```

If the plan file is present, STOP. An evaluation scenario must not run in a workspace that contains the evaluation.

- [ ] **Step 3: Dispatch two unnamed baseline agents**

Each gets `task-3-brief.md`'s scenario, works in its own detached worktree with its own `CARGO_TARGET_DIR`, and **writes its output to its archive file before returning**. Neither receives `task-3-criteria.md` — that is the rubric.

- [ ] **Step 4: Verify completeness at copy time**

The sub-project's owed process fix: an artifact copied out of a worktree needs a completeness check at copy time.

```bash
for f in .superpowers/sdd/2026-08-14-agent-skills/task-3-baseline-c.md \
         .superpowers/sdd/2026-08-14-agent-skills/task-3-baseline-d.md; do
  echo "$f lines=$(wc -l < $f) placeholders=$(grep -c 'TBD\|TODO\|\[fill' $f)"
done
```

A non-zero placeholder count means the agent did not finish; record it rather than archiving a truncated file as complete.

- [ ] **Step 5: Write the collection note and leave the worktrees**

`task-3-collection-note.md` states: these are UNSCORED; they were collected at `ee17b8c` specifically so the tree lacks the rubric; scoring requires reading `task-3-baseline-b-COMPLETED.md` not the placeholder; and whether the completed baseline B changes T3's outcome remains the open question the sub-project left. Do not remove the worktrees — their gitignored contents die with them.

- [ ] **Step 6: Commit the archive**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
git status --porcelain
```

`.superpowers/` is untracked and not in `.gitignore`. Do **not** add it to git in this run — that is a repo-shape decision for Noah, and it belongs in `decisions-pending.md`, not in an unattended commit.

---

### Task 5: The mutation sweep

The graceful tail. Runs last, when nothing else competes for the machine, and may be cut off at any point.

**Files:**
- Create: `.superpowers/sdd/2026-09-09-autonomous-run/mutation-findings.md`

**Interfaces:**
- Consumes: a green committed `main` from Task 1.
- Produces: a findings file. **No fixes.** A MISSED verdict means a one-sided property, and repairing one is design work.

- [ ] **Step 1: Confirm the tool and the pin**

```bash
cargo mutants --version
```

Expected: `cargo-mutants 27.1.0`, matching CLAUDE.md's pin. Any other version: stop and report.

- [ ] **Step 2: Create the sweep worktree against main**

```bash
git worktree add --detach ../charpente-mutants main
export CARGO_TARGET_DIR=/tmp/charpente-mutants-target
mkdir -p "$CARGO_TARGET_DIR"
cd ../charpente-mutants && git log --oneline -1
```

The baseline must be `main` at Task 1's commit — a known-green tree. Tasks 2 and 3's branches are deliberately NOT the baseline; unreviewed code would make every verdict ambiguous.

- [ ] **Step 3: Prove the baseline is green before trusting any counter**

`scripts/mutants.sh` exits 2 on a baseline that was not green, and a failed baseline leaves every counter reading zero — which looks identical to a clean sweep.

```bash
cd ../charpente-mutants && cargo test -p charpente-core --no-fail-fast 2>&1 | tee /tmp/mut-baseline.log
echo "ok=$(grep -c '^test result: ok' /tmp/mut-baseline.log) failed=$(grep -c FAILED /tmp/mut-baseline.log)"
```

- [ ] **Step 4: Run the sweep on the default oracle-bearing target set**

```bash
cd ../charpente-mutants && caffeinate -i ./scripts/mutants.sh 2>&1 | tee /tmp/mut-run.log
echo "verb_exit=${pipestatus[1]}"
```

**Read the verb's output, never cargo-mutants' own exit code** — the raw tool exits 0 when a filter matches nothing and writes no `outcomes.json` at all. Exit 0 means every viable mutant was caught; 1 means a finding or an unresolved run; 2 means usage, infrastructure, or a baseline that was not green.

- [ ] **Step 5: Triage into the findings file, distinguishing the three outcomes**

For each result record the mutant, its file and line, and its verdict as exactly one of: **MISSED** (no oracle noticed — a one-sided property, a real finding), **TIMEOUT** (**not a catch**, and never recorded as one), or **UNRESOLVED**. Append as you go so a cut-off run still leaves usable output.

- [ ] **Step 6: Report, do not fix**

Write the findings file with a header stating that nothing was fixed and why: a MISSED verdict names a property that is one-sided, and deciding what it should assert is design work that belongs with Noah.

---

## Controller: the phase-boundary gate

Run after **every** task, by the controller, never read from an agent's report.

- [ ] `git status --porcelain` empty in the relevant tree
- [ ] HEAD sha matches what the agent claimed — `git rev-parse --short HEAD`
- [ ] The task's verification **re-run by the controller**, with the expected count stated first, then counted from the output
- [ ] Any claim about a commit checked with `git show <sha>:<path>`, not the working tree
- [ ] The result — including a failure — appended to `.superpowers/sdd/2026-09-09-autonomous-run/report.md`

**If a gate fails twice on the same task, the run stops and writes the report.**

## The report

`.superpowers/sdd/2026-09-09-autonomous-run/report.md`, in the MAIN tree so it survives the worktrees. Sections, in order:

1. **Per task:** what landed, the sha, and the counted verification evidence — the actual numbers, not "green".
2. **Decisions pending** — every logged assumption, with its reasoning and what would falsify it. Includes at minimum whether `.superpowers/` should be tracked or ignored (Task 4 Step 6 defers it deliberately).
3. **Mutation findings** — MISSED separated from TIMEOUT and UNRESOLVED.
4. **Left undone, and why.**
5. **What I could not verify.** Populated, not empty-by-omission. A run that verified everything it attempted still has entries here — Task 2 Step 11's fence-engaged count of zero is one, since the absence of a counter-example is not evidence of a fix.

## Exit criteria

- [ ] Task 1's three files are committed to `main` and pushed, with the gate counted at 4 `test result: ok`, and `git rev-parse main origin/main` printing the same sha
- [ ] Task 2's test was observed RED at Step 3 for the stated reason, green at Step 7, and the hook proven not to swallow a real panic at Step 8
- [ ] Task 3's test was observed RED at Step 4 quoting redb's own text, and the census count is stated as "N doors out of M"
- [ ] Both defect branches exist locally, unpushed and unmerged
- [ ] Task 4's transcripts are archived, completeness-checked, and marked UNSCORED
- [ ] Task 5's findings distinguish MISSED from TIMEOUT and UNRESOLVED, and no fix was applied
- [ ] Every task boundary has a recorded gate result in the report — including any that failed
- [ ] The report's "what I could not verify" section is non-empty

## Operational preconditions

- [ ] The terminal stays open — this session is the controller
- [ ] `caffeinate` held for the duration; the machine must not sleep mid-run
- [ ] Only Task 1 pushes; Tasks 2 and 3 leave unmerged local branches
- [ ] No worktree is removed, and no stale branch is deleted, in this run
