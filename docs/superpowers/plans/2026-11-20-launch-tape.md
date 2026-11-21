# The launch tape Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A 30-second terminal tape in which `charpente test --sim` catches a double charge in a
small order API, built on sim output that fits one screen (0.6.0), and a README in retrace's shape.

**Architecture:** The demo app, `examples/double-charge/`, ships with the bug. It sits outside the
workspace and depends on the published crates, and a workspace test patches it to the current
code and pins its red output. The cleanup changes only `charpente-sim`'s human format; the JSON
and the six recorded batches don't move. The tape runs the released crates after Noah publishes
0.6.0.

**Tech Stack:** Rust 1.99 (repo pin), charpente-sim's harness, `vhs` 0.x with ffmpeg (both
installed via Homebrew), and the `charpente` CLI.

**Spec:** [docs/superpowers/specs/2026-11-20-launch-tape-design.md](../specs/2026-11-20-launch-tape-design.md)
(approved by Noah 2026-11-20, "write it up").

**Workspace:** `.superpowers/sdd/2026-11-20-launch-tape` (`$SDD` below). The builder check is there.

## Global Constraints

- The release is **0.6.0**: DECISIONS §15 bumps the minor for any output change.
- The demo depends on `charpente = "=0.6.0"` and `charpente-sim = "=0.6.0"`. It has its own
  empty `[workspace]` table, and its `Cargo.lock` is committed once 0.6.0 is on crates.io
  (Task 8).
- The demo's master seed is **`0x434841524745`** ("CHARGE" in ASCII), fixed here before any run.
  It is typed on screen in the tape and never re-rolled.
- The tape: 1600×900, Menlo **22**, LineHeight 1.25, Padding 28, Margin 36, retrace's
  "Evening paper" theme and margin fill. Measured 2026-11-20 at these settings, the frame is
  **23 rows × 103 columns**, prompt included. (At Menlo 26, retrace's size, it is 19 × 90,
  which the red output can't fit.)
- **The frame gate:** the demo's red output under the master seed wraps to **at most 20 rows at
  103 columns**. That is 23 rows, less the typed comment, the command and the closing prompt.
  Rows are counted per line as `max(1, ceil(chars / 103))`, in `char`s, not bytes.
- The JSON format doesn't change, and `$SDD/oracle5.sh <label>` exits 0 after every task.
- No timing figure in README unless docs/BENCH.md carries it. Every README count carries its
  master seed on its line or the next (`claims_census.rs`).
- Nothing on screen is edited, trimmed or piped through `head`.
- CLAUDE.md applies:
  - hard rules 5 and 6: error text is pinned by tests, and docs change in the same commit;
  - commits straight to main with named paths and no Co-Authored-By trailer;
  - `--no-fail-fast`, with green confirmed by counting `test result: ok`;
  - zsh's `${pipestatus[1]}`;
  - runs bounded with `perl -e 'alarm N; exec @ARGV' --`.
- Subagents never push, publish, touch Docker, `~/h2h-runs.noindex` or
  `~/Documents/GitHub/judge`, and never dispatch subagents.
- No employer, past or present, appears anywhere. Outward contact is noah@noahmitchem.com.

## Review Focus

1. A reader runs the demo without `--master-seed`. It should still go red, with the same check
   and the same named fix. Task 2 pins eight other master seeds.
2. A reader pastes the repro line the red output prints. It should replay the same failure. Task
   4 runs the printed repro and compares the message.
3. A batch where one check failed and another skipped every run. The failed check's skipped runs
   condense, and the untested check keeps its full reasons, because those reasons are its whole
   diagnosis. Task 3 pins both in one batch.
4. A reader on rustup's stale `stable`. On 2026-11-20 this machine had 1.95, and cargo refused
   `rust-version` 1.99 by name. README's Install names Rust 1.99 and `rustup update`. Task 5
   pins it.
5. The three lines put back. The demo must go green, or its red proves nothing (the victim
   pattern). Task 2 pins it.

---

### Task 0: Workspace, records of the decision (controller)

**Files:**
- Create: `$SDD/progress.md`, `$SDD/oracle5.sh`, `$SDD/oracle5-filter.jq`, `$SDD/baseline/*`
- Modify: `PLAN.md:392-393`, `docs/superpowers/specs/2026-11-06-launch-demo-design.md:3-4`,
  `docs/superpowers/specs/2026-11-20-launch-tape-design.md` (§5, §6)

- [ ] **Step 1:** Copy `oracle5.sh`, `oracle5-filter.jq` and `baseline/` from
  `.superpowers/sdd/2026-11-19-0.5.0/`. Repoint `sc=` from that session's scratchpad to
  `${TMPDIR:-/tmp}/oracle5-scaffold-$label`, which is outside the repo. Run
  `$SDD/oracle5.sh t0`. Expected: exit 0, six batches unchanged, because no code has changed
  since 0127f59e.
- [ ] **Step 2:** In PLAN's F1/F2 rows, replace "Fix red-first before the launch demo's public
  attempt (ruling R43)" with a trigger with no date, "the first adopter whose API needs it". The
  launch tape (spec 2026-11-20) replaced the public attempt as the launch's lead. In the launch
  demo spec's status line, add that its head-to-head no longer leads the launch, per Noah
  2026-11-20 and the launch tape spec.
- [ ] **Step 3:** Amend the tape spec with this plan's measured frame. §5 says Menlo 22, and §6
  says 23 × 103 measured with the 20-row output budget. The renders stay out of git: Noah
  uploads the MP4, and the GIF and MP4 live in `$SDD`.
- [ ] **Step 4:** Run `cargo test -p guide-examples --no-fail-fast`: 11 `test result: ok`.
  Commit: `plan: the launch tape; PLAN's F1/F2 lose the public-attempt trigger; the launch demo
  spec records that the tape leads`.

### Task 1: Versions to 0.6.0

**Files:**
- Modify: `Cargo.toml` (`[workspace.package] version`, path-dependency versions),
  `crates/*/Cargo.toml` wherever a path dependency names a version,
  `crates/charpente-cli/src/new.rs:192`, `docs/GUIDE.md:80,84` and any other `"0.5"` /
  `0.5.0` that describes the current version (grep, and leave history alone), `CHANGELOG.md`
  (a new top section `## 0.6.0 (unreleased)`)

- [ ] **Step 1:** Change `new.rs`'s test to `assert_eq!(dependency_version(), "0.6");` and run
  `cargo test -p charpente-cli --bin charpente dependency_version_is_major_minor`. Expected:
  FAIL, left "0.5".
- [ ] **Step 2:** Bump every version to 0.6.0, and GUIDE §1's manifest block to `"0.6"`.
- [ ] **Step 3:** Run `-p charpente-cli` and `-p guide-examples`, both with `--no-fail-fast`,
  and count `test result: ok`. Run `$SDD/oracle5.sh t1`. Expected: exit 0, or only `myapp-0x1`
  moving. A move is recorded as Ruling S-1 (the scaffold's lockfile) and re-baselined from
  `oracle5-t1`.
- [ ] **Step 4:** Commit: `release: versions to 0.6.0; CHANGELOG opens 0.6.0 (unreleased)`.

### Task 2: The demo app, with its bug, pinned red and green

**Files:**
- Create: `examples/double-charge/`, starting from `charpente new double-charge`'s files and
  then edited:
  - `Cargo.toml`;
  - `clippy.toml`;
  - `.gitignore`;
  - `src/lib.rs`;
  - `src/main.rs`;
  - `tests/gateway/mod.rs`;
  - `tests/sim.rs`;
  - `tests/once.rs`;
  - `README.md`, three lines: what it is and how to run it.
- Create: `crates/charpente-cli/tests/demo.rs`
- Modify: `crates/guide-examples/tests/driver_census.rs` (walk both demo test files),
  `CLAUDE.md` (workspace map: an `examples/double-charge` bullet, saying it is outside the
  workspace and why; test list: `charpente-cli: …/demo/…`)

**Interfaces:**
- Produces, in `double_charge` (`src/lib.rs`):
  - `pub struct Order { pub id: Id<Order>, pub paid: bool }` (`#[table(global)]`);
  - `pub struct Charge { pub order: Id<Order> }`, declared `#[job(global, retain_dead,
    outbound = "pay.example", retry(max = 8, backoff = "500ms"), timeout = "2s")]`, with
    `modelled_remote.rs`'s `call` and `settle`, and the one-line `retain_dead` reason GUIDE §6
    asks for;
  - `#[route(POST "/orders/:id", public)] pub fn place_order(tx: &mut Tx, id: Id<Order>) ->
    Result<Id<Order>, ApiError>`. It is `modelled_remote.rs:108-115` **without lines
    109-111**: it puts the order and enqueues its `Charge` every time;
  - `pub fn build_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> io::Result<App>`,
    mounting `routes![place_order]`, `jobs![Charge]` and `.outbound(Host::new("pay.example"))`.
- Produces, in `tests/gateway/mod.rs`: `pub struct Gateway`, the `Remote` from
  `modelled_remote.rs:141-185`.
- Produces, in `tests/sim.rs`: the check `each_order_is_charged_once`, `modelled_remote.rs:187`
  onward, driven by `charpente_sim::drive::Drive`.
  - Its double-charge message is the demo's own, under 140 characters.
  - It names the order and says it was charged twice.
  - It says why: its POST was sent again after a lost reply and placed the order again.
  - It names the fix: answer an order that is already stored.
- Produces, in `tests/once.rs`: `#[test] fn each_order_posted_once_is_charged_once()`. It
  runs `Sim::with_faults(1, FaultConfig::none())` through `Drive`, posts orders 1 to 48 once
  each, and asserts the gateway applied each one once. This is the table's "a test that posts
  each order once".
- Produces, in `demo.rs`:
  - `const MASTER: &str = "0x434841524745";`
  - `fn patched_demo(name: &str) -> PathBuf`: copies the demo, minus `target/` and
    `Cargo.lock`, to a scratch directory; appends `[patch.crates-io]` for `charpente` and
    `charpente-sim`; and pins the workspace lock. These are `new.rs`'s helpers at :231-260 and
    :451-470, copied rather than shared, because CLAUDE.md asks for no helper sprawl.
    `CARGO_TARGET_DIR` is `new.rs`'s shared target directory.
  - `fn wrapped_rows(text: &str, cols: usize) -> usize`, used by Task 4.

- [ ] **Step 1: Write `demo.rs`'s first four tests.** All fail while the directory is absent.
  - `the_demo_charges_an_order_twice_and_names_the_fix`: `charpente test --sim --master-seed
    MASTER` exits 1. stdout contains `"sim failure in each_order_is_charged_once: "`,
    `"charged twice"` and `"answer an order that is already stored"`. Task 4's golden block
    pins the rest.
  - `the_demo_goes_red_under_other_master_seeds`: for master seeds `0x1` to `0x8`, each batch
    exits 1, and its first failure is `each_order_is_charged_once` with the same named fix.
  - `restoring_the_three_lines_turns_the_demo_green`: insert `modelled_remote.rs:109-111` back
    at the top of `place_order`, and the batch under MASTER exits 0 with `sim result: ok.`
  - `the_demo_builds_lints_clean_and_passes_a_single_send_test`: `cargo build`, `cargo clippy
    --all-targets -- -D warnings` and `cargo test --test once` each exit 0 on the buggy demo.
- [ ] **Step 2:** Run `cargo test -p charpente-cli --test demo --no-fail-fast`. Expected:
  FAIL, because there is no demo directory.
- [ ] **Step 3:** Build the demo app to the Interfaces above. Its `Cargo.toml` has
  `rust-version = "1.99"` and `publish = false`, keeps the scaffold's `[[test]] name = "sim"
  harness = false` and its `[lints.clippy]` fence, and ends with an empty `[workspace]`.
- [ ] **Step 4:** Run the demo tests, `-p guide-examples` (`driver_census`, `doc_inventory`),
  and `$SDD/oracle5.sh t2` (exit 0). In the report, record the MASTER batch's tested, failed and
  skipped counts and its full stdout, which is the before-cleanup baseline for Task 4.
- [ ] **Step 5:** Commit: `examples: double-charge, an order API whose re-sent POST charges
  twice — pinned red under 0x434841524745 and eight other masters, green with its three lines
  restored`.

### Task 3: The red output, cleaned up (charpente-sim's human format)

**Files:**
- Modify: `crates/charpente-sim/src/harness.rs`:
  - `human_failure_block`, :2016-2080;
  - the skipped-runs summary, :1760-1800;
  - their unit tests (:2295, :3406, :3458, :3522, :3683, and every assertion on `" draws to "`
    or `"before shrinking"`).
- Modify: every `give_up` text in `crates/charpente-sim/src/*.rs` that says "corruption", and
  its pins (`checks.rs:1624` and the like).
- Modify: `crates/charpente-cli/tests/cli.rs:110`, `crates/merge-fixture/tests/sim.rs:219`,
  and `new.rs`'s `before shrinking` assertions.
- Modify: `docs/GUIDE.md`:
  - §8 "Reading a failure" (:5281-5320): the example block, and the prose on `shrunk:` and
    `before shrinking:`;
  - the pinned skipped block at :3578-3591, and the block at :3606.
- Modify: `CHANGELOG.md` 0.6.0.

**Interfaces:**
- Produces in `harness.rs`: `fn same_failure(a: &str, b: &str) -> bool`, which compares the two
  messages with every run of ASCII digits replaced by `#`.

The four changes. Each is red first, with unit tests in `harness.rs` against the rendered text:

- [ ] **Step 1, `before shrinking:`.** It prints only when `!same_failure(&s.message,
  &f.message)`.
  - Test `a_shrink_that_changes_only_numbers_prints_no_before_line`: the messages `"hits 2,
    done 1 for key 3"` and `"hits 3, done 1 for key 812"` give no line.
  - Test `a_shrink_that_changes_the_failure_prints_the_before_line`: `"exactly-once
    violated: …"` against `"ack-durability violated: …"` prints it.
  - GUIDE's example line `before shrinking: exactly-once violated: hits 3, done 1 for key 812`
    now wouldn't print. Rewrite the example so its unshrunk failure differs in kind, or drop the
    line, and state the rule in the prose.
- [ ] **Step 2, the shrink line.** A smaller shrink reads `  shrunk: {from} input{s} to {to},
  {k} fault{s} off, in {r} replays`, with the singular at 1. Examples: `shrunk: 3 inputs to 2,
  2 faults off, in 87 replays` and `shrunk: 1 input to 1, 5 faults off, in 10 replays`. The
  names of the faults turned off stay in `repro:`'s `--faults-off`, the only place a reader
  needs them. `shrunk: nothing smaller fails, in {r} replays` is unchanged. Update every parse
  of `" draws to "` to `" to "` on the new text.
- [ ] **Step 3, skipped runs.** The summary's header becomes `skipped runs hit an injected
  fault before reaching the check, so they prove nothing either way:`.
  - A check with at least one failed run prints one line instead of its reasons: the check,
    the skipped count, and that `--format json` lists the reasons. The wording is the
    builder's, but it must fit within 103 columns for `each_order_is_charged_once`.
  - A check with no failed run, including an untested one, keeps its full reasons.
  - Test `a_failed_checks_skips_condense_and_an_untested_checks_do_not`: one batch with a
    failing check that skips and an always-skipping check. The first gets one line, and the
    second gets its reasons and the untested fix line.
- [ ] **Step 4, corruption reasons.** Every platform-written skip reason that names corruption
  says it was injected. For example, `the boot was refused: injected corruption reached durable
  data at or below the store's horizon, which no restart clears, so nothing past it was
  adjudicated`.
- [ ] **Step 5:** Update GUIDE §8 and both skipped blocks in the same commit. Then run:
  - `-p charpente-sim`, `-p charpente-cli`, `-p merge-fixture` and `-p guide-examples`
    (`doc_truth`'s pinned texts), all with `--no-fail-fast`, counting `test result: ok`;
  - `$SDD/oracle5.sh t3`, which must exit 0, because the filter reads JSON decisions only;
  - `demo.rs`.
- [ ] **Step 6:** Commit: `sim: the red output fits a screen — before-shrinking only when the
  failure changed, the shrink line in plain words, a failed check's skips on one line, injected
  faults called injected`.

### Task 4: The demo's red block, pinned whole and to the frame

**Files:**
- Modify: `crates/charpente-cli/tests/demo.rs`

- [ ] **Step 1:** Add the three tests.
  - `the_demos_red_block_fits_the_tapes_frame`: `wrapped_rows(stdout, 103) <= 20` for the
    batch under MASTER. Show it red first by asserting it against Task 2's recorded
    before-cleanup stdout, which wraps to more than 20 rows. Paste that output into the report.
  - `the_demos_red_block_is_pinned_whole`: the batch's full stdout equals a `const
    GOLDEN: &str`. README quotes this block. Its doc comment says it is the README's and the
    tape's block, and that it moves only with a named cause.
  - `the_demos_repro_line_replays_its_failure`: run the `repro:` line the batch printed. It
    exits 1, and its `sim failure in …` line is byte-identical to the batch's.
- [ ] **Step 2:** If the frame test is still over 20 rows after Task 3, shorten the demo's own
  message (Task 2's interface allows it). Never touch the frame constants. A cut the harness
  needs goes back to Task 3 as a fix round.
- [ ] **Step 3:** Measure `wrapped_rows` for master seeds `0x1` to `0x20` (32 batches). Record
  each seed's row count and exit code, and the worst row count, in the report. This is
  measurement, not a gate (spec §6).
- [ ] **Step 4:** Run `demo.rs` and `$SDD/oracle5.sh t4`. Commit: `demo: the double charge's
  red block pinned whole and within the tape's 20 rows at 103 columns; its repro replays it`.

### Task 5: README in retrace's shape

**Files:**
- Modify: `README.md` (rewritten), `crates/guide-examples/tests/claims_census.rs` (`CLAIMS` and
  `EXEMPT` rows for figures README gains or loses), `crates/charpente-cli/tests/demo.rs` (one
  test), `CHANGELOG.md` 0.6.0, `docs/LEDGER.md`

The sections, in order (spec §7). Read retrace's `README.md` at `16149ad0` first, for tone:

1. `# charpente` and one plain sentence. It is a Rust backend in one binary: server, database,
   jobs and auth. It comes with a simulator that runs the whole app through crashes, lost replies
   and outages, and every failure replays from its seed. Then two sentences: you or your coding
   agent write the app as Rust, and `charpente test --sim` runs it hundreds of times under
   injected faults.
2. A line saying where the video goes, left empty until Task 10. No placeholder text shows.
3. `## Example: a double charge`:
   - `place_order` as the demo has it, with its three missing lines named;
   - the table "What says no to this code": `cargo build` compiles, `cargo clippy -- -D
     warnings` is clean, `cargo test --test once` passes, and `charpente test --sim` gives
     order N charged twice, with the seed and the fix. Each row is the demo test that measures
     it;
   - the two commands with Task 4's GOLDEN block, verbatim;
   - what happened, including §2's honest note that the bug needs an upsert followed by an
     unconditional enqueue;
   - how to run it: clone, `cd examples/double-charge`, then the commands, with "charpente
     0.6.0" named.
4. `## Install`: `cargo install charpente-cli`. Rust 1.99 or newer, and `rustup update stable`
   if cargo refuses the install by name.
5. `## Use`: `charpente new`, `test --sim` and `test --mutants`, keeping the scaffold sentence
   `crates/charpente-cli/tests/new.rs:282` describes (delete the `tx.enqueue` line in
   `src/lib.rs` and the second check goes red with a seed that replays).
6. `## How it works`: one paragraph on one log, one commit point and the deterministic core,
   linking DESIGN.md.
7. `## Limits`, in plain sentences:
   - one box, with no sharding;
   - Rust only;
   - a technical preview;
   - what the sim doesn't run: the HTTP edge server never runs in it (the rehearsal's testimony
     census), so it drives the app's kernel, not its socket.
8. The docs table, as today.

Dropped or moved: the etymology, "The three guarantees", "Why now", "Scaling stance" (one Limits
line and a DESIGN link), and "Status" (one line). No slogans, closers, "structurally" or "3am".

- [ ] **Step 1:** Add `readme_names_the_demos_rust_version_and_its_release` to `demo.rs`.
  README's Install section names the demo `Cargo.toml`'s `rust-version`, and README names
  `0.6.0` beside the demo. Run it. Expected: FAIL on today's README.
- [ ] **Step 2:** Write README. Run `-p guide-examples` (`claims_census`, `doc_truth`) and
  `demo.rs`, adjusting `CLAIMS` and `EXEMPT` rows so every figure is seed-bearing or in BENCH.md.
  Then read README top to bottom against MANIFESTO §3, §6 and §8, and against the anti-goals in
  spec §3.
- [ ] **Step 3:** CHANGELOG 0.6.0: Breaking (the human failure format: before-shrinking, the
  shrink line, condensed skips, "injected"), Added (`examples/double-charge`), Docs (README).
  LEDGER: a "the launch tape, built" entry with Task 4's 32-seed table. Commit: `docs: README
  in retrace's shape — the double charge, what says no to it, install, use, how it works,
  limits`.

### Task 6: Evidence and the release candidate (controller)

- [ ] **Step 1: The gate,** in the `charpente-gate` worktree at HEAD: fmt, clippy `-D warnings`,
  and `cargo test --workspace --no-fail-fast` counted (105 `test result: ok` expected: 104
  before, plus `demo`), plus the edge harness.
- [ ] **Step 2: Fresh seeds,** for the format change: `--runs 1000` under a fresh master on
  sim-fixture, invoicing, outbound-fixture and the scaffold, all exit 0. Also `--jobs 1` against
  `--jobs 8` on outbound-fixture: byte-identical JSON.
- [ ] **Step 3: Mutation:** `scripts/mutants.sh --jobs 1 --file
  crates/charpente-sim/src/harness.rs --re 'human_failure_block|same_failure|write_human'
  --test-package charpente-sim`. No MISSED. A survivor gets a test, shown red by hand.
- [ ] **Step 4: Final whole-branch review** (opus), then one fix wave and a scoped re-review.
- [ ] **Step 5:** `cargo publish --workspace --dry-run` exits 0. Ask Noah for the push and
  the publish. Both are his; the 0.5.0 authorization doesn't carry over.

### Task 7 (Noah): Push and publish 0.6.0

- [ ] `git push origin main`, CI green, then `cargo publish --workspace`.

### Task 8: After the publish (controller)

- [ ] **Step 1:** The registry smoke on stable and on 1.99: install, `--version` prints 0.6.0,
  `new` writes `"0.6"`, clippy, and sim. Date CHANGELOG, PLAN and README's dated-section line.
- [ ] **Step 2:** `cd examples/double-charge && cargo generate-lockfile` against crates.io, and
  commit the `Cargo.lock`. In a fresh clone, with `charpente-cli` 0.6.0 installed from crates.io,
  `charpente test --sim --master-seed 0x434841524745` prints Task 4's GOLDEN byte for byte.
  A difference stops the tape: find out why first.
- [ ] **Step 3:** LEDGER entry, commit, push. The push is covered by Noah's Task 7 approval for
  this release's records.

### Task 9: The tape

**Files:**
- Create: `docs/demo/double-charge.tape`. Renders go to `$SDD/`, not git.

- [ ] **Step 1:** Write the tape from retrace's `.superpowers/sdd/2026-11-17-hnprep/visits.tape`,
  with Global Constraints' geometry and theme.
  - Hidden setup: a clone at a short path (`/Users/Shared/double-charge`), with `charpente`
    0.6.0 from crates.io, a warm build (`charpente test --sim --runs 1`), and `clear`.
  - Shown: `# An order API: POST /orders/:id charges a card through a payment gateway.`, then
    `charpente test --sim --master-seed 0x434841524745`, waiting on `/each_order_is_charged_once/`.
  - Then `Ctrl+L`, `# Same seed, same double charge.`, and the printed `repro:` line typed
    verbatim (`Type@10ms` is allowed for it).
  - Optionally the fix and a green run, if the whole stays at 40 s or under.
  - The header says that every line on screen is real output from charpente 0.6.0 as installed
    from crates.io, and that nothing is edited.
- [ ] **Step 2:** Render with `vhs`. Extract the frame at the red block's end with ffmpeg and
  read it. The `sim failure in` line and the `sim result:` line are both on screen, and nothing
  is cut. Record the video's length.
- [ ] **Step 3:** Commit the tape. Give Noah the MP4's path in `$SDD/` to upload.

### Task 10: The video link (Noah, then controller)

- [ ] Noah uploads the MP4 through GitHub's web UI and sends the user-attachments URL. The
  controller puts it in README's video line, commits, and pushes. crates.io gets it with the
  next release.
