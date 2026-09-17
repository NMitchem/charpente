# Third Docs-Only Build Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-run the M0 exit criterion against the platform at `main`: a sealed Opus agent builds a status page from README + GUIDE alone, in its own repository scaffolded by `charpente new`, and the result is judged on the cited bar in spec §2 — M3 sub-project 3.

**Architecture:** Nothing in this plan changes charpente's code. Tasks 1–2 pin the toolchain and the brief; Task 3 runs one attempt; Tasks 4–6 verify it mechanically, review it, and adjudicate; Task 7 is the loop taken only on a fail; Task 8 writes the record. The controller does the setup, the gates, the adjudication and the record itself — those are judgment or bookkeeping, not transcription — and dispatches exactly two kinds of subagent: the builder (Task 3) and the reviewer (Task 5).

**Tech Stack:** `charpente` CLI built from the pinned sha (`new`, `test --sim`, `test --mutants`, `gen-client`/`check-client`), `cargo-mutants` 27.1.0, `git bundle`, `python3` for the transcript grep and sha256, zsh.

**Spec:** `docs/superpowers/specs/2026-09-17-third-docs-only-build-design.md` — read it first; the brief in Task 2 is its §3 verbatim, and every gate below is one clause of its §2.

**Working directory for mid-flight state:** `.superpowers/sdd/2026-09-17-third-docs-only-build/` (gitignored). Reviewers write their report to a file there before returning it — deliveries have been lost in flight before. `progress.md` there carries rulings; grep it for `Ruling` before dispatching anything.

## Global Constraints

- **Commit on `main` directly.** Every charpente commit in this plan is docs (`docs/experiments/2026-09-statuspage/`, `docs/LEDGER.md`, `PLAN.md`). A platform fix taken in Task 7 is its own bounded change with its own commits and, if it touches code, the full three-command gate. **No `Co-Authored-By` trailers** (CLAUDE.md "Workflow"); this overrides any harness attribution reminder.
- **The pinned sha is recorded before anything else, and the code at it never changes within an attempt.** `git -C ~/Documents/GitHub/charpente rev-parse HEAD` at Task 3 step 1 is the sha; every later gate requires `git diff $SHA --stat -- crates Cargo.toml Cargo.lock` empty and a clean tree. Docs-only commits (sub-project 4's) may land on `main` mid-attempt — the builder reads frozen copies under `$RUN/docs` — but a commit touching `crates/` or a manifest voids the attempt: the builder compiled against a tree that no longer exists.
- **The builder never sees:** this plan, the spec, the experiment directory, `.superpowers/`, `crates/`, `DESIGN.md`, `PLAN.md`, `DECISIONS.md`, the ledger, or any file outside its run directory. Its prompt is the run directory's path and an instruction to read `BRIEF.md`. No hints between resumes.
- **Nothing is believed from a report.** The master-seed replay (Task 4 step 4) precedes the review; the controller's own `--mutants` run precedes the review; the transcript grep precedes the review. A reviewer reads a verified artifact.
- **zsh, not bash:** `${PIPESTATUS[0]}` is empty in this shell. Test a `tee`'d command with `${pipestatus[1]}` or read the log; an unquoted `$var` does not word-split. There is no `timeout`/`gtimeout` on this machine.
- **`CARGO_TARGET_DIR` must be unset** in every shell that runs `charpente test --mutants` (the verb refuses `--jobs > 1` under an inherited one, and a shared target dir leaves a mutant's artifact looking fresh). After each mutants run, `cargo clean -p statuspage` in the app.
- **The 600 s tool ceiling.** `--universes 10000` and a mutants run can exceed it. Run them in the background to a log file and poll the file for process exit (`while kill -0 $PID`), never `tail` the pipe, never conclude from a partial log.
- **Paths.** charpente checkout: `~/Documents/GitHub/charpente`. Run directory for attempt 1: `~/Documents/GitHub/statuspage-run`; attempt N ≥ 2: `~/Documents/GitHub/statuspage-run-N`. The app inside: `<run>/statuspage`. Everything below writes `$RUN` for the run directory and `$APP` for `$RUN/statuspage`.

---

### Task 1: Preflight — the scaffold builds, tests, mutates and generates a client against the local crates

**Files:**
- Create: `.superpowers/sdd/2026-09-17-third-docs-only-build/preflight.md` (gitignored record of what worked)
- Scratch (deleted at the end): `~/Documents/GitHub/preflight-scratch/preflight/`

**Interfaces:**
- Produces: the exact dependency form (the `[patch.crates-io]` block, or the path-dep fallback) that Task 2 pastes into the brief verbatim; confirmation that `charpente` on PATH is built from the pinned sha and that all four verbs the bar needs work on an out-of-tree app.

- [ ] **Step 1: Pin the sha and install the CLI from it**

```bash
cd ~/Documents/GitHub/charpente && git status --porcelain | wc -l   # expect 0
SHA=$(git -C ~/Documents/GitHub/charpente rev-parse HEAD); echo $SHA
cargo install --path ~/Documents/GitHub/charpente/crates/charpente-cli --locked --force 2>&1 | tail -3
which charpente          # expect ~/.cargo/bin/charpente
cargo mutants --version  # expect cargo-mutants 27.1.0; if absent: cargo install cargo-mutants --version 27.1.0 --locked
```

Expected: tree clean, `charpente` resolves to `~/.cargo/bin/charpente`, cargo-mutants 27.1.0.

- [ ] **Step 2: Scaffold in a sibling scratch directory and append the patch block**

The scratch directory sits at the same depth as `$RUN` will, so the relative paths in the block are the ones the brief carries.

```bash
mkdir -p ~/Documents/GitHub/preflight-scratch && cd ~/Documents/GitHub/preflight-scratch
charpente new preflight && ls preflight preflight/src preflight/tests
cat >> preflight/Cargo.toml <<'TOML'

[patch.crates-io]
charpente = { path = "../../charpente/crates/charpente" }
charpente-sim = { path = "../../charpente/crates/charpente-sim" }
TOML
```

Expected: six files (`Cargo.toml`, `clippy.toml`, `.gitignore`, `src/lib.rs`, `src/main.rs`, `tests/sim.rs`).

- [ ] **Step 3: Prove the two gates GUIDE §1 names**

```bash
cd ~/Documents/GitHub/preflight-scratch/preflight
cargo clippy --all-targets -- -D warnings 2>&1 | tail -3
charpente test --sim 2>&1 | tail -5
```

Expected: clippy `Finished` with no errors; `--sim` prints `sim: N universe(s) per test from master seed 0x…` on stderr and exits 0 with the scaffold's two properties green.

If `cargo` fails to resolve `charpente = "0.1"` (the crates.io index has the reserved name at a different version and `[patch]` cannot match), apply the fallback and re-run this step:

```bash
cd ~/Documents/GitHub/preflight-scratch/preflight
sed -i '' 's|^charpente = "0.1"$|charpente = { path = "../../charpente/crates/charpente" }|' Cargo.toml
sed -i '' 's|^charpente-sim = "0.1"$|charpente-sim = { path = "../../charpente/crates/charpente-sim" }|' Cargo.toml
grep -n 'charpente' Cargo.toml
```

Record which form worked. Only one form goes into the brief.

- [ ] **Step 4: Prove the mutants verb and the client verbs work out of tree**

```bash
cd ~/Documents/GitHub/preflight-scratch/preflight
unset CARGO_TARGET_DIR
charpente test --mutants --dir . --file src/lib.rs --test-package preflight --timeout 300 2>&1 | tee mutants.log | tail -8
echo "MUTANTS_EXIT=${pipestatus[1]}"
charpente gen-client --dir . 2>&1 | tail -2 && ls client.ts
charpente check-client --dir . ; echo "CHECK_CLIENT_EXIT=$?"
```

Expected: the mutants verb prints a `mutants: N total — …` line and exits 0 or 1 (both mean it ran; 2 means it could not, which blocks the bar's clause (c) and is a finding to fix before dispatch); `gen-client` writes `client.ts`; `check-client` exits 0.

- [ ] **Step 5: Record and delete the scratch**

Write `.superpowers/sdd/2026-09-17-third-docs-only-build/preflight.md` with: the sha, the dependency form that worked (the exact lines), the mutants summary line and exit, the check-client exit. Then:

```bash
rm -rf ~/Documents/GitHub/preflight-scratch
```

No commit — nothing in charpente changed.

---

### Task 2: The brief and the record's skeleton

**Files:**
- Create: `docs/experiments/2026-09-statuspage/BRIEF.md`
- Create: `docs/experiments/2026-09-statuspage/README.md` (skeleton: status, sha, brief hash — the write-up lands in Task 8)

**Interfaces:**
- Produces: `BRIEF.md`, whose sha256 is the identity of "the same brief" across attempts; every attempt copies this file byte for byte.

- [ ] **Step 1: Write the brief**

In the shape of `docs/experiments/2026-09-helpdesk/BRIEF.md`. The dependency block is the one Task 1 proved — the `[patch.crates-io]` form is shown; substitute the fallback lines if that is what worked.

```markdown
# statuspage — product brief for a docs-only build (requirements, never implementation)

You are building a small backend on the charpente platform, from its documentation alone.
Your ONLY sources are this brief, `docs/README.md` and `docs/GUIDE.md` in the directory
you are in. Do not read anything outside this directory — not the platform's source, not
its other docs, not its examples. If the docs do not tell you how to do something, record
that in `FINDINGS.md` and choose the simplest thing the docs DO support — never patch the
framework. Every commit touches `statuspage/` only.

## Setup
The `charpente` CLI is on your PATH. Run `charpente new statuspage` in this directory,
then `git init` inside `statuspage/` — the scaffold does neither. Append these lines to
the scaffold's `Cargo.toml` exactly as written; they point the two dependencies at a
local checkout instead of the registry, and they are the only lines you may add that
name a path outside this directory:

    [patch.crates-io]
    charpente = { path = "../../charpente/crates/charpente" }
    charpente-sim = { path = "../../charpente/crates/charpente-sim" }

## Domain
- Orgs. Operators belong to exactly one org and authenticate by session login and API
  keys as GUIDE §5 describes.
- Components belong to an org and carry a current status: Operational, Degraded, Down.
- Incidents belong to an org, affect one or more of its components, and move
  Investigating → Identified → Monitoring → Resolved. Any other transition is an
  application error with a message an operator could act on. Incident updates (a posted
  note) belong to an incident.
- A subscriber is one email address across every page it follows: one row, however many
  orgs it subscribes to. A subscription joins a subscriber to an org.

## Public surface
Anyone, with no credential, can:
- read an org's current status — its components and its unresolved incidents — by the
  org's id in the path;
- subscribe an email address to an org's page;
- stream that org's incident events (`IncidentOpened`, `IncidentUpdated`,
  `IncidentResolved`) over SSE with exact `Last-Event-ID` resume (GUIDE §11).

No public read, feed, or notification may ever carry another org's rows. Every other
write is operator-only and scoped to the operator's own org: no route may return, modify,
or count another org's rows.

## Behaviours
- Resolving an incident, in ONE transaction: marks it Resolved, enqueues a
  notify-subscribers job, and emits `IncidentResolved`. The job writes one notification
  row per subscription of that org (a table; no external IO).
- Maintenance windows: an operator schedules a window (start, end, components). A cron
  tick opens a maintenance incident when a window's start passes and resolves it when its
  end passes. Each window opens at most once and resolves at most once, never before its
  start or its end.
- `Incident` ships at `version = 1`. A second, separate commit bumps it to `version = 2`,
  adding a `severity` field (Minor, Major, Critical) with a migration (GUIDE §12). Old
  rows read back with a default severity.
- The TypeScript client: `charpente gen-client` output committed; `charpente
  check-client` exits 0 (GUIDE §14).

## Proof (the part that matters)
`tests/sim.rs` using the platform's simulation harness, with properties for:
- acked writes are durable across crashes; replay is identical from a seed;
- notifications ≡ resolved incidents × that org's subscriptions at resolve time, exactly,
  under the full fault matrix;
- no org ever observes another org's components, incidents, subscriptions, or events —
  on the public page, in the feed, and in notifications — under the fault matrix;
- maintenance windows: each opened at most once and resolved at most once, never early,
  over virtual weeks.

`charpente test --sim --universes 10000` must be green; record the master seed it prints.
Then run `charpente test --mutants` (GUIDE §8) over the files carrying the job, the cron
sweep, and the incident state machine, with `--test-package statuspage`, and record its
full output in `FINDINGS.md`. Every mutant caught or unviable is the target; a MISSED is a
one-sided property to fix before you are done.

## Deliverables
- `statuspage/` as its own git repository, building with `cargo test` and
  `cargo clippy --all-targets -- -D warnings`.
- `FINDINGS.md` in the repository: a friction ledger in your own words, dated as you go;
  how many times you restarted an approach; every GUIDE section you had to re-read and
  why; what you could not find in the docs; the master seed; the `--mutants` output.
- Commit as you go, small commits, named paths.
```

- [ ] **Step 2: Hash it and write the README skeleton**

```bash
cd ~/Documents/GitHub/charpente
python3 -c "import hashlib,sys;print(hashlib.sha256(open('docs/experiments/2026-09-statuspage/BRIEF.md','rb').read()).hexdigest())"
```

`README.md`:

```markdown
# The docs-only status-page experiment (2026-09-17 → )

M3 sub-project 3 (PLAN.md; spec `docs/superpowers/specs/2026-09-17-third-docs-only-build-design.md`).
An agent is given `BRIEF.md`, `README.md` and `docs/GUIDE.md` — nothing else — and asked
to build a status page on charpente in its own repository. The point is not the app. It is
whether the platform, which now asks a route to say `auth` or `public` and a table to say
`scoped_by` or `global`, leads a stranger to a sound shape, and whether its oracles catch
what they claim to.

**Status: attempt 1 in flight.** This file is completed at close-out.

| | |
|---|---|
| Brief sha256 | `<hash from step 2>` |
| Attempt 1 pinned sha | (Task 3) |
| Dependency form | (Task 1's result: `[patch.crates-io]` or path deps) |
```

- [ ] **Step 3: Commit**

```bash
cd ~/Documents/GitHub/charpente
git add docs/experiments/2026-09-statuspage/BRIEF.md docs/experiments/2026-09-statuspage/README.md
git commit -m "experiment(statuspage): the brief for the third docs-only build, pinned by hash before any attempt"
```

---

### Task 3: Attempt N — set up the run directory and dispatch the builder

**Files:**
- Create: `$RUN/BRIEF.md`, `$RUN/docs/README.md`, `$RUN/docs/GUIDE.md` (copies)
- Modify: `.superpowers/sdd/2026-09-17-third-docs-only-build/progress.md` (attempt N: sha, dispatch time, output-file path, resume count)

**Interfaces:**
- Consumes: `BRIEF.md` from Task 2 (its hash is checked); the dependency form from Task 1 (already inside the brief).
- Produces: `$APP` — the builder's repository — and the builder's transcript path, which Task 4 greps.

- [ ] **Step 1: Pin, verify, and lay out the run directory**

```bash
RUN=~/Documents/GitHub/statuspage-run          # attempt N ≥ 2: statuspage-run-N
cd ~/Documents/GitHub/charpente && git status --porcelain | wc -l   # expect 0
SHA=$(git rev-parse HEAD); echo "attempt sha $SHA"
mkdir -p $RUN/docs
cp docs/experiments/2026-09-statuspage/BRIEF.md $RUN/BRIEF.md
cp README.md $RUN/docs/README.md
cp docs/GUIDE.md $RUN/docs/GUIDE.md
python3 -c "import hashlib;print(hashlib.sha256(open('$RUN/BRIEF.md','rb').read()).hexdigest())"   # must equal the hash in the record's README
ls -la $RUN $RUN/docs
```

Expected: exactly `BRIEF.md` and `docs/{README.md,GUIDE.md}`; the hash matches. Write the sha and the time into progress.md under `Attempt N`.

- [ ] **Step 2: Dispatch the builder**

Agent tool, `subagent_type: general-purpose`, `model: opus`, **no `name`** (a named dispatch routes through the teammate protocol and drops its return path). The prompt, verbatim, with `$RUN` expanded:

```
Your working directory is <RUN>. Read BRIEF.md there and do what it says; your only
sources are the three files it names, and you must not read anything outside that
directory. Work until every deliverable the brief lists exists, committing as you go.

Tooling notes (about this machine, not about the task): the shell is zsh; a command may
not run longer than ten minutes in the foreground, so run long builds and test batches in
the background writing to a log file, poll until the process has exited, and read the
whole log before concluding anything — never conclude from a partial log. When you are
done, reply with the path of the repository, the master seed the brief asks you to
record, and the last line of your FINDINGS.md.
```

Record the launch result's `output_file` path in progress.md — that JSONL is the transcript Task 4 greps, and it lives in a session-scoped temp directory.

- [ ] **Step 3: Wait; resume on a stall, never restart**

When the notification arrives, check the three deliverables exist: `$APP/.git`, `$APP/FINDINGS.md`, `$APP/tests/sim.rs`, and that FINDINGS.md carries a master seed and a `mutants:` summary line. If the agent stopped short — the discriminator is *no live cargo process under `$APP` and no final report* (`pgrep -fl "cargo|charpente" | grep statuspage`) — resume the **same** agent by SendMessage with exactly:

```
continue from your FINDINGS.md and git log
```

Count each resume in progress.md. Do not edit anything under `$RUN` between resumes. Do not answer questions; a question the agent asks is testimony, and the answer it does not get is the point.

While the builder runs, **no `cargo` runs in charpente's tree and nothing under `crates/` or a manifest changes** (spec §5, Global Constraints). Docs-only commits are fine — the builder's docs are the frozen copies under `$RUN/docs`.

- [ ] **Step 4: Freeze the attempt**

```bash
cp <output_file> $RUN/transcript.jsonl
git -C $APP log --oneline | tee $RUN/git-log.txt | wc -l
git -C $APP status --porcelain | wc -l     # expect 0; an uncommitted tree is noted, not fixed
git -C $APP bundle create ~/Documents/GitHub/statuspage-attempt-N.bundle --all
```

No charpente commit.

---

### Task 4: Mechanical gates — frame, app gates, replay, mutants

**Files:**
- Create: `.superpowers/sdd/2026-09-17-third-docs-only-build/attempt-N/gates.md`
- Create: `.superpowers/sdd/2026-09-17-third-docs-only-build/attempt-N/replay.json`, `agent-run.json` (if the agent kept one), `mutants-*.log`

**Interfaces:**
- Consumes: `$APP`, `$RUN/transcript.jsonl`, the attempt sha from Task 3.
- Produces: `gates.md` with a PASS/VOID/FAIL per clause of spec §2(a), (c), (d); Task 5 does not start until every line is PASS.

- [ ] **Step 1: Frame — the tree is unchanged and the builder stayed inside**

```bash
cd ~/Documents/GitHub/charpente
git status --porcelain | wc -l                                          # expect 0
git diff $SHA --stat -- crates Cargo.toml Cargo.lock | wc -l            # expect 0: the code the builder compiled is the code at the sha
```

Transcript grep — prints every tool call that named a path outside the run directory, for a human ruling; a genuine read voids the attempt, a `cargo` build touching the patched crates does not:

```bash
python3 - "$RUN" <<'PY'
import json, sys, os
run = os.path.abspath(sys.argv[1])
hits = []
for line in open(os.path.join(run, "transcript.jsonl")):
    try: m = json.loads(line)
    except ValueError: continue
    msg = m.get("message") or m
    if msg.get("role") != "assistant": continue
    for b in msg.get("content") or []:
        if not isinstance(b, dict) or b.get("type") != "tool_use": continue
        name, inp = b.get("name"), b.get("input") or {}
        text = " ".join(str(v) for v in inp.values())
        outside = any(k in text for k in ("/charpente/crates", "/charpente/docs", "/charpente/DESIGN", "/charpente/PLAN", "/charpente/DECISIONS", ".superpowers", "/charpente/examples", "../charpente"))
        readlike = name in ("Read", "Glob", "Grep", "LS") or (name == "Bash" and any(c in text for c in ("cat ", "sed ", "grep ", "ls ", "head ", "tail ", "find ", "rg ")))
        if outside and readlike:
            hits.append((name, text[:200]))
print(f"{len(hits)} suspicious call(s)")
for n, t in hits: print(f"  {n}: {t}")
PY
```

Expected: `0 suspicious call(s)`, or a list the controller rules on in gates.md. The brief's own patch line inside an `Edit`/`Write` of `Cargo.toml` is a write, not a read, and the filter above excludes it; a `cat ../../charpente/crates/...` is a read and voids.

- [ ] **Step 2: App gates**

```bash
cd $APP
cargo clippy --all-targets -- -D warnings 2>&1 | tail -3 ; echo "CLIPPY_EXIT=${pipestatus[1]}"
cargo test 2>&1 | grep -c '^test result: ok' ; cargo test 2>&1 | grep -c '^test result: FAILED'
charpente check-client --dir . ; echo "CHECK_CLIENT_EXIT=$?"
git -C $APP log --oneline | grep -i 'version = 2\|severity\|migrat'   # the separate migration commit
```

Expected: clippy 0, `cargo test` all `ok` and 0 FAILED, check-client 0, one commit that is the `version = 2` bump.

- [ ] **Step 3: Extract the agent's recorded run**

```bash
grep -n -i 'master seed' $APP/FINDINGS.md | head -3
MSEED=<the 0x… value>
grep -n '^mutants:' $APP/FINDINGS.md
```

If FINDINGS.md carries no master seed, the brief's definition of done was not met: gates.md says FAIL at §2(d) and Task 5 still runs (the review is testimony either way), but the verdict cannot be a pass.

- [ ] **Step 4: Replay the master seed — the precondition for believing anything**

```bash
cd $APP
( charpente test --sim --universes 10000 --master-seed $MSEED --format json > $RUN/replay-1.json 2> $RUN/replay-1.err; echo "EXIT=$?" >> $RUN/replay-1.err ) &
```

Poll until the process exits; then run it a **second** time to `replay-2.json` the same way. Compare:

```bash
cmp $RUN/replay-1.json $RUN/replay-2.json && echo REPLAY_IDENTICAL || echo REPLAY_DIFFERS
python3 -c "import json;r=json.load(open('$RUN/replay-1.json'));print('failures',len(r['failures']),'verdicts',r['verdicts'],'gave_up',r['gave_up'],'universes',r['universes'])"
grep 'master seed' $RUN/replay-1.err
```

Expected: `REPLAY_IDENTICAL`, `failures 0`, `universes 10000`. If the agent left its own JSON, `cmp` against that too. `REPLAY_DIFFERS` → VOID at §2(d): the app or the harness is nondeterministic, which is a platform finding to chase before anything else (a `Sim::new(seed)` built inside a checker, a `HashMap` iteration, a wall clock reaching the app through a route) — record it, do not proceed to Task 5.

- [ ] **Step 5: The controller's own mutants run**

```bash
cd $APP && unset CARGO_TARGET_DIR
grep -ln '#\[job\|#\[cron\|Investigating\|Identified\|Monitoring' src/*.rs src/**/*.rs 2>/dev/null
```

For every file listed (expect 1–3):

```bash
( charpente test --mutants --dir . --file src/<f>.rs --test-package statuspage --timeout 600 > $RUN/mutants-<f>.log 2>&1; echo "EXIT=$?" >> $RUN/mutants-<f>.log ) &
```

Poll until exit; then:

```bash
grep -h '^mutants:\|^MISSED\|^TIMEOUT\|^EXIT=' $RUN/mutants-*.log
cargo clean -p statuspage
```

Expected: every log ends `EXIT=0` and its `mutants:` line reads `0 missed, 0 timed out`. A MISSED is FAIL at §2(c), quoted in gates.md with the mutant's description. A `timed out` is re-run once with `--timeout 1200`; if it still times out it is recorded as unresolved and the attempt cannot pass until it resolves. Compare the counts with the agent's recorded `mutants:` line; a disagreement is noted (the agent may have run over different files).

- [ ] **Step 6: Write gates.md**

One line per clause: `§2(a) framework patches: PASS (sha unchanged, 0 suspicious calls)`; `§2(c) mutants: PASS/FAIL (<counts per file>)`; `§2(d) replay: PASS/VOID (<failures, verdicts, universes>)`; plus the app-gate exits. Every FAIL/VOID quotes the output that produced it.

---

### Task 5: The review — code first, blind to FINDINGS

**Files:**
- Create: `.superpowers/sdd/2026-09-17-third-docs-only-build/attempt-N/review.md` (written by the reviewer before it returns)

**Interfaces:**
- Consumes: `$APP` (the reviewer is told not to open `FINDINGS.md`), `$RUN/BRIEF.md`, `~/Documents/GitHub/charpente/docs/GUIDE.md`, `gates.md`.
- Produces: `review.md` with Critical/Important/Minor findings; every Critical names a platform claim or says `no claim`.

- [ ] **Step 1: Dispatch the reviewer**

Agent tool, `subagent_type: general-purpose`, `model: opus`, no `name`. Prompt:

```
You are reviewing an application built on the charpente platform by another agent from
the platform's documentation alone. Review the CODE, not the story: do not open
<APP>/FINDINGS.md — it is the builder's own account and would anchor you.

Read, in this order: <RUN>/BRIEF.md (what was asked), <charpente>/docs/GUIDE.md §2, §5,
§8, §11, §12, §16 (what the platform promises and refuses), then every file under
<APP>/src and <APP>/tests. <gates.md path> holds the mechanical results already
established: sha unchanged, replay identical, the controller's --mutants counts.

Report findings as Critical / Important / Minor. A Critical is a defect that violates the
brief's stated requirements or the platform's own guarantees — an unauthenticated write, a
cross-tenant read on any path (page, feed, notification, job), a lost or duplicated
notification under faults, a property that would stay green if the code it guards were
deleted, a state transition the brief forbids. For EVERY Critical, state which platform
claim, if any, should have refused it: cite a README guarantee by number, a GUIDE section
and the sentence, a compile-time refusal (quote the error the macro would print), a
`charpente test --sim` property class, or `charpente test --mutants` — or write "no
claim" if this is a domain-logic bug that no platform oracle claims to catch. That
citation is what the verdict turns on, so be exact and be honest about "no claim".

Pay particular attention to how the app reconciles anonymous reads and authenticated
writes on one org's rows (GUIDE §16's `authorize` receives `Option<&Principal>`): describe
the shape it chose, whether it is sound, and whether GUIDE §16 as written leads to it.

Write the FULL review to <review.md path> FIRST, then return a summary of at most fifteen
lines: counts per severity, each Critical in one line with its citation, and the shape
chosen for public reads.
```

- [ ] **Step 2: Verify the review exists and re-run its claims**

`review.md` present; every Critical's citation resolved by the controller against the file it names (`grep -n` the quoted sentence in GUIDE; open the macro's error string in `crates/charpente-macros/src/`; run the mutant it names if it names one). A citation that does not resolve is corrected in progress.md, not accepted.

---

### Task 6: Adjudication, testimony, and the verdict to Noah

**Files:**
- Modify: `.superpowers/sdd/2026-09-17-third-docs-only-build/progress.md` (rulings)
- Create: `.superpowers/sdd/2026-09-17-third-docs-only-build/attempt-N/verdict.md`

**Interfaces:**
- Consumes: `gates.md`, `review.md`, `$APP/FINDINGS.md`.
- Produces: `verdict.md` — PASS or FAIL with every ruling cited — and Noah's sign-off recorded in progress.md.

- [ ] **Step 1: Rule on each Critical**

For each Critical in review.md, one ruling in progress.md:

```
Ruling <n> (attempt N, Critical <k>): <one-line defect>. Claim: <citation or "no claim">.
  Holds? <yes/no + why, from the cited text>. → PLATFORM FINDING (fails §2(b)) | AGENT BUG (testimony).
```

A Critical fails the bar only when the cited claim holds — the platform's own docs say it catches that class. When the reviewer wrote "no claim" but the controller finds one, the controller's citation governs; the reverse too.

- [ ] **Step 2: Read FINDINGS.md as testimony**

For every sentence in FINDINGS.md that asserts something about charpente (a missing section, an error that was wrong, a checker that did not do what its docs say), check it against the code and the docs at the pinned sha, and classify it as the helpdesk write-up did: **right**, **wrong** (with the file:line that refutes it), or **right about a symptom for the wrong reason** (with the mechanism). List every "right" as a platform finding in verdict.md — they do not fail the bar, but they are what sub-project 3 is for.

- [ ] **Step 3: Write verdict.md and put it to Noah**

`verdict.md`: `PASS` or `FAIL`; the gates table from Task 4; each Critical with its ruling; the public-read shape the app chose and whether GUIDE §16 led it there; the testimony list; the resume count; the mutants counts. Then present it to Noah — the verdict is his to sign — and record his answer in progress.md as `Ruling: verdict attempt N signed <PASS/FAIL> by Noah <date>`. **Do not write the ledger or PLAN before this ruling exists.**

---

### Task 7: On a FAIL only — fix the platform, re-run the same brief

**Files:**
- Modify: whatever the finding names in charpente — this task cannot know in advance; it is a sub-plan.
- Create: the fix's pin (a compile-fail fixture under `crates/charpente-macros/tests/compile_fail/`, a sim property, or a `<!-- pinned -->` block in GUIDE held by `doc_truth.rs`).

**Interfaces:**
- Consumes: `verdict.md`'s first platform finding — the first oracle that should have refused and did not.
- Produces: a new `main` sha with the fix and its pin, and a ledger entry naming the attempt that found it; then Task 3 re-enters as attempt N+1.

- [ ] **Step 1: Classify the fix and brainstorm it**

Invoke `superpowers:brainstorming` on the finding. Bounded (a refusal message, a GUIDE section, a checker's documented behaviour) → short design in chat and Noah's yes. Architectural (a new capability, a change to what `authorize` receives, anything touching DESIGN §7/§8) → its own spec. If the fix contradicts a DECISIONS.md entry, that entry is reopened for Noah's sign-off before any code.

- [ ] **Step 2: Land it with its pin, green**

TDD: the pin is written first and shown red against the pinned sha, then the fix makes it green. For code: the three-command gate, counted (`grep -c "^test result: ok"` on a `tee`'d log, zero FAILED). For a GUIDE-only fix: `cargo build -p guide-examples` and `cargo test -p guide-examples`. Commit on `main` with docs in the same commit (hard rule 6). Ledger entry: `## <date> — <finding>, found by the status-page build, attempt N`.

- [ ] **Step 3: Re-enter Task 3 as attempt N+1**

New `$RUN` (`statuspage-run-<N+1>`), new pinned sha, `docs/` re-copied (GUIDE may have changed; README may have), `BRIEF.md` byte-identical — the hash check in Task 3 step 1 proves it. Then Tasks 4–6 again. Each attempt's `attempt-N/` directory is kept.

---

### Task 8: The record, the ledger, PLAN — and the final gate

**Files:**
- Modify: `docs/experiments/2026-09-statuspage/README.md` (the write-up)
- Create: `docs/experiments/2026-09-statuspage/attempt-N/FINDINGS.md`, `attempt-N/review.md` (one pair per attempt)
- Modify: `docs/LEDGER.md` (append one entry per attempt not already ledgered, and the close-out)
- Modify: `PLAN.md:219` (sub-project 3's status bracket)

**Interfaces:**
- Consumes: every `attempt-N/` directory, `verdict.md`s, Noah's signed ruling, the bundle paths.
- Produces: the committed record; PLAN says shipped.

- [ ] **Step 1: Copy the testimony and the reviews into the record**

```bash
cd ~/Documents/GitHub/charpente
for d in .superpowers/sdd/2026-09-17-third-docs-only-build/attempt-*; do
  n=${d##*attempt-}
  mkdir -p docs/experiments/2026-09-statuspage/attempt-$n
  cp ~/Documents/GitHub/statuspage-run$( [ $n -gt 1 ] && echo -$n )/statuspage/FINDINGS.md docs/experiments/2026-09-statuspage/attempt-$n/FINDINGS.md
  cp .superpowers/sdd/2026-09-17-third-docs-only-build/attempt-$n/review.md docs/experiments/2026-09-statuspage/attempt-$n/review.md
done
```

FINDINGS.md is copied byte for byte — it is testimony, and editing it would make it something else. Strip nothing; if it names a local path, that is what the agent wrote.

- [ ] **Step 2: Write the README.md write-up**

Replace the skeleton with, in the shape of `docs/experiments/2026-09-helpdesk/README.md`: the framing paragraph; a table per attempt (pinned sha, dependency form, resume count, master seed, replay result, controller's mutants counts, app-gate exits, verdict, Noah's sign-off date); "Read FINDINGS.md as testimony" with every right/wrong/wrong-reason ruling and its file:line; the public-read shape the app chose and what GUIDE §16 led it to; the platform findings taken and their commits (Task 7); the app repository's final commit sha and where the bundle is; and one paragraph on what the brief asked for that the app did not deliver, if anything. Every hash and count in this file is copied from a file in `attempt-N/`, never from memory — `git show <sha> --numstat` before any commit hash is written.

- [ ] **Step 3: Ledger and PLAN**

Append to `docs/LEDGER.md`:

```markdown
## <date> — the third docs-only build: a status page, <N> attempt(s), <PASS>

<What was asked, in two sentences. The pinned sha(s). The verdict with each Critical's
ruling and citation. The shape the agent chose for public reads and whether §16 led it
there. The platform findings, each with the commit that fixed it and the attempt that
found it. The testimony rulings in one paragraph. The replay and mutants numbers, with
the seed. What stays open, deliberately. "Next is M3 sub-project 5, the launch run, once
sub-project 4 and the real publish are done.">
```

`PLAN.md:219`: `3. **Third docs-only build** [shipped <date>; record \`docs/experiments/2026-09-statuspage/\`]. A new domain — …` (the existing sentence continues unchanged).

- [ ] **Step 4: The gate, counted, and the commit**

Docs only, but `guide-examples` reads CLAUDE.md and GUIDE and the ledger is prose nobody compiles — run the full three commands in the background to a log and count:

```bash
cd ~/Documents/GitHub/charpente
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -1
( cargo test --workspace --no-fail-fast > /tmp/sp-final.log 2>&1; echo "EXIT=$?" >> /tmp/sp-final.log ) &
# poll for exit, then:
grep -c '^test result: ok' /tmp/sp-final.log; grep -c '^test result: FAILED' /tmp/sp-final.log; tail -1 /tmp/sp-final.log
```

Expected: 86 ok (or the current count — compare with `grep -c '^test result:'`), 0 FAILED, `EXIT=0`. Then:

```bash
git add docs/experiments/2026-09-statuspage docs/LEDGER.md PLAN.md
git commit -m "experiment(statuspage): the third docs-only build — <N> attempt(s), <verdict>; PLAN marks M3 sub-project 3 shipped"
```

Push is Noah's call.
