# Launch Run Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** M3 sub-project 5 and exit criterion (a): a sealed Opus agent, given only the published README and one sentence, reproduces README's "Try it" (install, scaffold, sim green, delete `tx.enqueue`, red with a seed, replay), every command it runs executing in a fresh container. First as a rehearsal against the packaged crates, then, after Noah publishes, against crates.io.

**Architecture:** Task 1 pins README's red half with a test, the only charpente code change. Tasks 2–3 build the run's instruments as bash scripts under the record directory: the `./box` wrapper and its log, the rehearsal's stand-in registry, the sealing check, the failure-block extractor and the controller's replay. Tasks 4–11 are the controller's: the preflight, the brief, each attempt, adjudication, the publish hand-off, the real run, and the record. Subagents implement Tasks 1–3. The controller runs everything else and dispatches exactly one other kind of subagent: the stranger (Tasks 6 and 10).

**Tech Stack:** Rust (one integration test in `charpente-cli`), bash and `jq` 1.7 for the tools (macOS `/bin/bash` 3.2 compatible: no associative arrays, no `mapfile`), Docker 29.7.2 (server on this Mac), the `rust` image pinned by digest, `cargo package` and `cargo vendor --versioned-dirs`.

**Spec:** `docs/superpowers/specs/2026-10-06-launch-run-design.md`. Read it first. The brief in Task 5 is its §3 verbatim, and every gate below is one clause of its §2.

**Mid-flight state:** `.superpowers/sdd/2026-10-06-launch-run/progress.md` (gitignored). Reviewers write their report to a file there before returning it, because deliveries have been lost in flight before. Grep `progress.md` for `Ruling` before dispatching anything.

## Global Constraints

- **Commit on `main` directly; no `Co-Authored-By` trailers** (CLAUDE.md "Workflow"). This overrides any harness attribution reminder.
- **Merges, pushes and remote deletes are Noah's**, run by him via `!`; the auto-mode classifier refuses them from the controller. Hand him the exact command.
- **The repo's toolchain is pinned** (`rust-toolchain.toml`, 1.99.0). The box's toolchain is whatever the pinned `rust` image ships. That is the stranger's, and it is never adjusted to match.
- **The stranger never sees** this plan, the spec, the record directory, `.superpowers/`, the charpente checkout, the box log, or any file outside its run directory. Its run directory holds exactly `BRIEF.md`, `README.md` and `box`.
- **Nothing is believed from a report.** Every bar item is read from the box log or reproduced by the controller.
- **zsh, not bash, in the controller's shell:** `${PIPESTATUS[0]}` is empty; use `${pipestatus[1]}`. An unquoted `$var` does not word-split. There is no `timeout`/`gtimeout`. The tools in Tasks 2–3 are bash scripts with a bash shebang; run them as `bash <script>` or by path, never `source` them into zsh.
- **The 600 s tool ceiling.** `cargo install charpente-cli` in a box, a full workspace gate and `replay.sh` can each exceed it. Run them in the background to a log file and wait for the process to exit; never conclude from a partial log.
- **Paths.** Checkout `~/Documents/GitHub/charpente` (`$REPO`). Record `$REPO/docs/experiments/2026-10-launch-run` (`$REC`). Tools `$REC/tools`. Rehearsal attempt N: run directory `~/Documents/GitHub/launch-rehearsal-N` (`$RUN`), box log `$RUN.box.jsonl`, container `launch-rehearsal-N`, registry `~/Documents/GitHub/launch-registry-N`. Real attempt N: `~/Documents/GitHub/launch-real-N`, `$RUN.box.jsonl`, container `launch-real-N`. A container name is never reused; `mkbox.sh` refuses an existing one.
- **The box log's line schema** (Task 2 produces it; Tasks 3, 4, 7 and 10 read it). Start: `{"seq":N,"event":"start","utc":"YYYY-MM-DDTHH:MM:SSZ","cwd":"…","command":"…"}`. End: `{"seq":N,"event":"end","utc":"…","exit":N,"stdout":"…","stderr":"…"}`.

## Review Focus

The inputs and conditions the spec implies but no gate below would otherwise exercise, most likely to bite first. Each one's test is in the task that owns the code.

1. **A command full of quotes, `$`, `&&` or newlines** passed to `./box` must run, and be logged, exactly as the agent wrote it. One level of quoting lost means the stranger's transcript records a command it never ran. (Task 2, selftest case 1.)
2. **A `./box` call the agent's tool timeout kills** must still be on record. A missing record reads as a sealing breach, or worse, as nothing happening. (Task 2, case 4: a start line with no end line.)
3. **Parallel `./box` calls**, which agents issue, must each get their own sequence number and both lines. (Task 2, case 5.)
4. **A batch with several failing universes:** the block compared in (c) and (d) must be the one whose seed was replayed, not just the first one printed. (Task 3, `block.sh` case with two blocks.)
5. **A Mac-side `cd <run> && ./box …`** is the normal shape of an agent's call and must not be flagged. A `cat ~/Documents/GitHub/charpente/…` must be. (Task 3, `seal-check.sh` cases.)

---

### Task 1: README's red half gets its test

**Files:**
- Modify: `crates/charpente-cli/tests/new.rs`. Add one test directly after `a_scaffolded_app_is_fmt_clean_clippy_clean_under_the_fence_and_green_under_the_sim` (which ends just before `/// The first ```toml block after GUIDE §1's heading, verbatim.`).

**Interfaces:**
- Consumes: the file's existing helpers `scratch`, `run_in`, `read`, `charpente`, `target_dir`.
- Produces: nothing other tasks call. Task 4's preflight relies on what it pins.

- [ ] **Step 1: Write the test**

Insert after the e2e test:

```rust
/// README's "Try it", red half: delete the scaffold's `tx.enqueue` and
/// `charpente test --sim` goes red on the second property with a seed, and
/// the seed replays. README calls this the whole pitch; until the launch-run
/// spec (2026-10-06, §5) nothing ran it, and the e2e test above proves only
/// the green half. The master seed fixes the batch, and the assertions name
/// the reason (ack-durability), so a re-keyed fault stream that still fails
/// for this reason keeps the pin meaningful.
#[test]
// Rewriting the scaffold's manifest and src/lib.rs is edge tooling
// (DESIGN.md §1): the rig making README's edit, not code the sim exercises.
#[allow(clippy::disallowed_methods)]
fn deleting_the_scaffolds_enqueue_turns_the_second_property_red_with_a_seed_that_replays() {
    let dir = scratch("red");
    let (code, _, err) = run_in(&dir, &["new", "red"]);
    assert_eq!(code, 0, "stderr: {err}");
    let app = dir.join("red");

    let ws = std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap();
    let mut manifest = read(&app.join("Cargo.toml"));
    manifest.push_str(&format!(
        "\n[patch.crates-io]\ncharpente = {{ path = {:?} }}\ncharpente-sim = {{ path = {:?} }}\n",
        ws.join("crates/charpente"),
        ws.join("crates/charpente-sim")
    ));
    std::fs::write(app.join("Cargo.toml"), manifest).unwrap();

    // README: "delete the `tx.enqueue` line in `src/lib.rs`".
    let lib = read(&app.join("src/lib.rs"));
    let line = "    tx.enqueue(Echo { note: id });\n";
    assert_eq!(
        lib.matches("tx.enqueue").count(),
        1,
        "README names one `tx.enqueue` line in the scaffold's src/lib.rs:\n{lib}"
    );
    std::fs::write(app.join("src/lib.rs"), lib.replacen(line, "", 1)).unwrap();

    let target = target_dir();
    let batch = charpente()
        .current_dir(&app)
        .env("CARGO_TARGET_DIR", &target)
        .args(["test", "--sim", "--master-seed", "0x1"])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&batch.stdout);
    assert_eq!(
        batch.status.code(),
        Some(1),
        "README: the second property goes red\nstdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&batch.stderr)
    );
    assert!(
        !stdout.contains("sim failure in acked_note_survives_a_restart"),
        "the first property does not depend on the enqueue:\n{stdout}"
    );
    let start = stdout
        .find("sim failure in acked_post_implies_the_echo_after_recovery: ")
        .unwrap_or_else(|| panic!("the second property is the one that fails:\n{stdout}"));
    let rest = &stdout[start..];
    let repro_at = rest.find("  repro:  ").expect("a failure block ends with its repro line");
    let end = repro_at + rest[repro_at..].find('\n').expect("the repro line ends");
    let block = &rest[..end];
    assert!(
        block.contains("ack-durability violated"),
        "and it fails for the reason the deletion causes:\n{block}"
    );

    let repro = block[repro_at..].trim_start().strip_prefix("repro:  ").unwrap();
    let args: Vec<&str> = repro.split_whitespace().collect();
    assert_eq!(args[0], "charpente", "{repro}");
    let replay = charpente()
        .current_dir(&app)
        .env("CARGO_TARGET_DIR", &target)
        .args(&args[1..])
        .output()
        .unwrap();
    let replayed = String::from_utf8_lossy(&replay.stdout);
    assert_eq!(replay.status.code(), Some(1), "the seed replays red:\n{replayed}");
    assert!(
        replayed.contains(block),
        "README: the seed replays — the same block, byte for byte\nbatch:\n{block}\nreplay:\n{replayed}"
    );
}
```

- [ ] **Step 2: Run it. It should pass, because the behaviour already exists.**

Run: `cargo test -p charpente-cli --test new -- deleting_the_scaffolds_enqueue --nocapture 2>&1 | tee /tmp/red.log; grep -c '^test result: ok' /tmp/red.log`
Expected: `1`. If the batch exits 0 because master seed `0x1` happens to produce no verdict on the second property, try `0x2`, `0x3`, … and pin the first seed that fails. Write the seed found, and why, into the doc comment's last sentence.

- [ ] **Step 3: Inversion 1, the deletion. It must go red.**

Temporarily change `lib.replacen(line, "", 1)` to `lib.replacen(line, line, 1)`. This runs the untouched scaffold.
Run: the Step 2 command.
Expected: FAIL with `README: the second property goes red`, exit `Some(0)`. Restore `lib.replacen(line, "", 1)`.

- [ ] **Step 4: Inversion 2, the replay operand. It must go red.**

Temporarily change `.args(&args[1..])` to `.args(["test", "--sim", "--filter", "acked_post_implies_the_echo_after_recovery", "--seed", "0x0"])`.
Run: the Step 2 command.
Expected: FAIL on either `the seed replays red` or `README: the seed replays`. A different universe never prints this block's `seed:` line. Restore `.args(&args[1..])`.

- [ ] **Step 5: Confirm the file is back to the Step 1 text, then gate**

Run: `git diff --stat crates/charpente-cli/tests/new.rs` (one file, insertions only), then:
`cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p charpente-cli --test new 2>&1 | tee /tmp/new.log; grep -c '^test result: ok' /tmp/new.log; grep -c FAILED /tmp/new.log`
Expected: `1` and `0`. Neither grep can match on its own, so read both numbers.

- [ ] **Step 6: Commit**

```bash
git add crates/charpente-cli/tests/new.rs
git commit -m "test(new): README's red half is pinned — delete the scaffold's tx.enqueue and the second property fails on ack-durability with a seed that replays byte for byte (launch-run spec §5); both inversions red"
```

---

### Task 2: The box: `./box`, `mkbox.sh`, and their selftest

**Files:**
- Create: `docs/experiments/2026-10-launch-run/tools/box.template`
- Create: `docs/experiments/2026-10-launch-run/tools/mkbox.sh`
- Create: `docs/experiments/2026-10-launch-run/tools/selftest-box.sh`

**Interfaces:**
- Produces: `bash mkbox.sh <container> <image> <run-dir> <log> [<registry-dir>]`. It creates the container, writes `<run-dir>/box` (executable), and truncates `<log>`. With `<registry-dir>` the container has `--network none`, and crates.io is replaced by the directory source copied to `/opt/registry`. Without it, the default bridge network. Every box call appends the two-line schema in Global Constraints to `<log>`.

- [ ] **Step 1: Write the selftest first**

`selftest-box.sh` (bash). It creates its own container from `debian:bookworm-slim` (which has bash), exercises every case below, removes the container on exit, and ends with `selftest-box: N passed, M failed` and exit status `M > 0`:

```bash
#!/usr/bin/env bash
# The box's own oracle: every case the launch-run plan's Review Focus names.
set -u
here=$(cd "$(dirname "$0")" && pwd)
t=$(mktemp -d); name="box-selftest-$$"
trap 'docker rm -f "$name" >/dev/null 2>&1; rm -rf "$t"' EXIT
pass=0; fail=0
ok()  { pass=$((pass+1)); echo "  ok   $1"; }
bad() { fail=$((fail+1)); echo "  FAIL $1"; }
bash "$here/mkbox.sh" "$name" debian:bookworm-slim "$t" "$t/log.jsonl" >/dev/null || { echo "mkbox failed"; exit 1; }
box="$t/box"
endrec() { jq -c --argjson s "$1" 'select(.event=="end" and .seq==$s)' "$t/log.jsonl"; }

# 1. quotes, $, && and a newline run and log verbatim
cmd=$'printf \'%s|\' "a b" "c\'d" \'$HOME\' && echo\necho second'
out=$("$box" "$cmd")
[ "$out" = $'a b|c\'d|$HOME|\nsecond' ] && ok "quoting runs verbatim" || bad "quoting runs verbatim: got [$out]"
logged=$(jq -r 'select(.event=="start" and .seq==1) | .command' "$t/log.jsonl")
[ "$logged" = "$cmd" ] && ok "quoting logs verbatim" || bad "quoting logs verbatim: got [$logged]"

# 2. exit code, stdout and stderr kept apart
"$box" 'echo out; echo err >&2; exit 3' >"$t/o" 2>"$t/e"; rc=$?
[ "$rc" = 3 ] && [ "$(cat "$t/o")" = out ] && [ "$(cat "$t/e")" = err ] && ok "exit and streams" || bad "exit and streams: rc=$rc"
[ "$(endrec 2 | jq -r .exit)" = 3 ] && [ "$(endrec 2 | jq -r .stdout)" = out ] && [ "$(endrec 2 | jq -r .stderr)" = err ] \
  && ok "end line carries exit and both streams" || bad "end line: $(endrec 2)"

# 3. the working directory persists; a failed cd leaves it where it was
"$box" 'mkdir -p /tmp/w && cd /tmp/w' >/dev/null
[ "$("$box" pwd)" = /tmp/w ] && ok "cwd persists" || bad "cwd persists"
"$box" 'cd /nope' >/dev/null 2>&1 && bad "failed cd exits nonzero" || ok "failed cd exits nonzero"
[ "$("$box" pwd)" = /tmp/w ] && ok "failed cd keeps cwd" || bad "failed cd keeps cwd"

# 4. a call killed mid-run is still on record: a start line, no end line
"$box" 'sleep 20' & pid=$!; sleep 2; kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
s=$(jq -r 'select(.event=="start" and .command=="sleep 20") | .seq' "$t/log.jsonl")
[ -n "$s" ] && [ -z "$(endrec "$s")" ] && ok "killed call: start without end" || bad "killed call: seq=[$s]"

# 5. parallel calls get distinct seqs and both lines
"$box" 'echo p1' >/dev/null & "$box" 'echo p2' >/dev/null & wait
n=$(jq -r 'select(.event=="start" and (.command=="echo p1" or .command=="echo p2")) | .seq' "$t/log.jsonl" | sort -u | wc -l | tr -d ' ')
e=$(for q in $(jq -r 'select(.event=="start" and (.command=="echo p1" or .command=="echo p2")) | .seq' "$t/log.jsonl"); do endrec "$q"; done | wc -l | tr -d ' ')
[ "$n" = 2 ] && [ "$e" = 2 ] && ok "parallel calls" || bad "parallel calls: seqs=$n ends=$e"

# 6. output that is not UTF-8 still makes a valid log line
"$box" "printf '\\377ok'" >/dev/null
jq -e . "$t/log.jsonl" >/dev/null && ok "every log line is JSON" || bad "every log line is JSON"

# 7. usage: anything but exactly one argument is refused and not logged
lines=$(wc -l < "$t/log.jsonl")
"$box" >/dev/null 2>&1; rc0=$?; "$box" a b >/dev/null 2>&1; rc2=$?
[ "$rc0" = 2 ] && [ "$rc2" = 2 ] && [ "$(wc -l < "$t/log.jsonl")" = "$lines" ] && ok "usage refused, unlogged" || bad "usage: rc0=$rc0 rc2=$rc2"

# 8. mkbox refuses a name in use
bash "$here/mkbox.sh" "$name" debian:bookworm-slim "$t" "$t/log2.jsonl" >/dev/null 2>&1 && bad "mkbox refuses reuse" || ok "mkbox refuses reuse"

echo "selftest-box: $pass passed, $fail failed"
[ "$fail" = 0 ]
```

- [ ] **Step 2: Run it. It fails because nothing exists yet.**

Run: `bash docs/experiments/2026-10-launch-run/tools/selftest-box.sh; echo "exit=$?"`
Expected: `mkbox failed` and `exit=1`.

- [ ] **Step 3: Write `box.template`**

```bash
#!/usr/bin/env bash
# ./box '<command>': runs <command> on the run's machine, in the working
# directory the previous call left, and logs the call (launch-run spec §4).
# Not a login shell: Debian's /etc/profile would reset PATH and drop cargo's
# bin directory, which the image's own environment puts there.
set -u
BOX='@BOX@'
LOG='@LOG@'
if [ "$#" -ne 1 ]; then
  echo "usage: ./box '<command>'   (exactly one argument: the whole command, quoted)" >&2
  exit 2
fi
cmd=$1
tmp=$(mktemp -d)
locked=0
cleanup() { [ "$locked" = 1 ] && rmdir "$LOG.lock" 2>/dev/null; rm -rf "$tmp"; }
trap cleanup EXIT
lock()   { until mkdir "$LOG.lock" 2>/dev/null; do sleep 0.05; done; locked=1; }
unlock() { rmdir "$LOG.lock"; locked=0; }
now()    { date -u +%Y-%m-%dT%H:%M:%SZ; }

cwd=$(docker exec "$BOX" cat /tmp/.box-cwd 2>/dev/null || echo '?')
lock
seq=$(( $(cat "$LOG.seq") + 1 )); echo "$seq" > "$LOG.seq"
jq -nc --argjson seq "$seq" --arg utc "$(now)" --arg cwd "$cwd" --arg command "$cmd" \
  '{seq:$seq,event:"start",utc:$utc,cwd:$cwd,command:$command}' >> "$LOG"
unlock

docker exec -e BOX_CMD="$cmd" "$BOX" bash -c '
  cd "$(cat /tmp/.box-cwd 2>/dev/null || echo /root)" 2>/dev/null || cd /root
  eval "$BOX_CMD"
  rc=$?
  pwd > /tmp/.box-cwd
  exit $rc' >"$tmp/out" 2>"$tmp/err" </dev/null
rc=$?

lock
jq -nc --argjson seq "$seq" --arg utc "$(now)" --argjson exit "$rc" \
  --rawfile stdout "$tmp/out" --rawfile stderr "$tmp/err" \
  '{seq:$seq,event:"end",utc:$utc,exit:$exit,stdout:$stdout,stderr:$stderr}' >> "$LOG"
unlock
cat "$tmp/out"; cat "$tmp/err" >&2
exit "$rc"
```

- [ ] **Step 4: Write `mkbox.sh`**

```bash
#!/usr/bin/env bash
# mkbox.sh <container> <image> <run-dir> <log> [<registry-dir>]
# A fresh box for one attempt (launch-run spec §4). With <registry-dir> it is a
# rehearsal box: no network, crates.io replaced by that directory source.
set -euo pipefail
if [ "$#" -lt 4 ] || [ "$#" -gt 5 ]; then
  echo "usage: mkbox.sh <container> <image> <run-dir> <log> [<registry-dir>]" >&2; exit 2
fi
name=$1 image=$2 run=$3 log=$4 reg=${5:-}
here=$(cd "$(dirname "$0")" && pwd)
if docker container inspect "$name" >/dev/null 2>&1; then
  echo "mkbox: a container named $name exists — every attempt gets a fresh one" >&2; exit 2
fi
net=bridge; [ -n "$reg" ] && net=none
docker run -d --name "$name" --network "$net" "$image" sleep infinity >/dev/null
docker exec "$name" bash -c 'echo /root > /tmp/.box-cwd'
if [ -n "$reg" ]; then
  docker cp "$reg" "$name:/opt/registry"
  docker exec "$name" bash -c 'printf "[source.crates-io]\nreplace-with = \"rehearsal\"\n\n[source.rehearsal]\ndirectory = \"/opt/registry\"\n" > "${CARGO_HOME:-/root/.cargo}/config.toml"'
fi
mkdir -p "$run"
sed -e "s|@BOX@|$name|" -e "s|@LOG@|$log|" "$here/box.template" > "$run/box"
chmod +x "$run/box"
: > "$log"; echo 0 > "$log.seq"
echo "mkbox: $name (network $net) from $image; $run/box logs to $log"
```

- [ ] **Step 5: Run the selftest. It must pass.**

Run: `bash docs/experiments/2026-10-launch-run/tools/selftest-box.sh; echo "exit=$?"`
Expected: `selftest-box: 12 passed, 0 failed`, `exit=0`. Fix the scripts, not the cases. If a case looks wrong, report it to the controller rather than editing it.

- [ ] **Step 6: Prove the selftest can fail.** Temporarily drop `--arg command "$cmd"` (use `--arg command ""`) in `box.template`, rerun, and expect `quoting logs verbatim` FAIL. Restore the line and rerun green.

- [ ] **Step 7: Commit**

```bash
git add docs/experiments/2026-10-launch-run/tools/box.template docs/experiments/2026-10-launch-run/tools/mkbox.sh docs/experiments/2026-10-launch-run/tools/selftest-box.sh
git commit -m "launch-run tools: ./box runs each command in the attempt's container and logs a start and an end line per call; mkbox.sh makes a fresh box, no network and a directory-source registry for a rehearsal; selftest 12/12 over quoting, streams, cwd, a killed call, parallel calls, non-UTF-8 and usage"
```

---

### Task 3: The rehearsal registry, the sealing check, the block extractor and the replay

**Files:**
- Create: `docs/experiments/2026-10-launch-run/tools/make-registry.sh`
- Create: `docs/experiments/2026-10-launch-run/tools/seal-check.sh`
- Create: `docs/experiments/2026-10-launch-run/tools/block.sh`
- Create: `docs/experiments/2026-10-launch-run/tools/replay.sh`
- Create: `docs/experiments/2026-10-launch-run/tools/selftest-tools.sh`

**Interfaces:**
- Consumes: `mkbox.sh` and the box log schema (Task 2).
- Produces:
  - `bash make-registry.sh <checkout> <out>` → `<out>/registry/` (the directory source), `<out>/crates/*.crate`, `<out>/crates.sha256`, `<out>/README.md` (extracted from the packaged `charpente-cli` crate). It refuses a dirty checkout.
  - `bash seal-check.sh <box.jsonl> <agent.jsonl> <run-dir>` → prints `box side: N hit(s)`, `mac side: M hit(s)`, `agent ./box calls: A, box start lines: S`, each hit on its own line. Exit 0 only if N = M = 0 and A = S.
  - `bash block.sh <file> <test> [<seed>]` → prints the failure block for `<test>` from `<file>`, from its `sim failure in <test>: ` line through its `  repro:  ` line. With `<seed>`, it prints the block whose repro line ends `--seed <seed>`. Exit 1 if there is none.
  - `bash replay.sh <box-a> <app-dir-in-a> <image> <out-dir> '<repro>' [<registry-dir>]` → a second fresh box `<box-a>-replay`, `charpente-cli` installed fresh, the app copied from A without `target/`, the repro run there. Writes `<out-dir>/replay.stdout`, `<out-dir>/replay.box.jsonl`, `<out-dir>/install.log`.

- [ ] **Step 1: Write the selftest first**

`selftest-tools.sh` (bash) covers `block.sh` and `seal-check.sh` with synthetic files: no Docker and no cargo. `make-registry.sh` and `replay.sh` are proven end to end by the controller's preflight (Task 4), because each costs a full `cargo install`.

```bash
#!/usr/bin/env bash
set -u
here=$(cd "$(dirname "$0")" && pwd); t=$(mktemp -d); trap 'rm -rf "$t"' EXIT
pass=0; fail=0
ok()  { pass=$((pass+1)); echo "  ok   $1"; }
bad() { fail=$((fail+1)); echo "  FAIL $1"; }

# block.sh: two failing universes of one test; pick by seed
cat > "$t/batch.txt" <<'EOF'
sim: 256 universe(s) per test from master seed 0x1

sim failure in acked_post_implies_the_echo_after_recovery: ack-durability violated: one
  seed:   0x00000000000000aa
  config: FaultConfig { x: 1 }
  fired:  restart: 1
  repro:  charpente test --sim --filter acked_post_implies_the_echo_after_recovery --seed 0xaa

sim failure in acked_post_implies_the_echo_after_recovery: ack-durability violated: two
  seed:   0x00000000000000bb
  config: FaultConfig { x: 2 }
  fired:  restart: 2
  repro:  charpente test --sim --filter acked_post_implies_the_echo_after_recovery --seed 0xbb
EOF
b=$(bash "$here/block.sh" "$t/batch.txt" acked_post_implies_the_echo_after_recovery 0xbb)
[ "$(printf '%s\n' "$b" | head -1)" = "sim failure in acked_post_implies_the_echo_after_recovery: ack-durability violated: two" ] \
  && [ "$(printf '%s\n' "$b" | wc -l | tr -d ' ')" = 5 ] && ok "block by seed" || bad "block by seed: [$b]"
b1=$(bash "$here/block.sh" "$t/batch.txt" acked_post_implies_the_echo_after_recovery)
printf '%s\n' "$b1" | grep -q -- '--seed 0xaa$' && ok "first block without a seed" || bad "first block: [$b1]"
bash "$here/block.sh" "$t/batch.txt" acked_post_implies_the_echo_after_recovery 0xcc >/dev/null && bad "absent seed exits 1" || ok "absent seed exits 1"
bash "$here/block.sh" "$t/batch.txt" acked_note_survives_a_restart >/dev/null && bad "absent test exits 1" || ok "absent test exits 1"

# seal-check.sh: a clean pair, then one hit on each side
run=/Users/x/Documents/GitHub/launch-rehearsal-1
tu() { jq -nc --arg n "$1" --argjson i "$2" '{message:{role:"assistant",content:[{type:"tool_use",name:$n,input:$i}]}}'; }
{ tu Read "{\"file_path\":\"$run/README.md\"}"
  tu Bash "{\"command\":\"cd $run && ./box 'cargo install charpente-cli'\"}"
  tu Bash "{\"command\":\"./box 'charpente new myapp'\"}"; } > "$t/agent-clean.jsonl"
{ jq -nc '{seq:1,event:"start",utc:"u",cwd:"/root",command:"cargo install charpente-cli"}'
  jq -nc '{seq:2,event:"start",utc:"u",cwd:"/root",command:"charpente new myapp"}'; } > "$t/box-clean.jsonl"
bash "$here/seal-check.sh" "$t/box-clean.jsonl" "$t/agent-clean.jsonl" "$run" >/dev/null && ok "clean pair passes" || bad "clean pair passes"
cp "$t/agent-clean.jsonl" "$t/agent-dirty.jsonl"
tu Bash '{"command":"cat ~/Documents/GitHub/charpente/crates/charpente/src/lib.rs"}' >> "$t/agent-dirty.jsonl"
out=$(bash "$here/seal-check.sh" "$t/box-clean.jsonl" "$t/agent-dirty.jsonl" "$run"); rc=$?
[ "$rc" = 1 ] && printf '%s\n' "$out" | grep -q 'mac side: 1 hit' && ok "mac-side read flagged" || bad "mac-side read: rc=$rc [$out]"
cp "$t/box-clean.jsonl" "$t/box-dirty.jsonl"
jq -nc '{seq:3,event:"start",utc:"u",cwd:"/root",command:"cat /usr/local/cargo/registry/src/index.crates.io-x/charpente-0.1.0/src/lib.rs"}' >> "$t/box-dirty.jsonl"
tu Bash "{\"command\":\"./box 'cat x'\"}" >> "$t/agent-clean.jsonl"
out=$(bash "$here/seal-check.sh" "$t/box-dirty.jsonl" "$t/agent-clean.jsonl" "$run"); rc=$?
[ "$rc" = 1 ] && printf '%s\n' "$out" | grep -q 'box side: 1 hit' && ok "box-side registry read flagged" || bad "box-side: rc=$rc [$out]"
out=$(bash "$here/seal-check.sh" "$t/box-clean.jsonl" "$t/agent-clean.jsonl" "$run"); rc=$?
[ "$rc" = 1 ] && printf '%s\n' "$out" | grep -q 'agent ./box calls: 3, box start lines: 2' && ok "unlogged call flagged" || bad "count mismatch: rc=$rc [$out]"

echo "selftest-tools: $pass passed, $fail failed"
[ "$fail" = 0 ]
```

- [ ] **Step 2: Run it. It fails because nothing exists yet.**

Run: `bash docs/experiments/2026-10-launch-run/tools/selftest-tools.sh; echo "exit=$?"`
Expected: every case FAIL, and `exit=1`.

- [ ] **Step 3: Write `block.sh`**

```bash
#!/usr/bin/env bash
# block.sh <file> <test> [<seed>]: the failure block the sim harness prints
# (crates/charpente-sim/src/harness.rs, "sim failure in …" through "  repro:  …").
set -u
[ "$#" -ge 2 ] && [ "$#" -le 3 ] || { echo "usage: block.sh <file> <test> [<seed>]" >&2; exit 2; }
awk -v head="sim failure in $2: " -v seed="${3:-}" '
  index($0, head) == 1 { on = 1; buf = "" }
  on { buf = buf $0 "\n" }
  on && index($0, "  repro:  ") == 1 {
    on = 0
    n = split($0, w, " ")
    if (seed == "" || w[n] == seed) { printf "%s", buf; found = 1; exit }
  }
  END { exit found ? 0 : 1 }' "$1"
```

- [ ] **Step 4: Write `seal-check.sh`**

```bash
#!/usr/bin/env bash
# seal-check.sh <box.jsonl> <agent.jsonl> <run-dir>: launch-run spec §4's
# sealing, checked after. Hits are printed for a human ruling; any hit, or an
# agent ./box call the log never saw, exits 1.
set -u
[ "$#" -eq 3 ] || { echo "usage: seal-check.sh <box.jsonl> <agent.jsonl> <run-dir>" >&2; exit 2; }
boxlog=$1 agent=$2 run=${3%/}
box_hits=$(jq -r 'select(.event=="start") | .command
  | select(test("registry|github\\.com|docs\\.rs|static\\.crates\\.io|crates\\.io/api|\\.crate\\b|/charpente-[a-z-]*[0-9]"))' "$boxlog")
mac_hits=$(jq -r --arg run "$run" '
  (.message // .) | select(.role? == "assistant") | .content[]?
  | select(type == "object" and .type == "tool_use")
  | if .name == "Bash" then
      (.input.command | ltrimstr("cd " + $run + " && ") | ltrimstr("cd \"" + $run + "\" && ")) as $c
      | if ($c | startswith("./box ")) and ($c | contains("GitHub/charpente") | not) then empty
        else "Bash: " + .input.command end
    elif (.name | IN("Read","Glob","Grep","LS","Edit","Write","MultiEdit","NotebookEdit")) then
      ((.input.file_path // .input.path // .input.notebook_path // "") | tostring) as $p
      | if ($p | startswith($run + "/")) or $p == $run then empty else .name + ": " + $p end
    else empty end' "$agent")
calls=$(jq -r --arg run "$run" '
  (.message // .) | select(.role? == "assistant") | .content[]?
  | select(type == "object" and .type == "tool_use" and .name == "Bash")
  | .input.command | ltrimstr("cd " + $run + " && ") | ltrimstr("cd \"" + $run + "\" && ")
  | select(startswith("./box "))' "$agent" | wc -l | tr -d ' ')
starts=$(jq -r 'select(.event=="start") | .seq' "$boxlog" | wc -l | tr -d ' ')
nb=$(printf '%s' "$box_hits" | grep -c . ); nm=$(printf '%s' "$mac_hits" | grep -c . )
echo "box side: $nb hit(s)"; [ "$nb" = 0 ] || printf '%s\n' "$box_hits" | sed 's/^/  /'
echo "mac side: $nm hit(s)"; [ "$nm" = 0 ] || printf '%s\n' "$mac_hits" | sed 's/^/  /'
echo "agent ./box calls: $calls, box start lines: $starts"
[ "$nb" = 0 ] && [ "$nm" = 0 ] && [ "$calls" = "$starts" ]
```

The `agent ./box calls` count matches the box's start lines only if the agent issues one `./box` per Bash call. A call chaining two `./box` invocations shows up as a mismatch the controller reads and rules on. That is the intent: a mismatch is a question to answer, never a silent pass.

- [ ] **Step 5: Write `make-registry.sh`** (the 2026-10-06 prototype, productized)

```bash
#!/usr/bin/env bash
# make-registry.sh <checkout> <out>: the rehearsal's stand-in for crates.io
# (launch-run spec §5): the six .crate files `cargo package` makes at HEAD,
# unpacked into a directory source beside the vendored dependency closure.
set -euo pipefail
[ "$#" -eq 2 ] || { echo "usage: make-registry.sh <checkout> <out>" >&2; exit 2; }
repo=$1 out=$2
if [ -n "$(git -C "$repo" status --porcelain)" ]; then
  echo "make-registry: $repo is dirty — package from a commit" >&2; exit 2
fi
version=$(grep -m1 '^version = ' "$repo/Cargo.toml" | sed 's/version = "\(.*\)"/\1/')
rm -rf "$out"; mkdir -p "$out/registry" "$out/crates"
( cd "$repo" && CARGO_TARGET_DIR="$out/target" cargo package --no-verify \
    -p charpente-core -p charpente-macros -p charpente -p charpente-sim -p charpente-tsgen -p charpente-cli )
cp "$out/target/package/"*.crate "$out/crates/"
( cd "$repo" && cargo vendor --versioned-dirs "$out/registry" >/dev/null )
for c in "$out/crates/"*.crate; do
  tar -xzf "$c" -C "$out/registry"
  sum=$(shasum -a 256 "$c" | cut -d' ' -f1)
  printf '{"files":{},"package":"%s"}' "$sum" > "$out/registry/$(basename "$c" .crate)/.cargo-checksum.json"
done
tar -xzf "$out/crates/charpente-cli-$version.crate" -O "charpente-cli-$version/README.md" > "$out/README.md"
( cd "$out/crates" && shasum -a 256 *.crate ) > "$out/crates.sha256"
rm -rf "$out/target"
echo "make-registry: $(ls "$out/crates" | wc -l | tr -d ' ') crate(s) at $(git -C "$repo" rev-parse --short HEAD), $(ls "$out/registry" | wc -l | tr -d ' ') source dir(s), README sha256 $(shasum -a 256 "$out/README.md" | cut -c1-16)"
```

- [ ] **Step 6: Write `replay.sh`**

```bash
#!/usr/bin/env bash
# replay.sh <box-a> <app-dir-in-a> <image> <out-dir> '<repro>' [<registry-dir>]
# Launch-run spec §2(d): the controller's replay in a second fresh box, with
# charpente installed fresh and the agent's app copied over without target/.
set -euo pipefail
[ "$#" -ge 5 ] && [ "$#" -le 6 ] || { echo "usage: replay.sh <box-a> <app-dir-in-a> <image> <out-dir> '<repro>' [<registry-dir>]" >&2; exit 2; }
a=$1 app=$2 image=$3 out=$4 repro=$5 reg=${6:-}
here=$(cd "$(dirname "$0")" && pwd)
b="$a-replay"
mkdir -p "$out"
bash "$here/mkbox.sh" "$b" "$image" "$out/run" "$out/replay.box.jsonl" ${reg:+"$reg"}
parent=$(dirname "$app"); base=$(basename "$app")
docker exec "$a" tar -C "$parent" --exclude="$base/target" -cf - "$base" | docker exec -i "$b" tar -C /root -xf -
"$out/run/box" 'cargo install charpente-cli' > "$out/install.log" 2>&1
set +e
"$out/run/box" "cd /root/$base && $repro" > "$out/replay.stdout" 2> "$out/replay.stderr"
echo "replay.sh: repro exit $?"
```

- [ ] **Step 7: Run the selftest. It must pass.**

Run: `bash docs/experiments/2026-10-launch-run/tools/selftest-tools.sh; echo "exit=$?"`
Expected: `selftest-tools: 8 passed, 0 failed`, `exit=0`.

- [ ] **Step 8: Prove it can fail.** Temporarily change `block.sh`'s `w[n] == seed` to `w[n] != seed`. Expect `block by seed` FAIL. Restore it and rerun green.

- [ ] **Step 9: Commit**

```bash
git add docs/experiments/2026-10-launch-run/tools/make-registry.sh docs/experiments/2026-10-launch-run/tools/seal-check.sh docs/experiments/2026-10-launch-run/tools/block.sh docs/experiments/2026-10-launch-run/tools/replay.sh docs/experiments/2026-10-launch-run/tools/selftest-tools.sh
git commit -m "launch-run tools: make-registry.sh (the packaged crates plus their vendored closure as a directory source), seal-check.sh, block.sh, replay.sh; selftest 8/8 over block selection by seed and both sides of the seal"
```

---

### Task 4: Preflight (controller): the image, the registry, and "Try it" run by hand in a box

**Files:**
- Modify: `.superpowers/sdd/2026-10-06-launch-run/progress.md` (image digest, timings, findings)

- [ ] **Step 1: Pin the image**

```bash
docker pull rust:latest && docker inspect --format '{{index .RepoDigests 0}}' rust:latest
docker run --rm rust:latest bash -c 'rustc --version; cargo clippy --version 2>&1 | head -1; echo CARGO_HOME=$CARGO_HOME; command -v gcc tar'
```

Write `IMAGE=rust@sha256:…` (the digest line) into progress.md, together with the rustc version and whether clippy is present. A missing clippy is recorded as a fact the stranger will meet. It is not fixed.

- [ ] **Step 2: Build the registry at HEAD** (in the background; poll for exit)

`bash $REC/tools/make-registry.sh $REPO ~/Documents/GitHub/launch-registry-pre > /tmp/reg-pre.log 2>&1`
Expected last line: `make-registry: 6 crate(s) at <sha>, <N> source dir(s), README sha256 <h>`. Then `cmp ~/Documents/GitHub/launch-registry-pre/README.md $REPO/README.md && echo README-identical`.

- [ ] **Step 3: Run "Try it" by hand through a rehearsal box**

```bash
bash $REC/tools/mkbox.sh launch-preflight $IMAGE ~/Documents/GitHub/launch-preflight ~/Documents/GitHub/launch-preflight.box.jsonl ~/Documents/GitHub/launch-registry-pre/registry
B=~/Documents/GitHub/launch-preflight/box
$B 'cargo install charpente-cli'    # minutes: run this one call in the background, wait for it to exit, record the wall time
$B 'charpente new myapp'
$B 'cd myapp && cargo clippy --all-targets -- -D warnings; echo clippy=$?'
$B 'charpente test --sim'                                 # (a): exit 0, "no failures"
$B "sed -i '/tx.enqueue(/d' src/lib.rs && grep -c 'tx.enqueue' src/lib.rs"   # expect 0
$B 'charpente test --sim'                                 # (b): exit 1
```

Read the last end record: `jq -r 'select(.event=="end") | .stdout' ~/Documents/GitHub/launch-preflight.box.jsonl | tail -c 4000`. Take one failing seed `S` of `acked_post_implies_the_echo_after_recovery` from it, then:

```bash
jq -r 'select(.event=="end") | .stdout' ~/Documents/GitHub/launch-preflight.box.jsonl > /tmp/pre-all.txt
bash $REC/tools/block.sh /tmp/pre-all.txt acked_post_implies_the_echo_after_recovery $S > /tmp/pre-batch.block
$B "charpente test --sim --filter acked_post_implies_the_echo_after_recovery --seed $S" > /tmp/pre-replay.txt
bash $REC/tools/block.sh /tmp/pre-replay.txt acked_post_implies_the_echo_after_recovery $S | cmp - /tmp/pre-batch.block && echo "(c) identical"
```

- [ ] **Step 4: The controller's replay (d), end to end**

In the background:
`bash $REC/tools/replay.sh launch-preflight /root/myapp $IMAGE ~/Documents/GitHub/launch-preflight-replay "charpente test --sim --filter acked_post_implies_the_echo_after_recovery --seed $S" ~/Documents/GitHub/launch-registry-pre/registry > /tmp/pre-replay-d.log 2>&1`
Then: `bash $REC/tools/block.sh ~/Documents/GitHub/launch-preflight-replay/replay.stdout acked_post_implies_the_echo_after_recovery $S | cmp - /tmp/pre-batch.block && echo "(d) identical"`.

- [ ] **Step 5: The seal check on the controller's own log** (a sanity pass: it has no agent transcript, so pass an empty one)

`: > /tmp/empty.jsonl; bash $REC/tools/seal-check.sh ~/Documents/GitHub/launch-preflight.box.jsonl /tmp/empty.jsonl ~/Documents/GitHub/launch-preflight`
Expected: `box side: 0 hit(s)`, `mac side: 0 hit(s)`, and a calls/start-lines mismatch (0 against N), because the controller's calls are not in a transcript. That mismatch is expected here and only here.

- [ ] **Step 6: Record and clean up.** Into progress.md go: the timings (install, first sim), (a)–(d) results, the clippy fact, the README comparison, and any surprise. Each surprise is a `Ruling` before Task 5. Then `docker rm -f launch-preflight launch-preflight-replay` and delete the preflight directories. No charpente commit.

---

### Task 5: The brief and the record's skeleton (controller)

**Files:**
- Create: `docs/experiments/2026-10-launch-run/BRIEF.md`
- Create: `docs/experiments/2026-10-launch-run/README.md`

- [ ] **Step 1: `BRIEF.md`**, the spec §3 text verbatim, without the blockquote markers:

```markdown
You've heard of charpente. Its README is in `README.md` beside this file. Reproduce
what it claims.

You have a fresh Linux machine for this. Run every command there with
`./box '<command>'` from this directory: it runs the command in that machine's shell,
in a working directory that persists between calls. Use only the README; don't read
charpente's source code, whether in cargo's registry or online.

When you're done, say what you did and what you saw.
```

Run: `shasum -a 256 docs/experiments/2026-10-launch-run/BRIEF.md`, and record the hash.

- [ ] **Step 2: `README.md` skeleton**: a title, one framing paragraph naming the spec, `BRIEF.md`'s sha256, the image digest from Task 4, and an empty attempts table with columns: attempt, phase, sha or version, README sha256, image, network, resume count, (a)–(f), master seed (pristine), master seed (red), failing seed replayed, (c), (d), verdict, Noah's sign-off.

- [ ] **Step 3: Commit**

```bash
git add docs/experiments/2026-10-launch-run/BRIEF.md docs/experiments/2026-10-launch-run/README.md
git commit -m "launch-run: the brief (sha256 <hash>), byte-identical for every attempt, and the record's skeleton"
```

---

### Task 6: A rehearsal attempt N (controller dispatches the stranger)

**Files:**
- Create: `$RUN/{BRIEF.md,README.md,box}`, `$RUN.box.jsonl`
- Modify: progress.md (attempt N: sha, digest, dispatch time, `output_file`, resume count)

- [ ] **Step 1: Pin and lay out**

```bash
cd $REPO && git status --porcelain | wc -l      # expect 0
SHA=$(git rev-parse HEAD)
RUN=~/Documents/GitHub/launch-rehearsal-N; REG=~/Documents/GitHub/launch-registry-N
bash $REC/tools/make-registry.sh $REPO $REG > /tmp/reg-N.log 2>&1      # background; poll
mkdir -p $RUN && cp $REC/BRIEF.md $RUN/BRIEF.md && cp $REG/README.md $RUN/README.md
bash $REC/tools/mkbox.sh launch-rehearsal-N $IMAGE $RUN $RUN.box.jsonl $REG/registry
ls -A $RUN                                         # exactly: BRIEF.md README.md box
shasum -a 256 $RUN/BRIEF.md $RUN/README.md
```

The BRIEF hash must equal the record's. Write the sha, both hashes and the time into progress.md.

- [ ] **Step 2: Dispatch.** Agent tool, `subagent_type: general-purpose`, `model: opus`, **no `name`** (a named dispatch drops its return path). The prompt, verbatim, with `<RUN>` expanded:

```
Your directory is <RUN>. Read BRIEF.md there and do what it says.

About this machine, not about the task: one tool call may not run longer than ten
minutes, and a ./box call counts. Run anything long on the machine in the background,
for example ./box 'nohup <command> > <log> 2>&1 &', then check on it with further
./box calls until it has finished, and read the whole log before concluding anything.
```

Record the `output_file` path in progress.md. It is the transcript, and it lives in a session-scoped directory.

- [ ] **Step 3: Wait. Resume only on a harness failure.** If the agent stopped on an API error or overload, not on its own conclusion, resume it via SendMessage with exactly `continue`, and count the resume. Answer no question. A question it asks is testimony. Change nothing under `$RUN` and do not touch the container while it runs.

- [ ] **Step 4: Freeze**

```bash
A=$REC/attempt-N && mkdir -p $A
cp <output_file> $A/agent.jsonl
cp $RUN.box.jsonl $A/box.jsonl
```

Write the agent's final message verbatim into `$A/report.md`. **Do not** remove the container; Task 7's replay copies the app out of it. No commit yet.

---

### Task 7: Adjudicate attempt N (controller), and the verdict to Noah

- [ ] **Step 1: Frame.** `git -C $REPO diff $SHA --stat -- crates Cargo.toml Cargo.lock README.md | wc -l` must be 0. The BRIEF and README hashes and the image digest are as recorded.

- [ ] **Step 2: Sealing (e).** `bash $REC/tools/seal-check.sh $A/box.jsonl $A/agent.jsonl $RUN`. Rule on every hit and on any calls/start-lines mismatch in progress.md. A genuine read of charpente's source voids the attempt. A benign `cd $RUN` call or a chained `./box` pair does not.

- [ ] **Step 3: The bar (a)–(c), from the log**

```bash
jq -r 'select(.event=="start") | "\(.seq)\t\(.cwd)\t\(.command)"' $A/box.jsonl        # the whole sequence
jq -r 'select(.event=="end") | "\(.seq)\t\(.exit)"' $A/box.jsonl
```

- (a) A `charpente test --sim` end record before any edit of `src/lib.rs`, with exit 0 and `no failures` in its stdout.
- (b) The edit that removes `tx.enqueue`, by whatever means the agent chose, then a `charpente test --sim` end record with exit 1 whose stdout has `sim failure in acked_post_implies_the_echo_after_recovery` and no `sim failure in acked_note_survives_a_restart`.
- (c) The agent's replay of a seed `S` from that batch prints the identical block:

```bash
jq -r --argjson s <batch-seq> 'select(.event=="end" and .seq==$s) | .stdout' $A/box.jsonl > /tmp/batch.txt
jq -r --argjson s <replay-seq> 'select(.event=="end" and .seq==$s) | .stdout' $A/box.jsonl > /tmp/replay.txt
bash $REC/tools/block.sh /tmp/batch.txt acked_post_implies_the_echo_after_recovery $S > $A/batch.block
bash $REC/tools/block.sh /tmp/replay.txt acked_post_implies_the_echo_after_recovery $S | cmp - $A/batch.block && echo "(c) identical"
```

Record both master seeds (each batch prints `master seed`) and `S`. If the agent did no replay, (c) fails. That is a finding about the README, not a harness question.

- [ ] **Step 4: The controller's replay (d)**, in the background:

`bash $REC/tools/replay.sh launch-rehearsal-N <app-dir> $IMAGE $A/replay "<the repro line from $A/batch.block>" $REG/registry > /tmp/replay-N.log 2>&1`
Then `bash $REC/tools/block.sh $A/replay/replay.stdout acked_post_implies_the_echo_after_recovery $S | cmp - $A/batch.block && echo "(d) identical"`.

- [ ] **Step 5: (f) and testimony.** Read `report.md` and the transcript's narrative (`jq -r '(.message // .) | select(.role?=="assistant") | .content[]? | select(.type=="text") | .text' $A/agent.jsonl`). List every place the agent stalled, guessed, re-read or reached outside the README. Check every claim it makes about charpente against the code, and rule right, wrong, or right for the wrong reason, with a `file:line` each.

- [ ] **Step 6: Verdict to Noah: STOP.** PASS or FAIL against (a)–(f), with the seeds, both `cmp` results, the seal ruling and the testimony's top items. Wait for his sign-off.

- [ ] **Step 7: The attempt's record and ledger entry** (spec §8: a ledger entry per attempt). Commit `$A/` (gzip `agent.jsonl` if over 10 MB) together with a ledger entry: attempt, phase, sha or version, verdict, seeds, Noah's sign-off. Then `docker rm -f` both boxes. On a PASS, continue to Task 9 (rehearsal) or Task 11 (real run). On a FAIL, go to Task 8.

---

### Task 8: On a rehearsal FAIL only: fix and re-run (cap: three failed rehearsals)

- [ ] **Step 1:** Name the first failing bar item and the platform or README defect behind it, in progress.md with its citation.
- [ ] **Step 2:** Fix it on `main` as its own bounded change with a pin: a test, a compile-fail fixture, or a `doc_truth`-pinned block. If it is a code change, run the full three-command gate (`cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace --no-fail-fast`), counted by `grep -c '^test result: ok'`. If the fix contradicts a DECISIONS.md entry, Noah signs off first.
- [ ] **Step 3:** Write a ledger entry naming the attempt that found it, and commit.
- [ ] **Step 4:** Task 6 with N+1: fresh agent, fresh box, new sha, the same `BRIEF.md`. After a third failed rehearsal, stop and bring it to Noah.

---

### Task 9: The publish (Noah) and what the controller checks after it

- [ ] **Step 1: Hand Noah the commands.** Before them: the full gate green on HEAD and CI green on the pushed tip.

```
! scripts/publish.sh
! scripts/publish.sh --for-real
```

- [ ] **Step 2: Confirm it is on the registry, and compare the README (§9)**

```bash
V=0.1.0
curl -sSfL "https://static.crates.io/crates/charpente-cli/charpente-cli-$V.crate" -o /tmp/pub-cli.crate
tar -xzf /tmp/pub-cli.crate -O "charpente-cli-$V/README.md" > /tmp/pub-README.md
cmp /tmp/pub-README.md $REG/README.md && echo "published README = rehearsed README"
shasum -a 256 /tmp/pub-cli.crate; grep charpente-cli $REG/crates.sha256
```

The README `cmp` must be identical; a difference is a finding. The `.crate` hashes are recorded, and a difference between them is explained, not assumed. Commit the publish's ledger entry (version, sha, the date Noah ran it, both README hashes).

---

### Task 10: A real-run attempt N (controller)

Task 6, then Task 7, with these differences only:

- `RUN=~/Documents/GitHub/launch-real-N`, container `launch-real-N`, no registry: `bash $REC/tools/mkbox.sh launch-real-N $IMAGE $RUN $RUN.box.jsonl`. The fifth argument is absent, so the box has the default bridge network.
- `README.md` comes from the published crate: `cp /tmp/pub-README.md $RUN/README.md`.
- The image: re-run Task 4 Step 1. If `rust:latest` has moved, use and record the new digest, because the stranger's compiler is today's.
- `replay.sh` is called without its sixth argument, so the replay box installs from crates.io too.
- On a FAIL: the fix lands on `main` per Task 8 Steps 1–3, Noah publishes the next patch version (`0.1.1`), Task 9 Step 2 runs against it, and the next attempt installs that. There is no attempt cap. Every verdict goes to Noah.

A PASS signed by Noah is M3 exit criterion (a).

---

### Task 11: The record, the ledger, PLAN, README, CLAUDE.md, and the final gate

**Files:**
- Modify: `docs/experiments/2026-10-launch-run/README.md`, `docs/LEDGER.md`, `PLAN.md`, `README.md`, `CLAUDE.md`
- Add: `docs/experiments/2026-10-launch-run/attempt-*/` (box.jsonl, agent.jsonl, report.md, batch.block, replay/)

- [ ] **Step 1: The write-up.** Fill the attempts table, every hash and seed copied from `attempt-N/`, never from memory. Then the testimony census, in the statuspage write-up's shape. If any `agent.jsonl` exceeds 10 MB, `gzip -9` it and commit the `.gz`: compressed, not trimmed (spec §8).
- [ ] **Step 2: Hard rule 6.**
  - PLAN.md marks sub-project 5 `[shipped <date>; record docs/experiments/2026-10-launch-run/]` and M3 `[shipped <date>]`.
  - README.md's status paragraph says M3 shipped and links the record.
  - CLAUDE.md's "Current state" stops calling M3 active.
  - The ledger's final entry says the roadmap is complete and names what is after launch (PLAN's v1.x table). It also records that the passing real run's (a) step is the publish-path spec's §10.2 "post-publish run against the registry".
- [ ] **Step 3: The full gate**, because `claims_census` reads README.md and `doc_inventory` reads CLAUDE.md. Run all three commands, `--no-fail-fast`, in the background, counted. Every `test result:` line must be `ok`.
- [ ] **Step 4: Commit, and hand Noah the push**

```bash
git add docs/experiments/2026-10-launch-run PLAN.md README.md CLAUDE.md docs/LEDGER.md
git commit -m "launch run: PASS (attempt N against crates.io <version>, seeds <…>) — M3 exit criterion (a); M3 shipped"
```

Then `! git push origin main` for Noah, and the CI result goes into the ledger as one line.
