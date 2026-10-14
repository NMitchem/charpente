# Outbound Effects Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** MANIFESTO §5's external effects exist. An `outbound` job's read-only `call` decides an HTTP request after commit, the kernel makes the call outside any transaction with a platform-minted idempotency key, and `settle` commits the reply's consequences with the Done mark as one record. The simulator injects the outside world's faults.

**Architecture:**
- **Core.** A sixth IO trait, `Outbound`, with `start`/`poll`/`cancel`/`next_wake`. Two record tags, `0x0c`/`0x0d`, carry a 16-byte key drawn from the node's `Entropy` at staging. `JobRow` gains a trailing `outbound_key`.
- **Kernel.** The `Dispatcher` gains an outbound due-set and an in-flight table, so capacity and timeouts live next to the heap they must stay consistent with. The kernel gains a seventh ready source, `Ready::Outbound`. A call starts only after the group that read for it commits.
- **Surface.** `#[job(…, outbound = "host", timeout = "30s")]` generates `call`/`settle` glue. `charpente::outbound` holds `Call`, `Reply` and `Host`. `describe` lists hosts and credential variable names.
- **Sim.** `SimOutbound` lives in the sim's shared world. Each host has an app-written `Remote` model, each fault axis has its own stream, and replies go only to the handle (process) that started the call.
- **Production.** A hyper + rustls client sits beside the inbound edge in `serve.rs` and makes no decisions. TLS always verifies the declared host, and `--outbound-base` moves only the address.

**Tech Stack:**
- Rust: `charpente-core` records and tables, the `syn`/`quote` proc macros.
- The deterministic simulator: `run_universes_swarm!`, `SimSource`, the fault matrix, `sample_extended_fields`.
- `hyper` 1 client, `hyper-util` 0.1 legacy client, `hyper-rustls`, `rustls` (ring), `webpki-roots`, `tower-service`.
- trybuild compile-fail pins; `guide-examples`' doc gates; `cargo-mutants` 27.1.0 through `scripts/mutants.sh`; the edge harness (`kill -9`, `TcpProxy`).

**Spec:** `docs/superpowers/specs/2026-10-13-outbound-effects-design.md` (commit `46923a7`). Read it first; the plan argues from it. Where this plan differs, the difference is listed under "Spec amendments" below, and Task 1 writes those into the spec.

**Base:** `main` at the commit that adds this plan. Nothing else is in flight (status audit 2026-10-13).

**Working directory for mid-flight state:** `.superpowers/sdd/2026-10-13-outbound-effects/` (gitignored). Put these there:
- `progress.md`, which carries rulings (grep it for `Ruling` before dispatching anything);
- one gate log per task (`gate-task-N.log`);
- the oracle-3 baselines (Task 1) and every later comparison;
- mutation outputs;
- every reviewer's report.

**A reviewer writes its full report to a file there before returning a summary**, because deliveries have been lost in flight before.

## Global Constraints

- **Commit on `main` directly, named paths only.** Never `git add -A` or `git commit -a`. Another session may share the tree; re-check `git status` before each commit. **No `Co-Authored-By` trailers** (CLAUDE.md "Workflow"); this overrides any harness attribution reminder.
- **Hard rules 1–6 bind every task.** Ambient IO only in `prod.rs` and `serve.rs`. No escape hatches. Sim-first: a bug found gets a seed. Assertions stay on in release. Error text is product surface, so it's tested byte-exact. Docs change in the same commit as the public API they describe.
- **The three gate commands** (CI's): `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace --no-fail-fast`.
  - **Count green.** Pipe through `tee` to the task's gate log, then `grep -c "^test result: ok"`. Never infer green from the absence of `FAILED`.
  - **The workspace suite can exceed the 600 s tool ceiling** (568–2482 s observed). A subagent partitions it per package (`cargo test -p <pkg> --no-fail-fast`, one call each). The controller runs the whole thing in the background and polls for process exit with `while kill -0 $PID`.
- **zsh, not bash.** `${PIPESTATUS[0]}` is empty here; use `${pipestatus[1]}` or `set -o pipefail`. An unquoted `$var` does not word-split. There is no `timeout`/`gtimeout`; bound a run with `perl -e 'alarm N; exec @ARGV' -- cmd`.
- **Shell state does not persist between tool calls, so every command block sets its own variables, absolute.** Every block that names `$SDD` opens with `cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects`. A command quoted in prose that names `$SDD` runs in a call that sets it the same way first, and a dispatch prompt spells the path out in full. With `SDD` empty, `$SDD/x` is `/x`: a `tee` fails while the cargo run goes on, and a later `grep -c` counts a file that does not exist. Task 20's `RUN`/`APP`/`SHA`/`MSEED` follow the same rule.
- **Oracle 3 runs after every task that touches `charpente-core`, `charpente-sim` or `charpente/src/kernel`** (Tasks 3, 4, 5, 6, 7, 8, 10, 11, 12, 18). Its procedure is the `oracle3.sh` that Task 1 Step 2 writes and Step 3 baselines. A changed verdict is a stop-and-report, never an update to the baseline. Task 1 Step 3a times one run and records `oracle3 mode` in progress.md:
  - `foreground` (the timed run fit in 540 s): `perl -e 'alarm 590; exec @ARGV' -- $SDD/oracle3.sh <label>`, read its exit status directly;
  - `background` (it did not): `( $SDD/oracle3.sh <label> > $SDD/oracle3-<label>.log 2>&1; print $? > $SDD/oracle3-<label>.exit ) &`, then poll with bounded checks, one `[[ -s $SDD/oracle3-<label>.exit ]] && cat $SDD/oracle3-<label>.exit` per call. A foreground `sleep` loop is blocked in this harness.

  Every later task's oracle-3 block shows the foreground form; in background mode, run the background form with the same label instead.
- **Dead code before its first caller.** `clippy --workspace --all-targets -- -D warnings` denies dead code, and Tasks 7, 9 and 10 land `pub(crate)` items before the task that first calls them. Each such item carries `#[expect(dead_code, reason = "first caller: Task N")]`, or `#[cfg_attr(not(test), expect(dead_code, reason = "first caller: Task N"))]` when its own unit tests call it. Task N's text deletes the attribute. An expectation that stops being met is itself an error (`unfulfilled_lint_expectations`), so none can linger. Items reached only through an expected item (a private helper, a field it reads) need no attribute of their own.
- **Mutation runs:** `scripts/mutants.sh` with `--test-package charpente` for any file outside core/sim. Without it, every mutant reads MISSED having measured nothing. `CARGO_TARGET_DIR` must be unset. Run `cargo clean -p charpente` before and after.
- **Pinned seeds name universes.** A test pinning a seed asserts the *reason* (a coverage key, a message prefix), not only the outcome.
- **Never pass `name` to an Agent dispatch.** A named dispatch routes through the teammate protocol and drops its return path. Reviewers run on `opus` for kernel and core tasks.

## Review Focus

These are the inputs the spec implies but its sections don't test directly. Each line names the task whose tests pin it.

1. **A `call` that read a sibling's write, in a batch whose commit then fails.** No call fires (R6). Pinned in Task 11, `a_call_never_starts_when_its_group_commit_fails`, and by Task 11's R6 crash sweep.
2. **A late reply for a timed-out attempt that arrives after the retry started.** It never settles the retry (R7). Pinned in Task 11, `a_late_reply_never_settles_the_retry`.
3. **A control character or non-visible ASCII in a path, header name or header value, or a reserved or credential header set by app code.** A refused attempt naming the rule, never sent (amendment A12). Pinned in Task 7, `the_call_rules_name_each_violation`, and Task 10, `a_refused_call_is_an_attempt_naming_the_rule`.
4. **A reply larger than the host's cap.** A named attempt failure in the sim, and in production the edge stops reading at cap+1, so memory is bounded. Pinned in Task 11, `an_oversized_reply_is_a_named_attempt`, and Task 14, `prod_reply_cap_bounds_reading`.
5. **A node that stops being writable with calls in flight.** The in-flight table is dropped, late completions are discarded, and the jobs stay Pending for the next primary with the same key. Pinned in Task 11, `losing_writability_drops_in_flight_and_keeps_jobs_pending`.

## Spec amendments (Task 1 appends these to the spec, verbatim, under a new "## Amendments (implementation plan)" heading)

- **A1 — `sim.with_remote(host, |m: &M| …)` replaces `remote_ref`.** A reference cannot escape the world's lock. A closure model is wrapped by `charpente_sim::outbound::from_fn(f)`, not a blanket impl over `FnMut`.
- **A2 — `Outbound::next_wake() -> OutboundWake { Unknown, Never, At(Instant) }`, default `Unknown`.** The sim knows when its next completion is due, or that none ever will be (every reply in flight is lost), and says so. Production says `Unknown`, and the kernel parks on a 5 ms outbound tick. Same kernel code either way, as with `Repl::next_deadline`. Without it, a 30 s simulated call costs ~6,000 empty iterations. `Never` is distinct from `Unknown` because a sim whose every reply is lost must park until the in-flight deadline, not tick.
- **A3 — "process generation" is the `SimOutbound` handle.** Each kernel gets its own `sim.outbound(…)` handle, and a completion goes only to the handle that started the call. A restarted process builds a new kernel with a new handle, so a pre-restart reply has nowhere to land. Requests already on the wire still reach the model at their delivery time. No restart hook is needed.
- **A4 — `max_in_flight` is `App::outbound_max_in_flight(n)` (default 64), not an `AppConfig` field.** `AppConfig` has ~30 struct-literal sites. The setting also belongs beside the `Host` entries.
- **A5 — the host-set check runs when the kernel is built, not at mount.** `mount_jobs` and `outbound(Host)` may come in any order. The refusal is a `Refusal`, so `serve` prints "refused to start".
- **A6 — a job that wrote `run` instead of `call`/`settle` gets rustc's E0599 for the missing `call`.** The attribute sits on the struct and cannot see the `impl` block. A compile-fail fixture pins it, and GUIDE §9 lists it.
- **A7 — `Job::OUTBOUND` is a `bool` const, default `false`.** Core needs only "mint and pick the tag"; the host lives on `JobDef`.
- **A8 — `Db::drain_enqueued` returns `Vec<Enqueued { id, outbound }>`.** The flag comes from the record tag at apply. Looking a name up in the store would draw sim read-fault rolls and re-roll every universe (invariant 6).
- **A9 — only App-opened groups can mint.** `App::begin_group` and the kernel's job and cron arms mint. A bare `Db::transact`/`Db::begin_group` that enqueues an outbound job is a contract panic naming the fix. Tests seed outbound jobs through `App::transact_for_tests` (`#[doc(hidden)]`).
- **A10 — the reply-over-cap text says the remote applied the call**, since it answered: `outbound <host>: the reply exceeded the <cap>-byte reply cap — the remote answered, so it applied this call; raise the cap with Host::max_reply_bytes (the retry carries the same idempotency key)`. The text names no length: the production edge stops reading at cap + 1 bytes (Review Focus 4), so it never learns the true length, and the sim and production must print the same line.
- **A11 — TLS identity is always the declared host.** The client keeps `https://<host>[:port]` as the request URI, so SNI and certificate verification use the declared name. `--outbound-base <host>=<url>` changes only where the name resolves (a custom resolver) and the port, as `curl --resolve` does. This adds `tower-service` (a dependency of `hyper-util`'s `client` feature) as a direct dependency.
- **A12 — a path must be visible ASCII, a header name an HTTP token, and a header value free of control characters**; anything else is refused as an attempt naming the rule. This closes header injection (a CR or LF forging a header), which the spec's rules missed.
- **A13 — a new workspace member, `crates/outbound-fixture`.** It holds the app with outbound jobs, its `launch`-driven node binary, and the TLS fake remote that the edge harness drives. Putting outbound jobs in `sim-fixture` or `invoicing` would re-roll their recorded batches (oracle 3) and their goldens.
- **A14 — oracle 3 compares a batch's verdicts, failures (test, seed, message), outcomes and coverage, never `config`.** `config` gains the outbound axes by design. Raw JSON bytes therefore change while every universe stays the same.
- **A15 — `outbound_slow` adds think time drawn uniformly from `[10·hi, 100·hi]`**, where `hi` is `outbound_latency.1`, or from `[1 s, 100 s]` when `hi` is zero. It's drawn before the reply's own latency.
- **A16 — `call` and `settle` panics are absorbed as attempts** with `call panicked: <payload>` / `settle panicked: <payload>`, the prefixes `run`'s `handler panicked:` set.
- **A17 — the no-`Outbound` refusal builds the handle first.** The spec's text, `Kernel::with_outbound(app, clock, Box::new(sim.outbound(&app.outbound_hosts())?))`, does not compile: it moves `app` and then borrows it. The refusal says `let outbound = sim.outbound(&app.outbound_hosts())?; Kernel::with_outbound(app, clock, Box::new(outbound))`.
- **A18 — a refused connection carries its cause.** `ConnectionRefused` is the only "certainly not delivered" kind, and the edge also uses it for DNS and TLS failures before any request byte is sent. The attempt text is therefore `outbound <host>: <the error's own text> — the call did not reach the remote`. The sim's error text is exactly `connection refused`, so the sim reproduces spec §7's example line, and an operator still sees "invalid peer certificate: UnknownIssuer" when TLS is why.
- **A19 — oracle 2's switchover row is pinned at the write gate, not through a two-node rig.** A switchover reaches the kernel only as `Db::forbid_writes` (`Role::HandingOff`, `repl/mod.rs:300`), and invariant 3's drop is level-triggered on writability (Task 11). So Task 11's `losing_writability_drops_in_flight_and_keeps_jobs_pending` is that row. Its mutation proof (Task 11 Step 7) deletes the drop and goes red. A real switchover under outbound load waits for the first replicated app with outbound jobs, as a v1.x row (Task 17).
- **A20 — the docs-only build's ambiguous-call check is a controller gate, run before the review, and its result goes to the reviewer.** Spec §16(5) gave it to the reviewer. But it's a mechanical run whose output is a seed, and this repo's rule is that nothing is believed from a report (Task 20 Step 4).
- **A21 — `sim.outbound(hosts)`'s no-model refusal names the host, not the job (spec §10 names the job).** `sim.outbound` takes the app's host list as plain data, so no job is in view there. The text is the one Task 4 pins.

## File map

**Created**

| File | Responsibility |
|---|---|
| `crates/charpente-sim/src/outbound.rs` | `SimOutbound`, `Remote`, `from_fn`, `Recorder`, `Idempotent`, `Delivered`, the outbound world |
| `crates/charpente/src/outbound.rs` | `Call`, `Reply`, `Host`, the call-validation rules, re-exports of `IdempotencyKey`/`HttpMethod` |
| `crates/charpente/src/kernel/outbound.rs` | the in-flight table, token counter, completion buffer, deadline set, and every failure message |
| `crates/charpente/tests/outbound.rs` | the platform suite: §7's table, invariants, R6/R7, swarm property, planted bugs |
| `crates/charpente-macros/tests/compile_fail/outbound_*.rs` (+ `.stderr`) | the eight compile-time refusals |
| `crates/outbound-fixture/` (`Cargo.toml`, `src/lib.rs`, `src/bin/outbound_node.rs`, `src/bin/fake_remote.rs`, `tls/*`, `tests/sim.rs`) | the app the CLI dogfoods and the edge harness drives (A13) |
| `.superpowers/sdd/2026-10-13-outbound-effects/oracle3.sh`, `oracle3-filter.jq` | oracle 3's replay-and-compare (Task 1; gitignored) |
| `crates/edge-harness/tests/outbound.rs` | oracle 7 |
| `docs/experiments/2026-10-outbound/` (`BRIEF.md`, `README.md`, `attempt-N/`) | the docs-only build's record |

**Modified:** `charpente-core/src/{io,db,job,table}.rs`; `charpente-sim/src/{lib,harness,checks}.rs` (`checks.rs` gains the three outbound checkers in Task 12); `charpente/src/{lib,kernel/mod,kernel/dispatch,http/app,describe,serve,launch}.rs`; `charpente/tests/kernel.rs` and the other suites with `JobDef` literals (`outbound: None`); `charpente/Cargo.toml`; `charpente-macros/src/job.rs`; `edge-harness/src/child.rs`; root `Cargo.toml`; `CLAUDE.md`; `README.md`; `DESIGN.md`; `DECISIONS.md`; `PLAN.md`; `NEIGHBORS.md`; `docs/GUIDE.md`; `docs/BENCH.md`; `docs/LEDGER.md`; the describe goldens.

---

### Task 1: Baselines, amendments, and the working directory

**Files:**
- Create: `.superpowers/sdd/2026-10-13-outbound-effects/{progress.md,oracle3.sh,oracle3-filter.jq,baseline/*}`
- Modify: `docs/superpowers/specs/2026-10-13-outbound-effects-design.md` (append amendments)

**Interfaces:**
- Produces: `oracle3.sh <label>`, which every later core, sim or kernel task runs. It exits 0 when every baseline batch's filtered report is identical, and 1 with the diff otherwise.

- [ ] **Step 1: Create the working directory and record the base**

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
git status --porcelain | wc -l          # expect 0
mkdir -p $SDD/baseline
print "\nBase: $(git rev-parse HEAD)" >> $SDD/progress.md   # append: progress.md already holds the controller's ledger
```

- [ ] **Step 2: Write the filter and the script**

`$SDD/oracle3-filter.jq`. It keeps everything that names a universe's fate and drops `config`, which gains the outbound axes by design (A14):

```jq
{
  master_seed, universes, verdicts, gave_up, outcomes, coverage, missing_coverage,
  failures: [.failures[] | {test, seed, message}]
}
```

`$SDD/oracle3.sh` (a quoted heredoc, so nothing expands while writing it):

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
cat > $SDD/oracle3.sh <<'EOF'
#!/bin/zsh
# usage: oracle3.sh <label>  — re-runs every baseline batch and diffs it, filtered.
# `rc`, never `status`: zsh's `status` is read-only, and assigning it aborts the script.
set -u
SDD=${0:A:h}
cd ${SDD:h:h:h}
label=$1
mkdir -p $SDD/oracle3-$label
cargo build -q -p charpente-cli || exit 2
rc=0
for spec in "crates/sim-fixture 0x912313adf8115ad0" "crates/sim-fixture 0x483e230baed4922b" \
            "examples/invoicing 0x912313adf8115ad0" "examples/invoicing 0x483e230baed4922b"; do
  dir=${spec%% *}; seed=${spec##* }
  name=${dir:t}-$seed
  ./target/debug/charpente test --sim --dir $dir --universes 256 --master-seed $seed \
      --format json > $SDD/oracle3-$label/$name.raw.json 2> $SDD/oracle3-$label/$name.stderr
  print "$name exit ${?}"
  # A launch error (CLI exit 2) writes no report, and jq over an empty file prints nothing and
  # exits 0: refuse it here, or it would read as a changed verdict.
  [[ -s $SDD/oracle3-$label/$name.raw.json ]] || { print "$name: no report (launch error)"; exit 2 }
  jq -S -f $SDD/oracle3-filter.jq $SDD/oracle3-$label/$name.raw.json > $SDD/oracle3-$label/$name.json || exit 2
  if ! diff -u $SDD/baseline/$name.json $SDD/oracle3-$label/$name.json > $SDD/oracle3-$label/$name.diff; then
    print "ORACLE 3 CHANGED: $name (see $SDD/oracle3-$label/$name.diff)"; rc=1
  fi
done
exit $rc
EOF
chmod +x $SDD/oracle3.sh
```

- [ ] **Step 3: Capture the baselines** (in the background, since four batches can pass 600 s)

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
( for spec in "crates/sim-fixture 0x912313adf8115ad0" "crates/sim-fixture 0x483e230baed4922b" \
              "examples/invoicing 0x912313adf8115ad0" "examples/invoicing 0x483e230baed4922b"; do
    dir=${spec%% *}; seed=${spec##* }; name=${dir:t}-$seed
    ./target/debug/charpente test --sim --dir $dir --universes 256 --master-seed $seed --format json \
      > $SDD/baseline/$name.raw.json 2> $SDD/baseline/$name.stderr
    print "$name exit $?" >> $SDD/baseline/exits.txt
    jq -S -f $SDD/oracle3-filter.jq $SDD/baseline/$name.raw.json > $SDD/baseline/$name.json
  done; print done > $SDD/baseline/finished ) > $SDD/baseline/run.log 2>&1 &
print $! > $SDD/baseline/pid
```

Run `cargo build -q -p charpente-cli` first. Poll with bounded checks, one per call: `[[ -s $SDD/baseline/finished ]] && cat $SDD/baseline/exits.txt` (a foreground `sleep` loop is blocked in this harness).

Expected: four lines. Exit 0 or 1 are both valid baselines: a batch with failures is fine as long as it's recorded. Exit 2 is a launch error, so stop and report. Each `.json` must be non-empty.

- [ ] **Step 3a: Prove the comparison works, and time it**

The selfcheck is also the timing run that decides how every later task runs oracle 3 (Global Constraints). It runs in the background, since its length is what is being measured:

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
( t0=$(date +%s); $SDD/oracle3.sh selfcheck > $SDD/oracle3-selfcheck.log 2>&1; rc=$?
  print "exit $rc wall $(( $(date +%s) - t0 ))s" > $SDD/oracle3-selfcheck.exit ) &
```

Poll with bounded checks of `$SDD/oracle3-selfcheck.exit`. Expected: `exit 0` and no `ORACLE 3 CHANGED` in the log. Record in progress.md `oracle3 selfcheck: exit 0, wall <N>s`, then `oracle3 mode: foreground` when N ≤ 540 or `oracle3 mode: background` otherwise.

- [ ] **Step 4: Prove the comparison can fail**

Copy one baseline aside, flip its `verdicts` value with `jq '.verdicts += 1'`, run `oracle3.sh negcheck` in the mode Step 3a recorded, and expect exit 1 naming that file, then restore the original. Record "oracle3 negcheck: red as expected" in progress.md.

- [ ] **Step 5: Record the base gate count**

Run the three gate commands (the workspace test in the background to `$SDD/gate-task-1.log`). Record `grep -c "^test result: ok" $SDD/gate-task-1.log` in progress.md as `base ok count`. Every later full gate compares to it: the count may only grow.

- [ ] **Step 6: Append the amendments to the spec and commit**

Append the "Spec amendments" section above (A1–A21), verbatim, to the spec under `## Amendments (implementation plan)`. Then:

```bash
git add docs/superpowers/specs/2026-10-13-outbound-effects-design.md
git commit -m "docs: outbound effects spec — implementation-plan amendments A1–A21"
```

---

### Task 2: The `Outbound` IO trait and its value types

**Files:**
- Modify: `crates/charpente-core/src/io.rs` (after `ObjectStore`, before `#[cfg(test)]`)
- Modify: `CLAUDE.md` (hard rule 1's trait list), `DESIGN.md` §1's trait block (`DESIGN.md:66-84`)

**Interfaces:**
- Produces (all `pub` in `charpente_core::io`):
  - `HttpMethod { Get, Post, Put, Patch, Delete }` with `as_str(self) -> &'static str`
  - `IdempotencyKey`: `from_bytes([u8; 16])`, `as_bytes(&self) -> &[u8; 16]`, `as_str(&self) -> &str`
  - `OutboundCall { host: &'static str, method: HttpMethod, path: String, headers: Vec<(String, String)>, body: Vec<u8>, key: IdempotencyKey }`
  - `OutboundReply { status: u16, headers: Vec<(String, String)>, body: Vec<u8> }`
  - `enum OutboundWake { Unknown, Never, At(Instant) }` (`Clone, Copy, PartialEq, Eq, Debug`)
  - `trait Outbound: Send { fn start(&self, token: u64, call: OutboundCall) -> io::Result<()>; fn poll(&self) -> Option<(u64, io::Result<OutboundReply>)>; fn cancel(&self, token: u64); fn next_wake(&self) -> OutboundWake { OutboundWake::Unknown } }`

- [ ] **Step 1: Write the failing test** (append inside `io.rs`'s `mod tests`)

```rust
    #[test]
    fn an_idempotency_key_renders_as_32_lowercase_hex() {
        let key = IdempotencyKey::from_bytes([
            0x00, 0x01, 0x0a, 0x0f, 0x10, 0x7f, 0x80, 0xab, 0xcd, 0xef, 0xff, 0x12, 0x34, 0x56,
            0x78, 0x9a,
        ]);
        assert_eq!(key.as_str(), "00010a0f107f80abcdefff123456789a");
        assert_eq!(key.as_str().len(), 32);
        assert_eq!(key.as_bytes()[2], 0x0a);
        assert_eq!(format!("{key:?}"), "IdempotencyKey(00010a0f107f80abcdefff123456789a)");
    }

    #[test]
    fn http_methods_render_upper_case() {
        let all = [
            (HttpMethod::Get, "GET"),
            (HttpMethod::Post, "POST"),
            (HttpMethod::Put, "PUT"),
            (HttpMethod::Patch, "PATCH"),
            (HttpMethod::Delete, "DELETE"),
        ];
        for (m, s) in all {
            assert_eq!(m.as_str(), s);
        }
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p charpente-core --lib io::tests`
Expected: FAIL to compile with "cannot find type `IdempotencyKey`".

- [ ] **Step 3: Write the implementation** (append to `io.rs` after the `ObjectStore` trait)

```rust
/// The HTTP methods an outbound call may use (outbound-effects spec §3). Named
/// `HttpMethod` because `charpente::http::Method` is the inbound one.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl HttpMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Put => "PUT",
            HttpMethod::Patch => "PATCH",
            HttpMethod::Delete => "DELETE",
        }
    }
}

/// An outbound job's idempotency key: 16 bytes drawn from the node's `Entropy`
/// when the enqueue is staged, recorded in the enqueue effect, and stable across
/// every retry, re-dispatch, failover and restore of that job (spec §6). Two
/// diverged timelines draw different ones, which is why a key is never derived
/// from an id. The app places it where its API wants it (spec R5).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct IdempotencyKey {
    raw: [u8; 16],
    hex: [u8; 32],
}

impl IdempotencyKey {
    pub fn from_bytes(raw: [u8; 16]) -> IdempotencyKey {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut hex = [0u8; 32];
        for (i, b) in raw.iter().enumerate() {
            hex[2 * i] = DIGITS[usize::from(b >> 4)];
            hex[2 * i + 1] = DIGITS[usize::from(b & 0x0f)];
        }
        IdempotencyKey { raw, hex }
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.raw
    }

    /// 32 lowercase hex characters.
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.hex).expect("hex digits are ASCII")
    }
}

impl std::fmt::Debug for IdempotencyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IdempotencyKey({})", self.as_str())
    }
}

/// One outbound HTTP request, as the kernel hands it to `Outbound::start`. `host`
/// comes from the job's declaration, never from runtime data (spec §5).
#[derive(Clone, Debug)]
pub struct OutboundCall {
    pub host: &'static str,
    pub method: HttpMethod,
    /// Path and query only, beginning with `/`.
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Attribution only: an impl never transmits it. The app places the key where
    /// its API wants it (spec R5); the sim reads this field to attribute a
    /// delivered call to its job.
    pub key: IdempotencyKey,
}

/// A reply of any status. A transport failure is not a reply: it arrives as the
/// `Err` of a completion.
#[derive(Clone, Debug)]
pub struct OutboundReply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Outbound HTTP to declared hosts (outbound-effects spec §3): the sixth IO
/// trait. Many calls may be in flight, at most one per token. The trait carries
/// no timeout and no retry: both are the kernel's decisions, made on its own
/// `Clock` (spec R10).
///
/// An impl reports a failure that certainly did not reach the remote with
/// `io::ErrorKind::ConnectionRefused`. The kernel words every other error as
/// "the remote may have applied this call".
///
/// `Send` because the kernel that owns it crosses into the kernel thread.
pub trait Outbound: Send {
    /// Begins `call` under `token`. Asserts `token` is not in flight. An `Err`
    /// here is a completion the kernel records, not a kernel failure.
    fn start(&self, token: u64, call: OutboundCall) -> io::Result<()>;

    /// One completion or `None`. A transport failure IS a completion: the error
    /// rides inside the `Some`, as with `ObjectStore::poll`.
    fn poll(&self) -> Option<(u64, io::Result<OutboundReply>)>;

    /// The kernel abandoned `token` (its deadline passed, or the node stopped
    /// being writable). A completion for it may still surface; the kernel
    /// discards it.
    fn cancel(&self, token: u64);

    /// When this handle's next completion is due, asked only while something
    /// is in flight (plan amendment A2). The kernel parks no later than it.
    fn next_wake(&self) -> OutboundWake {
        OutboundWake::Unknown
    }
}

/// `Outbound::next_wake`'s answer.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OutboundWake {
    /// Production's answer, since replies arrive from the network: the kernel
    /// parks no longer than its outbound tick.
    Unknown,
    /// No completion will ever arrive for what is in flight (every reply is
    /// lost): only the kernel's own deadlines wake it.
    Never,
    /// The next completion is due at this instant.
    At(Instant),
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p charpente-core --lib io::tests`
Expected: PASS, including the two new tests.

- [ ] **Step 5: Update the two trait lists**

- `CLAUDE.md` hard rule 1: "all IO goes through the `Clock`/`Entropy`/`Disk`/`Net`/`ObjectStore` traits" becomes "…`Net`/`ObjectStore`/`Outbound` traits".
- DESIGN §1's `rust` trait block (the one opening `trait Clock   { fn now(&self) -> Instant;`): add after the `ObjectStore` lines:

```rust
trait Outbound { fn start(&self, token: u64, call: OutboundCall) -> Result<()>;
                 fn poll(&self) -> Option<(u64, Result<OutboundReply>)>;   // a failure IS a completion
                 fn cancel(&self, token: u64);
                 fn next_wake(&self) -> OutboundWake; }                    // sim: At/Never; prod: Unknown
```

- [ ] **Step 6: Gate and commit**

Oracle 3 is not owed: no behaviour changed.

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
cargo fmt --all --check
cargo clippy -p charpente-core --all-targets -- -D warnings
cargo test -p charpente-core --no-fail-fast 2>&1 | tee $SDD/gate-task-2.log; grep -c "^test result: ok" $SDD/gate-task-2.log
```

```bash
git add crates/charpente-core/src/io.rs CLAUDE.md DESIGN.md
git commit -m "core: the Outbound IO trait and its value types (outbound-effects spec §3)"
```

---

### Task 3: The outbound fault axes on `FaultConfig`

**Files:**
- Modify: `crates/charpente-sim/src/lib.rs` (`FaultConfig` fields, `Default`, `none`, `swarm`, `validate`, `sample_extended_fields`)
- Modify: `crates/charpente-sim/src/harness.rs` (`config_json`; the two `FaultConfig` literals in its tests, `human_failure_block_matches_the_pinned_shape`'s at `:774` and `config_json_carries_every_fault_axis`'s at `:966`; the pinned Debug string at `:809`)
- Modify: every `FaultConfig { … }` literal that lists fields without `..base`: `harness.rs`'s two above and `lib.rs`'s three (`Default` `:114`, `none` `:139`, `swarm` `:204`). Every other literal in the workspace uses `..base`.
- Modify: `docs/GUIDE.md` §8 "Reading a failure" sample (`:3218`), refreshed to the exact pinned line

**Interfaces:**
- Produces: `FaultConfig` fields `outbound_refused: f64`, `outbound_lost_request: f64`, `outbound_lost_reply: f64`, `outbound_throttle: f64`, `outbound_slow: f64`, `outbound_latency: (Duration, Duration)`. Task 4 consumes them.

- [ ] **Step 1: Write the failing tests** (in `lib.rs`'s test module, beside the existing `sample_extended_fields` tests)

```rust
    #[test]
    fn outbound_axes_are_off_in_none_and_in_a_bare_swarm() {
        let none = FaultConfig::none();
        assert_eq!(
            (none.outbound_refused, none.outbound_lost_request, none.outbound_lost_reply),
            (0.0, 0.0, 0.0)
        );
        assert_eq!((none.outbound_throttle, none.outbound_slow), (0.0, 0.0));
        assert_eq!(none.outbound_latency, (Duration::ZERO, Duration::ZERO));
        let swarm = FaultConfig::swarm(&mut Rng::new(7));
        assert_eq!(swarm.outbound_lost_reply, 0.0, "swarm alone draws no extended axis");
    }

    /// The A14 property, at the source: sampling the outbound axes perturbs no
    /// axis that existed before them, for any seed.
    #[test]
    fn outbound_axes_perturb_no_existing_draw() {
        for seed in [0u64, 1, 0x912313adf8115ad0, 0x483e230baed4922b, u64::MAX] {
            let mut with = FaultConfig::swarm(&mut Rng::new(seed));
            sample_extended_fields(seed, &mut with);
            let mut without = with.clone();
            without.outbound_refused = 0.0;
            without.outbound_lost_request = 0.0;
            without.outbound_lost_reply = 0.0;
            without.outbound_throttle = 0.0;
            without.outbound_slow = 0.0;
            without.outbound_latency = (Duration::ZERO, Duration::ZERO);
            let mut legacy = FaultConfig::swarm(&mut Rng::new(seed));
            sample_extended_fields_without_outbound_for_tests(seed, &mut legacy);
            assert_eq!(format!("{without:?}"), format!("{legacy:?}"), "seed {seed:#x}");
        }
    }

    #[test]
    fn some_seed_turns_each_outbound_axis_on() {
        let mut seen = [false; 6];
        for seed in 0..512u64 {
            let mut cfg = FaultConfig::swarm(&mut Rng::new(seed));
            sample_extended_fields(seed, &mut cfg);
            seen[0] |= cfg.outbound_refused > 0.0;
            seen[1] |= cfg.outbound_lost_request > 0.0;
            seen[2] |= cfg.outbound_lost_reply > 0.0;
            seen[3] |= cfg.outbound_throttle > 0.0;
            seen[4] |= cfg.outbound_slow > 0.0;
            seen[5] |= cfg.outbound_latency.1 > Duration::ZERO;
            cfg.validate();
        }
        assert_eq!(seen, [true; 6]);
    }
```

`sample_extended_fields_without_outbound_for_tests` is a `#[cfg(test)]` helper added in Step 3: the pre-change body of `sample_extended_fields`, kept so the property compares against what the code did before. `with.clone()` is `FaultConfig`'s own derived `Clone` (`lib.rs:33`).

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p charpente-sim --lib outbound_ax`
Expected: FAIL to compile, "no field `outbound_refused`".

- [ ] **Step 3: Add the fields and their sampling**

Add to `FaultConfig`, after `subscriber_stall` and before `disk_latency`:

```rust
    /// P(an outbound call is refused before it reaches the remote — the kernel
    /// sees an immediate `ConnectionRefused`). Rolled from its own stream.
    pub outbound_refused: f64,
    /// P(an outbound request vanishes: the remote never sees it, the kernel
    /// sees silence until its deadline).
    pub outbound_lost_request: f64,
    /// P(the remote applies a call but its reply never arrives — the ambiguous
    /// case idempotency keys exist for; `object_lost_ack`'s outbound twin).
    pub outbound_lost_reply: f64,
    /// P(the remote answers 429 or 503 without handling the call).
    pub outbound_throttle: f64,
    /// P(a call carries extra think time from [10·hi, 100·hi], hi =
    /// `outbound_latency.1`, or [1 s, 100 s] when hi is zero — the LLM tail
    /// that outlives a deadline (plan amendment A15).
    pub outbound_slow: f64,
    /// One-way latency range for an outbound request and for its reply.
    pub outbound_latency: (Duration, Duration),
```

- `Default`: every outbound rate `0.0`; `outbound_latency: (Duration::from_millis(1), Duration::from_millis(50))`.
- `none()` and `swarm()`'s struct literal: every outbound rate `0.0` and `outbound_latency: (Duration::ZERO, Duration::ZERO)`. In `swarm` they go in the "draw from field-keyed sub-streams" group, with that comment extended to name them.
- `validate`: add the five rates to the `[0, 1]` list, plus `assert!(self.outbound_latency.0 <= self.outbound_latency.1, "outbound_latency range inverted");`.

Append to `sample_extended_fields`, after `object_delete_error`. Keep the existing body unchanged above it, and copy that unchanged body into `#[cfg(test)] fn sample_extended_fields_without_outbound_for_tests`:

```rust
    let mut r = keyed_stream(seed, "outbound_refused");
    cfg.outbound_refused = if r.chance(0.25) { r.below(50) as f64 / 1000.0 } else { 0.0 };
    let mut r = keyed_stream(seed, "outbound_lost_request");
    cfg.outbound_lost_request = if r.chance(0.25) { r.below(30) as f64 / 1000.0 } else { 0.0 };
    let mut r = keyed_stream(seed, "outbound_lost_reply");
    cfg.outbound_lost_reply = if r.chance(0.25) { r.below(30) as f64 / 1000.0 } else { 0.0 };
    let mut r = keyed_stream(seed, "outbound_throttle");
    cfg.outbound_throttle = if r.chance(0.25) { r.below(100) as f64 / 1000.0 } else { 0.0 };
    let mut r = keyed_stream(seed, "outbound_slow");
    cfg.outbound_slow = if r.chance(0.25) { r.below(50) as f64 / 1000.0 } else { 0.0 };
    const OUTBOUND_HI: [Duration; 4] = [
        Duration::ZERO,
        Duration::from_millis(10),
        Duration::from_millis(200),
        Duration::from_secs(2),
    ];
    let mut r = keyed_stream(seed, "outbound_latency");
    cfg.outbound_latency = (Duration::ZERO, OUTBOUND_HI[r.below(4) as usize]);
```

`FaultConfig` already derives `Clone` (`lib.rs:33`), and production code clones it (`run_universe`'s `cfg.clone()`), so the test needs no helper. Never remove that derive.

- [ ] **Step 4: Carry the axes into the JSON record and re-pin the Debug line**

- In `harness.rs`'s `config_json`, add `,\"outbound_refused\":{},\"outbound_lost_request\":{},\"outbound_lost_reply\":{},\"outbound_throttle\":{},\"outbound_slow\":{},\"outbound_latency\":{{\"lo_ns\":{},\"hi_ns\":{}}}` before the closing `}}` of the second `format!`, with the six values. `config_json_carries_every_fault_axis` then passes, and it must fail before you add these. Run it once first and see the failure.
- In `human_failure_block_matches_the_pinned_shape`, add the six fields to its `FaultConfig` literal (`harness.rs:774`; rates `0.0`, latency `(Duration::ZERO, Duration::ZERO)`). Extend the expected Debug string (`:809`), inserting after `subscriber_stall: 0.0, `: `outbound_refused: 0.0, outbound_lost_request: 0.0, outbound_lost_reply: 0.0, outbound_throttle: 0.0, outbound_slow: 0.0, outbound_latency: (0ns, 0ns), `. The field order is declaration order: the six sit between `subscriber_stall` and `disk_latency`.
- `config_json_carries_every_fault_axis`'s literal (`harness.rs:966`) takes the same six fields. No other literal outside `lib.rs`'s three needs them: the rest use `..base`.
- GUIDE §8 (`docs/GUIDE.md:3218`): replace the abbreviated `config:` line with the exact string the pinned test now expects. It has been stale since `dir_entry_loss` landed; hard rule 6.

- [ ] **Step 5: Run the sim crate and oracle 3**

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
cargo test -p charpente-sim --no-fail-fast 2>&1 | tee $SDD/gate-task-3.log; grep -c "^test result: ok" $SDD/gate-task-3.log
perl -e 'alarm 590; exec @ARGV' -- $SDD/oracle3.sh task3   # oracle3 mode foreground; background: Global Constraints
```

Expected: every sim suite ok, and oracle 3 exit 0. A changed verdict means a draw moved, so stop and report it; don't update the baseline.

- [ ] **Step 6: Workspace gate and commit**

Run the three gate commands (the workspace test partitioned or backgrounded), count, and compare with the base count.

```bash
git add crates/charpente-sim/src/lib.rs crates/charpente-sim/src/harness.rs docs/GUIDE.md <each literal file the compiler named>
git commit -m "sim: outbound fault axes on FaultConfig, each from its own keyed stream (spec §10); oracle 3 unchanged"
```

---

### Task 4: `SimOutbound`, `Remote` models, and the outbound world

**Files:**
- Create: `crates/charpente-sim/src/outbound.rs`
- Modify: `crates/charpente-sim/src/lib.rs` (`pub mod outbound;`, `State.outbound`, `Sim::{remote, outbound, with_remote, outbound_deliveries}`)

**Interfaces:**
- Consumes: Task 2's types; Task 3's axes.
- Produces (`charpente_sim::outbound`):
  - `pub trait Remote: Any + Send { fn handle(&mut self, call: &OutboundCall, rng: &mut Rng) -> OutboundReply; }`
  - `pub fn from_fn<F: FnMut(&OutboundCall) -> OutboundReply + Send + 'static>(f: F) -> FnRemote<F>`
  - `pub struct Recorder` (`Recorder::new()`, `calls(&self) -> &[OutboundCall]`)
  - `pub struct Idempotent<M>` (`Idempotent::header(name: &str, inner: M)`, `Idempotent::by(extract: fn(&OutboundCall) -> Option<String>, inner: M)`, `applied(&self) -> &BTreeMap<String, u32>`, `inner(&self) -> &M`)
  - `pub struct Delivered { pub host: String, pub key: IdempotencyKey, pub at: u64, pub handle: u64 }`
  - `pub struct SimOutbound` implementing `charpente_core::io::Outbound`
- Produces (`Sim`):
  - `sim.remote(host: &str, model: impl Remote)`
  - `sim.outbound(hosts: &[&'static str]) -> io::Result<SimOutbound>`
  - `sim.with_remote<M: Remote, T>(host: &str, f: impl FnOnce(&M) -> T) -> T`
  - `sim.outbound_deliveries() -> Vec<Delivered>`

- [ ] **Step 1: Write the failing tests** (in `outbound.rs`'s own `#[cfg(test)] mod tests`)

Create `crates/charpente-sim/src/outbound.rs` holding only this test module, and add `pub mod outbound;` to `lib.rs`'s module list now, so the file is compiled. Without the declaration the file is never built, and Step 2's filter would come back green on Task 3's tests.

```rust
use super::*;
use crate::{FaultConfig, Sim};
use charpente_core::io::{
    Clock, HttpMethod, IdempotencyKey, Instant, Outbound, OutboundCall, OutboundReply, OutboundWake,
};
use std::time::Duration;

fn call(n: u8) -> OutboundCall {
    OutboundCall {
        host: "pay.test",
        method: HttpMethod::Post,
        path: "/charge".into(),
        headers: vec![("idempotency-key".into(), format!("k{n}"))],
        body: vec![n],
        key: IdempotencyKey::from_bytes([n; 16]),
    }
}

fn faults(f: impl FnOnce(&mut FaultConfig)) -> FaultConfig {
    let mut cfg = FaultConfig::none();
    f(&mut cfg);
    cfg
}

fn drain(ob: &SimOutbound) -> Vec<(u64, io::Result<OutboundReply>)> {
    std::iter::from_fn(|| ob.poll()).collect()
}

#[test]
fn a_host_with_no_model_is_refused_by_name() {
    let sim = Sim::with_faults(1, FaultConfig::none());
    let err = sim.outbound(&["pay.test"]).err().expect("no model registered");
    assert_eq!(
        err.to_string(),
        "sim: a mounted job calls pay.test, which has no model — add sim.remote(\"pay.test\", …) before sim.outbound(…)"
    );
}

#[test]
fn a_fault_free_call_is_delivered_then_answered() {
    let sim = Sim::with_faults(1, FaultConfig::none());
    sim.remote("pay.test", Recorder::new());
    let ob = sim.outbound(&["pay.test"]).unwrap();
    ob.start(0, call(1)).unwrap();
    let done = drain(&ob);
    assert_eq!(done.len(), 1);
    assert_eq!(done[0].0, 0);
    assert_eq!(done[0].1.as_ref().unwrap().status, 200);
    sim.with_remote("pay.test", |r: &Recorder| assert_eq!(r.calls().len(), 1));
    assert_eq!(sim.outbound_deliveries().len(), 1);
}

#[test]
fn a_refused_call_never_reaches_the_remote() {
    let sim = Sim::with_faults(1, faults(|c| c.outbound_refused = 1.0));
    sim.remote("pay.test", Recorder::new());
    let ob = sim.outbound(&["pay.test"]).unwrap();
    ob.start(0, call(1)).unwrap();
    let done = drain(&ob);
    assert_eq!(done[0].1.as_ref().unwrap_err().kind(), io::ErrorKind::ConnectionRefused);
    sim.with_remote("pay.test", |r: &Recorder| assert!(r.calls().is_empty()));
    assert_eq!(sim.coverage().get("fault:outbound_refused"), Some(&1));
}

#[test]
fn a_lost_reply_applies_at_the_remote_and_never_completes() {
    let sim = Sim::with_faults(1, faults(|c| c.outbound_lost_reply = 1.0));
    sim.remote("pay.test", Recorder::new());
    let ob = sim.outbound(&["pay.test"]).unwrap();
    ob.start(0, call(1)).unwrap();
    assert!(drain(&ob).is_empty());
    assert_eq!(ob.next_wake(), OutboundWake::Never, "a lost reply is never due");
    sim.with_remote("pay.test", |r: &Recorder| assert_eq!(r.calls().len(), 1));
    assert_eq!(sim.coverage().get("fault:outbound_lost_reply"), Some(&1));
}

#[test]
fn a_lost_request_reaches_nothing() {
    let sim = Sim::with_faults(1, faults(|c| c.outbound_lost_request = 1.0));
    sim.remote("pay.test", Recorder::new());
    let ob = sim.outbound(&["pay.test"]).unwrap();
    ob.start(0, call(1)).unwrap();
    assert!(drain(&ob).is_empty());
    sim.with_remote("pay.test", |r: &Recorder| assert!(r.calls().is_empty()));
}

#[test]
fn a_throttle_answers_429_or_503_without_handling() {
    let sim = Sim::with_faults(1, faults(|c| c.outbound_throttle = 1.0));
    sim.remote("pay.test", Recorder::new());
    let ob = sim.outbound(&["pay.test"]).unwrap();
    ob.start(0, call(1)).unwrap();
    let status = drain(&ob)[0].1.as_ref().unwrap().status;
    assert!(status == 429 || status == 503, "{status}");
    sim.with_remote("pay.test", |r: &Recorder| assert!(r.calls().is_empty()));
}

#[test]
fn latency_delays_completion_and_next_wake_names_it() {
    let sim = Sim::with_faults(
        1,
        faults(|c| c.outbound_latency = (Duration::from_millis(10), Duration::from_millis(10))),
    );
    sim.remote("pay.test", Recorder::new());
    let ob = sim.outbound(&["pay.test"]).unwrap();
    ob.start(0, call(1)).unwrap();
    assert!(drain(&ob).is_empty(), "nothing before 20ms");
    assert_eq!(ob.next_wake(), OutboundWake::At(Instant::from_nanos(20_000_000)));
    sim.clock().sleep(Duration::from_millis(20));
    assert_eq!(drain(&ob).len(), 1);
}

#[test]
fn a_cancelled_call_still_arrives_but_never_completes() {
    let sim = Sim::with_faults(
        1,
        faults(|c| c.outbound_latency = (Duration::from_millis(10), Duration::from_millis(10))),
    );
    sim.remote("pay.test", Recorder::new());
    let ob = sim.outbound(&["pay.test"]).unwrap();
    ob.start(0, call(1)).unwrap();
    ob.cancel(0);
    sim.clock().sleep(Duration::from_millis(50));
    assert!(drain(&ob).is_empty());
    sim.with_remote("pay.test", |r: &Recorder| assert_eq!(r.calls().len(), 1));
}

/// Amendment A3: a reply goes only to the handle that started the call.
#[test]
fn a_new_handle_never_receives_an_old_handles_reply() {
    let sim = Sim::with_faults(
        1,
        faults(|c| c.outbound_latency = (Duration::from_millis(10), Duration::from_millis(10))),
    );
    sim.remote("pay.test", Recorder::new());
    let old = sim.outbound(&["pay.test"]).unwrap();
    old.start(0, call(1)).unwrap();
    let new = sim.outbound(&["pay.test"]).unwrap();
    new.start(0, call(2)).unwrap(); // the same token, as a restarted kernel's first call
    sim.clock().sleep(Duration::from_millis(50));
    let got = drain(&new);
    assert_eq!(got.len(), 1);
    assert_eq!(sim.outbound_deliveries().len(), 2, "both requests were on the wire");
}

/// Answers 200 with how many calls it has handled — a named type, so
/// `with_remote` can name it to downcast.
struct Counter(u32);

impl Remote for Counter {
    fn handle(&mut self, _call: &OutboundCall, _rng: &mut Rng) -> OutboundReply {
        self.0 += 1;
        OutboundReply { status: 200, headers: vec![], body: self.0.to_string().into_bytes() }
    }
}

#[test]
fn a_closure_model_answers_through_from_fn() {
    let sim = Sim::with_faults(1, FaultConfig::none());
    sim.remote(
        "pay.test",
        from_fn(|c: &OutboundCall| OutboundReply { status: 201, headers: vec![], body: c.body.clone() }),
    );
    let ob = sim.outbound(&["pay.test"]).unwrap();
    ob.start(0, call(7)).unwrap();
    let done = drain(&ob);
    let reply = done[0].1.as_ref().unwrap();
    assert_eq!((reply.status, reply.body.clone()), (201, vec![7]));
}

#[test]
fn idempotent_replays_the_first_reply_and_refuses_new_parameters() {
    let sim = Sim::with_faults(1, FaultConfig::none());
    sim.remote("pay.test", Idempotent::header("idempotency-key", Counter(0)));
    let ob = sim.outbound(&["pay.test"]).unwrap();
    ob.start(0, call(1)).unwrap();
    ob.start(1, call(1)).unwrap(); // same key, same parameters: replayed
    let mut other = call(1);
    other.body = vec![9]; // same key, new parameters
    ob.start(2, other).unwrap();
    let mut keyless = call(3);
    keyless.headers.clear();
    ob.start(3, keyless.clone()).unwrap();
    ob.start(4, keyless).unwrap(); // no key: applied every time
    let done = drain(&ob);
    let body = |t: u64| {
        let r = done.iter().find(|d| d.0 == t).unwrap().1.as_ref().unwrap();
        (r.status, String::from_utf8(r.body.clone()).unwrap())
    };
    assert_eq!(body(0), (200, "1".into()));
    assert_eq!(body(1), (200, "1".into()));
    assert_eq!(body(2).0, 409);
    assert_eq!(body(3), (200, "2".into()));
    assert_eq!(body(4), (200, "3".into()));
    sim.with_remote("pay.test", |m: &Idempotent<Counter>| {
        assert_eq!(m.applied().get("k1"), Some(&1));
        assert_eq!(m.inner().0, 3, "k1 once, then the keyless call twice");
    });
}

#[test]
fn a_universe_replays_its_outbound_world_byte_for_byte() {
    let run = |seed| {
        let mut cfg = FaultConfig::swarm(&mut crate::rng::Rng::new(seed));
        crate::sample_extended_fields(seed, &mut cfg);
        let sim = Sim::with_faults(seed, cfg);
        sim.remote("pay.test", Recorder::new());
        let ob = sim.outbound(&["pay.test"]).unwrap();
        for t in 0..20 {
            ob.start(t, call(t as u8)).unwrap();
        }
        sim.clock().sleep(Duration::from_secs(300));
        let done: Vec<_> = drain(&ob)
            .into_iter()
            .map(|(t, r)| (t, r.map(|r| r.status).map_err(|e| e.kind())))
            .collect();
        (done, sim.outbound_deliveries().len(), sim.coverage())
    };
    for seed in [3u64, 0x912313adf8115ad0] {
        assert_eq!(format!("{:?}", run(seed)), format!("{:?}", run(seed)), "seed {seed:#x}");
    }
}
```

`Idempotent`'s `applied()` key in the test is the header *value* (`"k1"`). The model keys by what the app sent, never by `OutboundCall::key`.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p charpente-sim --lib outbound::tests`
Expected: FAIL to compile, "cannot find type `SimOutbound` in this scope" and "no method named `outbound` found for struct `Sim`".

- [ ] **Step 3: Write `outbound.rs`**

```rust
//! The simulated outside world for outbound calls (outbound-effects spec §10).
//! One world per universe, shared by every `SimOutbound` handle: a crash kills
//! the box, never the remote, so models and requests already on the wire
//! survive a restart. Each handle is one process's view (plan amendment A3): a
//! reply goes only to the handle that started its call. Every fault axis rolls
//! from its own keyed stream — a shared stream would let a 0.0 axis shift every
//! later draw (`object.rs`'s lesson). Delivery and reply times are drawn at
//! `start`, so `poll` is a pure virtual-time check. Models run under the world
//! lock: a model must never call back into the `Sim`.

use std::any::Any;
use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use charpente_core::io::{
    IdempotencyKey, Instant, Outbound, OutboundCall, OutboundReply, OutboundWake,
};

use crate::rng::Rng;
use crate::{State, World, keyed_stream};

/// The outside world behind one declared host. Deterministic: its own
/// variance draws from `rng`, the model's own seeded stream.
pub trait Remote: Any + Send {
    /// Runs when the request REACHES the remote, at its drawn delivery time.
    fn handle(&mut self, call: &OutboundCall, rng: &mut Rng) -> OutboundReply;
}

/// A closure as a model (plan amendment A1).
pub struct FnRemote<F>(F);

pub fn from_fn<F>(f: F) -> FnRemote<F>
where
    F: FnMut(&OutboundCall) -> OutboundReply + Send + 'static,
{
    FnRemote(f)
}

impl<F> Remote for FnRemote<F>
where
    F: FnMut(&OutboundCall) -> OutboundReply + Send + 'static,
{
    fn handle(&mut self, call: &OutboundCall, _rng: &mut Rng) -> OutboundReply {
        (self.0)(call)
    }
}

/// Answers 200 with an empty body and keeps every call it was handed, in
/// delivery order — fire-and-forget email.
#[derive(Default)]
pub struct Recorder {
    calls: Vec<OutboundCall>,
}

impl Recorder {
    pub fn new() -> Recorder {
        Recorder::default()
    }

    pub fn calls(&self) -> &[OutboundCall] {
        &self.calls
    }
}

impl Remote for Recorder {
    fn handle(&mut self, call: &OutboundCall, _rng: &mut Rng) -> OutboundReply {
        self.calls.push(call.clone());
        OutboundReply { status: 200, headers: Vec::new(), body: Vec::new() }
    }
}

/// Stripe's idempotency semantics over an inner model, keyed by what the APP
/// sent — never by `OutboundCall::key`, which no real remote can see. A
/// repeated key replays the first reply; a reused key with different
/// parameters answers 409; a request carrying no key is applied every time.
pub struct Idempotent<M> {
    extract: fn(&OutboundCall) -> Option<String>,
    header: Option<String>,
    inner: M,
    first: BTreeMap<String, (Vec<u8>, String, OutboundReply)>,
    applied: BTreeMap<String, u32>,
}

impl<M: Remote> Idempotent<M> {
    pub fn header(name: &str, inner: M) -> Idempotent<M> {
        Idempotent {
            extract: |_| None,
            header: Some(name.to_ascii_lowercase()),
            inner,
            first: BTreeMap::new(),
            applied: BTreeMap::new(),
        }
    }

    pub fn by(extract: fn(&OutboundCall) -> Option<String>, inner: M) -> Idempotent<M> {
        Idempotent { extract, header: None, inner, first: BTreeMap::new(), applied: BTreeMap::new() }
    }

    /// Applications per key — 1 for every key a correct app sent.
    pub fn applied(&self) -> &BTreeMap<String, u32> {
        &self.applied
    }

    pub fn inner(&self) -> &M {
        &self.inner
    }

    fn key_of(&self, call: &OutboundCall) -> Option<String> {
        match &self.header {
            Some(name) => call
                .headers
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone()),
            None => (self.extract)(call),
        }
    }
}

impl<M: Remote> Remote for Idempotent<M> {
    fn handle(&mut self, call: &OutboundCall, rng: &mut Rng) -> OutboundReply {
        let Some(key) = self.key_of(call) else {
            return self.inner.handle(call, rng);
        };
        let params = format!("{} {}", call.method.as_str(), call.path);
        if let Some((body, first_params, reply)) = self.first.get(&key) {
            if *body == call.body && *first_params == params {
                return reply.clone();
            }
            return OutboundReply {
                status: 409,
                headers: Vec::new(),
                body: b"idempotency key reused with different parameters".to_vec(),
            };
        }
        let reply = self.inner.handle(call, rng);
        *self.applied.entry(key.clone()).or_insert(0) += 1;
        self.first.insert(key, (call.body.clone(), params, reply.clone()));
        reply
    }
}

/// One request that reached a remote — the checkers' evidence (§10).
#[derive(Clone, Debug)]
pub struct Delivered {
    pub host: String,
    pub key: IdempotencyKey,
    /// Virtual nanos at delivery.
    pub at: u64,
    /// The `SimOutbound` handle (process) that sent it.
    pub handle: u64,
}

enum Fate {
    /// Reaches the remote at `deliver_at`; its reply returns at `reply_at`
    /// unless `reply_lost`.
    Delivered { deliver_at: u64, reply_at: u64, reply_lost: bool },
    /// Never reaches the remote; completes at `at` with `result` (a refusal or
    /// a throttle), or never (`at == None`, a lost request).
    Short { at: Option<u64>, result: Option<io::Result<OutboundReply>> },
}

struct Flight {
    handle: u64,
    token: u64,
    call: OutboundCall,
    fate: Fate,
    cancelled: bool,
    /// The model's reply once delivered; taken at completion.
    reply: Option<OutboundReply>,
    delivered: bool,
}

pub(crate) struct OutboundWorld {
    // Read by `Sim::{remote, outbound, with_remote, outbound_deliveries}` in lib.rs.
    pub(crate) models: BTreeMap<String, (Box<dyn Remote>, Rng)>,
    flights: BTreeMap<u64, Flight>,
    next_flight: u64,
    pub(crate) next_handle: u64,
    pub(crate) deliveries: Vec<Delivered>,
    refused: Rng,
    lost_request: Rng,
    lost_reply: Rng,
    throttle: Rng,
    slow: Rng,
    latency: Rng,
}

impl OutboundWorld {
    pub(crate) fn new(seed: u64) -> OutboundWorld {
        OutboundWorld {
            models: BTreeMap::new(),
            flights: BTreeMap::new(),
            next_flight: 0,
            next_handle: 0,
            deliveries: Vec::new(),
            refused: keyed_stream(seed, "outbound-rt:refused"),
            lost_request: keyed_stream(seed, "outbound-rt:lost_request"),
            lost_reply: keyed_stream(seed, "outbound-rt:lost_reply"),
            throttle: keyed_stream(seed, "outbound-rt:throttle"),
            slow: keyed_stream(seed, "outbound-rt:slow"),
            latency: keyed_stream(seed, "outbound-rt:latency"),
        }
    }

    /// Runs every delivery due by `now`, in (delivery time, flight) order —
    /// on the wire, whoever's handle sent it.
    pub(crate) fn deliver_due(&mut self, now: u64) {
        let due: Vec<u64> = self
            .flights
            .iter()
            .filter(|(_, f)| !f.delivered)
            .filter_map(|(id, f)| match f.fate {
                Fate::Delivered { deliver_at, .. } if deliver_at <= now => Some((deliver_at, *id)),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(|(_, id)| id)
            .collect();
        for id in due {
            let flight = self.flights.get_mut(&id).expect("listed above");
            let Fate::Delivered { deliver_at, .. } = flight.fate else { unreachable!() };
            let (model, rng) = self
                .models
                .get_mut(flight.call.host)
                .expect("sim.outbound refused any host without a model");
            flight.reply = Some(model.handle(&flight.call, rng));
            flight.delivered = true;
            self.deliveries.push(Delivered {
                host: flight.call.host.to_string(),
                key: flight.call.key,
                at: deliver_at,
                handle: flight.handle,
            });
        }
    }
}

fn draw(rng: &mut Rng, (lo, hi): (Duration, Duration)) -> u64 {
    let (lo, hi) = (lo.as_nanos() as u64, hi.as_nanos() as u64);
    lo + if hi > lo { rng.below(hi - lo + 1) } else { 0 }
}

/// One process's outbound handle.
pub struct SimOutbound {
    pub(crate) world: Arc<World>,
    pub(crate) handle: u64,
}

impl Outbound for SimOutbound {
    fn start(&self, token: u64, call: OutboundCall) -> io::Result<()> {
        let mut guard = self.world.lock();
        let cfg = &self.world.cfg;
        let now = guard.now;
        let State { outbound, coverage, .. } = &mut *guard;
        assert!(
            !outbound
                .flights
                .values()
                .any(|f| f.handle == self.handle && f.token == token && !f.cancelled),
            "contract: Outbound::start({token}) while that token is in flight"
        );
        let fate = if outbound.refused.chance(cfg.outbound_refused) {
            *coverage.entry("fault:outbound_refused").or_insert(0) += 1;
            Fate::Short {
                at: Some(now),
                result: Some(Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "connection refused",
                ))),
            }
        } else if outbound.lost_request.chance(cfg.outbound_lost_request) {
            *coverage.entry("fault:outbound_lost_request").or_insert(0) += 1;
            Fate::Short { at: None, result: None }
        } else if outbound.throttle.chance(cfg.outbound_throttle) {
            *coverage.entry("fault:outbound_throttle").or_insert(0) += 1;
            let status = if outbound.throttle.chance(0.5) { 429 } else { 503 };
            let at = now + draw(&mut outbound.latency, cfg.outbound_latency) * 2;
            Fate::Short {
                at: Some(at),
                result: Some(Ok(OutboundReply { status, headers: Vec::new(), body: Vec::new() })),
            }
        } else {
            let up = draw(&mut outbound.latency, cfg.outbound_latency);
            let think = if outbound.slow.chance(cfg.outbound_slow) {
                *coverage.entry("fault:outbound_slow").or_insert(0) += 1;
                let hi = cfg.outbound_latency.1;
                let (lo, hi) = if hi.is_zero() {
                    (Duration::from_secs(1), Duration::from_secs(100))
                } else {
                    (hi * 10, hi * 100)
                };
                draw(&mut outbound.slow, (lo, hi))
            } else {
                0
            };
            let down = draw(&mut outbound.latency, cfg.outbound_latency);
            let reply_lost = outbound.lost_reply.chance(cfg.outbound_lost_reply);
            if reply_lost {
                *coverage.entry("fault:outbound_lost_reply").or_insert(0) += 1;
            }
            Fate::Delivered { deliver_at: now + up, reply_at: now + up + think + down, reply_lost }
        };
        let id = outbound.next_flight;
        outbound.next_flight += 1;
        outbound.flights.insert(
            id,
            Flight { handle: self.handle, token, call, fate, cancelled: false, reply: None, delivered: false },
        );
        Ok(())
    }

    fn poll(&self) -> Option<(u64, io::Result<OutboundReply>)> {
        let mut guard = self.world.lock();
        let now = guard.now;
        let outbound = &mut guard.outbound;
        outbound.deliver_due(now);
        let next = outbound
            .flights
            .iter()
            .filter(|(_, f)| f.handle == self.handle && !f.cancelled)
            .filter_map(|(id, f)| completes_at(f).filter(|at| *at <= now).map(|at| (at, *id)))
            .min()?;
        let flight = outbound.flights.get_mut(&next.1).expect("found above");
        let result = match &mut flight.fate {
            Fate::Short { result, .. } => result.take().expect("a short fate completes once"),
            Fate::Delivered { .. } => Ok(flight.reply.take().expect("delivered before its reply")),
        };
        let token = flight.token;
        outbound.flights.remove(&next.1);
        guard.cover("outbound:completed");
        Some((token, result))
    }

    fn cancel(&self, token: u64) {
        let mut guard = self.world.lock();
        for f in guard.outbound.flights.values_mut() {
            if f.handle == self.handle && f.token == token {
                f.cancelled = true;
            }
        }
    }

    fn next_wake(&self) -> OutboundWake {
        let guard = self.world.lock();
        guard
            .outbound
            .flights
            .values()
            .filter(|f| f.handle == self.handle && !f.cancelled)
            .filter_map(completes_at)
            .min()
            .map_or(OutboundWake::Never, |at| OutboundWake::At(Instant::from_nanos(at)))
    }
}

fn completes_at(f: &Flight) -> Option<u64> {
    match f.fate {
        Fate::Delivered { reply_at, reply_lost: false, .. } => Some(reply_at),
        Fate::Delivered { reply_lost: true, .. } => None,
        Fate::Short { at, .. } => at,
    }
}
```

A completed flight is removed in `poll`, so `!f.cancelled` is the whole "still in flight" filter. A cancelled flight stays in the map, so its request still reaches the remote at its delivery time (a cancel cannot recall a request on the wire). Its completion is never handed out.

In `lib.rs`:
- `pub mod outbound;` (added in Step 1)
- `State` gains `outbound: outbound::OutboundWorld`, built in `Sim::with_faults` as `outbound::OutboundWorld::new(seed)`. That's keyed streams only; no existing stream is touched.
- `World::lock`, `State`, `World` and `keyed_stream` become `pub(crate)` where `outbound.rs` needs them. `State::cover` already exists.
- Add the four `Sim` methods:

```rust
    /// Registers the model for `host` (spec §10). One model per host; a second
    /// registration is a contract panic.
    pub fn remote(&self, host: &str, model: impl outbound::Remote) {
        let mut guard = self.world.lock();
        let rng = keyed_stream(self.world.seed, &format!("outbound-model:{host}"));
        let clash = guard.outbound.models.insert(host.to_string(), (Box::new(model), rng));
        assert!(clash.is_none(), "contract: sim.remote(\"{host}\", …) registered twice");
    }

    /// A new process's outbound handle. `hosts` is the app's declared host list
    /// (`App::outbound_hosts`) — plain data, since this crate cannot take an
    /// `App`. Refuses a host with no model, by name.
    pub fn outbound(&self, hosts: &[&'static str]) -> io::Result<outbound::SimOutbound> {
        let mut guard = self.world.lock();
        for host in hosts {
            if !guard.outbound.models.contains_key(*host) {
                return Err(io::Error::other(format!(
                    "sim: a mounted job calls {host}, which has no model — add \
                     sim.remote(\"{host}\", …) before sim.outbound(…)"
                )));
            }
        }
        let handle = guard.outbound.next_handle;
        guard.outbound.next_handle += 1;
        Ok(outbound::SimOutbound { world: self.world.clone(), handle })
    }

    /// Reads `host`'s model after every delivery due by now has run (amendment
    /// A1: a reference cannot escape the world lock).
    pub fn with_remote<M: outbound::Remote, T>(&self, host: &str, f: impl FnOnce(&M) -> T) -> T {
        let mut guard = self.world.lock();
        let now = guard.now;
        guard.outbound.deliver_due(now);
        let (model, _) = guard
            .outbound
            .models
            .get(host)
            .unwrap_or_else(|| panic!("contract: sim.with_remote(\"{host}\") — no model registered"));
        let any: &dyn std::any::Any = model.as_ref();
        f(any.downcast_ref::<M>().unwrap_or_else(|| {
            panic!("contract: sim.with_remote(\"{host}\") asked for the wrong model type")
        }))
    }

    /// Every request that reached a remote so far, after running deliveries due by now.
    pub fn outbound_deliveries(&self) -> Vec<outbound::Delivered> {
        let mut guard = self.world.lock();
        let now = guard.now;
        guard.outbound.deliver_due(now);
        guard.outbound.deliveries.clone()
    }
```

`model.as_ref()` coerces `&dyn Remote` to `&dyn Any` by trait upcasting, which is stable since Rust 1.86; the pin is 1.99.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p charpente-sim --lib outbound::tests`
Expected: PASS, all twelve. (The bare filter `outbound` would also run Task 3's three `outbound_ax…` tests and report 15.)

- [ ] **Step 5: Prove a test can fail**

Change `Idempotent::handle`'s replay arm to fall through to the inner model, so a repeat key is applied twice. Run `idempotent_replays_the_first_reply_and_refuses_new_parameters` and expect FAIL. Undo the edit and `touch crates/charpente-sim/src/outbound.rs`; restoring by `mv` keeps the old mtime, and cargo then reuses the mutated build. Record the red run in progress.md.

- [ ] **Step 6: GUIDE §8, the sim surface (hard rule 6)**

GUIDE §8 gains "The outside world: remote models". This step is prose and a table only. The compiled end-to-end workload lands in Task 12, once a kernel can run one. Cover:
- `sim.remote(host, model)`, one per declared host and registered twice is a panic;
- the `Remote` trait, with the model's own `rng`;
- `charpente_sim::outbound::from_fn` for a closure;
- the stock `Recorder` and `Idempotent::header(name, model)` / `Idempotent::by(extract, model)`, each a named choice and none a default;
- `sim.with_remote::<M, _>(host, |m| …)` to read model state;
- `sim.outbound_deliveries()`;
- the refusal text for a host with no model;
- spec §10's five-row axes table, with the six `FaultConfig` field names and their coverage keys;
- "a request in flight at a crash still arrives; only its reply is lost".

Run `cargo build -p guide-examples` and `cargo test -p guide-examples --no-fail-fast`. `doc_truth` and `claims_census` must stay green.

- [ ] **Step 7: Gate, oracle 3, commit**

Run the three gate commands, then oracle 3 (expect exit 0: `OutboundWorld::new` draws nothing, and only keyed streams are built):

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
perl -e 'alarm 590; exec @ARGV' -- $SDD/oracle3.sh task4   # oracle3 mode foreground; background: Global Constraints
```

```bash
git add crates/charpente-sim/src/outbound.rs crates/charpente-sim/src/lib.rs docs/GUIDE.md
git commit -m "sim: SimOutbound, Remote models (Recorder, Idempotent, from_fn), the outbound world (spec §10)"
```

---

### Task 5: Minted idempotency keys — record tags `0x0c`/`0x0d`, `JobRow.outbound_key`, minting groups

**Files:**
- Modify: `crates/charpente-core/src/db.rs`. Covers: module doc; `Job` trait; `scoped_fingerprint` plus a new `outbound_fingerprint`; `Enqueue`; `push_enqueue`; `encode_record`/`decode_record`; `Effect::Enqueue`; the apply arm (`:2660`); `TableMachine.enqueued`; `drain_enqueued`; `Group`; `begin_group`; `Group::stage`; `enqueue_keyed`'s doc (`:1456`); `decode_refuses_unknown_tags`.
- Modify: `crates/charpente-core/src/job.rs` (`JobRow` and its codec), `crates/charpente-core/src/table.rs:1601` (`materialize_job`).
- Modify: `crates/charpente/src/http/app.rs` (`begin_group` mints; new `begin_job_group`, `transact_for_tests`), `crates/charpente/src/kernel/mod.rs` (job and cron arms), `crates/charpente/src/kernel/dispatch.rs` (`note_enqueued`).
- Modify: every `JobRow { … }` and `Effect::Enqueue { … }` literal and every `drain_enqueued()` use the compiler names. Known: `charpente-core/tests/materialize.rs:103,126`, `charpente-core/tests/engine.rs:532,1191` (two `JobRow` literals), `charpente/tests/kernel.rs:3658`, `charpente/tests/transactions.rs:3886`, `charpente/tests/typed_tables.rs:1070-1077` (`materialize_job` calls), and the compile-fail fixtures `job_row_forged_on_bare_tx.rs:44` and `job_row_table_name_borrowed.rs:74`.
- Modify: `DESIGN.md`'s record-tag list (the `0x08`/`0x0a` paragraph near `:607-612`).
- Test: `crates/charpente-core/tests/materialize.rs`, plus `db.rs`/`job.rs` unit tests.

**Interfaces:**
- Produces (`charpente_core::db`):
  - `Job::OUTBOUND: bool` (default `false`)
  - `pub const fn outbound_fingerprint(fingerprint: u64) -> u64`
  - `pub struct Enqueued { pub id: Id<JobRow>, pub outbound: bool }`
  - `Db::drain_enqueued(&mut self) -> Vec<Enqueued>`
  - `Db::begin_group_minting<'g>(&'g mut self, mint: &'g dyn Entropy) -> io::Result<Group<'g>>`
  - `Effect::Enqueue { …, outbound_key: Option<[u8; 16]> }`
- Produces (`charpente_core::job`): `JobRow.outbound_key: Option<[u8; 16]>`.
- Produces (`charpente::http::App`):
  - `pub(crate) fn begin_job_group(&mut self) -> io::Result<Group<'_>>`
  - `#[doc(hidden)] pub fn transact_for_tests<T, E>(&mut self, f: impl FnOnce(&mut Tx) -> Result<T, E>) -> Result<T, TxError<E>>`
- Produces (`Dispatcher`): `note_enqueued(&mut self, enqueued: Vec<Enqueued>)`.

- [ ] **Step 1: Write the failing codec tests** (`db.rs`'s test module, beside `decode_refuses_unknown_tags`)

```rust
    /// Outbound-effects spec §6: `0x0c` is `0x08` plus the 16 key bytes, and
    /// `0x0d` is `0x0a` plus them. Byte-pinned, so a reordered field is a red test.
    #[test]
    fn outbound_enqueues_record_tags_0x0c_and_0x0d_with_their_key() {
        let mut effects = TxEffects::default();
        effects.enqueues.push(Enqueue {
            job: "Charge",
            payload: vec![0xAB],
            key: None,
            shape: 0x1122_3344_5566_7788,
            scope: None,
            outbound: true,
            okey: Some([7; 16]),
        });
        effects.enqueues.push(Enqueue {
            job: "Charge",
            payload: vec![],
            key: Some("k".into()),
            shape: 1,
            scope: Some(EnqueuedTenant {
                id: 9,
                scope_type: TypeId::of::<()>(),
                scope_name: "Org",
            }),
            outbound: true,
            okey: Some([0xEE; 16]),
        });
        let bytes = encode_record(&effects);
        let mut expected = Vec::new();
        2u32.encode(&mut expected);
        expected.push(0x0c);
        encode_bytes(b"Charge", &mut expected);
        encode_bytes(&[0xAB], &mut expected);
        Option::<String>::None.encode(&mut expected);
        0x1122_3344_5566_7788u64.encode(&mut expected);
        expected.extend_from_slice(&[7; 16]);
        expected.push(0x0d);
        encode_bytes(b"Charge", &mut expected);
        encode_bytes(&[], &mut expected);
        Some("k".to_string()).encode(&mut expected);
        1u64.encode(&mut expected);
        9u64.encode(&mut expected);
        expected.extend_from_slice(&[0xEE; 16]);
        assert_eq!(bytes, expected);

        let decoded = decode_record(&bytes).unwrap();
        let keys: Vec<_> = decoded
            .iter()
            .map(|e| match e {
                Effect::Enqueue { scope, outbound_key, .. } => (*scope, *outbound_key),
                _ => panic!("only enqueues were encoded"),
            })
            .collect();
        assert_eq!(keys, vec![(None, Some([7; 16])), (Some(9), Some([0xEE; 16]))]);
    }

    #[test]
    fn a_truncated_outbound_key_is_refused_by_name() {
        let mut out = Vec::new();
        1u32.encode(&mut out);
        out.push(0x0c);
        encode_bytes(b"Charge", &mut out);
        encode_bytes(&[], &mut out);
        Option::<String>::None.encode(&mut out);
        1u64.encode(&mut out);
        out.extend_from_slice(&[1; 15]);
        let err = decode_record(&out).unwrap_err();
        assert_eq!(
            err.to_string(),
            "record decode: an outbound enqueue (0x0c/0x0d) ends before its 16-byte idempotency key"
        );
    }
```

Change `decode_refuses_unknown_tags` to push `14` and expect `"unknown effect tag 14"`, with its doc saying `0x0e` is the next free tag. Adjust the `Effect::Enqueue` destructuring if the decoded effect type differs from what these tests assume. The test asserts the tag bytes and key placement; the destructuring is incidental.

In `job.rs`'s test module:

```rust
    #[test]
    fn an_outbound_job_row_roundtrips_with_and_without_a_scope() {
        for scope in [None, Some(4u64)] {
            let row = JobRow {
                id: Id::from_raw(1),
                name: "Charge".into(),
                payload: vec![9],
                key: None,
                status: JobStatus::Pending,
                attempts: 0,
                due_at: 0,
                last_error: None,
                shape: Some(0xFEED),
                scope,
                outbound_key: Some([3; 16]),
            };
            let mut bytes = Vec::new();
            row.encode_row(&mut bytes);
            let back = JobRow::decode_row(&bytes).unwrap();
            assert_eq!((back.shape, back.scope, back.outbound_key), (Some(0xFEED), scope, Some([3; 16])));
        }
    }

    #[test]
    #[should_panic(expected = "an outbound key is encoded after the shape")]
    fn an_outbound_key_without_a_shape_is_a_contract_panic() {
        let row = JobRow {
            id: Id::from_raw(1),
            name: "Charge".into(),
            payload: vec![],
            key: None,
            status: JobStatus::Pending,
            attempts: 0,
            due_at: 0,
            last_error: None,
            shape: None,
            scope: None,
            outbound_key: Some([3; 16]),
        };
        row.encode_row(&mut Vec::new());
    }
```

- [ ] **Step 2: Write the failing minting tests** (`charpente-core/tests/materialize.rs`)

```rust
/// Counts draws and hands out 0x01, 0x02, … so a test can name the bytes.
struct CountingEntropy(std::cell::Cell<u8>);
// `Entropy: Send`; a Cell is Send, and this test never crosses threads.
impl charpente_core::io::Entropy for CountingEntropy {
    fn fill(&self, buf: &mut [u8]) {
        for b in buf {
            self.0.set(self.0.get() + 1);
            *b = self.0.get();
        }
    }
}

struct Charge;
impl Codec for Charge {
    fn encode(&self, _out: &mut Vec<u8>) {}
    fn decode(_input: &mut &[u8]) -> io::Result<Charge> {
        Ok(Charge)
    }
}
impl Job for Charge {
    const NAME: &'static str = "charge";
    const FINGERPRINT: u64 = charpente_core::db::outbound_fingerprint(0x7E57_000C);
    type Scope = Unscoped;
    const OUTBOUND: bool = true;
}

#[test]
fn a_minting_group_draws_16_bytes_per_outbound_enqueue_and_none_otherwise() {
    let sim = Sim::with_faults(0x601, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    let mint = CountingEntropy(std::cell::Cell::new(0));
    let mut group = db.begin_group_minting(&mint).unwrap();
    group
        .stage(|tx: &mut Tx| {
            tx.enqueue(Ping);
            Ok::<(), io::Error>(())
        })
        .unwrap();
    assert_eq!(mint.0.get(), 0, "a pure enqueue draws nothing (spec §4 invariant 6)");
    group
        .stage(|tx: &mut Tx| {
            tx.enqueue(Charge);
            tx.enqueue(Charge);
            Ok::<(), io::Error>(())
        })
        .unwrap();
    group.commit().map_err(|f| f.error).unwrap();
    assert_eq!(mint.0.get(), 32, "16 bytes per outbound enqueue");

    let drained = db.drain_enqueued();
    assert_eq!(
        drained.iter().map(|e| (e.id.raw(), e.outbound)).collect::<Vec<_>>(),
        vec![(0, false), (1, true), (2, true)]
    );
    let jobs = db.snapshot().unwrap().scan::<JobRow>().unwrap();
    let first: [u8; 16] = std::array::from_fn(|i| i as u8 + 1);
    let second: [u8; 16] = std::array::from_fn(|i| i as u8 + 17);
    assert_eq!(jobs[0].outbound_key, None);
    assert_eq!(jobs[1].outbound_key, Some(first));
    assert_eq!(jobs[2].outbound_key, Some(second));
}

#[test]
#[should_panic(expected = "Db::transact and Db::begin_group cannot mint")]
fn a_non_minting_transaction_refuses_an_outbound_enqueue() {
    let sim = Sim::with_faults(0x602, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    let _ = db.transact(|tx: &mut Tx| {
        tx.enqueue(Charge);
        Ok::<(), io::Error>(())
    });
}

#[test]
fn an_outbound_key_survives_reopen() {
    let sim = Sim::with_faults(0x603, FaultConfig::none());
    let mint = CountingEntropy(std::cell::Cell::new(0x40));
    {
        let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
        let mut group = db.begin_group_minting(&mint).unwrap();
        group
            .stage(|tx: &mut Tx| {
                tx.enqueue(Charge);
                Ok::<(), io::Error>(())
            })
            .unwrap();
        group.commit().map_err(|f| f.error).unwrap();
    }
    sim.restart();
    let db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    let jobs = db.snapshot().unwrap().scan::<JobRow>().unwrap();
    let expected: [u8; 16] = std::array::from_fn(|i| 0x41 + i as u8);
    assert_eq!(jobs[0].outbound_key, Some(expected), "replay is a pure function of the record");
}
```

Change the existing uses in this file: `drained.iter().map(|id| id.raw())` becomes `.map(|e| e.id.raw())`, and `db.drain_enqueued()[0].raw()` becomes `db.drain_enqueued()[0].id.raw()`.

- [ ] **Step 3: Run them to verify they fail**

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
cargo test -p charpente-core --no-fail-fast 2>&1 | tee $SDD/task5-red.log | grep -E "^error|test result"
```

Expected: compile errors naming `outbound`, `okey`, `begin_group_minting`, `outbound_key`, `OUTBOUND`.

- [ ] **Step 4: Implement the core half**

`db.rs`. Hoist the FNV continuation out of `scoped_fingerprint` and add its outbound twin:

```rust
/// FNV-1a continued over `bytes` — the one mixer both fingerprint folds use.
const fn fnv_continue(mut hash: u64, bytes: &[u8]) -> u64 {
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x100000001b3);
        i += 1;
    }
    hash
}

pub const fn scoped_fingerprint(fold: u64, tenant: &str) -> u64 {
    fnv_continue(fnv_continue(fnv_continue(fold, b"#scoped_by\0"), tenant.as_bytes()), b"\0")
}

/// An outbound job's fingerprint (outbound-effects spec R8): its fingerprint
/// as a pure job, continued over `#outbound\0`. Declaring or removing
/// `outbound` is therefore a shape change, refused while Pending jobs of the
/// old shape exist, by the existing refusal and with its existing fix. The
/// host is not folded in: a minted key is host-agnostic. No identifier begins
/// with `#`, so no field list can produce the suffix.
pub const fn outbound_fingerprint(fingerprint: u64) -> u64 {
    fnv_continue(fingerprint, b"#outbound\0")
}
```

Add a test pinning that `scoped_fingerprint` is unchanged by the hoist: compute it for `(0x1234, "Org")` before the edit (run once and paste the value) and assert it after. Add the `Job` item after `RETRY`:

```rust
    /// `#[job(…, outbound = "<host>")]` (outbound-effects spec §5): its
    /// enqueue mints an idempotency key from the staging node's `Entropy` and
    /// records tag `0x0c`/`0x0d`, and dispatch runs `call`/`settle` rather
    /// than `run`. `#[job]` folds it into `FINGERPRINT` (`outbound_fingerprint`).
    const OUTBOUND: bool = false;
```

`Enqueue` gains two fields after `scope`:

```rust
    /// `J::OUTBOUND`, set by `push_enqueue`.
    outbound: bool,
    /// The idempotency key, drawn by `Group::stage` just before the record is
    /// encoded. `Some` exactly when `outbound`; `encode_record` asserts it.
    okey: Option<[u8; 16]>,
```

In `push_enqueue`, add `outbound: J::OUTBOUND, okey: None,` to the pushed `Enqueue`. In `encode_record`'s enqueue loop:

```rust
    for Enqueue { job, payload, key, shape, scope, outbound, okey } in &effects.enqueues {
        assert_eq!(
            okey.is_some(),
            *outbound,
            "record encode: job \"{job}\" reached encode with outbound={outbound} and a key \
             {} — Group::stage mints every outbound enqueue's key before encoding",
            if okey.is_some() { "present" } else { "absent" }
        );
        out.push(match (scope.is_some(), okey.is_some()) {
            (false, false) => 8,
            (true, false) => 10,
            (false, true) => 12,
            (true, true) => 13,
        });
        encode_bytes(job.as_bytes(), &mut out);
        encode_bytes(payload, &mut out);
        key.encode(&mut out);
        shape.encode(&mut out);
        if let Some(tenant) = scope {
            tenant.id.encode(&mut out);
        }
        if let Some(okey) = okey {
            out.extend_from_slice(okey);
        }
    }
```

`Effect::Enqueue` gains `outbound_key: Option<[u8; 16]>`, with the comment "`Some` exactly for `0x0c`/`0x0d`". Every existing decode arm (2, 8, 10) sets `outbound_key: None`. Add:

```rust
            12 => Effect::Enqueue {
                job: String::decode(input)?,
                payload: Vec::<u8>::decode(input)?,
                key: Option::<String>::decode(input)?,
                shape: Some(u64::decode(input)?),
                scope: None,
                outbound_key: Some(decode_outbound_key(input)?),
            },
            13 => Effect::Enqueue {
                job: String::decode(input)?,
                payload: Vec::<u8>::decode(input)?,
                key: Option::<String>::decode(input)?,
                shape: Some(u64::decode(input)?),
                scope: Some(u64::decode(input)?),
                outbound_key: Some(decode_outbound_key(input)?),
            },
```

```rust
fn decode_outbound_key(input: &mut &[u8]) -> io::Result<[u8; 16]> {
    let Some((key, rest)) = input.split_first_chunk::<16>() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "record decode: an outbound enqueue (0x0c/0x0d) ends before its 16-byte idempotency key",
        ));
    };
    *input = rest;
    Ok(*key)
}
```

The struct-literal evaluation order is field order, which is the byte order. That's the same reliance arms 8 and 10 already make.

The apply arm passes `*outbound_key` to `materialize_job` and pushes `Enqueued { id: Id::from_raw(id), outbound: outbound_key.is_some() }`:

```rust
/// One job materialized since the last `Db::drain_enqueued`. `outbound` is
/// read from the record's tag at apply, so the kernel routes the job without
/// a store read (plan amendment A8: a read would draw sim fault rolls and
/// re-roll every universe).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Enqueued {
    pub id: Id<JobRow>,
    pub outbound: bool,
}
```

`TableMachine.enqueued` becomes `Vec<Enqueued>`, and `drain_enqueued` returns `self.engine.machine_mut().enqueued.drain(..).collect()`.

`Group` gains `mint: Option<&'a dyn Entropy>` (`use crate::io::Entropy`). `begin_group` sets it to `None`. Add:

```rust
    /// `begin_group` for the App's staging paths (plan amendment A9): what
    /// they stage can enqueue an outbound job, whose idempotency key is drawn
    /// from `mint` when it is staged — never at apply, which must stay a pure
    /// function of the record (outbound-effects spec §6).
    pub fn begin_group_minting<'g>(&'g mut self, mint: &'g dyn Entropy) -> io::Result<Group<'g>> {
        let mut group = self.begin_group()?;
        group.mint = Some(mint);
        Ok(group)
    }
```

In `Group::stage`, immediately before `let record = encode_record(&effects);`:

```rust
        // Outbound-effects spec §6: minted at staging, and only for an
        // outbound enqueue, so a group that stages none draws nothing (§4
        // invariant 6) and every existing universe keeps its entropy stream.
        for enqueue in effects.enqueues.iter_mut().filter(|e| e.outbound) {
            let mint = self.mint.unwrap_or_else(|| {
                panic!(
                    "contract: job \"{}\" is outbound, so its enqueue needs an idempotency key \
                     drawn from the node's Entropy, and this group has none — Db::transact and \
                     Db::begin_group cannot mint; enqueue it from a route, a job or cron (the \
                     App's groups mint), or in a test through App::transact_for_tests",
                    enqueue.job
                )
            });
            let mut okey = [0u8; 16];
            mint.fill(&mut okey);
            enqueue.okey = Some(okey);
        }
```

Fix `enqueue_keyed`'s doc. It said "consumed by M1's external-IO surface"; it becomes "The app's own identity for the job, carried on its row and never deduped on. Not the outbound idempotency key: that one the platform mints (outbound-effects spec §6)". Add the two tags to the module doc's tag list. Change "`0x0c` is the next free tag" to "`0x0e` is…", and add after `0x0b`:

```text
//! `0x0c [job][payload][key: Option<String>][shape: u64 le][okey: 16 bytes]`
//! outbound enqueue, and `0x0d` the same with `[scope: u64 le]` before `okey`
//! — a job declared `outbound`, its idempotency key minted at staging
//! (outbound-effects spec §6),
```

`job.rs`: add the field after `scope`:

```rust
    /// An outbound job's idempotency key, from the record's `0x0c`/`0x0d` tag
    /// (outbound-effects spec §6). Encoded after `scope`, only when `Some`, so
    /// every row written before it decodes unchanged. Requires a shape: only
    /// `0x0c`/`0x0d` write one, and both carry the shape.
    pub outbound_key: Option<[u8; 16]>,
```

In `encode_row`, after the scope block:

```rust
        if let Some(okey) = self.outbound_key {
            assert!(
                self.shape.is_some(),
                "JobRow {}: an outbound key is encoded after the shape, so an outbound row must \
                 carry one — only tags 0x0c/0x0d write a key, and both carry the shape",
                self.id.raw()
            );
            out.extend_from_slice(&okey);
        }
```

In `decode_row`:

```rust
        let key16 = |input: &mut &[u8]| -> io::Result<[u8; 16]> {
            let (k, rest) = input.split_first_chunk::<16>().expect("length checked by the match");
            *input = rest;
            Ok(*k)
        };
        let (shape, scope, outbound_key) = match input.len() {
            0 => (None, None, None),
            8 => (Some(u64::decode(input)?), None, None),
            16 => (Some(u64::decode(input)?), Some(u64::decode(input)?), None),
            24 => (Some(u64::decode(input)?), None, Some(key16(input)?)),
            32 => (Some(u64::decode(input)?), Some(u64::decode(input)?), Some(key16(input)?)),
            n => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "row decode: {n} bytes after a JobRow's last_error — the shape and the \
                         tenant scope after it are 8 bytes each and the outbound key 16, each \
                         present or absent, so the stored row is corrupt"
                    ),
                ));
            }
        };
```

Two `job.rs` tests pin the old text byte-exact, and take the new text in this step (hard rule 5): `a_torn_job_row_shape_is_a_named_decode_error` (`job.rs:262-266`) and `a_torn_job_row_scope_is_a_named_decode_error` (`:322-326`). Their expected strings become:

```rust
            "row decode: 3 bytes after a JobRow's last_error — the shape and the tenant scope \
             after it are 8 bytes each and the outbound key 16, each present or absent, so the \
             stored row is corrupt"
```

and the same with `11 bytes`. `event.rs`'s similar strings (`:90`, `:180`, `:235`) are `EventRow`'s own and stay.

24 is unambiguous because a scope never comes without a shape and 16 + 8 = 24 would need a key of 8. `table.rs`: `materialize_job` gains a trailing `outbound_key: Option<[u8; 16]>` parameter, carried into the row. Its other callers (`db.rs:2667`, `typed_tables.rs:1070-1077`) pass `None` unless they are the apply arm.

- [ ] **Step 5: Implement the platform half**

`http/app.rs`. `begin_group` mints, replacing its `begin_group()?` line:

```rust
        let group = self
            .db
            .as_mut()
            .expect(DB_TAKEN)
            .begin_group_minting(&*self.entropy)?;
```

Add beside it:

```rust
    /// A minting group for the kernel's job, cron and outbound arms (plan
    /// amendment A9): a job, a cron tick or a `settle` can enqueue an
    /// outbound job, whose key is drawn from this app's entropy. Field
    /// projections, as in `begin_group`, so the borrows split.
    pub(crate) fn begin_job_group(&mut self) -> io::Result<Group<'_>> {
        let entropy = &*self.entropy;
        self.db.as_mut().expect(DB_TAKEN).begin_group_minting(entropy)
    }

    /// Test-only: one minting transaction — `Db::transact` refuses an
    /// outbound enqueue, so a test seeds one through here. Every commit
    /// failure is `Err` (a test has no request to answer).
    #[doc(hidden)]
    pub fn transact_for_tests<T, E>(
        &mut self,
        f: impl FnOnce(&mut Tx) -> Result<T, E>,
    ) -> Result<T, TxError<E>> {
        let mut group = self.begin_job_group().map_err(TxError::Io)?;
        let out = group.stage(f)?;
        group.commit().map_err(|failure| TxError::Io(failure.error))?;
        Ok(out)
    }
```

`kernel/mod.rs`: in `Ready::Job` and `Ready::Cron`, `let mut group = self.app.db_mut().begin_group()?;` becomes `let mut group = self.app.begin_job_group()?;`. Leave `dispatch.rs`'s `run_due` and `cron.rs`'s `tick` one-job wrappers on `Db::begin_group`: they take a `&mut Db`, and a test that enqueues an outbound job through them meets the A9 panic.

`dispatch.rs` adds `Enqueued` to its `use charpente_core::db::{…}` line (`dispatch.rs:9-12`), then:

```rust
    /// Feeds jobs drained from `Db::drain_enqueued` (fresh enqueues are due
    /// immediately: due_at 0).
    pub fn note_enqueued(&mut self, enqueued: Vec<Enqueued>) {
        for e in enqueued {
            self.due.insert((0, e.id.raw()));
        }
    }
```

Task 8 routes `outbound` into its own set. Until then no outbound job can be mounted: there is no macro surface, and `JobDef` has no outbound field.

Fix every site the compiler names. `JobRow` literals take `outbound_key: None`, `Effect::Enqueue` literals take `outbound_key: None`, and a `[0].raw()` becomes `[0].id.raw()`. If a compile-fail fixture's added line shifts its `.stderr`, re-bless it with `TRYBUILD=overwrite cargo test -p charpente-macros --test trybuild`. Then `git diff` the `.stderr` and confirm only line numbers moved.

- [ ] **Step 5a: The shape refusal names `outbound`**

`outbound_fingerprint` makes outbound-ness part of a job's shape (R8). The foreign-shape refusal at `db.rs:3839` tells the operator what to restore, so it must name the new thing that can have changed. Otherwise a deploy that added `outbound = …` is told to look at fields and tenancy. Change the clause

`or restore whichever of {name}'s fields, its \`global\`/\`scoped_by\` declaration or its tenant type's \`NAME\` changed`

to

`or restore whichever of {name}'s fields, its \`global\`/\`scoped_by\` declaration, its \`outbound\` declaration or its tenant type's \`NAME\` changed`

Make the same change at every site that pins the text: `charpente/tests/kernel.rs:742, :1408, :4475, :5509`, `charpente/tests/replication.rs:10641`, and GUIDE.md's pinned block at `:1798`. Find the full set with `grep -rn "tenant type's \`NAME\` changed" crates docs examples`, and expect only `docs/superpowers/` plans to remain. Plans are history, so leave them. `doc_truth` holds the GUIDE block to the code, so run `cargo test -p guide-examples --test doc_truth`. Task 11's `a_job_gaining_outbound_is_refused_while_old_rows_are_pending` asserts the new clause end to end.

- [ ] **Step 6: DESIGN's tag list**

In DESIGN.md's record paragraph listing `0x08`/`0x0a`, add: "`0x0c`/`0x0d` are the outbound enqueues: `0x08`/`0x0a` plus a 16-byte idempotency key the staging node drew from its `Entropy` (outbound-effects spec §6). An older binary refuses them by name as unknown tags, so downgrading after an outbound enqueue is unsupported."

- [ ] **Step 7: Run the tests, then oracle 3**

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
cargo test -p charpente-core --no-fail-fast 2>&1 | tee $SDD/gate-task-5-core.log; grep -c "^test result: ok" $SDD/gate-task-5-core.log
cargo test -p charpente --no-fail-fast 2>&1 | tee $SDD/gate-task-5-charpente.log; grep -c "^test result: ok" $SDD/gate-task-5-charpente.log
cargo test -p charpente-macros --no-fail-fast 2>&1 | tee $SDD/gate-task-5-macros.log; grep -c "^test result: ok" $SDD/gate-task-5-macros.log
perl -e 'alarm 590; exec @ARGV' -- $SDD/oracle3.sh task5   # oracle3 mode foreground; background: Global Constraints
```

Expected: every suite ok, and oracle 3 exit 0. Every existing record is still `0x08`/`0x0a`, and no group draws. A changed universe here means a draw moved, which is a stop-and-report.

- [ ] **Step 8: Prove the mint test can fail**

Comment out the `mint.fill(&mut okey);` line (the key stays all zeros) and run `a_minting_group_draws_16_bytes_per_outbound_enqueue_and_none_otherwise`. Expected: FAIL on the draw count. Restore with `git checkout -p` or by undoing the edit, then `touch crates/charpente-core/src/db.rs`. Record it in progress.md.

- [ ] **Step 9: Full gate and commit**

The three gate commands. The workspace test is partitioned or backgrounded, then counted and compared with the base count.

```bash
git add crates/charpente-core/src/db.rs crates/charpente-core/src/job.rs crates/charpente-core/src/table.rs \
  crates/charpente-core/tests/materialize.rs crates/charpente-core/tests/engine.rs \
  crates/charpente/src/http/app.rs crates/charpente/src/kernel/mod.rs \
  crates/charpente/src/kernel/dispatch.rs crates/charpente/tests/kernel.rs crates/charpente/tests/replication.rs \
  crates/charpente/tests/transactions.rs crates/charpente/tests/typed_tables.rs \
  crates/charpente-macros/tests/compile_fail/job_row_forged_on_bare_tx.rs \
  crates/charpente-macros/tests/compile_fail/job_row_forged_on_bare_tx.stderr \
  crates/charpente-macros/tests/compile_fail/job_row_table_name_borrowed.rs \
  crates/charpente-macros/tests/compile_fail/job_row_table_name_borrowed.stderr \
  DESIGN.md docs/GUIDE.md <each other file the compiler named>
git commit -m "core: minted idempotency keys — tags 0x0c/0x0d, JobRow.outbound_key, App groups mint at staging (outbound-effects spec §6); oracle 3 unchanged"
```

---

### Task 6: Pure refactor — the failure record becomes one function

The outbound arms (Task 11) stage the same failure record for six different causes. They call this function rather than copying the 40 lines a seventh time. Behaviour is unchanged, and oracle 3 plus the existing suites are the proof.

**Files:**
- Modify: `crates/charpente/src/kernel/dispatch.rs` (the `Err(TxError::App(JobError::App(err)))` arm of `run_due_into`)

**Interfaces:**
- Produces: `fn stage_failure_record(defs: &BTreeMap<&'static str, JobDef>, group: &mut Group, admission: &TenantAdmission<'_>, id: u64, now: u64, err: String) -> io::Result<JobOutcome>`. It's a free function, private to the module. It stages the attempt (`attempts += 1`, `last_error`, backoff or Dead) and touches no due-set. The caller does the due-set bookkeeping, because an outbound job returns to a different set.
- Produces: `fn assert_row_matches_def(id: u64, row: &JobRow, def: &JobDef)`. These are `run_due_into`'s two dispatcher-invariant asserts (shape equals `def.fingerprint`; a tenant exactly when `def.scoped_by`), moved with their texts and comments byte-for-byte. Task 10's call path and Task 11's settle path assert the same two things through it.
- Produces: `fn not_primary_bug() -> io::Error` and `fn verifying_bug() -> io::Error`. The two "BUG: …" texts are each written once, byte-identical to today's. `run_due_into`'s two gate arms, the failure record's `map_err` and Task 10's call path all use them.

- [ ] **Step 1: Move the code**

Move the two `assert_eq!`s on `row.shape` and `row.scope` (with the comment blocks above each) out of `run_due_into`'s closure into `assert_row_matches_def`, and call it at their old place. Cut the arm's `group.stage(|tx| { … })` and its `.map_err(…)` into `stage_failure_record`, unchanged except for parameter names. Move the BUG texts into `not_primary_bug` and `verifying_bug`, text unchanged. Both copies exist today, in the `NotPrimary`/`Verifying` arms and in the `map_err`; diff them first and stop if they differ. Every site then calls the function. The arm in `run_due_into` becomes:

```rust
            Err(TxError::App(JobError::App(err))) => {
                match stage_failure_record(&self.defs, group, &admission, id, now, err) {
                    Ok(outcome) => {
                        if let JobOutcome::Retried { due_at, .. } = outcome {
                            self.due.insert((due_at, id));
                        }
                        Ok(outcome)
                    }
                    Err(e) => {
                        self.due.insert((due_at, id));
                        Err(e)
                    }
                }
            }
```

Keep the arm's long comment beside the new function, with "this arm" reworded to "a failed attempt".

- [ ] **Step 2: Prove nothing moved**

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
cargo test -p charpente --no-fail-fast 2>&1 | tee $SDD/gate-task-6.log; grep -c "^test result: ok" $SDD/gate-task-6.log
perl -e 'alarm 590; exec @ARGV' -- $SDD/oracle3.sh task6   # oracle3 mode foreground; background: Global Constraints
```

Expected: the same ok count as Task 5's `charpente` log, and oracle 3 exit 0. `git diff --stat` touches only `dispatch.rs`.

- [ ] **Step 3: Gate and commit**

`cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

```bash
git add crates/charpente/src/kernel/dispatch.rs
git commit -m "kernel: the failure record is one function, ahead of the outbound arms (pure refactor; oracle 3 unchanged)"
```

---

### Task 7: `charpente::outbound` — `Call`, `Reply`, `Host`, the call rules, and `JobDef.outbound`

**Files:**
- Create: `crates/charpente/src/outbound.rs`
- Modify: `crates/charpente/src/lib.rs` (`pub mod outbound;`)
- Modify: `crates/charpente/src/kernel/dispatch.rs` (`OutboundDef`, `CallGlue`, `SettleGlue`, `JobDef.outbound`), `crates/charpente/src/kernel/mod.rs` (re-export them)
- Modify: every `JobDef { … }` literal. Known sites: `launch.rs:477`, `serve.rs:757`, `serve.rs:860`, `charpente-macros/src/job.rs:447`. Each gains `outbound: None`. `tests/kernel.rs`'s `JobDef` sites need nothing: `:789` is a fn signature, and `:1314`, `:1656`, `:1703`, `:1771` are struct-update literals (`..<File as JobEntry>::def()`).

**Interfaces:**
- Consumes: Task 2's `HttpMethod`, `IdempotencyKey`, `OutboundCall`, `OutboundReply`.
- Produces (`charpente::outbound`):
  - `pub use charpente_core::io::{HttpMethod, IdempotencyKey};`
  - `pub struct Call`: `get`/`post`/`put`/`patch`/`delete(path: impl Into<String>) -> Call`, plus `header(self, name: impl Into<String>, value: impl Into<String>) -> Call`, `json<T: Serialize>(self, &T) -> Call`, `form<V: AsRef<str>>(self, &[(&str, V)]) -> Call`, `body(self, Vec<u8>) -> Call`. Crate-internal: `pub(crate) fn into_wire(self, host: &'static str, key: IdempotencyKey) -> OutboundCall`.
  - `pub struct Reply`: `status(&self) -> u16`, `header(&self, &str) -> Option<&str>`, `body(&self) -> &[u8]`, `json<T: DeserializeOwned>(&self) -> Result<T, JobError>`. Crate-internal: `pub(crate) fn from_wire(OutboundReply) -> Reply`.
  - `pub struct Host`: `new(&'static str)`, `bearer_env(&'static str)`, `header_env(&str, &'static str)`, `basic_env(&'static str, &'static str)`, `max_request_bytes(usize)`, `max_reply_bytes(usize)`. Crate-internal: `pub(crate) fn name(&self) -> &'static str`, `max_request(&self) -> usize`, `max_reply(&self) -> usize`, `env_vars(&self) -> Vec<&'static str>`, `injected_headers(&self) -> Vec<String>`, `credentials(&self) -> &[Credential]`.
  - Each crate-internal item above lands before its first non-test caller, so it carries the dead-code attribute (Global Constraints): `into_wire` and `check` until Task 10, `from_wire` until Task 11, `name`/`max_request`/`max_reply`/`env_vars`/`injected_headers` until Task 9, and `credentials` until Task 14.
  - `pub(crate) enum Credential { Bearer(&'static str), Header(String, &'static str), Basic(&'static str, &'static str) }`
  - `pub const DEFAULT_MAX_REQUEST_BYTES: usize = 1 << 20;`, `DEFAULT_MAX_REPLY_BYTES: usize = 4 << 20;`, `MAX_CAP_BYTES: usize = 8 << 20;`
  - `pub(crate) fn check(call: &Call, host: &Host) -> Result<(), String>`
  - `pub(crate) fn host_name_problem(name: &str) -> Option<&'static str>`
- Produces (`charpente::kernel`):
  - `pub struct OutboundDef { pub host: &'static str, pub timeout: Duration, pub call: CallGlue, pub settle: SettleGlue }`
  - `pub type CallGlue = fn(&mut Tx, &[u8], &IdempotencyKey, Option<JobScope<'_>>) -> Result<Option<Call>, JobError>;`
  - `pub type SettleGlue = fn(&mut Tx, &[u8], Reply, Option<JobScope<'_>>) -> Result<(), JobError>;`
  - `JobDef.outbound: Option<OutboundDef>`

- [ ] **Step 1: Write the failing tests** (`outbound.rs`'s `#[cfg(test)] mod tests`)

Create `crates/charpente/src/outbound.rs` holding only this test module, and add `pub mod outbound;` to `lib.rs` now, so the file is compiled.

```rust
use super::*;

fn host() -> Host {
    Host::new("pay.test").bearer_env("PAY_KEY").header_env("X-Account", "PAY_ACCOUNT")
}

#[test]
fn a_call_becomes_a_wire_call_with_lowercased_header_names() {
    let key = IdempotencyKey::from_bytes([1; 16]);
    let wire = Call::post("/v1/charges")
        .header("Idempotency-Key", key.as_str())
        .body(b"x".to_vec())
        .into_wire("pay.test", key);
    assert_eq!(wire.host, "pay.test");
    assert_eq!(wire.method, HttpMethod::Post);
    assert_eq!(wire.path, "/v1/charges");
    assert_eq!(wire.headers, vec![("idempotency-key".to_string(), key.as_str().to_string())]);
    assert_eq!(wire.body, b"x");
    assert_eq!(wire.key, key);
}

#[test]
fn form_is_application_x_www_form_urlencoded() {
    let call = Call::post("/f").form(&[("amount", "10 00"), ("note", "a&b=c/é")]);
    assert_eq!(call.body, b"amount=10+00&note=a%26b%3Dc%2F%C3%A9");
    assert_eq!(
        call.headers,
        vec![("content-type".to_string(), "application/x-www-form-urlencoded".to_string())]
    );
}

#[test]
fn json_sets_the_body_and_its_content_type() {
    let call = Call::put("/j").json(&serde_json::json!({ "a": 1 }));
    assert_eq!(call.body, br#"{"a":1}"#);
    assert_eq!(call.headers, vec![("content-type".to_string(), "application/json".to_string())]);
}

#[test]
fn the_call_rules_name_each_violation() {
    let h = host();
    let cases = [
        (Call::get("v1"), "outbound pay.test: the call's path \"v1\" must begin with '/' and name no scheme or authority — the host is the job's declaration, outbound = \"pay.test\""),
        (Call::get("//evil.test/x"), "outbound pay.test: the call's path \"//evil.test/x\" must begin with '/' and name no scheme or authority — the host is the job's declaration, outbound = \"pay.test\""),
        (Call::get("/a b"), "outbound pay.test: the call's path \"/a b\" holds a byte outside visible ASCII — percent-encode it"),
        (Call::get("/x").header("x-a", "1\r\nhost: evil.test"), "outbound pay.test: the call's header \"x-a\" has a control character in its value — refused before sending, since a CR or LF there would forge a header"),
        (Call::get("/x").header("bad name", "1"), "outbound pay.test: the call's header name \"bad name\" is not an HTTP token — a name is letters, digits and !#$%&'*+-.^_`|~"),
        (Call::get("/x").header("Content-Length", "1"), "outbound pay.test: the call sets \"content-length\", which the edge sets — remove it from call"),
        (Call::get("/x").header("authorization", "Bearer x"), "outbound pay.test: the call sets \"authorization\", which this host's Host entry injects from PAY_KEY — remove it from call; a credential never enters app code"),
        (Call::get("/x").header("x-account", "1"), "outbound pay.test: the call sets \"x-account\", which this host's Host entry injects from PAY_ACCOUNT — remove it from call; a credential never enters app code"),
        (Call::post("/x").body(vec![0; (1 << 20) + 1]), "outbound pay.test: the request body is 1048577 bytes, over the 1048576-byte request cap — raise it with Host::max_request_bytes"),
    ];
    for (call, expected) in cases {
        assert_eq!(check(&call, &h).unwrap_err(), expected);
    }
    assert_eq!(check(&Call::get("/v1/x?y=1%20").header("idempotency-key", "k"), &h), Ok(()));
}

#[test]
fn a_host_name_is_a_bare_dns_name() {
    assert_eq!(host_name_problem("api.stripe.com"), None);
    assert_eq!(host_name_problem(""), Some("is empty"));
    assert_eq!(host_name_problem("https://api.stripe.com"), Some("carries a scheme"));
    assert_eq!(host_name_problem("user@api.stripe.com"), Some("carries userinfo"));
    assert_eq!(host_name_problem("api.stripe.com/v1"), Some("carries a path"));
    assert_eq!(host_name_problem("api.stripe.com:443"), Some("carries a port"));
    assert_eq!(host_name_problem("api_stripe.com"), Some("holds a character a DNS name cannot"));
}

#[test]
#[should_panic(expected = "contract: Host::max_reply_bytes(9437184) exceeds the 8388608-byte bound")]
fn a_cap_past_8_mib_is_a_contract_panic() {
    let _ = Host::new("pay.test").max_reply_bytes(9 << 20);
}

#[test]
#[should_panic(expected = "contract: Host::new(\"https://pay.test\") — the host carries a scheme")]
fn a_host_with_a_scheme_is_a_contract_panic() {
    let _ = Host::new("https://pay.test");
}

#[test]
fn a_reply_reads_headers_case_insensitively_and_bad_json_is_an_app_error() {
    let reply = Reply::from_wire(OutboundReply {
        status: 200,
        headers: vec![("Content-Type".into(), "application/json".into())],
        body: b"{\"id\":\"ch_1\"}".to_vec(),
    });
    assert_eq!(reply.status(), 200);
    assert_eq!(reply.header("content-type"), Some("application/json"));
    #[derive(serde::Deserialize)]
    struct Charge {
        id: String,
    }
    assert_eq!(reply.json::<Charge>().unwrap().id, "ch_1");
    match reply.json::<u64>() {
        Err(JobError::App(m)) => assert!(m.starts_with("reply body is not the expected JSON: "), "{m}"),
        other => panic!("{other:?}"),
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p charpente --lib outbound`
Expected: FAIL to compile, "cannot find type `Host` in this scope" (and "use of undeclared type `Call`").

- [ ] **Step 3: Write `outbound.rs`**

```rust
//! The outbound job surface (outbound-effects spec §5, §9): what `call` builds
//! (`Call`), what `settle` reads (`Reply`), and the per-host configuration an
//! `App` declares (`Host`). The kernel checks every built `Call` against its
//! host's `Host` entry before a start (`check`); each refusal is an attempt
//! whose `last_error` names the rule.

use charpente_core::io::{OutboundCall, OutboundReply};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::kernel::JobError;

pub use charpente_core::io::{HttpMethod, IdempotencyKey};

/// The request cap's default: the inbound body cap.
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 1 << 20;
pub const DEFAULT_MAX_REPLY_BYTES: usize = 4 << 20;
/// Half of `MAX_RECORD_BYTES` (16 MiB), so a reply `settle` stores whole
/// leaves room for the rest of its record (spec §9).
pub const MAX_CAP_BYTES: usize = 8 << 20;

/// The headers the edge sets itself (spec §5).
const EDGE_HEADERS: [&str; 4] = ["host", "content-length", "transfer-encoding", "connection"];

/// One outbound request, as `call` builds it. The host is not here: it is the
/// job's declaration, never runtime data.
pub struct Call {
    method: HttpMethod,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Call {
    fn new(method: HttpMethod, path: impl Into<String>) -> Call {
        Call { method, path: path.into(), headers: Vec::new(), body: Vec::new() }
    }
    pub fn get(path: impl Into<String>) -> Call {
        Call::new(HttpMethod::Get, path)
    }
    pub fn post(path: impl Into<String>) -> Call {
        Call::new(HttpMethod::Post, path)
    }
    pub fn put(path: impl Into<String>) -> Call {
        Call::new(HttpMethod::Put, path)
    }
    pub fn patch(path: impl Into<String>) -> Call {
        Call::new(HttpMethod::Patch, path)
    }
    pub fn delete(path: impl Into<String>) -> Call {
        Call::new(HttpMethod::Delete, path)
    }

    /// Names are matched case-insensitively and sent lowercase.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Call {
        self.headers.push((name.into().to_ascii_lowercase(), value.into()));
        self
    }

    /// Serializes `value` as the body and sets `content-type: application/json`.
    /// A value serde cannot serialize (a map with non-string keys) panics,
    /// and inside `call` a panic is an attempt (plan amendment A16).
    pub fn json<T: Serialize>(mut self, value: &T) -> Call {
        self.body = serde_json::to_vec(value)
            .unwrap_or_else(|e| panic!("Call::json: the value does not serialize as JSON: {e}"));
        self.header("content-type", "application/json")
    }

    /// An `application/x-www-form-urlencoded` body.
    pub fn form<V: AsRef<str>>(mut self, pairs: &[(&str, V)]) -> Call {
        let mut out = String::new();
        for (i, (k, v)) in pairs.iter().enumerate() {
            if i > 0 {
                out.push('&');
            }
            form_encode(k, &mut out);
            out.push('=');
            form_encode(v.as_ref(), &mut out);
        }
        self.body = out.into_bytes();
        self.header("content-type", "application/x-www-form-urlencoded")
    }

    pub fn body(mut self, bytes: Vec<u8>) -> Call {
        self.body = bytes;
        self
    }

    #[cfg_attr(not(test), expect(dead_code, reason = "first caller: Task 10"))]
    pub(crate) fn into_wire(self, host: &'static str, key: IdempotencyKey) -> OutboundCall {
        OutboundCall { host, method: self.method, path: self.path, headers: self.headers, body: self.body, key }
    }
}

fn form_encode(s: &str, out: &mut String) {
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'*' => out.push(b as char),
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
}

/// A reply of any status, as `settle` receives it. A 3xx is a reply too: the
/// edge never follows a redirect (spec §9).
pub struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Reply {
    #[cfg_attr(not(test), expect(dead_code, reason = "first caller: Task 11"))]
    pub(crate) fn from_wire(reply: OutboundReply) -> Reply {
        Reply { status: reply.status, headers: reply.headers, body: reply.body }
    }
    pub fn status(&self) -> u16 {
        self.status
    }
    /// The first header named `name`, case-insensitively.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(n, _)| n.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }
    pub fn body(&self) -> &[u8] {
        &self.body
    }
    /// `JobError::App` when the body is not `T`'s JSON — an attempt, as a
    /// `settle` that cannot read its reply should be.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, JobError> {
        serde_json::from_slice(&self.body)
            .map_err(|e| JobError::app(format!("reply body is not the expected JSON: {e}")))
    }
}

/// A credential the edge attaches per request, by environment-variable NAME
/// (spec §9): values never enter the log, the archive, `call`, the sim or
/// `describe`.
pub(crate) enum Credential {
    Bearer(&'static str),
    Header(String, &'static str),
    Basic(&'static str, &'static str),
}

/// One declared outside service (spec §9, R9): an `App` carries exactly one
/// per host its mounted jobs call.
pub struct Host {
    name: &'static str,
    credentials: Vec<Credential>,
    max_request: usize,
    max_reply: usize,
}

impl Host {
    pub fn new(name: &'static str) -> Host {
        if let Some(problem) = host_name_problem(name) {
            panic!(
                "contract: Host::new(\"{name}\") — the host {problem}; a host is a bare DNS name \
                 like api.stripe.com (an operator points it elsewhere with --outbound-base, GUIDE §5)"
            );
        }
        Host { name, credentials: Vec::new(), max_request: DEFAULT_MAX_REQUEST_BYTES, max_reply: DEFAULT_MAX_REPLY_BYTES }
    }
    /// `authorization: Bearer $VAR`.
    pub fn bearer_env(mut self, var: &'static str) -> Host {
        self.credentials.push(Credential::Bearer(var));
        self
    }
    /// `<name>: $VAR`.
    pub fn header_env(mut self, name: &str, var: &'static str) -> Host {
        self.credentials.push(Credential::Header(name.to_ascii_lowercase(), var));
        self
    }
    /// `authorization: Basic base64($USER_VAR:$PASS_VAR)`.
    pub fn basic_env(mut self, user_var: &'static str, pass_var: &'static str) -> Host {
        self.credentials.push(Credential::Basic(user_var, pass_var));
        self
    }
    pub fn max_request_bytes(mut self, n: usize) -> Host {
        assert!(n <= MAX_CAP_BYTES, "contract: Host::max_request_bytes({n}) exceeds the {MAX_CAP_BYTES}-byte bound — half of a log record's cap, so a call and its record fit (outbound-effects spec §9)");
        self.max_request = n;
        self
    }
    pub fn max_reply_bytes(mut self, n: usize) -> Host {
        assert!(n <= MAX_CAP_BYTES, "contract: Host::max_reply_bytes({n}) exceeds the {MAX_CAP_BYTES}-byte bound — half of a log record's cap, so a reply settle stores whole fits its record (outbound-effects spec §9)");
        self.max_reply = n;
        self
    }
    #[expect(dead_code, reason = "first caller: Task 9")]
    pub(crate) fn name(&self) -> &'static str {
        self.name
    }
    #[expect(dead_code, reason = "first caller: Task 9")]
    pub(crate) fn max_request(&self) -> usize {
        self.max_request
    }
    #[expect(dead_code, reason = "first caller: Task 9")]
    pub(crate) fn max_reply(&self) -> usize {
        self.max_reply
    }
    #[expect(dead_code, reason = "first caller: Task 14")]
    pub(crate) fn credentials(&self) -> &[Credential] {
        &self.credentials
    }
    /// Every variable this host reads, in declaration order.
    #[expect(dead_code, reason = "first caller: Task 9")]
    pub(crate) fn env_vars(&self) -> Vec<&'static str> {
        self.credentials
            .iter()
            .flat_map(|c| match c {
                Credential::Bearer(v) | Credential::Header(_, v) => vec![*v],
                Credential::Basic(u, p) => vec![*u, *p],
            })
            .collect()
    }
    /// The header each credential injects, with the variable that fills it.
    fn injects(&self) -> Vec<(String, &'static str)> {
        self.credentials
            .iter()
            .map(|c| match c {
                Credential::Bearer(v) => ("authorization".to_string(), *v),
                Credential::Header(n, v) => (n.clone(), *v),
                Credential::Basic(u, _) => ("authorization".to_string(), *u),
            })
            .collect()
    }
    #[expect(dead_code, reason = "first caller: Task 9")]
    pub(crate) fn injected_headers(&self) -> Vec<String> {
        self.injects().into_iter().map(|(h, _)| h).collect()
    }
}

/// Why `name` is not a bare host, or `None` (spec §5). `#[job]` checks the
/// same rule at compile time; charpente-macros cannot link this crate, so the
/// two are pinned to agree by the compile-fail fixtures and this module's tests.
pub(crate) fn host_name_problem(name: &str) -> Option<&'static str> {
    if name.is_empty() {
        return Some("is empty");
    }
    if name.contains("://") {
        return Some("carries a scheme");
    }
    if name.contains('@') {
        return Some("carries userinfo");
    }
    if name.contains('/') {
        return Some("carries a path");
    }
    if name.contains(':') {
        return Some("carries a port");
    }
    if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.') {
        return Some("holds a character a DNS name cannot");
    }
    None
}

fn is_token(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

/// The rules a built `Call` must satisfy (spec §5, plan amendment A12),
/// checked before a start. The edge would refuse most of these too, but only
/// the kernel's refusal is the same in production and the sim.
#[cfg_attr(not(test), expect(dead_code, reason = "first caller: Task 10"))]
pub(crate) fn check(call: &Call, host: &Host) -> Result<(), String> {
    let h = host.name;
    if !call.path.starts_with('/') || call.path.starts_with("//") || call.path.contains("://") {
        return Err(format!(
            "outbound {h}: the call's path \"{}\" must begin with '/' and name no scheme or \
             authority — the host is the job's declaration, outbound = \"{h}\"",
            call.path
        ));
    }
    if !call.path.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        return Err(format!(
            "outbound {h}: the call's path \"{}\" holds a byte outside visible ASCII — \
             percent-encode it",
            call.path
        ));
    }
    let injects = host.injects();
    for (name, value) in &call.headers {
        if !is_token(name) {
            return Err(format!(
                "outbound {h}: the call's header name \"{name}\" is not an HTTP token — a name is \
                 letters, digits and !#$%&'*+-.^_`|~"
            ));
        }
        if value.bytes().any(|b| (b < 0x20 && b != b'\t') || b == 0x7f) {
            return Err(format!(
                "outbound {h}: the call's header \"{name}\" has a control character in its value \
                 — refused before sending, since a CR or LF there would forge a header"
            ));
        }
        if EDGE_HEADERS.contains(&name.as_str()) {
            return Err(format!("outbound {h}: the call sets \"{name}\", which the edge sets — remove it from call"));
        }
        if let Some((_, var)) = injects.iter().find(|(n, _)| n == name) {
            return Err(format!(
                "outbound {h}: the call sets \"{name}\", which this host's Host entry injects from \
                 {var} — remove it from call; a credential never enters app code"
            ));
        }
    }
    if call.body.len() > host.max_request {
        return Err(format!(
            "outbound {h}: the request body is {} bytes, over the {}-byte request cap — raise it \
             with Host::max_request_bytes",
            call.body.len(),
            host.max_request
        ));
    }
    Ok(())
}
```

Header names reach `check` lowercased already, because `Call::header` lowercases. The token check runs on that form, which is fine: lowercasing never makes a non-token into a token.

The dead-code attributes (Global Constraints) are what keep `clippy -D warnings` green until each item's first caller lands. `into_wire`, `from_wire` and `check` take the `cfg_attr(not(test), …)` form because this module's own tests call them. `injects`, `is_token`, `EDGE_HEADERS`, `Credential`'s fields and `Host`'s fields carry nothing: they are reached through the expected items. There is no `Call::body_len`: nothing would ever call it.

`kernel/dispatch.rs`, beside `JobGlue`:

```rust
/// An outbound job's declaration and glue (outbound-effects spec §5), `Some`
/// on its `JobDef` exactly when its `#[job]` says `outbound = "<host>"`.
/// Dispatch runs `call`, the kernel makes the call outside any transaction,
/// and `settle` commits the reply's consequences with the Done mark.
pub struct OutboundDef {
    pub host: &'static str,
    pub timeout: std::time::Duration,
    pub call: CallGlue,
    pub settle: SettleGlue,
}

/// Decode the payload and call the job's `call`, which receives `&Tx` (or
/// `&ScopedTx`), so it cannot write (spec R3). Whatever a hand-written glue
/// writes anyway is discarded: the dispatcher leaves `call`'s stage through
/// its `Err` path whenever a call is to be made.
pub type CallGlue = fn(
    &mut Tx,
    &[u8],
    &crate::outbound::IdempotencyKey,
    Option<JobScope<'_>>,
) -> Result<Option<crate::outbound::Call>, JobError>;

pub type SettleGlue =
    fn(&mut Tx, &[u8], crate::outbound::Reply, Option<JobScope<'_>>) -> Result<(), JobError>;
```

Add `pub outbound: Option<OutboundDef>` to `JobDef`, last. Its doc: "`Some` for an outbound job (spec §5); `glue` is then never called, since dispatch routes by the row's minted key." Re-export `OutboundDef`, `CallGlue`, `SettleGlue` from `kernel/mod.rs`. Add `outbound: None` at every `JobDef` literal; the macro gets `outbound: None` for now and Task 8 fills it.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p charpente --lib outbound`
Expected: PASS, all eight.

- [ ] **Step 5: Gate and commit**

Run the three gate commands. Oracle 3 is not owed: nothing on any dispatch path changed, and the `JobDef` field is `None` everywhere.

```bash
git add crates/charpente/src/outbound.rs crates/charpente/src/lib.rs crates/charpente/src/kernel/dispatch.rs \
  crates/charpente/src/kernel/mod.rs crates/charpente/src/launch.rs crates/charpente/src/serve.rs \
  crates/charpente-macros/src/job.rs
git commit -m "charpente: Call, Reply, Host and the call rules; JobDef.outbound (outbound-effects spec §5, §9)"
```

---

### Task 8: `#[job(outbound = "host", timeout = "30s")]`

**Files:**
- Modify: `crates/charpente-macros/src/job.rs`
- Create: `crates/charpente-macros/tests/compile_fail/` seven fixtures with their `.stderr`: `outbound_host_has_scheme`, `outbound_host_has_port`, `outbound_host_empty`, `outbound_timeout_without_outbound`, `outbound_timeout_zero`, `outbound_with_cross_tenant`, `outbound_run_instead_of_call` (E0599, amendment A6), plus an eighth, `outbound_call_cannot_write` (E0596, R3).
- Create: `crates/charpente/tests/outbound.rs` (the suite's first tests)
- Modify: `docs/GUIDE.md` §6 (a new "Outbound jobs" subsection: the surface only; Task 11 adds the failure semantics) and §9 (the eight refusals), plus every quote of the old `ACCEPTED_FORMS` sentence (`:1969-1972`, `:1977-1981`, `:3578-3584`)
- Modify: `CLAUDE.md` (charpente's test list gains `outbound`: `doc_inventory` is red from the moment `tests/outbound.rs` exists)

**Interfaces:**
- Consumes: Task 5's `Job::OUTBOUND` and `outbound_fingerprint`; Task 7's `OutboundDef`, `CallGlue`, `SettleGlue`, `Call`, `Reply`, `IdempotencyKey`.
- Produces: an outbound `#[job]` emits `const OUTBOUND: bool = true;`, with `FINGERPRINT` folded through `outbound_fingerprint` and `JobDef.outbound = Some(OutboundDef { host, timeout, call, settle })`. `timeout` defaults to 30 s.
- The app writes, for a `global` job: `fn call(&self, tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError>` and `fn settle(self, tx: &mut Tx, reply: Reply) -> Result<(), JobError>`. A `scoped_by = Org` job takes `&ScopedTx<'_, '_, Org>` and `&mut ScopedTx<'_, '_, Org>` instead.

- [ ] **Step 1: Write the failing behaviour test** (create `crates/charpente/tests/outbound.rs`)

```rust
//! Outbound effects (outbound-effects spec): the `#[job(outbound)]` surface,
//! the dispatcher's call path, the kernel's outbound arm, §7's failure table,
//! §4's invariants, and the swarm property. Every sim failure prints its
//! repro line (hard rule 5).

use std::time::Duration;

use charpente::core::db::{Job, outbound_fingerprint};
use charpente::kernel::{JobEntry, JobError};
use charpente::outbound::{Call, IdempotencyKey, Reply};
use charpente::{Tx, job};

#[job(global, outbound = "pay.test", retry(max = 3, backoff = "1s"), timeout = "2s")]
struct Charge {
    cents: u64,
}

impl Charge {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(
            Call::post("/charges")
                .header("idempotency-key", key.as_str())
                .form(&[("amount", self.cents.to_string())]),
        ))
    }

    fn settle(self, _tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        match reply.status() {
            200 => Ok(()),
            s => Err(JobError::app(format!("pay.test answered {s}"))),
        }
    }
}

/// The same fields as a pure job: its fingerprint is `Charge`'s before the
/// outbound fold (`fingerprint::compute` hashes fields, not the struct name).
#[job(global)]
struct ChargeAsPure {
    cents: u64,
}

impl ChargeAsPure {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

#[job(global, outbound = "mail.test")]
struct Mail {}

impl Mail {
    fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(None)
    }

    fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
        Ok(())
    }
}

#[test]
fn an_outbound_job_declares_its_host_timeout_and_shape() {
    let def = <Charge as JobEntry>::def();
    let out = def.outbound.as_ref().expect("declared outbound");
    assert_eq!((out.host, out.timeout), ("pay.test", Duration::from_secs(2)));
    assert!(<Charge as Job>::OUTBOUND);
    assert!(!<ChargeAsPure as Job>::OUTBOUND);
    assert_eq!(
        <Charge as Job>::FINGERPRINT,
        outbound_fingerprint(<ChargeAsPure as Job>::FINGERPRINT),
        "R8: outbound-ness is part of the shape, the host is not"
    );
    assert_eq!(
        <Mail as JobEntry>::def().outbound.unwrap().timeout,
        Duration::from_secs(30),
        "spec §5: timeout defaults to 30s"
    );
    assert!(<ChargeAsPure as JobEntry>::def().outbound.is_none());
}
```

Add `charpente-sim` uses to this file when Task 10 needs them, not now.

- [ ] **Step 2: Write the compile-fail fixtures**

Each file is a complete crate root, the way the existing `job_*.rs` fixtures are, with exactly the `use` lines shown. The two that name platform types need theirs in full: without them name resolution fails first (E0412/E0433), `call` never reaches borrowck, and an overwrite would bless the wrong error. The bodies:

```rust
// outbound_host_has_scheme.rs
use charpente::job;
#[job(global, outbound = "https://api.stripe.com")]
struct Charge {}

// outbound_host_has_port.rs
use charpente::job;
#[job(global, outbound = "api.stripe.com:443")]
struct Charge {}

// outbound_host_empty.rs
use charpente::job;
#[job(global, outbound = "")]
struct Charge {}

// outbound_timeout_without_outbound.rs
use charpente::job;
#[job(global, timeout = "5s")]
struct Charge {}

// outbound_timeout_zero.rs
use charpente::job;
#[job(global, outbound = "api.stripe.com", timeout = "0s")]
struct Charge {}

// outbound_with_cross_tenant.rs
use charpente::job;
#[job(global, cross_tenant, outbound = "api.stripe.com")]
struct Charge {}

// outbound_run_instead_of_call.rs — plan amendment A6: the attribute cannot
// see the impl, so rustc's E0599 for the missing `call` is the refusal.
use charpente::kernel::JobError;
use charpente::{Tx, job};
#[job(global, outbound = "api.stripe.com")]
struct Charge {}
impl Charge {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> { Ok(()) }
}

// outbound_call_cannot_write.rs — spec R3: `call` takes `&Tx`, and a write
// needs `&mut`.
use charpente::kernel::JobError;
use charpente::outbound::{Call, IdempotencyKey, Reply};
use charpente::{Id, Tx, job, table};
#[job(global, outbound = "api.stripe.com")]
struct Charge {}
#[table(global)]
struct Note { id: Id<Note>, text: String }
impl Charge {
    fn call(&self, tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        tx.put(&Note { id: Id::from_raw(1), text: String::new() });
        Ok(None)
    }
    fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> { Ok(()) }
}
```

Each `fn main() {}` as the existing fixtures have it.

- [ ] **Step 3: Run them to verify they fail**

Run: `cargo test -p charpente --test outbound` and `cargo test -p charpente-macros --test trybuild`
Expected: `outbound.rs` fails to compile with "unknown word"/`ACCEPTED_FORMS` at `outbound`. Trybuild reports the eight new fixtures as missing `.stderr`.

- [ ] **Step 4: Implement the parser**

In `job.rs`:
- `ACCEPTED_FORMS` gains `, \`outbound = "<host>"\` with an optional \`timeout = "<n><unit>"\``. The full new text, used verbatim:

```rust
const ACCEPTED_FORMS: &str = "a job declares `global` or `scoped_by = <Type>`, and may add \
     `cross_tenant`, `retry(max = <int>, backoff = \"<n><unit>\")` (unit is one of ms, s, m, h), \
     and `outbound = \"<host>\"` with an optional `timeout = \"<n><unit>\"`, in any order — e.g. \
     `#[job(scoped_by = Org, retry(max = 3, backoff = \"10s\"))]`, `#[job(global, cross_tenant)]` \
     or `#[job(scoped_by = Org, outbound = \"api.stripe.com\", timeout = \"30s\")]`";
```

  Every existing `.stderr` that quotes `ACCEPTED_FORMS` changes with it. Re-bless with `TRYBUILD=overwrite`, then read the diff: only that sentence may move.
- `JobArg` gains `Outbound(Span, LitStr)` and `Timeout(Span, LitStr)`, parsed as `ident = "lit"` with the house errors.
- `JobArgs` gains `pub outbound: Option<String>` and `pub timeout_millis: Option<u64>`.
- Each is refused twice like the others ("`outbound` given twice — write it once").
- `parse_backoff` becomes `parse_duration(lit: &LitStr, what: &str)`, with `what` replacing the literal word `backoff` in its three messages. `parse_duration(&lit, "backoff")` reproduces today's text byte-for-byte, so no existing `.stderr` changes for it.
- After the loop, in this order:

```rust
        if let Some(lit) = &outbound_lit {
            if let Some(problem) = host_name_problem(&lit.value()) {
                return Err(Error::new(
                    lit.span(),
                    format!(
                        "outbound = \"{}\": the host {problem} — a host is a bare DNS name like \
                         \"api.stripe.com\"; where it resolves is the operator's (--outbound-base, \
                         GUIDE §5)",
                        lit.value()
                    ),
                ));
            }
            if let Some(span) = cross_tenant {
                return Err(Error::new(
                    span,
                    "`outbound` with `cross_tenant` is refused in v1 — an outbound call runs in \
                     the one tenant its job was enqueued in (outbound-effects spec §15); drop one",
                ));
            }
        }
        if let Some(lit) = &timeout_lit {
            if outbound_lit.is_none() {
                return Err(Error::new(
                    lit.span(),
                    "`timeout` applies only to an outbound job — add `outbound = \"<host>\"`, or \
                     drop `timeout`",
                ));
            }
            if parse_duration(lit, "timeout")? == 0 {
                return Err(Error::new(
                    lit.span(),
                    "timeout \"0s\" would cancel every call before the remote could answer — give \
                     a positive duration",
                ));
            }
        }
```

`host_name_problem` is copied into the macro crate byte-for-byte from Task 7 (the macro crate cannot link `charpente`). Put a comment on each copy naming the other.

- [ ] **Step 5: Implement the expansion**

Hoist decode into one local function inside `def()`, used by every glue:

```rust
                fn __charpente_decode(
                    payload: &[u8],
                ) -> ::std::result::Result<#name, ::charpente::kernel::JobError> {
                    let input = &mut &payload[..];
                    let job = <#name as ::charpente::core::table::Codec>::decode(input)
                        .map_err(|e| ::charpente::kernel::JobError::App(::std::format!(
                            "job decode for {}: {e} — {}", #name_str, #undecodable
                        )))?;
                    if !input.is_empty() {
                        return ::std::result::Result::Err(::charpente::kernel::JobError::App(
                            ::std::format!(
                                "job decode: {} trailing bytes after a {} payload — {}",
                                input.len(), #name_str, #undecodable,
                            ),
                        ));
                    }
                    ::std::result::Result::Ok(job)
                }
```

The run glue becomes `let job = __charpente_decode(payload)?; #run_call`. Its messages are unchanged, so existing pins hold. For an outbound job, `FINGERPRINT` wraps: `::charpente::core::db::outbound_fingerprint(#fingerprint)`. Add `const OUTBOUND: bool = true;` to the `impl Job`. The run glue's body becomes:

```rust
                    let _ = (tx, payload, cap, scope);
                    ::std::unreachable!(
                        "dispatcher invariant: outbound job {} reached its run glue — dispatch \
                         routes an outbound job to call/settle by its row's minted key",
                        #name_str
                    )
```

The two outbound glues, `global` form:

```rust
                fn __charpente_call_glue(
                    tx: &mut ::charpente::Tx,
                    payload: &[u8],
                    key: &::charpente::outbound::IdempotencyKey,
                    scope: ::std::option::Option<::charpente::kernel::JobScope<'_>>,
                ) -> ::std::result::Result<
                    ::std::option::Option<::charpente::outbound::Call>,
                    ::charpente::kernel::JobError,
                > {
                    let _ = scope; // a global job is handed no tenant
                    let job = __charpente_decode(payload)?;
                    let call: fn(
                        &#name,
                        &::charpente::Tx<'_>,
                        &::charpente::outbound::IdempotencyKey,
                    ) -> ::std::result::Result<
                        ::std::option::Option<::charpente::outbound::Call>,
                        ::charpente::kernel::JobError,
                    > = #name::call;
                    call(&job, tx, key)
                }

                fn __charpente_settle_glue(
                    tx: &mut ::charpente::Tx,
                    payload: &[u8],
                    reply: ::charpente::outbound::Reply,
                    scope: ::std::option::Option<::charpente::kernel::JobScope<'_>>,
                ) -> ::std::result::Result<(), ::charpente::kernel::JobError> {
                    let _ = scope;
                    let job = __charpente_decode(payload)?;
                    let settle: fn(
                        #name,
                        &mut ::charpente::Tx<'_>,
                        ::charpente::outbound::Reply,
                    ) -> ::std::result::Result<(), ::charpente::kernel::JobError> = #name::settle;
                    settle(job, tx, reply)
                }
```

`scoped_by = #path` form: each glue opens the scope as the run glue does (`let scope = scope.expect(…); let mut tx = scope.open::<#path>(tx);`, with the same `expect` text). `call` is bound as `fn(&#name, &::charpente::ScopedTx<'_, '_, #path>, &IdempotencyKey) -> …` and invoked `call(&job, &tx, key)`. `settle` is bound as `fn(#name, &mut ::charpente::ScopedTx<'_, '_, #path>, Reply) -> …` and invoked `settle(job, &mut tx, reply)`. The fn-pointer bindings are what make a `call` taking `&Tx` on a scoped job a compile error. That's the same reason the run glue binds (`compile_fail/scoped_job_run_takes_bare_tx.rs`). The `JobDef` literal gains:

```rust
                    outbound: ::std::option::Option::Some(::charpente::kernel::OutboundDef {
                        host: #host,
                        timeout: ::std::time::Duration::from_millis(#timeout_millis),
                        call: __charpente_call_glue,
                        settle: __charpente_settle_glue,
                    }),
```

`timeout_millis` defaults to `30_000`. A pure job keeps `outbound: ::std::option::Option::None` and emits neither glue.

- [ ] **Step 6: Bless and read the pins**

```bash
TRYBUILD=overwrite cargo test -p charpente-macros --test trybuild
git status --short crates/charpente-macros/tests/compile_fail/   # `git diff` would not list the new, untracked pairs
```

Open and read each new `.stderr` the status lists (`??`), and `git diff` each modified one. Read every new `.stderr` against the macro's strings, the house discipline (`tests/trybuild.rs`'s header). `outbound_run_instead_of_call.stderr` must show `error[E0599]: no function or associated item named \`call\` found for struct \`Charge\``. `outbound_call_cannot_write.stderr` must show `E0596` at the `tx.put` line. A changed existing `.stderr` may differ only in the `ACCEPTED_FORMS` sentence.

Then: `cargo test -p charpente --test outbound` → PASS. `cargo test -p charpente-macros --no-fail-fast` → every test ok.

- [ ] **Step 7: Prove the shape pin can fail**

Remove the `outbound_fingerprint(…)` wrap in the expansion and run `an_outbound_job_declares_its_host_timeout_and_shape`. Expected: FAIL at the R8 assertion. Restore it, touch the file, and record the result.

- [ ] **Step 8: GUIDE §6 and §9**

- §6 gains a subsection, "Outbound jobs: calling the world after commit". It holds the `Charge` example above as a compiled ```rust block (guide-examples compiles it) and the two method signatures for both tenancies.
- It says that `call` reads and cannot write, and that the key is handed to `call` for the app to place.
- It says `timeout` defaults to 30 s.
- It says outbound-ness is part of the shape, so adding or removing `outbound` needs the drain the existing "Changing a job's fields" text describes.
- Correct GUIDE.md:1698-1699: `enqueue_keyed`'s key is the app's own identity for the job, and the outbound idempotency key is minted by the platform.
- §9's refusal list gains the eight with their exact first lines.
- Replace the old `ACCEPTED_FORMS` sentence with the new one everywhere GUIDE quotes it: the two `rust,ignore` refusal comments at `docs/GUIDE.md:1969-1972` and `:1977-1981` (which escape inner quotes as `\"`), and §9's `text` block at `:3578`, `:3579`, `:3580`, `:3583` and `:3584` (unescaped). None of these is `<!-- pinned -->`, so no test would catch a stale one (hard rule 6). The new sentence, unescaped: a job declares `global` or `scoped_by = <Type>`, and may add `cross_tenant`, `retry(max = <int>, backoff = "<n><unit>")` (unit is one of ms, s, m, h), and `outbound = "<host>"` with an optional `timeout = "<n><unit>"`, in any order — e.g. `#[job(scoped_by = Org, retry(max = 3, backoff = "10s"))]`, `#[job(global, cross_tenant)]` or `#[job(scoped_by = Org, outbound = "api.stripe.com", timeout = "30s")]`
- Extend `crates/charpente-macros/src/job.rs`'s module doc. Its first line lists the accepted forms: add `[, outbound = "api.stripe.com"[, timeout = "30s"]]`. Where it lists what the expansion carries, add that an outbound job's `FINGERPRINT` is continued by `charpente_core::db::outbound_fingerprint` and that its `JobDef` carries the `call`/`settle` glue in `outbound`.

Run `cargo build -p guide-examples`.

- [ ] **Step 8a: CLAUDE.md's test list**

`crates/charpente/tests/outbound.rs` exists from Step 1, and `doc_inventory` holds CLAUDE.md's test lists exhaustive. In the Commands section's test-file parenthetical, `(charpente: archive/auth/describe/group_commit/http/kernel/realtime/` becomes `(charpente: archive/auth/describe/group_commit/http/kernel/outbound/realtime/`. Leave the parenthetical's line breaks alone; the gate requires `). Every list` to stay on one line. Run `cargo test -p guide-examples --test doc_inventory`. Expected: PASS.

- [ ] **Step 9: Gate and commit**

Run the three gate commands, counted.

```bash
git add crates/charpente-macros/src/job.rs crates/charpente-macros/tests/compile_fail/ \
  crates/charpente/tests/outbound.rs docs/GUIDE.md CLAUDE.md
git commit -m "macros: #[job(outbound = \"host\", timeout = \"30s\")] — call/settle glue, shape fold, eight compile-time refusals (outbound-effects spec §5)"
```

---

### Task 9: `Host` entries on the `App`, the host-set rule, and `describe`

**Files:**
- Modify: `crates/charpente/src/http/app.rs` (fields `hosts: Vec<Host>`, `max_in_flight: usize`; methods `outbound`, `outbound_max_in_flight`, `outbound_hosts`, `take_outbound`)
- Modify: `crates/charpente/src/outbound.rs` (`check_host_sets`, `DEFAULT_MAX_IN_FLIGHT`)
- Modify: `crates/charpente/src/describe.rs` (`JobInfo.outbound`, `OutboundInfo`, `Contract.outbound_hosts`, `HostInfo`, the human renderer near `:466`)
- Modify: the describe goldens, `examples/invoicing/tests/describe_golden.rs`, `crates/guide-examples/tests/describe_contract.rs` (via GUIDE §13's JSON), `crates/charpente-cli/tests/describe.rs`, plus any other the run names
- Modify: `crates/charpente/tests/describe.rs` (re-bless its full-document `GOLDEN` at `:166`; the two `builtin=false\n` pins in `to_human_carries_public_scope_and_scoped_by`, `:1321-1335`, become `builtin=false outbound=none\n`)
- Modify: `crates/charpente-cli/src/genclient.rs`, the owned mirror of `Contract` (`into_contract` builds the real types field by field, so it is E0063 without this): mirror `OutboundInfo` and `HostInfo`, `JobInfo.outbound`, `Contract.outbound_hosts`, their `into_*`, and `EVERY_SHAPE` gaining one `Some` outbound and one non-empty host
- Modify: `crates/charpente-tsgen/tests/common/mod.rs` (`fixture_contract`, `:57`) and `crates/charpente-tsgen/tests/revive.rs` (`base()`, `:37`): `outbound_hosts: Vec::new()`
- Modify: `docs/GUIDE.md` §6 (declaring a `Host`), §13 (the describe JSON and human lines), and the human jobs sample at `:7360`
- Test: `crates/charpente/tests/outbound.rs`, plus `outbound.rs` unit tests

**Interfaces:**
- Consumes: Task 7's `Host`; Task 8's macro jobs.
- Produces (`App`):
  - `pub fn outbound(self, host: Host) -> App`
  - `pub fn outbound_max_in_flight(self, n: usize) -> App`
  - `pub fn outbound_hosts(&self) -> Vec<&'static str>`: the hosts mounted jobs call, sorted and deduplicated, which is what `sim.outbound` takes. Read it before building a kernel, which takes the job defs.
  - `pub(crate) fn take_outbound(&mut self) -> (Vec<Host>, usize)`
  - `pub(crate) fn outbound_config(&self) -> &[Host]`
- Produces (`charpente::outbound`):
  - `pub const DEFAULT_MAX_IN_FLIGHT: usize = 64;`
  - `pub(crate) fn check_host_sets(callers: &[(&'static str, &'static str)], hosts: &[Host]) -> Result<(), String>`. `callers` is `(job name, host)` in mount order.
- Produces (describe):
  - `JobInfo.outbound: Option<OutboundInfo { host: &'static str, timeout_ms: u64 }>`
  - `Contract.outbound_hosts: Vec<HostInfo { host, env: Vec<&'static str>, injects: Vec<String>, max_request_bytes: usize, max_reply_bytes: usize }>`

- [ ] **Step 1: Write the failing tests**

In `outbound.rs`'s unit tests:

```rust
#[test]
fn the_host_sets_must_match_both_ways() {
    let hosts = [Host::new("pay.test"), Host::new("mail.test")];
    assert_eq!(check_host_sets(&[("Charge", "pay.test"), ("Mail", "mail.test")], &hosts), Ok(()));
    assert_eq!(
        check_host_sets(&[("Charge", "pay.test"), ("Mail", "mail.test"), ("Ask", "llm.test")], &hosts),
        Err("kernel: job Ask calls llm.test, which has no Host entry — add \
             app.outbound(Host::new(\"llm.test\")) with the credentials it needs (GUIDE §6)"
            .to_string())
    );
    assert_eq!(
        check_host_sets(&[("Charge", "pay.test")], &hosts),
        Err("kernel: app.outbound(Host::new(\"mail.test\")) declares a host no mounted job calls — \
             remove it, or mount the job that calls it"
            .to_string())
    );
}
```

In `tests/outbound.rs`, using `Charge` and `Mail` from Task 8:

```rust
use charpente::core::db::Schema;
use charpente::http::{App, AppConfig};
use charpente::jobs;
use charpente::outbound::Host;
use charpente_sim::{FaultConfig, Sim};

fn outbound_app(sim: &Sim) -> App {
    App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![Charge, Mail])
    .outbound(Host::new("pay.test").bearer_env("PAY_KEY").max_reply_bytes(1 << 20))
    .outbound(Host::new("mail.test").header_env("X-Postmark-Server-Token", "POSTMARK_TOKEN"))
}

#[test]
fn describe_lists_outbound_jobs_and_hosts_by_variable_name() {
    let sim = Sim::with_faults(0x0b0, FaultConfig::none());
    let app = outbound_app(&sim);
    assert_eq!(app.outbound_hosts(), vec!["mail.test", "pay.test"]);
    let json = serde_json::to_value(app.describe()).unwrap();
    let job = |name: &str| {
        json["jobs"].as_array().unwrap().iter().find(|j| j["name"] == name).unwrap().clone()
    };
    assert_eq!(job("Charge")["outbound"], serde_json::json!({ "host": "pay.test", "timeout_ms": 2000 }));
    assert_eq!(job("SessionSweep")["outbound"], serde_json::Value::Null);
    assert_eq!(
        json["outbound_hosts"],
        serde_json::json!([
            { "host": "pay.test", "env": ["PAY_KEY"], "injects": ["authorization"],
              "max_request_bytes": 1048576, "max_reply_bytes": 1048576 },
            { "host": "mail.test", "env": ["POSTMARK_TOKEN"], "injects": ["x-postmark-server-token"],
              "max_request_bytes": 1048576, "max_reply_bytes": 4194304 }
        ])
    );
}

#[test]
#[should_panic(expected = "contract: app.outbound(Host::new(\"pay.test\")) declared twice")]
fn a_host_declared_twice_is_a_contract_panic() {
    let sim = Sim::with_faults(0x0b1, FaultConfig::none());
    let _ = outbound_app(&sim).outbound(Host::new("pay.test"));
}
```

`tests/outbound.rs` needs `serde_json`. If `charpente`'s dev-dependencies lack it, add `serde_json = "1"` there: it is already a normal dependency, so the tree does not grow.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p charpente --lib outbound` and `cargo test -p charpente --test outbound`
Expected: FAIL to compile, naming `check_host_sets`, `App::outbound` and `outbound_hosts`.

- [ ] **Step 3: Implement**

`outbound.rs`:

```rust
/// How many calls a node keeps in flight at once (spec §4); set per app with
/// `App::outbound_max_in_flight`.
pub const DEFAULT_MAX_IN_FLIGHT: usize = 64;

/// R9, checked when the kernel is built (plan amendment A5): every host a
/// mounted job calls has exactly one `Host` entry, and every entry is called.
#[cfg_attr(not(test), expect(dead_code, reason = "first caller: Task 11"))]
pub(crate) fn check_host_sets(
    callers: &[(&'static str, &'static str)],
    hosts: &[Host],
) -> Result<(), String> {
    for &(job, host) in callers {
        if !hosts.iter().any(|h| h.name == host) {
            return Err(format!(
                "kernel: job {job} calls {host}, which has no Host entry — add \
                 app.outbound(Host::new(\"{host}\")) with the credentials it needs (GUIDE §6)"
            ));
        }
    }
    for h in hosts {
        if !callers.iter().any(|&(_, host)| host == h.name) {
            return Err(format!(
                "kernel: app.outbound(Host::new(\"{}\")) declares a host no mounted job calls — \
                 remove it, or mount the job that calls it",
                h.name
            ));
        }
    }
    Ok(())
}
```

`http/app.rs`: add `hosts: Vec<Host>` and `max_in_flight: usize` to `App`, initialised in `App::open` to `Vec::new()` and `DEFAULT_MAX_IN_FLIGHT`.

```rust
    /// Declares one outside service its jobs call (outbound-effects spec §9):
    /// exactly one entry per host a mounted job names, checked when the
    /// kernel is built, so mount order is free (plan amendment A5).
    pub fn outbound(mut self, host: Host) -> App {
        assert!(
            !self.hosts.iter().any(|h| h.name() == host.name()),
            "contract: app.outbound(Host::new(\"{}\")) declared twice — one Host entry per host",
            host.name()
        );
        self.hosts.push(host);
        self
    }

    /// How many outbound calls this node keeps in flight at once (default
    /// 64). A due outbound job waits while the table is full; a pure job
    /// never waits behind it (spec §4).
    pub fn outbound_max_in_flight(mut self, n: usize) -> App {
        assert!(n > 0, "contract: App::outbound_max_in_flight(0) would never start a call — give at least 1");
        self.max_in_flight = n;
        self
    }

    /// The hosts this app's mounted jobs call, sorted and deduplicated — what
    /// `sim.outbound(…)` takes (spec §10). Read it before building a kernel,
    /// which takes the job defs.
    pub fn outbound_hosts(&self) -> Vec<&'static str> {
        self.job_defs
            .iter()
            .filter_map(|d| d.outbound.as_ref().map(|o| o.host))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    #[expect(dead_code, reason = "first caller: Task 11")]
    pub(crate) fn take_outbound(&mut self) -> (Vec<Host>, usize) {
        (std::mem::take(&mut self.hosts), self.max_in_flight)
    }

    pub(crate) fn outbound_config(&self) -> &[Host] {
        &self.hosts
    }
```

`describe.rs`. `JobInfo` gains its last field:

```rust
    /// `#[job(…, outbound = "<host>")]`: the host and the kernel's deadline
    /// (outbound-effects spec §11); `None` for a pure job.
    pub outbound: Option<OutboundInfo>,
```

```rust
#[derive(Serialize)]
pub struct OutboundInfo {
    pub host: &'static str,
    pub timeout_ms: u64,
}

/// One `Host` entry: which variables an operator must set, and which headers
/// they fill — names only; a value never reaches `describe` (spec §9).
#[derive(Serialize)]
pub struct HostInfo {
    pub host: &'static str,
    pub env: Vec<&'static str>,
    pub injects: Vec<String>,
    pub max_request_bytes: usize,
    pub max_reply_bytes: usize,
}
```

`job_info` sets `outbound: def.outbound.as_ref().map(|o| OutboundInfo { host: o.host, timeout_ms: u64::try_from(o.timeout.as_millis()).expect("a timeout fits u64 millis") })`. `Contract` gains `pub outbound_hosts: Vec<HostInfo>` after `cron`, filled from `self.outbound_config()` in declaration order, each entry `HostInfo { host: h.name(), env: h.env_vars(), injects: h.injected_headers(), max_request_bytes: h.max_request(), max_reply_bytes: h.max_reply() }`. Those five accessors' first callers are `App::outbound` (`name`) and this, so delete the `expect(dead_code)` attributes Task 7 put on `Host::name`, `max_request`, `max_reply`, `env_vars` and `injected_headers`. `check_host_sets` and `take_outbound` carry theirs until Task 11 (Global Constraints). The human renderer appends ` outbound={host} timeout_ms={n}` (or ` outbound=none`) to each job line. After `cron:` it prints:

```text
outbound_hosts:
  pay.test env=PAY_KEY injects=authorization max_request_bytes=1048576 max_reply_bytes=1048576
```

A host with no credentials prints `env=none injects=none`.

`crates/charpente-cli/src/genclient.rs`, inside `mod contract` (the owned mirror). The mirror `JobInfo` gains `outbound: Option<OutboundInfo>` after `builtin`, the mirror `Contract` gains `outbound_hosts: Vec<HostInfo>` after `cron`, and:

```rust
    #[derive(Deserialize)]
    struct OutboundInfo {
        host: String,
        timeout_ms: u64,
    }

    #[derive(Deserialize)]
    struct HostInfo {
        host: String,
        env: Vec<String>,
        injects: Vec<String>,
        max_request_bytes: usize,
        max_reply_bytes: usize,
    }

    fn into_host(h: HostInfo) -> real::HostInfo {
        real::HostInfo {
            host: leak(h.host),
            env: leak_all(h.env),
            injects: h.injects,
            max_request_bytes: h.max_request_bytes,
            max_reply_bytes: h.max_reply_bytes,
        }
    }
```

`into_contract` gains `outbound_hosts: c.outbound_hosts.into_iter().map(into_host).collect(),` and `into_job` gains `outbound: j.outbound.map(|o| real::OutboundInfo { host: leak(o.host), timeout_ms: o.timeout_ms }),`. In the test module's `EVERY_SHAPE`, `SendMail` gains `"outbound": { "host": "mail.test", "timeout_ms": 30000 }`, and the document gains, after `"cron"`, `"outbound_hosts": [{ "host": "mail.test", "env": ["MAIL_TOKEN"], "injects": ["x-mail-token"], "max_request_bytes": 1048576, "max_reply_bytes": 4194304 }]`. `round_trip_preserves_every_shape` then pins both new shapes' serde spelling.

`crates/charpente-tsgen/tests/common/mod.rs`'s `fixture_contract` and `tests/revive.rs`'s `base()` build `Contract` without `..`: each gains `outbound_hosts: Vec::new(),`.

- [ ] **Step 4: Run the tests, then re-bless the goldens by reading them**

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
cargo test -p charpente --test outbound
cargo test -p charpente-cli --lib --no-fail-fast 2>&1 | tee $SDD/task9-goldens.log
cargo test -p charpente --test describe --no-fail-fast 2>&1 | tee -a $SDD/task9-goldens.log
cargo test -p invoicing --test describe_golden --no-fail-fast 2>&1 | tee -a $SDD/task9-goldens.log
cargo test -p guide-examples --test describe_contract --no-fail-fast 2>&1 | tee -a $SDD/task9-goldens.log
cargo test -p charpente-cli --test describe --no-fail-fast 2>&1 | tee -a $SDD/task9-goldens.log
```

Expected: `outbound` passes, and `charpente-cli --lib` passes (`round_trip_preserves_every_shape` with `EVERY_SHAPE`'s new fields). The goldens fail with exactly two kinds of difference: `"outbound": null` on every job, and `"outbound_hosts": []`, plus the human lines' ` outbound=none` and an empty `outbound_hosts:` section. Update each golden (`crates/charpente/tests/describe.rs`'s `GOLDEN` included) and GUIDE §13's JSON by hand to that, and nothing else. In `describe.rs`'s `to_human_carries_public_scope_and_scoped_by`, the `Reconcile` and `Rollup` pins end `builtin=false outbound=none\n`. A golden that differs in any other way is a stop-and-report.

- [ ] **Step 5: GUIDE**

- §6 gains "Declaring the hosts": the two `app.outbound(Host::new(…))` lines from spec §9, the builders, and the rule that hosts must match both ways (the two refusal texts).
- §6 also says secrets are variable names, with values read at `serve` start and never in the log, the sim or `describe`.
- §6 gives `outbound_max_in_flight` and its default.
- §13 shows the new fields.
- The human jobs sample at `docs/GUIDE.md:7360` (`Rollup max=2 … builtin=false`) gains ` outbound=none`, as the renderer now prints it.

Run `cargo build -p guide-examples` and `cargo test -p guide-examples --no-fail-fast`.

- [ ] **Step 6: Gate and commit**

Run the three gate commands, counted.

```bash
git add crates/charpente/src/http/app.rs crates/charpente/src/outbound.rs crates/charpente/src/describe.rs \
  crates/charpente/tests/outbound.rs crates/charpente/tests/describe.rs crates/charpente/Cargo.toml \
  crates/charpente-cli/src/genclient.rs crates/charpente-tsgen/tests/common/mod.rs crates/charpente-tsgen/tests/revive.rs \
  docs/GUIDE.md <each other golden updated>
git commit -m "charpente: Host entries on the App, the both-ways host rule, describe lists outbound jobs and hosts (outbound-effects spec §9, §11)"
```

---

### Task 10: The dispatcher's outbound due-set, in-flight table and call path

**Files:**
- Create: `crates/charpente/src/kernel/outbound.rs` (`InFlight`, `Flight`)
- Modify: `crates/charpente/src/kernel/dispatch.rs` (`Dispatcher` fields, `with_outbound`, `rebuild`, `note_enqueued`, `next_deadline`, `has_due`, `run_due_into`, `run_due`, new `run_call_into`, `fail_outbound`, `take_starts`, `in_flight_len`; `JobOutcome::Calling`)
- Modify: `crates/charpente/src/kernel/mod.rs` (`mod outbound;`; the `Ready::Job` arm counts `Calling`)
- Modify: `crates/charpente/src/outbound.rs` (delete Task 7's dead-code attributes on `Call::into_wire` and `check`)
- Modify: `crates/charpente/tests/kernel.rs`: its two exhaustive `JobOutcome` matches (`:466-475`, `:1632-1636`) gain `JobOutcome::Calling { .. } => unreachable!("no outbound job is mounted here")`, or they are E0004 once `Calling` exists
- Test: `crates/charpente/tests/outbound.rs`

**Interfaces:**
- Consumes: Task 5's `Enqueued`; Task 6's `stage_failure_record`, `not_primary_bug`, `verifying_bug`; Task 7's `Call`, `check`, `Host`, `OutboundDef`; Task 9's `DEFAULT_MAX_IN_FLIGHT`.
- Produces (`Dispatcher`, `pub` because the kernel suites drive it):
  - `with_outbound(self, hosts: Vec<Host>, max_in_flight: usize) -> Dispatcher`
  - `take_starts(&mut self) -> Vec<(u64, OutboundCall)>`
  - `in_flight_len(&self) -> usize`
  - `JobOutcome::Calling { id: u64, token: u64 }`
- Produces (`kernel::outbound`, crate-private):
  - `Flight { id, due_at, deadline, host, timeout }`
  - `InFlight` with `new(max)`, `len`, `has_room`, `holds_job`, `begin(id, due_at, now, host, timeout, call) -> u64`, `take_starts`, `abandon_starts() -> Vec<(u64, u64)>`, `finish(token) -> Option<Flight>`, `next_deadline() -> Option<u64>`, `expired(now) -> Option<u64>`, `push_completion`, `pop_completion`, `has_completions`, `drop_all() -> Vec<u64>`. Task 11 uses the completion half, so `next_deadline`, `expired`, `push_completion`, `pop_completion`, `has_completions`, `drop_all` and the fields `Flight.host`/`Flight.timeout` carry `#[expect(dead_code, reason = "first caller: Task 11")]` (`"first reader: Task 11"` on the fields) until then.

- [ ] **Step 1: Write the failing tests** (`tests/outbound.rs`)

```rust
use charpente::kernel::{Dispatcher, JobOutcome};
use charpente::core::job::{JobRow, JobStatus};
use charpente::core::table::View;

/// A global outbound job whose `call` reads a counter row so a test can see
/// what it saw, and builds the call only while `go` is set.
#[charpente::table(global)]
struct Gate {
    id: charpente::Id<Gate>,
    go: bool,
}

#[job(global, outbound = "pay.test", retry(max = 3, backoff = "1s"), timeout = "2s")]
struct Gated {
    n: u64,
}

impl Gated {
    fn call(&self, tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        let gate = tx.get(charpente::Id::<Gate>::from_raw(0))?;
        if !gate.is_some_and(|g| g.go) {
            return Ok(None);
        }
        Ok(Some(Call::post(format!("/charge/{}", self.n)).header("idempotency-key", key.as_str())))
    }

    fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
        Ok(())
    }
}

fn gated_app(sim: &Sim, go: bool, jobs: u64) -> App {
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Gate>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![Gated, ChargeAsPure])
    .outbound(Host::new("pay.test"));
    app.transact_for_tests(|tx| {
        tx.put(&Gate { id: charpente::Id::from_raw(0), go });
        for n in 0..jobs {
            tx.enqueue(Gated { n });
        }
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    app
}

fn dispatcher_for(app: &mut App, max_in_flight: usize) -> Dispatcher {
    let mut d = Dispatcher::new(app.take_job_defs_for_tests())
        .with_outbound(vec![Host::new("pay.test")], max_in_flight);
    d.rebuild(&app.db_for_tests().snapshot().unwrap()).unwrap();
    d
}

#[test]
fn a_call_that_answers_none_is_done_with_no_start() {
    let sim = Sim::with_faults(0x0c0, FaultConfig::none());
    let mut app = gated_app(&sim, false, 1);
    let mut d = dispatcher_for(&mut app, 64);
    let db = app.db_for_tests();
    let mut group = db.begin_group().unwrap();
    assert!(matches!(d.run_due_into(&mut group, 0).unwrap(), JobOutcome::Done { id: 0 }));
    group.commit().map_err(|f| f.error).unwrap();
    assert!(d.take_starts().is_empty());
    assert_eq!(d.in_flight_len(), 0);
    let row = db.snapshot().unwrap().scan::<JobRow>().unwrap().remove(0);
    assert_eq!(row.status, JobStatus::Done);
}

#[test]
fn a_decided_call_stages_nothing_and_buffers_its_start_with_the_row_key() {
    let sim = Sim::with_faults(0x0c1, FaultConfig::none());
    let mut app = gated_app(&sim, true, 1);
    let mut d = dispatcher_for(&mut app, 64);
    let db = app.db_for_tests();
    let key = db.snapshot().unwrap().scan::<JobRow>().unwrap()[0].outbound_key.unwrap();
    let mut group = db.begin_group().unwrap();
    let JobOutcome::Calling { id: 0, token } = d.run_due_into(&mut group, 0).unwrap() else {
        panic!("a decided call is Calling");
    };
    assert!(group.is_empty(), "R3: call's stage nets no record");
    group.commit().map_err(|f| f.error).unwrap();
    let starts = d.take_starts();
    assert_eq!(starts.len(), 1);
    assert_eq!(starts[0].0, token);
    assert_eq!(starts[0].1.path, "/charge/0");
    assert_eq!(starts[0].1.key.as_bytes(), &key, "the row's minted key, never a fresh draw");
    assert_eq!(d.in_flight_len(), 1);
    assert!(!d.has_due(u64::MAX), "invariant 2: an in-flight job is in no due-set");
}

#[test]
fn a_full_in_flight_table_never_blocks_a_pure_job() {
    let sim = Sim::with_faults(0x0c2, FaultConfig::none());
    let mut app = gated_app(&sim, true, 3);
    app.transact_for_tests(|tx| {
        tx.enqueue(ChargeAsPure { cents: 1 });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let mut d = dispatcher_for(&mut app, 1);
    let db = app.db_for_tests();
    let mut group = db.begin_group().unwrap();
    assert!(matches!(d.run_due_into(&mut group, 0).unwrap(), JobOutcome::Calling { id: 0, .. }));
    assert!(d.has_due(0), "the pure job (id 3) is still due with the table full");
    assert!(matches!(d.run_due_into(&mut group, 0).unwrap(), JobOutcome::Done { id: 3 }));
    assert!(!d.has_due(u64::MAX), "outbound ids 1 and 2 wait for room");
    group.commit().map_err(|f| f.error).unwrap();
}

#[test]
fn a_refused_call_is_an_attempt_naming_the_rule() {
    // `Gated` with a forbidden header: built by `Leaky`, below.
    let sim = Sim::with_faults(0x0c3, FaultConfig::none());
    let mut app = App::open(Box::new(sim.disk()), Schema::new(), Box::new(sim.entropy()), AppConfig::default())
        .unwrap()
        .mount_jobs(jobs![Leaky])
        .outbound(Host::new("pay.test").bearer_env("PAY_KEY"));
    app.transact_for_tests(|tx| {
        tx.enqueue(Leaky {});
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let mut d = Dispatcher::new(app.take_job_defs_for_tests())
        .with_outbound(vec![Host::new("pay.test").bearer_env("PAY_KEY")], 64);
    d.rebuild(&app.db_for_tests().snapshot().unwrap()).unwrap();
    let db = app.db_for_tests();
    let mut group = db.begin_group().unwrap();
    assert!(matches!(
        d.run_due_into(&mut group, 0).unwrap(),
        JobOutcome::Retried { id: 0, attempts: 1, .. }
    ));
    group.commit().map_err(|f| f.error).unwrap();
    assert!(d.take_starts().is_empty(), "a refused call never starts");
    let row = db.snapshot().unwrap().scan::<JobRow>().unwrap().remove(0);
    assert_eq!(
        row.last_error.as_deref(),
        Some("outbound pay.test: the call sets \"authorization\", which this host's Host entry \
              injects from PAY_KEY — remove it from call; a credential never enters app code")
    );
}

#[job(global, outbound = "pay.test")]
struct Leaky {}

impl Leaky {
    fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(Call::get("/x").header("Authorization", "Bearer sk_live_leaked")))
    }

    fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
        Ok(())
    }
}
```

`gated_app` mounts `ChargeAsPure` beside `Gated`, so `a_full_in_flight_table_never_blocks_a_pure_job` has a pure job to dispatch. The host set is unaffected, since `ChargeAsPure` calls nothing.

Add one more test, `a_call_panic_is_an_attempt_with_the_call_panicked_prefix`, using a job whose `call` panics with `"boom"`. Assert `last_error == Some("call panicked: boom")` (amendment A16).

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p charpente --test outbound`
Expected: FAIL to compile, naming `with_outbound`, `take_starts`, `in_flight_len`, `JobOutcome::Calling`.

- [ ] **Step 3: Write `kernel/outbound.rs`**

```rust
//! The kernel's outbound bookkeeping (outbound-effects spec §4): the
//! in-flight table, the per-process token counter (R7), the starts a job
//! batch buffers until its group commits (R6), and the completions drained
//! from `Outbound::poll`. Volatile by design: a crash, a restart or losing
//! writability drops it, and the store's Pending rows re-dispatch with the
//! same key (§7). Owned by the `Dispatcher`, beside the due-sets it must stay
//! disjoint from (§4 invariant 2).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::time::Duration;

use charpente_core::io::{OutboundCall, OutboundReply};

pub(crate) struct Flight {
    pub id: u64,
    /// The `due_at` it was dispatched at — where it goes back if its start
    /// is abandoned.
    pub due_at: u64,
    pub deadline: u64,
    #[expect(dead_code, reason = "first reader: Task 11")]
    pub host: &'static str,
    #[expect(dead_code, reason = "first reader: Task 11")]
    pub timeout: Duration,
}

pub(crate) struct InFlight {
    flights: BTreeMap<u64, Flight>,
    jobs: BTreeSet<u64>,
    deadlines: BTreeSet<(u64, u64)>,
    next_token: u64,
    max: usize,
    starts: Vec<(u64, OutboundCall)>,
    completions: VecDeque<(u64, io::Result<OutboundReply>)>,
}

impl InFlight {
    pub(crate) fn new(max: usize) -> InFlight {
        InFlight {
            flights: BTreeMap::new(),
            jobs: BTreeSet::new(),
            deadlines: BTreeSet::new(),
            next_token: 0,
            max,
            starts: Vec::new(),
            completions: VecDeque::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.flights.len()
    }

    pub(crate) fn has_room(&self) -> bool {
        self.flights.len() < self.max
    }

    pub(crate) fn holds_job(&self, id: u64) -> bool {
        self.jobs.contains(&id)
    }

    /// Records a decided call and buffers its start until the batch commits
    /// (R6). The token is fresh: never reused within a process (invariant 1).
    pub(crate) fn begin(
        &mut self,
        id: u64,
        due_at: u64,
        now: u64,
        host: &'static str,
        timeout: Duration,
        call: OutboundCall,
    ) -> u64 {
        assert!(self.has_room(), "dispatcher invariant: an outbound job was dispatched with the in-flight table full ({})", self.max);
        assert!(self.jobs.insert(id), "dispatcher invariant: job {id} began a call while already in flight (spec §4 invariant 2)");
        let token = self.next_token;
        self.next_token += 1;
        let deadline = now.saturating_add(u64::try_from(timeout.as_nanos()).unwrap_or(u64::MAX));
        self.flights.insert(token, Flight { id, due_at, deadline, host, timeout });
        self.deadlines.insert((deadline, token));
        self.starts.push((token, call));
        token
    }

    pub(crate) fn take_starts(&mut self) -> Vec<(u64, OutboundCall)> {
        std::mem::take(&mut self.starts)
    }

    /// The batch's commit failed: its calls never start (R6, invariant 5).
    /// Returns each abandoned `(due_at, id)` for the caller to requeue.
    pub(crate) fn abandon_starts(&mut self) -> Vec<(u64, u64)> {
        let tokens: Vec<u64> = self.starts.drain(..).map(|(t, _)| t).collect();
        tokens
            .into_iter()
            .map(|t| {
                let f = self.finish(t).expect("a buffered start is in flight");
                (f.due_at, f.id)
            })
            .collect()
    }

    /// Removes `token`'s flight. `None` for a token not in flight — a late
    /// completion, which the caller discards (invariant 4).
    pub(crate) fn finish(&mut self, token: u64) -> Option<Flight> {
        let f = self.flights.remove(&token)?;
        self.jobs.remove(&f.id);
        self.deadlines.remove(&(f.deadline, token));
        Some(f)
    }

    #[expect(dead_code, reason = "first caller: Task 11")]
    pub(crate) fn next_deadline(&self) -> Option<u64> {
        self.deadlines.first().map(|&(d, _)| d)
    }

    #[expect(dead_code, reason = "first caller: Task 11")]
    pub(crate) fn expired(&self, now: u64) -> Option<u64> {
        self.deadlines.first().filter(|&&(d, _)| d <= now).map(|&(_, t)| t)
    }

    #[expect(dead_code, reason = "first caller: Task 11")]
    pub(crate) fn push_completion(&mut self, c: (u64, io::Result<OutboundReply>)) {
        self.completions.push_back(c);
    }

    #[expect(dead_code, reason = "first caller: Task 11")]
    pub(crate) fn pop_completion(&mut self) -> Option<(u64, io::Result<OutboundReply>)> {
        self.completions.pop_front()
    }

    #[expect(dead_code, reason = "first caller: Task 11")]
    pub(crate) fn has_completions(&self) -> bool {
        !self.completions.is_empty()
    }

    /// Writable → not writable (invariant 3): every flight and buffered
    /// completion is dropped; returns the tokens to cancel. Their jobs stay
    /// Pending in the store for the next primary, with the same key (§7).
    #[expect(dead_code, reason = "first caller: Task 11")]
    pub(crate) fn drop_all(&mut self) -> Vec<u64> {
        assert!(self.starts.is_empty(), "dispatcher invariant: starts buffered across a step boundary");
        self.jobs.clear();
        self.deadlines.clear();
        self.completions.clear();
        std::mem::take(&mut self.flights).into_keys().collect()
    }
}
```

- [ ] **Step 4: Rework the dispatcher**

- `Dispatcher` gains `outbound_due: BTreeSet<(u64, u64)>`, documented as the outbound jobs' `(due_at, id)`, kept apart so a capacity-blocked outbound job never blocks a pure job due after it (spec §4). It also gains `hosts: BTreeMap<&'static str, Host>` and `in_flight: InFlight`, which `new` initialises with `InFlight::new(DEFAULT_MAX_IN_FLIGHT)`.

```rust
    /// The kernel's outbound configuration (built from the `App`'s `Host`
    /// entries, after the host-set check).
    pub fn with_outbound(mut self, hosts: Vec<Host>, max_in_flight: usize) -> Dispatcher {
        self.hosts = hosts.into_iter().map(|h| (h.name(), h)).collect();
        self.in_flight = InFlight::new(max_in_flight);
        self
    }

    /// The earliest dispatchable job: the pure heap's head, or the outbound
    /// set's head while the in-flight table has room. Ids are unique across
    /// both, so the tuple order never consults the flag.
    fn next_due(&self) -> Option<(u64, u64, bool)> {
        let pure = self.due.first().map(|&(d, id)| (d, id, false));
        let outbound = self
            .in_flight
            .has_room()
            .then(|| self.outbound_due.first().map(|&(d, id)| (d, id, true)))
            .flatten();
        match (pure, outbound) {
            (Some(p), Some(o)) => Some(p.min(o)),
            (p, o) => p.or(o),
        }
    }

    pub fn next_deadline(&self) -> Option<u64> {
        self.next_due().map(|(d, _, _)| d)
    }

    fn queue_outbound(&mut self, due_at: u64, id: u64) {
        assert!(
            !self.in_flight.holds_job(id),
            "dispatcher invariant: job {id} queued while in flight (spec §4 invariant 2)"
        );
        self.outbound_due.insert((due_at, id));
    }
```

`has_due` keeps its body (it reads `next_deadline`). `rebuild` clears both sets and asserts `self.in_flight.len() == 0`. The message: "dispatcher invariant: rebuild with calls in flight — a rebuild follows kernel start or a write-gate opening, and losing the gate drops the table (spec §4 invariant 3)". Then it routes each Pending row: `outbound_key.is_some()` goes to `queue_outbound(row.due_at, id)`, otherwise to `due`. `note_enqueued` routes `e.outbound` to `queue_outbound(0, id)`.

`run_due_into` opens with:

```rust
        let (due_at, id, outbound) = self.next_due().expect("run_due called with an empty heap");
        assert!(
            due_at <= now,
            "dispatcher bug: run_due at {now} but the earliest job is due at {due_at}"
        );
        if outbound {
            self.outbound_due.remove(&(due_at, id));
            return self.run_call_into(group, now, due_at, id);
        }
        self.due.remove(&(due_at, id));
```

The rest is unchanged. `run_due` (the one-job wrapper) captures `(due_at, id, outbound)` from `next_due()` the same way. It restores the original entry to the right set through a small `fn requeue(&mut self, due_at: u64, id: u64, outbound: bool)`. A commit failure after `Calling` gets its own arm, ahead of the general `(Ok(outcome), Err(failure))` one. It calls `abandon_starts()` first and requeues from the `(due_at, id)` that returns, never the other way round: `queue_outbound` asserts the job is no longer in flight, so requeueing first would fire it.

```rust
            (Ok(JobOutcome::Calling { .. }), Err(failure)) => {
                // R6: the group did not commit, so its call never starts.
                for (due_at, id) in self.in_flight.abandon_starts() {
                    self.queue_outbound(due_at, id);
                }
                Err(failure.error)
            }
```

Its existing behaviour for pure jobs is unchanged.

```rust
/// How `call`'s stage ends. A decided call leaves through `Err`, so whatever
/// the stage wrote — nothing, for generated glue (R3) — is discarded with it.
enum CallStage {
    Decided { call: Call, key: IdempotencyKey, host: &'static str, timeout: Duration },
    Failed(JobError),
}

impl From<io::Error> for CallStage {
    fn from(e: io::Error) -> CallStage {
        CallStage::Failed(JobError::Io(e))
    }
}
```

```rust
    fn run_call_into(&mut self, group: &mut Group, now: u64, due_at: u64, id: u64) -> io::Result<JobOutcome> {
        let defs = &self.defs;
        let admission = group.tenant_admission();
        let staged = group.stage(|tx| -> Result<(), CallStage> {
            let mut row = tx
                .get_job(Id::from_raw(id), &admission)?
                .unwrap_or_else(|| panic!("dispatcher invariant: due job {id} has no row"));
            assert_eq!(row.status, JobStatus::Pending, "dispatcher invariant: job {id} in the outbound set is not Pending");
            let Some(def) = defs.get(row.name.as_str()) else {
                return Err(CallStage::Failed(JobError::App(format!(
                    "job \"{}\" is not registered — add it to the jobs![…] list passed to \
                     App::mount_jobs (or this log outlived a renamed job type)",
                    row.name
                ))));
            };
            assert_row_matches_def(id, &row, def);
            let out = def.outbound.as_ref().unwrap_or_else(|| {
                panic!(
                    "dispatcher invariant: job {id} (\"{}\") carries a minted key but this binary \
                     declares it a pure job — outbound-ness is part of the shape (R8), and the \
                     shape was asserted equal above",
                    row.name
                )
            });
            let key = IdempotencyKey::from_bytes(row.outbound_key.unwrap_or_else(|| {
                panic!(
                    "dispatcher invariant: job {id} is in the outbound set without a minted key — \
                     only 0x0c/0x0d write one, and only a row with one is routed there"
                )
            }));
            let scope = row.scope.map(|sid| JobScope {
                id: sid,
                admission: &admission,
                job: def.name,
                scope_type: def.scope_type,
                scope_name: def.scoped_by,
            });
            let decided = match catch_unwind(AssertUnwindSafe(|| (out.call)(tx, &row.payload, &key, scope))) {
                Ok(Ok(decided)) => decided,
                Ok(Err(e)) => return Err(CallStage::Failed(e)),
                Err(payload) => {
                    return Err(CallStage::Failed(JobError::App(format!(
                        "call panicked: {}",
                        panic_text(&*payload)
                    ))));
                }
            };
            match decided {
                None => {
                    row.status = JobStatus::Done;
                    row.last_error = None;
                    tx.settle_job(&row, &admission);
                    Ok(())
                }
                Some(call) => Err(CallStage::Decided { call, key, host: out.host, timeout: out.timeout }),
            }
        });
        match staged {
            Ok(()) => Ok(JobOutcome::Done { id }),
            Err(TxError::App(CallStage::Decided { call, key, host, timeout })) => {
                let rules = self.hosts.get(host).unwrap_or_else(|| {
                    panic!(
                        "dispatcher invariant: job {id} calls {host}, which has no Host entry — \
                         the kernel refuses to build with an unmatched host set (R9)"
                    )
                });
                if let Err(rule) = crate::outbound::check(&call, rules) {
                    return self.fail_outbound(group, &admission, id, due_at, now, rule);
                }
                let token = self.in_flight.begin(id, due_at, now, host, timeout, call.into_wire(host, key));
                Ok(JobOutcome::Calling { id, token })
            }
            Err(TxError::App(CallStage::Failed(JobError::App(err)))) => {
                self.fail_outbound(group, &admission, id, due_at, now, err)
            }
            Err(TxError::App(CallStage::Failed(JobError::Io(e)))) if TableReadRefusal::of(&e).is_some() => {
                self.fail_outbound(group, &admission, id, due_at, now, e.to_string())
            }
            Err(TxError::App(CallStage::Failed(JobError::Io(e)))) => {
                self.queue_outbound(due_at, id);
                Err(e)
            }
            Err(TxError::Io(e)) => {
                assert!(
                    JobShapeRefusal::of(&e).is_some() || TableWriteRefusal::of(&e).is_some(),
                    "dispatcher invariant: Group::stage returned an io error that is neither its \
                     job-shape nor its table-write refusal — classify it in dispatch before it \
                     can reach a job: {e}"
                );
                self.fail_outbound(group, &admission, id, due_at, now, e.to_string())
            }
            Err(TxError::NotPrimary { .. }) => {
                self.queue_outbound(due_at, id);
                Err(not_primary_bug())
            }
            Err(TxError::Verifying { .. }) => {
                self.queue_outbound(due_at, id);
                Err(verifying_bug())
            }
        }
    }

    /// An outbound attempt failed (spec §7): the failure record, and the job
    /// back in the outbound set at its backoff — or at its old place if the
    /// record could not be staged.
    fn fail_outbound(
        &mut self,
        group: &mut Group,
        admission: &TenantAdmission<'_>,
        id: u64,
        due_at: u64,
        now: u64,
        err: String,
    ) -> io::Result<JobOutcome> {
        match stage_failure_record(&self.defs, group, admission, id, now, err) {
            Ok(outcome) => {
                if let JobOutcome::Retried { due_at, .. } = outcome {
                    self.queue_outbound(due_at, id);
                }
                Ok(outcome)
            }
            Err(e) => {
                self.queue_outbound(due_at, id);
                Err(e)
            }
        }
    }

    /// The starts the last job batch decided; the kernel issues them only
    /// after that batch's group commits (R6).
    pub fn take_starts(&mut self) -> Vec<(u64, OutboundCall)> {
        self.in_flight.take_starts()
    }

    pub fn in_flight_len(&self) -> usize {
        self.in_flight.len()
    }
```

`JobOutcome` gains `Calling { id: u64, token: u64 }`, documented as "an outbound job's call was decided; its start waits for the batch's commit, and its outcome comes from the outbound arm". In `kernel/mod.rs`'s `Ready::Job` arm, `JobOutcome::Calling { .. } => {}` joins the match; the arm still counts it toward `drained`. Starts are not issued yet: Task 11 does that. Until then the kernel calls `self.dispatcher.take_starts()` and asserts it is empty after the job arm's commit. No test builds a kernel over an outbound job before Task 11, which adds the refusal for a kernel without an `Outbound` and replaces that assert.

`run_call_into` is the first caller of `Call::into_wire` and `crate::outbound::check`, so delete the `cfg_attr(not(test), expect(dead_code, …))` Task 7 put on each. In `tests/kernel.rs`, the two exhaustive `JobOutcome` matches (`:466-475`, `:1632-1636`) each gain the arm `JobOutcome::Calling { .. } => unreachable!("no outbound job is mounted here")`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p charpente --test outbound` → PASS. Then `cargo test -p charpente --test kernel --no-fail-fast` → every test ok, since the pure path is unchanged.

- [ ] **Step 6: Prove the R3 discard is load-bearing**

Two mutations, one GREEN and one RED. Restore and touch the file after each, and record both runs. A green mutation needs its observable printed before you conclude anything.

1. GREEN: the discard holds. Make `call`'s stage write before it leaves through `Err`. In `run_call_into`'s closure, `Some(call) => …` becomes:

   ```rust
                   Some(call) => {
                       // R3 mutation: a write inside call's stage.
                       row.attempts += 1;
                       tx.settle_job(&row, &admission);
                       eprintln!("R3 mutation ran: staged attempts={} inside call's stage", row.attempts);
                       Err(CallStage::Decided { call, key, host: out.host, timeout: out.timeout })
                   }
   ```

   In the test, after `group.commit()`, temporarily add `eprintln!("attempts after commit: {}", db.snapshot().unwrap().scan::<JobRow>().unwrap()[0].attempts);`. Run `cargo test -p charpente --test outbound a_decided_call_stages_nothing_and_buffers_its_start_with_the_row_key -- --nocapture`. Expected: GREEN, with `R3 mutation ran: staged attempts=1` printed (the mutated arm ran) and `attempts after commit: 0` (the write was discarded with the `Err` exit, which is the property).
2. RED: the assertion can fire. Restore 1. In the outer `match staged`'s `Decided` arm, immediately before `self.in_flight.begin(…)`, stage a record outside `call`'s stage, so the outcome stays `Calling` and the group is not empty:

   ```rust
                   // R3 mutation: a record staged beside a decided call.
                   group
                       .stage(|tx| {
                           let mut row = tx.get_job(Id::from_raw(id), &admission)?.expect("the due row");
                           row.attempts += 1;
                           tx.settle_job(&row, &admission);
                           Ok::<_, io::Error>(())
                       })
                       .expect("the mutation's stage");
   ```

   Run the same test. Expected: RED at `assert!(group.is_empty(), "R3: call's stage nets no record")`, not at the `let … else` that expects `Calling`.

- [ ] **Step 7: Gate, oracle 3, commit**

Run the three gate commands, then oracle 3 (expect exit 0: with no outbound job, `next_due` is the old `due.first()`):

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
perl -e 'alarm 590; exec @ARGV' -- $SDD/oracle3.sh task10   # oracle3 mode foreground; background: Global Constraints
```

```bash
git add crates/charpente/src/kernel/outbound.rs crates/charpente/src/kernel/dispatch.rs crates/charpente/src/kernel/mod.rs \
  crates/charpente/src/outbound.rs crates/charpente/tests/outbound.rs crates/charpente/tests/kernel.rs
git commit -m "kernel: outbound due-set, in-flight table and call path — call decides, the start waits for commit (outbound-effects spec §4, R3, R7); oracle 3 unchanged"
```

---

### Task 11: The kernel's outbound arm — starts after commit, completions, deadlines, and §7's table

**Files:**
- Modify: `crates/charpente/src/kernel/mod.rs`. Covers: `Ready::Outbound`; `KernelStats`; the `outbound` field; `KernelBuilder` plus `Kernel::builder`/`with_outbound`; the `build` refusals; the top-of-step drop and poll drain; the ready push; the idle deadline; the job arm's start issue; the arm itself; `outbound_in_flight_for_tests`.
- Modify: `crates/charpente/src/kernel/dispatch.rs` (`settle_into`, `expire_into`, `push_completion`, `pop_completion`, `outbound_ready`, `next_outbound_deadline`, `abandon_starts`, `drop_in_flight`)
- Modify: `crates/charpente/src/kernel/outbound.rs` (the three failure texts; delete Task 10's dead-code attributes)
- Modify: `crates/charpente/src/outbound.rs` and `crates/charpente/src/http/app.rs` (delete the dead-code attributes on `Reply::from_wire`, `check_host_sets` and `App::take_outbound`)
- Modify: `DESIGN.md` §1 (the seventh ready source, its park rule, invariant 6), `docs/GUIDE.md` §6 (failure semantics)
- Test: `crates/charpente/tests/outbound.rs`

**Interfaces:**
- Consumes: Tasks 2, 4, 5, 7, 9 and 10.
- Produces (`charpente::kernel`):
  - `KernelBuilder` with `Kernel::builder(app, clock)`, plus `.replication(Repl)`, `.archive(Archive, Box<dyn ObjectStore>)`, `.outbound(Box<dyn Outbound>)`, and `.build() -> io::Result<Kernel>`
  - `Kernel::with_outbound(app: App, clock: Box<dyn Clock>, outbound: Box<dyn Outbound>) -> io::Result<Kernel>`
  - `KernelStats.{outbound_started, outbound_settled, outbound_failed, outbound_timed_out, outbound_late}: u64`
  - `#[doc(hidden)] Kernel::outbound_in_flight_for_tests(&self) -> usize`
- `Kernel::new`, `with_replication` and `with_archive` keep their signatures and delegate to the builder.

- [ ] **Step 1: Write the failing tests** (`tests/outbound.rs`)

```rust
use charpente::core::db::Refusal;
use charpente::core::io::{Clock, Outbound, OutboundCall, OutboundReply, OutboundWake};
use charpente::kernel::{Kernel, KernelStats, SimSource};
use charpente_sim::outbound::{Idempotent, Recorder, from_fn};

const SEC: u64 = 1_000_000_000;

#[charpente::table(global)]
struct Paid {
    id: charpente::Id<Paid>,
    /// How many times `settle` recorded this payment: 1 for every Done job,
    /// or `settle` committed twice (Task 12's exactly-once oracle).
    times: u32,
}

/// Charges once per job; `settle` records the payment on a 200 and treats
/// anything else as an attempt. Used by every §7 row below.
#[job(global, outbound = "pay.test", retry(max = 3, backoff = "500ms"), timeout = "2s")]
struct Pay {
    n: u64,
}

impl Pay {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(
            Call::post(format!("/pay/{}", self.n)).header("idempotency-key", key.as_str()),
        ))
    }

    fn settle(self, tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        match reply.status() {
            200 => {
                let id = charpente::Id::from_raw(self.n);
                let times = tx.get(id)?.map_or(0, |p: Paid| p.times);
                tx.put(&Paid { id, times: times + 1 });
                Ok(())
            }
            s => Err(JobError::app(format!("pay.test answered {s}"))),
        }
    }
}

fn pay_app(sim: &Sim, host: Host, jobs: u64) -> App {
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Paid>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![Pay])
    .outbound(host);
    app.transact_for_tests(|tx| {
        for n in 0..jobs {
            tx.enqueue(Pay { n });
        }
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    app
}

/// Runs to `horizon` virtual nanos and hands back the App and the stats.
fn run_to(sim: &Sim, app: App, horizon: u64) -> (App, KernelStats) {
    let outbound = sim.outbound(&app.outbound_hosts()).unwrap();
    let mut kernel = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)).unwrap();
    kernel.run(&mut SimSource::new(Box::new(sim.clock()), horizon)).unwrap();
    let stats = kernel.stats();
    (kernel.into_app(), stats)
}

fn jobs_and_paid(app: &mut App) -> (Vec<JobRow>, usize) {
    let snap = app.db_for_tests().snapshot().unwrap();
    (snap.scan::<JobRow>().unwrap(), snap.scan::<Paid>().unwrap().len())
}

fn faults(f: impl FnOnce(&mut FaultConfig)) -> FaultConfig {
    let mut cfg = FaultConfig::none();
    f(&mut cfg);
    cfg
}

#[test]
fn a_settled_reply_commits_its_effects_with_the_done_mark() {
    let sim = Sim::with_faults(0x0d0, FaultConfig::none());
    sim.remote("pay.test", Recorder::new());
    let app = pay_app(&sim, Host::new("pay.test"), 1);
    let (mut app, stats) = run_to(&sim, app, 60 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!((rows[0].status, paid), (JobStatus::Done, 1));
    assert_eq!((stats.outbound_started, stats.outbound_settled, stats.outbound_failed), (1, 1, 0));
    let key = IdempotencyKey::from_bytes(rows[0].outbound_key.unwrap());
    sim.with_remote("pay.test", |r: &Recorder| {
        assert_eq!(r.calls().len(), 1);
        assert_eq!(r.calls()[0].headers, vec![("idempotency-key".to_string(), key.as_str().to_string())]);
    });
}

#[test]
fn a_refused_connection_is_an_attempt_that_says_it_never_arrived() {
    let sim = Sim::with_faults(0x0d1, faults(|c| c.outbound_refused = 1.0));
    sim.remote("pay.test", Recorder::new());
    let (mut app, _) = run_to(&sim, pay_app(&sim, Host::new("pay.test"), 1), 60 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!((rows[0].status, rows[0].attempts, paid), (JobStatus::Dead, 3, 0));
    assert_eq!(
        rows[0].last_error.as_deref(),
        Some("outbound pay.test: connection refused — the call did not reach the remote")
    );
    sim.with_remote("pay.test", |r: &Recorder| assert!(r.calls().is_empty()));
}

#[test]
fn a_lost_reply_times_out_and_every_retry_carries_the_same_key() {
    let sim = Sim::with_faults(0x0d2, faults(|c| c.outbound_lost_reply = 1.0));
    sim.remote("pay.test", Idempotent::header("idempotency-key", Recorder::new()));
    let (mut app, stats) = run_to(&sim, pay_app(&sim, Host::new("pay.test"), 1), 60 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!((rows[0].status, paid), (JobStatus::Dead, 0), "Dead means no reply was settled");
    assert_eq!(
        rows[0].last_error.as_deref(),
        Some("outbound pay.test: timed out after 2s — the remote may have applied this call; the \
              retry carries the same idempotency key")
    );
    assert_eq!(stats.outbound_timed_out, 3);
    let key = IdempotencyKey::from_bytes(rows[0].outbound_key.unwrap());
    let delivered = sim.outbound_deliveries();
    assert_eq!(delivered.len(), 3);
    assert!(delivered.iter().all(|d| d.key == key));
    sim.with_remote("pay.test", |m: &Idempotent<Recorder>| {
        assert_eq!(m.applied().get(key.as_str()), Some(&1), "applied once, replayed twice");
    });
}

#[test]
fn a_settle_error_discards_its_effects_and_is_an_attempt() {
    let sim = Sim::with_faults(0x0d3, FaultConfig::none());
    sim.remote(
        "pay.test",
        from_fn(|_c: &OutboundCall| OutboundReply { status: 503, headers: vec![], body: vec![] }),
    );
    let (mut app, stats) = run_to(&sim, pay_app(&sim, Host::new("pay.test"), 1), 60 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!((rows[0].status, paid), (JobStatus::Dead, 0));
    assert_eq!(rows[0].last_error.as_deref(), Some("pay.test answered 503"));
    assert_eq!((stats.outbound_started, stats.outbound_failed), (3, 3));
}

/// Review Focus 4: a reply over the host's cap is an attempt whose text says
/// the remote applied the call (plan amendment A10).
#[test]
fn an_oversized_reply_is_a_named_attempt() {
    let sim = Sim::with_faults(0x0d4, FaultConfig::none());
    sim.remote(
        "pay.test",
        from_fn(|_c: &OutboundCall| OutboundReply { status: 200, headers: vec![], body: vec![b'x'; 17] }),
    );
    let host = Host::new("pay.test").max_reply_bytes(16);
    let (mut app, _) = run_to(&sim, pay_app(&sim, host, 1), 60 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!((rows[0].status, paid), (JobStatus::Dead, 0));
    assert_eq!(
        rows[0].last_error.as_deref(),
        Some("outbound pay.test: the reply exceeded the 16-byte reply cap — the remote \
              answered, so it applied this call; raise the cap with Host::max_reply_bytes (the \
              retry carries the same idempotency key)")
    );
}

/// Production's documented race (io.rs, `Outbound::cancel`: "A completion for
/// it may still surface; the kernel discards it"): a cancel that loses to the
/// reply. `SimOutbound` never hands out a cancelled flight's completion, so
/// the kernel's late-discard path runs only behind this wrapper, which drops
/// every cancel and forwards everything else.
struct NoCancel<O>(O);

impl<O: Outbound> Outbound for NoCancel<O> {
    fn start(&self, token: u64, call: OutboundCall) -> std::io::Result<()> {
        self.0.start(token, call)
    }
    fn poll(&self) -> Option<(u64, std::io::Result<OutboundReply>)> {
        self.0.poll()
    }
    fn cancel(&self, _token: u64) {}
    fn next_wake(&self) -> OutboundWake {
        self.0.next_wake()
    }
}

/// Review Focus 2 / R7, through `NoCancel`. Timeline (one-way latency 1.5s,
/// timeout 2s, backoff 500ms then 1s): token 0 starts at 0 and times out at
/// 2s; token 1 starts at 2.5s; token 0's reply lands at 3s while token 1 is in
/// flight — late, and it must not settle token 1's attempt. Token 1 times out
/// at 4.5s and its reply (5.5s) is late too, polled one step after token 2
/// starts at 5.5s; token 2 times out at 7.5s, and the job is Dead. Token 2's
/// own reply (8.5s) is never polled: nothing is in flight by then. If the
/// count below differs, re-derive it from this timeline before changing it.
#[test]
fn a_late_reply_never_settles_the_retry() {
    let lat = Duration::from_millis(1500);
    let sim = Sim::with_faults(0x0d5, faults(|c| c.outbound_latency = (lat, lat)));
    sim.remote("pay.test", Recorder::new());
    let app = pay_app(&sim, Host::new("pay.test"), 1);
    let outbound = NoCancel(sim.outbound(&app.outbound_hosts()).unwrap());
    let mut kernel = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)).unwrap();
    kernel.run(&mut SimSource::new(Box::new(sim.clock()), 60 * SEC)).unwrap();
    let stats = kernel.stats();
    let mut app = kernel.into_app();
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!((rows[0].status, paid), (JobStatus::Dead, 0), "no late reply settled anything");
    assert_eq!(stats.outbound_settled, 0);
    assert_eq!(stats.outbound_late, 2);
    assert_eq!(stats.outbound_timed_out, 3);
}

/// Review Focus 1 / R6: a call that read a sibling's uncommitted write never
/// starts when the batch's commit fails. Swept over every crash point the run
/// reaches; the property is "a delivery implies the write it read is durable".
#[test]
fn a_call_never_starts_when_its_group_commit_fails() {
    let mut failed_commits = 0;
    for crash_after in 0..400u64 {
        let sim = Sim::with_faults(0x0d6, FaultConfig::none());
        sim.remote("pay.test", Recorder::new());
        let mut app = gated_app(&sim, false, 0);
        app.transact_for_tests(|tx| {
            tx.enqueue(OpenGate {});
            tx.enqueue(Gated { n: 0 });
            Ok::<(), std::io::Error>(())
        })
        .unwrap();
        let outbound = sim.outbound(&app.outbound_hosts()).unwrap();
        sim.crash_disk_after(crash_after);
        let Ok(mut kernel) = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)) else {
            continue; // the crash landed in kernel build: nothing ran
        };
        if kernel.run(&mut SimSource::new(Box::new(sim.clock()), 60 * SEC)).is_err() {
            failed_commits += 1;
        }
        drop(kernel);
        let delivered = !sim.outbound_deliveries().is_empty();
        sim.restart();
        let mut reopened = gated_app_reopen(&sim);
        let gate_durable = reopened
            .db_for_tests()
            .snapshot()
            .unwrap()
            .get::<Gate>(charpente::Id::from_raw(0))
            .unwrap()
            .is_some_and(|g| g.go);
        assert!(
            !delivered || gate_durable,
            "crash_after {crash_after}: a call that read the gate was delivered, but the write it \
             read never became durable (R6)"
        );
    }
    assert!(failed_commits > 0, "the sweep never failed a commit, so it proved nothing");
}

#[job(global)]
struct OpenGate {}

impl OpenGate {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        tx.put(&Gate { id: charpente::Id::from_raw(0), go: true });
        Ok(())
    }
}

/// Review Focus 5 / invariant 3, and oracle 2's switchover row (plan
/// amendment A19): a switchover reaches the kernel only as this write gate
/// (`Role::HandingOff` calls `forbid_writes`, `repl/mod.rs:300`), and the
/// kernel's drop is level-triggered on the gate, never on the role.
#[test]
fn losing_writability_drops_in_flight_and_keeps_jobs_pending() {
    let lat = Duration::from_millis(500);
    let sim = Sim::with_faults(0x0d7, faults(|c| c.outbound_latency = (lat, lat)));
    sim.remote("pay.test", Idempotent::header("idempotency-key", Recorder::new()));
    let app = pay_app(&sim, Host::new("pay.test"), 1);
    let outbound = sim.outbound(&app.outbound_hosts()).unwrap();
    let mut kernel = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)).unwrap();
    let mut source = SimSource::new(Box::new(sim.clock()), 60 * SEC);
    while kernel.outbound_in_flight_for_tests() == 0 {
        kernel.step(&mut source).unwrap();
    }
    kernel.app_mut_for_tests().db_for_tests().forbid_writes("elsewhere:1".into());
    kernel.step(&mut source).unwrap();
    assert_eq!(kernel.outbound_in_flight_for_tests(), 0, "invariant 3");
    sim.clock().sleep(Duration::from_secs(5));
    kernel.step(&mut source).unwrap();
    let mut app = kernel.into_app();
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!((rows[0].status, rows[0].attempts, paid), (JobStatus::Pending, 0, 0));
    let key = rows[0].outbound_key;

    // The next writable kernel re-dispatches with the same key. The first
    // kernel's build took the job defs and the Host entries, so mount them again.
    let mut app = app.mount_jobs(jobs![Pay]).outbound(Host::new("pay.test"));
    app.db_for_tests().allow_writes();
    let (mut app, _) = run_to(&sim, app, 120 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!((rows[0].status, rows[0].outbound_key, paid), (JobStatus::Done, key, 1));
    let k = IdempotencyKey::from_bytes(key.unwrap());
    sim.with_remote("pay.test", |m: &Idempotent<Recorder>| {
        assert_eq!(m.applied().get(k.as_str()), Some(&1), "two deliveries, one application");
    });
    assert_eq!(sim.outbound_deliveries().len(), 2);
}

#[test]
fn a_crash_mid_flight_re_dispatches_with_the_same_key() {
    let lat = Duration::from_secs(1);
    let sim = Sim::with_faults(0x0d8, faults(|c| c.outbound_latency = (lat, lat)));
    sim.remote("pay.test", Idempotent::header("idempotency-key", Recorder::new()));
    let app = pay_app(&sim, Host::new("pay.test"), 1);
    let (app, _) = run_to(&sim, app, SEC / 2); // started, not answered
    drop(app);
    sim.restart();
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Paid>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![Pay])
    .outbound(Host::new("pay.test"));
    let (mut app, _) = run_to(&sim, app, 60 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!((rows[0].status, paid), (JobStatus::Done, 1));
    assert_eq!(sim.outbound_deliveries().len(), 2, "the pre-crash request still arrived");
    let k = IdempotencyKey::from_bytes(rows[0].outbound_key.unwrap());
    sim.with_remote("pay.test", |m: &Idempotent<Recorder>| {
        assert_eq!(m.applied().get(k.as_str()), Some(&1));
    });
}

#[test]
fn a_kernel_without_an_outbound_refuses_by_name() {
    let sim = Sim::with_faults(0x0d9, FaultConfig::none());
    let app = pay_app(&sim, Host::new("pay.test"), 0);
    let err = Kernel::new(app, Box::new(sim.clock())).err().expect("refused");
    assert_eq!(
        err.to_string(),
        "kernel: job Pay calls pay.test, but this kernel has no Outbound — serve() supplies the \
         production client; a sim test passes one: let outbound = \
         sim.outbound(&app.outbound_hosts())?; Kernel::with_outbound(app, clock, Box::new(outbound))"
    );
    assert!(Refusal::of(&err).is_some());
}

#[test]
fn an_unmatched_host_set_refuses_the_kernel() {
    let sim = Sim::with_faults(0x0da, FaultConfig::none());
    sim.remote("pay.test", Recorder::new());
    let app = pay_app(&sim, Host::new("pay.test"), 0).outbound(Host::new("extra.test"));
    let outbound = sim.outbound(&app.outbound_hosts()).unwrap();
    let err = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)).err().expect("refused");
    assert_eq!(
        err.to_string(),
        "kernel: app.outbound(Host::new(\"extra.test\")) declares a host no mounted job calls — \
         remove it, or mount the job that calls it"
    );
    assert!(Refusal::of(&err).is_some());
}

/// Invariant 6: a kernel that mounts no outbound job calls no `Outbound`
/// method, however long it runs.
struct Tripwire;

impl Outbound for Tripwire {
    fn start(&self, _: u64, _: OutboundCall) -> std::io::Result<()> {
        panic!("invariant 6: start on a kernel with no outbound job")
    }
    fn poll(&self) -> Option<(u64, std::io::Result<OutboundReply>)> {
        panic!("invariant 6: poll on a kernel with no outbound job")
    }
    fn cancel(&self, _: u64) {
        panic!("invariant 6: cancel on a kernel with no outbound job")
    }
    fn next_wake(&self) -> OutboundWake {
        panic!("invariant 6: next_wake on a kernel with no outbound job")
    }
}

#[test]
fn a_kernel_with_no_outbound_job_never_touches_its_outbound() {
    let sim = Sim::with_faults(0x0db, FaultConfig::none());
    let mut app = App::open(Box::new(sim.disk()), Schema::new(), Box::new(sim.entropy()), AppConfig::default())
        .unwrap()
        .mount_jobs(jobs![ChargeAsPure]);
    app.transact_for_tests(|tx| {
        tx.enqueue(ChargeAsPure { cents: 1 });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let mut kernel = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(Tripwire)).unwrap();
    kernel.run(&mut SimSource::new(Box::new(sim.clock()), 3 * 24 * 3600 * SEC)).unwrap();
    assert_eq!(kernel.stats().jobs_done, 1 + 3, "the job plus three daily session sweeps");
}
```

Three more tests cover spec rows that the global `Pay` cannot reach: the scoped form (§5's main example), cron (§5 "Enqueueing is unchanged"), and R8's shape refusal (§14):

```rust
mod scoped_outbound {
    use super::*;
    use charpente::auth::Principal;
    use charpente::http::ApiError;
    use charpente::{ScopedTx, Tenant};

    #[charpente::table(global)]
    pub struct Shop {
        pub id: charpente::Id<Shop>,
        pub name: String,
    }

    impl Tenant for Shop {
        const NAME: &'static str = "Shop";
        type Who = Principal;
        type Error = ApiError;
        fn authorize(_tx: &Tx, _who: &Principal, _scope: charpente::Id<Shop>) -> Result<(), ApiError> {
            panic!("a job has no principal — Shop::authorize is never asked")
        }
    }

    #[charpente::table(scoped_by = Shop)]
    pub struct Sale {
        pub id: charpente::Id<Sale>,
        pub charged: bool,
    }

    #[job(scoped_by = Shop, outbound = "pay.test", retry(max = 3, backoff = "500ms"), timeout = "2s")]
    pub struct ChargeSale {
        pub sale: charpente::Id<Sale>,
    }

    impl ChargeSale {
        fn call(&self, tx: &ScopedTx<'_, '_, Shop>, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
            let Some(sale) = tx.get(self.sale)? else {
                return Err(JobError::app("sale gone"));
            };
            if sale.charged {
                return Ok(None);
            }
            Ok(Some(
                Call::post(format!("/shops/{}/sales/{}", tx.scope().raw(), self.sale.raw()))
                    .header("idempotency-key", key.as_str()),
            ))
        }

        fn settle(self, tx: &mut ScopedTx<'_, '_, Shop>, reply: Reply) -> Result<(), JobError> {
            if reply.status() != 200 {
                return Err(JobError::app(format!("pay.test answered {}", reply.status())));
            }
            tx.put(&Sale { id: self.sale, charged: true });
            Ok(())
        }
    }

    #[test]
    fn a_scoped_outbound_job_calls_and_settles_inside_its_tenant() {
        let sim = Sim::with_faults(0x0dc, FaultConfig::none());
        sim.remote("pay.test", Recorder::new());
        let mut app = App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Sale>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .unwrap()
        .mount_jobs(jobs![ChargeSale])
        .outbound(Host::new("pay.test"));
        // The operator path (kernel.rs's `enqueue_in_desk`), through a minting group.
        let mint = sim.entropy();
        for shop in [3u64, 4] {
            let mut group = app.db_for_tests().begin_group_minting(&mint).unwrap();
            let admission = group.tenant_admission();
            group
                .stage(|tx| {
                    let mut scoped = tx.admit_scope(charpente::Id::<Shop>::from_raw(shop), &admission);
                    scoped.put(&Sale { id: charpente::Id::from_raw(1), charged: false });
                    scoped.enqueue(ChargeSale { sale: charpente::Id::from_raw(1) });
                    Ok::<(), std::io::Error>(())
                })
                .unwrap();
            group.commit().map_err(|f| f.error).unwrap();
        }
        let (mut app, _) = run_to(&sim, app, 60 * SEC);
        sim.with_remote("pay.test", |r: &Recorder| {
            let mut paths: Vec<&str> = r.calls().iter().map(|c| c.path.as_str()).collect();
            paths.sort_unstable();
            assert_eq!(paths, vec!["/shops/3/sales/1", "/shops/4/sales/1"], "call ran in each job's own tenant");
        });
        let snap = app.db_for_tests().snapshot().unwrap();
        for shop in [3u64, 4] {
            let sale = snap.get_scoped::<Sale>(shop, charpente::Id::from_raw(1)).unwrap().unwrap();
            assert!(sale.charged, "settle wrote in shop {shop}, its own tenant");
        }
    }
}

#[test]
fn cron_enqueues_an_outbound_job_with_a_minted_key() {
    let sim = Sim::with_faults(0x0dd, FaultConfig::none());
    sim.remote("pay.test", Recorder::new());
    let app = pay_app(&sim, Host::new("pay.test"), 0).schedule("hourly", Duration::from_secs(3600), || Pay { n: 9 });
    let (mut app, _) = run_to(&sim, app, 2 * 3600 * SEC + 60 * SEC);
    let (rows, _) = jobs_and_paid(&mut app);
    let pay: Vec<_> = rows.iter().filter(|r| r.name == "Pay").collect();
    assert!(pay.len() >= 2, "a tick per hour: {}", pay.len());
    let keys: std::collections::BTreeSet<_> = pay.iter().map(|r| r.outbound_key.expect("the cron arm's group minted it (plan amendment A9)")).collect();
    assert_eq!(keys.len(), pay.len(), "one key per tick, never shared");
    assert!(pay.iter().all(|r| r.status == JobStatus::Done));
}

mod shape_v1 {
    use super::*;
    #[job(global)]
    pub struct Settle {
        pub n: u64,
    }
    impl Settle {
        fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
            Ok(())
        }
    }
}

mod shape_v2 {
    use super::*;
    #[job(global, outbound = "pay.test")]
    pub struct Settle {
        pub n: u64,
    }
    impl Settle {
        fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
            Ok(None)
        }
        fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
            Ok(())
        }
    }
}

/// R8 / spec §14: declaring `outbound` is a shape change, refused by the
/// existing refusal while rows of the old shape are Pending.
#[test]
fn a_job_gaining_outbound_is_refused_while_old_rows_are_pending() {
    let sim = Sim::with_faults(0x0de, FaultConfig::none());
    sim.remote("pay.test", Recorder::new());
    let mut app = App::open(Box::new(sim.disk()), Schema::new(), Box::new(sim.entropy()), AppConfig::default()).unwrap();
    app.db_for_tests()
        .transact(|tx| {
            tx.enqueue(shape_v1::Settle { n: 1 });
            Ok::<(), std::io::Error>(())
        })
        .unwrap();
    let app = app.mount_jobs(jobs![shape_v2::Settle]).outbound(Host::new("pay.test"));
    let outbound = sim.outbound(&app.outbound_hosts()).unwrap();
    let err = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)).err().expect("refused");
    assert!(err.to_string().starts_with("1 Pending \"Settle\" job(s) (ids 0) were enqueued under a different shape"), "{err}");
    assert!(err.to_string().contains("its `outbound` declaration"), "Task 5 Step 5a: the fix names outbound: {err}");
    assert!(Refusal::of(&err).is_some());
}
```

`gated_app_reopen(&sim)` is `gated_app` without the seeding transaction: `App::open` over the same disk with the same mounts. Factor the opening half of Task 10's `gated_app` into it, so both share one body. Its `jobs![…]` becomes `jobs![Gated, ChargeAsPure, OpenGate]`, since `OpenGate` is defined here, after Task 10.

A kernel's build takes the app's job defs (`take_job_defs`) and its `Host` entries (`take_outbound`), so an `App` handed back by `into_app()` mounts nothing. Any test that builds a second kernel from one re-mounts first, `app.mount_jobs(jobs![…]).outbound(Host::new(…))`, as `losing_writability_drops_in_flight_and_keeps_jobs_pending` does. `register_job_shapes` accepts the same shape again. Otherwise the second dispatcher records `job "Pay" is not registered …` as an attempt.

`a_kernel_with_no_outbound_job_never_touches_its_outbound`'s `jobs_done` count assumes the built-in daily sweep runs once per day. If the first run lands at a different count, read `kernel_jobs_are_exactly_once…`'s cron facts and fix the arithmetic in the comment and the assertion together. The property under test is the absent panic.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p charpente --test outbound`
Expected: FAIL to compile, naming `Kernel::with_outbound`, `KernelStats::outbound_started`, `outbound_in_flight_for_tests`.

- [ ] **Step 3: The failure texts** (`kernel/outbound.rs`)

```rust
/// Spec §7: an attempt that may have reached the remote says so. Only
/// `ConnectionRefused` is "certainly not delivered" (io.rs's contract).
pub(crate) fn transport_failure(host: &str, e: &io::Error) -> String {
    if e.kind() == io::ErrorKind::ConnectionRefused {
        format!("outbound {host}: {e} — the call did not reach the remote")
    } else {
        format!(
            "outbound {host}: {e} — the remote may have applied this call; the retry carries the \
             same idempotency key"
        )
    }
}

pub(crate) fn timed_out(host: &str, timeout: Duration) -> String {
    format!(
        "outbound {host}: timed out after {timeout:?} — the remote may have applied this call; \
         the retry carries the same idempotency key"
    )
}

/// Plan amendment A10.
pub(crate) fn reply_over_cap(host: &str, cap: usize) -> String {
    format!(
        "outbound {host}: the reply exceeded the {cap}-byte reply cap — the remote \
         answered, so it applied this call; raise the cap with Host::max_reply_bytes (the retry \
         carries the same idempotency key)"
    )
}
```

- [ ] **Step 4: The dispatcher's completion half** (`dispatch.rs`)

```rust
    pub(crate) fn push_completion(&mut self, c: (u64, io::Result<OutboundReply>)) {
        self.in_flight.push_completion(c);
    }

    pub(crate) fn pop_completion(&mut self) -> Option<(u64, io::Result<OutboundReply>)> {
        self.in_flight.pop_completion()
    }

    /// The outbound arm's ready predicate: a completion to settle, or a
    /// deadline passed.
    pub(crate) fn outbound_ready(&self, now: u64) -> bool {
        self.in_flight.has_completions() || self.in_flight.expired(now).is_some()
    }

    pub(crate) fn next_outbound_deadline(&self) -> Option<u64> {
        self.in_flight.next_deadline()
    }

    /// The job batch's commit failed: no start is issued (R6), and each
    /// abandoned job returns to the outbound set at its old place.
    pub(crate) fn abandon_starts(&mut self) {
        for (due_at, id) in self.in_flight.abandon_starts() {
            self.queue_outbound(due_at, id);
        }
    }

    /// Invariant 3: the node stopped being writable. Returns the tokens to
    /// cancel; the jobs stay Pending in the store and come back through
    /// `rebuild` when this node is writable again, or on the new primary.
    pub(crate) fn drop_in_flight(&mut self) -> Vec<u64> {
        self.in_flight.drop_all()
    }

    /// The earliest passed deadline: the flight is finished and its failure
    /// record staged. Returns the token, which the caller cancels.
    pub(crate) fn expire_into(&mut self, group: &mut Group, now: u64) -> io::Result<Option<(u64, JobOutcome)>> {
        let Some(token) = self.in_flight.expired(now) else {
            return Ok(None);
        };
        let flight = self.in_flight.finish(token).expect("listed by expired");
        let admission = group.tenant_admission();
        let text = timed_out(flight.host, flight.timeout);
        let outcome = self.fail_outbound(group, &admission, flight.id, flight.due_at, now, text)?;
        Ok(Some((token, outcome)))
    }

    /// One completion. `None` for a token not in flight — a late completion,
    /// discarded by rule (invariant 4). A transport error and an oversized
    /// reply are attempts; a reply runs `settle`, whose `Ok` commits its
    /// effects with the Done mark as one record.
    pub(crate) fn settle_into(
        &mut self,
        group: &mut Group,
        now: u64,
        token: u64,
        result: io::Result<OutboundReply>,
    ) -> io::Result<Option<JobOutcome>> {
        let Some(Flight { id, due_at, host, .. }) = self.in_flight.finish(token) else {
            return Ok(None);
        };
        let admission = group.tenant_admission();
        let reply = match result {
            Ok(reply) => reply,
            Err(e) => {
                let text = transport_failure(host, &e);
                return self.fail_outbound(group, &admission, id, due_at, now, text).map(Some);
            }
        };
        let cap = self.hosts.get(host).expect("a flight's host has a Host entry (R9)").max_reply();
        if reply.body.len() > cap {
            let text = reply_over_cap(host, cap);
            return self.fail_outbound(group, &admission, id, due_at, now, text).map(Some);
        }
        let defs = &self.defs;
        let staged = group.stage(|tx| {
            let mut row = tx
                .get_job(Id::from_raw(id), &admission)?
                .unwrap_or_else(|| panic!("dispatcher invariant: in-flight job {id} has no row"));
            assert_eq!(row.status, JobStatus::Pending, "dispatcher invariant: in-flight job {id} is not Pending");
            let Some(def) = defs.get(row.name.as_str()) else {
                return Err(JobError::App(format!(
                    "job \"{}\" is not registered — add it to the jobs![…] list passed to \
                     App::mount_jobs (or this log outlived a renamed job type)",
                    row.name
                )));
            };
            assert_row_matches_def(id, &row, def);
            let out = def.outbound.as_ref().unwrap_or_else(|| {
                panic!("dispatcher invariant: in-flight job {id} (\"{}\") is not outbound in this binary", row.name)
            });
            let scope = row.scope.map(|sid| JobScope {
                id: sid,
                admission: &admission,
                job: def.name,
                scope_type: def.scope_type,
                scope_name: def.scoped_by,
            });
            let reply = Reply::from_wire(reply);
            match catch_unwind(AssertUnwindSafe(|| (out.settle)(tx, &row.payload, reply, scope))) {
                Ok(result) => result?,
                Err(payload) => {
                    return Err(JobError::App(format!("settle panicked: {}", panic_text(&*payload))));
                }
            }
            row.status = JobStatus::Done;
            row.last_error = None;
            tx.settle_job(&row, &admission);
            Ok(())
        });
        let outcome = match staged {
            Ok(()) => JobOutcome::Done { id },
            Err(TxError::Io(e)) => {
                assert!(
                    JobShapeRefusal::of(&e).is_some() || TableWriteRefusal::of(&e).is_some(),
                    "dispatcher invariant: Group::stage returned an io error that is neither its \
                     job-shape nor its table-write refusal — classify it in dispatch before it \
                     can reach a job: {e}"
                );
                self.fail_outbound(group, &admission, id, due_at, now, e.to_string())?
            }
            Err(TxError::App(JobError::Io(e))) if TableReadRefusal::of(&e).is_some() => {
                self.fail_outbound(group, &admission, id, due_at, now, e.to_string())?
            }
            Err(TxError::App(JobError::App(err))) => {
                self.fail_outbound(group, &admission, id, due_at, now, err)?
            }
            Err(TxError::App(JobError::Io(e))) => {
                // §7: settle's platform fault is not an attempt. Still Pending; called
                // again after the restart this error causes, with the same key.
                self.queue_outbound(due_at, id);
                return Err(e);
            }
            Err(TxError::NotPrimary { .. }) => {
                self.queue_outbound(due_at, id);
                return Err(not_primary_bug());
            }
            Err(TxError::Verifying { .. }) => {
                self.queue_outbound(due_at, id);
                return Err(verifying_bug());
            }
        };
        Ok(Some(outcome))
    }
```

`fail_outbound` already requeues on its own `Err`, so the `?`s above leave the job in the outbound set whenever they bubble.

This task is the first caller of everything still waiting under a dead-code attribute except `Host::credentials` (Task 14), so delete these attributes now:
- `Reply::from_wire`'s `cfg_attr` (Task 7), since `settle_into` calls it;
- `check_host_sets`'s `cfg_attr` and `App::take_outbound`'s `expect` (Task 9), since `Kernel::build` calls both (Step 5);
- the eight in `kernel/outbound.rs` (Task 10): `Flight.host`, `Flight.timeout`, and `InFlight::{next_deadline, expired, push_completion, pop_completion, has_completions, drop_all}`.

An `expect` left behind is an `unfulfilled_lint_expectations` error under the gate.

- [ ] **Step 5: The kernel**

`Ready` gains `Outbound`. `KernelStats` gains the five counters, each with a one-line doc: started (a `start` issued), settled (a reply's `settle` committed Done), failed (any outbound failure record, timeouts included), timed out, late (a completion for a token not in flight, discarded). `Kernel` gains `outbound: Option<Box<dyn Outbound>>`.

```rust
/// Builds a `Kernel` from its parts (outbound-effects spec §4): a node that
/// replicates, archives and calls out at once would otherwise need a
/// five-argument constructor.
pub struct KernelBuilder {
    app: App,
    clock: Box<dyn Clock>,
    repl: Option<Repl>,
    archive: Option<(Archive, Box<dyn ObjectStore>)>,
    outbound: Option<Box<dyn Outbound>>,
}

impl KernelBuilder {
    pub fn replication(mut self, repl: Repl) -> KernelBuilder {
        self.repl = Some(repl);
        self
    }
    pub fn archive(mut self, archive: Archive, store: Box<dyn ObjectStore>) -> KernelBuilder {
        self.archive = Some((archive, store));
        self
    }
    pub fn outbound(mut self, outbound: Box<dyn Outbound>) -> KernelBuilder {
        self.outbound = Some(outbound);
        self
    }
    pub fn build(self) -> io::Result<Kernel> {
        Kernel::build(self.app, self.clock, self.repl, self.archive, self.outbound)
    }
}

impl Kernel {
    pub fn builder(app: App, clock: Box<dyn Clock>) -> KernelBuilder {
        KernelBuilder { app, clock, repl: None, archive: None, outbound: None }
    }

    /// A kernel whose app mounts outbound jobs (spec §4). Build the handle
    /// first — `sim.outbound(&app.outbound_hosts())` borrows the app this moves.
    pub fn with_outbound(app: App, clock: Box<dyn Clock>, outbound: Box<dyn Outbound>) -> io::Result<Kernel> {
        Kernel::builder(app, clock).outbound(outbound).build()
    }
}
```

`new`/`with_replication`/`with_archive` become one-line builder calls. In `build`, replace `Dispatcher::new(app.take_job_defs())` with:

```rust
        let defs = app.take_job_defs();
        let (hosts, max_in_flight) = app.take_outbound();
        let callers: Vec<(&'static str, &'static str)> = defs
            .iter()
            .filter_map(|d| d.outbound.as_ref().map(|o| (d.name, o.host)))
            .collect();
        if let Some(&(job, host)) = callers.first()
            && outbound.is_none()
        {
            return Err(io::Error::other(Refusal(format!(
                "kernel: job {job} calls {host}, but this kernel has no Outbound — serve() \
                 supplies the production client; a sim test passes one: let outbound = \
                 sim.outbound(&app.outbound_hosts())?; Kernel::with_outbound(app, clock, \
                 Box::new(outbound))"
            ))));
        }
        crate::outbound::check_host_sets(&callers, &hosts).map_err(|m| io::Error::other(Refusal(m)))?;
        let mut dispatcher = Dispatcher::new(defs).with_outbound(hosts, max_in_flight);
```

In `step`, right after `let writable = …`:

```rust
        // Spec §4 invariant 3, level-triggered so every way a gate closes is
        // covered: a node that is not writable holds nothing in flight. The
        // jobs stay Pending in the store; their late completions are
        // discarded by rule.
        if !writable && self.dispatcher.in_flight_len() > 0 {
            let outbound = self.outbound.as_ref().expect("calls in flight imply an Outbound");
            for token in self.dispatcher.drop_in_flight() {
                outbound.cancel(token);
            }
        }
        // `poll` consumes (the archive arm's constraint), so completions are
        // drained into the dispatcher before the ready set is built — and
        // only while something is in flight, so a kernel with nothing in
        // flight calls no `Outbound` method (invariant 6).
        if self.dispatcher.in_flight_len() > 0 {
            let outbound = self.outbound.as_ref().expect("calls in flight imply an Outbound");
            while let Some(completion) = outbound.poll() {
                self.dispatcher.push_completion(completion);
            }
        }
```

`ready`'s capacity becomes 7. After the `Tail` push, append `if self.dispatcher.outbound_ready(now) { ready.push(Ready::Outbound); }`. Appending last keeps every existing source's index, and the push never fires for an app with nothing in flight. In the idle branch, add `self.dispatcher.next_outbound_deadline()` to the deadline list, then:

```rust
            // The outbound park (plan amendment A2): replies arrive from
            // outside the kernel, so while anything is in flight it parks no
            // longer than the next completion the impl can name — or than the
            // replication tick (DESIGN.md:326-328) when it cannot.
            if self.dispatcher.in_flight_len() > 0 {
                let outbound = self.outbound.as_ref().expect("calls in flight imply an Outbound");
                let wake = match outbound.next_wake() {
                    OutboundWake::At(at) => Some(at),
                    OutboundWake::Never => None,
                    OutboundWake::Unknown => Some(Instant::from_nanos(now + REPL_TICK_NANOS)),
                };
                if let Some(w) = wake {
                    deadline = Some(deadline.map_or(w, |d| d.min(w)));
                }
            }
```

In the `Ready::Job` arm, replace the Task 10 assert with R6's two sides:

```rust
                match (drain, group.commit()) {
                    // R6 (invariant 5): a call starts only after the group
                    // whose overlay its `call` read has committed.
                    (Ok(()), Ok(())) => self.start_calls(),
                    (Ok(()), Err(failure)) => {
                        self.dispatcher.abandon_starts();
                        return Err(failure.error);
                    }
                    (Err(e), Ok(())) => {
                        self.dispatcher.abandon_starts();
                        return Err(e);
                    }
                    (Err(_), Err(failure)) => {
                        self.dispatcher.abandon_starts();
                        return Err(failure.error);
                    }
                }
```

`JobOutcome::Calling { .. }` in that arm's stats match increments nothing. `outbound_started` counts in `start_calls`:

```rust
    fn start_calls(&mut self) {
        let starts = self.dispatcher.take_starts();
        if starts.is_empty() {
            return;
        }
        let outbound = self.outbound.as_ref().expect("Kernel::build refuses an outbound job without an Outbound");
        for (token, call) in starts {
            self.stats.outbound_started += 1;
            // An `Err` from `start` is a completion, not a kernel failure (io.rs).
            if let Err(e) = outbound.start(token, call) {
                self.dispatcher.push_completion((token, Err(e)));
            }
        }
    }
```

The arm:

```rust
            Ready::Outbound => {
                assert!(
                    writable,
                    "kernel invariant: the outbound arm ran on a node that is not writable — the \
                     top of step drops the in-flight table whenever the gate is shut (spec §4 \
                     invariant 3)"
                );
                // One group, commit once, commit-then-bubble: the job arm's
                // shape, for the job arm's reason (C-1).
                let mut group = self.app.begin_job_group()?;
                let mut drained = 0usize;
                let mut cancels = Vec::new();
                let drain = (|| -> io::Result<()> {
                    while drained < MAX_BATCH {
                        let outcome = if let Some((token, result)) = self.dispatcher.pop_completion() {
                            match self.dispatcher.settle_into(&mut group, now, token, result)? {
                                Some(outcome) => outcome,
                                None => {
                                    self.stats.outbound_late += 1;
                                    continue;
                                }
                            }
                        } else if let Some((token, outcome)) = self.dispatcher.expire_into(&mut group, now)? {
                            cancels.push(token);
                            self.stats.outbound_timed_out += 1;
                            outcome
                        } else {
                            break;
                        };
                        match outcome {
                            JobOutcome::Done { .. } => {
                                self.stats.outbound_settled += 1;
                                self.stats.jobs_done += 1;
                            }
                            JobOutcome::Retried { .. } => {
                                self.stats.outbound_failed += 1;
                                self.stats.jobs_retried += 1;
                            }
                            JobOutcome::Dead { .. } => {
                                self.stats.outbound_failed += 1;
                                self.stats.jobs_dead += 1;
                            }
                            JobOutcome::Calling { .. } => {
                                unreachable!("the outbound arm settles; it never decides a call")
                            }
                        }
                        drained += 1;
                    }
                    Ok(())
                })();
                let commit = group.commit();
                // A cancel is not an effect: it only tells the edge to stop
                // waiting, so it is issued whatever the commit did.
                let outbound = self.outbound.as_ref().expect("in flight implies an Outbound");
                for token in cancels {
                    outbound.cancel(token);
                }
                match (drain, commit) {
                    (Ok(()), Ok(())) => {}
                    (Ok(()), Err(failure)) => return Err(failure.error),
                    (Err(e), Ok(())) => return Err(e),
                    (Err(_), Err(failure)) => return Err(failure.error),
                }
            }
```

Add `#[doc(hidden)] pub fn outbound_in_flight_for_tests(&self) -> usize { self.dispatcher.in_flight_len() }`.

- [ ] **Step 6: Run the tests to verify they pass**

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
cargo test -p charpente --test outbound 2>&1 | tee $SDD/task11-outbound.log; grep -E "^test |test result" $SDD/task11-outbound.log
```

Expected: every test passes. A test with a numeric expectation (`outbound_late == 2`, `jobs_done == 1 + 3`) that fails must be re-derived from its stated timeline and reported in progress.md. Never adjust one silently.

- [ ] **Step 7: Prove R6 and invariant 6 can fail**

1. In the job arm, issue the starts before `group.commit()` instead of after. `self.start_calls()` cannot be called there: the group holds `&mut self.app` until it commits, and `start_calls` takes `&mut self` (E0499). So the mutation inlines its loop field-wise (`dispatcher` and `outbound` are disjoint from `app`), just above `match (drain, group.commit())`:

   ```rust
                   // R6 mutation: starts issued before the commit.
                   for (token, call) in self.dispatcher.take_starts() {
                       let _ = self.outbound.as_ref().expect("R6 mutation").start(token, call);
                   }
   ```

   Run `a_call_never_starts_when_its_group_commit_fails`: expect RED, naming a `crash_after`. Restore and touch.
2. Remove the `if self.dispatcher.in_flight_len() > 0` guard around `poll`. Run `a_kernel_with_no_outbound_job_never_touches_its_outbound`: expect RED, with "invariant 6: poll". Restore and touch.
3. Make a late completion settle its job's CURRENT attempt, which is the bug R7 guards against. By the time a late reply arrives, `expire_into`'s `finish` has already removed its token, so `settle_into` no longer knows the token's job. The mutation therefore needs scaffolding in `kernel/outbound.rs`, used only by the mutation and deleted with the restore:
   - `InFlight` gains `retired: BTreeMap<u64, u64>` (token → job id), set to `BTreeMap::new()` in `new`;
   - `finish` records `self.retired.insert(token, f.id);` before `Some(f)`;
   - and:

   ```rust
       // R7 mutation scaffolding: the in-flight token of the job a retired token belonged to.
       pub(crate) fn current_flight_of_retired(&self, late: u64) -> Option<u64> {
           let id = *self.retired.get(&late)?;
           self.flights.iter().find(|(_, f)| f.id == id).map(|(&t, _)| t)
       }
   ```

   In `settle_into`, `let Some(Flight { id, due_at, host, .. }) = self.in_flight.finish(token) else { return Ok(None); };` becomes:

   ```rust
           // R7 mutation: a late completion falls back to its job's current flight.
           let flight = match self.in_flight.finish(token) {
               Some(f) => f,
               None => {
                   let Some(current) = self.in_flight.current_flight_of_retired(token) else {
                       return Ok(None);
                   };
                   self.in_flight.finish(current).expect("R7 mutation: the current flight is in flight")
               }
           };
           let Flight { id, due_at, host, .. } = flight;
   ```

   Timeline under the mutation, with the test's numbers (one-way latency 1.5 s, timeout 2 s, first backoff 500 ms):
   - Token 0 starts at 0 and times out at 2 s. `expire_into`'s `finish` retires it as job 0's, and the job is Retried for 2.5 s.
   - Token 1 starts at 2.5 s.
   - At 3 s, token 0's 200 lands while token 1 is in flight. `finish(0)` is `None`, so the fallback finishes token 1 and runs `Pay::settle` with token 0's reply. That writes `Paid { times: 1 }`, and the job is Done.
   - From then on nothing is in flight, so token 1's own reply (due at 5.5 s) is never polled.

   Run `a_late_reply_never_settles_the_retry`. Expected: RED at its first assertion, before the counters, which would read `outbound_settled` 1, `outbound_late` 0 and `outbound_timed_out` 1:

   ```text
   assertion `left == right` failed: no late reply settled anything
     left: (Done, 1)
    right: (Dead, 0)
   ```

   Restore and touch both files, the scaffolding included. Before going on, check that `kernel/outbound.rs` has no `retired` and no `current_flight_of_retired`.
4. Delete the level-triggered drop at the top of `step` (the `!writable` branch that drops the in-flight table). Run `losing_writability_drops_in_flight_and_keeps_jobs_pending`: expect RED, either at its "invariant 3" assertion or as the outbound arm's "ran on a node that is not writable" panic. Record which. This is the proof amendment A19 rests on. Restore and touch.

Record each in progress.md with the failing line.

- [ ] **Step 8: DESIGN §1 and GUIDE §6**

- DESIGN §1's ready-source list gains the seventh source: drained only while something is in flight, parked by `next_wake` or the replication tick. The invariant list gains spec §4's invariant 6 verbatim.
- GUIDE §6's outbound subsection gains:
  - "When a call fails": spec §7's table;
  - "Dead means no reply was settled";
  - the money cost of key-ignoring remotes (LLM APIs);
  - the deploy hazard (a changed `call` under an in-flight key);
  - the crash-loop re-call;
  - `Kernel::with_outbound` for sim tests.

Run `cargo build -p guide-examples` and `cargo test -p guide-examples --no-fail-fast`.

- [ ] **Step 8a: Review gate**

Dispatch an `opus` reviewer, with no `name`. Give it the spec, this task's text and `git diff HEAD`. Tell it to write the full report to `$SDD/review-task11.md` before returning. It checks every §4 invariant against the code, re-runs `cargo test -p charpente --test outbound` itself rather than trusting this log, and reports anything that can start a call before commit or settle a token twice. Resolve its findings before Step 9.

- [ ] **Step 9: Gate, oracle 3, commit**

Run the three gate commands, counted, then oracle 3. Expect exit 0: no existing app has anything in flight, so neither the drop, the drain, the push nor the park fires.

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
perl -e 'alarm 590; exec @ARGV' -- $SDD/oracle3.sh task11   # oracle3 mode foreground; background: Global Constraints
```

```bash
git add crates/charpente/src/kernel/mod.rs crates/charpente/src/kernel/dispatch.rs crates/charpente/src/kernel/outbound.rs \
  crates/charpente/src/outbound.rs crates/charpente/src/http/app.rs \
  crates/charpente/tests/outbound.rs DESIGN.md docs/GUIDE.md
git commit -m "kernel: the outbound arm — starts after commit, completions, deadlines, late discards, the writability drop (outbound-effects spec §4, §7); oracle 3 unchanged"
```

---

### Task 12: The swarm property, the three checkers, and the two planted bugs

**Files:**
- Modify: `crates/charpente-sim/src/checks.rs` (`calls_have_durable_jobs`, `keys_are_per_job`, `outbound_settles_by_horizon`)
- Modify: `crates/charpente-sim/src/lib.rs` and `outbound.rs` (`Sim::quiesce_outbound_faults`)
- Modify: `crates/charpente/tests/outbound.rs` (the workload, the swarm property, the planted variants and their pinned seeds)
- Modify: `docs/GUIDE.md` §8 (a compiled end-to-end outbound workload, the checkers, and the planted bugs as the worked "why the key matters")

**Interfaces:**
- Consumes: Task 4's `Sim::outbound_deliveries`; Task 11's kernel and the `Pay`/`Paid` test items.
- Produces (`charpente_sim::checks`):
  - `calls_have_durable_jobs(sim: &Sim, snap: &Snapshot) -> Result<(), String>`
  - `keys_are_per_job(snap: &Snapshot) -> Result<(), String>`
  - `outbound_settles_by_horizon(snap: &Snapshot) -> Result<(), String>`
- Produces (`Sim`): `quiesce_outbound_faults(&self)`.

- [ ] **Step 1: The checkers and the quiesce, test-first** (in `checks.rs`'s unit tests)

```rust
    // Added to the module's existing `use super::*;` and
    // `use charpente_core::io::{Disk, Entropy};` (`Entropy` is already there).
    use charpente_core::db::{Db, DbConfig, Job, Schema, Tx, outbound_fingerprint};
    use charpente_core::io::{HttpMethod, Outbound, OutboundCall};
    use charpente_core::table::{Codec, Unscoped};

    struct Charge;
    impl Codec for Charge {
        fn encode(&self, _out: &mut Vec<u8>) {}
        fn decode(_input: &mut &[u8]) -> std::io::Result<Charge> {
            Ok(Charge)
        }
    }
    impl Job for Charge {
        const NAME: &'static str = "charge";
        const FINGERPRINT: u64 = outbound_fingerprint(0x7E57_0C0C);
        type Scope = Unscoped;
        const OUTBOUND: bool = true;
    }

    /// Hands out `keys` in order: the only way two jobs can share a key,
    /// which is exactly what `keys_are_per_job` must catch.
    struct Scripted(std::cell::RefCell<Vec<[u8; 16]>>);
    impl Entropy for Scripted {
        fn fill(&self, buf: &mut [u8]) {
            buf.copy_from_slice(&self.0.borrow_mut().remove(0));
        }
    }

    /// One Pending outbound job per key, each enqueued through a minting
    /// group over a fresh `Sim`.
    fn jobs_with_keys(keys: &[[u8; 16]]) -> (Sim, Db) {
        let sim = Sim::with_faults(0x0c4, crate::FaultConfig::none());
        let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
        let mint = Scripted(std::cell::RefCell::new(keys.to_vec()));
        let mut group = db.begin_group_minting(&mint).unwrap();
        group
            .stage(|tx: &mut Tx| {
                for _ in keys {
                    tx.enqueue(Charge);
                }
                Ok::<(), std::io::Error>(())
            })
            .unwrap();
        group.commit().map_err(|f| f.error).unwrap();
        (sim, db)
    }

    fn orphan_call(key: [u8; 16]) -> OutboundCall {
        OutboundCall {
            host: "pay.test",
            method: HttpMethod::Post,
            path: "/x".into(),
            headers: Vec::new(),
            body: Vec::new(),
            key: IdempotencyKey::from_bytes(key),
        }
    }

    #[test]
    fn keys_are_per_job_names_both_jobs_and_the_key() {
        let (_sim, db) = jobs_with_keys(&[[1; 16], [2; 16], [1; 16]]);
        let snap = db.snapshot().unwrap();
        assert_eq!(
            keys_are_per_job(&snap),
            Err("keys_are_per_job: jobs 0 and 2 carry the same idempotency key \
                 01010101010101010101010101010101 — keys are minted per enqueue at staging; a \
                 key derived from an id, or drawn at apply, repeats"
                .to_string())
        );
    }

    #[test]
    fn calls_have_durable_jobs_names_an_orphan_call() {
        let (sim, db) = jobs_with_keys(&[[1; 16]]);
        let snap = db.snapshot().unwrap();
        sim.remote("pay.test", crate::outbound::Recorder::new());
        let ob = sim.outbound(&["pay.test"]).unwrap();
        ob.start(0, orphan_call([9; 16])).unwrap();
        let err = calls_have_durable_jobs(&sim, &snap).unwrap_err();
        assert!(
            err.starts_with(
                "calls_have_durable_jobs: a call to pay.test with key \
                 09090909090909090909090909090909 reached the remote at 0ns, but no job in the \
                 recovered store carries that key"
            ),
            "{err}"
        );
    }

    #[test]
    fn outbound_settles_by_horizon_names_a_stuck_job() {
        let (_sim, db) = jobs_with_keys(&[[1; 16]]); // Pending
        let snap = db.snapshot().unwrap();
        let err = outbound_settles_by_horizon(&snap).unwrap_err();
        assert!(err.starts_with("outbound_settles_by_horizon: job 0 (\"charge\") is still Pending"), "{err}");
    }

    #[test]
    fn quiesced_outbound_faults_never_fire_again() {
        let mut cfg = crate::FaultConfig::none();
        cfg.outbound_refused = 1.0;
        let sim = Sim::with_faults(5, cfg);
        sim.remote("pay.test", crate::outbound::Recorder::new());
        sim.quiesce_outbound_faults();
        let ob = sim.outbound(&["pay.test"]).unwrap();
        ob.start(0, orphan_call([1; 16])).unwrap();
        assert_eq!(std::iter::from_fn(|| ob.poll()).next().unwrap().1.unwrap().status, 200);
    }
```

The scripted `Entropy` and the minting group are the platform's own enqueue path, so these rows are exactly what a minting regression would write. A forged store write would prove less.

- [ ] **Step 2: Run them to verify they fail**, then implement:

```rust
use std::collections::{BTreeMap, BTreeSet};

use charpente_core::io::IdempotencyKey;
use charpente_core::job::{JobRow, JobStatus};
use charpente_core::table::{Snapshot, View};

/// Every call that reached a remote carries the key of a job in `snap`, the
/// recovered store (outbound-effects spec §10): a call never fires for an
/// enqueue that is not durable. Single-node — one sim box has no loss window.
/// Attributes by `OutboundCall::key`, never by where the app put the key.
pub fn calls_have_durable_jobs(sim: &Sim, snap: &Snapshot) -> Result<(), String> {
    let keys: BTreeSet<[u8; 16]> = snap
        .scan::<JobRow>()
        .map_err(|e| format!("calls_have_durable_jobs: scanning the jobs: {e}"))?
        .into_iter()
        .filter_map(|row| row.outbound_key)
        .collect();
    for d in sim.outbound_deliveries() {
        if !keys.contains(d.key.as_bytes()) {
            return Err(format!(
                "calls_have_durable_jobs: a call to {} with key {} reached the remote at {}ns, but \
                 no job in the recovered store carries that key — a call fired for an enqueue \
                 that never became durable",
                d.host,
                d.key.as_str(),
                d.at
            ));
        }
    }
    Ok(())
}

/// No two jobs carry one idempotency key (spec §10).
pub fn keys_are_per_job(snap: &Snapshot) -> Result<(), String> {
    let mut seen: BTreeMap<[u8; 16], u64> = BTreeMap::new();
    for row in snap.scan::<JobRow>().map_err(|e| format!("keys_are_per_job: scanning the jobs: {e}"))? {
        let Some(key) = row.outbound_key else { continue };
        if let Some(first) = seen.insert(key, row.id.raw()) {
            return Err(format!(
                "keys_are_per_job: jobs {first} and {} carry the same idempotency key {} — keys \
                 are minted per enqueue at staging; a key derived from an id, or drawn at apply, \
                 repeats",
                row.id.raw(),
                IdempotencyKey::from_bytes(key).as_str()
            ));
        }
    }
    Ok(())
}

/// After a fault-free tail long enough for every retry, every outbound job is
/// Done or Dead (spec §10). Call it only after `quiesce_disk_faults`,
/// `quiesce_outbound_faults`, and a run past the last backoff and deadline.
pub fn outbound_settles_by_horizon(snap: &Snapshot) -> Result<(), String> {
    for row in snap.scan::<JobRow>().map_err(|e| format!("outbound_settles_by_horizon: scanning the jobs: {e}"))? {
        if row.outbound_key.is_some() && row.status == JobStatus::Pending {
            return Err(format!(
                "outbound_settles_by_horizon: job {} (\"{}\") is still Pending after the \
                 fault-free tail, at attempt {} — an outbound job settles or retries to Dead; one \
                 that does neither was lost by the kernel",
                row.id.raw(),
                row.name,
                row.attempts
            ));
        }
    }
    Ok(())
}
```

`OutboundWorld` gains `pub(crate) quiet: bool`, initialised `quiet: false` in `OutboundWorld::new`. It is `pub(crate)` for the reason Task 4 made `models`, `next_handle` and `deliveries` so: `Sim::quiesce_outbound_faults`, in `lib.rs`, sets it. Its doc: the fault-free tail's outbound half; latency stays, since it is not a fault; requests already in flight keep the fate they drew. In `SimOutbound::start`, each fault test becomes `!outbound.quiet && outbound.<axis>.chance(…)`. The short circuit means a quiet world draws nothing from those streams. Update the module docs of `checks.rs` ("v0 shipped exactly three…") to say these three earned their way in with outbound effects.

- [ ] **Step 3: The workload and the property** (`tests/outbound.rs`)

```rust
use std::collections::BTreeMap;

use charpente::TxError;
use charpente::core::io::Entropy as _;
use charpente::core::table::Codec as _;
use charpente_sim::checks::{
    boot_through_fallout, calls_have_durable_jobs, is_fault_fallout, keys_are_per_job,
    outbound_settles_by_horizon,
};
use charpente_sim::run_universes_swarm;

const POOL: u64 = 6;
const ROUNDS: u32 = 3;
const BOOT_TRIES: u32 = 8;
/// Provisional: Step 4 measures both bounds over ten fresh bases and replaces
/// these values and this comment.
const OUTBOUND_CHECKED_FLOOR: u64 = 1;
const OUTBOUND_GAVE_UP_CEILING: u64 = 256;

/// Which `Pay` the workload mounts: the honest one, or one of spec §12's two
/// planted bugs.
#[derive(Clone, Copy)]
enum Variant {
    Honest,
    Keyless,
    Trusting,
}

/// Planted bug (a): the call carries no idempotency key, so a re-call after
/// a lost reply (or a crash, or a timeout) is applied again.
#[job(global, outbound = "pay.test", retry(max = 3, backoff = "500ms"), timeout = "2s")]
struct PayKeyless {
    n: u64,
}

impl PayKeyless {
    fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(Call::post(format!("/pay/{}", self.n))))
    }
    fn settle(self, tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        Pay { n: self.n }.settle(tx, reply)
    }
}

/// Planted bug (b): `settle` takes a 503 for a success, recording a payment
/// the remote never applied.
#[job(global, outbound = "pay.test", retry(max = 3, backoff = "500ms"), timeout = "2s")]
struct PayTrusting {
    n: u64,
}

impl PayTrusting {
    fn call(&self, tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Pay { n: self.n }.call(tx, key)
    }
    fn settle(self, tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        match reply.status() {
            200 | 503 => {
                let id = charpente::Id::from_raw(self.n);
                let times = tx.get(id)?.map_or(0, |p: Paid| p.times);
                tx.put(&Paid { id, times: times + 1 });
                Ok(())
            }
            s => Err(JobError::app(format!("pay.test answered {s}"))),
        }
    }
}

fn open_variant(sim: &Sim, v: Variant) -> std::io::Result<App> {
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Paid>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?;
    let app = match v {
        Variant::Honest => app.mount_jobs(jobs![Pay]),
        Variant::Keyless => app.mount_jobs(jobs![PayKeyless]),
        Variant::Trusting => app.mount_jobs(jobs![PayTrusting]),
    };
    Ok(app.outbound(Host::new("pay.test")))
}

/// Seeds the pool unless an earlier round's seeding is durable: a seed whose
/// commit failed may still have landed, and seeding twice would put two jobs
/// on one `Paid` id.
fn seed_once(app: &mut App, v: Variant) -> Result<(), String> {
    let seeded = app
        .db_for_tests()
        .snapshot()
        .and_then(|s| s.scan::<JobRow>())
        .map_err(|e| e.to_string())?
        .iter()
        .any(|r| r.outbound_key.is_some());
    if seeded {
        return Ok(());
    }
    app.transact_for_tests(|tx| {
        for n in 0..POOL {
            match v {
                Variant::Honest => tx.enqueue(Pay { n }),
                Variant::Keyless => tx.enqueue(PayKeyless { n }),
                Variant::Trusting => tx.enqueue(PayTrusting { n }),
            }
        }
        Ok::<(), std::io::Error>(())
    })
    // `TxError` derives only `Debug`. The injected-fault text lives in `Io`, so
    // that arm must reach `is_fault_fallout` as the error's own text.
    .map_err(|e| match e {
        TxError::Io(e) => e.to_string(),
        other => format!("{other:?}"),
    })
}

/// Runs one kernel to `horizon`; fault fallout ends the round, anything else
/// fails the universe.
fn run_round(sim: &Sim, app: App, horizon: u64) -> Result<Option<App>, String> {
    let outbound = sim.outbound(&app.outbound_hosts()).map_err(|e| e.to_string())?;
    let mut kernel = match Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)) {
        Ok(k) => k,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(None),
        Err(e) => return Err(format!("kernel build failed non-injected: {e}")),
    };
    match kernel.run(&mut SimSource::new(Box::new(sim.clock()), horizon)) {
        Ok(()) => Ok(Some(kernel.into_app())),
        Err(e) if is_fault_fallout(&e.to_string()) => Ok(None),
        Err(e) => Err(format!("kernel stopped non-injected: {e}")),
    }
}

/// Oracle 2's swarm property (spec §12): outbound jobs under every outbound
/// fault axis, disk faults and crashes, then a fault-free tail. Properties:
/// the three checkers; no charge applied twice at the remote; no payment
/// recorded for a charge the remote never applied; `settle` committed at most
/// once per job.
fn outbound_workload(sim: &mut Sim, v: Variant) -> Result<(), String> {
    sim.remote("pay.test", Idempotent::header("idempotency-key", Recorder::new()));
    let entropy = sim.entropy();
    for _round in 0..ROUNDS {
        sim.crash_disk_after(36 + entropy.next_u64() % 438);
        let Some(mut app) =
            boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, || open_variant(sim, v))
                .map_err(|e| format!("open failed non-injected: {e}"))?
        else {
            if sim.gave_up().is_some() {
                return Ok(());
            }
            sim.restart();
            continue;
        };
        match seed_once(&mut app, v) {
            Ok(()) => {}
            Err(e) if is_fault_fallout(&e) => {
                drop(app);
                sim.restart();
                continue;
            }
            Err(e) => return Err(format!("seeding failed non-injected: {e}")),
        }
        let horizon = sim.now().as_nanos() + 20 * SEC;
        drop(run_round(sim, app, horizon)?);
        sim.restart();
    }

    // The fault-free tail (GUIDE §8): long enough for every backoff and deadline.
    sim.quiesce_disk_faults();
    sim.quiesce_outbound_faults();
    let Some(mut app) =
        boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, || open_variant(sim, v))
            .map_err(|e| format!("tail open failed non-injected: {e}"))?
    else {
        return if sim.gave_up().is_some() { Ok(()) } else { Err("the fault-free tail could not boot".into()) };
    };
    seed_once(&mut app, v)?;
    let horizon = sim.now().as_nanos() + 600 * SEC;
    let mut app = run_round(sim, app, horizon)?.ok_or("the fault-free tail's kernel stopped")?;

    let snap = app.db_for_tests().snapshot().map_err(|e| e.to_string())?;
    calls_have_durable_jobs(sim, &snap)?;
    keys_are_per_job(&snap)?;
    outbound_settles_by_horizon(&snap)?;
    let paid = snap.scan::<Paid>().map_err(|e| e.to_string())?;
    let jobs = snap.scan::<JobRow>().map_err(|e| e.to_string())?;
    sim.with_remote("pay.test", |m: &Idempotent<Recorder>| -> Result<(), String> {
        let mut applied: BTreeMap<&str, u32> = BTreeMap::new();
        for c in m.inner().calls() {
            *applied.entry(c.path.as_str()).or_insert(0) += 1;
        }
        if let Some((path, n)) = applied.iter().find(|(_, n)| **n > 1) {
            return Err(format!(
                "double charge: pay.test applied {path} {n} times — a re-call carried no \
                 idempotency key the remote could dedupe on"
            ));
        }
        for p in &paid {
            if p.times != 1 {
                return Err(format!("settle committed {} times for job {}", p.times, p.id.raw()));
            }
            let path = format!("/pay/{}", p.id.raw());
            if !applied.contains_key(path.as_str()) {
                return Err(format!(
                    "phantom charge: job {} recorded a payment, but pay.test never applied \
                     {path} — settle took a reply that was not a success for one",
                    p.id.raw()
                ));
            }
        }
        Ok(())
    })?;
    for row in jobs.iter().filter(|r| r.outbound_key.is_some()) {
        let n = u64::decode(&mut &row.payload[..]).map_err(|e| e.to_string())?;
        let has_paid = paid.iter().any(|p| p.id.raw() == n);
        if (row.status == JobStatus::Done) != has_paid {
            return Err(format!(
                "job {} is {:?} but its payment row is {} — settle's effects and the Done mark \
                 commit as one record",
                row.id.raw(),
                row.status,
                if has_paid { "present" } else { "absent" }
            ));
        }
        if row.status == JobStatus::Done && row.attempts > 0 {
            sim.cover("outbound:settled-after-retry");
        }
    }
    sim.cover("outbound:checked");
    Ok(())
}

#[test]
fn outbound_jobs_are_exactly_once_under_the_fault_matrix() {
    run_universes_swarm!(
        256,
        |sim: &mut Sim| outbound_workload(sim, Variant::Honest),
        require: [
            "outbound:checked",
            "outbound:settled-after-retry",
            "fault:outbound_lost_reply",
            "fault:outbound_lost_request",
            "fault:outbound_refused",
            "fault:outbound_throttle",
            "fault:outbound_slow",
            "fault:crash",
            "restart",
        ],
        floor: [("outbound:checked", OUTBOUND_CHECKED_FLOOR)],
        ceiling: [("universe:gave-up", OUTBOUND_GAVE_UP_CEILING)]
    );
}
```

The `Done ⇔ Paid` check depends on every variant's `settle` writing `Paid` exactly on `Ok`. `Pay`'s does; a variant that changes that must change this check with it. If `run_universes_swarm!` takes a `fn` rather than any `Fn`, the non-capturing closures coerce. If it takes neither, write two named `fn`s (`honest_workload`, …) that call `outbound_workload`.

- [ ] **Step 4: Measure the floor and the ceiling over ten fresh bases, then pin them (GUIDE §8's ambient rule)**

`run_universes_swarm!` runs at an ambient base, so both bounds are margins over ten or more fresh batches, never exact counts (GUIDE §8, "At a pinned base both bounds are exact counts…"). Step 3's block set both constants provisionally (`OUTBOUND_CHECKED_FLOOR = 1`, `OUTBOUND_GAVE_UP_CEILING = 256`). Now run ten fresh bases, in the background, since ten batches can pass 600 s:

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
( for i in 01 02 03 04 05 06 07 08 09 0a; do
    CHARPENTE_BASE_SEED=0x0b1d00$i cargo test -p charpente --test outbound outbound_jobs_are_exactly_once -- --nocapture \
      > $SDD/task12-measure-$i.log 2>&1
    print "$i exit $?" >> $SDD/task12-measure-exits.txt
  done ) &
```

When the loop exits (ten lines in the exits file), read the exits and each batch's counts from the runner's `coverage over 256 universes:` block:

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
grep -c " exit 0$" $SDD/task12-measure-exits.txt   # 10
grep -h -A40 "coverage over" $SDD/task12-measure-*.log | grep -E "outbound:checked|universe:gave-up|fault:outbound_|outbound:settled-after-retry|fault:crash|restart"
```

`CHARPENTE_BASE_SEED` skips `require:`, `floor:` and `ceiling:`, because they are rates measured at the pinned base, so the runner checks none of them here. Check by hand that every `require` key is non-zero in every one of the ten blocks. A key that is missing is a workload too weak to judge, so strengthen the workload rather than drop the key.

- `OUTBOUND_CHECKED_FLOOR` is the mean of the ten `outbound:checked` counts less four sample standard deviations, rounded down.
- `OUTBOUND_GAVE_UP_CEILING` is the larger of twice the largest `universe:gave-up` count and the smallest k with P(Poisson(λ) > k) < 1e-4. Here λ = 256 × U(x) / 2,560, x is the total give-ups over the ten batches, and U(x) is 2.996 for x = 0, 4.744 for 1, or 6.296 for 2. For x > 2, U(x) = ½·χ²₀.₉₅(2x+2); twice the largest count is then normally the larger number.

Give the two constants one doc comment in `crates/charpente-sim/tests/wal.rs`'s form (`FAULT_MATRIX_GAVE_UP_CEILING`). It states the ten bases, the range, mean and sample sd of `outbound:checked`, the give-up total, and the Poisson arithmetic. Record the raw counts in progress.md. Then run the property once without `CHARPENTE_BASE_SEED` and confirm it passes with the bounds enforced.

- [ ] **Step 5: Find and pin the planted bugs' seeds**

For each of `Variant::Keyless` and `Variant::Trusting`, add a temporary test that runs `run_universes_swarm!(1024, |sim: &mut Sim| outbound_workload(sim, Variant::Keyless))`. Run it, and copy the first failure's `seed:` and message from its output into progress.md. Expected: Keyless fails with `double charge: pay.test applied /pay/…`, and Trusting with `phantom charge: job …`. A planted bug that 1,024 universes never catch is a finding about the workload: stop and report. Delete the temporary tests, then pin:

```rust
/// The universe a pinned seed names (the swarm runner's own derivation).
fn universe(seed: u64) -> Sim {
    let mut state = seed;
    let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
    let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
    charpente_sim::sample_extended_fields(seed, &mut cfg);
    Sim::with_faults(seed, cfg)
}

/// Planted bug (a), spec §12 oracle 4: without the key, a re-call is a second
/// charge. Seed found by a 1,024-universe hunt on <date> (progress.md).
#[test]
fn planted_keyless_call_is_a_double_charge_with_a_recorded_seed() {
    const SEED: u64 = 0x0; // the hunt's seed — Step 5 replaces this literal
    let err = outbound_workload(&mut universe(SEED), Variant::Keyless)
        .expect_err("the planted bug must be caught");
    assert!(err.starts_with("double charge: pay.test applied /pay/"), "seed {SEED:#x}: {err}");
    outbound_workload(&mut universe(SEED), Variant::Honest)
        .unwrap_or_else(|e| panic!("seed {SEED:#x}: the honest job must pass where the planted one fails: {e}"));
}

/// Planted bug (b): a 503 taken for a success records a charge that never
/// happened. Seed found by a 1,024-universe hunt on <date> (progress.md).
#[test]
fn planted_trusting_settle_is_a_phantom_charge_with_a_recorded_seed() {
    const SEED: u64 = 0x0; // the hunt's seed — Step 5 replaces this literal
    let mut sim = universe(SEED);
    let err = outbound_workload(&mut sim, Variant::Trusting).expect_err("the planted bug must be caught");
    assert!(err.starts_with("phantom charge: job "), "seed {SEED:#x}: {err}");
    assert!(
        sim.coverage().contains_key("fault:outbound_throttle"),
        "seed {SEED:#x}: the reason is a throttled 503, and none fired: {:?}",
        sim.coverage()
    );
    outbound_workload(&mut universe(SEED), Variant::Honest)
        .unwrap_or_else(|e| panic!("seed {SEED:#x}: the honest job must pass where the planted one fails: {e}"));
}
```

Replace both `0x0` literals and both `<date>` markers with the hunt's values before committing. The commit is refused if `grep -n "const SEED: u64 = 0x0;" crates/charpente/tests/outbound.rs` matches.

- [ ] **Step 6: Prove each property is what catches its bug**

1. Delete the `double charge` check from `outbound_workload`. Run the keyless pin: expect RED at `expect_err` (the bug escapes). Restore and touch.
2. Delete the `phantom charge` check. Run the trusting pin: expect RED the same way. Restore and touch.
3. In Task 5's mint (`Group::stage`), replace the drawn key with one derived from the job name: in place of the `let mut okey …`, `mint.fill(&mut okey);` and `enqueue.okey = Some(okey);` lines, write `enqueue.okey = Some([enqueue.job.len() as u8; 16]);` (the loop binds `enqueue`; a key derived from data, not drawn). Run the swarm property for one base: expect RED from `keys_are_per_job`. Restore and touch.

Record all three in progress.md with the failing line.

- [ ] **Step 7: GUIDE §8**

Add "Calling the outside world in a sim" after "Driving the whole app in a sim". It's a compiled ```rust example in that section's style:
- a `#[job(global, outbound = "mail.example")]` `SendReceipt` whose `call` posts to `/send` with the key header, and whose `settle` marks a `Receipt` row sent;
- a `public` route that enqueues it;
- a workload that registers `Idempotent::header("idempotency-key", Recorder::new())` and builds `let outbound = sim.outbound(&app.outbound_hosts())?;` then `Kernel::with_outbound(app, …)`;
- pushes two requests through `SimSource`, quiesces, runs past the last backoff, and calls the three checkers.

Then a short "Why the key matters" paragraph that names the two planted bugs, what each one's property says, and their recorded seeds. Run `cargo build -p guide-examples` and `cargo test -p guide-examples --no-fail-fast`.

- [ ] **Step 8: Review gate, gate, oracle 3, commit**

Dispatch an `opus` reviewer, with no `name`, writing to `$SDD/review-task12.md` first. Its brief: re-run the swarm property and both pins itself. Ask whether any property can be satisfied by a workload that checks nothing: the `outbound:checked` floor, and the honest twin passing on each pinned seed. Ask whether the `Done ⇔ Paid` and `double charge` checks hold under a crash between the remote's apply and the reply. Resolve its findings. Then run the three gate commands, counted, and oracle 3:

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
perl -e 'alarm 590; exec @ARGV' -- $SDD/oracle3.sh task12   # oracle3 mode foreground; background: Global Constraints
```

```bash
git add crates/charpente-sim/src/checks.rs crates/charpente-sim/src/lib.rs crates/charpente-sim/src/outbound.rs \
  crates/charpente/tests/outbound.rs docs/GUIDE.md
git commit -m "sim: outbound swarm property, three checkers, two planted bugs with recorded seeds 0x… / 0x… (outbound-effects spec §10, §12 oracles 2 and 4)"
```

Put the two real seeds in the message.

---

### Task 13: Mutation run over the outbound code (oracle 6)

**Files:**
- Modify: `crates/charpente/tests/outbound.rs` and `crates/charpente-sim/src/outbound.rs`'s tests, as survivors demand
- Never modify: `.cargo/mutants.toml`. A survivor is a property to write, not an exclusion. `tests/mutants_exclusions.rs` pins the count.

**Interfaces:** none new.

- [ ] **Step 1: Prepare the instrument**

```bash
cd ~/Documents/GitHub/charpente
git status --porcelain | wc -l      # 0 — commit before mutating (a dying run must not take work with it)
unset CARGO_TARGET_DIR
cargo mutants --version             # 27.1.0
cargo clean -p charpente && cargo clean -p charpente-sim
```

- [ ] **Step 2: Validate every `--re` against the listing, function by function**

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
DISPATCH_RE='run_call_into|settle_into|expire_into|fail_outbound|queue_outbound|next_due|abandon_starts|drop_in_flight|outbound_ready|note_enqueued|rebuild|Dispatcher::with_outbound|take_starts|in_flight_len|push_completion|pop_completion|next_outbound_deadline|requeue|run_due'
MOD_RE='start_calls|Ready::Outbound|outbound_in_flight|KernelBuilder|with_outbound|Kernel::step'
print -r -- "$DISPATCH_RE" > $SDD/mutants-dispatch-re.txt; print -r -- "$MOD_RE" > $SDD/mutants-mod-re.txt
cargo mutants --list --file crates/charpente/src/kernel/dispatch.rs --re "$DISPATCH_RE" > $SDD/mutants-dispatch-list.txt
cargo mutants --list --file crates/charpente/src/kernel/mod.rs --re "$MOD_RE" > $SDD/mutants-mod-list.txt
wc -l $SDD/mutants-*-list.txt
```

Read both lists. Check every function name against Tasks 10 and 11's code as it actually landed (`grep -n "fn " crates/charpente/src/kernel/dispatch.rs`), and edit the two pattern files until the lists match: every outbound function must appear, and nothing must be listed only because a pattern matched by accident. `exclude_re` and `--re` are substring matches: anchor with `$` where a name is a prefix of another. A mutant's name reads `<file>:<line>:<col>: replace Kernel::step -> … with …`, so a pattern matches a path like `Kernel::step`, never `fn step`. If `Kernel::step` lists the whole of `step`, that's intended: the drop, drain, push and park all live there.

- [ ] **Step 2a: Estimate each set's wall time before launching any**

Every mutant runs its set's whole test package, so a set's cost is roughly its mutant count times one run of that package. List the two file-only sets, then time one test run per test set in the background (sequentially, so neither run slows the other):

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
cargo mutants --list --file crates/charpente/src/kernel/outbound.rs --file crates/charpente/src/outbound.rs > $SDD/mutants-charpente-outbound-list.txt
cargo mutants --list --file crates/charpente-sim/src/outbound.rs > $SDD/mutants-sim-outbound-list.txt
wc -l $SDD/mutants-*-list.txt
( t0=$(date +%s); cargo test -p charpente --no-fail-fast > $SDD/mutants-timing-charpente.log 2>&1; rc=$?
  print "charpente exit $rc wall $(( $(date +%s) - t0 ))s" >> $SDD/mutants-timing.txt
  t0=$(date +%s); cargo test -p charpente-core -p charpente-sim --no-fail-fast > $SDD/mutants-timing-core-sim.log 2>&1; rc=$?
  print "core+sim exit $rc wall $(( $(date +%s) - t0 ))s" >> $SDD/mutants-timing.txt ) &
```

When `mutants-timing.txt` has both lines (bounded checks), multiply each set's mutant count by its package's wall time. The three charpente sets use `charpente`'s time; the sim set is judged by the default core + sim test set, so it uses that one. Record the four estimates in progress.md. Split any set whose estimate exceeds 12 h into smaller `--re` sets before Step 3, validating each against the listing as Step 2 does.

- [ ] **Step 3: Run the scoped sets**

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
scripts/mutants.sh --file crates/charpente/src/kernel/outbound.rs --file crates/charpente/src/outbound.rs \
  --test-package charpente 2>&1 | tee $SDD/mutants-charpente-outbound.log
scripts/mutants.sh --file crates/charpente/src/kernel/dispatch.rs --re "$(cat $SDD/mutants-dispatch-re.txt)" \
  --test-package charpente 2>&1 | tee $SDD/mutants-dispatch.log
scripts/mutants.sh --file crates/charpente/src/kernel/mod.rs --re "$(cat $SDD/mutants-mod-re.txt)" \
  --test-package charpente 2>&1 | tee $SDD/mutants-mod.log
scripts/mutants.sh --file crates/charpente-sim/src/outbound.rs 2>&1 | tee $SDD/mutants-sim-outbound.log
```

Each runs in the background, since a set can pass 600 s. Poll the process, not the log. Read the verb's summary lines, never cargo-mutants' exit code. The verb exits 0 when every viable mutant was caught. A MISSED is a one-sided property, and a TIMEOUT is not a catch: rerun that file with a larger `--timeout` before judging it.

- [ ] **Step 4: Close every survivor with a property**

For each MISSED, write the test whose failure that mutant would cause. Pin the behaviour the mutant changes, not the function: a test of the edited function can stay green while its callers ignore it. Rerun that file's set until the verb exits 0. Record each survivor, its test and the rerun's counts in progress.md. A mutant believed equivalent is reported to the controller with its argument. It is not excluded on this task's authority.

- [ ] **Step 5: Commit**

```bash
cargo clean -p charpente && cargo clean -p charpente-sim
git add crates/charpente/tests/outbound.rs crates/charpente-sim/src/outbound.rs <any other test file touched>
git commit -m "tests: close the outbound mutation survivors — oracle 6, N caught of N viable across 4 sets"
```

---

### Task 14: The production client — hyper + rustls beside the inbound edge

**Files:**
- Modify: `crates/charpente/Cargo.toml` (`hyper` gains `client`; `hyper-util` gains `client-legacy`, `http1`; new `hyper-rustls`, `rustls`, `webpki-roots`, `tower-service`)
- Modify: `crates/charpente/src/serve.rs` (`ProdOutbound`, `OutboundOptions`, `serve_with`, `serve_replicated`, `backup` and `archive` supplying the client, the credential read, the resolver, the TLS config)
- Modify: `crates/charpente/src/http/app.rs` (`outbound_callers`), `crates/charpente/src/outbound.rs` (delete Task 7's dead-code attribute on `Host::credentials`)
- Modify: `docs/GUIDE.md` §5 "Starting the server" (`serve_with`, the operator flags come in Task 15) and §6 (where credentials are read), `docs/BENCH.md` (the clean-build delta)
- Test: `serve.rs`'s `#[cfg(test)]` module (plain-HTTP loopback; real TLS is Task 16's)

**Interfaces:**
- Consumes: Task 2's trait; Task 7's `Host`, `Credential`; Task 9's `App::outbound_config` and `check_host_sets`; Task 11's `Kernel::builder`.
- Produces (`charpente::serve`):
  - `pub struct OutboundOptions { pub bases: Vec<(String, String)>, pub cas: Vec<std::path::PathBuf> }` (`Default`)
  - `pub fn serve_with(addr: &str, app: App, outbound: OutboundOptions) -> io::Result<Server>`. `serve(addr, app)` becomes `serve_with(addr, app, OutboundOptions::default())`.
  - `pub(crate) struct ProdOutbound` implementing `Outbound`.

- [ ] **Step 1: Add the dependencies and measure what they cost**

First record the baseline clean-build time. Use BENCH.md's compile instrument (`crates/bench-harness`, its `compile` suite; read docs/BENCH.md's method section for the exact command) on the commit before this one, three runs. Then:

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
cargo add -p charpente hyper --features client
cargo add -p charpente hyper-util --features client-legacy,http1
cargo add -p charpente hyper-rustls --no-default-features --features http1,ring,tls12
cargo add -p charpente rustls --no-default-features --features ring,std,tls12
cargo add -p charpente webpki-roots tower-service
cargo tree -p charpente -e features -i aws-lc-rs 2>&1 | head -3   # expect: "package ID specification `aws-lc-rs` did not match any packages"
cargo tree -p charpente -e normal | grep -E "rustls|webpki|hyper-rustls|ring" > $SDD/task14-tree.txt
```

`aws-lc-rs` must be absent: it needs cmake, which is why spec §9 chose `ring`. Record the resolved versions, from `Cargo.lock`, for the commit message.

- [ ] **Step 2: Write the failing tests** (`serve.rs`'s test module)

The test module's imports gain `use crate::outbound::Host;` and `use charpente_core::io::{HttpMethod, IdempotencyKey};`. Neither `super::*` nor the existing test imports bring them in. `OutboundCall`, `OutboundReply` and the `Outbound` trait come through `super::*` from Step 4's imports.

```rust
    /// A one-shot plain-HTTP server on loopback: reads one whole request
    /// (headers, then `content-length` bytes of body, however the client
    /// split its writes), answers `reply`, and hands back the raw request.
    fn one_shot(reply: Vec<u8>) -> (u16, std::thread::JoinHandle<Vec<u8>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            use std::io::{Read, Write};
            let (mut conn, _) = listener.accept().unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let n = conn.read(&mut chunk).unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
                    let len = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length: "))
                        .map_or(0, |v| v.trim().parse::<usize>().unwrap());
                    if buf.len() >= end + 4 + len {
                        break;
                    }
                }
            }
            // The reply-cap test's client stops reading at cap + 1 and drops
            // the connection, so this write can fail by design.
            let _ = conn.write_all(&reply);
            buf
        });
        (port, handle)
    }

    fn wait_for(ob: &ProdOutbound) -> (u64, io::Result<OutboundReply>) {
        for _ in 0..500 {
            if let Some(c) = ob.poll() {
                return c;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("no completion within 5s");
    }

    #[crate::job(global, outbound = "pay.test")]
    struct ServeTestCharge {}

    impl ServeTestCharge {
        fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<crate::outbound::Call>, crate::kernel::JobError> {
            Ok(None)
        }
        fn settle(self, _tx: &mut Tx, _reply: crate::outbound::Reply) -> Result<(), crate::kernel::JobError> {
            Ok(())
        }
    }

    /// An `App` over a fresh temp `ProdDisk`, as `a_kernel_that_refuses_to_start_ends_wait_with_status_1` opens its own.
    fn temp_app(tag: &str) -> App {
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        let root = std::env::temp_dir().join(format!("charpente-serve-{tag}-{:016x}", u64::from_le_bytes(suffix)));
        App::open(
            Box::new(ProdDisk::new(&root).unwrap()),
            Schema::new(),
            Box::new(ProdEntropy::new().unwrap()),
            AppConfig::default(),
        )
        .unwrap()
    }

    fn charge_app(host: Host) -> App {
        temp_app("outbound").mount_jobs(crate::jobs![ServeTestCharge]).outbound(host)
    }

    fn plain_call(path: &str) -> OutboundCall {
        OutboundCall {
            host: "pay.test",
            method: HttpMethod::Get,
            path: path.into(),
            headers: Vec::new(),
            body: Vec::new(),
            key: IdempotencyKey::from_bytes([0; 16]),
        }
    }

    #[test]
    fn prod_sends_the_call_with_its_credential_and_returns_the_reply() {
        let (port, server) = one_shot(b"HTTP/1.1 201 Created\r\ncontent-length: 2\r\nx-id: ch_1\r\n\r\nok".to_vec());
        // Safety of set_var: this test binary's other tests read no PAY_KEY.
        unsafe { std::env::set_var("SERVE_TEST_PAY_KEY", "sk_test_1") };
        let app = charge_app(Host::new("pay.test").bearer_env("SERVE_TEST_PAY_KEY"));
        let opts = OutboundOptions { bases: vec![("pay.test".into(), format!("http://127.0.0.1:{port}"))], cas: vec![] };
        let ob = ProdOutbound::new(&app, &opts).unwrap().expect("the app calls out");
        let key = IdempotencyKey::from_bytes([5; 16]);
        ob.start(7, OutboundCall {
            host: "pay.test",
            method: HttpMethod::Post,
            path: "/v1/charges?x=1".into(),
            headers: vec![("idempotency-key".into(), key.as_str().into())],
            body: b"amount=1".to_vec(),
            key,
        }).unwrap();
        let (token, reply) = wait_for(&ob);
        let reply = reply.unwrap();
        assert_eq!((token, reply.status, reply.body.as_slice()), (7, 201, &b"ok"[..]));
        assert!(reply.headers.iter().any(|(n, v)| n == "x-id" && v == "ch_1"));
        let raw = String::from_utf8(server.join().unwrap()).unwrap();
        assert!(raw.starts_with("POST /v1/charges?x=1 HTTP/1.1\r\n"), "{raw}");
        assert!(raw.contains("authorization: Bearer sk_test_1\r\n"), "{raw}");
        assert!(raw.contains(&format!("idempotency-key: {}\r\n", key.as_str())), "{raw}");
        assert!(raw.to_ascii_lowercase().contains(&format!("host: pay.test:{port}\r\n")), "A11: the declared name: {raw}");
        assert!(raw.ends_with("amount=1"), "{raw}");
    }

    /// Review Focus 4: the edge stops reading at cap + 1, so memory is bounded
    /// whatever the remote sends.
    #[test]
    fn prod_reply_cap_bounds_reading() {
        let mut reply = b"HTTP/1.1 200 OK\r\ncontent-length: 65536\r\n\r\n".to_vec();
        reply.extend(std::iter::repeat_n(b'x', 65536));
        let (port, _server) = one_shot(reply);
        let app = charge_app(Host::new("pay.test").max_reply_bytes(16));
        let opts = OutboundOptions { bases: vec![("pay.test".into(), format!("http://127.0.0.1:{port}"))], cas: vec![] };
        let ob = ProdOutbound::new(&app, &opts).unwrap().unwrap();
        ob.start(0, plain_call("/big")).unwrap();
        let (_, reply) = wait_for(&ob);
        assert_eq!(reply.unwrap().body.len(), 17, "cap + 1, never the 64 KiB sent");
    }

    #[test]
    fn a_closed_port_is_connection_refused() {
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port(); // bound then dropped
        let app = charge_app(Host::new("pay.test"));
        let opts = OutboundOptions { bases: vec![("pay.test".into(), format!("http://127.0.0.1:{port}"))], cas: vec![] };
        let ob = ProdOutbound::new(&app, &opts).unwrap().unwrap();
        ob.start(0, plain_call("/x")).unwrap();
        let (_, reply) = wait_for(&ob);
        assert_eq!(reply.unwrap_err().kind(), io::ErrorKind::ConnectionRefused);
    }

    #[test]
    fn outbound_setup_refusals_name_the_fix() {
        let app = charge_app(Host::new("pay.test").bearer_env("SERVE_TEST_UNSET_VAR"));
        let err = ProdOutbound::new(&app, &OutboundOptions::default()).err().unwrap();
        assert_eq!(err.to_string(), "outbound: SERVE_TEST_UNSET_VAR is unset — host pay.test needs it for job ServeTestCharge");
        assert!(Refusal::of(&err).is_some());

        let app = charge_app(Host::new("pay.test"));
        let bad = |base: &str| {
            let opts = OutboundOptions { bases: vec![("pay.test".into(), base.into())], cas: vec![] };
            ProdOutbound::new(&app, &opts).err().unwrap().to_string()
        };
        assert_eq!(bad("http://10.0.0.5:80"), "--outbound-base pay.test=http://10.0.0.5:80: plain http is accepted only for a loopback address — use https");
        assert_eq!(bad("https://fake.internal:8443"), "--outbound-base pay.test=https://fake.internal:8443: the base must name an IP address or localhost — TLS still verifies pay.test, so only the address moves (GUIDE §5)");
        assert_eq!(bad("https://127.0.0.1:8443/v1"), "--outbound-base pay.test=https://127.0.0.1:8443/v1: a base is a scheme, an address and an optional port — no path");
        let opts = OutboundOptions { bases: vec![("other.test".into(), "https://127.0.0.1:1".into())], cas: vec![] };
        assert_eq!(
            ProdOutbound::new(&app, &opts).err().unwrap().to_string(),
            "--outbound-base names other.test, which no mounted job calls — the hosts this app calls are: pay.test"
        );
        let opts = OutboundOptions { bases: vec![], cas: vec!["/nonexistent/ca.pem".into()] };
        assert!(ProdOutbound::new(&app, &opts).err().unwrap().to_string().starts_with("--outbound-ca /nonexistent/ca.pem: "));
    }

    #[test]
    fn an_app_with_no_outbound_job_gets_no_client() {
        let app = temp_app("plain");
        assert!(ProdOutbound::new(&app, &OutboundOptions::default()).unwrap().is_none(), "invariant 6: no runtime thread");
    }
```

`#[crate::job]` resolves inside the crate through `extern crate self as charpente` (CLAUDE.md's workspace map). These tests leave their few-KiB temp directories under the system temp dir; nothing reads them again.

- [ ] **Step 3: Run them to verify they fail**

Run: `cargo test -p charpente --lib serve::tests`
Expected: FAIL to compile, naming `ProdOutbound`, `OutboundOptions`.

- [ ] **Step 4: Implement**

`serve.rs`'s imports gain `use std::collections::BTreeMap;`, `use std::sync::Arc;`, `use base64ct::Encoding;` (for `Base64::encode_string`, as `auth/oidc.rs` imports it), `use crate::outbound::Credential;`, and `Outbound, OutboundCall, OutboundReply` in its `use charpente_core::io::{…}` line.

```rust
/// Operator settings for the outbound client (outbound-effects spec §9):
/// `--outbound-base <host>=<url>` and `--outbound-ca <path>`, parsed by
/// `launch` (Task 15) or built by hand for `serve_with`.
#[derive(Default, Clone)]
pub struct OutboundOptions {
    pub bases: Vec<(String, String)>,
    pub cas: Vec<std::path::PathBuf>,
}

/// The production `Outbound` (spec §9): hyper + rustls on a current-thread
/// tokio runtime of its own. It makes no decisions (R10): no timers, no
/// retries (`retry_canceled_requests(false)`), no redirects (a 3xx is a reply),
/// no proxy. `start` hands the call to the runtime, completions return on a
/// std channel the kernel polls, and `cancel` aborts the task.
pub(crate) struct ProdOutbound {
    cmds: tokio::sync::mpsc::UnboundedSender<Cmd>,
    done: mpsc::Receiver<(u64, io::Result<OutboundReply>)>,
}

enum Cmd {
    Start(u64, OutboundCall),
    Cancel(u64),
}

/// Where one host is reached, and how it is authenticated. `scheme` is
/// `https` except for a loopback `--outbound-base`; `addr` overrides DNS but
/// never the TLS name (plan amendment A11).
struct Route {
    scheme: &'static str,
    port: Option<u16>,
    addr: Option<std::net::IpAddr>,
    headers: Vec<(String, String)>,
    max_reply: usize,
}

impl ProdOutbound {
    /// `Ok(None)` for an app whose jobs call nothing: no runtime thread exists
    /// (spec §4 invariant 6). Every setup failure is a `Refusal`, so `serve`
    /// reports "refused to start".
    pub(crate) fn new(app: &App, opts: &OutboundOptions) -> io::Result<Option<ProdOutbound>> {
        let called = app.outbound_hosts();
        if called.is_empty() {
            return Ok(None);
        }
        let refuse = |m: String| io::Error::other(Refusal(m));
        // R9 first: this client is built before the kernel, whose build runs
        // the same check, so an unmatched host set is the kernel's own
        // Refusal here too — and the `expect` below holds by construction.
        let callers = app.outbound_callers();
        crate::outbound::check_host_sets(&callers, app.outbound_config()).map_err(refuse)?;
        for (host, _) in &opts.bases {
            if !called.contains(&host.as_str()) {
                return Err(refuse(format!(
                    "--outbound-base names {host}, which no mounted job calls — the hosts this app calls are: {}",
                    called.join(", ")
                )));
            }
        }
        let mut routes = BTreeMap::new();
        for host in app.outbound_config() {
            let job = callers
                .iter()
                .find(|&&(_, h)| h == host.name())
                .map(|&(job, _)| job)
                .expect("check_host_sets above: every Host entry has a mounted job calling it");
            let mut headers = Vec::new();
            for cred in host.credentials() {
                let var = |v: &str| {
                    std::env::var(v).map_err(|_| {
                        refuse(format!("outbound: {v} is unset — host {} needs it for job {job}", host.name()))
                    })
                };
                headers.push(match cred {
                    Credential::Bearer(v) => ("authorization".to_string(), format!("Bearer {}", var(v)?)),
                    Credential::Header(name, v) => (name.clone(), var(v)?),
                    Credential::Basic(u, p) => {
                        let pair = format!("{}:{}", var(u)?, var(p)?);
                        ("authorization".to_string(), format!("Basic {}", base64ct::Base64::encode_string(pair.as_bytes())))
                    }
                });
            }
            let base = opts.bases.iter().find(|(h, _)| h == host.name()).map(|(_, b)| b.as_str());
            let (scheme, addr, port) = match base {
                None => ("https", None, None),
                Some(b) => parse_base(host.name(), b).map_err(refuse)?,
            };
            routes.insert(host.name(), Route { scheme, port, addr, headers, max_reply: host.max_reply() });
        }
        let tls = tls_config(&opts.cas).map_err(refuse)?;
        let (cmds, rx) = tokio::sync::mpsc::unbounded_channel();
        let (done_tx, done) = mpsc::channel();
        std::thread::Builder::new()
            .name("charpente-outbound".into())
            .spawn(move || outbound_runtime(rx, done_tx, routes, tls))?;
        Ok(Some(ProdOutbound { cmds, done }))
    }
}

impl Outbound for ProdOutbound {
    fn start(&self, token: u64, call: OutboundCall) -> io::Result<()> {
        self.cmds
            .send(Cmd::Start(token, call))
            .map_err(|_| io::Error::other("the outbound runtime thread is gone"))
    }
    fn poll(&self) -> Option<(u64, io::Result<OutboundReply>)> {
        self.done.try_recv().ok()
    }
    fn cancel(&self, token: u64) {
        let _ = self.cmds.send(Cmd::Cancel(token));
    }
}
```

`App` gains `outbound_callers`, read before the kernel takes the defs, as `outbound_hosts` is:

```rust
    /// `(job name, host)` for each mounted outbound job, in mount order —
    /// what `check_host_sets` takes.
    pub(crate) fn outbound_callers(&self) -> Vec<(&'static str, &'static str)> {
        self.job_defs
            .iter()
            .filter_map(|d| d.outbound.as_ref().map(|o| (d.name, o.host)))
            .collect()
    }
```

`ProdOutbound::new` is `Host::credentials`' first caller: delete the `expect(dead_code)` Task 7 put on it.

```rust
/// `--outbound-base`'s value: `http(s)://<ip or localhost>[:port]`, nothing more.
fn parse_base(host: &str, base: &str) -> Result<(&'static str, Option<std::net::IpAddr>, Option<u16>), String> {
    let flag = format!("--outbound-base {host}={base}");
    let (scheme, rest) = if let Some(r) = base.strip_prefix("https://") {
        ("https", r)
    } else if let Some(r) = base.strip_prefix("http://") {
        ("http", r)
    } else {
        return Err(format!("{flag}: a base begins with https:// (or http:// on loopback)"));
    };
    if rest.contains('/') {
        return Err(format!("{flag}: a base is a scheme, an address and an optional port — no path"));
    }
    let (name, port) = match rest.rsplit_once(':') {
        Some((n, p)) if !n.ends_with(']') || n.starts_with('[') => {
            (n, Some(p.parse::<u16>().map_err(|_| format!("{flag}: \"{p}\" is not a port"))?))
        }
        _ => (rest, None),
    };
    let name = name.trim_start_matches('[').trim_end_matches(']');
    let ip: std::net::IpAddr = if name == "localhost" {
        std::net::Ipv4Addr::LOCALHOST.into()
    } else {
        name.parse().map_err(|_| {
            format!("{flag}: the base must name an IP address or localhost — TLS still verifies {host}, so only the address moves (GUIDE §5)")
        })?
    };
    if scheme == "http" && !ip.is_loopback() {
        return Err(format!("{flag}: plain http is accepted only for a loopback address — use https"));
    }
    Ok((scheme, Some(ip), port))
}

/// Mozilla's roots (`webpki-roots`), so behaviour does not vary with the OS
/// store, plus each `--outbound-ca`. ALPN is left to hyper-rustls:
/// `HttpsConnectorBuilder::with_tls_config` asserts a config arrives with
/// none set, and the HTTP/1-only connector `enable_http1` builds speaks
/// HTTP/1.1.
fn tls_config(cas: &[std::path::PathBuf]) -> Result<rustls::ClientConfig, String> {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    let mut roots = rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
    for path in cas {
        let flag = format!("--outbound-ca {}", path.display());
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
            .map_err(|e| format!("{flag}: {e}"))?
            .collect::<Result<_, _>>()
            .map_err(|e| format!("{flag}: {e}"))?;
        if certs.is_empty() {
            return Err(format!("{flag}: the file holds no PEM certificate"));
        }
        for cert in certs {
            roots.add(cert).map_err(|e| format!("{flag}: {e}"))?;
        }
    }
    Ok(rustls::ClientConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth())
}
```

The runtime thread. The resolver maps a host with a base `addr` to it, and asks the system resolver otherwise. The request URI always carries the declared host, so SNI and verification use it (A11):

```rust
fn outbound_runtime(
    mut cmds: tokio::sync::mpsc::UnboundedReceiver<Cmd>,
    done: mpsc::Sender<(u64, io::Result<OutboundReply>)>,
    routes: BTreeMap<&'static str, Route>,
    tls: rustls::ClientConfig,
) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the outbound runtime builds");
    let overrides: Arc<BTreeMap<String, std::net::IpAddr>> = Arc::new(
        routes.iter().filter_map(|(h, r)| r.addr.map(|a| (h.to_string(), a))).collect(),
    );
    let routes = Arc::new(routes);
    rt.block_on(async move {
        let mut http = hyper_util::client::legacy::connect::HttpConnector::new_with_resolver(Resolver {
            overrides,
            system: hyper_util::client::legacy::connect::dns::GaiResolver::new(),
        });
        http.enforce_http(false);
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .wrap_connector(http);
        let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .retry_canceled_requests(false)
            .build::<_, http_body_util::Full<Bytes>>(connector);
        let mut tasks: BTreeMap<u64, tokio::task::AbortHandle> = BTreeMap::new();
        while let Some(cmd) = cmds.recv().await {
            tasks.retain(|_, h| !h.is_finished());
            match cmd {
                Cmd::Start(token, call) => {
                    let (client, routes, done) = (client.clone(), routes.clone(), done.clone());
                    let task = tokio::spawn(async move {
                        let result = send(&client, &routes, call).await;
                        let _ = done.send((token, result));
                    });
                    tasks.insert(token, task.abort_handle());
                }
                Cmd::Cancel(token) => {
                    if let Some(h) = tasks.remove(&token) {
                        h.abort();
                    }
                }
            }
        }
    });
}
```

`send` builds `{scheme}://{call.host}{:port if Some}{call.path}`. It adds the call's headers, then the route's credential headers. The kernel already refused any call that sets one of those (Task 7's `check`), so nothing collides. Map the client error: `e.is_connect()` → `io::Error::new(io::ErrorKind::ConnectionRefused, format!("could not connect: {e}"))`, so nothing was sent (A18). Anything else → `io::Error::other(format!("the connection failed after the request was sent: {e}"))`. Read the body frame by frame with `http_body_util::BodyExt::frame`, and stop once it holds more than `max_reply` bytes, truncated to `max_reply + 1`. A frame error after headers → `io::Error::other(format!("the reply broke off after {n} bytes: {e}"))`. Header values are taken with `String::from_utf8_lossy`.

The resolver. `HttpConnector<R>: Service<Uri>` needs `R: Resolve + Clone + Send + Sync + 'static` and `R::Future: Send`, and `Client` needs its connector `Clone`, so `Resolver` derives `Clone` and boxes a `Send` future:

```rust
/// `--outbound-base`'s address override in front of the system resolver
/// (plan amendment A11): only where a name resolves moves, never the name.
#[derive(Clone)]
struct Resolver {
    overrides: Arc<BTreeMap<String, std::net::IpAddr>>,
    system: hyper_util::client::legacy::connect::dns::GaiResolver,
}

impl tower_service::Service<hyper_util::client::legacy::connect::dns::Name> for Resolver {
    type Response = std::vec::IntoIter<SocketAddr>;
    type Error = io::Error;
    type Future = Pin<Box<dyn std::future::Future<Output = io::Result<std::vec::IntoIter<SocketAddr>>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: hyper_util::client::legacy::connect::dns::Name) -> Self::Future {
        // An override answers port 0; the connector applies the URI's port.
        if let Some(ip) = self.overrides.get(name.as_str()) {
            let addrs = vec![SocketAddr::new(*ip, 0)];
            return Box::pin(async move { Ok(addrs.into_iter()) });
        }
        // `GaiResolver::poll_ready` is always ready, so it is not polled first.
        let lookup = tower_service::Service::call(&mut self.system, name);
        Box::pin(async move { Ok(lookup.await?.collect::<Vec<_>>().into_iter()) })
    }
}
```

`serve_with`: build `ProdOutbound::new(&app, &opts)?` before `bind_http`, so a refusal leaves nothing running. In the kernel thread, use `Kernel::builder(app, clock)`, plus `.outbound(Box::new(ob))` when `Some`, then `.build()`. `serve` delegates with defaults. `serve_replicated` does the same with `OutboundOptions::default()` and `.replication(repl)`: a replicated primary runs jobs too, and without the client its kernel would refuse to build. Update `serve`'s doc: "`Err` when the outbound setup refuses (a `Refusal`, nothing running) or the HTTP listener cannot be bound."

`backup` and `archive` build kernels too, and after Task 11 an app with an outbound job refuses to start on either without a client (a backup is promoted into a primary, and an archive node runs jobs and cron). Each builds `let outbound = ProdOutbound::new(&app, &OutboundOptions::default())?;` first, then goes through the builder instead of `Kernel::with_replication`/`Kernel::with_archive`. `backup`:

```rust
    let mut builder =
        kernel::Kernel::builder(app, Box::new(crate::prod::ProdClock::new())).replication(repl);
    if let Some(ob) = outbound {
        builder = builder.outbound(Box::new(ob));
    }
    let mut kernel = builder.build()?;
```

`archive` is the same with `.archive(archive, store)` in place of `.replication(repl)`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p charpente --lib serve::tests` → PASS. Then `cargo test -p charpente --no-fail-fast` → every suite ok. Then `cargo clippy --workspace --all-targets -- -D warnings`: the deny-list must need no new allowance, since `serve.rs`'s file-level one covers this.

- [ ] **Step 6: BENCH.md's compile-time reading**

Re-run the compile instrument three times on this commit's tree. Record both medians, the delta and the method in BENCH.md, in the same form as its existing compile-time entries, conditioned the same way (machine, cold target dir, `cargo build -p <the instrument's target>`). It's a measurement, never an estimate, and it is not hidden behind a feature (spec §9).

- [ ] **Step 7: GUIDE**

- §5 "Starting the server" gains `serve_with` and `OutboundOptions`.
- §6 states that `serve` reads each credential variable once at start, and that an unset one refuses the start with the exact text.
- §6 states that hosts resolve to `https://<host>:443` against Mozilla's roots.
- §6 states that no redirect is followed, so a 3xx is a reply.

Run `cargo build -p guide-examples`.

- [ ] **Step 8: Gate and commit**

Run the three gate commands, counted.

```bash
git add crates/charpente/Cargo.toml Cargo.lock crates/charpente/src/serve.rs crates/charpente/src/http/app.rs \
  crates/charpente/src/outbound.rs docs/GUIDE.md docs/BENCH.md
git commit -m "serve: the production outbound client — hyper + rustls (ring) + webpki-roots, no decisions at the edge; clean build +N.Ns (BENCH.md)

Dependencies (spec §9): hyper client, hyper-util client-legacy/http1 (both already present),
hyper-rustls <v>, rustls <v> (ring provider: no cmake), webpki-roots <v>, tower-service <v>
(a dependency of hyper-util's client feature; named directly for the resolver). Edge-only; the deny-list
is unchanged. Rejected: reqwest (its own redirect/retry/timeout policy), native-tls (varies by OS)."
```

Fill in the versions and the measured delta.

---

### Task 15: The operator flags and the `outbound-fixture` crate

**Files:**
- Modify: `crates/charpente/src/launch.rs` (`LAUNCH_USAGE`; the serve arm parses `--outbound-base`/`--outbound-ca`; a `Refusal` from `serve_with` prints "refused to start")
- Create: `crates/outbound-fixture/{Cargo.toml, src/lib.rs, src/bin/outbound_node.rs, src/bin/fake_remote.rs, tests/sim.rs, tls/ca.pem, tls/ca.key, tls/server.pem, tls/server.key, tls/README.md}`
- Modify: root `Cargo.toml` (`members`), `CLAUDE.md` (the workspace map and a new `outbound-fixture: sim` test-list entry; `charpente`'s `outbound` landed in Task 8, and `edge-harness` gains `outbound` in Task 16), `docs/GUIDE.md` §5 and §13 (the flags, `LAUNCH_USAGE` verbatim)

**Interfaces:**
- Consumes: Task 14's `serve_with`, `OutboundOptions`.
- Produces: `outbound_fixture::build(disk, entropy) -> io::Result<App>`. The bin `outbound_node` reads `OUTBOUND_NODE_ADDR` and `OUTBOUND_NODE_DIR` and passes its argv to `launch`. The bin `fake_remote --tls-port N --admin-port M [--hold-ms MS]` prints `ready` on stdout when listening.

- [ ] **Step 1: Write the failing launch tests** (`launch.rs`'s test module, beside the existing `dispatch` tests)

```rust
    /// The serve arm's flags refuse before any disk is opened: every call
    /// here passes `MISSING_DATA_DIR`, so a parse that ran after
    /// `ProdDisk::new` would exit 1 with "charpente failed to start", not 2.
    #[test]
    fn serve_flags_must_come_in_pairs_and_be_known() {
        let run = |args: &[&str]| {
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
            let code = dispatch(&args, "127.0.0.1:0", MISSING_DATA_DIR, tiny_app, &mut out, &mut err).unwrap();
            assert!(out.is_empty(), "a refused flag prints nothing to stdout");
            (code, String::from_utf8(err).unwrap())
        };
        assert_eq!(
            run(&["--outbound-base"]),
            (2, format!("error: --outbound-base needs a value, HOST=URL\n\n{LAUNCH_USAGE}\n"))
        );
        assert_eq!(
            run(&["--outbound-base", "pay.test"]),
            (2, format!("error: --outbound-base takes HOST=URL — got 'pay.test'\n\n{LAUNCH_USAGE}\n"))
        );
        assert_eq!(
            run(&["--outbound-ca"]),
            (2, format!("error: --outbound-ca needs a value, PATH\n\n{LAUNCH_USAGE}\n"))
        );
        assert_eq!(
            run(&["--outbound-proxy", "x"]),
            (2, format!("error: unknown flag '--outbound-proxy'\n\n{LAUNCH_USAGE}\n"))
        );
    }

    /// The parsed flags reach `serve_with`: a base naming a host no job
    /// calls is `ProdOutbound::new`'s refusal (Task 14), printed as one.
    #[test]
    fn a_refused_outbound_setup_is_a_refused_start() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let dir = std::env::temp_dir().join(format!("charpente-launch-outbound-{}", std::process::id()));
        let args: Vec<String> = ["--outbound-base", "nobody.test=https://127.0.0.1:1"].iter().map(|a| a.to_string()).collect();
        let code = dispatch(&args, "127.0.0.1:0", dir.to_str().unwrap(), outbound_app, &mut out, &mut err).unwrap();
        assert_eq!(code, 1);
        assert_eq!(
            String::from_utf8(err).unwrap(),
            "charpente refused to start: --outbound-base names nobody.test, which no mounted job calls — \
             the hosts this app calls are: pay.test\n"
        );
        let _ = std::fs::remove_dir_all(dir);
    }
```

`outbound_app` is `tiny_app`'s shape plus one `#[crate::job(global, outbound = "pay.test")]` job, `LaunchTestCharge`, whose `call` returns `Ok(None)` and whose `settle` returns `Ok(())`, mounted with `.outbound(Host::new("pay.test"))`. Write it beside `tiny_app`. The three existing tests that pin the usage text literally (`launch.rs:866`, `:893`, `:925`) change with it in Step 2.

- [ ] **Step 2: Implement the flags**

The new usage text keeps today's first two lines and gives the serve form its own line, which takes over the "no verb serves the app" suffix:

```rust
pub const LAUNCH_USAGE: &str = "usage: <app> [describe [--format json|human]]\n       \
       <app> restore --from BUCKET --prefix PREFIX [--to DIR] [--force]\n       \
       <app> [--outbound-base HOST=URL]... [--outbound-ca PATH]...  — no verb serves the app";
```

In `dispatch`, the serve arm runs for no args, or when the first arg begins with `--outbound-`. It parses `--outbound-base HOST=URL` and `--outbound-ca PATH` pairs, each repeatable, into `OutboundOptions`, refusing with Step 1's four texts (exit 2). A value that is split at its first `=` and leaves either side empty is the `takes HOST=URL` refusal. The parse runs before `ProdDisk::new`. The serve call becomes `crate::serve::serve_with(addr, app, opts)`, and its `Err` arm checks `Refusal::of(&e)` first, printing `charpente refused to start: {e}` and exiting 1. Otherwise it keeps today's bind message. Update the comment above that call: `serve_with` can now refuse before binding, as well as fail the bind.

Then update every literal copy of the old text:
- the three `dispatch` tests at `launch.rs:866`, `:893` and `:925`: their expected strings become `format!("error: …\n\n{LAUNCH_USAGE}\n")`, matching the house form Step 1 uses;
- GUIDE §13's `<!-- pinned -->` block (`docs/GUIDE.md:5428-5431`), which becomes the new three lines verbatim. Two gates hold it: `launch.rs`'s `GUIDE.contains(LAUNCH_USAGE)` test and `guide-examples`' `doc_truth`, so run `cargo test -p guide-examples --test doc_truth` too.

Confirm with `git grep -n "no verb serves the app" -- ':!docs/superpowers'`: it must list only `launch.rs`'s constant and the GUIDE block. GUIDE §5 gains the two flags: A11's "only the address moves", plain `http` only on loopback, and an IP or `localhost` only.

Run: `cargo test -p charpente --lib launch` → PASS, including every existing `LAUNCH_USAGE` test.

- [ ] **Step 3: The test CA** (`crates/outbound-fixture/tls/`)

```bash
cd ~/Documents/GitHub/charpente
mkdir -p crates/outbound-fixture/tls && cd crates/outbound-fixture/tls   # Step 4 creates the rest of the crate
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.pem -days 36500 \
  -subj "/CN=charpente outbound-fixture test CA" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign"
openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=pay.fixture"
printf "subjectAltName=DNS:pay.fixture\nbasicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n" > ext.cnf
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out server.pem -days 36500 -extfile ext.cnf
rm server.csr ext.cnf ca.srl
openssl verify -CAfile ca.pem server.pem      # expect: server.pem: OK
openssl x509 -in server.pem -noout -text | grep -A1 "Subject Alternative Name"   # expect: DNS:pay.fixture
```

`tls/README.md` says: test-only, a private key committed on purpose, trusted nowhere except where a test passes `--outbound-ca` with it, and these regeneration commands verbatim.

- [ ] **Step 4: The fixture crate**

`Cargo.toml`:

```toml
[package]
name = "outbound-fixture"
version.workspace = true
edition.workspace = true
license.workspace = true
publish = false

[dependencies]
charpente.workspace = true
# The routes' `Wire` request/response types derive serde, as invoicing's do.
serde = { version = "1", features = ["derive"] }
# fake_remote only: a TLS server outside the deterministic boundary.
tokio = { version = "1", features = ["rt", "net", "io-util", "macros"] }
hyper = { version = "1", features = ["http1", "server"] }
hyper-util = { version = "0.1", features = ["tokio"] }
http-body-util = "0.1"
rustls = { version = "0.23", default-features = false, features = ["ring", "std", "tls12"] }
tokio-rustls = { version = "0.26", default-features = false, features = ["ring", "tls12"] }

[dev-dependencies]
charpente-sim = { path = "../charpente-sim" }

[lints]
workspace = true

# `sim_tests!` generates its own `fn main` (as sim-fixture's and invoicing's do).
[[test]]
name = "sim"
harness = false
```

Use the `rustls` version Task 14 resolved, so the tree carries one rustls. Check with `cargo tree -d | grep rustls`, which must print nothing.

`src/lib.rs`: one app the edge harness drives.

```rust
//! The app the edge harness drives over real TLS (outbound-effects spec §12
//! oracle 7, plan amendment A13): an order is created by a route, charged by
//! an outbound job, and marked paid by `settle`. Kept out of sim-fixture and
//! invoicing so their recorded batches and goldens never move (oracle 3).

use std::io;

use charpente::core::io::{Disk, Entropy};
use charpente::http::{ApiError, App, AppConfig};
use charpente::kernel::JobError;
use charpente::outbound::{Call, Host, IdempotencyKey, Reply};
use charpente::{Id, Schema, Tx, View, Wire, job, jobs, route, routes, table};
use serde::{Deserialize, Serialize};

#[table(global)]
pub struct Order {
    pub id: Id<Order>,
    pub paid: bool,
    pub charge: String,
}

#[job(global, outbound = "pay.fixture", retry(max = 8, backoff = "500ms"), timeout = "2s")]
pub struct ChargeOrder {
    pub order: Id<Order>,
}

impl ChargeOrder {
    fn call(&self, tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        let Some(order) = tx.get(self.order)? else {
            return Err(JobError::app("order gone"));
        };
        if order.paid {
            return Ok(None);
        }
        Ok(Some(
            Call::post("/charge")
                .header("idempotency-key", key.as_str())
                .form(&[("order", self.order.raw().to_string())]),
        ))
    }

    fn settle(self, tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        if reply.status() != 200 {
            return Err(JobError::app(format!("pay.fixture answered {}", reply.status())));
        }
        let charge = String::from_utf8_lossy(reply.body()).into_owned();
        tx.put(&Order { id: self.order, paid: true, charge });
        Ok(())
    }
}
```

Then:
- `POST /orders` (`public`) puts an `Order { paid: false }` with an id from the ids the platform hands a route (GUIDE §2 "Generating ids"), enqueues `ChargeOrder`, and returns the id.
- `GET /orders/:id` (`public`) returns `{ "paid": bool, "charge": String }`.

Write both in GUIDE §5's route form, with `Wire` request and response types as `examples/invoicing` does.

```rust
pub fn build(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> io::Result<App> {
    Ok(App::open(disk, Schema::new().table::<Order>(), entropy, AppConfig::default())?
        .mount(routes![create_order, get_order])
        .mount_jobs(jobs![ChargeOrder])
        .outbound(Host::new("pay.fixture").bearer_env("FIXTURE_PAY_KEY")))
}
```

`src/bin/outbound_node.rs`:

```rust
//! `charpente::launch` over the fixture app. The address and data directory
//! come from the environment so the edge harness can run several; argv is
//! launch's own (the serve verb's `--outbound-*` flags).

fn main() -> std::io::Result<()> {
    let addr = std::env::var("OUTBOUND_NODE_ADDR").unwrap_or_else(|_| "127.0.0.1:8090".into());
    let dir = std::env::var("OUTBOUND_NODE_DIR").unwrap_or_else(|_| "./data".into());
    charpente::launch(&addr, &dir, outbound_fixture::build)
}
```

`outbound_node.rs` carries no lint allow: `std::env` is not on the deny-list.

`src/bin/fake_remote.rs` is a TLS server with `tls/server.pem` + `tls/server.key`, located from `env!("CARGO_MANIFEST_DIR")`, serving HTTP/1.1. It opens with `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]` and a rationale comment: a test server outside the deterministic boundary, whose `tokio::net` listeners and `std::fs` key reads are the point, as in the edge harness's files and `serve.rs`.
- `POST /charge` requires `authorization: Bearer test-key`; anything else answers 401.
- It dedupes by the `idempotency-key` header. The first time a key arrives it mints `ch_<n>` and counts an application. A repeat returns the same body, at once.
- A request with no `idempotency-key` header is applied every time (a fresh `ch_<n>` each) and counted under the bucket `<none>`.
- It counts every request seen, by key (`<none>` for a keyless one).
- With `--hold-ms MS`, only a key's FIRST application waits that long before answering: that's the "in flight" window `kill -9` aims at. A repeat of a key and a keyless request are never held, so a re-call after a restart answers inside the job's 2 s timeout.
- A plain-HTTP admin listener on `--admin-port` answers `GET /stats` with plain text, one counter per line, sorted by key: `requests <key> <n>` lines, then `applied <key> <n>` lines. The edge harness has no dependencies, so it splits lines rather than parse JSON.
- It prints `ready` once both listeners are bound.

`tests/sim.rs` dogfoods `charpente test --sim --dir crates/outbound-fixture`. It holds one `sim_tests!` workload, `orders_charge_once require ["outbound:checked"]`:
- it registers `Idempotent::header("idempotency-key", from_fn(|_| OutboundReply { status: 200, headers: vec![], body: b"ch_1".to_vec() }))` on `pay.fixture`;
- it pushes three `POST /orders` through `SimSource`, then quiesces;
- it runs past the last backoff;
- it asserts the three checkers and that every `Order` is paid exactly when its job is Done, then covers `outbound:checked`.

It follows `crates/sim-fixture/tests/sim.rs`'s shape and `BOOT_TRIES` handling.

- [ ] **Step 5: Wire the workspace and its inventory**

Root `Cargo.toml` `members` gains `"crates/outbound-fixture"`. In CLAUDE.md:
- the workspace map gains `` `crates/outbound-fixture` — the app outbound effects are proven against: a launch-driven node binary and a TLS fake remote the edge harness drives (its own crate so sim-fixture's and invoicing's recorded batches never move) ``;
- the test lists gain `outbound-fixture: sim` (charpente's `outbound` landed in Task 8).

The `doc_inventory` gate enforces both lists, so run `cargo test -p guide-examples --test doc_inventory` and expect PASS.

- [ ] **Step 6: Run everything this task touched**

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
cargo build -p outbound-fixture --bins
cargo test -p outbound-fixture --no-fail-fast 2>&1 | tee $SDD/task15-fixture.log; print "EXIT=${pipestatus[1]}"
grep -c "error: test failed" $SDD/task15-fixture.log    # 0
grep -c "^test result: ok" $SDD/task15-fixture.log      # 4: the lib, the two bins, the doc-tests
cargo run -p charpente-cli -- test --sim --dir crates/outbound-fixture --universes 64 2>&1 | tee $SDD/task15-cli-sim.log | tail -5; print "CLI_EXIT=${pipestatus[1]}"
cargo test -p charpente --lib launch --no-fail-fast
cargo test -p guide-examples --no-fail-fast 2>&1 | tee $SDD/task15-guide.log; grep -c "^test result: ok" $SDD/task15-guide.log
```

Expected: the fixture builds; `EXIT=0` and 0 `error: test failed`; `charpente test --sim` exits 0 over 64 universes (`CLI_EXIT=0`); and `launch` and `guide-examples` stay green. The `sim` target is `harness = false`: it prints its own report and never a `test result:` line, so a red sim shows only as a non-zero exit and cargo's `error: test failed, to rerun pass --test sim`. Never judge it by the `test result:` count.

- [ ] **Step 7: Gate and commit**

Run the three gate commands, counted.

```bash
git add Cargo.toml Cargo.lock CLAUDE.md docs/GUIDE.md crates/charpente/src/launch.rs crates/outbound-fixture/
git commit -m "launch: --outbound-base/--outbound-ca; outbound-fixture — the launch-driven node and TLS fake remote oracle 7 drives (plan amendment A13)"
```

---

### Task 16: The edge-harness scenario (oracle 7)

**Files:**
- Create: `crates/edge-harness/tests/outbound.rs`
- Modify: `crates/edge-harness/src/child.rs` (`Bins` gains `outbound_node`, `fake_remote`; `build_bins` builds `-p outbound-fixture --bins` in its own invocation; `ChildNode::spawn_env` for a launch-driven binary with env and an explicit wait address)
- Modify: `CLAUDE.md` (edge-harness's test list gains `outbound`)

**Interfaces:**
- Consumes: Task 15's binaries and certificates.
- Produces: `ChildNode::spawn_env(bin: &Path, args: &[String], env: &[(&str, String)], wait_addr: &str, scenario: &mut Scenario) -> io::Result<ChildNode>`. It keeps `args` and `env` for `restart`, as `spawn` keeps its args.

- [ ] **Step 1: `spawn_env` and the bins**

Read `ChildNode::spawn` and `restart`, then add `spawn_env`. It's the same body with `.envs(env)` on the `Command`, waiting on `wait_addr` instead of the `--addr` it parses from args. The env is stored beside the args, so `restart` reapplies both. `build_bins` gains:

```rust
    // Its own invocation, like the example build above: resolving the fixture's
    // TLS dependencies in the invoicing build could unify features into the
    // bins under test.
    bins.extend(cargo_build_release(&root, &["-p", "outbound-fixture", "--bins"])?);
    let outbound_node = locate(&bins, "outbound_node")?;
    let fake_remote = locate(&bins, "fake_remote")?;
```

- [ ] **Step 2: Write the scenarios** (all `#[ignore]`, like every scenario in this crate)

```rust
//! Oracle 7 (outbound-effects spec §12): the stock launch-driven binary
//! against a local fake HTTPS remote on a local CA, over real TCP, under
//! `kill -9` and TCP-proxy faults. The fake dedupes by idempotency key, so
//! "applied once" is measured where the money would move.
//!
//! Outside DESIGN §1's determinism fence, as this whole crate is: real
//! `TcpStream`, `Command`, `std::fs` and `thread::sleep` calls are the point,
//! not a violation; the allow below matches `child.rs`/`scenario.rs`/`proxy.rs`'s.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use edge_harness::child::{ChildNode, build_bins};
use edge_harness::client::Http;
use edge_harness::proxy::TcpProxy;
use edge_harness::scenario::Scenario;

const CA: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../outbound-fixture/tls/ca.pem");
```

The scenarios, each a `#[test] #[ignore]`, each opening its own `Scenario`. Each starts `fake_remote` on two free ports through `spawn_env`, with its admin address as `wait_addr`: `spawn_env` waits on a port, not on stdout's `ready`. Each starts `outbound_node` through `spawn_env` with `OUTBOUND_NODE_ADDR`, `OUTBOUND_NODE_DIR`, `FIXTURE_PAY_KEY=test-key` and argv `--outbound-base pay.fixture=https://127.0.0.1:<tls or proxy port> --outbound-ca <CA>`. `TcpProxy::drop_all`, `partition` and `delay_ms` each take `&mut Scenario` as their last argument (they record into it). `/stats` is Task 15's plain lines, `requests <key> <n>` and `applied <key> <n>`, read by splitting each line on whitespace.
1. `an_order_is_charged_once_over_real_tls`: `POST /orders`, then poll `GET /orders/<id>` until `paid` (bounded at 30 s). Assert `/stats` shows that order's key applied 1 and requested 1.
2. `kill_9_with_a_call_in_flight_re_calls_with_the_same_key`: the fake runs with `--hold-ms 3000`. Poll `/stats` until one request is seen, then `kill9` the node and `restart` it. Poll until paid. Assert applied 1 and requested ≥ 2 for one key, and exactly one key in `/stats`.
3. `a_reset_between_node_and_remote_is_retried_with_the_same_key`: the base points through a `TcpProxy` to the fake's TLS port, and the fake holds 1000 ms. Once a request is seen, `proxy.drop_all`. Poll until paid. Assert one key, applied 1.
4. `a_stalled_remote_times_out_and_is_retried`: the base goes through the proxy, and the fake holds 1000 ms. A stall, not a partition: `partition` shuts each new connection at once, which is a connect error and so the refusal path, never a deadline. Once `/stats` shows the first request, call `proxy.delay_ms(3000, &mut scenario)`. Every chunk now crawls: the held reply misses the 2 s deadline, and a re-call's handshake stalls the same way. After 5 s, `proxy.delay_ms(0, &mut scenario)`. Poll until paid. Assert one key, with requests ≥ 2 and applied 1 for it: a deadline fired, and the re-call carried the same key.
5. `an_unset_credential_refuses_the_start`: spawn by hand without `FIXTURE_PAY_KEY`, with `.env_remove("FIXTURE_PAY_KEY")` on the `Command` so an inherited value cannot mask the refusal, as `smoke.rs`'s launch-refusal test does. Expect exit 1 and stderr containing `charpente refused to start: outbound: FIXTURE_PAY_KEY is unset — host pay.fixture needs it for job ChargeOrder`.
6. `without_the_test_ca_no_request_reaches_the_remote`: omit `--outbound-ca`. `POST /orders`, wait 5 s. Assert `/stats` shows zero requests and the order unpaid, so TLS verification refused the fake.

Write each with the crate's helpers and its existing poll-with-deadline idiom. Every assertion message must name the scenario's artifacts directory (`Scenario`'s path), as the existing scenarios do.

- [ ] **Step 3: Run them**

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
cargo test -p edge-harness --test outbound -- --ignored --test-threads=1 2>&1 | tee $SDD/task16-edge.log; grep -E "^test |test result" $SDD/task16-edge.log
./scripts/edge-harness.sh 2>&1 | tee $SDD/task16-edge-all.log; grep -c "^test result: ok" $SDD/task16-edge-all.log
```

Run in the background (release builds and real timeouts), then poll. Expected: six passed, and the whole harness green. The nightly job runs `scripts/edge-harness.sh`, which runs every `--ignored` test in the crate, so the new file joins it with no workflow change. Confirm by reading `.github/workflows/edge-harness.yml`, which must still shell out to the script.

- [ ] **Step 4: Prove the key is what makes it "applied once"**

On a scratch copy (`git stash` is not used; edit, run, then `git checkout -- crates/outbound-fixture/src/lib.rs` and `touch` it), delete the `.header("idempotency-key", …)` line from `ChargeOrder::call` and rebuild the bins. Run scenario 2. Expected: RED, with the `<none>` bucket's `applied` ≥ 2 at the fake (Task 15's fake applies a keyless request every time). Restore it. Record in progress.md.

- [ ] **Step 5: Inventory, gate, commit**

CLAUDE.md's edge-harness test list gains `outbound`; run `cargo test -p guide-examples --test doc_inventory`. Then run the three gate commands, counted.

```bash
git add crates/edge-harness/src/child.rs crates/edge-harness/tests/outbound.rs CLAUDE.md
git commit -m "edge-harness: outbound over real TLS — kill -9 mid-call, reset, stall, unset credential, untrusted CA (outbound-effects spec §12 oracle 7)"
```

---

### Task 17: The docs sweep — every §13 row not already landed

Tasks 2–16 landed their own docs with their code: DESIGN §1, GUIDE §5/§6/§8/§9/§13, CLAUDE.md's trait list and test lists, `db.rs:1456`, and BENCH.md. This task lands the rest. Nothing here changes code.

**Files:**
- Modify: `README.md` (the status section; the "not built" list at `:156-157`)
- Modify: `DESIGN.md` §5 (the loss window covers external effects), §6 (`:1675-1677`'s "M1's external-effect handlers" sentence), §7 (`describe`'s two new fields), `:2054` (the JWKS refresh line)
- Modify: `docs/GUIDE.md` §10 (promotion: the loss-window case) and §15 (a restore re-fires with the same key)
- Modify: `DECISIONS.md` (new §14, spec §17's text verbatim, dated the day it lands)
- Modify: `PLAN.md` (this sub-project recorded in the after-launch section; five v1.x rows from spec §15 and one from amendment A19)
- Modify: `NEIGHBORS.md` (the durable-execution lineage)

- [ ] **Step 1: Write each change**

- **README.**
  - The status section gains one paragraph after the TypeScript-client sentence, in that section's register: outbound effects have shipped. A job declared `outbound = "<host>"` decides an HTTP request after commit in a read-only `call`. The kernel makes it outside any transaction with a platform-minted idempotency key, and `settle` commits the reply's consequences with the Done mark as one record. The simulator injects the outside world's faults: refused, lost request, lost reply, throttled, slow (GUIDE §6, §8).
  - The "not built" sentence becomes: "no blob store, no WebSockets, and on the outbound side no `HTTP/2`, proxies, request signing (AWS `SigV4`) or streaming replies." The backticks are load-bearing: `claims_census` reads every unblanked digit as a figure needing provenance, and blanks only backticks and a few reference forms (`is_ref` covers `§` and `M`), so a bare `HTTP/2` or `SigV4` turns it red.
  - `:68-71` stays as written; it is now true.
- **DESIGN §5.** The loss-window paragraph gains spec §8's first paragraph, adapted to DESIGN's voice: a dead primary can call out from a record it acked but never shipped; the minted key bounds the rest.
- **DESIGN §6.** Replace the "M1's external-effect handlers" sentence with three sentences naming `call`/`settle`, the minted key and the `Outbound` trait, pointing at GUIDE §6.
- **DESIGN §7.** Add `describe`'s `outbound` job field and `outbound_hosts`.
- **DESIGN `:2054`.** "A JWKS refresh is now expressible as a global outbound cron job; the platform still ships none."
- **GUIDE §10.** Promotion and switchover gains "Calls in flight at a failover": the in-flight table is dropped, the new primary re-calls with the same key, and a call made from a record the old primary acked but never shipped is outside the surviving timeline (spec §8).
- **GUIDE §15.** One paragraph: a restored Pending outbound job re-fires with its recorded key, so a remote that dedupes sees a repeat, not a new request.
- **DECISIONS §14.** Spec §17's proposed text, verbatim, under `## 14. External effects are kernel-owned two-phase jobs (<date it lands>)`.
- **PLAN.** In the after-launch section, record outbound effects as shipped (date, spec path, plan path). The section's heading (`PLAN.md:271`) lists each row's source: after "two from M3's tenancy completion" it gains ", six from outbound effects". Add six v1.x rows, each with the trigger "the first app that needs it": outbound with `cross_tenant`, HTTP/2, outbound proxies, request signing (AWS SigV4, which SES needs), streaming replies, and a two-node switchover under outbound load (plan amendment A19; its trigger is the first replicated app with outbound jobs).
- **NEIGHBORS.** A "Durable execution" paragraph:
  - Temporal's activities are the shape `call`/`settle` takes, with the transaction as the workflow;
  - Restate journals the same idea per handler;
  - DBOS keeps workflow state in Postgres beside the app's;
  - what charpente does differently: one log, a sim that drives the remote, and keys minted at staging rather than derived.

- [ ] **Step 2: Run the doc gates**

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
cargo build -p guide-examples
cargo test -p guide-examples --no-fail-fast 2>&1 | tee $SDD/gate-task-17.log; grep -c "^test result: ok" $SDD/gate-task-17.log; grep -c "^test result: FAILED" $SDD/gate-task-17.log
```

Expected: every suite ok, 0 FAILED. That includes `claims_census` (README and MANIFESTO claims), `doc_truth` (pinned blocks), `doc_inventory` and `describe_contract`.

- [ ] **Step 3: Commit**

```bash
git add README.md DESIGN.md docs/GUIDE.md DECISIONS.md PLAN.md NEIGHBORS.md
git commit -m "docs: outbound effects across README, DESIGN §5–§7, GUIDE §10/§15, DECISIONS §14, PLAN v1.x rows, NEIGHBORS (outbound-effects spec §13)"
```

---

### Task 18: The evidence — 10,000 universes, oracle 3, the full gate, CI, the ledger

**Files:**
- Modify: `docs/LEDGER.md`

- [ ] **Step 1: Draw fresh master seeds and record them before running anything**

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
# BSD `od -An` ends its output with two newlines: strip them all, then end each seed with one.
for i in $(seq 1 40); do od -An -N8 -tx8 /dev/urandom | tr -d ' \n'; print; done > $SDD/task18-bases.txt
{ od -An -N8 -tx8 /dev/urandom | tr -d ' \n'; print } > $SDD/task18-fixture-master.txt
grep -c . $SDD/task18-bases.txt   # 40
```

They are fresh, stated in the commit, and never chosen after seeing results.

- [ ] **Step 2: The platform property over 40 × 256 = 10,240 universes** (in the background)

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
( for base in $(cat $SDD/task18-bases.txt); do
    CHARPENTE_BASE_SEED=0x$base cargo test -p charpente --test outbound outbound_jobs_are_exactly_once -- --nocapture \
      > $SDD/task18-swarm-$base.log 2>&1
    print "$base exit $?" >> $SDD/task18-swarm-exits.txt
  done ) &
print $! > $SDD/task18-swarm.pid
```

Poll with bounded checks, one `kill -0 $(cat $SDD/task18-swarm.pid) 2>/dev/null || print exited` per call (a foreground `sleep` loop is blocked in this harness). Then:

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
grep -c " exit 0$" $SDD/task18-swarm-exits.txt          # expect 40
grep -l "test result: ok. 1 passed" $SDD/task18-swarm-*.log | wc -l   # expect 40
```

A red base prints its seed and repro line. That's a finding: replay it (`CHARPENTE_SEED=…`), fix it test-first with the seed pinned in the regression suite (hard rule 3), and rerun all 40.

- [ ] **Step 3: The fixture app over 10,000 universes from one fresh master seed** (in the background)

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
cargo build -p charpente-cli   # the binary below must exist and be this tree's
( ./target/debug/charpente test --sim --dir crates/outbound-fixture --universes 10000 \
    --master-seed 0x$(cat $SDD/task18-fixture-master.txt) --format json \
    > $SDD/task18-fixture.json 2> $SDD/task18-fixture.err; print "EXIT=$?" >> $SDD/task18-fixture.err ) &
```

When it exits: `jq '{universes, failures: (.failures | length), verdicts, gave_up}' $SDD/task18-fixture.json` → `universes: 10000`, `failures: 0`. Then run it a second time to `task18-fixture-2.json` and `cmp` the two. They must be identical, since `--master-seed` replays a batch byte-for-byte.

- [ ] **Step 4: Oracle 3, final**

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
perl -e 'alarm 590; exec @ARGV' -- $SDD/oracle3.sh final   # oracle3 mode foreground; background: Global Constraints
```

Expected: exit 0. Every recorded batch replays with the same verdicts, failures, outcomes and coverage. Then run every existing pinned-seed test as part of the full gate below; they live in the suites it runs.

- [ ] **Step 5: The full gate, counted**

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
( cargo test --workspace --no-fail-fast > $SDD/gate-final.log 2>&1; print "EXIT=$?" >> $SDD/gate-final.log ) &
```

Poll with bounded checks until the log's last line is `EXIT=…`, then:

```bash
cd ~/Documents/GitHub/charpente && SDD=/Users/noahmitchem/Documents/GitHub/charpente/.superpowers/sdd/2026-10-13-outbound-effects
grep -c "^test result: ok" $SDD/gate-final.log; grep -c "^test result: FAILED" $SDD/gate-final.log
grep -c "error: test failed" $SDD/gate-final.log; tail -1 $SDD/gate-final.log
```

Expected: the ok count is the base count from Task 1 plus exactly six new `test result:` lines: charpente's `tests/outbound`, edge-harness's `tests/outbound` (its six scenarios ignored), and outbound-fixture's lib, `outbound_node` bin, `fake_remote` bin and doc-tests. outbound-fixture's `sim` target is `harness = false` and prints no `test result:` line, so it is judged by the exit status alone: `EXIT=0` and 0 `error: test failed`. 0 FAILED. Compare the count with `grep -c "^test result:" $SDD/gate-final.log`.

- [ ] **Step 6: The ledger**

Append to `docs/LEDGER.md`, in its form:

```markdown
## <date> — outbound effects shipped: call/settle jobs, minted keys, the Outbound trait, the production HTTPS edge

<Two sentences on what landed and why (MANIFESTO §5's promise, unbuilt until now). The commits,
by sha (`git log --oneline <base>..HEAD`). Oracle 1–7 results: the 40 bases and their 10,240
universes green; the fixture's 10,000 universes from master seed 0x… green and replayed
identically; oracle 3 unchanged over the four recorded batches; the planted bugs' seeds; the
mutation counts per set from Task 13; the edge-harness scenarios. The BENCH.md compile delta.
What stays open, deliberately: spec §15's v1.x rows. "Next: the docs-only build (exit criterion 5)."> 
```

Every number is copied from a file under `$SDD`, never from memory. Run `git show <sha> --stat` before writing any sha.

- [ ] **Step 7: Commit; push on Noah's word; CI**

```bash
git add docs/LEDGER.md
git commit -m "ledger: outbound effects — 10,240 platform universes over 40 fresh bases, 10,000 fixture universes from 0x…, oracle 3 unchanged, mutation N/N"
```

Ask Noah before pushing: CI minutes are limited, and `main` is ahead of `origin` by every commit in this plan. After his go-ahead, `git push` and watch the run: `gh run watch` on `ci.yml`, and the next scheduled `edge-harness.yml` for oracle 7. Record both run ids in the ledger entry with a one-line follow-up commit.

---

### Task 19: The docs-only build — preflight, the brief, and the record's skeleton

Spec §16(5): a fresh agent builds an app that sends email and calls an LLM from the docs alone, judged as helpdesk and statuspage were. This task and Task 20 follow `docs/superpowers/plans/2026-09-17-third-docs-only-build.md`'s method. Every command they need is written here.

**Files:**
- Create: `docs/experiments/2026-10-outbound/BRIEF.md`, `docs/experiments/2026-10-outbound/README.md`
- Create: `.superpowers/sdd/2026-10-13-outbound-effects/docs-only/preflight.md`

- [ ] **Step 1: Preflight against the local crates**

```bash
cd ~/Documents/GitHub/charpente && git status --porcelain | wc -l     # 0
SHA=$(git rev-parse HEAD); print $SHA
cargo install --path crates/charpente-cli --locked --force 2>&1 | tail -2; print "INSTALL_EXIT=${pipestatus[1]}"
mkdir -p ~/Documents/GitHub/preflight-scratch && cd ~/Documents/GitHub/preflight-scratch
charpente new preflight; print "NEW_EXIT=$?"
cat >> preflight/Cargo.toml <<'TOML'

[patch.crates-io]
charpente = { path = "../../charpente/crates/charpente" }
charpente-sim = { path = "../../charpente/crates/charpente-sim" }
TOML
```

Each gate's exit is captured on its own: `a | tail && b` reads `tail`'s status, which is always 0, so a red clippy would still run the sim and record 0 for both.

```bash
cd ~/Documents/GitHub/preflight-scratch/preflight
cargo clippy --all-targets -- -D warnings 2>&1 | tail -2; print "CLIPPY_EXIT=${pipestatus[1]}"
charpente test --sim 2>&1 | tail -3; print "SIM_EXIT=${pipestatus[1]}"
```

Then prove the mutants verb out of tree, as the third build's preflight Step 4 did: the brief requires `--mutants`, and the local crates now pull hyper-rustls and ring into the scratch copy, so a failure here would otherwise surface only mid-attempt.

```bash
cd ~/Documents/GitHub/preflight-scratch/preflight
unset CARGO_TARGET_DIR
charpente test --mutants --dir . --file src/lib.rs --test-package preflight --timeout 300 2>&1 | tee mutants.log | tail -8
print "MUTANTS_EXIT=${pipestatus[1]}"
```

Expected: `CLIPPY_EXIT=0`, `SIM_EXIT=0`, and a `mutants: N total — …` line with `MUTANTS_EXIT` 0 or 1 (both mean it ran; 2 means it could not, which is a finding to fix before dispatch).

0.1.0 on crates.io has no outbound surface, so the patch block is load-bearing here. Confirm that the scaffold compiled against the local crates: `cargo tree -i charpente | head -2` shows the path. If `[patch]` cannot match the registry version, use the third build's documented fallback (path dependencies via `sed`) and record which form worked. Write `preflight.md` (sha, dependency form, every captured exit, the mutants summary line), then `rm -rf ~/Documents/GitHub/preflight-scratch`.

- [ ] **Step 2: Write the brief** (`docs/experiments/2026-10-outbound/BRIEF.md`, verbatim)

```markdown
# triage — product brief for a docs-only build (requirements, never implementation)

You are building a small backend on the charpente platform, from its documentation alone.
Your ONLY sources are this brief, `docs/README.md` and `docs/GUIDE.md` in the directory
you are in. Do not read anything outside this directory — not the platform's source, not
its other docs, not its examples. If the docs do not tell you how to do something, record
that in `FINDINGS.md` and choose the simplest thing the docs DO support — never patch the
framework. Every commit touches `triage/` only.

## Setup
The `charpente` CLI is on your PATH. Run `charpente new triage` in this directory, then
`git init` inside `triage/` — the scaffold does neither. Append these lines to the
scaffold's `Cargo.toml` exactly as written; they are the only lines you may add that name
a path outside this directory:

    [patch.crates-io]
    charpente = { path = "../../charpente/crates/charpente" }
    charpente-sim = { path = "../../charpente/crates/charpente-sim" }

## Domain
- Orgs. Operators belong to exactly one org and authenticate by session login and API keys
  as GUIDE §5 describes. Each org has an on-call email address its operators can change.
- Anyone, with no credential, can submit a feedback item (a short text) to an org by the
  org's id in the path. Operators read and list their own org's items.
- Every item is classified by an outside language-model service into one of: bug, praise,
  urgent, unsure. An item's label is written once.
- An item labelled urgent pages the org: one email to the on-call address as it was when
  the item was labelled.

## The two outside services
- The classifier, host `api.llm.example`. `POST /v1/classify` with JSON `{"text": "…"}`,
  authenticated by `authorization: Bearer <key>` where the key is in the environment
  variable `LLM_API_KEY`. It answers 200 with `{"label": "bug" | "praise" | "urgent" |
  "unsure"}`, sometimes 429 or 503 when overloaded, and is slow at the tail. It does NOT
  deduplicate: every request is classified and billed, and two requests for one text may
  answer differently.
- The mailer, host `api.mail.example`. `POST /v1/send` with JSON `{"to", "subject",
  "body"}`, authenticated by the header `x-mail-token` from the environment variable
  `MAIL_TOKEN`. It honours an `Idempotency-Key` request header: a repeated key returns the
  first response without sending again. It answers 200 with `{"id": "…"}`, 422 for an
  address it will never accept, and sometimes 503.

## Behaviours
- Submitting an item enqueues its classification in the same transaction.
- An urgent label and the page it causes are decided together: the label is never written
  without its page being scheduled, and a page is never sent for an item that is not urgent.
- An address the mailer refuses with 422 is recorded on the item as a failed page; it is
  not retried.
- Nothing the outside services return may cross into another org's rows.

## Proof (the part that matters)
`tests/sim.rs` using the platform's simulation harness, with your own models of both
services and properties for:
- acked writes are durable across crashes; replay is identical from a seed;
- every urgent item causes exactly one email the mailer actually sent, and no other item
  causes one — under the full fault matrix, including the outside services' faults;
- an item's label is written exactly once, however often the classifier was asked;
- no org ever observes another org's items, labels or pages.

`charpente test --sim --universes 10000` must be green; record the master seed it prints.
Then run `charpente test --mutants` (GUIDE §8) over the files carrying the two jobs, with
`--test-package triage`, and record its full output in `FINDINGS.md`. Every mutant caught
or unviable is the target; a MISSED is a one-sided property to fix before you are done.

## Deliverables
- `triage/` as its own git repository, building with `cargo test` and
  `cargo clippy --all-targets -- -D warnings`.
- `FINDINGS.md` in the repository: a friction ledger in your own words, dated as you go;
  how many times you restarted an approach; every GUIDE section you had to re-read and why;
  what you could not find in the docs; the master seed; the `--mutants` output.
- Commit as you go, small commits, named paths.
```

The brief names the services' behaviour and never the platform's surface. Whether the docs lead the builder to `outbound`, `Idempotent`, `Host`, the checkers and `settle`-time enqueueing is the experiment.

- [ ] **Step 3: Hash it, write the README skeleton, commit**

```bash
python3 -c "import hashlib;print(hashlib.sha256(open('docs/experiments/2026-10-outbound/BRIEF.md','rb').read()).hexdigest())"
```

`README.md`, in the third build's skeleton form:
- the framing paragraph (spec §16(5): whether the docs lead a stranger to minted keys, read-only `call` and atomic `settle`, and whether the outbound oracles catch what they claim);
- `**Status: attempt 1 in flight.**`;
- the table with brief sha256, attempt 1 pinned sha, and dependency form.

```bash
git add docs/experiments/2026-10-outbound/BRIEF.md docs/experiments/2026-10-outbound/README.md
git commit -m "experiment(outbound): the brief for the outbound docs-only build, pinned by hash before any attempt"
```

---

### Task 20: The docs-only build — the attempt, the gates, the blind review, the verdict, the record

**Files:**
- Create: `$RUN` (`~/Documents/GitHub/triage-run`; attempt N ≥ 2: `triage-run-N`) with `BRIEF.md` and frozen `docs/README.md` and `docs/GUIDE.md`
- Create: `.superpowers/sdd/2026-10-13-outbound-effects/docs-only/attempt-N/{gates.md,review.md,verdict.md,keyless-check.md}`
- Modify, at close-out: `docs/experiments/2026-10-outbound/README.md`, `attempt-N/{FINDINGS.md,review.md}`, `docs/LEDGER.md`, `PLAN.md`

The constraints carry over from the third build's plan:
- the pinned sha's `crates/` and manifests never change within an attempt;
- the builder never sees this plan, the spec, `crates/` or anything outside `$RUN`;
- nothing is believed from a report;
- `CARGO_TARGET_DIR` stays unset for every mutants run, and `cargo clean -p triage` follows each one;
- long runs go in the background and are polled to process exit.

- [ ] **Step 1: Pin and lay out the run directory**

```bash
RUN=/Users/noahmitchem/Documents/GitHub/triage-run; APP=$RUN/triage   # attempt N ≥ 2: triage-run-N
cd ~/Documents/GitHub/charpente && git status --porcelain | wc -l      # 0
SHA=$(git rev-parse HEAD); print "attempt sha $SHA"
mkdir -p $RUN/docs
print $SHA > $RUN/attempt-sha.txt   # later blocks re-set SHA from this file, never from HEAD
cp docs/experiments/2026-10-outbound/BRIEF.md $RUN/BRIEF.md
cp README.md $RUN/docs/README.md && cp docs/GUIDE.md $RUN/docs/GUIDE.md
python3 -c "import hashlib;print(hashlib.sha256(open('$RUN/BRIEF.md','rb').read()).hexdigest())"   # equals the README's hash
```

- [ ] **Step 2: Dispatch the builder**

Agent tool, `subagent_type: general-purpose`, `model: opus`, **no `name`**. The prompt, verbatim, with `<RUN>` expanded:

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

Record the launch result's `output_file` in progress.md. When the notification arrives, check the deliverables (`$APP/.git`, `FINDINGS.md` with a master seed and a `mutants:` line, `tests/sim.rs`). If the builder stopped short, with no live cargo process under `$APP` and no final report, resume the same agent with exactly `continue from your FINDINGS.md and git log`. Count each resume. Never answer its questions; they are testimony. While it runs, no `cargo` runs in charpente's tree and nothing under `crates/` changes. Freeze with `cp <output_file> $RUN/transcript.jsonl` and `git -C $APP bundle create ~/Documents/GitHub/triage-attempt-N.bundle --all`.

- [ ] **Step 2a: Record the builder's master seed**

Read the master seed from `$APP/FINDINGS.md` (`grep -n -i 'master seed' $APP/FINDINGS.md`) and check it against the builder's final reply. Write it to `$RUN/mseed.txt` (`print 0x<16 hex digits> > $RUN/mseed.txt`). Every later block re-sets `MSEED` from that file.

Every block in Steps 3 and 4 opens with the same line, since shell state does not persist (Global Constraints); attempt N ≥ 2 writes `triage-run-N` in it, as Step 1 does:

```bash
RUN=/Users/noahmitchem/Documents/GitHub/triage-run; APP=$RUN/triage; SHA=$(cat $RUN/attempt-sha.txt); MSEED=$(cat $RUN/mseed.txt)
```

- [ ] **Step 3: The mechanical gates** (each result into `gates.md`, quoting output)

```bash
RUN=/Users/noahmitchem/Documents/GitHub/triage-run; APP=$RUN/triage; SHA=$(cat $RUN/attempt-sha.txt); MSEED=$(cat $RUN/mseed.txt)
cd ~/Documents/GitHub/charpente
git diff $SHA --stat -- crates Cargo.toml Cargo.lock | wc -l          # 0
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

```bash
RUN=/Users/noahmitchem/Documents/GitHub/triage-run; APP=$RUN/triage; SHA=$(cat $RUN/attempt-sha.txt); MSEED=$(cat $RUN/mseed.txt)
cd $APP
cargo clippy --all-targets -- -D warnings 2>&1 | tail -2; print "CLIPPY_EXIT=${pipestatus[1]}"
cargo test --no-fail-fast 2>&1 | tee $RUN/app-test.log | grep -c '^test result: ok'; print "EXIT=${pipestatus[1]}"
grep -c 'error: test failed' $RUN/app-test.log      # 0: the app's sim target is harness = false and prints no `test result:` line
grep -c '^test result: FAILED' $RUN/app-test.log
grep -n -i 'master seed' FINDINGS.md | head -3
```

Then replay the agent's master seed twice, each in the background, and compare:

```bash
RUN=/Users/noahmitchem/Documents/GitHub/triage-run; APP=$RUN/triage; SHA=$(cat $RUN/attempt-sha.txt); MSEED=$(cat $RUN/mseed.txt)
cd $APP
( charpente test --sim --universes 10000 --master-seed $MSEED --format json > $RUN/replay-1.json 2> $RUN/replay-1.err; print "EXIT=$?" >> $RUN/replay-1.err ) &
# poll; then the same to replay-2.json; then:
cmp $RUN/replay-1.json $RUN/replay-2.json && print REPLAY_IDENTICAL
jq '{universes, failures: (.failures|length), verdicts, gave_up}' $RUN/replay-1.json
```

Then run the controller's own mutants over every file holding an outbound job (`grep -ln 'outbound *=' src/**/*.rs src/*.rs`):

```bash
RUN=/Users/noahmitchem/Documents/GitHub/triage-run; APP=$RUN/triage; SHA=$(cat $RUN/attempt-sha.txt); MSEED=$(cat $RUN/mseed.txt)
cd $APP
unset CARGO_TARGET_DIR
( charpente test --mutants --dir . --file src/<f>.rs --test-package triage --timeout 600 > $RUN/mutants-<f>.log 2>&1; print "EXIT=$?" >> $RUN/mutants-<f>.log ) &
# poll; then:
grep -h '^mutants:\|MISSED\|TIMED OUT\|^EXIT=' $RUN/mutants-*.log; cargo clean -p triage
```

The verb indents its `N MISSED:` and `N TIMED OUT:` lines, so those two patterns are unanchored; the `mutants:` line carries the counts either way.

Every line must be PASS before the review starts:
- sha unchanged;
- 0 suspicious calls, or each one ruled;
- clippy 0;
- the app's `cargo test --no-fail-fast` gives `EXIT=0` and 0 `error: test failed`;
- `REPLAY_IDENTICAL` with `failures 0` and `universes 10000`;
- every mutants log `EXIT=0` with 0 missed and 0 timed out.

- [ ] **Step 4: The ambiguous-call check, on a copy** (spec §16(5); the controller's, before the review)

```bash
RUN=/Users/noahmitchem/Documents/GitHub/triage-run; APP=$RUN/triage; SHA=$(cat $RUN/attempt-sha.txt); MSEED=$(cat $RUN/mseed.txt)
rm -rf $RUN/keyless && cp -R $APP $RUN/keyless && cd $RUN/keyless
grep -n -i 'idempotency' src/**/*.rs src/*.rs      # the line(s) placing the key on the mail call
```

Delete the line(s) that put the key on the mailer request, and nothing else. Record the diff in `keyless-check.md`. Then run the agent's own properties:

```bash
RUN=/Users/noahmitchem/Documents/GitHub/triage-run; APP=$RUN/triage; SHA=$(cat $RUN/attempt-sha.txt); MSEED=$(cat $RUN/mseed.txt)
cd $RUN/keyless
( charpente test --sim --universes 10000 --master-seed $MSEED --format json > $RUN/keyless.json 2> $RUN/keyless.err; print "EXIT=$?" >> $RUN/keyless.err ) &
# poll; then:
jq '.failures[0] | {test, seed, message, repro}' $RUN/keyless.json
jq '.failures[0].config | {outbound_lost_reply, outbound_slow, outbound_lost_request}' $RUN/keyless.json
```

The check passes when the batch is red and its first failure is one of the agent's own email-exactly-once properties. Its config must carry a non-zero `outbound_lost_reply` (or `outbound_slow`, the other way a reply misses its deadline), and the failure must replay: run the printed `repro` line once and see the same failure. A green keyless batch fails the check: the agent's properties do not see a double send, so the reviewer's Critical list gains one. Write the result, the seed and the replay into `keyless-check.md`, then `cargo clean` there and `rm -rf $RUN/keyless`.

- [ ] **Step 5: The blind review**

Agent tool, `subagent_type: general-purpose`, `model: opus`, no `name`:

```
You are reviewing an application built on the charpente platform by another agent from
the platform's documentation alone. Review the CODE, not the story: do not open
<APP>/FINDINGS.md — it is the builder's own account and would anchor you.

Read, in this order: <RUN>/BRIEF.md (what was asked), <charpente>/docs/GUIDE.md §5, §6,
§8, §9, §16 (what the platform promises and refuses), then every file under <APP>/src and
<APP>/tests. <gates.md path> and <keyless-check.md path> hold the mechanical results
already established.

Report findings as Critical / Important / Minor. A Critical is a defect that violates the
brief's stated requirements or the platform's own guarantees — an unauthenticated write, a
cross-tenant read on any path, a page sent twice or never under faults, a label written
twice, an outside reply crossing into another org, a credential in code or the log, a
property that would stay green if the code it guards were deleted, a sim model that cannot
produce the faults the brief names. For EVERY Critical, state which platform claim, if any,
should have refused it: a README guarantee by number, a GUIDE section and the sentence, a
compile-time refusal (quote the error), a `charpente test --sim` property class or checker,
or `charpente test --mutants` — or "no claim". Be exact and honest about "no claim".

Pay particular attention to: where the app puts the idempotency key and why; whether the
classifier's lack of deduplication is reflected in what the app treats as exactly-once
(state) versus at-least-once (calls); whether the urgent page is enqueued in the same
transaction as the label; and whether its sim models are the outside world the brief
describes or a convenience that cannot fail.

Write the FULL review to <review.md path> FIRST, then return a summary of at most fifteen
lines: counts per severity, each Critical in one line with its citation.
```

Verify that `review.md` exists. Resolve every Critical's citation yourself against the file it names: `grep -n` the sentence in GUIDE, and open the macro's string in `crates/charpente-macros/src/`. A citation that does not resolve is corrected in progress.md.

- [ ] **Step 6: Adjudicate and put the verdict to Noah**

- Rule on each Critical in progress.md: `Ruling <n> (attempt N, Critical <k>): <defect>. Claim: <citation or "no claim">. Holds? <why>. → PLATFORM FINDING | AGENT BUG`. A Critical fails the bar only when the cited claim holds.
- Read FINDINGS.md as testimony. Classify each sentence about charpente as right, wrong (with the refuting file:line), or right about a symptom for the wrong reason.
- Write `verdict.md`: PASS or FAIL, the gates, the keyless check, each ruling, the testimony list and the resume count.
- Present it to Noah and record `Ruling: verdict attempt N signed <PASS/FAIL> by Noah <date>`.

The ledger and PLAN wait for that line.

- [ ] **Step 7: On a FAIL — fix the platform, re-run the same brief**

The first platform finding gets `superpowers:brainstorming`: bounded means a chat design and Noah's yes, architectural means its own spec. Its pin is written red-first against the pinned sha. It lands green with the three-command gate counted and docs in the same commit, with a ledger entry naming the attempt that found it. Then repeat Steps 1–6 as attempt N+1: a new `$RUN`, a new sha, the docs re-copied, and `BRIEF.md` byte-identical by hash. Every attempt's directory is kept.

- [ ] **Step 8: The record**

Copy each attempt's `FINDINGS.md` (byte for byte: it is testimony) and `review.md` into `docs/experiments/2026-10-outbound/attempt-N/`. Write the README in the helpdesk/statuspage form. It has a table per attempt: pinned sha, dependency form, resumes, master seed, replay, controller's mutants counts, the keyless check's seed, app-gate exits, verdict, and Noah's sign-off date. It records the testimony rulings with their file:lines, the platform findings and their commits, and where the bundle is. Every number is copied from a file in `attempt-N/`. Then:
- `docs/LEDGER.md` gains `## <date> — the outbound docs-only build: a triage service, <N> attempt(s), <verdict>`, in the third build's entry form.
- PLAN.md's outbound row records exit criterion 5 met, or the attempt count and the open finding.

Run the full gate counted, as Task 18 Step 5 does, then:

```bash
git add docs/experiments/2026-10-outbound docs/LEDGER.md PLAN.md
git commit -m "experiment(outbound): the outbound docs-only build — <N> attempt(s), <verdict>; exit criterion 5"
```

Push is Noah's call.
