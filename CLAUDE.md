# CLAUDE.md — charpente

Instructions for agents working in this repo. Read the docs in this order before writing
any code: README.md → DESIGN.md → PLAN.md (MANIFESTO.md for spirit; DECISIONS.md before
proposing to change an approach; NEIGHBORS.md before designing any subsystem with prior
art — DST, storage, replication).

## Why this exists (orientation; MANIFESTO.md carries the full argument)

Agents write expert systems code cheaply; what they cannot do cheaply is *prove it
right*. Agent output quality tracks **oracle density** — where a dense mechanical oracle
exists, machine-written code is excellent, and where none exists it is weak. So the
product here is not the feature list, it is the **oracle substrate** that makes a
machine-written backend mechanically checkable. Design questions resolve toward whatever
gives an agent a faster and more precise *no*.

Four consequences carry the design. Each can be violated without breaking a single test,
which is why they are written down:

- **One process, one commit point, one log.** State changes, job enqueues and realtime
  events commit in the same transaction. Replication ships the log, subscriptions tail
  it, failover replays it, the simulator feeds it. Introducing a second place where
  things become true is the one architectural mistake this codebase cannot absorb.
- **The framework is the reviewer.** Compile-time contracts are the oracle for business
  logic — a schema change that breaks a query must be a compile error, not a 500 — and
  deterministic simulation is the oracle for everything beneath it. That is why error
  text and seeds are product surface rather than developer conveniences (hard rules 5
  and 3).
- **Constraints are the feature.** No network await inside a transaction. A programming
  model that admits nondeterminism cannot be replayed; one that forbids it replays byte
  for byte from a seed. If a feature seems to need an escape hatch, the design is wrong
  (hard rule 2).
- **One big box, replicated.** One ordered log on one primary; followers replay it for
  reads and failover. There is no sharding story and there will not be one.

Refused on purpose, so you don't propose them: microservice/RPC/service-mesh anything,
SQL as the primary query interface (a read-only Postgres-wire endpoint for BI tools is
fine later), porting existing apps, and feature flags on determinism.

## What this is

An agent-native, single-binary Rust backend platform. M0 through M3 (launch) are complete,
and 0.1.0 is on crates.io (see "Current state" below). The milestones and exit criteria in
PLAN.md were the roadmap, and its "After launch (v1.x)" table holds what comes next, each
item with the trigger that reopens it.

## Commands

- Full check, same three commands as CI (.github/workflows/ci.yml):
  `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`
  (carries the nondeterminism deny-list), `cargo test --workspace --no-fail-fast`.
  **`--no-fail-fast` is load-bearing, not tidiness.** Without it cargo stops at the first
  failing package and every later suite silently never runs, which in a log is
  indistinguishable from a pass — that blind spot hid two `replication` failures for three
  commits. Confirm green by COUNTING: pipe through `tee`, then `grep -c "^test result:
  ok"`. Never conclude it from the absence of `FAILED`, and note that a `grep` matching
  nothing exits 1, so under `set -o pipefail` "nothing failed" and "the check itself
  broke" produce the same status.
- One test file: `cargo test -p charpente --test kernel` — suites live in each workspace
  member's `tests/` (charpente: archive/auth/clock/describe/group_commit/http/kernel/outbound/realtime/
  replication/tenant/transactions/typed_tables/wire; charpente-core: describe_tables/engine/
  group_commit/materialize/migrate_apply/migration_gate/migrations/store_latch;
  charpente-sim: crash_recovery/determinism/net_partition/wal; charpente-cli: cli/ctl/
  describe/genclient/new/repro_meta/restore; charpente-tsgen: revive/routes/types;
  charpente-macros: trybuild; edge-harness: acked_durable/clock_restart/failover/hunts/outbound/
  realtime_resume/smoke/tenant_wire/unopenable_store; bench-harness: app/compile/compile_bench_script/harness/phase_trace;
  ts-fixture-app: drift/samples; guide-examples: claims_census/describe_contract/doc_inventory/doc_truth/hook_census/mutants_exclusions; sim-fixture: sim;
  outbound-fixture: sim; invoicing: describe_golden/sim). Every list is exhaustive as
  written — if a name is missing, the list is stale, not selective.
  `guide-examples`' `doc_inventory` gate enforces that against `members`, so a NEW crate
  with tests cannot sit outside this list either.
- Replay one universe: `CHARPENTE_SEED=0x… cargo test -p <crate> --test <file> --
  --nocapture`. Every sim failure prints this exact repro line — paste it verbatim.
  `CHARPENTE_SEED` replays **one universe, not the batch**, so every coverage
  requirement is skipped — `floor:` and `ceiling:` as well as `require:`. A green replay says that seed
  passes; it says nothing about any rate, and it cannot be used as evidence a floor or a
  ceiling holds.
- App-level sim batch: `cargo run -p charpente-cli -- test --sim [--universes N]
  [--seed 0x…] [--master-seed 0x…] [--filter S] [--jobs N] [--format human|json]
  [--dir PATH]` — dogfood target `--dir crates/sim-fixture`. Exit 0 green, 1 failures,
  2 usage/launch error. `--master-seed` replays a whole batch byte-identically.
- **Mutation instrument (on demand, NOT in `cargo test --workspace`):** `scripts/mutants.sh`
  wraps `charpente test --mutants [--file PATH]… [--re REGEX] [--timeout SECS]`. Needs
  `cargo install cargo-mutants --version 27.1.0 --locked`. Scoped by design — the whole
  workspace is 4193 mutants (days of wall clock), so the default is a named oracle-bearing
  target set. The default test set is `charpente-core` + `charpente-sim` regardless of
  `--file`, so a `--file` in any other crate needs `--test-package <that crate>` or the run
  judges its mutants with suites that never exercise them and reports them all MISSED
  (2026-09-16, `charpente-macros`). cargo-mutants 27.1.0 times its baseline over ONE
  package (the mutated file's) while every mutant runs every test package, so whenever
  those differ (two or more test packages, as the default set is, or one that is not
  every `--file`'s package) and no `--timeout` is given, the verb first times one
  `cargo test` over the test set and passes `--timeout` as 5x that wall time, printing
  both numbers (an upper bound under `--property`); without that, slow-but-caught mutants
  read TIMEOUT. It checks `cargo mutants --version` before any of that, so a missing tool
  costs no timing run. Exit 0 every
  viable mutant caught, 1 a finding (**MISSED** = no oracle noticed, i.e. a one-sided
  property) or an unresolved run (**a timeout is not a catch**), 2 usage, infrastructure,
  or a baseline that was not green.
  `--jobs > 1` (flag or `CARGO_MUTANTS_JOBS`) is refused under an inherited
  `CARGO_TARGET_DIR`: cargo's artifact hash does not vary by mutation content, so parallel
  jobs sharing one target dir report results that measured nothing. Read the verb's output,
  never cargo-mutants' own exit code: the raw tool exits **0** when a filter matches nothing
  and writes no `outcomes.json` at all, and a failed baseline leaves every counter reading
  zero. The verb consults that exit code in exactly one place — when there is no
  `outcomes.json` — to tell "matched nothing" (exit 0) from "died before running" (any other
  exit), and it creates its own `--output` directory, so an exported CARGO_TARGET_DIR no
  longer needs a hand `mkdir`. Mutants proven equivalent are hidden by name in
  `.cargo/mutants.toml`, one rationale comment per entry, and `tests/mutants_exclusions.rs`
  pins the count — a third survivor is a property to write, not an entry to add.
- Editing docs/GUIDE.md can break the build: `crates/guide-examples`'s build.rs extracts
  and compiles every ```rust block. `cargo build -p guide-examples` checks it alone. A
  pinned `text` block (preceded by `<!-- pinned -->`) is gated
  by `tests/doc_truth.rs`: its quoted text must still appear in the code,
  and the pinned count is a floor.
- **Capturing a sweep's exit status: this shell is zsh, which does not populate bash's
  `PIPESTATUS` at all.** `${PIPESTATUS[0]}` expands to the empty string in every case,
  and an empty string in a log reads as "no failure" to whoever skims it. Use
  `${pipestatus[1]}` (lowercase, 1-indexed) or `set -o pipefail` and test `$?` — and
  prove the idiom against a deliberate failure once before trusting a run to it. Pipe
  cargo through `tee`, never `tail`: `tail` eats the aggregate counts.

## Workspace map

- `crates/charpente-core` — the deterministic core: `io` traits, `log` (WAL), `engine`,
  `table`, `db`, jobs. No ambient IO anywhere inside.
- `crates/charpente` — the platform crate apps depend on (re-exports `core`, officially
  load-bearing): `http` kernel, `kernel` loop, `serve` (the hyper/tokio wire edge),
  `prod` (the one deny-list-allowed IO module). `extern crate self as charpente` makes
  the macros usable in its own src.
- `crates/charpente-macros` — `#[table]`/`#[route]`/`#[job]`; all emit `::charpente::`
  paths, so app crates depend on `charpente` alone.
- `crates/charpente-sim` — simulator (seeded xoshiro, virtual time, fault matrix), the
  frozen `run_universes!` runner + swarm runner, `harness` (the `charpente test --sim`
  runtime), `checks` (the public checker library).
- `crates/charpente-cli` — the `charpente` binary; edge tooling, may spawn processes.
- `crates/charpente-tsgen` — the TypeScript client emitter: route/event types to
  `client.ts`, behind the CLI's `gen-client`/`check-client` verbs.
- `crates/sim-fixture` — minimal app the CLI dogfoods against; single-dep on `charpente`,
  so it also pins macro-path expansion.
- `crates/outbound-fixture` — the app outbound effects are proven against: a launch-driven
  node binary and a TLS fake remote the edge harness drives (its own crate so
  sim-fixture's and invoicing's recorded batches never move).
- `crates/clock-fixture` — the app the restart-stable-time wire oracles run against: a
  one-second schedule, a session route with a configurable TTL, and an OIDC verifier over a
  fixed test key (`mint_jwt`); driven by the edge harness's `clock_restart`.
- `crates/ts-fixture-app` — the app the TS client is generated from; its `tests/` are the
  codegen's oracles (staleness diff, wire-shape drift).
- `crates/guide-examples` — compile gate for docs/GUIDE.md, the CLAUDE.md inventory gate
  (`tests/doc_inventory.rs`) that keeps this map and the test lists above honest,
  `tests/describe_contract.rs`, which diffs §13's `describe` JSON against a freshly built
  `examples/invoicing` fixture and its outbound blocks against an outbound app the test
  builds, and `tests/hook_census.rs`, which holds "no panic-hook installer" by walking the
  sources — the deny-list is inert under a file-level allow — and also holds
  `host_name_problem`'s two copies (charpente's `outbound.rs`, charpente-macros' `job.rs`)
  byte-identical.
  `tests/mutants_exclusions.rs` pins the known-equivalent mutant list's size and demands a
  rationale per entry.
- `crates/edge-harness` — black-box wire rig outside the deterministic boundary: stock
  binaries over real TCP, `kill -9`, TCP-proxy fault injection.
- `crates/bench-harness` — the benchmark instrument, also outside the boundary. Each layer
  splits measure/accounting; the accounting half carries no deny-list suppression, so hard
  rule 1 mechanically holds it free of clocks and IO. docs/BENCH.md is its only quotable
  output.
- `examples/invoicing` — the M0 demo app, agent-built from docs alone (DEMO.md,
  FINDINGS.md).

## Hard rules (the guarantees are the product)

1. **No ambient nondeterminism inside the core.** Zero direct calls to wall clocks, OS
   randomness, raw filesystem/network APIs, or thread/task spawning from core code — all IO
   goes through the `Clock`/`Entropy`/`Disk`/`Net`/`ObjectStore`/`Outbound` traits
   (DESIGN §1) — every top-level trait in `io.rs`, since `File`/`Listener`/`Conn` are handed
   out by `Disk`/`Net`. Enforce mechanically (clippy `disallowed_methods`/`disallowed_types`
   deny-list in CI from the first commit), not by review.
2. **No escape hatches on guarantees.** No "fast mode" bypassing the log, no network awaits
   inside transactions, no optional determinism, no feature flags on correctness
   (MANIFESTO §8). If a feature seems to need one, the design is wrong — stop and redesign.
3. **Sim-first development.** Every core subsystem lands with seeded fault-injection tests
   in the same PR. A bug found in sim gets its seed recorded in the regression suite. If
   you fix a bug without a failing seed first, you haven't reproduced it — find the seed.
4. **Assertions in the TigerBeetle style.** Assert invariants and *negative space* (what
   must never happen) liberally in core code; assertions stay on in release builds until a
   measured reason says otherwise.
5. **Error messages are product surface** (DESIGN §7). A contract violation must name the
   contract, the site, and the fix; a sim failure must print the seed and a one-command
   repro. Write tests against error output.
6. **Public-API changes update the docs in the same commit.** The docs are the spec;
   drifted docs are bugs.

## Style

- Plain direct Rust; smallest code that does the job. No helper-function sprawl, no
  speculative abstraction — the only sanctioned abstraction boundaries are the ones in
  DESIGN.md (IO traits, log, table layer).
- Comments only for constraints the code can't express (a rationale comment is fine; a
  narration comment is noise).
- Let errors bubble unless there's a specific behavior for that error; in core code,
  prefer asserting an invariant over handling an "impossible" state.
- Dependencies: minimal and justified in the PR description; nothing inside the
  deterministic boundary that does its own IO, threading, or time. `redb` (behind `Disk`),
  `hyper`-family at the HTTP edge, proc-macro crates for the derive surface are expected.
  Study `madsim`/`turmoil` (NEIGHBORS.md) before writing the simulator rather than after.

## Workflow

- This is a personal project of Noah's; commit directly to `main` until told otherwise.
  No Co-Authored-By trailers on commits.
- Verification before completion: for any core change, the evidence bar is "sim suite
  green over N fresh seeds," not "unit tests pass." State the seed count in the PR/commit.
- When a decision contradicts DECISIONS.md, don't silently override it — reopen the entry
  with the new evidence and get Noah's sign-off.

## Current state

**The roadmap is complete: M0 through M3 shipped, and M3 (launch) closed 2026-10-10 with
0.1.0 on crates.io.** What comes next is PLAN.md's "After launch (v1.x)" table, each item
gated on its trigger. That is deliberately all the status this file carries. Task-level state goes stale every time a task lands, and a
stale instruction is worse than an absent one — so the chronological record is
**[docs/LEDGER.md](docs/LEDGER.md)**, and per-plan mid-flight state lives in
`.superpowers/sdd/<date>-<name>/progress.md` (gitignored: local working files, absent from
a fresh clone). Read either when you need the history behind a decision. Neither is
required to start work; **ask what is in flight before starting something adjacent**,
because two sub-projects have run concurrently in this repo before.

**docs/BENCH.md is the only source of quotable performance numbers.** A figure in a
ledger, a run artifact, or a commit message is an observation until it lands there — runs
get rejected for instrumentation and load defects after producing plausible-looking
output, so "I saw it in a report" is not provenance.

## Subagent dispatch

This repo intends to carry its own skills (`.claude/skills/`) and personas
(`.claude/agents/`). **Check whether those directories exist before relying on them** —
they are a work in progress, and if they are absent, dispatch subagents as you otherwise
would rather than naming a persona that does not resolve.

When they are present: for implementation, review, a census or an evidence sweep, select
the matching charpente persona rather than `general-purpose`. Note that superpowers'
`subagent-driven-development` templates hard-code `general-purpose` and vary only the
model — override that here.
