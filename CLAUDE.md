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

An agent-native, single-binary Rust backend platform. M0 and M1 are complete; M2 is in
progress (see "Current state" below). The milestones and exit criteria in PLAN.md are the
roadmap; do not reorder a milestone's dependency sequence.

## Commands

- Full check, same three commands as CI (.github/workflows/ci.yml):
  `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`
  (carries the nondeterminism deny-list), `cargo test --workspace`.
- One test file: `cargo test -p charpente --test kernel` — property suites live in each
  crate's `tests/` (charpente: http/kernel/replication/transactions/typed_tables;
  charpente-sim: crash_recovery/determinism/net_partition/wal; charpente-core:
  engine/materialize).
- Replay one universe: `CHARPENTE_SEED=0x… cargo test -p <crate> --test <file> --
  --nocapture`. Every sim failure prints this exact repro line — paste it verbatim.
  Coverage `require:` keys are skipped under `CHARPENTE_SEED`.
- App-level sim batch: `cargo run -p charpente-cli -- test --sim [--universes N]
  [--seed 0x…] [--master-seed 0x…] [--filter S] [--jobs N] [--format human|json]
  [--dir PATH]` — dogfood target `--dir crates/sim-fixture`. Exit 0 green, 1 failures,
  2 usage/launch error. `--master-seed` replays a whole batch byte-identically.
- Editing docs/GUIDE.md can break the build: `crates/guide-examples`'s build.rs extracts
  and compiles every ```rust block. `cargo build -p guide-examples` checks it alone.
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
- `crates/sim-fixture` — minimal app the CLI dogfoods against; single-dep on `charpente`,
  so it also pins macro-path expansion.
- `crates/guide-examples` — compile gate for docs/GUIDE.md.
- `examples/invoicing` — the M0 demo app, agent-built from docs alone (DEMO.md,
  FINDINGS.md).

## Hard rules (the guarantees are the product)

1. **No ambient nondeterminism inside the core.** Zero direct calls to wall clocks, OS
   randomness, raw filesystem/network APIs, or thread/task spawning from core code — all IO
   goes through the `Clock`/`Entropy`/`Disk`/`Net` traits (DESIGN §1). Enforce mechanically
   (clippy `disallowed_methods`/`disallowed_types` deny-list in CI from the first commit),
   not by review.
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

**M1 complete** (2026-07-27). **M2 in progress**: sub-projects 0a (store-latch poison), 0b
(dual-primary), 1 (benchmark harness) and three ruled interludes are done. Two sub-projects
are open, and they are independent of each other:

**Sub-project 2 — primary-side group commit (the active thread).** T1–T8b are committed:
the mechanism ships (`Engine::submit_all`, `GroupOverlay`, `Db::begin_group`,
`App::begin_group`, the kernel drain arms) with a deterministic property suite and a
contended swarm behind it. Open:
- **T9** (re-verify every pinned seed) — briefed, not dispatched. Its pre-flight found
  the task infeasible as written and resized it: of 214 seed literals only **four** are
  load-bearing revert-guards. Five rulings landed 2026-08-14; spec §7 was amended.
- **T10** (benchmark numbers of record) — attempt 1 rejected. Two defects: the wire
  layer's *derived* syncs/sec is unsound once one fsync serves many commits (fixed in
  `3ebd322`, which makes the server report real syncs), and box load contaminated the
  300s settle. The kernel layer is valid and is the headline: **6285.16 records/sec at
  101.44 durable syncs/sec, 29.0x sub-project 1's baseline** — ~62 records per sync
  against `MAX_BATCH` 64, so the win is amortization, not a skipped sync. Wire numbers
  are observation only; do not quote them.
- **T11** (DESIGN.md close-out) — open, and carries a ruled fairness bound: a due job is
  serviced within `ceil(FLOOD/MAX_BATCH)` request-arm wins.

**Next action:** re-run the benchmark matrix on a genuinely quiet box (a fixed-duration
wait is *not* a settle on this machine — measured, the load arrives from other daemons
rather than draining from ours), then T9, then T11.

**Sub-project 3 — agent skills and personas (2026-08-14, paused).** T1 (this file's split
from docs/LEDGER.md) shipped. T2, T4 and T5 are complete **with no skill written**: the
evidence gates refused, which is a valid outcome of the experiment, not a failure. T3, T6
(the persona files) and T7 (close-out) are not started — so `.claude/skills/` and
`.claude/agents/` do not exist on disk yet. See "Subagent dispatch" below, which is
blocked on T6.

Mid-plan records live in `.superpowers/sdd/<date>-<name>/progress.md` — **gitignored**, so
they are local working files, absent from a fresh clone.

The full chronological record — every milestone, sub-project, ruled interlude, and the M2
follow-up backlog — is **[docs/LEDGER.md](docs/LEDGER.md)**. Read it when you need the
history behind a decision; it is not required to start work.

## Subagent dispatch

**Not yet in effect — `.claude/skills/` and `.claude/agents/` do not exist.** They are
sub-project 3's T3 and T6, both unstarted (see "Current state"). Until those land,
dispatch subagents as you otherwise would; what follows is the intended end state, not a
description of the tree.

Once the personas exist: when dispatching a subagent for implementation, review, a census
or an evidence sweep, select the matching charpente persona rather than
`general-purpose`. Note that superpowers' `subagent-driven-development` templates
hard-code `general-purpose` and vary only the model — override that here.
