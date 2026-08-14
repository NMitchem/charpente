# CLAUDE.md — charpente

Instructions for agents working in this repo. Read the docs in this order before writing
any code: README.md → DESIGN.md → PLAN.md (MANIFESTO.md for spirit; DECISIONS.md before
proposing to change an approach; NEIGHBORS.md before designing any subsystem with prior
art — DST, storage, replication).

## What this is

An agent-native, single-binary Rust backend platform. M0 is complete; M1 is in progress
(see "Current state" below). The milestones and exit criteria in PLAN.md are the roadmap;
do not reorder a milestone's dependency sequence.

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
(dual-primary), 1 (benchmark harness) and three ruled interludes are done; **sub-project 2,
primary-side group commit, is in flight** — T1–T7 committed, T8–T11 open. The authoritative
mid-plan record is the ledger at `.superpowers/sdd/2026-08-10-group-commit/progress.md`.

The full chronological record — every milestone, sub-project, ruled interlude, and the M2
follow-up backlog — is **[docs/LEDGER.md](docs/LEDGER.md)**. Read it when you need the
history behind a decision; it is not required to start work.

## Subagent dispatch

This repo commits its own skills (`.claude/skills/`) and personas (`.claude/agents/`).
When dispatching a subagent for implementation, review, a census or an evidence sweep,
select the matching charpente persona rather than `general-purpose`. Note that
superpowers' `subagent-driven-development` templates hard-code `general-purpose` and vary
only the model — override that here.
