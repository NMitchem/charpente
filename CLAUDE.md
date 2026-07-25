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

## Current state / immediate next steps

- Founding + knowledge docs committed. License: MIT OR Apache-2.0 (DECISIONS §9). All
  five crate names reserved on crates.io (DECISIONS §8). Workspace scaffolded with the
  nondeterminism deny-list in `clippy.toml` enforced in CI; new nondeterminism sources
  (tokio, rand, …) get deny-list entries in the same commit that adds the dependency.
- **M0 step 1 done**: sync poll-shaped IO traits in `charpente-core::io` (DECISIONS §10),
  full simulator in `charpente-sim` (seeded xoshiro256++, virtual time, fault matrix:
  injected write/sync errors, crash between any two disk ops, subset-of-sectors torn
  writes, partitions), production impls in `charpente::prod` (the one deny-list-allowed
  module). Crash-recovery property tests prove the harness catches what it must: naive
  length-prefix recovery fails under torn writes (regression seed 0x8bee1752811ae873
  pinned in tests/crash_recovery.rs), checked recovery holds over fresh seeds every run.
  `CHARPENTE_SEED=0x… cargo test -p charpente-sim --test <file>` replays any universe.
- **M0 step 2 done**: segmented checksummed WAL (`charpente-core::log`, format in DESIGN
  §2) + `StateMachine`/`Engine` apply loop (`charpente-core::engine`). Recovery truncates
  at the cut and syncs it before appends (stale-frame-resurrection guard, property-tested
  with fixed-size payloads); IO failure poisons the log (fsyncgate). Sim found one
  workload-model bug — recovery itself can hit injected fsync faults and must be
  restart-retried (seed 0x5cafb3ac016dccf9, noted in tests/wal.rs).
- **M0 step 3 done**: typed tables on redb behind `Disk` (`charpente-core::table`:
  `Id<T>`, `Row`, `Store`/`Snapshot`/`Writer`, exact-match secondary indexes) + the
  `#[table]` macro in charpente-macros. Durability model in DESIGN §4: apply at
  Durability::None + applied_seq meta key, checkpoint persists, WAL replays the gap.
  redb's raw-path constructors are deny-listed (Store::open is the only mount).
  Sim-established: redb's layout is deterministic; `Database::drop` is a graceful
  close, so crash tests must crash with the db open. `File` gained a `Send + Sync`
  supertrait; the sim world is now `Arc<Mutex>` (honest, uncontended).
- **M0 step 4 done**: typed transactions (`charpente-core::db`). `Db::transact(fn(&mut
  Tx) -> Result<T, E>)`: reads via the `View` trait (snapshot + read-your-own-writes
  overlay), writes/enqueues/emits buffered and committed as ONE effects record (payload
  format in DESIGN §2), applied to the store post-sync through the `Schema` registry;
  `TxError::{App, Io}` separates app rejection from platform failure. Enqueues
  materialize in the built-in `charpente.jobs` table (step 6's substrate); emits stay
  log-only. `StateMachine::apply` is fallible; a post-sync apply failure poisons the
  engine (restart-and-recover). Property-tested under the fault matrix including the
  dual-write invariant (jobs ≡ committed txns, exactly), plus a meta-test proving the
  harness catches apply-before-sync ordering.
- **M0 step 5 done**: HTTP layer. Deterministic kernel in `charpente::http` (typed
  `Request`/`Response`, router with mount-time conflict panic, `#[route]` argument
  convention enforced by the macro, 1 MiB body cap fencing the record-size assert,
  sessions inside the commit point via the platform `Session` table — raw tokens, M1
  hashes); one transaction per request, 200 only after the WAL sync (ack-after-durable).
  hyper/tokio confined to `charpente::serve` (wire↔struct + channel into the kernel
  thread); tokio's ambient APIs deny-listed. Sim suite includes the request-level crash
  property (a 200 survives kill-at-any-point; jobs ≡ acked requests) and a determinism
  check; loopback smoke test drives real HTTP/1.1 bytes. `#[route]` emits
  `::charpente` paths (all three macros do, since 2026-07-06's macro-path unification,
  which also made them usable in charpente's own src via `extern crate self`).
  `Disk` and `Entropy` gained a `Send` supertrait (the kernel
  thread owns the App), mirroring `File`'s earlier `Send + Sync`.
- **Pre-step-6 harness upgrade done** (from the 2026-07-02 DST research pass): swarm-
  sampled `FaultConfig` per universe (`run_universes_swarm!`; sampling order is frozen
  replay contract) + sometimes-coverage (`Sim::cover`, auto-covered `fault:*`/`crash:*`/
  `restart`, batch-level `require:` keys that fail by name when never hit). The swarm
  runner catches in-universe panics and reports seed+config+repro — the old
  panic-bypasses-seed-printing gap is closed for migrated tests (it remains on the
  frozen legacy runner). All four fault-matrix property tests migrated; `run_universes!`
  and every pinned seed are frozen forever. DESIGN now records the executor same-shape
  rule (the "which ready source next" decision is identical code in prod and sim),
  tracked fault-matrix extensions (bit-level durable corruption, misdirected writes,
  latent read errors), and two log durability fences (recovery-horizon rule for mid-log
  corruption; directory-entry durability at rotation) — all land with WAL segment
  management. NEIGHBORS carries the 2026-07 quarterly intel (quadrant still empty).
- **M0 step 6 done**: queue + cron + the deterministic edge executor (spec
  docs/superpowers/specs/2026-07-02-queue-cron-design.md). `JobRow` moved into
  `charpente-core` (meta-counter ids from `next_job_id`, bumped at apply; the raw
  `(seq, idx)` substrate is gone); `#[job]` + `jobs![]` registration + typed `RetryPolicy`
  (doubling backoff, Dead at max). Handlers are pure state transitions run inline as one
  transaction — effects and the Done-mark commit atomically, exactly-once state effects
  structurally; on `Err` a second transaction commits the failure record (attempts,
  due_at, last_error), with `JobError::{App, Io}` keeping platform faults from consuming
  retry budget (not an attempt: heap restored, row untouched); handler panics are absorbed
  into the same retry path. Cron rows tick as transactions (recurring enqueuer, fire-once
  catch-up, no backfill). The kernel loop (`charpente::kernel::Kernel::run`) is the
  same-shape rule's home: one ready set over requests/jobs/cron, one entropy draw only
  when contended; `serve.rs`'s blocking `recv` loop is gone, replaced by `ProdSource`.
  Enqueue keys are external identity, unread by the platform in v0 (M1's external-IO
  surface consumes them). Property suite (2×256 fresh-seed swarm batches, flagship green
  repeatedly): exactly-once effects under the full fault matrix, dead-letter content, 121
  ticks over ~10 virtual years plus exactly one catch-up tick after downtime,
  no-starvation, key round-trip, a split-done-mark meta-test proving the harness would
  catch the dual-write bug this design deletes, and (folding followup #24) response-trace
  determinism with checkpoint `is_injected` routing in all three property test files. The
  suite found and fixed 3 real bugs plus 1 checker gap while landing: `Kernel::new`
  resurrecting serviced jobs via stale drain ids (regression seeds pinned in the
  120-ticks/starvation tests), a swallowed injected fault misclassified as unrelated
  poison, a cross-writer checker gap, and imprecise redb latch-wording matching.
  Ruling (2026-07-05, step-7 spec): the request arm now stops the loop when
  `Db::poisoned()` is true after a request's 500 — it no longer keeps serving off a
  dead engine, closing the prior asymmetry with the job/cron arms.
- **Open cleanup**: `Kernel` and `ProdSource` anchor two independent `ProdClock`s in
  `serve.rs` — precision-only skew, no correctness impact, converge on one shared
  `Arc<dyn Clock>` later.
- **Resolved (2026-07-05): the redb `page_manager.rs` panic.** Root-caused via seed
  0x64622c74f8660a09: a crash drops an unsynced store `set_len` while the header
  god-page write survives, leaving file length ≠ header layout; redb 4.1.0 *asserts* at
  open (:231/:257) instead of returning Err, bypassing `Db::open`'s discard-and-rebuild.
  Fix: `Store::open` converts open-time panics into open failures (fence scoped to open;
  DESIGN §4); deterministic RNG-free regressions for both assert sites in
  tests/transactions.rs. Verified: 8/8 fresh 256-universe flagship batches green (was
  10–25% red per batch) + 3 full-workspace runs. Upstream report drafted, not yet filed.
- **M0 step 7 done**: `charpente test --sim` (spec docs/superpowers/specs/
  2026-07-05-sim-cli-design.md). Ships the harness (`charpente-sim::harness`: `SimTest`
  registry, `HarnessArgs`, sequential/parallel batch runner reusing the frozen
  `run_universe` executor, human + JSON `Report`), the app-side convention
  (`charpente_sim::sim_tests!{}` generates `tests/sim.rs`'s `main`, `harness = false`;
  workloads are `fn(&mut Sim) -> Result<(), String>`, `Err`/panic both fail a universe),
  the CLI (`charpente-cli`: `charpente test --sim` builds the app's `sim` target via
  `cargo test --no-run --message-format=json` and execs it directly, passing through
  --universes/--seed/--master-seed/--filter/--jobs/--format, exit 0/1/2), and the
  three-item checker library (`charpente-sim::checks`: `is_fault_fallout`,
  `replays_identically`, `acked_implies_durable`). Also closes the step-6 open item: the
  request arm now stops the kernel loop when `Db::poisoned()` after a request's 500
  (`Wal`/`Engine`/`Db::poisoned()` exposed in T1, ruling landed in T2). Batch model: one
  master seed (OS entropy, or `--master-seed` for byte-identical replay) derives
  per-universe seeds in registry order; `--jobs`-many workers race over an index counter,
  so scheduling never touches the seed sequence or report order. JSON is DESIGN §7's
  machine-readable surface: per-failure `{test, seed, master_seed, config, message,
  repro}`, `coverage`, `missing_coverage` (`{test, key}`, present when a `require`d key
  went unmet), and a trailing `summary` — shape pinned by tests. The headline meta-test
  (T8) proves the product promise end-to-end: parse a failure report, extract its repro
  line, execute it, get the identical failure byte-for-byte; master-seed batch replay is
  separately proven byte-identical. Two review fix waves landed mid-plan: T5 (JSON shape
  had drifted from the spec — per-failure `master_seed` and the summary shape were
  under-enumerated in the brief; spec governs, fixed in a662240) and T7 (escape-path
  tests, signal/launch failures mapped to exit 2 not 1, `std::process::Command`
  deny-listed at the edge with site-local allows in `charpente-cli`, fixed in 0a05567).
  Evidence: 2x fresh `cargo test --workspace` runs green, kernel flagship green once more,
  the pinned crash-recovery regression seed (0x8bee1752811ae873) green, clippy/fmt clean,
  and the CLI eating its own dogfood against `crates/sim-fixture` — 1024 universes, all
  green, master seed 0xf04ab23a1645c496, ~3.5s wall time over 8 jobs.
- **Macro-path unification done (2026-07-06, spec docs/superpowers/specs/
  2026-07-06-macro-path-unification-design.md)**: `#[table]`/`#[job]` now emit
  `::charpente::core::` paths (through the public `charpente::core` re-export, which is
  officially load-bearing), matching `#[route]` — app crates depend on `charpente`
  alone. `extern crate self as charpente;` in charpente's lib.rs makes all three macros
  usable in charpente's own src. `sim-fixture` is single-dep: any expansion regression
  to `::charpente_core::` breaks the workspace build.
- **M0 step 8 done — M0 COMPLETE (2026-07-07, spec docs/superpowers/specs/
  2026-07-06-demo-app-design.md).** Phase 1: `docs/GUIDE.md` (9 sections, agent-first,
  compile-gated: `crates/guide-examples` extracts and builds every `rust` block; all ~40
  error-index strings review-verified byte-exact; the whole-guide review's sufficiency
  walk caught the one blocking gap — the app-level sim driver (`Request` builder/
  `SimSource`/`Kernel`) was undocumented — fixed in 60cbd10 before the experiment).
  Phase 2: `examples/invoicing` built by a fresh docs-only sonnet agent (context:
  BRIEF+README+GUIDE only) — **run 1 succeeded: zero restarts, zero framework patches,
  transcript audit clean** (no forbidden reads). Exit criteria, with evidence: 10k
  seeded universes with fault injection green (master seed 0x483e230baed4922b);
  crash-at-any-point never loses an acked write (in-sim acked-durability property under
  the full fault matrix, part of the 10k run; the wire-level kill -9 rig stays M1 per
  spec); zero framework patches (demo commits a513cae..ac5e95a touch only examples/ +
  the members line). The recorded dual-write demo (examples/invoicing/DEMO.md): the
  split-transaction outbox bug is *inexpressible from app code* (routes only see
  `&mut Tx`) — writing it required a temporary framework escape hatch (WRONG c0dbcea);
  the sim caught it in the first 1024-universe batch (24 failed, seed
  0xc9e68010250237f4, repro replayed byte-identical); revert is byte-exact (RIGHT
  bfecd64). Demo-agent friction (examples/invoicing/FINDINGS.md, guide follow-ups):
  no transactional time source documented (agent built a cron-ticked day-counter),
  no id-generation primitive for app rows (built an IdCounter singleton row),
  `Id<T>` Serialize story unclear, `#[index]` on enum fields unshown. The trybuild
  error-message suite (tracked follow-up) should prioritize the macro errors the demo
  agent actually hit.
- **M1 sub-project 1 done — WAL segment management (2026-07-08, spec docs/superpowers/
  specs/2026-07-07-wal-segment-management-design.md).** M1 runs as six sub-projects
  (spec records the decomposition). Shipped: `Disk::sync_dir` directory barrier (fence
  2 held — rotation, recovery cleanup, store creation/rebuild all barriered; sim models
  entry loss/resurrection, `dir_entry_loss`); `Wal::{base_seq, gc}` + checkpoint-driven
  GC (`DbConfig::{retain_segments: 2, checkpoint_every: 4096}`, retention pin for the
  M1 shipper, cadence at the transact funnel); the compacted-log rebuild rule and the
  recovery-horizon rule (fence 1 held: corruption ≤ horizon = named hard error, never a
  cut; vanished prefix base > horizon+1 rejected at `Db::open`; `Engine::recover`
  re-anchored, gaplessness triple-guarded); fault-matrix extensions `bit_flip`/
  `misdirected_write`/`read_error` via field-keyed sub-streams (frozen legacy sampling
  untouched, pinned by test) with meta-tests catching three naive victims. Two Noah
  rulings (spec §4): durable-corruption axes scoped to `"wal/"` (store bitrot is
  v0 truth — redb + M1 restore; M2 engine owns page checksums; the redb COW-fallback-
  defeat finding joins the upstream report), and the above-horizon truth (acked-but-
  uncheckpointed records lost only to media corruption, window = checkpoint_every,
  closed by M1 replication) handled by the fourth public checker
  `checks::durable_corruption_fired` — excuses fire-time-marked corruption universes
  only, consistency oracles never excused. Sim found 2 real findings while landing
  (both rulings above); evidence: every task opus-reviewed (0 unresolved findings), 2x
  fresh `cargo test --workspace` green, pinned seed 0x8bee1752811ae873 green, fresh
  1024-universe invoicing batch green (master seed 0x3e7dfacb98b685bb), clippy/fmt/
  guide gate clean.
- **M1 sub-project 2 done — replication (2026-07-14, spec docs/superpowers/specs/
  2026-07-08-replication-design.md, 10-task plan + final review + fix wave, commits
  f83d786..de17569).** Shipped: per-host sim worlds (host-scoped disks, crash/restart,
  net teardown; global world frozen) + `conn_drop`/`net_spike` fault axes via
  field-keyed sub-streams; verified WAL read-back/batch apply/`NotPrimary` write gate;
  logical snapshot seeder (follower store + WAL base at horizon+1, `next_job_id`
  continuity proven); 14-variant wire codec + durable role file (file wins over
  config, always); the ship/ack/seed/diverge data plane; three-state switchover
  (`Draining` abortable → `HandedOff` point of no return — structurally enforced: the
  abort arm exists only in `Draining`; every role flip durably writes the role file
  before acting) + `promote`; kernel integration (`Kernel::step` extraction — the
  single-node path byte-identical, pinned seeds intact; `Ready::Repl` arm; follower
  gating of jobs/cron; `REPL_TICK_NANOS` park cap; promote rebuilds due-heap/cron
  same-step) + `serve::serve_replicated`/`serve::backup`; `charpente promote`/
  `switchover` CLI verbs (exit 0/1/2, node's words forwarded verbatim). All 8 spec §4
  properties live with real oracles; all five error surfaces pinned by text
  (NotPrimary→421+primary, Diverged both-seqs-both-CRCs, switchover abort, role/config
  mismatch demote-requires-re-seed, version mismatch). The above-horizon IOU from
  sub-project 1 is closed: with a converged follower, WAL corruption on the primary no
  longer loses acked records (property 8, cover-pinned headline path, promised-nonempty
  require). Final opus whole-branch review: READY, 0 Critical; its 2 Importants fixed
  same-day in de17569 (empty-`Frames` peer message panicked a fresh follower — guarded,
  seed 0x2EB1_000F; silent `AwaitHello` conn pinned GC at 0 forever —
  `HELLO_DEADLINE_NANOS` reap, seed 0x2EB1_0010; fix-found hazard recorded: stale
  deadlines folded into `next_deadline` must be self-clearing, control conns excluded
  via `pin_floor`'s `u64::MAX` short-circuit). Evidence: 12/12-stage sweep green
  (replication swarm ×2, four core suites ×2, charpente-sim ×2, pinned
  0x8bee1752811ae873 + kernel 0x348270b38f166529 replays, invoicing 1024 universes
  master seed 0xb278a843d377e615, full workspace + clippy + fmt); demo-of-record: two
  loopback `serve_replicated` invoicing nodes, 50/50 acked curl writes survive
  `kill -9` + `charpente promote` (0 missing on the promoted follower, which then
  accepts writes), promote reply names the DECISIONS §6 bounded-loss window, restarted
  old primary prints the pinned demote/re-seed refusal — transcript in the ledger.
  Follow-up backlog (ledgered in the sub-project close-out entry): typed error kinds at
  the trust-boundary seams (re-Hello/not-a-primary/RESTORE_NEEDED substrings), seed
  0x94a70e2360154b09 explicitly triaged unpinned, swarm inconclusive-rate/runtime
  tuning (~27 min suite), ctl connect-refusal/malformed-reply tests, spec §4 stale
  `fault:conn-drop` token.
- **M1 sub-project 3 done — realtime SSE with exact resume (2026-07-15, spec
  docs/superpowers/specs/2026-07-14-realtime-design.md, 10-task plan + final review,
  commits 14a3684..57a0c94).** Shipped: keyed emits (`tx.emit_keyed`; emit format gained
  `key: Option<String>`, breaking pre-release) materialized at apply into the built-in
  `charpente.events` table (`next_event_id` meta counter, the `next_job_id` pattern —
  ids byte-identical on followers); count-based retention
  (`DbConfig::event_retention: 65_536`) GC'd at checkpoint + `Db::event_floor`;
  `#[event]` (NAME snake_case, Codec + Serialize via `::charpente::serde` with the
  `#[serde(crate)]` redirect); `#[route(SSE …)]` handlers returning typed
  `Subscription` (compile-checked event types, exact-match key filter, session auth via
  Ctx); the `<id>.<seq>` cursor (seq exists for failover honesty — cross-timeline id
  reuse is a named diverged-409, mirroring replication's Diverged); the kernel Tail
  ready-source (ONE delivery path: bounded 64-row table reads whenever behind, no
  live/catch-up modes; drop-on-full — cursor resume IS the backpressure recovery); the
  `Respond` seam (drivers hand the kernel an `EventSink`; SimSink in sim, bounded
  channel + spawn_blocking pump + hand-rolled SseBody at the hyper edge); browser-native
  `Last-Event-ID` resume. Error surface all pinned by text: below-floor 409, both
  diverged 409s, `charpente.gap` terminal frame, cursor-400. WS + TS codegen +
  `events![]` deliberately deferred (spec non-goals). Properties: the flagship
  reconnecting-client oracle (BTreeSet-membership settled() — the T6 review caught the
  original high-water bound silently excusing interior skips; fixed with a skip
  meta-test proven RED against the old oracle), off-by-one + skip discriminating twins,
  full-trace determinism, follower tails + failover resume/divergence multi-node
  property (bounded loss by construction, foreign cursor refused by name). Mid-plan
  interlude (Noah-ruled): T2's GC perturbation exposed a pre-existing replication hole —
  a freshly-seeded follower Hello'd `position: None` and a lossy post-cut primary
  silently re-seeded the SURVIVOR (seed 0xd4d9c59769a6738c); fixed in 0ef5fef
  (SnapshotStart carries the seed frontier CRC, durable meta fallback, diverged-ahead
  refusal holds; adversarially reviewed). Also found (Noah-ruled follow-up, NOT yet
  investigated): debug/release universe replay divergence, seed 0xff159ed76d2806e4 —
  the seed promise is per-build-mode today. Evidence: 14-stage sweep green (realtime ×2,
  replication ×2, core4 ×2, sim ×2, pinned 0x8bee1752811ae873 + 0x348270b38f166529
  replays, invoicing 1024u master 0xae4aec23d8d3707f, full workspace + clippy + fmt);
  live demo-of-record: curl -N tail against the invoicing binary, kill mid-stream,
  `Last-Event-ID` resume delivers exactly the missed frames, garbage/cross-timeline
  cursors print the pinned 400/409 byte-exact (transcript in the ledger). Final review:
  READY, 0 Critical; headline follow-ups ledgered: encode-error delivery is
  whole-node-fatal (documented contract; needs its pinning meta-test + a decision on a
  gentler per-tail-drop shape), T7 pump-linger under silent-disconnect churn, gap-frame
  `id:` line defeats EventSource auto-self-heal (fails loud, never wrong),
  double-buffered sink (~512 effective), the corrupt-frame double-fault window
  (0x820d5e907fbb8b55, fail-stop direction).
- **Mode-skew resolved (2026-07-15, bbeae2e)**: the debug/release replay divergence
  (seed 0xff159ed76d2806e4) was redb's `debug-assertions`-conditional layout, not
  charpente nondeterminism; fixed with a profile override so both build modes share
  one layout, plus a CI tripwire that replays the divergence seed and the crash-recovery
  pin in `--release` on every push. The seed promise is build-mode-independent again.
- **M1 sub-project 4 done — auth (2026-07-16, spec docs/superpowers/specs/
  2026-07-15-auth-design.md, 9-task plan + 2 Noah-ruled interludes, commits
  6ab45ba..3080e83).** Shipped: SHA-256 `hash_token` + branch-free `ct_eq_hex` (T1);
  hashed-at-rest session tokens + lazy expiry as the correctness path (T2); API keys
  (`ck_` + 64 hex, id from the token's first 16 hex chars, revoke-as-delete) (T3); the
  4-rung resolution ladder — ApiKey before Session, JWT-shape never falls through,
  `Principal`/`Via`, the named 401 family pinned byte-exact (T4, two rulings: cookies
  never OIDC-verify; presented-but-invalid 401s everywhere); OIDC static-JWKS RS256
  with per-check named 401s + mount-time config errors (T5, rsa's RNG surface
  deny-listed); the daily session-expiry sweep cron — hygiene, not correctness (T6);
  the replication peer secret — Hello/Ctrl proof checked first, refuse-before-any-
  state-change, constant-time over hash digests, appended to the wire without a proto
  bump, `--secret` on both CLI verbs (T7; review caught the GUIDE/spec rollout-order
  inversion — it's followers-first-then-primary, since the receiver checks and the
  follower dials; refused Hellos never pin GC). Interludes: T6's platform cron shifted
  the fault stream and exposed (a) a latent realtime oracle gap (afba8cc) and (b) a
  pre-existing WAL recovery-cut atomicity bug — orphaned segments now removed before
  truncation, durability fence 3 (9d11540, seed 0x4177819b4a2ba984). Flagship
  negative-space property (T8, tests/auth.rs): raw credentials never touch the durable
  store — two-host rig, 128 universes at base 0x2EB3_0001, media corruption ON, every
  acked token/key + the peer-secret literal scanned against every durable file on both
  hosts; naive-victim meta-test RED-proven against the shared oracle; determinism at
  0x2EB3_5EED via per-host fingerprints. DESIGN §8 sketch→mechanism + GUIDE auth/OIDC
  sections, every quoted error string sourced from its pinning test (3080e83); invoicing
  gained /keys mint+revoke + the authorizes-then-revoked sim (7dd5a2f). Threat model
  stated honestly everywhere: the replication wire is plaintext TCP — the secret
  authenticates peers, it does not encrypt. Evidence: 7-stage close-out sweep green
  (fmt/clippy; full workspace; all seven charpente suites ×2; charpente-sim ×2; pinned
  debug replays 0x8bee1752811ae873 + 0x348270b38f166529; release replays
  0x8bee1752811ae873 + 0xff159ed76d2806e4 per the mode-skew guard; invoicing 1024u
  master 0x4c91fc3370a7d730); live demo-of-record (transcript in the ledger): mint →
  key authorizes → revoke → the named 401 byte-exact, 2s-TTL login expires to its named
  401, wrong/missing `--secret` promote prints the pinned refusal and exits 1, correct
  secret admitted through to role logic. Follow-ups ledgered: the diverged-BEHIND
  re-seed hole (seed 0x752166a16e0d2c99, Important/borderline-Critical, decide_hello's
  below-base Snapshot arm — fix before or at sub-project 5's start), CtrlSwitchover
  secret arm untested end-to-end, `Some("")` behaves as `None`, refused sessions now
  excluded from switchover candidacy (beneficial, noted).
- **Diverged-BEHIND re-seed hole closed (2026-07-16, follow-on interlude; this
  commit).** `decide_hello`'s positioned below-base arm now refuses by name
  (`diverged_behind`, both seqs + both remedies: fresh-data-dir re-seed for a true
  laggard, promote-the-follower if the primary forked) instead of answering Snapshot —
  a primary that recovered behind a converged follower, kept serving, and GC'd its only
  witness can no longer silently rewind the last surviving copy of acked records.
  Noah-ruled Design B: refuse not demote (the primary can't know it's the loser),
  wire-free; Design C (durable timeline epoch at fork points, restores positioned
  auto-heal) ledgered as follow-up. None-position fresh followers still auto-seed
  (late-follower test unchanged). RED-first per hard rule 3: 0x2EB1_0014 (forked-behind
  — refusal + fingerprint + seed-count unchanged) and 0x2EB1_0015 (the benign
  same-timeline laggard, deliberately refused — the previously-uncovered positioned
  case), both proven red on unfixed code and via guard-reverted meta-check.
  `live_lagging_session_pins_gc`'s coda adapted to the fresh-data-dir operator remedy
  (its GC pin/release purpose untouched; adversarially adjudicated as legitimate, not
  weakening). Dossier: .superpowers/sdd/diverged-behind-investigation.md (seed
  0x752166a16e0d2c99 is historical provenance from an abandoned wal-interlude workload
  variant; it replays green — the deterministic tests are the reproduction). GUIDE §10
  gained the operator paragraph for followers offline past the retention window.
  Evidence: replication 27/27 green (~24.5 min under external CPU contention), realtime
  6/6, fmt + clippy -D warnings + guide gate clean; adversarial opus review READY,
  0 Critical/Important (ledgered polish notes: the shared "diverged:" label slightly
  over-claims for the benign laggard; the diverged-family refusals are fragment-pinned,
  not raw-client byte-exact — pre-existing class-wide).
- **M1 sub-project 5 done — migrations as log records (2026-07-19, spec
  docs/superpowers/specs/2026-07-16-migrations-design.md, 11-task plan rev. 2 in
  docs/superpowers/plans/2026-07-17-migrations.md, commits b03d644..285c351).** The
  first implementation plan was REJECTED by a 4-agent adversarial review (dossier
  .superpowers/sdd/migrations-plan-review.md — replay-decode brick, replace_row
  old-value decode, fresh-follower breakage, T7 harness defect) and redesigned around
  three Noah rulings before the rev. 2 plan landed. Shipped: `RowShape` + compile-time
  version chains (`TableMigration`), `#[table(version = N)]`/`#[row_shape(version =
  N)]` (macro-emitted assert-impl const forcing the migration impl to exist, plus a
  structural fingerprint catching a forgotten version bump); the **upgrade barrier**
  paired with the **role-aware post-replay schema gate** (decode-free below-version
  WAL replay during recovery — envelope-only classification, row bytes never touched
  under the wrong version — with the authoritative schema-gate check running
  afterward on the below-version case, per the T11 fix-wave's spec §2
  shipped-ordering amendment); the `MigratePut` log-record kind (Effect tag 4,
  decode-free apply, close-out index rebuild reading only final bytes); the batched
  resumable `Db::migrate_schema` executor (runs at kernel startup when
  writes_allowed / at promote — never on an ordinary follower); adoption-at-first-
  write (a table's `SchemaVersion` row piggybacks on its first data write, same
  disk-op count, leaving frozen fault schedules undisturbed); and
  `Hello.schema_digest` on the replication wire, appended without a proto bump,
  refusing a schema-mismatched peer by name. The error surface: all SEVEN named
  migration errors are test-pinned (the seventh — the in-flight-target-fingerprint
  refusal — was added mid-plan by Noah ruling after a T9 interlude found a foreign v2
  shape could silently resume another binary's migration), plus separately the
  no-adoption-row corruption-class refusal pinned full-string in `fc3854f`.
  Interludes: the T5/T6 boundary deviation (T5's gate and T6's first-write adoption
  turned out mutually load-bearing — T5 alone broke every #[table] app including
  invoicing — landed together as `575ac9d`+`caf94d7`); the T9 interlude `f1b711e`
  (two product-level findings fixed: discard-and-rebuild recovery bricking
  validly-migrated stores under the binary that migrated them, fixed by Candidate
  B's decode-free replay keep-barrier; the cross-fingerprint in-flight resume
  refusal above); and T9's third finding — the horizon-collapse silent-loss bug
  (seed `0x6b436e3ea995cb6f`, a discard-and-rebuild that loses the applied_seq
  horizon anchor and lets a corrupted first WAL frame silently truncate a whole log
  to an empty store) — pre-existing since sub-project 1, not migrations-specific,
  ledgered as the headline follow-up (dossier
  .superpowers/sdd/horizon-collapse-investigation.md, Noah-ruled 2026-07-19: Design
  A, a durable horizon breadcrumb, scheduled after T11). T10 proved
  replicated-migration properties (seeds `0x2EB8_0002`..`0004`): a fresh v2 follower
  seeds across the gate, a converged v1 follower catches up via shipped MigratePuts
  only, and promote-mid-migration resumes from the shipped watermark identically to
  an uninterrupted twin. T11 landed DESIGN §3 + GUIDE §12 (guide compile gate green)
  and the invoicing add-a-field demo (`501c5c7`, ZERO guide friction — the
  docs-sufficiency experiment's best outcome, 256-universe sim master seed
  `0x6041349437974712`), the final whole-branch opus review (READY, 0 Critical over
  17 commits), followed by the same-day fix wave `285c351` (spec §2
  shipped-ordering amendment, the close-out guard promoted debug_assert→assert per
  hard rule 4, a scan_raw_from u64::MAX brick guard + pin, and the plan's
  six→seven-errors text fix), and the kernel-pin re-verification (worktree-isolated
  revert-guard proof): outcome PIN-EXERCISES-BUG — pin seed `0x348270b38f166529`
  still exercises its historical bug post-migrations. Follow-ups ledgered: the
  horizon-collapse Design A fix (ruled, next up); `Db::poisoned()` reflects only
  apply-side poison, not redb read-side poison (needs its own investigation); and
  the final review's accepted minor-triage items.
- **Horizon-collapse Design A fix landed (2026-07-24, interlude commits 3c800c3 +
  d7d7116).** The durable horizon breadcrumb: a two-fixed-slot CRC'd sidecar
  (`<store>.horizon`) written+barriered at `Db::checkpoint`/`Seeder::finish` strictly
  after the store's own checkpoint persists; `Db::open` feeds
  `max(store.applied_seq, breadcrumb)` into recovery, so a store discard-and-rebuild
  can no longer silently truncate a whole log below a prior checkpoint (seed
  0x6b436e3ea995cb6f's class now refuses by name). Torn overwrite preserves the prior
  valid witness (40-crash-point sweep at crash_torn 1.0); `Seeder::new` clears a stale
  prior-timeline breadcrumb (RED-proven necessary); the residual
  above-last-checkpoint window stays silent per the ruling (normal-recovery parity);
  `.horizon` is documented as part of the node's durable restore set (DESIGN §2).
  Ruled landing order: the realtime flagship oracle first gained retention-gap
  awareness (3c800c3 — a pre-existing harness race where `sync_ledger` silently
  orphans a live-delivered id whose events row is retention-GC'd before the scan
  reaches it, exposed by the breadcrumb's schedule shift at seed 0xbe2f0aa2f20f34c7;
  the gone-arm confirms received-only ids, with a discriminating meta-test).
  Evidence: 9/9 final-bytes sweep (fmt, clippy, guide gate, full workspace incl.
  replication, 4 debug seed replays, both release tripwires, invoicing 1024u master
  0x25f8da922cb6441b); whole-interlude opus review clean (0 Critical,
  .superpowers/sdd/interlude-review-e978948-d7d7116.md), its one Important
  discharged same-day: kernel pin 0x348270b38f166529 revert-guard re-verified
  PIN-EXERCISES-BUG at d7d7116 (3 historical tests red incl. the pin itself,
  identical dispatch.rs:158 panic; restored 25/25).
- **M1 sub-project 6 OPEN (2026-07-24)** — introspection polish + the edge harness.
  Spec approved (all 8 §6 rulings adopted as recommended, 1f76801) plus 3 Noah-ruled
  pre-flight amendments (ea7fe4c): the Contract's events section is dropped in v0
  (event NAMEs are provably unreachable without a macro change — SSE visibility via
  `stream: true`); an empty-string peer secret becomes a named config error (the
  ledgered "behaves as None" claim was disproven — `Some("")` admits any peer
  presenting no proof); the harness vehicle is a committed
  `examples/invoicing/src/bin/node.rs`. 14-task plan at
  docs/superpowers/plans/2026-07-24-introspection-edge-harness.md (three phases:
  describe, error-quality suite, edge harness; adversarial opus plan review gating
  execution).
  **Execution state (2026-07-25, HEAD 9c111b1): 9 of 14 tasks closed.** The
  authoritative running record is the ledger
  `.superpowers/sdd/2026-07-24-introspection-edge-harness/progress.md` — this entry
  is a pointer until T14 writes the close-out. Phase A (describe, T1–T5) complete;
  Phase B (error quality, T6–T9) complete at 93bcc56. T10 (edge-harness crate +
  `examples/invoicing/src/bin/node.rs`) is in flight: implementation 39b0b37, opus
  review raised 5 Importants, fix round 1 landed 9c111b1 (restart child-identity,
  client timeouts, SSE timeout-vs-EOF, signal asserts) together with Noah's
  2026-07-25 ruling un-ignoring the 10 zero-IO edge tests — which amends plan ruling
  4's blanket `#[ignore]` wording, a docs debt T14 still owes. T10 is NOT closed: the
  ledger has no `Task 10: complete` line and the fix-round diff
  `review-516a83d..9c111b1.diff` has no verdict file. Two commits between T9 and T10's
  fix round (f5cfc98, 516a83d) are not plan tasks — they are the Noah-approved
  `sim-disk-fastpath` performance work (~2.6x on the sim suite), cherry-picked onto
  main with replay identity proven.
- Next: adjudicate T10's fix-round review and write its closing ledger line, then
  T11 (acked-implies-durable wire oracle + the ack-before-commit teeth meta-test),
  T12 (promote/failover + realtime-resume wire oracles), T13 (named hunts — the
  read-side-poison hunt HALTS for a Noah ruling at its characterization), T14 (final
  review, close-out sweep, M1 COMPLETE, push). Local `main` is 17 commits ahead of
  `origin/main`; the push is deliberately deferred to T14.
