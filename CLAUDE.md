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
  old primary prints the pinned demote/re-seed **notice** (a role/config-mismatch
  stderr line — it is not a refusal, and sub-project 6's T12 shows that a restart with
  the *original* primary config prints nothing at all: the dual-primary gap in the M2
  backlog below) — transcript in the ledger.
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
- **M1 sub-project 6 done — introspection polish + the edge harness (2026-07-27, spec
  docs/superpowers/specs/2026-07-23-introspection-edge-harness-design.md (1f76801) + 3
  Noah-ruled pre-flight amendments (ea7fe4c), 14-task plan
  docs/superpowers/plans/2026-07-24-introspection-edge-harness.md (8032611) +
  adversarial-review amendments (66b3673), commits 66b3673..6822554 in three phases plus
  this close-out commit; the authoritative task-by-task record is the ledger
  `.superpowers/sdd/2026-07-24-introspection-edge-harness/progress.md`).**
  **Phase A — `charpente describe` (T1–T5, 9a1b9d4..ffc5ca4).** `App::describe()` is a
  pure walk of a mounted app's own registries returning a `Contract` — tables (version,
  structural fingerprint, indexes, migration chain), routes (method, pattern, name, auth,
  `stream`), jobs (retry policy), cron, config — with `to_json`/`to_human` renderers and a
  `builtin: bool` keyed on each row's own `TABLE`/`NAME` const separating the platform's
  surface from the app's. Two entry points: `charpente::launch(addr, data_dir, build)`
  (the standard app `fn main`, so `describe` is free the moment an app adopts it; an
  unknown verb is a usage error, exit 2, never a silent serve) and `charpente describe
  [--dir PATH] [--format json|human] [--bin NAME]`, which builds any app crate from the
  outside and execs it through the same argv path. Both run against a throwaway temp dir,
  never the app's `data_dir`, so a describe is safe next to a live server. The contract is
  pinned three ways (invoicing golden, the CLI cross-check mechanically tied to it — the
  same 5329-byte literal in both, re-verified by the final review — and a determinism test)
  plus
  GUIDE §13. Event NAMEs are deliberately absent (ruling 6: unreachable without a macro
  change; `stream: true` is realtime's only trace in the contract).
  **Phase B — error quality (T6–T9, 447d7f1..93bcc56).** The trybuild compile-fail suite
  (9 `.stderr` pins + 2 pass regressions) over the macro errors the M0 demo agent actually
  hit; `checks::classify`/`FaultReason` productized out of four hand-rolled `is_injected`
  copies, its first-match arm order load-bearing and pinned; every named replication error
  string byte-exact (diverged family, NotPrimary, role, re-Hello marker) plus the
  `Some("")` peer-secret named config error (the ledgered "behaves as `None`" claim was
  disproven — it admits any peer presenting no proof); the realtime encode-error
  whole-node-fatal pinning meta-test (a hard-rule-3 gap closed); binary-level
  `charpente-cli` ctl tests.
  **Phase C — the edge harness (T10–T13, 39b0b37..6822554).** `crates/edge-harness` plus
  the committed `examples/invoicing/src/bin/node.rs` vehicle: child control (kill9,
  pause/resume, restart with child-identity asserts), a TCP fault proxy (sever, refuse,
  delay), a blocking HTTP/1.1 + SSE client, and the scenario script as the repro artifact —
  no seed promise, the §4.4 honesty contract now in DESIGN §7. **Three wire oracles, all
  green against stock:** (1) *acked-implies-durable* across `kill -9` + restart on the same
  data dir, with the ack-before-commit teeth meta-test driving a committed wrong-build
  patch in a throwaway worktree and demanding the oracle go red by name — plus the positive
  control (stock binary through the wrong-build wiring, asserted `Ok`) that closes the
  "the redness proves nothing" hole; (2) *promote/failover zero loss over CONVERGED writes*
  (ship-ack barrier, `MIN_CONVERGED` 40 of 50, the real `charpente promote` CLI asserted
  for exit 0 and the DECISIONS §6 window text, the non-converged tail reported and never
  folded into the violation, with a deterministic test that forces the tail branch by
  cutting the peer link at round 49); (3) *exact `Last-Event-ID` SSE resume* across a
  genuinely mid-stream kill, exactness enforced four ways (set-equality with the missing
  set, no intersection with what was consumed pre-kill, strict in-stream id monotonicity,
  superset). T13's three named hunts: four `serve.rs` #25 probes (no hangs, no crashes; the
  two absent contracts — no `header_read_timeout`/connection budget, no write/idle timeout —
  RECORDED not asserted, since asserting one would be this task deciding #25),
  `charpente switchover` end-to-end including the `CtrlSwitchover` secret arm (closing that
  ledgered item), and the `Db::poisoned()` hunt, whose brief-predicted mechanism was
  **disproven by measurement** and the disproof itself pinned. **Two gaps found,
  characterized, and ruled DEFERRED TO M2 (Noah, 2026-07-26) rather than fixed in a
  close-out task** — both carried in the M2 backlog below, each behind a test that turns
  red the day a fix lands. Kill -9 honesty is stated everywhere it matters (crate doc,
  DESIGN §7, GUIDE §8): process death does not drop the OS page cache, so the rig proves
  ack *ordering* plus end-to-end crash recovery through real hyper/TCP, and power-loss /
  torn-write durability stays the simulator's claim alone.
  Two commits in the range are not plan tasks: `f5cfc98` + `516a83d`, the Noah-approved
  `sim-disk-fastpath` performance work (~2.6x on the sim suite), cherry-picked onto main
  with replay identity independently proven twice.
  Final whole-branch opus review (`final-review.md`, dc87dec..6822554 — 35 commits, 72
  files, ~11.5k insertions): **READY TO MERGE, 0 Critical**, conditional on four record
  corrections, all landed in this close-out (I-1 DESIGN's promote-path dual-primary claim —
  a shipped spec sentence the branch's own green test disproved; the "only natural end of
  the window" claim scoped to the cache-resident shape at both sites; `hunts.rs`'s stale
  "HELD for a ruling"; the unqualified `begin_write` mechanism). Its verdict on the three
  oracles, each judged as a skeptic asking "can this be green while the guarantee is
  false?": oracle 1 not in the class it claims and the claim is stated honestly ("the
  anti-vacuity layering is the strongest I have seen in this repo"), oracle 2 "I could not
  construct a way", oracle 3 only in bounded, disclosed ways. The close-out also discharged
  the docs debt T6–T12 had routed here (the `misdirected_write`-is-not-a-wording correction
  in `checks`, the four-vs-six checker census in GUIDE §8 and DESIGN §7, the GUIDE
  `Some("")` refusal, the GUIDE promote-wipe warning, the replication spec's
  `fault:conn-drop`→`conn_drop` token, plan ruling 4's blanket `#[ignore]` wording, and two
  stale censuses of the edge crate's un-ignored set — all now stated as criteria, not
  counts).
  Evidence — 9/9 final-bytes close-out sweep, every stage green (spec §8; full log archived
  at `.superpowers/sdd/2026-07-24-introspection-edge-harness/task-14-sweep.log`): `cargo
  fmt --all --check` clean; `cargo clippy --workspace --all-targets -- -D warnings` clean;
  `cargo build -p guide-examples` (the GUIDE compile gate) clean; `cargo test --workspace`
  **twice** — 48 test binaries, **506 passed / 0 failed / 31 ignored** both times (10m18s
  and 9m25s; the replication suite's 32 tests dominate at 478s / 454s); debug pinned
  replays `CHARPENTE_SEED=0x8bee1752811ae873` (charpente-sim crash_recovery, 4/4) and
  `0x348270b38f166529` (charpente kernel, 26/26, including the new latch
  characterization); the two release mode-skew tripwires exactly as `ci.yml` runs them,
  `0x8bee1752811ae873` (release crash_recovery, 4/4) and `0xff159ed76d2806e4` (release
  `late_follower_seeds_below_the_gcd_base_and_converges`, 1/1); a fresh invoicing
  1024-universe batch, **all green, master seed `0x943560695b136113`**; and the full edge
  matrix `scripts/edge-harness.sh` — **31/31 `#[ignore]` scenarios green in 49s**, which is
  the demo-of-record: all three wire oracles plus the teeth meta-test, both known-gap
  characterizations, the four hunt probes, the switchover secret arm, and the smoke
  scenario, against stock binaries. Crate census after the close-out (observed, not
  quoted): edge-harness **55 un-ignored / 31 ignored, 86 total** — the final review's
  55/30/85 plus this task's one new `Scenario` test.
- **M1 COMPLETE (2026-07-27).** Both PLAN M1 exit criteria are discharged, each with an
  oracle at two layers: *a follower promoted mid-write-load loses nothing and serves reads
  throughout* — in-sim property 8 (sub-project 2) plus the wire oracle above, over
  converged ids, with the acked-but-unconverged tail named as DECISIONS §6's window rather
  than hidden; *a reconnecting realtime client provably misses no events* — the in-sim
  membership oracle with its skip/dup discriminating twins (sub-project 3) plus the exact
  `Last-Event-ID` wire oracle above. Six sub-projects (WAL segment management, replication,
  realtime SSE, auth, migrations-as-log-records, introspection + the edge harness) plus
  three ruled interludes (mode-skew, diverged-BEHIND, horizon-collapse Design A). **Named
  deferrals, so the claim is not read wider than it is:** WebSockets and TS client codegen
  (spec non-goals, sub-project 3), object-storage backup (`serve::backup` is a promotable
  follower, not a snapshot shipper), and multi-tenancy as typed key prefixes, which DESIGN
  §9 already carries as its own future design. None of the three gates an exit criterion.
- **M2 sub-project 0a done — the checkpoint-latch gap CLOSED (2026-07-31, spec
  docs/superpowers/specs/2026-07-30-store-latch-poison-design.md, 4-task plan
  docs/superpowers/plans/2026-07-30-store-latch-poison.md, ledger
  .superpowers/sdd/2026-07-30-store-latch-poison/progress.md, commits e57e62c..this
  commit).**
  The first M2 backlog item, retired from the backlog below. The gap: charpente had two
  independent "storage is broken" flags and saw only one, so a node whose checkpoint fsync
  failed gave a 500 to a record that **was** durable, reported `poisoned() == false`, kept
  serving, and could never checkpoint again — freezing WAL GC and the `.horizon`
  breadcrumb behind a checkpoint that would never succeed.
  **Mechanism:** `DiskBackend` sets a sticky `Arc<AtomicBool>` whenever its inner `File`
  call returns `Err` — precisely redb's own condition, verified site-by-site against
  `CheckedBackend` (`cached_file.rs`: `len`/`read`/`set_len`/`sync_data`/`write`, each an
  unconditional `if result.is_err()`; `check_failure` short-circuits before touching the
  backend, so the flags cannot drift after latching). `close` is deliberately **not**
  mirrored: it runs only from `Database::drop`, so a flag set there could never be read by
  a live `Store`. Exposed as `Store::failed()`, OR-ed into `Db::poisoned()` via
  `Db::store_failed()`, so the kernel's existing request-arm stop (the 2026-07-05 ruling)
  fires with no new control flow, no new durable state, and no protocol change. The cause
  stays distinct in the stop message because the two imply opposite things about durable
  state — a store latch leaves the WAL intact and authoritative; apply-poison means
  durability became unknowable — and both strings are pinned byte-exact.
  **Policy fork ruled (a): stop on the latch**, not (b) serve degraded reads behind a
  `Db::degraded()`. On a large store (a) is the only thing that ends the state; on a small
  one it moves an already-certain stop earlier.
  **The prose coupling is load-bearing, not decorative.** The stop message carries redb's
  own remedy prose ("close and re-open the database") verbatim, because `checks::classify`
  matches message *strings* and spec §8 makes touching it a non-goal. The plan's paraphrase
  hit none of `classify`'s four needles and silently classified every fault-caused latch
  stop as `Unrelated` — 212 invoicing universes plus 7 suite tests. Guarded from the
  message side by a rationale comment at the site and an `is_fault_fallout` assertion in
  the tripwire, so a reword reddens locally. This is now a **second** deliberate prose
  coupling; spec §1 says so (its earlier "adds no new one of any kind" claim was false as
  shipped and is corrected).
  **The correspondence assertion** — what makes the structural-coupling trade safe — lives
  in `kernel_stops_on_a_latched_store` (`crates/charpente/tests/kernel.rs`, seed
  `0x0DED_0002`). That is the former `kernel_keeps_serving_with_a_latched_store_known_gap`,
  **updated and renamed exactly as the retired entry instructed, never deleted**; it is
  un-ignored, so it runs in every `cargo test --workspace`. It takes **two** assertions:
  `Db::poisoned()` (charpente's mirror) and a `Db::checkpoint()` that must fail carrying
  redb's own `PreviousIo` prose. Either alone cannot tell a correct latch from a drifted
  one. **Scope it honestly:** both ride the checkpoint-fsync path, so together they pin
  drift on *that path* in either direction — including the over-approximation, where a redb
  that stopped latching there would otherwise leave charpente poisoning healthy nodes into
  permanent unavailability with the suite green. A future redb growing an entirely **new**
  latch path that `DiskBackend` never sees is **not** caught mechanically; that stays a
  redb-upgrade review item against the five-site census (DESIGN §4, spec §1).
  **The DERIVED caveat is kept, not promoted.** Two shapes share the fix but not their
  evidentiary standing: the **cache-resident** store is **MEASURED**; the store larger than
  redb's 1 GiB evicting read cache is **DERIVED, not measured** — its premises are verified
  in redb's source (`begin_read` is not latch-gated in either impl, so a read-first write
  transaction fails inside the handler and never reaches the latch-gated `begin_write`),
  but no store over that bound was ever built. Do not promote that row without measuring
  it. Incidentally re-confirmed while proving the tripwire's teeth: swapping its probe to
  `snapshot()` reddens *because the snapshot succeeds* on a latched handle.
  **Review** (`5e6412e..03a2158`, opus): READY with fixes, 0 Critical. Both Importants were
  test-integrity gaps — assertions that could not fail for the reason they stated while
  DESIGN and the spec claimed they could — fixed same-day in `55ae976`: the correspondence
  assertion had been reading only charpente's own flag (the over-approximation direction
  entirely unpinned, and the repo's only live sighting of redb's `PreviousIo` prose lost
  with the old tripwire's tail), and `a_graceful_close_does_not_latch` was structurally
  vacuous because `Store::open` mints a fresh flag (`table.rs:307`), so any re-opened store
  reads clean by construction. The close guard moved one layer down to
  `latch_tests::a_close_does_not_latch` against `DiskBackend` itself, proven red against a
  faithful mutation of redb's own close shape (latch, then return `Ok`). GUIDE §9's
  poisoned-engine section carries the operator paragraph: committed data is safe, the
  remedy is a process restart, and a restart on still-failing hardware will latch again.
  **Evidence** — 11-stage sweep, every stage green (archived at
  `.superpowers/sdd/2026-07-30-store-latch-poison/task-4-sweep.log`): `cargo fmt --all
  --check` clean; `cargo clippy --workspace --all-targets -- -D warnings` clean; `cargo
  build -p guide-examples` (the GUIDE compile gate) clean; `cargo test --workspace` **three
  times**, all green — 49 test binaries, **515 passed / 0 failed / 31 ignored** (514→515 is
  this sub-project's one new test, `a_close_does_not_latch`; the first pass predated a
  comment-only edit to `hunts.rs`, so passes 2 and 3 are the two covering the final bytes);
  pinned debug replays `CHARPENTE_SEED=0x8bee1752811ae873` (charpente-sim crash_recovery)
  and `0x348270b38f166529` (charpente kernel); both release mode-skew tripwires exactly as
  `ci.yml` runs them (`0x8bee1752811ae873` release crash_recovery, `0xff159ed76d2806e4`
  release `late_follower_seeds_below_the_gcd_base_and_converges`); a fresh invoicing
  1024-universe batch, **all green, master seed `0x58f5254bad001f64`**; and the edge matrix
  `scripts/edge-harness.sh`, **31/31 `#[ignore]` scenarios green** — run because T3 touched
  replication behavior. Note for whoever runs the next sweep: pipe cargo through `tee`, not
  `tail`. `tail` eats the aggregate counts *and* replaces cargo's exit status with its own,
  which is how the T1 sweep lost its counts and how this one nearly did.
- **M2 sub-project 0b done — the dual-primary gap CLOSED (2026-08-05, spec
  docs/superpowers/specs/2026-07-31-dual-primary-design.md (`cf80011`), 6-task plan
  docs/superpowers/plans/2026-07-31-dual-primary.md (`59746d1`) + two amendments
  (`2627fa7` the witness format's count trailer + address validation, `3da95b5` marking
  Task 1's pre-amendment code blocks historical), ledger
  `.superpowers/sdd/2026-07-31-dual-primary/progress.md`, commits `cf80011`..this commit).**
  The last *ruled follow-up* carried out of M1, now retired from the backlog below.
  **The gap:** a primary dies, an operator promotes the follower, and later the old box is
  restarted **exactly as it was configured** — same `--role primary`, same data dir. The
  durable role file says `Primary` and always wins over config, and `role.rs` emits a notice
  only when config *contradicts* the file, so it resumes as a primary in total silence.
  Neither node dials the other (a durable `Primary` has no follower client), so nothing on
  either side could observe the conflict: two live, writable, mutually invisible primaries on
  diverged timelines, the resurrected one serving stale reads **and acking new writes** whose
  mandated remedy (fresh data dir + re-seed) then destroys them. Acked-write loss reached
  through an ordinary "restart the old box the way it was" action.
  **Mechanism — the witness set.** A **witness** is a node this primary has shipped to, and
  the check is conclusive on its own: *if any node I shipped to now answers as a primary, I am
  not the primary any more*. No wire change, no new protocol field, no proto bump — `Hello`
  already carries the sender's listen address and the primary already holds it per session;
  this persists what was already in memory. Durable form is a two-slot CRC'd `witnesses` file
  in the data dir, sibling to `role`, written with `role.rs`'s exact discipline (`write_at` +
  `set_len` + `sync` + `sync_dir`) with generation, count and a format marker inside the CRC,
  so a torn write can neither brick startup nor parse as a shorter — and therefore *less
  safe* — set. Written when a ship session's peer `listen` becomes known, append-if-new: one
  durable write per **new** peer, so steady-state cost is zero. Cleared on any durable role
  flip away from `Primary`, and `promote` clears the stale set **before** the durable flip.
  **The gate moved, and that is the fix.** `mod.rs` called `db.allow_writes()` inside the
  `match &role` block, before the follower client was even constructed — writes were enabled
  strictly before any probe could run, so a check bolted on afterwards would not have been a
  fix. A durable `Primary` with a non-empty witness set now starts at `forbid_writes` and is
  promoted to `allow_writes` only when *every* witness has answered "not a primary". An empty
  witness set still resumes immediately and silently: single-node and never-shipped primaries
  are completely untouched, and that path is pinned.
  **A third write-gate state.** `Db` modelled the gate as `not_primary: Option<String>` →
  `TxError::NotPrimary { primary }` → 421 naming where to go instead. A witness-gated node has
  **no primary to name** — it does not know whether one exists and may itself still be the
  legitimate primary — so 421 with any address would be a lie and with its own would be a
  loop. The gate became three-valued (`Allowed`/`NotPrimary(addr)`/`Verifying`) with
  `TxError::Verifying { unreached }` rendering as **503**, the honest status for a transient
  state the node expects to leave on its own. A pre-release breaking addition to the public
  `TxError`, in the same class as realtime's `emit` format change; the existing
  `NotPrimary` → 421 pin is byte-unchanged.
  **The probe is a purpose-built classify-and-disconnect client, deliberately NOT
  `Follower::new_resolving`** — worth stating so it is not "simplified" back later. `Follower::
  step` classifies the first response and then *falls through to the normal handlers*:
  `Msg::Frames` reaches `apply_and_ack`, `SnapshotStart` opens a seed. For a resurrected
  primary that would silently auto-heal a forked node by adopting the winner's timeline, which
  is exactly what the 2026-07-16 diverged-BEHIND interlude ruled against. The probe sends
  `Hello`, classifies the first response, tears the connection down, and never applies a frame
  or opens a seed.
  **Ruling (Noah, 2026-07-31): option (c)** — after `WITNESS_DEADLINE_NANOS` with witnesses
  still unreachable, serve reads, withhold writes, keep probing, and emit one loud notice
  naming the unreached witnesses and the remedy. The notice is not a substitute for the gate;
  it is how the operator learns why writes are refused. `ReplConfig::strict_witnesses`
  (default `false`, shipped in `5af32f5`, documented in `bb91ff4`) is the opt-in alternative
  for operators who prefer a node that fails to start over one serving reads it cannot write
  to. **The honest cost, stated in GUIDE §10 rather than hidden:** a primary whose only
  follower is permanently decommissioned will not accept writes until an operator edits or
  deletes the `witnesses` file — reads stay up — and the guide carries the explicit warning
  that clearing that file re-opens exactly this gap. Retention is conservative on purpose: a
  decommissioned node *could still have been promoted* before it was decommissioned.
  **What the reviews caught** (every task opus-reviewed; each closed its fix round at 0
  Critical / 0 Important): Task 5's I-1 was a **real product defect** — every non-`Pending`
  verdict was terminal, but `Inconclusive` is precisely the verdict meaning "I learned
  nothing" and absorbs `decide_hello`'s whole prefix ladder including the schema digest, so
  one inconclusive answer stranded the gate shut **permanently and silently** while three
  shipped strings promised it would resolve itself (fixed in `87cd734`: an inconclusive
  witness is asked again). Task 5's I-2 is the counter-example worth keeping: the review's
  recommended fix was implemented, **measured, and reverted** — the new cover key fired in 0
  of 256 universes and inconclusive was 127 with the machinery and 127 without, byte for
  byte. The machinery was inert; measurement disproved the model behind it.
  **Known, documented at the time, closed 2026-08-06:** a `Verifying` node was still a live
  replication peer that could seed brand-new followers while withholding its own writes —
  manufacturing a durable replica of a timeline it had not confirmed it owned. Task 6's
  interlude built a first attempt at the refusal (`Db::verifying()` + a `Session::handle`
  arm), measured it, and **reverted it** on the re-review's C-1 below (Minor 5, judged new
  policy needing a ruling, not a Minor closure). The "M2 interlude — a verifying node's peer
  policy" entry later in this file is that ruling, landed for real: seeding a newcomer is now
  refused while catch-up to an already-positioned peer is not.
  **Tests** (seeds `0x2EB1_0102`..`0x2EB1_0118` in `crates/charpente/tests/replication.rs`,
  RED-first per hard rule 3): the headline `0x2EB1_0111` asserts **on the gate, never on a
  message** — a notice an operator may or may not read is not what keeps a second primary from
  acking writes — plus refusal-by-name when a witness answers as a primary (`0x2EB1_0112`),
  the silent empty-set path (`0x2EB1_0113`), unreachable-serves-reads-withholds-writes
  (`0x2EB1_0114`), `strict_witnesses` (`0x2EB1_0115`), promote-clears-before-the-flip
  (`0x2EB1_0117`), and the kernel completing its skipped startup work when the gate opens
  (`0x2EB1_0118`). The edge tripwire was **updated and renamed, never deleted**:
  `failover_known_gap_silent_dual_primary_resurrection` →
  `failover_resurrected_primary_does_not_accept_writes`, flipping from characterizing the gap
  to proving the fix (Phase A: witness unreachable → reads 200, writes 503 by name; Phase B:
  witness answers as a primary → refuse by name and stop the kernel). It is `#[ignore]`, so
  its red appears only under `scripts/edge-harness.sh`, never in `cargo test --workspace`.
  **Evidence** — 11-stage sweep, every stage green (logs in the session scratchpad; `tee`,
  never `tail`): `cargo fmt --all --check` clean; `cargo clippy --workspace --all-targets --
  -D warnings` clean; `cargo build -p guide-examples` (the GUIDE compile gate) clean; `cargo
  test --workspace` **twice**, identical both times — 49 test binaries, **570 passed / 0
  failed / 31 ignored** (354s, 353s). The 569→570 delta against the ledger's `87cd734`
  baseline is reconciled: `5af32f5` added exactly one test. Pinned debug replays
  `CHARPENTE_SEED=0x8bee1752811ae873` (charpente-sim crash_recovery, 4/4) and
  `0x348270b38f166529` (charpente kernel, 26/26); both release mode-skew tripwires exactly as
  `ci.yml` runs them (`0x8bee1752811ae873` release crash_recovery 4/4,
  `0xff159ed76d2806e4` release `late_follower_seeds_below_the_gcd_base_and_converges` 1/1); a
  fresh invoicing 1024-universe batch, **all green, master seed `0x4e5f579b23852b2f`**; and
  the edge matrix `scripts/edge-harness.sh`, **31/31 `#[ignore]` scenarios green in 47s**,
  including the renamed tripwire. Note for the next sweep: the edge crate's ignored tests live
  in `src/` as well as `tests/` — grepping only `tests/` undercounts them by more than half.
- **Pushed, and CI is green (2026-08-06).** `e5c7c0c..98c541f`, 29 commits — the metered run
  deferred through M1's final sub-project and both M2 sub-projects. GHA `ci` succeeded in
  22m01s on the first attempt, so the local-first policy held: nothing reached the remote
  that the local sweeps had not already caught.
- **Hard-rule-5 interlude done (2026-08-06, this commit)** — the two rule-5 items accepted
  but not fixed by sub-project 0b's Task 5 review, closed together because both live on the
  same operator-facing surface.
  **(1) An undecodable witness response no longer kills the node.** `WitnessProbe::step`
  took a bare `?` on `decode`, so a reply this node could not parse became an `io::Error`
  that propagated through `Repl::step` and out of `Kernel::run` — anything holding a
  witness's address (a half-open port, a wrong-protocol service, a corrupted reply) could
  take a resurrected primary down with eight bytes of garbage, reported as a decode fault
  rather than as the thing an operator must act on. **Noah-ruled 2026-08-06:** classify it
  `Inconclusive` rather than name-and-stay-fatal. Undecodable is not a verdict, it is the
  absence of one, and spec §2's trichotomy files "unknown" with unreachable — back off and
  ask again. This extends ruling (c) to an epistemically identical case rather than
  inventing a policy, and it makes the hard-rule-5 gap moot instead of papering over it: the
  reason now reaches the operator through the deadline notice. Payload bounded and escaped
  via `bounded_witness_message`, like every other peer-derived string in the module.
  **(2) The notice's `(inconclusive: …)` variant is pinned.** Its `(no answer yet)` sibling
  was pinned byte-exact from the start; the variant carrying the *reason* — the one an
  operator actually acts on — was pinned nowhere.
  **Deliberate asymmetry, recorded so it is not "fixed" later:** `Follower::step`
  (`follower.rs:219`) still takes the bare `?`. A follower's primary is its source of truth
  and a dead follower is not a dead primary; a probe's witness is only being asked a
  question. Whether the follower should get the same treatment is a **new follow-up**, not
  part of this ruling.
  Tests: `0x2EB1_0119` (survival + gate stays shut + keeps asking + the notice text) and
  `0x2EB1_011A` (the inconclusive variant against a `Refuse` on `classify`'s default arm).
  RED-first per hard rule 3 — (1) failed for exactly the right reason before the fix
  (`"p step: repl message decode: unknown tag 255 …"`, the node dying). (2) pins shipped
  output and so passed on arrival, **so its teeth were proven by mutation instead**:
  perturbing `unresolved_witnesses`'s inconclusive arm turns BOTH pins red, and `mod.rs` was
  restored byte-exact afterwards. GUIDE §10's third probe outcome and `probe.rs`'s module
  doc both name the undecodable case (hard rule 6).
  Evidence — the same 11-stage sweep, every stage green: fmt, clippy `-D warnings`, guide
  gate; `cargo test --workspace` **twice**, 49 binaries, **572 passed / 0 failed / 31
  ignored** both times (570 + these two tests); both pinned debug replays
  (`0x8bee1752811ae873` 4/4, `0x348270b38f166529` 26/26); both release mode-skew tripwires
  as `ci.yml` runs them (4/4, 1/1); a fresh invoicing 1024-universe batch, all green, master
  seed `0x20f0591d1069879b`; and `scripts/edge-harness.sh` **31/31**, run because this
  touches replication behavior.
- **M2 interlude — a verifying node's peer policy, CLOSED (2026-08-06, spec
  docs/superpowers/specs/2026-08-06-verifying-peer-policy-design.md, 4-task plan
  docs/superpowers/plans/2026-08-06-verifying-peer-policy.md, ledger
  .superpowers/sdd/2026-08-06-verifying-peer-policy/progress.md, commits `60be3f6`..this
  commit).** Closes Minor 5 from sub-project 0b's Task 5 review (recorded just above): that
  review judged the gap real but accepted it as unfixed pending a ruling, and a first attempt
  (Task 6's interlude) was built, measured, and reverted rather than shipped without one.
  **The gap:** `begin_seeding` is read-only, so 0b's write gate never saw it — a `Verifying`
  node remained a full replication peer that would **seed a brand-new follower** on request.
  Manufacturing a fresh durable replica of a timeline this node has not confirmed it owns is
  acting as the primary in the most consequential way short of acking a write, and unlike a
  write that replica **outlives this node's own fail-stop** once a witness finally answers
  "primary" — a gated `P` could seed newcomer `G`, then have its probe reach the real primary
  and refuse-and-stop, leaving `G` holding a durable replica of a dead fork with nothing alive
  left to say so.
  **The policy (spec §1):** a `Verifying` node may not render a seed to a peer that holds
  nothing and is not one of its recorded witnesses; everything else it does as a peer —
  reads, catch-up, control verbs — is unchanged. **The keying (spec §2, Task 3, `ee17b8c`):**
  `db.verifying() && position.is_none() && !witnesses.contains(listen)`, checked inside
  `decide_hello`'s shared `None`-position arm rather than on the `Decision::Snapshot`
  variant — the parked attempt from 0b keyed on the variant and was silently inert on a young
  primary that answers `Frames { from: 0 }` instead. Both conjuncts are load-bearing:
  `position.is_none()` alone is not "brand-new peer" (a **recorded witness** re-seeding from
  an empty data dir — GUIDE §10's own remedy, and what resolves the gate — also Hellos with no
  position), and membership alone would refuse ordinary catch-up to a positioned follower not
  yet recorded. **Retryability (spec §3, Task 2, `b126beb`):** a new marker,
  `VERIFYING_WITNESSES_MARKER`, recognized at both `follower.rs` refuse sites (the resolving
  arm and the ordinary ship arm — separately fatal before this, since the parked attempt's
  probe patched only one), mapping to back off and retry rather than dying;
  `probe.rs::classify`'s existing default arm already files it `Inconclusive` for free (no
  production change needed there — Task 2's tests are what hold that contract, since the
  behaviour is inherited). **The GC pin (spec §4, Task 1, `60be3f6`):** `Session::pin_floor()`
  now returns `u64::MAX` for a dead session, mirroring the `control` short-circuit beside it —
  **this is an efficiency fix, not a correctness one.** An earlier spec draft called a
  retrying peer's pin window "permanent"; the implementer's diagnostic trace disproved it
  (`0 → 0 → 0 → MAX → MAX`, ended by `Repl::step`'s pre-existing top-of-step reap every
  round) and the spec was corrected in place before landing: delayed GC, never wrong GC, at
  most one checkpoint's GC per occurrence. **Ruling (Noah, 2026-08-06):** the seed refusal
  **never lapses** while the witness gate is shut — not at `WITNESS_DEADLINE_NANOS`, not on a
  timer, chosen over lapse-at-deadline and seed-but-propagate-the-gate (the latter needs a
  wire field and new durable state on the newcomer; ledgered, not built).
  **What the reviews caught** (every task opus-reviewed, every fix round closed 0 Critical):
  Task 1's one Important was the controller's own comment, not the implementer's — "any fixed
  round count either has not refused yet or has already been reaped" was false for a count of
  1; four Minors were comment-precision (M-1: "until the next round's reap" understates —
  `Repl::step` has four early returns above the reap, including the boot gate; M-2: the guard
  covers any dead session, not only a refused one). Task 2's sharpest finding (I-2) was a doc
  citing the WRONG Task-3 test as this refusal's byte-exact pin — the real one is
  `the_seed_while_verifying_refusal_is_pinned` (`0x2EB1_0121`); three more Importants and
  four Minors, all comment accuracy. Task 3's review caught **a third vacuous assertion**
  (I-1): `assert_eq!(total_seeds(&g), 0, …)` in the retry test could never fail (this rig
  never checkpoints, so a successful seed would also leave `total_seeds` at 0), fixed with
  `last_applied() == None`, RED-first proof independently reproduced by the re-reviewer; I-2
  was the GUIDE contradiction Task 4 closes below; 10 Minors, all hygiene.
  **The headline process finding, caught by hard rule 3 rather than a reviewer:** the
  implementer's own `a_verifying_node_still_ships_catch_up_to_a_positioned_peer` passed under
  a mutation that should have reddened it — **in this simulator, not driving a node does not
  isolate it.** `crash_now`'s `teardown_host` tears down only the crashed side, so the
  survivor's inbound queue stays intact and a peer never polled still "converges" by draining
  stale pre-crash frames without ever negotiating a fresh `Hello`. Fixed with `recover_node`
  to force a real reconnect; mutation then produced a genuine RED (779s to budget exhaustion)
  and GREEN under the real fix (0.03s). The ledger counts this as the interlude's **second**
  controller-authored vacuity — the first was Task 1's test that could not go RED, caught
  before it was ever committed — and Task 3's own review then caught a **third**, reviewer-
  side this time (I-1): `assert_eq!(total_seeds(&g), 0, …)` in the retry test could never
  fail either way, since this rig never checkpoints and so a refused seed and a successful
  one both leave `total_seeds` at 0; fixed with `last_applied() == None`. All three were
  caught by RED-first being enforced, none by a test "obviously" passing being taken on
  faith.
  **The corollary, confirmed on all five rigs (Task 3's acceptance gate — the five property
  tests the 0b parked attempt broke):** `SEED_WHILE_VERIFYING_REFUSAL` is emitted in **none**
  of `convergence_universe`, `failover_universe`, `switchover_universe`, auth's
  `raw_credentials_universe`, or the kernel gate-opens test — every topology in them is either
  a strict two-node pair or admits its newcomer through the membership conjunct alone. **So
  the parked attempt's blast radius was entirely mis-keying, not receiver-fatality.** Stated
  honestly, not wider: those five prove the conjunct does not fire *spuriously*; they do not
  exercise retryability, which is covered separately by Task 2's five unit tests plus Task 3's
  two new property tests (`a_verifying_node_still_seeds_a_recorded_witness`,
  `a_refused_newcomer_retries_and_converges_once_the_gate_opens`).
  **Task 4 (this commit) — docs, and a correction the branch owed its own tests.**
  DESIGN §5 gained the policy/keying/retryability/safety-monotone-recording paragraph (a pure
  addition — DESIGN made no seed claim before this). `docs/GUIDE.md`'s "gated node is also
  still a replication peer" paragraph was rewritten, not appended to: it had shipped claiming
  a gated node "will still seed a brand-new follower" and records the newcomer "as a witness
  like any other" — both now the **opposite** of shipped behaviour (Task 3's own review,
  I-2). The branch was shipping documentation its own tests disproved; the corrected
  paragraph states the refusal (quoted byte-exact from `the_seed_while_verifying_refusal_is_
  pinned`), that a refused newcomer is never recorded (`listen` is assigned only on the two
  shipping arms), and the operator consequence — a permanently dead witness blocks new
  followers until the existing escape hatch (edit or delete the `witnesses` file) is used,
  which costs little since the only use for a freshly-seeded follower would be promotion,
  exactly the harm the gate exists to prevent. The evidence sweep below surfaced a second,
  unplanned doc gap in the same section: `TxError::Verifying`'s 503 is not incident-specific
  — it fires on *every* restart of a primary with a non-empty witness set, ordinary deploys
  included, since the node cannot yet distinguish "just restarted" from "restarted after
  being promoted away from." GUIDE §10 now says so explicitly, up front, so an operator whose
  deploy writes immediately after a restart recognizes the 503 rather than filing it as a
  bug.
  **Tests** (RED-first per hard rule 3): the GC pin
  (`a_refused_session_does_not_pin_gc`, `0x2EB1_011B`, `replication.rs`); five `follower.rs`
  tests seeded `0x2EB1_011C` (two marker-content assertions with no sim,
  `the_refusal_carries_its_marker` and `the_refusal_carries_neither_other_marker`, plus three
  seeded retry tests — both refuse sites backing off, and the negative control that an
  unmarked refusal stays fatal); in `replication.rs`, the headline
  `a_verifying_node_refuses_to_seed_a_brand_new_follower` (`0x2EB1_011D`, rigged so
  `log_base() == 0` to land on the arm a variant-keyed refusal would miss), the keying
  positive case `a_verifying_node_still_seeds_a_recorded_witness` (`0x2EB1_011E`), the
  retryability test `a_refused_newcomer_retries_and_converges_once_the_gate_opens`
  (`0x2EB1_011F`), the negative-space
  `a_verifying_node_still_ships_catch_up_to_a_positioned_peer` (`0x2EB1_0120`), and the
  byte-exact pin `the_seed_while_verifying_refusal_is_pinned` (`0x2EB1_0121`). Plus
  `probe.rs`'s classifier case (`fresh_db(0x0DEB_040C)`), now routed through a real
  `decide_hello` emission (`real_reply`) like its siblings rather than hand-constructed, per
  Task 2's review M-4.
  **Evidence** — 11-stage sweep (spec §9), 10 of 11 stages green on the first attempt; the
  edge matrix required a second run (see below) — `cargo fmt --all --check` clean;
  `cargo clippy --workspace --all-targets -- -D warnings` clean; `cargo build -p
  guide-examples` (the GUIDE compile gate) clean; `cargo test --workspace` **twice**,
  identical both times — 49 test binaries, **583 passed / 0 failed / 31 ignored** (the
  572→583 delta against the hard-rule-5 baseline reconciles exactly: Task 1 +1, Task 2 +5,
  Task 3 +5); pinned debug replays `CHARPENTE_SEED=0x8bee1752811ae873` (charpente-sim
  crash_recovery, 4/4) and `0x348270b38f166529` (charpente kernel, 26/26); both release
  mode-skew tripwires exactly as `ci.yml` runs them (`0x8bee1752811ae873` release
  crash_recovery 4/4, `0xff159ed76d2806e4` release
  `late_follower_seeds_below_the_gcd_base_and_converges` 1/1); a fresh invoicing
  1024-universe batch, **all green, master seed `0xc53bdfb3f3a7f1a4`**; and
  `scripts/edge-harness.sh`, run because replication, auth and realtime are all in the
  measured blast radius, reported here honestly rather than as only its clean attempt.
  **Run 1: two transient failures** (`failover_wire_oracle`,
  `failover_non_converged_tail_is_exercised_and_bounded`) — a `login()` POST fired within
  ~3ms of a mid-run primary restart raced sub-project 0b's witness-gate resolution (the HTTP
  port binds before the kernel thread finishes `Repl::open`, by design) and lost. **Run 2 at
  the same commit: 31/31 green.** Isolated re-runs of the two failing tests alone were 5/5
  green both at this interlude's base commit (`66cddde`) and at its tip — but isolation
  cannot discriminate a load-sensitive race, since neither run carries the full suite's ~74s
  of prior CPU load. **The base-commit comparison** (the experiment that could have settled
  it: the full suite, 3 times, at the base commit `66cddde`, worktree-isolated) —
  **31/31 green all three times, 93/93 aggregate, zero failures.** Stated as what is actually
  known, not overclaimed in either direction: the failure was observed once at HEAD and was
  not reproduced at the base commit across 3 runs. That does not establish this interlude
  widened the race — 3 green runs at base do not prove the interlude is uninvolved either,
  given how few samples either side has (1 failure in 2 against 0 in 3 is p ≈ 0.4 by Fisher's
  exact test — refusing a direction is the statistically correct call here, not excess
  caution). The mechanism (sub-project 0b's witness gate racing `ChildNode::restart`'s
  TCP-only readiness check) predates this interlude, but a timing contribution is not
  excluded: Task 1's `pin_floor` fix makes checkpoint GC proceed where it previously stalled
  behind a dead session's pin, which shifts work and scheduling — the same shape as M1
  sub-project 4's T6 platform cron "shifting the fault stream" and exposing a latent bug
  elsewhere. What would actually settle the causal question, cheaper than the n ≫ 3 runs at
  both commits the statistical route would need: fix `run_oracle`'s readiness check so it
  waits on the gate rather than the TCP port, which makes the race moot regardless of cause.
  Ledgered as a candidate M2 item either way (`run_oracle`'s restart-then-relogin flow has no
  tolerance for the witness gate's async resolution window, regardless of which commit
  widened it, if any), not fixed here — Task 4 is docs-and-evidence, not product or test
  code.
- Next: **M2 per PLAN.** Ruled 2026-08-06 (premise re-examination at M2 start): the own-LSM
  engine is **deferred behind measurement** — DECISIONS §7's scheduled reopen, re-closed with
  three named build triggers — and the **benchmark harness** (PLAN's unfulfilled "publish
  honest benchmarks early" mitigation) is the next sub-project. **It is in progress, not
  pending**: the spec (`docs/superpowers/specs/2026-08-06-benchmark-harness-design.md`,
  `2b47556`) and its 10-task plan (`be9b05a`) landed 2026-08-06; Tasks 1-5 shipped
  `crates/bench-harness` — the measurement discipline, the pinned report model, the device
  layer and the `bench` binary, and the core layer against its device ceiling — and a
  Noah-ruled mid-plan interlude (2026-08-08, spec `…/2026-08-08-bench-accounting-split-
  design.md`) split the core layer's *accounting* from its *measuring* so every guard, delta
  and INVALID decision is a deterministic unit test instead of something only a real benchmark
  run can reach. Tasks 6 (kernel layer), 7 (wire layer) and 8 (the invoicing wire target)
  then shipped, each adopting that interlude's seam from inception per its spec §5 — raw
  observations struct, pure accounting, thin driver — rather than copying an untested
  pattern. **Task 9 (the `all`/`smoke` subcommands, forced-INVALID-arm tests and
  `scripts/bench.sh`) is next**, then T10 (`docs/BENCH.md`, first numbers of record).
- **Bench interlude — the device-ratio gate, done (2026-08-10, spec
  docs/superpowers/specs/2026-08-10-device-ratio-gate-design.md, ledger
  .superpowers/sdd/2026-08-10-device-ratio-gate/progress.md, commits `c10b8fa`..this
  commit).** Opened by T8's §6 "What is NOT met" and widened by measurement: `bench` was
  refusing HEALTHY runs at **all three** gating layers, not just invoicing's — core 4 of 4
  red, kernel 2 of 3, wire-invoicing 3 of 6. **The premise was checked at the source and is
  sound** (`Engine::submit` is unconditionally `append`→`sync`→`apply`, no group commit; the
  follower's `submit_batch` is the only batching path and is explicitly "minus the client
  ack"), so one acked request really does pay one durable sync. The defect was comparing a
  *measured estimate* of the denominator against a `1e-6` tolerance: for any layer
  committing one record per acked unit the true ratio is exactly **1.0**, so the
  comparison's sign was decided entirely by measurement error. Three of the four shipped
  configurations sit there; bench-app was the only one with margin (2.0 records/request →
  ~0.5x), and the gate had been validated on it alone. Two error components measured: the
  bracket used the **wrong op shape** (offset-0 rewrite of a one-byte file vs the sequential
  WAL append it bounds — ~4.2% systematic, n=15 paired, sign test p≈0.035, with the loop-order
  confound ruled out by an order-alternating probe at 1.0433 vs 1.0411), and the estimator's
  own per-point spread ran 10–31%. **Three Noah rulings (2026-08-10):** shape fix + noise-aware
  tolerance (over tolerance-only or a flat wider slack); land it as its own interlude; cut
  across all three layers uniformly. Shipped: `device_bracket_sample` appends to a fresh
  growing file; core and kernel divide by brackets around their own windows (finishing T7
  rev. 3's fix at the two layers it skipped — `core_bench` already TOOK its brackets and
  merely published them, `kernel_bench` took none, and its shipped refusal message named
  that gap in so many words); and `measure::implausible_bracketed_ratio` gates on
  `max(RATIO_TOLERANCE_FLOOR 0.10, bracket spread)` with a `RATIO_HARD_LIMIT` of 1.5 keeping
  it structurally non-vacuous. `device_ceiling` left both observation structs, both `run`
  signatures and `bench.rs`'s threading; `bench device` is now the artifact's methodology
  header and no layer's gate input. Pre-release artifact change: the bracket fields are
  `device_appends_per_sec_*` (a field named "syncs" holding an appends number is the record
  inaccuracy this repo keeps paying for), and `KernelReport` gained its pair.
  **Reopens two rulings, both argued rather than overridden** (spec §7): T8's Ruling 1
  ("measure the ceiling on the same path") was refuted at **n=2**, which cannot see a 4.2%
  effect inside 10%+ noise — reinstated on evidence that can carry it; and T5 re-review N-4 /
  ruling #4, which tightened `1e-2`→`1e-6` precisely to stop absorbing sampling noise, is in
  real tension with this and is named as such — both its premises changed (the denominator
  was a one-shot sample then, and no layer had been measured at a true ratio of 1.0).
  **Measured before → after:** core 0/4 → **6/6** exit 0, kernel 1/3 → **6/6**, wire-invoicing
  3/6 → **6/6** (ratios 0.858–0.989, none above 1.0). Second-order effect worth knowing: the
  append-shaped bracket is not merely ~4% higher but far more STABLE (spreads 0–7% vs up to
  31.6%), so the shape fix did most of the work and the tolerance is margin — the option
  analysis put to Noah predicted the reverse balance and was wrong in its mechanism, right in
  its conclusion (the tolerance is still needed; core run 1 landed at 1.01 and kernel run 2 at
  1.022, both of which the old gate refuses). **Teeth: an 8-mutation table, 8/8 RED, restores
  sha256-identical — and it caught three of this interlude's OWN tests that could not fail for
  the reason they claimed** (the hard-limit probe passed with the hard limit deleted, because
  its 0.999 spread meant the tolerance arm already refused 2.0; and the kernel and wire gates
  could both be reverted to `> 1.0 + 1e-6` with every test still green, since nothing probed
  the band where the two gates differ). All three fixed, each now asserting its discriminating
  premise. Also closed a renderer gap found by accident: the `device:` and `kernel:` human
  lines were pinned nowhere, which is how edits to both passed a green suite. Evidence — 13
  stages green: fmt, clippy `-D warnings`, guide gate, `cargo test --workspace` **twice**
  identical (**693 passed / 0 failed / 40 ignored over 55 binaries**; the 680→693 delta is
  reconciled exactly as 16 added minus 3 renamed), the 9/9 `--ignored` bench smokes, both
  pinned debug replays (`0x8bee1752811ae873` 4/4, `0x348270b38f166529` 26/26), both release
  mode-skew tripwires as `ci.yml` runs them (4/4, 1/1), a fresh invoicing 1024-universe batch
  all green (master seed `0x4cf5a3839fb2f628`), and `git diff --stat` over `charpente-core`
  and `charpente` **empty**. The edge matrix was deliberately not run — no replication, auth
  or realtime behaviour is touched, which is spec §8's stated condition for skipping it.
  Do not start the LSM engine from this file alone; the triggers live in DECISIONS §7.
  The backlog below holds **one open ruled follow-up** — `applied_delta`'s release-mode wrap,
  added 2026-08-08 and the only entry in it that is open work; every other entry is record. Still-open work, none of it ruled or blocking: `TcpProxy::stop`'s
  registration window ("fix in M2"); the follower-decode asymmetry above; the edge-harness
  restart-then-relogin race noted in the verifying-peer-policy entry just above (mechanism
  present since sub-project 0b at the latest, observed once at this interlude's tip, not
  reproduced at its base in 3 runs — neither established nor ruled out as this interlude
  widening it); and 0b's remaining accepted minors
  (the `eprintln!` inside the `repl` boundary with `take_witness_notice()` having no
  production caller, self-witnessing unguarded, the two gated-503 test sites with no cover
  key, `tx_err_to_io`'s unbounded `{unreached:?}`, the 17 MiB recv buffer per connected probe,
  and `probe.rs::connect_and_hello`'s near-verbatim copy of `follower.rs` — a THIRD copy is
  the moment to consolidate).

## M2 follow-up backlog (carried from M1, ruled — written to be read cold)

- **OPEN, ruled 2026-08-08: `applied_delta`'s `(Some, Some)` arm wraps silently in release.**
  The one piece of open work in this section. `crates/bench-harness/src/core_accounting.rs`'s
  `applied_delta` computes the applied-record delta across a measured window, and its
  `(Some(b), Some(a))` arm is a bare `a - b` while its sibling impossible-state arm
  `(Some(_), None)` carries a named `unreachable!`. **The workspace sets no `overflow-checks`
  override** — the only `[profile.*]` entries in the root `Cargo.toml` are the redb
  `debug-assertions`/`opt-level` ones — so release inherits Rust's default
  `overflow-checks = false` and that subtraction wraps instead of panicking. Verified
  empirically twice, independently (the T2 controller and the fix-round re-reviewer): `rustc
  -O` on `(Some(5), Some(3))` prints `18446744073709551614` and exits 0. Consequence in the
  mode a real benchmark actually runs in: a backwards `last_applied` publishes
  `log_records_per_sec` ≈ 1.8e19 / elapsed and the run **continues**, flagged only as a
  non-fatal INVALID by the `delta != ops` crosscheck, rather than stopping and naming the
  contract that broke. **Not reachable through `core_bench::run` today** — `Db::last_applied()`
  is monotonic and the two samples are taken in window order — so this is negative-space
  assertion work, exactly what hard rule 4's "assertions stay on in release builds" clause is
  about, not a live bug. Recommended fix, one line:
  `a.checked_sub(b).unwrap_or_else(|| panic!("last_applied went backwards: {b} -> {a}"))`.
  **Why it is here and not already done** (Noah-ruled 2026-08-08): turning a release-mode wrap
  into a panic *is* a behaviour change in release, and it was found inside an interlude whose
  spec §3 opens "This is a refactor" and whose banner is zero behaviour change; under that
  spec §7 it also owes a mutation-proven test of its own. Both the T2 review and the fix-round
  re-review independently judged deferring correct rather than folding it into a test-only
  round. It is small enough to be its own interlude whenever it is picked up.
- **The dual-primary gap — RETIRED 2026-08-05, closed by M2 sub-project 0b** (close-out in
  "Current state" above). It was the last *ruled follow-up* carried out of M1; what remains
  in this section is record, not open work, with one exception named in the triage entry
  below (`TcpProxy::stop`'s registration window, marked "fix in M2"). The policy fork the
  entry existed to hold open — what a node does when its witnesses are **unreachable** — was
  re-decided at implementation time as ruled option (c): serve reads, withhold writes, keep
  probing, and emit one notice at the deadline, with the strict-refuse alternative shipped as
  the opt-in `ReplConfig::strict_witnesses`. The recommended fix in the retired entry is what
  shipped, essentially unchanged (durable witness set, probe before writes, refuse by name,
  never demote); the two things it did not anticipate are that the write gate needed a
  **third** state rather than reusing `NotPrimary` (a verifying node has no primary to name,
  so 421 would have been a lie — hence `TxError::Verifying` → 503), and that the probe had to
  be a purpose-built classify-and-disconnect client rather than `Follower::new_resolving`,
  which would have silently adopted the winner's timeline. Design C (a durable timeline epoch
  on `Hello`) stays ledgered and stays compatible — it would sharpen the refusal, not replace
  the probe.
- **Retired follow-up wording.** The migrations close-out's follow-up "`Db::poisoned()`
  reflects only apply-side poison, not redb **read-side** poison" is retired as imprecise:
  the reachable-anywhere mechanism is the **checkpoint fsync**, and the read mechanism is
  size-conditional. Both wordings are moot as of 2026-07-31: the gap itself is closed —
  `Db::poisoned()` now reflects the store latch too — and the accurate account is the
  sub-project 0a close-out in "Current state" above. Kept here only so the old wording is
  not re-derived from the migrations ledger as if it were still open.
- **Deferred items triaged by the final review — recorded, not acted on.** *Ship as is:*
  the `temp_dir`/`free_addr`/`extract_field`/`find_node_bin`/`Rng` helper copies duplicated
  across edge test files (verified identical today, and no silent-pass path exists — a
  broken `find_node_bin` returns `None` or the stock path, which `assert_ne!` catches);
  `acked_durable.rs` at ~1100 lines (about half is rationale prose a future reader needs at
  the site); `finish`'s four copies with three signatures (a fifth would be the moment to
  consolidate); the two T11 pin waivers; T12's fresh-timeline 409 arm, its connection
  over-count in the scenario log, and `StopReason`'s uneven per-variant docs. *Fix in M2:*
  `TcpProxy::stop`'s residual sub-microsecond registration window — one connection can
  survive a cut; the failure direction is loud (the tail test fails naming "the cut did not
  work as intended"), and the structural close is to clone inside the accept loop's existing
  critical section. *Notes worth knowing:* `classify`'s arm order is pinned for
  Injected-before-Poisoned but not DurableHorizon-last (stricter, not looser; no shipped
  string carries both needles); the nine trybuild `.stderr` pins render against whatever
  `dtolnay/rust-toolchain@stable` resolves to, so a rustc rendering change reddens nine
  tests at once (deliberate and loud); GUIDE §13's quoted contract JSON is byte-identical to
  the pinned golden today but nothing mechanically holds it there; the edge meta-test adds
  and removes a `git worktree` against the real checkout, so a matrix run killed mid-test
  leaves a stale entry until the next run's `prune`.
- **One correction to the ledger's own record** (found by the final review): T11's pin
  waiver for the "oracle is vacuous" string was recorded as if the string were unpinned. It
  **is** pinned byte-exact by `require_full_ack_names_a_vacuous_run`
  (`acked_durable.rs:1096`); the waiver covers only reaching it end-to-end at the wire,
  which needs a non-acking build.
- **The verifying-peer-policy interlude's uncovered `Decision::Snapshot` producer** (found by
  its final review, I-1): `decide_hello` has a *third* seed-shaped answer besides the two
  guarded `None`-position arms — `session.rs`'s positioned-peer ladder answers
  `Decision::Snapshot` when this node cannot read back the peer's frame at `pos_seq`. A
  verifying node reaching it would render a full snapshot over a peer's *existing* store,
  worse than the newcomer case. The shipped policy is correctly scoped to "a peer that holds
  nothing" (spec §1), so this is not a behaviour change, and the review could not construct a
  reachable case (it needs a hole in the log below `last_applied`, which recovery's cut, the
  gaplessness triple-guard, and the `pos_seq < base` → `diverged_behind` arm above jointly
  exclude) — but it was neither proven unreachable nor recorded anywhere before the review
  found it. A rationale comment now sits at the site. Extending the verifying guard to cover
  it (refuse, or assert unreachable) is a policy change outside spec §1's scope — a positioned
  peer, not a newcomer — and needs a ruling before it is anything but a comment.
