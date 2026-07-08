# CLAUDE.md — charpente

Instructions for agents working in this repo. Read the docs in this order before writing
any code: README.md → DESIGN.md → PLAN.md (MANIFESTO.md for spirit; DECISIONS.md before
proposing to change an approach; NEIGHBORS.md before designing any subsystem with prior
art — DST, storage, replication).

## What this is

An agent-native, single-binary Rust backend platform. Design phase → M0. The milestones
and exit criteria in PLAN.md are the roadmap; do not reorder M0's dependency sequence —
the sim-capable IO layer comes first because it cannot be retrofitted.

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
- Next: M1 sub-project 2 per the segment-mgmt spec — replication (async log shipping
  into the retention pin, follower reads, manual promotion, backup, multi-node sim);
  then realtime SSE/WS with exact resume, auth properly (OIDC, hashed session tokens),
  migrations as log records, error/introspection polish (`charpente describe`), and the
  Vörtex-style black-box edge harness for the hyper layer.
