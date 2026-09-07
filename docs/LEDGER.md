# charpente — development ledger

The chronological record of every milestone, sub-project and ruled interlude. Moved here
from CLAUDE.md on 2026-08-14: it is the project's memory, but it is not a standing
instruction, and every subagent dispatched in this repo was loading all of it.

CLAUDE.md carries the standing rules. This file carries what happened. The underlying
detail lives in git history and `.superpowers/sdd/`.

**How to read the pin-replay figures below (correction, 2026-08-15, M2 T9).** Entries
report pinned-seed replays as `0x348270b38f166529` "26/26" / "29/29" / "30/30" and
`0x8bee1752811ae873` "4/4". Those are **suite** counts, not pin evidence.
`CHARPENTE_SEED` is read only by `run_universes_in`/`run_universes_swarm_in`, so it forces
the seed of exactly one test per suite — measured at `de40e0b`: **1 of 30** in the
charpente kernel suite and **2 of 4** in charpente-sim crash_recovery (`grep -c
"replaying seed"`). A "30/30" is 29 tests that ran at their own literals and are not
evidence about the pin. The denominator that matters is the `replaying seed` count; read
every figure below that way. `.github/workflows/ci.yml` now names the consuming test
`--exact` for the kernel pin so the CI line cannot drift back into the per-suite reading.

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
  still exercises its historical bug post-migrations. **Corrected 2026-08-15
  (M2 T9):** this over-claims. The proof reverted `Kernel::build`'s drain-id
  fix, whose own seed is `0x650`; the bug `0x348270b38f166529` was pinned for
  is a *test-file* checker gap (the `is_fault_fallout` arm in `kernel.rs`).
  The seed does still exercise the drain-id fix — that much re-verified at
  `de40e0b` — but it no longer exercises its own historical bug; see
  `.superpowers/sdd/2026-08-10-group-commit/pin-reverification.md`. Follow-ups ledgered: the
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
  identical dispatch.rs:158 panic; restored 25/25). **Corrected 2026-08-15 (M2
  T9):** the reverted fix was the drain-id one, which is not what this seed was
  pinned for (as above). The `dispatch.rs:158` site has since MOVED — T7's
  `run_due`→`run_due_into` rewrite put the same `assert_eq!` ("dispatcher
  invariant: job {id} in the heap is not Pending") at `dispatch.rs:182`. Same
  assertion, new line; a "same panic, same line" claim carried forward from
  here would be false.
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
  three named build triggers. The **benchmark harness** built to read those triggers is now
  **done** (close-out entry below, 2026-08-10), and trigger (a) has been read against real
  numbers: it **does not fire**, so the LSM stays deferred and is NOT the next sub-project.
  What the measurement opens instead is **primary-side group commit**, already sized by
  BENCH.md's own table — 215 durable records/sec is the number to beat. That is now **M2
  sub-project 2, IN PROGRESS** (spec docs/superpowers/specs/2026-08-10-group-commit-design.md,
  11-task plan docs/superpowers/plans/2026-08-10-group-commit.md, ledger
  .superpowers/sdd/2026-08-10-group-commit/progress.md). **T1–T7 are committed** as of
  2026-08-13, ending at `3cd3ecd` (the job and cron arms drain, review fix round included);
  T8–T11 remain — the property suite, re-verifying every pinned seed, measuring the win, and
  docs/close-out. **This section is written at sub-project close-out, so it will not narrate
  T8–T11 as they land; the ledger is authoritative mid-plan.** *(Superseded 2026-08-19:
  T8–T11 all landed. See the T11 entry two bullets below for what shipped, what the numbers
  are worth, and the one close-out gate that came back red.)* Otherwise M2 per PLAN — the
  follow-up backlog below now holds **no open work** (`applied_delta`'s release-mode wrap, its
  last open item, closed 2026-08-10).
- **Group commit T10 — the win measured (2026-08-15, harness at `d9592c2`, report
  .superpowers/sdd/2026-08-10-group-commit/task-10-report.md).** T10 produces evidence and
  commits no code; this entry exists because the artifacts live under `.superpowers/`, which
  is **gitignored and absent from a fresh clone** — a number of record with no tracked
  pointer is a number no one can find. **Everything below is an OBSERVATION, not a quotable
  figure: docs/BENCH.md remains the only source of those** (CLAUDE.md), and T11 is what
  lands them there. **Artifacts:** `task-10-report-run1.json` md5
  `22dda358c86bed27f6fccc1431a0dd2d`, window 2026-08-15 **14:12:08–14:21:25 EDT**;
  `task-10-report-run2.json` md5 `04c1a24f3994ce40fe09df8b6c3dbd38`, window
  **14:24:00–14:33:02 EDT**; `task-10-report.json` is byte-identical to run 1. Both predate
  `72211e1` and therefore carry **no `syncs_counted` key** — tell counted from derived by the
  arithmetic instead (a derived figure is exactly `multiplier × throughput`; `invoicing`'s is,
  at all ten points, and `bench-app`'s is not, at all ten). **The claim, in the only form spec
  §8 permits** — it says the conditioning is "the price of it, to be repeated wherever the win
  is reported", and names *"group commit makes charpente Nx faster"* as the sentence it
  forbids: **group commit removes the one-fsync-per-transaction ceiling, and on hardware whose
  fsync is slow enough to keep a queue deep it recovers up to ~31x — and nothing at one
  connection.** This device flatters the mechanism (a slow fsync keeps queues deep, and greedy
  drain converts queue depth into batch size), so the multiple is closer to an upper bound than
  to a server's. The measured **connection-scaling curve** is the conditioning quantified
  rather than asserted — wire `bench-app` against sub-project 1's `111, 99, 103, 110, 112`
  req/s: **1c 1.02x, 4c 4.80x, 16c 15.44x, 64c 31.47x, 256c 24.93x**, every point green in the
  run it is taken from (1c run 2, the rest run 1). Kernel log records/sec **217 → 5,292**
  (24.4x), 50,000 requests in **17.55 s / 18.90 s against 460 s**, `jobs_done ==
  requests_total == 50,000` in both runs. **Core is unchanged (215 → 222.13, ratio 0.83 →
  0.8504) and that is the correct result** — `Db::transact` is a length-1 batch
  (`engine.rs:152`), so a serial loop has nothing to amortize; batching acts through
  `Group::commit` (`db.rs:2202`), which the kernel uses. **Amortization is established
  two-sidedly, because the ceiling test alone cannot exclude skipped syncs** (a
  sync-skipping engine also reports a low sync rate): counted sync rates stay **below** the
  device ceiling (max counted ratio anywhere = **0.9388**; `bench-app` at 64 conns pays **108
  durable syncs/s** for 6,924 records/s) **and** the batch factor stays **below** `MAX_BATCH`
  = 64 (`db.rs:2003`, assert-enforced `db.rs:2066`). Primary evidence is the kernel layer,
  counted on both operands: **61.9602 / 61.9614 records per durable sync**, and the same fixed
  workload paid **1,614 ± 1 syncs in both runs** (= 2 arms × ⌈50,000/64⌉ = 1,564 batch syncs
  plus ~50 store/`.horizon` syncs its unscoped counter also tallies). `bench-app`'s wire
  figure is corroboration only — its numerator is DERIVED (`2.0 × throughput`), proved by run
  2's 256c reading of **64.1735** against an assert-enforced cap of 64. **Layer-by-run green
  map** (neither run is green alone; T11 needs both): core ✅run 1 / ❌run 2 (23.04% bracket
  spread); kernel ❌run 1 (46.05%) / ✅run 2; wire `bench-app` 1c ❌run 1 / ✅run 2, 4c ✅✅,
  16c ✅run 1 / ❌run 2 (20.21%), 64c ✅✅, 256c ✅✅; wire `invoicing` 1c ✅✅, 4c/16c/64c/256c
  ❌❌. **Both runs exited 1 and `invalid: []` was unreachable on the matrix as it then stood**
  (T13's ruling (b), 2026-08-19, removed that — see below; these four invoicing refusals would
  not fire today): `invoicing`
  mounts no `/__syncs`, so its sync figure is the `1.0 × throughput` fallback, and group commit
  is exactly what makes that multiplier unsound — the harness is refusing its own unsound
  numerator (2.06x–30.49x the ceiling), which is what `wire_accounting`'s owed-work paragraph
  predicted. That is a T11 scoping decision; no threshold was touched here. **Caveats that must
  travel with these numbers:** the two headline points are the closest passing points to
  refusal in the matrix (core run 1 **17.65%**, kernel run 2 **17.61%**, against a 20% ceiling
  and BENCH.md's own 15.7% worst-clean-spread); the box was not quiet and **the cause of the
  bracket instability is NOT determined** (the foreign pytest workload observed at 14:18:18
  post-dates run 1's kernel and 1c refusals by ≥2m43s and cannot explain them); run 2's
  `bench-app` 64c/256c points carry p99→p999 excursions of **9.0x / 3.9x** that neither the
  bracket nor the heartbeat guard saw, so a run-1 throughput must not be published with a
  run-2 tail; and the kernel's sync counter is **unscoped** while the wire's is `wal/`-scoped
  though both divide by the same append-shaped bracket, making 61.96 a lower bound. Reviewed
  independently (`task-10-review.md`): evidence compliance PASS, every recomputed figure
  correct; the report was revised for framing, not for numbers.
- **M2 sub-project 2 — primary-side group commit: SHIPPED AND CLOSED (T11 published the
  docs, T12 closed the one red gate; 2026-08-19, spec
  docs/superpowers/specs/2026-08-10-group-commit-design.md (`017a81c`), 11-task plan
  docs/superpowers/plans/2026-08-10-group-commit.md (`bc03e63`), ledger
  .superpowers/sdd/2026-08-10-group-commit/progress.md, commits `017a81c`..`b9c2a5c` —
  **42** by `git rev-list --count 017a81c^..b9c2a5c` — plus this commit, the one that
  withdraws the CI step on Noah's ruling, for **43** through HEAD; four of the 43 belong to
  the concurrent skills/personas and ledger-restructure work). Both endpoints are named SHAs
  and the increment is spelled out rather than left as "this commit": the entry has now been
  amended four times and the count moved 38 → 42 → 43, twice underneath wording that never
  changed a word.** The follow-up BENCH.md named
  at sub-project 1's close, built, measured and now published. **The sub-project is
  closed**: everything it set out to build is in the tree, and T11's one red gate — the
  `scripts/edge-harness.sh` FIXTURE failure at 30/31, a regeneration T11 was explicitly
  scoped out of — was closed by T12 (below), which also added an un-ignored guard so the
  next drift reds the ordinary sweep instead of going a week unseen. Read that precisely:
  the *fixture* gate is closed and the teeth are proven, but the script itself is **not**
  reliably green — T12 measured 4 red in 8 runs, all of them the pre-existing failover
  witness-gate race, none of them this sub-project's code — and on that evidence Noah ruled
  the script must **not** become a CI step. **Five items stay OPEN and are carried forward
  here rather than closed with the sub-project**: request-vs-tail contention is unmeasured,
  DECISIONS §7 trigger (a) is owed a re-read, `scripts/bench.sh --smoke` is still deferred,
  the failover witness-gate race must be fixed before the edge rig can be wired into CI, and
  `engine.rs:159-163`'s batch-atomicity comment still contradicts the DESIGN and BENCH text
  corrected against it — all five below.
  **Shipped (T1–T11).** The commit point is a *group*, not a transaction:
  `Db::begin_group` → N × `Group::stage` → `Group::commit`, one store snapshot, a two-level
  overlay (txn → group → snapshot), one durable sync for the whole batch.
  `Engine::submit_all` is the single append→sync→apply core underneath both the batched and
  the length-1 path, so `Db::transact` is the group-of-one case rather than a second code
  path — byte-identity at one connection is a property of the code, not something tests must
  keep true. `MAX_BATCH = 64`, a constant asserted inside `Group::stage`, never a `DbConfig`
  field. Ack-after-durable is preserved structurally: `stage` never appends, replies are
  held until `commit` returns, apply stays strictly after sync. Batching is within one
  ready-set arm, never across arms; a subscribe ends its batch so cursors keep their
  meaning. DESIGN §2 carries the design, docs/GUIDE.md §5 the client contract, PLAN's M2
  list the status.
  **The numbers, and the ONLY form the claim may take (spec §8, and this conditioning is to
  be repeated wherever the win is reported):** *group commit removes the
  one-fsync-per-transaction ceiling, and on hardware whose fsync is slow enough to keep a
  queue deep it recovers up to ~31x — and nothing at one connection.* Never *"group commit
  makes charpente Nx faster."* **docs/BENCH.md is the only quotable source** and now carries
  the full section: the kernel at **5,292 log records/sec against 217** (24.4x) for **85
  durable syncs/sec**, 50,000 canonical requests in **18.9 s against 460 s**, `jobs_done ==
  requests_total == 50,000`; wire `bench-app` **1.02x / 4.80x / 15.44x / 31.47x / 24.93x**
  across 1→256 connections; core **unchanged** (215 → 222 records/sec, ratio 0.83 → 0.85),
  which is the correct result because `Db::transact` is a group of one. Amortization is
  argued two-sidedly, because a sync-skipping engine also shows a low sync rate: every
  counted sync ratio stays **below** the device ceiling (max **0.9388** anywhere) **and**
  the batch factor stays **below** the cap (**61.96** records per durable sync, both runs,
  against `MAX_BATCH` 64), with the fixed workload paying **1,614 ± 1 syncs in both runs** =
  2 arms × ⌈50,000/64⌉ + ~50 store/`.horizon` syncs the unscoped counter also tallies. The
  caveats travel with the numbers in BENCH.md itself, not only here: neither T10 run is
  green alone (a different layer was refused in each), no published point mixes runs, the
  bracket instability is **undiagnosed**, and the bare-metal Linux run stays owed.
  **T11 read the artifacts through two traps that would each have produced a wrong
  BENCH.md**, recorded because the first of them is still live in the code: the wire `device_ratio` key means
  *records* while core's and kernel's mean *syncs* (`wire_accounting.rs:286` versus
  `core_accounting.rs:278` / `kernel_accounting.rs:219`), so every wire sync ratio published
  was recomputed as `syncs_per_sec / max(bracket)` — BENCH.md's method section now discloses
  that split rather than leaving the next reader to rediscover it; and both artifacts
  pre-date `72211e1` and carry no `syncs_counted` key, so counted was told from derived by
  arithmetic (invoicing's sync rate equals its throughput exactly at all ten points;
  bench-app's does not).
  **The fairness change, recorded as an OBSERVED property and not a promise (DESIGN §1).**
  Batching moved job-arm service from *certain* to *probabilistic in the queue depth*, and
  it is stated as a bound because the mechanism is exact: arbitration is one uniform draw
  and a flood drains in `ceil(FLOOD / MAX_BATCH)` request-arm wins, so a due job must win
  one of that many coin flips — at flood 100 against `MAX_BATCH` 64 that is two draws,
  P(fail) = 0.25, measured **200/200 pre-batching and 151/200 after**; at flood 1024 it is
  16 draws and 200/200 pass. No guarantee was lost where none existed: T8b kept
  `due_jobs_are_not_starved_by_a_request_flood`, which asserts eventual **completeness**
  (`jobs_done == 101` at the horizon — a count identical whether a request-arm win consumes
  1 request or 64), and **added**
  `due_jobs_are_serviced_before_a_request_flood_exhausts_the_source`, which asserts the
  **interleaving** invariant. DESIGN states what each asserts rather than echoing the first
  name as if the name were the guarantee, and deliberately commits to no bound on degree.
  **OPEN — request-vs-tail contention is UNMEASURED, and this is the sub-project's one
  unmeasured consequence.** Carried here out of a gitignored workspace so it does not have
  to be rediscovered. The verdict is deliberately **split**: certain on the mechanism,
  unknown on the bound. `Ready::Tail` enters the same `ready` vector
  (`kernel/mod.rs:265-271`) and the same flat uniform draw (`:293-301`) as `Ready::Job` —
  no priority, no reserved slot, no out-of-band service path — so DESIGN §1's round-robin is
  fairness *among* tails and not protection *from* the request arm. But T8b's bound does
  **not** transfer, for two reasons its coin-flip arithmetic does not model: a job win
  discharges the job while a tail win advances only ONE tail by at most `TAIL_BATCH_EVENTS`
  ids (`mod.rs:583-591`, `tail.rs:23`), and lag is TERMINAL for a tail — past the retention
  floor it is dropped with the `charpente.gap` frame (`tail.rs:75-89`) where a starved job is
  merely late. Nothing in the workspace measures it; the realtime swarm reaches
  `realtime:gap` only by configuring `event_retention: 4, checkpoint_every: 2`
  (`realtime.rs:110-111`), which is a GC/resume oracle, not a fairness measurement.
  **The test that would settle it, specified rather than left as "someone should measure
  this":** the tail-side mirror of T8b's interleaving test — a `Kernel::step` loop, a
  `SimSource` flooded at t=0 with N event-emitting requests, one subscription registered
  first, arms discriminated via `stats().requests` deltas against `SimSinkHandle::frames()`
  deltas, asserting a frame was delivered while `before.requests < FLOOD`; sweep N seeds for
  the tail analogue of "151/200 at flood 100, 200/200 at flood 1024". A second variant at
  **default** `event_retention` with a long emit-heavy flood answers the sharper question —
  whether a request flood ALONE can drive a tail past the floor — by observing whether any
  `charpente.gap` appears with no injected fault. That second variant is the one that
  matters: it is the difference between a tail being late and a subscriber silently losing
  events. Seed namespace: `0x60C0` is CLOSED at `0x60C0_0013` and `0x60C1` is fully
  allocated `0x60C1_0001`–`0x60C1_0009`; a tail-fairness test opens a new namespace rather
  than extending either.
  **CLOSED by T12 — `scripts/edge-harness.sh` was 30 of 31, and the failure was this
  sub-project's own drift.** `acked_implies_durable_meta_test_has_teeth`
  (`crates/edge-harness/tests/acked_durable.rs`) applies
  `crates/edge-harness/patches/wrong-ack-before-commit.patch` to build a deliberately wrong
  node and prove the wire oracle can fail. That patch was written 2026-07-25 (`d7367f1`)
  against the old request arm — `handle_or_subscribe` then `reply.send` inline — and
  `d301a52` replaced exactly those lines with the batched drain, so the patch had not
  applied since **2026-08-12** — `d301a52`'s own author and committer date, `Wed Aug 12
  15:24:10 2026 -0400`; this entry said 2026-08-13 until T12 corrected it. Nothing caught
  it: every process-touching edge test is `#[ignore]`d, so `cargo test --workspace` cannot
  see it and only `scripts/edge-harness.sh` runs it, which no group-commit task ran — T4
  through T7 all changed production code inside that window. **The oracle itself never
  stopped passing** (`acked_implies_durable_wire_oracle` green throughout); what was
  unproven for a week is that it has TEETH, which in this repo is the more serious half. The
  fixture's own error message named the drift and the regeneration recipe, which is hard
  rule 5 doing its job. T11 shipped no test changes, so it left this red and named rather
  than papered over.
  **What T12 shipped, and why the mutation matters more than the diff.** The patch is
  regenerated against the batched arm and expresses the SAME bug class rather than merely
  applying cleanly — a mutation that applies but injects a weaker bug silently lowers the
  oracle's bar and no test would ever say so. The original acked the client with a synthetic
  200 before `handle_or_subscribe` was called at all, slept 100 ms, then ran the real
  handling and discarded it. The replacement sends that same synthetic 200 to every reply in
  the drained batch before `Group::commit` is called, sleeps 100 ms, then commits and
  discards the result. Both are ack-before-the-group's-fsync in its strongest form,
  ack-before-the-write-is-even-attempted — the form the original chose deliberately, because
  an ack-after-append-before-fsync variant SURVIVES `kill -9` (process death is not power
  loss; `edge-harness/src/lib.rs`'s honesty contract) and would be toothless in this rig.
  Every response the arm would really have produced is still discarded, so GETs and
  subscribes stay corrupted the same way POSTs are — the property `acked_durable.rs` relies
  on when it bootstraps its session and verifies its reads through the STOCK build.
  **RED-first, in that order** (hard rule 3): with the regenerated patch applied to a
  throwaway worktree the oracle FAILED — `acked-implies-durable violated: 40 of 40 acked
  id(s) missing`, every id `status 404`, no rig-failure reasons mixed in; with the patch
  reverted the oracle PASSED and `acked_implies_durable_meta_test_has_teeth` went green.
  **The durable fix, because the fixture breaking was not the defect — nothing telling anyone
  was.** A new un-ignored, zero-syscall guard
  `the_wrong_build_patch_still_applies_to_the_kernel_it_targets` reconstructs the patch
  hunk's pre-image with `include_str!` and requires it verbatim in `kernel/mod.rs`, so the
  ordinary `cargo test --workspace` sweep reds in microseconds when the fixture stops
  matching, and an edit to `kernel/mod.rs` re-runs it by construction. It is strictly
  stronger than `git apply`'s own match, which additionally tolerates a line offset, so it
  cannot go green on a patch the meta test would then fail to apply; it was itself proved
  RED against the stale patch before being trusted. It does not replace the meta test: a
  text match proves the patch applies, never that a build made from it loses acked writes.
  **A second fix was built, measured, and deliberately WITHHELD — `scripts/edge-harness.sh`
  is NOT in CI, and the reasoning is the record.** T12 wrote the CI step, ran it, and found
  the script red in 4 of 8 runs (below) — never on its own account, always on the
  pre-existing failover witness-gate race. **Noah's ruling, 2026-08-19: do not land it.** The
  guard above already runs inside `cargo test --workspace`, which IS in CI, so the defect
  class that caused this task — a fixture that silently stops applying — is now caught
  deterministically on every push, in microseconds. The script would have added only the
  behavioural half (that a build made from the patch really does lose acked writes), and
  trading that half for a gate that flaps near half the time is a bad trade: a flapping
  required gate destroys trust in every red CI prints, which is the exact rationale that
  authorised the step in the first place. A detail that sharpened the ruling — the withheld
  step's own comment said to "treat this step as informational if it flaps", which a bare
  `- run:` cannot do: it fails the job, and nothing short of `continue-on-error: true` makes
  it informational. The mitigation the comment offered was never implemented, so the step had
  no soft mode to fall back to. `.github/workflows/ci.yml` is therefore byte-identical to
  `52a655d` at this commit. Nothing about the rig is reverted: `scripts/edge-harness.sh`, the
  patch fixture and the meta test all stand and are run by hand as before.
  **Deferred, not skipped: `scripts/bench.sh --smoke`.** It needs a settled box and its
  output would feed the one doc CLAUDE.md sanctions for numbers. At dispatch the machine
  carried load average 4.47 with a VM at 42% CPU and Docker running; the smoke run is
  Noah's to hold. Nothing in BENCH.md was published from an unrun gate. **Still deferred and
  still owed at close (2026-08-19).** T12 did not run it either — the box carried Spotlight
  at ~300% CPU, and the whole point of the gate is that a number born on a box like that
  must not reach BENCH.md. The sub-project closes with this gate explicitly outstanding,
  not quietly dropped; the decision sits with Noah.
  **Evidence — T11's close-out sweep** (tree at `b13fcc3`; every command piped through
  `tee`, exit status read as `${pipestatus[1]}` — zsh does not populate bash's
  `PIPESTATUS`, and the idiom was proved
  against a deliberate failure before the sweep was trusted to it): `cargo fmt --all
  --check` clean; `cargo clippy --workspace --all-targets -- -D warnings` clean; `cargo
  build -p guide-examples` (the GUIDE compile gate) clean; `cargo test --workspace`
  **twice**, identical both times — **747 passed / 0 failed / 45 ignored** over 57 libtest
  result lines, plus the two app-level `tests/sim.rs` batches (invoicing and sim-fixture),
  which report their own summary rather than a libtest line: **256 universes per test, all
  green**, four fresh master seeds across the two runs (`0x0d6442a67510baab`,
  `0xa3fdbb77ccbc1cdc`, `0xad4f70fc2a8a7a90`, `0x768b4f3509830a56`); the pinned debug replays `CHARPENTE_SEED=0x8bee1752811ae873`
  (charpente-sim crash_recovery, suite 4/4, and **2 of those 4 actually replayed the pin** —
  the denominator this file's own 2026-08-15 correction says to read) and
  `0x348270b38f166529` (charpente kernel, suite 30/30, **1 of 30 replayed the pin**);
  **`.github/workflows/ci.yml`'s three `CHARPENTE_SEED` lines run verbatim**, which the
  first sweep had missed — the two `--release` mode-skew tripwires `0x8bee1752811ae873`
  (crash_recovery 4/4) and `0xff159ed76d2806e4`
  (`late_follower_seeds_below_the_gcd_base_and_converges --exact`, 1/1), plus the debug
  kernel pin `0x348270b38f166529` (`kernel_jobs_are_exactly_once_under_the_fault_matrix
  --exact`, 1/1) — **and beside it T9's live replacement pin `0xe653f43924f17fe8` on the
  same test, 1/1**, since the older seed is drifted (green against the checker-gap arm it
  was pinned for, still exercising `Kernel::build`'s drain-id fix). All four rc=0; `--exact`
  makes the denominator 1 by construction on three of them;
  `scripts/edge-harness.sh` **30/31 with the single named failure above** (re-run with
  `--no-fail-fast` to enumerate the whole matrix rather than stopping at the first red
  binary: acked_durable 1/2, failover 3/3, hunts 5/5, realtime_resume 1/1, smoke 1/1, lib
  19/19).
  **Evidence — T12's close-out sweep** (final tree, this commit; same `tee` discipline, and
  the exit-status idiom re-proved against a deliberate `false` before the sweep was trusted
  to it): `cargo fmt --all --check` clean; `cargo clippy --workspace --all-targets -- -D
  warnings` clean; `cargo build -p guide-examples` clean; `cargo test --workspace`
  **748 passed / 0 failed / 45 ignored** over 57 libtest result lines — exactly T11's 747
  plus the one new un-ignored guard, which is the entire delta and is why the number moved —
  plus the two app-level `tests/sim.rs` batches at **256 universes per test, all green**
  (invoicing master seed `0xcb1ea04269d37bc3`, sim-fixture `0xb908aebde1629561`); all four
  of `ci.yml`'s `CHARPENTE_SEED` lines rc=0 (`0x8bee1752811ae873` release crash_recovery
  4/4, `0xff159ed76d2806e4` release `late_follower…--exact` 1/1, `0x348270b38f166529` debug
  kernel `--exact` 1/1, `0xe653f43924f17fe8` debug kernel `--exact` 1/1).
  **`scripts/edge-harness.sh` is 4 GREEN and 4 RED across 8 runs at this tree, and the reds
  are reported rather than dropped — the number matters more than the green.** Every red is
  exclusively `failover_wire_oracle` and/or
  `failover_non_converged_tail_is_exercised_and_bounded`, each failing at `login failed:
  status 503 … has not confirmed that its follower … was never promoted`. Nothing else in
  the matrix was ever red: `acked_durable` — the meta test this task exists for, plus the
  new guard — was **2/2 green in all eight**, as were hunts 5/5, realtime_resume 1/1, smoke
  1/1 and lib 19/19. Green runs cost 45-87 s wall, 75-78 s CPU; reds fail fast.
  **This is the load-sensitive race already recorded at 2026-08-15** — same two names, same
  mechanism: a `login()` POST after a mid-run primary restart races sub-project 0b's witness
  gate and loses. The mechanism is stated once, in the OPEN item below; that paragraph is
  authoritative, and in particular the window is a witness round-trip, not the bind race an
  earlier draft of both passages claimed. **T12 cannot have caused it, and that is provable
  rather than argued:** its entire diff is four files — the patch fixture, a doc comment in
  `edge-harness/src/lib.rs`, a new test in `edge-harness/tests/acked_durable.rs`, and this
  file. **`tests/failover.rs` is untouched because that exhaustive four-file list does not
  contain it**, which is the proof that matters here; `edge-harness/` is where it lives, so
  the narrower `git diff 52a655d -- crates/charpente crates/charpente-core crates/charpente-sim
  crates/charpente-cli crates/charpente-macros examples` — also EMPTY — could never have
  spoken to it. What that narrower diff does prove, and the reason it is quoted, is the OTHER
  half: nothing compiled into the `node` binary the harness drives changed either. The box was visibly the recorded adverse
  environment (`mds_stores` 163%, `syspolicyd` 77%, an unrelated `retrace` build at ~100%,
  load average 9.82 off a 15-minute 27.84), and no orphaned node processes were present. So
  **4/8 is an upper bound on a loaded box, not the settled-box rate** — but it is the rate
  actually observed, and 2026-08-15's "31/31 green all three times" at a different commit
  does not license quoting a better one today.
  **OPEN — the failover witness-gate race, and it is the thing that blocks the edge rig from
  CI.** Specified rather than left as "flaky, someone look at it". **Mechanism** — stated from
  the code, because an earlier draft of this paragraph got it wrong in a way that would have
  sent the fixer to the wrong place. It is NOT "the port binds a few milliseconds before
  `Repl::open` finishes." `Repl::open` SHUTS the gate itself: `db.verify_writes(witnesses)` at
  `repl/mod.rs:287` is part of restarting as a former primary. The gate is opened only by
  `resolve_witness_gate` (`repl/mod.rs:651`), called from `Repl::step` (`repl/mod.rs:541`)
  inside the kernel loop **as witness probes answer over the network**. So a `login()` POST
  that arrives well AFTER `Repl::open` has returned still draws the 503. The window is a
  **witness round-trip**, not a sub-millisecond bind race — which is exactly what makes it
  load-sensitive. The harness loses because `ChildNode::restart` treats a TCP bind as
  readiness, and the gate's resolution is asynchronous to that bind. **And the window is
  UNBOUNDED in this rig — do not read `WITNESS_DEADLINE_NANOS` as a bound.** That constant's
  own doc is explicit: "The deadline moves NOTHING — the gate stays shut and the probes keep
  running; it only decides when the operator is told why writes are refused"
  (`repl/mod.rs:78-85`). At the deadline a **strict** node returns `Err` (`repl/mod.rs:549`);
  a **non-strict** one emits a single notice and falls through with the gate still shut
  (`:551-554`) — and the harness's node sets `strict_witnesses: false`
  (`examples/invoicing/src/bin/node.rs:169`). So the 503 ends when a witness answers, at no
  fixed time. This is worth stating precisely because a stated 30 s bound would quietly
  re-legitimise the "just wait longer" remedy the next paragraph forbids: if the window really
  closed at 30 s, widening a timeout past it would be a valid fix. It does not, and it is not.
  `503 "this node is not accepting writes yet: it was a primary before restarting and has not
  confirmed that its follower … was never promoted"`. This restores the framing `:932` and
  `:939-940` already carried — waiting "on the gate rather than the TCP port", against "the
  witness gate's async resolution window" — which a rewrite dropped. **Affected:**
  `failover_wire_oracle` and `failover_non_converged_tail_is_exercised_and_bounded` — the same
  two names 2026-08-15 already recorded failing the same way, so this is a second sighting of
  a known defect, not a new one. **The fix is WRITE-readiness**: `restart` must wait until the
  node will accept a write, not until something is listening. Explicitly NOT the fix: widening
  a timeout, or retrying the job — both hide the window instead of closing it, and the
  2026-08-10 rule ("fix the environment, not the thresholds") applies with more force here,
  since this window is in the rig's own readiness contract rather than in the box.
  **Measurement to beat:** 4 red in 8 runs at this commit, with the caveat that the box was
  loaded (`mds_stores` 163%, `syspolicyd` 77%, an unrelated build at ~100%), so 4/8 is an
  upper bound and not the settled-box rate. Two neighbouring gaps 2026-08-15 left open belong
  to the same fix round: nothing reaps orphaned children when a run is killed mid-test, and
  the harness cannot distinguish "child never reached `main`" from "child failed to bind" —
  which is exactly the ambiguity that makes this race read as a durability failure. **Until
  this lands, the edge rig stays a by-hand gate**, and the CLAUDE.md-level question of when it
  is owed is answered by the guard above for the fixture-rot half and by nobody for the
  behavioural half.
  **CLOSED (2026-08-19, T13) — `engine.rs`'s comment contradicted the two docs that were
  corrected against it, and now agrees with them.** Carried here for the same reason the
  request-vs-tail item is: its only other
  record is a commit message and a gitignored report. The final review found that "a crash
  between append and sync loses the whole batch and never part of it" is false — `Wal::append`
  calls `rotate` mid-batch (`log.rs:270-272`) and `rotate` opens with `tail.sync()`
  (`log.rs:429`), so a batch straddling a segment boundary leaves a durable PREFIX.
  `DESIGN.md` §2 and `docs/BENCH.md` were fixed; `engine.rs` was deliberately NOT, because
  that round was scoped to prose and it is production code. So the tree currently states two
  different things, and the engine comment is the one a reader of the core meets first. Its
  specific defect is the parenthetical "Batch atomicity is unaffected — that barrier falls
  between frames, never between the batch's appends and its sync": a barrier falling *between
  frames* is precisely what makes the prefix durable, so the sentence refutes itself. Safety
  is untouched either way — nothing in the batch was acked — so this is a comment fix, not a
  durability bug, and it must not be written up as one. **The fix (T13, this commit) is
  comment-only**: `submit_all`'s doc now states the batch is not durability-atomic, names the
  `Wal::append` → `rotate` → `tail.sync()` mechanism, and says a straddling batch leaves a
  durable PREFIX with no guarantee moving — the same three sentences `DESIGN.md` §2 and
  `docs/BENCH.md` already carry. No behaviour changed; `cargo test -p charpente-core` was run
  to prove it.
  **README.md carried two sentences its own source disproved, and they are FIXED, not
  flagged.** It said charpente "commits one durable record per transaction with no group
  commit" and called closing the fsync gap "a named future sub-project (group commit), not a
  shipped property" — both false since `3cd3ecd`, and both the M-3 defect class sitting in
  the file a new reader meets first. `README.md` was outside T11's original file list and was
  added to its scope for exactly that reason; the corrections landed in `b13fcc3` alongside
  this entry. It now says an uncontended commit costs one durable fsync which group commit
  amortizes across a batch once requests queue, and that two M2 sub-projects have shipped
  with the amortization half of the gap closed and the device-speed half not.
  **The wire layer stops gating a DERIVED sync rate, and stops publishing one — Noah's
  ruling, signed off 2026-08-19 (T13, ruling (b); ruling (d) landed first in the preceding
  commit).** The trigger was the first `scripts/bench.sh --smoke` run since batching landed:
  11 passed, 1 failed, and after the kernel arm's request count was resized the one remaining
  refusal was invoicing at 4 connections, 576 derived syncs/s against a 266/s ceiling.
  **The argument, recorded because the ruling turns on it rather than on the refusal itself.**
  A physical-plausibility gate is a claim about the SYSTEM; applied to
  `syncs_per_request × throughput` it is a claim about the CONSTANT. Group commit made that
  constant false at every contended point, so the gate could not fail to fire — and a check
  that must fire is not evidence, it is a standing refusal that teaches a reader to skip
  refusals. This ledger's own T10 entry (above) had already recorded the end state: both runs
  exited 1 and `invalid: []` was unreachable.
  **What decided it was an asymmetry the instrument had been carrying unnoticed**, found in
  the T13 investigation and confirmed independently from an earlier run: on the same report
  line, `bench-app`'s wire `device_ratio` — `records_per_request × throughput / ceiling`, the
  same shape of derivation — is published UNGATED at 3.438x the device ceiling and passes,
  while invoicing's derived sync ratio at 2.164x refuses. Two derived quantities, identical
  form, opposite treatment, decided by the order the two fields landed. `syncs_counted`
  existed and was documented inert ("Provenance, not arithmetic — nothing in this module
  branches on it … this flag cannot alter a verdict"), and the human render's
  `(sync, gated)`/`(records, ungated)` labels were string literals.
  **What shipped:** the device-ratio gate runs on a COUNTED sync rate and on nothing else, and
  a derived rate is WITHDRAWN rather than published — `"syncs_per_sec": null` in the artifact,
  `n/a (derived, unsound under batching)` with no sync ratio in the human render. **This is
  not a widened threshold and no number moved:** `RATIO_TOLERANCE_FLOOR` is untouched, and a
  counted WAL sync rate above the counted WAL ceiling still refuses — pinned by
  `a_counted_sync_rate_is_gated_exactly_as_before`, whose fixture is 5x the ceiling.
  Considered and rejected: keeping the derived rate at 1 connection, where a batch of one means
  the multiplier still holds. That replaces the one discriminator the harness observes
  (`syncs_counted`) with a guess about the server's internals from the client's connection
  count.
  **Standing: this exercises the 2026-08-10 T1 fix wave's own escape clause rather than
  overriding it.** That ruling gated a derived quantity on the stated premise that it is
  "sound while one commit costs one fsync, and NOT sound the moment a group-commit primary
  path makes it cost less". That path shipped. **`DECISIONS.md` carries no entry on this gate
  — verified, not assumed — so no reopen is owed**; the superseded reasoning lives in
  `wire_accounting`'s module doc and is rescoped there rather than deleted.
  **Doc claims this falsified, all rescoped to the runs they describe rather than removed:**
  `docs/BENCH.md`'s "every contended invoicing point below is refused rather than published",
  its "on this matrix `invalid: []` is unreachable", its section heading
  "`examples/invoicing` is refused at every contended point, on purpose", its device-ratio-gate
  bullet, and its "one key means two things by layer" note; this entry's own T10 record above;
  and in source, `Target::syncs_per_request`'s future-tense "a future group-commit primary
  path is where this stops holding", `WireObs::syncs_counted`'s inert-flag doc, and both
  harness tests that tolerated the refusal. **Still owed and unchanged:** a counted sync rate
  for `examples/invoicing`. Option (a) — a `/__syncs` counter there — was investigated and
  deliberately not taken: `CountingDisk` lives in `bench-harness` while the example is
  single-dep on `charpente` by design, the route would have to be unauthenticated in an app
  whose every other route carries `auth`, and it instruments the one target whose evidentiary
  value is that it is a stock, uninstrumented real application.
  **OPEN — DECISIONS §7 trigger (a) is owed a re-read, and nothing here performs one.** The
  2026-08-10 reading ("the commit path is fsync-bound at ≈1x the device ceiling, so redb is
  not the binding constraint at this workload") was taken before batching existed, and group
  commit is precisely what removes the one-fsync-per-transaction ceiling it rests on: kernel
  log records/sec went 217 → 5,292 at a 0.36x device ratio. The **core**-layer premise has
  not moved (215 → 222 — a serial one-at-a-time loop has nothing to batch); the **kernel**-
  layer premise has. Whether redb becomes the binding constraint once the fsync ceiling is
  amortized is a live question this measurement raises and does **not** answer — settling it
  needs the per-phase attribution inside the commit path that this instrument explicitly does
  not do. Recorded for Noah's sign-off per CLAUDE.md's workflow rule ("don't silently
  override it — reopen the entry with the new evidence"), deliberately not settled here.
  `PLAN.md` carried a copy of the same sentence, and it is scoped to its 2026-08-10
  measurement in the same fix round that added this paragraph — not in `b13fcc3`, whose
  `PLAN.md` diff is nine added lines and no edit. `DECISIONS.md` §7 itself is untouched
  pending the re-read.
  **ANSWERED 2026-08-20 by the per-phase attribution sub-project (entry below).** The
  re-read exists, it does **not** fire, and `DECISIONS.md` §7 is still untouched — now
  pending Noah's sign-off on a draft rather than pending a measurement that did not exist.

- **M2 sub-project — per-phase commit attribution, DONE (2026-08-20, plan
  docs/superpowers/plans/2026-08-19-phase-attribution.md, spec + ledger + raw observations
  under `.superpowers/sdd/2026-08-19-phase-attribution/` — gitignored, so `docs/BENCH.md` is
  the durable copy — commits `2c9e2d3`..this commit).** Built the instrument that answers
  the question T13 left open above: after group
  commit the fsync is no longer the constraint at batch cap, so **where did the constraint
  go, and is any of it redb?** A tracing `Disk` in `bench-harness` wraps `ProdDisk` under
  both the WAL and redb's `StorageBackend`, records `(path class, op kind, start, end)` per
  call, and `phase_accounting` partitions the measured window into fourteen fields that sum
  to it exactly (asserted). **No `charpente-core` file was touched** — hard rule 1 stands, no
  clock went near `Engine::submit_all`, and the whole attribution is recoverable from the op
  pattern because `submit_all` is N appends → one sync → N applies and a path-class
  transition is a phase boundary. Gaps are attributable only on a single-threaded arm, so
  `attribute` REFUSES an overlapping trace rather than publishing a share nobody can defend;
  that is why the wire arm is not instrumented and cannot be.
  **The measurement, published in `docs/BENCH.md`** ("Where a batched commit's time goes"),
  read at both levels because they disagree and the disagreement is the result. *Field by
  field* at batch ≈62 the largest single phase is still the durable sync — `wal_sync_io` at
  **40.48 / 40.53 / 41.15%**, counted — with `gap_within_store`, the store-side apply
  residual, second at **40.05 / 39.81 / 39.62%**, derived. *Grouped into `submit_all`'s three
  blocks* the same runs read **store path 55.1–55.9%, WAL path 42.1–43.0%, everything outside
  the commit path 1.27–1.30%**; on the unbatched core arm the same instrument reads WAL
  91.3–91.5%, store 7.95–8.17%, `gap_within_store` 0.05%. What is settled either way: group
  commit did **not** move the bottleneck to the kernel's single-threaded serialization
  (~1.3%, and ~1% in all three machine states measured — quiet, post-thermal, 24-spinner). It
  took the fsync from ~90% of an unbatched window to ~40% of a batched one and promoted
  `Store::apply` from 0.05% to a near-equal share: **two comparable occupants, neither
  dominant.**
  **DECISIONS §7 trigger (a) does NOT fire** — corrected from an initial FIRES verdict during
  the final whole-branch review, with no number moving. `SPEC.md:126` pre-committed the
  criterion as "redb apply is the **largest single non-idle phase** at batch cap": field
  level and singular. At the field level a **counted** phase (`wal_sync_io`) leads a
  **derived** one (`gap_within_store`) on all three published runs, so the criterion is not
  met; FIRES had been reached by summing store-side fields into a "store path" aggregate the
  spec never named, chosen after the numbers existed — the post-hoc regrouping the
  pre-commitment existed to prevent, taking the lead with a residual against a measurement
  when the spec's own guard says a derived phase share is not a measured one. Two further
  facts point the same way: ≈92% of the instrument's own 782 ms overhead bound lands in
  `gap_within_store` (correcting for it widens the field-level margin from 0.4 points to
  ≈5.8), and that field is the one the overhead gate never checks. The 0.4-point margin is
  itself inside the instrument's noise amplitude, so the verdict is delivered as "the
  pre-committed criterion is not met", not as "the sync beats the apply".
  **The spec's own stated expectation is falsified, and it is the most interesting single
  result here.** `SPEC.md:119-122` fixed the reference point as "~73 µs per record — so at 64
  connections the fsync accounts for a **small minority** of the per-record budget, and the
  attribution's job is to say what holds the rest." Measured: per-record budget **141.06 µs**,
  `wal_sync_io` **57.09 µs = 40.5%**, append+sync 59.21 µs = 42.0%. The arithmetic was good
  (73 predicted against 59 measured); the characterization was wrong. After a 62× amortization
  the durable sync is the *largest single item* in the per-record budget, not a small minority
  of it — which is also why the criterion's wording could not be stretched: it was written
  expecting the sync to have ceded, and it had not.
  **Both readings of the criterion are sized in the draft, because they are not close to
  balanced.** The reading taken decides *does not fire* by 0.42 / 0.72 / 1.53 points (≈5.8
  after the overhead-concentration correction); reading "phase" as a block fires by 13.7; and
  reading "non-idle" as excluding the device-blocked fsync fires by **28.3–37.1**. The choice
  of reading, not the closeness of the data, settles this — which is precisely why the choice
  is Noah's and is recorded as an overrule if he takes it.
  The draft re-reading is
  `.superpowers/sdd/2026-08-19-phase-attribution/trigger-a-rereading.md`; **`DECISIONS.md` is
  deliberately untouched pending Noah's sign-off**, per CLAUDE.md's reopen-don't-override
  rule. It puts three options to him — record not-fired on fresh evidence; record not-fired
  with a re-read owed on the first bare-metal Linux box (a faster fsync is the likeliest
  thing to invert a 0.4-point margin); or overrule the criterion's reading as block-level
  rather than field-level, which is his to take and should be recorded as such. Not firing
  does not mean the store path is cheap: it is ~40% of a batched commit window, and the two
  cheaper experiments stand either way. (1) `Store::apply` opens one redb write transaction
  PER RECORD inside a batch whose WAL cost is already amortized 62× — group commit amortized
  the fsync 62× and redb 0×, exactly as the spec's §2 predicted, and batching the apply has
  not been tried. (2) The redb `debug-assertions = true` release override, whose sensitivity
  probe is now reported as an **observation with unverifiable build provenance** rather than
  as evidence: no `cargo build` command for either probe binary is in the run log, the only
  machine-readable provenance field reads "debug-assertions on" for all eleven runs because
  it is a literal keyed off the harness's own build mode, the claimed rustc-invocation check
  has no record, and both target dirs are deleted. Its per-run pairing also refutes the
  earlier "the verdict survives the flag" claim: with the flag off the WAL path is the larger
  group in three of five runs. And the change is one line but not a one-line change — it
  re-rolls the sim IO stream and invalidates the recorded seed corpus (`Cargo.toml`'s comment
  and the two pinned cross-mode seeds in `ci.yml`), so it trades determinism for throughput.
  **Two method findings worth more than the numbers.** First, the loaded arm reproduced the
  `MIN_MEASURED_WINDOW` precedent exactly: at 586 ns/op the overhead gate refuses the kernel
  arm's `wal_sync_io` on every quiet run and **passes it** on one loaded run, because
  contention stretched the window and the phase while the threshold is a fraction of that
  phase — a quiet-only run would have recorded the gate as uniformly strict. Second, Task 4's
  reading of `cpu/wall` 0.41–0.52 as "this process got half a core" is **corrected**: the
  same 0.49–0.52 appears on a box sampled at 92% idle, because the kernel arm spends about
  half its wall time blocked in `fsync`. 0.50 is the arm's structural signature; the
  contention read that survives is the spread *between* the two arms of a pair.
  **The instrument's own cost is still not resolved, and is reported as such.** Nine
  alternating pairs on the quietest box available: mean −49.5 ns/op, range [−1285.2, +585.9],
  four pairs negative. Task 4's [−770.8, +619.9] is improved on and not closed. Every
  published phase block carries `phases_overhead_gated: true`,
  `phases_overhead_gate_ns_per_op: 586` (the range's high end) and
  `phases_overhead_gate_resolvable: false` — the refusals that produces mean "this machine
  cannot certify these phases at that amplitude", never "the instrument is too expensive".
  The honest report is "below the noise floor of n=9", never "0 ns/op". **Every IO phase of
  every published kernel run is refused at that amplitude**, `wal_sync_io` included; the
  exception in the published pair is the *core* arm's `wal_sync_io` (plus `store_write_io` on
  core run 2). BENCH.md had this backwards in one sentence and the table now carries the gate
  outcome per row, because the gate outcome is provenance and prose drifts from it.
  **The measurement-discipline claim is stated at the strength its own record supports**,
  after the review found it was not: protocol P3 (load averages within ~1.5x across a window,
  else discard and re-run) was fixed before the numbers existed and is violated by every
  published observation, and it was superseded at 00:33 by a direct `top` idleness check on
  the defensible ground that macOS load average lags — but after the first observations
  existed, and the substitute's own samples were never transcribed. BENCH.md now says that
  rather than claiming "≥88% idle across three consecutive readings", which the run log
  cannot corroborate. Deferred minors **M-3 and M-7 are closed as discharged by this task**
  (BENCH.md names which arm the single overhead figure came from, and carries
  `--overhead-ns-per-op`'s semantics and the gate's meaning); M-6's BENCH.md half is fixed
  with the provenance column, its `report.rs:566` half stays deferred as a code change; M-1,
  M-2, M-4 and r1-M1 stay deferred.
  **No published number moved.** `kernel_bench`'s sync counter stays UNSCOPED and the
  2026-08-15 rows stand as measured; the new runs are new observations on a quieter box
  (kernel 7,089 vs 2,646 acked writes/s, `device_ratio` 0.419–0.429 vs 0.36) and revise
  nothing. 62.0 log records per durable sync in every single run, control and probe alike,
  which is what makes the probe comparison a comparison of one workload rather than two.

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
  The backlog below held **one open ruled follow-up** when this entry was written —
  `applied_delta`'s release-mode wrap, added 2026-08-08 — which was closed by its own interlude
  on 2026-08-10, leaving that section all record. Still-open work, none of it ruled or blocking: `TcpProxy::stop`'s
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
- **M2 sub-project 1 done — the benchmark harness (2026-08-10, spec
  docs/superpowers/specs/2026-08-06-benchmark-harness-design.md (`2b47556`), 10-task plan
  docs/superpowers/plans/2026-08-06-benchmark-harness.md (`be9b05a`) + a pre-execution
  two-review fix wave (`27338b2`), ledger
  .superpowers/sdd/2026-08-06-benchmark-harness/progress.md, commits `2b47556`..this commit,
  34 in all).** PLAN's unfulfilled "publish honest benchmarks early" mitigation, discharged,
  and the instrument DECISIONS §7's LSM build triggers are read against.
  **Shipped:** `crates/bench-harness` — four layers, each a ratio against the one beneath it
  and none published without that one in the same invocation. **device**: two loops through
  the production `Disk` on the target filesystem, sync-only and append-shaped (`write_at` at
  the tail + `sync`, the WAL's own op shape). **core**: `Db::transact` in a closed loop, the
  commit path alone. **kernel**: a `Kernel` fed through the same `RequestSource` seam
  `SimSource` and `ProdSource` share, requests pre-built, no TCP. **wire**: the server as a
  real child process over real HTTP/1.1 keep-alive, two targets — the purpose-built
  `bench-app` and `examples/invoicing`, the latter labeled as measuring an app rather than
  the platform's limit. Plus the `bench` binary (`device|core|kernel|wire|all|smoke`),
  `scripts/bench.sh`, the pinned JSON report model, and the discipline that makes a number
  *refusable*: a CPU-time heartbeat sleep-guard spanning every measured window, totals two
  ways, client-saturation marking (`SATURATION_FRACTION` 0.9 — a point where the client is
  >90% busy measures the client), window and op floors, and the device-ratio gate.
  Two Noah-ruled mid-plan interludes: the **accounting/measuring split** (2026-08-08) making
  every guard, delta and INVALID decision a deterministic unit test rather than something
  only a real benchmark run can reach — adopted from inception by T6–T8 rather than copied as
  an untested pattern; and the **device-ratio gate** (2026-08-10), which found `bench`
  refusing HEALTHY runs at all three gating layers and fixed the bracket's op shape plus a
  noise-aware tolerance.
  **The prediction is CONFIRMED, and it is the headline.** The spec committed to testing one:
  *the core write path measures at ≈1x the device's durable-sync rate — sync-bound, far below
  the headline claim on any hardware.* Measured: core commits **215 log records/sec** against
  an append ceiling of **215–258/sec** bracketed either side of its own window (`device_ratio`
  0.833). The mechanism is not inferred — `Engine::submit` is unconditionally `append` →
  `sync` → `apply`, one durable sync per transaction, no group commit.
  **The most informative result is a column, not a row.** log-records/sec reads **215 / 215 /
  217 / 198–225 / 170–202** down device / core / kernel / wire·bench-app / wire·invoicing.
  Four different code paths — a bare fsync loop, an in-process commit loop, the full kernel
  with job execution, and a real HTTP server driven over TCP — converge on the same number.
  Adding the kernel halves the *request* rate (109 vs 215) precisely because each canonical
  request then costs two durable records, and leaves the *record* rate untouched. The system
  is fsync-bound end to end; at this workload everything above the log is free.
  **The gap is named, not hidden:** MANIFESTO §7 and README claim "hundreds of thousands of
  transactions per second"; this instrument measures 215/sec for the canonical transaction on
  this laptop under `F_FULLFSYNC` — roughly three orders of magnitude. README now says so
  directly above its own scaling paragraph, and BENCH.md's table is captioned NOT the launch
  claim (bare-metal Linux is spec ruling 4 and does not exist yet).
  **DECISIONS §7 trigger (a) does NOT fire; the LSM engine stays deferred** — recorded in §7
  itself, whose "no benchmark exists at all" clause this sub-project retired. Core sits *at*
  the device ceiling, so redb's materialization cannot be costing anything material at this
  rate; reads agree from the other side at 2.65M/sec. **Scoped honestly:** the core layer
  bundles WAL append + sync + apply + store materialization into ONE number, so this rules out
  redb as *the binding constraint at this workload on this hardware* — it does not measure
  redb in isolation. The follow-up this opens is **primary-side group commit**, now sized: the
  engine already has the shape (the follower's `Engine::submit_batch` appends a whole shipped
  batch and syncs once), and the primary deliberately does not use it because acking before
  the batch's sync breaks ack-after-durable. The number to beat is 215.
  **Three BENCH.md defects were found at close-out by checking the prose against the code, and
  none by a reviewer** — the class this repo keeps paying for, a doc its own source disproves.
  (1) The bracket-outlier note claimed the invoicing 1-connection point "would have passed
  against either sample". False: `implausible_bracketed_ratio` is `ratio > RATIO_HARD_LIMIT ||
  ratio > 1.0 + ratio_tolerance`, and against the smaller bracket (47.96) that point reads
  **4.14**, which the 1.5 hard limit refuses outright. Divide-by-the-larger is what kept the
  run valid, and because the 78% bracket spread widens the tolerance arm to 1.78, the *hard
  limit* — not the tolerance — is what bounded that point at all. (2) The window row said "5 s
  warmup, 30 s measure" for every layer; `kernel_bench::run(&dir, requests)` takes neither and
  is count-driven — a fixed 50,000 requests, **460 s** in this run. (3) The
  single-threaded-kernel disclosure cited a 256-connection p50 "over two seconds", true of
  bench-app (2.27 s) and not of invoicing (1.34 s); it now names its target.
  **The smoke stage failed three times before it passed, and the cause is worth recording
  because it is not charpente's.** `scripts/bench.sh --smoke` went red 3 runs running (2, 3
  and 1 failed tests), always the same shape: `point lost: target never answered on
  127.0.0.1:PORT within 10s / <empty — the child is still running but never bound>`, on a
  random target and connection count rather than a fixed one. Root-caused by sampling a
  failing child live: its entire stack was `_dyld_start` at a **96K** physical footprint —
  stalled in the dynamic loader, before `main`, so it had not run one line of charpente. The
  control (a healthy sibling child sampled the same way) showed the full tokio serving stack
  at 2384K. An isolated probe spawning the same binary 20 times bound in **46–66 ms every
  time**, so the binary's own startup is not at fault; `syspolicyd` — Gatekeeper's
  signature-validation daemon, which every exec of a freshly-built ad-hoc-signed binary must
  clear — was at 28.7% CPU with ~85 MB free memory. The box was also carrying **five orphaned
  `node` processes, PPID 1, 4h11m old**, with edge-harness `tmp.XXXX` data dirs — leftovers
  from a matrix run killed mid-test, the hazard the M1 close-out already notes for `git
  worktree`.
  **The discriminating variable is recent heavy load, NOT the orphans — and that correction is
  recorded because the first draft of this entry got it wrong.** Reaping the orphans was real
  hygiene and was initially written up as "the fix"; a later run with **zero orphans present**
  went red the same way, which rules them out as necessary. Across five runs the separation is
  clean and one-variable across **six** runs: **red whenever the smoke ran immediately after a
  full `cargo test --workspace`** (151 s / 128 s / 184 s, with 2, 3 and 1 failures), **green
  whenever it ran on a settled box** (99.75 s / 106.16 s / 103.53 s, 12/12 every time),
  against T9's recorded 101.31 s baseline — so the runtime tracks the verdict as tightly as
  the pass/fail does, 3 for 3 in each direction. Consistent with the
  mechanism: a just-finished 12-core cargo workload leaves `syspolicyd` and the page cache
  saturated, and every child exec has to clear Gatekeeper before `main`. `SPAWN_TIMEOUT` was
  deliberately NOT widened — the plan's Step 1 rule is *fix the environment, not the
  thresholds* — so the operational consequence is simply that **`scripts/bench.sh --smoke`
  must not be run back-to-back with `cargo test --workspace`**; give the box a moment first. **Two follow-ups, neither fixed here** (T10 is docs and
  evidence, not test code): the harness cannot distinguish "child never reached `main`" from
  "child failed to bind", which is why a macOS exec stall reads as a benchmark failure; and
  nothing reaps orphaned children when a run is killed mid-test.
  **Evidence** — the **scoped** sweep this sub-project owes, stated exactly rather than by
  count: bench-harness touches no core code and this commit changes no code at all (its whole
  diff is `docs/BENCH.md`, `CLAUDE.md`, `README.md`, `PLAN.md`, `DECISIONS.md`), so the
  11-stage sweep is not owed and the edge matrix, the pinned seed replays and the release
  mode-skew tripwires were deliberately not run — no replication, auth or realtime behaviour
  is in the blast radius. What ran: `cargo fmt --all --check` clean; `cargo clippy --workspace
  --all-targets -- -D warnings` clean; `cargo build -p guide-examples` (the GUIDE compile
  gate) clean; `cargo test --workspace` **twice**, identical both times — 55 test binaries,
  **695 passed / 0 failed / 43 ignored** (9m31s, 6m22s), and the 693/40 → 695/43 delta against
  the device-ratio-gate baseline reconciles exactly as T9's two added forced-arm tests plus
  its three binary-driving tests moving to ignored (9 → 12); `scripts/bench.sh --smoke`
  **12/12 green twice** (99.75 s, 106.16 s) on the cleaned box, after the three red runs
  root-caused above. Spec §9.4's two gates both held: the pathspec-filtered
  `git log --grep='^bench:' -- crates/charpente-core crates/charpente` printed **nothing**,
  and its positive control printed **24** `bench:` commits — so the empty list is meaningful
  rather than an artifact of no such commits existing. The numbers of record are the
  13:29 full-matrix run with `invalid: []`, archived at `target/bench/report.json`; every
  figure in BENCH.md was re-checked against that artifact field by field while writing this.
- **Bench interlude — `applied_delta`'s release-mode wrap, CLOSED (2026-08-10, this commit).**
  The last open item in the follow-up backlog below, ruled 2026-08-08 and retired here; that
  section is now entirely record.
  **The defect:** `core_accounting::applied_delta`'s `(Some(b), Some(a))` arm was a bare
  `a - b`, while its sibling impossible-state arm `(Some(_), None)` already carried a named
  `unreachable!`. This workspace sets no `overflow-checks` override — the only `[profile.*]`
  entries in the root `Cargo.toml` are redb's `debug-assertions`/`opt-level` ones — so release
  inherits Rust's default `overflow-checks = false` and that subtraction **wrapped**. A
  backwards `last_applied` would publish `log_records_per_sec` ≈ 1.8e19 / elapsed and let the
  run **continue**, flagged only by the non-fatal `delta != ops` crosscheck rather than
  stopping and naming the contract that broke — in exactly the build mode a real benchmark
  runs in. Fixed with the ruled one-liner (`checked_sub` + a panic naming both values), plus a
  rationale comment at the site recording why it cannot be a bare subtraction, so a future
  "simplification" back to `a - b` has to argue with the reason.
  **The headline is how the test had to be proven, and it is not what the backlog entry
  predicted.** That entry said the fix owed "a mutation-proven test". It owed a **two-mode**
  proof instead: under `cargo test` alone a `should_panic` test **passes against the unfixed
  code for the wrong reason**, because debug traps the overflow — just with the generic
  `"attempt to subtract with overflow"` rather than the named contract. So RED was proven in
  both modes per hard rule 3: debug red on the message mismatch, and **release red on `"test
  did not panic as expected"`, which is the bug itself**. Green in both after the fix. A test
  of this shape that is only ever run in debug would be decorative, and the comment on it says
  so.
  **Scope, stated so it is not read wider:** still unreachable through `core_bench::run`
  (`Db::last_applied()` is monotonic and the two samples are taken in window order), so this is
  negative-space assertion work under hard rule 4's "assertions stay on in release" clause —
  a silent-corruption path closed, not a live bug fixed. The `(None, Some(a)) => a + 1` arm can
  also wrap in principle (at `a == u64::MAX`) and was deliberately left alone: outside the
  ruling, and not a state any log can reach.

## M2 follow-up backlog (carried from M1, ruled — written to be read cold)

- **Design C (a durable timeline epoch) — REOPENED 2026-09-07 with a single-node instance.**
  DECISIONS §6 and the 2026-08-06 verifying-peer close-out both ledger Design C in REPLICATION
  terms: "a durable timeline epoch at fork points", "on `Hello`", sharpening a refusal a probe
  already makes. Seed `0x17004e6a572d737d` reaches the same gap **with no replication at all**.
  One node, one misdirected write that moved `wal/…0.log`'s first frame off offset 0, recovery
  cutting at 0, and event id 0 re-minted at seq 1 with different content — two timelines at one
  `(id, seq)`, on a single node, with no `Hello` to carry an epoch. DESIGN §the-cursor's claim
  that the seq half detects the cross-timeline case does not hold here, and DESIGN is corrected
  in the same commit (hard rule 6). **This does not reopen the DECISION** — async replication
  and the bounded loss window stand, and consensus is not implied. It says Design C's SCOPE as
  written is too narrow: an epoch attached to the replication handshake would not cover the
  single-node cut path. Not urgent (the platform owes nothing for a non-durable write — see
  below), but the scope note belongs with the item before someone implements the narrow version.

- **`applied_delta`'s release-mode wrap — RETIRED 2026-08-10, closed by its own interlude**
  (close-out in "Current state" above). Ruled 2026-08-08, fixed with exactly the one-liner
  recommended below; the account is kept here as record, and this section now holds **no open
  work**. `crates/bench-harness/src/core_accounting.rs`'s
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
  **How it actually closed (2026-08-10):** the deferral was right for a reason the entry did
  not anticipate. The owed "mutation-proven test" turned out to be a **two-mode** proof, not a
  mutation: a `should_panic` test run under `cargo test` alone would have passed against the
  unfixed code *for the wrong reason*, because debug traps the overflow — just with the generic
  `"attempt to subtract with overflow"` message instead of the named contract. The teeth are in
  release, where the unfixed code does not panic at all, and that is where RED was proven.
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

## 2026-08-20 — M2 sub-project: per-batch apply (store commit point), COMPLETE

**What it does.** The store now commits a whole contiguous batch in ONE redb write
transaction at `Durability::None`, atomically with the `applied_seq` meta key
(`Store::apply_batch` -> `StateMachine::apply_batch` -> `TableMachine::apply_batch`, the
engine calling it once per batch). Every record in the run commits or none does. Group
commit (2026-08-15) had already amortized the WAL sync 62x; this closes the same gap on the
apply side, which was still paying one write txn *and* one read txn per record. `apply(seq,
..)` is that batch with one record and does exactly the disk work a per-record commit always
did, which is what keeps a never-contended universe tracing identically to pre-batching code.

**Measured (docs/BENCH.md, "Per-batch apply — the store path amortized").** Kernel arm acked
3,444/3,327/3,500 -> 6,265/6,235/6,224 records/s (1.78-1.88x). Traced Disk ops 1.337M -> 123k
(10.8x), i.e. 13.37 -> 1.23 Disk ops per log record. Store block 82.59 -> 13.41 us/record
(6.2x). Criterion fixed before the run (spec §7) MET at n=3: `gap_within_store` 0.23/0.26/
0.23% of the kernel window (bar < 5%), `device_ratio` 0.766/0.772/0.766 (bar > 0.7), 61.96
records per durable sync. Fresh interleaved baseline built at `89cb165` in its own target
dir; no run discarded, and no re-run taken after the criterion was seen to pass.

**Provenance calls that constrain how these may be quoted.** `gap_within_store` fell ~300x,
and that is deliberately NOT published as the size of the win: a residual moves when its
bounding ops move, and this change moved them (`gap_wal_to_store` +6.60 us/record,
`gap_store_to_wal` +1.47). The publishable figure is the 6.2x on the store *block*, which
nets the relocation out. `gap_store_to_wal` rising is not the kernel loop slowing.
`wal_sync_io` per record is unchanged (57.4-58.3 -> 57.4-58.9 us) — that control is what
makes the rest attributable to the apply path rather than to a quieter machine. The
instrument's own cost was not resolvable at n=9 (pooled mean +101.7 ns/op, four of nine
pairs negative) and is published as a noise floor with `resolvable: false`, never as 0.
Side effect worth knowing: recorded ops fell 10.8x, so the instrument's bound fell with
them, and `wal_sync_io` now passes the overhead gate on all three runs — no published
kernel run before this had a single certified IO phase.

**A guarantee was retracted, not quietly weakened.** Implementing `TableMachine::apply_batch`
broke DESIGN §1's claim that followers arrive at *byte-identical* state. Batch boundaries are
a scheduling artifact (the primary's kernel ready-set vs the follower's ship payload), not
part of the log, so byte-identity was never structural once batching landed — it had been an
artifact of the old one-record-per-transaction shape. **Noah's ruling: demote to logical
convergence**, keep the byte-fingerprint oracle only where grouping provably matches, gated
on an observed fact rather than the old `universe_is_quiet` proxy. Byte-oracle coverage fell
from an implied 55/256 universes to **17/256**, which is the demotion's true cost, now
measured instead of argued; a floor of 17 is pinned so it cannot decay silently, with the
code stating that a drop is a finding to investigate and never a number to lower. DESIGN §1
and §7 and `docs/GUIDE.md:2512` were all corrected — the GUIDE still promised byte-identity
after §1 had been retracted, caught by a doc grep, and that is the doc app authors read.

**The T5 review found a Critical worth recording as a pattern.** Sim coverage for the batch
boundary shipped with `is_injected` widened to accept `FaultReason::RedbLatch`, justified in
the code comment as a classification fix for a mechanism the multi-record path could now
reach. It was neither. Instrumentation measured the cited mechanism (a read fault on "stage
2+") occurring **zero times in 8,825 groups**; the real one was a stage error at k==0
leaving an *empty* group, `commit()` returning Ok without touching the store, and the round
then walking into `checkpoint()` on the already-latched handle. And because `is_injected`
also gates the final-recovery arm, the widening made a genuine bug that renders every
`Db::open` permanently unopenable pass the 256-universe property **green with all coverage
satisfied** — proven both ways (green on `0bc1184`, red on the pre-T5 classification). Fixed
at the root: the new arm now breaks the round like `db.transact` does, after committing —
`Group::drop` panics on a non-empty group, so the obvious bare `break` would have been a
latent process abort. 55 runs x 256 = 14,080 universes clean afterwards. *The pattern: a
classifier widened to make a red suite green is a loosened oracle until proven otherwise,
and the justification comment is not the proof.*

**Net new public API: zero.** `Db::store_applied_seq()` was added for a test oracle and then
removed on Noah's ruling — `Snapshot::applied_seq()` was already public and already used in
the same file, so it did not clear the bar that T4's `Store.schedule` digest cleared on
evidence (no alternative existed there; one existed here). `git diff 55f15ec..6c0bd11 --
crates/charpente-core/src/db.rs` is empty. The store-boundary assertion itself was kept but
its claim narrowed: it is the *fourth* layer behind the gapless assert (`table.rs:469`),
`Db::open`'s own assert (`db.rs:1413`) and redb's transaction atomicity, and its exclusive
window is only a store advancing past the log tail *after* `Engine::recover` returned.
Measured `at == recovered-1` in 950/950 observations — zero margin, always.

**Seed gate (T6): GO.** All four load-bearing pins revert-proven on the new code in an
isolated worktree, each reddening for its original reason. `0x1f733f8d9eebf822` was the one
at genuine risk — it drives `tx_workload` itself, and this sub-project moved that function's
entropy draw stream twice — and it survived both shifts (`base=55, seeds=1` unchanged from
T9). A fifth `ci.yml` pin not in the brief was checked too and is live.

**Trigger (a) of DECISIONS §7 — the ambiguity that was awaiting a ruling is dissolved.** On
the new numbers no reading of "largest single non-idle phase" fires it. Field: `wal_sync_io`
led by 0.4-1.5 points before, now leads by 72.13/71.27/73.15. CPU-only: led by 28.3-37.1
points and *would have fired*, now 1.24-1.38% of all-gap time and not even the largest
residual. Block: led by 13.7 points and *would have fired*, now dominated 4.4-4.8x. The
asymmetry that made this Noah's call is gone. Two caveats stay attached: "negligible" is
exact at the field level (0.23-0.26%) while the store *block* at 16.2-17.5% is dominated
rather than negligible; and the remaining 0.42x gap has a named owner — **charpente's call
pattern into redb, not redb** (12.1 of every 13.4 Disk ops were per-record transaction
machinery). `.superpowers/sdd/2026-08-19-phase-attribution/trigger-a-rereading.md` is
superseded by prepended banner, not amended. **`DECISIONS.md` is untouched and remains
Noah's to edit.**

**Open, and NOT introduced here.** `0xff159ed76d2806e4` (the redb `debug-assertions`
cross-build-mode pin) is **vacuous and has been since at least 2026-08-15**: the replay stays
green with the override reverted, and a 10,000-universe hunt found no replacement (T9:
197.43 s zero hits; T6 2026-08-20: 195.46 s zero hits, against a build proven to carry the
reverted profile by an identical follower fingerprint). The structural reason is that the
property is robust to the skew — a diverged universe still converges — so a wider hunt is
unlikely to help, though non-existence is not proven. Spec §4 makes an unrecoverable pin a
merge blocker retired only by Noah's explicit sign-off; **it is recorded, not absorbed.**
`ci.yml:24` is also the only pin comment in that file with no paragraph saying what it
guards, and as written it misleads a reader into thinking the line protects the override; the
guard that would actually catch the override's deletion is a cross-mode fingerprint
comparison, which is a design change nobody has been asked for yet.

**Also open, non-blocking:** the swarm runner's `run_universes_swarm!` arm has no `floor:`
clause (only the `_in`/`_with`/`_with_in` paths do); the follower batch-atomicity test never
reads the store, and the accessor a reviewer suggested for it is the one Ruling 20 deleted,
so closing it is its own `replication.rs` change with its own mutation burden.

## 2026-08-29 — Interlude: the north-star fidelity audit, and CLAUDE.md gets an oracle

**What was done.** An eight-agent read-only audit of the tree at `8a1c28f` against MANIFESTO,
DESIGN, PLAN, DECISIONS and CLAUDE.md, run while the two sub-projects below were both in
flight. **The machinery is clean**: zero hard-rule violations in core and all ten DECISIONS
entries honoured with no silent override — checked, not reported: zero `[features]` sections
anywhere in the workspace (hard rule 2), `use redb::` in exactly one file
(`charpente-core/src/table.rs`), hyper/tokio confined to `serve.rs`, workspace lints at `deny`,
and zero `debug_assert!` anywhere (the only `debug_assert` tokens are `cfg!(debug_assertions)`
reads in bench-harness), so every assertion is unconditionally on (hard rule 4). **Every
finding was in the layer with no oracle** — docs and process, never code — which is MANIFESTO
§2's own law (output quality tracks oracle density) confirmed against the project itself;
GUIDE.md, the one document with a mechanical gate behind it, produced zero findings while the
ungated ones produced all of them.

**So the document every agent orients from got a gate (`13fa32c`, widened `12d8def`).**
`crates/guide-examples/tests/doc_inventory.rs` checks CLAUDE.md's workspace map against
`Cargo.toml`'s `members` and its "exhaustive as written" per-crate test lists against
`crates/*/tests/*.rs`. It ran RED first and named the real drift — the map had lost four crates
(`charpente-tsgen`, `ts-fixture-app`, `edge-harness`, `bench-harness`) and the `charpente` list
had lost `wire.rs` on the day it was added — and the same commit landed the entries that turn
it green. The first version checked only the three crates its own sentence happened to name,
which is a document checking itself; `12d8def` derives the crate set from `members` instead, so
a crate nobody thought to mention still gets checked (3 crates -> all 12 with `tests/`, 47
files), mutation-proved three ways. Its one trap is recorded at the site: the parser's sentinel
is a literal, so reflowing the paragraph would hide the whole inventory — it fails loudly
naming the constraint rather than passing vacuously. The gate paid for itself within hours: the
`archive` merge's CLAUDE.md conflict was resolved toward the superset list because taking the
other side would have reddened it (`4af81a5`).

**Owed to Noah — agents must not self-approve these.** DECISIONS triggers that passed their
dates unread: **§4** (typed indexes, due mid-M1) — censused: of the four `scan::<T>()` sites in
app code, three are legitimate list-all routes and only `examples/invoicing/src/jobs.rs`'s
overdue sweep is a workaround for a missing range index, so the trigger does *not* fire; but
the chain FINDINGS.md -> still-live scan -> GUIDE.md promoting it to a sanctioned idiom is
real, and §4 carries no annotation. **§6** (async replication vs consensus-first, "reopen at M1
exit criteria") was never read and is a month past due. **§10** is closable with evidence,
since the edge executor landed sync poll-shaped. And **§7**'s second reading (2026-08-20,
above) is still invisible in the file — DECISIONS.md was last edited 2026-08-10, so a fresh
agent sees only the first. The drift class is normalisation: a limitation becomes an idiom, a
trigger passes its date, and nothing reddens. Read the triggers as a checklist at every
milestone close. Technical gaps the audit listed, none new and most self-documented by the
code: group commit's checkpoint arm gets zero fault coverage (`checkpoint_every` 4096 vs at
most a few dozen records per universe); `gc_events` writes outside apply and skips
`mix_schedule`, invisible to the fingerprint oracle; `overflow-checks` is unset (the
`applied_delta` interlude above already paid for that once); CI's `crash_recovery` pin has no
`--exact`; `env::var` is unfenced. README's staleness Noah declared acceptable on purpose — not
a fix target.

## 2026-08-29 — M2 sub-project: object-storage archive, COMPLETE

**What it does.** `ObjectStore` (`start`/`poll` over `ObjectOp::{Put,Get,List,Delete}`) joins
`charpente-core::io`, with a seeded `SimObjectStore` (`charpente-sim`) and `FsObjectStore`
(`crates/charpente/src/prod.rs`) as the only production impl — atomic temp-file-then-rename
PUTs, two fsyncs the trait itself doesn't require but a filesystem substrate owes silently.
`Archive::step` (`crates/charpente/src/archive/mod.rs`) is a state machine driven one
transition per kernel iteration as a **sixth** `Ready` arm — start one upload, poll one in
flight, read one `read_budget_bytes` bite, never a loop to completion — so archive work
interleaves with requests/jobs/cron rather than racing them (spec §8). A **generation** is one
logical snapshot plus the sealed WAL segments continuing it under a key prefix; writing
`<prefix>/manifest` is the sole commit point — nothing readable until named, nothing deleted
until the next manifest is durable. `ArchiveConfig::writer` is the identity every manifest
records, checked by string equality at open and before every swap, never derived from anything
a redeploy moves; an archive meeting a foreign-identity manifest `stops` permanently rather
than interleaving generations ("detection, not prevention," spec §4). The pin is **bounded**:
`Archive::open` registers its floor through `Db::add_retention_pin`, composing with
replication's pin via `retention_floor()`'s `min()` (pre-flight finding 1); past `pin_segments`
sealed segments the archive yields rather than stalling the primary (spec pre-flight ruling 3,
Noah's). `serve::archive(app, store, cfg)` is the archive-only edge; `charpente restore --from
BUCKET --prefix PREFIX` rebuilds a data directory to the newest recoverable point and refuses
three cases by name — diverged, ahead, unrelated — with `--force` naming what it destroys
first. A rolled generation reclaims its predecessor's objects **by name**, out of the manifest
and snapshot metadata this writer itself wrote, only after the successor's manifest is durable;
the bucket is never listed (Task 13's grep for `ObjectOp::List` found only the two `prod.rs`
impl arms) — LIST-and-sweep was refused because a sweep whose arithmetic is wrong deletes
objects a restore needs (GUIDE §14, spec §3).

**The oracle and its seeds.** `archive_converges` (`crates/charpente/tests/archive.rs`) runs
256 universes at a fixed pinned base (`ARCHIVE_CONVERGENCE_BASE = 0xA2C0_0001`),
`replication.rs`'s convergence shape for the same reason: `floor:` is a rate only meaningful
against a base that does not move. As merged (`8acab0f`) the three floors are
`CONV_RESTORED_FLOOR = 219`, `CONV_SUBSTANTIAL_FLOOR = 183` (a restored `Db` of ≥200 rows),
`CONV_CHAINED_FLOOR = 169` (a segment chain, not a snapshot alone) — re-measured twice, once at
Task 12.5 when reclamation added `ObjectStore` ops to every universe's sequence (jitter both
directions, not decay), once when the delete axis turned on (pinned-base `restored` 215→219,
others unchanged — floors raised, never lowered to fit a change). Measured over four bases, 256
each: 219/183/169 (pinned), 216/178/177, 219/178/161, 223/170/168. Task 12's original close
found **216 of 256 reach a verdict at the pinned base, and the other 40 say which kind** — 28
`archive:nothing_published`, 12 `archive:verify_never_read_cleanly` (task-12-report.md §3-4;
`0e46ca1`). The **store-latch discovery rate was a lower bound, not a rate**: while a
disk-read-error latch on the archive's own snapshot read was still a fatal `Db::poisoned()`
violation, the batch aborted at the first occurrence and undercounted; recast as a coverage
counter, the true rate is 2 in 768 across three fresh bases, not 1 in 768 (`d8cbef5`) — the
latch is `redb`'s `io_failed`, which `Db::poisoned()` ORs in, closing the investigation the
2026-08-15 entry above left open. And an **unfireable check** —
`an_archive_io_failure_never_poisons_the_db` — passes by construction: `Archive::step` takes
`&Db`, never `&mut`, so no injected fault reaches engine poison or the store latch — a
regression guard, labelled as such rather than live coverage (`e76fbaf`, `da8f4e3`). Seeds
pinned in `archive.rs`, each naming what it reproduces: `0xb803ea7cab19d08c` is the store-latch
seed above (base `0xD9E3_0001`); `0x8304b6d7b52cbe01` found a defect in the property's own
model — it drives the store directly to read the manifest, plant a foreign one and restore, and
must settle in-flight ops first at every one of those seams (`conv_settle`);
`0x7280605429586c42` is another model defect, an outage-window commit (never a job) resurfacing
so the restart carried the NEXT phase's flag; and, from Task 12.5, `0xfadf45b42f89ce91` (pinned
base) is the retry-idempotence check needing a lost-ack object reclamation had legitimately
deleted (fixed by excusing what `store.deleted()` names), while `0x71cec6ac14e0525a` (base
`0xD9E3_0001`) caught the *first* fix's own bug — reading the manifest while an op could be in
flight tripped spec §1's own assert, found only because a fresh base was run.

**Review findings worth recording as patterns.** A **GREEN mutation** at Task 9 fix-round-1
stayed green on a park-cleared retry flag; chasing it found the flag never clears on a busy
node, so one transient store error stalls the archive for the life of the process with no
crash, notice, or gap — C-2 Critical, +0 archive steps over 5,000 iterations, 4,096 records
never archived, recurring at fix-round review as a **read-only-flood** costing 32–34% of
recovery iterations to retry (`a359356`, `7b12081`; fixed with two watermarks separating "can
new work exist" from "should a failed op retry," `91650fa`/`d297979`). A second GREEN mutation
there (dropping the retry hold entirely) also stayed green; probing found `Step::Idle` is never
reached during an outage without it, stalling virtual time — fixed with a threshold-free test,
measured 659 vs. 989 iterations over a 2,000-iteration outage (`d297979`). A **poisoned pin
mutex abort** (review M-4): `Drop for Archive` calling `.expect(..)` on an already-poisoned
mutex would abort the whole process mid-unwind, worse than the leak being fixed — fixed with
`unwrap_or_else(|e| e.into_inner())` (`91650fa`). Task 12.5's **`saturating_add` under a proven
guard** (N3): a mutation proved F2's over-claim guard correctly RED, but a `wrapping_add` swap
underneath it passed 103/103 green until a crafted two-table `SnapshotMeta` summing to 2^64
reopened the enumeration hole the guard was meant to close. A **fix-round agent died mid-task**
on Task 12.5 — "the seventh dispatch death on this plan, the first to leave real work stranded"
— 403 uncommitted lines never proven red; salvaged rather than re-dispatched, verified against
a pristine copy **restored and confirmed by sha256**. And a **notice with eleven runs of
fourteen spaces** (N1): `reclaim_bound_notice` was missing `\`-newline continuations every
sibling notice uses, invisible to `cargo fmt` and substring-matching tests — fixed, class
closed with `!notice.contains("  ")`.

**Retractions and limits, stated rather than engineered around.** Three orphan classes travel
with the design (GUIDE §14, spec §3), only one with a mechanical fix: incomplete multipart
uploads, closed by a **required** bucket-lifecycle rule. Abandoned-generation objects
(abandoned before their manifest, from a gap past the pin bound or a permanent ownership
refusal) print an operator notice but no rule reaches them. A third class prints **nothing**: a
reclamation drain cut short leaves whatever it had not yet deleted, since the next roll only
reclaims the generation it just superseded — "the most common source of orphans" in a healthy
deployment. All three are unreachable by any code, and LIST-and-sweep was refused for the
reason above. `pin_segments`'s default of 8 is **provisional** (ruling 53 voided its old "4×
`retain_segments`" derivation, and its replacement is unmeasured — `docs/BENCH.md` carries no
archive coverage at all). `delete` is idempotent by contract — `FsObjectStore::delete` maps
`NotFound` to `Ok`, which is what made Task 12.5's delete loop silent, and whether it should
stay so is named as open in the delete-axis section below. The larger gap at close was that
**no `object_delete_error` fault axis existed** (`SimObjectStore` never failed a delete),
leaving reclamation exercised on the happy path plus two hand-driven unit tests; a same-day
sibling sub-project (`b36dc30`, `5241838` — its own ledger section) turns that axis on, not
narrated further here. "The archiving standby is not constructible" travels unchanged to close:
`Kernel::build` is private, every public constructor passing at least one of `repl`/`archive`
as `None`.

**The merge.** `4af81a5` ("two semantic conflicts a clean textual merge hid") is the lesson:
both sessions' hunk-overlap analysis called DESIGN.md/GUIDE.md safe, git merged them without
complaint, and the merged tree did not compile. Three textual conflicts resolved cleanly
(CLAUDE.md's test inventory to archive's superset; `main.rs`'s verb union; GUIDE §13's closing
paragraph). Two semantic ones neither analysis predicted: E0061 (`build_and_locate`'s new
`verb` param collided with main's own new caller; the fix closed a live main defect where
`gen-client` told two-binary crates to run `charpente describe`), and E0277×3 (main's new
`Wire` bound made archive's bare-`u64` test fixtures illegal on the wire; fixed with `WireU64`,
fixtures only). Gate: **924 passed / 0 failed** — charpente 532, charpente-sim 137,
charpente-cli 88, charpente-core 81, charpente-tsgen 63, charpente-macros 14, ts-fixture-app 6,
guide-examples 3. `d57f438`/`daa8325` are bare "merge main into archive" syncs; before this
branch's own push to main it also picked up the delete-axis's two commits (`b36dc30`,
`5241838`), so `8acab0f` carries both the thirteen-task project and that same-day follow-on in
one merge against main's disjoint TS-codegen chunk. Task 13's gate (`43c76a8`, per-package due
to the 600s tool ceiling): **803 tests passed, 0 failed**, `guide-examples` clean, no seed
count (docs-only). CLAUDE.md's core-change evidence bar is what the property supplies for the
project as a whole: **1,024 universes across four bases, zero universe-level violations**,
reproduced before and after Task 12.5's re-measurement.

**Plan enumerations that were wrong, and other departures.** `RECORDS_PER_SEGMENT` assumed 512,
measured 1,365 (ruling 34); Task 10's crashed-restore cuts, planned `[3, 11, 29]`, measured at
8 object ops, "two of which sat past the end and would have cut nothing" (`22a3253`); Task 11's
brief named "four" `FsObjectStore` contract tests, the re-run covering all six of Task 1's
(`f64fb78`). The sibling delete-axis plan calls its own later miscount "the fourth wrong
enumeration" in its own numbering — that tally belongs to that section. Separately, the spec's
four pre-flight rulings (2026-08-20, Noah) fixed the shape (a new trait, not `Disk`/`Net`
reuse; restore to the newest recoverable point only; the archive yields on outage; v1 ships no
S3 client); the plan's own pre-flight findings ruled, among other things, that Task 12's
convergence property move to a new `crates/charpente/tests/archive_property.rs` since
`charpente-sim` cannot see `charpente` — overridden **twice**: the plan's own text had first
placed it inside `charpente-sim`, the finding then redirected it to that new file, and Task
12's report finally landed it in the **pre-existing** `crates/charpente/tests/archive.rs`,
reusing fixtures already defined there rather than dragging hyper/tokio/rsa/sha2 into
`charpente-sim`'s dev-dependencies. `ObjectStore` itself was first escalated as a DESIGN §1
amendment marked **[IRREVERSIBLE]**; reading §1's prose rather than its header showed that
wrong — object storage is network, a source §1 already names.

**Net new public API.** `ObjectStore` (`ObjectOp`/`ObjectDone`) in `charpente-core::io`;
`SimObjectStore`; `FsObjectStore::new`; `Db::sealed_segments()`; `Db::seed_schema` ("a
permanent API commitment made to unblock a CLI verb," per its own review); `charpente::archive`
— `ArchiveConfig` (5 fields, no `Default`), `Archive` (`open`, `pin_floor`, `step`, plus
`Drop`), the `manifest` and `restore` modules; `serve::archive`; `Kernel::with_archive`;
`KernelStats::archive_steps`; the `charpente-cli` `restore` verb. `io.rs` now declares eight
`pub trait`s, five of them top-level (`Clock`/`Entropy`/`Disk`/`Net`/`ObjectStore`;
`File`/`Listener`/`Conn` are handed out by `Disk`/`Net`), so DESIGN §1's "fifth IO trait" is
the right count and PLAN.md's "seventh" was drift, fixed in the commit that lands this entry.

**Tree housekeeping, 2026-08-29 evening.** With the merge landed, the `archive` branch and its
worktree (`.claude/worktrees/archive`) were retired, and the sub-project's gitignored SDD
record — the 4,037-line `progress.md`, every task brief, report, review and review diff, 115
files — was moved intact into the main tree at
`.superpowers/sdd/2026-08-20-object-storage-archive/` before the worktree was removed
(byte-compared before deletion; `progress.md`'s header still names the old worktree path, which
is now history rather than a location). Also retired: the detached `t9a-review` worktree, a
2026-08-12 stash holding group commit's T6 pre-flight (its `io_failure_response` and `pub
MAX_BATCH` shipped through the normal path on 2026-08-15, so the stash was a duplicate, not a
branch), and an orphaned 372 MB cargo target directory left beside the repo by an earlier
baseline run. One worktree, one branch, no stash.

## 2026-08-29 — Interlude: the object-delete fault axis, COMPLETE

**What it closes.** Reclamation shipped in the archive's Task 12.5 as its only destructive
operation — the one place the derived artifact can destroy what it derives from — with **no
delete-fault axis in the simulator**: `SimObjectStore`'s `Delete` arm took no roll and
`FaultConfig` had no delete field, so every DELETE in all 256 universes on all four pinned
bases succeeded, and the central safety claim ("correct and still advancing when every delete
fails forever") rested on two hand-driven modes in one unit test. The Task 12.5 re-review
routed it as "owed at the next seed-map break" — a requirement conditioned on an event nobody
had scheduled, owned by nobody, the same routing gap that task had already had to fix once —
and the controller raised it to Noah as hard-rule-3 debt against MANIFESTO §9's wager ("every
path … executed, under fault, thousands of times, and here is the seed") rather than as a
comment-sized one. Noah ruled it its own plan
(`docs/superpowers/plans/2026-08-29-object-delete-fault-axis.md`), sequenced after the
`archive` merge so it would not widen a divergence that was already the branch's largest risk;
it landed the same afternoon as `b36dc30` (Task 1) and `5241838` (Tasks 2-3).

**Both recorded cost estimates named the wrong obstacle, and the fix was an idiom already in
the file.** Round 1 told Noah the axis would break the frozen seed -> config map. Wrong:
`sample_extended_fields` draws each axis from its own `keyed_stream(seed, name)`, so a new
field survives it untouched. The re-review (N4) then blamed `Rng::chance` consuming a draw
before comparing (`rng.rs:59`) even at `p == 0.0`. Closer, but only because of the real
obstacle: `SimObjectStore` rolled put, get and lost_ack from ONE stream, `keyed_stream(seed,
"object_store")`, so a `Delete` roll on that stream would have shifted every later draw and
re-rolled every pinned universe merely by *declaring* the field. The answer sat one level below
where the crate already applies it (per subsystem, per host): the `Delete` arm draws from its
own `keyed_stream(seed, "object_store_delete")`, everything else keeps drawing from
`object_store` in the same order, and `chance`'s eager draw lands in a stream nothing else
reads. Third time on the archive that the answer was an idiom already in the tree (Task 11 I-2
and Task 12 H-1 were the others); the plan says to assume it will be again before assuming it
is not.

**Replay identity was proven, not argued (`b36dc30`).** With the axis at `0.0`, all twelve
convergence coverage counts reproduce exactly at four bases — `0xA2C0_0001` (pinned)
215/183/169, `0xD9E3_0001` 224/179/162, `0x5B71_0001` 224/192/168, `0x3C0D_0001` 225/169/177
(restored / restored_substantial / chained, 256 universes each), identical to the table in
`crates/charpente/tests/archive.rs` — and both Task 12.5 seeds (`0xfadf45b42f89ce91`,
`0x71cec6ac14e0525a`) replay green. An identity test cannot be red before the change (it
measures today's numbers), so its fireability is the five-row mutation table, every row RED and
both files sha-restored after: `Delete` rolling from the shared stream; put/get moved to the
delete stream; a non-zero default in `none()`; the axis sharing an existing axis's stream and
drawing first; the axis dropped from `validate()`'s range table. *Row 4 was wrong on its first
attempt and stayed GREEN*: drawing from a fresh `keyed_stream(seed, "bit_flip")` after
`bit_flip` is assigned perturbs nothing — an inert mutation, not a weak test; green was true
and meant nothing. The honest mutation shares an existing stream and draws first.

**Then it was turned on, with non-vacuity enforced rather than hoped for (`5241838`).** The
convergence property now draws `object_delete_error` through a fourth `conv_rate(&entropy, 40,
40)` beside the other three object axes. At the pinned base a delete fails in **32 of 256
universes, and all 32 still restore** — the Task 2 question (a delete failing mid-drain,
interleaved with put/get faults and whatever gap the universe drew, never costing a restore)
has 256 seeded answers instead of an argument. An axis that is a probability can sample to zero
and leave the property covering the happy path while *looking* like it covers failure, so
`archive:delete_failed` (universes whose inner store failed a DELETE) and
`archive:restored_after_delete_failure` both sit in `require:`: a batch in which no delete ever
failed is a RED property. The fourth draw shifted `snapshot_every`/`pin_segments`/`interleave`
in every universe, so the floors were **re-measured at four bases, not adjusted to fit**:
pinned 219/183/169 (delete failed in 32, restored after 32); `0xD9E3_0001` 216/178/177 (23/21);
`0x5B71_0001` 219/178/161 (23/23); `0x3C0D_0001` 223/170/168 (32/32). Movement runs in both
directions at fresh bases (chained 162 -> 177 at one, 168 -> 161 at another), which decay
cannot do; at the pinned base no floor fell and `CONV_RESTORED_FLOOR` was *raised* to the new
measurement. The 23-vs-21 shortfall at one base is not a refused restore — a refusal fails the
universe with an unclassified error rather than reaching a floor, and that run failed on the
floor alone. Mutation table: axis forced to 0.0 -> both required keys "never occurred", RED;
the failure witness drops `is_err()` -> its own test RED; `roll_delete` always false -> RED.
**Row B exists because the property cannot see it**: with `is_err()` dropped,
`archive:delete_failed` goes 32 -> 174 and the property stays GREEN — a `require:` key is a
floor of one, so an over-counting witness satisfies it as well as an honest one — which is why
the counter's meaning is pinned by its own test. A first attempt at that row was confounded
(forcing the axis to 0.0 also removes the entropy draws and trips the raised floor, so its red
proved nothing).

**The plan's enumerations were wrong four times, which is now its own pattern.** "Both
constructors" was four — `default`, `none`, `swarm`, and `harness.rs`'s pinned-shape literal,
which only surfaced as E0063. `validate()`'s range table was unlisted, and an axis omitted
there is silently exempt from `[0, 1]`, so `5.0` would have constructed and run — a
negative-space assertion under hard rule 4, not bookkeeping. Spec §9, which the plan said to
amend, spans 31 lines and never mentions delete or axis. And `FlakyStore.deletes`'s comment
claiming no axis could ever cover deletes was wrong on every clause. The new field then reached
operator-facing output, caught by `human_failure_block_matches_the_pinned_shape`; the repro
block should show it, so the pinned string grew rather than the field being hidden. Docs in the
same commits (hard rule 6): DESIGN §1 and PLAN.md's object-storage entry now say the axis
exists and is on; archive spec §3 is narrowed to "*reclamation* cannot reach it" and states
that the sim exercises the path that creates interrupted-drain residue **without** claiming an
oracle that observes the residue — Task 2 did not build one and the coverage keys must not be
read as one; the `Delete` arm's owed-axis comment is gone, since this is what it was owed
against.

**Gate.** fmt clean, clippy `--workspace --all-targets` clean, `cargo test --workspace
--no-fail-fast` 73 suites / 1194 passed / 0 failed / 46 ignored after Task 1 and 1196 / 0 / 46
after Tasks 2-3. Merged to `main` at `8acab0f`: the two sides touched disjoint file sets, so
the textual merge was clean by construction, and the message says why that does not make it
semantically clean and that the gate on the merged tree is what does.

**Open, Noah's.** (1) `Rng::chance` draws at `p == 0.0`: harmless once streams are split, but
any FUTURE axis added to an EXISTING shared stream re-rolls that stream's universes; worth a
numbered DECISIONS entry or a comment at `rng.rs:53`. (2) Whether `delete` should stay
idempotent in `ObjectStore`'s contract — `FsObjectStore` maps `NotFound` to `Ok`, which is what
made the Task 12.5 delete loop silent; not reopened here, named because the axis makes its
consequences observable for the first time. (3) The interrupted-drain orphan class still has no
oracle that observes its residue; the sim now creates it under fault, which is what the
coverage keys mean and all they mean.

## 2026-08-29 — M2 sub-project: TS client codegen, IN FLIGHT (chunks A-C landed, D owed)

**What it does.** `Wire` (`crates/charpente/src/wire.rs`) is a compile-time wire contract:
every type crossing HTTP or SSE derives it and produces a `TypeSchema` (19 variants — `Bool`,
eight small numerics, `String`, `Null`, `Int64{signed}`, `Id`, `Option`/`Seq`/`Map`, `Struct`,
`Enum(EnumRepr)`, `Ref`). A bare `u64`/`i64` is not `Wire`, so it is a compile error anywhere
on the wire — above 2^53 a JS `Number` silently loses precision — and `WireU64`/`WireI64`
(string-encoded, `bigint` in TypeScript) plus `Id<T>` (also string-encoded) are the sanctioned
carriers (Task 1 `b8a7dab`; Task 2 `87d209b`). `#[derive(Wire)]`
(`charpente-macros/src/wire.rs`) reads the serde attributes it can express faithfully and
rejects the rest by name (Task 3 `acf4516`; Task 4 adds enums, `73889e6`). `describe()`
assembles a `Contract{types, routes, events, …}`; `#[route]` carries
`response`/`error`/`params` as `TypeSchema`-producing fns and `emits` as `{event, payload}`
pairs naming both the SSE `event:` field and the payload's wire name (Tasks 5-7,
`9e676a0`/`d141916`/`b14319d`; Ruling 46). `crates/charpente-tsgen` (new in Task 8) emits
`client.ts`: interfaces, a discriminated result union per route, and per-event SSE dispatch
keyed by a `Map<string, reviveFn | null>` so an `Int64`-bearing event gets a walker and an
`Int64`-free one gets `null` (Task 9, `1ac64c9`..`af14e74`). `gen-client`/`check-client` (Task
10, `f2594d7`..`9a2bf45`) write and re-check that output; `check-client` exits 1 naming the fix
on drift. `crates/ts-fixture-app` + `ts-fixture/` (Task 11, `ffaf2e7`..`c5d0a15`) are the
codegen's oracle: a minimal charpente app plus a Node-native suite executing the generated
client against real `serde_json` documents through a stubbed `fetch`, not a hand-written
stand-in. **Transport is SSE only** — `grep -ri websocket` over `charpente-tsgen`,
`charpente/src` and both `ts-fixture*` trees returns nothing; PLAN.md's bullet is still one
undifferentiated "WebSockets + TS client codegen (deferred from M1)", splitting it is chunk D's
job below.

**What is proven, with numbers.** `cargo test -p charpente-tsgen` is 2+22+21+18 = 63
(mod/revive/routes/types, chunk C gate table); `-p ts-fixture-app` is drift 4 + samples 3 = 7.
`npm run test` in `ts-fixture/` is 16/16, executing real arithmetic on `Int64` past 2^53 (`n =
9_007_199_254_740_993`, pinned at Task 6 fix round 1, `2b9a657`). The `InvoiceView`
field-coverage floor (chunk C, `1c60fc2`): of 25 fields, 8 had no TypeScript call site
*dependent* on their declared type, closed with an annotated assignment per field plus a
`Record<keyof InvoiceView, unknown>` floor, both mutation-proved. The floor exists because
generated output regenerates on BOTH sides of a Rust change at once — `client.ts` declares the
new type and `samples.ts` carries a document that satisfies it — so a `ubyte: u8 -> String`
mutation regenerated fully and left `npm run check` green until this commit: not a weak gate, a
gate that could not see the change at all. Ten `TypeSchema`/`EnumRepr` shapes had coverage from
generated-source-**text** assertions only, nothing compiling or executing them (B1b review F6);
closed by porting a 13-route synthetic contract into `ts-fixture-app` as a standing oracle
rather than a Rust-side test (Ruling 30, `ac1a054`). `89bb7be` adopts
`exactOptionalPropertyTypes: true`: 4 real errors at BASE, one a genuine bug the flag *found*
rather than flagged — a `skip_serializing_if` key the walker had been materializing as an
explicit `null` when the wire omitted it entirely, fixed with the same conditional-spread
pattern `8a1c28f` had already proven for a sibling field. At the `4af81a5` pre-merge gate the
full workspace was 924 passed / 0 failed across 8 packages, matching the counts above.

**The rulings that changed the plan.** *Ruling 1*: `Wire::reference()` is a new provided method
returning `Ref(WIRE_NAME)` for field/route positions while `schema()` keeps returning the
expansion — the plan's alternative (`schema()` itself returns `Ref`) contradicted Tasks 3/4's
own tests; cost if wrong is a loud `unreachable!` at generation time rather than a wrong
client. *Ruling 2*: `#[event]` emits `#[derive(::charpente::Wire)]`, not
`::charpente_macros::Wire` — the latter forces every app crate emitting an event into a second
dependency, breaking `sim-fixture`'s single-dep build. *Ruling 3*: Task 11, not 9 or 10,
creates `ts-fixture/app` — no task as planned created an app to generate a client from, so exit
criteria were unreachable without it. *Rulings 21/25*: the IR gains `Null` and payload-free
enum variants — serde's own idiomatic `enum Tree { Leaf, Node(Vec<Tree>) }` was otherwise
unrepresentable — timed deliberately before any emitter existed to consume the old shape.
*Ruling 27*: `ApiError`'s `#[derive(Wire)]` came off for a hand-written schema of the real
envelope — faithful to `Serialize`, but every route's `error: Ref("ApiError")` described a
7-variant union the server never sends (it sends `{"error": "<message>"}`), zero overlap
measured per variant. *Ruling 29*: migrate `ReceiptSent.invoice` off bare `u64` inside Task 5
rather than deferring, closing a split Task 5 itself created where two `#[event]`s on one feed
disagreed on string-vs-number for an invoice id. *Ruling 46*: `Route.emits` carries BOTH the
event name and the payload's wire name — the join from a snake_case name to a PascalCase type
has no other source — resolving the unmounted-event gap completely rather than routing around
it (cost if wrong: a wide but shallow change, paid regardless since Task 9 needed the diff
anyway).

**Review patterns worth recording.** Twice a "fidelity" test asserted only on serde's own
output and could not redden under any charpente edit (Task 3 NEW-2, Task 4 N1/Ruling 22) — now
a standing rule that a ground-truth pin on serde must be *labelled* as one. A self-referential
type (`Vec<Node>`) genuinely stack-overflowed through Task 1's shipped `reference()` callers
and survived a full opus review plus a scoped re-review before Task 3's implementer found it —
both had verified `reference()` correct without checking that anything called it (Ruling 20,
"pin the behaviour, not the function"). Task 4's "measured" fidelity table had three wrong
cells; the reviewer actually measured and none agreed (Correction 2). B1b's conditional-spread
fix for a materialized-null bug needed its coverage gap named THREE times before it closed for
real (review F3 → N1 fix round → R1: each prior "fix" stayed satisfiable by deleting the field
it guarded, passing vacuously). A `$?`-after-`head` error produced a false "silent success"
reading of the `npx tsc` stub that agreed with what this session already believed (Ruling 39:
"a measurement error that agrees with your prior is the one you will not go back and check").
Standing instruction: ask of every green oracle what it is architecturally incapable of
observing, not merely how strong it is — the question the sample oracle's `ubyte`/`String`
blind spot (above) and B1b's thrice-named gap both answer independently.

**The `npx tsc` hazard — corrected finding, quoted from the sub-project's own close-out text:**

> A fresh `git worktree` has no `node_modules` (gitignored, never copied from the main
> checkout). With none present, `npx tsc` resolves the npm package literally named `tsc` — `npm
> view tsc version` reports 2.0.4, deprecated — not TypeScript. Measured 2026-08-29: that stub
> reads no source file, emits no diagnostic, and exits 1 with "This is not the tsc command you
> are looking for" plus the fix. The hazard is therefore a confusing red, not a false green: a
> gate that dies citing a 2016 package nobody installed, on a repo that pins `typescript:
> 5.9.3`. `ci.yml`'s own comment — "type-checking with a nine-year-old compiler is worse than
> having no gate" — is wrong in the same direction; it never type-checks at all.

Supersedes an earlier "silent false green" framing both this session and the archive session
had stated without running the stub. Kept: CI masked the resolution purely by step ordering
(`npm ci` before `npx tsc` gave the real binary whenever it ran), and a type-check is
structurally half a gate — `tsc --noEmit` proves a call site type-checks, never that a runtime
value matches its declared type, which is how the Int64 bug shipped through a green gate
originally. Fixed at `f81d35a`: `ts-client` now runs `npm run check`, enforcing `tsconfig.json`
(including B2's flag) by construction. **The job had never executed on a runner before
`8acab0f`**; `cd52216` records its first result — run `33272881382`, both jobs green — retiring
the "unconfirmed" caveats (Node resolution, `npm run test`'s cwd, `dist/`-not-yet-existing
ordering, the last fine *by construction*) for one real residual the job cannot check: the
runner is case-sensitive, a macOS checkout usually is not — audited (43 relative imports, 16
files, 4 specifiers, all exact-case) and clear today, flagged to re-audit if it ever reddens.

**What is owed — chunk D, per
`.superpowers/sdd/2026-08-20-ts-client-codegen/chunk-d-brief.md`.** A1 (verify no Task 5 `u64`
regression in `examples/invoicing`) is **done as a written finding, no code changed**: every
wire position carries a `Wire` type; survivors are a `#[job]` payload (deliberately outside
`Wire` — jobs never cross HTTP), five `.raw()` calls inside `format!` strings, and `#[table]`
row types no route echoes. A2 (CLAUDE.md's workspace map) and A3 (PLAN.md/DESIGN.md contention)
are both **void** — all twelve `Cargo.toml` members are already named in CLAUDE.md's map, and
the archive session has released both files. **Still outstanding**: DESIGN.md §3 (typed ids
surviving the process boundary) and §7 (`describe` carries types, not only topology);
`docs/GUIDE.md`'s client- generation workflow as its own §14, inserted before archive's "The
archive" (currently §14 at `docs/GUIDE.md:4368`) which chunk D must renumber to §15 including
its cross-references; the four `check-client` exit codes (0/1/2/101) matching `genclient.rs`'s
doc comments; and PLAN.md's M2 bullet, still one undifferentiated line (`PLAN.md:131`), split
into a shipped codegen bullet and a still-deferred WebSockets bullet as chunk D's own last
commit. **One item `4af81a5` flagged as owed turned out not to exist**: `describe.rs`'s
`exit_code_for` prints "rerun charpente describe" and was believed reachable from
`gen-client`/`check-client` via `contract_json` — verified against the shipped code,
`contract_json` has always used its own `.output()` handling and `exit_code_for`'s only
non-test caller is `run` (`charpente describe`'s own path), so there was never that call-graph
edge (Ruling 38: "a claim about the call graph is a claim, not a reading"). **Net new public
API** — `charpente::wire::{Wire, TypeSchema, WireU64, WireI64}` and `Id<T>`'s serde impls,
`#[derive(Wire)]`, `describe::{EventDef, EventEntry, events![]}`, `#[route]`'s
`params`/`request`/`response`/`error` fns and `emits(...)`, the `charpente-tsgen` crate,
`gen-client`/`check-client` — is named in DESIGN.md/GUIDE.md as of Tasks 5-10's fix rounds
except the two DESIGN.md paragraphs above. No WebSocket transport shipped anywhere in the tree.

## 2026-09-03 — tenant-scoping: the checkpoint is held, not merged; three items discharged

**The branch is not mergeable and the reason is hard rule 3, not a defect.** Tasks
1-7/6b/6c/9 landed the whole tenant mechanism across 31 commits; Task 8 — the flagship sim
property, its two naive victims, and the `require:` keys — does not exist. The final review
already caught this class once (Important 2, labelled hard rule 3: the scoped seed path and
scoped archive/restore had zero fault-matrix coverage, mutation M3 caught by exactly one
deterministic test) and R52/R56 back-filled two 256-universe swarms. What remains uncovered
is the four-read-path property itself: `get`/`scan`/`scan_from`/`scan_by` under the full
fault matrix with two tenants, including both tenants indexing the SAME key value — the case
the plan names as the one that catches a forgotten prefix, "without it the property certifies
less than it appears to." Until it exists the branch's headline claim is asserted, not proven.

**On R3's reorder, for the record.** R3 moved the overlay widening (plan Task 9) ahead of the
key/scan/index tasks and argued the plan's "instrument before the risky edit" intent was
"preserved in substance by the existing suites and the existing BENCH.md baseline." That
argument is sound *for Task 9*, which at that point was a pure slot-0 refactor with no scoped
table in existence. It was never extended to Task 8 also landing after 4/5/6/6b/6c/7, which
are the substantive scoping edits, and that lapse was not re-ruled. This is not a finding
against the landed work — it is why Task 8 is a debt being discharged rather than the next
item on a list.

**Ruling R58 — `tenant:crash_mid_batch` is reachable; it stays, scoped to the two states that
exist.** Read from `engine.rs::submit_all`, one group batch is exactly: N x `wal.append` ->
ONE `wal.sync()` -> ONE `machine.apply_batch`. Therefore:
- **State 1, crash between two appends, pre-sync — REACHABLE.** Nothing is acked (ack is
  strictly after durable), and recovery takes whatever survived the tail.
- **State 2, crash after sync and before apply — REACHABLE.** All N records durable, store
  `applied_seq` N behind. This is the staging/apply boundary the plan's comment names.
- **State 3, a partial apply inside `apply_batch` — UNREACHABLE by construction.** Per-batch
  apply (M2, 2026-08-20) commits the contiguous run's row effects atomically with
  `applied_seq` in ONE redb write transaction; a crash mid-transaction rolls back to a state
  indistinguishable from state 2. **The key must not be written to require state 3** — a
  `require:` key whose state is architecturally unreachable either reds every batch forever
  or gets quietly dropped, and either way certifies nothing.

  The obstruction the final fix wave hit was never reachability. It is that every existing
  swarm writes through `Db::transact`, documented at `db.rs:2092` as "a group of one
  (spec §1)" — with N=1 there is no *between*. Task 8 therefore builds the batch itself:
  `Db::begin_group` and `Group::stage` are both `pub`, so the property stages N >= 2
  transactions from two tenants **sharing an id inside one batch** — precisely the aliasing
  defect R3 predicted — and crosses a fault point inside it.

  **No `KernelStats` change is needed.** The fix wave's note that "`KernelStats` has no batch
  composition" is accurate but not binding: `Sim::cover` is `pub` and properties credit keys
  directly (`auth.rs:542`, `archive.rs:7554`). The property owns the batch construction, so it
  owns the observability.

**Docs-only helpdesk build — findings carried over.** The Step 4 Half A experiment
(branch `docs-only-helpdesk`, 5 commits, 10k universes green, master seed
`0x12124e6ebd9cf8b9`) is durable: its objects always lived in the main repo's shared `.git`,
and the branch is now pushed. Its four findings, F1 first because it is a product defect:
- **F1 (hard rule 5, confirmed by controller replay).** The July `catch_unwind` fence around
  `Store::open` (redb `page_manager` :231/:257, commit `1c439d5`) lets the DEFAULT PANIC HOOK
  print `panicked at ...page_manager.rs` before the panic is caught: **134 such lines on a
  fully green 10k-universe batch.** The docs-only agent read them as failure and discarded a
  correct full-App-stack property. DESIGN §7 and hard rule 5 make the fast, precise *no* the
  product; this is the inverse — a false alarm that cost a competent agent good work. The
  fence should silence the hook for that scope or print one named line. Repro: that master
  seed.
- **F2.** `require: ["restart"]` never credited on a property built over
  `acked_implies_durable` — whether the checker's restart path bypasses `Sim::restart`'s cover
  call is unresolved. Same class as R58: a coverage key that cannot fire. Two independent
  sightings is a pattern, not a coincidence.
- **F3.** GUIDE has no tenancy section; the app derived org from the session subject and
  filtered by hand everywhere. Expected — Task 13 owns it.
- **F4.** Route bodies need `serde::Serialize` for the TS client and no GUIDE example shows
  it.

**R57's bypass proof, completed.** The R57 seal (`UnscopedRow: Row + sealed::NotScoped`, so an
app cannot re-open a scoped table with a one-line orphan-legal `impl`) shipped with implementer
testing but its INDEPENDENT mutation proof was killed mid-run — between applying the mutation
and running the check — leaving the bypass re-opened in the working tree with a green-looking
HEAD. Reverted, and both mutations have now been run to completion, foreground, with the
restore verified against HEAD's blob rather than a dirty-count:
- **Mutation A**, unseal `UnscopedRow`. Baseline
  `compile_fail/unscoped_row_impl_from_an_app.rs [should fail to compile] ... ok`, exit 0.
  Mutated: *"Expected test case to fail to compile, but it succeeded"*, `macro_errors FAILED`,
  exit 101. **RED.**
- **Mutation B**, move `S::authorize` ABOVE the one-scope guard in `Tx::open_scope`. Baseline
  `a_refused_second_scope_never_reaches_the_apps_authorize ... ok`, 46/0. Mutated: FAILED at
  `engine.rs:2311` with `left: [1, 2]` / `right: [1]` — the values PREDICTED before the run,
  matched exactly. **RED.**
Both restores verified byte-equal to HEAD. The seal and the check order are load-bearing, not
decorative.

**Housekeeping.** `main` (5 doc commits), `tenant-scoping` and `docs-only-helpdesk` are pushed
to `origin` for the first time. The helpdesk worktree's objects were never actually at risk —
worktrees share the main repo's `.git` — but the branch was single-copy and unpushed, and its
untracked `HELPDESK-BRIEF.md` was genuinely perishable; it is copied to
`.superpowers/sdd/2026-08-30-tenant-scoping/`, keeping R54's "not to be committed" ruling.

**Task 8 landed the same day (`f453a95`), and the debt above is discharged.**
`every_scoped_read_returns_only_its_own_tenants_rows` — 256 universes at base `0x7E4A_0001`,
plus 768 more over three fresh bases, green at every one. All four read paths, two tenants
sharing both an id space and an index key. The two naive victims are committed in the
auth.rs meta-test shape rather than as a feature flag (ruling R6), each paired with a control
that proves the CORRECT read passes the same oracle on the same bytes, and each asserting
`leaked.len() > own.len()` so an inert victim fails the meta-test instead of passing it.

`tenant:crash_mid_batch` is credited exactly where R58 says it may be —
`staged >= 2 && failure.applied == 0 && !was_poisoned` — the fault landing inside the
append -> sync -> apply window. The checkpoint arm (`applied == n`) is excluded as
after-the-fact, and nothing credits the unreachable partial apply. The property builds its
batch with `Db::begin_group` + two `Group::stage` calls at the same id, per R58; no
`KernelStats` change was needed. It uses its own scope type `Firm` rather than `Org`, forced
rather than stylistic: `Org::authorize` admits exactly one tenant so it cannot host N >= 2
writable ones, and `AUTHORIZE_CALLS`/`BODIES_ENTERED` are process-wide statics an existing
unit test asserts are exactly 1, which a parallel swarm would flake.

Verified by re-run rather than by reading the report: 6/6 green, exit 0, at `f453a95` with a
clean tree. Because `require:` carries a floor of one, that green is itself the proof both
keys fired. **Still owed before merge: Tasks 10, 11, 12, 13**, and the workspace suite +
`cargo clippy --workspace` (Task 8 changed one test file and nothing else, so only
`-p charpente` was run).

## 2026-09-04 — tenant-scoping Tasks 10 and 11, and a finding that is not about tenancy

**Task 10 (`65821f0`) — cross-tenant enumeration is a declared capability.**
`#[route(…, cross_tenant)]` / `#[job(cross_tenant)]`, `Ctx::cap()`, and
`Tx::across_tenants::<S>(&self, cap: &CrossTenant) -> AllTenants<'_, 'a, S>` whose `scan`
yields `Vec<(Id<S>, R)>` — rows PAIRED WITH THEIR SCOPE, never bare `Vec<R>`, so a
cross-tenant result cannot be handed to code expecting one tenant's data (spec §3). The flag
joins `auth` in `charpente describe`, which is what turns an audit into an inventory.

The token is unforgeable from a handler: `CrossTenant`'s field is private and
`Group::cross_tenant_cap` is its only constructor, with `begin_group` living in `impl Db`
(never on `Tx`). A handler holds `&mut Tx` and `&Ctx` and can therefore never mint one; a `Db`
holder can, and R48 already classifies a `Db` holder as the operator. **This is the second
place the R48 boundary is now load-bearing**, which is why the DECISIONS entry it is owed
matters more than when it was first ruled.

Three deviations from the plan, all recorded in the commit and all sound: `across_tenants` is
infallible because `ApiError` cannot be named from core and a `Result` with no reachable `Err`
would be a lie; `Ctx::cap()` alone cannot deliver a compile-time property (with `Ctx` a single
type, an undeclared route calling `across_tenants(ctx.cap())` is a runtime `Option`, not a
compile error), so the token arrives as a macro-filled `cap` argument with `Ctx::cap()` kept as
the reflective accessor; and minting is gated on `Group` because Rust has no cross-crate friend
visibility.

**An oracle defect caught during the task, worth recording as a pattern.** Both compile_fail
cases were first written into ONE file, and the `#[route]` macro error aborts the compile
before type checking — so the token-forging attempt produced no error at all and the pinned
`.stderr` certified nothing. Split into two files. A compile_fail case that passes for an
unrelated reason is the compile-time twin of a vacuous property.

**Task 11 (`2270c11`) — a scoped transaction's events are keyed by their scope.**
`ScopedTx::emit` delegates to `emit_keyed` with `ScopedTx::event_key`, so spec §4's "one
tenancy mechanism, not two" holds. The test was written first and its FAILURE DIRECTION read
before the fix, which the plan specifically asked for and which mattered: it failed
fail-closed — tenant A received NOTHING, not B's event, because a keyed subscription receives
no unkeyed events. A one-sided "A never sees B" assertion would have passed vacuously, so the
committed test is two-sided with a third subscriber pinning the literal key encoding.

`event_key` is public and DECIMAL by deliberate choice. Public so emitter and subscriber cannot
drift into two spellings of one tenant; decimal because the key is durable — stored in
`EventRow.key`, shipped by replication, exact-matched on resume — which rules out `type_name`
and `TypeId`, neither stable across builds. A key whose spelling moved under a binary upgrade
would make every pre-upgrade backlog event silently invisible to a reconnecting subscriber. The
cost, stated rather than hidden: two DIFFERENT scope types sharing a raw id share a key.

**The hard-rule-3 follow-up (`3b2457f`), and why it was not optional.** Task 11 shipped its
core change with unit coverage but no fault-matrix coverage, reasoning that adding a scoped
emit to the flagship workload would perturb its pinned seeds. The premise is true; the
conclusion does not follow, because **ruling R56 already settled this exact case** — when a
scoped table moved `archive_converges`'s floors, the answer was a SIBLING property with its own
base and measured floors, not dropped coverage and not a re-pinned original. Both siblings
still stand at `archive.rs:8769` and `:8817`. Since the emit key is durable and resume-matched,
a crash between emit and durability, or a follower replay, must preserve it — squarely a fault
surface.

`a_reconnecting_tenant_subscriber_misses_nothing_and_leaks_nothing`, base `0x2EB2_5C09`, 128
universes, floors pinned AT the measurement with no margin: `tenant:both-tenants-emitted` 77,
`tenant:own-backlog-verified` 79, `tenant:foreign-backlog-not-delivered` 85,
`realtime:resume-backlog` 128. The original property's coverage block is byte-identical before
and after, and the diff is **562 insertions, 0 deletions** — the flagship's lines were never
touched.

**The design point that decides whether the sibling is worth anything.** The flagship's
`Client` validates every delivered frame against `charpente.events` — the very row whose `key`
is in question — so an emit writing the WRONG key writes it into the oracle's own ground truth
and the oracle agrees with it. Under Task 11's own constant-`event_key` mutation a naive
sibling settles GREEN. So each event carries its emitting tenant in its PAYLOAD and the oracle
compares the durable key against that instead. **A self-referential oracle is a vacuous
oracle**, and this one was caught only because the victim was run before the property was
trusted.

**FINDING, pre-existing and NOT about tenancy — owed its own follow-up.** While sweeping fresh
bases, the sibling failed at 2 of 3 with "client never caught up". Chased like-for-like by
pointing the sibling's own invocation at the flagship's untouched workload:

| base | flagship workload | scoped workload |
|---|---|---|
| `0x9F3A_0001` | FAIL | FAIL |
| `0x41D7_0001` | FAIL | FAIL |
| `0xC58E_0001` | pass | pass |
| `0x2EB2_5C09` | **FAIL** | pass |

So the fragility is the SHARED SETTLE BUDGET (`SETTLE_ATTEMPTS` = 12, `SETTLE_BOOT_TRIES` = 64
at `realtime.rs:653/656`), not the scope: **`reconnecting_client_misses_nothing_under_the_fault_matrix`
is green as a property of its pinned base `0x2EB2_0001`, and fails at other bases.** CLAUDE.md's
evidence bar is "sim suite green over N fresh seeds", which this M1 flagship does not currently
meet. The budget was NOT raised — doing so would move the flagship's pinned coverage.

This is the **second instance of one class**, not an isolated bug. `RT_RECOVER_ATTEMPTS`
(`realtime.rs:1443`) already yields `{INCONCLUSIVE}` on exhaustion (`:1637`), and the open
"43% of the convergence property reaches no verdict" item is the same shape: a budget constant
whose adequacy is a property of the seed rather than of the code. One fails loudly and one
fails silently; neither is robustness. Recommend a single follow-up covering both rather than
two point fixes.

**Verification note.** Tasks 10, 11 and the sibling were each re-run by the controller rather
than accepted from the implementer's report. That caught one misreported figure: Task 10's
report claimed `charpente-core --test engine` at 56 passed; it is 46, and `engine.rs` is not in
that commit at all, so the number could not have moved. Green either way and nothing hidden,
but not reproducible as written.

## 2026-09-04 — Task 12, and the wire layer is not in CI

**Task 12 (`fef12f6`) — two tenants over real TCP.** `crates/edge-harness/tests/tenant_wire.rs`
plus `examples/invoicing/src/bin/tenant_node.rs` as the vehicle: invoicing's own tables predate
`scoped_by`, so the rig needed a sibling of `node.rs` whose `Doc` is
`#[table(scoped_by = Org)]`, with routes covering scoped scan, scoped `#[index]` lookup, and a
scope-keyed SSE feed. `--legacy-unscoped` serves the pre-scoping shape of the same table for the
mixed-version arm alone. Both tenants deliberately use the SAME document ids and the SAME index
tag, and every assertion is two-sided, so Task 11's fail-closed vacuity cannot pass here.
CLAUDE.md's edge-harness inventory updated in the same commit — required, since
`doc_inventory` enumerates `tests/*.rs` by `read_dir` and would otherwise go red.

**R60 discharged — the mixed-version arm exists and refuses by name.** A primary with a scoped
`Doc` ships to two followers; the control converges, the legacy follower prints byte for byte:

> charpente kernel stopped on a platform IO failure — restart to recover: record decode: table
> "Doc" is not tenant-scoped (Row::Scope = Unscoped) but this Put effect carries scope 1 (tag
> 0x05/0x06) — the record was written for a scoped shape of this table; restore from a replica
> or backup, or replay under the schema that wrote it

and holds no misread row afterwards. **The reason this pairing is reachable at all is the
finding inside the finding:** the handshake's schema digest covers a table's fields and version
but NOT its scopedness (`schema_digest_of`), so a legacy follower is admitted and only the
record-level refusal stops it. A genuinely older `PROTO_VERSION` peer never reaches a record —
it is refused at the handshake, covered by `charpente/tests/replication.rs` and `protocol.rs`'s
tag-14 pins. Three mutation proofs, each with a revert-and-rerun control: constant `scoped_key`
reddened the HTTP oracle, `event_key` returning "0" reddened the SSE oracle, and
`resolve_effect_scope`'s `(false, Some)` arm returning `Ok(scope)` produced a raw `Writer::put`
assertion instead of the named refusal — exactly the distinction hard rule 5 draws. The
inventory gate was proved fireable too, by removing `tenant_wire` from CLAUDE.md.

**FINDING — the entire wire layer is outside CI.** `#[ignore]` is the edge-harness convention
(every file in the crate uses it; the line is drawn at syscalls), and Task 12 followed it
correctly. The consequence, counted rather than estimated: **15 `#[ignore]`d tests across
`acked_durable` (2), `failover` (3), `hunts` (5), `realtime_resume` (1), `smoke` (1) and
`tenant_wire` (3) never execute in CI.** `.github/workflows/ci.yml:34` runs
`cargo test --workspace`, which skips ignored tests, and the workflow's ONLY `--ignored`
invocation is `:75`, the bench-harness passthrough guard, named `--exact`.

This is pre-existing and already sat in the follow-up list as "`#[ignore]`d real-disk guard
tests are not run by CI". What is new is the consequence: **M1's exit criteria were discharged
with "an oracle at two layers — an in-sim property plus a wire oracle", and the second layer
has never run automatically.** It is a local-only gate. Task 12 has now put tenancy's wire
oracle in the same position, which is correct convention and still means the guarantee is
guarded by a human remembering to run it. Deciding this is Noah's; the honest options are a CI
job that runs edge-harness with `--include-ignored` (cost: real process spawns and TCP on a
metered runner) or an explicit written statement that the wire layer is a local gate, so the
exit-criteria claim stops implying otherwise.

**Three instances of one shape in this branch alone, worth naming.** A `require:` key whose
state is unreachable by construction (R58). An oracle that validates against the row whose own
field is the thing in question, so it agrees with the bug (the realtime sibling). And a test
suite that exists, passes, and never runs (this). All three are green-looking coverage that
cannot fail, and none is caught by reading the test — only by asking what single change should
make it red, and then making that change.

## 2026-09-04 — Task 13, and the tenant-scoping plan is complete (13 of 13)

**Task 13 (`1673bfb`) — the docs catch up with what shipped.** DESIGN §3 gains a `Scope`
bullet; **DESIGN §8's "deferred to its own design" sentence is REPLACED, not left standing
beside the new text** — the specific drift a previous review caught. `docs/GUIDE.md` gains
§16 "Tenant scoping" (~370 lines, two extractor-compiled ```rust blocks), PLAN's M2 bullet
moves to shipped in the shipped-bullet voice, and M1's deferral line stops claiming
multi-tenancy is a convention today.

**DECISIONS gains two entries and loses a stale number.** §11 records spec §3's hard-rule-2
reading so it binds future work: anything crossing tenants must be a declared, enumerable
capability carrying the tenant in its result type — not an escape hatch. §12 is written as a
**DRAFT explicitly marked "Status: not ratified"**, stating the R48/R49 boundary (the tenancy
guarantee covers the typed surface handed to handlers and jobs; a `Db` holder is the operator)
together with its evidence and the open question, because CLAUDE.md sends a decision of that
shape to Noah rather than letting a task write it in as settled. It is now load-bearing in two
places, not one: `CrossTenant`'s only constructor is `Group::cross_tenant_cap`, and
`begin_group` lives in `impl Db`.

**§7's "~30 call sites" is retired, with a measure attached.** Two independent recounts during
this design disagreed at 46 and 64 — the tell that a number was being quoted with no measure
behind it. The definition now recorded with the count: *one call expression in
`crates/charpente-core/src/table.rs` whose callee is an item defined by the `redb` crate —
inherent method, trait method, or associated function — counted per expression, comments
stripped, including `#[cfg(test)] mod latch_tests`.* That yields **170** (164 in the module
proper, 6 in tests), with a per-API breakdown recorded so a future recount that disagrees is
either a real change or a different definition, never an unresolvable difference of opinion.
The type-level surface is reported SEPARATELY (29 `redb::` paths plus one seven-name `use`)
because conflating the two is what made the earlier recounts diverge. The one-file containment
claim still holds exactly: `redb::` appears in `table.rs` alone.

Note what this does and does not change. §7's deferral rested on "deferral does not compound
the swap cost", and 170 is not 30 — but the containment argument was always the one-file
claim, which survives intact. The stale figure was evidence hygiene, not a load-bearing input.

**Two fixes beyond the six steps, both hard rule 6.** GUIDE §9's error index PARAPHRASED the
`across_tenants` macro error ("but this route does not declare cross_tenant") where the real
message reconstructs the attribute; the index quotes errors byte-for-byte by contract, so it is
now verbatim. And two source doc comments cited `DESIGN.md:1377` for a deferral that no longer
exists — line-number citations into a file being edited, already stale, and about to read
wrong. Both now name the bullet.

**Controller verification.** Every task on this branch was re-run rather than accepted from its
report. For Task 13: fmt 0, inventory gate 3/3 exit 0, clippy `--workspace --all-targets -D
warnings` exit 0. The first `cargo build -p guide-examples` returned in 0.05s — fully cached,
which proves nothing about the extractor — so it was **forced**: `cargo clean -p guide-examples`
(removed 4042 files, 314.8 MiB), then a real 20.05s compile, exit 0. The GUIDE's rust blocks
provably compile. Spot-checks that reproduced exactly: `open_table(` = 41 and `.value()` = 43
against the claimed buckets; `redb::` in one file; §8's old sentence gone; §12 marked
unratified.

**Plan complete: 13 of 13.** `f453a95` (8), `65821f0` (10), `2270c11` (11), `3b2457f` (the R56
fault sibling), `fef12f6` (12), `1673bfb` (13), on top of Tasks 1-7/6b/6c/9. **Merge to main is
Noah's call and has not been made.** Owed before it: the full workspace suite (only scoped
per-package runs were made), and the two decisions above — DECISIONS §12's ratification, and
whether the edge-harness wire layer should enter CI.

## 2026-09-05 — both owed decisions close, and the doc set is what closed them

Noah took the two decisions the tenant-scoping plan left owed. What is worth recording is not
the outcomes but the method: **both were answered by reading the existing doc set rather than
by fresh argument**, and in one case the doc set had already decided it and nobody had looked.
That is the second time this repo has produced an "open choice" the spec had settled (see the
north-star note in the fidelity audit), so the pattern is now two for two.

**DECISIONS §12 is ratified — the boundary stands, and the notch belongs to §11.** The line
holds as drafted: whoever holds the `Db` is the operator. The question that actually held the
entry open was the other one — should a future release put the raw reads behind a named,
`describe`-visible operator capability? Read against the rest of the doc set it collapses to
one survivor. *Sandboxing* the operator is refused by §5, which rejected the WASM plugin model,
so app code compiles into the same binary and there is no trust boundary between an app's
`main` and `charpente` to enforce; MANIFESTO §4 and §7 both assume one process in one trust
domain. *Discovering* operator reads is impossible by DESIGN §7's ruling 6 — enumeration cannot
walk call sites, the same reason `tx.emit` sites are undiscoverable, and a `Db::snapshot()` in
`main` sits in exactly that position. *Declaring* them is the only survivor, and a declaration
needs a registration site; `main` has one, `App::mount`, which is the louder notch §11 already
named as available and unbuilt. So the notch was not adopted as new work: it is §11's, and it
inherits §11's trigger — build it when declarations at the route prove too quiet in practice,
the concrete sign being an app or audit that had to read source to answer "what reaches every
tenant here". **A real tension was fixed in passing:** §11's binding clause forbids "an
undeclared bypass method, or an 'internal-only' accessor", which is very close to the shape
§12 permits, and the two entries did not speak to each other. Each now says the clause binds
the handler/job surface and that the operator path sits outside the guarantee rather than as an
exception inside it. DESIGN §8 and PLAN's M2 bullet drop "drafted and awaiting sign-off".

**The wire layer enters CI on a schedule, and the option to write it off was refused by the
docs.** The honest options recorded on 2026-09-04 were a CI job or an explicit statement that
this is a local gate. The second does not survive MANIFESTO §2 ("human review does not scale
… and pretending it does is how incidents happen"), CLAUDE.md hard rule 1's enforce-
mechanically-not-by-review, or MANIFESTO §9's wager. It is worse than a tone problem: PLAN
discharges M1's exit criteria as "an oracle at two layers", so writing the wire layer off as
local would have meant editing an exit criterion downward to stay honest. **The count was
re-measured rather than inherited** — 15 `#[ignore]`d tests, acked_durable 2, failover 3, hunts
5, realtime_resume 1, smoke 1, tenant_wire 3, counted directly; the older "31/31 scenarios"
figures in this ledger are a different unit and should not be read as a test count.
`.github/workflows/edge-harness.yml` runs `scripts/edge-harness.sh` — the script, not a copied
command line, so the runner cannot drift from a local sweep — nightly at 07:17 UTC plus
`workflow_dispatch`, with a 30-minute timeout against the 360-minute default because the rig
spawns real processes and can hang rather than fail. **Nightly and not per-push is a positive
choice, not a budget dodge:** MANIFESTO §6 sets the cadence for expensive fault work in its own
words, "thousands of seeded universes per night". PLAN's M1 exit criteria gain a cadence
sentence so the two-layer claim stops implying more than is true.

**Two things this does not yet prove, stated rather than left to inference.** The workflow has
never executed on a runner — every pass of this rig has been local, on macOS — so the first
scheduled run is the first evidence that process spawns, loopback binds and `kill -9` behave on
`ubuntu-latest`. And GitHub fires `schedule` only on the default branch, so while this sits on
`tenant-scoping` it runs never; the nightly begins the day it reaches `main`. Until then
`workflow_dispatch` is the only way to exercise it. If it reddens on a runner, diagnose the
runner before touching a test — a guard loosened for environment noise is a guard that never
fires, which `ci.yml:73` already says for the passthrough guard.

**Verification at this commit.** `cargo fmt --all --check` exit 0 and `cargo clippy --workspace
--all-targets -- -D warnings` exit 0, both at `14f5a94` before these edits; `cargo test
--workspace` was still running when this entry was written and its result is NOT recorded here.
The edits themselves touch no Rust: `doc_inventory` reads only `CLAUDE.md`, and nothing in the
workspace opens `DECISIONS.md`, `DESIGN.md` or `PLAN.md`, so the suite in flight is blind to
them. The new workflow YAML parses (`yaml.safe_load`, jobs `['edge-harness']`, triggers
`['schedule', 'workflow_dispatch']`). **Merge to main remains Noah's call**; of the three things
owed before it, the two decisions are now closed and the full workspace suite is outstanding.

## 2026-09-05 — the pre-merge sweep: the suite was red, and so was a tenant

Three reviewers over the two unmerged branches, then the full workspace suite that the plan
had recorded as owed. **Both of the things that actually block a merge were invisible to the
review that preceded them, and one was invisible to the reviewers too** — it took running the
suite. That is the entry's point: 42 commits reviewed task-by-task, twice, and the two real
defects surfaced only from execution.

**The full workspace suite was RED at `14f5a94`, and had never been run.** `TEST_EXIT=101`:
`genclient::tests::round_trip_preserves_every_shape` and
`a_contract_missing_a_field_is_refused_rather_than_defaulted`, both in `charpente-cli`.
Task 8-13 added `cross_tenant` to `RouteInfo` and `JobInfo` (`describe.rs:82`, `:157`), and
`EVERY_SHAPE` — the hand-written contract fixture at `genclient.rs:598` — was never updated.
`describe.rs` carries no `#[serde(default)]` anywhere, deliberately: the second test's own name
says a missing field is *refused rather than defaulted*, so the strictness is the contract and
the fixture was simply stale. The fix adds the field to both routes and the job, with
**different values on purpose** (`false` on the POST, `true` on the SSE route and the job) so
the round-trip proves each value survives, and so the `SSE + cross_tenant` serialization
combination has at least one pin — the reviewer's I4 having found that combination had no test
of any kind. **Why the branch's own gates missed it:** `EVERY_SHAPE` lives in a `src/` unit
test, not a `tests/` suite, and only per-package runs were made. `crates/charpente/tests/
describe.rs` and `examples/invoicing/tests/describe_golden.rs` — the fixtures people think of
as "the describe oracles" — were both updated correctly in the same commits.

**A reachable node-killing assert on a legal tenant, found by review and reproduced.**
`db.rs`'s `GroupOverlay` doc has said since Task 4 that on a scoped table "`0` is tenant 0, a
real tenant (R35), not an unset marker". Task 10's `merge_overlay_all_scopes` then asserted
that value could not occur. Tenant 0 is reachable off the wire — `Id::from_raw` takes any
`u64`, `FromParam for Id<T>` passes straight through, and no scope path has a zero-guard — so
`assert_ne!(scope, UNSCOPED)` fires on ordinary data; with no `catch_unwind` anywhere in
`crates/charpente/src` and assertions on in release, it unwinds out of the kernel loop.
`merge_overlay_all_scopes` also reads the GROUP overlay, which accumulates across a batch, so
**one request's tenant-0 write kills a different request's cross-tenant read committed
alongside it.** Invisible because every fixture on the branch starts its tenants at 1.
RED first, per hard rule 3: `tenant_zero_is_a_real_tenant_to_a_cross_tenant_read` is
deliberately the neighbouring test with one literal changed (tenant `2` -> tenant `0`), and it
panicked at `db.rs:1406` with `left: 0, right: 0` before any source was touched. The fix
deletes the assertion, because on a scoped table there is nothing left to assert: `0` is a
tenant, and the staging bug it reached for is already impossible by type — `Tx::put`/`delete`
take `R: UnscopedRow`, so a scoped row "cannot reach this method at all, which is a stronger
statement than any assertion here could make", in `Tx::put`'s own words at `db.rs:625`. The
mirror `assert_eq!` on the unscoped path stays: there the two values differ in meaning.

**A reviewer's headline finding was wrong, recorded so it is not re-raised.** The Tasks 1-7
review reported `NoTenant<R>` as a public unsealed trait, an app able to write
`impl NoTenant<Invoice> for Org {}` and turn the headline compile error off in one line, and
proposed a "3-line private-supertrait seal". That seal is already there: `table.rs:376` is
`pub trait NoTenant<R>: sealed::NotATenant {}` over a private `mod sealed` at `:384`, and
`UnscopedRow` is sealed the same way at `:407` — and the doc comment above both names that
exact one-line impl as the thing the seals exist to prevent. The reviewer did flag it as
needing a build to settle; it needed only reading four lines further down. Its other findings
held up when checked.

**Confirmed and still open, none merge-blocking.** `scope_flip_error`'s named refusal is wired
only into `scan_raw`/`scan_raw_from` (`table.rs:1808`, `:1871`), so the scoped reads and writes
an app hits first fall through to redb's unnamed type error — hard rule 5. The realtime settle
budget (`SETTLE_ATTEMPTS`/`SETTLE_BOOT_TRIES`) is red at 2 of 3 fresh bases, now the third
instance of the budget-constant-as-seed-property class. `#[route(SSE …, cross_tenant)]` has no
compile_pass, compile_fail or runtime test. DESIGN §3 states a compile-time property
`ctx.cap().unwrap()` defeats, and `Ctx::cap()` is public API documented in neither DESIGN nor
GUIDE. `README.md:127` listed "no tenant scoping as typed key prefixes" under what is *not*
built — fixed here, hard rule 6.

**The helpdesk experiment: keep the testimony, drop the app.** Reviewed against `14f5a94`:
zero API drift, the app would still compile. It is refused as an *example* for three Criticals
— an unauthenticated `POST /agents/:id` letting anyone join any org, an unscoped
`tx.get(id)?.is_some()` that works as a cross-tenant existence oracle, and five one-sided sim
properties (deleting the whole SLA sweep leaves the suite green) — plus it adds
`examples/helpdesk` to `members` without touching CLAUDE.md, reddening `doc_inventory` at its
own merge base. All three violate `BRIEF.md`'s own requirements, which is the useful part:
where FINDINGS says something was HARD that is signal; where it says something was DONE, check
the code. `docs/experiments/2026-09-helpdesk/` now holds the brief (rescued byte-identical from
an untracked file in a disposable `/private/tmp` worktree — it existed nowhere else), FINDINGS,
and a README recording which of its claims survive. **F4 is struck as factually wrong**: it
claims no GUIDE example derives `Serialize` on a route body, and `GUIDE.md:2039` is
`#[derive(Wire, Serialize, Deserialize)] struct HitBody`, sent with `Request::json` at `:2122`.

**The experiment's real payoff is a platform oracle defect, not an app.** `checks::
acked_implies_durable` takes a seed and builds its own `Sim::new(seed)` (`checks.rs:171-177`),
so the `restart` coverage `Sim::restart` credits (`lib.rs:667`) lands on a sim the harness never
reads — a `require: ["restart"]` over that checker **cannot fire, ever**. And `Sim::new` is
`FaultConfig::default()`, which zeroes `bit_flip`, `misdirected_write`, `dir_entry_loss`,
`read_error`, `conn_drop` and `net_spike`, so every property built on the public checker runs
outside the swarm matrix while reading as though it were inside it. Found because an outsider
used the documented checker library the documented way. Not fixed here; it is charpente's bug,
not the experiment's, and it wants its own change with its own failing seed.

## 2026-09-05 — tenant scoping merges, and the convergence property stops giving up quietly

**MERGED. `main` is `f05df9d`**, 45 commits, and **CI is green on its first exposure** — run
33973240362, both jobs (`check`, `ts-client`). Until this push, not one commit of the
tenant-scoping work had ever run on a runner. The merge needed a real `DECISIONS.md`
resolution: both sides had added different content at the same two places, and order was
load-bearing in both — §7's body says the call-site measure is "recorded at the end of this
entry" and main's §10 addition says "this is the closing paragraph", so main's blocks precede
the branch's. Both sides kept in full. `DESIGN.md` and `README.md` auto-merged with the
branch's edits intact. The `ts-client` job was known-safe in advance rather than hoped-for:
`git log main..tenant-scoping -- ts-fixture crates/ts-fixture-app` is EMPTY, so the job ran
against content byte-identical to what was already green on a Linux runner — worth recording
because `ci.yml:175-180` names case-sensitivity as the one hazard it cannot check locally, and
the honest answer here was "the surface did not move", not "we re-audited and it looked fine".

**THE CONVERGENCE PROPERTY GAVE UP ON 38% OF ITS UNIVERSES, SILENTLY.** Measured, not
estimated: at base `0x2EB1_0001` over 256 universes, `repl:inconclusive` **97**,
`repl:logical-converged` **159**. `finish_universe` (`replication.rs:252`) maps any
`INCONCLUSIVE` error to `sim.cover("repl:inconclusive"); Ok(())` — a covered skip, correct in
principle, since a disk too faulty to reopen is not a bug. The defect is that
**`repl:inconclusive` was required by nothing, floored by nothing, and the runner enforces no
ceiling**, so the give-up rate could climb to 90% with the suite green. It is the fifth
instance of the shape Task 12 named three of, and the first that was already quantified in
this ledger (41%/43%, 2026-07-25) without anyone reading the number as a defect.

**Two changes, both measured.** (1) `recover_node` power-cycled the host before EVERY one of
its 500 attempts. `settle_boot`'s doc comment (`realtime.rs:706-712`) had already measured
that exact pattern next door — seed `0x536007ad99fd90bf` at `write_error` 0.144, **0 of 20,000
boots** succeeded cycling before each try, versus success after ~16 cycles retrying in place,
because `Store::apply` commits at `Durability::None` so every restart vanishes uncheckpointed
state and forces another rebuild-from-WAL at the same per-write odds. `recover_node` never got
that treatment; it now cycles on attempt 0 (the caller needs the net torn so the listener
frees) and every 8th after, the same `SETTLE_CYCLE_EVERY` the sibling uses. **Result: 97 -> 89
inconclusive, 159 -> 167 converged, `restart` 4584 -> 4120, runtime 221s -> 193s.** Provably
not inert (the restart count moved by 464), reproduced identically on a second run, and the
full 59-test replication suite stays green.

**The honest part: that is a 3.1-point improvement, not the sharp drop the hypothesis
predicted.** The power-cycle pattern is a real contributor and NOT the main driver; 89
universes still give up and the cause of most of them is unestablished. (2) So the fix that
matters is the floor: `repl:logical-converged` is now pinned at `CONVERGED_FLOOR = 167`, the
`FINGERPRINT_FLOOR` move applied to the key one line over — pin the POSITIVE outcome, so
silence has to survive a number. Proven fireable rather than assumed: at 168 the property goes
RED naming `"repl:logical-converged: 167 of 168 required"`, at 167 it is green, both at 193s.

**Named next step, with the evidence for why.** The 89 is a MIXTURE and nothing distinguishes
its parts: five sites in `convergence_universe`'s reach produce `INCONCLUSIVE` — `spawn_node`
exhaustion (`replication.rs:241`), a torn witness file (`:304`), a torn role file (`:327`), a
store bricked below a compacted log (`:381`), and `recover_node` exhaustion (`:394`) — and
THREE of those are honestly unrecoverable states while two are budgets. All five cover the
same key, which is why "is 38% bad?" was unanswerable and why H2 had to be tested by
experiment instead of read off a report. Split the key by reason. Also unfixed and now
counted: the same run shows **five** properties in `replication.rs` skipping universes
(inconclusive counts 90, 39, 45, 109, 89 over their own universe sets) — only the 89 is
attributed with confidence, because `--nocapture` interleaves output and only that one was
re-run in isolation. Four `INCONCLUSIVE -> Ok(())` wrappers exist across `replication.rs`,
`realtime.rs`, `auth.rs` and `charpente-core/tests/migrations.rs`; none of their keys is
required or floored. The realtime settle budget (`SETTLE_ATTEMPTS`/`SETTLE_BOOT_TRIES`) is
untouched here and still fails loudly at 2 of 3 fresh bases — a false RED, not a false green,
and a different fix (denominate the budget in no-progress, not in reconnections).

**The wire layer ran on a runner for the first time, and corrected a number written earlier
today.** Run 33974865000, dispatched manually against `main` rather than left for the nightly
to discover: **green, 3m28s**, against a 30-minute timeout — an order of magnitude of headroom,
with acked_durable the only slow binary at 147s. So real process spawns, loopback binds and
`kill -9` do behave on `ubuntu-latest`, which no prior run of this rig had established.

**It ran 34 ignored tests, not 15.** The entry above, the workflow comment and PLAN.md all
said 15, and 15 is right for what it counted — the wire scenarios in
`crates/edge-harness/tests/` — reproduced exactly by the runner (acked_durable 2, failover 3,
hunts 5, realtime_resume 1, smoke 1, tenant_wire 3). What none of them counted is that
`cargo test -p edge-harness -- --ignored` also picks up **19 ignored real-disk/real-process
unit tests under the crate's `src/`**, equally skipped by `cargo test --workspace` and now
equally covered. The three sites are corrected in place; the correction is recorded here
rather than by rewriting the entry above, which was accurate about the thing it measured.
Worth noting how it surfaced: the runner printed per-binary counts, they summed to 34 against
an expected 15, and the gap was chased rather than waved through — a green run whose test
count nobody checks is the same defect as an unfloored coverage key, which is the subject of
this very entry.

## 2026-09-05 — the 89 are not a mixture: every one is the budget case

**CORRECTION to the entry above.** It said the 89 inconclusive universes were a MIXTURE —
"three of those are honestly unrecoverable states and two are budgets" — and used that to
argue the split would apportion blame. Splitting the key took one run to prove that wrong:

```
repl:inconclusive:                   89
repl:inconclusive:recover-exhausted: 89
```

**All 89 are `recover_node` exhausting its 500 attempts.** Zero `spawn-exhausted`, zero
`role-file-torn`, zero `store-bricked`, zero torn-witness. The honestly-unrecoverable reasons
— the ones that would have made a high inconclusive rate defensible — **never fire in this
property at all.** So the give-up rate is 100% budget-or-liveness, and the reading that made
35% look acceptable was wrong. Recorded as its own entry rather than by editing the one above:
the claim was made, it was wrong, and both belong in the record.

This is also the case for splitting keys generally. The previous entry proposed the split as
future work and reached its (wrong) mixture claim by reasoning about which producers *exist*.
The split answered it mechanically in a single run, which is the difference between an
inventory and a guess — the same argument DECISIONS §11 makes for `describe`.

**What it opens.** 89 universes in which 500 recovery attempts all fail — now WITH periodic
rather than per-attempt power-cycling — is not obviously a budget. The next question is
whether `RECOVER_ATTEMPTS` is simply too small or whether those universes are in a state no
number of attempts escapes, and the decisive experiment is cheap: raise the constant by 10x
and see whether the 89 moves at all. If it does not, the budget is innocent and the retry path
has a liveness bug. **Not run here** — it is a measurement, not a fix, and it wants its own
change. Four sites now carry reason keys (`replication.rs:241`, `:381`, `:394`, and the role
arm at `:327`); the pre-existing torn-witness arm keeps its own older key name
(`repl:witness-file-inconclusive`) because that arm is deliberately byte-identical across four
copies in three files and renaming it is a separate edit.

## 2026-09-05 — `acked_implies_durable` takes the caller's `Sim`, and the GUIDE taught the bug

The defect the docs-only helpdesk experiment surfaced, now fixed. `checks::
acked_implies_durable` took a `seed: u64` and built its own `Sim::new(seed)`. Three
consequences, all of them silent:

 1. **Coverage went nowhere.** `Sim::restart` credits `restart` on the sim it is called on
    (`lib.rs:667`) and the harness reports the keys of the sim it handed the workload, so
    `require: ["restart"]` over this checker could never fire.
 2. **The excuse helpers answered from the wrong world.** `durable_corruption_fired` and
    `dir_entry_loss_fired` read `sim.coverage()` and gate whether a data-loss shortfall may be
    EXCUSED. Asked of the caller's sim after this checker ran, they report on faults that
    universe never saw. This one was not in the original finding and is the worst of the three.
 3. **The fault matrix was replaced.** `Sim::new` is `FaultConfig::default()`, which zeroes
    eleven axes — `bit_flip`, `misdirected_write`, `dir_entry_loss`, `read_error`, `conn_drop`,
    `net_spike`, `subscriber_stall` and the four `object_*` — so a swarm universe's declared
    matrix was silently discarded while the property read as though it were inside it.

**The GUIDE taught it.** `docs/GUIDE.md`'s worked example ended
`acked_implies_durable(sim.seed(), 5, drive(), verify)` — pulling the seed off the harness's
sim and handing it to a checker that drove a different one. An outside agent building from the
docs alone hit exactly this and discarded a working property over it.

**Fixed by taking `&mut Sim`.** The alternatives are impossible rather than merely worse:
`Sim` exposes no public `FaultConfig` accessor, so a caller cannot supply one; and
`Sim::with_faults` derives all three RNG streams at construction with no rewind, so no
seed-shaped API can recover a caller's fault SCHEDULE. `replays_identically` KEEPS its seed
and is not a sibling bug — it needs two worlds identical at time zero, which a used sim cannot
give. The rule now stated in both the code and GUIDE §the-checker-library: **a checker that
needs a FRESH world takes a seed; one that continues THIS world takes `&mut Sim`.**

**The repo had already reasoned this out once and not generalized it.** `sim-fixture/tests/
sim.rs`'s `fixture_replays` carries a doc comment explaining that it deliberately has no
`require` because `replays_identically`'s restarts happen in worlds the outer sim cannot see.
That is this exact mechanism, understood correctly, sitting four lines from a checker with the
opposite requirement that nobody re-examined.

**RED first, and the end-to-end oracle is the real proof.** Two unit tests in `checks.rs` were
written against the OLD signature and failed on assertions rather than on compilation — a test
that merely fails to compile against old code proves nothing: `left: 0, right: 3` for the
coverage count, and `unwrap_err()` on an `Ok` value for a `read_error: 1.0` axis that never
fired. Then `sim-fixture` gains `acked_counter_survives_rounds require ["restart"]`, a real
app workload that never calls `sim.restart()` itself, so the require IS the assertion.
**Mutation-proved:** restoring the private `Sim::new` inside the checker makes it fail by name
— "sim coverage requirement failed in acked_counter_survives_rounds: \"restart\" never
occurred over 256 universes" — and removing the mutation returns it to green over 256
universes. That is the claim proved through the harness, not asserted in a unit test.

Verification: fmt 0, clippy `--workspace --all-targets -D warnings` 0, charpente-sim 143
tests green, sim-fixture 256 universes green, `guide-examples` builds (the GUIDE's compiled
```rust block is the backstop and was edited in the same commit, hard rule 6).

## 2026-09-06 — two corrections to yesterday's entries, and the machine could not finish a suite

**CORRECTION 1 — "no `catch_unwind` anywhere in `crates/charpente/src`" is FALSE, and I
repeated it into the record.** The tenant-0 entry and commit `9e5cd9b` both argue the severity
that way, citing `archive/mod.rs:842`'s doc comment. `kernel/dispatch.rs:212` is a
`catch_unwind`. **The finding and the fix stand**: it wraps job-handler glue (`def.glue`), so a
panicking JOB is absorbed into `JobError::App` as a failed attempt, while an HTTP route handler
has no such wrapper — and the route path (`/admin/notes` in the fixture, plus the cross-request
group-overlay case) is the one the repro demonstrates. What is wrong is the blanket sentence,
and how it was obtained: a doc comment was quoted instead of a grep being run. **Two source
comments carry the false claim and should be fixed** — `crates/charpente/src/archive/mod.rs:842`
and `crates/charpente/tests/tenant.rs:1027`, the latter written by me yesterday. That
`dispatch.rs` sibling deserves its own note: a panicking job handler also prints a raw trace
before absorption, and unlike F1 that trace should probably STAY — there the panic is the app
author's own bug and the trace is the useful artifact.

**CORRECTION 2 — `CONVERGED_FLOOR` pins one property, not five.** Yesterday's entry presents
the floor as closing the `repl:inconclusive` hole. `finish_universe` (`replication.rs:252`) is
shared by FIVE properties — `follower_converges_to_the_primary_fingerprint` (:880),
`late_follower_seeds_below_the_gcd_base_and_converges` (:1142),
`switchover_mid_write_load_loses_nothing_and_serves_reads_throughout` (:3700),
`failover_preserves_everything_the_follower_acked` (:3979), and
`wal_corruption_on_the_primary_no_longer_loses_acked_records_with_a_follower` (:5187) — and
`CONVERGED_FLOOR` constrains only the first. Four consumers of the same wrapper remain
unpinned, and `repl:logical-converged` is not even reachable in two of them. A sixth wrapper
exists that a marker grep cannot find: `repl:failover-inconclusive` has four
`cover(); return Ok(())` sites (`:4001, :4005, :4019, :4034`), three carrying no `INCONCLUSIVE`
string, on a property with **no `require:` at all**.

**The number that should worry us most is still unattributed.** This ledger records
`repl:inconclusive` at 90, 39, 45, 109, 89 across those five properties in one run. Their
universe sets are 256/256/256/128/128 — so **if the 109 lands on a 128-universe property, that
is an 85% give-up rate**, and the two 128-universe properties are the two with the weakest
instrumentation. Attribution needs `--exact --test-threads=1 --nocapture` one property per
invocation: the coverage block is tagged with the TEST FILE, never the test function, so two
properties in one file are indistinguishable even when run serially. That measurement is the
precondition for any further floor, and it has not been made.

**Also carried forward, unfixed:** `recover_auth_node` (`auth.rs:272`) and `recover_rt_node`
(`realtime.rs:1645`) still power-cycle before EVERY attempt — the pattern `settle_boot` measured
at 0 of 20,000 boots — over a budget of 200 against replication's 500. Porting
`RECOVER_CYCLE_EVERY` to both must land BEFORE their floors are measured, or the floors get
taken against a known-bad retry shape.

**No workspace suite was completed on 2026-09-06, and the reason is the machine.** Two
out-of-memory kills (a ~1 GB `Virtualization.framework` VM resident, ~62 MB free at the second
kill), and separately `syspolicyd` at **1044 minutes of CPU** stalling binary validation — one
test binary sat 11 minutes at `_dyld_start` with a 96 KB footprint and 0:00.00 CPU, having
never executed an instruction. Killed by pid to unblock the run. What DID pass at `c6e1dcc`
before the second kill: `charpente-core` 10 suites, `charpente-macros` 3, `charpente-sim` 6
(the package carrying the checker change), `charpente-tsgen` 3 of 4 — its `types.rs` is the one
that stalled in dyld and is unverified, not failed. `fmt` 0 and
`clippy --workspace --all-targets -D warnings` 0 were both banked at this HEAD earlier.
**`charpente`, `charpente-cli`, `sim-fixture`, `ts-fixture-app`, `guide-examples`,
`edge-harness`, `bench-harness` and `invoicing` are unverified at this commit** — recorded as
owed rather than assumed, since the individually-affected packages were run green at the time
each change landed.

## 2026-09-07 — an acked write does not survive a restart under directory-entry loss

**The most important thing to come out of the oracle-density sub-project is not its
deliverable — it is what the deliverable found.** Recorded here first, before any fix, because
until this commit it existed only in a gitignored `progress.md` on one laptop, on a branch with
no upstream.

```
cargo test -p sim-fixture --test sim -- --seed 0xb957ceaaf1716fd5 --filter acked_counter_survives_rounds
```

```
sim failure in acked_counter_survives_rounds: acked-implies-durable violated after round 2
under seed 0xb957ceaaf1716fd5: acked round 2 lost across a restart
  config: FaultConfig { write_error: 0.068, sync_error: 0.049, crash_apply: 0.354,
    crash_torn: 0.507756, dir_entry_loss: 0.477, bit_flip: 0.0, misdirected_write: 0.0,
    read_error: 0.0, corruption_scope: "wal/", conn_drop: 0.02, net_spike: 0.081, … }
sim: 1 universe(s) per test, 0 verdict(s), 0 gave up, 1 failure(s)
```

One command, one universe, exit 1. Reproduced independently at `85c2fc7` by a second party
after the finding was reported, from the printed repro alone. Rate measured by the finder at
**1 real failure in 33 full batches (~3%)**; the batch that found it had master seed
`0x0482ca1ad2b14fa2`. That rate is the finder's measurement and is not re-verified here.

**The fault profile is the lead, and it is unusually clean.** `dir_entry_loss` 0.477 and
`crash_torn` 0.508 with `bit_flip`, `misdirected_write` and `read_error` all at **0.0** —
high directory-entry loss and *no media corruption whatsoever*. Nothing has to be mangled for
an acknowledged write to disappear; a directory entry merely has to go missing across a crash.

**CORRECTION, same day, and it invalidates how the paragraph above was obtained.** Those are
CONFIGURED PROBABILITIES, not events. At the time that paragraph was written no artifact
anywhere recorded whether `dir_entry_loss` ever fired in this universe, because the failure
path discarded coverage entirely: `run_universe`'s error payload was `(FaultConfig, String)`,
with nowhere to put it. A GREEN run of the same workload reports coverage fine
(`fault:dir-entry: 3` over 20 universes); the failing single-universe run reported
`"coverage":{}`, and single-universe GREEN controls confirmed the emptiness came from the
FAILURE, not from the universe count. So three separate readers — the finder, a reviewing
agent, and the controller — each named `dir_entry_loss: 0.477` as "the lead" from a line that
says only that the axis was switched on. The repo had already learned this exact lesson once
and not generalized it: `run_universes_swarm_with_in`'s `CHARPENTE_SEED` branch prints
coverage with the comment "the batch path reports coverage; a single-seed replay was throwing
it away, which is what you most want when asking why *this* universe behaved (or cost) the way
it did" — the same reasoning, never applied to the failure path.

**Now measured.** The failure report carries a `fired:` line, and seed `0xb957ceaaf1716fd5`
answers:

```
  fired:  crash:dropped: 1, crash:torn: 7, fault:dir-entry: 1, fault:sync_error: 4,
          fault:write_error: 4, restart: 2
```

`fault:dir-entry` **did** fire — once. The guess was right; the reasoning behind it was not
evidence, and would have read identically had the axis never fired at all. Note also
`fault:sync_error: 4`: fsync failures occurred in this universe, and per the fsyncgate
convention a failed fsync poisons the log rather than permitting an ack. Both axes are now on
the table for the a-vs-b question, where before only one was, on worse grounds.

**Why it was invisible until now, and why that is the thesis working.** The 2026-09-05 entry
above records that `checks::acked_implies_durable` used to build its own `Sim::new(seed)`, and
that `Sim::new` is `FaultConfig::default()`, which zeroes eleven axes — `dir_entry_loss`
**among them**. So this checker had never once been shown directory-entry loss. It was shown
it nine days ago, and inside a fortnight it produced a reproducible violation of the guarantee
the README sells first. A fix to an *oracle* surfaced a defect in the *system*: that is the
oracle-density argument in MANIFESTO §2 paying out, and it needed someone to actually run the
thing enough times to catch a 3% event.

**Three readings. None is chosen here, and the third has not previously been named.**

 1. **A real durability defect.** charpente loses an acked write when directory entries are
    lost. The specific suspect, given a profile with zero media corruption, is the classic
    missing parent-directory fsync: create a WAL segment, write it, fsync the *file*, ack, and
    never fsync the *directory* — the crash then takes the directory entry and the segment goes
    with it, acked records inside. This is a hypothesis from the fault profile, not a code
    reading; nobody has yet opened the segment-creation path.
 2. **The checker's restart bookkeeping is wrong in the three-round shape**, and the assertion
    is firing on its own accounting rather than on lost data.
 3. **The fault is unsurvivable by construction.** If the sim drops a directory entry even for
    a file whose parent directory *was* correctly fsynced, then no implementation can pass this
    property and the red carries no information about durability at all. That is the mirror of
    the unfireable-test trap this repo already knows: a test that cannot fail proves nothing,
    and a fault that cannot be survived proves nothing either.

**The decisive experiment is small and reads two files, not a debugging session:** what does
the sim's `dir_entry_loss` injection actually drop, and does it spare a file whose parent
directory was synced; and does the WAL's segment-creation path fsync the parent directory
before the first ack. Those two answers separate all three readings. Reading 2 is ranked last
precisely because the profile is so narrow.

**Hard rule 3 was violated on the way here, and it is the second time this defect was nearly
buried.** Task 7's implementer *saw this failure*, dismissed it as "pre-existing, tracked, out
of scope" by citing the 2026-09-06 entry above, and **recorded no seed**. That entry records
the machine failing to finish suites; it says nothing about a durability assertion firing. The
misattribution is the exact failure mode rule 3 exists to prevent — "if you fix a bug without a
failing seed first, you haven't reproduced it" cuts equally against dismissing one without a
seed. Combined with the `FaultConfig::default()` masking, this bug has now survived two
separate opportunities to be seen.

**A false positive worth recording, because it has now caught two agents.** The first hunt for
this failure reported one at run 4 that was not real: it grepped for `panicked` and matched the
benign redb `page_manager.rs` trace that F1 describes, in a *green* universe. F1 already
records that trace destroying a competent agent's work; here it caught the controller running
the hunt. The second hunt keyed on the process exit code and `test result: FAILED`, and that is
what found the real one. **Grepping logs for panic text is not a failure detector in this repo.**

**One doc correction, narrower than first stated.** The sim's own output is correct — it printed
`repro: charpente test --sim --filter … --seed …`, which is a working one-command replay, so
hard rule 5 is satisfied by the tool. What is wrong is CLAUDE.md's blanket claim that
"every sim failure prints this exact repro line" for `CHARPENTE_SEED=0x… cargo test -p <crate>
--test <file>`: `sim-fixture`'s suite is `harness = false` with its own flags
(`--universes/--seed/--master-seed/--filter/--jobs/--format`), so `CHARPENTE_SEED` does not
reach it. Following the doc instead of the printed line cost one wasted run.

**Reading 3 is RULED OUT, and so is reading 1 in its naive form.** The sim models the barrier
faithfully: `collapse_dir_entries` (`charpente-sim/src/disk.rs`) touches only names still in
`pending_creates`/`pending_removes`, and `Disk::sync_dir` retains those out by prefix, so a
correctly barriered name is immune and the fault IS survivable. And every name-creation site on
this path barriers itself, each with a comment citing DESIGN §2: `log.rs`'s `rotate` (the new
segment's name, before frames land), `Wal::open_with_horizon`'s first segment, `gc` after each
removal, `Store::open` ("the store file's name must be durable too"), and the horizon
breadcrumb write. **There is no missing parent-directory fsync.** The suspect named above —
the classic textbook one — is not what is happening here.

**Status: OPEN at `85c2fc7`, narrowed but unfixed.** What remains is reading 2 (the checker's
three-round restart bookkeeping) or a subtler form of reading 1 in the interaction between the
single `fault:dir-entry` event, the four `fault:sync_error` events, and the checkpoint-then-gc
ordering in `Db::checkpoint`. Tasks 8-12 of the oracle-density plan stay held.

One further asymmetry, noted and NOT yet acted on: `checks.rs` exposes two excuse helpers,
`durable_corruption_fired` and `dir_entry_loss_fired`. `sim-fixture`'s property excuses only
the first, and `dir_entry_loss_fired` has exactly ONE consumer in the whole workspace
(`charpente/tests/replication.rs`, guarding a "refusing to guess" error). Whether losing an
acked write under directory-entry loss is excusable at all IS the open question — so that
asymmetry must be resolved BY the answer, never used to reach it. Excusing it to get green
would convert this finding into a decorative oracle, which is exactly what the floor-lowering
warning in `run_universes_swarm_with_in` exists to forbid.

## 2026-09-07 — a sim failure now says what actually fired, not just what was enabled

The instrument the entry above needed, built because that entry could not be written honestly
without it.

**The defect was structural, not cosmetic.** `run_universe`'s signature was
`Result<UniverseEnd, (FaultConfig, String)>`. `UniverseEnd` carries `coverage`; the `Err` arm
had nowhere to put it, so on every failing universe — in both the sequential and parallel
runners, and in the `run_universes_swarm!` panic path the platform's own suites use — the
coverage map was dropped on the floor. A green run reported it; a red run did not. The one
place a reader most needs to know which fault explains the behaviour was the one place the
harness refused to say.

**What it cost.** Everything in the entry above that reasons from `dir_entry_loss: 0.477` was
reasoning from a probability. Three readers in sequence took a configured axis for an observed
event, and no artifact existed that could have contradicted them. The measurement, once
possible, took one command.

**The fix.** Both `run_universe` and `run_universe_with_adjust` carry the coverage map in their
`Err` payload; `harness::Failure` gains a `coverage` field; the human block and the swarm panic
report gain a `fired:` line; the JSON failure record gains its own `coverage` object. Per-
failure, deliberately — `Report::coverage` stays the batch statistic that `require:` and floors
are measured against, untouched, because the question a failure raises is "which axis explains
THIS universe" and an aggregate cannot answer it.

**`config_json` was a second, quieter instance of the same defect.** It listed four
probabilities and two latency ranges, so the machine-readable record omitted `dir_entry_loss`,
`bit_flip`, `misdirected_write`, `read_error`, all four `object_*`, `corruption_scope`,
`conn_drop`, `net_spike` and `subscriber_stall` — the human format has always printed every
field via `{cfg:?}`. A tool consuming `--format json` could not see the axis under discussion.
Now every axis is emitted, and `config_json_carries_every_fault_axis` derives its expectations
from the Debug rendering rather than a hand-written list, so adding a `FaultConfig` field
without adding it here reddens a test instead of silently reopening the hole.

**The GUIDE taught the misreading and is corrected in the same commit** (hard rule 6). Its
"Reading a failure" section said of `config`: "Reading it tells you what kind of trouble was
injected." It tells you what was *possible*. That sentence now distinguishes enabled-vs-fired
explicitly, the block is five lines rather than four, and `fired:` is documented as the line to
reason from — including that `fired: nothing` is a real answer meaning the failure is a plain
bug rather than fault-induced.

**Verification.** RED-first was unavailable (the field did not exist, and a test that merely
fails to compile proves nothing — the 2026-09-05 rule), so both halves are **mutation-proved**
instead. Restoring the discard (`Err((cfg, BTreeMap::new(), msg))`) fails
`a_failing_universe_reports_the_coverage_it_marked` and
`json_failure_record_carries_the_coverage_that_fired` by name; dropping `dir_entry_loss` from
`config_json` fails `config_json_carries_every_fault_axis`, which names the missing field in
its message. Both restored green. `fmt` 0, `clippy --workspace --all-targets -D warnings` 0,
`cargo build --workspace --all-targets` 0, charpente-sim 117 + 4 + 1 + 1 + 31 green,
charpente-cli 7 suites green (`repro_meta` pins the JSON shape and still passes),
`guide-examples` builds. The assertion is through the public `Report`, not the renderer, so
narrowing the payload again fails regardless of how the text is formatted.

**The repo had reasoned this out once already.** `run_universes_swarm_with_in`'s
`CHARPENTE_SEED` branch prints per-universe coverage under the comment "the batch path reports
coverage; a single-seed replay was throwing it away, which is what you most want when asking
why *this* universe behaved (or cost) the way it did." Correct, and never generalized to the
failure path — the same shape as 2026-09-05's `fixture_replays` note, where the right
reasoning sat four lines from the code that needed it.

## 2026-09-07 — the acked-write bug is real: a failed dir barrier is never re-established

Reading (a), confirmed and fixed. Not the textbook missing-fsync — that barrier was always
there — but a subtler thing the earlier entries could not see.

**The mechanism.** `collapse_dir_entries` was instrumented to name the file it dropped, and
under seed `0xb957ceaaf1716fd5` it named `wal/00000000000000000000.log`, VANISH-CREATE — the
first WAL segment, lost as a still-pending create. Combined with `fault:sync_error: 4` from the
new `fired:` line, the sequence is:

 1. Round 1: `Wal::open_with_horizon` finds an empty directory, creates segment 0, and calls
    `disk.sync_dir(&prefix)?`. The injected `sync_error` FAILS that barrier. The error
    propagates, `Db::open` fails, the round acks nothing. **But the file now exists, and its
    directory entry is still pending** — a failed barrier guarantees nothing (fsyncgate).
 2. Round 2: `Db::open` again. `disk.list(&prefix)` is no longer empty, so the create branch is
    skipped and recovery runs. Every frame is valid, so there is nothing to cut or remove —
    and the clean path (`None => tail = Some((file, len))`) fell out of the loop and returned
    **without any directory barrier**. The log then accepted an append and ACKNOWLEDGED it.
 3. `sim.restart()`: the still-pending create is collapsed, the segment vanishes whole, and the
    acked round-2 record goes with it.

The module's own comment on the cut-path barrier reads "recovery leaves a durably-clean
directory". True on the cut path; **false on the clean path**, which is the common one.

**The fix** is one `disk.sync_dir(&prefix)?` before `open_with_horizon`'s success return, so
every successful open leaves the directory durable regardless of which recovery path it took.
The cut arm's barrier is kept — it must run before the truncation is treated as durable, a
stronger ordering than this one.

**Evidence.** A deterministic RED-first test pins the mechanism without seed-hunting
(`charpente-sim/tests/wal.rs::reopening_a_log_re_barriers_a_name_left_pending_by_a_failed_dir_sync`):
it builds the pending-name state directly, appends, syncs — the acknowledgement — crashes, and
asserts the segment survives. RED before the fix (`left: 0, right: 1`), green after. The
original seed `0xb957ceaaf1716fd5` now reaches a VERDICT rather than a give-up. A sweep of
**22,048 sim-fixture universes** (8 batches of 256 including master seed `0x0482ca1ad2b14fa2`,
the batch that originally found it, plus 4 batches of 5,000) produced ZERO failures, where the
original ~1-per-8,400 rate predicts ~2.6. Verdict rate is unchanged at 87%, so the extra
barrier did not push universes into give-ups.

**Two archive floors were re-measured, and the cause was PROVEN rather than assumed.**
`CONV_RESTORED_FLOOR` 219→218 and `CONV_SCOPED_SUBSTANTIAL_FLOOR` 164→159: one more fallible IO
op on every open means a handful of universes stop earlier and never reach the oracle. A
control run — reverting only the `log.rs` barrier — makes both properties pass at the same
bases, which is what identifies the cause instead of assuming it. `CONV_CHAINED_FLOOR` now
measures 172 against a floor of 169 and was left alone: a floor is a minimum, and raising one
that is not failing is a different decision from repairing one that is.

## 2026-09-07 — two realtime properties are pinned to a green base and fail at ~40% of others

Found while control-testing the entry above, and it is the more serious of the two findings.

The WAL fix reddened `charpente --test realtime`'s
`reconnecting_client_misses_nothing_under_the_fault_matrix` and
`a_reconnecting_tenant_subscriber_misses_nothing_and_leaks_nothing`. The obvious reading — the
fix broke realtime — is WRONG, and the mechanism that makes it wrong is worth stating: in the
sim, `Disk::sync_dir` consumes RNG draws. Adding one call to every `Wal::open` therefore
reshuffles the whole downstream fault schedule of every universe. That is equivalent to
changing the base seed, not to changing behaviour.

**Tested directly, with the fix removed from the tree entirely**, by varying only the pinned
base constants:

| property | base | result |
|---|---|---|
| `reconnecting_client_misses_nothing_under_the_fault_matrix` (`realtime.rs:927`) | `0x2EB2_0001` (pinned) | PASS |
| | `0x2EB2_0011` | PASS |
| | `0x2EB2_0021` | **FAIL** |
| | `0x2EB2_0031` | **FAIL** |
| | `0x2EB2_0041` | PASS |
| `a_reconnecting_tenant_subscriber…` (`SCOPED_REALTIME_BASE`, `realtime.rs:2628`) | `0x2EB2_5C09` (pinned) | PASS |
| | `0x2EB2_5C19` / `0x2EB2_5C29` | PASS |
| | `0x2EB2_5C39` / `0x2EB2_5C49` / `0x2EB2_5C59` | **FAIL** |

**Five of eleven alternative bases fail, with no change to the product code at all.** Both
properties are latently broken and both are pinned to a base that happens to be green. The
suite has been reporting them as passing.

The two failures are distinct and neither is a floor shortfall — they are the properties' own
oracles firing:

- `content mismatch at event id 0: expected (ping, {"n":"1638627"}) but received
  (ping, {"n":"1441716"})`, with `fired: crash:dropped: 18, fault:misdirected_write: 1,
  fault:sync_error: 5, restart: 2`
- `client never caught up: ledger has 5 entries scanned through id 5, floor 0, 2 frames still
  unverified, … cursor=Some(Cursor { id: 7, seq: 11 }) need_subscribe=true`, with
  `fired: crash:applied: 221, fault:crash: 2, fault:misdirected_write: 1, fault:sync_error: 806,
  realtime:resume-backlog: 2, restart: 101`

Both `fired:` lines come from the instrument built earlier today; neither would have been
available this morning.

**Why this matters beyond two tests.** A pinned base is the right instrument for a FLOOR — a
rate needs a fixed denominator, and `archive.rs`'s floor docs argue that case well. It is the
wrong instrument for a CORRECTNESS oracle, where a fixed base means the property only ever
examines the same 128 universes and a green run says nothing about the other 2^64. These two
properties assert correctness, not a rate, and they are pinned. **The whole suite should be
audited for correctness oracles running on pinned bases** — that is the general form of this
bug, and it is the same family as the unfireable-test trap: green is true, and the inference
from green is false.

**Status: OPEN, and the branch is RED at this commit.** The WAL fix lands anyway, because
holding it would not make realtime work — realtime is already broken at 5 of 11 bases, and the
pinned green base is what has been hiding it. Making that visible is the point of the
instrument. Both realtime failures need their own investigation and their own seeds; neither
is diagnosed here.

## 2026-09-07 — the barrier moves BEFORE replay, because it was on the causal path

**CORRECTION to the entry two above, and to my own reasoning about it.** That entry said the
new open-path barrier "widened a pre-existing hazard" rather than causing anything. Then the
base-seed audit below turned up `auth`'s `raw_credentials_never_touch_the_durable_store`
failing on a CORE invariant — `db.rs`: "store applied_seq 4 is ahead of the durable log end
Some(3) — the store outran durable truth, which append→sync→apply forbids" — at 1 of 20 fresh
bases, against 0 of 8 without the barrier.

**Statistics were the wrong instrument and nearly cost an hour.** One event in twenty against
zero in eight cannot attribute anything, and the reflex was to gather more samples. The
decisive experiment was a MECHANISM test taking one run: print when the new barrier returns
`Err`, then run the failing base. It fired five times immediately before the assertion. The
barrier was on the causal path, not merely correlated with it.

**Why.** `Engine::recover` (engine.rs) passes a closure to `Wal::open_with_horizon` that calls
`machine.apply(seq, payload)` for every recovered record — so **replay mutates the store from
inside the WAL open, before that open is known to succeed.** A barrier placed AFTER the replay
loop therefore returns `Err` having already advanced the store, and the next `Db::open` meets a
store whose `applied_seq` is ahead of the log. Pre-existing failure points on the cut path had
the same shape; putting one on the CLEAN path made it common.

**The fix is placement, and it is strictly better.** `disk.sync_dir(&prefix)?` now runs
immediately after `disk.list(&prefix)` and BEFORE the replay loop: the names about to be
treated as a real log are made durable first, and a failed barrier returns before the store is
touched. It closes the original durability hole exactly as well — the pinned mechanism test in
`charpente-sim/tests/wal.rs` still passes, seed `0xb957ceaaf1716fd5` still reaches a verdict,
20,000 fresh sim-fixture universes are clean — and `auth` goes to **0 of 12** fresh bases.

**The archive floors moved a second time**, and that is the more transferable lesson. A
`sync_dir` consumes RNG draws, so changing WHERE an IO op sits inside `Wal::open` reshuffles
every universe at a pinned base. Pinned-base floors are coupled to the IO-op ORDERING of code
they do not name. Final: `CONV_RESTORED_FLOOR` 219→217, `CONV_SUBSTANTIAL_FLOOR` 183→181,
`CONV_SCOPED_RESTORED_FLOOR` 214→213, `CONV_SCOPED_SUBSTANTIAL_FLOOR` 164→160,
`CONV_SCOPED_CHAINED_FLOOR` 158→156. `CONV_CHAINED_FLOOR` stays 169 (measures 174) — raising a
floor that is not failing is a different decision from repairing one that is. Recorded in
archive.rs so the next person meeting those panics knows the cause is usually mechanical.

## 2026-09-07 — pinned-base audit: the pathology is narrow, and it found two more properties

`CHARPENTE_BASE_SEED=0x…` re-bases every pinned property in a test binary, skipping floors and
`require:` keys under the override for the reason the floor's own panic text gives — they are
rates measured AT a base. What it tests is the property's own assertions. Documented in
GUIDE.md. **A correctness oracle should pass at EVERY base**; one that passes at its pinned
base and fails at others is not flaky, it is failing.

26 pinned sites across 8 files; 18 carry no floor. Swept at fresh bases:

| suite | pinned sites | result |
|---|---|---|
| `wal` | 8 | clean, 5 bases |
| `migrations` | 2 | clean, 5 bases |
| `group_commit` | 4 | clean, 5 bases |
| `tenant` | 1 | clean, 5 bases |
| `auth` | 1 | clean, 12 bases (after the placement fix above) |
| `archive` | 2 | floors only; re-measured |
| `realtime` | 3 | **`reconnecting_client…` 4/5 FAIL, `a_reconnecting_tenant_subscriber…` 3/5 FAIL** |
| `replication` | 5 | **`late_follower_seeds_below_the_gcd_base_and_converges` 2/4 FAIL** |

**The pathology is NARROW, and that is worth stating as plainly as the failures.** Five of
seven swept suites are robust at fresh bases. This is not a suite built on sand; it is two
realtime properties and one replication property, against 23 pinned sites that hold up.

**The replication failure is a different KIND, and less severe.** Its message is
`f: table store (redb): I/O error: injected read error`, with `fired: fault:read_error: 1`.
That is an INJECTED FAULT reported as a correctness violation — a false RED, where realtime's
is a false GREEN. Cause: `replication.rs:656` and its siblings wrap store errors with
`.map_err(|e| format!("f: {e}"))?` and bubble them straight out, never consulting
`checks::is_fault_fallout`. The classifier would have caught it — `classify` matches on
`contains("injected")` — it is simply never asked. The property's `no_media_corruption` adjust
zeroes `bit_flip` and `misdirected_write` but leaves `read_error` hot, so the fault is
reachable and unhandled. This wants an `is_fault_fallout` check and a `give_up`, not a fix to
the system.

**Still OPEN: the two realtime properties**, which are genuine correctness failures and are
next. Neither is diagnosed here.

## 2026-09-07 — realtime, root cause 1 of 3: "never caught up" when there was never a node

The first of the two red realtime properties, fixed. Systematic debugging rather than
guessing, and the decisive step was instrumentation, not inspection.

**Reproduced first, deterministically.** Re-basing found seed `0x22eac3ba9fa5bef7`; it
replays byte-identically. Counting the settle phase's attempt outcomes gave the answer in one
run: `boot_none=12, sub_fault=0, run_fault=0, clean=0`. **All 12 of `SETTLE_ATTEMPTS` ended
with `settle_boot` returning `None`.** The node never booted once, so the client never had a
single fault-free chance to receive event 0.

**The workload's own comment already named the distinction it then failed to make.** The
settle loop is documented: "'the client failed to catch up' and 'there was no node to catch up
FROM' are different facts and only the first is this property's subject." `settle_boot` exists
precisely to separate them. But when every boot failed, control fell through to
`unsettled_report()` — whose text asserts the platform lost an event. The property reported the
fact its author had explicitly ruled out of scope.

The neighbouring comment block argues the budget cannot mask a miss: "retries add chances to
RECEIVE, nothing else… an id the platform ever skips is one the cursor has already moved past."
That argument is sound **only for a client that made progress**. Here `cursor=None` and the
cursor never moved, so it does not apply — and the conclusion it licensed was wrong.

**Fix.** Count attempts that ran all the way through `kernel.run` without fault fallout. Zero
of them means the universe never observed a delivery and cannot testify about a missed one, so
it gives up rather than failing. Sound because `settle_boot` returns `Ok(None)` ONLY after
every boot failure classified as fault fallout — a non-injected failure returns `Err`
immediately — and both `continue` arms above it are likewise fault-only. Conservative by
construction: one clean attempt and still unsettled remains a hard failure, which is the
property's actual subject.

**RED first, genuinely.** The universe is reconstructed in-test from the seed
(`splitmix64` → `FaultConfig::swarm` → `sample_extended_fields`, the swarm runner's own
derivation), so the regression is a pinned seed in the suite per hard rule 3 rather than a
comment. It failed with the exact reported message before the fix and passes after.

Applied to `scoped_realtime_workload` too — same loop, same reasoning. **NOT applied to
`multi_node_realtime_universe`'s two sites**: they carry no `SETTLE_ATTEMPTS` loop and no
`settle_boot`, so the argument above does not transfer and assuming it would is how a give-up
becomes a mask. They are unexamined, not cleared.

**Result.** `reconnecting_client_misses_nothing_under_the_fault_matrix` now passes at its
pinned base and at 5 of 6 swept bases (was 1 of 6).

**Two root causes remain, both distinct from this one and neither diagnosed:**

 1. **`clean_attempts=1` and still unsettled** — seed `0x1890c2db82a370b2`, reached at base
    `0xA11CE002`. `cursor=None need_subscribe=false` with `fault:bit_flip: 1` and
    `fault:read_error: 69` fired. A clean attempt DID occur, so the give-up above correctly
    does not cover it. Whether durable corruption excuses it (the `durable_corruption_fired`
    doctrine) or the platform genuinely skipped is exactly the open question.
 2. **`content mismatch at event id 0`** — the scoped property at its pinned base
    `0x2EB2_5C09`: expected `(ping, {"n":"1638627"})`, received `(ping, {"n":"1441716"})`.
    Different content at the same id, which is not a delivery-budget shape at all. This is the
    one remaining red in `-p charpente`.

## 2026-09-07 — realtime root cause 2: one `(id, seq)` with two different payloads

Characterized, NOT fixed, and recorded now because it is the more serious of the two
remaining reds and the evidence is cheap to lose.

**Repro.** `CHARPENTE_SEED=0x17004e6a572d737d cargo test -p charpente --test realtime --
--nocapture`, deterministic, the scoped property at its own pinned base `0x2EB2_5C09`:

```
content mismatch at event id 0: expected (ping, {"n":"1638627"}) but received (ping, {"n":"1441716"})
  fired:  crash:dropped: 18, fault:misdirected_write: 1, fault:sync_error: 5, restart: 2
```

**An ordered trace settles what happened, and refutes the obvious reading.** Instrumenting the
client's ledger readback and its frame ingestion with a shared tick:

```
DIAG[0] LEDGER id=0 seq=1 json={"n":"1638627"}     <- read back from the STORE
DIAG[1] LEDGER id=1 seq=2 json={"n":"2215267"}
DIAG[2] FRAME  id=0 seq=1 json={"n":"1441716"}     <- DELIVERED afterwards
```

**The seqs are identical.** My working hypothesis had been id reuse across a crash-diverged
timeline, which DESIGN §the-cursor explicitly anticipates: "Two diverged timelines … can mint
the same event id for two different events; a bare id cannot detect that", defended by
"resume validation reads the retained row at `id == c` and requires its `seq == s`". That
would have made this a TEST defect — an oracle keying on a bare id, which the design says is
an invalid model. **It is not that.** The seq half is present and equal on both sides. One
`(id=0, seq=1)` maps to two different payloads, so the design's own disambiguator does not
separate them, and the oracle is asking a fair question.

**Why this is serious.** `checks::durable_corruption_fired`'s doctrine states the contract
directly: an excuse may never cover wrong DATA, because "corruption must surface as loss or a
named error, not as different bytes". Different bytes at the same `(id, seq)` is precisely the
outcome that sentence says cannot happen. `fault:misdirected_write` fired exactly once in this
universe, and a misdirected write is the one axis that can relocate a CHECKSUM-VALID frame to
a position where it does not belong — the failure mode CRCs do not catch, because nothing is
mangled.

**Two readings, NOT distinguished, and I am not guessing between them:**

 1. The stored row for id 0 was corrupted, so the ledger readback is the wrong value and the
    delivered frame was right.
 2. The delivery path read the wrong payload, so the frame is wrong and the store is right.

The discriminator is which side the misdirected write landed on, and it wants the same
treatment that found root cause 1: instrument the injection to name the file and offset it
relocated, then compare against where event 0's frame and the store's row actually live. That
is one run's work and it has not been done.

**Do not "fix" this by excusing it.** The tempting move — routing this shortfall through
`durable_corruption_fired`, which WOULD fire here — is the exact use its own doc forbids, and
would convert the finding into a decorative oracle. Root cause 3 (`clean_attempts=1` and still
unsettled, seed `0x1890c2db82a370b2`) remains untouched and is a separate question again.

## 2026-09-07 — root cause 2, diagnosed: a cut re-mints the same `(id, seq)`

The discriminator the entry above asked for, run. Instrumenting the injection to name what it
relocated:

```
DIAG misdirected_write path=wal/00000000000000000000.log intended_offset=0 landed_at=1024 bytes=154
```

**The first WAL frame — the record that mints event id 0 — was written to offset 1024 instead
of 0.** Offset 0 keeps whatever was there before, so recovery finds no valid frame at the
start, cuts there, and the log is empty. The store's event 0 goes with it. A new record is then
appended at the same position and mints **event id 0 again, at the same seq 1**, with new
content drawn from fresh entropy.

That explains every number in the trace: the ledger read `(id 0, seq 1) = {"n":"1638627"}` from
the pre-crash store, and the post-recovery node delivered `(id 0, seq 1) = {"n":"1441716"}`.
Neither side is corrupt. Both are internally consistent. They belong to two different
timelines that occupy the same coordinates.

**So neither of the two readings in the entry above is right, and I should record that.** I
framed it as "corrupted store row vs corrupted delivery read" and it is neither: the
misdirected write never corrupted a record at all, it *removed* one by landing it where nothing
reads, and the damage is the re-mint that follows. The `durable_corruption_fired` doctrine
("corruption must surface as loss or a named error, not as different bytes") is NOT violated —
corruption did surface as loss. What produced different bytes is the re-mint, one layer up.

**Finding A (test defect, and the one that makes this red).** `Client::sync_ledger` treats a
`db.snapshot()` readback as ground truth for what must be delivered. A snapshot sees COMMITTED
state, which is not DURABLE state — the platform promises durability for acked writes, and this
event's record was never durable. The oracle is asserting delivery consistency against state
the platform is entitled to roll back. Its own doc comment says a ledger entry is "never a
guess about what was *sent*, only a readback of what was *stored*" — correct, and the gap is
that "stored" was taken to mean "durable".

**Finding B (a design question, NOT a bug I am asserting).** DESIGN §the-cursor says the seq
half of the cursor exists so that "two diverged timelines … can mint the same event id for two
different events; a bare id cannot detect that", defended by requiring the retained row's
`seq == s` at resume. **This universe produces two different events at the same `(id, seq)`,**
because the cut returned the log to the same position and the replacement record took the same
seq. A client resuming at cursor `(0, 1)` would pass that validation and silently misattribute.
Whether that is in scope (the passage's stated case is failover, where replication's
diverged-`Hello` refusal is the primary defense) or a real gap in the single-node cut path is a
DECISIONS-level question, and it wants Noah rather than a unilateral answer. Recorded with the
seed so it can be reopened with evidence.

**Not fixed here, deliberately.** Finding A's fix changes what the oracle asserts, and doing
that while Finding B is unresolved risks weakening the property to match the bug. Root cause 3
(`clean_attempts=1` still unsettled, seed `0x1890c2db82a370b2`) remains untouched.

## 2026-09-07 — root cause 2 fixed, and a correction to how I escalated it

**CORRECTION to the two entries above.** Both ended by calling Finding B "a DECISIONS-level
question" that "wants Noah rather than a unilateral answer". That was wrong, and it is the
mistake this repo's own north-star rule exists to prevent: the spec had already decided it.
DECISIONS §6's trigger read names **Design C (a durable timeline epoch on `Hello`)** as the
ledgered sharpening step, and the 2026-08-06 verifying-peer close-out records it as "stays
ledgered and stays compatible". Escalating a decided item as an open one costs the reader the
context that already exists. What the evidence actually contributes is SCOPE — Design C is
written in replication terms and this instance has no replication — so the backlog item is
reopened with that note rather than a new question being asked.

**And the second correction: the fresh-base fragility was already documented.** The scoped
property's own doc comment records that the flagship "fails 'client never caught up' at fresh
bases `0x9F3A_0001` and `0x41D7_0001`, passes at `0xC58E_0001`", attributes it to the shared
settle budget, and declines to raise it ("Raising the budget is not this task's to do"). The
audit reported this as a discovery. It was known, measured, and correctly declined — what was
missing was the classification, not the measurement. The right answer was never "raise the
budget": a budget too small for a config is a universe that **adjudicated nothing**, which is a
give-up. Both bases now pass.

**The fix for root cause 2.** `Client::sync_ledger` scans forward and never revisits an id, so
a ledger entry is a snapshot of COMMITTED state — which is not DURABLE state. DESIGN makes
delivery commit-triggered by design ("a commit simply advances `next_event_id`, which makes
tails ready"), and MANIFESTO §5 makes the log the only place things become true, so a record
that never landed durably never became true. The platform owes nothing for it — this is not
even DECISIONS §6's loss window, which covers records the primary **acked**. The oracle was
asserting delivery consistency against state the platform is entitled to discard.

`diverged_ledger_entry` re-reads the store at the ids the ledger recorded and reports the first
whose content changed underneath it. On a drain error only — so a passing universe pays no
extra disk ops and its fault schedule is untouched. A divergence is a give-up with the reason
named; anything else is still the hard failure it was.

**One thing the first attempt got wrong, worth keeping.** The probe fired correctly and the
universe still failed, because the workload kept running afterwards and later returned `Err` —
and `Sim::give_up`'s own doc says "a workload that gives up and then returns `Err` is still a
failure — failures outrank give-ups." Both workloads now return at the top of their epoch and
settle loops once a give-up is recorded. A give-up that does not stop the universe is not a
give-up.

**Scoped floors re-measured** (`tenant:both-tenants-emitted` 77→75,
`tenant:foreign-backlog-not-delivered` 85→80; the two unmoved floors left alone), counts
reproduced identically across two consecutive batches first. Cause is the same mechanical one
as the archive floors: the probe's error-path disk ops shift the universes that hit it, and one
universe now gives up instead of running its epochs to completion — a universe that adjudicated
nothing, whose coverage was never evidence for these rates.

**Result: `-p charpente --test realtime` is GREEN, 17 of 17**, and the suite is clean at 7 of 8
fresh bases including both bases the doc comment recorded as failing. Base `0xA11CE002` still
fails both properties — that is root cause 3 (`clean_attempts=1` and still unsettled, seed
`0x1890c2db82a370b2`), untouched and still undiagnosed.

## 2026-09-07 — CORRECTION: `cargo test` stops at the first failing target

**The verification claim in commit `36b214a` — "Everything else in `-p charpente` passes" — was
not supported by the run it cited, and it was wrong.** `cargo test` stops after the first
failing test binary unless `--no-fail-fast`. Every full-package run that day short-circuited at
`realtime`, so `replication`, `tenant`, `transactions`, `typed_tables`, `wire` and the rest
**never executed**. Their absence from the failure list was read as a pass. It was not evidence
of anything.

The cost: two replication properties broke at `36b214a` and stayed hidden for three commits,
surfacing only once `realtime` went green and cargo finally reached them.

**This is the third instance of one shape in a single day, and the shape is worth naming.**
Coverage was empty on failure and read as "no faults fired"; a `git stash push` on an
already-clean path was a no-op whose "stashed" echo read as a control being applied; and a
suite that never ran read as a suite that passed. In all three the misleading signal was not a
wrong answer — it was an ABSENT MEASUREMENT wearing the shape of a passing one. This ledger
already names the family ("a green run whose test count nobody checks is the same defect as an
unfloored coverage key"); these are three more members of it.

**CLAUDE.md's documented full check inherits the same blind spot.** `cargo test --workspace`
stops at the first failing PACKAGE for the same reason. Left as a note rather than a silent
edit, because the fix is a one-word change to a documented command and belongs with Noah's eye
on it: `--no-fail-fast` on both the workspace and per-package forms, and a habit of reading a
POSITIVE count (`grep -c "^test result: ok"`) rather than the absence of failure lines. Note
the second half matters independently — `... | grep -E "FAILED"` exiting 1 because it matched
nothing is itself an absent measurement, and under `set -o pipefail` it even reports as a
failing pipeline, which is the same trap one level up.

## 2026-09-07 — replication: one false red fixed, one floor re-measured

Both surfaced by the WAL open-path barrier re-rolling `replication.rs`'s pinned bases, and
**attribution is a control, not an inference**: checking out the pre-barrier `log.rs` makes the
whole suite green (59 passed), restoring it reproduces both failures exactly.

**`late_follower_seeds_below_the_gcd_base_and_converges` — FIXED, and it was a false RED.**
Message: `f: table store (redb): I/O error: injected read error`. An INJECTED FAULT reported as
a tenancy violation. `follower_kept_its_tenants` tags its reads `f:` and propagates them with
`?`, never consulting `is_fault_fallout` — while its sibling `converged_read` has always been
classified, by `assert_converged`'s retry loop (`Err(e) if is_fault_fallout(&e) => continue`).
This oracle sits OUTSIDE that loop and never received the same treatment, despite taking `sim`
precisely so it could give up. It now classifies every read and gives up by name. This is the
defect the pinned-base audit predicted from a grep and could not then reach; the barrier move
reached it.

**`follower_converges_to_the_primary_fingerprint` — floor re-measured 167 → 166.**
`CONVERGED_FLOOR`'s own doc comment predicted this exactly: it is deliberately sharp, "a false
red on any change that merely re-rolls the fault stream", and it named the trigger — "the
decision point is the first stream-shifting change that actually makes it flap … the choice is
between re-measuring it here and giving it the same 2-SE headroom, with the new number in hand
rather than in advance." That change has now happened and the number is in hand: 166 of 256.

**Only the re-measure half is taken. The headroom half is Noah's** and is left explicitly open:
at n=256, p=0.648, SE=7.64, so the siblings' 2-SE convention would put this floor near **151**.
The case FOR headroom is today's evidence — a single correctness fix forced three separate
pinned-base floor re-measures (`archive.rs` twice, `realtime.rs`, and here), so stream-shifting
changes are frequent enough that the sharp convention costs real time. The case AGAINST is that
every flap so far was correctly diagnosed rather than blindly lowered, and 2-SE headroom would
tolerate a ~9% relative fall in reach before firing. Left sharp until ruled: a floor is lowered
with a reason, and "it flapped once, as designed" is a reason to record the flap.

## 2026-09-07 — realtime root cause 3: a ledger id the store rolled back PAST

The third and last of the realtime reds. Same family as root cause 2 — a stale ledger entry
from a discarded timeline — but the event is **absent** rather than **changed**, which is
exactly the case RC2's probe did not cover. I had seen `stored=ABSENT` in that probe's own
diagnostic output and did not act on it; it came back as this.

**Reproduced, then instrumented, and the instrumentation is what found it.** Seed
`0x1890c2db82a370b2` at base `0xA11CE002`, deterministic, both properties. Per-attempt tracing
of the settle loop:

```
a0: subscribe faulted        a1: boot=None
a2: CLEAN run -> cursor=None verified=0 pending=0   (delivered nothing)
a3..a11: boot=None           (nine consecutive — the node never boots again)
```

One clean attempt in twelve, and it delivered nothing. The tell was in the numbers already
printed: `floor=0` means `next_event_id` was **0** when the client subscribed, while the ledger
had already scanned event 0 (`scan=1`). Both cannot be true of one timeline. A direct probe
settled it: `store event0=Some(None)` — the snapshot succeeds and the row is simply not there.

**Root cause.** The client's ledger recorded event 0 from a node whose store later rolled back
past it. The event was committed but never durable, the platform is entitled to discard it
(MANIFESTO §5; DESIGN's delivery is commit-triggered), and no reconnect can ever deliver an id
the store no longer has. `first_unaccounted` then names id 0 forever and the settle budget
expires against a claim that cannot be satisfied.

**The discriminator is `next_event_id`, and it is precise.** That counter only advances on
commit — retention GC deletes rows without ever rewinding it — so:

- recorded id **at or above** the store's current `next_event_id` ⇒ the store rolled back past
  it ⇒ never durable ⇒ give up, it can never be delivered;
- recorded id **below** `next_event_id` but absent ⇒ the retention case, deliberately NOT
  excused: the row existed in this timeline, so a client subscribed below it should have
  received it, and forgiving that would hide the exact miss this property exists to catch.

That asymmetry is the whole safety of the fix. Excusing absence unconditionally would have been
the decorative-oracle move.

**Plumbing note.** RC2's probe runs on the drain-error path, and RC3 never errors in `drain` —
the client simply never receives the frame, and the verdict comes from the final
`!client.settled()` check, which has no node of its own to ask. The observation is therefore
taken during a clean attempt that left the client behind (a settled client breaks out at the
top of the next iteration and never reaches the probe, so no universe that catches up pays for
it) and carried to the verdict. No floors moved, unlike every other change today.

**Verification.** Seed `0x1890c2db82a370b2` gives up naming the id and the rolled-back counter.
The realtime suite is **17/17 at its pinned bases and clean at 12 of 12 fresh bases** —
`0xA11CE001`-`008`, plus `0x9F3A_0001`, `0x41D7_0001`, `0xC58E_0001` (the three the property's
own doc comment recorded verdicts for) and `0xBEEF_0001`. Before today it was 1 of 6.

**All three realtime root causes are now closed**, and the shape of all three is worth stating
together: none was a platform defect. A settle budget too small was reported as a missed event;
a snapshot readback was treated as durable truth; and a rolled-back id was treated as an
undelivered one. Every one was an oracle claiming more than the platform promises. The single
genuine platform bug found this session remains the acked-write durability hole in
`Wal::open_with_horizon`.

## 2026-09-07 — CORRECTION: neither "open decision" was Noah's, and the docs said so

I closed the realtime work by handing Noah two decisions: whether `CONVERGED_FLOOR` should
keep its sharp pin or take the siblings' 2-SE headroom, and how CLAUDE.md's check command
should be worded. He asked one question back — *"Based on our northstar documents, are the
decisions mine?"* — and the answer, checked rather than asserted, is **no, to both.**

**The rule I invoked does not apply.** CLAUDE.md's trigger for sign-off is specific: *"When a
decision contradicts DECISIONS.md, don't silently override it — reopen the entry with the new
evidence and get Noah's sign-off."* DECISIONS.md carries twelve entries; none covers coverage
floors or check commands. Neither item met the trigger. This is the second time in one day I
have promoted a question to Noah that the documents had already answered (the first was Design
C's scope, which DECISIONS §6 had named), and the shape is identical: reaching for "this is the
owner's call" at precisely the moment the work gets harder to finish.

**The floor question was posed on the wrong axis.** Sharp-versus-2-SE offers two ways to
*tolerate* a perturbation. DESIGN.md already rejected tolerating it. When declaring
`object_delete_error` on the shared object-store stream would have "re-roll[ed] every pinned
universe merely by declaring the field, and invalidat[ed] the convergence floors and both
recorded seeds with it," the repair was to key the axis onto its own stream and **prove** the
counts unchanged — twelve coverage counts across four bases reproducing exactly. The doctrine
is: remove the perturbation, do not loosen the oracle that noticed it.

Today's perturbation has the same shape. Every disk fault rolls off one shared per-host stream,
`keyed_stream(seed, "host-faults:{host}")` (`lib.rs:389`), and `sync_dir` consumes two draws
from it — `disk_op`, then `chance(sync_error)` (`lib.rs:1044-1047`). Moving one barrier inside
`Wal::open` therefore re-rolled all 256 universes, which is why a single correctness fix forced
pinned-base re-measures in three separate files. So the floor stays sharp at 166 on doctrine
rather than on a pending ruling, and the constant's doc comment has been rewritten to say that
instead of "Noah's to settle."

**The standing fix, with its cost stated honestly.** Key the directory barrier onto its own
stream, as the object store's delete arm is. This is *not* free the way that one was:
`object_delete_error` was non-perturbing because it defaulted to zero, whereas `sync_dir` fires
at nonzero rates in every existing run, so re-keying re-rolls every pinned universe **once** —
one final re-measure of every convergence floor, plus the two seeds pinned at `ci.yml:36-37` —
and then ends the recurrence permanently. Worth paying; flagged before starting rather than
after, because the blast radius reaches recorded seeds.

**The check command turned out to be worse than drift.** I went to CI expecting to find
CLAUDE.md lagging behind it. `ci.yml` runs bare `cargo test --workspace` as well, so CLAUDE.md's
"same three commands as CI" claim was *accurate* — the fail-fast blind spot that hid two
`replication` failures for three commits is live **in CI itself**, not merely in the
instructions describing it. Nothing in the north-star documents makes that a matter of taste:
hard rule 6 makes a wrong doc a bug, and MANIFESTO §2 makes a check that silently skips suites
an oracle defect of exactly the kind this whole session was spent removing. Both files now pass
`--no-fail-fast`, and both carry the reason plus the counting discipline (`grep -c "^test
result: ok"`, never the absence of `FAILED` — a `grep` matching nothing exits 1, so under
`set -o pipefail` "nothing failed" and "the check broke" are the same status).

**The generalisation worth keeping.** Every defect this session shared one shape: an absent
measurement wearing the shape of a passing one — dropped coverage on the failure path, a no-op
`git stash` control, five suites that never ran, a `grep` exiting 1 on no matches. An escalation
is the same failure in the process rather than the instrument: a question handed upward reads
like diligence and produces no verdict at all. Check the documents before deciding something is
undecided.

## 2026-09-07 — the barrier gets its own fault stream, and every universe re-rolls once

The standing fix from the entry above, taken. `SimDisk::sync_dir` now rolls `sync_error` off
`keyed_stream(seed, "host-dir-faults:{host}")` — and a global `dir_faults` stream for the
un-hosted disk — instead of the shared `faults` stream that reads, writes, file syncs and
misdirected writes draw from. DESIGN.md carries the doctrine paragraph next to the object
store's delete-arm split, which is the same repair for the same hazard.

**Written RED first, and the RED names the mechanism.** The new oracle,
`a_directory_barrier_does_not_perturb_the_other_disk_faults`, runs a fixed workload twice and
inserts one *semantically no-op* barrier into the second run — a prefix with nothing pending,
under `sync_error = 0.0` so `fault:sync_error` never marks and the two runs can be compared as
whole coverage maps rather than a filtered subset. `Rng::chance` draws before it compares, so
the barrier still consumes its draw at p = 0 and the perturbation is still exercised. Before
the split: `{read_error: 6, write_error: 11}` against `{read_error: 10, write_error: 9}` — one
no-op barrier moved four read faults and two write faults. After: identical. The test also
carries an anti-vacuity assertion, because two empty coverage maps compare equal.

**Two facts checked before building it, both of which could have made it the wrong fix.**
`disk_op` consumes no RNG at all, so `sync_dir`'s only draw was the `sync_error` roll — one
draw, and keying removes exactly it. And `disk_op` *does* tick `crash_countdown`, which counts
disk ops rather than RNG draws and which keying therefore cannot protect; that would have left
the perturbation half-fixed. It turns out `crash_countdown` is reachable only through the aimed
`crash_disk_after`/`crash_after` APIs, which nothing outside `lib.rs` calls, so it is `None` in
every swarm universe and adding a disk op cannot shift crash timing there. Keying is sufficient.
Had that gone the other way the honest answer would have been that the fix does not work.

**The one-time cost, itemised.** As predicted, introducing the split re-rolled all pinned
universes once: 6 failures across 4 targets out of 77. Five were floors moving by 2–3 counts
with reach intact (`archive:chained` 169→166, `archive:restored` scoped 213→211,
`archive:chained` scoped 156→153, `tenant:own-backlog-verified` 79→78,
`realtime:resume-backlog` 128→125, `repl:logical-converged` 166→164) plus the CLI fixture's
give-up split (36/28 → 32/32, still cross-checked two independent ways). All re-measured, none
widened.

**The sixth was not a floor, and it is the finding worth keeping.** The pinned regression seed
`0x22eac3ba9fa5bef7` stopped reproducing its scenario — **a pinned seed names a universe, and
re-keying a fault stream renames every universe at once.** The test did not go quietly green;
it went red, which is the good failure mode, but a differently-shaped test could easily have
gone vacuous instead. Re-found by scanning the same swarm construction for a universe ending
with zero clean settle attempts: `0xbb93400e0bc80ca4`, base `0xA11CE002`, universe 6 — four
candidates in 768 scanned. The assertion was strengthened while re-pinning: it now requires the
give-up REASON to contain "there was no node to catch up from" rather than merely requiring
*some* give-up, so a future re-roll landing on an unrelated give-up cannot keep it green while
testing nothing. Mutation-proved live (substitute an impossible substring ⇒ RED, restore ⇒
GREEN), and the RED printed the real reason, confirming the new seed reproduces the identical
12-attempt shape.

**One observation, deliberately not claimed as a finding.** A redb panic
(`page_manager.rs:257`, `left: 135168, right: 137216`) surfaced inside
`follower_converges_to_the_primary_fingerprint`. It is caught by the existing fence — the test
continued past it and failed on its floor — and it is the known upstream redb issue DESIGN §7
already records. Re-rolling changes which universes reach it. Recorded as an observation
because I have no pre-split control run proving it was reachable before, and "my change
surfaced it" and "my change caused it" are different claims.

**What this does and does not buy.** Barrier movement can no longer re-roll a pinned universe,
which was the recurring cost: one durability fix forced re-measures in three files in a day.
Other workload and IO-shape changes still re-roll, so a future floor panic is still usually
mechanical and upstream rather than a decayed oracle — just never again from a `sync_dir`.

## 2026-09-07 — the open acked-durability finding is CLOSED: it was the barrier bug

The oracle-density plan's Task 7 left an entry headed *"OPEN FINDING, HIGHEST PRIORITY, NOT YET
EXPLAINED"*: `sim-fixture`'s `acked_counter_survives_rounds` failing with "acked round 2 lost
across a restart" at seed `0xb957ceaaf1716fd5`, ~1 batch in 33. It named two readings and
deliberately chose neither — (a) charpente genuinely loses an acked write under directory-entry
loss, a defect in the guarantee the README sells first; (b) the checker's own restart
bookkeeping is wrong in the three-round shape. **It is (a), and it is already fixed.**

**The record was validated before anything was concluded from it.** At HEAD the pinned seed now
passes — which proves nothing, because two different stories predict that: the bug was fixed, or
today's stream split simply renamed every universe. So the seed was run in an isolated worktree
at `85c2fc7`, the commit it was recorded against, with its own `CARGO_TARGET_DIR`: it fails
there, exit 1. The finding was real.

**Instrumenting that deterministic universe named the mechanism outright.** Printing the disk's
files at each restart and every dropped directory entry:

```
[RESTART] files before: ["tables.redb"]                                   <- after round 1
[RESTART] files before: ["tables.redb", "wal/00000000000000000000.log"]   <- after round 2
[DROP-CREATE] wal/00000000000000000000.log
acked round 2 lost across a restart
```

Round 2's `Db::open` created the WAL segment, the commit was **acked**, and at the restart the
segment's directory entry was still pending — never made durable — so `dir_entry_loss` removed
the file and the acknowledged record with it. The recorded fault profile fits exactly:
`dir_entry_loss` 0.477 with `bit_flip`, `misdirected_write` and `read_error` all `0.0`, so
`durable_corruption_fired` could not excuse the absence, and nothing should have.

That is the defect fixed by `2d19535` / `36b214a`, whose title is literally "every successful
open barriers the directory, not just the cut path". **The two findings were the same bug,
reached from opposite ends** — one from the auth suite via a mechanism test, one from the
flagship acked-implies-durable property.

**A/B at the recording commit, 256 universes, master seed `0xC0FFEE`:** with the fixed `log.rs`
grafted on, 0 acked-lost failures and 256 verdicts, exit 0; with it reverted and everything else
identical, 1 acked-lost failure, exit 1. Honest caveat: pre-split the barrier itself consumed a
draw, so the two arms are not byte-identical universes — this is a same-master-seed comparison,
not a paired control. It is the instrumented mechanism above that carries the attribution; the
A/B corroborates it.

**The class was swept, not just the instance** — the recurring lesson that a fix confirmed on
its known instance leaves siblings alive. Every segment-creation site in `log.rs` audited
against every barrier: `:154` (segment 0 on fresh open) barriered at `:155`; `:196` (replay
loop) barriered at `:183` by the fix; `:367` opens only names already returned by `list`, on a
read path, and creates nothing; `:457` (`rotate`'s new segment) barriered at `:460` **before**
any append into it. No unbarriered creation remains.

**On the seed, under hard rule 3.** `0xb957ceaaf1716fd5` is not re-pinnable: it named a universe
at `85c2fc7`, and both the barrier fix and the stream split have since renamed every universe.
It is recorded here with the commit it reproduces at, which is the only form that stays true.
The durable regression pin is the deterministic mechanism test
`reopening_a_log_re_barriers_a_name_left_pending_by_a_failed_dir_sync`, which was RED before the
fix and needs no seed at all — a strictly better pin than the seed would have been.

**One correction to the earlier record.** Task 7's implementer dismissed this failure as
"pre-existing, tracked, out of scope" citing a ledger entry about OOM-killed runs, and recorded
no seed. The controller called that a misattribution at the time and was right to; the entry
recorded the machine failing to finish suites, not an assertion failing. Had the dismissal
stood, a real acked-write durability bug would have been closed as noise.
