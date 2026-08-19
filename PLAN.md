# Charpente — Plan

Bias: end-to-end working system as fast as possible, then refine. Every milestone ends in
something that runs and demonstrates a guarantee no incumbent stack offers.

## M0 — The demo that sells the thesis [shipped 2026-07-07]

**Goal:** an agent builds a small SaaS on charpente in one session, and `--sim` catches an
injected durability bug that would silently eat data on a conventional stack.

Scope, in dependency order:

1. **Sim-capable IO layer.** `Clock`/`Entropy`/`Disk`/`Net` traits; production impls
   (tokio/std); simulator impl with seeded PRNG, virtual time, IO reordering, and a fault
   matrix (torn write, failed sync, crash-restart between any two operations, partition).
   *This is first because it is the one thing that cannot be retrofitted.*
2. **Log + deterministic core.** Single-threaded apply loop; WAL append/sync/replay;
   crash-recovery invariant tests running under the simulator from the first week.
3. **Typed tables on redb** behind the `Disk` boundary: `#[table]`, `Id<T>`, get/put/scan,
   one secondary-index form, MVCC snapshots for reads.
4. **Transactions.** `fn(&mut Tx) -> Result<T, E>`; enqueue + emit recorded in the same
   log record; serializable by construction.
5. **HTTP layer.** `#[route]` macro over a hyper-based server; typed params/body/response;
   sessions (minimal auth).
6. **Queue + cron.** Transactional enqueue, post-commit dispatch, idempotency keys,
   typed retry policy; cron ticks as transactions.
7. **`charpente test --sim`** CLI: seeded parallel universes, fault schedules, failure =
   seed + minimal repro command.
8. **The demo app** (invoicing or todo-SaaS) written *by an agent against the docs*, plus
   the recorded sim run that catches an injected torn-write bug. The demo doubles as the
   launch post.

**Exit criteria:** demo app passes 10k seeded sim universes with fault injection; kill -9
at any point during commit never loses an acknowledged write; the agent-authored app
required zero framework patches.

## M1 — The platform becomes real [shipped 2026-07-27; four bullets deferred, marked below]

- **Replication:** async log shipping, follower reads, manual/arbiter failover. *Shipped.*
  Backup = log segments + snapshots to object storage: **deferred to M2, not built** —
  `serve::backup` is a promotable hot standby, not a snapshot shipper.
- **Realtime:** SSE subscription surface over `#[event]` with exact resume-from-cursor.
  *Shipped.* WebSockets and TS client codegen for events and routes: **deferred to M2**
  (both were declared non-goals of the realtime spec).
- **Auth, properly:** OIDC (static-JWKS RS256), API keys, sessions. *Shipped.* Tenant
  scoping as typed key prefixes: **deferred to M2** — multi-tenancy is a convention today,
  not compiler-enforced (DESIGN §9 carries it as its own future design).
- **Migrations:** schema versions as log records; compile-time incompatibility detection.
  *Shipped.*
- **Error/introspection polish:** `charpente describe`, structured sim-failure reports —
  the agent-interface quality bar (DESIGN §7) gets its own test suite. *Shipped.*
- **Edge harness:** Vörtex-style black-box rig for the hyper edge — stock binary,
  TCP-proxy fault injection, process kill/pause. The sim proves the kernel; this covers
  the wire (`serve`). *Shipped.*

**Exit criteria:** a follower promoted mid-write-load loses nothing and serves reads
throughout; a reconnecting realtime client provably misses no events. **Both discharged
2026-07-27**, each with an oracle at two layers (an in-sim property plus a wire oracle in
the edge harness). None of the four deferred bullets above gates either criterion — which
is why the milestone closed with them outstanding rather than silently absorbing them.

## M2 — The moat deepens [in progress]

- **Honest benchmark harness** — the "publish honest benchmarks early" mitigation below,
  and the instrument DECISIONS §7's LSM build triggers are read against. *Shipped
  2026-08-10*; numbers of record and method in [docs/BENCH.md](docs/BENCH.md). Trigger (a)
  does not fire on them: the commit path is fsync-bound at ≈1x the device ceiling, so redb
  is not the binding constraint at this workload.
- **Primary-side group commit** — the follow-up the harness above named and sized. *Shipped
  2026-08-15.* One commit point per kernel loop iteration rather than one per transaction:
  `begin_group`/`stage`/`commit`, a two-level overlay, one fsync for the whole batch,
  `MAX_BATCH = 64`. Ack-after-durable is unchanged — apply stays strictly after sync, and
  `Db::transact` is the group-of-one case, so the single-request path executes the
  pre-batching instruction sequence. Numbers and the conditioning that must travel with
  them are in [docs/BENCH.md](docs/BENCH.md): the win exists where the queue is deep enough
  to fill a batch and is nothing at one connection, on a device whose slow fsync flatters
  the mechanism. The bare-metal Linux run stays owed.
- **Own LSM storage engine**, proven under the (by now battle-hardened) sim harness before
  it replaces redb. The harness is what makes this safe to attempt. **Deferred behind
  measurement** (ruled 2026-08-06): the build triggers live in DECISIONS §7, and the
  benchmark harness above is what reads them.
- **Blob store** (content-addressed, spill-over columns, direct streaming).
- **Object-storage backup** (deferred from M1): log segments + periodic snapshots
  off-site, Litestream-style.
- **WebSockets + TS client codegen** (deferred from M1).
- **Tenant scoping as typed key prefixes** (deferred from M1; DESIGN §9).
- **Typed telemetry with change-time checks** — the observability contract as a lockfile;
  instrumentation that cannot silently rot.
- **Read-only Postgres-wire endpoint** for BI tools.
- **Compile-latency work:** platform-as-prebuilt-dylib; optionally cranelift hot reload for
  handlers if it proves out.
- **Hosted control plane** — if this becomes a company: managed replicas, backups,
  failover, metrics. The binary stays fully self-hostable; the moat is the platform, the
  business is not-having-to-operate-it.

## Risks, honestly

| Risk | Read | Mitigation |
|---|---|---|
| Rust compile latency poisons the dev loop | The bet accepted going in | Prebuilt platform dylib; measure app-only incremental times in M0; cranelift hot reload later; toolchains are improving underneath us |
| Storage engine is the hardest 20% | Real, why it's staged | redb first; own engine only after the sim harness can prove it; the harness is the safety equipment |
| "No await in transactions" alienates users | Real for humans, smaller for agents | Docs sell the constraint as the feature it is; queue idiom must be *excellent* and heavily exampled |
| Single-writer throughput ceiling | Overrated | No network hop between logic and data; TigerBeetle/Redis-class evidence; publish honest benchmarks early |
| Greenfield-only adoption | Accepted | Agents mint greenfield backends daily; distribution rides agent preference (MANIFESTO §3), not migration |
| Convex/PocketBase move into the quadrant | Watch | Neither has determinism or Rust's compile-time oracle; DST-from-day-one is a multi-year retrofit for them — that's the moat |

## Non-goals (permanent)

No sharding. No multi-master. No SQL-first API. No service mesh. No optional-determinism
escape hatches. No feature flags on guarantees.

## Naming

Crate `charpente` — verified available on crates.io 2026-07-01 (along with the rejected
alternates; `socle`, `massif`, `menhir`, `chassis`, `armature` are taken). Reserve the
crate name early with an empty publish.
