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
  scoping as typed key prefixes: **deferred to M2**, and shipped there 2026-09-04 (see the
  M2 bullet below; DESIGN §8 carries the mechanism).
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
  did not fire on that reading: the commit path measured fsync-bound at ≈1x the device
  ceiling, so redb was not the binding constraint at that workload on that hardware. **That
  reading pre-dates group commit**, which is precisely what removes the
  one-fsync-per-transaction ceiling it rests on — the core layer is unchanged, the kernel
  layer is not — so the trigger was owed a re-read. **That re-read was delivered
  2026-08-20** by the per-batch-apply sub-project below, on the same instrument and
  protocol with a fresh interleaved baseline: **trigger (a) does not fire under any
  reading** of "largest single non-idle phase" — the field reading leads by 71-73
  points, and the two readings that would previously have fired now decline. DECISIONS
  §7 remains untouched and Noah's to sign off; what changed is that the ambiguity which
  made it a judgment call is gone, so the deferral simply stands
  (docs/LEDGER.md carries both the item and the numbers).
- **Primary-side group commit** — the follow-up the harness above named and sized. *Shipped
  2026-08-15.* One commit point per kernel loop iteration rather than one per transaction:
  `begin_group`/`stage`/`commit`, a two-level overlay, one fsync for the whole batch,
  `MAX_BATCH = 64`. Ack-after-durable is unchanged — apply stays strictly after sync, and
  `Db::transact` is the group-of-one case, so the single-request path executes the
  pre-batching instruction sequence. Numbers and the conditioning that must travel with
  them are in [docs/BENCH.md](docs/BENCH.md): the win exists where the queue is deep enough
  to fill a batch and is nothing at one connection, on a device whose slow fsync flatters
  the mechanism. The bare-metal Linux run stays owed.
- **Per-batch apply (the store's commit point)** — the follow-on group commit exposed:
  the WAL sync was amortized 62x while the store still paid one write transaction *and*
  one read transaction per record. *Shipped 2026-08-20.* `Store::apply_batch` commits a
  contiguous run's row effects atomically with `applied_seq` in ONE redb write
  transaction; every record commits or none does. Kernel throughput 1.78-1.88x, Disk ops
  13.37 -> 1.23 per log record, store block 6.2x. It also **retracted a guarantee**:
  followers converge *logically*, not byte-identically — batch boundaries are a
  scheduling artifact, not part of the log (DESIGN §1, ruled 2026-08-20). Numbers and the
  provenance rules that constrain how they may be quoted are in
  [docs/BENCH.md](docs/BENCH.md).
- **Object-storage backup** (deferred from M1) — log segments and seq-cadenced snapshots
  shipped off-site, Litestream-style. *Shipped 2026-08-29.* `ObjectStore` joins
  `charpente-core::io` as the fifth top-level trait behind hard rule 1, with `FsObjectStore` in
  `prod` and a seeded `SimObjectStore`; the archive runs as a kernel arm, so it ships from
  the same log the primary commits to and holds a **bounded** retention pin that composes
  with replication's. `charpente restore` rebuilds a Db from a bucket and refuses a diverged
  timeline, an unreadable manifest and every §7 corruption case **by name** rather than
  guessing. Generations are committed by a manifest swap, and a manifest names its writer:
  an archive that finds another writer's manifest stops rather than interleaving. Bucket
  reclamation deletes a superseded generation's objects **by name, out of the manifest it
  just replaced** — never a LIST-and-sweep, which was refused because a sweep whose
  arithmetic is wrong deletes what a restore needs. The flagship oracle is a convergence
  property over 256 universes on each of four pinned bases: what the archive holds restores
  to what the Db held.
  **Two limits travel with it, both documented rather than engineered around.** Objects
  orphaned by an interrupted upload, an abandoned generation, or a drain cut short are
  unreachable by any code — an object is orphaned precisely in that no manifest names it —
  so operators MUST configure a bucket lifecycle rule; `docs/GUIDE.md` §15 states it as a
  requirement, not a footnote. And the delete-fault axis, absent when reclamation shipped,
  now exists — `FaultConfig.object_delete_error`, rolled from its own
  `keyed_stream(seed, "object_store_delete")` so that declaring it perturbed no pinned
  universe (all twelve convergence coverage counts across four bases reproduce exactly) —
  and **the convergence property now turns it on**: a delete fails in 32 of 256 universes at
  the pinned base, all 32 still restore, and `archive:delete_failed` is a `require:` key so a
  batch in which no delete ever failed is a red property. "Correct when every delete fails
  forever" keeps its two aimed hand-driven modes, which a per-op probability cannot replace,
  but it is no longer the only evidence.

- **Own LSM storage engine**, proven under the (by now battle-hardened) sim harness before
  it replaces redb. The harness is what makes this safe to attempt. **Deferred behind
  measurement** (ruled 2026-08-06): the build triggers live in DECISIONS §7, and the
  benchmark harness above is what reads them.
- **Blob store** (content-addressed, spill-over columns, direct streaming).
- **TypeScript client codegen** (deferred from M1). *Shipped 2026-08-29.*
  `charpente_tsgen::emit_client` is a pure `Contract -> client.ts` function: branded id types, a
  discriminated `Result<T, E>` per route, and per-event SSE dispatch for a route's
  `emits(...)`. `charpente gen-client`/`check-client` are the CLI verbs; `check-client` is
  CI's drift oracle (0 identical / 1 stale / 2 build-or-usage fault / 101 a contract the
  emitter refuses by name). A type-check alone is half a gate — `tsc --noEmit` proves a
  call site type-checks, never that a runtime value matches its declared type — which is
  why `ts-fixture/`'s Node-native suite runs the generated client against real
  `serde_json` documents through a stubbed `fetch`, not `tsc` alone. No bare `u64`/`i64`
  reaches the wire: the `Wire` bound refuses one at compile time, and `WireU64`/`WireI64`
  carry a 64-bit value as a decimal string, `bigint` client-side. Transport is SSE only.
- **WebSockets** (deferred from M1) — the generated client targets SSE; a WebSocket
  transport would be a swap behind the same generated API (routes, types, `Result<T,
  E>`), not a second client.
- **Tenant scoping as typed key prefixes** (deferred from M1). *Shipped 2026-09-04.*
  `#[table(scoped_by = Org)]` makes `(scope, id)` the row key and prefixes every secondary
  index, and the scope-taking primitives are private to the core — so a scoped row is
  reachable only through `tx.scope::<Org>(org, ctx)?`, which runs the app's own
  `Tenant::authorize` first. "Every query is tenant-scoped" stops being a review checklist
  item: a scoped read on a bare `Tx` is a compile error naming the table, its scope, and
  the line to write, and the traits underneath it (`NoTenant`, `UnscopedRow`) are sealed,
  so an app cannot opt back out in one orphan-legal line. The scope travels the whole way
  down — WAL effects, replication, archive, `restore`, snapshot seeding, and
  `migrate_schema`'s watermark — and a binary compiled against the unscoped shape of a
  scoped table refuses the record by name rather than misreading it. Realtime is the same mechanism rather than a second
  one: `emit` inside a scope is keyed by the scope. Enumerating every tenant is a
  **declared capability**, not an escape hatch — `#[route(…, cross_tenant)]` /
  `#[job(cross_tenant)]` mint the token `Tx::across_tenants` requires, rows come back
  paired with their scope, and `charpente describe` turns "audit the cross-tenant reads"
  into a mechanical inventory (DECISIONS §11). The flagship oracle is
  `every_scoped_read_returns_only_its_own_tenants_rows` — all four read paths, two tenants
  sharing an id space *and* an index key, 256 universes at a pinned base plus 768 over
  three fresh ones — with two naive victims that fail it by name, a scope-keyed realtime
  sibling, and a two-tenant wire oracle over real TCP in the edge harness.
  **Two limits travel with it.** The subscription key is the scope's decimal id, so two
  different scope types sharing a raw id share a key. And the guarantee's boundary is the
  typed surface handed to handlers and jobs: a `Db` holder reaches every tenant and can
  mint the cross-tenant capability, with the standing of whoever holds the data directory —
  an operator, not a request (DECISIONS §12, drafted and awaiting sign-off).
  Deliberately not built: nested scopes, cross-tenant writes in one transaction,
  platform-derived tenancy, tenant-local ids, and any forced retrofit of existing tables.
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
