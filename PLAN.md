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
**The second layer's cadence, stated because the claim above used to imply more than was
true:** the wire oracle is 15 `#[ignore]`d tests that `cargo test --workspace` skips, so
from 2026-07-27 until 2026-09-05 it ran only when a human ran `scripts/edge-harness.sh`.
It now runs nightly and on demand (`.github/workflows/edge-harness.yml`); it is still not
a per-push gate, by choice — MANIFESTO §6's cadence for expensive fault work is per night.
That job runs **34** ignored tests, not 15: the 15 wire scenarios in `crates/edge-harness/
tests/` plus 19 real-disk/real-process unit tests under its `src/` that `--ignored` also
picks up and `cargo test --workspace` also skipped. First runner execution 2026-09-05
(run 33974865000): green, 34/34, 3m28s.

## M2 — The moat deepens [closed 2026-09-13 — six shipped, one dropped, the rest reclassified]

M2 never had an exit criterion; it was a list, and a list closes when what remains on it
has been sorted rather than when it is empty. Sorted 2026-09-13 against the thesis: the
six bullets marked *Shipped* below stand; the compile-latency bullet stands as dropped;
the five unshipped bullets (own LSM engine, blob store, WebSockets, typed telemetry,
Postgres-wire) move to "After launch" below, each with the trigger that reopens it, and
the hosted control plane leaves the roadmap altogether. The reasoning is in
docs/LEDGER.md's 2026-09-13 entry; the short form is that none of the five strengthens
the oracle, and the oracle is the product.

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
  one: an emit inside a scope records the scope's tenant, apart from any app key, and a
  tenant feed built from the scope receives only its tenant's events. Enumerating every tenant is a
  **declared capability**, not an escape hatch — `#[route(…, cross_tenant)]` /
  `#[job(…, cross_tenant)]` mint the token `Tx::across_tenants` requires, rows come back
  paired with their scope, and `charpente describe` turns "audit the cross-tenant reads"
  into a mechanical inventory (DECISIONS §11). The flagship oracle is
  `every_scoped_read_returns_only_its_own_tenants_rows` — all four read paths, two tenants
  sharing an id space *and* an index key, 256 universes at a pinned base plus 768 over
  three fresh ones — with two naive victims that fail it by name, a tenant-feed realtime
  sibling, and a two-tenant wire oracle over real TCP in the edge harness.
  **Two limits travel with it.** An event row records its tenant's raw id, so two
  different scope types sharing a raw id share a tenant in the event log. And the guarantee's boundary is the
  typed surface handed to handlers and jobs: a `Db` holder reaches every tenant and can
  mint the cross-tenant capability, with the standing of whoever holds the data directory —
  an operator, not a request (DECISIONS §12, ratified 2026-09-05).
  Deliberately not built: nested scopes, cross-tenant writes in one transaction,
  platform-derived tenancy, tenant-local ids, and any forced retrofit of existing tables.
- **Compile-latency work: DROPPED 2026-09-09**, on the reading in docs/BENCH.md
  "Compile latency (2026-09-09)". The prebuilt-platform-dylib item was justified on the
  inner loop, and the inner loop is not where the cost is: its stated mechanism — "app
  iteration recompiles app code only" — is already delivered by cargo's fingerprinting,
  proven by 9 of 9 incremental observations printing exactly one `Compiling` line, every
  one of them `invoicing`. Its residual headroom is the once-per-checkout clean build,
  which is a CI-checkout cost and not this item's justification. Cranelift hot reload
  stays an open question in DESIGN §9.1, unchanged and unpromoted.

## M3 — Launch [in progress; defined 2026-09-13]

**Goal:** a stranger reproduces the wager in one sitting, and the reviewer catches what it
claims to. This is M0's shape again — it ends in a recorded run, not a checklist — because
M0's exit criterion is the only one that has ever been re-run against the platform, and on
2026-09-03 it failed: a docs-only agent building a helpdesk shipped three Critical defects
(`docs/experiments/2026-09-helpdesk/README.md`). One of the three — the one-sided sim
property — now has a mechanical oracle (`charpente test --mutants`, 2026-09-07). The other
two — an unauthenticated route and an unscoped existence check — are guarantees that exist
but are opt-in, which for a framework whose thesis is *the framework is the reviewer* is the
gap that matters.

Sub-projects, in dependency order:

1. **Publish path.** Workspace to `0.1.0`; the six crates on crates.io (five names reserved
   2026-07-01, confirmed 2026-09-13; `charpente-tsgen` publishes fresh). `charpente new
   <name>` scaffolds GUIDE §1's layout — `Cargo.toml` with the `[[test]] name = "sim"`
   block, `src/main.rs`, `tests/sim.rs` — and the scaffold is compile-gated the way
   GUIDE's Rust code blocks are, so it cannot drift from the guide it claims to match.
   GUIDE §1's dependency line is `charpente = "0.1"`, and a test holds the guide's
   `Cargo.toml` block byte-identical to the verb's output.
2. **Default-secure surfaces** [shipped 2026-09-16; spec
   `docs/superpowers/specs/2026-09-16-default-secure-surfaces-design.md`]. A
   `#[route]` that declares neither `auth` nor an explicit `public` is a compile error
   naming both. A table that should be tenant-scoped and is not is a compile error — *how*
   "should be" is detected is that sub-project's design question, not settled here. The
   oracle is already in the tree: the helpdesk app at `8913038`, rebased, must fail to
   compile at both of its Critical sites.
3. **Third docs-only build** [shipped 2026-09-24; record
   `docs/experiments/2026-09-statuspage/`]. A new domain — not invoicing, not helpdesk —
   with the brief written first and the agent given README + GUIDE only. Exit: zero
   framework patches, zero Criticals in review that a platform oracle should have caught,
   `--mutants` clean over its sim properties. Findings ledgered as testimony, as the
   helpdesk write-up was. The 0.1.0 publish waited on two platform fixes this build found,
   and both are fixed: (1) `#[event]` payload evolution is unguarded, and a delivery-time
   decode failure stops the whole node with an error naming neither the event nor a fix —
   **fixed 2026-09-25** (spec `docs/superpowers/specs/2026-09-24-payload-shapes-design.md`:
   every payload records its shape and is never decoded under another; delivery fails a
   tail, never the node; a stopped kernel ends the process); (2) a job cannot open a tenant
   scope the way GUIDE §16 documents, and `authorize` sees neither the operation nor the
   caller kind — **fixed 2026-09-30** (spec
   `docs/superpowers/specs/2026-09-27-job-tenancy-design.md`: a job declares `global` or
   `scoped_by`, and a scoped one runs in the tenant it was enqueued from, or that its
   fan-out named; `authorize` is asked about a principal only; a route's stance, `auth`,
   `public` or `public(Org)`, says who reaches a tenant).
4. **Claims audit** [shipped 2026-09-22; spec
   `docs/superpowers/specs/2026-09-21-claims-audit-design.md`]. The bare-metal Linux BENCH
   run owed since 2026-08-15 lands, or the README scaling paragraph is rewritten to what
   has been measured. README guarantee 1 stops naming telemetry until telemetry exists.
   F1 from the helpdesk experiment — the raw panic trace that "demonstrably destroyed a
   competent agent's work" — is verified closed against the helpdesk repro, not assumed
   closed because a hook now exists.
5. **The launch run.** DEMO.md re-executed against the published version by a docs-only
   agent starting from `cargo install charpente-cli`: reaches a red sim with a seed, replays
   it. That transcript is the launch post.

**Exit criteria:** (a) the launch run, recorded with its seeds; (b) the third docs-only
build passes on the bar in sub-project 3; (c) every number in README.md and MANIFESTO.md
that is a claim about charpente is in docs/BENCH.md or absent, and every other figure
cites its source or carries no digits (held by `claims_census`).

## After launch (v1.x) — five from M2, one from DECISIONS §6, one from M3's claims audit, three from M3's payload-shapes fix, one from M3's job-tenancy fix, each with the trigger that reopens it

None of these is debt. Each was weighed against the thesis on 2026-09-13 (the Linux run on
2026-09-21) — does it make the reviewer catch more, or a stranger's first hour shorter —
and none does. They are listed with their triggers so that "is it time yet" has a
mechanical answer. The payload-shapes three were deferred on 2026-09-24 by the spec that
shipped detection and refusal (its §1 and §8): a foreign shape is already refused by name,
so each waits for its trigger. The job-tenancy one was deferred on 2026-09-27 by the
job-tenancy spec's §12.

| Item | Trigger |
|---|---|
| **Blob store** (content-addressed, spill-over columns, direct streaming) | First adopter needs attachments. `ObjectStore` exists behind hard rule 1 since the archive shipped; the design cost is what it was minus that trait. |
| **WebSockets** | First adopter needs bidirectional. A transport swap behind the same generated API (routes, types, `Result<T, E>`), not a second client. |
| **Typed telemetry with change-time checks** — the observability contract as a lockfile | On-thesis, and the one item here that is reviewer work; when there is a dashboard to protect. |
| **Read-only Postgres-wire endpoint** | First BI ask. Compatible with DECISIONS §4; never the primary interface. |
| **Own LSM storage engine** | DECISIONS §7's triggers, unchanged. Trigger (c) — post-launch adoption — is after M3 by definition, which is why the engine cannot gate it. |
| **Design C timeline epoch** (DECISIONS §6's sharpening step short of consensus) | The first loss-window finding that the bounded, named window does not cover. |
| **Bare-metal Linux BENCH run** (owed since 2026-08-15) | A Linux box Noah runs docs/BENCH.md's protocol on. Resizes README's scaling paragraph, which since 2026-09-21 states the laptop figure and says no bare-metal run exists. |
| **Versioned migrations for event and job payloads** (payload-shapes spec §8, approach B) | The first app that needs exact resume, or a Pending job, to survive a shape change it cannot express as a new type. |
| **A deep fingerprint** — every `Codec` type carries its own shape constant, so a new variant on a nested enum changes the fingerprint of every table and payload that holds it | The first nested-type change that misdecodes in a real app or a docs-only build. |
| **A commit-time shape lock** — a committed fingerprint file plus a check verb | The first shape change that reaches a deploy unnoticed. Apps on the generated TS client already get a CI signal for event payload changes from `check-client`'s staleness diff. |
| **`enqueue_keyed_in`** — a keyed declared fan-out (job-tenancy spec §12) | The first fan-out whose job needs an external idempotency key. `enqueue_in` and `ScopedTx::enqueue_keyed` both exist; the keyed fan-out is the one combination deferred. |

**Hosted control plane — off the roadmap.** "If this becomes a company: managed replicas,
backups, failover, metrics; the binary stays fully self-hostable" is a business decision,
not a milestone, and a roadmap that carries it reads as if the platform were incomplete
without it. It is not. The moat is the platform; the business, if there is one, is
not-having-to-operate-it.

## Risks, honestly

| Risk | Read | Mitigation |
|---|---|---|
| Rust compile latency poisons the dev loop | **Measured 2026-09-09, and smaller than the bet assumed** — on one 12-core laptop, `dev` profile, against `examples/invoicing`: the inner loop an agent actually meets is 436 ms (handler-body edit) and 515 ms (schema edit); only the once-per-checkout clean build is expensive, at 5.97 s wall / 37.6 s compiler CPU | Prebuilt platform dylib **dropped 2026-09-09** — see the M2 list above; cranelift hot reload remains an open question, not a scheduled mitigation; toolchains are improving underneath us. The "measure app-only incremental times in M0" mitigation **did not happen in M0**: an instrument landed 2026-09-07 (`bench-harness` `compile`/`compile_accounting`), its first reading was refused in full, and the accepted reading came on 2026-09-09. The number, its conditioning and every refusal along the way are in docs/BENCH.md, "Compile latency (2026-09-09)" — read the three phases together, because no one of them is "the compile time". One laptop, one small app, `dev`, n=3 moves this row from "unmeasured" to "measured once", not to "closed". |
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
crate name early with an empty publish. Publishable at 0.1.0 since the 2026-09-13
publish-path sub-project; the real publish is Noah's, recorded in the ledger when it
happens.
