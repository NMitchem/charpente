# Charpente — Architecture & Design

Status: design phase. This document records the load-bearing decisions and their
rationale. Decisions marked **[IRREVERSIBLE]** cannot be retrofitted and are argued in
detail; everything else is chosen for speed with a clear upgrade path.

## 0. System overview

```
                        ┌─────────────────────────────────────────────┐
                        │              charpente binary                │
                        │                                             │
  HTTP/WS clients ────▶ │  ┌─────────┐   ┌──────────────────────────┐ │
                        │  │  HTTP    │   │   Deterministic core     │ │
                        │  │  layer   │──▶│                          │ │
                        │  │ (routes, │   │  ┌────────┐  ┌────────┐  │ │
                        │  │  auth)   │◀──│  │  Tx    │─▶│  Log   │  │ │
                        │  └─────────┘   │  │ engine │  │ (WAL)  │  │ │
                        │                │  └────────┘  └───┬────┘  │ │
  realtime (SSE/WS) ◀── │ ── event tail ─│─────────────────┤        │ │
                        │                │  ┌────────┐     │        │ │
                        │                │  │ Tables │◀────┘ apply  │ │
                        │                │  │(storage)│             │ │
                        │                │  └────────┘             │ │
                        │                └──────────┬──────────────┘ │
                        │   post-commit dispatch    │                │
                        │  ┌────────┐  ┌───────┐    │   log shipping │
                        │  │ Queue  │  │ Cron  │◀───┘        │       │
                        │  │ workers│  │       │             ▼       │
                        │  └────┬───┘  └───────┘        followers    │
                        └───────┼────────────────────────────────────┘
                                ▼
                        external effects (email, APIs) — post-commit only
```

One process. The HTTP layer translates requests into transactions; transactions run
against the deterministic core; the core's only output is an ordered log; everything else
(table state, queue dispatch, realtime feeds, replicas) is a deterministic function of
that log.

## 1. The deterministic core **[IRREVERSIBLE]**

**Decision.** Every source of nondeterminism — disk, network, time, randomness, task
scheduling — passes through trait objects owned by the runtime, with a production
implementation and a simulation implementation. Application transactions execute inside
the deterministic boundary and cannot perform ambient IO.

**Why irreversible.** FoundationDB and TigerBeetle are unambiguous on this: deterministic
simulation testing only works if the system is designed for it from the first line. One
`SystemTime::now()`, one stray `tokio::spawn`, one direct file write inside the core, and
replay determinism is gone — and with it the entire verification story that justifies the
platform. Retrofitting determinism onto a nondeterministic codebase is a rewrite.

**Shape.** Synchronous and poll-shaped, not async futures (DECISIONS §10). `Instant` is
charpente's own nanosecond type — std's is opaque and cannot be fabricated from virtual
time.

```rust
trait Clock   { fn now(&self) -> Instant; fn sleep(&self, d: Duration); }
trait Entropy { fn fill(&self, buf: &mut [u8]); }
trait Disk    { fn open(&self, path: &str) -> Result<Box<dyn File>>;
                fn remove(&self, path: &str) -> Result<()>;
                fn list(&self, prefix: &str) -> Result<Vec<String>>; }   // always sorted
trait File    { fn len(&self) -> Result<u64>;
                fn read_at(&self, offset: u64, out: &mut [u8]) -> Result<()>;
                fn write_at(&self, offset: u64, data: &[u8]) -> Result<()>;
                fn set_len(&self, len: u64) -> Result<()>;
                fn sync(&self) -> Result<()>; }  // the durability barrier
trait Net     { fn listen(&self, addr: &str) -> Result<Box<dyn Listener>>;
                fn connect(&self, addr: &str) -> Result<Box<dyn Conn>>; }
// Listener::accept and Conn::recv are non-blocking polls; Net is
// message-oriented until the HTTP edge demands byte streams.
```

The simulator implements all of it with one seed driving: latency distributions, virtual
time (a simulated year of heartbeats runs in seconds), and the fault matrix — injected
write/sync errors, crash-restart between any two disk operations, partitions, and
crash-time write-buffer collapse in which every unsynced write independently survives,
tears (an arbitrary strict subset of its sectors), or vanishes. Subset tears matter: they
produce the file-extends-past-garbage layouts that defeat length-prefix-only recovery,
which prefix-only tearing never catches (found while building the first meta-test).

**Consequence for the concurrency model.** The core state machine is single-threaded —
concurrency lives at the edges (HTTP handling, queue workers, replication) and is itself
scheduled through the deterministic executor in sim mode. Throughput comes from the absence
of network hops and lock contention, not from parallel mutation. This mirrors TigerBeetle
and Redis: a single thread applying a log is astonishingly fast when it never waits.

## 2. The write model: transactions as pure functions over a log **[IRREVERSIBLE]**

**Decision.** A transaction is a deterministic function `fn(&mut Tx) -> Result<T, E>` that
reads tables, writes tables, enqueues jobs, and emits events. On success, its effects are
serialized as one record into a single ordered write-ahead log. Table state is the result
of applying the log. There is no other way to mutate state.

**What a transaction cannot do:** await the network, read the wall clock directly (it gets
the transaction's logical timestamp), generate ambient randomness (it gets seeded entropy),
or spawn tasks. External effects are expressed as enqueued jobs that run *after* commit.

**Why.** Three properties fall out of this single decision:

1. **Replication for free.** Followers replay the same log through the same deterministic
   state machine and arrive at byte-identical state. Failover is log position arithmetic.
2. **Simulation for free.** A deterministic function of (state, input, seed) is trivially
   replayable. Every production incident is reproducible if you have the log segment.
3. **The dual-write class deleted.** Job enqueues and event emissions are rows in the same
   log record as the state change. Commit is a single atomic append + sync.

**The cost, stated honestly.** No `await reqwest::get(...)` inside business logic that also
writes state. The idiom is: transaction commits intent → queue worker performs the external
call → result comes back as a *new* transaction. This is the saga pattern made mandatory
and mechanical instead of optional and artisanal. Agents handle this discipline better than
humans do — it is a rule a compiler can enforce (`Tx` is `!Send` into IO contexts, and no IO
capability is in scope inside a transaction).

**Log format (v0, implemented).** Frames of `[seq u64][len u32][crc32 u32][payload]`
(crc over all three) in segments named `wal/<base_seq:020>.log`; rotation syncs the old
segment before the new one takes writes, so a tail `sync` is a whole-log durability
barrier. Recovery scans in order, cuts at the first invalid frame, truncates there and
syncs the truncation before accepting appends (load-bearing: without it, a same-length
discarded frame beyond the cut can be resurrected by a later recovery, checksum-valid and
seq-correct), and deletes later segments. Any IO failure poisons the log — fsync results
cannot honestly be retried (fsyncgate); the only exit from poison is restart-and-recover,
and recovery is idempotent. The apply loop (`Engine`) holds the standing invariant:
machine state ≡ fold(apply, durable records) — submit is append → sync → apply → ack.

**Record payload (v0, implemented).** A committed transaction's effects:
`[n: u32]` then n tagged effects — put `0x00 [table][row]`, delete
`0x01 [table][id]`, enqueue `0x02 [job][payload]`, emit `0x03 [event][payload]`,
strings and byte fields length-prefixed per the row codec. Table effects are
the transaction's collapsed write set in (table, id) order — intermediate
overwrites inside a transaction are observable by no one and are not logged;
enqueues and emits keep call order. Replay resolves table names through the
`Schema` registry passed to `Db::open`.

**Isolation.** Serializable, trivially: transactions apply one at a time to the
single-threaded core. Read-only queries run against consistent snapshots (see §4) and never
block writes. Long transactions are rejected by construction — a transaction is CPU-bound
by definition, so "one at a time" costs microseconds, not milliseconds.

## 3. The typed surface

Every contract in the system is a Rust item; every contract violation is a compile error.

- **Tables.** `#[table]` on a struct derives the row codec, key type (`Id<T>`, typed, never
  a bare u64), and secondary index declarations. Schema *is* the Rust definition.
- **Migrations.** A schema change generates a migration as a log record (schema versions
  live in the log like everything else). Incompatible changes — dropping a field a query
  still uses — fail at compile time because the query is code referencing the field.
- **Routes.** `#[route(POST "/invoices/:id/finalize")]` — path params, body, and response
  are typed; the OpenAPI description is *derived from* the types, never hand-written.
- **Jobs.** `#[job]` structs with typed payloads and typed retry policy. Enqueue sites and
  handler signatures are checked against each other.
- **Events.** `#[event]` structs; `tx.emit(...)` is typed; subscribers (server-side and
  client-side codegen) consume typed streams off the log.
- **Config.** A typed struct, validated at startup, no stringly `env::var` in app code.
- **Telemetry.** Counters, histograms, and log fields are declared as types with
  change-time checks: deleting a metric a dashboard depends on, or renaming a label, is a
  detectable breaking change against a lockfile of the observability contract, the same way
  Cargo.lock pins dependencies. Instrumentation that cannot silently rot.

**Query API [reversible — start simple].** V0 is get/put/scan over typed keys plus typed
secondary indexes — deliberately KV-shaped, not relational. Rich predicates, joins, and
aggregation arrive as typed combinators once real usage shows which ones matter. A
read-only Postgres-wire endpoint (for BI/analytics tools) is a later, separate surface and
never the primary API. Read accessors are generated against the `View` trait —
implemented by both `Snapshot` (outside transactions) and `Tx` (inside one, where reads
also see the transaction's own writes).

## 4. Storage engine [reversible — staged]

**Stage 1 (M0): `redb`** (pure-Rust embedded B-tree) mounted *behind the `Disk`
simulation boundary* via its backend abstraction, so crash/torn-write fault injection
exercises the real storage code from day one. Rationale: proven, pure Rust, and the WAL +
snapshot design above does not depend on the tree's internals.

**Stage 2 (post-M1): purpose-built LSM.** The log-structured write model wants log-
structured storage: the WAL is already the L0. Building our own engine is the scariest 20%
of the project and also the moat — but it becomes *safe* to build only once the simulation
harness exists to prove it. Sequence accordingly: harness first, engine second. Swapping
engines under the trait is a contained change because nothing above the `Disk`/table layer
knows tree shape.

**Table store (v0, implemented).** redb mounts behind `Disk` via a `StorageBackend`
adapter (`table::Store::open` — the raw-path constructors are deny-listed). The store is
a materialization of the log, never a second source of truth: every `apply(seq, …)`
commits row effects atomically with the `applied_seq` meta key at `Durability::None`
(no fsync — the WAL is the durability story); `checkpoint()` persists everything at
once; after a crash the store reverts to its last checkpoint and recovery replays the
WAL from `applied_seq + 1`. Two redb facts the sim established: its byte layout is
deterministic under identical op sequences (raw-fingerprint replay holds), and
`Database::drop` is a *graceful close* that durably flushes non-durable commits — a
crash is only a crash if the process dies with the database open, which is exactly what
the sim models. Index entries are `[key len: u32][key bytes][id: u64 be]` (length prefix
prevents prefix-collision ambiguity); exact-match lookups only until a real query
pattern demands order-preserving key encoding. A built-in `charpente.jobs` table (key
`(seq, idx)`, value `[name][payload]`) materializes transactional enqueues in the same
apply — the queue dispatcher's substrate (§6).

**Snapshots & reads.** MVCC snapshots at log positions; read-only queries (and follower
reads) run against snapshots without touching the write path. Blob columns over a
configurable size spill to a content-addressed blob store (same log-commit discipline for
the metadata, direct streaming for the bytes).

## 5. Replication & deployment [reversible in mechanism, fixed in stance]

- **Mechanism (v1):** asynchronous log shipping to N followers; a follower is promoted on
  primary failure (external arbiter or manual, v1 — consensus-elected promotion can come
  later *without* changing the data plane, because the log format is the contract).
- **Stance (fixed):** one primary, no sharding, no multi-master. See MANIFESTO §7.
- **Deploy:** one binary + one data directory. Upgrades: new binary replays/attaches to the
  same log (schema versions in-log make this well-defined). Backup: log segments + periodic
  snapshot to object storage, Litestream-style, built in — not an add-on.

## 6. Queue, cron, realtime — all faces of the log

- **Queue.** `tx.enqueue(job)` writes the job into the committing log record. Post-commit,
  the dispatcher (outside the deterministic core, but sim-schedulable) leases jobs to
  workers. At-least-once delivery; exactly-once *observability* via idempotency keys stored
  transactionally with the job's own resulting transaction. Retries, backoff, and dead-letter
  are typed policy on the `#[job]` item.
- **Cron.** Schedules are rows; ticks are transactions. In simulation, virtual time makes
  "run this monthly job 120 times" a subsecond test.
- **Realtime.** `tx.emit(event)` puts events in the log; SSE/WebSocket subscribers tail the
  log with a cursor. Resume-from-cursor is exact (it is a log position), so reconnects have
  no silent gap — the failure mode that plagues every bolted-on pub/sub.

## 7. The agent interface

Charpente treats the coding agent as the primary developer persona:

- **Introspection endpoint.** `charpente describe` (and a matching HTTP surface in dev)
  emits the full machine-readable contract: tables, indexes, routes, jobs, events, config,
  telemetry — the "what exists" query agents otherwise burn context reconstructing.
- **Errors are prompts.** Compile-time and sim-failure output is structured for machine
  consumption: the exact contract violated, the file/line, the seed and fault schedule for
  sim failures, and the minimal repro command. Error quality is a feature with a test suite.
- **`charpente test --sim`** is the agent's inner loop: seeded, parallel, deterministic.
  A failure message *is* a reproduction (`--seed 0x9f3a...`), so the agent's fix-verify
  cycle needs no flaky-test triage.
- **Skill/docs layout.** Documentation ships as both human prose and a machine-readable
  pack designed to be loaded into an agent's context (the framework's conventions are only
  worth having if the agent applying them has them in scope).

## 8. Auth & multi-tenancy [M1, reversible]

Sessions, API keys, and OIDC as built-in typed tables and route guards; org/tenant scoping
as a first-class key prefix so "every query is tenant-scoped" is a type-level property
rather than a code-review checklist item. Deliberately boring; the value is that it is
*inside* the commit point like everything else.

## 9. Open questions (tracked, not blocking)

1. **Hot reload of handlers** (cranelift-JIT route handlers for sub-second dev loop) —
   valuable, unproven, not load-bearing for M0.
2. **Read concurrency model** — snapshot reads on worker threads vs. everything through the
   core loop; measure before choosing.
3. **Query combinator surface** — how far toward relational before it stops paying for its
   type complexity.
4. **Client codegen** — TS client from route/event types is obvious; when.
5. **Compile latency mitigation** — platform as prebuilt dylib so app iteration recompiles
   app code only; quantify the win early, it is the adoption risk (see PLAN §Risks).
