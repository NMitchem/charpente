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
                fn list(&self, prefix: &str) -> Result<Vec<String>>;   // always sorted
                fn sync_dir(&self, prefix: &str) -> Result<()>; }  // the directory barrier (§2)
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
Three more axes (the Jepsen/TigerBeetle lessons — corruption *granularity* is its own fault
axis; whole-sector injection provably misses single-byte bugs), landed with WAL segment
management and paired with the recovery-horizon rule in §2: `FaultConfig.bit_flip` corrupts
one random bit of one random byte in one random file's durable content at `Sim::restart`,
before crash collapse; `FaultConfig.misdirected_write` lands a `write_at`'s payload past the
requested offset while the call still returns `Ok` (the requested offset stays stale);
`FaultConfig.read_error` is a transient `read_at` failure (the next read may succeed). All
three (plus `dir_entry_loss`, §2) draw from per-field sub-streams keyed by (universe seed,
FNV-1a of the field name) rather than `FaultConfig::swarm`'s legacy sequential draw — each
field's stream is independent, so adding one perturbs no existing universe's legacy draws
and no other extended field's draws (`sample_extended_fields`, charpente-sim, applied right
after `FaultConfig::swarm` in every swarm-sampled universe). The two durable-corruption
axes (`bit_flip`, `misdirected_write`) are scoped to `FaultConfig.corruption_scope`
(default `"wal/"`): they exist to prove the WAL fences (frame CRC + the recovery-horizon
rule), and store-file bitrot is beneath those fences — v0's answer to it is redb plus M1's
restore-from-replica, and M2's own storage engine owns page checksums; `read_error` and
`dir_entry_loss` stay global (they produce errors, never silent wrongness). Even inside
the scope, the horizon fence only defends checkpointed data: corruption of an acked frame
above `applied_seq` is indistinguishable from a torn tail and is legitimately cut (the
standard WAL convention; un-checkpointed data has one local copy, and M1 replication is
the answer), so acked-durability workloads route their shortfall through
`checks::durable_corruption_fired` — loss is excused only in universes where a
durable-corruption fault actually fired.

**Per-host worlds** (landed with M1 replication's first task): `Sim::host(name)` gives a
named simulated host its own disk (`DiskWorld`, unchanged — the same crash/countdown/epoch
model, just one instance per host) and its own fault + entropy streams, keyed sub-streams
off (universe seed, host name) via the same construction as the extended fault fields
(`keyed_stream`, formerly `sample_extended_fields`'s private `stream` helper) — a host's
rolls never depend on other hosts' op interleaving, and creating or using a host draws from
none of the frozen global streams (`Sim::disk()`, `Sim::entropy()`, `Sim::restart()`,
`Sim::durable_fingerprint()` are byte-identical to before this task; every pre-existing
pinned seed replays unchanged). Virtual time and disk latency stay universe-global — time
is one dimension, not a per-host one. `SimHost::crash_now`/`crash_after`/`restart` mirror
the global equivalents scoped to that host's disk (crash coverage is the new `"crash:host"`
key, distinct from the global `"fault:crash"`), and additionally tear down the host's
network presence (`NetWorld::teardown_host`): every connection with that host on one side
closes that side and drops the in-flight queue addressed to it (a dead host will never read
those bytes), and every listener it owns is removed so a restart's `listen` on the same
address succeeds. Listener tokens (bumped by a `next_listener` counter) stop a handle held
across a crash from accepting on, or clobbering, a later re-`listen` at the same address.
This is the sim-side prerequisite for the multi-node replication universes in M1's second
sub-project — nothing above the sim is testable without host-scoped crash/restart.

**Consequence for the concurrency model.** The core state machine is single-threaded —
concurrency lives at the edges (HTTP handling, queue workers, replication) and is itself
scheduled through the deterministic executor. The executor's load-bearing rule (the
FoundationDB half-measures lesson): the kernel loop's choice of *which ready event source
to service next* is an explicit decision made by the same code in production and in
simulation — only the entropy behind the decision differs. A production loop that
multiplexes by racing a real channel while the sim draws from a different structure is
not testing the scheduler production runs. Throughput comes from the absence of network
hops and lock contention, not from parallel mutation. This mirrors TigerBeetle and Redis:
a single thread applying a log is astonishingly fast when it never waits. The rule is
implemented (M0 step 6): `charpente::kernel`'s `Kernel::run` builds the ready set over
requests (`RequestSource`), due jobs, and due cron ticks and draws one entropy pick only
when more than one is ready — idle iterations spend no entropy. The request arm stops the
loop when the engine is poisoned (ruling 2026-07-05, spec `2026-07-05-sim-cli-design.md`):
a 500 goes to the client first, then `Db::poisoned()` gates continuing.

The HTTP kernel (`charpente::http`) lives inside the boundary — the sim drives
`App::handle` directly; hyper only translates wire↔struct at the edge
(`charpente::serve`) and feeds the single-threaded kernel loop.

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

**Two durability fences.** (1) *Held (recovery-horizon rule).* A cut can only ever
discard unacknowledged data, so recovery must tell a torn tail from mid-log corruption
of committed frames. `Db::open` reads the store's `applied_seq` before recovery — the
durable horizon — and threads it through `Engine::recover` into
`Wal::open_with_horizon`: an invalid frame at or below the horizon is a hard corruption
error naming the frame, the horizon, and restore-from-replica/backup, never a cut (and
the error path mutates nothing — no truncation, no segment deletion); above the horizon
it is a torn tail and cuts exactly as before. The rule's other half guards the state no
invalid frame can reveal: if the log's `base_seq` exceeds the horizon + 1, whole
acknowledged records exist in neither the store nor the log (never a state checkpoint
GC can produce — its floor is bounded by its own checkpoint's `applied_seq + 1`), and
`Db::open` refuses with the same restore-naming error before recovery runs. Standalone
`Wal::open` (no store in sight) keeps cut-at-first-invalid. M1 replication is what
makes repair possible.
(2) *Held.* `File::sync` does not persist a file's *directory entry*, so a freshly
rotated-in segment's name (or a store file's, at creation) could vanish on power loss,
taking acknowledged records with it, or a removed name could resurrect one a recovery
already discarded. `Disk::sync_dir(prefix)` is the barrier: on `Ok`, every prior
create/remove of a name starting with `prefix` is durable — same flat-namespace prefix
contract as `list`. The prod impl fsyncs the holding directory; the sim tracks pending
creates/removes and, only when `FaultConfig.dir_entry_loss > 0.0`, independently rolls
each pending entry at crash (an affected create vanishes even if its bytes synced; an
affected remove resurrects the file with its last durable content) — zero rng draws when
the field is 0.0 (its default), so every pre-existing pinned seed replays unchanged. The
log barriers rotation (before the new segment takes writes) and post-cut recovery cleanup
(a durably-clean directory after truncation); the store barriers creation and the
rebuild-discard-then-reopen path. Coverage key `fault:dir-entry`.

**Record payload (v0, implemented).** A committed transaction's effects:
`[n: u32]` then n tagged effects — put `0x00 [table][row]`, delete
`0x01 [table][id]`, enqueue `0x02 [job][payload][key: Option<String>]`, emit
`0x03 [event][payload]`, strings and byte fields length-prefixed per the row
codec. Table effects are the transaction's collapsed write set in (table, id)
order — intermediate overwrites inside a transaction are observable by no one
and are not logged; enqueues and emits keep call order. Replay resolves table
names through the `Schema` registry passed to `Db::open`. The enqueue key is
the job's stable external identity — carried, never interpreted inside the
deterministic boundary in v0; M1's external-IO surface is what consumes it.

**Isolation.** Serializable, trivially: transactions apply one at a time to the
single-threaded core. Read-only queries run against consistent snapshots (see §4) and never
block writes. Long transactions are rejected by construction — a transaction is CPU-bound
by definition, so "one at a time" costs microseconds, not milliseconds.

## 3. The typed surface

Every contract in the system is a Rust item; every contract violation is a compile
error. App crates depend on `charpente` alone: all three derive macros resolve through
`::charpente::` paths (`charpente::core` re-exports the deterministic core), so the
platform is one dependency, not two.

- **Tables.** `#[table]` on a struct derives the row codec, key type (`Id<T>`, typed, never
  a bare u64), and secondary index declarations. Schema *is* the Rust definition.
- **Migrations.** A schema change generates a migration as a log record (schema versions
  live in the log like everything else). Incompatible changes — dropping a field a query
  still uses — fail at compile time because the query is code referencing the field.
- **Routes (v0 implemented).** `#[route(METHOD "/path/:param"[, auth])]` — handler args:
  `tx: &mut Tx`, one per `:param` (same name, path order), at most one JSON body arg,
  optional trailing `ctx: &Ctx` (session access); return `Result<T: Serialize, ApiError>`.
  Handlers run inside one transaction; 200 is emitted only after the commit is durable.
  The OpenAPI description will be *derived from* the types, never hand-written (step 7).
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
pattern demands order-preserving key encoding. A built-in `charpente.jobs` table
materializes transactional enqueues in the same apply as a `JobRow` per enqueue effect —
id from the monotonic `next_job_id` meta counter (bumped atomically with `applied_seq` in
the same store transaction, deterministic across replay), plus name, payload, key,
status (Pending/Done/Dead), attempts, due_at, and last_error — the queue dispatcher's
substrate (§6). Done and Dead rows are retained (GC is a later question, filed with WAL
segment GC). A store that cannot open at all — e.g. a
crash tore its creation — is discarded at `Db::open` and rebuilt by replaying the WAL
from scratch; the log is the only source of truth. "Cannot open" includes redb panicking
at open: redb 4.1.0 asserts (page_manager.rs:231/:257) instead of erroring when a crash
leaves the store file's length disagreeing with its durable header's layout — an unsynced
`set_len` drops at crash while the header god-page write survives (sim-found; the crash
model is exactly ALICE's unordered-writeback). `Store::open` converts an open-time panic
into an open failure so the discard path covers it; the fence is scoped to open — a redb
panic during operation still propagates. Reported upstream; drop the fence when redb
returns `Err` for this state. That rebuild holds only while the log still retains seq 0:
`Db::checkpoint` durably persists the store, then deletes WAL segments below the
checkpoint (`Wal::gc`, floored at `DbConfig::retain_segments`, default 2; a caller-supplied
retention pin can hold more), auto-firing every `DbConfig::checkpoint_every` committed
records (default 4096; `0` disables the cadence — manual `checkpoint` only). Once the log
is compacted past seq 0, an unopenable store is no longer rebuildable from the log alone —
`Db::open` checks the log's base before discarding the store and returns a named hard
error instead (naming the compaction, and the fix: restore from a replica/backup, or widen
`retain_segments`) rather than silently losing the records below the old checkpoint.
A second sim-established redb finding (upstream-report material): direct corruption of the
store file's durable bytes — bit flips, misdirected writes — can defeat redb's
copy-on-write crash consistency entirely, either panicking internally or silently falling
back to an older, still self-consistent root that under-reports `applied_seq`; v0 does not
defend store-file bitrot (the corruption fault axes are scoped to `wal/`, §1), redb + M1
restore is the answer, and M2's own engine owns page checksums.

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

- **Queue.** `tx.enqueue(job)`/`enqueue_keyed(key, job)` writes the job into the committing
  log record; apply materializes a `JobRow` (§4). Post-commit, the kernel dispatcher runs
  due jobs inline on the kernel thread: a `#[job]` handler is `fn run(self, tx: &mut Tx) ->
  Result<(), JobError>` — itself a transaction. `Ok` commits its effects and the job's
  Done-mark atomically in one record: exactly-once state effects, structurally. `Err`
  aborts that transaction and a second commits the failure record — attempts, doubling
  backoff, Dead at the policy's `max`, last_error carried (typed `RetryPolicy` on the
  `#[job]` item; handler panics are absorbed into the same path). At-least-once execution
  plus idempotency-key observability is the framing M1's external-effect handlers need; v0
  handlers are pure state and don't need it yet.
- **Cron.** Schedules are rows; a due tick is one transaction that advances `next_due` and
  enqueues through the normal path — a recurring enqueuer, inheriting retry and
  observability for free. Catch-up fires once per wake, never backfills. In simulation,
  virtual time makes "run this monthly job 120 times" a subsecond test.
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
- **`charpente test --sim` (v0, implemented).** The agent's inner loop: seeded, parallel,
  deterministic. App crates write property workloads in `tests/sim.rs` (`harness = false`)
  registered via `charpente_sim::sim_tests! { name require ["fault:crash", …], … }` —
  workloads share the internal suite's signature, `fn(&mut Sim) -> Result<(), String>`;
  `Err` and panics both fail a universe. `charpente test --sim [--universes N] [--seed
  0x…] [--master-seed 0x…] [--filter S] [--jobs N] [--format human|json] [--dir PATH]`
  builds the app's `sim` target via `cargo test --no-run` and execs it directly — build
  errors surface verbatim, the compiler is already the oracle. Batch model: one master
  seed (OS entropy, printed at startup, or pinned via `--master-seed` for byte-identical
  replay) derives per-universe seeds in registry order; `--jobs`-many workers race over an
  index counter, so scheduling changes nothing about the seed sequence or report order.
  `--seed 0x…` replays exactly one universe and skips `require` coverage checks (one
  universe can't be expected to hit everything a batch would). A failure message *is* a
  reproduction (`--seed 0x9f3a...`), so the agent's fix-verify cycle needs no flaky-test
  triage. `--format json` is the machine-readable surface: per-failure `{test, seed,
  master_seed, config, message, repro}`, `coverage`, `missing_coverage` (`{test, key}`
  pairs for `require`d keys never hit), and a trailing `summary`. Ships with a three-item
  checker library (`charpente_sim::checks`): `is_fault_fallout` (the injected/poisoned/
  redb-latch fallout classifier, productized out of four hand-rolled internal copies),
  `replays_identically` (determinism oracle), and `acked_implies_durable` (the
  request-level ack-durability property, generalized for app workloads). Exit codes: 0
  green, 1 sim failures or unmet coverage, 2 build/usage/infrastructure error.
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
