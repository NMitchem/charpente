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
platform. Retrofitting determinism onto a nondeterministic codebase is a rewrite. The
promise holds across build modes too: redb-4.1.0's open path issues an extra,
`debug_assertions`-gated page walk that used to draw sim IO rolls in debug only and skew
release onto a different universe from the same seed (found via seed `0xff159ed76d2806e4`),
held now by forcing redb's debug-assertions on in release (`Cargo.toml`'s
`[profile.release.package.redb]`) so both modes consume one identical draw stream.

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
durable-corruption fault actually fired. Two wire-scoped axes landed with M1
replication's second sub-project, sampled from the same field-keyed sub-streams and
rolled at `Conn::send` on the global fault stream (a connection spans two hosts; the wire
belongs to neither): `FaultConfig.conn_drop` kills the live connection — the sender gets
`ConnectionReset`, the peer reads EOF, in-flight messages in both directions drop — and
`FaultConfig.net_spike` multiplies one message's drawn latency ×64 (delayed, never lost).

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
a 500 goes to the client first, then `Db::poisoned()` gates continuing. The loop body is
one `Kernel::step` (M1 replication T8): it services one ready source and returns, or
reports the deadline to park on — `run` is the thin wrapper that owns the park, so a
replicated driver can own it differently without forking the decision code. Replication
(M1) adds a fourth ready source, the `Repl` engine, whose role gates the set: a follower
(or a draining primary — `is_primary` is false while handing off) serves reads and ships
its log but never runs jobs or cron (those would hit `TxError::NotPrimary`), and a
follower→primary flip inside `Repl::step` rebuilds the due-heap and cron from the
replicated store on the next iteration. A repl-configured node also caps every idle wait at
a 5ms park tick, so in-flight replication IO — sim messages or prod TCP — is polled
promptly regardless of the next job/cron deadline. Realtime (M1 sub-project 3) adds a fifth
ready source, `Tail`: unlike the deadline-carrying sources (jobs, cron, repl) it is never
timer-gated — like a request, it is gated purely by something happening: a subscription
becomes eligible the instant some commit advances `next_event_id` past its `next_id`, and
the per-step read that checks this is skipped entirely whenever no tail is registered, so a
kernel that never sees an SSE subscribe does zero extra IO and every pre-existing pinned
seed replays unchanged. One unit of `Ready::Tail` work is a bounded table read
(`TAIL_BATCH_EVENTS` rows), serviced round-robin across live tails so one deep resume never
starves the rest of the ready set (§6).

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
barrier. Recovery scans in order and cuts at the first invalid frame: it deletes the
orphaned later segments first (highest base first, each `sync_dir`-barriered) and only
then truncates the cut segment, syncing the truncation before accepting appends. The
truncation-sync is load-bearing (without it, a same-length discarded frame beyond the cut
can be resurrected by a later recovery, checksum-valid and seq-correct); the
delete-before-truncate order is a fence in its own right (fence 3, below). Any IO failure
poisons the log — fsync results
cannot honestly be retried (fsyncgate); the only exit from poison is restart-and-recover,
and recovery is idempotent. The apply loop (`Engine`) holds the standing invariant:
machine state ≡ fold(apply, durable records) — submit is append → sync → apply → ack.
The log gains a verified read surface for the shipper (`Wal::read_from`/`Db::read_log`,
M1 replication T3): re-checksums every frame it hands back and refuses, by name, to hand
back one that fails — a frame corrupted on disk after append must never travel.

**Three durability fences.** (1) *Held (recovery-horizon rule).* A cut can only ever
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
makes repair possible. One shape is exempted because it provably loses nothing: a
checkpointed store beside an *entirely empty* log directory. Segment names are
dir-barriered before any frame lands in them (fence 2, below), so every route to a
checkpointed store leaves at least one name-durable segment — except a crash inside
`Seeder::finish`, between the store checkpoint and laying the seed segment. `Db::open`
recognizes the shape and lays the segment `finish` would have (base = horizon + 1,
nothing to replay) instead of starting a fresh log at 0 (found by replication T6, seed
0xcaef83b2feb5edf5).
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
(3) *Held (recovery-cut ordering).* A non-tail cut truncates one segment and removes the
orphaned later ones — two separate durability events. Truncating first strands a durable
gap whenever a crash keeps the truncation but drops the removals: a shortened segment
beside a surviving orphan, which the gaplessness check (fence 1's `refusing to guess`)
then refuses to recover across, bricking the node on data it was entitled to discard.
Recovery removes the orphans first — highest base first, each `sync_dir`-barriered,
exactly as `gc` removes ascending — and truncates last, so every crash-intermediate is a
contiguous prefix, recovery re-finds the same cut, and it is idempotent at every crash
point (found by replication seed 0x4177819b4a2ba984; the test-side restore path that had
masked it now gates on `checks::dir_entry_loss_fired`, so a platform-created gap fails
loud again).

**Record payload (v0, implemented).** A committed transaction's effects:
`[n: u32]` then n tagged effects — put `0x00 [table][row]`, delete
`0x01 [table][id]`, enqueue `0x02 [job][payload][key: Option<String>]`, emit
`0x03 [event][payload][key: Option<String>]`, strings and byte fields
length-prefixed per the row codec. Table effects are the transaction's
collapsed write set in (table, id)
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
also see the transaction's own writes). Follower reads need zero app-code changes:
effect-free transactions run unchanged on any node, while an effectful one on a node
marked a read-only follower (`Db::forbid_writes`) is rejected with `TxError::NotPrimary`
before it ever reaches the log (§5).

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

**Logical export (v1, implemented).** A `Snapshot` also exposes the seed path's read
surface (M1 replication T4): `applied_seq`/`next_job_id` read META off the snapshot's own
transaction — consistent with the rows it is about to hand out, unlike `Store::applied_seq`,
which opens a fresh one and so could observe a later write — and `scan_raw` hands back a
table's raw `(id, bytes)` pairs, undecoded, in id order (empty, not an error, for a table
that was never created). `Store::import`/`set_import_meta` are the write-side counterpart:
`import` commits typed rows through the same `Writer` (and so the same index maintenance)
as ordinary `apply`, but with no seq or `next_job_id` bookkeeping of its own — a logical
snapshot has no log position — and `set_import_meta` sets both meta keys explicitly, once,
after every table has landed.

## 5. Replication & deployment [reversible in mechanism, fixed in stance]

- **Mechanism (v1):** asynchronous log shipping to N followers; a follower is promoted on
  primary failure (external arbiter or manual, v1 — consensus-elected promotion can come
  later *without* changing the data plane, because the log format is the contract).
- **Stance (fixed):** one primary, no sharding, no multi-master. See MANIFESTO §7.
- **Deploy:** one binary + one data directory. Upgrades: new binary replays/attaches to the
  same log (schema versions in-log make this well-defined). Backup: log segments + periodic
  snapshot to object storage, Litestream-style, built in — not an add-on.

**Seeding (v1, implemented).** `Db::into_seeder`/`Seeder` builds a follower's (or a fresh
backup's) entire state from a primary's logical snapshot stream, no log replay involved
(M1 replication T4). `Seeder::new` first discards any store file and `wal/` segments
already at the target disk, dir-barriered — seeding always replaces a timeline, never
merges into one; `rows` applies one table's exported `(id, bytes)` pairs through the
schema's typed ops exactly as the live apply path does, indexes rebuilt as a side effect;
`finish` pins `applied_seq`/`next_job_id`, checkpoints, and lays a single empty WAL segment
at `applied_seq + 1` before handing the result through the ordinary `Db::open` — precisely
the base = horizon + 1 boundary the recovery-horizon rule (§2) already accepts, so open
sees a clean, un-compacted follower with nothing to replay. `Schema` is `Clone` and gains
`table_names` (the export walk order both sides agree on) and a crate-private `remove`:
`Db::open` always adds `JobRow` to the schema it's given, so `Db::into_seeder` — the
producer side, deconstructing a live `Db` into a `Seeder` over the same disk — strips it
back off before handing the schema to a fresh `Seeder`, which re-adds it itself; without
that, seeding a live `Db`'s own state would double-register the platform's own table. The
network-facing seed session (shipping rows to a remote follower) is a later M1 task; this
is the substrate it sends over the wire.

**The data plane (v1, implemented).** `charpente::repl::Repl` is the session engine
(M1 replication T6): the primary opens a replication listener and runs one `Session` per
inbound conn; a follower/backup runs one outbound `Follower` client against its primary.
Both are driven kernel-shaped — `pump_io` moves bytes without touching the `Db` (accept,
drain readable messages into per-conn queues, note reconnect timers), then `step` does one
unit of work against the `Db` per call (handle one message, push one `Frames`/snapshot
batch to one session round-robin, heartbeat, or reconnect). A session opens on its first
message: a `Hello` (proto version, the follower's own listen address, and its position —
last durable seq + that frame's CRC via `frame_crc`) runs the decision ladder pinned by
tests: version mismatch → `Refuse` naming both versions; a non-primary role → `Refuse`
naming the real primary (no cascading); follower at-or-ahead of the primary with unknown
bytes, or a CRC mismatch on a retained frame → `Refuse` naming both seqs, both CRCs, and
the re-seed fix (diverged timelines are refused, never merged); position below the GC'd
base (or an empty follower against a compacted log) → snapshot-seed; otherwise `Frames`
resume from position + 1. Shipping reads exclusively through `Db::read_log` — CRC
re-verified at read-back, capped at `last_applied`, so a frame corrupted on the primary's
disk is refused by name rather than shipped, and the unsynced tail never travels. The
follower appends via `Db::apply_shipped` (append → sync → apply, recovery's own
discipline) and sends `Ack { durable_seq }` only after that sync, never on receipt; a
shipped seq gap means a different timeline — drop the conn and re-`Hello`. An empty
`Frames` batch — never sent by the real shipper (it only ships when it has something to
ship), but not enforced by the wire — is a no-op on the follower side: `apply_shipped`
doesn't advance `last_applied` on an empty batch, so there is nothing to ack. The retention
pin is wired at `Repl::open`: `Db::pin_retention` reads a shared floor recomputed on every
ack and session change — the min over live sessions of acked + 1 (a not-yet-acked
session pins at its cursor; a seeding session at its snapshot's `applied_seq + 1`) — so a
live lagging session holds GC exactly as long as it lives and releases it when reaped. A
session still `AwaitHello` (accepted but silent) pins at 0 like any not-yet-acked session;
`HELLO_DEADLINE_NANOS` (10× the heartbeat interval, checked and reaped the same shape as
the heartbeat deadline) fires for one that never sends its first message — `Hello` or a
control verb — so a peer that opens the replication port and never speaks cannot hold GC
at the base forever.
Seeding streams `SnapshotStart`/`SnapshotRows`/`SnapshotEnd` out of ONE MVCC snapshot
(batches near `SNAPSHOT_BATCH_BYTES`, one message per step); the follower buffers the
stream and, at `SnapshotEnd`, latches `db_needs_reseed` — `Repl::step` cannot replace a
`Db` it only borrows, so the CALLER (the kernel arm, T8) runs `Repl::run_seed(db, clock)`,
which consumes the `Db` through `into_seeder`, replays the buffer, `finish`es at the
horizon + 1 boundary, re-gates writes, and returns the new `Db`; a conn death mid-stream
never latches, and the next `Hello` restarts the seed from scratch (idempotent —
`Seeder::new` discards). Stated plainly: v1 seeding holds the full logical snapshot in
memory on BOTH sides — the primary queues the whole stream when seeding begins, the
follower buffers it until `SnapshotEnd`. `SnapshotStart` also carries the seed frontier's
CRC — the primary's `frame_crc` of the record at `applied_seq`, which the follower persists
(`seed_frontier_seq`/`seed_frontier_crc` meta): a freshly-seeded follower holds none of that
record's WAL bytes (its own log starts empty at `applied_seq + 1`), so without it `Hello`
could only send `None`, and a primary that had since lost its own tail above the horizon
would answer that empty position with a re-seed — silently wiping a converged survivor from
the now-lossy primary. With the CRC the follower Hellos `(applied_seq, crc)`, so the ladder
runs its diverged-ahead / resume checks instead; the fallback holds across a restart before
any frame ships, and fires only while `last_applied` still equals the seed's seq — the CRC
vouches for that one record alone, and a node whose log vanished after applying past the
seed honestly sends `None` and re-seeds (finding-1 fix). A seeded-then-promoted primary that
has to seed others before its first commit chains the same stored frontier into its own
`SnapshotStart`. Right at M1 scale; incremental streaming is the
tracked fix before large-store seeding. The durable role file (§2's role semantics, T5) carries its own
CRC line: a torn role write is a named integrity error, never a parse of whatever bytes
survived — a torn "follower:p:1" that still parses as a role naming a ghost primary is
exactly what the T6 harness caught (seed 0x51dacce702b667c8).

**The control plane (v1, implemented).** Admin verbs travel over the replication port as
control messages in the same protocol (M1 replication T7) — the sim drives them through
exactly the code path the CLI uses (same-shape rule). A connection is a *control* conn
when its first message is a verb (`CtrlPromote`/`CtrlSwitchover{to}`) rather than a
`Hello`; a control conn ships nothing, pins no retention, and `Repl` (not the ship
round-robin) executes the verb and answers `CtrlOk`/`CtrlErr` on it — because every
transition flips the durable role file, re-gates the `Db`, and re-wires the pin, all
`Repl`-level. **Switchover** (`Primary → HandingOff(to) → Follower(to)`) is zero-loss and
crash-safe on the three-state role file: `store_role(HandingOff{to})` write-stops the
primary (`db.forbid_writes(to)` — reads and effect-free transactions still run, effectful
ones get `TxError::NotPrimary`), then it *drains* — the target session's `Ack`s are read
each step and, when its acked seq reaches `db.last_applied()`, the primary sends
`HandOff{at_seq}` and enters HandedOff, **the point of no return**. Draining is abortable:
the `SWITCHOVER_DEADLINE_NANOS` deadline while still Draining returns durably to `Primary`,
resumes writes, and the `CtrlErr` names the follower's remaining lag. HandedOff never
aborts: on `HandOffDone` the primary flips durably to `Follower(to)`, drops its ship
sessions, and starts a follower client toward the new primary (drained to byte equality,
no divergence possible); on the deadline it stays write-stopped and the `CtrlErr` says the
hand-off was sent — verify `to` and restart to resolve. The follower side of `HandOff`
verifies `last_applied == at_seq` (else a named `Refuse` protocol error), requires a
configured listen address (an unpromotable follower is never a switchover target — its
`Hello` carries no listen, so it never matches), flips durably to `Primary`, allows
writes, replies `HandOffDone` on the old conn, and begins accepting Hellos. A `HandOff`
buffered in the same drain as the primary's crash-EOF is not stranded: the follower client
drains its inbox before any reconnect, and the primary drains the target session's inbox
before the dead-session reap. **Promote** (`charpente promote`) is the bounded-loss
failover: a follower/backup drops its upstream client, flips durably to `Primary`, allows
writes, and the `CtrlOk` names `last_applied` — records the dead primary acked but had not
shipped are the DECISIONS §6 window, lost honestly and named. A node that restarts in a
durable `HandingOff(to)` stays write-stopped and resolves against `to`: a probe `Hello`
answered as a primary demotes it to `Follower(to)` (a diverged refusal is the operator's
re-seed signal, surfaced as the step error); `to` unreachable or not-yet-primary keeps it
`HandingOff` on a reconnect cadence — it never silently resumes `Primary`, so no
interleaving yields two writable primaries.

**Surfaces (v1, implemented).** The kernel wires the data + control plane in (M1
replication T8): `Kernel::with_replication(app, clock, repl)` runs the `Repl` engine as the
fourth ready source (§1), and `run`/`step` split so the park is the driver's. Three edges
in `charpente::serve` expose it: `serve_replicated(addr, app, repl_disk, repl_cfg)` mirrors
`serve` but the kernel thread opens `Repl` (durable role on `repl_disk`, wire on `ProdNet`)
and runs `with_replication` — a follower answers writes with 421 and reads from its
replicated copy, a primary ships; a startup role/config mismatch prints to stderr, config
never overriding the durable role. `backup(app, repl_disk, repl_cfg)` runs the same kernel
on the current thread against an `EmptySource` (no HTTP edge, no requests) — a node that
only follows and keeps a promotable copy. The reseed swap that `Repl::run_seed` needs lands
in `App`: the `Db` sits behind an `Option` so the `Ready::Repl` arm can `take` it, hand it
to `run_seed`, and `put` the rebuilt one back, adjacent so no other code observes the empty
slot.

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
- **Realtime.** `tx.emit(event)`/`tx.emit_keyed(key, event)` writes a typed event into the
  committing log record alongside the transaction's other effects — `key` is an opaque
  scoping filter (an org id, typically), carried but never interpreted inside the
  deterministic boundary. Apply materializes each into the built-in `charpente.events`
  table (`EventRow { id, seq, name, key, payload }`); `id` comes from a `next_event_id`
  meta-counter bumped in call order — the `next_job_id` pattern exactly, so ids are
  strictly monotonic, order-isomorphic to log position, and byte-identical on followers.
  Retention is count-based: `DbConfig::event_retention` (default 65,536) bounds the table,
  garbage-collected at the existing checkpoint cadence; `Db::event_floor()` reports the
  smallest id still retained.
- **The cursor is the event id, wire-encoded with its committing record's seq: `<id>.<seq>`.**
  Resume from cursor `(c, s)` delivers every matching event with `id > c` — exact by
  construction, since ids are minted at apply in log order. The seq half exists for one
  reason: failover honesty. Two diverged timelines — an old primary's unshipped tail and a
  promoted follower's new writes — can mint the same event id for two different events; a
  bare id cannot detect that. Resume validation reads the retained row at `id == c` and
  requires its `seq == s`; a mismatch is a named divergence, never a silent
  misattribution — the same shape as replication's diverged-`Hello` refusal (§5).
- **The subscription surface.** `#[route(SSE "/path")]` is a route flavor whose handler is
  an ordinary transaction (session auth via `Ctx` like any route) returning a typed
  `Subscription`: `Subscription::of::<Ev>().and::<Ev2>()` names event types compile-checked
  (never as strings in app code), `.key(...)` is an optional exact-match filter, and
  `.cursor(cursor)` takes the platform-extracted resume point (the handler passes it
  through, or clamps it — access control is the handler's own session/row logic, not the
  platform's). The platform extracts the client's cursor before the handler runs:
  `Last-Event-ID` header wins (browser-native SSE resume), else a `cursor` query
  parameter, else `None` ("from now" — only future events); an unparseable cursor is a 400
  and the handler never runs.
- **Kernel delivery is one path, no modes.** A validated subscribe registers a `Tail`
  (`{ subscription, sink, next_id }`) on the fifth ready source (§1) — eligible the instant
  a commit advances `next_event_id` past it, never a timer. One unit of work reads up to
  `TAIL_BATCH_EVENTS` rows from `next_id`, filters by name and key, JSON-encodes matches
  through the encoder each `Subscription::of::<Ev>()` captured statically at the subscribe
  site (there is no runtime event registry — "unregistered event at delivery" is
  inexpressible, not an error case), and pushes frames to a driver-supplied sink: a bounded
  channel drained by the hyper SSE writer in production, an in-memory buffer in sim. There
  is no separate push-on-commit path and no live/catch-up distinction — a commit simply
  advances `next_event_id`, which makes tails ready; "live" is just "zero behind." A sink
  that reports `Full` or `Closed` costs the tail its connection, deterministically and
  immediately: a slow or dead consumer never costs kernel progress, and the client's cursor
  resume is the recovery mechanism, not a server-side retry queue. Event payloads must be
  JSON-serializable — a serde_json rejection (NaN floats, non-string map keys) is a
  contract violation, and delivering the offending durable event is kernel-fatal (the
  encode error stops the loop; a restart re-encounters it).
- **Honesty at the edges.** A cursor below the retention floor, or one naming an event id
  this node never minted (or minted with a different seq — the cross-timeline case), is
  refused at subscribe with a 409 naming the floor or the divergence detail and the fix:
  re-sync current state through reads, then reconnect without a cursor. A tail that falls
  behind retention mid-stream gets the same fact set as a terminal SSE frame —
  `event: charpente.gap` — then its sink closes. Sink overflow itself sends nothing (there
  is no one left to tell); the client that reconnects with `Last-Event-ID` resumes exactly,
  which is the point. Follower tails read the same materialized table replication keeps
  byte-identical (§5), so a subscribe on a follower works with zero app-code changes; a
  failover honors the DECISIONS §6 bounded-loss window for events exactly as it does for
  state, and a cursor that crossed timelines gets the divergence refusal, never wrong
  events.

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

## 8. Auth & multi-tenancy [M1]

Sessions and API keys are built-in typed tables inside the commit point; a pre-transaction
resolution ladder turns a presented credential into one `Principal`, and `#[route(…, auth)]`
gates on it. Deliberately boring — the value is that credential state is a transaction like
everything else (durable, replicated, recovered on restart), not a bolted-on side store.
Everything here is pure computation plus the kernel's one `Clock` reading per request, so it
replays byte-identically in sim; the crypto is RustCrypto (`sha2`, `rsa`, `base64ct`), never
hand-rolled, and lives in `charpente`, never inside the deterministic `charpente-core`.

- **Hashed at rest.** `Session { id, token_hash #[index], subject, expires_at }` and
  `ApiKey { id, key_hash #[index], subject, created_at }` register platform-side in
  `App::open` (alongside `CronRow`), so recovery replays logins and minted keys with no app
  code. The stored `*_hash` is lowercase-hex SHA-256 of the raw credential; **the raw
  token/key exists only in the mint response and never reaches disk.** Hashing happens
  before the effects record is built, so the WAL carries the hash too — the flagship
  negative-space sim property (scan every durable file in the universe under the full fault
  matrix, assert no raw credential byte-string appears in any store, WAL segment, or role
  file) is what mechanically forbids a regression; its naive-victim meta-test (a resolver
  that stores the raw token) fails that oracle by name. `create_session(tx, subject)` sets
  `expires_at = now + session_ttl` (`AppConfig`, default 30 days in `Clock` nanos);
  `create_api_key(tx, subject)` returns `"ck_"` + 64 hex (the prefix makes a leaked key
  grep-able for secret scanners); revocation is `tx.delete::<ApiKey>(id)` — an ordinary
  write, durable and replicated like any other.
- **One entropy draw, one mint per request.** Both credential kinds are fed by the single
  existing pre-transaction `fresh_token` draw — zero new draws, so every pinned seed's
  entropy interleaving is structurally untouched (the frozen replay contract holds by
  construction, not re-verification). The consequence, enforced by a shared once-assert: one
  credential mint per request, session OR key, never both — a second mint panics
  (`Ctx::create_session/create_api_key called twice in one request — one credential mint per
  request`). Handlers cannot draw entropy (`Tx` has no `Entropy` by design), so this is the
  honest shape, not a limitation to route around.
- **The resolution ladder (pre-transaction, pure).** One entry point replaces the old
  session-only lookup, producing `Option<Principal { subject, via }>` with
  `enum Via { Session(id), ApiKey(id), Oidc }`: (1) an `Authorization: Bearer <t>` whose
  token contains exactly two `.` is judged as a JWT — OIDC-verified if a verifier is mounted,
  else a named unrecognized-credential 401; never a fall-through, a JWT-shaped credential is
  judged as a JWT. (2) Any other bearer is SHA-256'd and exact-matched against the `ApiKey`
  index, then the `Session` index (expiry-checked against `now`). (3) A `charpente_session`
  cookie takes the same hash path — and *only* the hash path: the two-dot JWT rung is scoped
  to the Authorization header, so a cookie value that happens to contain two dots is never
  OIDC-verified (a ruling, pinned by test). (4) Nothing presented is `principal = None`.
  `#[route(…, auth)]` answers the no-credential 401 when the principal is absent;
  `Ctx::principal()` is the surface, and `Ctx::session()` survives as a compat accessor
  returning the row only for a `Session` principal. The ladder runs unconditionally before
  the `auth` gate, so a garbage credential on a no-auth route is still rejected. Lookups are
  hash-then-index-equality on SHA-256 outputs — an attacker controls no stored preimage and
  learns nothing exploitable from index-lookup timing; the one equality on secret-derived
  data both sides control is the repl secret compare below, which is constant-time.
- **OIDC, static-JWKS only.** `App::with_oidc(OidcConfig { issuer, audience, jwks_json })`
  parses the JWKS once at mount — a bad key, unsupported `kty`/`alg`, or malformed document
  is a named mount-time panic (which key, what is wrong, the fix), never a request-time
  error. `verify` is pure over `(jwt, now)`: split the compact JWT (exactly three segments,
  else malformed), base64url-decode header/payload/signature, require header `alg == "RS256"`
  (v1 is RS256-only; `kid` selects the JWK when present, else all configured keys are tried),
  check RSA-PKCS1-v1.5-SHA256 over the `header.payload` signing input, then `iss`
  string-equals issuer, `aud` (string or array) contains audience, `exp > now - skew`,
  `nbf <= now + skew` (`JWT_CLOCK_SKEW_NANOS` = 60s; JWT `exp`/`nbf` seconds converted to the
  clock's nanos). Success yields an ephemeral `Principal { subject: sub, via: Oidc }` — no
  row, nothing to revoke but a short `exp`. Each failed check is its own named 401 (below).
  Verification is ~100µs of pure computation on the kernel thread; the test suite checks in a
  fixed RSA keypair and mints tokens in-test with the same crate, so the claim matrix replays
  byte-identically — proof the crypto path added no nondeterminism. Dynamic JWKS fetch (any
  outbound IO) waits for the external-IO surface; ES256 joins when someone needs it.
- **Expiry: lazy resolution is correctness, the sweep is hygiene.** A `Session` row with
  `expires_at <= now` is treated as absent at resolution — invisible the instant `now` passes
  it, restart or not (the same restart-stable `Clock` shape cron's catch-up already relies
  on). A built-in daily cron (`charpente.session_sweep`, registered platform-side with no
  opt-in, running through the ordinary cron/job machinery — retry and observability for free)
  deletes expired rows in one transaction per tick so the table doesn't grow unbounded.
  Because lazy resolution is authoritative, the sweep's cadence is not load-bearing; the tick
  threads its `now` into an enqueued `SessionSweep { before }` job (the cron enqueue closure
  was widened to see the scheduler's `now`), which deletes rows with `expires_at <= before`.
- **Replication peer secret.** `ReplConfig.peer_secret: Option<String>` closes the
  anyone-can-connect hole on the replication port (the AwaitHello-pins-GC / empty-`Frames`
  class, already hardened, now access-controlled). When `Some`, every inbound `Hello` and
  single-message control conn (`CtrlPromote`/`CtrlSwitchover`) must present a matching secret,
  checked *first* — before any position/pin/seed logic — and a mismatch is `Msg::Refuse` with
  the named text and close, zero state touched. The compare is constant-time over SHA-256
  digests of both sides (`ct_eq_hex(hash_token(a), hash_token(b))`), never a raw `==`. `None`
  preserves today's open behavior: a node enforces only its own config — a `Some` node refuses
  unproven inbound, a `None` node accepts everything and gracefully ignores a proof it never
  required (not an error). So a half-configured pair works in exactly one direction, which
  fixes the rollout order at **followers first, then the primary** (a ruling — configuring the
  primary first would lock every not-yet-configured follower out). The CLI verbs gain
  `--secret`; a `Refuse` maps to the existing exit-1-with-forwarded-text path.
- **Threat model, stated not implied.** The replication wire is plaintext TCP. `peer_secret`
  *authenticates* peers — it does not encrypt the link, so an eavesdropper on the path reads
  both the frames and the secret. The deployment assumption is a private network; transport
  encryption is an M2 conversation. This is documented in DESIGN and GUIDE, never left to
  inference.
- **The named 401 family (product surface, all pinned by tests against text).** No credential
  on an `auth` route, unrecognized bearer/cookie, expired session, the seven OIDC per-check
  rejections (bad signature, `exp`, `nbf`, wrong issuer, wrong audience, unsupported `alg`,
  malformed), the mount-time OIDC config errors, and the repl refusal each name the specific
  failure and its fix — precision over vagueness, since the leak profile is already acceptable
  (possession of an expired token proves past authorization; JWT claims are attacker-visible
  by construction). The exact strings live in the code and are pinned by the ladder/OIDC/repl
  test matrices; GUIDE §5 quotes the set byte-for-byte in its error-index voice.
- **Multi-tenancy (deferred to its own design).** Org/tenant scoping as a first-class key
  prefix — "every query is tenant-scoped" as a type-level property rather than a review
  checklist item — is a table-layer type-system feature, deferred to a design of its own; the
  session-subject pattern (and `emit_keyed`'s scoping key for realtime, §6) covers scoping
  until then, as the realtime demo shows. API-key scopes/permissions are subject-only in v1 —
  a permissions story earns its own design too.

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
