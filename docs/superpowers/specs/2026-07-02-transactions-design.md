# M0 step 4 — Transactions: design

Status: awaiting Noah's review. Scope decisions marked **[FLAGGED]** were made without
sign-off (Noah was away when asked); everything else follows DESIGN.md §2 directly.

## Goal

The typed transaction layer PLAN.md step 4 specifies: `fn(&mut Tx) -> Result<T, E>`,
reads from a snapshot, writes/enqueues/emits recorded into **one** WAL record, applied
to the store post-commit, serializable by construction on the single-threaded core.
This makes the platform own the wiring that `tests/typed_tables.rs` currently does by
hand, and makes the headline guarantee — a state change and its job enqueue commit
atomically or not at all — observable and crash-tested end-to-end.

## Scope

**In:** `Db` (composition of WAL + store + schema), `Schema` registry, `Tx` with
`get`/`scan`/`scan_by`/`put`/`delete`/`enqueue`/`emit`, the effects record format v0,
`TxError<E>`, minimal `Job`/`Event` traits, a built-in jobs table, a `View` read trait,
fallible `StateMachine::apply`, and the sim test suite for all of it.

**Out (deferred, each additive later):** `tx.now()` and `tx.entropy()` (wall-time
modeling is its own design conversation; nothing consumes them yet), id allocation
(`tx.allocate::<R>()` — add when the HTTP/demo steps need it; the counter travels in the
record, so it is additive), `#[job]`/`#[event]` macros (plain traits suffice until step
6), queue lease/dispatch (step 6), WAL compaction after checkpoint. **[FLAGGED]** —
this minimal scope was the recommended option in the unanswered scope question.

## Decisions

1. **Effects in the log, not commands.** Already decided by DESIGN §2 ("its effects are
   serialized as one record"). Command replay would require re-running the closure on
   recovery/followers, and a closure has no stable identity across binary versions.
2. **Build on `Engine`, generalized.** `StateMachine::apply` returns `io::Result<()>`
   (recovery can hit injected faults — the seed 0x5cafb3ac016dccf9 lesson) and
   `Wal::open`'s replay callback becomes fallible to carry it. The
   append→sync→apply→ack invariant keeps its single home. A rejected alternative — `Db`
   owning `Wal` + `Store` directly — would duplicate that sequence.
3. **Read-your-own-writes.** `tx.put` then `tx.get` sees the new row. Snapshot-only
   reads (writes invisible until commit) violate least surprise for app code; forbidding
   read-after-write is hostile to the target workloads. **[FLAGGED]**
4. **`TxError<E> { App(E), Io(io::Error) }`.** The platform must distinguish app
   rejection from platform failure (the future HTTP layer maps these to 4xx vs 5xx).
   `Tx` read methods return `io::Result`, so app error types convert with `From` as
   usual. **[FLAGGED]**
5. **Canonical collapsed records.** The record serializes the final overlay (per-table,
   per-id, last-write-wins), not every intermediate `put`. No observer can distinguish
   the two — effects apply atomically on a single-threaded core — and the canonical form
   is smaller and gives one source of truth inside `Tx`. Enqueues and emits are *not*
   collapsed: every call is a real effect, kept in call order.
6. **Jobs materialize minimally; emits do not.** Enqueues land in a built-in
   `charpente.jobs` table inside the same `Store::apply`, making enqueue atomicity
   crash-testable now and giving step 6 its substrate. Emits stay log-only — DESIGN §6:
   realtime subscribers tail the log. **[FLAGGED]**
7. **`View` trait for reads.** `get`/`scan`/`scan_by` on both `Snapshot` and `Tx`, so
   the `#[table]`-generated `by_<field>` accessors take `&impl View` and work in both
   contexts. One accessor surface instead of two.
8. **`put`/`delete`/`enqueue`/`emit` return `()`.** They buffer into the overlay and
   cannot fail. README's `tx.put(&inv)?` example gets updated in the implementation
   commit (hard rule 6). If unique-index validation lands later (M1), the signature
   revisits then.

## Architecture

```
charpente_core::db
├── Db            — owns Engine<TableMachine>; transact / snapshot / checkpoint
├── DbConfig      — WalConfig + store path
├── Schema        — table-name → TableOps vtable registry (built from Row types)
├── Tx            — owned Snapshot + overlay + enqueue/emit buffers
├── TxError<E>    — App(E) | Io(io::Error)
├── Job / Event   — v0 traits: Codec + const NAME
└── (private) TableMachine, Effect, record encode/decode
```

### Db

```rust
impl Db {
    /// Opens the store, then recovers the WAL, replaying every durable record
    /// with seq > store.applied_seq() through the schema into the store.
    pub fn open(disk: Box<dyn Disk>, cfg: DbConfig, schema: Schema) -> io::Result<Db>;

    /// &mut self: transactions are serializable because two cannot exist.
    pub fn transact<T, E>(
        &mut self,
        f: impl FnOnce(&mut Tx) -> Result<T, E>,
    ) -> Result<T, TxError<E>>;

    pub fn snapshot(&self) -> io::Result<Snapshot>;   // read-only queries
    pub fn checkpoint(&self) -> io::Result<()>;       // manual policy for now
}
```

Recovery asserts the negative space: the store's `applied_seq` must never exceed the
durable log's last seq ("the store must never outrun durable truth" — store applies
happen only after a successful WAL sync, and redb checkpoints only make synced-record
effects durable).

### Schema

`Schema::new().table::<R>()` registers a vtable per table:

```rust
struct TableOps {
    apply_put:    fn(&mut Writer<'_>, &[u8]) -> io::Result<()>, // decode R, w.put(&row)
    apply_delete: fn(&mut Writer<'_>, u64)   -> io::Result<()>, // w.delete::<R>(id)
}
```

Duplicate registration panics (contract violation). A record referencing an
unregistered table fails recovery with an error naming the table, the contract, and the
fix ("register it in the Schema passed to Db::open, or migrate the log").

### Tx

```rust
pub struct Tx {
    snapshot: Snapshot,
    // table name → id → Some(row bytes) | None (tombstone). BTreeMaps: canonical order.
    overlay: BTreeMap<&'static str, BTreeMap<u64, Option<Vec<u8>>>>,
    enqueues: Vec<(&'static str, Vec<u8>)>,   // call order
    emits: Vec<(&'static str, Vec<u8>)>,      // call order
}
```

- `get::<R>(id)` — overlay hit (tombstone → `None`, bytes → decode) else snapshot.
- `scan::<R>()` — snapshot scan merged with the table's overlay in id order.
- `scan_by::<R>(index, key)` — snapshot index scan minus overlaid ids, plus overlay
  rows whose computed index key (via `R::INDEXES`) matches, merged in id order.
- `put(&row)` / `delete::<R>(id)` — write the overlay.
- `enqueue(job)` / `emit(event)` — push the encoded payload.

### Job / Event (v0)

```rust
pub trait Job: Codec   { const NAME: &'static str; }
pub trait Event: Codec { const NAME: &'static str; }
```

Hand-implemented in tests for now; `#[job]`/`#[event]` derive them later.

## Data flow

**Commit:** `transact` takes a snapshot, runs `f`. On `Ok(t)` with a non-empty effect
set: encode the record → `engine.submit(record)` (append → sync → apply → ack) →
`Ok(t)`. Apply decodes the record and runs one `Store::apply(seq, ..)` that puts/deletes
rows through the vtables and inserts jobs rows — one redb write txn, atomic with the
`applied_seq` bump.

**Read-only:** empty effect set → no append, no seq consumed, `Ok(t)` directly.

**Abort:** `f` returns `Err(e)` → everything dropped → `Err(TxError::App(e))`.

**IO failure:** any failure in append/sync/apply → `Err(TxError::Io)` and the engine is
poisoned (a post-sync apply failure means machine state has diverged from durable truth;
a sync failure means durability is unknown — fsyncgate). The only exit is
restart-and-recover, which is idempotent. A txn that failed after sync is durable but
unacknowledged and may resurface after recovery — the same contract `Engine::submit`
documents today.

**Recovery:** open store → read `applied_seq` → `Wal::open` replays all records, the
`TableMachine` skips seq ≤ `applied_seq` and applies the rest. Recovery hitting an
injected fault fails `Db::open`; the workload restart-retries (established step-2
pattern).

## Record format v0

```
record := [n: u32 le] effect*
effect := 0x00 [table: String][row: Vec<u8>]      # put   (id is inside the row)
        | 0x01 [table: String][id: u64 le]        # delete
        | 0x02 [job: String][payload: Vec<u8>]    # enqueue
        | 0x03 [event: String][payload: Vec<u8>]  # emit
```

All via the existing `Codec` (length-prefixed strings/bytes, fixed-width ints). Order:
table effects in (table, id) order, then enqueues, then emits in call order. A record
must fit `MAX_RECORD_BYTES` (16 MiB) — asserted with a message naming the contract.
Unknown tag / trailing bytes → decode error (recovery refuses, names the site).

**Jobs table:** `charpente.jobs`, key `(seq: u64, idx: u32)`, value
`[name: String][payload: Vec<u8>]` — raw redb table owned by the store layer, not a
`#[table]`.

## Error surface (hard rule 5)

- Unregistered table in a record: names the table, the contract, the fix.
- Poisoned engine: names the cause (fsync-unknown durability vs apply divergence) and
  the fix (restart and recover).
- Oversized record: names the limit and the offending size.
- All tested against message text, as in the existing suites.

## Testing plan (sim-first, same PR — hard rule 3)

Lives in `crates/charpente/tests/transactions.rs` (needs the `#[table]` macro).

1. **Unit (FaultConfig::none):** RYW across put/get/overwrite/delete/scan/scan_by;
   abort discards effects and consumes no seq; read-only txn appends nothing;
   cross-table txn; record encode/decode roundtrip.
2. **Crash property (`run_universes!`):** rounds of recover → verify → random txns
   (mixed commit/abort, multi-table puts/deletes + enqueues) → random checkpoint →
   crash. Invariants: acked txns are a prefix of recovered; store ≡ fold(recovered
   effects); indexes consistent; **jobs table ≡ exactly the enqueues of recovered
   txns** — never a state change without its job or a job without its state change.
3. **Determinism:** same seed → same durable fingerprint (existing pattern).
4. **Negative-space meta-test:** a deliberately broken ordering (store apply *before*
   WAL sync) driven directly against `Wal` + `Store` must be caught by the harness as
   store-ahead-of-durable-truth / phantom state. Mirrors the step-1 naive-recovery
   meta-test. Any sim-found bug pins its seed in the regression suite.
5. **Error-quality tests** for the messages above.

Evidence bar: property suite green over fresh seeds every run (≥256 universes per test,
matching the existing suites); seed count stated in the commit message.

## Docs updated in the implementation commit (hard rule 6)

- DESIGN §2: record-format-v0 note for the effects encoding (frame format is already
  documented; this adds the payload layout).
- DESIGN §3: query API note — accessors take `&impl View`.
- README example: `put`/`enqueue`/`emit` without `?`; `get` returns `Option`.
- CLAUDE.md current-state: step 4 done + step 5 next.

## Rejected alternatives

- **Commands-in-log** — see Decisions §1.
- **`Db` bypassing `Engine`** — duplicates the core invariant's home.
- **Baking index deltas into the record** — replay would not need the schema registry,
  but records bloat and the log format leaks storage-layer detail; the registry is
  needed for decode anyway.
- **Snapshot-only reads / forbidding read-after-write** — see Decisions §3.
- **Materializing emits** — the log is the event feed by design (DESIGN §6).
