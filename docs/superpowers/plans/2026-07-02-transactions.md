# M0 Step 4 — Typed Transactions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `Db::transact(fn(&mut Tx) -> Result<T, E>)` — reads from a snapshot with read-your-own-writes, all writes/enqueues/emits committed as ONE durable WAL record, applied to the redb store post-sync, serializable by construction.

**Architecture:** A new `charpente_core::db` module composes the existing `Engine` (append→sync→apply→ack) with the existing `Store` via a `TableMachine` state machine that decodes effect records and applies them through a `Schema` name→vtable registry. `StateMachine::apply` becomes fallible first (recovery can hit injected faults); a `View` read trait unifies `Snapshot` and `Tx` so `#[table]`-generated accessors work in both.

**Tech Stack:** Rust, redb 4.1 (already a dep — no new dependencies), charpente-sim for all tests.

**Spec:** `docs/superpowers/specs/2026-07-02-transactions-design.md` (approved).

## Global Constraints

- **No ambient nondeterminism in core** — all IO through `Clock`/`Entropy`/`Disk`/`Net`; the clippy deny-list enforces this; no new dependencies are added by this plan.
- **Assertions TigerBeetle-style** — assert invariants and negative space; assertions stay on in release.
- **Error messages are product surface** — name the contract, the site, and the fix; tests assert on message text.
- **Sim-first** — every behavior lands with seeded fault-injection tests in the same task; a found bug pins its seed.
- **Docs in the same commit as API changes** (hard rule 6) — README/DESIGN edits are folded into the tasks below, not deferred.
- **Commit directly to `main`. No Co-Authored-By trailers** (CLAUDE.md).
- **Evidence bar:** property tests green over fresh seeds every run; state universe counts in commit messages.
- Run all commands from the repo root: `/Users/noahmitchem/Documents/GitHub/charpente`.

---

### Task 1: Fallible replay — `StateMachine::apply` returns `io::Result`, engine poisons on apply failure

The `TableMachine` (Task 3) applies records to redb, which can fail under injected faults. Recovery and submit must propagate that. A post-sync apply failure means machine state has diverged from durable truth — the engine must refuse further work (poison), mirroring the WAL's own fsyncgate stance.

**Files:**
- Modify: `crates/charpente-core/src/log.rs:58-62, 96-99` (fallible `on_record`)
- Modify: `crates/charpente-core/src/engine.rs` (trait signature, poison flag)
- Modify: `crates/charpente-sim/tests/wal.rs:28-42, 128-131, 317` (impl updates) + new tests
- Modify: `crates/charpente/tests/typed_tables.rs:257-263` (OpLog impl update)

**Interfaces:**
- Produces: `trait StateMachine { fn apply(&mut self, seq: u64, payload: &[u8]) -> io::Result<()>; }`
- Produces: `Wal::open(disk, cfg, on_record: impl FnMut(u64, &[u8]) -> io::Result<()>) -> io::Result<Wal>`
- Produces: `Engine::submit` — unchanged signature, but now poisons itself if apply fails after a successful sync; a poisoned engine refuses `submit` with a message containing `"restart and recover"`.

- [ ] **Step 1: Write the failing tests** — append to `crates/charpente-sim/tests/wal.rs` (they will not compile yet; a compile error is the red state for a signature change):

```rust
/// A machine that refuses one specific seq — the hook for proving the
/// engine's poison-on-apply-failure contract.
struct Balky {
    ledger: Ledger,
    refuse_seq: Option<u64>,
}

impl StateMachine for Balky {
    fn apply(&mut self, seq: u64, payload: &[u8]) -> io::Result<()> {
        if self.refuse_seq == Some(seq) {
            return Err(io::Error::other("injected apply failure"));
        }
        self.ledger.apply(seq, payload)
    }
}

#[test]
fn apply_failure_poisons_the_engine_until_recovery() {
    let sim = quiet_sim();
    let mut engine = Engine::recover(
        Box::new(sim.disk()),
        small_segments(),
        Balky { ledger: Ledger::default(), refuse_seq: Some(1) },
    )
    .unwrap();
    engine.submit(b"zero").unwrap();

    let err = engine.submit(b"one").unwrap_err();
    assert_eq!(err.to_string(), "injected apply failure");

    // Poisoned: even a healthy record must be refused now.
    let err = engine.submit(b"two").unwrap_err();
    assert!(err.to_string().contains("restart and recover"), "{err}");

    // The refused record was durable before apply failed: recovery (with a
    // machine that accepts it) surfaces it — durable but unacknowledged.
    let engine = Engine::recover(
        Box::new(sim.disk()),
        small_segments(),
        Balky { ledger: Ledger::default(), refuse_seq: None },
    )
    .unwrap();
    assert_eq!(
        engine.machine().ledger.records,
        vec![b"zero".to_vec(), b"one".to_vec()]
    );
}

#[test]
fn recovery_propagates_apply_failure() {
    let sim = quiet_sim();
    build_log(&sim, &[b"x"]);
    let err = Engine::recover(
        Box::new(sim.disk()),
        small_segments(),
        Balky { ledger: Ledger::default(), refuse_seq: Some(0) },
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "injected apply failure");
}
```

Also add `use std::io;` to the imports at the top of `wal.rs`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p charpente-sim --test wal 2>&1 | head -30`
Expected: compile error — `Balky`'s `apply` returns `io::Result<()>` but the trait expects `()`.

- [ ] **Step 3: Change the trait and engine** — `crates/charpente-core/src/engine.rs`, full new content of the changed items:

```rust
/// A deterministic function of the record stream. Implementations must not
/// touch anything outside their own state and the records handed to them.
/// An `Err` means the machine could not ingest a record the log already made
/// durable; the engine treats its state as diverged (see `submit`).
pub trait StateMachine {
    fn apply(&mut self, seq: u64, payload: &[u8]) -> io::Result<()>;
}

pub struct Engine<S: StateMachine> {
    wal: Wal,
    machine: S,
    last_applied: Option<u64>,
    poisoned: bool,
}

impl<S: StateMachine> Engine<S> {
    /// Recovers the log and replays every durable record into `machine`.
    /// Replay can fail (the machine's own IO, or the log's); recovery is
    /// idempotent — restart and retry.
    pub fn recover(disk: Box<dyn Disk>, cfg: WalConfig, mut machine: S) -> io::Result<Engine<S>> {
        let mut last_applied = None;
        let wal = Wal::open(disk, cfg, |seq, payload| {
            assert_eq!(
                seq,
                last_applied.map_or(0, |l: u64| l + 1),
                "replay skipped a seq — the log handed records out of order"
            );
            machine.apply(seq, payload)?;
            last_applied = Some(seq);
            Ok(())
        })?;
        Ok(Engine {
            wal,
            machine,
            last_applied,
            poisoned: false,
        })
    }

    /// Append → sync → apply → acknowledge. On `Ok`, the record is durable
    /// and applied; on `Err`, it is not acknowledged (it may still surface
    /// after a crash, like any unacknowledged write). If apply fails after a
    /// successful sync, machine state has diverged from durable truth and
    /// the engine poisons itself — the only exit is restart-and-recover.
    pub fn submit(&mut self, payload: &[u8]) -> io::Result<u64> {
        if self.poisoned {
            return Err(io::Error::other(
                "engine poisoned: a durable record failed to apply, so machine state no \
                 longer matches the log — restart and recover",
            ));
        }
        let seq = self.wal.append(payload)?;
        self.wal.sync()?;
        assert_eq!(
            seq,
            self.last_applied.map_or(0, |l| l + 1),
            "apply loop skipped a seq — append and apply have diverged"
        );
        if let Err(e) = self.machine.apply(seq, payload) {
            self.poisoned = true;
            return Err(e);
        }
        self.last_applied = Some(seq);
        Ok(seq)
    }

    pub fn machine(&self) -> &S {
        &self.machine
    }

    pub fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}
```

- [ ] **Step 4: Make `Wal::open`'s callback fallible** — `crates/charpente-core/src/log.rs`. Signature (line ~58):

```rust
    pub fn open(
        disk: Box<dyn Disk>,
        cfg: WalConfig,
        mut on_record: impl FnMut(u64, &[u8]) -> io::Result<()>,
    ) -> io::Result<Wal> {
```

and the call site inside the frame loop (line ~97):

```rust
                    Frame::Valid { payload } => {
                        on_record(expected_seq, &payload)?;
                        expected_seq += 1;
                        offset += HEADER_BYTES + payload.len() as u64;
                    }
```

Also update the doc comment on `open` from "replaying every recovered record into `on_record` in seq order" to note: "an `on_record` error aborts recovery".

- [ ] **Step 5: Update the existing `StateMachine` impls and closures**

`crates/charpente-sim/tests/wal.rs` — `Ledger::apply` gets the new signature and returns `Ok(())` after the existing body; the clean-replay loop (line ~129) becomes `replay.apply(seq as u64, record).unwrap();`; the `empty_records_are_a_contract_violation` test's `Wal::open(..., |_, _| {})` becomes `Wal::open(..., |_, _| Ok(()))`.

`crates/charpente/tests/typed_tables.rs` — `OpLog`:

```rust
impl StateMachine for OpLog {
    fn apply(&mut self, seq: u64, payload: &[u8]) -> io::Result<()> {
        assert_eq!(seq, self.ops.len() as u64);
        self.ops.push(payload.to_vec());
        Ok(())
    }
}
```

- [ ] **Step 6: Run the full suite**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets`
Expected: everything green including the two new tests; no clippy warnings.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "core+sim: fallible StateMachine::apply; engine poisons on post-sync apply failure

Groundwork for M0 step 4: the table machine applies records to redb, which
can fail under injected faults. Recovery propagates (restart-retry, the seed
0x5cafb3ac016dccf9 lesson); a post-sync apply failure diverges machine state
from durable truth, so the engine refuses further submits until recovery."
```

---

### Task 2: `View` read trait + built-in jobs table

**Files:**
- Modify: `crates/charpente-core/src/table.rs` (add `View` trait; move `Snapshot`'s three read methods into `impl View for Snapshot`; add `JOBS` table, `Writer::enqueue_raw`, `Snapshot::jobs`)
- Modify: `crates/charpente-macros/src/lib.rs:173-180` (accessors take `&impl View`)
- Modify: `crates/charpente/tests/typed_tables.rs:15` (import `View`) + one new test
- Modify: `DESIGN.md` §3 and §4 (same-commit doc updates)

**Interfaces:**
- Produces: `pub trait View { fn get<R: Row>(&self, id: Id<R>) -> io::Result<Option<R>>; fn scan<R: Row>(&self) -> io::Result<Vec<R>>; fn scan_by<R: Row>(&self, index_name: &str, key: &impl Codec) -> io::Result<Vec<R>>; }` — implemented by `Snapshot` here, by `Tx` in Task 3.
- Produces: `Writer::enqueue_raw(&mut self, seq: u64, idx: u32, name: &str, payload: &[u8]) -> io::Result<()>`
- Produces: `Snapshot::jobs(&self) -> io::Result<Vec<(u64, u32, String, Vec<u8>)>>` — in `(seq, idx)` order.
- Produces: generated `R::by_<field>(view: &impl View, key: &K) -> io::Result<Vec<R>>`

- [ ] **Step 1: Write the failing test** — append to `crates/charpente/tests/typed_tables.rs`:

```rust
#[test]
fn jobs_table_records_in_order() {
    let (_sim, store) = quiet_store();
    store
        .apply(0, |w| {
            w.enqueue_raw(0, 0, "send_receipt", b"invoice-10")?;
            w.enqueue_raw(0, 1, "audit", b"")
        })
        .unwrap();
    store.apply(1, |w| w.enqueue_raw(1, 0, "audit", b"x")).unwrap();

    let jobs = store.snapshot().unwrap().jobs().unwrap();
    assert_eq!(
        jobs,
        vec![
            (0, 0, "send_receipt".to_string(), b"invoice-10".to_vec()),
            (0, 1, "audit".to_string(), Vec::new()),
            (1, 0, "audit".to_string(), b"x".to_vec()),
        ]
    );
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p charpente --test typed_tables 2>&1 | head -20`
Expected: compile error — no method `enqueue_raw` on `Writer`.

- [ ] **Step 3: Add the `View` trait to `crates/charpente-core/src/table.rs`** — insert this section header block right before the `Snapshot` section, and change `impl Snapshot { ... }` (the block holding `get`/`scan`/`scan_by`) to `impl View for Snapshot { ... }` with the method bodies unchanged:

```rust
// ---------------------------------------------------------------- View

/// Read access to table state — implemented by `Snapshot` (reads outside
/// transactions) and `Tx` (reads inside one, which also see the
/// transaction's own writes). The `#[table]` accessors are generated
/// against this trait.
pub trait View {
    fn get<R: Row>(&self, id: Id<R>) -> io::Result<Option<R>>;

    /// Every row, in id order.
    fn scan<R: Row>(&self) -> io::Result<Vec<R>>;

    /// Exact-match secondary index lookup, in id order. Called through the
    /// typed accessors `#[table]` generates (`Invoice::by_org(...)`).
    fn scan_by<R: Row>(&self, index_name: &str, key: &impl Codec) -> io::Result<Vec<R>>;
}
```

(Move the `get`/`scan`/`scan_by` doc comments onto the trait; drop the `pub` on the moved methods — trait items are public through the trait.)

- [ ] **Step 4: Add the jobs table to `table.rs`** — next to the `META` definition:

```rust
/// Transactional enqueues, materialized in the same apply as the state
/// change they committed with (DESIGN.md §6). Key is (record seq, call
/// order); value is `[name][payload]` in the `Codec` length-prefix layout.
const JOBS: TableDefinition<(u64, u32), &[u8]> = TableDefinition::new("charpente.jobs");
```

`Writer` gains (after `delete`):

```rust
    /// Records a job enqueued by the record at `seq`, in call order via
    /// `idx`. Platform apply-path surface — applications enqueue through a
    /// transaction, never here.
    pub fn enqueue_raw(&mut self, seq: u64, idx: u32, name: &str, payload: &[u8]) -> io::Result<()> {
        let mut value = Vec::with_capacity(8 + name.len() + payload.len());
        (u32::try_from(name.len()).expect("job name exceeds u32 length")).encode(&mut value);
        value.extend_from_slice(name.as_bytes());
        (u32::try_from(payload.len()).expect("job payload exceeds u32 length")).encode(&mut value);
        value.extend_from_slice(payload);
        let mut jobs = self.txn.open_table(JOBS).map_err(redb_err)?;
        jobs.insert((seq, idx), value.as_slice()).map_err(redb_err)?;
        Ok(())
    }
```

`Snapshot` gains an inherent method (jobs are platform surface, not a `View` read):

```rust
impl Snapshot {
    /// Every job recorded by committed transactions, in (seq, idx) order:
    /// `(seq, idx, job name, payload)`. The queue dispatcher's substrate
    /// (PLAN.md M0 step 6); tests use it to observe enqueue atomicity.
    pub fn jobs(&self) -> io::Result<Vec<(u64, u32, String, Vec<u8>)>> {
        let table = match self.txn.open_table(JOBS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(redb_err(e)),
        };
        let mut out = Vec::new();
        for entry in table
            .range((0u64, 0u32)..=(u64::MAX, u32::MAX))
            .map_err(redb_err)?
        {
            let (key, value) = entry.map_err(redb_err)?;
            let (seq, idx) = key.value();
            let mut bytes = value.value();
            let name = String::decode(&mut bytes)?;
            let payload = Vec::<u8>::decode(&mut bytes)?;
            out.push((seq, idx, name, payload));
        }
        Ok(out)
    }
}
```

- [ ] **Step 5: Regenerate the accessor against `View`** — `crates/charpente-macros/src/lib.rs`, replace the generated `by_*` function (lines ~173-180) with:

```rust
                /// Exact-match lookup on the `#[index]` field, in id order.
                /// Works on any `View`: a `Snapshot` outside transactions or
                /// a `Tx` inside one.
                #vis fn #by_fn_idents(
                    view: &impl ::charpente_core::table::View,
                    key: &#index_field_types,
                ) -> ::std::io::Result<::std::vec::Vec<Self>> {
                    view.scan_by::<Self>(#index_names, key)
                }
```

- [ ] **Step 6: Fix imports in `crates/charpente/tests/typed_tables.rs`** — line 15 becomes:

```rust
use charpente_core::table::{Codec, Id, Snapshot, Store, View};
```

- [ ] **Step 7: Run the suite**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets`
Expected: all green including the new jobs test; no warnings.

- [ ] **Step 8: Update DESIGN.md in the same commit**

In §3, at the end of the **Query API** paragraph, append: `Read accessors are generated against the `View` trait — implemented by both `Snapshot` (outside transactions) and `Tx` (inside one, where reads also see the transaction's own writes).`

In §4, at the end of the **Table store (v0, implemented)** paragraph, append: `A built-in `charpente.jobs` table (key `(seq, idx)`, value `[name][payload]`) materializes transactional enqueues in the same apply — the queue dispatcher's substrate (§6).`

- [ ] **Step 9: Commit**

```bash
git add -A && git commit -m "core+macros: View read trait + built-in charpente.jobs table

by_<field> accessors are now generated against View so they work on both
Snapshot and (step 4) Tx. Enqueues materialize behind the store in the same
apply as the state change they commit with — the queue's substrate and the
observable half of the dual-write guarantee. DESIGN §3/§4 updated."
```

---

### Task 3: The `db` module — `Db`, `Schema`, `Tx`, effects record, and the typed transaction tests

One commit: the module is one coherent reviewable unit, but work through it in the TDD cycles below.

**Files:**
- Create: `crates/charpente-core/src/db.rs`
- Modify: `crates/charpente-core/src/lib.rs` (add `pub mod db;`)
- Modify: `crates/charpente/src/lib.rs` (re-exports)
- Create: `crates/charpente/tests/transactions.rs`
- Modify: `README.md:41-49` (example matches the real API), `DESIGN.md` §2 (record payload format)

**Interfaces:**
- Consumes: Task 1's fallible `StateMachine`, Task 2's `View`/`enqueue_raw`/`jobs`.
- Produces (all in `charpente_core::db`, re-exported from `charpente`):
  - `Db::open(disk: Box<dyn Disk>, cfg: DbConfig, schema: Schema) -> io::Result<Db>`
  - `Db::transact<T, E>(&mut self, f: impl FnOnce(&mut Tx) -> Result<T, E>) -> Result<T, TxError<E>>`
  - `Db::snapshot() -> io::Result<Snapshot>`, `Db::checkpoint() -> io::Result<()>`, `Db::last_applied() -> Option<u64>`
  - `DbConfig { wal: WalConfig, store_path: String }` (Default: `wal`, `"tables.redb"`)
  - `Schema::new().table::<R>()` (builder; panics on duplicate)
  - `Tx: View` plus `put<R: Row>(&mut self, &R)`, `delete<R: Row>(&mut self, Id<R>)`, `enqueue<J: Job>(&mut self, J)`, `emit<Ev: Event>(&mut self, Ev)` — all returning `()`
  - `TxError<E> { App(E), Io(io::Error) }` (`#[derive(Debug)]`)
  - `trait Job: Codec { const NAME: &'static str; }`, `trait Event: Codec { const NAME: &'static str; }`

- [ ] **Step 1: Create `crates/charpente-core/src/db.rs`** with the record codec and its unit tests first:

```rust
//! Typed transactions over the log (DESIGN.md §2, PLAN.md M0 step 4): a
//! transaction is `fn(&mut Tx) -> Result<T, E>` — reads come from a snapshot
//! plus the transaction's own writes; effects buffer and serialize as ONE
//! log record; the store applies them only after the record is durable.
//! Serializable by construction: `transact` takes `&mut Db`, so two
//! transactions cannot exist at once.
//!
//! Record payload v0: `[n: u32 le]` then n tagged effects —
//! `0x00 [table][row]` put, `0x01 [table][id: u64 le]` delete,
//! `0x02 [job][payload]` enqueue, `0x03 [event][payload]` emit — strings and
//! byte fields length-prefixed per the `Codec` layout. Table effects are the
//! transaction's collapsed overlay in (table, id) order: intermediate
//! overwrites inside a transaction are invisible to every observer and are
//! not logged. Enqueues and emits keep call order.

use std::collections::BTreeMap;
use std::io;

use crate::engine::{Engine, StateMachine};
use crate::io::Disk;
use crate::log::{MAX_RECORD_BYTES, WalConfig};
use crate::table::{Codec, Id, Row, Snapshot, Store, View, Writer};

// ---------------------------------------------------------------- config

#[derive(Clone, Debug)]
pub struct DbConfig {
    pub wal: WalConfig,
    /// The store's file name inside the `Disk` namespace.
    pub store_path: String,
}

impl Default for DbConfig {
    fn default() -> DbConfig {
        DbConfig {
            wal: WalConfig::default(),
            store_path: "tables.redb".to_string(),
        }
    }
}

// ---------------------------------------------------------------- traits

/// A queued unit of work (DESIGN.md §6). `tx.enqueue` records it atomically
/// with the transaction's state changes; dispatch arrives with M0 step 6.
/// v0: implemented by hand; `#[job]` will derive it.
pub trait Job: Codec {
    const NAME: &'static str;
}

/// A realtime event (DESIGN.md §6). Emits live in the log record itself —
/// subscribers tail the log; nothing is materialized in the store.
/// v0: implemented by hand; `#[event]` will derive it.
pub trait Event: Codec {
    const NAME: &'static str;
}

/// Why a transaction did not return `Ok`: the application's own rejection,
/// or a platform IO failure. The distinction is load-bearing — `App` means
/// nothing was written; `Io` after the WAL sync means the record is durable
/// but unacknowledged and will resurface on recovery.
#[derive(Debug)]
pub enum TxError<E> {
    App(E),
    Io(io::Error),
}

// ---------------------------------------------------------------- schema

struct TableOps {
    apply_put: fn(&mut Writer<'_>, &[u8]) -> io::Result<()>,
    apply_delete: fn(&mut Writer<'_>, u64) -> io::Result<()>,
}

fn apply_put<R: Row>(w: &mut Writer<'_>, row: &[u8]) -> io::Result<()> {
    w.put(&R::decode_row(row)?)
}

fn apply_delete<R: Row>(w: &mut Writer<'_>, id: u64) -> io::Result<()> {
    w.delete::<R>(Id::from_raw(id))
}

/// The set of tables a `Db` can replay log records into. Every `#[table]`
/// type the application writes must be registered — replay works on raw
/// record bytes and needs the typed codecs to maintain indexes.
#[derive(Default)]
pub struct Schema {
    tables: BTreeMap<&'static str, TableOps>,
}

impl Schema {
    pub fn new() -> Schema {
        Schema::default()
    }

    pub fn table<R: Row>(mut self) -> Schema {
        let prev = self.tables.insert(
            R::TABLE,
            TableOps {
                apply_put: apply_put::<R>,
                apply_delete: apply_delete::<R>,
            },
        );
        assert!(
            prev.is_none(),
            "Schema: table \"{}\" registered twice — register each #[table] type once",
            R::TABLE
        );
        self
    }

    fn ops(&self, table: &str) -> io::Result<&TableOps> {
        self.tables.get(table).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "log record references table \"{table}\" but the Schema does not include \
                     it — register it in the Schema passed to Db::open, or migrate the log"
                ),
            )
        })
    }
}

// ---------------------------------------------------------------- record

#[derive(Debug, PartialEq)]
enum Effect {
    Put { table: String, row: Vec<u8> },
    Delete { table: String, id: u64 },
    Enqueue { job: String, payload: Vec<u8> },
    Emit { event: String, payload: Vec<u8> },
}

/// A transaction's buffered effects: the collapsed per-table overlay
/// (`None` = tombstone) plus enqueues and emits in call order.
#[derive(Default)]
struct TxEffects {
    overlay: BTreeMap<&'static str, BTreeMap<u64, Option<Vec<u8>>>>,
    enqueues: Vec<(&'static str, Vec<u8>)>,
    emits: Vec<(&'static str, Vec<u8>)>,
}

impl TxEffects {
    fn is_empty(&self) -> bool {
        self.overlay.values().all(BTreeMap::is_empty)
            && self.enqueues.is_empty()
            && self.emits.is_empty()
    }
}

fn encode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    (u32::try_from(bytes.len()).expect("field exceeds u32 length")).encode(out);
    out.extend_from_slice(bytes);
}

fn encode_record(effects: &TxEffects) -> Vec<u8> {
    let n = effects.overlay.values().map(BTreeMap::len).sum::<usize>()
        + effects.enqueues.len()
        + effects.emits.len();
    let mut out = Vec::new();
    (u32::try_from(n).expect("transaction exceeds u32 effects")).encode(&mut out);
    for (table, entries) in &effects.overlay {
        for (id, entry) in entries {
            match entry {
                Some(row) => {
                    out.push(0);
                    encode_bytes(table.as_bytes(), &mut out);
                    encode_bytes(row, &mut out);
                }
                None => {
                    out.push(1);
                    encode_bytes(table.as_bytes(), &mut out);
                    id.encode(&mut out);
                }
            }
        }
    }
    for (job, payload) in &effects.enqueues {
        out.push(2);
        encode_bytes(job.as_bytes(), &mut out);
        encode_bytes(payload, &mut out);
    }
    for (event, payload) in &effects.emits {
        out.push(3);
        encode_bytes(event.as_bytes(), &mut out);
        encode_bytes(payload, &mut out);
    }
    out
}

fn decode_record(payload: &[u8]) -> io::Result<Vec<Effect>> {
    let input = &mut &payload[..];
    let n = u32::decode(input)?;
    let mut effects = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let tag = u8::decode(input)?;
        effects.push(match tag {
            0 => Effect::Put {
                table: String::decode(input)?,
                row: Vec::<u8>::decode(input)?,
            },
            1 => Effect::Delete {
                table: String::decode(input)?,
                id: u64::decode(input)?,
            },
            2 => Effect::Enqueue {
                job: String::decode(input)?,
                payload: Vec::<u8>::decode(input)?,
            },
            3 => Effect::Emit {
                event: String::decode(input)?,
                payload: Vec::<u8>::decode(input)?,
            },
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "record decode: unknown effect tag {other} — this log was written \
                         by a newer record format; upgrade the binary or restore the old one"
                    ),
                ));
            }
        });
    }
    if !input.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "record decode: {} trailing bytes after {n} effects — record corruption \
                 or format drift",
                input.len()
            ),
        ));
    }
    Ok(effects)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn effects_fixture() -> TxEffects {
        let mut effects = TxEffects::default();
        let invoices = effects.overlay.entry("invoice").or_default();
        invoices.insert(10, Some(vec![1, 2, 3]));
        invoices.insert(11, None);
        effects.overlay.entry("org").or_default().insert(1, Some(vec![9]));
        effects.enqueues.push(("send_receipt", vec![10, 0]));
        effects.emits.push(("invoice_finalized", vec![7]));
        effects
    }

    #[test]
    fn record_roundtrip_is_canonical() {
        let encoded = encode_record(&effects_fixture());
        assert_eq!(
            decode_record(&encoded).unwrap(),
            vec![
                Effect::Put { table: "invoice".into(), row: vec![1, 2, 3] },
                Effect::Delete { table: "invoice".into(), id: 11 },
                Effect::Put { table: "org".into(), row: vec![9] },
                Effect::Enqueue { job: "send_receipt".into(), payload: vec![10, 0] },
                Effect::Emit { event: "invoice_finalized".into(), payload: vec![7] },
            ]
        );
    }

    #[test]
    fn decode_refuses_unknown_tags() {
        let mut out = Vec::new();
        1u32.encode(&mut out);
        out.push(9);
        let err = decode_record(&out).unwrap_err();
        assert!(err.to_string().contains("unknown effect tag 9"), "{err}");
    }

    #[test]
    fn decode_refuses_trailing_bytes() {
        let mut encoded = encode_record(&effects_fixture());
        encoded.push(0xAA);
        let err = decode_record(&encoded).unwrap_err();
        assert!(err.to_string().contains("trailing bytes"), "{err}");
    }

    #[test]
    fn empty_effects_know_they_are_empty() {
        assert!(TxEffects::default().is_empty());
        assert!(!effects_fixture().is_empty());
    }
}
```

Add `pub mod db;` to `crates/charpente-core/src/lib.rs` (alphabetical: after `crc`, before `engine`).

- [ ] **Step 2: Run the codec tests** (the rest of the module doesn't exist yet — `Tx`, `Db` come next; the file compiles because everything above is self-contained)

Run: `cargo test -p charpente-core db::`
Expected: 4 passed.

- [ ] **Step 3: Add `Tx` and its `View` impl to `db.rs`** (after the record section):

```rust
// ---------------------------------------------------------------- tx

/// The transaction handle: reads see the snapshot plus this transaction's
/// own writes; writes, enqueues, and emits buffer until commit. Buffering
/// cannot fail, so the write methods return nothing.
pub struct Tx {
    snapshot: Snapshot,
    effects: TxEffects,
}

impl Tx {
    pub fn put<R: Row>(&mut self, row: &R) {
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        self.effects
            .overlay
            .entry(R::TABLE)
            .or_default()
            .insert(row.id().raw(), Some(bytes));
    }

    pub fn delete<R: Row>(&mut self, id: Id<R>) {
        self.effects
            .overlay
            .entry(R::TABLE)
            .or_default()
            .insert(id.raw(), None);
    }

    pub fn enqueue<J: Job>(&mut self, job: J) {
        let mut payload = Vec::new();
        job.encode(&mut payload);
        self.effects.enqueues.push((J::NAME, payload));
    }

    pub fn emit<Ev: Event>(&mut self, event: Ev) {
        let mut payload = Vec::new();
        event.encode(&mut payload);
        self.effects.emits.push((Ev::NAME, payload));
    }
}

impl View for Tx {
    fn get<R: Row>(&self, id: Id<R>) -> io::Result<Option<R>> {
        match self
            .effects
            .overlay
            .get(R::TABLE)
            .and_then(|t| t.get(&id.raw()))
        {
            Some(Some(bytes)) => Ok(Some(R::decode_row(bytes)?)),
            Some(None) => Ok(None),
            None => self.snapshot.get(id),
        }
    }

    fn scan<R: Row>(&self) -> io::Result<Vec<R>> {
        let mut merged: BTreeMap<u64, R> = BTreeMap::new();
        for row in self.snapshot.scan::<R>()? {
            merged.insert(row.id().raw(), row);
        }
        if let Some(overlay) = self.effects.overlay.get(R::TABLE) {
            for (&id, entry) in overlay {
                match entry {
                    Some(bytes) => {
                        merged.insert(id, R::decode_row(bytes)?);
                    }
                    None => {
                        merged.remove(&id);
                    }
                }
            }
        }
        Ok(merged.into_values().collect())
    }

    fn scan_by<R: Row>(&self, index_name: &str, key: &impl Codec) -> io::Result<Vec<R>> {
        let index = R::INDEXES
            .iter()
            .find(|i| i.name == index_name)
            .unwrap_or_else(|| panic!("no index named \"{index_name}\" on table {}", R::TABLE));
        let mut wanted = Vec::new();
        key.encode(&mut wanted);

        let mut merged: BTreeMap<u64, R> = BTreeMap::new();
        for row in self.snapshot.scan_by::<R>(index_name, key)? {
            merged.insert(row.id().raw(), row);
        }
        if let Some(overlay) = self.effects.overlay.get(R::TABLE) {
            for (&id, entry) in overlay {
                // The overlay overrides the snapshot for this id either way:
                // a tombstone, or a row whose key may no longer match.
                merged.remove(&id);
                if let Some(bytes) = entry {
                    let row = R::decode_row(bytes)?;
                    let mut row_key = Vec::new();
                    (index.key)(&row, &mut row_key);
                    if row_key == wanted {
                        merged.insert(id, row);
                    }
                }
            }
        }
        Ok(merged.into_values().collect())
    }
}
```

- [ ] **Step 4: Add `TableMachine` and `Db` to `db.rs`** (after the `Tx` section):

```rust
// ---------------------------------------------------------------- machine

/// The state machine the engine drives: decodes effect records and applies
/// them to the store through the schema's vtables. Records at or below the
/// store's `applied_seq`-at-open are skipped during recovery replay.
struct TableMachine {
    store: Store,
    schema: Schema,
    skip_through: Option<u64>,
}

impl StateMachine for TableMachine {
    fn apply(&mut self, seq: u64, payload: &[u8]) -> io::Result<()> {
        if self.skip_through.is_some_and(|s| seq <= s) {
            return Ok(());
        }
        let effects = decode_record(payload)?;
        self.store.apply(seq, |w| {
            let mut job_idx = 0u32;
            for effect in &effects {
                match effect {
                    Effect::Put { table, row } => (self.schema.ops(table)?.apply_put)(w, row)?,
                    Effect::Delete { table, id } => {
                        (self.schema.ops(table)?.apply_delete)(w, *id)?
                    }
                    Effect::Enqueue { job, payload } => {
                        w.enqueue_raw(seq, job_idx, job, payload)?;
                        job_idx += 1;
                    }
                    Effect::Emit { .. } => {} // the log itself is the event feed
                }
            }
            Ok(())
        })
    }
}

// ---------------------------------------------------------------- db

/// The composed deterministic core: WAL + store + schema. One `Db` is the
/// single writer; `transact` takes `&mut self`, so transactions are
/// serializable because two cannot exist at once.
pub struct Db {
    engine: Engine<TableMachine>,
}

impl Db {
    /// Opens the store, then recovers the WAL, replaying every durable
    /// record past the store's checkpoint into it. Recovery can fail under
    /// IO faults; it is idempotent — restart and retry.
    pub fn open(disk: Box<dyn Disk>, cfg: DbConfig, schema: Schema) -> io::Result<Db> {
        let store = Store::open(&*disk, &cfg.store_path)?;
        let skip_through = store.applied_seq()?;
        let machine = TableMachine {
            store,
            schema,
            skip_through,
        };
        let engine = Engine::recover(disk, cfg.wal, machine)?;
        if let Some(applied) = skip_through {
            let durable = engine.last_applied();
            assert!(
                durable.is_some_and(|d| d >= applied),
                "store applied_seq {applied} is ahead of the durable log end {durable:?} — \
                 the store outran durable truth, which append→sync→apply forbids"
            );
        }
        Ok(Db { engine })
    }

    /// Runs one transaction: `f` reads through `tx` and buffers effects; on
    /// `Ok` the effects commit as one durable log record and apply to the
    /// store; on `Err` nothing happened at all. A transaction with no
    /// effects writes nothing and consumes no seq.
    pub fn transact<T, E>(
        &mut self,
        f: impl FnOnce(&mut Tx) -> Result<T, E>,
    ) -> Result<T, TxError<E>> {
        let snapshot = self.engine.machine().store.snapshot().map_err(TxError::Io)?;
        let mut tx = Tx {
            snapshot,
            effects: TxEffects::default(),
        };
        let out = f(&mut tx).map_err(TxError::App)?;
        if tx.effects.is_empty() {
            return Ok(out);
        }
        let record = encode_record(&tx.effects);
        assert!(
            record.len() <= MAX_RECORD_BYTES,
            "transaction effects are {} bytes but one log record holds at most \
             {MAX_RECORD_BYTES} — split the work into smaller transactions",
            record.len()
        );
        self.engine.submit(&record).map_err(TxError::Io)?;
        Ok(out)
    }

    /// A consistent read-only view for queries outside transactions.
    pub fn snapshot(&self) -> io::Result<Snapshot> {
        self.engine.machine().store.snapshot()
    }

    /// Durably persists the store, bounding recovery replay at this seq.
    pub fn checkpoint(&self) -> io::Result<()> {
        self.engine.machine().store.checkpoint()
    }

    pub fn last_applied(&self) -> Option<u64> {
        self.engine.last_applied()
    }
}
```

Replace `crates/charpente/src/lib.rs` with:

```rust
//! The platform surface applications link against: typed tables, routes,
//! jobs, events, config, telemetry, and the HTTP edge (DESIGN.md §3, §5–§6).
//! Re-exports the deterministic core and the derive macros.

pub use charpente_core as core;
pub use charpente_core::db::{Db, DbConfig, Event, Job, Schema, Tx, TxError};
pub use charpente_core::table::{Id, Snapshot, View};
pub use charpente_macros::table;

pub mod prod;
```

- [ ] **Step 5: Verify it all compiles**

Run: `cargo build --workspace && cargo test -p charpente-core db::`
Expected: clean build, 4 codec tests still pass.

- [ ] **Step 6: Write the integration tests** — create `crates/charpente/tests/transactions.rs`:

```rust
//! Typed transactions (PLAN.md M0 step 4): read-your-own-writes, atomic
//! effects records, abort semantics, recovery replay, and the contract
//! violations — unit tests first; the crash-property suite follows in the
//! same file (added by the next plan task).

use std::io;

use charpente::{Db, DbConfig, Event, Job, Schema, TxError, View, table};
use charpente_core::table::{Codec, Id};
use charpente_sim::{FaultConfig, Sim};

#[table]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table]
struct Invoice {
    id: Id<Invoice>,
    #[index]
    org: Id<Org>,
    amount_cents: i64,
    paid: bool,
}

struct SendReceipt {
    invoice: u64,
}

impl Codec for SendReceipt {
    fn encode(&self, out: &mut Vec<u8>) {
        self.invoice.encode(out);
    }
    fn decode(input: &mut &[u8]) -> io::Result<SendReceipt> {
        Ok(SendReceipt {
            invoice: u64::decode(input)?,
        })
    }
}

impl Job for SendReceipt {
    const NAME: &'static str = "send_receipt";
}

struct InvoiceFinalized {
    invoice: u64,
}

impl Codec for InvoiceFinalized {
    fn encode(&self, out: &mut Vec<u8>) {
        self.invoice.encode(out);
    }
    fn decode(input: &mut &[u8]) -> io::Result<InvoiceFinalized> {
        Ok(InvoiceFinalized {
            invoice: u64::decode(input)?,
        })
    }
}

impl Event for InvoiceFinalized {
    const NAME: &'static str = "invoice_finalized";
}

fn invoice(id: u64, org: u64, amount_cents: i64) -> Invoice {
    Invoice {
        id: Id::from_raw(id),
        org: Id::from_raw(org),
        amount_cents,
        paid: false,
    }
}

fn ids(rows: &[Invoice]) -> Vec<u64> {
    rows.iter().map(|r| r.id.raw()).collect()
}

fn app_schema() -> Schema {
    Schema::new().table::<Org>().table::<Invoice>()
}

fn quiet_db(seed: u64) -> (Sim, Db) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let db = Db::open(Box::new(sim.disk()), DbConfig::default(), app_schema()).unwrap();
    (sim, db)
}

#[test]
fn reads_see_own_writes_and_commit_applies() {
    let (_sim, mut db) = quiet_db(0x51E4);
    let total = db
        .transact(|tx| {
            tx.put(&Org {
                id: Id::from_raw(1),
                name: "acme".into(),
            });
            tx.put(&invoice(10, 1, 5000));
            tx.put(&invoice(11, 1, 200));
            tx.delete::<Invoice>(Id::from_raw(11));
            let mine = tx.scan::<Invoice>()?;
            assert_eq!(ids(&mine), vec![10], "own put and own delete both visible");
            assert_eq!(
                tx.get::<Invoice>(Id::from_raw(10))?.expect("own put").amount_cents,
                5000
            );
            assert!(tx.get::<Invoice>(Id::from_raw(11))?.is_none(), "own tombstone");
            Ok::<i64, io::Error>(mine.iter().map(|i| i.amount_cents).sum())
        })
        .unwrap();
    assert_eq!(total, 5000);

    let snap = db.snapshot().unwrap();
    assert_eq!(snap.scan::<Invoice>().unwrap().len(), 1);
    assert_eq!(snap.scan::<Org>().unwrap().len(), 1);
    assert_eq!(db.last_applied(), Some(0), "one transaction, one record");
}

#[test]
fn index_reads_see_own_writes() {
    let (_sim, mut db) = quiet_db(0x51E5);
    let (acme, globex) = (Id::<Org>::from_raw(1), Id::<Org>::from_raw(2));
    db.transact(|tx| {
        tx.put(&invoice(10, 1, 100));
        tx.put(&invoice(11, 2, 200));
        Ok::<_, io::Error>(())
    })
    .unwrap();

    db.transact(|tx| {
        // Move 11 to acme, add 12 at globex, drop 10 — all inside the txn.
        let mut moved = tx.get::<Invoice>(Id::from_raw(11))?.expect("committed");
        moved.org = acme;
        tx.put(&moved);
        tx.put(&invoice(12, 2, 300));
        tx.delete::<Invoice>(Id::from_raw(10));

        assert_eq!(ids(&Invoice::by_org(tx, &acme)?), vec![11]);
        assert_eq!(ids(&Invoice::by_org(tx, &globex)?), vec![12]);
        Ok::<_, io::Error>(())
    })
    .unwrap();

    // And the committed state agrees.
    let snap = db.snapshot().unwrap();
    assert_eq!(ids(&Invoice::by_org(&snap, &acme).unwrap()), vec![11]);
    assert_eq!(ids(&Invoice::by_org(&snap, &globex).unwrap()), vec![12]);
}

#[test]
fn abort_discards_everything_and_consumes_no_seq() {
    let (_sim, mut db) = quiet_db(0xAB07);
    db.transact(|tx| {
        tx.put(&invoice(1, 1, 1));
        Ok::<_, io::Error>(())
    })
    .unwrap();

    let err = db
        .transact(|tx| -> Result<(), &str> {
            tx.put(&invoice(2, 1, 2));
            tx.enqueue(SendReceipt { invoice: 2 });
            Err("changed my mind")
        })
        .unwrap_err();
    assert!(matches!(err, TxError::App("changed my mind")));

    assert_eq!(db.last_applied(), Some(0), "abort consumed no seq");
    let snap = db.snapshot().unwrap();
    assert_eq!(snap.scan::<Invoice>().unwrap().len(), 1);
    assert!(snap.jobs().unwrap().is_empty(), "aborted enqueue left no trace");
}

#[test]
fn read_only_transactions_write_nothing() {
    let (_sim, mut db) = quiet_db(0x2EAD);
    db.transact(|tx| {
        tx.put(&invoice(1, 1, 7));
        Ok::<_, io::Error>(())
    })
    .unwrap();

    let amount = db
        .transact(|tx| {
            Ok::<_, io::Error>(tx.get::<Invoice>(Id::from_raw(1))?.expect("row").amount_cents)
        })
        .unwrap();
    assert_eq!(amount, 7);
    assert_eq!(db.last_applied(), Some(0), "read-only txn appended nothing");
}

#[test]
fn effects_commit_as_one_record_and_recovery_replays_them() {
    let sim = Sim::with_faults(0x0DE1, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), app_schema()).unwrap();
    db.transact(|tx| {
        tx.put(&invoice(10, 1, 5000));
        tx.enqueue(SendReceipt { invoice: 10 });
        tx.emit(InvoiceFinalized { invoice: 10 });
        Ok::<_, io::Error>(())
    })
    .unwrap();

    // No checkpoint: crash with everything open. The store reverts to
    // nothing; the WAL record must rebuild row AND job together.
    sim.restart();
    drop(db);

    let db = Db::open(Box::new(sim.disk()), DbConfig::default(), app_schema()).unwrap();
    let snap = db.snapshot().unwrap();
    assert_eq!(snap.scan::<Invoice>().unwrap().len(), 1, "replay rebuilt the row");
    let jobs = snap.jobs().unwrap();
    assert_eq!(jobs.len(), 1, "replay rebuilt the job with the row");
    let (seq, idx, name, payload) = &jobs[0];
    assert_eq!((*seq, *idx, name.as_str()), (0, 0, "send_receipt"));
    assert_eq!(
        SendReceipt::decode(&mut payload.as_slice()).unwrap().invoice,
        10
    );
    assert_eq!(db.last_applied(), Some(0));
}

#[test]
fn recovery_names_unregistered_tables() {
    let sim = Sim::with_faults(0x5C4E, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), app_schema()).unwrap();
    db.transact(|tx| {
        tx.put(&invoice(1, 1, 1));
        Ok::<_, io::Error>(())
    })
    .unwrap();
    sim.restart();
    drop(db);

    let err = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Org>(), // Invoice missing
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("table \"Invoice\"") && msg.contains("register it in the Schema"),
        "{msg}"
    );
}

#[test]
#[should_panic(expected = "registered twice")]
fn duplicate_schema_registration_is_a_contract_violation() {
    let _ = Schema::new().table::<Invoice>().table::<Invoice>();
}

#[table]
struct Blob {
    id: Id<Blob>,
    data: Vec<u8>,
}

#[test]
#[should_panic(expected = "one log record holds at most")]
fn oversized_transactions_are_a_contract_violation() {
    let sim = Sim::with_faults(0xB16, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Blob>(),
    )
    .unwrap();
    let _ = db.transact(|tx| {
        tx.put(&Blob {
            id: Id::from_raw(1),
            data: vec![0; 17 << 20],
        });
        Ok::<_, io::Error>(())
    });
}
```

- [ ] **Step 7: Run the new tests**

Run: `cargo test -p charpente --test transactions`
Expected: 8 passed.

- [ ] **Step 8: Update README.md and DESIGN.md in the same commit**

README.md — replace the route example body (lines 42-49) with the real API:

```rust
#[route(POST "/invoices/:id/finalize")]
fn finalize(tx: &mut Tx, id: Id<Invoice>) -> Result<Invoice, ApiError> {
    let mut inv = tx.get(id)?.ok_or(ApiError::NotFound)?; // typed row, no SQL string
    inv.state = InvoiceState::Final;
    tx.put(&inv);
    tx.enqueue(SendReceipt { invoice: id });   // job enqueued IN the same transaction
    tx.emit(InvoiceFinalized { invoice: id }); // realtime event, same transaction
    Ok(inv)
}
```

DESIGN.md §2 — after the **Log format (v0, implemented)** paragraph, add:

```markdown
**Record payload (v0, implemented).** A committed transaction's effects:
`[n: u32]` then n tagged effects — put `0x00 [table][row]`, delete
`0x01 [table][id]`, enqueue `0x02 [job][payload]`, emit `0x03 [event][payload]`,
strings and byte fields length-prefixed per the row codec. Table effects are
the transaction's collapsed write set in (table, id) order — intermediate
overwrites inside a transaction are observable by no one and are not logged;
enqueues and emits keep call order. Replay resolves table names through the
`Schema` registry passed to `Db::open`.
```

- [ ] **Step 9: Full suite + commit**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets`
Expected: all green, no warnings.

```bash
git add -A && git commit -m "core: M0 step 4 — typed transactions: Db/Schema/Tx, one effects record per commit

fn(&mut Tx) -> Result<T, E>: reads via View (snapshot + read-your-own-writes
overlay), put/delete/enqueue/emit buffered and committed as ONE WAL record,
applied to the store through the Schema registry after the sync. TxError
keeps app rejection distinct from platform failure. Read-only txns consume
no seq; aborts leave no trace. README example + DESIGN §2 record format
updated (hard rule 6)."
```

---

### Task 4: Crash property + determinism tests

**Files:**
- Modify: `crates/charpente/tests/transactions.rs` (append the property suite)

**Interfaces:**
- Consumes: everything from Task 3; `Sim::crash_disk_after`, `run_universes!`, `Snapshot::jobs`.
- Produces: `tx_workload(sim: &mut Sim) -> Result<(), String>` (Task 5's meta-test lives beside it).

The oracle trick: transaction *content* is a pure function of its index `i`, so the model over `0..n` is reconstructible no matter which prefix survived a crash. Every transaction commits exactly one `Audit` job at `idx 0`, so after `n` transactions the jobs table must be exactly `[(0,0,audit,0), (1,0,audit,1), …, (n-1,0,audit,n-1)]` — a torn dual-write shows up as an immediate mismatch in either direction.

- [ ] **Step 1: Append the property suite to `crates/charpente/tests/transactions.rs`**

Add to the imports at the top of the file:

```rust
use std::collections::BTreeMap;

use charpente_core::io::Entropy;
use charpente_core::log::WalConfig;
use charpente_core::table::Snapshot;
use charpente_sim::run_universes;
```

Then append:

```rust
// ---------------------------------------------------- crash property

#[table]
struct Account {
    id: Id<Account>,
    #[index]
    bucket: u8,
    balance: i64,
}

#[table]
struct EntryRow {
    id: Id<EntryRow>,
    account: Id<Account>,
    amount: i64,
}

struct Audit {
    txn: u64,
}

impl Codec for Audit {
    fn encode(&self, out: &mut Vec<u8>) {
        self.txn.encode(out);
    }
    fn decode(input: &mut &[u8]) -> io::Result<Audit> {
        Ok(Audit {
            txn: u64::decode(input)?,
        })
    }
}

impl Job for Audit {
    const NAME: &'static str = "audit";
}

fn property_schema() -> Schema {
    Schema::new().table::<Account>().table::<EntryRow>()
}

/// Transaction number `i`, derived from `i` alone so the oracle can
/// reconstruct any committed prefix: an account balance bump (read-modify-
/// write), a ledger entry keyed by `i`, sometimes a delete of an older
/// entry, and always exactly one audit job carrying `i`.
fn txn_shape(i: u64) -> (u64, i64, Option<u64>) {
    let account = i % 8;
    let amount = ((i as i64) * 7) % 1000;
    let delete_entry = if i > 0 && i.is_multiple_of(3) {
        Some(i / 2)
    } else {
        None
    };
    (account, amount, delete_entry)
}

fn apply_txn(tx: &mut charpente::Tx, i: u64) -> io::Result<()> {
    let (account, amount, delete_entry) = txn_shape(i);
    let balance = tx
        .get::<Account>(Id::from_raw(account))?
        .map_or(0, |a| a.balance);
    tx.put(&Account {
        id: Id::from_raw(account),
        bucket: (account % 4) as u8,
        balance: balance + amount,
    });
    tx.put(&EntryRow {
        id: Id::from_raw(i),
        account: Id::from_raw(account),
        amount,
    });
    if let Some(e) = delete_entry {
        tx.delete::<EntryRow>(Id::from_raw(e));
    }
    tx.enqueue(Audit { txn: i });
    Ok(())
}

struct Model {
    accounts: BTreeMap<u64, i64>,
    entries: BTreeMap<u64, u64>, // entry id -> account id
}

fn model_after(n: u64) -> Model {
    let mut model = Model {
        accounts: BTreeMap::new(),
        entries: BTreeMap::new(),
    };
    for i in 0..n {
        let (account, amount, delete_entry) = txn_shape(i);
        *model.accounts.entry(account).or_insert(0) += amount;
        model.entries.insert(i, account);
        if let Some(e) = delete_entry {
            model.entries.remove(&e);
        }
    }
    model
}

fn db_matches_model(snap: &Snapshot, n: u64) -> Result<(), String> {
    let model = model_after(n);

    let accounts = snap.scan::<Account>().map_err(|e| format!("scan: {e}"))?;
    let got: BTreeMap<u64, i64> = accounts.iter().map(|a| (a.id.raw(), a.balance)).collect();
    if got != model.accounts {
        return Err(format!(
            "accounts diverge from the model after {n} txns: {got:?} != {:?}",
            model.accounts
        ));
    }

    let entries = snap.scan::<EntryRow>().map_err(|e| format!("scan: {e}"))?;
    let got: BTreeMap<u64, u64> = entries.iter().map(|e| (e.id.raw(), e.account.raw())).collect();
    if got != model.entries {
        return Err(format!(
            "entries diverge from the model after {n} txns: {got:?} != {:?}",
            model.entries
        ));
    }

    for bucket in 0..4u8 {
        let by_index = Account::by_bucket(snap, &bucket).map_err(|e| format!("index: {e}"))?;
        let got: Vec<u64> = by_index.iter().map(|a| a.id.raw()).collect();
        let expected: Vec<u64> = model
            .accounts
            .keys()
            .copied()
            .filter(|a| (a % 4) as u8 == bucket)
            .collect();
        if got != expected {
            return Err(format!("index bucket {bucket}: {got:?} != model {expected:?}"));
        }
    }

    // The dual-write invariant: exactly one audit job per committed txn,
    // never a state change without its job or a job without its state.
    let jobs = snap.jobs().map_err(|e| format!("jobs: {e}"))?;
    if jobs.len() as u64 != n {
        return Err(format!(
            "dual-write torn: {n} committed txns but {} jobs",
            jobs.len()
        ));
    }
    for (i, (seq, idx, name, payload)) in jobs.iter().enumerate() {
        let audit = Audit::decode(&mut payload.as_slice()).map_err(|e| format!("job decode: {e}"))?;
        if (*seq, *idx, name.as_str(), audit.txn) != (i as u64, 0, "audit", i as u64) {
            return Err(format!(
                "job {i} diverges: seq {seq} idx {idx} name {name} txn {}",
                audit.txn
            ));
        }
    }
    Ok(())
}

/// One universe: recover → verify against the oracle → mixed commits and
/// aborts → random checkpoint → crash, everything seeded.
fn tx_workload(sim: &mut Sim) -> Result<(), String> {
    let entropy = sim.entropy();
    let cfg = DbConfig {
        wal: WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 4096,
        },
        store_path: "tables.redb".to_string(),
    };
    let mut committed = 0u64; // acknowledged transactions

    for _round in 0..3 {
        sim.crash_disk_after(64 + entropy.next_u64() % 512);
        let mut db = match Db::open(Box::new(sim.disk()), cfg.clone(), property_schema()) {
            Ok(db) => db,
            Err(_) => {
                sim.restart();
                continue;
            }
        };

        let recovered = db.last_applied().map_or(0, |s| s + 1);
        if recovered < committed {
            return Err(format!(
                "durability violated: {committed} txns acknowledged, {recovered} recovered"
            ));
        }
        committed = recovered; // unacked survivors are history now
        match db.snapshot() {
            Ok(snap) => db_matches_model(&snap, committed)?,
            Err(_) => {
                sim.restart();
                continue;
            }
        }

        for _ in 0..1 + entropy.next_u64() % 8 {
            let i = committed;
            if entropy.next_u64().is_multiple_of(5) {
                // An abort must leave no trace and consume no seq.
                let before = db.last_applied();
                match db.transact(|tx| -> Result<(), &str> {
                    let _ = apply_txn(tx, i);
                    Err("aborted on purpose")
                }) {
                    Err(TxError::App(_)) => {}
                    Err(TxError::Io(_)) => break, // snapshot failed: died here
                    Ok(()) => unreachable!("the closure always aborts"),
                }
                if db.last_applied() != before {
                    return Err("aborted transaction consumed a seq".to_string());
                }
            }
            match db.transact(|tx| apply_txn(tx, i)) {
                Ok(()) => committed += 1,
                Err(TxError::App(_)) => unreachable!("apply_txn never rejects"),
                Err(TxError::Io(_)) => break, // died mid-commit; durable or not
            }
            if entropy.next_u64().is_multiple_of(4) && db.checkpoint().is_err() {
                break;
            }
        }
        sim.restart();
    }
    Ok(())
}

#[test]
fn transactions_survive_the_fault_matrix() {
    run_universes!(256, tx_workload);
}

#[test]
fn transactions_replay_identically_for_the_same_seed() {
    let trace = |seed: u64| {
        let mut sim = Sim::new(seed);
        let result = tx_workload(&mut sim);
        assert_eq!(result, Ok(()), "seed {seed:#x}");
        (sim.durable_fingerprint(), sim.now())
    };
    for seed in [0x42, 0xDEAD_BEEF, 0x0BAD_5EED_0BAD_5EED] {
        assert_eq!(trace(seed), trace(seed));
    }
}
```

- [ ] **Step 2: Run the property tests repeatedly**

Run: `cargo test -p charpente --test transactions -- --nocapture` (three times, fresh seeds each run)
Expected: all pass each time. If a universe fails: the failure message prints the seed and repro command — **stop, reproduce with `CHARPENTE_SEED`, fix the bug, and pin the seed as a regression test before proceeding** (hard rule 3).

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "charpente: transactions crash property + determinism suite

256 fresh-seeded universes per run: recover → oracle check (accounts,
entries, indexes, and jobs ≡ committed txns exactly — the dual-write
invariant) → mixed commits/aborts → random checkpoint → crash. Same seed,
same durable fingerprint."
```

---

### Task 5: Negative-space meta-test — the harness must catch apply-before-sync

**Files:**
- Modify: `crates/charpente/tests/transactions.rs` (append)

**Interfaces:**
- Consumes: `Wal`, `Store` directly (bypassing `Db` — the point is a deliberately broken ordering `Db` cannot express), `charpente_sim::rng::splitmix64`.

- [ ] **Step 1: Append the meta-test**

Add `Wal` to the existing `charpente_core::log` import (`use charpente_core::log::{Wal, WalConfig};`) and add `use charpente_core::table::Store;`. Then append:

```rust
// ---------------------------------------------------- negative space

/// The classic dual-write bug, reconstructed on purpose: apply to the store
/// and checkpoint BEFORE the WAL record is synced. The harness must catch
/// the store outrunning durable truth — if it ever stops catching it, the
/// crash injection has regressed (same contract as crash_recovery.rs's
/// harness_catches_naive_recovery).
fn broken_ordering_workload(sim: &mut Sim) -> Result<(), String> {
    let entropy = sim.entropy();
    let cfg = WalConfig {
        dir: "wal".to_string(),
        segment_bytes: 4096,
    };

    for _round in 0..3 {
        sim.crash_disk_after(32 + entropy.next_u64() % 256);
        let opened = (|| {
            let mut durable = 0u64;
            let wal = Wal::open(Box::new(sim.disk()), cfg.clone(), |_, _| {
                durable += 1;
                Ok(())
            })?;
            let store = Store::open(&sim.disk(), "tables.redb")?;
            io::Result::Ok((wal, store, durable))
        })();
        let (mut wal, store, durable) = match opened {
            Ok(t) => t,
            Err(_) => {
                sim.restart();
                continue;
            }
        };
        let applied = match store.applied_seq() {
            Ok(a) => a,
            Err(_) => {
                sim.restart();
                continue;
            }
        };

        // THE invariant this workload exists to violate:
        let next = applied.map_or(0, |s| s + 1);
        if next > durable {
            return Err(format!(
                "store applied_seq {applied:?} outran the durable log ({durable} records) — \
                 phantom state a conventional dual-write stack would serve as truth"
            ));
        }

        // Catch the store up to durable truth (content-free: only the seq
        // bump matters to this workload).
        let mut alive = true;
        for s in next..durable {
            if store.apply(s, |_| Ok(())).is_err() {
                alive = false;
                break;
            }
        }
        if !alive {
            sim.restart();
            continue;
        }

        // New traffic, in the BROKEN order: append → apply → checkpoint →
        // (only now) sync. A crash in the window makes the store a liar.
        let mut seq = durable;
        for _ in 0..1 + entropy.next_u64() % 6 {
            if wal.append(&[0xAB; 64]).is_err() {
                break;
            }
            if store.apply(seq, |_| Ok(())).is_err() {
                break;
            }
            if store.checkpoint().is_err() {
                break;
            }
            if wal.sync().is_err() {
                break;
            }
            seq += 1;
        }
        sim.restart();
    }
    Ok(())
}

/// Fixed base so CI is deterministic (the positive property uses fresh
/// seeds every run).
const META_TEST_BASE: u64 = 0x7C4A_2026_0004;

#[test]
fn harness_catches_apply_before_sync() {
    let mut state = META_TEST_BASE;
    let mut caught = None;
    for _ in 0..400 {
        let seed = charpente_sim::rng::splitmix64(&mut state);
        let mut sim = Sim::new(seed);
        if let Err(msg) = broken_ordering_workload(&mut sim) {
            caught = Some((seed, msg));
            break;
        }
    }
    let (seed, msg) = caught.expect(
        "the harness no longer catches store-apply-before-WAL-sync — \
         the crash injection itself has regressed",
    );
    eprintln!("caught broken ordering: seed {seed:#018x}: {msg}");

    // The seed IS the reproduction.
    let mut replay = Sim::new(seed);
    assert_eq!(
        broken_ordering_workload(&mut replay),
        Err(msg),
        "replaying the seed must reproduce the identical failure"
    );
}
```

- [ ] **Step 2: Run it**

Run: `cargo test -p charpente --test transactions harness_catches_apply_before_sync -- --nocapture`
Expected: PASS, with an eprintln line naming the caught seed. If no seed out of 400 catches it, the workload or fault matrix is wrong — investigate before touching the 400 limit.

- [ ] **Step 3: Full suite + commit**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets`
Expected: all green.

```bash
git add -A && git commit -m "charpente: negative-space meta-test — harness catches store-apply-before-WAL-sync

The dual-write bug class charpente exists to delete, reconstructed
deliberately against Wal+Store: apply+checkpoint before sync. The sim must
catch the store outrunning durable truth; the caught seed replays
identically."
```

---

### Task 6: Close out — CLAUDE.md current state, final verification

**Files:**
- Modify: `CLAUDE.md` (current state / next steps section)

- [ ] **Step 1: Full verification, three property runs**

Run: `cargo test --workspace && cargo test --workspace && cargo test --workspace && cargo clippy --workspace --all-targets`
Expected: green three times over fresh seeds (each run is 256 transaction universes + 512×2 WAL universes + 512 crash-recovery universes + 256 table universes).

- [ ] **Step 2: Update CLAUDE.md** — replace the final `- Next: M0 step 4 …` bullet with:

```markdown
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
- Next: M0 step 5 per PLAN.md — HTTP layer: `#[route]` over a hyper-based server, typed
  params/body/response, minimal sessions; edge concurrency scheduled through a
  deterministic executor in sim (the cost accepted in DECISIONS §10).
```

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md && git commit -m "docs: M0 step 4 done — transactions landed; step 5 (HTTP layer) is next

Evidence: full workspace suite green over three fresh-seed runs (256
transaction universes per run, plus the standing WAL/crash/table suites)."
```

---

## Self-review notes (already applied)

- Spec coverage: `View` ✓, effects record v0 ✓, RYW (get/scan/scan_by) ✓, `TxError` ✓, collapsed canonical record ✓, jobs materialization + `(seq, idx)` key ✓, emits log-only ✓, fallible `StateMachine`/`Wal::open` ✓, engine poison ✓, recovery skip ≤ `applied_seq` + store-never-ahead assert ✓, read-only-no-seq ✓, abort ✓, `MAX_RECORD_BYTES` contract ✓, error-quality tests (unregistered table, duplicate registration, oversized, unknown tag, trailing bytes) ✓, crash property with dual-write oracle ✓, determinism fingerprint ✓, apply-before-sync meta-test ✓, README/DESIGN/CLAUDE.md updates in the right commits ✓.
- Type consistency: `enqueue_raw(seq: u64, idx: u32, name: &str, payload: &[u8])` used identically in Tasks 2, 3, 5; `Snapshot::jobs() -> Vec<(u64, u32, String, Vec<u8>)>` matches all consumers; `View::scan_by::<R>(index_name, key)` turbofish matches the existing macro pattern (`scan_by::<Self>` with an `impl Codec` argument is already in the committed macro, so the toolchain accepts explicit generics alongside APIT).
- Deliberate deviations from nothing: transaction content in the property test is a pure function of its index so the oracle survives any crash prefix — this is why `apply_txn` takes `i` rather than entropy.
