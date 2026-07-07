# Building an app on charpente

*(This file is compile-gated: every `rust` code block below is extracted
and built by `crates/guide-examples` — `cargo build -p guide-examples` fails
if a block stops compiling. Sections 1–4 cover the persistence layer:
project setup, tables, the database, and transactions. Sections 5–9 — HTTP
routes, jobs, cron, sim testing, and an error index — land in later
commits.)*

## 1. Project setup

A charpente app is a Rust crate that links against the `charpente` platform
crate and compiles into one binary. Deployment is one binary and one data
directory — no database server, queue service, or config store to
provision.

### `Cargo.toml`

One dependency is all a charpente app needs:

```toml
[package]
name = "myapp"
version = "0.1.0"
edition = "2024"

[dependencies]
charpente = "0.0.0"

[dev-dependencies]
charpente-sim = "0.0.0"

[[test]]
name = "sim"
harness = false
```

`charpente-sim` is dev-only: it drives your app's tables, routes, jobs, and
time through a deterministic simulator with seeded fault injection, and
never links into the shipped binary. The `[[test]] name = "sim", harness =
false` block is required for `charpente test --sim` (covered in a later
section): it tells Cargo to build a `tests/sim.rs` binary without the
default libtest harness, so the sim runner supplies its own `main` instead.

### The data directory

`charpente::prod::ProdDisk` is the production mount for the `Disk` trait —
the sanctioned place ambient filesystem calls happen. Every table's backing
file and the write-ahead log's segments live under one root directory, as
flat relative names (`wal/000001.log`, `tables.redb`); subdirectories are
created on demand.

```rust
use charpente::prod::ProdDisk;

/// Opens the app's one data directory, creating it if it doesn't exist yet.
fn open_data_dir(path: &str) -> std::io::Result<ProdDisk> {
    ProdDisk::new(path)
}
```

Paths handed to a `Disk` must be relative flat names — never absolute, never
containing a `.` or `..` segment, never empty. Violating this is a contract
panic ("Disk paths are relative flat names like \"wal/000001.log\" — got
\"...\""), not a recoverable error.

## 2. Tables

A table is a plain struct annotated `#[table]`. The macro generates the
`Row` implementation (the byte encoding, the table's name, its index list)
plus one lookup function per `#[index]` field.

### The `#[table]` contract

- The struct must be a non-generic, braced struct with named fields — a
  table is one concrete row shape.
- It must have an `id` field of type `Id<Self>` — a typed primary key.
  `Id<Invoice>` is not `Id<Org>`, and neither is a bare `u64`. By
  convention (every example here follows it) `id` is declared first, but
  the macro only checks that the field exists, not its position.
- Every other field's type must implement `Codec` — charpente's own
  deterministic, versionless (v0) encoding, not serde. `Codec` is
  implemented for `u8`/`u16`/`u32`/`u64`/`i8`/`i16`/`i32`/`i64`, `bool`,
  `String`, `Vec<u8>`, `Option<T> where T: Codec`, and `Id<T>` for any `T`.
  A field type that isn't `Codec` fails as an ordinary trait-bound error at
  the macro's generated encode/decode code, not a custom message.
- Fields encode and decode **in declaration order** — reordering fields
  changes the byte layout of every row already durable on disk.

```rust
use charpente::{Id, View, table};

struct Org; // referenced only through its typed id below

#[table]
struct Invoice {
    id: Id<Invoice>,
    #[index]
    org: Id<Org>,
    amount_cents: i64,
    memo: Option<String>,
    paid: bool,
}

/// `#[index]` on a field generates an exact-match lookup named
/// `by_<field>`. It takes any `View` — a `Snapshot` outside a transaction,
/// or a `Tx` inside one — and returns every row with that key, in id order.
fn invoices_for_org(view: &impl View, org: Id<Org>) -> std::io::Result<Vec<Invoice>> {
    Invoice::by_org(view, &org)
}
```

A table can carry any number of `#[index]` fields; each gets its own
function with this exact shape:

```rust,ignore
fn by_<field>(view: &impl View, key: &<FieldType>) -> std::io::Result<Vec<Self>>
```

### What the macro rejects

Every rejection happens at compile time, and the message names the contract
and the fix:

```rust,ignore
// no `id` field at all
#[table]
struct Ping { hits: u64 }
// "missing `id: Id<Ping>` field — every #[table] row needs a typed primary
//  key (add `id: Id<Ping>`)"

// `id` isn't `Id<Self>`
#[table]
struct Ping { id: u64, hits: u64 }
// "the `id` field of a #[table] must be `Id<Ping>` — a typed id, never a
//  bare integer"

// `#[index]` on the primary key
#[table]
struct Ping { #[index] id: Id<Ping>, hits: u64 }
// "`id` is the primary key — it is already the fastest lookup; remove
//  #[index]"

// not a braced struct with named fields (tuple structs and unit structs
// both hit this)
#[table]
struct Ping(u64);
// "#[table] needs named fields — `Ping` must be a braced struct with an
//  `id: Id<Ping>` field"

// enums and unions cannot be rows
#[table]
enum Ping { A, B }
// "#[table] applies to structs — enums and unions cannot be rows"

// generic structs — a table is one concrete row shape
#[table]
struct Ping<T> { id: Id<Ping<T>>, data: T }
// "#[table] structs cannot be generic — a table is one concrete row shape;
//  make `Ping` non-generic"
```

A row that fails to decode at runtime (schema drift, corruption, a
reordered field) fails with, for example, `"row decode: 3 trailing bytes
after a Ping row — schema drift or corruption"`.

## 3. The database

`Db` composes the write-ahead log with the typed table store behind one
writer:

```rust,ignore
pub fn open(disk: Box<dyn Disk>, cfg: DbConfig, schema: Schema) -> std::io::Result<Db>
```

`Schema` is a builder that registers every `#[table]` type your app writes.
Replay works from raw log bytes and needs each type's codec to reconstruct
rows and maintain indexes:

```rust
use charpente::prod::ProdDisk;
use charpente::{Db, DbConfig, Id, Schema, table};

#[table]
struct Counter {
    id: Id<Counter>,
    hits: u64,
}

fn open(data_dir: &str) -> std::io::Result<Db> {
    let disk = ProdDisk::new(data_dir)?;
    let schema = Schema::new().table::<Counter>();
    Db::open(Box::new(disk), DbConfig::default(), schema)
}
```

Registering the same table type twice is a contract panic ("Schema: table
\"Counter\" registered twice — register each #[table] type once").
`Db::open` registers one table of its own before yours — the platform's job
queue, `charpente.jobs` — so registering that table yourself hits the same
panic.

`DbConfig` names the WAL's directory and segment rotation size, and the
table store's file name:

```rust
use charpente::DbConfig;
use charpente::core::log::WalConfig;

fn config() -> DbConfig {
    DbConfig {
        wal: WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 64 << 20, // 64 MiB
        },
        store_path: "tables.redb".to_string(),
    }
}
```

`DbConfig::default()` already sets exactly these values — most apps never
construct one by hand.

### Recovery, behaviorally

`Db::open` opens the table store first; if the store itself can't open (a
crash mid-creation can leave bytes redb refuses to read), it's discarded and
rebuilt — the store is a materialization of the log, never a second source
of truth. `Db::open` then recovers the write-ahead log and replays every
durable record after the store's last checkpoint back into it, so a
reopened `Db` reflects every transaction that ever committed, even one the
store hadn't caught up to when the process died. Recovery can itself fail
under an IO fault; `Db::open` is idempotent, so the caller's answer to a
failed open is simply to retry it. Outside a transaction, `Db::snapshot() ->
io::Result<Snapshot>` gives the same read surface (a `View`) for read-only
queries — inside a transaction, reads additionally see the transaction's
own writes, covered next.

## 4. Transactions

Every state change goes through one transaction:

```rust,ignore
pub fn transact<T, E>(
    &mut self,
    f: impl FnOnce(&mut Tx) -> Result<T, E>,
) -> Result<T, TxError<E>>
```

`f` runs synchronously against a `Tx`: reads see a snapshot taken when the
transaction started, plus every write this same transaction has already
made — read-your-own-writes. `Tx` implements `View` (`get`, `scan`,
`scan_by`), so `Invoice::by_org` from §2 works on a `Tx` exactly as it does
on a `Snapshot`. On `Ok`, every buffered write, enqueue, and emit commits as
ONE durable log record — all or nothing — and only then applies to the
store. On `Err`, nothing happened at all: no log position consumed, nothing
written.

```rust
use charpente::core::table::Codec;
use charpente::{Db, Event, Id, Job, Tx, TxError, View, table};
use std::io;

#[table]
struct Invoice {
    id: Id<Invoice>,
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
    // `RETRY` defaults to `RetryPolicy::DEFAULT` — retry/backoff and cron
    // are covered in a later section.
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

/// The row update, the receipt job, and the realtime event all commit in
/// the one transaction — or none of them do.
fn finalize(db: &mut Db, id: u64) -> Result<(), TxError<io::Error>> {
    db.transact(|tx: &mut Tx| {
        let mut inv = tx
            .get::<Invoice>(Id::from_raw(id))?
            .ok_or_else(|| io::Error::other("invoice not found"))?;
        if inv.paid {
            return Err(io::Error::other("invoice already paid"));
        }
        inv.paid = true;
        tx.put(&inv);
        tx.enqueue(SendReceipt { invoice: id });
        tx.emit(InvoiceFinalized { invoice: id });
        Ok(())
    })
}
```

Every `Job` needs `Codec` + a `NAME`; every `Event` needs `Codec` + a
`NAME`. Both are implemented by hand today (v0); `#[job]` derives the rest
of a job's registration for you (a later section). `tx.put` and `tx.delete`
cannot fail — they only buffer a write into the transaction's overlay;
`tx.get`/`scan`/`scan_by` return `io::Result` because they read through the
snapshot. A transaction that never calls `put`/`delete`/`enqueue`/`emit`
writes nothing and consumes no log position — read-only queries are free.

Enqueuing also takes an idempotency key, for effects the outside world must
observe at most once: `tx.enqueue_keyed(key, job)`. An empty key is a
contract panic ("contract: an idempotency key names the job's external
identity — it cannot be empty"). One more hard limit: all of a
transaction's buffered effects together must fit in one 16 MiB log record —
a transaction that doesn't panics with "transaction effects are N bytes but
one log record holds at most 16777216 — split the work into smaller
transactions".

### `TxError::App` vs `TxError::Io`

```rust,ignore
pub enum TxError<E> {
    App(E),
    Io(std::io::Error),
}
```

The distinction is load-bearing. `App(E)` is whatever `f` returned as
`Err` — your own rejection, or an IO failure hit while *reading* inside the
transaction (both surface through the closure's own `Result<T, E>`); either
way, **nothing was written**. `Io` is a platform failure outside `f`'s own
logic: opening the read snapshot before `f` runs, or appending/syncing the
committed record's log entry after `f` returned `Ok`. That second case is
genuinely ambiguous — the record may already be durable and will reappear
after the next recovery even though the caller was told the commit failed —
so treat `TxError::Io` as "unknown," never as "definitely didn't happen."

### What a transaction cannot do

`f` only ever receives `&mut Tx` — no `Disk`, `Clock`, `Entropy`, or `Net`
handle reaches inside it. There is no way to await a network call from
inside a transaction: `f`'s signature is a plain, synchronous `FnOnce(&mut
Tx) -> Result<T, E>`, not `async`, so writing `.await` inside it is a
compile error, not a runtime one:

```rust,ignore
db.transact(|tx| {
    tx.put(&row);
    fetch_from_the_network().await?; // error[E0728]: `await` is only
                                      // allowed inside `async` functions
                                      // and blocks
    Ok::<_, std::io::Error>(())
})
```

Nothing in the type system stops you from calling an ambient clock or
randomness function (`std::time::SystemTime::now()`, OS randomness, …)
inside `f` — charpente's own crates ban these mechanically with a clippy
`disallowed-methods`/`disallowed-types` deny-list (`clippy.toml`) enforced
in CI, not by review; adopt the same deny-list in your own app for the same
guarantee.
