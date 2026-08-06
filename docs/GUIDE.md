# Building an app on charpente

*(This file is compile-gated: every `rust` code block below is extracted
and built by `crates/guide-examples` — `cargo build -p guide-examples` fails
if a block stops compiling. Sections 1–4 cover the persistence layer:
project setup, tables, the database, and transactions. Sections 5–7 cover
the deterministic kernel: HTTP, jobs, and cron. Section 8 covers sim
testing — writing `tests/sim.rs`, running `charpente test --sim`, and
replaying a failure's seed. Section 9 is an error index: look up the
message you're staring at. Section 10 covers replication; section 11
covers realtime SSE subscriptions; section 12 covers migrations —
evolving a `#[table]` row's shape safely; section 13 covers `describe` —
the machine-readable contract of a mounted app.)*

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
flat relative names (`tables.redb`; WAL segments are named by their
zero-padded 20-digit base sequence, so the first is
`wal/00000000000000000000.log`); subdirectories are created on demand.

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
  the macro only checks that the field exists and that its type is
  `Id<Self>` (the rejection list below), never its position.
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

### `#[index]` works on any `Codec` field, including a hand-rolled enum

`#[index]`'s only requirement is the same one every table field already
has: the field type implements `Codec`. The generated index-key function
is nothing but `Codec::encode`, and the lookup it feeds
(`View::scan_by`) is bounded on `key: &impl Codec` — there is no separate
`Eq`/`Hash`/`Ord`/serde bound. A hand-rolled `Codec` enum — a status
field, say — can carry `#[index]` exactly like an `Id<T>` or a `String`
field can:

```rust
use charpente::core::table::Codec;
use charpente::{Id, View, table};
use std::io;

enum InvoiceState {
    Draft,
    Final,
    Paid,
}

impl Codec for InvoiceState {
    fn encode(&self, out: &mut Vec<u8>) {
        let tag: u8 = match self {
            InvoiceState::Draft => 0,
            InvoiceState::Final => 1,
            InvoiceState::Paid => 2,
        };
        tag.encode(out);
    }

    fn decode(input: &mut &[u8]) -> io::Result<Self> {
        match u8::decode(input)? {
            0 => Ok(InvoiceState::Draft),
            1 => Ok(InvoiceState::Final),
            2 => Ok(InvoiceState::Paid),
            other => Err(io::Error::other(format!(
                "invalid InvoiceState tag {other} — schema drift or corruption"
            ))),
        }
    }
}

#[table]
struct Invoice {
    id: Id<Invoice>,
    #[index]
    state: InvoiceState,
}

/// Generated by `#[index]` on `state`, same shape as any other indexed
/// field: an exact-match lookup, in id order.
fn final_invoices(view: &impl View) -> std::io::Result<Vec<Invoice>> {
    Invoice::by_state(view, &InvoiceState::Final)
}
```

When a query doesn't fit an exact-match index — a range or "greater
than" scan, which `#[index]` doesn't support in v0 — the fallback is
`tx.scan::<Invoice>()` filtered in Rust: correct, just not indexed. Reach
for that before assuming a `Codec` field can't be indexed at all.

### Generating ids for rows created inside a transaction

Every example above takes a row's `id` from somewhere outside the
transaction — a `:param` in the route path, or a value the caller
supplied. That works when a row has a natural external identity, but
breaks down for a row created *inside* a transaction with nothing to
source an id from — a child row for each entry of a request body's
`Vec<...>`, say.

There is no platform id-generation primitive for `#[table]` rows in v0
(the platform's own internal job-id counter is not exposed to app code).
The pattern: a singleton counter row per id space, read, incremented, and
written back inside the same transaction that needs a fresh id. This is
safe without a lock or an atomic op because transactions are serialized
through one writer (§4) — two transactions racing to claim an id can
never both observe the same counter value, since the second one only
starts after the first has already committed its increment (or aborted
without writing anything at all).

```rust
use charpente::{Id, Tx, View, table};
use std::io;

#[table]
struct IdCounter {
    id: Id<IdCounter>,
    next: u64,
}

/// One counter row per id space you need (line items, reminders, …),
/// keyed by an arbitrary `seq` value you pick per space.
fn next_id<T>(tx: &mut Tx, seq: u64) -> io::Result<Id<T>> {
    let counter_id = Id::<IdCounter>::from_raw(seq);
    let mut counter = tx.get(counter_id)?.unwrap_or(IdCounter {
        id: counter_id,
        next: 0,
    });
    let value = counter.next;
    counter.next += 1;
    tx.put(&counter);
    Ok(Id::from_raw(value))
}
```

Bit-packing an id out of a caller-supplied parent id (e.g. `(invoice_id
<< 20) | index`) is tempting but unsafe in general: a caller-supplied id
is an arbitrary `u64`, and shifting an arbitrary `u64` left can silently
wrap and collide with another parent's children. The counter-row pattern
above has no such ceiling.

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
        retain_segments: 2,
        checkpoint_every: 4096,
        event_retention: 65_536,
        migration_batch_size: 1000,
    }
}
```

`DbConfig::default()` already sets exactly these values — most apps never
construct one by hand. `retain_segments` and `checkpoint_every` govern
`Db::checkpoint`'s log GC, covered next. `event_retention` bounds the
built-in `charpente.events` table the same way: each checkpoint deletes all
but the newest `event_retention` event rows (`Db::event_floor()` reports the
smallest id still retained). `migration_batch_size` caps how many rows one
schema-migration transaction rewrites at a time, bounding a single migration
commit's WAL record size.

### Recovery, behaviorally

`Db::open` opens the table store first; if the store itself can't open (a
crash mid-creation can leave bytes redb refuses to read), it's discarded and
rebuilt — the store is a materialization of the log, never a second source
of truth. That rebuild only works while the log still holds everything back
to seq 0, though: `Db::checkpoint(&mut self)` durably persists the store,
then deletes WAL segments below the checkpoint (`DbConfig::retain_segments`
floors how much it keeps; `checkpoint_every` auto-fires the cadence every N
committed records, `0` to disable it and checkpoint only by hand). Once the
log has been compacted this way, a store that can't open is unrecoverable
locally — `Db::open` returns a named error instead of a silent (wrong)
rebuild, naming the fix: restore from a replica/backup, or widen
`retain_segments`. `Db::open` then recovers the write-ahead log and replays
every durable record after the store's last checkpoint back into it, so a
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
use charpente::{Db, Id, Job, Tx, TxError, View, event, table};
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

#[event]
struct InvoiceFinalized {
    invoice: u64,
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

`Job` needs `Codec` + a `NAME`, implemented by hand today (v0); `#[job]`
derives the rest of a job's registration for you (a later section).
`#[event]` derives everything an event needs in one step: `Codec` (the log
format), `Event` (`NAME` is the snake_case of the type name — §11 has the
exact splitting rule), and `Serialize` (the wire format `#[route(SSE …)]`
subscribers receive, §11). `tx.put` and `tx.delete` cannot fail — they only
buffer a write into the transaction's overlay; `tx.get`/`scan`/`scan_by`
return `io::Result` because they read through the snapshot. A transaction
that never calls `put`/`delete`/`enqueue`/`emit` writes nothing and consumes
no log position — read-only queries are free.

Enqueuing also takes an idempotency key, for effects the outside world must
observe at most once: `tx.enqueue_keyed(key, job)`. An empty key is a
contract panic ("contract: an idempotency key names the job's external
identity — it cannot be empty"). One more hard limit: all of a
transaction's buffered effects together must fit in one 16 MiB log record —
a transaction that doesn't panics with "transaction effects are N bytes but
one log record holds at most 16777216 — split the work into smaller
transactions".

### `TxError`'s four variants

`TxError<E>` has four variants, and this block matches the real type — a
variant rename or addition breaks the guide's compile gate:

```rust
use charpente::TxError;

/// The four arms differ in what they promise: `App` means nothing was
/// written; `Io` means the outcome is unknown (details below); `NotPrimary`
/// means this node is a read-only follower and nothing was even attempted;
/// `Verifying` means this node cannot yet tell whether it is allowed to
/// write, and nothing was attempted either.
fn describe<E: std::fmt::Display>(err: &TxError<E>) -> String {
    match err {
        TxError::App(e) => format!("rejected by the app — nothing was written: {e}"),
        TxError::Io(e) => format!("platform IO failure — outcome unknown: {e}"),
        TxError::NotPrimary { primary } => {
            format!("read-only follower — writes go to {primary} instead")
        }
        TxError::Verifying { unreached } => {
            format!("still verifying witnesses {unreached:?} — writes are on hold")
        }
    }
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
`NotPrimary { primary }` fires only when `f` had at least one effect to
write and this node has been marked a read-only follower
(`Db::forbid_writes`, DESIGN §5) — reads and effect-free transactions never
see it; the HTTP layer maps it to 421, naming `primary`.

`Verifying { unreached }` (M2 sub-project 0b) also fires only when `f` had
at least one effect to write — reads and effect-free transactions never see
it either. It means this node was a durable primary before it restarted and
has not yet confirmed that none of its followers (its "witnesses") were
promoted while it was down (`Db::verify_writes`). Unlike `NotPrimary`, there
is no primary address to name: this node does not know whether one exists
elsewhere, and it may still be the legitimate primary itself. `unreached` is
the witness set this node is verifying, fixed at the moment the gate closed;
the HTTP layer maps it to 503, naming them. It does not shrink as individual
witnesses answer — the stderr notice described in §10 is where the live
per-witness detail lives. The state is transient — writes resume automatically
(`Db::allow_writes`) the moment every witness confirms it was never
promoted, with no caller action needed.

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

## 5. HTTP

`App` owns the `Db` plus a router; `App::handle(request: Request) -> Response`
is the whole surface — the sim drives `handle` directly, and
`charpente::serve` is what feeds it from real hyper traffic in production.
Every request that reaches a matched route runs inside exactly one
transaction: routing runs first (an unmatched request never opens one),
then the session lookup and your handler run inside the same `db.transact`
call covered in §4.

### The `#[route]` argument convention

`#[route(METHOD "/path/:param", ...)]` goes on a free function (`METHOD` is
one of `GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `SSE`; the trailing `, auth`
flag is optional and covered under Sessions below). The macro checks the
function's argument list against the path and emits the glue that extracts,
decodes, and dispatches to it:

- The first argument must be named exactly `tx`, typed `&mut Tx`.
- Then one argument per `:param` in the path, **in path order**, each named
  exactly like its param (`:id` needs an argument named `id`). Its type
  must implement `FromParam` — `u64`, `String`, and `Id<T>` (any `T`) are
  provided; a bad path segment fails as a named 400, not a panic.
- Then at most one JSON body argument, of any owned deserializable type
  (`serde::de::DeserializeOwned` — the glue's actual bound); a type
  borrowing from the input won't compile.
- Then, optionally, a trailing argument named exactly `ctx`, typed `&Ctx`
  (session access — see Sessions below).

The handler returns `Result<T, ApiError>` where `T: serde::Serialize`; on
`Ok`, the glue emits `Response::json(200, &value)`.

```rust
use charpente::core::table::Id;
use charpente::http::{ApiError, App, AppConfig, Ctx};
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::{Schema, Tx, View, route, routes, table};
use serde::Deserialize;

#[table]
struct Note {
    id: Id<Note>,
    text: String,
}

#[derive(Deserialize)]
struct NoteBody {
    text: String,
}

// tx, then :id (named `id`, path order), then the JSON body, then `ctx`.
#[route(POST "/notes/:id")]
fn create_note(tx: &mut Tx, id: u64, body: NoteBody, ctx: &Ctx) -> Result<u64, ApiError> {
    let _ = ctx; // available; this route just doesn't need a session
    tx.put(&Note {
        id: Id::from_raw(id),
        text: body.text,
    });
    Ok(id)
}

#[route(GET "/notes/:id")]
fn get_note(tx: &mut Tx, id: Id<Note>) -> Result<String, ApiError> {
    Ok(tx.get(id)?.ok_or(ApiError::NotFound)?.text)
}

fn build_app(data_dir: &str) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(ProdDisk::new(data_dir)?),
        Schema::new().table::<Note>(),
        Box::new(ProdEntropy::new()?),
        AppConfig::default(),
    )?
    .mount(routes![create_note, get_note]))
}
```

`routes![...]` accepts path-qualified handlers too, not just bare
identifiers in scope — `routes![handlers::create_note]` works exactly like
`routes![create_note]` after `use handlers::create_note;`.

`Request`'s shape (what a handler ultimately runs against, one layer down):
`method: Method`, `path: String`, `query: BTreeMap<String, String>`,
`headers: BTreeMap<String, String>` (keys lowercased), `body: Vec<u8>`.
`query` is parsed from the URL's `?...` suffix at construction; a plain
`#[route]` handler still has no argument-convention access to it (only
`#[route(SSE ...)]`'s platform-extracted resume cursor reads it today).

### Returning rows from a route: `Id<T>` isn't serializable

A handler's return type only needs `T: serde::Serialize` (above) — nothing
requires `T` to be a `#[table]` row, and in practice it can't be one
directly: `Id<T>` implements charpente's own `Codec` (§2), deliberately
not serde, so it does not implement `serde::Serialize` or `Deserialize`.
A route that tries to return a row whose type has an `Id<T>` field fails
to compile with a missing-trait-bound error on `Id`, not a runtime
surprise.

The fix is the one every other example in this guide already uses without
saying so: convert to a plain response type first, pulling `Id<T>` down to
its `u64` with `.raw()`.

```rust
use charpente::http::{ApiError, App, AppConfig};
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::{Id, Schema, Tx, View, route, routes, table};
use serde::Serialize;

#[table]
struct Note {
    id: Id<Note>,
    text: String,
}

/// The wire shape: plain, `Serialize`-able fields only — `Id<Note>` never
/// appears in it directly.
#[derive(Serialize)]
struct NoteView {
    id: u64,
    text: String,
}

impl From<Note> for NoteView {
    fn from(row: Note) -> Self {
        NoteView {
            id: row.id.raw(),
            text: row.text,
        }
    }
}

#[route(GET "/notes/:id")]
fn get_note(tx: &mut Tx, id: Id<Note>) -> Result<NoteView, ApiError> {
    Ok(tx.get(id)?.ok_or(ApiError::NotFound)?.into())
}

fn build_app(data_dir: &str) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(ProdDisk::new(data_dir)?),
        Schema::new().table::<Note>(),
        Box::new(ProdEntropy::new()?),
        AppConfig::default(),
    )?
    .mount(routes![get_note]))
}
```

Storage rows and wire responses are different concerns regardless of the
serde question — this DTO conversion is good hygiene even for a `Codec`
field type that happens to also implement `Serialize`.

### Mounting routes, and the conflict panic

`App::open(...)` returns the platform-registered `App`; `.mount(routes![...])`
adds a batch of routes, checking every new route against every route already
mounted. Two routes with the **same method and the same shape** — the same
sequence of literal-vs-`:param` segments, so both could match one real path
— panic at mount time, not at request time:

```rust
use charpente::http::{ApiError, App};
use charpente::{Tx, route, routes};

#[route(GET "/notes/:id")]
fn by_id(tx: &mut Tx, id: u64) -> Result<u64, ApiError> {
    let _ = tx;
    Ok(id)
}

#[route(GET "/notes/:key")]
fn by_key(tx: &mut Tx, key: String) -> Result<String, ApiError> {
    let _ = tx;
    Ok(key)
}

// `/notes/:id` and `/notes/:key` are the same shape under the same method —
// both could match `GET /notes/42` — so mounting both panics with:
// "route conflict: GET /notes/:key (fn by_key) collides with GET
//  /notes/:id (fn by_id) — two routes with the same method and shape can
//  both match one path; change one pattern"
fn mount_conflicting(app: App) -> App {
    app.mount(routes![by_id, by_key])
}
```

A literal segment beats a `:param` at the same position instead of
conflicting — `/notes/all` and `/notes/:id` can coexist, and a request for
`/notes/all` always prefers the literal route.

### Auth: sessions, API keys, and the resolution ladder

`App::open` registers two platform tables of its own alongside yours —
`Session` (`id: Id<Session>`, `#[index] token_hash: String`, `subject:
String`, `expires_at: u64`) and `ApiKey` (`id: Id<ApiKey>`, `#[index]
key_hash: String`, `subject: String`, `created_at: u64`) — both inside the
commit point, so a login or a minted key is a durable, replicated
transaction like any other. **Only the hash is ever stored:**
`token_hash`/`key_hash` are lowercase-hex SHA-256 of the raw credential; the
raw string exists only in the response that mints it and never reaches disk
(a sim property scans every durable file in the universe and proves it).

Every request gets a `Ctx`, built inside the same transaction as your
handler, carrying:

- `ctx.principal() -> Option<&Principal>` — the authenticated identity, if
  any: `Principal { subject: String, via: Via }`, where `via` names which
  rung authenticated it (`Via::Session(id)`, `Via::ApiKey(id)`, or
  `Via::Oidc`). This is the surface to read `subject` from on any `auth`
  route, whatever the credential kind.
- `ctx.session() -> Option<&Session>` — a compatibility accessor: the
  `Session` row, but only when the principal authenticated `via` a session
  (bearer or cookie); `None` for an API-key or OIDC principal.
- `ctx.create_session(tx, subject) -> String` — inserts a `Session` row and
  returns its raw token (a login). `expires_at` is this request's `now` plus
  `AppConfig::session_ttl` (default 30 days).
- `ctx.create_api_key(tx, subject) -> String` — inserts an `ApiKey` row and
  returns the raw key: `"ck_"` + 64 hex chars (the prefix makes a leaked key
  grep-able for scanners). Revoke a key by deleting its row —
  `tx.delete::<ApiKey>(id)`, an ordinary write.
- `ctx.revoke(tx)` — deletes the current session (logout); a no-op without
  one.

One credential mint per request, of either kind. The raw token/key is 32
entropy bytes drawn once *before* the transaction starts, so
`create_session` and `create_api_key` share a single mint budget — calling
either a second time in one request panics: "Ctx::create_session/create_api_key
called twice in one request — one credential mint per request".

**The resolution ladder.** Before your handler runs, the platform turns the
request's credential into a `Principal`, in this order:

1. `Authorization: Bearer <t>` where `<t>` contains exactly two `.` — judged
   as a JWT and OIDC-verified (below). With no verifier mounted, a JWT-shaped
   bearer is an unrecognized credential, never a fall-through to the hash
   path.
2. Any other bearer — SHA-256'd and matched against the `ApiKey` index, then
   the `Session` index (a session past its `expires_at` is treated as
   absent).
3. A `charpente_session=<t>` cookie — the same hash path as 2. Only a
   `Bearer` header is ever OIDC-verified; a cookie value that happens to
   contain two dots still goes down the hash path.
4. Nothing presented — `principal` is `None`.

`#[route(METHOD "/path", auth)]` rejects the request before your handler runs
when the principal is `None`. Without `auth`, the route always runs and
`ctx.principal()` is whatever the ladder resolved (possibly `None`) — but a
*presented* credential is still resolved and rejected even on a no-auth
route, so a garbage bearer never reaches a handler.

The 401s are named and fixed — copy these verbatim if you assert against
them:

```text
authentication required — send "Authorization: Bearer <token>" or the charpente_session cookie
unrecognized credential — it may have been revoked; obtain a new one
session expired — log in again
```

The first fires when an `auth` route saw no credential; the second for a
bearer/cookie that matched no live row (garbage, revoked, or a JWT-shaped
bearer with no OIDC configured); the third for a session past its TTL. Expiry
is *lazy*: a row is invisible the moment `now` passes `expires_at`, restart
or not — a built-in daily cron sweeps the dead rows for hygiene, but
resolution never waits on it.

```rust
use charpente::auth::ApiKey;
use charpente::http::{ApiError, App, AppConfig, Ctx};
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::{Id, Schema, Tx, route, routes};

#[route(POST "/login")]
fn login(tx: &mut Tx, subject: String, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_session(tx, subject))
}

#[route(GET "/me", auth)]
fn whoami(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let _ = tx;
    Ok(ctx
        .principal()
        .expect("auth routes always have a principal")
        .subject
        .clone())
}

#[route(POST "/logout", auth)]
fn logout(tx: &mut Tx, ctx: &Ctx) -> Result<&'static str, ApiError> {
    ctx.revoke(tx);
    Ok("bye")
}

// Mint an API key for the caller's own subject; revoke by deleting the row.
#[route(POST "/keys", auth)]
fn mint_key(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let subject = ctx.principal().expect("auth route").subject.clone();
    Ok(ctx.create_api_key(tx, subject))
}

#[route(DELETE "/keys/:id", auth)]
fn revoke_key(tx: &mut Tx, id: Id<ApiKey>, ctx: &Ctx) -> Result<&'static str, ApiError> {
    let _ = ctx;
    tx.delete::<ApiKey>(id);
    Ok("revoked")
}

fn build_app(data_dir: &str) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(ProdDisk::new(data_dir)?),
        Schema::new(), // Session and ApiKey are registered by App::open itself
        Box::new(ProdEntropy::new()?),
        AppConfig::default(),
    )?
    .mount(routes![login, whoami, logout, mint_key, revoke_key]))
}
```

### OIDC (static JWKS)

`App::with_oidc` mounts a verifier for RS256-signed JWTs against a static
JWKS — pure signature-and-claims checking on the kernel thread, no network
fetch (dynamic JWKS is a later milestone). Configure it with the IdP's
issuer, your audience, and the JWKS document verbatim:

```rust
use charpente::auth::OidcConfig;
use charpente::http::App;

fn mount_oidc(app: App) -> App {
    app.with_oidc(OidcConfig {
        issuer: "https://idp.example.com".to_string(),
        audience: "my-charpente-api".to_string(),
        // The IdP's JWKS document, verbatim — one or more RSA keys.
        jwks_json: r#"{"keys":[{"kty":"RSA","kid":"k1","n":"0vx7...","e":"AQAB"}]}"#.to_string(),
    })
}
```

The JWKS is parsed once, at mount — a malformed document or an unusable key
panics *then*, naming the offending key and the fix, never at request time
(`<kid>` is the key's `kid`, or its position when absent):

```text
OidcConfig::jwks_json is not valid JSON: <detail> — pass the IdP's JWKS document verbatim (a {"keys": [...]} object)
OidcConfig::jwks_json has no keys — the "keys" array is empty; add at least one RSA key
OIDC JWKS key <kid> has kty "EC" — only "RSA" is supported in v1; remove non-RSA keys from the JWKS
OIDC JWKS key <kid> has alg "RS384" — only "RS256" is supported; fix or remove the key's "alg" field
OIDC JWKS key <kid> is invalid: modulus "n" is not valid base64url: <detail>
```

A verified token yields a `Principal` with `via: Via::Oidc` and `subject` =
the token's `sub` claim — ephemeral, no row, nothing to revoke but a short
`exp`. `exp`/`nbf` are checked against the request clock with a 60-second skew
tolerance. Each failed check is its own named 401 (the `iss`/`aud` ones name
presented-vs-configured; `<presented>`/`<configured>` are the actual values):

```text
malformed OIDC token — expected "header.payload.signature" (three dot-separated segments); got 2 segment(s)
unsupported OIDC token alg "HS256" — only "RS256" is verified; mint the token with an RS256-signed key
OIDC token signature does not verify against any configured JWKS key — the token was not issued by a trusted key; obtain a new one
OIDC token expired — its "exp" claim is in the past; obtain a new one
OIDC token is not yet valid — its "nbf" claim is in the future; wait or mint a fresh token
OIDC token has issuer "<presented>" but this server is configured for issuer "<configured>" — mint the token against the configured issuer or fix OidcConfig::issuer
OIDC token has audience "<presented>" but this server is configured for audience "<configured>" — mint the token for the configured audience or fix OidcConfig::audience
```

The replication port has its own peer-authentication secret, unrelated to
this request-side auth — see §10.

### `ApiError` and status mapping

A handler rejects with `ApiError`; `From<io::Error> for ApiError` converts
to `Internal`, so `?` works on `tx` reads exactly like it does inside a
plain transaction (§4). Every response body is a JSON envelope
`{"error": "<message>"}` except a 200, which is the handler's own returned
value:

- `BadRequest(String)` → 400. What `FromParam`/JSON-body decoding produce
  (below), or your own rejection.
- The 401 family → 401. Auth rejections, produced before your handler runs
  (the resolution ladder, above): `Unauthorized` (no credential),
  `UnrecognizedCredential`, `SessionExpired`, and `Oidc(..)` (one per failed
  OIDC check). Their exact, named texts are in the auth section above — you
  rarely construct these yourself; the `auth` gate and the ladder emit them.
- `NotFound` → 404. Fixed message `"not found"` — the same text an
  unmatched path gets from the router directly.
- `Internal(String)` → 500. Carries `e.to_string()` when it arrived via
  `?`, or your own message.

Two more statuses never go through `ApiError` at all: an unmatched path is
404 ("not found") and a matched path with the wrong method is 405 ("method
not allowed for this path") — both produced by the router before a
transaction ever opens, so nothing was attempted, let alone written.

A path param that doesn't parse is a named 400 — e.g. a non-numeric `:id`
against a `u64` argument: `"path parameter :id must be an unsigned integer
— got \"abc\""`. A missing or malformed JSON body is a named 400 too, e.g.
`"this route requires a JSON body of type NoteBody"` (empty body) or
`"invalid JSON body for NoteBody: <serde_json's own message>"`.

### The 1 MiB body cap

`AppConfig::body_cap` (default `1 << 20` — 1,048,576 bytes, 1 MiB) fences
the request body — and, transitively, `Db::transact`'s 16 MiB record-size
assert (§4) from ever seeing raw network input. It is enforced in two
places:

- In production, `charpente::serve` enforces it at the wire — a
  length-limited body read rejects an oversized body *before* the request
  is even routed, so it applies uniformly to every path, matched or not.
  With a 64-byte cap and a 65-byte body, the wire's 413 reads exactly:
  `"request body exceeds the 64-byte cap — raise AppConfig.body_cap or
  send less"`.
- Inside the kernel (`App::handle` — what the sim drives directly, and
  what `serve` calls after decoding), the same cap is checked again, but
  only **after** routing succeeds: an oversized body sent to an unmatched
  path 404s, not 413, since the router runs first. With the same 64/65
  numbers, this 413 reads: `"request body is 65 bytes but the cap is 64 —
  raise AppConfig.body_cap or send less"`.

Apps exercised only through `App::handle` (sim tests, and anything built
directly against the kernel) only ever see the second message — the wire
check is production-only edge behavior.

```rust
use charpente::http::AppConfig;

fn small_cap_config() -> AppConfig {
    AppConfig {
        body_cap: 4096,
        ..AppConfig::default()
    }
}
```

### Ack-after-durable

A 200 is emitted only once the handler's transaction has returned `Ok` —
which, per §4, only happens after that transaction's one log record has
synced to disk. There is no path that returns a 200 before durability.

The other statuses fall out of §4's `TxError` split precisely:
`ApiError`-mapped statuses (400/401/404, and a handler-level 500 from
`Internal`/`?`) are `TxError::App(e)` — the transaction aborted, **nothing
was written**, so there is nothing ambiguous about them. The remaining
case, `TxError::Io` — a platform failure appending or syncing the record
itself, outside the handler's own logic — is genuinely unknown: the record
may already be durable. It surfaces as its own 500, with a message that
says so instead of guessing: `"internal storage failure — the request may
or may not have committed; retry after the server recovers: {e}"`.

### Starting the server

`charpente::serve::serve(addr: &str, app: App) -> io::Result<Server>` spawns
the kernel (requests, due jobs, and cron ticks — §6 and §7 ride the same
loop) on one thread and the hyper listener on another, then returns as soon
as both are up — it does not block. v0 has no graceful shutdown: the
process owns the server for as long as it runs.

```rust
use charpente::core::table::Id;
use charpente::http::{ApiError, App, AppConfig};
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::{Schema, Tx, View, route, routes, table};
use serde::Deserialize;

#[table]
struct Note {
    id: Id<Note>,
    text: String,
}

#[derive(Deserialize)]
struct NoteBody {
    text: String,
}

#[route(POST "/notes/:id")]
fn create_note(tx: &mut Tx, id: u64, body: NoteBody) -> Result<u64, ApiError> {
    tx.put(&Note {
        id: Id::from_raw(id),
        text: body.text,
    });
    Ok(id)
}

#[route(GET "/notes/:id")]
fn get_note(tx: &mut Tx, id: Id<Note>) -> Result<String, ApiError> {
    Ok(tx.get(id)?.ok_or(ApiError::NotFound)?.text)
}

fn main() -> std::io::Result<()> {
    let app = App::open(
        Box::new(ProdDisk::new("./data")?),
        Schema::new().table::<Note>(),
        Box::new(ProdEntropy::new()?),
        AppConfig::default(),
    )?
    .mount(routes![create_note, get_note]);

    charpente::serve::serve("0.0.0.0:8080", app)?;

    // `serve` already returned a running server; park the main thread to
    // keep the process alive for it (no graceful shutdown in v0).
    loop {
        std::thread::park();
    }
}
```

### What `#[route]` rejects

Every rejection is a compile error naming the contract and the fix:

```rust,ignore
// first argument isn't named `tx`
#[route(GET "/ping")]
fn ping(ctx: &Ctx) -> Result<&'static str, ApiError> {
    Ok("pong")
}
// "a #[route] handler's first argument must be `tx: &mut Tx` — `ping` is
//  missing it"

// argument count doesn't match the :param count
#[route(GET "/notes/:id")]
fn get_note(tx: &mut Tx) -> Result<String, ApiError> {
    Ok(String::new())
}
// "`get_note` takes 0 argument(s) after `tx` (excluding `ctx`) but the path
//  \"/notes/:id\" has 1 :param(s) — write one argument per :param (same
//  name, path order) plus at most one JSON body argument"

// argument name doesn't match the :param name
#[route(GET "/notes/:id")]
fn get_note(tx: &mut Tx, key: u64) -> Result<String, ApiError> {
    Ok(String::new())
}
// "this argument must be named `id` to match :id in \"/notes/:id\" — got
//  `key`"

// route path isn't a string literal, or doesn't start with '/', or names
// an unknown method — each fails at the same macro-argument-parsing step
// with its own named message (e.g. "route path must start with '/' — got
// \"notes\"", "unknown method PUSH — one of GET, POST, PUT, PATCH, DELETE, SSE").
```

## 6. Jobs

Jobs ride the same kernel loop as HTTP requests (§5's `charpente::serve`
starts it): mounting a job or a schedule needs no separate worker process
or entry point.

### `#[job]` and its retry policy

`#[job]` derives a job payload's `Codec`, its `NAME`, and the `JobEntry`
glue `jobs![...]` collects — the same shape `#[table]` derives for rows
(§2), minus the primary key. Bare `#[job]` takes `RetryPolicy::DEFAULT` (5
attempts, 30s base backoff); `#[job(retry(max = <int>, backoff =
"<n><unit>"))]` overrides either or both — `backoff` is a bare number plus
a unit, one of `ms`, `s`, `m`, `h`.

```rust
use charpente::core::job::RetryPolicy;
use charpente::kernel::JobError;
use charpente::{Job, Tx, job};

#[job]
struct Basic {}

impl Basic {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

#[job(retry(max = 3, backoff = "10s"))]
struct Custom {}

impl Custom {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

fn retry_policies() -> (RetryPolicy, RetryPolicy) {
    (Basic::RETRY, Custom::RETRY) // (5, 30s), (3, 10s)
}
```

### The handler signature

`#[job]` does not derive a handler — you write one yourself, an inherent
method with exactly this shape: `fn run(self, tx: &mut Tx) -> Result<(),
JobError>`. It runs as its own transaction, so it reads and writes through
`tx` exactly like a route handler or the closure in §4's `db.transact`.
`JobError: From<io::Error>` (mapping to `JobError::Io`), so `?` works on
`tx` reads the same way it does with `ApiError`:

```rust
use charpente::core::table::Id;
use charpente::kernel::JobError;
use charpente::{Tx, View, job, table};

#[table]
struct Counter {
    id: Id<Counter>,
    hits: u64,
}

#[job]
struct CountUp {
    slot: u64,
}

impl CountUp {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        let hits = tx.get(Id::<Counter>::from_raw(self.slot))?.map_or(0, |c| c.hits);
        tx.put(&Counter {
            id: Id::from_raw(self.slot),
            hits: hits + 1,
        });
        Ok(())
    }
}
```

### Registering jobs: `jobs![...]` and `mount_jobs`

Like `routes![...]`, `jobs![...]` collects descriptors explicitly — a job
type only dispatches once it's in this list, passed to
`App::mount_jobs(...)`:

```rust
use charpente::core::table::Id;
use charpente::http::{App, AppConfig};
use charpente::kernel::JobError;
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::{Schema, Tx, View, job, jobs, table};

#[table]
struct Counter {
    id: Id<Counter>,
    hits: u64,
}

#[job(retry(max = 3, backoff = "10s"))]
struct CountUp {
    slot: u64,
}

impl CountUp {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        let hits = tx.get(Id::<Counter>::from_raw(self.slot))?.map_or(0, |c| c.hits);
        tx.put(&Counter {
            id: Id::from_raw(self.slot),
            hits: hits + 1,
        });
        Ok(())
    }
}

fn build_app(data_dir: &str) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(ProdDisk::new(data_dir)?),
        Schema::new().table::<Counter>(),
        Box::new(ProdEntropy::new()?),
        AppConfig::default(),
    )?
    .mount_jobs(jobs![CountUp]))
}
```

Mounting the same job type twice — in one `mount_jobs` call, or across
several — panics at mount time: `"job \"CountUp\" registered twice — each
#[job] type is mounted once"`.

Like `routes![...]`, `jobs![...]` accepts path-qualified job types too —
`jobs![jobs::CountUp]` works exactly like `jobs![CountUp]` after
`use jobs::CountUp;`.

### Enqueueing: `enqueue` vs `enqueue_keyed`

From inside any transaction — a route handler, a job handler, a cron tick
— `tx.enqueue(job)` writes the job into that same committing log record;
`Db::transact`'s apply step materializes it as a row in the platform's
`charpente.jobs` table. `tx.enqueue_keyed(key, job)` does the same, plus
carries a key: the job's stable identity to the world **outside** the log
(the idempotency key an at-least-once external effect needs downstream —
M1's external-IO surface consumes it). Inside the deterministic boundary
the key means nothing in v0 — it is carried, never indexed or deduped on,
because exactly-once state effects here are structural (below), not
key-based. An empty key is a contract panic: `"contract: an idempotency
key names the job's external identity — it cannot be empty"`.

```rust
use charpente::core::table::Id;
use charpente::http::ApiError;
use charpente::kernel::JobError;
use charpente::{Tx, View, job, route, table};

#[table]
struct Invoice {
    id: Id<Invoice>,
    finalized: bool,
}

#[job]
struct SendReminder {
    invoice: u64,
}

impl SendReminder {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

#[route(POST "/invoices/:id/remind")]
fn remind(tx: &mut Tx, id: u64) -> Result<(), ApiError> {
    let mut inv = tx
        .get(Id::<Invoice>::from_raw(id))?
        .ok_or(ApiError::NotFound)?;
    inv.finalized = true;
    tx.put(&inv);
    tx.enqueue_keyed(format!("reminder:{id}"), SendReminder { invoice: id });
    Ok(())
}
```

### Retry, dead-letter, and exactly-once effects

The dispatcher runs the earliest-due job inline, on the kernel thread, as
its own transaction. On `Ok`, the handler's own effects (puts, deletes,
enqueues, emits) and the row's Done-mark commit **as one record** — there
is no window where the effects are durable but the Done-mark isn't, or
vice versa: exactly-once state effects, structurally.

On `Err(JobError::App(msg))` — your handler's own rejection, a panic inside
it (absorbed as `"handler panicked: <payload>"`), or a corrupt payload
(schema drift: `"job decode for <Name>: <e> — schema drift or corruption"`)
— that attempt's own effects never committed, and a *second* transaction
records the failure: `attempts` increments, `last_error` gets the message,
and:

- if `attempts < retry.max`: stays Pending, `due_at` moves to
  `now + backoff * 2^(attempts - 1)` (nanoseconds; doubles every attempt,
  saturating rather than overflowing at absurd attempt counts) — the
  policy's configured base `backoff` is attempt 1's wait.
- if `attempts >= retry.max`: status becomes Dead. Dead rows are retained,
  with `last_error` as the record of what killed them.

On `Err(JobError::Io(e))` — a platform fault the handler's own reads
surfaced, or one from the transaction machinery itself — none of this
happens: it is **not an attempt**. The row stays Pending, untouched, and
the error bubbles up (restart-and-recover, same as a request-path `Io`
failure in §5) instead of silently burning retry budget on a healthy job
that just hit a bad disk. `JobError::Io`'s own `Display` names this:
`"platform IO failure during the handler (not an attempt): <e>"`.

One more failure mode: if a `JobRow`'s `name` isn't in the `jobs![...]` list
`App::mount_jobs` was given — a job enqueued by a since-removed or renamed
type — it fails the same way as a handler rejection, with: `"job
\"<name>\" is not registered — add it to the jobs![…] list passed to
App::mount_jobs (or this log outlived a renamed job type)"`.

### What `#[job]` rejects

```rust,ignore
// unrecognized retry syntax
#[job(bogus)]
struct Foo {}
// "write `#[job]` or `#[job(retry(max = <int>, backoff = \"<n><unit>\"))]`
//  (unit is one of ms, s, m, h) — e.g. `#[job(retry(max = 3, backoff =
//  \"10s\"))]`"

// backoff with no unit suffix
#[job(retry(max = 3, backoff = "10"))]
struct Bar {}
// "backoff \"10\" has no unit — write `#[job]` or `#[job(retry(max =
//  <int>, backoff = \"<n><unit>\"))]` (unit is one of ms, s, m, h) — e.g.
//  `#[job(retry(max = 3, backoff = \"10s\"))]`"

// enums and unions cannot be job payloads
#[job]
enum Baz {
    A,
}
// "#[job] applies to structs — enums and unions cannot be job payloads"
```

## 7. Cron

Cron rides the same kernel loop as HTTP requests and jobs (§5, §6) — a
schedule is a row, and a due tick is one transaction that advances the
schedule and enqueues a job through the exact same path `tx.enqueue` uses:
a recurring enqueuer, inheriting retry and observability for free.

### Registering a schedule: `App::schedule`

`App::schedule(name: &'static str, every: Duration, make: impl Fn() -> J +
Send + 'static) -> App` registers one schedule; `make` builds the job
enqueued on each tick.

```rust
use charpente::core::table::Id;
use charpente::http::{App, AppConfig};
use charpente::kernel::JobError;
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::{Schema, Tx, View, job, jobs, table};
use std::time::Duration;

#[table]
struct DigestState {
    id: Id<DigestState>,
    sent: u64,
}

#[job]
struct SendDigest {}

impl SendDigest {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        let sent = tx.get(Id::<DigestState>::from_raw(0))?.map_or(0, |s| s.sent);
        tx.put(&DigestState {
            id: Id::from_raw(0),
            sent: sent + 1,
        });
        Ok(())
    }
}

fn build_app(data_dir: &str) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(ProdDisk::new(data_dir)?),
        Schema::new().table::<DigestState>(),
        Box::new(ProdEntropy::new()?),
        AppConfig::default(),
    )?
    .mount_jobs(jobs![SendDigest])
    .schedule("monthly-digest", Duration::from_secs(30 * 24 * 3600), || {
        SendDigest {}
    }))
}
```

Scheduling the same name twice panics at mount time, same shape as routes
and jobs:

```rust
use charpente::http::{App, AppConfig};
use charpente::kernel::JobError;
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::{Schema, Tx, job};
use std::time::Duration;

#[job]
struct Tick {}

impl Tick {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

// "cron schedule \"nightly\" scheduled twice — schedule each name once"
fn build_app(data_dir: &str) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(ProdDisk::new(data_dir)?),
        Schema::new(),
        Box::new(ProdEntropy::new()?),
        AppConfig::default(),
    )?
    .schedule("nightly", Duration::from_secs(3600), || Tick {})
    .schedule("nightly", Duration::from_secs(7200), || Tick {}))
}
```

An interval that overflows u64 nanoseconds (~584 years — not a realistic
schedule, but checked) panics too, with `"cron schedule \"<name>\":
interval <duration> exceeds u64 nanos (~584 years) — pick a smaller
interval"` — but unlike the duplicate-name panic, not synchronously at the
`.schedule(...)` call: `App::schedule` stores the `Duration` unconverted,
and the conversion only happens at kernel startup. For a `serve()`-based
app that startup runs on the spawned kernel thread, so `serve` has already
returned `Ok` when the panic takes the kernel down.

### Ticks are transactions

Nothing about a tick is special: `CronRow` (`id`, `#[index] name`,
`interval_nanos`, `next_due`) is a platform-registered row like `Session`,
and a due tick opens a transaction that reads that row, advances
`next_due` by one interval, and calls your `make()` job's `tx.enqueue` —
all committed together. A schedule's phase survives process restarts: its
row is upserted (not reset) at startup — `App::open` only registers the
table; the upsert itself happens when the kernel starts.

### Catch-up fires once, never backfills

If the process is down across more than one whole interval — say an
hourly schedule misses five hours of ticks — the next tick still only
enqueues **one** job, and `next_due` jumps to `now + interval`: a single
step past the present, not five queued catch-up runs for the five missed
hours. Backfill is a deliberate non-goal.

### Virtual time in tests

Because the simulator's clock is virtual, a schedule's real-world interval
costs nothing extra to exercise exhaustively: driving a monthly cron
schedule through ten simulated years (121 ticks) runs in well under a
second of wall-clock time. Nothing about cron's tick behavior depends on
wall-clock speed — §8 shows the machinery (a scripted kernel under virtual
time, in "Driving the whole app in a sim") and `charpente test --sim`, the
runner.

### A logical clock: reading "now" inside a transaction

§4 was explicit that a transaction closure receives only `&mut Tx` — no
`Clock` reaches inside it. That is by design (a wall-clock read inside a
transaction is exactly the ambient nondeterminism the platform exists to
remove), but it leaves a real question open: how does an app express
"overdue," or any other time-relative state, at all?

There is no platform primitive for this in v0 — the pattern is to build
one the same way cron itself is built: a singleton counter row, advanced
by exactly one cron-ticked job, is the app's own logical clock. Because a
cron tick is already one transaction, "what time is it" becomes an
ordinary table read instead of a call to an ambient clock, and the clock
inherits cron's catch-up rule (above) for free — a tick after downtime
still advances the counter by exactly one step, never backfilling every
missed interval, which is exactly the semantics most "advance the current
period" logic wants anyway.

```rust
use charpente::core::table::Id;
use charpente::http::{App, AppConfig};
use charpente::kernel::JobError;
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::{Schema, Tx, View, job, jobs, table};
use std::time::Duration;

/// Singleton row (fixed id 0): the app's own day counter. Nothing but
/// `AdvanceDay::run` ever writes it, and it only ever adds one.
#[table]
struct AppClock {
    id: Id<AppClock>,
    day: u64,
}

const APP_CLOCK_ID: u64 = 0;

#[job]
struct AdvanceDay {}

impl AdvanceDay {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        let clock_id = Id::<AppClock>::from_raw(APP_CLOCK_ID);
        let mut clock = tx.get(clock_id)?.unwrap_or(AppClock {
            id: clock_id,
            day: 0,
        });
        clock.day += 1;
        tx.put(&clock);
        // `clock.day` is now this transaction's only source of "today" —
        // e.g. an invoice with `due_day <= clock.day` is overdue, computed
        // from a table read, never from `SystemTime::now()`.
        Ok(())
    }
}

fn build_app(data_dir: &str) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(ProdDisk::new(data_dir)?),
        Schema::new().table::<AppClock>(),
        Box::new(ProdEntropy::new()?),
        AppConfig::default(),
    )?
    .mount_jobs(jobs![AdvanceDay])
    .schedule("advance-day", Duration::from_secs(24 * 3600), || {
        AdvanceDay {}
    }))
}
```

Any row that needs a "created at" in this same app-relative space stores
the `AppClock` reading at write time (`clock.day`, read like any other
row) rather than a timestamp. There is no wall-clock equivalent in v0 —
an app that genuinely needs real-world timestamps (an audit log callers
expect in UTC, say) has no sanctioned source for one yet.

### Putting it together

Routes, sessions, jobs, and a cron schedule all mount onto the same `App`;
one call to `charpente::serve::serve` starts all of it:

```rust
use charpente::core::table::Id;
use charpente::http::{ApiError, App, AppConfig, Ctx};
use charpente::kernel::JobError;
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::{Schema, Tx, View, job, jobs, route, routes, table};
use serde::Deserialize;
use std::time::Duration;

#[table]
struct Note {
    id: Id<Note>,
    text: String,
}

#[table]
struct DigestState {
    id: Id<DigestState>,
    sent: u64,
}

#[derive(Deserialize)]
struct NoteBody {
    text: String,
}

#[job]
struct SendDigest {}

impl SendDigest {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        let sent = tx.get(Id::<DigestState>::from_raw(0))?.map_or(0, |s| s.sent);
        tx.put(&DigestState {
            id: Id::from_raw(0),
            sent: sent + 1,
        });
        Ok(())
    }
}

#[route(POST "/notes/:id")]
fn create_note(tx: &mut Tx, id: u64, body: NoteBody, ctx: &Ctx) -> Result<u64, ApiError> {
    let _ = ctx;
    tx.put(&Note {
        id: Id::from_raw(id),
        text: body.text,
    });
    Ok(id)
}

#[route(GET "/notes/:id")]
fn get_note(tx: &mut Tx, id: Id<Note>) -> Result<String, ApiError> {
    Ok(tx.get(id)?.ok_or(ApiError::NotFound)?.text)
}

fn main() -> std::io::Result<()> {
    let app = App::open(
        Box::new(ProdDisk::new("./data")?),
        Schema::new().table::<Note>().table::<DigestState>(),
        Box::new(ProdEntropy::new()?),
        AppConfig::default(),
    )?
    .mount(routes![create_note, get_note])
    .mount_jobs(jobs![SendDigest])
    .schedule("monthly-digest", Duration::from_secs(30 * 24 * 3600), || {
        SendDigest {}
    });

    charpente::serve::serve("0.0.0.0:8080", app)?;
    loop {
        std::thread::park();
    }
}
```

A pure background worker — jobs and cron, no routes of its own — mounts
zero routes and still calls `serve`: v0's only entry point binds an HTTP
listener alongside the job/cron loop (unrouted requests just 404). There
is no separate headless-worker entry point yet.

## 8. Sim testing

Everything in §1–§7 routes ambient IO through `Disk`, `Entropy`, and
`Clock` instead of touching the OS directly — that is the whole reason
`charpente-sim` can exist. `charpente_sim::Sim` implements all three traits
against one seed: a `u64` that determines virtual time, every entropy draw,
and a fault matrix (write errors, sync errors, crashes between any two disk
operations, torn writes). The whole app is what the simulator drives:
requests through the same `App::handle` (§5), jobs and cron through the
same kernel loop (§6–§7) production runs — see "Driving the whole app in a
sim" below. Your handlers run completely unmodified; nothing about your app
becomes "testable" specially, because it was already built on the traits.
The product's core promise: the same seed always produces the same
universe, so a failure a workload returns is not just a bug report — it is
a one-command reproduction, forever.

### `tests/sim.rs`

§1 already set up the pieces: `charpente-sim` as a `dev-dependency`, and a
`[[test]] name = "sim", harness = false` target. `harness = false` matters
here — it tells Cargo not to link the default libtest harness into that
binary, so `charpente_sim::sim_tests! { ... }` can supply its own `main`
instead, one that knows how to run many seeded universes and report on
them, rather than running a fixed list of `#[test]` functions once each.

A workload is a plain function, `fn(&mut Sim) -> Result<(), String>`: `Ok(())`
means the universe passed; `Err(message)` means it didn't, and so does a
panic (both turn into the same kind of failure record). `sim_tests!` lists
the workloads to run by name, each optionally followed by `require [...]` —
sometimes-coverage keys (`Sim::cover`, plus the sim's own auto-covered
`fault:*`/`crash:*`/`restart` keys) that must be hit at least once across
that test's universes in a batch run, or the batch fails even if every
universe individually passed. A `require`d key that never fires is telling
you something real: either the workload never drives the system into that
state, or the universe count is too low to make it likely.

```rust
use charpente::{Db, DbConfig, Id, Schema, View, table};
use charpente_sim::Sim;
use charpente_sim::checks::{is_fault_fallout, replays_identically};

#[table]
struct Counter {
    id: Id<Counter>,
    hits: u64,
}

// This is the shape of `tests/sim.rs`'s top level — the macro generates the
// binary's `fn main`, wiring these two workloads (and their `require` keys,
// if any) into the harness's arg parsing and reporting.
charpente_sim::sim_tests! {
    counter_roundtrip require ["restart"],
    counter_replays,
}

/// `sim_tests!` workloads return `Result<(), String>` — this one delegates
/// to a helper that returns the interesting value instead, so the
/// determinism checker below can reuse the exact same logic.
fn counter_roundtrip(sim: &mut Sim) -> Result<(), String> {
    counter_roundtrip_result(sim).map(|_| ())
}

/// `replays_identically` is the determinism oracle: it runs `workload` twice
/// on two fresh `Sim`s seeded identically and demands both the return value
/// and the durable end-state match byte-for-byte.
fn counter_replays(sim: &mut Sim) -> Result<(), String> {
    replays_identically(sim.seed(), counter_roundtrip_result)
}

/// Puts a row, restarts the simulated disk (crashes and reopens), and reads
/// it back. Returns `None` — not a failure — when there is nothing to
/// assert: the write was never acked, or either `Db::open` hit fault
/// fallout (the harness's own fault paths, not this workload's contract,
/// are under test elsewhere). `is_fault_fallout` recognizes the sim's own
/// injected-fault wording, the WAL/engine poison wording, and redb's
/// "close and re-open the database" latch — anything else is treated as a
/// real bug and fails the universe.
fn counter_roundtrip_result(sim: &mut Sim) -> Result<Option<u64>, String> {
    let schema = || Schema::new().table::<Counter>();
    let mut db = match Db::open(Box::new(sim.disk()), DbConfig::default(), schema()) {
        Ok(db) => db,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(None),
        Err(e) => return Err(e.to_string()),
    };
    let acked = db
        .transact(|tx| {
            tx.put(&Counter {
                id: Id::from_raw(1),
                hits: 1,
            });
            Ok::<_, std::io::Error>(())
        })
        .is_ok();

    sim.restart(); // power-cycles the simulated disk: unsynced writes tear or vanish
    drop(db); // the old handle is dead after restart — reopen, per its own contract

    let db = match Db::open(Box::new(sim.disk()), DbConfig::default(), schema()) {
        Ok(db) => db,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(None),
        Err(e) => return Err(e.to_string()),
    };
    if !acked {
        return Ok(None);
    }
    let snap = db.snapshot().map_err(|e| e.to_string())?;
    match snap.get::<Counter>(Id::from_raw(1)).map_err(|e| e.to_string())? {
        Some(row) => Ok(Some(row.hits)),
        None => Err("acked Counter row lost across a crash".to_string()),
    }
}
```

### The checkers

`charpente_sim::checks` ships six reusable oracles — reach for these
before hand-rolling the same logic a fourth time:

- `is_fault_fallout(text: &str) -> bool` — classifies an error's *text*:
  is this the harness's own injected fault (or its consequences) rather
  than a bug your workload should fail on? It recognizes the sim's
  injected-fault wording (including the `read_error` axis), the WAL/engine
  poison wording, the WAL's durable-horizon hard error (bit-level corruption
  of an already-acknowledged frame — a fault-matrix consequence, not a bug),
  and redb's "close and re-open the database" latch. Used inside a workload
  to turn "the disk broke, as instructed" into "no answer this round" rather
  than a hard failure — never to swallow anything else. (`misdirected_write`
  is not in that list and cannot be: it returns `Ok`, so it has no wording —
  what you eventually see is the corrupt frame it left behind.)
- `classify(text: &str) -> FaultReason` — the same judgement, typed:
  `Injected`, `Poisoned`, `RedbLatch`, `DurableHorizon`, or `Unrelated`
  (`is_fault_fallout` is `!matches!(…, Unrelated)`). Use it when your
  workload wants to accept some families and fail on others — e.g. "an
  injected fault or a durable-horizon error is fallout this round, but a
  poisoned engine is a finding". Matching on a variant instead of on
  substrings keeps the prose coupling in one place: if a platform error
  string is ever reworded, `classify` is the single site that must change,
  and it fails by name instead of leaking a wrong classification into
  whichever oracle saw the text first.
- `replays_identically(seed: u64, workload: impl Fn(&mut Sim) -> Result<T, String>) -> Result<(), String>`
  — the determinism oracle. Runs `workload` twice, each on its own fresh
  `Sim::new(seed)`, and fails if the returned value or the durable
  end-state (`Sim::durable_fingerprint`) diverge. It always constructs
  plain `Sim::new` worlds — never a *swarm* `FaultConfig` (swarm testing:
  the harness samples each universe's fault rates and latency ranges from
  its own seed, so a batch explores many fault mixes instead of every
  universe sharing one fixed matrix) — so it re-runs the workload's logic
  under default fault rates rather than replaying the exact fault schedule
  of the swarm universe that called it.
- `acked_implies_durable(seed: u64, rounds: u32, drive: impl FnMut(&mut Sim) -> Result<Vec<String>, String>, verify: impl FnMut(&mut Sim, &[String]) -> Result<(), String>) -> Result<(), String>`
  — the ack-durability property, generically: `rounds` times, `drive` does
  one round of work and returns the ids it promised are durable (classifying
  its own faults with `is_fault_fallout`, same as above), the sim restarts,
  and `verify` checks every id acked *so far* survived. A failure names the
  round it broke on.
- `durable_corruption_fired(sim: &Sim) -> bool` — true when a
  durable-corruption fault (`bit_flip` or `misdirected_write`) actually
  fired in this universe. Those axes can corrupt an acked WAL record above
  the store's checkpoint horizon, and recovery then legitimately cuts it
  (an invalid frame is a torn tail unless provably below the horizon — the
  standard WAL convention; the un-checkpointed data had one local copy).
  Route an acked-durability check's *shortfall* through this so the loss is
  excused only in universes where corruption was actually injected — the
  strong oracle stands everywhere else. Never use it to excuse wrong data
  or a consistency violation (a cut reverts state to a prefix, which stays
  consistent). Note this is the *single-node* excuse: a workload with a
  converged follower keeps none of it, because a record the primary lost to
  media corruption still lives on the follower.
- `dir_entry_loss_fired(sim: &Sim) -> bool` — the same fire-time-marked
  discipline for the `dir_entry_loss` axis (a pending directory create
  vanished, or a pending removal resurrected). A workload that routes
  `Db::open`'s "a segment is missing … refusing to guess" to a restore path
  must do so *only* when this returns true: the platform never leaves a
  durable directory gap on its own, so a gap in a universe where the axis
  never fired is a real regression and must fail loud.

```rust,ignore
pub fn classify(text: &str) -> FaultReason   // Injected | Poisoned | RedbLatch
                                             // | DurableHorizon | Unrelated

pub fn is_fault_fallout(text: &str) -> bool

pub fn durable_corruption_fired(sim: &Sim) -> bool

pub fn dir_entry_loss_fired(sim: &Sim) -> bool

pub fn replays_identically<T: PartialEq + std::fmt::Debug>(
    seed: u64,
    workload: impl Fn(&mut Sim) -> Result<T, String>,
) -> Result<(), String>

pub fn acked_implies_durable(
    seed: u64,
    rounds: u32,
    drive: impl FnMut(&mut Sim) -> Result<Vec<String>, String>,
    verify: impl FnMut(&mut Sim, &[String]) -> Result<(), String>,
) -> Result<(), String>
```

A worked `acked_implies_durable` example — a toy that writes one byte per
round and only claims the ack once its `sync` returns `Ok`:

```rust
use charpente::core::io::Disk;
use charpente_sim::Sim;
use charpente_sim::checks::{acked_implies_durable, is_fault_fallout};

/// Returns a fresh `drive` closure: each call performs one round, writing
/// the next byte and syncing before claiming its ack — never before.
fn drive() -> impl FnMut(&mut Sim) -> Result<Vec<String>, String> {
    let mut round: u64 = 0;
    move |sim: &mut Sim| {
        let file = match sim.disk().open("bytes") {
            Ok(f) => f,
            Err(e) if is_fault_fallout(&e.to_string()) => return Ok(vec![]),
            Err(e) => return Err(e.to_string()),
        };
        if let Err(e) = file.write_at(round, &[0xAA]) {
            let msg = e.to_string();
            return if is_fault_fallout(&msg) { Ok(vec![]) } else { Err(msg) };
        }
        if let Err(e) = file.sync() {
            let msg = e.to_string();
            return if is_fault_fallout(&msg) { Ok(vec![]) } else { Err(msg) };
        }
        let id = format!("byte-{round}");
        round += 1;
        Ok(vec![id])
    }
}

fn verify(sim: &mut Sim, acked: &[String]) -> Result<(), String> {
    let file = sim.disk().open("bytes").map_err(|e| e.to_string())?;
    let len = file.len().map_err(|e| e.to_string())?;
    for id in acked {
        let offset: u64 = id
            .strip_prefix("byte-")
            .expect("ack ids are \"byte-N\"")
            .parse()
            .expect("ack ids carry a numeric offset");
        if offset >= len {
            return Err(format!("{id} missing after restart (file len {len})"));
        }
    }
    Ok(())
}

fn bytes_survive_restarts(sim: &mut Sim) -> Result<(), String> {
    acked_implies_durable(sim.seed(), 5, drive(), verify)
}
```

### Driving the whole app in a sim

The workloads above drive `Db` directly. The full app — routes, jobs, and
cron multiplexed on one loop — is just as drivable, because the loop itself
(`charpente::kernel::Kernel`) sits inside the deterministic boundary:
`charpente::serve` (§5) runs it against hyper's channel in production; a
sim test runs the identical loop code against a scripted timeline. Three
pieces:

- **Requests** are built with the public builder: `Request::new(Method::Post,
  "/hit")`, then `.header(key, value)` (keys are lowercased for you) and
  `.json(&value)` for a JSON body. An authenticated request carries its
  session token (§5) the same way production traffic does:
  `.header("authorization", format!("Bearer {token}"))`.
- **`SimSource`** is the scripted request source.
  `SimSource::new(Box::new(sim.clock()), horizon_nanos)` bounds the
  universe at a virtual-time horizon; `push_at(at_nanos, request)`
  schedules arrivals (in arrival order — pushing out of order is an
  assert); `responses()` hands back a shared handle to the recorded
  `(status, body)` pairs, filled in as the kernel replies.
- **`Kernel::new(app, Box::new(sim.clock()))`** consumes the mounted `App`
  (upserting cron rows and rebuilding the job due-heap — §6, §7);
  `kernel.run(&mut source)` then serves scripted requests at their virtual
  arrival times, runs due jobs, and ticks cron, exactly as in production —
  and since sleeping through an empty stretch just advances the virtual
  clock, a two-hour universe finishes in milliseconds. `run` returns
  `Ok(())` once nothing is left inside the horizon, `Err` if the engine
  poisoned mid-run (§9). Afterwards, `kernel.stats()` snapshots the loop's
  counters (`requests`, `jobs_done`, `jobs_retried`, `jobs_dead`,
  `cron_ticks`, `cron_catchups`) and `kernel.into_app()` hands the `App`
  back — `app.db().snapshot()` reads the durable end-state.

One mindset note before the example: under `charpente test --sim`, every
universe gets its own swarm-sampled fault matrix, so exact-trace assertions
("exactly 3 requests, exactly 5 jobs done") will fail whenever a fault
fires. Assert *invariants* — claims that hold under any fault schedule —
and classify operational failures with `is_fault_fallout`, exactly like the
`Db`-level workload above. (For an exact-trace test, build your own
fault-free world inside the workload: `Sim::with_faults(seed,
FaultConfig::none())`.)

```rust
use charpente::core::job::{JobRow, JobStatus};
use charpente::core::table::{Codec, Id};
use charpente::http::{ApiError, App, AppConfig, Method, Request};
use charpente::kernel::{JobError, Kernel, SimSource};
use charpente::{Schema, Tx, View, job, jobs, route, routes, table};
use charpente_sim::Sim;
use charpente_sim::checks::is_fault_fallout;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

const SEC: u64 = 1_000_000_000; // SimSource timestamps are virtual nanoseconds

#[table]
struct Counter {
    id: Id<Counter>,
    hits: u64,
}

#[job]
struct CountUp {
    slot: u64,
}

impl CountUp {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        let hits = tx.get(Id::<Counter>::from_raw(self.slot))?.map_or(0, |c| c.hits);
        tx.put(&Counter {
            id: Id::from_raw(self.slot),
            hits: hits + 1,
        });
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct HitBody {
    slot: u64,
}

#[route(POST "/hit")]
fn hit(tx: &mut Tx, body: HitBody) -> Result<u64, ApiError> {
    tx.enqueue(CountUp { slot: body.slot });
    Ok(body.slot)
}

fn build_app(sim: &Sim) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![hit])
    .mount_jobs(jobs![CountUp])
    .schedule("hourly-count", Duration::from_secs(3600), || CountUp {
        slot: 777,
    }))
}

/// The end-state oracle, run after the universe. Two invariants that hold
/// under ANY fault schedule:
///
/// - Ack-after-durable, app-level: every 200 from `/hit` committed its
///   enqueue in the same log record, so at least `acked` request-slot
///   CountUp rows exist. `>=`, not `==`: a request that got a 500 for a
///   platform Io failure is ambiguous (§4) — its record may have landed
///   anyway.
/// - Exactly-once (§6), checked from the outside: per slot, counter hits
///   ≡ Done job rows — a handler's effects and its Done-mark commit as one
///   record, so no fault schedule can split them.
fn check_invariants(app: &App, acked: u64) -> Result<(), String> {
    let snap = app.db().snapshot().map_err(|e| e.to_string())?;
    let mut request_jobs = 0u64;
    let mut done: BTreeMap<u64, u64> = BTreeMap::new();
    for job in snap.scan::<JobRow>().map_err(|e| e.to_string())? {
        if job.name != "CountUp" {
            continue;
        }
        let slot = u64::decode(&mut &job.payload[..]).map_err(|e| e.to_string())?;
        if slot < 3 {
            request_jobs += 1; // enqueued by a request, not by cron's slot 777
        }
        if job.status == JobStatus::Done {
            *done.entry(slot).or_insert(0) += 1;
        }
    }
    if request_jobs < acked {
        return Err(format!(
            "ack-durability violated: {acked} 200s acked but only {request_jobs} request jobs durable"
        ));
    }
    for counter in snap.scan::<Counter>().map_err(|e| e.to_string())? {
        let done_count = done.get(&counter.id.raw()).copied().unwrap_or(0);
        if counter.hits != done_count {
            return Err(format!(
                "exactly-once violated at slot {}: {} hits vs {done_count} Done jobs",
                counter.id.raw(),
                counter.hits
            ));
        }
    }
    Ok(())
}

/// One whole-app universe: three requests arrive over three virtual
/// seconds, each enqueueing a job; an hourly cron schedule ticks twice
/// before the two-hour horizon; then the durable end-state is checked.
fn whole_app(sim: &mut Sim) -> Result<(), String> {
    let app = match build_app(sim) {
        Ok(app) => app,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };

    let mut source = SimSource::new(Box::new(sim.clock()), 2 * 3600 * SEC + SEC);
    for slot in 0..3u64 {
        source.push_at(
            slot * SEC,
            Request::new(Method::Post, "/hit").json(&HitBody { slot }),
        );
    }
    let responses = source.responses();

    let mut kernel = match Kernel::new(app, Box::new(sim.clock())) {
        Ok(kernel) => kernel,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    // The same event loop `serve` runs in production. An `Err` here means
    // the engine poisoned mid-run — under an injected fault schedule that
    // is fallout, not a bug.
    if let Err(e) = kernel.run(&mut source)
        && !is_fault_fallout(&e.to_string())
    {
        return Err(e.to_string());
    }

    let stats = kernel.stats();
    if stats.requests > 0 {
        sim.cover("kernel:served"); // pair with `require ["kernel:served"]`
    }
    let acked = responses
        .lock()
        .map_err(|_| "responses lock poisoned".to_string())?
        .iter()
        .filter(|(status, _)| *status == 200)
        .count() as u64;

    let app = kernel.into_app();
    match check_invariants(&app, acked) {
        Ok(()) => Ok(()),
        // The oracle's own reads go through the same faulted disk (e.g. the
        // store can latch after an earlier injected write error): fallout
        // ends the universe with nothing to assert; anything else is real.
        Err(msg) if is_fault_fallout(&msg) => Ok(()),
        Err(msg) => Err(msg),
    }
}
```

Register it like any workload — add `whole_app require ["kernel:served"]`
to the `sim_tests! { ... }` list. Nothing limits a universe to one script,
either: `run` returns when its source is exhausted, and the same kernel can
be `run` again against a second `SimSource` — script a login phase, read
the token out of its recorded responses, then script the authenticated
phase with `.header("authorization", format!("Bearer {token}"))`.

### `charpente test --sim`

Run it from the app crate's own directory (or pass `--dir`):

```bash
charpente test --sim [--universes N] [--seed 0x…] [--master-seed 0x…] \
    [--filter S] [--jobs N] [--format human|json] [--dir PATH]
```

- `--universes N` — how many fresh seeds to run **per selected test**
  (default 256; must be at least 1 — a green run of zero universes proves
  nothing).
- `--seed 0x…` — bypass the batch entirely and run every selected test
  once, on this exact literal seed. Coverage `require`ments are skipped (one
  universe can't be expected to hit everything a batch would). This is the
  replay flag: it is exactly what a failure's own `repro:` line uses.
- `--master-seed 0x…` — pick where the batch's seed chain starts, instead
  of fresh OS entropy, for a byte-identical repeat of an entire batch
  (every test's every universe). Cannot be combined with `--seed`.
- `--filter S` — run only tests whose name contains `S`. Filtering happens
  **before** any seed is drawn, so a filtered run consumes a different (but
  still fully reproducible) prefix of the seed chain than an unfiltered one
  — this is why every individual failure's repro line carries its own
  universe seed rather than relying on the batch's master seed.
- `--jobs N` — how many universes to run in parallel (default: the host's
  available parallelism). Every universe's seed is pre-derived on the main
  thread, in registry order, before any work is handed out, so `--jobs 1`
  and `--jobs 8` run the identical seed sequence and produce byte-identical
  reports — worker scheduling is unobservable.
- `--format human|json` — human is the default; `json` emits one object
  with `master_seed`, `universes`, `failures` (each carrying its own `test`,
  `seed`, `master_seed`, `config`, `message`, and copy-pasteable `repro`
  command), `coverage`, `missing_coverage` (`{test, key}` pairs), and a
  trailing `summary`, for downstream tooling.
- `--dir PATH` — the app crate directory to build and run sim tests from
  (default: `.`). This flag is consumed by the CLI itself; every other flag
  passes straight through to the sim binary the CLI builds and execs.

Exit codes: **0** every universe of every selected test passed and every
`require`d coverage key was hit; **1** at least one universe failed, or a
coverage requirement went unmet — the run itself completed, but something
in it was red; **2** the run never got that far — a build failure, no
`tests/sim.rs` target found, an unrecognized flag, a filter matching no
tests, or the sim binary failing to launch or exiting by signal. Exit code
2 means "rerun once this is fixed," not "here is a reproducible bug."

### Reading a failure, replaying its seed

A human-format failure looks like this (the exact shape a pinned test in
`charpente-sim`'s harness checks byte-for-byte):

```text
sim failure in receipts_exactly_once: exactly-once violated: hits 2, done 1 for key 3
  seed:   0x8bee1752811ae873
  config: FaultConfig { write_error: 0.0, sync_error: 0.118, crash_apply: 0.002, crash_torn: 0.664668, dir_entry_loss: 0.0, disk_latency: (0ns, 0ns), net_latency: (0ns, 250ms) }
  repro:  charpente test --sim --filter receipts_exactly_once --seed 0x8bee1752811ae873
```

Four lines, each load-bearing:

- The first line is the workload's own `Err` message (or an absorbed panic
  payload) — the actual claim that failed.
- `seed` is the one `u64` that determined this entire universe: virtual
  time, every entropy draw, and the fault matrix's decisions.
- `config` is that universe's sampled `FaultConfig` — the fault rates and
  latency ranges actually in effect (per-universe swarm sampling, glossed
  under the checkers above). Reading it tells you what kind of trouble was
  injected — e.g. `crash_torn: 0.664668` here means torn writes were
  common in this universe.
- `repro` is not advice, it **is** the reproduction: paste that exact line
  and the same universe runs again, because `--seed` (above) replays a
  literal seed byte-identically. There is no separate "figure out how to
  reproduce this" step — the failure message already did that work.

The same discipline extends outward: `run_universes!`/`run_universes_swarm!`
(the frozen internal-suite macros the platform's own tests use) and
`charpente test --sim` share the same seed→behavior mapping, so
`CHARPENTE_SEED=0x… cargo test -p <pkg> --test <file>` and `charpente test
--sim --seed 0x…` are the same idea at two different layers: a failure's
seed is always enough, on its own, to see the exact same universe again.

### A note on the edge harness (platform dev tooling, not app surface)

The repo also carries `crates/edge-harness`, which drives real built binaries
over real TCP with process- and connection-level faults (`kill -9`,
SIGSTOP/SIGCONT, severed or delayed connections) to prove the wire-level
versions of three guarantees: acked-implies-durable across a kill and a
restart, zero loss over converged writes across `charpente promote`, and exact
`Last-Event-ID` SSE resume. **It is not part of your app's testing story and
there is nothing to adopt** — `charpente test --sim` above is the app-level
interface, and it is where your properties belong. Two things are worth
knowing anyway. First, the boundary: `kill -9` is not power loss (killing a
process does not drop the OS page cache), so that rig proves ack *ordering*
and end-to-end crash recovery through real hyper/TCP, while the simulator
above owns power-loss and torn-write durability — if you are reasoning about
what charpente's durability claims rest on, they rest on the sim. Second, it
makes no seed promise: a scenario's reproduction is a recorded script file, not
a seed, which is exactly why app-level properties belong in sim. Its
process-touching tests are `#[ignore]`d out of the ordinary sweep and run on
demand via `scripts/edge-harness.sh`.

## 9. Error index

A lookup, not a tutorial: find the message you're staring at (or its
stable prefix — noted where the real text carries a runtime value), read
what it means, and what to do. Every message below names its own fix
already (hard rule 5) — this index adds which subsystem produced it, since
that's what a bare string on a terminal doesn't tell you.

### `#[table]` shape violations (`charpente-macros/src/table.rs`, compile time)

Six ways `#[table]` rejects a struct — §2 walks through all of them on a
`Ping` example:

```text
missing `id: Id<Ping>` field — every #[table] row needs a typed primary key (add `id: Id<Ping>`)
the `id` field of a #[table] must be `Id<Ping>` — a typed id, never a bare integer
`id` is the primary key — it is already the fastest lookup; remove #[index]
#[table] needs named fields — `Ping` must be a braced struct with an `id: Id<Ping>` field
#[table] applies to structs — enums and unions cannot be rows
#[table] structs cannot be generic — a table is one concrete row shape; make `Ping` non-generic
```

In order: no `id` field at all — add one. `id` present but not `Id<Self>` —
type it. `#[index]` on `id` itself — remove it, `id` is already the primary
key. Not a braced struct with named fields (tuple/unit structs) — rewrite
as one. An enum or union — `#[table]` only applies to structs. A generic
struct — a table is one concrete row shape; monomorphize it yourself before
attaching `#[table]`.

One more, at runtime rather than macro-expansion time — corrupted or
schema-drifted bytes on disk fail to decode into a row:

```text
row decode: 3 trailing bytes after a Ping row — schema drift or corruption
```

Meaning: the byte layout on disk doesn't match the struct's current fields
— usually a field was added, removed, or reordered without a migration
(§2: fields encode in declaration order). Fix: don't reorder or remove
fields on a table with durable data; add new fields, or write a v1 schema
(nothing exists for that yet — v0 is one fixed layout per table type).

### `#[route]` argument-convention violations (`charpente-macros/src/route.rs`, compile time)

```text
a #[route] handler's first argument must be `tx: &mut Tx` — `ping` is missing it
`get_note` takes 0 argument(s) after `tx` (excluding `ctx`) but the path "/notes/:id" has 1 :param(s) — write one argument per :param (same name, path order) plus at most one JSON body argument
this argument must be named `id` to match :id in "/notes/:id" — got `key`
route path must start with '/' — got "notes"
unknown method PUSH — one of GET, POST, PUT, PATCH, DELETE, SSE
the route path must be a string literal — e.g. #[route(GET "/notes/:id")]
the only route flag is `auth` — write `#[route(GET "/path", auth)]`
#[route] handlers are free functions — no `self`
empty :param name in "/notes/:"
write `#[route(METHOD "/path")]` — e.g. #[route(GET "/notes/:id")]
```

§5's "What `#[route]` rejects" walks through the first three with full
examples. The rest, in order: the path literal doesn't start with `/`; the
method name isn't one of `GET`/`POST`/`PUT`/`PATCH`/`DELETE`/`SSE`; the path
isn't a string literal at all (e.g. a `const`); the trailing flag after the
path isn't `auth`; the handler takes `self` (route handlers are free
functions); a `:` segment has nothing after it; and the macro's own
argument list didn't parse as `METHOD "/path"` to begin with. Every one of
these is a compile error at the `#[route]` call site — fix the signature or
the pattern and it's gone.

### `#[job]` retry-syntax and shape violations (`charpente-macros/src/job.rs`, compile time)

```text
write `#[job]` or `#[job(retry(max = <int>, backoff = "<n><unit>"))]` (unit is one of ms, s, m, h) — e.g. `#[job(retry(max = 3, backoff = "10s"))]`
backoff "10" has no unit — write `#[job]` or `#[job(retry(max = <int>, backoff = "<n><unit>"))]` (unit is one of ms, s, m, h) — e.g. `#[job(retry(max = 3, backoff = "10s"))]`
unknown retry field `jitter` — write `#[job]` or `#[job(retry(max = <int>, backoff = "<n><unit>"))]` (unit is one of ms, s, m, h) — e.g. `#[job(retry(max = 3, backoff = "10s"))]`
`max` given twice — write it once
`backoff` given twice — write it once
backoff "12xs": "12x" is not a whole number — write `#[job]` or `#[job(retry(max = <int>, backoff = "<n><unit>"))]` (unit is one of ms, s, m, h) — e.g. `#[job(retry(max = 3, backoff = "10s"))]`
backoff "99999999999999h" overflows u64 milliseconds — write `#[job]` or `#[job(retry(max = <int>, backoff = "<n><unit>"))]` (unit is one of ms, s, m, h) — e.g. `#[job(retry(max = 3, backoff = "10s"))]`
#[job] applies to structs — enums and unions cannot be job payloads
#[job] structs cannot be generic — a job is one concrete payload shape; make `Baz` non-generic
#[job] needs named fields — `Baz` must be a braced struct
```

Anything other than bare `#[job]` or `#[job(retry(max = <int>, backoff =
"<n><unit>"))]` (unit one of `ms`, `s`, `m`, `h`) hits the first message —
it names the two accepted forms directly, so the fix is always "match one
of those two shapes." The rest are that same message with more specific
context prepended: a backoff string missing its unit, an unrecognized field
name inside `retry(...)`, `max`/`backoff` written twice, a backoff whose
number doesn't parse, or one so large it overflows `u64` milliseconds once
converted to it. The last three are the same struct-shape checks `#[table]`
makes (§2): only structs, only non-generic, only named fields.

Three more, at job-dispatch time rather than macro-expansion time
(`charpente/src/kernel/dispatch.rs` — covered behaviorally in §6's "Retry,
dead-letter, and exactly-once effects"):

```text
job decode for SendReceipt: <the underlying decode error> — schema drift or corruption
handler panicked: <the panic payload>
job "SendReceipt" is not registered — add it to the jobs![…] list passed to App::mount_jobs (or this log outlived a renamed job type)
```

The first: a `JobRow`'s stored payload no longer decodes as its named job
type — same schema-drift story as a table row, but for a job's fields.
Fix, same rule: don't reorder or remove a job payload's fields once jobs of
that type may exist Pending in the store. The second: your `run` method (or
something it called) panicked; charpente absorbs it as an ordinary handler
rejection (an attempt, counted against retry budget) rather than crashing
the kernel thread — fix the panic like any other bug in `run`. The third: a
`JobRow` in the store names a job type that isn't in the `jobs![...]` list
passed to `App::mount_jobs` — usually a job type was renamed or removed
without a matching change to old enqueued rows; add it back to the list, or
accept that those rows are now permanently unreachable.

One more from the same file, `JobError::Io`'s own `Display` — not a
rejection at all, the opposite: proof nothing was attempted, folded into
the poisoned-engine story below:

```text
platform IO failure during the handler (not an attempt): <e>
```

### Router mount conflict (`charpente/src/http/router.rs`, `App::mount` time)

```text
route conflict: GET /notes/:key (fn by_key) collides with GET /notes/:id (fn by_id) — two routes with the same method and shape can both match one path; change one pattern
```

Two mounted routes share a method and a shape — the same sequence of
literal-vs-`:param` segments, so both could match one real incoming path
(§5: a literal segment beats a `:param` at the same position instead of
conflicting, so `/notes/all` and `/notes/:id` are fine together; two
different `:param` names at the same position are not). This panics at
`.mount(...)` time, before the app ever serves a request — fix by changing
one of the two patterns so their shapes differ.

### Duplicate registration (`charpente/src/http/app.rs`, `charpente-core/src/db.rs`, mount time)

```text
job "CountUp" registered twice — each #[job] type is mounted once
cron schedule "nightly" scheduled twice — schedule each name once
Ctx::create_session/create_api_key called twice in one request — one credential mint per request
Schema: table "Counter" registered twice — register each #[table] type once
```

The first two are the same shape as the router conflict above, just for a
different registry: `App::mount_jobs([...])` (§6) and `App::schedule(...)`
(§7) panic the moment a name is reused. The third is the once-per-request
mint budget (§5's auth section): `create_session` and `create_api_key` share
one pre-transaction entropy draw, so a request that mints a second credential
of either kind panics. The fourth (§3) is `Schema::table::<R>()` — called
twice for the same `#[table]` type when building the `Schema` passed to
`Db::open`, including registering `Session`, `ApiKey`, or `charpente.jobs`
yourself (`App::open`/`Db::open` already register those). Fix in every case:
register that name/type exactly once, and mint at most one credential per
request.

### Call-site contract panics (`charpente-core/src/db.rs`, `charpente/src/prod.rs`, `charpente/src/kernel/cron.rs`)

```text
contract: an idempotency key names the job's external identity — it cannot be empty
Disk paths are relative flat names like "wal/000001.log" — got "<path>"
cron schedule "<name>": interval <duration> exceeds u64 nanos (~584 years) — pick a smaller interval
```

The first (§4, §6) is `tx.enqueue_keyed(key, job)` handed an empty `key` —
the key is the job's stable identity to the world outside the log, so an
empty one names nothing; pass a real identity (e.g. `format!("receipt:{id}")`)
or use plain `tx.enqueue` if the job has no external identity to protect.
The second (§1) is any `Disk` operation handed a path that is absolute,
empty, or contains a `.`/`..` segment — usually a hand-built `DbConfig`
`store_path`/`wal.dir`; fix: plain relative flat names only. The third
(§7) is a cron interval that overflows `u64` nanoseconds; note §7's timing
caveat — it fires at kernel startup, not at the `.schedule(...)` call, so
under `serve()` it takes the kernel thread down after `serve` has already
returned `Ok`.

### Record and body size limits (`charpente-core/src/db.rs`, `charpente-core/src/log.rs`, `charpente/src/http/app.rs`)

```text
transaction effects are 20000000 bytes but one log record holds at most 16777216 — split the work into smaller transactions
contract: WAL records are 1..=16777216 bytes — got 0
request body is 65 bytes but the cap is 64 — raise AppConfig.body_cap or send less
```

The first (§4) is `Db::transact`'s own assert: everything one transaction
buffers — every `put`/`delete`/`enqueue`/`emit` — must fit in one 16 MiB
(`1 << 24` byte) log record, checked right before the record is appended.
Fix: split the transaction into smaller ones. The second is the WAL's own
mirrored contract on `Wal::append` (`charpente-core::log::MAX_RECORD_BYTES`)
— the same ceiling, one layer lower; `Db::transact`'s assert fires first in
practice, since it checks the same limit before ever calling `append`. The
third (§5) is `AppConfig::body_cap` (default 1 MiB) rejecting an oversized
request body — this is what fences the first assert from ever seeing raw
network input; production also enforces the cap at the wire, before routing,
with its own near-identical 413 text (`"request body exceeds the N-byte cap
— raise AppConfig.body_cap or send less"`) — code exercised only through
`App::handle` (sim tests included) only ever sees the kernel-layer message
quoted above, since the wire check is a `charpente::serve`-only path.

### Schema / replay mismatches (`charpente-core/src/db.rs`)

```text
log record references table "widgets" but the Schema does not include it — register it in the Schema passed to Db::open, or migrate the log
```

The WAL holds a record naming a table that isn't in the `Schema` handed to
this `Db::open` call — almost always a `#[table]` type was removed from
your app (or its registration dropped) while old durable data still
references it by name. Fix: register the table again, at least until that
data is migrated away; §3 covers registration.

### Poisoned-engine errors (restart-and-recover)

```text
WAL poisoned by an earlier IO failure — durability is unknown and fsync cannot honestly be retried; restart and recover
engine poisoned: a durable record failed to apply, so machine state no longer matches the log — restart and recover
internal storage failure — the request may or may not have committed; retry after the server recovers: <e>
```

All three describe the same family: an IO failure landed somewhere the
platform cannot honestly shrug off. The first (`charpente-core/src/log.rs`)
is the WAL itself — any failed append or sync leaves the durability of
everything since the last good sync genuinely unknown ("fsyncgate"), so the
log refuses every further operation rather than guess. The second
(`charpente-core/src/engine.rs`) is the apply loop: a record synced
successfully (so it **is** durable) but the state machine then failed to
apply it, meaning in-memory state has diverged from the log — the only
honest response is to stop, not to keep applying against a machine that no
longer matches its own history. The third (`charpente/src/http/app.rs`,
§5's "Ack-after-durable") is how a poisoned engine surfaces to an HTTP
caller: `TxError::Io` after the handler's own logic already returned `Ok`,
so whether the write landed is genuinely unknown from the caller's side too
— the 500 says so instead of guessing. In every case the fix is the same:
restart the process. Recovery (`Db::open`/`Engine::recover`, §3) replays the
WAL from the last checkpoint and is itself idempotent, so restart-and-retry
is always the correct response to any of these three, never a retry loop
against the same live, poisoned handle. `Db::poisoned()` lets the kernel
loop check this directly — per the step-7 ruling, the request arm now stops
serving once a request's own 500 leaves the engine poisoned, rather than
keep accepting traffic against a dead engine.

The stop names *which* poison, because the two imply opposite things about
your data:

```text
kernel stopped: the engine is poisoned (a request-path IO failure made durability unknowable) — the request received its 500; restart the process to recover from the WAL
kernel stopped: the store handle is dead after a disk IO failure (redb refuses every further operation until you close and re-open the database) — durable state is intact and the WAL is authoritative; restart the process to recover
```

The second is the **store latch**, and it is worth understanding because the
message is reassuring on purpose. redb keeps its own independent "this handle
is broken" flag: any error from its storage backend sets it, after which every
later redb call fails with "close and re-open the database". The reachable
trigger is the store's own checkpoint fsync — `Store::apply` commits at
`Durability::None`, so the checkpoint is the only store sync charpente makes.
charpente observes that same condition at its own disk boundary and folds it
into `Db::poisoned()`, so the kernel stops exactly as it does for engine
poison.

Read the message literally: **committed data is safe.** A store latch is not a
durability failure. The WAL is intact and authoritative, and the write whose
checkpoint failed is already in it — that write got a 500, but it is on disk.
What died is the *handle*, and with it the node's ability to checkpoint at all,
which is why serving on would be wrong rather than merely untidy: WAL GC and
the `.horizon` breadcrumb both stall behind a checkpoint that can never
succeed again. The remedy is a process restart, which re-opens the database and
replays from the WAL (§3) — the same restart-and-recover exit as every other
error in this family.

The one thing a restart does not fix is the disk. The latch is a *symptom*:
some IO genuinely failed. Restarting on hardware that is still failing will
latch again, probably quickly. Treat a store-latch stop as a hardware signal
first and a charpente event second.

## 10. Replication

One primary accepts writes; followers replay its log and arrive at
byte-identical state (DESIGN §5 — replication is log-position arithmetic, not a
merge). It is asynchronous: a follower acks a record only after it has synced
it locally, and the primary's own ack to a client can be ahead of any
follower, so a failover can lose the records the primary acked but had not yet
shipped (the bounded-loss window, named in the `promote` reply). There is no
sharding and no multi-master — that stance is fixed.

### Configuring a node

A node's *effective* role lives in a durable role file on its data disk; the
`initial_role` in `ReplConfig` only seeds it on first boot, and a mismatch
between the two is surfaced on stderr (the file wins — config never silently
re-roles a node). `listen` is the node's replication address, a port distinct
from its HTTP port; a node without one cannot be promoted remotely.
`peer_secret` authenticates connections to this port: when `Some`, every
inbound `Hello`/control verb must present the same secret (hashed and
compared constant-time) or it is refused and the connection closed before any
state changes; `None` accepts anything inbound. A node with `peer_secret` set
always attaches it outbound, so a half-configured pair works in exactly one
direction — configure the followers first, then the primary (the reverse
would lock the not-yet-configured followers out). A refused inbound gets one
named message, before any state moves:

```text
peer secret missing or mismatched — set ReplConfig.peer_secret to match this node (or run the CLI verb with --secret)
```

`Some("")` is not a third option: an empty secret would admit any peer that
presents no proof at all — near-`None` inbound, while you believe auth is on —
which is easy to reach through an empty env var or a shell-quoting slip. It is
refused by name where `ReplConfig` is consumed, before any socket or state
work:

```text
ReplConfig.peer_secret is Some("") — an empty peer secret authenticates any peer that presents no proof, which is almost never what a configured secret intends; set a real secret, or use None to explicitly accept all peers (DESIGN §8)
```

and `--secret ''` on either CLI verb is the same usage error as omitting the
value (`error: --secret requires a value`, exit 2). If you mean "accept every
peer", write `None` and mean it.

The replication wire is plaintext TCP: `peer_secret` authenticates peers, it
does not encrypt the link (a private network is the deployment assumption;
transport encryption is a later milestone). An eavesdropper on the path can
read both the frames and the secret.

`serve::serve_replicated` is `serve` plus replication: same HTTP edge, but the
kernel also runs the replication engine. A `serve::backup` node runs the engine
with no HTTP edge at all — it only follows and keeps a promotable copy.

```rust
use charpente::http::App;
use charpente::prod::ProdDisk;
use charpente::repl::role::Role;
use charpente::repl::ReplConfig;
use charpente::serve;

fn run_primary(app: App) -> std::io::Result<()> {
    let cfg = ReplConfig {
        listen: Some("0.0.0.0:9090".to_string()),
        initial_role: Role::Primary,
        peer_secret: Some("shared-with-every-follower".to_string()),
        strict_witnesses: false,
    };
    // HTTP on 8080, replication on 9090. Returns once both are up.
    serve::serve_replicated("0.0.0.0:8080", app, Box::new(ProdDisk::new("./data")?), cfg)?;
    Ok(())
}

fn run_follower(app: App) -> std::io::Result<()> {
    let cfg = ReplConfig {
        listen: Some("0.0.0.0:9091".to_string()),
        initial_role: Role::Follower {
            primary: "primary-host:9090".to_string(),
        },
        peer_secret: Some("shared-with-every-follower".to_string()),
        strict_witnesses: false,
    };
    serve::serve_replicated("0.0.0.0:8080", app, Box::new(ProdDisk::new("./data")?), cfg)?;
    Ok(())
}

fn run_backup(app: App) -> std::io::Result<()> {
    let cfg = ReplConfig {
        listen: Some("0.0.0.0:9092".to_string()),
        initial_role: Role::Backup {
            primary: "primary-host:9090".to_string(),
        },
        peer_secret: Some("shared-with-every-follower".to_string()),
        strict_witnesses: false,
    };
    // A backup blocks on the current thread — no HTTP edge, no requests.
    serve::backup(app, Box::new(ProdDisk::new("./backup-data")?), cfg)
}
```

### What a follower does

A follower's kernel serves reads and ships its log, but never runs jobs or cron
(those are the primary's — the same store rows, jobs table and all, replicate to
the follower, so its `charpente.jobs` mirrors the primary's Done marks without
re-running anything). An effectful transaction on a follower — any route that
`put`s, `delete`s, `enqueue`s, or `emit`s — is refused with `TxError::NotPrimary`,
which the HTTP layer maps to **421** naming the primary to retry against:

```text
HTTP/1.1 421 Misdirected Request
this node is a read-only follower — writes go to the primary at
primary-host:9090; retry there (the request was not committed)
```

A read-only transaction (no effects) commits on a follower unchanged, so GET
routes work everywhere.

### What a restarting primary does

A node that was a durable primary before it restarted, and has not yet
confirmed that none of the followers it shipped to (its "witnesses") were
promoted while it was down, also refuses writes — but it cannot name a
primary the way a follower does, because it does not know whether one
exists elsewhere; it may still be the legitimate one. An effectful
transaction is refused with `TxError::Verifying`, which the HTTP layer maps
to **503** naming the witness set it is verifying:

```text
HTTP/1.1 503 Service Unavailable
this node is not accepting writes yet: it was a primary before restarting and has
not confirmed that its follower 127.0.0.1:7001 was never promoted — reads are being
served normally, and writes resume automatically once that node answers; if it is
gone for good, see GUIDE §10's witness-set remedy
```

Same as the 421, this is a read-side no-op — a read-only transaction (no
effects) still commits normally, the gate never touches reads — and it is
transient: writes resume automatically the moment every witness confirms it
was never promoted, with no operator action needed for a witness that
answers.

**Where the witness set comes from, and what the check can prove.** A witness
is a node this primary agreed to *ship* to; the addresses are recorded durably
as each shipping session is established, so a single-node primary and one that
never shipped have an empty set and are untouched by any of this — they resume
writable immediately and silently. On a restart with a non-empty set the node
asks each witness one question, "are you a primary now?", and:

- every witness answers *not a primary* → the gate opens and writes resume;
- a witness answers *as a primary* → it was promoted or handed off to while
  this node was down, so this node **refuses to serve at all** and says so,
  naming both remedies (re-seed this node from a fresh data dir if the other
  timeline is the one to keep; or stop the other node, clear this node's
  witness set, and restart it if these records are). It never demotes itself:
  it cannot know which timeline you want, and the writes it acked are not the
  platform's to discard;
- a witness cannot be reached, or answers something that says nothing about
  its role (a mismatched peer secret, protocol version, or schema digest — or
  bytes this node cannot decode at all, which is what you get when something
  other than a charpente node has taken over the address) →
  by default reads keep flowing, writes keep being refused, and after 30
  seconds (`WITNESS_DEADLINE_NANOS`) the node prints one notice on stderr
  naming what is still unresolved and why. It keeps probing; an unreachable
  witness that comes back resolves the gate on its own. Set
  `ReplConfig::strict_witnesses` to stop instead — see below.

The bound is worth stating plainly: this proves *none of the nodes I shipped
to is a primary now*, not *no other primary exists*. If the follower you
promoted was never in this node's witness set — it was promoted after a
switchover chain, say — this node resumes, and you are back to the hazard the
promote warning below describes.

A gated node is also still a replication peer: withholding writes does not
take it off the wire. It keeps accepting `Hello`s, keeps shipping records it
already holds, and will still seed a brand-new follower. A follower that first
connects during this window is recorded as a witness like any other — but it
joins neither the probe set nor the 503's list, both of which were fixed when
the gate closed, so it constrains the *next* restart rather than this one.

**Stopping instead of serving reads.** `ReplConfig::strict_witnesses` (default
`false`) changes the third outcome only. With it set, a node whose witnesses
are still unreached at the 30-second deadline stops rather than serving reads
it cannot write to. The other two outcomes are unaffected: every witness
answering *not a primary* still opens the gate, and one answering *as a
primary* still refuses regardless of this flag.

Two things about it are easy to get wrong. It is **not** "refuse to start":
whether a witness is unreachable is only established by probing, so a strict
node starts, serves reads for the whole 30-second window, and only then stops.
And **the stop reaches a supervisor under `serve::backup` only** — there the
error exits the process with a non-zero status, so a supervisor restarts it and
keeps restarting until a witness answers. Under `serve::serve_replicated` the
kernel runs on a spawned thread: it prints the error and the thread ends, but
the HTTP edge stays up and the process does not exit, so every subsequent
request gets `500 the kernel thread is gone` and nothing external notices.
Writes are fail-stopped either way; only the backup shape is loud. On an HTTP
node, set this flag only if something is watching for those 500s.

**The escape hatch, and its cost.** A follower that is permanently
decommissioned stays in the witness set forever, and a primary whose only
witness is gone for good will not accept writes until you intervene. Retention
is deliberately conservative — a decommissioned node *could* have been promoted
before it was decommissioned — so the exit is manual: stop the node and delete
both `witnesses.0` and `witnesses.1` from its data directory. Do that **only**
when you are certain no follower of this node was ever promoted: an empty
witness set lets it resume as a primary unchecked, which is exactly the gap the
set exists to close.

### Promotion and switchover

Two operator moves change which node is primary, both crash-safe on the durable
role file:

- **`promote`** is the bounded-loss failover: a follower or backup flips to
  primary and starts accepting writes immediately. Records the dead primary
  acked but had not shipped are lost — the reply names the `last_applied` it
  promoted at so the window is explicit.
- **`switchover`** is the zero-loss, planned handoff: the current primary
  write-stops and *drains* to the target follower, and only once the follower
  has acked every record does it hand off — no acked record is lost. If the
  target cannot catch up before the deadline, the switchover aborts and the
  primary resumes.

**After a `promote`, the old primary must be wiped before it is ever started
again.** Its durable role file still says `Primary`, and the durable role always
wins over config, so restarting it as it was configured brings up a *second*
primary again, on a timeline that has already diverged from the one you
promoted.

In the ordinary case the witness gate above catches exactly this — the promoted
node is one the old primary had shipped to, so it is in the witness set, and the
restarted box asks it, hears "I am a primary", and refuses to serve rather than
acking writes nobody will ever see again. **Do not lean on that instead of the
wipe.** The check is bounded by the witness set, so a node promoted after a
switchover chain (one this primary never shipped to) is invisible to it; the
verdict is a refusal to run, not a repair; and any write the resurrected box
acked before you noticed is still destroyed by the only remedy there is. So:
promote the follower, then treat the old box as a *new* node — stop it, clear
its data directory, and let it re-seed as a follower of the new primary. A
switchover does not have this hazard at all (the handed-off primary demotes
itself durably); it is specific to promote (DESIGN §5).

Both travel as control messages over the replication port, issued with the
`charpente` CLI:

```bash
charpente promote --node <addr> [--secret <s>]
charpente switchover --primary <addr> --to <addr> [--secret <s>]
```

`<addr>` is a node's *replication* address (`ReplConfig.listen`), never its
HTTP port. `--secret` is the target node's `peer_secret` proof — required
whenever that node is configured with one; absent, `None` is sent, which a
`peer_secret: None` node accepts and a configured one refuses. Both verbs
connect, send the control message, and poll for a reply up to the same 30s
deadline the primary's own switchover drain is bound by
(`SWITCHOVER_DEADLINE_NANOS`); the node's own words are the product surface,
forwarded verbatim:

- exit 0 — `CtrlOk`, printed to stdout (e.g. `promoted to primary at
  last_applied Some(41) — …`).
- exit 1 — `CtrlErr` or `Refuse`, printed to stderr (a refusal: already
  primary, no matching follower session, switchover deadline exceeded, a
  missing/mismatched `--secret`, …).
- exit 2 — usage, connect, or timeout failure (bad flags, the node
  unreachable, no reply before the deadline) — never a node-level decision.

When a follower's own local copy is unrecoverable — media corruption below its
checkpoint horizon, or a log directory a crash left gapped — the fix is not a
local repair but a restore: promote a healthy replica and re-seed the damaged
node from it as a fresh follower (its snapshot rebuilds the whole state, no
log replay). This is the promise a single node cannot make and a replicated
deployment can: a record one node loses to media corruption survives on
another.

The same deliberate re-seed applies to a follower that comes back holding data
after being offline past the primary's retention window (`retain_segments`):
the primary has GC'd the frames that would prove the follower's records are a
prefix of its timeline, so it refuses to re-seed it automatically — naming
both positions and both remedies — rather than risk silently overwriting a
follower that is actually *ahead* of a primary that forked behind it. If the
follower truly just lagged, stop it, clear its data directory, and restart it;
it re-seeds from a snapshot and converges.

## 11. Realtime

SSE subscribers tail the log through a materialized `charpente.events` table with
exact resume: a client that reconnects with its last-received id provably misses
nothing (DESIGN §6, PLAN M1's exit criterion). Nothing here needs a message broker
— `tx.emit`/`tx.emit_keyed` write into the same committing log record as everything
else in the transaction, and delivery reads that table the same way the job
dispatcher reads `charpente.jobs`.

### `#[event]`

```rust
use charpente::{Event, event};

#[event]
struct HTTPEvent {
    code: u16,
}

#[event]
struct InvoicePDFSent {
    invoice: u64,
}

fn names() -> (&'static str, &'static str) {
    (HTTPEvent::NAME, InvoicePDFSent::NAME) // ("http_event", "invoice_pdf_sent")
}
```

`#[event]` derives three things a plain struct doesn't have: `Codec` (the log/table
format — the same machinery `#[table]`/`#[job]` use), `impl Event` (`NAME`, the wire
and delivery-filter name), and `serde::Serialize` (the JSON a subscriber receives).
`NAME` is the snake_case of the type name, computed at compile time — not a name you
choose, so it can never drift from the type. The splitting rule inserts `_` before an
uppercase letter that follows a lowercase letter or digit, and before the *last*
letter of a run of capitals when that letter is immediately followed by a lowercase
one — so a whole acronym run stays one word except right at its boundary with the
next word: `NoteAdded` → `note_added`; `HTTPEvent` → `http_event` (the run `HTTP`
stays together, splitting only where the trailing `E` turns out to start `Event`);
`InvoicePDFSent` → `invoice_pdf_sent` (`PDF` stays together, `S` starts `Sent`).

### Emitting: `tx.emit` and `tx.emit_keyed`

```rust,ignore
pub fn emit<Ev: Event>(&mut self, event: Ev)
pub fn emit_keyed<Ev: Event>(&mut self, key: impl Into<String>, event: Ev)
```

Both buffer into the transaction's effects exactly like `enqueue`/`enqueue_keyed` —
nothing is visible to a subscriber until the transaction's one log record commits.
`emit_keyed`'s key is an opaque scoping filter (an org id, a tenant id, a room name —
whatever a `Subscription` should be allowed to narrow on); the platform never parses
it, only exact-matches it at delivery (`Subscription::key`, below). The match rule is
exact on both sides, and it is the load-bearing multi-tenant scoping rule, so
precisely: an *unkeyed* subscription (no `.key(...)`) receives every event of its
types, keyed or not; a *keyed* subscription receives only events whose key is an
exact match — which means a keyed subscription does **not** receive `tx.emit`
(unkeyed) events, since no key equals the absence of one. Scope a tenant feed by
emitting *everything* that tenant should see with `emit_keyed` and its key; reserve
bare `tx.emit` for events only an unkeyed (global) subscription should tail.

One contract rides along with the `Serialize` derive: every `#[event]` payload must
be JSON-serializable. A value serde_json rejects — a `NaN` or infinite float, a map
with non-string keys — is a contract violation, and it is not caught at emit time
(the log/table store `Codec` bytes; JSON encoding happens at delivery). Delivery of
an unserializable event is kernel-fatal: the encode error (`"event <name> failed
JSON encoding: <serde_json's message>"`) propagates out of the delivery arm as a
platform `io::Error` and stops the kernel loop, and since the event row is durable, a
restart re-encounters it the moment a matching subscription catches up to it. Keep
event payloads to plainly-JSON shapes.

### The SSE route and `Subscription`

`#[route(SSE "/path")]` is a route flavor, not a different mounting mechanism —
`routes![...]` and `App::mount` treat it exactly like `#[route(GET ...)]`. Its
handler signature is fixed, and unlike a plain route neither trailing argument is
optional:

```rust,ignore
fn handler(tx: &mut Tx, [captures,] ctx: &Ctx, cursor: Option<Cursor>)
    -> Result<Subscription, ApiError>
```

```rust
use charpente::event;
use charpente::http::{ApiError, App, AppConfig, Ctx, Cursor, Method, Request, Subscription};
use charpente::kernel::{Kernel, SimSource};
use charpente::{Schema, Tx, route, routes};
use charpente_sim::Sim;
use serde::{Deserialize, Serialize};

#[event]
struct Ping {
    n: u64,
}

#[derive(Serialize, Deserialize)]
struct PingBody {
    n: u64,
}

#[route(POST "/ping")]
fn send_ping(tx: &mut Tx, body: PingBody) -> Result<(), ApiError> {
    tx.emit(Ping { n: body.n });
    Ok(())
}

/// A real handler would narrow `.key(...)` to the caller's own org, read off
/// `ctx.session()` exactly like any other route's access control — the
/// platform enforces nothing about the key beyond the exact-match filter.
#[route(SSE "/feed")]
fn feed(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<Ping>().cursor(cursor))
}

fn build_app(sim: &Sim) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![feed, send_ping]))
}
```

```rust,ignore
impl Subscription {
    pub fn of<Ev: Event + serde::Serialize>() -> Subscription;
    pub fn and<Ev: Event + serde::Serialize>(self) -> Subscription;
    pub fn key(self, key: impl Into<String>) -> Subscription;
    pub fn cursor(self, cursor: Option<Cursor>) -> Subscription;
}
```

`Subscription::of::<Ping>()` names the event type as a Rust type, not a string — a
typo is a compile error, never a silently-empty subscription. `.and::<Ev2>()` adds a
second type to the same tail; `.key(...)` narrows delivery to one exact-match value;
`.cursor(...)` takes the resume point the platform already extracted (next) — the
handler can pass it straight through, as `feed` does above, or clamp it to some
app-level boundary.

### Cursor semantics

The platform extracts the client's resume cursor *before* the handler runs, so an
unparseable one never reaches app code:

1. The `Last-Event-ID` header, if present — this is what makes a browser's native
   `EventSource` reconnection resume automatically, no client code required.
2. Else the `cursor` query parameter (`?cursor=<id>.<seq>`) — for a first connect
   from a non-browser client, or a manual reconnect.
3. Else `None` — "from now": only events committed after this moment.

A present-but-unparseable cursor is a 400, and the handler never runs:

```text
cursor must be "<id>.<seq>" — got "not-a-cursor"; omit it to tail from now
```

The wire form is always `<id>.<seq>`: an event id and its committing record's log
seq, joined by a dot (`Cursor::wire`). The seq half is not decorative — two diverged
timelines (an old primary's unshipped tail vs. a promoted follower's new writes, §10)
can mint the same event id for two *different* events, and a bare id can't tell them
apart. Resume validation at subscribe time checks both, in order:

- **Below the retention floor** — the cursor's id predates everything this node
  still retains:

  ```text
  cursor 1.0 is below this node's retention floor 8 — events that old are
  gone; re-sync current state through reads, then reconnect without a cursor
  ```

- **Diverged** — either the cursor's id is ahead of anything this node has ever
  minted:

  ```text
  cursor 999.1 diverged from this node's timeline (next event id is 10) —
  your history came from another primary; re-sync current state through
  reads, then reconnect without a cursor
  ```

  or the retained row at that id carries a different seq than the client
  remembers (the cross-timeline case — the id was reused by a different
  history):

  ```text
  cursor 8.42 diverged from this node's timeline (event 8 here has seq 3,
  not 42) — your history came from another primary; re-sync current state
  through reads, then reconnect without a cursor
  ```

Every one of these three named errors ends with the identical fix: re-sync current
state through ordinary reads, then reconnect with no cursor at all — a fresh
"from now" subscribe never revalidates against history it doesn't have.

### Delivery: one bounded path, no live/catch-up modes

A validated subscribe joins the kernel's ready set as a `Tail` — data-gated, never a
timer (DESIGN §1): it becomes eligible the instant some commit advances
`charpente.events` past its position. One unit of delivery reads up to
`TAIL_BATCH_EVENTS` (64) rows, filters by the subscription's names and key,
JSON-encodes matches, and pushes frames to the connection's sink. There is no
separate "catch up" mode and no separate "live push" path — a freshly-resumed
subscription with thousands of events queued and a brand-new one sitting at "now"
are served by the exact same code, just starting at different positions.

A tail that falls behind retention *while already subscribed* (a slow consumer a
burst of writes outpaces) gets one terminal SSE frame instead of silently replaying
gone history, then its connection closes — the same three facts as the 409s, shaped
as a frame instead of a rejected request:

```text
id: 1.0
event: charpente.gap
data: {"floor":4,"tail":1,"fix":"re-sync current state through reads, then reconnect without a cursor"}

```

A sink that reports itself full or closed — a slow browser tab, a dead TCP
connection — costs its own tail immediately; it never blocks the kernel loop for
anyone else. Nothing is sent in that case (there is no one left to tell); the
client's own reconnect-with-`Last-Event-ID` behavior is the entire recovery story,
which is why every case above ends the same way: re-sync, then reconnect.

### The wire

```text
id: 0.0
event: ping
data: {"n":1}

id: 1.1
event: ping
data: {"n":2}

```

Each emit above is its own request, hence its own commit — a different `seq` each
time; two events emitted by the *same* transaction share one `seq` (§4's "call
order" note applies to delivery order too). Each frame is `id: <id>.<seq>`,
`event: <name>`, `data: <json>`, then a blank line.
`id` matches `Cursor::wire` exactly, which is what makes a browser's native
`Last-Event-ID` resume work with zero client code. An idle connection gets a `:hb`
comment frame every 15 seconds — pure keepalive, no semantics, never interpreted as
an event.

### Testing a tail in sim

The whole-app sim driver from §8 covers SSE routes with no extra machinery — a
subscribe is just a `GET` request whose kernel response is an upgrade instead of a
one-shot reply, recorded as a `SimSinkHandle`:

```rust
use charpente::event;
use charpente::http::{ApiError, App, AppConfig, Ctx, Cursor, Method, Request, Subscription};
use charpente::kernel::{Kernel, SimSource};
use charpente::{Schema, Tx, route, routes};
use charpente_sim::Sim;
use serde::{Deserialize, Serialize};

const SEC: u64 = 1_000_000_000;

#[event]
struct Ping {
    n: u64,
}

#[derive(Serialize, Deserialize)]
struct PingBody {
    n: u64,
}

#[route(POST "/ping")]
fn send_ping(tx: &mut Tx, body: PingBody) -> Result<(), ApiError> {
    tx.emit(Ping { n: body.n });
    Ok(())
}

#[route(SSE "/feed")]
fn feed(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<Ping>().cursor(cursor))
}

fn build_app(sim: &Sim) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![feed, send_ping]))
}

/// A client that always resumes with its last-received id.seq sees no gap and
/// no duplicate across a disconnect+reconnect — the property this whole
/// design exists to prove (DESIGN §6, PLAN M1's exit criterion).
fn tail_resumes_after_reconnect(sim: &mut Sim) -> Result<(), String> {
    let app = build_app(sim).map_err(|e| e.to_string())?;
    let mut source = SimSource::new(Box::new(sim.clock()), 10 * SEC);
    source.push_at(0, Request::new(Method::Get, "/feed"));
    for n in 0..3u64 {
        source.push_at(
            (n + 1) * SEC,
            Request::new(Method::Post, "/ping").json(&PingBody { n }),
        );
    }
    let sinks = source.sinks();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).map_err(|e| e.to_string())?;
    kernel.run(&mut source).map_err(|e| e.to_string())?;

    // Resume from the second frame the connection actually saw — never from
    // a hand-computed id; the client only ever knows what it received.
    let resume_cursor = {
        let handles = sinks.lock().map_err(|_| "sinks lock poisoned".to_string())?;
        let frames = handles[0].frames();
        Cursor {
            id: frames[1].id,
            seq: frames[1].seq,
        }
        .wire()
    };
    source.push_at(
        4 * SEC,
        Request::new(Method::Get, format!("/feed?cursor={resume_cursor}")),
    );
    source.push_at(
        5 * SEC,
        Request::new(Method::Post, "/ping").json(&PingBody { n: 3 }),
    );
    kernel.run(&mut source).map_err(|e| e.to_string())?;

    let handles = sinks.lock().map_err(|_| "sinks lock poisoned".to_string())?;
    let resumed: Vec<u64> = handles[1].frames().iter().map(|f| f.id).collect();
    if resumed != vec![2, 3] {
        return Err(format!("expected [2, 3], got {resumed:?} — gap or duplicate"));
    }
    Ok(())
}
```

`source.sinks()` hands back every SSE upgrade's handle, in upgrade order;
`handle.frames()` is everything that connection received so far, in delivery order;
`handle.closed()` is true once the kernel has dropped the tail (drop-on-full, or the
terminal gap frame). A stalled consumer is scripted with `source.push_stall(closure)`
(queued FIFO, one entry per upgrade) — a closure returning `true` makes that send
report `Full`, which is how the drop-on-full path above gets exercised without a real
slow client. Register a workload like `tail_resumes_after_reconnect` in `sim_tests!
{ ... }` exactly like any other (§8).

### Follower tails and failover

A follower serves subscribes unchanged — `charpente.events` replicates like any
other table, so its rows are byte-identical to the primary's the instant they're
applied (§10). A failover honors the same bounded-loss window for events that it
does for everything else: `promote` can lose events the old primary acked but
hadn't shipped, named honestly in its reply; a client whose cursor referenced one of
those lost events gets the diverged 409 on reconnect, never wrong events silently
delivered in their place.

## 12. Migrations

A `#[table]` row's shape can change safely — add a field with a default, drop one, or
retype one via an explicit conversion — without a hand-written backfill script and
without risking a silent misdecode of rows already on disk. The mechanism (DESIGN §3;
spec `docs/superpowers/specs/2026-07-16-migrations-design.md`) runs once, eagerly,
before a node serves any traffic; app code never sees a mixed-version table.

### Versioning a table: `#[table(version = N)]`

Every `#[table]` struct has an implicit version, `1`, until you write
`#[table(version = N)]`. Bumping `N` to `2` or higher and recompiling **does not
compile** unless you also keep the old shape in code — under a different name, marked
`#[row_shape(version = N-1)]` (codec only: no table identity, no `#[index]`, never
registered as a table itself) — and `impl TableMigration` describing how to get from
the old shape to the new one. The macro also computes a structural fingerprint of each
version's field names and types; if the fields change shape without a version bump,
the fingerprint catches it at the next startup — a compile-time check plus a runtime
backstop for the one hole compile-time checking cannot close (nothing forces you to
remember to bump `version`).

### Worked example: adding a field

Start from an ordinary version-1 table:

```rust
use charpente::{Id, table};

#[table]
struct Widget {
    id: Id<Widget>,
    count: u32,
}
```

To add a `label: String` field: rename the old struct (`WidgetV1`, marked
`#[row_shape]` at its old version), define the new `Widget` at the bumped version, and
write the conversion:

```rust
use charpente::core::migration::{MigrationError, TableMigration};
use charpente::{Db, DbConfig, Id, Schema, prod::ProdDisk, row_shape, table};

/// The superseded shape — kept only so `TableMigration::migrate` can decode
/// bytes an older binary wrote. No `#[index]`, no redb registration.
#[row_shape(version = 1)]
struct WidgetV1 {
    id: Id<WidgetV1>,
    count: u32,
}

/// The current shape. Bumping to `version = 2` without the `TableMigration`
/// impl below is a compile error — the macro emits an assert-impl const.
#[table(version = 2)]
struct Widget {
    id: Id<Widget>,
    count: u32,
    label: String,
}

impl TableMigration for Widget {
    type Prev = WidgetV1;
    fn migrate(prev: WidgetV1) -> Result<Widget, MigrationError> {
        Ok(Widget {
            id: Id::from_raw(prev.id.raw()),
            count: prev.count,
            label: String::new(), // the field's required default
        })
    }
}

fn open(data_dir: &str) -> std::io::Result<Db> {
    let disk = ProdDisk::new(data_dir)?;
    let schema = Schema::new()
        .table::<Widget>()
        .migration::<Widget>("Widget"); // registers the 1 -> 2 step
    Db::open(Box::new(disk), DbConfig::default(), schema)
}
```

`Schema::migration::<Step>(table)` registers one step of the chain — the table name
string (`"Widget"`, `Widget::TABLE`) must already be `.table()`-registered under the
same name. A multi-version bump (1 → 2 → 3) registers one step per version pair;
`migrate_schema` walks the chain and refuses by name if a link is missing.

### What happens at next startup

Nothing here is app code — the kernel runs it automatically, before serving any
request, job, or cron tick. `Db::open` itself only runs the read-only gate check: it
reads `charpente.schema_versions`, finds `Widget` at its persisted version 1 with a
registered chain to the compiled version 2, and admits (every open runs this check, on
every node — refusing by name instead is the barrier, below). The rewrite is a separate
step, `migrate_schema`, which the kernel calls right after open, before it serves
anything, on a single node or a primary: it reads existing rows in batches
(`DbConfig::migration_batch_size`, default 1000), applies `migrate` to each, and commits
the rewritten batch plus a resumable watermark in one transaction. A crash mid-migration
is exactly resumption — the next start picks up from the watermark and finishes; no row
is transformed twice, none skipped. Once every row has moved, one
closing transaction bumps `Widget` to `(2, fingerprint(2))` and rebuilds its secondary
indexes. Existing rows come back with `label` populated by whatever `migrate` computed
— here, `""` — with no manual backfill and no window where old- and new-shape rows are
both visible to the app. A table with no rows yet has nothing to migrate: its first
write simply adopts the compiled version directly.

### The upgrade procedure

For a replicated deployment (§10): stop both nodes — mixed-version serving is out of
scope, and `Hello`'s schema digest refuses it anyway — install the new binary on both,
then start the **primary first**. It runs the gate and migrates before it serves
anything. Start the followers after: they never migrate locally, they receive the
primary's `MigratePut` records through ordinary log shipping and rebuild indexes at the
same log position the primary did. No re-seed is ever required by a migration — the
only re-seed rule is the pre-existing retention-window one (§10, "Promotion and
switchover").

### The upgrade barrier

A crash *before* a clean shutdown leaves an old binary's writes sitting in the WAL
tail, uncheckpointed. Starting a version-bumped binary against that tail refuses
rather than guess at decoding bytes it cannot prove are its own shape:

```text
table "Widget" has unrecovered writes from the previous binary (persisted version 1,
this binary is compiled for version 2) — run the previous binary once to a clean
shutdown, then upgrade (spec §2's upgrade barrier)
```

The remedy is exactly what the message says, and nothing more: start the **previous**
binary once, let it recover and shut down cleanly (a clean exit always checkpoints),
then start the new one — it now opens and migrates without complaint. An interrupted
migration is a different case, and self-heals on its own: restarting the *same* binary
just resumes from its watermark, no barrier involved; the barrier fires only for a
tail a differently-versioned binary cannot prove is safe to decode.

### Hand-written `Row` opts out

`Row::FINGERPRINT` defaults to `None`, and a table whose row type is `None`-fingerprinted
is invisible to the gate — writing `Row` by hand instead of using `#[table]` silently
opts that table out of drift protection. Use `#[table]`.

## 13. describe — the machine-readable contract

`charpente describe` renders the full shape of a mounted `App` — every table (with its
version, structural fingerprint, indexes, and migration chain), every route (method,
pattern, name, whether it requires auth, whether it's an SSE stream), every job (name,
retry policy), every cron entry (name, interval), and the app's config — as one JSON (or
human-readable) document. It is a pure walk of the app's own registries (DESIGN §7): no
disk writes beyond the ephemeral describe run itself, no socket, no kernel loop. This is
the "what exists" query an agent would otherwise burn context reconstructing by reading
every `#[table]`/`#[route]`/`#[job]` item across the crate by hand.

There are two ways to ask for it: from inside the app binary itself (`charpente::launch`,
below), or from the outside without running the app at all (`charpente describe`, the CLI
verb, further below).

### The one-line main: `charpente::launch`

`charpente::launch(addr, data_dir, build)` replaces a hand-rolled `fn main` that calls
`App::open` and `charpente::serve::serve` directly (§5, §7). It owns argv dispatch: no
verb serves the app exactly as before; `describe [--format json|human]` renders the
contract to stdout and exits, without ever touching `data_dir`. `build` is the same
`fn(Box<dyn Disk>, Box<dyn Entropy>) -> io::Result<App>` shape used everywhere else an
app hands its construction over to the platform (sim tests, the CLI verb below) — the
one function this migration asks you to factor `fn main`'s body into.

```rust
use charpente::core::io::{Disk, Entropy};
use charpente::core::table::Id;
use charpente::http::{ApiError, App, AppConfig};
use charpente::{Schema, Tx, View, route, routes, table};
use serde::Deserialize;

#[table]
struct Note {
    id: Id<Note>,
    text: String,
}

#[derive(Deserialize)]
struct NoteBody {
    text: String,
}

#[route(POST "/notes/:id")]
fn create_note(tx: &mut Tx, id: u64, body: NoteBody) -> Result<u64, ApiError> {
    tx.put(&Note {
        id: Id::from_raw(id),
        text: body.text,
    });
    Ok(id)
}

fn build_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> std::io::Result<App> {
    Ok(App::open(disk, Schema::new().table::<Note>(), entropy, AppConfig::default())?
        .mount(routes![create_note]))
}

fn main() -> std::io::Result<()> {
    charpente::launch("0.0.0.0:8080", "./data", build_app)
}
```

That's the whole migration: no verb still serves on `"0.0.0.0:8080"` against `./data`
exactly as a hand-rolled `main` would; `myapp describe` and `myapp describe --format json`
are new, for free. An unrecognized verb or a bad `--format` value exits 2 with a pinned
usage message:

```text
usage: <app> [describe [--format json|human]]  — no verb serves the app
```

### `charpente describe` — the CLI verb

The CLI verb builds and runs any app crate's binary from the outside — no code changes to
the app required, not even the `launch` migration above (it works against any binary that
calls `charpente::launch`, which is what makes it a description of the app rather than
something the app has to expose over HTTP itself):

```text
usage: charpente describe [--dir PATH] [--format json|human] [--bin NAME]
       --dir PATH   the app crate directory to build and describe (default: .)
       --bin NAME   which binary to describe when the crate has several (default: the sole binary, else the one named like the package)
```

```bash
charpente describe --dir ./myapp --format json
```

`--dir` defaults to the current directory. `charpente describe` runs `cargo build` in
that directory, picks its one binary target (or the one matching `--bin`, or the one
named like the package when several exist and none is named), and execs it with
`describe --format <F>` — the exact same argv path `charpente::launch` handles above.
Cargo's own errors (a missing `Cargo.toml`, a build failure) are forwarded verbatim,
never wrapped in a framework message.

### The invoicing contract

The full JSON `examples/invoicing` renders — 12 routes across customers, invoices, and
API keys; 4 jobs (3 of the app's own plus the platform's built-in session sweep); 2 cron
entries (the app's daily overdue-check plus the platform's built-in session sweep
schedule); 13 tables (7 of the app's own plus 6 platform tables: `Session`, `ApiKey`,
`CronRow`, and the three untyped internal tables backing jobs, events, and schema
versions). Every `"builtin": true` entry above is a table, job, or cron entry the
platform itself registers, not app code — this is how an agent tells its own surface
apart from the platform's without memorizing a list:

```json
{
  "charpente_version": "0.0.0",
  "tables": [
    {
      "name": "ApiKey",
      "version": 1,
      "fingerprint": 11308777448940268015,
      "indexes": [
        "key_hash"
      ],
      "migrations": [],
      "builtin": true
    },
    {
      "name": "AppClock",
      "version": 1,
      "fingerprint": 1422665991556096363,
      "indexes": [],
      "migrations": [],
      "builtin": false
    },
    {
      "name": "CronRow",
      "version": 1,
      "fingerprint": 13750309978683829658,
      "indexes": [
        "name"
      ],
      "migrations": [],
      "builtin": true
    },
    {
      "name": "Customer",
      "version": 1,
      "fingerprint": 12205929389820576530,
      "indexes": [],
      "migrations": [],
      "builtin": false
    },
    {
      "name": "IdCounter",
      "version": 1,
      "fingerprint": 17814272651750401892,
      "indexes": [],
      "migrations": [],
      "builtin": false
    },
    {
      "name": "Invoice",
      "version": 2,
      "fingerprint": 16282652804679416816,
      "indexes": [
        "customer"
      ],
      "migrations": [
        {
          "from": 1,
          "to": 2
        }
      ],
      "builtin": false
    },
    {
      "name": "LineItem",
      "version": 1,
      "fingerprint": 261905951631396626,
      "indexes": [
        "invoice"
      ],
      "migrations": [],
      "builtin": false
    },
    {
      "name": "Receipt",
      "version": 1,
      "fingerprint": 18357282543607550790,
      "indexes": [
        "invoice"
      ],
      "migrations": [],
      "builtin": false
    },
    {
      "name": "Reminder",
      "version": 1,
      "fingerprint": 13477227825039761833,
      "indexes": [
        "invoice"
      ],
      "migrations": [],
      "builtin": false
    },
    {
      "name": "Session",
      "version": 1,
      "fingerprint": 11676348087434866846,
      "indexes": [
        "token_hash"
      ],
      "migrations": [],
      "builtin": true
    },
    {
      "name": "charpente.events",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "builtin": true
    },
    {
      "name": "charpente.jobs",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "builtin": true
    },
    {
      "name": "charpente.schema_versions",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "builtin": true
    }
  ],
  "routes": [
    {
      "method": "POST",
      "pattern": "/login",
      "name": "login",
      "auth": false,
      "stream": false
    },
    {
      "method": "POST",
      "pattern": "/customers/:id",
      "name": "create_customer",
      "auth": true,
      "stream": false
    },
    {
      "method": "GET",
      "pattern": "/customers/:id",
      "name": "get_customer",
      "auth": true,
      "stream": false
    },
    {
      "method": "GET",
      "pattern": "/customers",
      "name": "list_customers",
      "auth": true,
      "stream": false
    },
    {
      "method": "POST",
      "pattern": "/invoices/:id",
      "name": "create_invoice",
      "auth": true,
      "stream": false
    },
    {
      "method": "GET",
      "pattern": "/invoices/:id",
      "name": "get_invoice",
      "auth": true,
      "stream": false
    },
    {
      "method": "GET",
      "pattern": "/invoices",
      "name": "list_invoices",
      "auth": true,
      "stream": false
    },
    {
      "method": "POST",
      "pattern": "/invoices/:id/finalize",
      "name": "finalize_invoice",
      "auth": true,
      "stream": false
    },
    {
      "method": "POST",
      "pattern": "/invoices/:id/record-payment",
      "name": "record_payment",
      "auth": true,
      "stream": false
    },
    {
      "method": "GET",
      "pattern": "/events/feed",
      "name": "events_feed",
      "auth": true,
      "stream": true
    },
    {
      "method": "POST",
      "pattern": "/keys",
      "name": "create_key",
      "auth": true,
      "stream": false
    },
    {
      "method": "DELETE",
      "pattern": "/keys/:id",
      "name": "revoke_key",
      "auth": true,
      "stream": false
    }
  ],
  "jobs": [
    {
      "name": "SessionSweep",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "builtin": true
    },
    {
      "name": "SendReceipt",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "builtin": false
    },
    {
      "name": "SendOverdueReminder",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "builtin": false
    },
    {
      "name": "AdvanceDay",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "builtin": false
    }
  ],
  "cron": [
    {
      "name": "charpente.session_sweep",
      "interval_nanos": 86400000000000,
      "builtin": true
    },
    {
      "name": "daily-overdue-check",
      "interval_nanos": 86400000000000,
      "builtin": false
    }
  ],
  "config": {
    "body_cap": 1048576,
    "session_ttl": 2592000000000000,
    "store_path": "tables.redb",
    "retain_segments": 2,
    "checkpoint_every": 4096,
    "event_retention": 65536,
    "migration_batch_size": 1000,
    "oidc": false
  },
  "telemetry": {}
}
```

(`examples/invoicing/tests/describe_golden.rs` pins this exact document — byte-for-byte,
including field order — and `crates/charpente-cli/tests/describe.rs` cross-checks that
running `charpente describe --dir examples/invoicing --format json` renders the same
bytes. `human` format renders the same information as fixed-width lines instead of JSON,
starting with a `tables:` header.)

### What's not here (yet)

Two honesty notes. First: a `describe` run never touches the app's real data
directory — it builds and opens the app against a fresh, throwaway temp directory the
CLI verb (or `launch`, when invoked with the `describe` verb) creates and removes around
the run, so it is always safe to run against a live server's data dir. Second: emitted
event *types* are not enumerated here in v0 — `tx.emit`/`tx.emit_keyed` names are compiled
into each `#[route(SSE …)]` handler's `Subscription`, not registered anywhere an app-wide
walk could discover ahead of time (spec ruling 6). The only trace of realtime in the
contract above is `"stream": true` on `events_feed` — the fact that a route serves SSE at
all, not what it emits. Reading a route's handler is still how you find that out.
