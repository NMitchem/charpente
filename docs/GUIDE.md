# Building an app on charpente

*(This file is compile-gated: every `rust` code block below is extracted
and built by `crates/guide-examples` — `cargo build -p guide-examples` fails
if a block stops compiling. Sections 1–4 cover the persistence layer:
project setup, tables, the database, and transactions. Sections 5–7 cover
the deterministic kernel: HTTP, jobs, and cron. Section 8 covers sim
testing — writing `tests/sim.rs`, running `charpente test --sim`, and
replaying a failure's seed. Section 9 is an error index: look up the
message you're staring at.)*

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
    }
}
```

`DbConfig::default()` already sets exactly these values — most apps never
construct one by hand. `retain_segments` and `checkpoint_every` govern
`Db::checkpoint`'s log GC, covered next.

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

`TxError<E>` has exactly two variants, and this block matches the real
type — a variant rename or addition breaks the guide's compile gate:

```rust
use charpente::TxError;

/// The two arms differ in what they promise: `App` means nothing was
/// written; `Io` means the outcome is unknown (details below).
fn describe<E: std::fmt::Display>(err: &TxError<E>) -> String {
    match err {
        TxError::App(e) => format!("rejected by the app — nothing was written: {e}"),
        TxError::Io(e) => format!("platform IO failure — outcome unknown: {e}"),
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
one of `GET`, `POST`, `PUT`, `PATCH`, `DELETE`; the trailing `, auth` flag is
optional and covered under Sessions below). The macro checks the function's
argument list against the path and emits the glue that extracts, decodes,
and dispatches to it:

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

`Request`'s shape (what a handler ultimately runs against, one layer down):
`method: Method`, `path: String`, `headers: BTreeMap<String, String>` (keys
lowercased), `body: Vec<u8>`. The query string is stripped at the edge and
ignored in v0 — routes cannot see it.

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

### Sessions: the platform `Session` table and `Ctx`

`App::open` registers one table of its own alongside yours: `Session` (`id:
Id<Session>`, `#[index] token: String`, `subject: String`) — raw tokens in
v0; M1 adds hashing and expiry. Every request gets a `Ctx`, built inside the
same transaction as your handler, carrying:

- `ctx.session() -> Option<&Session>` — the session matched by the
  request's credentials, if any.
- `ctx.create_session(tx, subject) -> String` — inserts a `Session` row and
  returns its token (a login). The token is 32 entropy bytes, hex-encoded
  (64 characters), drawn once *before* the transaction starts — calling
  `create_session` twice in one request panics: "Ctx::create_session called
  twice in one request — one session per request".
- `ctx.revoke(tx)` — deletes the current session (a no-op without one).

The request's credential is read from `Authorization: Bearer <token>` if
present, else a `charpente_session=<token>` pair in the `Cookie` header.
`#[route(METHOD "/path", auth)]` rejects the request before your handler
runs if no session matched: 401, with the fixed message "no valid session —
send `Authorization: Bearer <token>` or a `charpente_session` cookie".
Without `auth`, the route always runs — `ctx.session()` is just `None`.

```rust
use charpente::http::{ApiError, App, AppConfig, Ctx};
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::{Schema, Tx, route, routes};

#[route(POST "/login")]
fn login(tx: &mut Tx, subject: String, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_session(tx, subject))
}

#[route(GET "/me", auth)]
fn whoami(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let _ = tx;
    Ok(ctx
        .session()
        .expect("auth routes always have a session")
        .subject
        .clone())
}

#[route(POST "/logout", auth)]
fn logout(tx: &mut Tx, ctx: &Ctx) -> Result<&'static str, ApiError> {
    ctx.revoke(tx);
    Ok("bye")
}

fn build_app(data_dir: &str) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(ProdDisk::new(data_dir)?),
        Schema::new(), // Session is registered by App::open itself
        Box::new(ProdEntropy::new()?),
        AppConfig::default(),
    )?
    .mount(routes![login, whoami, logout]))
}
```

### `ApiError` and status mapping

A handler rejects with `ApiError`; `From<io::Error> for ApiError` converts
to `Internal`, so `?` works on `tx` reads exactly like it does inside a
plain transaction (§4). Every response body is a JSON envelope
`{"error": "<message>"}` except a 200, which is the handler's own returned
value:

- `BadRequest(String)` → 400. What `FromParam`/JSON-body decoding produce
  (below), or your own rejection.
- `Unauthorized` → 401. Fixed message (quoted above) — produced by `auth`
  routes only, before your handler runs.
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
// \"notes\"", "unknown method PUSH — one of GET, POST, PUT, PATCH, DELETE").
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

### The three checkers

`charpente_sim::checks` ships three reusable oracles — reach for these
before hand-rolling the same logic a fourth time:

- `is_fault_fallout(text: &str) -> bool` — classifies an error's *text*:
  is this the harness's own injected fault (or its consequences) rather
  than a bug your workload should fail on? It recognizes the sim's
  injected-fault wording (including the `read_error` and
  `misdirected_write` axes), the WAL/engine poison wording, the WAL's
  durable-horizon hard error (bit-level corruption of an already-
  acknowledged frame — a fault-matrix consequence, not a bug), and redb's
  "close and re-open the database" latch. Used inside a workload to turn
  "the disk broke, as instructed" into "no answer this round" rather than
  a hard failure — never to swallow anything else.
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

```rust,ignore
pub fn is_fault_fallout(text: &str) -> bool

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
unknown method PUSH — one of GET, POST, PUT, PATCH, DELETE
the route path must be a string literal — e.g. #[route(GET "/notes/:id")]
the only route flag is `auth` — write `#[route(GET "/path", auth)]`
#[route] handlers are free functions — no `self`
empty :param name in "/notes/:"
write `#[route(METHOD "/path")]` — e.g. #[route(GET "/notes/:id")]
```

§5's "What `#[route]` rejects" walks through the first three with full
examples. The rest, in order: the path literal doesn't start with `/`; the
method name isn't one of `GET`/`POST`/`PUT`/`PATCH`/`DELETE`; the path
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
Ctx::create_session called twice in one request — one session per request
Schema: table "Counter" registered twice — register each #[table] type once
```

The first three are all the same shape as the router conflict above, just
for a different registry: `App::mount_jobs([...])` (§6), `App::schedule(...)`
(§7), and `Ctx::create_session` (§5's Sessions section — called at most once
per request, since the token is drawn before the transaction starts) each
panic the moment a name is reused. The fourth (§3) is `Schema::table::<R>()`
— called twice for the same `#[table]` type when building the `Schema`
passed to `Db::open`, including registering `Session` or `charpente.jobs`
yourself (`App::open`/`Db::open` already register those). Fix in every
case: register that name/type exactly once.

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
