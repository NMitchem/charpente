# Building an app on charpente

*(This file is compile-gated: every `rust` code block below is extracted
and built by `crates/guide-examples` — `cargo build -p guide-examples` fails
if a block stops compiling. Sections 1–4 cover the persistence layer:
project setup, tables, the database, and transactions. Sections 5–7 cover
the deterministic kernel: HTTP, jobs, and cron. Sections 8–9 — sim testing
and an error index — land in later commits.)*

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
wall-clock speed — the sim harness that drives this (`charpente test
--sim`) is covered in a later section.

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
