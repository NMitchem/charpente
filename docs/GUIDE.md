# Building an app on charpente

*(This file is compile-gated: every `rust` code block below is extracted
and built by `crates/guide-examples` — `cargo build -p guide-examples` fails
if a block stops compiling. A `text` block preceded by the line
`<!-- pinned -->` is gated too: `crates/guide-examples/tests/doc_truth.rs`
proves every fragment of 16+ characters outside a `<placeholder>` still
appears in a source or test file under `crates/` or `examples/`, so a quoted
error message cannot silently drift from what the code emits.
Sections 1–4 cover the persistence layer:
project setup, tables, the database, and transactions. Sections 5–7 cover
the deterministic kernel: HTTP, jobs, and cron. Section 8 covers sim
testing — writing `tests/sim.rs`, running `charpente test --sim`, and
replaying a failure's seed. Section 9 is an error index: look up the
message you're staring at. Section 10 covers replication; section 11
covers realtime SSE subscriptions; section 12 covers migrations —
evolving a `#[table]` row's shape safely; section 13 covers `describe` —
the machine-readable contract of a mounted app; section 14 covers the
generated TypeScript client — deriving `Wire`, 64-bit integers, and
`gen-client`/`check-client`; section 15 covers the archive — configuring
one, the notices it raises, and restoring a data directory from a
bucket; section 16 covers tenant scoping — `scoped_by`, opening a scope,
and the cross-tenant capability.)*

## 1. Project setup

Install the CLI once. The crate is `charpente-cli`; the binary it installs is `charpente`:

```text
cargo install charpente-cli
```

Then scaffold an app and run its two gates:

```text
charpente new myapp
cd myapp
cargo clippy --all-targets -- -D warnings   # hard rule 1, in your app
charpente test --sim                        # seeded fault-injection universes
```

`charpente new` writes six files and does nothing else — no `git init`, no build, no
prompts. It refuses a name cargo would reject, a Rust keyword or a reserved crate or
build-directory name, and an existing path, each by name.

| File | What it is |
|---|---|
| `Cargo.toml` | the manifest below, byte for byte |
| `clippy.toml` | the fence: the standard-library calls a transaction must never make |
| `.gitignore` | `/target` and `/data` |
| `src/lib.rs` | one table, one route, one job, one event, and `build_app` |
| `src/main.rs` | `charpente::launch(...)` — one line (§13) |
| `tests/sim.rs` | two properties in the give-up idiom (§8) |

### `Cargo.toml`

A charpente app is a Rust crate that links against the `charpente` platform
crate and compiles into one binary. Deployment is one binary and one data
directory — no database server, queue service, or config store to
provision. Two dependencies: `charpente`, and `serde` for the request and
response types you define — `charpente` re-exports serde, but
`#[derive(Deserialize)]` looks the crate up by name in your own dependency
list (§5). This is the file `charpente new myapp` writes; a test holds the
two together:

```toml
[package]
name = "myapp"
version = "0.1.0"
edition = "2024"

[dependencies]
charpente = "0.1"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
charpente-sim = "0.1"

[[test]]
name = "sim"
harness = false

[lints.clippy]
disallowed_methods = "deny"
disallowed_types = "deny"
```

`charpente-sim` is dev-only: it drives your app's tables, routes, jobs, and
time through a deterministic simulator with seeded fault injection, and
never links into the shipped binary. The `[[test]] name = "sim", harness =
false` block is required for `charpente test --sim` (§8): it tells Cargo to
build a `tests/sim.rs` binary without the default libtest harness, so the
sim runner supplies its own `main` instead.

### The fence

`clippy.toml` names the standard-library calls a transaction must never
make — `SystemTime::now`, `Instant::now`, `thread::spawn`, the `std::fs`
functions, `TcpStream::connect`, `HashMap` — with the trait to use instead.
The `[lints.clippy]` lines above turn each into an error, so a handler that
reaches for the wall clock is refused at `cargo clippy` by name:

```text
error: use of a disallowed method `std::time::SystemTime::now`
  --> src/lib.rs:51:17
   |
   = note: ambient wall clock — use the Clock trait (DESIGN.md §1)
```

It is the platform's own fence (its CLAUDE.md hard rule 1) minus the entries
that guard platform internals, and a test keeps it a subset. Where a real
edge needs ambient IO — the production `Disk` your `main` mounts — the
sanctioned form is `#[allow(clippy::disallowed_methods)]` on that one
statement with a rationale comment, never at file level.

### The data directory

`charpente::prod::ProdDisk` is the production mount for the `Disk` trait —
the sanctioned place ambient filesystem calls happen. Every table's backing
file and the write-ahead log's segments live under one root directory, as
flat relative names (`tables.redb`; WAL segments are named by their
zero-padded 20-digit base sequence, so the first is
`wal/00000000000000000000.log`); subdirectories are created on demand.
**The data directory's parent must exist, and the node must be able to read
it.** `ProdDisk::new` creates the data directory itself when it is absent, and
never a directory above it: `./data` works on a fresh checkout, while
`/srv/app/node` with no `/srv/app` is refused at startup by name (`charpente
failed to start: the data directory "/srv/app/node" has no parent directory:
"/srv/app" is not an existing directory, ...`). Create the parent when you
provision the machine, and make it durable before the node starts — a bare
`mkdir -p` leaves the new entries in the page cache, so follow it with `sync`.
The reason is durability: an acknowledged write rests on every directory entry
above it. Every directory barrier fsyncs the whole chain from the directory it
names up to the data directory; `ProdDisk::new` fsyncs the data directory's
parent, which holds the data directory's own entry, at every start, before
anything is acknowledged; and the parent's own entry is yours. That start sync
also means a parent the node cannot read stops the start there, under the same
line, rather than inside your app's first barrier. An empty path is
refused by name too. `restore --to` follows the same rule.

```rust
use charpente::prod::ProdDisk;

/// Opens the app's one data directory, creating it if it doesn't exist yet
/// (its parent must).
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

- It declares its tenancy: `#[table(scoped_by = Org)]` if every row belongs
  to one tenant (§16), or `#[table(global)]` if its rows belong to none. A
  bare `#[table]` is a compile error naming both — the platform never guesses
  which a table is, and the built-in tables (`Session`, `ApiKey`, …) are
  `global` like any other.
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

#[table(global)]
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

`global` here because this section is about `#[index]`, not tenancy; a real invoice table
belongs to its tenant and would say `#[table(scoped_by = Org)]` (§16) — which also retires
the `org` column, because the scope is the key prefix.

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

#[table(global)]
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

#[table(global)]
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
and the fix. The first rejection is the one every table meets once — a
declaration with no tenancy stance:

<!-- pinned -->
```text
`<table>` declares no tenancy — a table says which. Add one of these to its `#[table(…)]`, keeping any arguments it already has: `scoped_by = Org` if every row belongs to one tenant (the key becomes `(scope, id)` and a bare `Tx` cannot read it), or `global` if its rows belong to no tenant. Neither is the default.
`global` and `scoped_by = <scope>` on one table — a table is one or the other; drop one
`global` given twice — write it once
```

`<table>` is the struct's name. The other six rejections are about the
struct's shape:

```rust,ignore
// no `id` field at all
#[table(global)]
struct Ping { hits: u64 }
// "missing `id: Id<Ping>` field — every #[table] row needs a typed primary
//  key (add `id: Id<Ping>`)"

// `id` isn't `Id<Self>`
#[table(global)]
struct Ping { id: u64, hits: u64 }
// "the `id` field of a #[table] must be `Id<Ping>` — a typed id, never a
//  bare integer"

// `#[index]` on the primary key
#[table(global)]
struct Ping { #[index] id: Id<Ping>, hits: u64 }
// "`id` is the primary key — it is already the fastest lookup; remove
//  #[index]"

// not a braced struct with named fields (tuple structs and unit structs
// both hit this)
#[table(global)]
struct Ping(u64);
// "#[table] needs named fields — `Ping` must be a braced struct with an
//  `id: Id<Ping>` field"

// enums and unions cannot be rows
#[table(global)]
enum Ping { A, B }
// "#[table] applies to structs — enums and unions cannot be rows"

// generic structs — a table is one concrete row shape
#[table(global)]
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

#[table(global)]
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
`Db::open` registers three tables of its own before yours — the job queue
`charpente.jobs`, the event log `charpente.events` and the migration record
`charpente.schema_versions` — so registering any of them yourself hits the same
panic. They are the platform's: no app code reads or writes one (§6, "Job rows are
the platform's"; §16, "Where the guarantee stops"), and code that holds the `Db` reads
them through `db.snapshot()`.

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
commit's WAL record size. It must be at least 1: a batch of 0 rewrites nothing,
so `migrate_schema` refuses by name, before writing anything, when a table is due
to migrate and the size is 0.

### Recovery, behaviorally

`Db::open` opens the table store first; if redb rejects the store file's
bytes (a crash mid-creation can leave bytes redb refuses to read), it's
discarded and rebuilt — the store is a materialization of the log, never a
second source of truth. Only that verdict costs the store: `Store::open`
reports it as `table::StoreRejected` (corrupted, a file format that needs a
manual upgrade, or not a redb file), and any other open failure — an IO
error from the disk — is returned by `Db::open` with the site and the fix
named and the store left as it was, so retrying the open is the fix. The one
exception says so in its text: a store file cut shorter than redb's header,
which no crash leaves and no retry fixes. Its fix depends on the log beside
it, and the text names the right one: while the log still reaches seq 0,
remove the file and the next boot rebuilds it; once the log is compacted,
restore the node from a replica or backup (DESIGN §4). On a replicated node
that restore is a re-seed, and every text that names it says so in the same
breath: clear the data directory and restart the node configured as a follower
of the current primary, never with a primary config (§10). A rebuild only works
while the log still holds everything back
to seq 0, though: `Db::checkpoint(&mut self)` durably persists the store,
then deletes WAL segments below the checkpoint (`DbConfig::retain_segments`
floors how much it keeps; `checkpoint_every` auto-fires the cadence every N
committed records, `0` to disable it and checkpoint only by hand). Once the
log has been compacted this way, a rejected store is unrecoverable
locally — `Db::open` returns a named error instead of a silent (wrong)
rebuild, naming the fix: restore from a replica/backup, or widen
`retain_segments` (for a store whose bytes name an older redb file format, it
says that no charpente release wrote one, so the version byte is damage, and
names redb 2.6's `Database::upgrade()` for a format-2 file something else
wrote). A store whose records stop short of the log's base is refused the same
way, by name, whatever the horizon breadcrumb beside it says: a store file that
is missing, or holds no records, beside a compacted log, or an older copy put
back without its log. Removing or replacing the store is never a fix once the
log is compacted. For a follower the text names the re-seed instead, with the
config it must restart under (§10). `Db::open` then recovers the write-ahead log and replays
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

`f` runs synchronously against a `Tx`: reads see this transaction's own
writes first, then any writes already staged in the same commit batch —
`transact` runs a batch of exactly one, so that level is always empty here —
then the store snapshot the batch began from. `Tx` implements `View` (`get`,
`scan`, `scan_by`), so `Invoice::by_org` from §2 works on a `Tx` exactly as
it does on a `Snapshot`, for every table of the app's own. The platform's own
tables are read through a `Snapshot` alone (§16, "Where the guarantee stops").
On `Ok`, every buffered write, enqueue, and emit
commits as ONE durable log record — all or nothing — and only then applies
to the store. On `Err(TxError::App(_))`, nothing happened at all: no log
position consumed, nothing written. On `Err(TxError::Io(_))` the outcome is
unknown — the record may already be durable (below).

```rust
use charpente::core::table::{Codec, Unscoped};
use charpente::{Db, Id, Job, Tx, TxError, View, event, table};
use std::io;

#[table(global)]
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
    // The payload's shape, recorded beside every enqueue; a Pending job is
    // only ever decoded by a binary whose FINGERPRINT matches (§6). `#[job]`
    // computes it from the fields; by hand, change it whenever the fields
    // change.
    const FINGERPRINT: u64 = 0x5E4D_0001;
    // A global job: it belongs to no tenant (§6, §16).
    type Scope = Unscoped;
    // `RETRY` defaults to `RetryPolicy::DEFAULT` — retry/backoff and cron
    // are covered in a later section.
}

#[event(global)]
struct InvoiceFinalized {
    invoice: Id<Invoice>,
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
        tx.emit(InvoiceFinalized {
            invoice: Id::from_raw(id),
        });
        Ok(())
    })
}
```

`Job` needs `Codec`, a `NAME` and a `FINGERPRINT`, implemented by hand today
(v0); `#[job]` derives the rest of a job's registration for you (a later
section). `#[event]` derives everything an event needs in one step: `Codec`
(the log format), `Event` (`NAME` is the snake_case of the type name — §11 has
the exact splitting rule — and `FINGERPRINT` is the payload's structural
fingerprint, recorded beside every emit, §11 "Changing an event"), `Serialize`
(the wire format `#[route(SSE …)]` subscribers receive, §11), `Wire` (that wire
format's schema, so the contract can name it), and an `EventEntry` impl so
`events![...]` can register it (§11).
Because the payload IS what a subscriber receives, it obeys the wire rules a
route's types obey — which is why `InvoiceFinalized.invoice` above is
`Id<Invoice>` and not the bare `u64` the `SendReceipt` *job* beside it carries:
a job payload never leaves the process, an event payload is a JSON document
someone parses. `tx.put` and `tx.delete` cannot fail — they only
buffer a write into the transaction's overlay; `tx.get`/`scan`/`scan_by`
return `io::Result` because they read through the snapshot. A transaction
that never calls `put`/`delete`/`enqueue`/`emit` writes nothing and consumes
no log position — read-only queries are free.

Enqueuing also takes the app's own key for the job: `tx.enqueue_keyed(key,
job)` carries it on the job's row, and the platform never dedupes on it. It is
not the idempotency key an outbound job's call carries — the platform mints
that one (see Outbound jobs below). An empty key is a contract panic
("contract: an `enqueue_keyed` app key names the job's external identity — it
cannot be empty"). One more hard limit: all of a
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
The exceptions are an `Io` carrying one of `Group::stage`'s two refusals,
each read with its own `of`. A `JobShapeRefusal`
(`charpente::core::db::JobShapeRefusal::of`) is an enqueue under another shape
of a job name this binary mounts (§6, "Changing a job's fields"). A
`TableWriteRefusal` (`charpente::core::db::TableWriteRefusal::of`) is a write
the `Schema` disagrees with about its table: a table it does not register
(add `.table::<T>()`), a scoped `Row` whose `TABLE` is an unscoped table's or
the reverse, or a put or delete through any Rust type other than the one
registered under that name. A `#[table]` type's table name is its struct
name, so two `#[table] struct Invoice` in different modules collide this way
even when their fields, and so their fingerprints, are the same. The text
names the type that wrote and the registered one: write the registered type,
or give the other one another name. Either refusal comes before anything was
written, so a retry cannot succeed. The HTTP layer answers
either with a 500 carrying the refusal's own text, and in a job handler
either is an attempt, retried and then dead-lettered. The broader
`Refusal::of` is not that predicate: it also matches a refusal apply raises
after the log sync (a replicated record decoded under a mismatched shape),
whose record may already be durable. A read through a type other than the
registered one, a `Tx` or `ScopedTx` read or an `across_tenants` scan, is
refused too, by the read itself, as a `TableReadRefusal`
(`charpente::core::db::TableReadRefusal::of`). It names the table, the type
that read and the registered one, offers the same two fixes, and returns no
row. It reaches your code through the read's `io::Result`, so a route that
returns it with `?` answers a 500 carrying its text, and in a job handler it
is an attempt.
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
one of `GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `SSE`; exactly one of `, auth`,
`, public` or `, public(Org)` follows the path — a route says which callers
reach it, and `public(Org)` adds that every caller may act inside the `Org`
tenant it opens (see "A route's stance" below); a route that says none is a
compile error naming all three; `auth` is covered under Sessions below). The
macro checks the
function's argument list against the path and emits the glue that extracts,
decodes, and dispatches to it:

- The first argument must be named exactly `tx`, typed `&mut Tx`.
- Then one argument per `:param` in the path, **in path order**, each named
  exactly like its param (`:id` needs an argument named `id`). Its type
  must implement **both** `FromParam` (parses the path segment; a bad one
  fails as a named 400, not a panic) **and** `Wire` (§13 — the client needs
  a schema for it too). `String` and `Id<T>` (any `T`) satisfy both. `u64`
  implements `FromParam` but not `Wire` — a bare `u64` path param is a
  compile error naming the fix (`Id<T>` for a row id, or `WireU64`); use
  `Id<T>` for a row id.
- Then at most one JSON body argument, of any type that is both an owned
  deserializable AND `Wire` (`serde::de::DeserializeOwned + Wire` — the
  glue's actual bound); a type borrowing from the input won't compile, and
  neither will one missing `#[derive(Wire)]`.
- Then, optionally, a trailing argument named exactly `ctx`, typed `&Ctx`
  (session access — see Sessions below). `Ctx` borrows from the request's
  group (`Ctx<'g>`), and `&Ctx` elides that, so a handler never names it. A
  helper that returns something borrowed out of a `Ctx` must say which
  borrow: `fn subject_of<'c>(ctx: &'c Ctx) -> Option<&'c str>`. Left elided,
  it is `error[E0106]: missing lifetime specifier`.

The handler returns `Result<T, ApiError>` where `T: serde::Serialize + Wire`
— the `Result<_, _>` shape itself is enforced too, and named (a bare return
value is a compile error naming the fix, `-> Result<YourResponseType,
ApiError>`); on `Ok`, the glue emits `Response::json(200, &value)`.

```rust
use charpente::core::table::Id;
use charpente::http::{ApiError, App, AppConfig, Ctx};
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::{Schema, Tx, View, Wire, route, routes, table};
use serde::Deserialize;

#[table(global)]
struct Note {
    id: Id<Note>,
    text: String,
}

// A route's request body must be `Wire` too (§13) — `#[derive(Wire)]` sits
// alongside `Deserialize`, one for the schema, one for the actual parse.
#[derive(Wire, Deserialize)]
struct NoteBody {
    text: String,
}

// tx, then :id (named `id`, path order), then the JSON body, then `ctx`. A
// path param is a wire position too, so it is `Id<Note>`, not a bare `u64`.
#[route(POST "/notes/:id", public)]
fn create_note(tx: &mut Tx, id: Id<Note>, body: NoteBody, ctx: &Ctx) -> Result<Id<Note>, ApiError> {
    let _ = ctx; // available; this route just doesn't need a session
    tx.put(&Note {
        id,
        text: body.text,
    });
    Ok(id)
}

#[route(GET "/notes/:id", public)]
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

### Returning rows from a route: storage shape vs. wire shape

A handler's return type only needs `T: serde::Serialize + Wire` (above) —
nothing requires `T` to be a `#[table]` row, and `#[table]` does not derive
`Serialize` or `Wire` for you: a row's storage shape (`Id<T>`'s own `Codec`, §2) and
its wire shape are separate concerns on purpose, kept apart even though
`Id<T>` also implements `Serialize`/`Deserialize` — as a decimal string,
never a JSON number, because `u64` values above 2^53 lose precision
silently in JavaScript clients.

The fix is the one every other example in this guide already uses without
saying so: convert to a plain response type first. `Id<T>` belongs directly
in that view type — there's no need to unwrap it to a bare `u64` with
`.raw()` to make it serializable.

```rust
use charpente::http::{ApiError, App, AppConfig};
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::{Id, Schema, Tx, View, Wire, route, routes, table};
use serde::Serialize;

#[table(global)]
struct Note {
    id: Id<Note>,
    text: String,
}

/// The wire shape: `Id<Note>` crosses as a decimal string, so it appears
/// directly here — no `.raw()` unwrap, no separate id type. `#[derive(Wire)]`
/// is what lets this type appear in a route's response schema (§13).
#[derive(Wire, Serialize)]
struct NoteView {
    id: Id<Note>,
    text: String,
}

impl From<Note> for NoteView {
    fn from(row: Note) -> Self {
        NoteView {
            id: row.id,
            text: row.text,
        }
    }
}

#[route(GET "/notes/:id", public)]
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
use charpente::{Id, Tx, route, routes, table};

#[table(global)]
struct Note {
    id: Id<Note>,
}

// A path param is a wire position too — `Id<Note>`, not a bare `u64`.
#[route(GET "/notes/:id", public)]
fn by_id(tx: &mut Tx, id: Id<Note>) -> Result<Id<Note>, ApiError> {
    let _ = tx;
    Ok(id)
}

#[route(GET "/notes/:key", public)]
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
  any. `principal.subject()` is the subject the credential was minted for,
  and `principal.via()` names which rung authenticated it
  (`Via::Session(id)`, `Via::ApiKey(id)`, or `Via::Oidc`). Read the subject
  from here on any `auth` route, whatever the credential kind. A `Principal`
  is read-only to your code: its fields are private, so the only principals
  are the ones the ladder produced. It is not `Clone` either, so the only one
  your code holds is the `&Principal` this request's `Ctx` lends, which ends
  with the request: no principal can be parked in a `static` for a later
  request to hand `tx.open_scope`. So `Tenant::authorize` can trust the one
  it is handed: the ladder produced it, for the request that is asking.
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
when the principal is `None`. With `public`, the route always runs and
`ctx.principal()` is whatever the ladder resolved (possibly `None`) — though a
handler that then opens a tenant scope with no principal gets a 401 of its
own, unless the route declared `public(Org)` (below) — but a
*presented* credential is still resolved and rejected even on a public
route, so a garbage bearer never reaches a handler.

The 401s are named and fixed — copy these verbatim if you assert against
them:

<!-- pinned -->
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

#[route(POST "/login", public)]
fn login(tx: &mut Tx, subject: String, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_session(tx, subject))
}

#[route(GET "/me", auth)]
fn whoami(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let _ = tx;
    Ok(ctx
        .principal()
        .expect("auth routes always have a principal")
        .subject()
        .to_string())
}

#[route(POST "/logout", auth)]
fn logout(tx: &mut Tx, ctx: &Ctx) -> Result<&'static str, ApiError> {
    ctx.revoke(tx);
    Ok("bye")
}

// Mint an API key for the caller's own subject; revoke by deleting the row.
#[route(POST "/keys", auth)]
fn mint_key(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let subject = ctx.principal().expect("auth route").subject().to_string();
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

A verified token yields a `Principal` whose `via()` is `Via::Oidc` and whose
`subject()` is the token's `sub` claim — ephemeral, no row, nothing to revoke
but a short `exp`. `exp`/`nbf` are checked against the request clock with a
60-second skew tolerance. Each failed check is its own named 401 (the
`iss`/`aud` ones name presented-vs-configured; `<presented>`/`<configured>`
are the actual values):

<!-- pinned -->
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

### A route's stance, and who reaches a tenant

Every route declares one of three stances after its path, and the stance decides what
`tx.scope::<Org>(org, ctx)?` (§16) does for each caller:

| Stance | Who reaches the handler | What the scope line does |
|---|---|---|
| `auth` | a caller with a credential | runs `Org::authorize` once, about that principal |
| `public` | every caller | with a credential, the same; with none, the 401 below |
| `public(Org)` | every caller | admits every caller into the `Org` tenant it names; no `authorize` runs |

The scope line applies three rules, in order:

1. If the route declared `public(S)` for this very `S`, the scope is admitted by that
   declaration. No `authorize` runs, whoever the caller is. A credentialed caller whose
   membership check would refuse is admitted too: the route said every caller may act
   here.
2. Otherwise, with a principal present, `S::authorize` runs once, about that principal,
   and its `Err` is the handler's `Err`.
3. Otherwise the caller has no credential, and the request is answered 401, by name.
   Nothing below the scope line runs.

An `auth` request never reaches rule 3, because the `auth` gate refused the anonymous caller
before your handler ran. So your `authorize` is only ever asked about a real principal. A
`public(Org)` route that opens a *different* scope type gets rules 2 and 3 for it, and the
401 names `public(Org)` as the route's stance.

The rule-3 401, for a `POST /orgs/:org/incidents` declared plain `public`:

<!-- pinned -->
```text
authentication required — `POST /orgs/:org/incidents` is declared `public` and opened the tenant scope `Org` for a caller with no credential; only a route declared `public(Org)` admits one. Send "Authorization: Bearer <token>" or the charpente_session cookie, or, if every caller may act inside this tenant here, declare `public(Org)` in place of `public` in this route's `#[route(…)]` attribute, keeping its other flags. A tenant type is named here by its NAME; name the type in the attribute: `Org` is the tenant type `<path>`
```

The site is the word the route was declared with (`SSE` for a stream) and its pattern;
the scope is its tenant type's `NAME` (§16). Both fixes are there: the caller's (send a credential)
and the author's (declare the tenant public, if that really is the policy). The author's
names the change to make where the stance is written and restates no attribute, so a route's
other flags, `cross_tenant` or `emits(…)`, stay as they are. A `NAME` need not be how your
source spells the type (`billing::Org` and `crm::Org` take two), so the text ends with each
named type's `std::any::type_name` (`<path>` above), which says which type to write in the
attribute. On a route declared `public(BillingOrg)` that opened `CrmOrg`, it names both. §16's
"Who reaches a tenant" shows the three side by side.

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
  OIDC check); and, at the scope line inside it, `AnonymousScope(..)`, a
  scope line reached with no credential on a route that did not declare that
  tenant public (above). Their exact, named texts are in the auth and stance
  sections above — you rarely construct these yourself; the `auth` gate, the
  ladder and `tx.scope` emit them.
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

**Since group commit, one request's failure can be another request's 500 —
and a 500 can name a write that committed.** The kernel commits everything
it drained in one loop iteration under a single fsync, up to 64 requests.
An append or sync failure still commits no record in the batch — but "acks
nothing" would overstate it, and the fourth bullet below is the reason: a
request that ran ahead of every staged record observed only committed state,
so it is not invalidated and keeps its 200. What such a failure invalidates is
every request from the first staged record onward. The case with no analogue in
a one-at-a-time kernel is a failure to
*apply* a record after the batch has already synced: at that point every
record in the batch is on disk and none of them is in the store, because the
store takes a whole batch in one transaction — an apply failure anywhere rolls
every record in it back rather than leaving a prefix behind. So the platform
answers 500 to every request that staged a record in that batch, and to every
request that *read* one of those records through the batch's shared view. A
request that ran ahead of every staged record still keeps its 200: it only ever
observed committed state. Read it as the
rule above says: **a 500 never promises rejection, only the absence of
confirmation.** What that means in practice:

- **The write may be committed.** After an apply failure every record in the
  batch is durable and applies when the process restarts. The client was told
  "unknown"; the durable answer turns out to be "yes". Plan for it — this is
  a named contract, not an edge case you are expected to discover.
- **The reverse remains impossible.** No 200 is ever emitted for something
  that is not durable. Every reply in a batch is held until the commit
  returns, so the ack still follows the sync.
- **The node stops.** An apply failure means machine state no longer matches
  the log, so the engine poisons and the kernel stops rather than serving on
  (§9's "Poisoned-engine errors"). The fix is a restart, which replays the
  WAL and is idempotent.
- **A request that ran ahead of the failure keeps its answer.** The rule is
  what a request *saw*, not where it sat in the batch: a read that only ever
  observed committed state is not invalidated by a later sibling's failure.

Operationally this is the same epistemic class as any `TxError::Io`, so
handle it the same way: a blind retry is safe only where the operation is
idempotent in your own schema — the platform does not dedupe for you, and
§6 is explicit that an `enqueue_keyed` key is carried, not indexed. After the
restart, the honest way to learn what happened is to read the state back
rather than infer it from the status code.

### Starting the server

`charpente::serve::serve(addr: &str, app: App) -> io::Result<Server>` spawns
the kernel (requests, due jobs, and cron ticks — §6 and §7 ride the same
loop) on one thread and the hyper listener on another, then returns as soon
as both are up — it does not block. `Server::wait` blocks until the kernel
stops and returns the exit code the process should end with. A kernel that
stopped — on a platform IO failure or a named refusal, each printed on
stderr with its fix — must end the process, or a supervisor sees a live
process answering every request 500 and never restarts it.

```rust
use charpente::core::table::Id;
use charpente::http::{ApiError, App, AppConfig};
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::{Schema, Tx, View, Wire, route, routes, table};
use serde::Deserialize;

#[table(global)]
struct Note {
    id: Id<Note>,
    text: String,
}

#[derive(Wire, Deserialize)]
struct NoteBody {
    text: String,
}

#[route(POST "/notes/:id", public)]
fn create_note(tx: &mut Tx, id: Id<Note>, body: NoteBody) -> Result<Id<Note>, ApiError> {
    tx.put(&Note {
        id,
        text: body.text,
    });
    Ok(id)
}

#[route(GET "/notes/:id", public)]
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

    let server = charpente::serve::serve("0.0.0.0:8080", app)?;
    // `serve` returned once the listener was up; `wait` blocks until the
    // kernel stops. Exiting with its code is what lets a supervisor see a
    // stopped kernel and restart it (no graceful shutdown in v0).
    std::process::exit(server.wait());
}
```

### What `#[route]` rejects

Every rejection is a compile error naming the contract and the fix. The
first is the one every route meets once:

<!-- pinned -->
```text
`<handler>` declares neither `auth` nor `public` — a route says which callers reach it. Add one of these to the flags of its `#[route(<method> "<path>", …)]`, keeping any it already has: `auth` to require a principal (`ctx.principal()` is `Some`), `public` to serve anonymous callers, or `public(Org)` to also let every caller act inside the `Org` tenant it opens. None is the default.
`auth` and `public` on one route — a route is one or the other; drop one
`public` given twice — write it once
```

The rest are about the handler's shape:

```rust,ignore
// first argument isn't named `tx`
#[route(GET "/ping", public)]
fn ping(ctx: &Ctx) -> Result<&'static str, ApiError> {
    Ok("pong")
}
// "a #[route] handler's first argument must be `tx: &mut Tx` — `ping` is
//  missing it"

// argument count doesn't match the :param count
#[route(GET "/notes/:id", public)]
fn get_note(tx: &mut Tx) -> Result<String, ApiError> {
    Ok(String::new())
}
// "`get_note` takes 0 argument(s) after `tx` (excluding `ctx`) but the path
//  \"/notes/:id\" has 1 :param(s) — write one argument per :param (same
//  name, path order) plus at most one JSON body argument"

// argument name doesn't match the :param name
#[route(GET "/notes/:id", public)]
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

`#[job]` derives a job payload's `Codec`, its `NAME`, its `FINGERPRINT`,
and the `JobEntry` glue `jobs![...]` collects — the same shape `#[table]`
derives for rows (§2), minus the primary key. `Job::FINGERPRINT` is a
structural fingerprint of the payload's fields (§12's helper), recorded
beside every enqueue; a Pending job is decoded only by a binary whose
`FINGERPRINT` matches (see "Changing a job's fields" below).

A job says which tenancy it has, and neither is the default: `#[job(global)]`
belongs to no tenant, and `#[job(scoped_by = Org)]` acts inside one (below, and
§16). A bare `#[job]` is a compile error naming both. Either form takes
`RetryPolicy::DEFAULT` (5 attempts, 30s base backoff), and adding `retry(max =
<int>, backoff = "<n><unit>")` overrides either or both. `backoff` is a bare
number plus a unit, one of `ms`, `s`, `m`, `h`. The words `global` or
`scoped_by`, `cross_tenant` (§16), `retry(...)`, and `outbound = "<host>"` with
its optional `timeout = "<n><unit>"` ("Outbound jobs" below) may come in any
order, each at most once.

```rust
use charpente::core::job::RetryPolicy;
use charpente::kernel::JobError;
use charpente::{Job, Tx, job};

#[job(global)]
struct Basic {}

impl Basic {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

#[job(global, retry(max = 3, backoff = "10s"))]
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
JobError>`. That is a `global` job's. A `scoped_by = Org` job's `run` takes
`tx: &mut ScopedTx<'_, '_, Org>` instead (next subsection), and `cross_tenant`
adds `cap: &CrossTenant` after `tx` in either form. An outbound job writes `call`
and `settle` in place of `run` ("Outbound jobs" below). It runs as its own
transaction, so it reads and writes through
`tx` exactly like a route handler or the closure in §4's `db.transact`.
`JobError: From<io::Error>` (mapping to `JobError::Io`), so `?` works on
`tx` reads the same way it does with `ApiError`:

```rust
use charpente::core::table::Id;
use charpente::kernel::JobError;
use charpente::{Tx, View, job, table};

#[table(global)]
struct Counter {
    id: Id<Counter>,
    hits: u64,
}

#[job(global)]
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

### A job inside a tenant: `scoped_by`

A job that acts inside one tenant says so, and then it runs in the tenant it was
enqueued from. It is enqueued from inside that tenant's scope: `tx.enqueue` on the
`ScopedTx` a handler's `tx.scope::<Org>(org, ctx)?` returns. The enqueue records the
tenant beside the job, and when the job runs, its `run` receives that tenant's
`ScopedTx`. No `authorize` runs for it: a job has no principal, and its authority is
its enqueuer's, who was inside the tenant already — and nobody else's, since no app code
can write the row that carries it, and an enqueue inside a tenant of another scope type is
refused (both below). Its retries run in the same tenant,
and a follow-up it enqueues through its own `ScopedTx` inherits the tenant too.

```rust
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::kernel::JobError;
use charpente::{Id, Scoped, ScopedTx, Tenant, Tx, View, job, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, who: &Principal, scope: Id<Org>) -> Result<(), ApiError> {
        if who.subject() == format!("org:{}", scope.raw()) {
            Ok(())
        } else {
            Err(ApiError::BadRequest(format!(
                "not a member of org {}",
                scope.raw()
            )))
        }
    }
}

#[table(scoped_by = Org)]
struct Invoice {
    id: Id<Invoice>,
    receipts: u32,
}

#[table(global)]
struct Mailer {
    id: Id<Mailer>,
    sent: u64,
}

#[job(scoped_by = Org)]
struct SendReceipt {
    invoice: u64,
}

impl SendReceipt {
    fn run(self, tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        // Inside the org it was enqueued in, exactly as the handler was.
        if let Some(mut inv) = tx.get(Id::<Invoice>::from_raw(self.invoice))? {
            inv.receipts += 1;
            tx.put(&inv);
        }
        // A global table, through the deref: `run` holds `&mut ScopedTx`,
        // so it is `**tx` here where a handler writes `*tx`.
        let sent = (**tx).get(Id::<Mailer>::from_raw(0))?.map_or(0, |m| m.sent);
        (**tx).put(&Mailer {
            id: Id::from_raw(0),
            sent: sent + 1,
        });
        Ok(())
    }
}

#[route(POST "/orgs/:org/invoices/:id/receipt", auth)]
fn send_receipt(tx: &mut Tx, org: Id<Org>, id: Id<Invoice>, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.get(id)?.ok_or(ApiError::NotFound)?;
    tx.enqueue(SendReceipt { invoice: id.raw() }); // runs in this org
    Ok(())
}
```

`ScopedTx::enqueue` takes a job scoped by that same type, so a job scoped by another type
is a type error (E0271: rustc cannot resolve the job's `Scope` to `Org`, and points at the
`Team` its `#[job]` names). So is a `global` job: it is enqueued inside a scope through
the explicit deref, `(*tx).enqueue(job)` in a handler and `(**tx).enqueue(job)` in a
scoped job's `run`. A scoped job on a bare `Tx`, where no tenant is named, is refused with
the note shown in "What `#[job]` rejects". Cron takes `global` jobs only (§7), because a
tick has no tenant. A sweep that must act in every tenant fans out instead (§7).

**Job rows are the platform's.** The tenant a job runs in is the one its row carries, and
the dispatcher opens it without asking anyone, so no app code writes a job row. The
queue holds every tenant's payloads, so no app code reads one either: `tx.get`, `tx.scan`,
`tx.put` and `tx.delete` of a `JobRow` do not compile, and §8's oracles read the queue
through `db.snapshot()` (the read's text is in §16, "Where the guarantee stops"). Only the
enqueue family creates a job row, and only the dispatcher settles one. There is no
app-side cancel either: a job runs until it is Done or Dead. The platform's two other
tables are sealed the same way, against reads and writes. The event log (`EventRow`, §11) is what
every subscription's feed is read from, so only an emit writes an event row. The
migration record (`SchemaVersion`, §12) is what the migration gate trusts for each table's
version, so only a table's first write and `Db::migrate_schema` write one. rustc prints
this for a write of any of the three, with your row type in place of `<row>`, and one note
per table, since one bound refuses all three:

<!-- pinned -->
```text
`<row>` rows are the platform's, and app code cannot write them
job rows are the platform's: only the enqueue family writes one, so a job runs in the tenant its enqueuer opened or its fan-out named, and no app code cancels one — to run a job, enqueue it:
    tx.enqueue(job);
for a `global` job; for a `scoped_by` job, inside the tenant it acts in:
    let mut tx = tx.scope::<Org>(scope, ctx)?;
    tx.enqueue(job);
or from a `cross_tenant` route or job, naming the tenant:
    tx.enqueue_in::<Org>(scope, job, cap);
event rows are the platform's: only an emit writes one, because every subscription's feed is read from them — to publish an event, emit it with `tx.emit(event)`, or, inside a tenant, with `emit` on the `ScopedTx` of the tenant it belongs to
schema-version rows are the platform's: only a table's first write and `Db::migrate_schema` write one, because the migration gate trusts them for each table's version and a migration's resume point — to change a table's version, declare it with `#[table(version = N)]` and its `TableMigration`, and the kernel migrates at startup
```

The seal is on the table's name as well as its type, because apply routes a write by
table name. A hand-written `Row` whose `TABLE` is one of the three names fails to build
at each write the binary can reach, and rustc names the call site and the table. It is a
post-monomorphization error, so `cargo build` and `cargo test` report it, and `cargo
check` does not:

<!-- pinned -->
```text
this Row's TABLE is "charpente.jobs", the platform's job queue — job rows are the platform's: only the enqueue family writes one, because the dispatcher runs a job in the tenant its row names; give this table a name of its own, and run a job by enqueueing it (`tx.enqueue`, `ScopedTx::enqueue`, or `tx.enqueue_in` from a `cross_tenant` route or job)
this Row's TABLE is "charpente.events", the platform's event log — event rows are the platform's: only an emit writes one, because every subscription's feed is read from them; give this table a name of its own, and publish an event by emitting it (`tx.emit`, or `emit` on the `ScopedTx` of the tenant it belongs to)
this Row's TABLE is "charpente.schema_versions", the platform's migration record — schema-version rows are the platform's: only a table's first write and `Db::migrate_schema` write one, because the migration gate trusts them for each table's version and a migration's resume point; give this table a name of its own, and change a table's version by declaring it (`#[table(version = N)]` with its `TableMigration`), which the kernel migrates at startup
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

#[table(global)]
struct Counter {
    id: Id<Counter>,
    hits: u64,
}

#[job(global, retry(max = 3, backoff = "10s"))]
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
carries a key: the app's own identity for the job. It is not the idempotency
key an outbound call sends: the platform mints that one itself, for every
outbound job, keyed or not ("Outbound jobs" below). On a bare `Tx` these take
`global` jobs. A `scoped_by` job is enqueued the same two ways on its tenant's
`ScopedTx` (above). Inside the deterministic boundary
the key means nothing in v0 — it is carried, never indexed or deduped on,
because exactly-once state effects here are structural (below), not
key-based. An empty key is a contract panic: ``"contract: an `enqueue_keyed`
app key names the job's external identity — it cannot be empty"``.

```rust
use charpente::core::table::Id;
use charpente::http::ApiError;
use charpente::kernel::JobError;
use charpente::{Tx, View, job, route, table};

#[table(global)]
struct Invoice {
    id: Id<Invoice>,
    finalized: bool,
}

#[job(global)]
struct SendReminder {
    invoice: u64,
}

impl SendReminder {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

#[route(POST "/invoices/:id/remind", public)]
fn remind(tx: &mut Tx, id: Id<Invoice>) -> Result<(), ApiError> {
    let mut inv = tx.get(id)?.ok_or(ApiError::NotFound)?;
    inv.finalized = true;
    tx.put(&inv);
    // `SendReminder.invoice` stays a bare `u64`: a job payload is never
    // itself a wire type (it never crosses HTTP), so it has no `Wire`
    // requirement — only `#[route]` params/bodies/responses/errors do.
    tx.enqueue_keyed(
        format!("reminder:{}", id.raw()),
        SendReminder { invoice: id.raw() },
    );
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
it (absorbed as `"handler panicked: <payload>"`), or a payload that does not
decode under its own recorded shape (`"job decode for <Name>: <e> — its
recorded shape is this binary's, so either …"`: corrupt bytes, or a change
inside a type one of its fields holds, §12; a Pending job of another shape is
refused before dispatch can start, and this binary refuses to enqueue one —
see "Changing a job's fields") — that attempt's own effects never committed, and a *second*
transaction records the failure: `attempts` increments, `last_error` gets
the message, and:

- if `attempts < retry.max`: stays Pending, `due_at` moves to
  `now + backoff * 2^(attempts - 1)` (nanoseconds; doubles every attempt,
  saturating rather than overflowing at absurd attempt counts) — the
  policy's configured base `backoff` is attempt 1's wait.
- if `attempts >= retry.max`: status becomes Dead. Dead rows are retained,
  with `last_error` as the record of what killed them.

On `Err(JobError::Io(e))` — a platform fault the handler's own reads
surfaced, or one from the transaction machinery itself — none of this
happens: it is **not an attempt**, a `TableReadRefusal` excepted: it is
the handler's own deterministic bug, an attempt (§4). Otherwise the row
stays Pending, untouched, and
the error bubbles up (restart-and-recover, same as a request-path `Io`
failure in §5) instead of silently burning retry budget on a healthy job
that just hit a bad disk. `JobError::Io`'s own `Display` names this:
`"platform IO failure during the handler (not an attempt): <e>"`.

One more failure mode: if a `JobRow`'s `name` isn't in the `jobs![...]` list
`App::mount_jobs` was given — a job enqueued by a since-removed or renamed
type — it fails the same way as a handler rejection, with: `"job
\"<name>\" is not registered — add it to the jobs![…] list passed to
App::mount_jobs (or this log outlived a renamed job type)"`.

### Outbound jobs: calling the world after commit

A job that calls an outside service over HTTP declares the host it calls with
`outbound = "<host>"`, and writes two methods in place of `run`. `call` reads what
it needs and builds the request. The kernel sends it outside any transaction, since
no transaction waits on the network, and hands the reply to `settle`, which commits
the reply's consequences in the transaction that marks the job Done.
Enqueue one the way you enqueue any job, from a route, a job or a cron tick:
`enqueue`, `enqueue_keyed`, `enqueue_in` and cron take outbound jobs unchanged.

```rust
use charpente::kernel::JobError;
use charpente::outbound::{Call, IdempotencyKey, Reply};
use charpente::{Tx, job};

#[job(
    global,
    outbound = "pay.test",
    retry(max = 3, backoff = "1s"),
    timeout = "2s"
)]
struct Charge {
    cents: u64,
}

impl Charge {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(
            Call::post("/charges")
                .header("idempotency-key", key.as_str())
                .form(&[("amount", self.cents.to_string())]),
        ))
    }

    fn settle(self, _tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        match reply.status() {
            200 => Ok(()),
            s => Err(JobError::app(format!("pay.test answered {s}"))),
        }
    }
}
```

The two methods have exactly these shapes. A `global` job's:

```text
fn call(&self, tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError>
fn settle(self, tx: &mut Tx, reply: Reply) -> Result<(), JobError>
```

A `scoped_by = Org` job's take its tenant's `ScopedTx`, as its `run` would:

```text
fn call(&self, tx: &ScopedTx<'_, '_, Org>, key: &IdempotencyKey) -> Result<Option<Call>, JobError>
fn settle(self, tx: &mut ScopedTx<'_, '_, Org>, reply: Reply) -> Result<(), JobError>
```

`call` takes a shared `&Tx`, so it reads and cannot write: a `tx.put` inside it is
a compile error, and so is a scoped job's `call` or `settle` taking a bare `Tx`
(§9 lists both). Whatever the call's outcome should change, `settle` writes. When
there is nothing to call any more (the order was cancelled after the enqueue, say),
`call` returns `Ok(None)`, and the job is done without a call.

`key` is the job's idempotency key. The platform mints it when the job is
enqueued, 16 bytes from the node's entropy rendered as 32 lowercase hex
characters, and it stays the same across every retry and every re-dispatch after
a crash or a failover. charpente sends it nowhere by itself: `call` places it
where the remote reads one, the `idempotency-key` header above or a field of the
body, so a remote that honours it applies a repeated request once. It is not
`enqueue_keyed`'s app key.

Because the key is drawn as the enqueue is staged, an outbound job is enqueued
only in a transaction the platform opens: a route's, a job's or a cron tick's.
`Db::transact` and `Db::begin_group` (§4) cannot mint, and an outbound enqueue
inside either panics with a contract message (§9). Code that opens its own group
stages through `Db::begin_group_minting` instead, and a test seeds an outbound job
through `App::transact_for_tests`, which the message names.

The request is built on the declared host, which is never runtime data:
`Call::get`, `post`, `put`, `patch` and `delete` take a path that begins with `/`.
`.header(name, value)` sends the name lowercase. `.json(&value)` and
`.form(&[(name, value)])` set the body and its `content-type`, and `.body(bytes)`
sets a raw body with none. A call carries one `content-type`, so setting another
(through `json`, `form` or `header`) replaces the earlier one. `settle` receives
the reply of any status, a 3xx included, since a redirect is never followed:
`reply.status()`, `reply.header(name)` (the first of that name, case-insensitively),
`reply.body()`, and `reply.json::<T>()`, which is a `JobError::App` when the body is
not `T`'s JSON.

The host is a bare DNS name like `api.stripe.com`. A scheme, port, path or
userinfo in it is a compile error, because where the host resolves is the
operator's to say. `timeout` bounds the wait for the remote's reply, with
`backoff`'s syntax; it defaults to 30 s, and it is refused on a job without
`outbound`. `cross_tenant` beside `outbound` is refused: an outbound call runs in
the one tenant its job was enqueued in. An outbound job's `impl` that has `run`
instead of `call` and `settle` fails to compile too. §9 lists each refusal's
text.

Outbound-ness is part of a job's shape: an outbound job's `FINGERPRINT` continues
its fingerprint as a pure job (`charpente::core::db::outbound_fingerprint`), so
adding `outbound` to a job, or removing it, is a shape change. It is refused while
Pending jobs of the old shape exist, and the ways through are the two "Changing a
job's fields" below describes: drain first, or a new type. The host is not part of
the shape, so changing the host is not refused.

### Changing a job's fields

Every enqueue records the payload's shape — `Job::FINGERPRINT`, the structural
fingerprint `#[job]` computes from the fields (§12) — and a Pending job is only ever
decoded by a binary that compiled that same shape. A node about to become able to
dispatch refuses while any Pending job of a type it mounts was enqueued under another
shape. It checks at startup and at every moment its write gate opens (a witness gate
opening, a completed switchover), and `promote` checks before it changes anything:

<!-- pinned -->
```text
<n> Pending "<job>" job(s) (ids <ids>) were enqueued under a different shape of <job> (recorded <recorded>; this binary <fingerprint>) — this binary will not decode them; run the binary that enqueued them until they are Done or Dead, then deploy this one, or restore whichever of <job>'s fields, its `global`/`scoped_by` declaration, its `outbound` declaration or its tenant type's `NAME` changed, and give the new shape its own #[job] type under a new name (GUIDE §6)
```

A job's tenancy is part of its shape. A `scoped_by` job's fingerprint continues its
fields' fold over its tenant type's `NAME` (§16), so moving a job between `global` and
`scoped_by`, or to a tenant type with another `NAME`, is refused the same way. A deploy
therefore never hands a row enqueued with no tenant to a scoped `run`, one enqueued in a
tenant to a global one, or one enqueued in a tenant of one `NAME` to a job of another.
Respelling the `scoped_by` path is not a shape change while it names a type of the same
`NAME`: see below, after the scope-type refusal, for what that leaves unchecked.

At most five ids are listed, then `…`. A job enqueued before shapes were recorded reads
`recorded none — enqueued before job shapes were tagged`, and it is refused the same way:
nothing decodes bytes it cannot prove are its own shape.

- At startup, or when the write gate opens, the refusal stops the kernel, and a stopped
  kernel ends the process (§5).
- At `promote`, it answers `CtrlErr` prefixed `cannot promote:`, and the node stays a
  follower.
- A switchover is never refused partway. Once the old primary hands off it never resumes
  on its own (DESIGN §5 calls the hand-off its point of no return), so a refusal there
  would leave neither node writable. The switchover completes. The new primary's kernel
  then stops at the moment it becomes writable, with the refusal above, and its fix works
  from there: run the binary that enqueued the jobs on that node, now the primary, until
  they are Done or Dead. The old primary follows it throughout.

Done and Dead rows are never decoded again and never refuse anything. A Pending row of a
job type this binary does not mount keeps the "not registered" path above.

This binary never writes a foreign-shape row itself. `NAME` is the type's bare identifier,
so two `#[job]` types can share one: a route that still enqueues `v1::SendReceipt` in a
binary that mounts `v2::SendReceipt`, say. From `App::mount_jobs` on — whether or not a
kernel exists yet — an enqueue under another shape of a mounted name refuses its whole
transaction before anything is written:

<!-- pinned -->
```text
job "<job>" was enqueued under a different shape of <job> than this binary mounts (enqueued <shape>; this binary <fingerprint>) — two #[job] types share the name <job>; the transaction was refused and nothing was written, so a retry cannot succeed — enqueue the mounted type, or give the other type its own name (GUIDE §6)
```

A route answers it with a 500 carrying that text, never the retryable storage-failure 500
(§5). Inside a job handler it is an attempt, like a panic: the job retries and then
dead-letters, with the text as its `last_error`. In a cron schedule's enqueue it stops the
kernel. An enqueue of a job type this binary does not mount at all is not refused here: it
commits and takes the "not registered" path.

Tenancy has a twin of both refusals. `#[job]` fingerprints `scoped_by`, but a
hand-written `impl Job` can copy a mounted job's `NAME` and `FINGERPRINT` under the other
tenancy, and a hand-written `JobDef` can mount a job under the wrong one. Either would
hand the dispatcher a row with a tenant for a `global` job, or with none for a `scoped_by`
one. So an enqueue whose tenant presence disagrees with the mounted declaration refuses
its transaction the same way, and a Pending row that already disagrees is refused
wherever the shape refusal runs. `<presence>` is `a tenant` or `no tenant`, and
`<declaration>` is `` `global` `` or `` `scoped_by = Org` ``:

<!-- pinned -->
```text
job "<job>" was enqueued with <presence>, but this binary mounts <job> as <declaration> under the same shape (<shape>) — a hand-written `impl Job` or `JobDef` contradicts <job>'s #[job]; the transaction was refused and nothing was written, so a retry cannot succeed — enqueue the mounted type, give the other type its own name, or mount <job> as its #[job] declares it (GUIDE §6)
<n> Pending "<job>" job(s) (ids <ids>) carry <presence>, but this binary declares <job> <declaration> — the enqueue family writes a tenant exactly for a `scoped_by` job, so something that contradicts <job>'s #[job] wrote these rows: a hand-written `impl Job` or `JobDef`, or a raw store write; this binary will not dispatch them; mount <job> as its #[job] declares it, or restore the store from a replica or backup taken before these rows were written. For a follower, or a primary once a follower is promoted in its place, that is a re-seed: stop the node, delete its data dir, and restart it configured as a follower of the current primary so it snapshot-seeds from it, never with a primary config: the durable role file that outranks config is deleted with the data dir, so a cleared node started as a primary comes up as a second writable primary (GUIDE §6)
```

Presence is not the whole of a tenant. A row records the tenant's id and not the scope type
it is an id of, and the dispatcher opens that id as the type the mounted job declares. The
fingerprint tells two scope types apart only by their `NAME`s. A `#[job(scoped_by = Desk)]`
in a module that says `use super::Board as Desk;` fingerprints with `Board`'s `NAME`, so it
is refused as another shape. But a `#[job]` whose `scoped_by` names another tenant type
with the same `NAME` fingerprints alike, and a hand-written `impl Job` can copy both
constants under any `type Scope`. Enqueued inside a tenant of that other type, either would
run the mounted job in the same id of the type it declares, a tenant nothing opened or
named for it. So an
enqueue whose job's scope type is not the one the mounted job declares refuses its
transaction the same way. `<scope>` is the enqueuing job's tenant type's `NAME` and `<declared>` the
mounted one's (§16); `<id>` is the tenant id. The two NAMEs can be equal. A hand-built `JobDef` that
names one type and carries another's `scope_type` is refused here on the honest enqueue, which is why
the text says "with another scope type" and lists that cause:

<!-- pinned -->
```text
job "<job>" was enqueued in a `<scope>` tenant, but this binary mounts <job> as `scoped_by = <declared>` with another scope type under the same shape (<shape>) — a job row records its tenant's id and not its type, so tenant <id> would be read as the mounted declaration's type, not the one it was enqueued in; either a type standing in for <job>'s #[job] was enqueued (a hand-written `impl Job`, or a #[job] whose `scoped_by` names another tenant type with the same `NAME`), or a hand-written `JobDef` mounts <job> under a `scope_type` its #[job] does not declare; the transaction was refused and nothing was written, so a retry cannot succeed — enqueue the mounted <job> from a `<declared>` scope, give the other type its own name, or mount <job> as its #[job] declares it (GUIDE §6)
```

One door is left inside a binary, and it is a hand-built `JobDef`. Its fields are public,
so `JobDef { glue: <another job's glue>, ..<File as JobEntry>::def() }` mounts a glue that
opens a different scope type than `scope_type` names, and so does a `JobDef` whose
`scoped_by` and `scope_type` both name a type its glue does not open. `Group::stage` holds
the enqueuer to `scope_type`, and a scoped job's glue opens its row's tenant through
`JobScope::open`, which asserts that the type it opens is `scope_type`. The assert runs
inside the attempt, so the job retries and then dead-letters, and nothing runs in either
tenant. Its `last_error`, for `File`'s `JobDef` carrying a `Board` job's glue, is below.
`<path>` is `Board`'s `std::any::type_name`, printed beside its `NAME` so that a glue whose
type shares the declared `NAME` still reads as two types:

<!-- pinned -->
```text
handler panicked: contract: the glue mounted for job "File" opened its tenant 31 as `Board` (the tenant type `<path>`), but that `JobDef` declares `scoped_by = Desk` with a `scope_type` that is not `<path>` — a hand-built `JobDef` whose `glue` or `scope_type` disagrees with its #[job] (a glue taken from another job, or another type's `TypeId`); this attempt wrote nothing, and every retry fails the same way — mount File through `jobs![…]`, which builds its `JobDef` from its #[job] (GUIDE §6)
```

A `JobDef` whose glue and `scope_type` name the same other type passes the assert. Nothing
can tell it apart, and it does not cross a tenant: `Group::stage` held the enqueuer to
that same type, so the job runs in the tenant it was enqueued in, only with another job's
code. `jobs![…]` builds every `JobDef` from its `#[job]`, and it is the way to mount one.

This one has no startup twin, because a row carries no type to compare, only the `NAME`
its fingerprint mixes. The check runs where both types are known, at the enqueue, in a
binary that mounts the job. At startup the dispatching binary compares a Pending row's
shape, so a row whose job named a tenant type of another `NAME` is refused there by the
foreign-shape refusal above. That covers the refactor that turns a job module's `use
billing::Org;` into `use crm::Org;`, whenever the two types' NAMEs differ. What a later
binary that mounts the name dispatches unchecked, in the scope type it declares, is a row
whose shape matches:

- a row a hand-written `impl Job` enqueued from a binary that did not mount the job name,
  copying the job's `NAME` and `FINGERPRINT` under another `type Scope`;
- a row enqueued under a tenant type with the same `NAME` as the mounted one. A `NAME` is
  the tenant's identity, so this is the same tenant by declaration: giving a type another
  type's `NAME` declares it that tenant, and one app cannot mount both (§16).

Where the recorded shape matches the mounted one, neither is refused, at startup or at
dispatch. So before a deploy that mounts a scoped job the running binary does not mount,
drain that job: run the binary that enqueued its Pending rows until every one is Done or
Dead, the first way through a shape change below.

There are two ways through a shape change:

- **Drain first.** Run the binary that enqueued them — the old one after an upgrade, the
  newer one after a rollback — until every Pending job of the type is Done or Dead (each
  reaches one within its retry policy), then deploy. In a replicated pair, drain on the
  primary before a switchover onto a follower running another shape, or that follower
  stops as described above.
- **A new type.** Leave the old type's fields alone, put the new fields in a new `#[job]`
  type under a new name, mount both, and switch enqueues to the new one. The name is not
  optional: `NAME` is the type's own identifier, and `mount_jobs` panics on two jobs of
  one name. The old type keeps dispatching its rows, because their shape is still its
  own; remove it once none are Pending.

What the fingerprint cannot see, a change inside a type a field holds, is in §12.

### What `#[job]` rejects

Every rejection is a compile error naming the contract and the fix. The first is the
one every job meets once, and the next three are its neighbours:

<!-- pinned -->
```text
`<job>` declares no tenancy — a job says which. Add one of these to its `#[job(…)]`, keeping any arguments it already has: `scoped_by = Org` if it acts inside one tenant (it is enqueued from inside that tenant's scope, and its `run` receives that tenant's `ScopedTx`), or `global` if it belongs to no tenant (the only kind cron can schedule). Neither is the default.
`global` and `scoped_by = <scope>` on one job — a job is one or the other; drop one
`global` given twice — write it once
`scoped_by` given twice — write it once
```

A `scoped_by` job enqueued where no tenant is named — `tx.enqueue` on a bare `Tx`, or
`App::schedule` — is an unsatisfied bound, and rustc prints this note with your job and
scope in place of `<job>` and `<scope>` (§16 shows it rendered):

<!-- pinned -->
```text
`<job>` is a tenant-scoped job (`#[job(scoped_by = <scope>)]`) and a bare `Tx` names no tenant to run it in
enqueue it from inside the tenant it acts in, and it runs in that tenant:
    let mut tx = tx.scope::<<scope>>(scope, ctx)?;
    tx.enqueue(job);
or fan it out from a `cross_tenant` route or job, naming the tenant:
    tx.enqueue_in::<<scope>>(scope, job, cap);
cron (`App::schedule`) enqueues on a bare `Tx`, so it schedules `global` jobs only
```

The rest are about the attribute's syntax and the struct's shape:

```rust,ignore
// a word the attribute does not recognise
#[job(bogus)]
struct Foo {}
// "a job declares `global` or `scoped_by = <Type>`, and may add `cross_tenant`,
//  `retry(max = <int>, backoff = \"<n><unit>\")` (unit is one of ms, s, m, h),
//  and `outbound = \"<host>\"` with an optional `timeout = \"<n><unit>\"`, in
//  any order — e.g. `#[job(scoped_by = Org, retry(max = 3, backoff =
//  \"10s\"))]`, `#[job(global, cross_tenant)]` or `#[job(scoped_by = Org,
//  outbound = \"api.stripe.com\", timeout = \"30s\")]`"

// backoff with no unit suffix
#[job(global, retry(max = 3, backoff = "10"))]
struct Bar {}
// "backoff \"10\" has no unit — a job declares `global` or `scoped_by =
//  <Type>`, and may add `cross_tenant`, `retry(max = <int>, backoff =
//  \"<n><unit>\")` (unit is one of ms, s, m, h), and `outbound = \"<host>\"`
//  with an optional `timeout = \"<n><unit>\"`, in any order — e.g.
//  `#[job(scoped_by = Org, retry(max = 3, backoff = \"10s\"))]`,
//  `#[job(global, cross_tenant)]` or `#[job(scoped_by = Org, outbound =
//  \"api.stripe.com\", timeout = \"30s\")]`"

// enums and unions cannot be job payloads
#[job(global)]
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

A tick has no tenant, so `App::schedule` takes `global` jobs only; a `scoped_by` job there
is the §6 compile error. A sweep that must act in every tenant is a `global, cross_tenant`
job: it reads every tenant through the `CrossTenant` it declared, and fans a `scoped_by`
job out into each tenant it names with `tx.enqueue_in(org, job, cap)`. Each fanned-out job
then runs in its own tenant, exactly as if enqueued from inside it.

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

#[table(global)]
struct DigestState {
    id: Id<DigestState>,
    sent: u64,
}

#[job(global)]
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

#[job(global)]
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

A sweep that acts in every tenant, as the introduction describes: the schedule enqueues a
`global, cross_tenant` job, and that job fans a `scoped_by` job out into each tenant it
names.

```rust
use charpente::auth::Principal;
use charpente::http::{ApiError, App, AppConfig};
use charpente::kernel::JobError;
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::{CrossTenant, Id, Schema, ScopedTx, Tenant, Tx, job, jobs, table};
use std::time::Duration;

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        // Never asked by the sweep: a job has no principal.
        Err(ApiError::Unauthorized)
    }
}

#[table(scoped_by = Org)]
struct Window {
    id: Id<Window>,
    applied: bool,
}

/// Acts inside one org, the one the sweep named when it fanned this out.
#[job(scoped_by = Org)]
struct ApplyWindow {
    window: u64,
}

impl ApplyWindow {
    fn run(self, tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        if let Some(mut w) = tx.get(Id::<Window>::from_raw(self.window))? {
            w.applied = true;
            tx.put(&w);
        }
        Ok(())
    }
}

/// Reads every org's windows and fans one `ApplyWindow` into each window's org.
#[job(global, cross_tenant)]
struct MaintenanceSweep {}

impl MaintenanceSweep {
    fn run(self, tx: &mut Tx, cap: &CrossTenant) -> Result<(), JobError> {
        let windows = tx.across_tenants::<Org>(cap).scan::<Window>()?;
        for (org, window) in windows {
            if !window.applied {
                tx.enqueue_in(org, ApplyWindow { window: window.id.raw() }, cap);
            }
        }
        Ok(())
    }
}

fn build_app(data_dir: &str) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(ProdDisk::new(data_dir)?),
        Schema::new().table::<Window>(),
        Box::new(ProdEntropy::new()?),
        AppConfig::default(),
    )?
    .mount_jobs(jobs![ApplyWindow, MaintenanceSweep])
    .schedule("maintenance-sweep", Duration::from_secs(3600), || {
        MaintenanceSweep {}
    }))
}
```

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
#[table(global)]
struct AppClock {
    id: Id<AppClock>,
    day: u64,
}

const APP_CLOCK_ID: u64 = 0;

#[job(global)]
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
use charpente::{Schema, Tx, View, Wire, job, jobs, route, routes, table};
use serde::Deserialize;
use std::time::Duration;

#[table(global)]
struct Note {
    id: Id<Note>,
    text: String,
}

#[table(global)]
struct DigestState {
    id: Id<DigestState>,
    sent: u64,
}

#[derive(Wire, Deserialize)]
struct NoteBody {
    text: String,
}

#[job(global)]
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

#[route(POST "/notes/:id", public)]
fn create_note(tx: &mut Tx, id: Id<Note>, body: NoteBody, ctx: &Ctx) -> Result<Id<Note>, ApiError> {
    let _ = ctx;
    tx.put(&Note {
        id,
        text: body.text,
    });
    Ok(id)
}

#[route(GET "/notes/:id", public)]
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

    std::process::exit(charpente::serve::serve("0.0.0.0:8080", app)?.wait());
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
panic (both turn into the same kind of failure record). There is a third
ending — see "Giving up" below: a universe the injected faults stopped before
your property could be adjudicated at all. Say so with `sim.give_up(reason)`;
it is neither a pass nor a failure, and the report counts it separately.
`sim_tests!` lists
the workloads to run by name, each optionally followed by `require [...]` —
sometimes-coverage keys (`Sim::cover`, plus the sim's own auto-covered
`fault:*`/`crash:*`/`restart` keys) that must be hit at least once across
that test's universes in a batch run, or the batch fails even if every
universe individually passed. A `require`d key that never fires is telling
you something real: either the workload never drives the system into that
state, or the universe count is too low to make it likely.

```rust
use charpente::{Db, DbConfig, Id, Schema, TxError, View, table};
use charpente_sim::Sim;
use charpente_sim::checks::{boot_through_fallout, is_fault_fallout, replays_identically};

#[table(global)]
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
/// determinism checker below can reuse the exact same logic. `None` asserted
/// nothing, and a pass needs a verdict, so it gives the universe up ("Giving
/// up" below).
fn counter_roundtrip(sim: &mut Sim) -> Result<(), String> {
    if counter_roundtrip_result(sim)?.is_none() {
        sim.give_up("nothing to assert: the write never acked, or a boot never came up");
    }
    Ok(())
}

/// `replays_identically` is the determinism oracle: it runs `workload` twice
/// on two fresh `Sim`s seeded identically and demands both the return value
/// and the durable end-state match byte-for-byte.
fn counter_replays(sim: &mut Sim) -> Result<(), String> {
    replays_identically(sim.seed(), counter_roundtrip_result)
}

/// Puts a row, restarts the simulated disk (crashes and reopens), and reads
/// it back. Returns `None` — not a failure — when there is nothing to
/// assert: an injected fault kept the write from its ack, or either
/// `Db::open` never booted through fault fallout (the harness's own fault
/// paths, not this workload's contract, are under test elsewhere). Only a
/// `TxError::Io` can be fallout: the closure's own `App` error, or a
/// follower's refusal, is this workload's bug. `is_fault_fallout` recognizes the
/// sim's own injected-fault wording, the WAL/engine poison wording, and
/// redb's "close and re-open the database" latch; `boot_through_fallout`
/// boots again on those, and excuses a refusal for durable corruption only
/// where the harness injected some. Anything else is treated as a real bug
/// and fails the universe.
fn counter_roundtrip_result(sim: &mut Sim) -> Result<Option<u64>, String> {
    let schema = || Schema::new().table::<Counter>();
    let open = || Db::open(Box::new(sim.disk()), DbConfig::default(), schema());
    let Some(mut db) = boot_through_fallout(sim, 8, is_fault_fallout, open)
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    let acked = match db.transact(|tx| {
        tx.put(&Counter {
            id: Id::from_raw(1),
            hits: 1,
        });
        Ok::<_, std::io::Error>(())
    }) {
        Ok(()) => true,
        Err(TxError::Io(e)) if is_fault_fallout(&e.to_string()) => false,
        Err(e) => return Err(format!("{e:?}")),
    };

    sim.restart(); // power-cycles the simulated disk: unsynced writes tear or vanish
    drop(db); // the old handle is dead after restart — reopen, per its own contract

    let open = || Db::open(Box::new(sim.disk()), DbConfig::default(), schema());
    let Some(db) = boot_through_fallout(sim, 8, is_fault_fallout, open)
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
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

### Giving up: a universe that reached no verdict

Sooner or later a workload hits a universe where the faults are severe enough
that it cannot test its claim at all: the database never opened, the write was
never acknowledged, every recovery attempt was exhausted, or durable corruption
cut the very record the claim was about. That is not a pass —
nothing was proven — and it is not a failure either; the harness did exactly
what it was told.

Say so:

```rust
use charpente::{Db, DbConfig, Id, Schema, TxError, View, table};
use charpente_sim::Sim;
use charpente_sim::checks::{boot_through_fallout, durable_corruption_fired, is_fault_fallout};

#[table(global)]
struct Counter {
    id: Id<Counter>,
    hits: u64,
}

fn counter_survives(sim: &mut Sim) -> Result<(), String> {
    let schema = || Schema::new().table::<Counter>();
    // A boot that meets an injected fault restarts and boots again, as a
    // supervised process would: `Db::open`'s contract for an IO failure.
    let open = || Db::open(Box::new(sim.disk()), DbConfig::default(), schema());
    let Some(mut db) = boot_through_fallout(sim, 8, is_fault_fallout, open)
        .map_err(|e| e.to_string())?
    else {
        // Nothing to adjudicate: the disk never let the claim become testable.
        sim.give_up("Db::open never booted in 8 tries");
        return Ok(());
    };
    let put = db.transact(|tx| {
        tx.put(&Counter {
            id: Id::from_raw(1),
            hits: 1,
        });
        Ok::<_, std::io::Error>(())
    });
    match put {
        Ok(()) => {}
        // An injected fault kept the write from its ack: no claim to check.
        // Any other error is the platform's, and fails, as the opens do.
        Err(TxError::Io(e)) if is_fault_fallout(&e.to_string()) => {
            sim.give_up("the write was never acknowledged, so there is no claim to check");
            return Ok(());
        }
        Err(e) => return Err(format!("{e:?}")),
    }

    sim.restart(); // a crash: unsynced writes tear or vanish
    drop(db); // the old handle is dead after restart
    let open = || Db::open(Box::new(sim.disk()), DbConfig::default(), schema());
    let Some(db) = boot_through_fallout(sim, 8, is_fault_fallout, open)
        .map_err(|e| e.to_string())?
    else {
        sim.give_up("the reopen never booted in 8 tries");
        return Ok(());
    };
    match db.snapshot().and_then(|snap| snap.get::<Counter>(Id::from_raw(1))) {
        Ok(Some(_)) => Ok(()),
        // The one loss the platform excuses on a single node, and an excused
        // universe GIVES UP: the row's durability was not adjudicated, and a
        // pass needs a verdict.
        Ok(None) if durable_corruption_fired(sim) => {
            sim.give_up("durable corruption cut the acked row, so durability was not adjudicated");
            Ok(())
        }
        Ok(None) => Err("acked Counter row lost across a crash".to_string()),
        Err(e) if is_fault_fallout(&e.to_string()) => {
            sim.give_up(format!("the read-back hit fault fallout: {e}"));
            Ok(())
        }
        Err(e) => Err(e.to_string()),
    }
}
```

- `sim.give_up(reason)` records the give-up **on the universe**, not in the
  value you return. That is deliberate: a `Result` can be mapped back to
  `Ok(())` by a wrapper before the runner ever sees it, and for a long time
  that is exactly what happened — a property could report green having
  concluded nothing over most of its universes.
- First call wins; later ones are consequences of the first.
- **Give up and stop.** The record is sticky and nothing enforces terminality,
  so if you give up inside a retry arm and the loop then goes on to adjudicate,
  the universe is still reported as a give-up and that verdict is thrown away.
  `give_up` and the `return`/`break` belong on the same arm. This bites hardest
  in a multi-round checker on your own `sim` (`acked_implies_durable`).
- A workload that gives up and then returns `Err` is still a **failure**.
  Failures outrank give-ups.
- It also marks the coverage key `universe:gave-up` — once per universe, not
  once per call — so a give-up *rate* is counted like any other sometimes-key,
  and it cross-checks the reported count exactly. `ceiling:
  [("universe:gave-up", N)]` on `run_universes_swarm!`/`run_universes_swarm_with!`
  caps it: the batch fails when more than N universes gave up, naming the key,
  the count and the cap. Twenty of the platform's own swarm properties carry
  one:
  - kernel's exactly-once and job-tenancy properties;
  - the flagship properties of tenant, transactions, http, group_commit and
    typed_tables, and archive's two convergence properties;
  - realtime's flagship, scoped settled-client and tenant-feed properties,
    migrations' two, and the WAL suite's fault-matrix, bit-flip, misdirected-write, read-error,
    dir-entry and gc properties.

  The rest carry none yet, so a drift in their give-up rates is visible only in
  the split: realtime's mixed-shapes and follower properties, replication's,
  auth's, kernel's job-shape property, transactions' crashed-reseed property,
  and engine's. A give-up is
  reported and never failed, so without a cap a property can drift toward
  adjudicating nothing with every batch green; a floor on the key would bound
  give-ups from below, which guards nothing. An app's `sim_tests!` takes no
  `ceiling:`. Its property `require`s instead the key its verdict marks, past
  every give-up arm. That fails a batch in which no universe reached a
  verdict, but not one whose rate merely drifted.
- A cap bounds what a property fails to conclude, not how much it checks, so
  pair it with a `floor: [("key", N)]` on the key that counts its checks
  (written before `ceiling:`, on either macro). A change that skips a round's
  work without giving up meets every cap. At a pinned base the floor is the
  exact count; at an ambient one it is a margin, the mean over ten or more
  fresh batches less four standard deviations (§8).
- If your workload already funnels give-ups through a wrapper (the
  `Err("universe-inconclusive: …")` idiom the platform's own suites use), put
  the `sim.give_up(e)` call there, beside the `sim.cover(...)` mark. Keep
  both: the coverage keys are a floorable distribution over *causes*, and the
  outcome is the scalar the runner reports. Neither replaces the other.

Every batch now states the split, whether or not anything gave up:

```text
sim: 64 universe(s) per test, 36 verdict(s), 28 gave up, no failures (master seed: 0x0000000000005eed)
```

and a test where *every* universe gave up is called out by name, because a
property that adjudicated nothing is vacuous no matter how green it looks:

```text
sim: every universe of "counter_roundtrip" gave up — it adjudicated nothing over 64 universes; the property is vacuous until the faults it cannot survive are narrowed
```

**Where the split is visible, and where it is not.** `charpente test --sim`
always prints it: the sim binary is `harness = false`, so nothing captures its
output. The platform's own internal suites print the same line —

```text
charpente-sim [replication]: 38 of 128 universes reached a verdict, 90 gave up
```

— but that one goes to stderr from inside a libtest `#[test]`, so **a passing
`cargo test` swallows it.** Add `-- --nocapture` to see it (a failing test
shows it either way). So do not expect a CI run to surface give-ups; go
looking for them.

Read that example line the right way round: **38** of those 128 universes
adjudicated the property and **90** did not. It is a real measurement from
`wal_corruption_on_the_primary_no_longer_loses_acked_records_with_a_follower`,
and it means 70% of that property's universes prove nothing — which was
invisible before the split was printed, and is the entire reason it is.

### A fault-free tail for an end-of-universe check

A check that runs at the end of a universe — a checkpoint, a graceful close, a
byte comparison of two nodes' files — does IO, and the universe's
`FaultConfig` still applies to it. "No fault has fired so far" is a fact about
the past: the check's own fsync can still draw an injected failure. Call
`sim.quiesce_disk_faults()` first. For the rest of the universe, on every disk,
it disarms any pending crash countdown, and no injected read, write, sync or
directory-barrier error, misdirected write or restart bit flip fires after it.
Each skipped roll draws nothing, so a universe that never calls it
replays exactly as before. After it, an error in the check is a real failure,
not fallout to classify. Network and object-store faults are untouched, and a
restart still power-cycles: unsynced writes and directory entries collapse under
the crash model as usual (`crash_apply`, `crash_torn`, `dir_entry_loss`).

A realtime workload's settle phase has the same need for a different fault. The
subscriber-stall rate is the one fault axis the sim does not inject itself: a
workload reads `sim.subscriber_stall()` and drives its own sink's stall closure
at that rate. `sim.quiesce_subscriber_stalls()` makes that accessor read 0.0 for
the rest of the universe, so a stall closure built after it never refuses a send
and draws nothing. Call it before a phase whose verdict rests on a delivery
completing. A stall there is a fault the property excuses, so a stall that drops
the only clean attempt's tail leaves the universe with no verdict at all.

### Restarting the process, not the machine

`sim.restart()` is a power cycle: every unsynced write survives, tears or
vanishes before anything reopens. Most restarts in production are not that. A
supervisor restarting a crashed, killed or exited process finds the OS page
cache intact: every write the dead process made is still readable, synced or
not. `sim.restart_process()` models that. Every open file handle dies, exactly
as after `restart`, and nothing else happens. Unsynced writes and directory
entries stay pending and readable, and the next `restart()` still collapses
them.

The difference matters because readable is not durable. A node that reads back
what a dead process wrote and persists anything derived from it, before making
it durable, has built on data the next power loss can take away. A store
outran its own log exactly that way: recovery replayed a WAL frame whose fsync
had failed, the store checkpointed past it, and a power cycle dropped the frame.
Charpente now syncs what it reads back at boot before it acts on it. A property
that only ever power-cycles cannot reach that shape. One that mixes the two can.

The sim's kernel is an honest one: a failed sync leaves its writes pending, and
the next sync writes them. On Linux a failed fsync can instead mark the pages
clean, so the bytes stay readable and never reach the disk. No restart mode
models that, and no later sync can repair it.

Call `restart_process()` before you drop the old process's `Db` to model a kill:
its open files are dead, so nothing it flushes through them on the way out
reaches the disk. Only file handles are gated. A destructor that opens a file
afresh through a `Disk` it still holds would reach the disk; charpente's own
close goes through its open files. Call it after the drop to model an exit.

`sim.unsynced_files()` lists the files that hold writes no sync has taken yet.
Read it right after `restart_process()` to prove the restart left the next
process something unsynced to read back. A property that marks "read back an
unsynced write" from that list cannot be satisfied by a restart that quietly
flushed.

`restart_process()` draws from no random stream and clears no crash countdown,
so the sim gained it without re-keying anyone's universes. Calling it does
change everything your workload does afterwards, as any restart would. It marks
`restart:process`, never `restart`. A crashed disk is power loss, and
`restart_process()` refuses it by name: call `restart()` there. It covers the
global disk; hosts have no process restart.

### The checkers

`charpente_sim::checks` ships six reusable oracles and one boot idiom — reach
for these before hand-rolling the same logic a fourth time:

- `is_fault_fallout(text: &str) -> bool` — classifies an error's *text*:
  is this the harness's own injected fault (or its consequences) rather
  than a bug your workload should fail on? It recognizes the sim's
  injected-fault wording (including the `read_error` axis), the WAL/engine
  poison wording, and redb's "close and re-open the database" latch. Used
  inside a workload to turn "the disk broke, as instructed" into "no answer
  this round" rather than a hard failure — never to swallow anything else.
  (`misdirected_write` is not in that list and cannot be: it returns `Ok`, so
  it has no wording — what you eventually see is the corrupt frame it left
  behind.) **It never excuses the WAL's durable-horizon refusal**, the boot
  refused because an already-acknowledged frame failed its check at or below
  the store's horizon. That refusal is fallout where `bit_flip` or
  `misdirected_write` fired, and a durability bug in the platform everywhere
  else (a real one showed itself only as this refusal in 8 of 21 failing
  bases), and text cannot say which. So it is `false` here, and an open site
  asks `durable_refusal` (below) first, or, for one node of several,
  `SimHost::durable_corruption_fired()`. `boot_through_fallout` does that for
  you.
- `classify(text: &str) -> FaultReason` — the families, typed:
  `Injected`, `Poisoned`, `RedbLatch`, `DurableHorizon`, or `Unrelated`
  (`is_fault_fallout` is `Injected | Poisoned | RedbLatch`). Use it when your
  workload wants to accept some families and fail on others — e.g. "an
  injected fault is fallout this round, but a poisoned engine is a finding" —
  or to name the family in a coverage key. `DurableHorizon` is a name, never
  an excuse on its own. Matching on a variant instead of on substrings keeps
  the prose coupling in one place: if a platform error string is ever
  reworded, `classify` is the single site that must change, and it fails by
  name instead of leaking a wrong classification into whichever oracle saw
  the text first.
- `replays_identically(seed: u64, workload: impl Fn(&mut Sim) -> Result<T, String>) -> Result<(), String>`
  — the determinism oracle. Runs `workload` twice, each on its own fresh
  `Sim::new(seed)`, and fails if the returned value or the durable
  end-state (`Sim::durable_fingerprint`) diverge. It always constructs
  plain `Sim::new` worlds — never a *swarm* `FaultConfig` (swarm testing:
  the harness samples each universe's fault rates and latency ranges from
  its own seed, so a batch explores many fault mixes instead of every
  universe sharing one fixed matrix) — so it re-runs the workload's logic
  under default fault rates rather than replaying the exact fault schedule
  of the swarm universe that called it. **It takes a seed because it needs two
  worlds identical at time zero**, which a sim that has already run cannot give
  it — the opposite of the checker below, and the rule is: a checker that needs
  a FRESH world takes a seed, one that continues THIS world takes `&mut Sim`.
- `acked_implies_durable(sim: &mut Sim, rounds: u32, drive: impl FnMut(&mut Sim) -> Result<Vec<String>, String>, verify: impl FnMut(&mut Sim, &[String]) -> Result<(), String>) -> Result<(), String>`
  — the ack-durability property, generically: `rounds` times, `drive` does
  one round of work and returns the ids it promised are durable (classifying
  its own faults with `is_fault_fallout`, same as above), the sim restarts,
  and `verify` checks every id acked *so far* survived. A failure names the
  round it broke on. A run where no round acked anything gives the universe
  up by name, since no `verify` could have failed. **Pass it your own `sim`, not `sim.seed()`** — it restarts
  the world you give it, so `require: ["restart"]` over it fires, the excuse
  helpers below read the same world your workload ran in, and your universe's
  fault config actually applies. Until 2026-09-05 it took a seed and built its
  own `Sim::new` world, which silently did none of those three things; if you
  have code calling it with `sim.seed()`, that code was running outside its
  declared fault matrix and crediting coverage nowhere.
- `durable_corruption_fired(sim: &Sim) -> bool` — true when a
  durable-corruption fault (`bit_flip` or `misdirected_write`) actually
  fired in this universe. Those axes can corrupt an acked WAL record above
  the store's checkpoint horizon, and recovery then legitimately cuts it
  (an invalid frame is a torn tail unless provably below the horizon — the
  standard WAL convention; the un-checkpointed data had one local copy).
  Route an acked-durability check's *shortfall* through this so the loss is
  excused only in universes where corruption was actually injected — the
  strong oracle stands everywhere else. **An excused universe gives up**
  (`sim.give_up(..)` and stop, as `counter_survives` under "Giving up" does):
  the record's durability was not adjudicated, and a pass needs a verdict, so
  an excuse that returns `Ok` without giving up is a vacuous pass. Never use it
  to excuse wrong data or a consistency violation (a cut reverts state to a prefix, which stays
  consistent). Note this is the *single-node* excuse: a workload with a
  converged follower keeps none of it, because a record the primary lost to
  media corruption still lives on the follower. In a multi-node workload,
  ask `SimHost::durable_corruption_fired()` instead. It answers the same
  question for one host's disk, so a refusal on one node is excused only by
  corruption that fired on that node, never by a fault on another. A workload
  that wipes a host's data (a re-seed) reads
  `SimHost::durable_corruption_count()` and `dir_entry_loss_count()` at the
  wipe, and excuses a later refusal only when the count has grown since: the
  wipe deleted every byte an earlier fault touched.
- `boot_through_fallout(sim: &Sim, tries: u32, is_fallout: impl Fn(&str) -> bool, boot: impl FnMut() -> Result<T, E>) -> Result<Option<T>, E>`
  — boots the way a supervised process does: `boot`, and on an error your
  `is_fallout` accepts, boot again, `tries` boots in all. Each retry marks
  `boot:retried`, a retry that boots marks `boot:booted-after-retry` (require
  that one: it proves the retries do more than run), and running out marks
  `boot:exhausted`. A retry after an injected IO error restarts the process
  first (`sim.restart_process()`, so the page cache survives); a retry after a
  crash power-cycles first (`sim.restart()`, when `Sim::disk_crashed` says the
  disk is gone). `Ok(Some(t))` is the first clean boot and `Err(e)` the first
  error that is not fallout, unchanged. A durable-horizon refusal is judged
  before your `is_fallout` sees it, so no classifier of yours can excuse one
  where no durable corruption fired. `Ok(None)` means every boot met
  fallout, and what it means is yours to say: a workload whose only node
  never came up gives up by name (`counter_survives` above), and a round
  restarts and moves on. The one `None` the helper names itself is a boot
  refused for durable corruption at or below the store's horizon
  (`durable_refusal`, next): no restart clears bytes on the disk, so it stops
  at once, marks `boot:durable-refusal` and gives the universe up under that
  name. Where no durable corruption fired, the same refusal is a finding and
  comes back as `Err(e)`, unchanged. Use it at every boot, a round
  loop's included: an injected fault inside the table store's open is
  returned by `Db::open` as the error it is, and the next boot opens the same
  file, so a phase that gives up or skips on its first faulted boot throws
  away work a restart would have recovered. A round loop's next round does
  boot again, but the round it skipped is work the property never checks.
- `durable_refusal(sim: &Sim, text: &str) -> Result<bool, String>` — the
  same rule for a loop that boots on its own, such as a final verification's
  reopen loop: call it on each failed boot's error before the fallout check,
  since `is_fault_fallout` turns the refusal down. `Ok(false)`: not
  a durable refusal, carry on. `Ok(true)`: a refusal in a universe where
  durable corruption fired, already given up under its own name and marked
  `boot:durable-refusal`, so end the loop. `Err(..)`: a refusal where none
  fired, which nothing excuses, so fail with the text it names. A loop that
  retries a durable refusal spends its whole budget on bytes no restart
  clears, then gives up claiming the attempts all hit injected faults.
- `dir_entry_loss_fired(sim: &Sim) -> bool` — the same fire-time-marked
  discipline for the `dir_entry_loss` axis (a pending directory create
  vanished, or a pending removal resurrected). A workload that routes
  `Db::open`'s "a segment is missing … refusing to guess" to a restore path
  must do so *only* when this returns true: the platform never leaves a
  durable directory gap on its own, so a gap in a universe where the axis
  never fired is a real regression and must fail loud. In a multi-node
  workload, ask `SimHost::dir_entry_loss_fired()` instead: it answers for one
  host's disk, so one node's gap is never excused by entry loss on another.

```rust,ignore
pub fn classify(text: &str) -> FaultReason   // Injected | Poisoned | RedbLatch
                                             // | DurableHorizon | Unrelated

pub fn is_fault_fallout(text: &str) -> bool

pub fn durable_corruption_fired(sim: &Sim) -> bool

pub fn dir_entry_loss_fired(sim: &Sim) -> bool

pub fn boot_through_fallout<T, E: std::fmt::Display>(
    sim: &Sim,
    tries: u32,
    is_fallout: impl Fn(&str) -> bool,
    boot: impl FnMut() -> Result<T, E>,
) -> Result<Option<T>, E>

pub fn durable_refusal(sim: &Sim, text: &str) -> Result<bool, String>

pub fn replays_identically<T: PartialEq + std::fmt::Debug>(
    seed: u64,
    workload: impl Fn(&mut Sim) -> Result<T, String>,
) -> Result<(), String>

pub fn acked_implies_durable(
    sim: &mut Sim,
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
    // `sim`, not `sim.seed()`: the checker restarts the world you hand it, so
    // the `restart` coverage lands where the harness reads it and your
    // universe's fault config is the one the property runs under.
    acked_implies_durable(sim, 5, drive(), verify)
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
use charpente::wire::WireU64;
use charpente::{Schema, Tx, View, Wire, job, jobs, route, routes, table};
use charpente_sim::Sim;
use charpente_sim::checks::is_fault_fallout;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

const SEC: u64 = 1_000_000_000; // SimSource timestamps are virtual nanoseconds

#[table(global)]
struct Counter {
    id: Id<Counter>,
    hits: u64,
}

// A job payload never crosses HTTP, so it has no `Wire` requirement — this
// stays a bare `u64`.
#[job(global)]
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

// A route body IS a wire type, so `slot` is `WireU64` here even though the
// job payload above carries the same value as a bare `u64`.
#[derive(Wire, Serialize, Deserialize)]
struct HitBody {
    slot: WireU64,
}

#[route(POST "/hit", public)]
fn hit(tx: &mut Tx, body: HitBody) -> Result<WireU64, ApiError> {
    tx.enqueue(CountUp { slot: body.slot.0 });
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
    // Each fault-fallout exit gives up: the universe checked nothing, and a
    // pass needs a verdict ("Giving up" above).
    let app = match build_app(sim) {
        Ok(app) => app,
        Err(e) if is_fault_fallout(&e.to_string()) => {
            sim.give_up(format!("the app never booted: {e}"));
            return Ok(());
        }
        Err(e) => return Err(e.to_string()),
    };

    let mut source = SimSource::new(Box::new(sim.clock()), 2 * 3600 * SEC + SEC);
    for slot in 0..3u64 {
        source.push_at(
            slot * SEC,
            Request::new(Method::Post, "/hit").json(&HitBody { slot: WireU64(slot) }),
        );
    }
    let responses = source.responses();

    let mut kernel = match Kernel::new(app, Box::new(sim.clock())) {
        Ok(kernel) => kernel,
        Err(e) if is_fault_fallout(&e.to_string()) => {
            sim.give_up(format!("Kernel::new hit fault fallout: {e}"));
            return Ok(());
        }
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
        // ends the universe with nothing asserted, a give-up; anything else is
        // real.
        Err(msg) if is_fault_fallout(&msg) => {
            sim.give_up(format!("the invariant check hit fault fallout: {msg}"));
            Ok(())
        }
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

### The outside world: remote models

Outbound HTTP goes through the `Outbound` IO trait, to hosts the app
declares. In a sim, each declared host is a model you register: a `Remote`
that lives inside the universe, runs on its seed, and answers each request
when the request reaches it. The wire between the kernel and the model
belongs to the sim, and the outbound fault axes act on that wire.

Register one model per declared host, then build the outbound handle:

- `sim.remote(host, model)` makes `model` the outside world behind `host`.
  One model per host: registering a host twice is a contract panic,
  `contract: sim.remote("<host>", …) registered twice`.
- `sim.outbound(hosts)` builds one process's handle, a
  `charpente_sim::outbound::SimOutbound`. `hosts` is the app's declared host
  list, passed as plain data. A host with no model is refused by name before
  anything runs:

```text
sim: a mounted job calls <host>, which has no model — add sim.remote("<host>", …) before sim.outbound(…)
```

The same text is a panic when a handle starts a call to a host that has no
model, such as one the list given to `sim.outbound` left out.

A model implements `charpente_sim::outbound::Remote`, whose one method is
`fn handle(&mut self, call: &OutboundCall, rng: &mut Rng) -> OutboundReply`.
It runs when the request reaches the remote, at its drawn delivery time, not
when the kernel starts the call. `rng` is the model's own seeded stream,
keyed by the host's name, so a model's variance (a payment that is sometimes
declined, an LLM that sometimes answers ambiguously) replays with the seed
and moves no other draw. A model is `Send + 'static`. It runs under the sim's
world lock, so it must never call back into the `Sim`.

There are three ways to get a model. Each is a named choice, and none is a
default:

- `charpente_sim::outbound::from_fn(f)` wraps a closure,
  `FnMut(&OutboundCall) -> OutboundReply`. The closure gets no `rng`, so
  write a `Remote` impl when the model needs variance.
- `Recorder::new()` answers 200 with an empty body and keeps every call it
  was handed, in delivery order. `calls()` reads them. It models
  fire-and-forget email.
- `Idempotent::header(name, model)` wraps an inner model in Stripe's
  idempotency semantics, keyed by the value of the header `name` (matched
  case-insensitively). `Idempotent::by(extract, model)` takes the key from
  any part of the request, through `extract: fn(&OutboundCall) ->
  Option<String>`. A repeated key with the same method, path and body gets
  the first reply again, and the inner model does not run. A repeated key
  with a different method, path or body gets a 409. A request with no key is
  applied every time, as Stripe does. `applied()` counts applications per
  key. A repeated key replays, so every count is 1, and the map says which
  keys reached the inner model: a retry that sent a fresh key shows up as a
  second entry, never as a 2. `inner()` reads the wrapped model. The key is
  what the app put in the request, never `OutboundCall::key`: a real remote
  never sees that field.

A workload reads model state with `sim.with_remote::<M, _>(host, |m| …)`. The
closure borrows the model as its concrete type `M`, since a reference cannot
outlive the world lock. The closure runs under that lock, as a model does, so
it must not call the `Sim` either: return what you need and act on it after.
Naming a host with no model, or the wrong type, is a contract panic, and the
wrong-type text names both the type asked for and the one registered. A
wrapped model is read as its wrapper: `|m: &Idempotent<StripeModel>|`, then
`m.inner()`.

`sim.outbound_deliveries()` returns every request that has reached a
remote, as `charpente_sim::outbound::Delivered { host, key, at, handle }`.
`key` is the job's `OutboundCall::key`, which attributes a delivered call to
its job. `at` is the virtual arrival time in nanoseconds, and `handle` says
which `SimOutbound`, and so which process, sent it. Like `with_remote`, it
first runs every delivery due by now, so what it shows is current with the
clock.

A request in flight at a crash still arrives; only its reply is lost. Models
belong to the universe, not to the box, so `sim.restart()` leaves them and
every request already on the wire alone. The restarted process builds a new
handle with `sim.outbound(…)`. A reply goes only to the handle that started
its call, so a pre-restart reply has nowhere to land, even when the new
process reuses the old token. A cancelled call (`Outbound::cancel`) behaves
the same way: its request still reaches the remote at its delivery time, and
its completion is never handed back.

Every call rolls each fault axis once, from that axis's own keyed stream,
whatever the other axes decide, and draws both of its latencies the same
way. So turning one axis on or off moves no other axis's draw. It can still
change what happens downstream: a call it refuses never reaches the model,
so the model's own `rng` sees one call fewer. Delivery and reply times are
drawn when the call starts. The call's fate is chosen from its rolls in a
fixed precedence: refused, else lost, else throttled, else delivered. A
delivered call may also be slow, and may lose its reply. Coverage counts
only what applies, so a refused call that also rolled a lost reply counts as
refused:

| Axis (`FaultConfig` field) | The remote sees | The kernel sees | Coverage key |
|---|---|---|---|
| `outbound_refused` | nothing | an immediate transport error, `ConnectionRefused` | `fault:outbound_refused` |
| `outbound_lost_request` | nothing | silence, then its deadline | `fault:outbound_lost_request` |
| `outbound_lost_reply` | the call, applied | silence, then its deadline: the ambiguous case | `fault:outbound_lost_reply` |
| `outbound_throttle` | nothing (overloaded) | a 429 or 503 | `fault:outbound_throttle` |
| `outbound_latency` `(min, max)` and `outbound_slow` | the call, late | a reply late, or past the deadline (the LLM tail) | `fault:outbound_slow`; latency has none |

`outbound_latency` is a one-way range, drawn once for the request and once
for the reply, and a throttle answers after twice the request's draw.
`outbound_slow` adds think time between the request's arrival and the
reply's departure, uniform in [10·max, 100·max], or in [1 s, 100 s] when max
is zero. Every
completion handed back to the kernel also marks `outbound:completed`.
Duplicates are not an axis. In HTTP a duplicate is a retry after a lost
reply, and the sim produces those. Whether a duplicate applies twice is the
model's semantics, which is what `Idempotent` is for.

`FaultConfig::default()`, which `Sim::new` uses, turns the five outbound
faults off and sets latency to 1–50 ms. `FaultConfig::none()` makes every
call instantaneous. A swarm universe samples all six fields in
`sample_extended_fields`, each from its own stream, so the existing axes'
sampled values are unchanged:

| Field | Swarm sample |
|---|---|
| `outbound_refused` | on with chance 0.25, at a rate from 0 to 0.049 in steps of 0.001 |
| `outbound_lost_request` | on with chance 0.25, at 0 to 0.029 |
| `outbound_lost_reply` | on with chance 0.25, at 0 to 0.029 |
| `outbound_throttle` | on with chance 0.25, at 0 to 0.099 |
| `outbound_slow` | on with chance 0.25, at 0 to 0.049 |
| `outbound_latency` | `(0, max)`, with max one of 0, 10 ms, 200 ms or 2 s, each with chance 0.25 |

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
  `seed`, `master_seed`, `config` — every fault axis, not a subset — its own
  `coverage` of what actually fired in that universe, `message`, and a
  copy-pasteable `repro` command), `verdicts` and `gave_up` (batch totals), `outcomes` (the same
  split per test: `{test, verdicts, gave_up}`), `coverage`,
  `missing_coverage` (`{test, key}` pairs), and a trailing `summary`, for
  downstream tooling.
- `--dir PATH` — the app crate directory to build and run sim tests from
  (default: `.`). This flag is consumed by the CLI itself; every other flag
  passes straight through to the sim binary the CLI builds and execs.

Exit codes: **0** every universe of every selected test passed and every
`require`d coverage key was hit (universes that *gave up* are reported, not
failed — see "Giving up" above; a green run tells you how many reached a
verdict, and reading that number is on you); **1** at least one universe failed, or a
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
  config: FaultConfig { write_error: 0.0, sync_error: 0.118, crash_apply: 0.002, crash_torn: 0.664668, dir_entry_loss: 0.0, bit_flip: 0.0, misdirected_write: 0.0, read_error: 0.0, object_put_error: 0.0, object_get_error: 0.0, object_lost_ack: 0.0, object_delete_error: 0.0, corruption_scope: "wal/", conn_drop: 0.0, net_spike: 0.0, subscriber_stall: 0.0, outbound_refused: 0.0, outbound_lost_request: 0.0, outbound_lost_reply: 0.0, outbound_throttle: 0.0, outbound_slow: 0.0, outbound_latency: (0ns, 0ns), disk_latency: (0ns, 0ns), net_latency: (0ns, 250ms) }
  fired:  crash:torn: 7, fault:sync_error: 4, restart: 2
  repro:  charpente test --sim --filter receipts_exactly_once --seed 0x8bee1752811ae873
```

Five lines, each load-bearing:

- The first line is the workload's own `Err` message (or an absorbed panic
  payload) — the actual claim that failed.
- `seed` is the one `u64` that determined this entire universe: virtual
  time, every entropy draw, and the fault matrix's decisions.
- `config` is that universe's sampled `FaultConfig` — the fault rates and
  latency ranges that were ENABLED (per-universe swarm sampling, glossed
  under the checkers above). It is a set of probabilities, so it tells you
  what kind of trouble was *possible*: `crash_torn: 0.664668` means torn
  writes were likely in this universe, not that any occurred. Do not read a
  diagnosis off this line — that is the next one's job.
- `fired` is what ACTUALLY happened: the universe's coverage map, every
  fault and event that really occurred, with counts. This is the line to
  reason from. A high probability whose key is absent here never fired, and
  an explanation resting on it is wrong. `fired: nothing` is a real and
  valuable answer — it means no injected fault was involved and the failure
  is a plain bug. (This line exists because it once did not: a durability
  finding was attributed to `dir_entry_loss: 0.477` by three separate
  readers, none of whom could have known whether that axis fired, because
  the failure path discarded coverage entirely — docs/LEDGER.md 2026-09-07.)
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

### Re-basing a pinned property: `CHARPENTE_BASE_SEED`

Some properties fix their base seed (`run_universes_swarm_with!(BASE, N, …)`)
so their `floor:` rates have a stable denominator. That is right for a rate and
wrong for a correctness claim: a fixed base means the property only ever
examines the same N universes, while a green run reads as though it examined
the space.

`CHARPENTE_BASE_SEED=0x… cargo test -p <pkg> --test <file>` re-bases every
pinned property in that binary, so the claim "this property holds" can actually
be tested rather than assumed:

```text
charpente-sim [realtime]: re-basing 0x000000002eb20001 -> 0x00000000a11ce001 under
  CHARPENTE_BASE_SEED — coverage requirements, floors and ceilings skipped (they
  are rates measured at the pinned base)
```

Like `CHARPENTE_SEED`, it takes hex with or without `0x`, and `_` separators
are ignored, so a base copied from the source as written (`0x2EB2_5A10`) works
as is; a value that is not hex panics naming the variable it was read from.

Floors, ceilings and `require:` keys are skipped under the override, for the
reason the floor's own panic text gives: they are rates measured AT a base, so
evaluating them somewhere else compares against a denominator that was never
taken there.
What the mode tests is the property's own assertions, which are untouched.

**A correctness oracle should pass at every base.** If it passes at its pinned
base and fails at others, it is not flaky — it is failing, and the pinned base
is hiding it. Two realtime properties were found exactly that way
(docs/LEDGER.md 2026-09-07).

One caveat worth knowing before you chase a floor panic: an IO call consumes RNG
draws, so changing *where* an IO operation happens inside code a property never
names reshuffles every universe at a pinned base. Floors move for that reason far
more often than they move because an oracle decayed.

So a floor that falls after such a change is re-measured, not defended and not
quietly lowered. Run the property before and after the change at the same ten or
more fresh bases (`CHARPENTE_BASE_SEED`) and compare the key's mean. It has
decayed if the after-mean sits more than one Welch standard error below the
before-mean, where that error is `sqrt(sd_before²/n_before + sd_after²/n_after)`
over the per-base counts. Within one standard error, the floor is re-set to its
new exact count at its pinned base, with both distributions recorded; beyond
it, the fall is the finding. A base where either binary fails on an unrelated,
named bug is left out of both sides, and the record says which. One standard
error is deliberately strict: pure noise crosses it roughly a sixth to a quarter
of the time, which errs toward stopping to look.

A cap that rises after such a change asks a different question, because a
give-up is not a lost verdict when the same universe used to pass having
checked nothing. So judge the rise by the verdicts the property reached, over
the same ten or more fresh bases: count its passes that covered its reach key
(the key its `floor:` counts), and beside them its vacuous passes (universes
that passed without covering that key). A cap may rise only if the passes
covering the reach key did not fall by more than one Welch standard error.
Then the new give-ups replaced vacuous passes, not verdicts, and the cap is
re-set by its own convention, with both distributions recorded. If those
passes fell by more than one standard error, the rise is the finding. Neither
the reach key's own count nor the vacuous passes can decide it alone: 20 more
give-ups beside 1 fewer vacuous pass means 19 passes that checked something
became give-ups. A pinned cap can read the same
number on both binaries while the rate behind it moved, so the test runs
whenever the change could have moved it, not only when the pinned count did.
That is also why every capped property floors its reach key: without one, the
next change that costs it reach passes with every cap still met.

At a pinned base both bounds are exact counts. At an ambient base, where the
counts move batch to batch, both are margins over ten or more fresh batches.

An ambient give-up cap is the larger of two numbers:
- twice the largest fresh-batch count;
- the smallest k with P(Poisson(lambda) > k) < 1e-4, where
  lambda = n x U(x) / N. Here n is the universes in one batch, N the universes
  over all the fresh batches, and x the give-ups among them. U(x) is the exact
  one-sided 95% Poisson upper bound for x: 2.996 for x = 0, 4.744 for 1, and
  6.296 for 2.

The second number matters where give-ups are rare. When no batch gave up,
twice the largest count is 0, which one legitimately exhausted universe fails.
When one batch gave up once, it is 2, which a healthy batch fails about once
in eighty. Where counts are large the first number is the larger, and the
second only confirms it.

Two worked examples for the WAL fault matrix, whose 10 fresh batches of
n = 512 make N = 5,120:
- **x = 0**, as measured. lambda = 512 x 2.996 / 5,120 = 0.30. P(> 3) is
  2.6e-4 and P(> 4) is 1.6e-5, so k = 4, and the cap is max(0, 4) = 4.
- **x = 1**, as ten bases including 0x9E7C000A would have read: one batch gave
  up once. lambda = 512 x 4.744 / 5,120 = 0.47. P(> 4) is 1.4e-4 and P(> 5) is
  1.1e-5, so k = 5, and the cap is max(2, 5) = 5, not 2.

An ambient reach floor is the mean less four standard deviations. That flakes
about once in thirty thousand batches if the counts are near normal, and fails
any batch that falls further. So an ambient floor catches a collapse, not a
drift. Kernel exactly-once's `job:done` spreads by about 4% of its mean (sd
19.1 on 485.2), so its floor sits about 16% below the mean. A fall half that size trips about one batch in
forty, which a change can pass by luck. The before-and-after comparison above
is the instrument that sees a move that small, so it runs whenever a change
could move an ambient floor's rate, as it does for a cap, and not only when a
batch goes red.

The directory barrier used to be the worst offender, because `sync_dir` is the
call a *durability* fix is most likely to add — one such fix moved floors in
three separate files in a single day. It now draws from its own fault stream, so
adding or relocating a barrier perturbs nothing else. A file sync with nothing
to flush is the other exception: it cannot fail and draws no fault, so syncing a
file that is already durable moves no roll. That is the shape most durability
fixes take after a power cycle. It still counts as a disk operation, so it
ticks an armed crash countdown and advances virtual time. No other IO call has
either treatment, so the caveat above still holds for the rest of them.

### Finding a one-sided property: `charpente test --mutants`

A green suite tells you your properties passed. It does not tell you they would
have **failed** if the code stopped working. A property that asserts something
true whether or not your job ran is *one-sided*: it costs time, reports success,
and is not evidence about anything.

This is not hypothetical. A helpdesk app built on charpente shipped five such
properties — deleting its entire SLA sweep loop left the suite green.

The instrument answers one question: **if this code did nothing, would anything
go red?** It runs your suite repeatedly against deliberately broken copies of
your source (patched in a scratch directory — nothing about the mutation touches
your tree or ships), and a mutant that survives is a gap in your oracles.

```
scripts/mutants.sh --file src/jobs.rs
```

A mutant reported **MISSED** is the finding. Worked example, from this repo's own
fixture app — one job whose whole effect is a counter bump, and two properties
that drive it identically and differ only in what they assert:

```
$ charpente test --mutants --file crates/sim-fixture/src/lib.rs \
    --re 'BumpCounter::run.*Ok' --test-package sim-fixture \
    --property counter_bump_job_leaves_the_row_readable

MISSED   crates/sim-fixture/src/lib.rs:40:9: replace BumpCounter::run -> Result<(), JobError> with Ok(())
mutants: 1 total — 0 caught, 1 missed, 0 unviable, 0 timed out
  1 MISSED, judged by "counter_bump_job_leaves_the_row_readable" ALONE: that property
  runs, reaches a verdict, and does not notice this change — it is ONE-SIDED with
  respect to the mutated code, and would pass just the same if the code were deleted.
  Fix: assert the EFFECT the mutated code produces (a value that differs when it does
  not run), not merely that the surrounding state is still readable.
```

The same mutant, judged by the honest sibling, is **CAUGHT**:

```
$ charpente test --mutants … --property counter_bump_job_changes_the_row
mutants: 1 total — 1 caught, 0 missed, 0 unviable, 0 timed out
```

Both properties ran 256 universes and reached ~147 verdicts each, so neither
result is an artifact of a property that never executed. The only difference is
the assertion: one checks the row is still *readable*, the other checks it now
reads `hits = 2`.

**Scope is part of the claim, and this is the part that misleads.** A mutant is
reported CAUGHT if *any* test in scope fails, so one honest sibling hides every
one-sided property beside it. `--property NAME` judges the mutants by a single
property — that is how you ask whether *that* property earns its place.
Similarly `--test-package` decides which packages' suites are allowed to run;
without this, the verb judges every mutant with `charpente-core` and
`charpente-sim`'s tests — the default target set's oracles — so a `--file`
in any other crate must name its own package here, or its mutants are
judged by suites that never exercise them and all report MISSED.

**Two results that are not findings.** `unviable` means the mutant did not
compile — your types rejected it, which is the compile-time contract doing its
job. `timed out` means the run did **not resolve**: it is not a catch, and the
verb will not let it read as one.

The timeout follows the scope too. cargo-mutants times its own baseline over
one package, the mutated file's, while every mutant runs all the packages in
`--test-package`. So whenever those differ (two or more test packages, or one
that is not the `--file`s' package) and no `--timeout` is given, the verb first
times one `cargo test` over the test set, passes five times that as the
timeout, and prints both numbers:

```text
mutants: `cargo test` over charpente-core, charpente-sim took 8.6 s of wall time; that is what each mutant runs, and cargo-mutants' own baseline times only the mutated file's package, so the verb is passing --timeout 44 (5x)
```

Under `--property` each mutant runs that one sim property, less than the set
that was timed, so the line says the time is an upper bound on a mutant's run
instead. An explicit `--timeout SECS` skips the timing run and is passed through
as given.

**It is not part of `cargo test`.** Every mutant is a rebuild plus a test run,
so this is an on-demand instrument like the edge harness below, not something
your inner loop pays for. Scope it at a file or a function (`--file`, `--re`)
rather than turning it loose on a whole workspace. It needs the pinned tool:
`cargo install cargo-mutants --version 27.1.0 --locked`. The verb checks for it
(`cargo mutants --version`) before anything else runs, so a machine without it
exits 2 with that install line rather than after a timing run.

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

### `#[table]` tenancy and shape violations (`charpente-macros/src/lib.rs` and `src/table.rs`, compile time)

Nine ways `#[table]` rejects a struct — §2 pins the first three and walks
through the other six on a `Ping` example:

```text
`<table>` declares no tenancy — a table says which. Add one of these to its `#[table(…)]`, keeping any arguments it already has: `scoped_by = Org` if every row belongs to one tenant (the key becomes `(scope, id)` and a bare `Tx` cannot read it), or `global` if its rows belong to no tenant. Neither is the default.
`global` and `scoped_by = <scope>` on one table — a table is one or the other; drop one
`global` given twice — write it once
missing `id: Id<Ping>` field — every #[table] row needs a typed primary key (add `id: Id<Ping>`)
the `id` field of a #[table] must be `Id<Ping>` — a typed id, never a bare integer
`id` is the primary key — it is already the fastest lookup; remove #[index]
#[table] needs named fields — `Ping` must be a braced struct with an `id: Id<Ping>` field
#[table] applies to structs — enums and unions cannot be rows
#[table] structs cannot be generic — a table is one concrete row shape; make `Ping` non-generic
```

In order: No tenancy declared — add `scoped_by = …` or `global` to the attribute,
keeping its other arguments. Both at
once — drop one. `global` twice — once. No `id` field at all — add one.
`id` present but not `Id<Self>` — type it. `#[index]` on `id` itself —
remove it, `id` is already the primary key. Not a braced struct with named
fields (tuple/unit structs) — rewrite as one. An enum or union — `#[table]`
only applies to structs. A generic struct — a table is one concrete row
shape; monomorphize it yourself before attaching `#[table]`.

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
`<handler>` declares neither `auth` nor `public` — a route says which callers reach it. Add one of these to the flags of its `#[route(<method> "<path>", …)]`, keeping any it already has: `auth` to require a principal (`ctx.principal()` is `Some`), `public` to serve anonymous callers, or `public(Org)` to also let every caller act inside the `Org` tenant it opens. None is the default.
`auth` and `public` on one route — a route is one or the other; drop one
`public` given twice — write it once
a #[route] handler's first argument must be `tx: &mut Tx` — `ping` is missing it
`get_note` takes 0 argument(s) after `tx` (excluding `ctx`) but the path "/notes/:id" has 1 :param(s) — write one argument per :param (same name, path order) plus at most one JSON body argument
this argument must be named `id` to match :id in "/notes/:id" — got `key`
route path must start with '/' — got "notes"
unknown method PUSH — one of GET, POST, PUT, PATCH, DELETE, SSE
the route path must be a string literal — e.g. #[route(GET "/notes/:id", auth)]
a route flag is `auth`, `public`, `public(<Scope>)`, `cross_tenant` or `emits(Ev, …)` — write `#[route(GET "/path", auth)]`, `#[route(GET "/path", public)]`, `#[route(GET "/orgs/:org/status", public(Org))]`, or `#[route(SSE "/path", emits(NoteAdded, NoteRemoved))]`
`public(...)` names exactly one tenant scope type — write `public(Org)`, or plain `public` if anonymous callers enter no tenant here
emits(...) applies to SSE routes only — a plain route returns one response, not a stream of events; drop emits(...), or change this route's method to `SSE`, keeping its path and flags
emits(...) must name at least one #[event] type — write `emits(NoteAdded, NoteRemoved)`, or drop it
emits takes a parenthesized list of #[event] types — write `emits(NoteAdded, NoteRemoved)`
`auth` given twice — write it once
`cross_tenant` given twice — write it once
`revenue` takes `cap: &CrossTenant`, but its `#[route(GET "/admin/revenue", …)]` does not declare `cross_tenant` — enumerating a scoped table across every tenant (`tx.across_tenants`) and fanning a job out into a tenant the route names (`tx.enqueue_in`) are one declared capability, and `charpente describe` lists every route that declared it. Add `cross_tenant` to that attribute's flags, keeping the others, or drop the `cap` argument and read or enqueue in one tenant through `tx.scope::<Org>(org, ctx)?`
`emits(...)` given twice — a route declares one set, so name every event in one list: `emits(NoteAdded, NoteRemoved)`
`NoteAdded` is named twice in emits(...) — a stream declares each event type once, and a repeat would put the same member in a generated client's union twice; write it once
#[route] handlers are free functions — no `self`
empty :param name in "/notes/:"
write `#[route(METHOD "/path", auth | public | public(<Scope>))]` — e.g. #[route(GET "/notes/:id", auth)]
```

§5's "What `#[route]` rejects" pins the first three and walks through the
next three with full examples. The rest, in order: the path literal
doesn't start with `/`; the method name isn't one of
`GET`/`POST`/`PUT`/`PATCH`/`DELETE`/`SSE`; the path isn't a string literal
at all (e.g. a `const`); a flag after the path is none of `auth`, `public`,
`public(<Scope>)`, `cross_tenant`, `emits(...)`; `public(...)` names no scope
type, or more than one; `emits(...)` is on a route that isn't a
stream (only an SSE route has events to declare — see §11); `emits()` names
nothing at all; `emits` was written without its parentheses; `auth` or
`emits(...)` appears twice on the same route (a flag is a yes/no and a route
declares one set, so a repeat is always a typo or a bad merge); the same
event type appears twice inside one `emits(...)` list; the handler takes
`self` (route handlers are free
functions); a `:` segment has nothing after it; and the macro's own
argument list didn't parse as `METHOD "/path"` to begin with. Every one of
these is a compile error at the `#[route]` call site — fix the signature or
the pattern and it's gone.

### `#[job]` tenancy, retry-syntax, outbound and shape violations (`charpente-macros/src/job.rs`, compile time)

```text
`NotifySubscribers` declares no tenancy — a job says which. Add one of these to its `#[job(…)]`, keeping any arguments it already has: `scoped_by = Org` if it acts inside one tenant (it is enqueued from inside that tenant's scope, and its `run` receives that tenant's `ScopedTx`), or `global` if it belongs to no tenant (the only kind cron can schedule). Neither is the default.
`global` and `scoped_by = Org` on one job — a job is one or the other; drop one
`global` given twice — write it once
`scoped_by` given twice — write it once
`cross_tenant` given twice — write it once
`retry(...)` given twice — write it once
`outbound` given twice — write it once
`timeout` given twice — write it once
a job declares `global` or `scoped_by = <Type>`, and may add `cross_tenant`, `retry(max = <int>, backoff = "<n><unit>")` (unit is one of ms, s, m, h), and `outbound = "<host>"` with an optional `timeout = "<n><unit>"`, in any order — e.g. `#[job(scoped_by = Org, retry(max = 3, backoff = "10s"))]`, `#[job(global, cross_tenant)]` or `#[job(scoped_by = Org, outbound = "api.stripe.com", timeout = "30s")]`
backoff "10" has no unit — a job declares `global` or `scoped_by = <Type>`, and may add `cross_tenant`, `retry(max = <int>, backoff = "<n><unit>")` (unit is one of ms, s, m, h), and `outbound = "<host>"` with an optional `timeout = "<n><unit>"`, in any order — e.g. `#[job(scoped_by = Org, retry(max = 3, backoff = "10s"))]`, `#[job(global, cross_tenant)]` or `#[job(scoped_by = Org, outbound = "api.stripe.com", timeout = "30s")]`
unknown retry field `jitter` — a job declares `global` or `scoped_by = <Type>`, and may add `cross_tenant`, `retry(max = <int>, backoff = "<n><unit>")` (unit is one of ms, s, m, h), and `outbound = "<host>"` with an optional `timeout = "<n><unit>"`, in any order — e.g. `#[job(scoped_by = Org, retry(max = 3, backoff = "10s"))]`, `#[job(global, cross_tenant)]` or `#[job(scoped_by = Org, outbound = "api.stripe.com", timeout = "30s")]`
`max` given twice — write it once
`backoff` given twice — write it once
backoff "12xs": "12x" is not a whole number — a job declares `global` or `scoped_by = <Type>`, and may add `cross_tenant`, `retry(max = <int>, backoff = "<n><unit>")` (unit is one of ms, s, m, h), and `outbound = "<host>"` with an optional `timeout = "<n><unit>"`, in any order — e.g. `#[job(scoped_by = Org, retry(max = 3, backoff = "10s"))]`, `#[job(global, cross_tenant)]` or `#[job(scoped_by = Org, outbound = "api.stripe.com", timeout = "30s")]`
backoff "99999999999999h" is longer than u64::MAX nanoseconds (about 584 years), the longest duration the kernel's clock can count — give a shorter backoff
outbound = "https://api.stripe.com": the host carries a scheme — a host is a bare DNS name like "api.stripe.com"; where it resolves is the operator's (--outbound-base, GUIDE §5)
outbound = "api.stripe.com:443": the host carries a port — a host is a bare DNS name like "api.stripe.com"; where it resolves is the operator's (--outbound-base, GUIDE §5)
outbound = "": the host is empty — a host is a bare DNS name like "api.stripe.com"; where it resolves is the operator's (--outbound-base, GUIDE §5)
`timeout` applies only to an outbound job — add `outbound = "<host>"`, or drop `timeout`
timeout "0s" would cancel every call before the remote could answer — give a positive duration
`outbound` with `cross_tenant` is refused in v1 — an outbound call runs in the one tenant its job was enqueued in (outbound-effects spec §15); drop one
#[job] applies to structs — enums and unions cannot be job payloads
#[job] structs cannot be generic — a job is one concrete payload shape; make `Baz` non-generic
#[job] needs named fields — `Baz` must be a braced struct
```

The first is the one every job meets: a job declares its tenancy, and a bare `#[job]`
names both answers. The next seven refuse a word given twice or both tenancies at once.
Anything else the attribute does not recognise hits the accepted-forms message, which
lists every word; so does `outbound` written without its `= "<host>"`. Three of the lines
after it are that message with more specific context prepended: a backoff string missing
its unit, an unrecognized field inside `retry(...)`, or a backoff whose number doesn't
parse; between them, `max` or `backoff` written twice inside `retry(...)` is refused like
the words above. The line after those refuses a backoff longer than the kernel's clock
can count, which is `u64` nanoseconds: as a backoff it would panic at dispatch, and as
a timeout it would mean "never". A `timeout` string is refused in the same three ways as a
backoff, with `timeout` in place of `backoff`. The six lines after those refuse §6's
outbound words.
The three `outbound = …` lines are one refusal, a host that is not a bare DNS name, and
the reason after "the host" is one of: is empty, carries a scheme, carries userinfo,
carries a path, is an IPv6 literal, carries a port, has an uppercase letter, holds a
character a DNS name cannot, ends with a dot, has an empty label, has a label starting
or ending with '-', or is an IPv4 literal. `Host::new` refuses a host by the same rule.
Then `timeout` on a job that is not outbound, a zero timeout (the message quotes the
literal as written), and `outbound` beside `cross_tenant`. The last three are the
struct-shape checks `#[table]` makes (§2): only structs, only non-generic, only named
fields. A scoped job enqueued where no tenant is named, or scheduled by cron, is a
trait-bound error rather than a macro one; §6 pins its note.

An outbound job's methods are refused by rustc rather than by the macro, because the
attribute cannot see the job's `impl`. These are the first lines, for a job named
`Charge`:

```text
error[E0599]: expected an `Fn<_>` closure, found `Charge`
error[E0599]: no associated function or constant named `settle` found for struct `Charge` in the current scope
error[E0596]: cannot borrow `*tx` as mutable, as it is behind a `&` reference
error[E0308]: mismatched types
```

The first two come together: an outbound job whose `impl` has `run`, or nothing, where
`call` and `settle` belong. rustc reports each missing method, and words the one for
`call` as a closure error because the prelude's `Fn` trait also has a method named
`call`; its label reads "associated function or constant `call` not found for this
struct". Write both methods with §6's signatures. The third is a write inside `call`,
a `tx.put` say. rustc's help suggests making `call` take `&mut Tx`, and that signature
is refused next, by the fourth: `call` reads, and its writes belong in `settle`. The
fourth, with "expected fn pointer, found fn item", is a method whose signature is not
§6's: a `call` taking `&mut Tx`, or a scoped job's `call` or `settle` taking a bare
`Tx`, which would otherwise read or write in no tenant. Its note prints the expected
signature above yours.

Three more, at job-dispatch time rather than macro-expansion time
(`charpente/src/kernel/dispatch.rs` — covered behaviorally in §6's "Retry,
dead-letter, and exactly-once effects"):

```text
job decode for SendReceipt: <the underlying decode error> — its recorded shape is this binary's, so either a type one of its fields holds changed, which the fingerprint cannot see (GUIDE §12), or the stored payload is corrupt; it retries, then dead-letters — if such a type changed, restore that type's old definition and put the new one in a new #[job] type under a new name (GUIDE §6); if the bytes are corrupt and other rows are damaged too, restore this node from its archive (GUIDE §15) or re-seed it from its primary (GUIDE §10)
handler panicked: <the panic payload>
job "SendReceipt" is not registered — add it to the jobs![…] list passed to App::mount_jobs (or this log outlived a renamed job type)
```

The first: a Pending job's stored payload failed to decode although its
recorded shape is this binary's. A change to its own fields cannot cause
that, because it is refused before dispatch can start (see "Changing a
job's fields"), so the cause is one of two, and the text names both: corrupt
bytes, or a change inside a type one of its fields holds, which the
fingerprint cannot see (§12). For a changed type, restore that type's old
definition, so the Pending rows decode again, and put the new definition in
a new `#[job]` type under a new name. It retries and then dead-letters like
any rejection. The second: your `run` method (or
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

<!-- pinned -->
```text
platform IO failure during the handler (not an attempt): <e>
```

### `#[event]` declaration violations (`charpente-macros/src/event.rs`, compile time)

```text
`NoteAdded` declares no tenancy — an event says which, and neither is the default: add `global` or `scoped_by = <Tenant>` to this attribute's arguments, keeping its others. `scoped_by = <Tenant>` if it belongs to one tenant (it is emitted from inside that tenant's scope, and only that tenant's feed receives it); `global` if it belongs to no tenant (it is emitted on a bare `Tx`, and only a global `Subscription::of` feed receives it).
`global` and `scoped_by = Org` on one event — an event is one or the other; drop one
`global` given twice — write it once
`scoped_by` given twice — write it once
an event declares `global` or `scoped_by = <Tenant>` and nothing else (its name derives from the type) — e.g. `#[event(global)]` or `#[event(scoped_by = Org)]`
#[event] applies to structs — enums and unions cannot be events
#[event] structs cannot be generic — an event is one concrete payload shape; make `Baz` non-generic
#[event] needs named fields — `Baz` must be a braced struct
```

The first is the one every event meets: an event declares its tenancy, and a bare
`#[event]` names both answers. The next three refuse both tenancies at once, or a word
given twice. Anything else the attribute does not recognise hits the accepted-forms
message. That includes a job's words (`cross_tenant`, `retry(...)`), a `scoped_by` with
no type, and a name: an event's `NAME` is its type's snake_case, never an argument
(§11). The last three are the struct-shape checks `#[table]` and `#[job]` make. A scoped
event emitted on a bare `Tx`, or named in a global `Subscription`, is a trait-bound error
rather than a macro one; §11 pins its note.

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
`Db::open`, including registering `Session`, `ApiKey`, or one of the platform's
tables (`charpente.jobs`, `charpente.events`, `charpente.schema_versions`) yourself
(`App::open`/`Db::open` already register those). Fix in every case:
register that name/type exactly once, and mint at most one credential per
request.

### Call-site contract panics (`charpente-core/src/db.rs`, `charpente/src/prod.rs`, `charpente/src/kernel/cron.rs`)

<!-- pinned -->
```text
contract: an `enqueue_keyed` app key names the job's external identity — it cannot be empty
Disk paths are relative flat names like "wal/000001.log" — got "<path>"
cron schedule "<name>": interval <duration> exceeds u64 nanos (~584 years) — pick a smaller interval
contract: job "<job>" is outbound, so its enqueue needs an idempotency key drawn from the node's Entropy, and this group has none — Db::transact and Db::begin_group cannot mint; enqueue it from a route, a job or cron (the App's groups mint), or in a test through App::transact_for_tests
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
returned `Ok`. The fourth (§6) is an outbound job enqueued inside `Db::transact`
or `Db::begin_group`, which have no entropy to draw its idempotency key from:
enqueue it from a route, a job or a cron tick, stage it through
`Db::begin_group_minting` in code that opens its own group, or seed it in a
test through `App::transact_for_tests`.

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

<!-- pinned -->
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
— the 500 says so instead of guessing. Since group commit that third message
can also reach requests that wrote nothing at all, and can name a write that
is already durable; §5's "Ack-after-durable" states that contract in full. In every case the fix is the same:
restart the process. Recovery (`Db::open`/`Engine::recover`, §3) replays the
WAL from the last checkpoint and is itself idempotent, so restart-and-retry
is always the correct response to any of these three, never a retry loop
against the same live, poisoned handle. `Db::poisoned()` lets the kernel
loop check this directly — per the step-7 ruling, the request arm now stops
serving once a request's own 500 leaves the engine poisoned, rather than
keep accepting traffic against a dead engine.

The stop names *which* poison, because the two imply opposite things about
your data:

<!-- pinned -->
```text
kernel stopped: the engine is poisoned (a request-path IO failure made durability unknowable) — the request received its 500; restart the process to recover from the WAL
kernel stopped: the store handle is dead after a disk IO failure (redb refuses every further operation until you close and re-open the database) — durable state is intact and the WAL is authoritative; restart the process to recover
```

The second is the **store latch**, and it is worth understanding because the
message is reassuring on purpose. redb keeps its own independent "this handle
is broken" flag: any error from its storage backend sets it, after which every
later redb call fails with "close and re-open the database". The usual
trigger is the store's own checkpoint fsync — store applies commit at
`Durability::None`, so the checkpoint is the only store sync charpente asks for
(redb itself also fsyncs the file after growing it).
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

One primary accepts writes; followers replay its log and arrive at identical
state (DESIGN §5 — replication is log-position arithmetic, not a merge). At
equal log position a follower holds the same rows, the same job queue and the
same event stream as the primary, and those *contents* match byte for byte.

What it does not mean is a byte-identical store file, and this one bites
operators. redb lays its pages out according to how records were grouped into
write transactions, and grouping is a scheduling accident: a follower that
falls a little behind and catches up in one wide batch commits in a single
transaction what the primary committed in forty. Two perfectly healthy,
fully caught-up nodes therefore hold `tables.redb` files that differ byte for
byte — and so does one node against its own earlier self, since a restarted
node replaying its log cannot reproduce the grouping its live traffic had.

So **do not build a health check that compares store files, their sizes, or
their checksums across nodes.** It will page you for corruption on a cluster
that is exactly converged. Compare log position instead — that is the number
replication is actually about, and it is what `promote` and `switchover` report
(`last_applied`, below) — and if you want to compare content, compare rows.

Replication is asynchronous: a follower acks a record only after it has synced
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

<!-- pinned -->
```text
peer secret missing or mismatched — set ReplConfig.peer_secret to match this node (or run the CLI verb with --secret)
```

`Some("")` is not a third option: an empty secret would admit any peer that
presents no proof at all — near-`None` inbound, while you believe auth is on —
which is easy to reach through an empty env var or a shell-quoting slip. It is
refused by name where `ReplConfig` is consumed, before any socket or state
work:

<!-- pinned -->
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
    // HTTP on 8080, replication on 9090; exits when the kernel stops.
    std::process::exit(
        serve::serve_replicated("0.0.0.0:8080", app, Box::new(ProdDisk::new("./data")?), cfg)?
            .wait(),
    );
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
    // HTTP on 8080, replication on 9091; exits when the kernel stops.
    std::process::exit(
        serve::serve_replicated("0.0.0.0:8080", app, Box::new(ProdDisk::new("./data")?), cfg)?
            .wait(),
    );
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

**This is not specific to a failover or an incident.** Every restart of a
primary with a non-empty witness set starts this same window — an ordinary
deploy or bounce with nothing wrong included — because the node cannot tell
"I just restarted" from "I restarted after being promoted away from" until
its witnesses answer. If your deploy process writes immediately after
restarting a node, expect this 503 rather than treat it as a bug: in the
common case a witness is alive and reachable, so the window is a round trip
or two, not a stall.

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
  naming both remedies (if the other timeline is the one to keep, re-seed
  this node: clear its data directory and restart it configured as a follower
  of the other node, never with its primary config, which brings the cleared
  node up as a second writable primary; or stop the other node, clear this
  node's witness set, and restart it if these records are). It never demotes
  itself: it cannot know which timeline you want, and the writes it acked are
  not the platform's to discard;
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
take it off the wire. It keeps accepting `Hello`s, keeps shipping catch-up to
peers that already hold a prefix of this timeline, and re-seeds a **recorded
witness** that returns with an empty data dir — the fresh-data-dir remedy this
section prescribes below, and precisely what lets the gate resolve. But it
**refuses to seed a brand-new follower** until verification finishes, and a
refused newcomer is not recorded as a witness (the refuse arm never assigns
`listen`, so `record_witnesses` never sees it):

<!-- pinned -->
```text
cannot seed a new follower yet: this node restarted as a primary and is still
verifying its witnesses — it has not confirmed that none of the followers it
had shipped to was promoted while it was down, so seeding would copy a
timeline it does not yet know it owns. This clears on its own once every
witness answers, and an existing follower's catch-up is unaffected meanwhile.
If the witnesses are permanently gone, see GUIDE §10 — note that clearing the
witness file re-opens the dual-primary gap it exists to close.
```

A positioned follower this node had not yet recorded, catching up normally,
can still be newly recorded as a witness while verifying — but it joins
neither the probe set nor the 503's list, both of which were fixed when the
gate closed, so it constrains only the *next* restart, not this one.

Under a witness that is gone for good, this means a brand-new follower cannot
join at all until you intervene — the escape hatch below (editing or deleting
the `witnesses` file, and the gap it re-opens) is the only way forward. This
costs little in practice: a gated node already refuses writes, so the only use
for a newly-seeded follower would be promotion, which is exactly the harm the
gate exists to prevent.

**Stopping instead of serving reads.** `ReplConfig::strict_witnesses` (default
`false`) changes the third outcome only. With it set, a node whose witnesses
are still unreached at the 30-second deadline stops rather than serving reads
it cannot write to. The other two outcomes are unaffected: every witness
answering *not a primary* still opens the gate, and one answering *as a
primary* still refuses regardless of this flag.

Two things about it are easy to get wrong. It is **not** "refuse to start":
whether a witness is unreachable is only established by probing, so a strict
node starts, serves reads for the whole 30-second window, and only then stops.
And **the stop reaches a supervisor on every node shape** (§5): under
`serve::backup` the error returns from the call, and under
`serve::serve_replicated` the kernel's stop ends the process through
`Server::wait` (§5) with status 1 — so a supervisor restarts it, and keeps
restarting it until a witness answers.

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
  holds every record does it hand off — no acked record is lost. The follower
  shows it holds them by acking them, or, when it reconnects already caught up,
  by the position its `Hello` carries. If the target cannot catch up before the
  deadline, the switchover aborts and the primary resumes.

**Once the hand-off is sent, there is no abort.** The deadline aborts a
switchover only while the target is still catching up. After the hand-off is
sent, the target may already be the primary, so the old primary never resumes
writes on its own. A target that loses the hand-off (its connection drops, or it
restarts before applying it) and reconnects at least a heartbeat (1 s) before
the deadline is sent it again: the resend goes out on the old primary's next
step, which the reconnected session's heartbeat brings within a second. If no
confirmation returns before the deadline, the old primary stays
write-stopped and replies with both possible answers to "did the target take
it?":

<!-- pinned -->
```text
switchover to <to>: the hand-off was sent at seq <seq> but no confirmation
returned before the deadline — this node stays write-stopped and will not resume
writes on its own, because <to> may already be the primary. If <to>'s role file
says primary, restart this node to resolve against <to>: it rejoins as <to>'s
follower. If it still says follower, <to> has not taken the hand-off yet and no
node accepts writes: promote it (charpente promote --node <to>) — it confirmed it
holds every record up to seq <seq> and this node has accepted no write since, so
its reply names last_applied Some(<seq>) and nothing is lost — then restart this
node to resolve against <to>. If the promote answers "cannot promote: this node
is already the primary", <to> took a hand-off still in flight at the deadline:
just restart this node
```

Nothing resends the hand-off after this reply. A target that still says
follower is in one of two states:

- **The hand-off is still in flight.** The drain and the hand-off share one
  deadline, so a hand-off sent late in the window may still be on its way. The
  target can take it after the reply and become the primary, which is the first
  case. A promote sent after that answers `cannot promote: this node is already
  the primary`, and the reply's last sentence says what that means: restart the
  old primary.
- **The target was down past the deadline**, or came back within a heartbeat
  of it, too late for the resend. Nothing changes on its own, and
  restarting the old primary alone does not help: it comes back still handing
  off, and resolves only once the target answers as a primary. Promote the
  target, as the reply says.

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
its data directory, and restart it **configured as a follower of the new
primary**, so it re-seeds from it. Change its startup config before that
restart: with the role file gone, config alone decides the role, and a cleared
node started with a primary config comes up as a second writable primary that
no witness gate can see, because the witness set goes with the directory too.
A switchover does not have the restart hazard (the handed-off primary demotes
itself durably), but its durable demotion lives in that same role file: wipe a
switchover-demoted primary and restart it with its old primary config, and it
too comes up as a second writable primary. Every re-seed, after a promote or
a switchover, restarts the cleared node configured as a follower of the
current primary (DESIGN §5).

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
  missing/mismatched `--secret`, a Pending job of a foreign shape (§6), …).
- exit 2 — usage, connect, or timeout failure (bad flags, the node
  unreachable, no reply before the deadline) — never a node-level decision.

When a follower's own local copy is unrecoverable — media corruption below its
checkpoint horizon, or a log directory a crash left gapped — the fix is not a
local repair but a restore: re-seed the damaged node from a healthy replica as
a fresh follower, promoting the replica first if the damaged node was the
primary. Clear the damaged node's data directory and restart it configured as
a follower of the current primary, never with a primary config (the promote
warning above says why), and its snapshot rebuilds the whole state, no log
replay. This is the promise a single node cannot make and a replicated
deployment can: a record one node loses to media corruption survives on
another.

The same deliberate re-seed applies to a follower that comes back holding data
after being offline past the primary's retention window (`retain_segments`):
the primary has GC'd the frames that would prove the follower's records are a
prefix of its timeline, so it refuses to re-seed it automatically — naming
both positions and both remedies — rather than risk silently overwriting a
follower that is actually *ahead* of a primary that forked behind it. If the
follower truly just lagged, stop it, clear its data directory, and restart it
configured as a follower of the primary, never with a primary config;
it re-seeds from a snapshot and converges.
charpente's own replication suite applies that remedy itself in exactly one
case: a follower it rebuilt from the surviving primary, provided the primary has
never recovered. What proves that follower a prefix of the primary's timeline is
this handshake, not the wipe: the rebuild converged only once the primary
accepted the follower's `Hello`. Wherever it cannot prove that, it leaves the
refusal standing, and so should you.

A role file torn by a crash refuses the boot by name, because a torn role cannot
be told from a real one. If the node never finished its first boot, delete the
`role` file and the node re-initializes from config. Otherwise a role flip tore
it — a `promote`, either end of a `switchover`, or an aborted one — and the
node's data is intact: delete the `role` file and restart the node configured
as a follower of the writable primary if another node is serving as one, and
as primary only if none is. Never restart it as primary beside a writable
primary: after a promote nothing stops it becoming a second one, because the
promote cleared the node's witness set before its flip. Restarting it as the role the
flip was writing is not the same rule either: a node whose switchover tore at
its first step, `handing-off`, never handed anything off, and restarted as
`handing-off` it stays write-stopped. A restore from a replica works too, as a
re-seed configured as above.

## 11. Realtime

SSE subscribers tail the log through a materialized `charpente.events` table with
exact resume: a client that reconnects with its last-received id provably misses
nothing (DESIGN §6, PLAN M1's exit criterion). Nothing here needs a message broker
— `tx.emit`/`tx.emit_keyed` write into the same committing log record as everything
else in the transaction, and delivery reads that table the same way the job
dispatcher reads `charpente.jobs`.

### `#[event]`

```rust
use charpente::wire::WireU64;
use charpente::{Event, event};

#[event(global)]
struct HTTPEvent {
    code: u16,
}

#[event(global)]
struct InvoicePDFSent {
    invoice: WireU64,
}

fn names() -> (&'static str, &'static str) {
    (HTTPEvent::NAME, InvoicePDFSent::NAME) // ("http_event", "invoice_pdf_sent")
}
```

An `#[event(...)]` says which tenancy the event has (next subsection), and derives five things a plain struct doesn't have: `Codec` (the log/table
format — the same machinery `#[table]`/`#[job]` use), `impl Event` (`NAME`, the wire
and delivery-filter name), `serde::Serialize` (the JSON a subscriber receives),
`Wire` (that JSON's schema, so `charpente describe` and a generated client can name
the payload type — which is also why a bare `u64`/`i64` field is a compile error
here, exactly as in a route's request or response: use `Id<T>` for a row id, or
`WireU64`/`WireI64`), and an `EventEntry` impl so `events![...]` can collect the type
(next section). It also derives `Event::Scope`, the tenant type the event belongs to (`Unscoped` for a global one), and `Event::FINGERPRINT`, a structural fingerprint of the payload's fields (§12's helper) that a scoped event continues with its tenant's `NAME`, recorded beside every emit; delivery decodes a stored
event only under the shape it was written with (see "Changing an event" below). A
`#[job]` payload has no `Wire` requirement, because it never leaves the process — only
an event's payload is itself a wire document.
`NAME` is the snake_case of the type name, computed at compile time — not a name you
choose, so it can never drift from the type. The splitting rule inserts `_` before an
uppercase letter that follows a lowercase letter or digit, and before the *last*
letter of a run of capitals when that letter is immediately followed by a lowercase
one — so a whole acronym run stays one word except right at its boundary with the
next word: `NoteAdded` → `note_added`; `HTTPEvent` → `http_event` (the run `HTTP`
stays together, splitting only where the trailing `E` turns out to start `Event`);
`InvoicePDFSent` → `invoice_pdf_sent` (`PDF` stays together, `S` starts `Sent`).

### An event's tenancy: `global` or `scoped_by`

An event says which tenancy it has, and neither is the default:
- `#[event(global)]` belongs to no tenant. It is emitted on a bare `Tx` (from inside a scope, through the explicit deref `(*tx).emit(…)`), and only a global `Subscription::of` feed receives it.
- `#[event(scoped_by = Org)]` belongs to one tenant. It is emitted only on that tenant's `ScopedTx`, which records the tenant beside the event, and only that tenant's feed receives it. That feed is built from the same `ScopedTx` the subscriber's scope line returned.

A bare `#[event]` is a compile error naming both. The two sides are checked against each other by type, so no string names a tenant anywhere in the app:

```rust
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx, Cursor, Subscription};
use charpente::wire::WireU64;
use charpente::{Id, Scoped, Subscribe, Tenant, Tx, Wire, event, route, table};
use serde::{Deserialize, Serialize};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, who: &Principal, scope: Id<Org>) -> Result<(), ApiError> {
        if who.subject() == format!("org:{}", scope.raw()) {
            Ok(())
        } else {
            Err(ApiError::BadRequest(format!(
                "not a member of org {}",
                scope.raw()
            )))
        }
    }
}

#[table(scoped_by = Org)]
struct Invoice {
    id: Id<Invoice>,
    paid: bool,
}

/// Belongs to one org: only that org's feed receives it.
#[event(scoped_by = Org)]
struct InvoicePaid {
    invoice: Id<Invoice>,
}

/// Belongs to no org: every global feed receives it.
#[event(global)]
struct MaintenanceScheduled {
    minutes: WireU64,
}

#[derive(Wire, Serialize, Deserialize)]
struct MaintenanceBody {
    minutes: WireU64,
}

#[route(POST "/orgs/:org/invoices/:id/pay", auth)]
fn pay(tx: &mut Tx, org: Id<Org>, id: Id<Invoice>, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    let mut invoice = tx.get(id)?.ok_or(ApiError::NotFound)?;
    invoice.paid = true;
    tx.put(&invoice);
    tx.emit(InvoicePaid { invoice: id }); // recorded with this org
    Ok(())
}

#[route(POST "/maintenance", auth)]
fn schedule_maintenance(tx: &mut Tx, body: MaintenanceBody) -> Result<(), ApiError> {
    tx.emit(MaintenanceScheduled {
        minutes: body.minutes,
    }); // recorded with no org
    Ok(())
}

/// One org's feed. The scope line admits the subscriber, and the feed built
/// from what it returns carries that org: it receives this org's
/// `InvoicePaid` rows and nothing else.
#[route(SSE "/orgs/:org/feed", auth, emits(InvoicePaid))]
fn org_feed(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(tx.subscription::<InvoicePaid>().cursor(cursor).into())
}

#[route(SSE "/maintenance/feed", public, emits(MaintenanceScheduled))]
fn maintenance_feed(
    tx: &mut Tx,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<MaintenanceScheduled>().cursor(cursor))
}
```

Every mismatch is a compile error, never a feed that silently receives nothing:
- A scoped event on a bare `Tx` (`tx.emit`, `tx.emit_keyed`), or in a global feed (`Subscription::of`, `.and`), names no tenant. rustc prints the note below.
- A global event through `ScopedTx::emit` is E0271, "type mismatch resolving `<SystemNotice as Event>::Scope == Org`", with "expected this to be `Org`" pointing at the event's `#[event(global)]`. A global event emitted from inside a scope is written `(*tx).emit(ev)` in a handler, and `(**tx).emit(ev)` in a scoped job's `run`, which holds `&mut ScopedTx`.
- An event scoped by another type, through `ScopedTx::emit`, `tx.subscription::<Ev>()` or its `.and::<Ev2>()`, is the same E0271, its "expected this to be `Org`" pointing at that event's own `#[event(scoped_by = …)]`. So is a global event on a tenant feed.

The tenant rides the record beside the event, apart from any app key. `tx.emit_keyed(key, ev)` inside a scope records both, and `.key(…)` on either kind of feed narrows it further by that app key (a per-user feed, say). A key never names a tenant.

A scoped event's fingerprint continues its fields' fold with its tenant's `Tenant::NAME` (§16). So moving an event between `global` and `scoped_by`, or between two tenant types, is a change of shape, handled like any other ("Changing an event", below). Two same-named events scoped by two tenant types never share a shape, because the tenant census gives every tenant type the app mounts its own `NAME` (§16). So where the two types' raw ids coincide, one's row is never delivered to the other's feed. It meets that feed as a foreign shape, which ends the feed with `charpente.reshaped` rather than decoding. `mount_events` refuses two events with one name, so an app that mounts every event it emits never meets this.

The bound alone would not hold that for a hand-written `impl Event`, which states its own `FINGERPRINT`. One declared `type Scope = Team`, with `NAME` and `FINGERPRINT` copied from `Org`'s event, meets every `Team` bound. Emitted in `Team` 1, or subscribed in `Team` 1's feed, it would write into or read from `Org` 1's feed. So every typed site that names an event type also checks its shape at build time, against the tenancy its own bound names. `Event::PAYLOAD_FINGERPRINT` is the payload's field fold before any tenant is mixed in: `#[event(scoped_by = Org)]` states it, and for a global event it defaults to `FINGERPRINT`, so a hand-written scoped impl states it too, or the default fails the check below. `ScopedTx::emit`/`emit_keyed`, `tx.subscription::<Ev>()` and `TenantFeed::and` require `FINGERPRINT == scoped_fingerprint(PAYLOAD_FINGERPRINT, S::NAME)` for their own `S`. `Tx::emit`/`emit_keyed`, `Subscription::of` and `Subscription::and` require `FINGERPRINT == PAYLOAD_FINGERPRINT`. The check is a `const` block, so a violation fails the build, as rustc's E0080 naming the instantiation (`ScopedTx::<'_, '_, Team>::emit::<Spy>`, say), never a request. Like every post-monomorphization error, `cargo build` and `cargo test` report it and `cargo check` does not. The two texts, scoped and global:

<!-- pinned -->
```text
contract: a scoped event's recorded shape is its payload's field fold mixed with its declared tenant's NAME — `FINGERPRINT == scoped_fingerprint(PAYLOAD_FINGERPRINT, <S as Tenant>::NAME)` for an event scoped by `S` — and this event's is not; the likely causes are a hand-written `impl Event` that states `FINGERPRINT` but not `PAYLOAD_FINGERPRINT`, which then defaults to `FINGERPRINT` (a scoped event states both), or one whose `FINGERPRINT` was copied from another event type, or whose `type Scope` names another tenant than the one its `FINGERPRINT` mixes, either of which would let its rows read as another tenant type's event; fix: derive it with `#[event(scoped_by = S)]`, or set `PAYLOAD_FINGERPRINT` to its fields' fold and `FINGERPRINT` to `charpente::core::db::scoped_fingerprint(Self::PAYLOAD_FINGERPRINT, <S as Tenant>::NAME)` (docs/GUIDE.md §11, "An event's tenancy")
contract: a global event's recorded shape is its payload's field fold alone — `FINGERPRINT == PAYLOAD_FINGERPRINT` for an event whose `type Scope` is `Unscoped` — and this event's two differ, so its rows would read as another event type's; the likely cause is a hand-written `impl Event` whose `FINGERPRINT` was copied from a scoped event, or whose `type Scope` was changed to `Unscoped` and its `FINGERPRINT` was not; fix: derive it with `#[event(global)]`, or set `FINGERPRINT` to its fields' fold and drop `PAYLOAD_FINGERPRINT`, which defaults to it (docs/GUIDE.md §11, "An event's tenancy")
```

A global event's text guards consistency, not a door: `Subscription::admits` already keeps every global feed off every tenant's rows. A hand-written scoped event that passes the check is shaped exactly as `#[event]` would shape it:

```rust,ignore
impl Event for InvoiceVoided {
    const NAME: &'static str = "invoice_voided";
    const PAYLOAD_FINGERPRINT: u64 = 0x5e3a_91c4_07d2_6b18; // its fields' fold (§12's helper); illustrative
    const FINGERPRINT: u64 = scoped_fingerprint(Self::PAYLOAD_FINGERPRINT, <Org as Tenant>::NAME);
    type Scope = Org;
}
```

Two limits stay outside the check, stated rather than engineered around. First, a `PAYLOAD_FINGERPRINT` chosen so that its mix with `Team`'s `NAME` equals `Org`'s fingerprint passes it. FNV-1a is not a cryptographic hash, so such a value can be computed, and computing it is a bypass built on purpose in app code, which no type check refuses: it needs no `Db`, only a hand-written `impl Event` and the arithmetic. Second, the `NAME` the check mixes is only as unique as the tenant census makes it. A tenant type the app reaches only through an event it never mounts is outside the census. So two tenant types under one `NAME`, one of them never mounted, mix alike, and their same-named, same-field events share a shape. Mount every event with `events![…]` (next subsection), and `mount_events` refuses the pair before the app serves.

What `#[event]` rejects, all compile errors naming the contract and the fix. The first is the one every event meets once, and the next three are its neighbours (§9 lists the rest):

<!-- pinned -->
```text
`<event>` declares no tenancy — an event says which, and neither is the default: add `global` or `scoped_by = <Tenant>` to this attribute's arguments, keeping its others. `scoped_by = <Tenant>` if it belongs to one tenant (it is emitted from inside that tenant's scope, and only that tenant's feed receives it); `global` if it belongs to no tenant (it is emitted on a bare `Tx`, and only a global `Subscription::of` feed receives it).
`global` and `scoped_by = <scope>` on one event — an event is one or the other; drop one
`global` given twice — write it once
`scoped_by` given twice — write it once
```

A scoped event where no tenant is named is an unsatisfied bound. rustc prints this note with your event and scope in place of `<event>` and `<scope>` (§16 shows it rendered):

<!-- pinned -->
```text
`<event>` is a tenant-scoped event (`#[event(scoped_by = <scope>)]`), and a bare `Tx` or a global `Subscription` names no tenant
emit it and subscribe to it inside the tenant it belongs to:
    let mut tx = tx.scope::<<scope>>(scope, ctx)?;
    tx.emit(event); // recorded in this tenant
and, in an SSE handler, that tenant's feed (`use charpente::Subscribe;`):
    Ok(tx.subscription::<<event>>().cursor(cursor).into())
a job that emits it takes `scoped_by = <scope>` in place of `global` among its `#[job(…)]` arguments, keeping the others, and emits through the `ScopedTx` its `run` receives
```

### Registering events: `events![...]` and `mount_events`

Like `jobs![...]`, `events![...]` collects descriptors explicitly and
`App::mount_events(...)` registers them — but for a different purpose. This
registry is read by `charpente describe` (and by the client generator
downstream of it), by the mount-time tenant census (a scoped event's tenant type
joins it, §16, "One `NAME`, one tenant type"), and by **nothing on the delivery
path**: emitting and subscribing work exactly the same whether or not an event is
registered, because `Subscription::of::<Ev>()` and `tx.subscription::<Ev>()` capture
their encoders at the subscribe site. What registration buys is that the event, and
its payload type, appear in the contract at all, and that its tenant type is checked
against every other the app names:

```rust
use charpente::http::{App, AppConfig};
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::wire::WireU64;
use charpente::{Schema, event, events};

#[event(global)]
struct NoteAdded {
    note: WireU64,
}

fn build_app(data_dir: &str) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(ProdDisk::new(data_dir)?),
        Schema::new(),
        Box::new(ProdEntropy::new()?),
        AppConfig::default(),
    )?
    .mount_events(events![NoteAdded]))
}
```

That app's `Contract.events` carries one entry — `{"name": "note_added",
"payload": "NoteAdded", "scoped_by": null}` — and `NoteAdded`'s own field schema
joins `"types"` beside the route types (§13). `scoped_by` is `null` because `NoteAdded` is `global`. For an
`#[event(scoped_by = Org)]` it names the tenant type by its `Tenant::NAME`
(`"Org"`). An event's tenancy is a declaration on its type, so the contract
states it. An app key is a call-site choice (`tx.emit_keyed`), so the contract
lists no keys.

Mounting the same event type twice — in one `mount_events` call, or across
several — panics at mount time: `"event \"note_added\" registered twice —
each #[event] type is mounted once"`. The name in that message is the event's
`NAME` (snake_case), not the Rust type name.

### Emitting: `tx.emit` and `tx.emit_keyed`

```rust,ignore
impl Tx<'_> {
    pub fn emit<Ev: UnscopedEvent>(&mut self, event: Ev);                            // a global event
    pub fn emit_keyed<Ev: UnscopedEvent>(&mut self, key: impl Into<String>, event: Ev);
}
impl<S> ScopedTx<'_, '_, S> {
    pub fn emit<Ev: Event<Scope = S>>(&mut self, event: Ev);                        // an event scoped by S
    pub fn emit_keyed<Ev: Event<Scope = S>>(&mut self, key: impl Into<String>, event: Ev);
}
```

`UnscopedEvent` is exactly the events declared `global`. It is sealed, so `impl UnscopedEvent for InvoicePaid {}` is itself refused (§16).

Both buffer into the transaction's effects exactly like `enqueue`/`enqueue_keyed` —
nothing is visible to a subscriber until the transaction's one log record commits.
`emit_keyed`'s key is an app filter (a user's subject, a room name — whatever a
`Subscription` should be allowed to narrow on); the platform never parses it, only
exact-matches it at delivery (`Subscription::key`, below). The match rule is exact on
both sides: an *unkeyed* subscription (no `.key(...)`) receives every event of its
types, keyed or not; a *keyed* subscription receives only events whose key is an exact
match — which means a keyed subscription does **not** receive `tx.emit` (unkeyed)
events, since no key equals the absence of one. A key is not a tenant: which tenant an
event belongs to is recorded by the scope it was emitted in (next), never spelled as a
string.

### Emitting inside a tenant, and the tenant feed

Inside a tenant scope you do not choose. `tx.emit` and `tx.emit_keyed` on a `ScopedTx`
— the handle `tx.scope::<Org>(org, ctx)?` returns — record the scope's tenant in the
event's own row, apart from any key, so a transaction that had to be admitted to reach a
tenant's rows cannot hand that tenant's event to anyone outside it:

```rust,ignore
// on ScopedTx<'_, '_, S>:
pub fn emit<Ev: Event<Scope = S>>(&mut self, event: Ev)                                // recorded in this tenant
pub fn emit_keyed<Ev: Event<Scope = S>>(&mut self, key: impl Into<String>, event: Ev)  // the same, plus the app's key
```

A tenant's feed is built from the same scope: `tx.subscription::<Ev>()` on the
`ScopedTx` (the `charpente::Subscribe` trait) returns a `TenantFeed` carrying that
tenant, and `.into()` makes it the handler's `Subscription`:

```rust
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx, Cursor, Subscription};
use charpente::wire::WireU64;
use charpente::{Id, Scoped, Subscribe, Tenant, Tx, Wire, event, route, table};
use serde::{Deserialize, Serialize};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, who: &Principal, scope: Id<Org>) -> Result<(), ApiError> {
        if who.subject() == format!("member:{}", scope.raw()) {
            Ok(())
        } else {
            Err(ApiError::BadRequest(format!(
                "not a member of org {}",
                scope.raw()
            )))
        }
    }
}

#[event(scoped_by = Org)]
struct NoteAdded {
    n: WireU64,
}

#[derive(Wire, Serialize, Deserialize)]
struct NoteBody {
    n: WireU64,
}

/// Recorded in `org`: the row carries the tenant, apart from any key.
#[route(POST "/orgs/:org/notes", auth)]
fn add_note(tx: &mut Tx, org: Id<Org>, body: NoteBody, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.emit(NoteAdded { n: body.n });
    Ok(())
}

/// `org`'s feed: the scope that admitted the subscriber builds it, so it
/// delivers only events recorded in `org`.
#[route(SSE "/orgs/:org/feed", auth, emits(NoteAdded))]
fn feed(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(tx.subscription::<NoteAdded>().cursor(cursor).into())
}
```

Delivery compares tenancy exactly. A tenant's feed receives only the events recorded in
its own tenant — never another tenant's, never a global one — and a global subscription
(`Subscription::of`) never receives an event recorded in a tenant, tenant 0 included,
whatever key it narrows on. Nothing in the app spells a tenant as a string, on either
side. `.key(...)` still narrows a tenant feed further, by the app's own key.

An emit that genuinely belongs to no tenant is written `(*tx).emit(…)` — the same
explicit deref an unscoped table access takes — and `(**tx).emit(…)` in a scoped job's
`run`, which holds `&mut ScopedTx`. It is recorded with no tenant and reaches global
subscriptions only. A tenant feed carries only the events `TenantFeed::and` added before
`.into()`: `Subscription::and` adds global events, so on a tenant feed it panics rather
than build a subscription that could never deliver what it names:

<!-- pinned -->
```text
Subscription::and: event "<name>" is added to a tenant feed after .into() — Subscription::and adds global events, and a tenant feed receives only its own tenant's events, so this one could never arrive; add the tenant's events with TenantFeed::and before .into(), and serve global events on a subscription of their own
```

A tenant feed matches its tenant by type as well as by id. Its events are declared
`scoped_by` that type (the bound), each one's fingerprint carries the type's `Tenant::NAME`,
and the feed's builders check at build time that it does, so a hand-written impl cannot
copy another type's ("An event's tenancy", above, with its two stated limits). So where
two tenant types' raw ids coincide, one type's events never reach the other's feed.
A same-named event of the other type meets the feed as a foreign shape and ends it with
`charpente.reshaped`, never decoded. `mount_events` refuses two events with one name, so
an app that mounts every event it emits never meets that.

This is the realtime face of tenant scoping, and it is documented here rather than twice:
§16 covers the mechanism it belongs to — declaring `scoped_by`, writing the scope's
`authorize`, and the cross-tenant capability — and points back here for the emit and
subscribe shapes above.

One contract rides along with the `Serialize` derive: every `#[event]` payload must be
plainly JSON. serde_json writes a map key quoted when it serializes as a string, a
number or a bool, and refuses any other key (a `Vec<u8>`, tuple or struct key, say) and
a non-finite float key. It does not refuse a `NaN` or infinite float value — it writes
`null`, so a client receives `null` where a number was. Neither is caught at emit time
(the log stores `Codec` bytes; JSON encoding happens at delivery). An event that fails
to encode fails its own tail and never the node: the stream ends with one terminal
frame, the same frame a stored event whose bytes do not decode gets:

<!-- pinned -->
```text
event: charpente.error
data: {"event":"<event>","id":<id>,"error":"<message>"}
```

The frame has no `id:` line, so the client's `Last-Event-ID` stays before the event
and every reconnect ends at the same row, the way a corrupt table row answers 500 on
every read. The `error` text names the event and the fix: for an encode failure,
`event <name> failed JSON encoding: <serde_json's message> — every #[event] payload
must be plainly JSON (GUIDE §11); fix the emitting code`. A payload that fails to
decode although its recorded shape matches this binary has two possible causes, and
the text names both with their fixes: a change inside a type one of its fields holds,
which the fingerprint cannot see (§12) — restore that type's old definition and put the
new one in a new `#[event]` type under a new name ("Changing an event" below) — or
corrupt bytes — restore the node from its archive (§15) or re-seed it from its primary
(§10). A row whose own envelope is corrupt gets the same frame, with `id` naming the
first event this tail had not yet received and no `event`. The envelope is what holds a
row's name and key, so such a row ends every tail whose scan reaches it, whatever the
tail subscribes to or filters on. The frame also arrives in place of the whole batch the
row was read in (up to 64 rows): the good rows before it in that batch are withheld too,
and every reconnect from the client's cursor withholds them again.

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
use charpente::wire::WireU64;
use charpente::{Schema, Tx, Wire, route, routes};
use charpente_sim::Sim;
use serde::{Deserialize, Serialize};

// An `#[event]` payload IS a wire type — it is exactly the JSON a
// subscriber receives — so `#[event]` derives `Wire` and `n` needs the same
// `WireU64` a route body would.
#[event(global)]
struct Ping {
    n: WireU64,
}

#[derive(Wire, Serialize, Deserialize)]
struct PingBody {
    n: WireU64,
}

#[route(POST "/ping", public)]
fn send_ping(tx: &mut Tx, body: PingBody) -> Result<(), ApiError> {
    tx.emit(Ping { n: body.n });
    Ok(())
}

/// A real handler would narrow `.key(...)` to the caller's own subject, read
/// off `ctx.session()` exactly like any other route's access control — the
/// platform enforces nothing about the key beyond the exact-match filter. A
/// tenant's feed is built from its scope instead ("Emitting inside a tenant",
/// above).
///
/// `emits(Ping)` declares what this stream carries, for the contract and the
/// generated client — see "Declaring what a stream carries" below.
#[route(SSE "/feed", public, emits(Ping))]
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
impl Subscription {                             // a global feed
    pub fn of<Ev: UnscopedEvent + serde::Serialize>() -> Subscription;
    pub fn and<Ev: UnscopedEvent + serde::Serialize>(self) -> Subscription; // another global event
    pub fn key(self, key: impl Into<String>) -> Subscription;
    pub fn cursor(self, cursor: Option<Cursor>) -> Subscription;
}

pub trait Subscribe<S> {                        // on ScopedTx<'_, '_, S>: a tenant feed; `use charpente::Subscribe`
    fn subscription<Ev: Event<Scope = S> + serde::Serialize>(&self) -> TenantFeed<S>;
}

impl<S> TenantFeed<S> {                         // charpente::http::TenantFeed
    pub fn and<Ev: Event<Scope = S> + serde::Serialize>(self) -> TenantFeed<S>;
    pub fn key(self, key: impl Into<String>) -> TenantFeed<S>;
    pub fn cursor(self, cursor: Option<Cursor>) -> TenantFeed<S>;
}
// impl<S> From<TenantFeed<S>> for Subscription: a tenant feed ends `.into()`.
```

`Subscription::of::<Ping>()` names the event type as a Rust type, not a string — a typo
is a compile error, never a silently-empty subscription. `.and::<Ev2>()` adds a second
type to the same tail, and panics, naming the event, if one of its `NAME` is already
there (see "Changing an event" below); `.key(...)` narrows delivery to one exact-match
value; `.cursor(...)` takes the resume point the platform already extracted (next) — the
handler can pass it straight through, as `feed` does above, or clamp it to some
app-level boundary. A `Subscription` from `of` is global; one converted from a
`TenantFeed` carries its tenant, which no app code can set or read.

### Declaring what a stream carries

`Subscription::of::<Ping>()` runs inside the handler's body, so nothing outside the
handler can see it — not `App::describe`, and therefore not a generated client, which
would otherwise have to offer "subscribe to anything". A stream route says so on the
attribute instead:

```rust,ignore
#[route(SSE "/feed", public, emits(Ping, Pong))]
```

The names inside `emits(...)` are the `#[event]` TYPES, exactly like `Subscription::of`
— a typo is a compile error, and a type that isn't an `#[event]` fails with an
unsatisfied `Event` bound naming it. The contract records BOTH names each type has, so
`Ping` shows up in `RouteInfo.emits` as
`EmittedEvent { event: "ping", payload: "Ping" }` (`charpente::http::EmittedEvent`)
without you repeating the snake_case rule anywhere. `event` is the `Event::NAME` an SSE
frame's `event:` field carries; `payload` is the `Wire::WIRE_NAME` a generated client's
union member uses. Both are recorded because neither derives from the other —
`#[event]`'s acronym rule maps `HTTPServer` to `http_server`, and nothing inverts that.
Their payload types join `Contract.types` too, so a client can resolve the union even in
an app that never calls `mount_events`.

Three things it deliberately is not:

- **Not a filter.** Delivery still runs off the encoders `Subscription::of::<Ev>()`
  captured. Declaring `emits(Ping)` does not stop a handler subscribing to `Pong`, and
  it does not make the platform drop a `Pong` frame.
- **Not required.** Leave it off and the route's `emits` is `[]` — `stream: true` with
  no binding, exactly as before this existed. Combine it with `auth` in either order —
  `#[route(SSE "/feed", auth, emits(Ping))]` and `#[route(SSE "/feed", emits(Ping), auth)]`
  both parse, and both are pinned by
  `crates/charpente-macros/tests/compile_pass/route_emits_on_sse.rs`.
- **Not the key.** `.key(...)` narrows which *rows* reach the tail, and that value is a
  per-emit call-site choice the contract cannot know. `emits(...)` names event TYPES, so
  a keyed feed declares the same types an unkeyed one does.

Since it is a declaration rather than a rule, it can drift from the handler. That is
handled with an oracle: `App::subscription_event_names(route_name)` runs the route's
subscribe glue and reports the names the returned `Subscription` actually holds, and a
test compares the two. It takes `&mut self`, because reaching a live `Tx` means opening
a transaction group:

```rust,ignore
let mut app = build_app(&sim)?;                 // &mut — the probe runs the handler
let feed = app.describe().routes.iter().find(|r| r.name == "feed").unwrap().emits.clone();
// A `Subscription` holds `Event::NAME`s and nothing else, so the `event` half is
// the half this compares; the `payload` half is pinned by your contract golden.
let declared: Vec<&str> = feed.iter().map(|e| e.event).collect();
let actual = app.subscription_event_names("feed").unwrap();
assert_eq!(declared, actual);
```

Order counts — the declaration's order is the contract's order — so write `emits(...)`
in the same order the handler chains `of`/`and`. `crates/charpente/tests/describe.rs`
and `crates/charpente/tests/realtime.rs` both carry this check; copy one into your own
app's tests and every stream route you declare stays honest for free.

**One thing to know before you copy it: the probe really runs your handler**, inside a
transaction group it then commits. A read-only subscribe handler stages nothing and this
costs nothing, which covers essentially every handler. But if yours writes on the
subscribe path — minting a stream credential with `ctx.create_session` is the case that
comes up — the probe refuses it with an error naming the route, *after* the write has
committed: a group holding a staged record cannot be dropped uncommitted, so refusing
means telling you, not undoing it. And because the probe feeds a fixed token rather than
drawing entropy (it must not perturb your app's entropy stream), a credential minted
that way is the same one every run. Move the write off the subscribe path, or leave that
route out of the drift test.

The probe presents no credential and does not enforce `auth`, so an `auth` stream route is
probeable, unless its handler opens a tenant scope. Then the scope line refuses the probe
exactly as it refuses any caller without a credential (§5's rule 3), and the probe's error
quotes the 401 a request would get (status and message), which says the route requires a
credential, so the probe cannot run it. Leave that route out of
the drift test too; widening it to `public(Org)` so a test can read it is not the fix.

### Cursor semantics

The platform extracts the client's resume cursor *before* the handler runs, so an
unparseable one never reaches app code:

1. The `Last-Event-ID` header, if present — this is what makes a browser's native
   `EventSource` reconnection resume automatically, no client code required.
2. Else the `cursor` query parameter (`?cursor=<id>.<seq>`) — for a first connect
   from a non-browser client, or a manual reconnect.
3. Else `None` — "from now". The tail starts at the node's next event id at the
   moment the node *serves* the subscribe, not the moment the client sent it. An
   event the node committed before that moment is not delivered, and on a
   follower that includes every event it had applied from the primary's log, even
   one that landed while the request was on its way. On a primary, a change that
   arrived ahead of the subscribe is behind the tail and one that arrived behind it
   is delivered, even at the same instant, because a subscribe ends its request
   batch. The stream does not say where that moment fell, so a client that must not
   miss an event resumes with a cursor.

A present-but-unparseable cursor is a 400, and the handler never runs:

<!-- pinned -->
```text
cursor must be "<id>.<seq>" — got "not-a-cursor"; omit it to tail from now
```

The wire form is always `<id>.<seq>`: an event id and its committing record's log
seq, joined by a dot (`Cursor::wire`). The seq half is not decorative — two diverged
timelines (an old primary's unshipped tail vs. a promoted follower's new writes, §10)
can mint the same event id for two *different* events, and a bare id can't tell them
apart. Resume validation at subscribe time checks, in order:

- **Below the retention floor** — the cursor's id predates everything this node
  still retains:

  ```text
  cursor 1.0 is below this node's retention floor 8 — events that old are
  gone; reconnect without a cursor, wait for the stream to open, then read
  current state from the same node, holding every frame that arrives until
  that read is installed and then applying the held frames and every later
  one over it in order, idempotently
  ```

- **Corrupt** — the retained row at that id exists, but its own bytes fail to
  decode: a 500, not a 409, since retrying reads the same corrupt bytes again
  rather than a transient fault:

  ```text
  cursor 8.42 sits on a stored event row that could not be decoded: <the
  underlying decode error> — the stored bytes are corrupt; restore this node
  from its archive (GUIDE §15) or re-seed it from its primary (GUIDE §10)
  ```

- **Diverged** — either the cursor's id is ahead of anything this node has ever
  minted:

  ```text
  cursor 999.1 diverged from this node's timeline (next event id is 10) —
  your history came from another primary; reconnect without a cursor, wait
  for the stream to open, then read current state from the same node,
  holding every frame that arrives until that read is installed and then
  applying the held frames and every later one over it in order,
  idempotently
  ```

  or the retained row at that id carries a different seq than the client
  remembers (the cross-timeline case — the id was reused by a different
  history):

  ```text
  cursor 8.42 diverged from this node's timeline (event 8 here has seq 3,
  not 42) — your history came from another primary; reconnect without a
  cursor, wait for the stream to open, then read current state from the
  same node, holding every frame that arrives until that read is installed
  and then applying the held frames and every later one over it in order,
  idempotently
  ```

- **Another tenancy** — the retained row at that id is an event of another tenancy than
  this subscription's: another tenant's event on a tenant feed, a global event on a
  tenant feed, or a tenant's event on a global subscription. A cursor resumes only the
  feed that delivered it, so this one is refused rather than resumed, and the refusal
  names no tenant and no event (below).

- **Reshaped** — the retained row at that id is an event this subscription carries,
  written under a different shape of it (see "Changing an event" below).

The tenancy refusal, exactly:

<!-- pinned -->
```text
cursor <id>.<seq> sits on an event outside this feed's tenancy — a cursor resumes only the feed that delivered it, and this feed never delivers that event; reconnect without a cursor, wait for the stream to open, then read current state from the same node, holding every frame that arrives until that read is installed and then applying the held frames and every later one over it in order, idempotently
```

Four of these five named errors end with the identical fix: reconnect with no
cursor at all, wait for the stream to open, then read current state from the
same node with ordinary reads, holding every frame that arrives until that read
is installed and then applying the held frames and every later one over it in
order, idempotently — a fresh "from now" subscribe never revalidates against
history it doesn't have. The
corrupt-row case does not: reconnecting without a cursor does not un-corrupt
the stored bytes, so its fix is to restore this node from its archive or
re-seed it from its primary instead.

**The order is the fix**, twice over. The first order is the requests'.
Subscribe first and read second, from the node that serves the stream. A change
that lands between the stream opening and the read being served arrives by both
routes, which is why each frame must be safe to apply twice. Read first and
subscribe second, and a change that lands after the read is served and before
the subscribe is served reaches the client by neither route: it is not in what
the read returned, and the tail starts after it.

The second order is the one the client applies their results in. Frames keep
arriving while the read is in flight, on another connection, so the frame of a
change that landed after the read was served can reach the client before the
read does. Install the read when it arrives and it overwrites that change, and
nothing delivers it again. So hold every frame that arrives until the read is
installed, install it, then apply the held frames and every later one over it,
in stream order and idempotently.

`realtime.rs`'s
`the_resync_remedy_subscribe_first_then_read_misses_no_change_in_any_window`
lands one change in each of the five windows around the two requests, on a
follower and on a primary, and every change arrives.
`the_resync_remedy_read_first_then_subscribe_misses_the_changes_between_its_steps`
shows the read-first order missing exactly those two windows.
`the_resync_remedy_ends_at_the_nodes_state_when_frames_wait_for_the_read`
judges the state the client ends in, over every order the read and the frames
can arrive in: holding the frames ends at the node's state, and
`installing_the_read_on_arrival_or_reading_first_ends_short_of_the_nodes_state`
shows installing the read on arrival ending without the last change. The
TypeScript client's `onOpen` (§14) is the moment to read, and it is not
awaited: frames, and even `onEnd`, can reach the caller before that read
returns.

### Delivery: one bounded path, no live/catch-up modes

A validated subscribe joins the kernel's ready set as a `Tail` — data-gated, never a
timer (DESIGN §1): it becomes eligible the instant some commit advances
`charpente.events` past its position. One unit of delivery reads up to
`TAIL_BATCH_EVENTS` (64) rows, filters by the subscription's names, its tenancy and its
key — one predicate, which resume validation also asks before it refuses a reshaped
cursor — JSON-encodes matches, and pushes frames to the connection's sink. Validation's
tenancy refusal, checked before that, compares only the row's recorded tenant with the
subscription's, whatever the row's name and key: a row of another tenancy is one no feed
of this tenancy delivered, so it is refused even where the predicate would skip it for
its name or key. There is no
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
data: {"floor":4,"tail":1,"fix":"reconnect without a cursor, wait for the stream to open, then read current state from the same node, holding every frame that arrives until that read is installed and then applying the held frames and every later one over it in order, idempotently"}

```

A tail that reaches an event written under a different shape of its type ends the
same way, with `charpente.reshaped` (see "Changing an event"), and an event that
cannot be rendered ends it with `charpente.error`. None of these frames stops the
node.

A sink that reports itself full or closed — a slow browser tab, a dead TCP
connection — costs its own tail immediately; it never blocks the kernel loop for
anyone else. Nothing is sent in that case (there is no one left to tell); the
client's own reconnect-with-`Last-Event-ID` behavior is the entire recovery story,
which is why every case above ends the same way: reconnect, then re-sync.

### The wire

```text
id: 0.0
event: ping
data: {"n":"1"}

id: 1.1
event: ping
data: {"n":"2"}

```

Each emit above is its own request, hence its own commit — a different `seq` each
time; two events emitted by the *same* transaction share one `seq` (§4's "call
order" note applies to delivery order too). Each frame is `id: <id>.<seq>`,
`event: <name>`, `data: <json>`, then a blank line.
`id` matches `Cursor::wire` exactly, which is what makes a browser's native
`Last-Event-ID` resume work with zero client code — except `charpente.error`, which
carries no `id:` line (above). An idle connection gets a `:hb` comment frame every 15
seconds — pure keepalive, no semantics, never interpreted as an event.

### Changing an event

Every emit records its payload's shape — `Event::FINGERPRINT`, the structural
fingerprint `#[event]` computes from the fields (§12) — and delivery decodes a stored
event only under the shape it was written with. Change an event's fields, and every
retained row of the old shape is one this binary will not decode. There is no per-row
versioned decode and no override.

A live tail that reaches an old-shape row gets one terminal frame instead of a wrong
or crashing decode, and its connection closes — the `charpente.gap` lifecycle. Its
`id:` line is that row's own `<id>.<seq>`:

<!-- pinned -->
```text
event: charpente.reshaped
data: {"event":"<event>","id":<id>,"fix":"reconnect without a cursor, wait for the stream to open, then read current state from the same node, holding every frame that arrives until that read is installed and then applying the held frames and every later one over it in order, idempotently"}
```

A client that ignores the frame — a browser's `EventSource` reconnects on its own,
with `Last-Event-ID` set to that id — is refused at subscribe, so it cannot resume
past an event it never received:

<!-- pinned -->
```text
cursor <id>.<seq> sits on an "<event>" event written under a different shape of that event (recorded <recorded>; this binary <fingerprint>) — this binary will not decode it; reconnect without a cursor, wait for the stream to open, then read current state from the same node, holding every frame that arrives until that read is installed and then applying the held frames and every later one over it in order, idempotently
```

`<recorded>` reads `none — written before event shapes were tagged` for a row emitted
before shapes were recorded, and such a row is refused like any other foreign one. A
cursor on a foreign row of an event the subscription does not carry, or one its key
filter excludes, is not refused: the subscription would never deliver that row anyway.
A cursor on a row of another tenancy is refused either way ("Cursor semantics", above):
no feed of this subscription's tenancy ever delivered it. The node keeps serving
throughout, and every other tail is untouched.

**To keep exact resume across a deploy, do not change an existing event's fields.** Add
a new `#[event]` type under a new name, with the new fields, emit it from the new code,
and keep the old type in code — and in any subscription that still needs it — for as
long as its rows are retained (`DbConfig::event_retention`). A new name is not optional:
`NAME` is the type's own snake_case, so putting the new type in a new module with the
*same* type identifier (`mod v2 { struct IncidentOpened { ... } }`) still collides on
`incident_opened`. The pair compiles; `App::mount_events` panics on it, and so does a
subscription carrying both (`.and`, naming the event). A subscription carrying only the
older type treats every row of the newer shape as a foreign one, and ends at the first
with `charpente.reshaped`. Old rows keep decoding under their own type. Changing the
fields in place costs every subscriber positioned before the change one re-sync. What
the fingerprint cannot see, a change inside a type a field holds, is in §12; a stored
row such a change breaks ends its tail with `charpente.error` (above), whose text names
that cause beside corrupt bytes.

### Testing a tail in sim

The whole-app sim driver from §8 covers SSE routes with no extra machinery — a
subscribe is just a `GET` request whose kernel response is an upgrade instead of a
one-shot reply, recorded as a `SimSinkHandle`:

```rust
use charpente::event;
use charpente::http::{ApiError, App, AppConfig, Ctx, Cursor, Method, Request, Subscription};
use charpente::kernel::{Kernel, SimSource};
use charpente::wire::WireU64;
use charpente::{Schema, Tx, Wire, route, routes};
use charpente_sim::Sim;
use serde::{Deserialize, Serialize};

const SEC: u64 = 1_000_000_000;

// An `#[event]` payload IS a wire type — it is exactly the JSON a
// subscriber receives — so `#[event]` derives `Wire` and `n` needs the same
// `WireU64` a route body would.
#[event(global)]
struct Ping {
    n: WireU64,
}

#[derive(Wire, Serialize, Deserialize)]
struct PingBody {
    n: WireU64,
}

#[route(POST "/ping", public)]
fn send_ping(tx: &mut Tx, body: PingBody) -> Result<(), ApiError> {
    tx.emit(Ping { n: body.n });
    Ok(())
}

#[route(SSE "/feed", public, emits(Ping))]
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
            Request::new(Method::Post, "/ping").json(&PingBody { n: WireU64(n) }),
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
        Request::new(Method::Post, "/ping").json(&PingBody { n: WireU64(3) }),
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

**What the fingerprint cannot see.** It hashes each field's name and the *tokens* of
its type, not the type's definition.
The tokens are hashed as written, rendered by charpente itself with one space between
tokens: `Vec<u8>` and `std::vec::Vec<u8>` are different shapes to it, so respelling a
field's type is a shape change, and a compiler upgrade never is.
A `scoped_by` job's fingerprint also carries its tenant type's `NAME` (§16), not its path:
moving a job between `global` and `scoped_by`, or to a tenant type with another `NAME`, is
a shape change, and respelling the path, to the same type or to another with the same
`NAME`, is not (§6).
A change inside a type a field holds — a new variant on an enum, a field added to a
nested struct — leaves the fingerprint unchanged, for tables and for
`#[event]`/`#[job]` payloads alike (§6, §11). Such a
change is exactly as dangerous as a field change: stored bytes either misdecode
silently or fail to decode where they are read. A payload's decode failure under a
matching fingerprint is reported as a `charpente.error` frame (§11) or a retried, then
dead-lettered job (§6), and its text names both possible causes — this one, or corrupt
bytes — because the fingerprint cannot tell them apart. Treat an edit to any type a row
or payload contains as an edit to that row or payload itself. A deep fingerprint is on
PLAN.md's after-launch list with its trigger.

### Worked example: adding a field

Start from an ordinary version-1 table:

```rust
use charpente::{Id, table};

#[table(global)]
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
#[table(version = 2, global)]
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
(`DbConfig::migration_batch_size`, at least 1, default 1000), applies `migrate` to each, and commits
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

<!-- pinned -->
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
opts that table out of drift protection. Use `#[table]`. `Job::FINGERPRINT` and
`Event::FINGERPRINT` have no such default: a hand-written `impl Job` or `impl Event`
does not compile without one. A hand-written `impl Event` also has
`Event::PAYLOAD_FINGERPRINT`, its fields' fold before any tenant is mixed in, which
defaults to `FINGERPRINT`, and every emit and subscribe site checks at build time that
`FINGERPRINT` is that fold mixed with its declared tenant's `NAME`, or the fold alone for
a global event (§11, "An event's tenancy").

## 13. describe — the machine-readable contract

`charpente describe` renders the full shape of a mounted `App` — every table (with its
version, structural fingerprint, indexes, migration chain, and whether it's tenant-scoped),
every route (method, pattern, name, whether it requires auth, the tenant scope it admits every caller to if it is declared `public(Org)`, by that tenant type's `NAME`, whether it declared `cross_tenant`, whether it's an SSE stream),
every registered event (name, payload type, and the tenant it belongs to if it is `scoped_by` — §11), every job (name, retry policy, the tenant scope it acts in if it is `scoped_by`, by its `NAME`, whether it declared `cross_tenant`), every cron entry
(name, interval), and the app's config — as one JSON (or human-readable) document. It is a pure walk of the app's own registries (DESIGN §7): no
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
use charpente::{Schema, Tx, View, Wire, route, routes, table};
use serde::Deserialize;

#[table(global)]
struct Note {
    id: Id<Note>,
    text: String,
}

#[derive(Wire, Deserialize)]
struct NoteBody {
    text: String,
}

#[route(POST "/notes/:id", public)]
fn create_note(tx: &mut Tx, id: Id<Note>, body: NoteBody) -> Result<Id<Note>, ApiError> {
    tx.put(&Note {
        id,
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
are new, for free — and so is `restore` (§15). An unrecognized verb or a bad `--format`
value exits 2 with a pinned usage message:

<!-- pinned -->
```text
usage: <app> [describe [--format json|human]]
       <app> restore --from BUCKET --prefix PREFIX [--to DIR] [--force]  — no verb serves the app
```

(`launch.rs`'s `the_guide_quotes_the_usage_text_the_binary_actually_prints` `include_str!`s
this file and asserts the block above is the constant verbatim, so the two cannot drift
apart silently again — they did once, when §15 was added.)

A status-1 exit under `launch` (non-replicated — `launch` only ever calls `serve::serve`,
never `serve_replicated`, so neither a witness conflict nor a strict-witness deadline can
happen here; those are `charpente::serve_replicated`-only stops) comes from one of three
distinct moments, each with its own line on stderr:

- **Before any kernel exists.** The data directory cannot be opened — its parent is missing
  or the node cannot read it (§1), the path is empty, or it is not a directory you can
  create: `charpente failed to start: the data directory "<path>" ...` or `charpente failed
  to start: could not fsync directory "<parent>" ...`, naming the cause and the fix. Or opening
  `/dev/urandom` (`ProdEntropy::new`) or `build` (`App::open`) fails — a table-gate barrier
  from a schema this binary cannot open, or an ordinary platform IO failure (the store, the
  log): `charpente refused to start: ...`/`charpente failed to start — fix the store/log and
  restart: ...`. Or the HTTP bind fails, which `serve` attempts
  before it spawns the kernel thread: `charpente failed to start — could not bind <addr>:
  <the OS error> — free that address, or change the one the app passes to launch`.
- **The kernel stops on its own account** — a Pending job of a foreign shape (§6), a
  migration step this binary has no registration for, or a platform IO failure — and its
  stop line names which: `charpente kernel refused to start`/`stopped on a
  refusal`/`stopped on a platform IO failure`.
- **The HTTP edge thread ends before the kernel does** (most likely a panic — v0 has no
  admin-triggered shutdown): `charpente kernel stopped: the request channel closed, ...`.

In every case the line names the fix before `launch` exits.

### `charpente describe` — the CLI verb

The CLI verb builds and runs any app crate's binary from the outside — no code changes to
the app required, not even the `launch` migration above (it works against any binary that
calls `charpente::launch`, which is what makes it a description of the app rather than
something the app has to expose over HTTP itself):

<!-- pinned -->
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
API keys; 2 registered events (`invoice_finalized` and `receipt_sent`, the two payloads
`/events/feed` delivers, in event-name order rather than registration order); 9 named
types reachable from those routes' params, request, and response schemas and from those
two event payloads (`ApiError`, `CreateCustomerBody`, `CreateInvoiceBody`,
`CustomerView`, `InvoiceFinalized`, `InvoiceView`, `LineItemInput`, `LineItemView`,
`ReceiptSent`, alphabetized in `"types"` regardless of registration order — §5's
`register_types` walk). `ApiError`'s own
schema is the fixed `{"error": "<message>"}` envelope every route's error response
actually carries — not a per-variant union of its Rust shape, and not `OidcReject`
(which `ApiError::Oidc` wraps at the Rust level but never puts on the wire, so it does
not appear in `"types"` at all; see "`ApiError` and status mapping" below); 4 jobs (3 of the
app's own plus the platform's built-in session sweep); 2 cron entries (the app's daily
overdue-check plus the platform's built-in session sweep schedule); 13 tables (7 of the
app's own plus 6 platform tables: `Session`, `ApiKey`, `CronRow`, and the three untyped
internal tables backing jobs, events, and schema versions). Every `"builtin": true`
entry above is a table, job, or cron entry the platform itself registers, not app
code — this is how an agent tells its own surface apart from the platform's without
memorizing a list:

```json
{
  "charpente_version": "0.1.0",
  "tables": [
    {
      "name": "ApiKey",
      "version": 1,
      "fingerprint": 11308777448940268015,
      "indexes": [
        "key_hash"
      ],
      "migrations": [],
      "scoped": false,
      "builtin": true
    },
    {
      "name": "AppClock",
      "version": 1,
      "fingerprint": 1422665991556096363,
      "indexes": [],
      "migrations": [],
      "scoped": false,
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
      "scoped": false,
      "builtin": true
    },
    {
      "name": "Customer",
      "version": 1,
      "fingerprint": 12205929389820576530,
      "indexes": [],
      "migrations": [],
      "scoped": false,
      "builtin": false
    },
    {
      "name": "IdCounter",
      "version": 1,
      "fingerprint": 17814272651750401892,
      "indexes": [],
      "migrations": [],
      "scoped": false,
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
      "scoped": false,
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
      "scoped": false,
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
      "scoped": false,
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
      "scoped": false,
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
      "scoped": false,
      "builtin": true
    },
    {
      "name": "charpente.events",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "scoped": false,
      "builtin": true
    },
    {
      "name": "charpente.jobs",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "scoped": false,
      "builtin": true
    },
    {
      "name": "charpente.schema_versions",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "scoped": false,
      "builtin": true
    }
  ],
  "routes": [
    {
      "method": "POST",
      "pattern": "/login",
      "name": "login",
      "auth": false,
      "public_scope": null,
      "cross_tenant": false,
      "stream": false,
      "params": [],
      "request": "String",
      "response": "String",
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "POST",
      "pattern": "/customers/:id",
      "name": "create_customer",
      "auth": true,
      "public_scope": null,
      "cross_tenant": false,
      "stream": false,
      "params": [
        {
          "name": "id",
          "schema": {
            "Id": "Customer"
          }
        }
      ],
      "request": {
        "Ref": "CreateCustomerBody"
      },
      "response": {
        "Ref": "CustomerView"
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "GET",
      "pattern": "/customers/:id",
      "name": "get_customer",
      "auth": true,
      "public_scope": null,
      "cross_tenant": false,
      "stream": false,
      "params": [
        {
          "name": "id",
          "schema": {
            "Id": "Customer"
          }
        }
      ],
      "request": null,
      "response": {
        "Ref": "CustomerView"
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "GET",
      "pattern": "/customers",
      "name": "list_customers",
      "auth": true,
      "public_scope": null,
      "cross_tenant": false,
      "stream": false,
      "params": [],
      "request": null,
      "response": {
        "Seq": {
          "Ref": "CustomerView"
        }
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "POST",
      "pattern": "/invoices/:id",
      "name": "create_invoice",
      "auth": true,
      "public_scope": null,
      "cross_tenant": false,
      "stream": false,
      "params": [
        {
          "name": "id",
          "schema": {
            "Id": "Invoice"
          }
        }
      ],
      "request": {
        "Ref": "CreateInvoiceBody"
      },
      "response": {
        "Ref": "InvoiceView"
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "GET",
      "pattern": "/invoices/:id",
      "name": "get_invoice",
      "auth": true,
      "public_scope": null,
      "cross_tenant": false,
      "stream": false,
      "params": [
        {
          "name": "id",
          "schema": {
            "Id": "Invoice"
          }
        }
      ],
      "request": null,
      "response": {
        "Ref": "InvoiceView"
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "GET",
      "pattern": "/invoices",
      "name": "list_invoices",
      "auth": true,
      "public_scope": null,
      "cross_tenant": false,
      "stream": false,
      "params": [],
      "request": null,
      "response": {
        "Seq": {
          "Ref": "InvoiceView"
        }
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "POST",
      "pattern": "/invoices/:id/finalize",
      "name": "finalize_invoice",
      "auth": true,
      "public_scope": null,
      "cross_tenant": false,
      "stream": false,
      "params": [
        {
          "name": "id",
          "schema": {
            "Id": "Invoice"
          }
        }
      ],
      "request": null,
      "response": {
        "Ref": "InvoiceView"
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "POST",
      "pattern": "/invoices/:id/record-payment",
      "name": "record_payment",
      "auth": true,
      "public_scope": null,
      "cross_tenant": false,
      "stream": false,
      "params": [
        {
          "name": "id",
          "schema": {
            "Id": "Invoice"
          }
        }
      ],
      "request": null,
      "response": {
        "Ref": "InvoiceView"
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "GET",
      "pattern": "/events/feed",
      "name": "events_feed",
      "auth": true,
      "public_scope": null,
      "cross_tenant": false,
      "stream": true,
      "params": [],
      "request": null,
      "response": "Null",
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "POST",
      "pattern": "/keys",
      "name": "create_key",
      "auth": true,
      "public_scope": null,
      "cross_tenant": false,
      "stream": false,
      "params": [],
      "request": null,
      "response": "String",
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "DELETE",
      "pattern": "/keys/:id",
      "name": "revoke_key",
      "auth": true,
      "public_scope": null,
      "cross_tenant": false,
      "stream": false,
      "params": [
        {
          "name": "id",
          "schema": {
            "Id": "ApiKey"
          }
        }
      ],
      "request": null,
      "response": "String",
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    }
  ],
  "events": [
    {
      "name": "invoice_finalized",
      "payload": "InvoiceFinalized",
      "scoped_by": null
    },
    {
      "name": "receipt_sent",
      "payload": "ReceiptSent",
      "scoped_by": null
    }
  ],
  "types": [
    {
      "name": "ApiError",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "error",
              "schema": "String",
              "optional": false
            }
          ]
        }
      }
    },
    {
      "name": "CreateCustomerBody",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "name",
              "schema": "String",
              "optional": false
            },
            {
              "name": "email",
              "schema": "String",
              "optional": false
            }
          ]
        }
      }
    },
    {
      "name": "CreateInvoiceBody",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "customer",
              "schema": {
                "Id": "Customer"
              },
              "optional": false
            },
            {
              "name": "due_day",
              "schema": {
                "Int64": {
                  "signed": false
                }
              },
              "optional": false
            },
            {
              "name": "line_items",
              "schema": {
                "Seq": {
                  "Ref": "LineItemInput"
                }
              },
              "optional": false
            }
          ]
        }
      }
    },
    {
      "name": "CustomerView",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "id",
              "schema": {
                "Id": "Customer"
              },
              "optional": false
            },
            {
              "name": "name",
              "schema": "String",
              "optional": false
            },
            {
              "name": "email",
              "schema": "String",
              "optional": false
            }
          ]
        }
      }
    },
    {
      "name": "InvoiceFinalized",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "invoice",
              "schema": {
                "Id": "Invoice"
              },
              "optional": false
            }
          ]
        }
      }
    },
    {
      "name": "InvoiceView",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "id",
              "schema": {
                "Id": "Invoice"
              },
              "optional": false
            },
            {
              "name": "customer",
              "schema": {
                "Id": "Customer"
              },
              "optional": false
            },
            {
              "name": "state",
              "schema": "String",
              "optional": false
            },
            {
              "name": "due_day",
              "schema": {
                "Int64": {
                  "signed": false
                }
              },
              "optional": false
            },
            {
              "name": "line_items",
              "schema": {
                "Seq": {
                  "Ref": "LineItemView"
                }
              },
              "optional": false
            },
            {
              "name": "total_cents",
              "schema": {
                "Int64": {
                  "signed": true
                }
              },
              "optional": false
            }
          ]
        }
      }
    },
    {
      "name": "LineItemInput",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "description",
              "schema": "String",
              "optional": false
            },
            {
              "name": "unit_amount_cents",
              "schema": {
                "Int64": {
                  "signed": true
                }
              },
              "optional": false
            },
            {
              "name": "quantity",
              "schema": {
                "Int64": {
                  "signed": true
                }
              },
              "optional": false
            }
          ]
        }
      }
    },
    {
      "name": "LineItemView",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "description",
              "schema": "String",
              "optional": false
            },
            {
              "name": "unit_amount_cents",
              "schema": {
                "Int64": {
                  "signed": true
                }
              },
              "optional": false
            },
            {
              "name": "quantity",
              "schema": {
                "Int64": {
                  "signed": true
                }
              },
              "optional": false
            }
          ]
        }
      }
    },
    {
      "name": "ReceiptSent",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "invoice",
              "schema": {
                "Id": "Invoice"
              },
              "optional": false
            },
            {
              "name": "amount_cents",
              "schema": {
                "Int64": {
                  "signed": true
                }
              },
              "optional": false
            }
          ]
        }
      }
    }
  ],
  "jobs": [
    {
      "name": "SessionSweep",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "scoped_by": null,
      "cross_tenant": false,
      "builtin": true
    },
    {
      "name": "SendReceipt",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "scoped_by": null,
      "cross_tenant": false,
      "builtin": false
    },
    {
      "name": "SendOverdueReminder",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "scoped_by": null,
      "cross_tenant": false,
      "builtin": false
    },
    {
      "name": "AdvanceDay",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "scoped_by": null,
      "cross_tenant": false,
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
the run, so it is always safe to run against a live server's data dir. Second: everything
in the contract is *declared*, never discovered by walking bodies. That shows up twice
here. `events_feed`'s entry above carries `"stream": true` and an empty `"emits": []`
because this example has not adopted `emits(...)` yet (§11, "Declaring what a stream
carries") — `Subscription::of::<Ev>()` runs inside the handler's body, where no app-wide
walk can see it, so an undeclared feed tells you nothing about which events it delivers
and reading the handler is the only way to find out. Likewise an event never passed to
`mount_events` is absent from `events` entirely: registration is what puts an event in
the contract, and it still emits and delivers either way.

## 14. The TypeScript client

`crates/charpente-tsgen` turns one mounted app's `describe` contract (§13) into a
TypeScript client: branded id types and a declaration for every wire type — an `interface`
for a struct, a `type` union for an enum — a discriminated `Result<T, E>` per route, and
per-event dispatch for every SSE route that declared `emits(...)`. Nothing here is
hand-written per app — the emitter is a pure function of the contract — so getting the
generated client right is entirely a matter of getting the WIRE right.

### Deriving `Wire`

Every type that reaches a route's params, request body, response, or error, and every
`#[event]` payload, must implement `Wire` — the trait describing what a value looks like
once serde has encoded it, not what its Rust shape merely suggests. `#[derive(Wire)]`
reads serde's OWN attributes, so the schema matches what serde actually emits, and it
rejects, by name, any attribute it has no logic for — a schema that silently disagreed
with the wire would be worse than a compile error.

Read faithfully: `rename`, `rename_all` (`camelCase`, `snake_case`, `SCREAMING_SNAKE_CASE`,
`kebab-case`), `skip`/`skip_serializing` (the field never appears in the schema at all, not
merely marked optional), `skip_serializing_if` (marks the field `optional` — see "64-bit
integers" below for what that means at an `Int64` position), and `#[serde(crate = "...")]`
(accepted and ignored: it only redirects serde_derive's own generated paths, and changes no
field, name, or shape).

Rejected, with the fix named in the message: `flatten` (hoists the inner type's fields in a
way the wire IR cannot express); `default` (changes whether a field may be absent — say
`Option<T>` or `skip_serializing_if` instead, so the wire semantics are explicit);
`skip_deserializing` (the field IS still emitted on the wire; only reads are affected);
`untagged` (an untagged value carries no discriminator for the schema to key on); a
struct-level `tag` (tagging is an enum concern, resolved on the enum itself); and the split
`rename(serialize = ..., deserialize = ...)` form (a wire schema is one shape, and
divergent serialize/deserialize names can't be represented).

```rust
use charpente::wire::{WireI64, WireU64};
use charpente::Wire;
use serde::{Deserialize, Serialize};

#[derive(Wire, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LineItemView {
    #[serde(rename = "sku")]
    item_code: String,
    unit_amount_cents: WireI64,
    quantity: WireU64,
    #[serde(skip_serializing_if = "Option::is_none")]
    note: Option<String>,
}
```

An enum derives `Wire` too, and describes whichever of serde's *tagged* representations it
actually uses — a unit-only enum as a TypeScript string union, otherwise externally
(serde's default), internally (`tag = "..."`), or adjacently (`tag` + `content`) tagged;
`untagged` is refused for the same reason it is on a struct.

### 64-bit integers: `bigint` on both sides of the wire

A bare `u64`/`i64` at any wire position is a compile error naming the fix — above 2^53 a
JSON number silently loses precision in JavaScript, so `Wire` has no impl for either type
(DESIGN §3). `WireU64`/`WireI64` are the sanctioned carriers: they serialize as decimal
strings, and the generated TypeScript type is `bigint`. Every TypeScript fragment below is
copied verbatim from `ts-fixture/src/generated/client.ts` — the committed output of
`charpente gen-client` against `crates/ts-fixture-app`'s contract — not written from
memory:

```ts
export interface LimitExceeded {
  maxCents: bigint;
}
```

**Outbound (client to server)** needs no schema knowledge: `request()`'s `JSON.stringify`
runs a replacer that converts every `bigint` it meets to its decimal string BEFORE
serialization, unconditionally — dedented here out of the conditional spread it actually
sits inside, `request()`'s own `body` option:

```ts
JSON.stringify(body, (_k, v) =>
  typeof v === "bigint" ? v.toString() : v,
)
```

— which is sound because a type-checked call site can only ever hand this a `bigint`
where the schema put one.

**Inbound (server to client)** needs a schema-directed walk instead, because `JSON.parse`
has already parsed the string into a JavaScript value by the time anything could revive
it. The one primitive every generated walker calls:

```ts
function reviveInt64(v: unknown): bigint {
  return typeof v === "string" ? BigInt(v) : (v as bigint);
}
```

A generated method wraps `this.request(...)` for a route whose response or error
transitively contains an `Int64`, and calls a per-type revive function that converts just
those positions back to `bigint`:

```ts
async getInvoice(id: InvoiceId): Promise<Result<InvoiceView, ApiError>> {
  const r = await this.request<InvoiceView, ApiError>("GET", `/invoices/${encodeURIComponent(id)}`);
  return r.ok ? { ok: true, data: reviveInvoiceView(r.data) } : { ok: false, error: r.error };
}
```

A route whose response and error carry no `Int64` anywhere keeps the plain one-line
`return this.request(...)` — nothing extra is emitted for the common case:

```ts
async createCustomer(id: CustomerId, body: CreateCustomerBody): Promise<Result<CustomerView, ApiError>> {
  return this.request<CustomerView, ApiError>("POST", `/customers/${encodeURIComponent(id)}`, body);
}
```

**Absence is two different shapes, and the emitter answers them differently.** An
`Option<WireI64>` field the server always writes a key for sends an explicit `null` when
absent, and the walker guards it the same way:

```ts
adjustmentCents: (v.adjustmentCents) == null ? null : (reviveInt64(v.adjustmentCents)),
```

A bare `WireI64` behind `#[serde(skip_serializing_if = ...)]` never writes `null` — the key
is OMITTED entirely — so its TypeScript type is `?: bigint`, never `?: bigint | undefined`
(under `exactOptionalPropertyTypes`, which `ts-fixture/tsconfig.json` turns on, those are
different declarations, and only the first is a document the server can actually send), and
the walker omits the key with a conditional spread rather than materializing it as
`undefined`:

```ts
...(v.lateFeeCents === undefined ? {} : { lateFeeCents: reviveInt64(v.lateFeeCents) }),
```

The opposite call goes the opposite way, deliberately, for an INPUT type an app author
builds rather than a wire type the server sends: every OPTIONAL member of `SubscribeOptions`
is `?: T | undefined` (`onEvent` and `onEnd` are required, no `?`), because there absence and
`undefined` mean the same thing (neither is serialized) and `?: T` alone would refuse the
ordinary pass-through
`function tail(api: Api, cursor?: string) { return api.feed({ cursor, … }); }`.

### Ending a stream: `StreamEnd`

`onEnd` is how a subscribed stream tells its caller it stopped, typed as `StreamEnd`,
which carries four kinds:

- `closed` — the ordinary end of the response body.
- `gap` (§11) — the tail fell behind retention; events the server no longer holds
  were skipped.
- `reshaped` (§11, "Changing an event") — the server met a stored event written under
  a different shape of its type and will not decode it.
- `error` (§11) — a stored event does not decode (corrupt bytes, or a change inside a
  type one of its fields holds) or does not encode as JSON. Its `detail` names the
  server-side fix (§11): for a decode failure, restore the changed type and give the new
  shape its own event type, or restore or re-seed the node; for an encode failure, fix
  the emitting code.

None of `gap`, `reshaped` or `error` may be auto-resumed from: subscribe again with no
cursor, and once its `onOpen` fires, read current state from the same node, holding
every frame `onEvent` receives until that read is installed and then applying the held
frames and every later one over it in order, idempotently (§11 says why both orders
matter). `onOpen` is optional and fires once, when the server has accepted the
subscribe and before any frame; a refused subscribe never fires it. It is not awaited,
so a frame, and even `onEnd`, can reach the caller before a read started there returns
(`client-runtime.test.ts`, "frames that beat the re-sync's read must be held and
applied over it"). `onEnd` is deliberately not a
catch-all — `CursorRefused`, `StreamRejected`, and `UndeclaredEvent` are thrown, not
passed to it, so teardown that must run on every exit belongs in `.finally()` on the
promise the generated subscribe method (`feed` in the fixture) returns.

### `#[route]`'s `emits(...)`: per-event dispatch

A stream route that declares `emits(Ping, Pong)` (§11) gets a generated
subscribe method whose runtime dispatch is keyed by a `Map` from the SSE frame's `event:`
name to that event's revive function, or `null` when its payload carries no `Int64`. Again
from `ts-fixture/src/generated/client.ts` (shown here reflowed for width; the generated
file emits the `Map` on one line):

```ts
feed(opts: SubscribeOptions<FeedEvent>): Promise<void> {
  return this.stream<FeedEvent>("/feed", opts, new Map([
    ["invoice_paid", (d: unknown) => reviveInvoicePaid(d as InvoicePaid)],
    ["invoice_voided", null],
  ]));
}
```

A frame whose name is not a key in that map raises `UndeclaredEvent` by name — skew
between this generated file and a server that has since shipped a new event — unless the
caller passed `onUnknown`, which is handed the frame untouched: un-revived, since the
client holds no schema for a payload it wasn't generated for. A route that declared no
`emits(...)` at all passes no map — not an empty one, which the runtime would read as
"refuse every name" — so every frame it sees is accepted and surfaced as the honest
`UnknownEvent` shape instead of a fabricated union:

```ts
rawFeed(opts: SubscribeOptions<UnknownEvent>): Promise<void> {
  return this.stream<UnknownEvent>("/raw-feed", opts);
}
```

### Generating and checking the client

```bash
charpente gen-client --dir ./myapp --out ./myapp-web/src/generated/client.ts
```

builds the app crate, reads its `describe` contract, and writes the client — commit the
result, the same as any other generated, checked-in file a human doesn't hand-edit. CI
runs

```bash
charpente check-client --dir ./myapp --out ./myapp-web/src/generated/client.ts
```

instead: it regenerates the client the same way and compares it against what's committed,
printing the first differing line rather than dumping a generated file into the log.
`check-client` exits (matching `charpente-cli/src/genclient.rs`'s own doc comments):

- **0** — the committed client matches this app's contract exactly.
- **1** — the committed client is stale, or was never generated at all: a Rust wire type
  or route changed without `gen-client` being re-run.
- **2** — a usage error (an unknown or valueless flag), a build or launch fault (the app
  crate would not build, or its binary would not run), or a contract this CLI's own
  version cannot parse.
- **101** — `charpente_tsgen::emit_client` panicked on a contract it refuses to
  represent — a duplicate identifier, a wire type colliding with a runtime declaration, an
  unsubstitutable `:param` — Rust's own default panic exit code, naming the offending Rust
  route or type and the fix (hard rules 4 and 5) rather than a `Result` this CLI would have
  to invent a code for.

`gen-client` shares 0, 2, and 101 with `check-client` — its own 2 additionally covers a
failure to write `--out` — but has no code **1**: there is nothing for it to compare
against, so it has no failure verdict to report. The 1-vs-2 split on `check-client` is the
whole point of running it in CI: 1 says the committed client does not match this app's
contract; 2 says nothing could even be built to compare it against, which is a different
thing to fix.

A type-check alone is half a gate: `tsc --noEmit` proves a call site type-checks against
the generated types, never that a runtime value actually matches its declared type — which
is exactly how an `Int64` shipping as a plain string past a `bigint` declaration got
through a green gate before this sub-project's own executing suite existed. `ts-fixture/`'s
Node-native tests run the generated client against real `serde_json` documents through a
stubbed `fetch`, which is what catches that class of bug; an app adopting this workflow
wants both the type-check and the executing suite, not the type-check alone.

**Transport is SSE only.** The generated `stream()` method is `fetch`-based (not
`EventSource`, which cannot set an `Authorization` header) and no WebSocket client ships
anywhere in this tree — see PLAN.md's M2 list for what a transport swap behind the same
generated API would still owe.

## 15. The archive

The archive ships a bucket copy of the database, one **generation** at a time. A generation
is a full logical snapshot plus the sealed WAL segments that continue it, written under a
key prefix the archive owns; `<prefix>/manifest` is the pointer to the newest one, and
writing that manifest is the commit point. Nothing is readable until a manifest names it,
and nothing is deleted until the *next* manifest is durable.

Three things it is not. It is not `serve::backup`, which is a promotable hot standby holding
its own data directory — the archive holds objects, and getting a node back from them means
running `restore`. It is not incremental at the object level: every generation carries a
whole snapshot, so the snapshot cadence is what bounds restore time. And it is not a
background thread — hard rule 1 forbids the core spawning one, and an upload schedule off a
thread would not replay. `Archive::step` is a kernel arm doing one unit of work per call
(start one upload, poll one in flight, roll one generation), so archive work is interleaved
with requests, jobs and cron in the one kernel loop rather than racing them.

### Turning it on

`serve::archive` runs an archiving node on the current thread: no HTTP edge, no replication,
exactly the shape `serve::backup` has.

```rust
use charpente::archive::ArchiveConfig;
use charpente::http::App;
use charpente::prod::FsObjectStore;
use charpente::serve;

fn run_archive(app: App) -> std::io::Result<()> {
    let cfg = ArchiveConfig {
        // The key prefix this archive owns inside the bucket.
        prefix: "node-a".to_string(),
        // This node's identity, recorded in every manifest it writes.
        // A stability contract, not a label — see below.
        writer: "node-a".to_string(),
        // Roll a new generation every 100_000 committed records.
        snapshot_every: 100_000,
        // Sealed WAL segments this archive may pin before it yields.
        pin_segments: 8,
        // Payload budget for one segment-shipping step.
        read_budget_bytes: 8 << 20,
    };
    // Blocks: the kernel runs on this thread until it returns.
    serve::archive(app, Box::new(FsObjectStore::new("/mnt/archive")?), cfg)
}
```

`ArchiveConfig` has no `Default`: every field is a deployment decision, and two of them
behave in ways worth reading before you pick a value.

- **`writer` must be unique per node and stable across a redeploy.** Ownership of a prefix
  is decided by string equality on this one field, checked when the archive first reads the
  bucket and again before every manifest swap. Two nodes sharing a `writer` each recognise
  the other's manifest as their own; one node whose `writer` *changes* refuses permanently
  on a bucket it genuinely owns. So do not derive it from anything a redeploy moves — a bind
  address, a port, a pod name, a container id, an ephemeral IP. A port change would become a
  permanent archive outage.
- **`snapshot_every` counts committed records, never seconds.** A time-driven cadence would
  put a wall clock inside the archive's decisions and make replay depend on it. It also
  gates the *first* upload: the archive writes nothing at all until that many records have
  committed, because a manifest with no snapshot behind it names a generation that does not
  exist. `0` means "no cadence configured" — snapshot as soon as there is a frontier to name,
  and never roll on a record count.

`pin_segments` is the archive's retention-pin bound, in sealed WAL segments. While the
archive is behind, it holds the log above the segment it still needs; past this many, it
releases and lets `Wal::gc` proceed. That is the direction the trade always runs — **on an
object-store outage the archive yields, never the primary** — and it is why the number is
not a correctness knob: too small costs wasted uploads and gap churn, too large costs held
WAL on disk. Size it for the longer of the two waits it covers: an object-store outage you
want to ride out, and the time one full snapshot takes to upload, during which no segment
ships at all. `read_budget_bytes` bounds the payload one segment-shipping step reads
(8 MiB against the 64 MiB production segment size is eight steps per segment); it exists so
one kernel step is never a 64 MiB read.

Two things to know before you deploy this:

**`FsObjectStore` is the only production `ObjectStore` that ships.** It writes objects as
files under a root directory, atomically (temp file, sync, rename, then sync every directory
from the object's up to the root's parent), and it syncs an object and those directories
before a GET or LIST returns it. Point the root at a directory whose parent exists and the
node can read: a PUT creates the root but never its parent, so `FsObjectStore::new` refuses
by name, when the store is built, a root whose parent is not an existing directory (and an
empty path) — the `?` above, which stops the node before it starts. Provision the parent as
you would the data directory's (§1), durably. A PUT still refuses the root by name if the
parent is removed while the node runs, and under `serve::archive` that refusal is retried like
any failed PUT. A real bucket — S3, GCS,
Azure Blob — needs an `ObjectStore` implementation you write against the trait in
`charpente::core::io`; there is no cloud client in the tree. Read `FsObjectStore` first:
whole-object PUTs give a caller nothing to barrier, so the trait has no `sync` method, and
every implementation still owes the trait's contract — an object a GET or LIST returns is
durable, and an acknowledged PUT is durable — silently, because nothing in the trait can ask
for it.

**`serve::archive` is not a sidecar.** There is no `Repl` in it, so the node it runs is a
primary in every respect except its edges: it runs the app's jobs and cron while it
archives, and it will accept writes from anything that reaches its `Db`. It is "a primary
whose only edge is the archive", not "an archiver attached to someone else's database". The
deployment you probably want instead — archiving *from* a replication standby, leaving the
primary's loop untouched — is not constructible yet: `Kernel` takes replication and an
archive together only through a private constructor, and every public one passes at least one
of them `None`.

### The notices an archive raises

An archive failure is never a database failure. The kernel arm absorbs every error `step`
returns — a `?` there would kill a serving node over a transient PUT — and the archive
retries the same key with the same bytes, so absorbing loses nothing. A PUT that fails
inside the pin bound therefore produces **no notice at all**, on purpose: it is the expected
shape of an object store having a bad minute.

Three conditions do print, on stderr, prefixed `charpente archive:`. Under `serve::archive`
that is the entire operator surface — the kernel owns the `Archive`, and there is no handle
to poll — so this is what to watch.

**A gap.** The archive fell more than `pin_segments` behind, released its pin so `Wal::gc`
could proceed, and abandoned the generation it was building. The notice names the bound, how
many sealed segments were un-archived, the prefix and the generation, and both fixes: raise
`pin_segments` if the WAL can afford to hold more, or accept the new generation and the gap
it leaves. It is raised **once, on the edge into the gap**, never once per step, so the churn
a long outage produces collapses to a single line. What a gap costs is continuity: whatever
the log reclaims while the pin is released is in no archived generation at all, and the next
successful contact opens a fresh generation with a full snapshot rather than resuming the old
one. Recoverable, and the archive recovers on its own.

**A foreign manifest, or one that will not decode.** Either way the archive releases its pin
and **stops permanently** — `stopped()` is terminal, and because the archive is an arm of the
same single process, recovering it costs a *database* restart; there is no in-process
re-open. A foreign manifest is one naming a different `writer`, and the notice gives both
identities and both generations. An unreadable one wraps the decode failure — a CRC mismatch,
a truncated object and a malformed body call for different operator actions, and only the
decoder can tell them apart — and refuses rather than opening a fresh generation over objects
it cannot prove are its own. Both name the manifest key, the prefix and the fix, and both say
what is in the bucket conditionally on which check site raised them: at open this archive has
uploaded nothing and the generation the manifest names is the live, untouched one, while at a
swap its own objects are already there and the "remove that manifest object" remedy abandons
them. Neither retries — two writers on one prefix is a configuration fact no amount of
retrying reaches, and a silent forever-retry is how an archive stops shipping without ever
saying so.

**Reclamation refusing a retired generation's snapshot.** A superseded generation's
`snapshot/meta` claimed more batch objects than the archive will enumerate out of one
generation. Nothing is lost and nothing stops — the retired chain is still reclaimed, that
generation's snapshot objects stay in the bucket, and the archive advances — but the claim is
evidence about who is writing the prefix, so the notice names the bound, the claim, the key,
the prefix and the generation.

### Restoring from an archive

An archive is worth exactly what can be read back from it. `restore` is the reading-back:
it rebuilds a data directory from a bucket an archive wrote, to the newest recoverable
point in it.

Every app binary that calls `charpente::launch` has the verb already:

<!-- pinned -->
```text
usage: <app> restore --from BUCKET --prefix PREFIX [--to DIR] [--force]
       --from BUCKET    the archive's root directory (the bucket an archive wrote)
       --prefix PREFIX  the key prefix that archive owned — ArchiveConfig::prefix, verbatim
       --to DIR         the data directory to rebuild (default: this app's own data directory; its
                        parent must exist)
       --force          discard a destination timeline the archive cannot prove is its own
```

and the CLI runs it against any app crate from the outside, exactly as `charpente
describe` does:

```bash
charpente restore --dir ./myapp --from /mnt/archive --prefix node-a --to ./data
```

`--dir` defaults to the current directory. The CLI runs `cargo build` there, picks the
binary, and execs it with `restore …`. It has to go through the app's own binary rather
than doing the work itself: restoring a table applies its rows through that table's typed
ops, so the restore needs the app's `#[table]` types, and the CLI does not have them.

A successful restore prints what it rebuilt and exits 0:

```text
restored generation 7 into "./data"
  frontier      seq 41203
  segments      12
  pending jobs  4 — these WILL dispatch when this node starts (unless one is of a job type
  this binary mounts, enqueued under another shape, which refuses the start by name — GUIDE
  §6), so restore can re-fire mail and third-party calls that already ran (idempotency keys
  are what make that exactly-once-observable)
```

### Stop the node first

Restore discards the destination's store file and its whole `wal/` directory before it
writes anything, and **nothing detects a process still serving from them** — there is no
lock, and `restore` cannot tell a live data directory from a dead one. Stop the node, then
restore.

### Restoring in place needs `--force`, and `--force` destroys records

Read this before you need it.

**Restore replaces a data directory. It never merges into one.** Into an empty or absent
directory that is all there is to it. But the second time you restore into a directory a
node has *run on* — the ordinary shape of "restore, bring it up, something is still wrong,
restore again" — the destination now holds records **above** the archive's frontier, and
those records exist nowhere else. Restore refuses:

```text
error: restore: the destination is ahead of the archive: the two agree through seq 41203,
but the destination's frontier is 41255. Generation 7's frontier is 41203. Discarding it
loses the 52 record(s) above 41203, which exist nowhere else. Refusing rather than
overwriting it — restore never merges timelines, it replaces or refuses (spec §8). Re-run
with --force to discard the destination's timeline and replace it with generation 7.
Nothing in the destination has been touched
```

Exit code 1, and — as the last sentence says — the destination is exactly as it was.

`--force` is the way through, and it is the *normal* way through for an in-place
re-restore, not an exotic flag. It says what it is about to destroy before it acts:

```text
--force: about to discard the destination's timeline. the destination is ahead of the
archive: … Discarding it loses the 52 record(s) above 41203, which exist nowhere else.
This is irreversible and nothing here is archived; it happens before any restore write.
```

The same refusal covers two other cases: a **diverged** destination (the two disagree on
the checksum of a record they both hold — two timelines, not one) and an **unrelated**
one (there is no seq at which they can be compared at all). Neither is proven divergence
and neither is silently overwritten.

### The two things a restored node does that a fresh one does not

**It comes back with its job queue.** `charpente.jobs` is a table, so pending, due and
retrying rows ride the archive like any other state, and a restored node will dispatch
them. That means restore can re-fire emails and third-party calls that already ran before
the loss. This is correct — they are durable transactional state — and idempotency keys
(`enqueue_keyed`, §6) are exactly what make the re-fire exactly-once-*observable*. But it
is a consequence of running the command, which is why the command prints the count.

**A restore that fails part-way leaves a partial rebuild, not the old directory.** Reading
the bucket, verifying every segment's frames, and judging the destination all happen
before anything is written, and failures in that phase promise an untouched destination —
including the directory itself: if `--to` did not exist, the CLI removes it again rather
than leaving you a directory you never had. Precisely: **the `--to` directory itself, and
only while it is still empty.** A nested `--to a/b/c` leaves `a` and `a/b` behind, because
deleting ancestor directories you may have meant to keep is worse than leaving two empty
ones; and a directory holding a partial restore is never removed, since the failure text
below tells you to re-run against exactly that directory.
Past that point the old timeline is already discarded, and a failure says so:

```text
… The destination has been discarded and now holds a PARTIAL restore: its own timeline was
replaced before this failed, so what is there is neither the database it was nor the one
the archive holds. Do not start a node on it — it will open and serve stale or empty
state. Re-run restore against the same directory once the cause above is fixed; restore
discards and rebuilds from scratch, so a re-run converges rather than resuming
```

Take that sentence literally: a half-restored directory *opens*. Nothing stops a node
starting on one and serving stale reads. The remedy is the re-run, which converges because
restore discards and rebuilds rather than resuming.

### Required: a lifecycle rule on the archive's bucket

**This is a requirement on whoever runs the archive, not a tuning note.** Configure the
archive's bucket with a rule that **aborts incomplete multipart uploads after N days**:

```text
S3:                 lifecycle rule -> AbortIncompleteMultipartUpload, DaysAfterInitiation: N
GCS:                lifecycle rule -> AbortIncompleteMultipartUpload, age N
Azure Blob:         nothing to configure — uncommitted blocks are garbage-collected
                    automatically, seven days after the last write to them
```

Pick N larger than the longest upload the archive will ever make — a full snapshot of your
largest table — and small enough that a few of them are not a bill. Seven days is the
common default and is fine for most deployments. Azure has no N to pick: its seven days
are fixed, which is only a problem if one upload could run longer than that.

**What happens without it.** An upload that dies part-way (the process is killed, the
network drops, the node fails over) can leave the bucket holding parts nothing will ever
finish. Those parts are unbounded and **nothing in charpente can reach them** — not the
archive, which never lists the bucket; not `restore`, which reads only what a manifest
names; and not reclamation, which deletes only what a manifest *did* name. The bucket grows
for as long as the deployment lives.

**One thing the rule does not cover, said plainly.** An archive that abandons a generation
before writing its manifest — a gap past the pin bound, or a permanent ownership refusal —
leaves behind whole, completed objects for a generation nothing will ever name. Those are
not incomplete uploads, so the lifecycle rule above does not reach them, and neither does
anything else: they are orphaned for the same reason, in that no manifest recorded them.
They are bounded by how often those events happen rather than by a mechanism, and both
print an operator notice when they do. If you see those notices repeatedly, the bucket is
accumulating and the cause is worth fixing at the source.

**And a third class, which prints nothing.** When a generation rolls, the archive deletes the
superseded generation's objects one per step. If that drain is interrupted — a gap, a restart,
the process dying, or the retired generation's snapshot metadata failing to read — whatever it
had not reached yet stays in the bucket for good. The next roll reclaims the generation it just
superseded and never returns to an older one. Unlike the two above, **this leaves no operator
notice**: every cause of it is an ordinary event that reports itself already, and the leftover
objects are a silent side effect rather than a failure. In a healthy deployment it is the most
common source of orphans. Nothing in charpente can reach them, for the same reason as the rest:
no manifest names them.

**Why no code closes this.** An object left by a failed upload is orphaned *precisely in
that no manifest ever recorded it*. Nothing but a LIST of the prefix could find one, and a
periodic list-and-sweep is deliberately not built: a sweep whose arithmetic is wrong deletes
objects a restore needs, which is a strictly worse failure than a bucket that grows. The
lifecycle rule is the mechanism designed for exactly this, so it is where this belongs.

**What the archive does reclaim, so you do not double up.** When a generation rolls, the
archive deletes the superseded generation's objects **by name**, out of its own manifest and
its own snapshot metadata. That happens after the new manifest is durable, never before.
Do **not** add an age-based expiry over the generation objects themselves to "help": age has
nothing to do with supersession, and a rule aggressive enough to reclaim a superseded
generation will eventually delete a live one that a restore needs.

## 16. Tenant scoping

Multi-tenancy here is a property of the *types*, not a rule everyone has to
remember. A scoped table's key is the `(scope, id)` composite and every one of
its secondary-index entries carries the same prefix, so a tenant is a key
prefix rather than a `where` clause; and the only way a handler or a job can
reach one of those rows is a handle that the app's own membership check — or a
declaration: the route's own `public(Org)`, or the `cross_tenant` fan-out that
enqueued the job — had to hand back. A job's handle is its enqueuer's because no
app code can write the job row that carries its tenant, and an enqueue inside a tenant of
another scope type is refused (§6). A scoped read attempted on a bare `Tx` is a compile error, not a 500
and not a leak. (What that guarantee does *not* cover — an operator holding the
`Db` — is "Where the guarantee stops" below, and it is a boundary, not an oversight.
The platform's own job queue and event log, which hold every tenant's rows, are inside
it: no app code reads them through a transaction.)

The shape of an app that uses it:

- one **scope type** (`Org`, `Workspace`, `Tenant` — your name), with one
  `impl Tenant` carrying the membership check;
- `#[table(scoped_by = Org)]` on every table that belongs to a tenant,
  `#[table(global)]` on the scope type and every table that belongs to
  none — a bare `#[table]` does not compile;
- exactly one line per handler, `let tx = tx.scope::<Org>(org, ctx)?;`, after
  which everything reads and writes inside that tenant.
- each route that opens a scope says who may: `auth` or `public`, where the
  membership check decides for each principal, or `public(Org)`, where every
  caller may by declaration (§5, "A route's stance").
- `#[job(scoped_by = Org)]` on every job that acts inside one tenant, and
  `#[job(global)]` on every job that belongs to none. A scoped job's `run`
  receives the `ScopedTx` of the tenant it was enqueued from.
- `#[event(scoped_by = Org)]` on every event that belongs to one tenant, and `#[event(global)]` on every event that belongs to none. A scoped event is emitted on its tenant's `ScopedTx`, and only that tenant's feed, built on the same handle, receives it (§11).

### Declaring a scope and a scoped table

```rust
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::wire::WireU64;
use charpente::{Id, Scoped, Tenant, Tx, route, table};

/// The scope type. It owes two things: its `NAME` and `authorize`. It is a `#[table]`
/// here only so `Id<Org>` can appear as a route path parameter (`Id<T>`'s wire
/// impl is bounded on `T: Row`); a scope that never crosses the wire can be a
/// bare marker struct instead.
#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    /// Its identity: persisted inside every scoped job's fingerprint and
    /// printed wherever a scope is named. Renaming it is a change of
    /// identity, like renaming a table.
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;

    /// Runs once per credentialed `tx.scope::<Org>(…)` on an `auth` or plain
    /// `public` route — never on one declared `public(Org)` — and it is the
    /// app's own policy: the platform never derives a tenant from a subject,
    /// a header or a row field. It is only ever asked about a real
    /// principal: a caller with no credential gets §5's 401, and a
    /// `public(Org)` route admits without asking.
    fn authorize(_tx: &Tx, who: &Principal, scope: Id<Org>) -> Result<(), ApiError> {
        if who.subject() == format!("org:{}", scope.raw()) {
            Ok(())
        } else {
            Err(ApiError::BadRequest(format!(
                "not a member of org {}",
                scope.raw()
            )))
        }
    }
}

#[table(scoped_by = Org)]
struct Memo {
    id: Id<Memo>,
    #[index]
    author: String,
    text: String,
}

#[route(GET "/memos/:org", auth)]
fn count_memos(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    // The one auditable line. `Org::authorize` runs inside it; its `Err` is
    // this handler's `Err`, mapped like any other `ApiError`.
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(WireU64(tx.scan::<Memo>()?.len() as u64))
}

#[route(GET "/memos/:org/by/:author", auth)]
fn memos_by_author(
    tx: &mut Tx,
    org: Id<Org>,
    author: String,
    ctx: &Ctx,
) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    // A scoped table's generated `by_<field>` takes the `ScopedTx`, not a
    // `View` — there is no tenant-free way to ask.
    Ok(WireU64(Memo::by_author(&tx, &author)?.len() as u64))
}
```

Four things that fall out of that, worth stating plainly:

- **`scoped_by` without an `impl Tenant` does not compile**, at the struct.
- **A tenant type names itself.** `const NAME` is its identity, as `TABLE` is a table's. Everything that names a scope prints it: the stage refusal (§6), the 401 (§5), `describe` (§13). The 401 and `JobScope::open`'s assert (§6) print the type's `std::any::type_name` beside it, so a reader can find the type; that text is never compared. A scoped job's and a scoped event's fingerprint mixes it (§6, §11), so moving a job or an event to a tenant type of another `NAME` is a shape change, and renaming a `NAME` is one for every scoped job and event of that type. The convention is the type's own identifier. Two types in one app whose identifiers are alike (`billing::Org` and `crm::Org`) need two NAMEs, and the app refuses to mount them under one (below). A generic tenant impl gives every instantiation one NAME.
- **The scope type is never inspected.** `authorize` is the only hook, and
  what the platform records of a scope is its raw id, beside a scoped job or
  event (below). Nothing parses a key back into a tenant.
- **Ids stay globally unique and app-minted.** The key prefix is what separates
  tenants, not id re-use, so two tenants may hold the same `Id<Memo>` and the
  same `#[index]` value without colliding.

**One `NAME`, one tenant type.** The app refuses to mount two tenant types under one
`NAME`, or a tenant type whose `NAME` is empty, before it serves. It collects every tenant
type it names: its `#[table(scoped_by = …)]` tables at `App::open` (a hand-written scoped
`Row` is collected only if it states `SCOPE_NAME`; by default it carries no `NAME`), its
`public(S)` routes at `mount`, its scoped jobs at `mount_jobs`, and its scoped events at `mount_events`. `<name>` is the shared `NAME`; `<type>` and `<other type>` are
the two types as `std::any::type_name` renders them, the one named first first:

<!-- pinned -->
```text
two tenant types are named "<name>": <type> and <other type> — a tenant's NAME is its identity (it is inside every scoped job's and event's fingerprint), so give each type its own `const NAME` in its `impl Tenant`
tenant type <type> has an empty NAME — give its `impl Tenant` a `const NAME` that names it
two registrations of the tenant type <type> (NAME "<name>") are different types — a hand-built `JobDef` or `EventDef` whose `scope_type` and `scope_type_name` name different types (build it from its `#[job]`'s or `#[event]`'s `def()`, or set both fields from the type its `scoped_by` names), or two versions of one crate in the build (unify them)
```

The usual cause is two types spelled alike in two modules, such as `billing::Org` and
`crm::Org`: both follow the convention, so both claim `"Org"`. The other is a generic tenant
impl mounted at two instantiations, such as `Wrap<Alpha>` and `Wrap<Gamma>`, whose one impl
gives both one `NAME`. The same type named through a table, a route, a job and an event is
one tenant type, and mounts. The third line is the same type under one `NAME` with two `TypeId`s: not two
tenant types, but a hand-built `JobDef` or `EventDef` whose `scope_type` disagrees with its
`scope_type_name`, or two versions of one crate. A hand-built `EventDef` whose `scoped_by`
and `scope_type` disagree, a tenant in one and none in the other, is refused before it
reaches the census:

<!-- pinned -->
```text
contract: event "<name>" was mounted with `scoped_by: <scoped_by>` and `scope_type` <type> — an `EventDef` names its tenant in both, or, for a global event, in neither (`Unscoped`), so this hand-built def would <consequence>; fix: build it from its `#[event]`'s `def()`, or set `scoped_by`, `scope_type` and `scope_type_name` from the one type its `type Scope` names
```

### Who reaches a tenant: membership or declaration

A tenant is entered one of two ways, and never on an anonymous caller's word:

- **By membership.** On an `auth` or `public` route, `tx.scope` runs your `authorize`
  about the caller's principal. A caller with no credential gets the 401 from §5, not a
  call to `authorize` with nobody in it. Nor can a request borrow another's word:
  `Principal` is not `Clone` (§5), so no principal outlives its request, and the only one a
  handler can pass to `tx.open_scope` is its own caller's.
- **By declaration.** `public(Org)` on the route says every caller may act inside the org
  the handler opens. `tx.scope::<Org>` then admits without asking. The operator path in
  "Where the guarantee stops" below is the other declared way in.

```rust
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::wire::WireU64;
use charpente::{Id, Scoped, Tenant, Tx, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, who: &Principal, scope: Id<Org>) -> Result<(), ApiError> {
        if who.subject() == format!("operator:{}", scope.raw()) {
            Ok(())
        } else {
            Err(ApiError::BadRequest(format!(
                "not an operator of org {}",
                scope.raw()
            )))
        }
    }
}

#[table(scoped_by = Org)]
struct Incident {
    id: Id<Incident>,
    title: String,
}

/// Operators only: `Org::authorize` runs once, about the caller.
#[route(POST "/orgs/:org/incidents/:id", auth)]
fn open_incident(
    tx: &mut Tx,
    org: Id<Org>,
    id: Id<Incident>,
    title: String,
    ctx: &Ctx,
) -> Result<WireU64, ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.put(&Incident { id, title });
    Ok(WireU64(id.raw()))
}

/// The public status page: every caller, with a credential or without,
/// reads inside the org it names, and no `authorize` runs.
#[route(GET "/orgs/:org/status", public(Org))]
fn status_page(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(WireU64(tx.scan::<Incident>()?.len() as u64))
}
```

Flip `open_incident` to plain `public` and it still refuses every anonymous write: the
scope line is rule 3, whatever `authorize` would have said about nobody.

### What a `ScopedTx` gives you

```rust,ignore
impl<S> ScopedTx<'_, '_, S> {
    pub fn scope(&self) -> Id<S>;
    pub fn get<R: Row<Scope = S>>(&self, id: Id<R>) -> io::Result<Option<R>>;
    pub fn scan<R: Row<Scope = S>>(&self) -> io::Result<Vec<R>>;
    pub fn scan_by<R: Row<Scope = S>>(&self, index_name: &str, key: &impl Codec) -> io::Result<Vec<R>>;
    pub fn scan_from<R: Row<Scope = S>>(&self, from_id: u64, limit: usize) -> io::Result<Vec<R>>;
    pub fn put<R: Row<Scope = S>>(&mut self, row: &R);
    pub fn delete<R: Row<Scope = S>>(&mut self, id: Id<R>);
    pub fn emit<Ev: Event<Scope = S>>(&mut self, event: Ev);                          // records this tenant — §11
    pub fn emit_keyed<Ev: Event<Scope = S>>(&mut self, key: impl Into<String>, event: Ev);
}
```

`use charpente::Subscribe;` adds `tx.subscription::<Ev>()`, the tenant feed (§11).

Every one of those reads and writes is bounded to the tenant the handle names.
The type parameter travels with it: a `ScopedTx<'_, '_, OrgA>` is not a
`ScopedTx<'_, '_, OrgB>`, so a helper written for one scope type cannot be
handed another's transaction.

**Global tables stay reachable.** `ScopedTx` derefs to `Tx`, so a handler that
opened a scope still reads a global table — through an explicit deref, because
Rust does not fall through the deref when an inherent method of the same name
exists but its bound fails:

```rust,ignore
let tx = tx.scope::<Org>(org, ctx)?;
tx.scan::<Memo>()?;          // scoped:   Memo is scoped_by = Org
(*tx).scan::<Region>()?;     // global: Region has no tenant
(*tx).emit(SystemNotice {}); // an #[event(global)]: it belongs to no tenant
```

**One scope per transaction.** Opening a *second, different* tenant on the same
transaction is a contract violation and panics by name — including through the
deref — rather than quietly performing a cross-tenant write:

```text
contract: a transaction opens ONE tenant scope (spec §8) — this one already opened `Org` 1 (myapp::Org) and is now asked for `Org` 2 (myapp::Org); run the second tenant's work in its own transaction
```

(Each tenant prints as its type's `NAME` and its id, then its type as `std::any::type_name` renders it, module path
included.)

Re-opening the *same* scope is a no-op. Cross-tenant *writes* in one
transaction are deliberately not offered; a rollup that must touch two tenants
runs two transactions.

### The compile errors, so you recognize them

Each is rendered below with rustc's own `help:` chains trimmed away, leaving
the message line and the `note:` that carries the fix. Each is pinned
byte-for-byte by the compile-fail matrix in
`crates/charpente-macros/tests/compile_fail/`.

A scoped read on a bare `Tx` — the error the whole feature exists to produce:

```text
error[E0277]: `Invoice` is a tenant-scoped table (`#[table(scoped_by = Org)]`) and a bare `Tx` names no tenant
  --> src/routes.rs:31:18
   |
31 |     Ok(tx.scan::<Invoice>()?)
   |           ----   ^^^^^^^ `Invoice` is scoped by `Org`
   |
   = note: open the tenant scope first — in a handler, `Org`'s own `Tenant::authorize` runs there for a credentialed caller:
               let tx = tx.scope::<Org>(scope, ctx)?;
               tx.scan::<Invoice>()?
           in a job, write `scoped_by = Org` in place of `global` in its `#[job(…)]`, keeping its other arguments, and read through the `ScopedTx` its `run` receives
```

`scoped_by` naming a type with no membership check:

```text
error[E0277]: `Org` is used as a tenant scope but has no `Tenant` impl
 --> src/tables.rs:9:21
  |
9 | #[table(scoped_by = Org)]
  |                     ^^^ no `impl Tenant for Org`
  |
  = note: a tenant scope needs a name and the app's own membership check; add:
          impl Tenant for Org {
              const NAME: &'static str = "Org";
              type Who = Principal;
              type Error = ApiError;
              fn authorize(tx: &Tx, who: &Principal, scope: Id<Self>) -> Result<(), ApiError> { … }
          }
```

A scoped job enqueued on a bare `Tx`:

```text
error[E0277]: `NotifySubscribers` is a tenant-scoped job (`#[job(scoped_by = Org)]`) and a bare `Tx` names no tenant to run it in
  --> src/incidents.rs:49:16
   |
49 |     tx.enqueue(NotifySubscribers { incident: id.raw() });
   |        ------- ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ `NotifySubscribers` is scoped by `Org`
   |
   = note: enqueue it from inside the tenant it acts in, and it runs in that tenant:
               let mut tx = tx.scope::<Org>(scope, ctx)?;
               tx.enqueue(job);
           or fan it out from a `cross_tenant` route or job, naming the tenant:
               tx.enqueue_in::<Org>(scope, job, cap);
           cron (`App::schedule`) enqueues on a bare `Tx`, so it schedules `global` jobs only
```

A scoped event emitted on a bare `Tx`:

```text
error[E0277]: `IncidentOpened` is a tenant-scoped event (`#[event(scoped_by = Org)]`), and a bare `Tx` or a global `Subscription` names no tenant
  --> src/incidents.rs:35:13
   |
35 |     tx.emit(event);
   |        ---- ^^^^^ `IncidentOpened` is scoped by `Org`
   |
   = note: emit it and subscribe to it inside the tenant it belongs to:
               let mut tx = tx.scope::<Org>(scope, ctx)?;
               tx.emit(event); // recorded in this tenant
           and, in an SSE handler, that tenant's feed (`use charpente::Subscribe;`):
               Ok(tx.subscription::<IncidentOpened>().cursor(cursor).into())
           a job that emits it takes `scoped_by = Org` in place of `global` among its `#[job(…)]` arguments, keeping the others, and emits through the `ScopedTx` its `run` receives
```

A read of one of the platform's tables, on a bare `Tx` or through a `ScopedTx`'s deref
("Where the guarantee stops", below):

```text
error[E0277]: `JobRow` is one of the platform's tables, and a transaction does not read it
  --> src/admin.rs:12:26
   |
12 |     let jobs = tx.scan::<JobRow>()?;
   |                   ----   ^^^^^^ `JobRow` is read through `Db::snapshot`, never through a transaction
   |
   = note: the platform's tables — the job queue (`JobRow`), the event log (`EventRow`) and the migration record (`SchemaVersion`) — are the platform's own state, and the first two hold every tenant's rows, so neither a handler nor a job reads them; code that holds the `Db` reads them through a snapshot:
               db.snapshot()?.scan::<JobRow>()?
   = note: a route that shows a job's progress or an event's history reads a table of the app's own, written in the same transaction as the enqueue or the emit, or subscribes to the events
```

There is no one-line way out of any of them. The marker the message hangs on
(`NoTenant`) and the trait `View` actually bounds on (`UnscopedRow`) are both
sealed by a supertrait no other crate can name, so `impl UnscopedRow for
Invoice {}` — orphan-legal, and the obvious workaround — is itself a compile
error telling you to open the scope. The job pair, `NoJobTenant` and
`UnscopedJob`, and the event pair, `NoEventTenant` and `UnscopedEvent`, are sealed the
same way. The six scope-taking primitives underneath
(`get_in`, `scan_in`, `scan_by_in`, `scan_from_in`, `put_in`, `delete_in`) are
private to the core, so there is no back door on a bare `Tx` either. Nor is there
one through the job queue: a scoped job runs in the tenant its row carries, and
`tx.put` or `tx.delete` of a `JobRow` does not compile (§6, "Job rows are the
platform's"). So a bare `Tx` cannot move a job into another tenant, forge one
there, or cancel one there. Nor is there one through the event log: `tx.put` or
`tx.delete` of an `EventRow` does not compile either (§6), so its only rows are emits,
each recorded with the tenant it was emitted in, or with none. A tenant feed delivers only rows
recorded in its own tenant (`Subscription::admits`), and a same-named event emitted in
another tenant type at the same raw id carries that type's `NAME` in its fingerprint, so
the feed meets it as a foreign shape and never decodes it. Each emit and feed site checks
that fingerprint at build time against the tenant its bound names, so a hand-written
`impl Event` cannot borrow another type's. So no handler publishes, into a tenant's feed,
an event no emit made in that tenant (§11, "An event's tenancy: `global` or `scoped_by`",
which states the check's two limits: a payload fold computed on purpose to collide, and a
tenant type left out of the census by an event never mounted). Nor does any app code read the job queue, the event log or the migration record
through a transaction, so no handler pages through another tenant's jobs or events:
`ScopeReadableBy`, the marker that read's message hangs on, is sealed like `NoTenant`,
and `ReadableBy`, the trait `View` bounds on, has one blanket impl that an app's own impl
conflicts with.

### Realtime: one mechanism, not two

`tx.emit` on a `ScopedTx` records the scope's tenant in the event's row, apart from any
key, and a tenant feed — `tx.subscription::<Ev>()` on the handle — delivers only rows
recorded in its own tenant. A tenant's feed therefore never receives another tenant's
event or a global one, and a global subscription never receives a tenant's, for the
same reason its queries cannot cross: the app did not have to remember a second rule,
and it never spells a tenant as a string. §11 ("Emitting inside a tenant, and the tenant
feed") carries the code for both sides.

Two tenant types whose raw ids coincide no longer share events: an event names its
tenant type (`#[event(scoped_by = Org)]`), its fingerprint carries that type's
`Tenant::NAME`, and every emit and feed site checks that it does, at build time, against
the tenant its bound names (§11). Two limits are stated there: a hand-written
`PAYLOAD_FINGERPRINT` computed to collide on purpose, a bypass built on purpose in app code,
which no type check refuses, and two tenant types under one `NAME` when one is reached only
through an event the app never mounts. Mounting every event with `events![…]` closes the
second.

The declaration is what makes it one mechanism. An event declared `scoped_by = Org`
compiles only where an `Org` tenant is named: its `emit` on a `ScopedTx<Org>`, and its
feed from the same handle. So a tenant's event cannot be emitted into no tenant, a global
feed cannot name one, and a hand-written event cannot claim another tenant type's shape,
except by that computed collision. §11 shows each refusal.

### Reading across every tenant: the `cross_tenant` capability

Naming a tenant is always allowed — a point read already supplies the scope.
*Enumerating without naming one* is a declared capability. A billing rollup or
an admin console says so out loud on the route or job, and the platform hands
it the token:

```rust
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::kernel::JobError;
use charpente::wire::WireU64;
use charpente::{CrossTenant, Id, Tenant, Tx, job, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[table(scoped_by = Org)]
struct Ledger {
    id: Id<Ledger>,
    cents: u64,
}

#[route(GET "/admin/revenue", auth, cross_tenant)]
fn admin_revenue(tx: &mut Tx, cap: &CrossTenant) -> Result<WireU64, ApiError> {
    // Rows come back PAIRED WITH THEIR SCOPE.
    let rows: Vec<(Id<Org>, Ledger)> = tx.across_tenants::<Org>(cap).scan::<Ledger>()?;
    Ok(WireU64(rows.iter().map(|(_, l)| l.cents).sum()))
}

/// The job half of the same declaration: `run` takes the token after `tx`.
/// The flag composes with `retry(...)` — they share one attribute.
#[job(global, cross_tenant, retry(max = 2, backoff = "5s"))]
struct Rollup {
    day: u64,
}

impl Rollup {
    fn run(self, tx: &mut Tx, cap: &CrossTenant) -> Result<(), JobError> {
        for (org, row) in tx.across_tenants::<Org>(cap).scan::<Ledger>()? {
            let _ = (self.day, org, row.cents);
        }
        Ok(())
    }
}
```

Five properties make this a capability rather than an escape hatch:

- **You cannot take the argument without the declaration.** A handler that
  writes `cap: &CrossTenant` on a route that did not say `cross_tenant` is a
  macro error naming the contract, the site and the fix — it names the route by
  its method and path and tells you to add `cross_tenant` to that attribute,
  keeping its other flags. §9's `#[route]` list quotes it verbatim.

- **You cannot forge the token.** `CrossTenant`'s field is private and its only
  constructor hangs off `Db::begin_group`, which a handler never holds —
  `CrossTenant(())` is `error[E0423]: cannot initialize a tuple struct which
  contains private fields`.
- **You cannot keep the token.** It is tied to the group the request or job
  runs in (`CrossTenant<'g>`) and is not `Clone`. A handler is handed
  `&CrossTenant`; `cap.clone()` only copies that reference (rustc says so:
  "`CrossTenant<'_>` does not implement `Clone`, so `&CrossTenant<'_>` was
  cloned instead"), so there is no token of its own to park. Operator code
  that mints one from `Db::begin_group` cannot park it either: a `static`
  or a `thread_local!` needs `'static`, and the token lives only as long as
  the group's borrow of the `Db` —
  `error[E0521]: borrowed data escapes outside of function` — unless it leaks its
  `Db` (`Box::leak`), which is the operator's standing used on purpose.
- **The result carries the tenant.** `AllTenants::scan` yields
  `Vec<(Id<Org>, Ledger)>`, never `Vec<Ledger>`, so a cross-tenant answer cannot
  be handed to code expecting one tenant's data without a visible `.map`. The
  handle itself is read-only: there is no `put` or `delete` on it. The one thing
  a declared route or job does beyond reading is fan out:
  `tx.enqueue_in::<Org>(org, job, cap)` enqueues a `scoped_by = Org` job into the
  org it names, where it runs as if enqueued from inside (§7). So the capability
  is reads, plus fan-out enqueue, and `describe`'s `cross_tenant=true` inventory
  covers both.
- **It is enumerable.** Because the declaration lives on the route, `charpente
  describe` (§13) lists every endpoint and job that can read across tenants or
  fan out into them —
  an inventory, not a grep someone has to remember to run:

  ```text
  $ charpente describe --format human
  ...
  routes:
    GET /admin/revenue name=admin_revenue auth=true public_scope=none cross_tenant=true stream=false
      params=[] request=... response=... error=... emits=[]
    GET /memos/:org name=count_memos auth=true public_scope=none cross_tenant=false stream=false
      params=[...] request=... response=... error=... emits=[]
  ...
  jobs:
    Rollup max=2 backoff_nanos=5000000000 scoped_by=none cross_tenant=true builtin=false
  ```

  (Each route prints a second, indented line of type info, and the dump also
  carries `tables:`, `events:` and `cron:` sections — elided here.) The JSON
  form carries the same `"cross_tenant": true` on both `routes` and `jobs`,
  which is what makes "audit the cross-tenant reads" a machine check.
  The same dump carries the other two tenancy declarations beside it: a route's
  `public_scope` names the tenant a `public(Org)` route admits every caller to, and
  `none` otherwise, while a job's `scoped_by` names the tenant it acts in. So "who
  reaches a tenant without a membership check" and "what acts inside one" are
  inventories too, in JSON as the tenant type's `NAME` or `null`.

  The elided `tables:` section carries the matching signal for the table
  itself: `Ledger`'s entry reports `scoped=true` (`Org`'s own entry, and every
  `#[table(global)]`, reports `scoped=false`) — carried from the same
  `#[table(scoped_by = …)]` declaration, so an audit doesn't have to grep for
  it either.

### What happens underneath, and what it means operationally

The scope is part of the row key, so it travels the whole way down: WAL effect
records, replication frames, the archive, `charpente restore`, follower
snapshot seeding, and `migrate_schema`'s resume watermark all carry it. Three
consequences you may meet:

- **A table cannot gain a scope in place.** Adding `scoped_by` to a table that
  already holds unscoped rows is refused rather than silently misread:

  ```text
  table "Memo" is declared scoped_by but its rows are keyed unscoped: a table cannot gain a scope in place; create a new scoped table and copy rows through a job
  ```

- **A table cannot lose a scope in place, either.** Removing `scoped_by` from
  a table that already holds scope-keyed rows is refused the same way, not
  silently read back as a different tenant's `id`:

  ```text
  table "Doc" is declared without scoped_by but its rows are keyed with a tenant scope: a table cannot lose a scope in place; declare it scoped_by, or create a new unscoped table and copy rows through a job
  ```

- **A binary compiled against the wrong shape refuses the record by name.** A
  replica or a restored directory running an older binary that does not know a
  table is scoped stops instead of misreading the key:

  ```text
  record decode: table "Doc" is not tenant-scoped (Row::Scope = Unscoped) but this Put effect carries scope 1 (tag 0x05/0x06) — the record was written for a scoped shape of this table; restore from a replica or backup, or replay under the schema that wrote it. For a follower, or a primary once a follower is promoted in its place, that is a re-seed: stop the node, delete its data dir, and restart it configured as a follower of the current primary so it snapshot-seeds from it, never with a primary config: the durable role file that outranks config is deleted with the data dir, so a cleared node started as a primary comes up as a second writable primary
  ```

  Note that the replication handshake's schema digest covers a table's fields
  and version but **not** its scopedness, so a mixed-version follower is
  admitted at the handshake and stopped at the first scoped record. Upgrade
  followers before the primary starts writing a newly scoped table.

### Where the guarantee stops

**The guarantee is about the typed surface the framework hands your app.** A
route handler receives a `Tx`, and a scoped job the `ScopedTx` of the tenant it
was enqueued in; through a `Tx`, a scoped table is unreachable without
`tx.scope::<Org>(org, ctx)?`. That is the property a
reviewer checks by reading one line per handler, and the compiler enforces it.

It is **not** a sandbox around the process. Your own `main` builds the `App` it
hands to `launch`, so it holds `App::db()` — and a `Db` reaches every tenant:
`Db::snapshot()` exposes scoped and raw reads that take a scope on the caller's
word with no `authorize` on the path (replication seeding, the archive and
`restore` are built on them), and `Db::begin_group` can mint the `CrossTenant`
token, for the length of that group's borrow. Whoever holds the `Db` holds the store, with the same standing as
whoever holds the data directory: an operator, not a request.

**It covers the platform's own tables too.** The job queue (`JobRow`) holds every tenant's
job payloads and the tenant each job runs in, the event log (`EventRow`) holds every
tenant's events, and the migration record (`SchemaVersion`) is the migration gate's own
state. No app code reads or writes one through a transaction, whether a handler's or a
job's, on a bare `Tx` or through a `ScopedTx`'s deref (§6). rustc prints this for a read of any of the
three, with your row type in place of `<row>`:

<!-- pinned -->
```text
`<row>` is one of the platform's tables, and a transaction does not read it
`<row>` is read through `Db::snapshot`, never through a transaction
the platform's tables — the job queue (`JobRow`), the event log (`EventRow`) and the migration record (`SchemaVersion`) — are the platform's own state, and the first two hold every tenant's rows, so neither a handler nor a job reads them; code that holds the `Db` reads them through a snapshot:
    db.snapshot()?.scan::<<row>>()?
a route that shows a job's progress or an event's history reads a table of the app's own, written in the same transaction as the enqueue or the emit, or subscribes to the events
```

So an admin job list or an activity feed is a table of the app's own, written in the same
transaction as the enqueue or the emit and scoped like any other, or, for a live feed, a
`Subscription` (§11). Operator code reads the platform's tables through `db.snapshot()`, as
§8's oracles do, since whoever holds the `Db` holds every tenant already. The one
transaction that reads a job row is the dispatcher's own: `Tx::get_job` takes the
`TenantAdmission` only a `Group` mints, which no handler or job holds.

The read seal is on the table's name as well as its type, as the write seal is. A
hand-written `Row` whose `TABLE` is one of the three names, scoped or not, fails to build at
each read the binary can reach, on a bare `Tx`, a `ScopedTx` or an `across_tenants` scan,
reported by `cargo build` and not `cargo check`:

<!-- pinned -->
```text
this Row's TABLE is "charpente.jobs", the platform's job queue — a transaction does not read the platform's tables, because the queue holds every tenant's job payloads and the tenant each one runs in; give this table a name of its own, and read the queue through `db.snapshot()` from code that holds the `Db`
this Row's TABLE is "charpente.events", the platform's event log — a transaction does not read the platform's tables, because the log holds every tenant's events; give this table a name of its own, and read the log through `db.snapshot()` from code that holds the `Db`, or subscribe to its events
this Row's TABLE is "charpente.schema_versions", the platform's migration record — a transaction does not read the platform's tables, because the record is the migration gate's own state; give this table a name of its own, and read the record through `db.snapshot()` from code that holds the `Db`
```

**Sessions and API keys are the app's own.** `Session` and `ApiKey` (§5) are ordinary
`#[table(global)]` tables that `App::open` registers for you, and a handler reads and
writes them like any other global table. They carry no tenant data, and revoking a key is
`tx.delete::<ApiKey>(id)`, an ordinary write, by design.

**Code outside a handler never takes a scope on its own word.** A job declares
`#[job(scoped_by = Org)]` and runs in the tenant its enqueuer opened or its fan-out
named (§6). Nothing else decides which, because no app code can write the job row that
carries that tenant, and an enqueue inside a tenant of another scope type is refused.
**Operator code that must act inside one tenant** — seeding, a test oracle, an admin
step in `main` — does not take a scope on its own word either. It begins a group, mints
a `TenantAdmission` from it, and opens the tenant with `admit_scope`:

```rust
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::{Db, Id, Tenant, Tx, TxError, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        // Refuses everyone: the seed below never asks.
        Err(ApiError::Unauthorized)
    }
}

#[table(scoped_by = Org)]
struct Memo {
    id: Id<Memo>,
    text: String,
}

/// Seeds one tenant's first memo. `db.begin_group()` is the operator's
/// handle, and the admission it mints is what `admit_scope` asks for.
fn seed(db: &mut Db, org: Id<Org>) -> std::io::Result<()> {
    let mut group = db.begin_group()?;
    let admission = group.tenant_admission();
    let staged = group.stage(|tx| {
        tx.admit_scope::<Org>(org, &admission).put(&Memo {
            id: Id::from_raw(1),
            text: "welcome".to_string(),
        });
        Ok::<(), std::io::Error>(())
    });
    // `commit` on every path: a group dropped with staged writes panics.
    group.commit().map_err(|failure| failure.error)?;
    match staged {
        Ok(()) => Ok(()),
        Err(TxError::App(e)) | Err(TxError::Io(e)) => Err(e),
        Err(other) => Err(std::io::Error::other(format!("{other:?}"))),
    }
}
```

No `authorize` runs there. The operator has no principal to ask about, and already
holds the store. The admission carries the seal `CrossTenant` carries: its field is
private and `Group::tenant_admission` is its only constructor. So a handler, which holds
a `Tx` and a `Ctx` and never a `Group`, cannot mint one; `TenantAdmission(())` is
`error[E0423]`. Nor can anyone keep one: like `CrossTenant`, a `TenantAdmission`
borrows what its group borrows and is not `Clone`, so it goes out of reach with the group's
borrow of the `Db`, and a `static` or `thread_local!` refuses it — unless operator code
leaks its `Db`, which is the operator's standing used on purpose. The one-scope rule is the same contract whichever way a scope was
entered. Do not build an app-internal helper that takes a raw scope and reads through
`Db::snapshot`: that is the one shape that looks tenant-aware and is not.

### Deliberately not built

Stated so you do not go looking:

- **Nested or hierarchical scopes** (Org → Project → …). One level.
- **Cross-tenant writes in one transaction.** A transaction opens one scope.
- **Platform-derived tenancy.** There is no `Principal → Org` resolver hook;
  the tenant is named by your code at the `tx.scope` line and nowhere else.
- **Retrofitting existing tables.** Every table declares `scoped_by` or
  `global`, but no migration is forced: `global` is what a bare table
  always was, and a table cannot gain a scope in place — see the refusal
  above.
- **Tenant-local ids.** Ids stay globally unique and app-minted.
- **Permissions.** This answers *which tenant*, never *what may this principal
  do*; API-key scopes and permissions are their own future design.
