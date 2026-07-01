# charpente

**The load-bearing frame for the whole backend.**

*Une charpente* is the timber frame of a building — the one structure everything else hangs
off. Charpente is a Rust platform where one statically-linked binary *is* the entire
backend: the HTTP server, the database, the job queue, the cron scheduler, the realtime
event feed, auth, and the blob store. An application is a Rust crate that links against the
platform and compiles into it. Deployment is one binary and one data directory.

There is no Postgres to provision. No Redis to babysit. No queue service, no outbox table,
no sidecar collector, no serialization boundary between "services." One process, one commit
point, one log.

## The three guarantees

1. **Typed end-to-end.** Schema, queries, routes, jobs, events, config, and telemetry are
   all Rust types checked at compile time. A schema change that breaks a query is a compile
   error, not a 3am page.

2. **One commit point.** State changes, job enqueues, and realtime events commit in the
   same transaction. The dual-write bug class — outbox gaps, ghost webhooks, "the row
   updated but the event never fired" — does not exist here, structurally.

3. **Deterministic simulation.** `charpente test --sim` runs your *entire application* —
   HTTP, storage, queue, time, network — inside a deterministic simulator with seeded fault
   injection: torn writes, crashes at arbitrary commit points, partitions, clock skew.
   Thousands of seeded universes per night; every failure reproducible from its seed.

## What it looks like

```rust
#[table]
struct Invoice {
    id: Id<Invoice>,
    org: Id<Org>,
    amount_cents: i64,
    state: InvoiceState,
}

#[route(POST "/invoices/:id/finalize")]
fn finalize(tx: &mut Tx, id: Id<Invoice>) -> Result<Invoice, ApiError> {
    let mut inv = tx.get(id)?;                  // typed row, no SQL string
    inv.state = InvoiceState::Final;
    tx.put(&inv)?;
    tx.enqueue(SendReceipt { invoice: id })?;   // job enqueued IN the same transaction
    tx.emit(InvoiceFinalized { invoice: id })?; // realtime event, same transaction
    Ok(inv)
}
```

Note what's absent: no connection pool, no ORM session, no message-broker client, no
`await` on a network hop between your logic and your data. The transaction is a pure
function over state; external effects (the receipt email) run post-commit via the durable
queue, exactly-once-observable via idempotency keys.

## Why now

AI agents write expert-level systems code cheaply. That collapsed two assumptions this
industry was built on:

- **"A database/queue/server is a team-sized effort, so compose vendors."** It isn't
  anymore. The five-service SaaS stack was an artifact of team boundaries, not physics.
- **"Frameworks exist to save humans typing."** Agents don't need typing saved. They need
  *oracles* — fast, precise, mechanical feedback. The framework that wins the agent era is
  the one that is the best reviewer, not the best autocomplete.

Charpente is a framework designed for that world: maximally compile-time-checked, honest
about effects, deterministic by construction, and introspectable by machines. Read
[MANIFESTO.md](MANIFESTO.md) for the full argument.

## Scaling stance (read this before objecting)

One big box, replicated. Writes flow through a single ordered log applied by a
deterministic state machine; the log ships to followers for read scaling and failover.
There is no sharding story and there will not be one. A modern server executes hundreds of
thousands of transactions per second when there is no network hop between app and data —
you will not outgrow this before you can afford to build something bespoke, and pretending
otherwise is how platforms die building a worse Spanner instead of a better SQLite.

## Status

**Design phase.** Nothing runs yet. The documents are the project:

| Doc | What it is |
|---|---|
| [MANIFESTO.md](MANIFESTO.md) | Why this exists — the agent-era argument |
| [DESIGN.md](DESIGN.md) | Architecture: the deterministic core, write model, typed surface, simulation |
| [PLAN.md](PLAN.md) | Milestones (M0–M2), risks, non-goals |

## Non-goals

- Horizontal sharding / infinite scale
- SQL as the primary query interface (a read-only Postgres-wire endpoint comes later, for BI tools)
- Porting existing apps (charpente is for greenfield backends, which agents mint daily)
- Microservice interop patterns (if you want a service mesh, this is not your platform)
