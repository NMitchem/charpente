# charpente

**The load-bearing frame for the whole backend.**

*Une charpente* is the timber frame of a building — the one structure everything else hangs
off. Charpente is a Rust platform where one statically-linked binary *is* the entire
backend: the HTTP server, the database, the job queue, the cron scheduler, the realtime
event feed, and auth. An application is a Rust crate that links against the platform and
compiles into it. Deployment is one binary and one data directory.

There is no Postgres to provision. No Redis to babysit. No queue service, no outbox table,
no sidecar collector, no serialization boundary between "services." One process, one commit
point, one log.

## The three guarantees

1. **Typed end-to-end.** Schema, queries, routes, jobs, events, and wire types are all Rust
   types checked at compile time — a schema change that breaks a query is a compile error,
   not a 3am page; config is a typed struct with no startup validation yet, and typed
   telemetry with change-time checks is an M2 item that does not exist yet.

2. **One commit point.** State changes, job enqueues, and realtime events commit in the
   same transaction. The dual-write bug class — outbox gaps, ghost webhooks, "the row
   updated but the event never fired" — does not exist here, structurally.

3. **Deterministic simulation.** `charpente test --sim` runs your *entire application* —
   HTTP, storage, queue, time, network — inside a deterministic simulator with seeded fault
   injection: torn writes, crashes at arbitrary commit points, partitions, clock skew.
   Thousands of seeded universes per night; every failure reproducible from its seed.

## What it looks like

```rust
#[table(global)]
struct Invoice {
    id: Id<Invoice>,
    org: Id<Org>,
    amount_cents: i64,
    state: InvoiceState,
}

#[route(POST "/invoices/:id/finalize", public)]
fn finalize(tx: &mut Tx, id: Id<Invoice>) -> Result<Invoice, ApiError> {
    let mut inv = tx.get(id)?.ok_or(ApiError::NotFound)?; // typed row, no SQL string
    inv.state = InvoiceState::Final;
    tx.put(&inv);
    tx.enqueue(SendReceipt { invoice: id });   // job enqueued IN the same transaction
    tx.emit(InvoiceFinalized { invoice: id }); // realtime event, same transaction
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

That last sentence is the argument, not a measurement of what charpente does today.
Measured, an uncontended commit costs one durable fsync, which puts it at the device's fsync
rate — 215/sec on a dev laptop under `F_FULLFSYNC`. Group commit amortizes that fsync across
a batch once requests actually queue, which is worth a great deal on a device this slow to
sync and less on a faster one. See [docs/BENCH.md](docs/BENCH.md) for the method, the
numbers, and the conditioning that has to travel with them.

## Status

**M1 shipped (2026-07-27).** M0 built the sim-first core: a segmented
checksummed WAL, typed tables on redb, transactions with one commit point
across state/queue/events, the HTTP kernel, queue + cron, and `charpente
test --sim` — the deterministic simulator with seeded fault injection this
whole platform is staked on. A docs-only agent then built
[`examples/invoicing`](examples/invoicing) from these documents alone, with
zero framework patches, and it runs green over 10,000 seeded
fault-injection universes (master seed `0x483e230baed4922b`). The
dual-write bug class the second guarantee above claims to delete
structurally is demonstrated, caught, and reverted at
[examples/invoicing/DEMO.md](examples/invoicing/DEMO.md).

M1 made it a platform: replication with promotable followers and manual
failover, realtime SSE with exact `Last-Event-ID` resume, auth (sessions,
API keys, static-JWKS OIDC), schema migrations as log records, `charpente
describe`, and a black-box edge harness that drives stock binaries over
real TCP with `kill -9` and TCP-proxy fault injection.

**M2 closed 2026-09-13; M3 — launch — is in progress.** M2 shipped six
sub-projects, dropped one, and reclassified the rest to after launch with the
trigger that reopens each ([PLAN.md](PLAN.md)). M3 is the finish line before
this is public: a stranger installs charpente from crates.io, scaffolds an app,
and reaches a red sim with a seed in one sitting; a third docs-only agent build
passes review with zero Criticals the platform should have caught; and every
number in this file is measured or gone. The first two of M2's sub-projects — an
honest benchmark harness and primary-side group commit — have their numbers in
[docs/BENCH.md](docs/BENCH.md), measured on a dev laptop under `F_FULLFSYNC`
and explicitly not the launch claim. Read them before the scaling paragraph
above: one connection still costs one durable fsync per transaction, ~215
durable records/sec on that hardware, roughly three orders of magnitude below
the headline figure. Group commit closes the *amortization* half of that gap
and none of the device-speed half — one fsync for a whole batch, worth up to
~31x where the queue is deep enough to fill one and nothing at all at one
connection — and the bare-metal Linux numbers that would resize it do not
exist yet. The object-storage archive has since shipped, unmeasured — BENCH.md
carries nothing about it. It ships one generation at a time to a bucket: a full
logical snapshot plus the sealed WAL segments that continue it, held by a
retention pin that yields to the primary rather than growing the log until it
fills the disk, and `charpente restore` rebuilds a data directory from what is
there ([docs/GUIDE.md](docs/GUIDE.md) §15). `serve::backup` is still a different
thing — a promotable hot standby, not a snapshot shipper. A TypeScript client
generator has since shipped too ([docs/GUIDE.md](docs/GUIDE.md) §14) — SSE only, no
WebSocket transport — and so has tenant scoping, where a tenant is a typed key prefix
enforced by the compiler rather than a `where` clause anyone can forget
([docs/GUIDE.md](docs/GUIDE.md) §16). What is *not* built, so you don't have to go
looking: no blob store and no WebSockets.

## Try it

```text
cargo install charpente-cli
charpente new myapp && cd myapp
cargo clippy --all-targets -- -D warnings   # hard rule 1, in your app
charpente test --sim                        # seeded fault-injection universes
```

Then delete the `tx.enqueue` line in `src/lib.rs` and run the sim again: the second
property goes red with a seed, and the seed replays. That is the whole pitch.

| Doc | What it is |
|---|---|
| [MANIFESTO.md](MANIFESTO.md) | Why this exists — the agent-era argument |
| [DESIGN.md](DESIGN.md) | Architecture: the deterministic core, write model, typed surface, simulation |
| [PLAN.md](PLAN.md) | Milestones (M0–M3), what waits until after launch, risks, non-goals |
| [docs/GUIDE.md](docs/GUIDE.md) | How to build an app — agent-first, every runnable example compile-gated |
| [docs/BENCH.md](docs/BENCH.md) | Measured numbers, method first — dev hardware, honestly labeled as not the launch claim |
| [examples/invoicing](examples/invoicing) | The M0 demo: agent-built against the docs alone; DEMO.md + FINDINGS.md |

## Non-goals

- Horizontal sharding / infinite scale
- SQL as the primary query interface (a read-only Postgres-wire endpoint comes later, for BI tools)
- Porting existing apps (charpente is for greenfield backends, which agents mint daily)
- Microservice interop patterns (if you want a service mesh, this is not your platform)
