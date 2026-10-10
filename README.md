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
   not a 3am page; config is a typed struct with no startup validation yet.

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
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(scoped_by = Org)]
struct Invoice {
    id: Id<Invoice>,
    amount_cents: i64,
    state: InvoiceState,
}

#[job(scoped_by = Org)]
struct SendReceipt {
    invoice: Id<Invoice>,
}

#[event(scoped_by = Org)]
struct InvoiceFinalized {
    invoice: Id<Invoice>,
}

#[route(POST "/orgs/:org/invoices/:id/finalize", auth)]
fn finalize(tx: &mut Tx, org: Id<Org>, id: Id<Invoice>, ctx: &Ctx) -> Result<Invoice, ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;                // the one auditable line
    let mut inv = tx.get(id)?.ok_or(ApiError::NotFound)?;   // typed row, no SQL string
    inv.state = InvoiceState::Final;
    tx.put(&inv);
    tx.enqueue(SendReceipt { invoice: id });   // job enqueued IN the same transaction; it runs in this org
    tx.emit(InvoiceFinalized { invoice: id }); // realtime event, same transaction; only this org's feed receives it
    Ok(inv)
}
```

Note what's absent: no connection pool, no ORM session, no message-broker client, no
`await` on a network hop between your logic and your data. The transaction is a pure
function over state; external effects (the receipt email) run post-commit via the durable
queue, exactly-once-observable via idempotency keys. Note also what's declared: the route
says `auth`, the table says which tenant owns its rows, the job says it acts inside that
tenant, the event says which tenant's feed carries it, and a route, table, job or event
that says none of this does not compile.

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
There is no sharding story and there will not be one. With no network hop between app and
data, one box's ceiling is its disk's durable write rate, not a network round trip. What
charpente measures today is 215 durable records/sec per connection on a dev laptop under
`F_FULLFSYNC`, and up to ~31× that under group commit where the queue is deep enough to
fill a batch ([docs/BENCH.md](docs/BENCH.md)). No bare-metal run exists; this paragraph is
resized when one lands. You will not outgrow one box before you can afford to build
something bespoke, and pretending otherwise is how platforms die building a worse Spanner
instead of a better SQLite.

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
fault-injection universes (master seed `0x483e230baed4922b`;
[docs/LEDGER.md](docs/LEDGER.md), 2026-07-07). The
dual-write bug class the second guarantee above claims to delete
structurally is demonstrated, caught, and reverted at
[examples/invoicing/DEMO.md](examples/invoicing/DEMO.md).

M1 made it a platform: replication with promotable followers and manual
failover, realtime SSE with exact `Last-Event-ID` resume, auth (sessions,
API keys, static-JWKS OIDC), schema migrations as log records, `charpente
describe`, and a black-box edge harness that drives stock binaries over
real TCP with `kill -9` and TCP-proxy fault injection.

**M2 closed 2026-09-13; M3 — launch — shipped 2026-10-10.** M2 shipped six sub-projects,
dropped one, and reclassified the rest to after launch with the trigger that reopens each
([PLAN.md](PLAN.md)). M3 was the finish line before this went public. charpente `0.1.0` is
on crates.io, and a stranger given only this README installed it, scaffolded an app, and
reached a red sim with a seed that replays
([docs/experiments/2026-10-launch-run](docs/experiments/2026-10-launch-run/)). A third
docs-only agent build passed review with zero Criticals the platform should have caught.
Every number in this file is measured or gone. The first two of
M2's sub-projects — an honest benchmark harness and primary-side group commit — have their
numbers in [docs/BENCH.md](docs/BENCH.md), measured on a dev laptop under `F_FULLFSYNC`:
one connection costs one durable fsync per transaction, ~215 durable records/sec on that
hardware — the figure the scaling paragraph above states — and group commit amortizes that
fsync across a batch, worth up to ~31x where the queue is deep enough to fill one and
nothing at all at one connection. No bare-metal Linux run exists; PLAN.md's after-launch
table carries it. The object-storage archive has since shipped, unmeasured — BENCH.md
carries nothing about it. It ships one generation at a time to a bucket: a full logical
snapshot plus the sealed WAL segments that continue it, held by a retention pin that
yields to the primary rather than growing the log until it fills the disk, and `charpente
restore` rebuilds a data directory from what is there ([docs/GUIDE.md](docs/GUIDE.md)
§15). `serve::backup` is still a different thing — a promotable hot standby, not a
snapshot shipper. A TypeScript client generator has since shipped too
([docs/GUIDE.md](docs/GUIDE.md) §14) — SSE only, no WebSocket transport — and so has
tenant scoping, where a tenant is a typed key prefix enforced by the compiler rather than
a `where` clause anyone can forget ([docs/GUIDE.md](docs/GUIDE.md) §16), and since
2026-09-16 the platform asks: a route says `auth`, `public` or `public(Org)`, a table, a
job and an event say `scoped_by` or `global`, and none is a default. What is *not* built, so you don't have to go looking:
no blob store and no WebSockets.

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
