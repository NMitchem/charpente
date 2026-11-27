# The Charpente Manifesto

## 1. Code is no longer scarce

For seventy years, every architectural decision in software assumed that expert code was
the scarce resource. Frameworks, ORMs, managed services, microservices and the rest of the
vendor stack all exist to save elite engineering labor.

That constraint is gone. In early 2026, sixteen parallel AI agents built a ~100,000-line C
compiler in two weeks for about $20,000. It passes 99% of the GCC torture suite and boots a
Linux kernel ([Anthropic's write-up](https://www.anthropic.com/engineering/building-c-compiler);
sources in [RESEARCH.md](RESEARCH.md)). Agents now write storage engines, schedulers and
protocol implementations at close to zero marginal cost. Trusting what they write is the
hard part now.

## 2. The bottleneck moved from writing to verifying

The compiler experiment shows where agents do well. The GCC torture suite is a dense
mechanical oracle, and against it the agents hit 99%. Codegen efficiency, assembler
correctness and code quality had no oracle, and there the output was weak.

**Agent output quality tracks oracle density.** Cheap code is only worth something where
checking it is also cheap. A proof checker, a conformance suite, a differential test or a
deterministic simulator is what turns untrusted agent output into a system you can ship.
Human review can't keep up with what agents produce, so the most useful thing anyone can
build right now is the oracle substrate: whatever makes machine-written code mechanically
checkable.

## 3. Frameworks now compete for agents

Most of what Django, Rails and Next.js offer is onboarding. Their conventions exist so a
human doesn't have to decide things, and their docs exist so a human can learn them in a
weekend. That's worth less when the "developer" reads the whole source in seconds and never
forgets an API.

The feedback loop is worth more. An agent pointed at a framework succeeds or fails on how
fast and how precisely the framework says no. A compile error that names the exact type
mismatch beats a runtime 500, and a failing simulation with a seed beats a flaky repro in
staging. People will point their agents at the platforms where the agents succeed, so the
framework that wins is the one that reviews best.

So charpente's design target is the substrate on which an agent-built backend is most likely
to be correct, and provably so.

## 4. The service stack is an accident of team boundaries

A conventional SaaS backend is a web server, a relational database, a cache, a queue, a cron
runner and an observability pipeline. That's six systems and six failure domains, with
network hops and serialization boundaries between them.

The shape comes from Conway's Law. Each piece is its own networked service because each was
once its own company: building any one of them in-house took a team. When one process can
hold all of them, those boundaries cost more than they save:

- Every hop adds latency. A typical CRUD request spends most of its life serializing,
  deserializing and waiting on connection pools.
- Every boundary is a consistency hole. The database committed but the queue publish failed,
  the cache is stale, the webhook fired twice. Whole pattern literatures (outbox, saga, CDC)
  exist to paper over the missing shared commit point.
- Each service is one more thing to operate: provisioning, credentials, upgrades, monitoring
  and incident runbooks, six times over.

Charpente puts the whole stack in one process on purpose. Your business logic and your data
share an address space, and a "queue publish" is a write to the same log as your state
change, in the same transaction. That deletes the dual-write bug class outright: the state
change and the publish can't disagree, because they commit together.

## 5. One commit point

The deepest simplification in charpente is that exactly one place makes things true: the
log. A transaction reads state, writes state, enqueues jobs and emits events, and either all
of it commits or none of it does. External effects such as emails and third-party API calls
run after the commit, driven by the durable queue, and idempotency keys make them
exactly-once-observable.

Everything downstream follows from that. Replication ships the log, realtime subscriptions
tail it, audit history reads it, failover replays it, and deterministic testing feeds it to
the state machine under a controlled scheduler. The usual stack approximates that one
primitive with six services and duct tape.

## 6. The framework is the reviewer

Charpente replaces the human review that no longer happens with machine verification:

- **Compile-time contracts.** Tables, queries, routes, jobs, events and config are Rust
  types. Business logic has no torture suite, and the type system is the oracle for its
  shape: an agent that breaks a contract gets a precise compile error, local to the change
  and fixable, within seconds. Types can't tell a right value from a wrong one, and a wrong
  price is still a valid integer.
- **Deterministic simulation.** The core is a deterministic state machine and all IO passes
  through swappable interfaces, so the entire application runs inside a simulator. Fault
  injection runs it through thousands of seeded universes per night, with crashes between
  any two writes, torn pages, partitions and clock skew. FoundationDB and TigerBeetle proved
  the technique finds the bugs that take a decade to surface in production. Charpente makes
  it the default for ordinary application code.
- **Promises the builder can't edit.** The business states its promises as checks.
  Charpente keeps those checks apart from the code the agent writes, names their version in
  every verdict, and reports what no promise judged.
- **Constraints as features.** Inside a transaction you can't await the network. That will
  feel alien if you expect Express-style "await anything anywhere," and it's deliberate:
  effects are explicit and ordered, so the simulator can run them. A programming model that
  admits nondeterminism can't be verified. One that forbids it can be replayed, byte for
  byte, from a seed.

## 7. One big box, replicated

Charpente refuses the sharding arms race. Writes flow through one ordered log on one
primary, and followers replay it for reads and failover. It's a bet: with no network hop
between logic and data, one server's ceiling is its disk's durable write rate, which is more
than almost any business will ever need (what charpente measures is in docs/BENCH.md).
Platforms that promise infinite horizontal scale charge for it up front in complexity,
consistency compromises and ops burden, whether you ever need the scale or not.

If you outgrow one big box, by then you'll have the revenue to build something bespoke, and
the log gives you a clean way out. Most businesses never get there, and charpente is built
for that overwhelming majority.

## 8. What we refuse to build

- **A microservice platform.** There's one process, so there's no service mesh, no RPC
  framework and no distributed tracing across processes.
- **A SQL-first database.** SQL's ergonomics are for humans at a REPL. Typed queries take
  part in the compile-time oracle and strings don't. (A read-only Postgres-wire endpoint for
  BI tools fits with this. Making SQL the primary interface doesn't.)
- **A migration target for existing apps.** Porting a Postgres app here misses the point.
  Charpente is for greenfield backends, the kind agents now mint daily.
- **Feature flags for determinism.** There's no "fast mode" that bypasses the log and no
  escape hatch that awaits the network mid-transaction. The guarantees are the product, so
  none of them can be switched off.
- **A compatibility layer.** When charpente's own API or formats change, they change in
  place. The break shows up as a compile error or a named refusal that says what to do, and
  there are no parallel APIs. From 1.0, stored data is the exception. A format can still
  change, but an app's data and its unfinished work cross every later charpente release,
  and the simulator rehearses each upgrade on a copy of the data before the release ships.
  An agent fixes a broken call in seconds, but yesterday's customers can't place their
  orders again.

## 9. The wager

Charpente bets that within a few years most backends will be written by agents and directed
by people, and that those people will choose platforms the way they once chose CI systems:
by how reliably the machine catches the machine's mistakes. The framework that wins then
will be the one that can tell a person: *every path through this system has been executed,
under fault, thousands of times, and judged against promises its builder could not edit.
Here is the seed to prove it, and here is what nothing judged.*
