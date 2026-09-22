# The Charpente Manifesto

## 1. Code is no longer scarce

For seventy years, every architectural decision in software assumed that expert code was
the scarce resource. Frameworks, ORMs, managed services, microservices, the entire vendor
stack — all of it exists to *economize on elite engineering labor*.

That constraint is gone. In early 2026, a team of parallel AI agents built a C compiler
that passes nearly all of the GCC torture suite and boots a Linux kernel, in weeks, for
less than a junior engineer's quarter. Agents write storage engines, schedulers, and
protocol implementations at a marginal cost approaching zero. The question is no longer
"can we afford to build it?" It is "can we afford to *trust* it?"

## 2. The bottleneck moved from writing to verifying

Look closely at that compiler experiment and you see the shape of the new world. Where a
dense mechanical oracle existed — the torture suite — the agents passed nearly all of it.
Where no oracle existed — codegen efficiency, assembler correctness, code quality — the
output was weak.

**Agent output quality tracks oracle density.** Cheap code is only valuable where
verification is also cheap. A proof checker, a conformance suite, a differential test, a
deterministic simulator — these convert untrusted agent output into shippable systems.
Human review does not scale to the volume agents produce, and pretending it does is how
incidents happen.

So the highest-leverage artifact anyone can build right now is not more code. It is the
**oracle substrate**: the thing that makes machine-written code mechanically checkable.

## 3. Frameworks now compete for agent preference, not human mindshare

Django, Rails, and Next.js are crystallized onboarding. Their conventions exist so a human
doesn't have to decide things; their docs exist so a human can learn them in a weekend.
That value depreciates when the "developer" reads the entire source in seconds and never
forgets an API.

What appreciates is the feedback loop. An agent pointed at a framework succeeds or fails
based on how fast and how precisely the framework says *no*: a compile error with the exact
type mismatch beats a runtime 500; a reproducible simulation failure with a seed beats a
flaky staging repro. Agents will be directed toward the platforms they succeed on. **The
framework that wins the agent era is the best reviewer, not the best autocomplete.**

Charpente's design target is therefore not "less code for humans." It is: *the substrate on
which an agent-built backend is most likely to be correct, and provably so.*

## 4. The five-service stack is an accident of team boundaries

A conventional SaaS backend is a web server, a relational database, a cache, a queue, a
cron runner, and an observability pipeline — six systems, six failure domains, and a mesh
of network hops and serialization boundaries between them.

That shape is Conway's Law, not physics. Each piece is a separate networked service because
each piece was a separate *company* — because building any one of them in-house was a
team-sized effort. When one process can hold all of them, the boundaries stop paying rent
and start charging it:

- Every hop is latency. A typical CRUD request spends most of its life serializing,
  deserializing, and waiting on connection pools.
- Every boundary is a consistency hole. The database committed but the queue publish
  failed. The cache is stale. The webhook fired twice. Entire pattern literatures — outbox,
  saga, CDC — exist to paper over the absence of a shared commit point.
- Every service is an ops surface: provisioning, credentials, upgrades, monitoring,
  incident runbooks. Times six.

Charpente collapses the stack into one process on purpose. Your business logic and your
data live in the same address space. A "queue publish" is a write to the same log as your
state change, in the same transaction. This is not a convenience — it is the deletion of
the dual-write bug class, structurally, forever.

## 5. One commit point

The deepest simplification in charpente is that there is exactly one place where things
become true: the log. A transaction reads state, writes state, enqueues jobs, and emits
events — and all of it commits atomically or none of it does. External effects (emails,
third-party API calls) execute *after* commit, driven by the durable queue, with
idempotency keys making them exactly-once-observable.

Everything downstream falls out of this. Replication is shipping the log. Realtime
subscriptions are tailing the log. Audit history is reading the log. Failover is replaying
the log. Deterministic testing is feeding the log to the state machine under a controlled
scheduler. One primitive, honestly implemented, instead of six services duct-taped into an
approximation of it.

## 6. The framework is the reviewer

Charpente substitutes machine verification for the human review that no longer happens:

- **Compile-time contracts.** Tables, queries, routes, jobs, events, config, and telemetry
  are Rust types. The type system is the oracle for the layer that has no torture suite:
  business logic. An agent that breaks the schema contract gets a precise, local, fixable
  error in seconds — the exact feedback loop agents thrive on.
- **Deterministic simulation.** Because the core is a deterministic state machine and all
  IO passes through swappable interfaces, the entire application runs inside a simulator.
  Fault injection — crashes between any two writes, torn pages, partitions, clock skew —
  runs thousands of seeded universes per night. FoundationDB and TigerBeetle proved this
  technique finds the bugs that take a decade to surface in production; charpente makes it
  the *default* for ordinary application code, not a heroic discipline for database teams.
- **Constraints as features.** Inside a transaction you cannot await the network. This will
  feel alien to anyone expecting Express-style "await anything anywhere." It is the point:
  effects are explicit, ordered, and simulable. A programming model that admits
  nondeterminism cannot be verified; one that forbids it can be replayed, byte for byte,
  from a seed.

## 7. One big box, replicated

Charpente refuses the sharding arms race. Writes flow through one ordered log on one
primary; followers replay it for reads and failover. This is an honest bet: with no network
hop between logic and data, one server's ceiling is its disk's durable write rate, and that
is more than almost any business will ever need (what charpente measures is in
docs/BENCH.md). The platforms that promise infinite horizontal scale charge for it up
front, in complexity, consistency compromises, and ops burden, whether you ever need it or
not.

If you outgrow one big box, you will by then have the revenue to build something bespoke —
and the log gives you a clean migration path out. Designing for the overwhelming majority
is not a limitation. It is the moat.

## 8. What we refuse to build

- **A microservice platform.** No service mesh, no RPC framework, no distributed tracing
  across processes — there is one process.
- **A SQL-first database.** SQL ergonomics exist for humans at REPLs. Typed queries
  participate in the compile-time oracle; strings do not. (A read-only Postgres-wire
  endpoint for BI tools is compatible with this; making SQL the primary interface is not.)
- **A migration target for existing apps.** Porting a Postgres app here misses the point.
  Charpente is for greenfield backends — the kind agents now mint daily.
- **Feature flags for determinism.** There is no "fast mode" that bypasses the log, no
  escape hatch that awaits the network mid-transaction. The guarantees are the product;
  optional guarantees are no guarantees.

## 9. The wager

Charpente bets that within a few years, most backends will be written by agents and
*directed* by people — and that those people will choose platforms the way they once chose
CI systems: by how reliably the machine catches the machine's mistakes. When that day
comes, the winning framework will not be the one that saved the most keystrokes. It will be
the one that could look a human in the eye and say: *every path through this system has
been executed, under fault, thousands of times, and here is the seed to prove it.*

That is the frame we are building. Everything else hangs off it.
