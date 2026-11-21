# Neighbors & prior art

charpente's quadrant is Rust, single-binary, typed end to end,
deterministic-simulation-native and self-hostable. This is who lives near it, what each
neighbor proves, and why the quadrant was still empty as of mid-2026, followed by the
technique lineage an implementer must study.

## Products near the quadrant

| Neighbor | What it proves | Why it isn't charpente |
|---|---|---|
| Convex (convex.dev; backend OSS'd 2024, FSL) | Closest in spirit: typed transactional functions and reactive subscriptions in one platform, and developers love the model | It's TypeScript, so its checks run at runtime and there is no compile-time oracle. It's hosted-first, has no determinism or DST story, and is separate components instead of one binary |
| SpacetimeDB (Clockwork Labs) | Closest in shape: the database and server are fused into one process, "reducers" run inside the database, and realtime subscriptions are built in | Reducers are WASM modules, so a serialization boundary comes back and host languages vary. There's no deterministic-simulation testing story, and that's still true of 2.0 (2026-03), their explicit push into general web dev, which left the game-centric era behind without closing the DST gap. Study it seriously, because it validates demand for the fused shape |
| PocketBase (Go) | A massive appetite for "backend in one file" (auth, DB, realtime and files on SQLite) | An untyped, dynamic surface in Go, with no transactional queue and no sim, and it explicitly positions itself as hobby or small-scale |
| Supabase / Firebase | The bundle (auth, DB, realtime, storage and jobs) is the right product | The opposite shape: N separate services composed behind a dashboard, so they operate every boundary charpente deletes |
| Cloudflare Durable Objects (+ SQLite-in-DO, 2024) | Data next to compute with zero-latency storage is real and shipping at scale, and single-threading each object tames concurrency | Proprietary runtime, JS/WASM, a per-object sharding model, no self-host and no sim |
| TigerBeetle (Zig) | The engineering discipline charpente adopts: DST-native from day one (VOPR), a single-threaded state machine over a replicated log, static allocation and TIGER_STYLE | It's a ledger database, by design not a general app platform, with no HTTP or app layer. charpente takes its playbook and leaves the product |
| FoundationDB | The origin of industrial DST. Deterministic C++ actors and a simulated cluster found bugs nothing else could, and Apple runs it under iCloud | A distributed KV store you still build an app stack around. The technique stayed niche because it has to be designed in from day one, and agents now make that tax cheap |
| rqlite / Litestream / Turso(libsql) | SQLite plus replication or backup covers a huge share of real durability needs, and log shipping is a proven, simple way to replicate | Storage layer only. The app tier, queue, jobs and realtime are all still BYO |
| Loco.rs (Rails-of-Rust) | Someone tried human-era conventions in Rust | Convention over configuration, aimed at humans, on SQL and an ORM, with no new guarantees. It's the clone-the-past trap DECISIONS.md §3 rejects |
| Rama (Red Planet Labs, Clojure) | "Collapse the backend into one programmable system" resonates, and it is log-first dataflow end to end | Dynamic language, a dataflow/PState paradigm, a heavyweight cluster model, and proprietary |
| EdgeDB (now Gel) | A typed, schema-first database with rich tooling can win people's love | Database only and query-language-first, and it keeps the client-server shape |

Each neighbor validates one edge of the quadrant: Convex typed transactional functions,
SpacetimeDB the fused process, PocketBase the demand for a single binary, TigerBeetle and FDB
the DST discipline, and Durable Objects data beside compute. None combines them. The two edges
hardest to retrofit, the compile-time oracle and determinism, are the two charpente starts
from. That asymmetry is the moat: for Convex or SpacetimeDB, adding DST is a multi-year
rewrite (FDB and TigerBeetle show why), and for charpente, adding their features is milestone
work.

## Technique lineage (read before implementing)

**Deterministic simulation testing**
- Will Wilson, *Testing Distributed Systems w/ Deterministic Simulation* (Strange Loop
  2014): the FoundationDB origin story, and the argument for owning every IO source.
- TigerBeetle: the VOPR simulator, and TIGER_STYLE.md, their engineering-style doc
  (assertions, static limits, negative-space programming). Adopt the spirit in CLAUDE.md.
- Antithesis (antithesis.com) sells DST as a service through a deterministic hypervisor,
  which is proof the market values the property. charpente builds it in at the language
  level instead.
- turmoil (tokio-rs) is network simulation for tokio. madsim is a deterministic tokio
  replacement that RisingWave uses in production CI. Study both APIs before designing the
  `Net`/`Clock` traits. madsim's trick of staying compatible with tokio's API is the
  likeliest way to keep the ecosystem usable at the edges.
- TigerBeetle's Vörtex (2025) is the black-box complement to the VOPR: stock release
  binaries, TCP-proxy network faults, real process kill and pause. It's the model for
  testing charpente's hyper edge, which the in-process sim cannot reach (PLAN M1).
- Jepsen's TigerBeetle analysis (2025) is the sharpest independent critique of DST.
  Corruption granularity is its own fault axis (whole-sector injection missed a single-byte
  bug), and heavy fuzzing still leaves pattern gaps. Read it before expanding the fault
  matrix.
- Polar Signals' "theater of state machines" and S2.dev's DST writeup (2025) show Rust
  practitioners converging on bus-owned scheduling and total entropy control. Their gotchas
  (ambient HashMap seeds, wall-clock timestamps in log lines) are why this deny-list exists.
  S2's mad-turmoil fuses turmoil's net-sim with madsim's libc-level determinism.
- sled's simulation writeup (Tyler Neely) and FoundationDB's `flow` docs, on the failure
  modes of half-deterministic designs.

**Single-threaded state machines over logs**
- LMAX Disruptor (Fowler's writeup): mechanical sympathy, with business logic on one thread
  at millions of ops/sec.
- Redis: a single-threaded command loop as a feature, for simplicity and serializability.
- The Kafka and event-sourcing literature, for the log-is-the-truth framing. charpente is
  state-machine-first, though, not stream-processing-first.

**Storage**
- redb (a pure-Rust B-tree with a `StorageBackend` trait: mount it behind the `Disk`
  boundary). Compare sled's postmortems. For LSM background, the RocksDB wiki and the
  LevelDB paper. Health check 2026-07: actively maintained (v4.1.0, 2026-04). Its 2026
  changelogs credit AI coding agents with finding real corruption-class bugs. That's a data
  point for the thesis, and a reminder that the engine under our tables has had fresh
  corruption bugs, which is why the fault matrix runs through it.
- Litestream's WAL-shipping design doc, the blueprint for M1's replication and backup.

**Durable execution**
- Temporal's activities are the shape `call`/`settle` takes, with the transaction as the
  workflow: a side effect runs outside the deterministic part, and its result re-enters it.
  Restate journals the same idea per handler. DBOS keeps workflow state in Postgres beside
  the app's own. charpente differs in three ways. There is one log, with no workflow history
  kept beside the data. The simulator drives the remote itself, so the outside world's
  faults are injected and replayed from a seed. And the idempotency keys are minted at
  staging rather than derived from ids or app data (DECISIONS §14).

**Shrinking a failing run**
- Hypothesis (hypothesis.works) shrinks the recorded sequence of choices a test consumed,
  not the generated values, so any value built from those choices shrinks without a
  per-type shrink strategy. `sim.input()` and the shrinker follow that model. A run records
  the choice each draw used (the raw value, reduced into the draw's range), so a smaller
  choice is a simpler value, and a failure is replayed with smaller sequences.
- proptest (github.com/proptest-rs/proptest) keeps a value tree per strategy, and
  shrinking walks each value through the strategy that generated it. It's listed for
  contrast, since charpente has no strategies to carry.
- charpente differs from both in two ways. The fault axes are part of the same shrink,
  turned off one at a time. And a candidate counts only if it still fails the same check,
  since a smaller input can move where faults land and slip to a different bug.

**Deployment/scale stance**
- "Use One Big Server" (specbranch.com, 2022): the capacity math behind MANIFESTO §7.
- SQLite's "when to use" doc: the original argument for being honest about what a single node
  can do.

**Hot reload (open question DESIGN §9)**
- Dioxus "subsecond" and hot-patching work (2025), prior art for patching Rust at dev time.
  It's unproven for server handlers, so do not make anything depend on it.

## Standing intel questions

Check these quarterly. Each would change positioning if its answer flips. Last checked
2026-07-02, by a two-agent primary-source sweep:
1. Does Convex ship any determinism or replay story? No. It has only 2022-era property
   tests and a JS mock backend, and its agent push (an "Agent" component) targets agents as
   app users, not agents as developers.
2. Does SpacetimeDB generalize beyond games, and add a testing story? It generalized, yes,
   with 2.0 (2026-03), an explicit web-dev push. The row above is updated. It still has no
   testing story. Reducers are still WASM, and there's been no new funding since the 2022
   Series A.
3. Does anyone else publish an "agent-native framework" explicitly targeting feedback-loop
   quality? Still no. Agent tooling is table stakes everywhere now, though (Encore "for
   Humans and Agents", Supabase's $500M agents-build-databases raise, Cloudflare Project
   Think). The moat is specifically determinism plus the compile-time oracle. Being
   "agent-friendly" no longer sets anyone apart on its own.
4. Do Rust compile times improve enough (parallel frontend, cranelift backend) to void the
   compile-latency risk in PLAN.md? Not yet. The parallel frontend is still nightly-only,
   and its target was walked back to 20 to 25% at 8 threads. Cranelift is a ≈5% total
   clean-build win, with no debug info. The risk stands.
