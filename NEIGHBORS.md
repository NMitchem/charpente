# Neighbors & prior art

Who lives near the quadrant, what each proves, and why the quadrant
(**Rust · single-binary · typed end-to-end · deterministic-simulation-native · self-hostable**)
was still empty as of mid-2026. Also: the technique lineage an implementer must study.

## Products near the quadrant

| Neighbor | What it proves | Why it isn't charpente |
|---|---|---|
| **Convex** (convex.dev; backend OSS'd 2024, FSL) | Closest *in spirit*: typed transactional functions + reactive subscriptions in one platform; developers love the model | TypeScript (runtime checks, not a compile-time oracle); hosted-first; no determinism/DST story; separate components, not one binary |
| **SpacetimeDB** (Clockwork Labs) | Closest *in shape*: database and server fused into one process, "reducers" run in-DB, realtime subscriptions built in | Reducers are WASM modules (serialization boundary re-enters, host langs vary); no deterministic-simulation testing story — still true of 2.0 (2026-03), their explicit push into general web dev; the game-centric era is over, the DST gap is not. Study it seriously — it validates demand for the fused shape |
| **PocketBase** (Go) | Massive appetite for "backend in one file" (auth+DB+realtime+files on SQLite) | Untyped-dynamic surface, Go, no transactional queue, no sim, explicitly hobby/small-scale positioning |
| **Supabase / Firebase** | The bundle is the right product (auth+DB+realtime+storage+jobs) | The anti-shape: N separate services composed behind a dashboard — every boundary charpente deletes, they operate |
| **Cloudflare Durable Objects (+ SQLite-in-DO, 2024)** | Data-next-to-compute with zero-latency storage is real and shipping at scale; per-object single-threading tames concurrency | Proprietary runtime, JS/WASM, per-object sharding model, no self-host, no sim |
| **TigerBeetle** (Zig) | The engineering discipline charpente adopts: DST-native from day one (VOPR), single-threaded state machine over a replicated log, static allocation, TIGER_STYLE | It's a ledger *database*, deliberately not a general app platform; no HTTP/app layer. The playbook, not the product |
| **FoundationDB** | The origin of industrial DST; deterministic C++ actors + simulated cluster found bugs nothing else could; Apple runs it under iCloud | A distributed KV store you still build an app stack around; the technique stayed niche because it demands day-one design — exactly the tax agents now make cheap |
| **rqlite / Litestream / Turso(libsql)** | SQLite + replication/backup covers a huge share of real durability needs; log-shipping is a proven, simple replication story | Storage-layer only; app tier, queue, jobs, realtime all remain BYO |
| **Loco.rs** (Rails-of-Rust) | Someone tried human-era conventions in Rust | Convention-over-configuration for *humans*; SQL/ORM; no new guarantees. The clone-the-past trap DECISIONS.md §3 rejects |
| **Rama** (Red Planet Labs, Clojure) | "Collapse the backend into one programmable system" resonates; log-first dataflow end-to-end | Dynamic language, dataflow/PState paradigm, heavyweight cluster model, proprietary |
| **EdgeDB→Gel** | Typed-schema-first database with rich tooling can win love | Database-only, query-language-first, client-server shape retained |

**Synthesis:** every neighbor validates one edge of the quadrant — Convex (typed txn
functions), SpacetimeDB (fused process), PocketBase (single binary demand), TigerBeetle/FDB
(DST discipline), DO (data-beside-compute). None combines them, and the two hardest edges
to retrofit (compile-time oracle, determinism) are the two charpente starts from. That's
the moat: for Convex or SpacetimeDB to add DST is a multi-year rewrite (see FDB/TigerBeetle
on why); for charpente to add their features is milestone work.

## Technique lineage (read before implementing)

**Deterministic simulation testing**
- Will Wilson, *Testing Distributed Systems w/ Deterministic Simulation* (Strange Loop
  2014) — the FoundationDB origin story; the argument for owning every IO source.
- TigerBeetle: the VOPR simulator, and **TIGER_STYLE.md** (their engineering-style doc —
  assertions, static limits, negative-space programming). Adopt the spirit in CLAUDE.md.
- Antithesis (antithesis.com) — DST as a service via deterministic hypervisor; proof the
  market values the property; charpente builds it in-language instead.
- **turmoil** (tokio-rs) — network simulation for tokio; **madsim** — deterministic tokio
  replacement used by RisingWave in production CI. Study both APIs before designing the
  `Net`/`Clock` traits; madsim's tokio-API-compatibility trick is the likeliest shape for
  keeping the ecosystem usable at the edges.
- TigerBeetle's **Vörtex** (2025) — the black-box complement to the VOPR: stock release
  binaries, TCP-proxy network faults, real process kill/pause. The model for testing
  charpente's hyper edge, which the in-process sim cannot reach (PLAN M1).
- **Jepsen's TigerBeetle analysis** (2025) — the sharpest independent critique of DST:
  corruption *granularity* is its own fault axis (whole-sector injection missed a
  single-byte bug), and heavy fuzzing still leaves pattern gaps. Read before expanding
  the fault matrix.
- Polar Signals' "theater of state machines" and S2.dev's DST writeup (2025) — the Rust
  practitioner convergence on bus-owned scheduling and total entropy control (their
  gotchas — ambient HashMap seeds, wall-clock timestamps in log lines — are this
  deny-list's reason to exist); S2's mad-turmoil fuses turmoil's net-sim with madsim's
  libc-level determinism.
- sled's simulation writeup (Tyler Neely) + FoundationDB's `flow` docs — failure modes of
  half-deterministic designs.

**Single-threaded state machines over logs**
- LMAX Disruptor (Fowler's writeup) — mechanical sympathy; business logic on one thread at
  millions of ops/sec.
- Redis — single-threaded command loop as a feature (simplicity, serializability).
- Kafka/event-sourcing literature for the log-is-the-truth framing — but charpente is
  state-machine-first, not stream-processing-first.

**Storage**
- redb (pure-Rust B-tree; has a `StorageBackend` trait — mount it behind the `Disk`
  boundary); compare sled postmortems. LSM background: RocksDB wiki, LevelDB paper.
  Health check 2026-07: actively maintained (v4.1.0, 2026-04); its 2026 changelogs credit
  AI coding agents with finding real corruption-class bugs — a thesis data point, and a
  reminder the engine under our tables has had fresh corruption bugs: the fault matrix
  runs through it for a reason.
- Litestream's WAL-shipping design doc — the M1 replication/backup blueprint.

**Durable execution**
- Temporal's activities are the shape `call`/`settle` takes, with the transaction as the
  workflow: a side effect runs outside the deterministic part, and its result re-enters it.
  Restate journals the same idea per handler. DBOS keeps workflow state in Postgres beside
  the app's own. What charpente does differently: there is one log, not a workflow history
  beside the data; the simulator drives the remote itself, so the outside world's faults
  are injected and replayed from a seed; and the idempotency keys are minted at staging
  rather than derived from ids or app data (DECISIONS §14).

**Shrinking a failing run**
- Hypothesis (hypothesis.works) shrinks the recorded sequence of choices a test consumed,
  not the generated values, so any value built from those choices shrinks without a
  per-type shrink strategy. `sim.input()` and the shrinker follow that model: a run records the
  raw choices it drew, a smaller choice is a simpler value, and a failure is replayed with
  smaller sequences.
- proptest (github.com/proptest-rs/proptest) keeps a value tree per strategy, and
  shrinking walks each value through the strategy that generated it. Listed for contrast:
  charpente has no strategies to carry.
- Where charpente differs from both: the fault axes are part of the same shrink, turned
  off one at a time, and a candidate counts only if it still fails the same check, since
  a smaller input can move where faults land and slip to a different bug.

**Deployment/scale stance**
- "Use One Big Server" (specbranch.com, 2022) — the capacity math behind MANIFESTO §7.
- SQLite "when to use" doc — the original single-node honesty argument.

**Hot reload (open question DESIGN §9)**
- Dioxus "subsecond"/hot-patching work (2025) — prior art for patching Rust at dev time;
  unproven for server handlers, do not make load-bearing.

## Standing intel questions

Track quarterly; each would change positioning if it flips. Last checked 2026-07-02
(two-agent primary-source sweep):
1. Does Convex ship any determinism/replay story? — **No.** 2022-era property tests and a
   JS mock backend only; their agent push (an "Agent" component) targets agents as app
   *users*, not agent-developers.
2. Does SpacetimeDB generalize beyond games (and add a testing story)? — **Generalized,
   yes** (2.0, 2026-03, explicit web-dev push; row updated above). **Testing story:
   still none.** Still WASM reducers; no new funding since the 2022 Series A.
3. Does anyone else publish an "agent-native framework" explicitly targeting feedback-loop
   quality? — **Still no.** But agent *tooling* is now table stakes everywhere (Encore
   "for Humans and Agents", Supabase's $500M agents-build-databases raise, Cloudflare
   Project Think). The moat is specifically determinism + the compile-time oracle;
   "agent-friendly" alone no longer differentiates.
4. Do Rust compile times improve enough (parallel frontend, cranelift backend) to void the
   compile-latency risk in PLAN.md? — **Not yet.** Parallel frontend still nightly-only
   (target walked back to 20–25% at 8 threads); cranelift ≈5% total clean-build win, no
   debug info. The risk stands.
