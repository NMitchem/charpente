# M2 sub-project 1 — honest benchmarks: design

*The instrument for DECISIONS §7's build triggers (the own-LSM engine is deferred
behind measurement, ruled 2026-08-06, commit f099aac), and PLAN's own unfulfilled
risk mitigation: "publish honest benchmarks early." Measurement only — this
sub-project changes nothing inside the deterministic boundary.*

## Context

MANIFESTO §7 and README both claim "hundreds of thousands of transactions per
second" on one big box. No benchmark code exists anywhere in the repo; the claim
has never been measured. Two facts, verified in source, frame what measurement
will find:

- `prod.rs:171-174` — the production `File::sync` is `sync_all()`, which on macOS
  is `F_FULLFSYNC` (the comment at the site says so, and why: plain fsync there
  does not survive power loss). Laptop numbers will be honestly durable, and
  durably slow — `F_FULLFSYNC` runs low-thousands per second on Apple SSDs.
- `engine.rs:110-123` — the primary's submit path is append → sync → apply → ack:
  **one sync per transaction, no group commit**. The follower's apply path
  (`engine.rs:137-162`) already amortizes one sync over a shipped batch, so the
  engine shape supports batching; the primary path does not use it.

**The prediction this spec commits to testing:** the core write path measures at
≈1× the device's durable-sync rate — sync-bound, far below the headline claim on
any hardware. If confirmed, the gap between measured tps and the headline is a
*group-commit question for a future sub-project*, and this harness is what sizes
it. If refuted, we learn something better. Either way the number becomes
DECISIONS §7 trigger (a)'s instrument: "benchmarks show redb limiting the
headline throughput claim" is answerable only when the bottleneck's layer is
measurable — a sync-bound commit path indicts the log discipline, not redb, and
no LSM would change it.

## Scope (Noah rulings, 2026-08-06)

1. **The LSM premise re-examination resolved: defer, benchmark first.** Recorded
   in DECISIONS §7 (reopened on schedule, re-closed with three named triggers).
2. **Scope: the headline claim, layered.** Charpente alone at four measurement
   points (device / core / kernel / wire). No incumbent-stack comparison in this
   sub-project.
3. **Workload: canonical txn primary, invoicing secondary.** The README-shaped
   transaction is what "tps" means in every quoted figure; `examples/invoicing`
   provides a realistic-app figure at the wire layer only.
4. **Hardware: laptop now, server at launch.** Dev-hardware numbers are honestly
   labeled as such; the numbers of record for launch come from a rented
   bare-metal Linux box when launch nears (not part of this sub-project).

## §1 Shape and placement

A new workspace crate, `crates/bench-harness`, on the `edge-harness` precedent:
outside the deterministic boundary, wall clocks and threads permitted via
site-scoped deny-list allows, never on the `cargo test --workspace` hot path.
Single dependency on `charpente` (the officially load-bearing re-export surface
reaches `Db`, `Kernel`, and the sim-source seam; a second dep is a design smell).
No new external dependencies — load generation, timing, and histograms are
hand-rolled (§4).

Contents:

- **lib** — the canonical bench app: minimal `#[table]`/`#[route]`/`#[job]`/
  `#[event]` surface plus its `build()` fn, exported so every bin below shares
  one app definition.
- **bin `bench`** — subcommands `device`, `core`, `kernel`, `wire`, `smoke`,
  plus `all` (*amended at plan fix-wave, 2026-08-07: report assembly lives in
  Rust as a sixth subcommand because JSON is hand-rolled here — merging JSON
  fragments in bash without a JSON library would be worse; `scripts/bench.sh`
  delegates to it*); human and JSON output.
- **bin `bench-app`** — the wire-layer vehicle. *Amended at plan fix-wave
  (2026-08-07): originally "`charpente::launch(addr, data_dir, build)`, exactly
  the standard app `fn main`" — impossible as stated. `launch()` re-reads the
  live process argv (`launch.rs:31`) and its `dispatch` serves only on ZERO
  argv (or `describe`); a child spawned with `--addr`/`--data-dir` flags exits
  2 as an unknown verb and never binds a port. `bench-app` therefore follows
  the committed vehicle precedent (`examples/invoicing/src/bin/node.rs`, whose
  module doc records this exact trap): parse its own flags, call
  `charpente::serve::serve`, park.*
- **`scripts/bench.sh`** — runs the full matrix and assembles one report.

## §2 The four measurement points

Each layer's delta from the previous localizes cost; no number is published
without the layer beneath it.

- **`device`** — the hardware ceiling. Two loops against a file on the target
  data directory's filesystem, through the production `Disk` impl: sync-only
  (repeated `sync` on a dirty file) and append-shaped (`write_at` + `sync` per
  iteration, the WAL's op shape). Reports durable syncs/sec. Every other number
  is published as a ratio against this one.
- **`core`** — `Db::transact` in a closed loop against the prod `Disk` on a
  throwaway data dir: the commit path alone (WAL append + sync + apply + store
  materialization), no kernel, no HTTP. The prediction in Context is about this
  number.
- **`kernel`** — a `Kernel` fed by an in-process bench source implementing
  `RequestSource` (`crates/charpente/src/kernel/mod.rs:65`, the seam `SimSource`
  and `ProdSource` share), requests pre-built, no TCP.
  Adds loop scheduling, dispatch, session/auth resolution, and job execution to
  `core`'s cost.
- **`wire`** — spawn `bench-app` as a child process (edge-harness's child
  pattern), drive real HTTP/1.1 keep-alive load from in-process client threads.
  This is the number the README claim is about.

## §3 Workloads and metric definitions

**Canonical txn** (primary, all layers): get one row + mutate + put + enqueue
one job + emit one event — the README example's shape. The job handler is a
no-op state transition (its Done-mark still commits a transaction; that is the
point). Platform crons exist but are quiescent at benchmark timescales (the
session sweep is daily).

Two write metrics, always reported together:

- **acked writes/sec** — `Ok` returns from `Db::transact` (core), 200s at the
  kernel/wire layers. 200-after-durable makes this honest by construction.
- **log records/sec** — the canonical request commits one effects record and its
  job's completion commits another, so ≈2× acked writes at steady state. Stating
  both forecloses flattering double-counting in either direction.

**Snapshot reads/sec** (secondary, core layer only — *amended at plan-writing,
2026-08-06: a wire-level read sweep mostly re-measures HTTP overhead the write
sweep already shows; deferred unless the read story needs wire evidence*):
typed gets against a populated store. Reads never touch the log; this number
exists to show where the write ceiling does *not* apply.

**Invoicing** (tertiary, wire only): `examples/invoicing`'s node binary driven
on one write route (the plan pins which), labeled "realistic app" — its handler
mix measures an application, not the platform's limit.

## §4 Load generation and metrics collection

Closed-loop, hand-rolled: N client threads, one keep-alive connection each,
each thread issuing requests sequentially and recording per-request latency from
a monotonic clock. Connection sweep: 1, 4, 16, 64, 256; per sweep point,
throughput plus p50/p99/p999 from a full sorted latency vector (no lossy
histogram at these volumes). The kernel is single-threaded, so rising N mainly
measures queueing delay — that is a feature of the design, not a flaw of the
harness, and the report says so.

Two disclosures ride every wire report: **closed-loop semantics** (coordinated
omission — a slow response suppresses subsequent arrivals, flattering tail
latencies versus an open-loop arrival process) and **client saturation** (client
CPU time is measured; a client that cannot outrun the server invalidates the
point, detected by comparing client-side busy time against wall time).

## §5 The honesty contract

Every report — JSON and human — embeds, not as prose elsewhere but as fields in
the artifact: hardware model and core count, OS and filesystem, sync semantics
(`F_FULLFSYNC` named on macOS), warmup discarded (default 5 s), measured
duration (default 30 s per point), and the device-ceiling ratio
(core acked-writes/sec ÷ device syncs/sec).

Run-validity rules, mechanical:

- **Suspension guard** (the measurement-method rule: laptops sleep mid-run and
  wall clocks cannot tell). *Amended at plan-writing (2026-08-06): the original
  CPU-vs-wall divergence check is wrong for fsync-bound loops, where CPU is
  legitimately tiny against wall.* The shipped mechanism is a heartbeat thread
  ticking on a fixed interval: a suspended process misses ticks while wall time
  inflates, independent of how blocked the measured loop is. Observed ticks
  below half of expected marks the run INVALID in the artifact itself, not in a
  footnote. Process CPU time is still recorded — it drives the client-saturation
  check.
- **Totals two ways**: every throughput number is computed both as
  requests ÷ duration and as a sum over per-thread counters; disagreement beyond
  rounding is a harness bug and fails the run.
- A number without its device ceiling alongside it is not publishable output;
  the report generator refuses to render one.

## §6 Reporting artifacts

JSON + human output on the sim harness's report discipline (DESIGN §7:
machine-readable surface, shape pinned by tests). `docs/BENCH.md` is the
committed methodology-and-numbers page: the honesty contract stated for readers,
the current numbers-of-record table with hardware labels ("Apple M-series
laptop, F_FULLFSYNC" until the Linux box run at launch replaces them), and the
one-command repro for each row. BENCH.md carries no `rust` code blocks (it is
not compile-gated and must not accidentally become so).

## §7 Testing the harness

The harness is itself code that can lie, so it gets its own checks, `#[ignore]`d
on the edge-harness model and run by `scripts/bench.sh --smoke`:

- an end-to-end smoke driving every subcommand at ~1 s durations against the
  real binaries;
- the totals-two-ways cross-check exercised with a deliberate mismatch (proves
  the failure arm exists, RED-first);
- the JSON shape pinned by test, sim-harness style;
- the client-saturation and CPU-wall INVALID arms each forced once, by
  construction rather than by luck (a sleeping thread, a starved client).

## §8 Non-goals

- No comparative incumbent-stack rig (deferred; would roughly double the work
  and demands scrupulous fairness — its own sub-project if ever).
- No CI gating, no regression tracking, no perf dashboards.
- No group-commit or any other platform change — measurement only. The harness
  sizes the group-commit opportunity; a future spec spends it.
- No sim-loop universes/sec metric (developer-loop speed is a separate concern
  with its own memory entry).
- No server rental, no launch-numbers run — this sub-project ships the
  instrument and the first honestly-labeled dev-hardware numbers.

## §9 Exit criteria

1. `scripts/bench.sh` produces one assembled report covering all four layers
   plus the invoicing wire run, on the laptop, with every honesty-contract field
   populated and both validity cross-checks live.
2. `docs/BENCH.md` committed with the first numbers of record, dev-labeled, each
   row carrying its one-command repro.
3. The Context prediction confirmed or refuted, in BENCH.md, with the measured
   core-to-device ratio — and if confirmed, the sized group-commit gap recorded
   as the follow-up's opening evidence.
4. Zero diffs inside the deterministic boundary: no `charpente-core` or
   `charpente` source change, trivially preserving every pinned seed. (Should a
   seam prove genuinely missing for the kernel layer, that is a stop-and-ask,
   not a workaround.)
5. The usual final bytes: fmt, clippy `-D warnings` (deny-list allows
   site-scoped in the new crate only), full workspace green, guide gate
   untouched.
