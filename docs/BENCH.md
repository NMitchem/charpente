# Benchmarks

Honest numbers or none (PLAN's own risk rule). Method before results.

Everything here is reproducible with one command. The instrument is
`crates/bench-harness`; the design is
`docs/superpowers/specs/2026-08-06-benchmark-harness-design.md`.

## Method

### The four layers

Each layer's delta from the one beneath it localizes cost, and no number is
published without the layer beneath it in the same invocation.

**device** — the hardware ceiling. Two loops against a file on the target data
directory's filesystem, through the production `Disk` impl: a sync-only loop
(repeated `sync` on a dirty file) and an append-shaped loop (`write_at` at the
tail + `sync`, the WAL's op shape). This is the number every other layer is a
ratio against.

**core** — `Db::transact` in a closed loop against the production `Disk` on a
throwaway data directory: the commit path alone (WAL append + sync + apply +
store materialization). No kernel, no HTTP.

**kernel** — a `Kernel` fed by an in-process source through the same
`RequestSource` seam `SimSource` and `ProdSource` share, with requests pre-built
and no TCP. Adds loop scheduling, dispatch, and job execution to core's cost.

**wire** — the server spawned as a real child process, driven over real
HTTP/1.1 keep-alive connections by in-process client threads. Two targets:
`bench-app` (a purpose-built vehicle running the canonical transaction) and
`examples/invoicing` (a real application, labeled as such — its handler mix
measures an app, not the platform's limit).

### The workload

The **canonical transaction** is the README's shape: get one row, mutate it, put
it back, enqueue one job, emit one event. The job handler is a no-op state
transition — but its Done-mark still commits a transaction, which is the point.

Two write metrics are always reported together, because reporting either alone
invites flattering double-counting in one direction or the other:

- **acked writes/sec** — `Ok` from `Db::transact` at the core layer, 200s at the
  kernel and wire layers. Charpente acks only after the WAL sync, so this is
  honest by construction rather than by convention.
- **log records/sec** — the canonical request commits one effects record and its
  job's Done-mark commits another, so at the kernel and wire layers this is ≈2×
  the acked rate at steady state.

**The wire layer's record rate is DERIVED, not counted.** A client outside the
process cannot see the server's log, so that layer multiplies its counted
request rate by a per-target constant: 2.0 for `bench-app` (the canonical
transaction's own dual-write invariant) and 1.0 for invoicing's route, which is
a get + put with no enqueue.

**The gate's numerator is a derived quantity too, as of the 2026-08-10
group-commit interlude's T1 fix wave.** `syncs_per_sec` is
`syncs_per_request * throughput` — the same per-target multiplier, honestly
named for what it bounds. This reverses an earlier ruling that the wire gate
must stay on a COUNTED quantity: what made that ruling necessary was a
`1.0 + 1e-6` tolerance turning ordinary derivation error into a false alarm,
not anything wrong with deriving per se. The device-ratio-gate bullet below
carries the wider tolerance that absorbs it instead. **Owed work:** a
group-commit primary path breaks the one-fsync-per-commit premise this
multiplier assumes, and a client outside the process cannot count real sync
calls — either a server-reported sync counter lands, or this gate is
re-scoped honestly and this doc says so. See the group-commit plan's Task 10.

### What can invalidate a run

A run that trips any of these exits non-zero and says which, by name. The
thresholds are not tunable from the command line.

- **A heartbeat guard** spans every measured window. A laptop that sleeps, or a
  process starved of CPU, is detected and named rather than silently producing a
  fast-looking number over a wall-clock gap.
- **Totals two ways.** Per-thread counters are cross-checked against a shared
  counter; disagreement is a harness bug and invalidates the run that produced
  it.
- **Client saturation.** Client CPU time is measured against wall time; a point
  where the client is more than 90% busy is measuring the client, not the
  server, and is marked. Every point below is under 2%.
- **The device-ratio gate.** No layer may report a durable SYNC rate above what
  the device was measured to sustain, beyond that measurement's own error. The
  comparison is **syncs/sec against the device's own sync ceiling, not
  records/sec** (2026-08-10 group-commit interlude, T1): records per second
  only ever equalled the physical quantity because `Engine::submit` costs
  exactly one fsync per transaction with no batching, and that equality is
  what a group-commit primary path (N transactions under one fsync) breaks —
  a gate still comparing records/sec would refuse every healthy batched run.
  Each layer brackets the device either side of its own window and compares
  against the spread those two samples show. A gate this close to 1.0 cannot
  detect a violation smaller than its own measurement error, and does not
  claim to; it catches the class that matters — acking without syncing —
  which is an order-of-magnitude effect.
- **The bracket-stability gate.** The two device brackets either side of a
  window must agree to within **20%**. Past that they are not two samples of one
  quantity — the device's own capability moved *during* the measurement — so the
  rate has no stable denominator and the point is refused whichever bracket
  would have bounded it. This is independent of the device-ratio gate above and
  is checked even when that one passes: a plausible-looking ratio does not
  rescue a window whose denominator moved underneath it. The threshold is set
  from measured populations and the margin is narrow, which is stated rather
  than smoothed over: the worst spread a run this repo calls clean has produced
  is **15.7%** (and the core row in the table below is 14.0%), while the run
  this gate was added for produced **33.5%, 34.0% and 41.6%** at three of ten
  points after an unrelated build started mid-measurement. 20% is roughly
  forced by those two bounds — 15% would refuse the clean core row below.
- **Floors.** A window under one second, or under five operations, is refused
  rather than divided into a rate.

### Two disclosures that ride every wire number

**Closed-loop.** Each client thread issues its next request only after the
previous response arrives. A slow response therefore suppresses the arrivals
that would have queued behind it, which flatters tail latencies relative to an
open-loop arrival process. This is coordinated omission, and the tails below are
understated because of it.

**The kernel is single-threaded by design.** Rising connection counts mainly
measure queueing delay, not added capacity. The `bench-app` p50 at 256
connections below is 2.27 s against 8.8 ms at one connection, on flat
throughput; that is the design working as documented, not a defect the harness
found.

## Numbers of record — dev hardware (NOT the launch claim)

| | |
|---|---|
| Hardware | Apple M4 Pro, Darwin 25.5.0 arm64, 12 cores |
| Filesystem | apfs |
| Sync semantics | `F_FULLFSYNC` via `sync_all` |
| Build profile | release (with redb's debug-assertions ON — a workspace override, see below) |
| Window | 5 s warmup + 30 s measure, per window — device, core, and each wire point. The kernel layer is count-driven instead: a fixed 50,000 requests with no warmup, 460 s here. |
| Repro | `scripts/bench.sh` |

On macOS every measured write is `F_FULLFSYNC`-durable — a full device cache
flush, drastically slower than a Linux server's `fsync`. These numbers
**undersell** a server deliberately rather than oversell it. The workspace also
pins redb's `debug-assertions` on even in release (the 2026-07-15 mode-skew
fix), so the store path here carries assertion cost a tuned build would not.

| layer | metric | value | repro |
|---|---|---|---|
| device | durable syncs/sec | **199** | `cargo run --release -p bench-harness --bin bench -- device` |
| device | durable appends/sec (WAL op shape) | **215** | as above |
| core | acked writes/sec | **215** | `… -- core --json` |
| core | log records/sec | **215** | as above |
| core | snapshot reads/sec | **2,650,516** | as above |
| core | device ratio | **0.83×** (brackets 222 / 258) | as above |
| kernel | acked writes/sec | **109** | `… -- kernel --json` |
| kernel | log records/sec | **217** | as above |
| kernel | device ratio | **0.84×** (brackets 258 / 242) | as above |
| wire · bench-app | acked req/sec, 1 → 256 conns | **111, 99, 103, 110, 112** | `… -- wire --target bench-app --json` |
| wire · bench-app | p50 latency, 1 → 256 conns | **8.8 ms, 38 ms, 149 ms, 563 ms, 2.27 s** | as above |
| wire · invoicing | acked req/sec, 1 → 256 conns | **199, 202, 201, 170, 185** | `… -- wire --target invoicing --json` |
| wire · invoicing | p50 latency, 1 → 256 conns | **4.9 ms, 19 ms, 76 ms, 348 ms, 1.34 s** | as above |

The full artifact, including p99/p999 and both bracket samples per point, is
`target/bench/report.json` after a run. This run reported no INVALID entries.

### The single most informative row is not in the table

Read the **log-records/sec** column down the layers:

| layer | records/sec |
|---|---|
| device (append-shaped ceiling) | 215 |
| core | 215 |
| kernel | 217 |
| wire · bench-app | 198 – 225 |
| wire · invoicing | 170 – 202 |

Four different code paths — a bare fsync loop, an in-process commit loop, the
full kernel with job execution, and a real HTTP server driven over TCP —
converge on the same number. Adding the kernel halves the *request* rate
(109 vs 215) precisely because each request then costs two durable records
instead of one, and leaves the *record* rate untouched. Adding HTTP, TCP,
session cookies and a child process costs almost nothing on top.

The system is fsync-bound end to end. Everything above the log is, at this
workload, free.

## The prediction's verdict

The spec committed to testing one prediction: *the core write path measures at
≈1× the device's durable-sync rate — sync-bound, far below the headline claim on
any hardware.*

**Confirmed.** Core commits 215 records/sec against a device append ceiling
measured at 215–258/sec in the same invocation. The mechanism is not in doubt
and is visible in the source: `Engine::submit` is `wal.append` → `wal.sync()` →
`apply`, one durable sync per transaction, no group commit.

MANIFESTO §7 and README claim "hundreds of thousands of transactions per second"
on one big box. This instrument measures **215/sec** for the canonical
transaction on this laptop. That gap is roughly three orders of magnitude, and
naming it plainly is the point of building the instrument.

Two things the gap is **not**:

- It is not a Linux-server number. `F_FULLFSYNC` on an Apple SSD is the
  slow end of durable; a server with a battery-backed controller does far
  better per sync. The launch numbers come from bare metal (spec ruling 4) and
  do not exist yet.
- It is not evidence about redb. See below.

**What would close the gap** is group commit — amortizing one sync over a batch
of transactions. The engine already has the shape: the follower's
`Engine::submit_batch` appends every frame in a shipped batch and syncs once.
The primary path deliberately does not use it, because acking before the batch's
sync would break ack-after-durable. Designing a primary-side group commit that
keeps that guarantee is a sub-project, and this harness is what will size it.
The number to beat is in the table.

### DECISIONS §7 trigger (a)

Trigger (a) is "benchmarks show redb limiting the headline throughput claim."
**These benchmarks do not show that, and the LSM engine stays deferred.**

The attribution is available rather than ambiguous, because the core layer sits
*at* the device ceiling: 215 records/sec against 215–258 device appends/sec. If
redb's materialization were costing anything material at this rate, core would
sit well below that ceiling instead of within measurement error of it. Reads
tell the same story from the other side — 2.65 million/sec, four orders of
magnitude above the write path, because reads never touch the log.

Stated with its limits, so this section is not read wider than it is: the core
layer bundles WAL append + sync + apply + store materialization into one number,
so this rules out redb as *the binding constraint at this workload on this
hardware*. It does not measure redb's cost in isolation, and it says nothing
about a workload whose store cost is not swamped by an fsync. A finer
attribution would need per-phase profiling inside the commit path, which this
instrument does not do.

## What these numbers are not

**Not the launch claim.** These are dev-laptop numbers under `F_FULLFSYNC`,
labeled as such. The numbers of record for launch come from a rented bare-metal
Linux box when launch nears.

**Not a comparison with any other stack.** No incumbent was run. Anyone quoting
these against Postgres or anything else is inventing a comparison this document
refuses to make.

**Not a latency guarantee.** Closed-loop tails understate open-loop tails, as
disclosed above.

**Not a claim about scaling with connections.** The kernel is single-threaded;
the connection sweep exists to show queueing behaviour honestly, not to suggest
throughput grows with load. It does not.

## A note on one bracket sample

The invoicing 1-connection point recorded device brackets of 48 and 220
appends/sec either side of its window. The 48 is an outlier — most likely
contention with the child process spawning — and it is disclosed rather than
dropped.

The harness divides by the *larger* of the two samples, so this point's ratio is
198.68 / 219.59 = 0.905, and it passed. That rule is load-bearing here rather
than cosmetic: against the smaller sample the same point reads 4.14, which the
gate's hard limit of 1.5 refuses outright — a bad bracket would have failed the
run in place of a bad server.

The tolerance rule cuts the other way at this point. Tolerance widens with
bracket disagreement, and these two samples disagree by 78%, so the tolerance arm
alone would have admitted a ratio up to 1.78; the 1.5 hard limit is what actually
bounded it. The honest reading is that this point's gate was far more permissive
than the ~1.10 a well-bracketed point gets, and that the hard limit — not the
tolerance — is what kept it bounded at all. The artifact carries both samples for
every point so a reader can check this without taking the summary's word for it.

**This point does not survive the bracket-stability gate, and the row above
stands only because it predates it.** A 78% disagreement is nearly four times the
20% ceiling; re-measured today the point is refused rather than disclosed, which
is the outcome this section was already arguing for in prose. It is left in place
rather than deleted because the reasoning above is the record of how the gate came
to exist — the paragraph that says "this point's gate was far more permissive than
a well-bracketed point gets" is the observation the gate was built from. Treat the
invoicing 1-connection number as withdrawn, not as evidence; every other point in
the table brackets within the ceiling.
