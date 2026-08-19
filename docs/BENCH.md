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

**The gate's numerator was derived too, and for `bench-app` it no longer is.**
The 2026-08-10 group-commit interlude's T1 fix wave set `syncs_per_sec` to
`syncs_per_request * throughput` — the same per-target multiplier, honestly
named for what it bounds — and named the owed work in the same breath: a
group-commit primary path breaks the one-fsync-per-commit premise that
multiplier assumes, so either a server-reported sync counter lands or the gate
gets re-scoped and this doc says so. **The counter landed** (2026-08-14).
`bench-app` wraps its `Disk` in a `wal/`-scoped counting impl and publishes the
tally through a `/__syncs` route; the harness samples it at both window edges
and gates on the delta, so that target's sync rate is COUNTED. `invoicing`
mounts no such route and still derives at 1.0 × throughput. Every point carries
`syncs_counted` so two numbers of different evidentiary standing never sit alike
in one table — and the derived one is now unsound by construction, which is why
every contended invoicing point below is refused rather than published.

**One key in the artifact means two different things by layer. This is a known
defect, recorded rather than left to be inferred.** `device_ratio` is the SYNC
ratio at core and kernel (`core_accounting.rs:278`, `kernel_accounting.rs:219`)
and the RECORD ratio at wire (`wire_accounting.rs:286`); the wire gate itself
fires on the sync ratio, which is published under no name at all. Do not read a
wire `device_ratio` as a sync ratio: at 64 connections below it reads 26.87
while the gated sync ratio is 0.42. Both operands of the honest quantity are in
the artifact — compute `syncs_per_sec / max(device_appends_per_sec_before,
device_appends_per_sec_after)` yourself. Every wire sync ratio in this document
was computed that way.

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

## Numbers of record — dev hardware, before group commit (NOT the launch claim)

**This table is the 2026-08-10 measurement, and it is the baseline the
group-commit section below is read against — not what today's binary produces.**
It is kept because it is the number group commit had to beat and because the
prediction's verdict below is argued from it. `scripts/bench.sh` on today's tree
reproduces the *method*, not these rows: the kernel and wire layers batch now.

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

**Group commit is what breaks that convergence, and the break is the whole
point.** Post-batching the same column reads 222 at core — still a group of one
— and 5,292 at the kernel, because a kernel record rate is no longer pinned to
the device's sync rate. It is `records-per-sync × sync rate`, and batching moved
the first factor from 1 to as much as 64. The system is still fsync-bound; what
changed is how many transactions one fsync buys. The measurement is below.

## The prediction's verdict

The spec committed to testing one prediction: *the core write path measures at
≈1× the device's durable-sync rate — sync-bound, far below the headline claim on
any hardware.*

**Confirmed.** Core commits 215 records/sec against a device append ceiling
measured at 215–258/sec in the same invocation. The mechanism was not in doubt
and was visible in the source: `Engine::submit` was `wal.append` →
`wal.sync()` → `apply`, one durable sync per transaction, and the platform had
no group commit anywhere.

**The second half of that sentence stopped describing the code on 2026-08-15,
and the verdict survives the change — so the sentence is corrected here rather
than left for a reader to trip over.** The production commit path is now
`Db::begin_group` → N × `Group::stage` → `Group::commit`, and `Group::commit`
calls `Engine::submit_all`, which appends every staged record, syncs **once**,
then applies in seq order (`charpente-core/src/engine.rs`,
`charpente-core/src/db.rs`). `Engine::submit` survives only as the length-1
convenience `submit_all(&[payload])` and has no production caller. Core still
pays one durable sync per transaction, but now because `Db::transact` is a
*group of one*, not because the platform cannot batch — and the ≈1x reading
reproduces after batching at 222 records/sec against a 215–261/sec ceiling. What
batching changed is the kernel and the wire, measured under *Numbers of record —
group commit* below.

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

**What closes part of the gap is group commit** — amortizing one sync over a
batch of transactions — and it shipped as M2 sub-project 2 on 2026-08-15. It
keeps ack-after-durable rather than trading it away: `stage` never touches the
WAL, every reply is held until after `commit` returns, and apply stays strictly
after sync, so a crash before the sync loses the whole batch and never part of
it. This harness sized it and the result is below. It closes the *amortization*
component of the gap and none of the device-speed component — three orders of
magnitude was never one problem.

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

## Numbers of record — group commit (2026-08-15)

Same instrument, same box, same workload; the binary batches now. **The claim
these numbers support, in the only form spec §8 permits:** group commit removes
the one-fsync-per-transaction ceiling, and on hardware whose fsync is slow
enough to keep a queue deep it recovers up to ~31x — and nothing at all at one
connection. Never *"group commit makes charpente 31x faster."*

**This device flatters group commit, and that conditioning travels with the
number wherever it is quoted.** A ~215–260/sec `F_FULLFSYNC` ceiling keeps
queues deep at high connection counts, and greedy drain converts queue depth
directly into batch size. A faster device drains queues faster, so queues are
shallower, so batches are **smaller**. The multiple below is therefore closer to
an upper bound for this mechanism than to what a server would show. The
connection sweep is that conditioning quantified rather than asserted: at one
connection the batch is one and the win is 1.02x. The bare-metal Linux run stays
owed (sub-project 1 spec ruling 4) and is the correction that will resize this
result.

**Provenance, because it bounds what may be quoted.** Two full matrix runs on
2026-08-15, windows 14:12:08–14:21:25 and 14:24:00–14:33:02 EDT, artifacts
`task-10-report-run{1,2}.json` under `.superpowers/` (gitignored, so this
document is the durable copy). **Neither run is green on its own** — the box was
not quiet and the bracket-stability gate refused a different layer in each:

| layer | run 1 | run 2 |
|---|---|---|
| core | green (bracket spread 17.65%) | refused (23.04%) |
| kernel | refused (46.05%) | green (17.61%) |
| wire · bench-app 1c | refused (24.9%) | green |
| wire · bench-app 4c | green | green |
| wire · bench-app 16c | green | refused (20.2%) |
| wire · bench-app 64c, 256c | green | green |
| wire · invoicing 1c | green | green |
| wire · invoicing 4c / 16c / 64c / 256c | refused | refused |

Every figure below comes from a point the gate passed, and **no point mixes
runs**: its throughput, its sync rate and its latencies all come from the same
window. That rule is load-bearing rather than tidy — run 2's `bench-app` 64c and
256c points carry p99→p999 excursions of 9.0x and 3.9x that neither the bracket
gate nor the heartbeat guard saw, so a run-1 throughput published beside a run-2
tail would be a composite of two different machines. Both runs exited non-zero;
on this matrix `invalid: []` is unreachable, for the reason the invoicing note
below gives.

### The kernel layer — the primary evidence

Both operands are counted in-process here, which is why this is the headline and
the wire numbers are corroboration.

| metric | before | after | |
|---|---|---|---|
| acked writes/sec | 109 | **2,646** | 24.3x |
| log records/sec | 217 | **5,292** | 24.4x |
| durable syncs/sec | 217 | **85** | |
| device ratio (syncs ÷ ceiling) | 0.84x | **0.36x** | |
| 50,000 canonical requests | 460 s | **18.9 s** | |

From run 2, the green kernel window. Run 1 was refused by the bracket gate and
agreed anyway (5,697 records/sec, 92 syncs/sec, 17.55 s), which is an
observation, not a second measurement. "Before" reads syncs off the record rate
because pre-batching the two were the same quantity — one record, one fsync.

`jobs_done == requests_total == 50,000` in both runs. The dual-write invariant
holds under batching, so this is not fewer syncs bought by dropping records.

**Amortization is argued from two sides, because the ceiling test alone cannot
establish it.** A sync-skipping engine also reports a low sync rate, so "syncs
fell" is consistent with both the feature and a durability bug. The two bounds
together are not:

- **Every counted sync rate stays below the device ceiling.** The largest
  counted ratio anywhere in either run is **0.9388** (`bench-app`, 1 connection,
  run 2). Nothing acked faster than the device could sync.
- **The batch factor stays below its cap.** The kernel commits **61.96 records
  per durable sync** — 61.9602 in run 1 and 61.9614 in run 2 — against
  `MAX_BATCH = 64`, which is enforced by an assertion in `Group::stage` that
  stays on in release. A skipped sync would show a factor above the cap, not
  below it.

The reconciliation closes to the record: the same fixed workload paid **1,614 ±
1 durable syncs in both runs**, and 2 arms × ⌈50,000 / 64⌉ = 1,564 of those are
batch syncs, the remaining ~50 being store checkpoints and `.horizon` writes
that the kernel's unscoped counter also tallies. That unscoped counter is why
61.96 is a **lower bound** on the true batch factor: the wire's counter is
`wal/`-scoped and the kernel's is not, while both divide by the same
append-shaped bracket.

### The wire layer — the connection sweep

`bench-app` only. Its sync rate is COUNTED through `/__syncs`; its record rate is
derived at 2.0 × throughput, so the records-per-sync column is a derived
numerator over a counted denominator.

| conns | acked req/sec | vs. before | durable syncs/sec | syncs ÷ ceiling | records/sync | p50 | p99 | run |
|---|---|---|---|---|---|---|---|---|
| 1 | 113 | 1.02x | 226 | 0.94x | 1.0 | 8.2 ms | 17 ms | 2 |
| 4 | 475 | 4.80x | 238 | 0.92x | 4.0 | 8.0 ms | 13 ms | 1 |
| 16 | 1,590 | 15.44x | 199 | 0.76x | 16.0 | 10.0 ms | 19 ms | 1 |
| 64 | 3,462 | **31.47x** | 108 | 0.42x | 64.0 | 17.2 ms | 41 ms | 1 |
| 256 | 2,792 | 24.93x | 88 | 0.41x | 63.2 | 85.7 ms | 175 ms | 1 |

"vs. before" is against the pre-group-commit table's `111, 99, 103, 110, 112`
req/sec at the same connection counts. The `syncs ÷ ceiling` column is computed
here from `syncs_per_sec` and the point's own brackets; it is **not** the
artifact's wire `device_ratio`, which is the record ratio (method section
above).

**Read the curve, not the peak.** The win exists only where the queue is deep
enough to fill a batch — 1.0 records per sync at one connection, 4.0 at four,
16.0 at sixteen, the cap at sixty-four. Past that, 256 connections buy queueing
delay rather than capacity (the kernel is single-threaded, as the disclosures
above say): throughput turns over, the batch falls back off its cap to 63.2, and
the p50 goes to 85.7 ms. One point of the curve quoted alone is the thing this
section exists to prevent.

**Records/sync at 256 connections in run 2 reads 64.17, over a cap of 64.** That
is the derived numerator's error, not a batch that broke its bound — the
denominator is counted, the numerator is `2.0 × throughput`, and the assertion
that enforces the cap runs inside the process. It is left visible here because
it is the cleanest evidence in the artifact for which half of the wire ratio is
evidence and which half is arithmetic.

### `examples/invoicing` is refused at every contended point, on purpose

Eight of its ten points across the two runs are INVALID and the harness is right
to refuse them. invoicing mounts no `/__syncs`, so its sync numerator falls back
to 1.0 × throughput — and group commit is exactly what makes that multiplier
unsound. The harness reads 2.06x–30.49x the device ceiling and refuses its own
numerator, which is precisely what the owed-work paragraph in
`wire_accounting.rs` predicted it would do. Only the 1-connection point is green
(215 and 226 req/sec across the two runs), because a batch of one is where the
multiplier still holds. **No invoicing speedup is published here.** Its
pre-group-commit 1-connection baseline row is separately withdrawn — last
section.

### Core is unchanged, and that is the correct result

| metric | before | after |
|---|---|---|
| acked writes/sec | 215 | 222 |
| log records/sec | 215 | 222 |
| device ratio (syncs ÷ ceiling) | 0.83x | 0.85x |
| snapshot reads/sec | 2,650,516 | 2,527,819 |

From run 1, the green core window. `Db::transact` is a group of one —
`begin_group`, one `stage`, `commit` — so a serial loop has nothing to
amortize; batching acts through `Group::commit`, which is what the kernel calls
when a step drains more than one item. A core layer that had "improved" here
would mean the batch-of-one path had stopped being the pre-batching path, which
spec §3 makes an acceptance criterion rather than a win.

### What this measurement does not establish

- **Not a platform claim.** DECISIONS §6's measure-first discipline is satisfied
  on the mechanism and **not** on the platform: MANIFESTO §7's bet is about a
  server and no server has been measured. The three-order gap named above has
  two components, device speed and amortization; only the second is addressed
  here.
- **Not a settled box.** The bracket instability that refused a layer in each
  run is **undiagnosed**. A foreign workload was observed on the machine during
  the session, but it post-dates run 1's refusals by at least 2m43s and cannot
  explain them. Both headline points are also the closest passing points to
  refusal in the whole matrix (17.65% and 17.61% against a 20% ceiling, and
  against the 15.7% worst clean spread this document records elsewhere).
- **Not a latency result.** The closed-loop disclosure above applies unchanged,
  and batching raises what it hides: up to 64 requests now share one fsync, so a
  request's tail is a property of the batch it landed in. The two run-2
  excursions named under Provenance are the visible edge of that and were seen
  by no gate.

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
