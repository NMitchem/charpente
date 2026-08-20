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
mounts no such route and derives at 1.0 × throughput. The derived form is
unsound by construction under batching, which is why every contended invoicing
point below was refused rather than published.

**Since 2026-08-19 the derived form is not published either, and this paragraph
describes the runs below rather than today's harness (T13, Noah ruling (b)).**
The gate now runs on a COUNTED sync rate and on nothing else, at unchanged
strictness — a counted WAL sync rate above the counted WAL ceiling still
refuses — and a derived rate is WITHDRAWN: `null` in the artifact,
`n/a (derived, unsound under batching)` in the human render. The reasoning is
that a physical-plausibility gate applied to `syncs_per_request × throughput`
tests the CONSTANT rather than the system, and group commit made that constant
false at every contended point, so the gate could not fail to fire. What settled
it was an asymmetry the harness had been carrying without noticing: the wire
`device_ratio` is `records_per_request × throughput / ceiling`, the same shape of
derivation, and it is published ungated at 3.4x the ceiling — two derived
quantities, identical form, opposite treatment. So the numbers below stand as
measured, and a re-run today would produce the same invoicing throughput and
latencies with no sync rate beside them and no refusal.

**Today's harness marks which is which; the artifacts these numbers come from
do not.** Every point emits a `syncs_counted` flag — the counter itself landed
2026-08-14, the flag that reports which kind a point is landed 2026-08-15 — so
two numbers of different evidentiary standing never sit alike in one table. Both
runs published below finished about a quarter of an hour before that second
commit landed and **carry no such key**, so counted was told from derived here
by arithmetic instead: a derived figure is exactly
`syncs_per_request × throughput`, and `invoicing`'s `syncs_per_sec` equals its
throughput to the digit at all ten of its points while `bench-app`'s differs
from `2 × throughput` at all ten of its. That test is the whole basis for
treating the `bench-app` sync rates below as measured and the invoicing ones as
arithmetic; it is stated rather than assumed, because re-running the matrix on
today's harness would answer it by inspection and nobody re-ran it.

**One key in the artifact means two different things by layer. This is a known
defect, recorded rather than left to be inferred.** `device_ratio` is the SYNC
ratio at core and kernel (`core_accounting.rs:278`, `kernel_accounting.rs:219`)
and the RECORD ratio at wire (`wire_accounting.rs:286`); the wire gate itself
fires on the sync ratio, which the JSON artifact publishes under no name at
all. The human render does print it — `…x dev (sync, gated)` beside
`…x dev (records, ungated)`, computed inline from the same line's fields — so a
reader of the terminal output and a reader of the JSON are not looking at the
same set of quantities. Do not read a wire `device_ratio` as a sync ratio: at 64
connections below it reads 26.87 while the gated sync ratio is 0.42. Both
operands of the honest quantity are in the artifact — compute
`syncs_per_sec / max(device_appends_per_sec_before, device_appends_per_sec_after)`
yourself. Every wire sync ratio in this document was computed that way.

Since T13 this applies to COUNTED points only. A point whose sync rate was
withdrawn carries `"syncs_per_sec": null` in the artifact and prints
`n/a (derived, unsound under batching)` with no sync ratio at all — there is no
gate on that arm, so there is no gated ratio to show. The records ratio is
printed in both arms, unchanged and still ungated.

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
  which is an order-of-magnitude effect. **At the wire layer it applies to a
  COUNTED sync rate only** (T13, 2026-08-19): where a target reports no count,
  the rate would be `syncs_per_request × throughput`, which batching makes
  false, so it is withdrawn from the artifact rather than published and gated.
  Gating it would have tested the multiplier, not the system. Core and kernel
  count their own syncs in-process and are unaffected.
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
after sync, so no record is ever acked before it is durable. (The batch is not
durability-atomic — one straddling a segment boundary can leave a durable
prefix, since `Wal::rotate` syncs the old tail mid-append. Nothing was acked, so
no guarantee moves; DESIGN §2 carries it.) This harness sized it and the
result is below. It closes the *amortization* component of the gap and none of
the device-speed component — three orders of magnitude was never one problem.

### DECISIONS §7 trigger (a)

Trigger (a) is "benchmarks show redb limiting the headline throughput claim."
**These benchmarks do not show that on the path they measure, and the LSM engine
stays deferred.** That path is the *unbatched* one, and the scoping note at the
end of this section is where that stopped being the whole story.

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

**Superseded in scope on 2026-08-20, and correct as written for the workload it
was taken on.** Group commit is exactly the workload whose store cost is no
longer swamped by an fsync, and the per-phase profiling this paragraph says the
instrument does not do now exists — see *"Where a batched commit's time goes"*
below. On the batched path the store *path* holds more of the commit window
than the WAL path does as a **group**. The spec's pre-committed criterion,
however, is field-level and singular — "redb apply is the **largest single
non-idle phase** at batch cap" — and at the field level the largest phase in
every published run is still the durable sync, `wal_sync_io` at 40.5–41.2%,
against `gap_within_store`'s 39.6–40.1%. A counted phase ahead of a residual
one, on all three runs. **Trigger (a) therefore does not fire on the reading the
spec pre-committed to**, and the deferral holds on fresh evidence rather than on
the 2026-08-10 reading. Two readings of the criterion's own wording would reverse
that — "phase" as one of `submit_all`'s three blocks rather than one of the
fourteen trace fields, and "non-idle" as excluding the device-blocked fsync — and
both are put to Noah in the draft rather than settled here.
`DECISIONS.md` §7 is untouched pending Noah's sign-off,
per CLAUDE.md's reopen-don't-override rule; the draft re-reading is
`.superpowers/sdd/2026-08-19-phase-attribution/trigger-a-rereading.md`. Nothing
in the paragraphs above is withdrawn — the core layer really does sit at the
device ceiling, and that is reproduced below at 90.3–90.6% of its window inside
`wal_sync_io`.

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
on this matrix, **as the harness stood on 2026-08-15**, `invalid: []` was
unreachable, for the reason the invoicing note below gives. T13's ruling (b)
removed that: a derived sync rate is no longer gated, so those four invoicing
refusals would not fire on today's harness and `invalid: []` is reachable again.
The green map above is the record of what those two runs did, not a statement
about the current instrument.

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

`bench-app` only. Its sync rate is COUNTED through `/__syncs`; its record rate
is derived at 2.0 × throughput, so the records-per-sync column is a derived
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

### `examples/invoicing` was refused at every contended point, and the refusal was replaced rather than relaxed

Eight of its ten points across the two runs are INVALID and the harness was
right to refuse them **on the evidence it had at the time**. invoicing mounts no
`/__syncs`, so its sync numerator falls back to 1.0 × throughput — and group
commit is exactly what makes that multiplier unsound. The harness read
2.06x–30.49x the device ceiling and refused its own numerator, which is
precisely what the owed-work paragraph in `wire_accounting.rs` predicted it
would do. Only the 1-connection point is green (215 and 226 req/sec across the
two runs), because a batch of one is where the multiplier still holds. **No
invoicing speedup is published here.** Its pre-group-commit 1-connection
baseline row is separately withdrawn — last section.

**What changed on 2026-08-19 (T13, Noah ruling (b)), and why this is not a
relaxation.** Refusing an unsound number every single time is not a gate: a
check that must fire teaches a reader to skip refusals, and `invalid: []` had
become unreachable, so a genuine regression in this matrix would have looked
exactly like the expected failure. The instrument no longer computes that
numerator for publication at all — a derived sync rate is WITHDRAWN (`null` in
the artifact, `n/a (derived, unsound under batching)` in the human render) and
reaches no gate, while a COUNTED rate is gated at exactly the strictness it
always was. Nothing was widened: `RATIO_TOLERANCE_FLOOR` is untouched, and a
counted WAL sync rate above the counted WAL ceiling still refuses. The four
invoicing refusals recorded above would therefore not fire today, and the
absence of a sync figure beside invoicing's throughput is the point — it says
the harness cannot see that quantity from outside the process, which is true,
where the old refusal said it could see it and disbelieved what it saw.

**What is still owed, unchanged by the above.** A counted sync rate for
`examples/invoicing` needs a server-reported counter it does not have. Option
(a) of T13's investigation costs the example its single-dependency-on-`charpente`
property (`CountingDisk` lives in `bench-harness`), needs an unauthenticated
introspection route in an app whose every other route carries `auth`, and
instruments the one target whose evidentiary value is that it is a stock,
uninstrumented real application. It was investigated and deliberately not taken;
withdrawal is the honest interim, not a substitute.

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

## Where a batched commit's time goes — per-phase attribution (2026-08-20)

Reproduce with `cargo run --release -p bench-harness --bin bench -- kernel --json
--requests 50000 --overhead-ns-per-op 586` (and `-- core --json
--overhead-ns-per-op 586` for the batch-1 column); the overhead figure itself
comes from `bench kernel --trace-overhead`.

The connection sweep above leaves one number unexplained. At 64 connections the
batch is exactly at `MAX_BATCH`, p50 is 17.2 ms, and the device is **58% idle**
— `syncs ÷ ceiling` of 0.42x. That section explains the *256*-connection
turnover and does not attribute the 64-connection gap at all. This section
attributes it.

**The answer, before the method — at two levels, which do not agree, and the
disagreement is the result.**

*Field by field.* The durable sync is still the largest single phase at batch
≈62: `wal_sync_io` at **40.48 / 40.53 / 41.15%** of the window, with
`gap_within_store` — the store-side apply residual — second at **40.05 / 39.81 /
39.62%**. Group commit cut the sync's share from ~90% to ~40%; it did not
displace it from first place, and the two are 0.4 points apart.

*Grouped into `submit_all`'s three blocks.* **The store path holds 55.1–55.9%
of the commit window and the WAL path 42.1–43.0%, while everything outside the
commit path — the single-threaded kernel loop, dispatch, the handler, job
execution — holds 1.27–1.30%.** On the unbatched path the same instrument reads
91.3–91.5% WAL and 7.95–8.17% store.

What is settled either way: group commit did **not** move the bottleneck to the
kernel's single-threaded serialization, which is ~1.3% of the commit window here
and ~1% in every machine state measured below. What it produced instead is a
window with **two comparable occupants** — one durable sync and the apply run
that follows it — and which of the two "leads" depends on whether you count
fields or blocks. The 0.4-point field-level margin is well inside this
instrument's own noise amplitude (5.5% of the window, below), so at the field
level the ordering is not resolvable in either direction. The 13.7-point group
margin sits outside that amplitude and is real — but it belongs to a grouping,
and the criterion the trigger-(a) note above is read against asks about a single
phase. Both readings are published here so the difference is visible rather than
chosen.

### How the phases are measured, and why no clock went into the core

Hard rule 1 forbids a wall clock inside `charpente-core`, so `Engine::submit_all`
is not timed from the inside. It does not have to be. redb is mounted behind
charpente's own `Disk` trait — `DiskBackend` implements redb's `StorageBackend`
over a `File` (`charpente-core/src/table.rs`) — and the WAL goes through the same
trait, so **every byte either one touches crosses a seam the harness already
owns.** The harness wraps `ProdDisk` in a `TracingDisk`
(`crates/bench-harness/src/tracing_disk.rs`) that records `(path class, op kind,
start, end)` per call and decides nothing; all arithmetic lives in
`phase_accounting.rs`, which the tracer never calls.

Two facts turn the recording into an attribution. First, the op *pattern*
recovers the phases with no core change: `submit_all` is N appends to `wal/`, one
`wal/` sync, then N applies against `tables.redb`, so a path-class transition is a
phase boundary. Second — the load-bearing one — **the gaps between recorded ops
are attributable only because the arm is single-threaded.** The core and kernel
arms are in-process and serial; the wire arm is not, and `attribute` refuses an
overlapping trace by name rather than publishing a share nobody could defend.

The partition is exact by construction and asserted: the fourteen fields sum to
`window_end_nanos - window_start_nanos` on every run, and a window stamped from
the wrong place is caught from two directions — an event straddling a boundary is
refused, and a window wider than the caller's own independently-measured
`Instant` elapsed time is refused (`window_brackets_measured_section`).

Two arms, chosen for the contrast the question needs. **core — batch 1**:
`Db::transact` is a group of one, so one durable sync per record; this is the
workload the 2026-08-10 trigger-(a) reading was taken on. **kernel — batch ≈62**:
the kernel drains what is ready and commits it as one group, at 62.0 log records
per durable sync in every run below — the same quantity the group-commit section
above publishes as 61.96, and a **lower bound** for the same reason it gives
there: `kernel_bench`'s sync counter is unscoped and tallies store checkpoints
and `.horizon` writes alongside batch syncs. It is used here only to establish
that every run below committed the same batch shape, never as a denominator for
a phase share. The wire arm is not instrumented — it is multi-threaded and out of
process, so its trace would be refused rather than published.

### The instrument's own cost: below the noise floor of n=9, and not "0 ns/op"

`bench kernel --trace-overhead` runs the kernel arm alternately with the trace
recording and passed through, and reports **CPU** time per pair — a descheduled
process accumulates wall clock it never spent computing, and instrument overhead
is compute. Three invocations, nine alternating pairs, every one reported:

| invocation | ns of CPU per recorded op, per pair | cpu/wall |
|---|---|---|
| 1 | −32.2, −211.3, +60.6 | 0.49–0.50 |
| 2 | −1285.2, +68.6, +585.9 | 0.49–0.58 |
| 3 | +139.7, −63.4, +292.1 | 0.50–0.52 |

Pooled: n=9, mean **−49.5**, median +60.6, range **[−1285.2, +585.9]**, four
pairs negative. Dropping the single pair whose two runs disagree in `cpu/wall`
(invocation 2's first — the exclusion criterion is `run_trace_overhead`'s own
doc, not hindsight) gives n=8, mean +105.0, range [−211.3, +585.9], three
negative. **Both are published because the verdict is identical either way, which
is the only thing that makes the exclusion safe to mention.**

**The result is that the trace's own cost is below this machine's noise floor at
n=9 — not that it is zero.** A negative pair means passthrough measured slower
than recording, which no real cost produces; four of nine is noise, not a small
effect. Task 4's `--trace-overhead` result (range [−770.8, +619.9] on a contended
box) is improved on but not resolved.

Every phase block below is therefore published with
`phases_overhead_gated: true`, `phases_overhead_gate_ns_per_op: 586` — the high
end of the observed range — and `phases_overhead_gate_resolvable: false`. 586
ns/op is a **noise amplitude, not a measured cost**, so the refusals it produces
mean *this machine cannot certify these phases at that amplitude*, never *the
instrument is too expensive*. At 586 ns/op the gate refuses **every** IO phase of
**every** published kernel run — `wal_sync_io`, the largest field in that column,
included — and every IO phase of the core arm **except its own `wal_sync_io`**
(plus `store_write_io` on core run 2, which clears the threshold by a hair). The
kernel column is published entirely uncertified at that amplitude; that is what
each artifact's `invalid` array says, and the table below now carries the gate
outcome per row so the two statements cannot drift apart. The gap fields are
outside the gate by design, because an IO phase has a referent outside the trace
and a gap is defined as a residual.

That bound is worth stating in the direction that matters, and **it does not
spread across the fourteen fields.** 586 ns/op × 1.334 M recorded ops is 782 ms
against a 14.1 s kernel window — 5.5% of the window — but two facts about the
instrument decide where it lands, and both are in the code's own words.
`Trace::record` samples `end` *before* the mutex-guarded push, so recording's own
cost falls into the gap that FOLLOWS an op and never into the IO phase it timed
(`tracing_disk.rs`; `phase_accounting`'s gate-scope doc: "by construction … it
becomes part of what that gap IS"). And the ops are not evenly distributed:
100,000 log records at 62.0 records/sync is ≈100 k WAL appends plus ≈1.6 k WAL
syncs, so ≈102 k of the 1.334 M recorded ops are WAL ops and **≈92% are on
`tables.redb`**. Roughly 92% of the bound therefore lands in one field:
**≈720 ms, or 12.7% of `gap_within_store`** and ≈5.1 points of window — and
`gap_within_store` is the one field the gate never checks.

Correcting for it moves the reading against the store side, not toward it.
Subtract 720 ms from `gap_within_store` and from the window and run 1 reads
store path ≈53.5% against WAL ≈44.4%; at the field level `wal_sync_io` goes to
≈42.7% against `gap_within_store`'s ≈36.8%, so the field-level margin **widens**
from 0.4 points to ≈5.8. The correction can only push that way, because the sink
is the store-side residual.

The bound is generous, and one field says so arithmetically: ≈100,000 append
recordings at 586 ns is ≈59 ms of claimed cost against a `gap_within_wal` that
totals 22.4 ms in run 1 — larger than the whole field it would have to land in.
That is what "noise amplitude, not a measured cost" means when it is spent. It
is still the figure every block is gated at, because publishing against the
generous end is the conservative choice.

What survives all of it: 782 ms cannot convert a 1.3% phase into a 40% one, so
the kernel loop's ~1.3% and the 54-point gap over it stand, as does the 13.7-point
group margin. The 0.4-point margin between the two largest *fields* does not
survive it, in either direction — which is why this section reports those two as
comparable rather than ranked, and why the gate refusing them is the gate being
right rather than strict.

**A correction to Task 4's own reading, recorded because it was wrong in this
document's working notes and would otherwise be inherited.** Task 4 saw
`cpu/wall` of 0.41–0.52 and read it as contention: "this process got roughly half
a core." The runs above read 0.49–0.52 on a box sampled at 91.9–93.0% idle with
no process over ~5% CPU. **0.50 is this arm's structural signature, not a
contention read** — the kernel arm spends about half its wall time blocked inside
`fsync`, which burns no CPU. The contention read that survives is the *spread*
between the two arms of a pair, not the level of either.

### The table

One row per phase, share of that arm's own measured window, **all three
observations shown rather than an average**. Nanosecond totals are in the JSON
artifact; shares are what the question is about.

| phase | core, batch 1 — % of window (3 obs) | kernel, batch ≈62 — % of window (3 obs) | provenance of the ns total the % is taken from | overhead gate at 586 ns/op — core · kernel |
|---|---|---|---|---|
| `wal_append_io` | 0.86 / 0.95 / 0.80 | 1.50 / 1.75 / 1.68 | counted (traced IO) | refused 3/3 · refused 3/3 |
| `wal_read_io` | 0.00 / 0.00 / 0.00 | 0.00 / 0.00 / 0.00 | counted; zero by design on the steady-state commit path | not gated (phase is exactly zero) |
| `wal_sync_io` | **90.60 / 90.28 / 90.51** | **40.48 / 40.53 / 41.15** | counted (traced IO) | **passed 3/3** · **refused 3/3** |
| `store_write_io` | 1.98 / 2.21 / 1.99 | 11.53 / 11.49 / 11.21 | counted (traced IO) | refused 2/3 (passed on run 2) · refused 3/3 |
| `store_read_io` | 0.00 / 0.00 / 0.00 | 0.00 / 0.00 / 0.00 | counted (traced IO) | refused 3/3 · refused 3/3 |
| `store_sync_io` | 0.02 / 0.01 / 0.01 | 1.33 / 1.33 / 1.31 | counted (traced IO); checkpoints only | refused 3/3 · refused 3/3 |
| `other_io` | 0.01 / 0.01 / 0.01 | 0.64 / 0.65 / 0.64 | counted (traced IO); the `.horizon` breadcrumb | refused 3/3 · refused 3/3 |
| `gap_within_wal` | 0.01 / 0.02 / 0.02 | 0.16 / 0.15 / 0.15 | residual between two traced ops | outside the gate by design |
| `gap_wal_to_store` | 5.90 / 5.89 / 6.04 | 2.93 / 2.95 / 2.90 | residual | outside the gate by design |
| `gap_within_store` | 0.05 / 0.06 / 0.05 | **40.05 / 39.81 / 39.62** | residual | outside the gate by design — and the sink for ≈92% of the instrument's own bound |
| `gap_store_to_wal` | 0.54 / 0.55 / 0.55 | 1.30 / 1.28 / 1.27 | residual | outside the gate by design |
| `gap_other` | 0.02 / 0.02 / 0.02 | 0.06 / 0.07 / 0.06 | residual | outside the gate by design |
| `lead_in` | 0.00 / 0.00 / 0.00 | 0.01 / 0.00 / 0.01 | residual (window edge) | outside the gate by design |
| `tail` | 0.00 / 0.00 / 0.00 | 0.00 / 0.00 / 0.00 | residual (window edge) | outside the gate by design |
| **window** | 30.004 / 30.001 / 30.004 s | 14.106 / 14.042 / 14.434 s | counted | — |
| **traced ops** | 102,963 / 102,391 / 102,979 | 1,334,073 / 1,333,354 / 1,333,692 | counted | — |

Every figure in the "counted" rows is a sum of durations the tracer timed at the
`Disk` boundary. Every "residual" row is *the time that was not any traced op* —
measured as a subtraction, which is exactly what makes the reading below an
inference rather than a measurement. **The two percentage columns are derived**
(a counted phase over a counted window), which is why the provenance column
labels the ns total behind the percentage rather than the percentage itself; no
share here is built on the sweep table's `records/sync`, which is already a
derived numerator over a counted denominator, and **no ratio here crosses
layers.**

**The gate column is provenance too, and it is why the two largest kernel rows
are printed without a winner.** The gate outcome is per row and per run, so it
belongs beside the number rather than in prose that can drift from it: at 586
ns/op the kernel arm's `wal_sync_io` — the largest field in that column — is
refused on all three runs, exactly like every other kernel IO row, while the core
arm's `wal_sync_io` passes on all three. Every published kernel observation
therefore exited non-zero, by design. A refusal means *this machine cannot
certify this phase at that amplitude*; it does not withdraw the number, and the
`invalid` array in each artifact carries the same list.

Both arms' throughput for the same windows: core 221.6 / 220.7 / 221.5 records/s
at `device_ratio` 0.891 / 0.902 / 0.929; kernel 7,089 / 7,121 / 6,928 records/s
at `device_ratio` 0.429 / 0.425 / 0.419, **62.0 log records per durable sync in
all three**. These are new observations on a quieter box than the 2026-08-15
matrix, and they do **not** revise its rows — that table's figures come from its
own windows and stay as measured.

### The reading: which gap is redb, and which is the kernel

This is the inference the spec called the primary design requirement, and it is
argued here rather than encoded in a field name. `phase_accounting`'s module doc
is explicit about why: "This module reports where the time went. It does not
decide whose CPU runs during a gap — that reading belongs in `docs/BENCH.md`
where it can be argued with, not buried in a field name or a test message."

The separation the trace supports is **positional**. It says where in the commit
path the process was, and from that infers whose code was most likely running:

- **`gap_within_store` is the redb-side candidate.** Between two consecutive
  `tables.redb` ops, inside one batch's apply run, the stack is `Store::apply`
  and redb beneath it — `begin_write`, the B-tree descent, page allocation and
  dirtying, the `Durability::None` commit — plus charpente's own per-record
  dispatch through the `Schema` registry. The kernel loop cannot run here; it is
  blocked inside `Group::commit`.
- **`gap_store_to_wal` is everything OUTSIDE the commit path** — last store op of
  one batch to first WAL append of the next. That is where the kernel loop lives:
  dispatch, the handler, job execution, response construction, ready-set
  scheduling — but **not** HTTP decode, which this arm does not do at all
  (`kernel_bench` pushes pre-built `Request` structs), so no decode cost is being
  claimed here. It is the field that answers the kernel-serialization candidate
  separately from redb, and it reads **1.27–1.30%** of the window.
- **`gap_wal_to_store` is the sync→apply transition**, paid once per batch rather
  than once per record — 2.90–2.95% at batch ≈62 against 5.89–6.04% at batch 1,
  which is amortization behaving exactly as it should.
- **`gap_within_wal` is WAL framing CPU** between two appends of one batch.

Grouped that way, the batched commit window is **store path 55.1–55.9%, WAL path
42.1–43.0%, outside the commit path 1.27–1.30%**, with the remaining ~0.7% in
`other_io`, `gap_other` and the window edges. Against batch 1: WAL 91.3–91.5%,
store 7.95–8.17%, outside 0.54–0.55%.

**So the 58% idle device at batch cap is not the kernel's serialization**, and
the balance sits between the durable sync and `Store::apply` rather than in any
one of them. The spec named four candidates for that gap; this measurement puts
the kernel loop at ~1.3% of the commit window and the WAL append at ~1.7%, leaves
the wire edge unmeasured by construction, and splits the remainder ~40/40 between
`wal_sync_io` and `gap_within_store`.

**The transfer from this arm to the sweep's 64-connection point needs its own
argument, and there is one.** The 58% figure is the *wire* sweep's; this arm has
no TCP and pushes pre-built `Request` structs, so two of the spec's four
candidates — the wire edge, and whatever serialization the wire edge adds — are
absent here by construction. What licenses the transfer is published two pages
above and is worth citing here rather than assuming: the kernel arm independently
reproduces a `device_ratio` of **0.419–0.429**, the same 0.42x the sweep records
at 64 connections, **with no wire at all**. The wire edge is therefore not
required to produce the gap. That does not prove it contributes nothing at 64
connections; it establishes that the gap survives its removal, which is the claim
this section needs.

Two things sharpen that, and one weakens it.

**Sharpening (1): the store-path cost per record FELL under batching, while its
share rose.** Core spends 359–370 µs of store-path time per log record; kernel
spends 78.0–79.5 µs. The spec's §2 argument — that redb's share of a commit rises
with batch size because `Store::apply` opens a write transaction per record while
the fsync is amortized 62× — is confirmed for the *share* and falsified for the
*absolute cost*, which is a better result than the argument predicted. **The
mechanism is amortization of the sync→apply transition, not cache warmth**, and
the decomposition says so — per log record, from run 1 of each arm (the other two
runs of each are within a few percent):

| field | core (batch 1) | kernel (batch ≈62) | Δ |
|---|---|---|---|
| `gap_wal_to_store` | 266.5 µs | 4.14 µs | **−262.3** |
| `store_write_io` | 89.5 µs | 16.3 µs | −73.2 |
| `store_sync_io` | 0.8 µs | 1.9 µs | +1.1 |
| `gap_within_store` | 2.0 µs | **56.5 µs** | **+54.5** |

262 of the 280 µs fall — 94% of it — is `gap_wal_to_store` alone, and *per batch*
that field is 266 µs at batch 1 against 257 µs at batch ≈62 — one fixed cost
divided by 62,
which is the same amortization the bullet two above already names. Meanwhile the
field this section reads as redb's compute, `gap_within_store`, **rises 24× per
record** under batching. Cache warmth does not predict that; per-batch
amortization predicts all of it. This is a per-record derivation (window ÷
counted records) across two arms with different workload shapes — the kernel arm
commits two records per request, the core arm one — so treat it as an
order-of-magnitude statement, not a coefficient.

**Sharpening (2): `gap_within_store` behaves like CPU under a CPU load, which is
evidence it *is* CPU.** Under 24 synthetic spinners (below), the kernel arm's
`gap_within_store` rises from 39.6–40.1% to 46.6–55.4% of the window while
`wal_sync_io` falls from ~40.5% to ~19.7%. Time that was idle or device-bound
would not grow when cores are taken away. Time that is compute would, and does.

**Weakening: `gap_within_store` is a residual, and residuals do not name their
occupants.** redb's in-memory work never reaches the `Disk` boundary at all, so
it is only ever visible here, and inside this field it cannot be separated from
allocator time, page-fault time, or charpente's own `Schema`-registry dispatch
and row serialization. The instrument times IO directly and CPU only by
subtraction, and a subtraction cannot say what it subtracted. **Reading
`gap_within_store` and `gap_wal_to_store` as "redb's apply work" is a gloss on a
positional argument, not a measurement**, and the next measurement that would
sharpen it is a CPU profile of the apply path, which this instrument does not do.

### Three controls

**1. The loaded box, and the gate defect a quiet-only run would have hidden.** The
protocol required both arms on a loaded box as well as a quiet one, on the
`MIN_MEASURED_WINDOW` precedent: a loaded box makes some plausibility gates
*pass*, so deferring the smoke to a quiet machine is what hides that class of
bug. The load is synthetic and deliberately CPU-only — **24 `awk` spinners, twice
the 12 cores, with no filesystem access of any kind**, because a disk-heavy load
would perturb the very `Disk` boundary the instrument measures.

It reproduced the precedent exactly. At 586 ns/op the overhead gate refuses the
kernel arm's `wal_sync_io` on every quiet run — 782 ms of overhead against a
threshold of 10% of the phase, i.e. 569–594 ms — and **passes it on loaded run 2**
(alongside `store_write_io`), where contention stretched the window to 43.7 s and
the sync phase to 8.36 s, clearing the 836 ms threshold by 55 ms. Nothing
about the instrument got cheaper; the machine got slower, and a threshold defined
as a fraction of a measured phase moved with it. A quiet-only run would have
recorded that gate as uniformly strict.

The loaded runs also push the *device-ratio* gate toward its limit from the other
side: the core arm reads 0.968 / 0.971 / 0.973 loaded against 0.891 / 0.902 /
0.929 quiet, because CPU contention barely touches an fsync-bound write loop
while it collapses the read loop in the same window (2.24–2.25 M reads/s quiet,
448 K–898 K loaded). The bracket-stability gate did not fire in either arm — the
device is indifferent to CPU load, which is the correct behaviour and worth
having observed rather than assumed.

**2. The machine's own state, which moves the share and not the kernel loop.**
The same shipping *configuration* — two separate compilations in two target dirs,
not one binary — on the same 50,000-request workload, run before and after the
24-spinner arm, reads materially differently: 14.04–14.43 s per window before,
17.62–21.16 s after. The most likely cause is thermal — the spinners ran every
core flat out for twelve minutes on a laptop — and the correct response is not to
pick a number but to report what the share does across states:

| machine state | store path | WAL path | outside the commit path |
|---|---|---|---|
| quiet, before the loaded arm (3 obs) | 55.05–55.86% | 42.13–42.98% | 1.27–1.30% |
| quiet, after the loaded arm (6 obs) | 56.95–64.47% | 33.35–40.81% | 1.08–1.29% |
| 24 CPU spinners (3 obs) | 73.67–76.27% | 21.42–23.43% | 1.34–2.51% |

**The store path is the largest *group* in every state, and the kernel loop is
about 1% in every state.** The exact share is machine-dependent and is not a
constant; the group ordering is not close in any state.

**The single-*field* ordering, which is what the pre-committed criterion asks
about, is not state-stable and is worth recording as such.** In the published
quiet era `wal_sync_io` leads `gap_within_store` on all three runs (40.5–41.2%
against 39.6–40.1%). In the post-thermal era `gap_within_store` leads in five of
six runs, and under 24 spinners it leads in all three — because contention adds
CPU time to a residual while leaving an fsync alone. Those two eras are published
here as *controls* on a degraded machine, not as the measurement; the table above
publishes the quiet pre-heat era, which is both the fastest and the least
favourable to a store-side reading — the conservative choice. The honest summary
is that a 0.4-point field-level margin is not a stable fact about this system in
either direction, and no verdict should be built on its sign.

**3. redb's debug assertions, and how much of the store path they are.** This
workspace forces `[profile.release.package.redb] debug-assertions = true` on
purpose, to keep the seed-replay promise identical across build modes and redb's
own asserts live in production. Every published figure above is taken under it.
It is, however, a *charpente* choice rather than a property of redb, so a verdict
that reads "the store path is the bottleneck" owes an answer to "how much of that
is the flag?"

Rebuilt with `--config 'profile.release.package.redb.debug-assertions=false'` in
its own `CARGO_TARGET_DIR` and **interleaved in time with a control build of the
shipping configuration**, because control 2's finding above is that a control
from another era measures the era rather than the flag.

**The probe's build provenance is not verifiable from the record, and that is
stated here rather than glossed.** The run log records the three binary paths and
every invocation, but no `cargo build` command for the probe or its control; the
only machine-readable provenance field in the artifact, `build_profile`, reads
`release (redb debug-assertions on, workspace override)` on **all eleven runs**,
because it is a literal keyed off the *harness's* own `cfg!(debug_assertions)`
and cannot track a per-package override at all; and both probe target dirs have
since been deleted, so it cannot be re-run. An earlier draft of this section
claimed the flag was "verified at the rustc invocation" — **that check has no
record and the claim is withdrawn.** What the record does support is a complete
timing separation between two eras of binaries built for the purpose, which is
suggestive and is not the same thing.

**Nothing above depends on this probe.** The published table, the phase shares,
and the trigger-(a) re-reading all rest on the shipping-configuration runs; the
probe is a sensitivity observation with unverifiable build provenance, reported
for what it is worth to a future reader deciding whether the override is worth
re-examining. Eleven runs, every one reported, in the order taken:

| taken at | build | window | log records/s | `gap_within_store`, µs/record | store path |
|---|---|---|---|---|---|
| 01:13 | shipping | 20.25 s | 4,938 | 82.33 | 62.12% |
| 01:17 | assertions off | 13.55 s | 7,379 | 35.19 | 48.51% |
| 01:20 | assertions off | 12.68 s | 7,889 | 33.90 | 47.30% |
| 01:24 | assertions off | 12.19 s | 8,205 | 30.34 | 46.30% |
| 01:27 | shipping | 20.20 s | 4,950 | 86.29 | 63.45% |
| 01:35 | shipping | 21.16 s | 4,726 | 80.58 | 64.47% |
| 01:42 | shipping | 19.78 s | 5,055 | 76.51 | 63.17% |
| 02:02 | shipping | 18.19 s | 5,499 | 72.71 | 60.45% |
| 02:06 | assertions off | 13.50 s | 7,406 | 38.91 | 49.04% |
| 02:09 | shipping | 17.62 s | 5,674 | 57.95 | 56.95% |
| 02:11 | assertions off | 14.27 s | 7,010 | 47.38 | 53.47% |

**The two populations separate completely and the effect size does not.** Every
one of the five assertions-off windows (12.19–14.27 s) is shorter than every one
of the six shipping windows (17.62–21.16 s), with the last four runs strictly
alternating so drift subtracts out of each pair — and yet adjacent pairs put the
`gap_within_store` reduction anywhere from **18% to 65%**, and end-to-end
throughput anywhere from **+23% to +66%**. The direction is not in doubt; the
coefficient is, and quoting a single figure for it would be inventing precision
this run does not have. **All eleven runs read 62.0 log records per durable sync**
on the same lower-bound counter used above, so this is one workload shape
measured twice, not two.

The honest statement is that redb's debug assertions are the largest single lever
this measurement found — a substantial fraction of the apply phase and a
substantial fraction of end-to-end kernel throughput, for a change to one line of
a profile override rather than a new storage engine.

**"One line" is the size of the edit, not the size of the change, and the
difference is load-bearing.** `Cargo.toml`'s comment records why the override
exists: redb's open path walks pages only under `debug_assertions`, drawing sim
IO rolls debug-only and skewing every downstream draw (seed
`0xff159ed76d2806e4`), and `.github/workflows/ci.yml` carries two pinned seeds as
the cross-build-mode replay tripwire for exactly that fix. Turning the flag off
in release alone forfeits the cross-mode replay promise; turning it off in both
modes keeps the promise and **re-rolls the sim IO stream, so every seed recorded
under hard rule 3 stops reproducing its bug.** Hard rule 4's "assertions stay on
in release until a measured reason says otherwise" is also engaged rather than
sidestepped — this measurement is arguably that reason, and saying so is a case
to be made explicitly, not assumed. Either route is a project. That does not
weaken the recommendation to re-examine the override; it makes it decidable.

**What the flag does to the ordering, per run rather than by range.** Grouping
the five assertions-off runs the same way as the table above:

| run | store path | WAL path | larger |
|---|---|---|---|
| 01:17 | 48.51% | **48.78%** | WAL |
| 01:20 | 47.30% | **49.79%** | WAL |
| 01:24 | 46.30% | **50.79%** | WAL |
| 02:06 | **49.04%** | 48.23% | store |
| 02:11 | **53.47%** | 44.14% | store |

With the flag off the two paths are **statistically indistinguishable and the
group ordering is not established in either direction** — the WAL path is larger
in three of five runs — and at the field level `wal_sync_io` (41.81–48.72%)
exceeds `gap_within_store` (24.89–33.21%) in **all five**. Quoting the two ranges
side by side and concluding "the store path is still the largest thing in it"
would be a range-overlap fallacy; the pairing above is what the same runs
actually say. The kernel loop stays at 1.3–1.6% throughout, which is the one
thing the flag does not touch. **This probe is a sensitivity observation, not a
published column** — no figure in the table above is taken from it, it is one arm
on one build, and its build provenance is unverifiable as stated above.

### What this measurement does not resolve

- **It does not measure redb in isolation.** It measures `Store::apply` and
  everything on the stack beneath it, at the `Disk` boundary and by subtraction
  between `Disk` calls. redb's in-memory work is inside `gap_within_store` and
  inseparable there from allocator and page-fault time.
- **It does not cover the wire edge.** The kernel arm has no TCP and pre-built
  requests. One of the spec's four candidates for the 58% is untested here, and
  the wire arm cannot be traced by this instrument at all — it is multi-threaded,
  and `attribute` refuses an overlapping trace rather than publishing a share.
- **It is two points, not a curve.** batch 1 and batch ≈62. The 4-, 16- and
  256-connection points of the sweep have no phase attribution.
- **It is one device, and the field-level ordering is sensitive to that.**
  `F_FULLFSYNC` on an Apple SSD is the slow end of durable, which is what makes
  the fsync large enough to have dominated batch 1 in the first place. On a device
  with a battery-backed controller the sync shrinks and the store path's share
  rises further — so for the *group* reading this device is the conservative
  choice, but for the *field* reading it is the opposite: `wal_sync_io` leads
  `gap_within_store` by 0.4 points here largely because this device's fsync is
  slow, and a faster one is the most likely thing to invert that ordering. Any
  verdict read off the 0.4 points travels no further than this device.
- **The box was not quiet, and could not be made quiet.** It is shared with other
  agents whose jobs restarted faster than they could be waited out: a `pytest -q`
  from an unrelated repo peaking above 200% CPU (terminated on sight under a
  standing grant, every sighting timestamped in the run log), a booted iOS
  Simulator, a `retrace` debug binary and the `syspolicyd` signing load it
  generates. **Docker Desktop's Virtualization VM was deliberately left running**
  (10–30% CPU) — it holds a 1 TB sparse `Docker.raw` and killing it to recover
  under 1.5% of a 12-core box risks the volumes. **The quiet-box discipline is
  disclosed here at the strength its own record supports, which is less than an
  earlier draft claimed.** The protocol fixed before the run (P3) required load
  averages within ~1.5x across a window and said "discard and re-run; do not
  pool"; **every published observation violates it** (core run 1 reads 1.92x
  before and 2.23x after; kernel run 3 reads 4.5x). It was superseded at 00:33 by
  a direct `top` idleness check, on the defensible ground that macOS load average
  lags badly and counts non-runnable threads — measured at 00:33, `uptime` read
  7.71/15.05/20.57 while `top` read 90.8% idle with no process above 5.4% CPU.
  Two things about that substitution are stated rather than smoothed: a
  pre-committed criterion was replaced *after* the first observations existed,
  which is the move a pre-commitment exists to prevent; and **the substitute's own
  samples were not transcribed** — the run log holds the load averages either side
  of every window and the top-5 `%CPU` lists, not an idle percentage per run. So
  the honest form is "each run was preceded by a direct `top` idleness check,
  whose samples the record does not carry", and a reader who wants to audit the
  box's state has the load averages and process lists to do it with.

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
