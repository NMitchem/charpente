# M2 sub-project 2 — primary-side group commit: design

## Context

M2 sub-project 1 measured the commit path and confirmed the prediction it was built to
test: charpente commits **215 log records/sec** on dev hardware against a device append
ceiling of 215–258/sec bracketed around its own window — sync-bound at ≈1x
(`docs/BENCH.md`). The mechanism is not inferred. `Engine::submit` is unconditionally
`append` → `sync` → `apply`: **one durable fsync per transaction, no amortization.**

BENCH.md's most informative result is a column rather than a row. log-records/sec reads
215 / 215 / 217 / 198–225 / 170–202 down device / core / kernel / wire·bench-app /
wire·invoicing — four different code paths converging on one number. Everything above the
log is free at this workload; the fsync is the whole cost.

MANIFESTO §7 bets that "a single modern server sustains hundreds of thousands of
transactions per second." The measured figure is three orders of magnitude below that.
BENCH.md named the follow-up that closes the gap, and this is it: the engine already
proves the shape on the follower path, where `Engine::submit_batch` appends every frame in
a shipped batch and syncs once. The primary deliberately does not use it, because acking
before the batch's sync would break ack-after-durable. **Designing a primary-side group
commit that keeps that guarantee is this sub-project.**

DECISIONS §6's measure-first discipline is satisfied on the mechanism — unlike the LSM
engine, which was deferred for failing exactly this test, group commit has a benchmark
identifying its target as the binding constraint. It is **not** satisfied on the platform:
see §8.

## Scope (Noah rulings, 2026-08-10)

1. **Overlay across the batch**, not eager apply. Apply stays strictly after sync, so every
   acked-durability oracle and the apply-before-sync meta-test remain valid unchanged.
2. **Greedy drain, no linger.** A batch is exactly what is ready at this loop iteration. No
   timer enters the commit path.
3. **Mid-batch apply failure gives 500 to the failing record and the whole tail**, kernel
   stops, and "a 500 here may mean committed" becomes a named documented contract.
4. **Batching is within each ready-set arm independently**, never across arms. The arm
   arbitration is untouched.
5. **Every pinned seed is re-verified by revert-guard proof**; any that stops exercising its
   historical bug gets a replacement hunted and both recorded.
6. **Proceed on dev hardware; the bare-metal Linux number is a later correction** — with the
   honesty consequence in §8 stated wherever the win is reported.
7. The batch cap is a **constant, not a `DbConfig` field** (see §3).

## §1 The commit path: stage, then commit

`Db::transact` today runs a closure and commits it alone. It splits into two phases, and the
split is the whole feature:

```
let mut group = db.begin_group();          // snapshot + empty group overlay
for req in drained {
    group.stage(|tx| handler(tx, req))     // Ok  -> merge overlay, queue payload
}                                          // Err -> discard overlay, no record
group.commit()?                            // append all, ONE sync, apply all in order
```

`stage` runs a handler against the group's accumulated state and buffers its effects
record. It never touches the WAL. `commit` appends every queued payload, syncs **once**,
then applies each in seq order — the existing post-sync apply discipline, N-wide.

**`Engine::submit` becomes `submit_all` of length one.** There are already two copies of
append/sync/apply in `engine.rs` (`submit`, `submit_batch`); a third would cross the
consolidation threshold this repo flags elsewhere. Collapsing them is not tidiness — it is
what makes the batch-of-1 byte-identity that ruling 5 depends on a **property of the code
rather than something tests must keep true**. A single-request iteration must execute the
identical instruction path it executes today.

`submit_all` assigns its own seqs (the primary is the source of order); `submit_batch`
keeps validating shipped seqs against a trust boundary. That difference is why they stay
distinguishable after the shared core is factored out.

## §2 The two-level overlay

A handler reads through `View` — snapshot plus its own read-your-own-writes overlay. Group
commit adds a second level beneath it:

```
read order:  txn overlay  ->  group overlay  ->  store snapshot
```

The structure to merge already exists: `Tx::put`/`delete` write into `self.effects.overlay`,
keyed table → id → `Option<bytes>`. The group overlay is a merge of that same shape, so this
extends the existing mechanism rather than introducing a parallel one.

- On `stage` returning `Ok`, the transaction's overlay merges **into the group overlay** and
  its payload is queued.
- On `Err`, the transaction's overlay is **discarded** and nothing is queued. The group is
  unaffected.

The two levels are not a convenience. They make panic-safety **structural**: a handler that
panics mid-transaction cannot corrupt the group, because its writes were never merged. This
is the same reasoning that made the job dispatcher's Done-mark atomicity structural rather
than checked.

What the group overlay must carry, so a later transaction sees an earlier one's work:

- **Row puts and deletes** — whatever `View` already models within a transaction.
- **Secondary index effects** — index reads must reflect overlay writes exactly as they do
  within a single transaction today.
- **Meta counters** (`next_job_id`, `next_event_id`) — these stay **out** of the overlay.
  Verified at the source while writing this spec: `Tx::enqueue`, `enqueue_keyed`, `emit` and
  `emit_keyed` all return `()`, so no platform-allocated id is ever visible to a handler at
  stage time. The counters bump at apply, in seq order, which keeps ids deterministic and
  byte-identical on followers with no overlay involvement. §9's follower-identity property
  is what holds this true if an API ever grows a return value.
- **Session rows** — auth writes inside the commit point are ordinary rows and are covered.

## §3 Batch composition

A batch is the set of items the arm has ready at this iteration, capped. No timer, no clock
read, no entropy draw: **batch composition is a pure function of the ready set**, which the
same-shape rule already governs, so prod and sim agree by construction.

At one connection this degenerates to a batch of one and the path is today's path. That is
the acceptance criterion for §7, not merely an observation.

**The cap is a constant, not configuration.** `DbConfig` carries operator-facing policy
(`retain_segments`, `checkpoint_every`, `event_retention`, `migration_batch_size`); a batch
bound is internal mechanism, like `REPL_TICK_NANOS`. No operator has a reason to tune it —
greedy drain is already self-limiting by arrival rate, so the cap exists purely to bound
memory. A configurable throughput knob would also be tunable to flatter a benchmark, which
is the one thing not to put next to `docs/BENCH.md`. Proposed `MAX_BATCH: usize = 64`;
worst-case buffered payload is bounded by it times the existing 1 MiB body cap.

Checkpointing stays at the transact funnel and may only occur **between** batches, never
inside one.

## §4 Failure semantics

| failure | durable? | clients | engine |
|---|---|---|---|
| handler returns app `Err` | no record | that request's own error | unaffected, batch continues |
| handler panics | no record | 500 for that request | overlay discarded, batch continues |
| append fails | nothing | whole batch 500 | log poisoned |
| sync fails | nothing | whole batch 500 | log poisoned (fsyncgate, unchanged) |
| apply fails at record *i* | **all N durable** | 1..*i*-1 keep their 200s; *i* and tail get 500 | poisoned, kernel stops |

The last row is the only new contract. Records after the failure are durable and **will
apply on restart**, so their 500 is a false negative: the client is told "unknown" about a
write that committed. This is the same epistemic class as DECISIONS §6's bounded-loss
window and must be documented in GUIDE as a named contract, not left to be discovered.

Note the asymmetry that makes this acceptable: a 500 never promises rejection, only
absence of confirmation. The reverse error — a 200 for something not durable — remains
impossible, because the ack still follows the sync.

## §5 The device-ratio gate must be corrected FIRST

**This is a prerequisite task, landed and committed before any batching code exists.**

`measure::implausible_bracketed_ratio` refuses any layer reporting a durable rate above the
device's measured ceiling. Its premise was verified at the source during the 2026-08-10
interlude: one acked unit costs one durable sync. **Group commit invalidates that premise by
design** — N records per sync means records/sec legitimately exceeds syncs/sec — so the gate
as built would refuse every healthy batching run.

Correcting it inside the sub-project that produces the big number would mean changing the
instrument and the subject in one motion. The gate exists to catch **acking without
syncing**, whose signature its own comment puts at "2x or beyond" — which is precisely the
signature of a legitimate group-commit win. If anyone later asks how we know batching did
not break ack-after-durable, the answer must not be *"we adjusted the check that would have
caught it, in the commit that made the number."*

The correction: gate on **syncs/sec against the device sync ceiling**, not records/sec.
This is not an accommodation to a new feature — it is a fix to a proxy. Records/sec only
ever equalled the physical quantity because one transaction happened to equal one sync; the
gate has been measuring the right number by accident. Syncs/sec stays 1:1 with the device
whether or not batching exists, so the gate keeps its teeth in both worlds.

Acceptance for this task: it lands against **today's non-batching** code, where the true
ratio is known and measured at 1.0, and the full matrix still passes. Only then is the
instrument trustworthy enough to score the change. BENCH.md's method section is rewritten
with it.

## §6 Replication and determinism

Records ship per-record as they do today; a batch simply means N ship together, which the
follower's `submit_batch` already handles. Ids are still assigned at apply in seq order, so
follower byte-identity is preserved — a property that must be tested, not assumed (§9).

Determinism holds because nothing in batch composition consults a clock, entropy, or
scheduling: the batch is a function of the ready set alone. The one entropy draw on arm
contention is untouched by ruling 4.

## §7 The frozen pinned seeds

Batching changes the operation schedule wherever more than one request is ready, and sim
workloads routinely queue many at once. `run_universes!` and every pinned seed are frozen
forever, so this is handled explicitly rather than absorbed.

- **Batch-of-1 identity** (§1) means any universe that never has two items ready at once is
  byte-identical, and many pins survive for free.
- **Every pinned seed gets the worktree-isolated revert-guard proof** the migrations
  close-out established: revert the historical fix, confirm the seed still goes red for its
  original reason. Outcome recorded per seed.
- A pin that no longer exercises its bug gets a **replacement hunted**, and both are
  recorded — the old one with the reason it drifted. A silently-passing pin is
  indistinguishable from a working one, which is the failure mode this repo has caught in
  itself three times.

This is a dedicated task and will not be a quick one.

## §8 What the measurement is worth

Ruling 6 keeps measurement on dev hardware. That is workable, and this section is the price
of it, to be repeated wherever the win is reported.

**This device flatters group commit.** A ~215/sec fsync ceiling produces deep queues at high
connection counts — BENCH.md records a 2.27 s p50 at 256 connections on flat throughput —
and greedy drain converts queue depth directly into batch size. A faster device drains
queues faster, so queues are shallower, so batches are **smaller**. The multiple measured
here is therefore closer to an upper bound for this mechanism than to what a server would
show.

The honest form of the claim is: *group commit removes the one-fsync-per-transaction
ceiling, and on hardware whose fsync is slow enough to keep a queue deep it recovers Nx.*
Not: *group commit makes charpente Nx faster.*

DECISIONS §6's measure-first discipline is satisfied on the mechanism and **not on the
platform** — MANIFESTO §7's bet is about a server and no server has been measured. The gap
BENCH.md reports has two components, device speed and amortization, and only the second is
addressed here. The bare-metal run stays owed (sub-project 1 spec ruling 4) and is the
correction that will resize this result.

## §9 Testing

Every existing flagship property must hold unchanged with batching live: exactly-once
effects under the fault matrix, acked-implies-durable, the dual-write invariant (jobs ≡
committed txns), no-starvation, and the realtime and replication suites.

New properties, each RED-first per hard rule 3:

1. **The sync is atomic over the batch.** A crash between append and sync loses the *whole*
   batch and never part of it; nothing was acked. Discriminating twin: an implementation
   that syncs mid-batch must go red.
2. **Read-your-own-writes across a batch.** Transaction B sees A's write. Twin: a naive
   implementation handing B the bare snapshot goes red — this is the property the whole
   overlay exists for, so its twin is mandatory, not optional.
3. **Batch-of-1 trace identity.** A universe that never queues two items produces a
   byte-identical trace to pre-batching. This is §7's structural guarantee under test.
4. **Mid-batch apply failure.** 1..*i*-1 acked, *i* and tail 500, and after restart the
   durable tail applies. The "500 may mean committed" contract pinned by text.
5. **Follower id identity under batching.** `next_job_id`/`next_event_id` allocation is
   byte-identical on a follower replaying a batched primary's log.
6. **Panic isolation.** A handler that panics mid-batch leaves the group overlay clean and
   the other transactions in the batch correct.

## §10 Non-goals

- No linger, timer, or adaptive batching. Ruling 2.
- No cross-arm batching. Ruling 4.
- No conflict detection or read/write-set tracking.
- No change to ack-after-durable, to the single-writer model, or to apply-after-sync.
- No parallel apply — DECISIONS §6 rejects it until a workload proves the *core loop* is the
  bottleneck, and this sub-project's own evidence says the fsync is.
- Not the LSM engine, and this sub-project does not re-open DECISIONS §7 trigger (a).
- Not the bare-metal run.

## §11 Exit criteria

1. The corrected device-ratio gate (§5) is landed, committed, and green against
   non-batching code before any batching code is written.
2. All six new properties (§9) live with real oracles, each RED-proven first, twins included.
3. Every existing property suite green, including replication, realtime and auth.
4. Every pinned seed re-verified per §7, with each outcome recorded and any replacement
   hunted and pinned.
5. **A measured win**: `bench wire --target bench-app` at 64 and 256 connections against
   today's ~110 req/s, reported with §8's caveat in the same breath.
6. `docs/BENCH.md` re-run and rewritten — new numbers of record, the corrected gate's method,
   and the §8 conditioning.
7. GUIDE documents the mid-batch 500 contract (§4) and DESIGN documents the staged commit
   path and the two-level overlay.
