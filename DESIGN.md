# Charpente — Architecture & Design

Status: M0 and M1 shipped, M2 in progress. This document records the load-bearing decisions
and their rationale. Decisions marked **[IRREVERSIBLE]** cannot be retrofitted and are argued
in detail; everything else is chosen for speed with a clear upgrade path. Sections marked
"(v0/v1, implemented)" describe the tree as it stands; the rest is design intent, not yet
built. docs/LEDGER.md is the chronological record of what shipped when.

## 0. System overview

```
                        ┌─────────────────────────────────────────────┐
                        │              charpente binary                │
                        │                                             │
  HTTP/WS clients ────▶ │  ┌─────────┐   ┌──────────────────────────┐ │
                        │  │  HTTP    │   │   Deterministic core     │ │
                        │  │  layer   │──▶│                          │ │
                        │  │ (routes, │   │  ┌────────┐  ┌────────┐  │ │
                        │  │  auth)   │◀──│  │  Tx    │─▶│  Log   │  │ │
                        │  └─────────┘   │  │ engine │  │ (WAL)  │  │ │
                        │                │  └────────┘  └───┬────┘  │ │
  realtime (SSE/WS) ◀── │ ── event tail ─│─────────────────┤        │ │
                        │                │  ┌────────┐     │        │ │
                        │                │  │ Tables │◀────┘ apply  │ │
                        │                │  │(storage)│             │ │
                        │                │  └────────┘             │ │
                        │                └──────────┬──────────────┘ │
                        │   post-commit dispatch    │                │
                        │  ┌────────┐  ┌───────┐    │   log shipping │
                        │  │ Queue  │  │ Cron  │◀───┘        │       │
                        │  │ workers│  │       │             ▼       │
                        │  └────┬───┘  └───────┘        followers    │
                        └───────┼────────────────────────────────────┘
                                ▼
                        external effects (email, APIs) — post-commit only
```

One process. The HTTP layer translates requests into transactions; transactions run
against the deterministic core; the core's only output is an ordered log; everything else
(table state, queue dispatch, realtime feeds, replicas) is a deterministic function of
that log.

## 1. The deterministic core **[IRREVERSIBLE]**

**Decision.** Every source of nondeterminism — disk, network, time, randomness, task
scheduling — passes through trait objects owned by the runtime, with a production
implementation and a simulation implementation. Application transactions execute inside
the deterministic boundary and cannot perform ambient IO.

**Why irreversible.** FoundationDB and TigerBeetle are unambiguous on this: deterministic
simulation testing only works if the system is designed for it from the first line. One
`SystemTime::now()`, one stray `tokio::spawn`, one direct file write inside the core, and
replay determinism is gone — and with it the entire verification story that justifies the
platform. Retrofitting determinism onto a nondeterministic codebase is a rewrite. The
promise holds across build modes too: redb's open path issues an extra,
`debug_assertions`-gated page walk (`Database::new`'s `mark_allocated_page_for_debug`, in
4.1.0 and still in 4.3.0) that used to draw sim IO rolls in debug only and skew release onto
a different universe from the same seed (found via seed `0xff159ed76d2806e4` under 4.1.0),
held now by forcing redb's debug-assertions on in release (`Cargo.toml`'s
`[profile.release.package.redb]`) so both modes consume one identical draw stream.

**Shape.** Synchronous and poll-shaped, not async futures (DECISIONS §10). `Instant` is
charpente's own nanosecond type — std's is opaque and cannot be fabricated from virtual
time.

```rust
trait Clock   { fn now(&self) -> Instant; fn sleep(&self, d: Duration); }
trait Entropy { fn fill(&self, buf: &mut [u8]); }
trait Disk    { fn open(&self, path: &str) -> Result<Box<dyn File>>;
                fn remove(&self, path: &str) -> Result<()>;
                fn list(&self, prefix: &str) -> Result<Vec<String>>;   // always sorted
                fn sync_dir(&self, prefix: &str) -> Result<()>; }  // the directory barrier (§2)
trait File    { fn len(&self) -> Result<u64>;
                fn read_at(&self, offset: u64, out: &mut [u8]) -> Result<()>;
                fn write_at(&self, offset: u64, data: &[u8]) -> Result<()>;
                fn set_len(&self, len: u64) -> Result<()>;
                fn sync(&self) -> Result<()>; }  // the durability barrier
trait Net     { fn listen(&self, addr: &str) -> Result<Box<dyn Listener>>;
                fn connect(&self, addr: &str) -> Result<Box<dyn Conn>>; }
// Listener::accept and Conn::recv are non-blocking polls; Net is
// message-oriented until the HTTP edge demands byte streams.
trait ObjectStore { fn start(&self, op: ObjectOp) -> Result<()>;
                    fn poll(&self) -> Option<Result<ObjectDone>>; }  // PUT is atomic, never torn
```

The simulator implements all of it with one seed driving: latency distributions, virtual
time (a simulated year of heartbeats runs in seconds), and the fault matrix — injected
write/sync errors, crash-restart between any two disk operations, partitions, and
crash-time write-buffer collapse in which every unsynced write independently survives,
tears (an arbitrary strict subset of its sectors), or vanishes. Subset tears matter: they
produce the file-extends-past-garbage layouts that defeat length-prefix-only recovery,
which prefix-only tearing never catches (found while building the first meta-test).
Three more axes (the Jepsen/TigerBeetle lessons — corruption *granularity* is its own fault
axis; whole-sector injection provably misses single-byte bugs), landed with WAL segment
management and paired with the recovery-horizon rule in §2: `FaultConfig.bit_flip` corrupts
one random bit of one random byte in one random file's durable content at `Sim::restart`,
before crash collapse; `FaultConfig.misdirected_write` lands a `write_at`'s payload past the
requested offset while the call still returns `Ok` (the requested offset stays stale);
`FaultConfig.read_error` is a transient `read_at` failure (the next read may succeed). All
three (plus `dir_entry_loss`, §2) draw from per-field sub-streams keyed by (universe seed,
FNV-1a of the field name) rather than `FaultConfig::swarm`'s legacy sequential draw — each
field's stream is independent, so adding one perturbs no existing universe's legacy draws
and no other extended field's draws (`sample_extended_fields`, charpente-sim, applied right
after `FaultConfig::swarm` in every swarm-sampled universe). The two durable-corruption
axes (`bit_flip`, `misdirected_write`) are scoped to `FaultConfig.corruption_scope`
(default `"wal/"`): they exist to prove the WAL fences (frame CRC + the recovery-horizon
rule), and store-file bitrot is beneath those fences — v0's answer to it is redb plus M1's
restore-from-replica, and M2's own storage engine owns page checksums; `read_error` and
`dir_entry_loss` stay global (they produce errors, never silent wrongness). Even inside
the scope, the horizon fence only defends checkpointed data: corruption of an acked frame
above `applied_seq` is indistinguishable from a torn tail and is legitimately cut (the
standard WAL convention; un-checkpointed data has one local copy, and M1 replication is
the answer), so acked-durability workloads route their shortfall through
`checks::durable_corruption_fired` — loss is excused only in universes where a
durable-corruption fault actually fired. Two wire-scoped axes landed with M1
replication's second sub-project, sampled from the same field-keyed sub-streams and
rolled at `Conn::send` on the global fault stream (a connection spans two hosts; the wire
belongs to neither): `FaultConfig.conn_drop` kills the live connection — the sender gets
`ConnectionReset`, the peer reads EOF, in-flight messages in both directions drop — and
`FaultConfig.net_spike` multiplies one message's drawn latency ×64 (delayed, never lost).
Four object-store axes landed with the M2 archive, sampled from field-keyed sub-streams and
rolled off the store's own streams, so an archive's rolls never perturb — and are never
perturbed by — the disk and wire faults drawn from the same seed: `FaultConfig.object_put_error`
(the PUT does not land, and the prior value stays — a PUT is atomic, never torn),
`object_get_error` (a GET or LIST fails), `object_lost_ack` (the object lands durably and the
caller is told it failed, which is the axis that makes a same-key/same-bytes re-PUT
load-bearing rather than merely safe), and `object_delete_error` (a DELETE fails; `delete` is
contractually idempotent, so this is a retry rather than a lost object, which is what makes an
interrupted drain reachable).

The store draws from **two** streams, not one. Put, get, list and lost_ack roll off
`keyed_stream(seed, "object_store")`; the `Delete` arm rolls off
`keyed_stream(seed, "object_store_delete")`. The split is load-bearing rather than tidy:
`Rng::chance` draws from the stream *before* comparing, so it consumes a draw even at
`p == 0.0`, and a delete roll on the shared stream would have shifted every later put/get
draw — re-rolling every pinned universe merely by declaring the field, and invalidating the
convergence floors and both recorded seeds with it. Drawing it separately makes the axis
non-perturbing at zero, which was proven rather than argued: all twelve convergence coverage
counts across four bases reproduce exactly. The convergence property now **enables** it: a
delete fails in 32 of 256 universes at the pinned base and every one of those universes still
restored, and `archive:delete_failed` sits in `require:` so a batch where no delete ever fails
is a red property rather than a quiet happy-path pass. The two aimed `DeleteMode` switches stay
alongside it — a probability cannot guarantee the delete-fails-FOREVER shape in a finite run.
And `SimObjectStore` completes each operation in the `poll` that takes it, so object-store
latency is not modelled at all.

**What an object store owes, and what tests it** (2026-10-01). An object a `Get` or `List`
returns is durable, and an acknowledged `Put` is durable (`ObjectStore`'s trait doc).
`SimObjectStore` is that contract as a model: it has no page cache, so nothing it returns or
acknowledges can vanish, and the archive's properties test the archive against the contract
through it. The production store over a filesystem, `FsObjectStore`, has to earn it with
fsyncs — of the object and of every directory from its key's directory up to the bucket
root's parent, on every `Put` and before every `Get` or `List` returns (prod.rs). Before
2026-10-01 it did not: a `Put` that failed after its rename left an object a later process
could read back from the page cache and the next power loss could take away, and an
acknowledged `Put` under a new prefix could lose its whole subdirectory to a power loss,
because the directories it created were never synced into their parents. Its oracle is not the simulator. `Disk` has no rename, so
a `Put` through it could not be atomic; its `open` creates, so a `Get` through it would; and
its names are a flat namespace whose `sync_dir` makes every name under a prefix durable, so
`SimDisk` cannot express a directory whose own entry was never synced, the shape the second
defect needs. Giving `Disk` those would change a core trait — 23 implementations, and a
directory tree in `SimDisk`'s crash model — to serve one store. The oracle is a crash model of the filesystem calls `FsObjectStore` makes (`CrashFs`,
prod.rs's tests): the POSIX minimum, where a power loss keeps only what a sync took, each
directory on its own, and a process restart keeps everything. Every durability decision sits
above that seam, so the model runs the code production runs; it cuts the power after every
call of a workload and pins each sync by name. `ProdDisk` runs over the same seam and model,
for the reason `SimDisk` could not catch its defect: `SimDisk`'s flat names keep `Disk`'s
contract as written, and only a directory tree can show the production impl falling short of
it. Until 2026-10-01 it did fall short. `ProdDisk` created the data directory, any directory
above it and `wal/` without syncing any of them into its parent, and a barrier fsynced only
the one directory holding its prefix, so on a fresh node every write acknowledged before the
first checkpoint rested on a `wal` entry the next power loss dropped (§2's barrier). The model
states its own limits (prod.rs, `Fs`): a power loss drops all unsynced state, deterministically
and in full, so it finds a missing or misplaced sync and not a bug that needs some unsynced
state to survive; and its kernel never marks a page clean after a failed fsync, so the EIO
residual (§2) is out of its reach, as it is out of `SimDisk`'s.

**The store's crash surface, and the countdowns.** Every `Store::apply` commits at
`Durability::None`, and redb 4.3.0 keeps such a commit's pages in its own write buffer
(`write_barrier()` issues no file write, `cached_file.rs:594`) until a checkpoint — or until
the buffer passes 512 MiB, which no sim workload approaches. So the sim's crash surface in
the store file is checkpoint-only: what a crash can find unsynced there is one checkpoint's
page writes (beside the store's creation, and `grow()`'s extension, which 4.3.0 syncs before
any header records it, §4), and everything else a crash collapses is WAL. The torn-store
shapes that redb 4.1.0's unsynced page writes let the crash model produce — a file cut below
its header's layout, a sub-page tail — are therefore held by hand-built tests that write them
directly, not by the sim: engine.rs's `open_a_store_mangled_to` pair and transactions.rs's
`mangled_store_len_recovers` family. The sim does not model IO that no longer happens. The
same change re-sized the crash countdowns. A workload arms `crash_disk_after(A + rand % B)`
once per round, and it fires only if the round runs past the drawn op count, so a range is
calibrated to its workload's op volume: under 4.3.0 a round issues 57-90% of 4.1.0's disk ops
(mean, per workload), and at a fixed base the 4.1.0 ranges fired 24-86% fewer crashes. Each
range was re-sized on 2026-09-30 by counting every round's ops with the countdown disarmed, at
both versions and two bases. The goal is `fault:crash` at or above its 4.1.0 count with every
floor still holding, and the rule is set per workload. Each range is scaled until that
measured distribution predicts 4.1.0's crash count plus one standard deviation at the batch's
base — except `scoped_realtime_workload`, which takes no margin: with the margin it crashed
10% more often than under 4.1.0 (124 against 113) and its exact `realtime:resume-backlog`
floor fell to 122 of 125, while without it the batch crashes 117 times and the floor holds. A
uniform scale by the mean op ratio alone restores only 71-98% of the rate (per workload),
because 4.3.0's saving sits in the long rounds. Every batch's `fault:crash` is back at or
above its 4.1.0 count at its base. Where a property's hard case had depended on 4.1.0's IO
shape, the workload now reaches it by construction: the receipt-upgrade property needs v1 to
die while a receipt is durable but not yet run, a window 4.1.0 widened by writing store pages
inside it, so `receipt_upgrade_universe` holds its first receipt Pending for one retry
(kernel.rs). An oracle's reach must not depend on a dependency's IO shape. A redb upgrade that
changes the IO shape again re-opens this calibration, as it re-opens the latch census (§4).

**The same split a second time: the directory barrier.** `SimDisk::sync_dir` rolls
`sync_error` off `keyed_stream(seed, "host-dir-faults:{host}")` — and off a global
`dir_faults` stream for the un-hosted disk — not off the shared `faults` stream that reads,
writes, file syncs and misdirected writes draw from. The hazard is the object store's,
generalised: `Rng::chance` draws before it compares, so a barrier sharing the stream
displaces every later roll, and **adding or relocating a single `sync_dir` call re-rolls
every universe at a pinned base**. That is measured, not hypothetical — `open_with_horizon`'s
durability fix (§2) added exactly one barrier and forced convergence-floor re-measures in
three separate test files on the same day. `sync_dir` earns its own stream because it is the
disk op whose call sites move for *durability* reasons rather than workload reasons, so it is
the one most likely to be added by a correctness fix. Unlike the delete arm, this split was
**not** free: that axis defaulted to `0.0` and could be introduced with every pinned count
reproducing exactly, whereas `sync_error` fires in existing runs, so the split re-rolled every
pinned universe once and the floors were re-measured against it. The guarantee is mechanical
rather than argued — `a_directory_barrier_does_not_perturb_the_other_disk_faults` inserts a
semantically no-op barrier and demands the entire coverage map be unchanged; without the split
it fails, one barrier having moved four read faults and two write faults.

**A sync with nothing to flush rolls no fault** (2026-10-01). `SimFile::sync` rolls
`sync_error` only when the file holds unsynced writes. A file with none has nothing to flush
and nothing to fail, so its sync always succeeds and draws nothing from the `faults` stream.
It is still a disk operation: it ticks an armed crash countdown and advances virtual time
like any other. This is the directory barrier's hazard again, from the other side: syncs
move for durability reasons too. §2's recovery syncs every segment before replaying it, and
after a power cycle every one of those segments is clean. A roll on each injected failures
no disk produces in that state and re-keyed every universe that recovers; late_follower's
batch lost about 48 verdicts to them until this landed. What the sim gives up is a real
kernel reporting an earlier background-writeback failure, which a later fsync of a clean
file returns on Linux. The sim has no background writeback (a write stays pending until a
sync takes it), so a clean file has nothing to report. The recovery-sync failure path is
held by deterministic tests instead (§2). `a_sync_with_nothing_to_flush_draws_no_fault`
holds the rule by comparing whole coverage maps with and without a clean sync before every
operation.

**A process restart is not a power cycle.** `Sim::restart` power-cycles: the
page cache collapses under the crash model before anything reopens.
`Sim::restart_process` (2026-10-01) restarts only the process. Open file handles
die and nothing else happens: every unsynced write and directory entry stays pending
and readable by the next open, and the next power cycle still collapses it. That
is what a supervisor sees after a kill, a crash or an exit, and it is the only
way to reach "the next process read back what the dead one never made durable".
Before it existed, a property reached that shape only by accident, when a boot
loop reopened without a restart. Readable is not durable, and that accident
exposed a real ordering bug: recovery replayed WAL frames it never synced, and the
store outran the log (realtime seed 0x7df8c43a1d2591e8; fixed, §2). The mode draws
from no stream and clears no countdown, so introducing it re-keyed no universe. Three
pinned batches reproduced their coverage byte for byte across the change
(migrations, realtime's mixed-shape resume, and the wal dir-entry property). The
property that drives it on purpose is §2's class oracle,
`the_store_never_outruns_the_log_across_process_restarts_and_power_cycles`. It proves each
restart left the log something to read back with `Sim::unsynced_files()`, read right after
the restart. A restart that flushed the page cache would leave that list empty and the
property's coverage requirement unmet. Only open file handles are epoch-gated, as after
`restart`: a destructor that opens a file afresh through a `Disk` it still holds reaches the
disk. Charpente's own close goes through its open files.

**Per-host worlds** (landed with M1 replication's first task): `Sim::host(name)` gives a
named simulated host its own disk (`DiskWorld`, unchanged — the same crash/countdown/epoch
model, just one instance per host) and its own fault + entropy streams, keyed sub-streams
off (universe seed, host name) via the same construction as the extended fault fields
(`keyed_stream`, formerly `sample_extended_fields`'s private `stream` helper) — a host's
rolls never depend on other hosts' op interleaving, and creating or using a host draws from
none of the frozen global streams (`Sim::disk()`, `Sim::entropy()`, `Sim::restart()`,
`Sim::durable_fingerprint()` are byte-identical to before this task; every pre-existing
pinned seed replays unchanged). Virtual time and disk latency stay universe-global — time
is one dimension, not a per-host one. `SimHost::crash_now`/`crash_after`/`restart` mirror
the global equivalents scoped to that host's disk (crash coverage is the new `"crash:host"`
key, distinct from the global `"fault:crash"`), and additionally tear down the host's
network presence (`NetWorld::teardown_host`): every connection with that host on one side
closes that side and drops the in-flight queue addressed to it (a dead host will never read
those bytes), and every listener it owns is removed so a restart's `listen` on the same
address succeeds. Listener tokens (bumped by a `next_listener` counter) stop a handle held
across a crash from accepting on, or clobbering, a later re-`listen` at the same address.
This is the sim-side prerequisite for the multi-node replication universes in M1's second
sub-project — nothing above the sim is testable without host-scoped crash/restart.

**Consequence for the concurrency model.** The core state machine is single-threaded —
concurrency lives at the edges (HTTP handling, queue workers, replication) and is itself
scheduled through the deterministic executor. The executor's load-bearing rule (the
FoundationDB half-measures lesson): the kernel loop's choice of *which ready event source
to service next* is an explicit decision made by the same code in production and in
simulation — only the entropy behind the decision differs. A production loop that
multiplexes by racing a real channel while the sim draws from a different structure is
not testing the scheduler production runs. Throughput comes from the absence of network
hops and lock contention, not from parallel mutation. This mirrors TigerBeetle and Redis:
a single thread applying a log is astonishingly fast when it never waits. The rule is
implemented (M0 step 6): `charpente::kernel`'s `Kernel::run` builds the ready set over
requests (`RequestSource`), due jobs, and due cron ticks and draws one entropy pick only
when more than one is ready — idle iterations spend no entropy. The request arm stops the
loop when the engine is poisoned (ruling 2026-07-05, spec `2026-07-05-sim-cli-design.md`):
a 500 goes to the client first, then `Db::poisoned()` gates continuing. The loop body is
one `Kernel::step` (M1 replication T8): it services one ready source and returns, or
reports the deadline to park on — `run` is the thin wrapper that owns the park, so a
replicated driver can own it differently without forking the decision code. Replication
(M1) adds a fourth ready source, the `Repl` engine, whose role gates the set: a follower
(or a draining primary — `is_primary` is false while handing off) serves reads and ships
its log but never runs jobs or cron (those would hit `TxError::NotPrimary`), and a
follower→primary flip inside `Repl::step` rebuilds the due-heap and cron from the
replicated store **in the same step** (`Kernel::after_repl_step`, called from the
`Ready::Repl` arm before it returns) — a promoted node never services a request or a job
against a stale heap. A repl-configured node also caps every idle wait at
a 5ms park tick, so in-flight replication IO — sim messages or prod TCP — is polled
promptly regardless of the next job/cron deadline. Realtime (M1 sub-project 3) adds a fifth
ready source, `Tail`: unlike the deadline-carrying sources (jobs, cron, repl) it is never
timer-gated — like a request, it is gated purely by something happening: a subscription
becomes eligible the instant some commit advances `next_event_id` past its `next_id`, and
the per-step read that checks this is skipped entirely whenever no tail is registered, so a
kernel that never sees an SSE subscribe does zero extra IO and every pre-existing pinned
seed replays unchanged. One unit of `Ready::Tail` work is a bounded table read
(`TAIL_BATCH_EVENTS` rows), serviced round-robin across live tails so one deep resume never
starves the rest of the ready set (§6). The object-storage archive (M2) adds a **sixth**
ready source, `Archive` — `ObjectStore` is the fifth IO *trait*, which is not the same
count. Like `Tail` it carries no timer: spec §8 states the archive arm never
reads wall-clock time, directly or transitively, which is also why its snapshot cadence
counts records rather than seconds. §8 PERMITS the data-and-attempts scheduling below
rather than forcing it — a counter-based backoff would be clock-free too; what rules a
counter out is that it would need a tuning constant no measurement in `docs/BENCH.md` can
justify. Its readiness is therefore derived from data,
and the derivation is the subtle part. `Archive::idle()` cannot be it — `idle` answers true
in five states, four of which still hide un-archived data, and it is a snapshot that only
updates when you step, so "skip while idle" is self-sealing: a write-gated archive resumes
when the gate opens only if something keeps stepping it. The kernel instead remembers
`db.last_applied()` as of the last step that found nothing to do and re-arms the arm when a
freshly read `last_applied` differs, which is sound because every source of new archive work
requires it to advance. Three further clauses gate the set: `Archive::stopped()` (terminal —
a stopped archive answers `idle() == true` forever, so an arm that watched only idle would
drive a dead archive for the life of the process and present it as healthy), `Db::verifying()`
re-read every iteration (spec §4 rule 1: a write-gated node uploads nothing, and the gate
opening must re-arm the arm with no step in between), and a RETRY HOLD after a failed store
operation, since a failed `start` leaves the archive in a non-idle state permanently and
would otherwise peg the loop for the length of an outage. The hold is deliberately NOT
released by the data watermark: "new work can exist" and "a failed operation should be
retried" are different questions, and conflating them strands the archive exactly where it
hurts — under a flood of requests that stage no record the ready set is never empty (so the
kernel never parks) and `last_applied` never moves (so no commit fires either), and an
archive that hit one PUT error there stopped shipping permanently and silently. A retry's
precondition is ATTEMPTS PASSING, so the hold is released by any kernel iteration the
archive arm did not itself consume — one rule covering a parked kernel, a committing kernel
and a saturated read-only one. One unit of
`Ready::Archive` work is exactly one `Archive::step` — start one upload, poll one in flight,
or read one `read_budget_bytes` bite — never a loop to completion. An archive error is
absorbed, never bubbled: an archive failure is not a database failure (spec §8), and a `?`
there would let a transient PUT kill a serving node.

That round-robin is fairness *among* tails; it is not protection *from* the request arm.
`Ready::Tail` sits in the same ready set as every other source and is picked by the same
single uniform draw, so a request arm draining up to `MAX_BATCH` per win (§2) competes with
a registered tail on exactly the coin flip it competes with a due job on — the tail arm has
no priority, no reserved slot, and nothing that preempts a batch. Two things follow that the
job-arm measurement below does not cover, and neither has been measured: one tail win
advances **one** tail by at most `TAIL_BATCH_EVENTS` ids, so with several live tails, or
handlers that emit faster than that, a tail's lag can grow while a flood lasts; and unlike a
delayed job — which is merely late — a tail that lags past the retention floor is dropped,
receiving the terminal `charpente.gap` frame instead of its events (§6). Whether a request
flood alone can drive a tail past the floor at the default `event_retention` is an **open
question**; it is not a claim in either direction, and the job arm's numbers do not answer
it.

The job arm is the one that has been measured, and group commit changed it — from *certain*
service to *probabilistic in the queue depth*. **This is an observed property, not a
promise**, and it is stated as a bound because the mechanism is known exactly: arbitration
is one uniform draw, a request flood drains in `ceil(FLOOD / MAX_BATCH)` request-arm wins,
so a due job must win at least one of that many coin flips. At a flood of 100 against a
`MAX_BATCH` of 64 that is two draws — P(fail) = 0.25 — and measurement matches the
arithmetic: 200 of 200 seeds serviced the job arm before the source drained pre-batching,
**151 of 200** after. At a flood of 1024 it is 16 draws, P(fail) ≈ 1.5e-5, and 200 of 200
pass. What the platform holds to is therefore structural rather than statistical: the job arm
is never excluded from arbitration — it takes the same draw as every other ready source at
every contended step — and nothing is promised about *when* it wins, or about service
ratios. Two properties hold that line and they assert
different things, which is worth stating because one of the names overreaches:
`due_jobs_are_not_starved_by_a_request_flood` asserts eventual **completeness** — every due
job has run by the horizon — a count that is identical whether a request-arm win consumes 1
request or 64; `due_jobs_are_serviced_before_a_request_flood_exhausts_the_source` asserts
the **interleaving** invariant, that the job arm is serviced at least once while the source
still has outstanding work. The second is deliberately not a bound on degree: a bound pinned
by a test is a contract whatever this document says, and greedy drain cannot honour a fixed
ratio on a fast device anyway (docs/BENCH.md — shallower queues mean smaller batches).

The HTTP kernel (`charpente::http`) lives inside the boundary — the sim drives
`App::handle` directly; hyper only translates wire↔struct at the edge
(`charpente::serve`) and feeds the single-threaded kernel loop.

## 2. The write model: transactions as pure functions over a log **[IRREVERSIBLE]**

**Decision.** A transaction is a deterministic function `fn(&mut Tx) -> Result<T, E>` that
reads tables, writes tables, enqueues jobs, and emits events. On success, its effects are
serialized as one record into a single ordered write-ahead log. Table state is the result
of applying the log. There is no other way to mutate state.

**What a transaction cannot do:** await the network, read the wall clock directly (it gets
the transaction's logical timestamp), generate ambient randomness (it gets seeded entropy),
or spawn tasks. External effects are expressed as enqueued jobs that run *after* commit.

**Why.** Three properties fall out of this single decision:

1. **Replication for free.** Followers replay the same log through the same deterministic
   state machine and arrive at identical *logical* state — the same rows, the same job
   queue, the same event stream, at the same log position. Failover is log position
   arithmetic. What is deliberately **not** claimed is a byte-identical store file. redb's
   physical layout depends on how records were grouped into write transactions, and that
   grouping is a scheduling artifact — how many records happened to be ready at once —
   which the log does not record and is not asked to: the log fixes what became true and
   in what order, never how many commits it took to absorb. Byte-identity held
   incidentally while the store committed one transaction per record, and per-batch apply
   ends it — for a recovering primary as much as for a follower, since a node replaying
   its own log cannot reproduce the grouping its live traffic originally had either. The
   store is a materialization of the log (§4), and two materializations of one log agree
   on content, not on layout. `Db::store_write_schedule()` exposes a node's grouping as a
   digest, so the sim's byte-fingerprint oracle compares files only between nodes that
   actually scheduled their commits alike, instead of inferring it from which faults
   fired.
2. **Simulation for free.** A deterministic function of (state, input, seed) is trivially
   replayable. Every production incident is reproducible if you have the log segment.
3. **The dual-write class deleted.** Job enqueues and event emissions are rows in the same
   log record as the state change. Commit is a single atomic append + sync.

**The cost, stated honestly.** No `await reqwest::get(...)` inside business logic that also
writes state. The idiom is: transaction commits intent → queue worker performs the external
call → result comes back as a *new* transaction. This is the saga pattern made mandatory
and mechanical instead of optional and artisanal. Agents handle this discipline better than
humans do — it is a rule a compiler can enforce (`Tx` is `!Send` into IO contexts, and no IO
capability is in scope inside a transaction).

**Log format (v0, implemented).** Frames of `[seq u64][len u32][crc32 u32][payload]`
(crc over all three) in segments named `wal/<base_seq:020>.log`; rotation syncs the old
segment before the new one takes writes, so a tail `sync` is a whole-log durability
barrier. Recovery scans in order and cuts at the first invalid frame: it deletes the
orphaned later segments first (highest base first, each `sync_dir`-barriered) and only
then truncates the cut segment, syncing the truncation before accepting appends. The
truncation-sync is load-bearing (without it, a same-length discarded frame beyond the cut
can be resurrected by a later recovery, checksum-valid and seq-correct); the
delete-before-truncate order is a fence in its own right (fence 3, below). Any IO failure
poisons the log — fsync results
cannot honestly be retried (fsyncgate); the only exit from poison is restart-and-recover,
and recovery is idempotent. The apply loop (`Engine`) holds the standing invariant:
machine state ≡ fold(apply, durable records) — submit is append → sync → apply → ack,
and recovery is sync → read → apply. **Readable is not durable**: after a process restart
the page cache still holds every frame the dead process wrote, synced or not, and each
reads back valid. Recovery therefore syncs each segment before reading any of its frames,
every segment and not just the tail, so every frame it hands to replay is durable. A
segment whose sync fails hands none (fsyncgate: a failed sync guarantees nothing), and
the open fails naming the segment and the fix. Before 2026-10-01 the clean path replayed
without syncing. A process restart then replayed an unsynced frame and persisted the store
past it, at open or merely by closing it (redb's close commits durably). The next power
loss took the frame, and `Db::open`'s own "the store outran durable truth" assert fired.
Realtime found it (seeds 0x7df8c43a1d2591e8, 0x5d889f4c1a9ac543).
**What the sync does not close.** On Linux a failed fsync can mark the dirty pages clean
(fsyncgate). The frame then stays readable without ever reaching the disk, and the next
process's fsync succeeds without writing it. Recovery's sync closes the process-death case
(a kill between a frame's write and its fsync) and narrows the failed-fsync case to what
the kernel reports. It cannot close it: no later fsync recovers a write the kernel has
already dropped. The sim models an honest kernel, where a failed sync leaves the write
pending and the next sync writes it.
The pins are `a_record_recovery_replays_survives_the_next_power_loss` and
`a_recovery_whose_log_sync_fails_replays_nothing` (charpente-core `tests/engine.rs`) and
their WAL-level twins in charpente-sim `tests/wal.rs`. There,
`a_frame_recovery_replays_from_a_non_tail_segment_survives_the_next_power_loss` holds
the every-segment half: a tail-only sync loses a non-tail segment's unsynced frame and
the next open refuses the log as a gap. The class's oracle is
`the_store_never_outruns_the_log_across_process_restarts_and_power_cycles`, which mixes
§1's `Sim::restart_process` with power cycles.
**The rule covers every boot-time read of a durable file, not only the log's.** A read
that a boot acts on syncs first, or has a stated reason it need not:
- The role file and the witness slots barrier their names and sync their bytes before
  they are trusted (§5).
- The horizon breadcrumb need not. It is written only after the store's checkpoint
  returned `Ok`, so even a value still in the page cache is a lower bound on durable truth
  and never over-claims.
- The store's `applied_seq`, which `Db::open` trusts before recovery runs, rests on redb:
  4.3.0's open fsyncs the store file (§4).
- Restore's frontier probe reads the destination's segments unsynced because it decides
  only to refuse or to replace. A refusal writes nothing, and replacing discards every
  local byte.
- One layer up, the bucket: `FsObjectStore` syncs every object a `Get` or `List` returns,
  and the directories down to it, before returning it (§1). An archive that opens and
  adopts its manifest acts on that `Get`.
The log gains a verified read surface for the shipper (`Wal::read_from`/`Db::read_log`,
M1 replication T3): re-checksums every frame it hands back and refuses, by name, to hand
back one that fails — a frame corrupted on disk after append must never travel. That
surface's second entry is `Db::sealed_segments` (M2 object-storage-archive T5): the
sealed WAL segments, oldest first, excluding the active tail, each paired with its end
BY LAYOUT (the next segment's base minus one, inclusive) — the archive's completeness
*target* for that segment, never the `end_seq` it is entitled to record in a manifest's
`SegmentRef`, which must come from what `read_log` actually returned instead.

**Three durability fences.** (1) *Held (recovery-horizon rule).* A cut can only ever
discard unacknowledged data, so recovery must tell a torn tail from mid-log corruption
of committed frames. `Db::open` reads the store's `applied_seq` before recovery — the
durable horizon — and threads it through `Engine::recover` into
`Wal::open_with_horizon`: an invalid frame at or below the horizon is a hard corruption
error naming the frame, the horizon, and restore-from-replica/backup, never a cut (and
the error path mutates nothing — no truncation, no segment deletion); above the horizon
it is a torn tail and cuts exactly as before. The rule's other half guards the state no
invalid frame can reveal: if the log's `base_seq` exceeds the horizon + 1, whole
acknowledged records exist in neither the store nor the log (never a state checkpoint
GC can produce — its floor is bounded by its own checkpoint's `applied_seq + 1`), and
`Db::open` refuses with the same restore-naming error before recovery runs. Standalone
`Wal::open` (no store in sight) keeps cut-at-first-invalid. M1 replication is what
makes repair possible. One shape is exempted because it provably loses nothing: a
checkpointed store beside an *entirely empty* log directory. Segment names are
dir-barriered before any frame lands in them (fence 2, below), so every route to a
checkpointed store leaves at least one name-durable segment — except a crash inside
`Seeder::finish`, between the store checkpoint and laying the seed segment. `Db::open`
recognizes the shape and lays the segment `finish` would have (base = horizon + 1,
nothing to replay) instead of starting a fresh log at 0 (found by replication T6, seed
0xcaef83b2feb5edf5).
A store discard-and-rebuild (§4) used to erase the store's own `applied_seq` *before*
recovery ran, collapsing the horizon to `None` and reopening the exact hole this rule
closes — the store WAS the witness, and the rebuild throws it away first, so a corrupt
frame that was genuinely at-or-below the last checkpoint got cut instead of refused
(horizon-collapse, `.superpowers/sdd/horizon-collapse-investigation.md`, fixed 2026-07-23).
A durable horizon breadcrumb outside the store (`<store_path>.horizon`, written and
barriered by `Db::checkpoint`/`Seeder::finish` strictly *after* the store's own checkpoint
persists) survives the discard: `Db::open` now feeds `max(store.applied_seq, breadcrumb)`
into recovery, so a corrupt frame at-or-below a prior checkpoint still refuses by name even
once the store that recorded it is gone. Two fixed slots hold the breadcrumb (`Disk` has no
rename), each write targeting the slot holding the smaller/invalid value, so a torn
overwrite of one slot can never destroy the other's still-valid prior value; the breadcrumb
is always a lower bound (never over-claims) and degrades silently to no witness at all if
absent, torn, or CRC-invalid. `Seeder::new` discards any breadcrumb alongside the store and
WAL it already discards, so a stale breadcrumb from a different, higher-numbered prior
timeline sharing the disk can never survive a reseed and over-claim protection the new
timeline never earned. The `.horizon` sidecar is part of the node's durable set: any
manual file-level restore of `store` + `wal/` must copy it or delete it alongside them —
a leftover sidecar from the node's prior life would over-claim and refuse (by name, never
silently) a legitimate torn tail above the restored horizon. The residual window *above*
the last breadcrumbed checkpoint stays exactly as unprotected as ordinary above-horizon
recovery always has been — Noah-ruled: normal-recovery parity, not fail-closed.
(2) *Held.* `File::sync` does not persist a file's *directory entry*, so a freshly
rotated-in segment's name (or a store file's, at creation) could vanish on power loss,
taking acknowledged records with it, or a removed name could resurrect one a recovery
already discarded. `Disk::sync_dir(prefix)` is the barrier: on `Ok`, every prior
create/remove of a name starting with `prefix` is durable — same flat-namespace prefix
contract as `list`. The prod impl fsyncs every directory a name under the prefix sits in and
every directory above them up to the data directory's parent — the whole chain on every
barrier, because a boot that created a directory and failed before syncing it leaves one that
exists without being durable — and `ProdDisk::new` syncs every directory it creates into its
parent before it returns. Until 2026-10-01 it fsynced only the holding directory, and on a fresh
node the store's creation synced the data directory before `wal/` existed, so writes
acknowledged before the first checkpoint rested on an unsynced `wal` entry (held by `CrashFs`,
§1). The sim tracks pending
creates/removes and, only when `FaultConfig.dir_entry_loss > 0.0`, independently rolls
each pending entry at crash (an affected create vanishes even if its bytes synced; an
affected remove resurrects the file with its last durable content) — zero rng draws when
the field is 0.0 (its default), so every pre-existing pinned seed replays unchanged. The
log barriers rotation (before the new segment takes writes), post-cut recovery cleanup (a
durably-clean directory after truncation), and **every successful open, including the
clean recovery path that cut nothing** — a barrier that FAILS leaves its names pending and
the segment file still on disk, so without that last one a reopen finds a valid log and
acknowledges appends into a segment whose name a crash can still delete (found
2026-09-07, seed 0xb957ceaaf1716fd5). The store barriers creation and the
rebuild-discard-then-reopen path. Coverage key `fault:dir-entry`.
(3) *Held (recovery-cut ordering).* A non-tail cut truncates one segment and removes the
orphaned later ones — two separate durability events. Truncating first strands a durable
gap whenever a crash keeps the truncation but drops the removals: a shortened segment
beside a surviving orphan, which the gaplessness check (fence 1's `refusing to guess`)
then refuses to recover across, bricking the node on data it was entitled to discard.
Recovery removes the orphans first — highest base first, each `sync_dir`-barriered,
exactly as `gc` removes ascending — and truncates last, so every crash-intermediate is a
contiguous prefix, recovery re-finds the same cut, and it is idempotent at every crash
point (found by replication seed 0x4177819b4a2ba984; the test-side restore path that had
masked it now gates on `checks::dir_entry_loss_fired`, so a platform-created gap fails
loud again).

**Record payload (v0, implemented).** A committed transaction's effects:
`[n: u32]` then n tagged effects — put `0x00 [table][row]`, delete
`0x01 [table][id]`, enqueue `0x08 [job][payload][key: Option<String>][shape: u64]`,
emit `0x09 [event][payload][key: Option<String>][shape: u64]`, scoped enqueue
`0x0a [job][payload][key: Option<String>][shape: u64][scope: u64]` (a
`#[job(scoped_by = …)]` job's tenant, spec 2026-09-27-job-tenancy §6), migrate-put
`0x04 [table][id: u64][row]` (§3's `MigratePut`, below), strings and byte
fields length-prefixed per the row codec. The shape is the payload type's
structural fingerprint (§3). The pre-shape `0x02`/`0x03` (the same without the
shape) still decode, with the shape unknown, and are never written. A
tenant-scoped table's (`Row::Scope != Unscoped`, tenant-scoping Task 4) put
and delete carry the scope instead: `0x05 [table][scope: u64][row]`,
`0x06 [table][scope: u64][id]` — chosen by the table's own scopedness, never
by the scope's value, so a scoped table still uses `0x05`/`0x06` at scope 0
(tenant 0 is a legal tenant, not "no scope"); every other table's put/delete
stays exactly the `0x00`/`0x01` shape above, byte-identical to before tenant
scoping. A scoped table's `MigratePut` (tenant-scoping Task 6c) is
`0x07 [table][scope: u64][id: u64][row]` — a NEW tag rather than a widened
`0x04`, the same choice `0x05`/`0x06` made over `0x00`/`0x01`, so `0x04`'s
bytes for an unscoped table stay exactly as above. The scoped enqueue is a
new tag rather than a widened `0x08` for the same reason, chosen by presence
and never by the tenant's value, so tenant 0 is `0x0a` too; its `JobRow`
carries the tenant as a trailing `scope`, absent for every other row. `0x0b`
is the next free tag. A binary that does not know a tag (older, or a later
one it hasn't caught up to) refuses it by name rather than misreading a
wider record.
Table effects are the transaction's collapsed write set in (table, scope,
id) order — intermediate overwrites inside a transaction are observable by
no one and are not logged; enqueues and emits keep call order. Replay
resolves table names through the `Schema` registry passed to `Db::open`. The
enqueue key is the job's stable external identity — carried, never
interpreted inside the deterministic boundary in v0; M1's external-IO
surface is what consumes it.

**Group commit (M2 sub-project 2, 2026-08-15).** The commit point is a *group*, not a
transaction. `Db::begin_group` opens one store snapshot and an empty group overlay;
`Group::stage` runs one handler against it and buffers that handler's effects record without
touching the WAL; `Group::commit` appends every buffered record, syncs **once**, then
applies them in seq order. `Db::transact` is the group-of-one case — `begin_group`, one
`stage`, `commit` — so the single-request path and a batched path execute the identical
instruction sequence instead of being two paths that tests must keep in agreement.
`Engine::submit_all` is the one append→sync→apply implementation underneath both;
`Engine::submit` is its length-1 convenience and has no production caller. What this buys is
amortization and nothing else: N transactions, one fsync (docs/BENCH.md).

**Ack-after-durable survives structurally rather than by discipline.** `stage` never
appends, every reply is held until `commit` returns, and apply stays strictly after sync —
so no participant is ever acked for a record that is not durable, and the reverse error, a
200 for something not durable, remains impossible.

Note what is deliberately **not** claimed: the batch is not durability-atomic. A batch large
enough to straddle a segment boundary takes an extra barrier inside `Wal::append`, which
calls `rotate`, which opens with `tail.sync()` — the same "rotation syncs the old segment"
rule §2's log format states above. A crash between that rotate and the batch's own sync
therefore leaves a durable **prefix**, not nothing. This costs no guarantee, because nothing
in the batch was acked: the replies were still held, so no client was ever told those records
existed, and an unacked record that survives a crash is the ordinary "the write may be
committed" case the client contract already covers (docs/GUIDE.md §5). Atomicity of the batch
was never the promise; ack-after-durable is, and a prefix cannot violate it.

**The two-level overlay.** A handler reads through its own transaction overlay, then the
group overlay, then the store snapshot: read-your-own-writes across the whole batch, not
merely within one transaction. On `stage` returning `Ok` with at least one effect the
transaction's overlay merges into the group's and its record queues for commit; on `Err`, or
on no effects, that overlay is discarded and the group is untouched. The ordering is what
makes panic-safety structural — a handler that panics mid-transaction cannot corrupt the
batch, because its writes were never merged. Row puts and deletes, secondary-index effects
and session rows are all in the overlay. Platform-allocated ids (`next_job_id`,
`next_event_id`) deliberately are not: no API hands one to a handler, and the counters bump
at apply in seq order, which is what keeps ids byte-identical on a follower that has no
overlay at all.

**The overlay carries rows only, and that is the one user-visible semantic group commit
changed.** Enqueues, emits and migrate-puts are not in it, so a transaction batched *behind*
a sibling's `enqueue` or `emit` does not see it — where before batching it would have, since
each transaction applied before the next took its snapshot. `JobRow` and `EventRow`
materialize at APPLY from the log record's enqueue/emit sections, never from anything
`Group::stage` merges. Read-your-own-writes across a batch therefore means rows, not queue
and event effects. Pinned by
`enqueues_and_emits_are_invisible_to_a_later_sibling_in_the_same_batch`
(`crates/charpente/tests/group_commit.rs`, seed `0x60C1_0007`), which also asserts all four
requests land in one batch — the property exists only across a batch.

Scope that panic claim to the batch's *state*: the group is safe, but there
is no `catch_unwind` on the request path, so a panicking **route** handler still unwinds out
of the kernel and stops the node — and batching widened the blast radius from one request to
every reply held in that batch, which `serve` then answers with a generic 500. The job arm
is genuinely isolated (`dispatch.rs` catches inside the transaction closure and converts the
panic to an app error), which is why the two handler kinds are stated separately rather than
folded into one claim.

**What a participant SAW, not what it wrote, is the failure rule.** Every record in a batch
is durable before any of them applies, so an apply failure at record *i* leaves all N on
disk and destined to apply on restart. The participants that must not keep their answers are
the one whose record failed and everything that *observed* it — including a read-only
request, which sees a sibling's uncommitted effects through the group overlay. One
implementation of that rule (`CommitFailure::invalidates`) is shared by `Db::transact`,
`App::handle_or_subscribe` and the kernel's batched tail; three copies of it is how they
drifted the first time. Its client-facing half is a named contract in docs/GUIDE.md §5: a
500 never promises rejection, only absence of confirmation.

**The batch cap is a constant, not configuration.** `MAX_BATCH = 64`, asserted inside
`Group::stage` so the bound is enforced rather than advisory, and shared by the kernel
rather than duplicated as a literal. `DbConfig` carries operator-facing policy; a batch bound
is internal mechanism, like `REPL_TICK_NANOS`. There is nothing here for an operator to
tune — a batch is exactly what the arm has ready at this loop iteration, so greedy drain is
already self-limiting by arrival rate and the cap exists only to bound buffered memory
(`MAX_BATCH` times the existing 1 MiB body cap). A configurable throughput knob would also
be a knob tunable to flatter a benchmark, which is the one thing not to ship next to
docs/BENCH.md. The kernel's arms bound on *items drained*, never on records staged: a
handler that returns `Err` or writes nothing stages no record, so a record-bounded loop
would be unbounded in exactly the adversarial case the cap exists for.

**Batching is within one ready-set arm, never across arms.** Requests batch with requests and
due jobs with due jobs; arm arbitration is untouched. No timer, clock read or entropy draw
enters the commit path — batch composition is a pure function of the ready set, which the
same-shape rule (§1) already governs, so prod and sim agree by construction. Checkpointing
stays at the commit funnel and may occur only *between* batches, never inside one. Two things
end a request batch early: the cap, and a subscribe — its cursor is resolved after the
commit, so a sibling staged behind it would have its events committed before that
resolution and the tail would start past them, which is exactly the gap realtime's
membership oracle forbids.

**Isolation.** Serializable, trivially: transactions apply one at a time to the
single-threaded core. Read-only queries run against consistent snapshots (see §4) and never
block writes. Long transactions are rejected by construction — a transaction is CPU-bound
by definition, so "one at a time" costs microseconds, not milliseconds.

## 3. The typed surface

Every contract in the system is a Rust item; every contract violation is a compile
error. App crates depend on `charpente` alone: all three derive macros resolve through
`::charpente::` paths (`charpente::core` re-exports the deterministic core), so the
platform is one dependency, not two (plus `serde` for your own wire types — GUIDE §1).

- **Tables.** `#[table]` on a struct derives the row codec, key type (`Id<T>`, typed, never
  a bare u64), and secondary index declarations. Schema *is* the Rust definition. Every
  table declares its tenancy — `scoped_by = Org` or `global` — and a bare `#[table]` is
  a compile error naming both (spec 2026-09-16-default-secure-surfaces).
- **Scope.** `#[table(scoped_by = Org)]` sets a row's `Row::Scope`, and a scoped row's key
  becomes the `(scope, id)` composite (every secondary-index entry gains the same prefix) —
  a tenant is a key prefix, not a `where` clause someone has to remember. A
  `#[table(global)]` gets `type Scope = Unscoped`; a table that declares neither is a
  compile error (the Tables bullet above). The scope type owes exactly one thing, an
  `impl Tenant` with its own `authorize`; `scoped_by` without one is a compile error at the
  struct. The six scope-taking primitives on `Tx` are private to `charpente-core`, so the
  ONE way to reach a scoped row is `tx.scope::<Org>(org, ctx)?` — the single auditable line
  per handler — which runs the app's `authorize` (or, on a route declared `public(Org)`,
  admits by that declaration) before it returns the `ScopedTx` that
  `get`/`scan`/`scan_from`/`scan_by`/`put`/`delete` live on. `authorize(tx, who: &Principal,
  scope)` is asked only about a principal the resolution ladder produced (`Principal`'s
  fields are private): a caller with no credential is a 401 at the scope line, and the
  declared ways in, `public(Org)` and the operator's `TenantAdmission`, ask nobody (spec
  2026-09-27-job-tenancy §3). Both the marker
  (`NoTenant`) and the trait `View` actually bounds on (`UnscopedRow`) are sealed, so an app
  cannot re-open a scoped table with one orphan-legal line; a scoped read on a bare `Tx` is an
  unsatisfied trait bound whose text names the table, its scope, and the `tx.scope` line to
  write. `ScopedTx` derefs to `Tx`, so global tables stay reachable
  (`(*tx).scan::<Region>()`), and a transaction opens exactly one scope. Full treatment in
  §8; spec `docs/superpowers/specs/2026-08-30-tenant-scoping-design.md`, GUIDE §16.
- **Migrations.** `#[table(version = N)]` versions a row's shape; a bump without a
  registered `TableMigration` is a compile error. Ordinary Rust already catches a query
  referencing a field that no longer exists — the mechanism below is for what Rust
  cannot see: a same-length field swap or retype that recompiles clean and misdecodes on
  disk.
- **Routes (v0 implemented).** `#[route(METHOD "/path/:param", auth | public | public(Org)[, cross_tenant])]` —
  handler args: `tx: &mut Tx`, one per `:param` (same name, path order), at most one JSON
  body arg, optional `cap: &CrossTenant` (see below), optional trailing `ctx: &Ctx`
  (session access); return `Result<T: Serialize, ApiError>`.
  Handlers run inside one transaction; 200 is emitted only after the commit is durable.
  The OpenAPI description will be *derived from* the types, never hand-written (step 7).
  `cross_tenant` is the declared capability to enumerate a scoped table across every tenant
  (tenant-scoping spec §3): the kernel mints a `CrossTenant` token for a route that declared
  it, `#[route]` fills the handler's `cap` argument from it, and `Tx::across_tenants::<S>
  (cap)` requires one — so a handler on a route that did not declare it cannot compile the
  call, and `#[job(…, cross_tenant)]` says the same for a background rollup (whose `run` then
  takes the token after `tx`). The rows come back PAIRED WITH THEIR SCOPE —
  `AllTenants::scan` yields `Vec<(Id<Org>, Invoice)>`, never `Vec<Invoice>` — so a
  cross-tenant result cannot be handed to code expecting one tenant's data. Naming a tenant
  needs no capability: a point read already supplies the scope. A route declares exactly
  one of `auth`, `public` or `public(Org)`; none is a compile error naming all three.
  `public(Org)` declares that every caller, credentialed or not, may act inside the `Org`
  tenant the handler opens, with no `authorize` (spec 2026-09-27-job-tenancy §4). On any
  other route, a scope line reached with no credential is a named 401, so `authorize` is
  only ever asked about a real principal.
- **Jobs.** `#[job(global)]` or `#[job(scoped_by = Org)]` structs, with typed payloads and
  a typed retry policy. Neither tenancy is the default, and a bare `#[job]` is a compile
  error naming both. A `global` job's `run` takes `&mut Tx`; a scoped job's takes the
  `ScopedTx` of the tenant it was enqueued in, which the dispatcher reopens from the tenant
  its record carries (§6). A scoped job is enqueued only from inside its tenant
  (`ScopedTx::enqueue`) or by a declared fan-out (`Tx::enqueue_in` with a `CrossTenant`).
  `Tx::enqueue` and cron take `global` jobs only, and a scoped job at either is an
  unsatisfied bound whose text names the fix. Enqueue sites and handler signatures are
  checked against each other (spec 2026-09-27-job-tenancy §5). **Job rows are the
  platform's to write, and every transaction's to read**, because the dispatcher opens
  the tenant a row carries without asking anyone: `JobRow`'s `Row::Scope` is the sealed
  `PlatformOwned`, so `Tx::put`/`Tx::delete` of one is an unsatisfied bound naming the
  enqueue family; a hand-written `Row` that borrows the `charpente.jobs` name fails to
  build at each reachable write (a `const` check, reported by `cargo build`, not `cargo
  check`); and the dispatcher's Done mark and failure record go through
  `Tx::settle_job`, which takes a `TenantAdmission` only a `Group` mints. An enqueue whose
  tenant presence disagrees with the mounted declaration (a hand-written `impl Job` or
  `JobDef`) is refused by name, and so is a Pending row that disagrees, before dispatch
  starts (job-tenancy final review C1, M4). So is an enqueue in a tenant of another scope
  type than the mounted job declares (`JobDef.scope_type`, a `TypeId`): a row records a
  tenant id and not its type, and the fingerprint hashes the `scoped_by` path as spelled, so
  a stand-in copying `NAME` and `FINGERPRINT`, or a `#[job]` whose path spells another type
  alike, would otherwise run the job in the same id of the declared type. The check needs
  both types, so it runs at enqueue only. A Pending row enqueued by any other binary has no
  type to check when a later binary that mounts the name dispatches it, in the scope type
  that binary declares. The enqueuing binary may not have mounted the name at all, or it may
  have mounted it under a `scoped_by` path spelled alike but naming another type: a refactor
  that turns a job module's `use billing::Org` into `use crm::Org` keeps the fingerprint,
  so the job's Pending rows run in the same ids of `crm::Org`. Where the shape matches,
  nothing refuses either case, so the operator drains such a job (runs the binary that
  enqueued its Pending rows until they are Done or Dead) before the deploy (final review
  N1, NB2).
- **Events.** `#[event]` structs; `tx.emit(...)` is typed; subscribers (server-side and
  client-side codegen) consume typed streams off the log. An event payload is itself a
  wire type (it is exactly the JSON a subscriber receives), so `#[event]` derives `Wire`
  — and `events![…]`/`App::mount_events` register the type for `describe` only.
  Delivery deliberately has NO registry: `Subscription::of::<Ev>()` captures its encoder
  statically at the subscribe site, which is what makes "unregistered event at delivery"
  inexpressible rather than an error case.
- **Wire.** `Wire` is the trait describing what a type looks like once serde has
  encoded it — the mechanism behind the client-side codegen named above and behind
  `Id<T>`'s promise to never be a bare u64. `#[derive(Wire)]` implements it for a
  struct by reading serde's own `rename`, `rename_all`, `skip`, `skip_serializing`,
  and `skip_serializing_if`; any other `#[serde(...)]` attribute is a compile error
  rather than a silent pass-through, and that supported set is pinned by tests
  comparing the derived schema against real `serde_json` output. The one deliberate
  exception is `#[serde(crate = "...")]`, accepted and ignored on both structs and
  enums: it redirects serde_derive's own generated paths (`#[event]` emits it so app
  crates stay single-dep) and changes no field, name or wire shape, so there is nothing
  for the schema to get wrong — the `.stderr` pins list it as supported alongside
  `rename_all`. For an enum it
  describes whichever of serde's *tagged* representations the type actually uses
  — a unit-only enum as a string union, otherwise external (default), internal
  (`tag`), or adjacent (`tag` + `content`); `untagged` is refused outright, since
  an untagged value carries no discriminator for the schema to key on. It refuses
  at compile time any variant shape a representation still can't faithfully carry
  (e.g. a newtype variant under internal tagging, which serde either refuses to
  serialize at all or merges into the tag object in a way no per-variant schema
  slot can reconstruct).
  `TypeSchema`/`TypeDef` are the IR a generated client is emitted from. 64-bit
  integers are deliberately not wire primitives: a JSON number loses precision
  above 2^53 in JavaScript, so they cross the wire string-encoded. That guarantee
  survives the process boundary: `Id<T>` is itself string-encoded on the wire, and the
  generated TypeScript client brands the string (`type CustomerId = string & { readonly
  __table: "Customer" }`), so a value typed for one table cannot satisfy a parameter
  typed for another. `WireU64`/`WireI64` are the sanctioned carriers the `Wire` bound
  requires in place of a bare `u64`/`i64` — which is a compile error at any wire
  position, naming the fix, because a JSON number silently loses precision above 2^53 in
  JavaScript — and they cross the wire the same way `Id<T>` does: as decimal strings,
  surfacing in the generated client as `bigint`.
- **Config.** A typed struct, no stringly `env::var` in app code. Startup validation is not
  built yet — there is no `fn validate` on `AppConfig`/`DbConfig` today.
- **Telemetry (M2, not built).** Counters, histograms, and log fields are declared as types
  with change-time checks: deleting a metric a dashboard depends on, or renaming a label, is
  a detectable breaking change against a lockfile of the observability contract, the same
  way Cargo.lock pins dependencies. Instrumentation that cannot silently rot. Nothing in this
  bullet exists today — `describe.rs`'s `TelemetryInfo` is an empty struct.

**Migrations (v1, implemented; spec `docs/superpowers/specs/2026-07-16-migrations-design.md`).**
The organizing rule: no code path ever decodes row bytes under any version other than the
one they were encoded at; where that cannot be guaranteed, the node refuses by name instead
of guessing. `#[table(version = N)]` defaults `N` to 1 — every table until now is version 1
with no code change required. Bumping to `N >= 2` requires keeping the prior version's
struct in code under an explicit name (`#[row_shape(version = N-1)]`, codec-only — no table
identity, no indexes, no redb registration) and `impl TableMigration for CurrentStruct {
type Prev = PriorStruct; fn migrate(prev) -> Result<Self, MigrationError>; }`; the macro
emits a const assert requiring the impl, so a bump without one is a missing-trait-impl error
at the struct, not a runtime surprise. Every version also carries a structural fingerprint —
a hand-rolled, fixed 64-bit FNV-1a over its `(field_name, type tokens)` sequence (the tokens
rendered by the macro itself, one space between them, never by the compiler's printer, whose
spacing is not stable across toolchains) — computed at
macro-expansion time and exposed as `Row::FINGERPRINT: Option<u64>` — that catches the hole
compile-time checking cannot: a same-shape struct edited without bumping `version`.
`Row::FINGERPRINT` defaults to `None`; a hand-written `Row` impl (the platform's own
`JobRow`/`EventRow`/`SchemaVersion`) stays `None` and is invisible to the gate — silently
opting a table out of drift protection is the cost of not using `#[table]`.

**Payload shapes.** `#[event]` and `#[job]` carry the same structural fingerprint
`#[table]` does (`Event::FINGERPRINT`, `Job::FINGERPRINT` — required on a hand-written
impl, no default), and every emit and enqueue records it in the log beside the payload.
A payload is decoded only under the shape it was written with: delivery refuses a
foreign-shape event by name and ends that one tail, and a node refuses to dispatch while
a Pending job of a type it mounts carries a foreign shape (§6). There is no per-row
versioned decode — the migrations spec rejected it twice — and no override (MANIFESTO §8).
The fingerprint hashes type tokens, not definitions, so a change inside a type a field
holds keeps it (GUIDE §12). A decode failure under a matching fingerprint is therefore
reported naming both causes — that change, or corrupt bytes — with both fixes.

**The gate, the barrier, and the seven refusals.** A built-in table,
`charpente.schema_versions`, records each versioned table's last-known `(version,
fingerprint, migrating_through, migrating_to)`; its row is written lazily, piggybacked on
the table's first data write in the same effects record (adoption-at-first-write —
`Db::open` writes nothing unless it actually migrates, so pinned seeds' fault schedules are
undisturbed). The read-only check runs at every `Db::open`, on every node — including an
ordinary follower's, where it is a no-op admit for a converged, same-binary node. The
rewrite itself is a separate step, `migrate_schema()`: the kernel runs it at startup,
before serving anything, on a node that is writable (single node or primary); a follower
runs it only at promote (resuming any in-flight watermark the dead primary left). An
ordinary follower never runs it — it receives the rewrite only through shipped
`MigratePut` records (below). Ordinary WAL-tail records for a
table below its compiled version replay decode-free (a row's id — or, for a scoped table,
its `(scope, id)` composite — is a version-independent leading prefix, so no
foreign-version decode is needed, and a scoped table's below-compiled Put/Delete carries
its scope through this path exactly as its compiled-version replay does); the authoritative decision runs
once replay completes, admitting a table whose history reached the compiled version or is a
resumable in-flight migration, and refusing everything else by name. Seven named refusals
cover the space: fingerprint drift under an unchanged version; version downgrade; the
upgrade barrier itself (a foreign, unrecovered WAL tail from an older binary); its
in-flight-target variant (an interrupted migration aimed at a version other than the
compiled one); its in-flight-target-fingerprint variant (an interrupted migration to the
compiled version, but begun by a binary whose target shape differs — the persisted
`fingerprint` field already carries the original target's shape, so resuming under a
different one is caught without a new field); a missing migration chain link; and a
migration-transform failure (a rejecting `TableMigration::migrate`). Two checkpoints — on
clean shutdown, and at open after a replay — make the barrier's remedy exactly "run the
previous binary once to a clean shutdown, then upgrade": no new operator verb.

**Execution: `MigratePut`.** The rewrite is an ordinary sequence of `Db::transact` calls
using one new effect kind, `MigratePut(table, id, bytes)`, applied everywhere (live commit,
crash-replay, follower apply) by overwriting the row's stored bytes without decoding the old
value and leaving indexes untouched — the second decode-under-the-wrong-version path
(`Writer::replace_row`'s index-upkeep decode) never sees a mid-migration row. The executor
batches `DbConfig::migration_batch_size` rows (at least 1, default 1000; 0 is refused by
name before anything is written, since an empty batch is the close-out signal) per
transaction over a bounded range read, committing each batch's rewrites and its watermark advance
(`migrating_through`) together so a crash between them is impossible by construction; the
closing transaction clears the watermark and bumps `(version, fingerprint)` to the target —
the single moment a table is considered migrated, and the moment its apply trigger rebuilds
that table's secondary indexes wholesale (deterministic, idempotent, so primary and
followers rebuild at the same log position with no separate mechanism). `migrating_through`
is `Option<(u64, u64)>` — `(scope, id)` — for every table, scoped or not, since a bare-id
watermark cannot resume a composite-keyed table's bounded range read correctly (ids are not
globally ascending across a scope boundary, so a scoped table's migration walks the same
`(scope, id)`-ordered cursor `scan_raw_from` does). The persisted row tells an old
mid-migration watermark from the current shape by LENGTH, never by guessing: encoded as
`[1][scope: u64 le][id: u64 le]` (`migrating_to` is always `Some` exactly when
`migrating_through` is), the two shapes' possible byte counts are disjoint, so a binary from
before scoped tables existed is refused by name rather than misread as a fabricated scope —
an idle row (`[0]`, no in-flight migration) is byte-identical either way.

**Migrations: replication.** Followers never migrate locally: the primary's migration is
ordinary log records, received through the existing shipping path unchanged, applying
`MigratePut` decode-free and rebuilding indexes at the close-out apply like any other record
— a fresh follower is snapshot-seeded with the primary's `schema_versions` rows already in
it, a converged follower is simply behind, and a follower promoted mid-migration resumes the
remaining batches itself at promote. `Hello` gains `schema_digest: u64` — an FNV-1a fold
over every fingerprinted table's `(name, version, fingerprint)`, checked after the
peer-secret and protocol-version checks and refused by name on mismatch, mirroring the
`proto` refusal — so a schema-mismatched peer never seeds or ships a single frame. While a
table's migration is in flight to a follower its copy is mixed-version and must not be read
under the compiled type; v0 followers serve no typed app-table reads today (jobs/cron are
already follower-gated, realtime tails read `charpente.events`, untouched by table
migration), so this is a documented invariant rather than an enforced one.

**Query API [reversible — start simple].** V0 is get/put/scan over typed keys plus typed
secondary indexes — deliberately KV-shaped, not relational. Rich predicates, joins, and
aggregation arrive as typed combinators once real usage shows which ones matter. A
read-only Postgres-wire endpoint (for BI/analytics tools) is a later, separate surface and
never the primary API. Read accessors are generated against the `View` trait —
implemented by both `Snapshot` (outside transactions) and `Tx` (inside one, where reads
also see the transaction's own writes). Follower reads need zero app-code changes:
effect-free transactions run unchanged on any node, while an effectful one on a node
marked a read-only follower (`Db::forbid_writes`) is rejected with `TxError::NotPrimary`
before it ever reaches the log (§5).

## 4. Storage engine [reversible — staged]

**Stage 1 (M0): `redb`** (pure-Rust embedded B-tree) mounted *behind the `Disk`
simulation boundary* via its backend abstraction, so crash/torn-write fault injection
exercises the real storage code from day one. Rationale: proven, pure Rust, and the WAL +
snapshot design above does not depend on the tree's internals.

**Stage 2 (post-M1): purpose-built LSM.** The log-structured write model wants log-
structured storage: the WAL is already the L0. Building our own engine is the scariest 20%
of the project and also the moat — but it becomes *safe* to build only once the simulation
harness exists to prove it. Sequence accordingly: harness first, engine second. Swapping
engines under the trait is a contained change because nothing above the `Disk`/table layer
knows tree shape.

**Table store (v0, implemented).** redb mounts behind `Disk` via a `StorageBackend`
adapter (`table::Store::open` — the raw-path constructors are deny-listed). The store is
a materialization of the log, never a second source of truth: `apply_batch` commits a
contiguous run of records' row effects atomically with the `applied_seq` meta key in ONE
redb write transaction at `Durability::None` (no fsync asked for — the WAL is the
durability story; redb 4.3.0 keeps such a commit's pages in its own write buffer rather than
writing them to the file, and does fsync the file after extending it), and every record
in the run commits or none of them does: an apply failure anywhere rolls the whole batch back and reports nothing applied — so the `applied_seq`
that transaction leaves durable is always a batch boundary, never an interior seq of a
batch that did not commit. `apply(seq, …)` is that batch with one record, and does exactly
the disk work a per-record commit always did — one read txn, one write txn, one commit —
which is what keeps a never-contended universe tracing identically to pre-batching code.
What widened is the *store's* commit point, not the log's; the WAL's durability stays
prefix-shaped (see "the batch is not durability-atomic" under group commit above).
`checkpoint()` persists everything at once; after a crash the store reverts to its last
checkpoint and recovery replays the WAL from `applied_seq + 1`. Two redb facts the sim
established: its byte layout is deterministic under identical op sequences
(raw-fingerprint replay holds), and `Database::drop` is a *graceful close* that durably
flushes non-durable commits — a crash is only a crash if the process dies with the
database open, which is exactly what the sim models. A third is a dependency charpente does
not re-check at runtime: redb 4.3.0's writable open fsyncs the store file
(`Database::new` → `TransactionalMemory::begin_writable`, `db.rs:1672`,
`page_manager.rs:1458`; a repairing open also flushes at `page_manager.rs:888`). `Db::open`
relies on it. It reads the store's `applied_seq` before recovery runs and, beside an empty
log, lays a durable segment at `applied_seq + 1` (the mid-seed-finish shape, §2). A seeder
whose checkpoint fsync failed leaves that `applied_seq` in the page cache only. Without
the open-time fsync, a power loss after the laid segment would leave a store at `None`
beside a log based past it, a node that refuses to open. The D1 review's probe found the
branch safe for that reason alone: `Seeder::finish(5)` with every store fsync failing, a
process restart, then a power loss under the running node, and the reopen held `Some(5)`
(2026-10-01). `a_seeded_store_a_restarted_node_lays_its_segment_from_survives_the_next_power_loss`
(charpente-core `tests/engine.rs`) pins it, so a redb upgrade that drops the open-time
fsync turns it red. Re-walk it with the latch census below on every redb upgrade. Index entries are `[key len: u32][key
bytes][id: u64 be]` (length prefix prevents prefix-collision ambiguity); exact-match
lookups only until a real query pattern demands order-preserving key encoding. A built-in
`charpente.jobs` table materializes transactional enqueues in the same apply as a `JobRow`
per enqueue effect — id from the monotonic `next_job_id` meta counter (bumped atomically
with `applied_seq` in the same store transaction, deterministic across replay), plus name,
payload, key, status (Pending/Done/Dead), attempts, due_at, and last_error — the queue
dispatcher's substrate (§6). Done and Dead rows are retained (GC is a later question,
filed with WAL segment GC). A store that cannot open at all — e.g. a crash tore its
creation — is discarded at `Db::open` and rebuilt by replaying the WAL
from scratch; the log is the only source of truth. "Cannot open" includes a store file
whose length disagrees with its durable header's layout. redb 4.1.0 asserted on two such
states (page_manager.rs:231/:257) — an unsynced `set_len` dropped at crash while the header
god-page write survived (`:231`), or a crash tore a data-page write inside an unsynced
`set_len` extension that dropped, and the surviving sectors left a file length that is not a
whole number of pages (`:257`) (both sim-found; the crash model is exactly ALICE's
unordered-writeback) — and `Store::open` fenced that panic into an open failure. redb 4.3.0
returns `StorageError::Corrupted` for both ("File truncated below stored layout",
"File length does not correspond to a valid region layout"; `header.rs:254`/`:301`), so the
fence is gone: an unopenable store is an ordinary `Err`, and a redb panic, at open or later,
propagates as the real failure it is. 4.3.0 also closes the cause: `grow()` now syncs a file
extension before any header can record it (`page_manager.rs:2372-2379`), so the crash model
no longer produces either state (READ in source, and MEASURED: none in 16,896
`job_tenancy_universe` universes — 4,096 of them at the very bases where 4.1.0 produced two —
nor in 12,800 `kernel_workload` ones). That rebuild holds only while the log still retains
seq 0: `Db::checkpoint` durably persists the store, then deletes WAL segments below the
checkpoint (`Wal::gc`, floored at `DbConfig::retain_segments`, default 2; a caller-supplied
retention pin can hold more), auto-firing every `DbConfig::checkpoint_every` committed
records (default 4096; `0` disables the cadence — manual `checkpoint` only). Once the log
is compacted past seq 0, an unopenable store is no longer rebuildable from the log alone —
`Db::open` checks the log's base before discarding the store and returns a named hard
error instead (naming the compaction, and the fix: restore from a replica/backup, or widen
`retain_segments`) rather than silently losing the records below the old checkpoint.
A second sim-established redb finding (upstream-report material): direct corruption of the
store file's durable bytes — bit flips, misdirected writes — can defeat redb's
copy-on-write crash consistency entirely, either panicking internally or silently falling
back to an older, still self-consistent root that under-reports `applied_seq`; v0 does not
defend store-file bitrot (the corruption fault axes are scoped to `wal/`, §1), redb + M1
restore is the answer, and M2's own engine owns page checksums.

**Store-handle poison (M2 sub-project 0a, implemented).** The store boundary
now follows the same fsyncgate rule as the log (§2 above, DESIGN.md:212-214):
an un-retryable IO failure poisons, and restart-and-recover is the only exit.
redb has its own independent latch: any error from its inner `StorageBackend`
sets `CheckedBackend.io_failed` (five sites — `len`, `read`, `set_len`,
`sync_data`, `write`; `cached_file.rs:222/231/240/249/258` in redb 4.3.0), after which every
further backend call returns `PreviousIo` ("close and re-open the database").
Because that inner backend *is* charpente's `DiskBackend`, charpente observes the
identical condition at its own disk boundary, one layer down — a sticky
`Arc<AtomicBool>`, created in `Store::open`, set in each of `DiskBackend`'s five methods on `Err`, exposed as
`Store::failed()` (a graceful `close()` is deliberately not mirrored — it is
not a failure). `Db::poisoned()` ORs it in via `Db::store_failed()`, so the
kernel's existing request-arm poison stop (the 2026-07-05 ruling, §7) fires
with no new control flow; only the stop message is distinct, because a store
latch leaves the WAL intact and authoritative, unlike apply-poison, which
means durability became unknowable. A test at the kernel level asserts the
correspondence against a genuinely latched store, and it takes two assertions
to do it: one reading charpente's flag, one reading redb's own refusal (a test
that consulted only the mirror could not tell a correct latch from a drifted
one). Both ride the checkpoint-fsync path, so what they pin is drift on that
path in either direction — charpente missing a latch redb took, or charpente
latching where redb no longer does, which would poison healthy nodes into
permanent unavailability and which only the redb-side assertion catches. A
future redb that grows an entirely new latch path, one `DiskBackend` never
sees, is not caught mechanically; that stays a review item against the
five-site census above, to be re-walked on every redb upgrade. The 4.3.0
re-walk (2026-09-30) found the five sites unchanged and one new route that
drifts the other way: `CheckedBackend::write_best_effort` (`cached_file.rs:266`)
calls the inner `write` WITHOUT latching, and the read path uses it to reclaim
non-durable-commit pages from redb's write buffer when buffer plus read cache
exceed redb's cache budget (`:698-712`), discarding the error. `DiskBackend`
cannot tell that call from a latching one and latches on its `Err`, so there
charpente poisons where redb would carry on — the conservative direction (a
restart-and-recover, never a silent pass), and reachable only past the 1 GiB
cache budget, the DERIVED regime below. The new byte-range lock methods keep
redb's default `Unsupported` bodies: no IO, no latch.

Two shapes share the fix but not its evidentiary standing. On a
**cache-resident store — MEASURED** (under redb 4.1.0) — the only reachable
trigger is the store's own checkpoint fsync (store applies commit at
`Durability::None`, so checkpoint is the only store sync charpente asks for;
redb 4.3.0 adds one it does not, the fsync `grow()` issues after extending the
file inside an apply, which reaches the same latch but has not been measured
here); before this fix the latch was completely
silent, since reads kept answering 200 off redb's page cache, and the window
closed only when the next write converted the latch into apply-side poison —
or never closed at all on a node taking no further writes. On a **store larger
than redb's 1 GiB evicting read cache — DERIVED, not measured** —
cache-missing reads also reach the disk boundary and set the same flag, which
covers the shape where a read-first write transaction (the ordinary
get-then-put handler) fails inside the handler and never reaches the
latch-gated `begin_write` (`begin_read` is not latch-gated in either redb
impl) — without the read arm such a node would 500 indefinitely with
`poisoned()` false. That row is justified on redb's verified source, not a
measurement: no store over the 1 GiB bound was built to prove the symptom, by
design.

**Refused before the record exists (implemented).** Apply-poison is for failures the log
cannot take back, so a write the compiled `Schema` already says apply will refuse, or will
misread, is refused earlier. Apply treats every write to a table as one of the type the
`Schema` registered for its name, and three kinds of write disagree with it: a table name the
`Schema` does not register at all; a write whose scope shape is not the registered `Row`'s — a
scoped `Row` whose `TABLE` is an unscoped table's, or the reverse; and a put or delete through
any Rust type other than the registered one, compared by `TypeId` (in memory only: a `TypeId`
is not stable across builds, so it never reaches the log, a fingerprint or `describe`). The
last is easy to reach, because a `#[table]` type's `TABLE` is its bare struct name, so two
`#[table] struct Invoice` in different modules share one, and they can share `VERSION` and
`FINGERPRINT` too: a fingerprint folds field names and type spellings, not `scoped_by` and not
what a spelling resolves to (§3). Before this check, a write apply could not read as the
registered type — an unregistered name (the `Schema` lookup), the other scope shape
(`resolve_effect_scope`), a row that does not decode — was refused at apply, after the WAL
sync, where the record was durable, the engine poisoned, and every restart replayed it into
the same refusal. A write apply could read was stored as the registered type's with no error
anywhere: a row whose bytes happened to decode, or a delete, through a same-named type with
another `scoped_by` wrote or deleted the registered scope type's tenant of the same id; and an
unscoped write beside a scoped one to the same scoped table went out under the table's one tag
and landed in tenant 0. `Group::stage` now refuses all of them by name before the record
exists (`TableWriteRefusal`), naming the table, the type that wrote and the registered type,
as it refuses an enqueue under a foreign job shape; a job handler's transaction that hits it
is an attempt, and a request gets a 500 with its text. Apply's refusals stay for records from
elsewhere: shipped, or written by another binary and replayed under a `Schema` that disagrees.
The check covers writes, not reads: a scoped read through a same-named type with another
`scoped_by` still keys on the table name and the raw scope id, so it reads the registered
scope type's tenant of the same id.

**Snapshots & reads.** MVCC snapshots at log positions; read-only queries (and follower
reads) run against snapshots without touching the write path.

**Blobs (M2, not built).** Blob columns over a configurable size are *to* spill to a
content-addressed blob store (same log-commit discipline for the metadata, direct
streaming for the bytes). Nothing in this paragraph exists today — there is no blob type,
no spill threshold, and no streaming path; a large value is simply bytes in a row.

**Logical export (v1, implemented).** A `Snapshot` also exposes the seed path's read
surface (M1 replication T4): `applied_seq`/`next_job_id` read META off the snapshot's own
transaction — consistent with the rows it is about to hand out, unlike `Store::applied_seq`,
which opens a fresh one and so could observe a later write — and `scan_raw` hands back a
table's raw `(scope, id, bytes)` triples, undecoded, in stored key order (empty, not an
error, for a table that was never created). `scope` is `table::UNSCOPED` for every row of
an unscoped table; a scoped table's physical redb table is composite-`[u8;16]`-keyed, so
`scan_raw` and `scan_raw_from` (the migration/archive-export paged read) take `scoped: bool`
from the caller's own `TableOps.scoped` — the schema's word, never guessed by probing —
and open that shape. Scoping is declared per table (`scoped_by` or `global`, never inferred
— spec 2026-09-16-default-secure-surfaces) and never retrofitted onto an existing one (the
half of the tenant-scoping spec's §8 non-goal that survives): a table's physical shape is
fixed at its first write for the table's whole life, so a `TableTypeMismatch` is always a
schema/disk disagreement, never a state to tolerate, and both directions are refused by
name rather than silently resolved — `scoped = true` over a physically bare table names the
scope-flip contract and its fix ("create a new scoped table and copy rows through a job");
`scoped = false` over a physically composite table names both known shapes. (An earlier
build of this fallback tolerated the `scoped = true` case, reasoning that `migrate_table`'s
resume walk depended on reading it; it does not — the very next `MigratePut` for such a row
dies on redb's own unnamed type error the moment it tries to write the row back under a
scope, so the tolerance only ever bought a silent scope-0 collapse in the three paths that
complete through it: `Seeder::rows`, the archive export walk, and replication seeding.)
`scan_raw_from`'s cursor is `(scope, id)`: a bare-id watermark cannot resume a
composite-keyed table correctly, since ids are not globally ascending across a scope
boundary (tenant 2's id 0 can follow tenant 1's id `u64::MAX`).
`Store::import`/`set_import_meta` are the write-side counterpart:
`import` commits typed rows through the same `Writer` (and so the same index maintenance)
as ordinary `apply`, but with no seq or `next_job_id` bookkeeping of its own — a logical
snapshot has no log position — and `set_import_meta` sets both meta keys explicitly, once,
after every table has landed.

## 5. Replication & deployment [reversible in mechanism, fixed in stance]

- **Mechanism (v1):** asynchronous log shipping to N followers; a follower is promoted on
  primary failure (external arbiter or manual, v1 — consensus-elected promotion can come
  later *without* changing the data plane, because the log format is the contract).
- **Stance (fixed):** one primary, no sharding, no multi-master. See MANIFESTO §7.
- **Deploy:** one binary + one data directory. Upgrades: new binary replays/attaches to the
  same log (schema versions in-log make this well-defined).
- **Backup (M2, built):** log segments + periodic snapshot to object storage,
  Litestream-style, built in — not an add-on. Two distinct things exist and stay distinct.
  `serve::backup` (`crates/charpente/src/serve.rs`) is a replication-only node with no HTTP
  edge that keeps its own durable copy and can be promoted: a hot standby, not an off-site
  snapshot shipper.
  The archive proper (`crates/charpente/src/archive/`) is the shipper — a generation model
  over `<prefix>/manifest`, a full logical snapshot plus the sealed WAL segments that
  continue it, a bounded retention pin that yields rather than letting an object-store
  outage halt the primary, and a sixth kernel arm (§1) driving it one step at a time. A
  snapshot batch object's bytes are its table's own scopedness: an unscoped table's batch
  is unchanged from before tenant scoping (`[count][id, row]*[crc]`); a scoped table's
  batch leads with a `u32::MAX` count sentinel that no unscoped batch can ever produce
  (`SNAPSHOT_BATCH_ROWS` bounds a real count far below it), then the real count, then
  `(scope, id, row)` triples. The export walk carries each table's scopedness alongside
  its name, and restore decodes a batch against the destination schema's word for that
  table — a scoped table found unscoped in the archive (or the reverse) is refused by
  name naming the fix, never silently imported under the wrong shape.
  `serve::archive(app, store, cfg)` is the archive-only edge, and `ArchiveConfig::writer` is
  the identity every manifest records: an archive that meets a manifest written under a
  different identity — at open, or before any swap — stops permanently and reports, rather
  than interleaving two writers' generations into one prefix. Detection, not prevention, so
  the identity has to be unique per node *and* stable across a redeploy.
  Reading it back is `charpente::archive::restore`, behind the `restore` verb every
  `charpente::launch` binary carries (`charpente restore --dir` builds and execs it, as
  `describe` does): it rebuilds a data directory from the newest generation in a bucket, and
  refuses a destination it cannot prove is on that timeline — diverged, ahead, or unrelated
  — rather than merging, with `--force` naming the records it is about to destroy before it
  acts.
  A generation that rolls reclaims its predecessor's objects **by name**, out of the manifest
  and snapshot metadata this writer wrote and only after the successor's manifest is durable;
  the bucket is never listed, so what no manifest ever named is unreachable by design and is
  the operator's to expire (GUIDE §15 states the required lifecycle rule). What is still not
  built is the archiving standby: `Kernel::build` takes a `Repl` and an `Archive` together but
  is private, and every public constructor passes at least one of them `None`, so the
  deployment most operators should choose — archiving *from* the standby, leaving the
  primary's loop untouched — has no public constructor yet.

**Seeding (v1, implemented).** `Db::into_seeder`/`Seeder` builds a follower's (or a fresh
backup's) entire state from a primary's logical snapshot stream, no log replay involved
(M1 replication T4). `Seeder::new` first discards any store file and `wal/` segments
already at the target disk, dir-barriered — seeding always replaces a timeline, never
merges into one; `rows` applies one table's exported `(scope, id, bytes)` triples through
the schema's typed ops exactly as the live apply path does, indexes rebuilt as a side
effect — each row's own scope threads straight to the typed put (tenant-scoping Task 6c;
before it, `rows` refused a scoped table by name, since seed/restore export carried no
per-row scope);
`finish` pins `applied_seq`/`next_job_id`, checkpoints, and lays a single empty WAL segment
at `applied_seq + 1` before handing the result through the ordinary `Db::open` — precisely
the base = horizon + 1 boundary the recovery-horizon rule (§2) already accepts, so open
sees a clean, un-compacted follower with nothing to replay. `Schema` is `Clone` and gains
`table_names` (the export walk order both sides agree on) and a crate-private `remove`:
`Db::open` always adds `JobRow` to the schema it's given, so `Db::into_seeder` — the
producer side, deconstructing a live `Db` into a `Seeder` over the same disk — strips it
back off before handing the schema to a fresh `Seeder`, which re-adds it itself; without
that, seeding a live `Db`'s own state would double-register the platform's own table. The
network-facing seed session (shipping rows to a remote follower) is a later M1 task; this
is the substrate it sends over the wire.

**The data plane (v1, implemented).** `charpente::repl::Repl` is the session engine
(M1 replication T6): the primary opens a replication listener and runs one `Session` per
inbound conn; a follower/backup runs one outbound `Follower` client against its primary.
Both are driven kernel-shaped — `pump_io` moves bytes without touching the `Db` (accept,
drain readable messages into per-conn queues, note reconnect timers), then `step` does one
unit of work against the `Db` per call (handle one message, push one `Frames`/snapshot
batch to one session round-robin, heartbeat, or reconnect). A session opens on its first
message: a `Hello` (proto version, the follower's own listen address, and its position —
last durable seq + that frame's CRC via `frame_crc`) runs the decision ladder pinned by
tests: version mismatch → `Refuse` naming both versions; a non-primary role → `Refuse`
naming the real primary (no cascading); follower at-or-ahead of the primary with unknown
bytes, or a CRC mismatch on a retained frame → `Refuse` naming both seqs, both CRCs, and
the re-seed fix (diverged timelines are refused, never merged); a POSITIONED follower
below the GC'd base → `Refuse` (diverged behind — the primary GC'd the frame at that seq
and cannot prove the follower's records are a prefix of its timeline, so a re-seed could
silently wipe a follower the primary forked below; the refusal names both remedies,
re-seed a true laggard or promote the follower — Design B,
`.superpowers/sdd/diverged-behind-investigation.md`); an empty (None-position) follower
against a compacted log → snapshot-seed; otherwise `Frames` resume from position + 1.
Shipping reads exclusively through `Db::read_log` — CRC
re-verified at read-back, capped at `last_applied`, so a frame corrupted on the primary's
disk is refused by name rather than shipped, and the unsynced tail never travels. The
follower appends via `Db::apply_shipped` (append → sync → apply, recovery's own
discipline) and sends `Ack { durable_seq }` only after that sync, never on receipt; a
shipped seq gap means a different timeline — drop the conn and re-`Hello`. An empty
`Frames` batch — never sent by the real shipper (it only ships when it has something to
ship), but not enforced by the wire — is a no-op on the follower side: `apply_shipped`
doesn't advance `last_applied` on an empty batch, so there is nothing to ack. The retention
pin is wired at `Repl::open`: `Db::add_retention_pin` registers a pin that reads a shared
floor recomputed on every ack and session change — the min over live sessions of acked + 1
(a not-yet-acked session pins at its cursor; a seeding session at its snapshot's
`applied_seq + 1`) — so a live lagging session holds GC exactly as long as it lives and
releases it when reaped. A
session still `AwaitHello` (accepted but silent) pins at 0 like any not-yet-acked session;
`HELLO_DEADLINE_NANOS` (10× the heartbeat interval, checked and reaped the same shape as
the heartbeat deadline) fires for one that never sends its first message — `Hello` or a
control verb — so a peer that opens the replication port and never speaks cannot hold GC
at the base forever.
Seeding streams `SnapshotStart`/`SnapshotRows`/`SnapshotEnd` out of ONE MVCC snapshot
(batches near `SNAPSHOT_BATCH_BYTES`, one message per step); a table's own scopedness
picks the row message — an unscoped table batches into `SnapshotRows { table, rows:
Vec<(u64, Vec<u8>)> }` (tag 5, unchanged since before tenant scoping), a scoped table into
`ScopedSnapshotRows { table, rows: Vec<(u64, u64, Vec<u8>)> }` (tag 14, a new message
rather than a widened tag 5, so tag 5's bytes for an unscoped table stay exactly what an
older binary already expects); the receiving side checks the message against its own
schema's word for that table and refuses a mismatch by name rather than importing a row
under the wrong scope. `PROTO_VERSION` is 4 — a message body's shape changing is what
bumps it: tenant scoping's tag 14 made it 2, payload shapes' `0x08`/`0x09` record tags
made it 3, job tenancy's `0x0a` made it 4, and a `Hello` declaring an older version is
refused by name. The follower
buffers the stream and, at `SnapshotEnd`, latches `db_needs_reseed` — `Repl::step` cannot
replace a `Db` it only borrows, so the CALLER (the kernel arm, T8) runs
`Repl::run_seed(db, clock)`, which consumes the `Db` through `into_seeder`, replays the
buffer, `finish`es at the horizon + 1 boundary, re-gates writes, and returns the new `Db`;
a conn death mid-stream never latches, and the next `Hello` restarts the seed from scratch
(idempotent — `Seeder::new` discards). Stated plainly: v1 seeding holds the full logical
snapshot in memory on BOTH sides — the primary queues the whole stream when seeding
begins, the follower buffers it until `SnapshotEnd`. `SnapshotStart` also carries the seed
frontier's CRC — the primary's `frame_crc` of the record at `applied_seq`, which the
follower persists (`seed_frontier_seq`/`seed_frontier_crc` meta): a freshly-seeded
follower holds none of that record's WAL bytes (its own log starts empty at
`applied_seq + 1`), so without it `Hello` could only send `None`, and a primary that had
since lost its own tail above the horizon would answer that empty position with a re-seed
— silently wiping a converged survivor from the now-lossy primary. With the CRC the
follower Hellos `(applied_seq, crc)`, so the ladder runs its diverged-ahead / resume
checks instead; the fallback holds across a restart before any frame ships, and fires only
while `last_applied` still equals the seed's seq — the CRC vouches for that one record
alone, and a node whose log vanished after applying past the seed honestly sends `None`
and re-seeds (finding-1 fix). A seeded-then-promoted primary that has to seed others
before its first commit chains the same stored frontier into its own `SnapshotStart`.
Right at M1 scale; incremental streaming is the tracked fix before large-store seeding.
The durable role file (§2's role semantics, T5) carries its own CRC line: a torn role
write is a named integrity error, never a parse of whatever bytes survived — a torn
"follower:p:1" that still parses as a role naming a ghost primary is exactly what the T6
harness caught (seed 0x51dacce702b667c8). It is read the way recovery reads the log (§2,
"readable is not durable"): `load_role` barriers the file's name and syncs its bytes before
it trusts either, and every witness-slot read does the same (2026-10-01, D1 review I3).
Before that, a promote whose role fsync failed was still in the page cache after the
restart its own error asked for. The node booted as `Primary` from it and served writes,
and a power loss then reverted the file to `Follower` under a diverged log. A witness store
that failed the same way let a primary boot checking a set the next power loss shrank. A
sync that fails at boot fails the boot by name. The pins are in `repl/role.rs` and
`repl/witnesses.rs`: `a_role_a_restarted_node_boots_as_survives_the_next_power_loss`,
`a_witness_set_a_restarted_node_boots_with_survives_the_next_power_loss`, and for each the
name (`…_name_a_restarted_node_trusts_…`) and failed-sync (`…_whose_sync_fails_at_boot_is_not_trusted`)
twins. So a failed role or witness write has an unknown outcome, and its error says so: the
change may already have happened, because the restart it asks for boots as whatever the
file holds (2026-10-01; `a_failed_role_write_says_the_flip_may_already_have_happened` and
its witness twin).

**The control plane (v1, implemented).** Admin verbs travel over the replication port as
control messages in the same protocol (M1 replication T7) — the sim drives them through
exactly the code path the CLI uses (same-shape rule). A connection is a *control* conn
when its first message is a verb (`CtrlPromote`/`CtrlSwitchover{to}`) rather than a
`Hello`; a control conn ships nothing, pins no retention, and `Repl` (not the ship
round-robin) executes the verb and answers `CtrlOk`/`CtrlErr` on it — because every
transition flips the durable role file, re-gates the `Db`, and re-wires the pin, all
`Repl`-level. **Switchover** (`Primary → HandingOff(to) → Follower(to)`) is zero-loss and
crash-safe on the three-state role file: `store_role(HandingOff{to})` write-stops the
primary (`db.forbid_writes(to)` — reads and effect-free transactions still run, effectful
ones get `TxError::NotPrimary`), then it *drains* — the target session's `Ack`s are read
each step and, when its acked seq reaches `db.last_applied()`, the primary sends
`HandOff{at_seq}` and enters HandedOff, **the point of no return**. Draining is abortable:
the `SWITCHOVER_DEADLINE_NANOS` deadline while still Draining returns durably to `Primary`,
resumes writes, and the `CtrlErr` names the follower's remaining lag. HandedOff never
aborts: on `HandOffDone` the primary flips durably to `Follower(to)`, drops its ship
sessions, and starts a follower client toward the new primary (drained to byte equality,
no divergence possible); on the deadline it stays write-stopped and the `CtrlErr` says the
hand-off was sent — verify `to` and restart to resolve. The follower side of `HandOff`
verifies `last_applied == at_seq` (else a named `Refuse` protocol error), requires a
configured listen address (an unpromotable follower is never a switchover target — its
`Hello` carries no listen, so it never matches), flips durably to `Primary`, allows
writes, replies `HandOffDone` on the old conn, and begins accepting Hellos. A `HandOff`
buffered in the same drain as the primary's crash-EOF is not stranded: the follower client
drains its inbox before any reconnect, and the primary drains the target session's inbox
before the dead-session reap. **Promote** (`charpente promote`) is the bounded-loss
failover: a follower/backup drops its upstream client, flips durably to `Primary`, allows
writes, and the `CtrlOk` names `last_applied` — records the dead primary acked but had not
shipped are the DECISIONS §6 window, lost honestly and named. A node that restarts in a
durable `HandingOff(to)` stays write-stopped and resolves against `to`: a probe `Hello`
answered as a primary demotes it to `Follower(to)` (a diverged refusal is the operator's
re-seed signal, surfaced as the step error); `to` unreachable or not-yet-primary keeps it
`HandingOff` on a reconnect cadence — a node in durable `HandingOff` never silently
resumes `Primary`, so no *switchover* interleaving yields two writable primaries. **The
promote path did not carry that property** (the M1 sub-project 6 / T12 gap): a node
promoted *away from* — killed while `Primary`, its follower promoted underneath it — still
has `Primary` in its own durable role file, and the durable role always wins, so restarting
it with its original config used to resume it as a second writable primary with no notice
at all (neither node dials the other, so neither could observe the conflict). Two live,
mutually invisible, diverged timelines; the mandated fresh-data-dir re-seed then destroys
every write the resurrected node acked.

**The boot gate** (M2 sub-project 0b, spec `docs/superpowers/specs/2026-07-31-dual-primary-
design.md`) closes that for the case the node can see. A **witness** is a node this primary
agreed to ship to; the listen addresses are recorded durably (two CRC'd generation-stamped
slots, so a torn write can never parse as a shorter — and therefore weaker — set) as each
shipping session is established, and cleared on every durable flip *toward* `Primary`,
before the flip itself, so an interrupted promote is merely incomplete rather than a
primary carrying a stale set nothing will re-clear. At open, a durable `Primary` with a
non-empty set does not `allow_writes`: it enters the third write-gate state
(`TxError::Verifying` → 503, §2a of the spec) and probes every witness with one
classify-and-disconnect `Hello` — never `Follower::new_resolving`, which would fall through
to *adopt* the winner's timeline, the move the 2026-07-16 diverged-BEHIND ruling rejected.
Every witness answering "not a primary" opens the gate; one answering as a primary refuses
this node by name, naming both remedies and demoting nothing; unreachable or inconclusive
witnesses keep the gate shut, serve reads throughout, and produce one operator notice after
`WITNESS_DEADLINE_NANOS` (Noah's ruled option (c) — the availability cost is deliberate,
because the alternative is silent acked-write loss). `ReplConfig::strict_witnesses` (default
`false`) overrides that last arm alone: past the same deadline the node stops instead of
serving reads it cannot write to. It is not "refuse to start" — unreachability is only
established by probing, so a strict node starts, serves reads for the deadline window, and
only then stops; and the stop reaches a supervisor on every node shape — under
`serve::backup` the error returns from the call, and under `serve::serve_replicated` the
kernel's stop ends the process through `Server::wait` (below) with status 1, so a
supervisor restarts it and keeps restarting it until a witness answers. **The bound,
stated so it is not read wider: a probe answers "is THIS witness a primary", never "does
another primary exist".** A node promoted after a switchover chain, never in this node's
witness set, is invisible to the check — the witness set is the bound on the whole
defence, and the M2 backlog's Design C (a durable timeline epoch carried on `Hello`) would
sharpen the refusal rather than replace the probe.

**What a `Verifying` node may do as a peer** (M2 interlude, spec `docs/superpowers/specs/
2026-08-06-verifying-peer-policy-design.md`) narrows the one thing the boot gate above left
untouched: `begin_seeding` is read-only, so the write gate never saw it, and a `Verifying`
node remained a full replication peer — accepting Hellos, shipping, and **seeding brand-new
followers**. Manufacturing a fresh durable replica of a timeline this node has not confirmed
it owns is acting as the primary in the most consequential way short of acking a write, and
unlike a write that replica **outlives this node's own fail-stop** once a witness finally
answers "primary". The policy: a `Verifying` node may not render a seed to a peer that holds
nothing and is not one of its recorded witnesses; everything else it does as a peer — serving
reads, refusing writes, accepting Hellos, shipping catch-up — is unchanged, with one named
carve-out already in force before this interlude: `switchover` is refused while verifying
(`mod.rs:966-987`), since `abort_switchover`'s `allow_writes` would open the gate by a path
`resolve_witness_gate` never sanctioned. The line drawn: *catch-up ships a timeline the peer
already holds a prefix of; a seed creates a fresh copy of an unverified one.*
The keying is `db.verifying() && position.is_none() && !witnesses.contains(listen)`, with an
absent `listen` counting as not a witness (conservative — an unidentifiable peer is treated
as a newcomer), checked inside `decide_hello`'s shared `None`-position arm rather than on the
`Decision::Snapshot` variant. Both conjuncts are load-bearing and neither alone is correct:
`position.is_none()` does not mean "brand-new peer" — `hello_position` returns `None` for any
node with `last_applied() == None`, including a **recorded witness** returning with an empty
data dir (a fresh-data-dir re-seed, not a discard-and-rebuild — that replays the WAL and
leaves `last_applied` intact, so it Hellos positioned instead; GUIDE §10's own re-seed remedy,
and precisely what resolves this node's gate), so that peer must still be seeded; and
membership alone is not enough either, since a positioned peer absent from the set is a
genuine follower on this timeline and must still catch up. Keying on the `Decision::Snapshot`
variant instead would be silently inert on exactly a young primary that has not GC'd anything
yet — its `None`-position arm answers `Frames { from: 0 }` (a full replay that is
functionally a seed) rather than `Decision::Snapshot`.
The refusal is retryable, not node-fatal, at both places it can land. A newcomer's own
`follower.rs` client recognizes the marker at both its refuse-handling sites (the resolving
arm and the ordinary ship arm, separately fatal before this) and backs off to retry rather
than treating it as fatal. Separately, the witness probe's `classify` must resolve the
refusal correctly should it ever land there — it carries neither `NOT_A_PRIMARY_MARKER` nor
`DIVERGED_LABEL`, so `classify`'s existing default arm files it `Inconclusive` for free, with
either misclassification wrong: `NotPrimary` would let a probing peer open its own gate on no
evidence, and `IsPrimary` would refuse a healthy node over a phantom split-brain. In practice
a probe rarely reaches this: it Hellos with its own real position rather than a synthetic
`None`, so on any node that has ever applied a record it lands in the positioned ladder
instead, never this arm. Per Noah's ruling (2026-08-06), the refusal **never lapses** while
the gate is shut — admitting a newcomer on a timer would make the guarantee time-dependent
instead of evidence-dependent, and the availability cost is near-zero regardless: a gated
node already refuses writes, so the only use for a newly-seeded follower would be promotion,
which is exactly the harm the gate exists to prevent.
Recording witnesses while verifying is unchanged and safety-monotone: `probes` is built once
at `Repl::open` from the witness set on disk at that moment, so a witness recorded mid-
verification cannot affect the gate currently open — it can only make the *next* boot's gate
stricter, never looser. With the seed refusal in place a newcomer cannot be recorded at all
(`listen` is assigned only on the two shipping arms, never on `Refuse`); the only peer that
can newly join the set while verifying is a positioned follower this node had not yet
recorded — a genuine follower relationship worth remembering.

**Surfaces (v1, implemented).** The kernel wires the data + control plane in (M1
replication T8): `Kernel::with_replication(app, clock, repl)` runs the `Repl` engine as the
fourth ready source (§1), and `run`/`step` split so the park is the driver's. Three edges
in `charpente::serve` expose it: `serve_replicated(addr, app, repl_disk, repl_cfg)` mirrors
`serve` but the kernel thread opens `Repl` (durable role on `repl_disk`, wire on `ProdNet`)
and runs `with_replication` — a follower answers writes with 421 and reads from its
replicated copy, a primary ships; a startup role/config mismatch prints to stderr, config
never overriding the durable role. `backup(app, repl_disk, repl_cfg)` runs the same kernel
on the current thread against an `EmptySource` (no HTTP edge, no requests) — a node that
only follows and keeps a promotable copy. `archive(app, store, cfg)` (M2) runs against the
same `EmptySource` with `Kernel::with_archive(app, clock, archive, store)` instead — FOUR
arguments, because `Archive::step` takes the store on every call and the `Archive` never
holds it, so the `Kernel` keeps the pair. It defaults nothing about `cfg.writer`: the
manifest's ownership comparison is identity alone, so a writer derived from anything a
redeploy moves would make an ordinary redeployment a permanent refusal on a bucket the node
genuinely owns. The reseed swap that `Repl::run_seed` needs lands
in `App`: the `Db` sits behind an `Option` so the `Ready::Repl` arm can `take` it, hand it
to `run_seed`, and `put` the rebuilt one back, adjacent so no other code observes the empty
slot.

**A stopped kernel ends the process.** `serve`/`serve_replicated` run the kernel on a
joined thread; `Server::wait` returns its exit code, and `launch` exits with it. The stop
line — the first line of the kernel's final stderr output (a multi-type foreign-shape jobs
refusal continues one job type per line after it) — names the stop: `refused to
start`/`stopped on a refusal` for a `Refusal` (table-gate barrier, witness conflict,
strict-witness deadline, foreign-shape jobs, an unmarked replication refusal, a
listen-less switchover target), `failed to start`/`stopped on a platform IO
failure — restart to recover` otherwise. Restarting the same binary does not by itself
clear a refusal; each names what must change first — a witness answering, a fresh data
dir, a registered migration step — while a platform IO failure really can clear on retry.
A fifth line is neither: v0 has no admin-triggered shutdown, so `Kernel::run` returning
`Ok(())` (`Wake::Shutdown`) under `serve`/`serve_replicated` only means the HTTP edge
thread, the request channel's one sender, ended first (most likely a panic). It prints
`charpente kernel stopped: the request channel closed, which only happens when the HTTP
edge thread ended before the kernel did (most likely a panic) — check the edge thread's own
panic output above; this is not a clean shutdown, v0 has none`, with exit code 1: a live
kernel with no edge left to feed it answers nothing, so this is named and non-zero rather
than a clean-exit `0` a `Restart=on-failure` supervisor would take as "nothing to do here."
The HTTP bind runs before the kernel thread is spawned, so a bind failure is `serve`'s own
`Err`, with no kernel left running behind it; `launch` names it (`charpente failed to start
— could not bind <addr>: ...`, GUIDE §13).

## 6. Queue, cron, realtime — all faces of the log

- **Queue.** `tx.enqueue(job)`/`enqueue_keyed(key, job)` writes the job into the committing
  log record; apply materializes a `JobRow` (§4). Post-commit, the kernel dispatcher runs
  due jobs inline on the kernel thread: a `global` job's handler is `fn run(self, tx: &mut
  Tx) -> Result<(), JobError>` — itself a transaction — and a `scoped_by = Org` job's takes
  `&mut ScopedTx<Org>` instead. The enqueue records the tenant (record tag `0x0a`,
  `JobRow.scope`), and the dispatcher reopens exactly that tenant with its own
  `TenantAdmission` through a `JobScope` only it constructs, so a scoped job runs where it
  was enqueued, retries included. The row is the platform's to write: its Done mark and
  failure record go through `Tx::settle_job` with that same admission, and app code
  cannot write a `JobRow` at all (§3's Jobs bullet). `Ok` commits its effects and the job's
  Done-mark atomically in one record: exactly-once state effects, structurally. `Err`
  aborts that transaction and a second commits the failure record — attempts, doubling
  backoff, Dead at the policy's `max`, last_error carried (typed `RetryPolicy` on the
  `#[job]` item; handler panics are absorbed into the same path). At-least-once execution
  plus idempotency-key observability is the framing M1's external-effect handlers need; v0
  handlers are pure state and don't need it yet. Every enqueue records the payload type's
  structural fingerprint (§3). A scoped job's fingerprint also covers its `scoped_by` path
  as spelled, so moving a job between `global` and `scoped_by`, or respelling its path, is
  refused like a change of fields; a path that keeps its spelling but comes to name another
  type is not (§3's Jobs bullet). The kernel refuses to dispatch while a Pending job of a
  mounted type carries another shape, naming the job, its ids and both shapes. It checks at
  startup and wherever its write gate opens, and `promote` checks before its durable flip.
  A switchover is never refused at the hand-off, which is the primary's point of no return
  (§5): its target stops at the write-gate flip instead, where the refusal's fix — run the
  binary that enqueued them until they are Done or Dead — still works. The binary never
  writes such a row itself: the job-shape registry is filled at `App::mount_jobs`, and an
  enqueue under another shape of a mounted job name (two `#[job]` types sharing one)
  refuses its whole transaction by name, before any record exists. Dispatch asserts the
  invariant. A charpente release never changes the platform's own `SessionSweep` job's
  fields without renaming it: of the refusal's two fixes, an app author could apply only
  the drain to it, never the restore of its old fields.
- **Cron.** Schedules are rows; a due tick is one transaction that advances `next_due` and
  enqueues through the normal path — on a bare `Tx`, so a schedule takes `global` jobs
  only; a sweep that acts per tenant is a `global, cross_tenant` job that fans out with
  `enqueue_in` — a recurring enqueuer, inheriting retry and
  observability for free. Catch-up fires once per wake, never backfills. In simulation,
  virtual time makes "run this monthly job 120 times" a subsecond test.
- **Realtime.** `tx.emit(event)`/`tx.emit_keyed(key, event)` writes a typed event into the
  committing log record alongside the transaction's other effects — `key` is an opaque
  scoping filter (an org id, typically), carried but never interpreted inside the
  deterministic boundary. Inside a tenant scope the key is not the app's to remember:
  `ScopedTx::emit` supplies it from the scope itself (`ScopedTx::event_key`, the scope's
  decimal id), so tenancy has one mechanism rather than two that can disagree — an SSE
  handler keys its `Subscription` with the same `event_key`, and a subscription keyed to
  one tenant never receives another's event. Apply materializes each into the built-in
  `charpente.events` table (`EventRow { id, seq, name, key, payload }`); `id` comes from a
  `next_event_id` meta-counter bumped in call order — the `next_job_id` pattern exactly, so
  ids are strictly monotonic, order-isomorphic to log position, and byte-identical on
  followers.
  Retention is count-based: `DbConfig::event_retention` (default 65,536) bounds the table,
  garbage-collected at the existing checkpoint cadence; `Db::event_floor()` reports the
  smallest id still retained. Both read the oldest row's key and never decode a row, so a
  corrupt row at the floor fails only a tail whose scan reaches it, and GC collects it
  like any other.
- **The cursor is the event id, wire-encoded with its committing record's seq: `<id>.<seq>`.**
  Resume from cursor `(c, s)` delivers every matching event with `id > c` — exact by
  construction, since ids are minted at apply in log order. The seq half exists for one
  reason: failover honesty. Two diverged timelines — an old primary's unshipped tail and a
  promoted follower's new writes — can mint the same event id for two different events; a
  bare id cannot detect that. Resume validation reads the retained row at `id == c` and
  requires its `seq == s`; a mismatch is a named divergence, never a silent
  misattribution — the same shape as replication's diverged-`Hello` refusal (§5).
- **The subscription surface.** `#[route(SSE "/path")]` is a route flavor whose handler is
  an ordinary transaction (session auth via `Ctx` like any route) returning a typed
  `Subscription`: `Subscription::of::<Ev>().and::<Ev2>()` names event types compile-checked
  (never as strings in app code), `.key(...)` is an optional exact-match filter, and
  `.cursor(cursor)` takes the platform-extracted resume point (the handler passes it
  through, or clamps it — access control is the handler's own session/row logic, not the
  platform's). The platform extracts the client's cursor before the handler runs:
  `Last-Event-ID` header wins (browser-native SSE resume), else a `cursor` query
  parameter, else `None` ("from now" — only future events); an unparseable cursor is a 400
  and the handler never runs. The route may also DECLARE the event types its stream
  carries — `#[route(SSE "/path", emits(InvoicePaid, InvoiceVoided))]` — which is what
  lets a generated client type `subscribe()` as a discriminated union of exactly those
  payloads instead of "anything". The declaration names Rust types, and the contract
  records BOTH names each type has — `<Ev as Event>::NAME` (snake_case, what an SSE
  frame's `event:` field carries) and `<Ev as Wire>::WIRE_NAME` (the payload type's key
  into `Contract.types`, and the member a generated union writes) — so each name has one
  source rather than a copy of `#[event]`'s snake_case rule. Both are carried instead of
  one being derived from the other because that derivation does not exist: `#[event]`'s
  acronym rule maps `HTTPServer` to `http_server`, and nothing inverts it. The
  declaration is optional (an undeclared stream
  route simply carries an empty `emits`) and it is not a filter: nothing enforces it at
  delivery, because delivery is still the encoder each `Subscription::of::<Ev>()`
  captured. What holds the two together is an ORACLE, not a rule — a declaration nothing
  verifies is decorative — `App::subscription_event_names` runs a stream route's
  subscribe glue and reports the names its `Subscription` actually holds, and a test
  compares the two sets (TS-codegen spec §5).
- **Kernel delivery is one path, no modes.** A validated subscribe registers a `Tail`
  (`{ subscription, sink, next_id }`) on the fifth ready source (§1) — eligible the instant
  a commit advances `next_event_id` past it, never a timer. One unit of work reads up to
  `TAIL_BATCH_EVENTS` rows from `next_id`, filters by name and key, JSON-encodes matches
  through the encoder each `Subscription::of::<Ev>()` captured statically at the subscribe
  site (there is no runtime event registry — "unregistered event at delivery" is
  inexpressible, not an error case), and pushes frames to a driver-supplied sink: a bounded
  channel drained by the hyper SSE writer in production, an in-memory buffer in sim. There
  is no separate push-on-commit path and no live/catch-up distinction — a commit simply
  advances `next_event_id`, which makes tails ready; "live" is just "zero behind." A sink
  that reports `Full` or `Closed` costs the tail its connection, deterministically and
  immediately: a slow or dead consumer never costs kernel progress, and the client's cursor
  resume is the recovery mechanism, not a server-side retry queue. Event payloads must be
  plainly JSON — serde_json refuses a map key that does not serialize as a string, a
  number or a bool, and a non-finite float key (and writes a non-finite float value as
  `null`). An event that fails to encode, or whose stored bytes do not decode under its
  recorded shape (corrupt bytes, or a change inside a type one of its fields holds, which
  the fingerprint cannot see — the text names both), ends that subscriber's tail with a
  terminal `charpente.error` frame that carries no `id:`, so the client's cursor does not
  advance past it. No delivery path stops the kernel; only the platform's own store
  failures do.
- **Honesty at the edges.** A cursor below the retention floor, or one naming an event id
  this node never minted (or minted with a different seq — the cross-timeline case), is
  refused at subscribe with a 409 naming the floor or the divergence detail and the fix:
  re-sync current state through reads, then reconnect without a cursor. A tail that falls
  behind retention mid-stream gets the same fact set as a terminal SSE frame —
  `event: charpente.gap` — then its sink closes. A stored event whose recorded shape (§3)
  is not the subscribed type's is never decoded: the tail gets a terminal
  `event: charpente.reshaped` frame carrying that row's own `id.seq`, and a subscribe
  whose cursor sits on such a row — for a name the subscription carries and a key it
  passes — is refused with a 409 naming the event, both shapes and the same re-sync fix.
  Sink overflow itself sends nothing (there is no one left to tell); the client that
  reconnects with `Last-Event-ID` resumes exactly, which is the point. **The seq half
  does not separate every cross-timeline pair, and the sentence above should not be read
  as claiming it does.** A local recovery cut that returns the log to the same position
  lets the replacement record take the SAME seq, so two different events can occupy one
  `(id, seq)` and the divergence refusal cannot fire (seed 0x17004e6a572d737d,
  docs/LEDGER.md 2026-09-07 — a misdirected write moved a WAL segment's first frame off
  offset 0, recovery cut there, and event id 0 was re-minted at seq 1 with new content).
  This is the gap DECISIONS §6's Design C (a durable timeline epoch) closes; the M2
  follow-up backlog carries it, now with a single-node instance rather than only the
  failover one it was written for. Follower tails read the same materialized table whose
  ROWS replication keeps byte-identical (§5 — the file they live in is not, property 1),
  so a subscribe on a follower works with zero app-code changes; a failover honors the
  DECISIONS §6 bounded-loss window for events exactly as it does for state, and a cursor
  that crossed timelines gets the divergence refusal, never wrong events.

## 7. The agent interface

Charpente treats the coding agent as the primary developer persona:

- **Introspection: `charpente describe` (v0, implemented).** `App::describe()` is a pure
  walk of a mounted app's own registries — no disk writes beyond an ephemeral run's own
  temp store, no socket, no kernel loop. It returns a `Contract`: tables (name, version,
  structural fingerprint, indexes, migration chain, **whether it's `scoped`** —
  `#[table(scoped_by = …)]`, carried straight from the registry's own `TableOps::scoped`,
  the same value `Db::table_scoped` already exposes to the archive/replication callers),
  routes (method, pattern, name,
  whether it declared `auth` (`false` is a declared `public`, never an omission), **the scope a `public(Org)` route admits every caller to (`public_scope`, a type name or `null`)**,
  **whether it declared `cross_tenant`** — which is what turns
  "audit the cross-tenant reads" into a mechanical inventory — whether it's an SSE stream,
  **plus, since the TS-client-codegen
  work: `params` — each path param's name and wire schema (`ParamInfo { name,
  schema: TypeSchema }`); `request` — the JSON body's schema, `None` for a bodyless route;
  `response` and `error` — the success and `ApiError` schemas every handler's `Result<T,
  ApiError>` carries; and `emits` — the events a stream route declares via
  `#[route(SSE …, emits(Ev, …))]`, each an `EmittedEvent { event, payload }` holding the
  SSE frame's name and the payload type's name, empty on every non-stream route and on a
  stream route that has not adopted the declaration**), **`events` — every `#[event]` type
  registered with `mount_events(events![…])` as `EventInfo { name, payload, keyed }`,
  sorted by event name (`keyed` is always `false`: `emit` vs `emit_keyed` is a call-site
  choice, so nothing static can know it)**, jobs (name, retry policy, **`scoped_by`, the scope type a job acts in or `null`**, `cross_tenant`), cron
  (name, interval), and config, each serialized in registry order (routes/jobs/cron) or
  sorted-name order (tables — `Db::describe_tables`'s `BTreeMap` walk; events — by name);
  field order is
  part of the pinned shape. `Contract` also carries **`types: Vec<TypeDef>`** — the
  transitive closure of every NAMED wire type a route's params/request/response/error
  or a registered event's payload references, plus every event type a stream route
  declares with `emits(...)` — that last one registered by the ROUTE, so a declared
  payload is in the closure even when the event is never mounted. It is a
  `TypeRegistry` walk, `BTreeMap`-sorted by wire name so generation stays
  deterministic (spec §4); an anonymous type (a primitive, `Vec<T>`, `Option<T>`)
  contributes no entry of its own, only whatever named type it wraps. `ApiError` is
  always one such entry, and its schema describes the fixed `{"error": "<message>"}`
  envelope `Response::error` actually emits on the wire — not `ApiError`'s own Rust
  variant shape, which no client ever receives (a hand-written `Wire` impl, not the
  derive `#[table]`/`#[route]` types otherwise get). A `builtin: bool` on each table/job/cron
  entry marks the platform's own surface (`Session`, `ApiKey`, the internal
  job/event/schema-version tables, the session-sweep job and its cron entry) apart from
  the app's, keyed on each row's own `TABLE`/`NAME` const rather than a typed-in string
  list. `to_json`/`to_human` render it as pretty JSON or fixed-width text (`to_human`'s
  route line renders `params`/`request`/`response`/`error` too, via `Debug`, and `emits`
  as `event->payload` pairs, and it has an `events:` section of its own).
  Two entry points ship it:
  `charpente::launch(addr, data_dir, build)` is the standard app `fn main` — argv
  dispatch owned by the platform instead of hand-rolled per app, so `describe` is free
  the moment an app adopts it (no verb still serves, as a hand-rolled `main` would;
  `describe [--format json|human]` renders the contract to stdout and exits); `charpente
  describe [--dir PATH] [--format json|human] [--bin NAME]` is the CLI verb, which builds
  any app crate from the outside (`cargo build`, picking `--bin` or the crate's sole/
  package-named binary) and execs it through the same `describe` argv path — no code
  change required of the app, and Cargo's own build errors (missing `Cargo.toml`, a
  failing build) are forwarded verbatim rather than wrapped. Both paths build the app
  against a fresh, throwaway temp directory that is created and removed around the run —
  never the app's configured `data_dir` — so a describe is always safe to run next to a
  live server's real data. Event *types* ARE enumerated, but only through the
  describe-time registry `events![…]`/`mount_events` fills — never by walking
  emit sites, which is impossible for the same reason it always was: `tx.emit` and
  `Subscription::of::<Ev>()` run inside handler bodies, not anywhere an app-wide walk can
  observe (ruling 6, sub-project 6 spec). The BINDING between a stream route and the
  events it carries is therefore DECLARED rather than discovered: `emits(Ev, …)` on the
  route fills `RouteInfo.emits: Vec<EmittedEvent>`, each element pairing the
  `Event::NAME` an SSE frame carries with the `Wire::WIRE_NAME` of its payload. The pair
  exists because the two consumers need different names and neither is computable from
  the other — `#[event]`'s acronym rule maps `HTTPServer` to `http_server`, so
  snake_case -> PascalCase is not a function — and both are read off the type at
  expansion time, where both are in hand. The drift between that declaration and the
  handler's own `Subscription` is caught by an oracle
  (`App::subscription_event_names`, §6) comparing the `event` half, which is the only
  half a `Subscription` holds; the `payload` half is pinned by the contract goldens. It
  is an oracle, not a rule the platform could enforce at delivery. An undeclared stream
  route still reports `stream: true` with an empty `emits` — reading its handler is how
  you find out which of the app's events it delivers.
- **The generated TypeScript client (v0, implemented; `crates/charpente-tsgen`).**
  `charpente_tsgen::emit_client` is a pure `Contract -> client.ts` function: branded id
  types and a declaration per named `Wire` schema in `types` (an `interface` for a
  struct, a `type` union for an enum), a discriminated `Result<T, E>` per route keyed
  off `response`/`error`, and per-event dispatch — a `Map` from an SSE frame's `event:`
  name to that payload's revive function, or `null` — for a stream route's `emits(...)`.
  `charpente gen-client` writes the file; `charpente check-client` is the drift oracle
  CI runs, regenerating and comparing against the committed file rather than trusting
  that a Rust change was remembered to be followed by a re-run (docs/GUIDE.md §14).
  Transport is SSE only; no WebSocket client ships.
- **`charpente new` (v0, implemented; `crates/charpente-cli/src/new.rs`).** The scaffold
  a stranger starts from: six files — the GUIDE §1 manifest byte for byte, hard rule 1's
  standard-library fence as the app's own `clippy.toml`, one table/route/job/event, a
  one-line `launch` main, and two sim properties in the give-up idiom, the second of
  which is DEMO.md's outbox oracle. Its templates are compiled, linted under their own
  fence and run under the sim on every push (`tests/new.rs`), and refusals name the rule
  and the fix (docs/GUIDE.md §1).
- **Errors are prompts.** Compile-time and sim-failure output is structured for machine
  consumption: the exact contract violated, the file/line, the seed and fault schedule for
  sim failures, and the minimal repro command. Error quality is a feature with a test suite.
- **`charpente test --sim` (v0, implemented).** The agent's inner loop: seeded, parallel,
  deterministic. App crates write property workloads in `tests/sim.rs` (`harness = false`)
  registered via `charpente_sim::sim_tests! { name require ["fault:crash", …], … }` —
  workloads share the internal suite's signature, `fn(&mut Sim) -> Result<(), String>`;
  `Err` and panics both fail a universe. `charpente test --sim [--universes N] [--seed
  0x…] [--master-seed 0x…] [--filter S] [--jobs N] [--format human|json] [--dir PATH]`
  builds the app's `sim` target via `cargo test --no-run` and execs it directly — build
  errors surface verbatim, the compiler is already the oracle. Batch model: one master
  seed (OS entropy, printed at startup, or pinned via `--master-seed` for byte-identical
  replay) derives per-universe seeds in registry order; `--jobs`-many workers race over an
  index counter, so scheduling changes nothing about the seed sequence or report order.
  `--seed 0x…` replays exactly one universe and skips `require` coverage checks (one
  universe can't be expected to hit everything a batch would). A failure message *is* a
  reproduction (`--seed 0x9f3a...`), so the agent's fix-verify cycle needs no flaky-test
  triage. `--format json` is the machine-readable surface: per-failure `{test, seed,
  master_seed, config, message, repro}`, `coverage`, `missing_coverage` (`{test, key}`
  pairs for `require`d keys never hit), and a trailing `summary`. Ships with the public
  checker library (`charpente_sim::checks`): `is_fault_fallout` (the injected/poisoned/
  redb-latch/durable-horizon fallout classifier, productized out of four hand-rolled
  internal copies) and its typed form `classify` → `FaultReason`, for a workload that
  accepts some families and fails on others (sub-project 6 T7 — first-match arm order is
  load-bearing and pinned); `replays_identically` (determinism oracle);
  `acked_implies_durable` (the request-level ack-durability property, generalized for app
  workloads); and the two fire-time-marked axis companions `durable_corruption_fired` and
  `dir_entry_loss_fired`, which excuse a shortfall only in universes where that fault
  actually fired. Exit codes: 0 green, 1 sim failures or unmet coverage, 2
  build/usage/infrastructure error.
- **The edge harness (v0, implemented; `crates/edge-harness`).** The sim owns everything
  below the IO traits; the edge harness owns everything above them that only a real
  process can show. It drives **real built binaries** (`examples/invoicing/src/bin/node.rs`
  and the `charpente` CLI) over **real TCP** with process- and connection-granular faults
  — `kill -9`, SIGSTOP/SIGCONT, an in-process proxy that severs live connections, refuses
  new ones, or delays forwarded bytes — and proves three wire-level guarantees end to end
  (M1 sub-project 6, T11–T12): acked-implies-durable across a kill and a restart against
  the same data directory; zero loss over *converged* writes across `charpente promote`
  (with the non-converged tail reported separately — that tail is DECISIONS §6's window,
  not a violation); and exact `Last-Event-ID` SSE resume across a mid-stream kill
  (set-equality with the missing set, so an interior skip cannot pass). Each oracle carries
  an anti-vacuity floor and a positive control: the ack-before-commit meta-test drives a
  deliberately-wrong build (`crates/edge-harness/patches/`) and demands the oracle go red
  by name, which is what stops a green run from meaning "the rig did nothing". **The
  honesty contract (spec §4.4) bounds what this can claim.** `kill -9` is *not* power loss
  — process death does not drop the OS page cache — so what these oracles prove is ack
  **ordering** (nothing crosses the wire as acked before the commit path ran) plus
  end-to-end crash recovery through real hyper/TCP; power-loss and torn-write durability
  remain the simulator's claim alone, and the crate's own docs say so rather than
  overclaiming. There is **no seed promise** here: a scenario's reproduction is the script
  file it records (every fault action and observation, in order, dumped on drop and named
  loudly on failure), replayed by hand. And the sim stays the reproduction authority: a
  finding this rig makes that indicts the kernel or core gets reproduced in sim, with a
  pinned seed, *before* it is fixed (hard rule 3, extended past the sim's boundary);
  something genuinely edge-only — a real signal, a real socket close — lands a
  deterministic edge regression instead. Because each scenario builds release binaries and
  drives real processes, every test that touches a process, a socket or the filesystem is
  `#[ignore]` and the matrix runs on demand via `scripts/edge-harness.sh`; the crate's
  zero-syscall tests (parsers, classifiers, and the byte-exact pins for every verdict
  string an operator would read) are not ignored and run in the ordinary sweep.
- **Skill/docs layout.** Documentation ships as both human prose and a machine-readable
  pack designed to be loaded into an agent's context (the framework's conventions are only
  worth having if the agent applying them has them in scope).

## 8. Auth & multi-tenancy [auth M1; multi-tenancy M2]

Sessions and API keys are built-in typed tables inside the commit point; a pre-transaction
resolution ladder turns a presented credential into one `Principal`, and `#[route(…, auth)]`
gates on it — a route that declares neither `auth` nor `public` does not compile, so an
open route is a declaration, never an omission (default-secure, 2026-09-16). Deliberately
boring — the value is that credential state is a transaction like
everything else (durable, replicated, recovered on restart), not a bolted-on side store.
Everything here is pure computation plus the kernel's one `Clock` reading per request, so it
replays byte-identically in sim; the crypto is RustCrypto (`sha2`, `rsa`, `base64ct`), never
hand-rolled, and lives in `charpente`, never inside the deterministic `charpente-core`.

- **Hashed at rest.** `Session { id, token_hash #[index], subject, expires_at }` and
  `ApiKey { id, key_hash #[index], subject, created_at }` register platform-side in
  `App::open` (alongside `CronRow`), so recovery replays logins and minted keys with no app
  code. The stored `*_hash` is lowercase-hex SHA-256 of the raw credential; **the raw
  token/key exists only in the mint response and never reaches disk.** Hashing happens
  before the effects record is built, so the WAL carries the hash too — the flagship
  negative-space sim property (scan every durable file in the universe under the full fault
  matrix, assert no raw credential byte-string appears in any store, WAL segment, or role
  file) is what mechanically forbids a regression; its naive-victim meta-test (a resolver
  that stores the raw token) fails that oracle by name. `create_session(tx, subject)` sets
  `expires_at = now + session_ttl` (`AppConfig`, default 30 days in `Clock` nanos);
  `create_api_key(tx, subject)` returns `"ck_"` + 64 hex (the prefix makes a leaked key
  grep-able for secret scanners); revocation is `tx.delete::<ApiKey>(id)` — an ordinary
  write, durable and replicated like any other.
- **One entropy draw, one mint per request.** Both credential kinds are fed by the single
  existing pre-transaction `fresh_token` draw — zero new draws, so every pinned seed's
  entropy interleaving is structurally untouched (the frozen replay contract holds by
  construction, not re-verification). The consequence, enforced by a shared once-assert: one
  credential mint per request, session OR key, never both — a second mint panics
  (`Ctx::create_session/create_api_key called twice in one request — one credential mint per
  request`). Handlers cannot draw entropy (`Tx` has no `Entropy` by design), so this is the
  honest shape, not a limitation to route around.
- **The resolution ladder (pre-transaction, pure).** One entry point replaces the old
  session-only lookup, producing `Option<Principal { subject, via }>` with
  `enum Via { Session(id), ApiKey(id), Oidc }`: (1) an `Authorization: Bearer <t>` whose
  token contains exactly two `.` is judged as a JWT — OIDC-verified if a verifier is mounted,
  else a named unrecognized-credential 401; never a fall-through, a JWT-shaped credential is
  judged as a JWT. (2) Any other bearer is SHA-256'd and exact-matched against the `ApiKey`
  index, then the `Session` index (expiry-checked against `now`). (3) A `charpente_session`
  cookie takes the same hash path — and *only* the hash path: the two-dot JWT rung is scoped
  to the Authorization header, so a cookie value that happens to contain two dots is never
  OIDC-verified (a ruling, pinned by test). (4) Nothing presented is `principal = None`.
  `#[route(…, auth)]` answers the no-credential 401 when the principal is absent;
  `Ctx::principal()` is the surface, and `Ctx::session()` survives as a compat accessor
  returning the row only for a `Session` principal. The ladder runs unconditionally before
  the `auth` gate, so a garbage credential on a public route is still rejected. Lookups are
  hash-then-index-equality on SHA-256 outputs — an attacker controls no stored preimage and
  learns nothing exploitable from index-lookup timing; the one equality on secret-derived
  data both sides control is the repl secret compare below, which is constant-time.
- **OIDC, static-JWKS only.** `App::with_oidc(OidcConfig { issuer, audience, jwks_json })`
  parses the JWKS once at mount — a bad key, unsupported `kty`/`alg`, or malformed document
  is a named mount-time panic (which key, what is wrong, the fix), never a request-time
  error. `verify` is pure over `(jwt, now)`: split the compact JWT (exactly three segments,
  else malformed), base64url-decode header/payload/signature, require header `alg == "RS256"`
  (v1 is RS256-only; `kid` selects the JWK when present, else all configured keys are tried),
  check RSA-PKCS1-v1.5-SHA256 over the `header.payload` signing input, then `iss`
  string-equals issuer, `aud` (string or array) contains audience, `exp > now - skew`,
  `nbf <= now + skew` (`JWT_CLOCK_SKEW_NANOS` = 60s; JWT `exp`/`nbf` seconds converted to the
  clock's nanos). Success yields an ephemeral `Principal { subject: sub, via: Oidc }` — no
  row, nothing to revoke but a short `exp`. Each failed check is its own named 401 (below).
  Verification is ~100µs of pure computation on the kernel thread; the test suite checks in a
  fixed RSA keypair and mints tokens in-test with the same crate, so the claim matrix replays
  byte-identically — proof the crypto path added no nondeterminism. Dynamic JWKS fetch (any
  outbound IO) waits for the external-IO surface; ES256 joins when someone needs it.
- **Expiry: lazy resolution is correctness, the sweep is hygiene.** A `Session` row with
  `expires_at <= now` is treated as absent at resolution — invisible the instant `now` passes
  it, restart or not (the same restart-stable `Clock` shape cron's catch-up already relies
  on). A built-in daily cron (`charpente.session_sweep`, registered platform-side with no
  opt-in, running through the ordinary cron/job machinery — retry and observability for free)
  deletes expired rows in one transaction per tick so the table doesn't grow unbounded.
  Because lazy resolution is authoritative, the sweep's cadence is not load-bearing; the tick
  threads its `now` into an enqueued `SessionSweep { before }` job (the cron enqueue closure
  was widened to see the scheduler's `now`), which deletes rows with `expires_at <= before`.
- **Replication peer secret.** `ReplConfig.peer_secret: Option<String>` closes the
  anyone-can-connect hole on the replication port (the AwaitHello-pins-GC / empty-`Frames`
  class, already hardened, now access-controlled). When `Some`, every inbound `Hello` and
  single-message control conn (`CtrlPromote`/`CtrlSwitchover`) must present a matching secret,
  checked *first* — before any position/pin/seed logic — and a mismatch is `Msg::Refuse` with
  the named text and close, zero state touched. The compare is constant-time over SHA-256
  digests of both sides (`ct_eq_hex(hash_token(a), hash_token(b))`), never a raw `==`. `None`
  preserves today's open behavior: a node enforces only its own config — a `Some` node refuses
  unproven inbound, a `None` node accepts everything and gracefully ignores a proof it never
  required (not an error). So a half-configured pair works in exactly one direction, which
  fixes the rollout order at **followers first, then the primary** (a ruling — configuring the
  primary first would lock every not-yet-configured follower out). The CLI verbs gain
  `--secret`; a `Refuse` maps to the existing exit-1-with-forwarded-text path.
- **Threat model, stated not implied.** The replication wire is plaintext TCP. `peer_secret`
  *authenticates* peers — it does not encrypt the link, so an eavesdropper on the path reads
  both the frames and the secret. The deployment assumption is a private network; transport
  encryption is an M2 conversation. This is documented in DESIGN and GUIDE, never left to
  inference.
- **The named 401 family (product surface, all pinned by tests against text).** No credential
  on an `auth` route, unrecognized bearer/cookie, expired session, the seven OIDC per-check
  rejections (bad signature, `exp`, `nbf`, wrong issuer, wrong audience, unsupported `alg`,
  malformed), the mount-time OIDC config errors, and the repl refusal each name the specific
  failure and its fix — precision over vagueness, since the leak profile is already acceptable
  (possession of an expired token proves past authorization; JWT claims are attacker-visible
  by construction). The exact strings live in the code and are pinned by the ladder/OIDC/repl
  test matrices; GUIDE §5 quotes the set byte-for-byte in its error-index voice.
- **Multi-tenancy (shipped M2, 2026-09-04).** Org/tenant scoping is a first-class key prefix
  and a type-level property, not a review checklist item: the session-subject convention the
  realtime demo used is no longer the mechanism. Design:
  `docs/superpowers/specs/2026-08-30-tenant-scoping-design.md`; app-facing walkthrough:
  GUIDE §16; the typed surface is §3's `Scope` bullet.
  `#[table(scoped_by = Org)]` makes `(scope, id)` the row key and prefixes every index
  entry, and the scope-taking primitives are private, so a scoped row is reachable only
  through `tx.scope::<Org>(org, ctx)?` — which runs the app's own `Tenant::authorize`
  first, and whose refusal is the app's own `ApiError`. A tenant is entered by membership
  (a principal, through `authorize`) or by declaration (`public(Org)` on the route, or the
  operator's `TenantAdmission`), never by an anonymous `None`. A caller with no credential
  at a scope line its route did not declare `public(Org)` for gets a named 401 (spec
  2026-09-27-job-tenancy §4). A job acts inside a tenant only by declaring
  `#[job(scoped_by = Org)]`, and then only inside the tenant its enqueuer opened or its
  fan-out named, because no app code can write the job row that carries that tenant and
  an enqueue in a tenant of any other scope type is refused (§3's Jobs bullet). A table that declares neither
  `scoped_by` nor `global` does not compile: which tables are tenant data is the app's
  declaration at every struct, never the platform's inference (default-secure,
  2026-09-16; the field-type inference was weighed and rejected — the membership table
  must be `global` and must hold `Id<Org>`). The platform never *derives* a
  scope: the tenant is named by the app at that one line, or for a fan-out in
  `enqueue_in`'s argument, and nowhere else; a scoped job's record carries that name, and
  no key is ever parsed back into one. Realtime is the same mechanism rather than a second one —
  `emit` inside a scope is `emit_keyed` with the scope's key (§6), which retires
  `emit_keyed`'s hand-rolled scoping key as the stopgap it was. The composite key travels
  the whole way down: WAL effects, replication, the archive, `restore`, snapshot seeding
  and `migrate_schema`'s watermark all carry the scope, and a binary compiled against the
  unscoped shape of a scoped table refuses the record **by name** rather than misreading it.
  Enumerating every tenant is possible and **declared, never silent**:
  `#[route(…, cross_tenant)]` / `#[job(…, cross_tenant)]` mint the `CrossTenant` token
  `Tx::across_tenants::<S>(cap)` requires (and `Tx::enqueue_in` requires for a fan-out
  into a named tenant), rows come back paired with their scope
  (`Vec<(Id<Org>, Invoice)>`, never `Vec<Invoice>`), and `charpente describe` lists every
  route and job that declared it — an inventory, not a grep — beside each route's `public_scope` and each job's `scoped_by`. That is a capability, not an
  escape hatch on a guarantee (DECISIONS §11).
  The operator acts inside one tenant with a `TenantAdmission` minted from a `Group`
  (`Tx::admit_scope`), sealed the way `CrossTenant` is. No `authorize` runs, because the
  operator has no principal and already holds the store (DECISIONS §12).
  **Three limits travel with it, documented rather than engineered around.** The subscription
  key is the scope's decimal id, so two DIFFERENT scope types sharing a raw id share a key —
  an app with more than one scope type keys its subscriptions per scope type as well. And
  the guarantee's boundary is the typed surface handed to handlers and jobs: whoever holds
  the `Db` reaches every tenant through `Db::snapshot`'s scoped and raw reads, which take a
  scope on the caller's word because replication seeding and the archive are built on them,
  and can mint a `CrossTenant` through `begin_group`. A `Db` holder has the standing of
  whoever holds the data directory — an operator, not a request (DECISIONS §12, ratified
  2026-09-05; the `App::mount` notch that would make even the operator path enumerable is
  recorded there as available and unbuilt, under §11's trigger).
  And the platform's own tables are not scoped: the job queue and the event log are read
  through `get`/`scan` by any transaction, across every tenant — a job's payload and, for a
  scoped job, its tenant id; an event's payload and its `key`, which inside a scope is the
  tenant's id. The dispatcher and the sim oracles are built on those reads, so they stay
  open; what is sealed is writing a job row (§3's Jobs bullet). A route that exposes either
  table must filter by its own scope before it answers — for a job row, by the job (its
  `name`, whose `scoped_by` names the scope type) as well as its tenant id, since two scope
  types can share a raw id.
  Out of scope and staying out: nested scopes, cross-tenant writes in one transaction,
  platform-derived tenancy, and forced retrofitting of existing tables. API-key
  scopes/permissions are subject-only in v1 — this feature answers *which tenant*, never
  *what may this principal do*, and a permissions story earns its own design.

## 9. Open questions (tracked, not blocking)

1. **Hot reload of handlers** (cranelift-JIT route handlers for sub-second dev loop) —
   valuable, unproven, not load-bearing for M0.
2. **Read concurrency model** — snapshot reads on worker threads vs. everything through the
   core loop; measure before choosing.
3. **Query combinator surface** — how far toward relational before it stops paying for its
   type complexity. The one concrete instance so far: `#[index]` is exact-match only, so a
   range or comparison query falls back to `scan` filtered in Rust — the overdue sweep in
   `examples/invoicing/src/jobs.rs`, documented as the idiom under GUIDE's "`#[index]` works
   on any `Codec` field" heading. That is the evidence DECISIONS §4's trigger asks for, and
   one instance across one small app is not enough to read it either way.
4. **Client codegen** — answered: `crates/charpente-tsgen` shipped 2026-08-29, generating
   a TypeScript client from route/event types (§7, GUIDE §14).
5. **Compile latency mitigation** — platform as prebuilt dylib so app iteration recompiles
   app code only; quantify the win early, it is the adoption risk (see PLAN §Risks).
   **Quantified 2026-09-09.** The instrument — `bench-harness`'s
   `compile`/`compile_accounting` pair, measuring clean, handler-body and schema edits
   against `examples/invoicing` — refused its first reading in full on 2026-09-07, six of
   the nine observations to a floor that turned out to be measuring wall time where it
   meant compiler CPU. An accepted reading followed on 2026-09-09 (docs/BENCH.md,
   "Compile latency (2026-09-09)"), and it answers which phase hurts: on one 12-core
   laptop, `dev` profile, **the expensive phase is the clean build** — 5.97 s wall and
   37.6 s of compiler CPU — while the inner loop an agent actually meets is 436 ms for a
   handler-body edit and 515 ms for a schema edit, with only 1.18x between them. One
   reading, one laptop, one small app, n=3 is not a cross-platform claim, and it does not
   decompose those 436 ms into macro expansion, typecheck, codegen and link — so it sizes
   no mitigation on its own. **The prebuilt-dylib item is dropped (Noah, 2026-09-09).** Its
   stated mechanism — "app iteration recompiles app code only" — is already what cargo does:
   every incremental observation in that run rebuilt exactly one crate, the app's. What a
   dylib could still reach is the clean build, which is a CI-checkout cost rather than the
   dev-loop risk this item was opened against. Reopening it on inner-loop grounds needs the
   decomposition that reading does not have: split the 436 ms into macro expansion,
   typecheck, codegen and link, and show link dominant.
