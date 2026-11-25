# Charpente: Architecture & Design

Status: M0 through M3 shipped (launch, 0.1.0 on crates.io, 2026-10-10). PLAN.md's "After
launch (v1.x)" table holds what comes next. This document records the decisions the rest of
the design rests on, and the reasons for them. Decisions marked **[IRREVERSIBLE]** cannot be
retrofitted and are argued in detail. Everything else is chosen for speed and has a clear
upgrade path. Sections marked "(v0/v1, implemented)" describe the tree as it stands, and the
rest is design intent that hasn't been built yet. docs/LEDGER.md is the chronological record
of what shipped when.

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

Everything runs in one process. The HTTP layer translates requests into transactions, and
transactions run against the deterministic core. The core's only output is an ordered log.
Everything else (table state, queue dispatch, realtime feeds, replicas) is a deterministic
function of that log.

## 1. The deterministic core **[IRREVERSIBLE]**

**Decision.** Every source of nondeterminism (disk, network, time, randomness, task
scheduling) passes through trait objects owned by the runtime, each with a production
implementation and a simulation implementation. Application transactions execute inside the
deterministic boundary and cannot perform ambient IO.

**Why irreversible.** FoundationDB and TigerBeetle are unambiguous on this: deterministic
simulation testing only works if the system is designed for it from the first line. One
`SystemTime::now()`, one stray `tokio::spawn` or one direct file write inside the core is
enough to lose replay determinism, and with it the whole verification story that justifies
the platform. Retrofitting determinism onto a nondeterministic codebase is a rewrite. The
promise holds across build modes too: redb's open path issues an extra page walk gated on
`debug_assertions` (`Database::new`'s `mark_allocated_page_for_debug`, in 4.1.0 and still in
4.3.0). It used to draw sim IO rolls in debug builds only, which skewed a release build onto
a different universe from the same seed (found via seed `0xff159ed76d2806e4` under 4.1.0).
Release builds now force redb's debug-assertions on (`Cargo.toml`'s
`[profile.release.package.redb]`), so both modes consume one identical draw stream.

**Shape.** Synchronous and poll-shaped, not async futures (DECISIONS §10). `Instant` is
charpente's own nanosecond type, because std's is opaque and cannot be fabricated from
virtual time.

```rust
trait Clock   { fn now(&self) -> Instant; fn wall(&self) -> WallTime; fn sleep(&self, d: Duration); }
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
trait Outbound { fn start(&self, token: u64, call: OutboundCall) -> Result<()>;
                 fn poll(&self) -> Option<(u64, Result<OutboundReply>)>;   // a failure IS a completion
                 fn cancel(&self, token: u64);
                 fn next_wake(&self) -> OutboundWake; }                    // sim: At/Never; prod: Unknown
```

`Clock` has two readings. `now` is the monotonic `Instant`, process-local, for durations and
deadlines inside the process. It is never stored (no table codec exists for it) and means
nothing after a restart. `wall` is `WallTime`, UTC nanoseconds since the Unix epoch, for
every time that is stored or compared with anything outside the process. Each node builds
one `ProdClock` and clones it (same origin) to everything that reads time. The kernel reads
both once per step (`kernel::Now`). A stored wall due time (a job's `due_at`, which a timed
enqueue sets and the dispatcher queues at that time as soon as it commits, or a schedule's
`next_due`) becomes the loop's monotonic wake-up as `now.mono + (due - now.wall)`. That is
recomputed every step, so a wall-clock step moves a wake-up at the next step rather than
freezing it.

The wall half never runs backwards within a node. Every step's reading passes through a hold
(`kernel::clock::WallHold`). A reading at or behind the latest one is held at the latest,
and one more than `CLOCK_TOLERANCE` (60 s) behind stops the kernel by name. The hold is
bounded in time too. A reading still at or behind the held value, more than the tolerance of
monotonic time after that value last rose, stops the kernel by name (`clock stalled`), and
that is how a clock that has stopped advancing shows up. So each hold episode, a step back
or a freeze, extends sessions and delays ticks by at most the tolerance before the node
stops. No check sees a clock that runs slow, or steps within the tolerance that add up: a
clock that steps back within the tolerance, catches up to the time it held, and steps back
again has fallen behind by both. The node's own time stays consistent but drifts against the
clocks it is compared with (an identity provider's, another node's), and NTP bounds that
drift.

A restart loses the hold, so the same check runs at every open, whatever the node's role (a
follower judges session expiry on reads with its own clock). It also runs at a promote
(before its durable flip, so a refused promote leaves a follower) and at a hand-off's
target, which stops at its write-gate flip because the hand-off is the primary's point of no
return (§5). The bound it checks against is recovered from times the store already holds:
the newest `ApiKey.created_at`, and each `CronRow`'s `next_due -
interval_nanos`, the row keeping the interval its `next_due` was set with so that a
shortened interval cannot raise the bound above a time the store saw. A stored "latest wall
time" would be tighter, but it is a new field or record in the log, which changes the log
format and every pinned seed's byte counts. The recovered bound costs neither. Its price is
that it is only as recent as the last key or tick: a clock behind by less than the time
since then plus `CLOCK_TOLERANCE` opens unnoticed. That is also how long a session that
expired just before a restart or failover can resolve again.

`tx.now()` hands a transaction the same held reading, so the bound carries to it. A node
whose clock is behind by less than the time since the store's newest recorded time (an API
key's creation, or a schedule's last tick) plus `CLOCK_TOLERANCE` opens unnoticed, and an
app with no keys and no schedule of its own still has the daily session sweep, which makes
the figure about 24 h plus 60 s. A timed obligation an app builds on `tx.now()` can
therefore run late by up to that much, never early in the node's own frame.

The simulator implements all of it from one seed: latency distributions, virtual time (a
simulated year of heartbeats runs in seconds) and the fault matrix. The fault matrix covers
injected write/sync errors, crash-restart between any two disk operations, partitions, and
crash-time write-buffer collapse, in which every unsynced write independently survives,
tears (an arbitrary strict subset of its sectors) or vanishes. Subset tears matter because
they produce the file-extends-past-garbage layouts that defeat length-prefix-only recovery,
which prefix-only tearing never catches (found while building the first meta-test).

Three more axes landed with WAL segment management, paired with the recovery-horizon rule in
§2. They apply the Jepsen/TigerBeetle lessons: corruption granularity is its own fault axis,
and whole-sector injection provably misses single-byte bugs. `FaultConfig.bit_flip` corrupts
one random bit of one random byte in one random file's durable content at `Sim::restart`,
before crash collapse. `FaultConfig.misdirected_write` lands a `write_at`'s payload past the
requested offset while the call still returns `Ok` (the requested offset stays stale).
`FaultConfig.read_error` is a transient `read_at` failure (the next read may succeed). All
three (plus `dir_entry_loss`, §2) draw from per-field sub-streams keyed by (universe seed,
FNV-1a of the field name) rather than from `FaultConfig::swarm`'s legacy sequential draw.
Each field's stream is independent, so adding one perturbs no existing universe's legacy
draws and no other extended field's draws (`sample_extended_fields`, charpente-sim, applied
right after `FaultConfig::swarm` in every swarm-sampled universe).

The two durable-corruption axes (`bit_flip`, `misdirected_write`) are scoped to
`FaultConfig.corruption_scope` (default `"wal/"`). They exist to prove the WAL fences (frame
CRC + the recovery-horizon rule), and store-file bitrot is beneath those fences: v0's answer
to it is redb plus M1's restore-from-replica, and M2's own storage engine owns page
checksums. `read_error` and `dir_entry_loss` stay global, because they produce errors, never
silent wrongness. Even inside the scope, the horizon fence only defends checkpointed data.
Corruption of an acked frame above `applied_seq` is indistinguishable from a torn tail and
is legitimately cut. That is the standard WAL convention: un-checkpointed data has one local
copy, and M1 replication is the answer. So acked-durability workloads route their shortfall
through `checks::durable_corruption_fired`, and loss is excused only in universes where a
durable-corruption fault actually fired.

Two wire-scoped axes landed with M1 replication's second sub-project. They are sampled from
the same field-keyed sub-streams and rolled at `Conn::send` on the global fault stream,
since a connection spans two hosts and the wire belongs to neither. `FaultConfig.conn_drop`
kills the live connection: the sender gets `ConnectionReset`, the peer reads EOF, and
in-flight messages in both directions drop. `FaultConfig.net_spike` multiplies one message's
drawn latency ×64 (delayed, never lost).

Four object-store axes landed with the M2 archive. They are sampled from field-keyed
sub-streams and rolled off the store's own streams, so an archive's rolls never perturb the
disk and wire faults drawn from the same seed, and are never perturbed by them.
`FaultConfig.object_put_error` means the PUT does not land, and the prior value stays (a PUT
is atomic, never torn). `object_get_error` fails a GET or LIST. `object_lost_ack` lands the
object durably and tells the caller it failed. Under that axis, correctness depends on a
same-key/same-bytes re-PUT, which would otherwise be merely safe. `object_delete_error`
fails a DELETE. `delete` is contractually idempotent, so a failed delete is a retry and not
a lost object, and that makes an interrupted drain reachable.

The store draws from two streams. Put, get, list and lost_ack roll off
`keyed_stream(seed, "object_store")`, and the `Delete` arm rolls off
`keyed_stream(seed, "object_store_delete")`. The split is needed because `Rng::chance` draws
from the stream before comparing, so it consumes a draw even at `p == 0.0`. A delete roll on
the shared stream would have shifted every later put/get draw, re-rolling every pinned
universe merely by declaring the field and invalidating the convergence floors and both
recorded seeds with it. Drawing it separately makes the axis non-perturbing at zero, and a
measurement proves it: all twelve convergence coverage counts across four bases reproduce
exactly. The convergence property now enables the axis. A delete fails in 32 of 256
universes at the pinned base and every one of those universes still restored, and
`archive:delete_failed` sits in `require:`, so a batch where no delete ever fails is a red
property instead of a quiet happy-path pass. The two aimed `DeleteMode` switches stay
alongside it, because a probability cannot guarantee the shape where a delete fails forever
in a finite run. And `SimObjectStore` completes each operation in the `poll` that takes it,
so object-store latency is not modelled at all.

**What an object store owes, and what tests it** (2026-10-01). An object a `Get` or `List`
returns is durable, and an acknowledged `Put` is durable (`ObjectStore`'s trait doc).
`SimObjectStore` is that contract as a model. It has no page cache, so nothing it returns or
acknowledges can vanish, and the archive's properties test the archive against the contract
through it. The production store over a filesystem, `FsObjectStore`, has to earn the
contract with fsyncs: of the object, and of every directory from its key's directory up to
the bucket root's parent, on every `Put` and before every `Get` or `List` returns (prod.rs).
Before 2026-10-01 it did not. A `Put` that failed after its rename left an object that a
later process could read back from the page cache and the next power loss could take away.
And an acknowledged `Put` under a new prefix could lose its whole subdirectory to a power
loss, because the directories it created were never synced into their parents.

The simulator can't be its oracle. `Disk` has no rename, so a `Put` through it could not be
atomic. Its `open` creates, so a `Get` through it would create a file. And its names are a
flat namespace whose `sync_dir` makes every name under a prefix durable, so `SimDisk` cannot
express a directory whose own entry was never synced, which is the shape the second defect
needs. Giving `Disk` those would change a core trait (23 implementations, and a directory
tree in `SimDisk`'s crash model) to serve one store. The oracle is instead a crash model of
the filesystem calls `FsObjectStore` makes (`CrashFs`, prod.rs's tests). It is the POSIX
minimum: a power loss keeps only what a sync took, each directory on its own, and a process
restart keeps everything. Every durability decision sits above that seam, so the model runs
the code production runs. It cuts the power after every call of a workload and pins each
sync by name.

`ProdDisk` runs over the same seam and model, because `SimDisk` could not catch its defect:
`SimDisk`'s flat names keep `Disk`'s contract as written, and only a directory tree can show
the production impl falling short of it. Until 2026-10-01 it did fall short. `ProdDisk`
created the data directory, any directory above it and `wal/` without syncing any of them
into its parent, and a barrier fsynced only the one directory holding its prefix. So on a
fresh node every write acknowledged before the first checkpoint rested on a `wal` entry that
the next power loss dropped (§2's barrier).

The boundary of what charpente vouches for is the root's parent. `ProdDisk` creates the data
directory and `FsObjectStore` the bucket root, and neither ever creates a directory above
it. A root whose parent is missing is refused by name, because an entry created up there
would lie on no sync's chain, and a boot that failed before syncing it would leave it for
the next boot to trust. The ruling (2026-10-01) chose this over fsyncing every ancestor to
`/`, which fails on unreadable ancestors and on filesystems with no directory fsync, and
still cannot vouch for an operator's own unsynced `mkdir -p`. So the refusal tells the
operator to make the parent durable when they provision it. `FsObjectStore::new` refuses
such a root when the store is built, not at its first PUT. An archive reads a missing root
as a fresh bucket and absorbs a refused PUT, so a node would otherwise run with no backup
and no line on stderr.

Each entry in `ProdDisk`'s tree has exactly one sync that carries it, and each is pinned by
inverting it. A name under the data directory is carried by the barrier, which syncs up to
and including the data directory. The data directory's own entry is carried by
`ProdDisk::new`, which fsyncs its parent at every start, whichever boot created it and
before anything can be acknowledged (no barrier climbs above the data directory, and none
creates it). The parent's own entry is carried by the operator. That start sync also makes a
parent the node cannot read fail at startup, under the data directory's line. The model
states its own limits (prod.rs, `Fs`). A power loss drops all unsynced state,
deterministically and in full, so it finds a missing or misplaced sync but not a bug that
needs some unsynced state to survive. And its kernel never marks a page clean after a failed
fsync, so the EIO residual (§2) is out of its reach, as it is out of `SimDisk`'s.

**The store's crash surface, and the countdowns.** Every `Store::apply` commits at
`Durability::None`, and redb 4.3.0 keeps such a commit's pages in its own write buffer
(`write_barrier()` issues no file write, `cached_file.rs:594`) until a checkpoint, or until
the buffer passes 512 MiB, which no sim workload approaches. So the sim's crash surface in
the store file is checkpoint-only. What a crash can find unsynced there is one checkpoint's
page writes (beside the store's creation, and `grow()`'s extension, which 4.3.0 syncs before
any header records it, §4), and everything else a crash collapses is WAL. The torn-store
shapes that redb 4.1.0's unsynced page writes let the crash model produce (a file cut below
its header's layout, a sub-page tail) are therefore held by hand-built tests that write them
directly, not by the sim: engine.rs's `open_a_store_mangled_to` pair and transactions.rs's
`mangled_store_len_recovers` family. The sim does not model IO that no longer happens.

The same change re-sized the crash countdowns. A workload arms
`crash_disk_after(A + rand % B)` once per round, and it fires only if the round runs past
the drawn op count, so a range is calibrated to its workload's op volume. Under 4.3.0 a
round issues 57-90% of 4.1.0's disk ops (mean, per workload), and at a fixed base the 4.1.0
ranges fired 24-86% fewer crashes. Each range was re-sized on 2026-09-30 by counting every
round's ops with the countdown disarmed, at both versions and two bases. The goal is
`fault:crash` at or above its 4.1.0 count with every floor still holding, and the rule is
set per workload. Each range is scaled until that measured distribution predicts 4.1.0's
crash count plus one standard deviation at the batch's base. The exception is
`scoped_realtime_workload`, which takes no margin. With the margin it crashed 10% more often
than under 4.1.0 (124 against 113) and its exact `realtime:resume-backlog` floor fell to 122
of 125. Without it the batch crashes 117 times and the floor holds. A uniform scale by the
mean op ratio alone restores only 71-98% of the rate (per workload), because 4.3.0's saving
sits in the long rounds. Every batch's `fault:crash` is back at or above its 4.1.0 count at
its base.

Where a property's hard case had depended on 4.1.0's IO shape, the workload now reaches it
by construction. The receipt-upgrade property needs v1 to die while a receipt is durable but
not yet run, a window 4.1.0 widened by writing store pages inside it, so
`receipt_upgrade_universe` holds its first receipt Pending for one retry (kernel.rs). An
oracle's reach must not depend on a dependency's IO shape. A redb upgrade that changes the
IO shape again re-opens this calibration, as it re-opens the latch census (§4).

**The same split a second time: the directory barrier.** `SimDisk::sync_dir` rolls
`sync_error` off `keyed_stream(seed, "host-dir-faults:{host}")`, or off a global
`dir_faults` stream for the un-hosted disk. It does not roll off the shared `faults` stream
that reads, writes, file syncs and misdirected writes draw from. The hazard is the object
store's, generalised: `Rng::chance` draws before it compares, so a barrier sharing the
stream displaces every later roll, and adding or relocating a single `sync_dir` call
re-rolls every universe at a pinned base. That has been measured: `open_with_horizon`'s
durability fix (§2) added exactly one barrier and forced convergence-floor re-measures in
three separate test files on the same day. `sync_dir` gets its own stream because it is the
disk op whose call sites move for durability reasons rather than workload reasons, which
makes it the one a correctness fix is most likely to add. Unlike the delete arm, this split
was not free. That axis defaulted to `0.0` and could be introduced with every pinned count
reproducing exactly, whereas `sync_error` fires in existing runs, so the split re-rolled
every pinned universe once and the floors were re-measured against it. A test holds the
guarantee mechanically: `a_directory_barrier_does_not_perturb_the_other_disk_faults` inserts
a semantically no-op barrier and demands that the entire coverage map be unchanged. Without
the split it fails, one barrier having moved four read faults and two write faults.

**A sync with nothing to flush rolls no fault** (2026-10-01). `SimFile::sync` rolls
`sync_error` only when the file holds unsynced writes. A file with none has nothing to flush
and nothing to fail, so its sync always succeeds and draws nothing from the `faults` stream.
It is still a disk operation: it ticks an armed crash countdown and advances virtual time
like any other. This is the directory barrier's hazard again, from the other side: syncs
move for durability reasons too. §2's recovery syncs every segment before replaying it, and
after a power cycle every one of those segments is clean. Rolling on each of those syncs
injected failures no disk produces in that state and re-keyed every universe that recovers,
and late_follower's batch lost about 48 verdicts to them until this landed. The cost is that
the sim never shows a real kernel reporting an earlier background-writeback failure, which a
later fsync of a clean file returns on Linux. The sim has no background writeback (a write
stays pending until a sync takes it), so a clean file has nothing to report. The
recovery-sync failure path is held by deterministic tests instead (§2).
`a_sync_with_nothing_to_flush_draws_no_fault` holds the rule by comparing whole coverage
maps with and without a clean sync before every operation.

**A process restart is not a power cycle.** `Sim::restart` power-cycles: the page cache
collapses under the crash model before anything reopens. `Sim::restart_process` (2026-10-01)
restarts only the process. Open file handles die and nothing else happens. Every unsynced
write and directory entry stays pending and readable by the next open, and the next power
cycle still collapses it. That is what a supervisor sees after a kill, a crash or an exit,
and it is the only way to reach "the next process read back what the dead one never made
durable". Before it existed, a property reached that shape only by accident, when a boot
loop reopened without a restart. Readable doesn't mean durable, and that accident exposed a
real ordering bug: recovery replayed WAL frames it never synced, and the store outran the
log (realtime seed 0x7df8c43a1d2591e8; fixed, §2). The mode draws from no stream and clears
no countdown, so introducing it re-keyed no universe. Three pinned batches reproduced their
coverage byte for byte across the change (migrations, realtime's mixed-shape resume, and the
wal dir-entry property). The property that drives it on purpose is §2's class oracle,
`the_store_never_outruns_the_log_across_process_restarts_and_power_cycles`. It proves each
restart left the log something to read back with `Sim::unsynced_files()`, read right after
the restart. A restart that flushed the page cache would leave that list empty and the
property's coverage requirement unmet. Only open file handles are epoch-gated, as after
`restart`: a destructor that opens a file afresh through a `Disk` it still holds reaches the
disk. Charpente's own close goes through its open files.

**Per-host worlds** (landed with M1 replication's first task): `Sim::host(name)` gives a
named simulated host its own disk and its own fault + entropy streams. The disk is a
`DiskWorld`, unchanged: the same crash/countdown/epoch model, with one instance per host.
The streams are keyed sub-streams off (universe seed, host name), built the same way as the
extended fault fields (`keyed_stream`, formerly `sample_extended_fields`'s private `stream`
helper). So a host's rolls never depend on other hosts' op interleaving, and creating or
using a host draws from none of the frozen global streams. `Sim::disk()`, `Sim::entropy()`,
`Sim::restart()` and `Sim::durable_fingerprint()` are byte-identical to before this task,
and every pre-existing pinned seed replays unchanged. Virtual time and disk latency stay
universe-global. Virtual time is one dimension, not a per-host one, and a host's wall
reading is that time plus the Unix base (`SIM_WALL_BASE_NANOS`, 2026-01-01) plus the host's
offset (below). `SimHost::crash_now`/`crash_after`/`restart` mirror the global equivalents,
scoped to that host's disk (crash coverage is the new `"crash:host"` key, distinct from the
global `"fault:crash"`). They also tear down the host's network presence
(`NetWorld::teardown_host`). Every connection with that host on one side closes that side
and drops the in-flight queue addressed to it, since a dead host will never read those
bytes, and every listener the host owns is removed so that a restart's `listen` on the same
address succeeds. Listener tokens (bumped by a `next_listener` counter) stop a handle held
across a crash from accepting on, or clobbering, a later re-`listen` at the same address.
This is the sim-side prerequisite for the multi-node replication universes in M1's second
sub-project: nothing above the sim is testable without host-scoped crash/restart.

**Wall time and its faults** (0.3.0). `Sim::true_wall()` is virtual time plus the Unix base.
A clock's `wall()` (`Sim::clock`, or a host's `SimHost::clock`) is that plus its offset: its
own skew, drawn once and uniform within ±`FaultConfig::clock_skew` (the run's magnitude),
plus the drift its own faults added. A step back (`clock_step_back`: a chance per wall read,
and the largest step) lowers the offset and leaves it there. Downtime (`clock_downtime`: a
chance per power cycle, and the largest jump) raises it, as if the machine had been off,
while virtual time stays put. A process restart (`Sim::restart_process`) is not downtime.
The drift compounds. It is the running sum of every step and jump, and nothing pulls a clock
back toward true time, just as nothing does for a machine without NTP, so a long run's
offsets wander far past any single step. `SimClock::offset` reads one without drawing. Each
clock's skew and two axes draw on keyed streams of its own, so hosts disagree from their
first read and diverge further, and with an axis off nothing is drawn. They mark
`clock:skewed` (once per clock, at its first read), `clock:stepped-back` (with a
`:within-tolerance` or `:beyond-tolerance` sub-key by size) and `clock:downtime` (at the
first read that shows the jump). The sizes span both sides of `CLOCK_TOLERANCE`: skew within
±120 s, steps up to 120 s, jumps up to three days. That way the app harness sees the
kernel's hold, its stops, and the open refusal that follows a stop.

Only the app harness samples them (`run_app_universe`, each axis on in a quarter of runs,
from its own stream, after every other field). `default()`, `none()` and the swarm runners
leave them off, so no platform suite's pinned seed moved when they landed. A platform
property turns them on in its `run_universes_swarm_with!` `adjust`, which is handed the
universe's seed so that whatever it draws is keyed by the seed. A property holds a node to
its guarantee against that node's own clock readings, and states the true-time form through
each clock's offset from `true_wall`. The stops and the refusal (`checks::clock_refused`,
`FaultReason::ClockRefused`) carry a name, like the durable-horizon refusal. They count as
fallout only where a clock axis acted in the run (`checks::clock_fallout`), since a node
whose clock is right is refused only by a bug. `Sim::quiesce_clock_faults` freezes every
offset for a fault-free tail, which then waits out a refusal: the clock reaches the instant
the refusal names (`checks::clock_reopens_after`) as virtual time passes.

**Consequence for the concurrency model.** The core state machine is single-threaded.
Concurrency lives at the edges (HTTP handling, queue workers, replication) and is itself
scheduled through the deterministic executor. The executor's main rule comes from
FoundationDB's half-measures lesson: the kernel loop's choice of which ready event source to
service next is an explicit decision, made by the same code in production and in simulation,
and only the entropy behind the decision differs. If the production loop multiplexes by
racing a real channel while the sim draws from a different structure, the sim is not testing
the scheduler production runs. Throughput comes from the absence of network hops and lock
contention, not from parallel mutation. This mirrors TigerBeetle and Redis: a single thread
applying a log is astonishingly fast when it never waits.

The rule is implemented (M0 step 6). `charpente::kernel`'s `Kernel::run` builds the ready
set over requests (`RequestSource`), due jobs and due cron ticks, and draws one entropy pick
only when more than one is ready, so idle iterations spend no entropy. The request arm stops
the loop when the engine is poisoned (ruling 2026-07-05, spec
`2026-07-05-sim-cli-design.md`): a 500 goes to the client first, then `Db::poisoned()`
decides whether the loop continues. The loop body is one `Kernel::step` (M1 replication T8),
which services one ready source and returns, or reports the deadline to park on. `run` is
the thin wrapper that owns the park, so a replicated driver can own it differently without
forking the decision code.

Replication (M1) adds a fourth ready source, the `Repl` engine, whose role gates the set. A
follower (or a draining primary, since `is_primary` is false while handing off) serves reads
and ships its log but never runs jobs or cron, which would hit `TxError::NotPrimary`. A
follower→primary flip inside `Repl::step` rebuilds the due-heap and cron from the replicated
store in the same step (`Kernel::after_repl_step`, called from the `Ready::Repl` arm before
it returns), so a promoted node never services a request or a job against a stale heap. A
repl-configured node also caps every idle wait at a 5ms park tick, so in-flight replication
IO (sim messages or prod TCP) is polled promptly regardless of the next job/cron deadline.

Realtime (M1 sub-project 3) adds a fifth ready source, `Tail`. Unlike the deadline-carrying
sources (jobs, cron, repl), it is never timer-gated. Like a request, it is gated only by
something happening: a subscription becomes eligible the instant some commit advances
`next_event_id` past its `next_id`. The per-step read that checks this is skipped entirely
whenever no tail is registered, so a kernel that never sees an SSE subscribe does zero extra
IO and every pre-existing pinned seed replays unchanged. One unit of `Ready::Tail` work is a
bounded table read (`TAIL_BATCH_EVENTS` rows), serviced round-robin across live tails so one
deep resume never starves the rest of the ready set (§6).

The object-storage archive (M2) adds a sixth ready source, `Archive`. `ObjectStore` is the
fifth IO trait, which is a different count. Like `Tail`, the archive arm carries no timer.
Spec §8 states that it never reads wall-clock time, directly or transitively, which is also
why its snapshot cadence counts records rather than seconds. §8 permits the
data-and-attempts scheduling below but doesn't force it, since a counter-based backoff would
be clock-free too. A counter is ruled out because it would need a tuning constant that no
measurement in `docs/BENCH.md` can justify. So the archive's readiness is derived from data,
and the derivation is the subtle part.

`Archive::idle()` can't serve as the readiness signal. `idle` answers true in five states,
four of which still hide un-archived data, and it is a snapshot that only updates when you
step, so "skip while idle" is self-sealing: a write-gated archive resumes when the gate
opens only if something keeps stepping it. Instead, the kernel remembers `db.last_applied()`
as of the last step that found nothing to do and re-arms the arm when a freshly read
`last_applied` differs. That is sound because every source of new archive work requires it
to advance.

Three further clauses gate the set. The first is `Archive::stopped()`, which is terminal: a
stopped archive answers `idle() == true` forever, so an arm that watched only idle would
drive a dead archive for the life of the process and present it as healthy. The second is
`Db::verifying()`, re-read every iteration (spec §4 rule 1: a write-gated node uploads
nothing, and the gate opening must re-arm the arm with no step in between). The third is a
retry hold after a failed store operation, since a failed `start` leaves the archive in a
non-idle state permanently and would otherwise peg the loop for the length of an outage.

The data watermark does not release the hold. "New work can exist" and "a failed operation
should be retried" are different questions, and conflating them strands the archive where it
hurts most. Under a flood of requests that stage no record, the ready set is never empty (so
the kernel never parks) and `last_applied` never moves (so no commit fires either), and an
archive that hit one PUT error there stopped shipping, permanently and silently. A retry's
precondition is attempts passing, so the hold is released by any kernel iteration the
archive arm did not itself consume. That one rule covers a parked kernel, a committing
kernel and a saturated read-only one.

One unit of `Ready::Archive` work is exactly one `Archive::step` (start one upload, poll one
in flight, or read one `read_budget_bytes` bite), never a loop to completion. An archive
error is absorbed, never bubbled: an archive failure is not a database failure (spec §8),
and a `?` there would let a transient PUT kill a serving node.

The tail round-robin gives fairness among tails. It does not protect a tail from the request
arm. `Ready::Tail` sits in the same ready set as every other source and is picked by the
same single uniform draw, so a request arm draining up to `MAX_BATCH` per win (§2) competes
with a registered tail on the same coin flip it competes with a due job on. The tail arm has
no priority, no reserved slot, and nothing that preempts a batch. Two things follow that the
job-arm measurement below does not cover, and neither has been measured. First, one tail win
advances only one tail, by at most `TAIL_BATCH_EVENTS` ids, so with several live tails, or
handlers that emit faster than that, a tail's lag can grow while a flood lasts. Second, a
delayed job is merely late, but a tail that lags past the retention floor is dropped and
receives the terminal `charpente.gap` frame instead of its events (§6). Whether a request
flood alone can drive a tail past the floor at the default `event_retention` is an open
question. This document makes no claim either way, and the job arm's numbers do not answer
it.

The job arm is the one that has been measured, and group commit changed it from certain
service to service that is probabilistic in the queue depth. This is an observed property,
not a promise. It is stated as a bound because the mechanism is known exactly: arbitration
is one uniform draw, and a request flood drains in `ceil(FLOOD / MAX_BATCH)` request-arm
wins, so a due job must win at least one of that many coin flips. At a flood of 100 against
a `MAX_BATCH` of 64 that is two draws, so P(fail) = 0.25. Measurement matches the
arithmetic: 200 of 200 seeds serviced the job arm before the source drained pre-batching,
and 151 of 200 after. At a flood of 1024 it is 16 draws, P(fail) ≈ 1.5e-5, and 200 of 200
pass. So the platform commits to a structural property, not a statistical one. The job arm
is never excluded from arbitration: it takes the same draw as every other ready source at
every contended step. Nothing is promised about when it wins, or about service ratios. Two
properties hold that line, and they assert different things. That's worth stating because
one of the names overreaches. `due_jobs_are_not_starved_by_a_request_flood` asserts eventual
completeness: every due job has run by the horizon, a count that is identical whether a
request-arm win consumes 1 request or 64.
`due_jobs_are_serviced_before_a_request_flood_exhausts_the_source` asserts the interleaving
invariant, that the job arm is serviced at least once while the source still has outstanding
work. The second is not a bound on degree. A bound pinned by a test is a contract whatever
this document says, and greedy drain cannot honour a fixed ratio on a fast device anyway
(docs/BENCH.md: shallower queues mean smaller batches).

Outbound effects (spec `2026-10-13-outbound-effects-design.md` §4) add a seventh ready
source, `Outbound`, over the sixth IO trait. An outbound job's `call` runs in the job arm's
group and decides a request without sending it. The start is buffered and issued only after
that group commits, and a failed commit abandons it, so the job returns to its due-set with
no call on the wire. `Outbound::poll` consumes, as `ObjectStore::poll` does, so the kernel
drains it into its own completion buffer once per step before building the ready set, and
only while the in-flight table is non-empty. The source is ready when that buffer holds a
completion or the earliest in-flight deadline has passed. One win settles, fails or expires
a batch of flights in one group, committed once, with the job arm's commit-then-bubble
shape.

It also changes how the kernel parks. The idle deadline gains the earliest in-flight
deadline, and while anything is in flight the kernel parks no longer than the next
completion `Outbound::next_wake` names (`At`), or no longer than the replication park tick
when it answers `Unknown`. That is production's answer, since replies arrive from the
network. `Never` (every reply lost) leaves only the deadlines. The table is volatile. Every
step that finds the node not writable drops it, completion buffer included, and cancels its
tokens. The drop is keyed on the write gate, never the role, so every way the gate shuts is
covered. The jobs stay Pending in the store for whichever node is writable next, with the
same key.

`Kernel::with_outbound(app, clock, outbound)` wires it beside `with_archive`, and
`Kernel::builder(app, clock)` combines `.replication`, `.archive` and `.outbound` for a node
that needs more than one. Dispatch routes a row by its minted key, so an enqueue whose
`Job::OUTBOUND` contradicts the mounted `JobDef.outbound` (a hand-written `impl Job` or
`JobDef`) is refused by name at staging, before a key is drawn, as a tenancy contradiction
is. Dispatch's `contract:` panics stay the backstop for such a row already on disk. The
outbound source's invariants hold in release (hard rule 4):

1. A token is never started while in flight, and is never reused within a process.
2. A job in the in-flight table is never in a due-set.
3. The in-flight table is empty whenever the node is not writable.
4. `settle` never runs for a token that was not in flight.
5. No `start` precedes the commit of the group that staged its `call`.
6. With no outbound job mounted and nothing in flight, the kernel calls no `Outbound` method
   and draws no entropy for outbound: every existing app replays byte-for-byte, and every
   existing pinned seed names the same universe (the `Tail` arm's precedent above).

The HTTP kernel (`charpente::http`) lives inside the boundary. The sim feeds the
single-threaded kernel loop (`charpente::kernel`) through a scripted `SimSource`, and hyper
only translates wire↔struct at the edge (`charpente::serve`) and feeds the same loop.

## 2. The write model: transactions as pure functions over a log **[IRREVERSIBLE]**

**Decision.** A transaction is a deterministic function `fn(&mut Tx) -> Result<T, E>` that
reads tables, writes tables, enqueues jobs, and emits events. On success, its effects are
serialized as one record into a single ordered write-ahead log. Table state is the result
of applying the log. There is no other way to mutate state.

**What a transaction cannot do:** await the network, read the wall clock directly, generate
ambient randomness (it gets seeded entropy), or spawn tasks. In place of the wall clock it
reads `tx.now()`, the step's held wall reading: every route, job and job a schedule enqueued
in one kernel step reads the one value the loop took at the step's start, so a seed's replay
reproduces it. The log does not record the reading. External effects are expressed as
enqueued jobs that run after commit.

**Why.** Three properties fall out of this single decision:

1. **Replication for free.** Followers replay the same log through the same deterministic
   state machine and arrive at identical logical state: the same rows, the same job queue,
   the same event stream, at the same log position. Failover is log position arithmetic.
   Replication does not claim a byte-identical store file. redb's physical layout depends
   on how records were grouped into write transactions, and that grouping is a scheduling
   artifact (how many records happened to be ready at once). The log does not record it
   and is not asked to: the log fixes what became true and in what order, never how many
   commits it took to absorb. Byte-identity held incidentally while the store committed one
   transaction per record, and per-batch apply ends it. That is as true for a recovering
   primary as for a follower, since a node replaying its own log cannot reproduce the
   grouping its live traffic originally had either. The store is a materialization of the
   log (§4), and two materializations of one log agree on content, not on layout.
   `Db::store_write_schedule()` exposes a node's grouping as a digest, so the sim's
   byte-fingerprint oracle compares files only between nodes that actually scheduled their
   commits alike, instead of inferring it from which faults fired.
2. **Simulation for free.** A deterministic function of (state, input, seed) is trivially
   replayable. Every production incident is reproducible if you have the log segment.
3. **The dual-write class deleted.** Job enqueues and event emissions are rows in the same
   log record as the state change. Commit is a single atomic append + sync.

**The cost.** No `await reqwest::get(...)` inside business logic that also writes state.
The idiom is: transaction commits intent → queue worker performs the external call → result
comes back as a new transaction. This is the saga pattern, made mandatory and mechanical
where it is usually optional and built by hand. Agents handle this discipline better than
humans do, because it is a rule a compiler can enforce (`Tx` is `!Send` into IO contexts,
and no IO capability is in scope inside a transaction).

**Log format (v0, implemented).** Frames of `[seq u64][len u32][crc32 u32][payload]`
(crc over all three) are stored in segments named `wal/<base_seq:020>.log`. Rotation syncs
the old segment before the new one takes writes, so a tail `sync` is a whole-log durability
barrier. Recovery scans in order and cuts at the first invalid frame: it deletes the
orphaned later segments first (highest base first, each `sync_dir`-barriered) and only then
truncates the cut segment, syncing the truncation before accepting appends. Without the
truncation-sync, a same-length discarded frame beyond the cut can be resurrected by a later
recovery, checksum-valid and seq-correct. The delete-before-truncate order is a fence in its
own right (fence 3, below). Any IO failure poisons the log, because a failed fsync cannot be
retried with any confidence (fsyncgate). The only exit from poison is restart-and-recover,
and recovery is idempotent. The apply loop (`Engine`) holds the standing invariant: machine
state ≡ fold(apply, durable records). Submit is append → sync → apply → ack, and recovery is
sync → read → apply.

**Readable is not durable.** After a process restart the page cache still holds every frame
the dead process wrote, synced or not, and each one reads back valid. Recovery therefore
syncs each segment before reading any of its frames, every segment and not just the tail, so
every frame it hands to replay is durable. A segment whose sync fails hands none (fsyncgate:
a failed sync guarantees nothing), and the open fails naming the segment and the fix. Before
2026-10-01 the clean path replayed without syncing. A process restart then replayed an
unsynced frame and persisted the store past it, at open or merely by closing it (redb's
close commits durably). The next power loss took the frame, and `Db::open`'s own "the store
outran durable truth" assert fired. Realtime found it (seeds 0x7df8c43a1d2591e8,
0x5d889f4c1a9ac543).

**What the sync does not close.** On Linux a failed fsync can mark the dirty pages clean
(fsyncgate). The frame then stays readable without ever reaching the disk, and the next
process's fsync succeeds without writing it. Recovery's sync closes the process-death case
(a kill between a frame's write and its fsync) and narrows the failed-fsync case to what the
kernel reports. It cannot close it: no later fsync recovers a write the kernel has already
dropped. The sim models an honest kernel, where a failed sync leaves the write pending and
the next sync writes it.

The pins for recovery's sync are `a_record_recovery_replays_survives_the_next_power_loss`
and `a_recovery_whose_log_sync_fails_replays_nothing` (charpente-core `tests/engine.rs`) and
their WAL-level twins in charpente-sim `tests/wal.rs`. There,
`a_frame_recovery_replays_from_a_non_tail_segment_survives_the_next_power_loss` holds the
every-segment half: a tail-only sync loses a non-tail segment's unsynced frame, and the next
open refuses the log as a gap. The class's oracle is
`the_store_never_outruns_the_log_across_process_restarts_and_power_cycles`, which mixes §1's
`Sim::restart_process` with power cycles.

**The rule covers every boot-time read of a durable file, not only the log's.** A read that
a boot acts on syncs first, or has a stated reason it need not:

- The role file and the witness slots barrier their names and sync their bytes before they
  are trusted (§5).
- The horizon breadcrumb need not. It is written only after the store's checkpoint returned
  `Ok`, so even a value still in the page cache is a lower bound on durable truth and never
  over-claims.
- The store's `applied_seq`, which `Db::open` trusts before recovery runs, rests on redb:
  4.3.0's open fsyncs the store file (§4).
- Restore's frontier probe reads the destination's segments unsynced because it decides only
  to refuse or to replace. A refusal writes nothing, and replacing discards every local byte.
- One layer up, the bucket: `FsObjectStore` syncs every object a `Get` or `List` returns,
  and the directories down to it, before returning it (§1). An archive that opens and adopts
  its manifest acts on that `Get`.

The log has a verified read surface for the shipper (`Wal::read_from`/`Db::read_log`, M1
replication T3). It re-checksums every frame it hands back and refuses, by name, to hand back
one that fails: a frame corrupted on disk after append must never travel. The surface's
second entry is `Db::sealed_segments` (M2 object-storage-archive T5): the sealed WAL
segments, oldest first, excluding the active tail, each paired with its end by layout (the
next segment's base minus one, inclusive). That end is the archive's completeness target
for the segment. It is never the `end_seq` the archive is entitled to record in a manifest's
`SegmentRef`, which must come from what `read_log` actually returned.

**Three durability fences.** (1) *Held (recovery-horizon rule).* A cut can only ever discard
unacknowledged data, so recovery must tell a torn tail from mid-log corruption of committed
frames. `Db::open` reads the store's `applied_seq` (the durable horizon) before recovery and
threads it through `Engine::recover` into `Wal::open_with_horizon`. An invalid frame at or
below the horizon is a hard corruption error naming the frame, the horizon, and
restore-from-replica/backup, never a cut, and the error path mutates nothing: no truncation,
no segment deletion. Above the horizon it is a torn tail and cuts as before. The rule's
other half guards the state no invalid frame can reveal. If the log's `base_seq` exceeds the
store's own `applied_seq` + 1 (seq 0 for a store holding no records), whole acknowledged
records exist in neither the store nor the log, and `Db::open` refuses with a restore-naming
error before recovery runs. Checkpoint GC can never produce that state, since its floor is
bounded by its own checkpoint's `applied_seq + 1`. Standalone `Wal::open` (no store in
sight) keeps cut-at-first-invalid. M1 replication makes repair possible. One shape is
exempted because it provably loses nothing: a checkpointed store beside an entirely empty
log directory. Segment names are dir-barriered before any frame lands in them (fence 2,
below), so every route to a checkpointed store leaves at least one name-durable segment,
with one exception: a crash inside `Seeder::finish`, between the store checkpoint and laying
the seed segment. `Db::open` recognizes the shape and lays the segment `finish` would have
(base = horizon + 1, nothing to replay) instead of starting a fresh log at 0 (found by
replication T6, seed 0xcaef83b2feb5edf5).

A store discard-and-rebuild (§4) used to erase the store's own `applied_seq` before
recovery ran. That collapsed the horizon to `None` and reopened the hole this rule closes:
the store was the witness, and the rebuild threw it away first, so a corrupt frame at or
below the last checkpoint got cut instead of refused (horizon-collapse,
`.superpowers/sdd/horizon-collapse-investigation.md`, fixed 2026-07-23). A durable horizon
breadcrumb outside the store (`<store_path>.horizon`, written and barriered by
`Db::checkpoint`/`Seeder::finish` strictly after the store's own checkpoint persists)
survives the discard. `Db::open` now feeds `max(store.applied_seq, breadcrumb)` into
recovery, so a corrupt frame at or below a prior checkpoint still refuses by name even once
the store that recorded it is gone. Two fixed slots hold the breadcrumb (`Disk` has no
rename). Each write targets the slot holding the smaller or invalid value, so a torn
overwrite of one slot can never destroy the other's still-valid prior value. The breadcrumb
is always a lower bound (it never over-claims), and if it is absent, torn, or CRC-invalid it
degrades silently to no witness at all. `Seeder::new` discards any breadcrumb along with the
store and WAL it already discards, so a stale breadcrumb from a different, higher-numbered
prior timeline sharing the disk can never survive a reseed and over-claim protection the new
timeline never earned.

The breadcrumb protects a rebuild, and `Db::open` rebuilds only from a log that still
reaches seq 0 (§4). It never lifts the vanished-prefix bound: that bound is measured against
the store's own `applied_seq`, because the breadcrumb holds a horizon, not the records below
the log's base. Until 2026-10-01 the bound was the breadcrumb-lifted horizon, and a boot
whose store stopped short of the log's base passed it and panicked in the replay. A store
removed by hand reached that state, and so did a crash inside `Seeder::new` between
barriering the store's removal and the breadcrumb's (S5 R-I1, seed 0x64a183075fd398b9), and
so did an older copy of the store put back over a newer breadcrumb ("reached seq 38 but
expected 20", S5 R2-M1). The exempt empty-log shape above replays from the horizon, so a
breadcrumb past the store refuses there too, where it used to lay the segment past the store
and panic at the first write. `Seeder::finish`'s write therefore changes no outcome: a
seeded log starts at `applied_seq + 1`, never at 0. The `.horizon` sidecar is part of the
node's durable set: any manual file-level restore of `store` + `wal/` must copy it or delete
it alongside them. A leftover sidecar from the node's prior life would over-claim and refuse
a legitimate torn tail above the restored horizon (by name, never silently). The residual
window above the last breadcrumbed checkpoint stays as unprotected as ordinary above-horizon
recovery always has been. Noah's ruling: normal-recovery parity, not fail-closed.

(2) *Held.* `File::sync` does not persist a file's directory entry, so a freshly rotated-in
segment's name (or a store file's, at creation) could vanish on power loss, taking
acknowledged records with it, or a removed name could resurrect one a recovery already
discarded. `Disk::sync_dir(prefix)` is the barrier: on `Ok`, every prior create/remove of a
name starting with `prefix` is durable, under the same flat-namespace prefix contract as
`list`. The prod impl fsyncs every directory a name under the prefix sits in and every
directory above them, up to and including the data directory. It syncs the whole chain on
every barrier, because a boot that created a directory and failed before syncing it leaves
one that exists without being durable. The data directory's own entry is `ProdDisk::new`'s,
synced into its parent at every start, and nothing is created above the data directory (a
data directory whose parent is missing is refused by name, §1), so between them the barrier
and the start sync carry every entry the prod impl creates. Until 2026-10-01 it fsynced only
the holding directory, and on a fresh node the store's creation synced the data directory
before `wal/` existed, so writes acknowledged before the first checkpoint rested on an
unsynced `wal` entry (held by `CrashFs`, §1).

The sim tracks pending creates and removes. Only when `FaultConfig.dir_entry_loss > 0.0`
does it independently roll each pending entry at crash: an affected create vanishes even if
its bytes synced, and an affected remove resurrects the file with its last durable content.
When the field is 0.0 (its default) it makes zero rng draws, so every pre-existing pinned
seed replays unchanged. The log barriers rotation (before the new segment takes writes),
post-cut recovery cleanup (a durably clean directory after truncation), and every successful
open, including the clean recovery path that cut nothing. A barrier that fails leaves its
names pending and the segment file still on disk, so without that last barrier a reopen
finds a valid log and acknowledges appends into a segment whose name a crash can still
delete (found 2026-09-07, seed 0xb957ceaaf1716fd5). The store barriers creation and the
rebuild-discard-then-reopen path. Coverage key `fault:dir-entry`.

(3) *Held (recovery-cut ordering).* A non-tail cut truncates one segment and removes the
orphaned later ones, which are two separate durability events. Truncating first strands a
durable gap whenever a crash keeps the truncation but drops the removals: a shortened
segment beside a surviving orphan, which the gaplessness check
(fence 1's `refusing to guess`) then refuses to recover across, bricking the node on data it
was entitled to discard. Recovery removes the orphans first (highest base first, each
`sync_dir`-barriered, the same discipline `gc` follows when it removes ascending) and
truncates last. Every crash-intermediate is then a contiguous prefix, recovery re-finds the
same cut, and it is idempotent at every crash point (found by replication seed
0x4177819b4a2ba984). The test-side restore path that had masked the bug now gates on entry
loss having fired on the refused node's own disk since its last re-seed wipe
(`SimHost::dir_entry_loss_count`), so a platform-created gap fails loud again.

**Record payload (v0, implemented).** A committed transaction's effects are `[n: u32]`
followed by n tagged effects: put `0x00 [table][row]`, delete `0x01 [table][id]`, enqueue
`0x08 [job][payload][key: Option<String>][shape: u64]`, emit
`0x09 [event][payload][key: Option<String>][shape: u64]`, scoped enqueue
`0x0a [job][payload][key: Option<String>][shape: u64][scope: u64]` (a
`#[job(scoped_by = …)]` job's tenant, spec 2026-09-27-job-tenancy §6), scoped emit
`0x0b [event][payload][key: Option<String>][shape: u64][scope: u64]` (an event emitted on a
`ScopedTx`, its tenant beside the app's key, spec 2026-09-30-tenancy-completion §6),
outbound enqueue `0x0c [job][payload][key: Option<String>][shape: u64][okey: [u8; 16]]`,
scoped outbound enqueue
`0x0d [job][payload][key: Option<String>][shape: u64][scope: u64][okey: [u8; 16]]` (a job
declared `outbound`, its idempotency key minted at staging, spec
2026-10-13-outbound-effects §6), and migrate-put `0x04 [table][id: u64][row]` (§3's
`MigratePut`, below). Strings and byte fields are length-prefixed per the row codec. The
shape is the payload type's structural fingerprint (§3). The pre-shape `0x02`/`0x03` (the
same without the shape) still decode, with the shape unknown, and are never written.

A tenant-scoped table's (`Row::Scope != Unscoped`, tenant-scoping Task 4) put and delete
carry the scope instead: `0x05 [table][scope: u64][row]`, `0x06 [table][scope: u64][id]`.
The tag is chosen by the table's own scopedness, never by the scope's value, so a scoped
table still uses `0x05`/`0x06` at scope 0 (tenant 0 is a legal tenant, not "no scope").
Every other table's put and delete keep the `0x00`/`0x01` shape above, byte-identical to
before tenant scoping. A scoped table's `MigratePut` (tenant-scoping Task 6c) is
`0x07 [table][scope: u64][id: u64][row]`, a new tag rather than a widened `0x04` (the same
choice `0x05`/`0x06` made over `0x00`/`0x01`), so `0x04`'s bytes for an unscoped table stay
exactly as above. The scoped enqueue is a new tag rather than a widened `0x08` for the same
reason. It is chosen by presence, never by the tenant's value, so tenant 0 is `0x0a` too,
and its `JobRow` carries the tenant as a trailing `scope`, absent for every other row. The
scoped emit is a new tag rather than a widened `0x09` for the same reason. It is chosen by
presence, never by the tenant's value, so tenant 0 is `0x0b` too and a global emit stays
`0x09` byte for byte. Its `EventRow` carries the tenant as a trailing `scope`, absent for
every other row.

`0x0c`/`0x0d` are the outbound enqueues: `0x08`/`0x0a` plus a 16-byte idempotency key the
staging node drew from its `Entropy` (outbound-effects spec §6; a re-issue's key is copied
from its Dead record, not drawn). An older binary refuses them by name as unknown tags, so
downgrading after an outbound enqueue is unsupported. `0x0e
[flags: u8][job][payload][key: Option<String>][shape: u64][scope: u64 if flags & 0x01][okey: [u8; 16] if flags & 0x02][due_at: u64 nanos]`
is the timed enqueue (0.5), a job carrying its due wall time: flag `0x01` is scoped and
`0x02` is outbound, and any other bit is refused by name. The scoped flag is presence, never
the tenant's value, as for `0x0a`. An enqueue with no due time stays
`0x08`/`0x0a`/`0x0c`/`0x0d` byte for byte, and an older binary refuses `0x0e` as an unknown
tag, so downgrading after a timed enqueue is unsupported. `0x0f` is the next free tag. A
binary that does not know a tag (older, or a later one it hasn't caught up to) refuses it
by name rather than misreading a wider record.

Table effects are the transaction's collapsed write set in (table, scope, id) order.
Intermediate overwrites inside a transaction are observable by no one and are not logged.
Enqueues and emits keep call order. Replay resolves table names through the `Schema`
registry passed to `Db::open`. The enqueue key is the app's own identity for the job,
carried on its row and never interpreted or deduped on inside the deterministic boundary.
It is not the outbound idempotency key, which the platform mints (`0x0c`/`0x0d` above).

**Group commit (M2 sub-project 2, 2026-08-15).** The commit point is a group, not a
transaction. `Db::begin_group` opens one store snapshot and an empty group overlay.
`Group::stage` runs one handler against it and buffers that handler's effects record
without touching the WAL. `Group::commit` appends every buffered record, syncs once, then
applies them in seq order. `Db::transact` is the group-of-one case (`begin_group`, one
`stage`, `commit`), so the single-request path and a batched path execute the identical
instruction sequence, and tests have no second path to keep in agreement.
`Engine::submit_all` is the one append→sync→apply implementation underneath both.
`Engine::submit` is its length-1 convenience and has no production caller. The only thing
this buys is amortization: N transactions, one fsync (docs/BENCH.md).

**Ack-after-durable survives group commit by construction.** `stage` never appends, every
reply is held until `commit` returns, and apply stays strictly after sync. So no participant
is ever acked for a record that is not durable: a 200 for something not durable stays
impossible. The opposite case can happen: a record can be durable without any client having
been acked for it.

The batch is not durability-atomic. A batch large enough to straddle a segment boundary
takes an extra barrier inside `Wal::append`, which calls `rotate`, which opens with
`tail.sync()`: the same "rotation syncs the old segment" rule §2's log format states above.
A crash between that rotate and the batch's own sync therefore leaves a durable prefix, not
nothing. This costs no guarantee. The promise is ack-after-durable, not batch atomicity, and
a prefix cannot violate it: nothing in the batch was acked, since the replies were still
held, so no client was ever told those records existed. An unacked record that survives a
crash is the ordinary "the write may be committed" case the client contract already covers
(docs/GUIDE.md §5).

**The two-level overlay.** A handler reads through its own transaction overlay, then the
group overlay, then the store snapshot, which gives read-your-own-writes across the whole
batch and not only within one transaction. When `stage` returns `Ok` with at least one
effect, the transaction's overlay merges into the group's and its record queues for commit.
On `Err`, or on no effects, that overlay is discarded and the group is untouched. This
ordering makes panic-safety structural: a handler that panics mid-transaction cannot corrupt
the batch, because its writes were never merged. Row puts and deletes, secondary-index
effects and session rows are all in the overlay. Platform-allocated ids (`next_job_id`,
`next_event_id`) are not: no API hands one to a handler, and the counters bump at apply in
seq order, which keeps ids byte-identical on a follower that has no overlay at all.

**The overlay carries rows only.** Enqueues, emits and migrate-puts are not in it, so a
transaction batched behind a sibling's `enqueue` or `emit` does not see it. Before batching
it would have, since each transaction applied before the next took its snapshot. `JobRow`
and `EventRow` materialize at apply from the log record's enqueue/emit sections, never from
anything `Group::stage` merges. Read-your-own-writes across a batch therefore means rows,
not queue and event effects. This was the one semantic group commit changed that an app
could see, and it no longer can: no app code reads the platform's tables through a
transaction (§3's Jobs bullet). Inside the platform it still holds, and the dispatcher's
`Tx::get_job` reads through the same overlay. The pin is
`an_enqueue_is_invisible_to_a_later_sibling_until_the_batch_commits`
(`crates/charpente-core/tests/group_commit.rs`, seed `0x7C00_0901`).

The panic-safety claim above covers the batch's state only. The group is safe, but there is
no `catch_unwind` on the request path, so a panicking route handler still unwinds out of
the kernel and stops the node. Batching widened the blast radius from one request to every
reply held in that batch, which `serve` then answers with a generic 500. The job arm is
isolated (`dispatch.rs` catches inside the transaction closure and converts the panic to an
app error), which is why the two handler kinds are stated separately.

**The failure rule goes by what a participant saw, not what it wrote.** Every record in a
batch is durable before any of them applies, so an apply failure at record *i* leaves all N
on disk and destined to apply on restart. The participants that must not keep their answers
are the one whose record failed and everything that observed it, including a read-only
request, which sees a sibling's uncommitted effects through the group overlay. One
implementation of that rule (`CommitFailure::invalidates`) is shared by `Db::transact`,
`App::handle_or_subscribe` and the kernel's batched tail. They drifted the first time, when
there were three copies of it. The rule's client-facing half is a named contract in
docs/GUIDE.md §5: a 500 never promises rejection, only absence of confirmation.

**Retry-safe writes (0.7, spec 2026-11-22-retry-safe-writes §4.2).** A write route's
request that carries an intent id (`Idempotency-Key`) is looked up in the intents record
(§3) inside its own staged transaction, after auth. A repeat is answered from the record
without running its handler, so it stages nothing and costs no log record. A fresh write
that staged an effect records its reply in the same transaction, so the record commits with
the write it records or not at all, and there is no second commit point. A write that
changed nothing records nothing (`Tx::has_effects`), and a handler's `Err` drops the record
with every other effect, so a copy of either runs again. `Group::stage` reads through the
group overlay, so a repeat staged behind its first copy in one batch is answered from that
copy's uncommitted record. That answer is a read of a sibling's write, so
`CommitFailure::invalidates` fails both copies when the commit fails. The record keeps the
reply's headers and body up to 64 KiB. For a larger reply, or one whose transaction minted
a credential, it keeps the status and the reason, so no raw credential reaches it (§8) and
no kept reply pushes a record past `MAX_RECORD_BYTES`. A malformed id is refused before the
request's `fresh_token` draw, so a refused request moves no entropy stream. So is a write that
reached the wire with no id, with the same 400: `serve`'s edge conversion marks each request it
builds as from the network (`Request`'s crate-private `wire`), and the kernel refuses there,
after routing, because naming the route takes the router the edge thread does not hold. A
request handed to the kernel in process (`App::handle`, a `SimSource`) carries no such mark,
and without an id it runs as it always has. A route names the header it reads its id from with
`#[route(…, intent = header("…"))]`, for a webhook whose sender sends its own. The record's
`committed_at` is the `now` the request path is handed: the step's time, or the epoch
under `App::handle`, which has no time.

**The batch cap is a constant, not configuration.** `MAX_BATCH = 64` is asserted inside
`Group::stage`, so the bound is enforced, and the kernel shares the constant rather than
duplicating it as a literal. `DbConfig` carries operator-facing policy. A batch bound is
internal mechanism, like `REPL_TICK_NANOS`. There is nothing here for an operator to tune. A
batch is whatever the arm has ready at this loop iteration, so greedy drain already limits
itself by arrival rate, and the cap exists only to bound buffered memory (`MAX_BATCH` times
the existing 1 MiB body cap). A configurable throughput knob could also be tuned to flatter
a benchmark, and that is the one thing not to ship next to docs/BENCH.md. The kernel's arms
bound on items drained, never on records staged: a handler that returns `Err` or writes
nothing stages no record, so a record-bounded loop would be unbounded in the adversarial
case the cap exists for.

**Batching is within one ready-set arm, never across arms.** Requests batch with requests
and due jobs with due jobs, and arm arbitration is untouched. No timer, clock read or
entropy draw enters the commit path. Batch composition is a pure function of the ready set,
which the same-shape rule (§1) already governs, so prod and sim agree by construction.
Checkpointing stays at the commit funnel and may occur only between batches, never inside
one. Two things end a request batch early: the cap and a subscribe. A subscribe's cursor is
resolved after the commit, so a sibling staged behind it would have its events committed
before that resolution, and the tail would start past them, leaving the gap realtime's
membership oracle forbids.

**Isolation.** Serializable, trivially: transactions apply one at a time to the
single-threaded core. Read-only queries run against consistent snapshots (see §4) and never
block writes. Long transactions are rejected by construction. A transaction is CPU-bound by
definition, so "one at a time" costs microseconds, not milliseconds.

## 3. The typed surface

Every contract in the system is a Rust item, and every contract violation is a compile
error. App crates depend on `charpente` alone: all three derive macros resolve through
`::charpente::` paths, and `charpente::core` re-exports the deterministic core, so the
platform is a single dependency (plus `serde` for your own wire types, GUIDE §1).

- **Tables.** `#[table]` on a struct derives the row codec, the key type (`Id<T>`, typed,
  never a bare u64) and the secondary index declarations. The schema is the Rust
  definition. Every table declares its tenancy, `scoped_by = Org` or `global`, and a bare
  `#[table]` is a compile error naming both (spec 2026-09-16-default-secure-surfaces).
- **Scope.** `#[table(scoped_by = Org)]` sets a row's `Row::Scope`, and a scoped row's key
  becomes the `(scope, id)` composite (every secondary-index entry gains the same prefix).
  A tenant is a key prefix, so there is no `where` clause for someone to remember. A
  `#[table(global)]` gets `type Scope = Unscoped`, and a table that declares neither is a
  compile error (the Tables bullet above). The scope type owes an `impl Tenant` with two
  items: `NAME`, its identity, rendered wherever a scope is named (spec
  2026-09-30-tenancy-completion §4), and its own `authorize`. `scoped_by` without one is a
  compile error at the struct, and two tenant types under one `NAME`, or an empty `NAME`,
  are refused when the app mounts them. The six scope-taking primitives on `Tx` are private
  to `charpente-core`, so the only way to reach a scoped row is `tx.scope::<Org>(org, ctx)?`,
  the single auditable line per handler. It runs the app's `authorize` (or, on a route
  declared `public(Org)`, admits by that declaration) before it returns the `ScopedTx`
  that `get`/`scan`/`scan_from`/`scan_by`/`put`/`delete` live on. `authorize(tx, who: &Principal,
  scope)` is asked only about a principal the resolution ladder produced, for the request
  that asks (`Principal`'s fields are private, and it is not `Clone`). A caller with no
  credential is a 401 at the scope line, and the declared ways in, `public(Org)` and the
  operator's `TenantAdmission`, ask nobody (spec 2026-09-27-job-tenancy §3). The marker
  (`NoTenant`) and the trait that `View` bounds on (`UnscopedRow`) are both sealed, so an
  app cannot reopen a scoped table with one orphan-legal line. A scoped read on a bare `Tx`
  is an unsatisfied trait bound whose text names the table, its scope and the `tx.scope`
  line to write. `ScopedTx` derefs to `Tx`, so global tables stay reachable
  (`(*tx).scan::<Region>()`), and a transaction opens exactly one scope. §8 has the full
  treatment (spec `docs/superpowers/specs/2026-08-30-tenant-scoping-design.md`, GUIDE §16).
- **Migrations.** `#[table(version = N)]` versions a row's shape, and a bump without a
  registered `TableMigration` is a compile error. Ordinary Rust already catches a query
  that references a field that no longer exists. The mechanism below is for what Rust
  cannot see: a same-length field swap or retype that recompiles clean and misdecodes on
  disk.
- **Routes (v0 implemented).** `#[route(METHOD "/path/:param", auth | public | public(Org)[, cross_tenant])]`.
  The handler takes `tx: &mut Tx`, one arg per `:param` (same name, path order), at most
  one JSON body arg, an optional `cap: &CrossTenant` (see below) and an optional trailing
  `ctx: &Ctx` (session access), and returns `Result<T: Serialize, ApiError>`. Handlers
  run inside one transaction, and 200 is emitted only after the commit is durable. The
  OpenAPI description will be derived from the types, never hand-written (step 7).
  `cross_tenant` is the declared capability to enumerate a scoped table across every tenant
  (tenant-scoping spec §3). The kernel mints a `CrossTenant` token for a route that declared
  it, `#[route]` fills the handler's `cap` argument from it, and `Tx::across_tenants::<S>
  (cap)`, for a tenant type `S` (`S: Tenant`, so neither scope marker), requires one, so a
  handler on a route that did not declare it cannot compile the call, and
  `#[job(…, cross_tenant)]` says the same for a background rollup (whose `run` then takes
  the token after `tx`). The rows come back paired with their scope (`AllTenants::scan`
  yields `Vec<(Id<Org>, Invoice)>`, never `Vec<Invoice>`), so a cross-tenant result cannot
  be handed to code expecting one tenant's data. Naming a tenant needs no capability: a
  point read already supplies the scope. A route declares exactly one of `auth`, `public`
  or `public(Org)`, and declaring none is a compile error naming all three. `public(Org)`
  declares that every caller, credentialed or not, may act inside the `Org` tenant the
  handler opens, with no `authorize` (spec 2026-09-27-job-tenancy §4). On any other
  route, a scope line reached with no credential is a named 401, so `authorize` is only
  ever asked about a real principal. Both capability tokens, `CrossTenant<'g>` and
  `TenantAdmission<'g>`, borrow what the `Group` that minted them borrows and are not
  `Clone`, so neither can be parked in a `static` or a `thread_local!` past its group (spec
  2026-09-30-tenancy-completion §5).
- **Jobs.** `#[job(global)]` or `#[job(scoped_by = Org)]` structs, with typed payloads and
  a typed retry policy. Neither tenancy is the default, and a bare `#[job]` is a compile
  error naming both. A `global` job's `run` takes `&mut Tx`. A scoped job's takes the
  `ScopedTx` of the tenant it was enqueued in, which the dispatcher reopens from the tenant
  its record carries (§6). A scoped job is enqueued only from inside its tenant
  (`ScopedTx::enqueue`) or by a declared fan-out (`Tx::enqueue_in` with a `CrossTenant`).
  `Tx::enqueue` and cron take `global` jobs only, and a scoped job at either is an
  unsatisfied bound whose text names the fix. Enqueue sites and handler signatures are
  checked against each other (spec 2026-09-27-job-tenancy §5). Every job also declares
  what its Dead means, `on_dead` or `retain_dead`, and neither is the default (DECISIONS
  §13). An `on_dead` job writes `fn on_dead(self, tx, dead: Dead<Self>)`, which takes
  `run`'s transaction and capability. `retain_dead` keeps the Dead row with its last error
  and runs nothing. The stance is on `JobDef` and in `describe`, never in `FINGERPRINT`, so
  changing it changes no stored shape (spec 2026-11-18-0.5.0 §5 C). `Dead<'g, J>` and the
  `DeadRecord<'g>` it opens from borrow what the dispatcher's `Group` borrows, as the two
  capability tokens do, so a hook cannot park one past its group.

  Job rows are the platform's to write and to read, because the dispatcher opens the
  tenant a row carries without asking anyone. `JobRow`'s `Row::Scope` is the sealed
  `PlatformOwned`, so `Tx::put`/`Tx::delete` of one is an unsatisfied bound naming the
  enqueue family. A hand-written `Row` that borrows the `charpente.jobs` name fails to
  build at each reachable write (a `const` check, reported by `cargo build`, not `cargo
  check`). The dispatcher's Done mark and failure record go through
  `Tx::settle_job`, which takes a `TenantAdmission` only a `Group` mints. The event log and
  the migration record are sealed against writes the same way (spec
  2026-09-30-tenancy-completion §3). `EventRow` and `SchemaVersion` are `PlatformOwned` and
  named in `PLATFORM_TABLES`, so an app can forge neither an event into a tenant's feed nor
  a table's version past its migration. Apply writes an event row from an emit effect, and
  the migration executor writes schema-version rows through the crate-private
  `Tx::put_schema_version`, whose record is the one `tx.put` wrote. The intents record
  (`charpente.intents`, `IntentRow`), which the request path answers a re-sent write from
  without running it, is the fourth `PlatformOwned` table in `PLATFORM_TABLES` (spec
  2026-11-22-retry-safe-writes §4.2): a forged row would answer a request that never ran.
  Its one door is `Tx::get_intent`/`put_intent`/`delete_intent`/
  `intents_committed_before`, each taking an `IntentAccess` that only a `Group` mints, as
  `TenantAdmission` is; `put_intent` records its type in `written_as` like `tx.put`. The
  request path puts a row in the transaction of the write the row records (§2, "Retry-safe
  writes"), and a row's id is the first 8 bytes of a SHA-256 over the length-prefixed
  principal, route and intent id. No app
  code reads any of the four through a transaction either (spec
  2026-09-30-tenancy-completion §3).
  `View`'s reads bound `R: ReadableBy<Self>`, which a platform table meets for a `Snapshot`
  alone. A hand-written `Row` borrowing one of the names, scoped or not, fails to build at
  each reachable read, on a `Tx`, a `ScopedTx` or an `across_tenants` scan. The
  dispatcher's in-batch read of a job row is `Tx::get_job`, which takes the same
  `TenantAdmission` as `settle_job`.

  An enqueue whose tenant presence disagrees with the mounted declaration (a hand-written
  `impl Job` or `JobDef`) is refused by name, and so is a Pending row that disagrees,
  before dispatch starts (job-tenancy final review C1, M4). So is an enqueue in a tenant of
  another scope type than the mounted job declares (`JobDef.scope_type`, a `TypeId`). A row
  records a tenant id and not its type, and the fingerprint carries only the tenant type's
  `NAME`, so without this check a stand-in copying `NAME` and `FINGERPRINT`, or a `#[job]`
  whose `scoped_by` names another tenant type with the same `NAME`, would run the job in
  the same id of the declared type. The check needs both types, so it runs at enqueue only.
  A hand-built `JobDef` (its fields are public) can still pair a glue with a `scope_type`
  that glue does not open. `JobScope::open` asserts that the type the glue opens is
  `JobDef.scope_type`, inside the glue's `catch_unwind`, so every attempt of that job fails
  by name and it dead-letters with nothing written (spec 2026-09-30-tenancy-completion §7).

  Across binaries the fingerprint carries the tenant's `NAME` (spec
  2026-09-30-tenancy-completion §4), so a Pending row enqueued under a tenant type of
  another `NAME` is refused at startup as a foreign shape. That includes the refactor that
  turns a job module's `use billing::Org` into `use crm::Org`, whenever the two types'
  NAMEs differ (under the NAME-equals-identifier convention both may be "Org", and one app
  that mounts both is refused at mount). When a row's shape matches, a later binary that
  mounts the name has no type to check and dispatches the row in the scope type that
  binary declares. Such a row is either one a hand-written `impl Job` enqueued from a
  binary that did not mount the name, or one enqueued under another tenant type with the
  same `NAME`, which is that tenant by declaration. Nothing refuses either, so the operator
  drains such a job (runs the binary that enqueued its Pending rows until they are Done or
  Dead) before a deploy that mounts a scoped job the running binary does not (final review
  N1, NB2).
- **Events.** `#[event(global)]` or `#[event(scoped_by = Org)]` structs. Neither tenancy is
  the default, and a bare `#[event]` is a compile error naming both. `tx.emit(...)` is
  typed, and subscribers (server-side and client-side codegen) consume typed streams off
  the log. A global event is emitted on a bare `Tx` (`Tx::emit`/`emit_keyed` take global
  events only) and tailed by `Subscription::of`. A scoped event is emitted only on its
  tenant's `ScopedTx`, which records the tenant beside it (§4's `0x0b`, §6). It is tailed
  only by that tenant's feed, `tx.subscription::<Ev>()` on the same handle. Every mismatch
  is an unsatisfied bound. A scoped event on a bare `Tx` or in a global `Subscription`
  names the fix (`NoEventTenant`, sealed with `UnscopedEvent`). A global or other-tenant
  event on a `ScopedTx` or a tenant feed is E0271. A scoped event's fingerprint continues
  its fields' fold with its tenant's `Tenant::NAME`, so a change of tenancy is a change of
  shape (spec 2026-09-30-tenancy-completion §4, §6). `Event::PAYLOAD_FINGERPRINT` is that
  bare fold (the default, `FINGERPRINT`, for a global event), and every typed emit and
  subscribe site checks the equation in a `const` block against the tenant its bound names
  (§8). An event payload is itself a wire type (it is the JSON a subscriber receives), so
  `#[event]` derives `Wire`. `events![…]`/`App::mount_events` register the type for
  `describe`, and its scope for the mount-time tenant census. Delivery has no registry:
  `Subscription::of::<Ev>()` and `tx.subscription::<Ev>()` capture their encoders
  statically at the subscribe site, so "unregistered event at delivery" cannot be
  expressed, and there is no error case for it.
- **Wire.** `Wire` is the trait describing what a type looks like once serde has encoded
  it. It is the mechanism behind the client-side codegen named above and behind `Id<T>`'s
  promise to never be a bare u64. `#[derive(Wire)]` implements it for a struct by reading
  serde's own `rename`, `rename_all`, `skip`, `skip_serializing` and `skip_serializing_if`.
  Any other `#[serde(...)]` attribute is a compile error rather than a silent
  pass-through, and tests pin that supported set by comparing the derived schema against
  real `serde_json` output. The one exception is `#[serde(crate = "...")]`, accepted and
  ignored on both structs and enums. It redirects serde_derive's own generated paths
  (`#[event]` emits it so app crates stay single-dep) and changes no field, name or wire
  shape, so there is nothing for the schema to get wrong, and the `.stderr` pins list it as
  supported alongside `rename_all`. For an enum the derive describes whichever of serde's
  tagged representations the type uses: a unit-only enum as a string union, otherwise
  external (default), internal (`tag`) or adjacent (`tag` + `content`). `untagged` is
  refused outright, since an untagged value carries no discriminator for the schema to key
  on. The derive also refuses at compile time any variant shape a representation still
  can't faithfully carry, for example a newtype variant under internal tagging, which serde
  either refuses to serialize at all or merges into the tag object in a way no per-variant
  schema slot can reconstruct. `TypeSchema`/`TypeDef` are the IR a generated client is
  emitted from. 64-bit integers are not wire primitives: a JSON number loses precision
  above 2^53 in JavaScript, so they cross the wire string-encoded. That guarantee survives
  the process boundary. `Id<T>` is itself string-encoded on the wire, and the generated
  TypeScript client brands the string (`type CustomerId = string & { readonly
  __table: "Customer" }`), so a value typed for one table cannot satisfy a parameter
  typed for another. `WireU64`/`WireI64` are the sanctioned carriers the `Wire` bound
  requires in place of a bare `u64`/`i64`, which is a compile error at any wire position,
  naming the fix, because a JSON number silently loses precision above 2^53 in
  JavaScript. The carriers cross the wire the same way `Id<T>` does, as decimal strings,
  and surface in the generated client as `bigint`.
- **Config.** A typed struct, with no stringly `env::var` in app code. Startup validation is
  not built yet: there is no `fn validate` on `AppConfig`/`DbConfig` today.
- **Telemetry (M2, not built).** Counters, histograms, and log fields are declared as types
  with change-time checks. Deleting a metric a dashboard depends on, or renaming a label,
  is a detectable breaking change against a lockfile of the observability contract, the
  same way Cargo.lock pins dependencies, so instrumentation cannot silently rot. Nothing in
  this bullet exists today: `describe.rs`'s `TelemetryInfo` is an empty struct.

**Migrations (v1, implemented; spec `docs/superpowers/specs/2026-07-16-migrations-design.md`).**
The organizing rule is that no code path ever decodes row bytes under any version other
than the one they were encoded at. Where that cannot be guaranteed, the node does not
guess: it refuses by name. `#[table(version = N)]` defaults `N` to 1, so every table until
now is version 1 with no code change required. Bumping to `N >= 2` requires keeping the
prior version's struct in code under an explicit name (`#[row_shape(version = N-1)]`,
codec-only: no table identity, no indexes, no redb registration) and
writing `impl TableMigration for CurrentStruct {
type Prev = PriorStruct; fn migrate(prev) -> Result<Self, MigrationError>; }`. The macro
emits a const assert requiring the impl, so a bump without one fails to compile, with a
missing-trait-impl error at the struct. Every version also carries a structural
fingerprint, which catches the hole compile-time checking cannot: a same-shape struct
edited without bumping `version`. It is a hand-rolled, fixed 64-bit FNV-1a over the
version's `(field_name, type tokens)` sequence, computed at macro-expansion time and
exposed as `Row::FINGERPRINT: Option<u64>`. The macro renders the tokens itself, one space
between them, never through the compiler's printer, whose spacing is not stable across
toolchains. `Row::FINGERPRINT` defaults to `None`. A hand-written `Row` impl (the
platform's own `JobRow`/`EventRow`/`SchemaVersion`) stays `None` and is invisible to the
gate: not using `#[table]` silently opts a table out of drift protection.

**Payload shapes.** `#[event]` and `#[job]` carry the same structural fingerprint
`#[table]` does (`Event::FINGERPRINT` and `Job::FINGERPRINT`, which a hand-written impl
must supply: there is no default), and every emit and enqueue records it in the log beside
the payload. A payload is decoded only under the shape it was written with: delivery
refuses a foreign-shape event by name and ends that one tail, and a node refuses to
dispatch while a Pending job of a type it mounts carries a foreign shape (§6). There is no
per-row versioned decode (the migrations spec rejected it twice) and no override
(MANIFESTO §8). The fingerprint hashes type tokens, not definitions, so a change inside a
type a field holds leaves it unchanged (GUIDE §12). A decode failure under a matching
fingerprint is therefore reported naming both causes, that change or corrupt bytes, with
both fixes.

**The gate, the barrier, and the seven refusals.** A built-in table,
`charpente.schema_versions`, records each versioned table's last-known `(version,
fingerprint, migrating_through, migrating_to)`. Its row is written lazily, piggybacked on
the table's first data write in the same effects record (adoption-at-first-write:
`Db::open` writes nothing unless it migrates, so pinned seeds' fault schedules are
undisturbed). The read-only check runs at every `Db::open` on every node, an ordinary
follower's included, where it is a no-op admit for a converged, same-binary node. The
rewrite itself is a separate step, `migrate_schema()`. The kernel runs it at startup,
before serving anything, on a node that is writable (single node or primary). A follower
runs it only at promote, resuming any in-flight watermark the dead primary left. An
ordinary follower never runs it and receives the rewrite only through shipped `MigratePut`
records (below).

Ordinary WAL-tail records for a table below its compiled version replay decode-free. A
row's id (or, for a scoped table, its `(scope, id)` composite) is a version-independent
leading prefix, so no foreign-version decode is needed, and a scoped table's
below-compiled Put/Delete carries its scope through this path the same way its
compiled-version replay does. The authoritative decision runs once replay completes. It
admits a table whose history reached the compiled version or is a resumable in-flight
migration, and refuses everything else by name. Seven named refusals cover the space:
fingerprint drift under an unchanged version; version downgrade; the upgrade barrier
itself (a foreign, unrecovered WAL tail from an older binary); its in-flight-target
variant (an interrupted migration aimed at a version other than the compiled one); its
in-flight-target-fingerprint variant (an interrupted migration to the compiled version
begun by a binary whose target shape differs, caught without a new field because the
persisted `fingerprint` field already carries the original target's shape); a missing
migration chain link; and a migration-transform failure (a rejecting
`TableMigration::migrate`). Two checkpoints, one on clean shutdown and one at open after a
replay, reduce the barrier's remedy to "run the previous binary once to a clean shutdown,
then upgrade", with no new operator verb.

**Execution: `MigratePut`.** The rewrite is an ordinary sequence of `Db::transact` calls
using one new effect kind, `MigratePut(table, id, bytes)`. It is applied everywhere (live
commit, crash-replay, follower apply) by overwriting the row's stored bytes without
decoding the old value and leaving indexes untouched, so the second
decode-under-the-wrong-version path (`Writer::replace_row`'s index-upkeep decode) never
sees a mid-migration row. The executor batches `DbConfig::migration_batch_size` rows (at
least 1, default 1000; 0 is refused by name before anything is written, since an empty
batch is the close-out signal) per transaction over a bounded range read. It commits each
batch's rewrites and its watermark advance (`migrating_through`) together, so a crash
between them is impossible by construction. The closing transaction clears the watermark
and bumps `(version, fingerprint)` to the target. That is the single moment a table is
considered migrated, and the moment its apply trigger rebuilds that table's secondary
indexes wholesale. The rebuild is deterministic and idempotent, so primary and followers
rebuild at the same log position with no separate mechanism. `migrating_through` is
`Option<(u64, u64)>`, a `(scope, id)`, for every table, scoped or not. A bare-id watermark
cannot resume a composite-keyed table's bounded range read correctly: ids are not globally
ascending across a scope boundary, so a scoped table's migration walks the same
`(scope, id)`-ordered cursor `scan_raw_from` does. The persisted row tells an old
mid-migration watermark from the current shape by length alone. The current shape is
encoded as `[1][scope: u64 le][id: u64 le]` (`migrating_to` is always `Some` exactly when
`migrating_through` is), and the two shapes' possible byte counts are disjoint, so a binary
from before scoped tables existed is refused by name, not misread as a fabricated scope.
An idle row (`[0]`, no in-flight migration) is byte-identical either way.

**Migrations: replication.** Followers never migrate locally. The primary's migration is
ordinary log records, received through the existing shipping path unchanged. A follower
applies `MigratePut` decode-free and rebuilds indexes at the close-out apply like any
other record. A fresh follower is snapshot-seeded with the primary's `schema_versions`
rows already in it, a converged follower is simply behind, and a follower promoted
mid-migration resumes the remaining batches itself at promote. `Hello` gains
`schema_digest: u64`, an FNV-1a fold over every fingerprinted table's
`(name, version, fingerprint)`. It is checked after the peer-secret and protocol-version
checks and refused by name on mismatch, mirroring the `proto` refusal, so a
schema-mismatched peer never seeds or ships a single frame. While a table's migration is
in flight to a follower, its copy is mixed-version and must not be read under the
compiled type. No v0 follower serves typed app-table reads today (jobs/cron are already
follower-gated, and realtime tails read `charpente.events`, which table migration does not
touch), so this invariant is documented, not enforced.

**Query API [reversible, start simple].** V0 is get/put/scan over typed keys plus typed
secondary indexes: KV-shaped, not relational. Rich predicates, joins, and aggregation
arrive as typed combinators once real usage shows which ones matter. A read-only
Postgres-wire endpoint (for BI/analytics tools) is a later, separate surface and never the
primary API. Read accessors are generated against the `View` trait, which both `Snapshot`
(outside transactions) and `Tx` (inside one, where reads also see the transaction's own
writes) implement. Follower reads need zero app-code changes. Effect-free transactions run
unchanged on any node, and an effectful one on a node marked a read-only follower
(`Db::forbid_writes`) is rejected with `TxError::NotPrimary` before it ever reaches the
log (§5).

## 4. Storage engine [reversible, staged]

**Stage 1 (M0): `redb`** (a pure-Rust embedded B-tree) mounted behind the `Disk`
simulation boundary through its backend abstraction, so crash/torn-write fault injection
exercises the real storage code from day one. redb was chosen because it is proven and pure
Rust, and because the WAL + snapshot design above does not depend on the tree's internals.

**Stage 2 (post-M1): purpose-built LSM.** The log-structured write model wants
log-structured storage: the WAL is already the L0. Building our own engine is the scariest
20% of the project and also the moat, but it becomes safe to build only once the
simulation harness exists to prove it. So the harness comes first and the engine second.
Swapping engines under the trait is a contained change because nothing above the
`Disk`/table layer knows tree shape.

**Table store (v0, implemented).** redb mounts behind `Disk` through a `StorageBackend`
adapter (`table::Store::open`; the raw-path constructors are deny-listed). The store is a
materialization of the log, never a second source of truth. `apply_batch` commits a
contiguous run of records' row effects atomically with the `applied_seq` meta key in a
single redb write transaction at `Durability::None`. That asks for no fsync, since the WAL
is the durability story. redb 4.3.0 keeps such a commit's pages in its own write buffer
without writing them to the file, and does fsync the file after extending it. Every record
in the run commits or none of them does: an apply failure anywhere rolls the whole batch
back and reports nothing applied, so the `applied_seq` that transaction leaves durable is
always a batch boundary, never an interior seq of a batch that did not commit.
`apply(seq, …)` is that batch with one record. It does the same disk work a per-record
commit always did (one read txn, one write txn, one commit), which is what keeps a
never-contended universe tracing identically to pre-batching code. Batching widened the
store's commit point and left the log's alone: the WAL's durability stays prefix-shaped
(see "the batch is not durability-atomic" under group commit above). `checkpoint()`
persists everything at once. After a crash the store reverts to its last checkpoint and
recovery replays the WAL from `applied_seq + 1`.

The sim established two facts about redb: its byte layout is deterministic under identical
op sequences (raw-fingerprint replay holds), and `Database::drop` is a graceful close that
durably flushes non-durable commits. So a crash is only a crash if the process dies with
the database open, which is what the sim models. A third is a dependency charpente does not
re-check at runtime: redb 4.3.0's writable open fsyncs the store file (`Database::new` →
`TransactionalMemory::begin_writable`, `db.rs:1672`, `page_manager.rs:1458`; a repairing
open also flushes at `page_manager.rs:888`). `Db::open` relies on it. It reads the store's
`applied_seq` before recovery runs and, beside an empty log, lays a durable segment at
`applied_seq + 1` (the mid-seed-finish shape, §2). A seeder whose checkpoint fsync failed
leaves that `applied_seq` in the page cache only. Without the open-time fsync, a power loss
after the laid segment would leave a store at `None` beside a log based past it, a node
that refuses to open. The D1 review's probe found the branch safe for that reason alone:
`Seeder::finish(5)` with every store fsync failing, a process restart, then a power loss
under the running node, and the reopen held `Some(5)` (2026-10-01).
`a_seeded_store_a_restarted_node_lays_its_segment_from_survives_the_next_power_loss`
(charpente-core `tests/engine.rs`) pins it, so a redb upgrade that drops the open-time
fsync turns it red. Re-walk it with the latch census
below on every redb upgrade. Index entries are `[key len: u32][key
bytes][id: u64 be]` (the length prefix prevents prefix-collision ambiguity), with
exact-match lookups only until a real query pattern demands order-preserving key encoding.

A built-in `charpente.jobs` table, the queue dispatcher's substrate (§6), materializes
transactional enqueues in the same apply, one `JobRow` per enqueue effect. Its id comes
from the monotonic `next_job_id` meta counter (bumped atomically with `applied_seq` in the
same store transaction, deterministic across replay), and the row also holds name,
payload, key, status (Pending/Done/Dead), attempts, due_at and last_error. Done and Dead
rows are retained (GC is a later question, filed with WAL segment GC).

A store whose bytes redb rejects (a crash tore its creation, for example) is discarded at
`Db::open` and rebuilt by replaying the WAL from scratch, since the log is the only source
of truth. `Store::open` reports that verdict as its own error, `table::StoreRejected`,
classified by type in `table.rs`'s `open_error`: redb's `StorageError::Corrupted`, its
`DatabaseError::UpgradeRequired` (a commit slot naming an older file format), and the one
`io::Error` redb builds from the bytes at open, "Not a redb database: magic number
mismatch" (`page_manager.rs:1137`). That last one has the same type as an IO error the
disk returns, so `DiskBackend` wraps every error it passes up (`FromDisk`, `Display` and
kind unchanged), and only an unwrapped `InvalidData` counts as the verdict. A variant not
named there bubbles: the rejection deletes a store, so it needs positive evidence. Every
error redb 4.3.0's `Builder::create_with_backend` can return, read in its source:

| redb returns | class | why |
|---|---|---|
| `Storage(Corrupted(_))` | rejected, `Corrupted` | A verdict on the content: header fields, slot and page checksums, layout against length (`header.rs:254`/`:301`), btree, allocator. |
| `UpgradeRequired(v)` | rejected, `UpgradeRequired { version }` | The commit slot's format version byte, read before its checksum (`header.rs:505`). |
| `Storage(Io(e))`, `e` unmarked, kind `InvalidData` | rejected, `NotARedbFile` | The magic mismatch. The empty-file error beside it (`:1145`) cannot occur while creating is allowed. |
| `Storage(Io(e))`, `e` is `FromDisk`, kind `UnexpectedEof` | bubbles, named | A file of 9 to 319 bytes with its magic intact: redb reads its 320-byte header past the end. See below. |
| `Storage(Io(e))`, `e` is `FromDisk`, any other kind | bubbles | The disk's own error, passed through. |
| `Storage(Io(e))`, `e` unmarked, any other kind | bubbles | redb's `invalid_input`/`unsupported`: no verdict on the bytes. |
| `Storage(ValueTooLarge \| Unsupported \| PreviousIo \| DatabaseClosed \| LockPoisoned)`, `UnorderedKey` | bubbles | Not a verdict on the content. |
| `DatabaseAlreadyOpen` | bubbles | Unreachable: `DiskBackend`'s lock methods return Unsupported, and `lock_whole_storage` returns Ok. |
| `RepairAborted` | bubbles | Needs a repair callback; the default does nothing. |
| `TransactionInProgress` | bubbles | Not produced at open. |
| a later variant (both enums are `#[non_exhaustive]`) | bubbles | No positive evidence, so never destructive. |
| `Store::open`'s own `disk.open`, `disk.sync_dir` | bubbles | Never classified. |

Every other open failure is not about the bytes. It is the disk's own IO error, or redb's
latch after one, so `Db::open` leaves the store alone and returns the error with its site
and its fix named and its kind and text kept. A restart retries the same file, and if
every restart fails the same way, the disk is failing. One bubbling error no restart fixes
is a store file cut shorter than redb's header with the magic intact. redb makes a new
file's length durable before it writes the magic (`page_manager.rs:1181-1200`), so no
crash leaves one: it takes truncation outside the crash model. It is not a rejection,
since redb built no verdict, so `Store::open` names the damage and `Db::open` names the
fix, chosen by the log beside the store. While the log still reaches seq 0, the fix is to
remove the file, and the next boot rebuilds the store from the log. Once the log is
compacted, the store holds the only copy of the records below its base, so the fix is a
restore from a replica or backup. Removing the file anyway is refused by name: `Db::open`
refuses a store holding no records beside a log compacted past seq 0, whatever the horizon
breadcrumb says (§2). M1's first text named removal for both logs, and on a compacted one
the next boot panicked (S5 R-I1). The refusal names the restore, and for a follower the
re-seed it amounts to, with the config the node must restart under (§5's re-seed rule).

Before 2026-10-01 any `Store::open` error took the discard path. One transient fault
deleted a healthy store on a full log, and on a compacted log refused the boot with
"restore this node from a replica or backup" (follow-up 21, found by the S4 review: an
injected fsync failure inside `Store::open`).

The rejected class includes a store file whose length disagrees with its durable header's
layout. redb 4.1.0 asserted on two such states (page_manager.rs:231/:257), and
`Store::open` fenced that panic into an open failure. In one, an unsynced `set_len`
dropped at crash while the header god-page write survived (`:231`). In the other, a crash
tore a data-page write inside an unsynced `set_len` extension that dropped, and the
surviving sectors left a file length that is not a whole number of pages (`:257`). Both
were sim-found, under a crash model that is ALICE's unordered-writeback. redb 4.3.0
returns `StorageError::Corrupted` for both ("File truncated below stored layout", "File
length does not correspond to a valid region layout"; `header.rs:254`/`:301`), so the
fence is gone: an unopenable store is an ordinary `Err`, and a redb panic, at open or
later, propagates as the real failure it is. 4.3.0 also closes the cause: `grow()` now
syncs a file extension before any header can record it (`page_manager.rs:2372-2379`), so
the crash model no longer produces either state (READ in source, and MEASURED: none in
16,896 `job_tenancy_universe` universes, including 4,096 at the same bases where 4.1.0
produced two, and none in 12,800 `kernel_workload` ones).

Rebuilding a rejected store works only while the log still retains seq 0. `Db::checkpoint`
durably persists the store, then deletes WAL segments below the checkpoint (`Wal::gc`,
floored at `DbConfig::retain_segments`, default 2; a caller-supplied retention pin can hold
more). It fires automatically every `DbConfig::checkpoint_every` committed records
(default 4096; `0` disables the cadence, leaving manual `checkpoint` only). Once the log is
compacted past seq 0, a rejected store is no longer rebuildable from the log alone.
`Db::open` checks the log's base before discarding the store and returns a named hard error
instead, naming the compaction and the fix (restore from a replica/backup, or widen
`retain_segments`), so the records below the old checkpoint are not silently lost. When
the verdict is `UpgradeRequired`, the text says so and says what it means here. charpente
has only ever used redb 4.x, which writes format 3, so on a store the node created the
version byte is damage and the fix is the restore. Only a format-2 file written by
something else can be upgraded in place, with redb 2.6's `Database::upgrade()`, and
format 1 has no in-place upgrade. On a full log an `UpgradeRequired` store is rebuilt like
any other rejected one, which loses nothing.

A second sim-established redb finding (upstream-report material): direct corruption of the
store file's durable bytes, such as bit flips or misdirected writes, can defeat redb's
copy-on-write crash consistency entirely. When it does, redb either panics internally or
silently falls back to an older, still self-consistent root that under-reports
`applied_seq`. Store-file bitrot is not defended in v0 (the corruption fault axes are
scoped to `wal/`, §1): redb + M1 restore is the answer, and M2's own engine owns page
checksums.

**Store-handle poison (M2 sub-project 0a, implemented).** The store boundary now follows the
same fsyncgate rule as the log (§2 above, DESIGN.md:212-214): an un-retryable IO failure
poisons, and restart-and-recover is the only exit. redb has its own independent latch: any
error from its inner `StorageBackend` sets `CheckedBackend.io_failed` (five sites: `len`,
`read`, `set_len`, `sync_data` and `write`, at `cached_file.rs:222/231/240/249/258` in
redb 4.3.0), after which every further backend call returns `PreviousIo` ("close and
re-open the database"). That inner backend is charpente's `DiskBackend`, so charpente
observes the identical condition at its own disk boundary, one layer down: a sticky
`Arc<AtomicBool>`, created in `Store::open`, set in each of `DiskBackend`'s five methods on
`Err`, and exposed as `Store::failed()`. A graceful `close()` is not mirrored, because it
is not a failure. `Db::poisoned()` ORs it in via `Db::store_failed()`, so the kernel's
existing request-arm poison stop (the 2026-07-05 ruling, §7) fires with no new control
flow. Only the stop message differs: a store latch leaves the WAL intact and
authoritative, while apply-poison means durability became unknowable.

A test at the kernel level asserts the correspondence against a genuinely latched store,
and it takes two assertions to do it: one reads charpente's flag and one reads redb's own
refusal. A test that consulted only the mirror could not tell a correct latch from a
drifted one. Both assertions ride the checkpoint-fsync path, so they pin drift on that
path in either direction: charpente missing a latch redb took, or charpente latching where
redb no longer does. The second would poison healthy nodes into permanent unavailability,
and only the redb-side assertion catches it. A future redb that grows an entirely new
latch path, one `DiskBackend` never sees, is not caught mechanically. That stays a review
item against the five-site census above, to be re-walked on every redb upgrade. The 4.3.0
re-walk (2026-09-30) found the five sites unchanged and one new route that drifts the
other way. `CheckedBackend::write_best_effort` (`cached_file.rs:266`) calls the inner
`write` without latching, and the read path uses it to reclaim non-durable-commit pages
from redb's write buffer when buffer plus read cache exceed redb's cache budget
(`:698-712`), discarding the error. `DiskBackend` cannot tell that call from a latching
one and latches on its `Err`, so there charpente poisons where redb would carry on. That
is the conservative direction (a restart-and-recover, never a silent pass), and it is
reachable only past the 1 GiB cache budget, the DERIVED regime below. The new byte-range
lock methods keep redb's default `Unsupported` bodies: no IO, no latch.

Two shapes share the fix but not its evidentiary standing. On a cache-resident store
(MEASURED, under redb 4.1.0), the only reachable trigger is the store's own checkpoint
fsync. Store applies commit at `Durability::None`, so checkpoint is the only store sync
charpente asks for. redb 4.3.0 adds one charpente does not ask for: the fsync `grow()`
issues after extending the file inside an apply, which reaches the same latch but has not
been measured here. Before this fix the latch was completely silent, since reads kept
answering 200 off redb's page cache. The window closed only when the next write converted
the latch into apply-side poison, and on a node taking no further writes it never closed
at all. On a store larger than redb's 1 GiB evicting read cache (DERIVED, not measured),
cache-missing reads also reach the disk boundary and set the same flag. That covers the
shape where a read-first write transaction (the ordinary get-then-put handler) fails
inside the handler and never reaches the latch-gated `begin_write` (`begin_read` is not
latch-gated in either redb impl). Without the read arm such a node would 500 indefinitely
with `poisoned()` false. The larger-store shape is justified on redb's verified source,
not a measurement: by design, no store over the 1 GiB bound was built to prove the
symptom.

**Refused before the record exists (implemented).** Apply-poison is for failures the log
cannot take back, so a write the compiled `Schema` already says apply will refuse, or will
misread, is refused earlier. Apply treats every write to a table as a write of the type
the `Schema` registered for its name, and three kinds of write disagree with it: a table
name the `Schema` does not register at all; a write whose scope shape is not the
registered `Row`'s (a scoped `Row` whose `TABLE` is an unscoped table's, or the reverse);
and a put or delete through any Rust type other than the registered one, compared by
`TypeId` (in memory only: a `TypeId` is not stable across builds, so it never reaches the
log, a fingerprint or `describe`). The last is easy to reach, because a `#[table]` type's
`TABLE` is its bare struct name, so two `#[table] struct Invoice` in different modules
share one. They can share `VERSION` and `FINGERPRINT` too, since a fingerprint folds field
names and type spellings, not `scoped_by` and not what a spelling resolves to (§3).

Before this check, a write that apply could not read as the registered type was refused at
apply, after the WAL sync: an unregistered name (the `Schema` lookup), the other scope
shape (`resolve_effect_scope`), or a row that does not decode. By then the record was
durable, the engine poisoned, and every restart replayed it into the same refusal. A
write that apply could read was stored as the registered type's with no error anywhere. A
row whose bytes happened to decode, or a delete, through a same-named type with another
`scoped_by` wrote or deleted the registered scope type's tenant of the same id, and an
unscoped write beside a scoped one to the same scoped table went out under the table's one
tag and landed in tenant 0. `Group::stage` now refuses all of them by name before the
record exists (`TableWriteRefusal`), naming the table, the type that wrote and the
registered type, as it refuses an enqueue under a foreign job shape. A job handler's
transaction that hits it is an attempt, and a request gets a 500 with its text. Apply's
refusals stay for records from elsewhere: shipped, or written by another binary and
replayed under a `Schema` that disagrees.

Reads are held to the same type (spec 2026-09-30-tenancy-completion, amendment A18). A
scoped read keys on the table name and the raw scope id, so a read through a same-named
type with another `scoped_by` used to read the registered scope type's tenant of the same
id. A `Tx` read through a same-named `#[table(global)]` type used to read tenant 0's rows
staged earlier in its batch, since the overlays key an unscoped row as tenant 0. Each `Tx`
and `ScopedTx` read and `across_tenants` scan now compares its `Row` type with the
registered one first and refuses a mismatch by name (`TableReadRefusal`), returning no
row. In a job handler that is an attempt, as the write refusal is.

**Snapshots & reads.** MVCC snapshots at log positions. Read-only queries (and follower
reads) run against snapshots without touching the write path.

**Blobs (M2, not built).** Blob columns over a configurable size will spill to a
content-addressed blob store (same log-commit discipline for the metadata, direct
streaming for the bytes). Nothing in this paragraph exists today: there is no blob type,
no spill threshold and no streaming path, and a large value is simply bytes in a row.

**Logical export (v1, implemented).** A `Snapshot` also exposes the seed path's read
surface (M1 replication T4). `applied_seq`/`next_job_id` read META off the snapshot's own
transaction, so they are consistent with the rows it is about to hand out. That is unlike
`Store::applied_seq`, which opens a fresh one and so could observe a later write.
`scan_raw` hands back a table's raw `(scope, id, bytes)` triples, undecoded, in stored key
order (empty, not an error, for a table that was never created). `scope` is
`table::UNSCOPED` for every row of an unscoped table. A scoped table's physical redb table
is composite-`[u8;16]`-keyed, so `scan_raw` and `scan_raw_from` (the
migration/archive-export paged read) take `scoped: bool` from the caller's own
`TableOps.scoped` (the schema's word, never guessed by probing) and open that shape.
Scoping is declared per table (`scoped_by` or `global`, never inferred; spec
2026-09-16-default-secure-surfaces) and never retrofitted onto an existing one (the half
of the tenant-scoping spec's §8 non-goal that survives). A table's physical shape is fixed
at its first write for the table's whole life, so a `TableTypeMismatch` is always a
schema/disk disagreement, never a state to tolerate. Both directions are refused by name,
and neither is silently resolved. `scoped = true` over a physically bare table names the
scope-flip contract and its fix ("create a new scoped table and copy rows through a job").
`scoped = false` over a physically composite table names both known shapes. (An earlier
build of this fallback tolerated the `scoped = true` case, on the reasoning that
`migrate_table`'s resume walk depended on reading it. It does not. The next `MigratePut`
for such a row dies on redb's own unnamed type error the moment it tries to write the row
back under a scope, so the tolerance only ever bought a silent scope-0 collapse in the
three paths that complete through it: `Seeder::rows`, the archive export walk and
replication seeding.) `scan_raw_from`'s cursor is `(scope, id)`: a bare-id watermark
cannot resume a composite-keyed table correctly, since ids are not globally ascending
across a scope boundary (tenant 2's id 0 can follow tenant 1's id `u64::MAX`).
`Store::import`/`set_import_meta` are the write-side counterpart. `import` commits typed
rows through the same `Writer` (and so the same index maintenance) as ordinary `apply`,
but with no seq or `next_job_id` bookkeeping of its own, since a logical snapshot has no
log position. `set_import_meta` sets both meta keys explicitly, once,
after every table has landed.

## 5. Replication & deployment [reversible in mechanism, fixed in stance]

- **Mechanism (v1):** asynchronous log shipping to N followers. When the primary fails, a
  follower is promoted, by an external arbiter or manually in v1. Consensus-elected
  promotion can come later without changing the data plane, because the log format is the
  contract.
- **Stance (fixed):** one primary, no sharding, no multi-master. See MANIFESTO §7.
- **Deploy:** one binary and one data directory. An app's new binary replays and attaches to
  the same log across the app's own deploys (schema versions in-log make this well-defined).
  Across charpente minors there is no migration: a node refuses, by name, a data directory
  whose platform tables (`Session`, `ApiKey`, `CronRow`, fingerprinted like an app's) have
  another shape, a peer whose `Hello` declares another `PROTO_VERSION`, and an archive
  bucket whose manifest `format` is not `MANIFEST_FORMAT` (or is absent). The hand-written
  platform tables (`charpente.jobs`, `charpente.events`, `charpente.schema_versions`,
  `charpente.intents`) have no data-directory detector: a 0.2 directory is refused because
  `CronRow` (always recorded: the session-sweep cron) changed shape in the same release. A
  release that changes one of them must bump both constants and also give the directory a
  detector, either by gating the table (a named step) or by changing it with a
  fingerprinted platform table. New manifest fields go after `format`, so an older binary
  still reaches the refusal.
- **Backup (M2, built):** log segments plus a periodic snapshot to object storage,
  Litestream-style but built in, not an add-on. Two distinct things exist, and they stay
  distinct. `serve::backup` (`crates/charpente/src/serve.rs`) is a replication-only node
  with no HTTP edge that keeps its own durable copy and can be promoted: a hot standby, not
  an off-site snapshot shipper.
  The archive proper (`crates/charpente/src/archive/`) is the shipper. It has a generation
  model over `<prefix>/manifest`, a full logical snapshot plus the sealed WAL segments that
  continue it, a bounded retention pin that yields so that an object-store outage does not
  halt the primary, and a sixth kernel arm (§1) that drives it one step at a time. A
  snapshot batch object's bytes follow its table's own scopedness. An unscoped table's batch
  is unchanged from before tenant scoping (`[count][id, row]*[crc]`). A scoped table's batch
  leads with a `u32::MAX` count sentinel that no unscoped batch can ever produce
  (`SNAPSHOT_BATCH_ROWS` bounds a real count far below it), then the real count, then
  `(scope, id, row)` triples. The export walk carries each table's scopedness alongside its
  name, and restore decodes a batch against the destination schema's word for that table. A
  scoped table found unscoped in the archive (or the reverse) is refused by name, and the
  refusal names the fix. It is never silently imported under the wrong shape.
  `serve::archive(app, store, cfg)` is the archive-only edge, and `ArchiveConfig::writer` is
  the identity every manifest records. An archive that meets a manifest written under a
  different identity, at open or before any swap, stops permanently and reports it instead
  of interleaving two writers' generations into one prefix. This is detection, not
  prevention, so the identity has to be unique per node and also stable across a redeploy.
  Reading it back is `charpente::archive::restore`, behind the `restore` verb every
  `charpente::launch` binary carries (`charpente restore --dir` builds and execs it, as
  `describe` does). It rebuilds a data directory from the newest generation in a bucket, and
  refuses a destination it cannot prove is on that timeline (diverged, ahead or unrelated)
  without merging into it. `--force` names the records it is about to destroy before it
  acts.
  A generation that rolls reclaims its predecessor's objects by name, from the manifest and
  snapshot metadata this writer wrote, and only after the successor's manifest is durable.
  The bucket is never listed, so an object no manifest ever named is unreachable by design
  and is the operator's to expire (GUIDE §15 states the required lifecycle rule). The
  archiving standby is still not built. `Kernel::build` takes a `Repl` and an `Archive`
  together but is private, and every public constructor passes at least one of them `None`.
  So the deployment most operators should choose, archiving from the standby and leaving the
  primary's loop untouched, has no public constructor yet.

**Seeding (v1, implemented).** `Db::into_seeder`/`Seeder` builds a follower's (or a fresh
backup's) entire state from a primary's logical snapshot stream, with no log replay (M1
replication T4). `Seeder::new` first discards any store file and `wal/` segments already at
the target disk, dir-barriered: seeding always replaces a timeline and never merges into
one. `rows` applies one table's exported `(scope, id, bytes)` triples through the schema's
typed ops the same way the live apply path does, rebuilding indexes as a side effect, and
each row's own scope goes straight to the typed put (tenant-scoping Task 6c; before it,
`rows` refused a scoped table by name, since seed/restore export carried no per-row scope).
`finish` pins `applied_seq`/`next_job_id`, checkpoints, and lays a single empty WAL segment
at `applied_seq + 1` before handing the result through the ordinary `Db::open`. That is the
base = horizon + 1 boundary the recovery-horizon rule (§2) already accepts, so open sees a
clean, un-compacted follower with nothing to replay. `Schema` is `Clone` and gains
`table_names` (the export walk order both sides agree on) and a crate-private `remove`.
`Db::open` always adds `JobRow` to the schema it's given, so `Db::into_seeder` (the producer
side, which deconstructs a live `Db` into a `Seeder` over the same disk) strips it back off
before handing the schema to a fresh `Seeder`, which re-adds it itself. Without that,
seeding a live `Db`'s own state would register the platform's own table twice. The
network-facing seed session (shipping rows to a remote follower) is a later M1 task, and
this is the substrate it sends over the wire.

**The data plane (v1, implemented).** `charpente::repl::Repl` is the session engine (M1
replication T6). The primary opens a replication listener and runs one `Session` per inbound
conn, and a follower/backup runs one outbound `Follower` client against its primary. Both
are driven kernel-shaped. `pump_io` moves bytes without touching the `Db` (accept, drain
readable messages into per-conn queues, note reconnect timers), then `step` does one unit of
work against the `Db` per call (handle one message, push one `Frames`/snapshot batch to one
session round-robin, heartbeat, or reconnect). A session opens on its first message. A
`Hello` carries the proto version, the follower's own listen address and its position (last
durable seq plus that frame's CRC via `frame_crc`), and it runs this decision ladder, pinned
by tests:

- Version mismatch → `Refuse` naming both versions.
- A non-primary role → `Refuse` naming the real primary (no cascading).
- A follower at or ahead of the primary with unknown bytes, or a CRC mismatch on a retained
  frame → `Refuse` naming both seqs, both CRCs and the re-seed fix. Diverged timelines are
  refused, never merged.
- A positioned follower below the GC'd base → `Refuse`. This is diverged behind: the primary
  GC'd the frame at that seq and cannot prove the follower's records are a prefix of its
  timeline, so a re-seed could silently wipe a follower the primary forked below. The
  refusal names both remedies, re-seeding a true laggard or promoting the follower (Design
  B, `.superpowers/sdd/diverged-behind-investigation.md`).
- An empty (None-position) follower against a compacted log → snapshot-seed.
- Otherwise, `Frames` resume from position + 1.

The sim's above-horizon replication property
(`wal_corruption_on_the_primary_no_longer_loses_acked_records_with_a_follower`) meets that
diverged-behind refusal for a follower it rebuilt from the surviving primary. It answers
with the remedy the refusal names, a fresh-data-dir re-seed (`repl:laggard-reseeded`), and
only where the follower is provably a prefix of the primary. The proof comes from this
handshake, not from the rig's wipe. The rebuild converged only once the primary accepted the
follower's `Hello` (the prefix check at its position, or a snapshot seed of an empty
follower). Since then the follower has appended only shipped frames, and the primary never
recovered, so it never cut its log. The re-seed's wipe retries until it is whole and synced,
and a follower that still reopens non-empty fails by name at the property's freshness check,
before that handshake runs. A refusal inside a re-seed's own convergence gives up as
`repl:inconclusive:reseed-refused-behind`. Once the primary has recovered, a diverged-behind
refusal stays a failure. For the designated survivor it is a give-up of its own,
`repl:inconclusive:survivor-refused-behind`, because re-seeding the survivor would void the
promise it underwrites. A restore-needed refusal of either node is answered (restored, or
given up) only when a fault injected on that node's own disk explains it: durable corruption
for a below-horizon refusal, entry loss for a gapped log directory. Otherwise it fails as
the platform durability bug it would be.

Shipping reads only through `Db::read_log`, which re-verifies the CRC at read-back and caps
reads at `last_applied`, so a frame corrupted on the primary's disk is refused by name
instead of shipped, and the unsynced tail never travels. The follower appends via
`Db::apply_shipped` (append → sync → apply, recovery's own discipline) and sends
`Ack { durable_seq }` only after that sync, never on receipt. A shipped seq gap means a
different timeline: drop the conn and re-`Hello`. The real shipper never sends an empty
`Frames` batch (it only ships when it has something to ship), but the wire does not enforce
that. On the follower side an empty batch is a no-op: `apply_shipped` doesn't advance
`last_applied` on an empty batch, so there is nothing to ack.

The retention pin is wired at `Repl::open`. `Db::add_retention_pin` registers a pin that
reads a shared floor, recomputed on every ack and session change: the min over live sessions
of acked + 1 (a not-yet-acked session pins at its cursor, a seeding session at its
snapshot's `applied_seq + 1`). So a live lagging session holds GC for as long as it lives
and releases it when reaped. A session still in `AwaitHello` (accepted but silent) pins at 0
like any not-yet-acked session. `HELLO_DEADLINE_NANOS` (10× the heartbeat interval, checked
and reaped the same way as the heartbeat deadline) fires for a session that never sends its
first message, `Hello` or a control verb, so a peer that opens the replication port and
never speaks cannot hold GC at the base forever.

Seeding streams `SnapshotStart`/`SnapshotRows`/`SnapshotEnd` out of a single MVCC snapshot
(batches near `SNAPSHOT_BATCH_BYTES`, one message per step). A table's own scopedness picks
the row message. An unscoped table batches into
`SnapshotRows { table, rows: Vec<(u64, Vec<u8>)> }` (tag 5, unchanged since before tenant
scoping). A scoped table batches into
`ScopedSnapshotRows { table, rows: Vec<(u64, u64, Vec<u8>)> }` (tag 14), a new message
rather than a widened tag 5, so tag 5's bytes for an unscoped table stay exactly what an
older binary already expects. The receiving side checks the message against its own schema's
word for that table and refuses a mismatch by name instead of importing a row under the
wrong scope. `PROTO_VERSION` is 9. It bumps when a release changes the shape or the meaning
of anything a peer ships (0.3 changed no wire bytes; stored times moved to wall scale).
Tenant scoping's tag 14 made it 2, payload shapes' `0x08`/`0x09` record tags made it 3, job
tenancy's `0x0a` made it 4, event tenancy's `0x0b` made it 5, outbound effects'
`0x0c`/`0x0d` (and `JobRow`'s outbound key tail) made it 6, 0.3's wall-time platform columns
made it 7, 0.5's `0x0e` timed enqueue made it 8, and 0.7's platform table
`charpente.intents`, which seeding ships and a 0.6 schema does not register, made it 9. A
`Hello` declaring any other version is refused by name, with the fix (a fresh follower
running this release) in the text. Nothing is migrated.

The follower buffers the stream and, at `SnapshotEnd`, latches `db_needs_reseed`.
`Repl::step` cannot replace a `Db` it only borrows, so the caller (the kernel arm, T8) runs
`Repl::run_seed(db, clock)`, which consumes the `Db` through `into_seeder`, replays the
buffer, `finish`es at the horizon + 1 boundary, re-gates writes, and returns the new `Db`. A
conn death mid-stream never latches, and the next `Hello` restarts the seed from scratch,
which is idempotent because `Seeder::new` discards. In v1, seeding holds the full logical
snapshot in memory on both sides: the primary queues the whole stream when seeding begins,
and the follower buffers it until `SnapshotEnd`. That is right at M1 scale, and incremental
streaming is the tracked fix before large-store seeding.

`SnapshotStart` also carries the seed frontier's CRC: the primary's `frame_crc` of the
record at `applied_seq`, which the follower persists
(`seed_frontier_seq`/`seed_frontier_crc` meta). A freshly seeded follower holds none of that
record's WAL bytes (its own log starts empty at `applied_seq + 1`), so without the CRC its
`Hello` could only send `None`. A primary that had since lost its own tail above the horizon
would answer that empty position with a re-seed, silently wiping a converged survivor from
the now-lossy primary. With the CRC the follower Hellos `(applied_seq, crc)`, so the ladder
runs its diverged-ahead and resume checks instead. The fallback holds across a restart
before any frame ships, and fires only while `last_applied` still equals the seed's seq. The
CRC vouches for that one record alone, and a node whose log vanished after applying past the
seed sends `None` and re-seeds (finding-1 fix). A seeded-then-promoted primary that has to
seed others before its first commit chains the same stored frontier into its own
`SnapshotStart`.

The durable role file (§2's role semantics, T5) carries its own CRC line. A torn role write
is a named integrity error, never a parse of whatever bytes survived. The T6 harness caught
that case (seed 0x51dacce702b667c8): a torn "follower:p:1" that still parsed as a role
naming a ghost primary. The file is read the way recovery reads the log (§2, "readable is
not durable"): `load_role` barriers the file's name and syncs its bytes before it trusts
either, and every witness-slot read does the same (2026-10-01, D1 review I3). Before that, a
promote whose role fsync failed was still in the page cache after the restart its own error
asked for. The node booted as `Primary` from it and served writes, and a power loss then
reverted the file to `Follower` under a diverged log. A witness store that failed the same
way let a primary boot checking a set the next power loss shrank. A sync that fails at boot
fails the boot by name. The pins are in `repl/role.rs` and `repl/witnesses.rs`:
`a_role_a_restarted_node_boots_as_survives_the_next_power_loss`,
`a_witness_set_a_restarted_node_boots_with_survives_the_next_power_loss`, and for each the
name (`…_name_a_restarted_node_trusts_…`) and failed-sync
(`…_whose_sync_fails_at_boot_is_not_trusted`) twins. So a failed role or witness write has
an unknown outcome, and its error says so: the change may already have happened, because the
restart it asks for boots as whatever the file holds (2026-10-01;
`a_failed_role_write_says_the_flip_may_already_have_happened` and its witness twin).

A role file torn after first boot was torn by a flip whose write failed, so the node's data
is intact, and the torn-role text names an exit that keeps it: delete the file and restart
the node configured as a follower of the writable primary if another node is serving as one,
and as primary only if none is. The exit is probed and pinned for every flip that writes the
file (promote, both ends of a switchover, an abort, a restart resolving a hand-off;
`a_torn_role_file_restarted_as_its_text_says_keeps_its_data_and_one_writable_primary`), and
it brings the node back in that role with its data and leaves one writable primary. The rule
does not follow the flip's own target. A switchover's first flip, to handing-off, handed
nothing off, so a node restarted as handing-off stays write-stopped and no node takes
writes. A promote that tore while the operator promoted another follower instead, restarted
as the primary it was writing, is a second writable primary no witness gate sees: the
promote cleared its witness set first. Both are pinned beside the rule (S3b, R3-M1).

**The control plane (v1, implemented).** Admin verbs travel over the replication port as
control messages in the same protocol (M1 replication T7), and the sim drives them through
the same code path the CLI uses (same-shape rule). A connection is a control conn when its
first message is a verb (`CtrlPromote`/`CtrlSwitchover{to}`) rather than a `Hello`. A
control conn ships nothing and pins no retention. `Repl` itself, not the ship round-robin,
executes the verb and answers `CtrlOk`/`CtrlErr` on it, because every transition flips the
durable role file, re-gates the `Db` and re-wires the pin, all of it `Repl`-level work.

**Switchover** (`Primary → HandingOff(to) → Follower(to)`) is zero-loss and crash-safe on
the three-state role file. `store_role(HandingOff{to})` write-stops the primary
(`db.forbid_writes(to)`: reads and effect-free transactions still run, and effectful ones
get `TxError::NotPrimary`). Then the primary drains. It reads the target session's `Ack`s
each step, and when the acked seq reaches `db.last_applied()`, it sends `HandOff{at_seq}`
and enters HandedOff, the point of no return. A target session's `Hello` position, once
verified against this log, counts as acked. A target that reconnects already caught up is
shipped nothing by a write-stopped primary and never acks again, so without that rule
neither the drain nor the HandedOff resend (to a new session of `to`, once per session)
could fire (D4 round 2:
`a_switchover_target_that_restarts_before_taking_the_hand_off_is_sent_it_again`). Draining
is abortable: the `SWITCHOVER_DEADLINE_NANOS` deadline while still Draining returns durably
to `Primary`, resumes writes, and the `CtrlErr` names the follower's remaining lag.

HandedOff never aborts. On `HandOffDone` the primary flips durably to `Follower(to)`, drops
its ship sessions, and starts a follower client toward the new primary (drained to byte
equality, so no divergence is possible). On the deadline it stays write-stopped, and the
`CtrlErr` names the hand-off seq and both answers to "did `to` take it?" (2026-10-02, D4).
If `to`'s role file says primary, restarting this node resolves against it. If it still says
follower, no node accepts writes, and nothing resends after the reply. Either the `HandOff`
is still in flight (the drain and the hand-off share one deadline) and `to` may take it and
become the primary, which is the first case: a promote answers "already the primary", and
the `CtrlErr` says to just restart this node
(`a_hand_off_in_flight_at_the_deadline_can_land_after_the_stall_reply`). Or `to` was down
past the deadline (in the sim, its role write failed inside `accept_handoff` and its reboot
outlasted the window), and nothing changes on its own. Restarting this node alone leaves it
handing off. Promoting `to` loses nothing, since it confirmed it holds every record up to
the hand-off seq and this node has written nothing since, and a restart then rejoins this
node as its follower
(`a_switchover_stalled_on_a_never_promoted_target_says_so_and_its_remedy_loses_nothing`,
`restarting_a_stalled_primary_alone_does_not_resolve_a_never_promoted_target`).

The follower side of `HandOff` verifies `last_applied == at_seq` (else a named `Refuse`
protocol error), requires a configured listen address (an unpromotable follower is never a
switchover target: its `Hello` carries no listen, so it never matches), flips durably to
`Primary`, allows writes, replies `HandOffDone` on the old conn, and begins accepting
Hellos. A `HandOff` buffered in the same drain as the primary's crash-EOF is not stranded:
the follower client drains its inbox before any reconnect, and the primary drains the target
session's inbox before the dead-session reap.

**Promote** (`charpente promote`) is the bounded-loss failover. A follower/backup drops its
upstream client, flips durably to `Primary` and allows writes, and the `CtrlOk` names
`last_applied`. Records the dead primary acked but had not shipped are the DECISIONS §6
window: they are lost, and the loss is named. The window now covers external effects: a dead
primary can call out from a record it acked but never shipped, so the world saw an effect
the surviving timeline has no record of. Nothing short of synchronous replication closes
that, and DECISIONS §6 chose asynchronous replication. The minted key bounds the rest: an
enqueue that shipped without its Done mark is re-called with the same key, and one that
never shipped cannot collide with anything the new timeline mints. A re-issue from `on_dead`
copies the dead job's key with its request (name, payload bytes, app key), so a survivor
that re-dispatches the original and the old primary's re-issue call one request under one
key. Operator code holding a `Group` can build a Dead record, by `settle_job`'s standing. No
handler or job can.

A node that restarts in a durable `HandingOff(to)` stays write-stopped and resolves against
`to`. A probe `Hello` answered as a primary demotes it to `Follower(to)` (a diverged refusal
is the operator's re-seed signal, surfaced as the step error). If `to` is unreachable or not
yet primary, the node stays `HandingOff` on a reconnect cadence. A node in durable
`HandingOff` never silently resumes `Primary`, so no switchover interleaving yields two
writable primaries.

The promote path did not carry that property (the M1 sub-project 6 / T12 gap). A node
promoted away from (killed while `Primary`, its follower promoted underneath it) still has
`Primary` in its own durable role file, and the durable role always wins. So restarting it
with its original config used to resume it as a second writable primary with no notice at
all (neither node dials the other, so neither could observe the conflict). That left two
live, mutually invisible, diverged timelines, and the mandated fresh-data-dir re-seed then
destroyed every write the resurrected node had acked.

The re-seed itself carries the same hazard. The data directory holds the durable role file
and the witness set, so a node cleared for a re-seed has neither: config alone decides its
role, and a cleared node restarted with a primary config is a second writable primary that
no witness gate can see. A switchover-demoted former primary is such a node, since its role
file says follower while its startup config still says primary. Wiped and restarted as
configured, it acked a write beside the real primary (S5 re-review 2, R2-I1). So every
refusal and doc that prescribes a re-seed says, in the same sentence, to restart the cleared
node configured as a follower of the current primary, and what a primary config does instead
(`repl::role::reseed_steps`). So does every text that says "restore this node from a replica
or backup", since on a replicated node that restore is a re-seed. The core's refusals (the
recovery-horizon and read-back corruption refusals, the store refusals on a compacted or
empty log, the replay-shape refusals, the mistenanted-jobs refusal) all carry
`db::RESTORE_IS_A_RESEED`, and the torn-role-file text carries `reseed_steps` (S3b, R3-M1).
Nothing structural fences it: the guard is the operator's config.

**The boot gate** (M2 sub-project 0b, spec
`docs/superpowers/specs/2026-07-31-dual-primary-design.md`) closes that for the case the
node can see. A **witness** is a node this primary agreed to ship to. Witnesses' listen
addresses are recorded durably as each shipping session is established, in two CRC'd,
generation-stamped slots, so a torn write can never parse as a shorter (and therefore
weaker) set. The set is cleared on every durable flip toward `Primary`, before the flip
itself, so an interrupted promote is merely incomplete rather than a primary carrying a
stale set nothing will re-clear. At open, a durable `Primary` with a non-empty set does not
`allow_writes`. It enters the third write-gate state (`TxError::Verifying` → 503, §2a of the
spec) and probes every witness with one classify-and-disconnect `Hello`. It never uses
`Follower::new_resolving`, which would fall through to adopting the winner's timeline, the
move the 2026-07-16 diverged-BEHIND ruling rejected. When every witness answers "not a
primary", the gate opens. One answering as a primary refuses this node by name, naming both
remedies and demoting nothing. Unreachable or inconclusive witnesses keep the gate shut, and
the node serves reads throughout and produces one operator notice after
`WITNESS_DEADLINE_NANOS`. That is Noah's ruled option (c), which accepts the availability
cost because the alternative is silent acked-write loss.

`ReplConfig::strict_witnesses` (default `false`) overrides that last arm alone: past the
same deadline the node stops instead of serving reads it cannot write to. A strict node does
not refuse to start, because unreachability is only established by probing. It starts,
serves reads for the deadline window, and only then stops. The stop reaches a supervisor on
every node shape. Under `serve::backup` the error returns from the call, and under
`serve::serve_replicated` the kernel's stop ends the process through `Server::wait` (below)
with status 1, so a supervisor restarts it and keeps restarting it until a witness answers.

Don't read the check wider than it is: a probe answers "is this witness a primary", never
"does another primary exist". A node promoted after a switchover chain, never in this node's
witness set, is invisible to the check. The witness set bounds the whole defence, and the M2
backlog's Design C (a durable timeline epoch carried on `Hello`) would sharpen the refusal
rather than replace the probe.

**What a `Verifying` node may do as a peer** (M2 interlude, spec
`docs/superpowers/specs/2026-08-06-verifying-peer-policy-design.md`) narrows the one thing
the boot gate above left untouched. `begin_seeding` is read-only, so the write gate never
saw it, and a `Verifying` node remained a full replication peer: it accepted Hellos,
shipped, and seeded brand-new followers. Manufacturing a fresh durable replica of a timeline
this node has not confirmed it owns is acting as the primary in the most consequential way
short of acking a write, and unlike a write, that replica outlives this node's own fail-stop
once a witness finally answers "primary". The policy: a `Verifying` node may not render a
seed to a peer that holds nothing and is not one of its recorded witnesses. Everything else
it does as a peer (serving reads, refusing writes, accepting Hellos, shipping catch-up) is
unchanged, with one named carve-out already in force before this interlude: `switchover` is
refused while verifying (`mod.rs:966-987`), since `abort_switchover`'s `allow_writes` would
open the gate by a path `resolve_witness_gate` never sanctioned. The line between the two is
that catch-up ships a timeline the peer already holds a prefix of, while a seed creates a
fresh copy of an unverified one.

The keying is `db.verifying() && position.is_none() && !witnesses.contains(listen)`, with an
absent `listen` counting as not a witness (conservative: an unidentifiable peer is treated
as a newcomer). It is checked inside `decide_hello`'s shared `None`-position arm rather than
on the `Decision::Snapshot` variant. Both conjuncts are needed, and neither alone is
correct. `position.is_none()` does not mean "brand-new peer": `hello_position` returns
`None` for any node with `last_applied() == None`, including a recorded witness returning
with an empty data dir, so that peer must still be seeded. (That is a witness back from a
fresh-data-dir re-seed, which is GUIDE §10's own re-seed remedy and what resolves this
node's gate. A discard-and-rebuild is different: it replays the WAL and leaves
`last_applied` intact, so that node Hellos positioned instead.) Membership alone is not
enough either, since a positioned peer absent from the set is a genuine follower on this
timeline and must still catch up. Keying on the `Decision::Snapshot` variant instead would
be silently inert on a young primary that has not GC'd anything yet: its `None`-position arm
answers `Frames { from: 0 }` (a full replay that is functionally a seed), not
`Decision::Snapshot`.

The refusal is retryable, not node-fatal, at both places it can land. A newcomer's own
`follower.rs` client recognizes the marker at both its refuse-handling sites (the resolving
arm and the ordinary ship arm, separately fatal before this) and backs off to retry rather
than treating it as fatal. Separately, the witness probe's `classify` must resolve the
refusal correctly if it ever lands there. The refusal carries neither `NOT_A_PRIMARY_MARKER`
nor `DIVERGED_LABEL`, so `classify`'s existing default arm files it `Inconclusive` for free,
and either misclassification would be wrong: `NotPrimary` would let a probing peer open its
own gate on no evidence, and `IsPrimary` would refuse a healthy node over a phantom
split-brain. In practice a probe rarely reaches this. It Hellos with its own real position
rather than a synthetic `None`, so on any node that has ever applied a record it lands in
the positioned ladder, never this arm. Per Noah's ruling (2026-08-06), the refusal never
lapses while the gate is shut. Admitting a newcomer on a timer would make the guarantee
time-dependent instead of evidence-dependent, and the availability cost is near zero
regardless: a gated node already refuses writes, so the only use for a newly seeded follower
would be promotion, which is the harm the gate exists to prevent.

Recording witnesses while verifying is unchanged and safety-monotone. `probes` is built once
at `Repl::open` from the witness set on disk at that moment, so a witness recorded
mid-verification cannot affect the gate currently open. It can only make the next boot's
gate stricter, never looser. With the seed refusal in place, a newcomer cannot be recorded
at all (`listen` is assigned only on the two shipping arms, never on `Refuse`). The only
peer that can newly join the set while verifying is a positioned follower this node had not
yet recorded, which is a genuine follower relationship worth remembering.

**Surfaces (v1, implemented).** The kernel wires the data and control planes in (M1
replication T8). `Kernel::with_replication(app, clock, repl)` runs the `Repl` engine as the
fourth ready source (§1), and `run`/`step` are split so the park belongs to the driver.
Three edges in `charpente::serve` expose it.
`serve_replicated(addr, app, repl_disk, repl_cfg)` mirrors `serve`, except that the kernel
thread opens `Repl` (durable role on `repl_disk`, wire on `ProdNet`) and runs
`with_replication`. A follower answers writes with 421 and reads from its replicated copy,
and a primary ships. A startup role/config mismatch prints to stderr, and config never
overrides the durable role. `backup(app, repl_disk, repl_cfg)` runs the same kernel on the
current thread against an `EmptySource` (no HTTP edge, no requests): a node that only
follows and keeps a promotable copy. `archive(app, store, cfg)` (M2) runs against the same
`EmptySource` with `Kernel::with_archive(app, clock, archive, store)` instead. That
constructor takes four arguments because `Archive::step` takes the store on every call and
the `Archive` never holds it, so the `Kernel` keeps the pair. The archive edge defaults
nothing about `cfg.writer`: the manifest's ownership comparison is identity alone, so a
writer derived from anything a redeploy moves would make an ordinary redeployment a
permanent refusal on a bucket the node genuinely owns. The reseed swap that `Repl::run_seed`
needs lands in `App`. The `Db` sits behind an `Option` so the `Ready::Repl` arm can `take`
it, hand it to `run_seed`, and `put` the rebuilt one back, with the three steps adjacent so
no other code observes the empty slot.

**A stopped kernel ends the process.** `serve`/`serve_replicated` run the kernel on a joined
thread. `Server::wait` returns its exit code, and `launch` exits with it. The stop line is
the first line of the kernel's final stderr output (a multi-type foreign-shape jobs refusal
continues one job type per line after it), and it names the stop:
`refused to start`/`stopped on a refusal` for a `Refusal` (table-gate barrier, witness
conflict, strict-witness deadline, foreign-shape jobs, an unmarked replication refusal, a
listen-less switchover target), and
`failed to start`/`stopped on a platform IO failure — restart to recover` otherwise.
Restarting the same binary does not by itself clear a refusal. Each refusal names what must
change first (a witness answering, a fresh data dir, a registered migration step), while a
platform IO failure can clear on retry. A fifth line is neither: v0 has no admin-triggered
shutdown, so `Kernel::run` returning `Ok(())` (`Wake::Shutdown`) under
`serve`/`serve_replicated` only means the HTTP edge thread, the request channel's one
sender, ended first (most likely a panic). It prints
`charpente kernel stopped: the request channel closed, which only happens when the HTTP edge thread ended before the kernel did (most likely a panic) — check the edge thread's own panic output above; this is not a clean shutdown, v0 has none`,
with exit code 1. A live kernel with no edge left to feed it answers nothing, so this stop
is named and non-zero, not a clean-exit `0` that a `Restart=on-failure` supervisor would
take as "nothing to do here." The HTTP bind runs before the kernel thread is spawned, so a
bind failure is `serve`'s own `Err`, with no kernel left running behind it, and `launch`
names it (`charpente failed to start — could not bind <addr>: ...`, GUIDE §13).

## 6. Queue, cron, realtime: all faces of the log

- **Queue.** `tx.enqueue(job)`/`enqueue_keyed(key, job)` writes the job into the committing
  log record, and apply materializes a `JobRow` (§4). Post-commit, the kernel dispatcher runs
  due jobs inline on the kernel thread: a `global` job's handler is `fn run(self, tx: &mut
  Tx) -> Result<(), JobError>`, itself a transaction, and a `scoped_by = Org` job's takes
  `&mut ScopedTx<Org>` instead. The enqueue records the tenant (record tag `0x0a`,
  `JobRow.scope`), and the dispatcher reopens exactly that tenant with its own
  `TenantAdmission` through a `JobScope` only it constructs, so a scoped job runs where it
  was enqueued, retries included. The row is the platform's to write: its Done mark and
  failure record go through `Tx::settle_job` with that same admission, and app code
  cannot write a `JobRow` at all (§3's Jobs bullet). `Ok` commits its effects and the job's
  Done-mark atomically in one record, so its state effects are exactly-once by
  construction. `Err` aborts that transaction, and a second one commits the failure record:
  attempts, doubling backoff, Dead at the policy's `max`, last_error carried. The retry
  policy is a typed `RetryPolicy` on the `#[job]` item, and handler panics are absorbed into
  the same path. An `on_dead` job's hook runs inside the failure record that marks it Dead,
  so its effects and the Dead mark are one record. A hook that fails (an App error, a panic,
  a refused transaction) is discarded, and a third transaction commits the Dead mark alone,
  its `last_error` beginning `HOOK_FAILED` and carrying both errors. An app's own error text
  that begins with that prefix is stored behind `job error: `, so the prefix means a hook
  failed. A hook's other `JobError::Io` commits nothing and is not an attempt (spec
  2026-11-18-0.5.0 §5 C). External effects need at-least-once execution plus
  idempotency-key observability, and outbound jobs give them both. A job declared
  `outbound = "<host>"` implements `call` (read-only, by type) and `settle` instead of
  `run`, and the kernel makes the request between them, outside any transaction, through
  the `Outbound` IO trait. The idempotency key is minted from `Entropy` at staging (a
  re-issue's is copied from its Dead record) and recorded in the enqueue effect, so a
  retry, a restart and a follower's replay all carry the same one. GUIDE §6 is the
  app-facing account. Every enqueue records the payload type's structural fingerprint
  (§3). A scoped job's fingerprint also carries its tenant type's `NAME`, so moving a job
  between `global` and `scoped_by`, or to a tenant type of another `NAME`, is refused like
  a change of fields. Respelling its path to a type of the same `NAME` is not (§3's Jobs
  bullet). The kernel refuses to dispatch while a Pending job of a mounted type carries
  another shape, naming the job, its ids and both shapes. It checks at startup and wherever
  its write gate opens, and `promote` checks before its durable flip. A switchover is
  never refused at the hand-off, which is the primary's point of no return (§5). Its target
  stops at the write-gate flip instead, where the refusal's fix (run the binary that
  enqueued them until they are Done or Dead) still works. The binary never writes such a
  row itself: the job-shape registry is filled at `App::mount_jobs`, and an enqueue under
  another shape of a mounted job name (two `#[job]` types sharing one) refuses its whole
  transaction by name, before any record exists. Dispatch asserts the invariant. The
  platform's own `SessionSweep` job's fields change only between charpente minors, in
  place (DECISIONS §15), and `Session`, `ApiKey` and `CronRow` change with them. A node
  refuses a data directory whose platform tables have another shape, so a Pending sweep
  written under another release's fields is refused with its directory before it can meet
  this refusal, whose fix (restore the old fields) an app author could not apply to it.
- **Cron.** Schedules are rows. A due tick is one transaction that advances `next_due` and
  enqueues through the normal path, which makes cron a recurring enqueuer that inherits
  retry and observability for free. That enqueue runs on a bare `Tx`, so a schedule takes
  `global` jobs only, and a sweep that acts per tenant is a `global, cross_tenant` job that
  fans out with `enqueue_in`. A tick advances `next_due` to the schedule's next grid slot
  (`next_due + interval * ((now - next_due) / interval + 1)`), so lateness never drifts its
  phase. Catch-up fires once per wake and never backfills. In simulation, virtual time
  makes "run this monthly job 120 times" a subsecond test.
- **Realtime.** `tx.emit(event)`/`tx.emit_keyed(key, event)` writes a typed event into the
  committing log record alongside the transaction's other effects. The `key` is an opaque
  app filter (a user's subject, a room): it is carried but never interpreted inside the
  deterministic boundary, and it is never a tenant. Inside a tenant scope the tenant is not
  the app's to remember. `ScopedTx::emit`/`emit_keyed` record the scope's tenant in the
  event's own field (record tag `0x0b`, §4), apart from the key, so tenancy has one
  mechanism rather than two that can disagree. A tenant feed is built from the same scope
  (`tx.subscription::<Ev>()`, below), and one predicate, `Subscription::admits` (a name the
  subscription carries, exactly its tenancy, a key it passes), decides delivery. A tenant's
  feed never receives another tenant's event or a global one, and a global subscription
  never receives a tenant's. Resume validation asks the same predicate before it refuses a
  reshaped cursor, and checks tenancy on its own first: a cursor on a row whose recorded
  tenant is not the subscription's is refused whatever the row's name and key (amendment
  A16), because no feed of this tenancy delivered it. Apply materializes each event into
  the built-in `charpente.events` table
  (`EventRow { id, seq, name, key, payload, shape, scope }`). Its `id` comes from a
  `next_event_id` meta-counter bumped in call order, the same pattern as `next_job_id`, so
  ids are strictly monotonic, order-isomorphic to log position, and byte-identical on
  followers.
  Retention is count-based: `DbConfig::event_retention` (default 65,536) bounds the table,
  which is garbage-collected at the existing checkpoint cadence, and `Db::event_floor()`
  reports the smallest id still retained. Both read the oldest row's key and never decode a
  row, so a corrupt row at the floor fails only a tail whose scan reaches it, and GC
  collects it like any other.
- **The cursor is the event id, wire-encoded with its committing record's seq: `<id>.<seq>`.**
  Resume from cursor `(c, s)` delivers every matching event with `id > c`, which is exact by
  construction, since ids are minted at apply in log order. The seq half exists only for
  failover. Two diverged timelines (an old primary's unshipped tail and a promoted
  follower's new writes) can mint the same event id for two different events, and a bare id
  cannot detect that. Resume validation reads the retained row at `id == c` and requires
  its `seq == s`. A mismatch is a named divergence, never a silent misattribution, and has
  the same shape as replication's diverged-`Hello` refusal (§5). A cursor on a row of
  another tenancy than the subscription's is refused by name too: a cursor resumes only the
  feed that delivered it.
- **The subscription surface.** `#[route(SSE "/path")]` is a route flavor whose handler is
  an ordinary transaction (session auth via `Ctx` like any route) returning a typed
  `Subscription`. `Subscription::of::<Ev>().and::<Ev2>()` names event types compile-checked
  (never as strings in app code), `.key(...)` is an optional exact-match app filter, and
  `.cursor(cursor)` takes the platform-extracted resume point, which the handler passes
  through or clamps. Access control is the handler's own session/row logic, not the
  platform's. A tenant's feed is `tx.subscription::<Ev>()` on its `ScopedTx` (the
  `Subscribe` trait): a `TenantFeed<S>` carrying the tenant, which no app code can set or
  read, converted with `.into()`. `Subscription::and` refuses by name to widen one after
  the conversion. The platform extracts the client's cursor before the handler runs: the
  `Last-Event-ID` header wins (browser-native SSE resume), else a `cursor` query
  parameter, else `None` ("from now"). An unparseable cursor is a 400 and the handler never
  runs. "From now" is the node's `next_event_id` at the moment it serves the subscribe
  (`validate_subscribe`), not when the client sent it. Every event committed before that
  point is behind the tail, and on a follower so is every event applied from the primary's
  log before it, including one that landed while the request was in flight. A subscribe
  ends its request batch, so on a primary a change that arrived ahead of it is behind the
  tail and one that arrived behind it is delivered, even at the same instant
  (`a_bare_subscribe_on_a_primary_starts_between_the_changes_that_arrived_around_it`).
  The stream does not name that point. The route may also declare the event types its
  stream carries, as in `#[route(SSE "/path", emits(InvoicePaid, InvoiceVoided))]`, and
  that declaration is what lets a generated client type `subscribe()` as a discriminated
  union of exactly those payloads instead of "anything". The declaration names Rust types,
  and the contract records both names each type has: `<Ev as Event>::NAME` (snake_case,
  what an SSE frame's `event:` field carries) and `<Ev as Wire>::WIRE_NAME` (the payload
  type's key into `Contract.types`, and the member a generated union writes), so each name
  has one source rather than a copy of `#[event]`'s snake_case rule. Neither is derived
  from the other, because that derivation does not exist: `#[event]`'s acronym rule maps
  `HTTPServer` to `http_server`, and nothing inverts it. The declaration is optional (an
  undeclared stream route carries an empty `emits`), and it is not a filter. Nothing
  enforces it at delivery, because delivery still runs through the encoder each
  `Subscription::of::<Ev>()` captured. An oracle holds the two together, since a declaration
  nothing verifies is decorative: `App::subscription_event_names` runs a stream route's
  subscribe glue and reports the names its `Subscription` actually holds, and a test
  compares the two sets (TS-codegen spec §5).
- **Kernel delivery is one path, no modes.** A validated subscribe registers a `Tail`
  (`{ subscription, sink, next_id }`) on the fifth ready source (§1). It becomes eligible
  the instant a commit advances `next_event_id` past it, never on a timer. One unit of work
  reads up to `TAIL_BATCH_EVENTS` rows from `next_id`, filters by name, tenancy and key
  (`Subscription::admits`), JSON-encodes matches through the encoder each
  `Subscription::of::<Ev>()` captured statically at the subscribe site, and pushes frames
  to a driver-supplied sink: a bounded channel drained by the hyper SSE writer in
  production, an in-memory buffer in sim. There is no runtime event registry, so
  "unregistered event at delivery" is inexpressible, not an error case. There is also no
  separate push-on-commit path and no live/catch-up distinction. A commit advances
  `next_event_id`, which makes tails ready, and "live" is just "zero behind." A sink that
  reports `Full` or `Closed` costs the tail its connection, deterministically and
  immediately. A slow or dead consumer never costs kernel progress, and the recovery
  mechanism is the client's cursor resume, not a server-side retry queue. Event payloads
  must be plain JSON: serde_json refuses a map key that does not serialize as a string, a
  number or a bool, and a non-finite float key (and writes a non-finite float value as
  `null`). An event that fails to encode, or whose stored bytes do not decode under its
  recorded shape, ends that subscriber's tail with a terminal `charpente.error` frame that
  carries no `id:`, so the client's cursor does not advance past it. A decode failure comes
  from corrupt bytes or from a change inside a type one of the event's fields holds, which
  the fingerprint cannot see, and the error text names both. No delivery path stops the
  kernel. Only the platform's own store failures do.
- **Honesty at the edges.** A cursor below the retention floor, or one naming an event id
  this node never minted (or minted with a different seq, the cross-timeline case), is
  refused at subscribe with a 409 naming the floor or the divergence detail and the fix:
  reconnect without a cursor, wait for the stream to open, then read current state from
  the same node, holding every frame that arrives until that read is installed and then
  applying the held frames and every later one over it in order, idempotently. The fix
  depends on its order in two places. If the client reads first, a change that lands after
  the read is served and before the subscribe is (a follower apply in flight included)
  reaches the client by neither route. And a client that installs the read when it arrives
  overwrites any change whose frame beat it, which nothing delivers again (`realtime.rs`,
  the `the_resync_remedy_*` tests: a change in each of the five windows around the two
  requests, on a follower and a primary, judged by what arrives and by the state the client
  ends in; 2026-10-02, D4). A tail that falls behind retention mid-stream gets the same
  facts in a terminal SSE frame, `event: charpente.gap`, and then its sink closes. A stored
  event whose recorded shape (§3) is not the subscribed type's is never decoded: the tail
  gets a terminal `event: charpente.reshaped` frame carrying that row's own `id.seq`, and a
  subscribe whose cursor sits on such a row (for a name the subscription carries and a key
  it passes) is refused with a 409 naming the event, both shapes and the same re-sync fix.
  Sink overflow itself sends nothing, since there is no one left to tell. The client that
  reconnects with `Last-Event-ID` resumes exactly. The seq half does not separate every
  cross-timeline pair, and the sentence above should not be read as claiming it does. A
  local recovery cut that returns the log to the same position lets the replacement record
  take the same seq, so two different events can occupy one `(id, seq)` and the divergence
  refusal cannot fire (seed 0x17004e6a572d737d, docs/LEDGER.md 2026-09-07: a misdirected
  write moved a WAL segment's first frame off offset 0, recovery cut there, and event id 0
  was re-minted at seq 1 with new content). DECISIONS §6's Design C (a durable timeline
  epoch) closes this gap. The M2 follow-up backlog carries it, now with a single-node
  instance as well as the failover one it was written for. Follower tails read the same
  materialized table, whose rows replication keeps byte-identical, though not the file they
  live in (§5, property 1), so a subscribe on a follower works with zero app-code changes.
  A failover honors the DECISIONS §6 bounded-loss window for events exactly as it does for
  state, and a cursor that crossed timelines gets the divergence refusal, never wrong
  events.

## 7. The agent interface

Charpente treats the coding agent as the primary developer persona:

- **Introspection: `charpente describe` (v0, implemented).** `App::describe()` is a pure
  walk of a mounted app's own registries: no disk writes beyond an ephemeral run's own temp
  store, no socket, no kernel loop. It returns a `Contract`. Each table entry carries name,
  version, structural fingerprint, indexes, migration chain, and whether it's `scoped`
  (`#[table(scoped_by = …)]`, carried straight from the registry's own `TableOps::scoped`,
  the same value `Db::table_scoped` already exposes to the archive/replication callers).
  Each route carries method, pattern, name, whether it declared `auth` (`false` is a
  declared `public`, never an omission), the scope a `public(Org)` route admits every
  caller to (`public_scope`, the tenant type's `NAME` or `null`), whether it declared
  `cross_tenant` (which is what turns "audit the cross-tenant reads" into a mechanical
  inventory), and whether it's an SSE stream. Since the TS-client-codegen work, routes
  also carry `params` (each path param's name and wire schema, `ParamInfo { name,
  schema: TypeSchema }`), `request` (the JSON body's schema, `None` for a bodyless route),
  `response` and `error` (the success and `ApiError` schemas every handler's `Result<T,
  ApiError>` carries), and `emits`: the events a stream route declares via
  `#[route(SSE …, emits(Ev, …))]`, each an `EmittedEvent { event, payload }` holding the
  SSE frame's name and the payload type's name, empty on every non-stream route and on a
  stream route that has not adopted the declaration. `events` lists every `#[event]` type
  registered with `mount_events(events![…])` as `EventInfo { name, payload, scoped_by }`,
  sorted by event name. An entry's `scoped_by` is the tenant type's `Tenant::NAME` for a
  `#[event(scoped_by = Org)]` event and `null` for a global one. An app key is a call-site
  filter, so the contract lists no keys. Each job carries name, retry policy, `scoped_by`
  (the `NAME` of the tenant type a job acts in, or `null`), `cross_tenant`, `outbound`
  (`null`, or `{ host, timeout_ms }` for a job declared `outbound = "<host>"`) and `dead`,
  the job's Dead stance, `"on_dead"` or `"retain_dead"`. Cron entries carry name and
  interval, and the contract also carries config. Each is serialized in registry order
  (routes/jobs/cron) or sorted-name order (tables by `Db::describe_tables`'s `BTreeMap`
  walk, events by name). `outbound_hosts` has one entry per declared `Host` with the names
  of its credential variables (never their values), the headers it injects and its request
  and reply caps, so "which outside services does this app talk to, and what must an
  operator set" is an inventory. Field order is part of the pinned shape. `Contract` also
  carries `types: Vec<TypeDef>`: the transitive closure of every named wire type that a
  route's params/request/response/error or a registered event's payload references, plus
  every event type a stream route declares with `emits(...)`. The route registers those
  itself, so a declared payload is in the closure even when the event is never mounted. It
  is a `TypeRegistry` walk, `BTreeMap`-sorted by wire name so generation stays deterministic
  (spec §4). An anonymous type (a primitive, `Vec<T>`, `Option<T>`) contributes no entry of
  its own, only whatever named type it wraps. `ApiError` is always one such entry. Its
  schema describes the fixed `{"error": "<message>"}` envelope that `Response::error`
  actually emits on the wire, not `ApiError`'s own Rust variant shape, which no client ever
  receives. `ApiError`'s `Wire` impl is hand-written, where `#[table]`/`#[route]` types
  otherwise get the derive. A `builtin: bool` on each table/job/cron entry marks the
  platform's own surface (`Session`, `ApiKey`, the internal job/event/schema-version tables,
  the session-sweep job and its cron entry) apart from the app's, keyed on each row's own
  `TABLE`/`NAME` const rather than a typed-in string list. `to_json`/`to_human` render it as
  pretty JSON or fixed-width text. `to_human`'s route line renders
  `params`/`request`/`response`/`error` too, via `Debug`, and `emits` as `event->payload`
  pairs, and it has an `events:` section of its own.
  Two entry points ship it. `charpente::launch(addr, data_dir, build)` is the standard app
  `fn main`, with argv dispatch owned by the platform instead of hand-rolled per app, so
  `describe` is free the moment an app adopts it. With no verb it still serves, as a
  hand-rolled `main` would, and `describe [--format json|human]` renders the contract to
  stdout and exits. The CLI verb is `charpente
  describe [--dir PATH] [--format json|human] [--bin NAME]`. It builds any app crate from
  the outside (`cargo build`, picking `--bin` or the crate's sole or package-named binary)
  and execs it through the same `describe` argv path. The app needs no code change, and
  Cargo's own build errors (missing `Cargo.toml`, a failing build) are forwarded verbatim.
  Both paths build the app against a fresh, throwaway temp directory that is created and
  removed around the run, never the app's configured `data_dir`, so a describe is always
  safe to run next to a live server's real data. Event types are enumerated, but only
  through the describe-time registry that `events![…]`/`mount_events` fills, never by
  walking emit sites. That walk is impossible for the same reason it always was: `tx.emit`
  and `Subscription::of::<Ev>()` run inside handler bodies, not anywhere an app-wide walk
  can observe (ruling 6, sub-project 6 spec). So the binding between a stream route and the
  events it carries is declared: `emits(Ev, …)` on the route fills
  `RouteInfo.emits: Vec<EmittedEvent>`, each element pairing the `Event::NAME` an SSE frame
  carries with the `Wire::WIRE_NAME` of its payload. The pair exists because the two
  consumers need different names and neither is computable from the other (`#[event]`'s
  acronym rule maps `HTTPServer` to `http_server`, so snake_case -> PascalCase is not a
  function). Both are read off the type at expansion time, where both are in hand. An oracle
  (`App::subscription_event_names`, §6) catches drift between that declaration and the
  handler's own `Subscription` by comparing the `event` half, which is the only half a
  `Subscription` holds. The contract goldens pin the `payload` half. The drift check is an
  oracle, not a rule the platform could enforce at delivery. An undeclared stream route
  still reports `stream: true` with an empty `emits`, and reading its handler is how you
  find out which of the app's events it delivers.
- **The generated TypeScript client (v0, implemented; `crates/charpente-tsgen`).**
  `charpente_tsgen::emit_client` is a pure `Contract -> client.ts` function. It writes
  branded id types and a declaration per named `Wire` schema in `types` (an `interface`
  for a struct, a `type` union for an enum), a discriminated `Result<T, E>` per route
  keyed off `response`/`error`, and, for a stream route's `emits(...)`, per-event
  dispatch: a `Map` from an SSE frame's `event:` name to that payload's revive function, or
  `null`. `charpente gen-client` writes the file. `charpente check-client` is the drift
  oracle CI runs: it regenerates the client and compares it against the committed file
  instead of trusting that someone re-ran the generator after a Rust change
  (docs/GUIDE.md §14). Transport is SSE only, and no WebSocket client ships.
- **`charpente new` (v0, implemented; `crates/charpente-cli/src/new.rs`).** The scaffold a
  stranger starts from has six files: the GUIDE §1 manifest byte for byte, hard rule 1's
  standard-library fence as the app's own `clippy.toml`, one table/route/job/event, a
  one-line `launch` main, and three sim properties in the give-up idiom, driven through
  `charpente_sim::drive`, the second of which is DEMO.md's outbox oracle. Its templates are
  compiled, linted under their own fence and run under the sim on every push
  (`tests/new.rs`), and refusals name the rule and the fix (docs/GUIDE.md §1).
- **Errors an agent can act on.** Compile-time and sim-failure output is structured for
  machine consumption: the exact contract violated, the file/line, the seed and fault
  schedule for sim failures, and the minimal repro command. Error quality is a feature
  with a test suite.
- **`charpente test --sim` (v0, implemented).** The agent's inner loop: seeded, parallel,
  deterministic. App crates write property workloads in `tests/sim.rs` (`harness = false`)
  and register them via `charpente_sim::sim_tests! { name require ["fault:crash", …], … }`.
  Workloads share the internal suite's signature, `fn(&mut Sim) -> Result<(), String>`, and
  `Err` and panics both fail a run. `charpente test --sim [--runs N] [--seed
  0x…] [--master-seed 0x…] [--filter S] [--jobs N] [--format human|json] [--inputs HEX]
  [--faults-off AXIS,…] [--dir PATH]`
  builds the app's `sim` target via `cargo test --no-run` and execs it directly. Build
  errors surface verbatim, because the compiler is already the oracle. In the batch model,
  one master seed derives per-run seeds in registry order. The master seed comes from OS
  entropy and is printed at startup, or it is pinned via `--master-seed` (or
  `CHARPENTE_SIM_MASTER_SEED` when the flag is absent) for byte-identical replay.
  `--jobs`-many workers race over an index counter, so scheduling changes nothing about the
  seed sequence or report order. `--seed 0x…` replays exactly one run and skips `require`
  coverage checks, since one run can't be expected to hit everything a batch would.
  `--inputs` and `--faults-off` replay it with given input choices and fault axes off. Each
  check's first failure in a batch is shrunk by replays, under a 500-replay budget, to
  smaller inputs and fewer fault axes that still fail it. Shrinking stops when no step of
  the shrink passes makes the run smaller (a local minimum, GUIDE §8) or when its budget
  runs out, which can stop it short of one, and the failure's repro replays exactly that
  run. The human report is in `cargo test`'s shape. It has one failure block per failing
  check (shrunk unless the variable below is set), the skipped runs' reasons counted, each
  with its first run's seed, and the names of the checks that draw no inputs. Then comes a
  line per check (`ok`, `FAILED` or `untested`, with how many runs tested it, over how many
  distinct inputs, and how many were skipped), directly above a result line and the command
  that reruns exactly the batch, so a tail of the report carries every check's counts beside
  the verdict (GUIDE §8 "Reading a batch"). Under `CHARPENTE_SIM_NO_SHRINK`, which
  `charpente test --mutants` sets because a mutant needs only red or green, the blocks are
  unshrunk, each `shrunk:` line says it was skipped and why, and the rerun command starts by
  setting the variable. Under `CHARPENTE_SIM_FIRST_RUNS=N`, with N below `--runs`, the batch
  is a quick pass. It derives every check's seeds for the full count, runs the first N of
  each (a prefix of the full batch's, for every check), and judges failed runs only. It
  exits 1 for a failed run and for nothing else, since `require` and the untested rule
  depend on the run count. Its rerun command sets the variable, and its last line says what
  it left to the full count. Both variables are read in `sim_tests!` beside
  `CHARPENTE_SIM_NO_SHRINK`, and a `--seed` replay ignores them. A failure message is itself
  a reproduction (`--seed 0x9f3a...`), so the agent's fix-verify cycle needs no flaky-test
  triage. `--format json` is the machine-readable surface and carries every fact the human
  report does: `runs` and run totals, `coverage`, `missing_coverage` (`{check, key}`
  pairs for `require`d keys never hit), per-check `{check, status, passed, failed, skipped,
  distinct_inputs, drew_inputs, skipped_reasons}` with every skipped reason and its first
  run's seed, every
  failure as `{check, seed, config, fired, message, inputs, faults_off, first_message,
  not_shrunk, replays, repro}`, and last the batch's `result` and `exit`, followed in a
  quick pass only by `quick_pass` (`{first_runs, full_runs}`) (GUIDE §8). The verb ships
  with the public checker library, `charpente_sim::checks`. `is_fault_fallout` is the
  injected/poisoned/redb-latch fallout classifier, productized out of four hand-rolled
  internal copies. A durable-horizon refusal is never fallout by its text and is judged by
  `durable_refusal`, which excuses it only where durable corruption fired.
  `is_fault_fallout`'s typed form, `classify` → `FaultReason`, still names the
  durable-horizon family, for a workload that accepts some families and fails on others
  (sub-project 6 T7: first-match arm order decides the result and is pinned).
  `replays_identically` is the determinism oracle, and `acked_implies_durable` is the
  request-level ack-durability property, generalized for app workloads. There are three
  outbound checkers. `calls_have_durable_jobs` checks that every call that reached a
  remote carries the key of a job in the recovered store, so a call never fires for an
  enqueue that is not durable. `keys_are_per_effect` checks that a key carries exactly one
  request (equal names and payload bytes, at most one row not Dead), so only a re-issue
  from `on_dead` may copy one. `outbound_settles_by_horizon` checks that after the tail,
  every outbound job due at the wall time it judges at is Done or Dead. A row with no
  attempt made and a later due time is not yet due, and is not judged.
  `dead_hooks_succeeded` checks that no Dead row's `last_error` carries the prefix only a
  failed `on_dead` hook's Dead mark writes. `mark_dead_coverage` marks `dead:<job>` once
  per Dead row, so a sim can `require:` that a job's Dead path ran. The two
  fire-time-marked axis companions, `durable_corruption_fired` and `dir_entry_loss_fired`,
  excuse a shortfall only in universes where that fault actually fired.
  `SimHost::durable_corruption_fired` and `SimHost::dir_entry_loss_fired` are the same
  two scoped to a host, for a multi-node workload whose excuse must name the node the
  fault fired on. `SimHost::durable_corruption_count`/`dir_entry_loss_count` are their
  counts, so a workload that wipes a host's data reads them at the wipe and excuses a later
  refusal only by a fault since then. `charpente_sim::drive`'s `Drive::recover` tail is
  free of per-call faults, while a drawn provider outage runs to its end. The tail runs on
  past its horizon until every outbound job is Done, Dead or not yet due at the greater of
  the clock's reading and the kernel's held wall. Each extension runs as far as the
  largest, over the rows still due, of this sum: the longer of the row's wait (to its due
  time, or none once it is due) and the drawn outage's remaining length, plus its job's
  declared timeout, plus 1 s. That way a retry's wait or an outage costs a wake-up.
  `TAIL_SETTLE_CAP` (256) extensions with work still due fail the run by name.
  `Drive::recover` calls `mark_dead_coverage` last (0.5.0 spec §5 E). Exit code 0 is
  green. Exit code 1 means sim failures, unmet coverage, or a check that tested nothing
  (every run skipped: a vacuous check fails the batch, and a `--seed` replay of a skipped
  run exits 0). Exit code 2 is a build/usage/infrastructure error. The platform's own
  runners hold the same rule: a test in which every universe gave up fails.
- **The edge harness (v0, implemented; `crates/edge-harness`).** The sim owns everything
  below the IO traits. The edge harness owns everything above them that only a real
  process can show. It drives real built binaries (`examples/invoicing/src/bin/node.rs`
  and the `charpente` CLI) over real TCP with process- and connection-granular faults:
  `kill -9`, SIGSTOP/SIGCONT, and an in-process proxy that severs live connections,
  refuses new ones, or delays forwarded bytes. It proves three wire-level guarantees end
  to end (M1 sub-project 6, T11 and T12). The first is acked-implies-durable across a kill
  and a restart against the same data directory. The second is zero loss over converged
  writes across `charpente promote`, with the non-converged tail reported separately,
  because that tail is DECISIONS §6's window and not a violation. The third is exact
  `Last-Event-ID` SSE resume across a mid-stream kill, judged by set-equality with the
  missing set so an interior skip cannot pass. Each oracle carries an anti-vacuity floor
  and a positive control: the ack-before-commit meta-test drives a deliberately wrong
  build (`crates/edge-harness/patches/`) and demands the oracle go red by name, so a green
  run cannot mean "the rig did nothing". The honesty contract (spec §4.4) bounds what this
  can claim. `kill -9` is not power loss (process death does not drop the OS page cache),
  so what these oracles prove is ack ordering (nothing crosses the wire as acked before
  the commit path ran) plus end-to-end crash recovery through real hyper/TCP. Power-loss
  and torn-write durability remain the simulator's claim alone, and the crate's own docs
  say so. There is no seed promise here: a scenario's reproduction is the script file it
  records (every fault action and observation, in order, dumped on drop and named loudly
  on failure), replayed by hand. The sim stays the reproduction authority. A finding this
  rig makes that indicts the kernel or core gets reproduced in sim, with a pinned seed,
  before it is fixed (hard rule 3, extended past the sim's boundary). Something only the
  edge can show, such as a real signal or a real socket close, lands a deterministic edge
  regression instead. Because each scenario builds release binaries and drives real
  processes, every test that touches a process, a socket or the filesystem is `#[ignore]`
  and the matrix runs on demand via `scripts/edge-harness.sh`. The crate's zero-syscall
  tests (parsers, classifiers, and the byte-exact pins for every verdict string an
  operator would read) are not ignored and run in the ordinary sweep.
- **Skill/docs layout.** Documentation ships as both human prose and a machine-readable
  pack designed to be loaded into an agent's context (the framework's conventions are only
  worth having if the agent applying them has them in scope).

## 8. Auth & multi-tenancy [auth M1; multi-tenancy M2]

Sessions and API keys are built-in typed tables inside the commit point. A pre-transaction
resolution ladder turns a presented credential into one `Principal`, and `#[route(…, auth)]`
gates on it. A route that declares neither `auth` nor `public` does not compile, so an open
route is a declaration, never an omission (default-secure, 2026-09-16). It is deliberately
boring, and its value is that credential state is a transaction like everything else
(durable, replicated, recovered on restart), not a bolted-on side store. Everything here is
pure computation plus the kernel's one `Clock` reading per request, so it replays
byte-identically in sim. The crypto is RustCrypto (`sha2`, `rsa`, `base64ct`), never
hand-rolled, and it lives in `charpente`, never inside the deterministic `charpente-core`.

- **Hashed at rest.** `Session { id, token_hash #[index], subject, expires_at }` and
  `ApiKey { id, key_hash #[index], subject, created_at }` register platform-side in
  `App::open` (alongside `CronRow`), so recovery replays logins and minted keys with no app
  code. The stored `*_hash` is lowercase-hex SHA-256 of the raw credential. The raw
  token/key exists only in the mint response and never reaches disk. Hashing happens
  before the effects record is built, so the WAL carries the hash too. The flagship
  negative-space sim property is what mechanically forbids a regression: it scans every
  durable file in the universe under the full fault matrix and asserts that no raw
  credential byte-string appears in any store, WAL segment, or role file. Its naive-victim
  meta-test (a resolver that stores the raw token) fails that oracle by name.
  `create_session(tx, subject)` sets `expires_at = now + session_ttl` (`now` is the
  kernel's `Clock::wall` reading, `expires_at` is a `WallTime`, and the TTL is an
  `AppConfig` field, default 30 days in nanos). `create_api_key(tx, subject)` returns
  `"ck_"` + 64 hex, and the prefix makes a leaked key grep-able for secret scanners.
  Revocation is `tx.delete::<ApiKey>(id)`, an ordinary write, durable and replicated like
  any other.
- **One entropy draw, one mint per request.** Both credential kinds are fed by the single
  existing pre-transaction `fresh_token` draw. That adds zero new draws, so every pinned
  seed's entropy interleaving is structurally untouched (the frozen replay contract holds by
  construction, not by re-verification). A shared once-assert enforces the consequence: one
  credential mint per request, a session or a key, never both. A second mint panics
  (`Ctx::create_session/create_api_key called twice in one request — one credential mint per
  request`). Handlers cannot draw entropy (`Tx` has no `Entropy` by design), so one mint
  per request follows from the design and is not a limitation to route around.
- **The resolution ladder (pre-transaction, pure).** One entry point replaces the old
  session-only lookup, producing `Option<Principal { subject, via }>` with
  `enum Via { Session(id), ApiKey(id), Oidc }`. (1) An `Authorization: Bearer <t>` whose
  token contains exactly two `.` is judged as a JWT: OIDC-verified if a verifier is
  mounted, else a named unrecognized-credential 401. There is no fall-through: a
  JWT-shaped credential is judged as a JWT. (2) Any other bearer is SHA-256'd and
  exact-matched against the `ApiKey` index, then the `Session` index (expiry-checked
  against `now`). (3) A `charpente_session` cookie takes the same hash path, and only the
  hash path: the two-dot JWT rung is scoped to the Authorization header, so a cookie value
  that happens to contain two dots is never OIDC-verified (a ruling, pinned by test). (4)
  Nothing presented is `principal = None`. `#[route(…, auth)]` answers the no-credential
  401 when the principal is absent. `Ctx::principal()` is the surface, and
  `Ctx::session()` survives as a compat accessor returning the row only for a `Session`
  principal. The ladder runs unconditionally before the `auth` gate, so a garbage
  credential on a public route is still rejected. Lookups are hash-then-index-equality on
  SHA-256 outputs: an attacker controls no stored preimage and learns nothing exploitable
  from index-lookup timing. The one equality on secret-derived data that both sides
  control is the repl secret compare below, which is constant-time.
- **OIDC, static-JWKS only.** `App::with_oidc(OidcConfig { issuer, audience, jwks_json })`
  parses the JWKS once at mount. A bad key, an unsupported `kty`/`alg`, or a malformed
  document is a named mount-time panic (which key, what is wrong, the fix), never a
  request-time error. `verify` is pure over `(jwt, now)`: split the compact JWT (exactly
  three segments, else malformed), base64url-decode header/payload/signature, require
  header `alg == "RS256"` (v1 is RS256-only, and `kid` selects the JWK when present, else
  all configured keys are tried), check RSA-PKCS1-v1.5-SHA256 over the `header.payload`
  signing input, then check that `iss` string-equals issuer, `aud` (string or array)
  contains audience, `exp > now - skew` and `nbf <= now + skew`. The skew is
  `JWT_CLOCK_SKEW_NANOS` = `CLOCK_TOLERANCE`'s 60s, and JWT `exp`/`nbf` Unix seconds are
  converted to nanos and judged against `now`, the kernel's `Clock::wall` reading. Success
  yields an ephemeral `Principal { subject: sub, via: Oidc }`, with no row and nothing to
  revoke but a short `exp`. Each failed check is its own named 401 (below). Verification
  is ~100µs of pure computation on the kernel thread. The test suite checks in a fixed RSA
  keypair and mints tokens in-test with the same crate, so the claim matrix replays
  byte-identically, which proves the crypto path added no nondeterminism. A JWKS refresh is
  now expressible as a global outbound cron job; the platform still ships none. ES256
  joins when someone needs it.
- **Expiry: lazy resolution enforces it, the sweep only cleans up.** A `Session` row with
  `expires_at <= now` is treated as absent at resolution. It is invisible the instant
  `now` passes it, restart or not, because both are wall time (`WallTime`, `Clock::wall`),
  the same scale cron's `next_due` is stored on. A built-in daily cron
  (`charpente.session_sweep`, registered platform-side with no opt-in and running through
  the ordinary cron/job machinery, so it gets retry and observability for free) deletes
  expired rows in one transaction per tick so the table doesn't grow unbounded.
  Because lazy resolution is authoritative, the sweep's cadence doesn't affect
  correctness. The tick threads its `now` into an enqueued `SessionSweep { before }` job
  (the cron enqueue closure was widened to see the scheduler's `now`), which deletes rows
  with `expires_at <= before`.
- **Replication peer secret.** `ReplConfig.peer_secret: Option<String>` closes the
  anyone-can-connect hole on the replication port (the AwaitHello-pins-GC / empty-`Frames`
  class, already hardened, now access-controlled). When `Some`, every inbound `Hello` and
  single-message control conn (`CtrlPromote`/`CtrlSwitchover`) must present a matching
  secret. The secret is checked first, before any position/pin/seed logic, and a mismatch
  gets `Msg::Refuse` with the named text and a close, and no state is touched. The compare
  is constant-time over SHA-256 digests of both sides
  (`ct_eq_hex(hash_token(a), hash_token(b))`), never a raw `==`. `None` preserves today's
  open behavior. A node enforces only its own config: a `Some` node refuses unproven
  inbound, and a `None` node accepts everything and gracefully ignores a proof it never
  required (not an error). So a half-configured pair works in exactly one direction, which
  fixes the rollout order at followers first, then the primary (a ruling: configuring the
  primary first would lock every not-yet-configured follower out). The CLI verbs gain
  `--secret`, and a `Refuse` maps to the existing exit-1-with-forwarded-text path.
- **Threat model.** The replication wire is plaintext TCP. `peer_secret` authenticates
  peers but does not encrypt the link, so an eavesdropper on the path reads both the frames
  and the secret. The deployment assumption is a private network, and transport encryption
  is an M2 conversation. This is documented in DESIGN and GUIDE, never left to inference.
- **The named 401 family (product surface, all pinned by tests against text).** No
  credential on an `auth` route, unrecognized bearer/cookie, expired session, the seven
  OIDC per-check rejections (bad signature, `exp`, `nbf`, wrong issuer, wrong audience,
  unsupported `alg`, malformed), the mount-time OIDC config errors, and the repl refusal
  each name the specific failure and its fix. Precision wins over vagueness here because
  the leak profile is already acceptable: possession of an expired token proves past
  authorization, and JWT claims are attacker-visible by construction. The exact strings
  live in the code and are pinned by the ladder/OIDC/repl test matrices, and GUIDE §5
  quotes the set byte-for-byte in its error-index voice.
- **Multi-tenancy (shipped M2, 2026-09-04).** Org/tenant scoping is a first-class key
  prefix and a type-level property, not a review checklist item. The session-subject
  convention the realtime demo used is no longer the mechanism. The design is
  `docs/superpowers/specs/2026-08-30-tenant-scoping-design.md`, the app-facing walkthrough
  is GUIDE §16, and the typed surface is §3's `Scope` bullet.
  `#[table(scoped_by = Org)]` makes `(scope, id)` the row key and prefixes every index
  entry, and the scope-taking primitives are private, so a scoped row is reachable only
  through `tx.scope::<Org>(org, ctx)?`, which runs the app's own `Tenant::authorize` first
  and whose refusal is the app's own `ApiError`. A tenant is entered by membership (a
  principal, through `authorize`) or by declaration (`public(Org)` on the route, or the
  operator's `TenantAdmission`), never by an anonymous `None`. A caller with no credential
  at a scope line its route did not declare `public(Org)` for gets a named 401 (spec
  2026-09-27-job-tenancy §4). Its fix names the stance to change in place and restates no
  attribute, so a route's other flags survive it. Every declaration refusal names its fix
  the same way: the words to add or change, keeping the rest (spec
  2026-09-30-tenancy-completion §7). A job acts inside a tenant only by declaring
  `#[job(scoped_by = Org)]`, and then only inside the tenant its enqueuer opened or its
  fan-out named, because no app code can write the job row that carries that tenant and an
  enqueue in a tenant of any other scope type is refused (§3's Jobs bullet). A table that
  declares neither `scoped_by` nor `global` does not compile: which tables are tenant data
  is the app's declaration at every struct, never the platform's inference (default-secure,
  2026-09-16). Inference from field types was weighed and rejected, because the membership
  table must be `global` and must hold `Id<Org>`. The platform never derives a scope. The
  app names the tenant at that one line, or for a fan-out in `enqueue_in`'s argument, and
  nowhere else. A scoped job's record carries that name, and no key is ever parsed back
  into one. Realtime uses the same mechanism, not a second one: an emit inside a scope
  records the scope's tenant beside the event (§6), a tenant feed is built from the scope,
  and delivery compares the two exactly. `emit_keyed`'s key stays the app's own filter and
  is never a tenant. The composite key travels the whole way down: WAL effects,
  replication, the archive, `restore`, snapshot seeding and `migrate_schema`'s watermark
  all carry the scope, and a binary compiled against the unscoped shape of a scoped table
  refuses the record by name rather than misreading it.
  Enumerating every tenant is possible, and it is declared, never silent:
  `#[route(…, cross_tenant)]` / `#[job(…, cross_tenant)]` mint the `CrossTenant` token
  `Tx::across_tenants::<S>(cap)` requires (and `Tx::enqueue_in` requires for a fan-out
  into a named tenant), rows come back paired with their scope
  (`Vec<(Id<Org>, Invoice)>`, never `Vec<Invoice>`), and `charpente describe` lists every
  route and job that declared it (an inventory, not a grep) beside each route's
  `public_scope` and each job's `scoped_by`. That is a capability, not an escape hatch on
  a guarantee (DECISIONS §11).
  The operator acts inside one tenant with a `TenantAdmission` minted from a `Group`
  (`Tx::admit_scope`), sealed the way `CrossTenant` is. No `authorize` runs, because the
  operator has no principal and already holds the store (DECISIONS §12).
  The event log's shared-raw-id limit is closed by declaration (spec
  2026-09-30-tenancy-completion §6). An event names its tenant type, the emit and subscribe
  bounds check it, and a scoped event's fingerprint mixes the type's `Tenant::NAME`. Every
  typed emit and subscribe site also checks, in a `const` block, that the fingerprint is
  `scoped_fingerprint(PAYLOAD_FINGERPRINT, S::NAME)` for the `S` its bound names, or the
  payload fold alone for a global event, so a hand-written `impl Event` cannot copy another
  type's `NAME` and `FINGERPRINT` under its own `type Scope`. A raw id two tenant types
  share therefore never carries one's events into the other's feed. The check is static
  because delivery has no registry to check against at run time. A same-named event of the
  other type is a foreign shape there: it ends that tail (payload-shapes spec §3) and is
  never decoded. Two residuals sit outside the check. A `PAYLOAD_FINGERPRINT` chosen so its
  mix collides with another type's fingerprint (FNV-1a is computable backwards) is a bypass
  built on purpose in app code, which no type check refuses: it needs no `Db`, only a
  hand-written `impl Event` and the arithmetic. And a tenant type reached only through an
  event the app never mounts is outside the `NAME` census, so it can share a `NAME`, and
  therefore a shape, with another type: mount every event with `events![…]`.
  One limit comes with it, documented rather than engineered around. The guarantee's
  boundary is the typed surface handed to handlers and jobs. Whoever holds the `Db`
  reaches every tenant through `Db::snapshot`'s scoped and raw reads, which take a scope
  on the caller's word because replication seeding and the archive are built on them, and
  can mint a `CrossTenant` through `begin_group`, for the length of that group's borrow of
  the `Db`. A `Db` holder has the standing of whoever holds the data directory: an
  operator, not a request (DECISIONS §12, ratified 2026-09-05). The `App::mount` notch
  that would make even the operator path enumerable is recorded there as available and
  unbuilt, under §11's trigger.
  The platform's own tables are inside the boundary, not a limit on it: no app code reads
  or writes the job queue, the event log or the migration record (§3's Jobs bullet), so no
  handler or job reaches another tenant's jobs or events through them. The operator and
  the sim oracles read them through `Db::snapshot`. `Session` and `ApiKey` stay app-owned
  `#[table(global)]` tables (spec 2026-09-30-tenancy-completion §2): they carry no tenant
  data, and revocation is `tx.delete::<ApiKey>(id)` by design.
  Out of scope, and staying out: nested scopes, cross-tenant writes in one transaction,
  platform-derived tenancy, and forced retrofitting of existing tables. API-key
  scopes/permissions are subject-only in v1. This feature answers *which tenant*, never
  *what may this principal do*, and a permissions story earns its own design.

## 9. Open questions (tracked, not blocking)

1. **Hot reload of handlers** (cranelift-JIT route handlers for a sub-second dev loop):
   valuable and unproven, and M0 does not depend on it.
2. **Read concurrency model**: snapshot reads on worker threads, or everything through the
   core loop. Measure before choosing.
3. **Query combinator surface**: how far to go toward relational before its type
   complexity outweighs what it adds. The one concrete instance so far: `#[index]` is
   exact-match only, so a range or comparison query falls back to `scan` filtered in Rust.
   The overdue sweep in `examples/invoicing/src/jobs.rs` does this, and GUIDE documents it
   as the idiom under its "`#[index]` works on any `Codec` field" heading. That is the
   evidence DECISIONS §4's trigger asks for, and one instance across one small app is not
   enough to read it either way.
4. **Client codegen**: answered. `crates/charpente-tsgen` shipped 2026-08-29 and generates
   a TypeScript client from route/event types (§7, GUIDE §14).
5. **Compile latency mitigation**: platform as prebuilt dylib so app iteration recompiles
   app code only. Quantify the win early: compile latency is the adoption risk
   (see PLAN §Risks). It was quantified on 2026-09-09. The instrument is `bench-harness`'s
   `compile`/`compile_accounting` pair, which measures clean, handler-body and schema edits
   against `examples/invoicing`. It refused its whole first reading on 2026-09-07: six of
   the nine observations fell to a floor that turned out to be measuring wall time where it
   meant compiler CPU. An accepted reading followed on 2026-09-09 (docs/BENCH.md, "Compile
   latency (2026-09-09)"), and it answers which phase hurts. On one 12-core laptop with the
   `dev` profile, the expensive phase is the clean build, at 5.97 s wall and 37.6 s of
   compiler CPU. The inner loop an agent actually meets is 436 ms for a handler-body edit
   and 515 ms for a schema edit, only 1.18x apart. One reading on one laptop with one small
   app at n=3 is not a cross-platform claim. It also does not decompose those 436 ms into
   macro expansion, typecheck, codegen and link, so on its own it sizes no mitigation. The
   prebuilt-dylib item is dropped (Noah, 2026-09-09). Its stated mechanism, "app iteration
   recompiles app code only", is already what cargo does: every incremental observation in
   that run rebuilt exactly one crate, the app's. A dylib could still reach the clean build,
   but that is a CI-checkout cost, not the dev-loop risk this item was opened against.
   Reopening it on inner-loop grounds needs the decomposition that reading does not have:
   split the 436 ms into macro expansion, typecheck, codegen and link, and show link
   dominant.
