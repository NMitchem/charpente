# M2 — object-storage archive: design

## Context

PLAN.md:104-105 carries **object-storage backup** as M2 scope, deferred from M1: "log
segments + periodic snapshots off-site, Litestream-style." DESIGN §5 states the same and
names the collision this design has to work around:

> **Backup (M2, not built):** log segments + periodic snapshot to object storage,
> Litestream-style, built in — not an add-on. What exists today is `serve::backup`
> (`crates/charpente/src/serve.rs`): a replication-only node with no HTTP edge that keeps
> its own durable copy and can be promoted. That is a hot standby, not an off-site
> snapshot shipper — there is no object-storage code path anywhere in the tree.

Verified: there is none. Every `backup` hit in the tree is the hot standby, replication role
plumbing, test fixtures, or operator remedy text. No prior spec covers this.

**Recorded so it is not re-raised:** five core error messages tell operators to "restore from
a replica or backup" (`log.rs:186`, `log.rs:365`, `db.rs:1056`, `db.rs:1299`, `db.rs:1359`).
This session and a concurrent one both briefly mistook that for a hard-rule-5 defect naming a
fix that does not exist. It is not. "Backup" there means `serve::backup`, the promotable hot
standby, and the remedy is real: `replication.rs:358-360` reads the text and says so outright
("the path T7's promote/backup provides"), and GUIDE:2830 prescribes it ("not a local repair
but a restore: promote a healthy replica and re-seed"). **The five messages are correct as
written and this sub-project must not touch them.** When the archive ships,
restore-from-archive becomes an *additional* remedy they may name.

**The substrate already exists.** This design is mostly composition, not new machinery:

| need | what already provides it |
|---|---|
| hold WAL segments against GC | `Db::pin_retention` (db.rs:1650), a `Box<dyn Fn() -> u64>` floor — replication already uses it |
| read log frames, CRC-verified, never the unsynced tail | `Db::read_log` (db.rs:1731), capped at `last_applied` |
| logical snapshot of all tables, bounded memory | `Db::snapshot` (db.rs:1510) + `Snapshot::scan_raw_from` (table.rs:1043) — **not** `scan_raw`, see §10 |
| rebuild a Db from a logical snapshot | `Seeder` (db.rs:2381) — discards the target timeline, lays the horizon+1 boundary, hands off through ordinary `Db::open` |
| segment naming | `segment_name` (log.rs:504), `{dir}/{base_seq:020}.log` |
| whole-segment deletion with a retention floor | `Wal::gc` (log.rs:384) |

**Naming.** `archive`, not `backup` — `serve::backup` is the promotable hot standby and
keeps that meaning. Ruled 2026-08-20 (Noah).

## Scope

Four rulings fix the shape (2026-08-20, Noah):

1. **IO boundary:** a new `ObjectStore` capability trait in `charpente-core::io`, sim impl
   in `charpente-sim`, filesystem-backed prod impl in `charpente/src/prod.rs`. Not `Disk`
   reuse, not an S3 client over `Net`, not out-of-process.
2. **Restore is in scope**, to the newest recoverable point. Point-in-time deferred.
3. **On object-store outage the archive yields, never the primary.** The retention pin is
   bounded; past the bound GC proceeds and the archive records a named gap.
4. **v1 ships no S3 client.** Trait + sim + filesystem prod impl; the vendor client is a
   follow-on against an already-pinned trait contract.

## §1 The boundary

### Why this is a fifth trait, and why there is no lighter placement

This was first escalated as an amendment to **DESIGN §1, marked [IRREVERSIBLE]**. On
reading §1's text rather than its header, that framing was wrong, and the correction
matters more than the original claim did.

§1's irreversible **Decision** is about *sources*: "Every source of nondeterminism — disk,
network, time, randomness, task scheduling — passes through trait objects owned by the
runtime, with a production implementation and a simulation implementation." Object storage
is **network**, a source already named. `ObjectStore` adds no source; giving it a production
and a simulation implementation behind a trait is what §1 *requires*, not a departure from it.

Nor is there a closed four-trait list to breach. `io.rs` defines **seven** traits — `Clock`,
`Entropy`, `Disk`, `File`, `Net`, `Listener`, `Conn` — and §1's own Shape block shows five.
CLAUDE.md hard rule 1 names four of the seven, omitting `File`, `Listener` and `Conn`: it is
shorthand for the boundary, not an exhaustive contract, and was already incomplete before
this design existed.

What is genuinely at stake is narrower: §1's Shape block gains an illustrative entry, and the
capability surface grows by one. The real argument against is accretion, not constitutionality
— if every subsystem that talks off-box earns a trait, the boundary grows per feature. The
answer is that this one is forced rather than chosen:

**There is no lighter placement.** `crates/charpente-sim/Cargo.toml` declares exactly one
dependency: `charpente-core`. The simulator cannot see the `charpente` crate. A trait
defined in `charpente/src/archive/` is therefore unimplementable by the simulator, which
deletes the fault matrix and with it the reason to build the feature at all. Rescuing that
placement means making sim depend on `charpente`, dragging hyper/tokio/rsa into the
deterministic test crate — strictly worse.

**It is an instance of an existing shape, not a new category.** `charpente-core` *defines*
`Net` and never calls it: the only mention outside `io.rs` is the lib.rs doc comment, and
every consumer (`repl/`, `prod.rs`, `sim/net.rs`) sits outside core. So `io.rs` is already
the home for capability traits the *simulator* must substitute, independent of whether core
itself uses them. `ObjectStore` is the fifth entry of that shape.

Stated so it is not read wider: **the deterministic core takes on no new ambient IO.** Core
never calls `ObjectStore`. Only the archive arm in the `charpente` crate does, exactly as
only `repl/` calls `Net`.

### The trait

Poll-shaped, not async (DECISIONS §10), because the core loop is single-threaded and a
blocking 64 MiB upload would stall every request queued behind it.

```rust
pub enum ObjectOp {
    Put { key: String, data: Vec<u8> },
    Get { key: String },
    List { prefix: String },
    Delete { key: String },
}

pub enum ObjectDone {
    Put,
    Get(Vec<u8>),
    List(Vec<String>),
    Delete,
}

/// Off-box object storage. One operation in flight at a time.
///
/// `Send` for the same reason as `Disk`: `App` crosses into the kernel thread.
pub trait ObjectStore: Send {
    /// Begins `op`. Asserts nothing is already in flight — a second `start`
    /// before `poll` returns `Some` is a caller bug, not a runtime condition.
    fn start(&self, op: ObjectOp) -> io::Result<()>;

    /// `None` means still in flight.
    fn poll(&self) -> Option<io::Result<ObjectDone>>;
}
```

### The contract that distinguishes it from `Disk`

**A PUT is atomic. An object is never torn.** A failed PUT leaves the key absent or holding
its previous value — never half. This is the precise opposite of `File`'s documented
contract ("after a crash, unsynced writes may be applied in full, in part (torn), or not at
all, in any combination", io.rs), and it is why reusing `Disk` was rejected: fault-injecting
torn writes against a store that cannot tear would manufacture failures the real thing can
never produce, and the fault matrix would be testing a fiction.

There is no `sync_dir` analogue and none is needed — no directory entry to persist.
Ordering between distinct keys is **not** guaranteed; §2's write ordering depends on that
being explicit rather than assumed.

## §2 Bucket layout and the generation model

```
<prefix>/manifest                            — generation pointer, CRC'd
<prefix>/<gen>/snapshot/meta                 — applied_seq, next_job_id, table list, CRC
<prefix>/<gen>/snapshot/<table>/<n>          — batched raw (id, bytes), scan_raw_from
<prefix>/<gen>/segments/<base_seq:020>.log   — sealed WAL segments, verbatim
```

A **generation** is one full logical snapshot plus the chain of sealed segments continuing
it. `<gen>` is a monotonic counter, zero-padded, held in the manifest.

**Continuing it, precisely** (ruling 53). The chain starts at the base of the sealed segment
holding `applied_seq + 1` — the first record the snapshot does not carry — and never *below*
it. Every segment lying *wholly* below the frontier is the snapshot's: the archive releases it
and never ships it, which is what §3's "snapshots ... release the WAL below them" means in the
layout. (§3 is deliberate about the scope of that: it is WAL on disk, never objects in the
bucket, which nothing reclaims.) Three consequences worth stating because none is obvious:

- The chain's HEAD segment still carries the records between its own base and the frontier,
  which the snapshot has too. A frontier is wherever the `Db` had applied to, never a segment
  boundary, and segments ship verbatim rather than re-cut — so this residue is structural. It
  is bounded by one segment, and it is skippable by arithmetic rather than by search:
  `applied_seq` is the seq a restore replays the chain from.
- `applied_seq + 1` may sit above every sealed segment, in the live tail. The chain then
  starts at the tail's base, which is the base of the first segment that can ever seal above
  this snapshot, and the generation legitimately has an empty chain until it does.
- The chain therefore CAN start at the log's base, and that is not the defect: with nothing
  sealed at all the whole log is the live tail and its base is the log's base, where nothing
  lies wholly below the frontier to drop. What ruling 53 removes is a chain starting *below
  segments the snapshot already covers*, never the value `log_base` itself. The rule is
  "never below the frontier's own segment", not "never at zero" — a generation snapshotted
  before its first segment seals starts there by construction.

**Write ordering is the whole safety argument.** Upload every object of a generation, *then*
swap the manifest. Because inter-key ordering is not guaranteed, the manifest swap is the
one operation whose completion means anything: a crash at any earlier point leaves an older
*complete* generation still pointed at, never a torn one. The manifest is the only mutable
key in the layout.

**Stated so it is not read as violating the one-commit-point rule:** the manifest swap is
the commit point *of the archive*, not of the database. Nothing here acks a write, gates a
transaction, or changes what is true in the log. The archive is a derived materialization
downstream of the log, exactly as the table store is. A backup that acknowledged
independently of the log would be the second-place-things-become-true mistake MANIFESTO §5
and CLAUDE.md name as unabsorbable; this is not that.

**A gap closes a generation.** When the bounded pin (§5) releases and GC removes a segment
this generation never uploaded, the generation is closed and the next successful contact
opens a new one with a fresh full snapshot. Restore therefore never reasons across a hole —
a generation is either continuous or it is finished.

## §3 What ships, and when

**Sealed segments, verbatim.** Not re-encoded. Frames already carry CRCs and `read_log`
re-verifies at read-back, so shipping the original bytes lets restore replay through the
identical recovery path rather than a parallel one. Only *sealed* segments ship: the tail is
still being appended and its unsynced bytes must never travel — the discipline replication
already keeps by capping shipping at `last_applied` (DESIGN §5).

**Periodic logical snapshots** via `Db::snapshot` + `Snapshot::scan_raw_from` — the *bounded*
batch read (`table.rs:1043`), not `scan_raw`, for the memory reason in §10. Snapshots bound
restore time and release the WAL below them: a generation's chain starts at its own frontier's
segment, so a sealed segment lying *wholly* below that frontier is neither held by the
archive's retention pin nor shipped (§2, ruling 53). That is permission, not action — it is
`Wal::gc`, under the `Db`'s own `retain_segments` policy, that actually reclaims the file.

**What that sentence does NOT cover: the bucket.** The release above is of WAL on the
primary's disk. **Nothing in `charpente` or `charpente-core` ever issues an
`ObjectOp::Delete`**, so a superseded generation's snapshot and segment objects — and the
partial objects a crashed upload leaves behind — accumulate in the object store forever.
Reclaiming them is **unimplemented**: review finding R-3, filed against an earlier and more
ambiguous wording of this paragraph, alongside F-C's orphaned partials. It was carried unrouted
rather than dropped, because the paragraph above was narrowed to the WAL sense in Task 7.5, and a
narrowed sentence that quietly sheds a promise is exactly the drifted doc hard rule 6 exists to
prevent.

**Routed 2026-08-26.** Delete-on-supersede for what a manifest names: when a generation
rolls, the superseded generation's objects are deleted **by name, out of its own manifest**, so
no LIST and no key-discovery surface enters the system. The orphaned partials get a documented
bucket lifecycle rule instead, because an object orphaned by a failed upload is orphaned
precisely in that no manifest recorded it, and only a LIST could ever find it. It lands as
**Task 12.5, after restore and the convergence property** — deletion does not ship before the
oracle that proves what it would delete is recoverable. Reclamation is an optimisation and never
a correctness dependency: the archive stays correct when every delete fails forever.

**One operation in flight is a bound, not a free choice.** Serial uploads mean the archive
falls behind whenever segments seal faster than they upload — and a permanently-behind archive
releases its bounded pin continually, making gaps routine, which is the failure mode ruling 3
exists to avoid. The bound is: *seal interval must exceed upload duration*, i.e.
`segment_bytes / write_throughput_bytes_per_sec > segment_bytes / upload_bandwidth`, which
reduces to **upload bandwidth must exceed sustained write throughput in bytes/sec**. An
order-of-magnitude check against the apply-batching spike figures suggests comfortable
headroom at a 64 MiB `segment_bytes` on current hardware, but those are sub-project
observations, not quotable numbers — **docs/BENCH.md is the only source of those** (CLAUDE.md),
and it has no archive measurements at all. The plan therefore owes a measured seal rate and a
stated minimum bandwidth, plus a decision on whether N-in-flight is needed. Until then this is
an assumption with arithmetic attached, not a verified property.

**Cadence is seq-driven, never time-driven** — every `archive_snapshot_every` committed
records, mirroring `DbConfig::checkpoint_every`. This is not a convenience: a time-driven
cadence would put wall-clock into the archive's decisions and make replay depend on it.
Clock skew is therefore a non-issue by construction rather than by handling, and that is
an assertion in §9, not a hope.

## §4 Where it runs

A sixth `Ready` arm in the kernel — `Ready` already carried Request, Job, Cron, Repl and
Tail before this design; `ObjectStore` is the fifth IO *trait*, and the two counts were
conflated when this was written. `Archive::step()` does one unit of work per call — start
one upload, poll one in flight, seal one generation — exactly the shape `Repl` already has
(DESIGN §5, "driven kernel-shaped"). No threads: hard rule 1 forbids spawning from core, and
a background uploader thread would make the upload schedule nondeterministic and unreplayable.

Because `serve::backup` runs the same kernel against an `EmptySource`, an archive arm and a
hot standby compose for free: archiving *from* the standby leaves the primary's loop
entirely untouched, which is the deployment most operators should choose. New edge:
`serve::archive`, plus archive config on the existing replicated edges. IMPLEMENTED as of
T9: `serve::archive(app, store, cfg)` only. The `Kernel` supports the archive/replication
pair (`build` takes both), but no edge constructs both yet, so the archiving-standby
deployment this paragraph recommends is not reachable through `serve` today.

### Archive ownership — who may write a prefix

**Two nodes archiving to one prefix both write `manifest`, and the layout as described has
no defence against it.** This is the split-brain hazard the dual-primary sub-project exists
to contain, reappearing on a new surface — and the boot gate explicitly does not eliminate
it. DESIGN §5 states the bound: "a probe answers 'is THIS witness a primary', never 'does
another primary exist'." A node promoted after a switchover chain is invisible to the check.

Two rules, both following precedent already in this codebase:

1. **The archive arm is gated on the write gate.** A `Verifying` node — durable `Primary`,
   witnesses unresolved — must not upload. This is precisely the oversight the verifying-peer
   policy found and fixed (DESIGN §5): `begin_seeding` was read-only, so the write gate never
   saw it, and a node was manufacturing durable replicas of a timeline it had not confirmed it
   owned. Their line — *"a seed creates a fresh copy of an unverified one"* — describes an
   archive upload exactly, except that an object in a bucket outlives the node's fail-stop far
   more thoroughly than a seeded follower does. The archive arm is likewise read-only against
   the `Db` and would slip past the write gate for the identical reason. It must not.
2. **Generations carry their writer.** The manifest records the writing node's identity
   alongside the generation counter; a writer that finds the manifest advanced by another
   identity stops archiving and reports, rather than interleaving generations into a bucket
   two nodes each believe they own. Detection, not prevention — the same honest posture the
   promote path takes toward its bounded-loss window.

   **The check is asked at two places and neither subsumes the other** (Task 8, implemented):
   once when the archive first reads the bucket, *before it uploads anything*, and again before
   every manifest swap, since a peer can be promoted at any point in between. The first is what
   makes "ownership is decided before anything is uploaded" true, and it is not redundant: an
   archive that only checked before swapping declines to *extend* a foreign generation and then
   opens a fresh one **numbered above** it, so by the time it reaches a swap the foreign manifest
   sits *below* its own generation — measured at Task 8 against the open-time check removed, an
   archive wrote a complete generation 4 over another writer's generation 3 and swapped the
   manifest to name itself. The comparison is therefore **identity alone**: "is the other writer
   further along" is a different question from "is this manifest mine", and only the second is the
   invariant §8 states.

   A refusal is **terminal**, unlike a gap: two writers on one prefix is an operator-configuration
   fact that no amount of retrying reaches. The archive releases its retention pin on the way in —
   one that will never ship again but goes on holding the WAL is the archive acquiring the power
   to halt the primary (§5) — and raises the notice once, when it happens, not once per step.

   **The trade identity-alone makes, stated so nobody has to rediscover it** (Task 8 review
   §1(b), measured). A node whose writer identity legitimately *changes* — the same node
   redeployed on a new port, nothing else about the bucket or the timeline different — now refuses
   **permanently** at open on a bucket it genuinely owns, before anything is uploaded, reporting
   `stopped() == true` for the life of the process. Measured against the discarded generation
   conjunct, that same node would have kept archiving under a fresh generation. This is the
   deliberate side of an **asymmetric** choice: nothing in the bucket distinguishes "I am the same
   node, renamed" from "a second node has taken this prefix over" — the manifest carries one
   string and both cases present identically — and the two errors do not cost the same. Wrongly
   continuing interleaves two writers and leaves a bucket neither can restore from; wrongly
   stopping costs archive availability, with a named operator notice, a live intact bucket and an
   untouched `Db`. Recovery is the operator action §7's row names, and it is not free: it abandons
   the live generation's objects into a bucket with no reclamation (§14) and re-uploads a full
   snapshot.

   **Operational consequence, binding on whoever wires the archive up (Task 9):**
   `ArchiveConfig::writer` is part of the archive's durable state, not a label. It **must be
   stable across a redeploy and must not be derived from anything a redeploy moves** — a listen
   address, a port, a pod name, a container id, an ephemeral IP. Deriving it from the serve bind
   address turns an ordinary port change into a permanent archive outage that reports
   `idle() == true` while it lasts.

Both rules are **additive**, which is why §5's "only change to shipped code" still holds:
`Db::verifying()` is already public (`db.rs:1708`) and already read outside core by the seed
refusal itself (`repl/session.rs:744`). The archive gate consults the same accessor the
verifying-peer policy consults, and changes nothing that exists.

Archiving from a follower or a `serve::backup` standby is the recommended deployment and is
unaffected by either rule: a follower is not write-gated as a primary, and a standby that is
promoted acquires the gate along with the role.

## §5 The retention pin — the only change to shipped code

`Db::pin_retention` (db.rs:1650) takes a single `Box<dyn Fn() -> u64 + Send>` and
replication owns it (`Repl::open`). The archive needs a second floor, so the pin must
**compose as a min over pins**. Everything else in this design is additive.

The archive's pin is **bounded** (ruling 3): it holds at the base_seq of the lowest
un-uploaded sealed segment — never below its generation's chain start, since everything under
that is the snapshot's (§2) — but never further back than `archive_pin_segments` (config,
default deliberately unset here — §14 records it as Noah's to rule, and the plan must justify a number rather than invent one). Past the bound
the archive releases, `Wal::gc` proceeds, the primary keeps serving writes, and the
generation closes with a named gap.

The reasoning, recorded because the alternative is superficially attractive: an unbounded
pin means an object-store outage grows the WAL until the disk fills and the log poisons —
the archive would have acquired the power to stop the primary. A derived artifact must not
be able to halt the thing it derives from.

## §6 Restore

`charpente restore --from <store> --to <dir>`:

1. `Get` the manifest, verify CRC, read the newest generation.
2. `Get` the snapshot meta and every table's row batches.
3. Drive `Seeder`: `rows` per table, `set_import_meta`, `finish` at horizon+1.
4. `Get` the segment chain and lay it into `wal/`, verifying each frame CRC.
5. Hand off to ordinary `Db::open`, which replays from `applied_seq + 1`.

`Seeder::new` already discards any store file and `wal/` segments at the target, dir-barriered
(DESIGN §5, "seeding always replaces a timeline, never merges into one"), so restoring into a
dirty directory is already safe and a crashed restore already converges on re-run. Restore
inherits that property rather than reimplementing it.

**Restore refuses a diverged local timeline.** If the target directory holds a log whose
frontier CRC disagrees with the archive's timeline at the same seq, restore refuses by name
rather than overwriting or resuming across it. This is the same class the replication ladder
already refuses (DESIGN §5: "a CRC mismatch on a retained frame → `Refuse` naming both seqs,
both CRCs, and the re-seed fix; diverged timelines are refused, never merged"). An explicit
`--force` discards, naming what it destroys. Raised by the concurrent codegen session,
2026-08-20.

`--force` is an operator-invoked destructive admin action, not an escape hatch on a running
guarantee, and so does not offend hard rule 2: it bypasses no invariant the system upholds at
runtime, and it has direct precedent in the fresh-data-dir re-seed GUIDE §10 already
prescribes as a remedy. It must name both frontiers and the record count it is about to
destroy *before* acting. A `--force` that silently succeeded would be the escape hatch.

**Restore resurrects the job queue, and operators must be told before they run it.**
`charpente.jobs` is a table (DESIGN §4), so pending, due and retrying rows ride the snapshot
like any other state. A restored Db comes back with jobs that will dispatch — meaning restore
can re-fire emails and third-party calls that already ran before the loss. This is correct
behavior: they are durable transactional state, and idempotency keys are exactly what make the
re-fire exactly-once-observable (MANIFESTO §5). But it is a consequence of running `restore`
that the operator must know in advance, so it belongs in the command's own output and in
GUIDE's operator section, not only in this spec.

The CLI owns the edge, but the logic lives in a core-testable function over
`&dyn ObjectStore` + `Box<dyn Disk>` so the simulator drives the real path, not an analogue.

## §7 Failure semantics and error text

Hard rule 5: name the contract, the site, and the fix.

| condition | behavior |
|---|---|
| object store unreachable, within pin bound | retry with backoff; pin holds; no operator notice |
| unreachable past `archive_pin_segments` | release pin, GC proceeds, close generation with a gap; one operator notice naming the bound, the elapsed segments, the prefix, and both fixes (raise the bound, or accept the new generation) |
| PUT fails mid-generation | manifest unchanged; generation retried from its first un-acked object |
| PUT lands but ack is lost | retry is idempotent — same key, same bytes; re-PUT is always safe |
| manifest CRC mismatch on restore | refuse, naming both CRCs and the key; never guess |
| object listed in manifest but absent | refuse, naming the generation and the missing key |
| segment frame CRC fails on restore | refuse, naming the seq and the segment; never import corruption |
| segment chain has a hole | refuse, naming the expected base_seq |
| restore into a diverged timeline | refuse, naming both frontiers and `--force` |
| node is write-gated (`Verifying`) | archive arm uploads nothing until the gate opens; no error, no gap recorded — it was never this node's timeline to ship |
| manifest names another writer identity — at open, or before any swap | stop archiving permanently, releasing the retention pin; one operator notice naming both identities, both generations, the manifest key, the prefix, and the fix; never interleave. **What the notice says about the bucket is conditional on which of the two sites raised it**, because the fix differs: at open this archive has uploaded nothing and the generation the manifest names is the live one, intact; at **every** swap — the OPENING SNAPSHOT's included, where that whole snapshot is in the bucket while this archive's own `frontier` is still unset — its own generation objects are already there and unreferenced, and the "remove that manifest object" remedy abandons them too. A notice that states one site's answer at the other misdirects the operator at the moment they are acting on it (hard rule 5, Task 8 review R-1) |
| manifest fails CRC or will not decode **on the archive arm** | stop archiving permanently, releasing the retention pin; one operator notice wrapping the decode failure and naming the key, the prefix and both fixes (re-fetch or restore the object and restart; or remove it once no other writer is confirmed), and stating what restart costs: the archive is an arm of the same single process, so recovering a stopped archive is a **database** restart and there is no in-process re-open. Conditional on the site for the same reason as the row above — at `Opening` nothing was uploaded, while at **every** swap, the opening snapshot's first one included, this generation's objects are already in the bucket and "remove it" strands them too. Never open a fresh generation over it: the manifest is the only evidence of who owns the prefix, so an archive that cannot read it cannot know the objects there are its own. **Not a bounded retry** — the retry this replaces held the WAL at the archive's pin for as long as it lasted, which is forever, and never reached a verdict (Task 8) |
| `ObjectStore` IO error on the archive arm | archive arm degrades; **never poisons the Db** — see §8 |

## §8 Invariants (assert these, TigerBeetle style — hard rule 4)

Negative space first, since these are the properties that can be violated without failing a
test:

- The archive **never** advances `last_applied`, acks a write, gates a transaction, or
  affects `Db::poisoned()`. An archive failure is not a database failure.
- The archive **never** ships an unsealed segment or bytes above `last_applied`.
- `start` is **never** called with an operation already in flight (assert, don't handle).
- The manifest **never** points at a generation whose objects are not all uploaded.
- A generation's segment chain is **never** discontinuous — a hole closes the generation.
- The archive arm **never** reads wall-clock time, directly or transitively.
- Restore **never** merges timelines; it replaces or refuses.
- A write-gated (`Verifying`) node **never** uploads — it does not manufacture durable copies
  of a timeline it has not confirmed it owns.
- The manifest is **never** advanced by a node that is not its recorded writer; a writer that
  finds otherwise stops rather than interleaving.
- An archive that cannot establish ownership of a prefix — the manifest names another writer, or
  will not decode — **never** writes to it. Ownership is decided before anything is uploaded, not
  after.

## §9 Testing — how the archive gets faulted

The flagship oracle: **a Db restored from the archive is logically equal to the source at
the archived frontier**, over N seeded universes with faults. Logical, not byte-identical —
matching the guarantee narrowed at `55f15ec`, which retracted byte-identity to equal-grouping
where app authors read it.

New sim fault axes on `ObjectStore`:

1. **PUT fails mid-generation** → manifest still points at the last complete generation;
   restore from it converges at that frontier.
2. **PUT succeeds, ack lost** → retry with identical key and bytes; assert idempotent.
3. **Store 500s for K steps, then recovers** → either continuity within the bound, or
   exactly one named gap and a new generation. Never a silent hole.
4. **Store unreachable past the pin bound** → GC proceeds, primary still accepts writes,
   gap named. This is ruling 3 as a mechanical assertion.
5. **Crash mid-restore** → re-run from scratch converges (leans on `Seeder::new`'s discard).
6. **Corrupt object on download** → refuse by name; assert nothing was imported.
7. **Manifest present, generation incomplete** (misordered writes) → detect and refuse.
8. **Restore onto a diverged local timeline** → refuse; `--force` discards and names it.
9. **Clock skew** → assert no behavioral difference at all; the cadence is seq-driven.
10. **Two nodes, one prefix** → promote a follower while the old primary still archives; assert
    the second writer stops and reports rather than interleaving generations, and that whichever
    generation the manifest names restores to a consistent frontier.
11. **`Verifying` node with archive configured** → assert it uploads nothing until the gate
    opens. The verifying-peer sub-project found exactly this class by asking the question; the
    archive arm should not have to be found the same way.

Coverage keys (`require:`) for: gap-closed-generation, lost-ack-retry, diverged-restore-refused.
Every failure prints the seed and a one-command repro (hard rule 5).

## §10 Determinism

The archive arm's decisions — what to upload, in what order, when to snapshot, when to
release the pin — are pure functions of log state and config, so they replay byte for byte
from a seed. Nondeterminism lives entirely below `ObjectStore::poll`, on the far side of the
trait, exactly as it lives below `Net::connect` today.

**The archive does not inherit seeding's memory ceiling, and the reason is worth stating.**
DESIGN §5 records that "v1 seeding holds the full logical snapshot in memory on BOTH sides...
incremental streaming is the tracked fix before large-store seeding." That buffering is a
property of the *network seed protocol*: the follower must hold `SnapshotRows` until
`SnapshotEnd` to apply the timeline atomically.

**The archive has no such constraint.** Its atomicity comes from the manifest swap (§2), not
from buffering, so it writes `snapshot/<table>/<n>` objects as it produces them and never holds
a whole snapshot. The producer is therefore `Snapshot::scan_raw_from` (`table.rs:1043`) — the
bounded batch read already built for the migration executor, whose own comment states the rule
this design needs: "batching must bound memory, not just record size." One MVCC snapshot, paged
reads, peak memory **O(batch) rather than O(store)**.

`scan_raw` (`table.rs:1026`) returns a whole table at once and must **not** be used by the
archive, notwithstanding that it is the call the seed path uses. Snapshot cadence is therefore
irrelevant to peak memory — worth stating because the obvious mitigation, bounding
`archive_snapshot_every` against store size, would not have worked: cadence changes how often an
O(store) spike occurs, never its height. Seeding's own ceiling remains seeding's, untouched.

**Simulator-side risk, recorded rather than discovered later:** the sim `ObjectStore` holds
whole objects in memory (`Vec<u8>` per op). A 64 MiB default segment × the fault matrix × parallel universes
is a real RSS story, and v1 seeding already holds a full logical snapshot in memory on both
sides (DESIGN §5). The plan must bound this with a number — a reduced `segment_bytes` in sim
is the likely answer — not a sentence. Flagged after a peer session measured the box at 72%
swap on 9 days uptime; memory, not cores, is the binding constraint here.

## §11 Exit criteria

- `ObjectStore` in `charpente-core::io`; sim impl with all nine fault axes; filesystem prod impl.
- Archive arm ships sealed segments and seq-cadenced snapshots; bounded pin composes with
  replication's.
- `charpente restore` rebuilds a Db from a bucket; refuses diverged timelines and every
  corruption case in §7 by name.
- Convergence property green over N fresh seeds (N stated in the commit, per CLAUDE.md's
  evidence bar), plus the recorded seed for any bug found.
- **Every assertion mutation-proven.** House practice (LEDGER: "an 8-mutation table, 8/8 RED";
  "its own mutation burden"), and it matters more here than usual: this design's oracles are
  mostly *negative* — the archive never uploads while verifying, never advances a manifest it
  does not own, never ships an unsealed segment. A test asserting something never happens
  passes trivially against an implementation that never does anything. Each fault axis in §9
  therefore owes a mutation that reddens it, and each `require:` coverage key owes proof that
  it actually fires. A concurrent sub-project found five single-line mutations surviving a
  fully green suite on 2026-08-20; a fault matrix is more exposed to that than ordinary code,
  not less.
- `cargo fmt --all --check`, `clippy --workspace --all-targets -D warnings`, `cargo test --workspace` green.
- Docs updated in the same commit (§13).

## §12 Non-goals

Point-in-time restore (ruling 2 — and restore-to-a-*timestamp* would require wall-clock,
which hard rule 1 forbids; restore-to-a-*seq* is the cheap follow-on). A real S3/GCS/Azure
client (ruling 4). Encryption at rest — the store's own SSE covers it; a charpente-side
crypto layer is its own decision. Multi-bucket fan-out. Blob-store integration (blobs do not
exist). Archiving a follower's divergent timeline. Continuous verification of archive
integrity (a `charpente archive verify` verb is an obvious follow-on, deliberately not here).

## §13 Docs to update, and what is not this session's to edit

Hard rule 6 — same commit as the code:

- **DESIGN §1** — the capability trait list gains `ObjectStore`. [IRREVERSIBLE] section; approved 2026-08-20.
- **DESIGN §5** — the "Backup (M2, not built)" paragraph becomes what was built, keeping the `serve::backup` distinction explicit.
- **PLAN.md:104-105** — the M2 bullet. **Held**: a concurrent session is landing a structural split of the adjacent WebSockets/codegen bullet first, by agreement; this edit rebases onto theirs.
- **README.md** — the "what is *not* built" list names object-storage backup explicitly.
- **`charpente-cli`** — three new verbs land in this crate across two concurrent sub-projects:
  `restore` here, `gen-client` and `check-client` from the TS-codegen stream. No file overlap
  identified, but a shared `Cargo.toml` and probably a shared arg-dispatch site; whoever lands
  second takes the merge. Recorded here because this coordination otherwise exists only in
  cross-session chat.
- **docs/GUIDE.md** — an operator section for archive config and restore, including the
  job-resurrection consequence in §6. Note: `crates/guide-examples`'s build.rs extracts and compiles every fenced `rust` block there.

**Not this session's to edit:**

- **CLAUDE.md hard rule 1** names four of `io.rs`'s seven traits. Per §1 above it is shorthand
  rather than an exhaustive contract, and it was already incomplete before this design — so
  updating it is optional tidying, not a correctness obligation. Noah approved an update; if it
  happens it belongs in the commit where the trait exists, not in a spec — a rule naming a trait
  that does not exist yet is worse than a shorthand one.
- **DECISIONS.md** — §7 (redb vs LSM) is pending Noah's own edit for the 2026-08-20
  trigger-(a) re-read and is untouched here. Whether this design warrants its own numbered
  entry is §14 item 3.

## §14 Open items for Noah

1. `archive_pin_segments` default — **RE-DERIVED 2026-08-26, and the answer is a rule plus a
   measurement that does not exist yet, not a new number.** The old justification is not
   mis-computed but categorically invalid: it bounded a *history* quantity as a multiple of
   `retain_segments`, and ruling 53 made it a *rate x duration* quantity with no relationship to
   `retain_segments` at all. The replacing rule is at `ArchiveConfig::pin_segments`. Its dominant
   input — snapshot upload duration — has **no measured basis anywhere**: `docs/BENCH.md` has zero
   archive or object-store coverage, and CLAUDE.md makes it the only quotable source. So 8 is kept
   provisionally rather than re-picked, since raising it would repeat the original error with
   fresher-looking numbers. **What would settle it:** track the peak count of sealed segments at
   or above `next_needed` across a generation and have Task 12's convergence property record it
   across the fault matrix — one counter, on runs that task performs anyway, turning the default
   from a guess into a distribution. If a healthy generation routinely exceeds 8, that is a real
   finding: healthy boxes gap routinely, which is Task 7.5's startup-gap failure reappearing at
   steady state. The number remains Noah's.
   Prior wording, retained for the record: the plan proposes a number with reasoning; this spec
   deliberately does not invent one. **Ruling 53 moves this item.** The plan's default of 8
   was justified as "512 MiB of held WAL at the default 64 MiB `segment_bytes`", and that
   arithmetic was computed against the pre-ruling-53 held quantity — a chain that started at
   the log's base. Now that a generation holds only what seals above its own snapshot, the
   quantity the bound measures has changed and the default should be re-derived against it.
   Recorded rather than re-picked: the default is explicitly Noah's.
2. **RULED 2026-08-26 — bucket reclamation.** Recorded here because it was carried as an open
   item: nothing ever issued an `ObjectOp::Delete`, so superseded objects and orphaned partial
   uploads accumulated forever (findings R-3 and F-C, which are one gap seen twice). Ruling:
   delete-on-supersede by manifest name, plus a documented lifecycle rule for the orphans, as
   **Task 12.5** after restore and the convergence property. The full argument is in §3; the
   deciding asymmetry is whether the objects can be named without asking the bucket.
3. Whether the capability-surface decision warrants a numbered DECISIONS.md entry at all.
   §1's correction lowers the stakes — this is an application of §1, not an amendment to it —
   so an entry may be over-ceremony. Noah's call, and DECISIONS.md is a shared-spine file with
   concurrent sessions live.

**Resolved and recorded rather than carried**, both by inspection after first being raised
here as open questions: the snapshot memory ceiling (§10 — dissolved by `scan_raw_from`, which
already exists), and the five "replica or backup" error messages (Context — not a defect;
"backup" means `serve::backup`, and the remedy is real).
