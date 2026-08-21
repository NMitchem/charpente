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

Verified: there is none. Every `backup` hit in the tree is the hot standby, replication
role plumbing, test fixtures, or — notably — **operator remedy text**. Five error messages
in the deterministic core already instruct the operator to "restore from a replica or
backup" (`log.rs:186`, `log.rs:365`, `db.rs:1056`, `db.rs:1299`, `db.rs:1359`), including
the hard error `Db::open` raises when a compacted log can no longer rebuild an unopenable
store. Until this subsystem exists, that second noun is unbacked: the only remedy the
platform actually ships is a replica. No prior spec covers this.

**The substrate already exists.** This design is mostly composition, not new machinery:

| need | what already provides it |
|---|---|
| hold WAL segments against GC | `Db::pin_retention` (db.rs:1650), a `Box<dyn Fn() -> u64>` floor — replication already uses it |
| read log frames, CRC-verified, never the unsynced tail | `Db::read_log` (db.rs:1731), capped at `last_applied` |
| logical snapshot of all tables | `Db::snapshot` (db.rs:1510) + `Snapshot::scan_raw` (DESIGN §4, "Logical export") |
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

Adding to `charpente-core::io` amends **DESIGN §1, marked [IRREVERSIBLE]**, and makes
CLAUDE.md hard rule 1 stale — it enumerates exactly four traits by name. That escalation
was surfaced and approved explicitly (2026-08-20, Noah) rather than folded into a file
list. Two verified facts justify it.

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
<prefix>/<gen>/snapshot/<table>/<n>          — batched raw (id, bytes) from scan_raw
<prefix>/<gen>/segments/<base_seq:020>.log   — sealed WAL segments, verbatim
```

A **generation** is one full logical snapshot plus the chain of sealed segments continuing
it. `<gen>` is a monotonic counter, zero-padded, held in the manifest.

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

**Periodic logical snapshots** via `Db::snapshot` + `scan_raw`, the same producer the seeder
uses. Snapshots bound restore time and let the archive delete segments below them.

**Cadence is seq-driven, never time-driven** — every `archive_snapshot_every` committed
records, mirroring `DbConfig::checkpoint_every`. This is not a convenience: a time-driven
cadence would put wall-clock into the archive's decisions and make replay depend on it.
Clock skew is therefore a non-issue by construction rather than by handling, and that is
an assertion in §9, not a hope.

## §4 Where it runs

A fifth `Ready` arm in the kernel. `Archive::step()` does one unit of work per call — start
one upload, poll one in flight, seal one generation — exactly the shape `Repl` already has
(DESIGN §5, "driven kernel-shaped"). No threads: hard rule 1 forbids spawning from core, and
a background uploader thread would make the upload schedule nondeterministic and unreplayable.

Because `serve::backup` runs the same kernel against an `EmptySource`, an archive arm and a
hot standby compose for free: archiving *from* the standby leaves the primary's loop
entirely untouched, which is the deployment most operators should choose. New edge:
`serve::archive`, plus archive config on the existing replicated edges.

## §5 The retention pin — the only change to shipped code

`Db::pin_retention` (db.rs:1650) takes a single `Box<dyn Fn() -> u64 + Send>` and
replication owns it (`Repl::open`). The archive needs a second floor, so the pin must
**compose as a min over pins**. Everything else in this design is additive.

The archive's pin is **bounded** (ruling 3): it holds at the base_seq of the lowest
un-uploaded sealed segment, but never further back than `archive_pin_segments` (config,
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

Coverage keys (`require:`) for: gap-closed-generation, lost-ack-retry, diverged-restore-refused.
Every failure prints the seed and a one-command repro (hard rule 5).

## §10 Determinism

The archive arm's decisions — what to upload, in what order, when to snapshot, when to
release the pin — are pure functions of log state and config, so they replay byte for byte
from a seed. Nondeterminism lives entirely below `ObjectStore::poll`, on the far side of the
trait, exactly as it lives below `Net::connect` today.

**Risk, recorded rather than discovered later:** the sim `ObjectStore` holds whole objects in
memory (`Vec<u8>` per op). A 64 MiB default segment × the fault matrix × parallel universes
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
- **docs/GUIDE.md** — an operator section for archive config and restore. Note: `crates/guide-examples`'s build.rs extracts and compiles every fenced `rust` block there.

**Not this session's to edit:**

- **CLAUDE.md hard rule 1** enumerates four traits and goes stale when the fifth lands.
  Noah approved the update; it belongs in the commit where the trait actually exists, not
  in this spec — a rule naming a trait that does not exist yet is worse than a stale one.
- **DECISIONS.md** — a new numbered entry should record the fifth-trait decision. §7
  (redb vs LSM) is separately pending Noah's own edit for the 2026-08-20 trigger-(a)
  re-read and is untouched here.

## §14 Open items for Noah

1. `archive_pin_segments` default — the plan proposes a number with reasoning; this spec
   deliberately does not invent one.
2. Whether the DECISIONS.md entry for the fifth trait is written by this sub-project or by
   Noah directly, given DECISIONS.md is a shared-spine file with concurrent sessions live.
