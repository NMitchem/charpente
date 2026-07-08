# M1 sub-project 2 — replication: design

Status: approved by Noah 2026-07-08 (scope and all four rulings his: backup as a
Disk-target follower, zero-loss switchover / bounded-loss failover, follower reads via
normal routes with effects rejected at commit, startup role + admin verbs over the
replication port). One precision landed after section approval: the backup process is a
run mode of the *app's* binary, not a generic-CLI subcommand — apply resolves table
names through the compiled-in `Schema`, so only the app binary can materialize the
store (§1). The design essence (headless follower) is unchanged.

## Context

Sub-project 2 of the M1 decomposition (spec 2026-07-07-wal-segment-management-design.md):
async log shipping, follower reads, manual promotion, backup, multi-node sim. Everything
here implements DECISIONS §6 (async replication, loss window on crash failover, consensus
later behind the same log format — that decision stands, not reopened) and DESIGN §5's
mechanism. Two standing IOUs come due: the above-horizon truth (acked-but-uncheckpointed
records lost to WAL media corruption — "closed by M1 replication") and the compacted-log
hard error's "restore from replica/backup" fix text, which this sub-project makes real.

Prior art (NEIGHBORS): Litestream's WAL-shipping design is the blueprint in spirit;
frames are shipped instead of segment files because charpente sits inside the database —
frames are the native unit, the live tail segment needs no special-casing, and zero-loss
switchover needs frame-precise catch-up regardless. Rejected alternates: segment-file
shipping (freshness quantized to rotation; tail special-cases), apply-only shipping with
no follower WAL (a promoted follower must own a log the instant it becomes primary — the
log is the only source of truth, hard rule), semi-sync acks (contradicts DECISIONS §6;
adds a network round-trip inside the commit path), pin-the-log-forever for absent
followers (unbounded growth).

## Scope

1. The replication protocol over `Net` + the primary-side shipper and follower-side
   apply loop (frame-level, pull-positioned, push-streamed).
2. Logical snapshot seeding (fresh followers, followers behind the GC'd base).
3. Roles (durable), manual promotion, zero-loss switchover, divergence refusal.
4. Follower reads through the existing route surface; jobs/cron primary-only.
5. Backup as a headless follower mode; restore = open/promote its data dir.
6. Multi-node sim: per-host disk worlds, per-host crash/restart, replication fault
   axes, the property matrix below.

Non-goals: object-storage/S3 backup adapter (later in M1, an edge impl of the same
target machinery); cascading followers (a follower cannot feed another follower);
consensus-elected or automatic failover (arbiters call the same admin verbs later);
a typed read-only route form (a later refinement — compatible, not required); realtime
event tailing (sub-project 3); any change to the frame or record format.

## 1. The data plane

**Protocol.** Typed messages over the existing message-oriented `Net` traits
(`one send = one recv`), on a dedicated replication listener the primary opens when
configured. Every message carries a protocol version byte. Session:

1. Follower connects: `Hello { proto, last_seq, last_crc }` — its highest durable WAL
   seq (the WAL's none-sentinel when empty) and that frame's CRC as an integrity anchor.
2. Primary replies with exactly one of:
   - `Frames` — resume: stream from `last_seq + 1`. Precondition: the primary retains
     the frame at `last_seq` and its CRC matches `last_crc` (or `last_seq` is the empty
     sentinel and the primary's base is the log start).
   - `Snapshot` — the follower's next needed seq is below the primary's `base_seq` (or
     the follower is empty and the base has moved): logical re-seed, below.
   - `Diverged` — CRC mismatch on a retained frame, or the follower is at/ahead of the
     primary's `next_seq`. Refused, never merged (§3).
3. Steady state: the primary pushes `Frames { [(seq, payload)] }` batches as commits
   land, and `Heartbeat { next_seq }` when idle (lag measurement, drain detection).
   The follower sends `Ack { durable_seq }` only after its local WAL sync — never on
   receipt. A shipped frame's CRC is re-verified when read off the primary's segments
   and again by the follower's normal frame validation on append.

**Follower apply loop.** Deliberately unoriginal: append shipped frames to the
follower's own local WAL (same format, same segments, same rotation), `sync` per batch,
then apply through the same `Engine` path recovery uses, then `Ack`. Follower state ≡
fold(apply, durable records) by the same code that holds that invariant everywhere
else. Followers run their own checkpoint cadence and segment GC (no sessions to pin
them — no cascading — so GC floors at `retain_segments`). Any IO failure poisons the
follower exactly as it would a primary; restart-and-recover, then re-`Hello`.

**Retention pin.** The primary pins retention at the minimum over live sessions of
(acked `durable_seq` + 1); a seeding session pins at its snapshot's `applied_seq + 1`
from session start (the MVCC snapshot pins the read view; the pin keeps the frames that
must follow it). A dead session releases its pin — GC reverts to `retain_segments`
slack, and a returning follower that has fallen below the base re-seeds via `Snapshot`.
This is the hook sub-project 1 shipped (`Db::pin_retention`), now wired.

**Seeding.** `SnapshotStart { applied_seq, next_job_id }`, then `SnapshotRows { table,
rows }` batches — every row of every table out of one MVCC `Snapshot`, in
schema-registry order and id order (deterministic byte order), then `SnapshotEnd`, then
`Frames` from `applied_seq + 1`. The follower discards its store and WAL and rebuilds
from the stream; afterwards its WAL base is `applied_seq + 1` — exactly the
base = horizon + 1 boundary `Db::open` accepts. Logical, not physical: engine-agnostic
(survives the M2 redb→own-engine swap), safely streamable from a live primary (MVCC),
and byte-deterministic so the fingerprint oracle (§4) holds across a seed. Physical
store copy rejected: a live redb file with unsynced `Durability::None` commits is not
consistently copyable, and it welds the wire to redb's layout right before M2 replaces
it.

**Backup.** A headless follower: the app binary running with `Role::Backup` — the
identical session/apply machinery, no HTTP listener. (App binary, not the generic
`charpente` CLI: apply resolves table names through the compiled-in `Schema`.) Its data
dir is at all times a valid node dir: `Db::open` works on it, and promoting a copy of
it is the restore path the compacted-log error text has been promising. Archival copies
in v1: stop the process (graceful close is durable), copy the dir, restart; no
freeze/thaw verbs. Restoring *to the backup's position* loses whatever the backup had
not yet acked — the same honest window as failover.

## 2. The control plane

**Role is durable state, not just config.** Startup config declares the initial role —
`Primary { replication_listen: Option<addr> }`, `Follower { primary: addr }`, or
`Backup { primary: addr }` — but the effective role lives in a `role` file in the data
dir, written via `Disk` with sync + `sync_dir`, read at node open. Durable role wins
over config (a follower promoted during failover must not restart as a follower of a
dead primary); the node names the mismatch at startup when they disagree. First boot
initializes the file from config. No epochs/terms in v1 — divergence is detected by
content (seq/CRC), and consensus, when it comes, arrives behind the same log format
(DECISIONS §6).

**Admin verbs travel over the replication port** as control messages in the same
protocol — the sim drives promotion through exactly the code path the CLI uses
(same-shape rule), and no unauthenticated admin surface appears on the public HTTP
listener (auth is sub-project 4). The generic `charpente` CLI hosts the verbs (network
messages only; no Schema needed):

- **`charpente switchover --primary P --to F`** — zero loss. P's role file durably
  moves `Primary → HandingOff(F)` (write-stopped: reads continue, effectful
  transactions get the typed rejection), P drains until F's acked `durable_seq` equals
  P's last committed seq, sends `HandOff`; F durably flips to primary and opens its
  replication listener; P durably flips to `Follower(F)` (drained-to-equality — no
  divergence possible). If F cannot catch up within the deadline, the switchover
  aborts: P durably returns to `Primary` and the abort error names the remaining lag.
  Crash-safety is the three-state file: a P restarting in `HandingOff(F)` stays
  write-stopped and resolves against F's actual role — F already primary → P becomes
  its follower; F still a follower (the `HandOff` never landed) → the operator re-runs
  or aborts by verb. No interleaving yields two durable primaries.
- **`charpente promote --node F`** — failover, bounded loss. F drops its session,
  durably writes role, starts accepting writes from its current applied position.
  Acked-but-unshipped records on the dead primary are the DECISIONS §6 window, lost
  honestly and named in the operator output. The old primary rejoining later presents a
  `Hello` that is ahead or CRC-mismatched → `Diverged`, refused; the error names the
  re-seed path (restart with a fresh data dir, snapshot-seed as follower).

## 3. Node integration

**Kernel.** On the primary, replication is a fourth arm in the existing ready set
(pending accepts, readable sessions, unshipped frames) — the which-source-next decision
stays one piece of code in prod and sim (DESIGN §1's same-shape rule). On a follower,
the kernel runs the request arm and the apply arm and does **not** run job dispatch or
cron: enqueued jobs materialize in `charpente.jobs` via replicated records but execute
only on the primary; Done-marks replicate as ordinary records, so exactly-once survives
promotion, and a promoted follower's job/cron arms simply switch on against the rows
already in its store.

**Db.** Two new entry points: effectful-commit gating by role — `transact` whose
effects are non-empty on a non-primary returns the new `TxError::NotPrimary { primary }`
(effect-free transactions never touch the log and run unchanged — follower reads need
zero app-code changes) — and `apply_shipped` for the apply arm (append → sync → apply,
batched, the submit discipline minus the client ack). The HTTP layer maps `NotPrimary`
to 421 Misdirected Request naming the primary's address.

**The accepted wart.** The prod kernel parks on the request channel, which network
messages don't interrupt; when replication is configured, parks are capped at a short
tick. Latency-only, prod-only (sim time is virtual), tolerated by async shipping;
the sub-project-6 edge harness can measure it. Converges to a real wakeup with the
shared-clock cleanup later.

## 4. The multi-node sim

**Per-host worlds.** `Sim` grows host-scoped handles: `sim.host(name)` yields that
host's disk (own namespace, own pending-write state), entropy (sub-stream keyed by
(universe seed, host name) — same field-keyed construction as FaultConfig extensions),
net endpoint, and `durable_fingerprint()`. Virtual time stays universe-global.
`sim.crash_host(name)` collapses that host's pending writes through the existing
torn/vanish/dir-entry machinery and drops its connections (peers observe close);
`sim.restart_host(name)` re-opens through normal recovery. The universe-global
`disk()`/`crash_disk_*`/`restart()` are frozen forever — every pinned single-node seed
replays unchanged.

**Fault axes.** `FaultConfig.conn_drop` (a live replication conn dies; both sides see
close and must re-`Hello`) and `FaultConfig.ship_latency` (per-message latency spikes on
replication conns), both drawn from field-keyed sub-streams per the frozen-replay
contract (spec 2026-07-07 §4); the legacy sequential draw is untouched. Existing
`partition`/`repair` compose. Auto-covered keys: `fault:conn-drop`, `crash:host`.

**The property matrix** — each under full fault injection, swarm-sampled; any bug found
lands with its seed pinned:

1. **Convergence**: a caught-up follower's `durable_fingerprint` is byte-identical to
   the primary's at the same applied seq (determinism makes the strongest oracle
   nearly free).
2. **Zero-loss switchover mid-write-load**: every acked write survives the switchover;
   reads served on the follower throughout — PLAN's M1 exit criterion, verbatim.
3. **Failover**: everything the follower had acked survives; the new timeline is a
   consistent prefix (exactly-once job effects hold across promotion).
4. **Diverged rejoin refused** — by name, never merged, error text pinned.
5. **Seeding**: a fresh or lapsed follower converges via `Snapshot` to
   fingerprint-identical, and its WAL base lands at horizon + 1.
6. **Retention pin**: a live lagging session never has the log GC'd out from under it
   (shipping never fails for a retained-then-deleted frame while the session lives).
7. **Backup**: the headless follower's dir passes `Db::open` and promotes to a
   fingerprint-identical node.
8. **The above-horizon promise kept**: in universes where a WAL durable-corruption
   fault fires on the primary, acked records above the horizon still exist on a
   caught-up follower. Single-node workloads keep the `durable_corruption_fired`
   excuse; replicated workloads assert the stronger oracle.

**Meta-tests** (house style — the harness must catch each naive victim by name): a
follower that acks before sync (caught by host-crash + failover loss beyond the acked
point), a switchover that skips the drain (caught by the zero-loss oracle), a shipper
that streams frames without re-verifying CRC at segment read (caught by the
misdirected-write axis landing inside a retained segment — the sim's `Net` is
reliable, so the primary's own durable bytes are where shipped corruption comes from).

## 5. Error surface

Product surface, pinned by tests against text: `NotPrimary` (421 + primary address);
`Diverged` (both seqs, both CRCs, the re-seed fix); switchover abort (remaining lag and
the deadline); startup role-file/config mismatch (which won and why); protocol version
mismatch at `Hello` (both versions). The existing compacted-log hard error's
restore-from-replica/backup fix text is now backed by a real path and stays as written.

## 6. What changes where

- `charpente-core::log` — a frame reader (`read from seq N`, CRC re-verified) for the
  shipper; `base_seq`/`next_seq` already exist.
- `charpente-core::db` — `TxError::NotPrimary` + role gate in `transact`;
  `apply_shipped`; the logical snapshot stream (per-table row iteration through
  `Schema`); retention pin wired to sessions; role file read at open.
- `charpente::repl` (new) — protocol codec, primary session manager, follower client,
  control-verb handling.
- `charpente::kernel` — the replication arm; follower mode (no job/cron arms until
  promotion); park tick when replication is configured.
- `charpente::serve` + app config — `Role` in the serve config; `Backup` headless mode.
- `charpente-cli` — `promote`, `switchover` (network control messages only).
- `charpente-sim` — per-host worlds, per-host crash/restart, `conn_drop`/`ship_latency`
  axes, coverage keys.
- DESIGN §5 (mechanism → implemented), §1 (per-host sim model), GUIDE (replication
  how-to: config, modes, operator verbs), checker docs — same commits as the code they
  describe.

## 7. Evidence bar

- Every existing pinned seed green; frozen runners, legacy FaultConfig sampling, and
  the global sim world untouched (byte-identical replay of an old seed demonstrated).
- The property matrix (§4) green over multiple fresh 256-universe swarm batches;
  the three meta-tests catch their victims by name.
- A fresh 1024-universe `examples/invoicing` batch green (single-node regression).
- One recorded end-to-end demo: primary killed mid-write-load, follower promoted,
  acked writes verified present, old primary's rejoin refused — the sub-project's
  DEMO artifact, PLAN M1 exit criterion evidence.
- Sim found-bug protocol unchanged: any bug found lands with its seed pinned.
