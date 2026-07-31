# M2 sub-project 0b — the dual-primary gap

Status: DRAFT, awaiting Noah's sign-off. Ruling (Noah, 2026-07-31): **option (c)** —
a resurrected primary serves reads and withholds writes until its witnesses resolve.
Investigation and the fork's full statement: `.superpowers/sdd/dual-primary-investigation.md`.

## Context

A primary dies. An operator promotes the follower. Later the old box is restarted **exactly
as it was configured** — same `--role primary`, same data dir. Its durable role file says
`Primary`, the durable role always wins over config, and `role.rs:163-165` emits a notice
only when config *contradicts* the file. So it resumes as a primary in total silence.
Neither node dials the other (`mod.rs:218` — a durable `Primary` has no follower client), so
nothing on either side can observe the conflict. Two live, writable, mutually invisible
primaries on diverged timelines; the resurrected node serves stale reads **and acks new
writes**. The mandated remedy for a demoted primary is a fresh data dir plus a re-seed,
which destroys every write it acked in the meantime — acked-write loss reached through an
ordinary "restart the old box the way it was" action.

## 1. Mechanism: the witness set

A **witness** is a node this primary has shipped to. The witness set is the durable record
of them, and the resurrection check is: *if any node I shipped to now answers as a primary,
I am not the primary any more.* That is conclusive on its own — the only way a node I
shipped to is now a primary is that it was promoted or handed off to.

**The data already exists.** `Hello` carries the sender's listen address
(`follower.rs:317`), and the primary already stores it per session (`session.rs:60`, set at
`:288`/`:294`). This spec persists what is already in memory. No wire change, no new
protocol field, no proto bump.

**Durable form.** A `witnesses` file in the data dir, sibling to `role`, written with
`role.rs:115-125`'s exact discipline — `write_at` + `set_len` + `sync` + `sync_dir`, content
CRC'd — because a witness set that a crash tore must fail its integrity check by name rather
than parse as a shorter (and therefore less safe) set. One listen address per line.

**When written.** When a ship session is established and its peer `listen` becomes known
(`session.rs:288`/`:294`), append-if-new and sync. That is one durable write per *new* peer,
not per ship, so the steady-state cost is zero.

**When cleared.** On any durable role flip away from `Primary` (demote, hand-off completion).
A node that demoted and re-seeded is on the new timeline; its old witness set is stale and
keeping it would block writes after a future legitimate promotion. Never cleared
automatically otherwise — see §4.

## 2. Boot behaviour (the ruling)

At `Repl::new`, for a durable `Role::Primary`:

- **Empty witness set → `allow_writes()` immediately, silently.** Single-node primaries and
  never-shipped primaries are completely untouched. This is the common case and it must stay
  free.
- **Non-empty witness set → start write-stopped**, probe every witness, and open the write
  gate only when *every* witness has answered "not a primary".

**This is a constraint on where the gate lives, not just what it decides.** `mod.rs:191`
calls `db.allow_writes()` inside the `match &role` block, and the follower client is not
constructed until `:204` — writes are enabled strictly before any probe could run. The gate
must move: a durable `Primary` with witnesses starts at `forbid_writes` and is promoted to
`allow_writes` on resolution. A fix bolted on after `:191` is not a fix.

**Probe outcomes** map onto the trichotomy `follower.rs:221-236` already implements:

| Witness answers | Meaning | Action |
|---|---|---|
| `Refuse` containing `not a primary` | never promoted | this witness is satisfied |
| any ship message | it IS a primary | **refuse by name** (§3) |
| any other `Refuse` (the `diverged_*` family) | it is a primary on another timeline | **refuse by name** (§3) |
| unreachable | unknown | stay write-stopped, back off, retry |

**After `WITNESS_DEADLINE_NANOS`** with witnesses still unreachable: stay write-stopped,
emit one loud notice naming the unreached witnesses and the remedy, and keep probing. Reads
are served throughout; the node resolves itself the moment the witness returns. This is
option (c), and the notice is not a substitute for the gate — it is how the operator learns
why writes are refused.

**`ReplConfig::strict_witnesses: bool`** (default `false`) turns the unreached case into a
refusal to start at all, for operators who prefer a node that fails loudly over one that
serves reads it cannot write to.

## 2a. The write gate needs a third state (found while planning)

`Db` models the gate as `not_primary: Option<String>` (`db.rs:1077`) and surfaces it as
`TxError::NotPrimary { primary }` → **421 naming where to go instead**. A witness-gated node
has **no primary to name**: it does not know whether one exists, and it may itself still be
the legitimate primary. Answering 421 with any address would be a lie, and answering with its
own would be a loop.

So the gate becomes three-valued — `Allowed` / `NotPrimary(addr)` / `Verifying` — with
`TxError::Verifying { unreached: Vec<String> }` rendering as **503** (retry) rather than 421
(misdirected), naming the unreached witnesses and stating that reads are being served and
writes resume automatically. 503 is the honest status: this is a transient state the node
expects to leave on its own.

This is a **pre-release breaking addition to the public `TxError` enum**, in the same class as
realtime's `emit` format change (sub-project 3). The existing `NotPrimary` → 421 pin must stay
byte-unchanged: this adds a state, it does not alter the existing one.

## 3. The probe client is NOT `Follower::new_resolving`

Reusing it would be wrong, and the reason is worth stating so it is not "simplified" back
later. `Follower::step` classifies the first response **and then falls through to the normal
handlers** (`follower.rs:238-239`): `Msg::Frames` reaches `apply_and_ack`, `SnapshotStart`
opens a seed. For a `HandingOff` node that is exactly right — it is demoting to a follower of
`to`. For a resurrected primary it is exactly wrong: it would silently auto-heal a forked
node by adopting the winner's timeline, which is precisely what the 2026-07-16
diverged-BEHIND interlude ruled against (refuse, do not demote — the node cannot know it is
the loser, and its acked writes are not ours to discard).

So the witness probe is a **classify-and-disconnect** client: send `Hello`, read the first
response, classify it against the table in §2, tear the connection down. It never applies a
frame, never opens a seed, never writes durable state except the role-gate decision.

The refusal, when a witness answers as a primary, follows `decide_hello`'s `diverged_behind`
shape: name both nodes, both remedies (fresh data dir + re-seed for this node if it is the
loser; promote-this-node if the operator judges otherwise), and never demote automatically.

## 4. Operator escape hatch, and its cost stated honestly

A permanently decommissioned follower stays in the witness set forever and will block writes
after a restart. That is deliberate — a decommissioned node **could still have been promoted
before it was decommissioned**, so conservative retention is the safe default — but it needs
an exit. The exit is editing or deleting the `witnesses` file, documented in GUIDE §10 with
an explicit warning that doing so re-opens exactly the gap this closes.

**The honest cost of the ruling:** a primary whose only follower is permanently gone will not
accept writes until an operator intervenes. Reads stay up. This is a real regression in a real
deployment shape, accepted deliberately because the alternative is silent acked-write loss.

## 5. Non-goals

- **No wire change.** "The peer answers as a primary" is already distinguishable today.
- **Design C (a durable timeline epoch on `Hello`)** stays ledgered and stays compatible: an
  epoch would *sharpen* the refusal, not replace the probe.
- **No automatic witness eviction**, no TTL, no heartbeat liveness. Retention is conservative
  by design (§4).
- **Not the `HandingOff` path**, which already probes and is already write-stopped.

## 6. Tests

- The edge tripwire `failover_known_gap_silent_dual_primary_resurrection`
  (`crates/edge-harness/tests/failover.rs`) is **updated, never deleted** — it currently
  asserts the two primaries cannot see each other and goes red on any fix. It becomes the
  regression test, and its name should stop describing a gap. Note it is `#[ignore]`, so its
  red appears only under `scripts/edge-harness.sh`.
- A sim property: a shipped-to follower promoted, the old primary restarted with its original
  config, must not reach `allow_writes` — asserted on the gate, not on a message.
- The empty-witness-set path stays silent and immediate (single-node regression).
- The unreachable-witness path serves reads and refuses writes, by name.
- Witness durability under the fault matrix: a torn `witnesses` write fails its integrity
  check by name and never parses as a shorter set.
- RED first for every one of them, per hard rule 3.

## 7. Evidence bar

Sub-project 0a's bar, unchanged: fmt, clippy `-D warnings`, guide gate; `cargo test
--workspace` twice; the pinned debug replays and both release mode-skew tripwires; a fresh
invoicing 1024-universe batch with its master seed recorded; and `scripts/edge-harness.sh`,
which this sub-project touches directly. Counts captured via `tee`, never `tail`.
