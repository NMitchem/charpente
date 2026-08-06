# M2 interlude — what a `Verifying` node may do as a replication peer

Status: DRAFT, awaiting Noah's sign-off. Ruling (Noah, 2026-08-06): the seed refusal
**never lapses** while the gate is shut — not at `WITNESS_DEADLINE_NANOS`, not on a timer.
Closes Minor 5 from M2 sub-project 0b, which that sub-project's Task 5 review ACCEPTED as
not-fixed and whose first fix attempt was made, measured, and reverted
(`.superpowers/sdd/2026-07-31-dual-primary/PARKED-minor5-seed-refusal.diff`, re-review C-1
in `review-t5-fixes-report.md`).

## Context

Sub-project 0b gave a resurrected primary a boot gate: durably `Primary` with a non-empty
witness set, it starts at `forbid_writes` and reaches `allow_writes` only once every witness
answers "not a primary". That closed acked-write loss through the write path.

It did not close the seed path. `begin_seeding` is read-only, so the write gate never sees
it, and a `Verifying` node remains a full replication peer: it accepts Hellos, ships, and
**seeds brand-new followers**. Manufacturing a fresh durable converged replica of a timeline
this node has not confirmed it owns is acting as the primary in the most consequential way
short of acking a write — and unlike a write, that replica **outlives this node's own
fail-stop** when a witness finally answers "primary".

The failure chain is concrete. Gated `P` seeds newcomer `G`. `P`'s probe then reaches the
real primary `F`, `P` refuses by name and stops. `G` now holds a durable replica of a dead
fork with nothing alive left to tell it so; promoting `G` loses every write `F` acked. The
operator error it needs — pointing a new node at the wrong primary — is the same class of
action 0b was built to survive ("restart the old box the way it was configured").

The Task 5 review judged the harm narrow because it needs an operator error stacked on top,
and judged the fix new policy rather than a Minor closure: spec §3's "refuse, do not demote"
governs what a node does once it **learns** a witness is a primary, not what a gated node
may do while it still does not know. That reading is correct, and this spec is the ruling
that reaches it.

## 1. The policy

A `Verifying` node **may not render a seed to a peer that holds nothing and is not one of
its recorded witnesses.** Everything else it does as a peer is unchanged.

The line: *catch-up ships a timeline the peer already holds a prefix of; a seed creates a
fresh copy of an unverified one.* A positioned peer is by definition already on this
timeline — if this node is forked, that peer forked with it, so shipping extends an existing
replica rather than manufacturing a new one. A recorded witness returning with an empty
store is the GUIDE §10 remedy **and** is what resolves the gate, so it must be seeded.

Already correct, untouched by this interlude, and named here so the scope is not read wider
than it is:

- Writes are refused (`TxError::Verifying` → 503).
- Switchover is refused (`mod.rs:966-987`) — `begin_switchover` write-stops the node and
  `abort_switchover` later calls `allow_writes`, which on an unverified node would resume
  exactly the second primary the gate exists to prevent.
- `promote` on a durably-`Primary` node is already refused as "already the primary".

## 2. The keying, and where the check lives

The condition is `db.verifying() && position.is_none() && !witnesses.contains(listen)`, with
an absent `listen` counting as **not** a witness (conservative — an unidentifiable peer is
treated as a newcomer).

Both position and membership conjuncts are load-bearing:

- **`position.is_none()` alone was the parked attempt's defect.** It does not mean
  "brand-new node"; `hello_position` (`follower.rs:441-444`) returns `None` for any node
  with `last_applied() == None`, **including a recorded witness returning with a rebuilt
  store**. The parked refusal was aimed at newcomers and hit this repo's own prescribed
  cure, taking five property tests across two suites with it — including auth's flagship
  `raw_credentials_never_touch_the_durable_store` and the deterministic
  `the_kernel_completes_its_skipped_startup_work_when_the_witness_gate_opens`.
- **Membership alone is also wrong.** A positioned peer absent from the set is a genuine
  follower on this timeline, and refusing it would break ordinary convergence.

**The check belongs in `decide_hello` (`session.rs:611`), not as a guarded arm in
`handle`.** This is the one structural departure from the parked diff, and it removes a
hazard class rather than commenting about it. `decide_hello`'s `None`-position arm
(`:653-659`) answers `Frames { from: 0 }` when `log_base() == 0` — a full replay that is
functionally a seed — and `Decision::Snapshot` otherwise. A refusal keyed on the
`Decision::Snapshot` **variant** is therefore silently inert on exactly the young primary
that has not run long enough to GC anything. The parked diff handled this by ordering its
guarded arm above both shipping arms and relying on rustc's `unreachable_patterns` under
`-D warnings` to hold it there. Placing the check inside the `let Some(..) = position else`
arm instead puts it at the single point both sub-arms already flow through: the ordering
hazard cannot be expressed, and no new `Decision` variant is needed.

The sender still refuses and closes the session. Retryability (§3) is entirely a
receiver-side property of the refusal string, so `handle`'s existing `Decision::Refuse` arm
is unchanged.

**Which witness set, and how it gets there.** `decide_hello` is a free function; the set
lives on `Repl` (`mod.rs:200`). It reaches the check as one new `&[String]` parameter
threaded `Repl::step` → `Session::service` → `Session::handle` → `decide_hello`. The borrow
is disjoint (`&mut self.sessions[idx]` alongside `&self.witnesses`, exactly as
`&self.peer_secret` is passed today), so no restructuring is required. The set consulted is
the **in-memory mirror**, never a fresh disk read: `Repl::open` loads it from disk and
`record_witnesses` persists before committing to the mirror, so within a process the two
cannot disagree in the direction that matters — the mirror is never ahead of the disk. This
does take `decide_hello` to nine parameters; that is accepted rather than wrapped in a
context struct, per the repo's preference for the smallest code that does the job over
speculative abstraction.

## 3. The refusal must be retryable, and must classify `Inconclusive`

A **new marker const**, distinct from `NOT_A_PRIMARY_MARKER`, recognized at **both**
`follower.rs:233` and `:295` — the resolving arm and the ordinary arm are separately fatal
today — mapping to `disconnect(now); Ok(())`, so the peer backs off `RECONNECT_NANOS` and
retries rather than dying.

`probe.rs::classify` must read it as **`Inconclusive`**. Neither alternative is available:

- `NotPrimary` would let a probing node treat "I am also unsure" as "I am satisfied" and
  open its own gate on no evidence.
- `IsPrimary` would refuse a healthy node over a phantom split-brain.

This is the same producer/consumer const discipline already used for `NOT_A_PRIMARY_MARKER`
and `DIVERGED_LABEL` (producer in `session.rs`, consumers in `follower.rs` and `probe.rs`),
and it must be commented as such at all three sites so a reword reddens locally.

Simultaneous restart of two gated nodes is **waiting, not deadlock**: each answers the other
with this refusal, each classifies `Inconclusive`, both back off, both serve reads, both
emit the deadline notice. The genuine split-brain case is unchanged and stays loud,
symmetric and terminal — both nodes carry real positions, each answers `Frames`, each
classifies the other `IsPrimary`, both refuse by name and stop.

## 4. The GC pin (0b re-review M-2)

`Session::pin_floor()` (`session.rs:112`) returns `u64::MAX` when `self.dead`, mirroring the
`control` short-circuit immediately above it. A dead session ships nothing further and is
reaped next step, so it must never hold GC.

`Repl::step` reaps at the top of the step but calls `recompute_pin()` immediately after
`service` (`mod.rs:601`), so a session refused during that step is still present with
`cursor = 0, acked = None` → `pin_floor() == 0`. `pump_io`'s own unconditional
`recompute_pin()` (`mod.rs:394`) recomputes over the dead session too, so the window is not
an artifact of one call site. This is **pre-existing on every refusal path** (peer-secret,
schema-digest, invalid-listen, the diverged family) and is not introduced here; §2 makes it
recur every `RECONNECT_NANOS` for as long as the gate stays shut.

**Measured correction (2026-08-06, Task 1).** An earlier draft of this section called the
window "permanent" under a retrying peer and filed it as the same class as seed
`0x2EB1_0010`. Both claims are wrong, and the implementer's diagnostic trace is what
disproved them. `Repl::step`'s pre-existing top-of-step `sessions.retain(|s| !s.dead)`
reaps the refused session and restores the pin on the very next round, by machinery that
has nothing to do with `pin_floor`. The observed sequence across three rounds was
`0 → 0 → 0 → MAX → MAX`. So the window is **one step, recurring** — not forever — and each
occurrence costs at most **one checkpoint's GC**, and only when a checkpoint happens to land
inside it. `0x2EB1_0010` was genuinely unbounded (a silent `AwaitHello` session nothing
reaped until its deadline); this is not that.

The fix is still worth taking: it is three lines in the function the hazard lives in, and
the impact is real on a **healthy** primary, where any misconfigured peer retrying every
`RECONNECT_NANOS` (wrong secret, schema mismatch, a diverged follower) can skip checkpoint
GC repeatedly. On a gated node the practical impact is nil — it accepts no writes, so its
log is not growing. Severity: delayed GC, never wrong GC. This is an efficiency fix, not a
correctness one, and it should not be described as closing a correctness gap.

Fixing it here rather than ledgering it is deliberate: this interlude is what makes it
repeat indefinitely, and the fix is three lines in the function the hazard lives in.

## 5. Recording witnesses while verifying is unchanged (0b re-review M-4)

A `Verifying` node keeps recording new witnesses. **No code change** — this section exists
so the behaviour is documented rather than merely true.

It is safety-monotone. `probes` is built once at `Repl::open`, so a witness recorded during
verification cannot affect the current gate; it can only make the **next** boot's gate
stricter, never looser. With §2 in place a newcomer cannot be recorded at all, since it is
refused before `listen` is assigned. The only newly-recordable peer is a positioned follower
this node had not yet recorded — a genuine follower relationship worth remembering.

## 6. Non-goals

- **No wire change, no proto bump, no new durable state.** The witness set, the role file,
  and the `Hello` shape are all untouched.
- **`ReplConfig::strict_witnesses` semantics unchanged.** It governs what happens at
  `WITNESS_DEADLINE_NANOS`; §1 governs what the node may do meanwhile, and the two do not
  interact.
- **The refusal never lapses** (the ruling). Admitting newcomers after the deadline would
  make the guarantee time-dependent rather than evidence-dependent, and manufacturing a
  durable replica on a timer is the exact harm the gate exists to prevent. The availability
  cost is near-zero in any case: a gated node already refuses writes, so the only use for a
  newly-seeded follower is promotion — which is precisely the harm.
- **Not "seed but propagate the gate"** (make the newcomer boot `Verifying` too). It
  preserves availability and is coherent, but needs a wire field and new durable state on
  the newcomer. If it is ever wanted it is a sub-project, not an interlude.
- **Design C (a durable timeline epoch on `Hello`)** stays ledgered and stays compatible —
  it would sharpen the refusal, not replace it.
- **`Follower::step`'s bare `?` on decode** (the asymmetry recorded by the 2026-08-06
  hard-rule-5 interlude) is a separate open follow-up and is not reopened here.

## 7. The operator story

A gated node that refuses a newcomer must say why in the refusal itself, and must point at
the remedy — GUIDE §10's witness-set paragraph — the way the strict refusal and the deadline
notice already do (0b re-review M-5 noted the parked refusal named a fix that might never
arrive). GUIDE §10 gains the newcomer case alongside its existing three probe outcomes:
while a node is verifying, an existing follower catches up normally and a returning recorded
witness re-seeds normally, but a brand-new follower is refused until verification finishes —
and under a permanently dead witness that means the existing escape hatch (editing or
deleting the `witnesses` file, with its stated gap-reopening warning) is the only way
forward. Hard rule 6: DESIGN §5 and GUIDE §10 land in the same commits as the behaviour.

## 8. Tests

RED-first per hard rule 3, seeds continuing the 0b block at `0x2EB1_011B`+. Anything that
passes on arrival has its teeth proven by mutation instead, per the 2026-08-06 interlude's
precedent.

- **Headline:** a gated node refuses to seed a genuine newcomer, rigged with
  `log_base() == 0` so it lands on the `Frames { from: 0 }` arm — the arm a variant-keyed
  refusal misses. Asserted on the **gate outcome** (the newcomer holds nothing, no seed
  happened) rather than on an error shape. The parked test asserted a shape the correct fix
  removes and took 771s to reach its budget wall; this one must not.
- **The regression the parked attempt caused, as its own test:** a recorded witness
  returning with a rebuilt store still seeds and converges. Plus
  `the_kernel_completes_its_skipped_startup_work_when_the_witness_gate_opens`,
  `follower_converges_to_the_primary_fingerprint` (`0xcea9328519df4234`),
  `failover_preserves_everything_the_follower_acked` (`0xa84f22d1913dc2d1`),
  `switchover_mid_write_load_loses_nothing_and_serves_reads_throughout`, and auth's
  `raw_credentials_never_touch_the_durable_store` (`0xde584e99fba70883`) all staying green —
  the measured blast radius of the parked attempt, and the acceptance bar for this one.
- **Retryability at both follower sites:** a refused newcomer backs off and retries rather
  than dying, and converges once the gate opens. `follower.rs:233` and `:295` need separate
  coverage; the parked attempt's probe patched only one.
- **`probe.rs` unit test:** the new refusal classifies `Inconclusive`, alongside the
  existing classifier cases.
- **GC pin:** a peer refused repeatedly across steps never leaves the pin at 0.
- **Positioned peers are unaffected while verifying** — ordinary catch-up still ships.

## 9. Evidence bar

Sub-project 0b's bar, unchanged, because replication, auth and realtime are all in the
measured blast radius: `cargo fmt --all --check`; `cargo clippy --workspace --all-targets --
-D warnings`; `cargo build -p guide-examples`; `cargo test --workspace` twice with identical
counts; the pinned debug replays `0x8bee1752811ae873` and `0x348270b38f166529`; both release
mode-skew tripwires exactly as `ci.yml` runs them; a fresh invoicing 1024-universe batch with
its master seed recorded; and `scripts/edge-harness.sh`, since this changes replication
behaviour. Counts captured via `tee`, never `tail`. Note the edge crate's ignored tests live
in `src/` as well as `tests/`.
