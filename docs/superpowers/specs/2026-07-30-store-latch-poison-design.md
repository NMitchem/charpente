# M2 sub-project 0a — the checkpoint-latch gap: design

*Closes the first of the two gaps carried out of M1 (CLAUDE.md's "M2 follow-up
backlog"), characterized but deliberately not fixed by sub-project 6's T13
(proposal 1 of `.superpowers/sdd/2026-07-24-introspection-edge-harness/task-13-report.md`).*

## Context

charpente has two independent "this storage is broken" flags and knows about
only one of them.

Ours is `Db::poisoned()` (`crates/charpente-core/src/db.rs:1638-1641`,
delegating to `engine.rs:206-210`), set when a durable record fails to apply or
the WAL's fsync fails. redb has its own: any error from the storage backend sets
`CheckedBackend.io_failed` (`redb-4.1.0/src/tree_store/page_store/cached_file.rs:139`),
after which every backend operation returns `StorageError::PreviousIo` —
"Previous I/O error occurred. Please close and re-open the database."

Nothing maps redb's latch onto ours. Two mechanisms reach it on a live handle:

- **The store's checkpoint fsync** — always available, because `Store::apply`
  commits at `Durability::None`, so the checkpoint's is the *only* store sync.
- **A failed store read** — but only on a store whose working set exceeds redb's
  evicting 1 GiB read cache; a smaller one is served from memory and never
  touches the disk boundary after open.

Common to both shapes: the write whose checkpoint failed gets a 500 while its
record *is* durable in the WAL; `Db::poisoned()` is **false**; the kernel keeps
serving; and the node can no longer checkpoint, so WAL GC and the `.horizon`
breadcrumb are both frozen.

The two shapes do not have the same standing, and this spec does not pretend
otherwise:

- **Cache-resident store — MEASURED.** Completely silent: reads answered 200 off
  redb's page cache, no 500, no log line, nothing on the wire. The window is one
  write wide (the next write reaches the latch-gated `begin_write`,
  `redb-4.1.0/src/db.rs:1410`, poisons apply-side, and stops the kernel) — except
  on a node taking no writes, which stays in the window indefinitely, reporting
  healthy forever.
- **Store larger than the read cache — DERIVED, not measured.** Cache-missing
  reads 500 while every platform health signal still says fine. Here the window
  **may never close**: `begin_read` is *not* latch-gated (verified in both redb
  impls, `db.rs:404` and `db.rs:508`), so a read-first write transaction — the
  ordinary get-then-put shape — fails inside the handler, returns a plain 500,
  and never reaches `begin_write`. No append, no apply, no poison.

Verified **not** a disk-exhaustion hazard in either shape: after the latch the
WAL takes at most two more records and is then frozen.

**This is not new policy.** `DESIGN.md:212-214` already states the rule — "Any IO
failure poisons the log — fsync results cannot honestly be retried (fsyncgate);
the only exit from poison is restart-and-recover." The store latch is exactly an
un-retryable IO failure; redb says so in its own error text. The gap is that the
rule was never wired to the store boundary.

## Scope (Noah rulings, 2026-07-30)

1. **Two specs, latch first.** This gap and the dual-primary gap are independent
   in code, risk, and size. The dual-primary gap gets its own spec → plan →
   implementation cycle after this one; nothing in it is in scope here.
2. **Policy (a): stop.** A latched store poisons the node. The alternative —
   keep serving reads behind a `Db::degraded()` — was considered and rejected:
   MANIFESTO §8 ("optional guarantees are no guarantees") makes a node that
   reports healthy while structurally unable to make progress the silent-wrongness
   class this project exists to delete.
3. **Detection at charpente's own disk boundary** (amended 2026-07-30, before
   implementation — the original ruling was "typed detection at the `redb_err`
   boundary", superseded once `CheckedBackend`'s set-condition was read; the
   discarded alternative and the reason are kept in §1 so the amendment is not
   re-litigated). Not a prose match on redb's `Display` text either way.
4. **Fix both shapes; measure only the cheap one.** The read paths are in the
   flag-setting set, so the fix covers the larger-than-cache shape by
   construction — but no store over redb's 1 GiB bound will be built to prove the
   symptom. That row stays labelled **DERIVED** in every document that carries
   it. Its arm is justified on redb's verified source, not on a measurement, and
   this spec says so at the site.
5. **The cause stays distinct from apply-poison.** See §3.

## 1. Detection: charpente's own disk boundary

redb's latch is set in exactly one place and on exactly one condition.
`CheckedBackend` (`redb-4.1.0/src/tree_store/page_store/cached_file.rs:118-198`)
wraps a `Box<dyn StorageBackend>` and sets `io_failed` when **that inner backend
returns `Err`** — five sites, one per method: `len` (`:158`), `read` (`:167`),
`set_len` (`:176`), `sync_data` (`:185`), `write` (`:194`). The only other write
to `io_failed` is `close()` (`:148`), which sets it unconditionally as part of a
deliberate graceful close.

The inner backend **is charpente's own `DiskBackend`**
(`crates/charpente-core/src/table.rs:938-970`), whose `StorageBackend` impl is
five one-line delegations to `File`, in one-to-one correspondence with redb's
five latch sites.

So charpente does not need to *detect* redb's latch downstream. It can **observe
the identical condition at the same instant, one layer down**: set a sticky flag
in each of `DiskBackend`'s five methods when the inner `File` call returns `Err`.
`close` is deliberately **not** mirrored — a graceful close is not a failure.

**What this deletes** versus the originally-ruled typed-detection design: no
`IsPreviousIo` trait, no `StoreError` wrapper type, no `downcast_ref`, no change
to any of the ~89 `map_err(redb_err)` call sites, and — the largest saving — **no
flag threading into `Snapshot` or `Writer` at all.** A failed store read *is*
`DiskBackend::read` returning `Err`, so the read-path arm that Scope ruling 4
depends on is covered at the source. `Snapshot` and `Writer` are untouched by
this spec.

**The trade, stated so it is not rediscovered later.** This replaces a *typed*
coupling with a *structural* one. Typed `PreviousIo` detection would catch a
latch however it arose; the backend flag catches it only where redb's condition
is "the backend errored." Those are the same set in redb 4.1.0 — verified above,
`close()` being the sole exception and one we intentionally exclude — but a
future redb that latched on some internal consistency check would leave our flag
silently under-reporting.

That risk is **pinned in one direction and open in the other — state it
exactly.** The tripwire test (§6) builds a genuinely latched store and asserts
both flags: charpente's mirror, and redb's own refusal. But both observations
ride the checkpoint-fsync path, which is a path `DiskBackend` *does* observe.
So the test catches drift on that path either way — including the
over-approximation, where a redb that stopped latching there would otherwise
leave charpente poisoning a healthy node with the suite still green.

What it does **not** catch is the risk named in the paragraph directly above:
a future redb latching on some internal consistency check, through a path
`DiskBackend` never sees, leaves our flag silently under-reporting and this
test green. That direction is not mechanically pinned by anything; it is a
redb-upgrade review item against the five-site census.

**What this does not do.** `checks::classify`
(`crates/charpente-sim/src/checks.rs:72`) matches redb's latch prose today and
will continue to. It classifies message *strings* by design and has no error
value to inspect. This spec does not remove that coupling — and, as shipped, it
adds a second, deliberate one: the kernel's store-latch stop message carries
redb's own remedy prose verbatim so `classify` keeps recognising it. That was
forced, not chosen — §8 makes touching `classify` a non-goal, so the coupling
had to be satisfied from the message side; the plan's paraphrase hit none of
`classify`'s four needles and misclassified every fault-caused latch stop as
`Unrelated`. The site is guarded by a rationale comment and an
`is_fault_fallout` assertion in the tripwire. Anyone reading CLAUDE.md's note
about prose-coupling liability should understand that these sites are different
in kind from a typed check, and that there are now two of them.

## 2. Stickiness: where the flag lives

An `Arc<AtomicBool>`, created in `Store::open`, held by both `DiskBackend` (which
sets it) and `Store` (which reads it), exposed as `Store::failed()`.

An atomic bool is required because `DiskBackend`'s `StorageBackend` methods take
`&self`, and it is not a nondeterminism source — the kernel is single-threaded
and the deny-list does not reach it.

The flag is **sticky and never cleared**, matching redb: once latched,
`check_failure` (`cached_file.rs:134-144`) short-circuits before ever calling the
backend again, so the two states cannot drift apart afterwards.

`Db`'s only role is to read `Store::failed()` and OR it into `poisoned()` (§3).

**The read paths are in the set deliberately.** That is the entire reason the fix
covers the larger-than-cache shape, where a read-first write transaction fails in
the handler and never reaches the latch-gated `begin_write`. Without the read
arm, such a node 500s indefinitely with `poisoned()` false — the state the fix
exists to end.

Accepted and named: this creates a **new stop site on a read-only node's snapshot
path**, which the 2026-07-05 step-7 ruling never contemplated. Traced through,
the arm is narrower than it sounds — on a follower the next shipped record's
apply hits the latch and poisons anyway, so the arm only moves an already-certain
stop earlier; on an idle node nothing advances either way. The one genuinely new
stop is a node serving pure reads with no writes and no replication traffic,
which today serves correct-but-permanently-frozen data while reporting healthy.
Converting that into "stopped, restart me" is the point of the change, not a cost
of it.

## 3. Policy: the stop, and why the cause stays distinct

`Db::poisoned()` ORs in the store flag. The kernel's request arm already stops on
`poisoned()` (`crates/charpente/src/kernel/mod.rs:304-306`) per the 2026-07-05
step-7 ruling, so **the stop requires no new kernel control flow** — it inherits
the existing one.

But the two causes must not merge into one indistinguishable bool.

`poisoned()` today means *"a durable record failed to apply, so machine state no
longer matches the log — restart and recover."* A store latch means something
materially different: **the durable state is intact.** The WAL is fine, the
records are durable, only the handle is dead. Both remedies are "restart the
process," so the OR is operationally sound — but a merged flag would make the
stop message tell an operator their state might be inconsistent when it
demonstrably is not.

Hard rule 5 requires the message name the contract, the site, and the fix.
Therefore the cause is carried as a distinct variant, and the kernel emits a
distinct message for the store-latch case — naming that durable state is intact
and that the remedy is a process restart, which is what redb's own error text
asks for.

## 4. Blast radius

`crates/charpente/tests/replication.rs:526` and `:682` currently reach the latch
and **recover** from it (via `recover(sim, node)` — a restart). Under this fix
that store is poisoned and the node stops.

These tests encode today's behavior, so they are expected to change. They must be
**re-read, not blanket-updated**: they already respond to the latch with a
restart, which is the remedy this fix makes structural, so the likely outcome is
that they survive largely intact. If either turns out to encode a genuine
recovery path worth keeping, that is a finding against ruling 2 and must come
back to Noah rather than be edited to match the new behavior.

## 5. Error surface (named, pinned verbatim — hard rule 5)

Two strings, each pinned byte-exact by its own test:

1. **The kernel stop message** for a store-latched node — distinct from the
   apply-poison message, naming that durable state is intact and that the remedy
   is a process restart.
2. **The 500 body** returned to the client for the write whose checkpoint hit the
   latch. Today's wording already names the injected fault honestly; this spec
   does not change it, but the test that pins it must survive the behavior change
   in §4.

Exact wording is chosen during implementation and pinned there; no string is
quoted in this spec that is not also pinned by a test, per the sub-project 4
convention.

## 6. Testing strategy

RED first, per hard rule 3 — no fix lands without a test proven red against
unfixed code.

- **The tripwire.** `kernel_keeps_serving_with_a_latched_store_known_gap`
  (`crates/charpente/tests/kernel.rs:770`, seed `0x0DED_0002`) is **updated,
  never deleted**. Its `replies.len() == 3` assertion reddens *before* the
  `!poisoned()` headline; that earlier failure is the fix working, not a second
  break. CLAUDE.md's backlog entry already warns a reader about exactly this
  ordering, and the entry is retired as part of this work.
- **The correspondence assertion** (new under the amended §1). The tripwire
  builds a genuinely latched store, so it must assert that charpente's flag is
  set *there* — that is what pins the structural coupling §1 accepts. If a future
  redb latches through a path `DiskBackend` does not observe, this assertion is
  what goes red.
- **A read-path test.** Proves a failed store **read** sets the flag with no
  write involved — the `DiskBackend::read` arm. This is the arm with no
  measurement behind it (§ Scope ruling 4), so it carries that arm's weight on
  its own.
- **A negative test.** Note the amended §1 changes what this must assert. Every
  *disk-boundary* error now sets the flag — that is not over-breadth, it is
  exactly redb's own condition. What must **not** set it is a store error that
  never reached the disk boundary: a row-decode failure, or
  `TableError::TableDoesNotExist`, both of which `Store` already handles as
  ordinary results. Without this test, the flag could drift into "any error
  whatsoever is fatal" and nothing would notice.
- **A graceful-close test.** `close` is deliberately not mirrored (§1). A normal
  `Database::drop` must leave the flag clear, so a clean shutdown is never
  reported as a latch.
- **An anti-vacuity check** on the read-path test, in the style the existing
  tripwire already uses: prove the injector is attached and the store read
  actually reaches the `Disk` boundary, so the test cannot pass trivially.

## 7. What changes where

| File | Change |
|---|---|
| `crates/charpente-core/src/table.rs` | The shared `Arc<AtomicBool>` created in `Store::open`; a field on `DiskBackend` and on `Store`; five `Err`-observing edits in the `StorageBackend` impl; `Store::failed()`. `redb_err`, `Snapshot`, and `Writer` are untouched |
| `crates/charpente-core/src/db.rs` | Distinct cause variant; `poisoned()` ORs in `Store::failed()` |
| `crates/charpente/src/kernel/mod.rs` | Distinct stop message for the store-latch cause |
| `crates/charpente/tests/kernel.rs` | Tripwire updated; read-path and negative tests added |
| `crates/charpente/tests/replication.rs` | Re-read per §4 |
| `DESIGN.md` §4 | The store boundary now follows the §2 fsyncgate rule; the two shapes and their standing |
| `docs/GUIDE.md` | Operator surface: what a store-latch stop means and why a restart is the remedy |
| `CLAUDE.md` | Retire the checkpoint-latch backlog entry |

## 8. Non-goals

- Measuring the larger-than-1-GiB-read-cache shape. It stays **DERIVED**.
- Touching `checks::classify`'s prose match (§1).
- Anything in the dual-primary gap.
- A `Db::degraded()` predicate, or any read-serving-while-latched mode — rejected
  by ruling 2.
- Any change to `redb_err`, the ~89 `map_err(redb_err)` call sites, `Snapshot`,
  or `Writer` — the amended §1 needs none of them.

## 9. Evidence bar

Per CLAUDE.md's workflow rule, a core change's bar is "sim suite green over N
fresh seeds", not "unit tests pass":

- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo build -p guide-examples` (the GUIDE compile gate) — all clean.
- `cargo test --workspace` green **twice**.
- The pinned debug replays: `CHARPENTE_SEED=0x8bee1752811ae873` (charpente-sim
  crash_recovery) and `0x348270b38f166529` (charpente kernel).
- Both release mode-skew tripwires exactly as `ci.yml` runs them.
- A fresh invoicing 1024-universe batch, master seed recorded.
- Every new and updated test proven RED against unfixed code, with the proof
  recorded in the ledger.
- The `#[ignore]`d edge matrix (`scripts/edge-harness.sh`) green — this fix does
  not target it, but the replication behavior change in §4 makes it worth running
  once.
