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
3. **Typed detection at the boundary.** Not a prose match on redb's `Display`
   text. See §1.
4. **Fix both shapes; measure only the cheap one.** The read paths are in the
   flag-setting set, so the fix covers the larger-than-cache shape by
   construction — but no store over redb's 1 GiB bound will be built to prove the
   symptom. That row stays labelled **DERIVED** in every document that carries
   it. Its arm is justified on redb's verified source, not on a measurement, and
   this spec says so at the site.
5. **The cause stays distinct from apply-poison.** See §3.

## 1. Detection: typed at the boundary

`redb::StorageError::PreviousIo` is a typed variant
(`redb-4.1.0/src/error.rs:16`), as is `redb::Error::PreviousIo` (`:559`). T13's
proposal reached for a prose match only because charpente destroys the type at
its own boundary: `redb_err` (`crates/charpente-core/src/table.rs:283-285`) is
generic over `impl fmt::Display` and flattens every redb error into
`io::Error::other(format!("table store (redb): {e}"))`.

The fix is to stop destroying it.

- Introduce a private trait — `IsPreviousIo`, one method returning `bool` —
  implemented for each redb error type `redb_err` actually receives. **The impl
  set is enumerated by the compiler, not guessed here:** changing `redb_err`'s
  bound makes every call site that passes an un-impl'd type a compile error, and
  the implementation task adds exactly those impls. (`redb::TableError` is the
  only one named explicitly in charpente's source today; the rest arrive through
  inference.)
- `redb_err`'s body builds a private `StoreError { previous_io: bool, msg:
  String }` implementing `std::error::Error`, wrapped via `io::Error::other`.
  Callers recover the flag by `io::Error::get_ref` + `downcast_ref::<StoreError>`.
- **Every `map_err(redb_err)` call site stays byte-identical.** Only the
  function's bound and body change.

`StoreError`'s `Display` must reproduce today's `"table store (redb): {e}"`
string exactly. One site already depends on that prefix
(`table.rs:314`, the open-time panic conversion), and the sim's error-classification
surface reads these strings.

**What this does not do.** `checks::classify`
(`crates/charpente-sim/src/checks.rs:72`) matches redb's latch prose today and
will continue to. It classifies message *strings* by design and has no error
value to inspect. This spec does not remove that coupling; it avoids adding a
second one on the product path. Anyone reading CLAUDE.md's note about
prose-coupling liability should understand the two sites are different in kind.

## 2. Stickiness: where the flag lives

On `Store`, not on `Db`.

`Store::apply`, `checkpoint`, and `snapshot` all take `&self`, so the flag is an
`AtomicBool` (interior mutability is required, and an atomic bool is not a
nondeterminism source — the kernel is single-threaded and the deny-list does not
reach it). Exposed as `Store::failed()`.

Placing it on `Store` rather than `Db` is what makes the read-path arm work:
`Snapshot` and `Writer` hold the same handle, so their reads set the same flag
without a second mechanism.

Concretely the flag is an `Arc<AtomicBool>` owned by `Store` and cloned into
`Snapshot` and `Writer` when they are constructed, so all three share one latch
state.

**Sites that set it: all of them, set inside `charpente-core::table`** — not at
the `Db` layer, which owns neither the handle nor the flag and cannot see
`Snapshot`/`Writer` reads at all. Each `Store`/`Snapshot`/`Writer` method funnels
its `io::Result` through one small `note()` helper that sets the flag when the
error downcasts to `previous_io`. The set covers `checkpoint`, `apply`,
`snapshot`, `import`, `gc_events`, `set_import_meta`, `applied_seq`, and the
`Snapshot` readers (`scan_raw`, `scan_raw_from`, `next_job_id`, `next_event_id`,
`seed_frontier`).

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
- **A read-path test.** Proves `snapshot`/`scan` set the flag with no write
  involved. This is the arm with no measurement behind it (§ Scope ruling 4), so
  it carries that arm's weight on its own.
- **A negative test.** A non-latch store error must **not** set the flag —
  guarding against silently degrading into "any store error is sticky", the
  option considered and rejected in brainstorming. Without this, the typed
  detection could rot into the broad one and no test would notice.
- **An anti-vacuity check** on the read-path test, in the style the existing
  tripwire already uses: prove the injector is attached and the store read
  actually reaches the `Disk` boundary, so the test cannot pass trivially.

## 7. What changes where

| File | Change |
|---|---|
| `crates/charpente-core/src/table.rs` | `IsPreviousIo` trait + impls; `redb_err` body → `StoreError`; the shared `Arc<AtomicBool>` + `Store::failed()`; the `note()` helper on every `Store`/`Snapshot`/`Writer` result |
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
- Reworking redb's error handling beyond the single `redb_err` boundary.

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
