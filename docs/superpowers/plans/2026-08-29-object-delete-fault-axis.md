# Plan: `object_delete_error` — the fault axis reclamation shipped without

**Status:** not started. **Sequencing: AFTER the `archive` branch merges to `main`** — see
"When to run this" below; do not start it on `archive`.

## Why this exists

Task 12.5 of the object-storage archive plan shipped `ObjectOp::Delete`, the archive's only
destructive operation and the only thing in charpente that can remove an object a restore
needs. It shipped **with no delete-fault axis in the simulator.**

That is a hard-rule-3 debt, not a nice-to-have. CLAUDE.md: *"Every core subsystem lands with
seeded fault-injection tests in the same PR."* MANIFESTO §6 makes fault injection *"the
default for ordinary application code, not a heroic discipline for database teams"*, and §9
states the whole wager as *"every path through this system has been executed, under fault,
thousands of times, and here is the seed to prove it."*

For the delete path there is no seed. `SimObjectStore`'s `Delete` arm takes no roll and
`FaultConfig` has no delete field, so **every delete in all 256 universes on all four pinned
bases succeeds**. Task 12.5's central safety claim — *the archive stays correct and keeps
advancing when every delete fails forever* — rests on two hand-driven modes in one unit test,
standing exactly where §6 says deterministic simulation is supposed to stand.

The archive plan itself already made this argument once. It sited Task 12.5 after Task 12 with
the reasoning: *"Deletion must not ship before the convergence property proves that what it
would delete is recoverable; ordering it ahead of its own oracle is the correctness bet this
plan exists to avoid."* It proved recoverability. It did not prove delete-failure resilience.
Same principle, unfinished.

**The counter-argument, recorded so nobody has to re-derive it.** The archive lives in
`crates/charpente`, not `charpente-core`, and DESIGN §5 treats it as a derived artifact that
must not have power over the thing it derives from — so "core subsystem" arguably does not
reach it. That is rejected *here specifically* because reclamation deletes the objects a
restore needs: it is the one place the derived artifact can destroy what it derives from, so
it is where the bar should be highest rather than lowest.

## The finding that makes this cheap, and why the recorded cost was wrong

The Task 12.5 re-review recorded this axis as "owed at the next seed-map break", and the
controller reported to Noah that adding it would re-roll every pinned universe and invalidate
the three floors and both recorded seeds. **Both statements name the wrong obstacle.**

- It is **not** the seed -> config mapping. `sample_extended_fields`
  (`charpente-sim/src/lib.rs:~315-337`) draws each axis from its own
  `keyed_stream(seed, name)`, so a new *field* survives it untouched.
- It is **not** `Rng::chance`'s eager draw, either — not once the streams are split.
  `chance` does call `next_u64()` before comparing (`rng.rs:59`), so it consumes a draw even
  at `p == 0.0`. That only matters because of the next point.
- It **is** that `SimObjectStore` draws every roll — put, get, lost_ack — from ONE stream,
  `keyed_stream(seed, "object_store")` (`object.rs:58,68`, via `roll()` at `:80`). Adding a
  `Delete` roll to that stream shifts every later draw in it, and *that* is what re-rolls
  every universe.

**So the fix is a stream split, and the idiom is already in this file.**
`keyed_stream(seed, name)` is `seed ^ FNV-1a(name)` expanded through splitmix64 into a fresh
xoshiro256++ state (`lib.rs:346`), and its own doc states the purpose: streams that *"never
perturb the frozen global streams."* The crate already applies it at the subsystem level
(object_store vs disk vs net) and per host (`faults`/`entropy`). Apply it one level down:

> The `Delete` arm draws from `keyed_stream(seed, "object_store_delete")`. Put, get and
> lost_ack keep drawing, in the same order, from `keyed_stream(seed, "object_store")`.

Then with `object_delete_error == 0.0` **every existing universe is byte-identical**, the
three pinned floors and both recorded seeds survive, and `chance`'s eager draw is harmless
because it lands in a stream nothing else reads.

This is the third time on the archive plan that the answer was an idiom already in the tree
(Task 11 I-2 and Task 12 H-1 were the others). Assume it will be the answer again before
assuming it is not.

## When to run this

**After `archive` merges to `main`.** The convergence property, its four-base table and the
three floors all live in `crates/charpente/tests/archive.rs` on `archive`; the axis lives in
`crates/charpente-sim` on both. As of 2026-08-29 `archive` is 86 commits ahead of the
merge-base and `main` 62, with a live CLAUDE.md conflict already queued. Running this
pre-merge widens a divergence that is already the largest risk on the branch, for no benefit.

## Scope

**In:** the delete fault axis, its stream, its config sampling, turning it on in the
convergence property, and the coverage that buys.

**Out:** changing `Rng::chance` to skip the draw at `p == 0.0`. It is a real sharp edge and it
touches every axis in the crate; the stream split makes it irrelevant here. Note it, do not fix
it in this plan.

**Out:** leases or fencing for the shared-`writer` deletion window (archive spec §4). That is a
design change and remains explicitly unbuilt.

---

## Task 1: the non-perturbing split

**Files:** `crates/charpente-sim/src/lib.rs`, `crates/charpente-sim/src/object.rs`

- [ ] **Step 1: write the failing test.** The load-bearing one is an **identity** test, and it
      must exist before the field does: with the axis at `0.0`, every coverage count on all
      four bases equals the values recorded today in `archive.rs`'s four-base table
      (`~:6926`). Write it against today's numbers, so it can only pass by being true.
- [ ] **Step 2: run to verify failure.**
- [ ] **Step 3: implement.** `object_delete_error: f64` on `FaultConfig` (`lib.rs:34`),
      `0.0` in **both** constructors (`~:117-119` and `~:141-143` — the second is `none()`);
      sampled in `sample_extended_fields` from `keyed_stream(seed, "object_delete_error")`,
      appended AFTER the existing axes; a second roll stream on `SimObjectStore`,
      `keyed_stream(seed, "object_store_delete")`, read ONLY by the `Delete` arm.
- [ ] **Step 4: run to verify pass.**
- [ ] **Step 5: mutation table.**

| # | mutation | must redden |
|---|---|---|
| 1 | `Delete` rolls from the shared `"object_store"` stream | the four-base identity test |
| 2 | put/get moved to the `"object_store_delete"` stream | the four-base identity test |
| 3 | the axis defaults to non-zero in either constructor | the four-base identity test |
| 4 | `sample_extended_fields` draws the new axis from the sequential stream | the four-base identity test |

Row 1 is the one that matters: it is the obvious "simplification" a later agent will make, and
it silently re-rolls every universe. If the identity test cannot see it, the test is wrong.

- [ ] **Step 6: gate and commit.** Named paths. State in the commit message that replay
      identity was proven, and on which four bases.

## Task 2: turn it on, scoped to interleaving

**Files:** `crates/charpente/tests/archive.rs`, `crates/charpente-sim/src/object.rs`

`delete` is contractually idempotent, so *"can a delete fail"* is the weak question and a
happy-path-plus-error-return axis would buy little. **The question worth a seed is
interleaving**: a delete failing *mid-drain*, alongside put/get faults, under a gap or a crash
— which is precisely the path that creates the third orphan class (interrupted-drain residue,
archive spec §3) that today prints nothing and has no oracle.

- [ ] **Step 1: write the failing tests.** At minimum: the archive returns to `Idle` and
      re-holds its pin at the new floor when deletes fail mid-drain; the live generation never
      loses an object to a failed-delete path; a restore still succeeds after any interleaving
      of delete failures with a gap.
- [ ] **Step 2: run to verify failure.**
- [ ] **Step 3: enable the axis** in the convergence property's fault matrix.
- [ ] **Step 4: run.** Any defect found gets its **seed recorded** (hard rule 3). Expect the
      three floors to move: reclamation changes every universe's op trace once deletes can
      fail. **A floor moving DOWN is how a real regression looks** — check it the way the
      Task 12.5 review did, by running the before-tree, not by arguing about it.
- [ ] **Step 5: re-measure and record** all three floors plus per-key hit counts across four
      bases, and update the four-base table in the tree (not in a report that gets deleted).
- [ ] **Step 6: mutation table**, including a vacuity row: with the axis forced to `0.0`, the
      new interleaving assertions must go GREEN-but-unfired, and say so at the site rather than
      counting as coverage.
- [ ] **Step 7: gate and commit**, stating N universes and the master seed.

## Task 3: docs — hard rule 6, same commits

- [ ] `charpente-sim/src/object.rs`'s `Delete` arm carries a comment recording that the axis is
      absent and owed. **Delete it** — this plan is what it was owed against. Leaving it is a
      false claim about the tree, which is the exact defect class the archive plan spent three
      review rounds removing.
- [ ] `DESIGN.md` §1's honest-limits paragraph names `ObjectOp::Delete` as having no axis.
      Update it. Note the *other* limit stated there — `SimObjectStore` completes in the `poll`
      that takes the op, so object-store latency is unmodelled — is still true; do not delete
      that half.
- [ ] Archive spec §3 and §9: the orphan-class paragraph says the interrupted-drain class is
      reachable only by the refused LIST sweep. If Task 2 gives it an oracle in sim, that
      sentence needs narrowing to *reclamation* cannot reach it, which is the true claim.
- [ ] `PLAN.md`: the object-storage entry states the missing axis as a live limit. Update it.

## Exit criteria

- [ ] `object_delete_error` in `FaultConfig`, sampled per-axis, rolled from its own stream.
- [ ] **Replay identity proven** with the axis at `0.0`: all coverage counts on all four bases
      identical to the pre-change table. Stated in the commit message.
- [ ] The three pinned floors and both Task 12.5 seeds (`0xfadf45b42f89ce91`,
      `0x71cec6ac14e0525a`) still reproduce after Task 1.
- [ ] The convergence property exercises delete failure interleaved with put/get faults and
      gaps; N universes and the master seed stated in the commit.
- [ ] Every mutation table run, RED confirmed, recorded in its commit message.
- [ ] `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
      `cargo test --workspace` green.
- [ ] The `Delete` arm's owed-axis comment is gone and DESIGN §1, spec §3/§9 and PLAN.md say
      what is now true.

## Open items

1. **`Rng::chance` draws at `p == 0.0`.** Out of scope here and harmless once streams are
   split, but it means any FUTURE axis added to an EXISTING shared stream re-rolls that
   stream's universes. Worth a numbered DECISIONS entry or a comment at `rng.rs:53`; Noah's.
2. **Whether `delete` should stay idempotent in `ObjectStore`'s contract.** `FsObjectStore`
   maps `NotFound` to `Ok`, which is what made the Task 12.5 delete loop silent. Not reopened
   by this plan; named because the axis will make its consequences observable for the first
   time.
