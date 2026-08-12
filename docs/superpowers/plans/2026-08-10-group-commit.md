# Primary-side Group Commit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Commit N transactions under one fsync on the primary, without weakening
ack-after-durable, closing the one-fsync-per-transaction ceiling BENCH.md measured at 215
log records/sec.

**Architecture:** `Db::transact` splits into `stage` (run the handler, buffer its effects
record) and `commit` (append all, ONE sync, apply all in seq order). Reads inside a staged
transaction go through a two-level overlay — the transaction's own writes on top of the
group's accumulated writes on top of one shared store snapshot — so apply stays strictly
after sync and every existing acked-durability oracle keeps working unchanged. The kernel's
Request, Job and Cron arms each drain what is ready into one group; arms never mix.

**Tech Stack:** Rust, `charpente-core` (`db.rs`, `engine.rs`), `charpente`
(`kernel/mod.rs`, `http`), `charpente-sim`, `bench-harness`.

**Spec:** `docs/superpowers/specs/2026-08-10-group-commit-design.md`. Read it before Task 1.
Section references below (§1…§11) are to that spec.

## Global Constraints

- **Hard rule 1**: no ambient nondeterminism in the core. Batch composition reads no clock,
  no entropy, no wall time. It is a pure function of the ready set.
- **Hard rule 2**: no escape hatches. There is no "batching off" switch and no second commit
  path that runs only sometimes.
- **Hard rule 3**: RED-first. Every new test is proven to fail before the code that makes it
  pass. Every sim bug gets its seed pinned.
- **Hard rule 4**: assertions on in release. Negative-space asserts stay.
- **Hard rule 5**: error messages name the contract, the site, and the fix.
- **Hard rule 6**: public-API changes update DESIGN/GUIDE in the same commit.
- **`MAX_BATCH: usize = 64`** — a `const`, never a `DbConfig` field (§3).
- **Apply strictly after sync**, always. If a task seems to need otherwise, stop (§Scope 1).
- Commit directly to `main`, no Co-Authored-By trailers.
- Every task ends green on `cargo fmt --all --check` and
  `cargo clippy --workspace --all-targets -- -D warnings`.

## File Structure

| file | responsibility after this plan |
|---|---|
| `crates/bench-harness/src/measure.rs` | gate compares **syncs/sec** to the device ceiling (T1) |
| `crates/bench-harness/src/{core,kernel,wire}_accounting.rs` | carry a `syncs` observation (T1) |
| `crates/charpente-core/src/engine.rs` | `submit_all` is the one append/sync/apply core; `submit` is its length-1 caller (T2) |
| `crates/charpente-core/src/db.rs` | `Tx<'a>` borrows its snapshot (T3); `Group`/`stage`/`commit` (T4) |
| `crates/charpente/src/http/mod.rs` (or `App`'s dispatch site) | staged dispatch returning a deferred reply (T5) |
| `crates/charpente/src/kernel/mod.rs` | Request/Job/Cron arms drain into a group (T6, T7) |
| `crates/charpente/tests/group_commit.rs` | new property suite (**T5 creates it** + its counting-disk/trace harness; T6, T8, T9 extend it) |
| `crates/charpente/tests/fixtures/pre-batching-trace.txt` | tracked fixture: the pre-batching disk trace, captured at T5, asserted by T6 |
| `docs/BENCH.md`, `DESIGN.md`, `docs/GUIDE.md` | method, commit path, 500-may-mean-committed (T11) |

---

### Task 1: Correct the device-ratio gate to syncs/sec — BEFORE any batching code

**Why first (§5):** the gate exists to catch acking-without-syncing, whose signature its own
comment puts at "2x or beyond" — the same shape as a legitimate group-commit win. It must be
corrected and proven against **non-batching** code, where the true ratio is known to be 1.0,
so the instrument is trustworthy before it scores anything.

**Files:**
- Modify: `crates/bench-harness/src/measure.rs`
- Modify: `crates/bench-harness/src/core_accounting.rs`
- Modify: `crates/bench-harness/src/kernel_accounting.rs`
- Modify: `crates/bench-harness/src/wire_accounting.rs`

**Interfaces:**
- Produces: a `syncs: u64` field on `WriteObs`, `KernelObs`, `WireObs`; the gate input
  becomes `syncs as f64 / elapsed.as_secs_f64()`.
- Consumes: nothing from later tasks.

- [ ] **Step 1: Write the failing test** in `core_accounting.rs`'s `mod tests`.

```rust
/// The gate's subject is SYNCS, not records. Today they are equal (one
/// transaction, one fsync), which is why this passes trivially before group
/// commit — that equality is asserted by
/// `syncs_equal_records_before_group_commit` below. After group commit the two
/// diverge and only this framing stays physically meaningful.
#[test]
fn the_gate_compares_syncs_to_the_device_ceiling_not_records() {
    // 1000 records committed under 100 syncs, against a 120/s device ceiling.
    // Records/sec (1000) is 8.3x the ceiling and would be refused; syncs/sec
    // (100) is below it and is the honest comparison.
    let w = WriteObs {
        ops: 1000,
        loop_delta: 1000,
        syncs: 100,
        elapsed: Duration::from_secs(1),
        applied_before: Some(0),
        applied_after: Some(1000),
        device_before: 120.0,
        device_after: 120.0,
        heartbeat: None,
    };
    let a = write_accounting(Duration::from_secs(1), &w).expect("must not refuse");
    assert!(
        a.invalid.is_empty(),
        "a batched run under the sync ceiling must not be refused: {:?}",
        a.invalid
    );
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `cargo test -p bench-harness --lib the_gate_compares_syncs`
Expected: FAIL to compile — `WriteObs` has no field `syncs`. That is the correct first
failure; add the field in Step 3 and the assertion failure (the ratio gate firing on
1000/120) is the second.

- [ ] **Step 3: Add `syncs` to the three observation structs and move the gate onto it**

In `measure.rs`, the gate function itself is unchanged — it already takes a ratio. Change
each accounting site to compute the ratio from syncs:

```rust
// core_accounting.rs, in write_accounting — was acked_writes_per_sec / ceiling
let syncs_per_sec = w.syncs as f64 / w.elapsed.as_secs_f64();
let ceiling = w.device_before.max(w.device_after);
let device_ratio = syncs_per_sec / ceiling;
```

Apply the same shape in `kernel_accounting.rs` and `wire_accounting.rs`. Keep publishing
the existing per-second record figures in the report; only the **gate input** changes.

- [ ] **Step 4: Add the equality assertion that makes today's behaviour provably unchanged**

```rust
/// T1's no-behaviour-change claim, ASSERTED rather than implied. While one
/// transaction costs one fsync, the gate's new subject and its old one are the
/// same number, so the published ratio must equal the records-based ratio
/// exactly — that equality is the claim, and a test that merely ran a healthy
/// case without asserting it would pass no matter what T1 broke.
///
/// A later task makes syncs and records diverge. This test is then expected to
/// be REPLACED by the divergence case, never silently deleted.
#[test]
fn while_one_txn_costs_one_fsync_the_gate_input_is_unchanged() {
    let ceiling = 250.0;
    let w = WriteObs {
        ops: 500,
        loop_delta: 500,
        syncs: 500, // one fsync per transaction — today's engine
        elapsed: Duration::from_secs(2),
        applied_before: Some(0),
        applied_after: Some(500),
        device_before: ceiling,
        device_after: ceiling,
        heartbeat: None,
    };
    let a = write_accounting(Duration::from_secs(2), &w).expect("healthy run");
    let records_ratio = (w.ops as f64 / 2.0) / ceiling;
    assert_eq!(
        a.device_ratio, records_ratio,
        "with syncs == records the new gate input must equal the old one exactly, \
         or T1 changed behaviour it claimed not to"
    );
    assert!(a.invalid.is_empty(), "{:?}", a.invalid);
}
```

- [ ] **Step 5: Set `syncs` at every producing site**

`core_bench.rs`, `kernel_bench.rs`, `wire.rs` each populate their observation struct. Before
batching, `syncs` is the same count they already use for records — set it explicitly rather
than defaulting it, so the batching tasks have an obvious place to change.

- [ ] **Step 6: Verify the whole matrix still passes on non-batching code**

Run (on a **settled** box — never back-to-back with `cargo test --workspace`; see the T10
close-out note): `scripts/bench.sh --smoke`
Expected: 12/12, exit 0.
Then: `cargo test -p bench-harness` — all green.

- [ ] **Step 7: Update BENCH.md's method section**

In `docs/BENCH.md`, the "What can invalidate a run" bullet for the device-ratio gate must
now say the comparison is syncs/sec against the device's sync ceiling, and say why: records
per second only equalled the physical quantity while one transaction cost one fsync.

- [ ] **Step 8: Commit**

```bash
git add crates/bench-harness docs/BENCH.md
git commit -m "bench: the device-ratio gate compares syncs/sec, not records/sec

A correction to a proxy, not an accommodation. Records/sec only ever equalled
the physical quantity because one transaction cost one fsync; group commit
breaks that and the gate must survive it. Landed against non-batching code
where the true ratio is measured at 1.0, so the instrument is proven before it
scores the change that needs it."
```

---

### Task 2: `submit_all` — one append/sync/apply core

**Files:**
- Modify: `crates/charpente-core/src/engine.rs:115-176`
- Test: `crates/charpente-sim/tests/wal.rs` (existing suite must stay green)

**Interfaces:**
- Produces: `pub fn submit_all(&mut self, records: &[Vec<u8>]) -> io::Result<Vec<u64>>` —
  appends each record, syncs **once**, applies each in seq order, returns the assigned seqs.
  `pub fn submit(&mut self, payload: &[u8]) -> io::Result<u64>` remains, implemented as
  `Ok(self.submit_all(std::slice::from_ref(&payload.to_vec()))?[0])`.
- Consumes: nothing.

**Why:** §1. Two copies of append/sync/apply already exist (`submit`, `submit_batch`); a
third would cross this repo's consolidation threshold. More importantly, making `submit` the
length-1 caller is what makes batch-of-1 byte-identity a property of the **code** rather than
something the tests in T9 have to keep true.

- [ ] **Step 1: Write the failing test** in `engine.rs`'s test module.

```rust
#[test]
fn submit_all_costs_one_sync_for_many_records() {
    let (mut engine, counts) = engine_with_sync_counter(0x60C0_0001);
    let before = counts.syncs();
    let seqs = engine
        .submit_all(&[rec(b"a"), rec(b"b"), rec(b"c")])
        .expect("three records commit");
    assert_eq!(seqs, vec![0, 1, 2], "seqs are assigned in order");
    assert_eq!(
        counts.syncs() - before,
        1,
        "three records must cost exactly one fsync — that is the whole feature"
    );
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `cargo test -p charpente-core --lib submit_all_costs_one_sync`
Expected: FAIL — no method `submit_all`.

- [ ] **Step 3: Implement `submit_all` and rewrite `submit` as its caller**

```rust
/// Appends every record, syncs ONCE, then applies each in seq order — the
/// append→sync→apply discipline of `submit`, N-wide. Apply stays strictly
/// after sync: a crash before the sync loses the whole batch and never part
/// of it, which is what keeps ack-after-durable true for every record in it.
pub fn submit_all(&mut self, records: &[Vec<u8>]) -> io::Result<Vec<u64>> {
    if self.poisoned {
        return Err(io::Error::other(
            "engine poisoned: a durable record failed to apply, so machine state no \
             longer matches the log — restart and recover",
        ));
    }
    let mut seqs = Vec::with_capacity(records.len());
    for payload in records {
        seqs.push(self.wal.append(payload)?);
    }
    self.wal.sync()?;
    for (seq, payload) in seqs.iter().zip(records) {
        assert_eq!(
            *seq,
            self.last_applied.map_or(0, |l| l + 1),
            "apply loop skipped a seq — append and apply have diverged"
        );
        if let Err(e) = self.machine.apply(*seq, payload) {
            self.poisoned = true;
            return Err(e);
        }
        self.last_applied = Some(*seq);
    }
    Ok(seqs)
}
```

Note the empty-slice case: `submit_all(&[])` must **not** sync. Add
`if records.is_empty() { return Ok(Vec::new()); }` above the loop — an arm with nothing ready
must cost nothing.

- [ ] **Step 4: Run and verify**

Run: `cargo test -p charpente-core --lib`, then `cargo test -p charpente-sim --test wal`
Expected: all green. The WAL suite is the byte-identity check for the length-1 path.

- [ ] **Step 5: Replay both pinned seeds** — the first checkpoint that T9 will do exhaustively.

```bash
CHARPENTE_SEED=0x8bee1752811ae873 cargo test -p charpente-sim --test crash_recovery
CHARPENTE_SEED=0x348270b38f166529 cargo test -p charpente --test kernel
```
Expected: both green. If either changes, STOP — the length-1 path is not byte-identical and
Task 2's premise is broken.

- [ ] **Step 6: Commit**

```bash
git add crates/charpente-core/src/engine.rs
git commit -m "core: submit_all is the one append/sync/apply core, submit its length-1 caller

Makes batch-of-1 byte-identity a property of the code rather than something
the group-commit property tests have to keep true."
```

---

### Task 3: `Tx<'a>` borrows its snapshot — refactor only, zero behaviour change

**Files:**
- Modify: `crates/charpente-core/src/db.rs:454-560` (`Tx`, `impl View for Tx`)
- Modify: `crates/charpente-core/src/db.rs:1329-1436` (`transact` constructs it)
- Modify: `crates/charpente-macros/src/lib.rs` **only if** expansion breaks (see Step 4)

**Interfaces:**
- Produces: `pub struct Tx<'a> { snapshot: &'a Snapshot, group: Option<&'a GroupOverlay>,
  effects: TxEffects }`. Handler signatures stay `&mut Tx` via lifetime elision.
- Produces: `GroupOverlay` itself, which this plan referenced in two tasks and **defined in
  none** (pre-flight, 2026-08-11 — see the amendment block below). Spec §2 fixes its shape
  and forbids a parallel mechanism ("the group overlay is a merge of that same shape"), so
  it is the type `TxEffects::overlay` already has (`db.rs:331`):

  ```rust
  /// The group's accumulated row writes — the same shape as `TxEffects::overlay`,
  /// because spec §2 requires a merge of that structure, not a parallel one.
  type GroupOverlay = BTreeMap<&'static str, BTreeMap<u64, Option<Vec<u8>>>>;
  ```

  A private alias, not a newtype: nothing in T3 needs behaviour on it, and `Tx` holds only a
  `&`. If T4's merge/read logic argues for a newtype, that is T4's call to make with its
  reasons stated — it is not a T3 decision.
- Consumes: nothing.

**Why:** a group shares ONE snapshot across its staged transactions (`Snapshot` owns a
`redb::ReadTransaction` and is not cheap to duplicate per transaction). `group` is `None` in
this task and becomes `Some` in Task 4 — introducing the field now keeps Task 4's diff to
behaviour only.

- [ ] **Step 1: (amended 2026-08-11 — Noah ruling) There is no white-box test here.**

The original Step 1 constructed two `Tx::over(&snap)` values directly and asserted they are
isolated. That test cannot exist as written, for a reason that is structural rather than
fixable by tidying it (pre-flight findings 2–4 in the amendment block below): it needs a real
`Db`, `charpente-core`'s `--lib` target cannot build one, and the integration target that can
cannot reach a `pub(crate)` constructor.

**Ruled (Noah, 2026-08-11): defer the assertion to Task 4, keep `Tx::over` `pub(crate)`.**
The rejected alternative was making `over` `pub` so the test could reach it — which would let
any app holding a `&Db` mint a free-floating `Tx` from `Db::snapshot()`, write into it, and
drop it, losing writes silently with no error. That is precisely the bug class the M0 demo
exists to show is *inexpressible from app code* (`examples/invoicing/DEMO.md`: writing the
split-transaction outbox bug required a temporary framework escape hatch). Widening the API
to reach a constructor for a test is not worth re-opening it.

Nothing is lost by deferring. The isolation property becomes **publicly observable** the
moment `stage` exists — a staged transaction that returns `Err` has its overlay discarded and
the group is unaffected (spec §2) — and Task 4 Step 1 now carries that assertion.

**This task's evidence is therefore Step 5, and Step 5 is not a formality.** "Zero behaviour
change" is a claim about the whole workspace, and the thing that can actually falsify it is
the workspace suite plus both pinned seeds coming back identical. Treat a single changed
count as this refactor having failed, not as a count to reconcile.

- [ ] **Step 2: Confirm the starting counts before changing anything**

Run `cargo test --workspace` (piped through `tee`, never `tail`) and record the pass / fail /
ignored triple. That is the baseline Step 5 compares against; taking it *after* the change
proves nothing.

- [ ] **Step 3: Make the change**

```rust
pub struct Tx<'a> {
    snapshot: &'a Snapshot,
    /// The group's accumulated writes, `None` for a lone transaction. Reads
    /// fall through txn overlay -> group overlay -> snapshot (spec §2).
    group: Option<&'a GroupOverlay>,
    effects: TxEffects,
}

impl<'a> Tx<'a> {
    pub(crate) fn over(snapshot: &'a Snapshot) -> Tx<'a> {
        Tx { snapshot, group: None, effects: TxEffects::default() }
    }
}
```

`impl View for Tx<'_>`: in `get`, after the txn overlay misses, consult `self.group` (a no-op
while it is `None`) before `self.snapshot`. In `scan`, layer group writes between the
snapshot rows and the txn overlay. Do the same for any index-backed read — §2 requires
index reads to reflect overlay writes exactly as they do within a transaction today.

In `transact`, take the snapshot into a local and build `Tx::over(&snapshot)`.

- [ ] **Step 4: Verify the macro surface did not break**

Run: `cargo build -p sim-fixture && cargo build -p guide-examples && cargo build -p invoicing`
Expected: all compile. `sim-fixture` is single-dep on `charpente` and pins macro-path
expansion; `guide-examples` compiles every GUIDE `rust` block. If a handler signature now
needs `Tx<'_>`, fix the macro emission in `charpente-macros` and re-run — do not ask app
code to change.

- [ ] **Step 5: Verify zero behaviour change**

Run: `cargo test --workspace`
Expected: identical pass/fail/ignored counts to the T2 baseline. Then replay both pinned
seeds as in Task 2 Step 5. Any change means this refactor was not behaviour-free.

- [ ] **Step 6: Commit**

```bash
git add crates/charpente-core/src/db.rs crates/charpente-macros
git commit -m "core: Tx borrows its snapshot, ready for a shared group snapshot

Pure refactor. The group field is None everywhere until group commit lands."
```

---

### Task 4: `Db::begin_group` / `stage` / `commit`

**Files:**
- Modify: `crates/charpente-core/src/db.rs`
- Test: `crates/charpente-core/src/db.rs` `mod tests`

**Interfaces:**
- Produces:
  - `pub struct Group<'a>` holding `snapshot: Snapshot`, `overlay: GroupOverlay`,
    `records: Vec<Vec<u8>>`, `staged_adoptions: BTreeSet<String>`, `db: &'a mut Db`.
  - `pub fn begin_group(&mut self) -> io::Result<Group<'_>>`
  - `Group::stage<T, E>(&mut self, f: impl FnOnce(&mut Tx) -> Result<T, E>) -> Result<T, TxError<E>>`
  - `Group::len(&self) -> usize`, `Group::is_full(&self) -> bool` (`len() >= MAX_BATCH`)
  - `Group::commit(self) -> Result<(), TxError<std::convert::Infallible>>`
- Consumes: `Engine::submit_all` (T2), `Tx<'a>` (T3).

> **AS SHIPPED, this Interfaces block is out of date on two lines — corrected 2026-08-12 after
> T4's review. The lines above are kept as the task's original text; these are the facts.**
> - `commit` returns **`Result<(), CommitFailure>`**, not `Result<(), TxError<Infallible>>`,
>   where `CommitFailure { applied: usize, error: io::Error }`. Ruled after the review found the
>   original could report *that* a batch failed but never *where*, which left spec §4's only new
>   contract inexpressible and T8 Step 3 unassertable. It is fed by a new
>   `SubmitFailure { applied, error }` on `Engine::submit_all`, so the count originates where it
>   is known rather than being reconstructed a layer up.
> - `Group::is_full()` shipped but **T6 deletes it** — see T6's resolved MAX_BATCH block. The
>   drain bounds on requests polled, not records staged, because an `is_full()`-bounded loop is
>   unbounded when handlers return `Err`. `Group::len()` stays.
>
> Also shipped beyond this list: `Group::is_empty()` (forced by `clippy::len_without_is_empty`),
> an `impl Drop for Group` that panics on a non-empty group dropped without `commit` (guarded by
> `std::thread::panicking()`), and an assert in `stage` bounding `records.len()` by `MAX_BATCH`.

**Behaviour `stage` must reproduce from today's `transact`** (`db.rs:1329-1436`), in order:
run the closure; return `TxError::App` on `Err`; **return `Ok` with no record if effects are
empty**; check the write gate (`Allowed` / `NotPrimary` / `Verifying`); do first-write
adoption; `encode_record` and assert `MAX_RECORD_BYTES`. Then, instead of submitting: merge
the txn overlay into the group overlay and push the record.

**Two things that differ from a lone transaction and are easy to miss:**

1. **Adoption must be group-aware.** `self.adopted` is only updated after a successful
   submit, so two staged transactions writing the same versioned table would each emit a
   `SchemaVersion` row. Consult `self.adopted` **union** `group.staged_adoptions`, and add to
   `staged_adoptions` at stage time; fold them into `self.adopted` in `commit`.
2. **Checkpoint cadence is per-batch.** `records_since_checkpoint += records.len()` once in
   `commit`, then the threshold check once — never inside the batch (§3).

**A note for whoever implements this task (2026-08-10, T1's post-review fix wave, review
finding 3).** Once staged commits make `syncs != ops` (core) and
`syncs != requests_planned + stats_jobs_done` (kernel) for a real batched run,
`core_accounting`'s `while_one_txn_costs_one_fsync_the_gate_input_is_unchanged` and
`kernel_accounting`'s `while_one_commit_costs_one_fsync_the_gate_input_is_unchanged` will go
RED. Both already carry a doc comment saying they are expected to be REPLACED by the
divergence case, never silently deleted — this line exists because deleting them is the
cheapest way to make this task's own test suite pass, and a doc comment alone does not stop
that from happening.

- [ ] **Step 1: Write the failing tests — read-your-own-writes across the batch, and isolation**

**Amended 2026-08-11 (pre-flight).** These live in a NEW `crates/charpente-core/tests/group_commit.rs`,
not in `db.rs`'s `mod tests`. That module is pure `TxEffects` encode/decode and cannot build a
`Db`: `charpente_sim` appears nowhere in `charpente-core/src/`, because a `--lib` test target
is a distinct compilation unit from the one `charpente-sim` links against and the trait impls
do not unify (`tests/engine.rs`'s own header documents this). Every `Db`-constructing test in
this crate is already an integration test. `begin_group`/`stage`/`commit` are `pub`, so they
are reachable from one.

`fresh_db` and `Widget` **do not exist in `charpente-core`** and must be written here.
`fresh_db` exists only in `charpente`'s repl modules (`follower.rs:508`, `probe.rs:386`) —
do NOT reach for a third copy; the M1 close-out already names a third copy as the moment to
consolidate, and these are a different crate anyway. For the row, follow the pattern the
sibling integration tests already use: **core cannot use its own `#[table]` macro** (it emits
`::charpente::core::` paths), so hand-write `Row` (+ `RowShape` if versioned) exactly as
`tests/migration_gate.rs:12-44` and `tests/migrate_apply.rs:26` do, and open the db with
`Db::open(Box::new(sim.disk()), cfg(), schema)`.

```rust
/// The property the whole two-level overlay exists for.
#[test]
fn a_staged_transaction_sees_an_earlier_one_in_the_same_group() {
    let mut db = fresh_db(0x60C0_0005);
    let mut g = db.begin_group().expect("group");
    g.stage(|tx| { tx.put(&Widget { id: Id::from_raw(1), count: 7 }); Ok::<_, ()>(()) })
        .expect("first stages");
    let seen = g
        .stage(|tx| Ok::<_, ()>(tx.get::<Widget>(Id::from_raw(1)).unwrap()))
        .expect("second stages");
    assert_eq!(seen.map(|w| w.count), Some(7),
        "B must see A's write, or a batch silently loses updates");
    g.commit().expect("commit");
}
```

**Plus the assertion deferred from Task 3** (Noah ruling, 2026-08-11): isolation is what makes
panic-safety structural per spec §2, and `stage` is where it first becomes publicly
observable. A staged transaction that returns `Err` must leave the group untouched — its
overlay discarded, nothing queued — so a later `stage` cannot see its writes:

```rust
#[test]
fn a_staged_transaction_that_fails_leaves_the_group_untouched() {
    let mut db = fresh_db(0x60C0_0006);
    let mut g = db.begin_group().expect("group");
    let r = g.stage(|tx| {
        tx.put(&Widget { id: Id::from_raw(1), count: 1 });
        Err::<(), _>("rejected")
    });
    assert!(matches!(r, Err(TxError::App("rejected"))), "the app error surfaces as its own");
    assert_eq!(g.len(), 0, "a rejected transaction queues no record");
    let seen = g
        .stage(|tx| Ok::<_, ()>(tx.get::<Widget>(Id::from_raw(1)).unwrap()))
        .expect("stages");
    assert!(seen.is_none(),
        "a rejected transaction's writes must never reach the group overlay — this is what \
         makes a panicking handler structurally unable to corrupt the batch");
}
```

- [ ] **Step 2: Run them and watch them fail**

Run: `cargo test -p charpente-core --test group_commit`
Expected: FAIL — no method `begin_group`.

- [ ] **Step 3: Implement `Group`, `stage`, `commit`**

**Amended 2026-08-12 (pre-flight, finding C).** Define the batch bound here, next to `Group`,
and keep it **private to `db.rs`**:

```rust
/// The batch bound. A `const`, never a `DbConfig` field: spec §3 makes this
/// internal mechanism like `REPL_TICK_NANOS`, and a configurable throughput
/// knob next to `docs/BENCH.md` is the one thing not to ship. It exists only
/// to bound buffered memory — greedy drain is already self-limiting by
/// arrival rate.
const MAX_BATCH: usize = 64;
```

Start it private. **The visibility question this paragraph used to leave open was RESOLVED on
2026-08-12 by T6's pre-flight — see T6's own MAX_BATCH block for the reasoning and the
decision.** Short form, so this section does not mislead: `MAX_BATCH` becomes `pub` at T6 (one
shared constant, never a duplicated literal), the drain bounds on **requests polled**, and
`Group::is_full()` is **deleted** — an `is_full()`-bounded loop is unbounded exactly when
handlers return `Err`, because a rejected request stages no record while still costing a held
request and reply. T4 still ships it private; T6 widens it and removes `is_full()`.

`commit` is — **and note the shipped signature differs from this snippet**, which is kept as
the task's original text. See the corrected Interfaces note above: it returns
`Result<(), CommitFailure>`, and the `let Group { .. } = self` destructure below became an
E0509 compile error once `Drop` was added, so the shipped form is `commit(mut self)` with
`std::mem::take` on `records`/`staged_adoptions`:

```rust
pub fn commit(self) -> Result<(), TxError<std::convert::Infallible>> {
    let Group { db, records, staged_adoptions, .. } = self;
    if records.is_empty() {
        return Ok(());
    }
    let n = records.len();
    // Amended 2026-08-12 (pre-flight, finding K): `submit_all` takes
    // `&[&[u8]]`, not `&[Vec<u8>]` — the plan's original `&records` did not
    // compile. T2 chose that signature so `submit` is `submit_all(&[payload])`
    // with no heap copy on the single-commit hot path, so the borrow is
    // reconstructed here rather than the signature loosened. This collects N
    // pointers, not N payloads.
    let refs: Vec<&[u8]> = records.iter().map(|r| r.as_slice()).collect();
    db.engine.submit_all(&refs).map_err(TxError::Io)?;
    for table in staged_adoptions {
        db.adopted.insert(table);
    }
    db.records_since_checkpoint += n as u64;
    if db.cfg.checkpoint_every > 0
        && db.records_since_checkpoint >= db.cfg.checkpoint_every
    {
        db.checkpoint().map_err(TxError::Io)?;
    }
    Ok(())
}
```

- [ ] **Step 4: Make `transact` a one-transaction group**

```rust
pub fn transact<T, E>(
    &mut self,
    f: impl FnOnce(&mut Tx) -> Result<T, E>,
) -> Result<T, TxError<E>> {
    let mut g = self.begin_group().map_err(TxError::Io)?;
    let out = g.stage(f)?;
    g.commit().map_err(|e| match e {
        TxError::Io(e) => TxError::Io(e),
        _ => unreachable!("commit's app-error type is Infallible"),
    })?;
    Ok(out)
}
```

- [ ] **Step 5: Write the panic-isolation test (RED first)**

```rust
#[test]
fn a_panicking_stage_leaves_the_group_clean() {
    let mut db = fresh_db(0x60C0_0004);
    let mut g = db.begin_group().expect("group");
    g.stage(|tx| { tx.put(&Widget { id: Id::from_raw(1), count: 1 }); Ok::<_, ()>(()) })
        .expect("first stages");
    let boom = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        g.stage(|tx| -> Result<(), ()> {
            tx.put(&Widget { id: Id::from_raw(2), count: 2 });
            panic!("handler exploded");
        })
    }));
    assert!(boom.is_err(), "the panic propagates out of stage");
    let seen = g.stage(|tx| Ok::<_, ()>(tx.get::<Widget>(Id::from_raw(2)).unwrap()))
        .expect("third stages");
    assert_eq!(seen, None, "a panicking handler's writes must never reach the group");
    g.commit().expect("commit");
}
```

- [ ] **Step 6: Run everything**

Run: `cargo test -p charpente-core` then `cargo test --workspace`
Expected: green, with counts equal to T3's baseline plus the new tests.

- [ ] **Step 7: Commit**

```bash
# Amended 2026-08-12 (pre-flight, finding B): the new integration test file
# was missing from this list and would have been left uncommitted.
git add crates/charpente-core/src/db.rs crates/charpente-core/tests/group_commit.rs
git commit -m "core: Db::begin_group/stage/commit — N transactions, one fsync

transact becomes a one-transaction group, so the single path and the batched
path are the same code. Adoption is group-aware and the checkpoint cadence
counts once per batch."
```

---

### Task 5: Staged dispatch at the App layer

**Files:**
- Modify: `crates/charpente/src/http/mod.rs` (the `handle_or_subscribe` site)
- Modify: `crates/charpente/src/lib.rs` if `App`'s surface changes

**Interfaces (REVISED 2026-08-12 by pre-flight finding G + Noah ruling — the original
signature could not compile):**

The plan originally specified `App::stage_or_subscribe(&mut self, group: &mut Group, request:
Request, now: u64) -> Handled`. **That signature is unusable and the defect is in the signature,
not in any call site's phrasing.** `App::db_mut(&mut self) -> &mut Db` (`http/app.rs:296`) is
opaque to the borrow checker, so the `Group<'_>` built from it holds a loan against the *whole*
`App` for as long as the group lives. Calling any method on `app` afterwards — including
`stage_or_subscribe` itself — is a second overlapping borrow. Since `App` owns exactly one `Db`
and that is the only way a `Group` is ever constructed here, there is **no** call site at which
the original shape works, and T6's kernel drain loop would hit the identical wall.

**Ruled (Noah, 2026-08-12): the StagedDispatch borrow-split.** Everything the dispatch path needs
besides the `Db` requires only a *shared* borrow — `Router::find` is `&self`
(`http/router.rs:125`), `Entropy::fill` is `&self` (`charpente-core/src/io.rs:72`),
`config.body_cap`/`session_ttl` are field reads, and `oidc` is an `Arc` clone. So one `&mut self`
call hands back both halves as disjoint sub-borrows, which Rust permits for direct field
projections inside a single function body even though it rejects the chained opaque-method form.

- Produces: `App::begin_group(&mut self) -> io::Result<(Group<'_>, StagedDispatch<'_>)>` and
  `StagedDispatch::stage_or_subscribe(&self, group: &mut Group, request: Request, now: u64)
  -> Handled` — the latter identical to `handle_or_subscribe` except it stages into `group`
  instead of committing.
- Produces: `pub struct StagedDispatch<'a>` holding `&'a Router`, `&'a dyn Entropy`, `&'a AppConfig`
  and `Option<&'a OidcVerifier>`.

  **Named `StagedDispatch`, NOT `Dispatcher` — renamed 2026-08-12 by T6's pre-flight before
  either task was written.** `charpente` already has a `pub struct Dispatcher`: the *job*
  dispatcher (`kernel/dispatch.rs:90`), re-exported unqualified at `kernel/mod.rs:10` and stored
  as `Kernel::dispatcher`. A second public `Dispatcher` would need to be reachable from that same
  `kernel/mod.rs` for T6's drain loop, where the file already imports http types unqualified — so
  the natural `use` collides outright (E0252), and even where inference avoids the import, a local
  named `dispatcher` sitting beside `self.dispatcher` meaning something entirely different is a
  maintenance trap. The newcomer renames because it has no callers yet; the job dispatcher is
  `pub`, referenced, and older. Local bindings are `staged`, not `dispatcher`, for the same reason.
- `handle_or_subscribe` stays, rewritten as the one-request wrapper over this pair — the same
  "single path and batched path are the same code" reasoning T4 applied to `transact`.
- Consumes: `Group` (T4). Import it as `charpente_core::db::Group` inside `http/app.rs`, the way
  `Tx`/`Db`/`TxError` already are — the `charpente::core` re-export is for external app crates.

**The mixed-batch case, which the spec under-specified — and one correction to how this plan
stated it.** A drained batch can contain both write requests and SSE subscribes. Responses for
every request in the batch are held and sent only after `commit` returns; sending a 200 before
the sync would break ack-after-durable, which is the entire point.

> **The plan previously said "`Handled::Subscribe` still commits nothing." That is FALSE as a
> general statement and must not be implemented as one** (pre-flight finding F). `Handled` has
> exactly two variants and no unlisted writer, but an SSE glue closure receives `&mut Tx` and may
> legitimately write — `ctx.create_session(tx, …)` is the obvious case — and today's
> `handle_or_subscribe` already commits whatever an SSE handler produces, because **both**
> `RouteGlue` arms run inside the same `db_mut().transact(...)` call (`http/app.rs:451-468`).
> Nothing skips the commit for a `Subscribe`. What actually makes a typical read-only subscribe
> cost no record is `Group::stage`'s general empty-effects check (`db.rs:2016-2018`), which
> applies to any staged transaction regardless of flavour. **Route both arms through
> `group.stage(...)` identically.** Hard-coding "Subscribe never queues a record" would silently
> drop the writes of an SSE route that does write.

- [ ] **Step 1: Write the failing test — scoped to what T5 delivers**

This task ends green. The ack-ordering property belongs to T6, which is the task that makes
the kernel capable of violating it; asserting it here would leave a red test at task end.

**Amended 2026-08-12 (pre-flight finding A). None of `app_with_op_counter`, `post(...)` or a
counter exposing `.appends()` exists anywhere in the repo** — the original snippet called four
things into being by naming them. What exists: `Request::new(Method::Post, "/notes")` is how
every test in `crates/charpente/tests/http.rs` builds a request (there is no `post()`
shorthand), and the only counting-disk precedent is `SyncCounts` in
`crates/charpente-core/tests/engine.rs:169-176` — which counts syncs only, has no append
concept, and is a private struct in a *different crate's* test file, so it cannot be imported.

**So Step 1 must first write its own harness**, local to `crates/charpente/tests/group_commit.rs`:
a `CountingDisk`/`CountingFile` pair modelled on that `SyncCounts` precedent but tracking
write-shaped ops and syncs as two separate counters, plus
`fn app_with_op_counter(seed: u64) -> (App, OpCounts)` that builds a `Sim`, wraps its disk, and
calls `App::open` with it. Budget for that; it is not a one-liner.

```rust
/// `stage_or_subscribe` runs the whole dispatch path but commits nothing —
/// the record lands in the group, and the WAL is untouched until commit.
#[test]
fn stage_or_subscribe_produces_a_record_without_touching_the_wal() {
    let (mut app, counts) = app_with_op_counter(0x60C0_0007);
    let before = counts.appends() + counts.syncs();
    let (mut g, staged) = app.begin_group().expect("group");
    let handled =
        staged.stage_or_subscribe(&mut g, Request::new(Method::Post, "/hit/1"), 0);
    assert!(matches!(handled, Handled::Response(_)), "a write route responds");
    assert_eq!(g.len(), 1, "the record is staged");
    assert_eq!(
        counts.appends() + counts.syncs() - before,
        0,
        "staging must not touch the WAL — that is what makes one fsync per batch possible"
    );
    g.commit().expect("commit");
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `cargo test -p charpente --test group_commit stage_or_subscribe_produces_a_record`
Expected: FAIL — no method `stage_or_subscribe`.

- [ ] **Step 3: Implement `stage_or_subscribe`**

Factor the existing `handle_or_subscribe` body so the routing/auth/handler-invocation path is
shared and only the commit differs. `handle_or_subscribe` becomes a wrapper that makes a
one-transaction group, exactly as `transact` did in T4 — same reasoning, one code path.

Per the ruling in Interfaces above, the factoring lands on `StagedDispatch`, and `App::begin_group`
is what produces the two disjoint sub-borrows:

```rust
impl App {
    pub fn begin_group(&mut self) -> io::Result<(Group<'_>, StagedDispatch<'_>)> {
        // Field projections, NOT method calls: `self.db.as_mut()` and the
        // shared field borrows split cleanly here, where `self.db_mut()`
        // would re-borrow all of `*self` and poison the split. See the T5
        // pre-flight's finding G for the failure this shape exists to avoid.
        let staged = StagedDispatch {
            router: &self.router,
            entropy: &*self.entropy,
            config: &self.config,
            oidc: self.oidc.as_deref(),
        };
        let group = self
            .db
            .as_mut()
            .expect("db taken for reseed — a kernel bug left it out")
            .begin_group()?;
        Ok((group, staged))
    }
}
```

The `db_mut()`-vs-field-projection distinction is the whole fix and is easy to undo by accident:
calling `self.db_mut()` here reintroduces the exact borrow conflict the ruling resolved. The
rationale comment above must ship with the code.

- [ ] **Step 4: Verify**

Run: `cargo test -p charpente --test http && cargo test -p charpente --test kernel`
Expected: green.

- [ ] **Step 5: Capture the pre-batching disk trace — this step exists HERE and nowhere else**

T6 asserts that a never-contended universe traces byte-identically to the unbatched engine.
That constant must be recorded while the kernel is **still unbatched**, which is true at this
commit and false at every later one. Captured after T6, it would record whatever T6 produced
and the assertion would be a tautology.

**Amended 2026-08-12 (pre-flight). The original command was broken two independent ways, and
both would have produced a green T6 that proves nothing — the exact failure mode this repo
has caught in itself repeatedly.**

1. **The destination was gitignored.** `.superpowers/sdd/.gitignore` is a single `*`, and
   `git ls-files .superpowers` is empty — nothing under that tree is tracked, by design; it is
   scratch. `git add` of that path fails outright, and T6's `include_str!` of it would compile
   on the machine that captured it and **fail to build on a fresh clone or in CI**. Spec §7's
   batch-of-1 identity — the guarantee that lets every frozen pinned seed survive this
   sub-project — cannot rest on an untracked file.
2. **Redirecting `cargo test`'s stdout captures the wrong bytes.** Without `--nocapture` the
   harness swallows `println!` entirely, so the file would contain cargo's own progress and
   summary lines and none of the trace; with `--nocapture` it would contain the trace *plus*
   that noise. Either way `expected.trim()` never equals `disk_trace_string()`.

3. **`run_one_at_a_time_universe` and `disk_trace_string()` do not exist, and neither does the
   machinery under them** (pre-flight finding E). This is the item most likely to blow this
   task's estimate, so it is stated plainly rather than left implied by a one-line call.
   `charpente-sim` has **no per-operation disk trace at all**: `Sim::durable_fingerprint()`
   (`charpente-sim/src/lib.rs:678,823`) is a single hash over final store contents, and
   `kernel.rs`'s determinism test (`loop_replays_identically_for_the_same_seed`, :980-1003)
   asserts on `(outcome, recorded_responses, durable_fingerprint, now)` — a fingerprint, not a
   trace. **T5 must invent this infrastructure**, and it is not in this task's stated file list:
   a `Disk`/`File` wrapper recording each op (`write_at offset len`, `set_len`, `sync`, per file)
   into an ordered `Vec<String>`, plus `run_one_at_a_time_universe(seed)` driving a `Kernel` over
   a `SimSource` through that traced disk — the pattern `kernel.rs:984` already shows
   (`SimSource::new(Box::new(sim.clock()), HOUR)` + `Kernel::new(...).run(&mut source)`).
   It can share the counting-disk harness Step 1 already needs; build both once.
   A fingerprint would NOT substitute here: T6 must detect a reordered or extra disk op, which
   is exactly what a final-state hash cannot see.

So the `#[ignore]`d `capture_pre_batching_trace` **writes the file itself** and there is no
shell redirect:

```rust
/// Captures the pre-batching disk trace. `#[ignore]`d because it writes a
/// tracked fixture rather than asserting anything. Run once, HERE, while the
/// kernel is still unbatched — see the note below.
#[test]
#[ignore]
fn capture_pre_batching_trace() {
    let trace = run_one_at_a_time_universe(0x60C0_0008).disk_trace_string();
    std::fs::write(
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/pre-batching-trace.txt"),
        &trace,
    )
    .expect("write the fixture");
}
```

```bash
cargo test -p charpente --test group_commit -- --ignored capture_pre_batching_trace
```

The fixture lives at **`crates/charpente/tests/fixtures/pre-batching-trace.txt`** — tracked,
and following the repo's one existing precedent for non-`.rs` test data,
`crates/charpente-macros/tests/compile_fail/*.stderr`. A subdirectory is safe: cargo builds a
test target from every `.rs` file directly in `tests/`, never from a subdirectory. **T6 must
`include_str!("fixtures/pre-batching-trace.txt")`**, which resolves relative to the including
file, not the `.superpowers` path it currently names.

Commit the captured file, and state in the commit message that it was captured pre-batching —
that provenance is the only thing that makes T6's assertion meaningful.

- [ ] **Step 6: Commit**

```bash
git add crates/charpente/src/http crates/charpente/src/lib.rs \
        crates/charpente/tests/group_commit.rs \
        crates/charpente/tests/fixtures/pre-batching-trace.txt
git commit -m "http: stage_or_subscribe — dispatch into a group without committing

Also captures the pre-batching disk trace for seed 0x60C0_0008 while the kernel
is still unbatched. T6 asserts batch-of-1 traces identically against it;
captured any later, that assertion would compare T6's output to itself."
```

---

### Task 6: The Request arm drains

**Files:**
- Modify: `crates/charpente/src/kernel/mod.rs:65-75` (`RequestSource`), `:303-317` (the arm)
- Modify: `crates/charpente/src/serve.rs` (`ProdSource`), `crates/charpente-sim` (`SimSource`)

**Interfaces:**
- Produces: the Request arm drains up to `MAX_BATCH` via repeated `poll()`, stages each,
  commits once, then sends every reply in arrival order.
- Consumes: `stage_or_subscribe` (T5), `Group` (T4).

**No new trait method.** `RequestSource::poll` is already non-blocking and returns `None`
when empty, so draining is `while batch.len() < MAX_BATCH { match src.poll() { Some(r) =>
batch.push(r), None => break } }`. Adding a `drain` method would give the two sources a
chance to disagree; reusing `poll` keeps prod and sim on the same code, which is the
same-shape rule.

**RESOLVED 2026-08-12 by T6's pre-flight, and it went against the reading T4's pre-flight
leaned toward — keep the loop above, bounding on requests polled.** The question was whether to
bound the drain on requests polled or on `Group::is_full()` (records staged), since a handler
returning `Err` stages no record and the two are different quantities.

Three things settled it:

1. **An `is_full()`-bounded loop is UNBOUNDED in exactly the case a cap exists for.** A run of
   `Err`-returning or empty-effects requests stages zero records, never trips `is_full()`, and
   the loop keeps draining for as long as `poll()` returns `Some` — while the held `Request` and
   reply vector grows one entry per drained item, each up to the 1 MiB body cap. Bounding on
   requests caps records and replies at once (`records ≤ requests ≤ 64`). Spec §3's one sentence
   of arithmetic names the payload buffer, but its actual mandate is "purely to bound memory",
   and the replies are memory.
2. **The drafted loop cannot trip T4's new assert, and is exactly at the boundary.** `stage`
   asserts `records.len() < MAX_BATCH` *before* running the handler, and `stage_or_subscribe`
   calls it at most once per drained request. Worst case, 64 requests all staging: the 64th call
   sees `records.len() == 63`, passes, and leaves 64. No 65th call exists. Safe, exactly met,
   never exceeded.
3. **`is_full()` has no caller anywhere** — not in the plan, not in the suite. T4's own
   `stage_refuses_past_max_batch` proves the cap through the assert, not through `is_full()`.

**So this task: make `MAX_BATCH` `pub` in `charpente_core::db` and reference it from
`kernel/mod.rs` — one constant, two use sites (T4's assert and this drain bound), never a
duplicated literal — and DELETE `Group::is_full()`.** Deleting it is finishing the decision T4's
doc comment explicitly deferred here, not overriding it; a `pub` method with zero callers is the
speculative-abstraction the style rules reject. `Group::len()` stays (the tests use it).

**Put a one-line comment at the drain bound saying why it counts requests and not records**, or
the next reader "simplifies" it to `is_full()` and silently restores the unbounded case.

- [ ] **Step 1: Write the ack-ordering test (RED)**

This is the property T6 makes it possible to violate, so it belongs here.

```rust
/// Ack-after-durable, batch-wide: no reply may leave before the batch's sync.
#[test]
fn no_response_is_sent_before_the_group_syncs() {
    let trace = run_two_request_batch(0x60C0_0009);
    let sync_at = trace.iter().position(|op| op == "sync").expect("a sync happened");
    let first_reply = trace.iter().position(|op| op == "reply").expect("a reply happened");
    assert!(
        first_reply > sync_at,
        "a reply was sent before the batch synced — ack-after-durable is broken: {trace:?}"
    );
}
```

Run it: expected FAIL (the arm still replies per request). Then implement the drain — stage
each request, `commit()`, then send every reply in arrival order — and re-run: PASS.

> **Infrastructure this task owes and its file list does not name (T6 pre-flight, finding A).**
> `run_two_request_batch` does not exist, and the trace it returns is **not** the one T5 builds.
> T5's tracer records *disk* operations (`write_at`, `set_len`, `sync`); this test asserts on the
> ordering of `"sync"` against `"reply"`, and a reply is not a disk op — no disk-level tracer can
> see one. So T6 must extend T5's tracer with a reply event (or thread a shared ordered event log
> through both the traced `Disk` and the kernel's reply path) and write `run_two_request_batch` on
> top of it. Budget for it. Reuse T5's harness rather than starting a second one — that is why T5
> was told to build its counting-disk and trace facilities together.

- [ ] **Step 2: Write the batch-of-1 identity test against T5's captured trace**

```rust
/// §7's structural guarantee under test: a universe that never queues two
/// requests must produce a byte-identical disk trace to the pre-batching
/// engine. This is what lets the frozen pinned seeds survive.
#[test]
fn a_never_contended_universe_traces_identically() {
    // Amended 2026-08-12 (pre-flight): the fixture moved out of the gitignored
    // .superpowers tree, where this include_str! built only on the machine that
    // captured it and broke on any fresh clone. Path is relative to this file.
    let expected = include_str!("fixtures/pre-batching-trace.txt");
    let batched = run_one_at_a_time_universe(0x60C0_0008);
    assert_eq!(batched.disk_trace_string(), expected.trim(),
        "batch-of-1 must be byte-identical to the unbatched path");
}
```

The constant was captured at T5 Step 5, while the kernel was still unbatched. **Do not
re-capture it here** — a constant regenerated after batching lands compares T6's output to
itself and asserts nothing. If the file is missing, stop and report BLOCKED rather than
regenerating it.

- [ ] **Step 3: Handle the poison/stop path**

The existing request-arm ruling (stop when `Db::poisoned()` after a 500) now applies to a
batch: check once after `commit`, and if poisoned, send the tail's 500s and stop. Preserve
the store-latch-first ordering and both pinned message strings verbatim — `checks::classify`
matches on them.

**Amended 2026-08-12 (T4 review Important 1 + Noah ruling). "The tail" is now a counted
quantity, and this step is the reason it had to be.** As originally drafted, this step was
unsatisfiable: `commit` returned `Result<(), TxError<Infallible>>`, which tells a caller *that*
the batch failed and never *where*, so the only thing this arm could actually do was 500 the
**whole** batch — contradicting spec §4's last row and making T8 Step 3 unassertable. `commit`
now returns `Result<(), CommitFailure>` with `CommitFailure { applied: usize, error: io::Error }`.

So the split is exact, not inferred:

```rust
match group.commit() {
    Ok(()) => { /* every reply in the batch is a normal reply */ }
    Err(CommitFailure { applied, error }) => {
        // 1..=applied are durable and keep their 200s.
        // applied+1..N get 500s — and per spec §4 their records may STILL be
        // durable and will apply on restart, so this 500 means "unknown",
        // never "rejected". That contract is GUIDE's to state (T11).
    }
}
```

Do **not** re-derive `applied` from `db.last_applied()` before-vs-after. The ruling rejected
that shape explicitly, on this sub-project's own precedent: T1's review carried a Critical for
gating on a derived quantity where a counted one was available.

- [ ] **Step 4: Run the kernel suite and both pinned seeds**

Run: `cargo test -p charpente --test kernel`, then the two `CHARPENTE_SEED` replays.
Expected: kernel green. **The pinned seeds may now change** — that is expected from here on
and is Task 9's subject. Record the outcome; do not "fix" it here.

- [ ] **Step 5: Commit**

```bash
git add crates/charpente/src/kernel crates/charpente/src/serve.rs crates/charpente-sim
git commit -m "kernel: the request arm drains a batch and commits it under one fsync"
```

---

### Task 7: The Job and Cron arms drain

**Files:**
- Modify: `crates/charpente/src/kernel/mod.rs` (Job and Cron arms)

**Interfaces:**
- Produces: both arms drain into their own group, capped at `MAX_BATCH`. Arms never mix (§Scope 4).
- Consumes: T4, T6.

**Why this task matters to the number:** the canonical transaction is two durable records —
the request's effects and its job's Done-mark — which is why kernel throughput measured 109
req/s against 217 records/s. Batching requests alone caps the win near 2x.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn forty_due_jobs_cost_one_fsync() {
    let g = run_universe_with_due_jobs(0x60C0_000A, 40);
    assert_eq!(g.syncs_in_job_arm, 1,
        "40 due jobs must drain as one batch; got {} syncs", g.syncs_in_job_arm);
    assert_eq!(g.jobs_done, 40);
}
```

- [ ] **Step 2: Run and watch it fail** — expect 40 syncs.

- [ ] **Step 3: Implement the Job arm drain**

Each job run is a transaction staged into the group. **The retry path stays one transaction
per failure** as today: on `Err`, the failure record (attempts, due_at, last_error) is its
own staged transaction, and `JobError::Io` still does not consume retry budget. A handler
panic is absorbed into the retry path exactly as now — T4 Step 5's isolation is what makes
that safe inside a batch.

- [ ] **Step 4: Implement the Cron arm drain** — same shape; recurring enqueuer and
fire-once catch-up semantics unchanged, no backfill.

- [ ] **Step 5: Verify the dual-write invariant still holds**

Run: `cargo test -p charpente --test kernel`, `cargo test -p charpente --test transactions`
Expected: green — especially the jobs ≡ committed txns property, which is the oracle most
likely to catch a batching mistake in the job arm.

- [ ] **Step 6: Commit**

```bash
git add crates/charpente/src/kernel
git commit -m "kernel: the job and cron arms drain too — the canonical txn's second record amortizes"
```

---

### Task 8: The property suite

**Files:**
- Modify: `crates/charpente/tests/group_commit.rs` — **created by T5, not by this task**
  (corrected 2026-08-12, pre-flight finding D). By the time T8 runs, that file already holds
  T5's `stage_or_subscribe_produces_a_record_without_touching_the_wal`, its counting-disk and
  disk-trace harness, and the `#[ignore]`d `capture_pre_batching_trace`, plus T6's tests.
  Reuse that harness; do not write a second one.

**Interfaces:**
- Consumes: everything above. Produces the oracles §9 requires.

Each property is RED-proven first. Properties 1 and 2 need discriminating twins — a naive
implementation that must go red — because without one a green result proves nothing.

- [ ] **Step 1: Property — the sync is atomic over the batch** (seed `0x60C1_0001`)

Crash at every point between the first append and the sync; assert **no record of the batch
is durable** and nothing was acked. Twin: an implementation that syncs mid-batch must fail
this. Prove the twin red by patching a sync into the append loop in a scratch worktree, then
revert.

- [ ] **Step 2: Property — read-your-own-writes across a batch** (seed `0x60C1_0002`)

Two requests in one batch both increment the same row. Assert the final value is 2, not 1.
Twin: hand the second transaction the bare snapshot; it must produce 1 and go red.

- [ ] **Step 3: Property — mid-batch apply failure** (seed `0x60C1_0003`)

Inject an apply failure at record *i* of *N*. Assert: records before *i* were acked 200;
record *i* and the tail got 500; the engine is poisoned and the kernel stopped; and **after
restart the durable tail applies**. Pin the "a 500 here may mean committed" contract by
asserting the post-restart state contains the tail's writes.

- [ ] **Step 4: Property — follower id identity under batching** (seed `0x60C1_0004`)

A two-host rig: primary commits batched, follower replays. Assert `next_job_id` and
`next_event_id` and every materialized id are byte-identical to a single-node run of the
same workload.

- [ ] **Step 5: Property — panic isolation at kernel level** (seed `0x60C1_0005`)

A route handler panics mid-batch: the other requests in the batch commit correctly and the
panicking one gets its 500.

- [ ] **Step 5b: Two coverage gaps handed here by T4's review — added 2026-08-12**

Both are branches that exist in shipped code and are exercised by **nothing in the repo**. A
conjunct that never runs is indistinguishable from one that is wrong, which is why these are
requirements rather than suggestions.

1. **Group-aware adoption** (seed `0x60C1_0006`). `Group::stage` consults `self.adopted`
   **union** the group's `staged_adoptions` so two staged transactions writing the same
   versioned table emit only one `SchemaVersion` row. Every fixture in the repo is
   *unversioned* (`Widget` in `charpente-core/tests/group_commit.rs` has no `RowShape`), so
   that union conjunct has never executed. Batch two writes to one **versioned** table and
   assert exactly one adoption row results. Also assert the negative-space case T4's review
   raised: a batch whose `commit` fails must not fold `staged_adoptions` into `db.adopted`,
   since that would be a durable-state lie about a batch that never landed.

2. **Enqueues and emits are invisible mid-batch** (seed `0x60C1_0007`) — a real behaviour
   change this sub-project introduces, and one nothing currently pins. The group overlay
   carries **rows only**; enqueues, emits and `migrate_puts` are not in it. Before batching,
   each transaction applied before the next took its snapshot, so B saw A's `charpente.jobs`
   row; now it does not. T4's review confirmed no platform code is affected (every
   `JobRow`/`EventRow` read in production goes through a `Snapshot`, never a `Tx`) — but app
   code can reach it, so pin the actual behaviour with one probe rather than leaving it to be
   discovered. T11 states it in DESIGN.

- [ ] **Step 5c: A subscribe now sees its later-arriving siblings' events — added 2026-08-12**

Seed `0x60C1_0008`. Found by T6's pre-flight and named nowhere else, including not by Step 5b.2,
which is about a *different* gap (enqueue/emit invisibility inside the group overlay **before**
commit; this one is about the reply loop reading the store **after** commit).

`Group::commit` calls `Engine::submit_all`, which appends, syncs **and applies every record**
before returning. So when the post-commit reply loop reaches a `Subscribe` item and runs
`validate_subscribe(self.app.db(), …)`, the store already reflects the whole batch — including
requests that arrived *after* that subscribe. Its `next_event_id` is therefore computed past
events emitted by later-arriving siblings. Pre-batching this could not happen: each request's own
commit was the only thing visible when its `validate_subscribe` ran.

**Assert the behaviour explicitly, and do not simply pin whatever the code happens to do.** Build
a batch with a fresh subscribe arriving *before* a write that emits an event, then state in the
test's own message which semantics is intended and why. If the answer is that the subscribe's
cursor excludes its sibling's event, that is a named contract and T11 documents it; if it
includes it, that is a behaviour change from today and T11 documents *that*. Either is
defensible — silence is not, and a test that only records the current answer without judging it
is the vacuity this plan has already caught three times.

- [ ] **Step 6: Run a full swarm batch**

Run: `cargo test -p charpente --test group_commit` and a 256-universe swarm.
Expected: green. Any failure gets its seed pinned per hard rule 3.

**Give no-starvation real attention here, not a glance** (T6 pre-flight, finding E). Batching is
not determinism-affecting — batch composition draws no entropy and reads no clock, so a fixed
seed replays identically — but one `Ready::Request` win now consumes up to 64 requests instead of
1, at unchanged win *frequency*. That is a genuine change to the fairness profile between the
Request arm and the Job/Cron arms, and spec §9 requires no-starvation to "hold unchanged". It is
the property most likely to degrade quietly under this sub-project.

- [ ] **Step 7: Commit**

```bash
git add crates/charpente/tests/group_commit.rs
git commit -m "test: group-commit property suite — six properties, twins for the two that need them"
```

---

### Task 9: Re-verify every pinned seed

**Files:**
- Modify: whichever test files carry pinned seeds (`crash_recovery.rs`, `wal.rs`,
  `kernel.rs`, `transactions.rs`, `replication.rs`, `realtime.rs`, `auth.rs`)
- Create: `.superpowers/sdd/<this-subproject>/pin-reverification.md`

**Interfaces:** consumes the finished implementation. Produces a per-seed verdict.

**Procedure per seed** (the migrations close-out's revert-guard proof):

- [ ] **Step 1: Enumerate every pinned seed**

**Amended 2026-08-12 (T6 pre-flight, finding F — and the miss is not marginal).** The command
originally written here was:

```bash
grep -rn "CHARPENTE_SEED\|0x[0-9a-f]\{16\}" crates/*/tests/*.rs | grep -i "seed\|pin\|regression"
```

`0x[0-9a-f]{16}` requires exactly sixteen lowercase hex digits and no underscores, so it matches
`0x8bee1752811ae873` and **nothing in this repo's dominant seed style**. Measured on the tree at
this commit: it finds **22 of 192** distinct seed literals and misses **170** — every
underscore-grouped seed (`0x0DED_0002`, `0x2EB1_0001`, `0x60C0_0003`, …) and every uppercase one.
A task whose entire purpose is re-verifying *every* pinned seed would have enumerated 11% of them
and reported completeness. Use:

```bash
grep -rnE "CHARPENTE_SEED|0x[0-9a-fA-F_]{8,}" crates/*/tests/*.rs | grep -i "seed\|pin\|regression"
```

Cross-check the count two ways before trusting it — the distinct-literal census
(`grep -rhoE '0x[0-9a-fA-F_]{8,}' crates/*/tests/*.rs | sort -u | wc -l`) against the line hits —
and record both. **Note the corrected regex over-matches on purpose**: it catches non-seed hex
constants (magic numbers, CRC polynomials such as `0x0000_0100_0000_01b3`, and byte patterns), so
the list needs a human pass. Over-matching is the correct direction of error here: an extra
literal costs one line of triage, a missed pin costs the guarantee this task exists to protect.

Record the list and what bug each was pinned for. A seed whose bug you cannot name from the
comments is itself a finding — record it.

- [ ] **Step 2: For each seed, run the revert-guard proof in an isolated worktree**

```bash
git worktree add /tmp/pin-check HEAD
# in the worktree: revert the historical fix that seed was pinned for
CHARPENTE_SEED=<seed> cargo test -p <crate> --test <file>
```
Expected: RED, for the seed's original reason. Record `PIN-EXERCISES-BUG`.

- [ ] **Step 3: For any seed that no longer goes red, hunt a replacement**

Run fresh-seed swarm batches against the reverted fix until a seed reproduces the original
bug. Pin the new seed **next to** the old one, keeping the old one with a comment naming
why it drifted (group commit changed the op schedule) and that it is retained as history.

- [ ] **Step 4: Clean up worktrees**

```bash
git worktree remove /tmp/pin-check && git worktree prune
```

- [ ] **Step 5: Commit**

```bash
git add crates
git commit -m "test: re-verify every pinned seed against group commit, re-pin what drifted"
```

---

### Task 10: Measure the win

**Files:** none modified — this task produces evidence.

- [ ] **Step 1: Run the full matrix on a settled box**

**Do not run this back-to-back with `cargo test --workspace`.** The T10 close-out of
sub-project 1 established over six runs that a box saturated by a just-finished 12-core cargo
workload makes child processes stall in `_dyld_start` behind Gatekeeper, which reads as a
benchmark failure. Let the box settle first.

Run: `scripts/bench.sh`
Expected: exit 0, `invalid: []`. Keep `target/bench/report.json`.

- [ ] **Step 2: Record the before/after**

The comparison is `wire --target bench-app` at 64 and 256 connections against sub-project 1's
~110 req/s, and `core`/`kernel` records/sec against 215/217. Record syncs/sec too — T1's gate
subject — and confirm it stayed at or below the device ceiling, which is the evidence that
the win came from amortization and not from skipping a sync.

**Wire-layer note (T1's post-review fix wave, 2026-08-10, review finding C-1).** The wire
layer's `syncs_per_sec` is DERIVED (`Target::syncs_per_request() * throughput`), not counted
— a client outside the process cannot count the server's real `wal.sync()` calls. That
derivation assumes one fsync per commit, which is exactly what batching breaks: once a
group-commit primary path amortizes N commits over one fsync, the wire layer has no way to
see the real count from outside the process, and `syncs_per_sec` will silently keep reporting
the pre-batching (unbatched) rate. **Do not fudge the wire number to make it agree with
core/kernel's real counts.** If the wire gate needs fixing at this point, land a
server-reported sync counter (surfaced over HTTP or in the child's exit output) or re-scope
the wire gate honestly and say so in BENCH.md — "we adjusted the check that would have caught
it, in the commit that made the number" is exactly the outcome spec §5 exists to forbid.

- [ ] **Step 3: Commit the artifact reference in the ledger** (no code commit).

---

### Task 11: Docs and close-out

**Files:**
- Modify: `docs/BENCH.md`, `DESIGN.md`, `docs/GUIDE.md`, `CLAUDE.md`, `PLAN.md`

- [ ] **Step 1: BENCH.md** — new numbers of record, the corrected gate in the method
section, and **§8's conditioning in the same breath as the win**: this device flatters group
commit, because a slow fsync keeps queues deep and greedy drain converts depth into batch
size, so a faster server yields smaller batches. State the win as *"removes the
one-fsync-per-transaction ceiling; on hardware whose fsync is slow enough to keep a queue
deep it recovers Nx"* — never as *"charpente is Nx faster"*. Bare metal stays owed.

  **Named site, carried from the T2 review (M-3), because this step's brief is about *new*
  numbers and would otherwise not sweep it:** `docs/BENCH.md:181` reads "`Engine::submit` is
  `wal.append` → `wal.sync()` → `apply`, one durable sync per transaction, no group commit."
  That sentence was true when written and is *still* true at T2 (nothing batches yet, and the
  length-1 path is byte-identical), but a reader following the citation already finds a
  different shape at that name, and it becomes **outright false** the moment T6/T7 land. Rewrite
  it against the shipped code — the one-sync-per-transaction claim is what the whole sub-project
  removes, so leaving it standing would be the repo's recurring defect class (a doc its own
  source disproves) sitting in the file that publishes the numbers.

- [ ] **Step 2: DESIGN.md** — the staged commit path, the two-level overlay, `submit_all`,
and the batch cap constant with its rationale.

- [ ] **Step 3: GUIDE.md** — the mid-batch 500 contract from §4, in the operator voice: a
500 never promises rejection, only absence of confirmation, and after a mid-batch apply
failure a 500 may name a write that committed and will appear after restart.

- [ ] **Step 4: CLAUDE.md** — the Current-state close-out entry and the "Next:" bullet.

- [ ] **Step 5: The close-out sweep**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build -p guide-examples
cargo test --workspace          # twice
CHARPENTE_SEED=0x8bee1752811ae873 cargo test -p charpente-sim --test crash_recovery
CHARPENTE_SEED=0x348270b38f166529 cargo test -p charpente --test kernel
# release mode-skew tripwires exactly as ci.yml runs them
scripts/edge-harness.sh         # replication + realtime behaviour is in the blast radius
# then, on a SETTLED box:
scripts/bench.sh --smoke
```
Pipe through `tee`, never `tail` — `tail` eats the aggregate counts and replaces cargo's exit
status with its own.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "bench: group commit — close out M2 sub-project 2"
```

---

## Self-Review

**Spec coverage:** §1 → T2/T4; §2 → T3/T4; §3 → T6/T7 (cap constant in Global Constraints);
§4 → T4/T6/T8.3, GUIDE in T11.3; §5 → T1; §6 → T8.4; §7 → T6.2/T9; §8 → T11.1; §9 → T8;
§10 non-goals — no task implements one; §11 exit criteria → T8/T9/T10/T11.

**Known soft spots, called out rather than hidden:**
- T9's scale is unknown until Step 1 enumerates the seeds. It is the task most likely to
  need splitting once its size is known.
- From T6 onward the frozen pinned seeds may legitimately change. That is T9's subject, not
  a regression to fix in place. Task 6 Step 4 says so explicitly, because an implementer who
  "fixes" a drifted seed destroys the evidence T9 needs.

**Resolved during pre-flight (2026-08-10), recorded so the reasoning is not re-litigated:**
- T1's no-behaviour-change test originally ran a healthy case without asserting the
  equality it claimed, so it would have passed no matter what T1 broke. It now asserts
  `device_ratio == records_ratio` directly.
- T5 originally ended with a test that stayed red until T6, which the review rubric treats
  as a defect. T5 now ends green on a T5-scoped assertion (staging touches no WAL) and the
  ack-ordering property moved to T6, the task that can actually violate it.
- T6's pre-batching trace constant was originally captured in T6, where the kernel is
  already batched — the assertion would have compared T6's output to itself. Capture moved
  to T5 Step 5, with an explicit instruction never to regenerate it later.
- Test helper names (`fresh_db`, `engine_with_sync_counter`, `run_universe_with_due_jobs`)
  follow existing suite conventions; implementers should reuse the real helpers in each file
  rather than create these if equivalents exist.

**Resolved during T1's post-review fix wave (2026-08-10), recorded so the reasoning is not
re-litigated:**
- T1's Interfaces line above ("the gate input becomes `syncs as f64 /
  elapsed.as_secs_f64()`") was written as if core, kernel and wire observe alike. They do
  not: core and kernel observe the process they measure directly; wire is a separate process
  and cannot count the server's syncs at all. This is a plan defect as much as an
  implementation one (review finding C-1) — Step 3's "Apply the same shape in
  `wire_accounting.rs`" is where it bit, and the shipped fix set `syncs: acked`, wrong by 2x
  for `Target::BenchApp`. Fixed by giving `Target` a `syncs_per_request()` method and deriving
  honestly (`Target::syncs_per_request() * throughput`) — still a DERIVED input, not a counted
  one, and it stays that way until a server-reported sync counter lands (see Task 10's
  wire-layer note above).
- `syncs` is the first field in these three obs structs to break the accounting-split spec's
  stated rule that "every field is a count, a duration or a verdict that was actually
  observed; nothing here is derived" (`core_accounting.rs:34-36`). At core and kernel it is
  sound (one commit, one fsync, no batching, so it really is what was observed); at wire it is
  a deliberate, documented exception. Recorded here rather than left implicit.

**Resolved during T3's pre-flight (2026-08-11), recorded so the reasoning is not
re-litigated.** Four defects, each verified against the code rather than inferred, and one
Noah ruling. They are the same class as the three the 2026-08-10 pre-flight caught: the plan's
test snippets name things that do not exist.

- **`GroupOverlay` was referenced by T3 and T4 and defined nowhere** — not in this plan, not
  in the spec, not in the code. T3's `Tx` field could not have compiled. Defined in T3's
  Interfaces above, as the alias spec §2 already determines.
- **`fresh_db` does not exist in `charpente-core`.** The 2026-08-10 pre-flight note above says
  to "reuse the real helpers in each file rather than create these if equivalents exist" —
  here no equivalent exists. It lives only in `charpente`'s `repl/follower.rs:508` and
  `repl/probe.rs:386`, a different crate. T4 writes its own; it must not become a third copy
  of those.
- **The `Widget { id, n }` fixture does not exist either**, and cannot be written with
  `#[table]`: core cannot use its own macro, which emits `::charpente::core::` paths
  (`tests/migrate_apply.rs:26` states this). Hand-written `Row`/`RowShape` impls are the
  established pattern (`tests/migration_gate.rs:12-44`). `db.rs`'s only `Widget` is a local
  struct inside a migration test with a `count` field, not a shared fixture.
  *(Historical wording — `{ id, n }` is what the plan said when this finding was written. The
  2026-08-12 pre-flight found T4's own snippets still split between `n` and `count` and
  unified them on **`count`**, which is also what the existing `db.rs` struct named in the
  sentence above already uses. T4's task body is now the live text.)*
- **T3's and T4's tests were homed in the wrong target.** `cargo test -p charpente-core --lib`
  cannot build a `Db`: `charpente_sim` appears nowhere in `charpente-core/src/`, because the
  `--lib` test target is a distinct compilation unit from the one `charpente-sim` links
  against and trait impls do not unify across them (documented in `tests/engine.rs`'s header,
  and evidenced by every `Db`-constructing test in this crate already being an integration
  test). Both moved to `crates/charpente-core/tests/group_commit.rs`.
- **Noah ruling (2026-08-11) on the consequence for T3:** an integration test cannot reach
  `pub(crate) Tx::over`, so T3's white-box isolation test is deferred to T4 rather than
  widening `over` to `pub`. Making it public would let an app holding a `&Db` mint a
  free-floating `Tx` from `Db::snapshot()`, write into it and drop it — writes lost silently,
  no error — re-opening the bug class `examples/invoicing/DEMO.md` exists to show is
  inexpressible from app code. T3's evidence is its zero-behaviour-change gate; the isolation
  property is asserted in T4, where `stage` makes it publicly observable.

**Seed allocation for the `0x60C0_xxxx` namespace, so a seed number identifies exactly one
test.** The plan as written assigned `0x60C0_0002` to T3 and `0x60C0_0003` to T4, both already
taken — `0x60C0_0002` collided with this plan's own T2 snippet before any implementer touched
it. Taken and committed: `0x60C0_0001` and `0x60C0_0002` (T2's two sync-count tests),
`0x60C0_0003` (T2's fix-wave poison/empty ordering test, commit `44dd8ae`).

**Re-allocated 2026-08-12 (T4 pre-flight), because the forward allocation below was written
but never applied to the task bodies.** T3 shipped no test, so `0x60C0_0004` fell to T4's
panic test. T5, T6 and T7 were all still carrying their pre-allocation numbers, which put
**three distinct tests on `0x60C0_0005`** (T4's read-your-own-writes, T5's op-counter, T6's
ack-ordering) and put T4's isolation test on the same seed as the T5/T6 trace pair — breaking
the very invariant the first line of this block states. The full, current allocation:

| seed | task | test |
|---|---|---|
| `0x60C0_0001`, `0x60C0_0002` | T2 | the two sync-count tests (committed) |
| `0x60C0_0003` | T2 fix wave | poison/empty ordering (committed, `44dd8ae`) |
| `0x60C0_0004` | T4 | `a_panicking_stage_leaves_the_group_clean` |
| `0x60C0_0005` | T4 | `a_staged_transaction_sees_an_earlier_one_in_the_same_group` |
| `0x60C0_0006` | T4 | `a_staged_transaction_that_fails_leaves_the_group_untouched` |
| `0x60C0_0007` | T5 | `app_with_op_counter` |
| `0x60C0_0008` | T5 **and** T6 | the pre-batching trace capture and its replay |
| `0x60C0_0009` | T6 | `run_two_request_batch` (ack ordering) |
| `0x60C0_000A` | T7 | `run_universe_with_due_jobs` |

`0x60C0_0008` appearing twice is **deliberate and must stay that way**: T5 captures that
universe's disk trace while the kernel is still unbatched and T6 replays the same universe
after batching to prove byte-identity. Two different seeds there would compare two different
universes and assert nothing. Later tasks continue from `0x60C0_000B`.
