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
| `crates/charpente/tests/group_commit.rs` | new property suite (T8, T9) |
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
/// Pre-group-commit, one acked unit costs exactly one fsync, so the gate's new
/// subject and its old one are the same number. This test is what proves T1
/// changed no behaviour; it is EXPECTED to be deleted by the task that makes
/// batching real, and its deletion is the signal that the gate's new framing
/// started earning its keep.
#[test]
fn syncs_equal_records_before_group_commit() {
    let w = WriteObs { ops: 500, loop_delta: 500, syncs: 500, /* ..as above.. */ };
    let a = write_accounting(Duration::from_secs(2), &w).unwrap();
    assert!(a.invalid.is_empty());
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
- Consumes: nothing.

**Why:** a group shares ONE snapshot across its staged transactions (`Snapshot` owns a
`redb::ReadTransaction` and is not cheap to duplicate per transaction). `group` is `None` in
this task and becomes `Some` in Task 4 — introducing the field now keeps Task 4's diff to
behaviour only.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn two_transactions_can_share_one_snapshot() {
    let mut db = fresh_db(0x60C0_0002);
    let snap = db.snapshot().expect("snapshot");
    let mut a = Tx::over(&snap);
    let mut b = Tx::over(&snap);
    a.put(&Widget { id: Id::from_raw(1), n: 1 });
    b.put(&Widget { id: Id::from_raw(2), n: 2 });
    assert_eq!(a.get::<Widget>(Id::from_raw(2)).unwrap(), None,
        "staged transactions are isolated until merged");
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `cargo test -p charpente-core --lib two_transactions_can_share_one_snapshot`
Expected: FAIL — no associated function `Tx::over`, and `Tx` owns its snapshot.

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

- [ ] **Step 1: Write the failing test — read-your-own-writes across the batch**

```rust
/// The property the whole two-level overlay exists for.
#[test]
fn a_staged_transaction_sees_an_earlier_one_in_the_same_group() {
    let mut db = fresh_db(0x60C0_0003);
    let mut g = db.begin_group().expect("group");
    g.stage(|tx| { tx.put(&Widget { id: Id::from_raw(1), n: 7 }); Ok::<_, ()>(()) })
        .expect("first stages");
    let seen = g
        .stage(|tx| Ok::<_, ()>(tx.get::<Widget>(Id::from_raw(1)).unwrap()))
        .expect("second stages");
    assert_eq!(
        seen,
        Some(Widget { id: Id::from_raw(1), n: 7 }),
        "B must see A's write, or a batch silently loses updates"
    );
    g.commit().expect("commit");
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `cargo test -p charpente-core --lib a_staged_transaction_sees_an_earlier_one`
Expected: FAIL — no method `begin_group`.

- [ ] **Step 3: Implement `Group`, `stage`, `commit`**

`commit` is:

```rust
pub fn commit(self) -> Result<(), TxError<std::convert::Infallible>> {
    let Group { db, records, staged_adoptions, .. } = self;
    if records.is_empty() {
        return Ok(());
    }
    let n = records.len();
    db.engine.submit_all(&records).map_err(TxError::Io)?;
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
    g.stage(|tx| { tx.put(&Widget { id: Id::from_raw(1), n: 1 }); Ok::<_, ()>(()) })
        .expect("first stages");
    let boom = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        g.stage(|tx| -> Result<(), ()> {
            tx.put(&Widget { id: Id::from_raw(2), n: 2 });
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
git add crates/charpente-core/src/db.rs
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

**Interfaces:**
- Produces: `App::stage_or_subscribe(&mut self, group: &mut Group, request: Request, now: u64)
  -> Handled` — identical to `handle_or_subscribe` except it stages into `group` instead of
  committing. `Handled::Subscribe` still commits nothing.
- Consumes: `Group` (T4).

**The mixed-batch case, which the spec under-specified:** a drained batch can contain both
write requests and SSE subscribes. Subscribes contribute no record and must still become
tails in arrival order. Responses for every request in the batch are held and sent only
after `commit` returns — sending a 200 before the sync would break ack-after-durable, which
is the entire point.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn no_response_is_sent_before_the_group_syncs() {
    // A two-request batch against a Disk that records op order. Assert that
    // every reply lands after the sync, not between the appends.
    let trace = run_two_request_batch(0x60C0_0005);
    let sync_at = trace.iter().position(|op| op == "sync").expect("a sync happened");
    let first_reply = trace.iter().position(|op| op == "reply").expect("a reply happened");
    assert!(
        first_reply > sync_at,
        "a reply was sent before the batch synced — ack-after-durable is broken: {trace:?}"
    );
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `cargo test -p charpente --test group_commit no_response_is_sent_before`
Expected: FAIL — the kernel still replies per request (this goes green in Task 6; keep it
red here and say so in the commit, or land Steps 3–4 first if the reviewer prefers one
green task — see Task 6 Step 1).

- [ ] **Step 3: Implement `stage_or_subscribe`**

Factor the existing `handle_or_subscribe` body so the routing/auth/handler-invocation path is
shared and only the commit differs. `handle_or_subscribe` becomes a wrapper that makes a
one-transaction group, exactly as `transact` did in T4 — same reasoning, one code path.

- [ ] **Step 4: Verify**

Run: `cargo test -p charpente --test http && cargo test -p charpente --test kernel`
Expected: green.

- [ ] **Step 5: Commit**

```bash
git add crates/charpente/src/http crates/charpente/src/lib.rs
git commit -m "http: stage_or_subscribe — dispatch into a group without committing"
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

- [ ] **Step 1: Make Task 5 Step 1's test pass**

Implement the drain, stage each request, `commit()`, then send replies. Run the T5 test:
Expected: PASS.

- [ ] **Step 2: Write the batch-of-1 identity test (RED against a naive drain)**

```rust
/// §7's structural guarantee under test: a universe that never queues two
/// requests must produce a byte-identical disk trace to the pre-batching
/// engine. This is what lets the frozen pinned seeds survive.
#[test]
fn a_never_contended_universe_traces_identically() {
    let batched = run_one_at_a_time_universe(0x60C0_0006);
    assert_eq!(batched.disk_trace, PRE_BATCHING_TRACE_0x60C00006,
        "batch-of-1 must be byte-identical to the unbatched path");
}
```

Capture `PRE_BATCHING_TRACE_*` by running the same workload at the T5 commit and pinning the
bytes. Record in the ledger that the constant was captured pre-batching, or it proves nothing.

- [ ] **Step 3: Handle the poison/stop path**

The existing request-arm ruling (stop when `Db::poisoned()` after a 500) now applies to a
batch: check once after `commit`, and if poisoned, send the tail's 500s and stop. Preserve
the store-latch-first ordering and both pinned message strings verbatim — `checks::classify`
matches on them.

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
    let g = run_universe_with_due_jobs(0x60C0_0007, 40);
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
- Create: `crates/charpente/tests/group_commit.rs`

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

- [ ] **Step 6: Run a full swarm batch**

Run: `cargo test -p charpente --test group_commit` and a 256-universe swarm.
Expected: green. Any failure gets its seed pinned per hard rule 3.

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

```bash
grep -rn "CHARPENTE_SEED\|0x[0-9a-f]\{16\}" crates/*/tests/*.rs | grep -i "seed\|pin\|regression"
```
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
- T5 Step 1's test is written in T5 and goes green in T6. The alternative was a task boundary
  that splits an untestable deliverable; the reviewer should accept T5 with a red test or ask
  for T5+T6 to merge.
- T6 Step 2's `PRE_BATCHING_TRACE_*` constant must be captured **before** batching lands, at
  the T5 commit. If captured after, it proves nothing. Said at the step.
- T9's scale is unknown until Step 1 enumerates the seeds. It is the task most likely to
  need splitting once its size is known.
- Test helper names (`fresh_db`, `engine_with_sync_counter`, `run_universe_with_due_jobs`)
  follow existing suite conventions; implementers should reuse the real helpers in each file
  rather than create these if equivalents exist.
