# Per-Batch Apply Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the store one commit point per batch instead of one per record, so a batch of N applies atomically in a single redb write transaction.

**Architecture:** `Store` grows `apply_batch`, which opens one write transaction and drives a per-record closure inside it; the per-record body is extracted verbatim into a free function `apply_one_in_txn` that both `apply` and `apply_batch` call, so there is exactly one copy of the counter negative-space checks. `StateMachine` grows an `apply_batch` with a default per-record impl (today's semantics, unchanged) that only `TableMachine` overrides. `Engine::submit_all` and `submit_batch` call it once per batch instead of looping, and advance `last_applied` strictly by the count the call reports.

**Tech Stack:** Rust, redb 4.1.0 behind the `Disk` trait, `charpente-sim` (seeded xoshiro, virtual time, fault matrix).

**Spec:** `docs/superpowers/specs/2026-08-20-apply-batching-design.md` — read it before Task 1. The plan argues from the spec; where they disagree, the spec wins and the conflict gets a ledger ruling.

## Global Constraints

- **Hard rule 1 — no ambient nondeterminism in core.** No wall clock, OS randomness, raw fs/net, or thread/task spawn in `charpente-core` or `charpente`. All IO through `Clock`/`Entropy`/`Disk`/`Net`. Enforced by clippy `disallowed_methods`/`disallowed_types`; do not add an `#[allow]` to silence it.
- **Hard rule 2 — no escape hatches.** No feature flag, config knob, or "fast path" that selects between batched and unbatched apply. One code path.
- **Hard rule 3 — sim-first.** Every core subsystem lands with seeded fault-injection tests in the SAME commit. A bug found in sim gets its seed recorded.
- **Hard rule 4 — TigerBeetle-style assertions.** Assert invariants and negative space liberally; assertions stay on in release.
- **Hard rule 5 — error messages are product surface.** `REHELLO_MARKER` and the poison string are matched by other code (`follower.rs`, `checks::classify`). Do not reword any existing error or assert message in this plan's scope.
- **Hard rule 6 — public-API changes update docs in the same commit.** `StateMachine` is public API.
- **Style:** plain direct Rust, smallest code that does the job, no speculative abstraction. Comments only for constraints the code cannot express.
- **Verification bar:** for core changes the bar is "sim suite green over N fresh seeds," not "unit tests pass." State N in the commit message.
- **Commits:** directly to `main`, **no `Co-Authored-By` trailers**.
- **Only `docs/BENCH.md` may carry quotable performance numbers.** A figure anywhere else is an observation, not provenance.
- **`DECISIONS.md` is edited by Noah only.** Never by an implementer.
- **zsh, not bash:** `PIPESTATUS` is empty in every case. Use `${pipestatus[1]}` or `set -o pipefail`. Pipe cargo through `tee`, never `tail` (`tail` eats the aggregate counts).
- **Never share `CARGO_TARGET_DIR` across worktrees.** Revert proofs run in an isolated worktree with its own target dir.
- **Full check, the same three commands as CI:** `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`.

---

## File Structure

| File | Responsibility | Tasks |
|---|---|---|
| `crates/charpente-core/src/table.rs` | `apply_one_in_txn` (new free fn), `Store::apply` (delegates), `Store::apply_batch` (new) | 1, 2 |
| `crates/charpente-core/src/engine.rs` | `StateMachine::apply_batch` default, `submit_all`/`submit_batch` rewire | 3 |
| `crates/charpente-core/src/db.rs` | `TableMachine::apply_batch` override; `apply` becomes a one-line delegate | 4 |
| `crates/charpente/tests/typed_tables.rs` | `Store::apply_batch` unit tests (atomicity, gapless, read-through) | 2 |
| `crates/charpente-core/tests/engine.rs` | default-impl equivalence, batch advance/failure | 3 |
| `crates/charpente/tests/transactions.rs` | store-boundary invariant inside the existing `tx_workload` swarm | 5 |
| `crates/charpente/tests/replication.rs` | `submit_batch` atomicity, `REHELLO_MARKER` intact | 5 |
| `crates/charpente/tests/group_commit.rs` | batch-of-1 identity — **kept green, never re-captured** | 1, 6 |
| `docs/GUIDE.md`, `DESIGN.md` | public `StateMachine` surface (hard rule 6) | 3 |
| `docs/BENCH.md` | re-measured numbers | 7 |

---

## Task 1: Extract the per-record apply body (pure refactor, zero behavior change)

The whole point of this task is that **nothing changes**. It exists as its own task so a reviewer can reject the extraction independently of the batching that follows, and so the batch-of-1 identity fixture proves the extraction inert before any semantics move.

**Files:**
- Modify: `crates/charpente-core/src/table.rs:348-413` (`Store::apply`)

**Interfaces:**
- Consumes: nothing.
- Produces: `fn apply_one_in_txn<T>(txn: &redb::WriteTransaction, seq: u64, f: impl FnOnce(&mut Writer<'_>) -> io::Result<T>) -> io::Result<T>` — private to `table.rs`. Task 2 calls it in a loop.

- [ ] **Step 1: Confirm the body is `self`-free before touching it**

The extraction is only sound because the body between `begin_write` and `commit` touches no `Store` state. Prove it, don't assume it:

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
sed -n '361,411p' crates/charpente-core/src/table.rs | grep -n "self\." || echo "PROVEN: no self. references in the extractable body"
```

Expected: `PROVEN: no self. references in the extractable body`. If any `self.` appears, **stop and report** — the extraction premise is false and the design needs revisiting.

- [ ] **Step 2: Add the free function**

Insert immediately **above** `impl Store` (or directly above the `apply` method's `impl` block) in `crates/charpente-core/src/table.rs`. Move the body verbatim from the existing `apply` — same asserts, same message text, same order:

```rust
/// One record's effects inside an already-open write transaction: the
/// negative-space counter checks and the `applied_seq` bump. Free rather than
/// a method because it touches no `Store` state — which is exactly what lets a
/// batch hold one transaction across many calls (`apply_batch`).
fn apply_one_in_txn<T>(
    txn: &redb::WriteTransaction,
    seq: u64,
    f: impl FnOnce(&mut Writer<'_>) -> io::Result<T>,
) -> io::Result<T> {
    let (next_job_id_before, next_event_id_before) = {
        let meta = txn.open_table(META).map_err(redb_err)?;
        (
            meta.get("next_job_id")
                .map_err(redb_err)?
                .map_or(0, |g| g.value()),
            meta.get("next_event_id")
                .map_err(redb_err)?
                .map_or(0, |g| g.value()),
        )
    };
    let mut writer = Writer {
        txn,
        jobs_materialized: 0,
        events_materialized: 0,
    };
    let out = f(&mut writer)?;
    let mut meta = txn.open_table(META).map_err(redb_err)?;
    // Negative-space: next_job_id is a monotonic counter bumped only
    // by materialize_job. It must never move except by exactly one
    // per job materialized in this apply — never backwards, never
    // skipping, never double-counting a single effect.
    let next_job_id_after = meta
        .get("next_job_id")
        .map_err(redb_err)?
        .map_or(0, |g| g.value());
    assert_eq!(
        next_job_id_after,
        next_job_id_before + writer.jobs_materialized,
        "next_job_id moved from {next_job_id_before} to {next_job_id_after} in apply({seq}), \
         which materialized {} job(s) — the counter must advance by exactly one per job",
        writer.jobs_materialized
    );
    // Negative-space: same rule as next_job_id, for next_event_id and
    // materialize_event.
    let next_event_id_after = meta
        .get("next_event_id")
        .map_err(redb_err)?
        .map_or(0, |g| g.value());
    assert_eq!(
        next_event_id_after,
        next_event_id_before + writer.events_materialized,
        "next_event_id moved from {next_event_id_before} to {next_event_id_after} in \
         apply({seq}), which materialized {} event(s) — the counter must advance by \
         exactly one per event",
        writer.events_materialized
    );
    meta.insert("applied_seq", seq).map_err(redb_err)?;
    Ok(out)
}
```

Note the one mechanical difference from the original: the original wrote `Writer { txn: &txn, .. }` because `txn` was owned there; here the parameter is already a reference, so it is `Writer { txn, .. }`.

- [ ] **Step 3: Reduce `Store::apply` to call it**

Replace the whole body of `Store::apply` (currently `crates/charpente-core/src/table.rs:348-413`) with:

```rust
    pub fn apply<T>(
        &self,
        seq: u64,
        f: impl FnOnce(&mut Writer<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        let applied = self.applied_seq()?;
        assert_eq!(
            seq,
            applied.map_or(0, |s| s + 1),
            "Store::apply({seq}) but applied_seq is {applied:?} — apply must be gapless; replay the WAL in order"
        );
        let mut txn = self.db.begin_write().map_err(redb_err)?;
        txn.set_durability(Durability::None).map_err(redb_err)?;
        let out = apply_one_in_txn(&txn, seq, f)?;
        txn.commit().map_err(redb_err)?;
        Ok(out)
    }
```

Keep the existing doc comment on `apply` exactly as it is. The error path is unchanged: an `Err` from `apply_one_in_txn` returns early, dropping `txn`, which rolls back — the same thing the original `?` inside the block did.

- [ ] **Step 4: Run the full check**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tee /tmp/t1-clippy.log
cargo test --workspace 2>&1 | tee /tmp/t1-test.log; echo "exit=${pipestatus[1]}"
```

Expected: all green, `exit=0`.

- [ ] **Step 5: Prove the refactor inert against the identity fixture**

This is the task's real gate — a byte-for-byte disk trace comparison:

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
cargo test -p charpente --test group_commit -- a_never_contended_universe_traces_identically_to_the_pre_batching_fixture --exact --nocapture
```

Expected: PASS.

**If it fails, STOP and report.** Do NOT run `capture_pre_batching_trace` and do NOT edit `crates/charpente/tests/fixtures/pre-batching-trace.txt`. A pure refactor that moves the disk trace is not a pure refactor, and re-capturing the fixture would destroy the only oracle that proves batch-of-1 identity later. The test's own doc comment says this in as many words.

- [ ] **Step 6: Commit**

```bash
git add crates/charpente-core/src/table.rs
git commit -F - <<'EOF'
refactor: extract the per-record apply body so a batch can share one txn

Store::apply's body between begin_write and commit touches no Store state
(proven, not assumed: grep for `self.` over the extracted range is empty), so
it lifts out whole as a free fn. apply now opens the txn, calls it once, and
commits — same asserts, same message text, same order, same rollback-on-error.

Inert by construction and proven inert by measurement: the never-contended
universe still traces byte-identically to the pre-batching fixture, which is
the same oracle that will prove batch-of-1 identity once apply_batch lands.
EOF
```

---

## Task 2: `Store::apply_batch` — one transaction, all or nothing

**Files:**
- Modify: `crates/charpente-core/src/table.rs` (add `apply_batch` to `impl Store`, directly below `apply`)
- Test: `crates/charpente/tests/typed_tables.rs`

**Interfaces:**
- Consumes: `apply_one_in_txn` from Task 1.
- Produces: `pub fn apply_batch(&self, records: &[(u64, &[u8])], f: impl FnMut(&mut Writer<'_>, u64, &[u8]) -> io::Result<()>) -> io::Result<()>` — Task 4 calls it.

- [ ] **Step 1: Write the failing tests**

Append to `crates/charpente/tests/typed_tables.rs`. `quiet_store()` already exists at line 50 — use it.

```rust
/// A batch is one commit point (MANIFESTO §5): a failure anywhere in it must
/// leave the store exactly where it started, not partway through.
#[test]
fn apply_batch_rolls_back_every_record_when_one_fails() {
    let (_sim, store) = quiet_store();
    store.apply(0, |w| w.put(&invoice(1, 1, 100))).unwrap();

    let records: Vec<(u64, &[u8])> = vec![(1, b"a"), (2, b"b"), (3, b"c")];
    let err = store
        .apply_batch(&records, |w, seq, _payload| {
            if seq == 3 {
                return Err(io::Error::other("the third record refuses"));
            }
            w.put(&invoice(seq + 10, 1, 500))
        })
        .expect_err("the third record's failure must fail the whole batch");
    assert_eq!(err.to_string(), "the third record refuses");

    assert_eq!(
        store.applied_seq().unwrap(),
        Some(0),
        "a failed batch must not move applied_seq — the store is a materialization \
         of the log, and a partly-applied batch corresponds to no single commit point"
    );
    let snap = store.snapshot().unwrap();
    assert_eq!(
        snap.scan::<Invoice>().unwrap().len(),
        1,
        "records 1 and 2 succeeded inside the txn but must roll back with record 3"
    );
}

/// The happy path: every record lands and applied_seq ends at the last seq.
#[test]
fn apply_batch_commits_every_record_and_ends_at_the_last_seq() {
    let (_sim, store) = quiet_store();

    let records: Vec<(u64, &[u8])> = vec![(0, b"a"), (1, b"b"), (2, b"c")];
    store
        .apply_batch(&records, |w, seq, _payload| {
            w.put(&invoice(seq + 1, 1, 100))
        })
        .expect("all three records apply");

    assert_eq!(store.applied_seq().unwrap(), Some(2));
    assert_eq!(store.snapshot().unwrap().scan::<Invoice>().unwrap().len(), 3);
}

/// Gapless is preserved, not traded away: it used to cost a read txn per
/// record, and now costs one read txn plus a running counter. Both shapes of
/// violation must still fire — a wrong first seq, and a gap mid-batch.
#[test]
#[should_panic(expected = "apply must be gapless")]
fn apply_batch_rejects_a_wrong_first_seq() {
    let (_sim, store) = quiet_store();
    let records: Vec<(u64, &[u8])> = vec![(7, b"a")];
    let _ = store.apply_batch(&records, |w, _seq, _p| w.put(&invoice(1, 1, 1)));
}

#[test]
#[should_panic(expected = "apply must be gapless")]
fn apply_batch_rejects_a_gap_in_the_middle_of_a_batch() {
    let (_sim, store) = quiet_store();
    let records: Vec<(u64, &[u8])> = vec![(0, b"a"), (1, b"b"), (3, b"c")];
    let _ = store.apply_batch(&records, |w, seq, _p| w.put(&invoice(seq + 1, 1, 1)));
}

/// Spec §3's new invariant, from the reader's side: a read taken through the
/// Writer inside a batch sees the batch's OWN writes, because it reads through
/// the batch's transaction. This is why in-closure reads are correct by
/// construction and need no assertion — and this test is what makes that
/// claim falsifiable rather than asserted in a comment.
#[test]
fn a_read_through_the_writer_inside_a_batch_sees_the_batch_so_far() {
    let (_sim, store) = quiet_store();
    let records: Vec<(u64, &[u8])> = vec![(0, b"a"), (1, b"b")];
    let mut seen = Vec::new();
    store
        .apply_batch(&records, |w, seq, _payload| {
            seen.push(w.applied_seq()?);
            w.put(&invoice(seq + 1, 1, 100))
        })
        .expect("both records apply");

    assert_eq!(
        seen,
        vec![None, Some(0)],
        "the second record must observe the first record's applied_seq bump — a \
         fresh read txn opened mid-batch would have shown None twice"
    );
}

/// An empty batch must cost nothing: no read txn, no write txn, no commit.
#[test]
fn apply_batch_of_nothing_is_a_no_op() {
    let (_sim, store) = quiet_store();
    store
        .apply_batch(&[], |w, _s, _p| w.put(&invoice(1, 1, 1)))
        .expect("an empty batch is a no-op");
    assert_eq!(store.applied_seq().unwrap(), None);
}
```

- [ ] **Step 2: Run them to verify they fail**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
cargo test -p charpente --test typed_tables -- apply_batch a_read_through_the_writer 2>&1 | tee /tmp/t2-red.log
```

Expected: FAIL to **compile** — `no method named `apply_batch` found for struct `Store``. A compile failure is the correct red here; the method does not exist yet.

- [ ] **Step 3: Implement `apply_batch`**

Add to `impl Store` in `crates/charpente-core/src/table.rs`, directly below `apply`:

```rust
    /// Applies a contiguous run of records in ONE write transaction: all of
    /// them commit or none of them do. `apply` is this with `records.len() ==
    /// 1`, and does exactly the same disk work in that case (one read txn, one
    /// write txn, one commit) — which is what makes a never-contended universe
    /// replay byte-identically to pre-batching code.
    ///
    /// The gapless invariant is preserved, not traded: it used to cost a read
    /// transaction per record, and now costs one read plus a running counter.
    pub fn apply_batch(
        &self,
        records: &[(u64, &[u8])],
        mut f: impl FnMut(&mut Writer<'_>, u64, &[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let applied = self.applied_seq()?;
        let mut expected = applied.map_or(0, |s| s + 1);
        let mut txn = self.db.begin_write().map_err(redb_err)?;
        txn.set_durability(Durability::None).map_err(redb_err)?;
        for (seq, payload) in records {
            assert_eq!(
                *seq, expected,
                "Store::apply_batch reached seq {seq} but expected {expected} — apply must be gapless; replay the WAL in order"
            );
            apply_one_in_txn(&txn, *seq, |w| f(w, *seq, payload))?;
            expected += 1;
        }
        txn.commit().map_err(redb_err)?;
        Ok(())
    }
```

Two things that are deliberate, not oversights. `applied_seq` is called once for the whole batch, then a running `expected` carries the same check across the loop — that is the read-transaction saving. And `applied_seq` is inserted into META once **per record** rather than once at the end: it costs an in-memory btree update, and it is what makes `Writer::applied_seq` inside record *k* observe record *k-1* (the test in Step 1).

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
cargo test -p charpente --test typed_tables 2>&1 | tee /tmp/t2-green.log; echo "exit=${pipestatus[1]}"
```

Expected: all pass, `exit=0`.

- [ ] **Step 5: Full check + identity fixture still green**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace 2>&1 | tee /tmp/t2-ws.log; echo "exit=${pipestatus[1]}"
```

Expected: green. Nothing calls `apply_batch` yet, so the fixture cannot have moved.

- [ ] **Step 6: Commit**

```bash
git add crates/charpente-core/src/table.rs crates/charpente/tests/typed_tables.rs
git commit -F - <<'EOF'
feat: Store::apply_batch — one write transaction for a contiguous run

All or nothing: a failure anywhere rolls the whole batch back, so the store
never sits in a state that corresponds to no single commit point (MANIFESTO
§5). Gapless is preserved rather than traded — one applied_seq read plus a
running counter replaces one read transaction per record, and both shapes of
violation (wrong first seq, mid-batch gap) still fire.

applied_seq is bumped per record inside the batch, not once at the end, so a
read taken through the Writer sees the batch's own writes; the test pins that
(None, Some(0)) rather than leaving it to a comment.

Nothing calls this yet.
EOF
```

---

## Task 3: `StateMachine::apply_batch` + engine rewire

**Files:**
- Modify: `crates/charpente-core/src/engine.rs:16-18` (trait), `:195-238` (`submit_all`), `:246-280` (`submit_batch`)
- Test: `crates/charpente-core/tests/engine.rs`
- Docs: `docs/GUIDE.md`, `DESIGN.md` if either shows the `StateMachine` trait (hard rule 6)

**Interfaces:**
- Consumes: nothing from Tasks 1-2 (this is the engine side).
- Produces: `fn apply_batch(&mut self, records: &[(u64, &[u8])]) -> Result<(), SubmitFailure>` on `StateMachine`, with a default impl. Task 4 overrides it.

- [ ] **Step 1: Write the failing tests**

Append to `crates/charpente-core/tests/engine.rs`:

```rust
/// The default `apply_batch` must reproduce today's per-record semantics
/// exactly — including the partial count, which is the whole reason
/// `SubmitFailure` carries one. `FailsAfter` takes the default impl, so this
/// is the contrast against `TableMachine`'s all-or-nothing override.
#[test]
fn the_default_apply_batch_applies_per_record_and_reports_the_partial_count() {
    // No Sim and no disk: FailsAfter is pure, and the default impl is the
    // thing under test, not the store beneath it.
    let mut machine = FailsAfter { remaining: 2 };
    let records: Vec<(u64, &[u8])> = vec![(0, b"a"), (1, b"b"), (2, b"c")];
    let failure = machine
        .apply_batch(&records)
        .expect_err("the third record's apply must fail");
    assert_eq!(
        failure.applied, 2,
        "the default impl applies per record, so two really did land"
    );
    assert_eq!(failure.error.to_string(), "configured failure");
}

/// The engine advances last_applied by the count the call reports and by
/// nothing else — never inside a loop it no longer has.
#[test]
fn submit_all_advances_last_applied_to_the_last_record_of_the_batch() {
    let sim = Sim::with_faults(0x60C5_0002, FaultConfig::none());
    let mut engine =
        Engine::recover(Box::new(sim.disk()), wal_cfg(), Counter::default(), None).unwrap();

    let seqs = engine.submit_all(&[b"a", b"b", b"c"]).expect("all apply");
    assert_eq!(seqs, vec![0, 1, 2]);
    assert_eq!(
        engine.last_applied(),
        Some(2),
        "last_applied ends at the batch's last seq"
    );
    assert_eq!(
        engine.machine().applied.len(),
        3,
        "every record reached the machine"
    );
}
```

**Do not modify `submit_all_reports_how_many_applied_before_a_mid_batch_failure`** (`crates/charpente-core/tests/engine.rs:353`). It uses `FailsAfter`, which takes the default impl, so its existing expectations — `failure.applied == 2` and `engine.last_applied() == Some(1)` — must stay green **byte-unchanged**. That test is this task's real equivalence gate. If it goes red, the rewire changed default-impl semantics and the fix belongs in the engine, not in the test.

- [ ] **Step 2: Run to verify they fail**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
cargo test -p charpente-core --test engine -- the_default_apply_batch submit_all_advances 2>&1 | tee /tmp/t3-red.log
```

Expected: FAIL to compile — `no method named `apply_batch``.

- [ ] **Step 3: Add the trait method**

Replace the `StateMachine` trait at `crates/charpente-core/src/engine.rs:16-18`:

```rust
pub trait StateMachine {
    fn apply(&mut self, seq: u64, payload: &[u8]) -> io::Result<()>;

    /// Applies a contiguous run of records. On failure, `SubmitFailure::applied`
    /// reports how many of them are durably reflected in machine state; an
    /// implementation may apply all or none, so a caller must not assume either
    /// — `TableMachine` is atomic and reports 0, this default is per-record and
    /// reports the boundary.
    fn apply_batch(&mut self, records: &[(u64, &[u8])]) -> Result<(), SubmitFailure> {
        for (i, (seq, payload)) in records.iter().enumerate() {
            if let Err(error) = self.apply(*seq, payload) {
                return Err(SubmitFailure { applied: i, error });
            }
        }
        Ok(())
    }
}
```

The error type is the existing `SubmitFailure` (`engine.rs:53-56`), not a new struct: it is the same shape, in the same module, and `submit_all` already builds exactly this value at every error site, so reusing it removes a conversion instead of adding one. A `Result<usize>` here would discard the error on a partial apply and let the engine report records as landed that had not — the spec records that trap in §1.

- [ ] **Step 4: Rewire `submit_all`**

Replace the apply loop at `crates/charpente-core/src/engine.rs:227-238` (from `for (i, (seq, payload)) in seqs.iter()...` through `Ok(seqs)`) with:

```rust
        let batch: Vec<(u64, &[u8])> = seqs.iter().copied().zip(records.iter().copied()).collect();
        assert_eq!(
            batch[0].0,
            self.last_applied.map_or(0, |l| l + 1),
            "apply loop skipped a seq — append and apply have diverged"
        );
        match self.machine.apply_batch(&batch) {
            Ok(()) => {
                self.last_applied = Some(batch[batch.len() - 1].0);
                Ok(seqs)
            }
            Err(SubmitFailure { applied, error }) => {
                self.poisoned = true;
                if applied > 0 {
                    self.last_applied = Some(batch[applied - 1].0);
                }
                Err(SubmitFailure { applied, error })
            }
        }
```

`batch[0]` is safe: the `records.is_empty()` early return above guarantees at least one record. The assert message is unchanged verbatim (hard rule 5). `last_applied` moves only by the reported count — on the error path those `applied` records really did land under the default impl, so failing to advance would make recovery replay already-applied records.

- [ ] **Step 5: Rewire `submit_batch`**

Replace the apply loop at `crates/charpente-core/src/engine.rs:265-278` (from `for (seq, payload) in frames {` — the **second** such loop, the one after `self.wal.sync()?` — through `Ok(())`) with:

```rust
        let batch: Vec<(u64, &[u8])> = frames
            .iter()
            .map(|(seq, payload)| (*seq, payload.as_slice()))
            .collect();
        if let Some((first, _)) = batch.first() {
            assert_eq!(
                *first,
                self.last_applied.map_or(0, |l| l + 1),
                "apply loop skipped a seq — append and apply have diverged"
            );
        }
        match self.machine.apply_batch(&batch) {
            Ok(()) => {
                if let Some((last, _)) = batch.last() {
                    self.last_applied = Some(*last);
                }
                Ok(())
            }
            Err(SubmitFailure { applied, error }) => {
                self.poisoned = true;
                if applied > 0 {
                    self.last_applied = Some(batch[applied - 1].0);
                }
                Err(error)
            }
        }
```

Unlike `submit_all`, this path handles an empty `frames`: its doc says the empty case is guarded upstream at the peer boundary and it **does** sync on empty, so the guards here are `if let Some(..)` rather than an early return. Do not "reconcile" the two paths by adding an empty-batch early return — `submit_all`'s own comment forbids exactly that reconciliation in the other direction.

The seq-validation loop above (the first `for (seq, payload) in frames` block, with the `REHELLO_MARKER` error) is **untouched**.

- [ ] **Step 6: Run the tests**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
cargo test -p charpente-core --test engine 2>&1 | tee /tmp/t3-green.log; echo "exit=${pipestatus[1]}"
```

Expected: all pass including the unmodified `submit_all_reports_how_many_applied_before_a_mid_batch_failure`.

- [ ] **Step 7: Update the docs (hard rule 6)**

`StateMachine` is public API and just grew a method. Check both docs and update wherever the trait is shown:

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
grep -n "StateMachine" docs/GUIDE.md DESIGN.md
```

If `docs/GUIDE.md` shows the trait in a ```rust block, remember `crates/guide-examples`'s build.rs compiles every such block — verify with `cargo build -p guide-examples`. Add one sentence where the trait is documented: `apply_batch` has a default per-record impl, so existing implementors need no change, and an implementor that overrides it may apply all-or-none.

- [ ] **Step 8: Full check and commit**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace 2>&1 | tee /tmp/t3-ws.log; echo "exit=${pipestatus[1]}"
cargo build -p guide-examples
git add crates/charpente-core/src/engine.rs crates/charpente-core/tests/engine.rs docs/ DESIGN.md
git commit -F - <<'EOF'
feat: StateMachine::apply_batch, and the engine calls it once per batch

The trait grows apply_batch with a default per-record impl, so all six test
fixtures keep today's exact semantics — including the partial count, which
submit_all_reports_how_many_applied_before_a_mid_batch_failure still pins
byte-unchanged.

It returns Result<(), SubmitFailure> rather than Result<usize>: the engine
needs the count AND the error to build its failure, and a bare count would let
a partial apply read as success and report records as landed that had not.

submit_all and submit_batch now advance last_applied strictly by the count the
call reports — records.len() on success, exactly `applied` on failure — instead
of walking it forward inside a loop they no longer have. Both assert messages
and submit_batch's REHELLO_MARKER validation are unchanged.
EOF
```

---

## Task 4: `TableMachine::apply_batch` — the atomic override

**Files:**
- Modify: `crates/charpente-core/src/db.rs:722-846` (`impl StateMachine for TableMachine`)

**Interfaces:**
- Consumes: `Store::apply_batch` (Task 2), `StateMachine::apply_batch` (Task 3).
- Produces: nothing downstream; this is the last production-code task.

- [ ] **Step 1: Understand what must move, and why the delegation direction matters**

`TableMachine::apply` currently (db.rs:723-846) does five things in order: the `skip_through` early return; `decode_record`; disjoint field borrows of `schema`/`versions`/`saw_below`; `self.store.apply(seq, |w| {...})` returning `new_ids`; and `self.enqueued.extend(new_ids)` **after** the store commits.

The new `apply_batch` becomes the single home for that body, and `apply` becomes a one-line delegate. That direction — not duplicating the body into a second method — is what keeps one copy of a hundred-line closure, and batch-of-1 identity is what makes it safe.

Three constraints the move must honor:

1. **`enqueued` extends only after the batch commits.** A rolled-back batch must not enqueue jobs for records the store never took. This is why `new_ids` is collected into a local and drained after, not pushed into `self.enqueued` inside the closure.
2. **Skipped records are a contiguous prefix.** `skip_through` is set from `store.applied_seq()` (db.rs:1277), so records with `seq <= skip_through` are exactly the ones the store already holds; the remainder starts at `skip_through + 1` and is gapless from there. Split the batch at that boundary and hand only the live tail to `Store::apply_batch`.
3. **`versions` and `saw_below` are in-memory and are mutated inside the closure.** If the batch rolls back, they keep mutations the store discarded. That divergence is acceptable *only* because the engine poisons on this path and the process must restart and recover — the same reasoning that already covers a single failed apply today. Say so in a comment; do not attempt to roll them back.

- [ ] **Step 2: Write the failing test**

This one needs a real `Store`, so it goes in `crates/charpente/tests/transactions.rs`, which already has `quiet_db(seed) -> (Sim, Db)` at line 95 and the `Store`/`Codec`/`Id` imports. Drive `TableMachine` through the engine so the whole path is exercised.

The cheapest forced mid-batch failure is a payload that is not a valid encoded record: `decode_record` runs **inside** the closure, so record 3 fails after records 1 and 2 have already written into the shared transaction — exactly the interleaving atomicity has to survive.

```rust
/// TableMachine is atomic where the default impl is per-record: a failure
/// anywhere reports applied == 0, because the transaction rolled every record
/// back. This is the contrast that makes the trait contract's "may apply all
/// or none" a real distinction rather than a hedge — and the enqueued
/// assertion is the one that catches a job surviving a rolled-back batch.
#[test]
fn a_failed_table_machine_batch_reports_zero_applied_and_enqueues_nothing() {
    let (sim, mut db) = quiet_db(0x7AB1_E001);

    // Two well-formed records that materialize a job, then one that cannot
    // decode. Build the first two through the normal encode path so they are
    // genuinely valid, and hand the third deliberate garbage.
    db.transact(|tx| apply_txn(tx, 0)).expect("a normal commit");
    let applied_before = db.last_applied();

    let mut machine = db.into_seeder().into_machine();
    let good = encoded_record_that_materializes_a_job();
    let garbage: &[u8] = b"\xff\xff not a record";
    let next = applied_before.map_or(0, |s| s + 1);
    let records: Vec<(u64, &[u8])> = vec![
        (next, good.as_slice()),
        (next + 1, good.as_slice()),
        (next + 2, garbage),
    ];

    let failure = machine
        .apply_batch(&records)
        .expect_err("the third record cannot decode, so the batch must fail");

    assert_eq!(
        failure.applied, 0,
        "TableMachine is all-or-nothing: a rolled-back batch applied nothing, \
         so there is no mid-batch boundary to report"
    );
    assert_eq!(
        machine.store().applied_seq().unwrap(),
        applied_before,
        "a failed batch must not move applied_seq"
    );
    assert!(
        machine.drain_enqueued().is_empty(),
        "records 1 and 2 materialized jobs inside the transaction, but the \
         transaction rolled back — enqueueing them would hand the job runner \
         ids for rows the store never took"
    );
    drop(sim);
}
```

**Adapt the three helper calls to what `transactions.rs` actually exposes** — `into_seeder().into_machine()`, `machine.store()`, and `machine.drain_enqueued()` are the shape this test needs, not necessarily the names that exist. If `TableMachine` is not reachable from an integration test, drive the same three assertions through `Db` instead: commit two records, then feed the batch via the replication path (`Db::apply_shipped`, db.rs:1662), which reaches `submit_batch` → `apply_batch` with the same atomicity contract. Assert the same three properties either way; **the `drain_enqueued` assertion is non-negotiable** — it is the only test that catches constraint 1, and constraint 1 is the one a straightforward implementation gets wrong.

- [ ] **Step 3: Rewrite the impl**

Replace the whole `impl StateMachine for TableMachine` block (db.rs:722-846):

```rust
impl StateMachine for TableMachine {
    /// One record is a batch of one — and a batch of one does exactly the disk
    /// work this did before batching (one read txn, one write txn, one commit),
    /// which is the batch-of-1 identity the pre-batching trace fixture pins.
    fn apply(&mut self, seq: u64, payload: &[u8]) -> io::Result<()> {
        self.apply_batch(&[(seq, payload)]).map_err(|f| f.error)
    }

    fn apply_batch(&mut self, records: &[(u64, &[u8])]) -> Result<(), SubmitFailure> {
        // Skipped records are a contiguous prefix: skip_through is the store's
        // own applied_seq (db.rs, Db::recover), so seq <= skip_through is
        // exactly what the store already holds, and the live tail starts at
        // skip_through + 1 gapless.
        let skip_through = self.skip_through;
        let start = records
            .iter()
            .position(|(seq, _)| !skip_through.is_some_and(|s| *seq <= s))
            .unwrap_or(records.len());
        let live = &records[start..];
        if live.is_empty() {
            return Ok(());
        }

        let schema = &self.schema;
        let versions = &mut self.versions;
        let saw_below = &mut self.saw_below;
        // Collected locally and drained only after the batch commits: a
        // rolled-back batch must not enqueue jobs for records the store never
        // took. (The closure also cannot hold `&mut self.enqueued` while
        // `schema` is captured — the same borrow constraint as before.)
        let mut new_ids = Vec::new();

        let result = self.store.apply_batch(live, |w, seq, payload| {
            let effects = decode_record(payload)?;
            // ... the existing closure body, moved verbatim from the old
            // `apply`, with exactly three mechanical changes:
            //   - `let mut ids = Vec::new();`  (old db.rs:739)  DELETED
            //   - `ids.push(...)`              (old db.rs:831)  -> `new_ids.push(...)`
            //   - `Ok(ids)`                    (old db.rs:842)  -> `Ok(())`
            // `seq` now arrives as a closure parameter rather than from the
            // enclosing scope; its one use (materialize_event, old db.rs:838)
            // is unchanged.
            Ok(())
        });

        match result {
            Ok(()) => {
                self.enqueued.extend(new_ids);
                Ok(())
            }
            // A rolled-back batch applied nothing, so the count is 0, never a
            // mid-batch boundary. `versions` and `saw_below` keep mutations the
            // store discarded — tolerable only because the engine poisons here
            // and the process must restart and recover, which rebuilds both.
            Err(error) => Err(SubmitFailure { applied: 0, error }),
        }
    }
}
```

The closure body is a **verbatim move** of old `db.rs:740-841`, not a rewrite. Do not re-derive it, do not reformat it, and do not "improve" the pass-1/pass-2 comments — the diff should read as a move plus three one-line edits, so a reviewer can confirm it by eye.

Import `SubmitFailure` in `db.rs` if it is not already in scope.

- [ ] **Step 4: Run the tests**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
cargo test -p charpente-core 2>&1 | tee /tmp/t4-core.log; echo "exit=${pipestatus[1]}"
cargo test --workspace 2>&1 | tee /tmp/t4-ws.log; echo "exit=${pipestatus[1]}"
```

Expected: green.

- [ ] **Step 5: The batch-of-1 identity gate**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
cargo test -p charpente --test group_commit -- a_never_contended_universe_traces_identically_to_the_pre_batching_fixture --exact --nocapture
```

Expected: PASS. This is the moment the design's central determinism claim either holds or does not.

**If it fails, STOP and report the diff.** Do not re-capture the fixture, do not adjust the workload, do not mark the test `#[ignore]`. A red here means a one-record batch is *not* doing what a single apply did — a real finding about the design, and the seed-corpus exposure in §4 of the spec is then much larger than four pins. Capture the actual-vs-expected trace diff into the report and hand it back.

- [ ] **Step 6: Commit**

```bash
git add crates/charpente-core/src/db.rs crates/charpente-core/tests/engine.rs crates/charpente/tests/transactions.rs
git commit -F - <<'EOF'
feat: TableMachine applies a batch atomically, and apply becomes a batch of one

The hundred-line closure body moves once, into apply_batch; apply delegates to
it with a single record. That direction keeps one copy of the pass-1/pass-2
logic, and batch-of-1 identity is what makes it safe — a one-record batch does
one read txn, one write txn and one commit, exactly as before, and the
never-contended trace fixture still matches byte for byte.

Three things the move had to honor. enqueued extends only after the batch
commits, so a rolled-back batch cannot enqueue jobs for records the store never
took. Skipped records (seq <= skip_through, which is the store's own
applied_seq) are a contiguous prefix, so the batch splits there and the live
tail stays gapless. And versions/saw_below keep mutations a rollback discarded,
which is tolerable only because this path poisons the engine.

A failed batch reports applied: 0 — never a mid-batch boundary, because there
is no longer such a thing.
EOF
```

---

## Task 5: Sim coverage — the store boundary under the fault matrix

Hard rule 3: a core subsystem lands with seeded fault-injection tests. Task 2's tests are unit-level and fault-free; this task is the one that faults them.

**Do not add these to `crates/charpente-sim/tests/crash_recovery.rs`.** That file is the M0 step-1 property over a *toy* checksummed append-log — it constructs no `Store`, no `Db`, and no redb, so a batching test there would fault machinery this change never touches.

**Files:**
- Modify: `crates/charpente/tests/transactions.rs` (extend the existing `tx_workload` swarm)
- Modify: `crates/charpente/tests/replication.rs`

**Interfaces:**
- Consumes: everything from Tasks 1-4.

- [ ] **Step 1: Add the store-boundary invariant to the existing swarm**

`transactions.rs` already runs `run_universes_swarm!(256, tx_workload, require: [...])` (line ~679) with a final-verification block that opens the `Db`, compares `recovered` against `committed`, and runs `db_matches_model`. Extend that block rather than writing a new property — it already has crash injection, restart, checkpointing and the injected-fault routing that a new property would have to reproduce.

Add, immediately after the existing `recovered < committed` durability check:

```rust
        // Per-batch apply (spec §2): the store commits a batch whole or not at
        // all, so its applied_seq is always a batch boundary — never an
        // interior seq of a batch that did not commit. Asserted separately
        // from db_matches_model below because final state converges either
        // way once replay finishes: a store that had briefly been in a
        // mid-batch state still ends up correct, so a convergence-only oracle
        // cannot see the violation this change exists to make impossible.
        let store_at = db.store_applied_seq();
        if let Some(at) = store_at
            && at > recovered.saturating_sub(1)
        {
            return Err(format!(
                "store applied_seq {at} is ahead of recovered {recovered} — a batch \
                 committed to the store that the log does not cover"
            ));
        }
```

`db.store_applied_seq()` is the accessor this needs; if `Db` has no such method, add a small one that forwards to the machine's `Store::applied_seq`, or reach it through the existing seeder/machine accessor the file already uses. Do not add a public API surface wider than one read-only accessor.

- [ ] **Step 2: Add the coverage key**

A property that never reaches a multi-record batch proves nothing about batching. Add a coverage key where the workload commits a batch of more than one record, and add it to the `require:` list so a universe set that never exercises it fails loudly instead of passing quietly:

```rust
        require: [
            "fault:write_error",
            "fault:sync_error",
            "fault:crash",
            "crash:applied",
            "crash:torn",
            "crash:dropped",
            "restart",
            "batch:multi_record",
        ]
```

Emit `"batch:multi_record"` from the workload at the point a batch of ≥2 records actually commits. **Coverage `require:` keys are skipped under `CHARPENTE_SEED`**, so verify the key is genuinely reachable with a normal (unpinned) run, not a replay.

- [ ] **Step 3: Replication atomicity**

Add to `crates/charpente/tests/replication.rs`: a follower whose `submit_batch` fails mid-apply must have moved `applied_seq` by zero, must be poisoned, and must still produce the `REHELLO_MARKER` text for an out-of-order seq.

```rust
/// The follower path gets the same atomicity as the primary: a shipped batch
/// that fails mid-apply rolls back whole. The REHELLO_MARKER assertion is here
/// because submit_batch's seq-validation loop was deliberately NOT touched by
/// per-batch apply — this is what proves it stayed untouched, rather than
/// trusting that the diff looked small.
#[test]
fn a_shipped_batch_that_fails_mid_apply_rolls_back_whole() {
    // Build a follower, ship it a batch whose last frame cannot decode.
    // Assert: applied_seq unmoved, engine poisoned, and a subsequent
    // out-of-order ship still names REHELLO_MARKER.
    //
    // Follow the existing follower-construction helper in this file; the
    // three assertions above are the content, the setup is whatever this
    // file already does to stand a follower up.
}
```

Fill the body from this file's existing follower helpers. If a decode failure is not reachable on the ship path, use whatever forced-apply-failure this file already has — the three assertions are what matter, not the mechanism that triggers them.

- [ ] **Step 4: Run them**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
cargo test -p charpente --test transactions 2>&1 | tee /tmp/t5-tx.log; echo "exit=${pipestatus[1]}"
cargo test -p charpente --test replication 2>&1 | tee /tmp/t5-repl.log; echo "exit=${pipestatus[1]}"
```

Expected: green. Any failure prints an exact `CHARPENTE_SEED=0x…` repro line — paste it **verbatim** into the report, and pin the seed in the regression suite (hard rule 3: a bug fixed without a failing seed first was never reproduced).

- [ ] **Step 5: Fresh-seed sweep**

The evidence bar for a core change is "sim suite green over N fresh seeds," not "unit tests pass":

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
set -o pipefail
cargo run -p charpente-cli -- test --sim --universes 200 --dir crates/sim-fixture --format human 2>&1 | tee /tmp/t5-sweep.log
echo "exit=$?"
```

Expected: exit 0. Record the universe count and the master seed in the commit message — the sweep is only evidence if someone else can replay it.

- [ ] **Step 6: Commit**

```bash
git add crates/charpente/tests/transactions.rs crates/charpente/tests/replication.rs
git commit -F - <<'EOF'
test: fault the batch boundary — store atomicity and follower rollback

Extends the existing tx_workload swarm rather than adding a parallel property:
it already carries crash injection, restart, checkpointing and injected-fault
routing that a new property would have to reproduce badly.

The new assertion is deliberately not a convergence check. Final state
converges either way once replay finishes, so a store that had briefly held a
mid-batch state still ends up correct and a convergence-only oracle cannot see
it. The invariant asserted instead is the one batching makes true: the store's
applied_seq is always a batch boundary, never an interior seq of a batch that
did not commit.

Adds a batch:multi_record coverage key to require: — a property that never
reaches a multi-record batch proves nothing about batching, and without the key
that failure mode is silent.

The follower gets the same rollback assertion plus a check that submit_batch's
untouched REHELLO_MARKER path really is untouched.

Sim suite green over N fresh universes (master seed recorded above).
EOF
```

---

## Task 6: The seed gate — revert-prove the four load-bearing pins

Spec §4 makes this a **merge gate, not a cost line**. Nothing lands with a silently vacuous pin.

This is not a formality. `ci.yml:37-47` records `0x348270b38f166529` ceasing to reach its arm because the group-commit rewrite moved the op schedule; `0xe653f43924f17fe8` exists only as its replacement. This change moves the op schedule again.

**Files:**
- Modify: `.github/workflows/ci.yml` (only if a pin needs replacing)
- Report: `.superpowers/sdd/<this-plan>/seed-gate.md`

- [ ] **Step 1: Set up an isolated worktree**

Never share `CARGO_TARGET_DIR` across worktrees — a mutation proof's artifact leaking into the main tree shows up as a pass with `git diff` clean.

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
git worktree add /tmp/charpente-seedgate HEAD
cd /tmp/charpente-seedgate
export CARGO_TARGET_DIR=/tmp/charpente-seedgate-target
cargo build --workspace 2>&1 | tail -3
```

- [ ] **Step 2: Revert-prove each of the four pins**

The four load-bearing pins, all verified live at `93a5e41`:

| seed | command | guards |
|---|---|---|
| `0x8bee1752811ae873` | `CHARPENTE_SEED=0x8bee1752811ae873 cargo test --release -p charpente-sim --test crash_recovery` | crash-recovery replay |
| `0x348270b38f166529` | `CHARPENTE_SEED=0x348270b38f166529 cargo test -p charpente --test kernel -- kernel_jobs_are_exactly_once_under_the_fault_matrix --exact` | `Kernel::build` drain-id fix |
| `0xff159ed76d2806e4` | `CHARPENTE_SEED=0xff159ed76d2806e4 cargo test --release -p charpente --test replication -- late_follower_seeds_below_the_gcd_base_and_converges --exact` | cross-build-mode skew |
| `0x1f733f8d9eebf822` | (its call sites are in `db.rs` / `transactions.rs` — read them for the arm it drives) | see call site |

Note before starting: `0x8bee1752811ae873` pins `crash_recovery.rs`, which is the M0 *toy* append-log — it builds no `Store` and no redb, so this change should not move its op schedule at all. Run it anyway, but a RED there is a surprise worth reporting loudly rather than quietly fixing: it would mean the change reached further than the design says it does.

For each: identify the fix it guards, **delete that fix on the new code**, confirm the seed still goes RED for its original reason, restore, confirm GREEN. Green-after-deletion means the pin has gone vacuous.

Record each outcome in `seed-gate.md`: seed, the fix deleted, RED or GREEN with deletion, and the failure message if RED.

- [ ] **Step 3: Handle any vacuum**

If a pin stays green with its fix deleted, hunt a replacement seed that reddens for that reason on the new code, following the `0xe653f43924f17fe8` precedent: keep both lines, comment why, and add the replacement to `ci.yml`.

**If a replacement cannot be found: STOP.** Spec §4 makes an unrecoverable pin a merge blocker, retired only by Noah's explicit sign-off — never absorbed silently. Report the vacuum, what was tried, and hand it back.

- [ ] **Step 4: Clean up the worktree**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
git worktree remove /tmp/charpente-seedgate
rm -rf /tmp/charpente-seedgate-target
```

- [ ] **Step 5: Commit any ci.yml change**

Only if a pin was replaced. Follow the existing comment style in `ci.yml` — every pin there carries a paragraph explaining what it guards and why it is `--exact`.

---

## Task 7: Re-measure, publish, supersede

**Files:**
- Modify: `docs/BENCH.md`
- Modify: `.superpowers/sdd/2026-08-19-phase-attribution/trigger-a-rereading.md` (mark superseded)

- [ ] **Step 1: Re-measure with the same instrument and protocol**

Use the phase-attribution instrument and the `task-6-protocol.md` discipline from the previous sub-project — same harness, same quiet-box preconditions, same interleaved arms — so before and after are comparable rather than two unrelated runs. n ≥ 3, batch ≈ 62.

The overhead gate runs **first**, before any arm is published; an ungated arm is not publishable. If the trace buffer regrew or the gate cannot run, phases are not published (`phases_overhead_gated`).

- [ ] **Step 2: Check against the pre-committed criterion**

Fixed before the run, from spec §7: `gap_within_store` below **5%** of the kernel window, and `device_ratio` above **0.7**, at n ≥ 3.

The spike measured 0.3% and 0.76–0.85. **Landing materially short is the finding, not a number to explain away** — report it rather than re-running until it passes, and do not re-derive the criterion post hoc.

- [ ] **Step 3: Update `docs/BENCH.md`**

This change **moves published numbers**, which was forbidden during phase attribution and is the point here. Provenance rules still apply: counted vs derived vs residual stays labelled, and a residual is never quoted as a measurement.

- [ ] **Step 4: Mark the trigger-(a) re-reading superseded**

Add a header to `.superpowers/sdd/2026-08-19-phase-attribution/trigger-a-rereading.md` pointing at this plan and the new BENCH numbers: it concludes on a code path this change deletes. **Superseded with a pointer, not amended** — the original reading stays readable as what was true then.

Do not edit `DECISIONS.md`. Report to Noah that a fresh trigger-(a) reading is available and that on the new numbers redb's share is negligible under either reading of "non-idle", which dissolves the ambiguity that made it his call.

- [ ] **Step 5: Commit**

```bash
git add docs/BENCH.md .superpowers/
git commit -F - <<'EOF'
bench: re-measure the store path after per-batch apply

Same instrument, same protocol, same interleaved arms as the phase-attribution
run, so these numbers are comparable to the ones they replace rather than a
fresh unrelated measurement. Overhead gate ran first; no arm published ungated.

Checked against the criterion fixed before the run (spec §7): gap_within_store
under 5% of the kernel window, device_ratio above 0.7, n >= 3.

The trigger-(a) re-reading is marked superseded with a pointer rather than
amended — it concludes on a code path this change deletes, and it should stay
readable as what was true then. DECISIONS.md untouched: Noah's to edit.
EOF
```

---

## Notes for the executor

- **Two stop-and-report gates, both about the same fixture.** Task 1 Step 5 and Task 4 Step 5 both run `a_never_contended_universe_traces_identically_to_the_pre_batching_fixture`. If either goes red, the cheapest way to make it green is to re-capture the fixture — and that is precisely the move that destroys the oracle. The test's own doc comment was written to forbid it. Report instead.
- **`submit_all_reports_how_many_applied_before_a_mid_batch_failure` must stay byte-unchanged.** It is the default-impl equivalence gate. A red there is an engine bug, not a stale test.
- **The Task 4 closure body is a move, not a rewrite.** Three one-line edits, listed explicitly. If the diff shows a reformatted or re-derived body, the review should reject it — a verbatim move is checkable by eye and a rewrite is not.
- **`versions`/`saw_below` divergence on rollback is known and accepted**, documented in Task 4 Step 1 constraint 3. It is not a finding to re-litigate mid-task; if it looks wrong, ledger a ruling and continue.
- **Do not touch `Cargo.toml:38-39`.** `[profile.release.package.redb] debug-assertions = true` is deliberate: redb's assertion-gated page walk draws sim IO rolls, so the seed promise depends on it. Turning it off would look like a free speedup on this task's own benchmark and would trade determinism for it — the wrong side of MANIFESTO §8, and spec §8 names it a non-goal.
- **Do not start spec §6** (making the seed corpus robust to IO-shape change). It is raised in the spec as its own decision precisely so it does not get absorbed into this sub-project.
