# M2 — per-batch apply: design

## Context

M2 sub-project 2 (group commit, shipped 2026-08-15) amortized the fsync across a batch:
`Engine::submit_all` is N × `wal.append` → **one** `wal.sync()` → N × `machine.apply`
(`crates/charpente-core/src/engine.rs:216-238`). The phase-attribution sub-project
(2026-08-20, `docs/BENCH.md`) then measured where the remaining time went and found the
device still ~60% idle at batch ≈62.

The apply block is why. `Store::apply` (`crates/charpente-core/src/table.rs:348`) opens its
own write transaction **per record** — `self.db.begin_write()` at `:359` — and calls
`applied_seq()` at `:330`, which opens its own **read** transaction, also per record. A
62-record batch therefore costs 62 appends, **one** fsync, and **62 read + 62 write
transactions**. Group commit amortized the fsync 62×. It amortized the store 0×.

**This is not a claim about redb.** A throwaway spike (2026-08-20, patch preserved at
`.superpowers/sdd/2026-08-19-phase-attribution/spike-apply-batching.patch`, results at
`spike-apply-batching-results.txt`) held one write transaction across the batch and
measured, interleaved against an unmodified baseline, n=3 each, zero `invalid` entries:

| metric | baseline | spiked |
|---|---|---|
| acked writes/s | 3133 / 3617 / 2307 | 5878 / 6169 / 6225 |
| window | 6.38 / 5.53 / 8.67 s | 3.40 / 3.24 / 3.21 s |
| `device_ratio` | 0.412 / 0.438 / 0.304 | 0.764 / 0.816 / 0.852 |
| Disk ops | 521,558 / 522,395 / 523,155 | 49,338 / 49,300 / 49,314 |
| `store_write_io` | 740 / 669 / 986 ms | 54 / 43 / 37 ms |
| `gap_within_store` | 2116 / 2174 / 2370 ms | 16.4 / 8.4 / 6.9 ms |
| `wal_sync_io` | 2930 / 2210 / 4679 ms | 2495 / 2526 / 2531 ms |

Ranges are non-overlapping on every store-side metric. Per record: **23.7 of every 26.2
Disk operations were transaction machinery, not data.** `gap_within_store` — the field
carrying ~40% of the phase-attribution case against redb — falls ~99.5%. `charpente-core`'s
76 tests pass with the spike applied. The spike is throwaway and its error path is
knowingly wrong (see §2); its numbers are a hypothesis this design must confirm, not
evidence to quote.

## Why this is a correctness change that happens to be fast

MANIFESTO §5: *"there is exactly one place where things become true: the log... all of it
commits atomically or none of it does."*

Today a mid-batch apply failure leaves records `0..i` applied — each committed with
`Durability::None` — and `i..N` not. The store, which is a *materialization* of the log
rather than a second source of truth (`table.rs` module doc), is left in a state
corresponding to **no single commit point**. Per-batch apply makes that impossible: the
batch lands whole or not at all.

The throughput result is the corollary, not the thesis. This ordering matters for scoping:
where atomicity and speed disagree, atomicity wins.

## Scope

- **In:** `Engine::submit_all` (primary commit path) and `Engine::submit_batch`
  (replication apply path, `engine.rs:246`). Both are batch-shaped already and neither
  amortizes the store.
- **In:** the four load-bearing pinned seeds (§4) and the batch-of-1 identity fixture.
- **Out:** any change to append/sync structure. This design changes only what happens
  *after* `wal.sync()`.
- **Out:** exploiting the new atomicity to relax the poison bit (§2). Recorded as a door
  this opens; YAGNI until something needs it.
- **Out:** the seed-corpus fragility question (§6). It is real, it is not this
  sub-project's to solve, and it must not be silently absorbed.

## §1 The interface

```rust
pub trait StateMachine {
    fn apply(&mut self, seq: u64, payload: &[u8]) -> io::Result<()>;

    /// Applies a contiguous run of records. On failure reports how many of
    /// them are durably reflected in machine state; an implementation may
    /// apply all or none. Default: per-record, today's exact semantics.
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

The error type is the **existing** `SubmitFailure { applied, error }`
(`engine.rs:53-56`), not a new near-identical struct — same shape, same module,
and `submit_all` already builds exactly this value at every one of its error
sites, so reusing it removes a conversion rather than adding one. An earlier
draft of this design returned `io::Result<usize>`; that is wrong and is recorded
here because it is the trap this signature exists to avoid. `Ok(i)` with
`i < records.len()` carries the count but **discards the error**, and `submit_all`
needs both to construct its failure — so the engine would read a partial apply as
success and return `Ok(seqs)`, claiming records landed that had not. That is the
same class of bug as the spike's (§2), reintroduced one layer up.

Chosen over a `begin_batch`/`end_batch` hook pair (which the spike used) because the hook
form creates an implicit pairing protocol — every error path must close the batch or leak
an open transaction — and makes "am I in a batch" a second piece of mutable state.
`apply_batch` puts the batch in the type system: there is no open-batch state between
calls, and an unclosed transaction is **unrepresentable** rather than merely asserted
against (hard rule 4 prefers the former). A store-internal auto-batching variant that
commits on a size or time threshold was rejected outright: a time trigger is ambient
nondeterminism inside the core (hard rule 1), and a size trigger still needs a flush signal
at batch end, so it buys nothing.

Of the seven `StateMachine` implementors, six are test fixtures — `Counter`, `AlwaysFails`,
`FailsAfter` (`charpente-core/tests/engine.rs`), `Ledger`, `Balky`
(`charpente-sim/tests/wal.rs`), `OpLog` (`charpente/tests/typed_tables.rs`). All six take
the default and are untouched. Only `TableMachine` (`charpente-core/src/db.rs:722`)
overrides.

`Store` grows one method; the transaction is scoped to the call, so **no `RefCell`, no held
state** — the spike's `RefCell<Option<WriteTransaction>>` does not survive into the design:

```rust
impl Store {
    /// One write txn for a contiguous run. All or nothing.
    pub fn apply_batch(
        &self,
        records: &[(u64, &[u8])],
        per_record: impl FnMut(&mut Writer<'_>, u64, &[u8]) -> io::Result<()>,
    ) -> io::Result<()>
}
```

## §2 Failure semantics

`Engine` advances `last_applied` **by the count the call reports**, never inside the loop:
`records.len()` on `Ok(())`, and exactly `applied` on `Err(SubmitFailure { applied, .. })`.
The error path matters as much as the success path — under the default impl those
`applied` records really did land, so `last_applied` must reflect them or recovery
replays already-applied records.

A failure anywhere in a `TableMachine` batch rolls the transaction back: **zero records
applied**, so `SubmitFailure { applied }` is `0`, not `i`. The spike got this wrong — it
advanced `last_applied` per record and reported `applied: seqs.len()` on a commit failure,
claiming work landed that had not. That bug is recorded here because it is the natural
mistake this interface exists to prevent.

The store then sits at `applied_seq` with the WAL ahead of it, and recovery replays from
`applied_seq + 1` — the documented path, batch-aligned. Machine state still *matches* the
log, merely trails it, which means the poison bit is arguably unreachable for this case.
**We keep poisoning anyway** (§Scope): removing it is a separate argument requiring its own
evidence.

Under the **default** impl the old semantics survive exactly: a failure at record `i`
returns `Err(SubmitFailure { applied: i, .. })`, and those `i` records really did land —
byte-for-byte what `submit_all`'s loop reports today (`engine.rs:235`). So the same call
site yields `applied: i` for the six default implementors and `applied: 0` for
`TableMachine`. Both are correct; they differ because the implementations differ, which is
why the contract lives on the trait rather than in either impl: *`apply_batch` reports how
many records are durably reflected in machine state; an implementation may apply all or
none.* A caller that assumes either specific behaviour is relying on something the trait
does not promise.

`submit_batch`'s seq-validation loop and its `REHELLO_MARKER` error text are untouched
(hard rule 5 — `follower.rs` matches on that marker).

## §3 Invariants

- **Gapless is preserved, not traded.** Today it costs a `begin_read` per record
  (`table.rs:330`). Now: `applied_seq()` is checked **once** against the batch's first seq,
  then a running expectation carries the same `assert_eq!` across the loop. Same negative
  space asserted, one read transaction instead of 62 — a legitimate part of the measured
  win, not a check quietly dropped.
- **Per-record negative-space assertions stay put.** The `next_job_id` / `next_event_id`
  advance-by-exactly-one asserts live inside the body, cost nothing, and are precisely what
  a batching bug would trip.
- **`skip_through` migration handling stays per record.**
- **NEW: no read outside the batch's own transaction may occur between its applies.** Reads
  are MVCC snapshots (`table.rs` module doc), so a `Store::applied_seq`-style fresh read txn
  opened mid-batch observes *pre-batch* state and would silently return stale data — redb
  permits it, so nothing errors. Enforcement is **by construction, not by assertion**: the
  per-record closure receives only `&mut Writer<'_>`, and `Writer` holds `txn: &'txn
  redb::WriteTransaction` and opens every table on it, so its accessors already see the
  batch's own writes. (An earlier draft cited `table.rs:838` as `Writer::applied_seq`;
  that line is inside `impl Snapshot` and the method is `Snapshot::applied_seq` — the
  argument holds, the citation did not. `Writer::applied_seq` did not exist and was added
  by the implementation of this design.) Reads inside the
  closure are therefore correct by definition. The only residual surface is code *outside*
  the closure — `submit_all`'s apply loop — which contains no reads today. That held by
  accident before and is a stated precondition now; a debug-only re-entrancy flag is
  deliberately **not** added, since it would reintroduce the held mutable state §1 removed.
- `checkpoint` is untouched; it opens its own transaction.

## §4 Determinism and the pinned seeds

**This does not weaken determinism.** Same code plus same seed still replays byte for byte;
the sim's determinism properties are unaffected. What changes is that *recorded* seeds may
no longer walk the paths they were recorded for. Determinism is intact; the regression
corpus's **memory** is what is at risk. Anyone reading "batching breaks determinism" here
has read it wrong.

**Batch-of-1 identity holds by construction.** A one-record batch performs one read txn,
one `begin_write`, one commit — bit-for-bit what today's single apply performs. Every
universe that never has two records ready at once is therefore byte-identical, which is the
same escape hatch group commit relied on, and it is *pinned*, not asserted:
`a_never_contended_universe_traces_identically_to_the_pre_batching_fixture`
(`crates/charpente/tests/group_commit.rs:466`) against
`crates/charpente/tests/fixtures/pre-batching-trace.txt`. This design extends that fixture
rather than replacing it.

**Census by call site, never by literal.** The group-commit spec's §7 amendment established
this and it is binding: seeds here run from `0x1` to a full 64 bits, so any hex-length grep
is unsound by construction. (A literal grep during this design produced "66 seeds" and was
wrong — recorded so the method is not re-derived.) Enumerate by seed-consuming constructor
over `crates/*/tests/*.rs` **and** `crates/*/src/**/*.rs`, and list `CHARPENTE_SEED` replay
pins separately from `.github/workflows/ci.yml`, which no source census can find.

**The load-bearing population is four pins**, per the T9 census, all verified live at
`93a5e41`:

| seed | sites |
|---|---|
| `0x8bee1752811ae873` | `charpente-sim/src/harness.rs`, `ci.yml` |
| `0x348270b38f166529` | `group_commit.rs`, `kernel.rs`, `ci.yml` |
| `0xff159ed76d2806e4` | `ci.yml` only |
| `0x1f733f8d9eebf822` | `charpente-core/src/db.rs`, `transactions.rs` |

**This is a gate, not a cost line.** For each of the four: revert-prove on the *new* code —
delete the fix it guards, confirm it still reddens for its original reason, in an isolated
worktree with its own `CARGO_TARGET_DIR`. A pin that goes vacuous gets a replacement hunted
and both recorded. **A pin that cannot be re-derived blocks the merge** and is retired only
by explicit sign-off from Noah — never absorbed silently. MANIFESTO §9 makes the seed the
product (*"here is the seed to prove it"*); a silently green pin spends that to buy
throughput, which is the trade this project exists to refuse.

Precedent that this is necessary rather than paranoid: `ci.yml`'s own comment records
`0x348270b38f166529` ceasing to reach its arm because *"T7's group-commit rewrite moved the
op schedule"*, and `0xe653f43924f17fe8` exists solely as its replacement.

## §5 Testing

Hard rule 3 — lands with seeded fault-injection tests in the same PR.

- **Atomicity.** A batch whose *N*th record fails leaves `applied_seq` at its pre-batch
  value and none of the batch's effects visible.
- **Crash mid-batch (sim).** Crash injected between the last apply and the commit; recovery
  replays from `applied_seq + 1` into the same state. This is the test that earns §2's
  claim; without it, atomicity is an assertion about code that was never faulted.
- **Gapless, both shapes.** First-record mismatch and mid-batch gap, proving the running
  counter catches what the per-record read txn used to.
- **Default-impl equivalence.** The six fixtures keep today's semantics exactly, including
  the partial count on failure. `FailsAfter`'s expectations change under `TableMachine` and
  must *not* change under the default; that contrast is the test.
- **Replication.** `submit_batch` atomicity, and `REHELLO_MARKER` still present.
- **Batch-of-1 identity**, extending the existing fixture (§4).

Every guard gets a revert proof in an isolated worktree. This is not ceremony: during the
phase-attribution sub-project a guard shipped that could not fail, and was caught only
because a reviewer tried to break it.

## §6 The question this design deliberately does not answer

The seed corpus is coupled to the **physical Disk-operation sequence**, because the sim
draws rolls per operation. So any change to IO shape can silently vacate a pin: it stays
green and quietly stops being evidence. This has now happened at least twice — group commit
(recorded in `ci.yml`) and, prospectively, here.

Under MANIFESTO §2 (*"the highest-leverage artifact is the oracle substrate"*), making the
corpus robust to IO-shape change may be worth more than any single optimization it would
protect — including this one. That is a separate sub-project with its own spec, and it is
raised here rather than folded in because folding it in is how it would get skipped. If it
is done first, this change and every successor becomes substantially cheaper.

## §7 Exit criteria

1. `apply_batch` on both paths; `last_applied` advanced only by the returned count.
2. All four load-bearing pins revert-proved on the new code, each outcome recorded; no
   unresolved vacuum.
3. Batch-of-1 identity pinned against the fixture.
4. Sim suite green over N fresh seeds, N stated in the commit (not "unit tests pass").
5. **Pre-committed success criterion, fixed before the run:** on a quiet box at n≥3, batch
   ≈62, `gap_within_store` below **5%** of the kernel window and `device_ratio` above
   **0.7**. The spike measured 0.3% and 0.76–0.85. Landing materially short means something
   differs from the spike, and that gap is the finding — not a number to explain away.
6. `docs/BENCH.md` re-measured with the same instrument and protocol, so before/after are
   comparable rather than two unrelated runs. **This moves published numbers**, which is the
   point here and was forbidden in phase attribution; provenance rules still apply.
7. The trigger-(a) re-reading
   (`.superpowers/sdd/2026-08-19-phase-attribution/trigger-a-rereading.md`) marked
   **superseded** with a pointer — it concludes on a code path this change deletes — and a
   fresh reading taken. On the spike's evidence redb's share becomes negligible under either
   reading of §7's *"non-idle"*, dissolving the ambiguity that made it Noah's call.
8. `DECISIONS.md` edited by Noah, never by an agent.

## §8 Non-goals

Building an LSM. Cutting the engine trait boundary. Changing append/sync structure.
Removing the poison bit. Touching `Cargo.toml:38-39`'s `[profile.release.package.redb]
debug-assertions = true` — that override is deliberate (redb's assertion-gated page walk
draws sim IO rolls, so the seed promise depends on it) and turning it off trades
determinism for speed, which is the wrong side of MANIFESTO §8.
