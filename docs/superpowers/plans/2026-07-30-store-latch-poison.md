# M2 sub-project 0a — checkpoint-latch gap: implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A charpente node whose redb store handle has latched must stop, instead
of serving on with `Db::poisoned()` reporting false while it can never checkpoint
again.

**Architecture:** redb latches (`CheckedBackend.io_failed`) exactly when its inner
`StorageBackend` returns `Err`. That inner backend *is* charpente's `DiskBackend`,
so charpente observes the identical condition one layer down: a sticky
`Arc<AtomicBool>` set in `DiskBackend`'s five `StorageBackend` methods, read as
`Store::failed()`, OR-ed into `Db::poisoned()`. The kernel's existing poison stop
(`kernel/mod.rs:304`) then fires with no new control flow — only a distinct
message, because a store latch leaves durable state *intact* and apply-poison
does not.

**Tech Stack:** Rust, redb 4.1.0, `charpente-sim` (seeded sim + `SimDisk`) as the
core crate's dev-dependency.

**Spec:** `docs/superpowers/specs/2026-07-30-store-latch-poison-design.md`
(committed `e57e62c`, §1/§2/§6 amended in `1c88959`). The spec governs; where this
plan and the spec disagree, stop and reconcile before coding.

## Global Constraints

- **Hard rule 1 — no ambient nondeterminism in the core.** `AtomicBool` is
  permitted (not a deny-list type; the kernel is single-threaded). Add no clock,
  RNG, thread, or direct filesystem call.
- **Hard rule 3 — RED first.** Every test that asserts a *behavior change* must be
  *observed failing* against unfixed code before its implementation step. A test
  never seen red is not evidence. Record the observed failure text in the ledger.
  Exempt, and labelled as such at their site: tests that assert an **absence**
  (the flag must NOT be set), which cannot be meaningfully red-first — they are
  regression guards whose red value is realized only if a later change
  over-broadens the flag. T1 Steps 5-6 are the only such tests in this plan.
- **Hard rule 4 — assertions stay on in release.** Use `assert!`, never
  `debug_assert!`, for anything load-bearing.
- **Hard rule 5 — error messages are product surface.** Every named string is
  pinned byte-exact by a test in the same commit that introduces it.
- **Hard rule 6 — public-API changes update the docs in the same commit.**
  `Db::poisoned()`'s meaning is public API: DESIGN §4 lands in Task 2, not later.
- **No new dependencies.**
- **Commit directly to `main`. No `Co-Authored-By` trailers.**
- The three CI commands must pass at every commit: `cargo fmt --all --check`,
  `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`.

## File Structure

| File | Responsibility |
|---|---|
| `crates/charpente-core/src/table.rs` | The latch flag: created in `Store::open`, held by `DiskBackend` (sets it) and `Store` (reads it). Adds a `#[cfg(test)]` module for the five-method unit tests — `DiskBackend` is private, so these cannot be integration tests. |
| `crates/charpente-core/src/db.rs` | `Db::store_failed()`; `Db::poisoned()` ORs it in. |
| `crates/charpente/src/kernel/mod.rs` | Distinct stop message for the store-latch cause. |
| `crates/charpente/tests/kernel.rs` | Tripwire updated + correspondence assertion. |
| `crates/charpente-core/tests/store_latch.rs` | Store-level negative test (errors that never reach the disk boundary must not latch). |
| `DESIGN.md`, `docs/GUIDE.md`, `CLAUDE.md` | Docs + backlog retirement. |

## A trap this plan exists to keep you out of

**Do not try to write an integration test that faults a store *read* and expects
the latch to fire.** It will not fire, and you will lose hours. A small store is
entirely resident in redb's 1 GiB read cache, so after `Store::open` its reads
never reach the `Disk` boundary at all — this is measured, and the existing
tripwire (`crates/charpente/tests/kernel.rs:826-866`) pins exactly that fact with
a `reads_at_open` equality assertion.

That is why the read arm is covered by a **unit test on `DiskBackend::read`
directly** (Task 1). Our contract is "when `File::read_at` errors, set the flag."
Whether redb routes a given read to the boundary is redb's business, verified in
its source and recorded in the spec as **DERIVED**. Do not attempt to promote it
by building a >1 GiB store — that is an explicit spec non-goal.

---

### Task 1: Observe the disk-boundary latch in `DiskBackend`

Pure mechanism. Nothing reads the flag yet, so this task changes **no** runtime
behavior and `cargo test --workspace` must stay green on both sides of it.

**Files:**
- Modify: `crates/charpente-core/src/table.rs` (`DiskBackend` at `:938-970`, `Store` at `:260-262`, `Store::open` at `:288-319`)
- Test: `crates/charpente-core/src/table.rs` (new `#[cfg(test)]` module)
- Test: `crates/charpente-core/tests/store_latch.rs` (new)

**Interfaces:**
- Consumes: nothing.
- Produces: `Store::failed(&self) -> bool` — sticky, true once any `DiskBackend`
  method's inner `File` call has returned `Err`. Task 2 is its only caller.

- [ ] **Step 1: Write the failing unit tests for all five methods**

Append to `crates/charpente-core/src/table.rs`:

```rust
#[cfg(test)]
mod latch_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A `File` whose every operation fails, so each `DiskBackend` method can
    /// be driven down its error arm independently.
    struct AlwaysErr;

    impl crate::io::File for AlwaysErr {
        fn len(&self) -> io::Result<u64> {
            Err(io::Error::other("injected len failure"))
        }
        fn read_at(&self, _offset: u64, _out: &mut [u8]) -> io::Result<()> {
            Err(io::Error::other("injected read failure"))
        }
        fn write_at(&self, _offset: u64, _data: &[u8]) -> io::Result<()> {
            Err(io::Error::other("injected write failure"))
        }
        fn set_len(&self, _len: u64) -> io::Result<()> {
            Err(io::Error::other("injected set_len failure"))
        }
        fn sync(&self) -> io::Result<()> {
            Err(io::Error::other("injected sync failure"))
        }
    }

    fn backend() -> (DiskBackend, Arc<AtomicBool>) {
        let latch = Arc::new(AtomicBool::new(false));
        let backend = DiskBackend {
            file: Box::new(AlwaysErr),
            latch: latch.clone(),
        };
        (backend, latch)
    }

    // Each of redb's five latch sites (cached_file.rs:158/167/176/185/194) has
    // exactly one DiskBackend counterpart. If redb grows a sixth, the tripwire
    // in crates/charpente/tests/kernel.rs is what catches it.

    #[test]
    fn a_failed_len_latches() {
        let (b, latch) = backend();
        assert!(!latch.load(Ordering::SeqCst), "clean before");
        assert!(b.len().is_err());
        assert!(latch.load(Ordering::SeqCst), "len must latch on Err");
    }

    #[test]
    fn a_failed_read_latches() {
        let (b, latch) = backend();
        let mut out = [0u8; 8];
        assert!(b.read(0, &mut out).is_err());
        assert!(latch.load(Ordering::SeqCst), "read must latch on Err");
    }

    #[test]
    fn a_failed_set_len_latches() {
        let (b, latch) = backend();
        assert!(b.set_len(64).is_err());
        assert!(latch.load(Ordering::SeqCst), "set_len must latch on Err");
    }

    #[test]
    fn a_failed_sync_data_latches() {
        let (b, latch) = backend();
        assert!(b.sync_data().is_err());
        assert!(latch.load(Ordering::SeqCst), "sync_data must latch on Err");
    }

    #[test]
    fn a_failed_write_latches() {
        let (b, latch) = backend();
        assert!(b.write(0, &[1, 2, 3]).is_err());
        assert!(latch.load(Ordering::SeqCst), "write must latch on Err");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p charpente-core --lib latch_tests`
Expected: **compile failure** — `DiskBackend` has no `latch` field. That is the
correct red for this step; record the exact error text.

- [ ] **Step 3: Add the flag and the five observations**

In `crates/charpente-core/src/table.rs`, add to the imports:

```rust
use std::sync::atomic::{AtomicBool, Ordering};
```

(`std::sync::Arc` is already in scope; confirm before adding a duplicate.)

Change `DiskBackend` (`:938-940`):

```rust
struct DiskBackend {
    file: Box<dyn File>,
    /// Set whenever an operation below returns `Err`. redb's own
    /// `CheckedBackend` latches on exactly this condition and no other
    /// (redb-4.1.0 cached_file.rs:154-197), so this mirrors redb's
    /// `io_failed` rather than inferring it from a downstream error. redb's
    /// `close()` also sets `io_failed` (:148) — deliberately NOT mirrored
    /// here: a graceful close is not a failure.
    latch: Arc<AtomicBool>,
}
```

Replace the `StorageBackend` impl (`:948-970`) — every method keeps its exact
delegation and gains one observation:

```rust
impl DiskBackend {
    /// Mirrors redb's latch: set on `Err`, never cleared.
    fn note<T>(&self, r: Result<T, io::Error>) -> Result<T, io::Error> {
        if r.is_err() {
            self.latch.store(true, Ordering::SeqCst);
        }
        r
    }
}

impl StorageBackend for DiskBackend {
    fn len(&self) -> Result<u64, io::Error> {
        self.note(self.file.len())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        self.note(self.file.read_at(offset, out))
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.note(self.file.set_len(len))
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        self.note(self.file.sync())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        self.note(self.file.write_at(offset, data))
    }
}
```

Change `Store` (`:260-262`):

```rust
pub struct Store {
    db: Database,
    /// Shared with this store's `DiskBackend` — see its `latch` field.
    latch: Arc<AtomicBool>,
}
```

In `Store::open` (`:288-319`), create the flag before building the backend and
keep a clone. The two edited lines:

```rust
        let latch = Arc::new(AtomicBool::new(false));
        let db = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Builder::new().create_with_backend(DiskBackend {
                file,
                latch: latch.clone(),
            })
        }))
```

and the constructor at the end of `open`:

```rust
        Ok(Store { db, latch })
```

Add the reader next to `Store::checkpoint`:

```rust
    /// True once this store's handle is dead: some disk operation returned
    /// `Err`, which is precisely when redb latches `CheckedBackend.io_failed`
    /// and starts refusing every operation with `PreviousIo`. Sticky, and
    /// never cleared — redb's latch is not clearable either; the only exit is
    /// closing and re-opening the database (i.e. restarting the process).
    ///
    /// Durable state is NOT implicated: the WAL is intact and committed
    /// records are durable. Only this handle is unusable.
    pub fn failed(&self) -> bool {
        self.latch.load(Ordering::SeqCst)
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p charpente-core --lib latch_tests`
Expected: 5 passed.

- [ ] **Step 5: Write the failing negative test**

Errors that never reach the disk boundary must not latch. Create
`crates/charpente-core/tests/store_latch.rs`:

```rust
//! The latch flag's boundary (spec §6). Every *disk-boundary* error latches —
//! that is redb's own condition, mirrored deliberately. What must never latch
//! is a store error that no disk operation produced, or a clean shutdown.

use charpente_core::table::Store;
use charpente_sim::{FaultConfig, Sim};

fn store(sim: &Sim) -> Store {
    Store::open(&sim.disk(), "store.redb").unwrap()
}

#[test]
fn a_fresh_store_is_not_latched() {
    let sim = Sim::with_faults(0x0DED_0003, FaultConfig::none());
    let store = store(&sim);
    assert!(
        !store.failed(),
        "a store that has seen no IO error must not report a latch"
    );
}

#[test]
fn a_missing_table_does_not_latch() {
    let sim = Sim::with_faults(0x0DED_0004, FaultConfig::none());
    let store = store(&sim);
    // `applied_seq` on a virgin store takes the TableDoesNotExist arm
    // (table.rs:329) — an ordinary Ok(None), no disk error involved.
    assert_eq!(store.applied_seq().unwrap(), None);
    assert!(
        !store.failed(),
        "TableDoesNotExist is an ordinary result, not a disk failure — latching \
         here would mean the flag had drifted into 'any error whatsoever is fatal'"
    );
}

#[test]
fn a_graceful_close_does_not_latch() {
    let sim = Sim::with_faults(0x0DED_0005, FaultConfig::none());
    // redb's own CheckedBackend::close sets io_failed unconditionally
    // (cached_file.rs:148). charpente deliberately does not mirror that, so a
    // store dropped cleanly and re-opened must come back unlatched.
    {
        let store = store(&sim);
        store.checkpoint().unwrap();
        assert!(!store.failed(), "checkpoint on a healthy store must not latch");
    }
    let reopened = store(&sim);
    assert!(
        !reopened.failed(),
        "a store re-opened after a clean close must not report a latch"
    );
}
```

- [ ] **Step 6: Run it**

Run: `cargo test -p charpente-core --test store_latch`
Expected: PASS (these assert the *absence* of latching, so they pass once
`failed()` exists — they are regression guards, not red-first tests. Their red
value is realized if a later change over-broadens the flag).

- [ ] **Step 7: Full check and commit**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
git add crates/charpente-core/src/table.rs crates/charpente-core/tests/store_latch.rs
git commit -m "core: observe redb's latch condition at charpente's own disk boundary

DiskBackend now sets a sticky Arc<AtomicBool> whenever its inner File call
returns Err — precisely when redb's CheckedBackend sets io_failed
(cached_file.rs:154-197). Exposed as Store::failed(). Nothing reads it yet;
no behavior change. A graceful close is deliberately not mirrored."
```

---

### Task 2: Poison on the latch, with a message that tells the truth

The behavior change. This is the task a reviewer should scrutinize hardest.

**Files:**
- Modify: `crates/charpente-core/src/db.rs` (`poisoned()` at `:1638-1641`; `Db` reaches the store via `self.engine.machine().store`, as at `:1396`)
- Modify: `crates/charpente/src/kernel/mod.rs:304-309`
- Modify: `crates/charpente/tests/kernel.rs:770` (`kernel_keeps_serving_with_a_latched_store_known_gap`)
- Modify: `DESIGN.md` §4

**Interfaces:**
- Consumes: `Store::failed()` from Task 1.
- Produces: `Db::store_failed(&self) -> bool`; `Db::poisoned()` now true on a
  latched store.

- [ ] **Step 1: Turn the tripwire into the fix's test**

The existing test asserts today's *broken* behavior and is written to redden on
this change. Rewrite its tail (from `let replies = replies.lock().unwrap();`,
around `:869`) — keep everything above it, including the `fired`, `reads_at_open`
and `reads` assertions, which still hold and still carry the anti-vacuity load:

```rust
    let replies = replies.lock().unwrap();
    // THE FIX (was: the gap). The write's cadence checkpoint hits the store
    // latch, which now poisons the Db, so the kernel stops after replying to
    // the request that hit it. The two scripted reads are never served.
    assert_eq!(
        replies.len(),
        1,
        "the kernel must stop at the latched write, not serve on: {} replies",
        replies.len()
    );
    // The write still gets its honest 500 first — the record IS in the WAL.
    assert_eq!(replies[0].status, 500);
    let body = String::from_utf8_lossy(&replies[0].body);
    assert!(
        body.contains("simulated fsync failure"),
        "the write's 500 should name the injected fault: {body}"
    );
    // THE CORRESPONDENCE ASSERTION (spec §1). This store is genuinely latched
    // — redb answers PreviousIo from here on. charpente must agree. If a
    // future redb latches through a path DiskBackend does not observe, THIS
    // is the assertion that goes red, and the fix is to mirror the new path.
    assert!(
        db_poisoned,
        "the store handle is latched, so Db::poisoned() must be true — \
         charpente's flag has drifted from redb's io_failed condition"
    );
    // And the stop must say WHICH poison, because the remedies differ in what
    // they imply about durable state.
    let err = run.expect_err("a poisoned Db stops the kernel");
    let msg = err.to_string();
    assert_eq!(
        msg,
        "kernel stopped: the store handle is dead after a disk IO failure \
         (redb refuses every further operation until the database is re-opened) \
         — durable state is intact and the WAL is authoritative; restart the \
         process to recover",
        "the store-latch stop must be pinned byte-exact and must NOT reuse the \
         apply-poison wording, which claims durability became unknowable"
    );
```

`Kernel::run` takes `&mut self` and returns the `Result`, so `kernel` is still
alive afterwards. Insert this immediately after the existing
`let run = kernel.run(&mut source);` line:

```rust
    let db_poisoned = kernel.app().db().poisoned();
```

Both accessors already exist and need no new API: `Kernel::app(&self) -> &App`
(`kernel/mod.rs:389`) and `App::db(&self) -> &Db` (`http/app.rs:261`);
`Db::poisoned` takes `&self`. Do **not** reach for `db_for_tests()` here — it
takes `&mut self` and `Kernel::app()` hands out a shared borrow, so it will not
compile.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p charpente --test kernel kernel_keeps_serving_with_a_latched_store_known_gap -- --nocapture`
Expected: FAIL on `replies.len()` — 3 replies, expected 1. Per CLAUDE.md's
backlog note this assertion reddens *first*, before the poison headline; that
ordering is expected and is the fix working. Record the failure text.

- [ ] **Step 3: OR the latch into `poisoned()`**

In `crates/charpente-core/src/db.rs`, replace `poisoned()` (`:1638-1641`):

```rust
    /// True once the engine, its WAL, or the store handle is poisoned — every
    /// further transact fails. Callers (the kernel loop) stop and restart to
    /// recover.
    pub fn poisoned(&self) -> bool {
        self.engine.poisoned() || self.store_failed()
    }

    /// True once the store handle is dead (see `Store::failed`). Distinct from
    /// engine/WAL poison, and the distinction is load-bearing for the operator:
    /// engine poison means durability became UNKNOWABLE, while a store latch
    /// leaves the WAL intact and authoritative. Both remedies are a process
    /// restart; only one of them implies possible state loss.
    pub fn store_failed(&self) -> bool {
        self.engine.machine().store.failed()
    }
```

- [ ] **Step 4: Give the kernel the distinct message**

In `crates/charpente/src/kernel/mod.rs`, replace the stop block at `:304-309`:

```rust
                if self.app.db_mut().poisoned() {
                    // Check the store cause FIRST: a latched store can also
                    // poison the engine (its apply fails too), and the store
                    // latch is the root cause — reporting apply-poison there
                    // would tell the operator durability is unknowable when
                    // the WAL is in fact intact.
                    let store = self.app.db_mut().store_failed();
                    return Err(io::Error::other(if store {
                        "kernel stopped: the store handle is dead after a disk IO failure \
                         (redb refuses every further operation until the database is \
                         re-opened) — durable state is intact and the WAL is authoritative; \
                         restart the process to recover"
                    } else {
                        "kernel stopped: the engine is poisoned (a request-path IO failure \
                         made durability unknowable) — the request received its 500; \
                         restart the process to recover from the WAL"
                    }));
                }
```

- [ ] **Step 5: Run the tripwire to verify it passes**

Run: `cargo test -p charpente --test kernel kernel_keeps_serving_with_a_latched_store_known_gap -- --nocapture`
Expected: PASS.

- [ ] **Step 6: Rename the test**

`..._known_gap` is now false — the gap is closed. Rename to
`kernel_stops_on_a_latched_store` and update its module/doc comment: it is no
longer a known-gap characterization but the fix's regression test plus the
redb-correspondence pin. Keep seed `0x0DED_0002`.

Run: `cargo test -p charpente --test kernel kernel_stops_on_a_latched_store`
Expected: PASS.

- [ ] **Step 7: Update DESIGN §4 in this same commit (hard rule 6)**

Add to DESIGN's store/durability section: the store boundary now follows the same
fsyncgate rule as the log (`DESIGN.md:212-214`) — an un-retryable IO failure
poisons, and restart-and-recover is the only exit. State both shapes and their
standing honestly: the cache-resident shape is MEASURED, the
larger-than-read-cache shape is **DERIVED** from redb's source and deliberately
unmeasured. Name that `Db::poisoned()` now covers a cause under which durable
state is intact.

- [ ] **Step 8: Full check and commit**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
git add crates/charpente-core/src/db.rs crates/charpente/src/kernel/mod.rs crates/charpente/tests/kernel.rs DESIGN.md
git commit -m "core+kernel: a latched store poisons the node

Db::poisoned() now ORs in Store::failed(), so the kernel's existing poison
stop (the 2026-07-05 request-arm ruling) fires on a dead store handle instead
of serving on with poisoned() false and no way to ever checkpoint again.

The cause stays distinct: Db::store_failed() drives a separate stop message,
because a store latch leaves the WAL intact and authoritative while
apply-poison means durability became unknowable. Both strings pinned
byte-exact. The known-gap tripwire becomes the fix's regression test and
carries the redb-correspondence assertion (spec §1)."
```

---

### Task 3: Blast radius — the two replication sites

Spec §4. These are **re-read, not blanket-updated.**

**Files:**
- Inspect, and modify only if warranted: `crates/charpente/tests/replication.rs:526`, `:682`

- [ ] **Step 1: Run the replication suite and record what happens**

Run: `cargo test -p charpente --test replication`
Expected: this suite takes ~8 minutes. Record every failure verbatim.

- [ ] **Step 2: Read each affected site before touching it**

Both currently respond to the latch by calling `recover(sim, node)` — a restart,
which is exactly the remedy this fix makes structural. The likely outcome is that
they survive intact.

For each failure, answer in writing, in the ledger:
1. Did it fail because the node now stops where it used to continue?
2. Is the test's *intent* still served by restarting at the new stop point?
3. If the answer to (2) is no — if it encodes a genuine recovery path worth
   keeping — **stop. That is a finding against spec ruling 2.** Take it to Noah;
   do not edit the test to match the new behavior.

- [ ] **Step 3: Apply only the adjustments justified in Step 2**

No blanket search-and-replace. Each edit carries a one-line comment saying why
the new expectation is the correct one.

- [ ] **Step 4: Re-run and commit**

```bash
cargo test -p charpente --test replication
git add crates/charpente/tests/replication.rs
git commit -m "tests: replication sites adapted to the store-latch stop

<one line per site: what changed and why the new expectation is correct>"
```

If nothing needed changing, record that in the ledger and make no commit.

---

### Task 4: Docs, backlog retirement, and the evidence sweep

**Files:**
- Modify: `docs/GUIDE.md`, `CLAUDE.md`

- [ ] **Step 1: GUIDE operator paragraph**

In the GUIDE's operations section, add what a store-latch stop means: the node
stopped because its storage handle is dead after a disk IO error; **committed
data is safe** — the WAL is intact and authoritative; the remedy is a process
restart, which re-opens the database. Note that the underlying disk fault is the
real problem and a restart on failing hardware will latch again.

Run: `cargo build -p guide-examples`
Expected: clean (the gate compiles every ```rust block).

- [ ] **Step 2: Retire the CLAUDE.md backlog entry**

Delete the "checkpoint-latch gap" entry from the M2 follow-up backlog and replace
it with a close-out line in "Current state": what shipped, the mechanism, the
kept **DERIVED** caveat on the large-store shape, and the correspondence
assertion's location. Leave the dual-primary entry untouched.

Also correct the entry's own tripwire instruction, which said
`kernel_keeps_serving_with_a_latched_store_known_gap` must be updated and never
deleted: it was updated and renamed, and the new name belongs in the record.

- [ ] **Step 3: The evidence sweep (spec §9)**

Run each, record the result:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build -p guide-examples
cargo test --workspace                      # twice, both green
CHARPENTE_SEED=0x8bee1752811ae873 cargo test -p charpente-sim --test crash_recovery
CHARPENTE_SEED=0x348270b38f166529 cargo test -p charpente --test kernel
```

Both release mode-skew tripwires exactly as `.github/workflows/ci.yml` runs them
(`0x8bee1752811ae873` release crash_recovery; `0xff159ed76d2806e4` release
`late_follower_seeds_below_the_gcd_base_and_converges`).

A fresh invoicing batch, recording the master seed it prints:

```bash
cargo run -p charpente-cli -- test --sim --dir examples/invoicing --universes 1024
```

And, because Task 3 touched replication behavior, the edge matrix once:

```bash
scripts/edge-harness.sh
```

- [ ] **Step 4: Commit**

```bash
git add docs/GUIDE.md CLAUDE.md
git commit -m "docs: store-latch stop in the GUIDE; retire the checkpoint-latch backlog entry

Evidence: <the sweep results, incl. the invoicing master seed and edge matrix count>"
```

---

## Notes for the reviewer

- **The one thing worth attacking:** Task 2's correspondence assertion is what
  makes the whole structural-coupling trade (spec §1) safe. If it can be green
  while charpente's flag has drifted from redb's `io_failed`, the trade is
  unsound and the typed-detection design should be reconsidered.
- **Anti-vacuity:** Task 1's unit tests use an always-failing `File`, so they
  cannot pass by accident — but they also do not prove redb *routes* anything to
  those methods. The tripwire in Task 2 is what proves the wiring is real.
- **Ordering:** Tasks 1 and 2 are strictly sequential. Task 3 depends on Task 2.
  Task 4 is last. No task may be skipped to "come back to it."
