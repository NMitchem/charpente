# Object-Storage Archive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship sealed WAL segments and periodic logical snapshots to object storage, and restore a Db from them — Litestream-style, built into the binary, fully simulable.

**Architecture:** A fifth capability trait (`ObjectStore`) in `charpente-core::io`, poll-shaped like every other IO trait. A new `Archive` engine in the `charpente` crate runs as a fifth kernel `Ready` arm, doing one unit of work per `step()`. The bucket holds *generations* — a full snapshot plus the sealed-segment chain continuing it — and the manifest swaps last, so a crash leaves an older complete generation rather than a torn one. Restore drives the existing `Seeder`.

**Tech Stack:** Rust, no new dependencies. Prod impl is filesystem-backed (`std::fs` inside `prod.rs`, the one deny-list-allowed module). Sim impl is an in-memory bucket in `charpente-sim`.

**Spec:** `docs/superpowers/specs/2026-08-20-object-storage-archive-design.md` — read it before Task 1. The plan argues from the spec; where they disagree, the spec wins and the plan is the bug.

## Global Constraints

- **Hard rule 1:** no wall clocks, OS randomness, raw fs/net, or thread spawning outside `prod.rs`. The clippy deny-list enforces it; `-D warnings` in CI.
- **Hard rule 3:** every core subsystem lands with seeded fault-injection tests in the same commit. A bug fixed without a failing seed first was never reproduced.
- **Hard rule 4:** assert invariants and negative space liberally; assertions stay on in release.
- **Hard rule 5:** an error names the contract, the site, and the fix. Sim failures print the seed and a one-command repro. Write tests against error text.
- **Hard rule 6:** public-API changes update the docs in the same commit.
- **Mutation burden (spec §11):** this design's oracles are mostly negative — "never uploads while verifying", "never advances a foreign manifest". A test asserting something never happens passes trivially against an implementation that does nothing. **Every task's tests must be mutation-proven: name the one-line mutation, run it, show RED, revert.** A task whose tests survive their own mutation is not done.
- **Shared tree:** another session may be committing concurrently. **Commit named paths only — never `git add -A` or `git add .`** Hash the bytes you tested and compare against the staged blob before committing.
- **Worktree:** all cargo work runs in a dedicated worktree with its own `CARGO_TARGET_DIR`. Never share a target dir across worktrees.
- **zsh:** `PIPESTATUS` does not work. Use `${pipestatus[1]}` or `set -o pipefail`.
- **No benchmarks** without asking; the box is memory-constrained.
- **Commit style:** direct to `main`, no Co-Authored-By trailers.

---

## File Structure

**Created:**

| path | responsibility |
|---|---|
| `crates/charpente-core/src/io.rs` (modify) | `ObjectOp`, `ObjectDone`, `ObjectStore` — the trait only. Core never calls it. |
| `crates/charpente-sim/src/object.rs` | `SimObjectStore`: in-memory bucket + fault injection, seeded. |
| `crates/charpente/src/archive/mod.rs` | `Archive` engine: `open`, `step`, pin floor, generation state machine. |
| `crates/charpente/src/archive/manifest.rs` | `Manifest` type: encode/decode, CRC, writer identity. |
| `crates/charpente/src/archive/restore.rs` | `restore()` — the core-testable function over `&dyn ObjectStore` + `Box<dyn Disk>`. |
| `crates/charpente-cli/src/restore.rs` | CLI edge for `charpente restore`. |
| `crates/charpente/tests/archive.rs` | Integration + fault tests for the archive arm. |
| `crates/charpente-sim/tests/archive.rs` | The convergence property over seeded universes. |

**Modified:**

| path | change |
|---|---|
| `crates/charpente-core/src/db.rs:1650` | `pin_retention` → `add_retention_pin`, composing as a min over pins. |
| `crates/charpente/src/repl/mod.rs:255` | Call site follows the rename. |
| `crates/charpente/src/kernel/mod.rs:90,258,568` | `Ready::Archive` arm. |
| `crates/charpente/src/serve.rs` | `serve::archive` edge + archive config on replicated edges. |
| `crates/charpente/src/prod.rs` | `FsObjectStore`. |
| `crates/charpente-sim/src/lib.rs` | Export `SimObjectStore`; `FaultConfig` object-store axes. |
| `crates/charpente-cli/src/main.rs:5-18` | `mod restore;` + dispatch arm. |

**Why `archive/` is a directory, not a file:** the engine, the manifest codec, and restore have different reviewers' concerns and different test shapes. `repl/` is split the same way for the same reason.

---

## Task 1: The `ObjectStore` trait and its simulator

**Files:**
- Modify: `crates/charpente-core/src/io.rs` (append after the `Net`/`Listener`/`Conn` block)
- Create: `crates/charpente-sim/src/object.rs`
- Modify: `crates/charpente-sim/src/lib.rs` (add `pub mod object;` and re-export)
- Test: `crates/charpente-sim/src/object.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: nothing.
- Produces: `charpente_core::io::{ObjectOp, ObjectDone, ObjectStore}`; `charpente_sim::object::SimObjectStore` with `SimObjectStore::new() -> SimObjectStore` and `SimObjectStore::keys(&self) -> Vec<String>` (test-only inspection).

- [ ] **Step 1: Write the failing contract tests**

In `crates/charpente-sim/src/object.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use charpente_core::io::{ObjectDone, ObjectOp, ObjectStore};

    fn drain(s: &SimObjectStore) -> std::io::Result<ObjectDone> {
        loop {
            if let Some(r) = s.poll() {
                return r;
            }
        }
    }

    #[test]
    fn put_then_get_roundtrips() {
        let s = SimObjectStore::new();
        s.start(ObjectOp::Put { key: "a".into(), data: vec![1, 2, 3] }).unwrap();
        assert!(matches!(drain(&s).unwrap(), ObjectDone::Put));
        s.start(ObjectOp::Get { key: "a".into() }).unwrap();
        assert!(matches!(drain(&s).unwrap(), ObjectDone::Get(v) if v == vec![1, 2, 3]));
    }

    #[test]
    fn list_is_sorted_and_prefix_scoped() {
        let s = SimObjectStore::new();
        for k in ["p/b", "p/a", "q/c"] {
            s.start(ObjectOp::Put { key: k.into(), data: vec![] }).unwrap();
            drain(&s).unwrap();
        }
        s.start(ObjectOp::List { prefix: "p/".into() }).unwrap();
        match drain(&s).unwrap() {
            ObjectDone::List(v) => assert_eq!(v, vec!["p/a".to_string(), "p/b".to_string()]),
            other => panic!("expected List, got {other:?}"),
        }
    }

    // The contract that distinguishes ObjectStore from Disk (spec §1): a PUT
    // is atomic. A failed PUT leaves the key absent or holding its PREVIOUS
    // value — never half. Disk's File tears; this must not.
    #[test]
    fn failed_put_leaves_previous_value_never_a_partial() {
        let s = SimObjectStore::new();
        s.start(ObjectOp::Put { key: "a".into(), data: vec![1; 8] }).unwrap();
        drain(&s).unwrap();
        s.fail_next_put();
        s.start(ObjectOp::Put { key: "a".into(), data: vec![2; 8] }).unwrap();
        assert!(drain(&s).is_err());
        s.start(ObjectOp::Get { key: "a".into() }).unwrap();
        match drain(&s).unwrap() {
            ObjectDone::Get(v) => assert_eq!(v, vec![1; 8], "torn or half-written object"),
            other => panic!("expected Get, got {other:?}"),
        }
    }

    #[test]
    #[should_panic(expected = "operation already in flight")]
    fn second_start_before_poll_asserts() {
        let s = SimObjectStore::new();
        s.start(ObjectOp::Get { key: "a".into() }).unwrap();
        let _ = s.start(ObjectOp::Get { key: "b".into() });
    }

    #[test]
    fn get_absent_key_is_not_found() {
        let s = SimObjectStore::new();
        s.start(ObjectOp::Get { key: "nope".into() }).unwrap();
        let e = drain(&s).unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::NotFound);
        assert!(e.to_string().contains("nope"), "error must name the key: {e}");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p charpente-sim --lib object`
Expected: FAIL — `SimObjectStore` and the `io` types do not exist.

- [ ] **Step 3: Add the trait to core**

In `crates/charpente-core/src/io.rs`, after the `Conn` trait:

```rust
/// One object-storage operation. Whole-object, never offset-addressed —
/// object stores have no partial writes (DESIGN §1, archive spec §1).
#[derive(Debug)]
pub enum ObjectOp {
    Put { key: String, data: Vec<u8> },
    Get { key: String },
    List { prefix: String },
    Delete { key: String },
}

/// The completion of an `ObjectOp`.
#[derive(Debug)]
pub enum ObjectDone {
    Put,
    Get(Vec<u8>),
    /// All keys under the prefix, sorted — callers must never depend on
    /// enumeration order, same rule as `Disk::list`.
    List(Vec<String>),
    Delete,
}

/// Off-box object storage. One operation in flight at a time.
///
/// **A PUT is atomic.** A failed PUT leaves the key absent or holding its
/// previous value, never half of the new one — the precise opposite of
/// `File`'s torn-write contract, and the reason this is not `Disk` reuse.
/// There is no `sync_dir` analogue: there is no directory entry to persist.
/// Ordering between distinct keys is NOT guaranteed.
///
/// `Send` for the same reason as `Disk`: `App` crosses into the kernel thread.
pub trait ObjectStore: Send {
    /// Begins `op`. Asserts nothing is already in flight — a second `start`
    /// before `poll` returns `Some` is a caller bug, not a runtime condition.
    fn start(&self, op: ObjectOp) -> io::Result<()>;

    /// `None` means still in flight.
    fn poll(&self) -> Option<io::Result<ObjectDone>>;
}
```

- [ ] **Step 4: Implement `SimObjectStore`**

In `crates/charpente-sim/src/object.rs`:

```rust
//! The simulated object store: an in-memory bucket behind `ObjectStore`.
//! Completions are immediate — latency and faults arrive in Task 2.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;

use charpente_core::io::{ObjectDone, ObjectOp, ObjectStore};

pub struct SimObjectStore {
    objects: RefCell<BTreeMap<String, Vec<u8>>>,
    in_flight: RefCell<Option<ObjectOp>>,
    fail_next_put: RefCell<bool>,
}

impl SimObjectStore {
    #[allow(clippy::new_without_default)]
    pub fn new() -> SimObjectStore {
        SimObjectStore {
            objects: RefCell::new(BTreeMap::new()),
            in_flight: RefCell::new(None),
            fail_next_put: RefCell::new(false),
        }
    }

    /// Test hook: the next `Put` fails after the op is accepted.
    pub fn fail_next_put(&self) {
        *self.fail_next_put.borrow_mut() = true;
    }

    /// Test-only inspection of the bucket's keys, sorted.
    pub fn keys(&self) -> Vec<String> {
        self.objects.borrow().keys().cloned().collect()
    }
}

// SAFETY-equivalent note: the simulator is single-threaded; `Send` is honest
// because nothing hands a SimObjectStore across threads. Same reasoning as
// the other sim IO impls.
unsafe impl Send for SimObjectStore {}

impl ObjectStore for SimObjectStore {
    fn start(&self, op: ObjectOp) -> io::Result<()> {
        assert!(
            self.in_flight.borrow().is_none(),
            "ObjectStore::start called with an operation already in flight — \
             poll to completion before starting another (archive spec §1)"
        );
        *self.in_flight.borrow_mut() = Some(op);
        Ok(())
    }

    fn poll(&self) -> Option<io::Result<ObjectDone>> {
        let op = self.in_flight.borrow_mut().take()?;
        Some(match op {
            ObjectOp::Put { key, data } => {
                if self.fail_next_put.replace(false) {
                    // The prior value stays: a PUT is atomic, never torn.
                    Err(io::Error::other(format!("simulated PUT failure for \"{key}\"")))
                } else {
                    self.objects.borrow_mut().insert(key, data);
                    Ok(ObjectDone::Put)
                }
            }
            ObjectOp::Get { key } => match self.objects.borrow().get(&key) {
                Some(v) => Ok(ObjectDone::Get(v.clone())),
                None => Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no object at key \"{key}\""),
                )),
            },
            ObjectOp::List { prefix } => Ok(ObjectDone::List(
                self.objects
                    .borrow()
                    .range(prefix.clone()..)
                    .take_while(|(k, _)| k.starts_with(&prefix))
                    .map(|(k, _)| k.clone())
                    .collect(),
            )),
            ObjectOp::Delete { key } => {
                self.objects.borrow_mut().remove(&key);
                Ok(ObjectDone::Delete)
            }
        })
    }
}
```

In `crates/charpente-sim/src/lib.rs`, add `pub mod object;`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p charpente-sim --lib object`
Expected: PASS, 5 tests.

- [ ] **Step 6: Prove the tests have teeth (mutation burden)**

Run each mutation, confirm RED, revert:

| # | mutation | must redden |
|---|---|---|
| 1 | delete the `assert!` in `start` | `second_start_before_poll_asserts` |
| 2 | in the failed-put arm, `insert` before returning `Err` | `failed_put_leaves_previous_value_never_a_partial` |
| 3 | drop `.take_while(...)` from `List` | `list_is_sorted_and_prefix_scoped` |
| 4 | return `Ok(ObjectDone::Get(Vec::new()))` for an absent key | `get_absent_key_is_not_found` |

Record the 4/4 result in the commit message. **A mutation that stays green means the test is decorative — fix the test, not the mutation.**

- [ ] **Step 7: Gate and commit**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p charpente-core -p charpente-sim
git add crates/charpente-core/src/io.rs crates/charpente-sim/src/object.rs crates/charpente-sim/src/lib.rs
git commit -m "feat: the ObjectStore capability trait, and its simulator

A PUT is atomic where a File write tears — that contract is the reason this
is a trait rather than Disk reuse, and it is the second test. One op in
flight, asserted rather than handled. Mutation table 4/4 RED."
```

---

## Task 2: Fault injection on the simulated object store

**Files:**
- Modify: `crates/charpente-sim/src/object.rs`
- Modify: `crates/charpente-sim/src/lib.rs:33-68` (`FaultConfig` fields), `:255` (`sample_extended_fields`)
- Test: `crates/charpente-sim/src/object.rs` tests module

**Interfaces:**
- Consumes: `SimObjectStore` (Task 1), `keyed_stream`, `FaultConfig`.
- Produces: `SimObjectStore::with_faults(seed: u64, cfg: FaultConfig) -> SimObjectStore`; `FaultConfig::{object_put_error, object_get_error, object_lost_ack}` (all `f64`).

**Why three axes and not more:** `object_put_error` and `object_get_error` cover reachability. `object_lost_ack` is the one that is *not* a duplicate — the store applied the PUT but the caller was told it failed, which is the case that makes retry-idempotence load-bearing. There is deliberately no torn-object axis: a PUT is atomic (spec §1), and injecting a fault the real thing cannot produce would test a fiction.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn lost_ack_leaves_the_object_present_despite_the_error() {
    let mut cfg = FaultConfig::default();
    cfg.object_lost_ack = 1.0;
    let s = SimObjectStore::with_faults(0x5eed, cfg);
    s.start(ObjectOp::Put { key: "a".into(), data: vec![7; 4] }).unwrap();
    assert!(drain(&s).is_err(), "lost ack must surface as an error");
    // ...but the write landed. This is why retry must be idempotent.
    s.start(ObjectOp::Get { key: "a".into() }).unwrap();
    assert!(matches!(drain(&s).unwrap(), ObjectDone::Get(v) if v == vec![7; 4]));
}

#[test]
fn retrying_a_lost_ack_put_is_idempotent() {
    let mut cfg = FaultConfig::default();
    cfg.object_lost_ack = 1.0;
    let s = SimObjectStore::with_faults(0x5eed, cfg);
    for _ in 0..3 {
        s.start(ObjectOp::Put { key: "a".into(), data: vec![7; 4] }).unwrap();
        let _ = drain(&s);
    }
    assert_eq!(s.keys(), vec!["a".to_string()], "retry duplicated the object");
}

#[test]
fn put_error_leaves_the_object_absent() {
    let mut cfg = FaultConfig::default();
    cfg.object_put_error = 1.0;
    let s = SimObjectStore::with_faults(0x5eed, cfg);
    s.start(ObjectOp::Put { key: "a".into(), data: vec![1] }).unwrap();
    assert!(drain(&s).is_err());
    assert!(s.keys().is_empty(), "a failed PUT must not land");
}

// DESIGN §1: each extended field draws from its own keyed sub-stream, so
// adding a field perturbs no existing universe's draws. If this breaks,
// every pinned seed in the repo changes meaning.
#[test]
fn adding_object_axes_does_not_perturb_existing_extended_draws() {
    for seed in [0x1u64, 0xff159ed76d2806e4, 0x8bee1752811ae873] {
        let mut a = FaultConfig::default();
        sample_extended_fields(seed, &mut a);
        assert_eq!(a.bit_flip, { let mut b = FaultConfig::default();
            b.bit_flip = a.bit_flip; b.bit_flip });
        // The real assertion: these three are drawn from keyed_stream(seed, name)
        // and must equal the values recorded before the object axes existed.
        let mut r = keyed_stream(seed, "bit_flip");
        let expect = if r.chance(0.25) { r.below(20) as f64 / 1000.0 } else { 0.0 };
        assert_eq!(a.bit_flip, expect, "bit_flip draw moved for seed {seed:#x}");
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p charpente-sim --lib object`
Expected: FAIL — `with_faults` and the `FaultConfig` fields do not exist.

- [ ] **Step 3: Add the fault fields**

In `crates/charpente-sim/src/lib.rs`, in `FaultConfig` after `read_error`:

```rust
    /// P(an `ObjectOp::Put` fails and the object does NOT land).
    pub object_put_error: f64,
    /// P(an `ObjectOp::Get`/`List` returns a transient injected IO error).
    pub object_get_error: f64,
    /// P(a `Put` lands durably but the caller is told it failed — the case
    /// that makes retry-idempotence load-bearing. Not a duplicate of
    /// `object_put_error`: that one leaves the object absent, this one does
    /// not. There is deliberately no torn-object axis; a PUT is atomic.
    pub object_lost_ack: f64,
```

In `sample_extended_fields`, appended **after** the existing draws so no
existing field's stream position moves:

```rust
    let mut r = keyed_stream(seed, "object_put_error");
    cfg.object_put_error = if r.chance(0.25) { r.below(50) as f64 / 1000.0 } else { 0.0 };
    let mut r = keyed_stream(seed, "object_get_error");
    cfg.object_get_error = if r.chance(0.25) { r.below(50) as f64 / 1000.0 } else { 0.0 };
    let mut r = keyed_stream(seed, "object_lost_ack");
    cfg.object_lost_ack = if r.chance(0.20) { r.below(20) as f64 / 1000.0 } else { 0.0 };
```

- [ ] **Step 4: Implement `with_faults`**

Add a `faults: Option<(RefCell<Rng>, FaultConfig)>` field to `SimObjectStore`; in `poll`, roll `object_put_error` then `object_lost_ack` on the `Put` arm (put-error checked first — the object does not land), and `object_get_error` on the `Get`/`List` arms. `fail_next_put` stays for the deterministic Task 1 tests.

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p charpente-sim --lib object`
Expected: PASS, 9 tests.

- [ ] **Step 6: Verify the whole suite still replays identically**

Run: `cargo test --workspace`
Expected: PASS. **This is the real gate for this task** — if any pinned seed changed meaning, the sub-stream isolation is broken and the fix is in `sample_extended_fields`, not in the test.

- [ ] **Step 7: Mutation table**

| # | mutation | must redden |
|---|---|---|
| 1 | make `object_lost_ack` skip the insert | `lost_ack_leaves_the_object_present_despite_the_error` |
| 2 | make `object_put_error` insert before erroring | `put_error_leaves_the_object_absent` |
| 3 | move the object draws *before* `bit_flip` in `sample_extended_fields` | `adding_object_axes_does_not_perturb_existing_extended_draws` |

- [ ] **Step 8: Gate and commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
git add crates/charpente-sim/src/object.rs crates/charpente-sim/src/lib.rs
git commit -m "test: fault the object store — reachability, and the lost ack

Three axes. object_lost_ack is the one that is not a duplicate: the PUT landed
and the caller was told it did not, which is what makes retry-idempotence
load-bearing. No torn-object axis on purpose — a PUT is atomic, and injecting
a fault the real thing cannot produce would test a fiction. Draws appended
after the existing extended fields; full workspace green proves no pinned seed
changed meaning. Mutation table 3/3 RED."
```

---

## Task 3: Compose retention pins

**Files:**
- Modify: `crates/charpente-core/src/db.rs:1645-1652`
- Modify: `crates/charpente/src/repl/mod.rs:255`
- Modify: `crates/charpente/tests/transactions.rs:1205`
- Test: `crates/charpente/tests/transactions.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `Db::add_retention_pin(&mut self, pin: Box<dyn Fn() -> u64 + Send>)`. **`pin_retention` is removed, not deprecated** — the rename is deliberate so every call site is a compile error rather than a silent semantic change. This is the codebase's own principle: a contract change must be a compile error, not a 500.

- [ ] **Step 1: Write the failing test**

In `crates/charpente/tests/transactions.rs`:

```rust
#[test]
fn retention_pins_compose_as_a_min() {
    let (mut db, _dir) = open_test_db();
    let a = Arc::new(AtomicU64::new(50));
    let b = Arc::new(AtomicU64::new(20));
    db.add_retention_pin(Box::new({ let a = Arc::clone(&a); move || a.load(Ordering::Relaxed) }));
    db.add_retention_pin(Box::new({ let b = Arc::clone(&b); move || b.load(Ordering::Relaxed) }));

    assert_eq!(db.retention_floor(), 20, "floor must be the min over pins");
    b.store(90, Ordering::Relaxed);
    assert_eq!(db.retention_floor(), 50, "floor must track the new min, not the first pin");
    a.store(u64::MAX, Ordering::Relaxed);
    b.store(u64::MAX, Ordering::Relaxed);
    assert_eq!(db.retention_floor(), u64::MAX, "no pin holding means no floor");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p charpente --test transactions retention_pins_compose`
Expected: FAIL — `add_retention_pin` and `retention_floor` do not exist.

- [ ] **Step 3: Implement**

In `db.rs`, replace `retention_pin: Option<Box<dyn Fn() -> u64 + Send>>` with
`retention_pins: Vec<Box<dyn Fn() -> u64 + Send>>`, and:

```rust
    /// Registers a floor `checkpoint`'s GC will never delete past — e.g. a
    /// replication cursor, or the archive's un-uploaded segment (archive
    /// spec §5). **Pins accumulate; the effective floor is the min over all
    /// of them.** A pin that is not holding returns `u64::MAX`.
    ///
    /// Renamed from `pin_retention`, which overrode: two subsystems now hold
    /// floors independently, and an override would have let whichever
    /// registered last silently delete the other's records.
    pub fn add_retention_pin(&mut self, pin: Box<dyn Fn() -> u64 + Send>) {
        self.retention_pins.push(pin);
    }

    /// The effective retention floor: the min over registered pins, or
    /// `u64::MAX` when none holds.
    pub fn retention_floor(&self) -> u64 {
        self.retention_pins.iter().map(|p| p()).min().unwrap_or(u64::MAX)
    }
```

Update every read of the old field to call `retention_floor()`. Update `repl/mod.rs:255` and `transactions.rs:1205` to the new name.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p charpente --test transactions`
Expected: PASS.

- [ ] **Step 5: Verify replication's existing pin behavior is unchanged**

Run: `cargo test -p charpente --test replication`
Expected: PASS, no seed changes. Replication registering one pin must behave exactly as before.

- [ ] **Step 6: Mutation table**

| # | mutation | must redden |
|---|---|---|
| 1 | `.min()` → `.next()` (first pin wins) | `retention_pins_compose_as_a_min` (second assertion) |
| 2 | `.min()` → `.max()` | first assertion |
| 3 | `unwrap_or(u64::MAX)` → `unwrap_or(0)` | third assertion |

- [ ] **Step 7: Gate and commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
git add crates/charpente-core/src/db.rs crates/charpente/src/repl/mod.rs crates/charpente/tests/transactions.rs
git commit -m "refactor: retention pins compose as a min, and the rename forces the review

Two subsystems now hold retention floors independently. pin_retention
overrode, so whichever registered last would have silently deleted the
other's records. Renamed to add_retention_pin rather than changing the
semantics under the old name: every call site becomes a compile error, which
is the oracle this codebase asks for. Mutation table 3/3 RED."
```

---

## Task 4: The manifest and the generation model

**Files:**
- Create: `crates/charpente/src/archive/mod.rs` (module decl only this task)
- Create: `crates/charpente/src/archive/manifest.rs`
- Modify: `crates/charpente/src/lib.rs` (add `pub mod archive;`)
- Test: `crates/charpente/src/archive/manifest.rs` tests module

**Interfaces:**
- Consumes: nothing.
- Produces:

```rust
pub struct Manifest {
    pub generation: u64,
    pub writer: String,      // node identity; §4's ownership rule
    pub applied_seq: u64,    // the snapshot's frontier
    pub next_job_id: u64,
    pub tables: Vec<String>,
    pub segments: Vec<u64>,  // base_seqs, ascending, contiguous
    pub closed: bool,        // true once a gap ended this generation
}
impl Manifest {
    pub fn encode(&self) -> Vec<u8>;
    pub fn decode(bytes: &[u8]) -> io::Result<Manifest>;
}
pub fn manifest_key(prefix: &str) -> String;             // "<prefix>/manifest"
pub fn segment_key(prefix: &str, generation: u64, base_seq: u64) -> String;
pub fn snapshot_meta_key(prefix: &str, generation: u64) -> String;
pub fn snapshot_batch_key(prefix: &str, generation: u64, table: &str, n: u64) -> String;
```

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn manifest_roundtrips() {
    let m = Manifest {
        generation: 3, writer: "node-a:9000".into(), applied_seq: 4096,
        next_job_id: 12, tables: vec!["invoice".into(), "charpente.jobs".into()],
        segments: vec![4096, 8192], closed: false,
    };
    assert_eq!(Manifest::decode(&m.encode()).unwrap(), m);
}

#[test]
fn a_corrupt_manifest_is_refused_by_name_never_guessed() {
    let m = Manifest { generation: 1, writer: "n".into(), applied_seq: 0, next_job_id: 0,
                       tables: vec![], segments: vec![], closed: false };
    let mut bytes = m.encode();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x01;
    let e = Manifest::decode(&bytes).unwrap_err();
    let t = e.to_string();
    assert!(t.contains("manifest"), "must name the contract: {t}");
    assert!(t.contains("CRC"), "must name what failed: {t}");
    assert!(t.contains("restore"), "must name the fix: {t}");
}

#[test]
fn a_discontiguous_segment_chain_is_refused_at_decode() {
    // A generation's chain is never allowed to have a hole (spec §8).
    let m = Manifest { generation: 1, writer: "n".into(), applied_seq: 0, next_job_id: 0,
                       tables: vec![], segments: vec![0, 4096, 16384], closed: false };
    // encode() must assert; decode of a hand-built hole must refuse.
    let e = Manifest::decode(&hand_built_with_hole()).unwrap_err();
    assert!(e.to_string().contains("16384"), "must name the expected base_seq: {e}");
}

#[test]
fn keys_are_stable_and_zero_padded() {
    assert_eq!(manifest_key("bk"), "bk/manifest");
    assert_eq!(segment_key("bk", 2, 4096), "bk/00000000000000000002/segments/00000000000000004096.log");
    assert_eq!(snapshot_meta_key("bk", 2), "bk/00000000000000000002/snapshot/meta");
    assert_eq!(snapshot_batch_key("bk", 2, "invoice", 0), "bk/00000000000000000002/snapshot/invoice/00000000000000000000");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p charpente --lib archive::manifest`
Expected: FAIL — module does not exist.

- [ ] **Step 3: Implement**

Encode as length-prefixed fields followed by a trailing CRC32 over everything
prior — the same shape as the durable role file's CRC line (`repl/role.rs`),
which exists so a torn write is a named integrity error rather than a parse of
whatever survived. Zero-pad generation and base_seq to 20 digits so
lexicographic `List` order equals numeric order. `decode` verifies the CRC
first, then asserts chain contiguity, naming the first offending base_seq.

- [ ] **Step 4: Run to verify pass** — `cargo test -p charpente --lib archive::manifest`

- [ ] **Step 5: Mutation table**

| # | mutation | must redden |
|---|---|---|
| 1 | skip the CRC check in `decode` | `a_corrupt_manifest_is_refused_by_name_never_guessed` |
| 2 | drop the contiguity assert | `a_discontiguous_segment_chain_is_refused_at_decode` |
| 3 | `{:020}` → `{}` in `segment_key` | `keys_are_stable_and_zero_padded` |

- [ ] **Step 6: Gate and commit** (named paths; message states 3/3 RED)

---

## Task 5: The archive engine ships sealed segments

**Files:**
- Modify: `crates/charpente/src/archive/mod.rs`
- Create: `crates/charpente/tests/archive.rs`

**Interfaces:**
- Consumes: `ObjectStore` (T1), `Manifest` (T4), `Db::add_retention_pin`/`retention_floor` (T3), `Db::read_log`, `Db::log_base`, `Wal` segment naming.
- Produces:

```rust
pub struct ArchiveConfig {
    pub prefix: String,
    pub writer: String,
    pub snapshot_every: u64,   // records; mirrors DbConfig::checkpoint_every
    pub pin_segments: usize,   // default 8 — see below
}
pub struct Archive { /* ... */ }
impl Archive {
    pub fn open(cfg: ArchiveConfig, store: Box<dyn ObjectStore>) -> io::Result<Archive>;
    /// One unit of work. Never blocks, never touches the Db's write path.
    pub fn step(&mut self, db: &Db) -> io::Result<()>;
    /// The retention floor this archive holds; `u64::MAX` when not holding.
    pub fn pin_floor(&self) -> u64;
    pub fn gapped(&self) -> bool;
}
```

**`pin_segments` default = 8, justified** (spec §14 item 1 asked the plan to justify rather than invent): `DbConfig::segment_bytes` defaults to 64 MiB, so 8 segments bounds the WAL held above the checkpoint floor at **512 MiB** during an object-store outage. That is small next to any host running a 64 MiB-segment workload, and it is 4× `DbConfig::retain_segments` (default 2) — the same order as the existing floor, not a new magnitude. If Noah wants a different number this is the one line to change.

**Test fixture helpers — define these once, at the top of
`crates/charpente/tests/archive.rs`. Every later task's tests use them:**

```rust
const RECORDS_PER_SEGMENT: u64 = 512;   // at the reduced sim segment_bytes below

/// A Db + SimObjectStore + Archive, wired, writer identity "node-a:9000",
/// prefix "archive-test-prefix". `segment_bytes` is reduced to 64 KiB so a
/// test seals segments in hundreds of records rather than hundreds of
/// thousands — the production default is 64 MiB (`log.rs:75`).
fn archive_fixture() -> (Db, Rc<SimObjectStore>, Archive);
fn archive_fixture_with_unreachable_store() -> (Db, Rc<SimObjectStore>, Archive);
fn archive_fixture_with_lost_acks() -> (Db, Rc<SimObjectStore>, Archive);

fn write_records(db: &mut Db, n: u64);
fn step_until_idle(arc: &mut Archive, db: &Db);
fn step_until_error(arc: &mut Archive, db: &Db) -> io::Error;
fn step_until_notice(arc: &mut Archive, db: &Db) -> io::Error;
fn current_tail_base_seq(db: &Db) -> u64;
fn read_manifest(store: &SimObjectStore) -> Option<Manifest>;
fn write_foreign_manifest(store: &SimObjectStore, writer: &str, generation: u64);
fn restore_store_reachability(store: &SimObjectStore);
```

### Discharging spec §3's obligation to this plan

Spec §3 says: *"The plan therefore owes a measured seal rate and a stated
minimum bandwidth, plus a decision on whether N-in-flight is needed."*

**Decision: one operation in flight, for v1.** The bound reduces to *upload
bandwidth must exceed sustained write throughput in bytes/sec* (spec §3), and
that is a property this plan can **assert deterministically in the simulator
rather than benchmark** — the seal rate is a pure function of `segment_bytes`
and mean record size, and the sim counts seals exactly. **No benchmark run is
required, and none should be attempted:** `docs/BENCH.md` is the only source of
quotable numbers, it has no archive measurements, and the box is memory-
constrained.

Add this test to Task 5's set:

```rust
#[test]
fn the_seal_rate_stays_under_one_upload_per_step_budget() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 50 * RECORDS_PER_SEGMENT);
    let seals = sealed_segment_count(&db);
    let uploads = step_until_idle_counting(&mut arc, &db);
    assert!(uploads >= seals,
        "the archive fell permanently behind: {seals} seals, {uploads} uploads —          one-in-flight is insufficient and ArchiveConfig needs an N-in-flight knob");
}
```

If that assertion ever fails on real hardware, the fix is a documented
`ArchiveConfig::in_flight` field, not a silent widening — **and it needs its own
BENCH.md entry before any number about it is quoted.**

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn only_sealed_segments_ship_never_the_tail() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 200);          // seals at least one segment
    step_until_idle(&mut arc, &db);
    let shipped = store.keys();
    let tail = current_tail_base_seq(&db);
    assert!(!shipped.iter().any(|k| k.contains(&format!("{tail:020}"))),
        "shipped the live tail: {shipped:?}");
    assert!(shipped.iter().any(|k| k.contains("/segments/")), "shipped nothing at all");
}

#[test]
fn the_pin_holds_at_the_lowest_un_uploaded_segment() {
    let (mut db, _store, mut arc) = archive_fixture_with_unreachable_store();
    write_records(&mut db, 500);
    step_until_idle(&mut arc, &db);
    assert_eq!(arc.pin_floor(), db.log_base(), "pin must hold everything un-uploaded");
}

#[test]
fn an_archive_io_failure_never_poisons_the_db() {
    let (mut db, _store, mut arc) = archive_fixture_with_unreachable_store();
    write_records(&mut db, 100);
    for _ in 0..50 { let _ = arc.step(&db); }
    assert!(!db.poisoned(), "an archive failure is not a database failure");
    write_records(&mut db, 10);           // still writable
}

#[test]
fn a_lost_ack_retry_uploads_the_same_bytes_and_does_not_duplicate() {
    let (mut db, store, mut arc) = archive_fixture_with_lost_acks();
    write_records(&mut db, 300);
    step_until_idle(&mut arc, &db);
    let keys = store.keys();
    let mut deduped = keys.clone();
    deduped.dedup();
    assert_eq!(keys, deduped, "retry duplicated an object");
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test -p charpente --test archive`

- [ ] **Step 3: Implement `Archive::step`**

State machine, one transition per call: `Idle` → pick the lowest sealed,
un-uploaded segment → `start(Put)` → `Uploading` → `poll` → on `Ok` record it
and advance; on `Err` stay and retry the same key with the same bytes. A
segment is *sealed* when a segment with a higher base_seq exists (`Disk::list`
over the log dir), which is the same test recovery uses. Read bytes through
`Db::read_log` so the CRC is re-verified and nothing above `last_applied`
travels. The pin floor is the lowest un-uploaded sealed base_seq, or
`u64::MAX` when caught up.

- [ ] **Step 4: Run to verify pass**

- [ ] **Step 5: Mutation table**

| # | mutation | must redden |
|---|---|---|
| 1 | drop the sealed check (ship the tail too) | `only_sealed_segments_ship_never_the_tail` |
| 2 | `pin_floor` returns `u64::MAX` unconditionally | `the_pin_holds_at_the_lowest_un_uploaded_segment` |
| 3 | propagate the store error into `db.poison()` | `an_archive_io_failure_never_poisons_the_db` |
| 4 | on retry, re-read bytes and bump a counter into the key | `a_lost_ack_retry_uploads_the_same_bytes_and_does_not_duplicate` |

- [ ] **Step 6: Gate and commit** (named paths; 4/4 RED)

---

## Task 6: Snapshots, and the manifest swaps last

**Files:**
- Modify: `crates/charpente/src/archive/mod.rs`
- Modify: `crates/charpente/tests/archive.rs`

**Interfaces:**
- Consumes: T5's `Archive`, `Db::snapshot`, `Snapshot::scan_raw_from` (`table.rs:1043`), `Snapshot::applied_seq`/`next_job_id`, `Schema::table_names`.
- Produces: no new public names; `Archive::step` gains the snapshot and seal transitions.

**Memory rule, load-bearing (spec §10):** use `scan_raw_from(table, after_id, limit)` and page. **`scan_raw` must not appear anywhere in this module** — it returns a whole table at once, and the archive snapshots on a cadence rather than once per join, so it would hit the ceiling seeding has not.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn the_manifest_never_points_at_an_incomplete_generation() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 5_000);
    // Crash the archive at every step index; at each one, whatever the
    // manifest names must be fully present.
    for cut in 0..200 {
        let (store, mut arc) = replay_archive_to_step(&db, cut);
        if let Some(m) = read_manifest(&store) {
            for key in expected_keys(&m) {
                assert!(store.keys().contains(&key),
                    "manifest gen {} names missing key {key} (cut at step {cut})", m.generation);
            }
        }
        let _ = arc;
    }
}

#[test]
fn snapshot_upload_never_holds_a_whole_table_in_memory() {
    // Proxy assertion: the module must not reference scan_raw.
    let src = include_str!("../src/archive/mod.rs");
    assert!(!src.contains("scan_raw("),
        "archive must use scan_raw_from — scan_raw returns a whole table (spec §10)");
}

#[test]
fn snapshot_cadence_is_seq_driven_and_clock_skew_changes_nothing() {
    let a = run_archive_with_clock_skew(0);
    let b = run_archive_with_clock_skew(3_600_000_000_000);
    assert_eq!(a, b, "clock skew changed archive behavior; cadence must be seq-driven");
}
```

- [ ] **Step 2: Run to verify failure**

- [ ] **Step 3: Implement**

New transitions: when `db.last_applied()` — an **`Option<u64>`** (`db.rs:1741`), `None`
on a Db that has applied nothing — crosses the next `snapshot_every` multiple, open ONE `Db::snapshot` and walk
`Schema::table_names` in order, paging `scan_raw_from` into
`snapshot/<table>/<n>` objects. Then `snapshot/meta` (applied_seq,
next_job_id, table list, CRC). **Then, and only then, PUT the manifest.**
Order is the whole safety argument: inter-key ordering is not guaranteed, so
the manifest swap is the only operation whose completion means anything.

- [ ] **Step 4: Run to verify pass**

- [ ] **Step 5: Mutation table**

| # | mutation | must redden |
|---|---|---|
| 1 | swap the manifest *before* the snapshot objects | `the_manifest_never_points_at_an_incomplete_generation` |
| 2 | `scan_raw_from` → `scan_raw` | `snapshot_upload_never_holds_a_whole_table_in_memory` |
| 3 | drive cadence off `clock.now()` | `snapshot_cadence_is_seq_driven_and_clock_skew_changes_nothing` |

- [ ] **Step 6: Gate and commit** (named paths; 3/3 RED)

---

## Task 7: The bounded pin, and the named gap

**Files:**
- Modify: `crates/charpente/src/archive/mod.rs`
- Modify: `crates/charpente/tests/archive.rs`

**Interfaces:**
- Consumes: T5/T6 `Archive`, `ArchiveConfig::pin_segments`.
- Produces: `Archive::gapped()` becomes meaningful; `ArchiveNotice` in the step error path.

**The ruling this implements (spec ruling 3):** on outage the archive yields, never the primary. An unbounded pin would grow the WAL until the disk fills and the log poisons — the archive would have acquired the power to stop the database.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn past_the_bound_gc_proceeds_and_the_primary_keeps_writing() {
    let (mut db, _store, mut arc) = archive_fixture_with_unreachable_store();
    let base_before = db.log_base();
    write_records(&mut db, 20 * RECORDS_PER_SEGMENT);   // far past pin_segments
    step_until_idle(&mut arc, &db);
    db.checkpoint().unwrap();
    assert!(db.log_base() > base_before, "GC did not proceed; the archive stalled the primary");
    assert!(!db.poisoned());
    write_records(&mut db, 10);                          // still writable
    assert!(arc.gapped(), "a released pin must record a gap");
}

#[test]
fn the_gap_notice_names_the_bound_the_prefix_and_both_fixes() {
    let (mut db, _store, mut arc) = archive_fixture_with_unreachable_store();
    write_records(&mut db, 20 * RECORDS_PER_SEGMENT);
    let notice = step_until_notice(&mut arc, &db);
    let t = notice.to_string();
    for needle in ["pin_segments", "archive-test-prefix", "raise", "generation"] {
        assert!(t.contains(needle), "notice must name {needle}: {t}");
    }
}

#[test]
fn after_a_gap_the_next_contact_opens_a_new_generation_with_a_full_snapshot() {
    let (mut db, store, mut arc) = archive_fixture_with_unreachable_store();
    write_records(&mut db, 20 * RECORDS_PER_SEGMENT);
    step_until_idle(&mut arc, &db);
    restore_store_reachability(&store);
    step_until_idle(&mut arc, &db);
    let m = read_manifest(&store).expect("a generation must land");
    assert_eq!(m.generation, 2, "a gap must close a generation and open the next");
    assert!(!m.segments.is_empty() && m.applied_seq > 0, "the new generation needs its own snapshot");
}
```

- [ ] **Step 2: Run to verify failure**

- [ ] **Step 3: Implement**

Track un-uploaded sealed segment count. Under `pin_segments`, `pin_floor()`
returns the lowest un-uploaded base_seq. At or past it, `pin_floor()` returns
`u64::MAX` (release), set `gapped = true`, and emit the notice once — not once
per step. A gapped archive that regains the store closes the current
generation (`closed: true`) and starts the next at a fresh full snapshot.

- [ ] **Step 4: Run to verify pass**

- [ ] **Step 5: Mutation table**

| # | mutation | must redden |
|---|---|---|
| 1 | never release the pin | `past_the_bound_gc_proceeds_and_the_primary_keeps_writing` |
| 2 | release but leave `gapped = false` | same test's last assertion |
| 3 | drop `pin_segments` from the notice text | `the_gap_notice_names_the_bound_the_prefix_and_both_fixes` |
| 4 | continue the old generation after a gap | `after_a_gap_the_next_contact_opens_a_new_generation_with_a_full_snapshot` |

- [ ] **Step 6: Gate and commit** (named paths; 4/4 RED)

---

## Task 8: Archive ownership — the write gate and the writer identity

**Files:**
- Modify: `crates/charpente/src/archive/mod.rs`
- Modify: `crates/charpente/tests/archive.rs`

**Interfaces:**
- Consumes: `Db::verifying()` (`db.rs:1708`, already public, already read at `repl/session.rs:744`), `Manifest::writer` (T4).
- Produces: no new public names.

**Read the precedent before writing code:** DESIGN §5's "What a `Verifying` node may do as a peer", and `docs/superpowers/specs/2026-08-06-verifying-peer-policy-design.md`. That sub-project found `begin_seeding` slipping past the write gate because it was read-only, and a node was manufacturing durable replicas of a timeline it had not confirmed it owned. **The archive arm is read-only against the `Db` in exactly the same way, and an object in a bucket outlives the node's fail-stop more thoroughly than a seeded follower does.** This task exists so the archive is not found the same way later.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn a_verifying_node_uploads_nothing() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 500);
    db.verify_writes(vec!["witness-a:9000".into()]);   // durable Primary, witnesses unresolved
    step_until_idle(&mut arc, &db);
    assert!(store.keys().is_empty(),
        "a verifying node manufactured durable copies of an unconfirmed timeline: {:?}",
        store.keys());
    assert!(!arc.gapped(), "gating is not a gap — it was never this node's timeline to ship");
}

#[test]
fn the_gate_opening_releases_the_archive() {
    let (mut db, store, mut arc) = archive_fixture();
    write_records(&mut db, 500);
    db.verify_writes(vec!["witness-a:9000".into()]);
    step_until_idle(&mut arc, &db);
    assert!(store.keys().is_empty());
    db.allow_writes();                          // witnesses resolved
    step_until_idle(&mut arc, &db);
    assert!(!store.keys().is_empty(), "the archive never resumed after the gate opened");
}

#[test]
fn a_foreign_manifest_advance_stops_this_writer_rather_than_interleaving() {
    let (mut db, store, mut arc) = archive_fixture();   // writer "node-a"
    write_records(&mut db, 500);
    step_until_idle(&mut arc, &db);
    let gen_before = read_manifest(&store).unwrap().generation;

    write_foreign_manifest(&store, "node-b", gen_before + 5);   // a promoted peer
    write_records(&mut db, 500);
    let e = step_until_error(&mut arc, &db);

    let t = e.to_string();
    assert!(t.contains("node-a") && t.contains("node-b"), "must name both identities: {t}");
    assert!(t.contains("archive-test-prefix"), "must name the prefix: {t}");
    assert_eq!(read_manifest(&store).unwrap().writer, "node-b",
        "this writer interleaved into a bucket another node owns");
}
```

- [ ] **Step 2: Run to verify failure**

- [ ] **Step 3: Implement**

At the top of `step`, return `Ok(())` immediately when `db.verifying()` — no
upload, no gap, no notice. Before each manifest swap, `Get` the current
manifest; if its `writer` differs from `cfg.writer` **and** its generation is
at or above this archive's, stop permanently (a latched `foreign_writer`
state) and return a named error. Detection, not prevention — the same honest
posture the promote path takes toward its bounded-loss window.

- [ ] **Step 4: Run to verify pass**

- [ ] **Step 5: Mutation table**

| # | mutation | must redden |
|---|---|---|
| 1 | delete the `db.verifying()` guard | `a_verifying_node_uploads_nothing` |
| 2 | make the guard permanent (never re-check) | `the_gate_opening_releases_the_archive` |
| 3 | skip the writer comparison before the swap | `a_foreign_manifest_advance_stops_this_writer_rather_than_interleaving` |
| 4 | set `gapped = true` in the verifying arm | `a_verifying_node_uploads_nothing` (last assertion) |

- [ ] **Step 6: Gate and commit** (named paths; 4/4 RED)

---

## Task 9: The fifth kernel arm, and `serve::archive`

**Files:**
- Modify: `crates/charpente/src/kernel/mod.rs:90` (`Ready`), `:258` (ready set), `:568` (arm)
- Modify: `crates/charpente/src/serve.rs`
- Modify: `crates/charpente/tests/archive.rs`

**Interfaces:**
- Consumes: `Archive` (T5–T8).
- Produces: `Kernel::with_archive(app, clock, archive)`; `serve::archive(app, store, cfg)`; `Ready::Archive`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn the_archive_arm_ticks_without_starving_requests() {
    let mut k = kernel_with_archive_and_requests();
    let served = run_kernel_steps(&mut k, 1_000);
    assert!(served.requests > 0, "the archive arm starved the request arm");
    assert!(served.archive_steps > 0, "the archive arm never ran");
}

#[test]
fn a_kernel_without_an_archive_never_gates_on_one() {
    let mut k = kernel_without_archive();
    let served = run_kernel_steps(&mut k, 100);
    assert_eq!(served.archive_steps, 0);
    assert!(served.requests > 0);
}
```

- [ ] **Step 2: Run to verify failure**

- [ ] **Step 3: Implement**

Add `Archive` to `Ready`. Mirror the `repl` field exactly: `archive:
Option<Archive>`, pushed into the ready set only when `Some` and the store is
idle or has a completion pending, handled in its own match arm calling
`step(&self.app.db())`. `serve::archive(app, store, cfg)` runs the kernel on
the current thread against an `EmptySource`, exactly as `serve::backup` does —
so an archive node and a hot standby compose.

- [ ] **Step 4: Run to verify pass**

- [ ] **Step 5: Mutation table**

| # | mutation | must redden |
|---|---|---|
| 1 | push `Ready::Archive` unconditionally | `a_kernel_without_an_archive_never_gates_on_one` |
| 2 | make the archive arm loop to completion inside one step | `the_archive_arm_ticks_without_starving_requests` |

- [ ] **Step 6: Gate and commit** (named paths; 2/2 RED)

---

## Task 10: Restore

**Files:**
- Create: `crates/charpente/src/archive/restore.rs`
- Modify: `crates/charpente/tests/archive.rs`

**Interfaces:**
- Consumes: `ObjectStore`, `Manifest`, `Seeder` (`db.rs:2381`), `Db::open`.
- Produces:

```rust
pub struct RestoreOpts { pub prefix: String, pub force: bool }
pub struct RestoreReport {
    pub generation: u64, pub applied_seq: u64,
    pub segments: usize, pub pending_jobs: u64,
}
/// Rebuild a Db's data directory from an archive. Core-testable: the
/// simulator drives this exact function, not an analogue.
pub fn restore(
    store: &dyn ObjectStore,
    disk: Box<dyn Disk>,
    schema: Schema,
    opts: RestoreOpts,
) -> io::Result<RestoreReport>;
```

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn restore_reproduces_the_source_at_the_archived_frontier() {
    let (src, store) = archived_db_with_rows(1_000);
    let dst = restore(&store, fresh_disk(), schema(), opts()).unwrap();
    assert_eq!(dst.applied_seq, archived_frontier(&store));
    assert_eq!(logical_rows(&reopen(dst)), logical_rows(&src_at(&src, dst.applied_seq)));
}

#[test]
fn restore_refuses_a_diverged_local_timeline_and_names_both_frontiers() {
    let (_src, store) = archived_db_with_rows(100);
    let disk = disk_holding_a_diverged_timeline();
    let e = restore(&store, disk, schema(), opts()).unwrap_err();
    let t = e.to_string();
    assert!(t.contains("diverged"), "{t}");
    assert!(t.contains("--force"), "must name the fix: {t}");
}

#[test]
fn force_discards_and_names_what_it_destroys_before_acting() {
    let (_src, store) = archived_db_with_rows(100);
    let disk = disk_holding_a_diverged_timeline();
    let report = restore(&store, disk, schema(), RestoreOpts { force: true, ..opts() }).unwrap();
    assert!(destructive_notice_emitted().contains("discard"));
    assert!(report.applied_seq > 0);
}

#[test]
fn a_corrupt_segment_object_is_refused_never_imported() {
    let (_src, store) = archived_db_with_rows(500);
    corrupt_one_segment_object(&store);
    let e = restore(&store, fresh_disk(), schema(), opts()).unwrap_err();
    assert!(e.to_string().contains("CRC"), "{e}");
    assert!(fresh_disk_is_empty(), "corruption was partially imported");
}

#[test]
fn a_missing_object_named_by_the_manifest_is_refused() {
    let (_src, store) = archived_db_with_rows(500);
    delete_one_snapshot_batch(&store);
    let e = restore(&store, fresh_disk(), schema(), opts()).unwrap_err();
    assert!(e.to_string().contains("missing"), "{e}");
}

#[test]
fn a_crashed_restore_converges_on_re_run() {
    let (_src, store) = archived_db_with_rows(500);
    let disk = fresh_disk();
    for cut in [3, 11, 29] {
        let _ = restore_crashing_after(&store, disk.clone(), cut);
    }
    let report = restore(&store, disk, schema(), opts()).unwrap();
    assert_eq!(report.applied_seq, archived_frontier(&store));
}

#[test]
fn the_report_names_the_jobs_restore_is_about_to_resurrect() {
    let (_src, store) = archived_db_with_pending_jobs(7);
    let report = restore(&store, fresh_disk(), schema(), opts()).unwrap();
    assert_eq!(report.pending_jobs, 7,
        "operators must be told restore can re-fire effects (spec §6)");
}
```

- [ ] **Step 2: Run to verify failure**

- [ ] **Step 3: Implement**

Follow spec §6's five steps exactly. Divergence check happens **before**
`Seeder::new` discards anything. Every downloaded segment's frames are
CRC-verified before being laid down. `pending_jobs` is counted off the
restored `charpente.jobs` rows.

- [ ] **Step 4: Run to verify pass**

- [ ] **Step 5: Mutation table**

| # | mutation | must redden |
|---|---|---|
| 1 | skip the divergence check | `restore_refuses_a_diverged_local_timeline_and_names_both_frontiers` |
| 2 | run the divergence check *after* `Seeder::new` | `force_discards_and_names_what_it_destroys_before_acting` (the un-forced test still passes; this one catches the ordering) |
| 3 | skip frame CRC verification on download | `a_corrupt_segment_object_is_refused_never_imported` |
| 4 | `pending_jobs: 0` constant | `the_report_names_the_jobs_restore_is_about_to_resurrect` |
| 5 | treat a missing object as an empty one | `a_missing_object_named_by_the_manifest_is_refused` |

- [ ] **Step 6: Gate and commit** (named paths; 5/5 RED)

---

## Task 11: `charpente restore`, and the filesystem `ObjectStore`

**Files:**
- Create: `crates/charpente-cli/src/restore.rs`
- Modify: `crates/charpente-cli/src/main.rs:5-18`
- Modify: `crates/charpente/src/prod.rs`
- Test: `crates/charpente/tests/archive.rs` (prod impl against the T1 contract tests)

**Interfaces:**
- Consumes: `restore()` (T10), `ObjectStore` (T1).
- Produces: `charpente::prod::FsObjectStore::new(root: PathBuf) -> FsObjectStore`; CLI verb `restore`.

**Coordination:** `charpente-cli` is shared with the concurrent TS-codegen sub-project, which adds `gen-client` and `check-client`. Different files, shared `main.rs` dispatch and `Cargo.toml` — **expect a small merge and say so in the commit.**

- [ ] **Step 1: Write the failing tests**

Run T1's four contract tests against `FsObjectStore` in a tempdir — the same
contract, the same assertions, a different implementation. Plus:

```rust
#[test]
fn fs_object_store_put_is_atomic_under_a_kill() {
    // write to <key>.tmp then rename; a rename is atomic on POSIX, so a
    // crash leaves the old object or none — never half of the new one.
    let s = FsObjectStore::new(tmp());
    put(&s, "a", &[1; 4096]);
    kill_between_write_and_rename(|| put(&s, "a", &[2; 4096]));
    assert_eq!(get(&s, "a"), vec![1; 4096], "a torn object escaped the rename");
}

#[test]
fn restore_cli_reports_pending_jobs_and_exits_zero() {
    let out = run_cli(&["restore", "--from", &bucket, "--to", &dir]);
    assert_eq!(out.code, 0);
    assert!(out.stdout.contains("pending job"), "operators must be warned: {}", out.stdout);
}

#[test]
fn restore_cli_refuses_a_dirty_target_without_force_and_names_it() {
    let out = run_cli(&["restore", "--from", &bucket, "--to", &dirty_dir]);
    assert_eq!(out.code, 1);
    assert!(out.stderr.contains("--force"));
}
```

- [ ] **Step 2: Run to verify failure**

- [ ] **Step 3: Implement**

`FsObjectStore`: `Put` writes `<root>/<key>.tmp` then `rename` — **the rename
is what makes the PUT atomic**, and it is the whole reason a filesystem can
honestly implement this trait. `List` walks and sorts. Completions are
immediate (`poll` returns `Some` on the first call); real async arrives with
the S3 client. CLI: parse `--from`/`--to`/`--force`, call `restore`, print the
report including the pending-job count, exit 0/1.

- [ ] **Step 4: Run to verify pass**

- [ ] **Step 5: Mutation table**

| # | mutation | must redden |
|---|---|---|
| 1 | write directly to `<key>` instead of tmp+rename | `fs_object_store_put_is_atomic_under_a_kill` |
| 2 | drop the pending-job line from CLI output | `restore_cli_reports_pending_jobs_and_exits_zero` |
| 3 | exit 0 on a refused restore | `restore_cli_refuses_a_dirty_target_without_force_and_names_it` |

- [ ] **Step 6: Gate and commit** (named paths; note the expected `main.rs` merge)

---

## Task 12: The convergence property

**Files:**
- Create: `crates/charpente-sim/tests/archive.rs`
- Modify: `crates/charpente-sim/src/checks.rs` (coverage keys)

**Interfaces:**
- Consumes: everything above.
- Produces: the flagship oracle.

**This is the task the sub-project exists for.** Everything before it is machinery; this is the evidence.

- [ ] **Step 1: Write the property**

```rust
run_universes!(archive_converges, |sim| {
    let mut db = open_db(sim);
    let mut arc = open_archive(sim);
    let mut written = 0u64;

    while sim.steps() < BUDGET {
        interleave(sim, &mut db, &mut arc, &mut written)?;   // writes, archive steps, faults
    }

    // The oracle: whatever the manifest names must restore to a Db logically
    // equal to the source at that frontier. Logical, not byte-identical —
    // 55f15ec retracted byte-identity to equal-grouping.
    if let Some(m) = read_manifest(&arc.store()) {
        let restored = restore(&arc.store(), sim.fresh_disk(), schema(), opts())?;
        require!("archive:restored", restored.applied_seq == m.applied_seq);
        assert_logically_equal(&reopen(restored), &db_at(&db, m.applied_seq));
    }

    // Negative space, every universe:
    require!("archive:never_poisons", !db.poisoned() || db_poisoned_by_something_else());
    Ok(())
});
```

Coverage `require:` keys: `archive:restored`, `archive:gap_closed_generation`,
`archive:lost_ack_retried`, `archive:diverged_refused`,
`archive:verifying_gated`, `archive:foreign_writer_stopped`.

- [ ] **Step 2: Run and confirm each coverage key actually fires**

Run: `cargo test -p charpente-sim --test archive -- --nocapture`
Expected: every `require:` key reports non-zero. **A key that never fires means
the fault axis is not reachable and the property is decorative** — that is the
sim trap this codebase has been bitten by before (LEDGER: "in this simulator,
not driving a node does not..."). Fix the universe generator, not the key.

- [ ] **Step 3: Run 256 fresh seeds**

Run: `cargo test -p charpente-sim --test archive` with a fresh master seed.
Record the master seed and the universe count in the commit message — CLAUDE.md's
evidence bar is "sim suite green over N fresh seeds", not "unit tests pass".

- [ ] **Step 4: Mutation the property itself**

| # | mutation | must redden |
|---|---|---|
| 1 | `assert_logically_equal` → `assert!(true)` | the property must fail on a seeded run |
| 2 | restore from generation-1 instead of the manifest's | `archive:restored` |
| 3 | skip the manifest read entirely (property never runs its oracle) | coverage key `archive:restored` reports zero |

Mutation 3 is the important one: **a property that silently never reaches its
oracle passes forever.**

- [ ] **Step 5: Gate and commit** (named paths; state seed count and master seed)

---

## Task 13: Docs

**Files:**
- Modify: `DESIGN.md:41-110` (§1 Shape block), `DESIGN.md:683-...` (§5 backup paragraph)
- Modify: `README.md` ("what is *not* built" list)
- Modify: `docs/GUIDE.md` (operator section)
- Modify: `PLAN.md:104-105` — **HELD**
- Modify: `CLAUDE.md:103` (hard rule 1 enumeration)

**Hard rule 6: this task is not optional and does not get deferred.**

- [ ] **Step 1: DESIGN §1** — add `ObjectStore` to the Shape code block. Do **not** reframe §1's Decision paragraph: object storage is *network*, a source already named, so nothing about the decision changes (spec §1).

- [ ] **Step 2: DESIGN §5** — rewrite the "Backup (M2, not built)" paragraph into what was built, keeping the `serve::backup` distinction explicit so the two words stay separated.

- [ ] **Step 3: README** — remove object-storage backup from the not-built list; keep `serve::backup`'s clarification.

- [ ] **Step 4: GUIDE** — operator section: archive config, `charpente restore`, the gap notice, and **the job-resurrection warning** (spec §6). Note `crates/guide-examples`'s build.rs compiles every fenced rust block; run `cargo build -p guide-examples`.

- [ ] **Step 5: CLAUDE.md:103** — hard rule 1 names four of `io.rs`'s seven traits. It is shorthand, not an exhaustive contract (spec §1), so this is optional tidying. **If it happens it happens here**, in the commit where the trait exists — never earlier.

- [ ] **Step 6: PLAN.md — CHECK BEFORE EDITING.** The concurrent TS-codegen sub-project is landing a structural split of the adjacent WebSockets/codegen bullet, and by agreement they go first. Confirm their edit has landed before touching the file; rebase onto it.

- [ ] **Step 7: Full gate and commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
cargo build -p guide-examples
git add DESIGN.md README.md docs/GUIDE.md PLAN.md CLAUDE.md
git commit -m "docs: the archive ships — DESIGN §1/§5, README, GUIDE, PLAN"
```

---

## Exit criteria (spec §11)

- [ ] `ObjectStore` in `charpente-core::io`; sim impl with all fault axes; `FsObjectStore` in prod.
- [ ] Archive ships sealed segments and seq-cadenced snapshots; bounded pin composes with replication's.
- [ ] `charpente restore` rebuilds a Db; refuses diverged timelines and every §7 corruption case by name.
- [ ] Convergence property green over N fresh seeds — **N and the master seed stated in the commit**.
- [ ] Every task's mutation table run, RED confirmed, result recorded in its commit message.
- [ ] `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` all green.
- [ ] Docs updated (Task 13).

## Open items carried from the spec (§14)

1. `archive_pin_segments` default — **this plan proposes 8** (Task 5), justified as 512 MiB of held WAL at the default 64 MiB `segment_bytes`, 4× `retain_segments`. Noah's to override.
2. Whether the capability-surface decision merits a numbered DECISIONS.md entry. Not written by this plan; DECISIONS.md is shared-spine.
