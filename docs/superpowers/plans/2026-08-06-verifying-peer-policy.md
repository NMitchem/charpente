# Verifying-Peer Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A node whose witness boot gate is unresolved must refuse to seed a brand-new follower, because a seed manufactures a durable replica of a timeline it has not confirmed it owns — and that replica outlives its own fail-stop.

**Architecture:** One check added to `decide_hello`, keyed on `verifying && position.is_none() && !witnesses.contains(listen)`. The refusal carries a marker fragment that makes it *retryable* at the receiver (both `Follower::step` refuse arms back off instead of dying) and `Inconclusive` at a witness probe. No wire change, no new durable state. A pre-existing GC-pin window that this change would make repeat indefinitely is closed first, as its own task.

**Tech Stack:** Rust, `charpente-core` `Disk`/`Net`/`Conn` traits (poll-shaped, non-blocking), `charpente-sim` for fault-injected properties.

**Spec:** `docs/superpowers/specs/2026-08-06-verifying-peer-policy-design.md`. Ruling (Noah, 2026-08-06): the refusal **never lapses** while the gate is shut.
**Prior art — read before Task 3:** `.superpowers/sdd/2026-07-31-dual-primary/PARKED-minor5-seed-refusal.diff` (the reverted first attempt) and `review-t5-fixes-report.md` C-1 (why it was reverted). This plan is not that diff with fixes; §2 of the spec relocates the check.

## Global Constraints

- **Hard rule 1 — no ambient nondeterminism.** All IO through `Disk`/`Net`/`Clock`/`Entropy`. Never block, never sleep.
- **Hard rule 3 — RED first.** Every test is observed failing against unfixed code before the fix lands, and the RED output is recorded in the ledger. **Exemption** (inherited from `5e6412e`): absence-asserting guards — tests whose assertion is "this does NOT happen" and which therefore pass on unfixed code by construction — are exempt and must instead be proven by **mutation** (make the guarded thing happen; the test must redden; restore byte-exact afterwards).
- **Hard rule 4 — assertions on negative space**, on in release builds.
- **Hard rule 5 — error messages are product surface.** Every new operator-visible string is pinned byte-exact by a test in the same task that introduces it.
- **Hard rule 6 — public-API changes update the docs in the same commit.**
- **All three gates green at EVERY commit:** `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`. If a task cannot be green alone, say so and fold it into its neighbour rather than committing red.
- **Capture cargo output with `tee`, never `tail`** — `tail` eats aggregate counts and replaces cargo's exit status with its own.
- **No wire change.** No new `Msg` variant, no proto bump, no new durable state.
- **Task order is load-bearing.** Task 2 (receiver knows how to retry) MUST land before Task 3 (sender starts refusing). Reversed, the suite goes red exactly as the parked attempt did — five property tests across two suites.
- **Seeds** continue the 0b block: `0x2EB1_011B` onward. Never reuse a seed.

## File Structure

| File | Responsibility |
|---|---|
| `crates/charpente/src/repl/session.rs` | `pin_floor` dead short-circuit (T1); the two new consts + the `decide_hello` check + the witness-set parameter (T2, T3) |
| `crates/charpente/src/repl/follower.rs` | Recognize the retryable marker at **both** refuse arms; new `#[cfg(test)] mod tests` (T2) |
| `crates/charpente/src/repl/probe.rs` | **Test only** — `classify`'s default arm already returns `Inconclusive` for any `Refuse` carrying neither marker (`probe.rs:355`). No production change. (T2) |
| `crates/charpente-core/src/db.rs` | `Db::verifying()` accessor (T3) |
| `crates/charpente/src/repl/mod.rs` | Pass `&self.witnesses` into `Session::service` (T3) |
| `crates/charpente/tests/replication.rs` | Sim properties (T1, T3) |
| `DESIGN.md` §5, `docs/GUIDE.md` §10 | Operator + design docs (T4) |

---

### Task 1: A dead session must not pin GC

**Files:**
- Modify: `crates/charpente/src/repl/session.rs:112-118` (`pin_floor`)
- Test: `crates/charpente/tests/replication.rs` (new test, seed `0x2EB1_011B`)

**Interfaces:**
- Consumes: nothing — this task is independent of the rest of the plan.
- Produces: no new API. `Session::pin_floor(&self) -> u64` keeps its signature.

**Why first:** this is a **pre-existing** bug on every refusal path (peer-secret, schema-digest, invalid-listen, the diverged family), not one this plan introduces. Task 3 makes it recur every `RECONNECT_NANOS` for as long as the gate stays shut, so it is cleanest to close before that lands.

**The mechanism:** `Repl::step` reaps dead sessions at the *top* of the step (`mod.rs:560`) but calls `recompute_pin()` immediately after `service` (`mod.rs:601-602`); `pump_io` (`mod.rs:394`) recomputes over the dead session too. A session refused during this step is therefore still in `self.sessions` with `cursor = 0, acked = None`, so `pin_floor()` returns 0 and pins GC at the log base until the next round's reap.

**Severity, stated honestly (measured in the first Task 1 attempt).** The window is **one step, recurring** — not permanent. The pre-existing top-of-step reap ends it on the very next round (observed `0 → 0 → 0 → MAX → MAX`), so each occurrence costs at most one checkpoint's GC, and only when a checkpoint lands inside the window. This is an **efficiency** fix — delayed GC, never wrong GC — and must not be written up as closing a correctness gap. It is worth taking because it is three lines in the function the hazard lives in, and because on a healthy primary any misconfigured peer retrying can skip checkpoint GC repeatedly.

- [ ] **Step 1: Write the failing test**

Add to `crates/charpente/tests/replication.rs`, near `a_silent_await_hello_session_stops_pinning_gc_after_its_deadline` (which is the model this mirrors):

```rust
/// Regression (0b re-review M-2): `Repl::step` reaps dead sessions at the top
/// of the step but recomputes the pin right after `service`, so a session
/// REFUSED during this step is still present with `cursor = 0, acked = None`
/// and pins GC at 0. Pre-existing on every refusal path; the seed-while-
/// verifying refusal (task 3) is the first that a peer retries forever,
/// turning a one-step window into a permanent one.
///
/// Uses the peer-secret refusal because it needs no new code — the point is
/// the pin, not which refusal caused it. Deliberately does NOT drive the
/// scheduler after the refusal: that isolates the single-step window, which
/// is exactly what a retrying peer re-creates every RECONNECT_NANOS.
#[test]
fn a_refused_session_does_not_pin_gc() {
    let sim = Sim::with_faults(0x2EB1_011B, FaultConfig::none());
    let db_cfg = DbConfig {
        wal: WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 128,
        },
        store_path: "tables.redb".to_string(),
        retain_segments: 1,
        checkpoint_every: 4,
        event_retention: 65_536,
        migration_batch_size: 1000,
    };
    let mut cfg = primary_cfg("p:1");
    cfg.peer_secret = Some("right".to_string());
    let mut p = spawn_node(&sim, "p", cfg, db_cfg).unwrap();

    // A peer that presents the wrong secret: refused, session marked dead,
    // still present when this step recomputes the pin. Raw-conn shape copied
    // from `a_long_listen_address_is_refused_and_truncated` (replication.rs
    // ~:4993), the file's idiom for driving a Hello by hand.
    let schema_digest = p.db.as_ref().unwrap().schema_digest();
    let conn = sim
        .host("wrong")
        .net()
        .connect("p:1")
        .expect("connect to the primary's listener");
    conn.send(&encode(&Msg::Hello {
        proto: PROTO_VERSION,
        listen: Some("wrong:1".to_string()),
        position: None,
        secret: Some("wrong".to_string()),
        schema_digest,
    }))
    .unwrap();

    // Drive ONE round at a time and stop the instant the refusal reaches the
    // client. At that moment the session was marked dead inside this step's
    // `service` and `recompute_pin` has just run with it still present — that
    // IS the window. Driving further lets `Repl::step`'s pre-existing
    // top-of-step `sessions.retain(|s| !s.dead)` reap it and restore the pin,
    // which is why NO fixed round count can observe this.
    let mut buf = vec![0u8; MAX_MESSAGE_BYTES];
    let mut refused = false;
    for _ in 0..64 {
        drive_rounds(&sim, &mut [&mut p], 1).unwrap();
        if let Ok(Some(n)) = conn.recv(&mut buf) {
            assert!(
                matches!(decode(&buf[..n]).unwrap(), Msg::Refuse { .. }),
                "expected the peer-secret refusal"
            );
            refused = true;
            break;
        }
    }
    assert!(refused, "precondition: the peer must have been refused");

    // Commit past several checkpoint boundaries WITHOUT driving the replicator
    // again. GC is a function of commits-since-checkpoint and `commit_widget`
    // calls `db.transact` directly (replication.rs:515-519) without stepping
    // the scheduler, so this checkpoints while the dead session is still
    // present and still holding the pin.
    for i in 0..16u64 {
        commit_widget(&sim, &mut p, i).unwrap();
    }
    assert!(
        p.db.as_ref().unwrap().log_base() > 0,
        "a refused (dead) session must not hold the retention pin; base stuck at {}",
        p.db.as_ref().unwrap().log_base()
    );
}
```

If a `encode_hello_with_secret` helper does not already exist in the file, write it beside the test using the same `Msg::Hello`/`encode` shape the file's other raw-conn tests use (grep for `Msg::Hello` in `replication.rs` for the exact field list — `proto`, `listen`, `position`, `secret`, `schema_digest`). Do not invent field names; copy them from an existing call site.

- [ ] **Step 2: Run it and verify it FAILS**

```bash
cargo test -p charpente --test replication a_refused_session_does_not_pin_gc 2>&1 | tee /tmp/t1-red.log
```

Expected: FAIL, `base stuck at 0`. **Record this output in the ledger** — this is the hard-rule-3 RED.

- [ ] **Step 3: Fix `pin_floor`**

In `crates/charpente/src/repl/session.rs`, replace the body of `pin_floor`:

```rust
    pub(crate) fn pin_floor(&self) -> u64 {
        // A control connection ships nothing and must never hold GC.
        if self.control {
            return u64::MAX;
        }
        // Nor does a dead one. `Repl::step` reaps at the top of the step but
        // calls `recompute_pin` right after `service` (and `pump_io` does its
        // own), so a session refused during THIS step is still in
        // `self.sessions` with `cursor = 0, acked = None`. Unguarded that pins
        // GC at 0 until the next round's reap — costing that window's
        // checkpoint its GC, once per refusal on every refusal path, and
        // recurring against a peer that keeps retrying (0b re-review M-2).
        // Bounded, not permanent: the reap is what ends it, measured
        // 0 -> 0 -> 0 -> MAX. Delayed GC, never wrong GC.
        if self.dead {
            return u64::MAX;
        }
        self.acked.map_or(self.cursor, |a| a + 1)
    }
```

- [ ] **Step 4: Run the test and the surrounding suite**

```bash
cargo test -p charpente --test replication a_refused_session_does_not_pin_gc 2>&1 | tee /tmp/t1-green.log
cargo test -p charpente --test replication 2>&1 | tee /tmp/t1-suite.log
```

Expected: the new test PASSES; the replication suite is fully green (it is slow — roughly 25 minutes). Pay particular attention to `a_silent_await_hello_session_stops_pinning_gc_after_its_deadline` and `live_lagging_session_pins_gc`, the two existing pin tests.

- [ ] **Step 5: Gates, then commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace 2>&1 | tee /tmp/t1-workspace.log
git add crates/charpente/src/repl/session.rs crates/charpente/tests/replication.rs
git commit -m "repl: a dead session must not hold the retention pin"
```

`cargo test --workspace` is required before the commit by Global Constraints — `pin_floor` is shared by every session, so the blast radius is not just the replication suite. It subsumes Step 4's targeted run; Step 4 exists to get a fast signal first.

---

### Task 2: The retryable refusal, receiver side

**Files:**
- Modify: `crates/charpente/src/repl/session.rs` (add two consts near `NOT_A_PRIMARY_MARKER` at `:524`)
- Modify: `crates/charpente/src/repl/follower.rs:228-234` and `:295`
- Create: `#[cfg(test)] mod tests` at the end of `crates/charpente/src/repl/follower.rs`
- Test: `crates/charpente/src/repl/probe.rs` (extend the existing `mod tests`)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces, for Task 3:
  - `pub(crate) const VERIFYING_WITNESSES_MARKER: &str` — the keyed fragment.
  - `pub(crate) const SEED_WHILE_VERIFYING_REFUSAL: &str` — the full operator string, which contains the marker.

**Nothing emits this refusal yet.** That is the point of the ordering: this task is a no-op in production and green by construction, and Task 3 lands the behaviour into a receiver that already handles it.

- [ ] **Step 1: Add the two consts**

In `crates/charpente/src/repl/session.rs`, immediately after `NOT_A_PRIMARY_MARKER` (`:524`):

```rust
/// The keyed fragment of the seed-while-verifying refusal (M2 interlude,
/// spec §3) — same producer/consumer discipline as `NOT_A_PRIMARY_MARKER`.
/// Consumers: `follower.rs`'s BOTH refuse arms (back off and retry rather
/// than dying) and, by omission, `probe.rs::classify`, whose default arm
/// files any refusal carrying neither marker as `Inconclusive`. Rewording
/// this fragment deliberately reddens those consumers' tests.
pub(crate) const VERIFYING_WITNESSES_MARKER: &str = "still verifying its witnesses";

/// Refusing to seed a brand-new follower while the witness boot gate is
/// unresolved (M2 interlude, spec §1): pinned byte-exact by
/// `a_verifying_node_refuses_to_seed_a_brand_new_follower`.
///
/// Deliberately carries NEITHER `NOT_A_PRIMARY_MARKER` nor `DIVERGED_LABEL`.
/// A witness probe reading this as `NotPrimary` would treat "I am also
/// unsure" as "I am satisfied" and open its own gate on no evidence; reading
/// it as `IsPrimary` would refuse a healthy node over a phantom split-brain.
/// `probe.rs::classify`'s default arm gives the correct `Inconclusive` for
/// free — the absence of both markers is what earns it, so it is asserted.
pub(crate) const SEED_WHILE_VERIFYING_REFUSAL: &str = "cannot seed a new follower yet: this \
     node restarted as a primary and is still verifying its witnesses — it has not confirmed \
     that none of the followers it had shipped to was promoted while it was down, so seeding \
     would copy a timeline it does not yet know it owns. This clears on its own once every \
     witness answers, and an existing follower's catch-up is unaffected meanwhile. If the \
     witnesses are permanently gone, see GUIDE §10 — note that clearing the witness file \
     re-opens the dual-primary gap it exists to close.";
```

- [ ] **Step 2: Write the failing follower tests**

Append to `crates/charpente/src/repl/follower.rs`. Mirror `probe.rs`'s `mod tests` fixtures for the sim/`Db` setup — read `probe.rs:369-395` and copy its `fresh_db` helper rather than inventing one.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::repl::session::{SEED_WHILE_VERIFYING_REFUSAL, VERIFYING_WITNESSES_MARKER};
    use crate::{DbConfig, Schema};
    use charpente_sim::{FaultConfig, Sim};

    fn fresh_db(seed: u64) -> Db {
        let sim = Sim::with_faults(seed, FaultConfig::none());
        Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new())
            .expect("Db::open on a fresh sim disk")
    }

    /// The marker must actually be in the string it keys. Both consumers
    /// match on the fragment, so a reworded refusal that dropped it would
    /// become node-fatal again in silence.
    #[test]
    fn the_refusal_carries_its_marker() {
        assert!(SEED_WHILE_VERIFYING_REFUSAL.contains(VERIFYING_WITNESSES_MARKER));
    }

    /// Negative space (hard rule 4): carrying either OTHER marker would
    /// mis-route this refusal at a witness probe — `NotPrimary` opens a gate
    /// on no evidence, `IsPrimary` invents a split-brain.
    #[test]
    fn the_refusal_carries_neither_other_marker() {
        use crate::repl::session::{DIVERGED_LABEL, NOT_A_PRIMARY_MARKER};
        assert!(!SEED_WHILE_VERIFYING_REFUSAL.contains(NOT_A_PRIMARY_MARKER));
        assert!(!SEED_WHILE_VERIFYING_REFUSAL.contains(DIVERGED_LABEL));
    }

    /// The ordinary ship arm (`follower.rs:295`): a retryable refusal backs
    /// off, it does not kill the node.
    #[test]
    fn a_retryable_refusal_backs_off_on_the_ship_arm() {
        let sim = Sim::with_faults(0x2EB1_011C, FaultConfig::none());
        let mut db = fresh_db(0x2EB1_011C);
        let net = sim.host("f").net();
        let mut f = Follower::new("p:1".to_string(), Some("f:1".to_string()), None);
        f.inbox.push_back(encode(&Msg::Refuse {
            message: SEED_WHILE_VERIFYING_REFUSAL.to_string(),
        }));
        f.step(&*net, &mut db, 0)
            .expect("a retryable refusal must not be a node-fatal error");
        assert!(
            f.reconnect_at.is_some(),
            "it must have backed off for a retry"
        );
    }

    /// The resolving arm (`follower.rs:233`) is separately fatal — the parked
    /// attempt's probe patched only one of the two sites.
    #[test]
    fn a_retryable_refusal_backs_off_on_the_resolving_arm() {
        let sim = Sim::with_faults(0x2EB1_011C, FaultConfig::none());
        let mut db = fresh_db(0x2EB1_011C);
        let net = sim.host("f").net();
        let mut f = Follower::new_resolving("p:1".to_string(), Some("f:1".to_string()), None);
        f.inbox.push_back(encode(&Msg::Refuse {
            message: SEED_WHILE_VERIFYING_REFUSAL.to_string(),
        }));
        f.step(&*net, &mut db, 0)
            .expect("a retryable refusal must not be a node-fatal error");
        assert!(f.reconnect_at.is_some(), "it must have backed off");
        assert!(!f.resolved, "and must NOT have resolved the handoff");
    }

    /// Negative control, both arms: an unmarked refusal stays fatal. Without
    /// this the marker check could be widened to all refusals and still pass.
    #[test]
    fn an_unmarked_refusal_is_still_fatal() {
        let sim = Sim::with_faults(0x2EB1_011C, FaultConfig::none());
        let net = sim.host("f").net();
        for resolving in [false, true] {
            let mut db = fresh_db(0x2EB1_011C);
            let mut f = if resolving {
                Follower::new_resolving("p:1".to_string(), None, None)
            } else {
                Follower::new("p:1".to_string(), None, None)
            };
            f.inbox.push_back(encode(&Msg::Refuse {
                message: "some other refusal".to_string(),
            }));
            f.step(&*net, &mut db, 0)
                .expect_err("an unmarked refusal must remain node-fatal");
        }
    }
}
```

If `DIVERGED_LABEL` or `NOT_A_PRIMARY_MARKER` are not visible from `follower.rs`'s test module, widen their `pub(crate)` visibility rather than duplicating the literals.

- [ ] **Step 3: Run and verify they FAIL**

```bash
cargo test -p charpente --lib repl::follower 2>&1 | tee /tmp/t2-red.log
```

Expected: the two back-off tests FAIL (`expect` panics — the refusal is currently node-fatal). The marker/negative-control tests pass on arrival; that is fine, they are guards. **Record the RED in the ledger.**

- [ ] **Step 4: Make both arms retryable**

In `crates/charpente/src/repl/follower.rs`, the resolving arm at `:228-234` — add a second arm directly below the `NOT_A_PRIMARY_MARKER` one:

```rust
                Msg::Refuse { message } if message.contains(NOT_A_PRIMARY_MARKER) => {
                    self.disconnect(now);
                    return Ok(());
                }
                // `to` is a primary but has not finished its own witness boot
                // gate (M2 interlude): it cannot seed us yet, and it says so
                // in a form that resolves on its own. Back off and retry —
                // node-fatal here would kill a follower for asking too early.
                Msg::Refuse { message } if message.contains(VERIFYING_WITNESSES_MARKER) => {
                    self.disconnect(now);
                    return Ok(());
                }
                // A diverged/other refusal is the operator's re-seed signal.
                Msg::Refuse { message } => return Err(io::Error::other(message.clone())),
```

And the ordinary ship arm at `:295`:

```rust
            // The primary's witness boot gate is unresolved (M2 interlude):
            // retryable, not fatal — same fragment, same reasoning as the
            // resolving arm above.
            Msg::Refuse { message } if message.contains(VERIFYING_WITNESSES_MARKER) => {
                self.disconnect(now);
                Ok(())
            }
            Msg::Refuse { message } => Err(io::Error::other(message)),
```

Add `VERIFYING_WITNESSES_MARKER` to the existing `use crate::repl::session::NOT_A_PRIMARY_MARKER;` import at `:21`.

- [ ] **Step 5: Add the probe classifier test**

In `crates/charpente/src/repl/probe.rs`'s existing `mod tests`, extend the classifier test that already covers `PEER_SECRET_REFUSAL` (near `:842`):

```rust
        // The seed-while-verifying refusal (M2 interlude) must land on
        // classify's default arm. Load-bearing in BOTH directions: reading it
        // as NotPrimary would let a probe treat "I am also unsure" as "I am
        // satisfied" and open its gate on no evidence; IsPrimary would refuse
        // a healthy node over a phantom split-brain. No production change is
        // needed for this — the default arm already does it — so this test IS
        // the contract.
        let reply = Msg::Refuse {
            message: SEED_WHILE_VERIFYING_REFUSAL.to_string(),
        };
        match classify(&reply) {
            WitnessVerdict::Inconclusive(_) => {}
            other => panic!("expected Inconclusive, got {other:?}"),
        }
```

Add `SEED_WHILE_VERIFYING_REFUSAL` to that module's `use crate::repl::session::{PEER_SECRET_REFUSAL, Session};` import.

- [ ] **Step 6: Prove the classifier test's teeth by mutation**

It passes on arrival (`classify`'s default arm already returns `Inconclusive`), so per hard rule 3's exemption it needs a mutation proof. Temporarily add `SEED_WHILE_VERIFYING_REFUSAL`'s marker to `classify`'s `NOT_A_PRIMARY_MARKER` arm, confirm the test reddens, then **restore `probe.rs` byte-exact** (`git diff --stat` must show it unchanged).

- [ ] **Step 7: Run everything and commit**

```bash
cargo test -p charpente --lib repl 2>&1 | tee /tmp/t2-green.log
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace 2>&1 | tee /tmp/t2-workspace.log
git add crates/charpente/src/repl/session.rs crates/charpente/src/repl/follower.rs crates/charpente/src/repl/probe.rs
git commit -m "repl: a verifying primary's refusal is retryable, not node-fatal"
```

Expected: fully green. Nothing emits the refusal yet, so no behaviour changed.

---

### Task 3: A verifying node refuses to seed a newcomer

**Files:**
- Modify: `crates/charpente-core/src/db.rs` (add `verifying()` beside `writes_allowed` at `:1616`)
- Modify: `crates/charpente/src/repl/session.rs` (`decide_hello` at `:611`, `service` at `:191`, `handle` at `:258`)
- Modify: `crates/charpente/src/repl/mod.rs:580` (the `service` call site)
- Test: `crates/charpente/tests/replication.rs` (seeds `0x2EB1_011D`–`0x2EB1_0120`)

**Interfaces:**
- Consumes: `SEED_WHILE_VERIFYING_REFUSAL` and the retryable receiver from Task 2.
- Produces: `pub fn Db::verifying(&self) -> bool`; `Session::service`, `Session::handle` and `decide_hello` each gain exactly one trailing `witnesses: &[String]` parameter. `decide_hello` already takes `db: &Db`, so it reads the gate state via `db.verifying()` — do **not** also thread a `verifying: bool`. This keeps `decide_hello` at nine parameters, as spec §2 states.

- [ ] **Step 1: Add `Db::verifying()`**

In `crates/charpente-core/src/db.rs`, immediately after `writes_allowed` (`:1616-1618`):

```rust
    /// True while the witness boot gate is unresolved (M2 sub-project 0b):
    /// this node believes it is the primary but has not confirmed that none
    /// of the followers it shipped to was promoted while it was down.
    ///
    /// Distinct from `!writes_allowed()`, which is also true on an ordinary
    /// follower. Callers need the distinction when the question is "may I act
    /// on a timeline I have not confirmed I own" rather than "may I write" —
    /// `decide_hello` refuses to seed a newcomer in this state.
    pub fn verifying(&self) -> bool {
        matches!(self.gate, WriteGate::Verifying(_))
    }
```

- [ ] **Step 2: Write the failing headline test**

Add to `crates/charpente/tests/replication.rs`, after the `0x2EB1_011A` tests. Note the seed-constant block near `:5279` — add the new constants there in the file's existing style.

```rust
/// M2 interlude, spec §1: a gated node must not SEED a brand-new follower.
/// `begin_seeding` is read-only so the write gate does not stop it, but a
/// seed manufactures a durable converged replica of a timeline this node has
/// not confirmed it owns — and that replica OUTLIVES this node's own
/// fail-stop when a witness finally answers "primary".
///
/// Rigged so `log_base() == 0`, which makes `decide_hello` answer
/// `Frames { from: 0 }` — a full replay that is functionally a seed. A
/// refusal keyed on the `Decision::Snapshot` VARIANT is green here and seeds
/// anyway; keying on `position.is_none()` inside the None-position arm is
/// what makes both sub-arms impossible to miss (spec §2).
#[test]
fn a_verifying_node_refuses_to_seed_a_brand_new_follower() {
    let sim = Sim::with_faults(SEED_WHILE_VERIFYING_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    ship_then_crash_the_primary(&sim, &mut p, &mut f);
    f.host.crash_now(); // the witness is gone, so the gate cannot resolve
    recover_node(&sim, &mut p).unwrap();
    assert!(
        p.db.as_ref().unwrap().verifying(),
        "precondition: p restarted gated on its recorded witness"
    );
    assert_eq!(
        p.db.as_ref().unwrap().log_base(),
        0,
        "precondition: nothing GC'd, so a None-position peer takes the Frames{{from:0}} arm"
    );

    // A brand-new node, holding nothing, asks to join.
    let mut g = spawn_node(&sim, "g", follower_cfg("g:1", "p:1"), DbConfig::default()).unwrap();
    drive_rounds(&sim, &mut [&mut p, &mut g], 200).unwrap();

    // Assert on the GATE OUTCOME, not on an error shape: the newcomer holds
    // nothing and no seed happened. (The parked attempt asserted a shape the
    // correct fix removes, and took 771s to reach its budget wall.)
    assert_eq!(total_seeds(&g), 0, "no seed may have happened");
    assert_eq!(
        g.db.as_ref().unwrap().last_applied(),
        None,
        "the newcomer must hold nothing — a Frames{{from:0}} replay would have filled it"
    );
    // The bound this buys: a refused newcomer never enters the witness set,
    // because `listen` is assigned only on the two shipping arms.
    assert_eq!(
        load_witnesses(&p.host.disk()).unwrap(),
        vec!["f:1".to_string()],
        "a refused newcomer must not be recorded as a witness"
    );
    // And it is still alive to retry (task 2) rather than dead.
    assert!(
        g.db.is_some(),
        "the refused newcomer must have backed off, not died"
    );
}
```

Add the seed constants beside the existing block:

```rust
const SEED_WHILE_VERIFYING_SEED: u64 = 0x2EB1_011D;
const WITNESS_RESEED_SEED: u64 = 0x2EB1_011E;
const NEWCOMER_RETRIES_SEED: u64 = 0x2EB1_011F;
const POSITIONED_PEER_SEED: u64 = 0x2EB1_0120;
```

- [ ] **Step 3: Run it and verify it FAILS**

```bash
cargo test -p charpente --test replication a_verifying_node_refuses_to_seed_a_brand_new_follower 2>&1 | tee /tmp/t3-red.log
```

Expected: FAIL — the newcomer converges (`total_seeds` or `last_applied` non-zero). **Record the RED.**

- [ ] **Step 4: Thread the witness set and add the check**

`crates/charpente/src/repl/mod.rs:580` — pass the set:

```rust
                self.sessions[idx].service(db, now, &role, &self.peer_secret, &self.witnesses)?;
```

`crates/charpente/src/repl/session.rs` — `service` (`:191`) and `handle` (`:258`) each gain a trailing parameter, forwarded to `decide_hello`:

```rust
    pub(crate) fn service(
        &mut self,
        db: &Db,
        now: u64,
        role: &Role,
        peer_secret: &Option<String>,
        witnesses: &[String],
    ) -> io::Result<()> {
```

`decide_hello` (`:611`) gains one trailing `witnesses: &[String]` parameter — it already has `db`, so the gate state comes from `db.verifying()` — and the check goes **inside the existing `None`-position arm** at `:653-659`, replacing it:

```rust
    let last = db.last_applied();
    let Some((pos_seq, pos_crc)) = position else {
        // The peer holds NOTHING and is asking for a whole copy. If this
        // node's witness boot gate is unresolved and the peer is not one of
        // the witnesses we recorded, refuse (M2 interlude, spec §1): a seed
        // manufactures a fresh durable replica of a timeline we have not
        // confirmed we own, and it outlives our own fail-stop.
        //
        // Both conjuncts are load-bearing. `position.is_none()` alone is NOT
        // "brand-new node" — `hello_position` returns None for any empty-log
        // node, including a RECORDED WITNESS returning with a rebuilt store,
        // which is GUIDE §10's own prescribed remedy and what resolves this
        // gate. Membership alone is not enough either: a positioned peer
        // absent from the set is a genuine follower on this timeline, and
        // catch-up must still ship to it.
        //
        // Sited here, inside the None-position arm, so it covers BOTH the
        // `Frames { from: 0 }` and `Snapshot` answers below. Keying on the
        // `Decision::Snapshot` variant instead would be silently inert on
        // exactly a young primary that has not GC'd anything yet.
        if db.verifying()
            && !listen
                .as_deref()
                .is_some_and(|a| witnesses.iter().any(|w| w == a))
        {
            return Ok(Decision::Refuse(SEED_WHILE_VERIFYING_REFUSAL.to_string()));
        }
        return Ok(if db.log_base() == 0 {
            Decision::Frames { from: 0 }
        } else {
            Decision::Snapshot
        });
    };
```

An absent `listen` counts as **not** a witness (spec §2): `is_some_and` returns `false`, so the refusal fires. That is deliberate — an unidentifiable peer is treated as a newcomer.

Note the borrow at the `mod.rs` call site: `&mut self.sessions[idx]` alongside `&self.witnesses` are disjoint fields, exactly as `&self.peer_secret` already is, so this needs no restructuring. `record_witnesses(&mut self)` runs after `service` returns, so there is no overlap there either.

- [ ] **Step 5: Run the headline test and the full blast radius**

```bash
cargo test -p charpente --test replication a_verifying_node_refuses_to_seed_a_brand_new_follower 2>&1 | tee /tmp/t3-green.log
cargo test -p charpente --test replication 2>&1 | tee /tmp/t3-repl.log
cargo test -p charpente --test auth 2>&1 | tee /tmp/t3-auth.log
cargo test -p charpente --test realtime 2>&1 | tee /tmp/t3-realtime.log
```

**This is the acceptance bar for this task.** The parked attempt broke exactly these, and all five must be green:

| Suite | Test | Repro |
|---|---|---|
| replication | `the_kernel_completes_its_skipped_startup_work_when_the_witness_gate_opens` | deterministic |
| replication | `follower_converges_to_the_primary_fingerprint` | `CHARPENTE_SEED=0xcea9328519df4234` |
| replication | `failover_preserves_everything_the_follower_acked` | `CHARPENTE_SEED=0xa84f22d1913dc2d1` |
| replication | `switchover_mid_write_load_loses_nothing_and_serves_reads_throughout` | — |
| auth | `raw_credentials_never_touch_the_durable_store` | `CHARPENTE_SEED=0xde584e99fba70883` |

If any is red, **stop and report** — do not widen the refusal's exemptions to make a test pass. The keying is the design; a red here means the design is wrong, not the test.

- [ ] **Step 6: Add the three companion tests**

```rust
/// The other half of the keying (spec §2): a RECORDED WITNESS returning with
/// a rebuilt store Hellos with `position: None` — indistinguishable from a
/// newcomer on position alone. It must still be seeded: that is GUIDE §10's
/// prescribed remedy, and it is what resolves this node's own gate. This is
/// the case the parked attempt broke.
///
/// Construction copied from
/// `the_kernel_completes_its_skipped_startup_work_when_the_witness_gate_opens`
/// (~replication.rs:5885), which is the file's proven way to build "a node
/// whose address is a recorded witness but whose data dir is empty": seed the
/// witness set by hand on p's disk, then spawn that host fresh. Do NOT try to
/// get there with `recover_node`, which reopens the SAME data dir and so
/// yields a positioned peer — the opposite of what this test needs.
#[test]
fn a_verifying_node_still_seeds_a_recorded_witness() {
    let sim = Sim::with_faults(WITNESS_RESEED_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    ship_then_crash_the_primary(&sim, &mut p, &mut f);
    // Record a SECOND witness that has never held data, then bring p back.
    store_witnesses(&p.host.disk(), &["f:1".to_string(), "w:1".to_string()]).unwrap();
    recover_node(&sim, &mut p).unwrap();
    assert!(p.db.as_ref().unwrap().verifying(), "precondition: gated");

    // w is a recorded witness with an empty data dir: position None, and it
    // must be seeded anyway.
    let mut w = spawn_node(&sim, "w", follower_cfg("w:1", "p:1"), DbConfig::default()).unwrap();
    run_until(&sim, &mut [&mut p, &mut w], converged, CONVERGE_BUDGET)
        .expect("a recorded witness must still be seeded while the gate is unresolved");
    assert!(
        total_seeds(&w) >= 1,
        "it must have been seeded, not merely left empty"
    );
}

/// Task 2's retryability, end to end: the refused newcomer does not die, and
/// converges once the gate opens. `recover_node` is correct here — f comes
/// back on its OWN data dir as the positioned follower it always was, which
/// is what answers the probe and opens the gate.
#[test]
fn a_refused_newcomer_retries_and_converges_once_the_gate_opens() {
    let sim = Sim::with_faults(NEWCOMER_RETRIES_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    ship_then_crash_the_primary(&sim, &mut p, &mut f);
    recover_node(&sim, &mut p).unwrap();

    let mut g = spawn_node(&sim, "g", follower_cfg("g:1", "p:1"), DbConfig::default()).unwrap();
    drive_rounds(&sim, &mut [&mut p, &mut g], 200).unwrap();
    assert_eq!(total_seeds(&g), 0, "refused while the gate is shut");

    // f answers the probe, opening the gate.
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert!(
        p.db.as_ref().unwrap().writes_allowed(),
        "the gate must have opened once the witness answered"
    );
    run_until(&sim, &mut [&mut p, &mut g], converged, CONVERGE_BUDGET)
        .expect("the newcomer must converge once the gate is open");
    assert!(total_seeds(&g) >= 1, "and must have been seeded on the retry");
}

/// Negative space (hard rule 4): a POSITIONED peer is unaffected while
/// verifying. Catch-up ships a timeline the peer already holds a prefix of;
/// only a seed creates a new copy. Without this, the refusal could be widened
/// to all peers and every other test here would still pass.
///
/// Two followers on purpose. With one, that follower's own probe answer opens
/// the gate, so the catch-up would ship AFTER verification and prove nothing.
/// Here `f` is permanently gone (gate can never resolve) while `h` is behind
/// and positioned — so any frame `h` receives was shipped while verifying.
/// Does NOT reuse `ship_then_crash_the_primary`, which hard-asserts a single
/// witness.
#[test]
fn a_verifying_node_still_ships_catch_up_to_a_positioned_peer() {
    let sim = Sim::with_faults(POSITIONED_PEER_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    let mut h = spawn_node(&sim, "h", follower_cfg("h:1", "p:1"), DbConfig::default()).unwrap();

    // h converges on the first five records, then stops being driven.
    for i in 0..5u64 {
        commit_widget(&sim, &mut p, i).unwrap();
        drive_rounds(&sim, &mut [&mut p, &mut f, &mut h], 3).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut h], converged, CONVERGE_BUDGET).unwrap();
    let h_before = h.db.as_ref().unwrap().last_applied();

    // Five more that only f sees, so h is genuinely behind.
    for i in 5..10u64 {
        commit_widget(&sim, &mut p, i).unwrap();
        drive_rounds(&sim, &mut [&mut p, &mut f], 3).unwrap();
    }
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_eq!(
        load_witnesses(&p.host.disk()).unwrap(),
        vec!["f:1".to_string(), "h:1".to_string()],
        "sanity: both followers are recorded witnesses"
    );
    p.host.crash_now();
    f.host.crash_now(); // permanently gone: the gate can never resolve
    recover_node(&sim, &mut p).unwrap();
    assert!(p.db.as_ref().unwrap().verifying(), "precondition: gated on f:1");

    run_until(&sim, &mut [&mut p, &mut h], converged, CONVERGE_BUDGET)
        .expect("a positioned peer must still catch up while the gate is unresolved");
    assert!(
        h.db.as_ref().unwrap().last_applied() > h_before,
        "h must have actually advanced, not merely been already-converged"
    );
    assert!(
        p.db.as_ref().unwrap().verifying(),
        "and the gate must still be shut — proving the ship happened WHILE verifying"
    );
}
```

If `load_witnesses` returns the two addresses in a different order than written above, match the observed order rather than sorting — the witness file's order is part of its format, and this assertion is incidental to the test's purpose.

- [ ] **Step 7: Pin the refusal string byte-exact**

Hard rule 5. The tests above assert on the *gate outcome*, which is right for the property but leaves the operator-facing string unpinned. Drive the Hello by hand and read the reply, mirroring `a_long_listen_address_is_refused_and_truncated` (~`replication.rs:4993`):

```rust
/// Hard rule 5: the refusal an operator actually reads, pinned byte-exact.
/// The property tests above assert on the gate, which is the right oracle for
/// the guarantee but would stay green through any reword of this string.
#[test]
fn the_seed_while_verifying_refusal_is_pinned() {
    let sim = Sim::with_faults(REFUSAL_PIN_SEED, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    ship_then_crash_the_primary(&sim, &mut p, &mut f);
    f.host.crash_now();
    recover_node(&sim, &mut p).unwrap();
    assert!(p.db.as_ref().unwrap().verifying(), "precondition: gated");

    let schema_digest = p.db.as_ref().unwrap().schema_digest();
    let conn = sim
        .host("g")
        .net()
        .connect("p:1")
        .expect("connect to the primary's listener");
    conn.send(&encode(&Msg::Hello {
        proto: PROTO_VERSION,
        listen: Some("g:1".to_string()),
        position: None,
        secret: None,
        schema_digest,
    }))
    .unwrap();

    let mut buf = vec![0u8; MAX_MESSAGE_BYTES];
    let mut reply = None;
    for _ in 0..64 {
        drive_rounds(&sim, &mut [&mut p], 1).unwrap();
        match conn.recv(&mut buf) {
            Ok(Some(n)) => {
                reply = Some(decode(&buf[..n]).unwrap());
                break;
            }
            Ok(None) => {}
            Err(e) => panic!("conn recv failed: {e}"),
        }
    }
    let Some(Msg::Refuse { message }) = reply else {
        panic!("expected a Refuse reply, got {reply:?}");
    };
    assert_eq!(
        message,
        "cannot seed a new follower yet: this node restarted as a primary and is still \
         verifying its witnesses — it has not confirmed that none of the followers it had \
         shipped to was promoted while it was down, so seeding would copy a timeline it does \
         not yet know it owns. This clears on its own once every witness answers, and an \
         existing follower's catch-up is unaffected meanwhile. If the witnesses are \
         permanently gone, see GUIDE §10 — note that clearing the witness file re-opens the \
         dual-primary gap it exists to close.",
        "the refusal's exact shape is product surface (hard rule 5)"
    );
}
```

Add `const REFUSAL_PIN_SEED: u64 = 0x2EB1_0121;` to the seed block. If the literal above and `SEED_WHILE_VERIFYING_REFUSAL` disagree after `cargo fmt`, the **const** is authoritative — copy its exact bytes into the test, and update this plan's Task 2 Step 1 text to match rather than editing the const to match the test.

- [ ] **Step 8: Gates, full workspace, commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace 2>&1 | tee /tmp/t3-workspace.log
git add crates/charpente-core/src/db.rs crates/charpente/src/repl/session.rs crates/charpente/src/repl/mod.rs crates/charpente/tests/replication.rs
git commit -m "repl: a verifying node refuses to seed a newcomer"
```

---

### Task 4: Docs and the evidence sweep

**Files:**
- Modify: `DESIGN.md` §5, `docs/GUIDE.md` §10
- Modify: `CLAUDE.md` ("Current state" close-out entry)

**Interfaces:** consumes everything above; produces the record.

- [ ] **Step 1: DESIGN §5**

State the policy (spec §1) and the keying (spec §2) — what a `Verifying` node may do as a peer, and why catch-up and seeding differ. Include spec §5's note that recording witnesses while verifying is unchanged and safety-monotone (`probes` is built once at `Repl::open`, so it can only make the *next* boot's gate stricter).

- [ ] **Step 2: GUIDE §10**

Add the newcomer case beside the three existing probe outcomes: while a node is verifying, an existing follower catches up normally and a returning recorded witness re-seeds normally, but a brand-new follower is refused until verification finishes. Under a permanently dead witness that means the existing escape hatch — editing or deleting the `witnesses` file, with its stated gap-reopening warning — is the only way forward. Quote the refusal from its pinning test, never retyped.

- [ ] **Step 3: Verify the guide gate**

```bash
cargo build -p guide-examples 2>&1 | tee /tmp/t4-guide.log
```

- [ ] **Step 4: The 11-stage evidence sweep (spec §9)**

Run every stage; capture with `tee`. Replication, auth and realtime are all in the measured blast radius, so nothing here is optional.

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build -p guide-examples
cargo test --workspace 2>&1 | tee /tmp/sweep-workspace-1.log
cargo test --workspace 2>&1 | tee /tmp/sweep-workspace-2.log
CHARPENTE_SEED=0x8bee1752811ae873 cargo test -p charpente-sim --test crash_recovery 2>&1 | tee /tmp/sweep-pin1.log
CHARPENTE_SEED=0x348270b38f166529 cargo test -p charpente --test kernel 2>&1 | tee /tmp/sweep-pin2.log
CHARPENTE_SEED=0x8bee1752811ae873 cargo test --release -p charpente-sim --test crash_recovery 2>&1 | tee /tmp/sweep-rel1.log
CHARPENTE_SEED=0xff159ed76d2806e4 cargo test --release -p charpente --test replication late_follower_seeds_below_the_gcd_base_and_converges 2>&1 | tee /tmp/sweep-rel2.log
cargo run -p charpente-cli -- test --sim --dir examples/invoicing --universes 1024 2>&1 | tee /tmp/sweep-invoicing.log
./scripts/edge-harness.sh 2>&1 | tee /tmp/sweep-edge.log
```

Both `cargo test --workspace` runs must report **identical** counts. Record the invoicing master seed. Confirm the release tripwires match `ci.yml`'s exact invocation before running them.

- [ ] **Step 5: Write the close-out**

Add the CLAUDE.md "Current state" entry in the established style: what the gap was, the mechanism, the ruling and its date, what the reviews caught, the tests with their seeds, and the full evidence block with counts. Retire nothing from the M2 backlog — Minor 5 lives in the "Still-open work" bullet of the `Next:` entry, which should be updated to drop it and to drop the `pin_floor` item if Task 1 closed it.

- [ ] **Step 6: Commit**

```bash
git add DESIGN.md docs/GUIDE.md CLAUDE.md
git commit -m "docs: what a verifying node may do as a peer, and the interlude close-out"
```

---

## Notes for the reviewer

- **The ordering constraint is the whole plan.** Task 2 before Task 3. The parked attempt landed the sender check against a node-fatal receiver and took down five property tests across two suites; that evidence is in `review-t5-fixes-report.md` C-1.
- **Task 3 Step 5's table is a gate, not a suggestion.** If one of those five is red, the keying is wrong. Report it; do not add an exemption.
- **`probe.rs` gets no production change.** `classify`'s default arm (`probe.rs:355`) already files any marker-free `Refuse` as `Inconclusive`. That is why Task 2 Step 6 proves the test's teeth by mutation instead — the behaviour is inherited, so only a test can hold it.
- **Open question the plan does not settle:** whether the witness-membership conjunct alone rescues all five blast-radius tests, or whether Task 2's retryability is load-bearing for their greenness too. The plan is ordered so this never matters (retryability lands first), but Task 3 Step 5 is where the answer shows up — record which it was in the ledger, since it tells us how much of the parked attempt's breakage was mis-keying versus fatality.
