# Dual-Primary Gap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A primary restarted after one of its followers was promoted must not silently resume as a second writable primary.

**Architecture:** A **witness set** — the durable list of listen addresses this node has shipped to — is written beside the role file. A durable `Primary` with a non-empty witness set boots write-stopped, probes each witness with a classify-and-disconnect client, and opens the write gate only when every witness answers "not a primary". A witness answering as a primary is a named refusal. An empty witness set (single-node, never-shipped) resumes immediately and silently.

**Tech Stack:** Rust, `charpente-core` `Disk`/`Net`/`Conn` traits (poll-shaped, no blocking), `charpente-sim` for fault-injected properties, `crates/edge-harness` for the wire-level tripwire.

**Spec:** `docs/superpowers/specs/2026-07-31-dual-primary-design.md`. Ruling (Noah, 2026-07-31): option **(c)**.
**Investigation:** `.superpowers/sdd/dual-primary-investigation.md`.

## Global Constraints

- **Hard rule 1 — no ambient nondeterminism.** All IO through `Disk`/`Net`/`Clock`/`Entropy`. The probe client uses `Conn::recv`, which is non-blocking (`charpente-core/src/io.rs:150-152`); never block, never sleep.
- **Hard rule 3 — RED first.** Every test is observed failing against unfixed code before the fix lands, and the RED is recorded in the ledger. **Exemption (inherited from `5e6412e`, sub-project 0a):** absence-asserting guards — tests whose assertion is "this thing does NOT happen" and which therefore pass on unfixed code by construction — are exempt, and must instead be proven by mutation (make the guarded thing happen; the test must redden).
- **Hard rule 4 — assertions on negative space**, on in release builds.
- **Hard rule 5 — error messages are product surface.** Every new operator-visible string is pinned byte-exact by a test in the same task that introduces it.
- **Hard rule 6 — public-API changes update the docs in the same commit.**
- **All three gates green at EVERY commit:** `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`. If a task cannot be green alone, say so and fold it into its neighbour rather than committing red (this is what sub-project 0a's T2/T3 ruling established).
- **Capture cargo output with `tee`, never `tail`** — `tail` eats aggregate counts and replaces cargo's exit status with its own.
- **No wire change.** No new `Msg` variant, no proto bump. "The peer answers as a primary" is already distinguishable.
- Durable writes use `write_at` + `set_len` + `sync` + `sync_dir`, CRC'd (the `role.rs:115-125` discipline).

## File Structure

| File | Responsibility |
|---|---|
| `crates/charpente/src/repl/witnesses.rs` (**new**) | The durable witness file: load, store, clear (add-if-new lives in Task 2's `Repl::record_witnesses`, not here). CRC'd, torn-write-safe. Pure — no `Net`, no `Repl`. |
| `crates/charpente/src/repl/probe.rs` (**new**) | `WitnessProbe`: classify-and-disconnect client. Sends `Hello`, classifies the first response, never applies a frame. |
| `crates/charpente-core/src/db.rs` | Write gate widened from `Option<String>` to three states; new `TxError` variant. |
| `crates/charpente/src/repl/mod.rs` | Wiring: record witnesses from ship sessions; the boot gate; probe stepping; the deadline notice. |
| `crates/charpente/src/http/app.rs` | Map the new `TxError` variant to its status + named body. |
| `crates/charpente/tests/replication.rs` | Sim properties. |
| `crates/edge-harness/tests/failover.rs` | The tripwire, updated and renamed. |

---

### Task 1: The durable witness file

**Files:**
- Create: `crates/charpente/src/repl/witnesses.rs`
- Modify: `crates/charpente/src/repl/mod.rs` (add `pub mod witnesses;` beside the existing `pub mod role;` at `:29`)

**Visibility is load-bearing, not taste.** Declare `pub mod witnesses;` and make every item
below `pub`, exactly mirroring `pub mod role;` / `pub fn load_role` / `pub fn store_role`.
Two reasons: (1) nothing calls these until Task 2, and `pub(crate)` items with no caller are
`dead_code`, which under the Global Constraints' `clippy -D warnings` would make this task
impossible to commit green on its own; (2) the integration tests reach `role` as
`charpente::repl::role::Role` today, and Task 2's test reaches `load_witnesses` the same way.

**Interfaces:**
- Consumes: `charpente_core::crc::Crc32`, `charpente_core::io::Disk` (same imports `role.rs:25-26` uses).
- Produces:
  - `pub const WITNESS_FILE: &str = "witnesses";`
  - `pub fn load_witnesses(disk: &dyn Disk) -> io::Result<Vec<String>>` — absent file → `Ok(vec![])`.
  - `pub fn store_witnesses(disk: &dyn Disk, addrs: &[String]) -> io::Result<()>`
  - `pub fn clear_witnesses(disk: &dyn Disk) -> io::Result<()>`

**Format.** `<addr>\n<addr>\n…\n<n> <crc32 hex>` — addresses joined by `\n`, then a trailer line carrying the COUNT and the CRC of the joined body. The empty set stores as `"\n0 <crc of empty>"`, so "cleared" and "never written" are indistinguishable on read and both yield `vec![]`.

> **SUPERSEDED TWICE — this section is the historical Task 1 format.** Task 1b (Noah-ruled
> 2026-07-31) split it into **two files** (`witnesses.0`/`witnesses.1`, so `WITNESS_FILE` is gone)
> and added a **generation** to the trailer; Task 1b's fix round 1 (Noah-ruled 2026-08-01) brought
> the generation and count **inside the CRC** and added `open_witnesses`, which establishes both
> slots at `Repl::open` before any content is ever written. The shipped format is
> `<addr>\n…\n<gen> <count> <crc32 hex>`, the CRC covering everything before itself.
> **`crates/charpente/src/repl/witnesses.rs` is authoritative.**

**AMENDED 2026-07-31 (Noah's ruling, after the Task 1 review).** The original format was
`…\n<crc32 hex>` with no count. The reviewer constructed a truncation that loads cleanly as a
SHORTER set: `["10.0.0.1:7001", "ce6f4d26host.internal:7002"]` cut to 22 bytes, because
`ce6f4d26 == crc32("10.0.0.1:7001")`. That is ~2⁻³² for arbitrary addresses but **constructible**,
because addresses are peer-supplied off the wire (`Hello.listen`, unvalidated). Two changes close
it together, and neither closes it alone:
1. the count `n`, checked against the parsed address count on load — shortening must now match
   both a CRC and a count;
2. `store_witnesses` **rejects any address that is empty or contains ASCII whitespace** (space,
   tab, CR, LF) by named error. Without this, a crafted address containing `"1 ce6f4d26"` supplies
   the trailer bytes itself and defeats the count too. A real listen address has no whitespace.

Reject by **named error, never `assert!`** — despite hard rule 4, this input is peer-supplied, and
an assert there is a remotely-triggerable panic.

> **The Step 1 and Step 3 code blocks below predate this amendment and were deliberately left
> unedited as the historical record of what was dispatched. They are NOT the shipped design:**
> Step 1's test asserts `(crc mismatch)` for the `set_len(15)` tear, an arm the amended format
> provably no longer takes (it now lands in `malformed trailer` — the arm was called
> `missing the crc line` until task 1b's fix round split that one string across the three
> distinct malformations it covered — structurally: with whitespace
> banned, the trailer's space is the file's only space byte, so any truncation that drops a
> trailing address removes it); Step 3 still shows the countless format, no address validation, and
> a `trim_end_matches` that is dead under the amended parse. Step 4's "6 passed" is 11 as shipped.
> **`crates/charpente/src/repl/witnesses.rs` is authoritative.** Do not re-execute these blocks
> literally — doing so writes code the Format section above forbids.

**Why CRC.** A torn witness write must fail by name, never parse as a *shorter* set — a shorter set is a *less safe* set, so silent truncation is the dangerous direction. This is the same reasoning `role.rs:11-21` records for a torn role line.

- [ ] **Step 1: Write the failing tests**

Add to `crates/charpente/src/repl/witnesses.rs` (module and tests together; the module body comes in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use charpente_sim::{FaultConfig, Sim};

    #[test]
    fn an_absent_file_is_an_empty_set() {
        let sim = Sim::with_faults(0x0DEE_0001, FaultConfig::none());
        assert_eq!(load_witnesses(&sim.disk()).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn a_stored_set_round_trips() {
        let sim = Sim::with_faults(0x0DEE_0002, FaultConfig::none());
        let disk = sim.disk();
        let addrs = vec!["127.0.0.1:7001".to_string(), "127.0.0.1:7002".to_string()];
        store_witnesses(&disk, &addrs).unwrap();
        assert_eq!(load_witnesses(&disk).unwrap(), addrs);
    }

    #[test]
    fn an_empty_stored_set_reads_back_empty() {
        let sim = Sim::with_faults(0x0DEE_0003, FaultConfig::none());
        let disk = sim.disk();
        store_witnesses(&disk, &[]).unwrap();
        assert_eq!(load_witnesses(&disk).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn clear_leaves_an_empty_set() {
        let sim = Sim::with_faults(0x0DEE_0004, FaultConfig::none());
        let disk = sim.disk();
        store_witnesses(&disk, &["127.0.0.1:7001".to_string()]).unwrap();
        clear_witnesses(&disk).unwrap();
        assert_eq!(load_witnesses(&disk).unwrap(), Vec::<String>::new());
    }

    // The load-bearing pair: a tear must NOT parse as a shorter (less safe)
    // set. Both shapes are real and take DIFFERENT arms, which is why both are
    // tested — role.rs records the same two shapes with two pinned seeds.
    //
    // Content is "127.0.0.1:7001\n127.0.0.1:7002\n<8 hex>"; the first address
    // is 14 bytes. Truncating to 15 leaves "127.0.0.1:7001\n", which DOES
    // rsplit_once into ("127.0.0.1:7001", "") — a perfectly parseable
    // one-element set — and is caught by the CRC, not by the missing-line
    // check. Truncating to 5 leaves "127.0" with no newline at all, which is
    // the missing-line arm. If the address literals change, both numbers move.
    #[test]
    fn a_parseable_tear_is_caught_by_the_crc_not_parsed_as_a_shorter_set() {
        let sim = Sim::with_faults(0x0DEE_0005, FaultConfig::none());
        let disk = sim.disk();
        let addrs = vec!["127.0.0.1:7001".to_string(), "127.0.0.1:7002".to_string()];
        store_witnesses(&disk, &addrs).unwrap();
        let file = disk.open(WITNESS_FILE).unwrap();
        file.set_len(15).unwrap();
        file.sync().unwrap();
        let err = load_witnesses(&disk)
            .expect_err("a torn witness file must not parse as a shorter set");
        assert_eq!(
            err.to_string(),
            "witness file failed its integrity check (crc mismatch) — a torn write or \
             corruption; the safe repair is to delete the \"witnesses\" file ONLY if you are \
             certain no follower of this node was ever promoted, since an empty witness set \
             lets this node resume as a primary unchecked"
        );
    }

    #[test]
    fn an_unparseable_tear_names_the_missing_crc_line() {
        let sim = Sim::with_faults(0x0DEE_0006, FaultConfig::none());
        let disk = sim.disk();
        store_witnesses(&disk, &["127.0.0.1:7001".to_string()]).unwrap();
        let file = disk.open(WITNESS_FILE).unwrap();
        file.set_len(5).unwrap();
        file.sync().unwrap();
        let err = load_witnesses(&disk).expect_err("a tear with no newline must fail by name");
        assert!(
            err.to_string().contains("(missing the crc line)"),
            "expected the missing-line arm, got: {err}"
        );
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p charpente --lib repl::witnesses 2>&1 | tee /tmp/t1.log`
Expected: FAIL — the module does not exist yet (compile error naming `witnesses`).

- [ ] **Step 3: Write the implementation**

`crates/charpente/src/repl/witnesses.rs`, above the test module:

```rust
//! The durable witness set (spec docs/superpowers/specs/2026-07-31-dual-
//! primary-design.md §1): the listen addresses this node has shipped to.
//! A durable `Primary` with a non-empty set must confirm none of them has
//! been promoted before it accepts writes — that is the whole dual-primary
//! defence, so this file's integrity matters in one direction especially:
//! a torn write must never parse as a SHORTER set, because a shorter set is
//! a weaker check. Same discipline as `role.rs`, for the same reason.

use std::io;

use charpente_core::crc::Crc32;
use charpente_core::io::Disk;

pub const WITNESS_FILE: &str = "witnesses";

fn body_crc(body: &str) -> u32 {
    let mut crc = Crc32::new();
    crc.update(body.as_bytes());
    crc.finish()
}

fn corrupt_witness_error(reason: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "witness file failed its integrity check ({reason}) — a torn write or corruption; \
             the safe repair is to delete the \"{WITNESS_FILE}\" file ONLY if you are certain \
             no follower of this node was ever promoted, since an empty witness set lets this \
             node resume as a primary unchecked"
        ),
    )
}

pub fn store_witnesses(disk: &dyn Disk, addrs: &[String]) -> io::Result<()> {
    let body = addrs.join("\n");
    let content = format!("{body}\n{:08x}", body_crc(&body));
    let bytes = content.as_bytes();
    let file = disk.open(WITNESS_FILE)?;
    file.write_at(0, bytes)?;
    file.set_len(bytes.len() as u64)?;
    file.sync()?;
    disk.sync_dir(WITNESS_FILE)?;
    Ok(())
}

pub fn load_witnesses(disk: &dyn Disk) -> io::Result<Vec<String>> {
    if !disk.list(WITNESS_FILE)?.iter().any(|n| n == WITNESS_FILE) {
        return Ok(Vec::new());
    }
    let file = disk.open(WITNESS_FILE)?;
    let len = file.len()?;
    let mut buf = vec![0u8; len as usize];
    file.read_at(0, &mut buf)?;
    let text = String::from_utf8(buf).map_err(|_| corrupt_witness_error("not valid UTF-8"))?;
    let (body, stored_crc) = text
        .rsplit_once('\n')
        .ok_or_else(|| corrupt_witness_error("missing the crc line"))?;
    if format!("{:08x}", body_crc(body)) != stored_crc.trim_end_matches('\n') {
        return Err(corrupt_witness_error("crc mismatch"));
    }
    Ok(body
        .split('\n')
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect())
}

pub fn clear_witnesses(disk: &dyn Disk) -> io::Result<()> {
    store_witnesses(disk, &[])
}
```

Then add `pub mod witnesses;` to `crates/charpente/src/repl/mod.rs` beside the existing `pub mod role;` at `:29`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p charpente --lib repl::witnesses 2>&1 | tee /tmp/t1.log`
Expected: 6 passed.

- [ ] **Step 5: Gates and commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace 2>&1 | tee /tmp/ws.log
git add crates/charpente/src/repl/witnesses.rs crates/charpente/src/repl/mod.rs
git commit -m "repl: the durable witness set

The listen addresses this node has shipped to, written beside the role file
with the same write_at + set_len + sync + sync_dir + CRC discipline. Nothing
reads it yet. The CRC exists for one direction in particular: a torn write
must not parse as a SHORTER set, because a shorter witness set is a weaker
dual-primary check, and silent truncation is therefore the dangerous tear."
```

---

### Task 2: Record witnesses when a ship session is established

**Files:**
- Modify: `crates/charpente/src/repl/mod.rs` (the `Repl` struct, `Repl::new`, and `Repl::step`)
- Test: `crates/charpente/tests/replication.rs`

**Interfaces:**
- Consumes: Task 1's `load_witnesses`/`store_witnesses`.
- Produces: `Repl.witnesses: Vec<String>` (in-memory mirror of the durable set), and a private `fn record_witnesses(&mut self) -> io::Result<()>` called from `Repl::step`.

**Where the hook goes, and why not in `Session`.** `session.rs:287-296` sets `self.listen` on exactly the two `Decision` arms that mean "we agreed to ship" (`Frames` and `Snapshot`) and never on `Refuse` — so a refused peer is correctly never a witness. But `Session::handle` has no `Disk`; `Repl` owns `role_disk` and performs every durable write in this module (`store_role(&*self.role_disk, …)`). So `Repl` scans its sessions after servicing them and persists any newly-known `listen` not already in the set. This keeps `Session` pure and matches the existing ownership.

- [ ] **Step 1: Write the failing test**

In `crates/charpente/tests/replication.rs`, following the file's existing two-host rig conventions (copy the harness setup from the nearest existing multi-host test in that file — do not invent a new rig):

```rust
#[test]
fn a_primary_records_each_follower_it_ships_to_as_a_witness() {
    // Two hosts; the follower Hellos, the primary agrees to ship, and the
    // follower's listen address must be durable on the primary before the
    // test ends. Asserted through a restart, because the point of the set is
    // that it survives one.
    // ... rig setup per this file's existing pattern ...
    // After convergence:
    let witnesses = charpente::repl::witnesses::load_witnesses(&primary_disk).unwrap();
    assert_eq!(
        witnesses,
        vec![follower_listen.clone()],
        "the primary must durably record every follower it agreed to ship to"
    );
}
```

No test accessor is needed: Task 1 made `witnesses` a public module, so the test reaches `charpente::repl::witnesses::load_witnesses` exactly as the suite already reaches `charpente::repl::role::Role`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p charpente --test replication a_primary_records_each_follower 2>&1 | tee /tmp/t2.log`
Expected: FAIL — the assertion sees an empty vec against a one-element expectation (the file is never written yet).

- [ ] **Step 3: Implement**

Add to the `Repl` struct (`mod.rs:128-150` region):

```rust
    /// Listen addresses this node has shipped to, mirrored from the durable
    /// `witnesses` file. A durable `Primary` with a non-empty set must clear
    /// its witnesses before accepting writes (spec §2).
    witnesses: Vec<String>,
```

In `Repl::new`, load it before the role match and add `witnesses` to the struct literal:

```rust
        let witnesses = load_witnesses(&*role_disk)?;
```

Add the recorder, and call it at the end of `Repl::step` (after sessions are serviced):

```rust
    /// Persists any newly-known peer listen address. Called after sessions are
    /// serviced: `Session::handle` sets `listen` only on the two Decision arms
    /// that agree to ship (session.rs:287-296), so a refused peer is never a
    /// witness. One durable write per NEW peer, never per ship.
    fn record_witnesses(&mut self) -> io::Result<()> {
        let mut added = false;
        for s in &self.sessions {
            if let Some(addr) = &s.listen
                && !self.witnesses.contains(addr)
            {
                self.witnesses.push(addr.clone());
                added = true;
            }
        }
        if added {
            store_witnesses(&*self.role_disk, &self.witnesses)?;
        }
        Ok(())
    }
```

Clear the set on every durable flip away from `Primary` — a demoted-and-re-seeded node is on a new timeline and its old witnesses would block writes after a future legitimate promotion. Add `clear_witnesses(&*self.role_disk)?; self.witnesses.clear();` immediately after each `store_role(… &Role::Follower {…})` call in `mod.rs` (there are two, at `:708` and `:802`) and after the `HandingOff` → follower completion.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p charpente --test replication 2>&1 | tee /tmp/t2.log`
Expected: the new test passes and the existing replication suite is unchanged (32 tests today).

- [ ] **Step 5: Gates and commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace 2>&1 | tee /tmp/ws.log
git add crates/charpente/src/repl/mod.rs crates/charpente/tests/replication.rs
git commit -m "repl: record every follower this primary ships to

One durable write per NEW peer, taken from the listen address Hello already
carries and Session already stores. Cleared on every durable flip away from
Primary: a demoted-and-re-seeded node is on a new timeline, and a stale
witness would block writes after a later legitimate promotion."
```

---

### Task 3: The write gate's third state

**Files:**
- Modify: `crates/charpente-core/src/db.rs` (the `not_primary` field, `allow_writes`, `forbid_writes`, and `TxError`)
- Modify: `crates/charpente/src/http/app.rs` (status mapping)
- Test: `crates/charpente/tests/http.rs` (the named body, pinned byte-exact)

**Why this task exists.** `db.rs:1077` models the gate as `not_primary: Option<String>` and surfaces it as `TxError::NotPrimary { primary }` → 421 naming where to go instead. A witness-gated node has **no primary to name** — it does not know whether one exists, and it may itself still be the legitimate primary. Telling a client "go to X" would be a lie. So the gate needs a third state and its own honest error. This is a **pre-release breaking change to the public `TxError` enum**, in the same class as realtime's `emit` format change.

> **HISTORICAL — Task 3's interface sketch and the Step 1 body below are left
> unedited as the record of what was dispatched. They are NOT the shipped design**
> (superseded by `99f81a1` + `2cad5fc`): `WriteGate::Verifying` carries the witness
> list (`Verifying(Vec<String>)`, mirroring `NotPrimary(String)`) because
> `TxError::Verifying { unreached }` has to report it, and the enum carries no `pub`
> at all rather than the `pub` written here. The 503 body's closing clause shipped as
> "see GUIDE §10's witness-set remedy" — the section is named, so the reference is
> greppable. The shipped strings are authoritative in `crates/charpente/src/http/app.rs`
> and pinned byte-exact in `crates/charpente/tests/http.rs`; do not re-execute the
> blocks below.

**Interfaces:**
- Produces:
  - `pub enum WriteGate { Allowed, NotPrimary(String), Verifying }` (private to `Db`; the field type)
  - `TxError::Verifying { unreached: Vec<String> }`
  - `pub fn verify_writes(&mut self, unreached: Vec<String>)` on `Db`
  - `allow_writes()` and `forbid_writes(String)` keep their existing signatures.

- [ ] **Step 1: Write the failing test**

In `crates/charpente/tests/http.rs`, beside the existing `NotPrimary`→421 pin:

```rust
#[test]
fn a_verifying_node_refuses_writes_with_503_and_names_the_unreached_witnesses() {
    // ... mount an app, then: db.verify_writes(vec!["127.0.0.1:7001".into()]);
    // POST a write route; assert on the reply.
    assert_eq!(reply.status, 503);
    assert_eq!(
        String::from_utf8_lossy(&reply.body),
        "this node is not accepting writes yet: it was a primary before restarting and has \
         not confirmed that its follower 127.0.0.1:7001 was never promoted — reads are being \
         served normally, and writes resume automatically once that node answers; if it is \
         gone for good, see the witness-set remedy in the guide"
    );
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p charpente --test http a_verifying_node 2>&1 | tee /tmp/t3.log`
Expected: FAIL — `verify_writes` does not exist.

- [ ] **Step 3: Implement**

Replace the `not_primary: Option<String>` field with `gate: WriteGate`; `writes_allowed()` becomes `matches!(self.gate, WriteGate::Allowed)`. The transact entry point returns `TxError::Verifying { unreached }` when the gate is `Verifying`. Map it in `http/app.rs` to 503 with the body above, pluralising "its follower X" → "its followers X, Y" when `unreached.len() > 1` (write the plural form's own pinning test in the same step — hard rule 5 applies to both renderings).

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p charpente --test http 2>&1 | tee /tmp/t3.log`
Expected: PASS, including the pre-existing `NotPrimary`→421 pin, which must be **byte-unchanged** — this task adds a state, it does not alter the existing one.

- [ ] **Step 5: Gates and commit** (message: `core+http: a third write-gate state for witness verification`, noting the pre-release `TxError` addition.)

---

### Task 4: The probe client

**Files:**
- Create: `crates/charpente/src/repl/probe.rs`
- Modify: `crates/charpente/src/repl/mod.rs` (`pub mod probe;`)

**Same visibility constraint as Task 1**, for the same reason: nothing calls this until Task 5,
so `pub(crate)` items would be `dead_code` and this task could not commit green under
`clippy -D warnings`. Declare `pub mod probe;` with `pub` items, mirroring the sibling
`pub mod protocol;` at `mod.rs:28`.

**Interfaces:**
- Consumes: `Msg`, `encode`, `decode`, `PROTO_VERSION`, `hello_position` from `protocol.rs`/`follower.rs`; `Net`, `Conn` from `charpente_core::io`.
- Produces:
  - `pub enum WitnessVerdict { Pending, NotPrimary, IsPrimary(String) }`
  - `pub struct WitnessProbe { … }` with `pub fn WitnessProbe::new(addr, my_listen, secret)`, `fn step(&mut self, net: &dyn Net, db: &Db, now: u64) -> io::Result<()>`, `fn verdict(&self) -> &WitnessVerdict`.

**This must NOT reuse `Follower::new_resolving`, and the reason is load-bearing.** `Follower::step` classifies the first response and then **falls through to the normal handlers** (`follower.rs:238-239`): `Msg::Frames` reaches `apply_and_ack`, `SnapshotStart` opens a seed. For a `HandingOff` node that is correct — it is demoting to a follower of `to`. Here it would **silently auto-heal a forked node by adopting the winner's timeline**, which is exactly what the 2026-07-16 diverged-BEHIND ruling rejected: the node cannot know it is the loser, and its acked writes are not ours to discard. So this client classifies and disconnects, and never applies anything.

**Classification** (mirrors `follower.rs:221-236`, which is the shape to copy):

| First response | Verdict |
|---|---|
| `Msg::Refuse` whose message contains `"not a primary"` | `NotPrimary` |
| any other `Msg::Refuse` | `IsPrimary(message)` — the `diverged_*` family only a primary sends |
| any other message (`Frames`, `SnapshotStart`, …) | `IsPrimary(<describe the message kind>)` |
| connect fails / no response yet | stays `Pending`, back off and retry |

- [ ] **Step 1: Write the failing tests**

Three sim tests in `crates/charpente/src/repl/probe.rs`'s own `#[cfg(test)] mod tests`, driving a `WitnessProbe` against a scripted peer: one where the peer refuses with `"not a primary"` (expect `NotPrimary`), one where it refuses with a `diverged_behind` message (expect `IsPrimary`), one where it sends `Msg::Frames` (expect `IsPrimary`, **and** assert `db.last_applied()` is unchanged — the anti-adoption guard, which is the whole point of this task).

- [ ] **Step 2: Run to verify they fail** — `cargo test -p charpente --lib repl::probe 2>&1 | tee /tmp/t4.log`. Expected: compile failure, module absent.

- [ ] **Step 3: Implement** the struct and `step`, following `follower.rs:304-327`'s `connect_and_hello` for the connect/Hello half and `follower.rs:154-175`'s `drain` for the non-blocking recv half. On any verdict other than `Pending`, drop the conn immediately.

- [ ] **Step 4: Run to verify they pass.**

**Anti-vacuity note for the reviewer:** the `Msg::Frames` test's `last_applied` assertion is an absence-asserting guard and therefore exempt from RED-first per the Global Constraints — prove it by mutation instead: make `step` fall through to `apply_and_ack` and confirm the test reddens.

- [ ] **Step 5: Gates and commit.**

---

### Task 5: The boot gate

**Files:**
- Modify: `crates/charpente/src/repl/mod.rs` (`Repl::new`, `Repl::step`, `has_work`, the `Repl` struct)
- Test: `crates/charpente/tests/replication.rs`

**The constraint that shapes this task.** `mod.rs:191` calls `db.allow_writes()` inside the `match &role` block, and the follower client is not constructed until `:204` — **writes are enabled strictly before any probe could run.** The gate must move: a durable `Primary` with a non-empty witness set calls `db.verify_writes(witnesses.clone())` there instead, and only reaches `allow_writes()` when every probe has returned `NotPrimary`. A fix bolted on after `:191` is not a fix.

**Interfaces:**
- Consumes: Tasks 1–4.
- Produces: `Repl.probes: Vec<WitnessProbe>`, `Repl.witness_deadline: Option<u64>`, `Repl.witness_notice_sent: bool`; `pub(crate) const WITNESS_DEADLINE_NANOS: u64 = 30_000_000_000;` (30 virtual seconds).

- [ ] **Step 1: Write the failing tests**

Four in `crates/charpente/tests/replication.rs`:

1. `a_resurrected_primary_does_not_accept_writes_until_its_witness_answers` — the headline. Primary ships to follower, follower is promoted, primary restarts with its ORIGINAL config. Assert on the **gate** (`db.writes_allowed()` false), not on a message.
2. `a_resurrected_primary_refuses_by_name_when_its_witness_answers_as_a_primary` — pin the refusal string byte-exact.
3. `an_empty_witness_set_resumes_immediately_and_silently` — the single-node regression: no probe, no notice, `writes_allowed()` true straight out of `Repl::new`.
4. `an_unreachable_witness_serves_reads_and_withholds_writes` — the ruling's shape: after `WITNESS_DEADLINE_NANOS`, reads succeed, writes get the Task 3 503, and the notice fired exactly once.

- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement** the gate, probe stepping in `Repl::step`, the deadline notice (once — guard with `witness_notice_sent`), and `has_work` returning true while any probe is `Pending` and its backoff is due (otherwise the kernel could park past the retry).
- [ ] **Step 4: Run to verify they pass**, plus the full replication suite unchanged.
- [ ] **Step 5: Gates and commit.**

---

### Task 6: `strict_witnesses`, the edge tripwire, docs, and the evidence sweep

**Files:**
- Modify: `crates/charpente/src/repl/mod.rs` (`ReplConfig`), `crates/edge-harness/tests/failover.rs`, `DESIGN.md`, `docs/GUIDE.md`, `CLAUDE.md`

- [ ] **Step 1: `ReplConfig::strict_witnesses: bool`** (default `false`) — when set, an unreached witness past the deadline is a refusal to start rather than read-only service. Pin its error string.
- [ ] **Step 2: The tripwire.** `failover_known_gap_silent_dual_primary_resurrection` is **updated, never deleted**, and renamed to `failover_resurrected_primary_does_not_accept_writes` — the old name describes a gap that no longer exists. It currently proves the two primaries cannot see each other; it must now prove the resurrected node refuses writes and names why. **It is `#[ignore]`**, so its red appears only under `scripts/edge-harness.sh`, never in `cargo test --workspace` — state this in the commit, since a `--workspace` run will look green either way.
- [ ] **Step 3: Docs.** DESIGN §5's promote paragraph (which currently names this as a known gap) and GUIDE §10's operator warning both flip from "known gap" to the shipped behaviour, plus the witness-set remedy from spec §4 with its explicit warning that clearing the file re-opens the gap. Run `cargo build -p guide-examples`.
- [ ] **Step 4: CLAUDE.md.** Retire the dual-primary backlog entry; add the close-out to "Current state" — mechanism, the ruled option (c), the honest cost from spec §4, and the tripwire's new name. This empties the M2 follow-up backlog.
- [ ] **Step 5: The evidence sweep** (spec §7): fmt, clippy, guide gate, `cargo test --workspace` twice, both pinned debug replays (`0x8bee1752811ae873`, `0x348270b38f166529`), both release mode-skew tripwires as `ci.yml` runs them, a fresh invoicing 1024-universe batch with its master seed recorded, and `scripts/edge-harness.sh`. **`tee`, never `tail`.**
- [ ] **Step 6: Commit.**

---

## Notes for the reviewer

- **The one thing worth attacking:** Task 5's gate. If a durable `Primary` with a non-empty witness set can reach `allow_writes()` without every probe returning `NotPrimary`, the sub-project is decorative. Check the ordering against `mod.rs:191` specifically — that line is where the old behaviour lived.
- **Second:** Task 4's anti-adoption guard. If the probe can apply a frame or open a seed, a forked node silently adopts the winner's timeline and discards its own acked writes — the exact outcome the 2026-07-16 ruling forbade.
- **Anti-vacuity:** Task 5's test 3 (empty set resumes) passes trivially on unfixed code — it is the single-node regression guard, not evidence of the fix. Test 1 is the evidence.
- **Ordering:** 1 → 2 → 3 → 4 → 5 → 6, strictly. Task 5 consumes all of 1–4. No task may be skipped to "come back to it."
