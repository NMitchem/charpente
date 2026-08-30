# Tenant Scoping as Typed Key Prefixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make "every query is tenant-scoped" a type-level property — a cross-tenant read on a scoped table is a compile error, and a foreign row is unaddressable rather than merely rejected.

**Architecture:** `Row` gains an associated `Scope` type (default `Unscoped`). `#[table(scoped_by = Org)]` sets it, which moves that table's reads onto `ScopedTx<'_, Org>` and changes its physical key to a 16-byte big-endian `(scope, id)` composite with a matching index prefix. Cross-tenant enumeration is a capability declared on the route, so `charpente describe` can inventory it. Unscoped tables keep their exact stored bytes.

**Tech Stack:** Rust, redb (behind `Disk`), `charpente-macros` (proc macros + trybuild), `charpente-sim` (seeded fault injection), `edge-harness` (black-box wire rig).

**Spec:** `docs/superpowers/specs/2026-08-30-tenant-scoping-design.md` — read it before Task 1. The plan argues from the spec; both travel together.

## Global Constraints

- **Hard rule 1 — no ambient nondeterminism inside the core.** All IO through `Clock`/`Entropy`/`Disk`/`Net`/`ObjectStore`. The clippy deny-list enforces it; no `#[allow]` may be added to pass clippy.
- **Hard rule 2 — no escape hatches on guarantees.** `across_tenants` is a declared capability, not a flag; see spec §3's argument. Nothing may weaken the `ScopedTx` path.
- **Hard rule 3 — sim-first.** Every core subsystem lands with seeded fault-injection tests in the same PR. **State the seed count in the commit.** A bug found in sim gets its seed recorded.
- **Hard rule 4 — assertions in the TigerBeetle style**, including negative space; on in release.
- **Hard rule 5 — error messages are product surface.** A contract violation names the contract, the site, and the fix. Write tests against error text.
- **Hard rule 6 — public-API changes update the docs in the same commit.**
- **Commit directly to `main`. No Co-Authored-By trailers.** `git add` named paths only, never `-A` or `.`.
- **Full check:** `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`. The workspace suite cannot be relied on to finish inside one Bash call — **partition per package**.
- **This shell is zsh.** `${PIPESTATUS[0]}` expands to empty and reads as "no failure". Use `${pipestatus[1]}` or `set -o pipefail`. Pipe cargo through `tee`, never `tail`.
- **Replay:** `CHARPENTE_SEED=0x… cargo test -p <crate> --test <file> -- --nocapture`.

## Shipping-unit constraint (read before drawing a branch boundary)

**Tasks 4, 5 and 6 are ONE shipping unit.** Spec §2: shipping the index prefix later would leave `scan_by` as a fully-functional cross-tenant read on a table the type system had already certified as scoped — silent, and endorsed by the types. They are separate tasks because a reviewer can meaningfully gate each; **the branch must not be merged between Task 4 and Task 6.**

## File Structure

| file | responsibility |
|---|---|
| `crates/charpente-core/src/table.rs` | key encoding (`rows_def`, `index_entry`), prefix-bounded `scan`/`scan_from`/`scan_by`, index maintenance in `Writer::put`/`delete`, `scan_raw_from` |
| `crates/charpente-core/src/db.rs` | `Row::Scope`, `Unscoped`, `ScopedTx`, `TenantScope`, `Tx::scope`, `across_tenants`, `GroupOverlay` key, scope-keyed `emit` |
| `crates/charpente-macros/src/table.rs` | `scoped_by` attribute parsing, `Scope` emission, `TenantScope` const assert |
| `crates/charpente-macros/src/{route,job}.rs` | `cross_tenant` attribute, capability token in glue |
| `crates/charpente-macros/tests/trybuild/` | the compile-fail matrix and its pinned `.stderr` |
| `crates/charpente/src/http/{app,router,mod}.rs` | `Ctx::cap()`, glue signatures, `describe` surface for `cross_tenant` |
| `crates/charpente-sim/tests/tenant.rs` | the flagship property, its naive victim, the `require:` keys |
| `crates/edge-harness/tests/tenant_wire.rs` | two-tenant black-box wire oracle |
| `DESIGN.md`, `PLAN.md`, `docs/GUIDE.md`, `DECISIONS.md` | hard rule 6 |

---

### Task 1: Resolve the WAL record-shape question and record the ruling

The spec flags one fact as deliberately unverified: whether the WAL effects record encodes a row write as `(table, id, bytes)`. If it does, this sub-project is a **log-format change** reaching replication and archive/restore, not a table-layer change. Nothing else may start until this is answered, because the answer changes Task 4's blast radius.

**Files:**
- Read: `crates/charpente-core/src/log.rs`, `crates/charpente-core/src/db.rs:340-420` (the `TxEffects` / record encode path)
- Modify: `docs/superpowers/specs/2026-08-30-tenant-scoping-design.md` (§2's unverified paragraph becomes a ruling)

**Interfaces:**
- Consumes: nothing.
- Produces: a written ruling that Task 4 reads. If the answer is "yes, log-format change", Task 4 additionally owes a follower-refusal path and Task 12 owes a mixed-version replication oracle.

- [ ] **Step 1: Read the record encoding**

Run and read in full:

```bash
sed -n '330,430p' crates/charpente-core/src/db.rs
grep -n 'fn encode\|fn decode\|struct Record\|enum Effect\|RowWrite\|row_write' crates/charpente-core/src/log.rs
```

Answer exactly one question: **does the durable record carry a row's key, and in what form?**

- [ ] **Step 2: Write the ruling into the spec**

Replace the paragraph in spec §2 beginning "**Unverified, and the first thing the plan must confirm.**" with the finding, citing `file.rs:line`. State plainly which of the two worlds we are in:

- **World A — the record carries `(table, id, bytes)`.** This is a log-format change. Add to this plan, as Task 4 steps: a scope field on the record; a version discriminator; and a follower on an older binary **refusing by name** rather than misreading (the migrations idiom, DESIGN §3). Add a mixed-version replication oracle to Task 12.
- **World B — the record carries opaque pre-encoded key bytes.** No log-format change; the composite key rides through as bytes and replication is untouched. Say so explicitly, because the rest of the plan is written assuming this is *not* guaranteed.

- [ ] **Step 3: Commit the ruling**

```bash
git add docs/superpowers/specs/2026-08-30-tenant-scoping-design.md
git commit -m "docs(spec): rule the WAL record-shape question for tenant scoping"
```

---

### Task 2: `Row::Scope` and the `Unscoped` marker, with zero behavior change

Pure type plumbing. Every existing table becomes `Scope = Unscoped` and behaves byte-identically. This task exists on its own so that a reviewer can confirm "nothing changed" before anything does.

**Files:**
- Modify: `crates/charpente-core/src/db.rs` (the `Row` trait), `crates/charpente-core/src/table.rs` (any `Row` bound that must name `Scope`)
- Test: `crates/charpente-core/tests/engine.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `pub struct Unscoped;` and `trait Row { type Scope; … }` with `type Scope = Unscoped;` as the default. Tasks 3, 4 and 7 all bind on it.

- [ ] **Step 1: Write the failing test**

Add to `crates/charpente-core/tests/engine.rs`:

```rust
#[test]
fn an_ordinary_table_is_unscoped_by_default() {
    fn assert_unscoped<R: Row<Scope = Unscoped>>() {}
    // Any existing #[table] type in the test module works here.
    assert_unscoped::<Widget>();
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p charpente-core --test engine an_ordinary_table_is_unscoped_by_default 2>&1 | tee /tmp/t2.log
```

Expected: FAIL — `Unscoped` not found, or `Row` has no associated type `Scope`.

- [ ] **Step 3: Add the associated type**

In `crates/charpente-core/src/db.rs`, beside the `Row` trait:

```rust
/// Marker for a table whose rows are not tenant-scoped. The default.
pub struct Unscoped;
```

and on `Row`:

```rust
    /// The tenant scope this row is partitioned by, or `Unscoped`.
    /// Set by `#[table(scoped_by = …)]`; see DESIGN §8.
    type Scope = Unscoped;
```

If the toolchain rejects the defaulted associated type (`associated_type_defaults` is unstable), do **not** reach for a nightly feature — instead have `#[table]` emit `type Scope = ::charpente::core::Unscoped;` unconditionally in Task 3, and declare `type Scope;` undefaulted here. Record which branch was taken in the commit message; Task 3 depends on it.

- [ ] **Step 4: Run the test and the package suite**

```bash
cargo test -p charpente-core 2>&1 | tee /tmp/t2b.log; echo "exit=${pipestatus[1]}"
```

Expected: PASS, and **every pre-existing test still green**. Any change in behavior here is a defect, not a cost.

- [ ] **Step 5: Commit**

```bash
git add crates/charpente-core/src/db.rs crates/charpente-core/src/table.rs crates/charpente-core/tests/engine.rs
git commit -m "feat(core): Row gains an associated Scope type, defaulting to Unscoped"
```

---

### Task 3: `#[table(scoped_by = Org)]`, the `TenantScope` contract, and its compile errors

**Files:**
- Modify: `crates/charpente-macros/src/table.rs:116-189` (emission), and its attribute parser (mirror `is_index` at `table.rs:40`)
- Create: `crates/charpente-macros/tests/trybuild/scoped_by_without_tenant_scope_impl.rs` + `.stderr`
- Modify: `crates/charpente-core/src/db.rs` (the `TenantScope` trait)

**Interfaces:**
- Consumes: `Row::Scope` from Task 2.
- Produces: `#[table(scoped_by = T)]` emitting `type Scope = T;`, and
  `pub trait TenantScope: Sized { fn authorize(tx: &Tx, who: Option<&Principal>, scope: Id<Self>) -> Result<(), ApiError>; }`.
  Task 7 calls `authorize`; Task 4 reads `Scope` to pick the key encoding.

- [ ] **Step 1: Write the failing trybuild case**

`crates/charpente-macros/tests/trybuild/scoped_by_without_tenant_scope_impl.rs`:

```rust
use charpente::prelude::*;

#[table]
struct Org { id: Id<Org>, name: String }

#[table(scoped_by = Org)]
struct Invoice { id: Id<Invoice>, org: Id<Org>, total: u32 }

fn main() {}
```

- [ ] **Step 2: Run it and watch it fail the right way**

```bash
cargo test -p charpente-macros --test trybuild 2>&1 | tee /tmp/t3.log
```

Expected: FAIL — trybuild reports no `.stderr` yet. It must **not** compile cleanly; if it does, `scoped_by` is not yet enforcing anything.

- [ ] **Step 3: Parse the attribute and emit the const assert**

In `crates/charpente-macros/src/table.rs`, parse `scoped_by = <path>` off the table attribute alongside the existing `version`. Emit, per the Task 2 branch, either `type Scope = #scope_ty;` or nothing. Then emit the assert that forces the impl to exist, in the same shape the migration assert already uses:

```rust
const _: fn() = || {
    fn assert_tenant_scope<T: ::charpente::core::TenantScope>() {}
    let _ = assert_tenant_scope::<#scope_ty>;
};
```

Define the trait in `crates/charpente-core/src/db.rs`:

```rust
/// The app's own membership check for a tenant scope. The platform requires
/// that one exists; it never interprets the scope value (DESIGN.md:1060-1062).
pub trait TenantScope: Sized {
    fn authorize(tx: &Tx, who: Option<&Principal>, scope: Id<Self>) -> Result<(), ApiError>;
}
```

- [ ] **Step 4: Pin the error text**

```bash
TRYBUILD=overwrite cargo test -p charpente-macros --test trybuild 2>&1 | tee /tmp/t3b.log
```

Then **read the generated `.stderr`**. Per hard rule 5 it must name the contract, the site and the fix. If it reads as a bare "trait bound not satisfied", add `#[diagnostic::on_unimplemented]` to `TenantScope`:

```rust
#[diagnostic::on_unimplemented(
    message = "`{Self}` is used as a tenant scope but has no `TenantScope` impl",
    label = "no `impl TenantScope for {Self}`",
    note = "a scoped table needs the app's own membership check; add:\n\
            impl TenantScope for {Self} {{ fn authorize(tx, who, scope) -> Result<(), ApiError> {{ … }} }}"
)]
```

Re-run `TRYBUILD=overwrite` and read it again. Do not accept a message you would not want to receive.

- [ ] **Step 5: Add the passing counterpart**

Add a `tests/trybuild/pass/` case identical to Step 1 **plus** a `TenantScope` impl, and confirm it compiles. Without this, Step 1 passing proves only that something is broken, not that the right thing is.

- [ ] **Step 6: Commit**

```bash
git add crates/charpente-macros/src/table.rs crates/charpente-macros/tests/trybuild crates/charpente-core/src/db.rs
git commit -m "feat(macros): scoped_by requires a TenantScope impl, by named compile error"
```

---

### Task 4: The composite row key for scoped tables

**Shipping unit: Tasks 4-6 land together. Do not merge between them.**

**Files:**
- Modify: `crates/charpente-core/src/table.rs:346-348` (`rows_def`), `:736-780` (`Writer::put`/`delete`), the `get` path
- Test: `crates/charpente-core/tests/engine.rs`

**Interfaces:**
- Consumes: `Row::Scope` (Task 2), `scoped_by` (Task 3), the Task 1 ruling.
- Produces: `fn scoped_key(scope: u64, id: u64) -> [u8; 16]` and a scoped `rows_def` variant. Tasks 5 and 6 range over the layout it defines.

- [ ] **Step 1: Read the current key path before changing it**

```bash
sed -n '340,365p' crates/charpente-core/src/table.rs
sed -n '730,785p' crates/charpente-core/src/table.rs
```

You are looking for every place a `u64` id becomes a redb key. The census found `rows_def` at `:346-348` and the put/delete pair at `:736-780`; confirm the list is complete before editing, and note any site the census missed.

- [ ] **Step 2: Write the failing test — ordering is the property that matters**

In `crates/charpente-core/tests/engine.rs`:

```rust
#[test]
fn a_scoped_key_orders_by_scope_then_id() {
    // Big-endian is load-bearing: lexicographic byte order must agree with
    // (scope, id) numeric order, or a prefix range is not contiguous and the
    // whole scan guarantee collapses into a filter.
    let a = scoped_key(1, u64::MAX);
    let b = scoped_key(2, 0);
    assert!(a < b, "tenant 1's last row must sort before tenant 2's first");

    let c = scoped_key(1, 5);
    let d = scoped_key(1, 6);
    assert!(c < d, "within a tenant, ids must stay ordered");
}
```

- [ ] **Step 3: Run it and watch it fail**

```bash
cargo test -p charpente-core --test engine a_scoped_key_orders_by_scope_then_id 2>&1 | tee /tmp/t4.log
```

Expected: FAIL — `scoped_key` not found.

- [ ] **Step 4: Implement the encoding**

In `crates/charpente-core/src/table.rs`, next to `index_entry`:

```rust
/// A scoped table's row key: scope then id, both big-endian, so that
/// lexicographic order matches (scope, id) order and one tenant's rows are a
/// contiguous range. Same reason `index_entry` encodes its id big-endian.
pub(crate) fn scoped_key(scope: u64, id: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&scope.to_be_bytes());
    k[8..].copy_from_slice(&id.to_be_bytes());
    k
}
```

Add the scoped table definition beside `rows_def`. Pin the key type against redb's actual `Key` impls — check what redb implements before choosing between `[u8; 16]` and `&[u8]`:

```bash
grep -rn 'impl.*Key for' ~/.cargo/registry/src/*/redb-*/src/types.rs | head -30
```

Then route `put`/`delete`/`get` for `R::Scope != Unscoped` through it.

- [ ] **Step 5: Prove unscoped tables did not move**

```rust
#[test]
fn an_unscoped_table_still_writes_a_bare_u64_key() {
    // The pinned-seed guarantee rests on this: unscoped tables must be
    // byte-identical after this change, or every existing universe shifts.
    // Assert against the stored bytes, not against a round-trip.
}
```

Fill this in against the real store API — a round-trip test would pass even if the key encoding changed, which is exactly the ambiguity to avoid.

- [ ] **Step 6: Run the package suite**

```bash
cargo test -p charpente-core 2>&1 | tee /tmp/t4b.log; echo "exit=${pipestatus[1]}"
```

Expected: PASS, all pre-existing tests included.

- [ ] **Step 7: Commit**

```bash
git add crates/charpente-core/src/table.rs crates/charpente-core/tests/engine.rs
git commit -m "feat(core): scoped tables key rows by a big-endian (scope, id) composite"
```

---

### Task 5: Prefix-bounded `scan` and `scan_from`

**Shipping unit: Tasks 4-6 land together.**

**Files:**
- Modify: `crates/charpente-core/src/table.rs:957-969` (`scan`), `:1011-1023` (`scan_from`), `:1116-1144` (`scan_raw_from`)
- Test: `crates/charpente-core/tests/engine.rs`

**Interfaces:**
- Consumes: `scoped_key` (Task 4).
- Produces: scope-bounded ranges that Task 7's `ScopedTx` calls into.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn a_scoped_scan_cannot_see_another_tenants_rows() {
    // Two tenants, interleaved ids, so an off-by-one in the range bound shows up.
    // tenant 1 writes ids 1,3,5 ; tenant 2 writes ids 2,4,6
    // scan under tenant 1 must return exactly 1,3,5.
}
```

Write it against the real `Db`/`Writer` API — read `crates/charpente-core/tests/engine.rs` for the established setup idiom and follow it rather than inventing one.

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p charpente-core --test engine a_scoped_scan_cannot_see_another_tenants_rows 2>&1 | tee /tmp/t5.log
```

Expected: FAIL — the scan returns all six rows, because `scan` is still `range(0u64..=u64::MAX)`.

- [ ] **Step 3: Bound the ranges**

`scan` for a scoped table becomes `range(scoped_key(s, 0)..=scoped_key(s, u64::MAX))`. `scan_from(from_id, limit)` becomes `range(scoped_key(s, from_id)..=scoped_key(s, u64::MAX)).take(limit)`. `scan_raw_from` (the migration path) learns the key shape but stays scope-agnostic — it must continue to see every row of a table across all tenants, since `migrate_schema()` rewrites all of them.

- [ ] **Step 4: Run it**

```bash
cargo test -p charpente-core 2>&1 | tee /tmp/t5b.log; echo "exit=${pipestatus[1]}"
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/charpente-core/src/table.rs crates/charpente-core/tests/engine.rs
git commit -m "feat(core): scoped scan and scan_from are prefix-bounded ranges"
```

---

### Task 6: The index prefix and index maintenance

**Shipping unit: Tasks 4-6 land together. This is the task that closes the leak — without it `scan_by` reads across tenants on a table the types certify as scoped.**

**Files:**
- Modify: `crates/charpente-core/src/table.rs:354-361` (`index_entry`), `:736-780` (maintenance), `:971-1006` (`scan_by`)
- Test: `crates/charpente-core/tests/engine.rs`

**Interfaces:**
- Consumes: `scoped_key` (Task 4).
- Produces: `[scope u64 be][u32 le keylen][key][id u64 be]` for scoped tables. Task 8's property asserts against it.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn a_scoped_index_lookup_cannot_see_another_tenants_rows() {
    // Both tenants index the SAME key value — e.g. two invoices both with
    // status "open", one per tenant. This is the case a missing prefix leaks.
    // scan_by(status, "open") under tenant 1 must return exactly tenant 1's row.
}
```

The shared key value is the point: if both tenants use distinct index keys, a missing prefix passes by accident.

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p charpente-core --test engine a_scoped_index_lookup_cannot_see_another_tenants_rows 2>&1 | tee /tmp/t6.log
```

Expected: FAIL — returns both tenants' rows.

- [ ] **Step 3: Prefix the index entry**

Extend `index_entry` to take an optional scope, producing `[scope be][u32 le keylen][key][id be]` for scoped tables and the existing layout unchanged for unscoped ones. Update index maintenance in `Writer::put`/`delete` so the old entry removed and the new one inserted both carry the prefix, and bound `scan_by` to `scope_prefix ++ index_entry(key, 0) ..= scope_prefix ++ index_entry(key, u64::MAX)`.

- [ ] **Step 4: Run it, then the whole package**

```bash
cargo test -p charpente-core 2>&1 | tee /tmp/t6b.log; echo "exit=${pipestatus[1]}"
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/charpente-core/src/table.rs crates/charpente-core/tests/engine.rs
git commit -m "feat(core): scoped index keys carry the scope prefix, closing the scan_by leak"
```

---

### Task 7: `ScopedTx`, the `Tx` bound split, and `tx.scope()`

**Files:**
- Modify: `crates/charpente-core/src/db.rs:566-640` (the `View` impl for `Tx`), and the `Tx` struct at `:480-486`
- Create: the `ScopedTx` type in `crates/charpente-core/src/db.rs`
- Test: `crates/charpente-macros/tests/trybuild/`, `crates/charpente-core/tests/engine.rs`

**Interfaces:**
- Consumes: `Row::Scope` (2), `TenantScope` (3), the bounded reads (5, 6).
- Produces: `Tx::scope<S: TenantScope>(&mut self, scope: Id<S>, ctx: &Ctx) -> Result<ScopedTx<'_, S>, ApiError>`, and `ScopedTx<'_, S>: Deref<Target = Tx>`. Tasks 9, 10 and 11 all use it.

- [ ] **Step 1: Write the failing trybuild case — the headline claim**

`crates/charpente-macros/tests/trybuild/scoped_read_from_bare_tx.rs`: a handler that calls `tx.scan::<Invoice>()` on a bare `&mut Tx` where `Invoice` is `scoped_by = Org`.

- [ ] **Step 2: Run it and watch it fail to compile**

```bash
cargo test -p charpente-macros --test trybuild 2>&1 | tee /tmp/t7.log
```

Expected: currently it **compiles**, which is the bug this task fixes. Confirm that before proceeding — if it already fails, the bound is coming from somewhere unintended.

- [ ] **Step 3: Split the bounds**

Narrow `impl View for Tx` so its methods bound `R: Row<Scope = Unscoped>`, and add:

```rust
pub struct ScopedTx<'a, S> { tx: &'a mut Tx<'a>, scope: Id<S> }

impl<'a, S: TenantScope> ScopedTx<'a, S> {
    // get / scan / scan_from / scan_by, each bounding R: Row<Scope = S>
    // and threading self.scope into the ranges from Tasks 5 and 6.
}

impl<'a, S> core::ops::Deref for ScopedTx<'a, S> {
    type Target = Tx<'a>;
    fn deref(&self) -> &Tx<'a> { self.tx }
}
```

`Deref` is required, not optional: opening a scope must not lock a handler out of unscoped tables (spec §1).

- [ ] **Step 4: Implement `scope()` so the check cannot be skipped**

```rust
impl<'a> Tx<'a> {
    pub fn scope<S: TenantScope>(&mut self, scope: Id<S>, ctx: &Ctx)
        -> Result<ScopedTx<'_, S>, ApiError>
    {
        S::authorize(self, ctx.principal(), scope)?;
        Ok(ScopedTx { tx: self, scope })
    }
}
```

`authorize` runs here and only here — that is the single auditable line per handler the spec's thesis rests on.

- [ ] **Step 5: Pin the error text, and read it**

```bash
TRYBUILD=overwrite cargo test -p charpente-macros --test trybuild 2>&1 | tee /tmp/t7b.log
```

Add `#[diagnostic::on_unimplemented]` on the scoped `View` bound until the message names the table, says it is scoped, and shows `tx.scope::<Org>(org, ctx)?` as the fix. Hard rule 5 is a test, not a preference.

- [ ] **Step 6: Add the cross-scope trybuild case**

A second case: `ScopedTx<OrgA>` passed where `ScopedTx<OrgB>` is expected. Confirm it fails to compile and pin its `.stderr`.

- [ ] **Step 7: Commit**

```bash
git add crates/charpente-core/src/db.rs crates/charpente-macros/tests/trybuild
git commit -m "feat(core): scoped tables are reachable only through ScopedTx, by compile error"
```

---

### Task 8: The flagship sim property, its naive victim, and the `require:` keys

**This lands before the overlay change on purpose.** Task 9 is the risky edit; the property is the instrument that tells us whether it broke anything, so it must exist and be known-fireable first.

**Files:**
- Create: `crates/charpente-sim/tests/tenant.rs`
- Modify: `crates/charpente-sim/src/checks.rs` (if the `require:` key registry lives there — read it first)

**Interfaces:**
- Consumes: `ScopedTx` (7), the bounded reads (5, 6).
- Produces: the property Task 9's bench gate and Task 12's wire oracle are read against.

- [ ] **Step 1: Read how an existing property is written**

```bash
sed -n '1,80p' crates/charpente-sim/tests/crash_recovery.rs
grep -rn 'require:' crates/charpente-sim/src/ | head -20
```

Follow the established shape. Do not invent a new harness idiom.

- [ ] **Step 2: Write the property**

`crates/charpente-sim/tests/tenant.rs`: N ≥ 2 tenants each write rows carrying a tenant-unique marker. Under the full fault matrix — crash/restart between any two writes, torn writes, failed syncs — assert after every recovery that **every** scoped read returns only its own tenant's markers. It must exercise all four read paths:

```rust
// get     — a foreign (scope, id) must be absent, not merely filtered
// scan    — Task 5's range bound
// scan_from — the paginated bound, including from_id past the tenant's last row
// scan_by — Task 6's index prefix, with BOTH tenants indexing the same key value
```

`scan_by` with a shared index key is the case that catches a forgotten prefix; without it the property certifies less than it appears to.

- [ ] **Step 3: Add the `require:` keys**

```rust
// A single-tenant universe CANNOT detect a leak — every row legitimately
// belongs to the only tenant, so the property passes vacuously. This key
// makes a batch red if that is all that ever happened.
require: "tenant:multi_tenant_write"
require: "tenant:crash_mid_batch"   // the staging/apply boundary Task 9 edits
```

- [ ] **Step 4: Run it and record seeds**

```bash
cargo test -p charpente-sim --test tenant 2>&1 | tee /tmp/t8.log; echo "exit=${pipestatus[1]}"
```

Expected: PASS. **Record the seed count in the commit message** (hard rule 3).

- [ ] **Step 5: Build the naive victim and watch the property go red**

This step is the whole point of the task. Under a test-only cfg, add a victim that omits the index scope prefix, and a second that drops `scan`'s range bound. Each must fail the property **by name** — matching the credential property's twin described in DESIGN §8.

```bash
cargo test -p charpente-sim --test tenant --features naive_victim 2>&1 | tee /tmp/t8b.log
```

Expected: FAIL, naming the tenant that leaked and the seed. If the victim passes, the property is unfireable and certifies nothing — stop and fix the property, not the victim.

- [ ] **Step 6: Commit**

```bash
git add crates/charpente-sim/tests/tenant.rs crates/charpente-sim/src/checks.rs
git commit -m "test(sim): tenant isolation property over N seeds, with its naive victim

Both victims (unprefixed index write, unbounded scan) fail the property by name.
Seed count: <N>. Pinned seeds: <list>."
```

---

### Task 9: The overlay widening, behind the pre-authorized bench gate

**Files:**
- Modify: `crates/charpente-core/src/db.rs:346-352` (`GroupOverlay`) and every merge site that consumes it
- Test: the Task 8 property; `docs/BENCH.md` protocol

**Interfaces:**
- Consumes: everything above.
- Produces: staged writes keyed by `(scope, id)`.

- [ ] **Step 1: Find every overlay consumer before touching the type**

```bash
grep -rn 'GroupOverlay\|overlay' crates/charpente-core/src crates/charpente/src | tee /tmp/t9-sites.log
```

- [ ] **Step 2: Widen the key**

`BTreeMap<&'static str, BTreeMap<(u64, u64), Option<Vec<u8>>>>`, slot 0 for unscoped tables. Update every merge site found in Step 1.

- [ ] **Step 3: Run the property and the kernel suites**

```bash
cargo test -p charpente-core 2>&1 | tee /tmp/t9a.log; echo "exit=${pipestatus[1]}"
cargo test -p charpente-sim --test tenant 2>&1 | tee /tmp/t9b.log; echo "exit=${pipestatus[1]}"
cargo test -p charpente 2>&1 | tee /tmp/t9c.log; echo "exit=${pipestatus[1]}"
```

- [ ] **Step 4: Re-run the kernel layer on the BENCH.md protocol**

Read `docs/BENCH.md` §Method before running anything, and follow it exactly — including what invalidates a run. Measure CPU time, not wall clock.

Compare against the per-batch-apply numbers of record. **State the expected result before you look:** the prediction is "inside the established bracket".

- [ ] **Step 5: Apply the gate**

- **Inside the bracket** → keep the widened key. Record the numbers in the commit message; they are an observation, and only `docs/BENCH.md` makes them quotable.
- **Outside the bracket** → **take the pre-authorized fallback** (spec §2): revert to the original map and add a parallel scoped overlay, leaving the unscoped hot path untouched. Re-run Step 4. No further sign-off is needed for the fallback; taking it is not a failure, it is the decision rule firing.

- [ ] **Step 6: Commit**

```bash
git add crates/charpente-core/src/db.rs
git commit -m "feat(core): staging overlay keys by (scope, id)

Kernel layer re-run on the BENCH.md protocol: <numbers>. <Inside bracket | fallback taken>."
```

---

### Task 10: `across_tenants`, the `cross_tenant` capability, and the describe surface

**Files:**
- Modify: `crates/charpente-macros/src/{route,job}.rs`, `crates/charpente/src/http/{app,router,mod}.rs`, `crates/charpente-core/src/db.rs`
- Test: `crates/charpente-macros/tests/trybuild/`, `crates/charpente/tests/describe.rs`

**Interfaces:**
- Consumes: `ScopedTx` (7).
- Produces: `#[route(…, cross_tenant)]`, `Ctx::cap() -> Option<&CrossTenant>`, and
  `Tx::across_tenants<S>(&self, cap: &CrossTenant) -> Result<AllTenants<'_, S>, ApiError>` whose `scan` yields `Vec<(Id<S>, R)>`.

- [ ] **Step 1: Write the failing trybuild case**

A handler calling `across_tenants` from a route **without** `cross_tenant`. Expected: no capability token in scope, so it must not compile.

- [ ] **Step 2: Write the failing describe test**

In `crates/charpente/tests/describe.rs`: mount an app with one `cross_tenant` route and one ordinary route; assert `describe` output names exactly the first. This is what turns the audit into an inventory.

- [ ] **Step 3: Run both and watch them fail**

```bash
cargo test -p charpente-macros --test trybuild 2>&1 | tee /tmp/t10.log
cargo test -p charpente --test describe 2>&1 | tee /tmp/t10b.log
```

- [ ] **Step 4: Implement**

Add `cross_tenant` to the route/job attribute parsers; have the glue construct a `CrossTenant` token and expose it via `Ctx::cap()`. `across_tenants` takes `&CrossTenant` and returns rows **paired with their scope**:

```rust
// Vec<(Id<Org>, Invoice)>, never Vec<Invoice> — the tenant rides on every
// row so a cross-tenant result cannot be handed to code expecting one
// tenant's data (spec §3).
```

Add the flag to the describe output beside the existing `auth` flag.

- [ ] **Step 5: Run both to green, pin the trybuild `.stderr`, and read it**

- [ ] **Step 6: Commit**

```bash
git add crates/charpente-macros/src crates/charpente/src/http crates/charpente-core/src/db.rs crates/charpente-macros/tests/trybuild crates/charpente/tests/describe.rs
git commit -m "feat: cross-tenant enumeration is a declared route capability, listed by describe"
```

---

### Task 11: Scope-keyed event emission

**Files:**
- Modify: `crates/charpente-core/src/db.rs:549-563` (`emit`/`emit_keyed`)
- Test: `crates/charpente/tests/realtime.rs`

**Interfaces:**
- Consumes: `ScopedTx` (7).
- Produces: `emit` from within a `ScopedTx<S>` auto-keys with the scope.

- [ ] **Step 1: Write the failing test**

In `crates/charpente/tests/realtime.rs`: a subscription keyed to tenant A must not receive an event emitted inside `ScopedTx<Org>(B)`. Follow the existing subscription test setup in that file.

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p charpente --test realtime 2>&1 | tee /tmp/t11.log
```

Expected: FAIL — the event arrives, because `emit` is currently unkeyed and an unkeyed event reaches no keyed subscription… **verify which way it fails.** Per the GUIDE, a keyed subscription does *not* receive unkeyed events, so the naive failure may be "A receives nothing" rather than "A receives B's event". Read the failure before writing the fix; both are wrong, but they are wrong differently.

- [ ] **Step 3: Implement**

`ScopedTx::emit` delegates to `emit_keyed` with the scope's string encoding. One tenancy mechanism, not two (spec §4).

- [ ] **Step 4: Run to green, then run the tenant property again**

```bash
cargo test -p charpente --test realtime 2>&1 | tee /tmp/t11b.log; echo "exit=${pipestatus[1]}"
cargo test -p charpente-sim --test tenant 2>&1 | tee /tmp/t11c.log; echo "exit=${pipestatus[1]}"
```

- [ ] **Step 5: Commit**

```bash
git add crates/charpente-core/src/db.rs crates/charpente/tests/realtime.rs
git commit -m "feat(core): a scoped transaction's events are keyed by their scope"
```

---

### Task 12: The two-tenant wire oracle

The sim proves the kernel; the edge harness covers the wire. This is the second layer M1's exit criteria established as the bar.

**Files:**
- Create: `crates/edge-harness/tests/tenant_wire.rs`

**Interfaces:**
- Consumes: the whole stack.
- Produces: nothing downstream.

- [ ] **Step 1: Read an existing edge-harness test**

```bash
sed -n '1,60p' crates/edge-harness/tests/smoke.rs
```

- [ ] **Step 2: Write the oracle**

Stock binary over real TCP, two tenants with real sessions. Assert tenant A's session receives none of tenant B's rows over HTTP, and none of B's events over SSE. **If Task 1 landed in World A** (log-format change), add the mixed-version replication case here: an older follower must refuse by name rather than misread.

- [ ] **Step 3: Run it**

```bash
cargo test -p edge-harness --test tenant_wire 2>&1 | tee /tmp/t12.log; echo "exit=${pipestatus[1]}"
```

- [ ] **Step 4: Commit**

```bash
git add crates/edge-harness/tests/tenant_wire.rs
git commit -m "test(edge): two tenants over real TCP — no cross-tenant row or event on the wire"
```

---

### Task 13: The docs, in the same commits as the code they describe

Hard rule 6: drifted docs are bugs. This task exists because doc updates that ride along at the end are the ones that get dropped.

**Files:**
- Modify: `DESIGN.md` §3 and §8, `PLAN.md`, `docs/GUIDE.md`, `DECISIONS.md`, `CLAUDE.md` (test inventory)

- [ ] **Step 1: `DESIGN.md`**

§3's typed-surface list gains `Scope`. §8's multi-tenancy bullet stops saying "deferred to its own design" and instead records what shipped, pointing at the spec and the GUIDE section. Do not leave the old sentence standing next to the new one — that is the exact drift the last review caught.

- [ ] **Step 2: `docs/GUIDE.md`**

A tenancy section: declaring `scoped_by`, writing `TenantScope`, opening a scope, and the cross-tenant capability with its describe inventory. Any ```rust block here is compiled by `crates/guide-examples`'s build.rs — run `cargo build -p guide-examples` before committing.

- [ ] **Step 3: `PLAN.md`**

Move the M2 tenant-scoping bullet to shipped, in the voice the other shipped bullets use — what landed, what limit travels with it.

- [ ] **Step 4: `DECISIONS.md`**

New entry recording spec §3's hard-rule-2 reading (`across_tenants` is a declared capability, not an escape hatch), so it binds future work. Also act on spec §10: define what counts as a redb call site and record the count with the definition, replacing the stale "~30".

- [ ] **Step 5: `CLAUDE.md`**

Add `tenant` to `charpente-sim`'s test list and `tenant_wire` to `edge-harness`'s. The `doc_inventory` gate enforces this — it will fail if you forget.

- [ ] **Step 6: Full gate, then commit**

```bash
cargo fmt --all --check; echo "fmt=$?"
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tee /tmp/t13a.log; echo "clippy=${pipestatus[1]}"
cargo build -p guide-examples 2>&1 | tee /tmp/t13b.log; echo "guide=${pipestatus[1]}"
cargo test -p guide-examples 2>&1 | tee /tmp/t13c.log; echo "inv=${pipestatus[1]}"
```

```bash
git add DESIGN.md PLAN.md docs/GUIDE.md DECISIONS.md CLAUDE.md
git commit -m "docs: tenant scoping ships — DESIGN §3/§8, GUIDE, PLAN, DECISIONS"
```

---

## Self-Review

**Spec coverage.** §1 → Tasks 2, 3, 7. §2 → Tasks 4, 5, 6, 9, and the WAL question → Task 1. §3 → Task 10. §4 → Task 11. §5 → Tasks 3, 7, 8, 10, 12 (compile-fail matrix across 3/7/10; property and victim in 8; wire in 12). §6 → Task 13. §7's ten exit criteria: 1→7, 2→3, 3→8, 4→8, 5→12, 6→11, 7→10, 8→9, 9→4 step 5, 10→13 step 6. §8 non-goals introduce no tasks by design. §9's rulings are settled in the spec. **No gaps.**

**Placeholder scan.** Three tests are written as intent plus a comment rather than literal code — Task 4 step 5, Task 5 step 1, Task 6 step 1 — because each must be written against `engine.rs`'s established `Db`/`Writer` setup idiom, which the executor is told to read first. Every one states its assertion and the specific trap it exists to catch (stored bytes not a round-trip; interleaved ids; a shared index key). That is a deliberate instruction to follow local convention, not an unfilled blank.

**Type consistency.** `Unscoped` (2), `Row::Scope` (2, used 3/4/7), `TenantScope::authorize` (3, called 7), `scoped_key` (4, used 5/6), `ScopedTx`/`Tx::scope` (7, used 9/10/11), `CrossTenant`/`Ctx::cap` (10) — names match across tasks.

**One risk the executor must not smooth over.** Task 2 step 3 has a real fork: if `associated_type_defaults` is unstable on the pinned toolchain, the default moves into the macro and `Row` declares `type Scope;` undefaulted — which means a hand-written `Row` impl (the platform's own `JobRow`/`EventRow`/`SchemaVersion`) must name it explicitly. Record which branch was taken; Task 3 depends on it.
