# TS Client Codegen Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend charpente's compile-time contract across the process boundary — change a Rust wire type, and a TypeScript build goes red.

**Architecture:** A `TypeSchema` IR and a `Wire` trait give `Contract` a type system; `#[derive(Wire)]` and the existing `#[route]`/`#[event]` macros populate it during the runtime `App` walk that already collects topology; a pure, deterministic emitter turns `Contract` into TypeScript; and a checked-in client plus `charpente check-client` makes staleness a red build.

**Tech Stack:** Rust (proc macros via `syn`/`quote`), serde/serde_json, TypeScript 5.x + `tsc --noEmit`, Node in CI.

**Spec:** `docs/superpowers/specs/2026-08-20-ts-client-codegen-design.md` — read it first. This plan argues from it; where they disagree, the spec wins and the plan is the bug.

## Global Constraints

- **Read `CLAUDE.md` before writing code.** Its hard rules override default behavior.
- **No ambient nondeterminism.** The emitter is a pure function of `Contract`. No clocks, no RNG, no filesystem reads inside it, no `HashMap` iteration order — `BTreeMap`/sorted `Vec` only. Generation must be byte-identical across runs and machines (spec §4).
- **No new dependency inside the deterministic boundary** — meaning CLAUDE.md's rule: nothing
  that does its own IO, threading, or time. **AMENDED at Task 2 (Ruling 13):** `charpente-core`
  gains `serde` (no `derive` feature). Rust's orphan rule forces `Serialize`/`Deserialize` for
  `Id<T>` into the crate that owns `Id<T>`, and serde's traits are pure computation — no IO, no
  threading, no time. serde is already in this workspace's build graph (`charpente/Cargo.toml:11`,
  `Cargo.lock:528`), so this adds a manifest line, not third-party code. `Codec` stays hand-rolled:
  serde must never become the on-disk format, and a test pins the 8-byte little-endian encoding
  so that stays mechanical. No OTHER core dependency is in scope.
  **Correction (Task 2 review):** the `default-features = false` / no-`derive` declaration is NOT
  a fence — cargo unifies features and `charpente/Cargo.toml:11` asks for `serde/derive`, so a
  `#[derive(Serialize)]` inside core builds fine under `--workspace`, which is all CI ran. The
  fence is the CI line `cargo check -p charpente-core`, added in Task 2's fix round. Hard rule 1
  says mechanically, not by manifest comment.
  **Nearest prior art, which does NOT bind but should be cited rather than missed:**
  `DESIGN.md:1097-1098` — "the crypto is RustCrypto (`sha2`, `rsa`, `base64ct`) ... and lives in
  `charpente`, never inside the deterministic `charpente-core`." That is about libraries with an
  ambient surface (`rsa` draws randomness); serde's traits execute no IO, threading, or time, and
  `cargo tree -p charpente-core` resolves to `redb` + `serde`/`serde_core` with no proc-macro and
  no build script. Distinguished on the mechanism, not waved past. The emitter crate may depend only on `serde`/`serde_json` and `charpente` (for `Contract`).
- **Error messages are product surface** (hard rule 5). Every diagnostic names the contract, the site, and the fix, and is pinned by a test against its exact text.
- **Public-API changes update docs in the same commit** (hard rule 6).
- **Assertions in the TigerBeetle style** (hard rule 4) — assert negative space in core code.
- **Commit directly to `main`. No Co-Authored-By trailers.** `git add` named paths only, never `-A` or `.` — other sessions share this working tree.
- **zsh:** `PIPESTATUS` does not work. Use `${pipestatus[1]}` or `set -o pipefail`. Pipe cargo
  through `tee` — **never `tail`, `grep`, or `head`.** `tail` eats the aggregate counts; `grep`
  under `pipefail` reports failure when it simply matches nothing (a reviewer got a false
  `fmt exit=1` on a clean `cargo fmt` that way, and discarded a good run over it). The rule is
  not "avoid tail" — it is that **the last command in the pipe owns `$?`**, so the last command
  must be one whose success means what you think it does.

- **Full check before any task is done:** `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`. No `#[allow]` may be added to pass clippy.
- **Do not kill any process.** Another repo runs test suites on this machine.
- **A tool's DEFAULT is not a tool's LIMIT — check before building a workaround.** Three
  separate hazards in this plan trace to one root: the Bash tool's 2-minute default timeout
  was read as a hard ceiling. Task 5's implementer concluded `cargo test --workspace` "could
  not run as one command" and segmented the entire suite per-package/per-file; fix round 1's
  implementer backgrounded the same command, parked waiting on a notification a subagent never
  receives, and returned control with 19 paths uncommitted. **The tool accepts `timeout` up to
  600000 ms.** **CORRECTED 2026-08-26 by measurement, twice — read both numbers, because one
  alone misleads.** `cargo test --workspace` timed as a single command:
  - **2482.86 s wall (41.4 min)** at `1ac64c9` — cold-ish, with a concurrent session building in
    a sibling worktree. 789 s user CPU, so 32% utilisation: mostly waiting, not computing.
  - **568 s wall** at `af14e74` — warm target dir, machine otherwise quiet. Exit 0, 63 suites,
    968 tests.

  So the honest rule is **not** "it takes N minutes". It is: **the suite's wall time swings by
  more than 4x with build warmth and machine contention, straddling the tool's 600 s hard
  maximum, so it cannot be RELIED on to finish inside one Bash call.** Partition per package, or
  background it from the top-level agent (which gets re-invoked on completion — a mechanism
  subagents lack). Task 9a's implementer partitioned and was right to.
  "About six minutes" was wrong in the dangerous direction: it attached a confident number to a
  bullet whose whole point is checking limits rather than assuming them, and a later implementer
  had to defend correct practice against it. **Do not replace it with a different confident
  number.** Wall time here is a scheduling fact, never a benchmark — docs/BENCH.md remains the
  only source of quotable performance figures.
  **The generalisation below still stands, and so does its inverse: check the ceiling before
  designing around it, AND check it before insisting the ceiling is not there.** Each workaround produced evidence that was
  equivalent-or-worse at the cost of a whole session, plus a deviation for a reviewer to audit.
  The rule generalizes past this one parameter: when a tool appears to refuse something, read
  its parameters before designing around the refusal — and if you do work around it, say so in
  the report as a deviation, which both implementers did correctly.
- **`TRYBUILD=overwrite` blesses whatever the compiler emitted — it does not verify it.**
  The cargo test target in `charpente-macros` is `--test trybuild` (`tests/trybuild.rs`,
  fn `macro_errors`); `compile_fail/` and `compile_pass/` are fixture DIRECTORIES, not targets.
  Use `overwrite` only to create a new fixture, then READ the generated `.stderr` and confirm it
  says what the task requires before committing it. An unread blessed `.stderr` is a test that
  cannot fail. Applies to Tasks 1, 3, and 6.
- **A negative assertion owes a floor or a mutation; an identity assertion owes both.**
  Most of this plan's oracles are negative ("this must not compile", "no drift", "byte-identical")
  and a test asserting something never happens passes trivially against an implementation that
  never does anything. `assert_eq!(emit(c), emit(c))` is green when `emit` returns `String::new()`.
  House precedent is explicit (`55f15ec`): byte-oracle coverage sat at 17 of 256 and "2 of 256
  would still have passed", which is why `floor:` exists. So:
  - **Every `compile_fail/` fixture owes a `compile_pass/` control** — the same shape with the
    one offending element made legal, proving the rejection is the stated cause and not an
    unrelated bound, import, or typo. Task 1's reviewer ran this ad hoc (`u32` compiles clean,
    exit 0); it belongs in the plan so Tasks 3 and 6 repeat it.
  - **Every byte-identity test asserts a floor before it asserts equality** — non-trivial length
    plus named required substrings. Applies to `emission_is_byte_identical_across_runs` and
    `gen_client_is_byte_identical_on_a_second_run`.
  - **Every "never happens" assertion names the mutation that reddens it**, in a comment on the
    test. If no such mutation exists, the test is decorative — delete it or make it real.
  - **A mutation that leaves the suite GREEN has proved nothing until you show the mutation was
    not inert.** RED is self-proving: the mutation clearly changed behaviour. GREEN is ambiguous
    between "the test is vacuous" and "my mutation did nothing," and those look identical from the
    exit code alone. Learned the hard way at Task 2 (F4): a reviewer mutated `Id<T>`'s Visitor by
    adding `visit_u64`, saw green, and filed the test as vacuous — but `serde_json`'s
    `deserialize_str` is type-strict (`serde_json-1.0.150/src/de.rs:1524-1550`: only a `"` token
    reaches the visitor, everything else short-circuits into `peek_invalid_type`), so `visit_u64`
    was unreachable dead code and the mutation was a no-op. **So: when a mutation leaves the suite
    green, print the observable behaviour under the mutation before concluding anything** — the
    parsed value, the error text, a `dbg!` on the mutated path. An exit code is not a measurement
    of the thing you changed.
- **Pinning the function you fixed is not pinning the behaviour you fixed.** Four times in this
  plan a test has covered a function while leaving the property it exists to protect uncovered:
  Task 1's `reference()` tested in isolation while every container caller ignored it (a genuine
  stack overflow, found two tasks later); Task 3's serde fidelity, which no test compared
  against real `serde_json` output until a reviewer wrote a probe; Task 3's `register()`
  recursion guard, green under `if false`; and the F4 rejection, pinned against an inert
  mutation. **After fixing a bug, ask what the bug would have DONE, and pin that** — the
  overflow, the wrong JSON key, the non-termination — not the function you happened to edit.
- **A test named `fidelity_*` must read BOTH sides.** Twice now a fidelity test has asserted
  only what `serde_json` emitted and never called `<T as Wire>::schema()` — so no edit to
  charpente could redden it, while its own comment claimed a mutation it does not catch.
  Task 3 fixed one; Task 4 grew another, and the one it grew was pinning the subtlest rule in
  the new variant table (serde tests uppercase with Unicode rules but lowercases with ASCII
  ones). A ground-truth pin on serde's behaviour is legitimate — it catches a serde upgrade —
  but it must be **labelled as one in a comment**, never left looking like coverage.
- **Do not touch `Cargo.toml:38-39`** (`[profile.release.package.redb] debug-assertions = true`).

## File Structure

| file | responsibility |
|---|---|
| `crates/charpente/src/wire.rs` **(new)** | The `TypeSchema` IR, `TypeDef`, `Field`, `EnumRepr`, `TypeRegistry`, and the `Wire` trait with its primitive impls. One responsibility: describing wire shapes. |
| `crates/charpente/src/describe.rs` | `Contract` gains `types` and `events`; `RouteInfo` gains type refs. Stays the assembly point only. |
| `crates/charpente-core/src/table.rs` | `Id<T>` serde impls (string-encoded). Core-side only; the `Wire` impl lives in `charpente`. |
| `crates/charpente/src/http/router.rs` | `Route` carries type refs so the walk can see them. |
| `crates/charpente-macros/src/wire.rs` **(new)** | `#[derive(Wire)]` — struct and enum expansion, serde-attribute fidelity. |
| `crates/charpente-macros/src/route.rs` | Type capture + `emits(...)` parsing. |
| `crates/charpente-macros/src/event.rs` | Describe-time registration. |
| `crates/charpente-tsgen` **(new crate)** | `Contract -> String`. Pure, deterministic, no IO. Split: `types.rs` (type emission), `routes.rs` (call functions), `events.rs` (subscribe API), `runtime.rs` (the embedded fetch/SSE runtime as a const str). |
| `crates/charpente-cli/src/genclient.rs` **(new)** | `gen-client` / `check-client` verbs. |
| `ts-fixture/` **(new)** | The dogfood target: package.json, tsconfig, generated client, call sites, generated oracles. |

---

### Task 1: The `TypeSchema` IR, the `Wire` trait, and the 64-bit diagnostic

**Files:**
- Create: `crates/charpente/src/wire.rs`
- Modify: `crates/charpente/src/lib.rs` (add `pub mod wire;`)
- Test: `crates/charpente/tests/wire.rs` (new)

**Interfaces:**
- Consumes: nothing.
- Produces: `TypeSchema`, `TypeDef { name: &'static str, schema: TypeSchema }`, `Field { name: String, schema: TypeSchema, optional: bool }`, `EnumRepr`, `TypeRegistry`, and `trait Wire { const WIRE_NAME: &'static str; fn schema() -> TypeSchema; fn register(reg: &mut TypeRegistry); }`. Tasks 2-8 all consume these.

- [ ] **Step 1: Write the failing test**

Create `crates/charpente/tests/wire.rs`:

```rust
use charpente::wire::{TypeRegistry, TypeSchema, Wire};

#[test]
fn primitives_describe_themselves() {
    assert_eq!(<bool as Wire>::schema(), TypeSchema::Bool);
    assert_eq!(<u32 as Wire>::schema(), TypeSchema::U32);
    assert_eq!(<String as Wire>::schema(), TypeSchema::String);
}

#[test]
fn option_and_seq_nest() {
    assert_eq!(
        <Option<String> as Wire>::schema(),
        TypeSchema::Option(Box::new(TypeSchema::String))
    );
    assert_eq!(
        <Vec<u32> as Wire>::schema(),
        TypeSchema::Seq(Box::new(TypeSchema::U32))
    );
}

#[test]
fn the_registry_dedups_by_name() {
    let mut reg = TypeRegistry::default();
    <Vec<String> as Wire>::register(&mut reg);
    <Option<String> as Wire>::register(&mut reg);
    // primitives contribute no named defs
    assert!(reg.into_sorted().is_empty());
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p charpente --test wire 2>&1 | tee /tmp/t1.log; echo "exit=${pipestatus[1]}"
```
Expected: FAIL — `unresolved import charpente::wire`.

- [ ] **Step 3: Write `crates/charpente/src/wire.rs`**

```rust
//! The wire type IR (TS-codegen spec §1). Describes what a value looks like
//! after serde has encoded it — field names here are SERDE names, never Rust
//! names, because the wire shape is serde's to decide.

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum TypeSchema {
    Bool,
    I8, I16, I32,
    U8, U16, U32,
    F32, F64,
    String,
    /// A 64-bit integer. ALWAYS string-encoded on the wire: JSON numbers lose
    /// precision above 2^53 in JavaScript, and a `JSON.parse` reviver cannot
    /// recover it (the value is already parsed when the reviver runs).
    Int64 { signed: bool },
    /// A typed row id — the str is the table name, which the emitter turns
    /// into a branded string type.
    Id(&'static str),
    Option(Box<TypeSchema>),
    Seq(Box<TypeSchema>),
    Map(Box<TypeSchema>),
    Struct { fields: Vec<Field> },
    Enum(EnumRepr),
    /// A reference into `Contract.types`.
    Ref(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Field {
    pub name: String,
    pub schema: TypeSchema,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum EnumRepr {
    /// All variants are unit variants: a string union.
    Unit { variants: Vec<String> },
    /// serde's externally-tagged default: `{ "VariantName": {...} }`.
    External { variants: Vec<(String, TypeSchema)> },
    /// `#[serde(tag = "...")]`: `{ "type": "VariantName", ...fields }`.
    Internal { tag: String, variants: Vec<(String, TypeSchema)> },
    /// `#[serde(tag = "...", content = "...")]`.
    Adjacent { tag: String, content: String, variants: Vec<(String, TypeSchema)> },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TypeDef {
    pub name: &'static str,
    pub schema: TypeSchema,
}

/// Collects the transitive closure of named types a contract references.
/// `BTreeMap`, not `HashMap`: generation must be byte-deterministic (spec §4).
#[derive(Default)]
pub struct TypeRegistry {
    defs: BTreeMap<&'static str, TypeSchema>,
}

impl TypeRegistry {
    /// Returns true if this name was already present — the recursion guard.
    /// Without it a self-referential type registers forever.
    pub fn insert(&mut self, name: &'static str, schema: TypeSchema) -> bool {
        self.defs.insert(name, schema).is_some()
    }

    pub fn contains(&self, name: &str) -> bool {
        self.defs.contains_key(name)
    }

    pub fn into_sorted(self) -> Vec<TypeDef> {
        self.defs
            .into_iter()
            .map(|(name, schema)| TypeDef { name, schema })
            .collect()
    }
}

/// A type that can cross the wire.
///
/// There is deliberately NO impl for `u64` or `i64` — see the message below
/// and spec §1. `WireU64`/`WireI64`/`Id<T>` are the sanctioned carriers.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot cross the wire",
    label = "no `Wire` impl for `{Self}`",
    note = "if this is `u64` or `i64`: values above 2^53 lose precision in JavaScript clients, silently — use `Id<T>` for a row id, or `WireU64`/`WireI64` to carry it as a string",
    note = "otherwise: add `#[derive(Wire)]` to `{Self}`"
)]
pub trait Wire {
    /// The name this type appears under in `Contract.types`. Primitives use
    /// "" and contribute no named def.
    const WIRE_NAME: &'static str;
    fn schema() -> TypeSchema;
    /// Register this type and everything it references.
    fn register(reg: &mut TypeRegistry);
}

macro_rules! prim {
    ($($t:ty => $v:expr),* $(,)?) => {$(
        impl Wire for $t {
            const WIRE_NAME: &'static str = "";
            fn schema() -> TypeSchema { $v }
            fn register(_reg: &mut TypeRegistry) {}
        }
    )*};
}

prim! {
    bool => TypeSchema::Bool,
    i8 => TypeSchema::I8, i16 => TypeSchema::I16, i32 => TypeSchema::I32,
    u8 => TypeSchema::U8, u16 => TypeSchema::U16, u32 => TypeSchema::U32,
    f32 => TypeSchema::F32, f64 => TypeSchema::F64,
    String => TypeSchema::String,
}

impl<T: Wire> Wire for Option<T> {
    const WIRE_NAME: &'static str = "";
    fn schema() -> TypeSchema { TypeSchema::Option(Box::new(T::schema())) }
    fn register(reg: &mut TypeRegistry) { T::register(reg) }
}

impl<T: Wire> Wire for Vec<T> {
    const WIRE_NAME: &'static str = "";
    fn schema() -> TypeSchema { TypeSchema::Seq(Box::new(T::schema())) }
    fn register(reg: &mut TypeRegistry) { T::register(reg) }
}

impl<V: Wire> Wire for BTreeMap<String, V> {
    const WIRE_NAME: &'static str = "";
    fn schema() -> TypeSchema { TypeSchema::Map(Box::new(V::schema())) }
    fn register(reg: &mut TypeRegistry) { V::register(reg) }
}
```

Add to `crates/charpente/src/lib.rs`, beside the other `pub mod` lines:

```rust
pub mod wire;
```

- [ ] **Step 4: Run the tests**

```bash
cargo test -p charpente --test wire 2>&1 | tee /tmp/t1.log; echo "exit=${pipestatus[1]}"
```
Expected: PASS, 3 tests.

- [ ] **Step 5: Prove the diagnostic fires (this is the §1 guarantee, not a nicety)**

Add `crates/charpente-macros/tests/compile_fail/wire_bare_u64.rs` — note this uses the existing trybuild harness in that crate:

```rust
fn main() {
    fn needs_wire<T: charpente::wire::Wire>() {}
    needs_wire::<u64>();
}
```

Run the trybuild suite and **accept the generated `.stderr` only after reading it** — confirm it contains "values above 2^53 lose precision in JavaScript clients". If the note is missing, the `on_unimplemented` attribute is wrong; fix it rather than blessing the output.

```bash
TRYBUILD=overwrite cargo test -p charpente-macros --test trybuild 2>&1 | tee /tmp/t1-tb.log
cat crates/charpente-macros/tests/compile_fail/wire_bare_u64.stderr
```

- [ ] **Step 6: Full check and commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace 2>&1 | tee /tmp/t1-ws.log; echo "exit=${pipestatus[1]}"
git add crates/charpente/src/wire.rs crates/charpente/src/lib.rs crates/charpente/tests/wire.rs crates/charpente-macros/tests/compile_fail/wire_bare_u64.rs crates/charpente-macros/tests/compile_fail/wire_bare_u64.stderr
git commit -m "feat(wire): the type IR, and 64-bit integers are not a primitive

TypeSchema describes what serde emits, not what the Rust struct says — field
names in a Struct are serde names, because the wire shape is serde's to decide.

u64 and i64 have no Wire impl on purpose. A JSON number loses precision above
2^53 in JavaScript and a JSON.parse reviver cannot recover it, so the carriers
are string-encoded: Id<T>, WireU64, WireI64. diagnostic::on_unimplemented turns
the missing impl into the message spec §1 specifies, pinned by trybuild."
```

---

### Task 2: `Id<T>` and the 64-bit carriers cross the wire as strings

**Files:**
- Modify: `crates/charpente-core/src/table.rs` (the `Id<T>` block at :27-80)
- Modify: `crates/charpente/src/wire.rs` (add `WireU64`, `WireI64`, and `Wire for Id<T>`)
- Test: `crates/charpente/tests/wire.rs` (extend)

**Interfaces:**
- Consumes: `Wire`, `TypeSchema` from Task 1.
- Produces: `Serialize`/`Deserialize` for `Id<T>`; `pub struct WireU64(pub u64)` and `pub struct WireI64(pub i64)` in `charpente::wire`, both serde-string-encoded, both `Wire`.

- [ ] **Step 1: Write the failing test**

Append to `crates/charpente/tests/wire.rs`:

```rust
use charpente::wire::{WireI64, WireU64};
use charpente_core::table::Id;

struct Customer;

#[test]
fn an_id_is_a_json_string_and_survives_above_2_53() {
    let big = (1u64 << 53) + 1; // the first u64 a JSON number cannot hold
    let id = Id::<Customer>::from_raw(big);
    let json = serde_json::to_string(&id).unwrap();
    assert_eq!(json, format!("\"{big}\""), "an id must encode as a string");

    let back: Id<Customer> = serde_json::from_str(&json).unwrap();
    assert_eq!(back.raw(), big, "round trip must be lossless");
}

#[test]
fn wire_u64_and_i64_are_strings_and_lossless() {
    let big = u64::MAX;
    let json = serde_json::to_string(&WireU64(big)).unwrap();
    assert_eq!(json, format!("\"{big}\""));
    assert_eq!(serde_json::from_str::<WireU64>(&json).unwrap().0, big);

    let neg = i64::MIN;
    let json = serde_json::to_string(&WireI64(neg)).unwrap();
    assert_eq!(json, format!("\"{neg}\""));
    assert_eq!(serde_json::from_str::<WireI64>(&json).unwrap().0, neg);
}

#[test]
fn a_non_numeric_id_string_is_refused_by_name() {
    let err = serde_json::from_str::<Id<Customer>>("\"not-a-number\"").unwrap_err();
    assert!(
        err.to_string().contains("row id"),
        "the decode error must name what it wanted, got: {err}"
    );
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p charpente --test wire 2>&1 | tee /tmp/t2.log; echo "exit=${pipestatus[1]}"
```
Expected: FAIL — `Id<T>` does not implement `Serialize`.

- [ ] **Step 3: Add the serde impls to `crates/charpente-core/src/table.rs`**

Immediately after the existing `impl<T> Id<T>` block. Hand-written, not derived, because the representation is a string and derive would emit a number:

```rust
// A row id crosses the wire as a STRING, never a JSON number: ids are u64 and
// a JSON number silently loses precision above 2^53 in JavaScript clients
// (TS-codegen spec §1). This is a wire-format decision, not a style one — do
// not "simplify" it to a derive.
impl<T> serde::Serialize for Id<T> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&self.raw)
    }
}

impl<'de, T> serde::Deserialize<'de> for Id<T> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V<T>(std::marker::PhantomData<T>);
        impl<T> serde::de::Visitor<'_> for V<T> {
            type Value = Id<T>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a row id as a decimal string")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Id<T>, E> {
                v.parse::<u64>().map(Id::from_raw).map_err(|_| {
                    E::custom(format!(
                        "row id must be a decimal string — got {v:?}; ids cross the wire as \
                         strings because a JSON number loses precision above 2^53"
                    ))
                })
            }
        }
        d.deserialize_str(V(std::marker::PhantomData))
    }
}
```

- [ ] **Step 4: Add the carriers and the `Wire` impls to `crates/charpente/src/wire.rs`**

```rust
/// Carries a `u64` across the wire as a decimal string. The generated TS type
/// is `bigint`, parsed with `BigInt(str)` — lossless, unlike a JSON number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireU64(pub u64);

/// The signed sibling of `WireU64`; same reasoning, same encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireI64(pub i64);

macro_rules! wire_int {
    ($t:ident, $inner:ty, $signed:expr, $what:expr) => {
        impl serde::Serialize for $t {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_str(&self.0)
            }
        }
        impl<'de> serde::Deserialize<'de> for $t {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let s = <&str as serde::Deserialize>::deserialize(d)?;
                s.parse::<$inner>().map($t).map_err(|_| {
                    serde::de::Error::custom(format!(
                        concat!($what, " must be a decimal string — got {:?}"),
                        s
                    ))
                })
            }
        }
        impl Wire for $t {
            const WIRE_NAME: &'static str = "";
            fn schema() -> TypeSchema { TypeSchema::Int64 { signed: $signed } }
            fn register(_reg: &mut TypeRegistry) {}
        }
    };
}

wire_int!(WireU64, u64, false, "a WireU64");
wire_int!(WireI64, i64, true, "a WireI64");

impl<T: charpente_core::table::Row> Wire for charpente_core::table::Id<T> {
    const WIRE_NAME: &'static str = "";
    fn schema() -> TypeSchema { TypeSchema::Id(T::TABLE) }
    fn register(_reg: &mut TypeRegistry) {}
}
```

> **Note for the implementer:** the `Wire for Id<T>` impl lives in `charpente`, not `charpente-core`, because `Wire` is defined in `charpente`. That is legal (the trait is local) and is why the IR did not go in core. If the `Row` bound turns out to be wrong for the `T::TABLE` access, read `crates/charpente-core/src/table.rs`'s `Row` trait and use whatever const actually carries the table name — do not invent one.

- [ ] **Step 5: Run the tests**

```bash
cargo test -p charpente --test wire 2>&1 | tee /tmp/t2.log; echo "exit=${pipestatus[1]}"
```
Expected: PASS, 6 tests.

- [ ] **Step 6: Prove nothing else depended on `Id<T>` having no serde impl**

```bash
cargo test --workspace 2>&1 | tee /tmp/t2-ws.log; echo "exit=${pipestatus[1]}"
```
Expected: exit 0. If a test breaks because something now serializes an `Id` differently, that is a real finding — report it rather than adjusting the test.

- [ ] **Step 7: Full check and commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
git add crates/charpente-core/src/table.rs crates/charpente/src/wire.rs crates/charpente/tests/wire.rs
git commit -m "feat(wire): ids and 64-bit carriers cross the wire as strings

Id<T> had no serde impl at all, so app code unwrapped to bare u64 at the view
boundary and ids went out as JSON numbers — silently corrupting above 2^53 in
any JavaScript client. The impls are hand-written rather than derived because
the representation is a string; a derive would emit a number and reintroduce
the bug.

The decode error names what it wanted and why (hard rule 5), and the tests pin
the first value a JSON number cannot hold, u64::MAX, and i64::MIN."
```

---

### Task 3: `#[derive(Wire)]` for structs, with serde fidelity

**Files:**
- Create: `crates/charpente-macros/src/wire.rs`
- Modify: `crates/charpente-macros/src/lib.rs` (register the derive)
- Test: `crates/charpente/tests/wire.rs` (extend)

**Interfaces:**
- Consumes: `Wire`, `TypeSchema`, `Field`, `TypeRegistry` from Task 1.
- Produces: `#[derive(Wire)]` usable on named-field structs. Tasks 5-8 rely on it existing.

**The whole point of this task:** the emitted `Field.name` must be the **serde** name. If a struct carries `#[serde(rename_all = "camelCase")]` and the derive ignores it, the generated TypeScript is wrong *and still type-checks* — the worst failure this project can ship (spec §5). The derive must read `rename_all`, per-field `rename`, and `skip_serializing_if` (which makes a field optional on the wire).

- [ ] **Step 1: Write the failing test**

Append to `crates/charpente/tests/wire.rs`:

```rust
use charpente::wire::{Field, TypeRegistry, TypeSchema, Wire};
use charpente_macros::Wire as WireDerive;

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct LineItem {
    unit_amount_cents: u32,
    #[serde(rename = "qty")]
    quantity: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    note: Option<String>,
}

#[test]
fn the_derive_uses_serde_names_not_rust_names() {
    let TypeSchema::Struct { fields } = <LineItem as Wire>::schema() else {
        panic!("expected a struct schema");
    };
    let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, vec!["unitAmountCents", "qty", "note"]);
}

#[test]
fn skip_serializing_if_makes_a_field_optional() {
    let TypeSchema::Struct { fields } = <LineItem as Wire>::schema() else {
        panic!("expected a struct schema");
    };
    let note = fields.iter().find(|f| f.name == "note").unwrap();
    assert!(note.optional, "a skip_serializing_if field is optional on the wire");
    assert!(!fields[0].optional, "a plain field is not optional");
}

#[test]
fn the_derive_registers_itself_under_its_name() {
    let mut reg = TypeRegistry::default();
    <LineItem as Wire>::register(&mut reg);
    let defs = reg.into_sorted();
    assert_eq!(defs.len(), 1);
    assert_eq!(defs[0].name, "LineItem");
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p charpente --test wire 2>&1 | tee /tmp/t3.log; echo "exit=${pipestatus[1]}"
```
Expected: FAIL — no derive macro named `Wire`.

- [ ] **Step 3: Write `crates/charpente-macros/src/wire.rs`**

```rust
//! `#[derive(Wire)]` (TS-codegen spec §1, §2): describes a type's SERDE shape,
//! not its Rust shape. Field names are run through the same rename rules serde
//! applies, because a name mismatch produces TypeScript that is wrong and still
//! type-checks — the failure mode spec §5's literal oracle exists to catch.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields, LitStr};

pub(crate) fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;
    let name_str = name.to_string();

    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            format!(
                "#[derive(Wire)] structs cannot be generic — a wire type is one concrete \
                 shape; make `{name_str}` non-generic"
            ),
        ));
    }

    let rename_all = struct_rename_all(&input)?;

    let Data::Struct(data) = &input.data else {
        return Err(Error::new_spanned(
            &input,
            "#[derive(Wire)] applies to structs — enum support lands in Task 4",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(Error::new_spanned(
            &input,
            format!("#[derive(Wire)] needs named fields — `{name_str}` must be a braced struct"),
        ));
    };

    let mut field_toks = Vec::new();
    let mut register_toks = Vec::new();
    for f in &fields.named {
        let ident = f.ident.as_ref().expect("named fields have idents");
        let ty = &f.ty;
        let wire_name = field_wire_name(f, ident, rename_all.as_deref())?;
        let optional = has_skip_serializing_if(f);
        field_toks.push(quote! {
            ::charpente::wire::Field {
                name: #wire_name.to_string(),
                schema: <#ty as ::charpente::wire::Wire>::schema(),
                optional: #optional,
            }
        });
        register_toks.push(quote! {
            <#ty as ::charpente::wire::Wire>::register(reg);
        });
    }

    Ok(quote! {
        impl ::charpente::wire::Wire for #name {
            const WIRE_NAME: &'static str = #name_str;

            fn schema() -> ::charpente::wire::TypeSchema {
                ::charpente::wire::TypeSchema::Struct {
                    fields: ::std::vec![#(#field_toks),*],
                }
            }

            fn register(reg: &mut ::charpente::wire::TypeRegistry) {
                // Guard FIRST: a self-referential type would otherwise recurse
                // forever. `insert` returns true when the name was already
                // present.
                if reg.contains(#name_str) {
                    return;
                }
                reg.insert(#name_str, ::charpente::wire::TypeSchema::Struct {
                    fields: ::std::vec![#(#field_toks),*],
                });
                #(#register_toks)*
            }
        }
    })
}
```

Add the helper functions in the same file — `struct_rename_all` reads `#[serde(rename_all = "...")]` off the struct, `field_wire_name` prefers a per-field `#[serde(rename = "...")]` and otherwise applies the rename rule, and `has_skip_serializing_if` looks for that attribute:

```rust
fn struct_rename_all(input: &DeriveInput) -> syn::Result<Option<String>> {
    let mut found = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename_all") {
                let s: LitStr = meta.value()?.parse()?;
                found = Some(s.value());
            }
            Ok(())
        });
    }
    Ok(found)
}

fn field_wire_name(
    f: &syn::Field,
    ident: &syn::Ident,
    rename_all: Option<&str>,
) -> syn::Result<String> {
    for attr in &f.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let mut renamed = None;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let s: LitStr = meta.value()?.parse()?;
                renamed = Some(s.value());
            }
            Ok(())
        });
        if let Some(r) = renamed {
            return Ok(r);
        }
    }
    let raw = ident.to_string();
    Ok(match rename_all {
        Some("camelCase") => to_camel_case(&raw),
        Some("snake_case") | None => raw,
        Some("SCREAMING_SNAKE_CASE") => raw.to_uppercase(),
        Some("kebab-case") => raw.replace('_', "-"),
        Some(other) => {
            return Err(Error::new_spanned(
                f,
                format!(
                    "#[derive(Wire)] does not know serde's rename_all = \"{other}\" — the \
                     generated TypeScript would carry the wrong field names. Supported: \
                     camelCase, snake_case, SCREAMING_SNAKE_CASE, kebab-case"
                ),
            ));
        }
    })
}

fn to_camel_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut upper = false;
    for c in s.chars() {
        if c == '_' {
            upper = true;
        } else if upper {
            out.extend(c.to_uppercase());
            upper = false;
        } else {
            out.push(c);
        }
    }
    out
}

fn has_skip_serializing_if(f: &syn::Field) -> bool {
    f.attrs.iter().any(|attr| {
        if !attr.path().is_ident("serde") {
            return false;
        }
        let mut found = false;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("skip_serializing_if") {
                found = true;
            }
            Ok(())
        });
        found
    })
}
```

> **Deliberate refusal, do not "fix" it:** an unknown `rename_all` is a hard error, not a fallback to the Rust name. Falling back would emit TypeScript with the wrong field names and no failure until runtime — exactly the class of bug this design exists to remove.

- [ ] **Step 4: Register the derive in `crates/charpente-macros/src/lib.rs`**

Beside the existing macro entry points:

```rust
mod wire;

#[proc_macro_derive(Wire, attributes(serde))]
pub fn wire(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    wire::expand(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
```

- [ ] **Step 5: Run the tests**

```bash
cargo test -p charpente --test wire 2>&1 | tee /tmp/t3.log; echo "exit=${pipestatus[1]}"
```
Expected: PASS, 9 tests.

- [ ] **Step 6: Prove the unknown-`rename_all` refusal**

Add `crates/charpente-macros/tests/compile_fail/wire_unknown_rename_all.rs`:

```rust
#[derive(charpente_macros::Wire)]
#[serde(rename_all = "PascalCase")]
struct Bad {
    a_field: u32,
}

fn main() {}
```

Run trybuild, read the `.stderr`, confirm it names the supported list.

- [ ] **Step 7: Full check and commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace 2>&1 | tee /tmp/t3-ws.log; echo "exit=${pipestatus[1]}"
git add crates/charpente-macros/src/wire.rs crates/charpente-macros/src/lib.rs crates/charpente/tests/wire.rs crates/charpente-macros/tests/compile_fail/
git commit -m "feat(wire): derive for structs, reading serde's names not Rust's

The derive applies rename_all, per-field rename, and treats skip_serializing_if
as wire-optional, because a name the IR gets wrong produces TypeScript that is
wrong AND type-checks.

An unrecognized rename_all is a hard error rather than a silent fallback to the
Rust name: the fallback would compile and be wrong, which is the failure this
whole sub-project exists to make impossible."
```

---

### Task 4: `#[derive(Wire)]` for enums, including serde tagging

**Files:**
- Modify: `crates/charpente-macros/src/wire.rs`
- Test: `crates/charpente/tests/wire.rs` (extend)

**Interfaces:**
- Consumes: everything from Task 3.
- Produces: `#[derive(Wire)]` on enums. Task 5 needs it, because `ApiError` is an enum and is every route's declared error type.

- [ ] **Step 1: Write the failing test**

```rust
#[derive(WireDerive, serde::Serialize)]
enum Status { Draft, Sent, Paid }

#[derive(WireDerive, serde::Serialize)]
#[serde(tag = "kind")]
enum Notice {
    Overdue { days: u32 },
    Voided { reason: String },
}

#[test]
fn a_unit_only_enum_is_a_string_union() {
    let TypeSchema::Enum(EnumRepr::Unit { variants }) = <Status as Wire>::schema() else {
        panic!("expected a unit enum");
    };
    assert_eq!(variants, vec!["Draft", "Sent", "Paid"]);
}

#[test]
fn an_internally_tagged_enum_carries_its_tag() {
    let TypeSchema::Enum(EnumRepr::Internal { tag, variants }) = <Notice as Wire>::schema()
    else {
        panic!("expected an internally tagged enum");
    };
    assert_eq!(tag, "kind");
    assert_eq!(variants.len(), 2);
    assert_eq!(variants[0].0, "Overdue");
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p charpente --test wire 2>&1 | tee /tmp/t4.log; echo "exit=${pipestatus[1]}"
```
Expected: FAIL — the derive rejects enums with the Task-3 message.

- [ ] **Step 3: Extend `expand` to handle `Data::Enum`**

Replace the `Data::Struct(data) = &input.data else { ... }` rejection with a match that dispatches, and add:

```rust
fn expand_enum(
    name: &syn::Ident,
    name_str: &str,
    data: &syn::DataEnum,
    input: &DeriveInput,
) -> syn::Result<TokenStream> {
    let (tag, content) = enum_tagging(input)?;
    let all_unit = data.variants.iter().all(|v| matches!(v.fields, Fields::Unit));

    let repr = if all_unit && tag.is_none() {
        let names: Vec<String> = data.variants.iter().map(|v| v.ident.to_string()).collect();
        quote! {
            ::charpente::wire::EnumRepr::Unit { variants: ::std::vec![#(#names.to_string()),*] }
        }
    } else {
        let mut arms = Vec::new();
        for v in &data.variants {
            let vname = v.ident.to_string();
            let inner = match &v.fields {
                Fields::Unit => quote! {
                    ::charpente::wire::TypeSchema::Struct { fields: ::std::vec![] }
                },
                Fields::Named(f) => {
                    let mut fs = Vec::new();
                    for field in &f.named {
                        let fid = field.ident.as_ref().expect("named");
                        let fty = &field.ty;
                        let fname = fid.to_string();
                        fs.push(quote! {
                            ::charpente::wire::Field {
                                name: #fname.to_string(),
                                schema: <#fty as ::charpente::wire::Wire>::schema(),
                                optional: false,
                            }
                        });
                    }
                    quote! {
                        ::charpente::wire::TypeSchema::Struct { fields: ::std::vec![#(#fs),*] }
                    }
                }
                Fields::Unnamed(f) if f.unnamed.len() == 1 => {
                    let fty = &f.unnamed[0].ty;
                    quote! { <#fty as ::charpente::wire::Wire>::schema() }
                }
                Fields::Unnamed(_) => {
                    return Err(Error::new_spanned(
                        v,
                        format!(
                            "#[derive(Wire)] cannot describe multi-field tuple variant \
                             `{name_str}::{vname}` — serde encodes it as a JSON array, which \
                             has no useful TypeScript type. Use a braced variant with named \
                             fields."
                        ),
                    ));
                }
            };
            arms.push(quote! { (#vname.to_string(), #inner) });
        }
        match (tag, content) {
            (Some(t), None) => quote! {
                ::charpente::wire::EnumRepr::Internal {
                    tag: #t.to_string(),
                    variants: ::std::vec![#(#arms),*],
                }
            },
            (Some(t), Some(c)) => quote! {
                ::charpente::wire::EnumRepr::Adjacent {
                    tag: #t.to_string(),
                    content: #c.to_string(),
                    variants: ::std::vec![#(#arms),*],
                }
            },
            (None, _) => quote! {
                ::charpente::wire::EnumRepr::External { variants: ::std::vec![#(#arms),*] }
            },
        }
    };

    let mut register_toks = Vec::new();
    for v in &data.variants {
        for f in v.fields.iter() {
            let ty = &f.ty;
            register_toks.push(quote! { <#ty as ::charpente::wire::Wire>::register(reg); });
        }
    }

    Ok(quote! {
        impl ::charpente::wire::Wire for #name {
            const WIRE_NAME: &'static str = #name_str;
            fn schema() -> ::charpente::wire::TypeSchema {
                ::charpente::wire::TypeSchema::Enum(#repr)
            }
            fn register(reg: &mut ::charpente::wire::TypeRegistry) {
                if reg.contains(#name_str) {
                    return;
                }
                reg.insert(#name_str, <Self as ::charpente::wire::Wire>::schema());
                #(#register_toks)*
            }
        }
    })
}

fn enum_tagging(input: &DeriveInput) -> syn::Result<(Option<String>, Option<String>)> {
    let (mut tag, mut content) = (None, None);
    for attr in &input.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("tag") {
                tag = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("content") {
                content = Some(meta.value()?.parse::<LitStr>()?.value());
            }
            Ok(())
        });
    }
    if content.is_some() && tag.is_none() {
        return Err(Error::new_spanned(
            input,
            "serde's `content` requires `tag` — an adjacently tagged enum needs both",
        ));
    }
    Ok((tag, content))
}
```

- [ ] **Step 4: Run the tests**

```bash
cargo test -p charpente --test wire 2>&1 | tee /tmp/t4.log; echo "exit=${pipestatus[1]}"
```
Expected: PASS, 11 tests.

- [ ] **Step 5: Full check and commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace 2>&1 | tee /tmp/t4-ws.log; echo "exit=${pipestatus[1]}"
git add crates/charpente-macros/src/wire.rs crates/charpente/tests/wire.rs
git commit -m "feat(wire): derive for enums, with serde's four tagging modes

Unit-only enums become a string union; data-carrying enums carry whichever
tagging serde was told to use, because externally, internally and adjacently
tagged enums are three different JSON documents and a client that guesses is
wrong at runtime.

Multi-field tuple variants are refused by name: serde encodes them as a JSON
array, which has no useful TypeScript type. A braced variant is the fix, and
the error says so."
```

---

### Task 5: `Route` carries types, and `#[route]` captures them

> **SEQUENCING AMENDMENT (Ruling 14) — READ BEFORE STARTING.** This task is the one that
> makes a bare `u64` in a wire position a compile error, because `route.response` /
> `request` / `params` generate `<T as Wire>::reference()` and `u64` is deliberately not
> `Wire`. **That breaks two in-repo consumers the moment it lands**, and the plan originally
> deferred their migration to Task 12 — leaving a seven-task window where
> `cargo test --workspace` and `cargo build -p guide-examples` could not pass, in direct
> violation of this plan's own per-task full-check constraint.
>
> Migrating them is therefore **part of Task 5**, not Task 12:
>
> - `examples/invoicing/src/routes.rs` — bare `u64` in wire position at :19, :32, :94, :108,
>   :109, plus `.raw()` unwrapping. `CustomerView.id`/`InvoiceView.id`/`InvoiceView.customer`
>   become `Id<Customer>` / `Id<Invoice>`; `InvoiceFinalized.invoice` and `SendReceipt.invoice`
>   become `Id<Invoice>`. `due_day` stays `u64` **only if** it is not a wire field — check.
>   Leave genuine non-id `u64`s (counters, days) alone; the rule is about ids and about
>   64-bit values crossing the wire, not about the type `u64` everywhere.
> - `docs/GUIDE.md` — **six** route handlers return `Result<u64, ApiError>` (:606, :718,
>   :1074, :1631, :2005, :3391), the prose at :654 instructs unwrapping "its `u64` with
>   `.raw()`", and there are 5 `.raw()` call sites. Every ```rust block in that file is
>   compiled by `crates/guide-examples`'s build.rs, so these are build failures, not stale
>   prose. `cargo build -p guide-examples` is a required gate for this task.
>
> If the migration turns out to be larger than the mechanism, land the mechanism and the
> migration as two commits in this same task rather than splitting the task — the invariant
> that matters is that the workspace builds at the task boundary.

**Files:**
- Modify: `crates/charpente/src/http/router.rs:31-37` (the `Route` struct)
- Modify: `crates/charpente-macros/src/route.rs` (both `expand` and `expand_sse`)
- Modify: `crates/charpente/src/describe.rs` (`RouteInfo`, `route_info`, `Contract.types`)
- Test: `crates/charpente/tests/describe.rs` (extend the existing suite)

**Interfaces:**
- Consumes: `Wire`, `TypeRegistry`, `TypeSchema` (Tasks 1-4).
- Produces: `Route` gains `params: &'static [(&'static str, fn() -> TypeSchema)]`, `request: Option<fn() -> TypeSchema>`, `response: fn() -> TypeSchema`, `error: fn() -> TypeSchema`, and `register_types: fn(&mut TypeRegistry)`. `Contract` gains `types: Vec<TypeDef>`; `RouteInfo` gains `params`, `request`, `response`, `error`. Tasks 8-9 consume `Contract.types` and `RouteInfo`.

> **Why function pointers, not values:** `Route` is returned from a `const`-ish `RouteDef::route()` and `TypeSchema` is not `const`-constructible (it owns `Vec`s and `String`s). Fn pointers keep `Route` cheap and defer construction to describe time, which is the only place it is needed.

- [ ] **Step 1: Write the failing test**

Add to `crates/charpente/tests/describe.rs`:

```rust
#[test]
fn a_route_carries_its_request_and_response_types() {
    let app = test_app();
    let c = app.describe();
    let r = c.routes.iter().find(|r| r.name == "create_customer").unwrap();
    assert!(r.request.is_some(), "a route with a body must name its request type");
    assert_eq!(r.response.as_ref().unwrap(), "CustomerView");
    assert_eq!(r.error.as_ref().unwrap(), "ApiError");
}

#[test]
fn the_contract_carries_the_transitive_type_closure() {
    let app = test_app();
    let c = app.describe();
    let names: Vec<&str> = c.types.iter().map(|t| t.name).collect();
    assert!(names.contains(&"CustomerView"), "types: {names:?}");
    assert!(names.contains(&"ApiError"), "the error type must be described too");
}

#[test]
fn the_type_list_is_sorted_so_generation_is_deterministic() {
    let app = test_app();
    let c = app.describe();
    let names: Vec<&str> = c.types.iter().map(|t| t.name).collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted, "Contract.types must be sorted (spec §4)");
}
```

> Reuse whatever `test_app()` helper `crates/charpente/tests/describe.rs` already defines. If its app has no route with a body, add one to that helper rather than inventing a second harness.

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p charpente --test describe 2>&1 | tee /tmp/t5.log; echo "exit=${pipestatus[1]}"
```
Expected: FAIL — no field `request` on `RouteInfo`.

- [ ] **Step 3: Widen `Route` in `crates/charpente/src/http/router.rs`**

```rust
pub struct Route {
    pub method: Method,
    pub pattern: &'static str,
    pub auth: bool,
    pub name: &'static str,
    pub glue: RouteGlue,
    /// Path params in path order, each with its wire schema. Deferred behind
    /// a fn pointer because TypeSchema is not const-constructible.
    pub params: &'static [(&'static str, fn() -> crate::wire::TypeSchema)],
    /// `None` for routes with no JSON body.
    pub request: Option<fn() -> crate::wire::TypeSchema>,
    pub response: fn() -> crate::wire::TypeSchema,
    pub error: fn() -> crate::wire::TypeSchema,
    /// Registers this route's whole type closure into the contract registry.
    pub register_types: fn(&mut crate::wire::TypeRegistry),
    /// Event types a stream route declares via `emits(...)` (Task 7). Empty
    /// for non-stream routes.
    pub emits: &'static [&'static str],
}
```

- [ ] **Step 4: Emit the new fields from `crates/charpente-macros/src/route.rs`**

In `expand`, the macro already has `params` (names), the param types via `non_ctx[i].ty`, `has_body` and the body type, and the return type via `func.sig.output`. Extract the `Ok`/`Err` types from `Result<T, E>` — `ReturnType` and `GenericArgument` are already imported at `route.rs:13`.

Add before the final `quote!`:

```rust
    let ok_ty = result_ok_type(&func.sig.output)?;
    let err_ty = result_err_type(&func.sig.output)?;

    let param_schema_entries: Vec<TokenStream> = params
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let ty = &non_ctx[i].ty;
            quote! { (#p, (|| <#ty as ::charpente::wire::Wire>::schema()) as fn() -> _) }
        })
        .collect();

    let request_tok = if has_body {
        let ty = &non_ctx[params.len()].ty;
        quote! { ::std::option::Option::Some(
            (|| <#ty as ::charpente::wire::Wire>::schema()) as fn() -> _
        ) }
    } else {
        quote! { ::std::option::Option::None }
    };

    let body_register = if has_body {
        let ty = &non_ctx[params.len()].ty;
        quote! { <#ty as ::charpente::wire::Wire>::register(reg); }
    } else {
        quote! {}
    };
    let param_registers: Vec<TokenStream> = params
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let ty = &non_ctx[i].ty;
            quote! { <#ty as ::charpente::wire::Wire>::register(reg); }
        })
        .collect();
```

and extend the emitted `Route` literal:

```rust
                    params: &[#(#param_schema_entries),*],
                    request: #request_tok,
                    response: || <#ok_ty as ::charpente::wire::Wire>::schema(),
                    error: || <#err_ty as ::charpente::wire::Wire>::schema(),
                    register_types: |reg| {
                        #(#param_registers)*
                        #body_register
                        <#ok_ty as ::charpente::wire::Wire>::register(reg);
                        <#err_ty as ::charpente::wire::Wire>::register(reg);
                    },
                    emits: &[],
```

Add the two extractors, whose error messages name the contract (hard rule 5):

```rust
fn result_ok_type(out: &ReturnType) -> syn::Result<syn::Type> {
    result_arg(out, 0)
}

fn result_err_type(out: &ReturnType) -> syn::Result<syn::Type> {
    result_arg(out, 1)
}

fn result_arg(out: &ReturnType, idx: usize) -> syn::Result<syn::Type> {
    let ReturnType::Type(_, ty) = out else {
        return Err(Error::new_spanned(
            out,
            "a #[route] handler must return Result<T, E> — a bare value has no error type \
             for the generated client to model",
        ));
    };
    let syn::Type::Path(p) = &**ty else {
        return Err(Error::new_spanned(ty, "a #[route] handler must return Result<T, E>"));
    };
    let seg = p.path.segments.last().ok_or_else(|| {
        Error::new_spanned(ty, "a #[route] handler must return Result<T, E>")
    })?;
    if seg.ident != "Result" {
        return Err(Error::new_spanned(
            ty,
            format!(
                "a #[route] handler must return Result<T, E> — got `{}`; the client needs both \
                 the success and error types to generate a typed result",
                seg.ident
            ),
        ));
    }
    let PathArguments::AngleBracketed(args) = &seg.arguments else {
        return Err(Error::new_spanned(ty, "a #[route] handler must return Result<T, E>"));
    };
    match args.args.iter().filter_map(|a| match a {
        GenericArgument::Type(t) => Some(t.clone()),
        _ => None,
    }).nth(idx) {
        Some(t) => Ok(t),
        None => Err(Error::new_spanned(
            ty,
            "a #[route] handler's Result must name both type arguments explicitly — a type \
             alias that hides the error type leaves the client with nothing to model",
        )),
    }
}
```

Apply the equivalent additions to `expand_sse`, whose handler returns `Result<Subscription, ApiError>`: `request: None`, `response` is the subscription's payload union (left as `TypeSchema::Struct { fields: vec![] }` until Task 7 fills `emits`), `error` as above.

- [ ] **Step 5: Surface it in `crates/charpente/src/describe.rs`**

`RouteInfo` gains four fields; `Contract` gains `types`; `describe()` builds the registry:

```rust
#[derive(Serialize)]
pub struct RouteInfo {
    pub method: &'static str,
    pub pattern: &'static str,
    pub name: &'static str,
    pub auth: bool,
    pub stream: bool,
    pub params: Vec<ParamInfo>,
    pub request: Option<TypeSchema>,
    pub response: Option<String>,
    pub error: Option<String>,
    pub emits: Vec<&'static str>,
}

#[derive(Serialize)]
pub struct ParamInfo {
    pub name: &'static str,
    pub schema: TypeSchema,
}
```

In `describe()`, before building `routes`:

```rust
        let mut reg = TypeRegistry::default();
        for r in self.router().routes() {
            (r.register_types)(&mut reg);
        }
```

and add `types: reg.into_sorted()` to the `Contract` literal. `into_sorted` returns a `BTreeMap` walk, so §4's determinism requirement holds by construction rather than by a sort call someone can delete.

`route_info` gains:

```rust
        params: route
            .params
            .iter()
            .map(|(name, f)| ParamInfo { name, schema: f() })
            .collect(),
        request: route.request.map(|f| f()),
        response: named_ref(&(route.response)()),
        error: named_ref(&(route.error)()),
        emits: route.emits.to_vec(),
```

with a small helper that returns the referenced name for a named type:

```rust
/// A route's response and error are named types in `Contract.types`; this
/// returns that name. Returns `None` for an anonymous shape (a handler
/// returning a bare `Vec<T>`, say) — the emitter inlines those.
fn named_ref(s: &TypeSchema) -> Option<String> {
    match s {
        TypeSchema::Ref(n) => Some((*n).to_string()),
        TypeSchema::Struct { .. } | TypeSchema::Enum(_) => None,
        _ => None,
    }
}
```

> **Implementer note:** for this to return `Some`, `Wire::schema()` for a *derived* type must return `TypeSchema::Ref(WIRE_NAME)` rather than the inline struct, with the full shape living in the registry. Adjust Task 3/4's derive accordingly — `schema()` returns `Ref`, `register()` inserts the expanded shape. This is the one cross-task correction the plan expects you to make; make it in the derive, not by special-casing here.

- [ ] **Step 6: Run the tests**

```bash
cargo test -p charpente --test describe --test wire 2>&1 | tee /tmp/t5.log; echo "exit=${pipestatus[1]}"
```
Expected: PASS.

- [ ] **Step 7: Prove the bare-`u64` diagnostic fires from a real route**

Add `crates/charpente-macros/tests/compile_fail/route_bare_u64_body.rs` with a handler whose body struct holds a `u64` field, and confirm the `.stderr` carries the 2^53 note from Task 1. This is exit criterion 5 — the diagnostic must fire from the position an app author actually writes, not only from a synthetic bound.

- [ ] **Step 8: Full check and commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace 2>&1 | tee /tmp/t5-ws.log; echo "exit=${pipestatus[1]}"
git add crates/charpente/src/http/router.rs crates/charpente-macros/src/route.rs crates/charpente/src/describe.rs crates/charpente/tests/describe.rs crates/charpente-macros/tests/compile_fail/
git commit -m "feat(describe): routes carry their types, and the contract carries the closure

Route gains param/request/response/error schemas behind fn pointers, because
TypeSchema is not const-constructible and describe time is the only place the
values are wanted. #[route] fills them from the handler signature it already
parses.

A handler returning something other than an explicit Result<T, E> is now a
compile error naming why: the client needs both halves to generate a typed
result, and a type alias hiding the error type leaves it with nothing to model.

Contract.types is a BTreeMap walk, so spec §4's byte-determinism holds by
construction rather than by a sort call someone can delete."
```

---

### Task 6: `#[event]` gains a describe-time registry

**Files:**
- Modify: `crates/charpente-macros/src/event.rs`
- Modify: `crates/charpente/src/describe.rs` (`Contract.events`, `EventInfo`)
- Modify: wherever `App` holds registries (follow how `job_defs()` is stored)
- Test: `crates/charpente/tests/describe.rs`

**Interfaces:**
- Consumes: Tasks 1-5.
- Produces: `EventInfo { name: &'static str, payload: String, keyed: bool }` on `Contract.events`; an `events![...]` -style registration collected by `App`. Task 7 and Task 9 consume it.

**The constraint that governs this task:** the realtime spec deliberately refused a runtime event registry — delivery encodes through "the encoder each `Subscription::of::<Ev>()` captured statically at the subscribe site", so "unregistered event at delivery" is inexpressible. **That property must survive.** The registry added here is read by `App::describe` and by nothing on the delivery path. If you find yourself wiring it into `Tail` or `Subscription`, stop: you have broken the property the spec protects.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn the_contract_enumerates_events_with_their_payload_types() {
    let app = test_app();
    let c = app.describe();
    let e = c.events.iter().find(|e| e.name == "invoice_paid").unwrap();
    assert_eq!(e.payload, "InvoicePaid");
    assert!(
        c.types.iter().any(|t| t.name == "InvoicePaid"),
        "an event payload must be in the type closure too"
    );
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p charpente --test describe 2>&1 | tee /tmp/t6.log; echo "exit=${pipestatus[1]}"
```
Expected: FAIL — no field `events` on `Contract`.

- [ ] **Step 3: Have `#[event]` emit an `EventDef`**

In `crates/charpente-macros/src/event.rs`, append to the emitted `quote!` — mirroring how `#[job]` emits its `JobDef`, and requiring `Wire` so the payload is describable:

```rust
        impl ::charpente::describe::EventDef for #name {
            fn event_def() -> ::charpente::describe::EventEntry {
                ::charpente::describe::EventEntry {
                    name: #event_name,
                    payload: <#name as ::charpente::wire::Wire>::WIRE_NAME,
                    register_types: |reg| <#name as ::charpente::wire::Wire>::register(reg),
                }
            }
        }
```

and add `#[derive(::charpente_macros::Wire)]` to the types the macro already emits alongside `Serialize`, so an `#[event]` struct is describable without the app author adding a second derive.

Update the module doc comment at `event.rs:1-9`, which currently states "No runtime registry: unlike `#[job]`/`jobs![]`, there is no `events![]`". That is about to be half-true, and a stale doc is a bug (hard rule 6). Replace with wording that keeps the distinction sharp:

```rust
//! Two registries, deliberately: DELIVERY has none — `Subscription` captures
//! encoders statically at the subscribe site, so "unregistered event at
//! delivery" stays inexpressible (realtime spec §3). DESCRIBE has one, filled
//! by `events![]` and read only by `App::describe` (TS-codegen spec §2).
```

- [ ] **Step 4: Collect them on `App` and surface in `describe()`**

Follow the existing `jobs![]`/`job_defs()` pattern exactly — read it before writing, and mirror its shape rather than inventing a parallel one. Add `events: Vec<EventEntry>` to `App`, an `events![...]` macro beside `jobs![]`, and in `describe()`:

```rust
            events: {
                let mut v: Vec<EventInfo> = self
                    .event_defs()
                    .iter()
                    .map(|e| EventInfo {
                        name: e.name,
                        payload: e.payload.to_string(),
                        keyed: false,
                    })
                    .collect();
                v.sort_unstable_by_key(|e| e.name);
                v
            },
```

and extend the registry loop from Task 5 so event payloads join the closure:

```rust
        for e in self.event_defs() {
            (e.register_types)(&mut reg);
        }
```

> `keyed` is `false` for now: `tx.emit_keyed` is a call-site choice, not a property of the event type, so nothing static can know it. Leave the field, populate it honestly as `false`, and do not invent a declaration for it — that is a design change, not an implementation detail.

- [ ] **Step 5: Run the tests, then prove the delivery property is intact**

```bash
cargo test -p charpente --test describe --test realtime 2>&1 | tee /tmp/t6.log; echo "exit=${pipestatus[1]}"
grep -rn "event_defs\|EventEntry" crates/charpente/src/kernel/ crates/charpente-core/src/ || echo "GOOD: the describe registry is not on the delivery path"
```
Expected: tests PASS, and the grep finds nothing. If it finds a hit in the kernel, you have wired the describe registry into delivery — revert that.

- [ ] **Step 6: Full check and commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace 2>&1 | tee /tmp/t6-ws.log; echo "exit=${pipestatus[1]}"
git add crates/charpente-macros/src/event.rs crates/charpente/src/describe.rs crates/charpente/tests/describe.rs
git commit -m "feat(describe): events are enumerable, without a delivery registry

Subscription::of::<Ev>() runs inside a handler body, so the App walk could not
see events at all and the contract had no EventInfo. The fix keeps the realtime
spec's property intact by separating concerns: delivery still encodes through
the statically captured encoder, so 'unregistered event at delivery' remains
inexpressible; describe gets its own registry that nothing on the delivery path
reads. A grep over kernel/ and core/ is part of the task's evidence.

`keyed` is reported as false rather than guessed: emit_keyed is a call-site
choice, not a property of the event type, and inventing a declaration for it
would be a design change."
```

---

### Task 7: `emits(...)` on stream routes, and the drift oracle

**Files:**
- Modify: `crates/charpente-macros/src/route.rs` (`RouteArgs` parsing + `expand_sse`)
- Test: `crates/charpente/tests/describe.rs`, `crates/charpente/tests/realtime.rs`

**Interfaces:**
- Consumes: Tasks 5-6.
- Produces: `#[route(SSE "/path", emits(A, B))]`; `Route.emits` populated; `RouteInfo.emits` in the contract. Task 9 turns it into a discriminated union.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn a_stream_route_declares_the_events_it_carries() {
    let app = test_app();
    let c = app.describe();
    let feed = c.routes.iter().find(|r| r.name == "feed").unwrap();
    assert!(feed.stream);
    assert_eq!(feed.emits, vec!["invoice_paid", "invoice_voided"]);
}

#[test]
fn a_declared_emits_set_matches_the_handlers_subscription() {
    // The drift oracle: the declaration and the handler must agree, or a
    // client is generated for events the route never sends (spec §2).
    let app = test_app();
    let c = app.describe();
    for r in c.routes.iter().filter(|r| r.stream) {
        let actual = charpente::http::subscription_event_names(r.name, &app)
            .expect("a stream route must expose its subscription's event names");
        assert_eq!(
            r.emits, actual,
            "route `{}` declares emits{:?} but its Subscription names {:?}",
            r.name, r.emits, actual
        );
    }
}
```

> **Implementer note on the oracle:** `subscription_event_names` does not exist yet. Build it as a test-facing helper that invokes the route's subscribe glue against a throwaway `Tx` and reads the returned `Subscription`'s captured event names. If `Subscription` does not expose them, add a `pub(crate)`-plus-`#[doc(hidden)]` accessor returning `&[&'static str]` — reading the names it already holds is not new state. If invoking the glue proves to need more scaffolding than the oracle is worth, **stop and report** rather than deleting the check: an `emits` declaration nothing verifies is exactly the decorative pin this repo has been burned by.

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p charpente --test describe 2>&1 | tee /tmp/t7.log; echo "exit=${pipestatus[1]}"
```
Expected: FAIL — `emits` is not accepted by the attribute parser.

- [ ] **Step 3: Parse `emits(...)` in `RouteArgs`**

In `crates/charpente-macros/src/route.rs`, extend the attribute parser that already handles `auth`. Accept `emits(Ident, Ident, ...)`, store the idents, and reject it on non-SSE routes by name:

```rust
    if !args.emits.is_empty() && !args.is_sse {
        return Err(Error::new(
            args.path.span(),
            "emits(...) applies to SSE routes only — a plain route returns one response, \
             not a stream of events; drop emits(...) or make this `#[route(SSE \"…\")]`",
        ));
    }
```

In `expand_sse`, emit the names (snake_case, matching `Event::NAME`) and register their payload types:

```rust
    let emit_names: Vec<String> = args.emits.iter().map(|i| snake_case(&i.to_string())).collect();
    let emit_registers: Vec<TokenStream> = args
        .emits
        .iter()
        .map(|i| quote! { <#i as ::charpente::wire::Wire>::register(reg); })
        .collect();
```

then in the emitted `Route`: `emits: &[#(#emit_names),*],` and fold `#(#emit_registers)*` into `register_types`.

- [ ] **Step 4: Run the tests**

```bash
cargo test -p charpente --test describe --test realtime 2>&1 | tee /tmp/t7.log; echo "exit=${pipestatus[1]}"
```
Expected: PASS.

- [ ] **Step 5: Prove the drift oracle has teeth**

Temporarily change the fixture route's `emits(...)` to name only one of its two events. Re-run: the drift test **must** fail, naming both sets. Restore, confirm green, and confirm `git status --short` is empty.

```bash
cargo test -p charpente --test describe -- a_declared_emits_set_matches 2>&1 | tee /tmp/t7-red.log
git status --short   # must be empty after restoring
```

- [ ] **Step 6: Full check and commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace 2>&1 | tee /tmp/t7-ws.log; echo "exit=${pipestatus[1]}"
git add crates/charpente-macros/src/route.rs crates/charpente/src/http/ crates/charpente/tests/
git commit -m "feat(route): stream routes declare what they emit, and drift is caught

RouteInfo.stream was a bare bool, so a generated client could only offer
'subscribe to anything'. emits(...) names the event types, which lets the client
give subscribe() a discriminated union of exactly those payloads.

A declaration nothing verifies is a decorative pin, so the drift between
emits(...) and the handler's own Subscription is an oracle: proven red by
narrowing a fixture's declaration to one event, then restored."
```

---

### Task 8: The emitter crate — types

**Files:**
- Create: `crates/charpente-tsgen/Cargo.toml`, `src/lib.rs`, `src/types.rs`
- Modify: root `Cargo.toml` workspace members
- Test: `crates/charpente-tsgen/tests/types.rs`

**Interfaces:**
- Consumes: `Contract`, `TypeSchema`, `TypeDef` from `charpente`.
- Produces: `pub fn emit_types(c: &Contract) -> String`. Task 9 calls it; Task 10 ships it.

- [ ] **Step 1: Write the failing test**

```rust
use charpente_tsgen::emit_types;

#[test]
fn a_struct_becomes_an_interface_with_serde_names() {
    let ts = emit_types(&fixture_contract());
    assert!(ts.contains("export interface LineItem {"), "{ts}");
    assert!(ts.contains("  unitAmountCents: number;"), "{ts}");
    assert!(ts.contains("  note?: string | null;"), "optional field: {ts}");
}

#[test]
fn an_id_becomes_a_branded_string() {
    let ts = emit_types(&fixture_contract());
    assert!(
        ts.contains(r#"export type CustomerId = string & { readonly __table: "Customer" };"#),
        "{ts}"
    );
}

#[test]
fn a_64_bit_integer_becomes_bigint() {
    let ts = emit_types(&fixture_contract());
    assert!(ts.contains("bigint"), "{ts}");
}

#[test]
fn emission_is_byte_identical_across_runs() {
    let c = fixture_contract();
    assert_eq!(emit_types(&c), emit_types(&c), "spec §4: generation is deterministic");
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p charpente-tsgen 2>&1 | tee /tmp/t8.log; echo "exit=${pipestatus[1]}"
```
Expected: FAIL — no such crate.

- [ ] **Step 3: Create the crate**

`crates/charpente-tsgen/Cargo.toml`:

```toml
[package]
name = "charpente-tsgen"
version = "0.0.0"
edition.workspace = true

[dependencies]
charpente.workspace = true
```

Add `"crates/charpente-tsgen"` to the workspace `members` list in the root `Cargo.toml`, and a `charpente-tsgen.workspace = true` entry under `[workspace.dependencies]` if that is how this workspace wires internal deps — read the root manifest and follow its existing convention rather than guessing.

- [ ] **Step 4: Write `src/types.rs`**

```rust
//! Contract -> TypeScript type declarations. A PURE function: no clocks, no
//! IO, no map iteration. Determinism is a product requirement (spec §4) —
//! a generator that flaps trains people to ignore a red `check-client`.

use charpente::describe::Contract;
use charpente::wire::{EnumRepr, TypeSchema};
use std::collections::BTreeSet;
use std::fmt::Write;

pub fn emit_types(c: &Contract) -> String {
    let mut out = String::new();
    out.push_str("// GENERATED by `charpente gen-client`. Do not edit.\n");
    out.push_str("// Re-run generation after changing a Rust wire type;\n");
    out.push_str("// `charpente check-client` fails if this file is stale.\n\n");

    // Branded id types first, deduped and sorted: an id can appear in many
    // places and must be declared exactly once.
    let mut tables = BTreeSet::new();
    for def in &c.types {
        collect_id_tables(&def.schema, &mut tables);
    }
    for t in &tables {
        let _ = writeln!(
            out,
            "export type {t}Id = string & {{ readonly __table: \"{t}\" }};"
        );
    }
    if !tables.is_empty() {
        out.push('\n');
    }

    // `Contract.types` is already sorted (Task 5), so no sort here — and no
    // sort to forget, either.
    for def in &c.types {
        emit_def(&mut out, def.name, &def.schema);
        out.push('\n');
    }
    out
}

fn collect_id_tables(s: &TypeSchema, acc: &mut BTreeSet<&'static str>) {
    match s {
        TypeSchema::Id(t) => {
            acc.insert(t);
        }
        TypeSchema::Option(inner) | TypeSchema::Seq(inner) | TypeSchema::Map(inner) => {
            collect_id_tables(inner, acc)
        }
        TypeSchema::Struct { fields } => {
            for f in fields {
                collect_id_tables(&f.schema, acc);
            }
        }
        TypeSchema::Enum(repr) => match repr {
            EnumRepr::Unit { .. } => {}
            EnumRepr::External { variants }
            | EnumRepr::Internal { variants, .. }
            | EnumRepr::Adjacent { variants, .. } => {
                for (_, v) in variants {
                    collect_id_tables(v, acc);
                }
            }
        },
        _ => {}
    }
}

fn emit_def(out: &mut String, name: &str, schema: &TypeSchema) {
    match schema {
        TypeSchema::Struct { fields } => {
            let _ = writeln!(out, "export interface {name} {{");
            for f in fields {
                let opt = if f.optional { "?" } else { "" };
                let _ = writeln!(out, "  {}{}: {};", f.name, opt, ts_type(&f.schema));
            }
            out.push_str("}\n");
        }
        TypeSchema::Enum(EnumRepr::Unit { variants }) => {
            let union = variants
                .iter()
                .map(|v| format!("\"{v}\""))
                .collect::<Vec<_>>()
                .join(" | ");
            let _ = writeln!(out, "export type {name} = {union};");
        }
        TypeSchema::Enum(EnumRepr::Internal { tag, variants }) => {
            let arms: Vec<String> = variants
                .iter()
                .map(|(vname, vschema)| {
                    let mut inner = format!("{{ {tag}: \"{vname}\"");
                    if let TypeSchema::Struct { fields } = vschema {
                        for f in fields {
                            let opt = if f.optional { "?" } else { "" };
                            inner.push_str(&format!(
                                "; {}{}: {}",
                                f.name,
                                opt,
                                ts_type(&f.schema)
                            ));
                        }
                    }
                    inner.push_str(" }");
                    inner
                })
                .collect();
            let _ = writeln!(out, "export type {name} =\n  | {};", arms.join("\n  | "));
        }
        TypeSchema::Enum(EnumRepr::External { variants }) => {
            let arms: Vec<String> = variants
                .iter()
                .map(|(vname, vschema)| format!("{{ {vname}: {} }}", ts_type(vschema)))
                .collect();
            let _ = writeln!(out, "export type {name} =\n  | {};", arms.join("\n  | "));
        }
        TypeSchema::Enum(EnumRepr::Adjacent { tag, content, variants }) => {
            let arms: Vec<String> = variants
                .iter()
                .map(|(vname, vschema)| {
                    format!("{{ {tag}: \"{vname}\"; {content}: {} }}", ts_type(vschema))
                })
                .collect();
            let _ = writeln!(out, "export type {name} =\n  | {};", arms.join("\n  | "));
        }
        other => {
            let _ = writeln!(out, "export type {name} = {};", ts_type(other));
        }
    }
}

fn ts_type(s: &TypeSchema) -> String {
    match s {
        TypeSchema::Bool => "boolean".into(),
        TypeSchema::I8 | TypeSchema::I16 | TypeSchema::I32
        | TypeSchema::U8 | TypeSchema::U16 | TypeSchema::U32
        | TypeSchema::F32 | TypeSchema::F64 => "number".into(),
        TypeSchema::String => "string".into(),
        // Wire-encoded as a string, surfaced as bigint: BigInt(str) is
        // lossless, which a JSON number is not (spec §1).
        TypeSchema::Int64 { .. } => "bigint".into(),
        TypeSchema::Id(t) => format!("{t}Id"),
        TypeSchema::Option(inner) => format!("{} | null", ts_type(inner)),
        TypeSchema::Seq(inner) => format!("{}[]", ts_type(inner)),
        TypeSchema::Map(inner) => format!("Record<string, {}>", ts_type(inner)),
        TypeSchema::Ref(n) => (*n).to_string(),
        TypeSchema::Struct { .. } | TypeSchema::Enum(_) => {
            // Anonymous nested shapes are not expected: the derive returns
            // Ref for named types (Task 5). Reaching here means a type
            // escaped the registry.
            unreachable!("anonymous nested shape in a named position — the registry missed a type")
        }
    }
}
```

`src/lib.rs`:

```rust
mod types;
pub use types::emit_types;
```

- [ ] **Step 5: Run the tests, then commit**

```bash
cargo test -p charpente-tsgen 2>&1 | tee /tmp/t8.log; echo "exit=${pipestatus[1]}"
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
git add crates/charpente-tsgen Cargo.toml
git commit -m "feat(tsgen): Contract -> TypeScript types, deterministically

A pure function of the Contract: no clock, no IO, no map iteration. Ids emit as
branded strings so the guarantee DESIGN §3 makes inside Rust ('an Id<Invoice> is
not an Id<Org>') survives the wire, and 64-bit integers emit as bigint because
they arrive as strings.

The anonymous-shape arm is unreachable! rather than a fallback: reaching it
means a type escaped the registry, and emitting a plausible inline shape would
hide that."
```

---

### Task 9: The emitter — routes, the result union, and the SSE client

**Files:**
- Create: `crates/charpente-tsgen/src/routes.rs`, `src/events.rs`, `src/runtime.rs`
- Modify: `crates/charpente-tsgen/src/lib.rs`
- Test: `crates/charpente-tsgen/tests/routes.rs`

**Interfaces:**
- Consumes: `emit_types` (Task 8), `RouteInfo`/`ParamInfo`/`EventInfo` (Tasks 5-7).
- Produces: `pub fn emit_client(c: &Contract) -> String` — the whole file. Task 10 writes its output to disk.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn a_route_becomes_a_typed_function_returning_a_result_union() {
    let ts = charpente_tsgen::emit_client(&fixture_contract());
    assert!(ts.contains("async createCustomer("), "{ts}");
    assert!(ts.contains("Promise<Result<CustomerView, ApiError>>"), "{ts}");
}

#[test]
fn a_stream_route_becomes_a_typed_subscribe() {
    let ts = charpente_tsgen::emit_client(&fixture_contract());
    assert!(ts.contains("feed(opts: SubscribeOptions<FeedEvent>)"), "{ts}");
    assert!(
        ts.contains("export type FeedEvent =")
            && ts.contains("InvoicePaid")
            && ts.contains("InvoiceVoided"),
        "the union must name exactly the declared emits: {ts}"
    );
}

#[test]
fn the_gap_and_divergence_frames_are_modelled_not_swallowed() {
    let ts = charpente_tsgen::emit_client(&fixture_contract());
    assert!(ts.contains("charpente.gap"), "the gap frame must be handled: {ts}");
    assert!(ts.contains("CursorDiverged"), "divergence must be a typed condition: {ts}");
    assert!(
        ts.contains("must not auto-resume"),
        "the no-auto-resume rule must be stated at the site: {ts}"
    );
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p charpente-tsgen 2>&1 | tee /tmp/t9.log; echo "exit=${pipestatus[1]}"
```
Expected: FAIL — no `emit_client`.

- [ ] **Step 3: Write `src/runtime.rs` — the embedded client runtime**

A `pub const RUNTIME: &str` holding the hand-written TypeScript that never varies with the contract. It carries: the `Result<T, E>` type, `ApiClient` base (base URL + token), a `request()` helper, the fetch-based SSE reader, and the two honesty conditions.

```rust
pub const RUNTIME: &str = r#"
export type Result<T, E> =
  | { ok: true; data: T }
  | { ok: false; error: E };

/// A tail fell behind retention (DESIGN §6 `charpente.gap`), or the server
/// refused a cursor as cross-timeline. These are DELIBERATE, NAMED refusals —
/// a client that treats them as "connection dropped, reconnect" would resume
/// past a divergence and silently deliver events from another timeline, which
/// is exactly what the cursor's seq half exists to prevent.
export type StreamEnd =
  | { kind: "gap"; detail: string }
  | { kind: "diverged"; detail: string };

export class CursorDiverged extends Error {
  constructor(readonly detail: string) {
    super(`cursor diverged: ${detail} — re-sync current state through reads, then reconnect without a cursor`);
  }
}

export interface SubscribeOptions<Ev> {
  cursor?: string;
  onEvent: (ev: Ev, cursor: string) => void;
  onEnd: (end: StreamEnd) => void;
  signal?: AbortSignal;
}

export class ApiBase {
  constructor(protected baseUrl: string, protected token?: string) {}

  setToken(token: string | undefined) { this.token = token; }

  protected headers(json: boolean): HeadersInit {
    const h: Record<string, string> = {};
    if (json) h["content-type"] = "application/json";
    if (this.token) h["authorization"] = `Bearer ${this.token}`;
    return h;
  }

  protected async request<T, E>(
    method: string, path: string, body?: unknown,
  ): Promise<Result<T, E>> {
    const res = await fetch(this.baseUrl + path, {
      method,
      headers: this.headers(body !== undefined),
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await res.text();
    const parsed = text.length ? JSON.parse(text) : null;
    return res.ok ? { ok: true, data: parsed as T } : { ok: false, error: parsed as E };
  }

  // Fetch-based, NOT EventSource: EventSource cannot set an Authorization
  // header, which would force the token into a query string where it lands in
  // logs and referrers. The cost is that we owe reconnect explicitly.
  protected async stream<Ev>(path: string, opts: SubscribeOptions<Ev>): Promise<void> {
    const headers: Record<string, string> = { ...(this.headers(false) as Record<string, string>) };
    if (opts.cursor) headers["last-event-id"] = opts.cursor;
    const res = await fetch(this.baseUrl + path, { headers, signal: opts.signal });
    if (res.status === 409) {
      // The server refused the cursor. This client must not auto-resume across
      // a divergence; surface it and let the caller re-sync.
      throw new CursorDiverged(await res.text());
    }
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    for (;;) {
      const { done, value } = await reader.read();
      if (done) return;
      buf += decoder.decode(value, { stream: true });
      let idx: number;
      while ((idx = buf.indexOf("\n\n")) >= 0) {
        const frame = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        let name = "message", data = "", id = "";
        for (const line of frame.split("\n")) {
          if (line.startsWith("event:")) name = line.slice(6).trim();
          else if (line.startsWith("data:")) data += line.slice(5).trim();
          else if (line.startsWith("id:")) id = line.slice(3).trim();
        }
        if (name === "charpente.gap") {
          opts.onEnd({ kind: "gap", detail: data });
          return;
        }
        opts.onEvent(JSON.parse(data) as Ev, id);
      }
    }
  }
}
"#;
```

- [ ] **Step 4: Write `src/routes.rs` and `src/events.rs`**

`routes.rs` emits one method per non-stream route: camelCase name from `RouteInfo.name`, positional params typed from `ParamInfo.schema`, an optional `body`, and a `Promise<Result<Ok, Err>>` return. Path params are substituted into the pattern by replacing `:name` with the encoded argument.

`events.rs` emits, per stream route, a `FeedEvent`-style union named from the route plus `Event`, whose arms are the declared `emits` payload types, and a method delegating to `this.stream(...)`.

`lib.rs`:

```rust
mod events;
mod routes;
mod runtime;
mod types;

pub use types::emit_types;

/// The whole client file: header, branded ids, types, runtime, then the class.
pub fn emit_client(c: &charpente::describe::Contract) -> String {
    let mut out = emit_types(c);
    out.push_str(runtime::RUNTIME);
    out.push('\n');
    out.push_str(&events::emit_unions(c));
    out.push_str("export class Api extends ApiBase {\n");
    out.push_str(&routes::emit_methods(c));
    out.push_str(&events::emit_methods(c));
    out.push_str("}\n");
    out
}
```

- [ ] **Step 5: Run the tests, then prove determinism again at the whole-file level**

```bash
cargo test -p charpente-tsgen 2>&1 | tee /tmp/t9.log; echo "exit=${pipestatus[1]}"
```

- [ ] **Step 6: Full check and commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
git add crates/charpente-tsgen
git commit -m "feat(tsgen): route functions, the result union, and a fetch-based SSE client

Routes return { ok, data } | { ok, error } rather than throwing: the error type
is part of the route's declared Rust signature, and catch binds unknown, which
would discard exactly the information this project exists to carry.

The stream reader is fetch-based, not EventSource, because EventSource cannot
set an Authorization header and the alternative is a token in a query string.

DESIGN §6's two honesty frames are modelled, not swallowed: charpente.gap ends
the stream with a typed reason, and a 409 raises CursorDiverged instead of
reconnecting — auto-resuming past a divergence is the silent misattribution the
cursor's seq half was added to prevent."
```

---

### Task 10: `charpente gen-client` and `charpente check-client`

**Files:**
- Create: `crates/charpente-cli/src/genclient.rs`
- Modify: `crates/charpente-cli/src/main.rs` (verb dispatch)
- Modify: `crates/charpente-cli/Cargo.toml` (depend on `charpente-tsgen`)
- Test: `crates/charpente-cli/tests/genclient.rs`

**Interfaces:**
- Consumes: `charpente_tsgen::emit_client`.
- Produces: two CLI verbs. Task 11 wires `check-client` into CI.

**How generation gets a `Contract`:** exactly the way `charpente describe` already does — read `crates/charpente-cli/src/describe.rs` first and reuse its app-build-and-exec path rather than writing a second one. It builds the app crate, runs the binary with `describe --format json`, and reads stdout. `gen-client` does the same and pipes that JSON into the emitter. If `describe.rs` exposes that as a private helper, lift it to `pub(crate)` and call it; do not duplicate it.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn check_client_fails_on_a_stale_file_and_names_the_fix() {
    let dir = tempdir();
    write_fixture_app(&dir);
    run_ok(&["gen-client", "--dir", dir.path(), "--out", &out_path(&dir)]);

    // Staleness, simulated the way it actually happens: someone edits a Rust
    // type and forgets to regenerate.
    append_field_to_a_wire_type(&dir);

    let out = run(&["check-client", "--dir", dir.path(), "--out", &out_path(&dir)]);
    assert_eq!(out.code, 1, "a stale client must exit 1");
    assert!(
        out.stderr.contains("charpente gen-client"),
        "the failure must name the fix (hard rule 5): {}",
        out.stderr
    );
}

#[test]
fn gen_client_is_byte_identical_on_a_second_run() {
    let dir = tempdir();
    write_fixture_app(&dir);
    run_ok(&["gen-client", "--dir", dir.path(), "--out", &out_path(&dir)]);
    let first = std::fs::read(&out_path(&dir)).unwrap();
    run_ok(&["gen-client", "--dir", dir.path(), "--out", &out_path(&dir)]);
    let second = std::fs::read(&out_path(&dir)).unwrap();
    assert_eq!(first, second, "spec §4: generation must be byte-deterministic");
}

#[test]
fn check_client_passes_on_a_fresh_file() {
    let dir = tempdir();
    write_fixture_app(&dir);
    run_ok(&["gen-client", "--dir", dir.path(), "--out", &out_path(&dir)]);
    assert_eq!(run(&["check-client", "--dir", dir.path(), "--out", &out_path(&dir)]).code, 0);
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p charpente-cli --test genclient 2>&1 | tee /tmp/t10.log; echo "exit=${pipestatus[1]}"
```
Expected: FAIL — unknown verb.

- [ ] **Step 3: Write `crates/charpente-cli/src/genclient.rs`**

```rust
//! `gen-client` / `check-client` (TS-codegen spec §4). The contract comes from
//! the same app-build-and-exec path `describe` uses; the emitter is pure, so
//! the only IO here is writing or comparing a file.

use std::path::Path;
use std::process::ExitCode;

pub fn gen(dir: &Path, out: &Path) -> ExitCode {
    let contract = match crate::describe::contract_json(dir) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("charpente gen-client: {e}");
            return ExitCode::from(2);
        }
    };
    let ts = charpente_tsgen::emit_client(&contract);
    match std::fs::write(out, ts) {
        Ok(()) => {
            eprintln!("charpente gen-client: wrote {}", out.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("charpente gen-client: cannot write {}: {e}", out.display());
            ExitCode::from(2)
        }
    }
}

pub fn check(dir: &Path, out: &Path) -> ExitCode {
    let contract = match crate::describe::contract_json(dir) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("charpente check-client: {e}");
            return ExitCode::from(2);
        }
    };
    let fresh = charpente_tsgen::emit_client(&contract);
    let committed = match std::fs::read_to_string(out) {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "charpente check-client: cannot read {} ({e}) — run `charpente gen-client \
                 --out {}` to create it",
                out.display(),
                out.display()
            );
            return ExitCode::from(1);
        }
    };
    if fresh == committed {
        return ExitCode::SUCCESS;
    }
    eprintln!(
        "charpente check-client: {} is stale — a Rust wire type changed and the client was \
         not regenerated.\n  fix: charpente gen-client --out {}\n  then commit the result.",
        out.display(),
        out.display()
    );
    for line in first_difference(&committed, &fresh) {
        eprintln!("{line}");
    }
    ExitCode::from(1)
}

/// The first differing line with a little context — enough to see WHAT drifted
/// without printing an entire generated file into CI logs.
fn first_difference(a: &str, b: &str) -> Vec<String> {
    let (av, bv): (Vec<&str>, Vec<&str>) = (a.lines().collect(), b.lines().collect());
    for (i, (x, y)) in av.iter().zip(bv.iter()).enumerate() {
        if x != y {
            return vec![
                format!("  first difference at line {}:", i + 1),
                format!("    committed: {x}"),
                format!("    generated: {y}"),
            ];
        }
    }
    vec![format!(
        "  same prefix, different length: committed {} lines, generated {} lines",
        av.len(),
        bv.len()
    )]
}
```

- [ ] **Step 4: Wire the verbs into `main.rs`**

Follow the existing dispatch shape in `crates/charpente-cli/src/main.rs`; add `gen-client` and `check-client` arms with `--dir` (default `.`) and `--out` (default `./client.ts`), and extend the usage text. Exit codes: 0 green, 1 failure, 2 usage/launch error — the same convention `sim.rs` uses.

- [ ] **Step 5: Run the tests and commit**

```bash
cargo test -p charpente-cli --test genclient 2>&1 | tee /tmp/t10.log; echo "exit=${pipestatus[1]}"
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
git add crates/charpente-cli
git commit -m "feat(cli): gen-client and check-client

check-client is the oracle, not the convenience: a stale client exits 1, names
the command that fixes it, and prints the first differing line so CI shows WHAT
drifted without dumping a generated file into the log.

Both reuse describe's existing app-build-and-exec path rather than opening a
second way to obtain a Contract."
```

---

### Task 11: The `ts-fixture` dogfood target and its oracles

**Files:**
- Create: `ts-fixture/package.json`, `ts-fixture/tsconfig.json`, `ts-fixture/src/calls.ts`, `ts-fixture/src/negatives.ts`, `ts-fixture/src/generated/client.ts` (generated, committed)
- Create: `crates/charpente-tsgen/src/samples.rs` (the serde-fidelity literal emitter)
- Modify: `.github/workflows/ci.yml`
- Test: the `tsc --noEmit` run is the test.

**Interfaces:**
- Consumes: Tasks 8-10.
- Produces: `emit_samples(c: &Contract, json: &SampleSet) -> String`; the CI gate.

**This task carries exit criteria 1, 2 and 3.** It is where the whole design either is an oracle or is not.

- [ ] **Step 1: Emit the serde-fidelity literals**

The mechanism from spec §5: a Rust sample value per wire type, serialized by `serde_json`, emitted as a TypeScript literal checked with `satisfies`. The literal is the *real* wire shape, so a rename the IR missed fails `tsc`.

Add a `sample()` to the `Wire` derive returning a deterministic value per field type (`0` for numbers, `""` for strings, `"0"` for `Int64`/`Id`, `null` for `Option`, `[]` for `Seq`, first variant for an enum), then have `gen-client` also write `generated/samples.ts`:

```ts
// GENERATED. Each literal is a real serde_json document; `satisfies` proves the
// generated type accepts exactly what the server actually sends.
import type { CustomerView, LineItem } from "./client";

export const _customerView = { id: "0", name: "", email: "" } satisfies CustomerView;
export const _lineItem = { unitAmountCents: 0, qty: 0 } satisfies LineItem;
```

- [ ] **Step 2: Write the fixture app's call sites**

`ts-fixture/src/calls.ts` — small, real, and exercising each shape once:

```ts
import { Api } from "./generated/client";

export async function exercise(api: Api) {
  const created = await api.createCustomer("1", { name: "a", email: "b" });
  if (!created.ok) {
    // The error is typed: this must narrow, not be `unknown`.
    console.error(created.error);
    return;
  }
  const id: string = created.data.id; // branded, still assignable to string

  api.feed({
    onEvent: (ev) => {
      // The union must discriminate.
      if (ev.kind === "invoice_paid") console.log(ev.amountCents);
    },
    onEnd: (end) => console.warn(end.kind),
  });
}
```

- [ ] **Step 3: Write the negative assertions**

`ts-fixture/src/negatives.ts` — without these, §1's branding is decorative:

```ts
import type { CustomerId, InvoiceId } from "./generated/client";

declare const c: CustomerId;

// @ts-expect-error an InvoiceId is not a CustomerId — DESIGN §3's guarantee
// must survive the wire, or the branding is theatre.
export const wrong: InvoiceId = c;

// @ts-expect-error a bare string is not a branded id
export const alsoWrong: CustomerId = "42";
```

> `@ts-expect-error` fails the build if the line *compiles* — so these are real assertions, not comments.

- [ ] **Step 4: `tsconfig.json`, strict**

```json
{
  "compilerOptions": {
    "strict": true,
    "target": "ES2020",
    "module": "ES2020",
    "moduleResolution": "bundler",
    "noEmit": true,
    "lib": ["ES2020", "DOM"]
  },
  "include": ["src"]
}
```

- [ ] **Step 5: Prove exit criterion 2 — a Rust change reddens `tsc`**

This is the deliverable of the whole plan. Do all three, capture output for each, then revert:

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente
# (a) rename a field on a wire type in the fixture app
# (b) change a u32 field to String
# (c) add a required field
# after EACH: regenerate is NOT run — the point is the committed client is now wrong
cd ts-fixture && npx tsc --noEmit 2>&1 | tee /tmp/t11-red.log; echo "exit=${pipestatus[1]}"
```
Expected each time: **non-zero**, with an error naming the changed field. Record all three outputs in the task report. Revert each and confirm green.

- [ ] **Step 6: Wire CI**

Add a job to `.github/workflows/ci.yml` — Node setup, `cargo run -p charpente-cli -- check-client --dir ts-fixture/app --out ts-fixture/src/generated/client.ts`, then `npx tsc --noEmit` in `ts-fixture`. Follow the existing job style in that file, and add a comment paragraph explaining what the gate protects, matching how the pinned-seed entries there are documented.

- [ ] **Step 7: Full check and commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace 2>&1 | tee /tmp/t11-ws.log; echo "exit=${pipestatus[1]}"
git add ts-fixture crates/charpente-tsgen/src/samples.rs .github/workflows/ci.yml
git commit -m "test(tsgen): the dogfood target, and the oracle that makes this real

Three checks, each of which fails for a different reason. The serde-fidelity
literals are real serde_json documents checked with satisfies, so a rename_all
the IR missed fails tsc instead of shipping a type that is wrong and compiles.
The @ts-expect-error negatives fail if the wrong-id assignment COMPILES, which
is what keeps branding from being theatre. And check-client fails on staleness.

Exit criterion 2 proven three ways — renamed field, retyped field, added
required field — each reddening tsc, each reverted."
```

---

### Task 12: Migrate `examples/invoicing` off bare `u64`, and update the docs

**Files:**
- Modify: `DESIGN.md` §3 and §7, `PLAN.md`, and `docs/GUIDE.md` (the client-generation workflow section only)
- **Ruling 14 moved the `examples/invoicing` and `docs/GUIDE.md` bare-`u64` migration into Task 5**, where the compile error first appears. Do not redo it here. **Do** verify it holds: `cargo build -p guide-examples` and a grep for `.raw()` in a wire position must both come back clean, and if they do not, that is a Task 5 regression to report, not a Task 12 task.
- Test: the existing invoicing suite plus `cargo build -p guide-examples`

**Interfaces:**
- Consumes: everything.
- Produces: a workspace where no wire type carries a bare `u64`.

- [ ] **Step 1: Migrate the view types**

`examples/invoicing/src/routes.rs:31-35` and every sibling view. `pub id: u64` built from `c.id.raw()` becomes `pub id: Id<Customer>` carrying the id directly — which is both the fix and a simplification, since the `.raw()` unwrap goes away.

- [ ] **Step 2: Build and run the example's own suite**

```bash
cargo test -p invoicing 2>&1 | tee /tmp/t12.log; echo "exit=${pipestatus[1]}"
```
Expected: PASS. **If the demo's recorded transcript or FINDINGS reference the numeric id shape, that is a real documentation drift** — report it rather than quietly editing the demo's provenance record.

- [ ] **Step 3: Update `DESIGN.md`**

§3 (the typed surface) gains a paragraph: the typed-id guarantee now survives the process boundary, because `Id<T>` is string-encoded and the generated client brands it. §7 (the agent interface) records that `describe` carries types, not only topology.

- [ ] **Step 4: Update `docs/GUIDE.md`**

Add the client-generation workflow: derive `Wire`, run `gen-client`, commit the result, and the fact that `check-client` will fail CI if it drifts. **Any ```rust block added here is compiled by `crates/guide-examples`'s build.rs** — verify with `cargo build -p guide-examples` before committing.

- [ ] **Step 5: Split the PLAN.md bullet — coordinate first**

`PLAN.md`'s M2 list currently has one "WebSockets + TS client codegen" bullet. Split it: a shipped TS-codegen bullet, and a still-deferred WebSockets bullet noting the client targets SSE and WS is a transport swap behind the same API.

**Another session is holding `PLAN.md` waiting for this edit.** Before editing, re-check `git status`; after committing, message that session that it has landed. `git add` named paths only.

- [ ] **Step 6: Full check and commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace 2>&1 | tee /tmp/t12-ws.log; echo "exit=${pipestatus[1]}"
cargo build -p guide-examples 2>&1 | tee /tmp/t12-guide.log; echo "exit=${pipestatus[1]}"
git add examples/invoicing DESIGN.md docs/GUIDE.md PLAN.md
git commit -m "feat: ids stay typed across the wire, and the docs say so

examples/invoicing carried pub id: u64 in every view, unwrapped from Id<T> via
.raw() — so ids left the process as JSON numbers and corrupted silently above
2^53 in any JavaScript client. The views now carry Id<T> directly, which is
both the fix and less code.

DESIGN §3 records that the typed-id guarantee survives the process boundary;
§7 that describe carries types rather than only topology. PLAN's M2 bullet
splits: codegen ships, WebSockets stays deferred as a transport swap behind the
same client API."
```

---

## Self-Review

Run against the spec before executing.

**Spec coverage:** §1 (IR, u64, Id branding) → Tasks 1, 2, 8. §2 (capture, describe-time event registry, emits) → Tasks 3-7. §3 (client shape, result union, fetch SSE, honesty frames) → Task 9. §4 (gate, determinism) → Tasks 8, 10, 11. §5 (oracles) → Tasks 7, 11. §6 (what changes where) → all. §7 exit criteria: 1→T11, 2→T11 step 5, 3→T10, 4→T8/T10, 5→T1/T5, 6→T7, 7→T12. §8 non-goals — no task builds WebSockets, framework bindings, retry, or a second language backend.

**Known gaps, recorded rather than hidden:**
- `TypeSchema::Map` is only implemented for `BTreeMap<String, V>`. `HashMap` is deliberately absent — its iteration order is nondeterministic, which would violate §4. If an app needs it, that is a design conversation, not an implementation detail.
- `EventInfo.keyed` is always `false` (Task 6). Honest, and noted at the site.
- Task 5 requires a correction to Tasks 3/4 — `schema()` returning `Ref` for derived types while `register()` inserts the expansion. This is flagged inline in Task 5 Step 5 rather than silently assumed.

**Type consistency:** `Wire::WIRE_NAME`, `Wire::schema`, `Wire::register`, `TypeRegistry::insert/contains/into_sorted`, `emit_types`, `emit_client`, `Route.register_types`, `Route.emits`, `EventEntry`, `ParamInfo` are used with the same names and shapes in every task that references them.
