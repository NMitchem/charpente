# M1 sub-project 5 — migrations: implementation plan (rev. 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `#[table]` structs evolve safely across versions — a compile-time-enforced migration chain, an eager batched rewrite that runs only on writable nodes via a new decode-free `MigratePut` record kind, an upgrade barrier that refuses (never bricks, never misdecodes) a foreign WAL tail, and followers that receive the whole migration through ordinary log shipping.

**Architecture:** Spec docs/superpowers/specs/2026-07-16-migrations-design.md (as revised 2026-07-17, commit 46f127b) governs. Organizing rule: **no code path ever decodes row bytes under any version other than the one they were encoded at** — where that can't be guaranteed, refuse by name. The first plan for this sub-project was rejected by adversarial review (.superpowers/sdd/migrations-plan-review.md); its architecture is superseded but its code-anchor citations were verified accurate and are reused here.

**Tech Stack:** No new external dependencies. Hand-rolled FNV-1a for fingerprints/digests (not `DefaultHasher` — its algorithm is not a cross-toolchain stability guarantee).

## Global Constraints

- Commit directly to `main`, no Co-Authored-By trailers. One commit per task unless a task says otherwise. `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings` clean before every commit.
- **Frozen replay contracts:** adoption-at-first-write and the replayed-only checkpoint-at-open are *designed* to add zero disk ops to non-migrating universes, but "zero entropy" does not imply "unmoved interleavings" — T11's evidence sweep must **re-verify** (not merely replay) the pinned seeds listed there. A pinned seed that replays green but no longer reproduces its historical bug is a silent coverage loss; treat any that shifted as needing re-derivation and say so in the close-out entry.
- The `Hello` wire change (T8) is pre-release: `schema_digest: u64` appended after `secret`, no `PROTO_VERSION` bump (auth T7's peer-secret precedent). It breaks four hand-built `Msg::Hello` literals in `crates/charpente/tests/replication.rs` (lines ~1455, ~1485, ~1541, ~1557) — T8 fixes all four; the not-a-primary test (~1485) needs a *matching* digest pre-computed into a local before the `exchange_hello` call (an `f.db` borrow inside the call site conflicts with the `&mut f` argument).
- Error messages are product surface (hard rule 5): the **seven** named errors from spec §5 are pinned **verbatim** — full-string equality on the formatted message (minus dynamic values asserted separately), not 1–2 substring fragments.
- Sim-first (hard rule 3): every retry-until-open loop in tests uses the established idiom — `sim.restart()` between attempts, `charpente_sim::checks::is_fault_fallout` (or the local `is_injected` pattern, `crates/charpente/tests/transactions.rs:531-577`) to classify errors, bounded rounds. Never an unclassified unbounded `loop { match Db::open { Err(_) => continue } }`.
- Seed bases for new tests: `0x2EB7_xxxx` (core migration tests), `0x2EB8_xxxx` (replication-side migration tests). Before first use run `grep -rn "0x2EB7_\|0x2EB8_" crates/` — expected: no hits.
- `Row::FINGERPRINT: Option<u64>` defaults to `None`; only `#[table]`-generated types get `Some`. Hand-written platform `Row` impls (`JobRow` job.rs:81, `EventRow` event.rs:24, and T1's `SchemaVersion`) need zero changes and are skipped by the gate and excluded from the digest.

---

### Task 1: Core types — `RowShape`, `Row` versioning, `TableMigration`, `SchemaVersion`

**Files:**
- Modify: `crates/charpente-core/src/table.rs` (new `RowShape` trait right before `pub trait Row` at line 204; `Row` gains two defaulted consts)
- Create: `crates/charpente-core/src/migration.rs`
- Modify: `crates/charpente-core/src/lib.rs` (add `pub mod migration;` between `pub mod log;` and `pub mod table;` — the list is alphabetical)
- Test: `#[cfg(test)]` inside `crates/charpente-core/src/migration.rs`

**Interfaces:**
- Produces: `charpente_core::table::RowShape` (`const VERSION: u32; fn encode_row(&self, out: &mut Vec<u8>); fn decode_row(bytes: &[u8]) -> io::Result<Self>;`); `Row` gains `const VERSION: u32 = 1;` and `const FINGERPRINT: Option<u64> = None;`; `charpente_core::migration::{fnv1a_hash, TableMigration, MigrationError, MigrationStep, migration_step_fn, SchemaVersion, schema_version_id, fingerprint_matches}`.

- [ ] **Step 1: Add `RowShape` and the `Row` defaults.** In `crates/charpente-core/src/table.rs`, immediately before `pub trait Row` (line 204):

```rust
/// A row's codec only — no table identity, no indexes. Implemented by
/// `#[table]` (alongside `Row`) and by `#[row_shape]` (a superseded
/// version kept in code purely so the migration chain can decode it —
/// DESIGN §3). `Row: RowShape` is deliberately NOT a supertrait bound:
/// `Row`'s codec methods predate this trait; macro-generated types
/// implement both, delegating one to the other.
pub trait RowShape: Sized {
    const VERSION: u32;
    fn encode_row(&self, out: &mut Vec<u8>);
    fn decode_row(bytes: &[u8]) -> io::Result<Self>;
}
```

  Then add to `pub trait Row` (after `const INDEXES`, before `fn id`):

```rust
    /// The row's schema version (`#[table(version = N)]`; `1` if omitted).
    const VERSION: u32 = 1;
    /// Structural fingerprint of the field shape, `Some` only for
    /// macro-generated (`#[table]`) types. `None` (the default) opts the
    /// table out of the versioning gate and the replication digest —
    /// hand-written platform impls are unaffected by this sub-project.
    const FINGERPRINT: Option<u64> = None;
```

- [ ] **Step 2: `migration.rs`, tests first (they fail to compile until the items exist — write the whole file in one pass, the `#[cfg(test)]` module is the spec).** Create `crates/charpente-core/src/migration.rs`:

```rust
//! Schema versioning and migration (DESIGN §3; spec docs/superpowers/
//! specs/2026-07-16-migrations-design.md): the `TableMigration` chain,
//! the structural fingerprint, and the `charpente.schema_versions`
//! platform table the `Db::open` gate reads.

use std::io;

use crate::table::{Codec, Id, IndexDef, Row, RowShape};

/// Fixed 64-bit FNV-1a, hand-rolled: `DefaultHasher`'s algorithm is not a
/// cross-toolchain stability guarantee and a silent change would fire
/// false "shape drifted" refusals with zero code changes. The macro crate
/// carries its own copy (proc-macros cannot call this crate's code).
pub fn fnv1a_hash(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// One version step: `Prev` is the immediately-prior version's shape (a
/// `#[row_shape]` struct). For `#[table(version = N)]` with `N >= 2` the
/// macro emits a const assertion requiring this impl — a bump without it
/// does not compile (spec §1).
pub trait TableMigration: RowShape {
    type Prev: RowShape;
    fn migrate(prev: Self::Prev) -> Result<Self, MigrationError>;
}

/// Why a step failed. `Decode`: the bytes did not parse under the OLD
/// version's own codec (corruption — drift is the fingerprint gate's
/// job). `Reject`: the author's fallible conversion refused a value.
#[derive(Debug)]
pub enum MigrationError {
    Decode(io::Error),
    Reject(String),
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationError::Decode(e) => write!(f, "decode failed under the prior version: {e}"),
            MigrationError::Reject(msg) => write!(f, "{msg}"),
        }
    }
}

/// One type-erased step, registered by `Schema::migration` (T3): bytes
/// encoded under `from_version` in, bytes encoded under `to_version` out.
#[derive(Clone, Copy)]
pub struct MigrationStep {
    pub from_version: u32,
    pub to_version: u32,
    pub apply: fn(&[u8]) -> Result<Vec<u8>, MigrationError>,
}

/// The erased step fn for one `TableMigration` impl.
pub fn migration_step_fn<Step: TableMigration>(bytes: &[u8]) -> Result<Vec<u8>, MigrationError> {
    let prev = Step::Prev::decode_row(bytes).map_err(MigrationError::Decode)?;
    let cur = Step::migrate(prev)?;
    let mut out = Vec::new();
    cur.encode_row(&mut out);
    Ok(out)
}

/// Platform-owned. Hand-written `Row` impl (core cannot use `#[table]` —
/// macros depend on core; `JobRow`/`EventRow` pattern). Its own
/// `FINGERPRINT` stays the default `None` — this table is not subject to
/// the gate it implements.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaVersion {
    pub id: Id<SchemaVersion>,
    pub table: String,
    pub version: u32,
    pub fingerprint: u64,
    /// `None` idle; `Some(last_row_id_done)` mid-migration — the
    /// resumable watermark (spec §3).
    pub migrating_through: Option<u64>,
    /// `None` idle; `Some(target_version)` mid-migration. Recorded so an
    /// interrupted migration resumed by a binary compiled for a DIFFERENT
    /// target is a named refusal, not a mixed-version rewrite (spec §2).
    pub migrating_to: Option<u32>,
}

impl Row for SchemaVersion {
    const TABLE: &'static str = "charpente.schema_versions";
    const INDEXES: &'static [IndexDef<SchemaVersion>] = &[];

    fn id(&self) -> Id<SchemaVersion> {
        self.id
    }

    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.table.encode(out);
        self.version.encode(out);
        self.fingerprint.encode(out);
        self.migrating_through.encode(out);
        self.migrating_to.encode(out);
    }

    fn decode_row(bytes: &[u8]) -> io::Result<SchemaVersion> {
        let input = &mut &bytes[..];
        let id = Id::decode(input)?;
        let table = String::decode(input)?;
        let version = u32::decode(input)?;
        let fingerprint = u64::decode(input)?;
        let migrating_through = Option::<u64>::decode(input)?;
        let migrating_to = Option::<u32>::decode(input)?;
        if !input.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "row decode: {} trailing bytes after a SchemaVersion row — schema drift or \
                     corruption",
                    input.len()
                ),
            ));
        }
        Ok(SchemaVersion {
            id,
            table,
            version,
            fingerprint,
            migrating_through,
            migrating_to,
        })
    }
}

/// Deterministic id for a table's row — lookup is a direct `get`, no
/// index (spec §1's deliberate simplification of the original sketch).
pub fn schema_version_id(table: &str) -> Id<SchemaVersion> {
    Id::from_raw(fnv1a_hash(table.as_bytes()))
}

/// The same-version acceptance predicate the gate calls. `None` persisted
/// means the bootstrap-baseline case — nothing on disk to compare.
pub fn fingerprint_matches(persisted: Option<u64>, compiled: u64) -> bool {
    persisted.is_none_or(|p| p == compiled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a_is_deterministic_and_distinguishes_inputs() {
        assert_eq!(fnv1a_hash(b"abc"), fnv1a_hash(b"abc"));
        assert_ne!(fnv1a_hash(b"abc"), fnv1a_hash(b"abd"));
        assert_ne!(fnv1a_hash(b""), fnv1a_hash(b"\0"));
    }

    #[test]
    fn schema_version_id_is_stable_per_table_name() {
        assert_eq!(schema_version_id("Invoice"), schema_version_id("Invoice"));
        assert_ne!(schema_version_id("Invoice"), schema_version_id("Org"));
    }

    #[test]
    fn schema_version_row_roundtrips_including_in_flight_fields() {
        let row = SchemaVersion {
            id: schema_version_id("Invoice"),
            table: "Invoice".to_string(),
            version: 1,
            fingerprint: 0xdead_beef,
            migrating_through: Some(41),
            migrating_to: Some(3),
        };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        assert_eq!(SchemaVersion::decode_row(&bytes).unwrap(), row);
    }

    #[test]
    fn fingerprint_matches_refuses_only_a_real_mismatch() {
        assert!(fingerprint_matches(Some(0x1111), 0x1111));
        assert!(!fingerprint_matches(Some(0x1111), 0x2222));
        assert!(fingerprint_matches(None, 0x1111));
    }

    // Hand-written two-version toy chain — exactly what #[row_shape]/
    // #[table] generate, written by hand (core cannot use its own macros).
    struct WidgetV1 {
        count: u32,
    }
    impl RowShape for WidgetV1 {
        const VERSION: u32 = 1;
        fn encode_row(&self, out: &mut Vec<u8>) {
            self.count.encode(out);
        }
        fn decode_row(bytes: &[u8]) -> io::Result<WidgetV1> {
            let input = &mut &bytes[..];
            Ok(WidgetV1 {
                count: u32::decode(input)?,
            })
        }
    }
    struct Widget {
        count: u32,
        label: String,
    }
    impl RowShape for Widget {
        const VERSION: u32 = 2;
        fn encode_row(&self, out: &mut Vec<u8>) {
            self.count.encode(out);
            self.label.encode(out);
        }
        fn decode_row(bytes: &[u8]) -> io::Result<Widget> {
            let input = &mut &bytes[..];
            Ok(Widget {
                count: u32::decode(input)?,
                label: String::decode(input)?,
            })
        }
    }
    impl TableMigration for Widget {
        type Prev = WidgetV1;
        fn migrate(prev: WidgetV1) -> Result<Widget, MigrationError> {
            Ok(Widget {
                count: prev.count,
                label: "unlabeled".to_string(),
            })
        }
    }

    #[test]
    fn migration_step_fn_decodes_migrates_reencodes() {
        let mut old = Vec::new();
        WidgetV1 { count: 7 }.encode_row(&mut old);
        let new = migration_step_fn::<Widget>(&old).unwrap();
        let widget = Widget::decode_row(&new).unwrap();
        assert_eq!((widget.count, widget.label.as_str()), (7, "unlabeled"));
    }

    #[test]
    fn migration_step_fn_surfaces_decode_failure() {
        let err = migration_step_fn::<Widget>(&[]).unwrap_err();
        assert!(matches!(err, MigrationError::Decode(_)), "{err:?}");
    }
}
```

- [ ] **Step 3: Wire the module.** `crates/charpente-core/src/lib.rs`: `pub mod migration;` after `pub mod log;` (line 12), before `pub mod table;` (line 13).
- [ ] **Step 4: Run.** `cargo test -p charpente-core migration::` — Expected: PASS. `cargo test -p charpente-core` — Expected: everything else unaffected (defaults are additive).
- [ ] **Step 5: Full check + commit.**

```bash
cargo build --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check
git add crates/charpente-core/src/table.rs crates/charpente-core/src/migration.rs crates/charpente-core/src/lib.rs
git commit -m "core: RowShape, TableMigration, SchemaVersion — the versioning types (migrations T1)"
```

### Task 2: `#[table(version = N)]` with compile-time enforcement + `#[row_shape]`

**Files:**
- Create: `crates/charpente-macros/src/fingerprint.rs`
- Modify: `crates/charpente-macros/src/table.rs` (`expand` gains `version: u32`; emits `VERSION`/`FINGERPRINT`, a delegating `RowShape` impl, and — for `version >= 2` — the assert-impl const)
- Create: `crates/charpente-macros/src/row_shape.rs`
- Modify: `crates/charpente-macros/src/lib.rs` (arg parsing; new `row_shape` attribute; the current `table` fn at lib.rs:16-31 unconditionally errors on non-empty args — replace)
- Test: `crates/charpente/tests/typed_tables.rs` (extend)

**Interfaces:**
- Consumes: T1's `RowShape`, `Row::{VERSION, FINGERPRINT}`, `TableMigration`.
- Produces: `#[table(version = N)]` (N optional, default 1) emitting on `impl Row`: `const VERSION: u32 = N; const FINGERPRINT: Option<u64> = Some(fp);` plus a delegating `impl RowShape`, plus for `N >= 2` a const assertion that `Self: TableMigration` (the spec §1 compile error). `#[row_shape(version = N)]` (N required) emitting only `impl RowShape` — no `Row`, no registration, `#[index]` rejected by name.

- [ ] **Step 1: `fingerprint.rs`.**

```rust
//! Shared by `#[table]` and `#[row_shape]`: the structural fingerprint
//! over a struct's `(field_name, type_name)` sequence, computed at
//! expansion time (spec §1). A deliberate, exact duplicate of
//! `charpente_core::migration::fnv1a_hash` — proc-macros run in the host
//! compiler and cannot call core's compiled code.
use syn::Type;

fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// NUL-separated fold so ("ab","c") cannot collide with ("a","bc").
pub(crate) fn compute(field_idents: &[syn::Ident], field_types: &[Type]) -> u64 {
    let mut buf = String::new();
    for (ident, ty) in field_idents.iter().zip(field_types) {
        buf.push_str(&ident.to_string());
        buf.push('\0');
        buf.push_str(&quote::quote!(#ty).to_string());
        buf.push('\0');
    }
    fnv1a(buf.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn compute_is_deterministic_and_order_sensitive() {
        let idents: Vec<syn::Ident> = vec![parse_quote!(a), parse_quote!(b)];
        let types: Vec<Type> = vec![parse_quote!(u32), parse_quote!(String)];
        let fp = compute(&idents, &types);
        assert_eq!(fp, compute(&idents, &types));
        let ridents: Vec<syn::Ident> = vec![parse_quote!(b), parse_quote!(a)];
        let rtypes: Vec<Type> = vec![parse_quote!(String), parse_quote!(u32)];
        assert_ne!(fp, compute(&ridents, &rtypes));
    }

    #[test]
    fn compute_distinguishes_type_changes() {
        let idents: Vec<syn::Ident> = vec![parse_quote!(a)];
        assert_ne!(
            compute(&idents, &[parse_quote!(u32)]),
            compute(&idents, &[parse_quote!(u64)])
        );
    }
}
```

- [ ] **Step 2: Run.** `cargo test -p charpente-macros fingerprint::` — Expected: PASS.
- [ ] **Step 3: `table.rs` changes.** Add `mod fingerprint;` and `mod row_shape;` to `crates/charpente-macros/src/lib.rs`'s mod list (lib.rs:8-11 area). Change `table::expand`'s signature (table.rs:4) to take `version: u32`. After the field-collection loop (table.rs:37-73, unchanged — it already builds `field_idents`/`field_types`):

```rust
    let fingerprint = crate::fingerprint::compute(&field_idents, &field_types);
    // Spec §1's compile error: a version bump without the TableMigration
    // impl must not compile. A const-evaluated call whose bound is the
    // whole point — E0277 names the missing impl at the struct.
    let migration_assert = if version >= 2 {
        quote! {
            const _: () = {
                const fn __charpente_assert_migration<
                    T: ::charpente::core::migration::TableMigration,
                >() {}
                __charpente_assert_migration::<#name>();
            };
        }
    } else {
        quote! {}
    };
```

  Inside the emitted `impl ::charpente::core::table::Row for #name` (table.rs:105-137), right after `const TABLE`:

```rust
            const VERSION: u32 = #version;
            const FINGERPRINT: ::std::option::Option<u64> =
                ::std::option::Option::Some(#fingerprint);
```

  After that impl's closing brace (after table.rs:137), before the index-accessor `impl #name` block:

```rust
        impl ::charpente::core::table::RowShape for #name {
            const VERSION: u32 = #version;
            fn encode_row(&self, out: &mut ::std::vec::Vec<u8>) {
                <Self as ::charpente::core::table::Row>::encode_row(self, out)
            }
            fn decode_row(bytes: &[u8]) -> ::std::io::Result<Self> {
                <Self as ::charpente::core::table::Row>::decode_row(bytes)
            }
        }
        #migration_assert
```

- [ ] **Step 4: `row_shape.rs`.**

```rust
//! `#[row_shape(version = N)]`: a superseded `#[table]` version kept in
//! code only so a `TableMigration` chain can decode its bytes (spec §1).
//! No `TABLE`, no `INDEXES`, no redb registration.
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields, spanned::Spanned};

pub(crate) fn expand(input: DeriveInput, version: u32) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let name_str = name.to_string();
    if !input.generics.params.is_empty() {
        return Err(Error::new(
            input.generics.span(),
            format!("#[row_shape] structs cannot be generic — make `{name_str}` non-generic"),
        ));
    }
    let Data::Struct(data) = &input.data else {
        return Err(Error::new(
            input.span(),
            "#[row_shape] applies to structs — enums and unions cannot be historical row shapes",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(Error::new(
            input.span(),
            format!("#[row_shape] needs named fields — `{name_str}` must be a braced struct"),
        ));
    };

    let mut field_idents = Vec::new();
    let mut field_types = Vec::new();
    let mut emitted_fields = Vec::new();
    for field in &fields.named {
        let ident = field.ident.as_ref().expect("named fields have idents");
        if field.attrs.iter().any(|a| a.path().is_ident("index")) {
            return Err(Error::new(
                field.span(),
                "#[index] has no meaning on #[row_shape] — a historical shape is only decoded \
                 during a migration, never queried; remove #[index]",
            ));
        }
        field_idents.push(ident.clone());
        field_types.push(field.ty.clone());
        let vis = &field.vis;
        let ty = &field.ty;
        let attrs = &field.attrs;
        emitted_fields.push(quote! { #(#attrs)* #vis #ident: #ty });
    }

    let attrs = &input.attrs;
    let vis = &input.vis;
    Ok(quote! {
        #(#attrs)*
        #vis struct #name {
            #(#emitted_fields,)*
        }

        impl ::charpente::core::table::RowShape for #name {
            const VERSION: u32 = #version;

            fn encode_row(&self, out: &mut ::std::vec::Vec<u8>) {
                #(::charpente::core::table::Codec::encode(&self.#field_idents, out);)*
            }

            fn decode_row(bytes: &[u8]) -> ::std::io::Result<Self> {
                let input = &mut &bytes[..];
                #(let #field_idents =
                    <#field_types as ::charpente::core::table::Codec>::decode(input)?;)*
                if !input.is_empty() {
                    return Err(::std::io::Error::new(
                        ::std::io::ErrorKind::InvalidData,
                        ::std::format!(
                            "row decode: {} trailing bytes after a {} row — schema drift or corruption",
                            input.len(), #name_str,
                        ),
                    ));
                }
                Ok(Self { #(#field_idents,)* })
            }
        }
    })
}
```

- [ ] **Step 5: `lib.rs` arg parsing.** Replace the `table` attribute fn (lib.rs:16-31) and add `row_shape`:

```rust
#[proc_macro_attribute]
pub fn table(args: TokenStream, input: TokenStream) -> TokenStream {
    let version = match parse_version(args, "table", false) {
        Ok(v) => v,
        Err(e) => return e.to_compile_error().into(),
    };
    let input = parse_macro_input!(input as DeriveInput);
    match table::expand(input, version) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn row_shape(args: TokenStream, input: TokenStream) -> TokenStream {
    let version = match parse_version(args, "row_shape", true) {
        Ok(v) => v,
        Err(e) => return e.to_compile_error().into(),
    };
    let input = parse_macro_input!(input as DeriveInput);
    match row_shape::expand(input, version) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// `version = N`, N >= 1. `required`: `#[row_shape]` always names its
/// version; bare `#[table]` defaults to 1 (exact today's behavior).
fn parse_version(args: TokenStream, macro_name: &str, required: bool) -> syn::Result<u32> {
    if args.is_empty() {
        if required {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "#[row_shape] requires `version = N` — write `#[row_shape(version = 1)]`",
            ));
        }
        return Ok(1);
    }
    let args2: proc_macro2::TokenStream = args.into();
    struct Args(u32);
    impl syn::parse::Parse for Args {
        fn parse(input: syn::parse::ParseStream) -> syn::Result<Args> {
            let ident: syn::Ident = input.parse()?;
            if ident != "version" {
                return Err(syn::Error::new(
                    ident.span(),
                    "takes only `version = N` (or no arguments)",
                ));
            }
            input.parse::<syn::Token![=]>()?;
            let lit: syn::LitInt = input.parse()?;
            let version: u32 = lit.base10_parse()?;
            if version == 0 {
                return Err(syn::Error::new(
                    lit.span(),
                    "version 0 does not exist — versions start at 1",
                ));
            }
            Ok(Args(version))
        }
    }
    syn::parse2::<Args>(args2)
        .map(|a| a.0)
        .map_err(|e| syn::Error::new(e.span(), format!("#[{macro_name}] {e}")))
}
```

- [ ] **Step 6: Macro-expansion tests.** In `crates/charpente/tests/typed_tables.rs`, append:

```rust
#[charpente::row_shape(version = 1)]
struct GizmoV1 {
    id: u64,
    count: u32,
}

#[charpente::table(version = 2)]
struct Gizmo {
    id: charpente::core::table::Id<Gizmo>,
    count: u32,
    label: String,
}

impl charpente::core::migration::TableMigration for Gizmo {
    type Prev = GizmoV1;
    fn migrate(prev: GizmoV1) -> Result<Gizmo, charpente::core::migration::MigrationError> {
        Ok(Gizmo {
            id: charpente::core::table::Id::from_raw(prev.id),
            count: prev.count,
            label: String::new(),
        })
    }
}

#[test]
fn versioned_table_carries_version_and_fingerprint() {
    use charpente::core::table::Row;
    assert_eq!(Gizmo::VERSION, 2);
    assert!(Gizmo::FINGERPRINT.is_some());
}

#[test]
fn bare_table_stays_version_one_and_now_fingerprinted() {
    use charpente::core::table::Row;
    // Pick any pre-existing bare #[table] struct in this file and assert
    // on IT (replace `Existing` with its real name when editing):
    // assert_eq!(Existing::VERSION, 1);
    // assert!(Existing::FINGERPRINT.is_some());
}

#[test]
fn row_shape_roundtrips_under_its_own_codec() {
    use charpente::core::table::RowShape;
    let mut bytes = Vec::new();
    GizmoV1 { id: 4, count: 9 }.encode_row(&mut bytes);
    let back = GizmoV1::decode_row(&bytes).unwrap();
    assert_eq!((back.id, back.count), (4, 9));
}
```

  Fill in `bare_table_stays_version_one_and_now_fingerprinted` against a real existing struct in the file (find one: `grep -n "#\[charpente::table\]" crates/charpente/tests/typed_tables.rs`). The `Gizmo` block also proves the assert-impl accepts a present impl; the compile-*failure* direction (bump without impl) is deliberately left to the tracked trybuild follow-up (CLAUDE.md, M0 step 8) — do not add a trybuild dep in this task.

- [ ] **Step 7: Run.** `cargo test -p charpente-macros && cargo test -p charpente --test typed_tables && cargo build -p sim-fixture` — Expected: all PASS/clean (sim-fixture is the single-dep macro-path pin).
- [ ] **Step 8: Full check + commit.**

```bash
cargo build --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check
git add crates/charpente-macros crates/charpente/tests/typed_tables.rs
git commit -m "macros: #[table(version = N)] + fingerprint + assert-impl + #[row_shape] (migrations T2)"
```

### Task 3: Schema plumbing — versions in `TableOps`, `Schema::migration`, config, registration

**Files:**
- Modify: `crates/charpente-core/src/db.rs`
- Test: db.rs `#[cfg(test)]` (extend)

**Interfaces:**
- Consumes: T1's types.
- Produces: `TableOps` gains `version: u32`, `fingerprint: Option<u64>`, `rebuild_indexes: fn(&mut Writer<'_>) -> io::Result<()>`; `Schema::migration<Step: TableMigration>(self, table: &'static str) -> Schema`; `Schema::migrations_for(&self, table: &str) -> &[MigrationStep]`; `Schema::table_meta(&self, table: &str) -> Option<(u32, Option<u64>)>`; `DbConfig::migration_batch_size: u64` (default 1000); `SchemaVersion` registered as the third platform table; `Writer::rebuild_indexes_for::<R>()` (in table.rs, consumed by the fn pointer).

- [ ] **Step 1: `Writer::rebuild_indexes_for`.** In `crates/charpente-core/src/table.rs`, inside `impl Writer<'_>` (after `materialize_event`, before `replace_row` at 624):

```rust
    /// Drops and rebuilds every one of `R`'s secondary index tables from
    /// the rows table. Index entries are derived, store-local data —
    /// never logged, deterministic, idempotent — so this is safe to redo
    /// after a crash at any point (spec §3: triggered by applying a
    /// migration close-out record). Memory is bounded by index keys, not
    /// rows: each pass streams the rows table once per index.
    pub fn rebuild_indexes_for<R: Row>(&mut self) -> io::Result<()> {
        for index in R::INDEXES {
            let name = index_table_name(index);
            self.txn.delete_table(index_def(&name)).map_err(|e| {
                io::Error::other(format!("rebuild_indexes: dropping {name}: {e}"))
            })?;
            let mut entries: Vec<Vec<u8>> = Vec::new();
            {
                let rows = self.txn.open_table(rows_def(R::TABLE)).map_err(redb_err)?;
                for item in rows.iter().map_err(redb_err)? {
                    let (k, v) = item.map_err(redb_err)?;
                    let row = R::decode_row(v.value())?;
                    let mut key = Vec::new();
                    (index.key)(&row, &mut key);
                    entries.push(index_entry(key, k.value()));
                }
            }
            let mut table = self.txn.open_table(index_def(&name)).map_err(redb_err)?;
            for entry in &entries {
                table.insert(entry.as_slice(), ()).map_err(redb_err)?;
            }
        }
        Ok(())
    }
```

  (`delete_table` on redb's `WriteTransaction` returns `Result<bool, TableError>` — a missing table yields `Ok(false)`, so no not-found arm is needed; check the redb 4.x signature and adapt the error mapping if it differs.)

- [ ] **Step 2: db.rs plumbing.** Extend `TableOps` (db.rs:99-103) and its constructors:

```rust
#[derive(Clone)]
struct TableOps {
    apply_put: fn(&mut Writer<'_>, &[u8]) -> io::Result<()>,
    apply_delete: fn(&mut Writer<'_>, u64) -> io::Result<()>,
    rebuild_indexes: fn(&mut Writer<'_>) -> io::Result<()>,
    version: u32,
    fingerprint: Option<u64>,
}

fn rebuild_indexes<R: Row>(w: &mut Writer<'_>) -> io::Result<()> {
    w.rebuild_indexes_for::<R>()
}
```

  `Schema::table` (126-140) sets `rebuild_indexes: rebuild_indexes::<R>, version: R::VERSION, fingerprint: R::FINGERPRINT,`. `Schema` (116-119) gains `migrations: BTreeMap<&'static str, Vec<crate::migration::MigrationStep>>,` (`#[derive(Default, Clone)]` still works). New methods after `table_names` (145-147):

```rust
    /// Registers one migration step for `table` (which must already be
    /// `.table::<R>()`-registered under the same name). Steps may be
    /// registered in any order; the executor looks them up by
    /// `from_version`.
    pub fn migration<Step: crate::migration::TableMigration>(
        mut self,
        table: &'static str,
    ) -> Schema {
        assert!(
            self.tables.contains_key(table),
            "Schema: migration() registered for table \"{table}\" but it was never .table()'d \
             — call .table::<R>() for \"{table}\" before registering its migrations"
        );
        self.migrations
            .entry(table)
            .or_default()
            .push(crate::migration::MigrationStep {
                from_version: Step::Prev::VERSION,
                to_version: Step::VERSION,
                apply: crate::migration::migration_step_fn::<Step>,
            });
        self
    }

    pub fn migrations_for(&self, table: &str) -> &[crate::migration::MigrationStep] {
        self.migrations.get(table).map_or(&[], Vec::as_slice)
    }

    /// A registered table's `(version, fingerprint)`.
    pub fn table_meta(&self, table: &str) -> Option<(u32, Option<u64>)> {
        self.tables.get(table).map(|ops| (ops.version, ops.fingerprint))
    }
```

  (`Schema::migration` needs `use crate::table::RowShape;` in scope for `Step::Prev::VERSION`.) `DbConfig` (32-47) gains:

```rust
    /// Rows per transaction while a table migrates (spec §3) — bounds one
    /// migration commit's WAL record size.
    pub migration_batch_size: u64,
```

  with `migration_batch_size: 1000,` in `Default` (49-58). `Db::open` (564) registers the third platform table: `let schema = schema.table::<JobRow>().table::<EventRow>().table::<crate::migration::SchemaVersion>();`. `Db::into_seeder` (924-925) adds `schema.remove(crate::migration::SchemaVersion::TABLE);` beside the existing two removes, and `Seeder::new`'s `import_schema` (965) chains `.table::<crate::migration::SchemaVersion>()` beside its existing two re-adds (the seed export walks `table_names()`, which now yields `charpente.schema_versions` — the importer must resolve it or every seed of a versioned app fails).

- [ ] **Step 3: Tests**, appended inside db.rs's `#[cfg(test)] mod tests`:

```rust
    mod schema_migrations {
        use super::super::*;
        use crate::migration::{MigrationError, TableMigration};
        use crate::table::{Codec, RowShape};

        struct WidgetV1 {
            count: u32,
        }
        impl RowShape for WidgetV1 {
            const VERSION: u32 = 1;
            fn encode_row(&self, out: &mut Vec<u8>) {
                self.count.encode(out);
            }
            fn decode_row(bytes: &[u8]) -> io::Result<WidgetV1> {
                let input = &mut &bytes[..];
                Ok(WidgetV1 { count: u32::decode(input)? })
            }
        }
        struct Widget {
            id: Id<Widget>,
            count: u32,
        }
        impl RowShape for Widget {
            const VERSION: u32 = 2;
            fn encode_row(&self, out: &mut Vec<u8>) {
                self.id.encode(out);
                self.count.encode(out);
            }
            fn decode_row(bytes: &[u8]) -> io::Result<Widget> {
                let input = &mut &bytes[..];
                Ok(Widget { id: Id::decode(input)?, count: u32::decode(input)? })
            }
        }
        impl Row for Widget {
            const TABLE: &'static str = "Widget";
            const INDEXES: &'static [IndexDef<Widget>] = &[];
            const VERSION: u32 = 2;
            const FINGERPRINT: Option<u64> = Some(0xabc);
            fn id(&self) -> Id<Widget> {
                self.id
            }
            fn encode_row(&self, out: &mut Vec<u8>) {
                self.id.encode(out);
                self.count.encode(out);
            }
            fn decode_row(bytes: &[u8]) -> io::Result<Widget> {
                let input = &mut &bytes[..];
                Ok(Widget { id: Id::decode(input)?, count: u32::decode(input)? })
            }
        }
        impl TableMigration for Widget {
            type Prev = WidgetV1;
            fn migrate(prev: WidgetV1) -> Result<Widget, MigrationError> {
                Ok(Widget { id: Id::from_raw(0), count: prev.count })
            }
        }

        #[test]
        #[should_panic(expected = "never .table()'d")]
        fn migration_for_an_unregistered_table_panics_by_name() {
            let _ = Schema::new().migration::<Widget>("Widget");
        }

        #[test]
        fn registered_step_survives_schema_storage_and_applies() {
            let schema = Schema::new().table::<Widget>().migration::<Widget>("Widget");
            let steps = schema.migrations_for("Widget");
            assert_eq!((steps[0].from_version, steps[0].to_version), (1, 2));
            let mut old = Vec::new();
            WidgetV1 { count: 3 }.encode_row(&mut old);
            let new = (steps[0].apply)(&old).unwrap();
            assert_eq!(<Widget as Row>::decode_row(&new).unwrap().count, 3);
        }

        #[test]
        fn table_meta_reads_version_and_fingerprint() {
            let schema = Schema::new().table::<Widget>();
            assert_eq!(schema.table_meta("Widget"), Some((2, Some(0xabc))));
            assert_eq!(schema.table_meta("Nope"), None);
        }
    }
```

  Note the dual `RowShape`+`Row` on `Widget` with *matching bodies* — exactly what the macro emits; the plain `WidgetV1` has `RowShape` only, like `#[row_shape]`.

- [ ] **Step 4: Run.** `cargo test -p charpente-core` — Expected: PASS including every pre-existing test (additions are additive; `Schema` default still derives).
- [ ] **Step 5: Full check + commit.**

```bash
cargo build --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check
git add crates/charpente-core/src/db.rs crates/charpente-core/src/table.rs
git commit -m "core: Schema::migration + versioned TableOps + rebuild_indexes + migration_batch_size (migrations T3)"
```

### Task 4: The `MigratePut` record kind — decode-free write path end to end

**Files:**
- Modify: `crates/charpente-core/src/db.rs` (`Effect::MigratePut` tag 4, `TxEffects.migrate_puts`, encode/decode, `Tx::migrate_put`, apply arm, close-out rebuild trigger)
- Modify: `crates/charpente-core/src/table.rs` (`Writer::put_raw`, `Writer::get_raw`, `Snapshot::scan_raw_from`)
- Test: db.rs `#[cfg(test)]` (record roundtrip) + `crates/charpente-core/tests/migration_gate.rs` created in T5 exercises the full path

**Interfaces:**
- Consumes: T3's `TableOps.rebuild_indexes`, `SchemaVersion`.
- Produces: `Effect::MigratePut { table: String, id: u64, row: Vec<u8> }` (wire tag 4); `Tx::migrate_put(&mut self, table: &'static str, id: u64, row: Vec<u8>)` (`pub(crate)`); `Writer::put_raw(&mut self, table: &str, id: u64, row: &[u8])` (overwrite bytes, no decode, no index touch); `Writer::get_raw(&mut self, table: &str, id: u64) -> io::Result<Option<Vec<u8>>>`; `Snapshot::scan_raw_from(&self, table: &str, after_id: Option<u64>, limit: usize) -> io::Result<Vec<(u64, Vec<u8>)>>`; the apply-side close-out trigger (a `charpente.schema_versions` put whose `migrating_to` transitions `Some -> None` with a version increase rebuilds that table's indexes).

- [ ] **Step 1: table.rs raw accessors.** In `impl Writer<'_>`:

```rust
    /// Decode-free overwrite: the `MigratePut` apply (spec §3). The bytes
    /// are already encoded under the table's target version; the old value
    /// is never interpreted (it is one version older — decoding it under
    /// the compiled type is exactly the bug this path exists to avoid),
    /// and indexes are deliberately untouched: they are rebuilt wholesale
    /// by the close-out trigger.
    pub fn put_raw(&mut self, table: &str, id: u64, row: &[u8]) -> io::Result<()> {
        let mut rows = self.txn.open_table(rows_def(table)).map_err(redb_err)?;
        rows.insert(id, row).map_err(redb_err)?;
        Ok(())
    }

    /// Raw read of one row's stored bytes (apply-side helper for the
    /// close-out trigger's old-row peek).
    pub fn get_raw(&mut self, table: &str, id: u64) -> io::Result<Option<Vec<u8>>> {
        let rows = match self.txn.open_table(rows_def(table)) {
            Ok(rows) => rows,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(redb_err(e)),
        };
        Ok(rows.get(id).map_err(redb_err)?.map(|g| g.value().to_vec()))
    }
```

  In `impl Snapshot` (near `scan_raw` at table.rs:799), mirroring `scan_from`'s range shape (711):

```rust
    /// Raw rows with `id > after_id` (all rows when `None`), ascending, at
    /// most `limit` — the migration executor's bounded batch read (spec
    /// §3: batching must bound memory, not just record size).
    pub fn scan_raw_from(
        &self,
        table: &str,
        after_id: Option<u64>,
        limit: usize,
    ) -> io::Result<Vec<(u64, Vec<u8>)>> {
        let rows = match self.txn.open_table(rows_def(table)) {
            Ok(rows) => rows,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(redb_err(e)),
        };
        let from = after_id.map_or(0, |a| a.saturating_add(1));
        let mut out = Vec::new();
        for item in rows.range(from..).map_err(redb_err)? {
            if out.len() == limit {
                break;
            }
            let (k, v) = item.map_err(redb_err)?;
            out.push((k.value(), v.value().to_vec()));
        }
        Ok(out)
    }
```

- [ ] **Step 2: db.rs record format.** `Effect` (173-193) gains:

```rust
    MigratePut {
        table: String,
        id: u64,
        row: Vec<u8>,
    },
```

  `TxEffects` (197-202) gains `migrate_puts: Vec<(&'static str, u64, Vec<u8>)>,`; `is_empty` (205-209) adds `&& self.migrate_puts.is_empty()`. `encode_record` (217-252): count `n` adds `+ effects.migrate_puts.len()`; after the emits loop:

```rust
    for (table, id, row) in &effects.migrate_puts {
        out.push(4);
        encode_bytes(table.as_bytes(), &mut out);
        id.encode(&mut out);
        encode_bytes(row, &mut out);
    }
```

  `decode_record` (254-299) gains the arm before `other`:

```rust
            4 => Effect::MigratePut {
                table: String::decode(input)?,
                id: u64::decode(input)?,
                row: Vec::<u8>::decode(input)?,
            },
```

  `Tx` gains (next to the existing `put`/`delete` methods):

```rust
    /// Buffers one decode-free migration write (spec §3). `row` must
    /// already be encoded under the table's TARGET version. Used only by
    /// `Db::migrate_schema`'s executor — deliberately not `pub`.
    pub(crate) fn migrate_put(&mut self, table: &'static str, id: u64, row: Vec<u8>) {
        self.effects.migrate_puts.push((table, id, row));
    }
```

- [ ] **Step 3: The apply arm + close-out trigger.** In `TableMachine::apply`'s effect loop (db.rs:487-500), replace the `Effect::Put` arm and add the `MigratePut` arm:

```rust
                    Effect::Put { table, row } => {
                        if table == crate::migration::SchemaVersion::TABLE {
                            // Close-out detection (spec §3): a version
                            // transition with the in-flight marker
                            // clearing rebuilds the migrated table's
                            // indexes — at apply, so primary, replay, and
                            // followers rebuild at the same log position.
                            let new = crate::migration::SchemaVersion::decode_row(row)?;
                            let old = w
                                .get_raw(table, new.id.raw())?
                                .map(|b| crate::migration::SchemaVersion::decode_row(&b))
                                .transpose()?;
                            (schema.ops(table)?.apply_put)(w, row)?;
                            let closed_out = old.as_ref().is_some_and(|o| {
                                o.migrating_to.is_some()
                                    && new.migrating_to.is_none()
                                    && new.version > o.version
                            });
                            if closed_out {
                                (schema.ops(&new.table)?.rebuild_indexes)(w)?;
                            }
                        } else {
                            (schema.ops(table)?.apply_put)(w, row)?;
                        }
                    }
                    Effect::MigratePut { table, id, row } => {
                        schema.ops(table)?; // registered-table check, same error surface
                        w.put_raw(table, *id, row)?;
                    }
```

- [ ] **Step 4: Record roundtrip test**, in db.rs's `#[cfg(test)] mod tests` (next to the existing encode/decode tests):

```rust
    #[test]
    fn migrate_put_roundtrips_through_the_record_format() {
        let mut effects = TxEffects::default();
        effects.migrate_puts.push(("Widget", 7, vec![1, 2, 3]));
        let decoded = decode_record(&encode_record(&effects)).unwrap();
        assert_eq!(
            decoded,
            vec![Effect::MigratePut {
                table: "Widget".to_string(),
                id: 7,
                row: vec![1, 2, 3],
            }]
        );
    }
```

- [ ] **Step 5: Run.** `cargo test -p charpente-core` — Expected: PASS (the new arm is unreachable until T6 writes `MigratePut`s; the trigger arm changes `Put` handling for `charpente.schema_versions` only, a table nothing writes yet).
- [ ] **Step 6: Full check + commit.**

```bash
cargo build --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check
git add crates/charpente-core/src/db.rs crates/charpente-core/src/table.rs
git commit -m "core: MigratePut record kind — decode-free apply + close-out index rebuild (migrations T4)"
```

### Task 5: The pre-replay gate — barrier, drift, downgrade, in-flight-target

**Files:**
- Modify: `crates/charpente-core/src/db.rs` (`Db::open` restructure; `GateStatus`; `TableMachine.gate`; the four named refusals; conditional checkpoint-at-open)
- Test: Create `crates/charpente-core/tests/migration_gate.rs` (integration — needs `charpente_sim::Sim`, same dev-dep reasoning as materialize.rs:1-13; charpente-sim is already a dev-dependency, Cargo.toml:14-15)

**Interfaces:**
- Consumes: T1's `SchemaVersion`/`schema_version_id`/`fingerprint_matches`; T3's `table_meta`; T4's decode-free replay of `MigratePut`.
- Produces: `Db::open` seeds a per-table `(version, fingerprint)` map from the checkpointed store BEFORE `Engine::recover`; `TableMachine` tracks it live through replay (tail adoption rows update it) and refuses any ordinary record for a version-mismatched table — barrier/downgrade/drift, named, BEFORE decoding row bytes; post-recovery the pure checks re-run and replayed-record opens checkpoint. Pure helper `gate_status(table: &str, compiled: (u32, u64), persisted: Option<&SchemaVersion>, rows_exist: bool) -> GateStatus` (pub — T9's meta-test calls it against real stores) with `pub enum GateStatus { Current, Adopt, Migrate { from: u32 }, Refuse(String) }`; message helpers `upgrade_barrier_msg`/`drift_msg`/`downgrade_msg` shared between `gate_status` and the replay check.

- [ ] **Step 1: The pure verdict fn + status enum**, in db.rs (above `impl Db`):

```rust
/// One table's gate verdict (spec §2), computed from the compiled schema
/// and the persisted `charpente.schema_versions` row. Pure — T9's
/// naive-gate meta-test runs a check-skipping variant side by side with
/// this real one against the same real store.
#[derive(Debug, Clone, PartialEq)]
pub enum GateStatus {
    /// Persisted matches compiled — nothing to do.
    Current,
    /// No persisted row: baseline (rows predate versioning) or a fresh
    /// table. Adoption happens at first write, not at open.
    Adopt,
    /// Persisted version is behind the compiled one; the executor runs
    /// (on a writable node) from `from`.
    Migrate { from: u32 },
    /// A named refusal — the message is the product surface (hard rule 5).
    Refuse(String),
}

pub fn gate_status(
    table: &str,
    compiled: (u32, u64),
    persisted: Option<&crate::migration::SchemaVersion>,
    rows_exist: bool,
) -> GateStatus {
    let (compiled_version, compiled_fingerprint) = compiled;
    let Some(row) = persisted else {
        if rows_exist && compiled_version > 1 {
            // Pre-versioning data (baseline 1) under a bumped binary:
            // migrate from 1. Post-versioning tables always have a row by
            // first-write adoption, so baseline-1 is unambiguous (spec §1).
            return GateStatus::Migrate { from: 1 };
        }
        return GateStatus::Adopt;
    };
    if let (Some(through), Some(to)) = (row.migrating_through, row.migrating_to) {
        if to != compiled_version {
            return GateStatus::Refuse(format!(
                "table \"{table}\" has an interrupted migration to version {to} in flight \
                 (resumable watermark at row id {through}) but this binary is compiled for \
                 version {compiled_version} — rows on either side of the watermark are at \
                 different versions and no single chain start covers both; finish the \
                 interrupted migration with the binary that started it, or restore"
            ));
        }
    }
    if row.version == compiled_version {
        if !crate::migration::fingerprint_matches(Some(row.fingerprint), compiled_fingerprint) {
            return GateStatus::Refuse(format!(
                "table \"{table}\"'s shape changed but its #[table(version = ...)] did not — \
                 persisted fingerprint {:#x} != compiled fingerprint {compiled_fingerprint:#x} \
                 at version {compiled_version}; bump the version and add a TableMigration, or \
                 revert the struct change",
                row.fingerprint
            ));
        }
        return GateStatus::Current;
    }
    if row.version > compiled_version {
        return GateStatus::Refuse(format!(
            "table \"{table}\" is at persisted version {} but this binary is compiled for \
             version {compiled_version} — the binary is older than the data; run a binary \
             built for at least version {}",
            row.version, row.version
        ));
    }
    GateStatus::Migrate { from: row.version }
}
```

- [ ] **Step 2: Thread the gate into replay — the dynamic version map.** A static pre-computed verdict is NOT enough: in a fully-uncheckpointed universe (first-ever writes still in the tail) the checkpointed store says "nothing persisted" and a static gate would wave replay through into the misdecode. But adoption-at-first-write (T6) guarantees the record that first writes a table also carries that table's adoption row — the tail *declares its own versions*. So replay tracks them live. `TableMachine` (465-472) gains:

```rust
    /// Per-table persisted schema state as of replay progress: seeded
    /// from the checkpointed store at open, updated whenever a
    /// `charpente.schema_versions` put applies. Consulted before any row
    /// byte decodes (spec §2 — the upgrade barrier, enforced at the exact
    /// dangerous point). Keyed by table name; value is
    /// `(version, fingerprint)`.
    versions: BTreeMap<String, (u32, u64)>,
```

  and two message helpers (free fns near `gate_status` — single source for the pinned texts):

```rust
fn upgrade_barrier_msg(table: &str, persisted: u32, compiled: u32) -> String {
    format!(
        "table \"{table}\" has unrecovered writes from the previous binary (persisted version \
         {persisted}, this binary is compiled for version {compiled}) — run the previous \
         binary once to a clean shutdown, then upgrade (spec §2's upgrade barrier)"
    )
}

fn drift_msg(table: &str, persisted_fp: u64, compiled_fp: u64, version: u32) -> String {
    format!(
        "table \"{table}\"'s shape changed but its #[table(version = ...)] did not — \
         persisted fingerprint {persisted_fp:#x} != compiled fingerprint {compiled_fp:#x} at \
         version {version}; bump the version and add a TableMigration, or revert the struct \
         change"
    )
}
```

  (`gate_status`'s drift arm calls `drift_msg` instead of its inline `format!` — one source of truth.) `TableMachine::apply` restructures its effect loop into **two passes over the same decoded record**:

```rust
            // Pass 1: schema-table effects first, so a record that both
            // adopts a table and writes its first rows has declared the
            // table's version by the time pass 2 checks it.
            for effect in &effects {
                if let Effect::Put { table, row } = effect {
                    if table == crate::migration::SchemaVersion::TABLE {
                        let new = crate::migration::SchemaVersion::decode_row(row)?;
                        let old = w
                            .get_raw(table, new.id.raw())?
                            .map(|b| crate::migration::SchemaVersion::decode_row(&b))
                            .transpose()?;
                        (schema.ops(table)?.apply_put)(w, row)?;
                        let closed_out = old.as_ref().is_some_and(|o| {
                            o.migrating_to.is_some()
                                && new.migrating_to.is_none()
                                && new.version > o.version
                        });
                        if closed_out {
                            (schema.ops(&new.table)?.rebuild_indexes)(w)?;
                        }
                        versions.insert(new.table.clone(), (new.version, new.fingerprint));
                    }
                }
            }
            // Pass 2: everything else, version-checked before any row
            // byte decodes.
            for effect in &effects {
                match effect {
                    Effect::Put { table, row } => {
                        if table == crate::migration::SchemaVersion::TABLE {
                            continue; // pass 1 applied it
                        }
                        check_replay_version(schema, versions, table)?;
                        (schema.ops(table)?.apply_put)(w, row)?;
                    }
                    Effect::Delete { table, id } => {
                        check_replay_version(schema, versions, table)?;
                        (schema.ops(table)?.apply_delete)(w, *id)?;
                    }
                    Effect::MigratePut { table, id, row } => {
                        // The rewrite itself: exempt from the version
                        // equality check (its whole point is bytes at the
                        // target version over rows at the old one), still
                        // registered-table-checked, still decode-free.
                        schema.ops(table)?;
                        w.put_raw(table, *id, row)?;
                    }
                    Effect::Enqueue { job, payload, key } => {
                        ids.push(w.materialize_job(job, payload, key.as_deref())?);
                    }
                    Effect::Emit { event, payload, key } => {
                        w.materialize_event(seq, event, payload, key.as_deref())?;
                    }
                }
            }
```

  with the check as a free fn (borrow discipline: `versions` is passed as `&BTreeMap<..>` alongside the existing `schema` binding — T4's single-pass arm is superseded by this two-pass shape):

```rust
/// Refuses, by name, replaying an ordinary record for a table whose
/// declared version/fingerprint does not match the compiled one (spec
/// §2): the barrier (version behind), the downgrade (version ahead), and
/// drift (same version, different shape) — all BEFORE decoding row bytes.
/// A fingerprinted table with NO declared version cannot occur for
/// records written by an adoption-aware binary (first-write adoption);
/// seeing one is corruption-or-foreign-data and refuses by name.
fn check_replay_version(
    schema: &Schema,
    versions: &BTreeMap<String, (u32, u64)>,
    table: &str,
) -> io::Result<()> {
    let Some((compiled_version, Some(compiled_fp))) = schema.table_meta(table) else {
        return Ok(()); // FINGERPRINT: None — not a versioned table.
    };
    let Some(&(persisted_version, persisted_fp)) = versions.get(table) else {
        return Err(io::Error::other(format!(
            "replaying a record for versioned table \"{table}\" with no adoption row on \
             record — this log was not written by an adoption-aware binary, or is corrupt; \
             restore from a replica or backup"
        )));
    };
    if persisted_version < compiled_version {
        return Err(io::Error::other(upgrade_barrier_msg(
            table,
            persisted_version,
            compiled_version,
        )));
    }
    if persisted_version > compiled_version {
        return Err(io::Error::other(format!(
            "table \"{table}\" is at persisted version {persisted_version} but this binary is \
             compiled for version {compiled_version} — the binary is older than the data; run \
             a binary built for at least version {persisted_version}"
        )));
    }
    if persisted_fp != compiled_fp {
        return Err(io::Error::other(drift_msg(
            table,
            persisted_fp,
            compiled_fp,
            compiled_version,
        )));
    }
    Ok(())
}
```

  (Records at or below `skip_through` never reach either pass — the existing early return at 476-478 stays first. The downgrade message here and in `gate_status` must be byte-identical — extract it into a third helper `downgrade_msg(table, persisted, compiled)` used by both, same as `drift_msg`.)

- [ ] **Step 3: `Db::open` restructure.** After the store opens and `skip_through` is known (db.rs:603), before `TableMachine` construction (651), seed the version map from the checkpointed store:

```rust
        // Seed the replay-time version map (Step 2) from the CHECKPOINTED
        // store; tail records update it live as their adoption/watermark
        // rows apply.
        let mut versions: BTreeMap<String, (u32, u64)> = BTreeMap::new();
        {
            let snap = store.snapshot()?;
            for table in schema.table_names() {
                if !matches!(schema.table_meta(table), Some((_, Some(_)))) {
                    continue; // FINGERPRINT: None — not a versioned table.
                }
                if let Some(row) = snap.get::<crate::migration::SchemaVersion>(
                    crate::migration::schema_version_id(table),
                )? {
                    versions.insert(row.table.clone(), (row.version, row.fingerprint));
                }
            }
        }
```

  Pass it into `TableMachine { store, schema, skip_through, enqueued: Vec::new(), versions }`. (Note the seed loop reads the `SchemaVersion` rows only — `rows_exist` is not needed here; pre-feature baseline handling is `gate_status`'s job in the post-replay check and `migrate_schema`, and a pre-feature table cannot appear in a tail written by this pre-release codebase.) After `Engine::recover` succeeds and the existing outran-durable-truth assert (661-668), replace the direct `Ok(Db { .. })` tail (669-676) with:

```rust
        let replayed = engine.last_applied() != skip_through;
        let mut db = Db {
            engine,
            cfg,
            retention_pin: None,
            records_since_checkpoint: 0,
            not_primary: None,
            adopted: BTreeSet::new(),
        };
        // Post-replay: the authoritative pure checks against the
        // now-current store (a drifted/downgraded table with a CLEAN tail
        // still refuses; a bump with a clean tail is fine here — the
        // executor runs later, on a writable node, via migrate_schema).
        db.check_schema_gate()?;
        db.reload_adopted()?;
        // Checkpoint-at-open-after-replay (spec §2): makes "boot the
        // previous binary once" the complete barrier remedy. Conditional
        // on having replayed so no-op opens add zero disk ops (the
        // pinned-seed rule, same rationale as adoption-at-first-write).
        if replayed {
            db.checkpoint()?;
        }
        Ok(db)
```

  with the two helpers on `impl Db` (near `poisoned`):

```rust
    /// The pure post-replay gate walk: returns the first named refusal.
    fn check_schema_gate(&self) -> io::Result<()> {
        let snap = self.snapshot()?;
        let schema = &self.engine.machine().schema;
        for table in schema.table_names() {
            let Some((version, Some(fingerprint))) = schema.table_meta(table) else {
                continue;
            };
            let persisted: Option<crate::migration::SchemaVersion> =
                snap.get(crate::migration::schema_version_id(table))?;
            let rows_exist = !snap.scan_raw_from(table, None, 1)?.is_empty();
            if let GateStatus::Refuse(msg) =
                gate_status(table, (version, fingerprint), persisted.as_ref(), rows_exist)
            {
                return Err(io::Error::other(msg));
            }
        }
        Ok(())
    }

    /// Rebuilds the adopted-set (T6's first-write adoption) from the
    /// store. Called at open and again by migrate_schema (a promoted
    /// follower's rows arrived by shipping after its open).
    fn reload_adopted(&mut self) -> io::Result<()> {
        let rows: Vec<crate::migration::SchemaVersion> = self.snapshot()?.scan()?;
        self.adopted = rows.into_iter().map(|r| r.table).collect();
        Ok(())
    }
```

  `Db` gains the field `adopted: BTreeSet<String>,` (add `use std::collections::BTreeSet;`), and `Debug`'s hand-written impl (529-538) is untouched (the field is omitted like `retention_pin`).

- [ ] **Step 4: Gate tests**, `crates/charpente-core/tests/migration_gate.rs` (new file; fixtures follow the dual `Row`+`RowShape` pattern from T3's unit tests, with distinct fingerprints per shape — `WidgetV1`-as-Row fp `0x1111`, `WidgetDrifted` same-table same-version fp `0x2222`, `Widget2` version-2 fp `0x4444`):

```rust
//! The Db::open schema gate (spec §2): refusals fire pre-serving, the
//! upgrade barrier protects a foreign WAL tail, adoption waits for first
//! write.
use std::io;

use charpente_core::db::{Db, DbConfig, Schema};
use charpente_core::migration::{
    MigrationError, SchemaVersion, TableMigration, schema_version_id,
};
use charpente_core::table::{Codec, Id, IndexDef, Row, RowShape, View};
use charpente_sim::{FaultConfig, Sim};

struct WidgetV1 {
    id: Id<WidgetV1>,
    count: u32,
}
impl Row for WidgetV1 {
    const TABLE: &'static str = "Widget";
    const INDEXES: &'static [IndexDef<WidgetV1>] = &[];
    const VERSION: u32 = 1;
    const FINGERPRINT: Option<u64> = Some(0x1111);
    fn id(&self) -> Id<WidgetV1> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.count.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<WidgetV1> {
        let input = &mut &bytes[..];
        Ok(WidgetV1 { id: Id::decode(input)?, count: u32::decode(input)? })
    }
}
impl RowShape for WidgetV1 {
    const VERSION: u32 = 1;
    fn encode_row(&self, out: &mut Vec<u8>) {
        Row::encode_row(self, out)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<WidgetV1> {
        <WidgetV1 as Row>::decode_row(bytes)
    }
}

struct WidgetDrifted {
    id: Id<WidgetDrifted>,
    count: u32,
    extra: bool,
}
impl Row for WidgetDrifted {
    const TABLE: &'static str = "Widget";
    const INDEXES: &'static [IndexDef<WidgetDrifted>] = &[];
    const VERSION: u32 = 1;
    const FINGERPRINT: Option<u64> = Some(0x2222);
    fn id(&self) -> Id<WidgetDrifted> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.count.encode(out);
        self.extra.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<WidgetDrifted> {
        let input = &mut &bytes[..];
        Ok(WidgetDrifted {
            id: Id::decode(input)?,
            count: u32::decode(input)?,
            extra: bool::decode(input)?,
        })
    }
}

struct Widget2 {
    id: Id<Widget2>,
    count: u32,
    label: String,
}
impl Row for Widget2 {
    const TABLE: &'static str = "Widget";
    const INDEXES: &'static [IndexDef<Widget2>] = &[];
    const VERSION: u32 = 2;
    const FINGERPRINT: Option<u64> = Some(0x4444);
    fn id(&self) -> Id<Widget2> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.count.encode(out);
        self.label.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<Widget2> {
        let input = &mut &bytes[..];
        Ok(Widget2 {
            id: Id::decode(input)?,
            count: u32::decode(input)?,
            label: String::decode(input)?,
        })
    }
}
impl RowShape for Widget2 {
    const VERSION: u32 = 2;
    fn encode_row(&self, out: &mut Vec<u8>) {
        Row::encode_row(self, out)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<Widget2> {
        <Widget2 as Row>::decode_row(bytes)
    }
}
impl TableMigration for Widget2 {
    type Prev = WidgetV1;
    fn migrate(prev: WidgetV1) -> Result<Widget2, MigrationError> {
        Ok(Widget2 {
            id: Id::from_raw(prev.id.raw()),
            count: prev.count,
            label: "migrated".to_string(),
        })
    }
}

fn cfg() -> DbConfig {
    DbConfig { store_path: "t.redb".into(), ..DbConfig::default() }
}

fn seed_v1(sim: &Sim, rows: u64) {
    let mut db =
        Db::open(Box::new(sim.disk()), cfg(), Schema::new().table::<WidgetV1>()).unwrap();
    db.transact(|tx| -> Result<(), ()> {
        for i in 0..rows {
            tx.put(&WidgetV1 { id: Id::from_raw(i), count: i as u32 });
        }
        Ok(())
    })
    .unwrap();
    db.checkpoint().unwrap(); // clean shutdown: the tail is drained
}

#[test]
fn adoption_waits_for_first_write_and_lands_with_it() {
    let sim = Sim::with_faults(0x2EB7_0001, FaultConfig::none());
    let db = Db::open(Box::new(sim.disk()), cfg(), Schema::new().table::<WidgetV1>()).unwrap();
    // Open writes nothing (spec §1: adoption-at-first-write).
    assert_eq!(db.last_applied(), None);
    let row: Option<SchemaVersion> =
        db.snapshot().unwrap().get(schema_version_id("Widget")).unwrap();
    assert!(row.is_none());
    drop(db);
    let mut db = Db::open(Box::new(sim.disk()), cfg(), Schema::new().table::<WidgetV1>()).unwrap();
    db.transact(|tx| -> Result<(), ()> {
        tx.put(&WidgetV1 { id: Id::from_raw(1), count: 5 });
        Ok(())
    })
    .unwrap();
    let row: SchemaVersion = db
        .snapshot()
        .unwrap()
        .get(schema_version_id("Widget"))
        .unwrap()
        .expect("first write must carry the adoption row");
    assert_eq!((row.version, row.fingerprint), (1, 0x1111));
    assert_eq!((row.migrating_through, row.migrating_to), (None, None));
}

#[test]
fn same_version_fingerprint_drift_refuses_to_open() {
    let sim = Sim::with_faults(0x2EB7_0002, FaultConfig::none());
    seed_v1(&sim, 1);
    let err =
        Db::open(Box::new(sim.disk()), cfg(), Schema::new().table::<WidgetDrifted>()).unwrap_err();
    assert_eq!(
        err.to_string(),
        "table \"Widget\"'s shape changed but its #[table(version = ...)] did not — \
         persisted fingerprint 0x1111 != compiled fingerprint 0x2222 at version 1; bump the \
         version and add a TableMigration, or revert the struct change"
    );
}

#[test]
fn version_downgrade_refuses_to_open() {
    let sim = Sim::with_faults(0x2EB7_0003, FaultConfig::none());
    seed_v1(&sim, 1);
    {
        let schema = Schema::new().table::<Widget2>().migration::<Widget2>("Widget");
        let mut db = Db::open(Box::new(sim.disk()), cfg(), schema).unwrap();
        db.migrate_schema().unwrap();
        db.checkpoint().unwrap();
    }
    let err =
        Db::open(Box::new(sim.disk()), cfg(), Schema::new().table::<WidgetV1>()).unwrap_err();
    assert_eq!(
        err.to_string(),
        "table \"Widget\" is at persisted version 2 but this binary is compiled for version 1 \
         — the binary is older than the data; run a binary built for at least version 2"
    );
}

#[test]
fn upgrade_barrier_refuses_a_foreign_tail_and_old_binary_recovery_clears_it() {
    let sim = Sim::with_faults(0x2EB7_0004, FaultConfig::none());
    {
        // Seed WITHOUT the clean-shutdown checkpoint: the tail stays
        // foreign to a bumped binary.
        let mut db =
            Db::open(Box::new(sim.disk()), cfg(), Schema::new().table::<WidgetV1>()).unwrap();
        db.transact(|tx| -> Result<(), ()> {
            tx.put(&WidgetV1 { id: Id::from_raw(1), count: 5 });
            Ok(())
        })
        .unwrap();
        // dropped un-checkpointed — records live only in the WAL tail
    }
    let schema = || Schema::new().table::<Widget2>().migration::<Widget2>("Widget");
    let err = Db::open(Box::new(sim.disk()), cfg(), schema()).unwrap_err();
    assert_eq!(
        err.to_string(),
        "table \"Widget\" has unrecovered writes from the previous binary (persisted version \
         1, this binary is compiled for version 2) — run the previous binary once to a clean \
         shutdown, then upgrade (spec §2's upgrade barrier)"
    );
    // The remedy: one old-binary open replays + checkpoints (spec §2's
    // checkpoint-at-open-after-replay)...
    drop(Db::open(Box::new(sim.disk()), cfg(), Schema::new().table::<WidgetV1>()).unwrap());
    // ...and the bumped binary now opens and migrates clean.
    let mut db = Db::open(Box::new(sim.disk()), cfg(), schema()).unwrap();
    db.migrate_schema().unwrap();
    let rows: Vec<Widget2> = db.snapshot().unwrap().scan().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].label, "migrated");
}
```

  Wait to run the downgrade/barrier tests until T6 lands `migrate_schema` — this task's Step 5 runs only the first two; mark the other two `#[ignore = "migrate_schema lands in T6"]` in THIS commit and un-ignore them in T6's commit. (Baseline note: `gate_status` returns `Migrate { from: 1 }` for pre-feature data only when `compiled_version > 1`; a same-version-1 open of pre-feature rows is `Adopt` — covered by `adoption_waits_for_first_write_and_lands_with_it`'s reopen.)

- [ ] **Step 5: Run.** `cargo test -p charpente-core --test migration_gate` — Expected: 2 PASS, 2 ignored. Full `cargo test -p charpente-core` — Expected: PASS (fresh stores replay nothing, so the conditional checkpoint and read-only gate leave existing universes untouched — if any pre-existing core test fails, STOP and diagnose before continuing; that is the frozen-replay contract tripping).
- [ ] **Step 6: Full check + commit.**

```bash
cargo build --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check
git add crates/charpente-core/src/db.rs crates/charpente-core/tests/migration_gate.rs
git commit -m "core: pre-replay schema gate — barrier, drift, downgrade, in-flight-target refusals (migrations T5)"
```

### Task 6: `Db::migrate_schema` — the executor — and adoption-at-first-write

**Files:**
- Modify: `crates/charpente-core/src/db.rs`
- Test: `crates/charpente-core/tests/migration_gate.rs` (un-ignore T5's two; add executor tests)

**Interfaces:**
- Consumes: T4's `Tx::migrate_put`/`scan_raw_from`, T5's `gate_status`/`GateStatus`/`adopted`/`reload_adopted`.
- Produces: `pub fn Db::migrate_schema(&mut self) -> io::Result<()>` — the writable-node gate: verifies chains (named missing-link refusal), runs the batched resumable executor, close-out bumps the version (whose apply triggers T4's index rebuild). Adoption puts injected into first-write transactions. Helper `fn tx_err_to_io<E: fmt::Debug>(e: TxError<E>) -> io::Error`.

- [ ] **Step 1: `tx_err_to_io`** (near `TxError`'s definition, db.rs:75-95):

```rust
fn tx_err_to_io<E: fmt::Debug>(e: TxError<E>) -> io::Error {
    match e {
        TxError::Io(e) => e,
        TxError::App(e) => io::Error::other(format!("schema migration transact rejected: {e:?}")),
        TxError::NotPrimary { primary } => io::Error::other(format!(
            "schema migration ran on a non-primary node (primary: {primary}) — unreachable: \
             migrate_schema asserts writes are allowed before running"
        )),
    }
}
```

- [ ] **Step 2: Adoption in `transact`.** In `Db::transact` (682-722), after the `not_primary` check (700-704) and before `encode_record` (705), inject adoptions:

```rust
        let adoption_tables: Vec<&'static str> = tx
            .effects
            .overlay
            .keys()
            .copied()
            .filter(|t| {
                *t != crate::migration::SchemaVersion::TABLE
                    && !self.adopted.contains(*t)
                    && self
                        .engine
                        .machine()
                        .schema
                        .table_meta(t)
                        .is_some_and(|(_, fp)| fp.is_some())
            })
            .collect();
        for table in &adoption_tables {
            let (version, fingerprint) = self
                .engine
                .machine()
                .schema
                .table_meta(table)
                .expect("filtered on table_meta above");
            let vid = crate::migration::schema_version_id(table);
            // Skip if this very transaction already writes the table's
            // schema row (the executor's watermark/close-out puts).
            let already = tx
                .effects
                .overlay
                .get(crate::migration::SchemaVersion::TABLE)
                .is_some_and(|m| m.contains_key(&vid.raw()));
            if already {
                continue;
            }
            let row = crate::migration::SchemaVersion {
                id: vid,
                table: (*table).to_string(),
                version,
                fingerprint: fingerprint.expect("filtered on Some above"),
                migrating_through: None,
                migrating_to: None,
            };
            let mut bytes = Vec::new();
            row.encode_row(&mut bytes);
            tx.effects
                .overlay
                .entry(crate::migration::SchemaVersion::TABLE)
                .or_default()
                .insert(vid.raw(), Some(bytes));
        }
```

  and after the successful `self.engine.submit(&record)` (712), before the checkpoint cadence:

```rust
        for table in adoption_tables {
            self.adopted.insert(table.to_string());
        }
```

- [ ] **Step 3: `migrate_schema`** on `impl Db` (after `check_schema_gate`):

```rust
    /// The writable-node half of the schema gate (spec §2/§3): verifies
    /// each behind table's chain, then runs the batched, resumable,
    /// decode-free rewrite. Call sites: the kernel's startup (single-node
    /// and primary — `Kernel::run`) and `do_promote` (a promoted follower
    /// resumes any in-flight watermark). Followers never call this — they
    /// receive migration through log shipping (spec §4).
    pub fn migrate_schema(&mut self) -> io::Result<()> {
        assert!(
            self.writes_allowed(),
            "migrate_schema on a write-forbidden node — the gate is role-aware (spec §2): \
             call it only on single-node/primary at startup, or at promote"
        );
        self.reload_adopted()?;
        let tables: Vec<&'static str> = self.engine.machine().schema.table_names();
        for table in tables {
            let Some((compiled_version, Some(compiled_fingerprint))) =
                self.engine.machine().schema.table_meta(table)
            else {
                continue;
            };
            let vid = crate::migration::schema_version_id(table);
            let snap = self.snapshot()?;
            let persisted: Option<crate::migration::SchemaVersion> = snap.get(vid)?;
            let rows_exist = !snap.scan_raw_from(table, None, 1)?.is_empty();
            drop(snap);
            let from = match gate_status(
                table,
                (compiled_version, compiled_fingerprint),
                persisted.as_ref(),
                rows_exist,
            ) {
                GateStatus::Current | GateStatus::Adopt => continue,
                GateStatus::Refuse(msg) => return Err(io::Error::other(msg)),
                GateStatus::Migrate { from } => from,
            };
            // Chain existence, before touching anything.
            let steps: Vec<crate::migration::MigrationStep> =
                self.engine.machine().schema.migrations_for(table).to_vec();
            let mut cursor = from;
            while cursor != compiled_version {
                let Some(step) = steps.iter().find(|s| s.from_version == cursor) else {
                    return Err(io::Error::other(format!(
                        "table \"{table}\" has no registered migration from version {cursor} — \
                         persisted version is {from}, compiled version is {compiled_version}; \
                         register Schema::migration::<Step>(\"{table}\") for the \
                         version-{cursor} step, or this data cannot be opened by this binary"
                    )));
                };
                cursor = step.to_version;
            }
            self.migrate_table(table, from, compiled_version, compiled_fingerprint, &steps)?;
        }
        Ok(())
    }

    /// Spec §3's loop. Every batch commits its `MigratePut`s AND the
    /// watermark advance in ONE transaction — a crash between them is
    /// impossible; resumption is re-entry. The close-out transaction is
    /// the version bump whose apply rebuilds the table's indexes (T4).
    fn migrate_table(
        &mut self,
        table: &'static str,
        from: u32,
        to: u32,
        fingerprint: u64,
        steps: &[crate::migration::MigrationStep],
    ) -> io::Result<()> {
        let vid = crate::migration::schema_version_id(table);
        let batch_size = self.cfg.migration_batch_size as usize;
        loop {
            let snap = self.snapshot()?;
            let watermark: Option<u64> = snap
                .get::<crate::migration::SchemaVersion>(vid)?
                .and_then(|v| v.migrating_through);
            let batch = snap.scan_raw_from(table, watermark, batch_size)?;
            drop(snap);
            if batch.is_empty() {
                self.transact(|tx| -> Result<(), io::Error> {
                    tx.put(&crate::migration::SchemaVersion {
                        id: vid,
                        table: table.to_string(),
                        version: to,
                        fingerprint,
                        migrating_through: None,
                        migrating_to: None,
                    });
                    Ok(())
                })
                .map_err(tx_err_to_io)?;
                return Ok(());
            }
            let new_watermark = batch.last().map(|(id, _)| *id);
            let mut migrated: Vec<(u64, Vec<u8>)> = Vec::with_capacity(batch.len());
            for (id, bytes) in batch {
                let mut version = from;
                let mut bytes = bytes;
                while version != to {
                    let step = steps
                        .iter()
                        .find(|s| s.from_version == version)
                        .expect("chain verified by migrate_schema");
                    bytes = (step.apply)(&bytes).map_err(|e| {
                        io::Error::other(format!("migrating table \"{table}\" row {id}: {e}"))
                    })?;
                    version = step.to_version;
                }
                migrated.push((id, bytes));
            }
            self.transact(|tx| -> Result<(), io::Error> {
                for (id, bytes) in migrated {
                    tx.migrate_put(table, id, bytes);
                }
                tx.put(&crate::migration::SchemaVersion {
                    id: vid,
                    table: table.to_string(),
                    version: from, // not done until the close-out above
                    fingerprint,
                    migrating_through: new_watermark,
                    migrating_to: Some(to),
                });
                Ok(())
            })
            .map_err(tx_err_to_io)?;
        }
    }
```

  Resume note the implementer must preserve: a resumed migration (watermark `Some`) re-enters this loop with `from` still the *persisted* version (the schema row's `version` field never changes until close-out), and rows at-or-below the watermark are already at `to` — the `scan_raw_from(table, watermark, ..)` read never touches them, so the chain never re-decodes migrated bytes. That invariant is exactly what T9's crash property verifies.

- [ ] **Step 4: Un-ignore T5's two tests; add executor tests** to migration_gate.rs:

```rust
#[test]
fn migration_rewrites_every_row_and_batches_bound_the_records() {
    let sim = Sim::with_faults(0x2EB7_0005, FaultConfig::none());
    seed_v1(&sim, 5);
    let schema = Schema::new().table::<Widget2>().migration::<Widget2>("Widget");
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig { migration_batch_size: 2, ..cfg() },
        schema,
    )
    .unwrap();
    let before = db.last_applied();
    db.migrate_schema().unwrap();
    let rows: Vec<Widget2> = db.snapshot().unwrap().scan().unwrap();
    assert_eq!(rows.len(), 5);
    for row in &rows {
        assert_eq!(row.label, "migrated");
        assert_eq!(row.count, row.id.raw() as u32);
    }
    let version: SchemaVersion =
        db.snapshot().unwrap().get(schema_version_id("Widget")).unwrap().unwrap();
    assert_eq!(version.version, 2);
    assert_eq!((version.migrating_through, version.migrating_to), (None, None));
    // batch=2 over 5 rows: 3 batch records + 1 close-out — MORE than one
    // record, proving it did not commit as a single giant transaction.
    let applied = db.last_applied().unwrap() - before.unwrap_or(0);
    assert_eq!(applied, 4);
}

#[test]
fn a_rejecting_migration_names_the_row_and_leaves_the_store_untouched() {
    struct Widget2Rejecting {
        id: Id<Widget2Rejecting>,
        count: u32,
    }
    impl Row for Widget2Rejecting {
        const TABLE: &'static str = "Widget";
        const INDEXES: &'static [IndexDef<Widget2Rejecting>] = &[];
        const VERSION: u32 = 2;
        const FINGERPRINT: Option<u64> = Some(0x6666);
        fn id(&self) -> Id<Widget2Rejecting> {
            self.id
        }
        fn encode_row(&self, out: &mut Vec<u8>) {
            self.id.encode(out);
            self.count.encode(out);
        }
        fn decode_row(bytes: &[u8]) -> io::Result<Widget2Rejecting> {
            let input = &mut &bytes[..];
            Ok(Widget2Rejecting { id: Id::decode(input)?, count: u32::decode(input)? })
        }
    }
    impl RowShape for Widget2Rejecting {
        const VERSION: u32 = 2;
        fn encode_row(&self, out: &mut Vec<u8>) {
            Row::encode_row(self, out)
        }
        fn decode_row(bytes: &[u8]) -> io::Result<Widget2Rejecting> {
            <Widget2Rejecting as Row>::decode_row(bytes)
        }
    }
    impl TableMigration for Widget2Rejecting {
        type Prev = WidgetV1;
        fn migrate(prev: WidgetV1) -> Result<Widget2Rejecting, MigrationError> {
            if prev.count == 2 {
                return Err(MigrationError::Reject(format!(
                    "row {} has count 2, which this migration deliberately refuses",
                    prev.id.raw()
                )));
            }
            Ok(Widget2Rejecting { id: Id::from_raw(prev.id.raw()), count: prev.count })
        }
    }

    let sim = Sim::with_faults(0x2EB7_0006, FaultConfig::none());
    seed_v1(&sim, 4); // row 2 has count 2
    let schema = Schema::new()
        .table::<Widget2Rejecting>()
        .migration::<Widget2Rejecting>("Widget");
    let mut db = Db::open(Box::new(sim.disk()), cfg(), schema).unwrap();
    let err = db.migrate_schema().unwrap_err();
    assert_eq!(
        err.to_string(),
        "migrating table \"Widget\" row 2: row 2 has count 2, which this migration \
         deliberately refuses"
    );
    drop(db);
    // Nothing partially committed: the original v1 schema still opens and
    // every row is intact.
    let db = Db::open(Box::new(sim.disk()), cfg(), Schema::new().table::<WidgetV1>()).unwrap();
    let mut rows: Vec<WidgetV1> = db.snapshot().unwrap().scan().unwrap();
    rows.sort_by_key(|r| r.id.raw());
    let counts: Vec<u32> = rows.iter().map(|r| r.count).collect();
    assert_eq!(counts, vec![0, 1, 2, 3]);
}
```

  And the missing-chain-link verbatim pin (every one of spec §5's seven named errors must have a byte-exact pin somewhere in T5/T6/T8/T9; this is the one the others don't cover):

```rust
#[test]
fn missing_chain_link_refuses_by_name() {
    let sim = Sim::with_faults(0x2EB7_0008, FaultConfig::none());
    seed_v1(&sim, 1);
    // Compiled to version 2 with NO .migration() registered — the gap.
    let mut db =
        Db::open(Box::new(sim.disk()), cfg(), Schema::new().table::<Widget2>()).unwrap();
    let err = db.migrate_schema().unwrap_err();
    assert_eq!(
        err.to_string(),
        "table \"Widget\" has no registered migration from version 1 — persisted version is \
         1, compiled version is 2; register Schema::migration::<Step>(\"Widget\") for the \
         version-1 step, or this data cannot be opened by this binary"
    );
}
```

  (Atomicity subtlety the second test leans on: with `migration_batch_size` at the default 1000 the failing batch is the FIRST batch, its transact never commits, so no `migrating_to` marker ever lands and the v1 reopen sails through the gate. A batch-size-2 variant of the rejection scenario — where batch 1 commits and batch 2 rejects — leaves an in-flight marker and the v1 reopen then refuses with the in-flight-target error; that discriminating pair is T9's `rejecting_migration_mid_chain_leaves_a_named_in_flight_refusal` — do not "fix" it here.)

- [ ] **Step 5: Run.** `cargo test -p charpente-core --test migration_gate` — Expected: all 6 PASS (T5's two un-ignored included). `cargo test -p charpente-core` full — PASS.
- [ ] **Step 6: Full check + commit.**

```bash
cargo build --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check
git add crates/charpente-core/src/db.rs crates/charpente-core/tests/migration_gate.rs
git commit -m "core: migrate_schema — batched resumable decode-free executor + first-write adoption (migrations T6)"
```

### Task 7: Kernel and promote integration + clean-shutdown checkpoint

**Files:**
- Modify: `crates/charpente/src/kernel/mod.rs` (`Kernel::run` at line 180: gate call at top, checkpoint at clean exit)
- Modify: `crates/charpente/src/repl/mod.rs` (`do_promote` at line 507: gate call after `allow_writes`)
- Test: `crates/charpente/tests/kernel.rs` (extend)

**Interfaces:**
- Consumes: T6's `Db::migrate_schema`, `Db::checkpoint`, `Db::writes_allowed`, `Db::poisoned`.
- Produces: the gate runs before the first served request on every writable node; a promoted follower migrates before acking the promote; a clean kernel exit checkpoints (spec §2's clean-shutdown half).

- [ ] **Step 1: Kernel startup gate + exit checkpoint.** In `Kernel::run` (kernel/mod.rs:180), the `App`'s `Db` lives at `self.app.db: Option<Db>` (http/app.rs:156-157 — `Option` only for the reseed swap; confirm the access path with `grep -n "app.db\|\.db\.as_mut" crates/charpente/src/kernel/mod.rs`). At the very top of `run`, before the loop:

```rust
        // The role-aware schema gate (spec §2): single-node and primary
        // kernels migrate before serving anything; followers skip (their
        // migration arrives by shipping) and run it at promote instead.
        if let Some(db) = self.app.db.as_mut() {
            if db.writes_allowed() {
                db.migrate_schema()?;
            }
        }
```

  At `run`'s clean-exit path — every `return Ok(...)`/loop-break that is NOT the poisoned-stop (the request-arm `Db::poisoned()` stop from step 7 keeps its current behavior — checkpointing a poisoned engine would mask the poison):

```rust
        if let Some(db) = self.app.db.as_mut() {
            if !db.poisoned() {
                db.checkpoint()?;
            }
        }
```

  Find the exit sites with `grep -n "return Ok\|break" crates/charpente/src/kernel/mod.rs` within `run`/`step` and place the checkpoint at `run`'s exits only (not `step`'s — `step` is the per-iteration unit and must stay checkpoint-free or the cadence contract shifts).

- [ ] **Step 2: Promote gate.** In `do_promote` (repl/mod.rs:507), directly after `db.allow_writes();` (line ~549) and before the `reply_ctrl(.. CtrlOk ..)`:

```rust
        // A promoted follower is now the writable node: run the schema
        // gate before acking — it resumes any in-flight watermark the dead
        // primary left mid-migration (spec §4).
        db.migrate_schema()?;
```

  (On `Err` this bubbles out of `do_promote` — node-fatal, the fail-stop direction; the operator sees the named migration error, and the role file already durably says Primary so a restart re-attempts the migration at open via the kernel gate.)

- [ ] **Step 3: Kernel test**, in `crates/charpente/tests/kernel.rs` — a versioned app table migrates before the first request is served. Follow this file's existing app-construction pattern (find it: `grep -n "fn.*app\|Kernel::new" crates/charpente/tests/kernel.rs`); the assertion shape:

```rust
// Seed a v1 store via a bare Db (checkpoint = clean shutdown), then build
// the app/kernel against the v2 schema and step it once: the first
// request/tick observes ONLY migrated rows.
```

  Concretely: seed with a hand-written v1 fixture (same dual-impl pattern as migration_gate.rs), construct the kernel whose app schema registers the v2 type + migration, run one `step`, then scan through `db.snapshot()` and assert every row carries the migrated field. Seed `0x2EB7_0007`.

- [ ] **Step 4: Run.** `cargo test -p charpente --test kernel` — Expected: PASS, including every pre-existing kernel test (fresh single-node opens: gate finds all tables `Adopt`/`Current`, writes nothing; clean-exit checkpoint fires only where `run` exits — if a pinned kernel seed (`0x348270b38f166529`) fails here, STOP: that is the frozen-replay contract; diagnose whether the exit checkpoint shifted its fault schedule before touching anything else).
- [ ] **Step 5: Full check + commit.**

```bash
cargo build --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check
git add crates/charpente/src/kernel/mod.rs crates/charpente/src/repl/mod.rs crates/charpente/tests/kernel.rs
git commit -m "kernel/repl: role-aware gate at startup and promote + clean-exit checkpoint (migrations T7)"
```

### Task 8: Replication `Hello.schema_digest`

**Files:**
- Modify: `crates/charpente-core/src/db.rs` (`Db::schema_digest`)
- Modify: `crates/charpente/src/repl/protocol.rs` (field + codec + roundtrip tests)
- Modify: `crates/charpente/src/repl/session.rs` (the refusal const + `decide_hello`)
- Modify: `crates/charpente/src/repl/follower.rs` (outbound attach at the sole construction site, line ~314)
- Test: `crates/charpente/tests/replication.rs` (the digest property + the four broken literals)

**Interfaces:**
- Consumes: T3's `table_meta`/`table_names`, T1's `fnv1a_hash`.
- Produces: `pub fn Db::schema_digest(&self) -> u64`; `Msg::Hello.schema_digest: u64` (wire-appended after `secret`); `SCHEMA_DIGEST_MISMATCH` refusal, checked in `decide_hello` after the `proto` check (secret proof stays first — session.rs:538-540 secret, 541-547 proto, insert after 547 before `match role` at 548).

- [ ] **Step 1: `Db::schema_digest`** (db.rs, near `table_names`):

```rust
    /// One aggregate over every versioned table's `(name, version,
    /// fingerprint)`, sorted by name (BTreeMap order). `FINGERPRINT: None`
    /// tables are excluded — platform tables are covered by PROTO_VERSION,
    /// and a hand-written app `Row` impl opts out of the digest exactly as
    /// it opts out of the gate (spec §4 scope note).
    pub fn schema_digest(&self) -> u64 {
        let schema = &self.engine.machine().schema;
        let mut buf = Vec::new();
        for table in schema.table_names() {
            let Some((version, Some(fingerprint))) = schema.table_meta(table) else {
                continue;
            };
            buf.extend_from_slice(table.as_bytes());
            buf.push(0);
            buf.extend_from_slice(&version.to_le_bytes());
            buf.extend_from_slice(&fingerprint.to_le_bytes());
        }
        crate::migration::fnv1a_hash(&buf)
    }
```

- [ ] **Step 2: Protocol.** `Msg::Hello` (protocol.rs:24-29): add `schema_digest: u64,` after `secret`. Encode arm (150-161): `schema_digest.encode(&mut out);` after `secret.encode(&mut out);` (line 160). Decode arm (228-233): `schema_digest: u64::decode(input)?,` after the `secret` line (232). Update both `roundtrips(Msg::Hello { ... })` tests (~310/~316): one with `schema_digest: 0`, one with a nonzero value.
- [ ] **Step 3: The refusal.** session.rs, next to `PEER_SECRET_REFUSAL` (503-504):

```rust
const SCHEMA_DIGEST_MISMATCH: &str =
    "replication schema mismatch: this node's #[table] schema digest does not match the \
     peer's — one side is running a binary compiled against a different table shape; rebuild \
     both nodes from the same source and restart them (stop both, upgrade both, start the \
     primary first)";
```

  `decide_hello` (530-537) gains a `schema_digest: u64` parameter (threaded from the `Msg::Hello` destructure in `handle` at 267-273); after the proto check (547), before `match role` (548):

```rust
    if schema_digest != db.schema_digest() {
        return Ok(Decision::Refuse(SCHEMA_DIGEST_MISMATCH.to_string()));
    }
```

- [ ] **Step 4: Outbound + existing literals.** The sole outbound construction is follower.rs:314 (`db` is in scope there): add `schema_digest: db.schema_digest(),`. Then fix the four hand-built literals in `crates/charpente/tests/replication.rs` (grep `Msg::Hello {` to locate them; the line numbers drift):
  - ~1455 (bad-proto test) and ~1541/~1557 (peer-secret tests): `schema_digest: 0,` — each is refused before the digest check, and a placeholder keeps that true (the tests assert the EARLIER refusal, which still must win the ordering).
  - ~1485 (`hello_to_a_follower_is_refused_naming_the_not_a_primary_state`): control reaches the digest check, so the digest must MATCH — pre-compute before the call (a borrow of `f` inside `exchange_hello(&sim, &mut f, ...)`'s argument list conflicts):

```rust
    let digest = f.db.as_ref().unwrap().schema_digest();
    // ...then inside the Msg::Hello literal:
    schema_digest: digest,
```

- [ ] **Step 5: The digest property**, appended to replication.rs near `peer_secret_refuses_mismatch_and_admits_match` (1527), reusing its exact rig:

```rust
#[test]
fn hello_schema_digest_mismatch_refuses_zero_state_touched_and_match_converges() {
    let sim = Sim::with_faults(0x2EB8_0001, FaultConfig::none());
    let db_cfg = seeding_db_cfg();
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), db_cfg.clone()).unwrap();
    let real = p.db.as_ref().unwrap().schema_digest();

    let reply = exchange_hello(
        &sim,
        &mut p,
        "p:1",
        Msg::Hello {
            proto: charpente::repl::PROTO_VERSION,
            listen: Some("f:1".to_string()),
            position: None,
            secret: None,
            schema_digest: real.wrapping_add(1),
        },
    )
    .unwrap();
    match &reply {
        Msg::Refuse { message } => assert_eq!(
            message.as_str(),
            "replication schema mismatch: this node's #[table] schema digest does not match \
             the peer's — one side is running a binary compiled against a different table \
             shape; rebuild both nodes from the same source and restart them (stop both, \
             upgrade both, start the primary first)"
        ),
        other => panic!("expected the schema-digest Refuse, got {other:?}"),
    }
    drive_rounds(&sim, &mut [&mut p], 4).unwrap();
    assert_eq!(p.repl.stats().seeds, 0, "a digest-refused Hello must never begin a seed");

    // Happy path: a real follower's auto-attached digest matches and
    // convergence is unaffected.
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), db_cfg).unwrap();
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    assert_converged(&p, &f).unwrap();
}
```

- [ ] **Step 6: Run.** `cargo test -p charpente-core schema_digest` (add two digest unit tests beside T3's `schema_migrations` module: equal schemas → equal digests, drifted fingerprint → different digests — pure `Schema` construction, no `Db` needed since `schema_digest` reads only the schema… note: it is a method on `Db`; ALSO expose the fold as `pub(crate) fn schema_digest_of(schema: &Schema) -> u64` and have `Db::schema_digest` delegate, so the unit tests hit the pure fn). Then `cargo test -p charpente --test replication` (the ~25 min suite) — Expected: PASS including all pinned seeds (the wire append shifts no fault draws — auth T7's precedent — but the T5/T7 gate changes are already in: any pinned-seed failure here is the frozen-replay tripwire; STOP and diagnose).
- [ ] **Step 7: Full check + commit.**

```bash
cargo build --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check
git add crates/charpente-core/src/db.rs crates/charpente/src/repl crates/charpente/tests/replication.rs
git commit -m "repl: Hello.schema_digest — refuse a schema-mismatched peer by name (migrations T8)"
```

### Task 9: Core flagship properties — crash resumability, twin oracle, RED meta-test

**Files:**
- Create: `crates/charpente-core/tests/migrations.rs`
- Test: same file

**Interfaces:**
- Consumes: everything through T6; `charpente_sim::{run_universes_swarm_with!, checks}` (macro at charpente-sim/src/lib.rs:1537; invocation shape per crates/charpente/tests/auth.rs:627-635); `checks::{is_fault_fallout, replays_identically}` (checks.rs:24/80).
- Produces: spec §6's core properties.

Fixtures: duplicate migration_gate.rs's `WidgetV1`/`Widget2` dual-impl fixtures at the top of this file (integration test binaries do not share code in this repo — materialize.rs vs engine.rs precedent), plus `Widget3` (version 3, fp `0x5555`, adds `tag: u8`, `TableMigration<Prev = Widget2>` setting `tag: 9`) with its own `RowShape`. Include the `cfg()`/`seed_v1` helpers verbatim.

- [ ] **Step 1: Crash-at-any-point + twin oracle** (the flagship — the correct retry idiom AND spec §6's content comparison):

```rust
fn migrated_twin(rows: u64) -> Vec<(u64, u32, String)> {
    // The uninterrupted twin: same seed data, faultless universe, one
    // clean migration — the oracle every faulted universe must equal.
    let sim = Sim::with_faults(0x2EB7_1000, FaultConfig::none());
    seed_v1(&sim, rows);
    let schema = Schema::new().table::<Widget2>().migration::<Widget2>("Widget");
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig { migration_batch_size: 3, ..cfg() },
        schema,
    )
    .unwrap();
    db.migrate_schema().unwrap();
    let mut rows: Vec<Widget2> = db.snapshot().unwrap().scan().unwrap();
    rows.sort_by_key(|r| r.id.raw());
    rows.into_iter().map(|r| (r.id.raw(), r.count, r.label)).collect()
}

#[test]
fn migration_survives_the_fault_matrix_and_equals_the_uninterrupted_twin() {
    let twin = migrated_twin(20);
    run_universes_swarm_with!(
        0x2EB7_2000,
        128,
        |_cfg: &mut FaultConfig| {},
        |sim: &mut Sim| {
            seed_v1_result(sim, 20)?; // seed_v1 with faults mapped to Err — see below
            let schema = || Schema::new().table::<Widget2>().migration::<Widget2>("Widget");
            let mut db = None;
            for _round in 0..64 {
                match Db::open(
                    Box::new(sim.disk()),
                    DbConfig { migration_batch_size: 3, ..cfg() },
                    schema(),
                )
                .and_then(|mut d| d.migrate_schema().map(|()| d))
                {
                    Ok(d) => {
                        db = Some(d);
                        break;
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        if !charpente_sim::checks::is_fault_fallout(&msg) {
                            return Err(format!("non-injected failure: {msg}"));
                        }
                        sim.restart();
                    }
                }
            }
            let Some(db) = db else {
                return Err("64 restart rounds without a successful open".to_string());
            };
            sim.cover("migrations:resumed-to-completion");
            let mut rows: Vec<Widget2> =
                db.snapshot().map_err(|e| e.to_string())?.scan().map_err(|e| e.to_string())?;
            rows.sort_by_key(|r| r.id.raw());
            let got: Vec<(u64, u32, String)> =
                rows.into_iter().map(|r| (r.id.raw(), r.count, r.label)).collect();
            if got != twin() {
                return Err(format!(
                    "faulted universe's migrated store diverged from the uninterrupted twin: \
                     got {got:?}"
                ));
            }
            Ok(())
        },
        require: ["migrations:resumed-to-completion"]
    );
}
```

  Two mechanical adaptations the implementer makes while wiring this: (a) `seed_v1_result` is `seed_v1` returning `Result<(), String>` with the same fault-classified restart loop around its open+transact (a faulted universe can fail the SEED too — same idiom, same `is_fault_fallout` guard); (b) the twin is computed once outside the macro and captured — if the macro's closure requirements forbid the capture, compute it inside per universe from the faultless helper (it is deterministic and cheap at 20 rows); `twin()` above then becomes the direct call `migrated_twin(20)`.

- [ ] **Step 2: The RED-provable drift meta-test** (hard rule 3 — the naive gate is a real check-skipping variant run against the same real store, and the test then shows what proceeding would have done):

```rust
#[test]
fn the_real_gate_refuses_drift_the_naive_gate_would_misdecode() {
    use charpente_core::db::{GateStatus, gate_status};
    use charpente_core::migration::SchemaVersion;

    let sim = Sim::with_faults(0x2EB7_0010, FaultConfig::none());
    seed_v1(&sim, 3);
    // Read the REAL persisted row back from the REAL store.
    let db = Db::open(Box::new(sim.disk()), cfg(), Schema::new().table::<WidgetV1>()).unwrap();
    let persisted: SchemaVersion =
        db.snapshot().unwrap().get(schema_version_id("Widget")).unwrap().unwrap();
    let raw = db.snapshot().unwrap().scan_raw_from("Widget", None, 1).unwrap();
    drop(db);

    // The real gate, fed the real row + WidgetDrifted's compiled meta:
    // refuses by name.
    let real = gate_status("Widget", (1, 0x2222), Some(&persisted), true);
    assert!(matches!(real, GateStatus::Refuse(_)), "{real:?}");

    // The naive gate — same signature, fingerprint check deleted (the
    // exact bug the negative-space check exists to prevent):
    fn naive_gate_status(
        _table: &str,
        compiled: (u32, u64),
        persisted: Option<&SchemaVersion>,
        _rows_exist: bool,
    ) -> GateStatus {
        match persisted {
            Some(row) if row.version == compiled.0 => GateStatus::Current,
            _ => GateStatus::Adopt,
        }
    }
    let naive = naive_gate_status("Widget", (1, 0x2222), Some(&persisted), true);
    assert_eq!(naive, GateStatus::Current, "the victim must wrongly proceed");

    // And proceeding is not hypothetical damage: the drifted compiled
    // type MISDECODES the real stored bytes the naive gate just admitted.
    let (_, bytes) = &raw[0];
    assert!(
        <WidgetDrifted as Row>::decode_row(bytes).is_err(),
        "WidgetV1 bytes must not decode under WidgetDrifted — if they ever do, this \
         meta-test's damage half needs a same-length-retype fixture instead"
    );
}
```

- [ ] **Step 3: Multi-step chain determinism** (spec §6): path A `v1 -> v3` in one `migrate_schema` (schema registers `Widget3` + both steps `.migration::<Widget2>("Widget").migration::<Widget3>("Widget")`), path B `v1 -> v2` (open, migrate, checkpoint, drop) then `-> v3` (reopen with the v3 schema, migrate) — seeds `0x2EB7_0011`/`0x2EB7_0012`, 6 rows, compare full `(id, count, label, tag)` vectors. Same structure as the twin helper; write both paths as straight-line test code.
- [ ] **Step 4: In-flight-target refusal + rejection-mid-chain** (`0x2EB7_0013`): seed 5 rows, migrate v1→v2 with `migration_batch_size: 2` but a `TableMigration` that rejects on a row in the SECOND batch (count == 3) — batch 1 commits (watermark lands), batch 2 errors out of `migrate_schema`. Reopen with the *v1* schema → the in-flight-target refusal fires verbatim (`migrating_to = Some(2)`, compiled 1). Reopen with the v2 rejecting schema, `migrate_schema` again → same rejection error (resumption is honest). Reopen with a v2 non-rejecting schema of the same fingerprint — cannot exist (fingerprint pins shape); assert instead that the fixture with a DIFFERENT fingerprint refuses as drift. Pin all three messages verbatim.
- [ ] **Step 5: Index rebuild + idempotency** (`0x2EB7_0014`): a v1 fixture WITH an index (`IndexDef { name: "by_count", key: |r, out| r.count.encode(out) }` on both v1 and v2 shapes), 8 rows; migrate; assert `scan_by` under the v2 type returns exactly the rows per count key (the index was rebuilt from migrated rows). Then the idempotency half: `sim.restart()` immediately after `migrate_schema` returns (the close-out record is in the WAL, the store checkpoint is behind), reopen + `migrate_schema` (no-op) — replay re-applies the close-out, re-triggers the rebuild — `scan_by` results identical.
- [ ] **Step 6: Replay determinism** (`0x2EB7_0015`): `checks::replays_identically` over a migration universe — run the seed+migrate workload twice from the same seed, compare the full disk fingerprint (use the checker's established call shape from charpente-sim's own tests: `grep -rn "replays_identically" crates/`).
- [ ] **Step 7: Run.** `cargo test -p charpente-core --test migrations` twice (fresh master seeds) — Expected: PASS ×2, `require` met. Any red universe prints its seed — pin it in this file per hard rule 3 before proceeding.
- [ ] **Step 8: Commit.**

```bash
git add crates/charpente-core/tests/migrations.rs
git commit -m "test: migration fault-matrix twin property + RED drift meta-test + chain/index/replay properties (migrations T9)"
```

### Task 10: Replication properties — the gate across a replicated `#[table]` app

**Files:**
- Modify: `crates/charpente/tests/replication.rs` (three properties; the rig's `Widget`/`Gadget` fixtures at lines 28/36 are already `#[table]` — add v2 variants + a schema-parameterized spawn)
- Test: same file

**Interfaces:**
- Consumes: the full rig (`spawn_node`, `run_until`, `converged`, `assert_converged`, `seeding_db_cfg`, `sim.restart`), T6–T8.
- Produces: spec §6's replicated properties — the scenario class the reviewed plan never covered.

- [ ] **Step 1: Rig plumbing.** The rig builds every node's schema via `make_schema()` (replication.rs:63). Add `make_schema_v2()` returning the v2 `Widget` (a `WidgetV2 { id, count, note: String }` under the same `TABLE` with `version = 2` + `#[row_shape]`-style v1 shape + `TableMigration` filling `note: "v2"`) and thread an optional schema override through `spawn_node` (add `spawn_node_with_schema(sim, name, cfg, db_cfg, schema)` and make `spawn_node` delegate with `make_schema()` — smallest diff, no call-site churn).
- [ ] **Step 2: Fresh follower seeds across the gate** (`0x2EB8_0002`): primary on v2 schema, seed data + `migrate_schema` happens via its startup path, GC past the base (reuse `seeding_db_cfg`'s shape); spawn a FRESH v2 follower → it must Hello `position: None` (the T5/T6 design writes nothing at open on a fresh store) → seeds → `assert_converged`, plus `scan` on the follower returns migrated rows including their `note` values and the `schema_versions` rows arrived via the seed. This is the test that goes RED if anyone ever reintroduces gate writes at open.
- [ ] **Step 3: Converged follower catches up via shipped MigratePuts** (`0x2EB8_0003`): both nodes on v1, converge, stop both (drop the follower first — clean-shutdown checkpoint on both via kernel exit or explicit `checkpoint()`); restart the primary on v2 (its startup migrates), restart the follower on v2 (its gate is read-only — no local migration), reconnect, `run_until converged` → follower's rows carry the migrated field AND its `scan_by`-visible index state matches the primary's (the close-out apply rebuilt follower indexes at the same log position). No re-seed: assert `p.repl.stats().seeds == 0` across the whole test.
- [ ] **Step 4: Promote mid-migration resumes** (`0x2EB8_0004`): primary v2 with `migration_batch_size: 2` over ~10 rows and a follower attached; use the rig's step-driving to let SOME batches ship, then kill the primary (`sim`-side teardown per the rig's existing failover tests — mirror `late_follower_seeds`' neighborhood) and `promote` the follower; its `do_promote` gate resumes from the shipped watermark; assert the promoted node's final table equals the uninterrupted twin (compute twin with a single-node v2 migration of the same seed rows) and `migrating_through/migrating_to` are `None`.
- [ ] **Step 5: Run.** `cargo test -p charpente --test replication` (full suite — Expected: PASS ×1 here; T11 runs it ×2 fresh) — every pinned seed green per the frozen-replay contract.
- [ ] **Step 6: Commit.**

```bash
git add crates/charpente/tests/replication.rs
git commit -m "test: replicated migrations — fresh-follower seed, shipped catch-up, promote-mid-migration (migrations T10)"
```

### Task 11: Docs + invoicing demo + evidence sweep + close-out (controller-heavy)

- [ ] **Step 1: DESIGN §3 sketch → mechanism.** Replace DESIGN.md:289-293's sketch with the revised mechanism: the organizing never-decode-foreign-bytes rule, `#[table(version=N)]` + assert-impl, the fingerprint, `#[row_shape]`/`TableMigration`, the pre-replay gate + the six named refusals + the upgrade barrier, `MigratePut` + close-out index rebuild, adoption-at-first-write, role-aware placement (kernel startup / promote), `Hello.schema_digest`. Cross-reference the spec by path. Keep §3's "Schema *is* the Rust definition" framing.
- [ ] **Step 2: GUIDE migrations section.** Worked add-a-field example (rename to `WidgetV1` + `#[row_shape(version = 1)]`, bump `#[table(version = 2)]`, `impl TableMigration`, `.migration::<Widget>("Widget")` registration); what happens at next startup; the upgrade procedure verbatim from spec §4 (stop both, upgrade both, start primary first, followers catch up — no re-seeds); the barrier's remedy paragraph (crash-then-upgrade → run the previous binary once); the one-line hand-written-`Row` opt-out warning (spec §1). Compile gate: `cargo build -p guide-examples`.
- [ ] **Step 3: Commit docs.**

```bash
cargo build -p guide-examples
git add DESIGN.md docs/GUIDE.md
git commit -m "docs: migrations — the revised mechanism in DESIGN §3 + GUIDE worked example (migrations T11)"
```

- [ ] **Step 4: Invoicing demo.** Add `notes: String` (default empty) to an invoicing table per the GUIDE example exactly. Run `cargo run -p charpente-cli -- test --sim --dir examples/invoicing --universes 256 --jobs 8` — exit 0. Commit `examples/invoicing` as `invoicing: add a field via the migration mechanism (migrations T11 demo)`.
- [ ] **Step 5 (controller): evidence sweep.** Fresh batches ×2: `migrations`, `migration_gate`, `typed_tables`, `kernel`, `transactions`, `replication`, plus charpente-sim's own suites ×2. Pinned replays — and for each, **re-verification, not just replay** (Global Constraints): `0x8bee1752811ae873` (crash_recovery — debug AND `--release`), `0xff159ed76d2806e4` (`--release`, mode-skew guard), kernel `0x348270b38f166529` (its historical bug is the stale-drain-id resurrection — confirm the test's assertion still exercises it, e.g. by temporarily reverting the guard it pins and confirming RED, then restoring), the replication regression seeds this suite carries (grep `0x2EB1_` pins). Invoicing 1024u, record the master seed. Live demo-of-record (transcript to the ledger): add-a-field restart shows populated defaults; kill -9 old binary → new binary prints the barrier refusal byte-exact → old binary once → upgrade clean; un-migrated follower binary refused by `SCHEMA_DIGEST_MISMATCH` byte-exact. Full `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`.
- [ ] **Step 6 (controller): close-out.** CLAUDE.md current-state entry (what shipped; the three review-driven rulings; evidence seeds; the read-safety follower invariant; the deferred `#[job]`/`#[event]` follow-up restated) + move "Next:" to M1 sub-project 6. Delete the superseded plan `docs/superpowers/plans/2026-07-16-migrations.md` (untracked — `rm`, note it in the ledger). Ledger: per-task lines + seeds in `.superpowers/sdd/progress.md`. Commit `docs: M1 sub-project 5 done — migrations (evidence in entry)`.
