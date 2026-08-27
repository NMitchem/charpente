//! The INBOUND half of the `Int64` revive (chunk B1b; B1a did outbound).
//! `runtime.rs`'s `request()` does `parsed as T` / `parsed as E` over raw
//! `JSON.parse` output — a `bigint`-typed field is still a decimal STRING at
//! that point, so a route whose response/error transitively contains an
//! `Int64` needs its result walked once before the generated method returns
//! it. Keyed per ROUTE POSITION, not per named type (chunk-b1b-brief.md A2):
//! a bare `Int64` can sit directly in `RouteInfo.response`/`.error` with no
//! `TypeDef` to hang a function on, so [`revive_expr`] takes any
//! `&TypeSchema` — a route position or a field's — as its entry point, and
//! named types sharing a walker beneath it each get exactly one module-scope
//! function, discovered by [`collect_needed_types`] walking every non-stream
//! route's response/error and built once each by [`emit_revivers`]. This
//! MUST be reachability, not "every type in `Contract.types` that contains
//! an `Int64`": `ts-fixture/tsconfig.json` sets `noUnusedLocals`, so an
//! Int64-bearing type reachable only through an `#[event]` payload (not
//! wired into anything yet — that is chunk B1c's SSE dispatch) would emit an
//! unused top-level `function`, a `tsc` ERROR (TS6133, measured against this
//! exact shape before settling on reachability) — not a warning this chunk
//! could shrug off.
//!
//! **Single source of truth.** [`revive_expr`] decides "does this position
//! need conversion" by calling [`contains_int64`] FIRST and only then builds
//! the expression ([`revive_expr_assuming_needed`], which is infallible given
//! that precondition) — never the reverse (discovering "nothing to do" as a
//! side effect of trying to build something). So the boolean this module
//! treats as ground truth for "does this route / field / variant need a
//! function at all" and the expression actually emitted can never disagree
//! by construction, not just by two hand-written traversals happening to
//! agree today.
//!
//! **The visited-set ruling** (chunk-b1b-brief.md A3): [`contains_int64`] is
//! a PUBLIC, stateless entry point — every call builds its own fresh cycle-
//! guard stack ([`contains_int64_inner`]'s `stack` parameter), never a set
//! shared across calls. That is deliberate, not an oversight: the same named
//! type legitimately appears at two different positions in one document
//! (two sibling fields of the same `Ref`, or the same type reached from two
//! different routes), and each such occurrence must independently decide
//! "does this position need a call", not silently read a leftover "already
//! saw this type" flag from an unrelated earlier query and skip. A stack
//! shared across top-level calls would do exactly that — see
//! `chunk-b1b-report.md`'s worked example and the regression test this
//! module's test suite pins for it (`tests/revive.rs`). The guard exists
//! ONLY to stop `contains_int64_inner`'s OWN recursion from looping forever
//! on a self-referential or mutually-referential `Ref` cycle; it is popped
//! on the way back out, so it never outlives the one recursive call chain
//! that pushed it.
//!
//! **Function emission's OWN "have I already got this one" set is a
//! different, safe kind of memoization** — see [`collect_needed_types`]'s
//! doc comment for why deduplicating a NAME (a monotonic fact: once known
//! needed, a type's function is scheduled once and its own fields are fully
//! explored) is not the same hazard as skipping a CALL at one occurrence.
//!
//! **Naming cannot collide.** A generated function is named `revive` +
//! `TypeDef.name`; since every `Contract.types` name is already unique
//! (`TypeRegistry`) and TypeScript keeps type space (what `emit_types`
//! writes: `export type` / `export interface`) and value space (a plain
//! `function`) apart — the exact reasoning `runtime.rs::RESERVED`'s own doc
//! comment gives for `sseValue` coexisting with an interface of the same
//! name — a `function reviveOrder` cannot collide with a wire type, wire
//! types are never declared as a `class`/`enum` (the one case that doc
//! comment says really does collide), and a route's own camelCase method
//! name is a CLASS MEMBER (`this.foo()`), never a bare identifier, so it
//! cannot shadow or be shadowed by a module-scope `function` either. No
//! separate collision assert is needed for the reasons `check_reserved`
//! needs one; there is nothing reachable for it to catch.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use charpente::describe::Contract;
use charpente::wire::{EnumRepr, Field, TypeSchema};

use crate::types::key;

/// Every registered type by name, for chasing `Ref`.
pub(crate) fn type_map(c: &Contract) -> BTreeMap<&str, &TypeSchema> {
    c.types.iter().map(|d| (d.name, &d.schema)).collect()
}

/// Does `schema` transitively contain an `Int64`, chasing `Ref` through
/// `types`? The one predicate every other function in this module treats as
/// ground truth (see the module doc comment) — a fresh cycle-guard stack per
/// call, never shared.
pub(crate) fn contains_int64(schema: &TypeSchema, types: &BTreeMap<&str, &TypeSchema>) -> bool {
    contains_int64_inner(schema, types, &mut BTreeSet::new())
}

fn contains_int64_inner(
    schema: &TypeSchema,
    types: &BTreeMap<&str, &TypeSchema>,
    stack: &mut BTreeSet<String>,
) -> bool {
    match schema {
        TypeSchema::Int64 { .. } => true,
        // `Map`'s key is never part of the schema at all (it renders as
        // `Record<string, T>` — `types.rs`'s `ts_type` — so a Map key is
        // always `string` and never reachable here); only its value can
        // carry one.
        TypeSchema::Option(inner) | TypeSchema::Seq(inner) | TypeSchema::Map(inner) => {
            contains_int64_inner(inner, types, stack)
        }
        TypeSchema::Ref(name) => {
            if stack.contains(*name) {
                // A cycle back to a type already being explored on THIS
                // path: every other edge out of it is already being walked
                // by the ancestor call that pushed it, so this back-edge
                // adds no reachability an ancestor wouldn't also see —
                // returning false here does not hide an `Int64` anywhere.
                return false;
            }
            let Some(target) = types.get(name) else {
                // A dangling `Ref` is a different bug this module has no
                // contract to check (nothing here registers types).
                return false;
            };
            stack.insert((*name).to_string());
            let found = contains_int64_inner(target, types, stack);
            stack.remove(*name);
            found
        }
        TypeSchema::Struct { fields } => fields
            .iter()
            .any(|f| contains_int64_inner(&f.schema, types, stack)),
        TypeSchema::Enum(repr) => enum_payload_schemas(repr)
            .into_iter()
            .any(|v| contains_int64_inner(v, types, stack)),
        // Bool / I8..F64 / String / Null / Id never carry one. `Id` in
        // particular is A4/A6 made mechanical: the schema decides what
        // converts, never the value, and `Id`'s schema is never `Int64` no
        // matter how numeric-looking the branded string's VALUE is.
        _ => false,
    }
}

/// Every variant's payload schema, flattened past the `Option`-ness that
/// distinguishes a unit variant (`None`) from a braced/newtype one (`Some`) —
/// shared by [`contains_int64_inner`] and [`collect_needed_types`] so the
/// two do not each hand-roll the same four-way `EnumRepr` decomposition and
/// risk drifting apart on it.
fn enum_payload_schemas(repr: &EnumRepr) -> Vec<&TypeSchema> {
    match repr {
        EnumRepr::Unit { .. } => Vec::new(),
        EnumRepr::External { variants } => {
            variants.iter().filter_map(|(_, v)| v.as_ref()).collect()
        }
        EnumRepr::Internal { variants, .. } => variants.iter().map(|(_, v)| v).collect(),
        EnumRepr::Adjacent { variants, .. } => {
            variants.iter().filter_map(|(_, v)| v.as_ref()).collect()
        }
    }
}

/// A TS expression reviving `expr` (already-`JSON.parse`d, assumed to match
/// `schema` — the same trust `request()` places in the payload as a whole,
/// A3's "narrow transformation, not a validator") — `None` when `schema`
/// carries no `Int64` anywhere, so the common path (routes.rs's plain
/// `return this.request(...)`, and a struct field this module leaves out of
/// its override list) stays untouched, per A5 ("emit nothing for a type with
/// no Int64").
///
/// `depth` names the local variables a `.map()` / `Object.entries()`
/// callback introduces (`x0`, `k0`, `x1`, `k1`, ...) — one integer per
/// nesting level reached through THIS call, so a doubly-nested `Seq`/`Map`
/// never has an inner callback's parameter merely happen to be safe to reuse
/// the outer one's name; each level gets its own.
pub(crate) fn revive_expr(
    schema: &TypeSchema,
    expr: &str,
    types: &BTreeMap<&str, &TypeSchema>,
    depth: usize,
) -> Option<String> {
    if !contains_int64(schema, types) {
        return None;
    }
    Some(revive_expr_assuming_needed(schema, expr, types, depth))
}

/// Precondition: `contains_int64(schema, types)` is true. Every recursive
/// call re-checks containment on its own sub-schema via `revive_expr` rather
/// than threading a proof through — a fixture-sized contract, not a hot
/// loop, so the clarity of "always ask the one oracle" beats avoiding a
/// cheap recomputation.
fn revive_expr_assuming_needed(
    schema: &TypeSchema,
    expr: &str,
    types: &BTreeMap<&str, &TypeSchema>,
    depth: usize,
) -> String {
    match schema {
        TypeSchema::Int64 { .. } => format!("reviveInt64({expr})"),
        TypeSchema::Option(inner) => {
            let inner_expr = revive_expr(inner, expr, types, depth)
                .expect("Option's own containment implies its inner element's does");
            format!("({expr}) == null ? null : ({inner_expr})")
        }
        TypeSchema::Seq(inner) => {
            let x = format!("x{depth}");
            let inner_expr = revive_expr(inner, &x, types, depth + 1)
                .expect("Seq's containment implies its element's does");
            format!("({expr}).map(({x}) => {inner_expr})")
        }
        TypeSchema::Map(inner) => {
            let x = format!("x{depth}");
            let k = format!("k{depth}");
            let inner_expr = revive_expr(inner, &x, types, depth + 1)
                .expect("Map's containment implies its value's does");
            format!(
                "Object.fromEntries(Object.entries({expr}).map(([{k}, {x}]) => [{k}, {inner_expr}]))"
            )
        }
        TypeSchema::Ref(name) => format!("revive{name}({expr})"),
        other => unreachable!(
            "revive_expr_assuming_needed: {other:?} is never Int64-bearing on its own \
             (contains_int64 would have said so, and Struct/Enum only ever reach this \
             function via Ref — a bare occurrence means Ruling 1 was violated elsewhere)"
        ),
    }
}

/// A property access on `receiver`: dot notation for a valid identifier,
/// bracket notation (quoted) otherwise — `v.unit-price` parses as
/// `v.unit - price` in JavaScript, so a kebab-case or otherwise non-ident
/// wire name (`types.rs`'s `key` already draws this line for object-literal
/// KEYS; this is the same line for a property READ).
fn field_access(receiver: &str, name: &str) -> String {
    let k = key(name);
    if k.as_str() == name {
        format!("{receiver}.{name}")
    } else {
        format!("{receiver}[{k}]")
    }
}

/// `{ key: expr, ... }` for exactly the fields of `fields` that need
/// conversion — an optional (`skip_serializing_if`) field whose OWN schema
/// is not already `Option(_)` gets an extra outer null-guard: the wire omits
/// the key entirely when absent, so `field_access` reads back `undefined`
/// (not `null`), and passing `undefined` to `BigInt` throws the same as
/// `null` does (A1) — the `Option(_)` case already guards this at the
/// `revive_expr` level (`== null` is true for `undefined` too, by loose
/// equality), so doubling the guard there would be redundant, not wrong; the
/// non-`Option` case has no such guard yet, so it needs one here.
fn revive_struct_overrides(
    fields: &[Field],
    receiver: &str,
    types: &BTreeMap<&str, &TypeSchema>,
    depth: usize,
) -> Vec<String> {
    fields
        .iter()
        .filter_map(|f| {
            let access = field_access(receiver, &f.name);
            let inner = revive_expr(&f.schema, &access, types, depth)?;
            let wrapped = if f.optional && !matches!(f.schema, TypeSchema::Option(_)) {
                format!("({access}) == null ? ({access}) : ({inner})")
            } else {
                inner
            };
            Some(format!("{}: {wrapped}", key(&f.name)))
        })
        .collect()
}

/// `{ ...receiver, k: expr, ... }` — spread preserves every field this
/// schema does not need to touch (including one the schema doesn't mention
/// at all), which is the narrow transformation A3 asks for rather than a
/// full rebuild. `None` when no field needs anything, so a Struct with no
/// `Int64` anywhere emits nothing (A5).
fn revive_struct_expr(
    fields: &[Field],
    receiver: &str,
    types: &BTreeMap<&str, &TypeSchema>,
    depth: usize,
) -> Option<String> {
    let overrides = revive_struct_overrides(fields, receiver, types, depth);
    if overrides.is_empty() {
        None
    } else {
        Some(format!("{{ ...{receiver}, {} }}", overrides.join(", ")))
    }
}

/// `EnumRepr::External`'s revive body. A unit variant serializes as a bare
/// JSON string (`"Ping"`), never an object — so, unlike `Internal`/
/// `Adjacent` below (every one of whose variants, including unit ones, IS an
/// object), a per-variant presence check here must not run `in` on a value
/// that might be a primitive string: `"Ack" in "Ping"` THROWS
/// (`TypeError: Cannot use 'in' operator...`), which is exactly the kind of
/// throw-on-a-legitimate-document A3 forbids. The `typeof v === "object"`
/// guard exists for that reason, not as validation of anything the schema
/// doesn't already guarantee.
fn revive_external_body(
    variants: &[(String, Option<TypeSchema>)],
    types: &BTreeMap<&str, &TypeSchema>,
) -> Option<String> {
    let mut arms = Vec::new();
    for (vname, vschema) in variants {
        let Some(vs) = vschema else {
            continue; // unit variant: a bare string, nothing to convert.
        };
        let accessor = field_access("v", vname);
        let inner = match vs {
            TypeSchema::Struct { fields } => revive_struct_expr(fields, &accessor, types, 1),
            other => revive_expr(other, &accessor, types, 1),
        };
        if let Some(inner) = inner {
            arms.push(format!(
                "  if (v != null && typeof v === \"object\" && {:?} in v) return {{ ...v, {}: {inner} }};",
                vname,
                key(vname)
            ));
        }
    }
    finish_arms(arms)
}

/// `EnumRepr::Adjacent`'s revive body: every variant (including unit) is an
/// object (`{tag:"V"}` or `{tag:"V",content:...}`), so the tag read needs no
/// guard the way `External`'s does.
fn revive_adjacent_body(
    tag: &str,
    content: &str,
    variants: &[(String, Option<TypeSchema>)],
    types: &BTreeMap<&str, &TypeSchema>,
) -> Option<String> {
    let tag_access = field_access("v", tag);
    let content_access = field_access("v", content);
    let content_key = key(content);
    let mut arms = Vec::new();
    for (vname, vschema) in variants {
        let Some(vs) = vschema else {
            continue; // unit variant: omits `content` entirely, nothing to convert.
        };
        let inner = match vs {
            TypeSchema::Struct { fields } => revive_struct_expr(fields, &content_access, types, 1),
            other => revive_expr(other, &content_access, types, 1),
        };
        if let Some(inner) = inner {
            arms.push(format!(
                "  if ({tag_access} === {:?}) return {{ ...v, {content_key}: {inner} }};",
                vname
            ));
        }
    }
    finish_arms(arms)
}

/// `EnumRepr::Internal`'s revive body: the variant's fields merge directly
/// into the same object as the tag (never a nested payload key), so the
/// override list applies straight to `v` itself, guarded by the tag match.
fn revive_internal_body(
    tag: &str,
    variants: &[(String, TypeSchema)],
    types: &BTreeMap<&str, &TypeSchema>,
) -> Option<String> {
    let tag_access = field_access("v", tag);
    let mut arms = Vec::new();
    for (vname, vschema) in variants {
        let TypeSchema::Struct { fields } = vschema else {
            unreachable!(
                "revive_internal_body: EnumRepr::Internal's variant slot is always a Struct \
                 (types.rs's emit_def asserts the identical invariant, from the same derive \
                 guarantee) -- got {vschema:?} for variant `{vname}`"
            )
        };
        let overrides = revive_struct_overrides(fields, "v", types, 1);
        if !overrides.is_empty() {
            arms.push(format!(
                "  if ({tag_access} === {:?}) return {{ ...v, {} }};",
                vname,
                overrides.join(", ")
            ));
        }
    }
    finish_arms(arms)
}

fn finish_arms(mut arms: Vec<String>) -> Option<String> {
    if arms.is_empty() {
        None
    } else {
        arms.push("  return v;".to_string());
        Some(arms.join("\n"))
    }
}

/// The body of a named type's own revive function (`return <expr>;` for a
/// Struct, a chain of tag/presence-guarded early returns for an Enum) — or
/// `None` when nothing under `schema` needs one, in which case
/// `emit_revivers` writes no function for this type at all (A5).
fn revive_body(schema: &TypeSchema, types: &BTreeMap<&str, &TypeSchema>) -> Option<String> {
    match schema {
        TypeSchema::Struct { fields } => {
            let expr = revive_struct_expr(fields, "v", types, 0)?;
            Some(format!("  return {expr};"))
        }
        TypeSchema::Enum(EnumRepr::Unit { .. }) => None,
        TypeSchema::Enum(EnumRepr::External { variants }) => revive_external_body(variants, types),
        TypeSchema::Enum(EnumRepr::Internal { tag, variants }) => {
            revive_internal_body(tag, variants, types)
        }
        TypeSchema::Enum(EnumRepr::Adjacent {
            tag,
            content,
            variants,
        }) => revive_adjacent_body(tag, content, variants, types),
        other => unreachable!(
            "revive_body: a TypeDef's schema must be Struct or Enum -- types.rs's emit_def \
             dispatches the same way, on the same Ruling-1 guarantee -- got {other:?}"
        ),
    }
}

/// Which named types actually need a `reviveXxx` function, reached from
/// every non-stream route's response/error (`Contract.types` on its own
/// carries no notion of "used by a route" — a type can be registered and
/// never reached from anywhere `emit_client` walks). This is NOT optional
/// bookkeeping: `ts-fixture/tsconfig.json` sets `noUnusedLocals`, which
/// makes an unused top-level `function` a `tsc` ERROR (TS6133, measured), so
/// emitting one for every type that merely CONTAINS an `Int64` — regardless
/// of whether any route/field this file's `class Api` methods actually
/// build ever calls it — would break every contract with an Int64-bearing
/// type reachable ONLY through, say, an `#[event]` payload (chunk B1c's
/// territory, not built yet). `needed`/`stack` accumulate across every
/// route's response AND error, and across the fixed-point pass over each
/// newly-needed type's OWN fields (`Order` needing `LineItem`), so a type
/// used by two different routes still gets exactly one function.
///
/// This is a SECOND traversal from [`contains_int64`], deliberately: that
/// one answers "does this schema contain an Int64" for one position with a
/// fresh cycle guard every call (the module doc comment's visited-set
/// ruling); this one answers "which NAMED types, across the whole file,
/// need a function AT ALL" — a question `contains_int64` alone cannot
/// answer since it never looks past whichever single schema it was asked
/// about. `stack` here is the same kind of cycle guard, shared with the
/// `contains_int64_inner` calls this makes (a mutually-recursive `Ref` pair
/// must not let one function's guard be blind to the other's recursion).
fn collect_needed_types<'a>(
    schema: &'a TypeSchema,
    types: &BTreeMap<&'a str, &'a TypeSchema>,
    needed: &mut BTreeSet<&'a str>,
    stack: &mut BTreeSet<String>,
) {
    match schema {
        TypeSchema::Option(inner) | TypeSchema::Seq(inner) | TypeSchema::Map(inner) => {
            collect_needed_types(inner, types, needed, stack)
        }
        TypeSchema::Ref(name) => {
            // Already scheduled (and, since scheduling and recursing into a
            // name happen together below, already fully explored): a
            // positive memo of a monotonic fact, not the call-site hazard
            // the module doc comment warns about — that hazard is about
            // skipping a CONVERSION at a specific occurrence, not about
            // re-deriving "is this name on the list" twice.
            if needed.contains(name) || stack.contains(*name) {
                return;
            }
            let Some(target) = types.get(name) else {
                return;
            };
            stack.insert((*name).to_string());
            if contains_int64_inner(target, types, stack) {
                needed.insert(name);
                // The newly-needed type's OWN fields/variants may reference
                // further named types (`Order` -> `LineItem`), which must
                // also get a function or `reviveOrder`'s body would call
                // one that was never defined.
                collect_needed_types(target, types, needed, stack);
            }
            stack.remove(*name);
        }
        TypeSchema::Struct { fields } => {
            for f in fields {
                collect_needed_types(&f.schema, types, needed, stack);
            }
        }
        TypeSchema::Enum(repr) => {
            for v in enum_payload_schemas(repr) {
                collect_needed_types(v, types, needed, stack);
            }
        }
        _ => {}
    }
}

/// One `function revive<Name>(v: <Name>): <Name> { ... }` per named type
/// [`collect_needed_types`] finds reachable (transitively, through `Ref`)
/// from some non-stream route's response or error — sorted (`needed` is a
/// `BTreeSet`), so the output is a function of the contract alone regardless
/// of route order (spec §4), and never duplicated: a type reached from two
/// different routes, or from two different fields of the same type, still
/// gets exactly one definition, because `needed` is a set, checked once
/// before recursing into any given name (see `collect_needed_types`'s doc
/// comment for why that dedup is safe and the CALL-SITE decision is not
/// memoized the same way).
///
/// Parameter and return are both typed as the real wire type (never `any`):
/// every field a function's body does NOT touch is preserved verbatim by the
/// object spread, so the return expression only needs to satisfy the type at
/// the fields it actually overrides — `tsc` checking that a generated
/// revive function's own body produces the type it claims is a second, free
/// oracle on this module's codegen, the same "framework is the reviewer"
/// property the rest of this sub-project leans on.
pub(crate) fn emit_revivers(c: &Contract) -> String {
    let types = type_map(c);
    let mut needed: BTreeSet<&str> = BTreeSet::new();
    let mut stack: BTreeSet<String> = BTreeSet::new();
    for r in c.routes.iter().filter(|r| !r.stream) {
        collect_needed_types(&r.response, &types, &mut needed, &mut stack);
        collect_needed_types(&r.error, &types, &mut needed, &mut stack);
    }
    let mut out = String::new();
    for name in &needed {
        let schema = types[name];
        let body = revive_body(schema, &types).unwrap_or_else(|| {
            unreachable!(
                "emit_revivers: `{name}` only entered `needed` because contains_int64_inner said \
                 its schema contains an Int64, so revive_body must find something to convert -- a \
                 disagreement here means the two traversals in this module have drifted apart"
            )
        });
        let _ = writeln!(out, "function revive{name}(v: {name}): {name} {{");
        out.push_str(&body);
        out.push_str("\n}\n\n");
    }
    out
}
