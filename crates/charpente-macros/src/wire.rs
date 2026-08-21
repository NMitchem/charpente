//! `#[derive(Wire)]` (TS-codegen spec §1, §2): describes a type's SERDE shape,
//! not its Rust shape. Every `#[serde(...)]` item is either matched to
//! serde's own behavior or rejected at compile time — nothing is silently
//! passed through. That inversion (default-deny, not default-allow) is load
//! bearing: an attribute this derive doesn't understand yet is a compile
//! error naming the contract, the site and the fix (hard rule 5), never a
//! schema that quietly disagrees with what serde actually emits (spec §5's
//! literal oracle exists to catch exactly that).
//!
//! Two things below are both load-bearing, for two different failure modes:
//!
//! 1. Every branch that recognizes an item with a `= value` form ALWAYS
//!    consumes that value (`meta.value()?...`), even a branch that ends up
//!    returning `Err`. A first draft of this file had a `skip_serializing_if`
//!    branch that set a flag WITHOUT calling `meta.value()` — so a later
//!    item in the same list (e.g. `rename` in `#[serde(skip_serializing_if =
//!    "...", rename = "x")]`) was silently lost: syn's outer loop, finding
//!    the unconsumed `= "..."` where it expected a `,`, raised `expected
//!    \`,\`` and aborted the whole walk right there. Always consuming a
//!    recognized item's value is what stops the walk from aborting in the
//!    first place.
//! 2. Every `attr.parse_nested_meta(...)` call propagates its `Result` with
//!    `?`. `let _ = attr.parse_nested_meta(...)` (what a first draft also
//!    did) discards that same `expected \`,\`` error — which meant EVERY
//!    deliberate rejection below (flatten, tag, default, the split forms,
//!    the default-deny catch-all) would vanish into a silently-accepted
//!    attribute instead of surfacing as a compile error. Propagating `?` is
//!    what makes "reject the rest loudly" actually loud.
//!
//! A third check (fix round 2, item A) closes a gap those two don't touch:
//! two fields that resolve to the SAME post-serde wire name — via a direct
//! `rename` collision, via `rename_all` folding two different Rust names
//! onto one string, or via one field's `rename` landing on another's
//! untouched name — compile clean under both Rust and serde, but serde then
//! emits a literal duplicate JSON key and the schema carries two identically
//! named fields, with nothing downstream (registry, client `JSON.parse`)
//! erroring either. `expand`'s field loop tracks each resolved wire name and
//! rejects the second field that reaches one already seen.
//!
//! Task 4 extends every one of those disciplines to `Data::Enum` (`expand_enum`
//! and friends below): default-deny for enum- and variant-level `#[serde(...)]`
//! attributes, `?`-propagation through every `parse_nested_meta` call, and a
//! variant-name analogue of the duplicate-wire-name check.
//!
//! Fix round 1 corrected two things Task 4 got wrong, both found by MEASURING
//! real `serde_json` output rather than reasoning about it:
//!
//! - serde has TWO rename tables, `apply_to_field` and `apply_to_variant`,
//!   and they disagree (see `RenameAll::apply_to_field`/`apply_to_variant`'s
//!   own doc comments for the measured shapes). Task 4's first pass used the
//!   variant table for both, which regressed Task 3's struct-field fidelity;
//!   `RenameAll` now carries both tables, called from the site that matches.
//! - `EnumRepr::External` and `EnumRepr::Adjacent` carry
//!   `Vec<(String, Option<TypeSchema>)>`; `None` means "this variant has no
//!   payload on the wire" — exactly serde's own distinction. A unit variant
//!   serializes as a bare JSON string under external tagging (`"Draft"`, not
//!   `{"Draft":{}}`) and drops its `content` key entirely under adjacent
//!   tagging (`{"t":"A"}`, not `{"t":"A","c":{}}`), while a braced-EMPTY
//!   variant (`Msg {}`) is a genuinely different document (`{"Msg":{}}` /
//!   `{"t":"Msg","c":{}}`) that `Some(Struct { fields: [] })` still carries —
//!   `None` vs `Some` separates the two with nothing left over. Internally
//!   tagged tuple/newtype variants stay rejected, for a different, harder
//!   reason: serde itself refuses to serialize a newtype variant over
//!   anything but a struct under `tag`-only representation ("cannot
//!   serialize tagged newtype variant ... containing an integer"), and even
//!   the struct-shaped case merges the inner type's fields into the tag
//!   object in a way this derive has no way to reconstruct from a bare
//!   `Type` at macro-expansion time. A tuple variant with 0 or 2+ fields is
//!   also still rejected — not because a JSON array has no useful
//!   TypeScript type (it does: `[number, number]`), but because the wire IR
//!   itself has no tuple type yet; that omission is fail-closed (a compile
//!   error here, never a silently wrong client), so it can wait.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields, LitStr, Token};

pub(crate) fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;
    let name_str = name.to_string();

    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            format!(
                "#[derive(Wire)] types cannot be generic — a wire type is one concrete shape; \
                 make `{name_str}` non-generic"
            ),
        ));
    }

    match &input.data {
        Data::Struct(data) => expand_struct(name, &name_str, data, &input),
        Data::Enum(data) => expand_enum(name, &name_str, data, &input),
        Data::Union(_) => Err(Error::new_spanned(
            &input,
            format!(
                "#[derive(Wire)] applies to structs and enums — `{name_str}` is a union, which \
                 has no serde representation to describe"
            ),
        )),
    }
}

fn expand_struct(
    name: &syn::Ident,
    name_str: &str,
    data: &syn::DataStruct,
    input: &DeriveInput,
) -> syn::Result<TokenStream> {
    let Fields::Named(fields) = &data.fields else {
        return Err(Error::new_spanned(
            input,
            format!("#[derive(Wire)] needs named fields — `{name_str}` must be a braced struct"),
        ));
    };

    // Validated here, once, independent of the field loop below: a
    // rename_all whose VALUE is unsupported must be caught even for a
    // fieldless struct or one where every field carries its own explicit
    // `rename` (neither would otherwise ever ask `rename_all` to resolve a
    // name, so a check folded into the field loop would never run for them).
    //
    // Deliberately AFTER the shape check above (fix round 2, NEW-1): a
    // struct-level `#[serde(...)]` error must not be the first thing raised
    // for a tuple struct — "needs named fields" is the right diagnostic,
    // which fires when this call sits after the shape check instead of
    // before it. (The `Data::Struct` vs `Data::Enum` vs `Data::Union` split
    // now happens one level up, in `expand`, before this function is ever
    // called — so the historical "enum carrying a struct-level attribute
    // must get the shape error first" case has its own coverage under
    // `expand_enum` instead; see `enum_serde_attrs`.)
    let rename_all = struct_serde_attrs(input)?;

    let mut field_toks = Vec::new();
    let mut register_toks = Vec::new();
    // Tracks each field's POST-serde wire name (fix round 2, item A): two
    // fields resolving to the same name is silent at every downstream layer
    // — serde emits a literal duplicate JSON key, the schema carries two
    // identically-named `Field`s, and the registry stores it with no assert.
    // A collision can arise from an explicit `rename` alone, from
    // `rename_all` folding two distinct Rust names onto the same string
    // (e.g. `a_b` and `a__b` both become `aB` under camelCase), or from one
    // field's explicit `rename` landing on another field's untouched natural
    // name — so the check below runs on `wire_name`, never on `ident`.
    // `BTreeMap`, not `HashMap`: this repo's clippy deny-list bans `HashMap`
    // workspace-wide (nondeterministic iteration order/ambient hash seed),
    // with no macro-crate carve-out — and membership + first-seen lookup is
    // all this needs, so the ordering `BTreeMap` imposes costs nothing here.
    let mut wire_names: std::collections::BTreeMap<String, syn::Ident> =
        std::collections::BTreeMap::new();
    for f in &fields.named {
        let ident = f.ident.as_ref().expect("named fields have idents");
        let ty = &f.ty;
        let attrs = field_serde_attrs(f)?;
        if attrs.skip {
            // `#[serde(skip)]` / `#[serde(skip_serializing)]`: serde never
            // emits this field, so the schema must not claim it exists.
            // Omitting it here (not just marking it optional) is the fix —
            // marking it optional would still be wrong, since a schema
            // field that's always absent isn't "sometimes there", it's
            // never there.
            continue;
        }
        let wire_name = match &attrs.rename {
            Some(r) => r.clone(),
            None => {
                let raw = ident.to_string();
                match rename_all {
                    Some(rule) => rule.apply_to_field(&raw),
                    None => raw,
                }
            }
        };
        if let Some(prev_ident) = wire_names.get(&wire_name) {
            return Err(Error::new_spanned(
                ident,
                format!(
                    "#[derive(Wire)] requires one wire name per field, but `{prev_ident}` and \
                     `{ident}` both resolve to \"{wire_name}\" — serde would emit that key \
                     twice on the wire ({{\"{wire_name}\":...,\"{wire_name}\":...}}, last one \
                     wins on the client) and the schema would carry two fields with the same \
                     name; rename one of the two fields, or give it its own \
                     #[serde(rename = \"...\")], so their resolved names differ"
                ),
            ));
        }
        wire_names.insert(wire_name.clone(), ident.clone());
        let optional = attrs.skip_serializing_if;
        field_toks.push(quote! {
            ::charpente::wire::Field {
                name: #wire_name.to_string(),
                // `.reference()`, not `.schema()`: a named field type must
                // show up as `Ref(name)`, per `Wire::reference()`'s own
                // contract ("for use in a FIELD or route position"). Calling
                // `.schema()` here would inline the field type's full
                // expansion instead, and would recurse forever at runtime
                // for a self-referential type (`schema()` has no base case).
                schema: <#ty as ::charpente::wire::Wire>::reference(),
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
                // `insert_as`, not `insert`: names this Rust type (not just the
                // wire name) if a collision panic ever fires — see
                // `TypeRegistry::insert_as`.
                //
                // Called UNCONDITIONALLY, before the recursion guard below: a
                // guard that checks `contains` first (skipping the insert
                // entirely when the name is already present) would never let
                // a genuine collision — two DIFFERENT Rust types sharing a
                // WIRE_NAME — reach the schema-equality check at all, since
                // the second type's call would see the name already taken
                // and return early. `insert_as`'s return value (true = name
                // was already present with an EQUAL schema; it panics
                // instead of returning on a mismatch) is what the recursion
                // guard uses instead.
                //
                // `Self::schema()`, not a second `TypeSchema::Struct { fields:
                // ... }` built from `field_toks` again: the two would always
                // be identical (same tokens), so building it twice was only
                // ever extra emitted code, never extra correctness.
                let already_present = reg.insert_as(
                    #name_str,
                    <Self as ::charpente::wire::Wire>::schema(),
                    ::std::any::type_name::<#name>(),
                );
                // A self-referential type would otherwise recurse into its
                // own fields forever; a diamond-shaped reference graph would
                // redo the same subtree's work on every path that reaches it.
                if already_present {
                    return;
                }
                #(#register_toks)*
            }
        }
    })
}

/// Enum-container-level `#[serde(...)]`: `tag`, `content`, and `rename_all`
/// are supported — `tag`/`content` choose the representation (see the module
/// doc comment), `rename_all` folds every variant's name the same way the
/// struct-level one folds field names. `untagged` gets a dedicated message
/// (common enough to name specifically, like struct-level `tag`/`default`
/// do); anything else falls through to the default-deny catch-all.
struct EnumAttrs {
    tag: Option<String>,
    content: Option<String>,
    rename_all: Option<RenameAll>,
}

fn enum_serde_attrs(input: &DeriveInput) -> syn::Result<EnumAttrs> {
    let mut tag = None;
    let mut content = None;
    let mut rename_all = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("tag") {
                tag = Some(meta.value()?.parse::<LitStr>()?.value());
                return Ok(());
            }
            if meta.path.is_ident("content") {
                content = Some(meta.value()?.parse::<LitStr>()?.value());
                return Ok(());
            }
            if meta.path.is_ident("rename_all") {
                rename_all = Some(parse_rename_all(&meta)?);
                return Ok(());
            }
            if meta.path.is_ident("untagged") {
                return Err(meta.error(
                    "#[derive(Wire)] does not support serde's `untagged` — an untagged enum \
                     carries no discriminator, so the wire schema cannot tell which variant a \
                     given JSON value belongs to; #[derive(Wire)] only supports the tagged \
                     representations (the default external tagging, `tag`, or `tag` + \
                     `content`)",
                ));
            }
            Err(unrecognized_serde_attr(
                &meta,
                "an enum",
                "`tag`, `content`, `rename_all`",
            ))
        })?;
    }
    if content.is_some() && tag.is_none() {
        return Err(Error::new_spanned(
            input,
            "serde's `content` requires `tag` — an adjacently tagged enum needs both",
        ));
    }
    Ok(EnumAttrs {
        tag,
        content,
        rename_all,
    })
}

/// Variant-level `#[serde(...)]`: only `rename` is supported. `skip`,
/// `other`, `alias` and the rest have no wire semantics defined for them
/// here yet, so — same default-deny discipline as everywhere else in this
/// derive — they are rejected rather than silently ignored.
struct VariantAttrs {
    rename: Option<String>,
}

fn variant_serde_attrs(v: &syn::Variant) -> syn::Result<VariantAttrs> {
    let mut rename = None;
    for attr in &v.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                if meta.input.peek(syn::token::Paren) {
                    return Err(meta.error(
                        "#[derive(Wire)] does not support serde's split `rename(serialize = \
                         ...)` / `rename(deserialize = ...)` form — a wire schema is one shape, \
                         and divergent serialize/deserialize names can't be represented; use \
                         plain `rename = \"...\"` instead",
                    ));
                }
                let s: LitStr = meta.value()?.parse()?;
                rename = Some(s.value());
                return Ok(());
            }
            Err(unrecognized_serde_attr(&meta, "a variant", "`rename`"))
        })?;
    }
    Ok(VariantAttrs { rename })
}

/// Resolves every variant's post-serde wire name (`rename`, else
/// `rename_all`, else the bare ident) and rejects two variants landing on
/// the same one — the enum analogue of `expand_struct`'s field-name check.
/// Two variants sharing a wire name are indistinguishable on the wire
/// whichever representation is in play: a repeated string-union member, a
/// repeated externally-tagged object key, or a repeated tag/discriminant
/// value. Unlike the field check, Rust itself already forbids two variants
/// sharing an ident — this only becomes reachable once `rename`/`rename_all`
/// can fold two DIFFERENT idents onto the same string, the same way
/// `rename_all` can for fields (`a_b`/`a__b` both becoming `aB`).
fn resolve_variant_names(
    data: &syn::DataEnum,
    rename_all: Option<RenameAll>,
) -> syn::Result<Vec<String>> {
    let mut wire_names: std::collections::BTreeMap<String, syn::Ident> =
        std::collections::BTreeMap::new();
    let mut names = Vec::new();
    for v in &data.variants {
        let attrs = variant_serde_attrs(v)?;
        let wire_name = match attrs.rename {
            Some(r) => r,
            None => {
                let raw = v.ident.to_string();
                match rename_all {
                    Some(rule) => rule.apply_to_variant(&raw),
                    None => raw,
                }
            }
        };
        if let Some(prev_ident) = wire_names.get(&wire_name) {
            return Err(Error::new_spanned(
                &v.ident,
                format!(
                    "#[derive(Wire)] requires one wire name per variant, but `{prev_ident}` and \
                     `{}` both resolve to \"{wire_name}\" — two variants sharing a wire name are \
                     indistinguishable on the wire, whether as a string-union member, an \
                     externally-tagged object key, or an internally/adjacently-tagged \
                     discriminant value; rename one of the two variants, or give it its own \
                     #[serde(rename = \"...\")], so their resolved names differ",
                    v.ident
                ),
            ));
        }
        wire_names.insert(wire_name.clone(), v.ident.clone());
        names.push(wire_name);
    }
    Ok(names)
}

/// Builds one variant's CONTENT schema (its wire name is resolved
/// separately, by `resolve_variant_names`) and the `register()` calls for
/// whatever types it references. Never called for `EnumRepr::Unit`'s
/// variants — a unit-only, untagged enum's variants have no content at all,
/// only a name.
///
/// `internal` (`tag.is_some() && content.is_none()`) picks which of
/// `EnumRepr`'s two variant-slot shapes this function must emit tokens for:
/// `Internal`'s slot is a bare `TypeSchema` (a unit variant merges into the
/// tag object exactly as `Struct { fields: [] }`, nothing left over), while
/// `External`'s and `Adjacent`'s slot is `Option<TypeSchema>` (`None` for a
/// unit variant, `Some(..)` otherwise — fix round 1, F5; see `EnumRepr`'s own
/// doc comment in `crates/charpente/src/wire.rs` for the measured shapes
/// this distinction is drawn from). `expand_enum` calls this once per
/// variant for a SINGLE enum, whose representation (and therefore `internal`)
/// is fixed for every variant — there is no enum that mixes the two slot
/// shapes.
///
/// The two rejections still here (unnamed-under-internal, and a tuple
/// variant with 0 or 2+ fields) come from real limits, not from the
/// `Option` gap fix round 1 closed: serde itself refuses to serialize an
/// internally tagged newtype variant over anything but a struct, and the
/// wire IR has no tuple type at all — both confirmed by driving real
/// `serde_json::to_string` on each shape, not reasoned from serde's docs
/// alone (spec §5's fidelity discipline applies to what this derive
/// REFUSES, not just what it accepts).
fn variant_inner_schema(
    name_str: &str,
    v: &syn::Variant,
    tag: &Option<String>,
    content: &Option<String>,
) -> syn::Result<(TokenStream, Vec<TokenStream>)> {
    let vname = v.ident.to_string();
    let internal = tag.is_some() && content.is_none();
    match &v.fields {
        Fields::Unit => {
            if internal {
                Ok((
                    quote! { ::charpente::wire::TypeSchema::Struct { fields: ::std::vec![] } },
                    Vec::new(),
                ))
            } else {
                Ok((quote! { ::std::option::Option::None }, Vec::new()))
            }
        }
        Fields::Named(f) => {
            let mut fs = Vec::new();
            let mut registers = Vec::new();
            // Fresh per variant, deliberately: fields in two different
            // variants never appear in the same JSON object at once (only
            // one variant is ever serialized for a given value), so a
            // collision check spanning the whole enum would reject shapes
            // that are perfectly fine on the wire.
            let mut wire_names: std::collections::BTreeMap<String, syn::Ident> =
                std::collections::BTreeMap::new();
            for field in &f.named {
                let fid = field.ident.as_ref().expect("named fields have idents");
                let attrs = field_serde_attrs(field)?;
                if attrs.skip {
                    continue;
                }
                let fname = match &attrs.rename {
                    Some(r) => r.clone(),
                    None => fid.to_string(),
                };
                if let Some(prev_ident) = wire_names.get(&fname) {
                    return Err(Error::new_spanned(
                        fid,
                        format!(
                            "#[derive(Wire)] requires one wire name per field, but \
                             `{prev_ident}` and `{fid}` in variant `{name_str}::{vname}` both \
                             resolve to \"{fname}\" — serde would emit that key twice on the \
                             wire and the schema would carry two fields with the same name; \
                             rename one of the two fields, or give it its own \
                             #[serde(rename = \"...\")], so their resolved names differ"
                        ),
                    ));
                }
                wire_names.insert(fname.clone(), fid.clone());
                let fty = &field.ty;
                let optional = attrs.skip_serializing_if;
                fs.push(quote! {
                    ::charpente::wire::Field {
                        name: #fname.to_string(),
                        schema: <#fty as ::charpente::wire::Wire>::reference(),
                        optional: #optional,
                    }
                });
                registers.push(quote! { <#fty as ::charpente::wire::Wire>::register(reg); });
            }
            let schema =
                quote! { ::charpente::wire::TypeSchema::Struct { fields: ::std::vec![#(#fs),*] } };
            let out = if internal {
                schema
            } else {
                quote! { ::std::option::Option::Some(#schema) }
            };
            Ok((out, registers))
        }
        Fields::Unnamed(f) => {
            if internal {
                return Err(Error::new_spanned(
                    v,
                    format!(
                        "#[derive(Wire)] cannot describe tuple variant `{name_str}::{vname}` \
                         under serde's internally tagged representation (`tag = \"...\"`) — for \
                         anything but a single struct-shaped field serde refuses to serialize it \
                         at all (\"cannot serialize tagged newtype variant\"), and even a \
                         struct-shaped field has its fields merged into the tag object in a way \
                         this schema cannot reconstruct from a bare type at compile time; use a \
                         braced variant with named fields instead"
                    ),
                ));
            }
            if f.unnamed.len() == 1 {
                let fty = &f.unnamed[0].ty;
                Ok((
                    quote! { ::std::option::Option::Some(<#fty as ::charpente::wire::Wire>::reference()) },
                    vec![quote! { <#fty as ::charpente::wire::Wire>::register(reg); }],
                ))
            } else {
                Err(Error::new_spanned(
                    v,
                    format!(
                        "#[derive(Wire)] cannot describe tuple variant `{name_str}::{vname}` \
                         with {} fields — the wire IR has no tuple type (not, as the JSON \
                         array serde encodes it as might suggest, because a JSON array has no \
                         useful TypeScript type — it does, `[number, number]`; the IR simply \
                         doesn't model one yet). Use a braced variant with named fields.",
                        f.unnamed.len()
                    ),
                ))
            }
        }
    }
}

fn expand_enum(
    name: &syn::Ident,
    name_str: &str,
    data: &syn::DataEnum,
    input: &DeriveInput,
) -> syn::Result<TokenStream> {
    let EnumAttrs {
        tag,
        content,
        rename_all,
    } = enum_serde_attrs(input)?;
    let all_unit = data
        .variants
        .iter()
        .all(|v| matches!(v.fields, Fields::Unit));
    // Resolved (and dup-checked) once, up front, independent of which branch
    // below runs — the unit-string-union shortcut needs the names directly,
    // the general path zips them against each variant's content schema.
    let resolved_names = resolve_variant_names(data, rename_all)?;

    let mut register_toks = Vec::new();

    let repr = if all_unit && tag.is_none() {
        quote! {
            ::charpente::wire::EnumRepr::Unit {
                variants: ::std::vec![#(#resolved_names.to_string()),*]
            }
        }
    } else {
        let mut arms = Vec::new();
        for (v, vname) in data.variants.iter().zip(resolved_names.iter()) {
            let (inner, regs) = variant_inner_schema(name_str, v, &tag, &content)?;
            register_toks.extend(regs);
            arms.push(quote! { (#vname.to_string(), #inner) });
        }
        match (&tag, &content) {
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

    Ok(quote! {
        impl ::charpente::wire::Wire for #name {
            const WIRE_NAME: &'static str = #name_str;

            fn schema() -> ::charpente::wire::TypeSchema {
                ::charpente::wire::TypeSchema::Enum(#repr)
            }

            fn register(reg: &mut ::charpente::wire::TypeRegistry) {
                // Same `insert_as`-then-guard shape as `expand_struct`, and
                // for the same reason: checking `contains` FIRST would let a
                // genuine collision between two different enums sharing a
                // `WIRE_NAME` return early on the second one without ever
                // comparing schemas, silently keeping whichever registered
                // first instead of panicking (see `TypeRegistry::insert_as`'s
                // doc comment and `expand_struct`'s matching comment).
                let already_present = reg.insert_as(
                    #name_str,
                    <Self as ::charpente::wire::Wire>::schema(),
                    ::std::any::type_name::<#name>(),
                );
                if already_present {
                    return;
                }
                #(#register_toks)*
            }
        }
    })
}

/// Case-conversion rules serde's `rename_all` supports, resolved and
/// validated ONCE (see the call site in `expand`), not re-checked per field.
#[derive(Clone, Copy)]
enum RenameAll {
    Camel,
    Snake,
    ScreamingSnake,
    Kebab,
}

impl RenameAll {
    /// `s`'s span is used for the error, if any — pins the diagnostic to the
    /// offending string literal itself (F8: not the field, not even the
    /// whole attribute).
    fn parse(s: &LitStr) -> syn::Result<RenameAll> {
        match s.value().as_str() {
            "camelCase" => Ok(RenameAll::Camel),
            "snake_case" => Ok(RenameAll::Snake),
            "SCREAMING_SNAKE_CASE" => Ok(RenameAll::ScreamingSnake),
            "kebab-case" => Ok(RenameAll::Kebab),
            other => Err(Error::new_spanned(
                s,
                format!(
                    "#[derive(Wire)] does not know serde's rename_all = \"{other}\" — the \
                     generated TypeScript would carry the wrong field names. Supported: \
                     camelCase, snake_case, SCREAMING_SNAKE_CASE, kebab-case"
                ),
            )),
        }
    }

    /// Mirrors serde_derive_internals' `RenameRule::apply_to_field` exactly —
    /// Task 3's original three non-`Camel` bodies, verbatim.
    ///
    /// Fix round 1 (F1/F8): a first pass at Task 4 replaced these bodies with
    /// the *variant* table below (`apply_to_variant`), on the theory that
    /// Task 3's fidelity tests only "happened" to pass because they never
    /// exercised a field name with an internal uppercase letter. That theory
    /// was wrong: serde has two genuinely DIFFERENT rename tables —
    /// `apply_to_field` and `apply_to_variant` — and Task 3's three bodies
    /// were serde's field table, correct for every field name, not just
    /// already-snake_case ones. Measured directly against real `serde_json`
    /// on a field named `fooBar` (not just `unit_amount_cents`-shaped names):
    /// `snake_case` is a genuine no-op (`"fooBar"`, unchanged — serde's field
    /// snake rule never re-splits on case), `SCREAMING_SNAKE_CASE` is a plain
    /// `to_ascii_uppercase()` with NO separator inserted (`"FOOBAR"`, not
    /// `"FOO_BAR"`), and `kebab-case` only ever swaps `_` for `-`
    /// (`"fooBar"`, unchanged — no separator inserted there either). Using
    /// the variant table for fields was the actual regression; this reverts
    /// it. `Camel` needed no change either way: `to_camel_case` already IS
    /// serde's field camelCase rule (PascalCase-build, demote first char).
    fn apply_to_field(self, raw: &str) -> String {
        match self {
            RenameAll::Camel => to_camel_case(raw),
            RenameAll::Snake => raw.to_string(),
            RenameAll::ScreamingSnake => raw.to_ascii_uppercase(),
            RenameAll::Kebab => raw.replace('_', "-"),
        }
    }

    /// Mirrors serde_derive_internals' `RenameRule::apply_to_variant` — a
    /// DIFFERENT table from `apply_to_field` above, confirmed by measuring
    /// real `serde_json` output on a battery of variant names (PascalCase,
    /// acronym-leading and acronym-trailing, and a name already containing an
    /// underscore), not reasoned from serde's docs. Two things about it are
    /// easy to get wrong by analogy with the field table, so both are pinned
    /// by a fidelity test:
    ///
    /// - `Camel` here is NOT `to_camel_case` — serde's variant camelCase rule
    ///   lowercases only the FIRST character and leaves every other
    ///   character exactly as written, including any underscore. It does not
    ///   word-split on `_` the way the field rule does: `A_B` -> `"a_B"`, not
    ///   `"aB"` (measured).
    /// - `Snake`, `SCREAMING_SNAKE_CASE`, and `kebab-case` all insert a NEW
    ///   separator before every character position (other than the first)
    ///   testing true for Unicode `char::is_uppercase()` — not
    ///   `is_ascii_uppercase()` — and then lowercase that character with
    ///   `to_ascii_lowercase()` (ASCII-only). That asymmetry is deliberate on
    ///   serde's part and must be copied exactly: it is what makes a variant
    ///   named `AÉmile` fold to `"a_Émile"` under `snake_case` (separator
    ///   inserted because `É` is Unicode-uppercase; `É` itself left
    ///   untouched because `to_ascii_lowercase` doesn't touch non-ASCII) —
    ///   measured, not assumed.
    fn apply_to_variant(self, raw: &str) -> String {
        fn snake(raw: &str) -> String {
            let mut out = String::with_capacity(raw.len() + 4);
            for (i, c) in raw.chars().enumerate() {
                if i > 0 && c.is_uppercase() {
                    out.push('_');
                }
                out.push(c.to_ascii_lowercase());
            }
            out
        }
        match self {
            RenameAll::Camel => {
                let mut chars = raw.chars();
                match chars.next() {
                    Some(first) => {
                        let mut out = String::with_capacity(raw.len());
                        out.push(first.to_ascii_lowercase());
                        out.push_str(chars.as_str());
                        out
                    }
                    None => String::new(),
                }
            }
            RenameAll::Snake => snake(raw),
            RenameAll::ScreamingSnake => snake(raw).to_ascii_uppercase(),
            RenameAll::Kebab => snake(raw).replace('_', "-"),
        }
    }
}

/// Parses `rename_all`'s value form and rejects its split
/// `rename_all(serialize = ...)` form. Shared by `struct_serde_attrs` and
/// `enum_serde_attrs` — serde supports `rename_all` on both containers, with
/// the same split-form restriction either way.
fn parse_rename_all(meta: &syn::meta::ParseNestedMeta) -> syn::Result<RenameAll> {
    if meta.input.peek(syn::token::Paren) {
        return Err(meta.error(
            "#[derive(Wire)] does not support serde's split `rename_all(serialize = ...)` / \
             `rename_all(deserialize = ...)` form — a wire schema is one shape, and divergent \
             serialize/deserialize names can't be represented; use plain \
             `rename_all = \"...\"` instead",
        ));
    }
    let s: LitStr = meta.value()?.parse()?;
    RenameAll::parse(&s)
}

/// Struct-level `#[serde(...)]`: `rename_all` is supported (see `RenameAll`);
/// everything else is rejected. `tag` and container-level `default` get their
/// own message because they're common enough to name specifically; anything
/// else falls through to a generic default-deny error.
fn struct_serde_attrs(input: &DeriveInput) -> syn::Result<Option<RenameAll>> {
    let mut rename_all = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename_all") {
                rename_all = Some(parse_rename_all(&meta)?);
                return Ok(());
            }
            if meta.path.is_ident("tag") {
                return Err(meta.error(
                    "#[derive(Wire)] does not support serde's struct-level `tag = \"...\"` — it \
                     adds a wire field the schema does not model; tagging is a per-enum concern \
                     (`#[derive(Wire)]` on the enum itself resolves `tag`/`content` there), and \
                     a struct-level tag stays out of scope",
                ));
            }
            if meta.path.is_ident("default") {
                consume_optional_value(&meta)?;
                return Err(meta.error(
                    "#[derive(Wire)] does not support serde's container-level `default` — it \
                     changes whether fields may be absent, and the right wire semantics differ \
                     between a request type and a response type; say what you mean explicitly \
                     per field instead",
                ));
            }
            Err(unrecognized_serde_attr(&meta, "a struct", "`rename_all`"))
        })?;
    }
    Ok(rename_all)
}

struct FieldAttrs {
    rename: Option<String>,
    skip: bool,
    skip_serializing_if: bool,
}

/// Field-level `#[serde(...)]`: `rename`, `skip`, `skip_serializing`, and
/// `skip_serializing_if` are supported; everything else is rejected. One
/// pass over the field's attributes (not three), so "reject what we don't
/// understand" only has to walk the list once.
fn field_serde_attrs(f: &syn::Field) -> syn::Result<FieldAttrs> {
    let mut out = FieldAttrs {
        rename: None,
        skip: false,
        skip_serializing_if: false,
    };
    for attr in &f.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                if meta.input.peek(syn::token::Paren) {
                    return Err(meta.error(
                        "#[derive(Wire)] does not support serde's split `rename(serialize = \
                         ...)` / `rename(deserialize = ...)` form — a wire schema is one shape, \
                         and divergent serialize/deserialize names can't be represented; use \
                         plain `rename = \"...\"` instead",
                    ));
                }
                let s: LitStr = meta.value()?.parse()?;
                out.rename = Some(s.value());
                return Ok(());
            }
            if meta.path.is_ident("skip") || meta.path.is_ident("skip_serializing") {
                out.skip = true;
                return Ok(());
            }
            if meta.path.is_ident("skip_serializing_if") {
                let _: LitStr = meta.value()?.parse()?;
                out.skip_serializing_if = true;
                return Ok(());
            }
            if meta.path.is_ident("skip_deserializing") {
                return Err(meta.error(
                    "#[derive(Wire)] does not support serde's `skip_deserializing` — the field \
                     is still emitted on the wire (only reads are affected), so whether a \
                     client may omit it going IN differs by whether this type is a request or a \
                     response body; write the field out normally, or use `skip` / \
                     `skip_serializing` if it should never appear at all",
                ));
            }
            if meta.path.is_ident("flatten") {
                return Err(meta.error(
                    "#[derive(Wire)] does not support serde's `flatten` — it hoists the inner \
                     type's fields into this struct, which the wire IR cannot express; write \
                     the inner type's fields out on this struct directly instead",
                ));
            }
            if meta.path.is_ident("default") {
                consume_optional_value(&meta)?;
                return Err(meta.error(
                    "#[derive(Wire)] does not support serde's `default` — it changes whether \
                     the field may be absent, and the right wire semantics differ between a \
                     request type (absence is meaningful) and a response type (this server \
                     always writes a value); use `Option<T>` or `#[serde(skip_serializing_if = \
                     ...)]` to say what you mean explicitly instead",
                ));
            }
            Err(unrecognized_serde_attr(
                &meta,
                "a field",
                "`rename`, `skip`, `skip_serializing`, `skip_serializing_if`",
            ))
        })?;
    }
    Ok(out)
}

/// `default`'s error fires unconditionally, but a bare `#[serde(default)]`
/// has no `= value` to consume — only `#[serde(default = "path")]` does.
/// Consuming it when present keeps the (still-erroring) parse well formed
/// instead of leaving a dangling `= "path"` for syn to trip over.
fn consume_optional_value(meta: &syn::meta::ParseNestedMeta) -> syn::Result<()> {
    if meta.input.peek(Token![=]) {
        let _: LitStr = meta.value()?.parse()?;
    }
    Ok(())
}

/// Default-deny: any `#[serde(...)]` key neither explicitly supported nor
/// explicitly rejected above lands here. Whatever it does to serde's output,
/// this derive has no logic for it — passing it through silently is exactly
/// the failure mode spec §5 exists to catch, so it is a hard error instead.
/// `position` carries its own article ("a struct", "a field", "a variant",
/// "an enum") — an enum is the one position that needs "an", so the article
/// lives at each call site instead of this function guessing it from the
/// noun.
fn unrecognized_serde_attr(
    meta: &syn::meta::ParseNestedMeta,
    position: &str,
    supported: &str,
) -> Error {
    let key = meta
        .path
        .get_ident()
        .map(|i| i.to_string())
        .unwrap_or_else(|| "<path>".to_string());
    meta.error(format!(
        "#[derive(Wire)] does not recognize serde's `{key}` on {position} — an attribute this \
         derive doesn't have logic for is rejected rather than silently ignored, because a \
         silently wrong schema is worse than a compile error; supported on {position}: \
         {supported}"
    ))
}

/// Mirrors serde_derive_internals' `RenameRule::CamelCase::apply_to_field`,
/// verified against real `serde_json` output rather than read off serde's
/// source alone: build PascalCase — capitalize the first character and every
/// character following an underscore, drop the underscores, leave every
/// other character exactly as written — then lowercase ONLY the first
/// character of that result.
///
/// The naive "uppercase the character after every underscore" rule this
/// replaces gets a leading underscore wrong: `_id` has no letter *before*
/// the underscore to already be lowercase, so it comes out `Id` under that
/// rule. Real serde emits `id` (confirmed via `serde_json::to_string` on a
/// `#[serde(rename_all = "camelCase")] struct { _id: u32 }`) — PascalCase
/// gives `Id`, and the separate "lowercase the first char" pass then demotes
/// it. A schema field the emitter names `Id` where the wire actually says
/// `id` is exactly the fidelity bug this derive exists to prevent.
///
/// ASCII case conversion only (`to_ascii_uppercase`/`to_ascii_lowercase`),
/// not `char::to_uppercase`/`to_lowercase` (Unicode-aware): serde's own
/// `rename_all` is ASCII-only, so a non-ASCII letter passes through
/// unchanged on both sides. Confirmed against real serde_json: a field named
/// `a_émile` serializes as `"aémile"` — the `é` is NOT uppercased — while
/// the Unicode-aware version this replaced produced `"aÉmile"`.
fn to_camel_case(s: &str) -> String {
    let mut pascal = String::with_capacity(s.len());
    let mut capitalize = true;
    for c in s.chars() {
        if c == '_' {
            capitalize = true;
        } else if capitalize {
            pascal.push(c.to_ascii_uppercase());
            capitalize = false;
        } else {
            pascal.push(c);
        }
    }
    let mut chars = pascal.chars();
    match chars.next() {
        Some(first) => {
            let mut out = String::with_capacity(pascal.len());
            out.push(first.to_ascii_lowercase());
            out.push_str(chars.as_str());
            out
        }
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::to_camel_case;

    #[test]
    fn snake_to_camel() {
        assert_eq!(to_camel_case("unit_amount_cents"), "unitAmountCents");
    }

    #[test]
    fn single_word_is_unchanged() {
        assert_eq!(to_camel_case("note"), "note");
    }

    #[test]
    fn a_leading_underscore_does_not_survive_as_a_capital() {
        // Confirmed against real serde_json output: `#[serde(rename_all =
        // "camelCase")] struct { _id: u32 }` serializes as `"id"`, not
        // `"Id"` — PascalCase capitalizes the leading `_id` to `Id`, and the
        // separate "lowercase only the first char" pass demotes it back down.
        // A naive "uppercase after every underscore" rule gets this case
        // wrong because there is no earlier letter to demote.
        assert_eq!(to_camel_case("_id"), "id");
    }

    #[test]
    fn a_run_of_underscores_collapses_to_one_boundary() {
        assert_eq!(to_camel_case("a__b"), "aB");
    }

    #[test]
    fn non_ascii_letters_are_left_alone() {
        // Mutation: swap `to_ascii_uppercase`/`to_ascii_lowercase` back to
        // `char::to_uppercase`/`to_lowercase` (Unicode-aware) — this goes
        // red (`"aÉmile"` instead of `"aémile"`). Confirmed against real
        // serde_json: `#[serde(rename_all = "camelCase")] struct { a_émile:
        // u32 }` serializes as `{"aémile":...}` — the `é` is untouched.
        assert_eq!(to_camel_case("a_émile"), "aémile");
    }
}
