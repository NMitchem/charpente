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
                "#[derive(Wire)] structs cannot be generic — a wire type is one concrete \
                 shape; make `{name_str}` non-generic"
            ),
        ));
    }

    // Validated here, once, independent of the field loop below: a
    // rename_all whose VALUE is unsupported must be caught even for a
    // fieldless struct or one where every field carries its own explicit
    // `rename` (neither would otherwise ever ask `rename_all` to resolve a
    // name, so a check folded into the field loop would never run for them).
    let rename_all = struct_serde_attrs(&input)?;

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
                    Some(rule) => rule.apply(&raw),
                    None => raw,
                }
            }
        };
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

    /// Mirrors serde_derive_internals' `RenameRule::apply_to_field`, ASCII
    /// case conversion only (see `to_camel_case` below for why).
    fn apply(self, raw: &str) -> String {
        match self {
            RenameAll::Camel => to_camel_case(raw),
            RenameAll::Snake => raw.to_string(),
            RenameAll::ScreamingSnake => raw.to_ascii_uppercase(),
            RenameAll::Kebab => raw.replace('_', "-"),
        }
    }
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
                if meta.input.peek(syn::token::Paren) {
                    return Err(meta.error(
                        "#[derive(Wire)] does not support serde's split \
                         `rename_all(serialize = ...)` / `rename_all(deserialize = ...)` form \
                         — a wire schema is one shape, and divergent serialize/deserialize \
                         names can't be represented; use plain `rename_all = \"...\"` instead",
                    ));
                }
                let s: LitStr = meta.value()?.parse()?;
                rename_all = Some(RenameAll::parse(&s)?);
                return Ok(());
            }
            if meta.path.is_ident("tag") {
                return Err(meta.error(
                    "#[derive(Wire)] does not support serde's struct-level `tag = \"...\"` — it \
                     adds a wire field the schema does not model; enum tagging is a separate \
                     concern (Task 4), and a struct-level tag is out of scope here",
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
            Err(unrecognized_serde_attr(&meta, "struct", "`rename_all`"))
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
                "field",
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
        "#[derive(Wire)] does not recognize serde's `{key}` on a {position} — an attribute \
         this derive doesn't have logic for is rejected rather than silently ignored, because a \
         silently wrong schema is worse than a compile error; supported on a {position}: \
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
