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
                let already_present = reg.insert_as(
                    #name_str,
                    ::charpente::wire::TypeSchema::Struct {
                        fields: ::std::vec![#(#field_toks),*],
                    },
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
fn to_camel_case(s: &str) -> String {
    let mut pascal = String::with_capacity(s.len());
    let mut capitalize = true;
    for c in s.chars() {
        if c == '_' {
            capitalize = true;
        } else if capitalize {
            pascal.extend(c.to_uppercase());
            capitalize = false;
        } else {
            pascal.push(c);
        }
    }
    let mut chars = pascal.chars();
    match chars.next() {
        Some(first) => first.to_lowercase().chain(chars).collect(),
        None => String::new(),
    }
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
}
