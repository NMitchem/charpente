//! `#[event]` (DESIGN.md §6, realtime spec): turns a struct into a typed
//! realtime event — a field-wise `Codec` impl (mirrors `#[job]`'s payload
//! codec), `impl Event` carrying the name (snake_case of the type name), and
//! a `Serialize` derive emitted through the `::charpente::serde` re-export
//! (with `#[serde(crate = "::charpente::serde")]` redirecting serde_derive's
//! generated code off the ambient `::serde` path) so app crates emitting
//! events stay single-dep. No runtime registry: unlike `#[job]`/`jobs![]`,
//! there is no `events![]` — `Subscription` (T4) captures encoders
//! statically. Errors name the accepted form (hard rule 5).

use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Error, Fields};

pub(crate) fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;
    let name_str = name.to_string();

    if !input.generics.params.is_empty() {
        return Err(Error::new(
            input.generics.span(),
            format!(
                "#[event] structs cannot be generic — an event is one concrete payload shape; make `{name_str}` non-generic"
            ),
        ));
    }
    let Data::Struct(data) = &input.data else {
        return Err(Error::new(
            input.span(),
            "#[event] applies to structs — enums and unions cannot be events",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(Error::new(
            input.span(),
            format!("#[event] needs named fields — `{name_str}` must be a braced struct"),
        ));
    };

    let field_idents: Vec<_> = fields
        .named
        .iter()
        .map(|f| f.ident.clone().expect("named fields have idents"))
        .collect();
    let field_types: Vec<_> = fields.named.iter().map(|f| f.ty.clone()).collect();

    let event_name = snake_case(&name_str);

    Ok(quote! {
        #[derive(::charpente::serde::Serialize)]
        #[serde(crate = "::charpente::serde")]
        #input

        impl ::charpente::core::table::Codec for #name {
            fn encode(&self, out: &mut ::std::vec::Vec<u8>) {
                #(::charpente::core::table::Codec::encode(&self.#field_idents, out);)*
            }

            fn decode(input: &mut &[u8]) -> ::std::io::Result<Self> {
                #(let #field_idents =
                    <#field_types as ::charpente::core::table::Codec>::decode(input)?;)*
                ::std::result::Result::Ok(Self { #(#field_idents,)* })
            }
        }

        impl ::charpente::core::db::Event for #name {
            const NAME: &'static str = #event_name;
        }
    })
}

/// PascalCase (or camelCase) type name -> snake_case event name, computed at
/// macro-expansion time and baked into the `NAME` const as a string literal
/// (no runtime cost). Inserts `_` at a lower/digit -> upper transition, and
/// at the last letter of an acronym run when it's followed by a lowercase
/// letter — so `NoteAdded` -> `note_added` and `HTTPServer` -> `http_server`.
fn snake_case(s: &str) -> String {
    let mut out = String::new();
    let mut chars = s.chars().peekable();
    let mut prev_is_lower_or_digit = false;
    while let Some(c) = chars.next() {
        if c.is_uppercase() {
            let next_is_lower = chars.peek().is_some_and(|n| n.is_lowercase());
            if !out.is_empty() && (prev_is_lower_or_digit || next_is_lower) {
                out.push('_');
            }
            out.extend(c.to_lowercase());
            prev_is_lower_or_digit = false;
        } else {
            out.push(c);
            prev_is_lower_or_digit = c.is_lowercase() || c.is_ascii_digit();
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::snake_case;

    #[test]
    fn simple_pascal_case() {
        assert_eq!(snake_case("NoteAdded"), "note_added");
    }

    #[test]
    fn single_word() {
        assert_eq!(snake_case("Invoice"), "invoice");
    }

    #[test]
    fn acronym_run_followed_by_word() {
        assert_eq!(snake_case("HTTPServer"), "http_server");
    }

    #[test]
    fn trailing_acronym_run() {
        assert_eq!(snake_case("ServerHTTP"), "server_http");
    }

    #[test]
    fn digits_before_uppercase() {
        assert_eq!(snake_case("Item2Count"), "item2_count");
    }

    #[test]
    fn three_words() {
        assert_eq!(
            snake_case("InvoiceFinalizedEvent"),
            "invoice_finalized_event"
        );
    }
}
