//! `#[event(global | scoped_by = Org)]` (DESIGN.md §6, realtime spec,
//! tenancy-completion spec §6): exactly one of the two words, once. Turns a
//! struct into a typed realtime event:
//! - a field-wise `Codec` impl (mirrors `#[job]`'s payload codec);
//! - `impl Event`, carrying the name (snake_case of the type name), the
//!   payload's structural fingerprint (`fingerprint::compute`, continued with
//!   the tenant's `NAME` by `scoped_fingerprint` for a scoped event) and the
//!   scope type;
//! - a `Serialize` derive emitted through the `::charpente::serde` re-export
//!   (with `#[serde(crate = "::charpente::serde")]` redirecting serde_derive's
//!   generated code off the ambient `::serde` path), so app crates emitting
//!   events stay single-dep;
//! - a `Wire` derive: the payload is exactly the JSON a subscriber receives,
//!   so it is a wire type, which is also why a bare `u64` field is a compile
//!   error here (TS-codegen spec §1);
//! - an `EventEntry` impl, so `events![]` can collect the type.
//!
//! Two registries, deliberately. DELIVERY has none: `Subscription::of` and a
//! tenant feed's `subscription` capture encoders statically at the subscribe
//! site, so "unregistered event at delivery" stays inexpressible (realtime
//! spec §3). DESCRIBE has one, filled by `events![]` and read by
//! `App::describe` and by `App::mount_events`' tenant census (TS-codegen spec
//! §2, tenancy-completion spec §4). Errors name the accepted forms (hard
//! rule 5).

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Error, Fields, Ident, Token};

use crate::job::Tenancy;

const ACCEPTED_FORMS: &str = "an event declares `global` or `scoped_by = <Tenant>` and nothing \
     else (its name derives from the type) — e.g. `#[event(global)]` or \
     `#[event(scoped_by = Org)]`";

// ---------------------------------------------------------------- args

/// One word of the attribute. `#[job]`'s `JobArg` without the job-only
/// words, parsed one at a time for the same reason: a word repeated is a typo
/// or a bad merge, never an override.
enum EventArg {
    Global(proc_macro2::Span),
    ScopedBy(proc_macro2::Span, syn::Path),
}

impl Parse for EventArg {
    fn parse(input: ParseStream) -> syn::Result<EventArg> {
        let ident: Ident = input
            .parse()
            .map_err(|e| Error::new(e.span(), ACCEPTED_FORMS))?;
        match ident.to_string().as_str() {
            "global" => Ok(EventArg::Global(ident.span())),
            "scoped_by" => {
                input
                    .parse::<Token![=]>()
                    .map_err(|e| Error::new(e.span(), ACCEPTED_FORMS))?;
                let path: syn::Path = input
                    .parse()
                    .map_err(|e| Error::new(e.span(), ACCEPTED_FORMS))?;
                Ok(EventArg::ScopedBy(ident.span(), path))
            }
            _ => Err(Error::new(ident.span(), ACCEPTED_FORMS)),
        }
    }
}

pub(crate) struct EventArgs {
    /// `global` or `scoped_by = Org`. `None` only while parsing: `expand`
    /// refuses an event that gave neither, by name, before anything else.
    pub tenancy: Option<Tenancy>,
}

impl Parse for EventArgs {
    fn parse(input: ParseStream) -> syn::Result<EventArgs> {
        let mut global: Option<proc_macro2::Span> = None;
        let mut scoped_by: Option<syn::Path> = None;
        while !input.is_empty() {
            match input.parse::<EventArg>()? {
                EventArg::Global(span) => {
                    if global.is_some() {
                        return Err(Error::new(span, "`global` given twice — write it once"));
                    }
                    if let Some(path) = &scoped_by {
                        return Err(Error::new(span, global_and_scoped_by(path)));
                    }
                    global = Some(span);
                }
                EventArg::ScopedBy(span, path) => {
                    if scoped_by.is_some() {
                        return Err(Error::new(span, "`scoped_by` given twice — write it once"));
                    }
                    if global.is_some() {
                        return Err(Error::new(span, global_and_scoped_by(&path)));
                    }
                    scoped_by = Some(path);
                }
            }
            if input.is_empty() {
                break;
            }
            input
                .parse::<Token![,]>()
                .map_err(|e| Error::new(e.span(), ACCEPTED_FORMS))?;
        }
        Ok(EventArgs {
            tenancy: match (global, scoped_by) {
                (_, Some(path)) => Some(Tenancy::ScopedBy(path)),
                (Some(_), None) => Some(Tenancy::Global),
                (None, None) => None,
            },
        })
    }
}

/// `global` and `scoped_by` on one event, reported at whichever came second.
/// It quotes the scope path as written, through the renderer `job.rs`'s
/// `global_and_scoped_by` uses (A8), because the two refusals are one text
/// family. It is its own function because GUIDE §6 pins the job text, and
/// `doc_truth` needs that literal in `job.rs`.
fn global_and_scoped_by(path: &syn::Path) -> String {
    format!(
        "`global` and `scoped_by = {}` on one event — an event is one or the other; drop one",
        crate::fingerprint::path_as_written(path)
    )
}

// ---------------------------------------------------------------- expand

pub(crate) fn expand(args: EventArgs, input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;
    let name_str = name.to_string();

    let Some(tenancy) = &args.tenancy else {
        // Spanned at the attribute, as `#[job]`'s and `#[table]`'s
        // neither-refusals are: the fix is a word the attribute lacks, so
        // there is no token inside it to point at.
        return Err(Error::new(
            proc_macro2::Span::call_site(),
            format!(
                "`{name_str}` declares no tenancy — an event says which, and neither is the \
                 default: add `global` or `scoped_by = <Tenant>` to this attribute's \
                 arguments, keeping its others. `scoped_by = <Tenant>` if it belongs to one \
                 tenant (it is emitted from inside that tenant's scope, and only that tenant's \
                 feed receives it); `global` if it belongs to no tenant (it is emitted on a \
                 bare `Tx`, and only a global `Subscription::of` feed receives it)."
            ),
        ));
    };

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
    let fold = crate::fingerprint::compute(&field_idents, &field_types);

    let event_name = snake_case(&name_str);

    // A `global` event's fingerprint is the bare fold, the value `#[event]`
    // computed before events declared a tenancy, so every existing global
    // event keeps its shape and every global emit record keeps its bytes. A
    // scoped event's continues that fold with its tenant's `NAME`, through the
    // `scoped_fingerprint` a scoped job uses (tenancy-completion spec §4,
    // §6). A change of an event's tenancy, or of its tenant's identity, is
    // then a change of shape that delivery refuses by name. Two same-named
    // events scoped by two tenant types never share a shape, even where the
    // two types' raw ids coincide. `tenant_name`'s `TenantScope` bound is
    // the house diagnostic for a scope type with no `Tenant` impl (A7): a
    // scoped event's tenant is opened by `tx.scope::<Org>(…, ctx)` on both
    // sides, which needs the app's own membership check.
    let (scope_ty, fingerprint, scoped_by_name) = match tenancy {
        Tenancy::Global => (
            quote! { ::charpente::core::table::Unscoped },
            quote! { #fold },
            quote! { ::std::option::Option::None },
        ),
        Tenancy::ScopedBy(path) => (
            quote! { #path },
            quote! {
                ::charpente::core::db::scoped_fingerprint(
                    #fold,
                    ::charpente::tenant::tenant_name::<#path>(),
                )
            },
            quote! {
                ::std::option::Option::Some(::charpente::tenant::tenant_name::<#path>())
            },
        ),
    };

    Ok(quote! {
        #[derive(::charpente::Wire, ::charpente::serde::Serialize)]
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
            const FINGERPRINT: u64 = #fingerprint;
            type Scope = #scope_ty;
        }

        impl ::charpente::describe::EventEntry for #name {
            fn def() -> ::charpente::describe::EventDef {
                ::charpente::describe::EventDef {
                    name: #event_name,
                    payload: <#name as ::charpente::wire::Wire>::WIRE_NAME,
                    register_types: |reg| <#name as ::charpente::wire::Wire>::register(reg),
                    scoped_by: #scoped_by_name,
                    scope_type: ::std::any::TypeId::of::<
                        <#name as ::charpente::core::db::Event>::Scope,
                    >(),
                    scope_type_name: ::std::any::type_name::<
                        <#name as ::charpente::core::db::Event>::Scope,
                    >(),
                }
            }
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
