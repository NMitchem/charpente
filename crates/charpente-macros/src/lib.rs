//! The derive surface (DESIGN.md §3). `#[table]` turns a struct into a typed
//! row: `Row` impl (name, codec, indexes), plus a `by_<field>` accessor for
//! every `#[index]` field. Macro errors are product surface (hard rule 5):
//! each one names the contract and the fix.
//!
//! Generated code references `::charpente_core` paths, so the using crate
//! must depend on `charpente-core` directly (the platform crate re-exports
//! it; revisit path resolution when the demo app lands, M0 step 8).

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Error, Fields, parse_macro_input, spanned::Spanned};

#[proc_macro_attribute]
pub fn table(args: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    if !args.is_empty() {
        return Error::new(
            input.ident.span(),
            "#[table] takes no arguments (yet) — write `#[table]`",
        )
        .to_compile_error()
        .into();
    }
    match expand(input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let name_str = name.to_string();

    if !input.generics.params.is_empty() {
        return Err(Error::new(
            input.generics.span(),
            format!(
                "#[table] structs cannot be generic — a table is one concrete row shape; make `{name_str}` non-generic"
            ),
        ));
    }
    let Data::Struct(data) = &input.data else {
        return Err(Error::new(
            input.span(),
            "#[table] applies to structs — enums and unions cannot be rows",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(Error::new(
            input.span(),
            format!(
                "#[table] needs named fields — `{name_str}` must be a braced struct with an `id: Id<{name_str}>` field"
            ),
        ));
    };

    let mut field_idents = Vec::new();
    let mut field_types = Vec::new();
    let mut emitted_fields = Vec::new();
    let mut indexes = Vec::new(); // (ident, ty)
    let mut has_id = false;

    for field in &fields.named {
        let ident = field.ident.as_ref().expect("named fields have idents");
        let ty = &field.ty;
        let is_index = field.attrs.iter().any(|a| a.path().is_ident("index"));
        if ident == "id" {
            has_id = true;
            if is_index {
                return Err(Error::new(
                    field.span(),
                    "`id` is the primary key — it is already the fastest lookup; remove #[index]",
                ));
            }
            let ok = matches!(ty, syn::Type::Path(p)
                if p.path.segments.last().is_some_and(|s| s.ident == "Id"));
            if !ok {
                return Err(Error::new(
                    ty.span(),
                    format!(
                        "the `id` field of a #[table] must be `Id<{name_str}>` — a typed id, never a bare integer"
                    ),
                ));
            }
        }
        if is_index {
            indexes.push((ident.clone(), ty.clone()));
        }
        field_idents.push(ident.clone());
        field_types.push(ty.clone());

        let kept_attrs: Vec<_> = field
            .attrs
            .iter()
            .filter(|a| !a.path().is_ident("index"))
            .collect();
        let vis = &field.vis;
        emitted_fields.push(quote! { #(#kept_attrs)* #vis #ident: #ty });
    }

    if !has_id {
        return Err(Error::new(
            input.span(),
            format!(
                "missing `id: Id<{name_str}>` field — every #[table] row needs a typed primary key (add `id: Id<{name_str}>`)"
            ),
        ));
    }

    let attrs = &input.attrs;
    let vis = &input.vis;

    let index_fn_idents: Vec<_> = indexes
        .iter()
        .map(|(ident, _)| format_ident!("__charpente_index_{ident}"))
        .collect();
    let index_names: Vec<String> = indexes.iter().map(|(ident, _)| ident.to_string()).collect();
    let index_field_idents: Vec<_> = indexes.iter().map(|(ident, _)| ident.clone()).collect();
    let index_field_types: Vec<_> = indexes.iter().map(|(_, ty)| ty.clone()).collect();
    let by_fn_idents: Vec<_> = indexes
        .iter()
        .map(|(ident, _)| format_ident!("by_{ident}"))
        .collect();

    Ok(quote! {
        #(#attrs)*
        #vis struct #name {
            #(#emitted_fields,)*
        }

        impl ::charpente_core::table::Row for #name {
            const TABLE: &'static str = #name_str;
            const INDEXES: &'static [::charpente_core::table::IndexDef<Self>] = &[
                #(::charpente_core::table::IndexDef {
                    name: #index_names,
                    key: Self::#index_fn_idents,
                },)*
            ];

            fn id(&self) -> ::charpente_core::table::Id<Self> {
                self.id
            }

            fn encode_row(&self, out: &mut ::std::vec::Vec<u8>) {
                #(::charpente_core::table::Codec::encode(&self.#field_idents, out);)*
            }

            fn decode_row(bytes: &[u8]) -> ::std::io::Result<Self> {
                let input = &mut &bytes[..];
                #(let #field_idents =
                    <#field_types as ::charpente_core::table::Codec>::decode(input)?;)*
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

        impl #name {
            #(
                #[doc(hidden)]
                fn #index_fn_idents(row: &Self, out: &mut ::std::vec::Vec<u8>) {
                    ::charpente_core::table::Codec::encode(&row.#index_field_idents, out);
                }

                /// Exact-match lookup on the `#[index]` field, in id order.
                #vis fn #by_fn_idents(
                    snapshot: &::charpente_core::table::Snapshot,
                    key: &#index_field_types,
                ) -> ::std::io::Result<::std::vec::Vec<Self>> {
                    snapshot.scan_by::<Self>(#index_names, key)
                }
            )*
        }
    })
}
