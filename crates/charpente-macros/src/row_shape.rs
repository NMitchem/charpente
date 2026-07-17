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
