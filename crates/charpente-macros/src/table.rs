use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Error, Fields, spanned::Spanned};

pub(crate) fn expand(
    input: DeriveInput,
    version: u32,
    scoped_by: Option<syn::Path>,
) -> syn::Result<proc_macro2::TokenStream> {
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

    // `#[table(scoped_by = Org)]`: `type Scope` names the app's scope type
    // instead of `Unscoped`, and the const assert below forces `Org` to carry
    // the app's own membership check — the same shape `migration_assert` above
    // uses to force a `TableMigration` impl on a version bump. The bound is
    // `::charpente::TenantScope`, the platform's blanket-implemented alias for
    // `Tenant<Who = Principal, Error = ApiError>`, because it is the one that
    // carries the house diagnostic.
    let scope_ty = match &scoped_by {
        Some(path) => quote! { #path },
        None => quote! { ::charpente::core::table::Unscoped },
    };
    let tenant_scope_assert = if let Some(path) = &scoped_by {
        quote! {
            const _: () = {
                const fn __charpente_assert_tenant_scope<T: ::charpente::TenantScope>() {}
                __charpente_assert_tenant_scope::<#path>();
            };
        }
    } else {
        quote! {}
    };

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

    // The typed index accessor reads through whichever surface can address
    // this table: `View` (a `Snapshot` or a `Tx`) for an unscoped table,
    // `ScopedTx` for a scoped one, whose `scan_by` is already bounded to one
    // tenant. Emitting the `View` form for a scoped table would not merely
    // be a cross-tenant read — `View`'s bound is `UnscopedRow`, so it would
    // not compile at all, and `#[table(scoped_by = …)]` with an `#[index]`
    // would be unusable.
    let (by_fn_receiver, by_fn_docs): (proc_macro2::TokenStream, proc_macro2::TokenStream) =
        match &scoped_by {
            Some(path) => (
                quote! { view: &::charpente::ScopedTx<'_, '_, #path> },
                quote! {
                    /// Exact-match lookup on the `#[index]` field within ONE
                    /// tenant, in id order. Takes the `ScopedTx` that
                    /// `tx.scope::<…>(scope, ctx)?` hands back: this table is
                    /// scoped, so there is no tenant-free way to ask.
                },
            ),
            None => (
                quote! { view: &impl ::charpente::core::table::View },
                quote! {
                    /// Exact-match lookup on the `#[index]` field, in id order.
                    /// Works on any `View`: a `Snapshot` outside transactions or
                    /// a `Tx` inside one.
                },
            ),
        };

    Ok(quote! {
        #(#attrs)*
        #vis struct #name {
            #(#emitted_fields,)*
        }

        impl ::charpente::core::table::Row for #name {
            type Scope = #scope_ty;
            const TABLE: &'static str = #name_str;
            const VERSION: u32 = #version;
            const FINGERPRINT: ::std::option::Option<u64> =
                ::std::option::Option::Some(#fingerprint);
            const INDEXES: &'static [::charpente::core::table::IndexDef<Self>] = &[
                #(::charpente::core::table::IndexDef {
                    name: #index_names,
                    key: Self::#index_fn_idents,
                },)*
            ];

            fn id(&self) -> ::charpente::core::table::Id<Self> {
                self.id
            }

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
        #tenant_scope_assert

        impl #name {
            #(
                #[doc(hidden)]
                fn #index_fn_idents(row: &Self, out: &mut ::std::vec::Vec<u8>) {
                    ::charpente::core::table::Codec::encode(&row.#index_field_idents, out);
                }

                #by_fn_docs
                #vis fn #by_fn_idents(
                    #by_fn_receiver,
                    key: &#index_field_types,
                ) -> ::std::io::Result<::std::vec::Vec<Self>> {
                    view.scan_by::<Self>(#index_names, key)
                }
            )*
        }
    })
}
