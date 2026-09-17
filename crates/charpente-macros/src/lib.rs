//! The derive surface (DESIGN.md §3): `#[table]` turns a struct into a typed
//! row; `#[route]` turns a function into a typed HTTP handler. Macro errors
//! are product surface (hard rule 5): each one names the contract and the fix.
//!
//! All three macros reference `::charpente` paths (`charpente::core` for the
//! deterministic core) — the using crate depends on `charpente` alone.

mod event;
mod fingerprint;
mod job;
mod route;
mod row_shape;
mod table;
mod wire;

use proc_macro::TokenStream;
use syn::parse::Parser;
use syn::spanned::Spanned;
use syn::{DeriveInput, Error, ItemFn, parse_macro_input};

#[proc_macro_attribute]
pub fn table(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = match parse_table_args(args) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error().into(),
    };
    let input = parse_macro_input!(input as DeriveInput);
    if args.scoped_by.is_none() && !args.global {
        let name = &input.ident;
        return Error::new(
            proc_macro2::Span::call_site(),
            format!(
                "`{name}` declares no tenancy — a table says which: write \
                 `#[table(scoped_by = Org)]` if every row belongs to one tenant (the key \
                 becomes `(scope, id)` and a bare `Tx` cannot read it), or `#[table(global)]` \
                 if its rows belong to no tenant. Neither is the default."
            ),
        )
        .to_compile_error()
        .into();
    }
    match table::expand(input, args.version, args.scoped_by) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn row_shape(args: TokenStream, input: TokenStream) -> TokenStream {
    let version = match parse_version(args) {
        Ok(v) => v,
        Err(e) => return e.to_compile_error().into(),
    };
    let input = parse_macro_input!(input as DeriveInput);
    match row_shape::expand(input, version) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// `#[row_shape(version = N)]`, N >= 1 — always required (`#[row_shape]`
/// never defaults its version, unlike bare `#[table]`). The only caller is
/// `row_shape` above, so this no longer takes a macro name or an
/// optionality flag — inlined rather than kept generic for one call site.
fn parse_version(args: TokenStream) -> syn::Result<u32> {
    if args.is_empty() {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[row_shape] requires `version = N` — write `#[row_shape(version = 1)]`",
        ));
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
        .map_err(|e| syn::Error::new(e.span(), format!("#[row_shape] {e}")))
}

/// `#[table]`'s parsed argument list: `version = N` (default 1),
/// `scoped_by = <Type>` (`#[table(scoped_by = Org)]`) and the bare word
/// `global` (`#[table(global)]`), comma separated, any order — but each at
/// most once (`route`'s `auth`/`emits` and `job`'s `max`/`backoff` refuse a
/// repeat by name rather than last-write-wins; this follows the same
/// precedent), and never both `scoped_by` and `global`.
struct TableArgs {
    version: u32,
    scoped_by: Option<syn::Path>,
    /// The explicit declaration that no row belongs to a tenant
    /// (default-secure spec §3). Changes nothing in expansion — a `global`
    /// table is what a bare `#[table]` was before absence became a refusal.
    global: bool,
}

enum TableArg {
    Version(u32, proc_macro2::Span),
    ScopedBy(syn::Path, proc_macro2::Span),
    Global(proc_macro2::Span),
}

impl syn::parse::Parse for TableArg {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<TableArg> {
        let ident: syn::Ident = input.parse()?;
        if ident == "global" {
            return Ok(TableArg::Global(ident.span()));
        }
        input.parse::<syn::Token![=]>()?;
        if ident == "version" {
            let lit: syn::LitInt = input.parse()?;
            let version: u32 = lit.base10_parse()?;
            Ok(TableArg::Version(version, lit.span()))
        } else if ident == "scoped_by" {
            let path: syn::Path = input.parse()?;
            let span = path.span();
            Ok(TableArg::ScopedBy(path, span))
        } else {
            Err(syn::Error::new(
                ident.span(),
                "takes `version = N`, `scoped_by = <Type>` or `global`",
            ))
        }
    }
}

fn parse_table_args(args: TokenStream) -> syn::Result<TableArgs> {
    if args.is_empty() {
        return Ok(TableArgs {
            version: 1,
            scoped_by: None,
            global: false,
        });
    }
    let args2: proc_macro2::TokenStream = args.into();
    let parsed = syn::punctuated::Punctuated::<TableArg, syn::Token![,]>::parse_terminated
        .parse2(args2)
        .map_err(|e| syn::Error::new(e.span(), format!("#[table] {e}")))?;

    let mut version = None;
    let mut scoped_by = None;
    let mut global: Option<proc_macro2::Span> = None;
    for arg in parsed {
        match arg {
            TableArg::Version(v, span) => {
                if version.is_some() {
                    return Err(syn::Error::new(
                        span,
                        "`version` given twice — write it once",
                    ));
                }
                if v == 0 {
                    return Err(syn::Error::new(
                        span,
                        "#[table] version 0 does not exist — versions start at 1",
                    ));
                }
                version = Some(v);
            }
            TableArg::ScopedBy(path, span) => {
                if scoped_by.is_some() {
                    return Err(syn::Error::new(
                        span,
                        "`scoped_by` given twice — write it once",
                    ));
                }
                scoped_by = Some(path);
            }
            TableArg::Global(span) => {
                if global.is_some() {
                    return Err(syn::Error::new(
                        span,
                        "`global` given twice — write it once",
                    ));
                }
                global = Some(span);
            }
        }
    }
    if let (Some(span), Some(path)) = (global, &scoped_by) {
        let scope = &path
            .segments
            .last()
            .expect("a parsed path has a segment")
            .ident;
        return Err(syn::Error::new(
            span,
            format!(
                "`global` and `scoped_by = {scope}` on one table — a table is one or the other; drop one"
            ),
        ));
    }
    Ok(TableArgs {
        version: version.unwrap_or(1),
        scoped_by,
        global: global.is_some(),
    })
}

#[proc_macro_attribute]
pub fn route(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as route::RouteArgs);
    let func = parse_macro_input!(input as ItemFn);
    match route::expand(args, func) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn job(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as job::JobArgs);
    let input = parse_macro_input!(input as DeriveInput);
    match job::expand(args, input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn event(args: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    if !args.is_empty() {
        return Error::new(
            input.ident.span(),
            "write `#[event]` with no arguments; the name derives from the type",
        )
        .to_compile_error()
        .into();
    }
    match event::expand(input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_derive(Wire, attributes(serde))]
pub fn wire(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    wire::expand(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
