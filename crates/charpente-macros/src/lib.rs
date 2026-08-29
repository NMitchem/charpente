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
use syn::{DeriveInput, Error, ItemFn, parse_macro_input};

#[proc_macro_attribute]
pub fn table(args: TokenStream, input: TokenStream) -> TokenStream {
    let version = match parse_version(args, "table", false) {
        Ok(v) => v,
        Err(e) => return e.to_compile_error().into(),
    };
    let input = parse_macro_input!(input as DeriveInput);
    match table::expand(input, version) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn row_shape(args: TokenStream, input: TokenStream) -> TokenStream {
    let version = match parse_version(args, "row_shape", true) {
        Ok(v) => v,
        Err(e) => return e.to_compile_error().into(),
    };
    let input = parse_macro_input!(input as DeriveInput);
    match row_shape::expand(input, version) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// `version = N`, N >= 1. `required`: `#[row_shape]` always names its
/// version; bare `#[table]` defaults to 1 (exact today's behavior).
fn parse_version(args: TokenStream, macro_name: &str, required: bool) -> syn::Result<u32> {
    if args.is_empty() {
        if required {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "#[row_shape] requires `version = N` — write `#[row_shape(version = 1)]`",
            ));
        }
        return Ok(1);
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
        .map_err(|e| syn::Error::new(e.span(), format!("#[{macro_name}] {e}")))
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
