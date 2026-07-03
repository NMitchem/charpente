//! The derive surface (DESIGN.md §3): `#[table]` turns a struct into a typed
//! row; `#[route]` turns a function into a typed HTTP handler. Macro errors
//! are product surface (hard rule 5): each one names the contract and the fix.
//!
//! `#[table]` references `::charpente_core` paths; `#[route]` references
//! `::charpente` paths — the using crate must depend on the respective crate.

mod job;
mod route;
mod table;

use proc_macro::TokenStream;
use syn::{DeriveInput, Error, ItemFn, parse_macro_input};

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
    match table::expand(input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
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
