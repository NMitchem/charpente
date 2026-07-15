//! `#[route(METHOD "/path/:param"[, auth])]` (DESIGN.md §3, spec
//! 2026-07-02-http-layer): validates the handler signature against the path
//! and emits typed extraction glue plus a `RouteDef` marker struct. The
//! argument convention — `tx`, one arg per `:param` by name in path order,
//! at most one JSON body arg, optional trailing `ctx` — is enforced here
//! with errors that name the contract and the fix.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{
    Error, FnArg, GenericArgument, Ident, ItemFn, LitStr, Pat, PatType, PathArguments, ReturnType,
    Token, Type,
};

pub(crate) struct RouteArgs {
    method: Ident,
    path: LitStr,
    auth: bool,
}

impl Parse for RouteArgs {
    fn parse(input: ParseStream) -> syn::Result<RouteArgs> {
        let method: Ident = input.parse().map_err(|e| {
            Error::new(
                e.span(),
                "write `#[route(METHOD \"/path\")]` — e.g. #[route(GET \"/notes/:id\")]",
            )
        })?;
        let path: LitStr = input.parse().map_err(|e| {
            Error::new(
                e.span(),
                "the route path must be a string literal — e.g. #[route(GET \"/notes/:id\")]",
            )
        })?;
        let mut auth = false;
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            let flag: Ident = input.parse()?;
            if flag != "auth" {
                return Err(Error::new(
                    flag.span(),
                    "the only route flag is `auth` — write `#[route(GET \"/path\", auth)]`",
                ));
            }
            auth = true;
        }
        if !input.is_empty() {
            return Err(input.error("unexpected tokens after the route flags"));
        }
        Ok(RouteArgs { method, path, auth })
    }
}

fn arg_name(pat: &PatType) -> Option<String> {
    match &*pat.pat {
        Pat::Ident(ident) => Some(ident.ident.to_string()),
        _ => None,
    }
}

pub(crate) fn expand(args: RouteArgs, func: ItemFn) -> syn::Result<TokenStream> {
    if args.method == "SSE" {
        return expand_sse(args, func);
    }
    expand_http(args, func)
}

fn expand_http(args: RouteArgs, func: ItemFn) -> syn::Result<TokenStream> {
    let method_variant = match args.method.to_string().as_str() {
        "GET" => quote!(Get),
        "POST" => quote!(Post),
        "PUT" => quote!(Put),
        "PATCH" => quote!(Patch),
        "DELETE" => quote!(Delete),
        other => {
            return Err(Error::new(
                args.method.span(),
                format!("unknown method {other} — one of GET, POST, PUT, PATCH, DELETE, SSE"),
            ));
        }
    };
    let pattern = args.path.value();
    if !pattern.starts_with('/') {
        return Err(Error::new(
            args.path.span(),
            format!("route path must start with '/' — got \"{pattern}\""),
        ));
    }
    let params: Vec<String> = pattern[1..]
        .split('/')
        .filter_map(|s| s.strip_prefix(':').map(str::to_string))
        .collect();
    if params.iter().any(String::is_empty) {
        return Err(Error::new(
            args.path.span(),
            format!("empty :param name in \"{pattern}\""),
        ));
    }

    let name = &func.sig.ident;
    let name_str = name.to_string();
    let vis = &func.vis;

    let mut inputs = func.sig.inputs.iter();
    let tx_ok = matches!(
        inputs.next(),
        Some(FnArg::Typed(pat)) if arg_name(pat).as_deref() == Some("tx")
    );
    if !tx_ok {
        return Err(Error::new(
            func.sig.span(),
            format!(
                "a #[route] handler's first argument must be `tx: &mut Tx` — `{name_str}` is missing it"
            ),
        ));
    }
    let rest: Vec<&PatType> = inputs
        .map(|arg| match arg {
            FnArg::Typed(pat) => Ok(pat),
            FnArg::Receiver(r) => Err(Error::new(
                r.span(),
                "#[route] handlers are free functions — no `self`",
            )),
        })
        .collect::<syn::Result<_>>()?;

    let has_ctx = rest
        .last()
        .is_some_and(|p| arg_name(p).as_deref() == Some("ctx"));
    let non_ctx = if has_ctx {
        &rest[..rest.len() - 1]
    } else {
        &rest[..]
    };

    if non_ctx.len() < params.len() || non_ctx.len() > params.len() + 1 {
        return Err(Error::new(
            func.sig.span(),
            format!(
                "`{name_str}` takes {} argument(s) after `tx` (excluding `ctx`) but the path \
                 \"{pattern}\" has {} :param(s) — write one argument per :param (same name, \
                 path order) plus at most one JSON body argument",
                non_ctx.len(),
                params.len()
            ),
        ));
    }
    for (i, param) in params.iter().enumerate() {
        let got = arg_name(non_ctx[i]);
        if got.as_deref() != Some(param.as_str()) {
            return Err(Error::new(
                non_ctx[i].span(),
                format!(
                    "this argument must be named `{param}` to match :{param} in \"{pattern}\" — \
                     got `{}`",
                    got.unwrap_or_else(|| "<pattern>".to_string())
                ),
            ));
        }
    }
    let has_body = non_ctx.len() == params.len() + 1;

    let glue_name = format_ident!("__charpente_route_{name}");
    let param_lets: Vec<TokenStream> = params
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let ident = format_ident!("{p}");
            let ty = &non_ctx[i].ty;
            quote! {
                let #ident = <#ty as ::charpente::http::FromParam>::from_param(#p, &captures[#i])?;
            }
        })
        .collect();
    let body_let = if has_body {
        let pat = non_ctx[params.len()];
        let ident = format_ident!(
            "{}",
            arg_name(pat)
                .ok_or_else(|| Error::new(pat.span(), "the body argument must be a plain name"))?
        );
        let ty = &pat.ty;
        quote! { let #ident: #ty = ::charpente::http::decode_body(body)?; }
    } else {
        quote!()
    };
    let mut call_args: Vec<TokenStream> = vec![quote!(tx)];
    for p in &params {
        let ident = format_ident!("{p}");
        call_args.push(quote!(#ident));
    }
    if has_body {
        let ident = format_ident!(
            "{}",
            arg_name(non_ctx[params.len()]).expect("checked above")
        );
        call_args.push(quote!(#ident));
    }
    if has_ctx {
        call_args.push(quote!(ctx));
    }
    let auth = args.auth;
    let pattern_lit = &args.path;

    Ok(quote! {
        #func

        #[doc(hidden)]
        #[allow(non_snake_case)]
        fn #glue_name(
            tx: &mut ::charpente::Tx,
            ctx: &::charpente::http::Ctx,
            captures: &[::std::string::String],
            body: &[u8],
        ) -> ::std::result::Result<::charpente::http::Response, ::charpente::http::ApiError> {
            let _ = (ctx, captures, body); // routes without params/body/ctx use none of them
            #(#param_lets)*
            #body_let
            let out = #name(#(#call_args),*)?;
            ::std::result::Result::Ok(::charpente::http::Response::json(200, &out))
        }

        /// Route marker for `routes![…]` — `#[route]` emitted it.
        #[allow(non_camel_case_types)]
        #vis struct #name {}

        impl ::charpente::http::RouteDef for #name {
            fn route() -> ::charpente::http::Route {
                ::charpente::http::Route {
                    method: ::charpente::http::Method::#method_variant,
                    pattern: #pattern_lit,
                    auth: #auth,
                    name: #name_str,
                    glue: ::charpente::http::RouteGlue::Http(#glue_name),
                }
            }
        }
    })
}

/// `#[route(SSE "/path")]`: registers `Method::Get` plus a subscribe-
/// flavored wrapper. The handler contract (spec docs/superpowers/specs/
/// 2026-07-14-realtime-design.md §2): `fn(tx: &mut Tx, [captures,]
/// ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError>` —
/// unlike a plain route, `ctx` and `cursor` are both mandatory (no optional
/// trailing `ctx`, no body argument).
fn expand_sse(args: RouteArgs, func: ItemFn) -> syn::Result<TokenStream> {
    let pattern = args.path.value();
    if !pattern.starts_with('/') {
        return Err(Error::new(
            args.path.span(),
            format!("route path must start with '/' — got \"{pattern}\""),
        ));
    }
    let params: Vec<String> = pattern[1..]
        .split('/')
        .filter_map(|s| s.strip_prefix(':').map(str::to_string))
        .collect();
    if params.iter().any(String::is_empty) {
        return Err(Error::new(
            args.path.span(),
            format!("empty :param name in \"{pattern}\""),
        ));
    }

    let name = &func.sig.ident;
    let name_str = name.to_string();
    let vis = &func.vis;

    let mut inputs = func.sig.inputs.iter();
    let tx_ok = matches!(
        inputs.next(),
        Some(FnArg::Typed(pat)) if arg_name(pat).as_deref() == Some("tx")
    );
    if !tx_ok {
        return Err(Error::new(
            func.sig.span(),
            format!(
                "a #[route] handler's first argument must be `tx: &mut Tx` — `{name_str}` is missing it"
            ),
        ));
    }
    let rest: Vec<&PatType> = inputs
        .map(|arg| match arg {
            FnArg::Typed(pat) => Ok(pat),
            FnArg::Receiver(r) => Err(Error::new(
                r.span(),
                "#[route] handlers are free functions — no `self`",
            )),
        })
        .collect::<syn::Result<_>>()?;

    if rest.len() != params.len() + 2 {
        return Err(Error::new(
            func.sig.span(),
            format!(
                "an SSE route handler is `fn {name_str}(tx: &mut Tx, {}ctx: &Ctx, \
                 cursor: Option<Cursor>) -> Result<Subscription, ApiError>` — one argument \
                 per :param in \"{pattern}\" (path order), then `ctx`, then `cursor`",
                params
                    .iter()
                    .map(|p| format!("{p}: _, "))
                    .collect::<String>()
            ),
        ));
    }
    for (i, param) in params.iter().enumerate() {
        let got = arg_name(rest[i]);
        if got.as_deref() != Some(param.as_str()) {
            return Err(Error::new(
                rest[i].span(),
                format!(
                    "this argument must be named `{param}` to match :{param} in \"{pattern}\" — \
                     got `{}`",
                    got.unwrap_or_else(|| "<pattern>".to_string())
                ),
            ));
        }
    }
    let ctx_arg = rest[params.len()];
    if arg_name(ctx_arg).as_deref() != Some("ctx") {
        return Err(Error::new(
            ctx_arg.span(),
            format!(
                "an SSE route handler's next-to-last argument must be named `ctx: &Ctx` — got `{}`",
                arg_name(ctx_arg).unwrap_or_else(|| "<pattern>".to_string())
            ),
        ));
    }
    let cursor_arg = rest[params.len() + 1];
    if arg_name(cursor_arg).as_deref() != Some("cursor") {
        return Err(Error::new(
            cursor_arg.span(),
            format!(
                "an SSE route handler's last argument must be named `cursor: Option<Cursor>` — \
                 got `{}`",
                arg_name(cursor_arg).unwrap_or_else(|| "<pattern>".to_string())
            ),
        ));
    }

    let ReturnType::Type(_, ret_ty) = &func.sig.output else {
        return Err(Error::new(
            func.sig.span(),
            "an SSE route returns Result<Subscription, ApiError> — write \
             `-> Result<Subscription, ApiError>`",
        ));
    };
    if !is_subscription_result(ret_ty) {
        return Err(Error::new(
            ret_ty.span(),
            "an SSE route returns Result<Subscription, ApiError> — got a different return type",
        ));
    }

    let glue_name = format_ident!("__charpente_route_{name}");
    let param_lets: Vec<TokenStream> = params
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let ident = format_ident!("{p}");
            let ty = &rest[i].ty;
            quote! {
                let #ident = <#ty as ::charpente::http::FromParam>::from_param(#p, &captures[#i])?;
            }
        })
        .collect();
    let mut call_args: Vec<TokenStream> = vec![quote!(tx)];
    for p in &params {
        let ident = format_ident!("{p}");
        call_args.push(quote!(#ident));
    }
    call_args.push(quote!(ctx));
    call_args.push(quote!(cursor));

    let auth = args.auth;
    let pattern_lit = &args.path;

    Ok(quote! {
        #func

        #[doc(hidden)]
        #[allow(non_snake_case)]
        fn #glue_name(
            tx: &mut ::charpente::Tx,
            ctx: &::charpente::http::Ctx,
            captures: &[::std::string::String],
            cursor: ::std::option::Option<::charpente::http::Cursor>,
        ) -> ::std::result::Result<::charpente::http::Subscription, ::charpente::http::ApiError> {
            let _ = captures; // SSE routes without :params use none of them
            #(#param_lets)*
            #name(#(#call_args),*)
        }

        /// Route marker for `routes![…]` — `#[route]` emitted it.
        #[allow(non_camel_case_types)]
        #vis struct #name {}

        impl ::charpente::http::RouteDef for #name {
            fn route() -> ::charpente::http::Route {
                ::charpente::http::Route {
                    method: ::charpente::http::Method::Get,
                    pattern: #pattern_lit,
                    auth: #auth,
                    name: #name_str,
                    glue: ::charpente::http::RouteGlue::Sse(#glue_name),
                }
            }
        }
    })
}

/// Checks the syntactic shape `Result<Subscription, ApiError>` (a macro
/// can't do full type-checking — a mismatch that slips past this still
/// fails to compile against `SubGlue`'s signature, just with rustc's own
/// message instead of this one).
fn is_subscription_result(ty: &Type) -> bool {
    let Type::Path(path) = ty else { return false };
    let Some(seg) = path.path.segments.last() else {
        return false;
    };
    if seg.ident != "Result" {
        return false;
    }
    let PathArguments::AngleBracketed(generics) = &seg.arguments else {
        return false;
    };
    let types: Vec<&Type> = generics
        .args
        .iter()
        .filter_map(|a| match a {
            GenericArgument::Type(t) => Some(t),
            _ => None,
        })
        .collect();
    types.len() == 2
        && last_segment_is(types[0], "Subscription")
        && last_segment_is(types[1], "ApiError")
}

fn last_segment_is(ty: &Type, ident: &str) -> bool {
    matches!(ty, Type::Path(p) if p.path.segments.last().is_some_and(|s| s.ident == ident))
}
