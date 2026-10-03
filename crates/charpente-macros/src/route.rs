//! `#[route(METHOD "/path/:param", auth | public | public(Scope)[, cross_tenant][, emits(Ev, …)])]`
//! (DESIGN.md §3, specs 2026-07-02-http-layer,
//! 2026-08-20-ts-client-codegen §2 and 2026-08-30-tenant-scoping §3):
//! validates the handler signature against the path and emits typed
//! extraction glue plus a `RouteDef` marker struct. The argument convention
//! — `tx`, one arg per `:param` by name in path order, at most one JSON body
//! arg, an optional `cap: &CrossTenant` on a `cross_tenant` route, optional
//! trailing `ctx` — is enforced here with errors that name the contract and
//! the fix.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{
    Error, FnArg, GenericArgument, Ident, ItemFn, LitStr, Pat, PatType, PathArguments, ReturnType,
    Token, Type, parenthesized,
};

pub(crate) struct RouteArgs {
    method: Ident,
    path: LitStr,
    auth: bool,
    /// `public` — the explicit declaration that anonymous callers reach this
    /// handler (default-secure spec §2). Expands to nothing: a `public` route
    /// is what a bare route was before absence became a refusal. Exactly one
    /// of `auth`/`public` is required (checked in `expand`, where the handler
    /// name is known for the message).
    public: bool,
    /// `public(Scope)` — the declaration that every caller, credentialed or
    /// not, may act inside the `Scope` tenant this route opens, with no
    /// `authorize` (job-tenancy spec §4). `Some` implies `public`.
    public_scope: Option<syn::Path>,
    /// `cross_tenant` — the declared capability to enumerate a scoped table
    /// across every tenant (tenant-scoping spec §3). Drives two things: the
    /// `cap: &CrossTenant` argument the glue may fill, and the flag
    /// `charpente describe` lists so the audit is an inventory.
    cross_tenant: bool,
    /// The event types a stream route declares with `emits(Ev, …)`
    /// (TS-codegen spec §2) — empty when the attribute carries none, which
    /// is every non-stream route and any stream route that has not adopted
    /// the declaration.
    emits: Vec<Ident>,
}

impl Parse for RouteArgs {
    fn parse(input: ParseStream) -> syn::Result<RouteArgs> {
        let method: Ident = input.parse().map_err(|e| {
            Error::new(
                e.span(),
                "write `#[route(METHOD \"/path\", auth | public | public(<Scope>))]` — e.g. \
                 #[route(GET \"/notes/:id\", auth)]",
            )
        })?;
        let path: LitStr = input.parse().map_err(|e| {
            Error::new(
                e.span(),
                "the route path must be a string literal — e.g. #[route(GET \"/notes/:id\", auth)]",
            )
        })?;
        let mut auth = false;
        let mut public = false;
        let mut public_scope: Option<syn::Path> = None;
        let mut cross_tenant = false;
        let mut emits: Vec<Ident> = Vec::new();
        while input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            let flag: Ident = input.parse().map_err(|e| Error::new(e.span(), FLAGS))?;
            if flag == "auth" {
                if auth {
                    return Err(Error::new(
                        flag.span(),
                        "`auth` given twice — write it once",
                    ));
                }
                if public {
                    return Err(Error::new(flag.span(), ONE_OR_THE_OTHER));
                }
                auth = true;
            } else if flag == "public" {
                if public {
                    return Err(Error::new(
                        flag.span(),
                        "`public` given twice — write it once",
                    ));
                }
                if auth {
                    return Err(Error::new(flag.span(), ONE_OR_THE_OTHER));
                }
                public = true;
                if input.peek(syn::token::Paren) {
                    let content;
                    parenthesized!(content in input);
                    let mut scopes = content.parse_terminated(syn::Path::parse, Token![,])?;
                    if scopes.len() != 1 {
                        return Err(Error::new(flag.span(), PUBLIC_SCOPE_ARITY));
                    }
                    public_scope = scopes.pop().map(|pair| pair.into_value());
                }
            } else if flag == "cross_tenant" {
                if cross_tenant {
                    return Err(Error::new(
                        flag.span(),
                        "`cross_tenant` given twice — write it once",
                    ));
                }
                cross_tenant = true;
            } else if flag == "emits" {
                // Non-empty means an earlier `emits(...)` clause landed:
                // `emits()` is refused below, so the vector cannot be
                // non-empty for any other reason.
                if !emits.is_empty() {
                    return Err(Error::new(
                        flag.span(),
                        "`emits(...)` given twice — a route declares one set, so name every \
                         event in one list: `emits(NoteAdded, NoteRemoved)`",
                    ));
                }
                // Peeked, not left to `parenthesized!`: that macro's own
                // early return is syn's generic "expected parentheses",
                // which names no fix (hard rule 5). `emits` with the
                // parentheses forgotten is a realistic typo, so it gets a
                // charpente message like every other branch here.
                if !input.peek(syn::token::Paren) {
                    return Err(Error::new(
                        flag.span(),
                        "emits takes a parenthesized list of #[event] types — write \
                         `emits(NoteAdded, NoteRemoved)`",
                    ));
                }
                let content;
                parenthesized!(content in input);
                let names = content.parse_terminated(Ident::parse, Token![,])?;
                if names.is_empty() {
                    return Err(Error::new(
                        flag.span(),
                        "emits(...) must name at least one #[event] type — write \
                         `emits(NoteAdded, NoteRemoved)`, or drop it",
                    ));
                }
                for name in names {
                    if emits.contains(&name) {
                        return Err(Error::new(
                            name.span(),
                            format!(
                                "`{name}` is named twice in emits(...) — a stream declares each \
                                 event type once, and a repeat would put the same member in a \
                                 generated client's union twice; write it once"
                            ),
                        ));
                    }
                    emits.push(name);
                }
            } else {
                return Err(Error::new(flag.span(), FLAGS));
            }
        }
        if !input.is_empty() {
            return Err(input.error("unexpected tokens after the route flags"));
        }
        Ok(RouteArgs {
            method,
            path,
            auth,
            public,
            public_scope,
            cross_tenant,
            emits,
        })
    }
}

/// The one wording for "that isn't a route flag", shared by the two sites
/// that can reach it (a non-identifier token, and an identifier that isn't
/// one of the flags) so they cannot drift apart. Pinned by
/// `docs/GUIDE.md`'s error catalog (hard rule 5).
const FLAGS: &str = "a route flag is `auth`, `public`, `public(<Scope>)`, `cross_tenant` or \
                     `emits(Ev, …)` — write `#[route(GET \"/path\", auth)]`, \
                     `#[route(GET \"/path\", public)]`, \
                     `#[route(GET \"/orgs/:org/status\", public(Org))]`, or \
                     `#[route(SSE \"/path\", emits(NoteAdded, NoteRemoved))]`";

/// A route answers "who reaches this handler" once. Shared by the two
/// branches that can see both flags so the wording cannot drift.
const ONE_OR_THE_OTHER: &str =
    "`auth` and `public` on one route — a route is one or the other; drop one";

/// `public(...)` with no path, or with several. Spanned at the flag, the one
/// token both shapes share.
const PUBLIC_SCOPE_ARITY: &str = "`public(...)` names exactly one tenant scope type — write \
                                  `public(Org)`, or plain `public` if anonymous callers enter \
                                  no tenant here";

/// The refusal for a handler that takes the capability token on a route
/// that never declared it — the compile error tenant-scoping spec §3 calls
/// for. Names the contract, the site (the argument's own span, and the route
/// by its method and path) and the fix (hard rule 5). The fix names the one
/// flag to add and restates no attribute, so the route's other flags — its
/// stance, `emits(…)`, any later one — cannot drop out of a pasted fix
/// (tenancy-completion spec §7, item 12). Shared by both route flavors so the
/// two cannot drift.
fn undeclared_capability(name: &str, args: &RouteArgs, pat: &PatType) -> Error {
    let method = &args.method;
    let path = args.path.value();
    Error::new(
        pat.span(),
        format!(
            "`{name}` takes `cap: &CrossTenant`, but its `#[route({method} \"{path}\", …)]` \
             does not declare `cross_tenant` — enumerating a scoped table across every \
             tenant (`tx.across_tenants`) and fanning a job out into a tenant the route \
             names (`tx.enqueue_in`) are one declared capability, and `charpente describe` \
             lists every route that declared it. Add `cross_tenant` to that attribute's \
             flags, keeping the others, or drop the `cap` argument and read or enqueue in \
             one tenant through `tx.scope::<Org>(org, ctx)?`"
        ),
    )
}

fn arg_name(pat: &PatType) -> Option<String> {
    match &*pat.pat {
        Pat::Ident(ident) => Some(ident.ident.to_string()),
        _ => None,
    }
}

/// `public(Scope)`'s emission: the `PublicScope` the `Route` carries. `name`
/// is read through `::charpente::tenant::tenant_name`. Its `TenantScope`
/// bound refuses a scope type with no membership check, with the same
/// diagnostic `#[table(scoped_by = …)]` gives, spanned at the path inside
/// `public(...)`.
fn public_scope_tokens(args: &RouteArgs) -> TokenStream {
    let Some(path) = &args.public_scope else {
        return quote! { ::std::option::Option::None };
    };
    quote! {
        ::std::option::Option::Some(::charpente::http::PublicScope {
            name: ::charpente::tenant::tenant_name::<#path>(),
            type_id: ::std::any::TypeId::of::<#path>,
            type_name: ::std::any::type_name::<#path>,
        })
    }
}

pub(crate) fn expand(args: RouteArgs, func: ItemFn) -> syn::Result<TokenStream> {
    if !args.auth && !args.public {
        let name = &func.sig.ident;
        let method = &args.method;
        let path = args.path.value();
        return Err(Error::new(
            args.path.span(),
            format!(
                "`{name}` declares neither `auth` nor `public` — a route says which callers \
                 reach it. Add one of these to the flags of its \
                 `#[route({method} \"{path}\", …)]`, keeping any it already has: `auth` to \
                 require a principal (`ctx.principal()` is `Some`), `public` to serve anonymous \
                 callers, or `public(Org)` to also let every caller act inside the `Org` tenant \
                 it opens. None is the default."
            ),
        ));
    }
    if !args.emits.is_empty() && args.method != "SSE" {
        return Err(Error::new(
            args.path.span(),
            "emits(...) applies to SSE routes only — a plain route returns one response, \
             not a stream of events; drop emits(...), or change this route's method to `SSE`, \
             keeping its path and flags",
        ));
    }
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

    // The capability token, if the handler takes one: the argument named
    // `cap`, after the :params and any body, before the optional `ctx`.
    // Only a SPARE argument can be it — with exactly one argument per
    // :param there is nothing left over, so a route with a `:cap` param
    // still binds that param, not a token.
    let has_cap = non_ctx.len() > params.len()
        && non_ctx
            .last()
            .is_some_and(|p| arg_name(p).as_deref() == Some("cap"));
    if has_cap && !args.cross_tenant {
        return Err(undeclared_capability(
            &name_str,
            &args,
            non_ctx[non_ctx.len() - 1],
        ));
    }
    let non_ctx = if has_cap {
        &non_ctx[..non_ctx.len() - 1]
    } else {
        non_ctx
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
    if has_cap {
        call_args.push(quote!(cap));
    }
    if has_ctx {
        call_args.push(quote!(ctx));
    }
    // The token comes from the same `Ctx` field `Ctx::cap()` reads — one
    // source of truth, with the argument as its typed spelling. `expect` is
    // an invariant, not a fallible read: this `let` is emitted only for a
    // route whose `Route.cross_tenant` this same expansion set, and the
    // kernel mints the token from exactly that flag.
    let cap_let = if has_cap {
        quote! {
            let cap = ctx.cap().expect(
                "route declares cross_tenant but the kernel minted no CrossTenant — \
                 the flag on Route and the token in Ctx come from the same expansion"
            );
        }
    } else {
        quote!()
    };
    let auth = args.auth;
    let public_scope = public_scope_tokens(&args);
    let cross_tenant = args.cross_tenant;
    let pattern_lit = &args.path;

    let ok_ty = result_ok_type(&func.sig.output)?;
    let err_ty = result_err_type(&func.sig.output)?;

    let param_schema_entries: Vec<TokenStream> = params
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let ty = &non_ctx[i].ty;
            quote! { (#p, (|| <#ty as ::charpente::wire::Wire>::reference()) as fn() -> _) }
        })
        .collect();

    let request_tok = if has_body {
        let ty = &non_ctx[params.len()].ty;
        quote! { ::std::option::Option::Some(
            (|| <#ty as ::charpente::wire::Wire>::reference()) as fn() -> _
        ) }
    } else {
        quote! { ::std::option::Option::None }
    };

    let body_register = if has_body {
        let ty = &non_ctx[params.len()].ty;
        quote! { <#ty as ::charpente::wire::Wire>::register(reg); }
    } else {
        quote! {}
    };
    let param_registers: Vec<TokenStream> = params
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let ty = &non_ctx[i].ty;
            quote! { <#ty as ::charpente::wire::Wire>::register(reg); }
        })
        .collect();

    Ok(quote! {
        #func

        #[doc(hidden)]
        #[allow(non_snake_case)]
        fn #glue_name(
            tx: &mut ::charpente::Tx,
            ctx: &::charpente::http::Ctx<'_>,
            captures: &[::std::string::String],
            body: &[u8],
        ) -> ::std::result::Result<::charpente::http::Response, ::charpente::http::ApiError> {
            let _ = (ctx, captures, body); // routes without params/body/ctx use none of them
            #(#param_lets)*
            #body_let
            #cap_let
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
                    public_scope: #public_scope,
                    cross_tenant: #cross_tenant,
                    name: #name_str,
                    glue: ::charpente::http::RouteGlue::Http(#glue_name),
                    params: &[#(#param_schema_entries),*],
                    request: #request_tok,
                    response: || <#ok_ty as ::charpente::wire::Wire>::reference(),
                    error: || <#err_ty as ::charpente::wire::Wire>::reference(),
                    register_types: |reg| {
                        #(#param_registers)*
                        #body_register
                        <#ok_ty as ::charpente::wire::Wire>::register(reg);
                        <#err_ty as ::charpente::wire::Wire>::register(reg);
                    },
                    emits: &[],
                }
            }
        }
    })
}

/// Extracts `T` from a handler's `-> Result<T, E>` return type — the client
/// needs both halves to generate a typed result, so a handler that returns
/// anything else (a bare value, a type alias hiding the error type) is a
/// compile error naming why (hard rule 5).
fn result_ok_type(out: &ReturnType) -> syn::Result<syn::Type> {
    result_arg(out, 0)
}

/// Extracts `E` from `-> Result<T, E>` — see `result_ok_type`.
fn result_err_type(out: &ReturnType) -> syn::Result<syn::Type> {
    result_arg(out, 1)
}

fn result_arg(out: &ReturnType, idx: usize) -> syn::Result<syn::Type> {
    let ReturnType::Type(_, ty) = out else {
        return Err(Error::new_spanned(
            out,
            "a #[route] handler must return Result<T, E> — a bare value has no error type \
             for the generated client to model; write `-> Result<YourResponseType, ApiError>`",
        ));
    };
    let syn::Type::Path(p) = &**ty else {
        return Err(Error::new_spanned(
            ty,
            "a #[route] handler must return Result<T, E> — this return type isn't one; write \
             `-> Result<YourResponseType, ApiError>`",
        ));
    };
    let seg = p.path.segments.last().ok_or_else(|| {
        Error::new_spanned(
            ty,
            "a #[route] handler must return Result<T, E> — write \
             `-> Result<YourResponseType, ApiError>`",
        )
    })?;
    if seg.ident != "Result" {
        return Err(Error::new_spanned(
            ty,
            format!(
                "a #[route] handler must return Result<T, E> — got `{}`; the client needs both \
                 the success and error types to generate a typed result, so write \
                 `-> Result<YourResponseType, ApiError>`",
                seg.ident
            ),
        ));
    }
    let PathArguments::AngleBracketed(args) = &seg.arguments else {
        return Err(Error::new_spanned(
            ty,
            "a #[route] handler must return Result<T, E> — write \
             `-> Result<YourResponseType, ApiError>` with both type arguments explicit",
        ));
    };
    match args
        .args
        .iter()
        .filter_map(|a| match a {
            GenericArgument::Type(t) => Some(t.clone()),
            _ => None,
        })
        .nth(idx)
    {
        Some(t) => Ok(t),
        None => Err(Error::new_spanned(
            ty,
            "a #[route] handler's Result must name both type arguments explicitly — a type \
             alias that hides the error type leaves the client with nothing to model; write \
             `-> Result<YourResponseType, ApiError>`",
        )),
    }
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

    // A stream route may take the capability token too, in the same slot
    // the plain flavor puts it: after the :params, before `ctx`.
    let has_cap =
        rest.len() == params.len() + 3 && arg_name(rest[params.len()]).as_deref() == Some("cap");
    if has_cap && !args.cross_tenant {
        return Err(undeclared_capability(&name_str, &args, rest[params.len()]));
    }
    if rest.len() != params.len() + 2 + usize::from(has_cap) {
        return Err(Error::new(
            func.sig.span(),
            format!(
                "an SSE route handler is `fn {name_str}(tx: &mut Tx, {}ctx: &Ctx, \
                 cursor: Option<Cursor>) -> Result<Subscription, ApiError>` — one argument \
                 per :param in \"{pattern}\" (path order), then `ctx`, then `cursor` \
                 (with `cap: &CrossTenant` between the :params and `ctx` on a `cross_tenant` \
                 route)",
                params
                    .iter()
                    .map(|p| format!("{p}: _, "))
                    .collect::<String>()
            ),
        ));
    }
    let tail = params.len() + usize::from(has_cap);
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
    let ctx_arg = rest[tail];
    if arg_name(ctx_arg).as_deref() != Some("ctx") {
        return Err(Error::new(
            ctx_arg.span(),
            format!(
                "an SSE route handler's next-to-last argument must be named `ctx: &Ctx` — got `{}`",
                arg_name(ctx_arg).unwrap_or_else(|| "<pattern>".to_string())
            ),
        ));
    }
    let cursor_arg = rest[tail + 1];
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
    if has_cap {
        call_args.push(quote!(cap));
    }
    call_args.push(quote!(ctx));
    call_args.push(quote!(cursor));

    // See `expand_http`'s `cap_let`.
    let cap_let = if has_cap {
        quote! {
            let cap = ctx.cap().expect(
                "route declares cross_tenant but the kernel minted no CrossTenant — \
                 the flag on Route and the token in Ctx come from the same expansion"
            );
        }
    } else {
        quote!()
    };
    let auth = args.auth;
    let public_scope = public_scope_tokens(&args);
    let cross_tenant = args.cross_tenant;
    let pattern_lit = &args.path;

    let err_ty = result_err_type(&func.sig.output)?;
    let param_schema_entries: Vec<TokenStream> = params
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let ty = &rest[i].ty;
            quote! { (#p, (|| <#ty as ::charpente::wire::Wire>::reference()) as fn() -> _) }
        })
        .collect();
    let param_registers: Vec<TokenStream> = params
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let ty = &rest[i].ty;
            quote! { <#ty as ::charpente::wire::Wire>::register(reg); }
        })
        .collect();
    // `<Ev as Event>::NAME`, NOT a snake_case recomputed here (task 7,
    // amendment B1): `#[event]` already computed that name once, and a
    // second copy of the rule would be a drift source living inside the very
    // thing that detects drift — invisible to it whenever both copies were
    // wrong the same way. Naming the const also means `emits(NotAnEvent)`
    // fails with an unsatisfied `Event` bound naming the type, instead of
    // baking a wrong string into the contract.
    //
    // `<Ev as Wire>::WIRE_NAME` rides along for the same reason (ruling 46):
    // the SSE `event:` field carries the first name and a generated
    // TypeScript union names the second, and NEITHER is recoverable from the
    // other (`#[event]`'s acronym rule maps `HTTPServer` to `http_server`).
    // Both are one token apart here, where both are already in hand; a
    // downstream conversion would be the second copy of the rule this
    // comment refuses.
    let emit_names: Vec<TokenStream> = args
        .emits
        .iter()
        .map(|i| {
            quote! {
                ::charpente::http::EmittedEvent {
                    event: <#i as ::charpente::core::db::Event>::NAME,
                    payload: <#i as ::charpente::wire::Wire>::WIRE_NAME,
                }
            }
        })
        .collect();
    // The declared payloads join the route's own type closure. `describe()`
    // also registers every MOUNTED event's payload, so a declared-and-
    // mounted event is registered twice — that is the documented dedup path
    // (`TypeRegistry::insert_as` returns quietly for an identical schema
    // under a name already present), not a collision. Registering here is
    // what makes an emitted payload resolvable from the contract even for an
    // app that never calls `mount_events`.
    let emit_registers: Vec<TokenStream> = args
        .emits
        .iter()
        .map(|i| quote! { <#i as ::charpente::wire::Wire>::register(reg); })
        .collect();

    Ok(quote! {
        #func

        #[doc(hidden)]
        #[allow(non_snake_case)]
        fn #glue_name(
            tx: &mut ::charpente::Tx,
            ctx: &::charpente::http::Ctx<'_>,
            captures: &[::std::string::String],
            cursor: ::std::option::Option<::charpente::http::Cursor>,
        ) -> ::std::result::Result<::charpente::http::Subscription, ::charpente::http::ApiError> {
            let _ = captures; // SSE routes without :params use none of them
            #(#param_lets)*
            #cap_let
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
                    public_scope: #public_scope,
                    cross_tenant: #cross_tenant,
                    name: #name_str,
                    glue: ::charpente::http::RouteGlue::Sse(#glue_name),
                    params: &[#(#param_schema_entries),*],
                    request: ::std::option::Option::None,
                    // Placeholder, NOT `Struct { fields: vec![] }` — that
                    // would claim the stream emits an empty JSON object,
                    // which is a lie the emitter would faithfully
                    // transcribe. It STAYS a placeholder now that task 7 has
                    // landed: the payload union lives in `emits` below (a
                    // list of event NAMES, each resolvable through
                    // `Contract.types`), and the emitter forms the
                    // discriminated union from that. Duplicating it into
                    // `response` as well would be a second place the same
                    // fact becomes true.
                    response: || ::charpente::wire::TypeSchema::Null,
                    error: || <#err_ty as ::charpente::wire::Wire>::reference(),
                    register_types: |reg| {
                        #(#param_registers)*
                        <#err_ty as ::charpente::wire::Wire>::register(reg);
                        #(#emit_registers)*
                    },
                    emits: &[#(#emit_names),*],
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
