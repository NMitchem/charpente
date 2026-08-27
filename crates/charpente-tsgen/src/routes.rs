//! `Contract.routes` -> one method per non-stream route on `class Api`, plus
//! the pieces `events.rs` shares: the case helper, the path expression, and
//! the checks over the class's member namespace.
//!
//! Pure, like `types.rs`: every walk is over an ordered `Vec`, and the two
//! sets here are `BTree`s, so the output is a function of the contract alone
//! (spec §4). Route order is `describe()`'s MOUNT order and is deliberately
//! not sorted — an ordering guaranteed upstream is stronger than one a later
//! edit here can drop.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use charpente::describe::{Contract, RouteInfo};

use crate::runtime::APIBASE_MEMBERS;
use crate::types::ts_type;

/// The one line of generated doc that says a route needs a credential, shared
/// so the request and subscribe emitters cannot drift apart on its wording.
pub(crate) const AUTH_DOC: &str = "  /** Requires a session token: call `setToken` first. */\n";

/// snake_case -> camelCase (`capitalize_first: false`) or PascalCase (true).
///
/// The ONE case conversion in this crate (amendment D5), and the only
/// direction that is a function at all: `#[event]`'s acronym rule maps
/// `HTTPServer` to `http_server`, so nothing recovers a PascalCase name from
/// a snake_case one (ruling 46). Never write the inverse.
fn cased(snake: &str, capitalize_first: bool) -> String {
    let mut out = String::with_capacity(snake.len());
    let mut up = capitalize_first;
    for ch in snake.chars() {
        if ch == '_' {
            up = true;
        } else if up {
            out.push(ch.to_ascii_uppercase());
            up = false;
        } else {
            out.push(ch);
        }
    }
    out
}

pub(crate) fn camel(snake: &str) -> String {
    cased(snake, false)
}

pub(crate) fn pascal(snake: &str) -> String {
    cased(snake, true)
}

/// Every route contributes exactly one member to `class Api` — a request
/// method or a subscribe method — so both kinds share one namespace and are
/// checked together, before a byte is emitted. Both failures below render a
/// TypeScript file `tsc` rejects (or, worse, silently accepts with one route
/// gone) in an app author's build, pointing at generated code they did not
/// write; a named panic here is the better failure (hard rule 4).
pub(crate) fn check_member_names(c: &Contract) {
    let mut seen: BTreeMap<String, &'static str> = BTreeMap::new();
    for r in &c.routes {
        let m = camel(r.name);
        assert!(
            !m.is_empty(),
            "emit_client: route `{} {}` generates an empty client method name — its handler \
             name {:?} camel-cases to nothing; rename the Rust handler",
            r.method,
            r.pattern,
            r.name
        );
        assert!(
            !APIBASE_MEMBERS.contains(&m.as_str()),
            "emit_client: route `{}` (`{} {}`) generates a client method `{m}` that shadows a \
             member of the generated `ApiBase` ({}) — the generated class extends `ApiBase`, so \
             this route would replace the transport it is itself trying to call; rename the Rust \
             handler so its camelCase name differs",
            r.name,
            r.method,
            r.pattern,
            APIBASE_MEMBERS.join(", ")
        );
        if let Some(prev) = seen.insert(m.clone(), r.name) {
            panic!(
                "emit_client: two routes generate the same client method `{m}` — Rust handlers \
                 `{prev}` and `{}` (`{} {}`). TypeScript keeps only the last declaration of a \
                 class member, so one of the two routes would silently become unreachable from \
                 the client; rename one handler so their camelCase names differ",
                r.name, r.method, r.pattern
            );
        }
    }
}

/// The typed arguments a route's path params contribute, in path order.
/// `trailing` names the argument the caller appends after them (`body`,
/// `opts`) so a path param cannot collide with it; pass `""` when there is
/// none.
pub(crate) fn param_args(r: &RouteInfo, trailing: &str) -> Vec<String> {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut args = Vec::new();
    for p in &r.params {
        let n = camel(p.name);
        assert!(
            n != trailing,
            "emit_client: route `{}` (`{} {}`) has a path param `:{}` whose client argument is \
             `{n}`, which collides with the `{trailing}` argument the generated method already \
             takes; rename the param in the route pattern and handler",
            r.name,
            r.method,
            r.pattern,
            p.name
        );
        assert!(
            seen.insert(n.clone()),
            "emit_client: route `{}` (`{} {}`) has two path params whose client arguments are \
             both `{n}` (`:{}` is one of them) — rename one in the route pattern and handler so \
             their camelCase names differ",
            r.name,
            r.method,
            r.pattern,
            p.name
        );
        let ctx = format!("path param `:{}` of route `{}`", p.name, r.name);
        args.push(format!("{n}: {}", ts_type(&p.schema, &ctx)));
    }
    args
}

/// The request path as a TypeScript expression: a plain string literal when
/// the pattern names no params, a template literal substituting each `:name`
/// otherwise.
///
/// Patterns are colon-prefixed and charpente has no brace syntax at all —
/// both `#[route]` (`charpente-macros/src/route.rs`) and the router
/// (`charpente/src/http/router.rs`) parse a segment with `strip_prefix(':')`
/// (amendment D2).
pub(crate) fn path_expr(r: &RouteInfo) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut substituted = 0usize;
    for seg in r.pattern.split('/') {
        match seg.strip_prefix(':') {
            Some(name) => {
                let p = r.params.iter().find(|p| p.name == name).unwrap_or_else(|| {
                    panic!(
                        "emit_client: route `{}` (`{} {}`) declares a `:param` the contract has \
                         no ParamInfo for: `:{name}`. The pattern and the handler's argument \
                         list disagree, so nothing could be substituted and the client would \
                         request the literal text `:{name}`; add the param to the handler \
                         signature, or drop `:{name}` from the pattern",
                        r.name, r.method, r.pattern
                    )
                });
                substituted += 1;
                parts.push(format!("${{encodeURIComponent({})}}", camel(p.name)));
            }
            None => parts.push(seg.to_string()),
        }
    }
    assert!(
        substituted == r.params.len(),
        "emit_client: route `{}` (`{} {}`) carries {} ParamInfo entries but its pattern names \
         {substituted} `:param`s — a param the pattern never mentions has no place to be \
         substituted, so the generated method would take an argument it silently drops",
        r.name,
        r.method,
        r.pattern,
        r.params.len()
    );
    if substituted == 0 {
        return format!("{:?}", r.pattern);
    }
    assert!(
        !r.pattern.contains('`') && !r.pattern.contains('\\') && !r.pattern.contains("${"),
        "emit_client: route `{}`'s pattern {:?} contains text a TypeScript template literal \
         would interpret (a backtick, a backslash, or `${{`), and the substituted path must be \
         a template literal; remove it from the route pattern",
        r.name,
        r.pattern
    );
    format!("`{}`", parts.join("/"))
}

/// One `async` method per non-stream route, in mount order.
pub(crate) fn emit_methods(c: &Contract) -> String {
    let types = crate::revive::type_map(c);
    let mut out = String::new();
    for r in c.routes.iter().filter(|r| !r.stream) {
        let ok = ts_type(&r.response, &format!("response of route `{}`", r.name));
        let err = ts_type(&r.error, &format!("error of route `{}`", r.name));
        let mut args = param_args(r, if r.request.is_some() { "body" } else { "" });
        if let Some(req) = &r.request {
            let ctx = format!("request body of route `{}`", r.name);
            args.push(format!("body: {}", ts_type(req, &ctx)));
        }
        // A route with no request type must not pass one: `ApiBase.request`
        // keys `content-type: application/json` off `body !== undefined`, so
        // forwarding an absent body would announce a body it does not send.
        let body = if r.request.is_some() { ", body" } else { "" };
        if r.auth {
            out.push_str(AUTH_DOC);
        }
        let _ = writeln!(
            out,
            "  async {}({}): Promise<Result<{ok}, {err}>> {{",
            camel(r.name),
            args.join(", ")
        );
        // Chunk B1b: a route whose response or error transitively contains
        // an `Int64` (chunk-b1a-brief.md's decision, restated in
        // chunk-b1b-brief.md A2) gets its result revived HERE, in the
        // generated method — `request()` itself keeps its plain
        // `parsed as T` / `parsed as E` verbatim, unchanged since B1a. The
        // common case (no `Int64` anywhere in either half) stays the exact
        // one-line `return this.request(...)` it always was (A5).
        let ok_expr = crate::revive::revive_expr(&r.response, "r.data", &types, 0);
        let err_expr = crate::revive::revive_expr(&r.error, "r.error", &types, 0);
        if ok_expr.is_none() && err_expr.is_none() {
            let _ = writeln!(
                out,
                "    return this.request<{ok}, {err}>({:?}, {}{body});",
                r.method,
                path_expr(r)
            );
        } else {
            let _ = writeln!(
                out,
                "    const r = await this.request<{ok}, {err}>({:?}, {}{body});",
                r.method,
                path_expr(r)
            );
            let data = ok_expr.unwrap_or_else(|| "r.data".to_string());
            let error = err_expr.unwrap_or_else(|| "r.error".to_string());
            let _ = writeln!(
                out,
                "    return r.ok ? {{ ok: true, data: {data} }} : {{ ok: false, error: {error} }};"
            );
        }
        out.push_str("  }\n\n");
    }
    out
}
