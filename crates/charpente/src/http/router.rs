//! Route table and matching: exact segments plus `:param` captures, a
//! literal beating a `:param` at the same position, and mount-time conflict
//! detection — misrouting is a contract violation caught before traffic.

use std::any::TypeId;

use charpente_core::db::Tx;

use crate::http::app::Ctx;
use crate::http::{ApiError, Cursor, Method, Response, Subscription};

/// The glue signature a plain `#[route(METHOD …)]` generates: parse captures
/// and body, call the handler, encode the response. Runs inside the
/// request's transaction.
pub type Glue = fn(&mut Tx, &Ctx, &[String], &[u8]) -> Result<Response, ApiError>;

/// The glue signature `#[route(SSE …)]` generates: parse captures and the
/// platform-extracted cursor, call the handler, return the typed
/// subscription. Never a `Response` — an SSE route only makes sense
/// dispatched through the kernel (`App::handle_or_subscribe`'s
/// `Handled::Subscribe`), which has a sink to hand the tail.
pub type SubGlue = fn(&mut Tx, &Ctx, &[String], Option<Cursor>) -> Result<Subscription, ApiError>;

/// A path param's name paired with its fn-deferred wire schema (`Route::params`'s
/// element type — factored out so the field itself stays under clippy's
/// type-complexity threshold).
pub type ParamSchema = (&'static str, fn() -> crate::wire::TypeSchema);

/// A route's flavor-specific glue. The two flavors are dispatched
/// differently by `App::handle_or_subscribe` — that's the only reason this
/// wraps `Glue`/`SubGlue` instead of `Route` holding a bare `Glue`.
#[derive(Clone, Copy)]
pub enum RouteGlue {
    Http(Glue),
    Sse(SubGlue),
}

pub struct Route {
    pub method: Method,
    pub pattern: &'static str,
    pub auth: bool,
    /// `public(Org)`: `Some` exactly when the route declared a public tenant
    /// scope. Read by `stage_or_subscribe`, which mints the request's
    /// `TenantAdmission` for this route only, and listed by `describe`.
    pub public_scope: Option<PublicScope>,
    /// `#[route(…, cross_tenant)]` — the declared capability to enumerate a
    /// scoped table across every tenant (spec §3). Carried on the route so
    /// `charpente describe` can list every endpoint that can, and read by
    /// `StagedDispatch::stage_or_subscribe` to decide whether this request's
    /// `Ctx` gets a `CrossTenant` token at all.
    pub cross_tenant: bool,
    pub name: &'static str,
    pub glue: RouteGlue,
    /// Path params in path order, each with its wire schema. Deferred behind
    /// a fn pointer because `TypeSchema` is not const-constructible (it owns
    /// `Vec`s and `String`s) and `Route` is returned from a `const`-ish
    /// `RouteDef::route()`.
    pub params: &'static [ParamSchema],
    /// `None` for routes with no JSON body.
    pub request: Option<fn() -> crate::wire::TypeSchema>,
    pub response: fn() -> crate::wire::TypeSchema,
    pub error: fn() -> crate::wire::TypeSchema,
    /// Registers this route's whole type closure (params, request, response,
    /// error, and everything they transitively reference) into the contract
    /// registry, plus every event payload this route declares via
    /// `emits(...)` — a declared payload is registered even when the event
    /// is never mounted (task 7).
    pub register_types: fn(&mut crate::wire::TypeRegistry),
    /// Event types a stream route declares via `#[route(SSE …, emits(Ev, …))]`,
    /// in declaration order. Empty for non-stream routes, and for a stream
    /// route that has not adopted the declaration.
    pub emits: &'static [EmittedEvent],
}

/// One event a stream route declares with `emits(Ev, …)`, carrying BOTH names
/// the declaration produces — because the two consumers need different ones
/// and NEITHER can be computed from the other (ruling 46).
///
/// - `event` is `<Ev as Event>::NAME`, the snake_case name an SSE frame's
///   `event:` field carries and the one the drift oracle
///   (`App::subscription_event_names`) compares against.
/// - `payload` is `<Ev as Wire>::WIRE_NAME`, the payload type's key into
///   `Contract.types` and the name a generated TypeScript union writes.
///
/// Deriving one from the other is refused: `#[event]`'s acronym rule maps
/// `HTTPServer` to `http_server`, so snake_case -> PascalCase is not a
/// function, and a second case-conversion implementation would be a drift
/// source living inside the thing that detects drift. Both are read off the
/// type at expansion time instead, where both are already in hand.
///
/// A named two-field struct rather than a tuple so call sites read
/// `e.event`/`e.payload`; still const-constructible, which `Route` requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct EmittedEvent {
    pub event: &'static str,
    pub payload: &'static str,
}

/// `#[route(…, public(Org))]` — the tenant scope every caller of this route,
/// credentialed or not, may act inside (job-tenancy spec §4). `Scoped::scope`
/// admits a scope of exactly this type by the declaration, with no
/// `Tenant::authorize`; any other scope type the handler opens meets the
/// membership check as on any route.
#[derive(Clone, Copy, Debug)]
pub struct PublicScope {
    /// The scope type's `Tenant::NAME`, which `describe` lists and the 401
    /// renders (tenancy-completion spec §4).
    pub name: &'static str,
    /// The scope type's `TypeId`, behind a fn pointer like `response` and
    /// `error`, so a `Route` stays plain `'static` data and fn pointers.
    pub type_id: fn() -> TypeId,
}

/// Implemented by the marker struct `#[route]` emits next to each handler,
/// so `routes![a, b]` collects descriptors explicitly — distributed
/// registration (inventory/linkme) is rejected as a nondeterminism source.
pub trait RouteDef {
    fn route() -> Route;
}

#[macro_export]
macro_rules! routes {
    ($($handler:path),* $(,)?) => {
        ::std::vec![$(<$handler as $crate::http::RouteDef>::route()),*]
    };
}

enum Segment {
    Literal(&'static str),
    // The name is read only while validating the pattern (the assert in
    // `segments`); every match below discards it (`Param(_)`) since
    // captures are read positionally, not by name. `#[allow(dead_code)]` is
    // a mechanical fix for `-D warnings`, not a design change from the
    // brief.
    Param(#[allow(dead_code)] &'static str),
}

fn segments(pattern: &'static str) -> Vec<Segment> {
    let rest = pattern
        .strip_prefix('/')
        .unwrap_or_else(|| panic!("route pattern must start with '/' — got \"{pattern}\""));
    rest.split('/')
        .map(|s| match s.strip_prefix(':') {
            Some(name) => {
                assert!(!name.is_empty(), "empty :param name in \"{pattern}\"");
                Segment::Param(name)
            }
            None => Segment::Literal(s),
        })
        .collect()
}

pub enum RouteMatch<'r> {
    Found(&'r Route, Vec<String>),
    PathNotFound,
    MethodNotAllowed,
}

#[derive(Default)]
pub struct Router {
    routes: Vec<Route>,
}

impl Router {
    pub fn new() -> Router {
        Router::default()
    }

    /// Adds routes, panicking on a conflict (two routes whose method and
    /// pattern shape can both match one path).
    pub fn mount(&mut self, routes: Vec<Route>) {
        for route in routes {
            let _ = segments(route.pattern); // validates the pattern shape
            for existing in &self.routes {
                if existing.method == route.method && same_shape(existing.pattern, route.pattern) {
                    panic!(
                        "route conflict: {} {} (fn {}) collides with {} {} (fn {}) — \
                         two routes with the same method and shape can both match one \
                         path; change one pattern",
                        route.method.name(),
                        route.pattern,
                        route.name,
                        existing.method.name(),
                        existing.pattern,
                        existing.name,
                    );
                }
            }
            self.routes.push(route);
        }
    }

    /// Describe-only reach: every mounted route, in registration order.
    /// Never used by request matching itself (`find` walks `self.routes`
    /// directly).
    pub(crate) fn routes(&self) -> &[Route] {
        &self.routes
    }

    pub fn find(&self, method: Method, path: &str) -> RouteMatch<'_> {
        let Some(rest) = path.strip_prefix('/') else {
            return RouteMatch::PathNotFound;
        };
        let path_segments: Vec<&str> = rest.split('/').collect();
        let mut best: Option<(&Route, Vec<String>)> = None;
        let mut path_matched = false;
        for route in &self.routes {
            let Some(captures) = match_pattern(route.pattern, &path_segments) else {
                continue;
            };
            path_matched = true;
            if route.method != method {
                continue;
            }
            let better = match &best {
                None => true,
                Some((current, _)) => more_literal(route.pattern, current.pattern),
            };
            if better {
                best = Some((route, captures));
            }
        }
        match best {
            Some((route, captures)) => RouteMatch::Found(route, captures),
            None if path_matched => RouteMatch::MethodNotAllowed,
            None => RouteMatch::PathNotFound,
        }
    }
}

fn match_pattern(pattern: &'static str, path: &[&str]) -> Option<Vec<String>> {
    let pattern = segments(pattern);
    if pattern.len() != path.len() {
        return None;
    }
    let mut captures = Vec::new();
    for (seg, part) in pattern.iter().zip(path) {
        match seg {
            Segment::Literal(lit) => {
                if lit != part {
                    return None;
                }
            }
            Segment::Param(_) => captures.push((*part).to_string()),
        }
    }
    Some(captures)
}

/// Literal beats `:param` at the first differing position.
fn more_literal(a: &'static str, b: &'static str) -> bool {
    for (x, y) in segments(a).iter().zip(&segments(b)) {
        match (x, y) {
            (Segment::Literal(_), Segment::Param(_)) => return true,
            (Segment::Param(_), Segment::Literal(_)) => return false,
            _ => {}
        }
    }
    false
}

fn same_shape(a: &'static str, b: &'static str) -> bool {
    let (a, b) = (segments(a), segments(b));
    a.len() == b.len()
        && a.iter().zip(&b).all(|(x, y)| match (x, y) {
            (Segment::Literal(l), Segment::Literal(m)) => l == m,
            (Segment::Param(_), Segment::Param(_)) => true,
            _ => false,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(_: &mut Tx, _: &Ctx, _: &[String], _: &[u8]) -> Result<Response, ApiError> {
        Ok(Response::error(200, "ok"))
    }

    fn route(method: Method, pattern: &'static str, name: &'static str) -> Route {
        // Untyped glue, not a described route: `ok` returns a `Response`
        // directly with no typed handler, so there is no schema to give it
        // (Amendment D, task-5-brief.md).
        Route {
            method,
            pattern,
            auth: false,
            public_scope: None,
            cross_tenant: false,
            name,
            glue: RouteGlue::Http(ok),
            params: &[],
            request: None,
            response: || crate::wire::TypeSchema::Null,
            error: || <ApiError as crate::wire::Wire>::reference(),
            register_types: |_| {},
            emits: &[],
        }
    }

    fn router(routes: Vec<Route>) -> Router {
        let mut r = Router::new();
        r.mount(routes);
        r
    }

    #[test]
    fn matches_and_captures_in_pattern_order() {
        let r = router(vec![route(Method::Get, "/orgs/:org/notes/:id", "get_note")]);
        let RouteMatch::Found(found, captures) = r.find(Method::Get, "/orgs/7/notes/42") else {
            panic!("must match");
        };
        assert_eq!(found.name, "get_note");
        assert_eq!(captures, vec!["7".to_string(), "42".to_string()]);
    }

    #[test]
    fn unknown_path_and_wrong_method_are_distinct() {
        let r = router(vec![route(Method::Post, "/notes", "create")]);
        assert!(matches!(
            r.find(Method::Get, "/nope"),
            RouteMatch::PathNotFound
        ));
        assert!(matches!(
            r.find(Method::Get, "/notes"),
            RouteMatch::MethodNotAllowed
        ));
        assert!(matches!(
            r.find(Method::Post, "notes"),
            RouteMatch::PathNotFound
        ));
    }

    #[test]
    fn literal_beats_param_at_the_same_position() {
        let r = router(vec![
            route(Method::Get, "/notes/:id", "by_id"),
            route(Method::Get, "/notes/all", "all"),
        ]);
        let RouteMatch::Found(found, captures) = r.find(Method::Get, "/notes/all") else {
            panic!("must match");
        };
        assert_eq!(found.name, "all");
        assert!(captures.is_empty());
        let RouteMatch::Found(found, _) = r.find(Method::Get, "/notes/9") else {
            panic!("must match");
        };
        assert_eq!(found.name, "by_id");
    }

    #[test]
    #[should_panic(expected = "route conflict")]
    fn same_shape_same_method_conflicts_at_mount() {
        router(vec![
            route(Method::Get, "/notes/:id", "a"),
            route(Method::Get, "/notes/:key", "b"),
        ]);
    }

    #[test]
    #[should_panic(expected = "must start with '/'")]
    fn pattern_without_slash_is_a_contract_violation() {
        router(vec![route(Method::Get, "notes", "bad")]);
    }
}
