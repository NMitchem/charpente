//! Route table and matching: exact segments plus `:param` captures, a
//! literal beating a `:param` at the same position, and mount-time conflict
//! detection — misrouting is a contract violation caught before traffic.

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
    pub name: &'static str,
    pub glue: RouteGlue,
}

/// Implemented by the marker struct `#[route]` emits next to each handler,
/// so `routes![a, b]` collects descriptors explicitly — distributed
/// registration (inventory/linkme) is rejected as a nondeterminism source.
pub trait RouteDef {
    fn route() -> Route;
}

#[macro_export]
macro_rules! routes {
    ($($handler:ident),* $(,)?) => {
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
        Route {
            method,
            pattern,
            auth: false,
            name,
            glue: RouteGlue::Http(ok),
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
