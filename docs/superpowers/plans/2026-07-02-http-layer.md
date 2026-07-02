# M0 Step 5 — HTTP Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A deterministic HTTP kernel (`charpente::http` — typed Request/Response, router, `#[route]` glue, sessions, transact-wrapping) sim-tested by driving `App::handle` directly, plus a hyper/tokio edge adapter (`charpente::serve`) that only translates wire↔struct.

**Architecture:** Everything charpente owns about a request — routing, param/body decode, auth, the one transaction per request, error mapping — runs inside the simulation boundary as plain sync code. hyper/tokio live in one module-`allow`ed edge (like `prod.rs`) feeding a single-threaded kernel loop over an mpsc channel. A 200 is produced only after `Db::transact` returns Ok — ack-after-durable at the HTTP level.

**Tech Stack:** Rust; new deps in the `charpente` crate only: serde/serde_json (JSON surface), hyper 1.x + hyper-util + http-body-util + tokio (`rt`, `net`, `sync`) at the edge. `charpente-core` gains nothing.

**Spec:** `docs/superpowers/specs/2026-07-02-http-layer-design.md` (approved).

## Global Constraints

- **No ambient nondeterminism inside the boundary**: `charpente::http` is deny-list-fenced; the ONLY new ambient-IO module is `crates/charpente/src/serve.rs` with a module-wide `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]` and a rationale comment, exactly like `prod.rs`. tokio deny-list entries land in the same commit as the tokio dependency (CLAUDE.md hard rule 1).
- **Error messages are product surface** (hard rule 5): 400 names the parameter/field and expected type; 401 says how to authenticate; 413 names the cap and the fix; route-conflict and macro errors name both sides and the fix. Tests assert on message text.
- **Sim-first** (hard rule 3): a found bug's seed gets pinned; property failures are findings — stop and report, never weaken an assertion to get green.
- **Ack-after-durable**: `App::handle` returns 200 only after `transact` returns `Ok` (post-WAL-sync).
- Session cookie name is exactly `charpente_session`; `Authorization: Bearer <t>` takes precedence over the cookie.
- `AppConfig::default()`: `body_cap = 1 << 20` (1 MiB), `db = DbConfig::default()`.
- Known pre-existing flake (tracked separately): `tables_and_log_survive_the_fault_matrix` can panic inside redb ~1-in-thousands universes (seed 0x1c0189950421c6ac). Only that exact panic in that test may be re-run once and noted.
- Property workloads treat error messages containing `"simulated"`/`"injected"` as injected faults (step-4 convention); any other IO-flavored failure is a real finding.
- Commit directly to `main`, no Co-Authored-By trailers, targeted `git add` only (never `-A`; stray `.DS_Store` files exist).
- Evidence bar: property tests green over fresh seeds every run; universe counts in commit messages.
- Run all commands from `/Users/noahmitchem/Documents/GitHub/charpente`.

## File structure locked in by this plan

```
crates/charpente/src/http/mod.rs      — Method, Request, Response, ApiError, FromParam,
                                        decode_body, hex; re-exports app/router items
crates/charpente/src/http/router.rs   — Glue, Route, RouteDef, routes!, Router, RouteMatch
crates/charpente/src/http/app.rs      — Session (#[table]), Ctx, AppConfig, App
crates/charpente/src/serve.rs         — hyper/tokio edge + loopback smoke test
crates/charpente-macros/src/lib.rs    — thin entry points; implementation split into…
crates/charpente-macros/src/table.rs  — existing #[table] expansion (moved verbatim)
crates/charpente-macros/src/route.rs  — new #[route] expansion
crates/charpente/tests/http.rs        — sim tests: kernel unit tests, macro end-to-end,
                                        request-level crash property, determinism
```

Note a structural constraint that shapes the tasks: `#[route]`-generated code references `::charpente::…` paths, so the macro is usable by crates that depend on `charpente` (including `crates/charpente/tests/*`, which link the crate by name) but NOT by `charpente`'s own `src/`. Internal code (the smoke test's route) builds `Route` values by hand.

---

### Task 1: Dependencies + deny-list entries (same commit)

**Files:**
- Modify: `crates/charpente/Cargo.toml` (add dependencies)
- Modify: `clippy.toml` (tokio entries)

**Interfaces:**
- Produces: `serde` (derive), `serde_json`, `hyper`, `hyper-util`, `http-body-util`, `tokio` available to the `charpente` crate and its integration tests.

- [ ] **Step 1: Add the dependencies** — in `crates/charpente/Cargo.toml`, extend `[dependencies]`:

```toml
serde = { version = "1", features = ["derive"] }
serde_json = "1"
hyper = { version = "1", features = ["http1", "server"] }
hyper-util = { version = "0.1", features = ["tokio"] }
http-body-util = "0.1"
tokio = { version = "1", features = ["rt", "net", "sync"] }
```

- [ ] **Step 2: Add the deny-list entries** — in `clippy.toml`, append to `disallowed-methods`:

```toml
    { path = "tokio::spawn", reason = "ambient task spawning — edge modules only; core scheduling goes through the deterministic executor (DESIGN.md §1)" },
    { path = "tokio::task::spawn_blocking", reason = "ambient task spawning — edge modules only (DESIGN.md §1)" },
    { path = "tokio::time::sleep", reason = "ambient time — use Clock::sleep (DESIGN.md §1)" },
    { path = "tokio::net::TcpListener::bind", reason = "ambient network — use the Net trait (DESIGN.md §1)" },
    { path = "tokio::net::TcpStream::connect", reason = "ambient network — use the Net trait (DESIGN.md §1)" },
```

and to `disallowed-types`:

```toml
    { path = "tokio::net::TcpListener", reason = "ambient network — Net trait, or edge modules only (DESIGN.md §1)" },
    { path = "tokio::net::TcpStream", reason = "ambient network — Net trait, or edge modules only (DESIGN.md §1)" },
    { path = "tokio::time::Instant", reason = "ambient monotonic clock — use the Clock trait (DESIGN.md §1)" },
```

- [ ] **Step 3: Verify**

Run: `cargo build --workspace && cargo clippy --workspace --all-targets && cargo test --workspace`
Expected: builds (fetches the new crates), clippy clean, suite green.

- [ ] **Step 4: Commit**

```bash
git add crates/charpente/Cargo.toml Cargo.lock clippy.toml
git commit -m "charpente: HTTP-edge dependencies (serde, hyper, tokio) + tokio deny-list entries

Deps live in the platform crate only; charpente-core gains nothing. tokio's
ambient APIs are deny-listed in the same commit per hard rule 1."
```

---

### Task 2: `http` module core types (pure, no IO)

**Files:**
- Create: `crates/charpente/src/http/mod.rs`
- Modify: `crates/charpente/src/lib.rs` (add `pub mod http;`)

**Interfaces:**
- Produces (used by Tasks 3–6):
  - `enum Method { Get, Post, Put, Patch, Delete }` with `Method::parse(&str) -> Option<Method>` and `Method::name(self) -> &'static str`
  - `struct Request { method, path: String, headers: BTreeMap<String, String>, body: Vec<u8> }` + builder helpers `Request::new(method, path)`, `.header(k, v)`, `.json(&value)`
  - `struct Response { status: u16, headers: BTreeMap<String, String>, body: Vec<u8> }` + `Response::json(status, &value)`, `Response::error(status, msg)`
  - `enum ApiError { BadRequest(String), Unauthorized, NotFound, Internal(String) }`, `From<io::Error>`, `fn response(&self) -> Response`
  - `trait FromParam` (impls: `u64`, `String`, `Id<T>`), `fn decode_body<T: DeserializeOwned>(&[u8]) -> Result<T, ApiError>`, `pub(crate) fn hex(&[u8]) -> String`

The `mod app; mod router;` declarations and their re-exports arrive in Task 3 — this task's `mod.rs` ends at the decoding section.

- [ ] **Step 1: Create `crates/charpente/src/http/mod.rs`:**

```rust
//! The deterministic HTTP kernel (DESIGN.md §3; spec
//! docs/superpowers/specs/2026-07-02-http-layer-design.md): typed requests,
//! routing, decoding, sessions, and transact-wrapping — all inside the
//! simulation boundary. The wire lives in `crate::serve` (hyper); this
//! module never touches a socket. `App::handle(Request) -> Response` is the
//! whole surface, which is exactly what the sim drives.

use std::collections::BTreeMap;
use std::io;

use charpente_core::table::Id;

// ---------------------------------------------------------------- method

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Method {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl Method {
    pub fn parse(raw: &str) -> Option<Method> {
        Some(match raw {
            "GET" => Method::Get,
            "POST" => Method::Post,
            "PUT" => Method::Put,
            "PATCH" => Method::Patch,
            "DELETE" => Method::Delete,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Put => "PUT",
            Method::Patch => "PATCH",
            Method::Delete => "DELETE",
        }
    }
}

// ---------------------------------------------------------------- request

/// A request as the kernel sees it: the edge (or the sim) has already dealt
/// with the wire. Header keys are lowercase; the query string is stripped at
/// the edge and ignored in v0.
pub struct Request {
    pub method: Method,
    pub path: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn new(method: Method, path: impl Into<String>) -> Request {
        Request {
            method,
            path: path.into(),
            headers: BTreeMap::new(),
            body: Vec::new(),
        }
    }

    pub fn header(mut self, key: &str, value: impl Into<String>) -> Request {
        self.headers.insert(key.to_ascii_lowercase(), value.into());
        self
    }

    pub fn json(mut self, value: &impl serde::Serialize) -> Request {
        self.body = serde_json::to_vec(value).expect("request fixture serializes");
        self
    }
}

// ---------------------------------------------------------------- response

pub struct Response {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn json(status: u16, value: &impl serde::Serialize) -> Response {
        let body = serde_json::to_vec(value).expect("response type serializes");
        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_string(), "application/json".to_string());
        Response {
            status,
            headers,
            body,
        }
    }

    /// An error response: `{"error": "<message>"}`. The message is product
    /// surface (hard rule 5) — it names the contract and the fix.
    pub fn error(status: u16, message: &str) -> Response {
        #[derive(serde::Serialize)]
        struct Body<'a> {
            error: &'a str,
        }
        Response::json(status, &Body { error: message })
    }
}

// ---------------------------------------------------------------- errors

/// What a handler can reject with. `Io` failures convert via `From`, so `?`
/// works on tx reads inside handlers.
#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    Unauthorized,
    NotFound,
    Internal(String),
}

impl From<io::Error> for ApiError {
    fn from(e: io::Error) -> ApiError {
        ApiError::Internal(e.to_string())
    }
}

impl ApiError {
    pub fn response(&self) -> Response {
        match self {
            ApiError::BadRequest(msg) => Response::error(400, msg),
            ApiError::Unauthorized => Response::error(
                401,
                "no valid session — send `Authorization: Bearer <token>` or a \
                 `charpente_session` cookie",
            ),
            ApiError::NotFound => Response::error(404, "not found"),
            ApiError::Internal(msg) => Response::error(500, msg),
        }
    }
}

// ---------------------------------------------------------------- decoding

/// Typed path-parameter parsing. Errors name the parameter and the expected
/// shape — the 400 body is product surface.
pub trait FromParam: Sized {
    fn from_param(name: &str, raw: &str) -> Result<Self, ApiError>;
}

impl FromParam for u64 {
    fn from_param(name: &str, raw: &str) -> Result<u64, ApiError> {
        raw.parse().map_err(|_| {
            ApiError::BadRequest(format!(
                "path parameter :{name} must be an unsigned integer — got \"{raw}\""
            ))
        })
    }
}

impl FromParam for String {
    fn from_param(_name: &str, raw: &str) -> Result<String, ApiError> {
        Ok(raw.to_string())
    }
}

impl<T> FromParam for Id<T> {
    fn from_param(name: &str, raw: &str) -> Result<Id<T>, ApiError> {
        Ok(Id::from_raw(u64::from_param(name, raw)?))
    }
}

/// JSON body decoding for the glue `#[route]` generates.
pub fn decode_body<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, ApiError> {
    let type_name = std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("the body type");
    if body.is_empty() {
        return Err(ApiError::BadRequest(format!(
            "this route requires a JSON body of type {type_name}"
        )));
    }
    serde_json::from_slice(body).map_err(|e| {
        ApiError::BadRequest(format!("invalid JSON body for {type_name}: {e}"))
    })
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            write!(s, "{b:02x}").expect("writing to a String cannot fail");
            s
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_parse_and_name_roundtrip() {
        for m in [Method::Get, Method::Post, Method::Put, Method::Patch, Method::Delete] {
            assert_eq!(Method::parse(m.name()), Some(m));
        }
        assert_eq!(Method::parse("BREW"), None);
    }

    #[test]
    fn error_responses_carry_the_message() {
        let resp = ApiError::BadRequest("field `x` is required".into()).response();
        assert_eq!(resp.status, 400);
        let text = String::from_utf8(resp.body).unwrap();
        assert!(text.contains("field `x` is required"), "{text}");
        assert_eq!(
            resp.headers.get("content-type").map(String::as_str),
            Some("application/json")
        );
    }

    #[test]
    fn param_errors_name_param_and_type() {
        let err = u64::from_param("id", "abc").unwrap_err();
        let ApiError::BadRequest(msg) = err else {
            panic!("wrong variant")
        };
        assert!(msg.contains(":id") && msg.contains("unsigned integer") && msg.contains("abc"), "{msg}");
    }

    #[test]
    fn body_errors_name_the_type() {
        #[derive(serde::Deserialize)]
        struct CreateNote {
            #[allow(dead_code)]
            text: String,
        }
        let ApiError::BadRequest(empty) = decode_body::<CreateNote>(b"").unwrap_err() else {
            panic!("wrong variant")
        };
        assert!(empty.contains("CreateNote"), "{empty}");
        let ApiError::BadRequest(bad) = decode_body::<CreateNote>(b"{}").unwrap_err() else {
            panic!("wrong variant")
        };
        assert!(bad.contains("CreateNote") && bad.contains("text"), "{bad}");
    }

    #[test]
    fn hex_encodes_lowercase() {
        assert_eq!(hex(&[0x00, 0xff, 0x0a]), "00ff0a");
    }
}
```

- [ ] **Step 2: Register the module** — in `crates/charpente/src/lib.rs`, add `pub mod http;` after the re-exports (keep `pub mod prod;`).

- [ ] **Step 3: Run**

Run: `cargo test -p charpente http:: && cargo clippy --workspace --all-targets`
Expected: 5 tests pass, clippy clean.

- [ ] **Step 4: Commit**

```bash
git add crates/charpente/src/http/mod.rs crates/charpente/src/lib.rs
git commit -m "charpente: http kernel types — Method/Request/Response/ApiError/FromParam/decode_body

Pure, deterministic, no IO. Error bodies name the parameter/field/type per
hard rule 5, asserted in tests."
```

---

### Task 3: Router + Session/Ctx/App — the kernel

**Files:**
- Create: `crates/charpente/src/http/router.rs`
- Create: `crates/charpente/src/http/app.rs`
- Modify: `crates/charpente/src/http/mod.rs` (module decls + re-exports)
- Create: `crates/charpente/tests/http.rs` (kernel tests over the sim)

**Interfaces:**
- Consumes: Task 2's types; `Db`/`Schema`/`Tx`/`TxError` from step 4; `Session::by_token` accessor generated by `#[table]`; `Sim`/`FaultConfig` from charpente-sim.
- Produces:
  - `pub type Glue = fn(&mut Tx, &Ctx, &[String], &[u8]) -> Result<Response, ApiError>;`
  - `pub struct Route { pub method: Method, pub pattern: &'static str, pub auth: bool, pub name: &'static str, pub glue: Glue }`
  - `pub trait RouteDef { fn route() -> Route; }` and crate-root `routes![a, b]` macro
  - `pub struct Router` with `mount(&mut self, Vec<Route>)` (conflict panic) and `find(&self, Method, &str) -> RouteMatch<'_>` (`Found(&Route, Vec<String>) | PathNotFound | MethodNotAllowed`)
  - `#[table] pub struct Session { pub id: Id<Session>, pub token: String (#[index]), pub subject: String }`
  - `pub struct Ctx` with `session() -> Option<&Session>`, `create_session(&self, &mut Tx, subject: impl Into<String>) -> String` (panics if called twice), `revoke(&self, &mut Tx)`
  - `pub struct AppConfig { pub body_cap: usize, pub db: DbConfig }` (`Default`: 1 MiB, `DbConfig::default()`)
  - `pub struct App` with `open(disk: Box<dyn Disk>, schema: Schema, entropy: Box<dyn Entropy>, config: AppConfig) -> io::Result<App>` (registers `Session` in the schema itself), `mount(self, Vec<Route>) -> App`, `handle(&mut self, Request) -> Response`, `db(&self) -> &Db`, `config(&self) -> &AppConfig`

- [ ] **Step 1: Create `crates/charpente/src/http/router.rs`:**

```rust
//! Route table and matching: exact segments plus `:param` captures, a
//! literal beating a `:param` at the same position, and mount-time conflict
//! detection — misrouting is a contract violation caught before traffic.

use charpente_core::db::Tx;

use crate::http::app::Ctx;
use crate::http::{ApiError, Method, Response};

/// The glue signature `#[route]` generates: parse captures and body, call
/// the handler, encode the response. Runs inside the request's transaction.
pub type Glue = fn(&mut Tx, &Ctx, &[String], &[u8]) -> Result<Response, ApiError>;

pub struct Route {
    pub method: Method,
    pub pattern: &'static str,
    pub auth: bool,
    pub name: &'static str,
    pub glue: Glue,
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
    Param(&'static str),
}

fn segments(pattern: &'static str) -> Vec<Segment> {
    let rest = pattern.strip_prefix('/').unwrap_or_else(|| {
        panic!("route pattern must start with '/' — got \"{pattern}\"")
    });
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
                if existing.method == route.method
                    && same_shape(existing.pattern, route.pattern)
                {
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
            glue: ok,
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
        assert!(matches!(r.find(Method::Get, "/nope"), RouteMatch::PathNotFound));
        assert!(matches!(r.find(Method::Get, "/notes"), RouteMatch::MethodNotAllowed));
        assert!(matches!(r.find(Method::Post, "notes"), RouteMatch::PathNotFound));
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
```

- [ ] **Step 2: Create `crates/charpente/src/http/app.rs`:**

```rust
//! The app kernel: sessions inside the commit point (DESIGN §8), and `App` —
//! the owner of the `Db` that turns each request into one transaction.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::io;

use charpente_core::db::{Db, DbConfig, Schema, Tx, TxError};
use charpente_core::io::{Disk, Entropy};
use charpente_core::table::{Id, View};
use charpente_macros::table;

use crate::http::router::{RouteMatch, Router};
use crate::http::{hex, ApiError, Request, Response, Route};

// ---------------------------------------------------------------- session

/// Minimal auth: a session row inside the commit point. Tokens are stored
/// raw in M0 — M1 ("auth, properly") hashes them and adds expiry.
#[table]
pub struct Session {
    pub id: Id<Session>,
    #[index]
    pub token: String,
    pub subject: String,
}

// ---------------------------------------------------------------- ctx

/// Per-request context handed to handlers: the authenticated session (if
/// any) and one fresh entropy-derived token for session creation.
pub struct Ctx {
    session: Option<Session>,
    fresh_token: String,
    created: Cell<bool>,
}

impl Ctx {
    pub fn session(&self) -> Option<&Session> {
        self.session.as_ref()
    }

    /// Creates a session for `subject` and returns its token (login). One
    /// per request — the fresh token is drawn before the transaction, so a
    /// second call could only reuse it.
    pub fn create_session(&self, tx: &mut Tx, subject: impl Into<String>) -> String {
        assert!(
            !self.created.replace(true),
            "Ctx::create_session called twice in one request — one session per request"
        );
        let id = u64::from_str_radix(&self.fresh_token[..16], 16).expect("token is hex");
        tx.put(&Session {
            id: Id::from_raw(id),
            token: self.fresh_token.clone(),
            subject: subject.into(),
        });
        self.fresh_token.clone()
    }

    /// Deletes the current session (logout). A no-op without one.
    pub fn revoke(&self, tx: &mut Tx) {
        if let Some(session) = &self.session {
            tx.delete::<Session>(session.id);
        }
    }
}

// ---------------------------------------------------------------- app

pub struct AppConfig {
    /// Bodies over the cap get 413 before any decoding — this also fences
    /// `Db::transact`'s MAX_RECORD_BYTES assert from network input.
    pub body_cap: usize,
    pub db: DbConfig,
}

impl Default for AppConfig {
    fn default() -> AppConfig {
        AppConfig {
            body_cap: 1 << 20,
            db: DbConfig::default(),
        }
    }
}

/// The deterministic application kernel. Owns the `Db`; `handle` takes
/// `&mut self`, so requests are serializable by construction. The sim drives
/// `handle` directly; `crate::serve` feeds it from hyper in production.
pub struct App {
    db: Db,
    entropy: Box<dyn Entropy>,
    config: AppConfig,
    router: Router,
}

impl App {
    /// Opens the Db with the app's schema plus the platform's `Session`
    /// table — registered here so recovery can always replay logins.
    pub fn open(
        disk: Box<dyn Disk>,
        schema: Schema,
        entropy: Box<dyn Entropy>,
        config: AppConfig,
    ) -> io::Result<App> {
        let db = Db::open(disk, config.db.clone(), schema.table::<Session>())?;
        Ok(App {
            db,
            entropy,
            config,
            router: Router::new(),
        })
    }

    pub fn mount(mut self, routes: Vec<Route>) -> App {
        self.router.mount(routes);
        self
    }

    /// Snapshot/checkpoint access for the kernel loop and tests — handlers
    /// never see it.
    pub fn db(&self) -> &Db {
        &self.db
    }

    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    pub fn handle(&mut self, request: Request) -> Response {
        let (route, captures) = match self.router.find(request.method, &request.path) {
            RouteMatch::Found(route, captures) => (route, captures),
            RouteMatch::PathNotFound => return Response::error(404, "not found"),
            RouteMatch::MethodNotAllowed => {
                return Response::error(405, "method not allowed for this path")
            }
        };
        if request.body.len() > self.config.body_cap {
            return Response::error(
                413,
                &format!(
                    "request body is {} bytes but the cap is {} — raise AppConfig.body_cap \
                     or send less",
                    request.body.len(),
                    self.config.body_cap
                ),
            );
        }
        let mut fresh = [0u8; 32];
        self.entropy.fill(&mut fresh);
        let fresh_token = hex(&fresh);
        let token = bearer_or_cookie(&request.headers);
        let (auth_required, glue) = (route.auth, route.glue);

        let result = self.db.transact(|tx| {
            let session = match &token {
                Some(token) => Session::by_token(tx, token)?.into_iter().next(),
                None => None,
            };
            if auth_required && session.is_none() {
                return Err(ApiError::Unauthorized);
            }
            let ctx = Ctx {
                session,
                fresh_token,
                created: Cell::new(false),
            };
            glue(tx, &ctx, &captures, &request.body)
        });
        match result {
            Ok(response) => response,
            Err(TxError::App(e)) => e.response(),
            Err(TxError::Io(e)) => Response::error(
                500,
                &format!(
                    "internal storage failure — the request may or may not have \
                     committed; retry after the server recovers: {e}"
                ),
            ),
        }
    }
}

/// `Authorization: Bearer <t>` wins; else the `charpente_session` cookie.
fn bearer_or_cookie(headers: &BTreeMap<String, String>) -> Option<String> {
    if let Some(auth) = headers.get("authorization") {
        if let Some(token) = auth.strip_prefix("Bearer ") {
            return Some(token.trim().to_string());
        }
    }
    for pair in headers.get("cookie")?.split(';') {
        if let Some((name, value)) = pair.trim().split_once('=') {
            if name == "charpente_session" {
                return Some(value.to_string());
            }
        }
    }
    None
}
```

- [ ] **Step 3: Wire the modules** — in `crates/charpente/src/http/mod.rs`, add directly under the module doc comment:

```rust
mod app;
mod router;

pub use app::{App, AppConfig, Ctx, Session};
pub use router::{Glue, Route, RouteDef, RouteMatch, Router};
```

- [ ] **Step 4: Create `crates/charpente/tests/http.rs`** — kernel tests with hand-built glue (the macro arrives in Task 4 and swaps these fixtures):

```rust
//! The HTTP kernel under the sim (PLAN.md M0 step 5): routing, decoding,
//! sessions, error mapping — unit tests here; the macro end-to-end tests and
//! the request-level crash property are appended by later plan tasks.

use std::io;

use charpente::http::{
    decode_body, ApiError, App, AppConfig, Ctx, Method, Request, Response, Route,
};
use charpente::{table, Schema, Tx};
use charpente_core::io::{Disk, Entropy};
use charpente_core::table::Id;
use charpente_sim::{FaultConfig, Sim};
use serde::{Deserialize, Serialize};

#[table]
struct Note {
    id: Id<Note>,
    #[index]
    owner: u64,
    text: String,
}

#[derive(Serialize, Deserialize)]
struct CreateNote {
    id: u64,
    text: String,
}

fn create_note(tx: &mut Tx, body: CreateNote, ctx: &Ctx) -> Result<u64, ApiError> {
    let owner = ctx
        .session()
        .and_then(|s| s.subject.parse().ok())
        .unwrap_or(0);
    tx.put(&Note {
        id: Id::from_raw(body.id),
        owner,
        text: body.text,
    });
    Ok(body.id)
}

// Hand-written glue mirroring what #[route] generates (Task 4 swaps these).
fn create_note_glue(
    tx: &mut Tx,
    ctx: &Ctx,
    _captures: &[String],
    body: &[u8],
) -> Result<Response, ApiError> {
    let body: CreateNote = decode_body(body)?;
    Ok(Response::json(200, &create_note(tx, body, ctx)?))
}

fn get_note_glue(
    tx: &mut Tx,
    _ctx: &Ctx,
    captures: &[String],
    _body: &[u8],
) -> Result<Response, ApiError> {
    use charpente::http::FromParam;
    use charpente_core::table::View;
    let id: Id<Note> = Id::from_param("id", &captures[0])?;
    let note = tx.get(id)?.ok_or(ApiError::NotFound)?;
    Ok(Response::json(200, &note.text))
}

fn login_glue(
    tx: &mut Tx,
    ctx: &Ctx,
    _captures: &[String],
    body: &[u8],
) -> Result<Response, ApiError> {
    let subject: String = decode_body(body)?;
    let token = ctx.create_session(tx, subject);
    Ok(Response::json(200, &token))
}

fn whoami_glue(
    _tx: &mut Tx,
    ctx: &Ctx,
    _captures: &[String],
    _body: &[u8],
) -> Result<Response, ApiError> {
    Ok(Response::json(200, &ctx.session().expect("auth route").subject))
}

fn logout_glue(
    tx: &mut Tx,
    ctx: &Ctx,
    _captures: &[String],
    _body: &[u8],
) -> Result<Response, ApiError> {
    ctx.revoke(tx);
    Ok(Response::json(200, &"bye"))
}

fn note_routes() -> Vec<Route> {
    vec![
        Route { method: Method::Post, pattern: "/notes", auth: false, name: "create_note", glue: create_note_glue },
        Route { method: Method::Get, pattern: "/notes/:id", auth: false, name: "get_note", glue: get_note_glue },
        Route { method: Method::Post, pattern: "/login", auth: false, name: "login", glue: login_glue },
        Route { method: Method::Get, pattern: "/me", auth: true, name: "whoami", glue: whoami_glue },
        Route { method: Method::Post, pattern: "/logout", auth: true, name: "logout", glue: logout_glue },
    ]
}

fn quiet_app(seed: u64) -> (Sim, App) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(note_routes());
    (sim, app)
}

fn body_text(response: &Response) -> String {
    String::from_utf8(response.body.clone()).unwrap()
}

#[test]
fn create_then_get_roundtrip() {
    let (_sim, mut app) = quiet_app(0x477);
    let created = app.handle(
        Request::new(Method::Post, "/notes").json(&CreateNote { id: 7, text: "hi".into() }),
    );
    assert_eq!(created.status, 200, "{}", body_text(&created));

    let got = app.handle(Request::new(Method::Get, "/notes/7"));
    assert_eq!(got.status, 200);
    assert_eq!(body_text(&got), "\"hi\"");
    assert_eq!(app.db().last_applied(), Some(0), "one write txn, one record");
}

#[test]
fn read_only_requests_consume_no_seq() {
    let (_sim, mut app) = quiet_app(0x478);
    app.handle(Request::new(Method::Post, "/notes").json(&CreateNote { id: 1, text: "x".into() }));
    let before = app.db().last_applied();
    let missing = app.handle(Request::new(Method::Get, "/notes/99"));
    assert_eq!(missing.status, 404);
    let found = app.handle(Request::new(Method::Get, "/notes/1"));
    assert_eq!(found.status, 200);
    assert_eq!(app.db().last_applied(), before, "GETs appended nothing");
}

#[test]
fn unknown_path_404_wrong_method_405() {
    let (_sim, mut app) = quiet_app(0x479);
    assert_eq!(app.handle(Request::new(Method::Get, "/nope")).status, 404);
    assert_eq!(app.handle(Request::new(Method::Delete, "/notes")).status, 405);
}

#[test]
fn decode_errors_are_named_400s() {
    let (_sim, mut app) = quiet_app(0x47A);
    let bad_param = app.handle(Request::new(Method::Get, "/notes/abc"));
    assert_eq!(bad_param.status, 400);
    assert!(body_text(&bad_param).contains(":id"), "{}", body_text(&bad_param));

    let mut no_body = Request::new(Method::Post, "/notes");
    no_body.body = Vec::new();
    let resp = app.handle(no_body);
    assert_eq!(resp.status, 400);
    assert!(body_text(&resp).contains("CreateNote"), "{}", body_text(&resp));
    assert_eq!(app.db().last_applied(), None, "failed decodes write nothing");
}

#[test]
fn body_cap_is_a_413_before_decoding() {
    let sim = Sim::with_faults(0x47B, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig { body_cap: 64, ..AppConfig::default() },
    )
    .unwrap()
    .mount(note_routes());
    let mut big = Request::new(Method::Post, "/notes");
    big.body = vec![b'x'; 65];
    let resp = app.handle(big);
    assert_eq!(resp.status, 413);
    assert!(body_text(&resp).contains("body_cap"), "{}", body_text(&resp));
}

#[test]
fn sessions_login_auth_logout() {
    let (_sim, mut app) = quiet_app(0x47C);

    // No session: auth routes are 401 with instructions.
    let denied = app.handle(Request::new(Method::Get, "/me"));
    assert_eq!(denied.status, 401);
    assert!(body_text(&denied).contains("Authorization: Bearer"), "{}", body_text(&denied));

    // Login mints a token.
    let login = app.handle(Request::new(Method::Post, "/login").json(&"user-42"));
    assert_eq!(login.status, 200);
    let token: String = serde_json::from_slice(&login.body).unwrap();
    assert_eq!(token.len(), 64, "32 entropy bytes hex-encoded");

    // Bearer works; so does the cookie.
    let me = app.handle(
        Request::new(Method::Get, "/me").header("Authorization", format!("Bearer {token}")),
    );
    assert_eq!(me.status, 200);
    assert_eq!(body_text(&me), "\"user-42\"");
    let me = app.handle(
        Request::new(Method::Get, "/me")
            .header("Cookie", format!("theme=dark; charpente_session={token}")),
    );
    assert_eq!(me.status, 200);

    // A bad token is 401; logout revokes; the old token stops working.
    let bad = app.handle(Request::new(Method::Get, "/me").header("Authorization", "Bearer nope"));
    assert_eq!(bad.status, 401);
    let out = app.handle(
        Request::new(Method::Post, "/logout").header("Authorization", format!("Bearer {token}")),
    );
    assert_eq!(out.status, 200);
    let after = app.handle(
        Request::new(Method::Get, "/me").header("Authorization", format!("Bearer {token}")),
    );
    assert_eq!(after.status, 401, "revoked session must not authenticate");
}

#[test]
fn sessions_survive_recovery() {
    let sim = Sim::with_faults(0x47D, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(note_routes());
    let login = app.handle(Request::new(Method::Post, "/login").json(&"user-9"));
    let token: String = serde_json::from_slice(&login.body).unwrap();

    sim.restart(); // crash with everything open; the WAL replays the login
    drop(app);
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(note_routes());
    let me = app.handle(
        Request::new(Method::Get, "/me").header("Authorization", format!("Bearer {token}")),
    );
    assert_eq!(me.status, 200, "the session row is inside the commit point");
}

#[test]
fn handler_api_errors_map_to_statuses() {
    let (_sim, mut app) = quiet_app(0x47E);
    let resp = app.handle(Request::new(Method::Get, "/notes/12345"));
    assert_eq!(resp.status, 404, "handler NotFound becomes 404");
}
```

- [ ] **Step 5: Run**

Run: `cargo test -p charpente --test http && cargo test -p charpente http:: && cargo clippy --workspace --all-targets`
Expected: 8 integration tests + router/mod unit tests pass; clippy clean.

- [ ] **Step 6: Commit**

```bash
git add crates/charpente/src/http crates/charpente/tests/http.rs
git commit -m "charpente: HTTP kernel — Router, Session/Ctx, App::handle (one transaction per request)

Routing (literal-beats-param, mount-time conflict panic, 404/405), 1 MiB
body cap fencing the record-size assert from network input, sessions inside
the commit point (raw tokens, M1 hashes), bearer-then-cookie extraction, and
ack-after-durable: 200 only after transact returns Ok. Sim-tested including
session survival across crash recovery."
```

---

### Task 4: The `#[route]` macro

**Files:**
- Modify: `crates/charpente-macros/src/lib.rs` (split into thin entry points)
- Create: `crates/charpente-macros/src/table.rs` (existing expansion moved verbatim)
- Create: `crates/charpente-macros/src/route.rs`
- Modify: `crates/charpente/src/lib.rs` (re-export `route`)
- Modify: `crates/charpente/tests/http.rs` (swap hand-built glue for the macro)

**Interfaces:**
- Consumes: Task 3's `Route`, `RouteDef`, `Glue`, `FromParam`, `decode_body`, `Response::json`, and the crate-root `routes!` macro.
- Produces: `#[route(METHOD "/path/:param"[, auth])]` on `fn name(tx: &mut Tx, <one arg per :param, by name, in order>, [body: impl Deserialize,] [ctx: &Ctx]) -> Result<T: Serialize, ApiError>`. Emits: the untouched fn, a hidden glue fn, and `#[allow(non_camel_case_types)] struct name {}` implementing `RouteDef` (braced struct: type-namespace only, so it cannot collide with the fn's value-namespace name). Usable only from crates that depend on `charpente` (generated code references `::charpente::…`).

- [ ] **Step 1: Split the macro crate.** Move the entire body of `expand` (and its helpers) from `crates/charpente-macros/src/lib.rs` into a new `crates/charpente-macros/src/table.rs` as `pub(crate) fn expand(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream>` (verbatim move — the function body does not change). Rewrite `lib.rs` to:

```rust
//! The derive surface (DESIGN.md §3): `#[table]` turns a struct into a typed
//! row; `#[route]` turns a function into a typed HTTP handler. Macro errors
//! are product surface (hard rule 5): each one names the contract and the fix.
//!
//! `#[table]` references `::charpente_core` paths; `#[route]` references
//! `::charpente` paths — the using crate must depend on the respective crate.

mod route;
mod table;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput, Error, ItemFn};

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
```

(`table.rs` needs `use syn::spanned::Spanned;` etc. — carry over exactly the imports the moved code uses.)

- [ ] **Step 2: Create `crates/charpente-macros/src/route.rs`:**

```rust
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
use syn::{Error, FnArg, Ident, ItemFn, LitStr, Pat, PatType, Token};

pub(crate) struct RouteArgs {
    method: Ident,
    path: LitStr,
    auth: bool,
}

impl Parse for RouteArgs {
    fn parse(input: ParseStream) -> syn::Result<RouteArgs> {
        let method: Ident = input.parse().map_err(|e| {
            Error::new(e.span(), "write `#[route(METHOD \"/path\")]` — e.g. #[route(GET \"/notes/:id\")]")
        })?;
        let path: LitStr = input.parse().map_err(|e| {
            Error::new(e.span(), "the route path must be a string literal — e.g. #[route(GET \"/notes/:id\")]")
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
    let method_variant = match args.method.to_string().as_str() {
        "GET" => quote!(Get),
        "POST" => quote!(Post),
        "PUT" => quote!(Put),
        "PATCH" => quote!(Patch),
        "DELETE" => quote!(Delete),
        other => {
            return Err(Error::new(
                args.method.span(),
                format!("unknown method {other} — one of GET, POST, PUT, PATCH, DELETE"),
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
        return Err(Error::new(args.path.span(), format!("empty :param name in \"{pattern}\"")));
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
            format!("a #[route] handler's first argument must be `tx: &mut Tx` — `{name_str}` is missing it"),
        ));
    }
    let rest: Vec<&PatType> = inputs
        .map(|arg| match arg {
            FnArg::Typed(pat) => Ok(pat),
            FnArg::Receiver(r) => Err(Error::new(r.span(), "#[route] handlers are free functions — no `self`")),
        })
        .collect::<syn::Result<_>>()?;

    let has_ctx = rest.last().is_some_and(|p| arg_name(p).as_deref() == Some("ctx"));
    let non_ctx = if has_ctx { &rest[..rest.len() - 1] } else { &rest[..] };

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
            arg_name(pat).ok_or_else(|| Error::new(pat.span(), "the body argument must be a plain name"))?
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
        let ident = format_ident!("{}", arg_name(non_ctx[params.len()]).expect("checked above"));
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
                    glue: #glue_name,
                }
            }
        }
    })
}
```

- [ ] **Step 3: Re-export** — in `crates/charpente/src/lib.rs`, change the macro re-export line to `pub use charpente_macros::{route, table};`.

- [ ] **Step 4: Swap the test fixtures to the macro** — in `crates/charpente/tests/http.rs`, delete the five `*_glue` functions and `note_routes()`, change the handler fns to macro form, and mount with `routes![…]`:

```rust
use charpente::{route, routes};

#[route(POST "/notes")]
fn create_note(tx: &mut Tx, body: CreateNote, ctx: &Ctx) -> Result<u64, ApiError> {
    let owner = ctx
        .session()
        .and_then(|s| s.subject.parse().ok())
        .unwrap_or(0);
    tx.put(&Note {
        id: Id::from_raw(body.id),
        owner,
        text: body.text,
    });
    Ok(body.id)
}

#[route(GET "/notes/:id")]
fn get_note(tx: &mut Tx, id: Id<Note>) -> Result<String, ApiError> {
    use charpente_core::table::View;
    Ok(tx.get(id)?.ok_or(ApiError::NotFound)?.text)
}

#[route(POST "/login")]
fn login(tx: &mut Tx, subject: String, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_session(tx, subject))
}

#[route(GET "/me", auth)]
fn whoami(_tx_unused: … )
```

Careful — the convention requires the first argument named `tx` even when unused; write `whoami` and `logout` as:

```rust
#[route(GET "/me", auth)]
fn whoami(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let _ = tx;
    Ok(ctx.session().expect("auth route").subject.clone())
}

#[route(POST "/logout", auth)]
fn logout(tx: &mut Tx, ctx: &Ctx) -> Result<&'static str, ApiError> {
    ctx.revoke(tx);
    Ok("bye")
}
```

and mount everywhere `note_routes()` was used:

```rust
fn note_routes() -> Vec<Route> {
    routes![create_note, get_note, login, whoami, logout]
}
```

(Keep `note_routes()` as the single mount point; `quiet_app` and the recovery test keep working unchanged. `get_note` returning the text directly makes the `create_then_get_roundtrip` and `handler_api_errors_map_to_statuses` assertions hold as written.)

Add one macro-specific test:

```rust
#[test]
fn routes_macro_collects_descriptors() {
    let all = note_routes();
    assert_eq!(all.len(), 5);
    assert!(all.iter().any(|r| r.pattern == "/me" && r.auth));
    assert!(all.iter().any(|r| r.pattern == "/notes/:id" && !r.auth));
}
```

- [ ] **Step 5: Run**

Run: `cargo test -p charpente --test http && cargo test --workspace && cargo clippy --workspace --all-targets`
Expected: all green (the same 8 kernel tests now run through macro-generated glue, plus the new descriptor test), clippy clean.

- [ ] **Step 6: Commit**

```bash
git add crates/charpente-macros/src crates/charpente/src/lib.rs crates/charpente/tests/http.rs
git commit -m "macros+charpente: #[route] — typed handlers with macro-enforced argument convention

tx first; one arg per :param by name in path order; at most one JSON body
arg; optional trailing ctx. Emits extraction glue plus a braced marker
struct (type namespace only — no collision with the fn) implementing
RouteDef, collected explicitly by routes![]. Macro errors name the contract
and the fix. Kernel tests now run through generated glue end-to-end."
```

---

### Task 5: Request-level crash property + determinism

**Files:**
- Modify: `crates/charpente/tests/http.rs` (append)

**Interfaces:**
- Consumes: everything above; `run_universes!`, `Snapshot::jobs()`, `Job`/`Codec` from step 4.

The oracle mirrors step 4: request *content* is a pure function of its index, every 200 is an acknowledgment, and each committed request writes one `Item` row and one `ping` job — so after recovery, rows, jobs, and the acked count must all agree exactly.

- [ ] **Step 1: Append to `crates/charpente/tests/http.rs`:**

```rust
// ---------------------------------------------------- crash property

use charpente::{Db, Job};
use charpente_core::log::WalConfig;
use charpente_core::table::Codec;
use charpente_sim::run_universes;

#[table]
struct Item {
    id: Id<Item>,
    content: u64,
}

#[derive(Serialize, Deserialize)]
struct PutItem {
    i: u64,
}

struct Ping {
    i: u64,
}

impl Codec for Ping {
    fn encode(&self, out: &mut Vec<u8>) {
        self.i.encode(out);
    }
    fn decode(input: &mut &[u8]) -> io::Result<Ping> {
        Ok(Ping { i: u64::decode(input)? })
    }
}

impl Job for Ping {
    const NAME: &'static str = "ping";
}

#[route(POST "/items")]
fn put_item(tx: &mut Tx, body: PutItem) -> Result<u64, ApiError> {
    tx.put(&Item {
        id: Id::from_raw(body.i),
        content: body.i * 7,
    });
    tx.enqueue(Ping { i: body.i });
    Ok(body.i)
}

fn crash_app_config() -> AppConfig {
    AppConfig {
        body_cap: 1 << 20,
        db: charpente::DbConfig {
            wal: WalConfig { dir: "wal".to_string(), segment_bytes: 4096 },
            store_path: "tables.redb".to_string(),
        },
    }
}

fn open_crash_app(sim: &Sim) -> io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Item>(),
        Box::new(sim.entropy()),
        crash_app_config(),
    )?
    .mount(routes![put_item]))
}

/// Requests 0..n each wrote Item{i, 7i} and one ping job — verify exactly.
fn http_state_matches(db: &Db, n: u64) -> Result<(), String> {
    let snap = db.snapshot().map_err(|e| format!("snapshot: {e}"))?;
    let items = snap.scan::<Item>().map_err(|e| format!("scan: {e}"))?;
    if items.len() as u64 != n {
        return Err(format!("{n} requests acked but {} items", items.len()));
    }
    for (i, item) in items.iter().enumerate() {
        if (item.id.raw(), item.content) != (i as u64, i as u64 * 7) {
            return Err(format!("item {i} diverges: {:?}", (item.id.raw(), item.content)));
        }
    }
    let jobs = snap.jobs().map_err(|e| format!("jobs: {e}"))?;
    if jobs.len() as u64 != n {
        return Err(format!("dual-write torn over HTTP: {n} acked but {} jobs", jobs.len()));
    }
    for (i, (seq, idx, name, payload)) in jobs.iter().enumerate() {
        let ping = Ping::decode(&mut payload.as_slice()).map_err(|e| format!("job decode: {e}"))?;
        if (*seq, *idx, name.as_str(), ping.i) != (i as u64, 0, "ping", i as u64) {
            return Err(format!("job {i} diverges: seq {seq} idx {idx} {name} i={}", ping.i));
        }
    }
    Ok(())
}

fn is_injected(text: &str) -> bool {
    text.contains("simulated") || text.contains("injected")
}

/// One universe: recover → verify → requests through App::handle (an acked
/// 200 must survive anything) → crash. A 500 whose body is not an injected
/// fault is a real bug and fails the universe.
fn http_workload(sim: &mut Sim) -> Result<(), String> {
    let entropy = sim.entropy();
    let mut acked = 0u64;

    for _round in 0..3 {
        sim.crash_disk_after(64 + entropy.next_u64() % 512);
        let mut app = match open_crash_app(sim) {
            Ok(app) => app,
            Err(e) => {
                if !is_injected(&e.to_string()) {
                    return Err(format!("recovery failed with a non-injected error: {e}"));
                }
                sim.restart();
                continue;
            }
        };
        let recovered = app.db().last_applied().map_or(0, |s| s + 1);
        if recovered < acked {
            return Err(format!("durability violated: {acked} acked over HTTP, {recovered} recovered"));
        }
        acked = recovered;
        http_state_matches(app.db(), acked)?;

        for _ in 0..1 + entropy.next_u64() % 8 {
            // Sprinkle guaranteed-no-write requests: they must not consume a seq.
            if entropy.next_u64().is_multiple_of(5) {
                let before = app.db().last_applied();
                let resp = app.handle(Request::new(Method::Get, "/items"));
                if resp.status != 404 && resp.status != 405 {
                    return Err(format!("GET /items must be unroutable, got {}", resp.status));
                }
                if app.db().last_applied() != before {
                    return Err("an unrouted request consumed a seq".to_string());
                }
            }
            let resp = app.handle(
                Request::new(Method::Post, "/items").json(&PutItem { i: acked }),
            );
            match resp.status {
                200 => acked += 1,
                500 => {
                    let text = String::from_utf8_lossy(&resp.body).to_string();
                    if !is_injected(&text) {
                        return Err(format!("non-injected 500: {text}"));
                    }
                    break; // died mid-commit; durable or not, next round decides
                }
                other => return Err(format!("unexpected status {other}")),
            }
            if entropy.next_u64().is_multiple_of(4) && app.db().checkpoint().is_err() {
                break;
            }
        }
        sim.restart();
    }

    // Final verification (step-4 convention): injected-fault failures skip,
    // anything else is real.
    for _attempt in 0..8 {
        let app = match open_crash_app(sim) {
            Ok(app) => app,
            Err(e) => {
                if !is_injected(&e.to_string()) {
                    return Err(format!("final recovery failed with a non-injected error: {e}"));
                }
                sim.restart();
                continue;
            }
        };
        let recovered = app.db().last_applied().map_or(0, |s| s + 1);
        if recovered < acked {
            return Err(format!("durability violated: {acked} acked over HTTP, {recovered} recovered"));
        }
        return http_state_matches(app.db(), recovered);
    }
    Ok(()) // every attempt died to injected faults; earlier rounds verified every reachable prefix
}

#[test]
fn http_requests_survive_the_fault_matrix() {
    run_universes!(256, http_workload);
}

#[test]
fn http_replays_identically_for_the_same_seed() {
    let trace = |seed: u64| {
        let mut sim = Sim::new(seed);
        let result = http_workload(&mut sim);
        assert_eq!(result, Ok(()), "seed {seed:#x}");
        (sim.durable_fingerprint(), sim.now())
    };
    for seed in [0x51, 0xFACE_0FF5, 0x1DEA_C0DE_1DEA_C0DE] {
        assert_eq!(trace(seed), trace(seed));
    }
}
```

Note: `charpente::DbConfig` and `charpente::Db` are already re-exported from step 4; `Job` likewise.

- [ ] **Step 2: Run three times, fresh seeds**

Run: `cargo test -p charpente --test http -- --nocapture` (three times)
Expected: green each run. A failing universe prints its seed + repro command — **stop, reproduce, report; do not weaken the workload** (a non-injected 500 or a lost acked request is a real finding).

- [ ] **Step 3: Full suite + commit**

```bash
git add crates/charpente/tests/http.rs
git commit -m "charpente: request-level crash property — a 200 is an acknowledgment that survives anything

256 fresh universes/run: recover, verify items+jobs against the acked count
(dual-write invariant over HTTP), issue requests through App::handle under
the fault matrix, crash. Non-injected 500s fail the universe. Same seed,
same responses, same fingerprint."
```

---

### Task 6: The hyper edge (`charpente::serve`) + loopback smoke test

**Files:**
- Create: `crates/charpente/src/serve.rs`
- Modify: `crates/charpente/src/lib.rs` (add `pub mod serve;`)

**Interfaces:**
- Consumes: `App` (moved into the kernel thread), `App::config().body_cap`, `Method::parse`, `Request`/`Response`.
- Produces: `pub fn serve(addr: &str, app: App) -> io::Result<Server>` (non-blocking start; kernel thread + accept thread), `Server::local_addr(&self) -> String`.

hyper 1.x API details (`service_fn`, `TokioIo`, `http1::Builder::serve_connection`, `Limited` body collection) may need mechanical adjustment against the resolved crate versions — the shape below is the requirement; the loopback smoke test green is the acceptance. Do not change the kernel-facing types to make the edge easier.

- [ ] **Step 1: Create `crates/charpente/src/serve.rs`:**

```rust
//! The hyper edge (spec §edge): translates wire↔`http::Request`/`Response`
//! and feeds the single-threaded kernel over a channel. The one sanctioned
//! ambient-IO module besides `prod` — nothing here makes decisions; it moves
//! bytes. Everything behavioral stays in `crate::http`, inside the sim.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::io;
use std::net::SocketAddr;
use std::sync::mpsc;

use http_body_util::{BodyExt, Full, Limited};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;

use crate::http::{App, Method, Request, Response};

/// A running server. v0 has no graceful shutdown — the process owns it; the
/// threads end when the process does.
pub struct Server {
    addr: SocketAddr,
}

impl Server {
    pub fn local_addr(&self) -> String {
        self.addr.to_string()
    }
}

pub fn serve(addr: &str, app: App) -> io::Result<Server> {
    let body_cap = app.config().body_cap;
    let (kernel_tx, kernel_rx) =
        mpsc::channel::<(Request, tokio::sync::oneshot::Sender<Response>)>();

    // The kernel: one thread owning the App — requests serialize here. M0
    // step 6 adds more event sources (due jobs, cron ticks) to this loop.
    std::thread::spawn(move || {
        let mut app = app;
        while let Ok((request, reply)) = kernel_rx.recv() {
            let _ = reply.send(app.handle(request));
        }
    });

    let listener = std::net::TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    listener.set_nonblocking(true)?;

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("tokio runtime construction failed");
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener)
                .expect("listener registers with the runtime");
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                let kernel = kernel_tx.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: hyper::Request<Incoming>| {
                        let kernel = kernel.clone();
                        async move {
                            let response = match to_kernel_request(req, body_cap).await {
                                Ok(request) => {
                                    let (tx, rx) = tokio::sync::oneshot::channel();
                                    if kernel.send((request, tx)).is_err() {
                                        Response::error(500, "the kernel thread is gone")
                                    } else {
                                        rx.await.unwrap_or_else(|_| {
                                            Response::error(500, "the kernel dropped the request")
                                        })
                                    }
                                }
                                Err(response) => response,
                            };
                            Ok::<_, std::convert::Infallible>(from_kernel_response(response))
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
    });

    Ok(Server { addr: local })
}

async fn to_kernel_request(
    req: hyper::Request<Incoming>,
    body_cap: usize,
) -> Result<Request, Response> {
    let Some(method) = Method::parse(req.method().as_str()) else {
        return Err(Response::error(
            405,
            "unsupported method — one of GET, POST, PUT, PATCH, DELETE",
        ));
    };
    let path = req.uri().path().to_string(); // query string stripped: v0 ignores it
    let mut headers = std::collections::BTreeMap::new();
    for (name, value) in req.headers() {
        if let Ok(v) = value.to_str() {
            headers.insert(name.as_str().to_ascii_lowercase(), v.to_string());
        }
    }
    match Limited::new(req.into_body(), body_cap).collect().await {
        Ok(collected) => Ok(Request {
            method,
            path,
            headers,
            body: collected.to_bytes().to_vec(),
        }),
        Err(e) if e.downcast_ref::<http_body_util::LengthLimitError>().is_some() => {
            Err(Response::error(
                413,
                &format!("request body exceeds the {body_cap}-byte cap — raise AppConfig.body_cap or send less"),
            ))
        }
        Err(_) => Err(Response::error(400, "request body could not be read")),
    }
}

fn from_kernel_response(response: Response) -> hyper::Response<Full<Bytes>> {
    let mut builder = hyper::Response::builder().status(response.status);
    for (key, value) in &response.headers {
        builder = builder.header(key, value);
    }
    builder
        .body(Full::new(Bytes::from(response.body)))
        .expect("kernel responses are valid HTTP")
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use super::*;
    use crate::http::{ApiError, AppConfig, Ctx, Route};
    use crate::prod::{ProdDisk, ProdEntropy};
    use charpente_core::db::{Schema, Tx};

    // #[route] can't be used inside the charpente crate itself (it emits
    // ::charpente paths), so the smoke route is built by hand.
    fn hello_glue(
        _tx: &mut Tx,
        _ctx: &Ctx,
        _captures: &[String],
        _body: &[u8],
    ) -> Result<Response, ApiError> {
        Ok(Response::json(200, &"hello"))
    }

    #[test]
    fn serve_answers_http1_over_loopback() {
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        let root = std::env::temp_dir().join(format!(
            "charpente-serve-{:016x}",
            u64::from_le_bytes(suffix)
        ));
        let app = App::open(
            Box::new(ProdDisk::new(&root).unwrap()),
            Schema::new(),
            Box::new(ProdEntropy::new().unwrap()),
            AppConfig::default(),
        )
        .unwrap()
        .mount(vec![Route {
            method: Method::Get,
            pattern: "/hello",
            auth: false,
            name: "hello",
            glue: hello_glue,
        }]);

        let server = serve("127.0.0.1:0", app).unwrap();
        let mut stream = std::net::TcpStream::connect(server.local_addr()).unwrap();
        stream
            .write_all(b"GET /hello HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains("content-type: application/json"), "{response}");
        assert!(response.ends_with("\"hello\""), "{response}");

        // Unknown path routes to the kernel's 404.
        let mut stream = std::net::TcpStream::connect(server.local_addr()).unwrap();
        stream
            .write_all(b"GET /nope HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 404"), "{response}");

        std::fs::remove_dir_all(root).unwrap();
    }
}
```

Note: `charpente_core::io::Entropy` must be in scope in the test module for `.fill(…)` (`use charpente_core::io::Entropy;`) — add it to the test imports if the compiler asks.

- [ ] **Step 2: Register** — add `pub mod serve;` to `crates/charpente/src/lib.rs`.

- [ ] **Step 3: Run**

Run: `cargo test -p charpente serve:: && cargo test --workspace && cargo clippy --workspace --all-targets`
Expected: the smoke test passes (both requests answered), everything green, clippy clean. If a hyper/http-body-util API detail differs from the code above, adapt mechanically and note the change in your report; the smoke test is the acceptance.

- [ ] **Step 4: Commit**

```bash
git add crates/charpente/src/serve.rs crates/charpente/src/lib.rs
git commit -m "charpente: hyper edge — serve() feeds the single-threaded kernel over a channel

The edge translates wire<->struct and nothing else: current-thread tokio rt
for connections, mpsc into the kernel thread that owns the App. Body reads
capped at the edge (413 before buffering past the cap). Loopback smoke test
drives real HTTP/1.1 bytes end-to-end."
```

---

### Task 7: Close out — docs + triple verification

**Files:**
- Modify: `DESIGN.md` (§1 consequence paragraph, §3 Routes bullet)
- Modify: `CLAUDE.md` (current state / next steps)

- [ ] **Step 1: Triple verification**

Run: `cargo test --workspace && cargo test --workspace && cargo test --workspace && cargo clippy --workspace --all-targets && cargo fmt --all --check`
Expected: green three times over fresh seeds; clippy and fmt clean. (Known redb flake policy per Global Constraints.)

- [ ] **Step 2: DESIGN.md §1** — at the end of the "**Consequence for the concurrency model.**" paragraph (ends "…astonishingly fast when it never waits."), append:

```markdown
The HTTP kernel (`charpente::http`) lives inside the boundary — the sim drives
`App::handle` directly; hyper only translates wire↔struct at the edge
(`charpente::serve`) and feeds the single-threaded kernel loop.
```

- [ ] **Step 3: DESIGN.md §3** — replace the Routes bullet:

```markdown
- **Routes.** `#[route(POST "/invoices/:id/finalize")]` — path params, body, and response
  are typed; the OpenAPI description is *derived from* the types, never hand-written.
```

with:

```markdown
- **Routes (v0 implemented).** `#[route(METHOD "/path/:param"[, auth])]` — handler args:
  `tx: &mut Tx`, one per `:param` (same name, path order), at most one JSON body arg,
  optional trailing `ctx: &Ctx` (session access); return `Result<T: Serialize, ApiError>`.
  Handlers run inside one transaction; 200 is emitted only after the commit is durable.
  The OpenAPI description will be *derived from* the types, never hand-written (step 7).
```

- [ ] **Step 4: CLAUDE.md** — replace the final `- Next: M0 step 5 …` bullet with:

```markdown
- **M0 step 5 done**: HTTP layer. Deterministic kernel in `charpente::http` (typed
  `Request`/`Response`, router with mount-time conflict panic, `#[route]` argument
  convention enforced by the macro, 1 MiB body cap fencing the record-size assert,
  sessions inside the commit point via the platform `Session` table — raw tokens, M1
  hashes); one transaction per request, 200 only after the WAL sync (ack-after-durable).
  hyper/tokio confined to `charpente::serve` (wire↔struct + channel into the kernel
  thread); tokio's ambient APIs deny-listed. Sim suite includes the request-level crash
  property (a 200 survives kill-at-any-point; jobs ≡ acked requests) and a determinism
  check; loopback smoke test drives real HTTP/1.1 bytes. `#[route]` emits
  `::charpente` paths, so it works in dependent crates (and integration tests), not in
  charpente's own src.
- Next: M0 step 6 per PLAN.md — queue + cron: post-commit lease/dispatch off the
  `charpente.jobs` table, idempotency keys, typed retry policy, cron ticks as
  transactions; this is where the deterministic edge executor (DECISIONS §10) lands —
  the kernel loop gains its second and third event sources.
```

- [ ] **Step 5: Commit**

```bash
git add DESIGN.md CLAUDE.md
git commit -m "docs: M0 step 5 done — HTTP layer landed; step 6 (queue + cron) is next

Evidence: full workspace suite green over three fresh-seed runs (256 HTTP
universes per run plus the standing transaction/WAL/crash/table suites)."
```

---

## Self-review notes (already applied)

- Spec coverage: boundary split (kernel §Tasks 2–5 / edge §Task 6) ✓, argument convention + macro errors ✓, dispatch pipeline order (match → cap → decode → one transact → respond) ✓, sessions (table, bearer-then-cookie, `charpente_session`, create/revoke, 401) ✓, body cap 413 fencing `MAX_RECORD_BYTES` ✓ (kernel and edge), ack-after-durable ✓ (200 only from `transact` Ok, tested by the crash property), request-level crash property + determinism ✓, error-quality tests on 400/401/404/405/413 ✓, mount-conflict panic ✓, deps confined to `charpente` + tokio deny-list same commit ✓, executor deferred with the kernel-loop comment pointing at step 6 ✓, docs updates ✓, scope cuts respected (no query params — edge strips via `uri().path()`; no streaming; no TLS; no Set-Cookie automation) ✓.
- Type consistency: `Glue = fn(&mut Tx, &Ctx, &[String], &[u8]) -> Result<Response, ApiError>` identical in router.rs, hand-built test glue, and macro codegen; `App::open(disk, schema, entropy, config)` used identically in Tasks 3, 5, 6; `AppConfig { body_cap, db }` everywhere; `Server::local_addr` only consumed in Task 6's test.
- Known deviation from the spec's sketch, decided here: the spec sketched `App::new(db, …)`; the plan uses `App::open(disk, schema, …)` so the platform registers the `Session` table in the Schema *before* `Db::open` — otherwise recovery of a WAL containing logins would fail with unregistered-table on apps that forgot to register it. Footgun prevention; noted for the spec's record.
