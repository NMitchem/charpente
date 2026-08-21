//! The deterministic HTTP kernel (DESIGN.md §3; spec
//! docs/superpowers/specs/2026-07-02-http-layer-design.md): typed requests,
//! routing, decoding, sessions, and transact-wrapping — all inside the
//! simulation boundary. The wire lives in `crate::serve` (hyper); this
//! module never touches a socket. `App::handle(Request) -> Response` is the
//! whole surface, which is exactly what the sim drives.

mod app;
mod router;

pub use app::{App, AppConfig, Ctx, Handled, Session, StagedDispatch};
pub use router::{Glue, Route, RouteDef, RouteGlue, RouteMatch, Router, SubGlue};

/// Not public API: the kernel's batched commit-failure tail needs the exact
/// 500 the single-request path produces (group-commit T6).
pub(crate) use app::io_failure_response;

use std::collections::BTreeMap;
use std::io;

use charpente_core::db::Event;
use charpente_core::table::Id;

use crate::auth::OidcReject;

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
/// with the wire. Header keys are lowercase. `path` never carries a query
/// string — `Request::new` splits it into `query` at construction, the only
/// v0 consumer being the SSE `cursor` query parameter (`?cursor=<id>.<seq>`).
pub struct Request {
    pub method: Method,
    pub path: String,
    pub query: BTreeMap<String, String>,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

impl Request {
    /// `path` may carry a `?query` suffix (e.g. `"/feed?cursor=5.9"`) — it is
    /// split off into `query` here, never left in `path` (the router matches
    /// path segments only). No percent-decoding: v0's one query consumer
    /// (`cursor`) is plain digits and a dot.
    pub fn new(method: Method, path: impl Into<String>) -> Request {
        let raw = path.into();
        let (path, query) = match raw.split_once('?') {
            Some((path, query)) => (path.to_string(), parse_query(query)),
            None => (raw, BTreeMap::new()),
        };
        Request {
            method,
            path,
            query,
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

fn parse_query(raw: &str) -> BTreeMap<String, String> {
    raw.split('&')
        .filter(|pair| !pair.is_empty())
        .filter_map(|pair| pair.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
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

    /// The 200 that opens an SSE connection (T5): `content-type:
    /// text/event-stream`, `cache-control: no-cache`, empty body — frames
    /// follow over the sink the kernel pushes into for the rest of the
    /// connection (`Respond::stream`, `Kernel`'s Tail registration).
    pub fn sse_headers() -> Response {
        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_string(), "text/event-stream".to_string());
        headers.insert("cache-control".to_string(), "no-cache".to_string());
        Response {
            status: 200,
            headers,
            body: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------- realtime

/// The realtime resume cursor (DESIGN §6): an event id plus its committing
/// record's seq. The seq half exists for failover honesty — two diverged
/// timelines (an old primary's unshipped tail vs. a promoted follower's new
/// writes) can mint the same event id for different events; a bare id
/// cannot detect that. Resume validation (T5) requires the retained row at
/// `id` to carry `seq`, else the divergence error, never silent
/// misattribution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cursor {
    pub id: u64,
    pub seq: u64,
}

impl Cursor {
    /// Wire form "<id>.<seq>". `None` for an unparseable string — the caller
    /// maps that to the 400 below, never a silent from-now.
    pub fn parse(s: &str) -> Option<Cursor> {
        let (id, seq) = s.split_once('.')?;
        Some(Cursor {
            id: id.parse().ok()?,
            seq: seq.parse().ok()?,
        })
    }

    pub fn wire(&self) -> String {
        format!("{}.{}", self.id, self.seq)
    }
}

/// One subscribed event type's delivery entry: its wire name and its
/// monomorphized decode-then-JSON-encode function, captured statically at
/// the `Subscription::of::<Ev>()` call site. There is no runtime event
/// registry (spec §4) — the delivery path already holds the encoder for
/// every name it can match.
pub struct SubEntry {
    pub name: &'static str,
    pub encode_json: fn(&[u8]) -> io::Result<String>,
}

/// What an SSE route handler returns: which event types to tail (compile-
/// checked — names never appear as strings in app code), an optional
/// exact-match scoping key, and the resume cursor (the handler passes
/// through, or clamps, what the platform extracted from the request).
pub struct Subscription {
    pub entries: Vec<SubEntry>,
    pub key: Option<String>,
    pub cursor: Option<Cursor>,
}

impl Subscription {
    pub fn of<Ev: Event + serde::Serialize>() -> Subscription {
        Subscription {
            entries: vec![SubEntry {
                name: Ev::NAME,
                encode_json: ev_json::<Ev>,
            }],
            key: None,
            cursor: None,
        }
    }

    pub fn and<Ev: Event + serde::Serialize>(mut self) -> Subscription {
        self.entries.push(SubEntry {
            name: Ev::NAME,
            encode_json: ev_json::<Ev>,
        });
        self
    }

    /// An exact-match scoping filter (e.g. an org id) — the handler owns
    /// access control; the platform never parses the key.
    pub fn key(mut self, key: impl Into<String>) -> Subscription {
        self.key = Some(key.into());
        self
    }

    pub fn cursor(mut self, cursor: Option<Cursor>) -> Subscription {
        self.cursor = cursor;
        self
    }
}

fn ev_json<Ev: Event + serde::Serialize>(bytes: &[u8]) -> io::Result<String> {
    let mut input = bytes;
    let ev = Ev::decode(&mut input)?;
    serde_json::to_string(&ev).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("event {} failed JSON encoding: {e}", Ev::NAME),
        )
    })
}

// ---------------------------------------------------------------- errors

/// What a handler can reject with. `Io` failures convert via `From`, so `?`
/// works on tx reads inside handlers.
///
/// Derives `Wire` + `Serialize` (task 5, Amendment E) because every route's
/// `error: fn() -> TypeSchema` names this type — without it, no `#[route]`
/// handler compiles. Task 4's enum-payload-free-variant IR change
/// (`EnumRepr::External { variants: Vec<(String, Option<TypeSchema>)> }`)
/// exists specifically so this mixed unit/newtype enum can derive at all.
///
/// **This is a schema for `ApiError`'s own serde shape, not for the bytes a
/// client actually receives.** `ApiError::response()` (below) always emits
/// the fixed envelope `{"error": "<message>"}` regardless of variant (see
/// `docs/GUIDE.md`'s "`ApiError` and status mapping" section) — deriving
/// `Wire`/`Serialize` here does not change that. `Contract.types["ApiError"]`
/// therefore describes a shape the wire never actually carries; a future
/// task (the TS emitter, or a change to `response()` itself) needs to
/// reconcile the two. Flagged rather than silently redesigned in task 5.
#[derive(Debug, ::charpente::Wire, serde::Serialize)]
pub enum ApiError {
    BadRequest(String),
    /// No credential presented at all on an `auth` route.
    Unauthorized,
    /// A bearer/cookie token hashed to no live `ApiKey`/`Session` row:
    /// garbage, revoked (row deleted), or a two-dot bearer with no OIDC
    /// configured — distinct from `Unauthorized` (nothing presented) and
    /// from `SessionExpired` (a real row, just past its TTL).
    UnrecognizedCredential,
    /// A bearer/cookie token matched a session row past its `expires_at` —
    /// distinct from `UnrecognizedCredential` (no match at all): the client
    /// had a real session, it just aged out.
    SessionExpired,
    /// A two-dot bearer, judged as a JWT, failed OIDC verification — the
    /// per-check 401 text lives on `OidcReject` itself (spec §6); this
    /// variant is the one mapping seam from `AuthReject::Oidc`.
    Oidc(OidcReject),
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
                "authentication required — send \"Authorization: Bearer <token>\" or the \
                 charpente_session cookie",
            ),
            ApiError::UnrecognizedCredential => Response::error(
                401,
                "unrecognized credential — it may have been revoked; obtain a new one",
            ),
            ApiError::SessionExpired => Response::error(401, "session expired — log in again"),
            ApiError::Oidc(reject) => Response::error(401, &reject.to_string()),
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
    serde_json::from_slice(body)
        .map_err(|e| ApiError::BadRequest(format!("invalid JSON body for {type_name}: {e}")))
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
        for m in [
            Method::Get,
            Method::Post,
            Method::Put,
            Method::Patch,
            Method::Delete,
        ] {
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
        assert!(
            msg.contains(":id") && msg.contains("unsigned integer") && msg.contains("abc"),
            "{msg}"
        );
    }

    #[test]
    fn body_errors_name_the_type() {
        #[derive(serde::Deserialize, Debug)]
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
