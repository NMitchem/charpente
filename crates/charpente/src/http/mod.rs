//! The deterministic HTTP kernel (DESIGN.md §3; spec
//! docs/superpowers/specs/2026-07-02-http-layer-design.md): typed requests,
//! routing, decoding, sessions, and transact-wrapping — all inside the
//! simulation boundary. The wire lives in `crate::serve` (hyper); this
//! module never touches a socket. `App::handle(Request) -> Response` is the
//! whole surface, which is exactly what the sim drives.

mod app;
mod router;

pub use app::{App, AppConfig, Ctx, Handled, Session, StagedDispatch};
pub use router::{
    EmittedEvent, Glue, PublicScope, Route, RouteDef, RouteGlue, RouteMatch, Router, SubGlue,
};

/// Not public API: the kernel's batched commit-failure tail needs the exact
/// 500 the single-request path produces (group-commit T6).
pub(crate) use app::io_failure_response;

use std::collections::BTreeMap;
use std::io;
use std::marker::PhantomData;

use charpente_core::db::{Event, ScopedTx, Tenant};
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

/// The single definition of the envelope every `#[route]` error response
/// carries (task 5 fix round 1, F1): `Response::error` serializes exactly
/// this shape, and `<ApiError as Wire>::schema()` below describes exactly
/// this shape — one `error: String` field, always present. Kept as one
/// module-level type, rather than re-declared per call site, so there is a
/// single place a reader (or a future editor) sees both sides of the
/// envelope's contract next to each other. `fidelity_api_error_is_the_envelope_not_its_serde_shape`
/// (below) is the mechanical check that keeps the two in sync.
#[derive(serde::Serialize)]
struct ErrorEnvelope<'a> {
    error: &'a str,
}

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
        Response::json(status, &ErrorEnvelope { error: message })
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
    /// Wire form `"<id>.<seq>"`. `None` for an unparseable string — the caller
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
    /// The subscribed type's `Event::FINGERPRINT`: delivery decodes a stored
    /// row with `encode_json` only when the row's recorded shape equals this
    /// (payload-shapes spec §3).
    pub fingerprint: u64,
}

/// What an SSE route handler returns: which event types to tail (compile-
/// checked — names never appear as strings in app code), an optional
/// exact-match app key, the resume cursor (the handler passes through, or
/// clamps, what the platform extracted from the request), and — private —
/// the tenancy it delivers: `None` for a global subscription
/// (`Subscription::of`), the tenant for a tenant feed (`Subscribe`). No app
/// code can set or read it, so no string ever names a tenant.
pub struct Subscription {
    pub entries: Vec<SubEntry>,
    pub key: Option<String>,
    pub cursor: Option<Cursor>,
    pub(crate) scope: Option<u64>,
}

/// `Ev`'s delivery entry, captured statically.
fn sub_entry<Ev: Event + serde::Serialize>() -> SubEntry {
    SubEntry {
        name: Ev::NAME,
        encode_json: ev_json::<Ev>,
        fingerprint: Ev::FINGERPRINT,
    }
}

/// Adds `Ev`'s entry, refusing a second of one `NAME`: delivery matches a
/// row to the first entry of its name, so a second could never be delivered
/// — two `#[event]` types in different modules with the same type identifier
/// share a `NAME`. `method` names the caller in the refusal.
fn push_entry<Ev: Event + serde::Serialize>(entries: &mut Vec<SubEntry>, method: &str) {
    assert!(
        !entries.iter().any(|e| e.name == Ev::NAME),
        "{method}: event \"{}\" is already in this subscription — an event appears once, and \
         two #[event] types sharing a NAME (the type's snake_case) would leave the second \
         undeliverable; give the other type its own name (GUIDE §11, \"Changing an event\")",
        Ev::NAME
    );
    entries.push(sub_entry::<Ev>());
}

impl Subscription {
    /// A global subscription: it delivers events emitted on a bare `Tx`, and
    /// never an event recorded in a tenant. A tenant's feed is
    /// `tx.subscription::<Ev>()` on its `ScopedTx` (`Subscribe`).
    pub fn of<Ev: Event + serde::Serialize>() -> Subscription {
        Subscription {
            entries: vec![sub_entry::<Ev>()],
            key: None,
            cursor: None,
            scope: None,
        }
    }

    /// Adds another global event type. Panics if one of that `NAME` is
    /// already here, and panics on a tenant feed: a tenant feed receives only
    /// events recorded in its tenant, so an event added after `.into()` could
    /// never arrive. The tenant's own events go on `TenantFeed::and`, before
    /// the conversion.
    pub fn and<Ev: Event + serde::Serialize>(mut self) -> Subscription {
        assert!(
            self.scope.is_none(),
            "Subscription::and: event \"{}\" is added to a tenant feed after .into() — \
             Subscription::and adds global events, and a tenant feed receives only its own \
             tenant's events, so this one could never arrive; add the tenant's events with \
             TenantFeed::and before .into(), and serve global events on a subscription of \
             their own",
            Ev::NAME
        );
        push_entry::<Ev>(&mut self.entries, "Subscription::and");
        self
    }

    /// An exact-match app filter (a user's subject, a room) — the handler
    /// owns access control; the platform never parses the key. A key is not
    /// a tenant: a tenant feed is built from its scope (`Subscribe`), and
    /// delivery compares tenancy on its own (`admits`).
    pub fn key(mut self, key: impl Into<String>) -> Subscription {
        self.key = Some(key.into());
        self
    }

    pub fn cursor(mut self, cursor: Option<Cursor>) -> Subscription {
        self.cursor = cursor;
        self
    }

    /// The one predicate that says what this subscription delivers
    /// (tenancy-completion spec §6, "Delivering"): a name it carries, exactly
    /// its tenancy (`None` is not `Some(0)`), and its key if it has one.
    /// `deliver_unit` skips every row it refuses, and `validate_subscribe`
    /// asks it before refusing a reshaped cursor. The shape check stays
    /// separate because a row this admits but cannot decode ends the tail
    /// rather than being skipped.
    pub(crate) fn admits(&self, name: &str, key: Option<&str>, scope: Option<u64>) -> bool {
        self.entries.iter().any(|e| e.name == name)
            && scope == self.scope
            && (self.key.is_none() || self.key.as_deref() == key)
    }
}

/// A tenant's feed under construction: `tx.subscription::<Ev>()` on a
/// `ScopedTx<S>` (`Subscribe`), narrowed with `.and`, `.key` and `.cursor`,
/// and converted with `.into()` into the handler's `Subscription`, which
/// carries the tenant (amendment A3). `S` says whose tenant it is; the id
/// itself is in the `Subscription`, where no app code can read or set it.
pub struct TenantFeed<S> {
    sub: Subscription,
    tenant: PhantomData<fn() -> S>,
}

impl<S> TenantFeed<S> {
    /// Adds another of the tenant's event types. It pushes to the feed's own
    /// entries, never through `Subscription::and`, which refuses a tenant
    /// feed; a second event of one `NAME` is refused as it is there.
    pub fn and<Ev: Event + serde::Serialize>(mut self) -> TenantFeed<S> {
        push_entry::<Ev>(&mut self.sub.entries, "TenantFeed::and");
        self
    }

    /// `Subscription::key`: the app's own filter, within the tenant.
    pub fn key(mut self, key: impl Into<String>) -> TenantFeed<S> {
        self.sub.key = Some(key.into());
        self
    }

    pub fn cursor(mut self, cursor: Option<Cursor>) -> TenantFeed<S> {
        self.sub.cursor = cursor;
        self
    }
}

impl<S> From<TenantFeed<S>> for Subscription {
    fn from(feed: TenantFeed<S>) -> Subscription {
        feed.sub
    }
}

/// Builds a tenant's feed from the scope that admitted the subscriber
/// (tenancy-completion spec §6, "Subscribing"): `tx.subscription::<Ev>()` on
/// a `ScopedTx<S>`. A trait so the platform crate can give the core's
/// `ScopedTx` this method; `use charpente::Subscribe` brings it into scope.
pub trait Subscribe<S> {
    fn subscription<Ev: Event + serde::Serialize>(&self) -> TenantFeed<S>;
}

impl<S: Tenant> Subscribe<S> for ScopedTx<'_, '_, S> {
    fn subscription<Ev: Event + serde::Serialize>(&self) -> TenantFeed<S> {
        TenantFeed {
            sub: Subscription {
                entries: vec![sub_entry::<Ev>()],
                key: None,
                cursor: None,
                scope: Some(self.scope().raw()),
            },
            tenant: PhantomData,
        }
    }
}

/// Only ever called on a row whose recorded shape is `Ev::FINGERPRINT`
/// (`deliver_unit` checks first). A decode failure here — a short read or
/// bytes left over — therefore has two possible causes, and the text names
/// both with their fixes: corrupt stored bytes, or a change inside a type one
/// of the fields holds, which the fingerprint hashes by name and cannot see
/// (GUIDE §12).
fn ev_json<Ev: Event + serde::Serialize>(bytes: &[u8]) -> io::Result<String> {
    let undecodable = |detail: String| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "event {} failed to decode although its recorded shape matches this binary \
                 ({:#x}): {detail} — either a type one of its fields holds changed, which the \
                 fingerprint cannot see (GUIDE §12), or the stored bytes are corrupt; if such a \
                 type changed, restore that type's old definition and put the new one in a new \
                 #[event] type under a new name (GUIDE §11); otherwise restore this node from \
                 its archive (GUIDE §15) or re-seed it from its primary (GUIDE §10)",
                Ev::NAME,
                Ev::FINGERPRINT
            ),
        )
    };
    let mut input = bytes;
    let ev = Ev::decode(&mut input).map_err(|e| undecodable(e.to_string()))?;
    if !input.is_empty() {
        return Err(undecodable(format!(
            "{} bytes left over after the payload",
            input.len()
        )));
    }
    serde_json::to_string(&ev).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "event {} failed JSON encoding: {e} — every #[event] payload must be plainly \
                 JSON (GUIDE §11); fix the emitting code",
                Ev::NAME
            ),
        )
    })
}

// ---------------------------------------------------------------- errors

/// What a handler can reject with. `Io` failures convert via `From`, so `?`
/// works on tx reads inside handlers.
///
/// **Does NOT `#[derive(Wire)]`** (task 5 fix round 1, F1 — this reverses
/// task 5's original choice). Every route's `error: fn() -> TypeSchema` is
/// `<E as Wire>::reference()`, generic over the handler's declared error
/// type, so `E`'s `Wire` schema must describe what `E::response()` actually
/// puts on the wire — not `E`'s own serde shape. A derived schema here would
/// be faithful to `serde_json::to_string(&ApiError::BadRequest("bad"))`
/// (`{"BadRequest":"bad"}`), which is a document `ApiError::response()`
/// never sends: every variant instead goes through `Response::error` below
/// and comes out as the fixed envelope `{"error": "<message>"}`. Measured,
/// per variant: zero overlap between the derived shape and the wire bytes.
/// The hand-written `impl Wire for ApiError` below describes the envelope
/// instead, so `Route.error`'s generic reference is finally correct for
/// every handler. Does not derive `Serialize` either — nothing serializes
/// an `ApiError` directly (`Response::error` serializes `ErrorEnvelope`),
/// and a `Serialize` impl that disagreed with the `Wire` schema is exactly
/// what the Task 3/4 fidelity discipline forbids.
#[derive(Debug)]
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
    /// No credential presented, at a tenant scope line the route did not
    /// declare `public(S)` for (job-tenancy spec §4, rule 3). The text is
    /// built by `Ctx`, because it quotes the route's site and stance.
    AnonymousScope(String),
    NotFound,
    Internal(String),
}

/// Hand-written, not derived (see the doc comment above `ApiError`): the
/// schema describes the `ErrorEnvelope` shape `Response::error` actually
/// serializes, not `ApiError`'s own variants. `WIRE_NAME` stays `"ApiError"`
/// — the plan's Task 9 test asserts `Promise<Result<CustomerView, ApiError>>`,
/// and every `Route.error` reference still needs a name to point at.
impl crate::wire::Wire for ApiError {
    const WIRE_NAME: &'static str = "ApiError";

    fn schema() -> crate::wire::TypeSchema {
        crate::wire::TypeSchema::Struct {
            fields: vec![crate::wire::Field {
                name: "error".to_string(),
                schema: crate::wire::TypeSchema::String,
                optional: false,
            }],
        }
    }

    fn register(reg: &mut crate::wire::TypeRegistry) {
        // No recursion: the envelope's one field is a bare `String`, which
        // has nothing further to register (see `prim!` in `wire.rs`).
        reg.insert_as(
            Self::WIRE_NAME,
            <Self as crate::wire::Wire>::schema(),
            std::any::type_name::<Self>(),
        );
    }
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
            ApiError::AnonymousScope(msg) => Response::error(401, msg),
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

// Kept deliberately (task 5), even though a `u64` path param is now a
// compile error at `#[route]` (it lacks `Wire`, which `Route.params` needs
// alongside `FromParam`). Measured (fix round 1, F4): removing this impl
// does NOT trade one diagnostic for the other — rustc emits BOTH errors
// either way, a bare "`FromParam` is not satisfied" AND the `Wire`
// `on_unimplemented` note that names the 2^53 JS-precision hazard and the
// fix (`Id<T>`/`WireU64`). Keeping the impl keeps the `Wire` note the ONLY
// error, so a reader isn't led to the wrong remedy (implementing
// `FromParam`) first. Same rejection either way; this is about which
// error a reader sees first, not what compiles.
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
    use std::marker::PhantomData;

    use super::*;
    use crate::event;
    use crate::wire::WireU64;

    #[event(global)]
    struct UnitPing {
        n: WireU64,
    }

    #[event(global)]
    struct UnitPong {
        n: WireU64,
    }

    /// A scope type for `TenantFeed<S>`, which bounds nothing on `S`.
    struct UnitOrg;

    fn unit_sub(key: Option<&str>, scope: Option<u64>) -> Subscription {
        Subscription {
            entries: vec![sub_entry::<UnitPing>()],
            key: key.map(str::to_string),
            cursor: None,
            scope,
        }
    }

    /// Tenancy-completion spec §6, "Delivering": the one predicate delivery
    /// and resume validation both ask. Every clause is exercised alone, and
    /// tenant 0 is a tenant.
    #[test]
    fn admits_is_the_name_and_the_tenant_and_the_key() {
        let global = unit_sub(None, None);
        assert!(global.admits("unit_ping", None, None));
        assert!(
            global.admits("unit_ping", Some("k"), None),
            "a feed with no key passes every key"
        );
        assert!(
            !global.admits("unit_pong", None, None),
            "a name it does not carry"
        );
        assert!(
            !global.admits("unit_ping", None, Some(0)),
            "tenant 0's row is not global: None is not Some(0)"
        );
        let tenant_zero = unit_sub(None, Some(0));
        assert!(tenant_zero.admits("unit_ping", None, Some(0)));
        assert!(
            !tenant_zero.admits("unit_ping", None, None),
            "a global row never reaches a tenant feed"
        );
        assert!(
            !tenant_zero.admits("unit_ping", None, Some(1)),
            "another tenant's row"
        );
        let keyed = unit_sub(Some("k"), Some(7));
        assert!(keyed.admits("unit_ping", Some("k"), Some(7)));
        assert!(
            !keyed.admits("unit_ping", None, Some(7)),
            "a keyed feed never receives an unkeyed row"
        );
        assert!(!keyed.admits("unit_ping", Some("j"), Some(7)));
        assert!(
            !keyed.admits("unit_ping", Some("k"), Some(8)),
            "the right key in the wrong tenant"
        );
    }

    /// `TenantFeed::and` adds to its inner entries directly, so
    /// `Subscription::and`'s widening assert never fires for it.
    #[test]
    fn a_tenant_feeds_and_adds_without_the_widening_assert() {
        let feed = TenantFeed::<UnitOrg> {
            sub: unit_sub(None, Some(0)),
            tenant: PhantomData,
        }
        .and::<UnitPong>()
        .key("k");
        let sub: Subscription = feed.into();
        assert_eq!(
            sub.entries.iter().map(|e| e.name).collect::<Vec<_>>(),
            vec!["unit_ping", "unit_pong"]
        );
        assert_eq!((sub.key.as_deref(), sub.scope), (Some("k"), Some(0)));
    }

    #[test]
    #[should_panic(
        expected = "TenantFeed::and: event \"unit_ping\" is already in this subscription — an \
                    event appears once, and two #[event] types sharing a NAME (the type's \
                    snake_case) would leave the second undeliverable; give the other type its \
                    own name (GUIDE §11, \"Changing an event\")"
    )]
    fn a_tenant_feed_refuses_two_events_of_one_name() {
        let _ = TenantFeed::<UnitOrg> {
            sub: unit_sub(None, Some(0)),
            tenant: PhantomData,
        }
        .and::<UnitPing>();
    }

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

    /// F1 (task 5 fix round 1): `ApiError`'s `Wire` schema must describe
    /// the envelope `Response::error` actually serializes, not `ApiError`'s
    /// own serde shape — a derived schema would be faithful to
    /// `serde_json::to_string(&ApiError::BadRequest("bad"))`
    /// (`{"BadRequest":"bad"}`), a document the wire never sends. This
    /// reads BOTH sides per the standing fidelity constraint: real bytes
    /// from `ApiError::response().body` across every variant, AND
    /// `<ApiError as Wire>::schema()` — and shows they agree.
    ///
    /// Mutation that reddens this: change `ApiError::schema()` to describe
    /// anything other than `Struct{fields:[Field{name:"error",...}]}` (e.g.
    /// reintroducing the derive) — the real response body still has key
    /// "error", so the loop's key-set assertion fails against the new
    /// schema-shaped expectation.
    #[test]
    fn fidelity_api_error_is_the_envelope_not_its_serde_shape() {
        use crate::wire::{Field, TypeSchema, Wire};

        assert_eq!(
            <ApiError as Wire>::schema(),
            TypeSchema::Struct {
                fields: vec![Field {
                    name: "error".to_string(),
                    schema: TypeSchema::String,
                    optional: false,
                }],
            }
        );

        let variants: Vec<ApiError> = vec![
            ApiError::BadRequest("bad".into()),
            ApiError::Unauthorized,
            ApiError::UnrecognizedCredential,
            ApiError::SessionExpired,
            ApiError::NotFound,
            ApiError::Internal("boom".into()),
        ];
        for v in variants {
            let resp = v.response();
            let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
            let obj = body.as_object().expect("the envelope is a JSON object");
            assert_eq!(
                obj.keys().collect::<Vec<_>>(),
                vec!["error"],
                "the real wire body must have exactly the one field the schema describes: {body}"
            );
            assert!(
                obj["error"].is_string(),
                "the schema says `error` is a String: {body}"
            );
        }
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
