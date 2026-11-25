//! The request path's half of retry-safe writes (spec
//! docs/superpowers/specs/2026-11-22-retry-safe-writes-design.md §4.1–§4.3):
//! which routes are writes, which id a request presents, the key its record
//! lives under, and what a repeat is answered with. Pure functions; the
//! lookup and the write happen in `StagedDispatch::stage_or_subscribe`,
//! inside the request's own staged transaction, so the record commits with
//! the write it records or not at all.

use std::collections::BTreeMap;

use charpente_core::intent::{IntentReply, IntentRow};
use charpente_core::io::WallTime;
use charpente_core::table::Id;
use sha2::{Digest, Sha256};

use crate::http::{Method, Request, Response, Route, RouteGlue};

/// The header a write route reads its intent id from, unless the route
/// names its sender's (`Route::intent`). The IETF httpapi draft's name.
pub(crate) const DEFAULT_HEADER: &str = "Idempotency-Key";

/// The most a record keeps of a reply, headers plus body, in bytes. A larger
/// reply keeps its status and its size, and a repeat is answered 409 (spec
/// §4.3), so no stored reply can push a record past `MAX_RECORD_BYTES`.
pub(crate) const KEPT_REPLY_MAX: usize = 65_536;

/// The header a replayed reply carries, so a client can tell an answer from
/// the record apart from one that ran.
const REPLAYED: &str = "idempotent-replayed";

/// What a request presents in its route's id header.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Presented {
    Absent,
    Valid(String),
    Malformed,
}

/// What the record says to do with a request that presented a valid id.
pub(crate) enum Decision {
    /// No record: run the handler, and record its reply if it wrote.
    Fresh,
    /// The record answers: the replay, or the 409/422 that refuses it.
    Answer(Response),
}

/// The header `route` reads its intent id from.
pub(crate) fn header_name(route: &Route) -> &'static str {
    route.intent.unwrap_or(DEFAULT_HEADER)
}

/// A write route: an HTTP route whose method is not GET. An SSE route
/// registers `Method::Get`, and is excluded by its glue as well.
pub(crate) fn is_write(route: &Route) -> bool {
    matches!(route.glue, RouteGlue::Http(_)) && route.method != Method::Get
}

/// The id `request` presents in `route`'s header: absent, or valid when it
/// is 1 to 255 bytes, each visible ASCII (0x21..=0x7e), else malformed.
/// `Request`'s header keys are lowercase, so the name is lowercased here.
pub(crate) fn presented(request: &Request, route: &Route) -> Presented {
    match request
        .headers
        .get(&header_name(route).to_ascii_lowercase())
    {
        None => Presented::Absent,
        Some(id)
            if (1..=255).contains(&id.len()) && id.bytes().all(|b| (0x21..=0x7e).contains(&b)) =>
        {
            Presented::Valid(id.clone())
        }
        Some(_) => Presented::Malformed,
    }
}

/// Appends `bytes` with its length in front, so two fields can never be read
/// as one with the boundary moved.
fn prefixed(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

/// The record's key: the first 8 bytes, big-endian, of a SHA-256 over the
/// length-prefixed principal, route and id. So two principals or two routes
/// never share an intent; two triples that share a slot anyway are told
/// apart by `decide`, which compares the row's own fields.
pub(crate) fn row_id(principal: Option<&str>, route: &str, id: &str) -> Id<IntentRow> {
    let mut hasher = Sha256::new();
    match principal {
        None => hasher.update([0u8]),
        Some(subject) => {
            hasher.update([1u8]);
            prefixed(&mut hasher, subject.as_bytes());
        }
    }
    prefixed(&mut hasher, route.as_bytes());
    prefixed(&mut hasher, id.as_bytes());
    let digest: [u8; 32] = hasher.finalize().into();
    let mut slot = [0u8; 8];
    slot.copy_from_slice(&digest[..8]);
    Id::from_raw(u64::from_be_bytes(slot))
}

/// What makes two requests under one id the same request: a SHA-256 over
/// the length-prefixed method, path, query (in key order) and body.
pub(crate) fn fingerprint(request: &Request) -> [u8; 32] {
    let mut hasher = Sha256::new();
    prefixed(&mut hasher, request.method.name().as_bytes());
    prefixed(&mut hasher, request.path.as_bytes());
    hasher.update((request.query.len() as u64).to_be_bytes());
    for (key, value) in &request.query {
        prefixed(&mut hasher, key.as_bytes());
        prefixed(&mut hasher, value.as_bytes());
    }
    prefixed(&mut hasher, &request.body);
    hasher.finalize().into()
}

/// A reply's size as a record counts it: every header's name and value,
/// plus the body.
fn reply_bytes(reply: &Response) -> usize {
    reply
        .headers
        .iter()
        .map(|(name, value)| name.len() + value.len())
        .sum::<usize>()
        + reply.body.len()
}

/// What `row`, the record found under this request's key, says to do.
/// `header`, `id`, `principal`, `route` and `fingerprint` are this request's.
pub(crate) fn decide(
    row: Option<&IntentRow>,
    header: &str,
    id: &str,
    principal: Option<&str>,
    route: &str,
    fingerprint: &[u8; 32],
) -> Decision {
    let Some(row) = row else {
        return Decision::Fresh;
    };
    if row.intent != id || row.principal.as_deref() != principal || row.route != route {
        return Decision::Answer(Response::error(
            409,
            &format!(
                "{header} `{id}` shares a record slot with another request's id: send this \
                 request under a new {header}."
            ),
        ));
    }
    // Negative space: `record` derives a row's id from these three fields,
    // so a row that matches them under another id was not written by it.
    assert_eq!(
        row.id,
        row_id(principal, route, id),
        "intent record {}: its principal, route and id hash to another slot — only \
         `http::intent::record` writes an intent row, and it keys the row by those three",
        row.id.raw()
    );
    if row.fingerprint != *fingerprint {
        return Decision::Answer(Response::error(
            422,
            &format!(
                "{header} `{id}` was already used for a different request to `{route}`: send \
                 this request under a new {header}."
            ),
        ));
    }
    Decision::Answer(match &row.reply {
        IntentReply::Kept { headers, body } => {
            let mut headers: BTreeMap<String, String> = headers.iter().cloned().collect();
            headers.insert(REPLAYED.to_string(), "true".to_string());
            Response {
                status: row.status,
                headers,
                body: body.clone(),
            }
        }
        IntentReply::Credential => Response::error(
            409,
            &format!(
                "this request already committed under {header} `{id}`, and its reply carried \
                 a credential, which charpente shows once: send a new request under a new \
                 {header} to get a new one."
            ),
        ),
        IntentReply::TooLarge { bytes } => Response::error(
            409,
            &format!(
                "this request already committed under {header} `{id}`, and its reply was \
                 {bytes} bytes, over the {KEPT_REPLY_MAX} charpente keeps: read the result \
                 with a GET."
            ),
        ),
    })
}

/// The record of a fresh write's `reply`. It keeps the reply's headers and
/// body unless the transaction `minted` a credential (charpente shows one
/// once, and keeps it only hashed, DESIGN §8) or the reply is over
/// `KEPT_REPLY_MAX`; then it keeps the status and why.
pub(crate) fn record(
    id: &str,
    principal: Option<&str>,
    route: &str,
    fingerprint: [u8; 32],
    committed_at: WallTime,
    reply: &Response,
    minted: bool,
) -> IntentRow {
    let bytes = reply_bytes(reply);
    let kept = if minted {
        IntentReply::Credential
    } else if bytes > KEPT_REPLY_MAX {
        IntentReply::TooLarge {
            bytes: bytes as u64,
        }
    } else {
        IntentReply::Kept {
            headers: reply
                .headers
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            body: reply.body.clone(),
        }
    };
    IntentRow {
        id: row_id(principal, route, id),
        intent: id.to_string(),
        principal: principal.map(str::to_string),
        route: route.to_string(),
        fingerprint,
        committed_at,
        status: reply.status,
        reply: kept,
    }
}

/// The 400 for a write whose id header is not 1 to 255 visible ASCII bytes,
/// or that reached the wire with none: one text for both, since the edge
/// drops a non-ASCII value before the kernel can tell it from a missing one.
pub(crate) fn malformed(route: &Route) -> Response {
    let header = header_name(route);
    Response::error(
        400,
        &format!(
            "`{} {}` needs an {header} header, 1 to 255 visible ASCII characters: charpente \
             answers a re-sent write once (GUIDE §5). The generated client sends one; a \
             webhook route names its sender's id header with `intent = header(...)`.",
            route.method.name(),
            route.pattern
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::{ApiError, Ctx};
    use charpente_core::db::Tx;

    fn ok(_: &mut Tx, _: &Ctx, _: &[String], _: &[u8]) -> Result<Response, ApiError> {
        Ok(Response::error(200, "ok"))
    }

    fn route(method: Method, intent: Option<&'static str>) -> Route {
        Route {
            method,
            pattern: "/orders/:id",
            auth: false,
            public_scope: None,
            cross_tenant: false,
            name: "orders",
            glue: RouteGlue::Http(ok),
            params: &[],
            request: None,
            response: || crate::wire::TypeSchema::Null,
            error: || <ApiError as crate::wire::Wire>::reference(),
            register_types: |_| {},
            emits: &[],
            intent,
        }
    }

    fn post(id: &str) -> Request {
        Request::new(Method::Post, "/orders/1").intent(id)
    }

    const ROUTE: &str = "POST /orders/:id";
    const FP: [u8; 32] = [7; 32];

    fn body(response: &Response) -> String {
        String::from_utf8(response.body.clone()).unwrap()
    }

    fn answer(decision: Decision) -> Response {
        match decision {
            Decision::Answer(response) => response,
            Decision::Fresh => panic!("a row was found, so the record answers"),
        }
    }

    fn kept(status: u16, body: &[u8]) -> Response {
        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_string(), "application/json".to_string());
        headers.insert("x-app".to_string(), "kept".to_string());
        Response {
            status,
            headers,
            body: body.to_vec(),
        }
    }

    #[test]
    fn presented_accepts_the_boundaries() {
        let r = route(Method::Post, None);
        for id in [
            "a".to_string(),
            "a".repeat(255),
            "\u{21}".to_string(),
            "\u{7e}".to_string(),
        ] {
            assert_eq!(presented(&post(&id), &r), Presented::Valid(id.clone()));
        }
    }

    #[test]
    fn presented_rejects_past_the_boundaries() {
        let r = route(Method::Post, None);
        for id in [
            String::new(),
            "a".repeat(256),
            "a b".to_string(),
            "\u{20}".to_string(),
            "a\u{7f}".to_string(),
            "caf\u{e9}".to_string(),
        ] {
            assert_eq!(presented(&post(&id), &r), Presented::Malformed, "{id:?}");
        }
        assert_eq!(
            presented(&Request::new(Method::Post, "/orders/1"), &r),
            Presented::Absent
        );
    }

    #[test]
    fn presented_reads_the_routes_declared_header() {
        let r = route(Method::Post, Some("X-Event-Id"));
        assert_eq!(header_name(&r), "X-Event-Id");
        assert_eq!(
            presented(&post("k"), &r),
            Presented::Absent,
            "a route that names its sender's header ignores Idempotency-Key"
        );
        let request = Request::new(Method::Post, "/orders/1").header("X-Event-Id", "evt_1");
        assert_eq!(
            presented(&request, &r),
            Presented::Valid("evt_1".to_string())
        );
        assert_eq!(header_name(&route(Method::Post, None)), DEFAULT_HEADER);
    }

    #[test]
    fn a_write_is_an_http_route_that_is_not_a_get() {
        for method in [Method::Post, Method::Put, Method::Patch, Method::Delete] {
            assert!(is_write(&route(method, None)), "{method:?}");
        }
        assert!(!is_write(&route(Method::Get, None)));
        let mut sse = route(Method::Post, None);
        sse.glue = RouteGlue::Sse(|_, _, _, _| unreachable!());
        assert!(!is_write(&sse), "an SSE route is never a write");
    }

    #[test]
    fn row_id_differs_in_each_of_its_three_parts() {
        let base = row_id(Some("alice"), ROUTE, "k1");
        assert_ne!(base, row_id(Some("bob"), ROUTE, "k1"), "principal");
        assert_ne!(base, row_id(None, ROUTE, "k1"), "no principal");
        assert_ne!(base, row_id(Some(""), ROUTE, "k1"), "an empty subject");
        assert_ne!(
            base,
            row_id(Some("alice"), "PUT /orders/:id", "k1"),
            "route"
        );
        assert_ne!(base, row_id(Some("alice"), ROUTE, "k2"), "id");
        assert_eq!(base, row_id(Some("alice"), ROUTE, "k1"), "a pure function");
    }

    #[test]
    fn row_id_length_prefixes_its_parts() {
        assert_ne!(row_id(None, "ab", "c"), row_id(None, "a", "bc"));
        assert_ne!(row_id(Some("ab"), "c", "k"), row_id(Some("a"), "bc", "k"));
        assert_ne!(row_id(None, "", "k"), row_id(Some(""), "", "k"));
    }

    #[test]
    fn row_id_is_the_first_eight_bytes_of_the_digest_big_endian() {
        let mut hasher = Sha256::new();
        hasher.update([1u8]);
        hasher.update(5u64.to_be_bytes());
        hasher.update(b"alice");
        hasher.update(16u64.to_be_bytes());
        hasher.update(ROUTE.as_bytes());
        hasher.update(2u64.to_be_bytes());
        hasher.update(b"k1");
        let digest: [u8; 32] = hasher.finalize().into();
        let expected = u64::from_be_bytes(digest[..8].try_into().unwrap());
        assert_eq!(row_id(Some("alice"), ROUTE, "k1").raw(), expected);
    }

    #[test]
    fn fingerprint_covers_method_path_query_and_body() {
        let base = Request::new(Method::Post, "/orders/1?a=1").json(&"x");
        let fp = fingerprint(&base);
        let mut method = base.clone();
        method.method = Method::Put;
        let path = Request::new(Method::Post, "/orders/2?a=1").json(&"x");
        let query = Request::new(Method::Post, "/orders/1?a=2").json(&"x");
        let body = Request::new(Method::Post, "/orders/1?a=1").json(&"y");
        for (what, other) in [
            ("method", method),
            ("path", path),
            ("query", query),
            ("body", body),
        ] {
            assert_ne!(fp, fingerprint(&other), "{what}");
        }
        let mut headers = base.clone();
        headers.headers.insert("x-other".into(), "1".into());
        assert_eq!(
            fp,
            fingerprint(&headers),
            "headers are not the request's identity"
        );
        let ab = Request::new(Method::Post, "/ab");
        let mut a = Request::new(Method::Post, "/a");
        a.body = b"b".to_vec();
        assert_ne!(fingerprint(&ab), fingerprint(&a), "length-prefixed");
    }

    #[test]
    fn decide_without_a_row_runs_the_handler() {
        assert!(matches!(
            decide(None, DEFAULT_HEADER, "k1", None, ROUTE, &FP),
            Decision::Fresh
        ));
    }

    #[test]
    fn decide_replays_a_matching_rows_reply_marked() {
        let first = kept(200, b"\"1\"");
        let row = record(
            "k1",
            Some("alice"),
            ROUTE,
            FP,
            WallTime::UNIX_EPOCH,
            &first,
            false,
        );
        let replay = answer(decide(
            Some(&row),
            DEFAULT_HEADER,
            "k1",
            Some("alice"),
            ROUTE,
            &FP,
        ));
        assert_eq!(replay.status, 200);
        assert_eq!(replay.body, first.body);
        let mut expected = first.headers.clone();
        expected.insert("idempotent-replayed".into(), "true".into());
        assert_eq!(replay.headers, expected);
    }

    #[test]
    fn decide_refuses_another_fingerprint_with_422() {
        let row = record(
            "k1",
            None,
            ROUTE,
            FP,
            WallTime::UNIX_EPOCH,
            &kept(200, b"1"),
            false,
        );
        let refusal = answer(decide(
            Some(&row),
            DEFAULT_HEADER,
            "k1",
            None,
            ROUTE,
            &[8; 32],
        ));
        assert_eq!(refusal.status, 422);
        assert_eq!(
            body(&refusal),
            "{\"error\":\"Idempotency-Key `k1` was already used for a different request to \
             `POST /orders/:id`: send this request under a new Idempotency-Key.\"}"
        );
    }

    #[test]
    fn decide_refuses_a_withheld_credential_with_409() {
        let row = record(
            "k1",
            None,
            ROUTE,
            FP,
            WallTime::UNIX_EPOCH,
            &kept(200, b"1"),
            true,
        );
        assert_eq!(row.reply, IntentReply::Credential);
        let refusal = answer(decide(Some(&row), "X-Event-Id", "k1", None, ROUTE, &FP));
        assert_eq!(refusal.status, 409);
        assert_eq!(
            body(&refusal),
            "{\"error\":\"this request already committed under X-Event-Id `k1`, and its reply \
             carried a credential, which charpente shows once: send a new request under a new \
             X-Event-Id to get a new one.\"}"
        );
    }

    #[test]
    fn decide_refuses_a_reply_too_large_to_keep_with_409_and_its_size() {
        let big = Response {
            status: 200,
            headers: BTreeMap::new(),
            body: vec![b'x'; 70_000],
        };
        let row = record("k1", None, ROUTE, FP, WallTime::UNIX_EPOCH, &big, false);
        assert_eq!(row.reply, IntentReply::TooLarge { bytes: 70_000 });
        let refusal = answer(decide(Some(&row), DEFAULT_HEADER, "k1", None, ROUTE, &FP));
        assert_eq!(refusal.status, 409);
        assert_eq!(
            body(&refusal),
            "{\"error\":\"this request already committed under Idempotency-Key `k1`, and its \
             reply was 70000 bytes, over the 65536 charpente keeps: read the result with a \
             GET.\"}"
        );
    }

    #[test]
    fn decide_refuses_a_slot_shared_with_another_identity_with_409() {
        let row = record(
            "k1",
            Some("alice"),
            ROUTE,
            FP,
            WallTime::UNIX_EPOCH,
            &kept(200, b"1"),
            false,
        );
        // As if another triple hashed to this row's slot: each field alone.
        for (id, principal, route) in [
            ("k2", Some("alice"), ROUTE),
            ("k1", Some("bob"), ROUTE),
            ("k1", None, ROUTE),
            ("k1", Some("alice"), "PUT /orders/:id"),
        ] {
            let refusal = answer(decide(
                Some(&row),
                DEFAULT_HEADER,
                id,
                principal,
                route,
                &FP,
            ));
            assert_eq!(refusal.status, 409, "{id} {principal:?} {route}");
            assert_eq!(
                body(&refusal),
                format!(
                    "{{\"error\":\"Idempotency-Key `{id}` shares a record slot with another \
                     request's id: send this request under a new Idempotency-Key.\"}}"
                )
            );
        }
    }

    #[test]
    fn record_keeps_a_reply_of_exactly_the_cap_and_withholds_one_byte_more() {
        // Headers count: "content-type" + "application/json" + "x-app" + "kept"
        // is 37 bytes, so the body takes the rest of the cap.
        let at_cap = kept(200, &vec![b'x'; KEPT_REPLY_MAX - 37]);
        let row = record("k1", None, ROUTE, FP, WallTime::UNIX_EPOCH, &at_cap, false);
        assert_eq!(
            row.reply,
            IntentReply::Kept {
                headers: at_cap
                    .headers
                    .iter()
                    .map(|(n, v)| (n.clone(), v.clone()))
                    .collect(),
                body: at_cap.body.clone(),
            }
        );
        let over = kept(200, &vec![b'x'; KEPT_REPLY_MAX - 36]);
        let row = record("k1", None, ROUTE, FP, WallTime::UNIX_EPOCH, &over, false);
        assert_eq!(row.reply, IntentReply::TooLarge { bytes: 65_537 });
        let bare = Response {
            status: 200,
            headers: BTreeMap::new(),
            body: vec![b'x'; 65_536],
        };
        assert!(matches!(
            record("k1", None, ROUTE, FP, WallTime::UNIX_EPOCH, &bare, false).reply,
            IntentReply::Kept { .. }
        ));
    }

    #[test]
    fn record_withholds_a_minted_credential_whatever_its_size() {
        let small = kept(200, b"\"tok\"");
        let row = record("k1", None, ROUTE, FP, WallTime::UNIX_EPOCH, &small, true);
        assert_eq!(row.reply, IntentReply::Credential);
        assert_eq!(row.status, 200);
    }

    #[test]
    fn record_fills_its_row_from_the_request() {
        let at = WallTime::from_nanos(42);
        let row = record("k1", Some("alice"), ROUTE, FP, at, &kept(201, b"1"), false);
        assert_eq!(row.id, row_id(Some("alice"), ROUTE, "k1"));
        assert_eq!(row.intent, "k1");
        assert_eq!(row.principal.as_deref(), Some("alice"));
        assert_eq!(row.route, ROUTE);
        assert_eq!(row.fingerprint, FP);
        assert_eq!(row.committed_at, at);
        assert_eq!(row.status, 201);
    }

    #[test]
    fn malformed_names_the_route_the_header_and_the_fix() {
        let refusal = malformed(&route(Method::Post, None));
        assert_eq!(refusal.status, 400);
        assert_eq!(
            body(&refusal),
            "{\"error\":\"`POST /orders/:id` needs an Idempotency-Key header, 1 to 255 visible \
             ASCII characters: charpente answers a re-sent write once (GUIDE §5). The generated \
             client sends one; a webhook route names its sender's id header with `intent = \
             header(...)`.\"}"
        );
        let declared = malformed(&route(Method::Put, Some("X-Event-Id")));
        assert!(
            body(&declared)
                .starts_with("{\"error\":\"`PUT /orders/:id` needs an X-Event-Id header, 1 to 255"),
            "{}",
            body(&declared)
        );
    }
}
