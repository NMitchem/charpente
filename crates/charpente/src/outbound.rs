//! The outbound job surface (outbound-effects spec §5, §9): what `call` builds
//! (`Call`), what `settle` reads (`Reply`), and the per-host configuration an
//! `App` declares (`Host`). The kernel checks every built `Call` against its
//! host's `Host` entry before a start (`check`); each refusal is an attempt
//! whose `last_error` names the rule.

use charpente_core::io::{OutboundCall, OutboundReply};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::kernel::JobError;

pub use charpente_core::io::{HttpMethod, IdempotencyKey};

/// The request cap's default: the inbound body cap.
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 1 << 20;
/// The reply cap's default.
pub const DEFAULT_MAX_REPLY_BYTES: usize = 4 << 20;
/// Half of `MAX_RECORD_BYTES` (16 MiB), so a reply `settle` stores whole
/// leaves room for the rest of its record (spec §9).
pub const MAX_CAP_BYTES: usize = 8 << 20;

/// The headers the edge sets itself (spec §5).
const EDGE_HEADERS: [&str; 4] = ["host", "content-length", "transfer-encoding", "connection"];

/// One outbound request, as `call` builds it. The host is not here: it is the
/// job's declaration, never runtime data.
pub struct Call {
    method: HttpMethod,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Call {
    fn new(method: HttpMethod, path: impl Into<String>) -> Call {
        Call {
            method,
            path: path.into(),
            headers: Vec::new(),
            body: Vec::new(),
        }
    }
    /// A GET to `path` on the job's declared host; the path must begin with '/'.
    pub fn get(path: impl Into<String>) -> Call {
        Call::new(HttpMethod::Get, path)
    }
    /// A POST to `path` on the job's declared host; the path must begin with '/'.
    pub fn post(path: impl Into<String>) -> Call {
        Call::new(HttpMethod::Post, path)
    }
    /// A PUT to `path` on the job's declared host; the path must begin with '/'.
    pub fn put(path: impl Into<String>) -> Call {
        Call::new(HttpMethod::Put, path)
    }
    /// A PATCH to `path` on the job's declared host; the path must begin with '/'.
    pub fn patch(path: impl Into<String>) -> Call {
        Call::new(HttpMethod::Patch, path)
    }
    /// A DELETE to `path` on the job's declared host; the path must begin with '/'.
    pub fn delete(path: impl Into<String>) -> Call {
        Call::new(HttpMethod::Delete, path)
    }

    /// Names are matched case-insensitively and sent lowercase. `content-type`
    /// is a singleton: setting it replaces an earlier one (including the one
    /// `json` or `form` set), so a call never carries two.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Call {
        let name = name.into().to_ascii_lowercase();
        if name == "content-type" {
            self.headers.retain(|(n, _)| n != "content-type");
        }
        self.headers.push((name, value.into()));
        self
    }

    /// Serializes `value` as the body and sets `content-type: application/json`,
    /// replacing any earlier content-type (a later `header("content-type", ..)`
    /// replaces this one).
    /// A value serde cannot serialize (a map with non-string keys) panics,
    /// and inside `call` a panic is an attempt (plan amendment A16).
    pub fn json<T: Serialize>(mut self, value: &T) -> Call {
        self.body = serde_json::to_vec(value)
            .unwrap_or_else(|e| panic!("Call::json: the value does not serialize as JSON: {e}"));
        self.header("content-type", "application/json")
    }

    /// An `application/x-www-form-urlencoded` body; its content-type replaces
    /// any earlier one, as `json`'s does.
    pub fn form<V: AsRef<str>>(mut self, pairs: &[(&str, V)]) -> Call {
        let mut out = String::new();
        for (i, (k, v)) in pairs.iter().enumerate() {
            if i > 0 {
                out.push('&');
            }
            form_encode(k, &mut out);
            out.push('=');
            form_encode(v.as_ref(), &mut out);
        }
        self.body = out.into_bytes();
        self.header("content-type", "application/x-www-form-urlencoded")
    }

    /// The raw body, with no content-type: set one with `header` if the remote needs it.
    pub fn body(mut self, bytes: Vec<u8>) -> Call {
        self.body = bytes;
        self
    }

    #[cfg_attr(not(test), expect(dead_code, reason = "first caller: Task 10"))]
    pub(crate) fn into_wire(self, host: &'static str, key: IdempotencyKey) -> OutboundCall {
        OutboundCall {
            host,
            method: self.method,
            path: self.path,
            headers: self.headers,
            body: self.body,
            key,
        }
    }
}

fn form_encode(s: &str, out: &mut String) {
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'*' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
}

/// A reply of any status, as `settle` receives it. A 3xx is a reply too: the
/// edge never follows a redirect (spec §9).
pub struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Reply {
    #[cfg_attr(not(test), expect(dead_code, reason = "first caller: Task 11"))]
    pub(crate) fn from_wire(reply: OutboundReply) -> Reply {
        Reply {
            status: reply.status,
            headers: reply.headers,
            body: reply.body,
        }
    }
    /// The HTTP status code.
    pub fn status(&self) -> u16 {
        self.status
    }
    /// The first header named `name`, case-insensitively.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
    /// The reply body, at most the host's reply cap.
    pub fn body(&self) -> &[u8] {
        &self.body
    }
    /// `JobError::App` when the body is not `T`'s JSON — an attempt, as a
    /// `settle` that cannot read its reply should be.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, JobError> {
        serde_json::from_slice(&self.body)
            .map_err(|e| JobError::app(format!("reply body is not the expected JSON: {e}")))
    }
}

/// A credential the edge attaches per request, by environment-variable NAME
/// (spec §9): values never enter the log, the archive, `call`, the sim or
/// `describe`.
pub(crate) enum Credential {
    Bearer(&'static str),
    Header(String, &'static str),
    Basic(&'static str, &'static str),
}

/// One declared outside service (spec §9, R9): an `App` carries exactly one
/// per host its mounted jobs call.
pub struct Host {
    name: &'static str,
    credentials: Vec<Credential>,
    max_request: usize,
    max_reply: usize,
}

impl Host {
    /// A declared host: a bare DNS name like `api.stripe.com`.
    ///
    /// # Panics
    /// A contract panic when `name` carries a scheme, userinfo, path or port, or is
    /// not a lowercase DNS name.
    pub fn new(name: &'static str) -> Host {
        if let Some(problem) = host_name_problem(name) {
            panic!(
                "contract: Host::new(\"{name}\") — the host {problem}; a host is a bare DNS name \
                 like api.stripe.com (an operator points it elsewhere with --outbound-base, GUIDE §5)"
            );
        }
        Host {
            name,
            credentials: Vec::new(),
            max_request: DEFAULT_MAX_REQUEST_BYTES,
            max_reply: DEFAULT_MAX_REPLY_BYTES,
        }
    }
    /// `authorization: Bearer $VAR`.
    ///
    /// # Panics
    /// A contract panic when this host already injects `authorization`.
    pub fn bearer_env(mut self, var: &'static str) -> Host {
        self.claim("bearer_env", "authorization", var);
        self.credentials.push(Credential::Bearer(var));
        self
    }
    /// `<name>: $VAR`.
    ///
    /// # Panics
    /// A contract panic when `name` is not an HTTP token, is a header the edge
    /// sets, or is a header this host already injects.
    pub fn header_env(mut self, name: &str, var: &'static str) -> Host {
        let lower = name.to_ascii_lowercase();
        if !is_token(&lower) {
            panic!(
                "contract: Host::header_env({name:?}, ..) — the name is not an HTTP token; a name is \
                 letters, digits and !#$%&'*+-.^_`|~"
            );
        }
        if EDGE_HEADERS.contains(&lower.as_str()) {
            panic!(
                "contract: Host::header_env({name:?}, ..) — {lower:?} is a header the edge sets \
                 itself; pick another name"
            );
        }
        self.claim("header_env", &lower, var);
        self.credentials.push(Credential::Header(lower, var));
        self
    }
    /// `authorization: Basic base64($USER_VAR:$PASS_VAR)`.
    ///
    /// # Panics
    /// A contract panic when this host already injects `authorization`.
    pub fn basic_env(mut self, user_var: &'static str, pass_var: &'static str) -> Host {
        self.claim(
            "basic_env",
            "authorization",
            &format!("{user_var} and {pass_var}"),
        );
        self.credentials.push(Credential::Basic(user_var, pass_var));
        self
    }
    /// The request body cap (default 1 MiB).
    ///
    /// # Panics
    /// A contract panic when `n` exceeds `MAX_CAP_BYTES`.
    pub fn max_request_bytes(mut self, n: usize) -> Host {
        assert!(
            n <= MAX_CAP_BYTES,
            "contract: Host::max_request_bytes({n}) exceeds the {MAX_CAP_BYTES}-byte bound — both \
             caps share the bound, half of a log record's 16 MiB cap, so a reply `settle` stores \
             whole leaves room for the rest of its record (outbound-effects spec §9); pass a value \
             of at most MAX_CAP_BYTES ({MAX_CAP_BYTES})"
        );
        self.max_request = n;
        self
    }
    /// The reply body cap (default 4 MiB).
    ///
    /// # Panics
    /// A contract panic when `n` exceeds `MAX_CAP_BYTES`.
    pub fn max_reply_bytes(mut self, n: usize) -> Host {
        assert!(
            n <= MAX_CAP_BYTES,
            "contract: Host::max_reply_bytes({n}) exceeds the {MAX_CAP_BYTES}-byte bound — half of \
             a log record's 16 MiB cap, so a reply `settle` stores whole leaves room for the rest \
             of its record (outbound-effects spec §9); pass a value of at most MAX_CAP_BYTES \
             ({MAX_CAP_BYTES})"
        );
        self.max_reply = n;
        self
    }
    /// One credential per injected header: a second one would send two.
    fn claim(&self, method: &str, header: &str, vars: &str) {
        if let Some((_, other)) = self.injects().iter().find(|(n, _)| n == header) {
            panic!(
                "contract: Host::{method}(..) — this host already injects {header:?} from {other}, \
                 and {vars} would inject it again; keep one credential per header"
            );
        }
    }
    #[expect(dead_code, reason = "first caller: Task 9")]
    pub(crate) fn name(&self) -> &'static str {
        self.name
    }
    #[expect(dead_code, reason = "first caller: Task 9")]
    pub(crate) fn max_request(&self) -> usize {
        self.max_request
    }
    #[expect(dead_code, reason = "first caller: Task 9")]
    pub(crate) fn max_reply(&self) -> usize {
        self.max_reply
    }
    #[expect(dead_code, reason = "first caller: Task 14")]
    pub(crate) fn credentials(&self) -> &[Credential] {
        &self.credentials
    }
    /// Every variable this host reads, in declaration order.
    #[expect(dead_code, reason = "first caller: Task 9")]
    pub(crate) fn env_vars(&self) -> Vec<&'static str> {
        self.credentials
            .iter()
            .flat_map(|c| match c {
                Credential::Bearer(v) | Credential::Header(_, v) => vec![*v],
                Credential::Basic(u, p) => vec![*u, *p],
            })
            .collect()
    }
    /// The header each credential injects, with the variable(s) that fill it.
    fn injects(&self) -> Vec<(String, String)> {
        self.credentials
            .iter()
            .map(|c| match c {
                Credential::Bearer(v) => ("authorization".to_string(), v.to_string()),
                Credential::Header(n, v) => (n.clone(), v.to_string()),
                Credential::Basic(u, p) => ("authorization".to_string(), format!("{u} and {p}")),
            })
            .collect()
    }
    #[expect(dead_code, reason = "first caller: Task 9")]
    pub(crate) fn injected_headers(&self) -> Vec<String> {
        self.injects().into_iter().map(|(h, _)| h).collect()
    }
}

/// Why `name` is not a bare host, or `None` (spec §5). `#[job]` checks the
/// same rule at compile time; charpente-macros cannot link this crate, so the
/// two are pinned to agree by the compile-fail fixtures and this module's tests.
pub(crate) fn host_name_problem(name: &str) -> Option<&'static str> {
    if name.is_empty() {
        return Some("is empty");
    }
    if name.contains("://") {
        return Some("carries a scheme");
    }
    if name.contains('@') {
        return Some("carries userinfo");
    }
    if name.contains('/') {
        return Some("carries a path");
    }
    if name.contains('[') {
        return Some("is an IPv6 literal");
    }
    if name.contains(':') {
        return Some("carries a port");
    }
    if name.bytes().any(|b| b.is_ascii_uppercase()) {
        return Some("has an uppercase letter");
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
    {
        return Some("holds a character a DNS name cannot");
    }
    if name.ends_with('.') {
        return Some("ends with a dot");
    }
    let labels: Vec<&str> = name.split('.').collect();
    if labels.iter().any(|l| l.is_empty()) {
        return Some("has an empty label");
    }
    if labels
        .iter()
        .any(|l| l.starts_with('-') || l.ends_with('-'))
    {
        return Some("has a label starting or ending with '-'");
    }
    if labels.len() == 4 && labels.iter().all(|l| l.bytes().all(|b| b.is_ascii_digit())) {
        return Some("is an IPv4 literal");
    }
    None
}

fn is_token(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

/// The rules a built `Call` must satisfy (spec §5, plan amendment A12),
/// checked before a start. The edge would refuse most of these too, but only
/// the kernel's refusal is the same in production and the sim.
#[cfg_attr(not(test), expect(dead_code, reason = "first caller: Task 10"))]
pub(crate) fn check(call: &Call, host: &Host) -> Result<(), String> {
    let h = host.name;
    let before_query = call.path.split('?').next().unwrap_or_default();
    if !call.path.starts_with('/') || call.path.starts_with("//") || before_query.contains("://") {
        return Err(format!(
            "outbound {h}: the call's path {:?} must begin with '/' and name no scheme or \
             authority — the host is the job's declaration, outbound = \"{h}\"",
            call.path
        ));
    }
    if let Some(c) = call
        .path
        .chars()
        .find(|c| !('\x21'..='\x7e').contains(c) || "\"#<>`".contains(*c))
    {
        return Err(format!(
            "outbound {h}: the call's path {:?} holds {c:?}, which the edge cannot send verbatim — \
             percent-encode it",
            call.path
        ));
    }
    let injects = host.injects();
    for (name, value) in &call.headers {
        if !is_token(name) {
            return Err(format!(
                "outbound {h}: the call's header name {name:?} is not an HTTP token — a name is \
                 letters, digits and !#$%&'*+-.^_`|~"
            ));
        }
        if value.bytes().any(|b| (b < 0x20 && b != b'\t') || b == 0x7f) {
            return Err(format!(
                "outbound {h}: the call's header {name:?} has a control character in its value \
                 — refused before sending, since a CR or LF there would forge a header"
            ));
        }
        if EDGE_HEADERS.contains(&name.as_str()) {
            return Err(format!(
                "outbound {h}: the call sets {name:?}, which the edge sets — remove it from call"
            ));
        }
        if let Some((_, var)) = injects.iter().find(|(n, _)| n == name) {
            return Err(format!(
                "outbound {h}: the call sets {name:?}, which this host's Host entry injects from \
                 {var} — remove it from call; a credential never enters app code"
            ));
        }
    }
    if call.body.len() > host.max_request {
        return Err(format!(
            "outbound {h}: the request body is {} bytes, over the {}-byte request cap — raise it \
             with Host::max_request_bytes",
            call.body.len(),
            host.max_request
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host() -> Host {
        Host::new("pay.test")
            .bearer_env("PAY_KEY")
            .header_env("X-Account", "PAY_ACCOUNT")
    }

    #[test]
    fn a_call_becomes_a_wire_call_with_lowercased_header_names() {
        let key = IdempotencyKey::from_bytes([1; 16]);
        let wire = Call::post("/v1/charges")
            .header("Idempotency-Key", key.as_str())
            .body(b"x".to_vec())
            .into_wire("pay.test", key);
        assert_eq!(wire.host, "pay.test");
        assert_eq!(wire.method, HttpMethod::Post);
        assert_eq!(wire.path, "/v1/charges");
        assert_eq!(
            wire.headers,
            vec![("idempotency-key".to_string(), key.as_str().to_string())]
        );
        assert_eq!(wire.body, b"x");
        assert_eq!(wire.key, key);
    }

    #[test]
    fn form_is_application_x_www_form_urlencoded() {
        let call = Call::post("/f").form(&[("amount", "10 00"), ("note", "a&b=c/é")]);
        assert_eq!(call.body, b"amount=10+00&note=a%26b%3Dc%2F%C3%A9");
        assert_eq!(
            call.headers,
            vec![(
                "content-type".to_string(),
                "application/x-www-form-urlencoded".to_string()
            )]
        );
    }

    #[test]
    fn json_sets_the_body_and_its_content_type() {
        let call = Call::put("/j").json(&serde_json::json!({ "a": 1 }));
        assert_eq!(call.body, br#"{"a":1}"#);
        assert_eq!(
            call.headers,
            vec![("content-type".to_string(), "application/json".to_string())]
        );
    }

    #[test]
    fn the_call_rules_name_each_violation() {
        let h = host();
        let cases = [
            (
                Call::get("v1"),
                "outbound pay.test: the call's path \"v1\" must begin with '/' and name no scheme or authority — the host is the job's declaration, outbound = \"pay.test\"",
            ),
            (
                Call::get("//evil.test/x"),
                "outbound pay.test: the call's path \"//evil.test/x\" must begin with '/' and name no scheme or authority — the host is the job's declaration, outbound = \"pay.test\"",
            ),
            (
                Call::get("/a b"),
                "outbound pay.test: the call's path \"/a b\" holds ' ', which the edge cannot send verbatim — percent-encode it",
            ),
            (
                Call::get("/s?q=a#b"),
                "outbound pay.test: the call's path \"/s?q=a#b\" holds '#', which the edge cannot send verbatim — percent-encode it",
            ),
            (
                Call::get("/a\r\nX: y"),
                "outbound pay.test: the call's path \"/a\\r\\nX: y\" holds '\\r', which the edge cannot send verbatim — percent-encode it",
            ),
            (
                Call::get("/https://evil.test"),
                "outbound pay.test: the call's path \"/https://evil.test\" must begin with '/' and name no scheme or authority — the host is the job's declaration, outbound = \"pay.test\"",
            ),
            (
                Call::get("/x").header("x-a", "1\x7f"),
                "outbound pay.test: the call's header \"x-a\" has a control character in its value — refused before sending, since a CR or LF there would forge a header",
            ),
            (
                Call::get("/x").header("authorization", "Basic x"),
                "outbound pay.test: the call sets \"authorization\", which this host's Host entry injects from PAY_KEY — remove it from call; a credential never enters app code",
            ),
            (
                Call::get("/x").header("x-a", "1\r\nhost: evil.test"),
                "outbound pay.test: the call's header \"x-a\" has a control character in its value — refused before sending, since a CR or LF there would forge a header",
            ),
            (
                Call::get("/x").header("bad name", "1"),
                "outbound pay.test: the call's header name \"bad name\" is not an HTTP token — a name is letters, digits and !#$%&'*+-.^_`|~",
            ),
            (
                Call::get("/x").header("Content-Length", "1"),
                "outbound pay.test: the call sets \"content-length\", which the edge sets — remove it from call",
            ),
            (
                Call::get("/x").header("authorization", "Bearer x"),
                "outbound pay.test: the call sets \"authorization\", which this host's Host entry injects from PAY_KEY — remove it from call; a credential never enters app code",
            ),
            (
                Call::get("/x").header("x-account", "1"),
                "outbound pay.test: the call sets \"x-account\", which this host's Host entry injects from PAY_ACCOUNT — remove it from call; a credential never enters app code",
            ),
            (
                Call::post("/x").body(vec![0; (1 << 20) + 1]),
                "outbound pay.test: the request body is 1048577 bytes, over the 1048576-byte request cap — raise it with Host::max_request_bytes",
            ),
        ];
        for (call, expected) in cases {
            assert_eq!(check(&call, &h).unwrap_err(), expected);
        }
        assert_eq!(
            check(
                &Call::get("/v1/x?y=1%20").header("idempotency-key", "k"),
                &h
            ),
            Ok(())
        );
        assert_eq!(
            check(&Call::get("/oauth?redirect_uri=https://x/cb"), &h),
            Ok(())
        );
        assert_eq!(check(&Call::get("/!~"), &h), Ok(()));
        assert_eq!(check(&Call::get("/x").header("x-a", "a\tb"), &h), Ok(()));
    }

    #[test]
    fn a_basic_host_injects_authorization_and_names_both_variables() {
        let h = Host::new("pay.test").basic_env("PAY_USER", "PAY_PASS");
        assert_eq!(
            check(&Call::get("/x").header("authorization", "x"), &h).unwrap_err(),
            "outbound pay.test: the call sets \"authorization\", which this host's Host entry injects from PAY_USER and PAY_PASS — remove it from call; a credential never enters app code"
        );
    }

    #[test]
    fn the_body_cap_is_inclusive() {
        let h = Host::new("pay.test").max_request_bytes(4);
        assert_eq!(check(&Call::post("/x").body(vec![0; 4]), &h), Ok(()));
        assert!(check(&Call::post("/x").body(vec![0; 5]), &h).is_err());
        let _ = Host::new("pay.test")
            .max_request_bytes(MAX_CAP_BYTES)
            .max_reply_bytes(MAX_CAP_BYTES);
    }

    #[test]
    fn check_agrees_with_the_uri_parser_on_every_visible_byte() {
        let h = host();
        for b in 0x21u8..=0x7e {
            let c = b as char;
            // hyper takes a backtick in a query and a quote in a path; the one rule refuses both everywhere.
            for (path, exact) in [
                (format!("/a{c}b"), c != '"'),
                (format!("/a?q={c}"), c != '`'),
            ] {
                let verbatim = format!("https://h.test{path}")
                    .parse::<hyper::Uri>()
                    .map(|u| u.path_and_query().unwrap().as_str() == path)
                    .unwrap_or(false);
                let admitted = check(&Call::get(path.clone()), &h).is_ok();
                assert!(
                    !admitted || verbatim,
                    "{path:?} admitted but the edge would not send it verbatim"
                );
                assert!(
                    !exact || admitted == verbatim,
                    "{path:?}: check {admitted}, uri {verbatim}"
                );
            }
        }
    }

    #[test]
    fn content_type_is_a_singleton() {
        let ct = |c: &Call| {
            c.headers
                .iter()
                .filter(|(n, _)| n == "content-type")
                .map(|(_, v)| v.clone())
                .collect::<Vec<_>>()
        };
        let c = Call::patch("/p")
            .json(&1)
            .header("Content-Type", "application/merge-patch+json");
        assert_eq!(ct(&c), vec!["application/merge-patch+json"]);
        let c = Call::patch("/p").form(&[("a", "b")]).json(&1);
        assert_eq!(ct(&c), vec!["application/json"]);
        let c = Call::patch("/p")
            .header("content-type", "text/plain")
            .json(&1);
        assert_eq!(ct(&c), vec!["application/json"]);
    }

    #[test]
    fn header_env_refuses_what_the_edge_or_a_sibling_would_break() {
        let r = |f: fn() -> Host| {
            std::panic::catch_unwind(f)
                .err()
                .and_then(|b| b.downcast::<String>().ok())
                .map(|b| *b)
                .unwrap_or_default()
        };
        assert_eq!(
            r(|| Host::new("p.test").header_env("x tok", "V")),
            "contract: Host::header_env(\"x tok\", ..) — the name is not an HTTP token; a name is letters, digits and !#$%&'*+-.^_`|~"
        );
        assert_eq!(
            r(|| Host::new("p.test").header_env("Host", "V")),
            "contract: Host::header_env(\"Host\", ..) — \"host\" is a header the edge sets itself; pick another name"
        );
        assert_eq!(
            r(|| Host::new("p.test")
                .bearer_env("A")
                .header_env("Authorization", "B")),
            "contract: Host::header_env(..) — this host already injects \"authorization\" from A, and B would inject it again; keep one credential per header"
        );
        assert_eq!(
            r(|| Host::new("p.test")
                .header_env("authorization", "A")
                .bearer_env("B")),
            "contract: Host::bearer_env(..) — this host already injects \"authorization\" from A, and B would inject it again; keep one credential per header"
        );
        assert_eq!(
            r(|| Host::new("p.test").bearer_env("A").basic_env("U", "P")),
            "contract: Host::basic_env(..) — this host already injects \"authorization\" from A, and U and P would inject it again; keep one credential per header"
        );
    }

    #[test]
    #[should_panic(
        expected = "contract: Host::max_request_bytes(9437184) exceeds the 8388608-byte bound — both caps share the bound, half of a log record's 16 MiB cap, so a reply `settle` stores whole leaves room for the rest of its record (outbound-effects spec §9); pass a value of at most MAX_CAP_BYTES (8388608)"
    )]
    fn a_request_cap_past_8_mib_is_a_contract_panic() {
        let _ = Host::new("pay.test").max_request_bytes(9 << 20);
    }

    #[test]
    fn a_host_name_is_a_bare_dns_name() {
        assert_eq!(
            host_name_problem("API.stripe.com"),
            Some("has an uppercase letter")
        );
        assert_eq!(
            host_name_problem("api.stripe.com."),
            Some("ends with a dot")
        );
        assert_eq!(host_name_problem("a..b"), Some("has an empty label"));
        assert_eq!(host_name_problem(".a"), Some("has an empty label"));
        assert_eq!(
            host_name_problem("-a.com"),
            Some("has a label starting or ending with '-'")
        );
        assert_eq!(
            host_name_problem("a-.com"),
            Some("has a label starting or ending with '-'")
        );
        assert_eq!(host_name_problem("10.0.0.1"), Some("is an IPv4 literal"));
        assert_eq!(host_name_problem("[::1]"), Some("is an IPv6 literal"));
        assert_eq!(host_name_problem("api.stripe.com"), None);
        assert_eq!(host_name_problem(""), Some("is empty"));
        assert_eq!(
            host_name_problem("https://api.stripe.com"),
            Some("carries a scheme")
        );
        assert_eq!(
            host_name_problem("user@api.stripe.com"),
            Some("carries userinfo")
        );
        assert_eq!(
            host_name_problem("api.stripe.com/v1"),
            Some("carries a path")
        );
        assert_eq!(
            host_name_problem("api.stripe.com:443"),
            Some("carries a port")
        );
        assert_eq!(
            host_name_problem("api_stripe.com"),
            Some("holds a character a DNS name cannot")
        );
    }

    #[test]
    #[should_panic(
        expected = "contract: Host::max_reply_bytes(9437184) exceeds the 8388608-byte bound"
    )]
    fn a_cap_past_8_mib_is_a_contract_panic() {
        let _ = Host::new("pay.test").max_reply_bytes(9 << 20);
    }

    #[test]
    #[should_panic(
        expected = "contract: Host::new(\"https://pay.test\") — the host carries a scheme"
    )]
    fn a_host_with_a_scheme_is_a_contract_panic() {
        let _ = Host::new("https://pay.test");
    }

    #[test]
    fn a_reply_reads_headers_case_insensitively_and_bad_json_is_an_app_error() {
        let reply = Reply::from_wire(OutboundReply {
            status: 200,
            headers: vec![("Content-Type".into(), "application/json".into())],
            body: b"{\"id\":\"ch_1\"}".to_vec(),
        });
        assert_eq!(reply.status(), 200);
        assert_eq!(reply.header("content-type"), Some("application/json"));
        #[derive(serde::Deserialize)]
        struct Charge {
            id: String,
        }
        assert_eq!(reply.json::<Charge>().unwrap().id, "ch_1");
        match reply.json::<u64>() {
            Err(JobError::App(m)) => assert!(
                m.starts_with("reply body is not the expected JSON: "),
                "{m}"
            ),
            other => panic!("{other:?}"),
        }
    }
}
