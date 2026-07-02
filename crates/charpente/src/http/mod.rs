//! The deterministic HTTP kernel (DESIGN.md §3; spec
//! docs/superpowers/specs/2026-07-02-http-layer-design.md): typed requests,
//! routing, decoding, sessions, and transact-wrapping — all inside the
//! simulation boundary. The wire lives in `crate::serve` (hyper); this
//! module never touches a socket. `App::handle(Request) -> Response` is the
//! whole surface, which is exactly what the sim drives.

mod app;
mod router;

pub use app::{App, AppConfig, Ctx, Session};
pub use router::{Glue, Route, RouteDef, RouteMatch, Router};

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
