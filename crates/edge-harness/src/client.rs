//! A hand-rolled blocking HTTP/1.1 client over `TcpStream` — the
//! `crates/charpente/src/serve.rs` loopback-smoke lineage, no reqwest/curl
//! dependency (this crate takes none). Each request opens a fresh connection
//! with `Connection: close` and reads to EOF, so there is no
//! `Content-Length`/chunked-decoding ambiguity to get wrong for ordinary
//! request/response bodies; `sse_tail` is the one path that stays open and
//! decodes hyper's chunked framing by hand, mirroring `serve.rs`'s own test
//! helpers. `Http` carries a session cookie (`charpente_session`, the same
//! name `crates/charpente/src/http/app.rs` checks) so a scenario can log in
//! once and reuse the session across requests.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// Decodes `body` as a bare JSON string (the shape every `#[route]`
    /// handler that returns a plain `String` — `login`, `create_key` — sends
    /// over the wire): strips the surrounding quotes and undoes `\"`/`\\`
    /// escaping. `None` if the body isn't a quoted JSON string. This crate
    /// has no JSON dependency; a full parser would be overkill for the one
    /// shape the harness's scenarios actually need to read back.
    pub fn as_json_string(&self) -> Option<String> {
        let text = std::str::from_utf8(&self.body).ok()?;
        let text = text.trim();
        let inner = text.strip_prefix('"')?.strip_suffix('"')?;
        let mut out = String::with_capacity(inner.len());
        let mut chars = inner.chars();
        while let Some(c) = chars.next() {
            if c == '\\' {
                out.push(chars.next()?);
            } else {
                out.push(c);
            }
        }
        Some(out)
    }
}

pub struct SseFrame {
    pub id: String,
    pub event: String,
    pub data: String,
}

/// A live SSE connection: the still-open reader plus whatever partial frame
/// bytes have been read but not yet parsed.
pub struct SseTail {
    reader: BufReader<TcpStream>,
    pending: String,
}

impl SseTail {
    /// Blocks for up to `timeout` for the next complete SSE frame.
    /// `Ok(None)` means the connection closed (never a timeout — a timeout
    /// with no frame is also `Ok(None)`, since this client has no way to
    /// distinguish "still connected, just quiet" from "gone" without a
    /// second read attempt the caller is better positioned to retry).
    pub fn next_frame(&mut self, timeout: Duration) -> io::Result<Option<SseFrame>> {
        self.reader.get_ref().set_read_timeout(Some(timeout))?;
        loop {
            if let Some(at) = self.pending.find("\n\n") {
                let frame_text = self.pending[..at].to_string();
                self.pending.replace_range(..at + 2, "");
                if let Some(frame) = parse_sse_frame(&frame_text) {
                    return Ok(Some(frame));
                }
                continue; // a heartbeat comment (":hb") — keep reading
            }
            match read_one_chunk(&mut self.reader) {
                Ok(Some(chunk)) => self.pending.push_str(&String::from_utf8_lossy(&chunk)),
                Ok(None) => return Ok(None),
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::TimedOut =>
                {
                    return Ok(None);
                }
                Err(e) => return Err(e),
            }
        }
    }
}

fn parse_sse_frame(text: &str) -> Option<SseFrame> {
    if text.starts_with(':') {
        return None; // heartbeat comment frame
    }
    let mut id = String::new();
    let mut event = String::new();
    let mut data = String::new();
    for line in text.lines() {
        if let Some(v) = line.strip_prefix("id: ") {
            id = v.to_string();
        } else if let Some(v) = line.strip_prefix("event: ") {
            event = v.to_string();
        } else if let Some(v) = line.strip_prefix("data: ") {
            data = v.to_string();
        }
    }
    Some(SseFrame { id, event, data })
}

/// Reads one chunked-transfer-encoding chunk (serve.rs's SSE body is always
/// chunked — no `Content-Length` on an unbounded stream). `None` on the
/// terminal zero-length chunk or a closed connection.
fn read_one_chunk(reader: &mut BufReader<TcpStream>) -> io::Result<Option<Vec<u8>>> {
    let mut size_line = String::new();
    if reader.read_line(&mut size_line)? == 0 {
        return Ok(None);
    }
    let size = usize::from_str_radix(size_line.trim(), 16)
        .map_err(|e| io::Error::other(format!("malformed chunk size {size_line:?}: {e}")))?;
    if size == 0 {
        return Ok(None);
    }
    let mut data = vec![0u8; size];
    reader.read_exact(&mut data)?;
    let mut crlf = [0u8; 2];
    reader.read_exact(&mut crlf)?;
    Ok(Some(data))
}

/// A client bound to one node address. Not thread-safe (one connection at a
/// time) and not meant to be — scenarios drive one client per logical actor.
pub struct Http {
    addr: String,
    session: Option<String>,
}

impl Http {
    pub fn new(addr: impl Into<String>) -> Http {
        Http {
            addr: addr.into(),
            session: None,
        }
    }

    /// Stores `token` as this client's session cookie — call with the raw
    /// token read back from a login response (`HttpResponse::as_json_string`).
    /// Every subsequent request attaches it as `Cookie: charpente_session=…`.
    pub fn set_session(&mut self, token: impl Into<String>) {
        self.session = Some(token.into());
    }

    pub fn post(&mut self, path: &str, body: &str) -> io::Result<HttpResponse> {
        self.request("POST", path, Some(body.as_bytes()))
    }

    pub fn get(&mut self, path: &str) -> io::Result<HttpResponse> {
        self.request("GET", path, None)
    }

    fn request(
        &mut self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> io::Result<HttpResponse> {
        let mut stream = TcpStream::connect(&self.addr)?;
        let mut head =
            format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
        if let Some(session) = &self.session {
            head.push_str(&format!("Cookie: charpente_session={session}\r\n"));
        }
        if let Some(body) = body {
            head.push_str("Content-Type: application/json\r\n");
            head.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        head.push_str("\r\n");
        stream.write_all(head.as_bytes())?;
        if let Some(body) = body {
            stream.write_all(body)?;
        }
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw)?;
        parse_response(&raw)
    }

    /// Opens `path` as an SSE subscription (optionally resuming from
    /// `last_event_id`), reads the opening status/headers, and returns a
    /// live tail the caller polls with `SseTail::next_frame`.
    pub fn sse_tail(&mut self, path: &str, last_event_id: Option<&str>) -> io::Result<SseTail> {
        let mut stream = TcpStream::connect(&self.addr)?;
        let mut head = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n");
        if let Some(session) = &self.session {
            head.push_str(&format!("Cookie: charpente_session={session}\r\n"));
        }
        if let Some(id) = last_event_id {
            head.push_str(&format!("Last-Event-ID: {id}\r\n"));
        }
        head.push_str("\r\n");
        stream.write_all(head.as_bytes())?;

        let mut reader = BufReader::new(stream);
        let (status, _headers) = read_status_and_headers(&mut reader)?;
        if status != 200 {
            return Err(io::Error::other(format!(
                "sse_tail: expected a 200 opening response, got {status}"
            )));
        }
        Ok(SseTail {
            reader,
            pending: String::new(),
        })
    }
}

fn read_status_and_headers(reader: &mut impl BufRead) -> io::Result<(u16, Vec<(String, String)>)> {
    let mut status_line = String::new();
    if reader.read_line(&mut status_line)? == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "connection closed before a status line arrived",
        ));
    }
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| io::Error::other(format!("malformed status line: {status_line:?}")))?;

    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;
        if n == 0 || line == "\r\n" {
            break;
        }
        if let Some((k, v)) = line.trim_end().split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    Ok((status, headers))
}

/// Parses a complete, already-fully-read response (`Connection: close`
/// guarantees the peer closes right after writing it, so `read_to_end`
/// always captures exactly one response with no framing ambiguity).
fn parse_response(raw: &[u8]) -> io::Result<HttpResponse> {
    let split_at = find_double_crlf(raw).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "no header terminator (\\r\\n\\r\\n) in the response",
        )
    })?;
    let head = std::str::from_utf8(&raw[..split_at])
        .map_err(|e| io::Error::other(format!("response headers are not valid UTF-8: {e}")))?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| io::Error::other(format!("malformed status line: {status_line:?}")))?;
    let mut headers = Vec::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    let body = raw[split_at + 4..].to_vec();
    Ok(HttpResponse {
        status,
        headers,
        body,
    })
}

fn find_double_crlf(raw: &[u8]) -> Option<usize> {
    raw.windows(4).position(|w| w == b"\r\n\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn parse_response_splits_status_headers_and_body() {
        let raw = b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\r\n\"hello\"";
        let response = parse_response(raw).unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.header("content-type"), Some("application/json"));
        assert_eq!(response.body, b"\"hello\"");
    }

    #[test]
    #[ignore]
    fn as_json_string_strips_quotes_and_unescapes() {
        let response = HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: br#""a\"b\\c""#.to_vec(),
        };
        assert_eq!(response.as_json_string(), Some("a\"b\\c".to_string()));
    }

    #[test]
    #[ignore]
    fn as_json_string_none_for_a_non_string_body() {
        let response = HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: b"{\"id\":1}".to_vec(),
        };
        assert_eq!(response.as_json_string(), None);
    }

    #[test]
    #[ignore]
    fn parse_sse_frame_reads_id_event_and_data() {
        let frame = parse_sse_frame("id: 0.0\nevent: ping\ndata: {\"n\":1}").unwrap();
        assert_eq!(frame.id, "0.0");
        assert_eq!(frame.event, "ping");
        assert_eq!(frame.data, "{\"n\":1}");
    }

    #[test]
    #[ignore]
    fn parse_sse_frame_none_for_a_heartbeat_comment() {
        assert!(parse_sse_frame(":hb").is_none());
    }
}
