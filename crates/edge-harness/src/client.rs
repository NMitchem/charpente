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
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

/// Every client call gets a bound (review finding I3): this crate ships
/// `pause` (SIGSTOP), whose entire purpose is to make a node stop responding
/// while its socket stays open, and libtest has no per-test timeout of its
/// own — without this, a paused node hangs the whole test process instead of
/// failing with a named error. Generous relative to real request latency.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

/// Resolves `addr` and connects with a bound wait — `TcpStream::connect`
/// alone has no timeout at all (a SYN that never gets ACKed can hang
/// indefinitely). Tries every resolved address, as `TcpStream::connect`
/// does: a name resolving to `::1` first while the node listens on
/// `127.0.0.1` must still connect, not fail on the first candidate.
fn connect_with_timeout(addr: &str, timeout: Duration) -> io::Result<TcpStream> {
    let candidates: Vec<_> = addr
        .to_socket_addrs()
        .map_err(|e| io::Error::other(format!("could not resolve {addr}: {e}")))?
        .collect();
    let mut last = None;
    for sockaddr in candidates {
        match TcpStream::connect_timeout(&sockaddr, timeout) {
            Ok(stream) => return Ok(stream),
            Err(e) => last = Some(map_timeout(e, "connect", addr, timeout)),
        }
    }
    Err(last.unwrap_or_else(|| io::Error::other(format!("no addresses resolved for {addr}"))))
}

/// Renames a raw socket-timeout error (often just "Resource temporarily
/// unavailable" from the OS) to something that names the operation, the
/// address, and the bound — the whole point of I3 is a named failure instead
/// of a hang, so the message has to actually say "timed out".
fn map_timeout(e: io::Error, what: &str, addr: &str, timeout: Duration) -> io::Error {
    if is_timeout(&e) {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!("{what} to {addr} timed out after {timeout:?} (e.g. a paused node)"),
        )
    } else {
        e
    }
}

/// Socket read/write timeouts surface as `WouldBlock` on some platforms and
/// `TimedOut` on others — both mean the same thing here.
fn is_timeout(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct SseFrame {
    pub id: String,
    pub event: String,
    pub data: String,
}

/// A live SSE connection: the still-open reader plus whatever partial frame
/// bytes have been read but not yet parsed. `poisoned`, once set, means a
/// prior read failed mid-chunk (see `next_frame`) and this tail's byte
/// stream is desynced — every subsequent call refuses instead of parsing
/// whatever bytes happen to show up next as a fresh frame (review finding I4).
pub struct SseTail {
    reader: BufReader<TcpStream>,
    pending: String,
    poisoned: Option<String>,
}

impl SseTail {
    /// `timeout` bounds each underlying socket read, so a frame arriving in
    /// several chunks can take longer than `timeout` overall; what the bound
    /// guarantees is that this call never blocks indefinitely on a silent
    /// peer. `Ok(None)` means the connection closed cleanly (EOF); `Err` with
    /// `ErrorKind::TimedOut` means no frame arrived within `timeout` — the
    /// two are deliberately distinct (review finding I4): T12's oracles need
    /// "the stream ended" and "it's just quiet" to be different facts (e.g.
    /// a divergence's `charpente.gap` terminal frame vs. an idle heartbeat
    /// gap are not the same thing).
    ///
    /// A read that fails strictly BETWEEN frames — waiting for a fresh
    /// chunk's size line with none of it consumed yet — is safely retryable.
    /// A read that fails once ANY of a chunk has been consumed (a partial
    /// size line, or partway through the exact-length body/CRLF
    /// `read_exact`s) is not: those bytes are gone from the stream and
    /// neither `read_until` nor `read_exact` says how many it took, so the
    /// stream is desynced and this tail poisons itself. The same holds when
    /// the read itself SUCCEEDS but its content doesn't parse (a malformed
    /// hex size, a chunk extension, non-UTF-8 bytes): the size line's bytes
    /// are still off the wire, so a fully-read-but-unparseable line poisons
    /// exactly like an IO failure would — every later call fails immediately
    /// by name rather than risking a silent garbage parse. Timeouts and hard
    /// failures (a peer that closes mid-chunk) poison alike — a stream cut
    /// mid-frame must never later report a clean close.
    pub fn next_frame(&mut self, timeout: Duration) -> io::Result<Option<SseFrame>> {
        if let Some(reason) = &self.poisoned {
            return Err(io::Error::other(format!(
                "SseTail is poisoned (desynced by a prior mid-chunk read failure: {reason}) — \
                 reconnect with Http::sse_tail instead of reading this tail further"
            )));
        }
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
                Ok(ChunkRead::Chunk(chunk)) => {
                    self.pending.push_str(&String::from_utf8_lossy(&chunk));
                }
                Ok(ChunkRead::Eof) => {
                    // Negative-space (hard rule 4): a non-empty `pending`
                    // here is by construction a partial SSE frame (a
                    // complete one would already have been drained by the
                    // `find("\n\n")` arm above) — silently discarding it
                    // would report a clean close for a stream cut
                    // mid-frame, exactly the conflation this poison
                    // mechanism exists to prevent. NOT reachable against
                    // charpente today (serve.rs's `encode_frame` emits one
                    // complete frame per `Bytes`, hyper writes one chunk per
                    // `Frame::data`) — independent review of 2bfdf16,
                    // finding 3.
                    assert!(
                        self.pending.is_empty(),
                        "SseTail: EOF with {} byte(s) of an unterminated frame still pending: \
                         {:?} — a stream cut mid-frame must never report a clean close",
                        self.pending.len(),
                        self.pending
                    );
                    return Ok(None);
                }
                Err(ChunkError::Boundary(e)) if is_timeout(&e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("no SSE frame arrived within {timeout:?}"),
                    ));
                }
                Err(ChunkError::Boundary(e)) => return Err(e),
                Err(ChunkError::MidChunk(e)) => {
                    let kind = if is_timeout(&e) {
                        io::ErrorKind::TimedOut
                    } else {
                        e.kind()
                    };
                    let reason =
                        format!("a read failed mid-chunk under a {timeout:?} deadline ({e})");
                    self.poisoned = Some(reason.clone());
                    return Err(io::Error::new(
                        kind,
                        format!(
                            "SseTail desynced: {reason} — this tail is now poisoned, reconnect \
                             with Http::sse_tail instead of reading further"
                        ),
                    ));
                }
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

enum ChunkRead {
    Chunk(Vec<u8>),
    /// The terminal zero-length chunk, or a closed connection.
    Eof,
}

/// Distinguishes whether a failure inside one chunk's reads (size line,
/// body, CRLF) consumed any of the stream (review finding I4, sharpened by
/// the independent review of 2bfdf16, findings 1+2). `Boundary` — the
/// size-line read failed with nothing of it consumed — is safe to retry: the
/// read was simply waiting for the next chunk to start. `MidChunk` —
/// anything consumed already, whether a partial or malformed size line, or
/// part of the `size + 2` bytes the reader has committed to — is NOT safe:
/// once the size line's bytes are off the wire, `read_until` succeeding and
/// `from_str_radix` failing on its content is not "nothing consumed" just
/// because the failure is a parse failure, not an IO failure — the stream
/// can no longer be trusted to be chunk-aligned.
enum ChunkError {
    Boundary(io::Error),
    MidChunk(io::Error),
}

/// Reads one chunked-transfer-encoding chunk (serve.rs's SSE body is always
/// chunked — no `Content-Length` on an unbounded stream).
fn read_one_chunk(reader: &mut BufReader<TcpStream>) -> Result<ChunkRead, ChunkError> {
    // `read_until` into a `Vec<u8>` has no UTF-8 validation and no
    // truncation-on-error guard — unlike `read_line`'s `String`, whose
    // internal `Guard::drop` resets the buffer to empty when the appended
    // bytes fail UTF-8 validation, AFTER `consume()` already ate them off
    // the wire. `raw.is_empty()` is therefore an honest "nothing consumed
    // yet" check even for non-UTF-8 input (independent review of 2bfdf16,
    // finding 2).
    let mut raw = Vec::new();
    let n = match reader.read_until(b'\n', &mut raw) {
        Ok(n) => n,
        Err(e) if raw.is_empty() => return Err(ChunkError::Boundary(e)),
        Err(e) => return Err(ChunkError::MidChunk(e)),
    };
    if n == 0 {
        return Ok(ChunkRead::Eof);
    }
    // Past this point the size line's bytes are unconditionally off the
    // wire — a malformed line (bad UTF-8, bad hex, a chunk extension like
    // "a;ext") is a desync, not a retryable boundary, no matter which step
    // of decoding it (finding 1: `read_until` succeeding and consuming a
    // whole line is not "nothing consumed", regardless of what the content
    // turns out to be).
    let size_line = std::str::from_utf8(&raw).map_err(|e| {
        ChunkError::MidChunk(io::Error::other(format!(
            "malformed chunk size line {raw:?}: {e}"
        )))
    })?;
    let size = usize::from_str_radix(size_line.trim(), 16).map_err(|e| {
        ChunkError::MidChunk(io::Error::other(format!(
            "malformed chunk size {size_line:?}: {e}"
        )))
    })?;
    if size == 0 {
        return Ok(ChunkRead::Eof);
    }
    let mut data = vec![0u8; size];
    reader.read_exact(&mut data).map_err(ChunkError::MidChunk)?;
    let mut crlf = [0u8; 2];
    reader.read_exact(&mut crlf).map_err(ChunkError::MidChunk)?;
    Ok(ChunkRead::Chunk(data))
}

/// A client bound to one node address. Not thread-safe (one connection at a
/// time) and not meant to be — scenarios drive one client per logical actor.
pub struct Http {
    addr: String,
    session: Option<String>,
    bearer: Option<String>,
}

impl Http {
    pub fn new(addr: impl Into<String>) -> Http {
        Http {
            addr: addr.into(),
            session: None,
            bearer: None,
        }
    }

    /// Stores `token` as this client's session cookie — call with the raw
    /// token read back from a login response (`HttpResponse::as_json_string`).
    /// Every subsequent request attaches it as `Cookie: charpente_session=…`.
    pub fn set_session(&mut self, token: impl Into<String>) {
        self.session = Some(token.into());
    }

    /// Sends `Authorization: Bearer <token>` on every request from now on
    /// (an OIDC JWT or an API key).
    pub fn set_bearer(&mut self, token: impl Into<String>) {
        self.bearer = Some(token.into());
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
        let mut stream = connect_with_timeout(&self.addr, CLIENT_TIMEOUT)?;
        stream.set_read_timeout(Some(CLIENT_TIMEOUT))?;
        stream.set_write_timeout(Some(CLIENT_TIMEOUT))?;
        let mut head =
            format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
        if let Some(session) = &self.session {
            head.push_str(&format!("Cookie: charpente_session={session}\r\n"));
        }
        if let Some(bearer) = &self.bearer {
            head.push_str(&format!("Authorization: Bearer {bearer}\r\n"));
        }
        if let Some(body) = body {
            head.push_str("Content-Type: application/json\r\n");
            head.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        head.push_str("\r\n");
        stream
            .write_all(head.as_bytes())
            .map_err(|e| map_timeout(e, "write", &self.addr, CLIENT_TIMEOUT))?;
        if let Some(body) = body {
            stream
                .write_all(body)
                .map_err(|e| map_timeout(e, "write", &self.addr, CLIENT_TIMEOUT))?;
        }
        let mut raw = Vec::new();
        stream
            .read_to_end(&mut raw)
            .map_err(|e| map_timeout(e, "read", &self.addr, CLIENT_TIMEOUT))?;
        parse_response(&raw)
    }

    /// Opens `path` as an SSE subscription (optionally resuming from
    /// `last_event_id`), reads the opening status/headers, and returns a
    /// live tail the caller polls with `SseTail::next_frame`.
    pub fn sse_tail(&mut self, path: &str, last_event_id: Option<&str>) -> io::Result<SseTail> {
        let mut stream = connect_with_timeout(&self.addr, CLIENT_TIMEOUT)?;
        stream.set_read_timeout(Some(CLIENT_TIMEOUT))?;
        stream.set_write_timeout(Some(CLIENT_TIMEOUT))?;
        let mut head = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n");
        if let Some(session) = &self.session {
            head.push_str(&format!("Cookie: charpente_session={session}\r\n"));
        }
        if let Some(id) = last_event_id {
            head.push_str(&format!("Last-Event-ID: {id}\r\n"));
        }
        head.push_str("\r\n");
        stream
            .write_all(head.as_bytes())
            .map_err(|e| map_timeout(e, "write", &self.addr, CLIENT_TIMEOUT))?;

        let mut reader = BufReader::new(stream);
        let (status, _headers) = read_status_and_headers(&mut reader)
            .map_err(|e| map_timeout(e, "read", &self.addr, CLIENT_TIMEOUT))?;
        if status != 200 {
            return Err(io::Error::other(format!(
                "sse_tail: expected a 200 opening response, got {status}"
            )));
        }
        Ok(SseTail {
            reader,
            pending: String::new(),
            poisoned: None,
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
    fn parse_response_splits_status_headers_and_body() {
        let raw = b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\r\n\"hello\"";
        let response = parse_response(raw).unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.header("content-type"), Some("application/json"));
        assert_eq!(response.body, b"\"hello\"");
    }

    #[test]
    fn as_json_string_strips_quotes_and_unescapes() {
        let response = HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: br#""a\"b\\c""#.to_vec(),
        };
        assert_eq!(response.as_json_string(), Some("a\"b\\c".to_string()));
    }

    #[test]
    fn as_json_string_none_for_a_non_string_body() {
        let response = HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: b"{\"id\":1}".to_vec(),
        };
        assert_eq!(response.as_json_string(), None);
    }

    #[test]
    fn parse_sse_frame_reads_id_event_and_data() {
        let frame = parse_sse_frame("id: 0.0\nevent: ping\ndata: {\"n\":1}").unwrap();
        assert_eq!(frame.id, "0.0");
        assert_eq!(frame.event, "ping");
        assert_eq!(frame.data, "{\"n\":1}");
    }

    #[test]
    fn parse_sse_frame_none_for_a_heartbeat_comment() {
        assert!(parse_sse_frame(":hb").is_none());
    }

    // ---------------------------------------------------- I3: client timeouts

    /// Review finding I3: a paused node (SIGSTOP) accepts the connection but
    /// never writes a byte back — before the fix this hung forever; now it
    /// must fail with a named `TimedOut` error, well within `CLIENT_TIMEOUT`.
    #[test]
    #[ignore]
    fn request_times_out_against_a_silent_server() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                std::thread::sleep(Duration::from_secs(30));
                drop(stream);
            }
        });

        let mut http = Http::new(addr);
        let started = std::time::Instant::now();
        let err = http
            .get("/anything")
            .expect_err("a silent server must time out, not hang forever");
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "took too long: {:?}",
            started.elapsed()
        );
        assert_eq!(err.kind(), io::ErrorKind::TimedOut, "{err}");
    }

    // -------------------------------------------------- I4: SSE timeout/EOF

    /// Reads and discards an incoming request up to its blank line — like a
    /// real HTTP server, which always reads the request before responding.
    /// A stub that responds and closes WITHOUT draining first can trigger a
    /// TCP RST-on-close race (unread bytes still in the receive buffer at
    /// close time), which surfaced as flaky `ConnectionReset`/spurious-EINVAL
    /// failures while writing these tests — a test-fixture hazard, not a
    /// `client.rs` one (a real server never leaves a request unread).
    fn drain_request(stream: TcpStream) -> TcpStream {
        let mut reader = BufReader::new(stream);
        loop {
            let mut line = String::new();
            let n = reader.read_line(&mut line).unwrap_or(0);
            if n == 0 || line == "\r\n" {
                break;
            }
        }
        reader.into_inner()
    }

    /// Review finding I4: a clean close (EOF) and silence past the deadline
    /// (timeout) must be distinct outcomes — `Ok(None)` vs. `Err(TimedOut)`.
    #[test]
    #[ignore]
    fn next_frame_distinguishes_eof_from_timeout() {
        // EOF: the peer sends the opening response then closes immediately,
        // with no chunks at all.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let mut stream = drain_request(stream);
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n")
                    .unwrap();
            }
        });
        let mut http = Http::new(addr);
        let mut tail = http.sse_tail("/feed", None).unwrap();
        let frame = tail.next_frame(Duration::from_secs(2)).unwrap();
        assert!(frame.is_none(), "a clean close must surface as Ok(None)");

        // Timeout: the peer sends the opening response, then goes silent —
        // no chunks ever arrive, connection stays open.
        let listener2 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr2 = listener2.local_addr().unwrap().to_string();
        std::thread::spawn(move || {
            if let Ok((stream, _)) = listener2.accept() {
                let mut stream = drain_request(stream);
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n")
                    .unwrap();
                std::thread::sleep(Duration::from_secs(5));
            }
        });
        let mut http2 = Http::new(addr2);
        let mut tail2 = http2.sse_tail("/feed", None).unwrap();
        let err = tail2
            .next_frame(Duration::from_millis(200))
            .expect_err("silence past the deadline must be a distinct timeout, not Ok(None)");
        assert_eq!(err.kind(), io::ErrorKind::TimedOut, "{err}");
    }

    /// Review finding I4: a read that times out AFTER the chunk size line
    /// parsed but before the body finished must poison the tail — every
    /// later call must refuse by name rather than parse whatever bytes show
    /// up next as a fresh (garbage) frame.
    #[test]
    #[ignore]
    fn next_frame_poisons_the_tail_after_a_mid_chunk_timeout() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let mut stream = drain_request(stream);
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n")
                    .unwrap();
                // A chunk claiming 10 (0xa) bytes, but only 3 are ever sent —
                // the reader commits to reading 10+2 more bytes, then stalls.
                stream.write_all(b"a\r\nabc").unwrap();
                std::thread::sleep(Duration::from_secs(5));
            }
        });

        let mut http = Http::new(addr);
        let mut tail = http.sse_tail("/feed", None).unwrap();
        let err = tail
            .next_frame(Duration::from_millis(200))
            .expect_err("a mid-chunk timeout must be a named error, not silent garbage");
        let msg = err.to_string();
        assert!(msg.contains("SseTail desynced:"), "{msg}");
        assert!(msg.contains("this tail is now poisoned"), "{msg}");

        // The tail must now refuse everything further, fast (no more IO —
        // even a tiny timeout budget is enough since the check short-circuits
        // before touching the socket).
        let err2 = tail
            .next_frame(Duration::from_millis(10))
            .expect_err("a poisoned tail must keep refusing");
        let msg2 = err2.to_string();
        assert!(msg2.contains("SseTail is poisoned"), "{msg2}");
    }

    /// Review finding I4, size-line half: a read that times out with part of
    /// the chunk size line already consumed is NOT retryable — `read_until`
    /// ate those bytes, so a retry would read the remainder as a whole size
    /// ("1" eaten out of "1a", "a\r\n" read back as 0xa) and desync in
    /// silence. It must poison like any other mid-chunk failure.
    #[test]
    #[ignore]
    fn next_frame_poisons_the_tail_after_a_partial_size_line() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let mut stream = drain_request(stream);
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n")
                    .unwrap();
                // The first digit of a two-digit size line, then silence —
                // the rest of "1a\r\n" never arrives.
                stream.write_all(b"1").unwrap();
                std::thread::sleep(Duration::from_secs(5));
            }
        });

        let mut http = Http::new(addr);
        let mut tail = http.sse_tail("/feed", None).unwrap();
        let err = tail
            .next_frame(Duration::from_millis(200))
            .expect_err("a half-consumed size line is a desync, not a retryable quiet period");
        assert!(err.to_string().contains("SseTail desynced:"), "{err}");

        let err2 = tail
            .next_frame(Duration::from_millis(10))
            .expect_err("a half-consumed size line must poison the tail");
        assert!(err2.to_string().contains("SseTail is poisoned"), "{err2}");
    }

    /// Independent review of 2bfdf16, finding 1: a malformed chunk size —
    /// here a chunk extension (`a;ext`), valid chunked-encoding syntax but
    /// not valid hex — is not a retryable boundary just because
    /// `from_str_radix` is the thing that failed. `read_until` already
    /// consumed the whole line off the wire; only the content was
    /// unparseable. This must poison like any other mid-chunk failure.
    #[test]
    #[ignore]
    fn next_frame_poisons_the_tail_after_a_malformed_chunk_size() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let mut stream = drain_request(stream);
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n")
                    .unwrap();
                stream.write_all(b"a;ext\r\n").unwrap();
            }
        });

        let mut http = Http::new(addr);
        let mut tail = http.sse_tail("/feed", None).unwrap();
        let err = tail
            .next_frame(Duration::from_secs(2))
            .expect_err("a malformed chunk size is a desync, not a retryable boundary");
        assert!(err.to_string().contains("SseTail desynced:"), "{err}");

        let err2 = tail
            .next_frame(Duration::from_secs(2))
            .expect_err("a malformed chunk size must poison the tail, not read as a clean EOF");
        assert!(err2.to_string().contains("SseTail is poisoned"), "{err2}");
    }

    /// Independent review of 2bfdf16, finding 2: `read_line`'s consumed-byte
    /// guard is dishonest for non-UTF-8 input — its internal `Guard::drop`
    /// resets the `String` to empty on a UTF-8 validation failure AFTER
    /// `consume()` already ate the bytes off the wire, so
    /// `size_line.is_empty()` reads true when bytes are actually gone.
    #[test]
    #[ignore]
    fn next_frame_poisons_the_tail_after_a_non_utf8_size_line() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let mut stream = drain_request(stream);
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n")
                    .unwrap();
                stream.write_all(b"\xff\xfe\r\n").unwrap();
            }
        });

        let mut http = Http::new(addr);
        let mut tail = http.sse_tail("/feed", None).unwrap();
        let err = tail
            .next_frame(Duration::from_secs(2))
            .expect_err("a non-UTF-8 size line is a desync, not a retryable boundary");
        assert!(err.to_string().contains("SseTail desynced:"), "{err}");

        let err2 = tail
            .next_frame(Duration::from_secs(2))
            .expect_err("a non-UTF-8 size line must poison the tail, not read as a clean EOF");
        assert!(err2.to_string().contains("SseTail is poisoned"), "{err2}");
    }

    /// Review finding I4, hard-failure half: a peer that closes the
    /// connection PARTWAY through a chunk has cut the stream mid-frame. That
    /// must poison the tail too — otherwise the first call errors and the
    /// next one reads 0 bytes and reports `Ok(None)`, i.e. a clean close for
    /// a stream that was truncated mid-frame (exactly the conflation I4
    /// exists to prevent).
    #[test]
    #[ignore]
    fn next_frame_poisons_the_tail_after_a_mid_chunk_close() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let mut stream = drain_request(stream);
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n")
                    .unwrap();
                // 10 bytes promised, 3 delivered, then the peer closes.
                stream.write_all(b"a\r\nabc").unwrap();
            }
        });

        let mut http = Http::new(addr);
        let mut tail = http.sse_tail("/feed", None).unwrap();
        let err = tail
            .next_frame(Duration::from_secs(2))
            .expect_err("a close mid-chunk is a truncation, not a clean end");
        assert!(err.to_string().contains("SseTail desynced:"), "{err}");

        let err2 = tail
            .next_frame(Duration::from_secs(2))
            .expect_err("a truncated stream must never later report a clean close");
        assert!(err2.to_string().contains("SseTail is poisoned"), "{err2}");
    }

    /// T10 re-review, routed to T12: the negative-space assert in
    /// `next_frame`'s `Eof` arm ("EOF with N byte(s) of an unterminated
    /// frame still pending") had ZERO coverage — nothing proved it fires or
    /// that its message says what it claims. The precondition IS reachable
    /// through the ordinary public API with an adversarial stub (the
    /// original review's "not reachable" was about charpente's own SERVER,
    /// which always emits one complete frame per chunk — not about a stub
    /// deliberately withholding the trailing blank line): a stub that writes
    /// a COMPLETE chunk ("5\r\nid: 1\r\n" — chunk size 5, exactly "id: 1",
    /// its trailing CRLF) and then closes yields `Chunk(b"id: 1")` ->
    /// `pending = "id: 1"` (no "\n\n", so no frame is emitted) -> the next
    /// `read_one_chunk` sees an immediate EOF on the size-line read -> the
    /// `Eof` arm fires with a non-empty `pending`, which must panic rather
    /// than silently report a clean close.
    #[test]
    #[ignore]
    #[should_panic(expected = "unterminated frame still pending")]
    fn next_frame_panics_on_eof_with_an_unterminated_frame_pending() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let mut stream = drain_request(stream);
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n")
                    .unwrap();
                // One complete chunk, no terminating blank line — the peer
                // closes right after, mid-frame.
                stream.write_all(b"5\r\nid: 1\r\n").unwrap();
            }
        });

        let mut http = Http::new(addr);
        let mut tail = http.sse_tail("/feed", None).unwrap();
        let _ = tail.next_frame(Duration::from_secs(2));
    }
}
