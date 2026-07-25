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
/// indefinitely).
fn connect_with_timeout(addr: &str, timeout: Duration) -> io::Result<TcpStream> {
    let sockaddr = addr
        .to_socket_addrs()
        .map_err(|e| io::Error::other(format!("could not resolve {addr}: {e}")))?
        .next()
        .ok_or_else(|| io::Error::other(format!("no addresses resolved for {addr}")))?;
    TcpStream::connect_timeout(&sockaddr, timeout)
        .map_err(|e| map_timeout(e, "connect", addr, timeout))
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
/// prior read timed out mid-chunk (see `next_frame`) and this tail's byte
/// stream is desynced — every subsequent call refuses instead of parsing
/// whatever bytes happen to show up next as a fresh frame (review finding I4).
pub struct SseTail {
    reader: BufReader<TcpStream>,
    pending: String,
    poisoned: Option<String>,
}

impl SseTail {
    /// Blocks for up to `timeout` for the next complete SSE frame.
    /// `Ok(None)` means the connection closed cleanly (EOF); `Err` with
    /// `ErrorKind::TimedOut` means no frame arrived within `timeout` — the
    /// two are deliberately distinct (review finding I4): T12's oracles need
    /// "the stream ended" and "it's just quiet" to be different facts (e.g.
    /// a divergence's `charpente.gap` terminal frame vs. an idle heartbeat
    /// gap are not the same thing).
    ///
    /// A read that times out strictly BETWEEN frames (waiting for a fresh
    /// chunk's size line, nothing committed yet) is safely retryable. A read
    /// that times out or otherwise fails AFTER the size line was parsed —
    /// i.e. partway through the exact-length body/CRLF `read_exact`s — is
    /// not: `read_exact`'s own contract leaves it unspecified how many bytes
    /// it actually consumed before failing, so the stream is desynced and
    /// this tail poisons itself; every later call fails immediately by name
    /// rather than risking a silent garbage parse.
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
                Ok(ChunkRead::Eof) => return Ok(None),
                Err(ChunkError::Boundary(e)) if is_timeout(&e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("no SSE frame arrived within {timeout:?}"),
                    ));
                }
                Err(ChunkError::Boundary(e)) => return Err(e),
                Err(ChunkError::MidChunk(e)) if is_timeout(&e) => {
                    let reason = format!("a read timed out mid-chunk after {timeout:?} ({e})");
                    self.poisoned = Some(reason.clone());
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!(
                            "SseTail desynced: {reason} — this tail is now poisoned, reconnect \
                             with Http::sse_tail instead of reading further"
                        ),
                    ));
                }
                Err(ChunkError::MidChunk(e)) => return Err(e),
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

/// Distinguishes WHERE in one chunk's three reads (size line, body, CRLF) a
/// failure landed (review finding I4). `Boundary` — the size-line read
/// itself failed — is safe to retry in the common case (nothing has been
/// committed for this chunk yet; the read was simply waiting for the next
/// one to start). `MidChunk` — the size line already parsed, so the reader
/// has committed to reading exactly `size + 2` more bytes — is NOT safe:
/// `read_exact` leaves it unspecified how many of those bytes it actually
/// consumed before failing, so the stream can no longer be trusted to be
/// chunk-aligned.
enum ChunkError {
    Boundary(io::Error),
    MidChunk(io::Error),
}

/// Reads one chunked-transfer-encoding chunk (serve.rs's SSE body is always
/// chunked — no `Content-Length` on an unbounded stream).
fn read_one_chunk(reader: &mut BufReader<TcpStream>) -> Result<ChunkRead, ChunkError> {
    let mut size_line = String::new();
    let n = reader
        .read_line(&mut size_line)
        .map_err(ChunkError::Boundary)?;
    if n == 0 {
        return Ok(ChunkRead::Eof);
    }
    let size = usize::from_str_radix(size_line.trim(), 16).map_err(|e| {
        ChunkError::Boundary(io::Error::other(format!(
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
        let mut stream = connect_with_timeout(&self.addr, CLIENT_TIMEOUT)?;
        stream.set_read_timeout(Some(CLIENT_TIMEOUT))?;
        stream.set_write_timeout(Some(CLIENT_TIMEOUT))?;
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
        assert!(msg.contains("desync") || msg.contains("poison"), "{msg}");

        // The tail must now refuse everything further, fast (no more IO —
        // even a tiny timeout budget is enough since the check short-circuits
        // before touching the socket).
        let err2 = tail
            .next_frame(Duration::from_millis(10))
            .expect_err("a poisoned tail must keep refusing");
        let msg2 = err2.to_string();
        assert!(msg2.contains("desync") || msg2.contains("poison"), "{msg2}");
    }
}
