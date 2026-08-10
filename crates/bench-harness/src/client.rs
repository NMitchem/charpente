//! Hand-rolled keep-alive HTTP/1.1 client (spec §4). The edge-harness client
//! is deliberately Connection: close per request (client.rs:333-365) and
//! cannot drive a keep-alive load loop — this one reuses one TcpStream per
//! thread and frames responses by content-length.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::io::{self, BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Conn {
    reader: BufReader<TcpStream>,
}

impl Conn {
    pub fn connect(addr: &str) -> io::Result<Conn> {
        let sock: SocketAddr = addr
            .parse()
            .map_err(|e| io::Error::other(format!("bad addr {addr:?}: {e}")))?;
        let stream = TcpStream::connect_timeout(&sock, CLIENT_TIMEOUT)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(CLIENT_TIMEOUT))?;
        stream.set_write_timeout(Some(CLIENT_TIMEOUT))?;
        Ok(Conn {
            reader: BufReader::new(stream),
        })
    }

    pub fn request(
        &mut self,
        method: &str,
        path: &str,
        headers: &[(String, String)],
        body: &[u8],
    ) -> io::Result<(u16, Vec<u8>)> {
        let mut req = format!(
            "{method} {path} HTTP/1.1\r\nhost: bench\r\ncontent-length: {}\r\n",
            body.len()
        );
        for (k, v) in headers {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        req.push_str("\r\n");
        let mut bytes = req.into_bytes();
        bytes.extend_from_slice(body);
        self.reader.get_mut().write_all(&bytes)?;
        read_response(&mut self.reader)
    }
}

pub fn read_response(r: &mut impl BufRead) -> io::Result<(u16, Vec<u8>)> {
    let mut line = String::new();
    r.read_line(&mut line)?;
    let status: u16 = line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| io::Error::other(format!("bad status line: {line:?}")))?;
    let mut content_length: Option<usize> = None;
    loop {
        let mut h = String::new();
        r.read_line(&mut h)?;
        if h == "\r\n" || h == "\n" || h.is_empty() {
            break;
        }
        let lower = h.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            content_length = Some(
                v.trim()
                    .parse()
                    .map_err(|e| io::Error::other(format!("bad content-length {v:?}: {e}")))?,
            );
        }
        if lower.starts_with("transfer-encoding:") && lower.contains("chunked") {
            return Err(io::Error::other(
                "chunked responses are unsupported by the bench client — \
                 the bench routes must reply with content-length framing",
            ));
        }
    }
    let len = content_length.ok_or_else(|| {
        io::Error::other("response carried neither content-length nor a named refusal")
    })?;
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    Ok((status, body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;

    #[test]
    fn parses_a_keep_alive_response() {
        let raw: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                           content-length: 2\r\n\r\n42HTTP/1.1 404 NO\r\ncontent-length: 0\r\n\r\n";
        let mut r = BufReader::new(raw);
        assert_eq!(read_response(&mut r).unwrap(), (200, b"42".to_vec()));
        assert_eq!(read_response(&mut r).unwrap(), (404, Vec::new()));
    }

    #[test]
    fn chunked_is_refused_by_name() {
        let raw: &[u8] = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n";
        let err = read_response(&mut BufReader::new(raw)).unwrap_err();
        assert!(err.to_string().contains("chunked"), "got: {err}");
    }
}
