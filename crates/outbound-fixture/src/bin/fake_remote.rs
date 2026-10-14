//! A fake HTTPS payment remote for the edge harness (outbound-effects spec §12
//! oracle 7). `POST /charge` is served over TLS with `tls/server.pem`, a
//! certificate for `pay.fixture`, and deduped by the `idempotency-key` header
//! the way a payment API dedupes, so "applied once" is measured where the
//! money would move. `GET /stats` on a plain-HTTP admin port reports what
//! arrived, one counter per line, for a harness that splits lines rather than
//! parse JSON.
//!
//! usage: fake_remote --tls-port N --admin-port M [--hold-ms MS]
//!
//! Prints `ready` on stdout once both listeners are bound.

// A test server outside the deterministic boundary: its `tokio::net`
// listeners, its `--hold-ms` sleep, its spawned connections and its key-file
// reads are the point, as in the edge harness's files and `serve.rs`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::io::{self, Write as _};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

const USAGE: &str = "usage: fake_remote --tls-port N --admin-port M [--hold-ms MS]";

/// The bucket a request with no `idempotency-key` header is counted under.
const KEYLESS: &str = "<none>";

#[derive(Default)]
struct Ledger {
    /// Every request the TLS port saw, by key.
    requests: BTreeMap<String, u64>,
    /// Every charge applied, by key: 1 per key at most, any number keyless.
    applied: BTreeMap<String, u64>,
    /// The body each key's first application answered, replayed to a repeat.
    charges: BTreeMap<String, String>,
    minted: u64,
}

type Shared = Arc<Mutex<Ledger>>;

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (tls_port, admin_port, hold) = match parse(&args) {
        Ok(parsed) => parsed,
        Err(e) => {
            eprintln!("fake_remote: {e}\n{USAGE}");
            std::process::exit(2);
        }
    };
    let acceptor = TlsAcceptor::from(Arc::new(tls_config()?));
    let tls = TcpListener::bind(("127.0.0.1", tls_port)).await?;
    let admin = TcpListener::bind(("127.0.0.1", admin_port)).await?;
    let ledger = Shared::default();
    println!("ready");
    io::stdout().flush()?;

    tokio::spawn(serve_admin(admin, ledger.clone()));
    loop {
        let Ok((stream, _)) = tls.accept().await else {
            continue;
        };
        let (acceptor, ledger) = (acceptor.clone(), ledger.clone());
        tokio::spawn(async move {
            // A client that refuses the certificate ends the connection here,
            // before any request exists to count.
            let Ok(stream) = acceptor.accept(stream).await else {
                return;
            };
            let service = service_fn(move |req| charge(req, ledger.clone(), hold));
            let _ = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}

fn parse(args: &[String]) -> Result<(u16, u16, Duration), String> {
    let (mut tls_port, mut admin_port, mut hold_ms) = (None, None, 0u64);
    for pair in args.chunks(2) {
        let [flag, value] = pair else {
            return Err(format!("{} needs a value", pair[0]));
        };
        let bad = || format!("{flag} takes a number — got '{value}'");
        match flag.as_str() {
            "--tls-port" => tls_port = Some(value.parse().map_err(|_| bad())?),
            "--admin-port" => admin_port = Some(value.parse().map_err(|_| bad())?),
            "--hold-ms" => hold_ms = value.parse().map_err(|_| bad())?,
            other => return Err(format!("unknown flag '{other}'")),
        }
    }
    match (tls_port, admin_port) {
        (Some(tls), Some(admin)) => Ok((tls, admin, Duration::from_millis(hold_ms))),
        _ => Err("--tls-port and --admin-port are both required".into()),
    }
}

fn tls_config() -> io::Result<rustls::ServerConfig> {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tls");
    let certs = CertificateDer::pem_file_iter(format!("{dir}/server.pem"))
        .map_err(io::Error::other)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(io::Error::other)?;
    let key =
        PrivateKeyDer::from_pem_file(format!("{dir}/server.key")).map_err(io::Error::other)?;
    rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(io::Error::other)?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(io::Error::other)
}

/// Counts the request, then answers. A key's first authorized `POST /charge`
/// mints `ch_<n>` and is applied, and only it waits out `hold` (the window a
/// `kill -9` aims at); a repeat replays that body at once, and a keyless one
/// is applied every time under a fresh charge.
async fn charge(
    req: Request<Incoming>,
    ledger: Shared,
    hold: Duration,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let key = req
        .headers()
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let authorized = req
        .headers()
        .get("authorization")
        .is_some_and(|v| v == "Bearer test-key");
    let (status, body, first) = {
        let mut ledger = ledger.lock().expect("no handler panics holding the ledger");
        let bucket = key.clone().unwrap_or_else(|| KEYLESS.to_string());
        *ledger.requests.entry(bucket.clone()).or_insert(0) += 1;
        if req.method() != Method::POST || req.uri().path() != "/charge" {
            (404, "no such route".to_string(), false)
        } else if !authorized {
            (
                401,
                "authorization must be Bearer test-key".to_string(),
                false,
            )
        } else if let Some(body) = key.as_ref().and_then(|k| ledger.charges.get(k)) {
            (200, body.clone(), false)
        } else {
            ledger.minted += 1;
            let body = format!("ch_{}", ledger.minted);
            *ledger.applied.entry(bucket).or_insert(0) += 1;
            if let Some(key) = &key {
                ledger.charges.insert(key.clone(), body.clone());
            }
            (200, body, key.is_some())
        }
    };
    if first {
        tokio::time::sleep(hold).await;
    }
    Ok(text(status, body))
}

async fn serve_admin(listener: TcpListener, ledger: Shared) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let ledger = ledger.clone();
        tokio::spawn(async move {
            let service = service_fn(move |req| {
                let response = stats(&req, &ledger);
                async move { Ok::<_, Infallible>(response) }
            });
            let _ = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}

/// `requests <key> <n>` lines, then `applied <key> <n>` lines, each set
/// sorted by key.
fn stats(req: &Request<Incoming>, ledger: &Shared) -> Response<Full<Bytes>> {
    if req.method() != Method::GET || req.uri().path() != "/stats" {
        return text(404, "no such route".to_string());
    }
    let ledger = ledger.lock().expect("no handler panics holding the ledger");
    let mut out = String::new();
    for (key, n) in &ledger.requests {
        out.push_str(&format!("requests {key} {n}\n"));
    }
    for (key, n) in &ledger.applied {
        out.push_str(&format!("applied {key} {n}\n"));
    }
    text(200, out)
}

fn text(status: u16, body: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain")
        .body(Full::new(Bytes::from(body)))
        .expect("a status from this file and a fixed header build a response")
}
