//! The hyper edge (spec §edge): translates wire↔`http::Request`/`Response`
//! and feeds the single-threaded kernel over a channel. The one sanctioned
//! ambient-IO module besides `prod` — nothing here makes decisions; it moves
//! bytes. Everything behavioral stays in `crate::http`, inside the sim.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::io;
use std::net::SocketAddr;
use std::sync::mpsc;

use http_body_util::{BodyExt, Full, Limited};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;

use crate::http::{App, Method, Request, Response};

/// A running server. v0 has no graceful shutdown — the process owns it; the
/// threads end when the process does.
pub struct Server {
    addr: SocketAddr,
}

impl Server {
    pub fn local_addr(&self) -> String {
        self.addr.to_string()
    }
}

pub fn serve(addr: &str, app: App) -> io::Result<Server> {
    let body_cap = app.config().body_cap;
    let (kernel_tx, kernel_rx) =
        mpsc::channel::<(Request, tokio::sync::oneshot::Sender<Response>)>();

    // The kernel: one thread owning the App — requests serialize here. M0
    // step 6 adds more event sources (due jobs, cron ticks) to this loop.
    std::thread::spawn(move || {
        let mut app = app;
        while let Ok((request, reply)) = kernel_rx.recv() {
            let _ = reply.send(app.handle(request));
        }
    });

    let listener = std::net::TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    listener.set_nonblocking(true)?;

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("tokio runtime construction failed");
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener)
                .expect("listener registers with the runtime");
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                let kernel = kernel_tx.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: hyper::Request<Incoming>| {
                        let kernel = kernel.clone();
                        async move {
                            let response = match to_kernel_request(req, body_cap).await {
                                Ok(request) => {
                                    let (tx, rx) = tokio::sync::oneshot::channel();
                                    if kernel.send((request, tx)).is_err() {
                                        Response::error(500, "the kernel thread is gone")
                                    } else {
                                        rx.await.unwrap_or_else(|_| {
                                            Response::error(500, "the kernel dropped the request")
                                        })
                                    }
                                }
                                Err(response) => response,
                            };
                            Ok::<_, std::convert::Infallible>(from_kernel_response(response))
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
    });

    Ok(Server { addr: local })
}

async fn to_kernel_request(
    req: hyper::Request<Incoming>,
    body_cap: usize,
) -> Result<Request, Response> {
    let Some(method) = Method::parse(req.method().as_str()) else {
        return Err(Response::error(
            405,
            "unsupported method — one of GET, POST, PUT, PATCH, DELETE",
        ));
    };
    let path = req.uri().path().to_string(); // query string stripped: v0 ignores it
    let mut headers = std::collections::BTreeMap::new();
    for (name, value) in req.headers() {
        if let Ok(v) = value.to_str() {
            headers.insert(name.as_str().to_ascii_lowercase(), v.to_string());
        }
    }
    match Limited::new(req.into_body(), body_cap).collect().await {
        Ok(collected) => Ok(Request {
            method,
            path,
            headers,
            body: collected.to_bytes().to_vec(),
        }),
        Err(e)
            if e.downcast_ref::<http_body_util::LengthLimitError>()
                .is_some() =>
        {
            Err(Response::error(
                413,
                &format!(
                    "request body exceeds the {body_cap}-byte cap — raise AppConfig.body_cap or send less"
                ),
            ))
        }
        Err(_) => Err(Response::error(400, "request body could not be read")),
    }
}

fn from_kernel_response(response: Response) -> hyper::Response<Full<Bytes>> {
    let mut builder = hyper::Response::builder().status(response.status);
    for (key, value) in &response.headers {
        builder = builder.header(key, value);
    }
    builder
        .body(Full::new(Bytes::from(response.body)))
        .expect("kernel responses are valid HTTP")
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use super::*;
    use crate::http::{ApiError, AppConfig, Ctx, Route};
    use crate::prod::{ProdDisk, ProdEntropy};
    use charpente_core::db::{Schema, Tx};
    use charpente_core::io::Entropy;

    // #[route] can't be used inside the charpente crate itself (it emits
    // ::charpente paths), so the smoke route is built by hand.
    fn hello_glue(
        _tx: &mut Tx,
        _ctx: &Ctx,
        _captures: &[String],
        _body: &[u8],
    ) -> Result<Response, ApiError> {
        Ok(Response::json(200, &"hello"))
    }

    #[test]
    fn serve_answers_http1_over_loopback() {
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        let root = std::env::temp_dir().join(format!(
            "charpente-serve-{:016x}",
            u64::from_le_bytes(suffix)
        ));
        let app = App::open(
            Box::new(ProdDisk::new(&root).unwrap()),
            Schema::new(),
            Box::new(ProdEntropy::new().unwrap()),
            AppConfig::default(),
        )
        .unwrap()
        .mount(vec![Route {
            method: Method::Get,
            pattern: "/hello",
            auth: false,
            name: "hello",
            glue: hello_glue,
        }]);

        let server = serve("127.0.0.1:0", app).unwrap();
        let mut stream = std::net::TcpStream::connect(server.local_addr()).unwrap();
        stream
            .write_all(b"GET /hello HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(
            response.contains("content-type: application/json"),
            "{response}"
        );
        assert!(response.ends_with("\"hello\""), "{response}");

        // Unknown path routes to the kernel's 404.
        let mut stream = std::net::TcpStream::connect(server.local_addr()).unwrap();
        stream
            .write_all(b"GET /nope HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 404"), "{response}");

        std::fs::remove_dir_all(root).unwrap();
    }

    // Hard rule 5: the 413 is product surface — it must name the cap and the
    // fix, and it must fire at the edge (before routing: this POST would be a
    // kernel 405 otherwise).
    #[test]
    fn serve_caps_bodies_at_the_edge() {
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        let root = std::env::temp_dir().join(format!(
            "charpente-serve-cap-{:016x}",
            u64::from_le_bytes(suffix)
        ));
        let app = App::open(
            Box::new(ProdDisk::new(&root).unwrap()),
            Schema::new(),
            Box::new(ProdEntropy::new().unwrap()),
            AppConfig {
                body_cap: 64,
                ..AppConfig::default()
            },
        )
        .unwrap()
        .mount(vec![Route {
            method: Method::Get,
            pattern: "/hello",
            auth: false,
            name: "hello",
            glue: hello_glue,
        }]);

        let server = serve("127.0.0.1:0", app).unwrap();
        let mut stream = std::net::TcpStream::connect(server.local_addr()).unwrap();
        stream
            .write_all(
                b"POST /hello HTTP/1.1\r\nHost: localhost\r\nContent-Length: 65\r\n\
                  Connection: close\r\n\r\n",
            )
            .unwrap();
        stream.write_all(&[b'x'; 65]).unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 413"), "{response}");
        assert!(
            response.contains("body_cap") && response.contains("64"),
            "the 413 body must name the cap and the fix — got: {response}"
        );

        std::fs::remove_dir_all(root).unwrap();
    }
}
