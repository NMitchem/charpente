//! The hyper edge (spec §edge): translates wire↔`http::Request`/`Response`
//! and feeds the single-threaded kernel over a channel. The one sanctioned
//! ambient-IO module besides `prod` — nothing here makes decisions; it moves
//! bytes. Everything behavioral stays in `crate::http`, inside the sim.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::io;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::time::Duration;

use http_body_util::{BodyExt, Full, Limited};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;

use charpente_core::io::{Clock, Disk, Instant};

use crate::http::{App, Method, Request, Response};
use crate::kernel;
use crate::repl::{self, Repl};

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
    let (kernel_tx, kernel_rx) = mpsc::channel::<(Request, kernel::Reply)>();

    // The kernel: one thread owning the App — requests, due jobs, and cron
    // ticks all serialize through Kernel::run's event loop (DESIGN §1).
    std::thread::spawn(move || {
        let clock = crate::prod::ProdClock::new();
        let mut kernel = match kernel::Kernel::new(app, Box::new(crate::prod::ProdClock::new())) {
            Ok(kernel) => kernel,
            Err(e) => {
                eprintln!("charpente kernel failed to start — fix the store/log and restart: {e}");
                return;
            }
        };
        let mut source = ProdSource {
            rx: kernel_rx,
            stash: None,
            clock,
        };
        if let Err(e) = kernel.run(&mut source) {
            eprintln!(
                "charpente kernel stopped on a platform IO failure — restart to recover: {e}"
            );
        }
    });

    http_edge(addr, body_cap, kernel_tx)
}

/// A replicated node: the same HTTP edge, but the kernel thread opens a `Repl`
/// (over `repl_disk` for the durable role, `ProdNet` for the wire) and runs
/// `Kernel::with_replication`. A follower serves reads and answers writes with
/// 421; a primary ships its log. `repl_cfg.listen` is the replication port
/// (distinct from the HTTP `addr`); a startup role/config mismatch is surfaced
/// on stderr, config never overriding the durable role (DESIGN §5).
pub fn serve_replicated(
    addr: &str,
    app: App,
    repl_disk: Box<dyn Disk>,
    repl_cfg: repl::ReplConfig,
) -> io::Result<Server> {
    let body_cap = app.config().body_cap;
    let (kernel_tx, kernel_rx) = mpsc::channel::<(Request, kernel::Reply)>();

    std::thread::spawn(move || {
        let clock = crate::prod::ProdClock::new();
        let mut app = app;
        let repl = match Repl::open(
            Box::new(crate::prod::ProdNet),
            repl_disk,
            repl_cfg,
            app.db_mut(),
        ) {
            Ok((repl, notice)) => {
                if let Some(notice) = notice {
                    eprintln!("charpente replication: {notice}");
                }
                repl
            }
            Err(e) => {
                eprintln!(
                    "charpente replication failed to start — fix the role file and restart: {e}"
                );
                return;
            }
        };
        let mut kernel = match kernel::Kernel::with_replication(
            app,
            Box::new(crate::prod::ProdClock::new()),
            repl,
        ) {
            Ok(kernel) => kernel,
            Err(e) => {
                eprintln!("charpente kernel failed to start — fix the store/log and restart: {e}");
                return;
            }
        };
        let mut source = ProdSource {
            rx: kernel_rx,
            stash: None,
            clock,
        };
        if let Err(e) = kernel.run(&mut source) {
            eprintln!(
                "charpente kernel stopped on a platform IO failure — restart to recover: {e}"
            );
        }
    });

    http_edge(addr, body_cap, kernel_tx)
}

/// A backup node: replication only, no HTTP edge. Runs `Kernel::with_replication`
/// on the CURRENT thread against `EmptySource` (no requests ever arrive), so it
/// follows the primary, keeps its own durable copy, and can be promoted later.
/// Blocks until the kernel returns (an IO failure — restart to recover).
pub fn backup(app: App, repl_disk: Box<dyn Disk>, repl_cfg: repl::ReplConfig) -> io::Result<()> {
    let clock = crate::prod::ProdClock::new();
    let mut app = app;
    let (repl, notice) = Repl::open(
        Box::new(crate::prod::ProdNet),
        repl_disk,
        repl_cfg,
        app.db_mut(),
    )?;
    if let Some(notice) = notice {
        eprintln!("charpente replication: {notice}");
    }
    let mut kernel =
        kernel::Kernel::with_replication(app, Box::new(crate::prod::ProdClock::new()), repl)?;
    let mut source = EmptySource { clock };
    kernel.run(&mut source)
}

/// Binds the HTTP listener and spawns the hyper accept loop feeding `kernel_tx`.
/// Shared by `serve` and `serve_replicated` — the edge is identical; only the
/// kernel thread differs.
fn http_edge(
    addr: &str,
    body_cap: usize,
    kernel_tx: mpsc::Sender<(Request, kernel::Reply)>,
) -> io::Result<Server> {
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
                                    let reply: kernel::Reply = Box::new(move |response| {
                                        let _ = tx.send(response);
                                    });
                                    if kernel.send((request, reply)).is_err() {
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

/// The `RequestSource` for a backup node: no requests ever arrive, so `poll`
/// is always `None` and `wait_until` just sleeps to the deadline. The repl park
/// tick guarantees `Kernel::step` always hands back a `Some(deadline)`, so this
/// wakes every `REPL_TICK_NANOS` to pump replication IO.
struct EmptySource {
    clock: crate::prod::ProdClock,
}

impl kernel::RequestSource for EmptySource {
    fn poll(&mut self) -> Option<(Request, kernel::Reply)> {
        None
    }

    fn wait_until(&mut self, deadline: Option<Instant>) -> kernel::Wake {
        match deadline {
            Some(deadline) => {
                let now = self.clock.now();
                if deadline > now {
                    self.clock.sleep(deadline.duration_since(now));
                }
            }
            // A repl-configured kernel always sets a deadline (the park tick);
            // guard the impossible None so a backup never busy-spins.
            None => self.clock.sleep(Duration::from_millis(5)),
        }
        kernel::Wake::Deadline
    }
}

/// Feeds `Kernel::run` from hyper's channel — the production `RequestSource`
/// (DESIGN §1 same-shape rule: `SimSource` is the sim's counterpart).
struct ProdSource {
    rx: mpsc::Receiver<(Request, kernel::Reply)>,
    stash: Option<(Request, kernel::Reply)>,
    clock: crate::prod::ProdClock,
}

impl kernel::RequestSource for ProdSource {
    fn poll(&mut self) -> Option<(Request, kernel::Reply)> {
        self.stash.take().or_else(|| self.rx.try_recv().ok())
    }

    fn wait_until(&mut self, deadline: Option<Instant>) -> kernel::Wake {
        use charpente_core::io::Clock;
        use kernel::Wake;
        match deadline {
            Some(deadline) => {
                let now = self.clock.now();
                if deadline <= now {
                    return Wake::Deadline;
                }
                match self.rx.recv_timeout(deadline.duration_since(now)) {
                    Ok(item) => {
                        self.stash = Some(item);
                        Wake::Ready
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => Wake::Deadline,
                    Err(mpsc::RecvTimeoutError::Disconnected) => Wake::Shutdown,
                }
            }
            None => match self.rx.recv() {
                Ok(item) => {
                    self.stash = Some(item);
                    Wake::Ready
                }
                Err(_) => Wake::Shutdown,
            },
        }
    }
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
    use crate::http::{ApiError, AppConfig, Ctx, Route, Session};
    use crate::prod::{ProdDisk, ProdEntropy};
    use crate::repl::role::Role;
    use charpente_core::db::{Schema, Tx};
    use charpente_core::io::Entropy;
    use charpente_core::table::{Id, View};

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

    // ------------------------------------------- replicated smoke (repl T8)

    /// The smoke rows ride the platform `Session` table (already in every
    /// App's schema) so the test needs no #[table] of its own.
    fn put_row_glue(
        tx: &mut Tx,
        _ctx: &Ctx,
        captures: &[String],
        _body: &[u8],
    ) -> Result<Response, ApiError> {
        let id: u64 = captures[0].parse().expect("smoke test sends numeric ids");
        tx.put(&Session {
            id: Id::from_raw(id),
            token: format!("smoke-{id}"),
            subject: "replicated".to_string(),
        });
        Ok(Response::json(200, &id))
    }

    fn get_row_glue(
        tx: &mut Tx,
        _ctx: &Ctx,
        captures: &[String],
        _body: &[u8],
    ) -> Result<Response, ApiError> {
        let id: u64 = captures[0].parse().expect("smoke test sends numeric ids");
        let row = tx
            .get(Id::<Session>::from_raw(id))?
            .ok_or(ApiError::NotFound)?;
        Ok(Response::json(200, &row.subject))
    }

    fn smoke_routes() -> Vec<Route> {
        vec![
            Route {
                method: Method::Post,
                pattern: "/rows/:id",
                auth: false,
                name: "put_row",
                glue: put_row_glue,
            },
            Route {
                method: Method::Get,
                pattern: "/rows/:id",
                auth: false,
                name: "get_row",
                glue: get_row_glue,
            },
        ]
    }

    /// Reserve a loopback port by binding to :0 and dropping the listener —
    /// the replication listener needs a knowable address before it exists.
    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .expect("loopback bind")
            .local_addr()
            .expect("bound listener has an address")
            .port()
    }

    fn http1(addr: &str, method: &str, path: &str) -> String {
        let mut stream = std::net::TcpStream::connect(addr).expect("connect");
        stream
            .write_all(
                format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .expect("request write");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("response read");
        response
    }

    /// The end-to-end prod smoke: a primary and a follower, each a real
    /// `serve_replicated` over loopback TCP (real hyper, real `ProdNet` ship
    /// session). A row POSTed to the primary becomes readable on the FOLLOWER;
    /// a POST to the follower is refused 421 naming the primary.
    #[test]
    fn serve_replicated_ships_to_a_follower_over_loopback() {
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        let tag = u64::from_le_bytes(suffix);
        let root_p = std::env::temp_dir().join(format!("charpente-repl-p-{tag:016x}"));
        let root_f = std::env::temp_dir().join(format!("charpente-repl-f-{tag:016x}"));
        let repl_p = format!("127.0.0.1:{}", free_port());
        let repl_f = format!("127.0.0.1:{}", free_port());

        let open = |root: &std::path::Path| {
            App::open(
                Box::new(ProdDisk::new(root).unwrap()),
                Schema::new(),
                Box::new(ProdEntropy::new().unwrap()),
                AppConfig::default(),
            )
            .unwrap()
            .mount(smoke_routes())
        };

        let server_p = serve_replicated(
            "127.0.0.1:0",
            open(&root_p),
            Box::new(ProdDisk::new(&root_p).unwrap()),
            repl::ReplConfig {
                listen: Some(repl_p.clone()),
                initial_role: Role::Primary,
            },
        )
        .unwrap();
        let server_f = serve_replicated(
            "127.0.0.1:0",
            open(&root_f),
            Box::new(ProdDisk::new(&root_f).unwrap()),
            repl::ReplConfig {
                listen: Some(repl_f),
                initial_role: Role::Follower {
                    primary: repl_p.clone(),
                },
            },
        )
        .unwrap();

        // The primary commits the write (200 only after the WAL sync).
        let response = http1(&server_p.local_addr(), "POST", "/rows/7");
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");

        // The follower converges: poll until the replicated row reads back.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let response = http1(&server_f.local_addr(), "GET", "/rows/7");
            if response.starts_with("HTTP/1.1 200") {
                assert!(response.ends_with("\"replicated\""), "{response}");
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the follower never served the replicated row — last response: {response}"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }

        // A write on the follower is refused 421 naming the primary.
        let response = http1(&server_f.local_addr(), "POST", "/rows/9");
        assert!(response.starts_with("HTTP/1.1 421"), "{response}");
        assert!(
            response.contains(&repl_p),
            "the 421 must name the primary's address ({repl_p}): {response}"
        );

        std::fs::remove_dir_all(root_p).unwrap();
        std::fs::remove_dir_all(root_f).unwrap();
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
