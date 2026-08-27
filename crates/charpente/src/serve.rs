//! The hyper edge (spec §edge): translates wire↔`http::Request`/`Response`
//! and feeds the single-threaded kernel over a channel. The one sanctioned
//! ambient-IO module besides `prod` — nothing here makes decisions; it moves
//! bytes. Everything behavioral stays in `crate::http`, inside the sim.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::mpsc;
use std::task::{Context, Poll};
use std::time::Duration;

use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;

use charpente_core::io::{Clock, Disk, Instant, ObjectStore};

use crate::archive::{Archive, ArchiveConfig};
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

/// An archive node (archive spec §4): no HTTP edge, no replication — the
/// kernel runs on the CURRENT thread against `EmptySource`, exactly as
/// `backup` does, so the archive arm and a hot standby are the same shape
/// and compose. Blocks until the kernel returns.
///
/// `Archive::open` runs here rather than in the caller because it registers
/// the retention pin on the `Db`, which must happen before `Kernel` consumes
/// the `App`.
///
/// NOT a sidecar. There is no `Repl` here, so `Db::writes_allowed()` is true
/// and this node is a primary in every respect but its edges: it runs the
/// app's JOBS and CRON while it archives, and it will accept writes from
/// anything that reaches its `Db`. It is "a primary whose only edge is the
/// archive", not "an archiver attached to someone else's database".
///
/// **`cfg.writer` is taken verbatim, and nothing here defaults, derives, or
/// synthesises it.** `ArchiveConfig::writer` is a stability contract, not a
/// label: the manifest comparison is identity ALONE, so a `writer` derived
/// from anything a redeploy moves — a bind address, a port, a pod name, a
/// container id, a PID — turns an ordinary redeployment into a PERMANENT
/// refusal on a bucket this node genuinely owns. The archive latches
/// `stopped()` at `adopt`, before uploading anything, for the life of the
/// process; the only recovery is the operator action `foreign_writer_notice`
/// names, which abandons the live generation's objects. A helpful-looking
/// default here would convert an availability event into that, and the
/// failure would appear only on the second deployment.
///
/// NOT CONSTRUCTIBLE: an archive that also replicates. Spec §4 calls
/// archiving from a standby the deployment most operators should choose, and
/// `Kernel::build` does take both — but `build` is PRIVATE, and all three
/// public constructors pass one of them `None`. So this is not merely
/// un-wired at the `serve` layer: reaching that deployment needs a new
/// public constructor, not just a new function here. The design is otherwise
/// ready for it — the archive arm gates on `Db::verifying()` rather than
/// `writes_allowed()`, so a follower archives normally.
pub fn archive(app: App, store: Box<dyn ObjectStore>, cfg: ArchiveConfig) -> io::Result<()> {
    let clock = crate::prod::ProdClock::new();
    let mut app = app;
    let archive = Archive::open(cfg, app.db_mut())?;
    let mut kernel =
        kernel::Kernel::with_archive(app, Box::new(crate::prod::ProdClock::new()), archive, store)?;
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
                                    let (tx, rx) = tokio::sync::oneshot::channel::<Delivered>();
                                    let reply: kernel::Reply = Box::new(HttpReply { tx });
                                    if kernel.send((request, reply)).is_err() {
                                        from_kernel_response(Response::error(
                                            500,
                                            "the kernel thread is gone",
                                        ))
                                    } else {
                                        match rx.await {
                                            Ok(Delivered::Response(response)) => {
                                                from_kernel_response(response)
                                            }
                                            Ok(Delivered::Stream(response, frames)) => {
                                                from_kernel_stream(response, frames)
                                            }
                                            Err(_) => from_kernel_response(Response::error(
                                                500,
                                                "the kernel dropped the request",
                                            )),
                                        }
                                    }
                                }
                                Err(response) => from_kernel_response(response),
                            };
                            Ok::<_, std::convert::Infallible>(response)
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

/// Frames buffered per SSE connection before the kernel's drop-on-full
/// policy (T5) costs a slow consumer its tail. Fixed, no entropy. Also sizes
/// the edge-side byte channel (`from_kernel_stream`) — one bound, one place
/// a slow connection can back up before something gives.
const EVENT_SINK_BUFFER: usize = 256;

/// Idle gap after which the connection gets a `:hb` comment frame (spec's
/// wire format) instead of silence — keeps idle SSE connections (and any
/// proxy in between) from timing out. Purely wire liveness, like
/// response-write timing: no core decision depends on it, so `sse_pump`
/// timing it with `Receiver::recv_timeout` (not the `Clock` trait) is the
/// edge's business, not the sim's.
const SSE_HEARTBEAT: Duration = Duration::from_secs(15);

/// What the oneshot carries from the kernel back to the hyper connection
/// task: the one-shot response (`Respond::send`), or — for an SSE upgrade
/// (`Respond::stream`) — the opening response plus the receiver half this
/// task's writer (`sse_pump`) drains into wire bytes. The oneshot that
/// already carries the response to the connection task is the natural
/// vehicle for the receiver too; no second hand-off channel needed.
enum Delivered {
    Response(Response),
    Stream(Response, mpsc::Receiver<kernel::EventFrame>),
}

/// The prod `Respond`: `send` completes the request over the existing
/// oneshot back to the hyper service future. `stream` is the SSE upgrade's
/// other half: it delivers the opening response over that same oneshot,
/// riding along the receiver half of a fresh bounded channel, and hands the
/// sender half back as the sink the kernel pushes frames into. The
/// connection task (`from_kernel_stream`) is the receiver's only reader.
struct HttpReply {
    tx: tokio::sync::oneshot::Sender<Delivered>,
}

impl kernel::Respond for HttpReply {
    fn send(self: Box<Self>, response: Response) {
        let _ = self.tx.send(Delivered::Response(response));
    }

    fn stream(self: Box<Self>, response: Response) -> Box<dyn kernel::EventSink> {
        let (frame_tx, frame_rx) =
            std::sync::mpsc::sync_channel::<kernel::EventFrame>(EVENT_SINK_BUFFER);
        let _ = self.tx.send(Delivered::Stream(response, frame_rx));
        Box::new(BoundedSink { tx: frame_tx })
    }
}

/// The bounded-channel sink `sse_pump` drains. `try_send` maps directly onto
/// `SinkStatus` — full or disconnected both cost the tail its connection
/// (kernel policy, T5): a slow or dead consumer never blocks kernel
/// progress.
struct BoundedSink {
    tx: std::sync::mpsc::SyncSender<kernel::EventFrame>,
}

impl kernel::EventSink for BoundedSink {
    fn send(&mut self, frame: &kernel::EventFrame) -> kernel::SinkStatus {
        match self.tx.try_send(frame.clone()) {
            Ok(()) => kernel::SinkStatus::Ok,
            Err(std::sync::mpsc::TrySendError::Full(_)) => kernel::SinkStatus::Full,
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => kernel::SinkStatus::Closed,
        }
    }
}

/// Opens the SSE response over the same header/status path `from_kernel_response`
/// uses, then wires the kernel's frame channel to the body hyper streams to
/// the client: `sse_pump` runs on the blocking-task pool (`spawn_blocking` —
/// runtime-managed, not a raw OS thread) forwarding wire bytes into the async
/// channel `SseBody` polls. Both outlive this call, running for the rest of
/// the connection.
fn from_kernel_stream(
    response: Response,
    frames: mpsc::Receiver<kernel::EventFrame>,
) -> hyper::Response<BoxBody<Bytes, std::convert::Infallible>> {
    let (byte_tx, byte_rx) = tokio::sync::mpsc::channel::<Bytes>(EVENT_SINK_BUFFER);
    tokio::task::spawn_blocking(move || sse_pump(frames, byte_tx));
    let mut builder = hyper::Response::builder().status(response.status);
    for (key, value) in &response.headers {
        builder = builder.header(key, value);
    }
    builder
        .body(SseBody { rx: byte_rx }.boxed())
        .expect("kernel SSE responses are valid HTTP")
}

/// Drains one SSE connection's kernel-facing frame channel on a
/// `spawn_blocking` thread — `std::sync::mpsc::Receiver::recv` blocks, and
/// this runtime is `current_thread` (`http_edge`), so a blocking recv on the
/// async worker thread would stall every other connection. `recv_timeout`
/// doubles as the heartbeat clock (`SSE_HEARTBEAT`): a frame within the
/// window forwards as wire bytes, silence past it forwards a `:hb` comment.
/// Returns — dropping `frames` — when the kernel drops the tail (`frames`
/// disconnects) or the client is gone (`byte_tx.blocking_send` fails because
/// hyper dropped the response body, hence `byte_rx`), whichever comes
/// first. That drop is the whole disconnect story: this task never signals
/// the kernel directly — dropping `frames` here is what makes the kernel's
/// next `try_send` observe `Disconnected` (`SinkStatus::Closed`, T5).
fn sse_pump(frames: mpsc::Receiver<kernel::EventFrame>, byte_tx: tokio::sync::mpsc::Sender<Bytes>) {
    loop {
        let bytes = match frames.recv_timeout(SSE_HEARTBEAT) {
            Ok(frame) => encode_frame(&frame),
            Err(mpsc::RecvTimeoutError::Timeout) => Bytes::from_static(b":hb\n\n"),
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        };
        if byte_tx.blocking_send(bytes).is_err() {
            return;
        }
    }
}

/// Wire form (spec): `id: <id>.<seq>` — matching `Cursor::wire` exactly is
/// what makes the browser's native `Last-Event-ID` resume work — then
/// `event:`, `data:`, and the blank line ending the frame.
fn encode_frame(frame: &kernel::EventFrame) -> Bytes {
    Bytes::from(format!(
        "id: {}.{}\nevent: {}\ndata: {}\n\n",
        frame.id, frame.seq, frame.name, frame.json
    ))
}

/// The SSE response body: a thin `Body` impl over the async byte channel
/// `sse_pump` feeds. `Receiver::poll_recv` is already waker-aware, so this
/// needs no `Stream`/`futures` dependency for a body this shallow.
struct SseBody {
    rx: tokio::sync::mpsc::Receiver<Bytes>,
}

impl hyper::body::Body for SseBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        match self.get_mut().rx.poll_recv(cx) {
            Poll::Ready(Some(bytes)) => Poll::Ready(Some(Ok(Frame::data(bytes)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// The `RequestSource` for a node with no HTTP edge — `backup` and
/// `archive` both run against it. No request ever arrives, so `poll` is
/// always `None` and `wait_until` just sleeps to the deadline.
///
/// Under `backup` the repl park tick guarantees `Kernel::step` always hands
/// back a `Some(deadline)`, so it wakes every `REPL_TICK_NANOS` to pump
/// replication IO. Under `archive` there is no park tick and no
/// timer-carrying source at all — the archive arm is deliberately clock-free
/// (spec §8: "the archive arm never reads wall-clock time, directly or
/// transitively"), so `None` is its ORDINARY deadline rather than an
/// impossible one, and the 5 ms floor below is load-bearing: it is the park
/// that paces the kernel's archive retry after a failed store operation.
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
            // The ordinary case for `archive` and the impossible one for
            // `backup` (the park tick); either way this is what keeps a
            // request-less node from busy-spinning.
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
    // `path_and_query` keeps the `?cursor=…` suffix; `Request::new` is the
    // one place that splits it into `path`/`query` (the SSE cursor param's
    // only v0 consumer).
    let raw_path = match req.uri().path_and_query() {
        Some(pq) => pq.as_str().to_string(),
        None => req.uri().path().to_string(),
    };
    let mut request = Request::new(method, raw_path);
    for (name, value) in req.headers() {
        if let Ok(v) = value.to_str() {
            request
                .headers
                .insert(name.as_str().to_ascii_lowercase(), v.to_string());
        }
    }
    match Limited::new(req.into_body(), body_cap).collect().await {
        Ok(collected) => {
            request.body = collected.to_bytes().to_vec();
            Ok(request)
        }
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

fn from_kernel_response(
    response: Response,
) -> hyper::Response<BoxBody<Bytes, std::convert::Infallible>> {
    let mut builder = hyper::Response::builder().status(response.status);
    for (key, value) in &response.headers {
        builder = builder.header(key, value);
    }
    builder
        .body(Full::new(Bytes::from(response.body)).boxed())
        .expect("kernel responses are valid HTTP")
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read, Write};

    use super::*;
    use crate::http::{ApiError, AppConfig, Ctx, Cursor, Route, RouteGlue, Session, Subscription};
    use crate::prod::{ProdDisk, ProdEntropy};
    use crate::repl::role::Role;
    use charpente_core::db::{Event, Schema, Tx};
    use charpente_core::io::Entropy;
    use charpente_core::table::{Codec, Id, View};

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
            glue: RouteGlue::Http(hello_glue),
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
            token_hash: format!("smoke-{id}"),
            subject: "replicated".to_string(),
            expires_at: u64::MAX,
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
                glue: RouteGlue::Http(put_row_glue),
            },
            Route {
                method: Method::Get,
                pattern: "/rows/:id",
                auth: false,
                name: "get_row",
                glue: RouteGlue::Http(get_row_glue),
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
                peer_secret: None,
                strict_witnesses: false,
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
                peer_secret: None,
                strict_witnesses: false,
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
            glue: RouteGlue::Http(hello_glue),
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

    // ------------------------------------------------------------ SSE edge

    /// The smoke event (realtime T7): hand-implemented, not `#[event]`, for
    /// the same reason the smoke routes above are hand-built —
    /// `charpente-macros` emit `::charpente` paths meant for app crates, not
    /// unit tests living inside the `charpente` crate itself.
    #[derive(serde::Serialize)]
    struct Ping {
        n: u64,
    }

    impl Codec for Ping {
        fn encode(&self, out: &mut Vec<u8>) {
            self.n.encode(out);
        }

        fn decode(input: &mut &[u8]) -> std::io::Result<Ping> {
            Ok(Ping {
                n: u64::decode(input)?,
            })
        }
    }

    impl Event for Ping {
        const NAME: &'static str = "ping";
    }

    fn emit_ping_glue(
        tx: &mut Tx,
        _ctx: &Ctx,
        captures: &[String],
        _body: &[u8],
    ) -> Result<Response, ApiError> {
        let n: u64 = captures[0].parse().expect("smoke test sends numeric n");
        tx.emit(Ping { n });
        Ok(Response::json(200, &n))
    }

    fn feed_glue(
        tx: &mut Tx,
        _ctx: &Ctx,
        _captures: &[String],
        cursor: Option<Cursor>,
    ) -> Result<Subscription, ApiError> {
        let _ = tx;
        Ok(Subscription::of::<Ping>().cursor(cursor))
    }

    fn sse_routes() -> Vec<Route> {
        vec![
            Route {
                method: Method::Post,
                pattern: "/emit/:n",
                auth: false,
                name: "emit_ping",
                glue: RouteGlue::Http(emit_ping_glue),
            },
            Route {
                method: Method::Get,
                pattern: "/feed",
                auth: false,
                name: "feed",
                glue: RouteGlue::Sse(feed_glue),
            },
        ]
    }

    /// Reads HTTP/1.1 status + headers off a raw stream up to the blank
    /// line, leaving the reader positioned at the first byte of the
    /// (chunked) body.
    fn read_status_and_headers(reader: &mut impl BufRead) -> String {
        let mut headers = String::new();
        loop {
            let mut line = String::new();
            let n = reader.read_line(&mut line).expect("header line read");
            assert!(n > 0, "connection closed before headers ended");
            if line == "\r\n" {
                break;
            }
            headers.push_str(&line);
        }
        headers
    }

    /// Reads one chunked-transfer-encoding chunk — hyper chunks the SSE body
    /// (no `Content-Length`: the body's length is unknown up front).
    /// `None` on the terminal zero-length chunk or a closed connection.
    fn read_chunk(reader: &mut impl BufRead) -> Option<Vec<u8>> {
        let mut size_line = String::new();
        if reader.read_line(&mut size_line).unwrap_or(0) == 0 {
            return None;
        }
        let size = usize::from_str_radix(size_line.trim(), 16).expect("chunk size is hex");
        if size == 0 {
            return None;
        }
        let mut data = vec![0u8; size];
        reader.read_exact(&mut data).expect("chunk data read");
        let mut crlf = [0u8; 2];
        reader
            .read_exact(&mut crlf)
            .expect("chunk trailing CRLF read");
        Some(data)
    }

    /// Dechunks off `reader` into `pending` until it holds one complete SSE
    /// frame (`id:`/`event:`/`data:` lines, terminated by the wire format's
    /// blank line), then splits that frame off and returns it — without the
    /// trailing blank line, so `.lines()` gives exactly the frame's fields.
    fn read_sse_frame(reader: &mut impl BufRead, pending: &mut String) -> String {
        loop {
            if let Some(at) = pending.find("\n\n") {
                let frame = pending[..at].to_string();
                *pending = pending[at + 2..].to_string();
                return frame;
            }
            let chunk =
                read_chunk(reader).expect("connection closed before a full SSE frame arrived");
            pending.push_str(std::str::from_utf8(&chunk).expect("SSE bytes are UTF-8"));
        }
    }

    /// The realtime T7 exit criterion, over real bytes: an SSE GET, two
    /// emits delivered with sequential `id:`/`event:`/`data:` lines, a
    /// dropped connection, a third emit while nobody is subscribed, then a
    /// `Last-Event-ID` reconnect that resumes with exactly the missed event
    /// — no replay, no gap. Closes with the 409 divergence text (hard rule
    /// 5) surfacing over a real socket for a cursor ahead of everything this
    /// node has ever minted.
    #[test]
    fn sse_over_loopback_streams_ids_and_resumes_with_last_event_id() {
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        let root =
            std::env::temp_dir().join(format!("charpente-sse-{:016x}", u64::from_le_bytes(suffix)));
        let app = App::open(
            Box::new(ProdDisk::new(&root).unwrap()),
            Schema::new(),
            Box::new(ProdEntropy::new().unwrap()),
            AppConfig::default(),
        )
        .unwrap()
        .mount(sse_routes());

        let server = serve("127.0.0.1:0", app).unwrap();
        let addr = server.local_addr();

        // Subscribe from now (no cursor) — read the opening response before
        // POSTing anything, so the subscribe is provably registered first.
        let mut sub = std::net::TcpStream::connect(&addr).unwrap();
        sub.set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        sub.write_all(b"GET /feed HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let mut reader = BufReader::new(sub);
        let headers = read_status_and_headers(&mut reader);
        assert!(headers.starts_with("HTTP/1.1 200"), "{headers}");
        assert!(
            headers.contains("content-type: text/event-stream"),
            "{headers}"
        );

        // Two emits, each its own short-lived connection.
        let r1 = http1(&addr, "POST", "/emit/1");
        assert!(r1.starts_with("HTTP/1.1 200"), "{r1}");
        let r2 = http1(&addr, "POST", "/emit/2");
        assert!(r2.starts_with("HTTP/1.1 200"), "{r2}");

        let mut pending = String::new();
        let frame1 = read_sse_frame(&mut reader, &mut pending);
        let lines1: Vec<&str> = frame1.lines().collect();
        assert!(lines1[0].starts_with("id: 0."), "{frame1}");
        assert_eq!(lines1[1], "event: ping", "{frame1}");
        assert_eq!(lines1[2], "data: {\"n\":1}", "{frame1}");

        let frame2 = read_sse_frame(&mut reader, &mut pending);
        let lines2: Vec<&str> = frame2.lines().collect();
        assert!(lines2[0].starts_with("id: 1."), "{frame2}");
        assert_eq!(lines2[1], "event: ping", "{frame2}");
        assert_eq!(lines2[2], "data: {\"n\":2}", "{frame2}");
        let last_cursor = lines2[0].trim_start_matches("id: ").to_string();

        // The client is gone.
        drop(reader);

        // A third emit while nobody is subscribed — the tail that would
        // have missed it is dead; a resuming client must not be.
        let r3 = http1(&addr, "POST", "/emit/3");
        assert!(r3.starts_with("HTTP/1.1 200"), "{r3}");

        // Reconnect with Last-Event-ID: exactly the third event arrives.
        let mut resume = std::net::TcpStream::connect(&addr).unwrap();
        resume
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        resume
            .write_all(
                format!(
                    "GET /feed HTTP/1.1\r\nHost: localhost\r\nLast-Event-ID: {last_cursor}\r\n\r\n"
                )
                .as_bytes(),
            )
            .unwrap();
        let mut resume_reader = BufReader::new(resume);
        let resume_headers = read_status_and_headers(&mut resume_reader);
        assert!(
            resume_headers.starts_with("HTTP/1.1 200"),
            "{resume_headers}"
        );
        let mut resume_pending = String::new();
        let frame3 = read_sse_frame(&mut resume_reader, &mut resume_pending);
        let lines3: Vec<&str> = frame3.lines().collect();
        assert!(lines3[0].starts_with("id: 2."), "{frame3}");
        assert_eq!(lines3[1], "event: ping", "{frame3}");
        assert_eq!(lines3[2], "data: {\"n\":3}", "{frame3}");

        // A cursor ahead of everything this node has ever minted is a named
        // divergence, 409, over real HTTP (hard rule 5 — cheap to prove
        // here since it needs no extra fixture).
        let mut garbage = std::net::TcpStream::connect(&addr).unwrap();
        garbage
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        garbage
            .write_all(
                b"GET /feed HTTP/1.1\r\nHost: localhost\r\n\
                  Last-Event-ID: 999.0\r\nConnection: close\r\n\r\n",
            )
            .unwrap();
        let mut garbage_response = String::new();
        garbage.read_to_string(&mut garbage_response).unwrap();
        assert!(
            garbage_response.starts_with("HTTP/1.1 409"),
            "{garbage_response}"
        );
        assert!(garbage_response.contains("diverged"), "{garbage_response}");

        std::fs::remove_dir_all(root).unwrap();
    }
}
