//! The hyper edge (spec §edge): translates wire↔`http::Request`/`Response`
//! and feeds the single-threaded kernel over a channel. The one sanctioned
//! ambient-IO module besides `prod` — nothing here makes decisions; it moves
//! bytes. Everything behavioral stays in `crate::http`, inside the sim.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll};
use std::time::Duration;

use base64ct::Encoding;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;

use charpente_core::db::Refusal;
use charpente_core::io::{
    Clock, Disk, Instant, ObjectStore, Outbound, OutboundCall, OutboundReply,
};

use crate::archive::{Archive, ArchiveConfig};
use crate::http::{App, Method, Request, Response};
use crate::kernel;
use crate::outbound::Credential;
use crate::repl::{self, Repl};

/// A running server: the HTTP edge, and the kernel thread it feeds. v0 has no
/// graceful shutdown. `wait` is how a binary learns the kernel stopped —
/// dropping this without calling it is the old `serve(addr, app)?;` + park
/// pattern, which never learns a stopped kernel should end the process
/// (spec §5), so discarding it unused is a warning under `-D warnings`.
#[must_use]
pub struct Server {
    addr: SocketAddr,
    kernel: std::thread::JoinHandle<i32>,
}

impl Server {
    pub fn local_addr(&self) -> String {
        self.addr.to_string()
    }

    /// Blocks until the kernel thread ends, and returns the exit code the
    /// process should end with:
    /// - `1` when the kernel stopped on an error (its stop line is already
    ///   printed — the first line of the kernel's final stderr output,
    ///   naming the refusal or IO failure and the fix; a multi-type
    ///   foreign-shape jobs refusal continues one job type per line after
    ///   it), OR when the request channel closed without an error
    ///   (`EDGE_GONE_STOP_LINE`, already printed) — v0 has no admin-triggered
    ///   shutdown, so under `serve`/`serve_replicated` that only means the
    ///   HTTP edge thread ended first, most likely a panic (a
    ///   `Restart=on-failure` supervisor needs a nonzero code to act on a
    ///   kernel that can no longer answer anything);
    /// - `101` when the thread panicked (the panic message is already printed);
    /// - `0` never happens today. It is reserved for a future admin-triggered
    ///   graceful shutdown, which would need its own line distinguishing it
    ///   from the edge-gone case above before reusing this code.
    ///
    /// Serve with `std::process::exit(serve(addr, app)?.wait())`: a kernel that
    /// stopped must end the process, or a supervisor sees a live process
    /// answering every request 500 and never restarts it (payload-shapes spec
    /// §5).
    pub fn wait(self) -> i32 {
        self.kernel.join().unwrap_or(101)
    }
}

/// `Kernel::run` returns `Ok(())` on `Wake::Shutdown` (kernel/mod.rs), which
/// v0 has no admin-triggered path to under `serve`/`serve_replicated`. The
/// request channel's `Sender` belongs to the HTTP edge thread alone
/// (`http_edge`), and the kernel thread is spawned only after the listener
/// is bound (`bind_http`), so the channel closes one way: that edge thread
/// ended first — a panic, most likely. A live kernel with no edge left to
/// feed it answers nothing, so this is named and non-zero rather than the
/// clean-exit `0` a `Restart=on-failure` supervisor would take as "nothing
/// to do here."
const EDGE_GONE_STOP_LINE: &str = "charpente kernel stopped: the request channel closed, which \
     only happens when the HTTP edge thread ended before the kernel did (most likely a panic) — \
     check the edge thread's own panic output above; this is not a clean shutdown, v0 has none";

/// The kernel thread's stop line — the first line of the kernel's final stderr
/// output. A refusal is named as one because restarting the same binary does
/// not by itself clear it; every refusal's own text names what must change
/// first. A platform IO failure really can clear on a restart.
fn kernel_stop_line(started: bool, e: &io::Error) -> String {
    match (started, Refusal::of(e).is_some()) {
        (false, true) => format!("charpente kernel refused to start: {e}"),
        (false, false) => {
            format!("charpente kernel failed to start — fix the store/log and restart: {e}")
        }
        (true, true) => format!("charpente kernel stopped on a refusal: {e}"),
        (true, false) => {
            format!("charpente kernel stopped on a platform IO failure — restart to recover: {e}")
        }
    }
}

/// `serve_with` and no operator outbound settings: each declared host is
/// reached at `https://<host>:443`. `Err` when the outbound setup refuses (a
/// `Refusal`, nothing running) or the HTTP listener cannot be bound.
pub fn serve(addr: &str, app: App) -> io::Result<Server> {
    serve_with(addr, app, OutboundOptions::default())
}

/// `serve`, with the operator's outbound settings (outbound-effects spec §9).
/// The outbound client is built first, reading each credential variable
/// once, then the HTTP listener is bound; both run before the kernel thread
/// exists, so an `Err` leaves nothing running. It is one of three: a
/// `Refusal` from the outbound setup, a platform failure building the
/// client's runtime or thread, or a failed bind, marked `BindFailed`.
pub fn serve_with(addr: &str, app: App, outbound: OutboundOptions) -> io::Result<Server> {
    let body_cap = app.config().body_cap;
    let outbound = ProdOutbound::new(&app, &outbound)?;
    let listener = bind_http(addr)?;
    let (kernel_tx, kernel_rx) = mpsc::channel::<(Request, kernel::Reply)>();

    // The kernel: one thread owning the App — requests, due jobs, and cron
    // ticks all serialize through Kernel::run's event loop (DESIGN §1).
    let kernel = std::thread::spawn(move || -> i32 {
        let clock = crate::prod::ProdClock::new();
        let builder = kernel::Kernel::builder(app, Box::new(crate::prod::ProdClock::new()));
        let mut kernel = match build_kernel(builder, outbound) {
            Ok(kernel) => kernel,
            Err(e) => {
                eprintln!("{}", kernel_stop_line(false, &e));
                return 1;
            }
        };
        let mut source = ProdSource {
            rx: kernel_rx,
            stash: None,
            clock,
        };
        match kernel.run(&mut source) {
            Ok(()) => {
                eprintln!("{EDGE_GONE_STOP_LINE}");
                1
            }
            Err(e) => {
                eprintln!("{}", kernel_stop_line(true, &e));
                1
            }
        }
    });

    Ok(http_edge(listener, body_cap, kernel_tx, kernel))
}

/// A replicated node: the same HTTP edge, but the kernel thread opens a `Repl`
/// (over `repl_disk` for the durable role, `ProdNet` for the wire) and builds
/// a replicating kernel. A follower serves reads and answers writes with
/// 421; a primary ships its log. `repl_cfg.listen` is the replication port
/// (distinct from the HTTP `addr`); a startup role/config mismatch is surfaced
/// on stderr, config never overriding the durable role (DESIGN §5). A
/// primary runs jobs, so the outbound client is built as `serve` builds it,
/// with no operator settings. `Err` when the outbound setup refuses or the
/// HTTP listener cannot be bound, before any kernel thread exists — as for
/// `serve`.
pub fn serve_replicated(
    addr: &str,
    app: App,
    repl_disk: Box<dyn Disk>,
    repl_cfg: repl::ReplConfig,
) -> io::Result<Server> {
    let body_cap = app.config().body_cap;
    let outbound = ProdOutbound::new(&app, &OutboundOptions::default())?;
    let listener = bind_http(addr)?;
    let (kernel_tx, kernel_rx) = mpsc::channel::<(Request, kernel::Reply)>();

    let kernel = std::thread::spawn(move || -> i32 {
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
                // Each `Repl::open` error names its own fix: a corrupt role
                // file, a listen address that cannot be bound, an empty peer
                // secret.
                eprintln!("charpente replication failed to start: {e}");
                return 1;
            }
        };
        let builder =
            kernel::Kernel::builder(app, Box::new(crate::prod::ProdClock::new())).replication(repl);
        let mut kernel = match build_kernel(builder, outbound) {
            Ok(kernel) => kernel,
            Err(e) => {
                eprintln!("{}", kernel_stop_line(false, &e));
                return 1;
            }
        };
        let mut source = ProdSource {
            rx: kernel_rx,
            stash: None,
            clock,
        };
        match kernel.run(&mut source) {
            Ok(()) => {
                eprintln!("{EDGE_GONE_STOP_LINE}");
                1
            }
            Err(e) => {
                eprintln!("{}", kernel_stop_line(true, &e));
                1
            }
        }
    });

    Ok(http_edge(listener, body_cap, kernel_tx, kernel))
}

/// A backup node: replication only, no HTTP edge. Runs a replicating kernel
/// on the CURRENT thread against `EmptySource` (no requests ever arrive), so it
/// follows the primary, keeps its own durable copy, and can be promoted later.
/// Blocks until the kernel returns: `Err` on a platform IO failure or a named
/// `Refusal` (`Refusal::of`). A backup is promoted into a primary, so it
/// builds the outbound client first, as `serve` does with no operator
/// settings, and a missing credential refuses before replication opens.
pub fn backup(app: App, repl_disk: Box<dyn Disk>, repl_cfg: repl::ReplConfig) -> io::Result<()> {
    let outbound = ProdOutbound::new(&app, &OutboundOptions::default())?;
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
    let builder =
        kernel::Kernel::builder(app, Box::new(crate::prod::ProdClock::new())).replication(repl);
    let mut kernel = build_kernel(builder, outbound)?;
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
/// NOT WIRED HERE: an archive that also replicates. Spec §4 calls archiving
/// from a standby the deployment most operators should choose, and
/// `Kernel::builder` takes both (`.replication(repl).archive(archive,
/// store)`), so what is missing is a `serve` entry point that opens both. The
/// design is otherwise ready for it — the archive arm gates on
/// `Db::verifying()` rather than `writes_allowed()`, so a follower archives
/// normally.
///
/// Because it runs jobs, it builds the outbound client first, as `serve`
/// does with no operator settings: a missing credential refuses before the
/// archive opens.
pub fn archive(app: App, store: Box<dyn ObjectStore>, cfg: ArchiveConfig) -> io::Result<()> {
    let outbound = ProdOutbound::new(&app, &OutboundOptions::default())?;
    let clock = crate::prod::ProdClock::new();
    let mut app = app;
    let archive = Archive::open(cfg, app.db_mut())?;
    let builder = kernel::Kernel::builder(app, Box::new(crate::prod::ProdClock::new()))
        .archive(archive, store);
    let mut kernel = build_kernel(builder, outbound)?;
    let mut source = EmptySource { clock };
    kernel.run(&mut source)
}

/// `builder`'s kernel, with the outbound client when the app calls out.
fn build_kernel(
    builder: kernel::KernelBuilder,
    outbound: Option<ProdOutbound>,
) -> io::Result<kernel::Kernel> {
    match outbound {
        Some(ob) => builder.outbound(Box::new(ob)).build(),
        None => builder.build(),
    }
}

/// Binds the HTTP listener, ready for `http_edge`. `serve` and
/// `serve_replicated` call it BEFORE spawning the kernel thread, so a bind
/// failure returns before any kernel exists: nothing is left running
/// detached, and the caller names the failure (`launch` does).
fn bind_http(addr: &str) -> io::Result<(std::net::TcpListener, SocketAddr)> {
    let listener = std::net::TcpListener::bind(addr).map_err(BindFailed::wrap)?;
    let local = listener.local_addr()?;
    listener.set_nonblocking(true)?;
    Ok((listener, local))
}

/// Marks the HTTP bind's failure, the way `Refusal` marks a refusal:
/// `serve_with` builds the outbound client and then binds, both before any
/// kernel exists, and only the bind's failure is fixed by freeing the
/// address, so `launch` must tell it apart. It prints as the bind's own
/// error, and keeps its kind.
pub(crate) struct BindFailed(io::Error);

impl BindFailed {
    pub(crate) fn wrap(e: io::Error) -> io::Error {
        io::Error::new(e.kind(), BindFailed(e))
    }

    pub(crate) fn of(e: &io::Error) -> bool {
        e.get_ref().is_some_and(|inner| inner.is::<BindFailed>())
    }
}

impl std::fmt::Display for BindFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

/// The bind error's own `Debug`, as `Refusal`'s is its text's: the marker is
/// for `BindFailed::of`, not for whoever prints it.
impl std::fmt::Debug for BindFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl std::error::Error for BindFailed {}

/// Spawns the hyper accept loop over the already-bound listener, feeding
/// `kernel_tx`. Shared by `serve` and `serve_replicated` — the edge is
/// identical; only the kernel thread differs.
fn http_edge(
    (listener, local): (std::net::TcpListener, SocketAddr),
    body_cap: usize,
    kernel_tx: mpsc::Sender<(Request, kernel::Reply)>,
    kernel: std::thread::JoinHandle<i32>,
) -> Server {
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

    Server {
        addr: local,
        kernel,
    }
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
/// `event:`, `data:`, and the blank line ending the frame. `charpente.error`
/// alone carries no `id:` (payload-shapes spec §3): the client's cursor must
/// not advance past an event it never received.
fn encode_frame(frame: &kernel::EventFrame) -> Bytes {
    if frame.name == kernel::ERROR_FRAME {
        return Bytes::from(format!("event: {}\ndata: {}\n\n", frame.name, frame.json));
    }
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

/// Operator settings for the outbound client (outbound-effects spec §9):
/// `--outbound-base <host>=<url>` and `--outbound-ca <path>`, parsed by
/// `launch` (Task 15) or built by hand for `serve_with`.
#[derive(Default, Clone)]
pub struct OutboundOptions {
    pub bases: Vec<(String, String)>,
    pub cas: Vec<std::path::PathBuf>,
}

/// The production `Outbound` (spec §9): hyper + rustls on a current-thread
/// tokio runtime of its own. It makes no decisions (R10): no timers, no
/// retries (`retry_canceled_requests(false)`), no redirects (a 3xx is a reply),
/// no proxy. `start` hands the call to the runtime, completions return on a
/// std channel the kernel polls, and `cancel` aborts the task.
///
/// No `Debug`, here or on `Route`: the credential values it holds must never
/// reach a log line.
pub(crate) struct ProdOutbound {
    cmds: tokio::sync::mpsc::UnboundedSender<Cmd>,
    done: mpsc::Receiver<(u64, io::Result<OutboundReply>)>,
    /// Started and neither completed nor cancelled: what `start`'s contract
    /// assertion reads. Touched on the kernel thread only.
    in_flight: std::cell::RefCell<BTreeSet<u64>>,
}

enum Cmd {
    Start(u64, OutboundCall),
    Cancel(u64),
}

/// Where one host is reached, and how it is authenticated. `scheme` is
/// `https` except for a loopback `--outbound-base`; `addr` overrides DNS but
/// never the TLS name (plan amendment A11).
struct Route {
    scheme: &'static str,
    port: Option<u16>,
    addr: Option<std::net::IpAddr>,
    headers: Vec<(String, String)>,
    max_reply: usize,
}

impl ProdOutbound {
    /// `Ok(None)` for an app whose jobs call nothing: no runtime thread exists
    /// (spec §4 invariant 6). Every setup failure is a `Refusal`, so `serve`
    /// reports "refused to start".
    pub(crate) fn new(app: &App, opts: &OutboundOptions) -> io::Result<Option<ProdOutbound>> {
        let called = app.outbound_hosts();
        if called.is_empty() {
            return Ok(None);
        }
        let refuse = |m: String| io::Error::other(Refusal(m));
        // R9 first: this client is built before the kernel, whose build runs
        // the same check, so an unmatched host set is the kernel's own
        // Refusal here too — and the `expect` below holds by construction.
        let callers = app.outbound_callers();
        crate::outbound::check_host_sets(&callers, app.outbound_config()).map_err(refuse)?;
        for (host, _) in &opts.bases {
            if !called.contains(&host.as_str()) {
                return Err(refuse(format!(
                    "--outbound-base names {host}, which no mounted job calls — the hosts this app calls are: {}",
                    called.join(", ")
                )));
            }
        }
        let mut routes = BTreeMap::new();
        for host in app.outbound_config() {
            let job = callers
                .iter()
                .find(|&&(_, h)| h == host.name())
                .map(|&(job, _)| job)
                .expect("check_host_sets above: every Host entry has a mounted job calling it");
            let mut headers = Vec::new();
            for cred in host.credentials() {
                // The refusals name the variable, never its value. A UTF-8
                // value with no control character is a valid header value, so
                // `send`'s request always builds; a Basic pair travels
                // base64-encoded, so for it the rule catches a pasted newline
                // rather than a header limit. Only a Basic password may be
                // empty: key-as-username APIs (Stripe's `-u sk_…:`) send one.
                let h = host.name();
                let var = |v: &str, may_be_empty: bool| match std::env::var(v) {
                    Err(std::env::VarError::NotPresent) => Err(refuse(format!(
                        "outbound: {v} is unset — host {h} needs it for job {job}"
                    ))),
                    Ok(value) if value.is_empty() && !may_be_empty => Err(refuse(format!(
                        "outbound: {v} is set but empty — host {h} needs it for job {job}"
                    ))),
                    Ok(value) if !value.bytes().any(|b| b.is_ascii_control()) => Ok(value),
                    _ => Err(refuse(format!(
                        "outbound: {v} holds a control character (a trailing newline, say) or is \
                         not UTF-8 — host {h} needs it for job {job}; set it to the credential alone"
                    ))),
                };
                headers.push(match cred {
                    Credential::Bearer(v) => (
                        "authorization".to_string(),
                        format!("Bearer {}", var(v, false)?),
                    ),
                    Credential::Header(name, v) => (name.clone(), var(v, false)?),
                    Credential::Basic(u, p) => {
                        let pair = format!("{}:{}", var(u, false)?, var(p, true)?);
                        (
                            "authorization".to_string(),
                            format!("Basic {}", base64ct::Base64::encode_string(pair.as_bytes())),
                        )
                    }
                });
            }
            let base = opts
                .bases
                .iter()
                .find(|(h, _)| h == host.name())
                .map(|(_, b)| b.as_str());
            let (scheme, addr, port) = match base {
                None => ("https", None, None),
                Some(b) => parse_base(host.name(), b).map_err(refuse)?,
            };
            routes.insert(
                host.name(),
                Route {
                    scheme,
                    port,
                    addr,
                    headers,
                    max_reply: host.max_reply(),
                },
            );
        }
        let tls = tls_config(&opts.cas).map_err(refuse)?;
        // Built here, not in the thread, so a runtime that cannot be built is
        // a failed start rather than a server whose every call fails.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "outbound: the client's runtime could not be built: {e} — raise the \
                         process's thread and file limits, then restart"
                    ),
                )
            })?;
        let (cmds, rx) = tokio::sync::mpsc::unbounded_channel();
        let (done_tx, done) = mpsc::channel();
        std::thread::Builder::new()
            .name("charpente-outbound".into())
            .spawn(move || outbound_runtime(rt, rx, done_tx, routes, tls))
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "outbound: the client's thread could not be spawned: {e} — raise the \
                         process's thread and file limits, then restart"
                    ),
                )
            })?;
        Ok(Some(ProdOutbound {
            cmds,
            done,
            in_flight: std::cell::RefCell::new(BTreeSet::new()),
        }))
    }
}

impl Outbound for ProdOutbound {
    fn start(&self, token: u64, call: OutboundCall) -> io::Result<()> {
        assert!(
            self.in_flight.borrow_mut().insert(token),
            "contract: Outbound::start({token}) while that token is in flight"
        );
        self.cmds.send(Cmd::Start(token, call)).map_err(|_| {
            self.in_flight.borrow_mut().remove(&token);
            io::Error::other("the outbound runtime thread is gone")
        })
    }
    fn poll(&self) -> Option<(u64, io::Result<OutboundReply>)> {
        let (token, result) = self.done.try_recv().ok()?;
        self.in_flight.borrow_mut().remove(&token);
        Some((token, result))
    }
    fn cancel(&self, token: u64) {
        self.in_flight.borrow_mut().remove(&token);
        let _ = self.cmds.send(Cmd::Cancel(token));
    }
}

/// `--outbound-base`'s value: `http(s)://<ip or localhost>[:port]`, nothing more.
fn parse_base(
    host: &str,
    base: &str,
) -> Result<(&'static str, Option<std::net::IpAddr>, Option<u16>), String> {
    let flag = format!("--outbound-base {host}={base}");
    let (scheme, rest) = if let Some(r) = base.strip_prefix("https://") {
        ("https", r)
    } else if let Some(r) = base.strip_prefix("http://") {
        ("http", r)
    } else {
        return Err(format!(
            "{flag}: a base begins with https:// (or http:// on loopback)"
        ));
    };
    if rest.contains('/') {
        return Err(format!(
            "{flag}: a base is a scheme, an address and an optional port — no path"
        ));
    }
    // A bracketed IPv6 literal with no port ends in `]`; its colons are not a port's.
    let (name, port) = match rest.rsplit_once(':') {
        Some((n, p)) if !rest.ends_with(']') => (
            n,
            Some(
                p.parse::<u16>()
                    .map_err(|_| format!("{flag}: \"{p}\" is not a port"))?,
            ),
        ),
        _ => (rest, None),
    };
    let name = name.trim_start_matches('[').trim_end_matches(']');
    let ip: std::net::IpAddr = if name == "localhost" {
        std::net::Ipv4Addr::LOCALHOST.into()
    } else {
        name.parse().map_err(|_| {
            format!(
                "{flag}: the base must name an IP address or localhost — TLS still verifies {host}, so only the address moves (GUIDE §5)"
            )
        })?
    };
    if scheme == "http" && !ip.is_loopback() {
        return Err(format!(
            "{flag}: plain http is accepted only for a loopback address — use https"
        ));
    }
    Ok((scheme, Some(ip), port))
}

/// Mozilla's roots (`webpki-roots`), so behaviour does not vary with the OS
/// store, plus each `--outbound-ca`. ALPN is left to hyper-rustls:
/// `HttpsConnectorBuilder::with_tls_config` asserts a config arrives with
/// none set, and the HTTP/1-only connector `enable_http1` builds speaks
/// HTTP/1.1.
fn tls_config(cas: &[std::path::PathBuf]) -> Result<rustls::ClientConfig, String> {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    let mut roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    for path in cas {
        let flag = format!("--outbound-ca {}", path.display());
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
            .map_err(|e| format!("{flag}: {e}"))?
            .collect::<Result<_, _>>()
            .map_err(|e| format!("{flag}: {e}"))?;
        if certs.is_empty() {
            return Err(format!("{flag}: the file holds no PEM certificate"));
        }
        for cert in certs {
            roots.add(cert).map_err(|e| format!("{flag}: {e}"))?;
        }
    }
    Ok(
        rustls::ClientConfig::builder_with_provider(
            rustls::crypto::ring::default_provider().into(),
        )
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth(),
    )
}

type OutboundClient = hyper_util::client::legacy::Client<
    hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector<Resolver>>,
    Full<Bytes>,
>;

/// The runtime thread's body. The resolver maps a host with a base `addr` to
/// it, and asks the system resolver otherwise. The request URI always
/// carries the declared host, so SNI and verification use it (A11).
fn outbound_runtime(
    rt: tokio::runtime::Runtime,
    mut cmds: tokio::sync::mpsc::UnboundedReceiver<Cmd>,
    done: mpsc::Sender<(u64, io::Result<OutboundReply>)>,
    routes: BTreeMap<&'static str, Route>,
    tls: rustls::ClientConfig,
) {
    let overrides: Arc<BTreeMap<String, std::net::IpAddr>> = Arc::new(
        routes
            .iter()
            .filter_map(|(h, r)| r.addr.map(|a| (h.to_string(), a)))
            .collect(),
    );
    let routes = Arc::new(routes);
    rt.block_on(async move {
        let mut http =
            hyper_util::client::legacy::connect::HttpConnector::new_with_resolver(Resolver {
                overrides,
                system: hyper_util::client::legacy::connect::dns::GaiResolver::new(),
            });
        http.enforce_http(false);
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .wrap_connector(http);
        let client: OutboundClient =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .retry_canceled_requests(false)
                .build(connector);
        let mut tasks: BTreeMap<u64, tokio::task::AbortHandle> = BTreeMap::new();
        while let Some(cmd) = cmds.recv().await {
            tasks.retain(|_, h| !h.is_finished());
            match cmd {
                Cmd::Start(token, call) => {
                    let (client, routes, done) = (client.clone(), routes.clone(), done.clone());
                    let task = tokio::spawn(async move {
                        let result = send(&client, &routes, call).await;
                        let _ = done.send((token, result));
                    });
                    tasks.insert(token, task.abort_handle());
                }
                Cmd::Cancel(token) => {
                    if let Some(h) = tasks.remove(&token) {
                        h.abort();
                    }
                }
            }
        }
    });
}

/// Sends `call` as given, then its host's credential headers (the kernel
/// refused any call that sets one, so nothing collides), and reads the reply
/// body until it holds more than the host's cap: an over-cap reply arrives
/// as `max_reply + 1` bytes, which is how the kernel tells.
async fn send(
    client: &OutboundClient,
    routes: &BTreeMap<&'static str, Route>,
    call: OutboundCall,
) -> io::Result<OutboundReply> {
    let route = routes
        .get(call.host)
        .expect("the kernel starts calls only to a declared host (R9)");
    let port = route.port.map(|p| format!(":{p}")).unwrap_or_default();
    let mut request = hyper::Request::builder()
        .method(call.method.as_str())
        .uri(format!(
            "{}://{}{port}{}",
            route.scheme, call.host, call.path
        ));
    for (name, value) in call.headers.iter().chain(&route.headers) {
        request = request.header(name.as_str(), value.as_str());
    }
    let request = request
        .body(Full::new(Bytes::from(call.body)))
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("the request could not be built, so nothing was sent: {e}"),
            )
        })?;
    let reply = client.request(request).await.map_err(|e| {
        if e.is_connect() {
            // A18: the connector failed (DNS, TCP or the TLS handshake), so
            // no byte of the request was written.
            io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("could not connect: {}", with_causes(&e)),
            )
        } else {
            io::Error::other(format!("the connection failed: {}", with_causes(&e)))
        }
    })?;
    let status = reply.status().as_u16();
    let headers = reply
        .headers()
        .iter()
        .map(|(n, v)| {
            (
                n.as_str().to_string(),
                String::from_utf8_lossy(v.as_bytes()).into_owned(),
            )
        })
        .collect();
    let mut body = reply.into_body();
    let mut bytes = Vec::new();
    while bytes.len() <= route.max_reply {
        let Some(frame) = body.frame().await else {
            break;
        };
        let frame = frame.map_err(|e| {
            io::Error::other(format!(
                "the reply broke off after {} bytes: {}",
                bytes.len(),
                with_causes(&e)
            ))
        })?;
        if let Ok(data) = frame.into_data() {
            bytes.extend_from_slice(&data);
        }
    }
    bytes.truncate(route.max_reply + 1);
    Ok(OutboundReply {
        status,
        headers,
        body: bytes,
    })
}

/// hyper's errors print only their kind ("client error (Connect)"); the
/// cause an operator needs, refused or DNS or TLS, is in the source chain.
fn with_causes(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut source = e.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}

/// `--outbound-base`'s address override in front of the system resolver
/// (plan amendment A11): only where a name resolves moves, never the name.
#[derive(Clone)]
struct Resolver {
    overrides: Arc<BTreeMap<String, std::net::IpAddr>>,
    system: hyper_util::client::legacy::connect::dns::GaiResolver,
}

impl tower_service::Service<hyper_util::client::legacy::connect::dns::Name> for Resolver {
    type Response = std::vec::IntoIter<SocketAddr>;
    type Error = io::Error;
    type Future = Pin<
        Box<dyn std::future::Future<Output = io::Result<std::vec::IntoIter<SocketAddr>>> + Send>,
    >;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: hyper_util::client::legacy::connect::dns::Name) -> Self::Future {
        // An override answers port 0; the connector applies the URI's port.
        if let Some(ip) = self.overrides.get(name.as_str()) {
            let addrs = vec![SocketAddr::new(*ip, 0)];
            return Box::pin(async move { Ok(addrs.into_iter()) });
        }
        // `GaiResolver::poll_ready` is always ready, so it is not polled first.
        let lookup = tower_service::Service::call(&mut self.system, name);
        Box::pin(async move { Ok(lookup.await?.collect::<Vec<_>>().into_iter()) })
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read, Write};

    use super::*;
    use crate::http::{ApiError, AppConfig, Ctx, Cursor, Route, RouteGlue, Session, Subscription};
    use crate::outbound::Host;
    use crate::prod::{ProdDisk, ProdEntropy};
    use crate::repl::role::Role;
    use crate::wire::{TypeSchema, Wire};
    use charpente_core::db::{Event, Schema, Tx};
    use charpente_core::io::Entropy;
    use charpente_core::io::{HttpMethod, IdempotencyKey, OutboundWake};
    use charpente_core::table::{Codec, Id, Unscoped, View};

    /// The kernel thread's stop line — the first line of the kernel's final
    /// stderr output — names what stopped it (spec §5): a refusal is named as
    /// one, because restarting the same binary does not by itself clear it.
    /// Each refusal's own text carries what must change first.
    #[test]
    fn every_kernel_stop_line_names_what_stopped_it() {
        let refusal = io::Error::other(charpente_core::db::Refusal(
            "the refusal names its own fix".to_string(),
        ));
        let io_failure = io::Error::other("disk on fire");
        assert_eq!(
            kernel_stop_line(false, &refusal),
            "charpente kernel refused to start: the refusal names its own fix"
        );
        assert_eq!(
            kernel_stop_line(false, &io_failure),
            "charpente kernel failed to start — fix the store/log and restart: disk on fire"
        );
        assert_eq!(
            kernel_stop_line(true, &refusal),
            "charpente kernel stopped on a refusal: the refusal names its own fix"
        );
        assert_eq!(
            kernel_stop_line(true, &io_failure),
            "charpente kernel stopped on a platform IO failure — restart to recover: disk on fire"
        );
        // This is `kernel_stop_line`'s own text-formatting oracle, built from
        // a hand-made `Refusal` — the real gate-opening call site
        // (`after_repl_step`, kernel/mod.rs) typing its OWN error as a
        // `Refusal`, through a real `Kernel::step`, is
        // `replication.rs`'s `a_write_gate_opening_over_foreign_pending_jobs_is_a_refusal`.

        // A refusal naming several job types is several lines joined by
        // `\n` (`Db::pending_job_shape_refusal`) — the prefix labels only
        // the first line, but the operator still sees it and every line,
        // because `eprintln!` prints the whole string in one call.
        let multi_line_refusal = io::Error::other(charpente_core::db::Refusal(
            "job \"a\" ...\njob \"b\" ...".to_string(),
        ));
        assert_eq!(
            kernel_stop_line(false, &multi_line_refusal),
            "charpente kernel refused to start: job \"a\" ...\njob \"b\" ..."
        );
    }

    /// Spec §5: a kernel that refuses to start ends the process — `wait`
    /// returns 1 — rather than parking behind an edge that answers 500. The
    /// refusal is payload-shapes spec §5's: a Pending job enqueued under one
    /// shape, mounted under another.
    #[test]
    fn a_kernel_that_refuses_to_start_ends_wait_with_status_1() {
        struct Receipt {
            invoice: u64,
        }
        impl Codec for Receipt {
            fn encode(&self, out: &mut Vec<u8>) {
                self.invoice.encode(out);
            }
            fn decode(input: &mut &[u8]) -> io::Result<Receipt> {
                Ok(Receipt {
                    invoice: u64::decode(input)?,
                })
            }
        }
        impl charpente_core::db::Job for Receipt {
            const NAME: &'static str = "receipt";
            const FINGERPRINT: u64 = 0x7E57_0701;
            type Scope = Unscoped;
        }
        fn noop_glue(
            _tx: &mut Tx,
            _payload: &[u8],
            _cap: Option<&charpente_core::db::CrossTenant>,
            _scope: Option<crate::kernel::JobScope<'_>>,
        ) -> Result<(), crate::kernel::JobError> {
            Ok(())
        }
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        let root = std::env::temp_dir().join(format!(
            "charpente-serve-refuse-{:016x}",
            u64::from_le_bytes(suffix)
        ));
        let mut app = App::open(
            Box::new(ProdDisk::new(&root).unwrap()),
            Schema::new(),
            Box::new(ProdEntropy::new().unwrap()),
            AppConfig::default(),
        )
        .unwrap();
        // Seeded before `mount_jobs`, as the binary that enqueued it would
        // have: once `receipt` is mounted under another shape, the enqueue
        // itself is refused.
        app.db_mut()
            .transact(|tx| {
                tx.enqueue(Receipt { invoice: 1 });
                Ok::<(), io::Error>(())
            })
            .unwrap();
        let app = app.mount_jobs(vec![crate::kernel::JobDef {
            name: "receipt",
            retry: charpente_core::job::RetryPolicy::DEFAULT,
            cross_tenant: false,
            scoped_by: None,
            scope_type: std::any::TypeId::of::<charpente_core::table::Unscoped>(),
            scope_type_name: std::any::type_name::<charpente_core::table::Unscoped>(),
            fingerprint: 0x7E57_0702,
            glue: noop_glue,
            outbound: None,
        }]);
        let server = serve("127.0.0.1:0", app).unwrap();
        // A deadline before `wait`, whose join would otherwise block forever
        // on a kernel that does not refuse: this test then fails by name
        // rather than hanging the binary.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !server.kernel.is_finished() {
            assert!(
                std::time::Instant::now() < deadline,
                "the kernel thread is still running after 10 s: a Pending job enqueued under \
                 shape 0x7E57_0701 and mounted under 0x7E57_0702 must refuse at Kernel::new \
                 and end it"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert_eq!(server.wait(), 1);
        std::fs::remove_dir_all(root).unwrap();
    }

    /// The OTHER way `wait` returns non-zero from the RUN path — a panic
    /// inside `Kernel::new`, not an `Err`. A cron interval that overflows
    /// `u64` nanoseconds panics at kernel startup rather than at
    /// `.schedule(...)` (GUIDE §7's timing caveat), so `serve` itself returns
    /// `Ok` and the panic only surfaces once `wait` joins the thread.
    #[test]
    fn a_cron_interval_overflow_panics_the_kernel_thread_and_wait_returns_101() {
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        let root = std::env::temp_dir().join(format!(
            "charpente-serve-cron-overflow-{:016x}",
            u64::from_le_bytes(suffix)
        ));
        struct Never;
        impl charpente_core::db::Job for Never {
            const NAME: &'static str = "never";
            const FINGERPRINT: u64 = 0x7E57_0703;
            type Scope = Unscoped;
        }
        impl Codec for Never {
            fn encode(&self, _out: &mut Vec<u8>) {}
            fn decode(_input: &mut &[u8]) -> io::Result<Never> {
                Ok(Never)
            }
        }
        let app = App::open(
            Box::new(ProdDisk::new(&root).unwrap()),
            Schema::new(),
            Box::new(ProdEntropy::new().unwrap()),
            AppConfig::default(),
        )
        .unwrap()
        .schedule("overflow", std::time::Duration::MAX, || Never);
        let server = serve("127.0.0.1:0", app).unwrap();
        assert_eq!(server.wait(), 101);
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Same mounted name as the `receipt` `JobDef` below (fingerprint
    /// `0x7E57_0704`), a different fingerprint — what a cron tick stages
    /// when its own binary has drifted from the one that mounted the job.
    struct DriftedCronReceipt;
    impl Codec for DriftedCronReceipt {
        fn encode(&self, _out: &mut Vec<u8>) {}
        fn decode(_input: &mut &[u8]) -> io::Result<DriftedCronReceipt> {
            Ok(DriftedCronReceipt)
        }
    }
    impl charpente_core::db::Job for DriftedCronReceipt {
        const NAME: &'static str = "receipt";
        const FINGERPRINT: u64 = 0x7E57_0BAD;
        type Scope = Unscoped;
    }

    fn cron_foreign_shape_glue(
        _tx: &mut Tx,
        _payload: &[u8],
        _cap: Option<&charpente_core::db::CrossTenant>,
        _scope: Option<crate::kernel::JobScope<'_>>,
    ) -> Result<(), crate::kernel::JobError> {
        Ok(())
    }

    /// An app mounting `receipt` at the real `0x7E57_0704` fingerprint and
    /// scheduling a 1ms cron that stages `DriftedCronReceipt` under the SAME
    /// name — nothing is Pending at open, so `Kernel::new` admits it; the
    /// refusal only arrives once the tick fires.
    fn cron_foreign_shape_app(root: &std::path::Path) -> App {
        App::open(
            Box::new(ProdDisk::new(root).unwrap()),
            Schema::new(),
            Box::new(ProdEntropy::new().unwrap()),
            AppConfig::default(),
        )
        .unwrap()
        .mount_jobs(vec![crate::kernel::JobDef {
            name: "receipt",
            retry: charpente_core::job::RetryPolicy::DEFAULT,
            cross_tenant: false,
            scoped_by: None,
            scope_type: std::any::TypeId::of::<charpente_core::table::Unscoped>(),
            scope_type_name: std::any::type_name::<charpente_core::table::Unscoped>(),
            fingerprint: 0x7E57_0704,
            glue: cron_foreign_shape_glue,
            outbound: None,
        }])
        .schedule("drift", std::time::Duration::from_millis(1), || {
            DriftedCronReceipt
        })
    }

    fn cron_foreign_shape_root(tag: &str) -> std::path::PathBuf {
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        std::env::temp_dir().join(format!(
            "charpente-serve-cron-foreign-{tag}-{:016x}",
            u64::from_le_bytes(suffix)
        ))
    }

    /// `serve`'s RUN-arm `Err` → `1` (serve.rs, `kernel.run`'s own
    /// match), distinct from `Kernel::new`'s `Err` → `1` above — a refusal
    /// that arrives on a live kernel, after a successful start, rather than
    /// at startup. Two passes over the identical recipe (a fresh store each,
    /// same mount/schedule): one drives `Kernel::run` directly to observe
    /// the real error `serve`'s thread only prints and discards, the other
    /// goes through `serve`/`Server::wait` to pin the process-facing code at
    /// the site that actually returns it.
    #[test]
    fn a_cron_tick_over_a_foreign_shape_stops_the_kernel_on_the_run_path() {
        let direct_root = cron_foreign_shape_root("direct");
        let mut kernel = kernel::Kernel::new(
            cron_foreign_shape_app(&direct_root),
            Box::new(crate::prod::ProdClock::new()),
        )
        .expect("nothing Pending at open — the drifted shape is only staged by the tick");
        let (_tx, rx) = mpsc::channel::<(Request, kernel::Reply)>();
        // The refusal stops it on step 1 of the green run, and the served kernel
        // in about 25 ms; both bounds are far above, so a tick that never runs
        // fails by name instead of spinning the suite.
        let mut source = Bounded {
            inner: ProdSource {
                rx,
                stash: None,
                clock: crate::prod::ProdClock::new(),
            },
            steps: 0,
            max: 1_000,
            stops_when: "the 1ms cron tick stages a foreign shape and is refused",
        };
        let err = kernel
            .run(&mut source)
            .expect_err("the 1ms tick stages a foreign shape of a mounted name, failing staging");
        assert!(Refusal::of(&err).is_some(), "{err}");
        assert_eq!(
            kernel_stop_line(true, &err),
            format!("charpente kernel stopped on a refusal: {err}")
        );
        std::fs::remove_dir_all(&direct_root).unwrap();

        let served_root = cron_foreign_shape_root("served");
        let server = serve("127.0.0.1:0", cron_foreign_shape_app(&served_root)).unwrap();
        let (done, status) = mpsc::channel();
        std::thread::spawn(move || done.send(server.wait()));
        let status = status
            .recv_timeout(Duration::from_secs(30))
            .unwrap_or_else(|_| {
                panic!(
                    "serve's kernel never stopped within 30s: the 1ms cron tick over a foreign \
                 shape must end it with status 1"
                )
            });
        assert_eq!(status, 1);
        std::fs::remove_dir_all(&served_root).unwrap();
    }

    /// A `RequestSource` that fails by name after `max` kernel steps (`step` polls
    /// its source once per iteration), so a kernel that never stops (a tick that
    /// never runs, a deadline already past) fails the test instead of spinning it
    /// forever.
    struct Bounded<S> {
        inner: S,
        steps: u32,
        max: u32,
        stops_when: &'static str,
    }

    impl<S: kernel::RequestSource> kernel::RequestSource for Bounded<S> {
        fn poll(&mut self) -> Option<(Request, kernel::Reply)> {
            self.steps += 1;
            assert!(
                self.steps <= self.max,
                "the kernel never stopped: {} steps, and it should stop when {}",
                self.max,
                self.stops_when
            );
            self.inner.poll()
        }

        fn wait_until(&mut self, deadline: Option<Instant>) -> kernel::Wake {
            self.inner.wait_until(deadline)
        }
    }

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

    /// The reshaped frame on the wire carries the foreign row's OWN id, so a
    /// client that ignores it and reconnects with `Last-Event-ID` lands on the
    /// named 409 instead of skipping the event (payload-shapes spec §3).
    #[test]
    fn a_reshaped_frame_carries_the_foreign_rows_own_id_on_the_wire() {
        let frame = kernel::EventFrame {
            id: 5,
            seq: 12,
            name: kernel::RESHAPED_FRAME,
            json: r#"{"event":"incident_opened","id":5,"fix":"reconnect without a cursor, wait for the stream to open, then read current state from the same node, holding every frame that arrives until that read is installed and then applying the held frames and every later one over it in order, idempotently"}"#.to_string(),
        };
        assert_eq!(
            std::str::from_utf8(&encode_frame(&frame)).unwrap(),
            "id: 5.12\nevent: charpente.reshaped\ndata: {\"event\":\"incident_opened\",\"id\":5,\"fix\":\"reconnect without a cursor, wait for the stream to open, then read current state from the same node, holding every frame that arrives until that read is installed and then applying the held frames and every later one over it in order, idempotently\"}\n\n"
        );
    }

    /// `charpente.error` has NO `id:` line (payload-shapes spec §3, A7): a
    /// client's `Last-Event-ID` must not advance past an event it never
    /// received, so a reconnect fails at the same row rather than skipping it.
    #[test]
    fn an_error_frame_carries_no_id_on_the_wire() {
        let frame = kernel::EventFrame {
            id: 5,
            seq: 12,
            name: kernel::ERROR_FRAME,
            json: r#"{"event":"incident_opened","id":5,"error":"boom"}"#.to_string(),
        };
        assert_eq!(
            std::str::from_utf8(&encode_frame(&frame)).unwrap(),
            "event: charpente.error\ndata: {\"event\":\"incident_opened\",\"id\":5,\"error\":\"boom\"}\n\n"
        );
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
        // Untyped glue, not a described route (serve.rs can't use `::charpente`
        // macro paths — see the comment above `hello_glue`).
        .mount(vec![Route {
            method: Method::Get,
            pattern: "/hello",
            auth: false,
            public_scope: None,
            cross_tenant: false,
            name: "hello",
            glue: RouteGlue::Http(hello_glue),
            params: &[],
            request: None,
            response: || TypeSchema::Null,
            error: || <ApiError as Wire>::reference(),
            register_types: |_| {},
            emits: &[],
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

    // Untyped glue, not described routes (same reason as `hello_glue` above).
    fn smoke_routes() -> Vec<Route> {
        vec![
            Route {
                method: Method::Post,
                pattern: "/rows/:id",
                auth: false,
                public_scope: None,
                cross_tenant: false,
                name: "put_row",
                glue: RouteGlue::Http(put_row_glue),
                params: &[],
                request: None,
                response: || TypeSchema::Null,
                error: || <ApiError as Wire>::reference(),
                register_types: |_| {},
                emits: &[],
            },
            Route {
                method: Method::Get,
                pattern: "/rows/:id",
                auth: false,
                public_scope: None,
                cross_tenant: false,
                name: "get_row",
                glue: RouteGlue::Http(get_row_glue),
                params: &[],
                request: None,
                response: || TypeSchema::Null,
                error: || <ApiError as Wire>::reference(),
                register_types: |_| {},
                emits: &[],
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
        // Untyped glue, not a described route (serve.rs can't use `::charpente`
        // macro paths — see the comment above `hello_glue`).
        .mount(vec![Route {
            method: Method::Get,
            pattern: "/hello",
            auth: false,
            public_scope: None,
            cross_tenant: false,
            name: "hello",
            glue: RouteGlue::Http(hello_glue),
            params: &[],
            request: None,
            response: || TypeSchema::Null,
            error: || <ApiError as Wire>::reference(),
            register_types: |_| {},
            emits: &[],
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
        const FINGERPRINT: u64 = 0x7E57_0007;
        // A global event: it belongs to no tenant.
        type Scope = Unscoped;
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

    // Untyped glue, not described routes (same reason as `hello_glue` above).
    fn sse_routes() -> Vec<Route> {
        vec![
            Route {
                method: Method::Post,
                pattern: "/emit/:n",
                auth: false,
                public_scope: None,
                cross_tenant: false,
                name: "emit_ping",
                glue: RouteGlue::Http(emit_ping_glue),
                params: &[],
                request: None,
                response: || TypeSchema::Null,
                error: || <ApiError as Wire>::reference(),
                register_types: |_| {},
                emits: &[],
            },
            Route {
                method: Method::Get,
                pattern: "/feed",
                auth: false,
                public_scope: None,
                cross_tenant: false,
                name: "feed",
                glue: RouteGlue::Sse(feed_glue),
                params: &[],
                request: None,
                response: || TypeSchema::Null,
                error: || <ApiError as Wire>::reference(),
                register_types: |_| {},
                emits: &[],
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

    // ------------------------------------------------------ outbound client

    /// A one-shot plain-HTTP server on loopback: reads one whole request
    /// (headers, then `content-length` bytes of body, however the client
    /// split its writes), answers `reply`, and hands back the raw request.
    fn one_shot(reply: Vec<u8>) -> (u16, std::thread::JoinHandle<Vec<u8>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            use std::io::{Read, Write};
            let (mut conn, _) = listener.accept().unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let n = conn.read(&mut chunk).unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
                    let len = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length: "))
                        .map_or(0, |v| v.trim().parse::<usize>().unwrap());
                    if buf.len() >= end + 4 + len {
                        break;
                    }
                }
            }
            // The reply-cap test's client stops reading at cap + 1 and drops
            // the connection, so this write can fail by design.
            let _ = conn.write_all(&reply);
            buf
        });
        (port, handle)
    }

    fn wait_for(ob: &ProdOutbound) -> (u64, io::Result<OutboundReply>) {
        for _ in 0..500 {
            if let Some(c) = ob.poll() {
                return c;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("no completion within 5s");
    }

    #[crate::job(global, outbound = "pay.test")]
    struct ServeTestCharge {}

    impl ServeTestCharge {
        fn call(
            &self,
            _tx: &Tx,
            _key: &IdempotencyKey,
        ) -> Result<Option<crate::outbound::Call>, crate::kernel::JobError> {
            Ok(None)
        }
        fn settle(
            self,
            _tx: &mut Tx,
            _reply: crate::outbound::Reply,
        ) -> Result<(), crate::kernel::JobError> {
            Ok(())
        }
    }

    /// An `App` over a fresh temp `ProdDisk`, as `a_kernel_that_refuses_to_start_ends_wait_with_status_1` opens its own.
    fn temp_app(tag: &str) -> App {
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        let root = std::env::temp_dir().join(format!(
            "charpente-serve-{tag}-{:016x}",
            u64::from_le_bytes(suffix)
        ));
        App::open(
            Box::new(ProdDisk::new(&root).unwrap()),
            Schema::new(),
            Box::new(ProdEntropy::new().unwrap()),
            AppConfig::default(),
        )
        .unwrap()
    }

    fn charge_app(host: Host) -> App {
        temp_app("outbound")
            .mount_jobs(crate::jobs![ServeTestCharge])
            .outbound(host)
    }

    fn plain_call(path: &str) -> OutboundCall {
        OutboundCall {
            host: "pay.test",
            method: HttpMethod::Get,
            path: path.into(),
            headers: Vec::new(),
            body: Vec::new(),
            key: IdempotencyKey::from_bytes([0; 16]),
        }
    }

    #[test]
    fn prod_sends_the_call_with_its_credential_and_returns_the_reply() {
        let (port, server) =
            one_shot(b"HTTP/1.1 201 Created\r\ncontent-length: 2\r\nx-id: ch_1\r\n\r\nok".to_vec());
        // Safety of set_var: the hazard is foreign (libc) code reading the
        // environment concurrently; std's own reads take its lock. None runs
        // in this binary's tests: every outbound call resolves through a
        // base's address override, and every socket address is an IP literal,
        // so nothing reaches getaddrinfo.
        unsafe { std::env::set_var("SERVE_TEST_PAY_KEY", "sk_test_1") };
        let app = charge_app(Host::new("pay.test").bearer_env("SERVE_TEST_PAY_KEY"));
        let opts = OutboundOptions {
            bases: vec![("pay.test".into(), format!("http://127.0.0.1:{port}"))],
            cas: vec![],
        };
        let ob = ProdOutbound::new(&app, &opts)
            .unwrap()
            .expect("the app calls out");
        let key = IdempotencyKey::from_bytes([5; 16]);
        ob.start(
            7,
            OutboundCall {
                host: "pay.test",
                method: HttpMethod::Post,
                path: "/v1/charges?x=1".into(),
                headers: vec![("idempotency-key".into(), key.as_str().into())],
                body: b"amount=1".to_vec(),
                key,
            },
        )
        .unwrap();
        let (token, reply) = wait_for(&ob);
        let reply = reply.unwrap();
        assert_eq!(
            (token, reply.status, reply.body.as_slice()),
            (7, 201, &b"ok"[..])
        );
        assert!(
            reply
                .headers
                .iter()
                .any(|(n, v)| n == "x-id" && v == "ch_1")
        );
        let raw = String::from_utf8(server.join().unwrap()).unwrap();
        assert!(
            raw.starts_with("POST /v1/charges?x=1 HTTP/1.1\r\n"),
            "{raw}"
        );
        assert!(raw.contains("authorization: Bearer sk_test_1\r\n"), "{raw}");
        assert!(
            raw.contains(&format!("idempotency-key: {}\r\n", key.as_str())),
            "{raw}"
        );
        assert!(
            raw.to_ascii_lowercase()
                .contains(&format!("host: pay.test:{port}\r\n")),
            "A11: the declared name: {raw}"
        );
        assert!(raw.ends_with("amount=1"), "{raw}");
    }

    /// Review Focus 4: the edge stops reading at cap + 1, so memory is bounded
    /// whatever the remote sends. The remote promises 1 MiB, sends 64 KiB and
    /// closes: a client that read on past the cap would hit that close and
    /// return `Err`, so an `Ok` of cap + 1 bytes shows it stopped.
    #[test]
    fn prod_reply_cap_bounds_reading() {
        let mut reply = b"HTTP/1.1 200 OK\r\ncontent-length: 1048576\r\n\r\n".to_vec();
        reply.extend(std::iter::repeat_n(b'x', 65536));
        let (port, _server) = one_shot(reply);
        let app = charge_app(Host::new("pay.test").max_reply_bytes(16));
        let opts = OutboundOptions {
            bases: vec![("pay.test".into(), format!("http://127.0.0.1:{port}"))],
            cas: vec![],
        };
        let ob = ProdOutbound::new(&app, &opts).unwrap().unwrap();
        ob.start(0, plain_call("/big")).unwrap();
        let (_, reply) = wait_for(&ob);
        assert_eq!(
            reply.unwrap().body.len(),
            17,
            "cap + 1, never the 64 KiB sent"
        );
    }

    /// The bound's `<=`: a reply whose first frame ends exactly at the cap
    /// must still read one byte past it, or the kernel takes a cut reply as
    /// whole. hyper yields chunked data at most one chunk per frame, so the
    /// first frame here ends at byte 16, the cap.
    #[test]
    fn a_reply_whose_frame_ends_at_the_cap_still_reads_past_it() {
        let mut reply = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n10\r\n".to_vec();
        reply.extend([b'a'; 16]);
        reply.extend(b"\r\n10\r\n");
        reply.extend([b'b'; 16]);
        reply.extend(b"\r\n0\r\n\r\n");
        let (port, _server) = one_shot(reply);
        let app = charge_app(Host::new("pay.test").max_reply_bytes(16));
        let opts = OutboundOptions {
            bases: vec![("pay.test".into(), format!("http://127.0.0.1:{port}"))],
            cas: vec![],
        };
        let ob = ProdOutbound::new(&app, &opts).unwrap().unwrap();
        ob.start(0, plain_call("/edge")).unwrap();
        let (_, reply) = wait_for(&ob);
        let mut want = vec![b'a'; 16];
        want.push(b'b');
        assert_eq!(
            reply.unwrap().body,
            want,
            "cap + 1: one byte of the second chunk"
        );
    }

    #[test]
    fn a_closed_port_is_connection_refused() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port(); // bound then dropped
        let app = charge_app(Host::new("pay.test"));
        let opts = OutboundOptions {
            bases: vec![("pay.test".into(), format!("http://127.0.0.1:{port}"))],
            cas: vec![],
        };
        let ob = ProdOutbound::new(&app, &opts).unwrap().unwrap();
        ob.start(0, plain_call("/x")).unwrap();
        let (_, reply) = wait_for(&ob);
        let err = reply.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
        // hyper's own text is only "client error (Connect)": the cause rides in its sources.
        let text = err.to_string();
        assert!(
            text.starts_with("could not connect: ")
                && text.to_ascii_lowercase().contains("connection refused"),
            "{text}"
        );
    }

    /// A cancelled call never completes: `cancel` aborts its task, so the
    /// reply the remote sends afterwards never reaches `poll`. The remote holds
    /// each reply 300 ms; an uncancelled call to it afterwards completes, so the
    /// silence is the cancel's and not a remote that never answers.
    #[test]
    fn a_cancelled_call_never_completes() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            use std::io::{Read, Write};
            for _ in 0..2 {
                let (mut conn, _) = listener.accept().unwrap();
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = conn.read(&mut chunk).unwrap();
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                std::thread::sleep(Duration::from_millis(300));
                let _ = conn.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok");
            }
        });
        let app = charge_app(Host::new("pay.test"));
        let opts = OutboundOptions {
            bases: vec![("pay.test".into(), format!("http://127.0.0.1:{port}"))],
            cas: vec![],
        };
        let ob = ProdOutbound::new(&app, &opts).unwrap().unwrap();
        ob.start(0, plain_call("/cancelled")).unwrap();
        ob.cancel(0);
        for _ in 0..150 {
            if let Some((token, _)) = ob.poll() {
                panic!("token {token} completed after its cancel: the call was never aborted");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        ob.start(1, plain_call("/control")).unwrap();
        let (token, reply) = wait_for(&ob);
        assert_eq!(
            (token, reply.unwrap().status),
            (1, 200),
            "the remote answers"
        );
    }

    /// `BindFailed` marks a bind failure for `launch` and is otherwise
    /// invisible: it prints as the bind's own error, `{:?}` included, so an
    /// operator who meets it through `unwrap` or a debug log reads the OS cause.
    #[test]
    fn a_bind_failure_prints_as_the_bind_error_in_debug_too() {
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = taken.local_addr().unwrap().to_string();
        let bare = std::net::TcpListener::bind(&addr).unwrap_err();
        let err = bind_http(&addr).unwrap_err();
        assert!(BindFailed::of(&err));
        assert_eq!(err.kind(), bare.kind());
        assert_eq!(err.to_string(), bare.to_string());
        assert!(
            format!("{err:?}").contains(&format!("{bare:?}")),
            "the marked error's Debug hides the bind's cause: {err:?}"
        );
    }

    /// A credential is checked when it is read, so a value with a trailing
    /// newline, or an empty one, refuses the start instead of failing every
    /// call (an empty one as `Repl::open` refuses an empty peer secret), and
    /// each refusal names the variable, never the value. Basic's pair is held
    /// to the same rule, though base64 would carry it, except that its
    /// password may be empty (`a_basic_host_with_an_empty_password_sends_user_colon`).
    #[test]
    fn a_credential_that_cannot_be_sent_refuses_the_start() {
        // Safety of set_var: as in
        // `prod_sends_the_call_with_its_credential_and_returns_the_reply`.
        unsafe {
            std::env::set_var("SERVE_TEST_NEWLINE_KEY", "sk_newline_1\n");
            std::env::set_var("SERVE_TEST_EMPTY_KEY", "");
            std::env::set_var("SERVE_TEST_BASIC_USER", "basic_user_1");
            std::env::set_var("SERVE_TEST_BASIC_PASS", "basic_pass_1\r\n");
        }
        let refusal = |host: Host| {
            let err = ProdOutbound::new(&charge_app(host), &OutboundOptions::default())
                .err()
                .unwrap();
            assert!(Refusal::of(&err).is_some());
            let shown = format!("{err} {err:?}");
            for value in ["sk_newline_1", "basic_user_1", "basic_pass_1"] {
                assert!(!shown.contains(value), "{shown}");
            }
            err.to_string()
        };
        assert_eq!(
            refusal(Host::new("pay.test").header_env("x-api-key", "SERVE_TEST_NEWLINE_KEY")),
            "outbound: SERVE_TEST_NEWLINE_KEY holds a control character (a trailing newline, say) \
             or is not UTF-8 — host pay.test needs it for job ServeTestCharge; set it to the \
             credential alone"
        );
        let empty = "outbound: SERVE_TEST_EMPTY_KEY is set but empty — host pay.test needs it \
                     for job ServeTestCharge";
        assert_eq!(
            refusal(Host::new("pay.test").bearer_env("SERVE_TEST_EMPTY_KEY")),
            empty
        );
        assert_eq!(
            refusal(Host::new("pay.test").header_env("x-api-key", "SERVE_TEST_EMPTY_KEY")),
            empty
        );
        assert_eq!(
            refusal(
                Host::new("pay.test").basic_env("SERVE_TEST_EMPTY_KEY", "SERVE_TEST_BASIC_USER")
            ),
            empty,
            "an empty Basic user refuses; only the password may be empty"
        );
        assert_eq!(
            refusal(
                Host::new("pay.test").basic_env("SERVE_TEST_BASIC_USER", "SERVE_TEST_BASIC_PASS")
            ),
            "outbound: SERVE_TEST_BASIC_PASS holds a control character (a trailing newline, say) \
             or is not UTF-8 — host pay.test needs it for job ServeTestCharge; set it to the \
             credential alone"
        );
    }

    /// Key-as-username APIs (Stripe's documented `-u sk_…:`) send an empty
    /// Basic password, so an empty password variable starts, and the header
    /// carries `user:`.
    #[test]
    fn a_basic_host_with_an_empty_password_sends_user_colon() {
        // Safety of set_var: as in
        // `prod_sends_the_call_with_its_credential_and_returns_the_reply`.
        unsafe {
            std::env::set_var("SERVE_TEST_KEY_AS_USER", "sk_test_basic");
            std::env::set_var("SERVE_TEST_EMPTY_PASS", "");
        }
        let (port, server) = one_shot(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n".to_vec());
        let app = charge_app(
            Host::new("pay.test").basic_env("SERVE_TEST_KEY_AS_USER", "SERVE_TEST_EMPTY_PASS"),
        );
        let opts = OutboundOptions {
            bases: vec![("pay.test".into(), format!("http://127.0.0.1:{port}"))],
            cas: vec![],
        };
        let ob = ProdOutbound::new(&app, &opts)
            .unwrap()
            .expect("the app calls out");
        ob.start(0, plain_call("/basic")).unwrap();
        let (_, reply) = wait_for(&ob);
        assert_eq!(reply.unwrap().status, 200);
        let raw = String::from_utf8(server.join().unwrap()).unwrap();
        assert!(
            raw.contains("authorization: Basic c2tfdGVzdF9iYXNpYzo=\r\n"),
            "base64(\"sk_test_basic:\"): {raw}"
        );
    }

    /// A reply cut off with no answer is a transport failure that does not
    /// claim the request went out or stayed home: hyper can hand back a
    /// request unsent when a pooled connection died, so only the kernel's
    /// "may have applied" suffix speaks to delivery.
    #[test]
    fn a_remote_that_closes_without_answering_is_a_neutral_failure() {
        let (port, _server) = one_shot(Vec::new());
        let app = charge_app(Host::new("pay.test"));
        let opts = OutboundOptions {
            bases: vec![("pay.test".into(), format!("http://127.0.0.1:{port}"))],
            cas: vec![],
        };
        let ob = ProdOutbound::new(&app, &opts).unwrap().unwrap();
        ob.start(0, plain_call("/x")).unwrap();
        let (_, reply) = wait_for(&ob);
        let err = reply.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(
            err.to_string()
                .starts_with("the connection failed: client error"),
            "{err}"
        );
    }

    #[test]
    fn outbound_setup_refusals_name_the_fix() {
        let app = charge_app(Host::new("pay.test").bearer_env("SERVE_TEST_UNSET_VAR"));
        let err = ProdOutbound::new(&app, &OutboundOptions::default())
            .err()
            .unwrap();
        assert_eq!(
            err.to_string(),
            "outbound: SERVE_TEST_UNSET_VAR is unset — host pay.test needs it for job ServeTestCharge"
        );
        assert!(Refusal::of(&err).is_some());

        let app = charge_app(Host::new("pay.test"));
        let bad = |base: &str| {
            let opts = OutboundOptions {
                bases: vec![("pay.test".into(), base.into())],
                cas: vec![],
            };
            ProdOutbound::new(&app, &opts).err().unwrap().to_string()
        };
        assert_eq!(
            bad("http://10.0.0.5:80"),
            "--outbound-base pay.test=http://10.0.0.5:80: plain http is accepted only for a loopback address — use https"
        );
        assert_eq!(
            bad("https://fake.internal:8443"),
            "--outbound-base pay.test=https://fake.internal:8443: the base must name an IP address or localhost — TLS still verifies pay.test, so only the address moves (GUIDE §5)"
        );
        assert_eq!(
            bad("https://127.0.0.1:8443/v1"),
            "--outbound-base pay.test=https://127.0.0.1:8443/v1: a base is a scheme, an address and an optional port — no path"
        );
        // A bracketed IPv6 literal ends in `]` when it has no port.
        let v6 = Some(std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST));
        assert_eq!(
            parse_base("pay.test", "https://[::1]"),
            Ok(("https", v6, None))
        );
        assert_eq!(
            parse_base("pay.test", "https://[::1]:8443"),
            Ok(("https", v6, Some(8443)))
        );
        assert_eq!(
            parse_base("pay.test", "http://[::1]"),
            Ok(("http", v6, None))
        );
        let opts = OutboundOptions {
            bases: vec![("other.test".into(), "https://127.0.0.1:1".into())],
            cas: vec![],
        };
        assert_eq!(
            ProdOutbound::new(&app, &opts).err().unwrap().to_string(),
            "--outbound-base names other.test, which no mounted job calls — the hosts this app calls are: pay.test"
        );
        let opts = OutboundOptions {
            bases: vec![],
            cas: vec!["/nonexistent/ca.pem".into()],
        };
        assert!(
            ProdOutbound::new(&app, &opts)
                .err()
                .unwrap()
                .to_string()
                .starts_with("--outbound-ca /nonexistent/ca.pem: ")
        );
    }

    #[test]
    fn an_app_with_no_outbound_job_gets_no_client() {
        let app = temp_app("plain");
        assert!(
            ProdOutbound::new(&app, &OutboundOptions::default())
                .unwrap()
                .is_none(),
            "invariant 6: no runtime thread"
        );
    }

    /// R9 at the production client: the client is built before the kernel,
    /// so a host set that does not match its jobs is refused here, by name,
    /// as the kernel would refuse it. Both directions: a called host with no
    /// `Host` entry, and an entry no job calls.
    #[test]
    fn a_host_set_mismatch_refuses_the_client_by_name() {
        let refusal = |app: &App| {
            let err = ProdOutbound::new(app, &OutboundOptions::default())
                .err()
                .expect("a host set that does not match its jobs builds no client");
            assert!(Refusal::of(&err).is_some(), "{err}");
            err.to_string()
        };
        let unentered = temp_app("unentered").mount_jobs(crate::jobs![ServeTestCharge]);
        assert_eq!(
            refusal(&unentered),
            "kernel: job ServeTestCharge calls pay.test, which has no Host entry — add \
             app.outbound(Host::new(\"pay.test\")) with the credentials it needs (GUIDE §6)"
        );
        let uncalled = charge_app(Host::new("pay.test")).outbound(Host::new("extra.test"));
        assert_eq!(
            refusal(&uncalled),
            "kernel: app.outbound(Host::new(\"extra.test\")) declares a host no mounted job calls \
             — remove it, or mount the job that calls it"
        );
    }

    /// A base that cannot be parsed is a Refusal like every other setup
    /// failure, so `launch` reports "refused to start" with exit code 2, not
    /// a failed start.
    #[test]
    fn a_malformed_base_is_a_refusal() {
        let app = charge_app(Host::new("pay.test"));
        let opts = OutboundOptions {
            bases: vec![("pay.test".into(), "http://10.0.0.5:80".into())],
            cas: vec![],
        };
        let err = ProdOutbound::new(&app, &opts).err().unwrap();
        assert!(Refusal::of(&err).is_some(), "not a Refusal: {err}");
    }

    #[crate::job(global, outbound = "ledger.test")]
    struct ServeTestLedger {}

    impl ServeTestLedger {
        fn call(
            &self,
            _tx: &Tx,
            _key: &IdempotencyKey,
        ) -> Result<Option<crate::outbound::Call>, crate::kernel::JobError> {
            Ok(None)
        }
        fn settle(
            self,
            _tx: &mut Tx,
            _reply: crate::outbound::Reply,
        ) -> Result<(), crate::kernel::JobError> {
            Ok(())
        }
    }

    /// A credential refusal names the job that calls THAT host: with two
    /// jobs on two hosts, the unset variable of the second host's credential
    /// is the second job's.
    #[test]
    fn a_credential_refusal_names_the_job_that_calls_its_host() {
        let app = temp_app("two-hosts")
            .mount_jobs(crate::jobs![ServeTestCharge, ServeTestLedger])
            .outbound(Host::new("pay.test"))
            .outbound(Host::new("ledger.test").bearer_env("SERVE_TEST_UNSET_LEDGER_KEY"));
        let err = ProdOutbound::new(&app, &OutboundOptions::default())
            .err()
            .unwrap();
        assert_eq!(
            err.to_string(),
            "outbound: SERVE_TEST_UNSET_LEDGER_KEY is unset — host ledger.test needs it for job \
             ServeTestLedger"
        );
    }

    /// Plan amendment A2: replies arrive from the network, so production
    /// never knows when the next one is due, and the kernel parks no longer
    /// than its outbound tick.
    #[test]
    fn the_production_client_never_names_its_next_wake() {
        let app = charge_app(Host::new("pay.test"));
        let ob = ProdOutbound::new(&app, &OutboundOptions::default())
            .unwrap()
            .unwrap();
        assert_eq!(ob.next_wake(), OutboundWake::Unknown);
    }

    #[crate::job(global, outbound = "pay.test")]
    struct ServeTestPing {}

    impl ServeTestPing {
        fn call(
            &self,
            _tx: &Tx,
            _key: &IdempotencyKey,
        ) -> Result<Option<crate::outbound::Call>, crate::kernel::JobError> {
            Ok(Some(crate::outbound::Call::get("/served")))
        }
        fn settle(
            self,
            _tx: &mut Tx,
            _reply: crate::outbound::Reply,
        ) -> Result<(), crate::kernel::JobError> {
            Ok(())
        }
    }

    /// The serve path supplies the production client (the kernel's
    /// no-`Outbound` refusal says it does): an app holding a Pending
    /// outbound job, served, sends that job's call to the remote. Without
    /// the client its kernel refuses to build and nothing is ever sent.
    #[test]
    fn serve_with_supplies_the_production_client() {
        let (port, remote) = one_shot(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n".to_vec());
        let mut app = temp_app("served")
            .mount_jobs(crate::jobs![ServeTestPing])
            .outbound(Host::new("pay.test"));
        app.transact_for_tests(|tx| {
            tx.enqueue(ServeTestPing {});
            Ok::<(), io::Error>(())
        })
        .unwrap();
        let opts = OutboundOptions {
            bases: vec![("pay.test".into(), format!("http://127.0.0.1:{port}"))],
            cas: vec![],
        };
        let server = serve_with("127.0.0.1:0", app, opts).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !remote.is_finished() {
            assert!(
                std::time::Instant::now() < deadline,
                "no call reached the remote within 10 s (kernel thread finished: {}) — serve_with \
                 must hand the kernel the production client",
                server.kernel.is_finished()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let raw = String::from_utf8(remote.join().unwrap()).unwrap();
        assert!(raw.starts_with("GET /served HTTP/1.1\r\n"), "{raw}");
        assert!(
            !server.kernel.is_finished(),
            "the kernel serves on after the call"
        );
    }

    /// `serve_replicated` builds the client too: a primary over an app that
    /// mounts an outbound job starts and answers, where a kernel without one
    /// refuses to build (and the edge would answer 500, the kernel gone).
    #[test]
    fn serve_replicated_starts_an_app_that_calls_out() {
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        let root = std::env::temp_dir().join(format!(
            "charpente-repl-ob-{:016x}",
            u64::from_le_bytes(suffix)
        ));
        let app = App::open(
            Box::new(ProdDisk::new(&root).unwrap()),
            Schema::new(),
            Box::new(ProdEntropy::new().unwrap()),
            AppConfig::default(),
        )
        .unwrap()
        .mount_jobs(crate::jobs![ServeTestCharge])
        .outbound(Host::new("pay.test"));
        let server = serve_replicated(
            "127.0.0.1:0",
            app,
            Box::new(ProdDisk::new(&root).unwrap()),
            repl::ReplConfig {
                listen: Some(format!("127.0.0.1:{}", free_port())),
                initial_role: Role::Primary,
                peer_secret: None,
                strict_witnesses: false,
            },
        )
        .unwrap();
        let response = http1(&server.local_addr(), "GET", "/nope");
        assert!(response.starts_with("HTTP/1.1 404"), "{response}");
        assert!(!server.kernel.is_finished());
    }

    /// Every entry point that builds a kernel builds the client first, from
    /// the app's `Host` entries, so a missing credential refuses before a
    /// listener, a replication session or an archive opens. Each is handed a
    /// second fault in what it would open next (an address no listener binds,
    /// an empty peer secret `Repl::open` refuses, a `pin_segments` of 0 that
    /// `Archive::open` panics on), so built any later, the refusal changes.
    #[test]
    fn every_entry_point_reads_credentials_before_it_opens_anything() {
        let refusal = "outbound: SERVE_TEST_UNSET_VAR is unset — host pay.test needs it for job \
                       ServeTestCharge";
        let unbindable = "no-such-address";
        let app = || charge_app(Host::new("pay.test").bearer_env("SERVE_TEST_UNSET_VAR"));
        let repl_cfg = || repl::ReplConfig {
            listen: Some(format!("127.0.0.1:{}", free_port())),
            initial_role: Role::Primary,
            peer_secret: Some(String::new()),
            strict_witnesses: false,
        };
        let repl_disk = || -> Box<dyn Disk> {
            let mut suffix = [0u8; 8];
            ProdEntropy::new().unwrap().fill(&mut suffix);
            let root = std::env::temp_dir().join(format!(
                "charpente-serve-refuse-repl-{:016x}",
                u64::from_le_bytes(suffix)
            ));
            Box::new(ProdDisk::new(root).unwrap())
        };

        assert!(bind_http(unbindable).is_err());
        let err = serve_with(unbindable, app(), OutboundOptions::default())
            .err()
            .unwrap();
        assert_eq!(
            (err.to_string().as_str(), Refusal::of(&err).is_some()),
            (refusal, true)
        );
        let err = serve(unbindable, app()).err().unwrap();
        assert_eq!(err.to_string(), refusal);
        let err = serve_replicated(unbindable, app(), repl_disk(), repl_cfg())
            .err()
            .unwrap();
        assert_eq!(err.to_string(), refusal);
        let err = backup(app(), repl_disk(), repl_cfg()).unwrap_err();
        assert_eq!(err.to_string(), refusal);
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        let bucket = std::env::temp_dir().join(format!(
            "charpente-serve-refuse-bucket-{:016x}",
            u64::from_le_bytes(suffix)
        ));
        let err = archive(
            app(),
            Box::new(crate::prod::FsObjectStore::new(&bucket).unwrap()),
            ArchiveConfig {
                prefix: "refuse".into(),
                writer: "serve-test".into(),
                snapshot_every: 0,
                pin_segments: 0,
                read_budget_bytes: 1 << 20,
            },
        )
        .unwrap_err();
        assert_eq!(err.to_string(), refusal);
    }
}
