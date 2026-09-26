//! M2 sub-project 2 (group commit) T5: `App::begin_group` +
//! `StagedDispatch::stage_or_subscribe` stage a request's effects into a
//! `Group` without committing it (spec §1, §2, §4) — the split that lets a
//! future drain loop (T6) run many requests under one fsync.
//! `handle_or_subscribe` becomes the one-request wrapper over that pair, so
//! this file's first test proves the split itself: staging must not touch
//! the WAL at all, which is what makes "N requests, one sync" possible.
//!
//! The second half of this file builds a per-operation disk tracer and
//! captures `tests/fixtures/pre-batching-trace.txt` — the batch-of-1 byte-
//! identity fixture (spec §7). It had to be captured HERE, while the kernel
//! is still unbatched, or the assertion it backs would compare batched output
//! to itself. That assertion lives here too
//! (`a_never_contended_universe_traces_identically_to_the_pre_batching_fixture`),
//! deliberately: T6 must keep it green, not author it after changing the
//! thing it polices.
//!
//! T6 needs a second, reply-aware trace for its contended batch. It must NOT
//! add reply events to `run_one_at_a_time_universe`'s trace — that would grow
//! `disk_trace_string()` by lines the committed fixture does not have, and
//! the identity assertion would fail for a reason with nothing to do with
//! batching. Give `run_two_request_batch` its own instrumented `Respond`.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use charpente::http::{ApiError, App, AppConfig, Handled, Method, Request};
use charpente::kernel::{
    EventSink, JobError, Kernel, Reply, RequestSource, Respond, SimSource, Step, Wake,
};
use charpente::wire::WireU64;
use charpente::{Schema, Tx, event, job, jobs, route, routes, table};
use charpente_core::db::Db;
use charpente_core::event::EventRow;
use charpente_core::io::{Disk, File, Instant};
use charpente_core::job::{JobRow, JobStatus};
use charpente_core::migration::{SchemaVersion, schema_version_id};
use charpente_core::table::{Id, View};
use charpente_sim::{FaultConfig, Sim};

const SEC: u64 = 1_000_000_000;

// ------------------------------------------------------------------ routes

#[table(global)]
struct Hit {
    id: Id<Hit>,
    count: u64,
}

/// A minimal write route: reads the current count, writes it back
/// incremented. One `stage()` call against this always produces exactly one
/// record (the row always changes), which is what makes `g.len() == 1` a
/// meaningful assertion below rather than an accident of an empty diff.
#[route(POST "/hit/:id", public)]
fn hit(tx: &mut Tx, id: Id<Hit>) -> Result<WireU64, ApiError> {
    let count = tx.get(id)?.map_or(0, |h| h.count) + 1;
    tx.put(&Hit { id, count });
    Ok(WireU64(count))
}

/// A read route, so a batch can contain one. Mounting it is pure in-memory
/// router work — it touches no disk, so the committed pre-batching fixture is
/// unaffected (and `a_never_contended_universe_traces_identically_...` would
/// say so immediately if that were wrong).
#[route(GET "/count/:id", public)]
fn count(tx: &mut Tx, id: Id<Hit>) -> Result<WireU64, ApiError> {
    Ok(WireU64(tx.get(id)?.map_or(0, |h| h.count)))
}

fn hit_routes() -> Vec<charpente::http::Route> {
    routes![hit, count]
}

// -------------------------------------------------------- op-counting disk
//
// Modelled on `SyncCounts`/`CountingDisk` in
// `crates/charpente-core/tests/engine.rs:182-249` (group-commit T2's
// precedent) but tracking write-shaped ops (`write_at`/`set_len`) and syncs
// as two separate counters instead of syncs alone — neither that struct nor
// its counterpart exists in this crate, so this is a local rebuild, not an
// import (T5 pre-flight finding A).

#[derive(Clone, Default)]
struct OpCounts {
    appends: Arc<AtomicU64>,
    syncs: Arc<AtomicU64>,
}

impl OpCounts {
    fn appends(&self) -> u64 {
        self.appends.load(Ordering::SeqCst)
    }

    fn syncs(&self) -> u64 {
        self.syncs.load(Ordering::SeqCst)
    }
}

struct CountingDisk {
    inner: charpente_sim::SimDisk,
    counts: OpCounts,
}

impl Disk for CountingDisk {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        let file = self.inner.open(path)?;
        Ok(Box::new(CountingFile {
            inner: file,
            counts: self.counts.clone(),
        }))
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        self.inner.remove(path)
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.inner.list(prefix)
    }

    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        self.inner.sync_dir(prefix)
    }
}

struct CountingFile {
    inner: Box<dyn File>,
    counts: OpCounts,
}

impl File for CountingFile {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read_at(offset, out)
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.counts.appends.fetch_add(1, Ordering::SeqCst);
        self.inner.write_at(offset, data)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.counts.appends.fetch_add(1, Ordering::SeqCst);
        self.inner.set_len(len)
    }

    fn sync(&self) -> io::Result<()> {
        self.counts.syncs.fetch_add(1, Ordering::SeqCst);
        self.inner.sync()
    }
}

fn app_with_op_counter(seed: u64) -> (App, OpCounts) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let counts = OpCounts::default();
    let disk = CountingDisk {
        inner: sim.disk(),
        counts: counts.clone(),
    };
    let app = App::open(
        Box::new(disk),
        Schema::new().table::<Hit>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(hit_routes());
    (app, counts)
}

/// Group-commit T5 Step 1 (the exact test the brief specifies): staging a
/// request produces a record in the group, but touches neither the WAL's
/// append path nor its sync — the record lives only in the group's overlay
/// until `commit` runs. That is the property that makes "N requests, one
/// fsync" possible: if staging touched the WAL at all, N stages would already
/// cost N disk operations before `commit` ever ran.
#[test]
fn stage_or_subscribe_produces_a_record_without_touching_the_wal() {
    let (mut app, counts) = app_with_op_counter(0x60C0_0007);
    let before = counts.appends() + counts.syncs();
    let (mut g, staged) = app.begin_group().expect("group");
    let handled = staged.stage_or_subscribe(&mut g, Request::new(Method::Post, "/hit/1"), 0);
    assert!(
        matches!(handled, Handled::Response(_)),
        "a write route responds"
    );
    assert_eq!(g.len(), 1, "the record is staged");
    assert_eq!(
        counts.appends() + counts.syncs() - before,
        0,
        "staging must not touch the WAL — that is what makes one fsync per batch possible"
    );
    g.commit().expect("commit");
}

/// `stage_or_subscribe` never commits on its own: the caller decides when.
/// Dropping a non-empty `Group` without `commit` is a contract panic
/// (`Group`'s `Drop` impl, `charpente-core/src/db.rs:2004-2016`) — this test
/// exercises exactly that boundary to pin what "staged but not yet
/// committed" actually means, rather than asserting it only indirectly via
/// the disk-op count above.
#[test]
#[should_panic(expected = "staged record(s) never committed")]
fn a_staged_group_dropped_without_commit_panics() {
    let (mut app, _counts) = app_with_op_counter(0x60C0_000A);
    let (mut g, staged) = app.begin_group().expect("group");
    let _ = staged.stage_or_subscribe(&mut g, Request::new(Method::Post, "/hit/1"), 0);
    // `g` drops here, non-empty and never committed.
}

/// The one-request wrapper (`handle_or_subscribe`, rewritten in this task to
/// run `begin_group` + `stage_or_subscribe` + `commit`) must still behave
/// exactly as `App::handle` did before this task: one write, one durable
/// record, response comes back 200. This is the regression backstop for the
/// refactor — `http.rs`/`kernel.rs` cover the rest via `cargo test -p
/// charpente --test http`/`--test kernel` (Step 4).
#[test]
fn handle_or_subscribe_still_commits_exactly_one_request() {
    let (mut app, counts) = app_with_op_counter(0x60C0_000B);
    let before_syncs = counts.syncs();
    let response = app.handle_at(Request::new(Method::Post, "/hit/1"), 0);
    assert_eq!(response.status, 200, "the write route still responds 200");
    assert_eq!(
        counts.syncs() - before_syncs,
        1,
        "one request must still cost exactly one fsync"
    );
    let snap = app.db_for_tests().snapshot().unwrap();
    let hits = snap.scan::<Hit>().unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].count, 1, "the write actually landed durably");
}

/// A request whose commit fails at the WAL append is a storage failure, not
/// a refusal, so the one-request wrapper must answer it with
/// `io_failure_response`'s text: the client is told the write may or may not
/// have committed, and to retry after recovery. Answering the bare disk error
/// instead drops both facts. Driven through `App::handle` and not the kernel,
/// because the kernel's batched tail calls `io_failure_response` itself and
/// never reaches `tx_error_response`.
#[test]
fn a_commit_failure_answers_the_storage_failure_text_not_the_bare_error() {
    let seed = 0x60C1_000A;
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let armed = Arc::new(AtomicBool::new(false));
    let mut app = App::open(
        Box::new(OneShotWalFaultDisk {
            inner: sim.disk(),
            armed: armed.clone(),
        }),
        Schema::new().table::<Hit>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(hit_routes());
    armed.store(true, Ordering::SeqCst);

    let response = app.handle(Request::new(Method::Post, "/hit/1"));

    assert!(
        !armed.load(Ordering::SeqCst),
        "seed {seed:#x}: the armed append fault never fired, so nothing below was tested"
    );
    assert_eq!(response.status, 500, "seed {seed:#x}");
    assert_eq!(
        String::from_utf8(response.body).unwrap(),
        "{\"error\":\"internal storage failure — the request may or may not have committed; \
         retry after the server recovers: simulated write failure (injected)\"}",
        "seed {seed:#x}"
    );
}

// --------------------------------------------------- pre-batching disk trace
//
// Neither `run_one_at_a_time_universe` nor `disk_trace_string()` exists
// anywhere in the repo (T5 pre-flight finding E) — `charpente-sim` has no
// per-operation disk trace at all, only `Sim::durable_fingerprint()`'s
// final-state hash, which cannot see a reordered or extra disk op. This is
// new infrastructure, shared by the ignored capture test below and left
// extensible for T6 (which layers a "reply" event onto the same ordered
// trace — see the T6 pre-flight, finding A).

#[derive(Clone, Default)]
struct DiskTrace(Arc<Mutex<Vec<String>>>);

impl DiskTrace {
    fn push(&self, line: String) {
        self.0
            .lock()
            .expect("disk trace lock poisoned by an earlier panic")
            .push(line);
    }

    /// Named `render`, not `to_string`: an inherent `to_string` shadows the
    /// `Display`-derived one and `clippy::inherent_to_string` denies it. A
    /// `Display` impl would be the other way out, but this is a test helper
    /// joining lines, not a type with a display form worth publishing.
    fn render(&self) -> String {
        self.0
            .lock()
            .expect("disk trace lock poisoned by an earlier panic")
            .join("\n")
    }

    /// The raw ordered entries (T6): `render()`'s joined string is right for
    /// a byte-identity comparison against the fixture, but T6's ack-ordering
    /// test needs `Vec::position`/filtering over individual events (disk ops
    /// AND `"reply"` markers), which a joined string cannot offer without
    /// re-splitting it.
    fn entries(&self) -> Vec<String> {
        self.0
            .lock()
            .expect("disk trace lock poisoned by an earlier panic")
            .clone()
    }
}

struct TracingDisk {
    inner: charpente_sim::SimDisk,
    trace: DiskTrace,
}

impl Disk for TracingDisk {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        let file = self.inner.open(path)?;
        Ok(Box::new(TracingFile {
            inner: file,
            trace: self.trace.clone(),
            path: path.to_string(),
        }))
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        self.inner.remove(path)
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.inner.list(prefix)
    }

    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        self.inner.sync_dir(prefix)
    }
}

struct TracingFile {
    inner: Box<dyn File>,
    trace: DiskTrace,
    path: String,
}

impl File for TracingFile {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read_at(offset, out)
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.trace
            .push(format!("{} write_at {} {}", self.path, offset, data.len()));
        self.inner.write_at(offset, data)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.trace.push(format!("{} set_len {len}", self.path));
        self.inner.set_len(len)
    }

    fn sync(&self) -> io::Result<()> {
        self.trace.push(format!("{} sync", self.path));
        self.inner.sync()
    }
}

/// One traced universe's outcome: the ordered disk-op trace, plus the
/// responses the kernel actually sent. The responses are what let a test
/// distinguish "the workload ran and was served" from "the kernel shut down
/// at the horizon having served nothing" — a distinction the disk trace alone
/// cannot make, because `App::open`'s own syncs are present either way (T5
/// review, Important 2).
struct UniverseTrace {
    trace: DiskTrace,
    responses: charpente::kernel::RecordedResponses,
}

impl UniverseTrace {
    fn disk_trace_string(&self) -> String {
        self.trace.render()
    }

    /// The `(status, body)` pairs the kernel sent, in send order.
    fn statuses(&self) -> Vec<u16> {
        self.responses
            .lock()
            .expect("responses lock poisoned by an earlier panic")
            .iter()
            .map(|(status, _)| *status)
            .collect()
    }
}

/// Drives a fresh `Kernel` over a `SimSource` whose requests arrive one full
/// second apart — never contended, so exactly one request is ready per
/// `step()` — through a disk that traces every op. `FaultConfig::none()`:
/// the trace is a canonical reference, not a fault-injection property, so it
/// must not vary run to run for reasons unrelated to the code under test.
fn run_one_at_a_time_universe(seed: u64) -> UniverseTrace {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let trace = DiskTrace::default();
    let disk = TracingDisk {
        inner: sim.disk(),
        trace: trace.clone(),
    };
    let app = App::open(
        Box::new(disk),
        Schema::new().table::<Hit>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(hit_routes());

    let mut source = SimSource::new(Box::new(sim.clock()), 5 * SEC);
    for i in 0..5u64 {
        source.push_at(i * SEC, Request::new(Method::Post, "/hit/1"));
    }
    let responses = source.responses();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    kernel.run(&mut source).unwrap();
    UniverseTrace { trace, responses }
}

/// Sanity check on the tracer itself, before trusting it to produce a fixture
/// T6's headline guarantee rests on: the same seed must trace identically
/// twice, the five requests must actually have been served, and the WAL must
/// show the unbatched shape — one sync per append, six times.
///
/// The naive form of the last assertion (`syncs >= 5`) is VACUOUS here and was
/// replaced (T5 review, Important 2): `App::open` alone emits six
/// `tables.redb sync` entries before the first WAL byte is written, so a
/// threshold on the total sync count is met by a universe that served nothing
/// at all — including one whose kernel shut down at the horizon before the
/// first arrival. That would have let a degenerate fixture be captured and
/// committed, and made T6's batch-of-1 assertion green and empty, which is
/// exactly the tautology the capture-it-HERE discipline exists to prevent.
#[test]
fn run_one_at_a_time_universe_is_deterministic_and_unbatched() {
    let first = run_one_at_a_time_universe(0x60C0_000C);
    let a = first.disk_trace_string();
    let b = run_one_at_a_time_universe(0x60C0_000C).disk_trace_string();
    assert_eq!(a, b, "the same seed must trace identically twice");

    // The workload ran and was served — the disk trace cannot tell you this.
    assert_eq!(
        first.statuses(),
        vec![200; 5],
        "all five requests must have been served 200; trace:\n{a}"
    );

    // One sync per append is what "unbatched" means on the wire. Batched
    // output is N appends then ONE sync, so this goes red the moment
    // anything batches — which is the property the fixture is capturing.
    let wal: Vec<&str> = a.lines().filter(|l| l.starts_with("wal/")).collect();
    assert_eq!(
        wal.len(),
        12,
        "six WAL records, each its own append+sync pair; trace:\n{a}"
    );
    for pair in wal.chunks(2) {
        assert!(
            pair[0].contains(" write_at "),
            "expected an append, got {pair:?}"
        );
        assert!(
            pair[1].ends_with(" sync"),
            "unbatched means one sync per append, got {pair:?}"
        );
    }
}

/// The batch-of-1 identity guarantee (spec §7), asserted from the commit that
/// captures the fixture rather than from T6 — the task that changes the thing
/// being policed.
///
/// This is here on the T5 review's Important 3. A test written *after* the
/// change it is meant to catch has never been red for the right reason, and
/// the cheapest way to make such a test green is to adjust the workload until
/// it passes. Landing it here inverts that: it is green today on unbatched
/// code, it goes red the instant T6's drain loop alters the schedule, and it
/// can only go green again when batch-of-1 identity is genuinely restored.
/// It also stops any edit to the `Hit` route, the request script,
/// `AppConfig::default()` or the platform cron from silently invalidating the
/// fixture between now and T6 — which would otherwise first surface inside T6
/// looking exactly like a batching bug.
///
/// T6 does NOT need to author this test; it needs to keep it green.
#[test]
fn a_never_contended_universe_traces_identically_to_the_pre_batching_fixture() {
    let expected = include_str!("fixtures/pre-batching-trace.txt");
    let actual = run_one_at_a_time_universe(0x60C0_0008).disk_trace_string();
    assert_eq!(
        actual,
        expected.trim(),
        "a never-contended universe must trace byte-identically to the pre-batching \
         capture — one request per step must cost exactly what it cost before batching"
    );
}

/// Captures the pre-batching disk trace. `#[ignore]`d because it writes a
/// tracked fixture rather than asserting anything. Run once, HERE, while the
/// kernel is still unbatched — T6 asserts a never-contended universe traces
/// byte-identically against this fixture; captured after T6 lands, that
/// assertion would compare T6's own output to itself and prove nothing.
// Ambient filesystem write, outside the deterministic boundary (DESIGN.md
// §1) — the same sanctioned-edge pattern as `charpente-cli`'s process
// spawning in tests/cli.rs. This writes a source-tree fixture at a developer's
// explicit request; it is not a `Disk` the platform runs on, so routing it
// through the trait would mean mounting the real filesystem to write one file.
// The test is `#[ignore]`d, so this never runs in `cargo test --workspace`.
//
// Note what this widens, so the next reader sees a decision and not a
// precedent (T5 review, Minor 5): every other allow of this family in the
// workspace lives in an EDGE crate — charpente-cli, edge-harness,
// bench-harness, guide-examples. `crates/charpente` is the platform crate,
// and this is the first such allow inside it. The fence is unchanged for
// every other site: the deny-list still fires everywhere it is not spelled
// out, and this allow is function-scoped over ten `#[ignore]`d lines.
#[allow(clippy::disallowed_methods)]
#[test]
#[ignore]
fn capture_pre_batching_trace() {
    let trace = run_one_at_a_time_universe(0x60C0_0008).disk_trace_string();
    std::fs::write(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/pre-batching-trace.txt"
        ),
        &trace,
    )
    .expect("write the fixture");
}

// ------------------------------------------------------ reply-aware trace
//
// T6's ack-ordering property (spec §9.1: "no reply may leave before the
// batch's sync") needs a trace that records a kernel-level reply alongside
// disk ops, on one ordered timeline — and no existing tracer does that
// (T6 pre-flight, finding A). T5's `TracingDisk`/`DiskTrace` above records
// *disk* ops only; a reply is not a disk op (it is `Respond::send`/
// `stream`), so it is invisible to that tracer no matter how it is driven.
// This is deliberately a SEPARATE facility from `run_one_at_a_time_universe`
// — it must never touch that function's trace, which backs the committed
// fixture and must stay disk-only forever (module doc above; T5 review
// Minor 7).

/// Wraps a `Reply` so `send`/`stream` push a `"reply"` marker into the SAME
/// `DiskTrace` a `TracingDisk` writes disk ops into — the two together are
/// what let a test see "was the sync before or after the reply" on one
/// timeline. `"reply"` (bare, no path prefix) can never collide with a disk
/// entry: every disk line is `"<path> <op> ..."`.
struct TracingReply {
    inner: Reply,
    trace: DiskTrace,
}

impl Respond for TracingReply {
    fn send(self: Box<Self>, response: charpente::http::Response) {
        self.trace.push("reply".to_string());
        self.inner.send(response);
    }

    fn stream(self: Box<Self>, response: charpente::http::Response) -> Box<dyn EventSink> {
        self.trace.push("reply".to_string());
        self.inner.stream(response)
    }
}

/// A `RequestSource` that wraps every reply `SimSource` hands out in a
/// `TracingReply`, sharing one `DiskTrace` with a `TracingDisk` mounted on
/// the same `App`. `poll`/`wait_until` otherwise just forward — this adds
/// no scheduling behavior of its own, so it does not perturb which requests
/// are ready when (the same-shape rule, DESIGN §1: this is test
/// instrumentation, not a second source implementation).
struct TracingSource {
    inner: SimSource,
    trace: DiskTrace,
}

impl RequestSource for TracingSource {
    fn poll(&mut self) -> Option<(Request, Reply)> {
        let (request, reply) = self.inner.poll()?;
        Some((
            request,
            Box::new(TracingReply {
                inner: reply,
                trace: self.trace.clone(),
            }),
        ))
    }

    fn wait_until(&mut self, deadline: Option<Instant>) -> Wake {
        self.inner.wait_until(deadline)
    }
}

/// Drives two requests arriving at the SAME virtual instant (genuinely
/// contended — both are ready the moment the kernel's drain loop polls a
/// second time at the same `now`, not merely scheduled close together)
/// through a disk-and-reply trace. Asserts both were actually served before
/// handing the trace back, so a degenerate run (dropped request, kernel
/// shutting down early) fails here with a direct message instead of
/// producing a trace that silently satisfies the ordering assertion by
/// having nothing in it (T5 review, Important 2's same concern, one level
/// up).
fn run_two_request_batch(seed: u64) -> Vec<String> {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let trace = DiskTrace::default();
    let disk = TracingDisk {
        inner: sim.disk(),
        trace: trace.clone(),
    };
    let app = App::open(
        Box::new(disk),
        Schema::new().table::<Hit>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(hit_routes());

    let mut inner = SimSource::new(Box::new(sim.clock()), SEC);
    inner.push_at(0, Request::new(Method::Post, "/hit/1"));
    inner.push_at(0, Request::new(Method::Post, "/hit/2"));
    let responses = inner.responses();
    let mut source = TracingSource {
        inner,
        trace: trace.clone(),
    };
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    kernel.run(&mut source).unwrap();

    let statuses: Vec<u16> = responses
        .lock()
        .expect("responses lock poisoned by an earlier panic")
        .iter()
        .map(|(status, _)| *status)
        .collect();
    assert_eq!(
        statuses,
        vec![200, 200],
        "both requests must be served 200 before the ordering assertion means anything"
    );
    trace.entries()
}

/// Ack-after-durable, batch-wide (spec §9.1): no reply may leave before the
/// ONE sync covering the batch it belongs to. Two genuinely contended
/// requests must share a single WAL sync, and both replies must follow it.
///
/// Two assertions, because either alone is passable while the property is
/// false. Ordering alone would hold on unbatched code if the trace happened
/// to end after one request; batch shape alone would hold if the kernel
/// synced once and replied *before* it. Together they are the property.
///
/// Note what is NOT asserted: that the whole trace contains exactly one WAL
/// sync. It contains two — the batch's, and the startup schema-adoption
/// record, which commits before any request is served and belongs to no
/// batch. Keying on the total was this test's first form and it failed
/// against a kernel that batched correctly.
#[test]
fn no_response_is_sent_before_the_group_syncs() {
    // 0x60C0_0009 was this test's seed and collided with the plan's Task 6
    // allocation; the namespace census in the T7 pre-flight put the first
    // free value here (T6 review M-2).
    let trace = run_two_request_batch(0x60C0_0010);
    let wal_syncs: Vec<usize> = trace
        .iter()
        .enumerate()
        .filter(|(_, op)| op.starts_with("wal/") && op.ends_with(" sync"))
        .map(|(i, _)| i)
        .collect();
    let reply_positions: Vec<usize> = trace
        .iter()
        .enumerate()
        .filter(|(_, op)| op.as_str() == "reply")
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        reply_positions.len(),
        2,
        "both requests must be answered: {trace:?}"
    );
    let last_sync = *wal_syncs.last().expect("the batch synced");

    // (1) Ordering. Unbatched code fails here: it syncs request 1, replies,
    // then syncs request 2 — so a reply sits before the final sync.
    assert!(
        reply_positions.iter().all(|&p| p > last_sync),
        "a reply was sent before the batch's sync — ack-after-durable is \
         broken: {trace:?}"
    );

    // (2) Batch shape. Both records must ride the SAME sync: count the WAL
    // appends between the previous sync and the batch's. Unbatched code
    // fails here too — its final sync covers exactly one record.
    let prev_sync = wal_syncs.len().checked_sub(2).map(|i| wal_syncs[i]);
    let appends_in_batch = trace[prev_sync.map_or(0, |i| i + 1)..last_sync]
        .iter()
        .filter(|op| op.starts_with("wal/") && op.contains(" write_at "))
        .count();
    assert_eq!(
        appends_in_batch, 2,
        "two contended requests must share ONE sync — that sync covered \
         {appends_in_batch} record(s), not 2: {trace:?}"
    );
}

/// Group-commit T6 review C-1 (Critical, Noah ruling 2026-08-12): a read
/// batched behind a write whose commit fails must NOT be acked with that
/// write's data.
///
/// `Group::stage` points every participant's `tx` at the group overlay, so a
/// read running after a sibling's write returns that sibling's UNCOMMITTED
/// effects. The first form of `CommitFailure::invalidates` keyed only on
/// "which record did you stage", on the premise that a read had nothing for a
/// commit to make uncertain — so the writer got its honest 500 while the
/// reader was acked 200 carrying state no durable copy ever held, or ever
/// would. Pre-batching that is structurally impossible: the read is a
/// separate step served against committed state.
///
/// No existing oracle can see this — `acked_implies_durable` checks acked
/// WRITES, and nothing else asserts a read's content after a failed batch —
/// which is exactly why it needs its own pin.
#[test]
fn a_read_batched_behind_a_failed_write_is_not_acked_with_its_data() {
    let sim = Sim::with_faults(0x60C0_000F, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Hit>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(hit_routes());
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();

    let mut source = SimSource::new(Box::new(sim.clock()), 10 * SEC);
    // Same instant, write first: one batch, the read staged behind the write
    // and therefore reading through its overlay.
    source.push_at(0, Request::new(Method::Post, "/hit/1"));
    source.push_at(0, Request::new(Method::Get, "/count/1"));
    let responses = source.responses();

    // Every disk op from here fails, so the batch's commit cannot apply
    // either record: `applied == 0`.
    sim.crash_disk_now();
    let _ = kernel.run(&mut source);

    let statuses: Vec<u16> = responses
        .lock()
        .expect("responses lock poisoned by an earlier panic")
        .iter()
        .map(|(status, _)| *status)
        .collect();
    assert_eq!(
        statuses.len(),
        2,
        "both requests were staged in one batch, so both must be answered"
    );
    assert!(
        statuses.iter().all(|&s| s == 500),
        "nothing reached the store, so the read saw only the failed write's \
         overlay and cannot be acked: {statuses:?}"
    );
}

// -------------------------------------------------------------- job arm

/// A no-op job: its handler writes nothing, so its ONLY effect is its own
/// Done-mark — one record per job, deterministically, which is what makes
/// `syncs_in_job_arm == 1` a meaningful assertion below (T7 pre-flight §11).
#[job]
struct NoOp {}

impl NoOp {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

/// `n` due jobs, drained by a single `Kernel::step` call, plus however many
/// WAL syncs that cost.
struct JobBatchTrace {
    syncs_in_job_arm: u64,
    jobs_done: u64,
}

/// T7 Step 1's rig (pre-flight §10): a job becomes due the moment its
/// `JobRow` exists, and `Kernel::build` deliberately discards the drain
/// buffer left over from pre-construction enqueues (`kernel/mod.rs`'s
/// seed-0x650 comment) — so enqueuing all `n` jobs in ONE transaction before
/// `Kernel::new` is the one shape that reliably puts all of them in the heap
/// at once, with no race against construction order.
///
/// Drives exactly one `Kernel::step` — NOT `Kernel::run` — deliberately (T7
/// pre-flight §9): `run`'s clean-shutdown path unconditionally checkpoints
/// the STORE before returning, which would add its own syncs on top of the
/// job arm's and defeat a raw total (App::open's own startup syncs are
/// excluded the same way T5's fixture capture excludes them, by measuring
/// only after construction). A single `step()` call touches disk ONLY
/// inside the one `Ready` arm it services, so plain unscoped `OpCounts` IS
/// the job arm's own sync count here — no WAL-path filtering required, and
/// this reuses T5's `CountingDisk` unchanged rather than starting a fourth
/// wrapper family.
fn run_universe_with_due_jobs(seed: u64, n: u64) -> JobBatchTrace {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let counts = OpCounts::default();
    let disk = CountingDisk {
        inner: sim.disk(),
        counts: counts.clone(),
    };
    let mut app = App::open(
        Box::new(disk),
        Schema::new().table::<Hit>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![NoOp]);
    app.db_for_tests()
        .transact(|tx| {
            for _ in 0..n {
                tx.enqueue(NoOp {});
            }
            Ok::<(), std::io::Error>(())
        })
        .unwrap();

    let mut source = SimSource::new(Box::new(sim.clock()), 0);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let before = counts.syncs();
    let step = kernel.step(&mut source).unwrap();
    assert!(
        matches!(step, Step::Worked),
        "the job arm must have been the ready source — a degenerate step \
         would satisfy the assertions below vacuously"
    );
    JobBatchTrace {
        syncs_in_job_arm: counts.syncs() - before,
        jobs_done: kernel.stats().jobs_done,
    }
}

/// The headline job-arm property (T7 Step 1): 40 simultaneously-due jobs
/// must drain as one batch under one fsync, exactly as the request arm does
/// (T6) — the canonical transaction's SECOND record (the job's Done-mark) is
/// what T7 amortizes; T6 alone only amortized the first.
#[test]
fn forty_due_jobs_cost_one_fsync() {
    let g = run_universe_with_due_jobs(0x60C0_000D, 40);
    assert_eq!(
        g.syncs_in_job_arm, 1,
        "40 due jobs must drain as one batch; got {} syncs",
        g.syncs_in_job_arm
    );
    assert_eq!(
        g.jobs_done, 40,
        // Fix round M-6: this is the DISCRIMINATING conjunct, not a sanity
        // check — without it, `syncs_in_job_arm == 1` alone is satisfiable
        // by a degenerate step that ran only ONE of the 40 due jobs (a
        // single-job commit also costs exactly one sync).
        "all 40 due jobs must have run in this one step, not merely one job under one sync"
    );
}

/// A job that reads its own row's `count`, increments it, and writes it back
/// — a read-modify-write, so two of these against the SAME row in one batch
/// is the twin that discriminates a working group overlay from a bare
/// snapshot (T7 Step 3b, C-6).
#[job]
struct BumpHit {
    id: u64,
}

impl BumpHit {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        let id = Id::<Hit>::from_raw(self.id);
        let count = tx.get(id)?.map_or(0, |h| h.count) + 1;
        tx.put(&Hit { id, count });
        Ok(())
    }
}

/// Group-commit T7, C-6: the group overlay's read-your-own-writes property,
/// exercised for the JOB arm (T8 owns the request-arm twin). Two jobs
/// read-modify-write the SAME row, both due at once, so both land in one
/// `Kernel::step` batch. If the group overlay were absent or wrong, both
/// would read the pre-batch value and both would write `count = 1` — the
/// oracle here is a per-job REVIEW of what the store actually holds
/// afterward, not a proxy.
///
/// `due_jobs_are_not_starved_by_a_request_flood` (`tests/kernel.rs`) cannot
/// substitute: every job in it writes a distinct slot, so no two jobs in a
/// batch ever touch the same row (T7 pre-flight C-6).
#[test]
fn two_same_row_jobs_in_one_batch_see_each_others_writes() {
    let sim = Sim::with_faults(0x60C0_000E, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Hit>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![BumpHit]);
    app.db_for_tests()
        .transact(|tx| {
            tx.enqueue(BumpHit { id: 1 });
            tx.enqueue(BumpHit { id: 1 }); // same row, same batch
            Ok::<(), std::io::Error>(())
        })
        .unwrap();

    let mut source = SimSource::new(Box::new(sim.clock()), 0);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let step = kernel.step(&mut source).unwrap();
    assert!(
        matches!(step, Step::Worked),
        "the job arm must have been the ready source"
    );
    assert_eq!(
        kernel.stats().jobs_done,
        2,
        "both same-instant jobs must drain in this one step — a kernel that \
         still services one job per step is the pre-T7 shape and this \
         assertion is meant to catch exactly that"
    );
    let app = kernel.into_app();
    let snap = app.db().snapshot().unwrap();
    let hits = snap.scan::<Hit>().unwrap();
    assert_eq!(
        hits.len(),
        1,
        "both jobs touch the same row, got {} rows",
        hits.len()
    );
    assert_eq!(
        hits[0].count, 2,
        "both same-row jobs in one batch must see each other's staged \
         write through the group overlay, not the pre-batch snapshot \
         (a broken overlay would leave this at 1)"
    );
}

// ======================================================================
// T8a — the deterministic property suite (spec §9; task-8a-brief.md /
// task-8-preflight.md). Steps 1, 2, 3, 5, 5b.1 and 5b.2 below (Step 4 lives
// in tests/realtime.rs — C-6; Step 5c was closed at T6, freeing its seed).
// The burst-swarm workload and the fairness invariant (plan Step 6 /
// RULINGs 2-3) are 8b's, dispatched separately — nothing here touches
// `kernel_workload` or adds a swarm.
// ======================================================================

/// The full schema `App::open` builds internally (`app.rs:196-202`): the
/// app's own tables plus the platform's `Session`/`ApiKey`/`CronRow` —
/// EVERY app gets the built-in daily session-expiry sweep's cron entry with
/// no opt-in, so its `CronRow` lands in the log even for an app that never
/// calls `.schedule(...)`. A direct `Db::open` reopen (bypassing
/// `App::open`, as the crash-sweep and restart-durability checks below do)
/// must register the identical set, or a WAL record referencing one of
/// them fails to decode with "the Schema does not include it".
fn hit_schema() -> Schema {
    Schema::new()
        .table::<Hit>()
        .table::<charpente::http::Session>()
        .table::<charpente::auth::ApiKey>()
        .table::<charpente::kernel::CronRow>()
}

// --------------------------------------------- Step 1: the sync is atomic

/// T8a Step 1 (spec §9.1, task-8-preflight.md C-4): a crash landing
/// anywhere between the batch's first append and its sync must leave NO
/// record of the batch durable and must not have acked anything.
///
/// **Scoped to `FaultConfig::none()`, and only there** (C-4): under
/// `FaultConfig::default()` (`crash_apply: 0.4`) or any swarm config, an
/// unsynced write legitimately SURVIVES a crash some of the time — "nothing
/// survived" would then be false about correct code, not a bug. This
/// property must never be lifted into a swarm workload (8b's territory).
///
/// **The sweep is self-terminating, not pre-bounded by a separate op
/// count.** An earlier version of this test measured a "dry" op count via
/// `CountingDisk` and swept exactly that many crash points — but
/// `CountingDisk` and `Sim::crash_disk_after`'s own countdown do not charge
/// identically for every op kind (`sync_dir` is one gap), so the measured
/// bound silently undershot the real one and the sweep exited one crash
/// point before the batch's own sync legitimately completed, so the LAST
/// point swept still looked durable-free for the wrong reason. Instead:
/// sweep upward from `crash_after = 0` and stop the FIRST time the batch is
/// observed to have durably committed in full — that is the actual boundary
/// "between the first append and the sync", read off the platform's own
/// behaviour rather than guessed from a second counting mechanism. Sweep
/// shape otherwise follows `transactions.rs:1416`'s
/// `breadcrumb_write_is_crash_safe_the_old_checkpoint_stays_protected`.
#[test]
fn the_sync_is_atomic_over_the_batch() {
    fn two_request_batch(source: &mut SimSource) {
        source.push_at(0, Request::new(Method::Post, "/hit/1"));
        source.push_at(0, Request::new(Method::Post, "/hit/2"));
    }

    const MAX_CRASH_POINTS: u64 = 60;
    let mut observed_full_commit_at: Option<u64> = None;

    for crash_after in 0..MAX_CRASH_POINTS {
        let sim = Sim::with_faults(0x60C1_0001, FaultConfig::none());
        let app = App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Hit>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .unwrap()
        .mount(hit_routes());
        let mut source = SimSource::new(Box::new(sim.clock()), SEC);
        two_request_batch(&mut source);
        let responses = source.responses();
        let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
        // Baseline BEFORE the crash countdown starts: every app durably
        // applies the built-in daily session-expiry sweep's cron entry
        // inside `Kernel::new` itself (`hit_schema`'s doc), so
        // `last_applied()` is already `Some(0)`, not `None`, by this point
        // — the batch under test must leave it EXACTLY where it was (or
        // advance it by exactly 2, seq 1 and seq 2, if the crash landed
        // after a legitimate full commit), never partially.
        let baseline_applied = kernel.app().db().last_applied();
        sim.crash_disk_after(crash_after);
        let _ = kernel.step(&mut source);

        let statuses: Vec<u16> = responses
            .lock()
            .expect("responses lock poisoned by an earlier panic")
            .iter()
            .map(|(status, _)| *status)
            .collect();

        sim.restart();
        let db = Db::open(Box::new(sim.disk()), AppConfig::default().db, hit_schema()).unwrap();
        let after = db.last_applied();

        if after == baseline_applied {
            // The crash landed before the batch's sync completed: nothing
            // reached the store, and nothing may have been acked.
            assert!(
                statuses.iter().all(|&s| s != 200),
                "crash_after={crash_after}: a crash before the batch's sync completed must \
                 never leave a 200 behind: {statuses:?}"
            );
        } else {
            // The crash landed strictly AFTER the sync legitimately
            // completed — the batch really did commit, in full (never
            // partially: `after` must be exactly two seqs past the
            // baseline, both records at once, which is what "the sync is
            // atomic over the batch" means from the other side). This is
            // the sweep's natural stopping point.
            let advanced = match (baseline_applied, after) {
                (None, Some(a)) => a + 1,
                (Some(b), Some(a)) => a - b,
                other => panic!(
                    "crash_after={crash_after}: last_applied went backwards or \
                                  disappeared: {other:?}"
                ),
            };
            assert_eq!(
                advanced, 2,
                "crash_after={crash_after}: the batch must apply ALL of its records or NONE — \
                 got an advance of {advanced} from baseline {baseline_applied:?} to {after:?}. \
                 An advance of 2 is the pass; anything in between is a torn commit. But note \
                 that {advanced} == 1 most likely means the kernel did NOT batch at all (one \
                 record per commit), not that a two-record commit tore — check batching before \
                 hunting an atomicity bug"
            );
            observed_full_commit_at = Some(crash_after);
            break;
        }
    }

    let Some(boundary) = observed_full_commit_at else {
        panic!(
            "anti-vacuity: the sweep never observed the batch commit within \
             {MAX_CRASH_POINTS} crash points — widen the bound, or something is preventing \
             the batch from ever completing"
        );
    };
    assert!(
        boundary >= 2,
        "anti-vacuity: a two-record batch must cost at least two disk ops before it can \
         durably commit, so the boundary must be at crash_after >= 2 — got {boundary}, which \
         means the sweep covered too few points to be meaningful"
    );
}

// ---------------------------------- Step 2: read-your-own-writes (request arm)

/// T8a Step 2 (spec §9.2, task-8-preflight.md C-2): read-your-own-writes
/// across a batch — the property the whole group overlay exists for. Two
/// requests against the SAME row in one batch; the second must see the
/// first's write, so the final count is 2, not 1.
///
/// C-2's vacuity trap: "the final value is 2" alone holds on an UNBATCHED
/// kernel too, since pre-batching each request already committed before the
/// next one ran. `kernel.stats().requests == 2` after exactly ONE `step()`
/// call proves the two requests were actually drained into the SAME
/// batch — the identical guard T7's own M-6 fix had to add to its job-arm
/// twin (`two_same_row_jobs_in_one_batch_see_each_others_writes`,
/// `forty_due_jobs_cost_one_fsync`, above).
#[test]
fn two_same_row_requests_in_one_batch_see_each_others_writes() {
    let sim = Sim::with_faults(0x60C1_0002, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Hit>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(hit_routes());
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let mut source = SimSource::new(Box::new(sim.clock()), SEC);
    source.push_at(0, Request::new(Method::Post, "/hit/1"));
    source.push_at(0, Request::new(Method::Post, "/hit/1")); // SAME row, same batch
    let responses = source.responses();

    let step = kernel.step(&mut source).unwrap();
    assert!(
        matches!(step, Step::Worked),
        "the request arm must have been the ready source"
    );
    assert_eq!(
        kernel.stats().requests,
        2,
        "both requests must be drained into this ONE step — without this conjunct the row \
         assertion below is satisfiable by an UNBATCHED kernel too, since pre-batching each \
         request already committed before the next ran"
    );

    let statuses: Vec<u16> = responses
        .lock()
        .expect("responses lock poisoned by an earlier panic")
        .iter()
        .map(|(status, _)| *status)
        .collect();
    assert_eq!(statuses, vec![200, 200]);

    let app = kernel.into_app();
    let snap = app.db().snapshot().unwrap();
    let hits = snap.scan::<Hit>().unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].count, 2,
        "the second request must see the first's staged write through the group overlay, not \
         the pre-batch snapshot (a naive bare-snapshot implementation would leave this at 1)"
    );
}

// --------------------------------------------- Step 3: invalidated by what you SAW

/// `checkpoint_every: 1` so the FIRST batch that stages any record runs its
/// checkpoint inside that same commit — mirrors `kernel.rs`'s
/// `latch_config` (a different test binary, so this is a deliberate
/// duplicate, not an import: integration tests share no code, per
/// `charpente-core/tests/engine.rs`'s module doc).
fn checkpoint_config() -> AppConfig {
    AppConfig {
        db: charpente::DbConfig {
            checkpoint_every: 1,
            ..AppConfig::default().db
        },
        ..AppConfig::default()
    }
}

/// Combines a `TracingDisk`-shaped write trace with a ONE-SHOT store-sync
/// fault: the FIRST `.sync()` call on the store's own file fails once, then
/// disarms — transient, unlike `Sim::crash_disk_now`/`crash_disk_after`
/// (which set a PERMANENT crashed state that only `Sim::restart` clears).
/// Both halves are load-bearing, in different tests. The FAULT half is what
/// Step 3(a) and 5b.1's negative-space arm both need: a controlled
/// checkpoint failure that leaves the process able to keep serving
/// afterward. The TRACE half is read only by 5b.1's negative-space arm,
/// where record size is what proves `db.adopted` was folded before the
/// failure — Step 3(a) passes a trace it never inspects, because its own
/// oracle is the four response statuses.
struct FaultyTracingDisk {
    inner: charpente_sim::SimDisk,
    trace: DiskTrace,
    store_path: String,
    armed: Arc<AtomicBool>,
    fired: Arc<AtomicU64>,
}

impl Disk for FaultyTracingDisk {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        let inner = self.inner.open(path)?;
        Ok(Box::new(FaultyTracingFile {
            inner,
            trace: self.trace.clone(),
            path: path.to_string(),
            fault: (path == self.store_path).then(|| (self.armed.clone(), self.fired.clone())),
        }))
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        self.inner.remove(path)
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.inner.list(prefix)
    }

    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        self.inner.sync_dir(prefix)
    }
}

struct FaultyTracingFile {
    inner: Box<dyn File>,
    trace: DiskTrace,
    path: String,
    /// `Some` only for the store file — `(armed, fired)`.
    fault: Option<(Arc<AtomicBool>, Arc<AtomicU64>)>,
}

impl File for FaultyTracingFile {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read_at(offset, out)
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.trace
            .push(format!("{} write_at {} {}", self.path, offset, data.len()));
        self.inner.write_at(offset, data)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.trace.push(format!("{} set_len {len}", self.path));
        self.inner.set_len(len)
    }

    fn sync(&self) -> io::Result<()> {
        if let Some((armed, fired)) = &self.fault
            && armed.swap(false, Ordering::SeqCst)
        {
            fired.fetch_add(1, Ordering::SeqCst);
            return Err(io::Error::other(
                "simulated fsync failure — durability of writes since the last successful \
                 sync is unknown",
            ));
        }
        self.trace.push(format!("{} sync", self.path));
        self.inner.sync()
    }
}

/// T8a Step 3 (spec §9.4, task-8-preflight.md C-5): a failed batch's
/// participants are invalidated by what they SAW, per
/// `CommitFailure::invalidates` (`db.rs:2293`) — restated here per the
/// ruled correction (spec §4 amendment (1)), not the plan's writer-centric
/// original.
///
/// Per RULING 1 (task-8a-brief.md), only the two kernel-REACHABLE arms are
/// exercised: `applied == n` (checkpoint) and `applied == 0` (append/sync).
/// The interior `0 < i < n` case has no seam at the kernel layer
/// (`Store::apply` is a `Durability::None` redb write that never reaches
/// the `Disk` boundary for a cache-resident store — M2 sub-project 0a's own
/// measurement) and is already pinned deterministically one layer down by
/// `submit_all_reports_how_many_applied_before_a_mid_batch_failure`
/// (`charpente-core/tests/engine.rs:342`, seed `0x60C4_0001`).
#[test]
fn a_failed_batchs_participants_are_invalidated_by_what_they_saw() {
    // (a) checkpoint arm: applied == n — every record reached the store, so
    // the failure is bookkeeping after the fact and invalidates nobody.
    {
        let sim = Sim::with_faults(0x60C1_0003, FaultConfig::none());
        let cfg = checkpoint_config();
        let db_cfg = cfg.db.clone();
        let trace = DiskTrace::default();
        let armed = Arc::new(AtomicBool::new(false));
        let fired = Arc::new(AtomicU64::new(0));
        let disk = FaultyTracingDisk {
            inner: sim.disk(),
            trace: trace.clone(),
            store_path: db_cfg.store_path.clone(),
            armed: armed.clone(),
            fired: fired.clone(),
        };
        let app = App::open(
            Box::new(disk),
            Schema::new().table::<Hit>(),
            Box::new(sim.entropy()),
            cfg,
        )
        .unwrap()
        .mount(hit_routes());

        // Arm PAST `Kernel::new`, not merely past `App::open`: every app
        // gets the built-in daily session-expiry sweep's cron entry with no
        // opt-in (`app.rs:184-189`), and `Kernel::build` upserts it via its
        // own solo `db.transact()` — which, under THIS test's
        // `checkpoint_every: 1`, runs its own checkpoint. Arming before
        // `Kernel::new` faults that unrelated commit instead of this test's
        // batch, and — since a checkpoint failure latches the store rather
        // than merely erroring — leaves every later store operation broken
        // for a reason this test is not about.
        let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
        armed.store(true, Ordering::SeqCst);

        let mut source = SimSource::new(Box::new(sim.clock()), SEC);
        // Arrival order: read, write, read, write.
        source.push_at(0, Request::new(Method::Get, "/count/1"));
        source.push_at(0, Request::new(Method::Post, "/hit/1"));
        source.push_at(0, Request::new(Method::Get, "/count/1"));
        source.push_at(0, Request::new(Method::Post, "/hit/1"));
        let responses = source.responses();

        let step = kernel.step(&mut source);
        assert!(step.is_err(), "the checkpoint's own fsync must fail");
        assert_eq!(
            fired.load(Ordering::SeqCst),
            1,
            "the injector must fire exactly once"
        );

        let statuses: Vec<u16> = responses
            .lock()
            .expect("responses lock poisoned by an earlier panic")
            .iter()
            .map(|(status, _)| *status)
            .collect();
        // This vector-equality IS the batch-membership guard, and it is
        // load-bearing rather than cosmetic (T8a review M-5): unbatched,
        // request 1 is acked 200, request 2's `transact` swallows the
        // checkpoint failure (`db.rs:1441`), the kernel then stops on
        // poison, and requests 3 and 4 are never answered at all — so the
        // vector is TWO elements and this fails. Do not "simplify" it to
        // `statuses.iter().all(|&s| s == 200)`, which two elements satisfy.
        assert_eq!(
            statuses,
            vec![200, 200, 200, 200],
            "every record reached the store before the checkpoint's fsync failed — the \
             failure is durability BOOKKEEPING after the fact, and invalidates nobody: \
             {statuses:?}"
        );

        // The restart clause (spec §4): "a 500 here may mean committed" —
        // and here nothing WAS a 500, so pin the stronger, positive half:
        // after restart, the durable batch is present.
        sim.restart();
        let db2 = Db::open(Box::new(sim.disk()), db_cfg, hit_schema()).unwrap();
        let hits = db2.snapshot().unwrap().scan::<Hit>().unwrap();
        assert_eq!(hits.len(), 1, "one Hit row, written twice");
        assert_eq!(
            hits[0].count, 2,
            "both write requests' effects must be durable after restart — the checkpoint's \
             fsync failure never touched the WAL's own durability: got count={}",
            hits[0].count
        );
    }

    // (b) append/sync arm: applied == 0 — nothing reached the store, so
    // everything at or after the first staged record is invalidated, but a
    // read that ran strictly ahead of every write keeps its honest answer.
    {
        let sim = Sim::with_faults(0x60C1_0003, FaultConfig::none());
        let app = App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Hit>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .unwrap()
        .mount(hit_routes());
        let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
        let mut source = SimSource::new(Box::new(sim.clock()), SEC);
        source.push_at(0, Request::new(Method::Get, "/count/1"));
        source.push_at(0, Request::new(Method::Post, "/hit/1"));
        source.push_at(0, Request::new(Method::Get, "/count/1"));
        source.push_at(0, Request::new(Method::Post, "/hit/1"));
        let responses = source.responses();

        sim.crash_disk_now();
        let _ = kernel.run(&mut source);

        let statuses: Vec<u16> = responses
            .lock()
            .expect("responses lock poisoned by an earlier panic")
            .iter()
            .map(|(status, _)| *status)
            .collect();
        assert_eq!(
            statuses,
            vec![200, 500, 500, 500],
            "the leading read ran ahead of every staged record and saw only committed state \
             (its honest 200); everything from the first write onward observed — or was — an \
             uncommitted write that never applied: {statuses:?}"
        );
    }
}

// -------------------------------------------- Step 5: job-panic mid-batch

/// A job that panics on command instead of writing — the shape T8a Step 5
/// needs (task-8-preflight.md C-1): three same-instant jobs, one panicking,
/// so the other two's isolation is provable against real siblings rather
/// than a solo run.
#[job]
struct MaybeBoom {
    id: u64,
    boom: bool,
}

impl MaybeBoom {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        if self.boom {
            panic!("job handler exploded");
        }
        let id = Id::<Hit>::from_raw(self.id);
        let count = tx.get(id)?.map_or(0, |h| h.count) + 1;
        tx.put(&Hit { id, count });
        Ok(())
    }
}

/// T8a Step 5, REWRITTEN FOR JOB HANDLERS (spec §9.6 as amended 2026-08-12,
/// task-8-preflight.md C-1 — the headline finding). The plan's original
/// text asked for ROUTE-handler panic isolation, which does not exist: the
/// request path has no `catch_unwind`, a panicking route handler STOPS THE
/// NODE (pinned by `a_panicking_route_handler_stops_the_node_it_is_not_
/// isolated`, `tests/kernel.rs:193`, seed `0x0DED_0003`), and §9.6 itself
/// carries an explicit warning against writing this against a route handler
/// and then "fixing" the resulting red with a `catch_unwind` — that is a
/// design change disguised as a test repair, and this test does not do it.
///
/// What DOES survive, and is genuinely uncovered: job handlers ARE isolated
/// (`dispatch.rs:194`'s `catch_unwind` sits inside the transaction closure
/// and converts the panic to `Err(JobError::App(..))`), and nothing before
/// this task asserted that a panicking job leaves its BATCH SIBLINGS
/// correct — the only existing coverage
/// (`panicking_handler_is_absorbed_into_the_retry_path`, `kernel.rs:243`)
/// drives the one-job wrapper, never a batch.
///
/// **Scope: this covers §9.6's SIBLING clause, not its overlay clause.**
/// §9.6 also says the panicking job "leaves the group overlay clean", and
/// that clause is NOT assertable here — measured, not assumed (T8a review
/// M-2). The group overlay is a read view, never a record source, so a leak
/// is observable only by a sibling that READS the panicking job's row;
/// reordering the handler to write-then-panic and then mutating
/// `Group::stage` to merge a failed stage's overlay leaves this test green.
/// The clause is genuinely discharged one layer down, at the `Group` layer:
/// `a_staged_transaction_that_fails_leaves_the_group_untouched`
/// (`charpente-core/tests/group_commit.rs:90`, seed `0x60C0_0006`) covers
/// the `Err` exit — which is the exit `dispatch.rs`'s `catch_unwind`
/// actually takes — and `a_panicking_stage_leaves_the_group_clean`
/// (`:116`, `0x60C0_0004`) covers the unwind exit.
#[test]
fn a_panicking_job_leaves_its_batch_siblings_correct() {
    let sim = Sim::with_faults(0x60C1_0005, FaultConfig::none());
    let counts = OpCounts::default();
    let disk = CountingDisk {
        inner: sim.disk(),
        counts: counts.clone(),
    };
    let mut app = App::open(
        Box::new(disk),
        Schema::new().table::<Hit>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![MaybeBoom]);
    app.db_for_tests()
        .transact(|tx| {
            tx.enqueue(MaybeBoom { id: 1, boom: false });
            tx.enqueue(MaybeBoom { id: 2, boom: true });
            tx.enqueue(MaybeBoom { id: 3, boom: false });
            Ok::<(), std::io::Error>(())
        })
        .unwrap();

    let mut source = SimSource::new(Box::new(sim.clock()), 0);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let before = counts.syncs();
    let step = kernel.step(&mut source).unwrap();
    assert!(
        matches!(step, Step::Worked),
        "the job arm must have been the ready source"
    );
    assert_eq!(
        counts.syncs() - before,
        1,
        "three simultaneously-due jobs, one panicking, must still cost ONE sync — a panic \
         mid-batch must not fragment the commit"
    );
    assert_eq!(
        kernel.stats().jobs_done,
        2,
        "the two non-panicking jobs must both complete in this one step"
    );
    assert_eq!(
        kernel.stats().jobs_retried,
        1,
        "the panicking job takes the retry path, not the dead-letter one"
    );

    let app = kernel.into_app();
    let snap = app.db().snapshot().unwrap();
    let hits = snap.scan::<Hit>().unwrap();
    assert_eq!(
        hits.len(),
        2,
        "both non-panicking jobs' writes must be durable: {} row(s)",
        hits.len()
    );
    assert!(
        hits.iter().all(|h| h.count == 1),
        "each surviving job wrote its row exactly once"
    );

    let jobs = snap.scan::<JobRow>().unwrap();
    let done = jobs.iter().filter(|j| j.status == JobStatus::Done).count();
    let pending: Vec<_> = jobs
        .iter()
        .filter(|j| j.status == JobStatus::Pending)
        .collect();
    assert_eq!(done, 2, "two Done rows, {} of 3 total", jobs.len());
    assert_eq!(pending.len(), 1, "one Pending row (the retried panic)");
    assert_eq!(
        pending[0].attempts, 1,
        "one attempt charged to the panic, not zero and not more"
    );
    assert!(
        pending[0]
            .last_error
            .as_deref()
            .unwrap_or("")
            .contains("handler panicked"),
        "the retry record must name the panic: {:?}",
        pending[0].last_error
    );
}

// --------------------------------------------- Step 5b.1: group-aware adoption

/// Fires ONCE on the very first `write_at` call made through this disk
/// (targeting whichever path asks first — in these tests, that is always
/// the WAL, since nothing else is written yet), then disarms and passes
/// every later call straight through. Distinct from `Sim::crash_disk_now`/
/// `crash_disk_after`, which set a PERMANENT crashed state that only
/// `Sim::restart` clears — this needs to fail exactly one append and then
/// let the SAME live `Db` keep working, so a retry can run without a
/// restart in between.
struct OneShotWalFaultDisk {
    inner: charpente_sim::SimDisk,
    armed: Arc<AtomicBool>,
}

impl Disk for OneShotWalFaultDisk {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        Ok(Box::new(OneShotWalFaultFile {
            inner: self.inner.open(path)?,
            armed: self.armed.clone(),
        }))
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        self.inner.remove(path)
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.inner.list(prefix)
    }

    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        self.inner.sync_dir(prefix)
    }
}

struct OneShotWalFaultFile {
    inner: Box<dyn File>,
    armed: Arc<AtomicBool>,
}

impl File for OneShotWalFaultFile {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read_at(offset, out)
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        if self.armed.swap(false, Ordering::SeqCst) {
            return Err(io::Error::other("simulated write failure (injected)"));
        }
        self.inner.write_at(offset, data)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }

    fn sync(&self) -> io::Result<()> {
        self.inner.sync()
    }
}

/// The byte counts of the WAL appends the trace recorded after `from`, in
/// order. Record SIZE is 5b.1's discriminator on both of its halves: the
/// adoption row rides inside the record itself, so a record carrying one is
/// strictly larger than the same write without it, while row count cannot
/// tell them apart (see the headline test's doc).
fn wal_append_sizes(trace: &DiskTrace, from: usize) -> Vec<usize> {
    trace.entries()[from..]
        .iter()
        .filter(|op| op.starts_with("wal/") && op.contains(" write_at "))
        .map(|op| {
            op.rsplit(' ')
                .next()
                .expect("write_at trace line always ends in a byte count")
                .parse()
                .expect("the trailing token is the byte count")
        })
        .collect()
}

/// T8a Step 5b.1, headline (task-8-preflight.md C-7): two FIRST writes to
/// the same table in one batch must not each carry their own adoption row.
///
/// C-7's corrections: no new versioned fixture is needed — the adoption
/// filter keys on `FINGERPRINT.is_some()` (`db.rs:2118`), which EVERY
/// `#[table]` has (`charpente-macros/src/table.rs:125` emits it
/// unconditionally), not on `version >= 2`; `Hit` already qualifies. And
/// row COUNT cannot discriminate a working union from a broken one:
/// `schema_version_id` hashes the TABLE NAME deterministically
/// (`migration.rs:137`), so a redundant second write would silently
/// OVERWRITE the same row with byte-identical content — both a correct and
/// a broken union leave exactly one row. The discriminating signal is the
/// SECOND record's own size: with the union present it carries only the
/// Hit write; with it broken it also carries a redundant SchemaVersion
/// entry, which is strictly larger.
#[test]
fn a_batchs_second_first_write_does_not_duplicate_the_adoption_row() {
    let sim = Sim::with_faults(0x60C1_0006, FaultConfig::none());
    let trace = DiskTrace::default();
    let disk = TracingDisk {
        inner: sim.disk(),
        trace: trace.clone(),
    };
    let app = App::open(
        Box::new(disk),
        Schema::new().table::<Hit>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(hit_routes());
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    // Every app gets the built-in daily session-expiry sweep's cron entry
    // with no opt-in (`app.rs:184-189`), and `Kernel::build` upserts it via
    // its own solo commit — a WAL append this test's trace would otherwise
    // count as if it were part of the batch below. Baseline AFTER
    // `Kernel::new`, not after `App::open`, so that commit is excluded.
    let before_trace = trace.entries().len();
    let mut source = SimSource::new(Box::new(sim.clock()), SEC);
    source.push_at(0, Request::new(Method::Post, "/hit/1")); // first write to Hit
    source.push_at(0, Request::new(Method::Post, "/hit/2")); // ALSO a first write to Hit
    let responses = source.responses();

    let step = kernel.step(&mut source).unwrap();
    assert!(matches!(step, Step::Worked));
    assert_eq!(
        kernel.stats().requests,
        2,
        "both first-writes must land in ONE batch — the union's second half \
         (`staged_adoptions`) only matters within a single group"
    );
    let statuses: Vec<u16> = responses
        .lock()
        .expect("responses lock poisoned by an earlier panic")
        .iter()
        .map(|(status, _)| *status)
        .collect();
    assert_eq!(statuses, vec![200, 200]);

    let sizes = wal_append_sizes(&trace, before_trace);
    assert_eq!(
        sizes.len(),
        2,
        "two staged records means two WAL appends: {sizes:?}"
    );
    assert!(
        sizes[1] < sizes[0],
        "the SECOND first-write to a table already being adopted in this batch must not carry \
         its own redundant SchemaVersion row — its record should be strictly smaller than the \
         first (which does carry the adoption): {sizes:?}"
    );

    let app = kernel.into_app();
    let snap = app.db().snapshot().unwrap();
    let sv: Option<SchemaVersion> = snap.get(schema_version_id("Hit")).unwrap();
    assert!(sv.is_some(), "Hit must be adopted after its first write");
}

/// T8a Step 5b.1, negative space (task-8-preflight.md C-7d): a commit that
/// fails at append/sync (`applied == 0`) must leave `db.adopted` untouched;
/// one that fails at the checkpoint (`applied == n`) must have ALREADY
/// folded — `Group::commit` folds `staged_adoptions` into `db.adopted`
/// immediately after `submit_all` succeeds, strictly BEFORE the checkpoint
/// call (`db.rs:2221` vs `:2233`). Asserting "any commit failure leaves
/// adopted untouched" is wrong for the checkpoint arm and would provoke a
/// "fix" that breaks correct behaviour.
///
/// **Scope, and the asymmetry between the two arms is REAL, not an
/// oversight.** `db.adopted` is private and `Group` exposes no accessor for
/// it (by design — spec §1 has no group-snapshot escape hatch), so neither
/// arm can read the set directly. What each arm can do differs, and the
/// difference is structural:
///
/// - **Append/sync arm (a): the claim is structurally UNOBSERVABLE.** The
///   WAL itself poisons, so `submit_all`'s `?` returns before the fold loop
///   ever runs (`db.rs:2214-2221`) — `db.adopted` is UNREACHABLE on that
///   path, not merely untouched by luck — and every later `append` refuses
///   before writing a byte (`engine.rs:308-310`). There is no subsequent
///   record to measure, on this `Db` or any other, so the arm is pinned by
///   a source read plus its clean-retry-after-reopen half. A future reader
///   should not read this as a missing assertion: nothing can be emitted to
///   assert on.
/// - **Checkpoint arm (b): the claim IS observable, and is asserted at
///   runtime below.** Only the STORE latches; the WAL stays healthy. So a
///   follow-up `transact` on the SAME live `Db` still appends its record
///   (before failing in apply on the latched store), and that record's size
///   is the same discriminator the headline test above trusts: if the fold
///   had not already run, the follow-up would re-emit the table's adoption
///   row and be strictly LARGER. Observed instead: strictly smaller. This
///   is a direct runtime observation of the fold, not a source-ordering
///   argument (T8a review I-1 — the brief's "same live Db" premise is
///   unbuildable for arm (a) only, and the record owes that correction).
///
/// Both arms additionally recover cleanly: a fresh reopen — which always
/// rebuilds `db.adopted` from the durable `SchemaVersion` rows
/// (`reload_adopted`, `db.rs:1820`) — lands the row exactly once, with no
/// permanent brokenness and no double-adoption.
#[test]
fn adopted_state_survives_a_commit_failure_correctly_per_arm() {
    // (a) append/sync arm: nothing reached the store. `db.adopted` is
    // structurally unreachable on this path (the fold loop never runs);
    // the kernel poisons and stops, and a fresh reopen must cleanly retry
    // and land the adoption row it never durably made the first time.
    {
        let sim = Sim::with_faults(0x60C1_0006, FaultConfig::none());
        let armed = Arc::new(AtomicBool::new(false));
        let disk = OneShotWalFaultDisk {
            inner: sim.disk(),
            armed: armed.clone(),
        };
        let app = App::open(
            Box::new(disk),
            Schema::new().table::<Hit>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .unwrap()
        .mount(hit_routes());
        // Arm PAST `Kernel::new`, not merely past `App::open` — the
        // built-in cron-sweep upsert (see `hit_schema`'s doc) runs its own
        // solo WAL append inside `Kernel::new`, and arming any earlier
        // would fault THAT instead of this test's own write, propagating
        // an unrelated `Err` straight out of `Kernel::new(...).unwrap()`.
        let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
        armed.store(true, Ordering::SeqCst);

        let mut source = SimSource::new(Box::new(sim.clock()), SEC);
        source.push_at(0, Request::new(Method::Post, "/hit/1"));
        let responses = source.responses();
        assert!(
            kernel.step(&mut source).is_err(),
            "the armed append fault must poison the engine and stop the kernel"
        );
        let statuses: Vec<u16> = responses
            .lock()
            .expect("responses lock poisoned by an earlier panic")
            .iter()
            .map(|(status, _)| *status)
            .collect();
        assert_eq!(
            statuses,
            vec![500],
            "the failed write's own request must still be answered: {statuses:?}"
        );

        // Reopen fresh — the fault already fired once and disarmed, and it
        // fails BEFORE forwarding to the real disk, so nothing it touched
        // was ever durably written; this needs no `Sim::restart`, only
        // fresh objects to clear the in-memory poison, exactly as a real
        // process restart would (spec §4's "restart the process to
        // recover from the WAL").
        let app2 = App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Hit>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .unwrap()
        .mount(hit_routes());
        let mut kernel2 = Kernel::new(app2, Box::new(sim.clock())).unwrap();
        let mut source2 = SimSource::new(Box::new(sim.clock()), SEC);
        source2.push_at(0, Request::new(Method::Post, "/hit/1"));
        let responses2 = source2.responses();
        let step2 = kernel2.step(&mut source2).unwrap();
        assert!(matches!(step2, Step::Worked));
        let statuses2: Vec<u16> = responses2
            .lock()
            .expect("responses lock poisoned by an earlier panic")
            .iter()
            .map(|(status, _)| *status)
            .collect();
        assert_eq!(
            statuses2,
            vec![200],
            "the retry, on a fresh engine, must succeed cleanly: {statuses2:?}"
        );

        let app2 = kernel2.into_app();
        let snap = app2.db().snapshot().unwrap();
        let sv: Option<SchemaVersion> = snap.get(schema_version_id("Hit")).unwrap();
        assert!(
            sv.is_some(),
            "Hit must be adopted once the retry actually reaches the store"
        );
    }

    // (b) checkpoint arm: every record reached the store, so the fold
    // (db.rs:2221, unconditional, strictly BEFORE the checkpoint call at
    // db.rs:2233) already ran before this failure was even possible. Only
    // the STORE latches here, so — unlike arm (a) — the fold is observable
    // at runtime on this very `Db`: the probe below writes once more and
    // measures the record. The kernel still stops (the latch poisons), so
    // the clean-recovery half still goes through a fresh reopen.
    {
        let sim = Sim::with_faults(0x60C1_0006, FaultConfig::none());
        let cfg = checkpoint_config();
        let db_cfg = cfg.db.clone();
        let trace = DiskTrace::default();
        let armed = Arc::new(AtomicBool::new(false));
        let fired = Arc::new(AtomicU64::new(0));
        let disk = FaultyTracingDisk {
            inner: sim.disk(),
            trace: trace.clone(),
            store_path: db_cfg.store_path.clone(),
            armed: armed.clone(),
            fired: fired.clone(),
        };
        let app = App::open(
            Box::new(disk),
            Schema::new().table::<Hit>(),
            Box::new(sim.entropy()),
            cfg,
        )
        .unwrap()
        .mount(hit_routes());
        // Arm PAST `Kernel::new`, not merely past `App::open` — same
        // reason as part (a) and Step 3's checkpoint arm: the built-in
        // cron-sweep upsert runs its own checkpoint under this test's
        // `checkpoint_every: 1`, and arming any earlier would fault that
        // instead, latching the store before this test's own write ever
        // runs.
        let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
        armed.store(true, Ordering::SeqCst);
        // Baseline AFTER `Kernel::new` for the same reason the headline
        // test does it: the built-in cron-sweep upsert's own solo append
        // would otherwise be counted as this batch's record.
        let before_trace = trace.entries().len();

        let mut source = SimSource::new(Box::new(sim.clock()), SEC);
        source.push_at(0, Request::new(Method::Post, "/hit/1"));
        let responses = source.responses();
        let step = kernel.step(&mut source);
        assert!(step.is_err(), "the checkpoint's own fsync must fail");
        assert_eq!(fired.load(Ordering::SeqCst), 1);
        let statuses: Vec<u16> = responses
            .lock()
            .expect("responses lock poisoned by an earlier panic")
            .iter()
            .map(|(status, _)| *status)
            .collect();
        assert_eq!(
            statuses,
            vec![200],
            "the record is durable — the checkpoint failure is bookkeeping after the fact: \
             {statuses:?}"
        );

        // The fold, observed at RUNTIME on the same live `Db` (T8a review
        // I-1). The store is latched but the WAL is not, so this follow-up
        // transaction still APPENDS its record before failing in apply —
        // and that record is the measurement. `Hit` was adopted by the
        // batch above, so a fold that ran means this record carries the Hit
        // write alone; a fold that had NOT run would re-emit the table's
        // adoption row here, making the record strictly LARGER.
        let batch = wal_append_sizes(&trace, before_trace);
        assert_eq!(
            batch.len(),
            1,
            "one request in the batch means one WAL append: {batch:?}"
        );
        let mut app = kernel.into_app();
        let before_probe = trace.entries().len();
        let probe = app.db_for_tests().transact(|tx| {
            // A fresh id, so this is unambiguously a new row and cannot
            // produce an empty diff (and so no record) by writing back what
            // was already there.
            tx.put(&Hit {
                id: Id::from_raw(9),
                count: 1,
            });
            Ok::<(), std::io::Error>(())
        });
        assert!(
            probe.is_err(),
            "the store is latched, so the probe's APPLY must fail — its append, which happens \
             first, is what this measures"
        );
        let probe_appends = wal_append_sizes(&trace, before_probe);
        assert_eq!(
            probe_appends.len(),
            1,
            "the probe appends before it fails: {probe_appends:?}"
        );
        assert!(
            probe_appends[0] < batch[0],
            "`db.adopted` must ALREADY carry Hit: the fold runs strictly after `submit_all` \
             succeeds and strictly before the checkpoint call that failed (db.rs:2221 vs \
             :2233). A follow-up write that re-emitted the adoption row would be strictly \
             LARGER than the batch's own record, not smaller: probe {probe_appends:?} vs batch \
             {batch:?}"
        );

        // Reopen fresh: no fault injector needed any more (it already
        // fired once, and the store handle it latched is being replaced
        // outright), and confirm the follow-up write to the same
        // already-adopted table lands cleanly.
        let app2 = App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Hit>(),
            Box::new(sim.entropy()),
            checkpoint_config(),
        )
        .unwrap()
        .mount(hit_routes());
        let mut kernel2 = Kernel::new(app2, Box::new(sim.clock())).unwrap();
        let mut source2 = SimSource::new(Box::new(sim.clock()), SEC);
        source2.push_at(0, Request::new(Method::Post, "/hit/2"));
        let responses2 = source2.responses();
        let step2 = kernel2.step(&mut source2).unwrap();
        assert!(matches!(step2, Step::Worked));
        let statuses2: Vec<u16> = responses2
            .lock()
            .expect("responses lock poisoned by an earlier panic")
            .iter()
            .map(|(status, _)| *status)
            .collect();
        assert_eq!(
            statuses2,
            vec![200],
            "the follow-up write must land cleanly: {statuses2:?}"
        );

        let app2 = kernel2.into_app();
        let snap = app2.db().snapshot().unwrap();
        let sv: Option<SchemaVersion> = snap.get(schema_version_id("Hit")).unwrap();
        assert!(sv.is_some(), "Hit must still be adopted");
        assert_eq!(
            snap.scan::<Hit>().unwrap().len(),
            3,
            "the checkpoint-arm write (id=1), the probe's row (id=9) and the post-reopen \
             follow-up (id=2) must all be present. The probe counts BECAUSE its append reached \
             the WAL and synced before its apply hit the latched store — durable but \
             unacknowledged, which is exactly spec §4's false-negative contract, and \
             independent corroboration that the append the size assertion measures really \
             happened"
        );
    }
}

// --------------------------------- Step 5b.2: enqueues/emits invisible mid-batch

#[event]
struct Tick {
    n: WireU64,
}

#[job]
struct QuietJob {}

impl QuietJob {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

#[route(POST "/enqueue", public)]
fn enqueue_quiet(tx: &mut Tx) -> Result<(), ApiError> {
    tx.enqueue(QuietJob {});
    Ok(())
}

#[route(GET "/job-count", public)]
fn job_count(tx: &mut Tx) -> Result<WireU64, ApiError> {
    Ok(WireU64(tx.scan::<JobRow>()?.len() as u64))
}

#[route(POST "/emit-tick", public)]
fn emit_tick(tx: &mut Tx) -> Result<(), ApiError> {
    tx.emit(Tick { n: WireU64(1) });
    Ok(())
}

#[route(GET "/event-count", public)]
fn event_count(tx: &mut Tx) -> Result<WireU64, ApiError> {
    Ok(WireU64(tx.scan::<EventRow>()?.len() as u64))
}

/// T8a Step 5b.2 (task-8-preflight.md C-12): the group overlay carries
/// ROWS only — enqueues, emits and migrate_puts are not in it — so a
/// transaction batched behind a sibling's enqueue/emit does not see it,
/// where pre-batching it would have (each transaction applied before the
/// next took its snapshot). `JobRow`/`EventRow` materialize at APPLY, from
/// the log record's enqueue/emit sections, not from anything
/// `Group::stage` merges into its read overlay.
///
/// C-12's correction: the T4-review citation this step originally leaned
/// on ("every JobRow/EventRow read in production goes through a Snapshot,
/// never a Tx") is STALE as of T7 — `Dispatcher::run_due_into` reads
/// `JobRow` through `tx` at `dispatch.rs:179`/`:268`. The conclusion still
/// holds (those reads touch pre-existing rows; a sibling's fresh enqueue is
/// not due until `drain_enqueued` runs after the arm, `kernel/mod.rs:602`),
/// so this property is real and this test — not that stale citation — is
/// what proves it.
#[test]
fn enqueues_and_emits_are_invisible_to_a_later_sibling_in_the_same_batch() {
    let sim = Sim::with_faults(0x60C1_0007, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![enqueue_quiet, job_count, emit_tick, event_count])
    .mount_jobs(jobs![QuietJob]);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let mut source = SimSource::new(Box::new(sim.clock()), SEC);
    source.push_at(0, Request::new(Method::Post, "/enqueue"));
    source.push_at(0, Request::new(Method::Get, "/job-count"));
    source.push_at(0, Request::new(Method::Post, "/emit-tick"));
    source.push_at(0, Request::new(Method::Get, "/event-count"));
    let responses = source.responses();

    let step = kernel.step(&mut source).unwrap();
    assert!(matches!(step, Step::Worked));
    assert_eq!(
        kernel.stats().requests,
        4,
        "all four must land in ONE batch — the invisibility this test pins only exists across \
         a batch; pre-batching, each transaction applied before the next ran"
    );

    let bodies: Vec<(u16, String)> = responses
        .lock()
        .expect("responses lock poisoned by an earlier panic")
        .iter()
        .map(|(status, body)| (*status, String::from_utf8_lossy(body).to_string()))
        .collect();
    assert_eq!(bodies[0].0, 200, "the enqueue itself: {bodies:?}");
    assert_eq!(
        bodies[1],
        // job_count/event_count now return WireU64 (task 5): a 64-bit
        // integer is ALWAYS string-encoded on the wire, so the body is the
        // JSON string `"0"`, not the bare number `0`.
        (200, "\"0\"".to_string()),
        "a sibling's enqueue, staged earlier in this same batch, must be INVISIBLE to a later \
         transaction's tx: {bodies:?}"
    );
    assert_eq!(bodies[2].0, 200, "the emit itself: {bodies:?}");
    assert_eq!(
        bodies[3],
        (200, "\"0\"".to_string()),
        "a sibling's emit, staged earlier in this same batch, must be INVISIBLE the same way: \
         {bodies:?}"
    );

    let app = kernel.into_app();
    let snap = app.db().snapshot().unwrap();
    assert_eq!(
        snap.scan::<JobRow>().unwrap().len(),
        1,
        "the job DID materialize at apply, once the batch committed"
    );
    assert_eq!(
        snap.scan::<EventRow>().unwrap().len(),
        1,
        "the event DID materialize at apply, once the batch committed"
    );
}

// ======================================================================
// T8b — the contended-request burst swarm workload (RULING 2,
// task-8b-brief.md / task-8-preflight.md C-8). The fairness invariant
// (RULING 3) lives in `tests/kernel.rs`, beside
// `due_jobs_are_not_starved_by_a_request_flood`. `kernel_workload`
// (`tests/kernel.rs:1374`) is untouched — RULING 2's option (b), making it
// sometimes burst, was explicitly rejected: it would perturb the frozen
// pinned seed `0x348270b38f166529`'s fault stream.
// ======================================================================

use charpente_core::io::Entropy;
use charpente_sim::checks::{durable_corruption_fired, is_fault_fallout};
use charpente_sim::run_universes_swarm_with;

/// Drives `source` to completion via `Kernel::step`, not `run` — the
/// per-step `stats().requests` delta is the only way to prove a SINGLE step
/// drained more than one request (`stats.requests += drained` runs once per
/// request-arm service, `kernel/mod.rs:411`), which is what "a genuinely
/// contended batch" means. `sim.cover("request:contended")` fires exactly
/// there, which is `contended_request_bursts_survive_the_fault_matrix`'s
/// `require:` gate below — a swarm that only ever produced batches of one
/// would satisfy every other assertion in this workload while covering
/// nothing (RULING 2's own words), and this is what forecloses that.
///
/// Deliberately skips `Kernel::run`'s clean-shutdown checkpoint: `Kernel`
/// exposes no `app_mut`/`db_mut` accessor outside the crate to call it from
/// here. The next round's reopen replays the WAL either way (recovery
/// correctness is unaffected), so only the store's compaction is deferred —
/// harmless for a test that reopens every round regardless.
///
/// **The consequence is stronger than "deferred", and worth stating so the
/// coverage is not read wider than it is** (T8b review M-2): combined with
/// `DbConfig::checkpoint_every == 4096` and a workload writing at most 36
/// records per universe, `Db::checkpoint()` never executes here AT ALL.
/// So `Group::commit`'s checkpoint arm — the `CommitFailure { applied: n }`
/// that invalidates nobody — gets ZERO coverage from this swarm. That half
/// of the ruled `invalidates` rule stays pinned deterministically by 8a's
/// Step 3 (`a_failed_batchs_participants_are_invalidated_by_what_they_saw`)
/// and 5b.1, which is where most of 8a's effort went. Giving this workload
/// a small `checkpoint_every` would buy the coverage but move the fault
/// schedule, which is a T9-adjacent decision, not this test's to make.
fn drive_burst(kernel: &mut Kernel, source: &mut SimSource, sim: &Sim) -> io::Result<()> {
    loop {
        let before = kernel.stats().requests;
        match kernel.step(source)? {
            Step::Worked => {
                if kernel.stats().requests - before >= 2 {
                    sim.cover("request:contended");
                }
            }
            Step::Idle { deadline } => {
                if let Wake::Shutdown = source.wait_until(deadline) {
                    return Ok(());
                }
            }
        }
    }
}

/// T8b RULING 2 (C-8): no swarm workload anywhere in the repo ever produced
/// a contended REQUEST batch before this — `kernel_workload` spaces arrivals
/// one virtual second apart, and `http_workload`/`tx_workload` never build a
/// `Kernel` at all. So spec §9's "every existing flagship property must hold
/// unchanged with batching live" was, for the request arm, a claim about
/// code no swarm universe ever executed; batching's headline change had
/// ZERO fault-matrix coverage. This workload closes that: every round pushes
/// a burst of AT LEAST two same-instant writes to the SAME row under the
/// full fault matrix.
///
/// The oracle is the `acked_implies_durable` shape (`checks.rs`), applied
/// inline rather than by calling that function directly — it constructs its
/// OWN `Sim::new(seed)` internally, which cannot be reconciled with
/// `run_universes_swarm_with!`'s already-constructed, already-fault-
/// configured `&mut Sim` (see the report). `/hit/:id`'s count is monotonic
/// under a single writer route, so the running MAXIMUM count any 200 ever
/// reported is a durability watermark: Step 3's `invalidates` rule (a writer
/// at or past a failed commit's `applied` cutoff gets its 500, never a 200)
/// means every 200 this workload ever sees was for a genuinely durable
/// write, so whatever the highest such count promised must survive every
/// crash and reopen from then on — "no acked write is lost across
/// crash/restart", and "acked requests are durable", the other two halves
/// of RULING 2's stated invariant. The batch-of-1 identity (the invariant's
/// third half) is NOT exercised here, and the earlier claim that it was is
/// disproven by this workload's own behaviour (T8b review M-3): every burst
/// is 2..=9 requests pushed at one instant and `MAX_BATCH` is 64, so the
/// drain always takes them all. Measured over the shipped 256 universes —
/// 408 request-arm services, sizes 2 through 9, **none of size 1**. The
/// batch-of-1 case is pinned deterministically instead, by
/// `a_never_contended_universe_traces_identically_to_the_pre_batching_fixture`.
/// What one `verify_watermark` call actually established. `Checked` is the
/// only outcome that asserted anything; the other two exist so the caller
/// can tell "the oracle ran and held" from "the oracle never got to run",
/// which the coverage gate below turns into a batch-level requirement.
enum Verify {
    Checked,
    Excused,
    Unreachable,
}

/// Reopens (retrying past injected faults, exactly as `kernel_workload`'s
/// final verification does), reads the durable count, and compares it
/// against everything a 200 has promised SO FAR.
///
/// **Called at every round boundary, not only at the end, and that cadence
/// is the load-bearing part** (T8b review I-2). `watermark` is a running
/// max while the durable count is re-read fresh: check only once at the end
/// and an early round's loss is arithmetically ERASED by any later round
/// that writes the count back above it. That is not hypothetical —
/// measured on stock code before this was hoisted, one universe lost SEVEN
/// acked writes at a round boundary and the end-only oracle reported green
/// without even reaching the corruption excuse. All three references this
/// workload is modelled on verify per round: `checks::acked_implies_durable`
/// (`checks.rs:170-191`), `tx_workload` (`transactions.rs:519-560`) and
/// `kernel_workload` (`kernel.rs:1393`, which also keeps a final coda, as
/// this workload does).
fn verify_watermark(sim: &Sim, watermark: u64) -> Result<Verify, String> {
    for _attempt in 0..8 {
        match App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Hit>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        ) {
            Ok(mut app) => {
                // The checker's own reads are disk ops too — same
                // classify-and-reopen treatment as `kernel_workload`'s
                // `exactly_once_holds` call needs (found by seed
                // 0x348270b38f166529): an armed crash countdown can trip
                // right here.
                let snap = match app.db_for_tests().snapshot() {
                    Ok(s) => s,
                    Err(e) if is_fault_fallout(&e.to_string()) => {
                        sim.restart();
                        continue;
                    }
                    Err(e) => return Err(e.to_string()),
                };
                let durable = match snap.scan::<Hit>() {
                    Ok(rows) => rows.iter().find(|h| h.id.raw() == 1).map_or(0, |h| h.count),
                    Err(e) if is_fault_fallout(&e.to_string()) => {
                        sim.restart();
                        continue;
                    }
                    Err(e) => return Err(e.to_string()),
                };
                // Covered only when the comparison is NON-TRIVIAL. A
                // universe that never acked anything has `watermark == 0`,
                // and `0 < 0` is false — it passes this check without
                // comparing anything, which is 50 of 256 universes as
                // shipped (T8b review I-3). Keying the gate on `> 0` is
                // what makes the `require:` below mean "the oracle really
                // ran somewhere", not "the code path was entered".
                if watermark > 0 {
                    sim.cover("oracle:verified");
                }
                if durable < watermark {
                    // Excused ONLY when the harness actually injected
                    // durable corruption in this universe (`tx_workload`'s
                    // identical idiom, `transactions.rs:543-553`), and it
                    // keys on FIRE-TIME coverage, not on the sampled
                    // config, so a universe that merely had the axis
                    // configured is not excused. `Db::checkpoint` never
                    // runs in this workload (`checkpoint_every: 4096`, and
                    // `drive_burst` skips `Kernel::run`'s shutdown
                    // checkpoint), but `Db::open`'s `persist_store_at_open`
                    // (`db.rs:1399`) advances the durable `applied_seq` —
                    // the recovery horizon — on every replaying open, and
                    // this workload reopens every round. So only the
                    // CURRENT round's uncheckpointed tail sits in the
                    // silent-cut window M1 sub-project 1 documented;
                    // corruption below the horizon surfaces as the loud,
                    // named `DurableHorizon` hard error instead, which
                    // `is_fault_fallout` already accepts and which fires in
                    // 3 of 256 universes (measured — T8b review M-1
                    // disproved the "every record is above the horizon"
                    // reasoning this comment used to carry).
                    if durable_corruption_fired(sim) {
                        return Ok(Verify::Excused);
                    }
                    return Err(format!(
                        "acked-implies-durable violated: a 200 promised count {watermark} \
                         but the durable count is only {durable}"
                    ));
                }
                return Ok(Verify::Checked);
            }
            Err(e) if is_fault_fallout(&e.to_string()) => sim.restart(),
            Err(e) => return Err(format!("verification open failed non-injected: {e}")),
        }
    }
    Ok(Verify::Unreachable)
}

fn burst_workload(sim: &mut Sim) -> Result<(), String> {
    let entropy = sim.entropy();
    let mut watermark: u64 = 0;

    for _round in 0..4 {
        sim.crash_disk_after(64 + entropy.next_u64() % 768);
        let app = match App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Hit>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        ) {
            Ok(app) => app.mount(hit_routes()),
            Err(e) => {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("open failed non-injected: {e}"));
                }
                sim.restart();
                continue;
            }
        };

        let now = sim.now().as_nanos();
        let mut source = SimSource::new(Box::new(sim.clock()), now + 5 * SEC);
        // Always at least 2, so every round is a genuine burst by
        // construction — coverage (above) decides how many universes
        // actually land it in ONE step, not the workload shape.
        let burst = 2 + entropy.next_u64() % 8;
        for _ in 0..burst {
            source.push_at(now, Request::new(Method::Post, "/hit/1"));
        }
        let responses = source.responses();

        let mut kernel = match Kernel::new(app, Box::new(sim.clock())) {
            Ok(k) => k,
            Err(e) => {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("Kernel::new failed non-injected: {e}"));
                }
                sim.restart();
                continue;
            }
        };

        let run = drive_burst(&mut kernel, &mut source, sim);

        // Harvest every 200's reported count regardless of how the round
        // ended: a mid-batch fault legally acks a PREFIX of the batch
        // (`invalidates`), and those acks are exactly what the watermark
        // must hold onto.
        for (status, body) in responses
            .lock()
            .expect("responses lock poisoned by an earlier panic")
            .iter()
        {
            if *status == 200 {
                // `hit` returns `WireU64` (task 5): a 64-bit integer is
                // always string-encoded on the wire, so the body is the
                // JSON string `"N"`, not the bare number `N`.
                let count: u64 = String::from_utf8_lossy(body)
                    .trim()
                    .trim_matches('"')
                    .parse()
                    .unwrap_or_else(|e| panic!("a /hit/:id 200 body must be a wire u64: {e}"));
                if watermark == 0 && count > 0 {
                    // 80 of 256 universes as shipped never acked ANY
                    // request, so their oracle compared 0 against 0 (T8b
                    // review I-3). Requiring this key batch-wide means a
                    // change that 500'd every request could no longer leave
                    // the property green while asserting nothing.
                    sim.cover("request:acked");
                }
                watermark = watermark.max(count);
            }
        }

        if let Err(e) = run
            && !is_fault_fallout(&e.to_string())
        {
            return Err(format!("kernel stopped non-injected: {e}"));
        }
        sim.restart();

        // The round-boundary check. Without it a loss here is erased by any
        // later round that pushes the count back above the watermark — see
        // `verify_watermark`'s doc for the measurement.
        if let Verify::Excused = verify_watermark(sim, watermark)? {
            return Ok(());
        }
    }

    // The final coda, kept alongside the per-round checks exactly as
    // `kernel_workload` keeps its own (`kernel.rs:1393` and again at the
    // end): the last round's writes have no round boundary after them.
    verify_watermark(sim, watermark)?;
    Ok(())
}

/// The flagship request-arm batching property under the full fault matrix.
/// `0x60C1_0009` (`docs/superpowers/plans/2026-08-10-group-commit.md`'s
/// seed table): the next free `0x60C1_*` slot after 8a's seven and the
/// fairness test's reserved `0x60C1_0008`. Uses `run_universes_swarm_with!`
/// with a no-op `adjust` rather than the ambient `run_universes_swarm!` (the
/// shape `kernel_jobs_are_exactly_once_under_the_fault_matrix` uses) so this
/// permanent, always-in-the-suite property gets a pinned, reproducible base
/// seed like the newer flagship properties elsewhere in this sub-project
/// (`auth.rs`'s `raw_credentials_never_touch_the_durable_store`,
/// `replication.rs`'s several) — a judgment call, not a repeat of an
/// established pattern in THIS file, since nothing here used
/// `run_universes_swarm_with!` before; see the report. The census settles
/// it: 18 pinned sites against 5 ambient ones, all five M0/early-M1.
///
/// The cost of a pinned base, stated so nobody mistakes it for a wider
/// claim: this property runs the SAME 256 universes forever, so its fault
/// coverage is frozen at what was measured, against a repo whose evidence
/// bar is otherwise "green over N fresh seeds". The T8b review checked that
/// the pin is not hiding anything by running the property from three
/// unrelated bases — `0x1111222233334444`, `0xdeadbeef00000001` and
/// `0x0badc0de5eed0007` — all green.
///
/// The `require:` set is the anti-vacuity contract, and two of the four
/// keys exist because the first version could have been green while
/// asserting nothing. `request:contended` proves batching actually
/// happened; `request:acked` proves some universe got a 200 at all; and
/// `oracle:verified` proves the durability comparison ran somewhere with a
/// non-zero watermark. Without the last two, a change that 500'd every
/// request — or that made every verification reopen fail — would satisfy
/// every remaining key and pass 256 universes without comparing anything
/// (T8b review I-3).
#[test]
fn contended_request_bursts_survive_the_fault_matrix() {
    run_universes_swarm_with!(
        0x60C1_0009,
        256,
        |_cfg: &mut FaultConfig| {},
        burst_workload,
        require: [
            "request:contended",
            "request:acked",
            "oracle:verified",
            "fault:crash",
            "crash:torn",
            "restart"
        ]
    );
}
