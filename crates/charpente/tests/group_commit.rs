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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use charpente::http::{ApiError, App, AppConfig, Handled, Method, Request};
use charpente::kernel::{
    EventSink, JobError, Kernel, Reply, RequestSource, Respond, SimSource, Step, Wake,
};
use charpente::{Schema, Tx, job, jobs, route, routes, table};
use charpente_core::io::{Disk, File, Instant};
use charpente_core::table::{Id, View};
use charpente_sim::{FaultConfig, Sim};

const SEC: u64 = 1_000_000_000;

// ------------------------------------------------------------------ routes

#[table]
struct Hit {
    id: Id<Hit>,
    count: u64,
}

/// A minimal write route: reads the current count, writes it back
/// incremented. One `stage()` call against this always produces exactly one
/// record (the row always changes), which is what makes `g.len() == 1` a
/// meaningful assertion below rather than an accident of an empty diff.
#[route(POST "/hit/:id")]
fn hit(tx: &mut Tx, id: Id<Hit>) -> Result<u64, ApiError> {
    let count = tx.get(id)?.map_or(0, |h| h.count) + 1;
    tx.put(&Hit { id, count });
    Ok(count)
}

/// A read route, so a batch can contain one. Mounting it is pure in-memory
/// router work — it touches no disk, so the committed pre-batching fixture is
/// unaffected (and `a_never_contended_universe_traces_identically_...` would
/// say so immediately if that were wrong).
#[route(GET "/count/:id")]
fn count(tx: &mut Tx, id: Id<Hit>) -> Result<u64, ApiError> {
    Ok(tx.get(id)?.map_or(0, |h| h.count))
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
