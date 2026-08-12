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
use charpente::kernel::{Kernel, SimSource};
use charpente::{Schema, Tx, route, routes, table};
use charpente_core::io::{Disk, File};
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

fn hit_routes() -> Vec<charpente::http::Route> {
    routes![hit]
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
