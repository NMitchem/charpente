//! Kernel-layer tests (PLAN M0 step 6): #[job] surface, dispatcher, cron,
//! the event loop, and the step-6 swarm properties.

use charpente::http::{App, AppConfig, Session};
use charpente::kernel::{Dispatcher, JobDef, JobEntry, JobError, JobOutcome};
use charpente::{Db, DbConfig, Schema, Tx, job, jobs, table};
use charpente_core::db::{Job, TxError};
use charpente_core::io::Entropy;
use charpente_core::job::{JobRow, JobStatus};
use charpente_core::migration::{MigrationError, TableMigration};
use charpente_core::table::{Codec, Id, IndexDef, Row, RowShape, View};
// This workload's own sequential reads (`exactly_once_holds`'s snapshot/scan
// calls) can still hit a freshly armed crash mid-check, so all four
// `checks::classify` families stay reachable here — unlike
// transactions.rs/http.rs, which break-and-restart on the first Io error and
// so use a narrower local wrapper. This is the identical-4-family case: the
// shared classifier is used directly, no local wrapper needed.
use charpente_sim::checks::is_fault_fallout;
use charpente_sim::{FaultConfig, Sim};

#[table]
struct Counter {
    id: Id<Counter>,
    hits: u64,
}

#[job(retry(max = 3, backoff = "10s"))]
struct CountUp {
    slot: u64,
}

impl CountUp {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        let hits = tx
            .get(Id::<Counter>::from_raw(self.slot))?
            .map_or(0, |c| c.hits);
        tx.put(&Counter {
            id: Id::from_raw(self.slot),
            hits: hits + 1,
        });
        Ok(())
    }
}

#[test]
fn job_macro_derives_codec_name_retry_and_glue() {
    assert_eq!(CountUp::NAME, "CountUp");
    assert_eq!(CountUp::RETRY.max, 3);
    assert_eq!(CountUp::RETRY.backoff, std::time::Duration::from_secs(10));

    let defs = jobs![CountUp];
    assert_eq!(defs.len(), 1);
    assert_eq!(defs[0].name, "CountUp");

    // The glue decodes a payload and runs the handler inside a transaction.
    let sim = Sim::with_faults(0x610, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap();
    let def: JobDef = <CountUp as JobEntry>::def();
    let mut payload = Vec::new();
    charpente_core::table::Codec::encode(&CountUp { slot: 4 }, &mut payload);
    app.db_for_tests()
        .transact(|tx| (def.glue)(tx, &payload))
        .unwrap();
    let counters = app
        .db_for_tests()
        .snapshot()
        .unwrap()
        .scan::<Counter>()
        .unwrap();
    assert_eq!((counters[0].id.raw(), counters[0].hits), (4, 1));
}

#[job(retry(max = 2, backoff = "10s"))]
struct AlwaysFails {}

impl AlwaysFails {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Err(JobError::app("the widget was upside down"))
    }
}

#[job(retry(max = 2, backoff = "10s"))]
struct Panics {}

impl Panics {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        panic!("handler bug: unreachable state");
    }
}

fn quiet_kernel_app(seed: u64) -> (Sim, App) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![CountUp, AlwaysFails, Panics, ReadFaults]);
    (sim, app)
}

#[test]
fn dispatcher_runs_due_job_with_atomic_done_mark() {
    let (_sim, mut app) = quiet_kernel_app(0x620);
    let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
    let db = app.db_for_tests();
    db.transact(|tx| {
        tx.enqueue(CountUp { slot: 1 });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    dispatcher.note_enqueued(db.drain_enqueued());
    assert!(dispatcher.has_due(0));

    let outcome = dispatcher.run_due(db, 0).unwrap();
    assert!(matches!(outcome, JobOutcome::Done { id: 0 }));
    let snap = db.snapshot().unwrap();
    assert_eq!(snap.scan::<Counter>().unwrap()[0].hits, 1);
    assert_eq!(snap.scan::<JobRow>().unwrap()[0].status, JobStatus::Done);
    assert!(!dispatcher.has_due(u64::MAX), "done jobs leave the heap");
}

#[test]
fn failing_job_follows_backoff_then_dies_with_the_story() {
    let (_sim, mut app) = quiet_kernel_app(0x621);
    let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
    let db = app.db_for_tests();
    db.transact(|tx| {
        tx.enqueue(AlwaysFails {});
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    dispatcher.note_enqueued(db.drain_enqueued());

    let now0 = 1_000;
    let JobOutcome::Retried {
        attempts, due_at, ..
    } = dispatcher.run_due(db, now0).unwrap()
    else {
        panic!("first failure retries");
    };
    assert_eq!(attempts, 1);
    assert_eq!(due_at, now0 + 10_000_000_000); // backoff 10s * 2^0
    assert!(
        !dispatcher.has_due(now0),
        "not due again until backoff passes"
    );
    assert!(dispatcher.has_due(due_at));

    let JobOutcome::Dead { attempts, .. } = dispatcher.run_due(db, due_at).unwrap() else {
        panic!("attempts == max means dead");
    };
    assert_eq!(attempts, 2);
    let row = &db.snapshot().unwrap().scan::<JobRow>().unwrap()[0];
    assert_eq!(row.status, JobStatus::Dead);
    assert!(
        row.last_error.as_deref().unwrap().contains("upside down"),
        "the dead-letter record carries the last error: {:?}",
        row.last_error
    );
    // Handler effects from failed attempts never committed.
    assert!(db.snapshot().unwrap().scan::<Counter>().unwrap().is_empty());
}

/// The counterpart of the test below, and the asymmetry is the point: a JOB
/// handler's panic is absorbed into the retry path (`dispatch.rs`'s
/// `catch_unwind` inside the transaction closure), a ROUTE handler's is not.
/// There is no `catch_unwind` on the request path, so the panic unwinds out
/// of `Kernel::step`/`run` and the node stops.
///
/// This is a CHARACTERIZATION test, not an endorsement — spec §4's amendment
/// (2) records it as a known limitation. It predates group commit, which only
/// widened the blast radius from one request to a whole batch. If route
/// isolation is ever built (its own design change, needing its own ruling),
/// update and rename this test rather than deleting it — the repo precedent
/// is `kernel_keeps_serving_with_a_latched_store_known_gap` becoming
/// `kernel_stops_on_a_latched_store`.
///
/// `Group::Drop`'s `std::thread::panicking()` guard is what keeps this an
/// unwind rather than an abort: the batch's staged records are still in the
/// group when it drops during the unwind. If that guard regresses, this test
/// fails by SIGABRT instead of catching a panic, which is the loud failure
/// the guard exists to prevent.
#[test]
fn a_panicking_route_handler_stops_the_node_it_is_not_isolated() {
    let sim = Sim::with_faults(0x0DED_0003, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        crash_config(),
    )
    .unwrap()
    .mount(routes![boom]);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let (mut source, replies) = scripted_source(vec![Request::new(Method::Post, "/boom")]);

    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = kernel.run(&mut source);
    }))
    .expect_err(
        "a route handler's panic must unwind out of Kernel::run — if this now \
         returns normally, request-path isolation was added and spec §4's \
         amendment (2) plus §9 property 6 are stale",
    );
    // Assert it is THE handler's panic that escaped, not some incidental one:
    // `is_err()` alone would be satisfied by a `Group::Drop` contract panic or
    // any other unwind, and would then pass while the property it names is
    // false.
    let text = payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_default();
    assert!(
        text.contains("route handler exploded"),
        "the escaping panic must be the handler's own, not a destructor's: {text:?}"
    );
    // The request is never answered by the kernel. In production `serve.rs`
    // turns the dropped oneshot into a generic 500; here the reply is simply
    // dropped, which is what that 500 is made of.
    assert!(
        replies.lock().unwrap().is_empty(),
        "the panicking request gets no reply from the kernel itself"
    );
}

#[route(POST "/boom")]
fn boom(tx: &mut Tx) -> Result<(), ApiError> {
    let _ = tx;
    panic!("route handler exploded");
}

#[test]
fn panicking_handler_is_absorbed_into_the_retry_path() {
    let (_sim, mut app) = quiet_kernel_app(0x622);
    let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
    let db = app.db_for_tests();
    db.transact(|tx| {
        tx.enqueue(Panics {});
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    dispatcher.note_enqueued(db.drain_enqueued());
    let JobOutcome::Retried { .. } = dispatcher.run_due(db, 0).unwrap() else {
        panic!("a panic is a failure, not a crash");
    };
    let row = &db.snapshot().unwrap().scan::<JobRow>().unwrap()[0];
    assert!(
        row.last_error
            .as_deref()
            .unwrap()
            .contains("unreachable state"),
        "{:?}",
        row.last_error
    );
}

#[job]
struct ReadFaults {}

impl ReadFaults {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        // Stands in for a platform read fault mid-handler (real injection
        // arrives with the latent-read-error fault-matrix extension).
        Err(JobError::Io(std::io::Error::other(
            "simulated read fault (injected)",
        )))
    }
}

#[test]
fn platform_io_during_a_handler_is_not_an_attempt() {
    let (_sim, mut app) = quiet_kernel_app(0x625);
    let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
    let db = app.db_for_tests();
    db.transact(|tx| {
        tx.enqueue(ReadFaults {});
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    dispatcher.note_enqueued(db.drain_enqueued());

    let err = dispatcher.run_due(db, 0).unwrap_err();
    assert!(err.to_string().contains("simulated read fault"), "{err}");
    let row = &db.snapshot().unwrap().scan::<JobRow>().unwrap()[0];
    assert_eq!(
        row.status,
        JobStatus::Pending,
        "an IO fault is not an attempt"
    );
    assert_eq!(row.attempts, 0, "retry budget untouched by platform faults");
    assert!(row.last_error.is_none(), "nothing was recorded");
    assert!(
        dispatcher.has_due(0),
        "the job goes straight back on the heap"
    );
}

#[test]
fn unregistered_job_name_fails_with_the_fix() {
    let (_sim, mut app) = quiet_kernel_app(0x623);
    let mut dispatcher = Dispatcher::new(vec![]); // nothing registered
    let db = app.db_for_tests();
    db.transact(|tx| {
        tx.enqueue(CountUp { slot: 0 });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    dispatcher.note_enqueued(db.drain_enqueued());
    dispatcher.run_due(db, 0).unwrap();
    let row = &db.snapshot().unwrap().scan::<JobRow>().unwrap()[0];
    let msg = row.last_error.as_deref().unwrap();
    assert!(
        msg.contains("CountUp") && msg.contains("mount_jobs"),
        "{msg}"
    );
}

use charpente::kernel::CronScheduler;
use std::time::Duration;

const HOUR: u64 = 3_600_000_000_000;
const DAY: u64 = 24 * HOUR;

fn cron_app(seed: u64) -> (Sim, App) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![CountUp])
    .schedule("hourly-count", Duration::from_secs(3600), || CountUp {
        slot: 0,
    });
    (sim, app)
}

#[test]
fn cron_upsert_ticks_and_catches_up_once() {
    let (_sim, mut app) = cron_app(0x630);
    let mut cron = CronScheduler::new(app.take_cron_entries_for_tests());
    let db = app.db_for_tests();

    cron.upsert(db, 0).unwrap();
    assert_eq!(cron.next_deadline(), Some(HOUR));
    assert!(!cron.has_due(HOUR - 1) && cron.has_due(HOUR));

    // A tick advances the schedule and enqueues through the normal path.
    let (name, catchup) = cron.tick(db, HOUR).unwrap();
    assert_eq!((name, catchup), ("hourly-count", false));
    assert_eq!(db.drain_enqueued().len(), 1);
    assert_eq!(cron.next_deadline(), Some(2 * HOUR));

    // Downtime: three intervals pass unserved. Catch-up fires ONCE and the
    // schedule jumps past now — no backfill.
    let much_later = 5 * HOUR + 7;
    assert!(cron.has_due(much_later));
    let (_, catchup) = cron.tick(db, much_later).unwrap();
    assert!(
        catchup,
        "a tick past a whole missed interval reports catch-up"
    );
    assert_eq!(db.drain_enqueued().len(), 1, "one catch-up tick, not three");
    assert_eq!(cron.next_deadline(), Some(much_later + HOUR));
    assert!(!cron.has_due(much_later));
}

#[test]
fn cron_schedule_survives_reopen_without_resetting_phase() {
    let sim = Sim::with_faults(0x631, FaultConfig::none());
    let open = |sim: &Sim| {
        App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Counter>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .unwrap()
        .mount_jobs(jobs![CountUp])
        .schedule("hourly-count", Duration::from_secs(3600), || CountUp {
            slot: 0,
        })
    };
    let mut app = open(&sim);
    let mut cron = CronScheduler::new(app.take_cron_entries_for_tests());
    cron.upsert(app.db_for_tests(), 0).unwrap();

    sim.restart();
    drop(app);
    let mut app = open(&sim);
    let mut cron = CronScheduler::new(app.take_cron_entries_for_tests());
    // Re-upsert at a later now: the existing row keeps its phase.
    cron.upsert(app.db_for_tests(), 30 * 60 * 1_000_000_000)
        .unwrap();
    assert_eq!(
        cron.next_deadline(),
        Some(HOUR),
        "an existing schedule keeps next_due across restarts"
    );
}

#[test]
#[should_panic(expected = "scheduled twice")]
fn duplicate_cron_names_panic_at_mount() {
    let sim = Sim::with_faults(0x632, FaultConfig::none());
    let _ = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .schedule("n", Duration::from_secs(1), || CountUp { slot: 0 })
    .schedule("n", Duration::from_secs(2), || CountUp { slot: 1 });
}

#[test]
fn rebuild_restores_pending_jobs_after_reopen() {
    let sim = Sim::with_faults(0x624, FaultConfig::none());
    let open = |sim: &Sim| {
        App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Counter>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .unwrap()
        .mount_jobs(jobs![CountUp, AlwaysFails, Panics])
    };
    let mut app = open(&sim);
    app.db_for_tests()
        .transact(|tx| {
            tx.enqueue(CountUp { slot: 9 });
            Ok::<(), std::io::Error>(())
        })
        .unwrap();
    sim.restart();
    drop(app);
    let mut app = open(&sim);
    let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
    dispatcher
        .rebuild(&app.db_for_tests().snapshot().unwrap())
        .unwrap();
    assert!(
        dispatcher.has_due(0),
        "pending jobs survive recovery into the heap"
    );
    let outcome = dispatcher.run_due(app.db_for_tests(), 0).unwrap();
    assert!(matches!(outcome, JobOutcome::Done { .. }));
}

// ------------------------------------------------------------------- loop

use charpente::http::ApiError;
use charpente::http::{Method, Request, Response};
use charpente::kernel::{
    EventFrame, EventSink, Kernel, Reply, RequestSource, Respond, SimSource, SinkStatus, Step, Wake,
};
use charpente::route;
use charpente::routes;
use charpente_core::io::Instant;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const SEC: u64 = 1_000_000_000;

#[derive(Serialize, Deserialize)]
struct Hit {
    slot: u64,
}

#[route(POST "/hit")]
fn hit(tx: &mut Tx, body: Hit) -> Result<u64, ApiError> {
    tx.enqueue(CountUp { slot: body.slot });
    Ok(body.slot)
}

fn loop_app(sim: &Sim) -> App {
    App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![hit])
    .mount_jobs(jobs![CountUp])
    .schedule("hourly-count", Duration::from_secs(3600), || CountUp {
        slot: 777,
    })
}

/// Scripted request source for tests that need the reply itself (`SimSource`
/// above only records `(status, body)`): drains a fixed list of requests in
/// order, then reports `Shutdown`.
struct ScriptedSource {
    requests: VecDeque<Request>,
    replies: Arc<Mutex<Vec<Response>>>,
}

fn scripted_source(requests: Vec<Request>) -> (ScriptedSource, Arc<Mutex<Vec<Response>>>) {
    let replies = Arc::new(Mutex::new(Vec::new()));
    (
        ScriptedSource {
            requests: requests.into(),
            replies: replies.clone(),
        },
        replies,
    )
}

/// `ScriptedSource`'s `Respond`: `send` records the plain response;
/// `stream` (unreached by this file's tests — none subscribe) records the
/// opening response the same way and hands back a no-op sink so the trait
/// has a real implementation either way.
struct ScriptedReply {
    sink: Arc<Mutex<Vec<Response>>>,
}

impl Respond for ScriptedReply {
    fn send(self: Box<Self>, response: Response) {
        self.sink
            .lock()
            .expect("replies lock poisoned by an earlier panic")
            .push(response);
    }

    fn stream(self: Box<Self>, response: Response) -> Box<dyn EventSink> {
        self.sink
            .lock()
            .expect("replies lock poisoned by an earlier panic")
            .push(response);
        Box::new(NoopSink)
    }
}

struct NoopSink;

impl EventSink for NoopSink {
    fn send(&mut self, _frame: &EventFrame) -> SinkStatus {
        SinkStatus::Ok
    }
}

impl RequestSource for ScriptedSource {
    fn poll(&mut self) -> Option<(Request, Reply)> {
        let request = self.requests.pop_front()?;
        let reply: Reply = Box::new(ScriptedReply {
            sink: self.replies.clone(),
        });
        Some((request, reply))
    }

    fn wait_until(&mut self, _deadline: Option<Instant>) -> Wake {
        if self.requests.is_empty() {
            Wake::Shutdown
        } else {
            Wake::Ready
        }
    }
}

/// Spec ruling (2026-07-05): after a request-path Io becomes a 500, the
/// kernel checks Db::poisoned() and stops — it must not keep serving
/// guaranteed-500s off a dead engine. Non-poisoning failures keep step 5's
/// 500-and-continue contract.
#[test]
fn kernel_stops_after_a_request_poisons_the_engine() {
    // Fault-free open, then kill the disk: a lifetime sync_error would fail
    // Db::open itself before any request runs (Task 1 hit this; its test
    // validates the crash_disk_now -> Io -> poisoned mechanism used here).
    let sim = Sim::with_faults(0x0DED_0001, FaultConfig::none());
    let app = kernel_crash_app(&sim).unwrap();
    let hit = |n: u64| Request::new(Method::Post, "/hit").json(&Hit { slot: n });
    // Two requests scripted: the first 500s AND poisons; the loop must stop
    // with Err before the second is ever serviced.
    let (mut source, replies) = scripted_source(vec![hit(1), hit(2)]);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    sim.crash_disk_now(); // every disk op from here fails: request 1's commit Io-fails and poisons
    let err = kernel
        .run(&mut source)
        .expect_err("poisoned engine must stop the loop");
    let msg = err.to_string();
    assert!(
        msg.contains("poisoned"),
        "stop error must name the poison contract: {msg}"
    );
    assert!(
        msg.contains("durability unknowable") && msg.contains("restart the process"),
        "stop error must also name the site and the fix (hard rule 5): {msg}"
    );
    let replies = replies.lock().unwrap();
    // UPDATED at group-commit T6, and the guarantee is unchanged: the kernel
    // stops rather than keep serving off a dead engine (the `expect_err`
    // above is that assertion). What changed is the count. Both scripted
    // requests are drained into ONE batch before the commit is attempted, so
    // both are already in flight when it fails and both must be answered —
    // dropping a `Reply` hangs its client forever. Neither is "served off a
    // dead engine": they were staged before the engine was known dead, and
    // nothing arriving after this batch is served at all.
    assert_eq!(
        replies.len(),
        2,
        "both requests were staged in one batch, so both must be answered: {} replies",
        replies.len()
    );
    // Here `applied == 0` — the append/sync itself failed, so NEITHER record
    // reached the store and both requests are genuinely uncertain. Contrast
    // `kernel_stops_on_a_latched_store`, where the records ARE durable and
    // the same code path answers 200.
    assert!(
        replies.iter().all(|r| r.status == 500),
        "no record reached the store, so every request in the batch is uncertain: {:?}",
        replies.iter().map(|r| r.status).collect::<Vec<_>>()
    );
    // Moved here from `kernel_stops_on_a_latched_store` at group-commit T6:
    // that test has no 500 left to inspect now that a durable-record
    // checkpoint failure answers 200, so the "the 500 names the injected
    // fault" pin (hard rule 5) lives on the case that still produces one.
    let body = String::from_utf8_lossy(&replies[0].body);
    assert!(
        body.contains("simulated"),
        "the 500 should name the injected fault: {body}"
    );
}

// ------------- FIX: a latched redb handle now poisons `Db::poisoned()`
// (M2 sub-project 0a, spec docs/superpowers/specs/2026-07-30-store-latch-poison-design.md)
//
// Formerly a KNOWN GAP (edge sub-project T13, Hunt 1 — ledgered production
// follow-up, progress.md line 238 / spec §4.5). `Db::poisoned()` reflected
// only APPLY-side poison: the engine's "a durable record failed to apply"
// flag and its WAL's fsyncgate flag (`engine.rs`'s `poisoned()`). redb has a
// SECOND, independent poison of its own — `CheckedBackend` (redb 4.1.0
// `cached_file.rs:121`) sets `io_failed` on ANY backend error (read, write,
// set_len, sync_data) and every later backend op then returns
// `StorageError::PreviousIo`, "Previous I/O error occurred. Please close and
// re-open the database." Nothing mapped that latch onto `Db::poisoned()`, and
// the kernel's request arm had no other stop condition after a 500 (the
// 2026-07-05 ruling, pinned by `kernel_stops_after_a_request_poisons_the_engine`
// above) — so a node could serve off a store handle that was permanently dead
// for writes while `Db::poisoned()` reported healthy.
//
// `Store::failed()` (sub-project 0a, Task 1) now observes redb's identical
// `io_failed` condition one layer down, at charpente's own `DiskBackend`, and
// `Db::poisoned()` ORs it in — so the kernel's existing request-arm stop fires
// on a latched store with no new control flow, only a distinct message
// (`Db::store_failed()` selects it): a store latch leaves the WAL intact and
// authoritative, unlike apply-poison, which means durability became
// unknowable.
//
// **The brief's originally predicted mechanism did not hold FOR A
// CACHE-RESIDENT STORE, which is the shape this test builds.** T13's brief
// expected a request-path READ fault to set the latch and every later request
// to keep 500ing. Here a store read never reaches the `Disk` boundary at all,
// so there is no read to fault: redb's open path walks every page (forced on
// in BOTH build modes by the root `Cargo.toml`'s `[profile.*.package.redb]`
// debug-assertions override, so the seed promise stays cross-mode), and its
// read cache then answers every subsequent read from memory. Measured while
// writing the original characterization: a 10k-row store plus 300 further
// write transactions and 300 reads produce ZERO post-open reads at the
// boundary. The latch is therefore reached the only way it is reachable here:
// the store's own checkpoint fsync — exactly the mechanism this test drives.
//
// **That is a bound, not an absolute** (T13 review, I-1). redb's read cache is
// a bounded, EVICTING LRU (`cached_file.rs:236`'s striped `LRUCache`,
// `evict_from_read_cache`) sized by `max_cache_size`, 1 GiB by default
// (`db.rs:1126`) and not configured by `Store::open`. Once a store's working
// set exceeds that bound the open-time page walk itself evicts, later reads
// miss, and store reads DO reach the boundary — at which point a failed read
// sets `Store::failed()` too (its read arm, covering this shape by
// construction; spec §2). This shape stays DERIVED, not measured (T13 fix
// round 2, N-1 / spec Scope ruling 4): the premises above are verified in
// redb's source, but no store over the 1 GiB bound was ever built.
//
//   * **cache-resident store** (this test): the latch was SILENT before this
//     fix — reads kept answering 200 from cache, nothing surfaced at all.
//     MEASURED. Now: the write whose checkpoint hits the latch poisons the Db
//     immediately, and the kernel stops before either scripted read is
//     served — the silence never gets a chance to matter.
//   * **larger-than-cache store**: the latch was already LOUD — reads that
//     miss the cache 500. DERIVED, not measured. Now those same failed reads
//     also set `Store::failed()`, so `Db::poisoned()` catches the case the
//     brief predicted without a dedicated 500-storm test (`begin_read` stays
//     ungated in redb, `db.rs:404`/`db.rs:508`, so this is the only place the
//     larger-than-cache shape gets covered).
//
// **The correspondence assertion** (`db_poisoned`, below) is the load-bearing
// one. This test builds a genuinely latched store — proven by `fired == 1`
// plus the mechanism above — so it is the one place that can assert
// `Db::poisoned()` tracks redb's real `io_failed` state rather than
// charpente's belief about it. If a future redb version latches through a
// path `DiskBackend` does not observe, THIS is the assertion that goes red
// (spec §1's trade, stated so it is not rediscovered later).
//
// **For a reader diffing against the old characterization: the FIRST
// assertion to change is not the headline one.** `replies.len()` now reads 1,
// not 3 — the kernel stops after the latched write's 500, before either
// scripted read is served. That earlier redness (observed against unfixed
// code before this fix landed) is the fix working, not a second, unrelated
// break.

/// A pure read path: no effects, so nothing it does can reach the WAL, and any
/// status it returns is the store's answer alone.
#[route(GET "/count/:id")]
fn read_count(tx: &mut Tx, id: Id<Counter>) -> Result<u64, ApiError> {
    Ok(tx.get(id)?.map_or(0, |c| c.hits))
}

/// A pure store write — no enqueue, so servicing it never also arms the job
/// arm and the reply sequence below stays exactly one reply per request.
#[route(POST "/bump/:id")]
fn bump_count(tx: &mut Tx, id: Id<Counter>) -> Result<u64, ApiError> {
    let hits = tx.get(id)?.map_or(0, |c| c.hits) + 1;
    tx.put(&Counter { id, hits });
    Ok(hits)
}

/// Delegates every call to the sim's own disk with one deliberate exception:
/// while `armed`, the next `sync` of `store_path` fails ONCE — with the sim
/// `sync_error` axis's exact wording, so `checks::classify` still reads it as
/// `Injected` — and then disarms.
///
/// Why the store's `sync` and not a `write_at`: `Store::apply` commits at
/// `Durability::None` (no fsync), so the ONLY store sync is
/// `Store::checkpoint`'s. Targeting `sync` therefore lands the fault on the
/// checkpoint deterministically, never on a request's apply — where it would
/// poison the engine apply-side and characterize the already-ruled path
/// instead of this one. One-shot, and `fired` is asserted == 1, so the
/// persistence observed afterwards can only be redb's latch and not repeated
/// injection.
///
/// `reads` counts every store read that reaches this boundary — the
/// measurement behind the module comment's unreachability claim.
struct OneShotStoreSyncFaultDisk {
    inner: charpente_sim::SimDisk,
    store_path: String,
    armed: Arc<AtomicBool>,
    fired: Arc<AtomicU64>,
    reads: Arc<AtomicU64>,
}

struct StoreFaultFile {
    inner: Box<dyn charpente_core::io::File>,
    /// `None` for every file but the store — the WAL is delegated untouched.
    fault: Option<(Arc<AtomicBool>, Arc<AtomicU64>, Arc<AtomicU64>)>,
}

impl charpente_core::io::Disk for OneShotStoreSyncFaultDisk {
    fn open(&self, path: &str) -> std::io::Result<Box<dyn charpente_core::io::File>> {
        let inner = self.inner.open(path)?;
        let fault = (path == self.store_path)
            .then(|| (self.armed.clone(), self.fired.clone(), self.reads.clone()));
        Ok(Box::new(StoreFaultFile { inner, fault }))
    }

    fn remove(&self, path: &str) -> std::io::Result<()> {
        self.inner.remove(path)
    }

    fn list(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        self.inner.list(prefix)
    }

    fn sync_dir(&self, prefix: &str) -> std::io::Result<()> {
        self.inner.sync_dir(prefix)
    }
}

impl charpente_core::io::File for StoreFaultFile {
    fn len(&self) -> std::io::Result<u64> {
        self.inner.len()
    }

    fn read_at(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        if let Some((_, _, reads)) = &self.fault {
            reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.read_at(offset, out)
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.inner.write_at(offset, data)
    }

    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.inner.set_len(len)
    }

    fn sync(&self) -> std::io::Result<()> {
        if let Some((armed, fired, _)) = &self.fault
            && armed.swap(false, Ordering::SeqCst)
        {
            fired.fetch_add(1, Ordering::SeqCst);
            return Err(std::io::Error::other(
                "simulated fsync failure — durability of writes since the last successful sync \
                 is unknown",
            ));
        }
        self.inner.sync()
    }
}

/// `ScriptedSource`, plus: arms `armed` the moment it hands over its FIRST
/// request. That is the whole reason this source exists rather than arming
/// before `Kernel::run` — `run`'s startup schema gate (`migrate_schema`) and
/// `Kernel::new` both touch the store before any request is polled, and either
/// would consume the one-shot injection at a site that is not the request arm
/// under test.
struct ArmingSource {
    inner: ScriptedSource,
    armed: Arc<AtomicBool>,
    polled: u64,
}

impl RequestSource for ArmingSource {
    fn poll(&mut self) -> Option<(Request, Reply)> {
        let out = self.inner.poll()?;
        self.polled += 1;
        if self.polled == 1 {
            self.armed.store(true, Ordering::SeqCst);
        }
        Some(out)
    }

    fn wait_until(&mut self, deadline: Option<Instant>) -> Wake {
        self.inner.wait_until(deadline)
    }
}

/// `checkpoint_every: 1` so the FIRST write request runs the cadence
/// checkpoint inside its own `transact` — the fault lands on the request path,
/// not on a test-invoked `Db::checkpoint` call the kernel never makes.
fn latch_config() -> AppConfig {
    AppConfig {
        db: charpente::DbConfig {
            checkpoint_every: 1,
            ..crash_config().db
        },
        ..crash_config()
    }
}

#[test]
fn kernel_stops_on_a_latched_store() {
    let sim = Sim::with_faults(0x0DED_0002, FaultConfig::none());
    // Phase 1 (plain disk, no injector): commit and CHECKPOINT some rows, so
    // the GETs below read committed durable state, then drop the handle.
    {
        let mut warm = App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Counter>(),
            Box::new(sim.entropy()),
            latch_config(),
        )
        .unwrap();
        warm.db_for_tests()
            .transact(|tx| {
                tx.put(&Counter {
                    id: Id::from_raw(1),
                    hits: 7,
                });
                Ok::<(), std::io::Error>(())
            })
            .unwrap();
    }

    // Phase 2: a fresh handle over the injector disk.
    let armed = Arc::new(AtomicBool::new(false));
    let fired = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let app = App::open(
        Box::new(OneShotStoreSyncFaultDisk {
            inner: sim.disk(),
            store_path: latch_config().db.store_path.clone(),
            armed: armed.clone(),
            fired: fired.clone(),
            reads: reads.clone(),
        }),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        latch_config(),
    )
    .unwrap()
    .mount(routes![read_count, bump_count]);

    // One write (whose in-transact checkpoint eats the fault), then two reads
    // — deliberately NO second write, so the latched window is still open when
    // the run ends and `Db::poisoned()` can be observed inside it.
    let (inner, replies) = scripted_source(vec![
        Request::new(Method::Post, "/bump/1"),
        Request::new(Method::Get, "/count/1"),
        Request::new(Method::Get, "/count/1"),
    ]);
    let mut source = ArmingSource {
        inner,
        armed,
        polled: 0,
    };
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let reads_at_open = reads.load(Ordering::SeqCst);
    let run = kernel.run(&mut source);
    let db_poisoned = kernel.app().db().poisoned();

    assert_eq!(
        fired.load(Ordering::SeqCst),
        1,
        "the injector must fire EXACTLY once — the persistence observed below is only evidence \
         of redb's latch if nothing kept re-injecting"
    );
    // The mechanism tripwire (module comment): no store read reaches the
    // `Disk` boundary after open in THIS store's shape, which is why a
    // request-path READ fault — the shape T13's brief predicted — has nothing
    // to fault here.
    //
    // Read it for exactly what it proves, and no more (T13 review, M-5). Only
    // the PRE-latch part of the window is evidence about caching; after the
    // latch, `CheckedBackend::read` short-circuits on `io_failed` before ever
    // calling the backend, so the equality would hold post-latch even with no
    // cache at all. And this store is ~2 rows, far under redb's 1 GiB
    // `max_cache_size`, so the equality is insensitive to a change in that
    // bound — a shrunken cache would still not redden it. The "reads stay
    // silent" half of the old characterization is no longer observable in this
    // test at all: the fix stops the kernel before either scripted GET is
    // served, so there are no 200s left to point at. It survives only as
    // DESIGN §4's MEASURED row, recorded from this test's pre-fix behaviour.
    assert!(
        reads_at_open > 0,
        "anti-vacuity: zero store reads were counted even at open, so this injector is not \
         attached to the store file at all and the equality below would hold trivially"
    );
    assert_eq!(
        reads.load(Ordering::SeqCst),
        reads_at_open,
        "a store read reached the Disk boundary after open ({} at open, {} after the run) — \
         redb stopped serving this (cache-resident) store's reads from memory, so the \
         request-path READ fault this gap was first predicted through has something to fault \
         again. NOTE this equality is a tripwire, not a proof of unreachability in general: \
         redb's read cache is a bounded evicting LRU, so a store larger than it always reaches \
         the boundary (see the module comment's two shapes)",
        reads_at_open,
        reads.load(Ordering::SeqCst)
    );

    let replies = replies.lock().unwrap();
    // THE FIX (was: the gap). The write's cadence checkpoint hits the store
    // latch, which now poisons the Db, so the kernel stops — that is the
    // `expect_err` above and it is unchanged.
    //
    // UPDATED at group-commit T6 (Noah ruling 2026-08-12), and this test is
    // now the one that pins the ruling. The write and the two scripted reads
    // are drained into ONE batch, so all three are answered before the stop.
    // All three are 200s, and that is the honest answer, not a regression:
    // `Group::commit`'s checkpoint arm reports `applied == n`, meaning every
    // record REACHED THE STORE and the failure is durability bookkeeping
    // after the fact. The write's record is durable. Telling it "may or may
    // not have committed" was the half of this sub-project's own gap (M2 0a,
    // whose description opens "gave a 500 to a record that WAS durable")
    // that 0a's fix left open; it is closed here, on both paths at once, by
    // `CommitFailure::invalidates`.
    //
    // The reads were always going to be 200s — they stage no record, so no
    // commit outcome can make them uncertain.
    assert_eq!(
        replies.len(),
        3,
        "the write and both reads were staged in one batch, so all three are \
         answered before the kernel stops: {} replies",
        replies.len()
    );
    assert!(
        replies.iter().all(|r| r.status == 200),
        "a latched CHECKPOINT means the records are durable (applied == n) — \
         every request in the batch keeps its honest answer: {:?}",
        replies.iter().map(|r| r.status).collect::<Vec<_>>()
    );
    // THE CORRESPONDENCE ASSERTION (spec §1), and it takes BOTH halves: one
    // reading charpente's mirror, one reading redb itself. A test that consults
    // only the mirror cannot tell a correct latch from a drifted one.
    //
    // Scope it honestly. Both halves ride the checkpoint-fsync path — a path
    // `DiskBackend` observes — so together they pin drift ON THAT PATH in
    // either direction: redb latching where charpente missed it (the
    // under-approximation, caught by the mirror half), and charpente latching
    // where redb no longer does (the over-approximation, which would poison
    // healthy nodes into permanent unavailability, and which ONLY the redb half
    // catches). What no assertion here catches is a future redb growing an
    // entirely new latch path that `DiskBackend` never sees; that stays a
    // redb-upgrade review item against spec §1's five-site census.
    assert!(
        db_poisoned,
        "the store handle is latched, so Db::poisoned() must be true — \
         charpente's flag has drifted from redb's io_failed condition"
    );
    // The redb half. `checkpoint` is the probe because `begin_write` IS
    // latch-gated while `begin_read` is not (DESIGN §4), so a read would prove
    // nothing. This is also the repo's only LIVE observation of redb's real
    // PreviousIo prose — which is `checks::classify`'s third needle, and the
    // string the stop message below deliberately carries.
    let mut app = kernel.into_app();
    let refused = app
        .db_for_tests()
        .checkpoint()
        .expect_err("redb must refuse a checkpoint on a latched handle");
    assert!(
        refused
            .to_string()
            .contains("close and re-open the database"),
        "redb itself — not just charpente's mirror of it — must be refusing: {refused}"
    );
    // And the stop must say WHICH poison, because the remedies differ in what
    // they imply about durable state.
    let err = run.expect_err("a poisoned Db stops the kernel");
    let msg = err.to_string();
    assert_eq!(
        msg,
        "kernel stopped: the store handle is dead after a disk IO failure \
         (redb refuses every further operation until you close and re-open \
         the database) — durable state is intact and the WAL is \
         authoritative; restart the process to recover",
        "the store-latch stop must be pinned byte-exact and must NOT reuse the \
         apply-poison wording, which claims durability became unknowable"
    );
    // The stop must stay classifiable as redb-latch fallout, not an
    // unexplained bug: spec §8 forbids touching `checks::classify`, so the
    // coupling is satisfied from this side, by carrying redb's own prose.
    // Without this, every sim universe whose injected fault latches the store
    // fails as `Unrelated` (found by the auth suite, T2).
    assert!(
        is_fault_fallout(&msg),
        "the store-latch stop must classify as fault fallout: {msg}"
    );
}

/// Group-commit T7 fix round, I-2 (Noah ruling 2026-08-13): `Dispatcher::
/// run_due`'s wrapper must key its commit-failure policy on
/// `CommitFailure::invalidates` — the same rule `Db::transact` uses
/// (`db.rs:1438-1445`) — not an unconditional restore. Reuses
/// `OneShotStoreSyncFaultDisk`/`latch_config` (above) to drive the
/// CHECKPOINT arm specifically: `applied == n` (the job's Done-mark reached
/// the store) and the failure is already reported through
/// `Db::store_failed()`. This is the arm the old code answered wrong.
#[test]
fn run_due_answers_ok_when_the_commit_failure_is_a_reported_elsewhere_checkpoint() {
    let sim = Sim::with_faults(0x60C0_0012, FaultConfig::none());
    let armed = Arc::new(AtomicBool::new(false));
    let fired = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let mut app = App::open(
        Box::new(OneShotStoreSyncFaultDisk {
            inner: sim.disk(),
            store_path: latch_config().db.store_path.clone(),
            armed: armed.clone(),
            fired: fired.clone(),
            reads: reads.clone(),
        }),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        latch_config(),
    )
    .unwrap()
    .mount_jobs(jobs![CountUp]);

    let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
    let db = app.db_for_tests();
    // Enqueue while UNARMED: this transact's own checkpoint_every: 1 cadence
    // checkpoint fires and succeeds, resetting records_since_checkpoint to 0
    // before the fault is armed — the fault must land on run_due's OWN
    // commit, not this one.
    db.transact(|tx| {
        tx.enqueue(CountUp { slot: 1 });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    dispatcher.note_enqueued(db.drain_enqueued());
    assert!(dispatcher.has_due(0));

    armed.store(true, Ordering::SeqCst);
    let outcome = dispatcher.run_due(db, 0);

    assert_eq!(
        fired.load(Ordering::SeqCst),
        1,
        "the injector must fire exactly once — the checkpoint commit failure this test needs"
    );
    // RED-FIRST EVIDENCE (fix report, hard rule 3): against the
    // unconditional-restore code this `unwrap_or_else` panicked with the
    // commit failure's own io::Error text instead of ever reaching an Ok.
    let outcome = outcome.unwrap_or_else(|e| {
        panic!(
            "run_due must answer Ok for a job whose Done-mark reached the store — the \
             checkpoint-arm commit failure ({e}) is already reported through \
             Db::store_failed(), so nothing further should invalidate this job's outcome"
        )
    });
    assert!(
        matches!(outcome, JobOutcome::Done { id: 0 }),
        "got {outcome:?}"
    );
    assert!(
        db.store_failed(),
        "the fault must have actually latched the store, or this test proves nothing"
    );
    assert!(
        !dispatcher.has_due(0),
        "the job's Done-mark IS durable (checkpoint arm, applied == n) — leaving (due_at, id) \
         in the heap here is exactly the state Dispatcher::run_due_into's own invariant assert \
         exists to catch (a Done row the heap still calls Pending)"
    );
    let row = &db.snapshot().unwrap().scan::<JobRow>().unwrap()[0];
    assert_eq!(row.status, JobStatus::Done, "durable in the store");
}

/// The cron-wrapper twin of the test above: `CronScheduler::tick` must apply
/// the identical ruled policy. Same mechanism, same checkpoint arm — the
/// schedule's advanced `next_due` and its enqueue both reached the store, so
/// rolling `*cached` back to `previous` here would re-tick and re-enqueue the
/// SAME interval the next time `has_due` is checked.
///
/// Seed `0x60C0_0013`: the fix-round brief allocated `0x60C0_0012` for "the
/// I-2 test" (singular); this covers the SECOND wrapper the same review
/// flagged (`CronScheduler::tick`, `cron.rs:142-163`) rather than leaving it
/// unreached, so it mints one more from the same free range (namespace
/// census re-run before use: `0x60C0_0001`..`0x60C0_0010` plus this task's
/// own `0x0011`/`0x0012` were the only ones taken).
#[test]
fn tick_answers_ok_when_the_commit_failure_is_a_reported_elsewhere_checkpoint() {
    let sim = Sim::with_faults(0x60C0_0013, FaultConfig::none());
    let armed = Arc::new(AtomicBool::new(false));
    let fired = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let mut app = App::open(
        Box::new(OneShotStoreSyncFaultDisk {
            inner: sim.disk(),
            store_path: latch_config().db.store_path.clone(),
            armed: armed.clone(),
            fired: fired.clone(),
            reads: reads.clone(),
        }),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        latch_config(),
    )
    .unwrap()
    .mount_jobs(jobs![CountUp])
    .schedule("hourly-count", Duration::from_secs(3600), || CountUp {
        slot: 1,
    });

    let mut cron = CronScheduler::new(app.take_cron_entries_for_tests());
    let db = app.db_for_tests();
    // upsert while UNARMED, same reasoning as the job-wrapper test above: its
    // own checkpoint_every: 1 cadence checkpoint must fire and succeed
    // before the fault is armed.
    cron.upsert(db, 0).unwrap();
    assert!(cron.has_due(HOUR));

    armed.store(true, Ordering::SeqCst);
    let outcome = cron.tick(db, HOUR);

    assert_eq!(
        fired.load(Ordering::SeqCst),
        1,
        "the injector must fire exactly once — the checkpoint commit failure this test needs"
    );
    // RED-FIRST EVIDENCE (fix report, hard rule 3): against the
    // unconditional-restore code this `unwrap_or_else` panicked with the
    // commit failure's own io::Error text instead of ever reaching an Ok.
    let (name, catchup) = outcome.unwrap_or_else(|e| {
        panic!(
            "tick must answer Ok for a schedule whose advance reached the store — the \
             checkpoint-arm commit failure ({e}) is already reported through \
             Db::store_failed(), so nothing further should invalidate this tick's outcome"
        )
    });
    assert_eq!((name, catchup), ("hourly-count", false));
    assert!(
        db.store_failed(),
        "the fault must have actually latched the store, or this test proves nothing"
    );
    assert_eq!(
        cron.next_deadline(),
        Some(2 * HOUR),
        "the schedule's next_due IS durable (checkpoint arm, applied == n) — rolling *cached \
         back to `previous` here would re-tick and re-enqueue the SAME interval the next time \
         has_due is checked"
    );
    assert_eq!(
        db.drain_enqueued().len(),
        1,
        "the enqueue landed in the same durable batch as the schedule's advance"
    );
}

#[test]
fn loop_serves_requests_runs_jobs_and_ticks_cron() {
    let sim = Sim::with_faults(0x640, FaultConfig::none());
    let mut source = SimSource::new(Box::new(sim.clock()), 2 * HOUR + SEC);
    for i in 0..3u64 {
        source.push_at(
            i * SEC,
            Request::new(Method::Post, "/hit").json(&Hit { slot: i }),
        );
    }
    let responses = source.responses();

    let mut kernel = Kernel::new(loop_app(&sim), Box::new(sim.clock())).unwrap();
    kernel.run(&mut source).unwrap(); // returns at the horizon (Shutdown)

    let stats = kernel.stats();
    assert_eq!(stats.requests, 3);
    assert_eq!(stats.cron_ticks, 2, "two hours inside the horizon");
    assert_eq!(stats.jobs_done, 5, "3 request jobs + 2 cron jobs");
    assert_eq!((stats.jobs_retried, stats.jobs_dead), (0, 0));

    let recorded = responses.lock().unwrap();
    assert_eq!(recorded.len(), 3);
    assert!(recorded.iter().all(|(status, _)| *status == 200));

    let mut app = kernel.into_app();
    let snap = app.db_for_tests().snapshot().unwrap();
    let counters = snap.scan::<Counter>().unwrap();
    // slots 0,1,2 hit once each; cron slot 777 hit twice.
    assert_eq!(counters.len(), 4);
    assert_eq!(
        counters
            .iter()
            .map(|c| (c.id.raw(), c.hits))
            .collect::<Vec<_>>(),
        vec![(0, 1), (1, 1), (2, 1), (777, 2)]
    );
    let jobs = snap.scan::<JobRow>().unwrap();
    assert!(jobs.iter().all(|j| j.status == JobStatus::Done));
}

#[test]
fn loop_replays_identically_for_the_same_seed() {
    let trace = |seed: u64| {
        let sim = Sim::new(seed); // default faults, including latency
        let mut source = SimSource::new(Box::new(sim.clock()), HOUR);
        for i in 0..5u64 {
            source.push_at(
                i * SEC,
                Request::new(Method::Post, "/hit").json(&Hit { slot: i }),
            );
        }
        let responses = source.responses();
        let run = (|| -> Result<(), std::io::Error> {
            let mut kernel = Kernel::new(loop_app(&sim), Box::new(sim.clock()))?;
            kernel.run(&mut source)
        })();
        let outcome = format!("{run:?}");
        let recorded = responses.lock().unwrap().clone();
        (outcome, recorded, sim.durable_fingerprint(), sim.now())
    };
    for seed in [0x7, 0xB0B, 0xC0FFEE] {
        assert_eq!(trace(seed), trace(seed), "seed {seed:#x}");
    }
}

// ---------------------------------------------------------- swarm properties

use charpente_core::log::WalConfig;
use charpente_sim::run_universes_swarm;

fn crash_config() -> AppConfig {
    AppConfig {
        body_cap: 1 << 20,
        db: charpente::DbConfig {
            wal: WalConfig {
                dir: "wal".to_string(),
                segment_bytes: 4096,
            },
            store_path: "tables.redb".to_string(),
            ..charpente::DbConfig::default()
        },
        ..AppConfig::default()
    }
}

#[job(retry(max = 3, backoff = "5s"))]
struct Flaky {
    roll: u64,
}

impl Flaky {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        // Deterministic per-payload failure: rolls 0/1 always fail (their
        // retries march to Dead through the log); 2.. succeed and count.
        if self.roll < 2 {
            return Err(JobError::app(format!("flaky roll {} refused", self.roll)));
        }
        let hits = tx
            .get(Id::<Counter>::from_raw(self.roll))?
            .map_or(0, |c| c.hits);
        tx.put(&Counter {
            id: Id::from_raw(self.roll),
            hits: hits + 1,
        });
        Ok(())
    }
}

fn kernel_crash_app(sim: &Sim) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        crash_config(),
    )?
    .mount(routes![hit])
    .mount_jobs(jobs![CountUp, Flaky])
    .schedule("minutely", Duration::from_secs(60), || CountUp {
        slot: 999,
    }))
}

/// The exactly-once invariant: every Done job that writes to `Counter` — both
/// `CountUp{slot}` and `Flaky{roll}` (Flaky rolls 0/1 can never reach Done;
/// they fail deterministically and die Dead, so every Done Flaky wrote
/// exactly once, same as CountUp) — bumped its counter exactly once, hits ≡
/// Done-count per key, regardless of crashes. `CountUp` and `Flaky` sharing
/// one `Counter` id space is deliberate, not an oversight: exactly-once
/// holding across two interleaved writer job types on one key is a strictly
/// stronger property than each type owning a disjoint domain.
fn exactly_once_holds(app: &mut App) -> Result<(), String> {
    let snap = app.db_for_tests().snapshot().map_err(|e| e.to_string())?;
    let jobs = snap.scan::<JobRow>().map_err(|e| e.to_string())?;
    let counters: std::collections::BTreeMap<u64, u64> = snap
        .scan::<Counter>()
        .map_err(|e| e.to_string())?
        .into_iter()
        .map(|c| (c.id.raw(), c.hits))
        .collect();
    let mut done: std::collections::BTreeMap<u64, u64> = Default::default();
    for job in &jobs {
        if job.status == JobStatus::Done && (job.name == "CountUp" || job.name == "Flaky") {
            let key = u64::decode(&mut &job.payload[..]).map_err(|e| e.to_string())?;
            *done.entry(key).or_insert(0) += 1;
        }
    }
    for (&key, &done_count) in &done {
        let hits = counters.get(&key).copied().unwrap_or(0);
        if hits != done_count {
            return Err(format!(
                "exactly-once violated at key {key}: {done_count} Done jobs but {hits} hits"
            ));
        }
    }
    // And no counter without a Done job (effects never outrun the done-mark —
    // this holds for cron's slot 999 too; its hits also commit with a done-mark).
    for (&key, &hits) in &counters {
        if done.get(&key).copied().unwrap_or(0) == 0 && hits > 0 {
            return Err(format!("key {key} has {hits} hits but no Done job"));
        }
    }
    Ok(())
}

fn kernel_workload(sim: &mut Sim) -> Result<(), String> {
    let entropy = sim.entropy();
    for _round in 0..3 {
        sim.crash_disk_after(64 + entropy.next_u64() % 768);
        let mut app = match kernel_crash_app(sim) {
            Ok(app) => app,
            Err(e) => {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("open failed non-injected: {e}"));
                }
                sim.restart();
                continue;
            }
        };
        // The checker's own reads (snapshot/scan) are disk ops too: an armed
        // crash countdown can trip right here, so this call needs the same
        // classify-and-reopen treatment as every other fallible step below —
        // otherwise an injected fault mid-check reads as a real violation
        // (found by seed 0x348270b38f166529).
        match exactly_once_holds(&mut app) {
            Ok(()) => {}
            Err(msg) => {
                if !is_fault_fallout(&msg) {
                    return Err(msg);
                }
                sim.restart();
                continue;
            }
        }

        let horizon = sim.now().as_nanos() + 400 * SEC;
        let mut source = SimSource::new(Box::new(sim.clock()), horizon);
        let base = sim.now().as_nanos();
        for i in 0..(1 + entropy.next_u64() % 6) {
            let slot = entropy.next_u64() % 8;
            source.push_at(
                base + i * SEC,
                Request::new(Method::Post, "/hit").json(&Hit { slot }),
            );
        }
        // A couple of Flaky jobs enqueued directly (retry path under faults).
        // A fault here can poison the Db (the WAL marks itself poisoned on a
        // write/sync failure); building Kernel::new on a poisoned Db would
        // surface a different, generic "poisoned" message on the next WAL op
        // instead of the real injected one, so classify right at the site —
        // same "reopen on injected, fail otherwise" shape as every other
        // fallible step in this workload.
        match app.db_for_tests().transact(|tx| {
            tx.enqueue(Flaky {
                roll: entropy.next_u64() % 4,
            });
            Ok::<(), std::io::Error>(())
        }) {
            Ok(()) => {}
            Err(TxError::Io(e)) => {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("Flaky pre-enqueue failed non-injected: {e}"));
                }
                sim.restart();
                continue;
            }
            Err(TxError::App(e)) => {
                // The closure only ever returns Ok — an App rejection here
                // would mean the closure itself changed underneath us.
                return Err(format!("Flaky pre-enqueue: impossible App rejection: {e}"));
            }
            Err(TxError::NotPrimary { primary }) => {
                // This workload never calls forbid_writes.
                return Err(format!(
                    "Flaky pre-enqueue: impossible NotPrimary rejection (primary {primary})"
                ));
            }
            Err(TxError::Verifying { unreached }) => {
                // This workload never calls verify_writes.
                return Err(format!(
                    "Flaky pre-enqueue: impossible Verifying rejection (unreached {unreached:?})"
                ));
            }
        }

        let run = (|| -> Result<(), std::io::Error> {
            let mut kernel = Kernel::new(app, Box::new(sim.clock()))?;
            let out = kernel.run(&mut source);
            let stats = kernel.stats();
            if stats.jobs_done > 0 {
                sim.cover("job:done");
            }
            if stats.jobs_retried > 0 {
                sim.cover("job:retried");
            }
            if stats.jobs_dead > 0 {
                sim.cover("job:dead");
            }
            if stats.cron_ticks > 0 {
                sim.cover("cron:tick");
            }
            if stats.cron_catchups > 0 {
                sim.cover("cron:catchup");
            }
            out
        })();
        if let Err(e) = run
            && !is_fault_fallout(&e.to_string())
        {
            return Err(format!("kernel stopped non-injected: {e}"));
        }
        sim.restart();
    }

    // Final quiet verification (step-4/5 convention): reopen until injected
    // faults relent, then check the invariant one last time.
    for _attempt in 0..8 {
        match kernel_crash_app(sim) {
            // Same classify-and-reopen treatment as the round loop above: the
            // checker's own reads can hit an armed crash too.
            Ok(mut app) => match exactly_once_holds(&mut app) {
                Ok(()) => return Ok(()),
                Err(msg) => {
                    if !is_fault_fallout(&msg) {
                        return Err(msg);
                    }
                    sim.restart();
                }
            },
            Err(e) if is_fault_fallout(&e.to_string()) => sim.restart(),
            Err(e) => return Err(format!("final open failed non-injected: {e}")),
        }
    }
    Ok(())
}

#[test]
fn kernel_jobs_are_exactly_once_under_the_fault_matrix() {
    run_universes_swarm!(256, kernel_workload, require: [
        "job:done",
        "job:retried",
        "cron:tick",
        "fault:crash",
        "crash:torn",
        "restart",
    ]);
}

#[test]
fn always_failing_job_dies_and_the_monthly_cron_ticks_120_times() {
    let sim = Sim::with_faults(0x650, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![AlwaysFails, CountUp])
    .schedule("monthly", Duration::from_secs(30 * 24 * 3600), || CountUp {
        slot: 1,
    });

    // Kernel::new consumes the App: all seeding happens BEFORE construction.
    let mut app = app;
    app.db_for_tests()
        .transact(|tx| {
            tx.enqueue(AlwaysFails {});
            Ok::<(), std::io::Error>(())
        })
        .unwrap();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let mut source = SimSource::new(
        Box::new(sim.clock()),
        10 * 365 * 24 * 3600 * SEC, // ten years — DESIGN §6's promise, literally
    );
    kernel.run(&mut source).unwrap();

    let stats = kernel.stats();
    assert_eq!(stats.jobs_dead, 1, "AlwaysFails dies at max attempts");
    assert_eq!(stats.jobs_retried, 1, "max 2: one retry, then dead");
    assert_eq!(
        stats.cron_catchups, 0,
        "neither schedule — monthly or the platform's built-in daily sweep \
         (auth T6) — ever needs catch-up in this quiet decade-long run"
    );
    let app = kernel.into_app();
    // stats.cron_ticks now aggregates BOTH schedules: "monthly" and the
    // platform's built-in daily session-expiry sweep, which every App gets
    // with no opt-in (auth T6) — over ten years that's ~3650 extra ticks,
    // so a raw total is no longer a precise stand-in for "did the monthly
    // schedule fire 121 times". Scope the check to the monthly schedule's
    // own effect instead: it enqueues CountUp{slot: 1} every tick, so
    // Counter{id: 1}.hits IS its tick count, immune to how many other
    // schedules also ticked in the same run.
    let monthly_hits = app
        .db()
        .snapshot()
        .unwrap()
        .scan::<Counter>()
        .unwrap()
        .into_iter()
        .find(|c| c.id.raw() == 1)
        .map_or(0, |c| c.hits);
    // First tick lands one interval in; ten years of 30-day months:
    assert_eq!(monthly_hits, 121, "the monthly schedule's own tick count");
    // ...and this whole decade of queue traffic ran in well under a second
    // of wall time, or the suite would be unshippable. Virtual time works.
    drop(app);
}

/// A deliberately WRONG executor: handler effects commit first, the
/// done-mark commits separately. The exactly-once invariant must be able to
/// see the difference under a crash between the two — otherwise our flagship
/// property is theater.
#[test]
fn harness_catches_a_split_done_mark() {
    let sim = Sim::with_faults(0x660, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![CountUp]);
    let db = app.db_for_tests();
    db.transact(|tx| {
        tx.enqueue(CountUp { slot: 5 });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let id = db.drain_enqueued()[0];

    // Wrong executor, transaction 1: handler effects WITHOUT the done-mark.
    db.transact(|tx| {
        let hits = tx.get(Id::<Counter>::from_raw(5))?.map_or(0, |c| c.hits);
        tx.put(&Counter {
            id: Id::from_raw(5),
            hits: hits + 1,
        });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    // Crash lands here, before transaction 2 (the separate done-mark).
    sim.restart();
    drop(app);

    // Recovery: the job is still Pending; a correct re-execution double-counts.
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![CountUp]);
    let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
    dispatcher
        .rebuild(&app.db_for_tests().snapshot().unwrap())
        .unwrap();
    assert!(
        dispatcher.has_due(0),
        "the split executor left the job Pending"
    );
    dispatcher.run_due(app.db_for_tests(), 0).unwrap();

    let err = exactly_once_holds(&mut app).unwrap_err();
    assert!(
        err.contains("exactly-once violated at key 5"),
        "the invariant must catch the split done-mark: {err}"
    );
    let _ = id;
}

#[test]
fn idempotency_key_rides_the_row_and_means_nothing_to_the_platform() {
    let (_sim, mut app) = quiet_kernel_app(0x670);
    let db = app.db_for_tests();
    db.transact(|tx| {
        tx.enqueue_keyed("receipt:42", CountUp { slot: 1 });
        tx.enqueue_keyed("receipt:42", CountUp { slot: 2 }); // same key: BOTH exist
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let jobs = db.snapshot().unwrap().scan::<JobRow>().unwrap();
    assert_eq!(jobs.len(), 2, "keys are identity, not dedupe — spec ruling");
    assert!(jobs.iter().all(|j| j.key.as_deref() == Some("receipt:42")));
}

#[test]
fn due_jobs_are_not_starved_by_a_request_flood() {
    let sim = Sim::with_faults(0x680, FaultConfig::none());
    let mut source = SimSource::new(Box::new(sim.clock()), 200 * SEC);
    for i in 0..100u64 {
        // All requests arrive at t=0: maximal contention with the due job.
        source.push_at(0, Request::new(Method::Post, "/hit").json(&Hit { slot: i }));
    }
    let mut app = loop_app(&sim);
    app.db_for_tests()
        .transact(|tx| {
            tx.enqueue(CountUp { slot: 7777 });
            Ok::<(), std::io::Error>(())
        })
        .unwrap();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    kernel.run(&mut source).unwrap();
    assert_eq!(
        kernel.stats().jobs_done,
        101,
        "every job ran despite the flood (100 request jobs + the seeded one)"
    );
}

/// T8b RULING 3 (task-8b-brief.md, C-9): the test above asserts EVENTUAL
/// completeness (`jobs_done == 101` once `run` reaches the horizon), which
/// is numerically IDENTICAL whether a request-arm win consumes 1 request or
/// 64 — post-batching, the 100-request flood drains in two `Ready::Request`
/// wins instead of a hundred, and the count at the end is 101 either way.
/// This is the INTERLEAVING invariant that can see the difference: the job
/// arm must be serviced at least once while the request source still has
/// outstanding work, not merely by the time the whole run winds down.
/// Driven via a `Kernel::step` loop rather than `run`, discriminating which
/// arm each step serviced from `kernel.stats()` deltas — `requests` for the
/// request arm, `jobs_done + jobs_retried + jobs_dead` for the job arm (the
/// same technique the request-arm swarm workload in `group_commit.rs` uses
/// to prove a batch was genuinely contended).
///
/// Deliberately NOT a bound on how many request-arm wins may precede the
/// job's service (RULING 3, option (b) over (a)): a bound pinned by a test
/// IS a contract whatever DESIGN says, DESIGN documents one entropy draw on
/// contention and nothing about service ratios, and greedy drain cannot
/// honour a fixed ratio on a fast device anyway (spec §8's own argument —
/// shallower queues mean smaller batches). What is asserted here is strictly
/// STRONGER than the test above (it catches a genuine regression to
/// "requests always win", which that one cannot — see the mutation proof in
/// the report) and commits the platform to nothing about degree. T11 records
/// the fairness-profile change in DESIGN as an OBSERVED property, not a
/// promise.
///
/// **`FLOOD` is 1024 for a measured reason — do not "simplify" it back
/// down.** The assertion is probabilistic in the flood size, because
/// arbitration is a uniform draw over the ready set
/// (`kernel/mod.rs:297-301`) and the flood drains in `ceil(FLOOD /
/// MAX_BATCH)` request-arm wins, so the job arm must win at least one of
/// that many coin flips. At the original `FLOOD` of 100 with `MAX_BATCH` of
/// 64 that is TWO draws — P(fail) = 0.25, and the T8b review measured
/// exactly that: **49 failures over 200 seeds against stock, correct
/// code**. At 1024 it is 16 draws, P(fail) ≈ 1.5e-5, measured 0 over 200
/// seeds. The runtime cost is negligible.
///
/// **The product observation that measurement exposed, recorded here
/// because it is the sharpest form of what T11 owes DESIGN.** The same
/// review measured the PRE-batching kernel (request arm draining exactly
/// one request per step) at 200/200 passes with `FLOOD` at 100. So
/// batching genuinely changed job-arm fairness from *certain* to
/// *probabilistic in the queue depth*: with a flood at or below
/// `MAX_BATCH`, a due job can legitimately wait behind the entire flood
/// about a quarter of the time. That is a real behavioural change, not a
/// defect — and raising `FLOOD` here makes this test assert an invariant
/// instead of a coin flip WITHOUT hiding the change, which is why the
/// measurement lives in this comment rather than only in a ledger.
#[test]
fn due_jobs_are_serviced_before_a_request_flood_exhausts_the_source() {
    /// Both the push count and the "source still has outstanding work"
    /// predicate below read this ONE constant: two independent literals
    /// could drift apart with no compile error and no failure at the site
    /// of the mistake, leaving the predicate either unsatisfiable or
    /// trivially satisfiable (T8b review M-5).
    const FLOOD: u64 = 1024;

    let sim = Sim::with_faults(0x60C1_0008, FaultConfig::none());
    let mut source = SimSource::new(Box::new(sim.clock()), 200 * SEC);
    for i in 0..FLOOD {
        // Same shape as the eventual-completeness test above: all requests
        // arrive at t=0, maximal contention with the due job.
        source.push_at(0, Request::new(Method::Post, "/hit").json(&Hit { slot: i }));
    }
    let mut app = loop_app(&sim);
    app.db_for_tests()
        .transact(|tx| {
            tx.enqueue(CountUp { slot: 7777 });
            Ok::<(), std::io::Error>(())
        })
        .unwrap();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();

    let mut serviced_while_source_open = false;
    loop {
        let before = kernel.stats();
        match kernel.step(&mut source).unwrap() {
            Step::Worked => {
                let after = kernel.stats();
                let job_delta = (after.jobs_done + after.jobs_retried + after.jobs_dead)
                    - (before.jobs_done + before.jobs_retried + before.jobs_dead);
                // `before.requests`, not `after.requests`: a step that
                // services the job arm leaves `requests` unchanged, so
                // either reading discriminates the SAME way for a Job step
                // — but for a Request step (`job_delta == 0` always) it
                // does not matter which is read. Using `before` throughout
                // keeps the reading uniform and answers the question this
                // test actually asks: was there still outstanding request
                // work the INSTANT the job ran?
                if job_delta > 0 && before.requests < FLOOD {
                    serviced_while_source_open = true;
                }
            }
            Step::Idle { deadline } => {
                if let Wake::Shutdown = source.wait_until(deadline) {
                    break;
                }
            }
        }
    }

    assert!(
        serviced_while_source_open,
        "the due job was serviced only AFTER the {FLOOD}-request flood had already fully \
         drained — the request arm starved it until the source was exhausted, exactly \
         the regression this test exists to catch (the eventual-completeness test above \
         cannot see this: its count is identical either way). Before concluding a \
         regression: this assertion is probabilistic in FLOOD, so a red here can also mean \
         FLOOD was lowered toward MAX_BATCH — see the doc comment"
    );
}

// ------------------------------------------------------------------- sweep

/// Mints a session the same way `http.rs`'s `login` does — the sweep test
/// only needs its own routes to avoid cross-file coupling between the two
/// integration test binaries.
#[route(POST "/sweep-login")]
fn sweep_login(tx: &mut Tx, subject: String, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_session(tx, subject))
}

#[route(GET "/sweep-whoami", auth)]
fn sweep_whoami(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let _ = tx;
    Ok(ctx.principal().expect("auth route").subject.clone())
}

/// Auth T6: the platform's built-in daily session-expiry sweep. Every `App`
/// gets it with no opt-in (like `Session`/`ApiKey`/`CronRow` themselves).
/// `session_ttl: 0` lets each login's own `now` argument set `expires_at`
/// directly, so the two sessions land squarely on either side of the
/// platform's real `Duration::from_secs(86_400)` cadence (`DAY`, in nanos)
/// without needing a test-only knob on the interval.
#[test]
fn session_sweep_deletes_expired_rows_on_tick() {
    let sim = Sim::with_faults(0x6B0, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig {
            session_ttl: 0,
            ..AppConfig::default()
        },
    )
    .unwrap()
    .mount(routes![sweep_login, sweep_whoami]);

    let login_a = app.handle_at(
        Request::new(Method::Post, "/sweep-login").json(&"alice"),
        100,
    ); // expires_at = 100 — long before the first tick at DAY
    assert_eq!(login_a.status, 200);
    let token_a: String = serde_json::from_slice(&login_a.body).unwrap();

    let login_b = app.handle_at(
        Request::new(Method::Post, "/sweep-login").json(&"bob"),
        2 * DAY,
    ); // expires_at = 2*DAY — long after the first tick
    assert_eq!(login_b.status, 200);

    assert_eq!(
        app.db()
            .snapshot()
            .unwrap()
            .scan::<Session>()
            .unwrap()
            .len(),
        2,
        "the sweep hasn't run yet — both rows are still on disk"
    );

    // Lazy resolution (T2) already refuses the expired token on its own,
    // with zero help from the sweep — the sweep is storage hygiene, not the
    // correctness mechanism.
    let denied = app.handle_at(
        Request::new(Method::Get, "/sweep-whoami")
            .header("Authorization", format!("Bearer {token_a}")),
        DAY / 2,
    );
    assert_eq!(
        denied.status, 401,
        "lazy expiry must already hide alice's row"
    );

    // Run the kernel forward past the platform's first daily tick: now=DAY
    // sits strictly between the two expiries (100 <= DAY < 2*DAY), so the
    // sweep's enqueued `before` deletes exactly alice's row.
    let mut source = SimSource::new(Box::new(sim.clock()), DAY + SEC);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    kernel.run(&mut source).unwrap();

    let app = kernel.into_app();
    let sessions = app.db().snapshot().unwrap().scan::<Session>().unwrap();
    assert_eq!(
        sessions.len(),
        1,
        "the expired row is swept; the live one survives"
    );
    assert_eq!(sessions[0].subject, "bob");
}

// ------------------------------------------------------------------- tail

use charpente::event;
use charpente::http::{Ctx, Cursor, Subscription};

#[event]
struct Bumped {
    n: u64,
}

#[derive(Serialize, Deserialize)]
struct EmitBody {
    n: u64,
}

#[route(POST "/emit")]
fn emit_one(tx: &mut Tx, body: EmitBody) -> Result<(), ApiError> {
    tx.emit(Bumped { n: body.n });
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct FloodBody {
    count: u64,
}

/// Emits `count` events in ONE transaction — the atomic multi-event commit
/// the retention tests need: `Db::checkpoint`'s GC only ever runs at the
/// cadence-driven checkpoint (never mid-transaction), so a single commit
/// that both mints many events AND crosses the checkpoint threshold is the
/// only way to move the retention floor without ever giving a live tail a
/// chance to interleave a delivery attempt in between.
#[route(POST "/flood")]
fn flood(tx: &mut Tx, body: FloodBody) -> Result<(), ApiError> {
    for i in 0..body.count {
        tx.emit(Bumped { n: i });
    }
    Ok(())
}

#[route(SSE "/feed")]
fn tail_feed(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<Bumped>().cursor(cursor))
}

fn tail_app(sim: &Sim, db: charpente::DbConfig) -> App {
    App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig {
            db,
            ..AppConfig::default()
        },
    )
    .unwrap()
    .mount(routes![tail_feed, emit_one, flood])
}

/// A `SimSink` stall closure: the first `n` calls succeed, every call after
/// that reports `Full`.
fn full_after(n: usize) -> Box<dyn FnMut() -> bool + Send> {
    let mut calls = 0usize;
    Box::new(move || {
        let full = calls >= n;
        calls += 1;
        full
    })
}

#[test]
fn a_tail_receives_matching_events_exactly_and_in_order() {
    let sim = Sim::with_faults(0x6A0, FaultConfig::none());
    let app = tail_app(&sim, charpente::DbConfig::default());
    let mut source = SimSource::new(Box::new(sim.clock()), 10 * SEC);
    source.push_at(0, Request::new(Method::Get, "/feed"));
    for (i, n) in [10u64, 20, 30].into_iter().enumerate() {
        source.push_at(
            (i as u64 + 1) * SEC,
            Request::new(Method::Post, "/emit").json(&EmitBody { n }),
        );
    }
    let sinks = source.sinks();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    kernel.run(&mut source).unwrap();

    let handles = sinks.lock().unwrap();
    assert_eq!(handles.len(), 1, "one SSE upgrade");
    let frames = handles[0].frames();
    assert_eq!(frames.len(), 3);
    let mut prev: Option<u64> = None;
    for (frame, n) in frames.iter().zip([10u64, 20, 30]) {
        assert_eq!(frame.name, "bumped");
        assert_eq!(frame.json, serde_json::to_string(&Bumped { n }).unwrap());
        if let Some(p) = prev {
            assert!(frame.id > p, "ids must strictly ascend");
        }
        prev = Some(frame.id);
    }
    // The subscribe wrote nothing (no seq consumed); each emit is its own
    // commit. Seq starts at 1, not 0: the platform's built-in daily
    // session-sweep cron (auth T6, every App gets one with no opt-in)
    // upserts its CronRow at kernel construction, consuming seq 0 before any
    // user transaction runs.
    assert_eq!(
        frames.iter().map(|f| f.seq).collect::<Vec<_>>(),
        vec![1, 2, 3],
        "frames carry the committing record's seq"
    );
}

#[test]
fn resume_from_cursor_delivers_the_successor_onward_no_dup_no_gap() {
    let sim = Sim::with_faults(0x6A1, FaultConfig::none());
    let app = tail_app(&sim, charpente::DbConfig::default());
    let mut source = SimSource::new(Box::new(sim.clock()), 20 * SEC);
    source.push_at(0, Request::new(Method::Get, "/feed"));
    for n in 0..5u64 {
        source.push_at(
            (n + 1) * SEC,
            Request::new(Method::Post, "/emit").json(&EmitBody { n }),
        );
    }
    let sinks = source.sinks();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    kernel.run(&mut source).unwrap();

    // The wire cursor for "event 2" comes straight from what the first tail
    // actually received — no peeking at the raw event table.
    let cursor_wire = {
        let handles = sinks.lock().unwrap();
        let frames = handles[0].frames();
        assert_eq!(frames.len(), 5);
        Cursor {
            id: frames[2].id,
            seq: frames[2].seq,
        }
        .wire()
    };

    source.push_at(
        6 * SEC,
        Request::new(Method::Get, format!("/feed?cursor={cursor_wire}")),
    );
    source.push_at(
        7 * SEC,
        Request::new(Method::Post, "/emit").json(&EmitBody { n: 5 }),
    );
    kernel.run(&mut source).unwrap();

    let handles = sinks.lock().unwrap();
    assert_eq!(handles.len(), 2, "two SSE upgrades");
    let resumed_ids: Vec<u64> = handles[1].frames().iter().map(|f| f.id).collect();
    assert_eq!(
        resumed_ids,
        vec![3, 4, 5],
        "no dup, no gap, then the live one"
    );
}

#[test]
fn cursor_below_floor_and_diverged_cursor_are_named_409s() {
    let sim = Sim::with_faults(0x6A2, FaultConfig::none());
    let db_cfg = charpente::DbConfig {
        event_retention: 2,
        checkpoint_every: 1,
        ..charpente::DbConfig::default()
    };
    let app = tail_app(&sim, db_cfg);
    let mut source = SimSource::new(Box::new(sim.clock()), 10 * SEC);
    // One commit, ten events, checkpoint_every=1 fires right after: floor
    // lands at 10 - 2 = 8 before any subscribe is ever evaluated.
    source.push_at(
        0,
        Request::new(Method::Post, "/flood").json(&FloodBody { count: 10 }),
    );
    source.push_at(SEC, Request::new(Method::Get, "/feed?cursor=1.0"));
    source.push_at(2 * SEC, Request::new(Method::Get, "/feed?cursor=999.1"));
    source.push_at(
        3 * SEC,
        Request::new(Method::Get, format!("/feed?cursor=8.{}", u64::MAX)),
    );
    let responses = source.responses();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    kernel.run(&mut source).unwrap();

    let responses = responses.lock().unwrap();
    assert_eq!(responses.len(), 4);
    assert_eq!(responses[0].0, 200, "the flood commits");
    let body = |i: usize| String::from_utf8(responses[i].1.clone()).unwrap();
    assert_eq!(responses[1].0, 409, "{}", body(1));
    assert!(body(1).contains("retention floor"), "{}", body(1));
    assert_eq!(responses[2].0, 409, "{}", body(2));
    assert!(
        body(2).contains("diverged") && body(2).contains("next event id"),
        "{}",
        body(2)
    );
    assert_eq!(responses[3].0, 409, "{}", body(3));
    assert!(body(3).contains("not"), "{}", body(3));
}

#[test]
fn a_tail_behind_retention_gets_the_gap_frame_then_closes() {
    let sim = Sim::with_faults(0x6A3, FaultConfig::none());
    let db_cfg = charpente::DbConfig {
        event_retention: 2,
        checkpoint_every: 1,
        ..charpente::DbConfig::default()
    };
    let app = tail_app(&sim, db_cfg);
    let mut source = SimSource::new(Box::new(sim.clock()), 10 * SEC);
    source.push_at(0, Request::new(Method::Get, "/feed")); // from now: next_id = 0
    source.push_at(
        SEC,
        Request::new(Method::Post, "/emit").json(&EmitBody { n: 99 }),
    );
    // One atomic commit of 5 more events + its checkpoint jumps the floor to
    // 6 - 2 = 4, past the tail's stalled position (1) — no interleaved
    // delivery attempt is possible in between (see `flood`'s doc comment).
    source.push_at(
        2 * SEC,
        Request::new(Method::Post, "/flood").json(&FloodBody { count: 5 }),
    );
    let sinks = source.sinks();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    kernel.run(&mut source).unwrap();

    let handles = sinks.lock().unwrap();
    assert_eq!(handles.len(), 1);
    let frames = handles[0].frames();
    assert_eq!(frames.len(), 2, "one real frame, then the terminal gap");
    assert_eq!(frames[0].id, 0);
    let last = &frames[1];
    assert_eq!(last.name, "charpente.gap");
    assert!(last.json.contains("\"floor\":4"), "{}", last.json);
    assert!(last.json.contains("\"tail\":1"), "{}", last.json);
    assert!(
        last.json.contains("reconnect without a cursor"),
        "{}",
        last.json
    );
    assert!(handles[0].closed());
}

#[test]
fn a_full_sink_drops_the_tail_and_resume_recovers() {
    let sim = Sim::with_faults(0x6A4, FaultConfig::none());
    let app = tail_app(&sim, charpente::DbConfig::default());
    let mut source = SimSource::new(Box::new(sim.clock()), 20 * SEC);
    source.push_stall(full_after(2)); // the 3rd send (index 2) reports Full
    source.push_at(0, Request::new(Method::Get, "/feed"));
    for n in 0..3u64 {
        source.push_at(
            (n + 1) * SEC,
            Request::new(Method::Post, "/emit").json(&EmitBody { n }),
        );
    }
    let sinks = source.sinks();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    kernel.run(&mut source).unwrap();

    let resume_cursor = {
        let handles = sinks.lock().unwrap();
        assert_eq!(handles.len(), 1);
        let frames = handles[0].frames();
        assert_eq!(frames.len(), 2, "the 3rd send was Full — the tail dropped");
        assert!(handles[0].closed());
        Cursor {
            id: frames[1].id,
            seq: frames[1].seq,
        }
        .wire()
    };

    // Two more events land (ids 3, 4) while the tail is dead.
    source.push_at(
        4 * SEC,
        Request::new(Method::Post, "/emit").json(&EmitBody { n: 3 }),
    );
    source.push_at(
        5 * SEC,
        Request::new(Method::Post, "/emit").json(&EmitBody { n: 4 }),
    );
    source.push_at(
        6 * SEC,
        Request::new(Method::Get, format!("/feed?cursor={resume_cursor}")),
    );
    kernel.run(&mut source).unwrap();

    let handles = sinks.lock().unwrap();
    assert_eq!(handles.len(), 2);
    let resumed_ids: Vec<u64> = handles[1].frames().iter().map(|f| f.id).collect();
    assert_eq!(
        resumed_ids,
        vec![2, 3, 4],
        "resume recovers events 3,4,5 (ids 2,3,4)"
    );
}

// -------------------------------------------------------- T7: migration gate

/// A hand-rolled v1 shape sharing table name "Gadget" with the
/// `#[table(version = 2)]` `Gadget` below — migration_gate.rs's dual-impl
/// pattern: the `#[table]` macro always ties `TABLE` to the Rust struct
/// identifier, so a from-scratch v1 fixture under a different Rust name
/// needs a hand-written `Row`+`RowShape` pair to land on the same table.
struct GadgetV1 {
    id: Id<GadgetV1>,
    count: u32,
}
impl Row for GadgetV1 {
    const TABLE: &'static str = "Gadget";
    const INDEXES: &'static [IndexDef<GadgetV1>] = &[];
    const VERSION: u32 = 1;
    const FINGERPRINT: Option<u64> = Some(0x5EED_0001);
    fn id(&self) -> Id<GadgetV1> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.count.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> std::io::Result<GadgetV1> {
        let input = &mut &bytes[..];
        Ok(GadgetV1 {
            id: Id::decode(input)?,
            count: u32::decode(input)?,
        })
    }
}
impl RowShape for GadgetV1 {
    const VERSION: u32 = 1;
    fn encode_row(&self, out: &mut Vec<u8>) {
        Row::encode_row(self, out)
    }
    fn decode_row(bytes: &[u8]) -> std::io::Result<GadgetV1> {
        <GadgetV1 as Row>::decode_row(bytes)
    }
}

#[table(version = 2)]
struct Gadget {
    id: Id<Gadget>,
    count: u32,
    label: String,
}
impl TableMigration for Gadget {
    type Prev = GadgetV1;
    fn migrate(prev: GadgetV1) -> Result<Gadget, MigrationError> {
        Ok(Gadget {
            id: Id::from_raw(prev.id.raw()),
            count: prev.count,
            label: "migrated".to_string(),
        })
    }
}

#[test]
fn kernel_migrates_the_schema_before_the_first_request_is_served() {
    let sim = Sim::with_faults(0x2EB7_0007, FaultConfig::none());
    // Seed a v1 store via a bare Db, clean-shutdown checkpointed — the
    // fixture the kernel below must migrate before it serves anything.
    let mut seed_db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<GadgetV1>(),
    )
    .unwrap();
    seed_db
        .transact(|tx| -> Result<(), ()> {
            for i in 0..3u64 {
                tx.put(&GadgetV1 {
                    id: Id::from_raw(i),
                    count: i as u32,
                });
            }
            Ok(())
        })
        .unwrap();
    seed_db.checkpoint().unwrap(); // clean shutdown: the tail is drained
    drop(seed_db);

    // Build the app/kernel against the v2 schema + migration, then run:
    // Kernel::run's startup gate must migrate every row before the first
    // (only) scripted request is served.
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new()
            .table::<Gadget>()
            .migration::<Gadget>("Gadget"),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let mut source = SimSource::new(Box::new(sim.clock()), SEC);
    kernel.run(&mut source).unwrap();

    let mut app = kernel.into_app();
    let mut rows: Vec<Gadget> = app.db_for_tests().snapshot().unwrap().scan().unwrap();
    rows.sort_by_key(|r| r.id.raw());
    assert_eq!(rows.len(), 3);
    for row in &rows {
        assert_eq!(row.label, "migrated");
        assert_eq!(row.count, row.id.raw() as u32);
    }
}
