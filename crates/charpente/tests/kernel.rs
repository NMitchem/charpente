//! Kernel-layer tests (PLAN M0 step 6): #[job] surface, dispatcher, cron,
//! the event loop, and the step-6 swarm properties.

use charpente::http::{App, AppConfig};
use charpente::kernel::{Dispatcher, JobDef, JobEntry, JobError, JobOutcome};
use charpente::{Schema, Tx, job, jobs, table};
use charpente_core::db::Job;
use charpente_core::job::{JobRow, JobStatus};
use charpente_core::table::{Id, View};
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
use charpente::http::{Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente::route;
use charpente::routes;
use serde::{Deserialize, Serialize};

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
