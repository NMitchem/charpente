//! `charpente_sim::drive`: the faulted phase, the fault-free tail and the
//! re-sends, one behaviour per step. Each test runs a tiny app on a pinned
//! seed, found by a bounded search (seeds 0xD7_0000 and up), and the comment
//! on each seed says what that seed shows. A pinned seed names a universe:
//! each test also asserts the case its seed was chosen for, so a change that
//! renames the universe fails on that assertion instead of passing on
//! another case.

use std::cell::Cell;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use charpente::auth::ApiKey;
use charpente::core::io::{
    Clock, Disk, File, HttpMethod, IdempotencyKey as CallKey, Outbound, OutboundCall,
    OutboundReply, WallTime,
};
use charpente::core::job::{JobRow, JobStatus};
use charpente::http::{ApiError, App, AppConfig, Method, Request};
use charpente::kernel::JobError;
use charpente::outbound::{Call, Host, IdempotencyKey, Reply};
use charpente::{Id, Schema, Snapshot, Tx, View, job, jobs, route, routes, table};
use charpente_sim::checks::{
    clock_reopens_after, durable_corruption_fired, is_fault_fallout, keys_are_per_effect,
    outbound_settles_by_horizon,
};
use charpente_sim::drive::{Drive, Phase, TAIL_SETTLE_CAP};
use charpente_sim::outbound::Remote;
use charpente_sim::rng::Rng;
use charpente_sim::{FaultConfig, Sim, SimDisk};

#[table(global)]
struct Note {
    id: Id<Note>,
    n: u64,
}

/// Note 400 is the app's own 400, note 500 its own 500 (a handler's error,
/// not fault fallout); note 401 is the app's own 400 whose message holds a
/// word the fallout classifier matches; any other note is stored.
#[route(POST "/notes/:id", public)]
fn add_note(tx: &mut Tx, id: Id<Note>) -> Result<Id<Note>, ApiError> {
    match id.raw() {
        400 => Err(ApiError::BadRequest("note 400 is refused".to_string())),
        401 => Err(ApiError::BadRequest(
            "note 401 was injected twice".to_string(),
        )),
        500 => Err(ApiError::Internal("the ledger is closed".to_string())),
        n => {
            tx.put(&Note { id, n });
            Ok(id)
        }
    }
}

fn notes(sim: &Sim) -> io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![add_note]))
}

fn post(id: u64) -> Request {
    Request::new(Method::Post, format!("/notes/{id}"))
}

/// The requests the disk-fault tests send: the app's 400, its own 500, then
/// six notes, one millisecond apart. The seeds below were searched with
/// exactly this list.
const NOTES: [u64; 8] = [400, 500, 1, 2, 3, 4, 5, 6];

fn posts(ids: &[u64]) -> Vec<(Duration, Request)> {
    ids.iter()
        .enumerate()
        .map(|(i, id)| (Duration::from_millis(i as u64), post(*id)))
        .collect()
}

/// Injected write and sync errors at `rate`, on top of the default crash
/// model.
fn disk_faults(rate: f64) -> FaultConfig {
    FaultConfig {
        write_error: rate,
        sync_error: rate,
        ..FaultConfig::default()
    }
}

/// Each answer's status, with `f` marking a 5xx whose body is fault fallout:
/// `"400"`, `"500f"`, `"-"` for none.
fn shape(answers: &[Option<(u16, Vec<u8>)>]) -> Vec<String> {
    answers
        .iter()
        .map(|answer| match answer {
            None => "-".to_string(),
            Some((status, body)) if is_fault_fallout(&String::from_utf8_lossy(body)) => {
                format!("{status}f")
            }
            Some((status, _)) => status.to_string(),
        })
        .collect()
}

/// An `Id` answers as a JSON string.
fn id_body(id: u64) -> Vec<u8> {
    format!("\"{id}\"").into_bytes()
}

/// Always fails, so its retries must run out in the tail: a job the tail
/// carries to Dead.
#[job(global, retain_dead, retry(max = 3, backoff = "1s"))]
struct Doomed {
    note: Id<Note>,
}

impl Doomed {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Err(JobError::app(format!("note {} is doomed", self.note.raw())))
    }
}

/// The 1 s schedule's job.
#[job(global, retain_dead)]
struct Beat {
    n: u64,
}

impl Beat {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

#[route(POST "/doomed/:id", public)]
fn doom(tx: &mut Tx, id: Id<Note>) -> Result<Id<Note>, ApiError> {
    tx.enqueue(Doomed { note: id });
    Ok(id)
}

fn scheduled(sim: &Sim) -> io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![doom])
    .mount_jobs(jobs![Doomed, Beat])
    .schedule("beat", Duration::from_secs(1), || Beat { n: 0 }))
}

/// The notes app with the 1 s beat. The beat's schedule row is what records
/// a wall time in the store, so a clock left behind it is refused at open.
fn ticking(sim: &Sim) -> io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![add_note])
    .mount_jobs(jobs![Beat])
    .schedule("beat", Duration::from_secs(1), || Beat { n: 0 }))
}

/// One mail per note, to a declared host.
#[job(
    global,
    retain_dead,
    outbound = "mail.example",
    retry(max = 8, backoff = "500ms"),
    timeout = "2s"
)]
struct Mail {
    note: Id<Note>,
}

impl Mail {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(
            Call::post("/send")
                .header("idempotency-key", key.as_str())
                .form(&[("note", self.note.raw().to_string())]),
        ))
    }

    fn settle(self, _tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        match reply.status() {
            200 => Ok(()),
            s => Err(JobError::app(format!("mail.example answered {s}"))),
        }
    }
}

#[route(POST "/mail/:id", public)]
fn send_mail(tx: &mut Tx, id: Id<Note>) -> Result<Id<Note>, ApiError> {
    tx.enqueue(Mail { note: id });
    Ok(id)
}

fn mailer(sim: &Sim) -> io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![send_mail])
    .mount_jobs(jobs![Mail])
    .outbound(Host::new("mail.example")))
}

fn mails(ids: std::ops::RangeInclusive<u64>) -> Vec<(Duration, Request)> {
    ids.map(|id| {
        (
            Duration::from_millis(id),
            Request::new(Method::Post, format!("/mail/{id}")),
        )
    })
    .collect()
}

/// Answers 200 and counts every call that reaches it.
struct Counting(Arc<AtomicU64>);

impl Remote for Counting {
    fn handle(&mut self, _call: &OutboundCall, _rng: &mut Rng) -> OutboundReply {
        self.0.fetch_add(1, Ordering::Relaxed);
        OutboundReply {
            status: 200,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }
}

/// Every outbound fault the sim has injected so far.
fn outbound_faults(sim: &Sim) -> u64 {
    let coverage = sim.coverage();
    [
        "fault:outbound_refused",
        "fault:outbound_lost_request",
        "fault:outbound_lost_reply",
        "fault:outbound_throttle",
        "fault:outbound_slow",
    ]
    .iter()
    .map(|key| coverage.get(key).copied().unwrap_or(0))
    .sum()
}

fn job_statuses(snap: &Snapshot) -> Vec<JobStatus> {
    snap.scan::<JobRow>()
        .unwrap()
        .into_iter()
        .map(|row| row.status)
        .collect()
}

#[test]
fn a_fallout_crash_in_the_faulted_phase_is_not_a_skip() {
    // 0xD7_0005 at rate 0.05: the app boots and answers five requests, then a
    // commit meets an injected fault: notes 2 and 3 get the fallout 500, the
    // poisoned kernel stops, and notes 4 to 6 go unanswered.
    let sim = Sim::with_faults(0xD7_0005, disk_faults(0.05));
    let drive = Drive::new(&sim, notes);
    let phase = drive
        .faulted_phase(&posts(&NOTES), Duration::from_secs(1))
        .expect("fault fallout ends the phase, never fails it");
    assert_eq!(
        shape(&phase.answers),
        ["400", "500", "200", "500f", "500f", "-", "-", "-"],
        "the seed's case: the kernel stopped on fault fallout mid-phase"
    );
    assert_eq!(
        sim.gave_up(),
        None,
        "a crash in the faulted phase is no give-up"
    );
    let recovered = drive.recover(&phase, Duration::from_secs(1)).unwrap();
    assert!(
        recovered.is_some(),
        "the tail runs after a crash in the faulted phase: it is not a skip"
    );
    assert_eq!(sim.gave_up(), None);
}

#[test]
fn a_first_boot_lost_to_fallout_still_reaches_the_tail_and_resends_everything() {
    // 0xD7_0000 at rate 0.3: all eight boots of the faulted phase meet an
    // injected fault (`boot:exhausted`), so nothing is sent before the tail.
    let sim = Sim::with_faults(0xD7_0000, disk_faults(0.3));
    let drive = Drive::new(&sim, notes);
    let phase = drive
        .faulted_phase(&posts(&NOTES), Duration::from_secs(1))
        .unwrap();
    assert_eq!(
        sim.coverage().get("boot:exhausted").copied(),
        Some(1),
        "the seed's case: the first boot never got past fallout"
    );
    assert!(phase.answers.iter().all(Option::is_none), "an empty phase");
    assert_eq!(sim.gave_up(), None, "a lost first boot is no give-up");
    let recovered = drive
        .recover(&phase, Duration::from_secs(1))
        .unwrap()
        .expect("the fault-free tail boots");
    let resent: Vec<Option<u16>> = (0..NOTES.len()).map(|i| recovered.status(i)).collect();
    assert_eq!(
        resent,
        [400, 500, 200, 200, 200, 200, 200, 200].map(Some),
        "every request is sent in the tail, the 400 and the 500 included"
    );
    let snap = recovered.kernel.app().db().snapshot().unwrap();
    for n in 1..=6 {
        assert!(
            snap.get::<Note>(Id::from_raw(n)).unwrap().is_some(),
            "note {n}"
        );
    }
    assert_eq!(sim.gave_up(), None);
}

#[test]
fn only_unanswered_or_fallout_answers_are_resent() {
    // 0xD7_000D at rate 0.05: the phase holds every kind of answer: the app's
    // 400, its own 500 ("the ledger is closed"), a 200, two fallout 500s and
    // three unanswered requests. Re-sending every non-200 would re-send the
    // 400 and the 500; re-sending every 5xx would re-send the 500; re-sending
    // only the unanswered would skip the fallout 500s.
    let sim = Sim::with_faults(0xD7_000D, disk_faults(0.05));
    let drive = Drive::new(&sim, notes);
    let phase = drive
        .faulted_phase(&posts(&NOTES), Duration::from_secs(1))
        .unwrap();
    assert_eq!(
        shape(&phase.answers),
        ["400", "500", "200", "500f", "500f", "-", "-", "-"],
        "the seed's case"
    );
    let own_500 = &phase.answers[1].as_ref().unwrap().1;
    assert_eq!(own_500, br#"{"error":"the ledger is closed"}"#);
    let recovered = drive
        .recover(&phase, Duration::from_secs(1))
        .unwrap()
        .expect("the fault-free tail boots");
    assert_eq!(
        shape(&recovered.resent),
        ["-", "-", "-", "200", "200", "200", "200", "200"],
        "only the fallout 500s and the unanswered are re-sent"
    );
}

#[test]
fn an_apps_own_4xx_is_not_resent_even_when_its_body_reads_as_fallout() {
    // No faults: the case is the app's own 400 whose message holds a word the
    // fallout classifier matches ("injected"), which any seed shows. Only the
    // re-send rule's 5xx bound keeps it from being re-sent.
    let sim = Sim::with_faults(0xD7_6000, FaultConfig::none());
    let drive = Drive::new(&sim, notes);
    let phase = drive
        .faulted_phase(&posts(&[401, 1]), Duration::from_secs(1))
        .unwrap();
    assert_eq!(
        shape(&phase.answers),
        ["400f", "200"],
        "the case: the app's own 400 reads as fallout to the classifier"
    );
    assert_eq!(
        phase.answers[0].as_ref().unwrap().1,
        br#"{"error":"note 401 was injected twice"}"#
    );
    let recovered = drive
        .recover(&phase, Duration::from_secs(1))
        .unwrap()
        .expect("the fault-free tail boots");
    assert_eq!(
        recovered.resent,
        [None, None],
        "a 4xx is the app's answer, whatever its body says"
    );
}

#[test]
fn resent_answers_are_reported_apart() {
    // 0xD7_0019 at rate 0.05: the first four requests are answered (400, 500,
    // 200, 200), then two fallout 500s and two unanswered. The re-sent run
    // starts at index 4, so an answer filed under its position among the
    // re-sends (0 to 3) instead of its request would land on an answered one.
    let sim = Sim::with_faults(0xD7_0019, disk_faults(0.05));
    let drive = Drive::new(&sim, notes);
    let phase = drive
        .faulted_phase(&posts(&NOTES), Duration::from_secs(1))
        .unwrap();
    assert_eq!(
        shape(&phase.answers),
        ["400", "500", "200", "200", "500f", "500f", "-", "-"],
        "the seed's case"
    );
    let recovered = drive
        .recover(&phase, Duration::from_secs(1))
        .unwrap()
        .expect("the fault-free tail boots");
    assert_eq!(recovered.resent.len(), NOTES.len());
    for (i, id) in NOTES.iter().enumerate() {
        let want = (i >= 4).then(|| (200, id_body(*id)));
        assert_eq!(recovered.resent[i], want, "request {i} (note {id})");
    }
}

#[test]
fn a_durable_refusal_in_the_tail_gives_the_run_up() {
    // 0xD7_1000 with only bit_flip on, at 1.0: the phase runs fault-free and
    // ends clean, which checkpoints, and the tail's power cycle flips a bit at
    // or below the store's horizon. Every seed of 0xD7_1000..+60 does this.
    let sim = Sim::with_faults(
        0xD7_1000,
        FaultConfig {
            bit_flip: 1.0,
            ..FaultConfig::none()
        },
    );
    let drive = Drive::new(&sim, notes);
    let phase = drive
        .faulted_phase(&posts(&[1, 2, 3, 4, 5, 6]), Duration::from_secs(1))
        .unwrap();
    assert!(
        phase.answers.iter().all(|a| matches!(a, Some((200, _)))),
        "the seed's case: the refusal is the tail's, after a clean phase"
    );
    let recovered = drive.recover(&phase, Duration::from_secs(1)).unwrap();
    assert!(recovered.is_none(), "a durable refusal gives the run up");
    assert!(durable_corruption_fired(&sim));
    assert_eq!(sim.coverage().get("boot:durable-refusal").copied(), Some(1));
    assert_eq!(
        sim.gave_up(),
        Some(
            "the boot was refused for durable corruption at or below the store's horizon, \
             which no restart clears, so nothing past it was adjudicated"
        )
    );
}

#[test]
fn a_clock_stop_ends_the_phase_and_the_tail_waits_out_the_refusal() {
    // 0xD7_8006 with only the clock's step-back (0.02 a read, up to 120 s)
    // and downtime (0.5 a power cycle, up to 3 days) axes on: 15 of the 30
    // notes are answered, then one step back beyond the 60 s tolerance stops
    // the kernel ("clock went backwards"), which only `clock_fallout` calls
    // fallout. The tail's power cycle draws no downtime jump, so the beat's
    // recorded time is still more than the tolerance ahead of the clock and
    // the tail's kernel is refused at open; the tail waits that out once. 50
    // of 0xD7_8000..+200 show both.
    let sim = Sim::with_faults(
        0xD7_8006,
        FaultConfig {
            clock_step_back: (0.02, Duration::from_secs(120)),
            clock_downtime: (0.5, Duration::from_secs(3 * 86_400)),
            ..FaultConfig::none()
        },
    );
    let drive = Drive::new(&sim, ticking);
    let requests: Vec<(Duration, Request)> = (1..=30)
        .map(|n| (Duration::from_millis(n * 100), post(n)))
        .collect();
    let phase = drive
        .faulted_phase(&requests, Duration::from_secs(4))
        .expect("a clock stop the run's clock faults explain ends the phase, never fails it");
    let cov = |key: &str| sim.coverage().get(key).copied();
    assert_eq!(
        shape(&phase.answers),
        [vec!["200"; 15], vec!["-"; 15]].concat(),
        "the seed's case: the kernel stopped mid-phase"
    );
    assert_eq!(cov("clock:stepped-back:beyond-tolerance"), Some(1));
    assert_eq!(cov("fault:write_error"), None, "no disk fault ran");
    assert_eq!(cov("restart:process"), None);
    assert_eq!(
        sim.gave_up(),
        None,
        "a clock stop in the phase is no give-up"
    );
    let recovered = drive
        .recover(&phase, Duration::from_secs(1))
        .expect("the tail's refusal at open is waited out, not a failure")
        .expect("the tail boots after one wait");
    assert_eq!(
        cov("restart:process"),
        Some(1),
        "the seed's case: the tail was refused once and restarted after the wait"
    );
    assert_eq!(cov("clock:downtime"), None);
    assert_eq!(sim.gave_up(), None);
    assert_eq!(
        shape(&recovered.resent),
        [vec!["-"; 15], vec!["200"; 15]].concat(),
        "the tail re-sends the 15 the stop left unanswered"
    );
}

/// A tail the clock keeps refusing fails the run. Each build stamps an API
/// key an hour ahead of the node's clock, so the kernel's open is refused
/// again after every wait. A real store's refusal clears after one wait, so a
/// third is a platform clock defect, never a give-up.
#[test]
fn a_tail_the_clock_keeps_refusing_fails_the_run() {
    // Only the clock's skew is on, and it acts at the build's first read, so
    // `clock_fallout` excuses every refusal and each is waited out. Every
    // boot is refused by construction, so any seed with a skew shows it.
    let sim = Sim::with_faults(
        0xD7_9000,
        FaultConfig {
            clock_skew: 90_000_000_000,
            ..FaultConfig::none()
        },
    );
    let boots = Cell::new(0u64);
    let drive = Drive::new(&sim, |sim: &Sim| {
        boots.set(boots.get() + 1);
        let mut app = notes(sim)?;
        let ahead = WallTime::from_nanos(sim.clock().wall().as_nanos() + 3_600_000_000_000);
        app.transact_for_tests(|tx| {
            tx.put(&ApiKey {
                id: Id::from_raw(boots.get()),
                key_hash: String::new(),
                subject: "ahead".to_string(),
                created_at: ahead,
            });
            Ok::<(), io::Error>(())
        })
        .map_err(|e| io::Error::other(format!("{e:?}")))?;
        Ok(app)
    });
    let phase = Phase {
        requests: vec![post(1)],
        answers: vec![None],
    };
    let err = match drive.recover(&phase, Duration::from_secs(1)) {
        Err(err) => err,
        Ok(recovered) => panic!(
            "a tail the clock refuses after two waits fails the run, but it booted: {}, gave up: \
             {:?}",
            recovered.is_some(),
            sim.gave_up()
        ),
    };
    let cov = |key: &str| sim.coverage().get(key).copied();
    assert_eq!(cov("clock:skewed"), Some(1), "the case: a clock axis acted");
    assert_eq!(
        (boots.get(), cov("restart:process")),
        (3, Some(2)),
        "three refusals: two waits, each with its restart, and no third"
    );
    let defect = "the boot was refused for the clock a third time, after two waits past the \
                  instant each refusal named. One wait always suffices, since nothing moves the \
                  clock or the recorded time in between, so this is a platform clock defect, \
                  not the app's: report it with this run's seed. The third refusal: the \
                  fault-free tail could not boot: ";
    let refusal = err
        .strip_prefix(defect)
        .unwrap_or_else(|| panic!("the text names the defect: {err}"));
    assert!(
        refusal.starts_with("refusing to open: this node's wall clock reads ")
            && clock_reopens_after(refusal).is_some(),
        "the refusal's own text follows, once: {refusal}"
    );
}

#[test]
fn the_tail_runs_to_its_horizon_so_cron_and_retries_drain() {
    // 0xD7_3000 with no faults: the case is the tail's horizon, which no fault
    // touches. The phase ends at 500 ms, before Doomed's first retry and the
    // first beat, so both are the tail's to drain.
    let sim = Sim::with_faults(0xD7_3000, FaultConfig::none());
    let drive = Drive::new(&sim, scheduled);
    let phase = drive
        .faulted_phase(
            &[(Duration::ZERO, Request::new(Method::Post, "/doomed/7"))],
            Duration::from_millis(500),
        )
        .unwrap();
    assert_eq!(phase.status(0), Some(200));
    let recovered = drive
        .recover(&phase, Duration::from_secs(30))
        .unwrap()
        .expect("the fault-free tail boots");
    let stats = recovered.kernel.stats();
    assert!(
        stats.cron_ticks >= 29,
        "a 1 s schedule over a 30 s tail ticks at least 29 times: {stats:?}"
    );
    assert_eq!(stats.jobs_dead, 1, "Doomed's retries ran out in the tail");
    let snap = recovered.kernel.app().db().snapshot().unwrap();
    let statuses = job_statuses(&snap);
    assert!(
        statuses
            .iter()
            .all(|s| matches!(s, JobStatus::Done | JobStatus::Dead)),
        "no job is left Pending at the tail's horizon: {statuses:?}"
    );
}

#[test]
fn an_outbound_app_without_a_registered_remote_names_the_fix() {
    // No faults: the case is the workload's missing sim.remote, which any
    // seed shows.
    let want = "the app calls mail.example, but the workload registered no model for it — \
                call sim.remote(\"mail.example\", …) before Drive::faulted_phase";
    let sim = Sim::with_faults(0xD7_4000, FaultConfig::none());
    let drive = Drive::new(&sim, mailer);
    let err = drive
        .faulted_phase(&mails(1..=1), Duration::from_secs(1))
        .err()
        .expect("the faulted phase cannot build an outbound kernel");
    assert_eq!(err, want);
    // A phase whose first boot was lost reaches the tail with the same gap.
    let phase = Phase {
        requests: mails(1..=1).into_iter().map(|(_, r)| r).collect(),
        answers: vec![None],
    };
    let err = drive
        .recover(&phase, Duration::from_secs(1))
        .err()
        .expect("the tail cannot build an outbound kernel either");
    assert_eq!(err, want);
}

#[test]
fn the_tail_quiesces_outbound_only_when_the_app_declares_hosts() {
    // An app that declares a host. 0xD7_2009: the phase meets nine outbound
    // faults, and five calls reach the remote in the tail, the most of
    // 0xD7_2000..+20, so a tail left noisy would almost surely fault again.
    let sim = Sim::with_faults(
        0xD7_2009,
        FaultConfig {
            outbound_refused: 0.3,
            outbound_lost_request: 0.2,
            outbound_lost_reply: 0.2,
            outbound_throttle: 0.2,
            outbound_latency: (Duration::from_millis(1), Duration::from_millis(20)),
            ..FaultConfig::none()
        },
    );
    let calls = Arc::new(AtomicU64::new(0));
    sim.remote("mail.example", Counting(Arc::clone(&calls)));
    let drive = Drive::new(&sim, mailer);
    let phase = drive
        .faulted_phase(&mails(1..=6), Duration::from_secs(1))
        .unwrap();
    let phase_faults = outbound_faults(&sim);
    let phase_calls = calls.load(Ordering::Relaxed);
    assert_eq!(phase_faults, 9, "the seed's case: the phase is noisy");
    let recovered = drive
        .recover(&phase, Duration::from_secs(300))
        .unwrap()
        .expect("the fault-free tail boots");
    assert_eq!(
        calls.load(Ordering::Relaxed) - phase_calls,
        5,
        "the seed's case: the tail calls out"
    );
    assert_eq!(
        outbound_faults(&sim),
        phase_faults,
        "no outbound fault fires in the tail of an app that declares a host"
    );
    let snap = recovered.kernel.app().db().snapshot().unwrap();
    assert_eq!(job_statuses(&snap), [JobStatus::Done; 6]);

    // An app that declares none. Every call is refused (rate 1.0), so the
    // seed does not matter: a call started after its tail is refused unless
    // the tail switched outbound faults off.
    let sim = Sim::with_faults(
        0xD7_2100,
        FaultConfig {
            outbound_refused: 1.0,
            ..FaultConfig::none()
        },
    );
    sim.remote("probe.example", Counting(Arc::new(AtomicU64::new(0))));
    let drive = Drive::new(&sim, notes);
    let phase = drive
        .faulted_phase(&posts(&[1]), Duration::from_secs(1))
        .unwrap();
    drive
        .recover(&phase, Duration::from_secs(1))
        .unwrap()
        .expect("the fault-free tail boots");
    let probe = sim.outbound(&["probe.example"]).unwrap();
    probe
        .start(
            0,
            OutboundCall {
                host: "probe.example",
                method: HttpMethod::Post,
                path: "/".to_string(),
                headers: Vec::new(),
                body: Vec::new(),
                key: CallKey::from_bytes([0; 16]),
            },
        )
        .unwrap();
    assert_eq!(
        sim.coverage().get("fault:outbound_refused").copied(),
        Some(1),
        "the tail of an app with no hosts leaves outbound faults on"
    );
}

#[test]
#[should_panic(
    expected = "contract: Drive::recover's phase must hold one answer per request — pass the \
                Phase Drive::faulted_phase returned, or one answer (None to send it) per request"
)]
fn recover_refuses_a_phase_without_one_answer_per_request() {
    // No faults: the contract panics before the sim runs anything, so any
    // seed shows it.
    let sim = Sim::with_faults(0xD7_5000, FaultConfig::none());
    let drive = Drive::new(&sim, notes);
    let phase = Phase {
        requests: vec![post(1), post(2)],
        answers: vec![None],
    };
    let _ = drive.recover(&phase, Duration::from_secs(1));
}

/// A request past the faulted phase's horizon would never be sent under
/// faults: the phase's source stops at the horizon, its answer stays `None`,
/// and only the fault-free tail sends it. A request exactly at the horizon is
/// still served, so the contract is on the last offset alone.
#[test]
#[should_panic(
    expected = "contract: Drive::faulted_phase's last request is 1.5s from the phase's start, \
                past its 1s horizon, so the faulted phase would never send it and only the \
                fault-free tail would — raise the horizon past the last request, or space the \
                requests closer"
)]
fn faulted_phase_refuses_a_request_past_its_horizon() {
    // No faults: the contract panics before the sim runs anything, so any
    // seed shows it.
    let sim = Sim::with_faults(0xD7_5001, FaultConfig::none());
    let drive = Drive::new(&sim, notes);
    let requests = [
        (Duration::from_secs(1), post(1)),
        (Duration::from_millis(1500), post(2)),
    ];
    let _ = drive.faulted_phase(&requests, Duration::from_secs(1));
}

/// The contract's boundary: a request exactly at the horizon is no breach,
/// and the phase sends it.
#[test]
fn a_request_at_the_horizon_is_sent_in_the_phase() {
    let sim = Sim::with_faults(0xD7_5001, FaultConfig::none());
    let drive = Drive::new(&sim, notes);
    let phase = drive
        .faulted_phase(&[(Duration::from_secs(1), post(1))], Duration::from_secs(1))
        .unwrap();
    assert_eq!(phase.answers, vec![Some((200, id_body(1)))]);
}

/// The tail's three ways not to boot, each with its own text. No faults run:
/// each failure is the build's doing, so no seed is needed to show it, and
/// the drive sends one request through a hand-built phase.
#[test]
fn a_tail_that_cannot_boot_says_so() {
    let send_one = || Phase {
        requests: vec![post(1)],
        answers: vec![None],
    };

    // Every boot meets fault fallout: each one arms a crash on its first disk
    // op, so all eight crash and nothing gives the run up.
    let sim = Sim::with_faults(0xD7_7000, FaultConfig::none());
    let boots = Cell::new(0u32);
    let drive = Drive::new(&sim, |sim: &Sim| {
        boots.set(boots.get() + 1);
        sim.crash_disk_after(0);
        notes(sim)
    });
    let err = drive
        .recover(&send_one(), Duration::from_secs(1))
        .err()
        .expect("eight boots lost to fallout");
    assert_eq!(err, "the fault-free tail could not boot");
    assert_eq!(boots.get(), 8);
    assert_eq!(sim.coverage().get("boot:exhausted").copied(), Some(1));
    assert_eq!(sim.gave_up(), None);

    // The app opens, and the kernel cannot be built: a crash on the kernel's
    // first disk op (offset 0 of the search in the next test).
    let sim = Sim::with_faults(0xD7_7000, FaultConfig::none());
    let drive = Drive::new(&sim, |sim: &Sim| {
        let app = notes(sim)?;
        sim.crash_disk_after(0);
        Ok(app)
    });
    let err = drive
        .recover(&send_one(), Duration::from_secs(1))
        .err()
        .expect("the kernel cannot be built");
    assert_eq!(
        err,
        "the fault-free tail could not boot: simulated crash: the disk is gone until Sim::restart()"
    );

    // The build fails with an error of its own, not fault fallout.
    let sim = Sim::with_faults(0xD7_7000, FaultConfig::none());
    let drive = Drive::new(&sim, |_: &Sim| Err(io::Error::other("the schema moved")));
    let err = drive
        .recover(&send_one(), Duration::from_secs(1))
        .err()
        .expect("the build fails");
    assert_eq!(err, "the fault-free tail could not boot: the schema moved");
}

#[test]
fn a_tail_kernel_stop_is_reported_in_the_kernels_words() {
    // No faults. The build arms a crash two disk ops after the tail's App
    // opens: a search of offsets 0..40 put 0 and 1 in the kernel's build, 2 to
    // 4 in the re-sent request's commit (the engine poisons and the kernel
    // stops, as here), and 29 on past the run.
    let sim = Sim::with_faults(0xD7_7000, FaultConfig::none());
    let drive = Drive::new(&sim, |sim: &Sim| {
        let app = notes(sim)?;
        sim.crash_disk_after(2);
        Ok(app)
    });
    let phase = Phase {
        requests: vec![post(1), post(2)],
        answers: vec![None, None],
    };
    let err = drive
        .recover(&phase, Duration::from_secs(1))
        .err()
        .expect("the tail's kernel stops");
    assert_eq!(
        err,
        "the fault-free tail's kernel stopped: the engine is poisoned (a request-path IO failure \
         made durability unknowable) — the request received its 500; restart the process to \
         recover from the WAL"
    );
}

/// The faulted phase's kernel, the two ways it ends: fault fallout ends the
/// phase, and anything else fails it with its own text. Each case is the
/// build's doing, so no faults run and no seed is searched.
#[test]
fn a_kernel_build_lost_to_fallout_ends_the_faulted_phase() {
    // The first boot arms a crash on the kernel's first disk op (offset 0 of
    // the search in `a_tail_kernel_stop_is_reported_in_the_kernels_words`):
    // the App opens, and the kernel cannot be built.
    let sim = Sim::with_faults(0xD7_7000, FaultConfig::none());
    let boots = Cell::new(0u32);
    let drive = Drive::new(&sim, |sim: &Sim| {
        let app = notes(sim)?;
        boots.set(boots.get() + 1);
        if boots.get() == 1 {
            sim.crash_disk_after(0);
        }
        Ok(app)
    });
    let phase = drive
        .faulted_phase(&posts(&[1, 2]), Duration::from_secs(1))
        .expect("fallout building the kernel ends the phase, never fails it");
    assert!(
        sim.disk_crashed() && boots.get() == 1,
        "the case: one boot, and its kernel's build met the crash"
    );
    assert_eq!(shape(&phase.answers), ["-", "-"], "nothing was sent");
    assert_eq!(
        sim.gave_up(),
        None,
        "a kernel lost to fallout is no give-up"
    );
    let recovered = drive
        .recover(&phase, Duration::from_secs(1))
        .unwrap()
        .expect("the fault-free tail boots");
    assert_eq!(
        (recovered.status(0), recovered.status(1)),
        (Some(200), Some(200)),
        "the tail sends what the lost kernel never answered"
    );
}

#[test]
fn a_kernel_refused_by_name_fails_the_faulted_phase_with_its_text() {
    // The job calls mail.example and the app declares no Host for it: the
    // kernel refuses to build, by name. A refusal is not fault fallout.
    let sim = Sim::with_faults(0xD7_4000, FaultConfig::none());
    sim.remote("mail.example", Counting(Arc::new(AtomicU64::new(0))));
    let drive = Drive::new(&sim, |sim: &Sim| {
        Ok(App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Note>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )?
        .mount(routes![send_mail])
        .mount_jobs(jobs![Mail]))
    });
    let err = drive
        .faulted_phase(&mails(1..=1), Duration::from_secs(1))
        .err()
        .expect("the kernel refuses to build");
    assert_eq!(
        err,
        "kernel: job Mail calls mail.example, which has no Host entry — add \
         app.outbound(Host::new(\"mail.example\")) with the credentials it needs (GUIDE §6)"
    );
}

/// Set by `unplug`'s handler, after which `Unplugged` refuses every
/// `Disk::open`. Only the test below mounts `unplug` or opens `Unplugged`.
static UNPLUGGED: AtomicBool = AtomicBool::new(false);

#[route(POST "/unplug/:id", public)]
fn unplug(tx: &mut Tx, id: Id<Note>) -> Result<Id<Note>, ApiError> {
    UNPLUGGED.store(true, Ordering::Relaxed);
    tx.put(&Note { id, n: id.raw() });
    Ok(id)
}

/// The sim's disk until `unplug` runs; then every `open` fails with an error
/// of the test's own, which names no fault, so nothing reads it as fallout.
struct Unplugged(SimDisk);

impl Disk for Unplugged {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        if UNPLUGGED.load(Ordering::Relaxed) {
            return Err(io::Error::other("the test unplugged the disk"));
        }
        self.0.open(path)
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        self.0.remove(path)
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.0.list(prefix)
    }

    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        self.0.sync_dir(prefix)
    }
}

#[test]
fn a_kernel_stop_that_is_not_fallout_fails_the_faulted_phase_with_its_text() {
    // The request is answered, and the kernel's clean-shutdown checkpoint
    // then cannot open its file: the run stops on an error that is not fault
    // fallout.
    let sim = Sim::with_faults(0xD7_7000, FaultConfig::none());
    let drive = Drive::new(&sim, |sim: &Sim| {
        Ok(App::open(
            Box::new(Unplugged(sim.disk())),
            Schema::new().table::<Note>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )?
        .mount(routes![unplug]))
    });
    let unplug_one = [(
        Duration::from_millis(1),
        Request::new(Method::Post, "/unplug/1"),
    )];
    let err = drive
        .faulted_phase(&unplug_one, Duration::from_secs(1))
        .err()
        .expect("the kernel's run stops on an error that is not fallout");
    assert!(
        UNPLUGGED.load(Ordering::Relaxed),
        "the case: the handler ran, so the stop came from the kernel's run, not its build"
    );
    assert_eq!(err, "the test unplugged the disk");
}

/// Task 5 (0.5.0) (d): a timed job never runs before its due time in true
/// time. The route enqueues at `tx.now() + d`; the job is outbound so that
/// the run is seen from outside the kernel, which cannot reach the `Sim`: the
/// remote model records which note each call is for, and the sim's delivery
/// record gives the true time the call arrived (a call arrives after it
/// started, so its arrival bounds the run from above).
#[table(global)]
struct Booked {
    id: Id<Booked>,
}

#[table(global)]
struct Ran {
    id: Id<Ran>,
    times: u64,
}

thread_local! {
    /// Note number -> the due time (wall nanoseconds) its latest enqueue
    /// meant. A universe runs on one thread, so this is that universe's.
    static NOTED: std::cell::RefCell<std::collections::BTreeMap<u64, u64>> =
        const { std::cell::RefCell::new(std::collections::BTreeMap::new()) };
}

#[job(
    global,
    retain_dead,
    outbound = "later.test",
    retry(max = 8, backoff = "500ms"),
    timeout = "2s"
)]
struct Timed {
    n: u64,
}

impl Timed {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(
            Call::post("/ran")
                .header("idempotency-key", key.as_str())
                .form(&[("n", self.n.to_string())]),
        ))
    }

    fn settle(self, tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        if reply.status() != 200 {
            return Err(JobError::app(format!(
                "later.test answered {}",
                reply.status()
            )));
        }
        let id = Id::<Ran>::from_raw(self.n);
        let times = tx.get(id).map_err(|e| JobError::app(e.to_string()))?;
        tx.put(&Ran {
            id,
            times: times.map_or(0, |r| r.times) + 1,
        });
        Ok(())
    }
}

/// Enqueues the timed job at `due` and notes the due time it was meant for.
fn note_enqueued(tx: &mut Tx, n: u64, due: WallTime) {
    tx.enqueue_at(due, Timed { n });
    NOTED.with(|m| m.borrow_mut().insert(n, due.as_nanos()));
}

/// Idempotent per note: a re-sent request finds its marker and enqueues
/// nothing, so a note has at most one job however often it is sent.
#[route(POST "/later/:id/:ms", public)]
fn later(tx: &mut Tx, id: Id<Booked>, ms: String) -> Result<Id<Booked>, ApiError> {
    let n = id.raw();
    let ms: u64 = ms
        .parse()
        .map_err(|_| ApiError::BadRequest(format!("{ms} is not a number of milliseconds")))?;
    if tx
        .get(id)
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .is_none()
    {
        tx.put(&Booked { id });
        let due = WallTime::from_nanos(tx.now().as_nanos() + ms * 1_000_000);
        note_enqueued(tx, n, due);
    }
    Ok(id)
}

fn timed_app(sim: &Sim) -> io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Booked>().table::<Ran>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![later])
    .mount_jobs(jobs![Timed])
    .outbound(Host::new("later.test")))
}

/// The clock axes on in every universe, as in determinism.rs, and a short
/// outbound latency: the call's arrival then bounds the run closely enough
/// to see a job run seconds early.
fn timed_axes(seed: u64, cfg: &mut FaultConfig) {
    let mut r = Rng::new(seed ^ 0x7A_C10C);
    cfg.clock_skew = r.between(0, 120_000_000_000) as i64;
    cfg.clock_step_back = (0.01, Duration::from_secs(120));
    cfg.clock_downtime = (0.5, Duration::from_secs(3 * 86_400));
    cfg.outbound_latency = (Duration::from_millis(1), Duration::from_millis(50));
}

const TIMED_UNIVERSES: u64 = 256;
const TIMED_DOWNTIME_FLOOR: u64 = 100;

fn timed_universe(sim: &mut Sim) -> Result<(), String> {
    NOTED.with(|m| m.borrow_mut().clear());
    // Which note each arriving call is for, in arrival order (the order of
    // `sim.outbound_deliveries()`, which is pushed at the same step).
    let arrivals = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let seen = arrivals.clone();
    sim.remote(
        "later.test",
        charpente_sim::outbound::from_fn(move |call: &OutboundCall| {
            let n = String::from_utf8_lossy(&call.body)
                .strip_prefix("n=")
                .and_then(|s| s.parse().ok())
                .expect("the call's form body is n=<note>");
            seen.lock().unwrap().push(n);
            OutboundReply {
                status: 200,
                headers: Vec::new(),
                body: Vec::new(),
            }
        }),
    );
    // (virtual boot instant, the offset the boot started with). Offsets peak
    // at boots: a power cycle's downtime jump precedes the build and reads
    // only step back, so the largest offset any reading up to an instant
    // carried is the largest among the boots up to it. A jump from a later
    // restart therefore does not loosen a note that arrived before it.
    let boots = std::cell::RefCell::new(Vec::<(u64, i64)>::new());
    let drive = Drive::new(sim, |s: &Sim| {
        boots
            .borrow_mut()
            .push((s.now().as_nanos(), s.clock().offset()));
        timed_app(s)
    });
    let mut r = Rng::new(sim.seed() ^ 0x71_3D);
    let requests: Vec<(Duration, Request)> = (1..=8u64)
        .map(|n| {
            let ms = r.between(500, 6_000);
            (
                Duration::from_millis(n * 100),
                Request::new(Method::Post, format!("/later/{n}/{ms}")),
            )
        })
        .collect();
    let phase = drive.faulted_phase(&requests, Duration::from_secs(4))?;
    let Some(recovered) = drive.recover(&phase, Duration::from_secs(200))? else {
        return Ok(());
    };
    let snap = recovered
        .kernel
        .app()
        .db()
        .snapshot()
        .map_err(|e| e.to_string())?;
    let booked: Vec<u64> = snap
        .scan::<Booked>()
        .map_err(|e| e.to_string())?
        .iter()
        .map(|b| b.id.raw())
        .collect();
    let ran: std::collections::BTreeMap<u64, u64> = snap
        .scan::<Ran>()
        .map_err(|e| e.to_string())?
        .iter()
        .map(|r| (r.id.raw(), r.times))
        .collect();
    let noted = NOTED.with(|m| m.borrow().clone());
    let deliveries = sim.outbound_deliveries();
    let arrivals = arrivals.lock().unwrap().clone();
    assert_eq!(
        arrivals.len(),
        deliveries.len(),
        "contract: one delivery record per arrival"
    );
    let mut first_arrival = std::collections::BTreeMap::new();
    for (n, d) in arrivals.iter().zip(&deliveries) {
        first_arrival.entry(*n).or_insert(d.at);
    }
    for &n in &booked {
        sim.cover("timed:checked");
        let due = *noted
            .get(&n)
            .ok_or_else(|| format!("note {n} is booked but its enqueue was never noted"))?;
        if ran.get(&n) != Some(&1) {
            return Err(format!(
                "note {n}'s timed job ran {:?} times by the end of the tail, not exactly once",
                ran.get(&n)
            ));
        }
        let at = first_arrival[&n];
        let true_run = i128::from(at) + i128::from(charpente_sim::SIM_WALL_BASE_NANOS);
        let peak = boots
            .borrow()
            .iter()
            .filter(|&&(boot, _)| boot <= at)
            .map(|&(_, offset)| offset)
            .max()
            .expect("a call arrives after the boot that sent it");
        let bound = i128::from(due) - i128::from(peak);
        if true_run < bound {
            return Err(format!(
                "note {n}'s timed job ran at true wall {true_run} ns, {} ns before its due time \
                 {due} ns less the clock's peak offset {} ns up to its arrival",
                bound - true_run,
                peak
            ));
        }
    }
    Ok(())
}

#[test]
fn a_timed_job_never_runs_before_its_due_time_in_true_time() {
    charpente_sim::run_universes_swarm_with!(
        0x7A_5001,
        TIMED_UNIVERSES,
        timed_axes,
        timed_universe,
        require: ["clock:downtime", "timed:checked"],
        floor: [("clock:downtime", TIMED_DOWNTIME_FLOOR)]
    );
}

/// An outage refuses every call to its one host for its whole window, whether
/// or not the tail has quiesced the outbound faults, and the host answers
/// again when the window ends. Two hosts, so the one that is refused is the
/// drawn one and the other is the control. Seed 0xD7_0A01 is arbitrary: the
/// test finds the drawn host by probing during the window and asserts it is
/// exactly one of the two.
#[test]
fn an_outage_refuses_every_call_to_its_host_through_the_quiesce() {
    fn call(host: &'static str, n: u8) -> OutboundCall {
        OutboundCall {
            host,
            method: HttpMethod::Post,
            path: "/x".into(),
            headers: Vec::new(),
            body: vec![n],
            key: CallKey::from_bytes([n; 16]),
        }
    }
    let ok = |_: &OutboundCall| OutboundReply {
        status: 200,
        headers: Vec::new(),
        body: Vec::new(),
    };
    let mut cfg = FaultConfig::none();
    // Window: [10 s, 70 s) of virtual time.
    cfg.outbound_outage = (Duration::from_secs(10), Duration::from_secs(60));
    let sim = Sim::with_faults(0xD7_0A01, cfg);
    sim.remote("a.test", charpente_sim::outbound::from_fn(ok));
    sim.remote("b.test", charpente_sim::outbound::from_fn(ok));
    let ob = sim.outbound(&["a.test", "b.test"]).unwrap();
    // One call; Ok(status) or the transport error's kind.
    let try_call = |host: &'static str, n: u8| -> Result<u16, io::ErrorKind> {
        ob.start(u64::from(n), call(host, n)).unwrap();
        let (token, result) = ob
            .poll()
            .expect("a fault-free-latency call completes at once");
        assert_eq!(token, u64::from(n));
        result.map(|r| r.status).map_err(|e| e.kind())
    };
    // Before the window: both hosts answer. (The pick happens at this call.)
    assert_eq!(try_call("a.test", 1), Ok(200));
    assert_eq!(try_call("b.test", 2), Ok(200));
    sim.clock().sleep(Duration::from_secs(20));
    let during: Vec<_> = ["a.test", "b.test"]
        .into_iter()
        .map(|h| (h, try_call(h, 3)))
        .collect();
    let refused: Vec<_> = during.iter().filter(|(_, r)| r.is_err()).collect();
    assert_eq!(
        refused.len(),
        1,
        "exactly the drawn host is refused: {during:?}"
    );
    let (down, _) = *refused[0];
    let up = if down == "a.test" { "b.test" } else { "a.test" };
    assert_eq!(refused[0].1, Err(io::ErrorKind::ConnectionRefused));
    assert_eq!(try_call(up, 4), Ok(200));
    // The tail's quiesce ends every roll, and the outage goes on.
    sim.quiesce_outbound_faults();
    assert_eq!(try_call(down, 5), Err(io::ErrorKind::ConnectionRefused));
    sim.clock().sleep(Duration::from_secs(40));
    assert_eq!(try_call(down, 6), Err(io::ErrorKind::ConnectionRefused));
    assert_eq!(try_call(up, 7), Ok(200));
    // Past the window's end it answers again.
    sim.clock().sleep(Duration::from_secs(20));
    assert_eq!(try_call(down, 8), Ok(200));
    // Three refused calls (probe, after quiesce, late in the window).
    assert_eq!(sim.coverage().get("fault:outbound_outage"), Some(&3));
}

/// The window is [start, start + length): a call at exactly the start is
/// refused, and one at exactly the end is answered.
#[test]
fn an_outage_window_is_closed_at_its_start_and_open_at_its_end() {
    let mut cfg = FaultConfig::none();
    cfg.outbound_outage = (Duration::from_secs(10), Duration::from_secs(60));
    let sim = Sim::with_faults(0xD7_0A02, cfg);
    sim.remote(
        "a.test",
        charpente_sim::outbound::from_fn(|_: &OutboundCall| OutboundReply {
            status: 200,
            headers: Vec::new(),
            body: Vec::new(),
        }),
    );
    let ob = sim.outbound(&["a.test"]).unwrap();
    let try_call = |n: u8| {
        ob.start(
            u64::from(n),
            OutboundCall {
                host: "a.test",
                method: HttpMethod::Post,
                path: "/x".into(),
                headers: Vec::new(),
                body: vec![n],
                key: CallKey::from_bytes([n; 16]),
            },
        )
        .unwrap();
        ob.poll()
            .expect("answers at once")
            .1
            .map(|r| r.status)
            .map_err(|e| e.kind())
    };
    sim.clock()
        .sleep(Duration::from_secs(10) - Duration::from_nanos(1));
    assert_eq!(try_call(1), Ok(200), "one nanosecond before the start");
    sim.clock().sleep(Duration::from_nanos(1));
    assert_eq!(
        try_call(2),
        Err(io::ErrorKind::ConnectionRefused),
        "exactly the start"
    );
    sim.clock()
        .sleep(Duration::from_secs(60) - Duration::from_nanos(1));
    assert_eq!(
        try_call(3),
        Err(io::ErrorKind::ConnectionRefused),
        "one nanosecond before the end"
    );
    sim.clock().sleep(Duration::from_nanos(1));
    assert_eq!(try_call(4), Ok(200), "exactly the end");
}

// ------------------------------------------- the tail settles outbound work
//
// 0.5.0 spec §5 E: once its horizon has passed, `recover` runs on until every
// outbound job is Done, Dead or not yet due, under a cap. Every test here runs
// under `FaultConfig::none()`, at most with the outage axis or a fixed latency
// on, from virtual time 0, so the seed only names the universe: nothing here
// draws a fault.

const HOUR: Duration = Duration::from_secs(3600);

/// No per-call fault, and every call to the app's one host refused from the
/// universe's start for `length`.
fn outage(length: Duration) -> FaultConfig {
    FaultConfig {
        outbound_outage: (Duration::ZERO, length),
        ..FaultConfig::none()
    }
}

fn post_to(key: &IdempotencyKey, n: u64) -> Call {
    Call::post("/x")
        .header("idempotency-key", key.as_str())
        .form(&[("n", n.to_string())])
}

fn settled(host: &str, reply: &Reply) -> Result<(), JobError> {
    match reply.status() {
        200 => Ok(()),
        s => Err(JobError::app(format!("{host} answered {s}"))),
    }
}

/// Twelve attempts on the default 30 s backoff: past a 4 h outage, so it
/// ends Done.
#[job(global, retain_dead, outbound = "slow.test", retry(max = 12))]
struct Patient {
    n: u64,
}

impl Patient {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(post_to(key, self.n)))
    }

    fn settle(self, _tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        settled("slow.test", &reply)
    }
}

/// The default policy, five attempts over 450 s: a 4 h outage exhausts it,
/// so it ends Dead.
#[job(global, retain_dead, outbound = "slow.test")]
struct Brief {
    n: u64,
}

impl Brief {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(post_to(key, self.n)))
    }

    fn settle(self, _tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        settled("slow.test", &reply)
    }
}

#[route(POST "/slow/:id", public)]
fn enqueue_slow(tx: &mut Tx, id: Id<Note>) -> Result<Id<Note>, ApiError> {
    tx.enqueue(Patient { n: id.raw() });
    tx.enqueue(Brief { n: id.raw() });
    Ok(id)
}

fn slow_app(sim: &Sim) -> io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![enqueue_slow])
    .mount_jobs(jobs![Patient, Brief])
    .outbound(Host::new("slow.test")))
}

/// Each outbound row as (name, status, attempts), in id order.
fn outbound_rows(snap: &Snapshot) -> Vec<(String, JobStatus, u32)> {
    snap.scan::<JobRow>()
        .unwrap()
        .into_iter()
        .filter(|row| row.outbound_key.is_some())
        .map(|row| (row.name, row.status, row.attempts))
        .collect()
}

#[test]
fn the_tail_extends_until_retries_finish_or_die() {
    let sim = Sim::with_faults(0xD7_0B01, outage(4 * HOUR));
    let calls = Arc::new(AtomicU64::new(0));
    sim.remote("slow.test", Counting(Arc::clone(&calls)));
    let drive = Drive::new(&sim, slow_app);
    let phase = drive
        .faulted_phase(
            &[(Duration::ZERO, Request::new(Method::Post, "/slow/1"))],
            Duration::from_secs(1),
        )
        .unwrap();
    assert_eq!(phase.status(0), Some(200));
    let recovered = drive
        .recover(&phase, Duration::from_secs(60))
        .unwrap()
        .expect("the fault-free tail boots");
    let snap = recovered.kernel.app().db().snapshot().unwrap();
    assert_eq!(
        outbound_rows(&snap),
        [
            ("Patient".to_string(), JobStatus::Done, 9),
            ("Brief".to_string(), JobStatus::Dead, 5),
        ],
        "Patient's tenth call, at 15 330 s, is past the outage and answered; Brief's five \
         are all refused"
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "only Patient's last call got through"
    );
    assert!(
        sim.now().as_nanos() >= (4 * HOUR).as_nanos() as u64,
        "the tail ran past the outage: {:?}",
        sim.now()
    );
    // The tail's own 60 s run makes the attempt at 30 s. The outage lasts
    // to 14 400 s, so the first extension reaches its end plus the 30 s
    // timeout plus 1 s: the attempts at 90, 210, 450, 930, 1890, 3810 and
    // 7650 s are refused inside it, and Brief dies at 450 s. Patient's next
    // is due at 15 330 s, past that reach, so a second extension runs to it.
    // Before the outage's end counted (0.5.0 Task 11 fix round 1, ruling
    // R-12) this took 8, one a retry.
    assert_eq!(
        recovered.extensions, 2,
        "far inside the cap of {TAIL_SETTLE_CAP}"
    );
    assert_eq!(
        recovered.wall,
        sim.true_wall(),
        "no clock fault, so the node's wall is true time"
    );
    assert_eq!(outbound_settles_by_horizon(&snap, recovered.wall), Ok(()));
    assert_eq!(sim.coverage().get("dead:Brief"), Some(&1));
    assert_eq!(sim.coverage().get("dead:Patient"), None);
}

/// A pure job on the default policy fails once, so a hook-made chain must
/// reach a remote to end; `n` counts the chain's links.
#[job(global, on_dead, outbound = "chain.test")]
struct Renew {
    n: u64,
}

impl Renew {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(post_to(key, self.n)))
    }

    fn settle(self, _tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        settled("chain.test", &reply)
    }

    /// Review Focus 1: a fresh job, under a fresh key, on every Dead.
    fn on_dead(self, tx: &mut Tx, _dead: charpente::Dead<Self>) -> Result<(), JobError> {
        tx.enqueue(Renew { n: self.n + 1 });
        Ok(())
    }
}

/// The same chain made by re-issuing at once, as GUIDE §6's example does
/// later: one key and one request down the chain, each link at attempt 0.
#[job(global, on_dead, outbound = "chain.test")]
struct Resend {
    n: u64,
}

impl Resend {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(post_to(key, self.n)))
    }

    fn settle(self, _tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        settled("chain.test", &reply)
    }

    fn on_dead(self, tx: &mut Tx, dead: charpente::Dead<Self>) -> Result<(), JobError> {
        tx.reissue_after(Duration::ZERO, dead);
        Ok(())
    }
}

#[route(POST "/chain/:id", public)]
fn start_chains(tx: &mut Tx, id: Id<Note>) -> Result<Id<Note>, ApiError> {
    tx.enqueue(Renew { n: 0 });
    tx.enqueue(Resend { n: id.raw() });
    Ok(id)
}

fn chain_app(sim: &Sim) -> io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![start_chains])
    .mount_jobs(jobs![Renew, Resend])
    .outbound(Host::new("chain.test")))
}

#[test]
fn a_hook_that_reenqueues_on_every_dead_ends_with_the_outage() {
    let sim = Sim::with_faults(0xD7_0B02, outage(4 * HOUR));
    let calls = Arc::new(AtomicU64::new(0));
    sim.remote("chain.test", Counting(Arc::clone(&calls)));
    let drive = Drive::new(&sim, chain_app);
    let phase = drive
        .faulted_phase(
            &[(Duration::ZERO, Request::new(Method::Post, "/chain/7"))],
            Duration::from_secs(1),
        )
        .unwrap();
    assert_eq!(phase.status(0), Some(200));
    let recovered = drive
        .recover(&phase, Duration::from_secs(60))
        .unwrap()
        .expect("the fault-free tail boots");
    let snap = recovered.kernel.app().db().snapshot().unwrap();
    let rows = snap.scan::<JobRow>().unwrap();
    for name in ["Renew", "Resend"] {
        let chain: Vec<&JobRow> = rows.iter().filter(|r| r.name == name).collect();
        let (last, dead) = chain.split_last().expect("the chain has a link");
        assert_eq!(
            last.status,
            JobStatus::Done,
            "{name}'s last link is answered"
        );
        assert!(
            dead.iter()
                .all(|r| r.status == JobStatus::Dead && r.attempts == 5),
            "{name}: every link before the last died after five attempts"
        );
        // A link lasts 450 s: link k's fifth attempt is at 450 (k + 1) s, so
        // links 0 to 30 die inside the 14 400 s outage, and link 31's fifth,
        // at exactly 14 400 s, where the window opens again, is answered.
        assert_eq!(dead.len(), 31, "{name}: links that died in the outage");
        assert_eq!(
            last.attempts, 4,
            "{name}'s last link: four refused, then answered"
        );
        assert_eq!(
            sim.coverage().get(format!("dead:{name}").as_str()),
            Some(&31),
            "{name}: one dead:<name> mark per Dead row"
        );
    }
    let keys = |name: &str| -> std::collections::BTreeSet<[u8; 16]> {
        rows.iter()
            .filter(|r| r.name == name)
            .map(|r| r.outbound_key.unwrap())
            .collect()
    };
    assert_eq!(
        keys("Renew").len(),
        32,
        "every fresh link mints its own key"
    );
    assert_eq!(
        keys("Resend").len(),
        1,
        "a re-issue keeps the dead job's key"
    );
    let deliveries = sim.outbound_deliveries();
    assert_eq!(
        deliveries.len(),
        2,
        "only each chain's last call got through"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    assert!(
        deliveries
            .iter()
            .all(|d| d.at >= (4 * HOUR).as_nanos() as u64),
        "each chain ended with the outage, not before: {deliveries:?}"
    );
    // One extension, to the outage's end plus the 30 s timeout plus 1 s:
    // both chains run all 32 links inside it, and end with the outage. Before
    // the outage's end counted (ruling R-12) this took 96, three a link.
    assert_eq!(
        recovered.extensions, 1,
        "inside the cap of {TAIL_SETTLE_CAP}"
    );
    assert_eq!(keys_are_per_effect(&snap), Ok(()));
    assert_eq!(outbound_settles_by_horizon(&snap, recovered.wall), Ok(()));
}

/// outbound-fixture's `ChargeOrder` policy, eight attempts backing off 0.5 s
/// doubling (63.5 s in all), each with a 2 s timeout, and a fresh job on
/// every Dead: a link a minute through a 4 h outage.
#[job(
    global,
    on_dead,
    outbound = "hasty.test",
    retry(max = 8, backoff = "500ms"),
    timeout = "2s"
)]
struct Hasty {
    n: u64,
}

impl Hasty {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(post_to(key, self.n)))
    }

    fn settle(self, _tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        settled("hasty.test", &reply)
    }

    fn on_dead(self, tx: &mut Tx, _dead: charpente::Dead<Self>) -> Result<(), JobError> {
        tx.enqueue(Hasty { n: self.n + 1 });
        Ok(())
    }
}

#[route(POST "/hasty/:id", public)]
fn start_hasty(tx: &mut Tx, id: Id<Note>) -> Result<Id<Note>, ApiError> {
    tx.enqueue(Hasty { n: id.raw() });
    Ok(id)
}

fn hasty_app(sim: &Sim) -> io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![start_hasty])
    .mount_jobs(jobs![Hasty])
    .outbound(Host::new("hasty.test")))
}

#[test]
fn a_short_policy_chain_ends_with_the_outage_inside_the_cap() {
    // Ruling R-12: while the drawn outage lasts, an extension reaches its
    // end. Without that, each extension covers only the backoffs that fit in
    // the 2 s timeout plus 1 s, about five a 63.5 s link, and this chain ran
    // into the cap at link 52 of 227 (its job 52, at attempt 3).
    let sim = Sim::with_faults(0xD7_0B05, outage(4 * HOUR));
    let calls = Arc::new(AtomicU64::new(0));
    sim.remote("hasty.test", Counting(Arc::clone(&calls)));
    let drive = Drive::new(&sim, hasty_app);
    let phase = drive
        .faulted_phase(
            &[(Duration::ZERO, Request::new(Method::Post, "/hasty/0"))],
            Duration::from_secs(1),
        )
        .unwrap();
    assert_eq!(phase.status(0), Some(200));
    let recovered = drive
        .recover(&phase, Duration::from_secs(60))
        .unwrap()
        .expect("the fault-free tail boots");
    let snap = recovered.kernel.app().db().snapshot().unwrap();
    let chain = outbound_rows(&snap);
    let (last, dead) = chain.split_last().expect("the chain has a link");
    // Link k's eighth attempt is at 63.5 (k + 1) s, so links 0 to 225 die
    // inside the 14 400 s outage, and link 226's eighth, at 14 414.5 s, is
    // answered.
    assert_eq!(dead.len(), 226, "links that died in the outage");
    assert!(
        dead.iter()
            .all(|(_, status, attempts)| *status == JobStatus::Dead && *attempts == 8),
        "every link before the last died after eight attempts"
    );
    assert_eq!(
        last,
        &("Hasty".to_string(), JobStatus::Done, 7),
        "the last link: seven refused, then answered"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(
        sim.outbound_deliveries()
            .iter()
            .all(|d| d.at >= (4 * HOUR).as_nanos() as u64),
        "the chain ended with the outage, not before"
    );
    // The first extension reaches the outage's end plus 3 s, where link
    // 226's eighth attempt is still 11.5 s off; the second runs to it.
    assert_eq!(
        recovered.extensions, 2,
        "inside the cap of {TAIL_SETTLE_CAP}"
    );
    assert_eq!(sim.coverage().get("dead:Hasty"), Some(&226));
    assert_eq!(outbound_settles_by_horizon(&snap, recovered.wall), Ok(()));
}

/// `Brief`, enqueued three days out.
#[route(POST "/remind/:id", public)]
fn remind_later(tx: &mut Tx, id: Id<Note>) -> Result<Id<Note>, ApiError> {
    tx.enqueue_after(Duration::from_secs(3 * 86_400), Brief { n: id.raw() });
    Ok(id)
}

fn remind_app(sim: &Sim) -> io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![remind_later])
    .mount_jobs(jobs![Brief])
    .outbound(Host::new("slow.test")))
}

#[test]
fn a_delayed_outbound_job_is_not_waited_for() {
    // 0xD7_0B06 with no faults at all: the case is the stop rule, which no
    // fault touches. A row with no attempt made and a due time past the wall
    // is not yet due, so the tail does not run three days to reach it.
    let sim = Sim::with_faults(0xD7_0B06, FaultConfig::none());
    let calls = Arc::new(AtomicU64::new(0));
    sim.remote("slow.test", Counting(Arc::clone(&calls)));
    let drive = Drive::new(&sim, remind_app);
    let phase = drive
        .faulted_phase(
            &[(Duration::ZERO, Request::new(Method::Post, "/remind/1"))],
            Duration::from_secs(1),
        )
        .unwrap();
    assert_eq!(phase.status(0), Some(200));
    let recovered = drive
        .recover(&phase, Duration::from_secs(60))
        .unwrap()
        .expect("the fault-free tail boots");
    assert_eq!(
        recovered.extensions, 0,
        "a delayed row keeps nothing running"
    );
    let snap = recovered.kernel.app().db().snapshot().unwrap();
    assert_eq!(
        outbound_rows(&snap),
        [("Brief".to_string(), JobStatus::Pending, 0)]
    );
    let row = snap.scan::<JobRow>().unwrap().remove(0);
    assert!(
        row.due_at.saturating_duration_since(recovered.wall) > Duration::from_secs(2 * 86_400),
        "the row is due days after the tail ended: due {}, wall {}",
        row.due_at,
        recovered.wall
    );
    assert!(
        sim.now().as_nanos() < Duration::from_secs(120).as_nanos() as u64,
        "the tail stopped minutes in, not at the row's due time: {:?}",
        sim.now()
    );
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(outbound_settles_by_horizon(&snap, recovered.wall), Ok(()));
}

#[test]
fn the_tail_judges_due_at_the_kernels_held_wall() {
    // 0xD7_0B07 with no faults: the case is built. Each build stamps an API
    // key 30 s ahead of the clock, inside the 60 s tolerance, so the tail's
    // kernel opens without a refusal and holds its wall at the key's time,
    // above the clock's reading for the whole 1 s tail. A job due in that
    // gap is due to the kernel, so the tail judges at the hold (ruling R-14).
    let sim = Sim::with_faults(0xD7_0B07, FaultConfig::none());
    let ahead = Cell::new(0u64);
    let drive = Drive::new(&sim, |sim: &Sim| {
        let mut app = notes(sim)?;
        ahead.set(sim.clock().wall().as_nanos() + 30_000_000_000);
        app.transact_for_tests(|tx| {
            tx.put(&ApiKey {
                id: Id::from_raw(1),
                key_hash: String::new(),
                subject: "ahead".to_string(),
                created_at: WallTime::from_nanos(ahead.get()),
            });
            Ok::<(), io::Error>(())
        })
        .map_err(|e| io::Error::other(format!("{e:?}")))?;
        Ok(app)
    });
    let phase = Phase {
        requests: vec![post(1)],
        answers: vec![None],
    };
    let recovered = drive
        .recover(&phase, Duration::from_secs(1))
        .unwrap()
        .expect("a key inside the tolerance opens without a refusal");
    assert_eq!(sim.coverage().get("restart:process"), None, "no wait-out");
    assert_eq!(
        recovered.wall,
        WallTime::from_nanos(ahead.get()),
        "the tail ended at the kernel's held wall"
    );
    assert!(
        recovered.wall > sim.clock().wall(),
        "the hold sits above the clock: {} against {}",
        recovered.wall,
        sim.clock().wall()
    );
}

/// Never answers inside a call's 1 s timeout: every reply is drawn 20 s
/// after its call (10 s each way, below), and it would be a 503 anyway.
struct Unanswering;

impl Remote for Unanswering {
    fn handle(&mut self, _call: &OutboundCall, _rng: &mut Rng) -> OutboundReply {
        OutboundReply {
            status: 503,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }
}

/// `Unanswering`, counting the calls that reach it. The cap bounds the
/// tail's work, so past `bound` calls the tail has run on without one, and
/// the model stops the test with that rather than let it spin.
struct Capped {
    calls: u64,
    bound: u64,
}

impl Remote for Capped {
    fn handle(&mut self, call: &OutboundCall, rng: &mut Rng) -> OutboundReply {
        self.calls += 1;
        assert!(
            self.calls <= self.bound,
            "the tail made {} calls to {}, more than {TAIL_SETTLE_CAP} extensions of a \
             never-settling job make: it did not stop at its cap",
            self.calls,
            call.host
        );
        Unanswering.handle(call, rng)
    }
}

/// Retries without backoff, effectively forever: each attempt is a 1 s
/// timeout, so virtual time moves and no due time grows.
#[job(
    global,
    retain_dead,
    outbound = "stuck.test",
    retry(max = 4000000000, backoff = "0ms"),
    timeout = "1s"
)]
struct Stuck {
    n: u64,
}

impl Stuck {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(post_to(key, self.n)))
    }

    fn settle(self, _tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        settled("stuck.test", &reply)
    }
}

/// `Stuck` on the default 30 s backoff: its doubling wait puts its due time
/// past the year 2262 by its 29th attempt, and at the end of the u64 range
/// soon after.
#[job(
    global,
    retain_dead,
    outbound = "stuck.test",
    retry(max = 4000000000),
    timeout = "1s"
)]
struct Doubling {
    n: u64,
}

impl Doubling {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(post_to(key, self.n)))
    }

    fn settle(self, _tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        settled("stuck.test", &reply)
    }
}

#[route(POST "/stuck/:id", public)]
fn enqueue_stuck(tx: &mut Tx, id: Id<Note>) -> Result<Id<Note>, ApiError> {
    tx.enqueue(Stuck { n: id.raw() });
    Ok(id)
}

#[route(POST "/doubling/:id", public)]
fn enqueue_doubling(tx: &mut Tx, id: Id<Note>) -> Result<Id<Note>, ApiError> {
    tx.enqueue(Doubling { n: id.raw() });
    Ok(id)
}

fn stuck_app(sim: &Sim) -> io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![enqueue_stuck, enqueue_doubling])
    .mount_jobs(jobs![Stuck, Doubling])
    .outbound(Host::new("stuck.test")))
}

/// Runs `path` through a phase and the tail on a sim where every call is
/// answered too late, and hands back the tail's error.
fn never_settles(seed: u64, path: &str) -> (Sim, String) {
    let sim = Sim::with_faults(
        seed,
        FaultConfig {
            outbound_latency: (Duration::from_secs(10), Duration::from_secs(10)),
            ..FaultConfig::none()
        },
    );
    sim.remote(
        "stuck.test",
        Capped {
            calls: 0,
            bound: 4 * u64::from(TAIL_SETTLE_CAP),
        },
    );
    let drive = Drive::new(&sim, stuck_app);
    let phase = drive
        .faulted_phase(
            &[(Duration::ZERO, Request::new(Method::Post, path))],
            Duration::from_secs(1),
        )
        .unwrap();
    assert_eq!(phase.status(0), Some(200));
    let err = drive
        .recover(&phase, Duration::from_secs(5))
        .err()
        .expect("a job that never settles fails the tail");
    drop(drive);
    (sim, err)
}

#[test]
fn a_tail_that_cannot_settle_fails_by_name_at_the_cap() {
    // Two attempts an extension, each a 1 s timeout with no backoff.
    let (_sim, err) = never_settles(0xD7_0B03, "/stuck/1");
    assert_eq!(
        err,
        "Drive::recover: the tail ran 256 extensions past its horizon and outbound job 0 \
         (\"Stuck\") is still Pending at attempt 518 — an outbound job settles, retries to \
         Dead, or waits for a later due time; this one did none of them within the cap of \
         256, so fix the model of the host it calls, or give the job, and the chain its on_dead \
         hook makes, a retry policy that ends"
    );
    // A due time doubling toward the end of the u64 range: no run goes past
    // the year 2262, so the clock never overflows and the kernel never spins
    // on a due time pinned at the range's end, and the tail still fails by
    // name.
    // Attempt 28's retry is due in the year 2281. The 28th extension runs
    // to the limit, and a 29th could reach no further, so the tail fails
    // there, by the limit's own text naming the due time and the fix (final
    // review M-2), rather than run 228 extensions that do nothing over a
    // store of 86 000-odd daily sweep rows. Without the limit the kernel
    // stops at the range's end on the sweep's overflow ("next slot after …
    // overflows u64 nanos").
    let (sim, err) = never_settles(0xD7_0B04, "/doubling/1");
    assert_eq!(
        err,
        "Drive::recover: outbound job 0 (\"Doubling\") is still Pending at attempt 28, due \
         2281-03-11T18:07:58.000Z, and the tail ran 28 extensions, the last to the year 2262, \
         past which it does not run — a due time that far out means the job's retry backoff \
         grew past the year 2262, so no run reaches its next attempt; give the job a retry \
         policy that ends, a retry(max = …) small enough that it reaches Dead"
    );
    assert!(
        sim.true_wall().as_nanos() < u64::MAX / 2,
        "the tail stopped short of the year 2262: {}",
        sim.true_wall()
    );
}

/// An extension whose reach meets the limit to the nanosecond runs to the
/// limit, so the tail fails after it, by the limit's text, without a second
/// extension that could reach no further (R-21, the mutation sweep's 362:34).
/// The universe is built, not searched: fault-free but for an outage on every
/// call that ends 2 s (the 1 s timeout plus the 1 s slack) before the limit,
/// so the one extension's reach, the outage's remaining length plus those
/// 2 s, equals the limit exactly.
#[test]
fn a_tail_whose_reach_meets_the_limit_exactly_fails_after_one_extension() {
    let end = u64::MAX / 2 - charpente_sim::SIM_WALL_BASE_NANOS - 2_000_000_000;
    let sim = Sim::with_faults(
        0xD7_0B05,
        FaultConfig {
            outbound_outage: (Duration::ZERO, Duration::from_nanos(end)),
            ..FaultConfig::none()
        },
    );
    sim.remote("stuck.test", Unanswering);
    let drive = Drive::new(&sim, stuck_app);
    let phase = drive
        .faulted_phase(
            &[(Duration::ZERO, Request::new(Method::Post, "/doubling/1"))],
            Duration::from_secs(1),
        )
        .unwrap();
    assert_eq!(phase.status(0), Some(200));
    let err = drive
        .recover(&phase, Duration::from_secs(5))
        .err()
        .expect("a job due past the limit fails the tail");
    assert_eq!(
        err,
        "Drive::recover: outbound job 0 (\"Doubling\") is still Pending at attempt 28, due \
         2281-03-11T18:07:30.000Z, and the tail ran 1 extension, the last to the year 2262, past \
         which it does not run — a due time that far out means the job's retry backoff grew past the year \
         2262, so no run reaches its next attempt; give the job a retry policy that ends, a \
         retry(max = …) small enough that it reaches Dead"
    );
    // The one extension ran to the limit: the kernel's last step was the
    // daily sweep's last slot before the year 2262.
    assert_eq!(sim.true_wall().as_nanos(), 9_223_286_400_000_000_000);
}
