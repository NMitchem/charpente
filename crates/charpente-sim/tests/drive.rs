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
use charpente_sim::checks::{clock_reopens_after, durable_corruption_fired, is_fault_fallout};
use charpente_sim::drive::{Drive, Phase};
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
#[job(global, retry(max = 3, backoff = "1s"))]
struct Doomed {
    note: Id<Note>,
}

impl Doomed {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Err(JobError::app(format!("note {} is doomed", self.note.raw())))
    }
}

/// The 1 s schedule's job.
#[job(global)]
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
    let defect = "the fault-free tail could not boot: its open was refused for the clock a \
                  third time, after two waits past the instant each refusal named. One wait \
                  always suffices, since nothing moves the clock or the recorded time in \
                  between, so this is a platform clock defect, not the app's: ";
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
