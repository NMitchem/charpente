//! `charpente_sim::drive`: the faulted phase, the fault-free tail and the
//! re-sends, one behaviour per step. Each test runs a tiny app on a pinned
//! seed, found by a bounded search (seeds 0xD7_0000 and up), and the comment
//! on each seed says what that seed shows. A pinned seed names a universe:
//! each test also asserts the case its seed was chosen for, so a change that
//! renames the universe fails on that assertion instead of passing on
//! another case.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use charpente::core::io::{
    HttpMethod, IdempotencyKey as CallKey, Outbound, OutboundCall, OutboundReply,
};
use charpente::core::job::{JobRow, JobStatus};
use charpente::http::{ApiError, App, AppConfig, Method, Request};
use charpente::kernel::JobError;
use charpente::outbound::{Call, Host, IdempotencyKey, Reply};
use charpente::{Id, Schema, Snapshot, Tx, View, job, jobs, route, routes, table};
use charpente_sim::checks::{durable_corruption_fired, is_fault_fallout};
use charpente_sim::drive::{Drive, Phase};
use charpente_sim::outbound::Remote;
use charpente_sim::rng::Rng;
use charpente_sim::{FaultConfig, Sim};

#[table(global)]
struct Note {
    id: Id<Note>,
    n: u64,
}

/// Note 400 is the app's own 400, note 500 its own 500 (a handler's error,
/// not fault fallout); any other note is stored.
#[route(POST "/notes/:id", public)]
fn add_note(tx: &mut Tx, id: Id<Note>) -> Result<Id<Note>, ApiError> {
    match id.raw() {
        400 => Err(ApiError::BadRequest("note 400 is refused".to_string())),
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
    let before = phase.answers.clone();
    let recovered = drive
        .recover(&phase, Duration::from_secs(1))
        .unwrap()
        .expect("the fault-free tail boots");
    assert_eq!(
        phase.answers, before,
        "the phase's answers stand as recorded"
    );
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
    let sim = Sim::with_faults(0xD7_5000, FaultConfig::none());
    let drive = Drive::new(&sim, notes);
    let phase = Phase {
        requests: vec![post(1), post(2)],
        answers: vec![None],
    };
    let _ = drive.recover(&phase, Duration::from_secs(1));
}
