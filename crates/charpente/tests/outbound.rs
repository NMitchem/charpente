//! Outbound effects (outbound-effects spec): the `#[job(outbound)]` surface,
//! the dispatcher's call path, the kernel's outbound arm, §7's failure table,
//! §4's invariants, and the swarm property. Every sim failure prints its
//! repro line (hard rule 5).

use std::time::Duration;

use charpente::auth::Principal;
use charpente::core::db::{Job, Refusal, Schema, TableReadRefusal, TxError, outbound_fingerprint};
use charpente::core::io::{Clock, Outbound, OutboundCall, OutboundReply, OutboundWake};
use charpente::core::job::{JobRow, JobStatus};
use charpente::core::table::{Codec, View};
use charpente::http::{ApiError, App, AppConfig};
use charpente::kernel::{
    Dispatcher, JobDef, JobEntry, JobError, JobOutcome, JobScope, Kernel, KernelStats, SimSource,
};
use charpente::outbound::{Call, Host, IdempotencyKey, Reply};
use charpente::{Id, ScopedTx, Tenant, Tx, job, jobs};
use charpente_sim::outbound::{Idempotent, Recorder, from_fn};
use charpente_sim::{FaultConfig, Sim};

#[job(
    global,
    outbound = "pay.test",
    retry(max = 3, backoff = "1s"),
    timeout = "2s"
)]
struct Charge {
    cents: u64,
}

impl Charge {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(
            Call::post("/charges")
                .header("idempotency-key", key.as_str())
                .form(&[("amount", self.cents.to_string())]),
        ))
    }

    fn settle(self, _tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        match reply.status() {
            200 => Ok(()),
            s => Err(JobError::app(format!("pay.test answered {s}"))),
        }
    }
}

/// The same fields as a pure job: its fingerprint is `Charge`'s before the
/// outbound fold (`fingerprint::compute` hashes fields, not the struct name).
#[job(global)]
struct ChargeAsPure {
    cents: u64,
}

impl ChargeAsPure {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

#[job(global, outbound = "mail.test")]
struct Mail {}

impl Mail {
    fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(None)
    }

    fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
        Ok(())
    }
}

#[test]
fn an_outbound_job_declares_its_host_timeout_and_shape() {
    let def = <Charge as JobEntry>::def();
    let out = def.outbound.as_ref().expect("declared outbound");
    assert_eq!(
        (out.host, out.timeout),
        ("pay.test", Duration::from_secs(2))
    );
    const { assert!(<Charge as Job>::OUTBOUND) };
    const { assert!(!<ChargeAsPure as Job>::OUTBOUND) };
    assert_eq!(
        <Charge as Job>::FINGERPRINT,
        outbound_fingerprint(<ChargeAsPure as Job>::FINGERPRINT),
        "R8: outbound-ness is part of the shape, the host is not"
    );
    assert_eq!(
        <Mail as JobEntry>::def().outbound.unwrap().timeout,
        Duration::from_secs(30),
        "spec §5: timeout defaults to 30s"
    );
    assert!(<ChargeAsPure as JobEntry>::def().outbound.is_none());
}

struct Org;

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

/// The `scoped_by` form: `call` reads through its tenant's `&ScopedTx`, and
/// `settle` writes through its `&mut ScopedTx`.
#[job(scoped_by = Org, outbound = "pay.test")]
struct ChargeInOrg {
    cents: u64,
}

impl ChargeInOrg {
    fn call(
        &self,
        _tx: &ScopedTx<'_, '_, Org>,
        key: &IdempotencyKey,
    ) -> Result<Option<Call>, JobError> {
        Ok(Some(
            Call::post("/charges")
                .header("idempotency-key", key.as_str())
                .form(&[("amount", self.cents.to_string())]),
        ))
    }

    fn settle(self, _tx: &mut ScopedTx<'_, '_, Org>, _reply: Reply) -> Result<(), JobError> {
        Ok(())
    }
}

#[job(scoped_by = Org)]
struct ChargeInOrgAsPure {
    cents: u64,
}

impl ChargeInOrgAsPure {
    fn run(self, _tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        Ok(())
    }
}

#[test]
fn a_scoped_outbound_job_folds_outbound_over_its_tenant_shape() {
    let def = <ChargeInOrg as JobEntry>::def();
    assert_eq!(def.scoped_by, Some("Org"));
    let out = def.outbound.as_ref().expect("declared outbound");
    assert_eq!(
        (out.host, out.timeout),
        ("pay.test", Duration::from_secs(30))
    );
    const { assert!(<ChargeInOrg as Job>::OUTBOUND) };
    assert_eq!(
        <ChargeInOrg as Job>::FINGERPRINT,
        outbound_fingerprint(<ChargeInOrgAsPure as Job>::FINGERPRINT),
        "R8: the outbound fold continues the scoped shape"
    );
    assert_ne!(
        <ChargeInOrg as Job>::FINGERPRINT,
        <Charge as Job>::FINGERPRINT,
        "tenancy stays part of an outbound job's shape"
    );
}

mod describe_hosts {
    use super::*;
    use charpente::describe::{to_human, to_json};

    fn outbound_app(sim: &Sim) -> App {
        App::open(
            Box::new(sim.disk()),
            Schema::new(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .unwrap()
        .mount_jobs(jobs![Charge, Mail])
        .outbound(
            Host::new("pay.test")
                .bearer_env("PAY_KEY")
                .max_reply_bytes(1 << 20),
        )
        .outbound(Host::new("mail.test").header_env("X-Postmark-Server-Token", "POSTMARK_TOKEN"))
    }

    const CHARGE_JOB: &str = r#"    {
      "name": "Charge",
      "retry": {
        "max": 3,
        "backoff_nanos": 1000000000
      },
      "scoped_by": null,
      "cross_tenant": false,
      "builtin": false,
      "outbound": {
        "host": "pay.test",
        "timeout_ms": 2000
      }
    },
"#;

    const HOSTS_JSON: &str = r#"  "outbound_hosts": [
    {
      "host": "pay.test",
      "env": [
        "PAY_KEY"
      ],
      "injects": [
        "authorization"
      ],
      "max_request_bytes": 1048576,
      "max_reply_bytes": 1048576
    },
    {
      "host": "mail.test",
      "env": [
        "POSTMARK_TOKEN"
      ],
      "injects": [
        "x-postmark-server-token"
      ],
      "max_request_bytes": 1048576,
      "max_reply_bytes": 4194304
    }
  ],
"#;

    #[test]
    fn describe_lists_outbound_jobs_and_hosts_by_variable_name() {
        let sim = Sim::with_faults(0x0b0, FaultConfig::none());
        let app = outbound_app(&sim);
        assert_eq!(app.outbound_hosts(), vec!["mail.test", "pay.test"]);
        let c = app.describe();
        let json = to_json(&c);
        assert!(json.contains(CHARGE_JOB), "{json}");
        assert!(json.contains(HOSTS_JSON), "{json}");
        let human = to_human(&c);
        assert!(human.contains("  Charge max=3 backoff_nanos=1000000000 scoped_by=none cross_tenant=false builtin=false outbound=pay.test timeout_ms=2000\n"), "{human}");
        assert!(
            human.contains(
                "cron:\n  charpente.session_sweep interval_nanos=86400000000000 builtin=true\n\
                 outbound_hosts:\n  \
                 pay.test env=PAY_KEY injects=authorization max_request_bytes=1048576 max_reply_bytes=1048576\n  \
                 mail.test env=POSTMARK_TOKEN injects=x-postmark-server-token max_request_bytes=1048576 max_reply_bytes=4194304\n\
                 config:\n"
            ),
            "{human}"
        );
    }

    #[test]
    fn outbound_hosts_is_the_hosts_jobs_call_not_every_declared_entry() {
        let sim = Sim::with_faults(0x0b2, FaultConfig::none());
        let app = outbound_app(&sim)
            .outbound(Host::new("unused.test"))
            .outbound(Host::new("basic.test").basic_env("B_USER", "B_PASS"));
        assert_eq!(app.outbound_hosts(), vec!["mail.test", "pay.test"]);
        let c = app.describe();
        assert_eq!(c.outbound_hosts.len(), 4);
        assert_eq!(c.outbound_hosts[2].host, "unused.test");
        let human = to_human(&c);
        assert!(
            human.contains("  unused.test env=none injects=none max_request_bytes=1048576 max_reply_bytes=4194304\n"),
            "{human}"
        );
        assert!(
            human.contains("  basic.test env=B_USER,B_PASS injects=authorization max_request_bytes=1048576 max_reply_bytes=4194304\n"),
            "{human}"
        );
    }

    #[test]
    fn two_jobs_on_one_host_list_it_once() {
        let sim = Sim::with_faults(0x0b3, FaultConfig::none());
        let app = App::open(
            Box::new(sim.disk()),
            Schema::new(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .unwrap()
        .mount_jobs(jobs![Charge, ChargeInOrg, Mail]);
        assert_eq!(app.outbound_hosts(), vec!["mail.test", "pay.test"]);
    }

    #[test]
    #[should_panic(
        expected = "contract: app.outbound(Host::new(\"pay.test\")) declared twice — one Host entry per host"
    )]
    fn a_host_declared_twice_is_a_contract_panic() {
        let sim = Sim::with_faults(0x0b1, FaultConfig::none());
        let _ = outbound_app(&sim).outbound(Host::new("pay.test"));
    }

    #[test]
    #[should_panic(
        expected = "contract: App::outbound_max_in_flight(0) would never start a call — give at least 1"
    )]
    fn zero_in_flight_is_a_contract_panic() {
        let sim = Sim::with_faults(0x0b4, FaultConfig::none());
        let _ = outbound_app(&sim).outbound_max_in_flight(0);
    }
}

/// A global outbound job whose `call` reads a counter row so a test can see
/// what it saw, and builds the call only while `go` is set.
#[charpente::table(global)]
struct Gate {
    id: charpente::Id<Gate>,
    go: bool,
}

#[job(
    global,
    outbound = "pay.test",
    retry(max = 3, backoff = "1s"),
    timeout = "2s"
)]
struct Gated {
    n: u64,
}

impl Gated {
    fn call(&self, tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        let gate = tx.get(charpente::Id::<Gate>::from_raw(0))?;
        if !gate.is_some_and(|g| g.go) {
            return Ok(None);
        }
        Ok(Some(
            Call::post(format!("/charge/{}", self.n)).header("idempotency-key", key.as_str()),
        ))
    }

    fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
        Ok(())
    }
}

/// `gated_app` without its seeding transaction: the same mounts over the same
/// disk, for a test that reopens after a restart.
fn gated_app_reopen(sim: &Sim) -> App {
    App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Gate>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![Gated, ChargeAsPure, OpenGate])
    .outbound(Host::new("pay.test"))
}

fn gated_app(sim: &Sim, go: bool, jobs: u64) -> App {
    let mut app = gated_app_reopen(sim);
    app.transact_for_tests(|tx| {
        tx.put(&Gate {
            id: charpente::Id::from_raw(0),
            go,
        });
        for n in 0..jobs {
            tx.enqueue(Gated { n });
        }
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    app
}

fn dispatcher_for(app: &mut App, max_in_flight: usize) -> Dispatcher {
    let mut d = Dispatcher::new(app.take_job_defs_for_tests())
        .with_outbound(vec![Host::new("pay.test")], max_in_flight);
    d.rebuild(&app.db_for_tests().snapshot().unwrap()).unwrap();
    d
}

#[test]
fn a_call_that_answers_none_is_done_with_no_start() {
    let sim = Sim::with_faults(0x0c0, FaultConfig::none());
    let mut app = gated_app(&sim, false, 1);
    let mut d = dispatcher_for(&mut app, 64);
    let db = app.db_for_tests();
    let mut group = db.begin_group().unwrap();
    assert!(matches!(
        d.run_due_into(&mut group, 0).unwrap(),
        JobOutcome::Done { id: 0 }
    ));
    group.commit().map_err(|f| f.error).unwrap();
    assert!(d.take_starts().is_empty());
    assert_eq!(d.in_flight_len(), 0);
    let row = db.snapshot().unwrap().scan::<JobRow>().unwrap().remove(0);
    assert_eq!(row.status, JobStatus::Done);
}

#[test]
fn a_decided_call_stages_nothing_and_buffers_its_start_with_the_row_key() {
    let sim = Sim::with_faults(0x0c1, FaultConfig::none());
    let mut app = gated_app(&sim, true, 1);
    let mut d = dispatcher_for(&mut app, 64);
    let db = app.db_for_tests();
    let key = db.snapshot().unwrap().scan::<JobRow>().unwrap()[0]
        .outbound_key
        .unwrap();
    let mut group = db.begin_group().unwrap();
    let JobOutcome::Calling { id: 0, token } = d.run_due_into(&mut group, 0).unwrap() else {
        panic!("a decided call is Calling");
    };
    assert!(group.is_empty(), "R3: call's stage nets no record");
    group.commit().map_err(|f| f.error).unwrap();
    let starts = d.take_starts();
    assert_eq!(starts.len(), 1);
    assert_eq!(starts[0].0, token);
    assert_eq!(starts[0].1.path, "/charge/0");
    assert_eq!(
        starts[0].1.key.as_bytes(),
        &key,
        "the row's minted key, never a fresh draw"
    );
    assert_eq!(d.in_flight_len(), 1);
    assert!(
        !d.has_due(u64::MAX),
        "invariant 2: an in-flight job is in no due-set"
    );
}

#[test]
fn a_full_in_flight_table_never_blocks_a_pure_job() {
    let sim = Sim::with_faults(0x0c2, FaultConfig::none());
    let mut app = gated_app(&sim, true, 3);
    app.transact_for_tests(|tx| {
        tx.enqueue(ChargeAsPure { cents: 1 });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let mut d = dispatcher_for(&mut app, 1);
    let db = app.db_for_tests();
    let mut group = db.begin_group().unwrap();
    assert!(matches!(
        d.run_due_into(&mut group, 0).unwrap(),
        JobOutcome::Calling { id: 0, .. }
    ));
    assert!(
        d.has_due(0),
        "the pure job (id 3) is still due with the table full"
    );
    assert!(matches!(
        d.run_due_into(&mut group, 0).unwrap(),
        JobOutcome::Done { id: 3 }
    ));
    assert!(!d.has_due(u64::MAX), "outbound ids 1 and 2 wait for room");
    group.commit().map_err(|f| f.error).unwrap();
    let rows = db.snapshot().unwrap().scan::<JobRow>().unwrap();
    for id in [1, 2] {
        let row = &rows[id];
        assert_eq!(
            (
                row.id.raw(),
                row.status,
                row.attempts,
                row.last_error.as_deref()
            ),
            (id as u64, JobStatus::Pending, 0, None),
            "waiting for room charges no attempt"
        );
    }
}

#[test]
fn a_refused_call_is_an_attempt_naming_the_rule() {
    // `Gated` with a forbidden header: built by `Leaky`, below.
    let sim = Sim::with_faults(0x0c3, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![Leaky])
    .outbound(Host::new("pay.test").bearer_env("PAY_KEY"));
    app.transact_for_tests(|tx| {
        tx.enqueue(Leaky {});
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let mut d = Dispatcher::new(app.take_job_defs_for_tests())
        .with_outbound(vec![Host::new("pay.test").bearer_env("PAY_KEY")], 64);
    d.rebuild(&app.db_for_tests().snapshot().unwrap()).unwrap();
    let db = app.db_for_tests();
    let mut group = db.begin_group().unwrap();
    assert!(matches!(
        d.run_due_into(&mut group, 0).unwrap(),
        JobOutcome::Retried {
            id: 0,
            attempts: 1,
            ..
        }
    ));
    group.commit().map_err(|f| f.error).unwrap();
    assert!(d.take_starts().is_empty(), "a refused call never starts");
    let row = db.snapshot().unwrap().scan::<JobRow>().unwrap().remove(0);
    assert_eq!(
        row.last_error.as_deref(),
        Some(
            "outbound pay.test: the call sets \"authorization\", which this host's Host entry \
              injects from PAY_KEY — remove it from call; a credential never enters app code"
        )
    );
}

#[job(global, outbound = "pay.test")]
struct Leaky {}

impl Leaky {
    fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(
            Call::get("/x").header("Authorization", "Bearer sk_live_leaked"),
        ))
    }

    fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
        Ok(())
    }
}

/// An app over `schema` with `defs` mounted and `Host::new("pay.test")`
/// declared, and a dispatcher over the same defs and host, rebuilt from the
/// store once `seed` has run in one minting transaction.
fn seeded(
    sim: &Sim,
    schema: Schema,
    defs: Vec<JobDef>,
    seed: impl FnOnce(&mut Tx),
) -> (App, Dispatcher) {
    let mut app = App::open(
        Box::new(sim.disk()),
        schema,
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(defs)
    .outbound(Host::new("pay.test"));
    app.transact_for_tests(|tx| {
        seed(tx);
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let mut d = Dispatcher::new(app.take_job_defs_for_tests())
        .with_outbound(vec![Host::new("pay.test")], 64);
    d.rebuild(&app.db_for_tests().snapshot().unwrap()).unwrap();
    (app, d)
}

#[job(global, outbound = "pay.test")]
struct Boom {}

impl Boom {
    fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        panic!("boom")
    }

    fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
        Ok(())
    }
}

/// Plan amendment A16: a panic inside `call` is an attempt, named as `call`'s.
#[test]
fn a_call_panic_is_an_attempt_with_the_call_panicked_prefix() {
    let sim = Sim::with_faults(0x0c4, FaultConfig::none());
    let (mut app, mut d) = seeded(&sim, Schema::new(), jobs![Boom], |tx| tx.enqueue(Boom {}));
    let db = app.db_for_tests();
    let mut group = db.begin_group().unwrap();
    let outcome = d.run_due_into(&mut group, 0).unwrap();
    group.commit().map_err(|f| f.error).unwrap();
    let JobOutcome::Retried {
        id: 0,
        attempts: 1,
        due_at,
    } = outcome
    else {
        panic!("a panicking call is an attempt: {outcome:?}");
    };
    assert!(d.take_starts().is_empty(), "a panicking call never starts");
    let row = db.snapshot().unwrap().scan::<JobRow>().unwrap().remove(0);
    assert_eq!(row.last_error.as_deref(), Some("call panicked: boom"));
    assert!(
        d.has_due(due_at) && !d.has_due(due_at - 1),
        "the attempt returns to the outbound set at its backoff"
    );
}

#[job(global, outbound = "pay.test")]
struct Probe {
    answer: u64,
}

impl Probe {
    fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(None)
    }

    fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
        Ok(())
    }
}

/// A hand-written `CallGlue` holds `&mut Tx`, so nothing stops it writing: a
/// `Gate` row whose id names the answer it then gives.
fn writing_call(
    tx: &mut Tx,
    payload: &[u8],
    _key: &IdempotencyKey,
    _scope: Option<JobScope<'_>>,
) -> Result<Option<Call>, JobError> {
    let probe = <Probe as Codec>::decode(&mut &payload[..])?;
    tx.put(&Gate {
        id: charpente::Id::from_raw(10 + probe.answer),
        go: true,
    });
    match probe.answer {
        0 => Ok(None),
        1 => Ok(Some(Call::get("/probe"))),
        _ => Err(JobError::app("probe refused")),
    }
}

/// R3 on every path: whatever a hand-written `call` glue writes is discarded
/// whether it declines (`Ok(None)`, the job Done), decides a call, or fails.
#[test]
fn a_hand_written_call_glue_writes_nothing_whatever_it_answers() {
    let sim = Sim::with_faults(0x0c5, FaultConfig::none());
    let mut def = <Probe as JobEntry>::def();
    def.outbound.as_mut().expect("declared outbound").call = writing_call;
    let (mut app, mut d) = seeded(&sim, Schema::new().table::<Gate>(), vec![def], |tx| {
        for answer in 0..3 {
            tx.enqueue(Probe { answer });
        }
    });
    let db = app.db_for_tests();
    let mut group = db.begin_group().unwrap();
    let outcomes: Vec<JobOutcome> = (0..3)
        .map(|_| d.run_due_into(&mut group, 0).unwrap())
        .collect();
    group.commit().map_err(|f| f.error).unwrap();
    assert!(
        matches!(
            outcomes[..],
            [
                JobOutcome::Done { id: 0 },
                JobOutcome::Calling { id: 1, .. },
                JobOutcome::Retried { id: 2, .. }
            ]
        ),
        "{outcomes:?}"
    );
    let snap = db.snapshot().unwrap();
    let written: Vec<u64> = snap
        .scan::<Gate>()
        .unwrap()
        .iter()
        .map(|g| g.id.raw())
        .collect();
    assert_eq!(
        written,
        Vec::<u64>::new(),
        "R3: call's writes never commit (10 = declined, 11 = decided, 12 = failed)"
    );
    let rows = snap.scan::<JobRow>().unwrap();
    assert_eq!(
        rows.iter().map(|r| r.status).collect::<Vec<_>>(),
        vec![JobStatus::Done, JobStatus::Pending, JobStatus::Pending],
        "the declined job is Done all the same"
    );
}

/// The misroute guard: a row with no minted key reaching a `JobDef` that
/// declares `outbound`. Here a hand-built def is `Charge`'s (its run glue is
/// the `unreachable!` stub) under `ChargeAsPure`'s name and fingerprint, so
/// a pure `ChargeAsPure` enqueue reaches it.
#[test]
#[should_panic(
    expected = "contract: job 0 (\"ChargeAsPure\") has no minted key, so dispatch routed it to its \
                run glue, but the mounted `JobDef` declares it outbound (it calls pay.test), and an \
                outbound job's run glue is never called — the `JobDef` is hand-built, with \
                `outbound` set beside a pure job's fingerprint, or a hand-written `impl Job` \
                enqueued its shape with `OUTBOUND = false`; nothing ran, and dispatch stops here \
                rather than spend the job's retries on a glue that cannot run — mount ChargeAsPure \
                through `jobs![…]` and enqueue its #[job] type, which mints the key (R8)"
)]
fn a_row_without_a_key_never_reaches_an_outbound_def_s_run_glue() {
    let sim = Sim::with_faults(0x0c6, FaultConfig::none());
    let mut misrouted = <Charge as JobEntry>::def();
    misrouted.name = <ChargeAsPure as Job>::NAME;
    misrouted.fingerprint = <ChargeAsPure as Job>::FINGERPRINT;
    let (mut app, mut d) = seeded(&sim, Schema::new(), vec![misrouted], |tx| {
        tx.enqueue(ChargeAsPure { cents: 1 })
    });
    let db = app.db_for_tests();
    let mut group = db.begin_group().unwrap();
    let outcome = d.run_due_into(&mut group, 0);
    group.commit().map_err(|f| f.error).unwrap();
    eprintln!("the misroute did not stop dispatch: {outcome:?}");
}

/// The guard's mirror: a row with a minted key reaching a `JobDef` that
/// declares no `outbound`. Here a hand-built def is `Charge`'s with its
/// `outbound` cleared, so a `Charge` enqueue (minted, by `Job::OUTBOUND`)
/// reaches it under the same fingerprint.
#[test]
#[should_panic(
    expected = "contract: job 0 (\"Charge\") carries a minted key, so dispatch routed it to its \
                call glue, but the mounted `JobDef` declares it a pure job (its `outbound` is \
                `None`), and a pure job has no call glue — the `JobDef` is hand-built, with \
                `outbound` cleared beside an outbound job's fingerprint, or a hand-written `impl \
                Job` enqueued a pure job's shape with `OUTBOUND = true`; nothing ran, and dispatch \
                stops here rather than spend the job's retries on a call it cannot build — mount \
                Charge through `jobs![…]` and enqueue its #[job] type (R8)"
)]
fn a_row_with_a_key_never_reaches_a_pure_def() {
    let sim = Sim::with_faults(0x0c9, FaultConfig::none());
    let mut cleared = <Charge as JobEntry>::def();
    cleared.outbound = None;
    let (mut app, mut d) = seeded(&sim, Schema::new(), vec![cleared], |tx| {
        tx.enqueue(Charge { cents: 1 })
    });
    let db = app.db_for_tests();
    let mut group = db.begin_group().unwrap();
    let outcome = d.run_due_into(&mut group, 0);
    group.commit().map_err(|f| f.error).unwrap();
    eprintln!("the misroute did not stop dispatch: {outcome:?}");
}

#[job(global, outbound = "pay.test")]
struct ReadFaults {}

impl ReadFaults {
    fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        // Stands in for a platform read fault inside `call`.
        Err(JobError::Io(std::io::Error::other(
            "simulated read fault (injected)",
        )))
    }

    fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
        Ok(())
    }
}

/// Spec §7: `call` returning `Err(Io)` is not an attempt. It bubbles, the row
/// stays Pending with nothing recorded, the job is due again at once, and
/// nothing starts.
#[test]
fn platform_io_inside_call_is_not_an_attempt() {
    let sim = Sim::with_faults(0x0ca, FaultConfig::none());
    let (mut app, mut d) = seeded(&sim, Schema::new(), jobs![ReadFaults], |tx| {
        tx.enqueue(ReadFaults {})
    });
    let db = app.db_for_tests();
    let err = d.run_due(db, 0).unwrap_err();
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
        d.has_due(0),
        "the job goes straight back in the outbound set"
    );
    assert!(d.take_starts().is_empty(), "and nothing starts");
    assert_eq!(d.in_flight_len(), 0);
}

/// A second Row type over `Gate`'s table name: a read through it is refused.
mod gate_alias {
    #[charpente::table(global)]
    pub struct Gate {
        pub id: charpente::Id<Gate>,
        pub go: bool,
    }
}

#[job(global, outbound = "pay.test")]
struct ReadsAlias {}

impl ReadsAlias {
    fn call(&self, tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        tx.get(charpente::Id::<gate_alias::Gate>::from_raw(0))?;
        Ok(Some(Call::get("/never")))
    }

    fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
        Ok(())
    }
}

/// A read the `Schema` refuses inside `call` comes back through `?` as
/// `JobError::Io`, the variant a platform fault takes, but it is the job's
/// own deterministic bug: an attempt whose `last_error` is the refusal, and
/// no start (the tenancy-completion plan's amendment A18, docs/LEDGER.md,
/// which the run path in dispatch.rs cites too; §7 has no row of its own).
#[test]
fn a_refused_read_inside_call_is_an_attempt_naming_the_refusal() {
    let sim = Sim::with_faults(0x0cb, FaultConfig::none());
    let (mut app, mut d) = seeded(
        &sim,
        Schema::new().table::<Gate>(),
        jobs![ReadsAlias],
        |tx| tx.enqueue(ReadsAlias {}),
    );
    let db = app.db_for_tests();
    let refusal = match db.transact(|tx| {
        tx.get(charpente::Id::<gate_alias::Gate>::from_raw(0))
            .map(|_| ())
    }) {
        Err(TxError::App(e)) if TableReadRefusal::of(&e).is_some() => e.to_string(),
        other => panic!("the alias read is a TableReadRefusal: {other:?}"),
    };
    let outcome = d.run_due(db, 0).unwrap();
    assert!(
        matches!(
            outcome,
            JobOutcome::Retried {
                id: 0,
                attempts: 1,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert!(d.take_starts().is_empty(), "a refused read never starts");
    let row = db.snapshot().unwrap().scan::<JobRow>().unwrap().remove(0);
    assert_eq!(row.last_error.as_deref(), Some(refusal.as_str()));
    assert!(!db.poisoned());
}

#[test]
#[should_panic(
    expected = "contract: Dispatcher::with_outbound(_, 0) would never start a call — give \
                max_in_flight at least 1"
)]
fn a_dispatcher_with_no_room_is_a_contract_panic() {
    let _ = Dispatcher::new(vec![]).with_outbound(vec![], 0);
}

/// The scoped form end to end: `call` reads a tenant-scoped row through its
/// `&ScopedTx`, and the queued start carries what it read.
mod scoped_call {
    use super::*;

    #[charpente::table(scoped_by = Org)]
    struct Invoice {
        id: charpente::Id<Invoice>,
        cents: u64,
    }

    #[job(scoped_by = Org, outbound = "pay.test")]
    struct ChargeInvoice {
        invoice: charpente::Id<Invoice>,
    }

    impl ChargeInvoice {
        fn call(
            &self,
            tx: &ScopedTx<'_, '_, Org>,
            key: &IdempotencyKey,
        ) -> Result<Option<Call>, JobError> {
            let Some(invoice) = tx.get(self.invoice)? else {
                return Err(JobError::app("invoice gone"));
            };
            Ok(Some(
                Call::post(format!("/orgs/{}/charges", tx.scope().raw()))
                    .header("idempotency-key", key.as_str())
                    .form(&[("amount", invoice.cents.to_string())]),
            ))
        }

        fn settle(self, _tx: &mut ScopedTx<'_, '_, Org>, _reply: Reply) -> Result<(), JobError> {
            Ok(())
        }
    }

    #[test]
    fn a_scoped_call_reads_its_own_tenant_and_the_start_carries_it() {
        let sim = Sim::with_faults(0x0c7, FaultConfig::none());
        let mut app = App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Invoice>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .unwrap()
        .mount_jobs(jobs![ChargeInvoice])
        .outbound(Host::new("pay.test"));
        // Invoice 1 exists in both orgs, at different amounts; the job is
        // enqueued in org 7 only, so only org 7's amount may reach the call.
        let mint = sim.entropy();
        let mut group = app.db_for_tests().begin_group_minting(&mint).unwrap();
        let admission = group.tenant_admission();
        for (org, cents) in [(8u64, 999u64), (7, 250)] {
            group
                .stage(|tx| {
                    let mut scoped = tx.admit_scope(Id::<Org>::from_raw(org), &admission);
                    scoped.put(&Invoice {
                        id: Id::from_raw(1),
                        cents,
                    });
                    if org == 7 {
                        scoped.enqueue(ChargeInvoice {
                            invoice: Id::from_raw(1),
                        });
                    }
                    Ok::<(), std::io::Error>(())
                })
                .unwrap();
        }
        group.commit().map_err(|f| f.error).unwrap();
        let mut d = Dispatcher::new(app.take_job_defs_for_tests())
            .with_outbound(vec![Host::new("pay.test")], 64);
        d.rebuild(&app.db_for_tests().snapshot().unwrap()).unwrap();
        let db = app.db_for_tests();
        let row = db.snapshot().unwrap().scan::<JobRow>().unwrap().remove(0);
        assert_eq!(row.scope, Some(7));
        let mut group = db.begin_group().unwrap();
        let outcome = d.run_due_into(&mut group, 0).unwrap();
        assert!(
            matches!(outcome, JobOutcome::Calling { id: 0, .. }),
            "{outcome:?}"
        );
        assert!(
            group.is_empty(),
            "R3: the scoped call's stage nets no record"
        );
        group.commit().map_err(|f| f.error).unwrap();
        let starts = d.take_starts();
        assert_eq!(starts.len(), 1);
        let call = &starts[0].1;
        assert_eq!(
            (call.host, call.path.as_str()),
            ("pay.test", "/orgs/7/charges")
        );
        assert_eq!(call.body, b"amount=250", "org 7's invoice, not org 8's");
        let key = IdempotencyKey::from_bytes(row.outbound_key.expect("minted"));
        assert_eq!(call.key, key);
        assert!(
            call.headers
                .contains(&("idempotency-key".to_string(), key.as_str().to_string())),
            "{:?}",
            call.headers
        );
    }
}

/// Every group the platform opens mints an outbound enqueue's key (plan
/// amendment A9): a route's (`App::begin_group`), the kernel's job arm (a
/// job's `run` enqueues) and its cron arm (a tick enqueues). `Minted` is
/// deliberately not mounted: a kernel over a mounted outbound job needs an
/// `Outbound` to start its calls, and `Group::stage` mints by `Job::OUTBOUND`
/// at the enqueue, not by the mount. Unmounted, each `Minted` row dispatches
/// as a "not registered" attempt, which keeps its row and its key.
mod mint_paths {
    use super::*;
    use charpente::http::{Method, Request};
    use charpente::kernel::{Kernel, SimSource};
    use charpente::{route, routes};

    const SEC: u64 = 1_000_000_000;
    const ROUTE: u64 = 0;
    const JOB: u64 = 1;
    const CRON: u64 = 2;

    #[job(global, outbound = "pay.test")]
    pub struct Minted {
        pub via: u64,
    }

    impl Minted {
        fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
            Ok(None)
        }

        fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
            Ok(())
        }
    }

    #[job(global)]
    pub struct Relay {}

    impl Relay {
        fn run(self, tx: &mut Tx) -> Result<(), JobError> {
            tx.enqueue(Minted { via: JOB });
            Ok(())
        }
    }

    #[route(POST "/relay", public)]
    fn relay(tx: &mut Tx) -> Result<(), ApiError> {
        tx.enqueue(Minted { via: ROUTE });
        Ok(())
    }

    #[test]
    fn a_route_a_job_and_a_cron_tick_each_mint_their_own_key() {
        let sim = Sim::with_faults(0x0c8, FaultConfig::none());
        let mut app = App::open(
            Box::new(sim.disk()),
            Schema::new(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .unwrap()
        .mount(routes![relay])
        .mount_jobs(jobs![Relay])
        .schedule("mint", Duration::from_secs(60), || Minted { via: CRON });
        app.transact_for_tests(|tx| {
            tx.enqueue(Relay {});
            Ok::<(), std::io::Error>(())
        })
        .unwrap();
        let mut source = SimSource::new(Box::new(sim.clock()), 90 * SEC);
        source.push_at(0, Request::new(Method::Post, "/relay"));
        let responses = source.responses();
        let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
        kernel.run(&mut source).unwrap();
        assert_eq!(
            responses
                .lock()
                .unwrap()
                .iter()
                .map(|r| r.0)
                .collect::<Vec<_>>(),
            vec![200],
            "the route ran"
        );
        let rows = kernel
            .app()
            .db()
            .snapshot()
            .unwrap()
            .scan::<JobRow>()
            .unwrap();
        let mut keys = std::collections::BTreeMap::new();
        for row in rows.iter().filter(|r| r.name == "Minted") {
            let via = <Minted as Codec>::decode(&mut &row.payload[..])
                .unwrap()
                .via;
            let key = row
                .outbound_key
                .unwrap_or_else(|| panic!("the enqueue via {via} drew no key"));
            assert!(keys.insert(via, key).is_none(), "one enqueue via {via}");
            assert_eq!(
                row.last_error.as_deref(),
                Some(
                    "job \"Minted\" is not registered — add it to the jobs![…] list passed to \
                     App::mount_jobs (or this log outlived a renamed job type)"
                ),
                "the call path names an unmounted job as the run path does"
            );
        }
        assert_eq!(
            keys.keys().copied().collect::<Vec<_>>(),
            vec![ROUTE, JOB, CRON],
            "a route, a job and a cron tick each enqueued once"
        );
        let distinct: std::collections::BTreeSet<_> = keys.values().collect();
        assert_eq!(distinct.len(), 3, "each enqueue drew its own key");
        assert!(
            rows.iter()
                .filter(|r| r.name == "Relay")
                .all(|r| r.outbound_key.is_none()),
            "a pure enqueue draws none"
        );
    }
}

// ------------------------------------------------- the kernel's outbound arm

const SEC: u64 = 1_000_000_000;

#[charpente::table(global)]
struct Paid {
    id: charpente::Id<Paid>,
    /// How many times `settle` recorded this payment: 1 for every Done job,
    /// or `settle` committed twice (Task 12's exactly-once oracle).
    times: u32,
}

/// Charges once per job; `settle` records the payment on a 200 and treats
/// anything else as an attempt. Used by every §7 row below.
#[job(
    global,
    outbound = "pay.test",
    retry(max = 3, backoff = "500ms"),
    timeout = "2s"
)]
struct Pay {
    n: u64,
}

impl Pay {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(
            Call::post(format!("/pay/{}", self.n)).header("idempotency-key", key.as_str()),
        ))
    }

    fn settle(self, tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        match reply.status() {
            200 => {
                let id = charpente::Id::from_raw(self.n);
                let times = tx.get(id)?.map_or(0, |p: Paid| p.times);
                tx.put(&Paid {
                    id,
                    times: times + 1,
                });
                Ok(())
            }
            s => Err(JobError::app(format!("pay.test answered {s}"))),
        }
    }
}

fn pay_app(sim: &Sim, host: Host, jobs: u64) -> App {
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Paid>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![Pay])
    .outbound(host);
    app.transact_for_tests(|tx| {
        for n in 0..jobs {
            tx.enqueue(Pay { n });
        }
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    app
}

/// Runs to `horizon` virtual nanos and hands back the App and the stats.
fn run_to(sim: &Sim, app: App, horizon: u64) -> (App, KernelStats) {
    let outbound = sim.outbound(&app.outbound_hosts()).unwrap();
    let mut kernel = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)).unwrap();
    kernel
        .run(&mut SimSource::new(Box::new(sim.clock()), horizon))
        .unwrap();
    let stats = kernel.stats();
    (kernel.into_app(), stats)
}

fn jobs_and_paid(app: &mut App) -> (Vec<JobRow>, usize) {
    let snap = app.db_for_tests().snapshot().unwrap();
    (
        snap.scan::<JobRow>().unwrap(),
        snap.scan::<Paid>().unwrap().len(),
    )
}

fn faults(f: impl FnOnce(&mut FaultConfig)) -> FaultConfig {
    let mut cfg = FaultConfig::none();
    f(&mut cfg);
    cfg
}

#[test]
fn a_settled_reply_commits_its_effects_with_the_done_mark() {
    let sim = Sim::with_faults(0x0d0, FaultConfig::none());
    sim.remote("pay.test", Recorder::new());
    let app = pay_app(&sim, Host::new("pay.test"), 1);
    let (mut app, stats) = run_to(&sim, app, 60 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!((rows[0].status, paid), (JobStatus::Done, 1));
    assert_eq!(
        (
            stats.outbound_started,
            stats.outbound_settled,
            stats.outbound_failed
        ),
        (1, 1, 0)
    );
    let key = IdempotencyKey::from_bytes(rows[0].outbound_key.unwrap());
    sim.with_remote("pay.test", |r: &Recorder| {
        assert_eq!(r.calls().len(), 1);
        assert_eq!(
            r.calls()[0].headers,
            vec![("idempotency-key".to_string(), key.as_str().to_string())]
        );
    });
}

#[test]
fn a_refused_connection_is_an_attempt_that_says_it_never_arrived() {
    let sim = Sim::with_faults(0x0d1, faults(|c| c.outbound_refused = 1.0));
    sim.remote("pay.test", Recorder::new());
    let (mut app, _) = run_to(&sim, pay_app(&sim, Host::new("pay.test"), 1), 60 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!(
        (rows[0].status, rows[0].attempts, paid),
        (JobStatus::Dead, 3, 0)
    );
    assert_eq!(
        rows[0].last_error.as_deref(),
        Some("outbound pay.test: connection refused — the call did not reach the remote")
    );
    sim.with_remote("pay.test", |r: &Recorder| assert!(r.calls().is_empty()));
}

#[test]
fn a_lost_reply_times_out_and_every_retry_carries_the_same_key() {
    let sim = Sim::with_faults(0x0d2, faults(|c| c.outbound_lost_reply = 1.0));
    sim.remote(
        "pay.test",
        Idempotent::header("idempotency-key", Recorder::new()),
    );
    let (mut app, stats) = run_to(&sim, pay_app(&sim, Host::new("pay.test"), 1), 60 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!(
        (rows[0].status, paid),
        (JobStatus::Dead, 0),
        "Dead means no reply was settled"
    );
    assert_eq!(
        rows[0].last_error.as_deref(),
        Some(
            "outbound pay.test: timed out after 2s — the remote may have applied this call; the \
              retry carries the same idempotency key"
        )
    );
    assert_eq!(stats.outbound_timed_out, 3);
    let key = IdempotencyKey::from_bytes(rows[0].outbound_key.unwrap());
    let delivered = sim.outbound_deliveries();
    assert_eq!(delivered.len(), 3);
    assert!(delivered.iter().all(|d| d.key == key));
    sim.with_remote("pay.test", |m: &Idempotent<Recorder>| {
        assert_eq!(
            m.applied().get(key.as_str()),
            Some(&1),
            "applied once, replayed twice"
        );
        assert_eq!(
            m.inner().calls().len(),
            1,
            "the remote applied the effect once"
        );
    });
}

#[test]
fn a_settle_error_discards_its_effects_and_is_an_attempt() {
    let sim = Sim::with_faults(0x0d3, FaultConfig::none());
    sim.remote(
        "pay.test",
        from_fn(|_c: &OutboundCall| OutboundReply {
            status: 503,
            headers: vec![],
            body: vec![],
        }),
    );
    let (mut app, stats) = run_to(&sim, pay_app(&sim, Host::new("pay.test"), 1), 60 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!((rows[0].status, paid), (JobStatus::Dead, 0));
    assert_eq!(rows[0].last_error.as_deref(), Some("pay.test answered 503"));
    assert_eq!((stats.outbound_started, stats.outbound_failed), (3, 3));
}

/// Review Focus 4: a reply over the host's cap is an attempt whose text says
/// the remote applied the call (plan amendment A10).
#[test]
fn an_oversized_reply_is_a_named_attempt() {
    let sim = Sim::with_faults(0x0d4, FaultConfig::none());
    sim.remote(
        "pay.test",
        from_fn(|_c: &OutboundCall| OutboundReply {
            status: 200,
            headers: vec![],
            body: vec![b'x'; 17],
        }),
    );
    let host = Host::new("pay.test").max_reply_bytes(16);
    let (mut app, _) = run_to(&sim, pay_app(&sim, host, 1), 60 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!((rows[0].status, paid), (JobStatus::Dead, 0));
    assert_eq!(
        rows[0].last_error.as_deref(),
        Some(
            "outbound pay.test: the reply exceeded the 16-byte reply cap — the remote \
              answered, so it applied this call; raise the cap with Host::max_reply_bytes (the \
              retry carries the same idempotency key)"
        )
    );
}

/// Production's documented race (io.rs, `Outbound::cancel`: "A completion for
/// it may still surface; the kernel discards it"): a cancel that loses to the
/// reply. `SimOutbound` never hands out a cancelled flight's completion, so
/// the kernel's late-discard path runs only behind this wrapper, which drops
/// every cancel and forwards everything else.
struct NoCancel<O>(O);

impl<O: Outbound> Outbound for NoCancel<O> {
    fn start(&self, token: u64, call: OutboundCall) -> std::io::Result<()> {
        self.0.start(token, call)
    }
    fn poll(&self) -> Option<(u64, std::io::Result<OutboundReply>)> {
        self.0.poll()
    }
    fn cancel(&self, _token: u64) {}
    fn next_wake(&self) -> OutboundWake {
        self.0.next_wake()
    }
}

/// Review Focus 2 / R7, through `NoCancel`. Timeline (one-way latency 1.5s,
/// timeout 2s, backoff 500ms then 1s): token 0 starts at 0 and times out at
/// 2s; token 1 starts at 2.5s; token 0's reply lands at 3s while token 1 is in
/// flight — late, and it must not settle token 1's attempt. Token 1 times out
/// at 4.5s and its reply (5.5s) is late too, polled one step after token 2
/// starts at 5.5s; token 2 times out at 7.5s, and the job is Dead. Token 2's
/// own reply (8.5s) is never polled: nothing is in flight by then. If the
/// count below differs, re-derive it from this timeline before changing it.
#[test]
fn a_late_reply_never_settles_the_retry() {
    let lat = Duration::from_millis(1500);
    let sim = Sim::with_faults(0x0d5, faults(|c| c.outbound_latency = (lat, lat)));
    sim.remote("pay.test", Recorder::new());
    let app = pay_app(&sim, Host::new("pay.test"), 1);
    let outbound = NoCancel(sim.outbound(&app.outbound_hosts()).unwrap());
    let mut kernel = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)).unwrap();
    kernel
        .run(&mut SimSource::new(Box::new(sim.clock()), 60 * SEC))
        .unwrap();
    let stats = kernel.stats();
    let mut app = kernel.into_app();
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!(
        (rows[0].status, paid),
        (JobStatus::Dead, 0),
        "no late reply settled anything"
    );
    assert_eq!(stats.outbound_settled, 0);
    assert_eq!(stats.outbound_late, 2);
    assert_eq!(stats.outbound_timed_out, 3);
}

/// Review Focus 1 / R6: a call that read a sibling's uncommitted write never
/// starts when the batch's commit fails. Swept over every crash point the run
/// reaches; the property is "a delivery implies the write it read is durable".
/// Beside it, the other side of R6: a batch that fails to commit abandons its
/// starts, so no flight is left in the table without its start on the wire.
#[test]
fn a_call_never_starts_when_its_group_commit_fails() {
    let mut failed_commits = 0;
    let mut abandoned = 0;
    for crash_after in 0..400u64 {
        let sim = Sim::with_faults(0x0d6, FaultConfig::none());
        sim.remote("pay.test", Recorder::new());
        let mut app = gated_app(&sim, false, 0);
        app.transact_for_tests(|tx| {
            tx.enqueue(OpenGate {});
            tx.enqueue(Gated { n: 0 });
            Ok::<(), std::io::Error>(())
        })
        .unwrap();
        let outbound = sim.outbound(&app.outbound_hosts()).unwrap();
        sim.crash_disk_after(crash_after);
        let Ok(mut kernel) = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound))
        else {
            continue; // the crash landed in kernel build: nothing ran
        };
        let ran = kernel.run(&mut SimSource::new(Box::new(sim.clock()), 60 * SEC));
        let stats = kernel.stats();
        let in_flight = kernel.outbound_in_flight_for_tests() as u64;
        if ran.is_err() {
            failed_commits += 1;
            // The job batch staged its jobs (`OpenGate`'s Done counts as it
            // is staged) and no start went out: its commit, or a read inside
            // it, failed with the call decided or about to be.
            if stats.jobs_done > 0 && stats.outbound_started == 0 {
                abandoned += 1;
            }
        }
        drop(kernel);
        let delivered = !sim.outbound_deliveries().is_empty();
        sim.restart();
        let mut reopened = gated_app_reopen(&sim);
        let gate_durable = reopened
            .db_for_tests()
            .snapshot()
            .unwrap()
            .get::<Gate>(charpente::Id::from_raw(0))
            .unwrap()
            .is_some_and(|g| g.go);
        assert!(
            !delivered || gate_durable,
            "crash_after {crash_after}: a call that read the gate was delivered, but the write it \
             read never became durable (R6)"
        );
        assert!(
            in_flight <= stats.outbound_started,
            "crash_after {crash_after}: {in_flight} flight(s) in the table but {} start(s) \
             issued — a batch that failed to commit kept a flight whose start never went out, \
             so its job is in no due-set and its call is not on the wire (abandon_starts, R6)",
            stats.outbound_started
        );
    }
    assert!(
        failed_commits > 0,
        "the sweep never failed a commit, so it proved nothing"
    );
    assert!(
        abandoned > 0,
        "the sweep never failed a job batch before its start, so R6's abandoned side went \
         unexercised"
    );
}

#[job(global)]
struct OpenGate {}

impl OpenGate {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        tx.put(&Gate {
            id: charpente::Id::from_raw(0),
            go: true,
        });
        Ok(())
    }
}

/// Review Focus 5 / invariant 3, and oracle 2's switchover row (plan
/// amendment A19): a switchover reaches the kernel only as this write gate
/// (`Role::HandingOff` calls `forbid_writes`, `repl/mod.rs:300`), and the
/// kernel's drop is level-triggered on the gate, never on the role.
#[test]
fn losing_writability_drops_in_flight_and_keeps_jobs_pending() {
    let lat = Duration::from_millis(500);
    let sim = Sim::with_faults(0x0d7, faults(|c| c.outbound_latency = (lat, lat)));
    sim.remote(
        "pay.test",
        Idempotent::header("idempotency-key", Recorder::new()),
    );
    let app = pay_app(&sim, Host::new("pay.test"), 1);
    let outbound = sim.outbound(&app.outbound_hosts()).unwrap();
    let mut kernel = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)).unwrap();
    let mut source = SimSource::new(Box::new(sim.clock()), 60 * SEC);
    while kernel.outbound_in_flight_for_tests() == 0 {
        kernel.step(&mut source).unwrap();
    }
    kernel
        .app_mut_for_tests()
        .db_for_tests()
        .forbid_writes("elsewhere:1".into());
    kernel.step(&mut source).unwrap();
    assert_eq!(kernel.outbound_in_flight_for_tests(), 0, "invariant 3");
    sim.clock().sleep(Duration::from_secs(5));
    kernel.step(&mut source).unwrap();
    let mut app = kernel.into_app();
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!(
        (rows[0].status, rows[0].attempts, paid),
        (JobStatus::Pending, 0, 0)
    );
    let key = rows[0].outbound_key;

    // The next writable kernel re-dispatches with the same key. The first
    // kernel's build took the job defs and the Host entries, so mount them again.
    let mut app = app.mount_jobs(jobs![Pay]).outbound(Host::new("pay.test"));
    app.db_for_tests().allow_writes();
    let (mut app, _) = run_to(&sim, app, 120 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!(
        (rows[0].status, rows[0].outbound_key, paid),
        (JobStatus::Done, key, 1)
    );
    let k = IdempotencyKey::from_bytes(key.unwrap());
    sim.with_remote("pay.test", |m: &Idempotent<Recorder>| {
        assert_eq!(
            m.applied().get(k.as_str()),
            Some(&1),
            "two deliveries, one application"
        );
        assert_eq!(
            m.inner().calls().len(),
            1,
            "the remote applied the effect once"
        );
    });
    let delivered = sim.outbound_deliveries();
    assert_eq!(delivered.len(), 2);
    assert!(
        delivered.iter().all(|d| d.key == k),
        "the re-dispatch sent the recorded key, not a fresh one"
    );
}

#[test]
fn a_crash_mid_flight_re_dispatches_with_the_same_key() {
    let lat = Duration::from_secs(1);
    let sim = Sim::with_faults(0x0d8, faults(|c| c.outbound_latency = (lat, lat)));
    sim.remote(
        "pay.test",
        Idempotent::header("idempotency-key", Recorder::new()),
    );
    let app = pay_app(&sim, Host::new("pay.test"), 1);
    let (app, _) = run_to(&sim, app, SEC / 2); // started, not answered
    drop(app);
    sim.restart();
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Paid>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![Pay])
    .outbound(Host::new("pay.test"));
    let (mut app, _) = run_to(&sim, app, 60 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!((rows[0].status, paid), (JobStatus::Done, 1));
    let delivered = sim.outbound_deliveries();
    assert_eq!(delivered.len(), 2, "the pre-crash request still arrived");
    let k = IdempotencyKey::from_bytes(rows[0].outbound_key.unwrap());
    assert!(
        delivered.iter().all(|d| d.key == k),
        "the re-dispatch sent the recorded key, not a fresh one"
    );
    sim.with_remote("pay.test", |m: &Idempotent<Recorder>| {
        assert_eq!(m.applied().get(k.as_str()), Some(&1));
        assert_eq!(
            m.inner().calls().len(),
            1,
            "the remote applied the effect once"
        );
    });
}

/// Task 10 could not free a slot: with the table full, a due outbound job
/// waits, Pending and charged nothing, and dispatches the moment a reply
/// frees the slot.
#[test]
fn a_capacity_blocked_outbound_job_dispatches_once_a_slot_frees() {
    let lat = Duration::from_millis(500);
    let sim = Sim::with_faults(0x0df, faults(|c| c.outbound_latency = (lat, lat)));
    sim.remote("pay.test", Recorder::new());
    let app = pay_app(&sim, Host::new("pay.test"), 2).outbound_max_in_flight(1);
    let outbound = sim.outbound(&app.outbound_hosts()).unwrap();
    let mut kernel = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)).unwrap();
    // Job 0's reply lands at 1s; stop at 0.75s, with its call in flight.
    kernel
        .run(&mut SimSource::new(Box::new(sim.clock()), 3 * SEC / 4))
        .unwrap();
    assert_eq!(kernel.outbound_in_flight_for_tests(), 1);
    assert_eq!(
        kernel.stats().outbound_started,
        1,
        "job 1 waits for the slot"
    );
    let rows = kernel
        .app()
        .db()
        .snapshot()
        .unwrap()
        .scan::<JobRow>()
        .unwrap();
    assert_eq!(
        (
            rows[1].status,
            rows[1].attempts,
            rows[1].last_error.as_deref()
        ),
        (JobStatus::Pending, 0, None),
        "waiting for a slot charges no attempt"
    );
    kernel
        .run(&mut SimSource::new(Box::new(sim.clock()), 60 * SEC))
        .unwrap();
    let stats = kernel.stats();
    let mut app = kernel.into_app();
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!(
        rows.iter()
            .map(|r| (r.status, r.attempts))
            .collect::<Vec<_>>(),
        vec![(JobStatus::Done, 0); 2]
    );
    assert_eq!(paid, 2);
    assert_eq!((stats.outbound_started, stats.outbound_settled), (2, 2));
    let at: Vec<u64> = sim.outbound_deliveries().iter().map(|d| d.at).collect();
    assert_eq!(
        at,
        vec![SEC / 2, 3 * SEC / 2],
        "job 1's call started only when job 0's reply (1s) freed the slot"
    );
}

#[test]
fn a_kernel_without_an_outbound_refuses_by_name() {
    let sim = Sim::with_faults(0x0d9, FaultConfig::none());
    let app = pay_app(&sim, Host::new("pay.test"), 0);
    let err = Kernel::new(app, Box::new(sim.clock()))
        .err()
        .expect("refused");
    assert_eq!(
        err.to_string(),
        "kernel: job Pay calls pay.test, but this kernel has no Outbound — serve() supplies the \
         production client; a sim test passes one: let outbound = \
         sim.outbound(&app.outbound_hosts())?; Kernel::with_outbound(app, clock, Box::new(outbound))"
    );
    assert!(Refusal::of(&err).is_some());
}

#[test]
fn an_unmatched_host_set_refuses_the_kernel() {
    let sim = Sim::with_faults(0x0da, FaultConfig::none());
    sim.remote("pay.test", Recorder::new());
    let app = pay_app(&sim, Host::new("pay.test"), 0).outbound(Host::new("extra.test"));
    let outbound = sim.outbound(&app.outbound_hosts()).unwrap();
    let err = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound))
        .err()
        .expect("refused");
    assert_eq!(
        err.to_string(),
        "kernel: app.outbound(Host::new(\"extra.test\")) declares a host no mounted job calls — \
         remove it, or mount the job that calls it"
    );
    assert!(Refusal::of(&err).is_some());
}

/// Invariant 6: a kernel that mounts no outbound job calls no `Outbound`
/// method, however long it runs.
struct Tripwire;

impl Outbound for Tripwire {
    fn start(&self, _: u64, _: OutboundCall) -> std::io::Result<()> {
        panic!("invariant 6: start on a kernel with no outbound job")
    }
    fn poll(&self) -> Option<(u64, std::io::Result<OutboundReply>)> {
        panic!("invariant 6: poll on a kernel with no outbound job")
    }
    fn cancel(&self, _: u64) {
        panic!("invariant 6: cancel on a kernel with no outbound job")
    }
    fn next_wake(&self) -> OutboundWake {
        panic!("invariant 6: next_wake on a kernel with no outbound job")
    }
}

#[test]
fn a_kernel_with_no_outbound_job_never_touches_its_outbound() {
    let sim = Sim::with_faults(0x0db, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![ChargeAsPure]);
    app.transact_for_tests(|tx| {
        tx.enqueue(ChargeAsPure { cents: 1 });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let mut kernel = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(Tripwire)).unwrap();
    kernel
        .run(&mut SimSource::new(
            Box::new(sim.clock()),
            3 * 24 * 3600 * SEC,
        ))
        .unwrap();
    assert_eq!(
        kernel.stats().jobs_done,
        1 + 3,
        "the job plus three daily session sweeps"
    );
}

mod scoped_outbound {
    use super::*;

    #[charpente::table(global)]
    pub struct Shop {
        pub id: charpente::Id<Shop>,
        pub name: String,
    }

    impl Tenant for Shop {
        const NAME: &'static str = "Shop";
        type Who = Principal;
        type Error = ApiError;
        fn authorize(
            _tx: &Tx,
            _who: &Principal,
            _scope: charpente::Id<Shop>,
        ) -> Result<(), ApiError> {
            panic!("a job has no principal — Shop::authorize is never asked")
        }
    }

    #[charpente::table(scoped_by = Shop)]
    pub struct Sale {
        pub id: charpente::Id<Sale>,
        pub charged: bool,
    }

    #[job(
        scoped_by = Shop,
        outbound = "pay.test",
        retry(max = 3, backoff = "500ms"),
        timeout = "2s"
    )]
    pub struct ChargeSale {
        pub sale: charpente::Id<Sale>,
    }

    impl ChargeSale {
        fn call(
            &self,
            tx: &ScopedTx<'_, '_, Shop>,
            key: &IdempotencyKey,
        ) -> Result<Option<Call>, JobError> {
            let Some(sale) = tx.get(self.sale)? else {
                return Err(JobError::app("sale gone"));
            };
            if sale.charged {
                return Ok(None);
            }
            Ok(Some(
                Call::post(format!(
                    "/shops/{}/sales/{}",
                    tx.scope().raw(),
                    self.sale.raw()
                ))
                .header("idempotency-key", key.as_str()),
            ))
        }

        fn settle(self, tx: &mut ScopedTx<'_, '_, Shop>, reply: Reply) -> Result<(), JobError> {
            if reply.status() != 200 {
                return Err(JobError::app(format!(
                    "pay.test answered {}",
                    reply.status()
                )));
            }
            tx.put(&Sale {
                id: self.sale,
                charged: true,
            });
            Ok(())
        }
    }

    #[test]
    fn a_scoped_outbound_job_calls_and_settles_inside_its_tenant() {
        let sim = Sim::with_faults(0x0dc, FaultConfig::none());
        sim.remote("pay.test", Recorder::new());
        let mut app = App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Sale>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .unwrap()
        .mount_jobs(jobs![ChargeSale])
        .outbound(Host::new("pay.test"));
        // The operator path (kernel.rs's `enqueue_in_desk`), through a minting group.
        let mint = sim.entropy();
        for shop in [3u64, 4] {
            let mut group = app.db_for_tests().begin_group_minting(&mint).unwrap();
            let admission = group.tenant_admission();
            group
                .stage(|tx| {
                    let mut scoped =
                        tx.admit_scope(charpente::Id::<Shop>::from_raw(shop), &admission);
                    scoped.put(&Sale {
                        id: charpente::Id::from_raw(1),
                        charged: false,
                    });
                    scoped.enqueue(ChargeSale {
                        sale: charpente::Id::from_raw(1),
                    });
                    Ok::<(), std::io::Error>(())
                })
                .unwrap();
            group.commit().map_err(|f| f.error).unwrap();
        }
        let (mut app, stats) = run_to(&sim, app, 60 * SEC);
        sim.with_remote("pay.test", |r: &Recorder| {
            let mut paths: Vec<&str> = r.calls().iter().map(|c| c.path.as_str()).collect();
            paths.sort_unstable();
            assert_eq!(
                paths,
                vec!["/shops/3/sales/1", "/shops/4/sales/1"],
                "call ran in each job's own tenant"
            );
        });
        let snap = app.db_for_tests().snapshot().unwrap();
        for shop in [3u64, 4] {
            let sale = snap
                .get_scoped::<Sale>(shop, charpente::Id::from_raw(1))
                .unwrap()
                .unwrap();
            assert!(sale.charged, "settle wrote in shop {shop}, its own tenant");
        }
        let jobs = snap.scan::<JobRow>().unwrap();
        assert_eq!(
            jobs.iter()
                .map(|r| (r.scope, r.status, r.attempts))
                .collect::<Vec<_>>(),
            vec![(Some(3), JobStatus::Done, 0), (Some(4), JobStatus::Done, 0)],
            "each settle committed with its job's Done mark, first time"
        );
        assert_eq!((stats.outbound_started, stats.outbound_settled), (2, 2));
    }
}

#[test]
fn cron_enqueues_an_outbound_job_with_a_minted_key() {
    let sim = Sim::with_faults(0x0dd, FaultConfig::none());
    sim.remote("pay.test", Recorder::new());
    let app = pay_app(&sim, Host::new("pay.test"), 0).schedule(
        "hourly",
        Duration::from_secs(3600),
        || Pay { n: 9 },
    );
    let (mut app, _) = run_to(&sim, app, 2 * 3600 * SEC + 60 * SEC);
    let (rows, _) = jobs_and_paid(&mut app);
    let pay: Vec<_> = rows.iter().filter(|r| r.name == "Pay").collect();
    assert!(pay.len() >= 2, "a tick per hour: {}", pay.len());
    let keys: std::collections::BTreeSet<_> = pay
        .iter()
        .map(|r| {
            r.outbound_key
                .expect("the cron arm's group minted it (plan amendment A9)")
        })
        .collect();
    assert_eq!(keys.len(), pay.len(), "one key per tick, never shared");
    assert!(pay.iter().all(|r| r.status == JobStatus::Done));
}

mod shape_v1 {
    use super::*;
    #[job(global)]
    pub struct Settle {
        pub n: u64,
    }
    impl Settle {
        fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
            Ok(())
        }
    }
}

mod shape_v2 {
    use super::*;
    #[job(global, outbound = "pay.test")]
    pub struct Settle {
        pub n: u64,
    }
    impl Settle {
        fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
            Ok(None)
        }
        fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
            Ok(())
        }
    }
}

/// R8 / spec §14: declaring `outbound` is a shape change, refused by the
/// existing refusal while rows of the old shape are Pending.
#[test]
fn a_job_gaining_outbound_is_refused_while_old_rows_are_pending() {
    let sim = Sim::with_faults(0x0de, FaultConfig::none());
    sim.remote("pay.test", Recorder::new());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap();
    app.db_for_tests()
        .transact(|tx| {
            tx.enqueue(shape_v1::Settle { n: 1 });
            Ok::<(), std::io::Error>(())
        })
        .unwrap();
    let app = app
        .mount_jobs(jobs![shape_v2::Settle])
        .outbound(Host::new("pay.test"));
    let outbound = sim.outbound(&app.outbound_hosts()).unwrap();
    let err = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound))
        .err()
        .expect("refused");
    assert!(
        err.to_string().starts_with(
            "1 Pending \"Settle\" job(s) (ids 0) were enqueued under a different shape"
        ),
        "{err}"
    );
    assert!(
        err.to_string().contains("its `outbound` declaration"),
        "Task 5 Step 5a: the fix names outbound: {err}"
    );
    assert!(Refusal::of(&err).is_some());
}

#[job(
    global,
    outbound = "pay.test",
    retry(max = 1, backoff = "1s"),
    timeout = "1s"
)]
struct Expiring {}

impl Expiring {
    fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(Call::post("/expiring")))
    }

    fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
        Ok(())
    }
}

/// The completion buffer can outlive the table's last flight: a drain stops
/// at `MAX_BATCH` (64) settled completions, and a late one queued behind them
/// stays buffered with nothing in flight. Losing writability then must drop
/// it with the table, or the outbound arm runs behind a shut gate. Timeline
/// (one-way latency 0.75s): 64 `Pay` calls and one `Expiring` start at 0;
/// `Expiring` times out at 1s (Dead, its cancel dropped by `NoCancel`); all 65
/// replies land at 1.5s, `Expiring`'s last; the arm settles the 64 and stops.
#[test]
fn a_completion_left_behind_the_last_flight_goes_with_the_gate() {
    use charpente::kernel::{RequestSource, Step};
    let lat = Duration::from_millis(750);
    let sim = Sim::with_faults(0x0e0, faults(|c| c.outbound_latency = (lat, lat)));
    sim.remote("pay.test", Recorder::new());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Paid>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![Pay, Expiring])
    .outbound(Host::new("pay.test"))
    .outbound_max_in_flight(65);
    app.transact_for_tests(|tx| {
        for n in 0..64 {
            tx.enqueue(Pay { n });
        }
        tx.enqueue(Expiring {});
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let outbound = NoCancel(sim.outbound(&app.outbound_hosts()).unwrap());
    let mut kernel = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)).unwrap();
    let mut source = SimSource::new(Box::new(sim.clock()), 60 * SEC);
    while kernel.stats().outbound_settled < 64 {
        if let Step::Idle { deadline } = kernel.step(&mut source).unwrap() {
            source.wait_until(deadline);
        }
    }
    let stats = kernel.stats();
    assert_eq!(
        (
            stats.outbound_timed_out,
            stats.outbound_late,
            kernel.outbound_in_flight_for_tests()
        ),
        (1, 0, 0),
        "nothing in flight, and Expiring's late reply still buffered"
    );
    kernel
        .app_mut_for_tests()
        .db_for_tests()
        .forbid_writes("elsewhere:1".into());
    kernel.step(&mut source).unwrap();
    kernel.app_mut_for_tests().db_for_tests().allow_writes();
    kernel.step(&mut source).unwrap();
    assert_eq!(
        kernel.stats().outbound_late,
        0,
        "the buffered completion was dropped with the gate, never handed to the arm"
    );
}
