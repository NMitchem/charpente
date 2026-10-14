//! Outbound effects (outbound-effects spec): the `#[job(outbound)]` surface,
//! the dispatcher's call path, the kernel's outbound arm, §7's failure table,
//! §4's invariants, and the swarm property. Every sim failure prints its
//! repro line (hard rule 5).

use std::time::Duration;

use charpente::auth::Principal;
use charpente::core::db::{Job, Schema, TableReadRefusal, TxError, outbound_fingerprint};
use charpente::core::job::{JobRow, JobStatus};
use charpente::core::table::{Codec, View};
use charpente::http::{ApiError, App, AppConfig};
use charpente::kernel::{Dispatcher, JobDef, JobEntry, JobError, JobOutcome, JobScope};
use charpente::outbound::{Call, Host, IdempotencyKey, Reply};
use charpente::{Id, ScopedTx, Tenant, Tx, job, jobs};
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

fn gated_app(sim: &Sim, go: bool, jobs: u64) -> App {
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Gate>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![Gated, ChargeAsPure])
    .outbound(Host::new("pay.test"));
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
/// no start (amendment A18, as on the run path; §7 has no row of its own).
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
