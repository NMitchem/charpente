//! Outbound effects (outbound-effects spec): the `#[job(outbound)]` surface,
//! the dispatcher's call path, the kernel's outbound arm, §7's failure table,
//! §4's invariants, and the swarm property. Every sim failure prints its
//! repro line (hard rule 5).

use std::collections::BTreeMap;
use std::time::Duration;

use charpente::auth::Principal;
use charpente::core::db::{
    Enqueued, Job, JobShapeRefusal, Refusal, Schema, TableReadRefusal, TableWriteRefusal, TxError,
    outbound_fingerprint,
};
use charpente::core::io::{
    Clock, Entropy as _, Instant, Outbound, OutboundCall, OutboundReply, OutboundWake, WallTime,
};
use charpente::core::job::{JobRow, JobStatus};
use charpente::core::table::{Codec, View};
use charpente::http::{ApiError, App, AppConfig};
use charpente::kernel::{
    Dispatcher, JobDef, JobEntry, JobError, JobOutcome, JobScope, Kernel, KernelStats, Now,
    SimSource,
};
use charpente::outbound::{Call, Host, IdempotencyKey, Reply};
use charpente::{Id, ScopedTx, Tenant, Tx, job, jobs};
use charpente_sim::checks::{
    boot_through_fallout, calls_have_durable_jobs, durable_corruption_fired, is_fault_fallout,
    keys_are_per_job, outbound_settles_by_horizon,
};
use charpente_sim::outbound::{Idempotent, Recorder, from_fn};
use charpente_sim::{FaultConfig, Sim, run_universes_swarm};

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

/// Both clocks at zero, for driving a `Dispatcher` directly.
const T0: Now = Now {
    mono: Instant::ZERO,
    wall: WallTime::UNIX_EPOCH,
};

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
        d.run_due_into(&mut group, T0).unwrap(),
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
    let JobOutcome::Calling { id: 0, token } = d.run_due_into(&mut group, T0).unwrap() else {
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
        !d.has_due(WallTime::from_nanos(u64::MAX)),
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
        d.run_due_into(&mut group, T0).unwrap(),
        JobOutcome::Calling { id: 0, .. }
    ));
    assert!(
        d.has_due(WallTime::UNIX_EPOCH),
        "the pure job (id 3) is still due with the table full"
    );
    assert!(matches!(
        d.run_due_into(&mut group, T0).unwrap(),
        JobOutcome::Done { id: 3 }
    ));
    assert!(
        !d.has_due(WallTime::from_nanos(u64::MAX)),
        "outbound ids 1 and 2 wait for room"
    );
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
        d.run_due_into(&mut group, T0).unwrap(),
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
    let outcome = d.run_due_into(&mut group, T0).unwrap();
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
        d.has_due(due_at) && !d.has_due(due_at.saturating_sub(Duration::from_nanos(1))),
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
        .map(|_| d.run_due_into(&mut group, T0).unwrap())
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

/// `seeded`, but the rows are enqueued while nothing is mounted, so no stage
/// check sees them: rows already on disk when a binary that mounts `defs`
/// starts. An enqueue that contradicts a mounted `JobDef` on outbound-ness is
/// refused (final review M1), so this is how such a row reaches dispatch's
/// misroute guards, the backstop for rows on disk.
fn seeded_unmounted(sim: &Sim, defs: Vec<JobDef>, seed: impl FnOnce(&mut Tx)) -> (App, Dispatcher) {
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap();
    app.transact_for_tests(|tx| {
        seed(tx);
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let mut d = Dispatcher::new(defs).with_outbound(vec![Host::new("pay.test")], 64);
    d.rebuild(&app.db_for_tests().snapshot().unwrap()).unwrap();
    (app, d)
}

/// The enqueue-time refusal (final review M1), in its two directions: the
/// enqueued type's `OUTBOUND` disagrees with the mounted `JobDef.outbound`
/// under the same shape. Nothing is written and nothing is queued, so no row
/// ever reaches dispatch's misroute guards from this binary.
fn refused_enqueue(def: JobDef, enqueue: impl FnOnce(&mut Tx)) -> std::io::Error {
    let sim = Sim::with_faults(0x0d1, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(vec![def]);
    let err = match app.transact_for_tests(|tx| {
        enqueue(tx);
        Ok::<(), std::io::Error>(())
    }) {
        Err(TxError::Io(e)) => e,
        Err(other) => panic!("expected the stage refusal, got {other:?}"),
        Ok(()) => panic!("a row contradicting its mounted JobDef reached the store"),
    };
    assert!(
        JobShapeRefusal::of(&err).is_some(),
        "the stage refusal, which a route answers with its own text: {err}"
    );
    let db = app.db_for_tests();
    assert!(db.snapshot().unwrap().scan::<JobRow>().unwrap().is_empty());
    assert!(db.drain_enqueued().is_empty());
    err
}

/// `Charge`'s def with its `outbound` cleared: `Charge`'s own enqueue
/// (`OUTBOUND = true`, so it would mint a key) is refused by name.
#[test]
fn an_outbound_enqueue_of_a_job_mounted_pure_is_refused_by_name() {
    let mut cleared = <Charge as JobEntry>::def();
    cleared.outbound = None;
    let err = refused_enqueue(cleared, |tx| tx.enqueue(Charge { cents: 1 }));
    assert_eq!(
        err.to_string(),
        format!(
            "job \"Charge\" was enqueued with `OUTBOUND = true`, but this binary mounts Charge \
             as a pure job under the same shape ({:#x}) — dispatch routes a row by whether its \
             enqueue minted an idempotency key, which only an `OUTBOUND = true` enqueue does, \
             so this row would reach a glue the mounted Charge does not have; a hand-written \
             `impl Job` or `JobDef` contradicts Charge's #[job]; the transaction was refused \
             and nothing was written, so a retry cannot succeed — enqueue the mounted type, \
             give the other type its own name, or mount Charge as its #[job] declares it \
             (GUIDE §6)",
            <Charge as Job>::FINGERPRINT
        )
    );
}

/// The mirror: `Charge`'s def under `ChargeAsPure`'s name and fingerprint, so
/// a pure `ChargeAsPure` enqueue (no key) is refused, naming the host the
/// mounted def calls.
#[test]
fn a_pure_enqueue_of_a_job_mounted_outbound_is_refused_by_name() {
    let mut misrouted = <Charge as JobEntry>::def();
    misrouted.name = <ChargeAsPure as Job>::NAME;
    misrouted.fingerprint = <ChargeAsPure as Job>::FINGERPRINT;
    let err = refused_enqueue(misrouted, |tx| tx.enqueue(ChargeAsPure { cents: 1 }));
    assert_eq!(
        err.to_string(),
        format!(
            "job \"ChargeAsPure\" was enqueued with `OUTBOUND = false`, but this binary mounts \
             ChargeAsPure as `outbound = \"pay.test\"` under the same shape ({:#x}) — dispatch \
             routes a row by whether its enqueue minted an idempotency key, which only an \
             `OUTBOUND = true` enqueue does, so this row would reach a glue the mounted \
             ChargeAsPure does not have; a hand-written `impl Job` or `JobDef` contradicts \
             ChargeAsPure's #[job]; the transaction was refused and nothing was written, so a \
             retry cannot succeed — enqueue the mounted type, give the other type its own \
             name, or mount ChargeAsPure as its #[job] declares it (GUIDE §6)",
            <ChargeAsPure as Job>::FINGERPRINT
        )
    );
}

/// The misroute guard: a row with no minted key reaching a `JobDef` that
/// declares `outbound`. Here a hand-built def is `Charge`'s (its run glue is
/// the `unreachable!` stub) under `ChargeAsPure`'s name and fingerprint, so
/// a pure `ChargeAsPure` row enqueued while nothing was mounted reaches it.
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
    let (mut app, mut d) = seeded_unmounted(&sim, vec![misrouted], |tx| {
        tx.enqueue(ChargeAsPure { cents: 1 })
    });
    let db = app.db_for_tests();
    let mut group = db.begin_group().unwrap();
    let outcome = d.run_due_into(&mut group, T0);
    group.commit().map_err(|f| f.error).unwrap();
    eprintln!("the misroute did not stop dispatch: {outcome:?}");
}

/// The guard's mirror: a row with a minted key reaching a `JobDef` that
/// declares no `outbound`. Here a hand-built def is `Charge`'s with its
/// `outbound` cleared, so a `Charge` row (minted, by `Job::OUTBOUND`)
/// enqueued while nothing was mounted reaches it under the same fingerprint.
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
    let (mut app, mut d) =
        seeded_unmounted(&sim, vec![cleared], |tx| tx.enqueue(Charge { cents: 1 }));
    let db = app.db_for_tests();
    let mut group = db.begin_group().unwrap();
    let outcome = d.run_due_into(&mut group, T0);
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
    let err = d.run_due(db, T0).unwrap_err();
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
        d.has_due(WallTime::UNIX_EPOCH),
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
    let outcome = d.run_due(db, T0).unwrap();
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
        let outcome = d.run_due_into(&mut group, T0).unwrap();
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

/// Charges once per job; `settle` records the payment, then keeps it on a
/// 200 and treats anything else as an attempt, so a failed settle's write
/// must be discarded. Used by every §7 row below.
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
        let id = charpente::Id::from_raw(self.n);
        let times = tx.get(id)?.map_or(0, |p: Paid| p.times);
        tx.put(&Paid {
            id,
            times: times + 1,
        });
        match reply.status() {
            200 => Ok(()),
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
    assert_eq!(
        (stats.jobs_done, stats.jobs_retried, stats.jobs_dead),
        (1, 0, 0),
        "the outbound arm counts its Done as a job's"
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

/// Planted bug (a)'s mechanism without a swarm seed: the test above with the
/// key left out of the request. The kernel's side is unchanged (three
/// attempts under one minted key), but the remote has nothing to dedupe on,
/// so it applies every retry where the keyed job is applied once.
#[test]
fn a_keyless_call_whose_replies_are_lost_is_applied_on_every_retry() {
    let sim = Sim::with_faults(0x0e9, faults(|c| c.outbound_lost_reply = 1.0));
    sim.remote(
        "pay.test",
        Idempotent::header("idempotency-key", Recorder::new()),
    );
    let mut app = open_variant(&sim, Variant::Keyless).unwrap();
    app.transact_for_tests(|tx| {
        tx.enqueue(PayKeyless { n: 0 });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let (mut app, stats) = run_to(&sim, app, 60 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!((rows[0].status, paid), (JobStatus::Dead, 0));
    assert_eq!(stats.outbound_timed_out, 3);
    let key = IdempotencyKey::from_bytes(rows[0].outbound_key.unwrap());
    let delivered = sim.outbound_deliveries();
    assert_eq!(delivered.len(), 3);
    assert!(delivered.iter().all(|d| d.key == key));
    sim.with_remote("pay.test", |m: &Idempotent<Recorder>| {
        assert_eq!(
            m.inner().calls().len(),
            3,
            "the remote applied the effect on every retry"
        );
        assert!(m.applied().is_empty(), "no call carried a key");
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
    assert_eq!(
        (rows[0].status, paid),
        (JobStatus::Dead, 0),
        "settle wrote its payment before it failed, and the write was discarded each time"
    );
    assert_eq!(rows[0].last_error.as_deref(), Some("pay.test answered 503"));
    assert_eq!((stats.outbound_started, stats.outbound_failed), (3, 3));
    assert_eq!(
        (stats.jobs_done, stats.jobs_retried, stats.jobs_dead),
        (0, 2, 1),
        "the outbound arm counts its failure records as a job's"
    );
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

/// The same timeline with the cancel left in: each timed-out call is
/// cancelled at the edge, so its reply never comes back as a completion and
/// nothing is discarded late. The kernel cancels what it timed out; it does
/// not wait for the stale reply and throw it away.
#[test]
fn a_timed_out_call_is_cancelled_so_its_reply_never_comes_back() {
    let lat = Duration::from_millis(1500);
    let sim = Sim::with_faults(0x7A13_0011, faults(|c| c.outbound_latency = (lat, lat)));
    sim.remote("pay.test", Recorder::new());
    let (mut app, stats) = run_to(&sim, pay_app(&sim, Host::new("pay.test"), 1), 60 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!((rows[0].status, paid), (JobStatus::Dead, 0));
    assert_eq!(stats.outbound_timed_out, 3);
    assert_eq!(
        stats.outbound_late, 0,
        "a timed-out call's reply came back as a completion: the kernel never cancelled it"
    );
}

/// Forwards everything but `next_wake`, which answers `Unknown`, as the
/// production client always does.
struct UnknownWake<O>(O);

impl<O: Outbound> Outbound for UnknownWake<O> {
    fn start(&self, token: u64, call: OutboundCall) -> std::io::Result<()> {
        self.0.start(token, call)
    }
    fn poll(&self) -> Option<(u64, std::io::Result<OutboundReply>)> {
        self.0.poll()
    }
    fn cancel(&self, token: u64) {
        self.0.cancel(token)
    }
    fn next_wake(&self) -> OutboundWake {
        OutboundWake::Unknown
    }
}

/// Plan amendment A2's park: an `Outbound` that cannot name its next
/// completion is polled one replication tick after now while a call is in
/// flight, never parked until the call's 2s deadline.
#[test]
fn an_outbound_that_cannot_name_its_wake_is_polled_one_tick_later() {
    let lat = Duration::from_millis(500);
    let sim = Sim::with_faults(0x7A13_0012, faults(|c| c.outbound_latency = (lat, lat)));
    sim.remote("pay.test", Recorder::new());
    sim.clock().sleep(Duration::from_secs(1));
    let app = pay_app(&sim, Host::new("pay.test"), 1);
    let outbound = UnknownWake(sim.outbound(&app.outbound_hosts()).unwrap());
    let mut kernel = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)).unwrap();
    let mut source = SimSource::new(Box::new(sim.clock()), 60 * SEC);
    // Steps never park here, so virtual time stands still until the first Idle.
    let deadline = (0..100)
        .find_map(|_| match kernel.step(&mut source).unwrap() {
            charpente::kernel::Step::Idle { deadline } => Some(deadline),
            charpente::kernel::Step::Worked => None,
        })
        .expect("the kernel never went idle in 100 steps with one call out");
    assert_eq!(kernel.outbound_in_flight_for_tests(), 1, "the call is out");
    let now = sim.clock().now().as_nanos();
    assert_eq!(
        deadline.map(|d| d.as_nanos()),
        Some(now + charpente::repl::REPL_TICK_NANOS),
        "an in-flight call whose completion the Outbound cannot name is polled one replication \
         tick after now"
    );
}

/// Invariant 2, by name: a second enqueue notice for a job whose call is in
/// flight never queues it again, since the next dispatch would call it a
/// second time while the first call is still out.
#[test]
#[should_panic(
    expected = "dispatcher invariant: job 0 queued while in flight (spec §4 invariant 2)"
)]
fn a_job_in_flight_is_never_queued_again() {
    let sim = Sim::with_faults(0x7A13_0013, FaultConfig::none());
    let mut app = gated_app(&sim, true, 1);
    let mut d = dispatcher_for(&mut app, 64);
    let db = app.db_for_tests();
    let mut group = db.begin_group().unwrap();
    let outcome = d.run_due_into(&mut group, T0).unwrap();
    group.commit().map_err(|f| f.error).unwrap();
    assert!(matches!(outcome, JobOutcome::Calling { id: 0, .. }));
    assert_eq!(d.in_flight_len(), 1);
    d.note_enqueued(vec![Enqueued {
        id: charpente::Id::from_raw(0),
        outbound: true,
    }]);
}

#[charpente::table(global)]
struct Receipt {
    id: charpente::Id<Receipt>,
    body: Vec<u8>,
}

/// Keeps the reply's body, as a `settle` that records the remote's receipt.
#[job(
    global,
    outbound = "pay.test",
    retry(max = 1, backoff = "1s"),
    timeout = "2s"
)]
struct KeepReceipt {}

impl KeepReceipt {
    fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(Call::post("/receipt")))
    }

    fn settle(self, tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        tx.put(&Receipt {
            id: charpente::Id::from_raw(0),
            body: reply.body().to_vec(),
        });
        Ok(())
    }
}

/// `settle` reads the remote's reply body byte for byte through
/// `Reply::body`: what the remote answered is what the job records.
#[test]
fn settle_reads_the_reply_body_the_remote_sent() {
    let sim = Sim::with_faults(0x7A13_0014, FaultConfig::none());
    let body = b"receipt 42: paid in full".to_vec();
    let sent = body.clone();
    sim.remote(
        "pay.test",
        from_fn(move |_c: &OutboundCall| OutboundReply {
            status: 200,
            headers: vec![],
            body: sent.clone(),
        }),
    );
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Receipt>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![KeepReceipt])
    .outbound(Host::new("pay.test"));
    app.transact_for_tests(|tx| {
        tx.enqueue(KeepReceipt {});
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let (mut app, stats) = run_to(&sim, app, 60 * SEC);
    assert_eq!(stats.outbound_settled, 1);
    let kept = app
        .db_for_tests()
        .snapshot()
        .unwrap()
        .scan::<Receipt>()
        .unwrap();
    assert_eq!(
        kept.iter().map(|r| r.body.clone()).collect::<Vec<_>>(),
        vec![body],
        "settle must read exactly the bytes the remote answered"
    );
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
    // The call goes in flight after 1 step on the green run; the bound turns a
    // call that never goes in flight into a named failure, not a hang.
    let mut steps = 0u32;
    while kernel.outbound_in_flight_for_tests() == 0 {
        assert!(
            steps < 100,
            "the Pay call never went in flight: {steps} kernel steps, outbound started {}, \
             in flight {}",
            kernel.stats().outbound_started,
            kernel.outbound_in_flight_for_tests()
        );
        kernel.step(&mut source).unwrap();
        steps += 1;
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
    use charpente::kernel::{RequestSource, Step, Wake};
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
    // The 64 settle after 6 steps on the green run. Two ways they never do, each
    // named rather than hung: nothing left to wake for inside the horizon, or
    // steps that never advance time.
    let mut steps = 0u32;
    while kernel.stats().outbound_settled < 64 {
        let s = kernel.stats();
        assert!(
            steps < 1_000,
            "64 Pay replies never settled: {} settled after {steps} kernel steps at virtual \
             {}ns; outbound started {}, in flight {}, timed out {}, late {}",
            s.outbound_settled,
            sim.clock().now().as_nanos(),
            s.outbound_started,
            kernel.outbound_in_flight_for_tests(),
            s.outbound_timed_out,
            s.outbound_late
        );
        if let Step::Idle { deadline } = kernel.step(&mut source).unwrap()
            && let Wake::Shutdown = source.wait_until(deadline)
        {
            let s = kernel.stats();
            panic!(
                "64 Pay replies never settled: nothing left to wake for before the 60s horizon \
                 with {} settled; outbound started {}, in flight {}, timed out {}, late {}",
                s.outbound_settled,
                s.outbound_started,
                kernel.outbound_in_flight_for_tests(),
                s.outbound_timed_out,
                s.outbound_late
            );
        }
        steps += 1;
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
    assert_eq!(
        kernel.stats().outbound_late,
        1,
        "the buffered completion was dropped with the gate, and counted as discarded"
    );
    kernel.app_mut_for_tests().db_for_tests().allow_writes();
    kernel.step(&mut source).unwrap();
    assert_eq!(
        kernel.stats().outbound_late,
        1,
        "and never handed to the arm afterwards"
    );
}

// ------------------------------------------------ §7's settle-side rows

/// A settle whose platform read faults on a 500 and records the payment on
/// anything else: §7's "settle returns `Err(Io)`" row.
#[job(
    global,
    outbound = "pay.test",
    retry(max = 3, backoff = "500ms"),
    timeout = "2s"
)]
struct SettleFaults {
    n: u64,
}

impl SettleFaults {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(
            Call::post(format!("/faults/{}", self.n)).header("idempotency-key", key.as_str()),
        ))
    }

    fn settle(self, tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        tx.put(&Paid {
            id: charpente::Id::from_raw(self.n),
            times: 1,
        });
        if reply.status() == 500 {
            return Err(JobError::Io(std::io::Error::other(
                "simulated settle fault (injected)",
            )));
        }
        Ok(())
    }
}

fn settle_faults_app(sim: &Sim) -> App {
    App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Paid>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![SettleFaults])
    .outbound(Host::new("pay.test"))
}

/// §7: a settle's platform fault is not an attempt. It bubbles out of `run`,
/// the row stays Pending and untouched, and the restarted process calls
/// again with the same key.
#[test]
fn a_settle_platform_fault_is_not_an_attempt_and_re_calls_after_restart() {
    let sim = Sim::with_faults(0x0e1, FaultConfig::none());
    let mut replies = 0u32;
    sim.remote(
        "pay.test",
        from_fn(move |_c: &OutboundCall| {
            replies += 1;
            OutboundReply {
                status: if replies == 1 { 500 } else { 200 },
                headers: vec![],
                body: vec![],
            }
        }),
    );
    let mut app = settle_faults_app(&sim);
    app.transact_for_tests(|tx| {
        tx.enqueue(SettleFaults { n: 0 });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let outbound = sim.outbound(&app.outbound_hosts()).unwrap();
    let mut kernel = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)).unwrap();
    let err = kernel
        .run(&mut SimSource::new(Box::new(sim.clock()), 60 * SEC))
        .unwrap_err();
    assert_eq!(err.to_string(), "simulated settle fault (injected)");
    let stats = kernel.stats();
    assert_eq!(
        (
            stats.outbound_started,
            stats.outbound_settled,
            stats.outbound_failed
        ),
        (1, 0, 0)
    );
    let mut app = kernel.into_app();
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!(
        (
            rows[0].status,
            rows[0].attempts,
            rows[0].last_error.as_deref(),
            paid
        ),
        (JobStatus::Pending, 0, None, 0),
        "not an attempt, and settle's write was discarded"
    );
    let k = IdempotencyKey::from_bytes(rows[0].outbound_key.unwrap());
    drop(app);
    sim.restart();
    let (mut app, _) = run_to(&sim, settle_faults_app(&sim), 120 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!(
        (rows[0].status, rows[0].attempts, paid),
        (JobStatus::Done, 0, 1)
    );
    let delivered = sim.outbound_deliveries();
    assert_eq!(
        delivered.len(),
        2,
        "called once before the restart, once after"
    );
    assert!(
        delivered.iter().all(|d| d.key == k),
        "the re-call carries the recorded key"
    );
}

/// A table no `Schema` in this file registers: a write to it is refused.
#[charpente::table(global)]
struct Unlisted {
    id: charpente::Id<Unlisted>,
    n: u64,
}

/// What a `SettleProbe` wrote before it failed.
#[charpente::table(global)]
struct Effect {
    id: charpente::Id<Effect>,
    mode: u64,
}

const PANICS: u64 = 0;
const READS_ALIAS: u64 = 1;
const WRITES_UNLISTED: u64 = 2;
const ENQUEUES_FOREIGN_SHAPE: u64 = 3;

/// Writes an `Effect`, then fails the way its `mode` names.
#[job(
    global,
    outbound = "pay.test",
    retry(max = 1, backoff = "1s"),
    timeout = "2s"
)]
struct SettleProbe {
    mode: u64,
}

impl SettleProbe {
    fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(Call::post(format!("/probe/{}", self.mode))))
    }

    fn settle(self, tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
        tx.put(&Effect {
            id: charpente::Id::from_raw(self.mode),
            mode: self.mode,
        });
        match self.mode {
            PANICS => panic!("settle exploded"),
            READS_ALIAS => {
                tx.get(charpente::Id::<gate_alias::Gate>::from_raw(0))?;
            }
            WRITES_UNLISTED => tx.put(&Unlisted {
                id: charpente::Id::from_raw(0),
                n: 1,
            }),
            _ => tx.enqueue(shape_v1::Settle { n: 1 }),
        }
        Ok(())
    }
}

/// §7's "settle panics" row (plan amendment A16's `settle panicked:` prefix),
/// and the three refusals a settle's own transaction can meet: each is an
/// attempt whose `last_error` is the refusal, and none of settle's writes
/// commits.
#[test]
fn a_failed_settle_is_an_attempt_naming_its_cause_and_writes_nothing() {
    let sim = Sim::with_faults(0x0e2, FaultConfig::none());
    sim.remote("pay.test", Recorder::new());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Gate>().table::<Effect>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![SettleProbe, shape_v2::Settle])
    .outbound(Host::new("pay.test"));
    // The refusal texts, read off the same refusals outside any job.
    let db = app.db_for_tests();
    let read_refusal = match db.transact(|tx| {
        tx.get(charpente::Id::<gate_alias::Gate>::from_raw(0))
            .map(|_| ())
    }) {
        Err(TxError::App(e)) if TableReadRefusal::of(&e).is_some() => e.to_string(),
        other => panic!("the alias read is a TableReadRefusal: {other:?}"),
    };
    let write_refusal = match db.transact(|tx| {
        tx.put(&Unlisted {
            id: charpente::Id::from_raw(0),
            n: 1,
        });
        Ok::<(), std::io::Error>(())
    }) {
        Err(TxError::Io(e)) if TableWriteRefusal::of(&e).is_some() => e.to_string(),
        other => panic!("the unlisted write is a TableWriteRefusal: {other:?}"),
    };
    let shape_refusal = match db.transact(|tx| {
        tx.enqueue(shape_v1::Settle { n: 1 });
        Ok::<(), std::io::Error>(())
    }) {
        Err(TxError::Io(e)) if JobShapeRefusal::of(&e).is_some() => e.to_string(),
        other => panic!("the foreign-shape enqueue is a JobShapeRefusal: {other:?}"),
    };
    app.transact_for_tests(|tx| {
        for mode in [PANICS, READS_ALIAS, WRITES_UNLISTED, ENQUEUES_FOREIGN_SHAPE] {
            tx.enqueue(SettleProbe { mode });
        }
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let (mut app, stats) = run_to(&sim, app, 60 * SEC);
    let snap = app.db_for_tests().snapshot().unwrap();
    let rows = snap.scan::<JobRow>().unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| (r.status, r.attempts, r.last_error.as_deref()))
            .collect::<Vec<_>>(),
        vec![
            (JobStatus::Dead, 1, Some("settle panicked: settle exploded")),
            (JobStatus::Dead, 1, Some(read_refusal.as_str())),
            (JobStatus::Dead, 1, Some(write_refusal.as_str())),
            (JobStatus::Dead, 1, Some(shape_refusal.as_str())),
        ]
    );
    assert!(
        snap.scan::<Effect>().unwrap().is_empty(),
        "no failed settle's write committed"
    );
    assert_eq!(
        (
            stats.outbound_started,
            stats.outbound_settled,
            stats.outbound_failed,
            stats.jobs_dead
        ),
        (4, 0, 4, 4)
    );
}

/// Every completion becomes a reset after the request was sent: the
/// transport failure that is not a refusal, which `SimOutbound` never
/// produces.
struct ResetOnPoll<O>(O);

impl<O: Outbound> Outbound for ResetOnPoll<O> {
    fn start(&self, token: u64, call: OutboundCall) -> std::io::Result<()> {
        self.0.start(token, call)
    }
    fn poll(&self) -> Option<(u64, std::io::Result<OutboundReply>)> {
        self.0.poll().map(|(token, _)| {
            (
                token,
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "connection reset after the request was sent",
                )),
            )
        })
    }
    fn cancel(&self, token: u64) {
        self.0.cancel(token)
    }
    fn next_wake(&self) -> OutboundWake {
        self.0.next_wake()
    }
}

/// Spec §7's second text: any transport failure but a refusal may have
/// reached the remote, and says so.
#[test]
fn a_reset_after_sending_is_an_attempt_that_may_have_applied() {
    let sim = Sim::with_faults(0x0e4, FaultConfig::none());
    sim.remote("pay.test", Recorder::new());
    let app = pay_app(&sim, Host::new("pay.test"), 1);
    let outbound = ResetOnPoll(sim.outbound(&app.outbound_hosts()).unwrap());
    let mut kernel = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)).unwrap();
    kernel
        .run(&mut SimSource::new(Box::new(sim.clock()), 60 * SEC))
        .unwrap();
    let mut app = kernel.into_app();
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!(
        (rows[0].status, rows[0].attempts, paid),
        (JobStatus::Dead, 3, 0)
    );
    assert_eq!(
        rows[0].last_error.as_deref(),
        Some(
            "outbound pay.test: connection reset after the request was sent — the remote may have \
             applied this call; the retry carries the same idempotency key"
        )
    );
    sim.with_remote("pay.test", |r: &Recorder| {
        assert_eq!(r.calls().len(), 3, "the remote did get every call")
    });
}

/// `start` fails without forwarding: no flight exists at the edge, so only
/// the kernel's own bookkeeping can turn the error into an attempt.
struct FailsToStart<O>(O);

impl<O: Outbound> Outbound for FailsToStart<O> {
    fn start(&self, _token: u64, _call: OutboundCall) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset by peer",
        ))
    }
    fn poll(&self) -> Option<(u64, std::io::Result<OutboundReply>)> {
        self.0.poll()
    }
    fn cancel(&self, token: u64) {
        self.0.cancel(token)
    }
    fn next_wake(&self) -> OutboundWake {
        self.0.next_wake()
    }
}

/// io.rs: an `Err` from `start` is a completion. Its attempt carries the
/// start's own error at once, never a timeout: all three attempts (at 0,
/// 0.5s and 1.5s) are recorded before the first 2s deadline could pass.
#[test]
fn a_start_error_is_an_immediate_attempt_not_a_timeout() {
    let sim = Sim::with_faults(0x0e5, FaultConfig::none());
    sim.remote("pay.test", Recorder::new());
    let app = pay_app(&sim, Host::new("pay.test"), 1);
    let outbound = FailsToStart(sim.outbound(&app.outbound_hosts()).unwrap());
    let mut kernel = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)).unwrap();
    kernel
        .run(&mut SimSource::new(Box::new(sim.clock()), 19 * SEC / 10))
        .unwrap();
    let stats = kernel.stats();
    let mut app = kernel.into_app();
    let (rows, _) = jobs_and_paid(&mut app);
    assert_eq!(
        (rows[0].status, rows[0].attempts),
        (JobStatus::Dead, 3),
        "every attempt was recorded by 1.9s, before any deadline"
    );
    assert_eq!(
        rows[0].last_error.as_deref(),
        Some(
            "outbound pay.test: connection reset by peer — the remote may have applied this call; \
             the retry carries the same idempotency key"
        )
    );
    assert_eq!(
        (
            stats.outbound_started,
            stats.outbound_failed,
            stats.outbound_timed_out
        ),
        (3, 3, 0)
    );
}

/// A plain job that always fails: its failure records are a job's, never
/// an outbound one's.
#[job(global, retry(max = 2, backoff = "1s"))]
struct PlainFails {}

impl PlainFails {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Err(JobError::app("plain failure"))
    }
}

/// `KernelStats::outbound_failed` counts every outbound failure record, the
/// ones `call` causes in the job arm included: a refused call (`Leaky`), a
/// panicking `call` (`Boom`) and a refused read inside `call` (`ReadsAlias`),
/// each Dead at the default policy's 5 attempts. None starts. A plain job
/// failing beside them (`PlainFails`, 2 attempts) is not counted.
#[test]
fn call_side_failure_records_count_as_outbound_failures() {
    let sim = Sim::with_faults(0x0e6, FaultConfig::none());
    sim.remote("pay.test", Recorder::new());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Gate>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![Leaky, Boom, ReadsAlias, PlainFails])
    .outbound(Host::new("pay.test").bearer_env("PAY_KEY"));
    app.transact_for_tests(|tx| {
        tx.enqueue(Leaky {});
        tx.enqueue(Boom {});
        tx.enqueue(ReadsAlias {});
        tx.enqueue(PlainFails {});
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let (mut app, stats) = run_to(&sim, app, 3600 * SEC);
    let (rows, _) = jobs_and_paid(&mut app);
    assert!(
        rows.iter().all(|r| (r.status, r.attempts)
            == (JobStatus::Dead, if r.name == "PlainFails" { 2 } else { 5 })),
        "{:?}",
        rows.iter()
            .map(|r| (&r.name, r.status, r.attempts))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        (
            stats.outbound_failed,
            stats.outbound_started,
            stats.jobs_retried,
            stats.jobs_dead
        ),
        (15, 0, 13, 4)
    );
}

/// Spec §9's cap is inclusive: a reply of exactly `max_reply_bytes` settles.
#[test]
fn a_reply_of_exactly_the_cap_settles() {
    let sim = Sim::with_faults(0x0e7, FaultConfig::none());
    sim.remote(
        "pay.test",
        from_fn(|_c: &OutboundCall| OutboundReply {
            status: 200,
            headers: vec![],
            body: vec![b'x'; 16],
        }),
    );
    let host = Host::new("pay.test").max_reply_bytes(16);
    let (mut app, _) = run_to(&sim, pay_app(&sim, host, 1), 60 * SEC);
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!(
        (rows[0].status, rows[0].attempts, paid),
        (JobStatus::Done, 0, 1)
    );
}

/// `Kernel::builder` combines parts. Each part leaves its own mark, so a
/// builder that dropped any one of them turns one assertion red: the
/// outbound job settles (`.outbound`), the archive arm stepped
/// (`.archive`), and an idle step parks no longer than the replication tick
/// (`.replication`; a kernel without `Repl` parks to its next deadline, the
/// daily session sweep).
#[test]
fn a_kernel_builder_wires_replication_archive_and_outbound_together() {
    use charpente::archive::{Archive, ArchiveConfig};
    use charpente::kernel::Step;
    use charpente::repl::role::Role;
    use charpente::repl::{REPL_TICK_NANOS, Repl, ReplConfig};
    use charpente_sim::object::SimObjectStore;

    let sim = Sim::with_faults(0x0e8, FaultConfig::none());
    sim.remote("pay.test", Recorder::new());
    let node = sim.host("p");
    let mut app = App::open(
        Box::new(node.disk()),
        Schema::new().table::<Paid>(),
        Box::new(node.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![Pay])
    .outbound(Host::new("pay.test"));
    app.transact_for_tests(|tx| {
        tx.enqueue(Pay { n: 0 });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let (repl, _) = Repl::open(
        Box::new(node.net()),
        Box::new(node.disk()),
        ReplConfig {
            listen: Some("p:1".to_string()),
            initial_role: Role::Primary,
            peer_secret: None,
            strict_witnesses: false,
        },
        app.db_for_tests(),
    )
    .unwrap();
    let archive = Archive::open(
        ArchiveConfig {
            prefix: "archive/".to_string(),
            writer: "p".to_string(),
            snapshot_every: 0,
            pin_segments: 4,
            read_budget_bytes: 8 << 20,
        },
        app.db_for_tests(),
    )
    .unwrap();
    let outbound = sim.outbound(&app.outbound_hosts()).unwrap();
    let mut kernel = Kernel::builder(app, Box::new(sim.clock()))
        .replication(repl)
        .archive(archive, Box::new(SimObjectStore::new()))
        .outbound(Box::new(outbound))
        .build()
        .unwrap();
    let mut source = SimSource::new(Box::new(sim.clock()), 10 * SEC);
    kernel.run(&mut source).unwrap();
    let stats = kernel.stats();
    assert_eq!(stats.outbound_settled, 1, "the outbound part");
    assert!(stats.archive_steps > 0, "the archive part");
    let step = (0..1000)
        .map(|_| kernel.step(&mut source).unwrap())
        .find(|s| matches!(s, Step::Idle { .. }))
        .expect("the kernel idles");
    let now = sim.clock().now().as_nanos();
    let Step::Idle {
        deadline: Some(deadline),
    } = step
    else {
        panic!("a replicated kernel always names a park deadline");
    };
    assert!(
        deadline.as_nanos() <= now + REPL_TICK_NANOS,
        "the replication part: parks at most one tick, not until {}",
        deadline.as_nanos()
    );
    let mut app = kernel.into_app();
    let (rows, paid) = jobs_and_paid(&mut app);
    assert_eq!((rows[0].status, paid), (JobStatus::Done, 1));
}

// ------------------------------------- the swarm property (spec §12, oracle 2)

const POOL: u64 = 6;
const ROUNDS: u32 = 3;
const BOOT_TRIES: u32 = 8;
/// The swarm property's reach floors and give-up cap. Its base is ambient, so
/// all three are margins over 10 fresh bases (0x0B1D1001..0A, 768 universes
/// each). The floors count what the exactly-once checks had to judge, not
/// universes that merely ran: `outbound:re-called` (a key reached the remote
/// twice) was 333-376 (mean 358.1, sample sd 13.1), and
/// `outbound:re-called-across-restart` (one key's deliveries from two
/// processes) 222-246 (mean 233.6, sample sd 8.7), so each floor is the mean
/// less 4 sd. The batch is 768 because at 256 a required axis missed about once
/// in 200 runs; here the rarest, `fault:outbound_lost_request` and
/// `fault:outbound_lost_reply`, fired at least 13 and 17 times in every batch.
/// It gave up 104 times in all, at most 16 in one batch: 96 boots refused for
/// durable corruption and 8 corruption-cut enqueues. The cap follows GUIDE §8's
/// ambient rule, the larger of twice the largest count (32) and the Poisson
/// cap. With x = 104 give-ups over N = 7,680 universes, U(104) =
/// ½·χ²₀.₉₅(210) = 122.40, so a batch of n = 768 has lambda = 768 x 122.40 /
/// 7,680 = 12.24. P(> 26) is 1.8e-4 and P(> 27) is 7.8e-5, so k = 27, and the
/// cap is 32.
const OUTBOUND_RE_CALLED_FLOOR: u64 = 305;
const OUTBOUND_RE_CALLED_ACROSS_RESTART_FLOOR: u64 = 198;
const OUTBOUND_GAVE_UP_CEILING: u64 = 32;

/// Which `Pay` the workload mounts: the honest one, or one of spec §12's two
/// planted bugs.
#[derive(Clone, Copy)]
enum Variant {
    Honest,
    Keyless,
    Trusting,
}

/// Planted bug (a): the call carries no idempotency key, so a re-call after
/// a lost reply (or a crash, or a timeout) is applied again.
#[job(
    global,
    outbound = "pay.test",
    retry(max = 3, backoff = "500ms"),
    timeout = "2s"
)]
struct PayKeyless {
    n: u64,
}

impl PayKeyless {
    fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(Call::post(format!("/pay/{}", self.n))))
    }

    fn settle(self, tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        Pay { n: self.n }.settle(tx, reply)
    }
}

/// Planted bug (b): `settle` takes a 503 for a success, recording a payment
/// the remote never applied.
#[job(
    global,
    outbound = "pay.test",
    retry(max = 3, backoff = "500ms"),
    timeout = "2s"
)]
struct PayTrusting {
    n: u64,
}

impl PayTrusting {
    fn call(&self, tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Pay { n: self.n }.call(tx, key)
    }

    fn settle(self, tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        match reply.status() {
            200 | 503 => {
                let id = Id::from_raw(self.n);
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

fn open_variant(sim: &Sim, v: Variant) -> std::io::Result<App> {
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Paid>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?;
    let app = match v {
        Variant::Honest => app.mount_jobs(jobs![Pay]),
        Variant::Keyless => app.mount_jobs(jobs![PayKeyless]),
        Variant::Trusting => app.mount_jobs(jobs![PayTrusting]),
    };
    Ok(app.outbound(Host::new("pay.test")))
}

/// Seeds the pool unless an earlier round's seeding is durable: a seed whose
/// commit failed may still have landed, and seeding twice would put two jobs
/// on one `Paid` id.
fn seed_once(app: &mut App, v: Variant) -> Result<(), String> {
    let seeded = app
        .db_for_tests()
        .snapshot()
        .and_then(|s| s.scan::<JobRow>())
        .map_err(|e| e.to_string())?
        .iter()
        .any(|r| r.outbound_key.is_some());
    if seeded {
        return Ok(());
    }
    app.transact_for_tests(|tx| {
        for n in 0..POOL {
            match v {
                Variant::Honest => tx.enqueue(Pay { n }),
                Variant::Keyless => tx.enqueue(PayKeyless { n }),
                Variant::Trusting => tx.enqueue(PayTrusting { n }),
            }
        }
        Ok::<(), std::io::Error>(())
    })
    // `TxError` derives only `Debug`. The injected-fault text lives in `Io`, so
    // that arm must reach `is_fault_fallout` as the error's own text.
    .map_err(|e| match e {
        TxError::Io(e) => e.to_string(),
        other => format!("{other:?}"),
    })
}

/// Runs one kernel to `horizon`; fault fallout ends the round, anything else
/// fails the universe.
fn run_round(sim: &Sim, app: App, horizon: u64) -> Result<Option<App>, String> {
    let outbound = sim
        .outbound(&app.outbound_hosts())
        .map_err(|e| e.to_string())?;
    let mut kernel = match Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)) {
        Ok(k) => k,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(None),
        Err(e) => return Err(format!("kernel build failed non-injected: {e}")),
    };
    match kernel.run(&mut SimSource::new(Box::new(sim.clock()), horizon)) {
        Ok(()) => Ok(Some(kernel.into_app())),
        Err(e) if is_fault_fallout(&e.to_string()) => Ok(None),
        Err(e) => Err(format!("kernel stopped non-injected: {e}")),
    }
}

/// Oracle 2's swarm property (spec §12): outbound jobs under every outbound
/// fault axis, disk faults and crashes, then a fault-free tail. Properties:
/// the three checkers; no charge applied twice at the remote; no payment
/// recorded for a charge the remote never applied; `settle` committed at most
/// once per job.
fn outbound_workload(sim: &mut Sim, v: Variant) -> Result<(), String> {
    sim.remote(
        "pay.test",
        Idempotent::header("idempotency-key", Recorder::new()),
    );
    let entropy = sim.entropy();
    for _round in 0..ROUNDS {
        sim.crash_disk_after(36 + entropy.next_u64() % 438);
        let Some(mut app) =
            boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, || open_variant(sim, v))
                .map_err(|e| format!("open failed non-injected: {e}"))?
        else {
            if sim.gave_up().is_some() {
                return Ok(());
            }
            sim.restart();
            continue;
        };
        match seed_once(&mut app, v) {
            Ok(()) => {}
            Err(e) if is_fault_fallout(&e) => {
                drop(app);
                sim.restart();
                continue;
            }
            Err(e) => return Err(format!("seeding failed non-injected: {e}")),
        }
        let horizon = sim.now().as_nanos() + 20 * SEC;
        drop(run_round(sim, app, horizon)?);
        sim.restart();
    }

    // The fault-free tail (GUIDE §8): long enough for every backoff and deadline.
    sim.quiesce_disk_faults();
    sim.quiesce_outbound_faults();
    let Some(mut app) =
        boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, || open_variant(sim, v))
            .map_err(|e| format!("tail open failed non-injected: {e}"))?
    else {
        return if sim.gave_up().is_some() {
            Ok(())
        } else {
            Err("the fault-free tail could not boot".into())
        };
    };
    seed_once(&mut app, v)?;
    let horizon = sim.now().as_nanos() + 600 * SEC;
    let mut app = run_round(sim, app, horizon)?.ok_or("the fault-free tail's kernel stopped")?;

    let snap = app.db_for_tests().snapshot().map_err(|e| e.to_string())?;
    keys_are_per_job(&snap)?;
    outbound_settles_by_horizon(&snap)?;
    if let Err(e) = calls_have_durable_jobs(sim, &snap) {
        // One sim box's only loss window is media corruption: a bit flip or
        // misdirected write on a WAL frame above the checkpoint horizon is cut
        // at recovery, and a call already made for the cut enqueue keeps its
        // key while the store loses it (`checks::durable_corruption_fired`).
        // Excused only where that fired, and as a give-up, never a pass: the
        // re-seeded pool charges again under fresh keys, so nothing below is
        // adjudicated.
        if durable_corruption_fired(sim) {
            sim.give_up(
                "durable corruption cut an enqueue whose call had already reached the remote, \
                 so exactly-once was not adjudicated",
                e,
            );
            return Ok(());
        }
        return Err(e);
    }
    let paid = snap.scan::<Paid>().map_err(|e| e.to_string())?;
    let jobs = snap.scan::<JobRow>().map_err(|e| e.to_string())?;
    sim.with_remote(
        "pay.test",
        |m: &Idempotent<Recorder>| -> Result<(), String> {
            let mut applied: BTreeMap<&str, u32> = BTreeMap::new();
            for c in m.inner().calls() {
                *applied.entry(c.path.as_str()).or_insert(0) += 1;
            }
            if let Some((path, n)) = applied.iter().find(|(_, n)| **n > 1) {
                return Err(format!(
                    "double charge: pay.test applied {path} {n} times — a re-call carried no \
                     idempotency key the remote could dedupe on"
                ));
            }
            for p in &paid {
                if p.times != 1 {
                    return Err(format!(
                        "settle committed {} times for job {}",
                        p.times,
                        p.id.raw()
                    ));
                }
                let path = format!("/pay/{}", p.id.raw());
                if !applied.contains_key(path.as_str()) {
                    return Err(format!(
                        "phantom charge: job {} recorded a payment, but pay.test never applied \
                         {path} — settle took a reply that was not a success for one",
                        p.id.raw()
                    ));
                }
            }
            Ok(())
        },
    )?;
    for row in jobs.iter().filter(|r| r.outbound_key.is_some()) {
        let n = u64::decode(&mut &row.payload[..]).map_err(|e| e.to_string())?;
        let has_paid = paid.iter().any(|p| p.id.raw() == n);
        if (row.status == JobStatus::Done) != has_paid {
            return Err(format!(
                "job {} is {:?} but its payment row is {} — settle's effects and the Done mark \
                 commit as one record",
                row.id.raw(),
                row.status,
                if has_paid { "present" } else { "absent" }
            ));
        }
        if row.status == JobStatus::Done && row.attempts > 0 {
            sim.cover("outbound:settled-after-retry");
        }
    }
    // What the exactly-once checks above had to judge: a key that reached the
    // remote twice, and one whose deliveries came from two processes.
    let mut handles: BTreeMap<IdempotencyKey, Vec<u64>> = BTreeMap::new();
    for d in sim.outbound_deliveries() {
        handles.entry(d.key).or_default().push(d.handle);
    }
    if handles.values().any(|h| h.len() > 1) {
        sim.cover("outbound:re-called");
    }
    if handles.values().any(|h| h.iter().any(|&x| x != h[0])) {
        sim.cover("outbound:re-called-across-restart");
    }
    sim.cover("outbound:checked");
    Ok(())
}

#[test]
fn outbound_jobs_are_exactly_once_under_the_fault_matrix() {
    run_universes_swarm!(
        768,
        |sim: &mut Sim| outbound_workload(sim, Variant::Honest),
        require: [
            "outbound:checked",
            "outbound:settled-after-retry",
            "fault:outbound_lost_reply",
            "fault:outbound_lost_request",
            "fault:outbound_refused",
            "fault:outbound_throttle",
            "fault:outbound_slow",
            "fault:crash",
            "restart",
        ],
        floor: [
            ("outbound:re-called", OUTBOUND_RE_CALLED_FLOOR),
            (
                "outbound:re-called-across-restart",
                OUTBOUND_RE_CALLED_ACROSS_RESTART_FLOOR
            ),
        ],
        ceiling: [("run:skipped", OUTBOUND_GAVE_UP_CEILING)]
    );
}

/// The universe a pinned seed names (the swarm runner's own derivation).
fn universe(seed: u64) -> Sim {
    let mut state = seed;
    let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
    let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
    charpente_sim::sample_extended_fields(seed, &mut cfg);
    Sim::with_faults(seed, cfg)
}

/// Final review M3 (carry 6): kernel-level replay under the outbound fault
/// axes. `checks::replays_identically` builds `Sim::new`, which carries no
/// fault config, and charpente-sim's world-level replay runs no kernel. So
/// the swarm property's workload runs one faulted universe twice, and both
/// runs must leave the same durable bytes, the same coverage and the same
/// deliveries (host, key, virtual time, process). Seed found by a 400-seed
/// scan from 0x0E5E_ED00_0000_0000 on 2026-10-15: a reply is lost and its job
/// called again, and a crash makes a later process call one key too, so the
/// replay crosses the outbound arm, a restart and recovery.
#[test]
fn an_outbound_faulted_universe_replays_identically_through_the_kernel() {
    const SEED: u64 = 0x0E5E_ED00_0000_00A5;
    let run = || {
        let mut sim = universe(SEED);
        outbound_workload(&mut sim, Variant::Honest)
            .unwrap_or_else(|e| panic!("seed {SEED:#x}: the honest workload must pass: {e}"));
        assert_eq!(
            sim.gave_up(),
            None,
            "seed {SEED:#x}: a verdict, not a give-up"
        );
        let deliveries: Vec<(String, String, u64, u64)> = sim
            .outbound_deliveries()
            .iter()
            .map(|d| (d.host.clone(), d.key.as_str().to_string(), d.at, d.handle))
            .collect();
        (sim.durable_fingerprint(), sim.coverage(), deliveries)
    };
    let first = run();
    for reason in [
        "fault:outbound_lost_reply",
        "outbound:re-called",
        "fault:crash",
        "outbound:re-called-across-restart",
        "outbound:checked",
    ] {
        assert!(
            first.1.contains_key(reason),
            "seed {SEED:#x}: the pin's reason includes {reason}, and it did not fire: {:?}",
            first.1
        );
    }
    let second = run();
    assert_eq!(
        first, second,
        "seed {SEED:#x}: one faulted universe, run twice through the kernel, diverged \
         (durable fingerprint, coverage, deliveries)"
    );
}

/// One sim box's only loss window is media corruption, and the property gives
/// it up: found by the property's first batch (2026-10-14). A bit flip at the
/// first round's restart cut the seeded pool's enqueue after its six calls had
/// reached the remote, and the next round re-seeded the pool under fresh keys.
#[test]
fn a_media_cut_enqueue_whose_calls_were_made_gives_the_universe_up() {
    const SEED: u64 = 0x81c2_6254_2dfc_e560;
    let mut sim = universe(SEED);
    outbound_workload(&mut sim, Variant::Honest)
        .unwrap_or_else(|e| panic!("seed {SEED:#x}: excused, so never a failure: {e}"));
    assert!(
        sim.coverage().contains_key("fault:bit_flip"),
        "seed {SEED:#x}: the reason is a bit flip, and none fired: {:?}",
        sim.coverage()
    );
    let reason = sim.gave_up().expect("excused, so never a pass");
    assert_eq!(
        reason,
        "durable corruption cut an enqueue whose call had already reached the remote, so \
         exactly-once was not adjudicated"
    );
    let detail = sim.gave_up_detail().unwrap_or_default();
    assert!(
        detail.starts_with(
            "calls_have_durable_jobs: a call to pay.test with key fa35dc5568cc92447a55045d8f217e5a"
        ),
        "seed {SEED:#x}: {detail}"
    );
}

/// Planted bug (a), spec §12 oracle 4: without the key, a re-call is a second
/// charge. Seed found by a 1,024-universe hunt on 2026-10-14 (progress.md).
/// The universe is the crash between the remote's apply and the reply's
/// settle: process 0 delivered `/pay/2`, its round ended on an injected write
/// fault with the job unsettled, and process 1, which exists only after that
/// round's power cycle, called again.
#[test]
fn planted_keyless_call_is_a_double_charge_with_a_recorded_seed() {
    const SEED: u64 = 0xc064_fe5a_a310_86a7;
    let mut sim = universe(SEED);
    let err =
        outbound_workload(&mut sim, Variant::Keyless).expect_err("the planted bug must be caught");
    assert_eq!(
        err,
        "double charge: pay.test applied /pay/2 2 times — a re-call carried no idempotency key \
         the remote could dedupe on",
        "seed {SEED:#x}"
    );
    // One job called again with nothing to dedupe on, not two jobs charging
    // one path: every application carried that job's key and no key header.
    let (keys, headed) = sim.with_remote("pay.test", |m: &Idempotent<Recorder>| {
        let calls: Vec<&OutboundCall> = m
            .inner()
            .calls()
            .iter()
            .filter(|c| c.path == "/pay/2")
            .collect();
        (
            calls
                .iter()
                .map(|c| c.key.as_str().to_string())
                .collect::<Vec<_>>(),
            calls.iter().filter(|c| !c.headers.is_empty()).count(),
        )
    });
    assert_eq!(
        (keys.len(), headed),
        (2, 0),
        "seed {SEED:#x}: /pay/2 must be applied twice, with no key header"
    );
    assert_eq!(keys[0], keys[1], "seed {SEED:#x}: one job, re-called");
    let key = keys[0].clone();
    // (virtual nanos, handle) of each delivery of that job's call. A new
    // handle is a new process, built only after a power cycle.
    let delivered = |sim: &Sim| -> Vec<(u64, u64)> {
        sim.outbound_deliveries()
            .iter()
            .filter(|d| d.key.as_str() == key)
            .map(|d| (d.at, d.handle))
            .collect()
    };
    assert_eq!(
        delivered(&sim),
        [(3_449_721, 0), (8_332_991, 1)],
        "seed {SEED:#x}: applied by process 0, re-called by process 1 after the power cycle"
    );
    let mut honest = universe(SEED);
    outbound_workload(&mut honest, Variant::Honest).unwrap_or_else(|e| {
        panic!("seed {SEED:#x}: the honest job must pass where the planted one fails: {e}")
    });
    assert_eq!(
        (honest.gave_up(), honest.coverage().get("outbound:checked")),
        (None, Some(&1)),
        "seed {SEED:#x}: the honest twin must reach a verdict, not give up"
    );
    // The twin goes through the same crash, and its key is what saves it:
    // delivered twice, applied once.
    assert_eq!(
        delivered(&honest),
        [(3_449_721, 0), (8_332_991, 1)],
        "seed {SEED:#x}: the twin runs the same scenario"
    );
    assert_eq!(
        honest.with_remote("pay.test", |m: &Idempotent<Recorder>| m
            .applied()
            .get(&key)
            .copied()),
        Some(1),
        "seed {SEED:#x}: the twin's re-call is deduped"
    );
}

/// Planted bug (b): a 503 taken for a success records a charge that never
/// happened. Seed found by a 1,024-universe hunt on 2026-10-14 (progress.md).
#[test]
fn planted_trusting_settle_is_a_phantom_charge_with_a_recorded_seed() {
    const SEED: u64 = 0xce58_4205_0b68_d0cb;
    let mut sim = universe(SEED);
    let err =
        outbound_workload(&mut sim, Variant::Trusting).expect_err("the planted bug must be caught");
    assert_eq!(
        err,
        "phantom charge: job 2 recorded a payment, but pay.test never applied /pay/2 — settle \
         took a reply that was not a success for one",
        "seed {SEED:#x}"
    );
    assert!(
        sim.coverage().contains_key("fault:outbound_throttle"),
        "seed {SEED:#x}: the reason is a throttled 503, and none fired: {:?}",
        sim.coverage()
    );
    let mut honest = universe(SEED);
    outbound_workload(&mut honest, Variant::Honest).unwrap_or_else(|e| {
        panic!("seed {SEED:#x}: the honest job must pass where the planted one fails: {e}")
    });
    assert_eq!(
        (honest.gave_up(), honest.coverage().get("outbound:checked")),
        (None, Some(&1)),
        "seed {SEED:#x}: the honest twin must reach a verdict, not give up"
    );
    assert!(
        honest.coverage().contains_key("fault:outbound_throttle"),
        "seed {SEED:#x}: the twin must meet the same throttle: {:?}",
        honest.coverage()
    );
}
