//! Outbound effects (outbound-effects spec): the `#[job(outbound)]` surface,
//! the dispatcher's call path, the kernel's outbound arm, §7's failure table,
//! §4's invariants, and the swarm property. Every sim failure prints its
//! repro line (hard rule 5).

use std::time::Duration;

use charpente::auth::Principal;
use charpente::core::db::{Job, outbound_fingerprint};
use charpente::http::ApiError;
use charpente::kernel::{JobEntry, JobError};
use charpente::outbound::{Call, IdempotencyKey, Reply};
use charpente::{Id, ScopedTx, Tenant, Tx, job};

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
    use charpente::core::db::Schema;
    use charpente::describe::{to_human, to_json};
    use charpente::http::{App, AppConfig};
    use charpente::jobs;
    use charpente::outbound::Host;
    use charpente_sim::{FaultConfig, Sim};

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
