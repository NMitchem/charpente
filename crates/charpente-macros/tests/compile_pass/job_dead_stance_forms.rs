// Every form of the Dead stance (0.5.0 spec §5 C). `on_dead`'s signature is
// `run`'s plus `dead: Dead<Self>` last: a global job takes `&mut Tx`, a
// scoped one its tenant's `&mut ScopedTx`, a `cross_tenant` one the
// capability after `tx`, and an outbound job the same hook as a pure one.
// `retain_dead` writes no hook. Each word composes with the others in any
// order. trybuild runs `main`, so its assertions are part of the pin.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::kernel::{DeadStance, JobEntry, JobError};
use charpente::outbound::{Call, IdempotencyKey, Reply};
use charpente::{CrossTenant, Dead, Id, ScopedTx, Tenant, Tx, job, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(global)]
struct Note {
    id: Id<Note>,
    text: String,
}

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[job(global, on_dead, retry(max = 2, backoff = "1s"))]
struct Sweep {
    n: u64,
}

impl Sweep {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        let _ = self.n;
        Ok(())
    }

    // The hook's writes commit with the Dead mark.
    fn on_dead(self, tx: &mut Tx, dead: Dead<Self>) -> Result<(), JobError> {
        let attempts: u32 = dead.attempts();
        let error: &str = dead.last_error();
        let key: Option<IdempotencyKey> = dead.key();
        tx.put(&Note {
            id: Id::from_raw(self.n),
            text: format!("{attempts} {error} {key:?}"),
        });
        Ok(())
    }
}

#[job(scoped_by = Org, on_dead)]
struct Remind {
    n: u64,
}

impl Remind {
    fn run(self, _tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        let _ = self.n;
        Ok(())
    }

    fn on_dead(self, _tx: &mut ScopedTx<'_, '_, Org>, dead: Dead<Self>) -> Result<(), JobError> {
        let _ = (self.n, dead.attempts());
        Ok(())
    }
}

#[job(on_dead, global, cross_tenant)]
struct Rollup {}

impl Rollup {
    fn run(self, _tx: &mut Tx, _cap: &CrossTenant) -> Result<(), JobError> {
        Ok(())
    }

    fn on_dead(self, _tx: &mut Tx, _cap: &CrossTenant, _dead: Dead<Self>) -> Result<(), JobError> {
        Ok(())
    }
}

#[job(scoped_by = Org, cross_tenant, on_dead)]
struct Fanout {}

impl Fanout {
    fn run(self, _tx: &mut ScopedTx<'_, '_, Org>, _cap: &CrossTenant) -> Result<(), JobError> {
        Ok(())
    }

    fn on_dead(
        self,
        _tx: &mut ScopedTx<'_, '_, Org>,
        _cap: &CrossTenant,
        _dead: Dead<Self>,
    ) -> Result<(), JobError> {
        Ok(())
    }
}

#[job(scoped_by = Org, outbound = "api.stripe.com", on_dead)]
struct Charge {
    cents: u64,
}

impl Charge {
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

    // `Some` for an outbound job: the key its calls were sent under.
    fn on_dead(self, _tx: &mut ScopedTx<'_, '_, Org>, dead: Dead<Self>) -> Result<(), JobError> {
        let _ = (self.cents, dead.key().map(|k| k.as_str().to_string()));
        Ok(())
    }
}

#[job(global, outbound = "mail.test", on_dead)]
struct Mail {}

impl Mail {
    fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(None)
    }

    fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
        Ok(())
    }

    fn on_dead(self, _tx: &mut Tx, _dead: Dead<Self>) -> Result<(), JobError> {
        Ok(())
    }
}

#[job(retain_dead, scoped_by = Org)]
struct Kept {}

impl Kept {
    fn run(self, _tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        Ok(())
    }
}

fn main() {
    for def in [
        <Sweep as JobEntry>::def(),
        <Remind as JobEntry>::def(),
        <Rollup as JobEntry>::def(),
        <Fanout as JobEntry>::def(),
        <Charge as JobEntry>::def(),
        <Mail as JobEntry>::def(),
    ] {
        assert!(matches!(def.dead, DeadStance::Hook(_)), "{}", def.name);
        assert_eq!(def.dead_coverage, format!("dead:{}", def.name));
    }
    let kept = <Kept as JobEntry>::def();
    assert!(matches!(kept.dead, DeadStance::Retain));
    assert_eq!(kept.dead_coverage, "dead:Kept");
}
