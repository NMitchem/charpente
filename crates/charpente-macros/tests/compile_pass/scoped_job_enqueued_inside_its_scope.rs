// The shapes a scoped job is written in (job-tenancy spec §5): enqueued, keyed
// or not, from inside its tenant's scope; `scoped_by` composed with
// `cross_tenant` and `retry(...)`; a follow-up handed on through the job's
// own `ScopedTx`; and a global job enqueued beside it through the deref.
// trybuild runs `main`, so its assertions are part of the pin.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::kernel::{JobEntry, JobError};
use charpente::{CrossTenant, Id, Scoped, ScopedTx, Tenant, Tx, job, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[table(scoped_by = Org)]
struct Incident {
    id: Id<Incident>,
    open: bool,
}

#[job(retry(max = 2, backoff = "1s"), scoped_by = Org)]
struct Notify {
    incident: u64,
}

impl Notify {
    fn run(self, tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        if let Some(incident) = tx.get(Id::<Incident>::from_raw(self.incident))? {
            let _ = incident.open;
        }
        tx.enqueue(Tally {});
        (**tx).enqueue(Audit {});
        Ok(())
    }
}

#[job(scoped_by = Org, cross_tenant)]
struct Tally {}

impl Tally {
    fn run(self, tx: &mut ScopedTx<'_, '_, Org>, cap: &CrossTenant) -> Result<(), JobError> {
        let _ = (tx.scope(), (**tx).across_tenants::<Org>(cap).scan::<Incident>()?);
        Ok(())
    }
}

#[job(global)]
struct Audit {}

impl Audit {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

#[route(POST "/orgs/:org/incidents/:id/resolve", auth)]
fn resolve(tx: &mut Tx, org: Id<Org>, id: Id<Incident>, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.put(&Incident { id, open: false });
    tx.enqueue(Notify { incident: id.raw() });
    tx.enqueue_keyed(format!("notify:{}", id.raw()), Notify { incident: id.raw() });
    (*tx).enqueue(Audit {});
    Ok(())
}

fn main() {
    assert_eq!(<Notify as JobEntry>::def().scoped_by, Some("Org"));
    assert_eq!(<Tally as JobEntry>::def().scoped_by, Some("Org"));
    assert!(<Tally as JobEntry>::def().cross_tenant);
    assert_eq!(<Audit as JobEntry>::def().scoped_by, None);
    assert_eq!(<Notify as JobEntry>::def().retry.max, 2);
}
