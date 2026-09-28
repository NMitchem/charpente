// A scoped job enqueued on a bare `Tx` (job-tenancy spec §5): the `Tx` names
// no tenant for the job to run in. The statuspage enqueued `NotifySubscribers`
// inside its org's scope (`6037704:src/incidents.rs:261`, on a
// `&mut ScopedTx<Org>`), reaching `Tx::enqueue` through the deref; the job
// then opened its org on its own word. `ScopedTx::enqueue` now shadows that
// deref, so that site compiles and the job runs in its enqueuer's org. This
// pins the case that is refused: the same job on a bare `Tx`. The
// `NoJobTenant` note says where to enqueue it instead.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::kernel::JobError;
use charpente::{Id, ScopedTx, Tenant, Tx, job, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[table(scoped_by = Org)]
struct Incident {
    id: Id<Incident>,
    title: String,
}

#[job(scoped_by = Org)]
struct NotifySubscribers {
    incident: u64,
}

impl NotifySubscribers {
    fn run(self, _tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        let _ = self.incident;
        Ok(())
    }
}

#[route(POST "/orgs/:org/incidents/:id/resolve", auth)]
fn resolve(tx: &mut Tx, org: Id<Org>, id: Id<Incident>, ctx: &Ctx) -> Result<(), ApiError> {
    let _ = (org, ctx);
    tx.enqueue(NotifySubscribers { incident: id.raw() });
    Ok(())
}

fn main() {}
