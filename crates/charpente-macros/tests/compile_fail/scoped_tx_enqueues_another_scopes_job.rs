// A `ScopedTx<Org>` enqueues `Org`'s jobs only (job-tenancy spec §5): a job
// scoped by another type would run in a tenant of a type this transaction
// never opened. rustc's own E0271 — pinned so GUIDE §16 can show it.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::kernel::JobError;
use charpente::{Id, Scoped, ScopedTx, Tenant, Tx, job, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(global)]
struct Team {
    id: Id<Team>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

impl Tenant for Team {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Team>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[job(scoped_by = Team)]
struct Rebuild {}

impl Rebuild {
    fn run(self, _tx: &mut ScopedTx<'_, '_, Team>) -> Result<(), JobError> {
        Ok(())
    }
}

#[route(POST "/orgs/:org/rebuild", auth)]
fn rebuild(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.enqueue(Rebuild {});
    Ok(())
}

fn main() {}
