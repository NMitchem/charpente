// Fan-out is the `cross_tenant` capability's (job-tenancy spec §2 R5): a
// route that never declared it has no `&CrossTenant` to hand `enqueue_in`,
// and `#[route]` refuses the argument, naming the one flag to add.
// `CrossTenant` and `WireU64` are named by path for
// `across_tenants_without_capability.rs`'s reason.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::kernel::JobError;
use charpente::{Id, ScopedTx, Tenant, Tx, job, route, table};

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

#[job(scoped_by = Org)]
struct ApplyWindow {}

impl ApplyWindow {
    fn run(self, _tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        Ok(())
    }
}

#[route(POST "/admin/apply/:org", auth)]
fn apply_everywhere(
    tx: &mut Tx,
    org: Id<Org>,
    cap: &charpente::CrossTenant,
) -> Result<charpente::wire::WireU64, ApiError> {
    tx.enqueue_in(org, ApplyWindow {}, cap);
    Ok(charpente::wire::WireU64(org.raw()))
}

fn main() {}
