// A global job enqueued through a `ScopedTx` without the deref (job-tenancy
// spec §5) is an E0271, not a silent enqueue in no tenant.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::kernel::JobError;
use charpente::{Id, Scoped, Tenant, Tx, job, route, table};

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

#[job(global)]
struct Audit {}

impl Audit {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

// `ScopedTx::enqueue` shadows `Tx::enqueue`, and Rust does not fall through the
// deref when the inherent method's bound fails: a global job is enqueued inside
// a scope as `(*tx).enqueue(Audit {})`. This pins the E0271 an author meets
// first, so GUIDE §16 can show it.
#[route(POST "/orgs/:org/audit", auth)]
fn audit(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.enqueue(Audit {});
    Ok(())
}

fn main() {}
