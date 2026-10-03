// Job rows are the platform's (job-tenancy final review C1). The dispatcher
// opens the tenant a `JobRow`'s `scope` names without asking anyone, so a
// handler that could re-put a Pending row with another tenant would run the
// job there: this anonymous route, with no scope line and no `cross_tenant`,
// moved an org-1 job into org 9 at 79ae1c0. The read still compiles; the
// write is refused, and the note says how a job gets its tenant.
use charpente::auth::Principal;
use charpente::core::job::JobRow;
use charpente::http::ApiError;
use charpente::{Id, Tenant, Tx, View, route, table};

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
        Err(ApiError::NotFound)
    }
}

#[route(POST "/jobs/:job/redirect", public)]
fn redirect(tx: &mut Tx, job: Id<JobRow>) -> Result<(), ApiError> {
    let mut row = tx.get(job)?.ok_or(ApiError::NotFound)?;
    row.scope = Some(9);
    tx.put(&row);
    Ok(())
}

fn main() {}
