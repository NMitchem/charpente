// Job rows are the platform's (job-tenancy final review C1): a handler that
// nulled a scoped job's `scope` left a row the dispatcher would open no
// tenant for, and at 79ae1c0 one anonymous request stopped the kernel on it,
// again at every restart. The write is refused, so the row cannot be made.
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
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Err(ApiError::NotFound)
    }
}

#[route(POST "/jobs/:job/untenant", public)]
fn untenant(tx: &mut Tx, job: Id<JobRow>) -> Result<(), ApiError> {
    let mut row = tx.get(job)?.ok_or(ApiError::NotFound)?;
    row.scope = None;
    tx.put(&row);
    Ok(())
}

fn main() {}
