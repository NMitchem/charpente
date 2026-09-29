// Job rows are the platform's (job-tenancy final review C1): a handler that
// could delete one would cancel another tenant's pending work, a tenant it
// was never admitted to. There is no app-side cancel: the delete is refused.
use charpente::core::job::JobRow;
use charpente::http::ApiError;
use charpente::{Id, Tx, route};

#[route(POST "/jobs/:job/cancel", public)]
fn cancel(tx: &mut Tx, job: Id<JobRow>) -> Result<(), ApiError> {
    tx.delete::<JobRow>(job);
    Ok(())
}

fn main() {}
