// The job queue is the platform's to read as well as to write
// (tenancy-completion spec §3). It holds every tenant's job payloads and the
// tenant each job runs in, so a handler that could scan it would list another
// tenant's work, and one that could `get` a row would read a payload it was
// never handed. Both reads are refused, and the note names the operator's
// path: a snapshot, from code that holds the `Db`.
use charpente::core::job::JobRow;
use charpente::http::ApiError;
use charpente::{Id, Tx, View, route};

#[route(GET "/jobs", public)]
fn list_jobs(tx: &mut Tx) -> Result<(), ApiError> {
    let _every_tenants_jobs = tx.scan::<JobRow>()?;
    Ok(())
}

#[route(GET "/jobs/:job", public)]
fn peek_job(tx: &mut Tx, job: Id<JobRow>) -> Result<(), ApiError> {
    let _row = tx.get(job)?;
    Ok(())
}

fn main() {}
