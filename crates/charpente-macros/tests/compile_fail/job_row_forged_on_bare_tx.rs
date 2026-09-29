// Job rows are the platform's (job-tenancy final review C1): `JobRow`'s
// fields are public, so a handler can build one, and at 79ae1c0 a forged
// Pending row naming org 13 ran its job there after the next restart. The
// write is refused; enqueueing is the only way to make a job row.
use charpente::auth::Principal;
use charpente::core::db::Job;
use charpente::core::job::{JobRow, JobStatus};
use charpente::core::table::Codec;
use charpente::http::ApiError;
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
        Err(ApiError::NotFound)
    }
}

#[job(scoped_by = Org)]
struct CloseIncident {
    incident: u64,
}

impl CloseIncident {
    fn run(self, _tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        let _ = self.incident;
        Ok(())
    }
}

#[route(POST "/forge", public)]
fn forge(tx: &mut Tx) -> Result<(), ApiError> {
    let mut payload = Vec::new();
    CloseIncident { incident: 7 }.encode(&mut payload);
    tx.put(&JobRow {
        id: Id::from_raw(500),
        name: "CloseIncident".into(),
        payload,
        key: None,
        status: JobStatus::Pending,
        attempts: 0,
        due_at: 0,
        last_error: None,
        shape: Some(<CloseIncident as Job>::FINGERPRINT),
        scope: Some(13),
    });
    Ok(())
}

fn main() {}
