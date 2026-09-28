// The job twin of `no_tenant_impl_from_an_app.rs` (job-tenancy spec §5):
// `UnscopedJob` holds exactly when the job's scope type implements
// `NoJobTenant<Job>`. The impl below is legal under the orphan rules — `Org`
// is local — so without a seal it is one safe line that lets a scoped job be
// enqueued on a bare `Tx`, where no tenant is named. The seal, the same
// `NotATenant` that `NoTenant` carries, keeps that a compile error.
use charpente::auth::Principal;
use charpente::core::table::NoJobTenant;
use charpente::http::ApiError;
use charpente::kernel::JobError;
use charpente::{Id, ScopedTx, Tenant, Tx, job, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Err(ApiError::Unauthorized)
    }
}

#[job(scoped_by = Org)]
struct Notify {
    incident: u64,
}

impl Notify {
    fn run(self, _tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        let _ = self.incident;
        Ok(())
    }
}

impl NoJobTenant<Notify> for Org {}

fn enqueue(tx: &mut Tx) {
    tx.enqueue(Notify { incident: 1 });
}

fn main() {
    let _ = enqueue;
}
