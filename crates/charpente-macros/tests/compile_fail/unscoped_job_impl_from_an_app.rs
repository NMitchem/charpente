// The job twin of `unscoped_row_impl_from_an_app.rs` (job-tenancy spec §5):
// `UnscopedJob` is the bound `Tx::enqueue` and `App::schedule` actually name,
// so sealing `NoJobTenant` alone leaves the same one-line opt-out one level
// up. The impl below is orphan-legal (`Notify` is local) and, unsealed, it
// would let a scoped job be enqueued on a bare `Tx`, where no tenant is named.
// Sealed, so the impl is refused with the note that says where to enqueue it.
use charpente::auth::Principal;
use charpente::core::table::UnscopedJob;
use charpente::http::ApiError;
use charpente::kernel::JobError;
use charpente::{Id, ScopedTx, Tenant, Tx, job, table};

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

impl UnscopedJob for Notify {}

fn enqueue(tx: &mut Tx) {
    tx.enqueue(Notify { incident: 1 });
}

fn main() {
    let _ = enqueue;
}
