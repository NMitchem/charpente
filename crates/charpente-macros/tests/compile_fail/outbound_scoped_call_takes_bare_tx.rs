// The fn-pointer bindings, for the reason `scoped_job_run_takes_bare_tx.rs`
// gives: a scoped outbound job's `call` takes its tenant's `&ScopedTx` and its
// `settle` its `&mut ScopedTx`. Either taking a bare `Tx` would compile under a
// method call, because `ScopedTx` derefs to `Tx`, and would then read or write
// in no tenant. The bindings make each a type error.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::kernel::JobError;
use charpente::outbound::{Call, IdempotencyKey, Reply};
use charpente::{Id, Tenant, Tx, job, table};

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

#[job(scoped_by = Org, outbound = "api.stripe.com")]
struct Charge {}

impl Charge {
    fn call(&self, _tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(None)
    }

    fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
        Ok(())
    }
}

fn main() {}
