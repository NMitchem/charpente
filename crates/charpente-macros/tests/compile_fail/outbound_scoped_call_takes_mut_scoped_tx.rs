// The scoped twin of `outbound_call_takes_mut_tx.rs`: the binding holds a
// scoped outbound job's `call` to a shared `&ScopedTx`, so a `call` taking
// `&mut ScopedTx` (which could write in its tenant) is a type error.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::kernel::JobError;
use charpente::outbound::{Call, IdempotencyKey, Reply};
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
        Ok(())
    }
}

#[job(scoped_by = Org, outbound = "api.stripe.com")]
struct Charge {}

impl Charge {
    fn call(
        &self,
        _tx: &mut ScopedTx<'_, '_, Org>,
        _key: &IdempotencyKey,
    ) -> Result<Option<Call>, JobError> {
        Ok(None)
    }

    fn settle(self, _tx: &mut ScopedTx<'_, '_, Org>, _reply: Reply) -> Result<(), JobError> {
        Ok(())
    }
}

fn main() {}
