// Spec R3 rests on the glue's fn-pointer binding: it holds a global outbound
// job's `call` to a shared `&Tx`. A direct call from the glue, which holds a
// `&mut Tx`, would accept a `call` taking `&mut Tx`, and that `call` could
// write. The binding makes it a type error.
use charpente::kernel::JobError;
use charpente::outbound::{Call, IdempotencyKey, Reply};
use charpente::{Tx, job};

#[job(global, outbound = "api.stripe.com")]
struct Charge {}

impl Charge {
    fn call(&self, _tx: &mut Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(None)
    }

    fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
        Ok(())
    }
}

fn main() {}
