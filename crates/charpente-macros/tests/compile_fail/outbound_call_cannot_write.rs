// Spec R3: `call` takes `&Tx`, and a write needs `&mut`, so `call` reads and
// cannot write — its effects belong in `settle`, which commits with the Done
// mark.
use charpente::kernel::JobError;
use charpente::outbound::{Call, IdempotencyKey, Reply};
use charpente::{Id, Tx, job, table};

#[job(global, outbound = "api.stripe.com")]
struct Charge {}

#[table(global)]
struct Note {
    id: Id<Note>,
    text: String,
}

impl Charge {
    fn call(&self, tx: &Tx, _key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        tx.put(&Note {
            id: Id::from_raw(1),
            text: String::new(),
        });
        Ok(None)
    }

    fn settle(self, _tx: &mut Tx, _reply: Reply) -> Result<(), JobError> {
        Ok(())
    }
}

fn main() {}
