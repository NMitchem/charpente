// Plan amendment A6: an outbound job writes `call` and `settle`, not `run`.
// The attribute cannot see the impl, so rustc's E0599 for the missing `call`
// (and `settle`) is the refusal.
use charpente::kernel::JobError;
use charpente::{Tx, job};

#[job(global, outbound = "api.stripe.com")]
struct Charge {}

impl Charge {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

fn main() {}
