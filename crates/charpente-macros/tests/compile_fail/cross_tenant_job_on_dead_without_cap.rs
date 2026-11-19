// A `cross_tenant` job's hook takes the capability after `tx`, as its `run`
// does (0.5.0 spec §5 C). One written without it is a type error at the
// fn-pointer binding, whose note prints the expected signature.
use charpente::kernel::JobError;
use charpente::{CrossTenant, Dead, Tx, job};

#[job(global, cross_tenant, on_dead)]
struct Rollup {}

impl Rollup {
    fn run(self, _tx: &mut Tx, _cap: &CrossTenant) -> Result<(), JobError> {
        Ok(())
    }

    fn on_dead(self, _tx: &mut Tx, _dead: Dead<Self>) -> Result<(), JobError> {
        Ok(())
    }
}

fn main() {}
