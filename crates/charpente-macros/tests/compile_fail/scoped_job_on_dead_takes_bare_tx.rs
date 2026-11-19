// The fn-pointer binding, for the hook (0.5.0 spec §5 C): a scoped job's
// `on_dead` takes its tenant's `ScopedTx`, as its `run` does. One taking a
// bare `Tx` would compile under a method call, because `&mut ScopedTx`
// deref-coerces to `&mut Tx`, and its writes would land in no tenant. The
// binding makes it a type error instead (`scoped_job_run_takes_bare_tx.rs`).
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::kernel::JobError;
use charpente::{Dead, Id, ScopedTx, Tenant, Tx, job, table};

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

#[job(scoped_by = Org, on_dead)]
struct File {
    n: u64,
}

impl File {
    fn run(self, _tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        let _ = self.n;
        Ok(())
    }

    fn on_dead(self, _tx: &mut Tx, _dead: Dead<Self>) -> Result<(), JobError> {
        Ok(())
    }
}

fn main() {}
