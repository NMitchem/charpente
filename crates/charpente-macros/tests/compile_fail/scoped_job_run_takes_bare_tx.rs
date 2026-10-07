// The fn-pointer binding (job-tenancy spec §5): a scoped job's `run` takes
// its tenant's `ScopedTx`. A `run` that takes a bare `Tx` would compile under
// a method call, because `&mut ScopedTx` deref-coerces to `&mut Tx`, and it
// would then run in no tenant. The binding makes it a type error instead.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::kernel::JobError;
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

#[job(scoped_by = Org)]
struct File {
    n: u64,
}

impl File {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        let _ = self.n;
        Ok(())
    }
}

fn main() {}
