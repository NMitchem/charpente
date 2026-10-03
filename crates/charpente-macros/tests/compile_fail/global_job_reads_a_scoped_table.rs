// The scoped-table refusal inside a `global` job that carries other
// arguments (tenancy-completion spec §7, item 12's class). The note's job
// case names the word to change and restates no attribute, so a pasted fix
// keeps `cross_tenant` and `retry(...)`. A restated `#[job(scoped_by = Org)]`
// reset this job's retry policy to the default without a word.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::kernel::JobError;
use charpente::{CrossTenant, Id, Tenant, Tx, View, job, table};

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

#[table(scoped_by = Org)]
struct Invoice {
    id: Id<Invoice>,
    total: u32,
}

#[job(global, cross_tenant, retry(max = 10, backoff = "1m"))]
struct Rollup {}

impl Rollup {
    fn run(self, tx: &mut Tx, cap: &CrossTenant) -> Result<(), JobError> {
        let _ = cap;
        let _ = tx.scan::<Invoice>()?;
        Ok(())
    }
}

fn main() {}
