// A scoped row written on a bare `Tx` gets exactly one diagnostic, the
// tenant one (`NoTenant`), as a scoped read does. `Tx::put`'s bound is
// `WritableRow`, which also refuses a platform row; it asks that of the
// scope's owner through `NoTenant`'s seal precisely so that this mistake is
// not also told "the row is the platform's" (job-tenancy final review C1).
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::{Id, Tenant, Tx, route, table};

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

#[route(POST "/invoices/:id", public)]
fn create(tx: &mut Tx, id: Id<Invoice>) -> Result<(), ApiError> {
    tx.put(&Invoice { id, total: 0 });
    Ok(())
}

fn main() {}
