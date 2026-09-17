// The headline claim of the tenant-scoping design (spec §1, §7 exit
// criterion 1): a scoped table is unreachable from a bare `Tx`. The handler
// below is the shape a reviewer used to have to catch by eye — a `scan` with
// no tenant filter — and it must not compile.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::{Id, Tenant, Tx, View, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(scoped_by = Org)]
struct Invoice {
    id: Id<Invoice>,
    org: Id<Org>,
    total: u32,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: Option<&Principal>, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

fn list(tx: &mut Tx) -> Result<Vec<Invoice>, ApiError> {
    Ok(tx.scan::<Invoice>()?)
}

fn main() {
    let _ = list;
}
