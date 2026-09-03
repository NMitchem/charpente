// Positive control for compile_fail/table_scoped_by_missing_tenant_scope.rs:
// the identical scoped table, with the `Tenant` impl the missing-impl
// case lacks — proving the rejection is the missing impl and not `scoped_by`
// itself.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::{Id, Tenant, Tx, table};

#[table]
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

fn main() {}
