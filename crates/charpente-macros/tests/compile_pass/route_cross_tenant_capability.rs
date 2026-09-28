// Positive control for compile_fail/across_tenants_without_capability.rs:
// the SAME handler, on a route that declares `cross_tenant`. What that file
// refuses is the missing declaration, not the `cap` argument or the
// `across_tenants` call — without this, "it does not compile" could equally
// mean the capability is unusable.
//
// Also the happy path itself: the rows come back PAIRED WITH THEIR SCOPE, so
// the annotated `Vec<(Id<Org>, Invoice)>` below is part of the pin.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::wire::WireU64;
use charpente::{CrossTenant, Id, Tenant, Tx, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(scoped_by = Org)]
struct Invoice {
    id: Id<Invoice>,
    total: u32,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[route(GET "/admin/revenue", auth, cross_tenant)]
fn revenue(tx: &mut Tx, cap: &CrossTenant) -> Result<WireU64, ApiError> {
    let rows: Vec<(Id<Org>, Invoice)> = tx.across_tenants::<Org>(cap).scan::<Invoice>()?;
    Ok(WireU64(rows.iter().map(|(_, i)| u64::from(i.total)).sum()))
}

fn main() {}
