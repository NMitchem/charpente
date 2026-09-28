// `public` composes with `cross_tenant` (default-secure spec §2; DECISIONS
// §11): an anonymous cross-tenant read is the maximal-leak shape, which is
// exactly why it must stay declarable and enumerable — `charpente describe`
// prints `auth=false cross_tenant=true` for it, and that line is the audit —
// rather than inexpressible. This is route_cross_tenant_capability.rs with
// `public` where `auth` stood.
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

#[route(GET "/admin/revenue", public, cross_tenant)]
fn revenue(tx: &mut Tx, cap: &CrossTenant) -> Result<WireU64, ApiError> {
    let rows: Vec<(Id<Org>, Invoice)> = tx.across_tenants::<Org>(cap).scan::<Invoice>()?;
    Ok(WireU64(rows.iter().map(|(_, i)| u64::from(i.total)).sum()))
}

fn main() {}
