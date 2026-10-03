// `Tx::across_tenants::<S>` enumerates every tenant of a tenant type, so `S`
// is bounded `Tenant` (tenancy-completion final review M2). `PlatformOwned`
// and `Unscoped` are scope markers with no tenants: each compiled, and its
// `scan` then panicked on the request path, which stops the node, with a text
// that named the wrong scope and a fix that is itself a compile error. Both
// are refused at the call.
use charpente::core::job::JobRow;
use charpente::core::table::{PlatformOwned, Unscoped};
use charpente::http::ApiError;
use charpente::wire::WireU64;
use charpente::{CrossTenant, Id, Tx, route, routes, table};

#[table(global)]
struct Region {
    id: Id<Region>,
    name: String,
}

#[route(GET "/admin/jobs", public, cross_tenant)]
fn every_job(tx: &mut Tx, cap: &CrossTenant) -> Result<WireU64, ApiError> {
    Ok(WireU64(
        tx.across_tenants::<PlatformOwned>(cap).scan::<JobRow>()?.len() as u64,
    ))
}

#[route(GET "/admin/regions", public, cross_tenant)]
fn every_region(tx: &mut Tx, cap: &CrossTenant) -> Result<WireU64, ApiError> {
    Ok(WireU64(
        tx.across_tenants::<Unscoped>(cap).scan::<Region>()?.len() as u64,
    ))
}

fn main() {
    let _ = routes![every_job, every_region];
}
