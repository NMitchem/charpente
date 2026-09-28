// The capability refusal (tenant-scoping spec §3) reconstructs the attribute
// the author should have written, so on a `public(Org)` route it keeps the
// `public(Org)` (job-tenancy spec §4). `CrossTenant` and `WireU64` are named
// by path for `across_tenants_without_capability.rs`'s reason.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::{Id, Tenant, Tx, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: Option<&Principal>, _scope: Id<Org>) -> Result<(), ApiError> {
        Err(ApiError::Unauthorized)
    }
}

#[route(GET "/orgs/:org/revenue", public(Org))]
fn revenue(
    tx: &mut Tx,
    org: Id<Org>,
    cap: &charpente::CrossTenant,
) -> Result<charpente::wire::WireU64, ApiError> {
    let _ = (tx, org, cap);
    Ok(charpente::wire::WireU64(0))
}

fn main() {}
