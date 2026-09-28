// Spec §3 — "naming a tenant is always allowed; enumerating without naming
// one is a declared capability". The trybuild case the spec's oracle list
// names: `across_tenants()` from a route lacking `cross_tenant`.
//
// The handler takes the `&CrossTenant` the platform hands a declared route,
// and calls `across_tenants` with it — but the route never declared the
// capability, so there is no token for the glue to fill the argument from.
// `#[route]` refuses, naming the flag.
//
// The other half of the property — that a handler cannot simply mint its own
// token — is `cross_tenant_token_cannot_be_forged.rs`, in its own file
// because a macro error aborts the compile before type checking, so a
// forging attempt in THIS file would be reported as nothing at all.
//
// `CrossTenant` and `WireU64` are named by path rather than imported: the
// refused item is the only thing that uses them, so an import would come
// back as an "unused import" warning in the pin below and bury the one
// message this case is about.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::{Id, Tenant, Tx, route, table};

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
        Err(ApiError::Unauthorized)
    }
}

#[route(GET "/admin/revenue", public)]
fn revenue(
    tx: &mut Tx,
    cap: &charpente::CrossTenant,
) -> Result<charpente::wire::WireU64, ApiError> {
    Ok(charpente::wire::WireU64(
        tx.across_tenants::<Org>(cap).scan::<Invoice>()?.len() as u64,
    ))
}

fn main() {}
