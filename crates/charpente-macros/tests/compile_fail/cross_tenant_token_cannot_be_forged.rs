// The other half of spec §3's capability (see
// `across_tenants_without_capability.rs`): the declaration is only an
// inventory if a handler cannot mint the token itself. `CrossTenant`'s field
// is private and `Group::cross_tenant_cap` is its only constructor — and a
// handler holds `&mut Tx` and `&Ctx`, never a `Group`. So this route, which
// declares nothing and forges its own capability, does not compile.
//
// Its own file rather than a second case in
// `across_tenants_without_capability.rs`: a `#[route]` macro error aborts
// the compile before type checking, so a forging attempt sharing that file
// would be reported as nothing at all — a pin that certifies nothing.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::wire::WireU64;
use charpente::{CrossTenant, Id, Tenant, Tx, route, table};

#[table]
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
    fn authorize(_tx: &Tx, _who: Option<&Principal>, _scope: Id<Org>) -> Result<(), ApiError> {
        Err(ApiError::Unauthorized)
    }
}

#[route(GET "/admin/revenue")]
fn revenue(tx: &mut Tx) -> Result<WireU64, ApiError> {
    Ok(WireU64(
        tx.across_tenants::<Org>(&CrossTenant(()))
            .scan::<Invoice>()?
            .len() as u64,
    ))
}

fn main() {}
