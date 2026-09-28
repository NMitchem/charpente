// The seal on `TenantAdmission` (job-tenancy spec §3, §7): its field is
// private and `Group::tenant_admission` is its only constructor, and a
// handler holds `&mut Tx` and `&Ctx`, never a `Group`. So a handler that
// forges one to enter a tenant without its membership check does not
// compile — the seal `cross_tenant_token_cannot_be_forged.rs` pins for
// `CrossTenant`, on the admission.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::wire::WireU64;
use charpente::{Id, Tenant, TenantAdmission, Tx, route, table};

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

#[route(GET "/orgs/:org/invoices", public)]
fn invoices(tx: &mut Tx, org: Id<Org>) -> Result<WireU64, ApiError> {
    let tx = tx.admit_scope::<Org>(org, &TenantAdmission(()));
    Ok(WireU64(tx.scan::<Invoice>()?.len() as u64))
}

fn main() {}
