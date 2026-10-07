// Neither capability token is `Clone` (tenancy-completion spec §5), so there
// is no owned token to keep beyond the request or group that minted it. A
// handler is handed `&CrossTenant`: `cap.clone()` only copies the reference,
// so it is not a token of the handler's own. Operator code holds an owned
// `TenantAdmission` and cannot copy it either.
use charpente::http::ApiError;
use charpente::wire::WireU64;
use charpente::{CrossTenant, Db, Tx, route};

#[route(GET "/admin/revenue", public, cross_tenant)]
fn revenue(tx: &mut Tx, cap: &CrossTenant) -> Result<WireU64, ApiError> {
    let owned: CrossTenant = cap.clone();
    let _ = (tx, owned);
    Ok(WireU64(0))
}

fn copy_the_admission(db: &mut Db) -> std::io::Result<()> {
    let group = db.begin_group()?;
    let admission = group.tenant_admission();
    let _copy = admission.clone();
    group.commit().map_err(|failure| failure.error)
}

fn main() {
    let _ = copy_the_admission;
}
