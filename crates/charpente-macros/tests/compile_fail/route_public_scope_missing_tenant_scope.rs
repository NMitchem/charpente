// `public(Foo)` puts `Foo` in a tenant scope's place, so `Foo` must carry
// the app's own membership check. The route emits the same `TenantScope`
// const assert `#[table(scoped_by = …)]` does, and gets the same diagnostic,
// spanned at the path inside `public(...)`.
use charpente::http::ApiError;
use charpente::wire::WireU64;
use charpente::{Id, Tx, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[route(GET "/orgs/:org/status", public(Org))]
fn status_page(tx: &mut Tx, org: Id<Org>) -> Result<WireU64, ApiError> {
    let _ = tx;
    Ok(WireU64(org.raw()))
}

fn main() {}
