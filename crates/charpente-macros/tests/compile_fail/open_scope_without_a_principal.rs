// C-2 edit (b), from the statuspage review (job-tenancy spec, Context): an
// `auth` write route rewritten to open its tenant with
// `open_scope(org, None)` instead of `tx.scope::<Org>(org, ctx)?`. Under
// `Option<&Principal>` that compiled. It asked `authorize` about nobody, and
// with an `authorize` that admitted `None` for its jobs' sake, one org's key
// wrote another org's incidents while the sim batch stayed byte-identical.
// `authorize` now takes `&Principal`, so there is no nobody to pass.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::wire::WireU64;
use charpente::{Id, Tenant, Tx, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[table(scoped_by = Org)]
struct Incident {
    id: Id<Incident>,
    title: String,
}

#[route(POST "/orgs/:org/incidents/:id/resolve", auth)]
fn resolve_incident(
    tx: &mut Tx,
    org: Id<Org>,
    id: Id<Incident>,
    ctx: &Ctx,
) -> Result<WireU64, ApiError> {
    let _ = ctx;
    let mut tx = tx.open_scope::<Org>(org, None)?;
    tx.delete::<Incident>(id);
    Ok(WireU64(id.raw()))
}

fn main() {}
