// Positive control for compile_fail/across_tenants_without_capability.rs:
// the SAME handler, on a route that declares `cross_tenant`. What that file
// refuses is the missing declaration, not the `cap` argument or the
// `across_tenants` call — without this, "it does not compile" could equally
// mean the capability is unusable.
//
// Also the happy path itself: the rows come back PAIRED WITH THEIR SCOPE, so
// the annotated `Vec<(Id<Org>, Invoice)>` below is part of the pin.
//
// `incidents` is the token on a stream route: after the :params, before
// `ctx`, the slot an SSE handler's shape check counts it in. `by_cap` is the
// other side of the token rule: only a SPARE argument named `cap` is the
// token, so a route whose :param is named `cap` binds it as that param, and
// needs no `cross_tenant`.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx, Cursor, Subscription};
use charpente::wire::WireU64;
use charpente::{CrossTenant, Id, Tenant, Tx, event, route, table};

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
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[event(global)]
struct IncidentOpened {
    n: WireU64,
}

#[route(GET "/admin/revenue", auth, cross_tenant)]
fn revenue(tx: &mut Tx, cap: &CrossTenant) -> Result<WireU64, ApiError> {
    let rows: Vec<(Id<Org>, Invoice)> = tx.across_tenants::<Org>(cap).scan::<Invoice>()?;
    Ok(WireU64(rows.iter().map(|(_, i)| u64::from(i.total)).sum()))
}

#[route(SSE "/admin/orgs/:org/incidents", auth, cross_tenant, emits(IncidentOpened))]
fn incidents(
    tx: &mut Tx,
    org: Id<Org>,
    cap: &CrossTenant,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let _ = (tx, org, cap, ctx);
    Ok(Subscription::of::<IncidentOpened>().cursor(cursor))
}

#[route(GET "/orgs/:cap", auth)]
fn by_cap(tx: &mut Tx, cap: Id<Org>) -> Result<(), ApiError> {
    let _ = (tx, cap);
    Ok(())
}

fn main() {}
