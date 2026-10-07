// A global `Subscription` names no tenant (tenancy-completion spec §6), so
// `Subscription::of` and `.and` take global events only. A scoped event
// there is the `NoEventTenant` refusal, and its note names the tenant feed.
// Before this spec, such a feed received every tenant's rows of the type.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx, Cursor, Subscription};
use charpente::{Id, Tenant, Tx, event, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[event(scoped_by = Org)]
struct IncidentOpened {
    incident: charpente::wire::WireU64,
}

#[event(global)]
struct SystemNotice {}

#[route(SSE "/incidents", public)]
fn incidents(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<IncidentOpened>().cursor(cursor))
}

#[route(SSE "/everything", public)]
fn everything(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<SystemNotice>()
        .and::<IncidentOpened>()
        .cursor(cursor))
}

fn main() {}
