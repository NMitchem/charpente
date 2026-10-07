// A scoped event emitted on a bare `Tx` (tenancy-completion spec §6). The
// `Tx` names no tenant, so the event would reach no tenant's feed, and as a
// global row it would reach every global one. The `NoEventTenant` note says
// where to emit it instead.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
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

#[route(POST "/orgs/:org/incidents", auth)]
fn open_incident(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<(), ApiError> {
    let _ = (org, ctx);
    let event = IncidentOpened {
        incident: charpente::wire::WireU64(7),
    };
    tx.emit(event);
    Ok(())
}

fn main() {}
