// `emit_keyed` is `emit` plus an app key (tenancy-completion spec §2.3), and
// a key is never a tenant. So a scoped event on a bare `Tx` is refused here
// exactly as under `emit`, whatever the key says. Before this spec, a key
// holding the tenant's id was how an app scoped an event by hand.
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
    let _ = ctx;
    let event = IncidentOpened {
        incident: charpente::wire::WireU64(7),
    };
    tx.emit_keyed(org.raw().to_string(), event);
    Ok(())
}

fn main() {}
