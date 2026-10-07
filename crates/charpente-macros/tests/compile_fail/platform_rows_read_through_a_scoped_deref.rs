// Opening a tenant's scope does not open the platform's tables
// (tenancy-completion spec §3). A `ScopedTx` reaches a bare `Tx` through its
// explicit deref, for the app's global tables
// (compile_pass/scoped_read_through_scoped_tx.rs), and there the platform's
// three are refused exactly as on a bare `Tx`.
use charpente::auth::Principal;
use charpente::core::event::EventRow;
use charpente::core::job::JobRow;
use charpente::core::migration::SchemaVersion;
use charpente::http::{ApiError, Ctx};
use charpente::{Id, Scoped, Tenant, Tx, View, route, table};

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

#[route(GET "/orgs/:org/platform", auth)]
fn platform(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<(), ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    let _jobs = (*tx).scan::<JobRow>()?;
    let _event = (*tx).get::<EventRow>(Id::from_raw(0))?;
    let _versions = (*tx).scan::<SchemaVersion>()?;
    Ok(())
}

fn main() {}
