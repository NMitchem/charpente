// A `ScopedTx<Org>` emits `Org`'s events only (tenancy-completion spec §6).
// An event scoped by another type would be recorded under an `Org` id, where
// no `Team` feed could ever match it. This is rustc's own E0271, pinned so
// GUIDE §11 can name it.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::{Id, Scoped, Tenant, Tx, event, route, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(global)]
struct Team {
    id: Id<Team>,
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

impl Tenant for Team {
    const NAME: &'static str = "Team";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Team>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[event(scoped_by = Team)]
struct RotaChanged {}

#[route(POST "/orgs/:org/rota", auth)]
fn rota(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.emit(RotaChanged {});
    Ok(())
}

fn main() {}
