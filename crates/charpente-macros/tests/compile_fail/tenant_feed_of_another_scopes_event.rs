// A tenant feed carries its tenant's type (tenancy-completion spec §6,
// amendment A3): on a `ScopedTx<Org>`, `tx.subscription::<Ev>()` and its
// `.and::<Ev2>()` take events scoped by `Org` only. An event scoped by
// another type, or a global one, could never match the feed's tenant. Each
// is rustc's own E0271, never an entry that silently receives nothing.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx, Cursor, Subscription};
use charpente::{Id, Scoped, Subscribe, Tenant, Tx, event, route, table};

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

#[event(scoped_by = Org)]
struct IncidentOpened {}

#[event(scoped_by = Team)]
struct RotaChanged {}

#[event(global)]
struct SystemNotice {}

#[route(SSE "/orgs/:org/feed", auth)]
fn feed(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(tx
        .subscription::<IncidentOpened>()
        .and::<RotaChanged>()
        .cursor(cursor)
        .into())
}

#[route(SSE "/orgs/:org/notices", auth)]
fn notices(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(tx.subscription::<SystemNotice>().cursor(cursor).into())
}

fn main() {}
