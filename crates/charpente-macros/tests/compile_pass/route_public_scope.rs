// `#[route(…, public(Org))]` (job-tenancy spec §4): every caller,
// credentialed or not, may act inside the `Org` tenant the route opens. The
// flag composes with `cross_tenant` and `emits(...)` in any order, the scope
// may be path-qualified, and the `Route` carries its name and `TypeId`.
// trybuild runs `main`, so its assertions are part of the pin.
use charpente::http::{ApiError, Ctx, Cursor, RouteDef, Subscription};
use charpente::wire::WireU64;
use charpente::{CrossTenant, Id, Scoped, Tx, event, route, table};

mod tenancy {
    use charpente::auth::Principal;
    use charpente::http::ApiError;
    use charpente::{Id, Tenant, Tx, table};

    #[table(global)]
    pub struct Org {
        pub id: Id<Org>,
        pub name: String,
    }

    impl Tenant for Org {
        type Who = Principal;
        type Error = ApiError;
        fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
            Err(ApiError::Unauthorized)
        }
    }
}
use tenancy::Org;

#[table(scoped_by = Org)]
struct Incident {
    id: Id<Incident>,
    title: String,
}

#[event(global)]
struct IncidentOpened {
    n: WireU64,
}

#[route(GET "/orgs/:org/status", public(Org))]
fn status_page(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(WireU64(tx.scan::<Incident>()?.len() as u64))
}

#[route(GET "/orgs/:org/summary", public(tenancy::Org))]
fn summary(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(WireU64(tx.scope().raw()))
}

#[route(GET "/status", cross_tenant, public(Org))]
fn every_status(tx: &mut Tx, cap: &CrossTenant) -> Result<WireU64, ApiError> {
    Ok(WireU64(
        tx.across_tenants::<Org>(cap).scan::<Incident>()?.len() as u64,
    ))
}

#[route(SSE "/orgs/:org/feed", public(Org), emits(IncidentOpened))]
fn feed(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(Subscription::of::<IncidentOpened>()
        .key(tx.event_key())
        .cursor(cursor))
}

fn main() {
    for route in [
        status_page::route(),
        summary::route(),
        every_status::route(),
        feed::route(),
    ] {
        let scope = route
            .public_scope
            .expect("public(Org) is carried on the Route");
        assert_eq!(scope.name, "Org", "{}", route.name);
        assert_eq!((scope.type_id)(), std::any::TypeId::of::<Org>(), "{}", route.name);
        assert!(!route.auth, "public(Org) is public: {}", route.name);
    }
}
