// The shapes a scoped event is written in (tenancy-completion spec §6):
// - declared `scoped_by`, with the tenant's path bare or qualified;
// - emitted, keyed or not, inside its tenant's scope, from a handler and from
//   a scoped job's `run`;
// - a global event emitted beside it through the deref;
// - a tenant feed of two scoped events, narrowed by an app key;
// - a global feed beside it.
// trybuild runs `main`, so its assertions are part of the pin.
use charpente::describe::EventEntry;
use charpente::http::{ApiError, Ctx, Cursor, Subscription};
use charpente::kernel::JobError;
use charpente::wire::WireU64;
use charpente::{Event, Id, Scoped, ScopedTx, Subscribe, Tx, event, job, route};
use std::any::TypeId;

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
        const NAME: &'static str = "Org";
        type Who = Principal;
        type Error = ApiError;
        fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
            Ok(())
        }
    }
}
use tenancy::Org;

#[event(scoped_by = Org)]
struct IncidentOpened {
    incident: WireU64,
}

/// The same fields and the same tenant as `IncidentOpened`, spelled through
/// the qualified path. One tenant is one shape, however its path is written.
#[event(scoped_by = tenancy::Org)]
struct IncidentClosed {
    incident: WireU64,
}

#[event(global)]
struct SystemNotice {
    n: WireU64,
}

#[job(scoped_by = Org)]
struct Notify {
    incident: u64,
}

impl Notify {
    fn run(self, tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        tx.emit(IncidentOpened {
            incident: WireU64(self.incident),
        });
        // `run` holds `&mut ScopedTx`, so the global deref is `**`.
        (**tx).emit(SystemNotice {
            n: WireU64(self.incident),
        });
        Ok(())
    }
}

#[route(POST "/orgs/:org/incidents/close", auth)]
fn close(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<(), ApiError> {
    let id = WireU64(7);
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.emit(IncidentOpened { incident: id });
    tx.emit_keyed(format!("user:{}", id.0), IncidentClosed { incident: id });
    (*tx).emit(SystemNotice { n: id });
    tx.enqueue(Notify { incident: id.0 });
    Ok(())
}

#[route(SSE "/orgs/:org/feed", auth, emits(IncidentOpened, IncidentClosed))]
fn feed(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(tx
        .subscription::<IncidentOpened>()
        .and::<IncidentClosed>()
        .key("user:7")
        .cursor(cursor)
        .into())
}

#[route(SSE "/notices", public, emits(SystemNotice))]
fn notices(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<SystemNotice>().cursor(cursor))
}

fn main() {
    let opened = <IncidentOpened as EventEntry>::def();
    assert_eq!(opened.scoped_by, Some("Org"));
    assert_eq!(opened.scope_type, TypeId::of::<Org>());
    let closed = <IncidentClosed as EventEntry>::def();
    assert_eq!(closed.scoped_by, Some("Org"), "a qualified path names the same tenant");
    assert_eq!(closed.scope_type, TypeId::of::<Org>());
    let notice = <SystemNotice as EventEntry>::def();
    assert_eq!(notice.scoped_by, None);
    assert_eq!(
        notice.scope_type,
        TypeId::of::<charpente::core::table::Unscoped>()
    );
    assert_eq!(
        <IncidentOpened as Event>::FINGERPRINT,
        <IncidentClosed as Event>::FINGERPRINT,
        "one tenant is one shape, however its path is spelled"
    );
}
