// Tenancy-completion spec §6 (Task 11 fix round 1): a hand-written
// `impl Event` that states its shape as `#[event]` would passes every emit
// and subscribe site's `const` check. A scoped one sets `PAYLOAD_FINGERPRINT`
// to its fields' fold and `FINGERPRINT` to that fold mixed with its tenant's
// `NAME`; a global one states `FINGERPRINT` alone, which `PAYLOAD_FINGERPRINT`
// defaults to. `main` mounts every route, so the binary reaches all eight
// sites, and asserts that the derived event of the same fields agrees.
use charpente::core::db::scoped_fingerprint;
use charpente::core::table::{Codec, Unscoped};
use charpente::http::{ApiError, Ctx, Cursor, Subscription};
use charpente::wire::WireU64;
use charpente::{Event, Id, Scoped, Subscribe, Tenant, Tx, event, route, routes};

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

/// The derived event the hand-written ones are checked against.
#[event(scoped_by = Org)]
struct Derived {
    n: WireU64,
}

#[derive(serde::Serialize)]
struct HandScoped {
    n: WireU64,
}

impl Codec for HandScoped {
    fn encode(&self, out: &mut Vec<u8>) {
        self.n.encode(out)
    }
    fn decode(input: &mut &[u8]) -> std::io::Result<HandScoped> {
        Ok(HandScoped {
            n: WireU64::decode(input)?,
        })
    }
}

impl Event for HandScoped {
    const NAME: &'static str = "hand_scoped";
    const PAYLOAD_FINGERPRINT: u64 = <Derived as Event>::PAYLOAD_FINGERPRINT;
    const FINGERPRINT: u64 = scoped_fingerprint(Self::PAYLOAD_FINGERPRINT, <Org as Tenant>::NAME);
    type Scope = Org;
}

#[derive(serde::Serialize)]
struct HandGlobal {
    n: WireU64,
}

impl Codec for HandGlobal {
    fn encode(&self, out: &mut Vec<u8>) {
        self.n.encode(out)
    }
    fn decode(input: &mut &[u8]) -> std::io::Result<HandGlobal> {
        Ok(HandGlobal {
            n: WireU64::decode(input)?,
        })
    }
}

impl Event for HandGlobal {
    const NAME: &'static str = "hand_global";
    const FINGERPRINT: u64 = 0x7E57_0C0C;
    type Scope = Unscoped;
}

#[route(POST "/orgs/:org/hand", auth)]
fn emit_both(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.emit(HandScoped { n: WireU64(1) });
    tx.emit_keyed("user:7", HandScoped { n: WireU64(2) });
    (*tx).emit(HandGlobal { n: WireU64(3) });
    (*tx).emit_keyed("user:7", HandGlobal { n: WireU64(4) });
    Ok(())
}

#[route(SSE "/orgs/:org/hand-feed", auth)]
fn scoped_feed(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(tx
        .subscription::<Derived>()
        .and::<HandScoped>()
        .cursor(cursor)
        .into())
}

#[route(SSE "/hand-feed", public)]
fn global_feed(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<HandGlobal>().cursor(cursor))
}

#[route(SSE "/orgs/:org/hand-only", auth)]
fn hand_only_feed(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(tx.subscription::<HandScoped>().cursor(cursor).into())
}

#[route(SSE "/hand-feed-and", public)]
fn global_feed_and(
    tx: &mut Tx,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<HandGlobalTwin>()
        .and::<HandGlobal>()
        .key("user:7")
        .cursor(cursor))
}

#[event(global)]
struct HandGlobalTwin {
    n: WireU64,
}

fn main() {
    let _ = routes![emit_both, scoped_feed, global_feed, hand_only_feed, global_feed_and];
    assert_eq!(
        <HandScoped as Event>::FINGERPRINT,
        <Derived as Event>::FINGERPRINT,
        "the stated shape is the derived one for the same fields and tenant"
    );
    assert_eq!(
        <HandGlobalTwin as Event>::PAYLOAD_FINGERPRINT,
        <HandGlobalTwin as Event>::FINGERPRINT,
        "a global event's payload fold is its fingerprint"
    );
}
