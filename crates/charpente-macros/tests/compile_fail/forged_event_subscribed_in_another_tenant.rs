// Tenancy-completion spec §6 (Task 11 fix round 1): a hand-written
// `impl Event` declared `type Scope = Team`, with `NAME` and `FINGERPRINT`
// copied from `Org`'s `InvoicePaid`, meets every tenant-feed bound. A `Team`
// feed of it would admit `Org`'s rows at every raw id the two types share,
// and decode them, since the copied shape matches. So the tenant feed's
// builders check, in a `const` block, that the event's `FINGERPRINT` mixes
// `Team`'s `NAME`. That is a post-monomorphization error, which `cargo build`
// and `cargo test` report and `cargo check` does not, for the builders the
// binary can reach, which is why `main` mounts both routes. One route per
// builder, so each builder's refusal is pinned.
use charpente::core::table::Codec;
use charpente::http::{ApiError, Ctx, Cursor, Subscription};
use charpente::wire::WireU64;
use charpente::{Event, Id, Scoped, Subscribe, Tx, event, route, routes};

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

    #[table(global)]
    pub struct Team {
        pub id: Id<Team>,
        pub name: String,
    }

    impl Tenant for Team {
        const NAME: &'static str = "Team";
        type Who = Principal;
        type Error = ApiError;
        fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Team>) -> Result<(), ApiError> {
            Ok(())
        }
    }
}
use tenancy::{Org, Team};

#[event(scoped_by = Org)]
struct InvoicePaid {
    invoice: WireU64,
}

#[event(scoped_by = Team)]
struct TeamNote {
    n: WireU64,
}

/// `Team`'s by declaration, `Org`'s `InvoicePaid` by identity.
#[derive(serde::Serialize)]
struct Spy {
    invoice: WireU64,
}

impl Codec for Spy {
    fn encode(&self, out: &mut Vec<u8>) {
        InvoicePaid {
            invoice: self.invoice,
        }
        .encode(out)
    }
    fn decode(input: &mut &[u8]) -> std::io::Result<Spy> {
        Ok(Spy {
            invoice: InvoicePaid::decode(input)?.invoice,
        })
    }
}

impl Event for Spy {
    const NAME: &'static str = <InvoicePaid as Event>::NAME;
    const FINGERPRINT: u64 = <InvoicePaid as Event>::FINGERPRINT;
    type Scope = Team;
}

#[route(SSE "/teams/:team/spy", public(Team))]
fn spy_feed(
    tx: &mut Tx,
    team: Id<Team>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Team>(team, ctx)?;
    Ok(tx.subscription::<Spy>().cursor(cursor).into())
}

#[route(SSE "/teams/:team/notes", public(Team))]
fn notes_feed(
    tx: &mut Tx,
    team: Id<Team>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Team>(team, ctx)?;
    Ok(tx.subscription::<TeamNote>().and::<Spy>().cursor(cursor).into())
}

fn main() {
    let _ = routes![spy_feed, notes_feed];
}
