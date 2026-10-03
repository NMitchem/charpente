// Tenancy-completion spec §6 (Task 11 fix round 1), the write twin of
// `forged_event_subscribed_in_another_tenant.rs`: `Spy` is `Team`'s by
// declaration and `Org`'s `InvoicePaid` by identity, so emitted in `Team` 1
// it would be recorded under `InvoicePaid`'s name and shape with raw tenant
// 1, and `Org` 1's feed would deliver it as its own. `ScopedTx::emit` and
// `emit_keyed` check, in a `const` block, that the event's `FINGERPRINT`
// mixes `Team`'s `NAME`. Post-monomorphization, so `main` mounts the route;
// one call per method, so each method's refusal is pinned.
use charpente::core::table::Codec;
use charpente::http::{ApiError, Ctx};
use charpente::wire::WireU64;
use charpente::{Event, Id, Scoped, Tx, event, route, routes};

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

#[route(POST "/teams/:team/spy", public(Team))]
fn spy_emit(tx: &mut Tx, team: Id<Team>, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Team>(team, ctx)?;
    tx.emit(Spy {
        invoice: WireU64(1),
    });
    tx.emit_keyed("user:7", Spy {
        invoice: WireU64(2),
    });
    Ok(())
}

fn main() {
    let _ = routes![spy_emit];
}
