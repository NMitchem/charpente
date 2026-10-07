// Tenancy-completion spec §6 (Task 11 fix round 1): the global sites'
// check. A global event is recorded under its payload's field fold alone, so
// its `FINGERPRINT` equals its `PAYLOAD_FINGERPRINT`, which is the default.
// `Notice` below copied a scoped event's two fingerprints and changed only its
// `type Scope`. That is not a door into a tenant (`admits` keeps every global
// feed off every tenant's rows), but its recorded shape would claim a tenant
// it does not have. `Tx::emit`, `Tx::emit_keyed`, `Subscription::of` and
// `Subscription::and` check it in a `const` block. Post-monomorphization, so
// `main` mounts both routes; one call per site, so each refusal is pinned.
use charpente::core::table::{Codec, Unscoped};
use charpente::http::{ApiError, Ctx, Cursor, Subscription};
use charpente::wire::WireU64;
use charpente::{Event, Tx, event, route, routes};

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
struct InvoicePaid {
    invoice: WireU64,
}

#[event(global)]
struct SystemNotice {
    n: WireU64,
}

#[derive(serde::Serialize)]
struct Notice {
    invoice: WireU64,
}

impl Codec for Notice {
    fn encode(&self, out: &mut Vec<u8>) {
        self.invoice.encode(out)
    }
    fn decode(input: &mut &[u8]) -> std::io::Result<Notice> {
        Ok(Notice {
            invoice: WireU64::decode(input)?,
        })
    }
}

impl Event for Notice {
    const NAME: &'static str = "notice";
    const FINGERPRINT: u64 = <InvoicePaid as Event>::FINGERPRINT;
    const PAYLOAD_FINGERPRINT: u64 = <InvoicePaid as Event>::PAYLOAD_FINGERPRINT;
    type Scope = Unscoped;
}

#[route(POST "/notices", public)]
fn post_notice(tx: &mut Tx) -> Result<(), ApiError> {
    tx.emit(Notice {
        invoice: WireU64(1),
    });
    tx.emit_keyed("user:7", Notice {
        invoice: WireU64(2),
    });
    Ok(())
}

#[route(SSE "/notices", public)]
fn notices(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    let _alone = Subscription::of::<Notice>();
    Ok(Subscription::of::<SystemNotice>()
        .and::<Notice>()
        .cursor(cursor))
}

fn main() {
    let _ = routes![post_notice, notices];
}
