// A job may be declared inside a function body, as any item may. The glue
// `#[job]` emits names the struct from functions nested in `def()`, which
// resolve fn-local items; a `#[no_implicit_prelude]` module would not, which
// is why the expansion does not use one (it would have given `call`'s E0599
// plainer text). A pure job and a scoped outbound job, both in `main`.
// trybuild runs `main`, so its assertions are part of the pin.
use charpente::auth::Principal;
use charpente::core::db::Job;
use charpente::http::ApiError;
use charpente::kernel::{JobEntry, JobError};
use charpente::outbound::{Call, IdempotencyKey, Reply};
use charpente::{Id, ScopedTx, Tenant, Tx, job, table};

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

fn main() {
    #[job(global)]
    struct Tidy {
        n: u64,
    }

    impl Tidy {
        fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
            let _ = self.n;
            Ok(())
        }
    }

    #[job(scoped_by = Org, outbound = "api.stripe.com")]
    struct Charge {
        cents: u64,
    }

    impl Charge {
        fn call(
            &self,
            _tx: &ScopedTx<'_, '_, Org>,
            key: &IdempotencyKey,
        ) -> Result<Option<Call>, JobError> {
            Ok(Some(
                Call::post("/charges")
                    .header("idempotency-key", key.as_str())
                    .form(&[("amount", self.cents.to_string())]),
            ))
        }

        fn settle(self, _tx: &mut ScopedTx<'_, '_, Org>, _reply: Reply) -> Result<(), JobError> {
            Ok(())
        }
    }

    assert!(<Tidy as JobEntry>::def().outbound.is_none());
    let def = <Charge as JobEntry>::def();
    assert_eq!(def.scoped_by, Some("Org"));
    assert_eq!(def.outbound.map(|o| o.host), Some("api.stripe.com"));
    assert!(<Charge as Job>::OUTBOUND);
}
