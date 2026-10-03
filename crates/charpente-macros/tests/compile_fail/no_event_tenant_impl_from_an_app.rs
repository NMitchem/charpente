// The event twin of `no_job_tenant_impl_from_an_app.rs` (tenancy-completion
// spec §6). `UnscopedEvent` holds exactly when the event's scope type
// implements `NoEventTenant<Event>`. The impl below is legal under the orphan
// rules, since `Org` is local. So without a seal it is one safe line that
// lets a scoped event be emitted on a bare `Tx`. The seal is the same
// `NotATenant` that `NoTenant` and `NoJobTenant` carry, and it keeps that a
// compile error.
use charpente::auth::Principal;
use charpente::core::table::NoEventTenant;
use charpente::http::ApiError;
use charpente::{Id, Tenant, Tx, event, table};

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
        Err(ApiError::Unauthorized)
    }
}

#[event(scoped_by = Org)]
struct Noted {
    n: charpente::wire::WireU64,
}

impl NoEventTenant<Noted> for Org {}

fn emit(tx: &mut Tx) {
    tx.emit(Noted {
        n: charpente::wire::WireU64(1),
    });
}

fn main() {
    let _ = emit;
}
