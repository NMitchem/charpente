// `UnscopedRow` is the trait `View`'s methods actually name, so sealing
// `NoTenant` alone leaves the same one-line opt-out one level up: the impl
// below is orphan-legal (`Invoice` is local) and, unsealed, it satisfies
// `View`'s bound directly without the marker ever being mentioned — a scoped
// table read from a bare `Tx` with no `authorize` anywhere. Spec §7's exit
// criterion 1 is a compile error, so the bound cannot be an opt-in either.
use charpente::auth::Principal;
use charpente::core::table::UnscopedRow;
use charpente::http::ApiError;
use charpente::{Id, Tenant, Tx, View, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(scoped_by = Org)]
struct Invoice {
    id: Id<Invoice>,
    total: u32,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Err(ApiError::Unauthorized)
    }
}

impl UnscopedRow for Invoice {}

fn list(tx: &mut Tx) -> Result<Vec<Invoice>, ApiError> {
    Ok(tx.scan::<Invoice>()?)
}

fn main() {
    let _ = list;
}
