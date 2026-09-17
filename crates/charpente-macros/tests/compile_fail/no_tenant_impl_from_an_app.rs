// `NoTenant` is the marker spec §7's exit criterion 1 rests on: `View`'s bound
// is `UnscopedRow`, which holds exactly when the table's scope type implements
// `NoTenant<Row>`. The impl below is legal under the orphan rules — `Org` is
// local — so without a seal it is one safe line that makes every scoped table
// readable from a bare `Tx` with no `authorize` anywhere. The seal is what
// keeps the criterion a compile error rather than an opt-in.
use charpente::auth::Principal;
use charpente::core::table::NoTenant;
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
    fn authorize(_tx: &Tx, _who: Option<&Principal>, _scope: Id<Org>) -> Result<(), ApiError> {
        Err(ApiError::Unauthorized)
    }
}

impl NoTenant<Invoice> for Org {}

fn list(tx: &mut Tx) -> Result<Vec<Invoice>, ApiError> {
    Ok(tx.scan::<Invoice>()?)
}

fn main() {
    let _ = list;
}
