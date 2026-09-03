// Spec §7 exit criterion 1 — "a cross-tenant read on a scoped table does not
// compile" — held against the SEAM, not just against `View`. The six
// scope-taking primitives are the only way to address a tenant's rows, and
// they are reachable from app code only through the `ScopedTx` that
// `tx.scope::<Org>(scope, ctx)?` hands back once `Tenant::authorize` has
// passed. A handler holding a bare `&mut Tx` names no tenant, so none of the
// six may resolve for it.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::{Id, Tenant, Tx, table};

#[table]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(scoped_by = Org)]
struct Invoice {
    id: Id<Invoice>,
    #[index]
    total: u32,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: Option<&Principal>, _scope: Id<Org>) -> Result<(), ApiError> {
        Err(ApiError::Unauthorized)
    }
}

fn read(tx: &mut Tx, other: Id<Org>) -> Result<Vec<Invoice>, ApiError> {
    let _ = tx.get_in(other, Id::<Invoice>::from_raw(7))?;
    let _ = tx.scan_by_in::<Org, Invoice>(other, "by_total", &1u32)?;
    let _ = tx.scan_from_in::<Org, Invoice>(other, 0, 10)?;
    Ok(tx.scan_in::<Org, Invoice>(other)?)
}

fn write(tx: &mut Tx, other: Id<Org>) {
    tx.put_in(
        other,
        &Invoice {
            id: Id::from_raw(7),
            total: 1,
        },
    );
    tx.delete_in(other, Id::<Invoice>::from_raw(7));
}

fn main() {
    let _ = read;
    let _ = write;
}
