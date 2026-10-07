// Positive control for compile_fail/scoped_read_from_bare_tx.rs and
// compile_fail/scoped_tx_wrong_scope_type.rs: the identical scoped table,
// read through the `ScopedTx` that `tx.scope::<Org>(org, ctx)?` hands back —
// proving the two rejections are the missing scope and the wrong scope, not
// `scoped_by` making the table unreadable. Also pins the surface Tasks 9-11
// build on: every read, both writes, the typed `by_*` index accessor, and
// the deref back out to an unscoped table.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::{Id, Scoped, Tenant, Tx, View, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(global)]
struct Region {
    id: Id<Region>,
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

#[table(scoped_by = Org)]
struct Invoice {
    id: Id<Invoice>,
    #[index]
    paid: bool,
    total: u32,
}

fn list(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<Vec<Invoice>, ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;

    tx.put(&Invoice {
        id: Id::from_raw(1),
        paid: false,
        total: 500,
    });
    tx.delete::<Invoice>(Id::from_raw(2));

    let _one = tx.get::<Invoice>(Id::from_raw(1))?;
    let _page = tx.scan_from::<Invoice>(0, 10)?;
    let _unpaid = tx.scan_by::<Invoice>("paid", &false)?;
    let _also_unpaid = Invoice::by_paid(&tx, &false)?;
    let _mine = tx.scope();

    // Opening a scope must not lock the handler out of unscoped tables
    // (spec §1). Method resolution does not fall through a deref when an
    // inherent method of the same name exists, so the deref is explicit.
    (*tx).put(&Region {
        id: Id::from_raw(9),
        name: "eu".into(),
    });
    let _global = (*tx).scan::<Region>()?;

    Ok(tx.scan::<Invoice>()?)
}

fn main() {
    let _ = list;
}
