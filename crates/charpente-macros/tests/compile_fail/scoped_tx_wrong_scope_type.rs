// The second half of spec §1's compile-time property, and §5's compile-fail
// matrix: a `ScopedTx` opened on one scope type cannot stand in for another.
// Without this, `scope()` would only prove that SOME tenant was authorized,
// not that it was the one the callee reads.
//
// The `.stderr` is a stock `E0308`, not the house diagnostic the sibling case
// carries: `#[diagnostic::on_unimplemented]` fires only on an unimplemented
// TRAIT obligation, and this is a generic-parameter mismatch between two
// concrete `ScopedTx` types, which names both tenants by itself.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::{Id, Scoped, ScopedTx, Tenant, Tx, table};

#[table]
struct OrgA {
    id: Id<OrgA>,
    name: String,
}

#[table]
struct OrgB {
    id: Id<OrgB>,
    name: String,
}

impl Tenant for OrgA {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: Option<&Principal>, _scope: Id<OrgA>) -> Result<(), ApiError> {
        Ok(())
    }
}

impl Tenant for OrgB {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: Option<&Principal>, _scope: Id<OrgB>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[table(scoped_by = OrgB)]
struct Invoice {
    id: Id<Invoice>,
    total: u32,
}

fn billing(tx: &ScopedTx<'_, '_, OrgB>) -> Result<Vec<Invoice>, ApiError> {
    Ok(tx.scan::<Invoice>()?)
}

fn handler(tx: &mut Tx, a: Id<OrgA>, ctx: &Ctx) -> Result<Vec<Invoice>, ApiError> {
    let tx = tx.scope::<OrgA>(a, ctx)?;
    billing(&tx)
}

fn main() {
    let _ = handler;
}
