// The second half of spec §1's compile-time property, and §5's compile-fail
// matrix: a `ScopedTx` opened on one scope type cannot stand in for another.
// Without this, `scope()` would only prove that SOME tenant was authorized,
// not that it was the one the callee reads.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::{Id, Scoped, ScopedTx, TenantScope, Tx, table};

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

impl TenantScope for OrgA {
    fn authorize(_tx: &Tx, _who: Option<&Principal>, _scope: Id<OrgA>) -> Result<(), ApiError> {
        Ok(())
    }
}

impl TenantScope for OrgB {
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
