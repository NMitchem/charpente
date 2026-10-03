// A global event emitted through a `ScopedTx` without the deref
// (tenancy-completion spec §6) is an E0271. It is never silently recorded in
// this tenant, where no global feed would receive it. `ScopedTx::emit`
// shadows `Tx::emit`, and Rust does not fall through the deref when the
// inherent method's bound fails. The fix is `(*tx).emit(SystemNotice {})`,
// and GUIDE §11 shows it.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::{Id, Scoped, Tenant, Tx, event, route, table};

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

#[event(global)]
struct SystemNotice {}

#[route(POST "/orgs/:org/notice", auth)]
fn notice(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.emit(SystemNotice {});
    Ok(())
}

fn main() {}
