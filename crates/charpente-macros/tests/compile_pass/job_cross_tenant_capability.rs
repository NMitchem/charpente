// `#[job(global, cross_tenant)]` — the job half of spec §3's declared capability.
// A declared job's `run` takes the token after `tx`; the glue `#[job]` emits
// passes the one the dispatcher minted, so a background rollup can enumerate
// every tenant for the same reason a declared route can.
//
// Also pins that the flag composes with `retry(...)`, since the two share
// one attribute.
use charpente::auth::Principal;
use charpente::kernel::JobError;
use charpente::{CrossTenant, Id, Tenant, Tx, job, route, table};

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
    type Error = charpente::http::ApiError;
    fn authorize(
        _tx: &Tx,
        _who: &Principal,
        _scope: Id<Org>,
    ) -> Result<(), charpente::http::ApiError> {
        Ok(())
    }
}

#[job(global, cross_tenant, retry(max = 2, backoff = "5s"))]
struct Rollup {
    day: u64,
}

impl Rollup {
    fn run(self, tx: &mut Tx, cap: &CrossTenant) -> Result<(), JobError> {
        let _ = self.day;
        let rows: Vec<(Id<Org>, Invoice)> = tx.across_tenants::<Org>(cap).scan::<Invoice>()?;
        let _ = rows;
        Ok(())
    }
}

/// A job without `cross_tenant` still calls `run(tx)`: the argument is added only by
/// the declaration, so every existing job keeps its signature.
#[job(global)]
struct Plain {
    n: u64,
}

impl Plain {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        let _ = self.n;
        Ok(())
    }
}

#[route(GET "/x", public)]
fn handler(tx: &mut Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
