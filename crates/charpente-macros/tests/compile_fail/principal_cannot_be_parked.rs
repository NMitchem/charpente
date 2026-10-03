// A `Principal` cannot outlive its request (tenancy-completion spec §2
// ruling 5, amendment A19). `Tx::open_scope` takes any `&Principal` as the
// `who` it asks `authorize` about, so a principal copied into a `static` by
// one request would let every later request, on any route and with no
// credential of its own, open a tenant that principal belongs to: here a
// plain `public` route reads org `org`'s memos on a parked member's word.
// `Principal` is not `Clone`, so there is no copy to park: `.cloned()` has no
// `Clone` to call, and `.clone()` copies only the `&Principal` the request's
// `Ctx` lends.
use std::sync::Mutex;

use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx};
use charpente::wire::WireU64;
use charpente::{Id, Tenant, Tx, route, routes, table};

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

#[table(scoped_by = Org)]
struct Memo {
    id: Id<Memo>,
    text: String,
}

static PARKED: Mutex<Option<Principal>> = Mutex::new(None);

#[route(POST "/park", auth)]
fn park(tx: &mut Tx, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let _ = tx;
    *PARKED.lock().unwrap() = ctx.principal().cloned();
    Ok(WireU64(1))
}

#[route(POST "/park-by-hand", auth)]
fn park_by_hand(tx: &mut Tx, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let _ = tx;
    let held: Principal = ctx.principal().expect("auth route").clone();
    *PARKED.lock().unwrap() = Some(held);
    Ok(WireU64(1))
}

#[route(GET "/orgs/:org/memos", public)]
fn read_as_parked(tx: &mut Tx, org: Id<Org>) -> Result<WireU64, ApiError> {
    let parked = PARKED.lock().unwrap();
    let who = parked.as_ref().ok_or(ApiError::NotFound)?;
    let tx = tx.open_scope::<Org>(org, who)?;
    Ok(WireU64(tx.scan::<Memo>()?.len() as u64))
}

fn main() {
    let _ = routes![park, park_by_hand, read_as_parked];
}
