// Positive control for route_bare_u64_param.rs: the identical route shape
// with `id` carried as `Id<Thing>` instead of a bare `u64` — proves the
// compile_fail case is rejected because of the 64-bit path param
// specifically, not because of an unrelated bound, import, or typo in that
// fixture. `Id<T>` implements both `FromParam` and `Wire`
// (`crates/charpente/src/http/mod.rs`), so it is a legal path param type.
use charpente::core::table::Id;
use charpente::{Tx, route, table};

#[table]
struct Thing {
    id: Id<Thing>,
}

#[route(GET "/things/:id")]
fn handler(tx: &mut Tx, id: Id<Thing>) -> Result<(), charpente::http::ApiError> {
    let _ = (tx, id);
    Ok(())
}

fn main() {}
