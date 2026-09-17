//! Fix round 1 (F8): the Step 7 fixture (`route_bare_u64_body.rs`) pins the
//! `#[route]`-position `Wire` bound; nothing pinned the OTHER new rejection
//! surface `result_ok_type`/`result_err_type` create — a handler whose
//! return type isn't `Result<T, E>` at all. `handler` returns `String`
//! directly, which hits `result_arg`'s `seg.ident != "Result"` branch.
use charpente::route;

#[route(GET "/x", public)]
fn handler(tx: &mut charpente::Tx) -> String {
    let _ = tx;
    String::new()
}

fn main() {}
