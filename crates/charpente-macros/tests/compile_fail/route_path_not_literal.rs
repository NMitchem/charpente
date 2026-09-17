// The second token in `#[route(...)]` must be the path as a string literal;
// a bare identifier in that position is refused by name rather than syn's
// generic parse error (route.rs:54).
//
// Positive control: compile_pass/route_auth_flag.rs — the same route with
// the path as a string literal.
use charpente::route;

#[route(GET x, auth)]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
