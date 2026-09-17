// A route is `auth` or `public`, never both (default-secure spec §2): the two
// flags answer one question — who reaches this handler — and a route that
// says both has not answered it. Refused at the second flag, by name.
//
// Positive controls: compile_pass/route_auth_flag.rs and
// compile_pass/route_public_flag.rs — this same route with one flag.
//
// Paths are fully qualified so the dropped expansion leaves no unused-import
// warnings in the pinned .stderr.
use charpente::route;

#[route(GET "/x", auth, public)]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
