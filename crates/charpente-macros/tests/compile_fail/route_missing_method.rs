// The first token in `#[route(...)]` must be the HTTP method, an identifier
// (GET, POST, …); a route written with the path first and no method has
// nothing there for `RouteArgs::parse` to read as one (route.rs:47), which
// is a different failure than an unrecognized method name.
//
// Positive control: compile_pass/route_auth_flag.rs — the same route with
// the method present.
use charpente::route;

#[route("/x", auth)]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
