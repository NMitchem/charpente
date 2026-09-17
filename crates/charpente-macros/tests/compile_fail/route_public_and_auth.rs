// The other order of route_auth_and_public.rs: the one-or-the-other refusal
// is checked in BOTH parse branches (route.rs), and a bare `if public { … }`
// guard has no operator for cargo-mutants to mutate, so only a pin can see
// it.
//
// Positive controls: compile_pass/route_auth_flag.rs and
// compile_pass/route_public_flag.rs.
//
// Paths are fully qualified so the dropped expansion leaves no unused-import
// warnings in the pinned .stderr.
use charpente::route;

#[route(GET "/x", public, auth)]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
