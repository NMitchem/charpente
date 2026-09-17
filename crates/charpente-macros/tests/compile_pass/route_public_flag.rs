// Positive control for compile_fail/route_duplicate_public.rs and
// compile_fail/route_auth_and_public.rs: `public` once is the legal spelling
// of "anonymous callers reach this handler" (default-secure spec §2), and it
// expands to exactly what a bare route expanded to before the flag existed.
use charpente::route;

#[route(GET "/x", public)]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
