// A flag written twice is refused, not silently folded — the same call
// route_duplicate_auth.rs makes for `auth`. `public` is a yes/no, so the
// second one adds nothing and is always a typo or a bad merge.
//
// Positive control: compile_pass/route_public_flag.rs — this same route with
// `public` written once.
use charpente::route;

#[route(GET "/x", public, public)]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
