// Positive control for compile_fail/route_unknown_flag.rs: the identical
// route with `auth` — a real flag — where the bogus one stood, proving the
// rejection is the flag's NAME and not the trailing-comma form itself.
use charpente::route;

#[route(GET "/x", auth)]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
