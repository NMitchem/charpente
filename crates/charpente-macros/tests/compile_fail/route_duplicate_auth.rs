// A flag written twice is refused, not silently folded (task 7 review,
// Minor 5). `auth` is a yes/no, so the second one adds nothing and is
// always a typo or a bad merge — the same call `#[job]` already makes for
// `max`/`backoff` given twice.
//
// Positive control: compile_pass/route_auth_flag.rs — this same route with
// the one offending element made legal (`auth` written once).
//
// Paths are fully qualified so the dropped expansion leaves no unused-import
// warnings in the pinned .stderr.
use charpente::route;

#[route(GET "/x", auth, auth)]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
