// The route flags are `auth` and `emits(Ev, …)`; anything else in that
// position is rejected by name, with both accepted forms spelled out (hard
// rule 5). Pinned here because `docs/GUIDE.md`'s error catalog quotes this
// message verbatim, and before task 7 nothing tested it — a reword and the
// docs would have drifted silently.
//
// Positive control: compile_pass/route_auth_flag.rs — the same route with a
// real flag in that same position.
use charpente::route;

#[route(GET "/x", cached)]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
