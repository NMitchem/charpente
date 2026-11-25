// `intent` names where a webhook sender's id arrives, and in v1 that is a
// header: `intent = header("X-Event-Id")`. A bare string (the `key = value`
// shape `#[job]`'s `outbound = "<host>"` takes) is the likeliest slip, and
// it is refused naming the one accepted form (hard rule 5) rather than with
// syn's generic "expected identifier".
//
// Positive control: compile_pass/route_intent_header.rs's `stripe_hook`,
// this same route with the header written as `header(...)`.
use charpente::route;

#[route(POST "/hooks/pay", public, intent = "X-Event-Id")]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
