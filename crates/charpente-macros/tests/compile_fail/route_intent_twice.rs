// A route reads its id from one header. A second `intent` is a typo or a bad
// merge, and keeping either one silently would key the route's writes by a
// header its sender may not send: refused, like every other repeated flag.
//
// Positive control: compile_pass/route_intent_header.rs's `stripe_hook`,
// this same route with `intent` written once.
use charpente::route;

#[route(
    POST "/hooks/pay",
    public,
    intent = header("X-Event-Id"),
    intent = header("Stripe-Event-Id")
)]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
