// A header name is an HTTP token: letters, digits and !#$%&'*+-.^_`|~. A
// name with a space (or a colon, or nothing at all) is one no request can
// carry, so the route would refuse every write it gets with a 400 naming a
// header the sender cannot send. Refused at compile time instead, naming
// the rule.
//
// Positive control: compile_pass/route_intent_header.rs's `stripe_hook`,
// this same route with the space written as a dash.
use charpente::route;

#[route(POST "/hooks/pay", public, intent = header("X Event Id"))]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
