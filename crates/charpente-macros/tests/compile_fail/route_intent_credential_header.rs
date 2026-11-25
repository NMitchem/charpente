// The intent id is stored raw in the record of answered writes, so naming a
// credential header would put the credential on disk as it was sent (DESIGN
// §8): `Cookie: charpente_session=<token>` holds no space, so it would pass
// as an id. Refused at compile time by name, whatever its case:
// `authorization`, `proxy-authorization` and `cookie`.
//
// Positive control: compile_pass/route_intent_header.rs's `stripe_hook`,
// this same route naming its sender's event-id header.
use charpente::route;

#[route(POST "/hooks/pay", public, intent = header("Cookie"))]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
