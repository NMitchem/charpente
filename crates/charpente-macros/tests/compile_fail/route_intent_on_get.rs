// An intent id is what makes a re-sent WRITE answer once (GUIDE §5). A GET
// route never reads one, so `intent` on it would be a declaration the
// request path drops: refused by name, at the attribute.
//
// Positive control: compile_pass/route_intent_header.rs's `stripe_hook`,
// the same `intent = header(...)` on a POST, the remedy this error names.
use charpente::route;

#[route(GET "/hooks/pay", public, intent = header("X-Event-Id"))]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
