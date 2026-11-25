// An intent id is what makes a re-sent WRITE answer once (GUIDE §5). A GET
// route never reads one, and neither does an SSE route (it registers as a
// GET), so `intent` on either would be a declaration the request path drops:
// refused by name, at the attribute. One route each, since the refusal's
// condition names both methods.
//
// Positive control: compile_pass/route_intent_header.rs's `stripe_hook`,
// the same `intent = header(...)` on a POST, the remedy this error names.
//
// Paths are fully qualified so the dropped expansion leaves no unused-import
// warnings in the pinned .stderr.
use charpente::route;

#[route(GET "/hooks/pay", public, intent = header("X-Event-Id"))]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

#[route(SSE "/hooks/feed", public, intent = header("X-Event-Id"))]
fn feed(
    tx: &mut charpente::Tx,
    ctx: &charpente::http::Ctx,
    cursor: Option<charpente::http::Cursor>,
) -> Result<charpente::http::Subscription, charpente::http::ApiError> {
    let _ = (tx, ctx, cursor);
    unimplemented!()
}

fn main() {}
