// `emits()` naming nothing is rejected rather than treated as "no
// declaration". Silently accepting it would also punch a hole in the
// non-SSE rejection next door — `#[route(GET "/x", emits())]` would sail
// past a guard that reads "declared something, on a non-stream route".
//
// Positive control: compile_pass/route_emits_on_sse.rs's `handler`, which is
// this same declaration with the one offending element made legal (a name
// inside the parentheses).
//
// Paths are fully qualified so the dropped expansion leaves no unused-import
// warnings in the pinned .stderr.
use charpente::route;

#[route(SSE "/x", public, emits())]
fn handler(
    tx: &mut charpente::Tx,
    ctx: &charpente::http::Ctx,
    cursor: Option<charpente::http::Cursor>,
) -> Result<charpente::http::Subscription, charpente::http::ApiError> {
    let _ = (tx, ctx, cursor);
    unimplemented!()
}

fn main() {}
