// `emits` with the parentheses forgotten. Without the peek in the parser
// this falls through to syn's generic "expected parentheses", which names no
// fix — every other branch of this parser gives a charpente message, so this
// one does too (hard rule 5).
//
// Positive control: compile_pass/route_emits_on_sse.rs's `handler`, which is
// this same route with the one offending element made legal (the
// parenthesized list).
//
// Paths are fully qualified so the dropped expansion leaves no unused-import
// warnings in the pinned .stderr.
use charpente::route;

#[route(SSE "/x", emits)]
fn handler(
    tx: &mut charpente::Tx,
    ctx: &charpente::http::Ctx,
    cursor: Option<charpente::http::Cursor>,
) -> Result<charpente::http::Subscription, charpente::http::ApiError> {
    let _ = (tx, ctx, cursor);
    unimplemented!()
}

fn main() {}
