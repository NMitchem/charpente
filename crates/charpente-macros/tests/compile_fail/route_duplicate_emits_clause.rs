// Two `emits(...)` clauses on one route are refused rather than concatenated
// (task 7 review, Minor 5). A route declares ONE set; two clauses read as
// two sets, and whichever order they concatenate in silently becomes the
// contract's — and `RouteInfo.emits`'s order is the generated client's union
// order, so a reader guessing wrong guesses the contract wrong.
//
// Positive control: compile_pass/route_emits_on_sse.rs's `feed_merged` —
// this same route with the one offending element made legal (the two names
// in one list), which is the exact remedy this error prescribes.
//
// The events are defined, not just named, so the rejection cannot be blamed
// on an unresolved type.
use charpente::wire::WireU64;
use charpente::{event, route};

#[event]
struct NoteAdded {
    n: WireU64,
}

#[event]
struct NoteRemoved {
    n: WireU64,
}

#[route(SSE "/x", public, emits(NoteAdded), emits(NoteRemoved))]
fn handler(
    tx: &mut charpente::Tx,
    ctx: &charpente::http::Ctx,
    cursor: Option<charpente::http::Cursor>,
) -> Result<charpente::http::Subscription, charpente::http::ApiError> {
    let _ = (tx, ctx, cursor);
    unimplemented!()
}

fn main() {}
