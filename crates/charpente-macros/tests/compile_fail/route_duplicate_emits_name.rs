// The same event type named twice inside one `emits(...)` list is refused
// (task 7 review, Minor 5). The drift oracle cannot catch this one: a
// handler chaining `.of::<NoteAdded>().and::<NoteAdded>()` would agree with
// it, and the contract would carry the same union member twice.
//
// Positive control: compile_pass/route_emits_on_sse.rs's `handler` — this
// same route with the one offending element made legal (the duplicate
// dropped), which is the exact remedy this error prescribes.
//
// The event is defined, not just named, so the rejection cannot be blamed on
// an unresolved type.
use charpente::wire::WireU64;
use charpente::{event, route};

#[event(global)]
struct NoteAdded {
    n: WireU64,
}

#[route(SSE "/x", public, emits(NoteAdded, NoteAdded))]
fn handler(
    tx: &mut charpente::Tx,
    ctx: &charpente::http::Ctx,
    cursor: Option<charpente::http::Cursor>,
) -> Result<charpente::http::Subscription, charpente::http::ApiError> {
    let _ = (tx, ctx, cursor);
    unimplemented!()
}

fn main() {}
