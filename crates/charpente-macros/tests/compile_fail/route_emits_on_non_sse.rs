// `emits(...)` names the event types a STREAM carries, so it is meaningless
// on a route that returns one response. Rejected by name, at the attribute,
// rather than silently ignored — a declaration the contract drops would
// generate a client for events nothing ever sends.
//
// Positive control: compile_pass/route_emits_on_sse.rs — the same
// `emits(NoteAdded)` with the one offending element made legal (the method),
// which is also the exact remedy this error prescribes.
//
// Paths are fully qualified rather than imported so the expansion being
// dropped on error leaves no unused-import warnings in the pinned .stderr.
use charpente::wire::WireU64;
use charpente::{event, route};

#[event]
struct NoteAdded {
    n: WireU64,
}

#[route(GET "/x", public, emits(NoteAdded))]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
