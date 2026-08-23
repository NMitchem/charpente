// Positive control for compile_fail/route_emits_on_non_sse.rs: the identical
// `emits(NoteAdded)` declaration on an SSE route — which is the remedy that
// error prescribes verbatim ("make this `#[route(SSE \"…\")]`"). It proves
// the rejection is caused by the route not being a stream, not by an
// unrelated bound, import, or typo in the event type.
//
// `feed_private` additionally pins the claim docs/GUIDE.md makes about
// combining the two flags in either order — `auth` before `emits(...)`, and
// more than one event named.
use charpente::http::{ApiError, Ctx, Cursor, Subscription};
use charpente::wire::WireU64;
use charpente::{Tx, event, route};

#[event]
struct NoteAdded {
    n: WireU64,
}

#[event]
struct NoteRemoved {
    n: WireU64,
}

#[route(SSE "/x", emits(NoteAdded))]
fn handler(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<NoteAdded>().cursor(cursor))
}

#[route(SSE "/y", auth, emits(NoteAdded, NoteRemoved))]
fn feed_private(
    tx: &mut Tx,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<NoteAdded>()
        .and::<NoteRemoved>()
        .cursor(cursor))
}

fn main() {}
