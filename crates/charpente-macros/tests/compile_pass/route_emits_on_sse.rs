// Positive control for compile_fail/route_emits_on_non_sse.rs: the identical
// `emits(NoteAdded)` declaration on an SSE route — which is the remedy that
// error prescribes verbatim ("make this `#[route(SSE \"…\")]`"). It proves
// the rejection is caused by the route not being a stream, not by an
// unrelated bound, import, or typo in the event type.
//
// `feed_private` and `feed_reversed` additionally pin the claim
// docs/GUIDE.md makes about combining the two flags in EITHER order — one
// route each way, since a parser that accepted only `auth, emits(…)` would
// leave half that claim untested (task 7 review, Minor 6). `feed_private`
// also pins the multi-event form.
//
// `handler` and `feed_merged` are also the positive controls for the two
// duplicate rejections (task 7 review, Minor 5): `handler` is
// compile_fail/route_duplicate_emits_name.rs with the repeat dropped, and
// `feed_merged` is compile_fail/route_duplicate_emits_clause.rs with the two
// clauses written as the one list that error prescribes.
use charpente::http::{ApiError, Ctx, Cursor, Subscription};
use charpente::wire::WireU64;
use charpente::{Tx, event, route};

#[event(global)]
struct NoteAdded {
    n: WireU64,
}

#[event(global)]
struct NoteRemoved {
    n: WireU64,
}

#[route(SSE "/x", public, emits(NoteAdded))]
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

#[route(SSE "/z", emits(NoteRemoved), auth)]
fn feed_reversed(
    tx: &mut Tx,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<NoteRemoved>().cursor(cursor))
}

#[route(SSE "/w", public, emits(NoteAdded, NoteRemoved))]
fn feed_merged(
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
